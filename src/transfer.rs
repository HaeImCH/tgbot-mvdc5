//! Parallel part-based download/upload engine with the stall/rate-limit handling
//! ported from telebackup's `download_pool.py`:
//!
//! * per-request timeout -> drop that connection, reconnect, requeue part
//!   (does not consume the part's retry budget)
//! * progress watchdog -> no bytes for `stall_timeout` across all workers ->
//!   abort every worker and restart the round with fresh connections; completed
//!   parts are kept (resume), give up after `max_stall_restarts`
//! * FLOOD_WAIT_X -> job-wide pause (all workers of this job back off), watchdog
//!   does not count paused time as a stall
//! * FLOOD_PREMIUM_WAIT_X -> rebuild connection (default) or wait
//! * connection closed / IO -> reconnect with jitter + exponential backoff
//! * AUTH_KEY_UNREGISTERED -> invalidate the DC key and re-import authorization
//! * FILE_REFERENCE_* -> refetch the message to refresh the file location
//! * short/empty response before EOF -> retry the part

use std::collections::VecDeque;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use grammers_mtproto::transport;
use grammers_mtsender::InvocationError;
use grammers_tl_types::{self as tl, Deserializable, Serializable};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::PremiumWaitStrategy;
use crate::net::{Conn, Net, OpenError};
use crate::status::Progress;

pub const UPLOAD_PART: u64 = 512 * 1024;
pub const BIG_FILE: u64 = 10 * 1024 * 1024;
/// Bots and non-Premium users: at most 4000 parts of 512 KB (2000 MiB).
pub const MAX_UPLOAD_PARTS: u64 = 4000;
/// Premium users: 8000 parts of 512 KB (4000 MiB).
pub const PREMIUM_UPLOAD_PARTS: u64 = 8000;

pub type BoxFut<T> = std::pin::Pin<Box<dyn Future<Output = T> + Send>>;
pub type Refresher = Box<dyn Fn() -> BoxFut<Result<tl::enums::InputFileLocation>> + Send + Sync>;

/// File location that can be refreshed when the file reference expires.
pub struct Location {
    current: Mutex<(u64, tl::enums::InputFileLocation)>,
    refresh_lock: tokio::sync::Mutex<()>,
    refresher: Refresher,
}

impl Location {
    pub fn new(loc: tl::enums::InputFileLocation, refresher: Refresher) -> Self {
        Self {
            current: Mutex::new((0, loc)),
            refresh_lock: tokio::sync::Mutex::new(()),
            refresher,
        }
    }
    fn get(&self) -> (u64, tl::enums::InputFileLocation) {
        self.current.lock().unwrap().clone()
    }
    /// Refresh once per generation even if several workers hit the error.
    async fn refresh(&self, seen_gen: u64) -> Result<()> {
        let _g = self.refresh_lock.lock().await;
        if self.current.lock().unwrap().0 != seen_gen {
            return Ok(());
        }
        let new = (self.refresher)().await?;
        let mut cur = self.current.lock().unwrap();
        *cur = (cur.0 + 1, new);
        Ok(())
    }
}

pub enum Direction {
    Download {
        location: Arc<Location>,
        /// Size is exact (documents). Photos may be slightly off; accept a short last part.
        exact_size: bool,
    },
    Upload {
        file_id: i64,
        big: bool,
        /// Byte offset of this upload inside the source file (split pieces).
        base: u64,
    },
}

/// Static description of how a file of `size` bytes is split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Plan {
    pub size: u64,
    pub part_size: u64,
    pub parts: u32,
}

impl Plan {
    pub fn new(size: u64, part_size: u64) -> Self {
        let parts = size.div_ceil(part_size).max(1) as u32;
        Self {
            size,
            part_size,
            parts,
        }
    }
    pub fn offset(&self, part: u32) -> u64 {
        part as u64 * self.part_size
    }
    /// Expected byte length of `part`.
    pub fn len(&self, part: u32) -> u64 {
        let off = self.offset(part);
        self.size.saturating_sub(off).min(self.part_size)
    }
}

impl Ctx {
    fn is_home_download(&self) -> bool {
        self.dc == self.net.home_dc && matches!(self.dir, Direction::Download { .. })
    }
}

struct Ctx {
    net: Arc<Net>,
    dc: i32,
    plan: Plan,
    dir: Direction,
    path: PathBuf,
    progress: Arc<Progress>,
    done: Vec<AtomicBool>,
    /// Actual end-of-file seen (for photos with inexact sizes).
    actual_end: AtomicU64,
    // Stats for the end-of-transfer log line.
    requests: AtomicU64,
    request_micros: AtomicU64,
    flood_waits: AtomicU64,
    /// Connections acquired on the media-only address / the regular address.
    media_acquired: AtomicU64,
    regular_acquired: AtomicU64,
}

enum PartErr {
    Flood(u64),
    PremiumWait(u64),
    Reconnect(String),
    Reauth(String),
    FileRef,
    Retry(String),
    Fatal(String),
}

fn classify(e: InvocationError) -> PartErr {
    use InvocationError as E;
    match e {
        E::Rpc(r) if r.code == 420 && r.name.starts_with("FLOOD_PREMIUM_WAIT") => {
            PartErr::PremiumWait(r.value.unwrap_or(3) as u64)
        }
        E::Rpc(r) if r.code == 420 => PartErr::Flood(r.value.unwrap_or(5).max(1) as u64),
        E::Rpc(r) if r.code == 401 || r.name.starts_with("AUTH_KEY") => {
            PartErr::Reauth(r.to_string())
        }
        E::Rpc(r) if r.name.starts_with("FILE_REFERENCE") => PartErr::FileRef,
        E::Rpc(r) if r.code >= 500 || r.code < 0 || r.name == "TIMEOUT" => {
            PartErr::Retry(r.to_string())
        }
        E::Rpc(r) => PartErr::Fatal(r.to_string()),
        E::Transport(transport::Error::BadStatus { status: 404 }) => {
            PartErr::Reauth("transport 404 (auth key unknown)".into())
        }
        E::Transport(transport::Error::BadStatus { status: 429 }) => {
            PartErr::Flood(3) // too many connections from this IP
        }
        e @ (E::Io(_) | E::Transport(_) | E::Deserialize(_) | E::Dropped(_)) => {
            PartErr::Reconnect(e.to_string())
        }
        e @ E::Authentication(_) => PartErr::Reconnect(e.to_string()),
        e @ (E::Session(_) | E::InvalidDc) => PartErr::Fatal(e.to_string()),
    }
}

fn jitter(max_ms: u64) -> Duration {
    if max_ms == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rand::random::<u64>() % max_ms)
}

/// Run a full download (DC -> file) or upload (file -> DC) with `conns` connections.
pub async fn run(
    net: Arc<Net>,
    dc: i32,
    conns: usize,
    plan: Plan,
    dir: Direction,
    path: PathBuf,
    progress: Arc<Progress>,
) -> Result<u64> {
    let tcfg = net.cfg.transfer.clone();
    let ctx = Arc::new(Ctx {
        net,
        dc,
        plan,
        dir,
        path,
        progress: progress.clone(),
        done: (0..plan.parts).map(|_| AtomicBool::new(false)).collect(),
        actual_end: AtomicU64::new(0),
        requests: AtomicU64::new(0),
        request_micros: AtomicU64::new(0),
        flood_waits: AtomicU64::new(0),
        media_acquired: AtomicU64::new(0),
        regular_acquired: AtomicU64::new(0),
    });
    let started = Instant::now();
    let stall = Duration::from_secs(tcfg.stall_timeout_secs);
    let mut restarts = 0u32;

    loop {
        let remaining: VecDeque<u32> = (0..plan.parts)
            .filter(|&p| !ctx.done[p as usize].load(Ordering::Acquire))
            .collect();
        if remaining.is_empty() {
            break;
        }
        let workers = conns.min(remaining.len());
        let queue = Arc::new(Mutex::new(remaining));
        let mut set = JoinSet::new();
        for id in 0..workers {
            set.spawn(worker(id, ctx.clone(), queue.clone()));
        }

        let mut last_bytes = progress.bytes();
        let mut last_change = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let outcome: Result<bool> = loop {
            tokio::select! {
                r = set.join_next() => match r {
                    None => break Ok(true),
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(e))) => break Err(e),
                    Some(Err(e)) => break Err(anyhow!("worker panicked: {e}")),
                },
                _ = tick.tick() => {
                    let b = progress.bytes();
                    if b != last_bytes || progress.paused_until().is_some() || progress.holding() {
                        last_bytes = b;
                        last_change = Instant::now();
                    } else if last_change.elapsed() >= stall {
                        break Ok(false);
                    }
                }
            }
        };
        set.abort_all();
        while set.join_next().await.is_some() {}
        progress.reset_conns();

        match outcome {
            Ok(true) => restarts = 0,
            Ok(false) => {
                restarts += 1;
                warn!(
                    "DC{dc}: no progress for {}s, restarting all connections ({restarts}/{})",
                    stall.as_secs(),
                    tcfg.max_stall_restarts
                );
                if restarts > tcfg.max_stall_restarts {
                    bail!(
                        "傳輸連續停滯 {} 次（每次 {} 秒無進度），已放棄",
                        restarts - 1,
                        stall.as_secs()
                    );
                }
                progress.note(format!(
                    "偵測到傳輸停滯 {} 秒，正在重新建立所有連線（第 {restarts} 次）",
                    stall.as_secs()
                ));
            }
            Err(e) => return Err(e),
        }
    }

    let reqs = ctx.requests.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64().max(0.001);
    let is_download = matches!(ctx.dir, Direction::Download { .. });
    let media = if is_download {
        let m = ctx.media_acquired.load(Ordering::Relaxed);
        let r = ctx.regular_acquired.load(Ordering::Relaxed);
        format!(", media conns: {m}/{}", m + r)
    } else {
        String::new()
    };
    info!(
        "DC{dc} {}: {} in {secs:.1}s ({:.2} MB/s), {conns} conn(s), {reqs} requests (avg {:.0} ms), {} FLOOD_WAIT, separate auth: {}{media}",
        if is_download { "download" } else { "upload" },
        crate::status::human_bytes(plan.size),
        plan.size as f64 / secs / 1048576.0,
        ctx.request_micros.load(Ordering::Relaxed) as f64 / reqs.max(1) as f64 / 1000.0,
        ctx.flood_waits.load(Ordering::Relaxed),
        ctx.net.separate_keys(dc),
    );

    let end = ctx.actual_end.load(Ordering::Acquire);
    Ok(if end > 0 { end } else { plan.size })
}

/// One request in flight on a pipelined connection.
struct InFlight {
    part: u32,
    started: Instant,
    /// File-location generation used for this request (download only).
    generation: u64,
    rx: oneshot::Receiver<Result<Vec<u8>, InvocationError>>,
}

/// Pipelined worker: one connection keeping up to `pipeline_depth` requests in
/// flight (like Telegram Desktop, which keeps up to 16 x 128 KB outstanding per
/// session). With a single outstanding request, throughput is capped at
/// part_size / round-trip time per connection.
async fn worker(id: usize, ctx: Arc<Ctx>, queue: Arc<Mutex<VecDeque<u32>>>) -> Result<()> {
    let tcfg = &ctx.net.cfg.transfer;
    let req_timeout = Duration::from_secs(tcfg.request_timeout_secs);
    let depth = tcfg.pipeline_depth.max(1);
    let tag = format!("DC{}#{id}", ctx.dc);

    // Stagger connection setup to avoid a burst of handshakes.
    tokio::time::sleep(Duration::from_millis(id as u64 * 200) + jitter(tcfg.reconnect_jitter_ms))
        .await;

    let mut conn: Option<Conn> = None;
    let mut file: Option<File> = None;
    let mut fails = 0u32;
    let mut attempts: std::collections::HashMap<u32, u32> = Default::default();
    let mut inflight: Vec<InFlight> = Vec::with_capacity(depth);
    // Per-connection hold (FLOOD_WAIT on our own authorization, retry backoff):
    // no new requests are sent until then, in-flight ones still complete.
    let mut hold_until: Option<Instant> = None;
    let mut last_response = Instant::now();

    let requeue = |parts: &mut Vec<InFlight>| {
        let mut q = queue.lock().unwrap();
        for f in parts.drain(..).rev() {
            q.push_front(f.part);
        }
    };
    let drop_conn = |conn: &mut Option<Conn>| {
        if let Some(c) = conn.take() {
            ctx.progress.conn_dropped(c.via_media);
        }
    };

    let result: Result<()> = async {
        loop {
            if ctx.progress.paused_until().is_some() {
                // Job-wide pause (shared authorization). Requests may still be in
                // flight; do not count the pause as "no response from the server".
                ctx.progress.wait_pause().await;
                last_response = Instant::now();
            }

            // ---- connection ----
            if conn.is_none() {
                let conn_res = if ctx.is_home_download() {
                    ctx.net.take_download(ctx.dc, id).await
                } else {
                    ctx.net.take(ctx.dc, id).await
                };
                match conn_res {
                    Ok(c) => {
                        ctx.progress.conn_acquired(c.via_media);
                        let counter = if c.via_media { &ctx.media_acquired } else { &ctx.regular_acquired };
                        counter.fetch_add(1, Ordering::Relaxed);
                        conn = Some(c);
                        last_response = Instant::now();
                    }
                    Err(e) => {
                        fails += 1;
                        warn!("{tag}: connect failed ({fails}): {e:#}");
                        if let Some(OpenError(ie)) = e.downcast_ref::<OpenError>()
                            && matches!(classify_ref(ie), Some(true))
                        {
                            ctx.net.invalidate(ctx.dc, id).await;
                        }
                        if fails > tcfg.max_reconnects_per_worker {
                            bail!("無法連線到 DC{}：{e:#}", ctx.dc);
                        }
                        ctx.progress.note(format!("連線 DC{} 失敗，重試中（{fails}）", ctx.dc));
                        tokio::time::sleep(backoff(fails) + jitter(tcfg.reconnect_jitter_ms)).await;
                        continue;
                    }
                }
            }
            if file.is_none() {
                file = Some(open_file(&ctx).await?);
            }
            let sender = &mut conn.as_mut().unwrap().sender;
            let f = file.as_mut().unwrap();

            // ---- fill the window ----
            let now = Instant::now();
            let holding = hold_until.is_some_and(|t| t > now);
            if !holding {
                hold_until = None;
                while inflight.len() < depth {
                    let Some(part) = queue.lock().unwrap().pop_front() else {
                        break;
                    };
                    let (body, generation) = match build_request(&ctx, f, part).await {
                        Ok(x) => x,
                        Err(e) => {
                            queue.lock().unwrap().push_front(part);
                            return Err(e);
                        }
                    };
                    let (tx, rx) = oneshot::channel();
                    sender.enqueue_body(body, tx);
                    if inflight.is_empty() {
                        last_response = Instant::now();
                    }
                    inflight.push(InFlight {
                        part,
                        started: Instant::now(),
                        generation,
                        rx,
                    });
                }
            }
            if inflight.is_empty() {
                match hold_until {
                    Some(t) => {
                        tokio::time::sleep_until(t.into()).await;
                        continue;
                    }
                    None => return Ok(()), // queue drained, nothing outstanding
                }
            }

            // ---- drive I/O until something completes (or the connection stalls) ----
            // Stall if nothing arrived for req_timeout, or if a single request has
            // been outstanding for 2x req_timeout (a lost response).
            let oldest = inflight.iter().map(|f| f.started).min().unwrap_or(last_response);
            let deadline = (last_response + req_timeout).min(oldest + req_timeout * 2);
            let stepped = tokio::time::timeout_at(deadline.into(), sender.step()).await;
            let failure = match stepped {
                Err(_) => Some(format!(
                    "no response for {}s with {} request(s) in flight",
                    req_timeout.as_secs(),
                    inflight.len()
                )),
                Ok(Err(e)) => Some(format!("connection error: {e}")),
                Ok(Ok(_updates)) => None,
            };
            if let Some(msg) = failure {
                fails += 1;
                warn!("{tag}: {msg}; reconnecting and requeueing ({fails})");
                requeue(&mut inflight);
                drop_conn(&mut conn);
                if fails > tcfg.max_reconnects_per_worker {
                    bail!("DC{} 連線反覆中斷：{msg}", ctx.dc);
                }
                if msg.starts_with("no response") {
                    ctx.progress.note("請求逾時（連線無回應），已重新連線");
                }
                tokio::time::sleep(backoff(fails) + jitter(tcfg.reconnect_jitter_ms)).await;
                continue;
            }

            // ---- collect finished requests ----
            let mut i = 0;
            let mut reconnect: Option<String> = None;
            let mut flood_reconnect = false;
            while i < inflight.len() {
                let res = match inflight[i].rx.try_recv() {
                    Ok(r) => r,
                    Err(oneshot::error::TryRecvError::Empty) => {
                        i += 1;
                        continue;
                    }
                    Err(oneshot::error::TryRecvError::Closed) => {
                        Err(InvocationError::Dropped("request dropped by sender"))
                    }
                };
                let done = inflight.swap_remove(i);
                last_response = Instant::now();
                let outcome = match res {
                    Ok(body) => handle_response(&ctx, f, done.part, &body).await,
                    Err(e) => Err(classify(e)),
                };
                match outcome {
                    Ok(n) => {
                        ctx.requests.fetch_add(1, Ordering::Relaxed);
                        ctx.request_micros
                            .fetch_add(done.started.elapsed().as_micros() as u64, Ordering::Relaxed);
                        ctx.done[done.part as usize].store(true, Ordering::Release);
                        ctx.progress.add_bytes(n);
                        fails = 0;
                    }
                    Err(err) => {
                        queue.lock().unwrap().push_front(done.part);
                        match err {
                            PartErr::Flood(secs) => {
                                ctx.flood_waits.fetch_add(1, Ordering::Relaxed);
                                if secs > tcfg.max_flood_wait_secs {
                                    bail!("Telegram 要求等待 {secs} 秒（FLOOD_WAIT），超過上限");
                                }
                                debug!("{tag}: FLOOD_WAIT {secs}s");
                                if tcfg.flood_wait_strategy == PremiumWaitStrategy::Reconnect
                                    && matches!(ctx.dir, Direction::Download { .. })
                                {
                                    // New MTProto session on the same authorization, no wait.
                                    flood_reconnect = true;
                                    continue;
                                }
                                let until = Instant::now() + Duration::from_secs(secs) + jitter(300);
                                if ctx.net.separate_keys(ctx.dc) {
                                    // Own authorization -> own limit: only this connection holds.
                                    ctx.progress.note(format!(
                                        "部分連線觸發 Telegram 限流（FLOOD_WAIT {secs} 秒），其他連線持續傳輸中"
                                    ));
                                    hold_until = Some(hold_until.map_or(until, |t| t.max(until)));
                                    // Tell the watchdog this idle time is legitimate.
                                    ctx.progress.hold_hint(until);
                                } else {
                                    ctx.progress.pause_for(until - Instant::now());
                                }
                            }
                            PartErr::PremiumWait(secs) => {
                                debug!("{tag}: FLOOD_PREMIUM_WAIT {secs}s");
                                match tcfg.premium_wait_strategy {
                                    PremiumWaitStrategy::Reconnect => {
                                        ctx.progress.note("觸發非 Premium 速度限制，重建連線中");
                                        reconnect.get_or_insert_with(|| "FLOOD_PREMIUM_WAIT".into());
                                    }
                                    PremiumWaitStrategy::Wait => {
                                        hold_until = Some(Instant::now() + Duration::from_secs(secs.max(1)));
                                    }
                                }
                            }
                            PartErr::Reconnect(msg) => {
                                reconnect.get_or_insert(msg);
                            }
                            PartErr::Reauth(msg) => {
                                let a = attempts.entry(done.part).or_default();
                                *a += 1;
                                if *a > tcfg.max_part_retries {
                                    bail!("DC{} 授權失效且重試失敗：{msg}", ctx.dc);
                                }
                                warn!("{tag}: {msg}; re-importing authorization");
                                ctx.net.invalidate(ctx.dc, id).await;
                                reconnect.get_or_insert(msg);
                            }
                            PartErr::FileRef => {
                                let a = attempts.entry(done.part).or_default();
                                *a += 1;
                                if *a > tcfg.max_part_retries {
                                    bail!("檔案參照已過期且無法刷新");
                                }
                                if let Direction::Download { location, .. } = &ctx.dir {
                                    info!("{tag}: file reference expired, refreshing");
                                    location.refresh(done.generation).await?;
                                }
                            }
                            PartErr::Retry(msg) => {
                                let a = attempts.entry(done.part).or_default();
                                *a += 1;
                                warn!("{tag}: part {}: {msg} (attempt {a})", done.part);
                                if *a > tcfg.max_part_retries {
                                    bail!("分塊 {} 重試 {} 次仍失敗：{msg}", done.part, tcfg.max_part_retries);
                                }
                                hold_until = Some(Instant::now() + backoff(*a));
                            }
                            PartErr::Fatal(msg) => bail!("Telegram 回傳錯誤：{msg}"),
                        }
                    }
                }
            }
            if flood_reconnect && reconnect.is_none() {
                // Does not count as a failure: the connection itself is healthy.
                requeue(&mut inflight);
                drop_conn(&mut conn);
                tokio::time::sleep(jitter(tcfg.reconnect_jitter_ms.min(200))).await;
                continue;
            }
            if let Some(msg) = reconnect {
                fails += 1;
                warn!("{tag}: {msg}; reconnecting and requeueing ({fails})");
                requeue(&mut inflight);
                drop_conn(&mut conn);
                if fails > tcfg.max_reconnects_per_worker {
                    bail!("DC{} 連線反覆中斷：{msg}", ctx.dc);
                }
                tokio::time::sleep(backoff(fails) + jitter(tcfg.reconnect_jitter_ms)).await;
            }
        }
    }
    .await;

    if let Some(c) = conn {
        ctx.progress.conn_dropped(c.via_media);
        // Healthy connection after a clean finish (nothing in flight) -> keep it warm.
        // Home-DC download connections use the media address: not for the pool.
        if result.is_ok() && inflight.is_empty() && !ctx.is_home_download() {
            ctx.net.give_back(ctx.dc, id, c);
        }
    }
    result
}

/// `Some(true)` when the error means the auth key is not usable on that DC.
fn classify_ref(e: &InvocationError) -> Option<bool> {
    match e {
        InvocationError::Rpc(r) => Some(r.code == 401 || r.name.starts_with("AUTH_KEY")),
        InvocationError::Transport(transport::Error::BadStatus { status: 404 }) => Some(true),
        _ => None,
    }
}

fn backoff(n: u32) -> Duration {
    Duration::from_millis((250u64 << n.min(5)).min(8_000))
}

async fn open_file(ctx: &Ctx) -> Result<File> {
    Ok(match ctx.dir {
        Direction::Download { .. } => OpenOptions::new().write(true).open(&ctx.path).await?,
        Direction::Upload { .. } => File::open(&ctx.path).await?,
    })
}

/// Serialize the request for `part` (reads the data from disk for uploads).
async fn build_request(ctx: &Ctx, file: &mut File, part: u32) -> Result<(Vec<u8>, u64)> {
    let plan = ctx.plan;
    let offset = plan.offset(part);
    Ok(match &ctx.dir {
        Direction::Download { location, .. } => {
            let (generation, loc) = location.get();
            let req = tl::functions::upload::GetFile {
                precise: false,
                cdn_supported: false,
                location: loc,
                offset: offset as i64,
                limit: plan.part_size as i32,
            };
            (req.to_bytes(), generation)
        }
        Direction::Upload { file_id, big, base } => {
            let mut buf = vec![0u8; plan.len(part) as usize];
            file.seek(SeekFrom::Start(base + offset))
                .await
                .context("本機檔案錯誤")?;
            file.read_exact(&mut buf).await.context("本機檔案錯誤")?;
            let body = if *big {
                tl::functions::upload::SaveBigFilePart {
                    file_id: *file_id,
                    file_part: part as i32,
                    file_total_parts: plan.parts as i32,
                    bytes: buf,
                }
                .to_bytes()
            } else {
                tl::functions::upload::SaveFilePart {
                    file_id: *file_id,
                    file_part: part as i32,
                    bytes: buf,
                }
                .to_bytes()
            };
            (body, 0)
        }
    })
}

/// Validate a successful response for `part` (and write downloaded data).
async fn handle_response(
    ctx: &Ctx,
    file: &mut File,
    part: u32,
    body: &[u8],
) -> Result<u64, PartErr> {
    let plan = ctx.plan;
    let offset = plan.offset(part);
    let expected = plan.len(part);
    let io_err = |e: std::io::Error| PartErr::Fatal(format!("本機檔案錯誤：{e}"));
    let de_err = |e: tl::deserialize::Error| PartErr::Retry(format!("bad response: {e}"));

    match &ctx.dir {
        Direction::Download { exact_size, .. } => {
            let bytes = match tl::enums::upload::File::from_bytes(body).map_err(de_err)? {
                tl::enums::upload::File::File(f) => f.bytes,
                tl::enums::upload::File::CdnRedirect(_) => {
                    return Err(PartErr::Fatal("unexpected CDN redirect".into()));
                }
            };
            let got = bytes.len() as u64;
            let is_last = part + 1 == plan.parts;
            let ok = if *exact_size || !is_last {
                got == expected
            } else {
                got > 0 && got <= plan.part_size
            };
            if !ok {
                return Err(PartErr::Retry(format!(
                    "short response at offset {offset}: got {got}, expected {expected}"
                )));
            }
            file.seek(SeekFrom::Start(offset)).await.map_err(io_err)?;
            file.write_all(&bytes).await.map_err(io_err)?;
            // tokio's write_all returns before the blocking write lands; flush so
            // "part done" really means on disk and disk errors surface here.
            file.flush().await.map_err(io_err)?;
            if is_last {
                ctx.actual_end.store(offset + got, Ordering::Release);
            }
            Ok(got)
        }
        Direction::Upload { .. } => {
            if !bool::from_bytes(body).map_err(de_err)? {
                return Err(PartErr::Retry("server refused to store part".into()));
            }
            Ok(expected)
        }
    }
}

/// Re-upload a single part (used when sendMedia reports FILE_PART_X_MISSING).
pub async fn reupload_part(
    net: &Net,
    plan: Plan,
    file_id: i64,
    big: bool,
    base: u64,
    path: &std::path::Path,
    part: u32,
) -> Result<()> {
    let req_timeout = Duration::from_secs(net.cfg.transfer.request_timeout_secs);
    let mut sender = net.take(net.home_dc, 0).await?.sender;
    let mut f = File::open(path).await?;
    let mut buf = vec![0u8; plan.len(part) as usize];
    f.seek(SeekFrom::Start(base + plan.offset(part))).await?;
    f.read_exact(&mut buf).await?;
    let fut = async {
        if big {
            sender
                .invoke(&tl::functions::upload::SaveBigFilePart {
                    file_id,
                    file_part: part as i32,
                    file_total_parts: plan.parts as i32,
                    bytes: buf,
                })
                .await
        } else {
            sender
                .invoke(&tl::functions::upload::SaveFilePart {
                    file_id,
                    file_part: part as i32,
                    bytes: buf,
                })
                .await
        }
    };
    match timeout(req_timeout, fut).await {
        Ok(Ok(true)) => Ok(()),
        Ok(Ok(false)) => bail!("server refused part {part}"),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => bail!("re-upload of part {part} timed out"),
    }
}

/// Split `size` bytes into consecutive `(base, len)` pieces of at most `piece` bytes.
pub fn split_ranges(size: u64, piece: u64) -> Vec<(u64, u64)> {
    let piece = piece.max(1);
    if size <= piece {
        return vec![(0, size)];
    }
    (0..size.div_ceil(piece))
        .map(|i| {
            let base = i * piece;
            (base, (size - base).min(piece))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_4gb_file() {
        let piece = 2000 * MB;
        let size = 4 * 1024 * MB; // 4 GiB
        let r = split_ranges(size, piece);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0], (0, piece));
        assert_eq!(r[1], (piece, piece));
        assert_eq!(r[2], (2 * piece, size - 2 * piece));
        assert_eq!(r.iter().map(|x| x.1).sum::<u64>(), size);
        for (_, len) in r {
            assert!(Plan::new(len, UPLOAD_PART).parts as u64 <= MAX_UPLOAD_PARTS);
        }
    }

    #[test]
    fn split_not_needed() {
        assert_eq!(split_ranges(2000 * MB, 2000 * MB), vec![(0, 2000 * MB)]);
        assert_eq!(split_ranges(5, 2000 * MB), vec![(0, 5)]);
    }

    const MB: u64 = 1024 * 1024;

    #[test]
    fn plan_exact_multiple() {
        let p = Plan::new(4 * MB, MB);
        assert_eq!(p.parts, 4);
        assert_eq!(p.len(3), MB);
    }

    #[test]
    fn plan_remainder() {
        let p = Plan::new(4 * MB + 123, MB);
        assert_eq!(p.parts, 5);
        assert_eq!(p.offset(4), 4 * MB);
        assert_eq!(p.len(4), 123);
        assert_eq!((0..p.parts).map(|i| p.len(i)).sum::<u64>(), p.size);
    }

    #[test]
    fn plan_small_and_empty() {
        assert_eq!(Plan::new(10, UPLOAD_PART).parts, 1);
        assert_eq!(Plan::new(10, UPLOAD_PART).len(0), 10);
        assert_eq!(Plan::new(0, UPLOAD_PART).parts, 1);
    }

    #[test]
    fn download_requests_never_cross_1mb_boundary() {
        // GetFile rules: offset % 4096 == 0, limit % 4096 == 0, 1MB % limit == 0,
        // and offset / 1MB == (offset + limit - 1) / 1MB.
        for kb in [4u64, 64, 128, 256, 512] {
            let p = Plan::new(37 * MB + 4321, kb * 1024);
            for i in 0..p.parts {
                let off = p.offset(i);
                let lim = p.part_size;
                assert_eq!(off % 4096, 0);
                assert_eq!(lim % 4096, 0);
                assert_eq!(MB % lim, 0);
                assert_eq!(off / MB, (off + lim - 1) / MB);
            }
        }
    }

    #[test]
    fn upload_limit() {
        let max = MAX_UPLOAD_PARTS * UPLOAD_PART;
        assert_eq!(Plan::new(max, UPLOAD_PART).parts as u64, MAX_UPLOAD_PARTS);
        assert_eq!(
            Plan::new(max + 1, UPLOAD_PART).parts as u64,
            MAX_UPLOAD_PARTS + 1
        );
    }
}
