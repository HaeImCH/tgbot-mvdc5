//! Raw MTProto connections to arbitrary DCs.
//!
//! grammers' `SenderPool` keeps exactly one socket per DC and multiplexes every
//! request on it, so "4 connections" there is really 4 tasks on 1 socket. To get
//! real parallel connections we open our own `Sender`s here:
//!
//! * foreign DC: generate one auth key, `auth.exportAuthorization` on the home DC,
//!   `auth.importAuthorization` on the new key; then N connections reuse that key
//!   (each `Sender` gets a fresh random session_id).
//! * home DC: reuse the bot's authorized home key from the session.
//! * transfer connections to a foreign DC go to that DC's `media_only` address
//!   from `help.getConfig` (Telegram Desktop's "media cluster"), not the regular
//!   API front-end: the front-end answers pipelined upload.getFile with constant
//!   FLOOD_WAIT (~13 MB/s on DC4), the media cluster does not (~52 MB/s).
//!   Keys are DC-wide, so they are still created/imported on the regular address
//!   (the media cluster does not answer the key exchange).
//!
//! Every new connection must start with `invokeWithLayer(initConnection(..))`.
//! All socket operations are wrapped in timeouts: grammers has no read timeout and a
//! half-open SOCKS5 socket would otherwise hang forever (silent stall).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use grammers_client::Client;
use grammers_mtproto::{mtp, transport, transport::Full};
use grammers_mtsender::{
    InvocationError, ReadError, Sender, ServerAddr, connect, connect_with_auth,
};
use grammers_session::Session;
use grammers_session::storages::SqliteSession;
use grammers_tl_types as tl;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::Config;

pub type RawSender = Sender<Full, mtp::Encrypted>;

/// A transfer connection, tagged with whether it was opened on the DC's
/// media-only address. The tag is taken from the address actually dialed, so it
/// stays correct for warm connections opened before the media list was loaded
/// or before the media address was marked broken.
pub struct Conn {
    pub sender: RawSender,
    pub via_media: bool,
}

pub struct Net {
    pub client: Client,
    pub session: Arc<SqliteSession>,
    pub cfg: Arc<Config>,
    pub home_dc: i32,
    /// Our own user id (bot or user account).
    pub me_id: i64,
    /// Upload limit in 512 KB parts: 4000 normally, 8000 for Premium users.
    pub max_upload_parts: u64,
    /// (dc_id, slot) -> authorized auth key. With `auth_per_connection` every
    /// connection slot to a foreign DC has its own authorization: Telegram's
    /// getFile rate limit (FLOOD_WAIT) is per authorization, so one shared key
    /// throttles all connections at once. The mutex also serializes
    /// export/import (concurrent exports cause AUTH_BYTES_INVALID, see telebackup).
    keys: Mutex<HashMap<(i32, usize), [u8; 256]>>,
    /// Warm pool: idle, initialized connections per DC, tagged with their slot.
    idle: std::sync::Mutex<HashMap<i32, Vec<(usize, Conn)>>>,
    /// dc_id -> media-only address (from help.getConfig), see module docs.
    media: std::sync::RwLock<HashMap<i32, SocketAddr>>,
    /// DCs whose media address failed while the regular one worked; they use
    /// the regular address until the cooldown ends.
    media_broken: std::sync::Mutex<MediaBackoff>,
    /// Per-DC cooldown after a transport-level -429, see [`Cooldowns`].
    cooldowns: std::sync::Mutex<Cooldowns>,
}

impl Net {
    pub fn new(
        client: Client,
        session: Arc<SqliteSession>,
        cfg: Arc<Config>,
        home_dc: i32,
        me_id: i64,
        max_upload_parts: u64,
    ) -> Self {
        Self {
            client,
            session,
            cfg,
            home_dc,
            me_id,
            max_upload_parts,
            keys: Mutex::new(HashMap::new()),
            idle: std::sync::Mutex::new(HashMap::new()),
            media: std::sync::RwLock::new(HashMap::new()),
            media_broken: std::sync::Mutex::new(MediaBackoff::new(MEDIA_RETRY_AFTER)),
            cooldowns: std::sync::Mutex::new(Cooldowns::default()),
        }
    }

    /// Record a transport -429 ("too many connections from this IP") on `dc_id`.
    /// Returns when new connections to that DC may be opened again.
    pub fn transport_flood(&self, dc_id: i32) -> Instant {
        let now = Instant::now();
        let (until, new_episode) = self.cooldowns.lock().unwrap().hit(dc_id, now);
        if new_episode {
            warn!(
                "DC{dc_id}: transport flood (-429, too many connections from this IP); no new connections for {}s",
                (until - now).as_secs()
            );
        }
        until
    }

    /// Wait out any cooldown of `dc_id`. Called right before every dial, so
    /// every path (workers, warm pool, key creation, media fallback) obeys it.
    /// Rechecks after waking (a new cooldown may have started) and adds jitter
    /// so waiting dials do not all fire at the same instant.
    async fn wait_cooldown(&self, dc_id: i32) {
        while let Some(t) = self.cooldown_until(dc_id) {
            let spread = Duration::from_millis(rand::random::<u64>() % 1000);
            tokio::time::sleep_until((t + spread).into()).await;
        }
    }

    /// While `dc_id` cools down after a -429: until when.
    pub fn cooldown_until(&self, dc_id: i32) -> Option<Instant> {
        self.cooldowns.lock().unwrap().until(dc_id, Instant::now())
    }

    /// Fetch the DC list and remember each DC's media-only address. Failure is
    /// not fatal: transfers then use the regular addresses.
    pub async fn load_media_dcs(&self) {
        if !self.cfg.transfer.use_media_dc {
            return;
        }
        let conf = match timeout(
            self.connect_timeout(),
            self.client.invoke(&tl::functions::help::GetConfig {}),
        )
        .await
        {
            Ok(Ok(tl::enums::Config::Config(c))) => c,
            Ok(Err(e)) => return warn!("help.getConfig failed ({e}); using regular DC addresses"),
            Err(_) => return warn!("help.getConfig timed out; using regular DC addresses"),
        };
        let mut found = HashMap::new();
        for tl::enums::DcOption::Option(o) in conf.dc_options {
            if !o.media_only
                || o.cdn
                || o.tcpo_only
                || o.ipv6 != self.cfg.use_ipv6
                || found.contains_key(&o.id)
            {
                continue;
            }
            let ip: std::net::IpAddr = match o.ip_address.parse() {
                Ok(ip) => ip,
                Err(_) => continue,
            };
            found.insert(o.id, SocketAddr::new(ip, o.port as u16));
        }
        let mut list: Vec<_> = found.iter().map(|(dc, a)| format!("DC{dc}={a}")).collect();
        list.sort();
        info!(
            "media DC addresses: {}",
            if list.is_empty() {
                "none".into()
            } else {
                list.join(", ")
            }
        );
        *self.media.write().unwrap() = found;
    }

    /// Media address for transfer connections. The home DC only uses it for
    /// downloads (`reupload_same_dc`); uploads go to the regular address like
    /// the official clients do.
    fn media_addr(&self, dc_id: i32, download: bool) -> Option<SocketAddr> {
        if !wants_media(self.home_dc, dc_id, download) {
            return None;
        }
        let addr = self.media.read().unwrap().get(&dc_id).copied()?;
        match self
            .media_broken
            .lock()
            .unwrap()
            .state(dc_id, Instant::now())
        {
            MediaState::Usable => Some(addr),
            MediaState::Retry => {
                info!("DC{dc_id}: retrying the media address");
                Some(addr)
            }
            MediaState::Blocked => None,
        }
    }

    /// A warm connection on the regular address while the media address is
    /// usable again: drop it so a new one opens on the media address.
    fn stale(&self, dc_id: i32, conn: &Conn) -> bool {
        is_stale(conn.via_media, || self.media_addr(dc_id, false).is_some())
    }

    fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.cfg.transfer.connect_timeout_secs)
    }

    /// Address to dial, and whether it is the DC's media-only address (`media`
    /// is only a preference: DCs without a known media address use the regular one).
    fn server_addr(&self, dc_id: i32, media: bool) -> Result<(ServerAddr, bool)> {
        let opt = self
            .session
            .dc_option(dc_id)
            .map_err(|e| anyhow!("session error: {e}"))?
            .ok_or_else(|| anyhow!("未知的 DC{dc_id}"))?;
        let regular: SocketAddr = if self.cfg.use_ipv6 {
            opt.ipv6.into()
        } else {
            opt.ipv4.into()
        };
        let media_address = media
            .then(|| self.media.read().unwrap().get(&dc_id).copied())
            .flatten();
        let address = media_address.unwrap_or(regular);
        let addr = match &self.cfg.proxy {
            Some(proxy) => ServerAddr::Proxied {
                address,
                proxy: proxy.clone(),
            },
            None => ServerAddr::Tcp { address },
        };
        Ok((addr, media_address.is_some()))
    }

    fn init<Q>(
        &self,
        query: Q,
    ) -> tl::functions::InvokeWithLayer<tl::functions::InitConnection<Q>> {
        tl::functions::InvokeWithLayer {
            layer: tl::LAYER,
            query: tl::functions::InitConnection {
                api_id: self.cfg.api_id,
                device_model: "tgbot_mvdc5".into(),
                system_version: std::env::consts::OS.into(),
                app_version: env!("CARGO_PKG_VERSION").into(),
                system_lang_code: "zh-TW".into(),
                lang_pack: "".into(),
                lang_code: "zh-hant".into(),
                proxy: None,
                params: None,
                query,
            },
        }
    }

    /// Whether connections to `dc_id` each have their own authorization.
    pub fn separate_keys(&self, dc_id: i32) -> bool {
        dc_id != self.home_dc && self.cfg.transfer.auth_per_connection
    }

    fn key_slot(&self, dc_id: i32, slot: usize) -> usize {
        if self.separate_keys(dc_id) { slot } else { 0 }
    }

    /// Authorized auth key for (`dc_id`, `slot`), creating & importing one if needed.
    /// The home DC always uses the account's own key from the session.
    pub async fn auth_key(&self, dc_id: i32, slot: usize) -> Result<[u8; 256]> {
        if dc_id == self.home_dc {
            return self
                .session
                .dc_option(dc_id)
                .map_err(|e| anyhow!("session error: {e}"))?
                .and_then(|o| o.auth_key)
                .ok_or_else(|| anyhow!("home DC{dc_id} has no auth key in session"));
        }
        let slot = self.key_slot(dc_id, slot);

        let mut keys = self.keys.lock().await;
        if let Some(k) = keys.get(&(dc_id, slot)) {
            return Ok(*k);
        }

        let mut last_err = None;
        for attempt in 1..=4u64 {
            // `while`: a new cooldown may start while waiting for the lock again.
            while self.cooldown_until(dc_id).is_some() {
                // Wait outside the key lock (it is shared by every DC) and
                // outside the timeout (a cooldown can be longer than it).
                drop(keys);
                self.wait_cooldown(dc_id).await;
                keys = self.keys.lock().await;
                if let Some(k) = keys.get(&(dc_id, slot)) {
                    return Ok(*k);
                }
            }
            match timeout(
                self.connect_timeout() * 2,
                self.create_authorized_key(dc_id),
            )
            .await
            {
                Ok(Ok(key)) => {
                    info!("DC{dc_id}: authorization imported (slot {slot})");
                    keys.insert((dc_id, slot), key);
                    return Ok(key);
                }
                Ok(Err(e)) => {
                    warn!(
                        "DC{dc_id}: export/import authorization failed (slot {slot}, attempt {attempt}): {e:#}"
                    );
                    // Respect FLOOD_WAIT on exportAuthorization instead of hammering.
                    let flood = e
                        .chain()
                        .find_map(|c| match c.downcast_ref::<InvocationError>() {
                            Some(InvocationError::Rpc(r)) if r.code == 420 => r.value,
                            _ => None,
                        });
                    // -429 on the target DC (export errors carry no transport
                    // status, see create_authorized_key): retrying only adds
                    // connections. Recorded here so direct callers count too.
                    if open_error_status(&e) == Some(429) {
                        self.transport_flood(dc_id);
                        return Err(e);
                    }
                    last_err = Some(e);
                    if let Some(secs) = flood {
                        if secs > 120 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(secs as u64 + 1)).await;
                        continue;
                    }
                }
                Err(_) => {
                    warn!(
                        "DC{dc_id}: export/import authorization timed out (slot {slot}, attempt {attempt})"
                    );
                    last_err = Some(anyhow!("取得授權逾時"));
                }
            }
            tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
        }
        Err(last_err.unwrap_or_else(|| anyhow!("取得授權失敗")))
    }

    async fn create_authorized_key(&self, dc_id: i32) -> Result<[u8; 256]> {
        let (addr, _) = self.server_addr(dc_id, false)?;
        let mut sender = connect(Full::new(), addr)
            .await
            .context("產生授權金鑰失敗")?;

        let tl::enums::auth::ExportedAuthorization::Authorization(exported) = timeout(
            self.connect_timeout(),
            self.client
                .invoke(&tl::functions::auth::ExportAuthorization { dc_id }),
        )
        .await
        .context("匯出授權逾時")?
        .map_err(|e| match transport_status(&e) {
            // Export runs on the home DC: its transport error must not start a
            // cooldown for `dc_id`, so drop the typed error.
            Some(status) => anyhow!("匯出授權失敗：home DC transport error -{status}"),
            None => anyhow::Error::new(e).context("匯出授權失敗"),
        })?;

        sender
            .invoke(&self.init(tl::functions::InvokeWithoutUpdates {
                query: tl::functions::auth::ImportAuthorization {
                    id: exported.id,
                    bytes: exported.bytes,
                },
            }))
            .await
            .context("匯入授權失敗")?;
        Ok(sender.auth_key())
    }

    /// Forget a key that the server reported as unregistered (and every warm
    /// connection built on it).
    pub async fn invalidate(&self, dc_id: i32, slot: usize) {
        if dc_id == self.home_dc {
            return;
        }
        let slot = self.key_slot(dc_id, slot);
        self.keys.lock().await.remove(&(dc_id, slot));
        let separate = self.separate_keys(dc_id);
        if let Some(v) = self.idle.lock().unwrap().get_mut(&dc_id) {
            v.retain(|(s, _)| separate && *s != slot);
        }
    }

    /// Get a connection for `slot`: the warm idle one of that slot if available
    /// (so each worker keeps its own authorization), otherwise open a new one.
    pub async fn take(&self, dc_id: i32, slot: usize) -> Result<Conn> {
        let warm = self.idle.lock().unwrap().get_mut(&dc_id).and_then(|v| {
            let i = v.iter().position(|(s, _)| *s == slot)?;
            Some(v.swap_remove(i).1)
        });
        match warm {
            Some(s) if !self.stale(dc_id, &s) => Ok(s),
            _ => self.open(dc_id, slot).await,
        }
    }

    /// Return a healthy connection to the warm pool (one idle connection per slot).
    pub fn give_back(&self, dc_id: i32, slot: usize, conn: Conn) {
        if !self.cfg.warm.enabled || slot >= self.cfg.warm.connections_per_dc {
            return;
        }
        let mut idle = self.idle.lock().unwrap();
        let v = idle.entry(dc_id).or_default();
        if !v.iter().any(|(s, _)| *s == slot) {
            v.push((slot, conn));
        }
    }

    fn idle_has(&self, dc_id: i32, slot: usize) -> bool {
        self.idle
            .lock()
            .unwrap()
            .get(&dc_id)
            .is_some_and(|v| v.iter().any(|(s, _)| *s == slot))
    }

    /// Keep the warm pool alive: ping idle connections (MTProto ping, no update
    /// subscription), drop dead ones (half-open sockets fail the ping timeout),
    /// and top each configured DC back up to `connections_per_dc`.
    pub async fn keepalive_loop(self: Arc<Self>) {
        let w = self.cfg.warm.clone();
        let ping_timeout = Duration::from_secs(10);
        let mut first = true;
        loop {
            if !first {
                tokio::time::sleep(Duration::from_secs(w.keepalive_secs)).await;
            }
            let mut dcs: Vec<i32> = w.dcs.clone();
            dcs.extend(self.idle.lock().unwrap().keys().copied());
            dcs.sort_unstable();
            dcs.dedup();

            for dc in dcs {
                // Ping idle connections concurrently.
                let taken = self.idle.lock().unwrap().remove(&dc).unwrap_or_default();
                let mut set = tokio::task::JoinSet::new();
                for (slot, mut s) in taken {
                    set.spawn(async move {
                        let ping = tl::functions::Ping {
                            ping_id: rand::random(),
                        };
                        match timeout(ping_timeout, s.sender.invoke(&ping)).await {
                            Ok(Ok(_)) => Ok((slot, s)),
                            Ok(Err(e)) => Err(transport_status(&e)),
                            Err(_) => Err(None),
                        }
                    });
                }
                let (mut dead, mut stale) = (0, 0);
                while let Some(r) = set.join_next().await {
                    match r {
                        Ok(Ok((_, s))) if self.stale(dc, &s) => stale += 1,
                        Ok(Ok((slot, s))) => self.give_back(dc, slot, s),
                        Ok(Err(Some(429))) => {
                            self.transport_flood(dc);
                            dead += 1;
                        }
                        _ => dead += 1,
                    }
                }
                if dead > 0 {
                    warn!("warm pool DC{dc}: dropped {dead} dead connection(s)");
                }
                if stale > 0 {
                    info!("warm pool DC{dc}: reopening {stale} connection(s) on the media address");
                }

                // Refill (not while the DC cools down after a -429: new
                // connections are what triggers it).
                if !w.dcs.contains(&dc) || self.cooldown_until(dc).is_some() {
                    continue;
                }
                let mut opened = 0;
                for slot in 0..w.connections_per_dc {
                    if self.cooldown_until(dc).is_some() {
                        break;
                    }
                    if self.idle_has(dc, slot) {
                        continue;
                    }
                    match self.open(dc, slot).await {
                        Ok(s) => {
                            self.give_back(dc, slot, s);
                            opened += 1;
                        }
                        Err(e) => {
                            warn!("warm pool DC{dc}: cannot open connection: {e:#}");
                            if open_error_status(&e) == Some(429) {
                                self.transport_flood(dc);
                            }
                            break;
                        }
                    }
                }
                if first && opened > 0 {
                    info!("warm pool DC{dc}: {opened} connection(s) ready");
                }
            }
            first = false;
        }
    }

    /// Open a new initialized connection to `dc_id` (bounded by connect timeout).
    /// Uses the DC's media address when known. If that fails and the regular
    /// address works, this connection uses the regular one, and the DC is
    /// blocked from media for a while only when no media connection opened
    /// within the last minute (see `MediaBackoff::mark`).
    pub async fn open(&self, dc_id: i32, slot: usize) -> Result<Conn> {
        self.open_conn(dc_id, slot, false).await
    }

    /// Like `take`, for download workers. Downloads from the home DC
    /// (`reupload_same_dc`) bypass the warm pool, which holds regular-address
    /// connections for uploads, and use the media address instead.
    pub async fn take_download(&self, dc_id: i32, slot: usize) -> Result<Conn> {
        if dc_id == self.home_dc {
            self.open_conn(dc_id, slot, true).await
        } else {
            self.take(dc_id, slot).await
        }
    }

    async fn open_conn(&self, dc_id: i32, slot: usize, download: bool) -> Result<Conn> {
        let key = self.auth_key(dc_id, slot).await?;
        let via_media = self.media_addr(dc_id, download).is_some();
        match self.open_at(dc_id, key, via_media).await {
            Ok(conn) => {
                if conn.via_media {
                    // Proof that the media address works (an authenticated
                    // round trip on the address actually dialed).
                    let mut backoff = self.media_broken.lock().unwrap();
                    backoff.ok(dc_id, Instant::now());
                }
                Ok(conn)
            }
            Err(e) if retry_on_regular(via_media, &e) => {
                let regular = self.open_at(dc_id, key, false).await;
                // Only blame the media address when the regular one works; if
                // both fail, the network is down and media stays preferred.
                if regular.is_ok() {
                    let outcome = {
                        let mut backoff = self.media_broken.lock().unwrap();
                        backoff.mark(dc_id, Instant::now())
                    };
                    match outcome {
                        MarkOutcome::Blocked { last_ok_ago } => warn!(
                            "DC{dc_id}: media address failed ({e:#}); using the regular address for {}s (last media success: {})",
                            MEDIA_RETRY_AFTER.as_secs(),
                            last_ok_text(last_ok_ago)
                        ),
                        MarkOutcome::KeptByProof { ago } => debug!(
                            "DC{dc_id}: media address failed ({e:#}) but worked {}s ago; keeping it",
                            ago.as_secs()
                        ),
                        MarkOutcome::AlreadyBlocked => {}
                    }
                }
                regular
            }
            r => r,
        }
    }

    async fn open_at(&self, dc_id: i32, key: [u8; 256], media: bool) -> Result<Conn> {
        let (addr, via_media) = self.server_addr(dc_id, media)?;
        // Outside the connect timeout: a cooldown can be longer than it.
        self.wait_cooldown(dc_id).await;
        let fut = async {
            let mut sender = connect_with_auth(Full::new(), addr, key).await?;
            // InvokeWithoutUpdates: these raw senders discard updates, so they must
            // not subscribe the session (otherwise user messages could be lost).
            sender
                .invoke(&self.init(tl::functions::InvokeWithoutUpdates {
                    query: tl::functions::help::GetConfig {},
                }))
                .await?;
            Ok::<_, InvocationError>(sender)
        };
        match timeout(self.connect_timeout(), fut).await {
            Ok(Ok(sender)) => Ok(Conn { sender, via_media }),
            Ok(Err(e)) => {
                if transport_status(&e) == Some(429) {
                    self.transport_flood(dc_id);
                }
                Err(anyhow::Error::new(OpenError(e)))
            }
            Err(_) => bail!("連線到 DC{dc_id} 逾時"),
        }
    }
}

/// Wrapper so callers can downcast and inspect the underlying invocation error
/// (e.g. AUTH_KEY_UNREGISTERED -> invalidate key).
#[derive(Debug)]
pub struct OpenError(pub InvocationError);

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for OpenError {}
impl OpenError {
    /// The server answered with an RPC error, i.e. the address itself works.
    pub fn is_rpc(&self) -> bool {
        matches!(self.0, InvocationError::Rpc(_))
    }
}

/// Whether a connection to `dc_id` should go to the media address. The home
/// DC only uses it for downloads; uploads go to the regular address like the
/// official clients do.
fn wants_media(home_dc: i32, dc_id: i32, download: bool) -> bool {
    dc_id != home_dc || download
}

/// A warm connection is stale when it is on the regular address while the
/// media address is usable. `media_usable` is only asked for regular ones.
fn is_stale(via_media: bool, media_usable: impl FnOnce() -> bool) -> bool {
    !via_media && media_usable()
}

/// After failing to open on the media address, try the regular one, unless
/// the server answered with an RPC error (the address itself works then) or
/// with a transport -429 (about our IP, not the address: dialing the regular
/// address right away would add to the flood and mark media broken).
fn retry_on_regular(via_media: bool, err: &anyhow::Error) -> bool {
    via_media
        && open_error_status(err) != Some(429)
        && err.downcast_ref::<OpenError>().is_none_or(|o| !o.is_rpc())
}

/// How long a DC stays on its regular address after its media address failed.
const MEDIA_RETRY_AFTER: Duration = Duration::from_secs(300);

/// A media connection that opened this recently proves the media address
/// works, so a failed connect is not blamed on it (SPEC-media-fallback-proof.md).
/// Since 2026-11-20 DC5 drops connections after 1-2.5 s; sooner or later a drop
/// hits a handshake, and one such failure used to block media for 300 s.
const MEDIA_PROOF_WINDOW: Duration = Duration::from_secs(60);

/// What [`MediaBackoff::mark`] did.
#[derive(Debug, PartialEq, Eq)]
enum MarkOutcome {
    /// A new block started. How long ago media last opened (`None`: never).
    Blocked { last_ok_ago: Option<Duration> },
    /// A block is already running; it is not extended.
    AlreadyBlocked,
    /// Media opened `ago` ago (within [`MEDIA_PROOF_WINDOW`]): no block.
    KeptByProof { ago: Duration },
}

/// "75s ago" / "never", for the block log line.
fn last_ok_text(ago: Option<Duration>) -> String {
    ago.map_or_else(|| "never".into(), |a| format!("{}s ago", a.as_secs()))
}

#[derive(Debug, PartialEq, Eq)]
enum MediaState {
    /// Not marked: use the media address.
    Usable,
    /// Marked and cooling down: use the regular address.
    Blocked,
    /// The cooldown just ended (reported once): use the media address again.
    Retry,
}

/// Per-DC cooldown after a media address failure. A failure is often a
/// passing network problem, so the mark expires instead of lasting until
/// restart.
///
/// Both maps live behind one mutex and callers sample `now` while holding it,
/// so `ok` and `mark` take effect in lock order: a `mark` before a concurrent
/// `ok` blocks for the full cooldown, a later `ok` does not end it.
struct MediaBackoff {
    cooldown: Duration,
    until: HashMap<i32, Instant>,
    /// When a media connection to the DC last opened successfully.
    last_ok: HashMap<i32, Instant>,
}

impl MediaBackoff {
    fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            until: HashMap::new(),
            last_ok: HashMap::new(),
        }
    }

    /// A media connection to `dc` opened at `now`.
    fn ok(&mut self, dc: i32, now: Instant) {
        self.last_ok.insert(dc, now);
    }

    /// A media connect to `dc` failed at `now` while the regular address
    /// worked. Blocks media unless it is already blocked or opened within
    /// [`MEDIA_PROOF_WINDOW`].
    fn mark(&mut self, dc: i32, now: Instant) -> MarkOutcome {
        if self.until.get(&dc).is_some_and(|&t| now < t) {
            return MarkOutcome::AlreadyBlocked;
        }
        let last_ok_ago = self
            .last_ok
            .get(&dc)
            .map(|&t| now.saturating_duration_since(t));
        if let Some(ago) = last_ok_ago
            && ago < MEDIA_PROOF_WINDOW
        {
            return MarkOutcome::KeptByProof { ago };
        }
        self.until.insert(dc, now + self.cooldown);
        MarkOutcome::Blocked { last_ok_ago }
    }

    fn state(&mut self, dc: i32, now: Instant) -> MediaState {
        match self.until.get(&dc) {
            None => MediaState::Usable,
            Some(&t) if now < t => MediaState::Blocked,
            Some(_) => {
                self.until.remove(&dc);
                MediaState::Retry
            }
        }
    }
}

/// Status of a transport-level error (negative length from the server: 404 =
/// auth key unknown, 429 = too many connections from this IP).
///
/// grammers reports it as `Deserialize(Transport(BadStatus))`, both from
/// `Sender::step` and on every pending request; `InvocationError::Transport`
/// is only used by other code paths, so both shapes are checked.
pub fn transport_status(e: &InvocationError) -> Option<u32> {
    match e {
        InvocationError::Transport(t)
        | InvocationError::Deserialize(mtp::DeserializeError::Transport(t)) => bad_status(t),
        _ => None,
    }
}

/// [`transport_status`] for an error returned by `Sender::step`.
pub fn read_status(e: &ReadError) -> Option<u32> {
    match e {
        ReadError::Deserialize(mtp::DeserializeError::Transport(t)) => bad_status(t),
        _ => None,
    }
}

/// [`transport_status`] for an error from [`Net::open`] / [`Net::take`].
/// Also finds an `InvocationError` wrapped with `.context(..)` (key creation).
pub fn open_error_status(e: &anyhow::Error) -> Option<u32> {
    e.chain().find_map(|c| {
        c.downcast_ref::<OpenError>()
            .map(|o| &o.0)
            .or_else(|| c.downcast_ref::<InvocationError>())
            .and_then(transport_status)
    })
}

fn bad_status(t: &transport::Error) -> Option<u32> {
    match t {
        transport::Error::BadStatus { status } => Some(*status),
        _ => None,
    }
}

/// How long no new connection to a DC is opened after a -429.
///
/// Fixed and short on purpose. A prod bench (2026-11-21, DC5) showed the -429s
/// follow connections the server drops after 1-2.5 s (`read 0 bytes`), not our
/// reconnect rate: after a 60 s pause the first new connections died the same
/// way. Escalating cooldowns (2 s up to 60 s) then only idled the workers and
/// cut throughput from 11.6 to 7.0 MB/s. Two seconds still spreads out the
/// reconnects of all workers, jobs and the warm pool.
const COOLDOWN: Duration = Duration::from_secs(2);

/// Per-DC cooldowns after a transport -429 (too many connections from this
/// IP). During a cooldown no new connection to that DC is opened; existing ones
/// keep working.
#[derive(Default)]
pub struct Cooldowns {
    until: HashMap<i32, Instant>,
}

impl Cooldowns {
    /// Record a -429 at `now`. Returns the end of the cooldown and whether this
    /// started a new one. Hits during a running cooldown (the other connections
    /// reporting the same flood) do not extend it.
    pub fn hit(&mut self, dc_id: i32, now: Instant) -> (Instant, bool) {
        if let Some(t) = self.until(dc_id, now) {
            return (t, false);
        }
        let until = now + COOLDOWN;
        self.until.insert(dc_id, until);
        (until, true)
    }

    /// End of the running cooldown of `dc_id`, if any.
    pub fn until(&self, dc_id: i32, now: Instant) -> Option<Instant> {
        self.until.get(&dc_id).copied().filter(|t| *t > now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COOL: Duration = Duration::from_secs(300);

    #[test]
    fn home_dc_uses_media_only_for_downloads() {
        assert!(!wants_media(4, 4, false));
        assert!(wants_media(4, 4, true));
        assert!(wants_media(4, 2, false));
        assert!(wants_media(4, 2, true));
    }

    #[test]
    fn only_regular_conns_with_usable_media_are_stale() {
        assert!(is_stale(false, || true));
        assert!(!is_stale(false, || false));
        assert!(!is_stale(true, || panic!("not asked for media conns")));
    }

    fn rpc_error() -> anyhow::Error {
        let rpc = grammers_mtsender::RpcError::from(tl::types::RpcError {
            error_code: 401,
            error_message: "AUTH_KEY_UNREGISTERED".into(),
        });
        anyhow::Error::new(OpenError(InvocationError::Rpc(rpc)))
    }

    fn io_error() -> anyhow::Error {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        anyhow::Error::new(OpenError(InvocationError::Session(Box::new(io))))
    }

    #[test]
    fn media_failure_retries_on_regular_unless_rpc() {
        assert!(retry_on_regular(true, &io_error()));
        assert!(retry_on_regular(true, &anyhow!("連線到 DC4 逾時")));
        assert!(!retry_on_regular(true, &rpc_error()));
        assert!(!retry_on_regular(false, &io_error()));
        assert!(!retry_on_regular(false, &anyhow!("timeout")));
        let flood = anyhow::Error::new(OpenError(InvocationError::Deserialize(
            mtp::DeserializeError::Transport(transport::Error::BadStatus { status: 429 }),
        )));
        assert!(!retry_on_regular(true, &flood));
    }

    #[test]
    fn unmarked_dc_is_usable() {
        let mut b = MediaBackoff::new(COOL);
        assert_eq!(b.state(4, Instant::now()), MediaState::Usable);
    }

    #[test]
    fn marked_dc_is_blocked_until_cooldown_ends() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        assert_eq!(b.mark(4, t0), MarkOutcome::Blocked { last_ok_ago: None });
        assert_eq!(b.state(4, t0), MediaState::Blocked);
        assert_eq!(
            b.state(4, t0 + COOL - Duration::from_secs(1)),
            MediaState::Blocked
        );
        assert_eq!(b.state(2, t0), MediaState::Usable);
    }

    #[test]
    fn expired_mark_reports_retry_once_then_usable() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        b.mark(4, t0);
        assert_eq!(b.state(4, t0 + COOL), MediaState::Retry);
        assert_eq!(b.state(4, t0 + COOL), MediaState::Usable);
    }

    #[test]
    fn mark_while_blocked_does_not_extend() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        assert_eq!(b.mark(4, t0), MarkOutcome::Blocked { last_ok_ago: None });
        assert_eq!(
            b.mark(4, t0 + Duration::from_secs(10)),
            MarkOutcome::AlreadyBlocked
        );
        assert_eq!(b.state(4, t0 + COOL), MediaState::Retry);
    }

    #[test]
    fn mark_after_expiry_starts_new_cooldown() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        b.mark(4, t0);
        let t1 = t0 + COOL + Duration::from_secs(5);
        assert_eq!(b.mark(4, t1), MarkOutcome::Blocked { last_ok_ago: None });
        assert_eq!(
            b.state(4, t1 + COOL - Duration::from_secs(1)),
            MediaState::Blocked
        );
        assert_eq!(b.state(4, t1 + COOL), MediaState::Retry);
    }

    const SEC: Duration = Duration::from_secs(1);

    #[test]
    fn recent_media_success_keeps_media() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        b.ok(4, t0);
        let t = t0 + MEDIA_PROOF_WINDOW - SEC;
        assert_eq!(b.mark(4, t), MarkOutcome::KeptByProof { ago: 59 * SEC });
        assert_eq!(b.state(4, t), MediaState::Usable);
    }

    #[test]
    fn stale_media_success_does_not_prevent_block() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        b.ok(4, t0);
        let t = t0 + MEDIA_PROOF_WINDOW;
        assert_eq!(
            b.mark(4, t),
            MarkOutcome::Blocked {
                last_ok_ago: Some(60 * SEC)
            }
        );
        assert_eq!(b.state(4, t), MediaState::Blocked);
    }

    #[test]
    fn success_after_mark_does_not_end_block() {
        // Lock order mark -> ok: the full cooldown still applies.
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        assert_eq!(b.mark(4, t0), MarkOutcome::Blocked { last_ok_ago: None });
        b.ok(4, t0 + SEC);
        assert_eq!(b.state(4, t0 + COOL - SEC), MediaState::Blocked);
        assert_eq!(b.mark(4, t0 + 2 * SEC), MarkOutcome::AlreadyBlocked);
        assert_eq!(b.state(4, t0 + COOL), MediaState::Retry);
        assert_eq!(b.state(4, t0 + COOL), MediaState::Usable);
    }

    #[test]
    fn success_before_mark_keeps_media() {
        // Lock order ok -> mark (same instant): kept.
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        b.ok(4, t0);
        assert_eq!(
            b.mark(4, t0),
            MarkOutcome::KeptByProof {
                ago: Duration::ZERO
            }
        );
    }

    #[test]
    fn proof_is_per_dc() {
        let mut b = MediaBackoff::new(COOL);
        let t0 = Instant::now();
        b.ok(2, t0);
        assert_eq!(b.mark(4, t0), MarkOutcome::Blocked { last_ok_ago: None });
        assert!(matches!(b.mark(2, t0), MarkOutcome::KeptByProof { .. }));
        assert_eq!(b.state(2, t0), MediaState::Usable);
    }

    #[test]
    fn last_ok_text_formats_age() {
        assert_eq!(last_ok_text(None), "never");
        assert_eq!(last_ok_text(Some(75 * SEC)), "75s ago");
        assert_eq!(last_ok_text(Some(Duration::from_millis(1500))), "1s ago");
    }

    fn bad(status: u32) -> transport::Error {
        transport::Error::BadStatus { status }
    }

    #[test]
    fn transport_status_sees_both_shapes() {
        let nested = InvocationError::Deserialize(mtp::DeserializeError::Transport(bad(429)));
        assert_eq!(transport_status(&nested), Some(429));
        assert_eq!(
            transport_status(&InvocationError::Transport(bad(404))),
            Some(404)
        );
        let other = InvocationError::Transport(transport::Error::BadLen { got: 3 });
        assert_eq!(transport_status(&other), None);
        assert_eq!(transport_status(&InvocationError::Dropped("x")), None);
    }

    #[test]
    fn read_status_of_step_errors() {
        let e = ReadError::Deserialize(mtp::DeserializeError::Transport(bad(429)));
        assert_eq!(read_status(&e), Some(429));
        let io = ReadError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "read 0 bytes",
        ));
        assert_eq!(read_status(&io), None);
    }

    #[test]
    fn open_error_status_unwraps_anyhow() {
        let e = anyhow::Error::new(OpenError(InvocationError::Deserialize(
            mtp::DeserializeError::Transport(bad(429)),
        )))
        .context("outer");
        assert_eq!(open_error_status(&e), Some(429));
        // Key creation wraps the raw InvocationError with .context().
        let ctx_err = Err::<(), _>(InvocationError::Transport(bad(429)))
            .context("匯入授權失敗")
            .unwrap_err();
        assert_eq!(open_error_status(&ctx_err), Some(429));
        assert_eq!(open_error_status(&anyhow!("timeout")), None);
    }

    #[test]
    fn hits_during_cooldown_do_not_extend_it() {
        let mut c = Cooldowns::default();
        let t0 = Instant::now();
        let (until, new) = c.hit(5, t0);
        assert!(new);
        assert_eq!(until, t0 + Duration::from_secs(2));
        let (again, new) = c.hit(5, t0 + Duration::from_secs(1));
        assert!(!new);
        assert_eq!(again, until);
        assert_eq!(c.until(5, t0 + Duration::from_secs(1)), Some(until));
        assert_eq!(c.until(5, until), None);
        assert_eq!(c.until(4, t0), None);
    }

    #[test]
    fn repeated_floods_do_not_escalate() {
        let mut c = Cooldowns::default();
        let t0 = Instant::now();
        let (u0, _) = c.hit(5, t0);
        // Right after it ended and much later: always the same short cooldown.
        let (u1, new) = c.hit(5, u0);
        assert!(new);
        assert_eq!(u1, u0 + COOLDOWN);
        let later = u1 + Duration::from_secs(600);
        let (u2, new) = c.hit(5, later);
        assert!(new);
        assert_eq!(u2, later + COOLDOWN);
        // Other DCs are independent.
        assert_eq!(c.hit(2, t0).0, t0 + COOLDOWN);
        assert_eq!(c.until(2, t0), Some(t0 + COOLDOWN));
    }
}
