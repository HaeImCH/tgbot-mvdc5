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
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use grammers_client::Client;
use grammers_mtproto::{mtp, transport::Full};
use grammers_mtsender::{InvocationError, Sender, ServerAddr, connect, connect_with_auth};
use grammers_session::Session;
use grammers_session::storages::SqliteSession;
use grammers_tl_types as tl;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{info, warn};

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
    /// DCs whose media address failed to connect; they use the regular one.
    media_broken: std::sync::Mutex<std::collections::HashSet<i32>>,
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
            media_broken: std::sync::Mutex::new(Default::default()),
        }
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
        if (dc_id == self.home_dc && !download)
            || self.media_broken.lock().unwrap().contains(&dc_id)
        {
            return None;
        }
        self.media.read().unwrap().get(&dc_id).copied()
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
        .context("匯出授權失敗")?;

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
            Some(s) => Ok(s),
            None => self.open(dc_id, slot).await,
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
                            Ok(Ok(_)) => Some((slot, s)),
                            _ => None,
                        }
                    });
                }
                let mut dead = 0;
                while let Some(r) = set.join_next().await {
                    match r {
                        Ok(Some((slot, s))) => self.give_back(dc, slot, s),
                        _ => dead += 1,
                    }
                }
                if dead > 0 {
                    warn!("warm pool DC{dc}: dropped {dead} dead connection(s)");
                }

                // Refill.
                if !w.dcs.contains(&dc) {
                    continue;
                }
                let mut opened = 0;
                for slot in 0..w.connections_per_dc {
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
    /// Uses the DC's media address when known; if that fails, the DC is marked
    /// and this and later connections use the regular address.
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
            Err(e) if via_media && e.downcast_ref::<OpenError>().is_none_or(|o| !o.is_rpc()) => {
                if self.media_broken.lock().unwrap().insert(dc_id) {
                    warn!(
                        "DC{dc_id}: media address failed ({e:#}); falling back to the regular address"
                    );
                }
                self.open_at(dc_id, key, false).await
            }
            r => r,
        }
    }

    async fn open_at(&self, dc_id: i32, key: [u8; 256], media: bool) -> Result<Conn> {
        let (addr, via_media) = self.server_addr(dc_id, media)?;
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
            Ok(Err(e)) => Err(anyhow::Error::new(OpenError(e))),
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
