use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// https://my.telegram.org 取得
    pub api_id: i32,
    pub api_hash: String,
    /// "bot"（預設，使用 bot token）或 "user"（使用者帳號，需先執行 `login`）
    #[serde(default)]
    pub account: AccountType,
    /// 使用者帳號的手機號碼（國際格式，例如 +886912345678）；留空則登入時詢問
    #[serde(default)]
    pub phone: Option<String>,
    /// 直接填寫 token，或留空並使用 `bot_token_file`
    #[serde(default)]
    pub bot_token: Option<String>,
    #[serde(default = "default_token_file")]
    pub bot_token_file: PathBuf,
    #[serde(default = "default_session")]
    pub session_file: PathBuf,
    #[serde(default = "default_temp_dir")]
    pub temp_dir: PathBuf,
    /// 允許使用的 Telegram user id；空陣列 = 所有人
    #[serde(default)]
    pub allowed_user_ids: Vec<i64>,
    /// SOCKS5 代理，例如 socks5://127.0.0.1:1080 或 socks5://user:pass@host:port
    #[serde(default)]
    pub proxy: Option<String>,
    #[serde(default)]
    pub use_ipv6: bool,
    #[serde(default)]
    pub transfer: TransferConfig,
    #[serde(default)]
    pub status: StatusConfig,
    #[serde(default)]
    pub warm: WarmConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WarmConfig {
    /// 預先建立並保持連線，新任務可立即開始傳輸（省去握手 / 授權時間）
    pub enabled: bool,
    /// 要保持連線的 DC（包含 bot 所在 DC 時，上傳也會使用預熱連線）
    pub dcs: Vec<i32>,
    /// 每個 DC 保持的閒置連線數
    pub connections_per_dc: usize,
    /// 保活 ping 間隔（秒）；同時會偵測並替換已失效的連線
    pub keepalive_secs: u64,
}

impl Default for WarmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dcs: vec![1, 2, 3, 4, 5],
            connections_per_dc: 4,
            keepalive_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TransferConfig {
    /// 從來源 DC 下載時的並行連線數
    pub download_connections: usize,
    /// 上傳到 bot 所在 DC 時的並行連線數
    pub upload_connections: usize,
    /// 每次 upload.getFile 的大小（KB），必須是 4 的倍數、能整除 1024，且不超過 512
    pub download_chunk_kb: u32,
    /// 每條連線同時進行中的請求數（pipelining）。總在途資料量 =
    /// 連線數 × pipeline_depth × 分塊大小；Telegram Desktop 約保持 16 MB 在途。
    pub pipeline_depth: usize,
    /// 單一請求逾時（秒）；逾時後丟棄該連線並重連（處理無聲卡死）
    pub request_timeout_secs: u64,
    /// 建立連線 / 授權逾時（秒），含 SOCKS5 握手
    pub connect_timeout_secs: u64,
    /// 全部連線持續多久沒有任何進度就判定停滯並全部重連（秒）
    pub stall_timeout_secs: u64,
    /// 連續停滯重連的次數上限，超過則任務失敗
    pub max_stall_restarts: u32,
    /// 單一分塊遇到非網路錯誤時的重試上限
    pub max_part_retries: u32,
    /// 單一連線連續網路錯誤上限
    pub max_reconnects_per_worker: u32,
    /// FLOOD_WAIT 超過此秒數直接放棄
    pub max_flood_wait_secs: u64,
    /// 遇到 FLOOD_PREMIUM_WAIT 時："reconnect"（重建連線，不等待）或 "wait"
    pub premium_wait_strategy: PremiumWaitStrategy,
    /// 下載遇到 FLOOD_WAIT 時："wait"（等待指定秒數）或 "reconnect"
    /// （立即以新的 MTProto session 重建該連線，不等待；同 telebackup 的 TDL 策略）
    pub flood_wait_strategy: PremiumWaitStrategy,
    /// 重連前的隨機抖動上限（毫秒）
    pub reconnect_jitter_ms: u64,
    /// 同時處理的任務數，其餘排隊
    pub max_concurrent_jobs: usize,
    /// 對來源 DC 的每條連線使用各自獨立的授權（建議開啟）。
    /// Telegram 的下載限流（FLOOD_WAIT）是以授權為單位計算，共用一個授權時所有連線會同時被限流。
    /// Download from foreign DCs via their media-only address (like Telegram
    /// Desktop's media cluster) instead of the regular API front-end.
    pub use_media_dc: bool,
    pub auth_per_connection: bool,
    /// 檔案已位於本帳號的 DC 時仍重新下載並上傳（預設關閉，直接回覆「已位於 DCx」）。
    pub reupload_same_dc: bool,
    /// 超過上傳上限的檔案會分割成多個部分，每部分最大 MiB
    /// （實際上限：bot / 一般帳號 2000 MiB，Premium 帳號 4000 MiB）
    pub split_size_mb: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AccountType {
    #[default]
    Bot,
    User,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PremiumWaitStrategy {
    Reconnect,
    Wait,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StatusConfig {
    /// 狀態訊息最短更新間隔（秒）
    pub edit_interval_secs: u64,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            download_connections: 4,
            upload_connections: 4,
            download_chunk_kb: 512,
            pipeline_depth: 8,
            request_timeout_secs: 15,
            connect_timeout_secs: 20,
            stall_timeout_secs: 60,
            max_stall_restarts: 5,
            max_part_retries: 5,
            max_reconnects_per_worker: 20,
            max_flood_wait_secs: 600,
            premium_wait_strategy: PremiumWaitStrategy::Reconnect,
            flood_wait_strategy: PremiumWaitStrategy::Wait,
            reconnect_jitter_ms: 500,
            max_concurrent_jobs: 1,
            auth_per_connection: true,
            use_media_dc: true,
            split_size_mb: 2000,
            reupload_same_dc: false,
        }
    }
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            edit_interval_secs: 3,
        }
    }
}

fn default_token_file() -> PathBuf {
    "token.txt".into()
}
fn default_session() -> PathBuf {
    "bot.session".into()
}
fn default_temp_dir() -> PathBuf {
    "./tmp".into()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config file {}", path.display()))?;
        let mut cfg: Config = toml::from_str(&text).context("invalid config file")?;

        if cfg.account == AccountType::Bot
            && cfg.bot_token.as_deref().is_none_or(|t| t.trim().is_empty())
        {
            let token = std::fs::read_to_string(&cfg.bot_token_file).with_context(|| {
                format!(
                    "bot_token not set and cannot read {}",
                    cfg.bot_token_file.display()
                )
            })?;
            cfg.bot_token = Some(token.trim().to_string());
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn token(&self) -> &str {
        self.bot_token.as_deref().unwrap_or_default()
    }

    fn validate(&self) -> Result<()> {
        let t = &self.transfer;
        if self.api_id == 0 || self.api_hash.is_empty() {
            bail!("api_id / api_hash must be set (https://my.telegram.org)");
        }
        if self.account == AccountType::Bot && !self.token().contains(':') {
            bail!("bot token looks invalid");
        }
        if let Some(p) = &self.proxy
            && !p.starts_with("socks5://")
        {
            bail!("proxy must start with socks5://");
        }
        if !(1..=16).contains(&t.download_connections) || !(1..=16).contains(&t.upload_connections)
        {
            bail!("download_connections / upload_connections must be 1..=16");
        }
        let kb = t.download_chunk_kb;
        // grammers rejects (panics on) any MTProto message >= ~1 MB, and a 1024 KB
        // getFile response is 1 MB of data + headers, so 512 KB is the real maximum.
        if kb == 0 || kb > 512 || !kb.is_multiple_of(4) || !1024u32.is_multiple_of(kb) {
            bail!(
                "download_chunk_kb must be a multiple of 4 that divides 1024 and is <= 512 (e.g. 128, 256, 512)"
            );
        }
        if t.request_timeout_secs == 0 || t.connect_timeout_secs == 0 || t.stall_timeout_secs == 0 {
            bail!("timeouts must be > 0");
        }
        if t.stall_timeout_secs <= t.connect_timeout_secs + t.request_timeout_secs {
            bail!(
                "stall_timeout_secs must be greater than connect_timeout_secs + request_timeout_secs"
            );
        }
        let w = &self.warm;
        if w.enabled {
            if w.dcs.iter().any(|dc| !(1..=5).contains(dc)) {
                bail!("warm.dcs must only contain DC ids 1..=5");
            }
            if w.connections_per_dc > 16 || w.keepalive_secs < 5 {
                bail!("warm.connections_per_dc must be <= 16 and warm.keepalive_secs >= 5");
            }
        }
        if !(1..=4000).contains(&t.split_size_mb) {
            bail!(
                "split_size_mb must be 1..=4000 (it is also capped at runtime to the account's upload limit)"
            );
        }
        if !(1..=32).contains(&t.pipeline_depth) {
            bail!("pipeline_depth must be 1..=32");
        }
        if t.max_concurrent_jobs == 0 {
            bail!("max_concurrent_jobs must be >= 1");
        }
        Ok(())
    }
}
