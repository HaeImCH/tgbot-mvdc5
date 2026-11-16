//! Message handling: detect media not stored on the bot's home DC, move it, and
//! keep the user informed by editing a status message (zh_TW).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use grammers_client::media::{Downloadable, Media};
use grammers_client::message::{Button, InputMessage, Message, ReplyMarkup};
use grammers_client::update::CallbackQuery;
use grammers_session::types::{PeerId, PeerKind, PeerRef};
use grammers_tl_types as tl;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, error, info, warn};

use crate::config::AccountType;
use crate::net::Net;
use crate::status::{self, Progress, Stage, human_bytes, human_duration};
use crate::transfer::{self, BIG_FILE, Direction, Location, Plan, UPLOAD_PART};

pub struct Bot {
    pub net: Arc<Net>,
    jobs: Semaphore,
    waiting: AtomicUsize,
    /// Ids of original messages we are moving / recently moved (user-mode loop guard).
    sources: std::sync::Mutex<std::collections::HashSet<i32>>,
    /// Jobs in flight (queued or running), for cancelling and for cleanup on
    /// shutdown, by source message id.
    active: std::sync::Mutex<std::collections::HashMap<i32, ActiveJob>>,
}

struct ActiveJob {
    /// Chat of the source message (as `msg.peer_id()` reports it).
    chat: PeerId,
    peer: PeerRef,
    status_id: i32,
    tmp: PathBuf,
    job: String,
    cancel: Arc<Notify>,
}

/// Callback data prefix of the cancel button: `cancel:<source message id>`.
const CANCEL_DATA: &str = "cancel:";
/// User accounts cannot send inline buttons, so user mode cancels by reply.
const USER_CANCEL_HINT: &str = "\n\n↩️ 回覆此訊息「取消」即可取消任務";

/// Decrements the queue counter when a job leaves the queue (or is cancelled in it).
struct Waiting<'a>(&'a AtomicUsize);
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Text that asks to cancel: `/cancel` (also `/cancel@bot`), `取消` or `cancel`.
fn is_cancel_text(text: &str) -> bool {
    let t = text.trim();
    let cmd = t.split('@').next().unwrap_or(t);
    cmd.eq_ignore_ascii_case("/cancel") || t == "取消" || t.eq_ignore_ascii_case("cancel")
}

/// A link to one message.
#[derive(Debug, PartialEq)]
enum MsgLink {
    /// `t.me/<username>/<id>` (also `t.me/<username>/<topic>/<id>`, `t.me/s/...`).
    Public { username: String, id: i32 },
    /// `t.me/c/<channel id>/<id>` (also with a topic id): needs membership.
    Private { channel: i64, id: i32 },
}

/// Parse a message whose whole text is one t.me message link.
fn parse_link(text: &str) -> Option<MsgLink> {
    let t = text.trim();
    let t = t
        .strip_prefix("https://")
        .or_else(|| t.strip_prefix("http://"))
        .unwrap_or(t);
    let (host, path) = t.split_once('/')?;
    let host = host.to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    if !matches!(host, "t.me" | "telegram.me" | "telegram.dog") {
        return None;
    }
    let path = path.split(['?', '#']).next()?;
    let mut parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    if parts.first() == Some(&"s") {
        parts.remove(0);
    }
    let id: i32 = match parts.as_slice() {
        [_, .., last] if parts.len() >= 2 && parts.len() <= 4 => last.parse().ok()?,
        _ => return None,
    };
    if id <= 0 {
        return None;
    }
    let name = parts[0];
    let middle = &parts[1..parts.len() - 1];
    if name == "c" {
        // c/<channel>/<id> or c/<channel>/<topic>/<id>
        let [channel, rest @ ..] = middle else {
            return None;
        };
        if rest.len() > 1 || rest.iter().any(|p| p.parse::<i32>().is_err()) {
            return None;
        }
        return Some(MsgLink::Private {
            channel: channel.parse().ok()?,
            id,
        });
    }
    // <username>/<id> or <username>/<topic>/<id>
    let valid = name.len() >= 4
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.starts_with(|c: char| c.is_ascii_alphabetic());
    if !valid || middle.len() > 1 || middle.iter().any(|p| p.parse::<i32>().is_err()) {
        return None;
    }
    Some(MsgLink::Public {
        username: name.to_string(),
        id,
    })
}

/// What we know about the incoming file.
pub(crate) struct FileInfo {
    pub(crate) name: String,
    pub(crate) size: u64,
    pub(crate) dc: i32,
    pub(crate) location: tl::enums::InputFileLocation,
    pub(crate) exact_size: bool,
    kind: FileKind,
}

enum FileKind {
    Photo,
    Document {
        mime: String,
        attributes: Vec<tl::enums::DocumentAttribute>,
    },
}

const HELP: &str = "👋 歡迎使用 DC 搬移機器人\n\n\
把任何含有檔案的訊息轉傳給我（影片、文件、照片、音訊…），或直接傳送訊息連結（例如 https://t.me/頻道/123，禁止轉傳的頻道也可以）。\n\
如果檔案不在本機器人所在的 DC，我會用多條連線從原本的 DC 下載，再重新上傳到本機器人所在的 DC，然後把新檔案傳回給你。\n\n\
處理過程中我會持續更新狀態訊息，讓你知道目前的進度。";

impl Bot {
    pub fn new(net: Arc<Net>) -> Self {
        let n = net.cfg.transfer.max_concurrent_jobs;
        Self {
            net,
            jobs: Semaphore::new(n),
            waiting: AtomicUsize::new(0),
            sources: Default::default(),
            active: Default::default(),
        }
    }

    /// Decide whether to act on a message. Returns false to ignore it silently.
    ///
    /// Bot mode: incoming private messages (optionally restricted to allowed users).
    /// User mode: the account sees every chat, so only your own messages in Saved
    /// Messages and incoming private messages from `allowed_user_ids` are handled
    /// (strangers are ignored). Plain text never gets an answer, and messages that
    /// reply to a file we are/were moving are ignored, so our own status messages
    /// and re-uploaded files can never trigger another job.
    async fn accept(&self, msg: &Message) -> bool {
        let peer = msg.peer_id();
        if peer.kind() != PeerKind::User {
            return false;
        }
        let allowed = &self.net.cfg.allowed_user_ids;
        let sender = msg.sender_id().and_then(|p| p.bare_id());

        if self.is_user_mode() {
            // Saved Messages: the chat with ourselves. Only we can post there, so do
            // not rely on the `out` flag (forwarded messages may not carry it).
            if self.is_saved_messages(peer) {
                let reply_to = msg.reply_to_message_id();
                info!(
                    "saved message {} received: out={} media={} reply_to={reply_to:?}",
                    msg.id(),
                    msg.outgoing(),
                    msg.media().is_some()
                );
                // Our re-uploaded files are always sent as replies to the original.
                if let Some(r) = reply_to
                    && self.sources.lock().unwrap().contains(&r)
                {
                    return false;
                }
                return true;
            }
            debug!(
                "user mode: ignoring message {} in {:?} (out={})",
                msg.id(),
                peer,
                msg.outgoing()
            );
            return !msg.outgoing() && sender.is_some_and(|id| allowed.contains(&id));
        }

        if msg.outgoing() {
            return false;
        }
        if !allowed.is_empty() && !sender.is_some_and(|id| allowed.contains(&id)) {
            info!("ignoring message from unauthorized user {sender:?}");
            let _ = msg.reply("⛔ 你沒有使用此機器人的權限。").await;
            return false;
        }
        true
    }

    /// Called on SIGTERM / Ctrl+C: delete temp files of unfinished jobs and tell
    /// each user their transfer was interrupted (instead of a frozen progress bar).
    pub async fn shutdown(&self) {
        let jobs: Vec<_> = self.active.lock().unwrap().drain().collect();
        if jobs.is_empty() {
            return;
        }
        info!("shutdown: interrupting {} unfinished job(s)", jobs.len());
        for (
            _,
            ActiveJob {
                peer,
                status_id,
                tmp,
                job,
                ..
            },
        ) in jobs
        {
            info!("shutdown: interrupted {job}");
            match tokio::fs::remove_file(&tmp).await {
                Ok(()) => info!("shutdown: removed {}", tmp.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => warn!("shutdown: cannot remove {}: {e}", tmp.display()),
            }
            let text =
                "⚠️ 服務正在重新啟動，此任務已中斷，暫存檔已清除。\n\n請稍後重新轉傳檔案再試一次。";
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                status::edit(&self.net.client, peer, status_id, text, None),
            )
            .await;
        }
    }

    /// Cancel the jobs in `chat` that `pick(source id, job)` selects. They stop at
    /// their next await point and clean up themselves. Returns the number cancelled.
    fn cancel_jobs(&self, chat: PeerId, pick: impl Fn(i32, &ActiveJob) -> bool) -> usize {
        let mut active = self.active.lock().unwrap();
        let ids: Vec<i32> = active
            .iter()
            .filter(|(id, j)| j.chat == chat && pick(**id, j))
            .map(|(id, _)| *id)
            .collect();
        for id in &ids {
            // Removed here so a second press does not report success again.
            if let Some(j) = active.remove(id) {
                info!("cancel requested: {}", j.job);
                j.cancel.notify_one();
            }
        }
        ids.len()
    }

    /// The cancel button under a status message (bot mode).
    pub async fn on_callback(self: Arc<Self>, q: CallbackQuery) {
        let source = std::str::from_utf8(q.data())
            .ok()
            .and_then(|d| d.strip_prefix(CANCEL_DATA))
            .and_then(|id| id.parse::<i32>().ok());
        let Some(source) = source else {
            let _ = q.answer().send().await;
            return;
        };
        let n = self.cancel_jobs(q.peer_id(), |id, _| id == source);
        let text = if n > 0 {
            "🚫 已取消任務"
        } else {
            "此任務已經結束"
        };
        if let Err(e) = q.answer().text(text).send().await {
            debug!("cannot answer callback query: {e}");
        }
    }

    /// A text message asking to cancel: a reply to a status message cancels that
    /// job; a bare `/cancel` cancels every job in the chat. Returns false when
    /// the text is not a cancel request.
    async fn handle_cancel_text(&self, msg: &Message, text: &str) -> bool {
        if !is_cancel_text(text) {
            return false;
        }
        let chat = msg.peer_id();
        let n = match msg.reply_to_message_id() {
            Some(r) => self.cancel_jobs(chat, |_, j| j.status_id == r),
            None if text.starts_with('/') => self.cancel_jobs(chat, |_, _| true),
            None => return false,
        };
        if !self.is_user_mode() && n == 0 {
            let _ = msg.reply("目前沒有可以取消的任務。").await;
        }
        true
    }

    fn is_user_mode(&self) -> bool {
        self.net.cfg.account == AccountType::User
    }

    /// The chat with ourselves (user mode).
    fn is_saved_messages(&self, peer: PeerId) -> bool {
        peer == PeerId::self_user() || peer.bare_id() == Some(self.net.me_id)
    }

    /// Who asked for this job, for the logs: id plus @username when known.
    fn requester(&self, msg: &Message) -> String {
        if self.is_user_mode() && self.is_saved_messages(msg.peer_id()) {
            return "Saved Messages".to_string();
        }
        let Some(id) = msg.sender_id().and_then(|p| p.bare_id()) else {
            return "unknown user".to_string();
        };
        match msg.sender().and_then(|p| p.username().map(str::to_string)) {
            Some(u) => format!("user {id} (@{u})"),
            None => format!("user {id}"),
        }
    }

    pub async fn handle(self: Arc<Self>, msg: Message) {
        if !self.accept(&msg).await {
            return;
        }
        let user_mode = self.is_user_mode();

        let text = msg.text().trim().to_string();
        if msg.media().is_none() && self.handle_cancel_text(&msg, &text).await {
            return;
        }
        // A message link (t.me/<channel>/<id>, t.me/c/<id>/<id>): move the file
        // of the linked message. Works for channels that forbid forwarding too.
        // Telegram attaches a link preview (WebPage media) to link messages.
        let has_file = msg.media().is_some_and(|m| !matches!(m, Media::WebPage(_)));
        let linked = match (has_file, parse_link(&text)) {
            (false, Some(link)) => match self.fetch_linked(&link).await {
                Ok(m) => Some(m),
                Err(e) => {
                    info!(
                        "link {link:?} from {} (message {}): {e:#}",
                        self.requester(&msg),
                        msg.id()
                    );
                    let _ = msg.reply(format!("⚠️ 無法取得連結中的訊息：{e}")).await;
                    return;
                }
            },
            _ => None,
        };
        let (src, src_peer) = match &linked {
            Some((m, p)) => (m, Some(*p)),
            None => (&msg, None),
        };
        let Some(media) = src.media() else {
            if linked.is_some() {
                let _ = msg.reply("⚠️ 連結中的訊息沒有檔案。").await;
                return;
            }
            // Never answer text in user mode (would talk in your chats / loop on itself).
            if !user_mode
                && (text.starts_with("/start") || text.starts_with("/help") || !text.is_empty())
            {
                let home = self.net.home_dc;
                let _ = msg
                    .reply(format!("{HELP}\n\n📍 本機器人位於 DC{home}。"))
                    .await;
            }
            return;
        };

        // User mode moves only forwarded messages and message links: files you
        // send or upload directly, and stickers, are left alone.
        if user_mode {
            let sticker = matches!(media, Media::Sticker(_));
            if sticker && linked.is_some() {
                let _ = msg.reply("⚠️ 連結中的訊息是貼圖，不會搬移。").await;
                return;
            }
            if sticker || (linked.is_none() && msg.forward_header().is_none()) {
                debug!(
                    "user mode: ignoring message {} ({})",
                    msg.id(),
                    if sticker { "sticker" } else { "not forwarded" }
                );
                return;
            }
        }

        let info = match file_info(&media) {
            Some(i) => i,
            None => {
                if !user_mode || linked.is_some() {
                    let _ = msg.reply("⚠️ 這則訊息的內容不是可搬移的檔案（僅支援文件、影片、音訊、照片、貼圖）。").await;
                }
                return;
            }
        };

        let home = self.net.home_dc;
        // File names come from the sender: always log them escaped ({:?}) so a
        // name with newlines cannot forge log lines.
        let mut job = format!(
            "{:?} from {} (message {})",
            info.name,
            self.requester(&msg),
            msg.id()
        );
        if linked.is_some() {
            job.push_str(&format!(" via link {text:?}"));
        }
        info!(
            "file request: {job}, {} on DC{}",
            human_bytes(info.size),
            info.dc
        );
        if info.dc == home && !self.net.cfg.transfer.reupload_same_dc {
            // Our own re-uploaded files are already filtered out in accept() (they
            // reply to a source we are moving), so it is safe to answer here.
            info!("skipped {job}: already on DC{home}");
            if let Err(e) = msg
                .reply(format!(
                    "✅ 此檔案已經位於 DC{home}（與目前帳號相同），不需要搬移。\n\n📄 {}\n📦 {}",
                    info.name,
                    human_bytes(info.size)
                ))
                .await
            {
                warn!("{job}: cannot send reply: {e}");
            }
            return;
        }
        let source_id = msg.id();
        self.sources.lock().unwrap().insert(source_id);
        let forget = {
            let this = self.clone();
            move || {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    this.sources.lock().unwrap().remove(&source_id);
                });
            }
        };

        // Bot mode: a cancel button (re-sent with every status edit). User mode:
        // a hint to reply instead, since user accounts cannot send buttons.
        let (markup, footer) = if user_mode {
            (None, USER_CANCEL_HINT)
        } else {
            let button = Button::data("❌ 取消", format!("{CANCEL_DATA}{source_id}"));
            (Some(ReplyMarkup::from_buttons(&[vec![button]]).raw), "")
        };
        let mut first = InputMessage::new().text(format!("⏳ 已收到檔案，準備處理中…{footer}"));
        if let Some(raw) = &markup {
            first = first.reply_markup(ReplyMarkup { raw: raw.clone() });
        }
        let status_msg = match msg.reply(first).await {
            Ok(m) => m,
            Err(e) => {
                warn!("{job}: cannot send status message, giving up: {e}");
                forget();
                return;
            }
        };
        let peer = match msg.peer_ref().await {
            Ok(Some(p)) => p,
            _ => {
                warn!("{job}: chat not in session cache, giving up");
                forget();
                return;
            }
        };
        let src_peer = src_peer.unwrap_or(peer);

        let tmp = self.net.cfg.temp_dir.join(format!(
            "{}_{}.part",
            msg.peer_id().bare_id_unchecked(),
            msg.id()
        ));
        let cancel = Arc::new(Notify::new());
        self.active.lock().unwrap().insert(
            source_id,
            ActiveJob {
                chat: msg.peer_id(),
                peer,
                status_id: status_msg.id(),
                tmp: tmp.clone(),
                job: job.clone(),
                cancel: cancel.clone(),
            },
        );

        let progress = Arc::new(Progress::new(info.name.clone(), info.size, info.dc, home));
        let reporter = tokio::spawn(status::run_reporter(
            self.net.client.clone(),
            peer,
            status_msg.id(),
            progress.clone(),
            Duration::from_secs(self.net.cfg.status.edit_interval_secs.max(1)),
            markup,
            footer,
        ));

        // Queue.
        let waiting_before = self.waiting.fetch_add(1, Ordering::SeqCst);
        let running = self.net.cfg.transfer.max_concurrent_jobs - self.jobs.available_permits();
        let ahead = if self.jobs.available_permits() == 0 {
            waiting_before + running
        } else {
            0
        };
        progress.set_stage(Stage::Queued { ahead });
        let work = async {
            let permit = {
                let _waiting = Waiting(&self.waiting);
                self.jobs.acquire().await
            };
            let result = self
                .process(&msg, src, src_peer, peer, info, &tmp, &progress)
                .await;
            drop(permit);
            result
        };
        // Cancelling drops `work`: a queued job leaves the queue, a running one
        // stops its transfer (the worker JoinSet aborts its tasks on drop).
        let result = tokio::select! {
            biased;
            _ = cancel.notified() => None,
            r = work => Some(r),
        };
        let _ = tokio::fs::remove_file(&tmp).await;
        self.active.lock().unwrap().remove(&source_id);
        reporter.abort();

        let elapsed = human_duration(progress.started.elapsed().as_secs());
        let outcome: String;
        let final_text = match result {
            None => {
                outcome = format!("job cancelled: {job} after {elapsed}");
                format!(
                    "🚫 任務已取消\n\n📄 檔案：{}\n🕒 耗時：{elapsed}\n\n暫存檔已清除。",
                    progress.file_name
                )
            }
            Some(Ok(done)) => {
                let pieces = done.pieces.len().max(1);
                let location = match done.new_dc {
                    _ if !done.all_same_dc => "pieces on different DCs".to_string(),
                    Some(dc) if dc == home => format!("DC{dc}"),
                    Some(dc) => format!("DC{dc}, expected DC{home}"),
                    None => "unknown DC".to_string(),
                };
                outcome = format!(
                    "job done: {job} delivered in {elapsed} ({pieces} piece(s), now on {location})"
                );
                let avg =
                    progress.total() as f64 / progress.started.elapsed().as_secs_f64().max(0.001);
                let where_now = match done.new_dc {
                    Some(dc) if dc == home && done.all_same_dc => format!("✅ 新檔案已位於 DC{dc}"),
                    Some(_) if !done.all_same_dc => "⚠️ 各部分位於不同的 DC".to_string(),
                    Some(dc) => format!("⚠️ 新檔案位於 DC{dc}（預期 DC{home}）"),
                    None => "✅ 已重新上傳".to_string(),
                };
                let split_info = split_instructions(
                    &progress.file_name,
                    &done.pieces,
                    self.net.max_upload_parts * UPLOAD_PART,
                );
                format!(
                    "🎉 搬移完成！\n\n📄 檔案：{}\n📦 大小：{}\n🔀 DC{} → DC{home}\n{where_now}{split_info}\n\n{}\n🕒 總耗時：{elapsed}（整體平均 {}/s）",
                    progress.file_name,
                    human_bytes(progress.total()),
                    progress.src_dc,
                    progress.speed_summary(),
                    human_bytes(avg as u64),
                )
            }
            Some(Err(e)) => {
                error!("job failed: {job} after {elapsed}: {e:#}");
                outcome = String::new();
                let chain: Vec<String> = e.chain().skip(1).map(|c| c.to_string()).collect();
                let details = if chain.is_empty() {
                    String::new()
                } else {
                    format!("\n🔧 技術細節：{}", chain.join(" ← "))
                };
                format!(
                    "❌ 搬移失敗\n\n📄 檔案：{}\n原因：{e}{details}\n🕒 耗時：{elapsed}\n\n可以重新轉傳一次再試。",
                    progress.file_name
                )
            }
        };
        forget();
        // Final edit: retry a few times so the user is not left with a stale message.
        let mut edited = false;
        for _ in 0..3 {
            match status::edit(&self.net.client, peer, status_msg.id(), &final_text, None).await {
                status::EditResult::Ok => {
                    edited = true;
                    break;
                }
                status::EditResult::Flood(s) => {
                    tokio::time::sleep(Duration::from_secs(s.min(30))).await
                }
                status::EditResult::Err => tokio::time::sleep(Duration::from_secs(2)).await,
            }
        }
        if !outcome.is_empty() {
            info!("{outcome}");
        }
        if !edited {
            warn!("{job}: could not update the final status message");
        }
    }

    /// Fetch the message a link points to, with a usable reference to its chat.
    async fn fetch_linked(&self, link: &MsgLink) -> Result<(Message, PeerRef)> {
        let client = &self.net.client;
        let (peer, id) = match link {
            MsgLink::Public { username, id } => {
                let peer = client
                    .resolve_username(username)
                    .await
                    .context("查詢使用者名稱失敗")?
                    .ok_or_else(|| anyhow!("找不到 @{username}"))?;
                let peer = peer
                    .to_ref()
                    .await
                    .map_err(|e| anyhow!("{e}"))?
                    .ok_or_else(|| anyhow!("無法存取 @{username}"))?;
                (peer, *id)
            }
            MsgLink::Private { channel, id } => {
                use grammers_session::Session;
                let pid = PeerId::channel(*channel).ok_or_else(|| anyhow!("連結格式錯誤"))?;
                let peer = self
                    .net
                    .session
                    .peer_ref(pid)
                    .await
                    .map_err(|e| anyhow!("{e}"))?
                    .ok_or_else(|| anyhow!("帳號尚未加入此私人頻道或群組"))?;
                (peer, *id)
            }
        };
        let m = client
            .get_messages_by_id(peer, &[id])
            .await
            .context("讀取訊息失敗")?
            .pop()
            .flatten()
            .ok_or_else(|| anyhow!("訊息不存在或已被刪除"))?;
        Ok((m, peer))
    }

    #[allow(clippy::too_many_arguments)]
    async fn process(
        &self,
        msg: &Message,
        src: &Message,
        src_peer: PeerRef,
        peer: PeerRef,
        info: FileInfo,
        tmp: &PathBuf,
        progress: &Arc<Progress>,
    ) -> Result<Done> {
        let cfg = &self.net.cfg;
        tokio::fs::create_dir_all(&cfg.temp_dir)
            .await
            .context("無法建立暫存資料夾")?;
        if let Some(free) = free_space(&cfg.temp_dir)
            && free < info.size + 64 * 1024 * 1024
        {
            bail!(
                "伺服器磁碟空間不足：需要 {}，剩餘 {}",
                human_bytes(info.size),
                human_bytes(free)
            );
        }
        {
            let f = tokio::fs::File::create(tmp)
                .await
                .context("無法建立暫存檔")?;
            f.set_len(info.size)
                .await
                .context("磁碟空間不足或無法配置暫存檔")?;
        }

        // ---- download from source DC ----
        progress.set_stage(Stage::Connecting { dc: info.dc });
        self.net
            .auth_key(info.dc, 0)
            .await
            .with_context(|| format!("無法取得 DC{} 的授權", info.dc))?;

        progress.set_stage(Stage::Downloading { dc: info.dc });
        let client = self.net.client.clone();
        let msg_id = msg.id();
        let src_id = src.id();
        let refresher: transfer::Refresher = Box::new(move || {
            let client = client.clone();
            Box::pin(async move {
                let m = client
                    .get_messages_by_id(src_peer, &[src_id])
                    .await?
                    .pop()
                    .flatten()
                    .ok_or_else(|| anyhow!("原始訊息已被刪除"))?;
                let media = m.media().ok_or_else(|| anyhow!("原始訊息已沒有檔案"))?;
                file_info(&media)
                    .map(|i| i.location)
                    .ok_or_else(|| anyhow!("無法取得檔案位置"))
            })
        });
        let location = Arc::new(Location::new(info.location.clone(), refresher));
        let chunk = cfg.transfer.download_chunk_kb as u64 * 1024;
        let dl_plan = Plan::new(info.size, chunk);
        let t0 = std::time::Instant::now();
        let real_size = transfer::run(
            self.net.clone(),
            info.dc,
            cfg.transfer.download_connections,
            dl_plan,
            Direction::Download {
                location,
                exact_size: info.exact_size,
            },
            tmp.clone(),
            progress.clone(),
        )
        .await
        .context("下載失敗")?;
        if real_size != info.size {
            tokio::fs::OpenOptions::new()
                .write(true)
                .open(tmp)
                .await?
                .set_len(real_size)
                .await?;
        }
        info!(
            "downloaded {:?} ({}) from DC{} in {:?}",
            info.name,
            human_bytes(real_size),
            info.dc,
            t0.elapsed()
        );

        // ---- upload to home DC (split into pieces if over the bot limit) ----
        let home = self.net.home_dc;
        progress.set_stage(Stage::Uploading { dc: home });
        let upload_limit = self.net.max_upload_parts * UPLOAD_PART;
        let piece_size = (cfg.transfer.split_size_mb * 1024 * 1024).min(upload_limit);
        let ranges = transfer::split_ranges(real_size, piece_size);
        let split = ranges.len() > 1;
        if split {
            info!(
                "{:?} is {}, splitting into {} pieces",
                info.name,
                human_bytes(real_size),
                ranges.len()
            );
        }
        let mut uploaded = Vec::with_capacity(ranges.len());
        for (i, &(base, len)) in ranges.iter().enumerate() {
            progress.set_piece(i + 1, ranges.len());
            let piece = Piece {
                file_id: rand::random(),
                big: len > BIG_FILE,
                base,
                plan: Plan::new(len, UPLOAD_PART),
                name: if split {
                    format!("{}.{:03}", info.name, i + 1)
                } else {
                    info.name.clone()
                },
            };
            transfer::run(
                self.net.clone(),
                home,
                cfg.transfer.upload_connections,
                piece.plan,
                Direction::Upload {
                    file_id: piece.file_id,
                    big: piece.big,
                    base: piece.base,
                },
                tmp.clone(),
                progress.clone(),
            )
            .await
            .with_context(|| {
                if split {
                    format!("上傳第 {}/{} 部分失敗", i + 1, ranges.len())
                } else {
                    "上傳失敗".to_string()
                }
            })?;
            uploaded.push(piece);
        }

        // ---- send ----
        progress.set_stage(Stage::Sending);
        let caption = src.text().to_string();
        let entities = src.fmt_entities().cloned().unwrap_or_default();
        let total = uploaded.len();
        let mut new_dcs = Vec::with_capacity(total);
        for (i, piece) in uploaded.iter().enumerate() {
            progress.set_piece(i + 1, total);
            let input_file = piece.input_file();
            let (media, text, ents): (
                tl::enums::InputMedia,
                String,
                Vec<tl::enums::MessageEntity>,
            ) = if !split {
                let media = match &info.kind {
                    FileKind::Photo => tl::types::InputMediaUploadedPhoto {
                        spoiler: false,
                        live_photo: false,
                        file: input_file,
                        stickers: None,
                        ttl_seconds: None,
                        video: None,
                    }
                    .into(),
                    FileKind::Document { mime, attributes } => {
                        tl::types::InputMediaUploadedDocument {
                            nosound_video: false,
                            force_file: false,
                            spoiler: false,
                            file: input_file,
                            thumb: None,
                            mime_type: mime.clone(),
                            attributes: attributes.clone(),
                            stickers: None,
                            ttl_seconds: None,
                            video_cover: None,
                            video_timestamp: None,
                        }
                        .into()
                    }
                };
                (media, caption.clone(), entities.clone())
            } else {
                // Pieces are raw byte ranges: send as plain files.
                let media = tl::types::InputMediaUploadedDocument {
                    nosound_video: false,
                    force_file: true,
                    spoiler: false,
                    file: input_file,
                    thumb: None,
                    mime_type: "application/octet-stream".into(),
                    attributes: vec![
                        tl::types::DocumentAttributeFilename {
                            file_name: piece.name.clone(),
                        }
                        .into(),
                    ],
                    stickers: None,
                    ttl_seconds: None,
                    video_cover: None,
                    video_timestamp: None,
                }
                .into();
                let suffix = format!("🧩 {}（第 {}/{} 部分）", info.name, i + 1, total);
                // Keep the original caption on the first piece if it fits (1024 limit).
                if i == 0
                    && !caption.is_empty()
                    && caption.encode_utf16().count() + suffix.encode_utf16().count() + 2 <= 1024
                {
                    (media, format!("{caption}\n\n{suffix}"), entities.clone())
                } else {
                    (media, suffix, Vec::new())
                }
            };
            let sent = self
                .send_with_retry(peer, msg_id, media, text, ents, piece, tmp, progress)
                .await
                .with_context(|| {
                    if split {
                        format!("傳送第 {}/{} 部分失敗", i + 1, total)
                    } else {
                        "傳送檔案失敗".to_string()
                    }
                })?;
            new_dcs.push(sent.media().as_ref().and_then(media_dc));
        }

        Ok(Done {
            new_dc: new_dcs.first().copied().flatten(),
            all_same_dc: new_dcs.windows(2).all(|w| w[0] == w[1]),
            pieces: if split {
                uploaded
                    .iter()
                    .map(|p| (p.name.clone(), p.plan.size))
                    .collect()
            } else {
                Vec::new()
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_with_retry(
        &self,
        peer: PeerRef,
        reply_to: i32,
        media: tl::enums::InputMedia,
        text: String,
        entities: Vec<tl::enums::MessageEntity>,
        piece: &Piece,
        tmp: &std::path::Path,
        progress: &Arc<Progress>,
    ) -> Result<Message> {
        let mut tries = 0;
        loop {
            let out = InputMessage::new()
                .text(text.clone())
                .fmt_entities(entities.clone())
                .media(media.clone())
                .reply_to(Some(reply_to));
            let send = tokio::time::timeout(
                Duration::from_secs(120),
                self.net.client.send_message(peer, out),
            );
            let res = match send.await {
                Ok(r) => r,
                Err(_) if tries < 3 => {
                    tries += 1;
                    warn!("send_message timed out, retrying ({tries})");
                    progress.note("傳送逾時，重試中");
                    continue;
                }
                Err(_) => bail!("傳送檔案逾時"),
            };
            match res {
                Ok(m) => return Ok(m),
                Err(
                    grammers_client::InvocationError::Io(_)
                    | grammers_client::InvocationError::Dropped(_),
                ) if tries < 3 => {
                    tries += 1;
                    progress.note("傳送時連線中斷，重試中");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) if tries < 3 && e.is("FILE_PART_MISSING") => {
                    tries += 1;
                    let part = match &e {
                        grammers_client::InvocationError::Rpc(r) => r.value.unwrap_or(0),
                        _ => 0,
                    };
                    warn!("server reports part {part} missing, re-uploading");
                    progress.note(format!("伺服器回報缺少分塊 {part}，重新上傳中"));
                    transfer::reupload_part(
                        &self.net,
                        piece.plan,
                        piece.file_id,
                        piece.big,
                        piece.base,
                        tmp,
                        part,
                    )
                    .await?;
                }
                Err(grammers_client::InvocationError::Rpc(r)) if r.code == 420 && tries < 3 => {
                    tries += 1;
                    let s = r.value.unwrap_or(5) as u64;
                    progress.pause_for(Duration::from_secs(s));
                    tokio::time::sleep(Duration::from_secs(s + 1)).await;
                }
                Err(e) => bail!("{e}"),
            }
        }
    }
}

/// One uploaded file (the whole file, or one split piece of it).
struct Piece {
    file_id: i64,
    big: bool,
    base: u64,
    plan: Plan,
    name: String,
}

impl Piece {
    fn input_file(&self) -> tl::enums::InputFile {
        if self.big {
            tl::types::InputFileBig {
                id: self.file_id,
                parts: self.plan.parts as i32,
                name: self.name.clone(),
            }
            .into()
        } else {
            tl::types::InputFile {
                id: self.file_id,
                parts: self.plan.parts as i32,
                name: self.name.clone(),
                md5_checksum: String::new(),
            }
            .into()
        }
    }
}

struct Done {
    new_dc: Option<i32>,
    all_same_dc: bool,
    /// (name, size) of each piece when the file was split; empty otherwise.
    pieces: Vec<(String, u64)>,
}

/// Free bytes available to this process on the filesystem holding `dir`.
#[cfg(unix)]
fn free_space(dir: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path and `st` is a valid out-pointer.
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)]
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

#[cfg(not(unix))]
fn free_space(_dir: &std::path::Path) -> Option<u64> {
    None
}

/// Explain the split and how to join the pieces back together.
fn split_instructions(name: &str, pieces: &[(String, u64)], limit: u64) -> String {
    if pieces.is_empty() {
        return String::new();
    }
    let list: Vec<String> = pieces
        .iter()
        .map(|(n, size)| format!("• {n}（{}）", human_bytes(*size)))
        .collect();
    let quoted: Vec<String> = pieces.iter().map(|(n, _)| format!("\"{n}\"")).collect();
    format!(
        "\n\n🧩 檔案超過此帳號的上傳上限（{}），已分割為 {} 個部分：\n{}\n\n🔧 下載所有部分後合併：\nmacOS / Linux：\ncat {} > \"{name}\"\nWindows（命令提示字元）：\ncopy /b {} \"{name}\"",
        human_bytes(limit),
        pieces.len(),
        list.join("\n"),
        quoted.join(" "),
        quoted.join("+"),
    )
}

fn media_dc(m: &Media) -> Option<i32> {
    match m {
        Media::Photo(p) => p.dc_id(),
        Media::Document(d) => d.dc_id(),
        Media::Sticker(s) => s.document.dc_id(),
        _ => None,
    }
}

pub(crate) fn file_info(media: &Media) -> Option<FileInfo> {
    match media {
        Media::Photo(p) => {
            let location = p.to_raw_input_location()?;
            let tl::enums::Photo::Photo(photo) = p.raw.photo.as_ref()? else {
                return None;
            };
            Some(FileInfo {
                name: format!("photo_{}.jpg", photo.id),
                size: p.size()? as u64,
                dc: photo.dc_id,
                location,
                exact_size: false,
                kind: FileKind::Photo,
            })
        }
        Media::Document(d) => doc_info(d),
        Media::Sticker(s) => doc_info(&s.document),
        _ => None,
    }
}

fn doc_info(d: &grammers_client::media::Document) -> Option<FileInfo> {
    let location = d.to_raw_input_location()?;
    let tl::enums::Document::Document(doc) = d.raw.document.as_ref()? else {
        return None;
    };
    let name = d
        .name()
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            let ext = doc.mime_type.split('/').nth(1).unwrap_or("bin");
            format!("file_{}.{ext}", doc.id)
        });
    Some(FileInfo {
        name,
        size: doc.size as u64,
        dc: doc.dc_id,
        location,
        exact_size: true,
        kind: FileKind::Document {
            mime: doc.mime_type.clone(),
            attributes: doc.attributes.clone(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_message_lists_pieces_and_join_commands() {
        let t = split_instructions(
            "a b.mkv",
            &[("a b.mkv.001".into(), 10), ("a b.mkv.002".into(), 5)],
            2000 * 1024 * 1024,
        );
        assert!(t.contains("2 個部分"));
        assert!(
            t.contains(r#"cat "a b.mkv.001" "a b.mkv.002" > "a b.mkv""#),
            "{t}"
        );
        assert!(
            t.contains(r#"copy /b "a b.mkv.001"+"a b.mkv.002" "a b.mkv""#),
            "{t}"
        );
        assert_eq!(split_instructions("x", &[], 1), "");
    }

    #[test]
    fn message_links() {
        let public = |u: &str, id| {
            Some(MsgLink::Public {
                username: u.into(),
                id,
            })
        };
        assert_eq!(
            parse_link("https://t.me/testnoforward/2"),
            public("testnoforward", 2)
        );
        assert_eq!(
            parse_link(" t.me/testnoforward/2?single "),
            public("testnoforward", 2)
        );
        assert_eq!(
            parse_link("https://t.me/s/testnoforward/2"),
            public("testnoforward", 2)
        );
        assert_eq!(
            parse_link("https://telegram.me/some_chan/10/25"),
            public("some_chan", 25)
        );
        assert_eq!(
            parse_link("https://t.me/c/1234567890/55"),
            Some(MsgLink::Private {
                channel: 1234567890,
                id: 55
            })
        );
        assert_eq!(
            parse_link("https://t.me/c/1234567890/3/55"),
            Some(MsgLink::Private {
                channel: 1234567890,
                id: 55
            })
        );
        assert_eq!(parse_link("https://t.me/testnoforward"), None);
        assert_eq!(parse_link("https://t.me/joinchat/abcdef"), None);
        assert_eq!(parse_link("https://t.me/+abcdef/2"), None);
        assert_eq!(parse_link("https://example.com/a/2"), None);
        assert_eq!(parse_link("see https://t.me/testnoforward/2"), None);
    }

    #[test]
    fn cancel_text() {
        for t in ["/cancel", "/cancel@mvdc_bot", " 取消 ", "Cancel", "/CANCEL"] {
            assert!(is_cancel_text(t), "{t}");
        }
        for t in ["", "取消了", "please cancel", "/cancelled"] {
            assert!(!is_cancel_text(t), "{t}");
        }
    }
}
