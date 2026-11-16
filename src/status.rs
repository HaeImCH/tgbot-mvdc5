//! Shared job progress and the zh_TW status-message renderer/reporter.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use grammers_client::Client;
use grammers_client::message::{InputMessage, ReplyMarkup};
use grammers_session::types::PeerRef;
use grammers_tl_types as tl;
use tracing::debug;

#[derive(Clone, Debug, PartialEq)]
pub enum Stage {
    Queued { ahead: usize },
    Connecting { dc: i32 },
    Downloading { dc: i32 },
    Uploading { dc: i32 },
    Sending,
}

pub struct Progress {
    pub file_name: String,
    pub src_dc: i32,
    pub dst_dc: i32,
    stage: Mutex<(Stage, Instant)>,
    bytes: AtomicU64,
    total: AtomicU64,
    pub active_conns: AtomicUsize,
    pause_until: Mutex<Option<Instant>>,
    /// Latest per-connection FLOOD hold end (connections with their own
    /// authorization wait individually). Only a hint for the stall watchdog.
    hold_until: Mutex<Option<Instant>>,
    note: Mutex<Option<(String, Instant)>>,
    pub started: Instant,
    /// Finished transfer stages: (is_download, dc, bytes, duration).
    finished: Mutex<Vec<(bool, i32, u64, Duration)>>,
    /// (current piece, total pieces) when the file is being split.
    piece: Mutex<Option<(usize, usize)>>,
}

impl Progress {
    pub fn new(file_name: String, size: u64, src_dc: i32, dst_dc: i32) -> Self {
        Self {
            file_name,
            src_dc,
            dst_dc,
            stage: Mutex::new((Stage::Queued { ahead: 0 }, Instant::now())),
            bytes: AtomicU64::new(0),
            total: AtomicU64::new(size),
            active_conns: AtomicUsize::new(0),
            pause_until: Mutex::new(None),
            hold_until: Mutex::new(None),
            note: Mutex::new(None),
            started: Instant::now(),
            finished: Mutex::new(Vec::new()),
            piece: Mutex::new(None),
        }
    }

    pub fn set_stage(&self, stage: Stage) {
        let mut s = self.stage.lock().unwrap();
        if s.0 != stage {
            // Remember the average speed of the transfer stage we are leaving.
            let leaving = match s.0 {
                Stage::Downloading { dc } => Some((true, dc)),
                Stage::Uploading { dc } => Some((false, dc)),
                _ => None,
            };
            if let Some((is_dl, dc)) = leaving {
                let bytes = self.bytes.load(Ordering::Relaxed);
                self.finished
                    .lock()
                    .unwrap()
                    .push((is_dl, dc, bytes, s.1.elapsed()));
            }
            let reset = !matches!((&s.0, &stage), (Stage::Queued { .. }, Stage::Queued { .. }));
            *s = (stage, Instant::now());
            if reset {
                self.bytes.store(0, Ordering::Relaxed);
            }
        }
    }

    pub fn set_piece(&self, current: usize, total: usize) {
        *self.piece.lock().unwrap() = (total > 1).then_some((current, total));
    }

    pub fn stage(&self) -> (Stage, Instant) {
        self.stage.lock().unwrap().clone()
    }

    pub fn add_bytes(&self, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Pause all workers of this job until at least `now + d`.
    pub fn pause_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut p = self.pause_until.lock().unwrap();
        if p.is_none_or(|old| old < until) {
            *p = Some(until);
        }
    }
    pub fn paused_until(&self) -> Option<Instant> {
        let p = *self.pause_until.lock().unwrap();
        p.filter(|t| *t > Instant::now())
    }
    pub fn hold_hint(&self, until: Instant) {
        let mut h = self.hold_until.lock().unwrap();
        if h.is_none_or(|old| old < until) {
            *h = Some(until);
        }
    }
    /// True while at least one connection is in a FLOOD_WAIT hold.
    pub fn holding(&self) -> bool {
        self.hold_until
            .lock()
            .unwrap()
            .is_some_and(|t| t > Instant::now())
    }

    pub async fn wait_pause(&self) {
        while let Some(t) = self.paused_until() {
            tokio::time::sleep_until(t.into()).await;
        }
    }

    /// Transient note shown for ~20 s (rate limit, reconnect, …).
    pub fn note(&self, text: impl Into<String>) {
        *self.note.lock().unwrap() = Some((text.into(), Instant::now()));
    }
    fn current_note(&self) -> Option<String> {
        let n = self.note.lock().unwrap();
        n.as_ref()
            .filter(|(_, at)| at.elapsed() < Duration::from_secs(20))
            .map(|(t, _)| t.clone())
    }

    /// One line per finished stage, e.g. "📥 下載（DC4）：平均 12.3 MB/s，用時 1 分 5 秒".
    pub fn speed_summary(&self) -> String {
        self.finished
            .lock()
            .unwrap()
            .iter()
            .map(|(is_dl, dc, bytes, d)| {
                let avg = *bytes as f64 / d.as_secs_f64().max(0.001);
                let (icon, label) = if *is_dl {
                    ("📥", "下載")
                } else {
                    ("📤", "上傳")
                };
                format!(
                    "{icon} {label}速度（DC{dc}）：平均 {}/s，用時 {}",
                    human_bytes(avg as u64),
                    human_duration(d.as_secs())
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn render(&self, speed_bps: Option<f64>) -> String {
        let (stage, since) = self.stage();
        let mut out = String::new();
        let header = match &stage {
            Stage::Queued { ahead } if *ahead > 0 => {
                format!("⏳ 排隊中（前面還有 {ahead} 個任務）")
            }
            Stage::Queued { .. } => "⏳ 排隊中…".to_string(),
            Stage::Connecting { dc } => format!("🔌 正在連線到 DC{dc} 並取得授權…"),
            Stage::Downloading { dc } => format!("📥 正在從 DC{dc} 下載"),
            Stage::Uploading { dc } => format!("📤 正在上傳到 DC{dc}"),
            Stage::Sending => "📨 上傳完成，正在傳送檔案…".to_string(),
        };
        out.push_str(&header);
        if let Some((i, n)) = *self.piece.lock().unwrap() {
            match stage {
                Stage::Uploading { .. } => {
                    out.push_str(&format!("\n🧩 檔案超過上傳上限，分割上傳：第 {i}/{n} 部分"))
                }
                Stage::Sending => out.push_str(&format!("\n🧩 正在傳送第 {i}/{n} 部分")),
                _ => {}
            }
        }
        out.push_str(&format!(
            "\n\n📄 檔案：{}\n📦 大小：{}\n🔀 路徑：DC{} → DC{}",
            self.file_name,
            human_bytes(self.total()),
            self.src_dc,
            self.dst_dc
        ));

        if matches!(stage, Stage::Downloading { .. } | Stage::Uploading { .. }) {
            let done = self.bytes();
            let total = self.total().max(1);
            let pct = (done as f64 / total as f64 * 100.0).min(100.0);
            let elapsed = since.elapsed().as_secs_f64().max(0.001);
            let avg = done as f64 / elapsed;
            let speed = speed_bps.unwrap_or(avg);
            let label = if matches!(stage, Stage::Downloading { .. }) {
                "⬇️ 下載速度"
            } else {
                "⬆️ 上傳速度"
            };
            out.push_str(&format!(
                "\n\n{} {pct:.1}%\n{} / {}\n{label}：{}/s（平均 {}/s）",
                bar(pct),
                human_bytes(done),
                human_bytes(total),
                human_bytes(speed as u64),
                human_bytes(avg as u64),
            ));
            if avg > 0.0 && done < total {
                let eta = (total - done) as f64 / avg;
                out.push_str(&format!("\n⏱ 剩餘時間：{}", human_duration(eta as u64)));
            }
            out.push_str(&format!(
                "\n🔗 連線數：{}",
                self.active_conns.load(Ordering::Relaxed)
            ));
        }

        let summary = self.speed_summary();
        if !summary.is_empty() {
            out.push_str(&format!("\n\n✅ 已完成階段\n{summary}"));
        }

        if let Some(until) = self.paused_until() {
            let secs = until.saturating_duration_since(Instant::now()).as_secs() + 1;
            out.push_str(&format!(
                "\n\n🚦 觸發 Telegram 限流，暫停中，約 {secs} 秒後繼續"
            ));
        } else if let Some(note) = self.current_note() {
            out.push_str(&format!("\n\n⚠️ {note}"));
        }
        out.push_str(&format!(
            "\n\n🕒 已耗時：{}",
            human_duration(self.started.elapsed().as_secs())
        ));
        out
    }
}

/// Periodically edits the status message. Abort the task to stop it.
///
/// `markup` (the cancel button) is re-sent on every edit, because an edit
/// without a reply markup removes the keyboard. `footer` is appended to the
/// rendered text (the cancel hint in user mode, where buttons are not allowed).
pub async fn run_reporter(
    client: Client,
    peer: PeerRef,
    msg_id: i32,
    progress: std::sync::Arc<Progress>,
    interval: Duration,
    markup: Option<tl::enums::ReplyMarkup>,
    footer: &'static str,
) {
    let mut last_text = String::new();
    let mut last_sample: Option<(Instant, u64, Stage)> = None;
    let mut blocked_until: Option<Instant> = None;
    loop {
        tokio::time::sleep(interval).await;
        let now = Instant::now();
        let (stage, _) = progress.stage();
        let bytes = progress.bytes();
        let speed = match &last_sample {
            Some((t, b, s)) if *s == stage && bytes >= *b => {
                Some((bytes - b) as f64 / now.duration_since(*t).as_secs_f64().max(0.001))
            }
            _ => None,
        };
        last_sample = Some((now, bytes, stage));

        if blocked_until.is_some_and(|t| t > now) {
            continue;
        }
        let text = progress.render(speed) + footer;
        if text == last_text {
            continue;
        }
        match edit(&client, peer, msg_id, &text, markup.as_ref()).await {
            EditResult::Ok => last_text = text,
            EditResult::Flood(secs) => blocked_until = Some(now + Duration::from_secs(secs)),
            EditResult::Err => {}
        }
    }
}

pub enum EditResult {
    Ok,
    Flood(u64),
    Err,
}

/// Edit a message; never fails the job (MESSAGE_NOT_MODIFIED / flood are swallowed).
/// Without `markup`, any inline keyboard on the message is removed.
pub async fn edit(
    client: &Client,
    peer: PeerRef,
    msg_id: i32,
    text: &str,
    markup: Option<&tl::enums::ReplyMarkup>,
) -> EditResult {
    let mut m = InputMessage::new().text(text);
    if let Some(raw) = markup {
        m = m.reply_markup(ReplyMarkup { raw: raw.clone() });
    }
    match client.edit_message(peer, msg_id, m).await {
        Ok(()) => EditResult::Ok,
        Err(e) if e.is("MESSAGE_NOT_MODIFIED") => EditResult::Ok,
        Err(grammers_client::InvocationError::Rpc(r)) if r.code == 420 => {
            EditResult::Flood(r.value.unwrap_or(10) as u64)
        }
        Err(e) => {
            debug!("edit status message failed: {e}");
            EditResult::Err
        }
    }
}

fn bar(pct: f64) -> String {
    let filled = ((pct / 10.0).round() as usize).min(10);
    format!("{}{}", "▓".repeat(filled), "░".repeat(10 - filled))
}

pub fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.2} {}", UNITS[i])
    }
}

pub fn human_duration(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if h > 0 {
        format!("{h} 小時 {m} 分 {s} 秒")
    } else if m > 0 {
        format!("{m} 分 {s} 秒")
    } else {
        format!("{s} 秒")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.50 KB");
        assert_eq!(human_duration(65), "1 分 5 秒");
        assert_eq!(human_duration(3725), "1 小時 2 分 5 秒");
        assert_eq!(bar(45.0), "▓▓▓▓▓░░░░░");
    }

    #[test]
    fn records_download_and_upload_stages() {
        let p = Progress::new("f".into(), 100, 4, 5);
        p.set_stage(Stage::Downloading { dc: 4 });
        p.add_bytes(100);
        p.set_stage(Stage::Uploading { dc: 5 });
        assert_eq!(p.bytes(), 0, "bytes reset for the next stage");
        p.add_bytes(100);
        p.set_stage(Stage::Sending);
        let s = p.speed_summary();
        assert!(s.contains("下載速度（DC4）"), "{s}");
        assert!(s.contains("上傳速度（DC5）"), "{s}");
        assert_eq!(s.lines().count(), 2);
    }
}
