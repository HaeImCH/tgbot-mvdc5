//! One-time interactive login for `account = "user"`.
//!
//! The service itself runs non-interactively under systemd, so the phone code and
//! the optional 2FA password are entered once here; the authorized key is stored
//! in the session file and reused by the service afterwards.

use std::io::{self, BufRead, Write};

use anyhow::{Result, anyhow, bail};
use grammers_client::{Client, SignInError};

use crate::config::Config;

fn prompt(question: &str) -> Result<String> {
    print!("{question}");
    io::stdout().flush()?;
    let mut line = String::new();
    if io::stdin().lock().read_line(&mut line)? == 0 {
        bail!("stdin closed");
    }
    Ok(line.trim().to_string())
}

/// Read a line without echoing it (2FA password).
fn prompt_hidden(question: &str) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let fd = io::stdin().as_raw_fd();
        // SAFETY: plain termios calls on stdin with a zeroed, then filled, struct.
        let mut term: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut term) } == 0 {
            let original = term;
            term.c_lflag &= !libc::ECHO;
            unsafe { libc::tcsetattr(fd, libc::TCSANOW, &term) };
            let result = prompt(question);
            unsafe { libc::tcsetattr(fd, libc::TCSANOW, &original) };
            println!();
            return result;
        }
    }
    prompt(question)
}

pub async fn login(client: &Client, cfg: &Config) -> Result<()> {
    if client.is_authorized().await? {
        let me = client.get_me().await?;
        if me.is_bot() {
            bail!(
                "此 session 檔（{}）已登入 bot 帳號，請在設定中改用另一個 session_file",
                cfg.session_file.display()
            );
        }
        println!(
            "✅ 已經登入：{}（id {}）",
            me.full_name(),
            me.id().bare_id_unchecked()
        );
        return Ok(());
    }

    let phone = match cfg.phone.as_deref().filter(|p| !p.trim().is_empty()) {
        Some(p) => p.trim().to_string(),
        None => prompt("請輸入手機號碼（國際格式，例如 +886912345678）：")?,
    };
    let token = client
        .request_login_code(&phone, &cfg.api_hash)
        .await
        .map_err(|e| anyhow!("無法發送驗證碼：{e}"))?;
    println!("📨 驗證碼已發送到你的 Telegram（或簡訊）。");

    let mut attempts = 0;
    let user = loop {
        let code = prompt("請輸入驗證碼：")?;
        match client.sign_in(&token, &code).await {
            Ok(user) => break user,
            Err(SignInError::InvalidCode) if attempts < 2 => {
                attempts += 1;
                println!("❌ 驗證碼錯誤，請再試一次。");
            }
            Err(SignInError::PasswordRequired(mut password_token)) => {
                let mut tries = 0;
                break loop {
                    let hint = password_token.hint().unwrap_or("無").to_string();
                    let pw = prompt_hidden(&format!(
                        "此帳號已啟用兩步驟驗證，請輸入密碼（提示：{hint}）："
                    ))?;
                    match client.check_password(password_token, pw.trim()).await {
                        Ok(user) => break user,
                        Err(SignInError::InvalidPassword(t)) if tries < 2 => {
                            tries += 1;
                            println!("❌ 密碼錯誤，請再試一次。");
                            password_token = t;
                        }
                        Err(e) => bail!("登入失敗：{e}"),
                    }
                };
            }
            Err(SignInError::SignUpRequired) => {
                bail!("此手機號碼尚未註冊 Telegram，請先用官方 App 註冊");
            }
            Err(e) => bail!("登入失敗：{e}"),
        }
    };

    println!(
        "✅ 登入成功：{}（id {}）。session 已儲存到 {}",
        user.full_name(),
        user.id().bare_id_unchecked(),
        cfg.session_file.display()
    );
    Ok(())
}
