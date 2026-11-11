mod bench;
mod bot;
mod config;
mod login;
mod net;
mod status;
mod transfer;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use grammers_client::Client;
use grammers_client::update::Update;
use grammers_mtsender::{ConnectionParams, SenderPool, UpdatesConfiguration};
use grammers_session::Session;
use grammers_session::storages::SqliteSession;
use grammers_tl_types as tl;
use tracing::{error, info, warn};

use crate::config::{AccountType, Config};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    // updates_receiver warns for every channel a user account is in but
                    // has not cached ("missing its hash"); harmless, we ignore channels.
                    "info,grammers_mtsender=warn,grammers_mtsender::updates_receiver=error,grammers_client=warn,grammers_session=warn".into()
                }),
        )
        .with_ansi(false)
        .init();

    // Usage: tgbot_mvdc5 [login] [config.toml]
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let login_mode = args.first().is_some_and(|a| a == "login");
    let bench_mode = args.first().is_some_and(|a| a == "bench");
    if login_mode || bench_mode {
        args.remove(0);
    }
    let cfg_path: PathBuf = args
        .first()
        .cloned()
        .unwrap_or_else(|| "config.toml".into())
        .into();
    let mut cfg = Config::load(&cfg_path)?;
    if bench_mode {
        // Never touch the live session or temp dir of a running service.
        let copy = std::env::temp_dir().join(format!("tgbot_bench_{}.session", std::process::id()));
        std::fs::copy(&cfg.session_file, &copy).context("copy session for bench")?;
        cfg.session_file = copy;
    } else if !login_mode {
        sweep_temp_dir(&cfg.temp_dir);
    }
    let cfg = Arc::new(cfg);

    let session = Arc::new(
        SqliteSession::open(&cfg.session_file)
            .await
            .context("open session file")?,
    );
    let params = ConnectionParams {
        proxy_url: cfg.proxy.clone(),
        use_ipv6: cfg.use_ipv6,
        ..Default::default()
    };
    let SenderPool {
        runner,
        updates,
        handle,
    } = SenderPool::with_configuration(Arc::clone(&session), cfg.api_id, params);
    let client = Client::new(handle.clone());
    let pool_task = tokio::spawn(runner.run());

    if let Some(p) = &cfg.proxy {
        info!("using SOCKS5 proxy {}", redact_proxy(p));
    }
    if login_mode {
        let result = match cfg.account {
            AccountType::User => login::login(&client, &cfg).await,
            AccountType::Bot => Err(anyhow::anyhow!(
                "login 指令只用於 account = \"user\"；bot 模式會在啟動時自動用 token 登入"
            )),
        };
        handle.quit();
        let _ = pool_task.await;
        return result;
    }

    if !client.is_authorized().await? {
        match cfg.account {
            AccountType::Bot => {
                info!("signing in as bot…");
                client.bot_sign_in(cfg.token(), &cfg.api_hash).await?;
            }
            AccountType::User => {
                error!(
                    "user account is not logged in yet; run once interactively: tgbot_mvdc5 login {}",
                    cfg_path.display()
                );
                // EX_CONFIG: systemd unit uses RestartPreventExitStatus=78 so this
                // does not turn into a restart loop before the first login.
                std::process::exit(78);
            }
        }
    }
    let me = client.get_me().await?;
    if me.is_bot() != (cfg.account == AccountType::Bot) {
        anyhow::bail!(
            "session {} belongs to a {} account but config says account = {:?}; use a separate session_file",
            cfg.session_file.display(),
            if me.is_bot() { "bot" } else { "user" },
            cfg.account
        );
    }
    let home_dc = session.home_dc_id().map_err(|e| anyhow::anyhow!("{e}"))?;
    let premium = matches!(&me.raw, tl::enums::User::User(u) if u.premium);
    // Upload limit: 4000 x 512 KB (2000 MiB) normally, 8000 parts (4000 MiB) with Premium.
    let max_upload_parts = if premium {
        transfer::PREMIUM_UPLOAD_PARTS
    } else {
        transfer::MAX_UPLOAD_PARTS
    };
    info!(
        "signed in as {} @{} (id {}), home DC{home_dc}, premium={premium}, upload limit {} MiB",
        if me.is_bot() { "bot" } else { "user" },
        me.username().unwrap_or("?"),
        me.id().bare_id_unchecked(),
        max_upload_parts / 2
    );
    if cfg.account == AccountType::User {
        if cfg.allowed_user_ids.is_empty() {
            info!("user mode: only files you send/forward to your own Saved Messages are processed");
        } else {
            info!(
                "user mode: processing Saved Messages and private chats from {:?}",
                cfg.allowed_user_ids
            );
        }
    }

    if bench_mode {
        let r = bench::run(
            client.clone(),
            session.clone(),
            &cfg,
            home_dc,
            me.id().bare_id_unchecked(),
            max_upload_parts,
            &args[1..],
        )
        .await;
        handle.quit();
        let _ = pool_task.await;
        let _ = std::fs::remove_file(&cfg.session_file);
        return r;
    }

    let net = Arc::new(net::Net::new(
        client.clone(),
        session.clone(),
        cfg.clone(),
        home_dc,
        me.id().bare_id_unchecked(),
        max_upload_parts,
    ));
    net.load_media_dcs().await;
    if cfg.warm.enabled {
        info!(
            "warm pool: keeping {} connection(s) to DC{:?}",
            cfg.warm.connections_per_dc, cfg.warm.dcs
        );
        tokio::spawn(net.clone().keepalive_loop());
    }
    let bot = Arc::new(bot::Bot::new(net));

    // Home-pool health check: the pool's runner has no read timeout, so a
    // half-open proxied socket would silently freeze updates. Ping under a
    // timeout and force a reconnect when it fails.
    {
        let client = client.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            let mut fails = 0u32;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                // updates.getState is an API call, so a freshly reconnected home
                // connection gets subscribed to updates again (Ping would not).
                let probe = tl::functions::updates::GetState {};
                match tokio::time::timeout(Duration::from_secs(20), client.invoke(&probe)).await {
                    Ok(Ok(_)) => fails = 0,
                    other => {
                        fails += 1;
                        warn!("home DC health check failed ({fails}): {other:?}; forcing reconnect");
                        handle.disconnect_from_dc(home_dc);
                    }
                }
            }
        });
    }

    let mut stream = client
        .stream_updates(updates, UpdatesConfiguration { catch_up: false })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    info!("waiting for messages…");
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
            update = stream.next() => match update {
                Ok(Update::NewMessage(msg)) => {
                    tokio::spawn(bot.clone().handle(msg.into_inner()));
                }
                Ok(Update::CallbackQuery(q)) => {
                    tokio::spawn(bot.clone().on_callback(q));
                }
                Ok(_) => {}
                Err(e) => {
                    error!("update error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    info!("shutting down…");
    bot.shutdown().await;
    let _ = stream.sync_update_state().await;
    handle.quit();
    let _ = pool_task.await;
    Ok(())
}

/// Remove temp files left behind by a crash or kill (each service has its own temp_dir).
fn sweep_temp_dir(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let (mut n, mut bytes) = (0u32, 0u64);
    for e in entries.flatten() {
        let path = e.path();
        if path.extension().is_some_and(|x| x == "part") {
            bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(&path) {
                Ok(()) => n += 1,
                Err(err) => warn!("cannot remove leftover {}: {err}", path.display()),
            }
        }
    }
    if n > 0 {
        info!("removed {n} leftover temp file(s) ({}) from {}", status::human_bytes(bytes), dir.display());
    }
}

fn redact_proxy(p: &str) -> String {
    match (p.find("://"), p.rfind('@')) {
        (Some(s), Some(at)) if at > s => format!("{}***{}", &p[..s + 3], &p[at..]),
        _ => p.to_string(),
    }
}
