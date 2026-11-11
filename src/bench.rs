//! `tgbot_mvdc5 bench <config> <saved_msg_id> <conns> <depth> <chunk_kb> <mb>`
//! `tgbot_mvdc5 bench <config> upload <conns> <depth> <mb>`
//!
//! The upload form uploads <mb> MiB of random bytes to the home DC in 512 KB
//! parts and never attaches them to a message, so nothing is sent anywhere;
//! Telegram discards unattached parts by itself.
//!
//! Downloads the first <mb> MiB of a file in your Saved Messages (user mode) or
//! in the chat with <msg peer> using the given connection layout, then prints
//! throughput and FLOOD_WAIT count. Nothing is uploaded or sent. Runs on a copy
//! of the session file and its own temp file, so the running services are not
//! affected.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use grammers_client::Client;
use grammers_session::storages::SqliteSession;
use grammers_session::types::PeerRef;

use crate::config::Config;
use crate::net::Net;
use crate::status::{Progress, Stage};
use crate::transfer::{self, Direction, Location, Plan};

pub async fn run(
    client: Client,
    session: Arc<SqliteSession>,
    cfg: &Config,
    home_dc: i32,
    me_id: i64,
    max_upload_parts: u64,
    args: &[String],
) -> Result<()> {
    let num = |i: usize, name: &str| -> Result<u64> {
        args.get(i)
            .ok_or_else(|| anyhow!("missing <{name}>"))?
            .parse::<u64>()
            .with_context(|| format!("invalid <{name}>"))
    };
    if args.first().is_some_and(|a| a == "upload") {
        return upload(client, session, cfg, home_dc, me_id, max_upload_parts, &args[1..]).await;
    }
    let msg_id = num(0, "saved_msg_id")? as i32;
    let conns = num(1, "conns")? as usize;
    let depth = num(2, "depth")? as usize;
    let chunk_kb = num(3, "chunk_kb")? as u32;
    let mb = num(4, "mb")?;
    if chunk_kb == 0 || chunk_kb > 512 || !chunk_kb.is_multiple_of(4) || !1024u32.is_multiple_of(chunk_kb) {
        bail!("chunk_kb must be 4..=512, a multiple of 4 dividing 1024");
    }

    if std::env::var_os("BENCH_DC_OPTIONS").is_some() {
        let grammers_tl_types::enums::Config::Config(conf) =
            client.invoke(&grammers_tl_types::functions::help::GetConfig {}).await?;
        for o in conf.dc_options {
            let grammers_tl_types::enums::DcOption::Option(o) = o;
            println!(
                "dc_option DC{} {}:{} ipv6={} media_only={} cdn={} tcpo_only={} static={} this_port_only={}",
                o.id, o.ip_address, o.port, o.ipv6, o.media_only, o.cdn, o.tcpo_only, o.r#static, o.this_port_only
            );
        }
        return Ok(());
    }
    let me = client.get_me().await?;
    let peer: PeerRef = me
        .to_ref()
        .await
        .map_err(|e| anyhow!("{e}"))?
        .ok_or_else(|| anyhow!("cannot reference own user"))?;
    let msg = client
        .get_messages_by_id(peer, &[msg_id])
        .await?
        .pop()
        .flatten()
        .ok_or_else(|| anyhow!("message {msg_id} not found in Saved Messages"))?;
    let media = msg.media().ok_or_else(|| anyhow!("message has no media"))?;
    let info = crate::bot::file_info(&media).ok_or_else(|| anyhow!("media not downloadable"))?;

    let mut c = cfg.clone();
    c.transfer.download_connections = conns;
    c.transfer.pipeline_depth = depth;
    c.transfer.download_chunk_kb = chunk_kb;
    c.warm.enabled = false;
    if std::env::var_os("BENCH_FLOOD_RECONNECT").is_some() {
        c.transfer.flood_wait_strategy = crate::config::PremiumWaitStrategy::Reconnect;
    }
    if std::env::var_os("BENCH_NO_MEDIA_DC").is_some() {
        c.transfer.use_media_dc = false;
    }
    let c = Arc::new(c);
    let net = Arc::new(Net::new(client.clone(), session, c.clone(), home_dc, me_id, max_upload_parts));
    net.load_media_dcs().await;

    let chunk = chunk_kb as u64 * 1024;
    let size = (mb * 1024 * 1024).min(info.size);
    let size = if size == info.size { size } else { size / chunk * chunk };
    let dir = std::env::temp_dir().join("tgbot_bench");
    tokio::fs::create_dir_all(&dir).await?;
    let tmp = dir.join(format!("bench_{msg_id}.part"));
    tokio::fs::File::create(&tmp).await?.set_len(size).await?;

    println!(
        "bench: {} on DC{} | {} MiB | {conns} conn x depth {depth} x {chunk_kb} KB = {} KB in flight | on FLOOD_WAIT: {:?}",
        info.name,
        info.dc,
        size / 1048576,
        conns as u64 * depth as u64 * chunk_kb as u64,
        c.transfer.flood_wait_strategy
    );
    // Authorize all slots first so the measurement is transfer-only.
    let t_auth = Instant::now();
    for slot in 0..conns {
        net.auth_key(info.dc, slot).await?;
    }
    println!("bench: {conns} authorization(s) ready in {:.1}s", t_auth.elapsed().as_secs_f64());

    // CDN probe: does Telegram redirect this file to a CDN DC (like Telegram
    // Desktop gets with cdn_supported = true)?
    if std::env::var_os("BENCH_CDN_PROBE").is_some() {
        let mut s = net.open(info.dc, 0).await?;
        let r = s
            .invoke(&grammers_tl_types::functions::upload::GetFile {
                precise: false,
                cdn_supported: true,
                location: info.location.clone(),
                offset: 0,
                limit: chunk as i32,
            })
            .await;
        match r {
            Ok(grammers_tl_types::enums::upload::File::CdnRedirect(c)) => println!(
                "bench: CDN PROBE -> REDIRECT to CDN DC{} ({} file hashes, token {} bytes)",
                c.dc_id,
                c.file_hashes.len(),
                c.file_token.len()
            ),
            Ok(grammers_tl_types::enums::upload::File::File(f)) => {
                println!("bench: CDN PROBE -> no redirect, served directly ({} bytes)", f.bytes.len())
            }
            Err(e) => println!("bench: CDN PROBE -> error {e}"),
        }
        let _ = tokio::fs::remove_file(&tmp).await;
        return Ok(());
    }

    let progress = Arc::new(Progress::new(info.name.clone(), size, info.dc, home_dc));
    progress.set_stage(Stage::Downloading { dc: info.dc });
    let loc = info.location.clone();
    let location = Arc::new(Location::new(
        info.location,
        Box::new(move || {
            let l = loc.clone();
            Box::pin(async move { Ok(l) })
        }),
    ));
    let t = Instant::now();
    let res = transfer::run(
        net,
        info.dc,
        conns,
        Plan::new(size, chunk),
        Direction::Download {
            location,
            exact_size: size == info.size && info.exact_size,
        },
        tmp.clone(),
        progress,
    )
    .await;
    let secs = t.elapsed().as_secs_f64();
    let _ = tokio::fs::remove_file(&tmp).await;
    res?;
    println!(
        "bench: RESULT {conns}x{depth}x{chunk_kb}KB -> {:.2} MB/s ({:.0} Mbps) in {secs:.1}s (FLOOD_WAIT count is in the log line above)",
        size as f64 / secs / 1048576.0,
        size as f64 * 8.0 / secs / 1e6
    );
    Ok(())
}

async fn upload(
    client: Client,
    session: Arc<SqliteSession>,
    cfg: &Config,
    home_dc: i32,
    me_id: i64,
    max_upload_parts: u64,
    args: &[String],
) -> Result<()> {
    let num = |i: usize, name: &str| -> Result<u64> {
        args.get(i)
            .ok_or_else(|| anyhow!("missing <{name}>"))?
            .parse::<u64>()
            .with_context(|| format!("invalid <{name}>"))
    };
    let conns = num(0, "conns")? as usize;
    let depth = num(1, "depth")? as usize;
    let mb = num(2, "mb")?;
    if conns == 0 || depth == 0 || mb == 0 {
        bail!("conns, depth and mb must be > 0");
    }
    let mut c = cfg.clone();
    c.transfer.upload_connections = conns;
    c.transfer.pipeline_depth = depth;
    c.warm.enabled = false;
    let c = Arc::new(c);
    let net = Arc::new(Net::new(client, session, c.clone(), home_dc, me_id, max_upload_parts));

    let size = mb * 1024 * 1024;
    let dir = std::env::temp_dir().join("tgbot_bench");
    tokio::fs::create_dir_all(&dir).await?;
    let tmp = dir.join(format!("bench_upload_{}.part", std::process::id()));
    {
        use tokio::io::AsyncWriteExt;
        let mut f = tokio::fs::File::create(&tmp).await?;
        let mut buf = vec![0u8; 1024 * 1024];
        for _ in 0..mb {
            rand::fill(&mut buf[..]);
            f.write_all(&buf).await?;
        }
        f.flush().await?;
    }
    println!(
        "bench: upload {mb} MiB random data to DC{home_dc} | {conns} conn x depth {depth} x 512 KB = {} KB in flight",
        conns as u64 * depth as u64 * 512
    );
    let progress = Arc::new(Progress::new("bench_upload".into(), size, home_dc, home_dc));
    progress.set_stage(Stage::Uploading { dc: home_dc });
    let t = Instant::now();
    let res = transfer::run(
        net,
        home_dc,
        conns,
        Plan::new(size, transfer::UPLOAD_PART),
        Direction::Upload { file_id: rand::random(), big: size > transfer::BIG_FILE, base: 0 },
        tmp.clone(),
        progress,
    )
    .await;
    let secs = t.elapsed().as_secs_f64();
    let _ = tokio::fs::remove_file(&tmp).await;
    res?;
    println!(
        "bench: RESULT upload {conns}x{depth}x512KB -> {:.2} MB/s ({:.0} Mbps) in {secs:.1}s",
        size as f64 / secs / 1048576.0,
        size as f64 * 8.0 / secs / 1e6
    );
    Ok(())
}
