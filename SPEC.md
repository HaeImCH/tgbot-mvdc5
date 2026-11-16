# Spec: Media-server indicator — V1

Status: approved

## Objective

When a file is moved, the bot downloads it over several connections. Each one
goes either to the source DC's **media-only address** (`use_media_dc`, from
`help.getConfig`, the same as Telegram Desktop's media cluster, about 4× faster)
or to the regular API address. Today nobody can see which. The bot falls back
to the regular address without saying so when the media address fails
(`media_broken`), when the DC has no media address, or when
`use_media_dc = false`. When a download is slow, there is no way to tell whether
that is the reason.

**Users:** the bot operator and the people who send files (bot and user mode).

**What we're building:** a per-connection indicator showing how many of a
download's connections use the media server. It appears in three places:

1. the **live status message** during the download stage
2. the **final 🎉 summary**, on the 📥 download line
3. the **server log**, on the existing `DC{dc} download: …` line

"Media server" means the media-only DC address. It does **not** mean the CDN
DCs (`upload.fileCdnRedirect`).

## Assumptions

1. The indicator applies only to **downloads**. Uploads always use the regular
   address, so the upload display stays exactly as it is today: no "0 media" text.
2. Downloads from the home DC (`reupload_same_dc`) use the media address too
   (`take_download` → `open_conn(download = true)`), so they get the indicator.
3. A connection's kind is decided by the **address actually dialed** when it was
   opened. It is never re-derived later from `media_addr()` or `media_broken`.
   A warm connection opened before `load_media_dcs` ran, or before the DC was
   marked broken, must report what it really is.
4. No config option: the indicator is always shown.

## Design

### Tagging connections (`src/net.rs`)

- `open_at` returns whether the address it dialed was the media address.
  `server_addr` can fall back to the regular address when the DC is missing
  from the media map, so `open_at` uses the address `server_addr` resolved,
  not the `media` argument.
- New type `pub struct Conn { pub sender: RawSender, pub via_media: bool }`.
  - `open`, `take` and `take_download` return `Conn` instead of `RawSender`.
  - `give_back(dc, slot, conn: Conn)` takes a `Conn`.
  - The warm pool stores `(slot, Conn)`, so a connection keeps its tag across
    keepalive pings and reuse.
- Call sites that need updating:
  - `transfer.rs` ≈ lines 355–361: worker connect
  - `transfer.rs` ≈ line 592: `give_back`
  - `transfer.rs` ≈ line 719: `net.take(home_dc, 0)`
  - `bench.rs:122`: `net.open`
  - `keepalive_loop`: ping and `give_back`
  - Any caller that only needs the sender uses `.sender`.

### Counting (`src/status.rs` / `src/transfer.rs`)

- `Progress` gains `media_conns: AtomicUsize`, which moves in lockstep with
  `active_conns`. Every place that changes `active_conns` changes `media_conns`
  too when the connection is a media one:
  - `fetch_add` on connect (≈ line 361)
  - `drop_conn` (≈ line 339)
  - the worker-exit decrement (≈ line 588)
  - `store(0)` after a stall restart (≈ line 255)

  To make this possible, the worker keeps `via_media` alongside its `conn`.
- Per transfer stage, `Progress` also counts **acquisitions by kind**: every
  successful `take`/`take_download` during the stage adds one to either `media`
  or `regular`.
  - Both counts reset when the stage changes, like `bytes`.
  - When the stage ends, they are stored in the `finished` tuple:
    `(is_download, dc, bytes, duration, media_acq, regular_acq)`.
- Definition of the final-summary number: the number of connection
  **acquisitions** during the download stage, split by kind. Reconnects and
  stall restarts count again. This is not the peak number of concurrent
  connections.

### Exact display strings (zh-TW)

Live status, download stage only. These replace today's `🔗 連線數：N` line:

| State | Text |
|---|---|
| all active connections are media (N > 0) | `🔗 連線數：4（媒體伺服器 4）` |
| mixed | `🔗 連線數：4（媒體伺服器 3／一般 1）` |
| none are media (N > 0) | `🔗 連線數：4（一般位址，未使用媒體伺服器）` |
| N = 0 (connecting / restarting) | `🔗 連線數：0` (unchanged) |

Upload stage: `🔗 連線數：N`, unchanged.

Final summary and the "已完成階段" list: a suffix on the 📥 line only. The 📤 line
is unchanged.

| State | 📥 line |
|---|---|
| all acquisitions media | `📥 下載速度（DC4）：平均 50.00 MB/s，用時 20 秒（媒體伺服器）` |
| mixed | `📥 下載速度（DC4）：平均 20.00 MB/s，用時 1 分 5 秒（媒體伺服器 3／一般 2 條連線）` |
| none media | `📥 下載速度（DC4）：平均 13.00 MB/s，用時 2 分 0 秒（未使用媒體伺服器）` |

Server log: the `DC{dc} download: …` line (≈ `transfer.rs:284`) gets
`, media conns: {media}/{media+regular}` appended. Upload log lines are unchanged.

## Tech Stack

Rust 2024; tokio; grammers 0.12 (with the vendored `grammers-mtsender`). No new
dependencies.

## Commands

Rust builds on this machine go through the mbx cache, so plain `cargo` is fine.
If `which cargo` is not the mbx shim, prefix the command with `mbx`.

```
Build:  cargo build --release
Test:   cargo test
Lint:   cargo clippy --all-targets -- -D warnings
Bench (manual check):   cargo run --release -- bench …   (see src/bench.rs for args)
Bench without media:    BENCH_NO_MEDIA_DC=1 cargo run --release -- bench …
Deploy: cross build --release --target x86_64-unknown-linux-gnu, then copy to the server
        (per the existing deploy procedure; never build on the server)
```

Never `cargo clean`, `rm -rf target`, or set `CARGO_TARGET_DIR`/`RUSTC_WRAPPER`.

## Project Structure

```
src/net.rs       → connection pool: Conn type, tagging, warm pool
src/transfer.rs  → workers: count media/regular, log line
src/status.rs    → Progress counters, render(), speed_summary(), unit tests
src/bench.rs     → adapt to Conn
src/bot.rs       → final summary (uses speed_summary; likely no change)
vendor/          → do not touch
```

## Code Style

Follow the existing code. Comments and log messages are in English;
user-facing text is in zh-TW. Use atomics with `Ordering::Relaxed` for the
counters, the same as `active_conns`. Keep each change small. For example:

```rust
Ok(c) => {
    ctx.progress.conn_acquired(c.via_media);   // bumps active (+media) and stage acquisitions
    via_media = c.via_media;
    conn = Some(c.sender);
    last_response = Instant::now();
}
```

## Testing Strategy

Unit tests go in the existing `#[cfg(test)] mod tests` in `src/status.rs`:

- `render` during download with all-media, mixed, no-media and zero connections
  produces exactly the strings in the table above.
- `render` during upload contains no `媒體伺服器` text.
- `speed_summary` puts the media suffix on the 📥 line only, and the counts reset
  between stages.
- Counter invariant: after arbitrary acquire/drop sequences, media ≤ active, and
  both are 0 after the restart reset.

Network tagging cannot be unit-tested, so it is checked by hand:

- Run bench on a foreign-DC file normally: the log shows media conns `N/N`.
- Run it with `BENCH_NO_MEDIA_DC=1`: the log shows `0/N`.
- Send a real file to the bot and check the live message and the final summary.

## Boundaries

- **Always:** run `cargo test` and `cargo clippy` before committing; keep the
  counters in lockstep with `active_conns`; keep user-facing strings in zh-TW.
- **Ask first:** changing the existing status-message layout beyond the lines
  listed above; adding config options; any deploy to the server.
- **Never:** edit `vendor/`; derive a connection's kind after the fact from
  `media_broken`/`media_addr()`; change upload connection routing.

## Success Criteria

1. A download from a foreign DC with working media addresses shows
   `🔗 連線數：N（媒體伺服器 N）` live, `（媒體伺服器）` on the final 📥 line, and
   `media conns: N/N` in the log.
2. With `use_media_dc = false`, or `BENCH_NO_MEDIA_DC=1`, all three show the
   no-media variant.
3. A warm connection opened with the regular address before `load_media_dcs`
   finished is counted as regular, not media.
4. After a media-address failure (`media_broken`) in the middle of a job, newly
   opened connections count as regular. Mixed counts show correctly.
5. A home-DC download (`reupload_same_dc = true`) shows the indicator.
6. Upload-stage text and log lines are byte-for-byte unchanged.
7. media ≤ active at all times, and both are 0 after a stall restart.
8. `cargo test` and `cargo clippy -D warnings` pass. Throughput is unchanged:
   no extra locks or RPCs on the hot path.

## Out of Scope (Deferred)

- A transient `note()` such as "媒體伺服器連線失敗，已改用一般位址" when the
  fallback happens. Useful, but nobody asked for it.
- Downloading through Telegram CDN DCs (`fileCdnRedirect`). That is a separate,
  much bigger feature.
- An indicator for upload connections. They never use the media address.

## Open Questions

1. Is the "acquisitions" definition right for the final summary, or would you
   rather see the peak number of concurrent media/regular connections?
2. Are the zh-TW wording choices (`媒體伺服器`, `一般位址`, `未使用媒體伺服器`)
   acceptable?
