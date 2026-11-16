# Tasks: Media-server indicator (SPEC.md)

## Task 1: Tag connections with the address actually dialed

**Description:** Add `pub struct Conn { pub sender: RawSender, pub via_media: bool }`
to `net.rs`.
- `open_at` reports whether the address it dialed (as resolved by `server_addr`)
  was the media address.
- `open_conn` passes that through, including after a fallback to the regular
  address.
- `open`, `take` and `take_download` return `Conn`; `give_back` takes `Conn`.
- The warm pool stores `(slot, Conn)`; `keepalive_loop` pings `conn.sender`.
- Update the call sites in `transfer.rs` (worker connect ≈355, give_back ≈592,
  `net.take(home_dc, 0)` ≈719) and `bench.rs:122` to compile. The worker keeps
  `via_media` but does not use it yet.

**Acceptance criteria:**
- [x] The kind comes only from the dialed address, never from `media_addr()` or
      `media_broken` after the fact.
- [x] A warm connection keeps its tag through keepalive and reuse.
- [x] No behavior change: same addresses, same pool rules.

**Verification:**
- [x] `cargo build --release` and `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test`

**Dependencies:** None

**Files:** `src/net.rs`, `src/transfer.rs`, `src/bench.rs`

**Scope:** M

## Task 2: Progress media counters and rendering (TDD)

**Description:** Write failing tests in `status.rs` first, then implement.

- **`Progress` state:**
  - `media_conns: AtomicUsize`
  - per-stage `stage_media_acq` / `stage_regular_acq` atomics, reset in `set_stage`
  - the `finished` tuple extended with `(media_acq, regular_acq)`
- **Methods:**
  - `conn_acquired(media)`: active +1, media +1 if media, stage acquisition +1
  - `conn_dropped(media)`
  - `reset_conns()`: both counters to 0
- **`render`:** during the download stage, the `🔗 連線數` line follows the SPEC
  table. The upload stage is unchanged.
- **`speed_summary`:** the 📥 line gets the SPEC suffix; 📤 is unchanged.
- Make `active_conns` private if nothing outside needs it after Task 3.
  Otherwise keep a read accessor.

**Acceptance criteria:**
- [x] Tests cover the all-media, mixed, no-media and zero-connection live strings;
      the upload stage has no `媒體伺服器`; the 📥 suffix in each of 3 states; 📤
      unchanged; acquisitions reset between stages.
- [x] Invariant test: media ≤ active after acquire/drop sequences; both are 0
      after `reset_conns()`.

**Verification:**
- [x] `cargo test` (new tests red first, then green)
- [x] `cargo clippy --all-targets -- -D warnings`

**Dependencies:** None (independent of Task 1)

**Files:** `src/status.rs`

**Scope:** S

## Checkpoint: Foundation
- [x] All tests pass, clippy clean, release build OK

## Task 3: Wire the worker to the counters, plus the log line

**Description:** In `transfer.rs`, replace every direct `active_conns` mutation:
- connect: `conn_acquired(c.via_media)`
- `drop_conn`: `conn_dropped(via_media)`
- worker exit (≈588): `conn_dropped(via_media)`
- stall restart (≈255): `reset_conns()`

Append `, media conns: {m}/{m+r}` to the download `info!` line (≈284). Read the
counts from `Progress` before the stage changes, or pass them through `Ctx`.
Upload log lines stay unchanged.

**Acceptance criteria:**
- [x] No remaining direct `active_conns.fetch_*` or `store` in `transfer.rs`.
- [x] Download log line includes media conns; upload log line is byte-identical
      to before.
- [x] The home-DC download path (`take_download`) counts media correctly.

**Verification:**
- [x] `cargo test`, `cargo clippy --all-targets -- -D warnings`
- [ ] Manual: bench on a foreign-DC file → `media conns: N/N`;
      `BENCH_NO_MEDIA_DC=1` → `0/N`
- [ ] Manual: send a file to the bot and check the live `🔗` line and the final 📥 suffix

**Dependencies:** Task 1, Task 2

**Files:** `src/transfer.rs`

**Scope:** S

## Checkpoint: Complete
- [ ] All SPEC success criteria met
- [ ] Human review before committing or deploying
