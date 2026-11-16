# Implementation Plan: Media-server indicator

Source spec: SPEC.md

## Overview

Mark every transfer connection as media or regular at the moment it is opened,
based on the address it actually dialed. Count the media ones alongside
`active_conns`, and record acquisitions by kind for each stage. Show the result
in three places: the live status message, the final 📥 summary line, and the
download log line.

## Dependency graph

```
net.rs: open_at reports dialed kind ─► Conn { sender, via_media } ─► warm pool stores Conn
                                                   │
status.rs: Progress counters + render/summary ◄────┤ (independent; pure, unit-tested)
                                                   │
transfer.rs: worker uses Conn, drives Progress counters, log line
bench.rs: adapt to Conn (.sender)
```

Tasks 1 and 2 are independent of each other. Task 3 needs both.

## Architecture Decisions

- **Return the dialed kind from `open_at`.** `server_addr` falls back to the
  regular address when the DC is not in the media map. So the tag comes from
  the address actually used, not from the `media` argument or `media_addr()`.
  This also makes SPEC criteria 3 and 4 (an old warm connection, a fallback
  mid-job) correct without any special handling.
- **`Conn { sender, via_media }` as the pool's currency.** `open`, `take`,
  `take_download` and `give_back` all use it, so the tag can't come apart from
  the connection. Internal callers that only invoke RPCs use `.sender`.
- **Counter mutations live in `Progress` methods**
  (`conn_acquired(media)`, `conn_dropped(media)`, `reset_conns()`).
  `transfer.rs` stops touching `active_conns` directly, which makes the
  lockstep invariant impossible to break by accident.
- **Per-stage acquisitions** go in two atomics that reset in `set_stage`
  (alongside `bytes`). They are pushed into the `finished` tuple when the stage
  ends.

## Task List

### Phase 1: Foundation
- [x] Task 1: Tag connections in `net.rs` (`Conn` type, warm pool, call sites)
- [x] Task 2: `Progress` media counters and zh-TW rendering (TDD in `status.rs`)

### Checkpoint: Foundation
- [x] `cargo test` and `cargo clippy --all-targets -- -D warnings` pass
- [x] Behavior unchanged so far (nothing wired to the UI yet)

### Phase 2: Wiring
- [x] Task 3: Worker drives the counters, plus the download log line (`transfer.rs`)

### Checkpoint: Complete
- [x] `cargo test` and `cargo clippy` pass
- [ ] Manual: bench normally → `media conns: N/N`; `BENCH_NO_MEDIA_DC=1` → `0/N`
- [ ] Manual: real file through the bot shows the live and final indicator
- [ ] Review with the human before committing or deploying

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Counter drift (a missed decrement path) | Med: wrong numbers shown | All mutations go through `Progress` methods; worker keeps `via_media` next to `conn`; unit test for the invariant |
| Signature churn breaks bench or upload paths | Low | Compiler catches every call site; `.sender` for RPC-only uses |
| Extra overhead on the hot path | Low | Relaxed atomics only, touched on connect/drop, not per request |
| Home-DC download connections must not enter the warm pool | Low | Keep the existing `!ctx.is_home_download()` guard unchanged |

## Open Questions

None. The spec was approved as written.
