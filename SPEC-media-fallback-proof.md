# Spec Delta: Keep the media address while it demonstrably works

Status: implemented (2716b3e, deployed to both prod services 2026-11-20 21:06 UTC; reviewed with Codex for the owner)

## Objective

Stop a single failed connect from moving a DC off its media address while
media connections to that DC keep succeeding.

Since 2026-11-20 about 19:23 UTC, DC5 drops connections after 1 to 2.5 s
(`read 0 bytes`), on the media and the regular address, with or without the
SOCKS proxy. Sooner or later a drop lands during a connect handshake. Then the
fallback from `ced4e63` sees "media failed, regular worked" and blocks the
media address for 300 s. Downloads move to the regular address, which is about
8× slower (11 to 27 MB/s against 66 to 88 MB/s in the 2026-11-21 prod bench)
and draws transport -429.

Observed every time:

- The 2026-11-21 prod benches: the media address was blocked after about 5 s in
  every media run (`media conns: 13/17`, `13/25`).
- Live job 2026-11-20 19:31: `media conns: 11/48`.

## Current Behavior (ced4e63)

`Net::open_conn`: if opening on the media address fails with anything but an
RPC error or a -429, it retries on the regular address. If that works,
`MediaBackoff::mark(dc)` blocks media for that DC for `MEDIA_RETRY_AFTER`
(300 s). One failure is enough. `media_addr` then returns `None` for that DC
until the block expires. The warm pool's `stale()` reopens regular connections
on media once it is usable again.

## New Behavior

A media connect failure blocks the media address only when there is **no proof
that it works**: no media connection to that DC opened successfully in the last
`MEDIA_PROOF_WINDOW` (60 s).

- `MediaBackoff` remembers, per DC, when a media connection last opened
  successfully. `ok(dc, now)` is called in `open_conn`'s success arm when the
  returned `Conn.via_media` is true: `open_at` has finished an authenticated
  `help.getConfig` round trip on the address it actually dialed. This covers
  workers, warm-pool refills and home-DC downloads. Reused warm sockets and
  regular fallbacks are not proof.
- A connection that opens and is dropped a second later still counts as proof:
  the address is reachable, and in the 2026-11-21 benches transfers on such
  connections still ran at 66 to 88 MB/s.
- `mark(dc, now)` returns an outcome instead of a bool:
  `Blocked { last_ok_ago: Option<Duration> }` (new block started),
  `AlreadyBlocked` (block running, unchanged), or `KeptByProof { ago }` (last
  success less than `MEDIA_PROOF_WINDOW` = 60 s ago: no block). The durations are
  computed from the `now` sampled under the lock. `open_conn` logs `Blocked` at
  warn, with today's message plus the last success, so every block can be
  checked from the default logs:
  `DC5: media address failed (…); using the regular address for 300s (last media success: 75s ago)`,
  or `(… : never)`. `KeptByProof` is logged at debug
  (`DC5: media address failed but worked 3s ago; keeping it`).
- Ordering: `last_ok` and the block deadline live in the same `MediaBackoff`
  behind the existing mutex, and `now` is sampled while the lock is held, so
  each `ok`/`mark` sees the other's effects in lock order. Outcomes follow that
  order. A `mark` that runs before a concurrent worker's `ok` (no recent proof
  yet) starts the normal 300 s block, and a later `ok` does not end it. This is
  today's behavior for that race.
- The failing open still retries on the regular address, so that one worker
  gets a connection right away, exactly as today. Only the DC-wide block
  changes.
- Unchanged: when both addresses fail, media is not blocked; RPC errors and
  -429 never retry or block; the block length (300 s); the `Retry` state and
  log; `stale()`.
- Accepted consequence: while media is not blocked, `stale()` treats regular
  warm connections as stale. A regular fallback returned to the warm pool is
  then dropped at the next checkout or keepalive pass, and a media connection
  is dialed instead. This is the intended preference for media. It costs one
  extra dial, which the -429 cooldown from `1275c1e` still gates.

The rule needs no error-type matching. Through the SOCKS proxy, an EOF can also
mean the proxy failed to reach the address. Then no media connection succeeds,
so there is no proof and the block happens as before.

## Success Criteria

1. Unit tests on `MediaBackoff` (pure, `Instant` passed in):
   - a failure with no success ever → blocked (today's behavior);
   - a failure 59 s after a success → `KeptByProof { ago: 59 s }`, not blocked;
   - a failure 60 s or more after the last success → `Blocked`;
   - a failure while blocked → `AlreadyBlocked`;
   - both orders of the race: `mark` then `ok` → stays blocked for the full
     300 s; `ok` then `mark` → `KeptByProof`;
   - after a block expires, `Retry` is reported once, then `Usable` (unchanged);
   - other DCs are independent.
2. Existing `ced4e63` tests keep their intent. Assertions on `mark`'s old `bool`
   move to the matching outcome (`true` → `Blocked { .. }`, `false` →
   `AlreadyBlocked`), with the same number of asserts. The floor guard is clean,
   and no test or assert is removed.
3. `scripts/check.sh full` passes.
4. Prod bench (`bench user.toml 243599 4 8 512 1000`, media, bench binary only,
   default info log level, observed over the whole run; 3 runs): every
   `media address failed … using the regular address` line reports
   `last media success: never` or an age of at least 60 s. **Zero** block lines
   report a younger age, which the code makes impossible, so the bench checks the
   wiring. Each block, if any, is listed in the report. The
   `media conns` ratio and the speed are diagnostics, compared with the
   2026-11-21 media runs (13/17 and 13/25; 65 to 88 MB/s), not pass/fail. The
   drops themselves are expected to continue.
5. Not deployed to the live services without the owner's go-ahead.

## Boundaries (deltas only)

- Always: keep the change inside `MediaBackoff` and `open_conn` in
  `src/net.rs`. Behavior without a recent success is identical to `ced4e63`.
- Ask first: changing `MEDIA_RETRY_AFTER`, matching on error types or messages,
  deploying.
- Never: edit `vendor/`, weaken the -429 handling from `1275c1e`.

## Out of Scope (Deferred)

- Finding out why DC5 drops connections since 2026-11-20 (Telegram side or
  network); no code change fixes that.
- Warm-pool refill opening extra connections for slots that active non-home
  transfers have taken (noted in the `1275c1e` review).
- Requiring several failures before blocking (K-of-N): the proof window covers
  the observed case with less state.

## Results (prod bench, 2026-11-21, bench binary only, before the deploy)

`RUST_LOG=info,tgbot_mvdc5::net=debug bench user.toml 243599 4 8 512 1000`, 3 runs:

| Run | Block lines | `keeping it` (proof) | media conns | Speed |
|---|---|---|---|---|
| 1 | 0 | 2 | 32/34 | 65.06 MB/s |
| 2 | 0 | 1 | 30/31 | 69.56 MB/s |
| 3 | 0 | 2 | 33/35 | 64.40 MB/s |
| before (ced4e63 + 1275c1e) | 1 per run | n/a | 13/17, 13/25 | 65.65 to 87.85 MB/s |

- Criterion 4 met: no block lines at all, so none with an age under 60 s. The
  `keeping it` debug events show that recording proof works, as Codex asked.
- Each failed media connect fell back alone (the 1 or 2 regular conns per run).
- Bench speed did not improve: each run is a new process lasting about 15 s,
  so the old 300 s block only hurt its last seconds. The expected gain is in
  the live services, where one block sent every job for 5 minutes to the
  regular address (11 to 27 MB/s).
