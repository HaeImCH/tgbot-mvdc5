# Constraints

Last reviewed: 2026-11-16 by @HaeImCH

The quality bar for tgbot_mvdc5. `scripts/check.sh` runs it; this file is the
source of truth, and the script mirrors it. Tightening the bar needs no
discussion; loosening it does, and the floor guard flags any loosening.

## Floor (always enforced, blocks)

- No new suppressions: `#[allow(..)]`, `#[expect(..)]`, `#[coverage(off)]`, `#[mutants::skip]`, `rustfmt::skip`, `gitleaks:allow`
- No unfinished work: `todo!()`, `unimplemented!()`, `TODO`, `FIXME` in Rust code
- No tests made easier: no `#[ignore]`, no net removal of `#[test]` functions or `assert*!` calls in a file
- No secrets in source: checked by gitleaks with `--redact`, which reports the rule and file but never the value
- This file does not get weakened to make a change pass
- `vendor/` is not edited (patched grammers-mtsender; see `vendor/grammers-mtsender/PATCH.md`)

## Enforced with numbers

| Dimension | Rule | Checked by | Runs at | On failure |
|-----------|------|-----------|---------|-----------|
| Floor | the floor above, on the diff vs `origin/main` | `node scripts/floor-guard.mjs` | fast | block |
| Format | zero rustfmt differences | `cargo fmt --check` | fast | block |
| Lint | zero clippy warnings | `cargo clippy --all-targets -- -D warnings` | fast | block |
| Secrets (changes) | no findings in the diff and untracked files | `gitleaks stdin --redact` | fast | block |
| Tests | all pass | `cargo llvm-cov` (one run produces both the test result and coverage) | task | block |
| Dependencies | no known vulnerability in Cargo.lock (RustSec; unmaintained and yanked are warnings only) | `cargo audit` | task | block |
| Changed-line coverage | at least 80% of changed lines covered, except W1 | `diff-cover target/lcov.info --fail-under=80` | task | warn until 2026-11-30, then block |
| Project coverage | at least 17.01% of lines, must not fall | `cargo llvm-cov` + lcov totals | task | warn until 2026-11-30, then block |
| Mutation score | at least 40% of mutants in changed lines caught | `cargo mutants --in-diff` | full | warn until 2026-11-30, then block |
| Secrets (history) | no findings | `gitleaks git --redact` | full | block |

Why these numbers:
- **80% of changed lines**: high enough to force a test, low enough to allow a config line or a log message.
- **Project coverage at least 17.01%**: 17.51% measured today (after `cargo fmt`), minus 0.5 points of tolerance. Most of the code needs a live Telegram connection, so the rule is "must not fall". The tolerance absorbs drift when unrelated code moves. When coverage improves, raise both this row and `COVERAGE_RATCHET` in `scripts/check.sh`.
- **Mutation 40%**: `status.rs`, the best-tested module, scores 39.7% today (58 caught out of 146 viable mutants). The rule holds that line on new code. Raise it as tests improve.
- **Warn period**: coverage and mutation are new, so they warn for the first two weeks while the floor blocks from day one.

**External checks**, which this project's own test suite cannot satisfy by being wrong: `cargo audit`, which uses the RustSec database, and gitleaks.

## Stages

| Stage | Command | When | Budget |
|-------|---------|------|--------|
| fast | `scripts/check.sh fast` | after an edit | about 10 s, incremental |
| task | `scripts/check.sh task` | before handing work back | about 90 s (coverage run is about 30 s) |
| full | `scripts/check.sh full` | before a commit or deploy | minutes (mutation testing) |

There is no CI. These stages are the only gates, so agents run `task` before
reporting work as done and `full` before committing or deploying.

## Measured, not yet enforced

| Metric | Today (2026-11-16) | Direction |
|--------|-------|-----------|
| Mutation score, `src/status.rs` (whole file) | 39.7% | must not fall |
| Line coverage per file | status 72.86%, transfer 13.45%, bot 13.56%, others 0% | must not fall |

## Exceptions

| ID | Rule | Path | Reason | Owner | Expires |
|----|------|------|--------|-------|---------|
| W1 | Changed-line coverage | `src/net.rs`, `src/main.rs`, `src/login.rs`, `src/bench.rs` | Network-only code that needs a live Telegram session. Checked by hand with `bench` instead | @HaeImCH | 2027-02-14 |

## Setup

- **Required**: `cargo-llvm-cov`, `cargo-audit`, `cargo-mutants`, `gitleaks`, `node` (for the floor guard) and `diff-cover` (`uv tool install diff-cover`). All are installed on this Mac. Without `diff-cover`, the changed-line coverage row is skipped and the run prints a notice.
