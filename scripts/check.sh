#!/usr/bin/env bash
# Runs the checks in CONSTRAINTS.md. CONSTRAINTS.md is the source of truth;
# this script must mirror it.
#
#   scripts/check.sh fast   after an edit             (floor: blocks)
#   scripts/check.sh task   before handing work back  (~90 s: fast + tests, coverage, audit)
#   scripts/check.sh full   before commit / deploy    (task + secret history + mutation)
#
# Exit status: 1 if any BLOCK check failed. WARN checks only print.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 2

STAGE="${1:-task}"
BASE="${BASE:-origin/main}"
# Measured 2026-11-21 (see CONSTRAINTS.md). Update when the number improves.
COVERAGE_RATCHET=23.31
COVERAGE_TOLERANCE=0.5
CHANGED_LINES_MIN=80
MUTATION_MIN=40
# Coverage and mutation warn until this date, then block.
WARN_UNTIL=2026-11-30

failed=0
block() { echo "✗ BLOCK: $*"; failed=1; }
warn_or_block() {
  if [[ "$(date +%F)" > "$WARN_UNTIL" ]]; then block "$*"; else echo "! WARN:  $* (blocks after $WARN_UNTIL)"; fi
}
step() { echo; echo "── $*"; }

fast() {
  step "floor guard (diff vs $BASE)"
  node scripts/floor-guard.mjs --base "$BASE"
  case $? in 0) ;; 1) block "floor guard" ;; *) block "floor guard could not run" ;; esac

  step "rustfmt"
  cargo fmt --check >/dev/null 2>&1 || block "cargo fmt --check (run: cargo fmt)"

  step "clippy"
  cargo clippy --all-targets --quiet -- -D warnings || block "clippy -D warnings"

  step "secrets in uncommitted changes"
  local mb; mb="$(git merge-base "$BASE" HEAD)" || { block "no merge base"; return; }
  { git diff "$mb"; git ls-files -o --exclude-standard -z | xargs -0 cat 2>/dev/null; } \
    | gitleaks stdin --redact --no-banner --log-level warn
  case $? in 0) ;; 1) block "gitleaks found a secret (value redacted)" ;; *) block "gitleaks could not run" ;; esac
}

task() {
  fast

  step "tests + coverage (cargo llvm-cov)"
  if cargo llvm-cov --quiet --lcov --output-path target/lcov.info; then
    local pct
    pct="$(awk -F: '/^LF:/{lf+=$2} /^LH:/{lh+=$2} END{printf "%.2f", lh*100/lf}' target/lcov.info)"
    echo "project line coverage: ${pct}% (ratchet ${COVERAGE_RATCHET}%)"
    if awk -v p="$pct" -v r="$COVERAGE_RATCHET" -v t="$COVERAGE_TOLERANCE" 'BEGIN{exit !(p < r - t)}'; then
      warn_or_block "project coverage fell: ${pct}% < ${COVERAGE_RATCHET}%"
    fi
    if command -v diff-cover >/dev/null; then
      # Exceptions W1: network-only modules (see CONSTRAINTS.md).
      diff-cover target/lcov.info --compare-branch="$BASE" --fail-under="$CHANGED_LINES_MIN" \
        --exclude src/net.rs src/main.rs src/login.rs src/bench.rs \
        || warn_or_block "changed-line coverage < ${CHANGED_LINES_MIN}%"
    else
      echo "! SKIPPED: changed-line coverage (diff-cover not installed)"
    fi
  else
    block "cargo test failed"
  fi

  step "dependency advisories (cargo audit)"
  cargo audit --quiet || block "cargo audit"
}

full() {
  task

  step "secrets in git history"
  gitleaks git --redact --no-banner --log-level warn || block "gitleaks (history)"

  step "mutation testing on the diff (cargo mutants)"
  local d; d="$(mktemp)"
  git diff "$(git merge-base "$BASE" HEAD)" -- 'src/*.rs' >"$d"
  if [[ ! -s "$d" ]]; then
    echo "no Rust changes"
  else
    cargo mutants --in-diff "$d" --timeout 60 --no-shuffle >/dev/null 2>&1
    # 0 = all caught, 2 = some missed, 3 = timeouts; anything else means it did not run.
    case $? in 0|2|3) ;; *) block "cargo mutants could not run"; rm -f "$d"; return ;; esac
    local caught missed score
    caught="$(wc -l <mutants.out/caught.txt 2>/dev/null | tr -d " " || echo 0)"
    missed="$(wc -l <mutants.out/missed.txt 2>/dev/null | tr -d " " || echo 0)"
    if (( caught + missed == 0 )); then
      echo "no viable mutants in the diff"
    else
      score=$(( caught * 100 / (caught + missed) ))
      echo "mutation score on diff: ${score}% (${caught} caught, ${missed} missed; see mutants.out/missed.txt)"
      (( score >= MUTATION_MIN )) || warn_or_block "mutation score ${score}% < ${MUTATION_MIN}%"
    fi
  fi
  rm -f "$d"
}

case "$STAGE" in
  fast) fast ;; task) task ;; full) full ;;
  *) echo "usage: $0 fast|task|full"; exit 2 ;;
esac

echo
if (( failed )); then echo "constraints: FAILED"; exit 1; fi
echo "constraints: OK"
