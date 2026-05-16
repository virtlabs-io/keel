#!/usr/bin/env bash
# ============================================================================
# perf-lock-artifacts.sh — Archive benchmark raw artifacts into versioned dir
# ============================================================================
#
# Copies benchmark raw artifact directories into a date-tagged versioned
# directory under artifacts/perf/<DATE_TAG>/raw/.  This preserves a permanent
# record of each benchmark run so results can be compared across commits.
#
# Artifacts collected (default paths):
#   - keel_300s/     — 300-second sysbench run through KEEL proxy
#   - direct_300s/   — 300-second baseline run direct to PostgreSQL
#   - paired_200s/   — 200-second paired A/B run (keel vs direct)
#   - overhead_curve.json — computed overhead-vs-concurrency curve
#
# The script uses a copy_item() helper that logs each copied item and skips
# missing source directories gracefully (with a warning).
#
# Environment Variables:
#   DATE_TAG    Date tag for the versioned directory (default: YYYY-MM-DD)
#   DEST_DIR    Override destination path (default: artifacts/perf/<DATE_TAG>/raw)
#
# Usage:
#   ./bench/perf-lock-artifacts.sh
#   DATE_TAG=2024-01-15 ./bench/perf-lock-artifacts.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATE_TAG="${DATE_TAG:-$(date +%F)}"
DEST_DIR="${DEST_DIR:-$ROOT_DIR/artifacts/perf/$DATE_TAG/raw}"

SRC_KEEL_300="${SRC_KEEL_300:-/tmp/keel_ab3_strict2_20260315_192432}"
SRC_DIRECT_300="${SRC_DIRECT_300:-/tmp/direct_ab3_strict_20260315_193246}"
SRC_PAIR_200="${SRC_PAIR_200:-/tmp/keel_direct_strict_200_inlier_20260315_194138}"
SRC_CURVE_JSON="${SRC_CURVE_JSON:-/tmp/strict_overhead_curve_200_300.json}"

log() {
  printf '[perf-lock] %s\n' "$*"
}

copy_item() {
  local src="$1"
  if [[ -e "$src" ]]; then
    cp -a "$src" "$DEST_DIR/"
    log "copied: $src"
  else
    log "missing: $src"
  fi
}

mkdir -p "$DEST_DIR"

copy_item "$SRC_KEEL_300"
copy_item "$SRC_DIRECT_300"
copy_item "$SRC_PAIR_200"
copy_item "$SRC_CURVE_JSON"

log "done"
log "dest: $DEST_DIR"

find "$DEST_DIR" -maxdepth 2 -type f | sort | sed -n '1,120p'
