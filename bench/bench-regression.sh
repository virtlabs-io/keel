#!/usr/bin/env bash
# =============================================================================
# bench-regression.sh — PR benchmark regression check
# =============================================================================
#
# Measures proxy overhead dynamically by running pgbench twice:
#   1. Directly against PostgreSQL (live baseline, same runner / same load)
#   2. Through the keel proxy
#
# The overhead ratio (proxy_tps / direct_tps) is compared against a minimum
# acceptable ratio.  This approach is environment-agnostic: CI runners,
# developer laptops, and cloud VMs all produce self-consistent results because
# both legs run on the same hardware back-to-back.
#
# Exit codes:
#   0 — all metrics within threshold
#   1 — overhead exceeds threshold (or usage error)
#
# Environment variables:
#   KEEL_BENCH_CONNSTR    libpq connstr → keel proxy  (default: port 7432)
#   DIRECT_BENCH_CONNSTR  libpq connstr → PostgreSQL  (default: port 5432)
#   KEEL_BENCH_DURATION   seconds per run              (default: 60)
#   KEEL_BENCH_CLIENTS    concurrent clients           (default: 32)
#   KEEL_BENCH_THREADS    pgbench threads              (default: 8)
#   TPS_RATIO_MIN         minimum proxy_tps/direct_tps (default: 0.60 = 60%)
#   LAT_RATIO_MAX         maximum proxy_lat/direct_lat (default: 2.50 = 2.5×)
#
# Usage:
#   ./bench/bench-regression.sh
#   KEEL_BENCH_DURATION=120 KEEL_BENCH_CLIENTS=64 ./bench/bench-regression.sh
#
# =============================================================================
set -euo pipefail

CONNSTR="${KEEL_BENCH_CONNSTR:-host=127.0.0.1 port=7432 dbname=keelbench user=postgres}"
DIRECT_CONNSTR="${DIRECT_BENCH_CONNSTR:-host=127.0.0.1 port=5432 dbname=keelbench user=postgres}"
DURATION="${KEEL_BENCH_DURATION:-60}"
CLIENTS="${KEEL_BENCH_CLIENTS:-32}"
THREADS="${KEEL_BENCH_THREADS:-8}"
TPS_RATIO_MIN="${TPS_RATIO_MIN:-0.60}"   # proxy must reach ≥ 60% of direct TPS
LAT_RATIO_MAX="${LAT_RATIO_MAX:-2.50}"   # proxy latency must be ≤ 2.5× direct

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPTS_DIR="${BASE_DIR}/scripts"
DIRECT_OUTPUT=$(mktemp /tmp/keel-direct-XXXXXX.txt)
PROXY_OUTPUT=$(mktemp /tmp/keel-proxy-XXXXXX.txt)
trap 'rm -f "$DIRECT_OUTPUT" "$PROXY_OUTPUT"' EXIT

# ── Helpers ──────────────────────────────────────────────────────────────────

die() { echo "ERROR: $*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "'$1' not found on PATH"
}

# Parse "tps = 12345.67 (without initial connection time)" → 12345.67
parse_tps() {
    grep -E "^tps = " "$1" | tail -1 | awk '{print $3}'
}

# Parse "latency average = 12.345 ms" → 12.345
parse_avg_latency() {
    grep -E "^latency average" "$1" | awk '{print $4}'
}

run_pgbench() {
    local connstr="$1"
    local outfile="$2"
    pgbench "$connstr" \
        -n \
        -r \
        -T "$DURATION" \
        -c "$CLIENTS" \
        -j "$THREADS" \
        --random-seed=1 \
        --file="${SCRIPTS_DIR}/read_point.sql"@45 \
        --file="${SCRIPTS_DIR}/read_range.sql"@15 \
        --file="${SCRIPTS_DIR}/read_aggregate.sql"@10 \
        --file="${SCRIPTS_DIR}/write_update.sql"@20 \
        --file="${SCRIPTS_DIR}/write_insert.sql"@8 \
        --file="${SCRIPTS_DIR}/write_delete.sql"@2 \
        2>&1 | tee "$outfile"
}

# ── Preflight ─────────────────────────────────────────────────────────────────

require_cmd pgbench
require_cmd python3

echo "==================================================================="
echo "  KEEL Benchmark Regression Check  (dynamic overhead mode)"
echo "  proxy:    ${CONNSTR}"
echo "  direct:   ${DIRECT_CONNSTR}"
echo "  duration: ${DURATION}s  clients: ${CLIENTS}  threads: ${THREADS}"
echo "  thresholds: TPS ratio >= ${TPS_RATIO_MIN}  latency ratio <= ${LAT_RATIO_MAX}"
echo "==================================================================="

# ── Step 1: direct baseline ───────────────────────────────────────────────────

echo ""
echo "[1/3] Running DIRECT pgbench against PostgreSQL (${DURATION}s)..."
run_pgbench "$DIRECT_CONNSTR" "$DIRECT_OUTPUT"

DIRECT_TPS=$(parse_tps "$DIRECT_OUTPUT")
DIRECT_LAT=$(parse_avg_latency "$DIRECT_OUTPUT")
[[ -z "$DIRECT_TPS" ]] && die "Could not parse TPS from direct pgbench output"
[[ -z "$DIRECT_LAT" ]] && die "Could not parse latency from direct pgbench output"

echo ""
echo "  Direct TPS:      ${DIRECT_TPS}"
echo "  Direct avg lat:  ${DIRECT_LAT} ms"

# ── Step 2: proxy benchmark ───────────────────────────────────────────────────

echo ""
echo "[2/3] Running PROXY pgbench through keel (${DURATION}s)..."
run_pgbench "$CONNSTR" "$PROXY_OUTPUT"

PROXY_TPS=$(parse_tps "$PROXY_OUTPUT")
PROXY_LAT=$(parse_avg_latency "$PROXY_OUTPUT")
[[ -z "$PROXY_TPS" ]] && die "Could not parse TPS from proxy pgbench output"
[[ -z "$PROXY_LAT" ]] && die "Could not parse latency from proxy pgbench output"

echo ""
echo "  Proxy  TPS:      ${PROXY_TPS}"
echo "  Proxy  avg lat:  ${PROXY_LAT} ms"

# ── Step 3: overhead analysis ─────────────────────────────────────────────────

echo ""
echo "[3/3] Overhead analysis..."

python3 - <<PYEOF
import sys

direct_tps  = float("${DIRECT_TPS}")
direct_lat  = float("${DIRECT_LAT}")
proxy_tps   = float("${PROXY_TPS}")
proxy_lat   = float("${PROXY_LAT}")
tps_min     = float("${TPS_RATIO_MIN}")
lat_max     = float("${LAT_RATIO_MAX}")

tps_ratio = proxy_tps / direct_tps if direct_tps > 0 else 0.0
lat_ratio = proxy_lat / direct_lat if direct_lat > 0 else 0.0

print()
print(f"  Direct  TPS:      {direct_tps:.2f}")
print(f"  Proxy   TPS:      {proxy_tps:.2f}   ({tps_ratio:.1%} of direct)")
print(f"  Direct  avg lat:  {direct_lat:.3f} ms")
print(f"  Proxy   avg lat:  {proxy_lat:.3f} ms   ({lat_ratio:.2f}× direct)")
print(f"  Thresholds:       TPS ratio >= {tps_min:.0%}  /  latency ratio <= {lat_max:.2f}×")

failed = False

if tps_ratio < tps_min:
    print(f"\n  FAIL: Proxy TPS ratio {tps_ratio:.1%} < minimum {tps_min:.0%}")
    print(f"        Proxy is {1.0 - tps_ratio:.1%} slower than direct — overhead too high")
    failed = True
else:
    print(f"\n  PASS: TPS ratio {tps_ratio:.1%} >= {tps_min:.0%}")

if lat_ratio > lat_max:
    print(f"  FAIL: Proxy latency {lat_ratio:.2f}× direct > maximum {lat_max:.2f}×")
    failed = True
else:
    print(f"  PASS: Latency ratio {lat_ratio:.2f}× <= {lat_max:.2f}×")

if failed:
    print("\nBenchmark regression detected. See output above.")
    sys.exit(1)
else:
    print("\nAll benchmark checks passed.")
PYEOF
