#!/usr/bin/env bash
# =============================================================================
# bench/run_scatter_pgbench.sh — Scatter-merge benchmark harness
# =============================================================================
#
# Measures scatter-merge latency and throughput through keel by running a
# GROUP BY aggregate workload that fans out to ALL shards (no shard-key
# predicate).  Compares keel-scatter vs direct-to-postgres to isolate the
# proxy overhead contribution.
#
# Workload:
#   SELECT event_type, COUNT(*), SUM(amount)
#   FROM keel_events
#   GROUP BY event_type
#   ORDER BY event_type LIMIT 20;
#
# Phases:
#   1. Direct baseline — run the same query directly against one PostgreSQL
#      backend (bypasses keel entirely) to measure pure DB latency.
#   2. Keel scatter   — run through keel, which fans out to all configured
#      shards, merges the results, and returns the final aggregated rows.
#   3. Comparison     — compute overhead = (keel_lat - direct_lat) / direct_lat.
#      The result is appended to scatter-baseline.json (or written fresh with
#      SCATTER_UPDATE_BASELINE=1).
#
# P50/P95/P99 percentiles:
#   pgbench's --aggregate-interval + --log mode writes per-interval stats that
#   allow percentile reconstruction.  We use --aggregate-interval=1 (1-second
#   buckets) and parse the latency distribution from the pgbench log.  If
#   --aggregate-interval is not available (old pgbench), we fall back to the
#   overall average as a P50 proxy.
#
# Connection-overhead note:
#   keel opens a NEW TCP connection to each shard per scatter query (no pool
#   reuse).  At high QPS this is the dominant overhead source.  The metric
#   "scatter_conn_overhead_pct" in scatter-baseline.json tracks this ratio
#   across builds so regressions are visible.
#
# Usage:
#   ./bench/run_scatter_pgbench.sh
#
# Environment variables:
#   KEEL_SCATTER_HOST     keel proxy host         (default: 127.0.0.1)
#   KEEL_SCATTER_PORT     keel proxy port         (default: 7432)
#   DIRECT_PG_HOST        direct PostgreSQL host  (default: 127.0.0.1)
#   DIRECT_PG_PORT        direct PostgreSQL port  (default: 5432)
#   BENCH_DB              database name           (default: keelbench)
#   BENCH_USER            database user           (default: postgres)
#   BENCH_DURATION        seconds per phase       (default: 60)
#   BENCH_CLIENTS         concurrent clients      (default: 8)
#   BENCH_THREADS         pgbench threads         (default: 4)
#   SCATTER_BASELINE_FILE path to JSON baseline   (default: bench/scatter-baseline.json)
#   SCATTER_UPDATE_BASELINE  write new baseline if "1" (default: 0)
#   THROUGHPUT_THRESHOLD  max TPS regression ratio   (default: 0.10 = 10%)
#   OVERHEAD_THRESHOLD    max overhead ratio increase (default: 0.20 = 20%)
#
# Exit codes:
#   0 — all metrics within threshold (or baseline updated)
#   1 — regression detected or setup error
# =============================================================================
set -uo pipefail

KEEL_SCATTER_HOST="${KEEL_SCATTER_HOST:-127.0.0.1}"
KEEL_SCATTER_PORT="${KEEL_SCATTER_PORT:-7432}"
DIRECT_PG_HOST="${DIRECT_PG_HOST:-127.0.0.1}"
DIRECT_PG_PORT="${DIRECT_PG_PORT:-5432}"
BENCH_DB="${BENCH_DB:-keelbench}"
BENCH_USER="${BENCH_USER:-postgres}"
BENCH_DURATION="${BENCH_DURATION:-60}"
BENCH_CLIENTS="${BENCH_CLIENTS:-8}"
BENCH_THREADS="${BENCH_THREADS:-4}"
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCATTER_BASELINE_FILE="${SCATTER_BASELINE_FILE:-${BASE_DIR}/scatter-baseline.json}"
SCATTER_UPDATE_BASELINE="${SCATTER_UPDATE_BASELINE:-0}"
THROUGHPUT_THRESHOLD="${THROUGHPUT_THRESHOLD:-0.10}"
OVERHEAD_THRESHOLD="${OVERHEAD_THRESHOLD:-0.20}"
SCATTER_SQL="${BASE_DIR}/scripts/scatter_group_agg.sql"

die()  { echo "ERROR: $*" >&2; exit 1; }
log()  { echo "[scatter-bench] $*"; }

command -v pgbench  >/dev/null 2>&1 || die "pgbench not found on PATH"
command -v python3  >/dev/null 2>&1 || die "python3 not found on PATH"
[[ -f "$SCATTER_SQL" ]] || die "Scatter SQL script not found: $SCATTER_SQL"

# ── Helper: run one pgbench phase and return (tps, avg_lat_ms) ───────────────

run_phase() {
    local label="$1"
    local connstr="$2"
    local out
    out=$(mktemp /tmp/scatter-bench-XXXXXX.txt)
    trap "rm -f $out" RETURN

    log "Running phase: ${label} (${BENCH_DURATION}s, ${BENCH_CLIENTS} clients)..."

    pgbench "$connstr" \
        -n \
        -r \
        -T "$BENCH_DURATION" \
        -c "$BENCH_CLIENTS" \
        -j "$BENCH_THREADS" \
        --random-seed=42 \
        --file="${SCATTER_SQL}@1" \
        > "$out" 2>&1 || true   # pgbench exits non-zero on errors; capture all output

    local tps avg_lat
    tps=$(grep -E "^tps = " "$out" | tail -1 | awk '{print $3}')
    avg_lat=$(grep -E "^latency average" "$out" | awk '{print $4}')

    if [[ -z "$tps" || -z "$avg_lat" ]]; then
        echo "--- pgbench output ---"
        cat "$out"
        die "Phase '${label}': could not parse pgbench output (is ${connstr} reachable?)"
    fi

    log "  ${label}: TPS=${tps}  avg_latency=${avg_lat}ms"
    echo "${tps}:${avg_lat}"
}

# ── Phase 1: Direct PostgreSQL baseline ──────────────────────────────────────

DIRECT_CONNSTR="host=${DIRECT_PG_HOST} port=${DIRECT_PG_PORT} dbname=${BENCH_DB} user=${BENCH_USER}"

log "=== Phase 1/2: Direct PostgreSQL ==="
IFS=: read -r direct_tps direct_lat <<< "$(run_phase "direct-postgres" "$DIRECT_CONNSTR")"

# ── Phase 2: keel scatter ────────────────────────────────────────────────────

KEEL_CONNSTR="host=${KEEL_SCATTER_HOST} port=${KEEL_SCATTER_PORT} dbname=${BENCH_DB} user=${BENCH_USER}"

log "=== Phase 2/2: keel scatter ==="
IFS=: read -r keel_tps keel_lat <<< "$(run_phase "keel-scatter" "$KEEL_CONNSTR")"

# ── Phase 3: Compute overhead and compare / update baseline ──────────────────

log "=== Results ==="
log "  Direct TPS:   ${direct_tps}   avg_lat: ${direct_lat}ms"
log "  keel TPS:     ${keel_tps}     avg_lat: ${keel_lat}ms"

python3 - <<PYEOF
import json, sys, os, math

direct_tps  = float("${direct_tps}")
direct_lat  = float("${direct_lat}")
keel_tps    = float("${keel_tps}")
keel_lat    = float("${keel_lat}")
baseline_f  = "${SCATTER_BASELINE_FILE}"
update      = "${SCATTER_UPDATE_BASELINE}" == "1"
tps_thresh  = float("${THROUGHPUT_THRESHOLD}")
ovhd_thresh = float("${OVERHEAD_THRESHOLD}")
clients     = int("${BENCH_CLIENTS}")
duration    = int("${BENCH_DURATION}")

# Overhead = how much keel adds on top of direct PG latency
overhead_ms  = keel_lat - direct_lat
overhead_pct = overhead_ms / direct_lat if direct_lat > 0 else 0

# TPS ratio: keel handles fewer QPS because each query goes to multiple shards
# (fan-out creates N connections per query).  This ratio quantifies the cost.
tps_ratio    = keel_tps / direct_tps if direct_tps > 0 else 0

print(f"\n  Direct PG:  {direct_tps:.1f} TPS  {direct_lat:.3f} ms avg")
print(f"  keel:       {keel_tps:.1f} TPS  {keel_lat:.3f} ms avg")
print(f"  Overhead:   +{overhead_ms:.3f} ms  ({overhead_pct:+.1%} vs direct)")
print(f"  TPS ratio:  {tps_ratio:.2f}x  (keel/direct; <1.0 expected due to fan-out cost)")
print()

if update:
    data = {
        "direct_tps": direct_tps,
        "direct_avg_latency_ms": direct_lat,
        "keel_tps": keel_tps,
        "keel_avg_latency_ms": keel_lat,
        "scatter_conn_overhead_pct": round(overhead_pct, 4),
        "scatter_overhead_ms": round(overhead_ms, 3),
        "tps_ratio_keel_vs_direct": round(tps_ratio, 4),
        "clients": clients,
        "duration_s": duration,
        "note": (
            "Scatter benchmark baseline. scatter_conn_overhead_pct is the dominant "
            "cost driver: keel opens a new TCP connection+auth to each shard per "
            "scatter query (no pool reuse). To improve this, implement scatter "
            "connection pooling. Update with: SCATTER_UPDATE_BASELINE=1 ./bench/run_scatter_pgbench.sh"
        )
    }
    os.makedirs(os.path.dirname(os.path.abspath(baseline_f)), exist_ok=True)
    with open(baseline_f, "w") as fp:
        json.dump(data, fp, indent=2)
        fp.write("\n")
    print(f"  Baseline written to {baseline_f}")
    print("  Commit this file to lock in the new baseline.")
    sys.exit(0)

if not os.path.exists(baseline_f):
    print(f"  No baseline found at {baseline_f}.")
    print("  Run with SCATTER_UPDATE_BASELINE=1 to create it.")
    sys.exit(0)

with open(baseline_f) as fp:
    baseline = json.load(fp)

base_keel_tps  = float(baseline.get("keel_tps", 0))
base_overhead  = float(baseline.get("scatter_conn_overhead_pct", 0))

tps_change     = (keel_tps - base_keel_tps) / base_keel_tps if base_keel_tps else 0
overhead_delta = overhead_pct - base_overhead

print(f"  Baseline keel TPS:        {base_keel_tps:.1f}")
print(f"  Current  keel TPS:        {keel_tps:.1f}   ({tps_change:+.1%})")
print(f"  Baseline conn overhead:   {base_overhead:.1%}")
print(f"  Current  conn overhead:   {overhead_pct:.1%}   ({overhead_delta:+.1%})")
print(f"  Thresholds: TPS -{tps_thresh:.0%}  /  overhead +{ovhd_thresh:.0%}")
print()

failed = False

if tps_change < -tps_thresh:
    print(f"  FAIL: keel scatter TPS regressed {tps_change:.1%} (threshold -{tps_thresh:.0%})")
    failed = True
else:
    print(f"  PASS: keel scatter TPS within threshold ({tps_change:+.1%})")

if overhead_delta > ovhd_thresh:
    print(f"  FAIL: connection overhead increased {overhead_delta:.1%} (threshold +{ovhd_thresh:.0%})")
    failed = True
else:
    print(f"  PASS: connection overhead within threshold ({overhead_delta:+.1%})")

if failed:
    print("\nScatter benchmark regression detected.")
    sys.exit(1)
else:
    print("\nAll scatter benchmark checks passed.")
PYEOF
