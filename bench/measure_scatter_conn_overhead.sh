#!/usr/bin/env bash
# =============================================================================
# bench/measure_scatter_conn_overhead.sh
# =============================================================================
#
# Measures the TCP connection + authentication overhead incurred by keel's
# scatter engine, which opens a new connection to each shard per query
# rather than reusing pooled connections.
#
# This is the primary scatter performance bottleneck at high QPS:
#   - Each scatter query opens N_SHARDS TCP connections.
#   - Each connection completes a full startup/auth handshake.
#   - On a local network, this adds ~0.5–2 ms per shard per query.
#   - At 1000 QPS × 2 shards = 2000 new connections/s — well above the
#     typical PG backend process-spawn limit.
#
# Measurement method:
#   1. "Warm" query: run the scatter SQL repeatedly through keel (Nt times),
#      compute avg latency. This includes: conn overhead + query + merge.
#   2. "Direct" query: run the same SQL directly against one PG shard (Nd
#      times), compute avg latency. This measures query + wire only.
#   3. "Null" query: run "SELECT 1" through keel scatter (Nn times).
#      This isolates connection overhead + wire overhead (minimal query work).
#   4. Overhead estimate:
#        conn_overhead_ms ≈ avg(null_scatter) - avg(null_direct)
#        query_overhead_ms ≈ avg(scatter) - avg(direct)
#        connection_fraction = conn_overhead_ms / avg(scatter)
#
# Usage:
#   ./bench/measure_scatter_conn_overhead.sh
#
# Environment variables:
#   KEEL_HOST        keel proxy host       (default: 127.0.0.1)
#   KEEL_PORT        keel proxy port       (default: 7432)
#   DIRECT_PG_HOST   direct PG host        (default: 127.0.0.1)
#   DIRECT_PG_PORT   direct PG port        (default: 5432)
#   BENCH_DB         database name         (default: keelbench)
#   BENCH_USER       database user         (default: postgres)
#   N_QUERIES        queries per phase     (default: 200)
# =============================================================================
set -uo pipefail

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-7432}"
DIRECT_PG_HOST="${DIRECT_PG_HOST:-127.0.0.1}"
DIRECT_PG_PORT="${DIRECT_PG_PORT:-5432}"
BENCH_DB="${BENCH_DB:-keelbench}"
BENCH_USER="${BENCH_USER:-postgres}"
N_QUERIES="${N_QUERIES:-200}"
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCATTER_SQL="${BASE_DIR}/scripts/scatter_group_agg.sql"

die() { echo "ERROR: $*" >&2; exit 1; }
log() { echo "[conn-overhead] $*"; }

command -v psql    >/dev/null 2>&1 || die "psql not found on PATH"
command -v python3 >/dev/null 2>&1 || die "python3 not found on PATH"
[[ -f "$SCATTER_SQL" ]] || die "Scatter SQL not found: $SCATTER_SQL"

SCATTER_SQL_TEXT=$(cat "$SCATTER_SQL" | grep -v '^--' | tr -s ' \n' ' ' | sed 's/^ *//;s/ *$//')

# ── Helper: time N serial queries and return avg latency in ms ───────────────

time_queries() {
    local label="$1"
    local host="$2"
    local port="$3"
    local sql="$4"
    local n="$5"

    log "Timing ${n} queries: ${label}..."

    python3 - <<PYEOF
import subprocess, time, sys, statistics

sql = """${sql}"""
cmd = [
    "psql",
    "-h", "${host}",
    "-p", "${port}",
    "-U", "${BENCH_USER}",
    "-d", "${BENCH_DB}",
    "-t", "-c", sql
]

latencies = []
errors = 0
for i in range(${n}):
    t0 = time.perf_counter()
    r = subprocess.run(cmd, capture_output=True, text=True, env={"PGPASSWORD": ""})
    t1 = time.perf_counter()
    if r.returncode == 0:
        latencies.append((t1 - t0) * 1000)  # ms
    else:
        errors += 1

if not latencies:
    print(f"ERROR: all {errors} queries failed", file=sys.stderr)
    sys.exit(1)

avg  = statistics.mean(latencies)
med  = statistics.median(latencies)
p95  = sorted(latencies)[int(len(latencies) * 0.95)]
p99  = sorted(latencies)[int(len(latencies) * 0.99)]

print(f"  {avg:.3f}:{med:.3f}:{p95:.3f}:{p99:.3f}:{errors}")
PYEOF
}

# ── Phase 1: Scatter query latency (through keel) ────────────────────────────

log "=== Phase 1/3: keel scatter (GROUP BY query) ==="
IFS=: read -r s_avg s_med s_p95 s_p99 s_err <<< \
    "$(time_queries "keel-scatter-groupby" "$KEEL_HOST" "$KEEL_PORT" "$SCATTER_SQL_TEXT" "$N_QUERIES")"
log "  avg=${s_avg}ms  p50=${s_med}ms  p95=${s_p95}ms  p99=${s_p99}ms  errors=${s_err}"

# ── Phase 2: Direct query latency (one PG shard, no keel) ────────────────────

log "=== Phase 2/3: direct PostgreSQL (same GROUP BY, one shard) ==="
IFS=: read -r d_avg d_med d_p95 d_p99 d_err <<< \
    "$(time_queries "direct-postgres-groupby" "$DIRECT_PG_HOST" "$DIRECT_PG_PORT" "$SCATTER_SQL_TEXT" "$N_QUERIES")"
log "  avg=${d_avg}ms  p50=${d_med}ms  p95=${d_p95}ms  p99=${d_p99}ms  errors=${d_err}"

# ── Phase 3: Null scatter (SELECT 1) — isolates connection overhead ───────────

log "=== Phase 3/3: null scatter (SELECT 1) and direct ==="
NULL_SQL="SELECT 1"
IFS=: read -r ns_avg ns_med ns_p95 ns_p99 ns_err <<< \
    "$(time_queries "keel-scatter-null" "$KEEL_HOST" "$KEEL_PORT" "$NULL_SQL" "$N_QUERIES")"
IFS=: read -r nd_avg nd_med nd_p95 nd_p99 nd_err <<< \
    "$(time_queries "direct-null" "$DIRECT_PG_HOST" "$DIRECT_PG_PORT" "$NULL_SQL" "$N_QUERIES")"

# ── Compute and report overhead breakdown ────────────────────────────────────

python3 - <<PYEOF
scatter_avg = float("${s_avg}")
direct_avg  = float("${d_avg}")
null_keel   = float("${ns_avg}")
null_direct = float("${nd_avg}")

conn_overhead_ms     = null_keel - null_direct
query_overhead_ms    = scatter_avg - direct_avg
conn_fraction        = conn_overhead_ms / scatter_avg if scatter_avg > 0 else 0
query_work_fraction  = (direct_avg) / scatter_avg if scatter_avg > 0 else 0

print()
print("=" * 62)
print("  Scatter Connection Overhead Analysis")
print("=" * 62)
print()
print(f"  Scatter query (keel):     {scatter_avg:.3f} ms avg  (P95={float('${s_p95}'):.3f}ms  P99={float('${s_p99}'):.3f}ms)")
print(f"  Direct query (1 shard):   {direct_avg:.3f} ms avg  (P95={float('${d_p95}'):.3f}ms  P99={float('${d_p99}'):.3f}ms)")
print(f"  Null scatter (SELECT 1):  {null_keel:.3f} ms avg")
print(f"  Null direct  (SELECT 1):  {null_direct:.3f} ms avg")
print()
print(f"  Connection + auth overhead: ~{conn_overhead_ms:.3f} ms per shard")
print(f"    (null_scatter - null_direct = {null_keel:.3f} - {null_direct:.3f})")
print()
print(f"  Total scatter overhead:     +{query_overhead_ms:.3f} ms vs direct")
print(f"  Connection fraction:        {conn_fraction:.1%} of scatter total latency")
print(f"  Query-work fraction:        {query_work_fraction:.1%} of scatter total latency")
print()
print("  Interpretation:")
if conn_fraction > 0.5:
    print("  *** Connection overhead dominates (>50% of total scatter latency).")
    print("  *** Implementing scatter connection pooling would be the highest-impact")
    print("  *** optimization, potentially halving scatter latency at moderate QPS.")
elif conn_fraction > 0.25:
    print("  Connection overhead is significant (25-50% of total scatter latency).")
    print("  Connection pooling would provide measurable gains at high QPS (>200 QPS).")
else:
    print("  Connection overhead is modest (<25% of total scatter latency).")
    print("  Query execution cost dominates; connection pooling is lower priority.")
print()
print(f"  Connection reuse priority: {'HIGH' if conn_fraction > 0.5 else 'MEDIUM' if conn_fraction > 0.25 else 'LOW'}")
print("=" * 62)
PYEOF
