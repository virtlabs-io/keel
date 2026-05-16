#!/usr/bin/env bash
# ============================================================================
# perf-rootcause-3pass.sh — 3-pass performance root-cause analysis
# ============================================================================
#
# Automated root-cause analysis tool that progressively enables KEEL's
# instrumentation layers across three benchmark passes to pinpoint where
# latency is being spent in the proxy pipeline.
#
# Methodology:
#   The script runs the same sysbench workload three times, each time with
#   an additional instrumentation layer enabled:
#
#   Pass 1: "coarse" — Pool wait + backend wait instrumentation only.
#     Measures total time spent waiting for a pool slot and for a backend
#     connection.  Isolates pool contention from backend latency.
#
#   Pass 2: "split" — Adds query-level split instrumentation.
#     Breaks backend wait into sub-components: query execution time vs I/O
#     time vs reactor scheduling vs framing overhead.  Isolates whether
#     latency is from the database itself or from proxy I/O handling.
#
#   Pass 3: "full" — Adds deferred-send instrumentation.
#     Measures client→backend send queueing.  Detects socket backpressure
#     where the proxy has data to send but the socket is not ready.
#
#   After all passes, a Python analysis script computes:
#   - Per-event average wait times (pool, query, I/O sub-categories)
#   - Share breakdown: what percentage of total query wait is I/O, execution,
#     reactor scheduling, service time, and framing
#   - Cross-pass TPS deltas to quantify instrumentation overhead
#   - Heuristic root-cause diagnosis based on the share breakdown
#
# Instrumentation knobs (KEEL_HOT_INSTR_* environment variables):
#   KEEL_HOT_INSTR_ALL                            Master toggle (set to 0)
#   KEEL_HOT_INSTR_WAIT_POOL                      Pool wait timing
#   KEEL_HOT_INSTR_WAIT_BACKEND                   Backend wait timing
#   KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT       Query-level split timing
#   KEEL_HOT_INSTR_DEFERRED_SEND                  Deferred send timing
#
# Key stats counters consumed (from SHOW STATS admin command):
#   flow_wait_pool_events / flow_wait_pool_ns_total
#   flow_wait_backend_query_events / flow_wait_backend_query_ns_total
#   flow_wait_backend_query_exec_ns_total
#   flow_wait_backend_query_io_ns_total
#   flow_wait_backend_query_io_reactor_ns_total
#   flow_wait_backend_query_io_reactor_ready_ns_total
#   flow_wait_backend_query_io_reactor_ready_wakeup_ns_total
#   flow_wait_backend_query_io_reactor_ready_sched_ns_total
#   flow_wait_backend_query_io_reactor_dispatch_ns_total
#   flow_wait_backend_query_io_service_ns_total
#   flow_wait_backend_query_framing_ns_total
#   flow_wait_backend_query_deferred_send_events / _ns_total
#
# Heuristic diagnosis examples:
#   - "Pool contention dominates" → increase max_pool_size or num_workers
#   - "Backend wait is mostly IO" → network latency or kernel scheduling
#   - "Execution share elevated"  → optimize slow queries on the DB side
#   - "Deferred send events"      → client or socket backpressure detected
#
# Output Artifacts (per pass):
#   <OUT_DIR>/<pass>/keel.log           KEEL proxy log
#   <OUT_DIR>/<pass>/sysbench.txt       Raw sysbench output
#   <OUT_DIR>/<pass>/stats_before.txt   SHOW STATS snapshot before workload
#   <OUT_DIR>/<pass>/stats_after.txt    SHOW STATS snapshot after workload
#   <OUT_DIR>/<pass>/summary.json       Parsed TPS, P95, stat deltas
#   <OUT_DIR>/result.txt                Final root-cause summary
#
# Environment Variables:
#   KEEL_BIN          Path to keel binary (default: build-linux/src/main/keel)
#   CFG_FILE          Path to keel config (default: etc/keel-mix.ini)
#   OUT_DIR           Output directory (default: /tmp/keel_rootcause_3pass_<timestamp>)
#   DB_HOST/PORT      Proxy connection parameters (default: 127.0.0.1:7432)
#   DB_USER/PASSWORD  Database credentials (default: postgres/postgres)
#   DB_NAME           Database name (default: testdb)
#   ADMIN_HOST/PORT   Admin console parameters (default: 127.0.0.1:6433)
#   ADMIN_USER/PASS   Admin credentials (default: admin/admin)
#   THREADS           Sysbench thread count (default: 300)
#   DURATION          Per-pass workload duration in seconds (default: 20)
#   SCALE             pgbench scale factor (default: 10)
#   READ_PCT          Read percentage for tpcc_like (default: 70)
#   READ_TXN_MODE     Read transaction mode (default: readonly)
#   SYSBENCH_SCRIPT   Lua workload script (default: tpcc_like_persistent.lua)
#
# Prerequisites:
#   - KEEL binary built with instrumentation support
#   - sysbench with PostgreSQL driver
#   - psql for admin console queries
#   - python3 for summary/analysis scripts
#   - PostgreSQL backends running and reachable
#
# Exit Codes:
#   0  All three passes completed successfully
#   1  Build/start failure or admin console not ready
#
# Usage:
#   ./bench/perf-rootcause-3pass.sh
#   THREADS=100 DURATION=60 ./bench/perf-rootcause-3pass.sh
#   cat /tmp/keel_rootcause_3pass_*/result.txt
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/keel_rootcause_3pass_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

KEEL_BIN="${KEEL_BIN:-$ROOT_DIR/build-linux/src/main/keel}"
CFG_FILE="${CFG_FILE:-$ROOT_DIR/etc/keel-mix.ini}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-7432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-testdb}"

ADMIN_HOST="${ADMIN_HOST:-127.0.0.1}"
ADMIN_PORT="${ADMIN_PORT:-6433}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"
ADMIN_DB="${ADMIN_DB:-postgres}"

THREADS="${THREADS:-300}"
DURATION="${DURATION:-20}"
SCALE="${SCALE:-10}"
READ_PCT="${READ_PCT:-70}"
READ_TXN_MODE="${READ_TXN_MODE:-readonly}"
SYSBENCH_SCRIPT="${SYSBENCH_SCRIPT:-$ROOT_DIR/bench/tpcc_like_persistent.lua}"

PASS_LIST=(coarse split full)
KEEL_PID=""

log() { printf "[rootcause] %s\n" "$*"; }

cleanup() {
  if [[ -n "${KEEL_PID}" ]]; then
    kill "${KEEL_PID}" 2>/dev/null || true
    wait "${KEEL_PID}" 2>/dev/null || true
    KEEL_PID=""
  fi
}
trap cleanup EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd psql
require_cmd sysbench

wait_admin_ready() {
  local tries=40
  while (( tries > 0 )); do
    if PGPASSWORD="$ADMIN_PASSWORD" psql -h "$ADMIN_HOST" -p "$ADMIN_PORT" -U "$ADMIN_USER" -d "$ADMIN_DB" -At -c "SHOW VERSION;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
    tries=$((tries - 1))
  done
  return 1
}

snap_stats() {
  local out_file="$1"
  PGPASSWORD="$ADMIN_PASSWORD" psql -h "$ADMIN_HOST" -p "$ADMIN_PORT" -U "$ADMIN_USER" -d "$ADMIN_DB" -At -c "SHOW STATS;" > "$out_file"
}

start_keel_for_pass() {
  local pass="$1"
  local pass_dir="$2"

  local wait_pool=1
  local wait_backend=1
  local query_split=0
  local deferred=0

  case "$pass" in
    coarse)
      ;;
    split)
      query_split=1
      ;;
    full)
      query_split=1
      deferred=1
      ;;
    *)
      echo "unknown pass: $pass" >&2
      exit 1
      ;;
  esac

  cleanup

  log "starting KEEL pass=$pass (pool=$wait_pool backend=$wait_backend query_split=$query_split deferred=$deferred)"
  KEEL_HOT_INSTR_ALL=0 \
  KEEL_HOT_INSTR_WAIT_POOL="$wait_pool" \
  KEEL_HOT_INSTR_WAIT_BACKEND="$wait_backend" \
  KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT="$query_split" \
  KEEL_HOT_INSTR_DEFERRED_SEND="$deferred" \
  "$KEEL_BIN" -c "$CFG_FILE" > "$pass_dir/keel.log" 2>&1 &
  KEEL_PID=$!

  if ! wait_admin_ready; then
    log "admin console not ready for pass=$pass"
    exit 1
  fi
}

run_pass() {
  local pass="$1"
  local pass_dir="$OUT_DIR/$pass"
  mkdir -p "$pass_dir"

  start_keel_for_pass "$pass" "$pass_dir"

  snap_stats "$pass_dir/stats_before.txt"

  log "running sysbench for pass=$pass"
  READ_TXN_MODE="$READ_TXN_MODE" sysbench "$SYSBENCH_SCRIPT" \
    --db-driver=pgsql --pgsql-host="$DB_HOST" --pgsql-port="$DB_PORT" \
    --pgsql-user="$DB_USER" --pgsql-password="$DB_PASSWORD" --pgsql-db="$DB_NAME" \
    --report-interval=5 --scale="$SCALE" --read-pct="$READ_PCT" \
    --threads="$THREADS" --time="$DURATION" run > "$pass_dir/sysbench.txt" 2>&1 || true

  snap_stats "$pass_dir/stats_after.txt"

  PGPASSWORD="$ADMIN_PASSWORD" psql -h "$ADMIN_HOST" -p "$ADMIN_PORT" -U "$ADMIN_USER" -d "$ADMIN_DB" -At -c "SHOW VERSION;" > "$pass_dir/admin_version.txt" || true

  cleanup

  python3 - <<'PY' "$pass_dir"
import pathlib, re, json, sys
p = pathlib.Path(sys.argv[1])

def parse_stats(path):
    d = {}
    for ln in path.read_text(errors='ignore').splitlines():
        if '|' in ln:
            k, v = ln.split('|', 1)
            d[k.strip()] = v.strip()
    return d

def iv(d, k):
    try:
        return int(float(d.get(k, '0') or '0'))
    except Exception:
        return 0

def fv(d, k):
    try:
        return float(d.get(k, '0') or '0')
    except Exception:
        return 0.0

before = parse_stats(p / 'stats_before.txt')
after = parse_stats(p / 'stats_after.txt')

keys = [
    'flow_wait_pool_events','flow_wait_pool_ns_total','flow_wait_backend_events','flow_wait_backend_ns_total',
    'flow_wait_backend_query_events','flow_wait_backend_query_ns_total','flow_wait_backend_query_exec_ns_total',
    'flow_wait_backend_query_io_ns_total','flow_wait_backend_query_io_reactor_ns_total',
    'flow_wait_backend_query_io_reactor_ready_ns_total','flow_wait_backend_query_io_reactor_ready_wakeup_ns_total',
    'flow_wait_backend_query_io_reactor_ready_sched_ns_total','flow_wait_backend_query_io_reactor_dispatch_ns_total',
    'flow_wait_backend_query_io_service_ns_total','flow_wait_backend_query_framing_ns_total',
    'flow_wait_backend_query_deferred_send_events','flow_wait_backend_query_deferred_send_ns_total',
    'errors_total','errors_backend','errors_timeout'
]

delta = {k: iv(after, k) - iv(before, k) for k in keys}

txt = (p / 'sysbench.txt').read_text(errors='ignore')

def g(rx):
    m = re.search(rx, txt)
    return float(m.group(1)) if m else float('nan')

summary = {
    'tps': g(r'transactions:\s+\d+\s+\(([0-9.]+) per sec\.\)'),
    'qps': g(r'queries:\s+\d+\s+\(([0-9.]+) per sec\.\)'),
    'p95_ms': g(r'95th percentile:\s+([0-9.]+)'),
    'avg_ms': g(r'avg:\s+([0-9.]+)'),
    'delta': delta,
}

(p / 'summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary))
PY
}

for pass in "${PASS_LIST[@]}"; do
  run_pass "$pass"
done

python3 - <<'PY' "$OUT_DIR"
import json, pathlib, statistics, math, sys
root = pathlib.Path(sys.argv[1])
passes = ['coarse', 'split', 'full']
rows = {}
for p in passes:
    rows[p] = json.loads((root / p / 'summary.json').read_text())

def nz(v):
    return 0 if v is None or (isinstance(v, float) and math.isnan(v)) else v

def pct(a, b):
    if a == 0:
        return float('nan')
    return (b - a) * 100.0 / a

full = rows['full']
d = full['delta']
qev = max(d.get('flow_wait_backend_query_events', 0), 1)
pev = max(d.get('flow_wait_pool_events', 0), 1)
qns = max(d.get('flow_wait_backend_query_ns_total', 0), 1)

avg_pool_us = d.get('flow_wait_pool_ns_total', 0) / pev / 1e3
avg_query_us = d.get('flow_wait_backend_query_ns_total', 0) / qev / 1e3
io_share = d.get('flow_wait_backend_query_io_ns_total', 0) / qns
exec_share = d.get('flow_wait_backend_query_exec_ns_total', 0) / qns
sched_share = d.get('flow_wait_backend_query_io_reactor_ready_sched_ns_total', 0) / qns
service_share = d.get('flow_wait_backend_query_io_service_ns_total', 0) / qns
framing_share = d.get('flow_wait_backend_query_framing_ns_total', 0) / qns

reason = []
if avg_pool_us > avg_query_us * 1.5 and d.get('flow_wait_pool_events', 0) > 0:
    reason.append('Pool contention dominates (WAIT_POOL per-event latency is highest).')
if io_share >= 0.85:
    if sched_share >= 0.15:
        reason.append('Backend wait is mostly IO with notable reactor scheduling delay.')
    else:
        reason.append('Backend wait is mostly IO/network/backend-read wait.')
if exec_share >= 0.20:
    reason.append('Backend execution share is elevated (possible DB CPU/query cost).')
if d.get('flow_wait_backend_query_deferred_send_events', 0) > 0:
    reason.append('Deferred FE→BE send occurred (possible client or socket backpressure).')
if not reason:
    reason.append('Mixed signals; inspect per-pass artifacts for detailed correlation.')

print('\n=== KEEL 3-Pass Root-Cause Summary ===')
for p in passes:
    r = rows[p]
    print(f"{p:>6}: tps={nz(r['tps']):.2f} qps={nz(r['qps']):.2f} p95={nz(r['p95_ms']):.2f}ms")

print('\n--- Full-pass wait breakdown ---')
print(f"avg_pool_wait_us={avg_pool_us:.1f}")
print(f"avg_backend_query_wait_us={avg_query_us:.1f}")
print(f"share_io={io_share*100:.2f}%")
print(f"share_exec={exec_share*100:.2f}%")
print(f"share_sched={sched_share*100:.2f}%")
print(f"share_service={service_share*100:.2f}%")
print(f"share_framing={framing_share*100:.2f}%")
print(f"deferred_send_events={d.get('flow_wait_backend_query_deferred_send_events', 0)}")

print('\n--- Pass deltas ---')
print(f"split_vs_coarse_tps={pct(nz(rows['coarse']['tps']), nz(rows['split']['tps'])):.2f}%")
print(f"full_vs_split_tps={pct(nz(rows['split']['tps']), nz(rows['full']['tps'])):.2f}%")

print('\n--- Likely causes ---')
for r in reason:
    print(f"- {r}")

(root / 'result.txt').write_text('\n'.join([
    'KEEL 3-Pass Root-Cause Summary',
    f"coarse_tps={nz(rows['coarse']['tps']):.2f}",
    f"split_tps={nz(rows['split']['tps']):.2f}",
    f"full_tps={nz(rows['full']['tps']):.2f}",
    f"avg_pool_wait_us={avg_pool_us:.1f}",
    f"avg_backend_query_wait_us={avg_query_us:.1f}",
    f"share_io_pct={io_share*100:.2f}",
    f"share_exec_pct={exec_share*100:.2f}",
    f"share_sched_pct={sched_share*100:.2f}",
    f"deferred_send_events={d.get('flow_wait_backend_query_deferred_send_events', 0)}",
] + [f"cause={r}" for r in reason]))

print(f"\nArtifacts: {root}")
print(f"Result: {root / 'result.txt'}")
PY

log "done: $OUT_DIR"
log "tip: cat $OUT_DIR/result.txt"
