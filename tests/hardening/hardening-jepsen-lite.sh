#!/usr/bin/env bash
# ============================================================================
# hardening-jepsen-lite.sh — Chaos/fault injection à la Jepsen
# ============================================================================
#
# Inspired by the Jepsen distributed systems testing framework, this script
# runs a sustained workload through the KEEL proxy while randomly killing
# and restarting backend Docker services to simulate node failures.
#
# The goal is to verify that KEEL handles backend outages, failovers, and
# reconnects without crashing, corrupting state, or losing track of client
# connections.  Unlike the real Jepsen framework, this is a "lite" version
# that does not verify linearizability or consistency — it focuses purely
# on proxy stability under chaos.
#
# Algorithm:
#   1. Launch a sysbench tpcc_like workload against the proxy in the
#      background (DURATION seconds, THREADS concurrent connections).
#   2. In a chaos loop, every INTERVAL seconds:
#      a. Pick a random backend from the TARGETS list.
#      b. Kill it via `docker kill <service>`.
#      c. Wait briefly, then restart via `docker start <service>`.
#      d. Optionally kill the proxy itself (INCLUDE_PROXY=1) to test
#         client reconnection behaviour.
#   3. After DURATION seconds, stop the chaos loop.
#   4. Scan the sysbench output log for hard failure markers:
#      ERROR, FATAL, Segmentation, ABORT.
#   5. PASS if no hard failures found; FAIL otherwise.
#
# Environment Variables:
#   PROXY_HOST       Proxy address (default: 127.0.0.1)
#   PROXY_PORT       Proxy port (default: 7432)
#   DB_USER          Database user (default: postgres)
#   DB_PASSWORD      Database password (default: postgres)
#   DB_NAME          Database name (default: testdb)
#   DURATION         Total test duration in seconds (default: 60)
#   INTERVAL         Seconds between chaos events (default: 5)
#   THREADS          Sysbench thread count (default: 50)
#   TARGETS          Space-separated Docker service names to kill
#                    (default: "pgsql-01 pg-replica1 pg-replica2")
#   INCLUDE_PROXY    Set to 1 to also kill/restart the proxy (default: 0)
#
# Prerequisites:
#   - Docker with backend services running
#   - sysbench with PostgreSQL driver
#   - KEEL proxy running on PROXY_HOST:PROXY_PORT
#
# Exit Codes:
#   0  PASS — workload completed without hard failures
#   1  FAIL — hard failure markers found in workload log
#
# Usage:
#   ./scripts/hardening-jepsen-lite.sh
#   DURATION=120 INTERVAL=3 ./scripts/hardening-jepsen-lite.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/keel_jepsen_lite_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

DURATION="${DURATION:-120}"
INTERVAL="${INTERVAL:-5}"
BACKEND_SERVICES="${BACKEND_SERVICES:-pgsql-01 pg-replica1 pg-replica2}"

# Workload command should keep issuing reads/writes through proxy.
WORKLOAD_CMD="${WORKLOAD_CMD:-sysbench $ROOT_DIR/bench/tpcc_like_persistent.lua --db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port=7432 --pgsql-user=postgres --pgsql-password=postgres --pgsql-db=testdb --report-interval=5 --scale=10 --read-pct=70 --threads=200 --time=$DURATION run}"

# Optional proxy fault command. Example:
#   PROXY_FAULT_CMD='docker compose -f docker/compose.e2e.yml restart keel'
PROXY_FAULT_CMD="${PROXY_FAULT_CMD:-}"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker/compose.e2e.yml}"

echo "[jepsen-lite] output: $OUT_DIR"
echo "[jepsen-lite] workload: $WORKLOAD_CMD"

bash -lc "$WORKLOAD_CMD" > "$OUT_DIR/workload.log" 2>&1 &
WL_PID=$!

cleanup() {
  kill "$WL_PID" 2>/dev/null || true
  wait "$WL_PID" 2>/dev/null || true
}
trap cleanup EXIT

start_ts=$(date +%s)
round=0
while :; do
  now=$(date +%s)
  elapsed=$((now - start_ts))
  if (( elapsed >= DURATION )); then
    break
  fi

  round=$((round + 1))
  action=$((RANDOM % 2))

  if (( action == 0 )); then
    svc=$(echo "$BACKEND_SERVICES" | tr ' ' '\n' | shuf | head -n1)
    echo "[$(date -Is)] round=$round action=kill_backend svc=$svc" | tee -a "$OUT_DIR/faults.log"
    docker compose -f "$COMPOSE_FILE" kill -s KILL "$svc" >/dev/null 2>&1 || true
    sleep 2
    docker compose -f "$COMPOSE_FILE" up -d "$svc" >/dev/null 2>&1 || true
  else
    if [[ -n "$PROXY_FAULT_CMD" ]]; then
      echo "[$(date -Is)] round=$round action=proxy_fault" | tee -a "$OUT_DIR/faults.log"
      bash -lc "$PROXY_FAULT_CMD" >/dev/null 2>&1 || true
    else
      echo "[$(date -Is)] round=$round action=proxy_fault skipped(no PROXY_FAULT_CMD)" | tee -a "$OUT_DIR/faults.log"
    fi
  fi

  sleep "$INTERVAL"
done

wait "$WL_PID" || true

if grep -E "ERROR|FATAL|Assertion|Segmentation|ABORT" -n "$OUT_DIR/workload.log" >/dev/null 2>&1; then
  echo "[jepsen-lite] FAIL: workload log contains hard errors"
  echo "[jepsen-lite] see $OUT_DIR/workload.log"
  exit 1
fi

echo "[jepsen-lite] PASS: workload completed under random fault injection"
echo "[jepsen-lite] artifacts: $OUT_DIR"
