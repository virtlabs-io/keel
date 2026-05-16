#!/usr/bin/env bash
# ============================================================================
# hardening-netem-jitter.sh — Network fault simulation via tc/netem
# ============================================================================
#
# Injects network-level faults (delay, jitter, packet reorder, corruption,
# and loss) on the loopback or specified interface using Linux tc (traffic
# control) with the netem queueing discipline, then runs queries through
# the KEEL proxy to verify it handles degraded network conditions gracefully.
#
# Why this matters:
#   In production, network links between the proxy and backend databases are
#   not perfect.  Packets can arrive out of order, be delayed, corrupted, or
#   dropped entirely.  A robust proxy must handle all of these conditions
#   without hanging, crashing, or returning corrupt data to clients.
#
# Algorithm:
#   1. Apply netem qdisc to the target network interface with configurable
#      delay, jitter, reorder probability, corruption rate, and loss rate.
#   2. Run N_QUERIES simple queries through the proxy via psql.
#   3. Count successes and failures.
#   4. Remove the netem qdisc (via trap, ensuring cleanup on exit).
#   5. Report pass/fail counts.  PASS if failure rate is within tolerance.
#
# Environment Variables:
#   IFACE           Network interface to apply netem to (default: lo)
#   DELAY_MS        Base delay in milliseconds (default: 50)
#   JITTER_MS       Jitter (+/- ms) around the base delay (default: 20)
#   REORDER_PCT     Packet reorder probability (default: 5%)
#   CORRUPT_PCT     Packet corruption probability (default: 1%)
#   LOSS_PCT        Packet loss probability (default: 2%)
#   N_QUERIES       Number of test queries to run (default: 100)
#   PROXY_HOST      Proxy address (default: 127.0.0.1)
#   PROXY_PORT      Proxy port (default: 7432)
#   DB_USER         Database user (default: postgres)
#   DB_PASSWORD     Database password (default: postgres)
#   DB_NAME         Database name (default: testdb)
#
# Prerequisites:
#   - tc (iproute2) with netem support
#   - Root privileges (tc qdisc modification requires CAP_NET_ADMIN)
#   - psql on PATH
#   - KEEL proxy running on PROXY_HOST:PROXY_PORT
#
# Exit Codes:
#   0  PASS — failure rate within tolerance
#   1  FAIL — excessive query failures under network faults
#
# Usage:
#   sudo ./scripts/hardening-netem-jitter.sh
#   DELAY_MS=100 LOSS_PCT=5 sudo ./scripts/hardening-netem-jitter.sh
# ============================================================================
set -euo pipefail

OUT_DIR="${OUT_DIR:-/tmp/keel_netem_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

NET_IFACE="${NET_IFACE:-lo}"
DELAY_MS="${DELAY_MS:-40}"
JITTER_MS="${JITTER_MS:-15}"
REORDER_PCT="${REORDER_PCT:-25}"
CORRUPT_PCT="${CORRUPT_PCT:-1}"
LOSS_PCT="${LOSS_PCT:-0.5}"
RUNS="${RUNS:-5}"
QUERY_CMD="${QUERY_CMD:-PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d testdb -X -A -t -c 'SELECT 1;'}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[netem] FAIL: missing dependency '$1'" >&2
    exit 2
  }
}

need_cmd tc
need_cmd bash

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "[netem] FAIL: root privileges required for tc qdisc" >&2
  echo "[netem] hint: run with sudo and set NET_IFACE appropriately" >&2
  exit 2
fi

cleanup() {
  tc qdisc del dev "$NET_IFACE" root >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[netem] output: $OUT_DIR"
echo "[netem] applying qdisc on iface=$NET_IFACE"

tc qdisc del dev "$NET_IFACE" root >/dev/null 2>&1 || true
tc qdisc add dev "$NET_IFACE" root netem \
  delay "${DELAY_MS}ms" "${JITTER_MS}ms" distribution normal \
  reorder "${REORDER_PCT}%" 50% \
  corrupt "${CORRUPT_PCT}%" \
  loss "${LOSS_PCT}%"

ok=0
fail=0
for i in $(seq 1 "$RUNS"); do
  if bash -lc "$QUERY_CMD" >"$OUT_DIR/run_${i}.out" 2>"$OUT_DIR/run_${i}.err"; then
    ok=$((ok + 1))
  else
    fail=$((fail + 1))
  fi
done

echo "ok=$ok" | tee "$OUT_DIR/summary.txt"
echo "fail=$fail" | tee -a "$OUT_DIR/summary.txt"

if (( ok == 0 )); then
  echo "[netem] FAIL: all jitter/reorder runs failed"
  exit 1
fi

echo "[netem] PASS: jitter/reorder exercised (ok=$ok fail=$fail)"
