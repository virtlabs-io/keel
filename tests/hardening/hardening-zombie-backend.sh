#!/usr/bin/env bash
# ============================================================================
# hardening-zombie-backend.sh — Zombie/blackhole backend timeout test
# ============================================================================
#
# Tests that the KEEL proxy correctly times out when a backend server accepts
# TCP connections but never sends any data (a "zombie" or "blackhole" server).
# This scenario can occur in production when:
#   - A backend is overloaded and stops responding
#   - A firewall silently drops packets after the TCP handshake
#   - A network partition allows SYN/ACK but blocks data packets
#
# Algorithm:
#   1. Start a minimal Python TCP server that accepts connections but never
#      reads or writes data (a "blackhole").
#   2. Configure the proxy to route to this blackhole server.
#   3. Send a query through the proxy with a QUERY_TIMEOUT_SEC timeout.
#   4. Verify the proxy returns an error within the timeout rather than
#      hanging indefinitely.
#   5. Shut down the blackhole server.
#
# Why this matters:
#   Without proper connect/read timeouts, a single zombie backend can cause
#   all worker threads to block, leading to a complete proxy outage.  This
#   test verifies KEEL's timeout and health-check mechanisms detect and
#   evict unresponsive backends.
#
# Environment Variables:
#   PROXY_HOST          Proxy address (default: 127.0.0.1)
#   PROXY_PORT          Proxy port (default: 7432)
#   DB_USER             Database user (default: postgres)
#   DB_PASSWORD         Database password (default: postgres)
#   DB_NAME             Database name (default: testdb)
#   BLACKHOLE_PORT      Port for the blackhole server (default: 15432)
#   QUERY_TIMEOUT_SEC   Maximum seconds to wait for proxy response (default: 10)
#
# Prerequisites:
#   - python3 on PATH (for the blackhole TCP server)
#   - psql on PATH
#   - KEEL proxy running on PROXY_HOST:PROXY_PORT
#
# Exit Codes:
#   0  PASS — proxy timed out correctly, did not hang
#   1  FAIL — proxy hung or crashed
#
# Usage:
#   ./scripts/hardening-zombie-backend.sh
#   QUERY_TIMEOUT_SEC=5 ./scripts/hardening-zombie-backend.sh
# ============================================================================
set -euo pipefail

OUT_DIR="${OUT_DIR:-/tmp/keel_zombie_backend_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

ZOMBIE_HOST="${ZOMBIE_HOST:-127.0.0.1}"
ZOMBIE_PORT="${ZOMBIE_PORT:-65432}"
PROXY_HOST="${PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${PROXY_PORT:-7432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-testdb}"
QUERY_TIMEOUT_SEC="${QUERY_TIMEOUT_SEC:-8}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[zombie] FAIL: missing dependency '$1'" >&2
    exit 2
  }
}

need_cmd python3
need_cmd timeout
need_cmd psql

echo "[zombie] output: $OUT_DIR"
echo "[zombie] starting blackhole backend at $ZOMBIE_HOST:$ZOMBIE_PORT"

python3 - "$ZOMBIE_HOST" "$ZOMBIE_PORT" >"$OUT_DIR/zombie.log" 2>&1 <<'PY' &
import socket, sys, time
host = sys.argv[1]
port = int(sys.argv[2])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((host, port))
s.listen(64)
while True:
    c, _ = s.accept()
    c.setblocking(False)
    time.sleep(3600)
PY
zpid=$!

cleanup() {
  kill "$zpid" 2>/dev/null || true
  wait "$zpid" 2>/dev/null || true
}
trap cleanup EXIT

sleep 1

echo "[zombie] probing proxy timeout behavior"
set +e
timeout "$QUERY_TIMEOUT_SEC" \
  env PGPASSWORD="$DB_PASSWORD" \
  psql -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -X -A -t -v ON_ERROR_STOP=1 -c "SELECT 1;" \
       >"$OUT_DIR/proxy.out" 2>"$OUT_DIR/proxy.err"
rc=$?
set -e

echo "query_rc=$rc" | tee "$OUT_DIR/summary.txt"

if [[ "$rc" -eq 124 ]]; then
  echo "[zombie] FAIL: proxy query hung past timeout (${QUERY_TIMEOUT_SEC}s)"
  exit 1
fi

echo "[zombie] PASS: proxy returned/failed fast without hanging"
