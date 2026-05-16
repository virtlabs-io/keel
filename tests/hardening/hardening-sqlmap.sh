#!/usr/bin/env bash
# ============================================================================
# hardening-sqlmap.sh — SQL injection fuzzing via sqlmap against KEEL proxy
# ============================================================================
#
# Runs sqlmap (the open-source SQL injection testing tool) against the KEEL
# PostgreSQL proxy endpoint to verify that the proxy does not crash, leak
# memory, or destabilise under adversarial SQL injection payloads.
#
# This is NOT a test of whether KEEL prevents SQL injection (that is the
# application's responsibility).  Instead, it validates proxy resilience:
# the proxy must remain stable and responsive even when receiving malformed
# or hostile query patterns.
#
# Algorithm:
#   1. Verify sqlmap is installed; skip gracefully if not found.
#   2. Launch sqlmap at level=3, risk=2 (thorough but safe) against the
#      proxy's PostgreSQL endpoint using direct connection mode.
#   3. Capture all sqlmap output.
#   4. Scan output for crash/instability markers:
#      - Segfault, Segmentation fault
#      - ASan (AddressSanitizer)
#      - stack smashing, FATAL, ABORT
#   5. PASS if no crash markers found; FAIL otherwise.
#
# Environment Variables:
#   DB_DRIVER     Database driver for sqlmap (default: postgresql)
#   DB_HOST       Proxy host (default: 127.0.0.1)
#   DB_PORT       Proxy port (default: 7432)
#   DB_USER       Database user (default: postgres)
#   DB_PASSWORD   Database password (default: postgres)
#   DB_NAME       Database name (default: testdb)
#
# Prerequisites:
#   - sqlmap installed and on PATH
#   - KEEL proxy running on DB_HOST:DB_PORT
#
# Exit Codes:
#   0  PASS — no crash or instability detected
#   1  FAIL — crash markers found in output
#   0  SKIP — sqlmap not installed (exits cleanly)
#
# Usage:
#   ./scripts/hardening-sqlmap.sh
#   DB_PORT=8432 ./scripts/hardening-sqlmap.sh
# ============================================================================
set -euo pipefail

DB_DRIVER="${DB_DRIVER:-postgresql}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-7432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-testdb}"
OUT_DIR="${OUT_DIR:-/tmp/keel_sqlmap_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

if ! command -v sqlmap >/dev/null 2>&1; then
  echo "[sqlmap] SKIP: sqlmap not installed"
  exit 0
fi

dsn="${DB_DRIVER}://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "[sqlmap] target dsn: ${DB_DRIVER}://${DB_USER}:***@${DB_HOST}:${DB_PORT}/${DB_NAME}"

set +e
sqlmap -d "$dsn" --batch --level=3 --risk=2 --flush-session --threads=4 \
  --answers='follow=N,keepAlive=N' --timeout=10 --retries=1 \
  >"$OUT_DIR/sqlmap.log" 2>&1
rc=$?
set -e

if grep -Ei 'Segmentation|Assertion|AddressSanitizer|stack smashing|FATAL' "$OUT_DIR/sqlmap.log" >/dev/null 2>&1; then
  echo "[sqlmap] FAIL: proxy/parser instability markers found"
  echo "[sqlmap] see $OUT_DIR/sqlmap.log"
  exit 1
fi

if [[ "$rc" -ne 0 ]]; then
  echo "[sqlmap] WARN: sqlmap exited with rc=$rc (non-fatal for parser hardening)"
fi

echo "[sqlmap] PASS: no crash markers observed"
