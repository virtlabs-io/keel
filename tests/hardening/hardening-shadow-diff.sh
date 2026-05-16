#!/usr/bin/env bash
# ============================================================================
# hardening-shadow-diff.sh — Shadow diff: direct DB vs proxy output comparison
# ============================================================================
#
# Runs the same SQL file against the database both directly and through the
# KEEL proxy, then diffs the output.  Any difference indicates the proxy is
# altering query results, which is a correctness violation.
#
# This is the gold-standard correctness test for a transparent proxy: the
# proxy must be byte-for-byte identical to a direct connection for all
# supported query types.
#
# Algorithm:
#   1. Preflight: validate the SQL file exists and contains no pgbench
#      meta-commands (\set, \sleep, \setrandom) or variable placeholders
#      (:var) that would require pgbench to execute.
#   2. Preflight: verify direct and proxy connections are reachable.
#   3. Execute the SQL file against the direct backend via psql, capturing
#      stdout to direct_output.txt.
#   4. Execute the same SQL file through the proxy via psql, capturing
#      stdout to proxy_output.txt.
#   5. Diff the two outputs.  If identical: PASS.  If different: FAIL,
#      and write the diff to diff_output.txt for inspection.
#
# Environment Variables:
#   SQL_FILE       Path to the SQL file to execute
#                  (default: <root>/bench/pgbench_read_rw_split.sql)
#   DIRECT_HOST    Direct backend host (default: pgsql-01)
#   DIRECT_PORT    Direct backend port (default: 5432)
#   PROXY_HOST     Proxy host (default: 127.0.0.1)
#   PROXY_PORT     Proxy port (default: 7432)
#   DB_USER        Database user (default: postgres)
#   DB_PASSWORD    Database password (default: postgres)
#   DB_NAME        Database name (default: testdb)
#   OUT_DIR        Output directory for results (default: /tmp/keel_shadow_diff_<timestamp>)
#
# Prerequisites:
#   - psql on PATH
#   - Direct backend and KEEL proxy both running and reachable
#
# Exit Codes:
#   0  PASS — outputs are identical
#   1  FAIL — outputs differ, or preflight check failed
#
# Usage:
#   ./scripts/hardening-shadow-diff.sh
#   SQL_FILE=my_queries.sql ./scripts/hardening-shadow-diff.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SQL_FILE="${SQL_FILE:-$ROOT_DIR/bench/pgbench_read_rw_split.sql}"
OUT_DIR="${OUT_DIR:-/tmp/keel_shadow_diff_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

DIRECT_HOST="${DIRECT_HOST:-127.0.0.1}"
DIRECT_PORT="${DIRECT_PORT:-5432}"
PROXY_HOST="${PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${PROXY_PORT:-7432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-testdb}"

validate_sql_file() {
  if [[ ! -f "$SQL_FILE" ]]; then
    echo "[shadow] FAIL: SQL_FILE not found: $SQL_FILE" >&2
    exit 2
  fi

  if grep -nE '^[[:space:]]*\\' "$SQL_FILE" >/dev/null 2>&1; then
    echo "[shadow] FAIL: SQL_FILE contains psql/pgbench meta-commands (lines starting with '\\')" >&2
    echo "[shadow]       use a plain SQL file for psql-based diff testing" >&2
    exit 2
  fi

  if grep -nE ':[A-Za-z_][A-Za-z0-9_]*' "$SQL_FILE" >/dev/null 2>&1; then
    echo "[shadow] FAIL: SQL_FILE contains variable placeholders like ':var'" >&2
    echo "[shadow]       pgbench scripts are not valid direct psql inputs" >&2
    exit 2
  fi
}

preflight_conn() {
  local host="$1"; local port="$2"; local name="$3"
  if ! PGPASSWORD="$DB_PASSWORD" psql -h "$host" -p "$port" -U "$DB_USER" -d "$DB_NAME" -X -A -t -c "SELECT 1;" >/dev/null 2>&1; then
    echo "[shadow] FAIL: cannot connect to $name ($host:$port)" >&2
    exit 3
  fi
}

run_sql() {
  local host="$1"; local port="$2"; local out="$3"

  local err
  err="${out%.txt}.err"

  PGPASSWORD="$DB_PASSWORD" psql \
    -h "$host" -p "$port" -U "$DB_USER" -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 -X -A -t -f "$SQL_FILE" > "$out" 2> "$err"
}

validate_sql_file
preflight_conn "$DIRECT_HOST" "$DIRECT_PORT" "direct DB"
preflight_conn "$PROXY_HOST" "$PROXY_PORT" "proxy"

echo "[shadow] running direct DB suite"
if ! run_sql "$DIRECT_HOST" "$DIRECT_PORT" "$OUT_DIR/direct.txt"; then
  echo "[shadow] FAIL: direct DB execution error (see $OUT_DIR/direct.err)" >&2
  exit 4
fi

echo "[shadow] running proxy suite"
if ! run_sql "$PROXY_HOST" "$PROXY_PORT" "$OUT_DIR/proxy.txt"; then
  echo "[shadow] FAIL: proxy execution error (see $OUT_DIR/proxy.err)" >&2
  exit 4
fi

if diff -u "$OUT_DIR/direct.txt" "$OUT_DIR/proxy.txt" > "$OUT_DIR/diff.txt"; then
  echo "[shadow] PASS: outputs match"
  exit 0
fi

echo "[shadow] FAIL: output mismatch"
echo "[shadow] see: $OUT_DIR/diff.txt"
exit 1
