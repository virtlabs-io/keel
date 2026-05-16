#!/usr/bin/env bash
# ============================================================================
# hardening-ci.sh — Unified CI entry point for all hardening test suites
# ============================================================================
#
# Master orchestrator script that runs the full KEEL hardening test battery
# in CI.  Each test suite is gated by an environment variable toggle, allowing
# the CI pipeline to enable/disable individual suites per job or stage.
#
# This script always performs a build first (cmake + make), then selectively
# invokes each hardening script based on the RUN_* flags.
#
# Test Suites (in execution order):
#   1. Hardening unit/integration tests (ctest -L hardening)
#      - Toggle: RUN_HARDENING_TESTS (default: 1)
#
#   2. Sanitizer matrix (ASan+UBSan, TSan, optionally MSan)
#      - Toggle: RUN_SANITIZERS (default: 0)
#      - Delegates to: hardening-sanitizers.sh
#
#   3. Shadow diff (direct vs proxy output comparison)
#      - Toggle: RUN_SHADOW_DIFF (default: 0)
#      - Delegates to: hardening-shadow-diff.sh
#      - Auto-generates a default SQL workload if SHADOW_SQL_FILE is missing.
#
#   4. Slow-client backpressure smoke test
#      - Toggle: RUN_SLOW_CLIENT (default: 0)
#      - Delegates to: hardening-slow-client-fast-server.sh
#
#   5. Syscall fault injection (strace -e inject)
#      - Toggle: RUN_CHAOS_SYSCALLS (default: 0)
#      - Delegates to: hardening-syscall-fault-injection.sh
#
#   6. Network fault simulation (tc netem)
#      - Toggle: RUN_CHAOS_NETEM (default: 0)
#      - Delegates to: hardening-netem-jitter.sh
#
#   7. Zombie backend timeout test
#      - Toggle: RUN_CHAOS_ZOMBIE (default: 0)
#      - Delegates to: hardening-zombie-backend.sh
#
#   8. Static analysis (clang scan-build)
#      - Toggle: RUN_SECURITY_SAST (default: 0)
#      - Delegates to: hardening-static-analysis.sh
#
#   9. Binary hardening verification (checksec)
#      - Toggle: RUN_SECURITY_CHECKSEC (default: 1)
#      - Delegates to: hardening-checksec.sh
#
#  10. TLS configuration audit
#      - Toggle: RUN_SECURITY_TLS (default: 0)
#      - Delegates to: hardening-tls-scan.sh
#
#  11. SQL injection fuzzing (sqlmap)
#      - Toggle: RUN_SECURITY_SQLMAP (default: 0)
#      - Delegates to: hardening-sqlmap.sh
#
#  12. Proxy SSV integration end-to-end test
#      - Toggle: RUN_PROXY_SSV_E2E (default: 0)
#      - Runs ctest -R test_proxy_ssv_e2e
#
# Environment Variables:
#   BUILD_DIR             Build directory (default: <root>/build-linux)
#   JOBS                  Parallel build jobs (default: nproc)
#   RUN_*                 Toggle flags for each suite (see above)
#   SHADOW_SQL_FILE       SQL file for shadow-diff (default: /tmp/keel_shadow_plain.sql)
#   SHADOW_DIRECT_HOST    Direct backend host for shadow-diff (default: pgsql-01)
#   SHADOW_DIRECT_PORT    Direct backend port (default: 5432)
#   SHADOW_PROXY_HOST     Proxy host (default: 127.0.0.1)
#   SHADOW_PROXY_PORT     Proxy port (default: 7432)
#   DB_USER/PASSWORD/NAME Database credentials (default: postgres/postgres/testdb)
#
# Prerequisites:
#   - cmake + make/ninja on PATH
#   - Individual script prerequisites (see each script's header)
#
# Exit Codes:
#   0  All enabled suites passed
#   1  Any enabled suite failed
#
# Usage:
#   ./tests/hardening/run_all.sh                                  # defaults (build + checksec)
#   RUN_SANITIZERS=1 RUN_SHADOW_DIFF=1 ./tests/hardening/run_all.sh
#   RUN_HARDENING_TESTS=1 RUN_SECURITY_CHECKSEC=1 ./tests/hardening/run_all.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build-linux}"
export BUILD_DIR
JOBS="${JOBS:-$(nproc)}"

RUN_HARDENING_TESTS="${RUN_HARDENING_TESTS:-1}"
RUN_SANITIZERS="${RUN_SANITIZERS:-0}"
RUN_SHADOW_DIFF="${RUN_SHADOW_DIFF:-0}"
RUN_SLOW_CLIENT="${RUN_SLOW_CLIENT:-0}"
RUN_CHAOS_SYSCALLS="${RUN_CHAOS_SYSCALLS:-0}"
RUN_CHAOS_NETEM="${RUN_CHAOS_NETEM:-0}"
RUN_CHAOS_ZOMBIE="${RUN_CHAOS_ZOMBIE:-0}"
RUN_SECURITY_SAST="${RUN_SECURITY_SAST:-0}"
RUN_SECURITY_CHECKSEC="${RUN_SECURITY_CHECKSEC:-1}"
RUN_SECURITY_TLS="${RUN_SECURITY_TLS:-0}"
RUN_SECURITY_SQLMAP="${RUN_SECURITY_SQLMAP:-0}"
RUN_PROXY_SSV_E2E="${RUN_PROXY_SSV_E2E:-0}"

SHADOW_SQL_FILE="${SHADOW_SQL_FILE:-/tmp/keel_shadow_plain.sql}"
SHADOW_DIRECT_HOST="${SHADOW_DIRECT_HOST:-pgsql-01}"
SHADOW_DIRECT_PORT="${SHADOW_DIRECT_PORT:-5432}"
SHADOW_PROXY_HOST="${SHADOW_PROXY_HOST:-127.0.0.1}"
SHADOW_PROXY_PORT="${SHADOW_PROXY_PORT:-7432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-testdb}"

log() { printf "[hardening-ci] %s\n" "$*"; }

log "configure/build: $BUILD_DIR"
cmake -S "$ROOT_DIR" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}" \
  -DCMAKE_C_STANDARD=23 \
  -DKEEL_ENABLE_TESTS=ON \
  -DKEEL_ENABLE_HARDENING=ON
cmake --build "$BUILD_DIR" -j"$JOBS"

if [[ "$RUN_HARDENING_TESTS" == "1" ]]; then
  log "ctest hardening label"
  ctest --test-dir "$BUILD_DIR" -L hardening --output-on-failure
fi

if [[ "$RUN_SANITIZERS" == "1" ]]; then
  log "sanitizer matrix"
  "$ROOT_DIR/tests/hardening/hardening-sanitizers.sh"
else
  log "sanitizer matrix skipped (RUN_SANITIZERS=0)"
fi

if [[ "$RUN_SHADOW_DIFF" == "1" ]]; then
  if [[ ! -f "$SHADOW_SQL_FILE" ]]; then
    log "writing default plain SQL workload to $SHADOW_SQL_FILE"
    cat > "$SHADOW_SQL_FILE" <<'SQL'
SELECT 1;
SELECT count(*) FROM pgbench_accounts;
SELECT min(aid), max(aid) FROM pgbench_accounts;
SELECT count(*) FROM pgbench_branches;
SELECT count(*) FROM pgbench_tellers;
SQL
  fi

  log "shadow diff (direct=$SHADOW_DIRECT_HOST:$SHADOW_DIRECT_PORT proxy=$SHADOW_PROXY_HOST:$SHADOW_PROXY_PORT)"
  OUT_DIR="${OUT_DIR:-/tmp/keel_shadow_ci_$(date +%Y%m%d_%H%M%S)}" \
  SQL_FILE="$SHADOW_SQL_FILE" \
  DIRECT_HOST="$SHADOW_DIRECT_HOST" DIRECT_PORT="$SHADOW_DIRECT_PORT" \
  PROXY_HOST="$SHADOW_PROXY_HOST" PROXY_PORT="$SHADOW_PROXY_PORT" \
  DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
  "$ROOT_DIR/tests/hardening/hardening-shadow-diff.sh"
else
  log "shadow diff skipped (RUN_SHADOW_DIFF=0)"
fi

if [[ "$RUN_SLOW_CLIENT" == "1" ]]; then
  log "slow-client backpressure smoke"
  TOTAL_ROWS="${TOTAL_ROWS:-1000}" ROW_BYTES="${ROW_BYTES:-2048}" CLIENT_BPS="${CLIENT_BPS:-102400}" \
  DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
  PROXY_HOST="$SHADOW_PROXY_HOST" PROXY_PORT="$SHADOW_PROXY_PORT" \
  "$ROOT_DIR/tests/hardening/hardening-slow-client-fast-server.sh"
else
  log "slow-client smoke skipped (RUN_SLOW_CLIENT=0)"
fi

if [[ "$RUN_CHAOS_SYSCALLS" == "1" ]]; then
  log "chaos syscall fault injection"
  "$ROOT_DIR/tests/hardening/hardening-syscall-fault-injection.sh"
else
  log "chaos syscall injection skipped (RUN_CHAOS_SYSCALLS=0)"
fi

if [[ "$RUN_CHAOS_NETEM" == "1" ]]; then
  log "chaos netem jitter/reorder"
  "$ROOT_DIR/tests/hardening/hardening-netem-jitter.sh"
else
  log "chaos netem skipped (RUN_CHAOS_NETEM=0)"
fi

if [[ "$RUN_CHAOS_ZOMBIE" == "1" ]]; then
  log "chaos zombie backend"
  "$ROOT_DIR/tests/hardening/hardening-zombie-backend.sh"
else
  log "chaos zombie skipped (RUN_CHAOS_ZOMBIE=0)"
fi

if [[ "$RUN_SECURITY_SAST" == "1" ]]; then
  log "security SAST"
  "$ROOT_DIR/tests/hardening/hardening-static-analysis.sh"
else
  log "security SAST skipped (RUN_SECURITY_SAST=0)"
fi

if [[ "$RUN_SECURITY_CHECKSEC" == "1" ]]; then
  log "security checksec"
  "$ROOT_DIR/tests/hardening/hardening-checksec.sh"
else
  log "security checksec skipped (RUN_SECURITY_CHECKSEC=0)"
fi

if [[ "$RUN_SECURITY_TLS" == "1" ]]; then
  log "security TLS scan"
  "$ROOT_DIR/tests/hardening/hardening-tls-scan.sh"
else
  log "security TLS scan skipped (RUN_SECURITY_TLS=0)"
fi

if [[ "$RUN_SECURITY_SQLMAP" == "1" ]]; then
  log "security sqlmap"
  "$ROOT_DIR/tests/hardening/hardening-sqlmap.sh"
else
  log "security sqlmap skipped (RUN_SECURITY_SQLMAP=0)"
fi

if [[ "$RUN_PROXY_SSV_E2E" == "1" ]]; then
  log "proxy SSV integration E2E"
  ctest --test-dir "$BUILD_DIR" -R '^test_proxy_ssv_e2e$' --output-on-failure
else
  log "proxy SSV integration E2E skipped (RUN_PROXY_SSV_E2E=0)"
fi

log "done"
