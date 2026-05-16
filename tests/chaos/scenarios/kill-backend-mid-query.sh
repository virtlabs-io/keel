#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/kill-backend-mid-query.sh
# =============================================================================
#
# Scenario: Kill the PostgreSQL primary while a pgbench workload is running.
#
# Multi-sentinel verification (the core reliability check):
#   Phase A — PRE_KILL  (15 rows via keel): must ALL survive the backend kill
#   Phase B — DURING_KILL (background 5-row txns via keel): each txn is either
#             fully committed or fully rolled back — no partial commits
#   Phase C — POST_RECOVERY (15 rows via keel): must ALL succeed after recovery
#
#   Individual row values are verified (not just counts), preventing false
#   passes from row-count coincidences.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/sentinel.sh"

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"

PRIMARY_CONTAINER="chaos-pgsql-primary"
PRIMARY_HOST="172.30.0.10"
PRIMARY_PORT=5432

RECONNECT_TIMEOUT_S=20
WORKLOAD_DURATION_S=18
KILL_DELAY_S=5
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=15
SENTINEL_TXN_SIZE=5
SCENARIO="kill_backend"

die() { echo "FAIL: $*" >&2; exit 1; }
log() { echo "[kill-backend] $*"; }

_BENCH_LOG=$(mktemp /tmp/chaos-bench-XXXXXX.log)
_BENCH_ERR=$(mktemp /tmp/chaos-bench-err-XXXXXX.log)
_WRITER_PID_FILE=$(mktemp /tmp/sentinel-pid-XXXXXX)
_WRITER_COUNT_FILE=$(mktemp /tmp/sentinel-count-XXXXXX)
echo "0" > "$_WRITER_COUNT_FILE"

_cleanup() {
    local pid
    pid=$(cat "$_WRITER_PID_FILE" 2>/dev/null || echo "0")
    [[ "$pid" -gt 0 ]] && kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
    if docker inspect "$PRIMARY_CONTAINER" >/dev/null 2>&1; then
        if ! docker inspect -f '{{.State.Running}}' "$PRIMARY_CONTAINER" 2>/dev/null | grep -q true; then
            log "(cleanup) Restarting ${PRIMARY_CONTAINER}..."
            docker start "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true
            for _i in $(seq 1 30); do
                docker exec "$PRIMARY_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
                sleep 1
            done
        fi
    fi
    rm -f "$_BENCH_LOG" "$_BENCH_ERR" "$_WRITER_PID_FILE" "$_WRITER_COUNT_FILE"
}
trap _cleanup EXIT

# Preflight
command -v pgbench >/dev/null 2>&1 || die "pgbench not found on PATH"
docker inspect "$PRIMARY_CONTAINER" >/dev/null 2>&1 \
    || die "Container ${PRIMARY_CONTAINER} not running"

# Sentinel table setup
log "Setting up sentinel table..."
sentinel_setup "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "Cannot set up sentinel table on primary"

RUN_TAG=$(sentinel_tag "kill")
log "Run tag: ${RUN_TAG}"

# Phase A: 15 pre-kill sentinel rows via keel
log "=== Phase A: writing ${SENTINEL_N} pre-kill sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_kill" "keel" "$SENTINEL_N" \
    || die "Phase A: pre-kill sentinel batch write failed"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_kill" "$SENTINEL_N" "phase-A pre-kill values" \
    || die "Phase A: not all pre-kill sentinel values present before kill"

# Phase B: start background atomic sentinel writer (5-row txns)
log "=== Phase B: starting background atomic sentinel writer ==="
sentinel_background_writer \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_mid" "during_kill" "$SENTINEL_TXN_SIZE" \
    200 "$_WRITER_PID_FILE" "$_WRITER_COUNT_FILE" "keel"

# Start pgbench workload
log "Starting pgbench workload (${WORKLOAD_DURATION_S}s, 8 clients)..."
PGPASSWORD="$CHAOS_PASS" pgbench \
    -h "$KEEL_HOST" -p "$KEEL_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -c 8 -j 4 -T "$WORKLOAD_DURATION_S" \
    --random-seed=42 \
    > "$_BENCH_LOG" 2> "$_BENCH_ERR" &
BENCH_PID=$!

# Kill primary mid-workload
log "Waiting ${KILL_DELAY_S}s before killing primary..."
sleep "$KILL_DELAY_S"
log "Killing ${PRIMARY_CONTAINER}..."
docker kill "$PRIMARY_CONTAINER" >/dev/null 2>&1 \
    || die "Failed to kill ${PRIMARY_CONTAINER}"
KILL_TIME=$(date +%s)
log "Primary killed at $(date -d @${KILL_TIME} '+%H:%M:%S')"

# Wait for keel to recover
log "Waiting up to ${RECONNECT_TIMEOUT_S}s for keel to recover..."
recovered=0
for i in $(seq 1 "$RECONNECT_TIMEOUT_S"); do
    if PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT 1" >/dev/null 2>&1; then
        RECOVER_TIME=$(date +%s)
        log "keel recovered after $((RECOVER_TIME - KILL_TIME))s"
        recovered=1
        break
    fi
    sleep 1
done

wait "$BENCH_PID" || true

WRITER_TXN_COUNT=$(sentinel_stop_background_writer "$_WRITER_PID_FILE" "$_WRITER_COUNT_FILE")
log "Background writer ran ${WRITER_TXN_COUNT} transactions during kill window"

if [[ $recovered -eq 0 ]]; then
    log "BENCH_LOG:"; cat "$_BENCH_LOG" >&2
    die "keel did not recover within ${RECONNECT_TIMEOUT_S}s after primary kill"
fi

PROCESSED=$(grep -Eo "number of transactions actually processed: [0-9]+" "$_BENCH_LOG" \
    | awk '{print $NF}' | tail -1 || echo "0")
log "pgbench: processed=${PROCESSED:-0}"
[[ "${PROCESSED:-0}" -lt 10 ]] && { cat "$_BENCH_LOG"; die "Too few pgbench transactions (${PROCESSED:-0})"; }

# Restart primary
log "Restarting ${PRIMARY_CONTAINER}..."
docker start "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true
for i in $(seq 1 30); do
    docker exec "$PRIMARY_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 \
        && { log "Primary healthy after ${i}s"; break; }
    sleep 1
done
sleep 3  # let keel re-probe

# Verification: Phase A — all pre-kill rows must still be present (individual value check)
log "=== Verifying Phase A: ${SENTINEL_N} pre-kill rows survived ==="
sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_kill" "$SENTINEL_N" "phase-A survival after kill" \
    || die "Phase A FAIL: pre-kill sentinel rows lost after backend kill"

# Verification: Phase B — all during-kill txns must be atomic (0 or 5 rows each)
log "=== Verifying Phase B: ${WRITER_TXN_COUNT} during-kill transactions are atomic ==="
if [[ "${WRITER_TXN_COUNT:-0}" -gt 0 ]]; then
    sentinel_assert_none_partial \
        "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
        "${RUN_TAG}_mid" "during_kill" "$WRITER_TXN_COUNT" "$SENTINEL_TXN_SIZE" \
        "phase-B atomicity" \
        || die "Phase B FAIL: partial commit detected in during-kill transactions"
fi

# Phase C: 15 post-recovery rows via keel
log "=== Phase C: writing ${SENTINEL_N} post-recovery sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_recovery" "keel" "$SENTINEL_N" \
    || die "Phase C: post-recovery sentinel batch write failed"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_post" "post_recovery" "$SENTINEL_N" "phase-C post-recovery values" \
    || die "Phase C: post-recovery sentinel rows not all visible"

# Cross-phase: pre-kill rows still readable through keel after restart
log "=== Cross-phase: pre-kill rows readable through keel ==="
sentinel_assert_count \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_kill" "$SENTINEL_N" "cross-phase pre-kill via keel" \
    || die "Cross-phase FAIL: pre-kill rows not readable through keel after recovery"

log "PASS: keel reconnected after primary kill in $((RECOVER_TIME - KILL_TIME))s"
log "      Phase A (${SENTINEL_N} pre-kill rows): all present and individually verified"
log "      Phase B (${WRITER_TXN_COUNT} txns x ${SENTINEL_TXN_SIZE} rows): all atomic"
log "      Phase C (${SENTINEL_N} post-recovery rows): all committed and verified"
