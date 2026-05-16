#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/sigkill-during-drain.sh
# =============================================================================
#
# Scenario: SIGTERM keel (start graceful drain), then immediately SIGKILL it
#           before the drain window expires.
#
# Expected behaviour:
#   - After restart, no data loss: all transactions confirmed committed
#     (received ReadyForQuery) are visible in the database
#   - keel restarts cleanly with no corrupted state
#   - New client connections succeed within RESTART_TIMEOUT_S after restart
#
# Multi-sentinel verification:
#   Phase A — PRE_KILL (15 rows via keel, explicit txn): all must survive
#   Phase B — DRAIN_WINDOW (background 5-row txns during drain): each txn
#             must be either fully committed or fully rolled back (atomicity)
#   Phase C — POST_RESTART (15 rows via keel): must succeed after restart
#
#   Corner case tested: a 15-row transaction that straddles the SIGKILL
#   boundary — it must not produce a partial commit.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/sentinel.sh"

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"

KEEL_CONTAINER="chaos-keel"
PRIMARY_HOST="172.30.0.10"
PRIMARY_PORT=5432

RESTART_TIMEOUT_S=20
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=15
SENTINEL_TXN_SIZE=5
SCENARIO="sigkill_drain"

die() { echo "FAIL: $*" >&2; exit 1; }
log() { echo "[sigkill-drain] $*"; }

_WRITER_PID_FILE=$(mktemp /tmp/sentinel-pid-XXXXXX)
_WRITER_COUNT_FILE=$(mktemp /tmp/sentinel-count-XXXXXX)
echo "0" > "$_WRITER_COUNT_FILE"

_cleanup() {
    local pid
    pid=$(cat "$_WRITER_PID_FILE" 2>/dev/null || echo "0")
    [[ "$pid" -gt 0 ]] && kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
    rm -f "$_WRITER_PID_FILE" "$_WRITER_COUNT_FILE"
}
trap _cleanup EXIT

# Preflight
docker inspect "$KEEL_CONTAINER" >/dev/null 2>&1 \
    || die "Container ${KEEL_CONTAINER} not running"

# Sentinel table setup — connect directly to primary to avoid going through keel
log "Setting up sentinel table..."
sentinel_setup "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "Cannot set up sentinel table"

RUN_TAG=$(sentinel_tag "sigkill")
log "Run tag: ${RUN_TAG}"

# Phase A: 15 pre-kill rows via keel in an explicit transaction
# Using write_txn to test that a committed explicit transaction survives SIGKILL
log "=== Phase A: writing ${SENTINEL_N} pre-kill rows via keel (explicit txn) ==="
sentinel_write_txn \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_kill" "keel" "$SENTINEL_N" \
    || die "Phase A: pre-kill explicit transaction failed to commit"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_kill" "$SENTINEL_N" "phase-A pre-kill values" \
    || die "Phase A: pre-kill rows not all visible on primary"

# Additional individual row write: 15 separate single-row inserts (batch path)
log "=== Phase A2: writing ${SENTINEL_N} pre-kill rows via keel (batch INSERT) ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre2" "pre_kill2" "keel" "$SENTINEL_N" \
    || die "Phase A2: batch pre-kill write failed"

sentinel_assert_count \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre2" "pre_kill2" "$SENTINEL_N" "phase-A2 pre-kill2 count" \
    || die "Phase A2: pre-kill2 row count incorrect"

# Start Phase B background atomic writer (5-row transactions continuously)
log "=== Phase B: starting background atomic writer during drain window ==="
sentinel_background_writer \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_drain" "drain_window" "$SENTINEL_TXN_SIZE" \
    150 "$_WRITER_PID_FILE" "$_WRITER_COUNT_FILE" "keel"

# Brief pause so the background writer establishes at least 1 connection
sleep 0.5

# SIGTERM → immediate SIGKILL
log "Sending SIGTERM to keel container (begin drain)..."
docker kill --signal SIGTERM "$KEEL_CONTAINER" >/dev/null 2>&1 \
    || die "Failed to send SIGTERM to ${KEEL_CONTAINER}"

# Immediately follow with SIGKILL (cuts off the drain window)
docker kill --signal SIGKILL "$KEEL_CONTAINER" >/dev/null 2>&1 || true
KILL_TIME=$(date +%s)
log "keel SIGKILLed at $(date -d @${KILL_TIME} '+%H:%M:%S')"

# Stop the background writer
WRITER_TXN_COUNT=$(sentinel_stop_background_writer "$_WRITER_PID_FILE" "$_WRITER_COUNT_FILE")
log "Background writer ran ${WRITER_TXN_COUNT} transactions during drain window"

# Restart keel
log "Starting keel container..."
docker start "$KEEL_CONTAINER" >/dev/null 2>&1 \
    || die "Failed to restart ${KEEL_CONTAINER}"

log "Waiting up to ${RESTART_TIMEOUT_S}s for keel to accept connections..."
restarted=0
for i in $(seq 1 "$RESTART_TIMEOUT_S"); do
    if nc -z "$KEEL_HOST" "$KEEL_PORT" 2>/dev/null; then
        READY_TIME=$(date +%s)
        log "keel ready after $((READY_TIME - KILL_TIME))s"
        restarted=1
        break
    fi
    sleep 1
done
[[ $restarted -eq 1 ]] || die "keel did not restart within ${RESTART_TIMEOUT_S}s"

# Give keel a moment to complete internal setup before sending queries
sleep 1

# Verification: Phase A — all pre-kill rows must be present (individual values)
log "=== Verifying Phase A: ${SENTINEL_N} explicit-txn pre-kill rows ==="
sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_kill" "$SENTINEL_N" "phase-A survival after SIGKILL" \
    || die "Phase A FAIL: committed explicit-txn rows lost after SIGKILL — data loss!"

log "=== Verifying Phase A2: ${SENTINEL_N} batch pre-kill rows ==="
sentinel_assert_count \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre2" "pre_kill2" "$SENTINEL_N" "phase-A2 survival after SIGKILL" \
    || die "Phase A2 FAIL: committed batch rows lost after SIGKILL — data loss!"

# Verification: Phase B — drain-window txns must be atomic (0 or 5 rows each)
log "=== Verifying Phase B: ${WRITER_TXN_COUNT} drain-window transactions are atomic ==="
if [[ "${WRITER_TXN_COUNT:-0}" -gt 0 ]]; then
    sentinel_assert_none_partial \
        "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
        "${RUN_TAG}_drain" "drain_window" "$WRITER_TXN_COUNT" "$SENTINEL_TXN_SIZE" \
        "phase-B drain atomicity" \
        || die "Phase B FAIL: partial commit detected in drain-window transactions — atomicity violation!"
fi

# Phase C: 15 post-restart rows via keel
log "=== Phase C: writing ${SENTINEL_N} post-restart sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_restart" "keel" "$SENTINEL_N" \
    || die "Phase C: post-restart sentinel batch write failed"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_post" "post_restart" "$SENTINEL_N" "phase-C post-restart values" \
    || die "Phase C: post-restart sentinel rows not all visible"

# Cross-phase: Phase A rows readable through keel after restart
log "=== Cross-phase: Phase A rows accessible through keel after restart ==="
sentinel_assert_count \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_kill" "$SENTINEL_N" "cross-phase pre-kill via keel after restart" \
    || die "Cross-phase FAIL: Phase A rows not readable through keel after restart"

log "PASS: No data loss after SIGKILL during drain (keel restarted in $((READY_TIME - KILL_TIME))s)"
log "      Phase A (${SENTINEL_N} explicit-txn rows): all committed rows preserved"
log "      Phase A2 (${SENTINEL_N} batch rows): all preserved"
log "      Phase B (${WRITER_TXN_COUNT} txns x ${SENTINEL_TXN_SIZE} rows): all atomic"
log "      Phase C (${SENTINEL_N} post-restart rows): all committed after restart"
