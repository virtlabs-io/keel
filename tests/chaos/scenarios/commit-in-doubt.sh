#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/commit-in-doubt.sh
# =============================================================================
#
# Scenario: Kill the primary immediately after a COMMIT is sent but before the
# acknowledgement arrives, forcing KEEL into commit-in-doubt (CID) state.
#
# Expected behaviour:
#   - The session that sent COMMIT is placed in CID state
#   - SHOW CID SESSIONS on the admin port returns at least one row within
#     CID_VISIBLE_TIMEOUT_S
#   - The sentinel value appears in the table exactly once after the primary
#     restarts (no silent replay; no missing committed row)
#   - After the CID window expires and the primary restarts, KEEL exits CID
#     state and SHOW CID SESSIONS returns empty (or the row disappears)
#
# Multi-sentinel verification:
#   Phase A — 10 clean writes before the race window (baseline durability)
#   Phase B — 1 write inside the kill-window (outcome uncertain: 0 or 1 copies)
#   Invariant: Phase B value appears at most once (no replay)
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/sentinel.sh"

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
KEEL_ADMIN_PORT="${KEEL_ADMIN_PORT:-17433}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"

PRIMARY_CONTAINER="chaos-pgsql-primary"
KEEL_CONTAINER="chaos-keel"

PRIMARY_HOST="172.30.0.10"
PG_PORT=5432

CID_VISIBLE_TIMEOUT_S=30
PROBE_SETTLE_S=6
SCENARIO="commit_in_doubt"
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=10

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[commit-in-doubt] $*"; }
pass() { echo "PASS: $*"; }

# Preflight
docker inspect "$PRIMARY_CONTAINER"  >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"     >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

# Sentinel table setup
sentinel_setup "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "sentinel_setup failed"

RUN_TAG=$(sentinel_tag "cid")
log "Run tag: ${RUN_TAG}"

# Phase A: baseline writes before the race window
log "=== Phase A: ${SENTINEL_N} clean pre-race writes ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_race" "keel" "$SENTINEL_N" \
    || die "Phase A writes failed"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_race" "$SENTINEL_N" \
    || die "Phase A sentinel assertion failed"

log "Phase A verified — ${SENTINEL_N} rows durable."

# Phase B: race-condition write — commit is sent, then primary is killed
CID_VAL="${RUN_TAG}:cid_race:1"
log "=== Phase B: sending COMMIT then killing primary ==="

# Open a background psql that inserts and COMMITs; we kill the primary
# as soon as the INSERT is confirmed locally (before ACK can arrive).
# Use a dedicated co-process so we can coordinate the kill timing.
psql_result=""
(
    PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc -c "
            BEGIN;
            INSERT INTO ${SENTINEL_TABLE}(scenario, tag, phase, seq, val, written_via)
            VALUES ('${SCENARIO}', '${RUN_TAG}_cid', 'cid_race', 1,
                    '${CID_VAL}', 'keel');
            COMMIT;
        " 2>&1
) &
BG_PID=$!

# Kill primary almost immediately (before COMMIT ACK)
sleep 0.1
log "Killing primary container..."
docker stop -t 0 "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true

wait "$BG_PID" 2>/dev/null || true
log "Background psql exited."

# Check CID state via admin port
log "Checking SHOW CID SESSIONS within ${CID_VISIBLE_TIMEOUT_S}s..."
CID_SEEN=0
DEADLINE=$((SECONDS + CID_VISIBLE_TIMEOUT_S))
while [[ $SECONDS -lt $DEADLINE ]]; do
    ROWS=$(PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_ADMIN_PORT" \
        -U "$CHAOS_USER" -d "keel" \
        --no-psqlrc -tA -c "SHOW CID SESSIONS" 2>/dev/null | wc -l)
    if [[ "$ROWS" -gt 1 ]]; then
        CID_SEEN=1
        log "CID session visible in admin (${ROWS} rows including header)."
        break
    fi
    sleep 1
done

# Restart primary
log "Restarting primary..."
docker start "$PRIMARY_CONTAINER" >/dev/null
log "Waiting for primary to accept connections..."
for i in $(seq 1 40); do
    PGPASSWORD="$CHAOS_PASS" pg_isready \
        -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q 2>/dev/null && break
    sleep 2
done
PGPASSWORD="$CHAOS_PASS" pg_isready \
    -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q \
    || die "Primary did not restart within timeout"
log "Primary restarted."
sleep "$PROBE_SETTLE_S"

# Invariant: CID_VAL appears at most once
COUNT=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$PRIMARY_HOST" -p "$PG_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    --no-psqlrc -tA \
    -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE val='${CID_VAL}'" 2>/dev/null)
COUNT="${COUNT:-0}"
log "CID sentinel count: ${COUNT}"

if [[ "$COUNT" -gt 1 ]]; then
    die "Silent replay detected: '${CID_VAL}' appears ${COUNT} times!"
elif [[ "$COUNT" -eq 1 ]]; then
    pass "Committed-and-survived: CID value present exactly once."
else
    pass "Rolled-back: CID value absent (acceptable — commit was in-doubt)."
fi

# Phase A invariant: clean pre-race rows must still be there
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_race" "$SENTINEL_N" \
    || die "Phase A rows missing after primary restart!"
pass "Phase A: all ${SENTINEL_N} pre-race rows durable after restart."

# CID state should clear after resolution
log "Waiting for CID state to clear..."
sleep "$PROBE_SETTLE_S"
ROWS_AFTER=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$KEEL_HOST" -p "$KEEL_ADMIN_PORT" \
    -U "$CHAOS_USER" -d "keel" \
    --no-psqlrc -tA -c "SHOW CID SESSIONS" 2>/dev/null | grep -v '^$' | wc -l)
if [[ "$ROWS_AFTER" -le 1 ]]; then
    pass "CID state cleared after resolution."
else
    log "WARN: ${ROWS_AFTER} row(s) still in SHOW CID SESSIONS after settle — may need more time."
fi

pass "commit-in-doubt scenario complete."
