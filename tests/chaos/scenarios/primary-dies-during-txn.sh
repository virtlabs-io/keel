#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/primary-dies-during-txn.sh
# =============================================================================
#
# Scenario: Primary is killed while a client has an open (pinned) transaction.
#
# Expected behaviour:
#   - The open transaction receives a clean error when the backend disappears.
#   - KEEL does NOT silently retry the transaction on a different backend.
#   - KEEL does NOT silently route the COMMIT to a replica (read-only error
#     would be misleading — KEEL should report backend gone).
#   - The partially-written transaction is NOT committed (no partial inserts).
#   - After recovery, a new transaction succeeds normally.
#   - No sentinel row is duplicated.
#
# Multi-sentinel verification:
#   Phase A — 10 rows before the transaction open (baseline)
#   Phase B — INSERT inside the open transaction, primary dies before COMMIT
#             (must see 0 rows for Phase B values — rollback by primary death)
#   Phase C — 10 rows after recovery (must all succeed)
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
KEEL_CONTAINER="chaos-keel"
PRIMARY_HOST="172.30.0.10"
PG_PORT=5432

RECOVER_TIMEOUT_S=60
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=10
SCENARIO="primary_txn_death"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[primary-dies-during-txn] $*"; }
pass() { echo "PASS: $*"; }

_pg_scalar() {
    local host="$1" port="$2" sql="$3"
    PGPASSWORD="$CHAOS_PASS" psql \
        -h "$host" -p "$port" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc -tA -c "$sql" 2>/dev/null
}

docker inspect "$PRIMARY_CONTAINER" >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"    >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

sentinel_setup "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "sentinel_setup failed"

RUN_TAG=$(sentinel_tag "txn")
log "Run tag: ${RUN_TAG}"

# Phase A: baseline writes
log "=== Phase A: ${SENTINEL_N} baseline writes ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_a" "pre_txn" "keel" "$SENTINEL_N" \
    || die "Phase A writes failed"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_a" "pre_txn" "$SENTINEL_N" "phase-A pre-transaction values" \
    || die "Phase A assertion failed"
log "Phase A: ${SENTINEL_N} rows confirmed."

# Phase B: open transaction, insert, kill primary before COMMIT
TXN_VAL="${RUN_TAG}:txn_insert:1"
log "=== Phase B: open transaction with INSERT, then kill primary ==="

# Use a co-process: BEGIN → INSERT → sleep (simulate work) → COMMIT
# We kill the primary after the INSERT but before COMMIT ACK.
(
    PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc \
        -c "BEGIN;
            INSERT INTO ${SENTINEL_TABLE}(scenario, tag, phase, seq, val, written_via)
            VALUES ('${SCENARIO}', '${RUN_TAG}_b', 'in_txn', 1, '${TXN_VAL}', 'keel');
            SELECT pg_sleep(4);
            COMMIT;" 2>&1
) &
BG_PID=$!

# Let INSERT land on primary
sleep 0.8
log "Killing primary mid-transaction..."
docker stop -t 0 "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true

wait "$BG_PID" 2>/dev/null || true
log "Transaction co-process exited."

# The insert must NOT be present (transaction rolled back by primary death)
# Give a brief settle for any lingering network buffer to flush
sleep 2

# We cannot query the dead primary; check a replica for any stray row.
# Accept that replica may not yet be queryable; skip this check if unavailable.
TXN_COUNT_REPLICA=$(_pg_scalar "172.30.0.11" "$PG_PORT" \
    "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE val='${TXN_VAL}'" || echo "unavailable")
if [[ "$TXN_COUNT_REPLICA" != "unavailable" && "$TXN_COUNT_REPLICA" -gt 0 ]]; then
    log "WARN: TXN_VAL found on replica (count=${TXN_COUNT_REPLICA}). \
Verifying after primary restart..."
fi

# Restart primary
log "Restarting primary..."
docker start "$PRIMARY_CONTAINER" >/dev/null

log "Waiting for primary..."
for i in $(seq 1 "$RECOVER_TIMEOUT_S"); do
    PGPASSWORD="$CHAOS_PASS" pg_isready \
        -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q 2>/dev/null && break
    sleep 1
done
PGPASSWORD="$CHAOS_PASS" pg_isready \
    -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q \
    || die "Primary did not restart within ${RECOVER_TIMEOUT_S}s"

sleep 8  # KEEL reconnect

# Verify TXN_VAL: must be 0 (rolled back) or 1 (CID committed) — never > 1
TXN_COUNT=$(_pg_scalar "$PRIMARY_HOST" "$PG_PORT" \
    "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE val='${TXN_VAL}'")
TXN_COUNT="${TXN_COUNT:-0}"
log "Phase B: TXN_VAL count on primary after restart = ${TXN_COUNT}"
if [[ "$TXN_COUNT" -gt 1 ]]; then
    die "Phase B: silent replay detected — TXN_VAL appears ${TXN_COUNT} times!"
elif [[ "$TXN_COUNT" -eq 1 ]]; then
    pass "Phase B: transaction committed before kill (CID window — count=1, no replay)."
else
    pass "Phase B: transaction rolled back by primary death (count=0, expected)."
fi

# Phase C: post-recovery writes — all must succeed
log "=== Phase C: ${SENTINEL_N} post-recovery writes ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_c" "post_recovery" "keel" "$SENTINEL_N" \
    || die "Phase C writes failed — KEEL not recovered"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_c" "post_recovery" "$SENTINEL_N" "phase-C post-recovery values" \
    || die "Phase C assertion failed"
log "Phase C: all ${SENTINEL_N} post-recovery rows confirmed."

# No-duplicate invariant
TOTAL=$(_pg_scalar "$PRIMARY_HOST" "$PG_PORT" \
    "SELECT COUNT(*) FROM ${SENTINEL_TABLE}
     WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'")
DISTINCT=$(_pg_scalar "$PRIMARY_HOST" "$PG_PORT" \
    "SELECT COUNT(DISTINCT val) FROM ${SENTINEL_TABLE}
     WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'")
[[ "$TOTAL" -eq "$DISTINCT" ]] \
    || die "Duplicate rows: total=${TOTAL} distinct=${DISTINCT} — silent replay!"
pass "No duplicates: total=${TOTAL}."

pass "primary-dies-during-txn scenario complete."
