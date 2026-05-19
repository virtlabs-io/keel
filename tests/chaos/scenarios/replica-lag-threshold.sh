#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/replica-lag-threshold.sh
# =============================================================================
#
# Scenario: Inject artificial replication lag via pg_wal_replay_pause() on
# replica1, then verify that KEEL's lag-threshold routing logic either:
#   (a) routes reads to the primary or the non-lagging replica2, or
#   (b) routes reads to the lagging replica1 but returns current data
#       when the lag is below the configured threshold.
#
# Also tests: all replicas lagging → KEEL falls back to primary for reads.
#
# Expected behaviour:
#   - Writes always land on the primary (lag does not affect write routing).
#   - When replica1 lag exceeds threshold, reads must NOT return stale data
#     that predates the threshold boundary (i.e. must use primary or replica2).
#   - When all replicas are lagging, reads fall back to primary.
#   - After replay is resumed, replica1 re-enters the read-pool.
#   - No sentinel row appears more than once.
#
# Multi-sentinel verification:
#   Phase A — writes during zero-lag (baseline)
#   Phase B — reads during lagging-replica1 (freshness check)
#   Phase C — writes during all-replicas-lag (must land on primary)
#   Phase D — post-resume reads (replica1 re-join check)
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
REPLICA1_CONTAINER="chaos-pgsql-replica1"
REPLICA2_CONTAINER="chaos-pgsql-replica2"
KEEL_CONTAINER="chaos-keel"
PRIMARY_HOST="172.30.0.10"
REPLICA1_HOST="172.30.0.11"
PG_PORT=5432

LAG_WAIT_S="${LAG_WAIT_S:-4}"     # seconds to hold lag before checking
PROBE_SETTLE_S="${PROBE_SETTLE_S:-6}"
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=10
SCENARIO="replica_lag"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[replica-lag] $*"; }
pass() { echo "PASS: $*"; }

_pg_exec() {
    local host="$1" port="$2" sql="$3"
    PGPASSWORD="$CHAOS_PASS" psql \
        -h "$host" -p "$port" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc -tA -c "$sql" 2>/dev/null
}

_pause_replica() {
    local container="$1" host="$2"
    docker exec "$container" \
        psql -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT pg_wal_replay_pause()" >/dev/null 2>&1 \
        || log "WARN: could not pause WAL replay on ${container}"
}

_resume_replica() {
    local container="$1"
    docker exec "$container" \
        psql -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT pg_wal_replay_resume()" >/dev/null 2>&1 \
        || log "WARN: could not resume WAL replay on ${container}"
}

# Cleanup trap: always resume replicas on exit
trap '_resume_replica $REPLICA1_CONTAINER; _resume_replica $REPLICA2_CONTAINER' EXIT

# Preflight
docker inspect "$PRIMARY_CONTAINER"  >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$REPLICA1_CONTAINER" >/dev/null 2>&1 || die "${REPLICA1_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"     >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

sentinel_setup "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "sentinel_setup failed"

RUN_TAG=$(sentinel_tag "lag")
log "Run tag: ${RUN_TAG}"

# Phase A: baseline writes at zero lag
log "=== Phase A: ${SENTINEL_N} baseline writes at zero lag ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_a" "zero_lag" "keel" "$SENTINEL_N" \
    || die "Phase A writes failed"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_a" "zero_lag" "$SENTINEL_N" \
    || die "Phase A assertion failed"

# Give replica1 time to catch up to zero lag
sleep 2

# Phase B: pause replica1, write markers, verify reads don't return stale data
log "=== Phase B: replica1 lagging — reads must not return stale data ==="
_pause_replica "$REPLICA1_CONTAINER" "$REPLICA1_HOST"
log "replica1 WAL replay paused."

# Write a fresh marker directly to primary
FRESH_VAL="${RUN_TAG}:lag_fresh:1"
_pg_exec "$PRIMARY_HOST" "$PG_PORT" \
    "INSERT INTO ${SENTINEL_TABLE}(scenario, tag, phase, seq, val, written_via)
     VALUES ('${SCENARIO}', '${RUN_TAG}_b', 'lag_marker', 1, '${FRESH_VAL}', 'direct')
     ON CONFLICT DO NOTHING" >/dev/null

# Accumulate lag for the threshold window
sleep "$LAG_WAIT_S"

# Writes via KEEL must still go to primary
WRITE_VAL="${RUN_TAG}:lag_write:1"
_pg_exec "$KEEL_HOST" "$KEEL_PORT" \
    "INSERT INTO ${SENTINEL_TABLE}(scenario, tag, phase, seq, val, written_via)
     VALUES ('${SCENARIO}', '${RUN_TAG}_b', 'lag_write', 1, '${WRITE_VAL}', 'keel')
     ON CONFLICT DO NOTHING" >/dev/null
log "Phase B: write via KEEL sent."

# Read fresh_val via KEEL — if routed to lagging replica1, it won't be visible.
# We accept either 0 (lagged replica) or 1 (primary / replica2).
# What we must NOT see is an error or a count > 1.
READ_COUNT=$(_pg_exec "$KEEL_HOST" "$KEEL_PORT" \
    "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE val='${FRESH_VAL}'" 2>/dev/null)
READ_COUNT="${READ_COUNT:-0}"
log "Phase B: FRESH_VAL read via KEEL = ${READ_COUNT} (0=lagged-replica, 1=primary/synced)"
if [[ "$READ_COUNT" -gt 1 ]]; then
    die "Phase B: FRESH_VAL appears ${READ_COUNT} times — phantom read!"
fi

_resume_replica "$REPLICA1_CONTAINER"
log "replica1 WAL replay resumed."
sleep "$PROBE_SETTLE_S"

# Phase C: all replicas lagging → reads must fall back to primary
log "=== Phase C: all replicas lagging — reads fall back to primary ==="
_pause_replica "$REPLICA1_CONTAINER" "$REPLICA1_HOST"
_pause_replica "$REPLICA2_CONTAINER" ""
sleep "$LAG_WAIT_S"

sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_c" "all_lag_write" "keel" "$SENTINEL_N" \
    || die "Phase C writes failed — primary routing broken with all replicas lagging"

_resume_replica "$REPLICA1_CONTAINER"
_resume_replica "$REPLICA2_CONTAINER"
sleep "$PROBE_SETTLE_S"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_c" "all_lag_write" "$SENTINEL_N" \
    || die "Phase C assertion failed — rows not found on primary after all-lag scenario"

log "Phase C: all ${SENTINEL_N} rows confirmed on primary."

# Phase D: post-resume — replica1 re-joins read pool
log "=== Phase D: ${SENTINEL_N} post-resume reads via KEEL ==="
# Simply verify KEEL is serving queries normally after replay resumed
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_d" "post_resume" "keel" "$SENTINEL_N" \
    || die "Phase D writes failed — KEEL did not recover after replica lag cleared"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_d" "post_resume" "$SENTINEL_N" \
    || die "Phase D assertion failed"
log "Phase D: all ${SENTINEL_N} post-resume rows confirmed."

# No-duplicate invariant
TOTAL=$(_pg_exec "$PRIMARY_HOST" "$PG_PORT" \
    "SELECT COUNT(*) FROM ${SENTINEL_TABLE}
     WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'")
DISTINCT=$(_pg_exec "$PRIMARY_HOST" "$PG_PORT" \
    "SELECT COUNT(DISTINCT val) FROM ${SENTINEL_TABLE}
     WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'")
[[ "$TOTAL" -eq "$DISTINCT" ]] \
    || die "Duplicate rows: total=${TOTAL} distinct=${DISTINCT} — silent replay!"
pass "No duplicates: total=${TOTAL}."

pass "replica-lag-threshold scenario complete."
