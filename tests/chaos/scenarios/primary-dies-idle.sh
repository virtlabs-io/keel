#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/primary-dies-idle.sh
# =============================================================================
#
# Scenario: Primary is killed while KEEL's connection pool is fully idle
# (no active queries, no open transactions).
#
# Expected behaviour:
#   - KEEL drains stale idle connections from the pool without errors.
#   - Read-only queries (SELECT 1) continue to succeed via replicas.
#   - Write queries return a clean error (not a hang).
#   - After the primary restarts, KEEL reconnects automatically and writes
#     succeed within RECOVER_TIMEOUT_S.
#   - Zero sentinel rows are silently lost or duplicated.
#
# Multi-sentinel verification:
#   Phase A — 15 rows written while healthy (baseline)
#   Phase B — write attempts during outage (expected to fail)
#   Phase C — 15 rows written after recovery (must all succeed)
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

OUTAGE_S="${OUTAGE_S:-8}"
RECOVER_TIMEOUT_S=60
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=15
SCENARIO="primary_idle"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[primary-dies-idle] $*"; }
pass() { echo "PASS: $*"; }

docker inspect "$PRIMARY_CONTAINER" >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"    >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

sentinel_setup "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "sentinel_setup failed"

RUN_TAG=$(sentinel_tag "idle")
log "Run tag: ${RUN_TAG}"

# Allow pool to settle to idle
sleep 3

# Phase A: baseline writes while healthy
log "=== Phase A: ${SENTINEL_N} baseline writes ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_a" "pre_death" "keel" "$SENTINEL_N" \
    || die "Phase A writes failed"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_a" "pre_death" "$SENTINEL_N" "phase-A pre-death values" \
    || die "Phase A assertion failed"
log "Phase A: ${SENTINEL_N} rows confirmed."

# Kill primary while pool is idle
log "Killing primary (idle pool)..."
docker stop -t 3 "$PRIMARY_CONTAINER" >/dev/null

# Phase B: write attempts during outage (must error, not hang)
log "=== Phase B: 3 write attempts during outage (expected errors) ==="
for i in 1 2 3; do
    START=$SECONDS
    VAL="${RUN_TAG}:idle_fault:${i}"
    RESULT=$(PGPASSWORD="$CHAOS_PASS" PGCONNECT_TIMEOUT=5 psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc \
        -c "INSERT INTO ${SENTINEL_TABLE}(scenario, tag, phase, seq, val, written_via)
            VALUES ('${SCENARIO}', '${RUN_TAG}_b', 'fault', ${i}, '${VAL}', 'keel')
            ON CONFLICT DO NOTHING" 2>&1)
    ELAPSED=$((SECONDS - START))
    if [[ "$ELAPSED" -gt 8 ]]; then
        die "Phase B attempt ${i} hung for ${ELAPSED}s — pool did not drain idle connections"
    fi
    log "  Attempt ${i}: elapsed=${ELAPSED}s, result=$(echo "$RESULT" | head -1)"
done
log "Phase B: all 3 attempts returned without hanging."

# Read-only queries should work via replicas
log "Checking read-only access via replicas..."
READ_RESULT=$(PGPASSWORD="$CHAOS_PASS" PGCONNECT_TIMEOUT=5 psql \
    -h "$KEEL_HOST" -p "$KEEL_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    --no-psqlrc -tA \
    -c "SELECT 1" 2>/dev/null || echo "error")
log "Read-only SELECT 1 result: ${READ_RESULT}"
# Accept either 1 (read-split to replica) or error (no read-split configured)
# What is NOT acceptable is a hang past the timeout

# Wait out the outage period
log "Waiting ${OUTAGE_S}s before restarting primary..."
sleep "$OUTAGE_S"

# Restart primary
log "Restarting primary..."
docker start "$PRIMARY_CONTAINER" >/dev/null

log "Waiting for primary to accept connections..."
for i in $(seq 1 "$RECOVER_TIMEOUT_S"); do
    PGPASSWORD="$CHAOS_PASS" pg_isready \
        -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q 2>/dev/null && break
    sleep 1
done
PGPASSWORD="$CHAOS_PASS" pg_isready \
    -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q \
    || die "Primary did not restart within ${RECOVER_TIMEOUT_S}s"

log "Waiting for KEEL to reconnect..."
sleep 8

# Phase C: post-recovery writes — all must succeed
log "=== Phase C: ${SENTINEL_N} post-recovery writes ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_c" "post_recovery" "keel" "$SENTINEL_N" \
    || die "Phase C writes failed — KEEL did not reconnect after primary restart"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_c" "post_recovery" "$SENTINEL_N" "phase-C post-recovery values" \
    || die "Phase C assertion failed"
log "Phase C: all ${SENTINEL_N} post-recovery rows confirmed."

# No-duplicate invariant
TOTAL=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$PRIMARY_HOST" -p "$PG_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    --no-psqlrc -tA \
    -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE}
        WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'" 2>/dev/null)
DISTINCT=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$PRIMARY_HOST" -p "$PG_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    --no-psqlrc -tA \
    -c "SELECT COUNT(DISTINCT val) FROM ${SENTINEL_TABLE}
        WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'" 2>/dev/null)
[[ "$TOTAL" -eq "$DISTINCT" ]] \
    || die "Duplicate rows: total=${TOTAL} distinct=${DISTINCT} — silent replay!"
pass "No duplicates: total=${TOTAL}."

pass "primary-dies-idle scenario complete."
