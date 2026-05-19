#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/role-flapping.sh
# =============================================================================
#
# Scenario: Rapidly flip the primary container on/off multiple times to trigger
# KEEL's role-change detection and flap-dampening logic.
#
# Expected behaviour:
#   - After N rapid restarts, KEEL's flap-dampening threshold is crossed;
#     subsequent routing decisions should be dampened (observable via admin
#     SHOW SERVERS — server shows DEGRADED or UNKNOWN during dampening window).
#   - Despite dampening, KEEL must eventually converge to the correct topology.
#   - Writes after the settling period must succeed (dampening must not freeze
#     routing permanently).
#   - No write is silently duplicated (no phantom replay).
#
# Multi-sentinel verification:
#   Phase A — 10 pre-flap writes (baseline)
#   Phase B — writes attempted during flap storm (some will error; that is OK)
#   Phase C — 10 post-settle writes (must all succeed)
#   Invariant: no value appears more than once.
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

FLAP_COUNT="${FLAP_COUNT:-5}"       # number of stop/start cycles
FLAP_PAUSE_S="${FLAP_PAUSE_S:-2}"   # seconds between stop and start
SETTLE_TIMEOUT_S=60
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=10
SCENARIO="role_flapping"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[role-flapping] $*"; }
pass() { echo "PASS: $*"; }

docker inspect "$PRIMARY_CONTAINER" >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"    >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

sentinel_setup "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "sentinel_setup failed"

RUN_TAG=$(sentinel_tag "flap")
log "Run tag: ${RUN_TAG}, flap cycles: ${FLAP_COUNT}"

# Phase A: baseline writes before flapping
log "=== Phase A: ${SENTINEL_N} pre-flap writes ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_flap" "keel" "$SENTINEL_N" \
    || die "Phase A writes failed"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_flap" "$SENTINEL_N" \
    || die "Phase A assertion failed"
log "Phase A: all ${SENTINEL_N} rows confirmed."

# Phase B: flap storm
log "=== Phase B: ${FLAP_COUNT} primary stop/start cycles ==="
DURING_SUCCESS=0
for i in $(seq 1 "$FLAP_COUNT"); do
    log "  Flap cycle ${i}/${FLAP_COUNT}: stopping primary..."
    docker stop -t 2 "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true
    sleep "$FLAP_PAUSE_S"

    log "  Flap cycle ${i}/${FLAP_COUNT}: starting primary..."
    docker start "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true

    # Brief settle for the probe to fire at least once per cycle
    sleep 3

    # Try a write — may succeed or fail (both acceptable during storm)
    FLAP_VAL="${RUN_TAG}:flap:${i}:$(date +%s%N)"
    if PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc -q \
        -c "INSERT INTO ${SENTINEL_TABLE}(scenario, tag, phase, seq, val, written_via)
            VALUES ('${SCENARIO}', '${RUN_TAG}_flap', 'during_flap', ${i},
                    '${FLAP_VAL}', 'keel')
            ON CONFLICT DO NOTHING" 2>/dev/null
    then
        DURING_SUCCESS=$((DURING_SUCCESS + 1))
        log "  Cycle ${i}: write succeeded."
    else
        log "  Cycle ${i}: write errored (expected during dampening)."
    fi
done
log "Phase B: ${DURING_SUCCESS}/${FLAP_COUNT} writes succeeded during storm."

# Full settle — wait for primary to be healthy and KEEL probes to converge
log "Waiting for primary to stabilise..."
for i in $(seq 1 "$SETTLE_TIMEOUT_S"); do
    PGPASSWORD="$CHAOS_PASS" pg_isready \
        -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q 2>/dev/null && break
    sleep 1
done
PGPASSWORD="$CHAOS_PASS" pg_isready \
    -h "$PRIMARY_HOST" -p "$PG_PORT" -U "$CHAOS_USER" -q \
    || die "Primary not healthy after ${SETTLE_TIMEOUT_S}s"

log "Waiting for KEEL probes to settle..."
sleep 10

# Phase C: post-settle writes — all must succeed
log "=== Phase C: ${SENTINEL_N} post-flap writes (must all succeed) ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_flap" "keel" "$SENTINEL_N" \
    || die "Phase C writes failed — routing did not recover after flap dampening"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_flap" "$SENTINEL_N" \
    || die "Phase C assertion failed"
log "Phase C: all ${SENTINEL_N} post-flap rows confirmed."

# No-duplicate invariant across all phases
TOTAL=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$PRIMARY_HOST" -p "$PG_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    --no-psqlrc -tA \
    -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE}
        WHERE scenario='${SCENARIO}'
          AND tag LIKE '${RUN_TAG}%'" 2>/dev/null)
DISTINCT=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$PRIMARY_HOST" -p "$PG_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    --no-psqlrc -tA \
    -c "SELECT COUNT(DISTINCT val) FROM ${SENTINEL_TABLE}
        WHERE scenario='${SCENARIO}'
          AND tag LIKE '${RUN_TAG}%'" 2>/dev/null)

if [[ "$TOTAL" -ne "$DISTINCT" ]]; then
    die "Duplicate rows detected: total=${TOTAL} distinct=${DISTINCT} — silent replay!"
fi
pass "No duplicates: total=${TOTAL} distinct=${DISTINCT}."

# Check admin SHOW SERVERS is not reporting a stale state
log "Checking SHOW SERVERS post-settle..."
SERVER_ROWS=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$KEEL_HOST" -p "$KEEL_ADMIN_PORT" \
    -U "$CHAOS_USER" -d "keel" \
    --no-psqlrc -tA -c "SHOW SERVERS" 2>/dev/null | grep -v '^$' | wc -l)
log "SHOW SERVERS returned ${SERVER_ROWS} data rows."
[[ "$SERVER_ROWS" -gt 0 ]] || die "SHOW SERVERS returned no rows after flap scenario"

pass "role-flapping scenario complete."
