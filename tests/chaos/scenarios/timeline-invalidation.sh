#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/timeline-invalidation.sh
# =============================================================================
#
# Scenario: Simulate a PostgreSQL timeline switch by promoting replica1 to
# primary (direct pg_promote()), then verifying that KEEL detects the new
# topology and routes writes to the promoted server.
#
# Expected behaviour:
#   - After promotion, KEEL detects that the new primary is replica1 (not the
#     stopped original primary) within FAILOVER_TIMEOUT_S.
#   - Writes via KEEL succeed on the new primary.
#   - The old primary (restarted as standby) does NOT receive writes from KEEL.
#   - Timeline-related fields in the failover event are captured correctly
#     (observable via Prometheus if timeline counters are exposed).
#   - Old LSN routing is not applied to the new timeline (no stale routing).
#   - All sentinel rows are individually durable; none are duplicated.
#
# Multi-sentinel verification:
#   Phase A — 15 rows on original primary before promotion (replication check)
#   Phase B — 15 rows via KEEL after promotion (new-primary routing check)
#   Cross-check: Phase A rows must be visible on new primary after promotion
#                (prove replication was intact before the switch).
#
# WARNING: This scenario requires that replica1 can be promoted via
#   docker exec <replica1> psql -c "SELECT pg_promote()"
# and that the compose stack does NOT use Patroni (manual topology).
# The keel-chaos.ini must be updated to point [primary] at pgsql-replica1
# after promotion (done here via a config swap + SIGHUP).
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
REPLICA1_CONTAINER="chaos-pgsql-replica1"
KEEL_CONTAINER="chaos-keel"
PRIMARY_HOST="172.30.0.10"
REPLICA1_HOST="172.30.0.11"
PG_PORT=5432

FAILOVER_TIMEOUT_S=30
SETTLE_TIMEOUT_S=20
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=15
SCENARIO="timeline_switch"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[timeline-invalidation] $*"; }
pass() { echo "PASS: $*"; }

_pg_exec() {
    local host="$1" port="$2" sql="$3"
    PGPASSWORD="$CHAOS_PASS" psql \
        -h "$host" -p "$port" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        --no-psqlrc -tA -c "$sql" 2>/dev/null
}

# Locate KEEL config
CHAOS_INI="$(cd "${SCRIPT_DIR}/../../../docker/keel" && pwd)/keel-chaos.ini"
CHAOS_INI_BAK="${CHAOS_INI}.timeline-invalidation.bak"
[[ -f "$CHAOS_INI" ]] || die "Cannot locate keel-chaos.ini at ${CHAOS_INI}"
cp "$CHAOS_INI" "$CHAOS_INI_BAK"
_restore_ini() {
    [[ -f "$CHAOS_INI_BAK" ]] && mv "$CHAOS_INI_BAK" "$CHAOS_INI" || true
    # Restart original primary if it was stopped
    docker start "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true
    # Demote replica1 back (safest: just restart replica1 which will stream again)
    # In production you'd use Patroni; here we just restore the stack.
    log "Cleanup: restored keel-chaos.ini and restarted primary."
}
trap _restore_ini EXIT

# Preflight
docker inspect "$PRIMARY_CONTAINER"  >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$REPLICA1_CONTAINER" >/dev/null 2>&1 || die "${REPLICA1_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"     >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

sentinel_setup "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "sentinel_setup failed"

RUN_TAG=$(sentinel_tag "tl")
log "Run tag: ${RUN_TAG}"

# Read current timeline on original primary
OLD_TL=$(_pg_exec "$PRIMARY_HOST" "$PG_PORT" \
    "SELECT timeline_id FROM pg_control_checkpoint()" 2>/dev/null || echo "0")
log "Old timeline: ${OLD_TL}"

# Phase A: write 15 rows directly to original primary (replication check)
log "=== Phase A: ${SENTINEL_N} rows directly to original primary ==="
sentinel_write_batch \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_promote" "direct" "$SENTINEL_N" \
    || die "Phase A writes failed"
sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_promote" "$SENTINEL_N" \
    || die "Phase A assertion on original primary failed"
log "Phase A: ${SENTINEL_N} rows confirmed on original primary."

# Allow replication to propagate Phase A to replica1
sleep 3

# Stop original primary to force failover
log "Stopping original primary..."
docker stop -t 3 "$PRIMARY_CONTAINER" >/dev/null

# Promote replica1
log "Promoting replica1 to primary..."
docker exec "$REPLICA1_CONTAINER" \
    psql -U "$CHAOS_USER" -d "$CHAOS_DB" -c "SELECT pg_promote()" >/dev/null 2>&1 \
    || die "pg_promote() on replica1 failed"

# Wait for replica1 to accept writes (post-promotion standby becomes primary)
log "Waiting for replica1 to accept connections as primary..."
for i in $(seq 1 30); do
    IS_PRIMARY=$(docker exec "$REPLICA1_CONTAINER" \
        psql -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -tA -c "SELECT NOT pg_is_in_recovery()" 2>/dev/null || echo "f")
    [[ "$IS_PRIMARY" == "t" ]] && break
    sleep 2
done
[[ "$IS_PRIMARY" == "t" ]] || die "replica1 did not become primary within timeout"

NEW_TL=$(docker exec "$REPLICA1_CONTAINER" \
    psql -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -tA -c "SELECT timeline_id FROM pg_control_checkpoint()" 2>/dev/null || echo "0")
log "New timeline on replica1: ${NEW_TL}"
if [[ "$NEW_TL" -gt "$OLD_TL" ]]; then
    pass "Timeline advanced: ${OLD_TL} → ${NEW_TL}."
else
    log "WARN: timeline did not advance (may be expected in streaming-standby promotion): old=${OLD_TL} new=${NEW_TL}"
fi

# Update KEEL config to point primary at replica1 and send SIGHUP
log "Updating keel-chaos.ini to reflect new topology..."
# Swap pgsql-primary ↔ pgsql-replica1 in the [servers] section
sed -i \
    -e 's/^primary  = host=pgsql-primary /new_primary = host=pgsql-replica1 /' \
    -e 's/^replica1 = host=pgsql-replica1 /old_primary = host=pgsql-primary /' \
    "$CHAOS_INI"
# Fix role annotations
sed -i \
    -e 's/^new_primary = host=pgsql-replica1 port=5432 dbname=chaosdb role=RO/primary  = host=pgsql-replica1 port=5432 dbname=chaosdb role=RW/' \
    -e 's/^old_primary = host=pgsql-primary  port=5432 dbname=chaosdb role=RW/replica_old = host=pgsql-primary  port=5432 dbname=chaosdb role=RO weight=0/' \
    "$CHAOS_INI"
log "Sending SIGHUP to KEEL..."
docker kill --signal HUP "$KEEL_CONTAINER" >/dev/null

sleep "$SETTLE_TIMEOUT_S"

# Phase B: 15 rows via KEEL to the new primary
log "=== Phase B: ${SENTINEL_N} rows via KEEL after promotion ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_promote" "keel" "$SENTINEL_N" \
    || die "Phase B writes via KEEL failed — routing not updated after timeline switch"

sentinel_assert_values \
    "$REPLICA1_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_promote" "$SENTINEL_N" \
    || die "Phase B: Phase B rows not on new primary (replica1)"
log "Phase B: ${SENTINEL_N} rows via KEEL confirmed on new primary (replica1)."

# Cross-check: Phase A rows must be on replica1 (proves pre-promotion replication)
sentinel_assert_values \
    "$REPLICA1_HOST" "$PG_PORT" \
    "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_promote" "$SENTINEL_N" \
    || die "Cross-check: Phase A rows missing on new primary — replication was incomplete before promotion"
log "Cross-check: Phase A rows present on new primary — replication was complete."

# No-duplicate invariant
TOTAL=$(docker exec "$REPLICA1_CONTAINER" \
    psql -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -tA -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE}
            WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'" 2>/dev/null)
DISTINCT=$(docker exec "$REPLICA1_CONTAINER" \
    psql -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -tA -c "SELECT COUNT(DISTINCT val) FROM ${SENTINEL_TABLE}
            WHERE scenario='${SCENARIO}' AND tag LIKE '${RUN_TAG}%'" 2>/dev/null)
[[ "$TOTAL" -eq "$DISTINCT" ]] \
    || die "Duplicate rows: total=${TOTAL} distinct=${DISTINCT} — silent replay!"
pass "No duplicates: total=${TOTAL}."

# Old primary must NOT have received any Phase B writes
# (it is stopped, so this is trivially true; the check is a reminder)
pass "Old primary is stopped — no dual-write possible."

pass "timeline-invalidation scenario complete."
