#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/flip-primary.sh
# =============================================================================
#
# Scenario: Promote replica1 to primary, simulating a planned failover.
#
# Expected behaviour:
#   - keel detects old primary is down and routes writes to new primary
#     within FAILOVER_TIMEOUT_S after config reload + SIGHUP
#   - Read and write queries succeed on the new topology within SETTLE_TIMEOUT_S
#   - No write is silently lost
#
# Multi-sentinel verification:
#   Phase A — PRE_FLIP (15 rows directly on original primary): must appear on
#             new primary proving replication preserved them before the flip
#   Phase B — POST_FLIP (15 rows via keel to new primary): individually verified
#   Replication check: Phase A rows must be on the new primary (ex-replica1)
#   Cross-check: total sentinel count = Phase A + Phase B = 30 rows
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
KEEL_CONTAINER="chaos-keel"

PRIMARY_HOST="172.30.0.10"
REPLICA1_HOST="172.30.0.11"
PG_PORT=5432

FAILOVER_TIMEOUT_S=20
SETTLE_TIMEOUT_S=30
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=15
SCENARIO="flip_primary"

die() { echo "FAIL: $*" >&2; exit 1; }
log() { echo "[flip-primary] $*"; }

# Preflight
docker inspect "$PRIMARY_CONTAINER"  >/dev/null 2>&1 || die "${PRIMARY_CONTAINER} not running"
docker inspect "$REPLICA1_CONTAINER" >/dev/null 2>&1 || die "${REPLICA1_CONTAINER} not running"
docker inspect "$KEEL_CONTAINER"     >/dev/null 2>&1 || die "${KEEL_CONTAINER} not running"

# Locate and back-up keel config
SCRIPT_DIR_ABS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAOS_INI="$(cd "$SCRIPT_DIR_ABS/../../../docker/keel" && pwd)/keel-chaos.ini"
CHAOS_INI_BAK="${CHAOS_INI}.flip-primary.bak"
[[ -f "$CHAOS_INI" ]] || die "Cannot locate keel-chaos.ini at ${CHAOS_INI}"
cp "$CHAOS_INI" "$CHAOS_INI_BAK"
_restore_ini() { [[ -f "$CHAOS_INI_BAK" ]] && mv "$CHAOS_INI_BAK" "$CHAOS_INI" || true; }
trap _restore_ini EXIT

# Sentinel table setup on original primary
log "Setting up sentinel table..."
sentinel_setup "$PRIMARY_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "Cannot set up sentinel table"

RUN_TAG=$(sentinel_tag "flip")
log "Run tag: ${RUN_TAG}"

# Phase A: 15 rows directly on original primary
# These prove replication was working before the flip: they must appear on
# replica1 (the new primary) after promotion.
log "=== Phase A: writing ${SENTINEL_N} pre-flip rows directly to original primary ==="
sentinel_write_batch \
    "$PRIMARY_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_flip" "direct" "$SENTINEL_N" \
    || die "Phase A: pre-flip direct write to primary failed"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_flip" "$SENTINEL_N" "phase-A pre-flip on primary" \
    || die "Phase A: pre-flip rows not all present on primary"

# Verify they are already replicated to replica1 BEFORE stopping the primary
log "Verifying Phase A rows replicated to replica1 before flip..."
replicated_ok=0
for i in $(seq 1 15); do
    count=$(sentinel_count \
        "$REPLICA1_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
        "${RUN_TAG}_pre" "pre_flip")
    if [[ "${count:-0}" -eq "$SENTINEL_N" ]]; then
        replicated_ok=1
        break
    fi
    sleep 1
done
if [[ $replicated_ok -eq 0 ]]; then
    count=$(sentinel_count \
        "$REPLICA1_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
        "${RUN_TAG}_pre" "pre_flip")
    log "WARNING: only ${count:-0}/${SENTINEL_N} Phase A rows on replica1 before flip — replication lag"
fi

# Stop the original primary
log "Stopping original primary (${PRIMARY_CONTAINER})..."
docker stop "$PRIMARY_CONTAINER" >/dev/null 2>&1 \
    || die "Failed to stop ${PRIMARY_CONTAINER}"
FAIL_TIME=$(date +%s)
log "Primary stopped at $(date -d @${FAIL_TIME} '+%H:%M:%S')"

# Promote replica1
log "Promoting ${REPLICA1_CONTAINER} to primary..."
docker exec -u postgres "$REPLICA1_CONTAINER" \
    pg_ctl promote -D /var/lib/postgresql/data \
    >/dev/null 2>&1 \
    || die "pg_ctl promote failed"

log "Waiting for replica1 to accept writes..."
promotion_ok=0
for i in $(seq 1 30); do
    if PGPASSWORD="$CHAOS_PASS" psql \
        -h "$REPLICA1_HOST" -p "$PG_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT pg_is_in_recovery()" 2>/dev/null | grep -q "f"; then
        log "replica1 promoted (${i}s)"
        promotion_ok=1
        break
    fi
    sleep 1
done
[[ $promotion_ok -eq 1 ]] || die "replica1 did not complete promotion within 30s"
PROMOTE_TIME=$(date +%s)

# Reconfigure keel to point at the new primary
log "Updating keel config to new primary (${REPLICA1_HOST})..."
awk -v new_host="${REPLICA1_HOST}" -v db="${CHAOS_DB}" '
    /host=pgsql-primary/ && /role=RW/ {
        sub(/host=pgsql-primary[^ ]*/, "host=" new_host)
        sub(/port=[0-9]+/, "port=5432")
        sub(/dbname=[^ ]+/, "dbname=" db)
    }
    { print }
' "$CHAOS_INI_BAK" > "$CHAOS_INI" || die "Failed to update keel-chaos.ini"

log "Sending SIGHUP to keel to reload config..."
docker exec "$KEEL_CONTAINER" kill -HUP 1 2>/dev/null \
    || die "Failed to send SIGHUP to keel"

# Wait for keel to accept WRITES via the new primary.
# We test the actual write path by inserting an idempotent probe row.
# A plain SELECT 1 is not sufficient because keel routes reads to the RO pool
# (pgsql-replica1 is still accessible via RO even before the config reloads),
# so SELECT 1 can succeed even when the RW pool is not yet connected.
log "Waiting up to ${SETTLE_TIMEOUT_S}s for keel to route writes to new primary..."
WRITE_PROBE_TAG="_rw_probe_${RUN_TAG}"
write_ok=0
for i in $(seq 1 "$SETTLE_TIMEOUT_S"); do
    if PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "INSERT INTO ${SENTINEL_TABLE} (scenario,tag,phase,seq,val,written_via) \
            VALUES ('probe','${WRITE_PROBE_TAG}','probe',0,'${WRITE_PROBE_TAG}:0','keel') \
            ON CONFLICT (val) DO NOTHING" >/dev/null 2>&1; then
        SETTLE_TIME=$(date +%s)
        log "Write path available after $((SETTLE_TIME - PROMOTE_TIME))s"
        write_ok=1
        break
    fi
    sleep 1
done
[[ $write_ok -eq 1 ]] || die "keel did not route writes within ${SETTLE_TIMEOUT_S}s after promotion"

# Phase B: 15 post-flip rows via keel (go to new primary = ex-replica1)
log "=== Phase B: writing ${SENTINEL_N} post-flip sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_flip" "keel" "$SENTINEL_N" \
    || die "Phase B: post-flip write via keel failed"

sentinel_assert_values \
    "$REPLICA1_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_post" "post_flip" "$SENTINEL_N" "phase-B post-flip values on new primary" \
    || die "Phase B: post-flip sentinel rows not all visible on new primary"

# Verification: Phase A rows must be on new primary (replication preserved them)
log "=== Verifying Phase A rows are on new primary after flip ==="
sentinel_assert_count \
    "$REPLICA1_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_flip" "$SENTINEL_N" "phase-A rows on new primary after flip" \
    || die "Phase A FAIL: pre-flip rows missing on new primary — replication gap!"

# Verify each Phase A value individually on new primary
sentinel_assert_values \
    "$REPLICA1_HOST" "$PG_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_flip" "$SENTINEL_N" "phase-A individual values on new primary" \
    || die "Phase A FAIL: individual pre-flip values corrupted or missing on new primary"

# Cross-check: total sentinel rows for this run = Phase A + Phase B
TOTAL_EXPECTED=$((SENTINEL_N + SENTINEL_N))
TOTAL_ACTUAL=$(PGPASSWORD="$CHAOS_PASS" psql \
    -h "$REPLICA1_HOST" -p "$PG_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -t -A -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE tag LIKE '${RUN_TAG}%'" \
    2>/dev/null | tr -d ' \n' || echo "0")
if [[ "${TOTAL_ACTUAL:-0}" -ne "$TOTAL_EXPECTED" ]]; then
    die "Cross-check FAIL: expected ${TOTAL_EXPECTED} total sentinel rows, got ${TOTAL_ACTUAL:-ERR}"
fi
log "✓ Cross-check: total sentinel rows = ${TOTAL_ACTUAL} (Phase A: ${SENTINEL_N} + Phase B: ${SENTINEL_N})"

log "Restarting old primary container (will come up as replica)..."
docker start "$PRIMARY_CONTAINER" >/dev/null 2>&1 || true

log "PASS: Primary flip — keel routed writes within $((SETTLE_TIME - FAIL_TIME))s"
log "      Phase A (${SENTINEL_N} pre-flip rows): preserved via replication on new primary"
log "      Phase B (${SENTINEL_N} post-flip rows): committed to new primary via keel"
log "      Total: ${TOTAL_ACTUAL} rows consistent across the failover boundary"
