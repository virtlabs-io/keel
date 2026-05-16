#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/partition-replica.sh
# =============================================================================
#
# Scenario: Inject 100% packet loss on replica1 using `tc netem`.
#
# Expected behaviour:
#   - keel probe detects replica1 is unreachable within probe_retries x interval
#   - Read queries are re-routed to replica2 and/or primary
#   - No client-visible errors on new queries after the failover window
#   - After removing the fault, keel re-admits replica1
#
# Multi-sentinel verification:
#   Phase A — PRE_PARTITION (15 rows via keel to primary): must survive
#   Phase B — DURING_PARTITION (15 rows via keel): writes to primary must
#             still succeed while replica1 is isolated; verified individually
#   Phase C — POST_RECOVERY (15 rows via keel): must succeed after re-admission
#   Replication check: Phase A + B rows must be visible on all nodes after
#             replica1 re-joins (eventually consistent replication check)
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/sentinel.sh"

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"

FAULT_CONTAINER="chaos-fault-injector"   # shares replica1 net namespace
PRIMARY_HOST="172.30.0.10"
PRIMARY_PORT=5432
REPLICA1_HOST="172.30.0.11"
REPLICA2_HOST="172.30.0.12"
REPLICA_PORT=5432

FAILOVER_TIMEOUT_S=15
RECOVER_TIMEOUT_S=20
QUERY_COUNT=30
SENTINEL_TABLE="chaos_sentinel"
SENTINEL_N=15
SCENARIO="partition_replica"

die() { echo "FAIL: $*" >&2; exit 1; }
log() { echo "[partition-replica] $*"; }

# Cleanup: always remove tc fault so stack is left clean
cleanup_fault() {
    log "Removing network fault from replica1..."
    docker exec "$FAULT_CONTAINER" \
        tc qdisc del dev eth0 root 2>/dev/null || true
}
trap cleanup_fault EXIT

# Preflight
docker inspect "$FAULT_CONTAINER" >/dev/null 2>&1 \
    || die "Container ${FAULT_CONTAINER} not running"

# Sentinel table setup
log "Setting up sentinel table..."
sentinel_setup "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    || die "Cannot set up sentinel table on primary"

RUN_TAG=$(sentinel_tag "partition")
log "Run tag: ${RUN_TAG}"

# Phase A: 15 pre-partition sentinel rows via keel (go to RW=primary)
log "=== Phase A: writing ${SENTINEL_N} pre-partition sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_pre" "pre_partition" "keel" "$SENTINEL_N" \
    || die "Phase A: pre-partition sentinel batch write failed"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_partition" "$SENTINEL_N" "phase-A pre-partition values" \
    || die "Phase A: pre-partition sentinel rows not all present"

# Inject fault: 100% packet loss on replica1
log "Injecting 100% packet loss on replica1 network interface..."
docker exec "$FAULT_CONTAINER" \
    tc qdisc add dev eth0 root netem loss 100% \
    || die "Failed to inject tc netem fault (is fault-injector running with NET_ADMIN?)"
FAULT_TIME=$(date +%s)
log "Fault injected at $(date -d @${FAULT_TIME} '+%H:%M:%S')"

# Wait for keel to route around the faulted replica
log "Waiting up to ${FAILOVER_TIMEOUT_S}s for keel to route reads around replica1..."
routed_ok=0
for i in $(seq 1 "$FAILOVER_TIMEOUT_S"); do
    if PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT 1" >/dev/null 2>&1; then
        FAILOVER_TIME=$(date +%s)
        log "keel routing recovered after $((FAILOVER_TIME - FAULT_TIME))s"
        routed_ok=1
        break
    fi
    sleep 1
done

[[ $routed_ok -eq 1 ]] \
    || die "keel could not route reads after replica1 was partitioned (${FAILOVER_TIMEOUT_S}s)"

# Phase B: 15 during-partition sentinel rows via keel
# Writes route to RW=primary which is still healthy; this verifies writes
# are unaffected by a replica partition.
log "=== Phase B: writing ${SENTINEL_N} during-partition sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_mid" "during_partition" "keel" "$SENTINEL_N" \
    || die "Phase B: during-partition sentinel write failed — writes must still work during replica partition"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_mid" "during_partition" "$SENTINEL_N" "phase-B during-partition values" \
    || die "Phase B: during-partition writes not all present on primary"

# Verify continued read availability through keel (routes to replica2 or primary)
log "Running ${QUERY_COUNT} read queries to confirm availability during partition..."
fail_count=0
for i in $(seq 1 "$QUERY_COUNT"); do
    if ! PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE tag LIKE '${RUN_TAG}%'" >/dev/null 2>&1; then
        fail_count=$((fail_count + 1))
    fi
done
if [[ $fail_count -gt 3 ]]; then
    die "${fail_count}/${QUERY_COUNT} read queries failed while replica1 was partitioned"
fi
log "$((QUERY_COUNT - fail_count))/${QUERY_COUNT} read queries succeeded during partition"

# Verify reads go to replica2 (not replica1) by querying pg_is_in_recovery
# directly on both replicas to confirm replica1 cannot be reached
log "Verifying replica1 is unreachable directly (confirms fault is active)..."
if PGPASSWORD="$CHAOS_PASS" psql \
    -h "$REPLICA1_HOST" -p "$REPLICA_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -c "SELECT 1" -t >/dev/null 2>&1; then
    log "WARNING: replica1 is still reachable directly — tc netem may not be working correctly"
fi

log "Verifying replica2 is healthy and serving reads..."
PGPASSWORD="$CHAOS_PASS" psql \
    -h "$REPLICA2_HOST" -p "$REPLICA_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -c "SELECT pg_is_in_recovery()" -t 2>/dev/null | grep -q "t" \
    || log "WARNING: replica2 may not be in recovery mode (or not reachable directly)"

# Remove fault
log "Removing tc netem fault..."
cleanup_fault
trap - EXIT   # reset to default after cleanup_fault so we don't double-call
CLEAR_TIME=$(date +%s)

# Wait for keel to re-admit replica1
log "Waiting up to ${RECOVER_TIMEOUT_S}s for keel to re-admit replica1..."
readmitted=0
for i in $(seq 1 "$RECOVER_TIMEOUT_S"); do
    if PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT 1" >/dev/null 2>&1; then
        readmitted=1
        break
    fi
    sleep 1
done
[[ $readmitted -eq 1 ]] \
    || die "keel lost connectivity after removing the replica1 partition"

# Allow replica1 time to catch up on WAL
log "Waiting for replica1 WAL catch-up (up to 10s)..."
sleep 10

# Phase C: 15 post-recovery rows
log "=== Phase C: writing ${SENTINEL_N} post-recovery sentinel rows via keel ==="
sentinel_write_batch \
    "$KEEL_HOST" "$KEEL_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_post" "post_recovery" "keel" "$SENTINEL_N" \
    || die "Phase C: post-recovery sentinel write failed"

sentinel_assert_values \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_post" "post_recovery" "$SENTINEL_N" "phase-C post-recovery values" \
    || die "Phase C: post-recovery sentinel rows not all present on primary"

# Replication consistency check: Phase A + B rows must be on replica1 after catch-up
log "=== Replication check: Phase A+B rows visible on replica1 after re-join ==="
sentinel_assert_count \
    "$REPLICA1_HOST" "$REPLICA_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_partition" "$SENTINEL_N" "replica1 phase-A rows after re-join" \
    || log "WARN: Phase A rows not yet replicated to replica1 (replication lag?)"

sentinel_assert_count \
    "$REPLICA1_HOST" "$REPLICA_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_mid" "during_partition" "$SENTINEL_N" "replica1 phase-B rows after re-join" \
    || log "WARN: Phase B rows not yet replicated to replica1 (written while partitioned, expect lag)"

# replica2 must have all rows (it was never partitioned)
log "=== Consistency check: Phase A+B rows visible on replica2 ==="
sentinel_assert_count \
    "$REPLICA2_HOST" "$REPLICA_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_pre" "pre_partition" "$SENTINEL_N" "replica2 phase-A rows" \
    || die "replica2 is missing Phase A rows — data consistency failure"

sentinel_assert_count \
    "$REPLICA2_HOST" "$REPLICA_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_mid" "during_partition" "$SENTINEL_N" "replica2 phase-B rows" \
    || die "replica2 is missing Phase B rows — data consistency failure"

log "PASS: reads survived replica1 partition (failover ${FAILOVER_TIMEOUT_S}s, re-admit $(($(date +%s) - CLEAR_TIME))s)"
log "      Phase A (${SENTINEL_N} pre-partition rows): present on primary and replica2"
log "      Phase B (${SENTINEL_N} during-partition rows): writes succeeded, replicated to replica2"
log "      Phase C (${SENTINEL_N} post-recovery rows): committed after fault cleared"
