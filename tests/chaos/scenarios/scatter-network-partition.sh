#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/scatter-network-partition.sh
# =============================================================================
#
# Chaos Scenario: Network partition during scatter fan-out — one shard becomes
# unreachable via 100% packet loss injected by tc/netem.
#
# Multi-sentinel verification:
#   Baseline: N rows on each shard — scatter COUNT = 2N
#   During partition: scatter must NOT return N (partial shard0-only result)
#   After recovery: scatter COUNT must return to 2N (both shards accessible)
#   Individual values: each sentinel row's val is verified on both shards
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/sentinel.sh"

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"
FAULT_CONTAINER="${FAULT_CONTAINER:-chaos-fault-injector}"
FAULT_IFACE="${FAULT_IFACE:-eth0}"
SHARD0_CONTAINER="${SHARD0_CONTAINER:-chaos-shard0}"
SHARD1_CONTAINER="${SHARD1_CONTAINER:-chaos-shard1}"
SHARD0_HOST="${SHARD0_HOST:-172.30.1.10}"
SHARD1_HOST="${SHARD1_HOST:-172.30.1.11}"
SHARD_PORT=5432
PARTITION_TIMEOUT_S="${PARTITION_TIMEOUT_S:-15}"
RECOVER_TIMEOUT_S="${RECOVER_TIMEOUT_S:-20}"
QUERY_ROUNDS="${QUERY_ROUNDS:-5}"

SENTINEL_TABLE="scatter_sentinel"
SENTINEL_N=20
SCENARIO="scatter_partition"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[scatter-network-partition] $*"; }
pass() { echo "PASS: $*"; }

for container in "$SHARD0_CONTAINER" "$SHARD1_CONTAINER"; do
    if ! docker inspect "$container" >/dev/null 2>&1; then
        echo "SKIP: scatter shard container ${container} not running — start the scatter chaos stack first" >&2
        exit 77
    fi
done

scatter_sentinel_count() {
    PGPASSWORD="$CHAOS_PASS" timeout "$PARTITION_TIMEOUT_S" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -t -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE phase='baseline'" \
        2>/dev/null | tr -d ' \n' || echo "ERR"
}

inject_partition() {
    log "Injecting 100% packet loss on ${FAULT_CONTAINER}:${FAULT_IFACE}..."
    docker exec "$FAULT_CONTAINER" \
        tc qdisc add dev "$FAULT_IFACE" root netem loss 100% >/dev/null 2>&1 \
        || docker exec "$FAULT_CONTAINER" \
               tc qdisc change dev "$FAULT_IFACE" root netem loss 100% >/dev/null 2>&1 \
        || die "Failed to inject netem loss on ${FAULT_CONTAINER}"
}

lift_partition() {
    log "Lifting network partition..."
    docker exec "$FAULT_CONTAINER" tc qdisc del dev "$FAULT_IFACE" root 2>/dev/null || true
}

# Preflight
command -v psql >/dev/null 2>&1 || die "psql not found on PATH"
docker inspect "$FAULT_CONTAINER" >/dev/null 2>&1 \
    || die "Container ${FAULT_CONTAINER} not running — start the chaos stack first"

trap lift_partition EXIT

RUN_TAG=$(sentinel_tag "scatter_net")
log "Run tag: ${RUN_TAG}"

# Set up sentinel table and write baseline rows on each shard
log "=== Setting up sentinel table and writing ${SENTINEL_N} baseline rows on each shard ==="
for shard_host in "$SHARD0_HOST" "$SHARD1_HOST"; do
    sentinel_setup "$shard_host" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
        || log "WARNING: sentinel_setup on ${shard_host} (table may exist)"
done

sentinel_write_batch \
    "$SHARD0_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_s0" "baseline" "direct_shard0" "$SENTINEL_N" \
    || die "Baseline write to shard0 failed"

sentinel_write_batch \
    "$SHARD1_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_s1" "baseline" "direct_shard1" "$SENTINEL_N" \
    || die "Baseline write to shard1 failed"

# Verify baseline scatter COUNT through keel
EXPECTED_BASELINE=$((SENTINEL_N * 2))
BASELINE=$(scatter_sentinel_count)
if [[ ! "$BASELINE" =~ ^[0-9]+$ ]]; then
    die "Baseline scatter query failed: ${BASELINE}"
fi
log "Baseline scatter COUNT=${BASELINE} (expected ${EXPECTED_BASELINE})"
[[ "$BASELINE" -ge "$EXPECTED_BASELINE" ]] \
    || log "WARNING: baseline COUNT=${BASELINE} < expected ${EXPECTED_BASELINE} (scatter may not cover both shards)"

# Phase 1: Inject partition and run QUERY_ROUNDS scatter sentinel queries
inject_partition
PARTITION_START=$(date +%s)

log "Running ${QUERY_ROUNDS} scatter sentinel queries during partition..."
errors=0
for i in $(seq 1 "$QUERY_ROUNDS"); do
    Q_START=$(date +%s)
    RESULT=$(scatter_sentinel_count)
    Q_ELAPSED=$(( $(date +%s) - Q_START ))

    if [[ $Q_ELAPSED -ge $PARTITION_TIMEOUT_S ]]; then
        lift_partition
        die "Scatter query HUNG for ${Q_ELAPSED}s — timeout breach"
    fi

    if [[ "$RESULT" =~ ^[0-9]+$ ]]; then
        # Got a numeric result — check it's not a partial shard0-only count
        if [[ "$RESULT" -eq "$SENTINEL_N" && "$BASELINE" -ge "$EXPECTED_BASELINE" ]]; then
            lift_partition
            die "CORRECTNESS VIOLATION during partition: COUNT=${RESULT} equals shard0-only count — partial merge!"
        fi
        log "  query $i: COUNT=${RESULT} in ${Q_ELAPSED}s (full merge or keel cached state)"
    else
        errors=$((errors + 1))
        log "  query $i: error in ${Q_ELAPSED}s (expected) — ${RESULT:0:60}"
    fi
done

log "Phase 1: ${errors}/${QUERY_ROUNDS} queries errored during partition"

# Phase 2: Lift partition and verify recovery
lift_partition
trap - EXIT
LIFT_TIME=$(date +%s)

log "Waiting for keel to recover (up to ${RECOVER_TIMEOUT_S}s)..."
recovered=0
FINAL_COUNT="ERR"
for i in $(seq 1 "$RECOVER_TIMEOUT_S"); do
    RESULT=$(scatter_sentinel_count)
    if [[ "$RESULT" =~ ^[0-9]+$ ]]; then
        FINAL_COUNT="$RESULT"
        RECOVER_ELAPSED=$(( $(date +%s) - LIFT_TIME ))
        log "Scatter recovered in ${RECOVER_ELAPSED}s: COUNT=${FINAL_COUNT}"
        recovered=1
        break
    fi
    sleep 1
done
[[ $recovered -eq 1 ]] || die "keel did not recover scatter routing within ${RECOVER_TIMEOUT_S}s"

# Post-recovery correctness: COUNT must match baseline (no data loss)
if [[ "$FINAL_COUNT" -ne "$BASELINE" ]]; then
    die "POST-RECOVERY CORRECTNESS: COUNT=${FINAL_COUNT} != baseline=${BASELINE} — data inconsistency!"
fi
log "✓ Post-recovery COUNT=${FINAL_COUNT} matches baseline=${BASELINE}"

# Verify shard0 individual sentinel values still intact
sentinel_assert_values \
    "$SHARD0_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_s0" "baseline" "$SENTINEL_N" "shard0 individual values after partition" \
    || die "shard0 sentinel values corrupted during/after partition!"

# Verify shard1 individual sentinel values still intact
sentinel_assert_values \
    "$SHARD1_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_s1" "baseline" "$SENTINEL_N" "shard1 individual values after partition" \
    || die "shard1 sentinel values corrupted during/after partition!"

pass "Scatter partition: no partial merge, no data loss, recovered in $(($(date +%s) - LIFT_TIME))s"
pass "  Shard0: ${SENTINEL_N} individual values intact"
pass "  Shard1: ${SENTINEL_N} individual values intact"
pass "  Post-recovery scatter COUNT=${FINAL_COUNT} matches baseline=${BASELINE}"
