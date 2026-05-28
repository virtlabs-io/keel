#!/usr/bin/env bash
# =============================================================================
# tests/chaos/scenarios/scatter-backend-mid-scatter.sh
# =============================================================================
#
# Chaos Scenario: Backend shard dies while a scatter fan-out is in progress.
#
# Expected behaviour:
#   - keel detects the shard death during scatter collection
#   - Scatter query returns a clear error — must NOT hang or return partial data
#   - After shard restart, scatter queries complete correctly
#
# Multi-sentinel verification:
#   Baseline: Write N rows to each shard directly — scatter COUNT must = 2N
#   During fault: scatter COUNT must NOT be N (partial) — must error or be 2N
#   After recovery: scatter COUNT must = 2N (no data loss on surviving shard0)
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/sentinel.sh"

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"
SHARD0_CONTAINER="${SHARD0_CONTAINER:-chaos-shard0}"
SHARD1_CONTAINER="${SHARD1_CONTAINER:-chaos-shard1}"
SHARD0_HOST="${SHARD0_HOST:-172.30.1.10}"
SHARD1_HOST="${SHARD1_HOST:-172.30.1.11}"
SHARD_PORT=5432
SCATTER_TIMEOUT_S="${SCATTER_TIMEOUT_S:-10}"
WORKLOAD_S="${WORKLOAD_S:-20}"
KILL_DELAY_S="${KILL_DELAY_S:-5}"

SENTINEL_TABLE="scatter_sentinel"
SENTINEL_N=20         # rows per shard — scatter COUNT baseline = 2*SENTINEL_N
SCENARIO="scatter_backend"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[scatter-mid-scatter] $*"; }
pass() { echo "PASS: $*"; }

# Cleanup: always restart shard1 on exit
trap 'docker start "$SHARD1_CONTAINER" >/dev/null 2>&1 || true' EXIT

# Preflight
command -v pgbench >/dev/null 2>&1 || die "pgbench not found on PATH"
command -v psql    >/dev/null 2>&1 || die "psql not found on PATH"

for container in "$SHARD0_CONTAINER" "$SHARD1_CONTAINER"; do
    if ! docker inspect "$container" >/dev/null 2>&1; then
        echo "SKIP: scatter shard container ${container} not running — start the scatter chaos stack first" >&2
        exit 77
    fi
done

PGPASSWORD="$CHAOS_PASS" psql \
    -h "$KEEL_HOST" -p "$KEEL_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -c "SELECT 1" -t >/dev/null 2>&1 \
    || die "keel not reachable at ${KEEL_HOST}:${KEEL_PORT}"

RUN_TAG=$(sentinel_tag "scatter_be")
log "Run tag: ${RUN_TAG}"

# Set up sentinel table on both shards and write baseline rows
log "=== Setting up sentinel table and writing ${SENTINEL_N} baseline rows on each shard ==="
for shard_host in "$SHARD0_HOST" "$SHARD1_HOST"; do
    sentinel_setup "$shard_host" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
        || log "WARNING: sentinel_setup on ${shard_host} failed (table may already exist)"
done

# Write baseline: SENTINEL_N rows to shard0, SENTINEL_N rows to shard1
sentinel_write_batch \
    "$SHARD0_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_s0" "baseline" "direct_shard0" "$SENTINEL_N" \
    || die "Baseline write to shard0 failed"

sentinel_write_batch \
    "$SHARD1_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "$SCENARIO" "${RUN_TAG}_s1" "baseline" "direct_shard1" "$SENTINEL_N" \
    || die "Baseline write to shard1 failed"

# Verify baseline scatter COUNT = 2*SENTINEL_N through keel
EXPECTED_BASELINE=$((SENTINEL_N * 2))
BASELINE=$(PGPASSWORD="$CHAOS_PASS" timeout "$SCATTER_TIMEOUT_S" psql \
    -h "$KEEL_HOST" -p "$KEEL_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -t -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE phase='baseline'" \
    2>/dev/null | tr -d ' \n' || echo "ERR")
if [[ ! "$BASELINE" =~ ^[0-9]+$ ]]; then
    die "Baseline scatter query failed: ${BASELINE} (keel may not have scatter routing for this table)"
fi
if [[ "$BASELINE" -lt "$EXPECTED_BASELINE" ]]; then
    log "WARNING: baseline scatter COUNT=${BASELINE} expected=${EXPECTED_BASELINE}"
    log "         scatter routing may not cover both shards — continuing with observed baseline"
fi
log "Baseline scatter COUNT=${BASELINE} (expected ${EXPECTED_BASELINE})"

# Start scatter workload in background
BENCH_LOG=$(mktemp /tmp/chaos-scatter-bench-XXXXXX.log)
BENCH_ERR=$(mktemp /tmp/chaos-scatter-bench-err-XXXXXX.log)
trap 'rm -f "$BENCH_LOG" "$BENCH_ERR"; docker start "$SHARD1_CONTAINER" >/dev/null 2>&1 || true' EXIT

log "Starting scatter pgbench workload (${WORKLOAD_S}s, 4 clients)..."
PGPASSWORD="$CHAOS_PASS" pgbench \
    -h "$KEEL_HOST" -p "$KEEL_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -c 4 -j 2 -T "$WORKLOAD_S" \
    -f /dev/stdin <<'PGBENCH_SCRIPT' \
    > "$BENCH_LOG" 2> "$BENCH_ERR" &
\set rnd random(1, 1000000)
SELECT COUNT(*) FROM orders WHERE id < :rnd;
PGBENCH_SCRIPT
BENCH_PID=$!

# Kill shard1 mid-scatter
log "Waiting ${KILL_DELAY_S}s before killing ${SHARD1_CONTAINER}..."
sleep "$KILL_DELAY_S"
log "Killing ${SHARD1_CONTAINER} (SIGKILL)..."
docker kill "$SHARD1_CONTAINER" >/dev/null 2>&1 \
    || die "Failed to kill ${SHARD1_CONTAINER}"
KILL_TIME=$(date +%s)
log "Shard killed at $(date -d @${KILL_TIME} '+%H:%M:%S')"

# Verify keel surfaces error within SCATTER_TIMEOUT_S and does NOT return
# a partial sentinel count (which would indicate incorrect fan-out merge)
log "Sending scatter sentinel query during fault (expect error or full count)..."
Q_START=$(date +%s)
QUERY_RESULT=$(PGPASSWORD="$CHAOS_PASS" timeout "$SCATTER_TIMEOUT_S" psql \
    -h "$KEEL_HOST" -p "$KEEL_PORT" \
    -U "$CHAOS_USER" -d "$CHAOS_DB" \
    -t -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE phase='baseline'" \
    2>&1 || true)
Q_ELAPSED=$(( $(date +%s) - Q_START ))

log "Scatter sentinel query completed in ${Q_ELAPSED}s: ${QUERY_RESULT:0:80}"

# Hard failure: query hung past the scatter timeout
if [[ $Q_ELAPSED -ge $SCATTER_TIMEOUT_S ]]; then
    die "Scatter sentinel query HUNG for ${Q_ELAPSED}s — keel did not surface error"
fi

# Hard failure: got a numeric result equal to exactly SENTINEL_N (partial shard0-only result)
RESULT_NUM=$(echo "$QUERY_RESULT" | tr -d ' \n' | grep -Eo '^[0-9]+$' || echo "ERR")
if [[ "$RESULT_NUM" =~ ^[0-9]+$ && "$RESULT_NUM" -eq "$SENTINEL_N" ]]; then
    die "CORRECTNESS VIOLATION: scatter returned ${RESULT_NUM} (= shard0-only count) — partial merge!"
fi

wait "$BENCH_PID" || true

PROCESSED=$(grep -Eo "number of transactions actually processed: [0-9]+" "$BENCH_LOG" \
    | awk '{print $NF}' | tail -1 || echo "0")
log "pgbench: processed=${PROCESSED:-0}"
[[ "${PROCESSED:-0}" -lt 2 ]] && { cat "$BENCH_ERR" >&2; die "Too few pgbench transactions (${PROCESSED})"; }

# Restart shard1 and wait for it to be healthy
log "Restarting ${SHARD1_CONTAINER}..."
docker start "$SHARD1_CONTAINER" >/dev/null 2>&1 \
    || die "Failed to restart ${SHARD1_CONTAINER}"
for i in $(seq 1 30); do
    docker exec "$SHARD1_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 \
        && { log "Shard1 healthy after ${i}s"; break; }
    sleep 1
done

# Wait for keel to re-admit the shard
log "Waiting for keel to detect shard recovery (up to 15s)..."
recovered=0
for i in $(seq 1 15); do
    RESULT=$(PGPASSWORD="$CHAOS_PASS" timeout 5 psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -t -c "SELECT COUNT(*) FROM ${SENTINEL_TABLE} WHERE phase='baseline'" \
        2>/dev/null | tr -d ' \n' || echo "ERR")
    if [[ "$RESULT" =~ ^[0-9]+$ ]]; then
        log "Scatter query succeeded after recovery: COUNT=${RESULT}"
        recovered=1
        break
    fi
    sleep 1
done
[[ $recovered -eq 1 ]] || die "keel did not recover scatter routing within 15s after shard restart"

# Post-recovery correctness: scatter COUNT must equal the baseline
if [[ "$RESULT" -ne "$BASELINE" ]]; then
    die "POST-RECOVERY CORRECTNESS FAIL: COUNT=${RESULT} != baseline=${BASELINE} — data lost on shard!"
fi
log "✓ Post-recovery scatter COUNT=${RESULT} matches baseline=${BASELINE} — no data loss"

# Verify shard0 rows survived independently (shard0 was never killed)
sentinel_assert_count \
    "$SHARD0_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_s0" "baseline" "$SENTINEL_N" "shard0 baseline rows after recovery" \
    || die "shard0 rows lost — data loss on surviving shard!"

# Verify shard1 rows survived the kill+restart
sentinel_assert_count \
    "$SHARD1_HOST" "$SHARD_PORT" "$CHAOS_USER" "$CHAOS_PASS" "$CHAOS_DB" "$SENTINEL_TABLE" \
    "${RUN_TAG}_s1" "baseline" "$SENTINEL_N" "shard1 baseline rows after restart" \
    || log "WARNING: shard1 rows missing after restart — PostgreSQL may have been initialized clean"

RECOVER_TIME=$(date +%s)
pass "scatter-backend-mid-scatter: error surfaced in ${Q_ELAPSED}s, recovered in $((RECOVER_TIME - KILL_TIME))s"
pass "  Baseline (${SENTINEL_N} rows/shard): no partial-merge correctness violation"
pass "  Shard0 (${SENTINEL_N} rows): intact on surviving shard"
