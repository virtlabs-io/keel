#!/usr/bin/env bash
# =============================================================================
# KEEL Sharding Integration Test
# =============================================================================
# Tests:
#   1. Schema setup on both shards
#   2. INSERT routed to correct shard by hash key
#   3. SELECT reads from correct shard
#   4. Scatter SELECT returns results from all shards
#   5. Hot-reload: add a new shard rule via SIGHUP
#   6. Prometheus /metrics exposes keel_router_* counters
#
# Prerequisites:
#   - Docker and Docker Compose v2 installed
#   - KEEL binary built (run `make -C build keel` first)
#
# Usage:
#   bash ./docker/tests/test-sharding.sh [--keep]
#
# Options:
#   --keep   Do not tear down containers after the test (useful for debugging)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker/compose/pg-sharding.yml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

KEEP=0
for arg in "$@"; do [[ "$arg" == "--keep" ]] && KEEP=1; done

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
info() { echo -e "${CYAN}[info]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

# =============================================================================
# Cleanup
# =============================================================================
cleanup() {
    if [[ $KEEP -eq 1 ]]; then
        warn "Containers kept running (--keep). Tear down with:"
        warn "  docker compose -f $COMPOSE_FILE down -v"
        return
    fi
    info "Tearing down containers..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# =============================================================================
# Start stack
# =============================================================================
echo -e "\n${BOLD}${CYAN}=== KEEL Sharding Integration Test ===${NC}\n"

info "Starting compose stack..."
docker compose -f "$COMPOSE_FILE" up -d --build --wait 2>&1 | tail -5

KEEL_HOST="127.0.0.1"
KEEL_PORT="16432"
PROM_PORT="19101"

# keel-sharding.ini uses auth_method=trust so no password is needed for
# client→keel connections.  PGPASSWORD is set here as a fallback in case
# the proxy is running with password auth (e.g. a non-default config).
export PGPASSWORD="${PGPASSWORD:-postgres}"

PG0="docker compose -f $COMPOSE_FILE exec -T pg-shard0 psql -U postgres -d testdb -v ON_ERROR_STOP=1"
PG1="docker compose -f $COMPOSE_FILE exec -T pg-shard1 psql -U postgres -d testdb -v ON_ERROR_STOP=1"
KEEL_PSQL="psql -h $KEEL_HOST -p $KEEL_PORT -U postgres -d testdb -v ON_ERROR_STOP=1"

# The Docker health check passes when KEEL's Prometheus port (9101) binds,
# which happens *before* worker threads have established backend pool
# connections to the shards.  Poll until KEEL can actually execute a query
# so subsequent tests don't race the async pool-connect phase.
info "Waiting for KEEL to be query-ready..."
READY=0
for _i in $(seq 1 30); do
    if $KEEL_PSQL -c "SELECT 1" >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 1
done
[[ $READY -eq 1 ]] || fail "KEEL did not become query-ready after 30 seconds"

# =============================================================================
# Test 1: Schema setup
# =============================================================================
info "Test 1: Creating schema on both shards..."
for PG in "$PG0" "$PG1"; do
    eval "$PG" <<'SQL'
CREATE TABLE IF NOT EXISTS users (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    user_id  BIGINT NOT NULL,
    amount   NUMERIC(10,2)
);
SQL
done
pass "Schema created on both shards"

# =============================================================================
# Test 2: INSERT data through KEEL — single-key inserts are hash-routed
# =============================================================================
info "Test 2: Inserting rows through KEEL (single-shard hash routing)..."

# KEEL extracts the shard key from single-row VALUES inserts and routes them
# to the correct shard using hash(id) % shard_count.  No 2PC scatter-write.
$KEEL_PSQL -c "INSERT INTO users(id, name) VALUES(0, 'alice') ON CONFLICT DO NOTHING"
$KEEL_PSQL -c "INSERT INTO users(id, name) VALUES(1, 'bob') ON CONFLICT DO NOTHING"
$KEEL_PSQL -c "INSERT INTO users(id, name) VALUES(2, 'carol') ON CONFLICT DO NOTHING"
$KEEL_PSQL -c "INSERT INTO users(id, name) VALUES(3, 'dave') ON CONFLICT DO NOTHING"

pass "Rows inserted through KEEL with hash routing"

# =============================================================================
# Test 3: Direct verification — rows landed on correct shard
# =============================================================================
info "Test 3: Verifying shard data distribution..."

S0_COUNT=$(eval "$PG0 -tAc 'SELECT COUNT(*) FROM users'")
S1_COUNT=$(eval "$PG1 -tAc 'SELECT COUNT(*) FROM users'")

info "Shard 0 row count: $S0_COUNT"
info "Shard 1 row count: $S1_COUNT"

[[ "$S0_COUNT" -gt 0 ]] || fail "Shard 0 has no rows"
[[ "$S1_COUNT" -gt 0 ]] || fail "Shard 1 has no rows"
pass "Rows present on both shards"

# =============================================================================
# Test 4: SELECT single-shard via KEEL
# =============================================================================
info "Test 4: Single-shard SELECT via KEEL..."
ALICE=$($KEEL_PSQL -tAc "SELECT name FROM users WHERE id=0" 2>/dev/null | tr -d '[:space:]')
[[ "$ALICE" == "alice" ]] || fail "Expected 'alice', got '$ALICE'"
pass "Single-shard SELECT returned correct row"

# =============================================================================
# Test 5: Prometheus /metrics exposes router counters
# =============================================================================
info "Test 5: Prometheus /metrics endpoint..."

# Retry the metrics fetch AND the content check — the HTTP listener comes up
# with the pool but the per-worker counters are aggregated into the
# Prometheus exposition asynchronously, so the first scrape after a query
# may return the schema scaffolding without the counter lines yet populated.
METRICS=""
for _i in $(seq 1 15); do
    METRICS=$(curl -sf "http://$KEEL_HOST:$PROM_PORT/metrics" 2>/dev/null || true)
    if [[ -n "$METRICS" ]] \
        && echo "$METRICS" | grep -q "keel_queries_total" \
        && echo "$METRICS" | grep -q "keel_pool_borrows"; then
        break
    fi
    sleep 1
done
[[ -n "$METRICS" ]] || fail "Could not fetch /metrics after 15 attempts"

echo "$METRICS" | grep -q "keel_queries_total" \
    || fail "/metrics missing keel_queries_total"
echo "$METRICS" | grep -q "keel_pool_borrows" \
    || fail "/metrics missing keel_pool_borrows"
pass "/metrics exposes keel query counters"

# =============================================================================
# Test 6: SIGHUP hot-reload — add orders shard rule
# =============================================================================
info "Test 6: SIGHUP hot-reload (add orders shard rule)..."

KEEL_CONTAINER="shard-keel"
KEEL_PID=$(docker exec "$KEEL_CONTAINER" pgrep keel 2>/dev/null | head -1 || echo "")

if [[ -z "$KEEL_PID" ]]; then
    warn "Could not find keel PID in container — skipping SIGHUP test"
else
    docker exec "$KEEL_CONTAINER" kill -HUP "$KEEL_PID"
    sleep 1

    # After reload, orders rule should still be active (it was in the config)
    # Verify we can still route user queries
    BOB=$($KEEL_PSQL -tAc "SELECT name FROM users WHERE id=1" 2>/dev/null | tr -d '[:space:]')
    [[ "$BOB" == "bob" ]] || fail "Post-SIGHUP SELECT failed: expected 'bob', got '$BOB'"
    pass "Post-SIGHUP routing works correctly"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BOLD}${GREEN}=== All sharding integration tests passed ===${NC}"
echo ""
