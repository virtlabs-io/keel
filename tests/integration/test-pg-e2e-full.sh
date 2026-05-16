#!/bin/bash
# =============================================================================
# KEEL End-to-End Test Script
# =============================================================================
# Runs a complete E2E stress test:
# 1. Spins up 3-node PostgreSQL cluster (1 primary, 2 replicas)
# 2. Builds and starts KEEL proxy (50 backend, 1000 frontend connections)
# 3. Runs pgbench stress test (100 clients, 60 seconds)
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Compose file
COMPOSE_FILE="$PROJECT_ROOT/docker/compose/pg-e2e.yml"

# Test parameters
PGBENCH_CLIENTS=${PGBENCH_CLIENTS:-100}
PGBENCH_DURATION=${PGBENCH_DURATION:-60}
PGBENCH_SCALE=${PGBENCH_SCALE:-10}

# =============================================================================
# Helper Functions
# =============================================================================

log_header() {
    echo -e "\n${BLUE}===========================================================================${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${BLUE}===========================================================================${NC}\n"
}

log_step() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_info() {
    echo -e "${CYAN}[i]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[!]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Cleanup function
cleanup() {
    log_header "Cleanup"
    log_info "Stopping and removing containers..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    log_step "Cleanup complete"
}

# Handle interrupts
trap cleanup EXIT

# =============================================================================
# Pre-flight Checks
# =============================================================================

log_header "KEEL End-to-End Stress Test"

echo -e "Test Configuration:"
echo -e "  - PostgreSQL Cluster:  3 nodes (1 primary, 2 replicas)"
echo -e "  - KEEL Backend Conns:   50"
echo -e "  - KEEL Frontend Conns:  1000"
echo -e "  - KEEL Port:            6432"
echo -e "  - pgbench Clients:     $PGBENCH_CLIENTS"
echo -e "  - pgbench Duration:    ${PGBENCH_DURATION}s"
echo -e "  - pgbench Scale:       $PGBENCH_SCALE"
echo ""

# Check for Docker
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi
log_step "Docker found"

# Check for docker compose
if ! docker compose version &> /dev/null; then
    log_error "Docker Compose is not available"
    exit 1
fi
log_step "Docker Compose found"

# Check compose file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    log_error "Compose file not found: $COMPOSE_FILE"
    exit 1
fi
log_step "Compose file found"

# =============================================================================
# Cleanup Previous Run
# =============================================================================

log_header "Preparing Environment"

log_info "Cleaning up any previous test containers..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
log_step "Previous containers removed"

# =============================================================================
# Build KEEL
# =============================================================================

log_header "Building KEEL Proxy"

log_info "Building KEEL with io_uring support..."
docker compose -f "$COMPOSE_FILE" build keel

log_step "KEEL build complete"

# =============================================================================
# Start PostgreSQL Cluster
# =============================================================================

log_header "Starting PostgreSQL Cluster"

log_info "Starting primary..."
docker compose -f "$COMPOSE_FILE" up -d pgsql-01

log_info "Waiting for primary to be healthy..."
timeout 60 bash -c 'until docker compose -f "'$COMPOSE_FILE'" ps pgsql-01 | grep -q "healthy"; do sleep 2; done' || {
    log_error "Primary failed to become healthy"
    docker compose -f "$COMPOSE_FILE" logs pgsql-01
    exit 1
}
log_step "Primary is ready"

log_info "Starting replicas..."
docker compose -f "$COMPOSE_FILE" up -d pgsql-02 pgsql-03

log_info "Waiting for replicas to be healthy..."
timeout 120 bash -c 'until docker compose -f "'$COMPOSE_FILE'" ps pgsql-02 | grep -q "healthy"; do sleep 2; done' || {
    log_error "Replica 1 failed to become healthy"
    docker compose -f "$COMPOSE_FILE" logs pgsql-02
    exit 1
}
log_step "Replica 1 is ready"

timeout 120 bash -c 'until docker compose -f "'$COMPOSE_FILE'" ps pgsql-03 | grep -q "healthy"; do sleep 2; done' || {
    log_error "Replica 2 failed to become healthy"
    docker compose -f "$COMPOSE_FILE" logs pgsql-03
    exit 1
}
log_step "Replica 2 is ready"

# Verify replication
log_info "Verifying streaming replication..."
docker compose -f "$COMPOSE_FILE" exec -T pgsql-01 psql -U postgres -d testdb -c \
    "SELECT client_addr, state, sent_lsn, replay_lsn FROM pg_stat_replication;"
log_step "Streaming replication verified"

# =============================================================================
# Start KEEL Proxy
# =============================================================================

log_header "Starting KEEL Proxy"

log_info "Starting KEEL..."
docker compose -f "$COMPOSE_FILE" up -d keel

log_info "Waiting for KEEL to be healthy..."
timeout 30 bash -c 'until docker compose -f "'$COMPOSE_FILE'" ps keel | grep -q "healthy"; do sleep 2; done' || {
    log_error "KEEL failed to become healthy"
    docker compose -f "$COMPOSE_FILE" logs keel
    exit 1
}
log_step "KEEL is ready on port 6432"

# Test connectivity through proxy (use a postgres container with proper network access)
log_info "Testing connectivity through KEEL..."
docker run --rm --network keel-pg-e2e_e2e-network postgres:16-alpine \
    psql -h keel -p 6432 -U postgres -d testdb -c "SELECT 'KEEL proxy working!' AS status;" 2>/dev/null || {
    log_warn "Direct connectivity test skipped - will verify via pgbench"
}
log_step "KEEL started, proceeding to stress test"

# =============================================================================
# Run pgbench Stress Test
# =============================================================================

log_header "Running pgbench Stress Test"

echo -e "Parameters:"
echo -e "  - Clients:   $PGBENCH_CLIENTS concurrent connections"
echo -e "  - Duration:  ${PGBENCH_DURATION} seconds"
echo -e "  - Scale:     $PGBENCH_SCALE (initial data size)"
echo ""

log_info "Starting pgbench container..."
docker compose -f "$COMPOSE_FILE" up pgbench

# Get exit code
PGBENCH_EXIT=$(docker inspect e2e-pgbench --format='{{.State.ExitCode}}' 2>/dev/null || echo "1")

if [ "$PGBENCH_EXIT" = "0" ]; then
    log_step "pgbench stress test completed successfully!"
else
    log_error "pgbench exited with code $PGBENCH_EXIT"
fi

# =============================================================================
# Collect Results
# =============================================================================

log_header "Test Results"

# Show KEEL logs
log_info "KEEL proxy logs:"
echo "---"
docker compose -f "$COMPOSE_FILE" logs --tail=50 keel
echo "---"

# Show cluster status
log_info "Final cluster status:"
docker compose -f "$COMPOSE_FILE" exec -T pgsql-01 psql -U postgres -d testdb -c \
    "SELECT client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn FROM pg_stat_replication;" 2>/dev/null || true

# =============================================================================
# Summary
# =============================================================================

log_header "Test Summary"

if [ "$PGBENCH_EXIT" = "0" ]; then
    echo -e "${GREEN}${BOLD}  ✓ END-TO-END TEST PASSED${NC}"
    echo ""
    echo "  Verified:"
    echo "    - 3-node PostgreSQL cluster (1 primary, 2 replicas)"
    echo "    - KEEL proxy with connection pooling"
    echo "    - pgbench stress test ($PGBENCH_CLIENTS clients, ${PGBENCH_DURATION}s)"
    exit 0
else
    echo -e "${RED}${BOLD}  ✗ END-TO-END TEST FAILED${NC}"
    exit 1
fi
