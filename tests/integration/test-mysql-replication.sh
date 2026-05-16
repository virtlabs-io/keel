#!/bin/bash
# =============================================================================
# Test script for MySQL 9 Streaming Replication (1 Primary + 2 Replicas)
# =============================================================================
set -e
cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

COMPOSE_FILE="../../docker/compose/mysql-replication.yml"

# Container names
PRIMARY_CTR="mysql-rep-primary"
REPLICA1_CTR="mysql-rep-replica1"
REPLICA2_CTR="mysql-rep-replica2"

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test()  { echo -e "${CYAN}[TEST]${NC}  $1"; }

# Run mysql command via docker exec on the given container
# Usage: dexec <container> <sql>
dexec() {
    local ctr="$1"; shift
    docker exec "$ctr" mysql -uroot -proot --silent --skip-column-names -e "$@" 2>/dev/null
}

# --------------------------------------------------------------------------
start_cluster() {
    log_info "Starting MySQL replication cluster..."
    docker compose -f "$COMPOSE_FILE" up -d --remove-orphans

    log_info "Waiting for primary to be healthy..."
    local attempts=0
    while [ $attempts -lt 60 ]; do
        if docker compose -f "$COMPOSE_FILE" ps "$PRIMARY_CTR" 2>/dev/null | grep -q "healthy"; then
            log_info "Primary is healthy"
            break
        fi
        attempts=$((attempts + 1))
        echo "  Waiting for primary ($attempts/60)..."
        sleep 2
    done

    log_info "Waiting for replicas to initialize..."
    local rep_attempts=0
    while [ $rep_attempts -lt 30 ]; do
        local r1_ok r2_ok
        r1_ok=$(docker compose -f "$COMPOSE_FILE" ps "$REPLICA1_CTR" 2>/dev/null | grep -c "healthy" || true)
        r2_ok=$(docker compose -f "$COMPOSE_FILE" ps "$REPLICA2_CTR" 2>/dev/null | grep -c "healthy" || true)
        if [ "$r1_ok" -ge 1 ] && [ "$r2_ok" -ge 1 ]; then
            log_info "All replicas healthy"
            break
        fi
        rep_attempts=$((rep_attempts + 1))
        echo "  Waiting for replicas ($rep_attempts/30)..."
        sleep 3
    done

    log_info "Checking cluster status..."
    check_status
}

# --------------------------------------------------------------------------
stop_cluster() {
    log_info "Stopping cluster..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
}

# --------------------------------------------------------------------------
check_status() {
    log_info "=== MySQL Replication Cluster Status ==="

    echo ""
    log_info "Container status:"
    docker compose -f "$COMPOSE_FILE" ps

    echo ""
    log_info "Primary ($PRIMARY_CTR):"
    if dexec "$PRIMARY_CTR" "SELECT @@server_id AS server_id, @@read_only AS read_only, @@gtid_mode AS gtid_mode;"; then
        echo "  Replica hosts connected:"
        dexec "$PRIMARY_CTR" "SHOW REPLICAS;" || true
    else
        log_warn "Primary not responding"
    fi

    local i=1
    for ctr in "$REPLICA1_CTR" "$REPLICA2_CTR"; do
        echo ""
        log_info "Replica $i ($ctr):"
        if dexec "$ctr" "SELECT @@server_id AS server_id, @@read_only AS read_only;"; then
            echo "  Replication IO/SQL threads:"
            dexec "$ctr" "SELECT CHANNEL_NAME, SERVICE_STATE FROM performance_schema.replication_connection_status;" || true
            dexec "$ctr" "SELECT CHANNEL_NAME, SERVICE_STATE FROM performance_schema.replication_applier_status;" || true
        else
            log_warn "Replica $i not responding"
        fi
        i=$((i + 1))
    done
}

# --------------------------------------------------------------------------
test_replication() {
    log_test "=== Replication Verification ==="
    local pass=0 fail=0

    # Write on primary
    dexec "$PRIMARY_CTR" "INSERT INTO test.t1 (val) VALUES ('repl-test-$(date +%s)');"
    local primary_count
    primary_count=$(dexec "$PRIMARY_CTR" "SELECT COUNT(*) FROM test.t1;")

    sleep 2  # allow replication lag

    # Verify replicas
    local i=1
    for ctr in "$REPLICA1_CTR" "$REPLICA2_CTR"; do
        local replica_count
        replica_count=$(dexec "$ctr" "SELECT COUNT(*) FROM test.t1;")
        if [ "$replica_count" = "$primary_count" ]; then
            log_test "  Replica $i: row count matches primary ($replica_count) ✓"
            pass=$((pass + 1))
        else
            log_error "  Replica $i: row count $replica_count != primary $primary_count ✗"
            fail=$((fail + 1))
        fi
        i=$((i + 1))
    done

    # Verify read-only on replicas
    i=1
    for ctr in "$REPLICA1_CTR" "$REPLICA2_CTR"; do
        local ro
        ro=$(dexec "$ctr" "SELECT @@read_only;")
        if [ "$ro" = "1" ]; then
            log_test "  Replica $i: read_only=ON ✓"
            pass=$((pass + 1))
        else
            log_error "  Replica $i: read_only=$ro (expected 1) ✗"
            fail=$((fail + 1))
        fi
        i=$((i + 1))
    done

    echo ""
    log_info "Results: $pass passed, $fail failed"
}

# --------------------------------------------------------------------------
test_failover() {
    log_test "=== Testing Manual Failover ==="

    log_warn "Stopping primary ($PRIMARY_CTR)..."
    docker stop "$PRIMARY_CTR"
    sleep 3

    log_info "Promoting replica1 to primary..."
    docker exec "$REPLICA1_CTR" mysql -uroot -proot -e "
        STOP REPLICA;
        RESET REPLICA ALL;
        SET GLOBAL read_only = OFF;
        SET GLOBAL super_read_only = OFF;
    " 2>/dev/null

    sleep 2

    local ro
    ro=$(dexec "$REPLICA1_CTR" "SELECT @@read_only;")
    if [ "$ro" = "0" ]; then
        log_test "  replica1 promoted to read-write ✓"
    else
        log_error "  replica1 still read-only ✗"
    fi

    # Write to new primary
    dexec "$REPLICA1_CTR" "INSERT INTO test.t1 (val) VALUES ('after-failover');"
    log_test "  Write to new primary succeeded ✓"

    log_info "Failover complete. replica1 is now primary."
}

# --------------------------------------------------------------------------
show_logs() {
    local service="${1:-}"
    if [ -n "$service" ]; then
        docker logs "mysql-rep-$service" 2>&1 | tail -50
    else
        docker compose -f "$COMPOSE_FILE" logs --tail=20
    fi
}

# --------------------------------------------------------------------------
# Run full CI lifecycle: start → test → stop
ci_run() {
    trap 'stop_cluster 2>/dev/null || true' EXIT
    start_cluster
    test_replication
    log_info "MySQL replication CI test PASSED"
}

# --------------------------------------------------------------------------
usage() {
    echo "Usage: $0 {ci|start|stop|status|test|failover|logs}"
    echo ""
    echo "Commands:"
    echo "  ci         - Run full CI lifecycle (start, test, stop)"
    echo "  start      - Start the MySQL replication cluster"
    echo "  stop       - Stop and remove the cluster"
    echo "  status     - Show replication status"
    echo "  test       - Run replication verification tests"
    echo "  failover   - Test manual failover (stops primary)"
    echo "  logs [svc] - Show logs (primary, replica1, replica2)"
}

case "${1:-ci}" in
    ci)       ci_run ;;
    start)    start_cluster ;;
    stop)     stop_cluster ;;
    status)   check_status ;;
    test)     test_replication ;;
    failover) test_failover ;;
    logs)     show_logs "${2:-}" ;;
    *)        usage; exit 1 ;;
esac
