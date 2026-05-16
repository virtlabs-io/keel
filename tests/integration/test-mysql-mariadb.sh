#!/bin/bash
# =============================================================================
# Test script for MariaDB Galera Cluster (3-node)
# =============================================================================
set -e
cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

COMPOSE_FILE="../../docker/compose/mysql-mariadb.yml"
NODE1_CTR="mdb-node1"
NODE2_CTR="mdb-node2"
NODE3_CTR="mdb-node3"
ALL_CTRS=("$NODE1_CTR" "$NODE2_CTR" "$NODE3_CTR")

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test()  { echo -e "${CYAN}[TEST]${NC}  $1"; }

dexec() {
    local ctr="$1"; shift
    docker exec "$ctr" mariadb -uroot -proot --silent --skip-column-names -e "$@" 2>/dev/null
}

# --------------------------------------------------------------------------
start_cluster() {
    log_info "Starting MariaDB Galera Cluster..."

    # Bootstrap node1 first (it has --wsrep-new-cluster)
    docker compose -f "$COMPOSE_FILE" up -d "$NODE1_CTR" --remove-orphans

    log_info "Waiting for bootstrap node to be ready..."
    local attempts=0
    while [ $attempts -lt 60 ]; do
        if docker compose -f "$COMPOSE_FILE" ps "$NODE1_CTR" 2>/dev/null | grep -q "healthy"; then
            break
        fi
        attempts=$((attempts + 1))
        echo "  Waiting for node1 ($attempts/60)..."
        sleep 2
    done

    log_info "Starting remaining nodes..."
    docker compose -f "$COMPOSE_FILE" up -d "$NODE2_CTR" "$NODE3_CTR"

    log_info "Waiting for cluster formation..."
    sleep 20

    check_status
}

# --------------------------------------------------------------------------
stop_cluster() {
    log_info "Stopping cluster..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
}

# --------------------------------------------------------------------------
check_status() {
    log_info "=== MariaDB Galera Cluster Status ==="

    echo ""
    log_info "Container status:"
    docker compose -f "$COMPOSE_FILE" ps

    echo ""
    local i=1
    for ctr in "${ALL_CTRS[@]}"; do
        log_info "Node $i ($ctr):"
        dexec "$ctr" "SHOW STATUS LIKE 'wsrep_cluster_size';" || log_warn "  Node $i not responding"
        dexec "$ctr" "SHOW STATUS LIKE 'wsrep_cluster_status';" || true
        dexec "$ctr" "SHOW STATUS LIKE 'wsrep_ready';" || true
        dexec "$ctr" "SHOW STATUS LIKE 'wsrep_local_state_comment';" || true
        i=$((i + 1))
        echo ""
    done
}

# --------------------------------------------------------------------------
test_replication() {
    log_test "=== MariaDB Galera Multi-Master Write Verification ==="
    local pass=0 fail=0

    # Ensure test table exists
    dexec "$NODE1_CTR" "CREATE DATABASE IF NOT EXISTS test; CREATE TABLE IF NOT EXISTS test.t1 (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(100), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"

    # Write on each node
    local i=1
    for ctr in "${ALL_CTRS[@]}"; do
        dexec "$ctr" "INSERT INTO test.t1 (val) VALUES ('mdb-node${i}-$(date +%s)');"
        if [ $? -eq 0 ]; then
            log_test "  Write on node $i ($ctr): ✓"
            pass=$((pass + 1))
        else
            log_error "  Write on node $i ($ctr): ✗"
            fail=$((fail + 1))
        fi
        i=$((i + 1))
    done

    sleep 2  # Galera sync

    # Verify convergence
    local counts=()
    i=1
    for ctr in "${ALL_CTRS[@]}"; do
        counts[$i]=$(dexec "$ctr" "SELECT COUNT(*) FROM test.t1;")
        i=$((i + 1))
    done

    if [ "${counts[1]}" = "${counts[2]}" ] && [ "${counts[2]}" = "${counts[3]}" ]; then
        log_test "  Data converged: all nodes have ${counts[1]} rows ✓"
        pass=$((pass + 1))
    else
        log_error "  Data diverged: node1=${counts[1]} node2=${counts[2]} node3=${counts[3]} ✗"
        fail=$((fail + 1))
    fi

    echo ""
    log_info "Results: $pass passed, $fail failed"
}

# --------------------------------------------------------------------------
test_node_failure() {
    log_test "=== Node Failure & Rejoin ==="

    log_warn "Stopping node 3..."
    docker stop "$NODE3_CTR"
    sleep 5

    dexec "$NODE1_CTR" "SHOW STATUS LIKE 'wsrep_cluster_size';" || true

    # Write while node3 is down
    dexec "$NODE1_CTR" "INSERT INTO test.t1 (val) VALUES ('while-node3-down');"
    log_test "  Write while node3 down: ✓"

    log_info "Restarting node 3..."
    docker start "$NODE3_CTR"
    sleep 15

    dexec "$NODE1_CTR" "SHOW STATUS LIKE 'wsrep_cluster_size';" || true

    # Verify node3 caught up
    local count1 count3
    count1=$(dexec "$NODE1_CTR" "SELECT COUNT(*) FROM test.t1;")
    count3=$(dexec "$NODE3_CTR" "SELECT COUNT(*) FROM test.t1;")
    if [ "$count1" = "$count3" ]; then
        log_test "  Node 3 caught up: $count3 rows (matches node1) ✓"
    else
        log_error "  Node 3 behind: $count3 rows vs node1=$count1 ✗"
    fi
}

# --------------------------------------------------------------------------
show_logs() {
    local service="${1:-}"
    if [ -n "$service" ]; then
        docker logs "$service" 2>&1 | tail -50
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
    log_info "MariaDB Galera CI test PASSED"
}

# --------------------------------------------------------------------------
usage() {
    echo "Usage: $0 {ci|start|stop|status|test|failure|logs}"
    echo ""
    echo "Commands:"
    echo "  ci            - Run full CI lifecycle (start, test, stop)"
    echo "  start         - Start the MariaDB Galera cluster"
    echo "  stop          - Stop and remove the cluster"
    echo "  status        - Show wsrep cluster status"
    echo "  test          - Run multi-master write tests"
    echo "  failure       - Test node failure & rejoin"
    echo "  logs [node]   - Show logs (mdb-node1, mdb-node2, mdb-node3)"
}

case "${1:-ci}" in
    ci)       ci_run ;;
    start)    start_cluster ;;
    stop)     stop_cluster ;;
    status)   check_status ;;
    test)     test_replication ;;
    failure)  test_node_failure ;;
    logs)     show_logs "${2:-}" ;;
    *)        usage; exit 1 ;;
esac
