#!/bin/bash
# =============================================================================
# Test script for Percona XtraDB Cluster 8.4 (3-node Galera)
# =============================================================================
set -e
cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

COMPOSE_FILE="../../docker/compose/mysql-pxc.yml"
NODE1_CTR="pxc-node1"
NODE2_CTR="pxc-node2"
NODE3_CTR="pxc-node3"
ALL_CTRS=("$NODE1_CTR" "$NODE2_CTR" "$NODE3_CTR")

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test()  { echo -e "${CYAN}[TEST]${NC}  $1"; }

dexec() {
    local ctr="$1"; shift
    docker exec "$ctr" mysql -uroot -proot --silent --skip-column-names -e "$@" 2>/dev/null
}

# --------------------------------------------------------------------------
start_cluster() {
    log_info "Starting Percona XtraDB Cluster..."

    # Bootstrap node1 first
    docker compose -f "$COMPOSE_FILE" up -d "$NODE1_CTR" --remove-orphans

    log_info "Waiting for bootstrap node to be ready (wsrep Synced)..."
    local attempts=0
    while [ $attempts -lt 60 ]; do
        if docker compose -f "$COMPOSE_FILE" ps "$NODE1_CTR" 2>/dev/null | grep -q "healthy"; then
            break
        fi
        attempts=$((attempts + 1))
        echo "  Waiting for node1 ($attempts/60)..."
        sleep 5
    done

    log_info "Starting node2 (SST from node1)..."
    docker compose -f "$COMPOSE_FILE" up -d "$NODE2_CTR"

    log_info "Waiting for node2 to sync (xtrabackup SST takes ~30s)..."
    local n2_attempts=0
    while [ $n2_attempts -lt 40 ]; do
        local n2_state
        n2_state=$(docker exec "$NODE2_CTR" mysql -h 127.0.0.1 -uroot -proot --silent --skip-column-names \
            -e "SHOW STATUS LIKE 'wsrep_local_state_comment';" 2>/dev/null | awk '{print $2}')
        [ "$n2_state" = "Synced" ] && break
        n2_attempts=$((n2_attempts + 1))
        echo "  Waiting for node2 sync ($n2_attempts/40, state=$n2_state)..."
        sleep 3
    done

    log_info "Starting node3 (SST from node1 or node2)..."
    docker compose -f "$COMPOSE_FILE" up -d "$NODE3_CTR"

    log_info "Waiting for cluster formation (PXC IST/SST can take a while)..."
    sleep 40

    check_status
}

# --------------------------------------------------------------------------
stop_cluster() {
    log_info "Stopping cluster..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
}

# --------------------------------------------------------------------------
check_status() {
    log_info "=== Percona XtraDB Cluster Status ==="

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
    log_test "=== PXC Multi-Master Write Verification ==="
    local pass=0 fail=0

    # Write on each node
    local i=1
    for ctr in "${ALL_CTRS[@]}"; do
        dexec "$ctr" "INSERT INTO test.t1 (val) VALUES ('pxc-node${i}-$(date +%s)');"
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
    sleep 20

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
usage() {
    echo "Usage: $0 {ci|start|stop|status|test|failure|logs}"
    echo ""
    echo "Commands:"
    echo "  ci            - Run full CI lifecycle (start, test, stop)"
    echo "  start         - Start the PXC cluster"
    echo "  stop          - Stop and remove the cluster"
    echo "  status        - Show wsrep cluster status"
    echo "  test          - Run multi-master write tests"
    echo "  failure       - Test node failure & rejoin"
    echo "  logs [node]   - Show logs (pxc-node1, pxc-node2, pxc-node3)"
}

# Run full CI lifecycle: start → test → stop
ci_run() {
    trap 'stop_cluster 2>/dev/null || true' EXIT
    start_cluster
    test_replication
    log_info "PXC CI test PASSED"
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
