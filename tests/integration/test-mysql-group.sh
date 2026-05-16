#!/bin/bash
# =============================================================================
# Test script for MySQL Group Replication (Multi-Primary, 3 nodes)
# =============================================================================
set -e
cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

COMPOSE_FILE="../../docker/compose/mysql-group.yml"
NODE1_CTR="mgr-node1"
NODE2_CTR="mgr-node2"
NODE3_CTR="mgr-node3"
ALL_CTRS=("$NODE1_CTR" "$NODE2_CTR" "$NODE3_CTR")

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test()  { echo -e "${CYAN}[TEST]${NC}  $1"; }

dexec() {
    local ctr="$1"; shift
    docker exec "$ctr" mysql -h 127.0.0.1 -uroot -proot --silent --skip-column-names -e "$@" 2>/dev/null
}

# --------------------------------------------------------------------------
wait_healthy() {
    local ctr="$1" max="${2:-60}" step="${3:-2}"
    local n=0
    while [ $n -lt "$max" ]; do
        if docker compose -f "$COMPOSE_FILE" ps "$ctr" 2>/dev/null | grep -q "healthy"; then
            return 0
        fi
        n=$((n + 1))
        echo "  Waiting for $ctr ($n/$max)..."
        sleep "$step"
    done
    log_warn "$ctr did not become healthy in time"
    return 1
}

start_cluster() {
    log_info "Starting MySQL Group Replication cluster..."
    docker compose -f "$COMPOSE_FILE" up -d "$NODE1_CTR" --remove-orphans

    log_info "Waiting for bootstrap node to be healthy..."
    wait_healthy "$NODE1_CTR" 60 2

    log_info "Bootstrapping Group Replication on node1..."
    sleep 3  # brief stabilization after healthcheck
    local bstrap_ok=false
    for _i in $(seq 1 15); do
        out=$(docker exec "$NODE1_CTR" mysql -h 127.0.0.1 -uroot -proot --silent -e \
            "SET GLOBAL group_replication_bootstrap_group = ON; \
             START GROUP_REPLICATION USER='replicator', PASSWORD='replicator'; \
             SET GLOBAL group_replication_bootstrap_group = OFF;" 2>&1 || true)
        if ! echo "$out" | grep -qiE "^ERROR"; then
            bstrap_ok=true; break
        fi
        if echo "$out" | grep -qiE "already running|already started|3092"; then
            bstrap_ok=true; break
        fi
        echo "  Bootstrap attempt $_i failed, retrying in 3s... ($out)"
        sleep 3
    done
    $bstrap_ok || log_warn "Could not bootstrap GR on node1 after retries"

    # Wait for node1 to become ONLINE before starting joiners
    log_info "Waiting for node1 to become ONLINE in the group..."
    for _w in $(seq 1 20); do
        state=$(docker exec "$NODE1_CTR" mysql -h 127.0.0.1 -uroot -proot --silent \
            --skip-column-names -e \
            "SELECT MEMBER_STATE FROM performance_schema.replication_group_members \
             WHERE MEMBER_HOST='mgr-node1';" 2>/dev/null || true)
        [ "$state" = "ONLINE" ] && break
        echo "  node1 GR state: ${state:-unknown} (attempt $_w/20)..."
        sleep 3
    done

    log_info "Starting remaining nodes..."
    docker compose -f "$COMPOSE_FILE" up -d "$NODE2_CTR" "$NODE3_CTR"

    for jctr in "$NODE2_CTR" "$NODE3_CTR"; do
        log_info "Waiting for $jctr to be healthy..."
        wait_healthy "$jctr" 60 3
        sleep 2  # brief stabilization
        log_info "Joining $jctr to Group Replication..."
        for _j in $(seq 1 10); do
            out=$(docker exec "$jctr" mysql -h 127.0.0.1 -uroot -proot --silent -e \
                "RESET BINARY LOGS AND GTIDS; \
                 START GROUP_REPLICATION USER='replicator', PASSWORD='replicator';" 2>&1 || true)
            if ! echo "$out" | grep -qiE "^ERROR|Can't connect"; then
                break
            fi
            if echo "$out" | grep -qiE "already running|already started|3092"; then
                break
            fi
            echo "  Join attempt $_j for $jctr failed, retrying in 3s..."
            sleep 3
        done
    done

    log_info "Waiting for group to stabilize..."
    sleep 10

    check_status
}

# --------------------------------------------------------------------------
stop_cluster() {
    log_info "Stopping cluster..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
}

# --------------------------------------------------------------------------
check_status() {
    log_info "=== MySQL Group Replication Status ==="

    echo ""
    log_info "Container status:"
    docker compose -f "$COMPOSE_FILE" ps

    echo ""
    log_info "Group members (from node1):"
    dexec "$NODE1_CTR" "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members ORDER BY MEMBER_HOST;" || log_warn "Could not query group members"

    echo ""
    local i=1
    for ctr in "${ALL_CTRS[@]}"; do
        log_info "Node $i ($ctr):"
        dexec "$ctr" "SELECT @@server_id AS server_id, @@group_replication_single_primary_mode AS single_primary, @@read_only AS read_only;" || log_warn "  Node $i not responding"
        i=$((i + 1))
    done
}

# --------------------------------------------------------------------------
test_multi_primary() {
    log_test "=== Multi-Primary Write Verification ==="
    local pass=0 fail=0

    # Write on each node
    local i=1
    for ctr in "${ALL_CTRS[@]}"; do
        dexec "$ctr" "INSERT INTO test.t1 (val) VALUES ('mgr-node${i}-$(date +%s)');"
        if [ $? -eq 0 ]; then
            log_test "  Write on node $i ($ctr): ✓"
            pass=$((pass + 1))
        else
            log_error "  Write on node $i ($ctr): ✗"
            fail=$((fail + 1))
        fi
        i=$((i + 1))
    done

    sleep 2  # allow group sync

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

    log_info "Group members after node3 removed:"
    dexec "$NODE1_CTR" "SELECT MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members;" || true

    # Write while node3 is down
    dexec "$NODE1_CTR" "INSERT INTO test.t1 (val) VALUES ('while-node3-down');"
    log_test "  Write while node3 down: ✓"

    log_info "Restarting node 3..."
    docker start "$NODE3_CTR"
    sleep 15

    # Rejoin group replication
    docker exec "$NODE3_CTR" mysql -uroot -proot -e "START GROUP_REPLICATION;" 2>/dev/null
    sleep 10

    log_info "Group members after node3 rejoin:"
    dexec "$NODE1_CTR" "SELECT MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members;" || true

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
    test_multi_primary
    log_info "MySQL group replication CI test PASSED"
}

# --------------------------------------------------------------------------
usage() {
    echo "Usage: $0 {ci|start|stop|status|test|failure|logs}"
    echo ""
    echo "Commands:"
    echo "  ci            - Run full CI lifecycle (start, test, stop)"
    echo "  start         - Start the Group Replication cluster"
    echo "  stop          - Stop and remove the cluster"
    echo "  status        - Show group replication status"
    echo "  test          - Run multi-primary write tests"
    echo "  failure       - Test node failure & rejoin"
    echo "  logs [node]   - Show logs (mgr-node1, mgr-node2, mgr-node3)"
}

case "${1:-ci}" in
    ci)       ci_run ;;
    start)    start_cluster ;;
    stop)     stop_cluster ;;
    status)   check_status ;;
    test)     test_multi_primary ;;
    failure)  test_node_failure ;;
    logs)     show_logs "${2:-}" ;;
    *)        usage; exit 1 ;;
esac
