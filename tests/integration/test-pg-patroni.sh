#!/bin/bash
# Test script for Patroni + etcd cluster
set -e

cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_header() { echo -e "${BLUE}=== $1 ===${NC}"; }

# Start cluster
start_cluster() {
    log_info "Starting Patroni + etcd cluster..."
    docker compose -f ../../docker/compose/pg-patroni.yml up -d
    
    log_info "Waiting for cluster to initialize (this may take 30-60 seconds)..."
    
    # Wait for etcd
    until curl -s http://localhost:2379/health >/dev/null 2>&1; do
        echo "Waiting for etcd..."
        sleep 2
    done
    log_info "etcd is ready"
    
    # Wait for at least one Patroni node
    local attempts=0
    until curl -s http://localhost:8008/ >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [ $attempts -gt 30 ]; then
            log_error "Timeout waiting for Patroni"
            exit 1
        fi
        echo "Waiting for Patroni ($attempts/30)..."
        sleep 3
    done
    log_info "Patroni is ready"
    
    sleep 5
    check_status
}

# Stop cluster
stop_cluster() {
    log_info "Stopping cluster..."
    docker compose -f ../../docker/compose/pg-patroni.yml down -v
}

# Check cluster status
check_status() {
    log_header "Patroni Cluster Status"
    
    echo ""
    log_info "Cluster members:"
    
    # Get status from each node
    for port in 8008 8009 8010; do
        local node_name="pg-patroni-$((port - 8007))"
        local pg_port=$((port - 2576))
        
        if response=$(curl -s http://localhost:$port/ 2>/dev/null); then
            local role=$(echo "$response" | grep -o '"role": *"[^"]*"' | grep -o '[^"]*"$' | tr -d '"')
            local state=$(echo "$response" | grep -o '"state": *"[^"]*"' | grep -o '[^"]*"$' | tr -d '"')
            
            if [ "$role" = "primary" ] || [ "$role" = "master" ]; then
                echo -e "  ${GREEN}●${NC} $node_name (port $pg_port): ${GREEN}PRIMARY${NC} - $state"
            else
                echo -e "  ${YELLOW}○${NC} $node_name (port $pg_port): ${YELLOW}REPLICA${NC} - $state"
            fi
        else
            echo -e "  ${RED}✗${NC} $node_name (port $pg_port): ${RED}UNREACHABLE${NC}"
        fi
    done
    
    echo ""
    log_info "Leader endpoint:"
    if leader=$(curl -s http://localhost:8008/leader 2>/dev/null); then
        echo "  $leader" | head -c 200
        echo ""
    else
        echo "  No leader found"
    fi
}

# Check via patronictl
check_patronictl() {
    log_header "Patronictl Status"
    docker exec pg-patroni-1 patronictl list 2>/dev/null || \
        docker exec pg-patroni-2 patronictl list 2>/dev/null || \
        docker exec pg-patroni-3 patronictl list 2>/dev/null
}

# Test automatic failover
test_failover() {
    log_header "Testing Automatic Failover"
    
    # Find current leader
    log_info "Finding current leader..."
    local leader=""
    for port in 8008 8009 8010; do
        if response=$(curl -s http://localhost:$port/ 2>/dev/null); then
            local role=$(echo "$response" | grep -o '"role": *"[^"]*"' | grep -o '[^"]*"$' | tr -d '"')
            if [ "$role" = "primary" ] || [ "$role" = "master" ]; then
                leader="pg-patroni-$((port - 8007))"
                log_info "Current leader: $leader"
                break
            fi
        fi
    done
    
    if [ -z "$leader" ]; then
        log_error "No leader found!"
        exit 1
    fi
    
    log_warn "Stopping leader ($leader)..."
    docker stop "$leader"
    
    log_info "Waiting for automatic failover..."
    sleep 15
    
    log_info "New cluster status:"
    check_status
}

# Trigger planned switchover
test_switchover() {
    log_header "Testing Planned Switchover"
    
    log_info "Current status:"
    check_patronictl
    
    echo ""
    log_info "Initiating switchover..."
    docker exec pg-patroni-1 patronictl switchover --force 2>/dev/null || \
        docker exec pg-patroni-2 patronictl switchover --force 2>/dev/null || \
        docker exec pg-patroni-3 patronictl switchover --force 2>/dev/null
    
    sleep 10
    
    log_info "New status after switchover:"
    check_patronictl
}

# Restart a stopped node
restart_node() {
    local node="${1:-pg-patroni-1}"
    log_info "Restarting $node..."
    docker start "$node"
    
    sleep 10
    check_status
}

# Watch cluster changes in real-time
watch_cluster() {
    log_info "Watching cluster status (Ctrl+C to stop)..."
    watch -n2 "curl -s http://localhost:8008/ http://localhost:8009/ http://localhost:8010/ 2>/dev/null | grep -E 'role|state' || echo 'Fetching...'"
}

# Test KEEL probe endpoints
test_probe_endpoints() {
    log_header "Testing Patroni Probe Endpoints"
    
    echo ""
    log_info "Health endpoints:"
    for port in 8008 8009 8010; do
        local status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/health)
        echo "  patroni$((port - 8007)): $status"
    done
    
    echo ""
    log_info "Primary endpoint (returns 200 only on primary):"
    for port in 8008 8009 8010; do
        local status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary)
        if [ "$status" = "200" ]; then
            echo -e "  patroni$((port - 8007)): ${GREEN}$status (PRIMARY)${NC}"
        else
            echo "  patroni$((port - 8007)): $status"
        fi
    done
    
    echo ""
    log_info "Replica endpoint (returns 200 only on replica):"
    for port in 8008 8009 8010; do
        local status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/replica)
        if [ "$status" = "200" ]; then
            echo -e "  patroni$((port - 8007)): ${YELLOW}$status (REPLICA)${NC}"
        else
            echo "  patroni$((port - 8007)): $status"
        fi
    done
    
    echo ""
    log_info "JSON status:"
    curl -s http://localhost:8008/ | python3 -m json.tool 2>/dev/null || \
        curl -s http://localhost:8008/
}

# Run full CI lifecycle: start → verify primary/replica → probe → stop
ci_run() {
    trap 'docker compose -f ../../docker/compose/pg-patroni.yml down -v 2>/dev/null || true' EXIT
    start_cluster

    log_info "Verifying a Patroni primary exists..."
    primary_found=false
    for port in 8008 8009 8010; do
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/primary" 2>/dev/null || true)
        if [ "$code" = "200" ]; then
            primary_found=true
            log_info "Primary confirmed on patroni$((port - 8007)) (port $port)"
            break
        fi
    done
    if ! $primary_found; then
        log_error "No Patroni primary found after start"
        exit 1
    fi

    log_info "Verifying a Patroni replica exists..."
    replica_found=false
    for port in 8008 8009 8010; do
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/replica" 2>/dev/null || true)
        if [ "$code" = "200" ]; then
            replica_found=true
            log_info "Replica confirmed on patroni$((port - 8007)) (port $port)"
            break
        fi
    done
    if ! $replica_found; then
        log_error "No Patroni replica found after start"
        exit 1
    fi

    test_probe_endpoints
    log_info "Patroni CI test PASSED"
}

# Usage
usage() {
    echo "Usage: $0 {ci|start|stop|status|patronictl|failover|switchover|restart|watch|probe}"
    echo ""
    echo "Commands:"
    echo "  ci         - Run full CI lifecycle (start, verify, probe, stop)"
    echo "  start      - Start the Patroni + etcd cluster"
    echo "  stop       - Stop and remove the cluster"
    echo "  status     - Check cluster status via REST API"
    echo "  patronictl - Check status via patronictl"
    echo "  failover   - Test automatic failover (kills leader)"
    echo "  switchover - Test planned switchover"
    echo "  restart    - Restart a node (default: patroni1)"
    echo "  watch      - Watch cluster status in real-time"
    echo "  probe      - Test Patroni probe endpoints for KEEL"
}

case "${1:-ci}" in
    ci)         ci_run ;;
    start)      start_cluster ;;
    stop)       stop_cluster ;;
    status)     check_status ;;
    patronictl) check_patronictl ;;
    failover)   test_failover ;;
    switchover) test_switchover ;;
    restart)    restart_node "${2:-pg-patroni-1}" ;;
    watch)      watch_cluster ;;
    probe)      test_probe_endpoints ;;
    *)          usage; exit 1 ;;
esac
