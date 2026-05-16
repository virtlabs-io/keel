#!/bin/bash
# Test script for streaming replication cluster
set -e

cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Start cluster
start_cluster() {
    log_info "Starting streaming replication cluster..."
    docker compose -f ../../docker/compose/pg-streaming.yml up -d
    
    log_info "Waiting for primary to be healthy..."
    local attempts=0
    while [ $attempts -lt 60 ]; do
        if docker compose -f ../../docker/compose/pg-streaming.yml ps primary | grep -q "healthy"; then
            log_info "Primary is healthy"
            break
        fi
        attempts=$((attempts + 1))
        echo "Waiting for primary ($attempts/60)..."
        sleep 2
    done
    
    log_info "Waiting for replicas to start..."
    sleep 15
    
    log_info "Checking cluster status..."
    check_status
}

# Stop cluster
stop_cluster() {
    log_info "Stopping cluster..."
    docker compose -f ../../docker/compose/pg-streaming.yml down -v
}

# Check replication status
check_status() {
    log_info "=== Cluster Status ==="
    
    echo ""
    log_info "Container status:"
    docker compose -f ../../docker/compose/pg-streaming.yml ps
    
    echo ""
    log_info "Primary (port 5432):"
    if PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c \
        "SELECT pg_is_in_recovery() as is_replica, pg_current_wal_lsn() as wal_lsn;" 2>/dev/null; then
        :
    else
        log_warn "Primary not responding yet"
    fi
    
    echo ""
    log_info "Replica 1 (port 5433):"
    if PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -c \
        "SELECT pg_is_in_recovery() as is_replica, pg_last_wal_receive_lsn() as received_lsn;" 2>/dev/null; then
        :
    else
        log_warn "Replica 1 not responding yet"
    fi
    
    echo ""
    log_info "Replica 2 (port 5434):"
    if PGPASSWORD=postgres psql -h localhost -p 5434 -U postgres -c \
        "SELECT pg_is_in_recovery() as is_replica, pg_last_wal_receive_lsn() as received_lsn;" 2>/dev/null; then
        :
    else
        log_warn "Replica 2 not responding yet"
    fi
}

# Show logs
show_logs() {
    local service="${1:-}"
    if [ -n "$service" ]; then
        docker logs "pg-$service" 2>&1 | tail -50
    else
        docker compose -f ../../docker/compose/pg-streaming.yml logs --tail=20
    fi
}

# Test failover (manual)
test_failover() {
    log_info "=== Testing Manual Failover ==="
    
    log_warn "Stopping primary..."
    docker stop pg-stream-primary
    
    sleep 2
    
    log_info "Promoting replica1 to primary..."
    docker exec pg-stream-replica1 pg_ctl promote -D /var/lib/postgresql/data
    
    sleep 3
    
    log_info "New primary status (port 5433):"
    PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -c \
        "SELECT pg_is_in_recovery() as is_replica;"
    
    log_info "Failover complete. replica1 is now primary."
}

# Run full CI lifecycle: start → verify replication → stop
ci_run() {
    trap 'docker compose -f ../../docker/compose/pg-streaming.yml down -v 2>/dev/null || true' EXIT
    start_cluster

    log_info "Verifying primary is not in recovery..."
    is_replica=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -tAq \
        -c "SELECT pg_is_in_recovery();" 2>/dev/null || true)
    if [ "$is_replica" != "f" ]; then
        log_error "Primary check failed: pg_is_in_recovery()=${is_replica:-<no response>}"
        exit 1
    fi
    log_info "Primary OK (not in recovery)"

    log_info "Verifying replicas are in recovery..."
    for port in 5433 5434; do
        is_replica=$(PGPASSWORD=postgres psql -h localhost -p "$port" -U postgres -tAq \
            -c "SELECT pg_is_in_recovery();" 2>/dev/null || true)
        if [ "$is_replica" != "t" ]; then
            log_error "Replica port $port check failed: pg_is_in_recovery()=${is_replica:-<no response>}"
            exit 1
        fi
        log_info "Replica port $port OK (in recovery)"
    done

    log_info "Verifying streaming replication is active..."
    replica_count=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -tAq \
        -c "SELECT count(*) FROM pg_stat_replication WHERE state='streaming';" 2>/dev/null || true)
    if [ "${replica_count:-0}" -lt 1 ]; then
        log_error "No streaming replicas found (count=${replica_count:-0})"
        exit 1
    fi
    log_info "Streaming replication active: $replica_count replica(s)"
    log_info "Streaming replication CI test PASSED"
}

# Usage
usage() {
    echo "Usage: $0 {ci|start|stop|status|logs|failover}"
    echo ""
    echo "Commands:"
    echo "  ci        - Run full CI lifecycle (start, verify, stop)"
    echo "  start     - Start the streaming replication cluster"
    echo "  stop      - Stop and remove the cluster"
    echo "  status    - Check replication status"
    echo "  logs [svc]- Show logs (primary, replica1, replica2)"
    echo "  failover  - Test manual failover (stops primary)"
}

case "${1:-ci}" in
    ci)       ci_run ;;
    start)    start_cluster ;;
    stop)     stop_cluster ;;
    status)   check_status ;;
    logs)     show_logs "${2:-}" ;;
    failover) test_failover ;;
    *)        usage; exit 1 ;;
esac
