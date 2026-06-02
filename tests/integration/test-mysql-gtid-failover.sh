#!/usr/bin/env bash
# =============================================================================
# Integration test: MySQL GTID-aware failover through KEEL.
# =============================================================================
#
# Validates the failover-manager track from
# proposals/keel-v.05-alpha-consistent_read-failover-pstmt.md (§4 MySQL):
#
#   1. Bring up MySQL primary + 2 replicas (async GTID replication).
#   2. Start KEEL pointed at the cluster.
#   3. Write through KEEL, capture the resulting GTID set from the primary.
#   4. Wait until both replicas have applied that GTID (GTID catch-up).
#   5. Hard-stop the primary, promote replica1 (READ_ONLY=OFF).
#   6. Wait for KEEL's MySQL probe to detect the role flip
#      (probe_interval=2s + failover_delay=5s ≤ ~25s budget).
#   7. Write again through KEEL → must succeed against the new primary
#      with no "read-only" error.
#   8. Read back the row through KEEL → must observe the post-failover write.
#
# This is a Docker-backed integration test and is NOT registered with ctest.
# Run manually or from CI when Docker is available.  Requires:
#   - docker / docker compose
#   - mysql client in $PATH
#   - keel binary at build/src/main/keel (run cmake --build build first)
#
# Usage:
#   tests/integration/test-mysql-gtid-failover.sh         # full lifecycle
#   tests/integration/test-mysql-gtid-failover.sh keep    # keep cluster up
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/../.."
REPO_ROOT="$PWD"

COMPOSE_FILE="docker/compose/mysql-replication.yml"
KEEL_BIN="${KEEL_BIN:-$REPO_ROOT/build/src/main/keel}"
KEEL_PORT="${KEEL_PORT:-17306}"
KEEL_ADMIN_PORT="${KEEL_ADMIN_PORT:-16433}"
KEEL_PROM_PORT="${KEEL_PROM_PORT:-19101}"

PRIMARY_CTR="mysql-rep-primary"
REPLICA1_CTR="mysql-rep-replica1"
REPLICA2_CTR="mysql-rep-replica2"

# docker-compose `ps SVC` expects service names, which differ from container
# names in this compose file (no "rep-" prefix). Health gating uses
# `docker inspect` against the container directly so we don't care.
is_healthy() {
    local ctr="$1" status
    status=$(docker inspect --format='{{.State.Health.Status}}' "$ctr" 2>/dev/null || true)
    [[ "$status" == "healthy" ]]
}

WORKDIR="$(mktemp -d -t keel-mysql-failover.XXXXXX)"
KEEL_INI="$WORKDIR/keel.ini"
KEEL_LOG="$WORKDIR/keel.log"
KEEL_PID=""

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()   { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR ]${NC}  $*" >&2; }
test_h(){ echo -e "${CYAN}[TEST]${NC}  $*"; }

dexec() { docker exec "$1" mysql -uroot -proot --silent --skip-column-names -e "$2" 2>/dev/null; }

mysql_through_keel() {
    MYSQL_PWD=keel mysql -h 127.0.0.1 -P "$KEEL_PORT" -u keel \
          --silent --skip-column-names --connect-timeout=5 \
          -e "$1" test 2>&1
}

cleanup() {
    local rc=$?
    if [[ -n "$KEEL_PID" ]] && kill -0 "$KEEL_PID" 2>/dev/null; then
        log "Stopping KEEL (pid=$KEEL_PID)"
        kill "$KEEL_PID" 2>/dev/null || true
        wait "$KEEL_PID" 2>/dev/null || true
    fi
    if [[ "${1:-}" != "keep" ]] && [[ "${KEEP_CLUSTER:-0}" != "1" ]]; then
        log "Tearing down MySQL cluster"
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
    else
        warn "Leaving cluster up. KEEL log: $KEEL_LOG"
    fi
    [[ $rc -ne 0 ]] && err "Test FAILED (exit=$rc). Logs preserved at $WORKDIR"
    [[ $rc -eq 0 ]] && rm -rf "$WORKDIR"
    exit "$rc"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# 1. Cluster bring-up
# ---------------------------------------------------------------------------
start_cluster() {
    log "Starting MySQL replication cluster..."
    docker compose -f "$COMPOSE_FILE" up -d --remove-orphans >/dev/null

    log "Waiting for primary to become healthy..."
    for i in $(seq 1 60); do
        if is_healthy "$PRIMARY_CTR"; then
            log "  Primary healthy after $((i*2))s"; break
        fi
        sleep 2
    done

    log "Waiting for replicas..."
    for i in $(seq 1 60); do
        if is_healthy "$REPLICA1_CTR" && is_healthy "$REPLICA2_CTR"; then
            log "  Both replicas healthy after $((i*2))s"; break
        fi
        sleep 2
    done

    if ! dexec "$PRIMARY_CTR" "SELECT 1;" >/dev/null; then
        err "Primary unreachable after bring-up"; exit 1
    fi
    log "Cluster ready: primary=$(dexec "$PRIMARY_CTR" 'SELECT @@gtid_mode;')"
}

# ---------------------------------------------------------------------------
# 2. KEEL config + launch
# ---------------------------------------------------------------------------
write_keel_config() {
    cat > "$KEEL_INI" <<EOF
[keel]
config_version = 2
log_level = 2

[worker_group.mysql_failover]
name = mysql_failover
protocol = mysql
bind_addr = 127.0.0.1
bind_port = $KEEL_PORT
num_workers = 2
max_conns_per_worker = 256

min_pool_size = 2
max_pool_size = 16

probe = mysql
probe_interval = 2s
probe_timeout = 2s
probe_retries = 2
failover_delay = 5s

server_user = keel
server_password = keel

[worker_group.mysql_failover.servers]
primary  = host=127.0.0.1 port=3306 user=keel password=keel dbname=test role=RW weight=100
replica1 = host=127.0.0.1 port=3307 user=keel password=keel dbname=test role=RO weight=100
replica2 = host=127.0.0.1 port=3308 user=keel password=keel dbname=test role=RO weight=100

[failover]
failover_provider = native
failover_detection_interval = 2s
failover_failure_threshold = 2
failover_promotion_grace = 1s
failover_old_primary_fencing_required = true
failover_transaction_during_failover = fail
failover_read_during_failover = degraded

[admin]
enabled = true
listen_addr = 127.0.0.1
listen_port = $KEEL_ADMIN_PORT
users = admin

[prometheus]
enabled = true
listen_addr = 127.0.0.1
port = $KEEL_PROM_PORT
EOF
}

start_keel() {
    [[ -x "$KEEL_BIN" ]] || { err "KEEL binary not found at $KEEL_BIN — run cmake --build build"; exit 1; }
    log "Launching KEEL: $KEEL_BIN -c $KEEL_INI"
    "$KEEL_BIN" -c "$KEEL_INI" >"$KEEL_LOG" 2>&1 &
    KEEL_PID=$!
    sleep 2
    if ! kill -0 "$KEEL_PID" 2>/dev/null; then
        err "KEEL exited immediately. Log:"
        tail -40 "$KEEL_LOG" >&2
        exit 1
    fi
    log "KEEL up (pid=$KEEL_PID, port=$KEEL_PORT)"

    # Wait for KEEL to accept connections and probe to complete first cycle.
    for i in $(seq 1 30); do
        if mysql_through_keel "SELECT 1;" >/dev/null 2>&1; then
            log "  KEEL accepting queries after ${i}s"; return
        fi
        sleep 1
    done
    err "KEEL never accepted a query"; tail -40 "$KEEL_LOG" >&2; exit 1
}

# ---------------------------------------------------------------------------
# 3. Pre-failover sanity
# ---------------------------------------------------------------------------
schema_setup() {
    dexec "$PRIMARY_CTR" "
        CREATE DATABASE IF NOT EXISTS test;
        USE test;
        DROP TABLE IF EXISTS failover_t;
        CREATE TABLE failover_t (id INT PRIMARY KEY AUTO_INCREMENT, marker VARCHAR(64));
    "
    # let replicas catch up
    sleep 2
}

write_then_wait_gtid_catchup() {
    local marker="$1"
    test_h "Writing '$marker' through KEEL"
    local out
    out=$(mysql_through_keel "INSERT INTO failover_t (marker) VALUES ('$marker'); SELECT @@global.gtid_executed;")
    log "  Insert OK. GTID executed: $out"

    local gtid
    gtid=$(dexec "$PRIMARY_CTR" "SELECT @@global.gtid_executed;")
    log "  Primary GTID: $gtid"

    for ctr in "$REPLICA1_CTR" "$REPLICA2_CTR"; do
        for i in $(seq 1 20); do
            local applied
            applied=$(dexec "$ctr" "SELECT GTID_SUBSET('$gtid', @@global.gtid_executed);")
            if [[ "$applied" == "1" ]]; then
                test_h "  $ctr caught up to primary GTID ($i tries) ✓"; break
            fi
            sleep 1
            if [[ $i -eq 20 ]]; then
                err "$ctr never caught up to GTID $gtid"; return 1
            fi
        done
    done
}

# ---------------------------------------------------------------------------
# 4. Failover + post-failover assertions
# ---------------------------------------------------------------------------
do_failover() {
    test_h "=== Triggering failover: stop primary, promote replica1 ==="
    docker stop "$PRIMARY_CTR" >/dev/null
    log "  Primary stopped"
    sleep 2

    # Ensure replica1 has caught up before promotion (real failover orchestration
    # would do this too); the prior catch-up check guarantees this here.
    docker exec "$REPLICA1_CTR" mysql -uroot -proot -e "
        STOP REPLICA;
        RESET REPLICA ALL;
        SET GLOBAL read_only = OFF;
        SET GLOBAL super_read_only = OFF;
    " >/dev/null 2>&1
    local ro
    ro=$(dexec "$REPLICA1_CTR" "SELECT @@read_only;")
    [[ "$ro" == "0" ]] || { err "replica1 still read_only=$ro"; return 1; }
    test_h "  replica1 promoted (read_only=0) ✓"
}

wait_for_keel_to_detect() {
    test_h "Waiting for KEEL probe to detect role flip (budget=30s)..."
    for i in $(seq 1 30); do
        # An INSERT through KEEL should eventually succeed against the new
        # primary on port 3307 once probes mark replica1 as RW.
        local out
        out=$(mysql_through_keel "INSERT INTO failover_t (marker) VALUES ('probe-${i}');" 2>&1 || true)
        if [[ -z "$out" || "$out" == "Query OK"* ]]; then
            test_h "  KEEL routed write to new primary after ${i}s ✓"
            return 0
        fi
        sleep 1
    done
    err "KEEL did not recover routing within 30s. Recent log tail:"
    tail -60 "$KEEL_LOG" >&2
    return 1
}

verify_post_failover_visibility() {
    test_h "Reading 'post-failover' marker back through KEEL"
    local marker="post-failover-$(date +%s)"
    # Single session so KEEL's sticky-primary TTL pins the SELECT to the same
    # backend that received the INSERT (RYW). Otherwise the SELECT could be
    # routed to a replica that is no longer following the new primary.
    local rows
    rows=$(mysql_through_keel "
        INSERT INTO failover_t (marker) VALUES ('$marker');
        SELECT COUNT(*) FROM failover_t WHERE marker='$marker';
    " | tr -d '[:space:]')
    if [[ "$rows" == "1" ]]; then
        test_h "  Post-failover write+read visible ✓"
    else
        err "Post-failover row not visible (count=$rows)"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
main() {
    start_cluster
    write_keel_config
    start_keel
    schema_setup
    write_then_wait_gtid_catchup "pre-failover-$(date +%s)"
    do_failover
    wait_for_keel_to_detect
    verify_post_failover_visibility
    echo
    log "=== MySQL GTID failover integration test PASSED ==="
}

main "$@"
