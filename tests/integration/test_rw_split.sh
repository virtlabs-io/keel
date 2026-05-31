#!/usr/bin/env bash
# ============================================================================
# test_rw_split.sh — Verify read/write split routing through KEEL proxy
# ============================================================================
#
# VALIDATION METHOD: PostgreSQL's own pg_stat_database counters.
#
# Proxy logs can miss queries that go through the pool wait queue (which is
# the majority under load). Instead we take before/after snapshots of
# xact_commit on every backend and measure the deltas. This is authoritative.
#
# Three phases:
#   1. SMOKE TEST — send 50 bare SELECTs via psql (no pgbench, no ambiguity).
#      Verify that replicas received transactions and primary did not.
#   2. LOAD TEST  — pgbench readers + writers in parallel.
#   3. VERDICT    — compare pg_stat_database deltas across backends.
#
# Usage:
#   ./scripts/test_rw_split.sh                         # defaults: 750R + 250W
#   ./scripts/test_rw_split.sh --clients 3000          # 2250R + 750W
#   ./scripts/test_rw_split.sh --duration 30           # longer run
#   ./scripts/test_rw_split.sh --select-only           # readers only
#   ./scripts/test_rw_split.sh --smoke-only            # smoke test only, no load
# ============================================================================
set -euo pipefail

# ---- Configuration (override via env or flags) ----
PROXY_HOST="${PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${PROXY_PORT:-7432}"
DB_USER="${DB_USER:-postgres}"
DB_PASS="${DB_PASS:-postgres}"
DB_NAME="${DB_NAME:-testdb}"
DURATION="${DURATION:-10}"
READ_CLIENTS="${READ_CLIENTS:-750}"
WRITE_CLIENTS="${WRITE_CLIENTS:-250}"
SELECT_ONLY=false
WRITE_ONLY=false
SMOKE_ONLY=false
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/bench"
TMPDIR_BASE=$(mktemp -d /tmp/keel_rw_test_XXXXXX)

# Raise FD limit for high connection counts
ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

# Backend hosts/port (must match the Docker network service names)
PRIMARY_HOST="${PRIMARY_HOST:-pgsql-01}"
REPLICA1_HOST="${REPLICA1_HOST:-pg-replica1}"
REPLICA2_HOST="${REPLICA2_HOST:-pg-replica2}"
BACKEND_PORT="${BACKEND_PORT:-5432}"
BACKEND_USER="${BACKEND_USER:-usr_test}"
BACKEND_PASS="${BACKEND_PASS:-qaz123}"
ALL_HOSTS="$PRIMARY_HOST $REPLICA1_HOST $REPLICA2_HOST"
REPLICA_HOSTS="$REPLICA1_HOST $REPLICA2_HOST"

# Auto-stack management: set MANAGE_STACK=0 to require a pre-running proxy
MANAGE_STACK="${MANAGE_STACK:-1}"
E2E_COMPOSE_FILE="${E2E_COMPOSE_FILE:-$ROOT_DIR/docker/compose/pg-e2e.yml}"
# Prefer build-test binary (ASAN/debug), fall back to release build
KEEL_BIN="${KEEL_BIN:-$ROOT_DIR/build-test/src/main/keel}"
[[ -x "$KEEL_BIN" ]] || KEEL_BIN="$ROOT_DIR/build/src/main/keel"

_STACK_STARTED_BY_US=0
_MANAGED_KEEL_PID=""
_MANAGED_KEEL_CONF=""

_rw_cleanup() {
    rm -rf "$TMPDIR_BASE" "$_MANAGED_KEEL_CONF"
    if [[ -n "$_MANAGED_KEEL_PID" ]]; then
        kill "$_MANAGED_KEEL_PID" 2>/dev/null || true
        wait "$_MANAGED_KEEL_PID" 2>/dev/null || true
    fi
    if [[ "$_STACK_STARTED_BY_US" == "1" ]]; then
        docker compose -f "$E2E_COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    fi
}
trap _rw_cleanup EXIT

# Start the pg-e2e backend containers and a host-side keel with query routing.
# Sets PRIMARY_HOST/REPLICA1_HOST/REPLICA2_HOST to Docker container IPs
# (bridge IPs are directly routable from the host), and starts keel on
# PROXY_PORT with auth_method=trust so the test can connect without passwords.
start_stack_and_keel() {
    if [[ ! -f "$E2E_COMPOSE_FILE" ]]; then
        warn "Compose file not found: $E2E_COMPOSE_FILE"
        return 1
    fi
    if [[ ! -x "$KEEL_BIN" ]]; then
        warn "keel binary not found at $KEEL_BIN (build the project first)"
        return 1
    fi

    warn "Proxy not reachable — auto-starting e2e backends and keel"
    # Start only the three PG services (not the keel container from the e2e stack,
    # which has query_routing=off; we start our own keel on the host instead).
    docker compose -f "$E2E_COMPOSE_FILE" up -d pgsql-01 pgsql-02 pgsql-03 --wait 2>&1 | tail -5
    _STACK_STARTED_BY_US=1

    # Docker bridge IPs are routable from the host
    PRIMARY_HOST=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e2e-pgsql-01)
    REPLICA1_HOST=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e2e-pgsql-02)
    REPLICA2_HOST=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e2e-pgsql-03)
    BACKEND_USER=postgres
    BACKEND_PASS=postgres
    ALL_HOSTS="$PRIMARY_HOST $REPLICA1_HOST $REPLICA2_HOST"
    REPLICA_HOSTS="$REPLICA1_HOST $REPLICA2_HOST"

    _MANAGED_KEEL_CONF=$(mktemp /tmp/keel-rw-XXXXXX.ini)
    cat > "$_MANAGED_KEEL_CONF" <<EOINI
[keel]
log_level = 0
[worker_group.rw]
protocol = postgres
bind_addr = 127.0.0.1
bind_port = ${PROXY_PORT}
num_workers = 2
min_pool_size = 2
max_pool_size = 50
auth_method = trust
server_user = postgres
server_password = postgres
connect_timeout_ms = 5000
[worker_group.rw.servers]
pgsql-01 = host=${PRIMARY_HOST} port=5432 dbname=${DB_NAME} role=auto weight=100
pgsql-02 = host=${REPLICA1_HOST} port=5432 dbname=${DB_NAME} role=auto weight=100
pgsql-03 = host=${REPLICA2_HOST} port=5432 dbname=${DB_NAME} role=auto weight=100
[security]
privilege_drop = false
require_privilege_drop = false
seccomp = off
require_seccomp = false
no_new_privs = false
EOINI

    "$KEEL_BIN" -c "$_MANAGED_KEEL_CONF" >/dev/null 2>&1 &
    _MANAGED_KEEL_PID=$!

    # Wait up to 20s for keel to accept connections
    local i
    for i in $(seq 1 20); do
        PGPASSWORD="$DB_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" \
            -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 1
    done
    warn "keel did not become ready after 20s"
    return 1
}

# ---- Parse arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)        DURATION="$2"; shift 2 ;;
        --read-clients)    READ_CLIENTS="$2"; shift 2 ;;
        --write-clients)   WRITE_CLIENTS="$2"; shift 2 ;;
        --clients)
            total="$2"
            READ_CLIENTS=$(( total * 3 / 4 ))
            WRITE_CLIENTS=$(( total - READ_CLIENTS ))
            shift 2 ;;
        --select-only)     SELECT_ONLY=true; shift ;;
        --write-only)      WRITE_ONLY=true; shift ;;
        --smoke-only)      SMOKE_ONLY=true; shift ;;
        --help|-h)
            sed -n '2,/^# ====/{ /^#/s/^# \?//p }' "$0"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---- Colors & helpers ----
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

info()  { echo -e "${CYAN}▸${NC} $*"; }
ok()    { echo -e "${GREEN}✓${NC} $*"; }
warn()  { echo -e "${YELLOW}⚠${NC} $*"; }
fail()  { echo -e "${RED}✗${NC} $*"; }
header(){ echo -e "\n${BOLD}═══ $* ═══${NC}"; }
dim()   { echo -e "${DIM}  $*${NC}"; }

# Query a specific backend directly (bypasses the proxy)
pg_direct() {
    local host="$1"; shift
    PGPASSWORD="$BACKEND_PASS" psql -h "$host" -p "$BACKEND_PORT" \
        -U "$BACKEND_USER" -d "$DB_NAME" -AXtc "$*" 2>/dev/null | grep -v '^Pager'
}

# Query through the proxy
pg_proxy() {
    PGPASSWORD="$DB_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" \
        -U "$DB_USER" -d "$DB_NAME" -AXtc "$*" 2>/dev/null | grep -v '^Pager'
}

# Get xact_commit for a backend
get_xact_commit() {
    pg_direct "$1" "SELECT xact_commit FROM pg_stat_database WHERE datname = '$DB_NAME'"
}

# ============================================================================
# PREFLIGHT
# ============================================================================
header "Preflight"

# Find pgbench
PGBENCH=""
if [[ -x /app/bin/pgbench ]]; then
    PGBENCH=/app/bin/pgbench
elif command -v pgbench &>/dev/null; then
    PGBENCH="$(command -v pgbench)"
else
    for p in /usr/lib/postgresql/*/bin/pgbench /usr/pgsql-*/bin/pgbench; do
        if [[ -x "$p" ]]; then PGBENCH="$p"; break; fi
    done
fi
if [[ -n "$PGBENCH" ]]; then
    ok "pgbench: $PGBENCH"
else
    if ! $SMOKE_ONLY; then
        fail "pgbench not found (required for load test, use --smoke-only for smoke test)"
        exit 1
    fi
    warn "pgbench not found — smoke test only"
    SMOKE_ONLY=true
fi

# Check proxy is reachable; auto-start stack+keel if MANAGE_STACK=1
if ! pg_proxy "SELECT 1" &>/dev/null; then
    if [[ "$MANAGE_STACK" == "1" ]]; then
        start_stack_and_keel || { warn "Cannot start stack/keel — skipping test"; exit 77; }
    fi
fi
if ! pg_proxy "SELECT 1" &>/dev/null; then
    warn "Cannot connect to proxy at $PROXY_HOST:$PROXY_PORT"
    echo "  Start KEEL: ./build/src/main/keel -c etc/keel-pg-test.ini"
    echo "  Skipping test (proxy not available)"
    exit 77
fi
ok "Proxy reachable at $PROXY_HOST:$PROXY_PORT"

# If PRIMARY_HOST is still the default service name (not an IP) and we're
# managing the stack, try to resolve container IPs via docker inspect so the
# stats-validation backends are reachable from the host.
if [[ "$MANAGE_STACK" == "1" && "$PRIMARY_HOST" == "pgsql-01" ]]; then
    _p1=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e2e-pgsql-01 2>/dev/null) || true
    _p2=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e2e-pgsql-02 2>/dev/null) || true
    _p3=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e2e-pgsql-03 2>/dev/null) || true
    if [[ -n "$_p1" && -n "$_p2" && -n "$_p3" ]]; then
        PRIMARY_HOST="$_p1"
        REPLICA1_HOST="$_p2"
        REPLICA2_HOST="$_p3"
        BACKEND_USER=postgres
        BACKEND_PASS=postgres
        ALL_HOSTS="$PRIMARY_HOST $REPLICA1_HOST $REPLICA2_HOST"
        REPLICA_HOSTS="$REPLICA1_HOST $REPLICA2_HOST"
        warn "Resolved backend IPs from Docker: primary=$PRIMARY_HOST replicas=$REPLICA1_HOST $REPLICA2_HOST"
    fi
fi

# Check each backend is reachable directly
for host in $ALL_HOSTS; do
    if pg_direct "$host" "SELECT 1" &>/dev/null; then
        role=$(pg_direct "$host" "SELECT CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END")
        ok "Backend ${host} reachable (${role})"
    else
        fail "Cannot connect directly to backend ${host}"
        echo "  Stats-based validation requires direct access to each backend."
        exit 77
    fi
done

# Check pgbench tables
if ! pg_proxy "SELECT count(*) FROM pgbench_accounts" &>/dev/null; then
    if [[ -n "$PGBENCH" ]]; then
        warn "pgbench tables not found — initializing (scale=10)..."
        PGPASSWORD="$DB_PASS" "$PGBENCH" -h "$PROXY_HOST" -p "$PROXY_PORT" \
            -U "$DB_USER" -d "$DB_NAME" -i -s 10 2>&1 | tail -3
    else
        fail "pgbench tables not found and no pgbench to initialize them"
        exit 1
    fi
fi
SCALE=$(pg_proxy "SELECT count(*)::int / 100000 FROM pgbench_accounts" || echo "10")
ok "pgbench tables ready (scale=${SCALE})"

# Check custom scripts
if [[ -n "$PGBENCH" ]]; then
    for f in pgbench_read_rw_split.sql pgbench_write.sql; do
        if [[ ! -f "$BENCH_DIR/$f" ]]; then
            fail "Missing script: $BENCH_DIR/$f"
            exit 1
        fi
    done
    ok "pgbench scripts found"
fi

# ============================================================================
# PHASE 1: SMOKE TEST
# ============================================================================
# Send bare SELECTs through the proxy (one at a time via psql, no pgbench,
# no ambiguity about BEGIN/COMMIT wrapping). If replicas receive txns and
# primary doesn't, routing is definitively working.
# ============================================================================
header "Phase 1: Smoke Test (bare SELECTs via psql)"

SMOKE_COUNT=50

# Snapshot xact_commit on all backends BEFORE
declare -A SMOKE_BEFORE
for host in $ALL_HOSTS; do
    SMOKE_BEFORE[$host]=$(get_xact_commit "$host")
done
dim "Snapshots: primary=${SMOKE_BEFORE[$PRIMARY_HOST]}, replicas=$(for h in $REPLICA_HOSTS; do echo -n "$h=${SMOKE_BEFORE[$h]} "; done)"

# Send bare SELECTs through the proxy — one per psql invocation to ensure
# each is a completely independent autocommit query.
info "Sending ${SMOKE_COUNT} bare SELECTs through proxy..."
for i in $(seq 1 $SMOKE_COUNT); do
    pg_proxy "SELECT abalance FROM pgbench_accounts WHERE aid = $((RANDOM % 1000 + 1))" >/dev/null
done
ok "Sent ${SMOKE_COUNT} SELECTs"

# pg_stat_database may take a moment to flush cumulative stats.
# Wait long enough for the stats collector to update.
sleep 2

# Snapshot AFTER
declare -A SMOKE_AFTER
for host in $ALL_HOSTS; do
    SMOKE_AFTER[$host]=$(get_xact_commit "$host")
done

# Compute deltas
declare -A SMOKE_DELTA
SMOKE_REPLICA_TOTAL=0
for host in $ALL_HOSTS; do
    SMOKE_DELTA[$host]=$(( ${SMOKE_AFTER[$host]} - ${SMOKE_BEFORE[$host]} ))
done
for host in $REPLICA_HOSTS; do
    SMOKE_REPLICA_TOTAL=$(( SMOKE_REPLICA_TOTAL + ${SMOKE_DELTA[$host]} ))
done

echo ""
printf "  ${BOLD}%-18s %10s${NC}\n" "Backend" "Δ xact_commit"
printf "  %-18s %10s\n"             "──────────────" "─────────────"
printf "  %-18s %10d\n"             "Primary ($PRIMARY_HOST)" "${SMOKE_DELTA[$PRIMARY_HOST]}"
for host in $REPLICA_HOSTS; do
    printf "  %-18s %10d\n"         "Replica ($host)" "${SMOKE_DELTA[$host]}"
done
printf "  %-18s %10s\n"             "──────────────" "─────────────"
printf "  ${BOLD}%-18s %10d${NC}\n" "Replicas total" "$SMOKE_REPLICA_TOTAL"
echo ""

# Verdict
SMOKE_PASS=true
# Threshold is 60% because:
#  - pg_stat_database has flush lag (some txns counted before/after our snapshot)
#  - Health probes add a few txns to primary
#  - The 50 SELECTs are sent sequentially via psql so timing jitter matters
if [[ $SMOKE_REPLICA_TOTAL -ge $(( SMOKE_COUNT * 60 / 100 )) ]]; then
    ok "Smoke test PASSED: ${SMOKE_REPLICA_TOTAL}/${SMOKE_COUNT} SELECTs routed to replicas"
elif [[ $SMOKE_REPLICA_TOTAL -gt 0 ]]; then
    warn "Partial routing: ${SMOKE_REPLICA_TOTAL}/${SMOKE_COUNT} SELECTs on replicas (expected ≥$((SMOKE_COUNT*60/100)))"
    SMOKE_PASS=false
else
    fail "Smoke test FAILED: 0 SELECTs went to replicas — all ${SMOKE_DELTA[$PRIMARY_HOST]} on primary"
    SMOKE_PASS=false
fi

# Check primary delta
if [[ ${SMOKE_DELTA[$PRIMARY_HOST]} -le 5 ]]; then
    ok "Primary received ≤5 transactions (health-check noise only)"
elif [[ ${SMOKE_DELTA[$PRIMARY_HOST]} -ge $(( SMOKE_COUNT / 2 )) ]]; then
    fail "Primary received ${SMOKE_DELTA[$PRIMARY_HOST]} txns — SELECTs are NOT being split"
    SMOKE_PASS=false
else
    warn "Primary received ${SMOKE_DELTA[$PRIMARY_HOST]} txns (some noise expected)"
fi

# Replica balance
if [[ $SMOKE_REPLICA_TOTAL -gt 0 ]]; then
    info "Replica distribution:"
    for host in $REPLICA_HOSTS; do
        pct=$(( ${SMOKE_DELTA[$host]} * 100 / SMOKE_REPLICA_TOTAL ))
        info "  ${host} → ${SMOKE_DELTA[$host]} txns (${pct}%)"
    done
fi

if $SMOKE_ONLY; then
    echo ""
    if $SMOKE_PASS; then
        echo -e "${GREEN}${BOLD}  ✓ READ/WRITE SPLIT IS WORKING${NC}"
    else
        echo -e "${RED}${BOLD}  ✗ READ/WRITE SPLIT NEEDS ATTENTION${NC}"
    fi
    echo ""
    exit $( $SMOKE_PASS && echo 0 || echo 1 )
fi

# ============================================================================
# PHASE 2: LOAD TEST (pgbench)
# ============================================================================
header "Phase 2: Load Test (pgbench)"

RUN_READERS=true
RUN_WRITERS=true
if $SELECT_ONLY; then RUN_WRITERS=false; fi
if $WRITE_ONLY;  then RUN_READERS=false; fi

# Auto-scale pgbench threads (≤500 connections per thread)
MAX_CONNS_PER_THREAD=500
calc_threads() {
    local clients=$1
    local t=$(( (clients + MAX_CONNS_PER_THREAD - 1) / MAX_CONNS_PER_THREAD ))
    [[ $t -lt 1 ]] && t=1
    while (( t > 1 && clients % t != 0 )); do t=$((t - 1)); done
    echo "$t"
}

r_threads=$(calc_threads "$READ_CLIENTS")
w_threads=$(calc_threads "$WRITE_CLIENTS")

if $RUN_READERS && $RUN_WRITERS; then
    info "Mode: Parallel — ${READ_CLIENTS} readers + ${WRITE_CLIENTS} writers"
    info "Readers: ${r_threads} threads ($(( READ_CLIENTS / r_threads ))/thread)  |  Writers: ${w_threads} threads ($(( WRITE_CLIENTS / w_threads ))/thread)"
elif $RUN_READERS; then
    info "Mode: SELECT-only — ${READ_CLIENTS} clients → should all go to replicas"
else
    info "Mode: WRITE-only — ${WRITE_CLIENTS} clients → should all go to primary"
fi
echo ""

# ---- Snapshot BEFORE ----
declare -A LOAD_BEFORE
for host in $ALL_HOSTS; do
    LOAD_BEFORE[$host]=$(get_xact_commit "$host")
done

reader_out="$TMPDIR_BASE/readers.out"
writer_out="$TMPDIR_BASE/writers.out"
reader_pid="" writer_pid=""

# Launch readers
if $RUN_READERS; then
    info "Starting ${READ_CLIENTS} reader clients..."
    PGPASSWORD="$DB_PASS" "$PGBENCH" \
        -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$DB_USER" -d "$DB_NAME" \
        -n -c "$READ_CLIENTS" -j "$r_threads" -T "$DURATION" -M simple \
        -f "${BENCH_DIR}/pgbench_read_rw_split.sql" \
        > "$reader_out" 2>&1 &
    reader_pid=$!
fi

# Launch writers
if $RUN_WRITERS; then
    info "Starting ${WRITE_CLIENTS} writer clients..."
    PGPASSWORD="$DB_PASS" "$PGBENCH" \
        -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$DB_USER" -d "$DB_NAME" \
        -n -c "$WRITE_CLIENTS" -j "$w_threads" -T "$DURATION" -M simple \
        -f "${BENCH_DIR}/pgbench_write.sql" \
        > "$writer_out" 2>&1 &
    writer_pid=$!
fi

# Wait with progress
elapsed=0
while true; do
    still_running=false
    [[ -n "$reader_pid" ]] && kill -0 "$reader_pid" 2>/dev/null && still_running=true
    [[ -n "$writer_pid" ]] && kill -0 "$writer_pid" 2>/dev/null && still_running=true
    $still_running || break
    sleep 1
    elapsed=$((elapsed + 1))
    printf "\r  ⏱  Running... %ds / %ds" "$elapsed" "$DURATION"
done
printf "\r  ⏱  Finished in %ds              \n" "$elapsed"

# Collect exit codes
reader_rc=0 writer_rc=0
[[ -n "$reader_pid" ]] && { wait "$reader_pid" 2>/dev/null || reader_rc=$?; }
[[ -n "$writer_pid" ]] && { wait "$writer_pid" 2>/dev/null || writer_rc=$?; }

sleep 1

# ---- Parse pgbench output ----
r_tps="0" r_lat="0" r_txn="0"
w_tps="0" w_lat="0" w_txn="0"

if $RUN_READERS && [[ -f "$reader_out" ]]; then
    r_tps=$(grep -m1 '^tps' "$reader_out" | awk '{print $3}' || echo "0")
    r_lat=$(grep -m1 '^latency average' "$reader_out" | awk '{print $4}' || echo "0")
    r_txn=$(grep -m1 '^number of transactions actually processed' "$reader_out" | awk '{print $NF}' || echo "0")
    [[ -z "$r_tps" ]] && r_tps="0"
    [[ -z "$r_lat" ]] && r_lat="0"
    [[ -z "$r_txn" ]] && r_txn="0"
    if grep -q 'FATAL\|could not' "$reader_out" 2>/dev/null; then
        fail "Reader pgbench had errors:"
        grep 'FATAL\|could not' "$reader_out" | head -3 | sed 's/^/    /'
    fi
fi

if $RUN_WRITERS && [[ -f "$writer_out" ]]; then
    w_tps=$(grep -m1 '^tps' "$writer_out" | awk '{print $3}' || echo "0")
    w_lat=$(grep -m1 '^latency average' "$writer_out" | awk '{print $4}' || echo "0")
    w_txn=$(grep -m1 '^number of transactions actually processed' "$writer_out" | awk '{print $NF}' || echo "0")
    [[ -z "$w_tps" ]] && w_tps="0"
    [[ -z "$w_lat" ]] && w_lat="0"
    [[ -z "$w_txn" ]] && w_txn="0"
    if grep -q 'FATAL\|could not' "$writer_out" 2>/dev/null; then
        fail "Writer pgbench had errors:"
        grep 'FATAL\|could not' "$writer_out" | head -3 | sed 's/^/    /'
    fi
fi

combined_tps=$(LC_ALL=C awk "BEGIN {printf \"%.1f\", ${r_tps} + ${w_tps}}")

echo ""
printf "  ${BOLD}%-14s %12s %12s %14s${NC}\n" "Workload" "TPS" "Avg Latency" "Transactions"
printf "  %-14s %12s %12s %14s\n"             "──────────" "─────────" "───────────" "────────────"
$RUN_READERS && printf "  %-14s %12s %10s ms %14s\n" "Readers" "$r_tps" "$r_lat" "$r_txn"
$RUN_WRITERS && printf "  %-14s %12s %10s ms %14s\n" "Writers" "$w_tps" "$w_lat" "$w_txn"
printf "  %-14s %12s %12s %14s\n"             "──────────" "─────────" "───────────" "────────────"
printf "  ${BOLD}%-14s %12s${NC}\n"           "Combined" "$combined_tps"
echo ""

# ============================================================================
# PHASE 3: STATS-BASED ROUTING ANALYSIS
# ============================================================================
header "Phase 3: Routing Validation (pg_stat_database)"

info "Source: xact_commit deltas from each PostgreSQL backend"
info "This measures actual transactions — not sampled, not lossy."
echo ""

# ---- Snapshot AFTER ----
declare -A LOAD_AFTER LOAD_DELTA
LOAD_REPLICA_TOTAL=0
for host in $ALL_HOSTS; do
    LOAD_AFTER[$host]=$(get_xact_commit "$host")
    LOAD_DELTA[$host]=$(( ${LOAD_AFTER[$host]} - ${LOAD_BEFORE[$host]} ))
done
for host in $REPLICA_HOSTS; do
    LOAD_REPLICA_TOTAL=$(( LOAD_REPLICA_TOTAL + ${LOAD_DELTA[$host]} ))
done
LOAD_PRIMARY_DELTA=${LOAD_DELTA[$PRIMARY_HOST]}
LOAD_TOTAL=$(( LOAD_PRIMARY_DELTA + LOAD_REPLICA_TOTAL ))

printf "  ${BOLD}%-22s %12s %8s${NC}\n" "Backend" "Δ xact_commit" "Share"
printf "  %-22s %12s %8s\n"             "────────────────" "─────────────" "────────"
if [[ $LOAD_TOTAL -gt 0 ]]; then
    pct=$(( LOAD_PRIMARY_DELTA * 100 / LOAD_TOTAL ))
else
    pct=0
fi
printf "  %-22s %12d %7d%%\n" "Primary ($PRIMARY_HOST)" "$LOAD_PRIMARY_DELTA" "$pct"

for host in $REPLICA_HOSTS; do
    if [[ $LOAD_TOTAL -gt 0 ]]; then
        pct=$(( ${LOAD_DELTA[$host]} * 100 / LOAD_TOTAL ))
    else
        pct=0
    fi
    printf "  %-22s %12d %7d%%\n" "Replica ($host)" "${LOAD_DELTA[$host]}" "$pct"
done

printf "  %-22s %12s %8s\n"             "────────────────" "─────────────" "────────"
printf "  ${BOLD}%-22s %12d %7d%%${NC}\n" "Replicas (sum)" "$LOAD_REPLICA_TOTAL" \
    "$(( LOAD_TOTAL > 0 ? LOAD_REPLICA_TOTAL * 100 / LOAD_TOTAL : 0 ))"
printf "  ${BOLD}%-22s %12d${NC}\n" "TOTAL" "$LOAD_TOTAL"
echo ""

# Cross-reference with pgbench output
r_txn_i=${r_txn:-0}
w_txn_i=${w_txn:-0}

if [[ $r_txn_i -gt 0 ]] || [[ $w_txn_i -gt 0 ]]; then
    dim "pgbench reported: ${r_txn_i} reader txns, ${w_txn_i} writer txns"
    dim "Expected on replicas: ~${r_txn_i} txns from readers"
    dim "Expected on primary:  ~${w_txn_i} txns from writers (+ in-txn SELECTs stay on primary)"
    dim "(Small variances from health probes & pool keepalive are normal)"
    echo ""
fi

# ============================================================================
# VERDICT
# ============================================================================
header "Verdict"

PASS=true

# Check 1: Replicas received significant read traffic
if $RUN_READERS; then
    if [[ $LOAD_REPLICA_TOTAL -gt 0 ]]; then
        if [[ $r_txn_i -gt 0 ]]; then
            coverage=$(( LOAD_REPLICA_TOTAL * 100 / r_txn_i ))
            if [[ $coverage -ge 70 ]]; then
                ok "Replicas handled ${LOAD_REPLICA_TOTAL} transactions (~${coverage}% of reader load)"
            else
                warn "Replicas got ${LOAD_REPLICA_TOTAL} txns but expected ~${r_txn_i} (${coverage}%)"
                warn "Some reads may be going to primary — check pool_mode and transaction wrapping"
            fi
        else
            ok "Replicas handled ${LOAD_REPLICA_TOTAL} transactions"
        fi
    else
        fail "REPLICAS GOT 0 TRANSACTIONS — read/write split is NOT working"
        PASS=false
    fi
fi

# Check 2: Primary got write traffic
if $RUN_WRITERS; then
    if [[ $LOAD_PRIMARY_DELTA -gt 0 ]]; then
        ok "Primary handled ${LOAD_PRIMARY_DELTA} transactions"
    else
        fail "Primary got 0 transactions — writers are not working"
        PASS=false
    fi
fi

# Check 3: Select-only mode — primary should have minimal traffic
if $SELECT_ONLY; then
    if [[ $LOAD_PRIMARY_DELTA -le 10 ]]; then
        ok "Primary received ≤10 txns in select-only mode (probe noise only)"
    else
        warn "Primary received ${LOAD_PRIMARY_DELTA} txns in select-only mode"
        warn "Expected ~0 — some SELECTs may be routing to primary"
    fi
fi

# Check 4: Replica balance
if [[ $LOAD_REPLICA_TOTAL -gt 0 ]]; then
    info "Replica load distribution:"
    max_skew=0
    replica_count=0
    for host in $REPLICA_HOSTS; do
        replica_count=$((replica_count + 1))
    done
    expected_per_replica=$(( LOAD_REPLICA_TOTAL / replica_count ))
    for host in $REPLICA_HOSTS; do
        d=${LOAD_DELTA[$host]}
        if [[ $LOAD_REPLICA_TOTAL -gt 0 ]]; then
            pct=$(( d * 100 / LOAD_REPLICA_TOTAL ))
        else
            pct=0
        fi
        info "  ${host} → ${d} txns (${pct}%)"
        if [[ $expected_per_replica -gt 0 ]]; then
            skew=$(( (d - expected_per_replica) * 100 / expected_per_replica ))
            skew=${skew#-}  # abs
            [[ $skew -gt $max_skew ]] && max_skew=$skew
        fi
    done
    if [[ $max_skew -le 30 ]]; then
        ok "Replica balance within ±30% skew"
    else
        warn "Replica skew: ±${max_skew}% (>30% may indicate uneven distribution)"
    fi
fi

# Check 5: Cross-check pgbench reader txns vs replica delta
if $RUN_READERS && [[ $r_txn_i -gt 100 ]] && [[ $LOAD_REPLICA_TOTAL -gt 0 ]]; then
    ratio=$(( LOAD_REPLICA_TOTAL * 100 / r_txn_i ))
    if [[ $ratio -ge 80 ]]; then
        ok "Cross-check: ${ratio}% of pgbench reader transactions confirmed on replicas"
    elif [[ $ratio -ge 50 ]]; then
        warn "Cross-check: only ${ratio}% of reader txns on replicas (expected ≥80%)"
    else
        fail "Cross-check: only ${ratio}% of reader txns on replicas"
        fail "This indicates most reads are going to primary instead of replicas"
        PASS=false
    fi
fi

echo ""
if $PASS && $SMOKE_PASS; then
    echo -e "${GREEN}${BOLD}  ✓ READ/WRITE SPLIT IS WORKING${NC}"
    echo ""
    echo -e "  ${DIM}Validated via PostgreSQL pg_stat_database counters (authoritative).${NC}"
    echo -e "  ${DIM}Smoke: ${SMOKE_REPLICA_TOTAL}/${SMOKE_COUNT} bare SELECTs → replicas${NC}"
    if [[ $LOAD_REPLICA_TOTAL -gt 0 ]]; then
        echo -e "  ${DIM}Load:  ${LOAD_REPLICA_TOTAL} reader txns → replicas, ${LOAD_PRIMARY_DELTA} writer txns → primary${NC}"
    fi
else
    echo -e "${RED}${BOLD}  ✗ READ/WRITE SPLIT NEEDS ATTENTION${NC}"
fi
echo ""
exit $( $PASS && $SMOKE_PASS && echo 0 || echo 1 )
