#!/usr/bin/env bash
# ============================================================================
# Comparative benchmark: Direct vs Keel vs HAProxy vs PgBouncer
#
# Usage:
#   ./bench/bench-compare.sh [workload] [threads] [duration] [runs]
#
# Workloads: point_select, read_only, read_write  (default: point_select)
#
# The script auto-starts Keel, HAProxy, and PgBouncer if they are not already
# running, and stops them on exit.  sysbench tables (sbtest1..4) must already
# exist in the target database.
#
# Environment overrides:
#   KEEL_BIN        path to keel binary   (default: build dir auto-detect)
#   KEEL_CFG        path to keel config   (default: etc/keel-pg.ini)
#   KEEL_WORKERS    num_workers override   (default: from config)
#   SKIP_TARGETS    space-separated list of targets to skip (e.g. "haproxy pgbouncer")
# ============================================================================
set -uo pipefail   # no -e: we handle errors per-target

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

WORKLOAD="${1:-point_select}"
THREADS="${2:-16}"
DURATION="${3:-30}"
RUNS="${4:-3}"
TABLE_SIZE=100000
TABLES=4

PG_USER=usr_test
PG_PASS=qaz123
PG_DB=testdb
PG_HOST=pgsql-01

SKIP_TARGETS="${SKIP_TARGETS:-}"

# ── Keel binary detection ──────────────────────────────────────────────────
find_keel_bin() {
    if [[ -n "${KEEL_BIN:-}" && -x "$KEEL_BIN" ]]; then
        echo "$KEEL_BIN"; return
    fi
    for d in build-release build-linux build; do
        local p="$ROOT_DIR/$d/src/main/keel"
        [[ -x "$p" ]] && { echo "$p"; return; }
    done
    echo ""
}

KEEL_BIN="$(find_keel_bin)"
KEEL_CFG="${KEEL_CFG:-$ROOT_DIR/etc/keel-pg.ini}"

# ── Logging ────────────────────────────────────────────────────────────────
log()  { printf "\033[36m[bench]\033[0m %s\n" "$*"; }
warn() { printf "\033[33m[bench]\033[0m %s\n" "$*"; }
err()  { printf "\033[31m[bench]\033[0m %s\n" "$*"; }

# ── Process management ─────────────────────────────────────────────────────
STARTED_PIDS=()

cleanup() {
    log "Cleaning up..."
    for pid in "${STARTED_PIDS[@]}"; do
        kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
    done
    pkill pgbouncer 2>/dev/null || true
    rm -f /tmp/pgbouncer.pid
}
trap cleanup EXIT

port_listening() {
    ss -tlnH "sport = :$1" 2>/dev/null | grep -q .
}

wait_port() {
    local port="$1" label="$2" tries=40
    while (( tries-- > 0 )); do
        port_listening "$port" && return 0
        sleep 0.25
    done
    err "$label: port $port not listening after 10s"
    return 1
}

# ── Start services ─────────────────────────────────────────────────────────
start_keel() {
    if port_listening 7432; then log "Keel already listening on 7432"; return 0; fi
    if [[ -z "$KEEL_BIN" ]]; then err "Keel binary not found"; return 1; fi
    if [[ ! -f "$KEEL_CFG" ]]; then err "Keel config not found: $KEEL_CFG"; return 1; fi

    local cfg="$KEEL_CFG"
    # Apply worker count override if requested
    if [[ -n "${KEEL_WORKERS:-}" ]]; then
        cfg="/tmp/keel-bench-$$.ini"
        cp "$KEEL_CFG" "$cfg"
        sed -i "s/^num_workers *=.*/num_workers = $KEEL_WORKERS/" "$cfg"
        # Scale session pool down for more workers (saves memlock for io_uring)
        sed -i "s/^max_conns_per_worker *=.*/max_conns_per_worker = 1024/" "$cfg"
    fi

    log "Starting Keel: $KEEL_BIN -c $cfg"
    "$KEEL_BIN" -c "$cfg" > /tmp/keel-bench.log 2>&1 &
    STARTED_PIDS+=($!)
    wait_port 7432 "Keel"
}

start_haproxy() {
    if port_listening 7433; then log "HAProxy already listening on 7433"; return 0; fi
    local cfg="$ROOT_DIR/docker/haproxy/haproxy.cfg"
    if [[ ! -f "$cfg" ]]; then err "HAProxy config not found: $cfg"; return 1; fi

    log "Starting HAProxy"
    haproxy -f "$cfg" -D
    # HAProxy daemonises, find its pid
    local pid
    pid=$(pgrep -f "haproxy.*$cfg" | head -1) || true
    [[ -n "$pid" ]] && STARTED_PIDS+=("$pid")
    wait_port 7433 "HAProxy"
}

start_pgbouncer() {
    if port_listening 7434; then log "PgBouncer already listening on 7434"; return 0; fi
    local cfg="$ROOT_DIR/docker/pgbouncer/pgbouncer.ini"
    if [[ ! -f "$cfg" ]]; then err "PgBouncer config not found: $cfg"; return 1; fi

    # PgBouncer refuses to run as root — use the postgres user
    # Kill any leftover pgbouncer and remove stale PID file
    pkill pgbouncer 2>/dev/null || true
    sleep 0.3
    rm -f /tmp/pgbouncer.pid /tmp/pgbouncer.log
    # Do NOT pre-create the PID file — pgbouncer fails if it exists but is empty
    touch /tmp/pgbouncer.log
    chown postgres /tmp/pgbouncer.log
    log "Starting PgBouncer (as postgres user)"
    su -s /bin/sh postgres -c "pgbouncer -d $cfg" 2>/tmp/pgbouncer-bench.log || true
    wait_port 7434 "PgBouncer"
}

# ── Connectivity test ──────────────────────────────────────────────────────
pg_test() {
    local host="$1" port="$2"
    PGPASSWORD="$PG_PASS" psql -h "$host" -p "$port" -U "$PG_USER" -d "$PG_DB" \
        -c "SELECT 1" > /dev/null 2>&1
}

# ── Targets ────────────────────────────────────────────────────────────────
declare -a TARGET_NAMES=()
declare -a TARGET_HOSTS=()
declare -a TARGET_PORTS=()

add_target() { TARGET_NAMES+=("$1"); TARGET_HOSTS+=("$2"); TARGET_PORTS+=("$3"); }

should_skip() {
    for s in $SKIP_TARGETS; do [[ "$s" == "$1" ]] && return 0; done
    return 1
}

# ── Main ───────────────────────────────────────────────────────────────────
SYSBENCH_CMD="oltp_${WORKLOAD}"

# 1. Verify direct connectivity
if ! pg_test "$PG_HOST" 5432; then
    err "Cannot connect to PostgreSQL at $PG_HOST:5432 — aborting"
    exit 1
fi
add_target "direct" "$PG_HOST" "5432"

# 2. Start & verify services
if ! should_skip keel; then
    if start_keel && pg_test 127.0.0.1 7432; then
        add_target "keel" "127.0.0.1" "7432"
    else
        warn "Keel: skipping (failed to start or connect)"
    fi
fi

if ! should_skip haproxy; then
    if start_haproxy && pg_test 127.0.0.1 7433; then
        add_target "haproxy" "127.0.0.1" "7433"
    else
        warn "HAProxy: skipping (failed to start or connect)"
    fi
fi

if ! should_skip pgbouncer; then
    if start_pgbouncer && pg_test 127.0.0.1 7434; then
        add_target "pgbouncer" "127.0.0.1" "7434"
    else
        warn "PgBouncer: skipping (failed to start or connect)"
    fi
fi

NUM_TARGETS=${#TARGET_NAMES[@]}
if (( NUM_TARGETS < 2 )); then
    err "Need at least 2 targets (have $NUM_TARGETS: ${TARGET_NAMES[*]})"
    exit 1
fi

echo ""
echo "============================================================================"
echo " Benchmark: ${SYSBENCH_CMD}  threads=${THREADS}  duration=${DURATION}s  runs=${RUNS}"
echo " Targets:   ${TARGET_NAMES[*]}"
echo "============================================================================"
echo ""

# column widths
COL_W=$(( 10 * RUNS + 30 + 12 ))
printf "%-12s " "target"
for r in $(seq 1 "$RUNS"); do printf "%10s " "run_${r}"; done
printf "%10s %10s %10s\n" "avg_tps" "p95_ms" "overhead%"
printf '%0.s─' $(seq 1 "$COL_W"); echo ""

declare -A AVG_TPS
declare -A AVG_P95
DIRECT_AVG=0

for i in $(seq 0 $(( NUM_TARGETS - 1 ))); do
    name="${TARGET_NAMES[$i]}"
    host="${TARGET_HOSTS[$i]}"
    port="${TARGET_PORTS[$i]}"

    # Warmup (3s, errors swallowed)
    sysbench "$SYSBENCH_CMD" \
        --db-driver=pgsql \
        --pgsql-host="$host" --pgsql-port="$port" \
        --pgsql-user="$PG_USER" --pgsql-password="$PG_PASS" \
        --pgsql-db="$PG_DB" \
        --table-size="$TABLE_SIZE" --tables="$TABLES" \
        --threads="$THREADS" --time=5 --db-ps-mode=disable \
        run > /dev/null 2>&1 || true

    total_tps=0
    total_p95=0
    last_p95=""
    run_ok=0
    printf "%-12s " "$name"

    for r in $(seq 1 "$RUNS"); do
        out=$(sysbench "$SYSBENCH_CMD" \
            --db-driver=pgsql \
            --pgsql-host="$host" --pgsql-port="$port" \
            --pgsql-user="$PG_USER" --pgsql-password="$PG_PASS" \
            --pgsql-db="$PG_DB" \
            --table-size="$TABLE_SIZE" --tables="$TABLES" \
            --threads="$THREADS" --time="$DURATION" --db-ps-mode=disable \
            run 2>&1) || true

        tps=$(echo "$out" | grep -i "transactions:" | awk -F'[( ]+' '{print $3}')
        p95=$(echo "$out" | grep -i "95th percentile:" | awk '{print $NF}')

        if [[ -z "$tps" || "$tps" == "0" ]]; then
            printf "%10s " "FAIL"
        else
            printf "%10.0f " "$tps"
            total_tps=$(echo "$total_tps + $tps" | bc)
            total_p95=$(echo "$total_p95 + ${p95:-0}" | bc)
            last_p95="$p95"
            (( run_ok++ )) || true
        fi
    done

    if (( run_ok == 0 )); then
        printf "%10s %10s %10s\n" "FAIL" "-" "-"
        continue
    fi

    avg=$(echo "scale=0; $total_tps / $run_ok" | bc)
    avg_p95=$(echo "scale=2; $total_p95 / $run_ok" | bc)
    AVG_TPS[$name]=$avg
    AVG_P95[$name]=$avg_p95

    if [[ "$name" == "direct" ]]; then
        DIRECT_AVG=$avg
        printf "%10s %10s %10s\n" "$avg" "$avg_p95" "baseline"
    else
        if (( DIRECT_AVG > 0 )); then
            overhead=$(echo "scale=1; (1 - $avg / $DIRECT_AVG) * 100" | bc)
        else
            overhead="N/A"
        fi
        printf "%10s %10s %9s%%\n" "$avg" "$avg_p95" "$overhead"
    fi
done

echo ""
echo "============================================================================"
echo " Summary  (${SYSBENCH_CMD}, ${THREADS} threads, ${DURATION}s)"
echo "============================================================================"

for i in $(seq 0 $(( NUM_TARGETS - 1 ))); do
    name="${TARGET_NAMES[$i]}"
    tps="${AVG_TPS[$name]:-FAIL}"
    p95="${AVG_P95[$name]:-—}"
    if [[ "$name" == "direct" ]]; then
        printf " %-12s %8s TPS  p95=%s ms  (baseline)\n" "$name" "$tps" "$p95"
    elif [[ "$tps" != "FAIL" && "$DIRECT_AVG" -gt 0 ]]; then
        overhead=$(echo "scale=1; (1 - $tps / $DIRECT_AVG) * 100" | bc)
        printf " %-12s %8s TPS  p95=%s ms  (overhead: %s%%)\n" "$name" "$tps" "$p95" "$overhead"
    else
        printf " %-12s %8s TPS\n" "$name" "$tps"
    fi
done

echo "============================================================================"
