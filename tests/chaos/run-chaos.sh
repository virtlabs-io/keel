#!/usr/bin/env bash
# =============================================================================
# tests/chaos/run-chaos.sh — Chaos Test Orchestrator
# =============================================================================
#
# Runs all chaos scenarios in sequence against a live keel + PostgreSQL stack.
# The stack must already be running:
#
#   docker compose -f docker/compose/pg-chaos.yml up -d --wait
#
# Usage:
#   tests/chaos/run-chaos.sh [SCENARIO...]
#
# Examples:
#   tests/chaos/run-chaos.sh                      # run all scenarios
#   tests/chaos/run-chaos.sh kill-backend          # run one scenario
#   tests/chaos/run-chaos.sh kill-backend partition # run two scenarios
#
# Exit code:
#   0 — all selected scenarios passed
#   1 — one or more scenarios failed
#
# Environment:
#   KEEL_HOST         keel proxy host (default: 127.0.0.1)
#   KEEL_PORT         keel proxy port (default: 17432)
#   KEEL_ADMIN_PORT   keel admin port (default: 17433)
#   CHAOS_DB          test database name (default: chaosdb)
#   CHAOS_USER        database user (default: postgres)
#   CHAOS_PASS        database password (default: postgres)
#   CHAOS_TIMEOUT     per-scenario timeout in seconds (default: 120)
#   SKIP_CLEANUP      if set to "1", leave injected faults in place on failure
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS_DIR="${SCRIPT_DIR}/scenarios"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CHAOS_COMPOSE_FILE="${CHAOS_COMPOSE_FILE:-$REPO_ROOT/docker/compose/pg-chaos.yml}"
MANAGE_STACK="${MANAGE_STACK:-1}"   # set to 0 to require a pre-running stack

export KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
export KEEL_PORT="${KEEL_PORT:-17432}"
export KEEL_ADMIN_PORT="${KEEL_ADMIN_PORT:-17433}"
export CHAOS_DB="${CHAOS_DB:-chaosdb}"
export CHAOS_USER="${CHAOS_USER:-postgres}"
export CHAOS_PASS="${CHAOS_PASS:-postgres}"
export CHAOS_TIMEOUT="${CHAOS_TIMEOUT:-120}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"

PASS=0
FAIL=0
SKIP=0
FAILED_SCENARIOS=()

# ── Helpers ──────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[chaos]${NC} $*"; }
pass() { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }

check_prereqs() {
    local missing=0
    for cmd in docker psql nc; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            echo "ERROR: '$cmd' not found on PATH" >&2
            missing=1
        fi
    done
    [[ $missing -eq 0 ]] || exit 1
}

# Start the chaos Docker Compose stack and wait for it to be healthy
start_stack() {
    if [[ ! -f "$CHAOS_COMPOSE_FILE" ]]; then
        echo "ERROR: compose file not found: $CHAOS_COMPOSE_FILE" >&2
        exit 1
    fi
    log "Starting chaos stack ($CHAOS_COMPOSE_FILE)..."
    docker compose -f "$CHAOS_COMPOSE_FILE" up -d --build --wait 2>&1 | tail -8
    log "Stack started."
}

# Tear down the chaos stack and its volumes
stop_stack() {
    log "Tearing down chaos stack..."
    docker compose -f "$CHAOS_COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

restart_stack() {
    stop_stack
    start_stack
}

# Verify keel and PostgreSQL primary are reachable
check_stack() {
    log "Checking stack availability..."

    # keel proxy — auto-start if MANAGE_STACK=1 (default)
    if ! nc -z "$KEEL_HOST" "$KEEL_PORT" 2>/dev/null; then
        if [[ "$MANAGE_STACK" == "1" ]]; then
            warn "keel proxy not reachable — starting chaos stack automatically"
            start_stack
            # Wait up to 30s for keel to become reachable
            local i
            for i in $(seq 1 30); do
                nc -z "$KEEL_HOST" "$KEEL_PORT" 2>/dev/null && break
                sleep 1
            done
        fi
        # Re-check after potential start
        if ! nc -z "$KEEL_HOST" "$KEEL_PORT" 2>/dev/null; then
            echo "ERROR: keel proxy not reachable at ${KEEL_HOST}:${KEEL_PORT}" >&2
            echo "       Start the stack first: docker compose -f $CHAOS_COMPOSE_FILE up -d --wait" >&2
            exit 77
        fi
    fi

    # keel admin console
    if ! nc -z "$KEEL_HOST" "$KEEL_ADMIN_PORT" 2>/dev/null; then
        warn "keel admin console not reachable at ${KEEL_HOST}:${KEEL_ADMIN_PORT} (some assertions may be skipped)"
    fi

    # test connectivity through keel
    PGPASSWORD="$CHAOS_PASS" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -c "SELECT 1" >/dev/null 2>&1 \
        || { echo "ERROR: Cannot connect to ${CHAOS_DB} through keel proxy" >&2; exit 1; }

    log "Stack OK — keel ${KEEL_HOST}:${KEEL_PORT}, db=${CHAOS_DB}"
}

# Initialise pgbench tables in chaosdb so kill-backend-mid-query can run a
# TPC-B-like workload without needing a separate setup step.
init_pgbench() {
    if ! command -v pgbench >/dev/null 2>&1; then
        warn "pgbench not on PATH — skipping pgbench table initialisation"
        return
    fi
    # Idempotent: pgbench -i drops and recreates the tables, which is fine
    # because the chaos DB is ephemeral (torn down after each run).
    log "Initialising pgbench tables in ${CHAOS_DB}..."
    PGPASSWORD="$CHAOS_PASS" pgbench \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -i -s 1 >/dev/null 2>&1 \
        || { warn "pgbench -i failed — kill-backend-mid-query scenario may fail"; return; }
    log "pgbench tables ready."
}

# Run a single scenario with timeout
run_scenario() {
    local name="$1"
    local script="${SCENARIOS_DIR}/${name}.sh"

    if [[ ! -f "$script" ]]; then
        warn "Scenario not found: ${script} — skipping"
        SKIP=$((SKIP + 1))
        return
    fi

    echo ""
    echo "──────────────────────────────────────────────────────────"
    log "Running scenario: ${name}"
    echo "──────────────────────────────────────────────────────────"

    local start; start=$(date +%s)
    local rc=0

    if timeout "$CHAOS_TIMEOUT" bash "$script"; then
        rc=0
    else
        rc=$?
    fi

    local elapsed=$(( $(date +%s) - start ))

    if [[ $rc -eq 0 ]]; then
        pass "${name} (${elapsed}s)"
        PASS=$((PASS + 1))
    elif [[ $rc -eq 77 ]]; then
        warn "${name} — skipped (${elapsed}s)"
        SKIP=$((SKIP + 1))
    elif [[ $rc -eq 124 ]]; then
        fail "${name} — TIMED OUT after ${CHAOS_TIMEOUT}s"
        FAIL=$((FAIL + 1))
        FAILED_SCENARIOS+=("${name}:timeout")
    else
        fail "${name} — exit code ${rc} (${elapsed}s)"
        FAIL=$((FAIL + 1))
        FAILED_SCENARIOS+=("${name}:rc=${rc}")
    fi

    if [[ "$MANAGE_STACK" == "1" && "$SKIP_CLEANUP" != "1" ]]; then
        case "$name" in
            flip-primary|timeline-invalidation)
                log "Resetting chaos stack after topology-mutating scenario: ${name}"
                restart_stack
                init_pgbench
                ;;
        esac
    fi

    # Brief pause between scenarios to let the stack settle
    sleep 3
}

# ── Main ─────────────────────────────────────────────────────────────────────

# Track whether we started the stack so we can stop it on exit
_STACK_STARTED_BY_US=0

check_prereqs
# Start stack if managed mode is on (default)
if [[ "$MANAGE_STACK" == "1" ]] && ! nc -z "$KEEL_HOST" "$KEEL_PORT" 2>/dev/null; then
    _STACK_STARTED_BY_US=1
fi
check_stack
init_pgbench
_chaos_cleanup() {
    if [[ "$_STACK_STARTED_BY_US" == "1" && "$SKIP_CLEANUP" != "1" ]]; then
        stop_stack
    fi
}
trap _chaos_cleanup EXIT

# Determine which scenarios to run
ALL_SCENARIOS=(
    "kill-backend-mid-query"
    "partition-replica"
    "sigkill-during-drain"
    "flip-primary"
    # Issue 8 — Failover Gate scenarios
    "primary-dies-idle"
    "primary-dies-during-txn"
    "commit-in-doubt"
    "role-flapping"
    "replica-lag-threshold"
    "timeline-invalidation"
    "scatter-backend-mid-scatter"
    "scatter-network-partition"
)

if [[ $# -gt 0 ]]; then
    REQUESTED=("$@")
else
    REQUESTED=("${ALL_SCENARIOS[@]}")
fi

echo ""
echo "==================================================================="
echo "  KEEL Chaos Test Suite"
echo "  keel:  ${KEEL_HOST}:${KEEL_PORT}"
echo "  db:    ${CHAOS_DB}  user: ${CHAOS_USER}"
echo "  scenarios: ${REQUESTED[*]}"
echo "==================================================================="

for scenario in "${REQUESTED[@]}"; do
    run_scenario "$scenario"
done

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "==================================================================="
echo "  Results: PASS=${PASS}  FAIL=${FAIL}  SKIP=${SKIP}"
if [[ ${#FAILED_SCENARIOS[@]} -gt 0 ]]; then
    echo "  Failed scenarios:"
    for s in "${FAILED_SCENARIOS[@]}"; do
        echo "    - ${s}"
    done
fi
echo "==================================================================="
echo ""

[[ $FAIL -eq 0 ]]
