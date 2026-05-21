#!/usr/bin/env bash
# =============================================================================
# run_torture.sh — KEEL Protocol Torture Suite launcher
# =============================================================================
#
# One-command entry point that builds all Docker images, boots a disposable
# PostgreSQL + KEEL stack, runs every driver torture test inside a container
# that already has psql, pgbench, asyncpg, psycopg3, JDBC, pgx and Prisma
# pre-installed, reports results, and tears everything down.
#
# Usage:
#   tests/suites/run_torture.sh [options] [-- suite-args...]
#
# Options:
#   -h, --help            Show this message and exit
#   --no-build            Skip rebuilding Docker images (use cached layers)
#   --keep-stack          Leave the stack running after tests
#   --soak <seconds>      Override soak-test duration (default: 60)
#   --verbose             Pass --verbose to the test suite
#   --report-dir <path>   Write JSON, logs and Markdown report here
#
# Suite args (after --) are forwarded directly to suite_torture.py, e.g.:
#   tests/suites/run_torture.sh -- --verbose --soak 3600
#
# Environment:
#   KEEL_IMAGE            Pre-built KEEL image tag (skips keel source build)
#   KEEL_TORTURE_SOAK_S   Soak duration in seconds (alternative to --soak)
#   KEEL_E2E_SKIP_BUILD   Set to 1 — same as --no-build
#   KEEL_E2E_KEEP_STACK   Set to 1 — same as --keep-stack
#
# Examples:
#   # Full suite — build everything, run, tear down:
#   tests/suites/run_torture.sh
#
#   # Quick smoke run with pre-built KEEL image, 10-second soak:
#   KEEL_IMAGE=keel:latest tests/suites/run_torture.sh --soak 10
#
#   # Long soak (1 hour) — keep stack so you can inspect state afterwards:
#   tests/suites/run_torture.sh --soak 3600 --keep-stack
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/compose/pg-torture.yml"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SKIP_BUILD="${KEEL_E2E_SKIP_BUILD:-0}"
KEEP_STACK="${KEEL_E2E_KEEP_STACK:-0}"
SOAK_S="${KEEL_TORTURE_SOAK_S:-60}"
REPORT_DIR=""
SUITE_ARGS=()
BUILD_LOG_DIR=""

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'
CYN='\033[0;36m'; BLD='\033[1m';    NC='\033[0m'
info()  { echo -e "${CYN}[i]${NC} $*"; }
ok()    { echo -e "${GRN}[✓]${NC} $*"; }
warn()  { echo -e "${YLW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*" >&2; }
die()   { error "$*"; exit 1; }

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
    sed -n '/^# Usage:/,/^# ===/{/^# ===/d;s/^# \{0,3\}//;p}' "$0"
    exit 0
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
need_arg() {
    local opt="$1"
    local val="${2:-}"
    [[ -n "$val" && "$val" != --* ]] || die "$opt requires an argument"
}

validate_bool_flag() {
    local name="$1"
    local val="$2"
    [[ "$val" == "0" || "$val" == "1" ]] || die "$name must be 0 or 1 (got: $val)"
}

container_health() {
    local container="$1"
    docker inspect \
        --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "$container" 2>/dev/null || true
}

build_service() {
    local service="$1"
    local label="$2"
    local log_file="$BUILD_LOG_DIR/${service}.log"

    info "Building $label…"
    if docker compose -f "$COMPOSE_FILE" build --progress=plain "$service" \
            > "$log_file" 2>&1; then
        grep -E "^#[0-9]|DONE|CACHED|Successfully built|ERROR|error" "$log_file" \
            | tail -20 || true
        ok "$label built (log: $log_file)"
        return
    else
        local rc=$?
        error "$label build failed (exit code $rc). Last log lines:"
        tail -80 "$log_file" >&2 || true
        die "$label build failed; full log: $log_file"
    fi
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)      usage ;;
        --no-build)     SKIP_BUILD=1 ;;
        --keep-stack)   KEEP_STACK=1 ;;
        --soak)         need_arg "$1" "${2:-}"; SOAK_S="$2"; shift ;;
        --verbose)      SUITE_ARGS+=("--verbose") ;;
        --report-dir)   need_arg "$1" "${2:-}"; REPORT_DIR="$2"; shift ;;
        --)             shift; SUITE_ARGS+=("$@"); break ;;
        *)              die "Unknown option: $1.  Run with --help." ;;
    esac
    shift
done

validate_bool_flag "KEEL_E2E_SKIP_BUILD" "$SKIP_BUILD"
validate_bool_flag "KEEL_E2E_KEEP_STACK" "$KEEP_STACK"
[[ "$SOAK_S" =~ ^[0-9]+$ && "$SOAK_S" -gt 0 ]] || die "--soak must be a positive integer number of seconds"

# Auto-generate report directory if not specified
if [[ -z "$REPORT_DIR" ]]; then
    REPORT_DIR="$REPO_ROOT/reports/torture-$(date +%Y-%m-%d-%H-%M-%S)"
fi
mkdir -p "$REPORT_DIR"
REPORT_DIR="$(cd "$REPORT_DIR" && pwd -P)"
BUILD_LOG_DIR="$REPORT_DIR/build-logs"
mkdir -p "$BUILD_LOG_DIR"

export KEEL_TORTURE_SOAK_S="$SOAK_S"

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
echo -e "\n${BLD}${CYN}================================================================${NC}"
echo -e "${BLD}${CYN}  KEEL Protocol Torture Suite${NC}"
echo -e "${BLD}${CYN}================================================================${NC}\n"
echo -e "  Repo:        $REPO_ROOT"
echo -e "  Compose:     $COMPOSE_FILE"
echo -e "  KEEL image:  ${KEEL_IMAGE:-build from source}"
echo -e "  Soak time:   ${SOAK_S}s"
echo -e "  Skip build:  $SKIP_BUILD"
echo -e "  Keep stack:  $KEEP_STACK"
echo -e "  Report dir:  $REPORT_DIR"
echo ""

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
info "Checking prerequisites…"

command -v docker >/dev/null 2>&1   || die "Docker not found in PATH"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 not found"
ok "Docker $(docker --version | awk '{print $3}' | tr -d ',')"

# Export UID so postgres/keel containers do not create root-owned vol files
export CURRENT_UID="${UID:-$(id -u)}"

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
STACK_STARTED=0
RUNNER_PID=""
INTERRUPTED=0
SUITE_RC=0

# ---------------------------------------------------------------------------
# _finalize — collect diagnostics + generate report (idempotent)
# ---------------------------------------------------------------------------
FINALIZED=0
_finalize() {
    [[ "$FINALIZED" == "1" ]] && return
    FINALIZED=1

    info "Collecting post-run diagnostics into $REPORT_DIR …"

    if [[ "$STACK_STARTED" == "1" ]]; then
        # Admin console snapshot (keel exposes port 28433 to the host)
        if command -v psql >/dev/null 2>&1; then
            PGPASSWORD=postgres psql -h localhost -p 28433 -U postgres -d postgres \
                -c 'SHOW STATS' -c 'SHOW POOLS' -c 'SHOW SERVERS' \
                > "$REPORT_DIR/admin-final.txt" 2>/dev/null || true
        else
            docker compose -f "$COMPOSE_FILE" --profile runner run --rm --no-deps \
                --entrypoint psql \
                -e PGPASSWORD=postgres \
                runner \
                --host=keel --port=6433 --username=postgres --dbname=postgres \
                --no-password \
                -c 'SHOW STATS' -c 'SHOW POOLS' -c 'SHOW SERVERS' \
                > "$REPORT_DIR/admin-final.txt" 2>/dev/null || true
        fi

        # Prometheus metrics snapshot (keel exposes port 29102 to the host)
        if command -v curl >/dev/null 2>&1; then
            curl -sf http://localhost:29102/metrics > "$REPORT_DIR/prometheus-final.txt" 2>/dev/null || true
        elif command -v wget >/dev/null 2>&1; then
            wget -qO- http://localhost:29102/metrics > "$REPORT_DIR/prometheus-final.txt" 2>/dev/null || true
        else
            docker compose -f "$COMPOSE_FILE" --profile runner run --rm --no-deps \
                --entrypoint wget runner \
                -qO- http://keel:9101/metrics \
                > "$REPORT_DIR/prometheus-final.txt" 2>/dev/null || true
        fi

        # KEEL container logs
        docker compose -f "$COMPOSE_FILE" logs keel \
            > "$REPORT_DIR/keel.log" 2>/dev/null || true
    else
        warn "Stack was not started — skipping live diagnostics"
    fi

    # Markdown report
    if [[ -f "$REPORT_DIR/report.json" ]]; then
        info "Generating Markdown report…"
        if python3 "$REPO_ROOT/tests/suites/generate_report.py" "$REPORT_DIR" \
                > "$REPORT_DIR/report.md" 2>/dev/null; then
            ok "Report: $REPORT_DIR/report.md"
        else
            warn "Report generation failed (suite result still in $REPORT_DIR/report.json)"
        fi
    else
        warn "No report.json found in $REPORT_DIR — skipping Markdown generation"
    fi

    echo ""
    info "Artifacts saved to: $REPORT_DIR"
}

# EXIT trap — always runs: collect diagnostics then tear down the stack
_on_exit() {
    _finalize
    if [[ "$STACK_STARTED" == "1" && "$KEEP_STACK" != "1" ]]; then
        info "Tearing down stack…"
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
        ok "Stack removed"
    elif [[ "$KEEP_STACK" == "1" ]]; then
        warn "Stack left running (--keep-stack).  To remove:"
        warn "  docker compose -f $COMPOSE_FILE down -v"
    fi
}

# INT/TERM trap — stop the runner gracefully, then let the EXIT trap finish
_on_signal() {
    INTERRUPTED=1
    echo ""
    warn "Interrupted — collecting diagnostics and generating report…"
    if [[ -n "$RUNNER_PID" ]]; then
        kill -TERM "$RUNNER_PID" 2>/dev/null || true
        wait  "$RUNNER_PID" 2>/dev/null || true
        RUNNER_PID=""
    fi
    exit 130
}

trap _on_exit  EXIT
trap _on_signal INT TERM

# ---------------------------------------------------------------------------
# Build images
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    if [[ -n "${KEEL_IMAGE:-}" ]]; then
        info "Using pre-built KEEL image: $KEEL_IMAGE"
    else
        build_service keel "KEEL image"
    fi

    build_service runner "torture-runner image"
else
    info "Skipping image builds (--no-build)"
fi

# ---------------------------------------------------------------------------
# Boot backend services
# ---------------------------------------------------------------------------
info "Starting postgres and keel…"
docker compose -f "$COMPOSE_FILE" up -d postgres keel
STACK_STARTED=1

info "Waiting for services to become healthy…"
TIMEOUT=120
ELAPSED=0
PG_STATUS=""
KEEL_STATUS=""
until [[ "$PG_STATUS" == "healthy" && "$KEEL_STATUS" == "healthy" ]]; do
    PG_STATUS="$(container_health torture-postgres)"
    KEEL_STATUS="$(container_health torture-keel)"
    [[ "$PG_STATUS" == "healthy" && "$KEEL_STATUS" == "healthy" ]] && break

    sleep 2
    ELAPSED=$((ELAPSED + 2))
    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        docker compose -f "$COMPOSE_FILE" ps >&2 || true
        docker compose -f "$COMPOSE_FILE" logs --tail=120 postgres keel >&2 || true
        die "Timed out waiting for services to become healthy (postgres=${PG_STATUS:-unknown}, keel=${KEEL_STATUS:-unknown})"
    fi
done
ok "postgres + keel healthy"

# ---------------------------------------------------------------------------
# Run the torture suite
# ---------------------------------------------------------------------------
info "Running torture suite (soak=${SOAK_S}s)…"
echo ""

# Run the suite in the background so INT/TERM can stop it cleanly.
docker compose -f "$COMPOSE_FILE" \
    --profile runner \
    run --rm \
    -v "$REPORT_DIR:/report:rw" \
    -e KEEL_TORTURE_SOAK_S="$SOAK_S" \
    runner "${SUITE_ARGS[@]}" --json-out /report/report.json &
RUNNER_PID=$!
wait "$RUNNER_PID" && SUITE_RC=0 || SUITE_RC=$?
RUNNER_PID=""

echo ""
if [[ $INTERRUPTED -eq 1 ]]; then
    warn "Torture suite INTERRUPTED"
elif [[ $SUITE_RC -eq 0 ]]; then
    ok "Torture suite PASSED"
else
    error "Torture suite FAILED (exit code $SUITE_RC)"
fi

# _finalize (diagnostics + report) and _on_exit (teardown) run via EXIT trap.
exit $SUITE_RC
