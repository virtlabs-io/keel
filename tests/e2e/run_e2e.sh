#!/usr/bin/env bash
# =============================================================================
# run_e2e.sh — KEEL End-to-End Python Test Suite Entry Point
# =============================================================================
#
# Usage:
#   ./run_e2e.sh [options]
#
# Options:
#   -h, --help            Show this message and exit
#   --no-build            Skip rebuilding the KEEL Docker image
#   --keep-stack          Leave the Docker Compose stack running after tests
#   --only <marker>       Run only tests with the given pytest marker
#                         (pool|sharding|scatter|twopc|failover|chaos|metrics|stress)
#   --skip <marker>       Skip tests with the given pytest marker
#   --no-chaos            Alias for --skip chaos  (skips container-kill tests)
#   --no-stress           Alias for --skip stress (skips load tests)
#   --parallel            Run tests in parallel (requires pytest-xdist)
#   --report-dir <dir>    Directory for HTML/JSON reports (default: reports/)
#
# Environment variables (can replace CLI flags):
#   KEEL_E2E_SKIP_BUILD=1    Skip Docker image rebuild
#   KEEL_E2E_KEEP_STACK=1    Keep stack running after tests
#   KEEL_HOST                Override proxy host  (default: 127.0.0.1)
#   KEEL_PORT                Override proxy port  (default: 26432)
#   KEEL_SHARD0_PORT         Override shard-0 port (default: 25432)
#   KEEL_SHARD1_PORT         Override shard-1 port (default: 25433)
#   KEEL_PROM_PORT           Override Prometheus port (default: 29101)
#
# Prerequisites:
#   - Docker + Docker Compose v2
#   - Python >= 3.10 with pip
#   - psycopg2-binary, pytest, pytest-html, pytest-json-report, requests
#     (installed automatically if missing and a venv is available)
#
# Examples:
#   # Full suite (builds KEEL image, starts stack, runs all tests):
#   ./run_e2e.sh
#
#   # Fast smoke test — pool + sharding only, no chaos or stress:
#   ./run_e2e.sh --only pool --only sharding --no-chaos --no-stress
#
#   # Run against a pre-existing stack:
#   ./run_e2e.sh --no-build --keep-stack
#
#   # Run only observability tests in parallel:
#   ./run_e2e.sh --only metrics --parallel
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Locate project root and suite directory
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SUITE_DIR="$SCRIPT_DIR"
COMPOSE_FILE="$REPO_ROOT/docker/compose/e2e-suite.yml"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SKIP_BUILD="${KEEL_E2E_SKIP_BUILD:-0}"
KEEP_STACK="${KEEL_E2E_KEEP_STACK:-0}"
REPORT_DIR="$SUITE_DIR/reports"
PARALLEL=0
MARKER_ARGS=()
SKIP_MARKERS=()
ONLY_MARKERS=()

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[1;33m'
CYN='\033[0;36m'
BLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${CYN}[i]${NC} $*"; }
ok()    { echo -e "${GRN}[✓]${NC} $*"; }
warn()  { echo -e "${YLW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*" >&2; }
die()   { error "$*"; exit 1; }

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
    sed -n '/^# Usage:/,/^# ====/{/^# ====/d;s/^# \{0,3\}//;p}' "$0"
    exit 0
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)         usage ;;
        --no-build)        SKIP_BUILD=1 ;;
        --keep-stack)      KEEP_STACK=1 ;;
        --parallel)        PARALLEL=1 ;;
        --no-chaos)        SKIP_MARKERS+=("chaos") ;;
        --no-stress)       SKIP_MARKERS+=("stress") ;;
        --report-dir)      REPORT_DIR="$2"; shift ;;
        --only)            ONLY_MARKERS+=("$2"); shift ;;
        --skip)            SKIP_MARKERS+=("$2"); shift ;;
        *)                 die "Unknown option: $1.  Run with --help." ;;
    esac
    shift
done

# Export environment variables used by conftest.py
[[ "$SKIP_BUILD" == "1" ]] && export KEEL_E2E_SKIP_BUILD=1
[[ "$KEEP_STACK" == "1" ]] && export KEEL_E2E_KEEP_STACK=1

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
echo -e "\n${BLD}${CYN}================================================================${NC}"
echo -e "${BLD}${CYN}  KEEL End-to-End Python Test Suite${NC}"
echo -e "${BLD}${CYN}================================================================${NC}\n"
echo -e "  Repo:        $REPO_ROOT"
echo -e "  Suite dir:   $SUITE_DIR"
echo -e "  Compose:     $COMPOSE_FILE"
echo -e "  Reports:     $REPORT_DIR"
echo -e "  Skip build:  $SKIP_BUILD"
echo -e "  Keep stack:  $KEEP_STACK"
echo ""

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
info "Checking prerequisites…"

command -v docker  >/dev/null 2>&1 || die "Docker not found in PATH"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 not found"
ok "Docker $(docker --version | awk '{print $3}' | tr -d ',')"

PYTHON=$(command -v python3 || command -v python || echo "")
[[ -n "$PYTHON" ]] || die "Python 3 not found in PATH"
ok "Python $($PYTHON --version)"

# ---------------------------------------------------------------------------
# Virtual environment & dependencies
# ---------------------------------------------------------------------------
VENV_DIR="$SUITE_DIR/.venv"
if [[ ! -d "$VENV_DIR" ]]; then
    info "Creating virtual environment at $VENV_DIR …"
    "$PYTHON" -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

info "Installing / verifying Python dependencies …"
pip install --quiet --upgrade pip
pip install --quiet -r "$SUITE_DIR/requirements.txt"
ok "Python dependencies ready"

# ---------------------------------------------------------------------------
# Build the KEEL Docker image (unless skipped)
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    info "Building KEEL Docker image from source …"
    docker compose -f "$COMPOSE_FILE" build --progress=plain 2>&1 \
        | grep -E "^#[0-9]|Successfully|error|ERRO" | tail -20
    ok "KEEL image built"
else
    info "Skipping KEEL image build (KEEL_E2E_SKIP_BUILD=1)"
fi

# ---------------------------------------------------------------------------
# Prepare report directory
# ---------------------------------------------------------------------------
mkdir -p "$REPORT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
HTML_REPORT="$REPORT_DIR/e2e_report_${TIMESTAMP}.html"
JSON_REPORT="$REPORT_DIR/e2e_report_${TIMESTAMP}.json"
LATEST_HTML="$REPORT_DIR/e2e_report.html"
LATEST_JSON="$REPORT_DIR/e2e_report.json"

# ---------------------------------------------------------------------------
# Build pytest marker expression
# ---------------------------------------------------------------------------
if [[ ${#ONLY_MARKERS[@]} -gt 0 ]]; then
    MARKER_EXPR=$(printf " or %s" "${ONLY_MARKERS[@]}")
    MARKER_EXPR="${MARKER_EXPR:4}"  # strip leading " or "
    MARKER_ARGS+=("-m" "$MARKER_EXPR")
fi

if [[ ${#SKIP_MARKERS[@]} -gt 0 ]]; then
    SKIP_EXPR=$(printf " or %s" "${SKIP_MARKERS[@]}")
    SKIP_EXPR="not (${SKIP_EXPR:4})"
    if [[ ${#ONLY_MARKERS[@]} -gt 0 ]]; then
        MARKER_ARGS=("-m" "($MARKER_EXPR) and $SKIP_EXPR")
    else
        MARKER_ARGS=("-m" "$SKIP_EXPR")
    fi
fi

# Parallel flag
PARALLEL_ARGS=()
[[ "$PARALLEL" == "1" ]] && PARALLEL_ARGS=("-n" "auto")

# ---------------------------------------------------------------------------
# Run pytest
# ---------------------------------------------------------------------------
info "Starting pytest …"
echo ""

PYTEST_RC=0
pytest \
    "$SUITE_DIR" \
    "${MARKER_ARGS[@]+"${MARKER_ARGS[@]}"}" \
    "${PARALLEL_ARGS[@]+"${PARALLEL_ARGS[@]}"}" \
    --html="$HTML_REPORT" \
    --self-contained-html \
    --json-report \
    --json-report-file="$JSON_REPORT" \
    --json-report-indent=2 \
    2>&1 || PYTEST_RC=$?

# Copy to "latest" symlinks for easy access
cp -f "$HTML_REPORT" "$LATEST_HTML"
cp -f "$JSON_REPORT" "$LATEST_JSON"

# ---------------------------------------------------------------------------
# Print summary from JSON report
# ---------------------------------------------------------------------------
echo ""
echo -e "${BLD}${CYN}================================================================${NC}"
echo -e "${BLD}${CYN}  Test Results Summary${NC}"
echo -e "${BLD}${CYN}================================================================${NC}"

if command -v python3 >/dev/null 2>&1 && [[ -f "$JSON_REPORT" ]]; then
    python3 - "$JSON_REPORT" <<'PYEOF'
import json, sys

with open(sys.argv[1]) as f:
    data = json.load(f)

summary = data.get("summary", {})
total    = summary.get("total",   0)
passed   = summary.get("passed",  0)
failed   = summary.get("failed",  0)
error    = summary.get("error",   0)
skipped  = summary.get("skipped", 0)
duration = data.get("duration",   0.0)

# ANSI colours
GRN = "\033[0;32m"; RED = "\033[0;31m"; YLW = "\033[1;33m"
CYN = "\033[0;36m"; BLD = "\033[1m";    NC  = "\033[0m"

print(f"\n  Total:    {total:>4}")
print(f"  {GRN}Passed:   {passed:>4}{NC}")
if failed or error:
    print(f"  {RED}Failed:   {failed + error:>4}{NC}")
if skipped:
    print(f"  {YLW}Skipped:  {skipped:>4}{NC}")
print(f"\n  Duration: {duration:.1f}s")

# Per-category breakdown
cats: dict = {}
for t in data.get("tests", []):
    for marker in t.get("markers", []):
        if marker not in ("pool","sharding","scatter","twopc","failover","chaos","metrics","stress"):
            continue
        cats.setdefault(marker, {"passed":0,"failed":0,"skipped":0})
        cats[marker][t.get("outcome","passed")] = cats[marker].get(t.get("outcome","passed"),0) + 1

if cats:
    print(f"\n  {'Category':<12}  {'Pass':>5}  {'Fail':>5}  {'Skip':>5}")
    print(f"  {'-'*12}  {'-'*5}  {'-'*5}  {'-'*5}")
    for cat in sorted(cats):
        p = cats[cat].get("passed",  0)
        f = cats[cat].get("failed",  0)
        s = cats[cat].get("skipped", 0)
        col = GRN if f == 0 else RED
        print(f"  {cat:<12}  {col}{p:>5}{NC}  {RED if f else NC}{f:>5}{NC}  {YLW if s else NC}{s:>5}{NC}")

# Failed test details
failures = [t for t in data.get("tests", []) if t.get("outcome") in ("failed","error")]
if failures:
    print(f"\n  {RED}FAILED TESTS:{NC}")
    for t in failures[:10]:
        name = t.get("nodeid", "?")
        msg  = ""
        call = t.get("call", {}) or {}
        longrepr = call.get("longrepr", "") or ""
        if longrepr:
            # Last non-empty line
            lines = [l for l in str(longrepr).splitlines() if l.strip()]
            msg = lines[-1][:100] if lines else ""
        print(f"    {RED}✗{NC} {name}")
        if msg:
            print(f"      {msg}")
    if len(failures) > 10:
        print(f"    … and {len(failures)-10} more")
print()
PYEOF
fi

echo -e "  HTML report: ${CYN}$LATEST_HTML${NC}"
echo -e "  JSON report: ${CYN}$LATEST_JSON${NC}"
echo ""

# ---------------------------------------------------------------------------
# Exit code
# ---------------------------------------------------------------------------
if [[ $PYTEST_RC -eq 0 ]]; then
    echo -e "${GRN}${BLD}  ALL TESTS PASSED${NC}\n"
else
    echo -e "${RED}${BLD}  TESTS FAILED (pytest exit code: $PYTEST_RC)${NC}\n"
fi

exit $PYTEST_RC
