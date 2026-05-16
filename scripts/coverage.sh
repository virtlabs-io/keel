#!/usr/bin/env bash
# =============================================================================
# scripts/coverage.sh — Branch & Line Coverage Analysis (Gcov / Lcov / Gcovr)
# =============================================================================
#
# Builds KEEL with gcov instrumentation, runs the full CTest suite, collects
# coverage counters, and produces:
#
#   1. An HTML report with line AND branch coverage (genhtml --branch-coverage)
#   2. A Cobertura XML (for CI badge / codecov upload)
#   3. A "dark corners" text summary — source files ranked by lowest branch
#      coverage so maintainers can target new tests at unexplored paths.
#
# By default this script enforces minimum thresholds and exits non-zero if
# either threshold is not met.
#
# Usage
# -----
#   # Standard run (build + test + report):
#   bash scripts/coverage.sh
#
#   # Skip cmake rebuild if build-coverage already exists:
#   SKIP_BUILD=1 bash scripts/coverage.sh
#
#   # Override thresholds:
#   COVERAGE_LINE_MIN=75 COVERAGE_BRANCH_MIN=50 bash scripts/coverage.sh
#
#   # Output directory override:
#   COVERAGE_HTML_DIR=my-report bash scripts/coverage.sh
#
# Environment Variables
# ---------------------
#   SKIP_BUILD            Set to 1 to skip the cmake configure + build steps.
#   COVERAGE_LINE_MIN     Minimum line coverage % (default: 70)
#   COVERAGE_BRANCH_MIN   Minimum branch coverage % (default: 40)
#   COVERAGE_HTML_DIR     HTML output directory (default: coverage-html)
#   DARK_CORNERS_N        Number of worst-covered files to show (default: 20)
#   JOBS                  Parallel jobs for cmake --build (default: nproc)
#
# Prerequisites
# -------------
#   GCC with gcov support (gcov must be on PATH)
#   lcov >= 1.14
#   genhtml (part of lcov package)
#   gcovr >= 5.0 (for Cobertura XML + decision coverage)
#
#   Ubuntu/Debian:
#     sudo apt-get install lcov gcovr
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build-coverage"
HTML_DIR="${REPO_ROOT}/${COVERAGE_HTML_DIR:-coverage-html}"
SKIP_BUILD="${SKIP_BUILD:-0}"
LINE_MIN="${COVERAGE_LINE_MIN:-70}"
BRANCH_MIN="${COVERAGE_BRANCH_MIN:-40}"
DARK_N="${DARK_CORNERS_N:-20}"
JOBS="${JOBS:-$(nproc)}"

RAW_INFO="${BUILD_DIR}/coverage-raw.info"
FILTERED_INFO="${BUILD_DIR}/coverage-filtered.info"
COBERTURA_XML="${BUILD_DIR}/coverage.xml"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_RED='\033[0;31m'; _GREEN='\033[0;32m'; _YELLOW='\033[1;33m'
_CYAN='\033[0;36m'; _BOLD='\033[1m'; _NC='\033[0m'

info()  { printf "${_CYAN}[cov] %s${_NC}\n" "$*"; }
ok()    { printf "${_GREEN}[✓]   %s${_NC}\n" "$*"; }
warn()  { printf "${_YELLOW}[!]   %s${_NC}\n" "$*"; }
fail()  { printf "${_RED}[✗]   %s${_NC}\n" "$*" >&2; }
head()  { printf "\n${_BOLD}${_CYAN}═══ %s ═══${_NC}\n" "$*"; }

# ---------------------------------------------------------------------------
# Prerequisite check
# ---------------------------------------------------------------------------
head "Checking prerequisites"
for cmd in cmake ninja gcov lcov genhtml gcovr python3; do
  if command -v "$cmd" &>/dev/null; then
    ok "$cmd found at $(command -v "$cmd")"
  else
    case "$cmd" in
      ninja)   warn "ninja not found — will fall back to make" ;;
      gcovr)   warn "gcovr not found — Cobertura XML and dark-corners analysis will be skipped" ;;
      *)       fail "$cmd is required but not installed"; exit 1 ;;
    esac
  fi
done

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
if [[ "${SKIP_BUILD}" == "1" && -f "${BUILD_DIR}/CTestTestfile.cmake" ]]; then
  info "SKIP_BUILD=1 — skipping cmake configure and build"
else
  head "Configuring (coverage build)"
  GENERATOR_ARGS=()
  if command -v ninja &>/dev/null; then
    GENERATOR_ARGS=(-G Ninja)
  fi
  cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" \
    "${GENERATOR_ARGS[@]}" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_STANDARD=23 \
    -DKEEL_ENABLE_TESTS=ON \
    -DKEEL_ENABLE_COVERAGE=ON \
    -DKEEL_ENABLE_HARDENING=OFF

  head "Building"
  cmake --build "${BUILD_DIR}" -j"${JOBS}"
fi

# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------
head "Running test suite"
# Reset all coverage counters before running so stale .gcda files do not
# accumulate across multiple runs in the same build directory.
lcov --directory "${BUILD_DIR}" --zerocounters --quiet 2>/dev/null || true

ctest --test-dir "${BUILD_DIR}" \
      --output-on-failure \
      -j"${JOBS}" \
  || { fail "Some tests failed — coverage data may be incomplete"; }

# ---------------------------------------------------------------------------
# Collect raw coverage counters
# ---------------------------------------------------------------------------
head "Collecting coverage data"

# lcov >= 2.0 uses a slightly different flag name; detect and adapt
LCOV_VER="$(lcov --version 2>&1 | grep -oP '\d+\.\d+' | head -1)"
LCOV_MAJOR="${LCOV_VER%%.*}"

LCOV_IGNORE_ERRORS=()
if [[ "${LCOV_MAJOR}" -ge 2 ]]; then
  LCOV_IGNORE_ERRORS=(--ignore-errors source,negative,unused)
else
  LCOV_IGNORE_ERRORS=(--ignore-errors source --ignore-errors negative)
fi

lcov \
  --capture \
  --directory "${BUILD_DIR}" \
  --output-file "${RAW_INFO}" \
  --rc lcov_branch_coverage=1 \
  "${LCOV_IGNORE_ERRORS[@]}"

info "Filtering system headers and test drivers"
lcov \
  --remove "${RAW_INFO}" \
    '/usr/*' \
    '/usr/include/*' \
    '*/build-coverage/*' \
    '*/tests/*' \
    '*/oss-fuzz/*' \
  --output-file "${FILTERED_INFO}" \
  --rc lcov_branch_coverage=1 \
  "${LCOV_IGNORE_ERRORS[@]}"

# ---------------------------------------------------------------------------
# Parse coverage summary
# ---------------------------------------------------------------------------
SUMMARY="$(lcov --summary "${FILTERED_INFO}" --rc lcov_branch_coverage=1 2>&1)"
echo "${SUMMARY}"

LINE_PCT="$(echo "${SUMMARY}" | grep -oP 'lines\.*:\s*\K[\d.]+' | head -1 || echo "0")"
BRANCH_PCT="$(echo "${SUMMARY}" | grep -oP 'branches\.*:\s*\K[\d.]+' | head -1 || echo "0")"

info "Line coverage:   ${LINE_PCT}%  (threshold: ${LINE_MIN}%)"
info "Branch coverage: ${BRANCH_PCT}%  (threshold: ${BRANCH_MIN}%)"

# ---------------------------------------------------------------------------
# HTML report (line + branch)
# ---------------------------------------------------------------------------
head "Generating HTML report"
mkdir -p "${HTML_DIR}"
genhtml \
  "${FILTERED_INFO}" \
  --output-directory "${HTML_DIR}" \
  --branch-coverage \
  --function-coverage \
  --title "KEEL Coverage Report" \
  --legend \
  --demangle-cpp \
  --quiet

ok "HTML report: ${HTML_DIR}/index.html"

# ---------------------------------------------------------------------------
# Cobertura XML + decision/branch summary (gcovr)
# ---------------------------------------------------------------------------
if command -v gcovr &>/dev/null; then
  head "Generating Cobertura XML (gcovr)"
  gcovr \
    --root "${REPO_ROOT}" \
    --object-directory "${BUILD_DIR}" \
    --exclude '^tests/' \
    --exclude '^build-coverage/' \
    --exclude '^oss-fuzz/' \
    --branches \
    --xml-pretty \
    --output "${COBERTURA_XML}" \
    --print-summary \
    "${BUILD_DIR}" 2>/dev/null || warn "gcovr failed — XML not generated"

  ok "Cobertura XML: ${COBERTURA_XML}"

  # ----- Dark Corners report -----------------------------------------------
  head "Dark Corners — Files With Lowest Branch Coverage"
  printf "\nRanking the ${DARK_N} files with the fewest branches covered.\n"
  printf "These are the 'dark corners' of the codebase most in need of new tests.\n\n"

  # gcovr JSON output lets us compute per-file branch coverage
  GCOVR_JSON="${BUILD_DIR}/coverage-gcovr.json"
  if gcovr \
       --root "${REPO_ROOT}" \
       --object-directory "${BUILD_DIR}" \
       --exclude '^tests/' \
       --exclude '^build-coverage/' \
       --json-pretty \
       --output "${GCOVR_JSON}" \
       "${BUILD_DIR}" 2>/dev/null; then
    python3 - <<PYEOF
import json, sys, os

with open("${GCOVR_JSON}") as f:
    data = json.load(f)

# Build per-file branch stats
files = []
for fdata in data.get("files", []):
    fname   = fdata.get("filename", "?")
    # Relative path for display
    rel     = os.path.relpath(fname, "${REPO_ROOT}")
    # Skip test drivers, generated files, external
    if any(rel.startswith(p) for p in ("tests/", "build-", "oss-fuzz/")):
        continue
    branches = fdata.get("branches", [])
    total   = len(branches)
    covered = sum(1 for b in branches if b.get("count", 0) > 0)
    if total == 0:
        continue
    pct = 100.0 * covered / total
    files.append((pct, covered, total, rel))

files.sort()  # lowest branch-coverage first

if not files:
    print("  (no branch data found — gcovr may need a newer version)")
    sys.exit(0)

print(f"{'Branch%':>9}  {'Cov/Tot':>10}  File")
print("-" * 70)
for pct, cov, tot, name in files[:${DARK_N}]:
    bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
    print(f"{pct:>8.1f}%  {cov:>4}/{tot:<5}  {bar}  {name}")

# Highlight files with zero branch coverage
zero = [(n, t) for (_, _, t, n) in files if _ == 0.0]
if zero:
    print(f"\n⚠  {len(zero)} file(s) with 0% branch coverage:")
    for n, t in zero[:10]:
        print(f"   {n}  ({t} branches unreachable)")
PYEOF
  else
    warn "gcovr JSON generation failed — dark corners report skipped"
  fi
else
  head "Dark Corners (lcov-based fallback)"
  printf "Install gcovr for a richer per-file analysis.\n\n"
  # Fallback: grep uncovered branch lines from the lcov info file
  awk '
    /^SF:/ { file=$0; gsub("^SF:", "", file); branches=0; covered=0 }
    /^BRDA:/ { branches++ }
    /^BRDA:.*[^-],0$/ { }  # 0 = not covered, but we are counting total
    /^BRH:/ { split($0,a,":"); split(a[2],b,","); if (b[2]+0 > 0) covered=b[1]+0; branches_total=b[2]+0 }
    /^end_of_record/ {
      if (branches_total > 0) {
        pct = 100.0 * covered / branches_total
        printf "%.1f%%\t%d/%d\t%s\n", pct, covered, branches_total, file
      }
    }
  ' "${FILTERED_INFO}" \
    | sort -n \
    | head -"${DARK_N}"
fi

# ---------------------------------------------------------------------------
# Enforce thresholds
# ---------------------------------------------------------------------------
head "Threshold enforcement"

FAIL=0

python3 - <<PYEOF
import sys
line_pct   = float("${LINE_PCT}"   or 0)
branch_pct = float("${BRANCH_PCT}" or 0)
line_min   = float("${LINE_MIN}")
branch_min = float("${BRANCH_MIN}")
fail = False

if line_pct < line_min:
    print(f"✗  Line coverage {line_pct:.1f}% < threshold {line_min:.0f}%", file=sys.stderr)
    fail = True
else:
    print(f"✓  Line coverage {line_pct:.1f}% ≥ threshold {line_min:.0f}%")

if branch_pct < branch_min:
    print(f"✗  Branch coverage {branch_pct:.1f}% < threshold {branch_min:.0f}%", file=sys.stderr)
    fail = True
else:
    print(f"✓  Branch coverage {branch_pct:.1f}% ≥ threshold {branch_min:.0f}%")

sys.exit(1 if fail else 0)
PYEOF
FAIL=$?

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
echo ""
printf "${_BOLD}Coverage results${_NC}\n"
printf "  Line:    ${LINE_PCT}%%   (min %s%%)\n" "${LINE_MIN}"
printf "  Branch:  ${BRANCH_PCT}%%   (min %s%%)\n" "${BRANCH_MIN}"
printf "  HTML:    %s/index.html\n" "${HTML_DIR}"
[[ -f "${COBERTURA_XML}" ]] && printf "  XML:     %s\n" "${COBERTURA_XML}"
echo ""

if [[ "${FAIL}" -ne 0 ]]; then
  fail "Coverage threshold not met — see above for details"
  exit 1
fi

ok "All coverage thresholds met"
