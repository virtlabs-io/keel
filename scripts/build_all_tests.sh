#!/usr/bin/env bash
# =============================================================================
# scripts/build_all_tests.sh — Build all sanitizer variants needed by the
#                              full KEEL test suite
# =============================================================================
#
# Produces three build trees so that every suite in run_tests.py can find
# the binaries it expects:
#
#   build-asan/   AddressSanitizer + LeakSanitizer  →  unit, memory (A1/A2),
#                                                      resilience (E1-E5)
#   build-lsan/   Alias of the asan build (LSAN is included in ASAN on Linux)
#                 →  memory (A2)
#   build-tsan/   ThreadSanitizer                   →  concurrency (B1-B8)
#
# The "protocol" and "resilience" live-proxy tests (D1-D20, E6-E10) require
# a running KEEL proxy and cannot be satisfied by a build alone.
#
# Usage
# -----
#   bash scripts/build_all_tests.sh            # build all three variants
#   bash scripts/build_all_tests.sh --asan     # build-asan only
#   bash scripts/build_all_tests.sh --tsan     # build-tsan only
#   bash scripts/build_all_tests.sh --force    # rebuild even if dirs exist
#
# Environment Variables
# ---------------------
#   JOBS          Parallel jobs (default: nproc)
#   SKIP_ASAN     Set to 1 to skip the ASAN build
#   SKIP_TSAN     Set to 1 to skip the TSAN build
#   FORCE_REBUILD Set to 1 to re-configure even if the build dir exists
#
# Prerequisites
# -------------
#   cmake, ninja (or make), gcc or clang with sanitizer support
#   Ubuntu/Debian:  sudo apt-get install cmake ninja-build gcc
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBS="${JOBS:-$(nproc)}"
SKIP_ASAN="${SKIP_ASAN:-0}"
SKIP_TSAN="${SKIP_TSAN:-0}"
FORCE_REBUILD="${FORCE_REBUILD:-0}"

# ---------------------------------------------------------------------------
# Parse flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --asan)   SKIP_TSAN=1   ;;
    --tsan)   SKIP_ASAN=1   ;;
    --force)  FORCE_REBUILD=1 ;;
    --jobs|-j) shift; JOBS="$1" ;;
    -h|--help)
      grep '^#' "$0" | sed -n '/^# Usage/,/^# Prerequisites/{ /^# ===*/d; s/^# \{0,1\}//; p }' | sed '/^Prerequisites/q'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_RED='\033[0;31m'; _GREEN='\033[0;32m'; _YELLOW='\033[1;33m'
_CYAN='\033[0;36m'; _BOLD='\033[1m'; _NC='\033[0m'

info()  { printf "${_CYAN}[build] %s${_NC}\n" "$*"; }
ok()    { printf "${_GREEN}[✓]    %s${_NC}\n" "$*"; }
warn()  { printf "${_YELLOW}[!]    %s${_NC}\n" "$*"; }
fail()  { printf "${_RED}[✗]    %s${_NC}\n" "$*" >&2; }
step()  { printf "\n${_BOLD}${_CYAN}═══ %s ═══${_NC}\n" "$*"; }

# ---------------------------------------------------------------------------
# Prerequisite check
# ---------------------------------------------------------------------------
step "Checking prerequisites"
for cmd in cmake; do
  if ! command -v "$cmd" &>/dev/null; then
    fail "$cmd is required but not installed"; exit 1
  fi
  ok "$cmd found at $(command -v "$cmd")"
done

# Choose generator: prefer Ninja, fall back to Unix Makefiles
GENERATOR_ARGS=()
if command -v ninja &>/dev/null; then
  GENERATOR_ARGS=(-G Ninja)
  ok "ninja found — using Ninja generator"
else
  warn "ninja not found — falling back to Unix Makefiles"
fi

# ---------------------------------------------------------------------------
# Helper: configure + build one variant
#
# Usage: _build_variant <dir-name> <cmake-flags...>
# Skips the configure step when the build directory already has a valid
# CTestTestfile.cmake and FORCE_REBUILD is not set.
# ---------------------------------------------------------------------------
_build_variant() {
  local dir_name="$1"; shift
  local build_dir="${REPO_ROOT}/${dir_name}"

  step "Variant: ${dir_name}"

  if [[ "${FORCE_REBUILD}" == "0" && -f "${build_dir}/CTestTestfile.cmake" ]]; then
    info "  ${dir_name}/ already built — skipping configure (use --force to rebuild)"
  else
    info "  Configuring in ${build_dir} ..."
    cmake -S "${REPO_ROOT}" -B "${build_dir}" \
      "${GENERATOR_ARGS[@]}" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_C_STANDARD=23 \
      -DKEEL_ENABLE_TESTS=ON \
      -DKEEL_ENABLE_HARDENING=OFF \
      "$@"
  fi

  info "  Building ${dir_name}/ (-j${JOBS}) ..."
  cmake --build "${build_dir}" -j"${JOBS}"
  ok "${dir_name}/ ready"
}

# ---------------------------------------------------------------------------
# build-asan  (AddressSanitizer — also provides LeakSanitizer on Linux)
# ---------------------------------------------------------------------------
if [[ "${SKIP_ASAN}" != "1" ]]; then
  _build_variant "build-asan" \
    -DKEEL_ENABLE_ASAN=ON

  # build-lsan is a logical alias: on Linux, ASAN always enables LSAN.
  # The memory suite's find_lsan_build() looks for build-lsan/ with a valid
  # CTestTestfile.cmake.  Create a symlink so no second full build is needed.
  LSAN_DIR="${REPO_ROOT}/build-lsan"
  if [[ ! -e "${LSAN_DIR}" ]]; then
    ln -snf "${REPO_ROOT}/build-asan" "${LSAN_DIR}"
    ok "build-lsan → build-asan (symlink created; ASAN includes LSAN on Linux)"
  elif [[ -L "${LSAN_DIR}" ]]; then
    ok "build-lsan → build-asan (symlink already present)"
  else
    warn "build-lsan/ exists as a real directory — not replacing with symlink"
  fi
else
  warn "SKIP_ASAN=1 — skipping build-asan and build-lsan"
fi

# ---------------------------------------------------------------------------
# build-tsan  (ThreadSanitizer)
# ---------------------------------------------------------------------------
if [[ "${SKIP_TSAN}" != "1" ]]; then
  _build_variant "build-tsan" \
    -DKEEL_ENABLE_TSAN=ON
else
  warn "SKIP_TSAN=1 — skipping build-tsan"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
step "Build Summary"

_suite_status() {
  local dir="$1"
  if [[ -f "${REPO_ROOT}/${dir}/CTestTestfile.cmake" ]]; then
    printf "${_GREEN}ready${_NC}"
  elif [[ -L "${REPO_ROOT}/${dir}" ]]; then
    local target
    target="$(readlink "${REPO_ROOT}/${dir}")"
    if [[ -f "${target}/CTestTestfile.cmake" ]]; then
      printf "${_GREEN}ready (→ %s)${_NC}" "$(basename "$target")"
    else
      printf "${_RED}broken symlink${_NC}"
    fi
  else
    printf "${_RED}missing${_NC}"
  fi
}

printf "  %-14s  %-30s  %s\n" "Build dir" "Status" "Unlocks suites"
printf "  %-14s  %-30s  %s\n" "---------" "------" "--------------"
printf "  %-14s  " "build-asan";   echo -e "$(_suite_status build-asan)    unit, memory (A1), resilience (E1-E5)"
printf "  %-14s  " "build-lsan";   echo -e "$(_suite_status build-lsan)    memory (A2)"
printf "  %-14s  " "build-tsan";   echo -e "$(_suite_status build-tsan)    concurrency (B1-B8)"
echo ""
warn "protocol (D1-D20) and resilience live-proxy tests (E6-E10) require a"
warn "running KEEL proxy: set KEEL_HOST / KEEL_PORT and start the proxy first."
echo ""
info "Run the full suite with:"
printf "  python3 tests/run_tests.py \\\\\n"
printf "    --suite unit --suite memory --suite concurrency \\\\\n"
printf "    --suite protocol --suite resilience \\\\\n"
printf "    --build-dir build-asan --ci\n"
