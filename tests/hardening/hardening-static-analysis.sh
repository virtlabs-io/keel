#!/usr/bin/env bash
# ============================================================================
# hardening-static-analysis.sh — SAST via clang scan-build
# ============================================================================
#
# Runs the Clang Static Analyzer (scan-build) over the entire KEEL codebase
# to detect bugs at compile time without executing code.  The analyzer checks
# for null dereferences, use-after-free, buffer overruns, dead stores, logic
# errors, and other defect classes.
#
# Algorithm:
#   1. Verify scan-build is installed; skip gracefully if not found.
#   2. Clean and configure the build directory with cmake.
#   3. Run scan-build with --status-bugs flag, which causes a non-zero exit
#      if any bug reports are generated.
#   4. Analyzer HTML reports are written to OUT_DIR for manual inspection.
#
# The --status-bugs flag is critical: it turns analyzer findings into CI
# failures, enforcing a zero-bug baseline for static analysis.
#
# Environment Variables:
#   BUILD_DIR   Build directory (default: <root>/build-linux)
#   OUT_DIR     Output directory for HTML reports (default: /tmp/keel_static_analysis_<timestamp>)
#
# Prerequisites:
#   - clang and scan-build on PATH (typically from clang-tools package)
#   - cmake for configuration
#
# Exit Codes:
#   0  PASS — no static analysis issues found
#   1  FAIL — one or more bugs detected
#   0  SKIP — scan-build not installed (exits cleanly)
#
# Usage:
#   ./scripts/hardening-static-analysis.sh
#   BUILD_DIR=/tmp/build-sast ./scripts/hardening-static-analysis.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build-linux}"
OUT_DIR="${OUT_DIR:-/tmp/keel_static_analysis_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[sast] FAIL: missing dependency '$1'" >&2
    exit 2
  }
}

need_cmd cmake

echo "[sast] output: $OUT_DIR"
cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DKEEL_ENABLE_TESTS=ON >/dev/null

if command -v scan-build >/dev/null 2>&1; then
  echo "[sast] running clang static analyzer"
  set +e
  scan-build --status-bugs -o "$OUT_DIR/scan-build" cmake --build "$BUILD_DIR" -j"$(nproc)" \
    >"$OUT_DIR/clang-analyzer.log" 2>&1
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    echo "[sast] FAIL: clang analyzer found issues or build failed"
    echo "[sast] see $OUT_DIR/clang-analyzer.log"
    exit 1
  fi
  echo "[sast] PASS: clang analyzer run completed"
else
  echo "[sast] SKIP: scan-build not installed"
fi

echo "[sast] done"
