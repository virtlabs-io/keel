#!/usr/bin/env bash
# ============================================================================
# hardening-sanitizers.sh — Sanitizer matrix: ASan+UBSan, TSan, MSan
# ============================================================================
#
# Builds and runs the KEEL test suite under multiple memory/thread sanitizers
# to catch undefined behaviour, data races, and memory errors that may not
# manifest in normal builds.
#
# Sanitizer passes:
#   1. ASan + UBSan (AddressSanitizer + Undefined Behavior Sanitizer)
#      - Detects heap buffer overflows, use-after-free, stack overflows,
#        signed integer overflow, null pointer dereference, etc.
#      - Built with -fsanitize=address,undefined.
#
#   2. TSan (ThreadSanitizer)
#      - Detects data races between threads.
#      - Uses `setarch $(uname -m) -R` to disable ASLR entropy randomisation,
#        which is required on Linux 6.x+ kernels where the default ASLR
#        entropy (28+ bits) exceeds TSan's address space layout assumptions.
#      - Applies tsan_suppressions.txt to suppress known false positives
#        (e.g., races inside third-party libraries).
#
#   3. MSan (MemorySanitizer) — Optional, gated by RUN_MSAN=1
#      - Detects reads of uninitialized memory.
#      - Requires clang (GCC does not support MSan).
#      - Excludes tests labelled 'openssl' since OpenSSL is not MSan-clean.
#
# Environment Variables:
#   BUILD_ASAN    ASan build directory (default: <root>/build-asan)
#   BUILD_TSAN    TSan build directory (default: <root>/build-tsan)
#   BUILD_MSAN    MSan build directory (default: <root>/build-msan)
#   RUN_MSAN      Set to 1 to enable the MSan pass (default: 0)
#   JOBS          Parallel build jobs (default: nproc)
#
# Prerequisites:
#   - GCC or Clang with sanitizer support
#   - Clang required for MSan pass
#   - cmake and ctest on PATH
#
# Exit Codes:
#   0  All sanitizer passes passed
#   1  Build or test failure under any sanitizer
#
# Usage:
#   ./scripts/hardening-sanitizers.sh
#   RUN_MSAN=1 ./scripts/hardening-sanitizers.sh   # include MSan pass
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_ASAN="${BUILD_ASAN:-$ROOT_DIR/build-asan}"
BUILD_TSAN="${BUILD_TSAN:-$ROOT_DIR/build-tsan}"
BUILD_MSAN="${BUILD_MSAN:-$ROOT_DIR/build-msan}"
JOBS="${JOBS:-$(nproc)}"
CTEST_LABELS="${CTEST_LABELS:-hardening}"
RUN_MSAN="${RUN_MSAN:-0}"

echo "[hardening] ASAN+UBSAN build: $BUILD_ASAN"
cmake -S "$ROOT_DIR" -B "$BUILD_ASAN" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DKEEL_ENABLE_TESTS=ON \
  -DKEEL_ENABLE_ASAN=ON \
  -DKEEL_ENABLE_UBSAN=ON
cmake --build "$BUILD_ASAN" -j"$JOBS"
ctest --test-dir "$BUILD_ASAN" -L "$CTEST_LABELS" --output-on-failure

echo "[hardening] TSAN build: $BUILD_TSAN"
cmake -S "$ROOT_DIR" -B "$BUILD_TSAN" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DKEEL_ENABLE_TESTS=ON \
  -DKEEL_ENABLE_TSAN=ON
cmake --build "$BUILD_TSAN" -j"$JOBS"
# TSAN requires reduced ASLR entropy (setarch -R) on newer kernels (6.x+)
# to avoid "unexpected memory mapping" false failures.
export TSAN_OPTIONS="suppressions=$ROOT_DIR/cmake/tsan_suppressions.txt"
setarch "$(uname -m)" -R \
  ctest --test-dir "$BUILD_TSAN" -L "$CTEST_LABELS" --output-on-failure
unset TSAN_OPTIONS

if [[ "$RUN_MSAN" == "1" ]]; then
  if ! command -v clang >/dev/null 2>&1; then
    echo "[hardening] SKIP: RUN_MSAN=1 but clang is not installed"
  else
    echo "[hardening] MSAN build: $BUILD_MSAN"
    cmake -S "$ROOT_DIR" -B "$BUILD_MSAN" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_C_COMPILER=clang \
      -DKEEL_ENABLE_TESTS=ON \
      -DKEEL_ENABLE_MSAN=ON
    cmake --build "$BUILD_MSAN" -j"$JOBS"
    # Exclude tests labelled 'openssl' — the system OpenSSL shared library is not
    # MSAN-instrumented and produces unavoidable false positives.
    ctest --test-dir "$BUILD_MSAN" -L "$CTEST_LABELS" -LE openssl --output-on-failure
  fi
fi

echo "[hardening] sanitizer matrix complete"
