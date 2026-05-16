#!/usr/bin/env bash
# ============================================================================
# ci-hard-guarantee.sh — CI gate: build + unit tests + kernel TLS verification
# ============================================================================
#
# Hard CI gate that ensures the KEEL binary builds cleanly and all unit tests
# pass before merging.  Additionally runs the kernel TLS (kTLS) test explicitly
# to verify TLS offload is functional.
#
# Pipeline stages:
#   1. cmake configure with KEEL_ENABLE_TESTS=ON
#   2. Ninja build (parallel, using all available cores)
#   3. ctest — runs the full unit/integration test suite
#   4. Explicit test_tls_ktls — validates kernel TLS send/recv paths
#   5. Optional KEEL_REQUIRE_KTLS=1 strict gate — fails the pipeline if
#      the kernel does not support kTLS (useful for production-image CI).
#
# Environment Variables:
#   KEEL_REQUIRE_KTLS   Set to 1 to hard-fail when kTLS is unavailable.
#                       Default: unset (kTLS test runs but skip is tolerated).
#
# Prerequisites:
#   - cmake, ninja, ctest on PATH
#   - OpenSSL with kTLS support for the kTLS gate
#
# Exit Codes:
#   0  All gates passed
#   1  Build failure, test failure, or kTLS requirement not met
#
# Usage:
#   ./tests/hardening/run_ci_gate.sh
#   KEEL_REQUIRE_KTLS=1 ./tests/hardening/run_ci_gate.sh   # strict kTLS gate
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build-linux"

if [[ ! -d "${BUILD_DIR}" ]]; then
  mkdir -p "${BUILD_DIR}"
fi

cd "${BUILD_DIR}"

cmake ..
ninja -j"$(nproc)"

# Full unit/integration-in-ctest regression suite
ctest --output-on-failure -j"$(nproc)"

# Always run TLS/kTLS regression test explicitly
./tests/test_tls_ktls

# Optional strict gate for kTLS-capable runners.
# Set KEEL_REQUIRE_KTLS=1 to fail if kTLS is not activated.
if [[ "${KEEL_REQUIRE_KTLS:-0}" == "1" ]]; then
  KEEL_TEST_REQUIRE_KTLS=1 ./tests/test_tls_ktls
fi

echo "PASS: hard guarantee gate completed"
