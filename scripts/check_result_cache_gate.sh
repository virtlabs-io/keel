#!/usr/bin/env bash
# =============================================================================
# check_result_cache_gate.sh — Issue 9: result_cache experimental feature gate
#
# Binary-level integration test.  Verifies four invariants:
#
#   Case 1: result_cache=on  (no experimental_features)   → rejected (exit 1)
#   Case 2: result_cache=off (default safe value)          → accepted (exit 0)
#   Case 3: result_cache=on  + experimental_features=true  → accepted (exit 0)
#   Case 4: result_cache key absent (defaults to off)       → accepted (exit 0)
#
# Uses the --check-config flag added in Issue 9 so the binary validates the
# config file and exits immediately without binding any sockets.
#
# Usage (called by CTest):
#   check_result_cache_gate.sh <source_dir> [build_dir]
#
# The KEEL binary is resolved in order from:
#   1. KEEL_BIN environment variable (if set)
#   2. <build_dir>/src/main/keel  (when build_dir is passed by CTest)
#   3. <source_dir>/build/src/main/keel  (legacy fallback)
# =============================================================================

set -euo pipefail

SOURCE_DIR="${1:-.}"
BUILD_DIR="${2:-${SOURCE_DIR}/build}"
KEEL_BIN="${KEEL_BIN:-${BUILD_DIR}/src/main/keel}"

if [[ ! -x "$KEEL_BIN" ]]; then
    echo "SKIP: keel binary not found at '$KEEL_BIN' — build the project first"
    exit 0
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

PASS=0
FAIL=0

check_pass() { echo "PASS: $1"; (( PASS++ )) || true; }
check_fail() { echo "FAIL: $1"; (( FAIL++ )) || true; }

# Helper: run keel --check-config, capture output + exit code without triggering set -e
run_check() {
    local config_file="$1"
    _exit_code=0
    _output=$(timeout 5 "$KEEL_BIN" --config "$config_file" --check-config 2>&1) || _exit_code=$?
}

# ---------------------------------------------------------------------------
# Case 1: result_cache = on WITHOUT experimental_features → MUST be rejected
# ---------------------------------------------------------------------------
cat > "$TMP/case1_bad.ini" <<'EOINI'
[keel]
listen_address = 127.0.0.1
listen_port = 29321

[worker_group.default]
host = 127.0.0.1
port = 5432
result_cache = on
EOINI

run_check "$TMP/case1_bad.ini"
if [[ $_exit_code -ne 0 ]] && echo "$_output" | grep -q "result_cache=on"; then
    check_pass "Case 1: result_cache=on without experimental_features → rejected (exit $_exit_code)"
else
    check_fail "Case 1: expected rejection (exit!=0 + gate message)"
    echo "  exit_code=$_exit_code"
    echo "  Output: $_output"
fi

# ---------------------------------------------------------------------------
# Case 2: result_cache = off → MUST be accepted
# ---------------------------------------------------------------------------
cat > "$TMP/case2_off.ini" <<'EOINI'
[keel]
listen_address = 127.0.0.1
listen_port = 29322

[worker_group.default]
host = 127.0.0.1
port = 5432
result_cache = off
EOINI

run_check "$TMP/case2_off.ini"
if [[ $_exit_code -eq 0 ]]; then
    check_pass "Case 2: result_cache=off → accepted (exit 0)"
else
    check_fail "Case 2: result_cache=off: expected exit 0; got exit=$_exit_code"
    echo "  Output: $_output"
fi

# ---------------------------------------------------------------------------
# Case 3: result_cache = on WITH experimental_features = true → MUST be accepted
# ---------------------------------------------------------------------------
cat > "$TMP/case3_exp.ini" <<'EOINI'
[keel]
listen_address = 127.0.0.1
listen_port = 29323
experimental_features = true

[worker_group.default]
host = 127.0.0.1
port = 5432
result_cache = on
EOINI

run_check "$TMP/case3_exp.ini"
if [[ $_exit_code -eq 0 ]]; then
    check_pass "Case 3: result_cache=on + experimental_features=true → accepted (exit 0)"
else
    check_fail "Case 3: expected acceptance (exit 0); got exit=$_exit_code"
    echo "  Output: $_output"
fi

# ---------------------------------------------------------------------------
# Case 4: result_cache key absent (defaults to off) → MUST be accepted
# ---------------------------------------------------------------------------
cat > "$TMP/case4_default.ini" <<'EOINI'
[keel]
listen_address = 127.0.0.1
listen_port = 29324

[worker_group.default]
host = 127.0.0.1
port = 5432
EOINI

run_check "$TMP/case4_default.ini"
if [[ $_exit_code -eq 0 ]]; then
    check_pass "Case 4: result_cache absent (default off) → accepted (exit 0)"
else
    check_fail "Case 4: expected acceptance (exit 0); got exit=$_exit_code"
    echo "  Output: $_output"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Results: ${PASS} passed, ${FAIL} failed"
[[ $FAIL -eq 0 ]]
