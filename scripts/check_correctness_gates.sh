#!/usr/bin/env bash
# Static release-gate manifest check for correctness-under-failure coverage.

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cmake="$root/tests/CMakeLists.txt"

required_tests=(
  test_replay_log
  test_crash_recovery_matrix
  test_failover_gates
  test_drain_shutdown
  test_state_machine
  test_state_contracts
  test_sm_sequence_walk
  test_sm_fuzz
  test_sm_stress
  test_invariant_model
  test_pre_query_replay
  test_ps_semantic_compat
  test_parser_registry
  test_router
  check_chaos_manifest
  check_dangerous_marketing_claims
)

missing=0
for test_name in "${required_tests[@]}"; do
  if ! grep -q "NAME[[:space:]]\+$test_name\|add_test(NAME $test_name\|add_executable($test_name" "$cmake"; then
    echo "FAIL: correctness gate missing from tests/CMakeLists.txt: $test_name" >&2
    missing=1
  fi
done

docs=(
  docs/CORRECTNESS_UNDER_FAILURE.md
  docs/OPERATIONAL_REPLAY_LOGS.md
  tests/chaos/CHAOS_MANIFEST.md
)

for doc in "${docs[@]}"; do
  if [[ ! -f "$root/$doc" ]]; then
    echo "FAIL: required correctness document missing: $doc" >&2
    missing=1
  fi
done

if [[ $missing -ne 0 ]]; then
  exit 1
fi

echo "OK: correctness gate manifest is complete."
