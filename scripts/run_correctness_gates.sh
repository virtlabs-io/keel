#!/usr/bin/env bash
# Run the deterministic correctness-under-failure gates for release candidates.

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
build_dir="${BUILD_DIR:-$root/build}"

if [[ ! -d "$build_dir" ]]; then
  echo "ERROR: build directory not found: $build_dir" >&2
  echo "Run: cmake -S $root -B $build_dir" >&2
  exit 1
fi

regex='test_replay_log|test_crash_recovery_matrix|test_failover_gates|test_drain_shutdown|test_state_machine|test_state_contracts|test_sm_sequence_walk|test_sm_fuzz|test_sm_stress|test_invariant_model|test_pre_query_replay|test_ps_semantic_compat|test_parser_registry|test_router$|check_chaos_manifest|check_correctness_gates|check_dangerous_marketing_claims'

ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}" \
LSAN_OPTIONS="${LSAN_OPTIONS:-detect_leaks=0}" \
ctest --test-dir "$build_dir" -R "$regex" --output-on-failure

cat <<'MSG'

Deterministic correctness gates passed.

Docker chaos remains the live failure gate. Run separately:
  tests/chaos/run-chaos.sh
MSG
