#!/usr/bin/env bash
# Verify that the required chaos inventory exists and uses sentinel checks.

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
scenarios_dir="$root/tests/chaos/scenarios"
manifest="$root/tests/chaos/CHAOS_MANIFEST.md"

required=(
  kill-backend-mid-query.sh
  primary-dies-idle.sh
  primary-dies-during-txn.sh
  commit-in-doubt.sh
  flip-primary.sh
  partition-replica.sh
  replica-lag-threshold.sh
  role-flapping.sh
  timeline-invalidation.sh
  sigkill-during-drain.sh
  scatter-backend-mid-scatter.sh
  scatter-network-partition.sh
)

[[ -f "$manifest" ]] || {
  echo "FAIL: missing chaos manifest: $manifest" >&2
  exit 1
}

missing=0
for scenario in "${required[@]}"; do
  path="$scenarios_dir/$scenario"
  if [[ ! -f "$path" ]]; then
    echo "FAIL: missing chaos scenario: $scenario" >&2
    missing=1
    continue
  fi
  if ! grep -q "$scenario" "$manifest"; then
    echo "FAIL: scenario not listed in manifest: $scenario" >&2
    missing=1
  fi
done

sentinel_required=(
  primary-dies-during-txn.sh
  commit-in-doubt.sh
  partition-replica.sh
  replica-lag-threshold.sh
  sigkill-during-drain.sh
  scatter-backend-mid-scatter.sh
  scatter-network-partition.sh
)

for scenario in "${sentinel_required[@]}"; do
  path="$scenarios_dir/$scenario"
  [[ -f "$path" ]] || continue
  if ! grep -q "sentinel_" "$path"; then
    echo "FAIL: sentinel assertions missing from data-corruption chaos scenario: $scenario" >&2
    missing=1
  fi
done

if [[ $missing -ne 0 ]]; then
  exit 1
fi

echo "OK: chaos manifest covers required scenarios."
