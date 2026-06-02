# Chaos Manifest

This is the production chaos inventory. Scenarios are intentionally
named after failure modes, not implementation tricks, so CI and release reviews
can verify coverage without reading every shell script.

## Required Scenarios

| Scenario | Script | Required invariant |
|----------|--------|--------------------|
| Backend killed mid-query | `kill-backend-mid-query.sh` | Client receives an error or retryable disconnect, never partial rows. |
| Primary dies idle | `primary-dies-idle.sh` | Idle backend pools drain and rebuild cleanly. |
| Primary dies during transaction | `primary-dies-during-txn.sh` | No silent replay; sentinel rows are atomic. |
| Commit in doubt | `commit-in-doubt.sh` | Unknown COMMIT outcome is surfaced and not replayed. |
| Primary flip/failover | `flip-primary.sh` | Writes follow only the elected primary after convergence. |
| Replica partition | `partition-replica.sh` | Reads route around the partition; writes keep reaching primary. |
| Replica lag threshold | `replica-lag-threshold.sh` | Lagged replicas are avoided or reads fail conservatively. |
| Role flapping | `role-flapping.sh` | Flapping roles trigger conservative routing. |
| Timeline invalidation | `timeline-invalidation.sh` | Stale timeline/token state is rejected or pinned. |
| SIGKILL during drain | `sigkill-during-drain.sh` | Confirmed writes survive; in-flight writes are atomic. |
| Scatter backend killed | `scatter-backend-mid-scatter.sh` | Scatter returns an error, not a partial success. |
| Scatter network partition | `scatter-network-partition.sh` | Scatter fan-out fails closed or returns only supported partial-failure state. |

Every data-corruption scenario must use the sentinel helper in
`tests/chaos/lib/sentinel.sh` so the run checks presence, atomicity, content,
and duplicate prevention.

## Running

```sh
tests/chaos/run-chaos.sh
```

Before tagging a release, run this against the Docker chaos stack from a clean
checkout and archive KEEL logs plus operational replay logs as artifacts.

Scatter scenarios require a scatter/sharding chaos topology. If that topology is
not running, the scenario scripts exit `77` and the orchestrator records them as
skipped rather than failed. A release that promotes scatter beyond experimental
must run those scenarios with the shard containers present.
