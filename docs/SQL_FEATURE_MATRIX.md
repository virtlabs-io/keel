# SQL Feature Matrix

This matrix is the production contract for KEEL SQL routing.  The default
production profile keeps `scatter_merge = off`; enabling scatter/sharding is an
explicit experimental opt-in and must be tested against the application's shard
rules and workload.

Status meanings:

| Status | Meaning |
|--------|---------|
| Production | Intended for first-release production use when the documented configuration is followed. |
| Experimental | Implemented but not a production guarantee until the listed gates pass. |
| Rejects | KEEL must fail closed with SQLSTATE `0A000` / `KEEL_ERR_NOT_SUPPORTED`, never silently return an approximate result. |
| Planned | Not implemented as a supported behavior yet. |

## Core Routing

| SQL pattern | Route/result | Status | Notes |
|-------------|--------------|--------|-------|
| Non-sharded table query | Normal pool routing | Production | Uses read/write split, transaction pinning, and configured pool mode. |
| Sharded table with equality predicate on shard key | `SINGLE` shard | Experimental | Correctness depends on shard rule configuration and parser extraction. |
| Sharded table without shard-key predicate, `scatter_merge = off` | Rejects | Production guard | Prevents accidental fan-out in the default production profile. |
| Sharded table without shard-key predicate, `scatter_merge = on` | `SCATTER` | Experimental | Executor must use `dr->scatter.decisions[]`, not array position or shard id alone. |
| Primary + replica per shard | Router-selected backend per shard | Experimental | Reads may choose replicas; writes must choose primaries. |
| Missing or unhealthy shard participant | Partial plan with failed count / unavailable path | Experimental | Load and failover gates must validate operator-visible behavior. |
| Sparse shard ids / reordered server config | Router decision mapping | Experimental | Covered by unit regression; requires live topology coverage before promotion. |

## Scatter `SELECT`

| SQL pattern | Route/result | Status | Notes |
|-------------|--------------|--------|-------|
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` scalar aggregates | Global merge | Experimental | Merge implementation exists; keep behind scatter gate. |
| `COUNT(DISTINCT col)` | Global dedup/merge | Experimental | Uses proxy-side deduplication. |
| `GROUP BY` with supported aggregates | Global group merge | Experimental | Per-shard groups are merged by key. |
| `ORDER BY` | Global sort | Experimental | Memory/spill and tail-latency gates required. |
| `LIMIT` / `OFFSET` | Global post-merge limit | Experimental | `GROUP BY` strips per-shard limit/offset before fan-out. |
| `HAVING` or outer aggregate filters | Rejects | Rejects | Fail closed until the merge/filter path has complete proof and load coverage. |
| Window functions using `OVER` / `WINDOW` | Rejects | Rejects | Requires full global row context; current production contract rejects scatter. |
| `PERCENTILE_CONT` / `PERCENTILE_DISC` | Rejects | Rejects | Exact global percentiles require global ordered streams. |
| `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` | Rejects | Rejects | Set semantics are global and cannot be evaluated independently per shard. |
| Recursive CTE | Rejects | Rejects | Cross-shard recursion can silently miss links. |
| Read-only CTE without recursion | Scatter pass-through or single fallback for constant CTEs | Experimental | Do not rely on cross-shard joins inside CTEs unless data is colocated. |
| `SELECT DISTINCT col` | Planned | Reject before production promotion | Needs global deduplication for row-returning distinct. |
| `STRING_AGG`, `ARRAY_AGG`, `json*_agg` | Planned | Reject before production promotion | Needs aggregate-specific merge semantics. |
| Cross-shard joins across independent shard keys | Rejects | Rejects | Co-locate tables or join in the application. |

## Scatter DML

| SQL pattern | Route/result | Status | Notes |
|-------------|--------------|--------|-------|
| Single-shard `INSERT` / `UPDATE` / `DELETE` with shard key | `SINGLE` shard | Experimental | Normal backend transaction semantics. |
| Scatter `UPDATE` / `DELETE` without `RETURNING` | Statement-level scatter write | Experimental | Current scope is statement-level atomic scatter DML only. |
| Scatter `INSERT ... SELECT` without `RETURNING` | Statement-level scatter write | Experimental | Must be validated per workload. |
| Scatter DML with `RETURNING` | Rejects | Rejects | Cross-shard row ordering and result merge are undefined today. |
| Multi-row `INSERT ... VALUES` with mixed shard keys | Planned | Reject before production promotion | Current extraction risks first-row routing; needs decomposition by shard. |
| Writable CTE | Planned | Reject before production promotion | Avoid duplicate writes across shards. |

## 2PC Scope

| Capability | Status | Required before production |
|------------|--------|----------------------------|
| In-memory coordinator state machine and deterministic GIDs | Experimental | Unit-tested but not durable. |
| Statement-level scatter write prepare/commit | Experimental | Must remain opt-in and gated. |
| True distributed frontend transactions | Planned | Defer prepare/commit to frontend `COMMIT`/`ROLLBACK` boundaries. |
| Durable coordinator log | Planned | Record participants, prepared state, final decision, phase-2 acknowledgements, recovery attempts. |
| Startup recovery | Planned | Scan `pg_prepared_xacts WHERE gid LIKE 'keel_%'`, reconcile with durable log, resolve or expose admin-required state. |
| Admin in-doubt transaction view and controls | Planned | Operators need inspect/commit/rollback tooling for unresolved prepared xacts. |

## Release Gates

Scatter cannot be promoted from experimental until these gates are green:

| Gate | Required scenarios |
|------|--------------------|
| Plan fidelity | Primary+replica per shard, reordered servers, missing shard, unhealthy replica fallback, sparse shard ids. |
| Fail-closed SQL | Recursive CTE, set ops, DML `RETURNING`, window functions, percentiles, `HAVING`, unsupported aggregates, row-returning `DISTINCT`, mixed-shard multi-row insert. |
| 2PC chaos | kill-before-prepare, kill-after-one-prepare, kill-after-decision-before-phase2, shard restart during phase2, coordinator restart, orphan cleanup. |
| Scatter load | concurrency, pool exhaustion, slow shard, dead shard, large result spill, cancellation, p95/p99 tail-latency SLOs. |
| Observability | Rejection counters, scatter duration histogram, per-shard failure counters, in-doubt 2PC state, recovery attempt counters. |
