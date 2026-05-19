# Production Readiness Matrix

This document separates the code that is ready for production hardening from
features that are implemented but still under failure-mode validation, and from
features that are aspirational or roadmap-only.

## Production Support Status for v0.2-alpha

Recommended deployment mode: `mode = pool` with `prepared_statement = virtualize`
and `experimental_features = false`.

| Status | Features |
|--------|----------|
| Production candidate | PostgreSQL pool mode, PostgreSQL prepared-statement virtualization after replay validation, admin inspection and basic metrics |
| Hardening | Smart routing, SSV, Patroni failover, transaction tracking |
| Experimental | Sharding, scatter-merge, multi-shard 2PC, WAL/GTID catch-up probes, cluster compression |
| Aspirational | Result cache correctness guarantees |

## Maturity Levels

| Level | Meaning | Operational rule |
|-------|---------|------------------|
| Stable | The hot path is reactor-owned, has focused tests, exposes operator-visible counters, and is expected to be safe under normal production load. | Can be enabled by default for supported deployments. |
| Hardening | Code exists and is useful, but cross-feature behavior or failure semantics still need deterministic chaos/fault coverage. | Enable deliberately and watch the listed counters/logs. |
| Experimental | Prototype or advanced feature with known unclosed production questions. | Do not advertise as a production guarantee. |
| Aspirational | Design target or roadmap item. | Documentation must describe it as planned work only. |

## Feature Maturity

| Area | Current maturity | Notes |
|------|------------------|-------|
| Reactor worker hot path | Stable | Worker, engine, and pool production paths must remain free of blocking `send`, `recv`, `select`, `poll`, and wait loops. `scripts/check_forbidden_blocking.sh` enforces this. |
| Backend connect/auth | Stable | Backend refill and warmup use the async connect state machine. Legacy synchronous helpers must remain out of worker production flow. |
| PostgreSQL transaction pooling | Stable | State sync, deferred BEGIN, prepared-statement replay, and cleanup are ordered pre-query phases. Reuse requires protocol-confirmed idle status. |
| MySQL transaction pooling | Hardening | Wire support exists, but keep parity tests with PostgreSQL for cleanup, replay, and pin handling. |
| Prepared statement pooling | Stable for tracked/virtualized PostgreSQL; hardening for all cross-protocol combinations | Replay/discard must be atomic from the client perspective. |
| Session-context virtualization / SSV | Hardening | State/profile hashing is implemented; semantic GUC coverage and consistency-token capture require continued protocol-level tests. |
| Sticky-primary read-after-write | Stable | Sticky window is conservative and reactor-safe. |
| WAL LSN / GTID replica catch-up probes | Experimental | Token parsing/storage exists; live replica catch-up probes must remain reactor-owned before production promotion. |
| Automatic failover and role detection | Hardening | Routing changes are implemented; deterministic behavior under Patroni failover, split-brain windows, timeline switches, stale replicas, and role flapping remains a required test gate. |
| Horizontal sharding and scatter-merge | Experimental | Keep enabled only for deployments that accept feature-specific risk and test their shard rules. |
| Multi-shard 2PC | Experimental | Requires commit-in-doubt and crash-recovery matrix validation before production promotion. |
| TLS/mTLS | Stable | kTLS acceleration is hardening because kernel and cipher compatibility vary by deployment. |
| Cloud and enterprise auth | Hardening | Token caching and provider hooks exist; provider outages and renewal edge cases must be validated per environment. |
| Admin console, JSON API, Prometheus | Stable for inspection; hardening for UI polish | Operational inspectability takes priority over UI expansion. |
| Web management UI | Experimental | Useful as a read-only view, but not a production control plane. |
| Connection migration and multi-proxy cluster compression | Experimental | Requires stronger drain, residual, and peer-failure coverage. |
| Result cache framework | Aspirational | Framework hooks exist; query-result correctness and invalidation are not production guarantees. |

## Production-Supported Profiles (v0.2-alpha)

Default profile (enabled without opt-in):

```ini
[keel]
experimental_features = false

[worker_group.main]
mode = pool
prepared_statement = virtualize
result_cache = off
```

Experimental profile (explicitly opt-in):

```ini
[keel]
experimental_features = true

[worker_group.main]
mode = smart    # hardening tier
scatter_merge = on
wal_lsn_capture = on
gtid_capture = on
```

Do not treat the experimental profile as a supported production baseline. It is
meant for deliberate feature evaluation with targeted tests and rollout controls.

## Required Failure-Mode Matrix

| Failure mode | Required behavior | Required observability | Current gate |
|--------------|-------------------|------------------------|--------------|
| Backend dies before `ReadyForQuery` | Close or reject the backend; do not reuse without protocol-confirmed idle state. | `backend_close_*`, `proxy_backend_reuse_failure_total`, backend error class. | Pool/protocol cleanup tests plus commit-in-doubt tests. |
| Client disconnects during COMMIT | Preserve commit-in-doubt sessions until outcome is resolved or explicitly surfaced. | `commit_in_doubt_*`, session/admin CID flag. | `test_drain_shutdown`, session engine CID tests. |
| Replica lag exceeds threshold | Route conservatively to primary or reject according to policy; never serve a read known to violate the requested token. | route decision trace, sticky-primary counters, replica lag probe result. | Sticky-primary stable; token catch-up probes experimental. |
| Primary role changes mid-transaction | Keep the transaction pinned to its backend until completion or connection failure; do not silently replay the transaction elsewhere. | pin reason, backend close reason, failover event logs. | Failover tests plus transaction-pin invariants. |
| `DISCARD` / cleanup fails | Close the backend; do not return it to idle lists. | `discard_all_failure`, `cleaning_timeout_total`, `backend_close_cleanup_*`. | Pool cleanup parser tests. |
| Prepared-statement replay fails | Close/reject backend or surface protocol error; never forward queued client traffic after failed replay. | stmt replay counters, backend close reason, protocol error counter. | `test_pre_query_replay`, protocol-flow tests. |
| TLS renegotiation or TLS error | Close affected session; keep backend ownership invariants intact. | TLS failure counters, session close reason. | TLS security tests. |
| Partial send | Resume through deferred-send infrastructure or close conservatively; never assume full write. | deferred-send counters and flow wait timings. | Reactor and pre-query replay tests. |
| Partial recv | Parse only complete protocol frames; buffer fragments or treat malformed data as protocol error. | protocol desync counter. | split-protocol tests and plugin-flow tests. |
| OOM | Fail allocation path cleanly without leaks or state corruption. | allocation failure logs and OOM injection tests. | `test_alloc_inject`, memory tests. |
| FD exhaustion | Reject/admit with bounded resources; no waiter leaks. | FD system stats, pool queue rejects, admission counters. | admission and pool backpressure tests. |
| Worker drain | Stop accepting, finish safe sessions, protect commit-in-doubt, cancel pool waiters. | drain state, wait timeout/cancel counters, CID gauges. | drain/shutdown tests. |
| Shutdown during commit-in-doubt | Do not force-close CID session unless timeout policy explicitly allows unresolved termination. | `sessions_commit_in_doubt`, admin session flag. | drain invariant tests. |

## Failover Semantics

KEEL must prefer deterministic refusal over ambiguous replay:

- Patroni failover: role changes rebuild routing indices and drain idle
  connections to the old role; in-flight transactions stay on their current
  backend until completion, failure, or commit-in-doubt handling.
- Split-brain windows: route reads to primary only when role information is
  ambiguous; never route writes to more than one primary candidate.
- Stale replicas: sticky-primary is the stable fallback. Token-gated replica
  routing remains experimental until replica probes are fully reactor-owned.
- Timeline switch: stale LSN/GTID gates must be invalidated on timeline change.
- Role flapping: health probes should dampen routing changes and avoid
  reconnect storms; closed slots refill asynchronously with backoff.

## Operator Inspection Contract

Before adding UI polish, the admin and Prometheus surfaces must answer:

- why a backend was selected;
- whether state sync, statement replay, or cleanup was required;
- why a session is pinned;
- how many sessions are waiting, pinned, cleaning, or commit-in-doubt;
- why a backend was closed;
- whether cleanup/replay has timed out or failed.
