# Production Readiness Matrix

This document separates the code that is ready for production hardening from
features that are implemented but still under failure-mode validation, and from
features that are aspirational or roadmap-only.

For v0.4-alpha, the controlling contract is
[Correctness Under Failure](CORRECTNESS_UNDER_FAILURE.md): when parser, session,
transaction, or failover certainty is missing, KEEL must route conservatively,
pin, reject, or close.

## Production Support Status for v0.4-alpha

Recommended deployment mode: `mode = pool` with `prepared_statement = virtualize`
only after replay validation, and `experimental_features = false`.

| Status | Features |
|--------|----------|
| Stable target | PostgreSQL proxy mode, PostgreSQL pool mode, admin inspection, Prometheus/OTLP observability |
| Beta / hardening | Smart read routing, PostgreSQL prepared-statement virtualization, SSV, Patroni failover, transaction tracking |
| Experimental | Sharding, scatter-merge, multi-shard 2PC, WAL/GTID catch-up probes, result cache, cluster compression |
| Alpha | MySQL wire protocol and pooling |
| Research | GraphQL, MCP, natural-language parsing |

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
| Horizontal sharding and scatter-merge | Experimental | Gated at dispatch behind `scatter_merge = on` per worker group (default `off`); scatter-eligible queries are rejected with SQLSTATE `0A000` until opted in. Recursive CTEs over sharded tables always fail closed with `0A000` (see [LIMITATIONS §1.1](LIMITATIONS.md#11-recursive-common-table-expressions-ctes)). Keep enabled only for deployments that accept feature-specific risk and test their shard rules. |
| Multi-shard 2PC | Experimental | Requires commit-in-doubt and crash-recovery matrix validation before production promotion. |
| TLS/mTLS | Stable | kTLS acceleration is hardening because kernel and cipher compatibility vary by deployment. |
| Cloud and enterprise auth | Hardening | Token caching and provider hooks exist; provider outages and renewal edge cases must be validated per environment. |
| Admin console, JSON API, Prometheus | Stable for inspection; hardening for UI polish | Operational inspectability takes priority over UI expansion. |
| Observability — metric inventory + admin/Prometheus surfaces | Stable | 188 metrics catalogued in [docs/METRICS_REFERENCE.md](METRICS_REFERENCE.md), enforced in CI by `scripts/check_metrics_invariants.sh` and `scripts/check_metrics_reference.sh`. Zero per-request label cardinality on the hot path. |
| Observability — OTLP/HTTP push exporter (`KEEL_ENABLE_OTLP=ON`) | Stable | Background aggregator, non-blocking submit, collector outage isolated from serving (`test_otlp_fault_injection`), perf budget enforced by `test_otlp_overhead_bench`. Operator quick-start: [docs/OBSERVABILITY.md](OBSERVABILITY.md). |
| Web management UI | Experimental | Useful as a read-only view, but not a production control plane. |
| Connection migration and multi-proxy cluster compression | Experimental | Requires stronger drain, residual, and peer-failure coverage. |
| Result cache framework | Aspirational | Framework hooks exist; query-result correctness and invalidation are not production guarantees. |

## Production-Supported Profiles (v0.3-alpha)

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
| Parser error, partial parse, or unknown function | Route to primary or reject; never route to a replica without positive semantic proof. | `SEMANTIC_UNSAFE` route reason, parser status, semantic safety level. | `test_parser_registry`, `test_router`. |
| Parser plugin resource limit or failure | Route to primary, reject, or close; do not trust partially populated plugin output. | parser status, plugin name, semantic safety level. | Parser plugin containment gates planned. |
| Primary role changes mid-transaction | Keep the transaction pinned to its backend until completion or connection failure; do not silently replay the transaction elsewhere. | pin reason, backend close reason, failover event logs. | Failover tests plus transaction-pin invariants. |
| `DISCARD` / cleanup fails | Close the backend; do not return it to idle lists. | `discard_all_failure`, `cleaning_timeout_total`, `backend_close_cleanup_*`. | Pool cleanup parser tests. |
| Prepared-statement replay fails | Close/reject backend or surface protocol error; never forward queued client traffic after failed replay. | stmt replay counters, backend close reason, protocol error counter. | `test_pre_query_replay`, protocol-flow tests. |
| Production incident must be reproducible | Write redacted NDJSON replay events with payload hashes, route reasons, semantic safety, state transitions, and CID outcomes. | `keel_replay_log_*`, replay artifact path, event count. | `test_replay_log`, chaos artifact archival. |
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
- why a backend was closed (one of the 16 reason-coded
  `keel.backend.close.*` counters; see
  [docs/METRICS_REFERENCE.md](METRICS_REFERENCE.md) §5.9);
- whether cleanup/replay has timed out or failed;
- whether the OTLP exporter is delivering snapshots — when enabled,
  `GET /api/observability/exporter.json` reports attempts, successes,
  failures, queue depth, drops, and last error (see
  [docs/OBSERVABILITY.md](OBSERVABILITY.md) §5).

Reason-coded metrics enforce a single-writer point in the relevant
state machine (proposal §28 R1/R2): every backend-close transition
increments exactly one `keel.backend.close.<reason>` counter, and
every commit-in-doubt transition increments exactly one
`keel.commit_in_doubt.<outcome>` counter — operators can sum reason
counters and reconcile against the close total without double-counting.

---

## Release Gates

A release is **blocked** until every gate below passes.  Unit and integration
tests alone are not sufficient — production readiness requires long soak tests
with real PostgreSQL workloads and real client drivers.

### Static / lint gates (run on every CI push)

| Gate | Script | Label |
|------|--------|-------|
| Memory policy — no direct `malloc`/`free` outside `mem/` | `scripts/check_forbidden_syscalls.sh` | `lint` |
| Reactor-blocking lint | `scripts/check_forbidden_blocking.sh` | `lint` |
| Metric invariants — no per-request label cardinality, no hot-path allocations | `scripts/check_metrics_invariants.sh` | `lint` |
| Metrics reference parity — every registered metric is documented in [docs/METRICS_REFERENCE.md](METRICS_REFERENCE.md) and vice versa | `scripts/check_metrics_reference.sh` | `lint` |
| Public-claim safety — forbid risky production promises in Markdown docs | `scripts/check_dangerous_marketing_claims.sh` | `lint` |
| Chaos manifest — every release chaos scenario exists and data-corruption cases use sentinels | `scripts/check_chaos_manifest.sh` | `lint;hardening` |
| Correctness gate manifest — deterministic proof/CID/replay gates are wired into CTest | `scripts/check_correctness_gates.sh` | `lint;hardening` |
| Auth log safety — no auth material in log calls | `scripts/check_auth_log_safety.sh` | `gate;security` |
| Result-cache experimental gate | `scripts/check_result_cache_gate.sh` | `gate` |

Run all lint + gate tests:
```bash
ctest --test-dir build -L "lint|gate" --output-on-failure
```

### Release artifact signing (required for every tagged release)

Tagged releases (`refs/tags/*`) **must** publish signed artifacts. The
`package-linux` workflow hard-fails when `PACKAGE_SIGNING_PRIVATE_KEY`
is not configured for a tagged build — an unsigned tagged release is not
a valid release. Non-tag branch/PR builds may produce unsigned artifacts
for development only and must not be promoted. Operator setup and key
management procedure: [docs/RELEASE_SIGNING.md](RELEASE_SIGNING.md).

### Driver-level torture suite (required before each release)

The **PostgreSQL Protocol Torture Suite** (`tests/suites/suite_torture.py`)
must pass against a real PostgreSQL backend with every supported client driver:

| Test | Driver | What is exercised |
|------|--------|-------------------|
| I1 | psql | Simple/extended protocol, COPY, large result sets |
| I2 | pgbench | TPC-B throughput, prepared-statement mode |
| I3 | JDBC (pgjdbc) | Server-side prepare, threshold re-prepare |
| I4 | pgx v5 (Go) | Named prepared statements, extended protocol |
| I5 | asyncpg (Python) | Async pools, concurrent prepared-statement reuse |
| I6 | psycopg3 (Python) | Pipeline mode, COPY FROM STDIN, async |
| I7 | Prisma (Node.js) | ORM raw query, connection pool lifecycle |
| I8 | Hibernate (Java) | ORM native SQL, SessionFactory lifecycle |
| I9 | Failover chaos | Primary killed mid-txn; clean error + 5 s recovery |
| I10 | Pool cycle | Prepared-statement virtualisation across pool recycle |
| I11 | Long soak | Mixed load for ≥ 1 h; p99 stable, error rate < 1 % |
| I12 | Connection storm | 500 sequential connect/query/close cycles |

Run the torture suite:
```bash
python tests/suites/suite_torture.py --verbose --soak 3600
```

A release **must not** be tagged if I11 fails at `--soak 3600` (1-hour soak).

### Soak pass criteria

| Metric | Threshold |
|--------|-----------|
| Error rate | < 1 % across all query attempts |
| p99 latency drift | < 3× compared with the first 5-minute window |
| RSS growth | < 50 MiB over the full soak duration |
| Open FD trend | Must not grow monotonically (no FD leak) |
| Proxy process | Must not crash or restart during the soak |

These thresholds are enforced by suite `H` (`tests/suites/suite_soak.py`) and
by the torture suite's I11 test.  Both must pass.

### Unsupported SQL/protocol edge cases (documented, not gated)

See [docs/PROTOCOL_EDGE_CASES.md](PROTOCOL_EDGE_CASES.md) for the full
inventory of PostgreSQL wire-protocol and SQL patterns that KEEL does not
support.  Attempting unsupported patterns through a production proxy **must**:

1. Return a well-formed PostgreSQL `ErrorResponse` (never silently corrupt data).
2. Emit a `keel_scatter_unsupported_pattern_total{kind=…}` counter increment so
   operators can alert on unexpected usage.
3. Never crash the proxy worker.

CI verifies (1) and (3) via `test_pg_protocol_flow` and the protocol torture
suite (I-series).  Counter correctness is verified by `test_router_metrics`.
