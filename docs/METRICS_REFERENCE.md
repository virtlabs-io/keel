# KEEL Metrics Reference

This document is the operator-facing reference for every metric KEEL
exports. It is the source of truth for dashboards, alert
rules, and capacity planning.

The reference is **gated by CI** (`check_metrics_reference`): every
metric registered in `src/observability/otlp/keel_prom_format.c`'s
metadata table must appear in this file under its canonical Prometheus
name, and vice versa. Adding or removing a metric without updating this
document fails the lint stage.

---

## 1. Naming convention

KEEL uses a single internal metric model and renders it into two wire
formats:

| Surface | Wire format | Name shape |
|---|---|---|
| OTLP/HTTP (`/v1/metrics`) | OpenTelemetry protobuf | dotted, e.g. `keel.pool.borrows` |
| Prometheus `/metrics` | Text exposition 0.0.4 | snake_case, e.g. `keel_pool_borrows_total` |
| Admin `/metrics.json` | JSON object | snake_case, same as Prometheus |

The canonical OpenTelemetry name is derived from the Prometheus name
mechanically:

1. Strip the trailing `_total` for counters.
2. Replace every `_` with `.`.

So `keel_pool_borrows_total` ↔ `keel.pool.borrows` and
`keel_sessions_active` ↔ `keel.sessions.active`.

## 2. Resource attributes

Every OTLP export carries a fixed resource set (see §17 of the
observability proposal):

| Attribute | Value | Source |
|---|---|---|
| `service.name` | `keel` | encoder constant |
| `service.version` | build version string | encoder constant |
| `telemetry.sdk.name` | `keel-otlp` | encoder constant |
| `telemetry.sdk.language` | `c` | encoder constant |

Optional operator-supplied attributes (`keel.instance.id`,
`keel.cluster.name`, `keel.node.name`) are not emitted by the current exporter
and will land with the wider deployment-metadata work.

## 3. Cumulative temporality

All counters use OTLP cumulative temporality (`is_monotonic = true`,
`AGGREGATION_TEMPORALITY_CUMULATIVE`). The `start_time_unix_nano` of
every exported point is the aggregator's start instant, not the
process's. Operators graphing rates should use `rate()`/`irate()` (Prom)
or the standard cumulative-to-delta processor (OTel collector).

## 4. Histogram consistency

> Histograms are eventually consistent. Temporary mismatches between
> bucket totals, `_count`, and `_sum` are possible during lock-free
> snapshot collection. These mismatches converge in subsequent
> snapshots and must not be interpreted as data corruption.

This disclaimer applies to **every** exported histogram family.
The current default exporter emits no histograms (`KEEL_TIER_FULL` is gated
out at compile time); the disclaimer is reproduced here so it is in
place the moment the histogram tier is enabled.

## 5. Metric inventory

KEEL exposes metrics across two surfaces:

- **OTLP/HTTP and `/metrics` core families** — section 5.1–5.8 below.
  Cleanly defined, per-OpenTelemetry-semantic-conventions, exported via
  both surfaces. Lockstep with the OTLP encoder's metadata table in
  `src/observability/otlp/keel_prom_format.c::k_meta[]`.
- **Admin `/metrics` extended families** — section 6 below. A superset
  emitted only by the admin Prometheus endpoint. These cover
  pinning-reason attribution, replay/cleanup result breakdowns, TLS/kTLS
  internals, cluster state, trace-pipeline self-stats, and
  process-resource gauges. Per-worker label `{worker="N"}` is included
  alongside an unlabeled `_total`/`_aggregate` line for direct scraping.

All metrics in section 5.1–5.8 are emitted on every worker, summed by
the aggregator before export. None carry dynamic per-request labels —
KEEL enforces zero label cardinality on the hot path (invariant I15).

`Enabled` reflects the default export state.

### 5.1 Sessions

#### `keel.sessions.created`
- **Prometheus:** `keel_sessions_created_total`
- **Type:** Counter (monotonic, cumulative)
- **Unit:** `{session}`
- **Description:** Total frontend sessions created since process start.
- **Attributes:** none
- **Enabled:** yes

#### `keel.sessions.closed`
- **Prometheus:** `keel_sessions_closed_total`
- **Type:** Counter
- **Unit:** `{session}`
- **Description:** Total frontend sessions closed since process start.
- **Attributes:** none
- **Enabled:** yes

#### `keel.sessions.active`
- **Prometheus:** `keel_sessions_active`
- **Type:** Gauge
- **Unit:** `{session}`
- **Description:** Currently active frontend sessions (created minus closed, clamped at zero).
- **Attributes:** none
- **Enabled:** yes

#### `keel.sessions.pinned`
- **Prometheus:** `keel_sessions_pinned`
- **Type:** Gauge
- **Unit:** `{session}`
- **Description:** Sessions currently holding at least one pin reason (transaction, prepared statement, extended protocol, LISTEN/NOTIFY, large object, advisory lock).
- **Attributes:** none
- **Enabled:** yes

### 5.2 Queries

#### `keel.queries`
- **Prometheus:** `keel_queries_total`
- **Type:** Counter
- **Unit:** `{query}`
- **Description:** Total queries routed (read + write + transactional).
- **Attributes:** none
- **Enabled:** yes

#### `keel.queries.read`
- **Prometheus:** `keel_queries_read_total`
- **Type:** Counter
- **Unit:** `{query}`
- **Description:** Read-only queries routed.
- **Attributes:** none
- **Enabled:** yes

#### `keel.queries.write`
- **Prometheus:** `keel_queries_write_total`
- **Type:** Counter
- **Unit:** `{query}`
- **Description:** Write queries routed.
- **Attributes:** none
- **Enabled:** yes

#### `keel.queries.tx`
- **Prometheus:** `keel_queries_tx_total`
- **Type:** Counter
- **Unit:** `{transaction}`
- **Description:** Explicit transactions routed (BEGIN-bounded blocks).
- **Attributes:** none
- **Enabled:** yes

### 5.3 Errors

#### `keel.errors`
- **Prometheus:** `keel_errors_total`
- **Type:** Counter
- **Unit:** `{error}`
- **Description:** Total error events observed (sum of all categories below).
- **Attributes:** none
- **Enabled:** yes

#### `keel.errors.auth`
- **Prometheus:** `keel_errors_auth_total`
- **Type:** Counter
- **Unit:** `{error}`
- **Description:** Authentication errors (bad credentials, SCRAM failures, denied roles).
- **Attributes:** none
- **Enabled:** yes

#### `keel.errors.proto`
- **Prometheus:** `keel_errors_proto_total`
- **Type:** Counter
- **Unit:** `{error}`
- **Description:** PostgreSQL/MySQL wire-protocol errors.
- **Attributes:** none
- **Enabled:** yes

#### `keel.errors.backend`
- **Prometheus:** `keel_errors_backend_total`
- **Type:** Counter
- **Unit:** `{error}`
- **Description:** Backend-side errors propagated to the client (ErrorResponse, fatal disconnects).
- **Attributes:** none
- **Enabled:** yes

#### `keel.errors.timeout`
- **Prometheus:** `keel_errors_timeout_total`
- **Type:** Counter
- **Unit:** `{error}`
- **Description:** Timeout errors (statement timeout, pool wait timeout, idle-in-transaction).
- **Attributes:** none
- **Enabled:** yes

### 5.4 Bytes

#### `keel.bytes.recv`
- **Prometheus:** `keel_bytes_recv_total`
- **Type:** Counter
- **Unit:** `By`
- **Description:** Bytes received from frontends.
- **Attributes:** none
- **Enabled:** yes

#### `keel.bytes.sent`
- **Prometheus:** `keel_bytes_sent_total`
- **Type:** Counter
- **Unit:** `By`
- **Description:** Bytes sent to frontends.
- **Attributes:** none
- **Enabled:** yes

#### `keel.bytes.backend.recv`
- **Prometheus:** `keel_bytes_backend_recv_total`
- **Type:** Counter
- **Unit:** `By`
- **Description:** Bytes received from backends.
- **Attributes:** none
- **Enabled:** yes

#### `keel.bytes.backend.sent`
- **Prometheus:** `keel_bytes_backend_sent_total`
- **Type:** Counter
- **Unit:** `By`
- **Description:** Bytes sent to backends.
- **Attributes:** none
- **Enabled:** yes

#### `keel.bytes.spliced`
- **Prometheus:** `keel_bytes_spliced_total`
- **Type:** Counter
- **Unit:** `By`
- **Description:** Bytes moved between sockets via zero-copy splice. A growing gap between `bytes_recv`+`bytes_backend_recv` and `bytes_spliced` indicates the slow forwarding path is being taken.
- **Attributes:** none
- **Enabled:** yes

### 5.5 Backend pool

#### `keel.pool.borrows`
- **Prometheus:** `keel_pool_borrows_total`
- **Type:** Counter
- **Unit:** `{borrow}`
- **Description:** Backend connections borrowed from the pool by a session.
- **Attributes:** none
- **Enabled:** yes

#### `keel.pool.returns`
- **Prometheus:** `keel_pool_returns_total`
- **Type:** Counter
- **Unit:** `{return}`
- **Description:** Backend connections returned to the idle pool by a session.
- **Attributes:** none
- **Enabled:** yes

#### `keel.pool.creates`
- **Prometheus:** `keel_pool_creates_total`
- **Type:** Counter
- **Unit:** `{connection}`
- **Description:** Backend connections opened (TCP+startup+auth).
- **Attributes:** none
- **Enabled:** yes

#### `keel.pool.destroys`
- **Prometheus:** `keel_pool_destroys_total`
- **Type:** Counter
- **Unit:** `{connection}`
- **Description:** Backend connections destroyed (closed, evicted, errored out).
- **Attributes:** none
- **Enabled:** yes

#### `keel.pool.hits`
- **Prometheus:** `keel_pool_hits_total`
- **Type:** Counter
- **Unit:** `{borrow}`
- **Description:** Pool borrows satisfied immediately by an idle backend.
- **Attributes:** none
- **Enabled:** yes

#### `keel.pool.misses`
- **Prometheus:** `keel_pool_misses_total`
- **Type:** Counter
- **Unit:** `{borrow}`
- **Description:** Pool borrows that required opening a new backend connection.
- **Attributes:** none
- **Enabled:** yes

### 5.6 Reactor

#### `keel.loop.iterations`
- **Prometheus:** `keel_loop_iterations_total`
- **Type:** Counter
- **Unit:** `{iteration}`
- **Description:** Reactor event-loop iterations across all workers.
- **Attributes:** none
- **Enabled:** yes

#### `keel.ops.submitted`
- **Prometheus:** `keel_ops_submitted_total`
- **Type:** Counter
- **Unit:** `{op}`
- **Description:** io_uring SQEs submitted (or epoll equivalents on legacy reactors).
- **Attributes:** none
- **Enabled:** yes

#### `keel.ops.completed`
- **Prometheus:** `keel_ops_completed_total`
- **Type:** Counter
- **Unit:** `{op}`
- **Description:** io_uring CQEs reaped (or epoll equivalents).
- **Attributes:** none
- **Enabled:** yes

### 5.7 Multiplexing safety

#### `keel.discard_all`
- **Prometheus:** `keel_discard_all_total`
- **Type:** Counter
- **Unit:** `{cleanup}`
- **Description:** Full backend cleanup commands issued (`DISCARD ALL` on PostgreSQL, `RESET CONNECTION` on MySQL).
- **Attributes:** none
- **Enabled:** yes

#### `keel.state_sync`
- **Prometheus:** `keel_state_sync_total`
- **Type:** Counter
- **Unit:** `{sync}`
- **Description:** Session-state sync replays issued (SET / search_path / prepared-statement re-establishment on a freshly borrowed backend).
- **Attributes:** none
- **Enabled:** yes

#### `keel.backends.cleaning`
- **Prometheus:** `keel_backends_cleaning`
- **Type:** Gauge
- **Unit:** `{connection}`
- **Description:** Backends currently inside the cleanup state machine.
- **Attributes:** none
- **Enabled:** yes

### 5.8 Process meta

#### `keel.uptime`
- **Prometheus:** `keel_uptime_seconds`
- **Type:** Gauge
- **Unit:** `s`
- **Description:** Process uptime in seconds.
- **Attributes:** none
- **Enabled:** yes

#### `keel.workers`
- **Prometheus:** `keel_workers`
- **Type:** Gauge
- **Unit:** `{worker}`
- **Description:** Configured worker count.
- **Attributes:** none
- **Enabled:** yes

### 5.9 Reason-coded backend close

Per proposal §28 R1, the backend connection state machine has a
**single-writer point** for the close reason — the backend
connection's close-reason field is set exactly once in
[src/worker/backend_pool.c](../src/worker/backend_pool.c) on the path
that transitions a backend into `CLOSING`. Each transition increments
exactly one of the counters below; aggregated together they sum to the
process-wide backend close total.

The 16 reasons cover every code path that retires a backend connection,
from steady-state liveness pruning to fatal protocol violations. None
carry dynamic labels — KEEL prefers one well-named counter per reason
over a single `reason="..."` label family (invariant I15).

| OTLP name | Prometheus | Description |
|---|---|---|
| `keel.backend.close.dead_idle` | `keel_backend_close_dead_idle_total` | Idle backend closed after failed liveness check |
| `keel.backend.close.cleanup_error` | `keel_backend_close_cleanup_error_total` | Cleaning backend closed after cleanup/protocol error |
| `keel.backend.close.cleanup_timeout` | `keel_backend_close_cleanup_timeout_total` | Cleaning backend closed after cleanup deadline |
| `keel.backend.close.client_disconnect` | `keel_backend_close_client_disconnect_total` | Backend closed because the owning client disconnected |
| `keel.backend.close.io_error` | `keel_backend_close_io_error_total` | Backend closed after socket-level I/O error |
| `keel.backend.close.prune_idle` | `keel_backend_close_prune_idle_total` | Idle backend pruned by pool size policy |
| `keel.backend.close.prune_aged` | `keel_backend_close_prune_aged_total` | Backend pruned for exceeding max-age |
| `keel.backend.close.drain_idle` | `keel_backend_close_drain_idle_total` | Idle backend closed during pool drain |
| `keel.backend.close.backend_eof` | `keel_backend_close_backend_eof_total` | Backend closed on unexpected EOF/RST outside cleanup |
| `keel.backend.close.connect_failed` | `keel_backend_close_connect_failed_total` | Backend closed after connect/handshake failure |
| `keel.backend.close.auth_failed` | `keel_backend_close_auth_failed_total` | Backend closed after authentication denial |
| `keel.backend.close.protocol_error` | `keel_backend_close_protocol_error_total` | Backend closed after steady-state protocol violation |
| `keel.backend.close.sync_error` | `keel_backend_close_sync_error_total` | Backend closed after extended-protocol Sync mismatch |
| `keel.backend.close.stmt_replay_error` | `keel_backend_close_stmt_replay_error_total` | Backend closed after prepared-statement replay failure |
| `keel.backend.close.shutdown` | `keel_backend_close_shutdown_total` | Backend closed during process shutdown |
| `keel.backend.close.pool_eviction` | `keel_backend_close_pool_eviction_total` | Backend closed due to pool resize/policy eviction |

- **Type:** Counter (monotonic, cumulative) for all rows.
- **Unit:** `{close}`.
- **Attributes:** none.
- **Enabled:** yes.

### 5.10 Commit-in-doubt resolution

Per proposal §28 R2, transitions in and out of the commit-in-doubt
state have a **single-writer point** spread across `engine_flow.c`,
`state_machine.c`, and `session.c` — the state field is set exactly
once per transition. The counters below decompose those transitions by
outcome; the gauge tracks the in-flight set.

| OTLP name | Prometheus | Type | Description |
|---|---|---|---|
| `keel.commit_in_doubt.started` | `keel_commit_in_doubt_started_total` | Counter | Recovery sessions entering commit-in-doubt |
| `keel.commit_in_doubt.resolved` | `keel_commit_in_doubt_resolved_total` | Counter | Recovery sessions that resolved cleanly |
| `keel.commit_in_doubt.failed` | `keel_commit_in_doubt_failed_total` | Counter | Recovery sessions that could not resolve |
| `keel.sessions.commit_in_doubt` | `keel_sessions_commit_in_doubt` | Gauge | Sessions currently resolving commit outcome |

- **Unit:** `{recovery}` for counters, `{session}` for the gauge.
- **Attributes:** none.
- **Enabled:** yes.
- **Invariant:** `started >= resolved + failed`; the difference equals
  the current value of the `keel.sessions.commit_in_doubt` gauge at any
  instant when no transitions are in flight.

---

## 6. Admin `/metrics` extended families

The admin Prometheus endpoint (`http://<prom_addr>:<prom_port>/metrics`)
exposes a superset of the OTLP/HTTP families catalogued above. These
extended families are emitted by the admin formatter in
[src/admin/admin.c](../src/admin/admin.c) and cover subsystems whose
fine-grained attribution is operator-facing but not yet projected into
the OTLP pipeline.

Conventions:

- Each metric is emitted **both** per-worker (`{worker="N"}` label) and
  as a single unlabeled aggregate line. Operators can scrape either,
  depending on whether they want worker-level breakdown.
- `proxy_*` aliases exist for a subset of the families and are kept for
  backwards compatibility with existing dashboards built against earlier
  KEEL.
- Tables below are generated from `src/admin/admin.c` by
  `scripts/extract_metrics_metadata.py` and enforced by
  `scripts/check_metrics_reference.sh` (CI lint label).

### keel_sessions_*

| Metric | Type | Description |
|---|---|---|
| `keel_sessions_closed` | counter | Total frontend sessions closed |
| `keel_sessions_commit_in_doubt` | gauge | Sessions currently resolving commit outcome |
| `keel_sessions_created` | counter | Total frontend sessions created |
| `keel_sessions_pinned_extended_protocol` | gauge | Sessions pinned by extended protocol |
| `keel_sessions_pinned_prepared_stmt` | gauge | Sessions pinned by prepared statements |
| `keel_sessions_pinned_transaction` | gauge | Sessions pinned by transaction state |

### keel_mem_*

| Metric | Type | Description |
|---|---|---|
| `keel_mem_allocation_count` | gauge | Live keel_malloc allocations |
| `keel_mem_allocations_peak` | gauge | Peak live allocation count since process start |
| `keel_mem_allocations_total` | counter | Total keel_malloc calls since process start |
| `keel_mem_arena_bytes` | gauge | Bytes currently held by all arena instances |
| `keel_mem_arena_count` | gauge | Active keel_arena instances |
| `keel_mem_bytes_allocated` | gauge | Bytes currently allocated via keel_malloc |
| `keel_mem_bytes_committed` | gauge | Bytes committed from backing store (pool footprint or allocated) |
| `keel_mem_bytes_peak` | gauge | Peak bytes allocated since process start |
| `keel_mem_bytes_total` | counter | Total bytes requested via keel_malloc since process start |
| `keel_mem_frees_total` | counter | Total keel_free calls since process start |
| `keel_mem_pool_bytes` | gauge | Bytes currently held by all object pool instances |
| `keel_mem_pool_count` | gauge | Active keel_pool instances |

### keel_shared_pool_*

| Metric | Type | Description |
|---|---|---|
| `keel_shared_pool_free_bytes` | gauge | Bytes available in the shared-buffers pool (total − used) |
| `keel_shared_pool_peak_bytes` | gauge | Historical peak footprint of the shared-buffers pool |
| `keel_shared_pool_total_bytes` | gauge | Total bytes reserved for the shared-buffers pool |
| `keel_shared_pool_used_bytes` | gauge | Bytes currently allocated from the shared-buffers pool |

### keel_queries_*

| Metric | Type | Description |
|---|---|---|
| `keel_queries_read` | counter | Read-only queries |
| `keel_queries_tx` | counter | Explicit transactions |
| `keel_queries_write` | counter | Write queries |

### keel_errors_*

| Metric | Type | Description |
|---|---|---|
| `keel_errors_auth` | counter | Authentication failures |
| `keel_errors_backend` | counter | Backend errors |
| `keel_errors_proto` | counter | Protocol errors |
| `keel_errors_timeout` | counter | Timeout errors |

### keel_bytes_*

| Metric | Type | Description |
|---|---|---|
| `keel_bytes_backend_recv` | counter | Bytes received from backends |
| `keel_bytes_backend_sent` | counter | Bytes sent to backends |
| `keel_bytes_recv` | counter | Bytes received from frontends |
| `keel_bytes_sent` | counter | Bytes sent to frontends |

### keel_pool_*

| Metric | Type | Description |
|---|---|---|
| `keel_pool_borrow_attempts` | counter | Pool borrow decisions attempted |
| `keel_pool_borrow_cleanup_required` | counter | Borrows requiring setup cleanup before use |
| `keel_pool_borrow_exact_state_match` | counter | Borrows satisfied by exact session-state match |
| `keel_pool_borrow_exact_stmt_match` | counter | Borrows satisfied by exact prepared-statement match |
| `keel_pool_borrow_state_replay` | counter | Borrows requiring state replay |
| `keel_pool_borrow_stmt_replay` | counter | Borrows requiring prepared-statement replay |
| `keel_pool_borrows` | counter | Backend connections borrowed |
| `keel_pool_connections_active` | gauge | Backend connections currently in use |
| `keel_pool_connections_clean` | gauge | Backend connections on clean idle list |
| `keel_pool_connections_cleaning` | gauge | Backend connections being cleaned |
| `keel_pool_connections_closed` | gauge | Backend connection slots closed and awaiting refill |
| `keel_pool_connections_dirty` | gauge | Backend connections waiting for cleanup |
| `keel_pool_connections_idle` | gauge | Backend connections idle in pool |
| `keel_pool_connections_pinned` | gauge | Backend connections pinned to sessions |
| `keel_pool_connections_stateful` | gauge | Backend connections on stateful idle list |
| `keel_pool_connections_total` | gauge | Total backend connection slots |
| `keel_pool_creates` | counter | Backend connections opened |
| `keel_pool_destroys` | counter | Backend connections destroyed |
| `keel_pool_hits` | counter | Pool had idle connection ready |
| `keel_pool_misses` | counter | Pool empty, created new backend |
| `keel_pool_returns` | counter | Backend connections returned |
| `keel_pool_utilization_ratio` | gauge | Ratio of active to total backend connections |
| `keel_pool_wait_cancelled` | counter | Pool waiters cancelled because session closed |
| `keel_pool_wait_queue_enqueued` | counter | Sessions enqueued waiting for backend |
| `keel_pool_wait_queue_full_rejects` | counter | Pool wait enqueue attempts rejected because queue was full |
| `keel_pool_wait_resume_requeues` | counter | Pool wait callbacks that requeued because no backend was available |
| `keel_pool_wait_resume_success` | counter | Pool wait callbacks that resumed with a backend |
| `keel_pool_wait_timeout_events` | counter | Pool waiters expired by wait_timeout_ms |
| `keel_pool_waiting_sessions` | gauge | Sessions waiting for a backend connection |

### keel_backend_*

| Metric | Type | Description |
|---|---|---|
| `keel_backend_borrow_total_failed_incompatible` | counter | Borrow attempts rejected by lifecycle incompatibility |
| `keel_backend_borrow_total_failed_quarantined` | counter | Borrow attempts rejected because backend was quarantined |
| `keel_backend_borrow_total_success` | counter | Borrow attempts that passed lifecycle predicate |
| `keel_backend_close_cleanup_error` | counter | Backends closed after cleanup/protocol error |
| `keel_backend_close_cleanup_timeout` | counter | Backends closed after cleanup timeout |
| `keel_backend_close_client_disconnect` | counter | Backends closed because owning client disconnected |
| `keel_backend_close_dead_idle` | counter | Idle backends closed after liveness failure |
| `keel_backend_close_total` | counter | Backend close totals by reason |

### keel_pin_*

| Metric | Type | Description |
|---|---|---|
| `keel_pin_reason_extended_protocol` | counter | Extended protocol pin activations |
| `keel_pin_reason_other` | counter | Other pin reason activations |
| `keel_pin_reason_prepared_stmt` | counter | Prepared statement pin activations |
| `keel_pin_reason_transaction` | counter | Transaction pin activations |

### keel_commit_*

| Metric | Type | Description |
|---|---|---|
| `keel_commit_in_doubt_failed` | counter | Commit-in-doubt recovery sessions unresolved or failed |
| `keel_commit_in_doubt_resolved` | counter | Commit-in-doubt recovery sessions resolved |
| `keel_commit_in_doubt_started` | counter | Commit-in-doubt recovery sessions started |

### keel_cleanup_*

| Metric | Type | Description |
|---|---|---|
| `keel_cleanup_result_backend_eof` | counter | Cleanup runs interrupted by backend EOF |
| `keel_cleanup_result_protocol_error` | counter | Cleanup runs aborted on unsafe protocol stream |
| `keel_cleanup_result_send_failure` | counter | Cleanup runs aborted on send failure |
| `keel_cleanup_result_success` | counter | Cleanup runs completed at reusable boundary |
| `keel_cleanup_result_timeout` | counter | Cleanup runs timed out |
| `keel_cleanup_timeout_total` | counter | Cleanup runs that exceeded timeout |
| `keel_cleanup_total` | counter | Cleanup result totals by outcome |

### keel_cleaning_*

| Metric | Type | Description |
|---|---|---|
| `keel_cleaning_timeout_total` | counter | Backend cleanup timeout events |

### keel_discard_*

| Metric | Type | Description |
|---|---|---|
| `keel_discard_all_count` | counter | Full backend cleanup commands issued |
| `keel_discard_all_failure` | counter | Full backend cleanup failures |

### keel_state_*

| Metric | Type | Description |
|---|---|---|
| `keel_state_sync_count` | counter | Session-state sync replays issued |

### keel_replay_*

| Metric | Type | Description |
|---|---|---|
| `keel_replay_result_drain_error` | counter | Replay/setup failed while draining setup responses |
| `keel_replay_result_oom` | counter | Replay/setup failed due to allocation failure |
| `keel_replay_result_parse_error` | counter | Replay/setup failed due to protocol parse error |
| `keel_replay_result_partial_send_failure` | counter | Replay/setup failed during send/deferred-send path |
| `keel_replay_result_success` | counter | Pre-query replay/setup runs completed |
| `keel_replay_result_timeout` | counter | Replay/setup timed out |

### keel_loop_*

| Metric | Type | Description |
|---|---|---|
| `keel_loop_iterations` | counter | Event loop iterations |

### keel_ops_*

| Metric | Type | Description |
|---|---|---|
| `keel_ops_completed` | counter | CQEs reaped |
| `keel_ops_submitted` | counter | io_uring SQEs submitted |

### keel_notify_*

| Metric | Type | Description |
|---|---|---|
| `keel_notify_relayed` | counter | NotificationResponse messages relayed to LISTEN clients |

### keel_osc_*

| Metric | Type | Description |
|---|---|---|
| `keel_osc_sessions_detected` | counter | Sessions identified as Online Schema Change tool connections (gh-ost/pt-osc) |

### keel_migrations_*

| Metric | Type | Description |
|---|---|---|
| `keel_migrations_received` | counter | Sessions received from another worker |
| `keel_migrations_sent` | counter | Sessions migrated to another worker |

### keel_rebalance_*

| Metric | Type | Description |
|---|---|---|
| `keel_rebalance_checks` | counter | Rebalance timer ticks |
| `keel_rebalance_migrations` | counter | Sessions migrated by auto-rebalance |
| `keel_rebalance_skipped` | counter | Rebalance checks skipped (within threshold) |

### keel_tls_*

| Metric | Type | Description |
|---|---|---|
| `keel_tls_cert_reload_failures` | counter | Total failed certificate reloads |
| `keel_tls_cert_reloads` | counter | Total certificate reload events |
| `keel_tls_connections_failed` | counter | Total failed TLS handshakes |
| `keel_tls_connections_succeeded` | counter | Total successful TLS handshakes |
| `keel_tls_connections_total` | counter | Total TLS contexts created |
| `keel_tls_downgrade_rejected` | counter | Plaintext connections rejected in TLS-require mode |
| `keel_tls_ktls_active` | gauge | Active connections with kTLS enabled |
| `keel_tls_ktls_fallback` | counter | Total kTLS activation fallback events |

### keel_ktls_*

| Metric | Type | Description |
|---|---|---|
| `keel_ktls_cipher_incompatible` | counter | Total kTLS attempts rejected by cipher compatibility |
| `keel_ktls_installations_attempted` | counter | Total attempted kernel TLS installs |
| `keel_ktls_installations_failed` | counter | Total failed kernel TLS installs |
| `keel_ktls_installations_succeeded` | counter | Total successful kernel TLS installs |
| `keel_ktls_kernel_errors` | counter | Total kernel errors during kTLS installation |

### keel_cluster_*

| Metric | Type | Description |
|---|---|---|
| `keel_cluster_election_term` | counter | Current Raft election term |
| `keel_cluster_elections_total` | counter | Elections started by this node |
| `keel_cluster_elections_won_total` | counter | Elections won by this node |
| `keel_cluster_heartbeats_sent_total` | counter | Heartbeats sent |
| `keel_cluster_is_leader` | gauge | 1 if this node is the elected leader |
| `keel_cluster_peers_down` | gauge | Cluster peers DOWN |
| `keel_cluster_peers_up` | gauge | Cluster peers currently UP |

### keel_trace_*

| Metric | Type | Description |
|---|---|---|
| `keel_trace_export_batches` | counter | Total trace export batches sent |
| `keel_trace_export_errors` | counter | Total trace export errors |
| `keel_trace_spans_created` | counter | Total trace spans created |
| `keel_trace_spans_dropped` | counter | Total trace spans dropped (ring full) |
| `keel_trace_spans_exported` | counter | Total trace spans exported |

### keel_proxy_*

| Metric | Type | Description |
|---|---|---|
| `keel_proxy_backend_reuse_failure_total` | counter | Backend cleanup/reuse failures |
| `keel_proxy_buffer_pool_utilization_bytes` | gauge | Recv buffer pool utilization in bytes |
| `keel_proxy_connection_age_seconds` | gauge | Oldest active frontend connection age |
| `keel_proxy_heartbeat_last_ns` | gauge | Last worker heartbeat monotonic timestamp |
| `keel_proxy_io_uring_sq_overflow_total` | counter | io_uring SQ overflow events sampled by workers |
| `keel_proxy_orphaned_transactions_total` | counter | Sessions closed with open backend transaction |
| `keel_proxy_state_desync_total` | counter | Protocol state desynchronization events |

### keel_wait_catchup_*

Emitted by the engine's `stale_read_policy=wait` consultation path
(Patch 2d-4). Both counters are aggregated across workers; per-worker
labeled lines (`{worker="N"}`) are also emitted before the aggregate
line.

| Metric | Type | Description |
|---|---|---|
| `keel_wait_catchup_consulted_total` | counter | Token-bearing replica reads that triggered a router WAIT_CATCHUP consultation |
| `keel_wait_catchup_degraded_to_primary_total` | counter | Reads degraded to primary because the router emitted WAIT_CATCHUP (non-zero only when `stale_read_policy=wait` and a session carries a RYW token) |

**Operational note:** In normal operation `keel_wait_catchup_consulted_total`
grows with every token-bearing read that could be served by a replica, and
`keel_wait_catchup_degraded_to_primary_total / keel_wait_catchup_consulted_total`
is the safe-degrade ratio. A ratio close to 1.0 means replicas are
consistently behind the RYW token — either the replica lag is high or
`max_replica_catchup_ms` is too tight. A ratio of 0.0 means the engine
never had to degrade (normal when all replicas are close to the primary).

When the async-park + resume path is enabled, the degrade counter will drop
toward 0 as the engine waits up to `max_replica_catchup_ms` for a replica
to catch up before falling back.

### proxy_*

| Metric | Type | Description |
|---|---|---|
| `proxy_backend_error_fatal_total` | counter | Fatal backend error classification count |
| `proxy_backend_error_transient_total` | counter | Transient backend error classification count |
| `proxy_backend_reuse_failure_total` | counter | Backend cleanup/reuse failures |
| `proxy_buffer_pool_utilization_bytes` | gauge | Active recv-context buffer usage in bytes |
| `proxy_connection_age_seconds` | gauge | Oldest active frontend connection age |
| `proxy_heartbeat_stalled_workers` | gauge | Workers with stale heartbeat over 20 seconds |
| `proxy_io_uring_sq_overflow_total` | counter | io_uring SQ ring overflow events |
| `proxy_multiplex_ratio` | gauge | Frontend active sessions divided by active backend connections |
| `proxy_orphaned_transactions_total` | counter | Sessions closed with open backend transaction |
| `proxy_stale_connection_workers` | gauge | Workers with oldest connection age over 300 seconds |
| `proxy_state_desync_total` | counter | Protocol state desynchronization events |
| `proxy_sticky_sessions` | gauge | Sessions currently pinned/sticky to backend |

### misc

| Metric | Type | Description |
|---|---|---|
| `keel_cpu_sys_pct` | gauge | CPU system percentage |
| `keel_cpu_user_pct` | gauge | CPU user percentage |
| `keel_fd_limit` | gauge | File descriptor limit |
| `keel_fd_open` | gauge | Open file descriptors |
| `keel_rss_bytes` | gauge | Resident set size in bytes |


---

## 7. Metrics planned for later tiers

The following metric families are specified by the observability design but are
**not** exported yet. They will surface as the relevant subsystems land:

- `keel.pool.wait.duration` (histogram) — backend pool wait queue time
- `keel.pool.scan.duration` (histogram) — backend selection scan time
- `keel.ssv.sync.{required,success,failure,timeout,duration}` — SSV
  resync attribution
- `keel.statement.replay.{required,success,failure,statements,duration}`
- `keel.cleanup.discard.{required,success,failure,skipped}`,
  `keel.cleanup.timeout`, `keel.cleanup.duration`
- `keel.session.{pin,unpin}.transitions` with `reason` attribute
- `keel.backend.close.count` with `reason` attribute
- `keel.transaction.commit.{sent,confirmed_success,confirmed_failure,in_doubt}`
- `keel.observability.export.{attempts,successes,failures,dropped,duration,queue_depth}`
  (exporter self-metrics; lives outside worker hot-path stats by I22)

When any of these are enabled, this document must be updated in the
same change (the CI gate enforces parity with the metadata table).
