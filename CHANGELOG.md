# Changelog

All notable changes to KEEL are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
KEEL uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from `alpha-0.3.0` forward.

> **Migration notes** for breaking config key changes appear in each release
> section under a `### Migration` heading.

---

## [Unreleased]

### Added — Consistent-read catch-up track, Patch 2d-4 / 2d-5

Wires the `stale_read_policy=wait` router verdict (Patch 2d-2) into the
engine read path with a **safe-degrade** semantics for v0.5-alpha. When
a session carries a `keel_consistency_token_t` (RYW) and the router would
emit `KEEL_ROUTE_REASON_WAIT_CATCHUP` against the candidate replica, the
engine logs the verdict, bumps two new per-worker counters, and routes
the query to the primary instead. (The full async-park + reactor-thread
resume continuation that re-dispatches against the now-caught-up replica
remains a v0.5-beta deliverable; the enqueue bridge from Patch 2d-3 is
already in tree to support it.)

- **New helper.** `bool keel_engine_should_degrade_to_primary_on_wait(router, qt, token, in_transaction, &decision)` in `include/keel/engine/catchup_consult.h` / `src/engine/engine_catchup_consult.c`. Returns `true` iff the router's verdict is `WAIT_CATCHUP`.
- **Engine wiring.** `keel_engine_flow_on_fe_data()` (`src/engine/engine_flow.c`) parses the SQL via `keel_sql_analyze_full` with a 64 KiB scratch arena **only on the token-bearing replica-read path** (no overhead for the common case), then consults the helper just before the `FROUTE_READ → switch (route)` dispatch.
- **New stats.** Two `uint64_t` counters on `keel_worker_t.stats`:
  - `wait_catchup_consulted_total` — total token-bearing reads consulted.
  - `wait_catchup_degraded_to_primary` — subset that were degraded to primary.
- **Tests.** 8 unit tests (`tests/test_engine_catchup_consult.c`) cover the predicate exhaustively (NULL guards, in-txn skip, no-token skip, WARN/FAIL policies, etc.). Patch 2d-5 adds a live-PostgreSQL e2e (`tests/test_engine_catchup_consult_pg_e2e.c`) that captures the live primary's WAL LSN via `libpq` (`SELECT pg_current_wal_lsn()`), feeds it to the helper, and asserts the WAIT_CATCHUP+degrade verdict plus exact LSN echo plus the in-transaction negative case. A matching live-MySQL e2e (`tests/test_engine_catchup_consult_my_e2e.c`) captures `@@global.gtid_executed` from the live primary via the `mysql` CLI and asserts the same contract for the GTID token path. Both e2es skip gracefully when no primary is reachable.
- **Full regression.** 139/139 ctest green (including the two new live e2es).

### Added — Failover-manager track (proposals/keel-v.05-alpha-consistent_read-failover-pstmt.md §3-§4)

Brings the v0.5-alpha *failover manager* into the router as a first-class
subsystem and closes the §3 (PG/Patroni) and §4 (MySQL GTID) deliverables.
Behavior is opt-in via the new `[failover]` per-worker section; defaults are
conservative (fence old primary, fail in-flight transactions).

- **Per-router cluster epoch.** `keel_router_t` now owns a monotonic
  `keel_cluster_epoch_t` plus a `pthread_mutex_t epoch_mu`. Each detected
  primary flip increments the epoch under the lock so all subsequent
  routing decisions observe a single, totally-ordered view of the cluster
  generation. Public API: `keel_router_get_cluster_epoch()`.
- **Role-state axis.** New `keel_node_role_state_t` enum
  (`UNKNOWN/PRIMARY/REPLICA/UNHEALTHY/DRAINING/DEMOTED`) is **orthogonal**
  to the existing `keel_server_health_t` and `keel_server_role_t`.
  `rebuild_indices()` filters `DEMOTED`/`DRAINING` nodes out of `rw/ro/wo`
  index arrays so fenced ex-primaries cannot serve any traffic. Public
  API: `keel_router_set_node_role_state()`, `keel_router_get_node_role_state()`,
  `keel_node_role_state_name()`.
- **Observation hook.** `keel_router_observe_primary(router, host, port)`
  is invoked by the Patroni probe ([src/core/router_plugin.c](src/core/router_plugin.c))
  and the SQL discovery path ([src/core/router_discovery.c](src/core/router_discovery.c))
  whenever a new primary is observed. On a flip it bumps the epoch,
  promotes the observed node to `PRIMARY`, demotes the prior primary to
  `DEMOTED` (fenced; no traffic) and sets `config.role` so future
  `rebuild_indices()` calls place servers in the correct index arrays.
- **Routing gates.** `route_internal_ex()` now hard-rejects any decision
  with `KEEL_ROUTE_REASON_OLD_PRIMARY_FENCED` (factor `KEEL_DF_NODE_FENCED`)
  when the candidate node is `DEMOTED`, and with
  `KEEL_ROUTE_REASON_DEGRADED_MODE` (factor `KEEL_DF_DEGRADED_MODE`) when
  the router is in degraded mode and no eligible RW node exists. The
  timeline-mismatch gate (RYW with no caught-up replica) now hard-rejects
  only when `stale_read_policy == KEEL_STALE_READ_REJECT`; otherwise it
  falls through to the existing `ROUTE_PRIMARY` fallback.
- **`stale_read_policy = wait` documented as reserved.** The reactor-owned
  catch-up loop is not implemented in this build; `keel_router_create()`
  logs a WARN and degrades `WAIT` → `ROUTE_PRIMARY`. Operators wanting
  fail-closed behavior should set `stale_read_policy = reject`.
- **MySQL GTID startup probe.** `src/probe/probe_mysql.c` issues
  `SELECT @@gtid_mode, @@enforce_gtid_consistency` on the first successful
  authenticated probe per backend and emits a one-shot WARN (deduped per
  `host:port` per process via a 32-slot table) if GTID is disabled or not
  enforced. Probe results continue to flow normally; the warning is
  visibility-only.
- **`[failover]` per-worker-group config.** New keys parsed in
  [src/main/main.c](src/main/main.c):
  `failover_provider` (`native|patroni|maxscale`),
  `failover_detection_interval`, `failover_failure_threshold`,
  `failover_promotion_grace`, `failover_old_primary_fencing_required`
  (default `true`), `failover_allow_ambiguous_write_retry` (default
  `false`), `failover_read_during_failover` (`reject|degraded|stale_ok`,
  default `degraded`), `failover_transaction_during_failover`
  (`fail|hold|continue`, default `fail`).
- **Tests.** New unit suite [tests/test_failover_manager.c](tests/test_failover_manager.c)
  (10 tests) covers epoch monotonicity, primary-flip fencing, degraded-mode
  rejection, role-state transitions, and the WAIT-policy degradation. New
  Docker-backed integration test
  [tests/integration/test-mysql-gtid-failover.sh](tests/integration/test-mysql-gtid-failover.sh)
  exercises end-to-end failover against a real MySQL primary + 2 replicas
  with GTID catch-up assertion (not registered with ctest; run manually or
  in CI when Docker is available).

### Migration — Failover

- **Operators with `stale_read_policy = wait` in INI:** routing will log a
  WARN at startup and degrade to `route_primary` until the reactor-owned
  WAIT path lands. Set `stale_read_policy = reject` if you need
  fail-closed RYW.
- **Operators parsing routing JSON:** two new `reason_code` values
  (`OLD_PRIMARY_FENCED`, `DEGRADED_MODE`) and two new `factors` entries
  (`NODE_FENCED`, `DEGRADED_MODE`) may appear in route-explain output.

### Changed — Route-decision explainer, conservative function policy, commit-in-doubt gate

These changes lay the groundwork for per-query route explainability and
tighten the policies the router enforces by default. Behavioral changes are
limited to *reason taxonomy* (no silent re-routing); existing decisions
remain identical except for the reason code surfaced in logs and the
`/api/diagnostics/route_explain` admin endpoint.

- **New routing reason codes.** `keel_route_reason_t` gains
  `KEEL_ROUTE_REASON_UNKNOWN_FUNCTION` (the SQL references a function not in
  the metadata cache; conservative policy keeps it on primary) and
  `KEEL_ROUTE_REASON_COMMIT_AMBIGUOUS` (the session has an unresolved
  commit-in-doubt; any further query is refused until the prior COMMIT
  outcome is resolved). The previous catch-all `SEMANTIC_UNSAFE` reason for
  the function-presence case is replaced by `UNKNOWN_FUNCTION` so logs
  distinguish parse failure from unknown-function conservatism.
- **Multi-factor explanation: `decision_factors` bitmask.** Every routing
  decision now carries a bitmask of contributing factors (`KEEL_DF_*`) in
  addition to the single dominant `reason_code`. `IN_TRANSACTION`,
  `HAS_TEMP_TABLE`, `UNKNOWN_FUNCTION`, `PARSE_FAILED`, `FAILOVER_FALLBACK`,
  `COMMIT_IN_DOUBT`, etc. are all stable names exposed in the JSON output.
  `keel_route_factors_to_json_array()` is the public formatter;
  `keel_route_decision_to_json()` embeds it under the `factors` key.
- **Commit-in-doubt is sacred at the router level too.** A new
  `commit_in_doubt` field on `keel_route_session_t` makes `route_internal()`
  fail closed with `KEEL_ROUTE_REASON_COMMIT_AMBIGUOUS` and
  `KEEL_ERR_UNAVAILABLE` whenever the session still has an unresolved
  prior COMMIT. The engine-level guard
  (`keel_engine_flow_handle_commit_doubt()` in `src/engine/engine_flow.c`)
  remains the primary surface for clients; the router check is defense in
  depth for any non-engine caller (admin, tests, future shard re-router).
  No ambiguous transaction is ever replayed after a backend disconnect:
  when the in-doubt XID was never captured (`pending_commit_xid == 0`) the
  session is closed with `KEEL_CIDR_NO_XID`; when a capture exists but no
  RW pool is available the session is closed with `KEEL_CIDR_NO_RW_POOL`.
- **Admin endpoint: `GET /api/diagnostics/route_explain`.** Returns a
  stable JSON taxonomy of all routing reason codes and decision factors so
  operators can map log events to their definitions without reading the
  source. Static reference dump only; per-SQL simulation against the live
  router is tracked in
  [proposals/v0.2-alpha_route_explainer.md](proposals/v0.2-alpha_route_explainer.md).

### Changed — Build variants: `core` and `full`

Lua and Python hook interpreters are now off by default. The shipped
package is the smaller `core` variant; operators who use scripting hooks
opt into `full`.

- `KEEL_ENABLE_LUA` and `KEEL_ENABLE_PYTHON` now default to `OFF` in
  `CMakeLists.txt`. Existing source guards in `src/hook/lua_bridge.c` and
  `src/hook/python_bridge.c` already provide no-op stubs when the
  interpreters are absent, so the call-site contract is unchanged.
- `CPACK_PACKAGE_FILE_NAME` now includes a variant suffix
  (`keel-core-...` vs `keel-full-...`) so distributors can ship both side
  by side.
- `docker/build-linux.sh` honours `KEEL_VARIANT=core|full`
  (default `core`); `docker/Dockerfile.linux` takes a `KEEL_VARIANT`
  build-arg with the same meaning.
- CI matrix builds and tests both variants in parallel
  (`.github/workflows/ci.yml`).

### Migration

- **Operators reading `keel_route_decision_t.reason_code` in tests:** if
  you matched on `KEEL_ROUTE_REASON_SEMANTIC_UNSAFE` for queries with
  function calls, switch to `KEEL_ROUTE_REASON_UNKNOWN_FUNCTION`.
- **Packagers:** the default produced package is now
  `keel-core-<version>-...`. Existing pipelines that downloaded
  `keel-<version>-...` must either pin the `core` suffix or build with
  `-DKEEL_ENABLE_LUA=ON -DKEEL_ENABLE_PYTHON=ON` to keep the historical
  full-feature artifact (now `keel-full-...`).
- **Docker users:** `./docker/build-linux.sh test` continues to work; to
  reproduce the previous (Lua + Python) image use
  `KEEL_VARIANT=full ./docker/build-linux.sh test`.

### Changed — Scatter/Sharding fail-closed gating

Before marketing sharding as a differentiator, the dispatcher now refuses
query shapes that would silently produce wrong results, and scatter dispatch
itself is hidden behind an explicit per-worker-group opt-in.

- **Scatter dispatch is gated behind `scatter_merge = on`** (default `off`).
  `keel_router_config_t` gains a `scatter_merge_enabled` flag that
  `keel_server_init` populates from the worker-group INI. When the gate is
  off, `keel_router_dispatch_sql()` rejects scatter-eligible statements with
  `KEEL_ERR_NOT_SUPPORTED`, bumps the
  `keel_scatter_unsupported_pattern_total{kind="gate_disabled"}` counter,
  and the engine returns a PostgreSQL `ErrorResponse` with SQLSTATE `0A000`
  (`feature_not_supported`) + a `ReadyForQuery` so the session stays usable.
- **`WITH RECURSIVE` over sharded tables now always fails closed.** The
  dispatcher checks for `with_recursive` against the registered shard rules
  *before* the routing decision and rejects matching statements with the
  same `0A000` error path. This closes the silent-duplication failure mode
  documented in [docs/LIMITATIONS.md §1.1](docs/LIMITATIONS.md#11-recursive-common-table-expressions-ctes).
- New tests: `tests/test_sharding.c::test_dispatch_scatter_gate_off_rejects`
  and `::test_dispatch_recursive_cte_rejected`.
- New public surface: `keel_dispatch_result_t` carries `reject_reason`
  (`keel_dispatch_reject_t`) and `reject_message[200]` so the engine can
  surface a human-readable cause without re-parsing.
- Docs updated: `docs/LIMITATIONS.md` §1.1,
  `docs/PRODUCTION_READINESS.md` scatter row,
  `docs/COMPATIBILITY.md` `scatter_merge` row.

### Added

**MySQL ↔ PostgreSQL parity — phase A: commit-in-doubt + semantic profile**

The four MySQL vtable hooks introduced in `79f1646`
(`build_commit_doubt_check`, `get_stmt_compat_profile`,
`replica_reached_token`, `notify_write_lsn`) are now wired end-to-end so the
engine receives the same signals as on PostgreSQL.

- **Post-COMMIT GTID-token capture** (`src/protocol/mysql/mysql_flow.c`):
  When a client issues `COMMIT`, the flow now arms a `commit_pending` flag.
  If the backend reply is an `OK` packet whose status carries
  `SERVER_SESSION_STATE_CHANGED` with a `SESSION_TRACK_GTIDS` entry
  (server-side: `session_track_gtids = OWN_GTID`), the freshly captured GTID
  is hashed via FNV-1a-64 into `keel_be_action_t.commit_xid` and
  `commit_xid_captured` is set. The engine then drives
  `build_commit_doubt_check` exactly as on PostgreSQL when a replacement
  backend has to verify the transaction outcome. A `COMMIT` that returns
  `ERR` (e.g. deadlock) clears `commit_pending` so a later unrelated `OK`
  with a GTID cannot poison the next transaction.

- **Semantic statement-compat profile** (`src/protocol/mysql/mysql_flow.c`):
  `myf_get_stmt_compat_profile` now populates the full profile instead of
  signalling `semantic_unknown = true` on every call:
  - `role_hash` ← FNV-1a-64 of the authenticated user (set at handshake).
  - `search_path_hash` / `schema_epoch` ← FNV-1a-64 of the current database,
    refreshed on `COM_INIT_DB` and on `USE <db>`.
  - `guc_hash` ← XOR-fold of `(key_hash ^ (value_hash + 0x9E3779B97F4A7C15))`
    for every tracked GUC observed in `SET` statements.
  - `semantic_unknown` flips to `true` on the first untracked `SET`
    (notably user-defined variables `SET @foo = ...`), which is the conservative
    signal that prevents the engine from reusing the session.

  The tracked-GUC allowlist matches what KEEL already replays via
  `build_state_sync`: `sql_mode`, `time_zone`, `autocommit`,
  `character_set_{client,results,connection}`, `collation_connection`,
  `transaction_isolation` / `tx_isolation`,
  `transaction_read_only` / `tx_read_only`, `foreign_key_checks`,
  `unique_checks`.

- **Tests** (`tests/test_mysql_protocol_flow.c`): 12 new wire-level cases
  in two sections — *§23 commit-in-doubt* (6 cases covering happy path,
  no-GTID fallback, ERR clearing, GTID-subset probe formatting and
  injection rejection) and *§24 stmt-compat profile* (6 cases covering
  handshake seeding, role/db cross-product, `COM_INIT_DB` epoch bump,
  tracked SET hashing, user-variable opacification, `SET NAMES` tracking).
  Suite is now 373/373 passing; full repo `ctest` remains 117/117.

**Documented limitation:** capture only happens on the `OK` packet of
`COMMIT`. If the client connection dies mid-COMMIT before the `OK`
arrives, KEEL reports the outcome as **UNKNOWN** (engine returns
`08006` / `40000` SQLSTATE on the next attempt). This is the intended
sound behaviour — see *Removed* below for why the previous "phase B"
attempt to close this window was withdrawn.

**PostgreSQL Protocol Torture Suite (Category I) — 56/56 tests pass**
- New torture tests added to `tests/suites/suite_torture.py`:
  - `test_i29_live_reload_under_load` — issues admin `RELOAD` while 30 concurrent
    workers run queries; verifies reload completes with zero query failures
  - `test_i34_set_tracking_across_pool_cycles` — verifies `SET` parameters
    (e.g. `search_path`, `TimeZone`, `application_name`) are preserved across
    pool-cycle boundaries via SSV (Semantic State Virtualization); requires
    `mode = smart`
  - `test_i35_temp_table_session_pin` — verifies that creating a `TEMP TABLE`
    causes KEEL to pin the session to its backend for the remainder of the
    connection lifetime; session pinning is asserted via repeated `SELECT` on
    the temp table across multiple queries
  - `test_i36_with_hold_cursor_across_commit` — declares a `WITH HOLD` cursor,
    commits the transaction, then fetches from the cursor; proxy must keep the
    backend pinned and forward `FETCH` responses correctly
  - `test_i40_cancel_request_storm` — launches 10 concurrent long-running
    queries and fires a `CancelRequest` for each; asserts all 10 queries cancel
    within 5 seconds and the connection pool remains fully healthy afterwards

- Torture suite runner Docker image (`docker/Dockerfile.torture-runner`):
  - Go toolchain pre-installed with `github.com/jackc/pgx/v5` module cache
    populated at image-build time — pgx tests run offline with no network access
  - Node.js, npm, and `npx` pre-installed — Prisma tests run without manual
    package installation

**Torture suite convenience script** (`tests/suites/run_torture.sh`) — one-command
entry point: builds all Docker images, boots a disposable PostgreSQL + KEEL stack,
runs the full 56-test suite, and tears everything down.  Flags: `--no-build`,
`--keep-stack`, `--soak <seconds>`, `--verbose`, `--report-dir <path>`.

### Removed

**MySQL pre-COMMIT GTID probe (the previous "phase B") — withdrawn as unsound.**

The phase B path rewrote `COMMIT` into the multi-statement payload
`SELECT @@gtid_executed AS _keel_token;COMMIT`, captured the GTID set
returned by the embedded `SELECT`, and later resolved commit-in-doubt
episodes via `SELECT GTID_SUBSET('<captured_set>', @@global.gtid_executed)`
on a replacement backend.

That design is unsound: the captured `@@gtid_executed` snapshot is taken
**before** the new commit lands, so by construction it is already a
subset of any later `@@global.gtid_executed`. The `GTID_SUBSET()` probe
therefore returns `1` regardless of whether the in-flight COMMIT actually
committed — a lost COMMIT response would always be falsely resolved as
`RESOLVED_COMMITTED`.

What was removed from `src/protocol/mysql/mysql_flow.c`:

- The `kMyXidCommitMsg` rewrite payload.
- The FE-side COMMIT rewrite and its `txn_tracking` gate (struct field,
  worker-config plumbing inside the plugin, and the test seam
  `keel_mysql_flow_test_enable_txn_tracking`).
- The BE-side absorption state machine (column_count → column_def → EOF →
  data_row → EOF MORE_RESULTS) and the `seq_id` rewrite on the forwarded
  COMMIT OK / mid-probe ERR.

What was removed from `myf_gen_startup`:

- The **unconditional** advertisement of `CLIENT_MULTI_STATEMENTS` and
  `CLIENT_MULTI_RESULTS` in the handshake response. Those caps existed
  solely to support the phase B rewrite. With phase B gone the proxy
  reverts to the upstream MySQL default of rejecting client-supplied
  multi-statement bundles, restoring the per-session security semantics
  callers expect from a stock MySQL connection.

MySQL commit-in-doubt resolution now follows the sound phase A path only:
a token is captured iff the COMMIT's `OK` packet carried a
`SESSION_TRACK_GTIDS` entry **in that same packet**. The post-COMMIT
capture is now gated on a per-packet `gtid_refreshed_this_packet` flag —
a stale `ctx->keel_write_gtid` populated by an earlier round-trip or by
`notify_write_lsn()` (RYW intercept) can no longer be promoted into a
commit token. If no fresh `SESSION_TRACK_GTIDS` is delivered, the engine
reports the outcome as **UNKNOWN** rather than committed.

Negative test added in `tests/test_mysql_protocol_flow.c` §25
(`test_stale_gtid_does_not_resolve_lost_commit`) plants a pre-existing
GTID via `notify_write_lsn()`, drives a COMMIT whose OK reply lacks
`SESSION_TRACK_GTIDS`, and asserts that `commit_xid_captured` stays
`false` and `commit_xid == 0` — the proof that a pre-existing GTID set
cannot resolve a lost COMMIT as committed.

### Fixed

**PostgreSQL Engine — Critical Protocol Correctness**

- **BackendKeyData capture during SCRAM authentication** (`src/worker/backend_connect_async.c`):
  The `case 'K':` handler was absent from the SCRAM authentication state machine,
  so `BackendKeyData` (the 8-byte PID + secret used for cancel requests) was
  silently discarded during SCRAM logins.  Cancel requests issued against SCRAM
  connections would have had a wrong or zeroed secret and would have been ignored
  by PostgreSQL.  Added `case 'K':` to capture the PID + secret correctly during
  SCRAM auth.

- **Cancel request forwarding** — Cancel requests are now forwarded to the correct
  backend.  Previously, a cancel issued by a client could be sent to the wrong
  backend connection, or lost entirely.

- **ReadyForQuery suppression after cancel (cancel_rfq_suppress)**
  (`src/protocol/postgres/postgres_flow.c`):
  After a query is cancelled, PostgreSQL sends `ErrorResponse` with SQLSTATE
  `57014` (query_cancelled) followed by `ReadyForQuery`.  KEEL was forwarding
  both messages verbatim, which caused some drivers (JDBC, pgx) to see an
  unexpected `Z` message and go out of sync.  Now: when KEEL observes an
  `ErrorResponse` with SQLSTATE `57014`, it sets `cancel_rfq_suppress` in the
  flow context; the subsequent `Z` is absorbed internally and not forwarded to
  the client.

- **cancel_pending early-cancel race** (`include/keel/session/session.h`):
  Added `_Atomic bool cancel_pending` to the session struct.  A `CancelRequest`
  that arrives before the query is dispatched to a backend is now latched and
  forwarded as soon as the backend connection is established, preventing the
  race where a fast cancel arrived "too early" and was silently dropped.

- **Batch send pipeline stall**: A bug in the batch-send path caused a pipeline
  to stall when multiple client messages were coalesced into a single `writev`
  call.  Fixed write-completion tracking to correctly account for all bytes
  of a vectored send.

- **state_sync + Extended Protocol pipeline deadlock**
  (`src/engine/engine_flow.c`):
  When a client sent an Extended Protocol pipeline (Parse → Bind → Execute →
  Sync) and the Parse triggered a state-sync (e.g. because the new backend
  needed `SET search_path` replayed before the prepared statement), KEEL
  computed `resume = KEEL_FLOW_OK` (Parse is non-terminal) and re-armed only the
  frontend receiver.  Backend responses were never read, causing KEEL to deadlock
  for the full `statement_timeout` (15 s on JDBC test connections).
  Fix: after draining and forwarding the full pipeline buffer via
  `captured_fe_pin_effects(setup_follow_buf)`, if the buffer contains a `Sync`
  message (`pin_clear & KEEL_FPIN_EXTENDED_PROTO`), override
  `resume = KEEL_FLOW_WAIT_BACKEND` so the backend receiver is armed and the
  `ReadyForQuery` response is read.

**Torture suite test infrastructure**

- **test_i4_pgx / test_i7_prisma missing `cwd`** (`tests/suites/suite_torture.py`):
  All `_run()` calls in `test_i4_pgx` and `test_i7_prisma` were missing
  `cwd=str(tdp)`.  Without it, `go mod init`, `go get`, `go run .`, `npm install`,
  `npx prisma generate`, and `node test.js` all ran in `/keel` (the read-only
  bind-mount of the source tree) rather than in the temp directory where the test
  files were written.  `go mod init` failed immediately with
  `open /keel/go.mod: read-only file system`; `npm install` found no `package.json`.
  Both tests always skipped regardless of whether Go or Node.js were installed.

- **Prisma schema missing model** (`tests/suites/suite_torture.py`):
  Prisma v5 requires at least one model in `schema.prisma` to generate a working
  `@prisma/client`, even for raw-query-only usage (`$queryRawUnsafe`).  The test
  schema had no models; `prisma generate` exited 0 (printing a warning) but
  produced an uninitialised client stub that threw at runtime.  Added a minimal
  `KeelTest` model so `prisma generate` produces a fully usable client.

- Added `mode = smart` to `docker/keel/keel-torture.ini` — required for SSV
  (session state virtualization) tests i34 and beyond.

### Added
- Fuzzing CI workflow (`.github/workflows/fuzzing.yml`): libFuzzer + ASan/UBSan
  on every PR for `test_fuzz_harness`, `test_admin_sql_fuzz`, `test_sm_fuzz`,
  `test_cluster_fuzz`; nightly 10-minute campaign; `KEEL_ENABLE_FUZZ` CMake option
- OSS-Fuzz enrollment files (`oss-fuzz/`) for continuous fuzzing via Google infrastructure
- Compatibility matrix in `README.md`: PostgreSQL 14–17, MySQL 8.0/8.4/9, kernel
  requirements (io_uring ≥ 5.6, epoll fallback ≥ 5.4), OS/arch support table
- Pre-built Grafana dashboard (`monitoring/keel-grafana.json`) — 16 panels covering
  pool health, latency P50/P95/P99, QPS, routing, FD/RSS, TLS, cluster HA, tracing
- Prometheus alerting + recording rules (`monitoring/keel-rules.yml`) — 13 alerts,
  8 recording rules using verified metric names
- Operations guide (`docs/OPERATIONS.md`) — zero-downtime upgrade (Helm rolling,
  systemd binary replace, HA cluster rolling), SIGHUP reload semantics,
  graceful drain (`shutdown_timeout_ms`), per-transaction-state drain table,
  backend drain via admin console, TLS hot-swap, troubleshooting
- Operator admission webhook (`operator/api/v1alpha1/keelpool_webhook.go`) —
  MutatingWebhookConfiguration (defaulter) and ValidatingWebhookConfiguration (validator)
- Operator CI workflow (`.github/workflows/operator.yml`) with unit, Helm lint,
  and kind-based E2E jobs
- Helm `webhook.yaml` template and `webhook.*` values stanza
- `BENCHMARKS.md` — throughput and latency comparison vs PgBouncer 1.22

### Changed
- Production-status documentation now separates `v0.2-alpha` features into
  `Production candidate`, `Hardening`, `Experimental`, and `Aspirational`
  buckets across `README.md`, `docs/PRODUCTION_READINESS.md`, and config examples
- Recommended production deployment is now documented consistently as
  PostgreSQL `pool` mode with `prepared_statement = virtualize` and
  `experimental_features = false`
- Config examples now visibly mark experimental profiles and require explicit
  opt-in; sharding, scatter-merge, WAL/GTID capture, and multi-proxy
  clustering examples are no longer presented as default production candidates

---

## [alpha-0.3.0] — 2026-05-07

### Added
- Epoll reactor: fixed three bugs — timeout dispatch, stats sync, and
  `EPOLL_CTL_DEL`/re-add race condition (`src/arch/linux/reactor_epoll.c`)
- io_uring → epoll automatic fallback when `io_uring_setup` returns `EPERM`
  (Docker / unprivileged containers); no config change required
- GitHub Actions bumped to Node 24 (`actions/checkout@v6`, `upload-artifact@v7`,
  `actions/setup-go@v6`)
- `integration-db` CI job: postgres:16 + mysql:8 service containers, runs
  `ctest -L integration`, `ctest -L protocol`, `./test_proxy_ssv_e2e`
- `sanitizer-msan` CI job: clang, `-DKEEL_ENABLE_MSAN=ON`, `KEEL_USE_IOURING=OFF`
- Codecov coverage upload (`codecov/codecov-action@v5`) in coverage CI job
- Codecov and Hardening workflow badges in `README.md`

### Changed
- `CMakeLists.txt` `project(keel VERSION …)` is now the single source of truth
  for the version string (`0.3.0`); packaging scripts derive the version from CMake
- `CHANGELOG.md` restructured to strict Keep a Changelog format

### Fixed
- Epoll reactor: `KEEL_OP_TIMEOUT` completions were dispatched to the wrong handler
- Epoll reactor: stats counters were updated before the operation completed
- Epoll reactor: `EPOLL_CTL_DEL` followed immediately by `EPOLL_CTL_ADD` on the
  same FD could silently lose the new registration

---

## [0.3.0-dev] — 2026-05-06

> Internal development snapshot. Not released publicly.

### Added

**Core Engine**
- io_uring share-nothing reactor with per-worker rings, linked SQEs, registered
  FDs, batch sends, submit+wait merging, and zero-poll hot path
- Full dual-protocol support: PostgreSQL v3 wire protocol and MySQL client/server
  protocol from a single binary (MySQL 9, PXC, MariaDB Galera, Group Replication)
- MySQL protocol at full feature parity: 64-slot PS session map, `get_stmt_replay`,
  `SESSION_TRACK_SCHEMA`/`GTIDS`, cross-service RYW, XA distributed-transaction
  pinning (340 protocol assertions)
- Connection multiplexing with `SO_REUSEPORT` kernel-level worker distribution
- Async pool warmup, auto FD limit (up to 1 M), crash signal handlers

**SQL & Routing**
- Full SQL lexer, parser, and query tree (AST)
- Automatic read/write splitting with weighted load balancing
- Transaction pinning, `FOR UPDATE` detection, sticky-primary override (100 ms)
- CTE / Window / Set-operation / MERGE / LOCK / LISTEN / VACUUM support
- Per-worker 1 024-entry XXHash64 route cache with LRU eviction
- Pluggable router plugin API (`keel_router_plugin_ops_t`)
- Horizontal sharding — transparent shard-key extraction from SQL AST, modulo +
  xxhash64 strategies, range-based shard map, scatter fan-out with aggregation,
  multi-shard transaction coordinator, connection pool per shard, config hot-reload,
  admin virtual tables, Prometheus counters, query timeout
- Declarative query rules — INI `[query_rule.*]` with POSIX ERE, route override,
  hard block, query rewrite; SIGHUP hot-reload
- OSC-aware proxying — gh-ost and pt-osc shadow-table detection; session
  primary-pin for migration duration
- NOTIFY/LISTEN proxying — full pub/sub through a transaction-mode pool
- Cross-service Read-Your-Writes — `SET keel.read_after_lsn` / `SHOW keel.write_lsn`

**Session & State Management**
- Session-context preservation across backend reassignment (sorted K/V state
  profiles, XXHash64 fingerprinting, 5-tier pool borrow)
- SSV (Semantic State Virtualization) — atom-layer consistency model for prepared
  statements and GUC session state
- Prepared statement pooling: virtualize, pinning, tracking, and anonymous strategies
- XID probe + commit-in-doubt recovery
- Read-after-write consistency via WAL LSN tokens
- Four runtime mode tiers (`PROXY` / `POOL` / `SMART` / `FULL`)
- 12×12 cross-feature invariant model with runtime checker
- Formal 9-domain session state machine with transition matrices

**Security & TLS**
- TLS + mTLS + kTLS (frontend termination, backend TLS, client-cert verification,
  kernel TLS acceleration, cipher enforcement, cert hot-reload, downgrade protection)
- Seccomp BPF syscall filter (baseline and strict profiles)
- Privilege drop after bind; binary hardening (PIE, full RELRO, NX, stack canary)
- Structured NDJSON audit logging with event-type filtering
- Ephemeral test PKI via `scripts/generate-test-certs.sh`

**Authentication**
- SCRAM-SHA-256, MD5 (PostgreSQL); `caching_sha2_password`, `mysql_native_password` (MySQL)
- Trust, password, user-file auth
- Cloud-native auth: AWS RDS IAM (SigV4, 14-min cache), GCP Cloud SQL IAM, Azure AD/Entra IMDS
- Enterprise auth: PAM, LDAP (with session-level result caching), mTLS certificate
  identity, auth query
- Pluggable `keel_auth_provider_ops_t` vtable

**Health & Failover**
- Pluggable probe system: postgres (SQL), patroni (HTTP REST), mysql, tcp, exec
- Automatic role detection, Patroni cluster discovery, failover handling, WAL position tracking

**Observability & Admin**
- Admin console (PostgreSQL wire protocol) with 21+ commands and SQL-syntax virtual table DML
- `keel-cli` standalone binary
- Prometheus `/metrics`, latency histograms (P50/P95/P99), Kubernetes health
  endpoints (`/healthz`, `/readyz`, `/livez`)
- Pluggable log backend (stdout, file, syslog), structured NDJSON logging, query logging
- Distributed tracing (OpenTelemetry) — W3C traceparent injection, OTLP/HTTP
  export, lock-free span ring buffer
- Embedded Web Management UI (`GET /ui`) + JSON Status API (`GET /api/status.json`)
- Per-rule query throttling (token-bucket rate limiting via `[throttle.N]` INI sections)

**Extensibility**
- Hook/trigger system with 4 pipeline extension points
- Lua 5.4/LuaJIT, CPython 3.x, and native `.so`/`.dylib` plugin hooks
- Priority chains, mutable context (session, query, routing, flags)

**Memory Architecture**
- Arena, slab, pool, ring buffer, and NUMA-aware allocators
- Lazy I/O buffers (300× lower idle memory vs. embedded 64 KB arrays)

**Operations & Deployment**
- Live configuration reload via SIGHUP (pools, timeouts, weights, TLS, shard rules,
  query rules, throttle, audit)
- Graceful drain/shutdown lifecycle state machine (`shutdown_timeout_ms`)
- Connection lifecycle management (max age, per-user/db quotas, idle eviction, pool prefill)
- Multi-proxy HA cluster with heartbeat, config gossip, transitive peer discovery
- Cluster wire-protocol compression (zlib/zstd) for WAN/cross-region deployments
- Kubernetes native: Helm chart (`helm/keel/`) + Go controller-runtime operator with `KeelPool` CRD
- Multi-arch Docker images (`linux/amd64`, `linux/arm64`) at `ghcr.io/virtlabs-io/keel`
- DEB, RPM, and TGZ release packages with man pages, systemd unit, logrotate config
- GitHub Actions CI/CD (build+test, sanitizer matrix, hardening schedule, automated packaging on tags)

**Testing**
- 90+ unit, integration, combinatorial, and fuzz tests
- 9 Docker Compose E2E stacks
- Fuzz harnesses (protocol fuzzer, state machine AFL++/libfuzzer)
- Concurrent stress tests, OOM injection, sanitizer matrix (ASan+UBSan, TSan, MSan)

### Changed
- Replaced `alpha-0.1` tag; this release marks the first public beta

### Fixed
- `TESTING.md` table formatting: blank line between rows 50 and 51 broke GitHub
  Markdown rendering

---

## [alpha-0.1] — 2026-03-25

Initial internal alpha release. Core engine, PostgreSQL pooling, basic routing, TLS.

---

[Unreleased]: https://github.com/virtlabs-io/keel/compare/alpha-0.3.0...HEAD
[alpha-0.3.0]: https://github.com/virtlabs-io/keel/compare/alpha-0.1...alpha-0.3.0
[0.3.0-dev]: https://github.com/virtlabs-io/keel/compare/alpha-0.1...alpha-0.3.0
[alpha-0.1]: https://github.com/virtlabs-io/keel/releases/tag/alpha-0.1
