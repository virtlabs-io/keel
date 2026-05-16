# KEEL Testing Guide

This document describes how to build, test, and benchmark the KEEL (Database Proxy) project — covering unit tests, Docker integration tests for every supported database topology, pgbench and sysbench stress tests, hardening/sanitizer runs, and comprehensive troubleshooting.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Building the Project](#building-the-project)
3. [Unit Tests](#unit-tests)
4. [Docker Integration Tests](#docker-integration-tests)
   - [PostgreSQL — Streaming Replication](#1-postgresql--streaming-replication)
   - [PostgreSQL — Patroni + etcd HA](#2-postgresql--patroni--etcd-ha)
   - [PostgreSQL — Full E2E with pgbench](#3-postgresql--full-e2e-with-pgbench)
   - [PostgreSQL — Horizontal Sharding](#4-postgresql--horizontal-sharding)
   - [Cloud Auth E2E](#5-cloud-auth-e2e)
   - [MySQL — Streaming Replication](#6-mysql--streaming-replication)
   - [MySQL — Group Replication](#7-mysql--group-replication)
   - [MySQL — MariaDB Galera](#8-mysql--mariadb-galera)
   - [MySQL — Percona XtraDB Cluster (PXC)](#9-mysql--percona-xtradb-cluster-pxc)
5. [Running pgbench Stress Tests](#running-pgbench-stress-tests)
6. [Running sysbench Stress Tests](#running-sysbench-stress-tests)
7. [Manual Testing](#manual-testing)
8. [Interpreting Results](#interpreting-results)
9. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Build Requirements

- **CMake** 3.25+
- **GCC 13+** or **Clang 17+** with C23 support
- **OpenSSL** (required — for SCRAM-SHA-256 and TLS/mTLS)
- **liburing-dev** (recommended on Linux — enables io_uring reactor)

#### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake gcc-13 \
    pkg-config liburing-dev libssl-dev
```

#### Fedora/RHEL

```bash
sudo dnf install cmake gcc openssl-devel liburing-devel
```

#### macOS

```bash
brew install cmake openssl
# io_uring not available — kqueue is used automatically
```

### Docker Requirements (for E2E tests)

- **Docker** 20.10+
- **Docker Compose** v2+

---

## Building the Project

### Standard Build

```bash
# Create build directory
mkdir -p build && cd build

# Configure with CMake
cmake .. -DCMAKE_BUILD_TYPE=Debug

# Build
make -j$(nproc)
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `KEEL_ENABLE_TESTS` | `ON` | Build unit tests |
| `KEEL_USE_IOURING` | `ON` (Linux) | Enable io_uring reactor |
| `KEEL_USE_EPOLL` | `ON` (Linux) | Enable epoll reactor (fallback) |
| `KEEL_USE_KQUEUE` | `ON` (macOS) | Enable kqueue reactor |
| `KEEL_ENABLE_ASAN` | `OFF` | AddressSanitizer |
| `KEEL_ENABLE_TSAN` | `OFF` | ThreadSanitizer |
| `KEEL_ENABLE_UBSAN` | `OFF` | UndefinedBehaviorSanitizer |
| `KEEL_ENABLE_MSAN` | `OFF` | MemorySanitizer |
| `KEEL_ENABLE_COVERAGE` | `OFF` | Code coverage |
| `KEEL_ENABLE_LUA` | `ON` | Build Lua hook runtime |
| `KEEL_ENABLE_PYTHON` | `ON` | Build Python hook runtime |
| `CMAKE_BUILD_TYPE` | `Release` | `Debug`, `Release`, `RelWithDebInfo` |

### Verify Build

```bash
# Check the binary exists
ls -la build/src/main/keel

# List registered tests
cd build && ctest -N
```

---

## Unit Tests

KEEL currently includes **106 test suites** covering core components, protocol behavior, hardening checks, TLS/kTLS regressions, TLS security enforcement, graceful drain/shutdown, combinatorial matrix tests, formal invariant models, runtime mode tier gating, live configuration reload infrastructure, formal state machine verification (exhaustive walks, fuzz, concurrent stress), route cache correctness and adversarial collision stress, admission control, SQL admin query language, cloud-native authentication (AWS SigV4, GCP OAuth2, Azure IMDS), white-box provider internals, query result caching, comprehensive horizontal sharding (Phases 1–6: key extraction, bound parameters, unified plan API, shard rule registry, scatter fan-out), **scatter-merge aggregation engine** (end-to-end merge pipeline, prepared-statement scatter, GROUP BY + LIMIT correctness, COUNT DISTINCT, 2PC full commit/abort/crash-recovery matrix, deterministic GIDs, 64-shard boundary), **scatter Prometheus histogram** (bucket accuracy, PromQL output), **scatter SQL fuzz** (134-assertion randomized aggregate SQL generator), OOM/allocation-failure injection (systematic fault coverage via `keel_mem_set_fail_countdown()`), split-at-every-byte protocol generator testing (all PG and MySQL wire messages), connection pool exhaustion and wait-queue verification, prepared-statement replay × failover × session-hash stability (PS/failover/TLS matrix), NOTIFY/LISTEN transparent proxying, declarative INI-based query routing and rewriting rules, Online Schema Change connection affinity, cross-service Read-Your-Writes via GUC propagation, embedded web management UI (HTML/CSS/JS structure, JSON API, security hardening), plus deep unit coverage of the probe subsystem, growable byte-buffers, string hashing, encoding helpers, NUMA topology, the memory-safety layer, the I/O reactor, and the log-plugin system.

### Running All Unit Tests

```bash
cd build

# Using CTest
ctest --output-on-failure

# Verbose output
ctest -V
```

### Running Specific Tests

```bash
# List available tests
ctest -N

# Run a specific test
ctest -R test_parser --output-on-failure

# Run tests matching a pattern
ctest -R "test_mem|test_auth" --output-on-failure
```

### Test Suites (Representative)

The table below lists the primary and most frequently referenced suites.
Use `ctest -N` to see the full, current list in your build directory.

| # | Test | Description |
|---|------|-------------|
| 1 | `test_mem` | Memory allocators (arena, slab, pool) |
| 2 | `test_auth` | Authentication (SCRAM-SHA-256, MD5) |
| 3 | `test_parser` | SQL parser & lexer |
| 4 | `test_router` | Query routing & load balancing |
| 5 | `test_router_plugin` | Router plugin system |
| 6 | `test_util` | Utility functions (hash, buffer, string) |
| 7 | `test_sql` | SQL analysis & query classification |
| 8 | `test_config` | INI configuration parser |
| 9 | `test_config_reload` | Config reload infrastructure: INI diff, parameter classification (safe vs restart-required), server weight re-parse, probe timing, pool sizing, timeout propagation, rebalancing, human-readable durations, no-change detection, edge cases (151 assertions) |
| 10 | `test_log` | Logging subsystem |
| 11 | `test_string_view` | String view operations |
| 12 | `test_ringbuf` | Ring buffer |
| 13 | `test_xxhash` | Hash functions (xxhash) |
| 14 | `test_residual` | Residual data handling |
| 15 | `test_session_engine` | Session engine lifecycle, including transaction-tracking commit rewrite and commit-in-doubt recovery |
| 16 | `test_pool_correctness` | Pool borrow/return correctness and connection reuse |
| 17 | `test_plugin_contract` | Plugin contract verification (router/hook ABI) |
| 18 | `test_pg_protocol_flow` | PostgreSQL wire-protocol flow: parse/bind/execute/sync, all four PS pooling modes (virtualize/pinning/tracking/anonymous), replication tracking XID capture, commit-in-doubt synthesis |
| 19 | `test_mysql_protocol_flow` | MySQL wire-protocol flow: COM_QUERY classification (SELECT/INSERT/UPDATE/DELETE/DDL/BEGIN/COMMIT/SET/CALL/DO/XA), COM_STMT_PREPARE/EXECUTE/CLOSE/RESET, prepared-statement tracking (64-slot session map, FNV-1a session hash, `get_stmt_replay` replay buffer), SESSION_TRACK_SCHEMA/GTIDS capture in OK packets, cross-service RYW intercepts (`SET`/`SELECT @keel_write_gtid`), `notify_write_lsn` hook, XA distributed-transaction pinning, transaction-pin/unpin lifecycle, LOAD DATA, multi-result-set, SSV session-track, admin commands, error classification (340 assertions) |
| 20 | `test_hooks` | Hook system (register, fire, abort, priority ordering, stats) |
| 21 | `test_failover` | Failover detection and probe-driven reconnect |
| 22 | `test_query_log` | Per-query logging (format, rotation, flush) |
| 23 | `test_migration` | Worker connection migration (lifecycle, SCM_RIGHTS FD transfer, eligibility, inbox-full, residual blocking) |
| 24 | `test_tls_ktls` | TLS handshake/data path and kTLS activation/fallback accounting |
| 25 | `test_tls_security` | TLS cipher policy enforcement, cert reload, downgrade protection, mTLS peer info, version enforcement, engine drain API (12 tests, 79 assertions) |
| 26 | `test_drain_shutdown` | Graceful drain/shutdown lifecycle: state machine (CREATED→ACTIVE→DRAINING→STOPPING→STOPPED), force-close with CID protection, PG FATAL 57P03 on drain rejection, live engine drain+stop, timeout force-close (10 tests, 57 assertions) |
| 27 | `test_pool_audit` | Pool allocator metadata integrity, canary validation, SIGUSR1 diagnostics |
| 28 | `test_fd_tracking` | File descriptor tracking, leak detection |
| 29 | `test_state_machine` | Session state machine transitions, invariant enforcement |
| 30 | `test_dirty_connection` | Dirty connection cleanup (DISCARD ALL), non-blocking drain |
| 31 | `test_concurrency_stress` | Thundering herd, TIME_WAIT pressure, RLIMIT_MEMLOCK checks, multi-worker stress |
| 32 | `test_fuzz_harness` | Protocol fuzz harness: malformed packets, truncated messages, random payloads |
| 33 | `test_shadow_diff_harness` | Shadow diff testing against direct PostgreSQL |
| 34 | `test_runtime_security_harness` | Seccomp BPF, privilege drop, syscall filtering |
| 35 | `test_runtime_security_strict_harness` | Strict seccomp (opt-in, minimal syscall set) |
| 36 | `test_stats_alignment` | Stats structure alignment and atomic counter layout |
| 37 | `test_ps_pool_matrix` | Combinatorial: 4 PS modes × pool states × concurrency levels (729 assertions) |
| 38 | `test_tls_splice_matrix` | Combinatorial: TLS modes × splice paths × kTLS states |
| 39 | `test_session_hooks_matrix` | Combinatorial: session states × hook points × abort/continue |
| 40 | `test_crash_recovery_matrix` | Combinatorial: crash points × recovery modes × txn states |
| 41 | `test_dual_protocol_matrix` | Combinatorial: PG × MySQL protocol interactions |
| 42 | `test_invariant_model` | Formal invariant model: 12×12 compatibility matrix symmetry, pin conflict detection, 20 session violation classes, pool counter invariants, clean-session happy path, live protocol flow (6 sections) |
| 43 | `test_runtime_mode` | Runtime mode tiers: tier parsing (case-insensitive), ordering (PROXY<POOL<SMART<FULL), 8 gate macros × 4 tiers, name round-trip, PROXY overrides (PS_MODE_OFF + txn_tracking=false), default tier (78 assertions) |
| 44 | `test_state_context` | Session-context preservation: SET parameter persistence, diff generation, sync SQL correctness, multi-parameter compose, RESET, profile round-trip, cross-transaction state continuity (55 assertions) |
| 45 | `test_state_contracts` | Contract-driven state model: derived enums, transition matrices, contract sync, invariant checks, event journal, derived predicates (218 assertions, 15 test sections) |
| 46 | `test_sm_sequence_walk` | State machine exhaustive walk: DFS of all phase/replay/CID transition matrices, illegal edge rejection (phase 17, replay 24, CID 66), combined lifecycle walks (289 assertions, 14 tests) |
| 47 | `test_sm_fuzz` | State machine fuzz harness: AFL++/libfuzzer compatible, random byte-pair commands across all 9 state domains, contract checked after every transition (2304+ inputs, 12 deterministic scenarios) |
| 48 | `test_sm_stress` | State machine concurrent stress: 64-thread independent lifecycles, journal ring-buffer stress, contract derivation storm, thundering-herd phase + lifecycle (5 tests) |
| 49 | `test_route_cache` | Route cache correctness: init/flush, insert/lookup, collision probing, LRU eviction, stats accuracy, NULL safety, 10K stress (13 tests) |
| 50 | `test_admission` | Admission control: frontend/backend limits, wait queue depth, queue-full rejection, timeout, peak tracking, combined pressure, rapid 10K cycles, load factor, NULL safety (24 tests) |
| 51 | `test_admin_sql` | Admin SQL query language: SELECT/UPDATE/INSERT/DELETE parsing for virtual tables, edge cases, IPv6 peer addresses (26 tests, 58 assertions) |
| 52 | `test_query_cache` | Query result caching: LRU cache, TTL expiry, concurrent access, digest computation, normalization, non-cacheable detection (14 tests, 110 assertions) |
| 53 | `test_cloud_auth` | Cloud-native authentication: AWS SigV4, GCP OAuth2, Azure IMDS, static providers, token cache lifecycle, refresh, format validation (27 tests) |
| 54 | `test_cloud_auth_internals` | Cloud auth internals: white-box testing of static helpers, hex encoding, base64url encoding, JSON parsing, HMAC-SHA256, JWT generation, KMS parsing (12 tests, 41 assertions) |
| 55 | `test_cloud_auth_e2e` | Cloud auth E2E: cache lifecycle, provider fallback, concurrent access, token expiry detection (8 tests, 35 assertions) |
| 56 | `test_conn_lifecycle` | Connection lifecycle management |
| 57 | `test_proxy_ssv_e2e` | Proxy-level E2E semantic state virtualization |
| 58 | `test_sharding` | Horizontal sharding — Phases 1–6: shard-key extraction from AST (SELECT/INSERT/UPDATE/DELETE), literal and `$N` bound-parameter detection, deterministic shard mapping (int64 modulo, xxhash64 string), explicit shard-aware router path, bound-parameter resolution, unified `keel_shard_plan_t` routing plan API, shard rule registry with case-insensitive CRUD, `keel_router_plan_sql()` auto-dispatch across all registered rules, scatter fan-out `keel_router_scatter_servers()` with per-shard read/write routing (48 tests, 157 assertions) |
| 59 | `test_connpool` | Connection pool — `keel_connpool_t` and `keel_connpool_registry_t`: create/destroy, acquire on unreachable host, release (reusable vs. close), stats accounting (borrows/creates/destroys), idle eviction, health-check probing, warm pre-connect, registry lifecycle and per-server lookup, null-guard coverage (34 assertions) |
| 60 | `test_proxy_session` | Client session — `keel_client_session_t`: create/destroy, null-router guard, routing-state access, tx begin/end lifecycle, pin/unpin backend, dispatch, record_scatter_write, end-tx-clears-scatter, null-guard coverage (30 assertions) |
| 61 | `test_shard_hot_reload` | Config hot-reload for shard rules — `keel_config_reload_shard_rules()`: adds rules from INI, changes shard count, skips invalid strategies, range strategy builds thresholds, empty config is a no-op, null guards, multiple rules in one reload (27 assertions) |
| 62 | `test_router_metrics` | Prometheus router metrics — `keel_router_write_prometheus()`: non-empty output, all 7 metric families present, `# HELP`/`# TYPE` lines emitted, no per-shard lines when zero, counters updated after dispatch, 4-byte buffer returns 0, null guards, per-shard line emitted after dispatch (32 assertions) |
| 63 | `test_router_timeout` | Query timeout enforcement — `keel_router_dispatch_sql_timed()`: zero timeout → no enforcement, generous timeout → pass-through, 1 ns timeout fires (or pass-through if unsupported), config default respected, 1 ns config fires, null guards, output zeroed on timeout (14 assertions) |
| 64 | `test_router_stress` | Router fuzz/stress — random SQL strings across all templates (no crash), concurrent dispatch storm (8 threads × 500 ops, one router per thread matching production architecture), timed dispatch storm, Prometheus buffer-boundary torture (all sizes 1→full), shard-rule churn (100 add/remove cycles), null/empty/long SQL tolerance |
| 65 | `test_connpool_stress` | Connpool multi-threaded stress — concurrent acquire/release on a real loopback acceptor (6 threads × 40 ops), idle eviction while acquires are in flight, registry concurrent get+evict (4 threads), stats consistency (hits+misses == borrows) |
| 66 | `test_graceful_restart` | Graceful restart lifecycle: in-flight connection handling, worker drain sequencing, SIGHUP vs SIGTERM paths, restart without dropped client connections |
| 67 | `test_tls_auto` | TLS auto-negotiation: clientless SSL probe detection, SSLRequest handling, auto-upgrade from plaintext to TLS when client probes, mixed TLS/plaintext listener |
| 68 | `test_cluster` | Cluster management: multi-backend topology updates, primary/replica role transitions, server add/remove under load, Patroni REST health integration, cluster state convergence |
| 69 | `test_cancel_forwarding` | PostgreSQL cancel-request forwarding: extract `BackendKeyData` PID + secret, proxy `CancelRequest` to correct backend, cancel for unknown PID is safe no-op, cancel during active query |
| 70 | `test_trace` | Tracing subsystem: span creation, parent-child relationships, attribute tagging, sampling policy, flush/export lifecycle, zero-overhead disabled-trace fast path |
| 71 | `test_admin_auth` | Admin authentication: password-protected admin listener, reject wrong credential, accept correct credential, session isolation from client traffic |
| 72 | `test_histogram` | Histogram statistics: bucket boundaries, increment/reset, percentile computation (P50/P95/P99), merge across workers, serialization to Prometheus text format |
| 73 | `test_otlp_encoding` | OpenTelemetry OTLP encoding: protobuf metric serialization, span encoding, attribute value types (int/double/string/bool), batch size limits, wire-format correctness |
| 74 | `test_ndjson_log` | NDJSON log format: structured field emission, log-level filtering, timestamp format, field escaping, multi-line suppression, log-plugin round-trip |
| 75 | `test_ssv_core` | SSV (Semantic State Virtualization) core: session-state atom lifecycle, hash-bucket pool index O(1) matching, OPAQUE/CONFIG domain split, WAL LSN integration, semantic replay ordering |
| 76 | `test_ssv_atom` | SSV atom layer: individual atom create/update/compare, domain classification, diff generation between atom sets, conflict detection, merge semantics, serialization round-trip |
| 77 | `test_alloc_inject` | Allocation-failure injection (OOM): `keel_mem_set_fail_countdown()` systematically injects NULL returns into router, connpool, registry, session, and config subsystems; staircase N-th-failure scan; fail-then-recover state validation (15 assertions) |
| 78 | `test_connpool_exhaust` | Connection pool exhaustion and wait-queue: timeout on full pool returns `KEEL_ERR_POOL_TIMEOUT`, `stats.timeouts` increments on each timeout, release unblocks a waiting acquirer, pool consistent after timeout, evict-idle on all-active, single-conn serialization, timeout precision (2× window), non-reusable release frees slot, 16-thread timeout storm — all get `KEEL_ERR_POOL_TIMEOUT`, no crash (36 assertions) |
| 79 | `test_route_cache_stress` | Route cache adversarial stress: basic insert/lookup round-trip, miss on unknown query, flush clears entries, update changes route_type, probe-chain overflow triggers LRU eviction, 10K round-trips hit rate > 50%, hits+misses == total lookups invariant, null and empty-string guards, 4096-entry single-bucket adversarial workload, 4-thread × 50K concurrent read-only (21 assertions) |
| 80 | `test_proto_split` | Split-at-every-byte protocol generator: for each PG and MySQL wire message, feeds bytes 1…N−1 via `frame_len()` confirming need-more semantics; feeds full message confirming correct frame size; covers PG startup, Q, P, B, E, H, S, X, Z, R, ParameterStatus, CommandComplete, MySQL COM_QUERY, HandshakeResponse41, and zero/NULL guards (26 assertions) |
| 81 | `test_ps_failover_tls` | Prepared-statement replay × failover × GUC session-hash stability: fresh-ctx baseline, TRACKING mode PREPARE populates replay buf, stmt_count == PREPARE count, each stmt name in replay buf, hash stable across two replay calls, identical stmts → identical hash on two contexts, DEALLOCATE ALL resets hash, hash-only probe (NULL buf/len/count), every replay frame starts with 'P', replay roundtrip into fresh ctx verifies matching hash, GUC SET TimeZone changes and restores hash, PINNING/ANONYMOUS/OFF modes return empty replay (56 assertions) |
| 82 | `test_listen_notify` | NOTIFY/LISTEN transparent proxying: LISTEN registration, NOTIFY forwarding to all subscribers, UNLISTEN deregistration, payload delivery, multi-client fan-out, backend notification multiplexing |
| 83 | `test_query_rules` | Declarative query rules: INI-based routing/blocking/rewriting rules, regex matching, route override (primary/replica/any), hard block enforcement, query rewriting with capture groups, rule priority ordering, config hot-reload (74 assertions) |
| 84 | `test_osc_proxying` | Online Schema Change connection affinity: gh-ost/pt-osc/Flyway/Liquibase detection, primary-pin on OSC start, affinity release on session close, non-OSC sessions unaffected, concurrent OSC + normal session isolation (29 assertions) |
| 85 | `test_ryw_propagation` | Cross-service Read-Your-Writes via GUC: `SET keel.read_after_lsn` injection into session atoms, `SHOW keel.write_lsn` interception, LSN gate enforcement on replica dispatch, cross-service propagation round-trip, no backend round-trip for GUC intercept (64 assertions) |
| 86 | `test_web_ui` | Embedded web management UI: HTML structural integrity, dark-theme SPA JavaScript behavior, JSON status API body structure and values, route matching (`GET /ui`, `GET /api/status.json`), security hardening (no `eval`, no external scripts, no inline event handlers), auto-refresh polling logic (91 assertions) |
| 87 | `test_probe` | Probe subsystem — `keel_probe_t` lifecycle: registry create/register/lookup/duplicate-reject/NULL-guard, `register_builtins` (postgres/patroni/mysql/mariadb), health enum mapping, mock vtable open/check/close/destroy, config defaults, server-state atomics, concurrent atomic access (200 threads), TCP connect error path, capacity exhaustion (64 assertions) |
| 88 | `test_buffer` | Growable byte-buffer — `keel_buffer_t`: create/destroy, single/multi-segment appends, in-place read, read into external buf, reset, auto-grow beyond initial capacity, peek/consume, large-stress (256 × 4 KB), NULL-guard coverage (14 sections, all assertions pass) |
| 89 | `test_string_hash` | String hashing and `keel_str_t` — FNV-1a and xxhash32/64 stability, empty/NULL/single-char edge cases, hash distribution across 10 000 unique keys (collision rate < 1%), `keel_str_t` equality, hash-map stress (1 000 inserts + 1 000 lookups with no collisions), NULL-guard coverage |
| 90 | `test_encoding` | Encoding helpers: `keel_hex_encode` (empty, single byte, full block, large 64-byte input, uppercase output), `keel_json_escape` (plain text passthrough, all special characters including both `\b`/`\u0008` and `\f`/`\u000c` forms, Unicode code points, long string, NULL-guard) |
| 91 | `test_numa` | NUMA topology discovery and NUMA-aware allocation: `keel_numa_init`, node count (≥ 1), CPU-to-node mapping, `keel_numa_alloc`/`keel_numa_free` round-trip, NULL-guard on free, NUMA-aware memset/memcpy, `keel_numa_migrate` (accepts `KEEL_OK`, `KEEL_ERR_NOT_SUPPORTED`, or `KEEL_ERR_INVALID_ARG` on non-NUMA systems), topology struct validity |
| 92 | `test_mem_safety` | Memory safety layer — `keel_safe_alloc`/`keel_safe_free`: alloc/free lifecycle, NULL-arg guards, zero-size allocation, large allocation (1 MB), head-canary validation, tail-canary validation (detects write past end), freed-pointer state check (validates returns non-OK on already-freed ptr), leak report (zero leaks after balanced alloc/free), stress (10 000 allocs × 5 sizes, all validated, all freed), double-free detection via leak report (20 193 assertions total) |
| 93 | `test_reactor` | I/O reactor (io_uring / epoll) — `keel_reactor_t`: create AUTO/EPOLL/IOURING, get type, tick empty reactor, write-readability via socketpair, recv callback, multiple concurrent fds (3-way socketpair fan-out), 50 ms timeout fires (50 × 20 ms ticks), cancel in-flight op, stress (100 iterations × write + recv), create/destroy lifecycle leak check (44 assertions) |
| 94 | `test_log_plugins` | Log plugin system: stdout plugin create/open/write/close/destroy lifecycle, NULL-record guard (lifecycle-only, no crash), file plugin round-trip (write + verify file content), file plugin bad-path rejection (`/dev/null/…`), syslog plugin lifecycle, plugin level filtering (INFO passes, DEBUG filtered), concurrent writes (8 threads × 50 records per plugin, ≥ 400 writes), plugin replace/reload sequence (45 assertions) |

#### Scatter-Merge Test Suites (Phase 4)

| # | Test | Description |
|---|------|-------------|
| 95 | `test_scatter_merge_e2e` | Scatter-merge end-to-end pipeline — library-only and cluster-required tests: GROUP BY + SUM merge from 2 shards; COUNT/SUM scalar merge; prepared-statement scatter (PREPARE + EXECUTE on shard connections, 2 groups per shard, combined cnt/tot verified); prepared-statement param merge (param binding simulation, grp='A' cnt=4/tot=100); **GROUP BY + LIMIT correctness** (8 partial-group rows for A/B/C/D across 2 shards, DESC LIMIT 3 post-merge, D=260 at rank #1 — proves `sc_strip_limit_offset` prevents shard truncation) (14 tests) |
| 96 | `test_scatter_2pc` | 2PC coordinator full matrix — `keel_2pc_coord_*` API: happy-path prepare+commit; rollback-after-commit is `KEEL_ERR_INVALID_ARG` + state preserved; commit-after-rollback is `KEEL_ERR_INVALID_ARG`; partial-abort-then-rollback (s0=PREPARED, s1=ABORTED, s2=PREPARED — commit refused, PREPARE→ROLLED_BACK, ABORTED unchanged, second rollback → `KEEL_ERR_INVALID_ARG`); deterministic GIDs (same session+seq+mask → identical GIDs; different seq → different GIDs); 64-shard boundary (full mask, all valid GIDs, prepare+commit succeeds) (27 tests) |
| 97 | `test_scatter_sql_fuzz` | Scatter SQL fuzz harness — 134-assertion randomized aggregate SQL generator: random combinations of COUNT/SUM/AVG/MIN/MAX/COUNT DISTINCT × GROUP BY × HAVING × ORDER BY × LIMIT never crash the merge pipeline; malformed SQL strings accepted without panic; edge cases (0 rows from all shards, 1 shard returns error, max column count) |
| 98 | `test_scatter_store` | Scatter result store — `keel_scatter_result_t` lifecycle: init/destroy, feed rows, merge accumulation, total_rows accounting, shards_completed/shards_failed counters, null merge function (no-op), user_ctx threading, overflow guards |
| 99 | `test_router_metrics` (scatter extension) | Scatter-merge Prometheus histogram — `keel_router_record_scatter_merge_ns()` + `keel_router_write_prometheus()`: `keel_router_scatter_merge_duration_seconds` metric family present in output, correct bucket boundaries (1ms/5ms/10ms/25ms/50ms/100ms/250ms/500ms/1s/2.5s), count/sum updated after recording, `# HELP` and `# TYPE` lines emitted, null-guard coverage (2 new assertions added to existing test_router_metrics suite) |
| 100 | `test_scatter_chaos` (ASAN/TSan) | Chaos hardening — run via `tests/chaos/`: shard kill mid-scatter (SIGKILL one shard process after TCP connect), verify client receives error not partial result; random shard latency injection (sleep inside shard responder), verify SO_RCVTIMEO fires; concurrent scatter storms (8 goroutines × 100 scatter queries), zero ASAN/TSan violations; memory-leak clean (LSan build) |
| 101–106 | `test_scatter_hardening_*` | Hardening suite for scatter engine — syscall fault injection into `connect()`/`recv()`/`send()` paths during scatter; OOM injection at merge pipeline entry; partial-write recovery; `sc_strip_limit_offset` unit tests (strip with GROUP BY, no-op without GROUP BY, OFFSET-only strip, LIMIT+OFFSET combined, no clause → unchanged) |

### Hard Guarantee Gate

Use the one-command strict gate for build + tests + TLS regression:

```bash
# Standard: full build + ctest + explicit TLS/kTLS test
bash ./scripts/ci-hard-guarantee.sh

# Strict mode: fail if kernel TLS cannot be activated in this environment
KEEL_REQUIRE_KTLS=1 bash ./scripts/ci-hard-guarantee.sh
```

The strict mode is intended for hosts/runners where kTLS capability is a hard requirement.

### Test Certificates

Sample PKI material for local TLS/mTLS testing lives under `etc/certs/`.
See `etc/certs/README.md` for CA, frontend cert, backend cert, and client cert usage.

### Running Individual Test Binaries

```bash
# Run directly for more detailed output
./build/tests/test_router
./build/tests/test_parser
./build/tests/test_auth
```

### Sanitizer Builds

```bash
# AddressSanitizer (memory errors, buffer overflows, use-after-free)
cmake -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_ASAN=ON ..
make -j$(nproc) && ctest --output-on-failure

# ThreadSanitizer (data races)
cmake -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_TSAN=ON ..
make -j$(nproc) && ctest --output-on-failure

# UndefinedBehaviorSanitizer
cmake -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_UBSAN=ON ..
make -j$(nproc) && ctest --output-on-failure

# MemorySanitizer (uninitialized reads — requires clang)
cmake -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_MSAN=ON \
      -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ ..
make -j$(nproc) && ctest -LE openssl --output-on-failure
```

> **MSAN note:** OpenSSL is not built with MSAN instrumentation, so tests that
> exercise OpenSSL code paths (TLS handshake, SCRAM-SHA-256) produce false
> positives.  Use `-LE openssl` to exclude tests labelled `openssl` when running
> under MSAN.

### Sanitizer Matrix (Automated)

```bash
./scripts/hardening-sanitizers.sh
```

This runs:
- ASAN + UBSAN build and `ctest -L hardening`
- TSAN build and `ctest -L hardening`
- MSAN build and `ctest -LE openssl` (excludes OpenSSL-dependent tests)

### Running Tests with Valgrind

```bash
valgrind --leak-check=full --track-origins=yes ./build/tests/test_mem
```

---

## Python Test Framework

In addition to the C/CTest suite, KEEL ships a Python-based test framework under
`tests/suites/` that provides eight standalone test categories (A–H) and a
unified coordinator (`tests/run_tests.py`).

### Architecture

```
tests/
├── run_tests.py            # Master coordinator (all 14 suites, HTML+JSON reports)
├── suites/
│   ├── __init__.py         # SuiteRunner, SuiteResult, CaseResult base classes
│   ├── common.py           # ProxyConn, protocol builders, latency helpers
│   ├── suite_memory.py     # A: ASAN/LSAN/Valgrind + alloc-inject
│   ├── suite_concurrency.py# B: TSAN + stress binary repetition
│   ├── suite_throughput.py # C: TPS / latency baseline vs. live proxy
│   ├── suite_protocol.py   # D: Wire-protocol compliance, raw-TCP fuzzing
│   ├── suite_resilience.py # E: Fault injection, C-level error paths
│   ├── suite_regression.py # F: Read/write round-trip, txn correctness
│   ├── suite_chaos.py      # G: tc netem network jitter/loss/reorder
│   ├── suite_soak.py       # H: RSS / FD stability over sustained load
│   └── requirements.txt    # psycopg2-binary (optional: hypothesis)
└── e2e/
    ├── test_protocol_compliance.py  # pytest: wire-protocol (marker: protocol)
    ├── test_performance.py          # pytest: TPS/latency (marker: stress)
    └── test_resilience.py           # pytest: error recovery (markers: failover, chaos)
```

### Quick start (no Docker needed)

The memory, concurrency, protocol, and resilience suites work against the
host-compiled binaries only — no running proxy or database is required:

```bash
# Install Python dependencies
pip install psycopg2-binary

# Build with tests
cmake -S . -B build-host -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DKEEL_ENABLE_TESTS=ON
cmake --build build-host -j$(nproc)

# Run protocol and resilience suites
python3 tests/run_tests.py \
  --suite protocol \
  --suite resilience \
  --build-dir build-host \
  --py-verbose
```

### Master coordinator

`tests/run_tests.py` is the single entry point to run any combination of suites:

```bash
# List all 14 available suites
python3 tests/run_tests.py --list

# Run all Python standalone suites (host build, no Docker)
python3 tests/run_tests.py \
  --suite memory --suite concurrency --suite protocol --suite resilience \
  --build-dir build-host

# Live-proxy suites (proxy must be reachable at $KEEL_HOST:$KEEL_PORT)
python3 tests/run_tests.py \
  --suite throughput --suite regression \
  --bench-clients 20 --bench-duration 30

# Network chaos (requires root / sudo for tc netem)
sudo python3 tests/run_tests.py --suite chaos-py --chaos-iface eth0

# Soak test — 5 minutes, monitor proxy RSS + FD count
python3 tests/run_tests.py \
  --suite soak \
  --soak-duration 300 --soak-clients 10 \
  --proxy-pid "$(pgrep keel)"

# Full CI gate (unit + sanitizers + Python suites)
python3 tests/run_tests.py \
  --suite unit --suite memory --suite concurrency --suite protocol \
  --suite resilience \
  --build-dir build-asan \
  --ci
```

Reports (HTML + JSON) are written to `tests/reports/report_latest.{html,json}`.

### Running a suite standalone

Every suite can be run independently with its own `--help`:

```bash
python3 tests/suites/suite_protocol.py --list
python3 tests/suites/suite_protocol.py --filter "D05"   # run only test D05
python3 tests/suites/suite_memory.py   --verbose --json > mem.json
python3 tests/suites/suite_chaos.py    --iface lo
python3 tests/suites/suite_soak.py     --duration 120 --proxy-pid 12345
```

### Suite descriptions

| Suite key | Category | Description |
|-----------|----------|-------------|
| `memory`      | A | ASAN/LSAN CTest + Valgrind + alloc-inject binary |
| `concurrency` | B | TSAN CTest + stress binary repetition |
| `throughput`  | C | TPS/latency/large-resultset vs. live proxy |
| `protocol`    | D | 20 raw-TCP protocol compliance + fuzzing tests |
| `resilience`  | E | C-level fault injection + half-open flood + storm |
| `regression`  | F | 10 SQL correctness round-trips via psycopg2 |
| `chaos-py`    | G | tc netem: 50ms jitter, 5% loss, 1Mbit cap, reorder |
| `soak`        | H | RSS/FD stability + latency window drift over time |

### Environment variables for live-proxy suites

| Variable | Default | Description |
|----------|---------|-------------|
| `KEEL_HOST` | `127.0.0.1` | Proxy host |
| `KEEL_PORT` | `5432` | Proxy port |
| `KEEL_USER` | `postgres` | Database user |
| `KEEL_PASSWORD` | `postgres` | Database password |
| `KEEL_DATABASE` | `postgres` | Database name |
| `KEEL_BENCH_CLIENTS` | `10` | Concurrent clients (throughput suite) |
| `KEEL_BENCH_DURATION_S` | `10` | Seconds per sub-test (throughput suite) |
| `KEEL_BENCH_BASELINE` | — | Path to baseline JSON for regression comparison |
| `KEEL_SOAK_DURATION_S` | `60` | Soak test total duration |
| `KEEL_SOAK_CLIENTS` | `5` | Soak worker threads |
| `KEEL_PROXY_PID` | — | Proxy PID for RSS/FD monitoring in soak suite |
| `KEEL_CHAOS_IFACE` | `lo` | Network interface for tc netem |

---

## Code Coverage

Coverage reporting is built on **lcov** / **genhtml** (line + branch) with hard
thresholds enforced in CI: **line ≥ 70%**, **branch ≥ 40%**.

#### Local coverage build

```bash
cmake -S . -B build-coverage \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_STANDARD=23 \
  -DKEEL_ENABLE_TESTS=ON \
  -DKEEL_ENABLE_COVERAGE=ON

cmake --build build-coverage -j$(nproc)
ctest --test-dir build-coverage --output-on-failure -j$(nproc)

# Collect raw counters
lcov --capture \
     --directory build-coverage \
     --output-file coverage.info \
     --rc lcov_branch_coverage=1

# Strip system headers and test driver code
lcov --remove coverage.info '/usr/*' '*/tests/*' '*/build-coverage/*' \
     --output-file coverage-filtered.info \
     --rc lcov_branch_coverage=1

# Summary (includes branch coverage stats)
lcov --summary coverage-filtered.info --rc lcov_branch_coverage=1

# HTML report with per-line AND per-branch annotations
genhtml coverage-filtered.info \
  --output-directory coverage-html \
  --branch-coverage \
  --function-coverage \
  --legend \
  --title "Keel Coverage Report"
# Open coverage-html/index.html in a browser
```

The one-command version delegates to the coverage script which also produces a
"dark corners" ranking:

```bash
# Full build + test + report (enforces thresholds, prints dark corners):
bash scripts/coverage.sh

# Skip build if build-coverage already exists:
SKIP_BUILD=1 bash scripts/coverage.sh

# Override thresholds:
COVERAGE_LINE_MIN=75 COVERAGE_BRANCH_MIN=50 bash scripts/coverage.sh
```

#### Branch coverage and "Dark Corners"

Branch coverage (enabled via `--rc lcov_branch_coverage=1` and `--branch-coverage`
in genhtml) tells you which **decision outcomes** were never exercised — the true
measure of whether error-handling, boundary, and recovery paths are actually
reachable by tests.  `scripts/coverage.sh` also runs **gcovr** to produce a
ranked "dark corners" table:

```
Branch%   Cov/Tot    File
───────────────────────────────────────────────────────────────────
  12.5%     1/8    src/core/backend_resolver.c
  18.2%     2/11   src/core/migration.c
  23.1%     3/13   src/log/ndjson.c
  ...
```

Files near 0% branch coverage are the highest-priority targets for new tests.
The Cobertura XML (`coverage.xml`) is uploaded to Codecov for badge and
trend tracking on every PR.

#### CI coverage gate

The `coverage` job in `.github/workflows/ci.yml` runs on every pull request and
enforces **two** hard thresholds:

| Metric | Threshold |
|--------|-----------|
| Line coverage | ≥ 70% |
| Branch coverage | ≥ 40% |

Both thresholds must pass or the PR is blocked.  The job also:

- Uploads the full HTML report as a 14-day GitHub Actions artifact.
- Appends a "Dark Corners" branch-coverage table to the GitHub Step Summary.
- Uploads `coverage-filtered.info` to Codecov.

---

## Comprehensive Hardening Architecture

The production-hardening stack is layered and maps to the test suites/scripts in this repo.

### Phase 0 — Memory & Resource Integrity

- Unit tests: `test_pool_audit`, `test_fd_tracking`
- Pool allocator audit:
    - Every pool allocation stores allocation-site metadata (`file:line`) in a per-block header.
    - Pool blocks carry head/tail canaries and are validated on free.
    - `SIGUSR1` now dumps active pool blocks (allocation site + pointer) in addition to stats.
- Runtime check during load:
    - `kill -USR1 <keel_pid>` and inspect active-block dump + stats output.
    - Use `/proc/<pid>/fd` and `lsof -p <pid>` to verify FD stability.

### Phase 1 — Concurrency & Logic-Race Stress

- Unit tests: `test_concurrency_stress` (thundering herd, TIME_WAIT pressure, RLIMIT_MEMLOCK checks)
- Slow-client / fast-server backpressure harness:

```bash
./scripts/hardening-slow-client-fast-server.sh
```

This forces a very slow client read rate while streaming a large result through the proxy, exercising partial-write and backpressure paths.

### Phase 2 — Protocol & Session Isolation

- Unit tests: `test_dirty_connection`, `test_state_machine`, protocol flow tests
- Shadow validation / diff testing harness:

```bash
SQL_FILE=./bench/pgbench_read_rw_split.sql ./scripts/hardening-shadow-diff.sh
```

This executes the same SQL workload against direct DB and proxy and diffs normalized output.

### Phase 3 — Fault Injection (Jepsen-Lite)

```bash
./scripts/hardening-jepsen-lite.sh
```

Behavior:
- Runs a continuous mixed read/write workload through the proxy
- Randomly kills/restarts backend nodes
- Optionally injects proxy faults with `PROXY_FAULT_CMD`
- Fails if workload logs contain hard errors

### Phase 3.1 — Advanced Chaos Engineering (Blast Radius)

Subtle degradation tests that trigger edge-case state-machine behavior:

```bash
# Inject kernel syscall failures into proxy execution path
./scripts/hardening-syscall-fault-injection.sh

# Add delay/jitter/reordering/corruption using tc netem (requires root)
sudo RUNS=10 NET_IFACE=lo ./scripts/hardening-netem-jitter.sh

# Simulate zombie backend behavior (socket accepts but never responds)
./scripts/hardening-zombie-backend.sh
```

Success criteria:
- No proxy crash or abort while faults are active
- No unbounded FD growth during fault window
- Zombie backend requests fail fast (bounded timeout), not indefinite hang

### Phase 4 — Parser Chaos

- Unit harness + AFL++ entrypoint: `test_fuzz_harness`
- Run deterministic bad-input battery via `ctest -R test_fuzz_harness`
- Run AFL++ campaign using instructions in `tests/test_fuzz_harness.c`

### Phase 4.1 — State Machine Verification

Three-layer formal verification of the state machine model defined in `include/keel/engine/state_machine.h`.

**Layer 1 — Exhaustive sequence walk** (`test_sm_sequence_walk`): DFS traversal of every legal edge in the phase (6×6), replay (7×7), and CID (10×10) transition matrices, plus exhaustive rejection of all illegal edges. Combined lifecycle walks exercise bind types, 10× transaction round-trips, hard-pin upgrade, and quarantine.

**Layer 2 — Fuzz / property-based** (`test_sm_fuzz`): AFL++/libfuzzer dual-purpose harness that interprets random byte pairs as (opcode, argument) across 9 opcodes (PHASE/BIND/UNBIND/BEGIN_TXN/END_TXN/REPLAY/CID/HARD_PIN/QUARANTINE). Contract invariants are validated after every successful transition. Includes a deterministic battery of 2304+ inputs covering all opcode × argument pairs.

**Layer 3 — Concurrent stress** (`test_sm_stress`): 64-thread tests for independent lifecycles (HANDSHAKE→bind→txn→unbind→CLOSING), journal ring-buffer wrap stress (192 events per thread), contract derivation storm (18 phase×tx configurations), and barrier-synchronized thundering-herd phase + lifecycle transitions.

```bash
# Run all state machine tests
ctest -R "test_state_contracts|test_sm_" --output-on-failure
```

### Phase 5 — Security Hardening & Protocol Defense

```bash
# Binary exploit-mitigation posture (PIE/NX/Canary/RELRO)
./scripts/hardening-checksec.sh

# Static analysis pass (clang analyzer via scan-build)
./scripts/hardening-static-analysis.sh

# TLS scanner (testssl.sh preferred, openssl fallback)
./scripts/hardening-tls-scan.sh

# sqlmap-driven parser robustness check through proxy endpoint
./scripts/hardening-sqlmap.sh
```

Notes:
- `hardening-checksec.sh` is enabled by default in `hardening-ci.sh`.
- TLS/sqlmap stages are opt-in because they depend on runtime topology and installed tools.

### Safe-Fail Architecture Guardrails

Recommended deployment controls (defense in depth):

- **Privilege drop**: run bootstrap as root only if needed for privileged bind/setup, then drop to a non-privileged UID/GID before accepting client traffic.
- **Seccomp allowlist**: constrain runtime syscall surface to required calls only (`read`, `write`, `epoll/io_uring`, socket ops, timers, futex, etc.).
- **Memory guard pages**: use guarded memory regions (already supported in memory safety layer) in debug/hardening builds to fail fast on over/under-runs.

These controls should be staged with canary rollout and monitored for false positives before broad production enablement.

Runtime configuration (implemented):

```ini
[security]
privilege_drop = true
run_user = nobody
run_group = nogroup
require_privilege_drop = true

seccomp = baseline   # off | baseline | strict
require_seccomp = true
no_new_privs = true
```

Behavior:
- Privilege drop is applied after startup privileged operations and before worker threads start.
- Seccomp is applied process-wide before workers start (`baseline` denylist or `strict` allowlist).
- `require_* = true` enforces fail-closed startup.

### Production Go/No-Go Checklist

| Category | Test / Tool | Success Criteria |
|---|---|---|
| Resilience | Chaos suite (`syscall`, `netem`, `zombie`) | Proxy remains available/recovering; no crash loops |
| Leakage | `valgrind --leak-check=full` on memory-focused tests | Zero definitely-lost blocks |
| Isolation | Shadow diff + dirty-connection/state tests | No cross-session result/state bleed |
| Hardening | `./scripts/hardening-checksec.sh` | PIE + NX + stack canary + full RELRO |
| Protocol | Protocol flow tests + parser fuzz harness | No parser crashes or state desync |

### Metrics for Invisible Failures

Track these counters/derived signals continuously to detect non-crash corruption and stale-state bugs:

- `pool_wait_resume_requeues` and `pool_wait_timeout_events` growth rate (stuck wait-path pressure)
- `errors_proto`, `errors_backend`, `errors_timeout` per 1k transactions (silent degradation)
- `flow_wait_pool_ns_total / flow_wait_pool_events` and `flow_wait_backend_query_ns_total / flow_wait_backend_query_events` (hidden latency inflation)
- Shadow parity mismatch rate from `hardening-shadow-diff.sh` in scheduled runs
- Dirty connection incidence from `test_dirty_connection` and backend cleanup counters (`discard_all_count`, `state_sync_count`, `quarantine_count`)

Suggested SLO alarms:
- Any non-zero shadow mismatch in production-like replay
- Sustained increase in timeout/error counters over baseline
- Significant divergence between pool-wait and backend-query wait profiles

---

## Docker Integration Tests

All Docker integration tests live under `docker/tests/`. They spin up real
database topologies via Docker Compose and validate KEEL behaviour end-to-end.

### Prerequisites

- **Docker** 20.10+ and **Docker Compose** v2 (`docker compose version`)
- KEEL binary on `$PATH` (see [Building the Project](#building-the-project)):
  ```bash
  export PATH="$HOME/.local/bin:$PATH"
  ```
- All scripts are executed from the project root or `docker/tests/`:
  ```bash
  cd /path/to/keel/docker/tests
  ```

### Pre-pulling Images

Pull all required Docker images before running tests (avoids timeout during
cluster health-check windows):

```bash
docker pull postgres:15
docker pull bitnami/postgresql:15
docker pull patroni/patroni:latest
docker pull bitnami/etcd:latest
docker pull mysql:9
docker pull mariadb:latest
docker pull percona/percona-xtradb-cluster:8.4
```

### Full Validation Run (all tests)

Run all tests sequentially for a complete pass:

```bash
cd docker/tests
export PATH="$HOME/.local/bin:$PATH"

bash test-pg-streaming.sh start && bash test-pg-streaming.sh status && bash test-pg-streaming.sh stop
bash test-pg-patroni.sh start && bash test-pg-patroni.sh probe && bash test-pg-patroni.sh stop
PGBENCH_DURATION=30 PGBENCH_CLIENTS=50 bash test-pg-e2e-full.sh
bash test-sharding.sh
bash test-cloud-auth-e2e.sh
bash test-mysql-replication.sh start && bash test-mysql-replication.sh test && bash test-mysql-replication.sh stop
bash test-mysql-group.sh start && bash test-mysql-group.sh test && bash test-mysql-group.sh stop
bash test-mysql-mariadb.sh start && bash test-mysql-mariadb.sh test && bash test-mysql-mariadb.sh stop
bash test-mysql-pxc.sh start && bash test-mysql-pxc.sh test && bash test-mysql-pxc.sh stop
```

Expected result: **all 9 test suites pass** with zero errors.

---

### 1. PostgreSQL — Streaming Replication

| Item | Value |
|------|-------|
| Script | `docker/tests/test-pg-streaming.sh` |
| Compose file | `docker/compose/pg-streaming.yml` |
| Docker images | `postgres:15` |
| Network | `pg-streaming-net` (bridge) |
| Topology | 1 primary (`pg-stream-primary`, port 5432) + 2 replicas (ports 5433, 5434) |
| External ports | 5432–5434 |

#### What it tests

Physical streaming replication — primary WAL sender to two hot-standby replicas.

#### Commands

```bash
cd docker/tests

# Start cluster (primary first, then replicas)
bash test-pg-streaming.sh start

# Check replication status (WAL LSN, pg_is_in_recovery)
bash test-pg-streaming.sh status

# Stop and remove volumes
bash test-pg-streaming.sh stop

# Tail logs for a specific service
bash test-pg-streaming.sh logs primary
bash test-pg-streaming.sh logs replica1
```

#### Expected output (start)

```
[INFO] Starting streaming replication cluster...
[INFO] Primary is healthy
[INFO] Waiting for replicas to start...
[INFO] === Cluster Status ===
[INFO] Primary (port 5432):
 pg_is_in_recovery | wal_lsn
-------------------+---------
 f                 | 0/3000060
[INFO] Replica 1 (port 5433):
 pg_is_in_recovery | received_lsn
-------------------+-------------
 t                 | 0/3000060
[INFO] Replica 2 (port 5434):
 pg_is_in_recovery | received_lsn
-------------------+-------------
 t                 | 0/3000060
```

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Replica 1 not responding yet` | Replica start lag | Wait a few seconds; normal if replicas haven't connected yet |
| `connection refused` on port 5433/5434 | Replicas still initialising | Re-run `status` after 10–15 s |
| `FATAL: requested WAL segment has been removed` | Replica lagged too far behind | Stop, remove volumes (`docker compose down -v`), restart |

---

### 2. PostgreSQL — Patroni + etcd HA

| Item | Value |
|------|-------|
| Script | `docker/tests/test-pg-patroni.sh` |
| Compose file | `docker/compose/pg-patroni.yml` |
| Docker images | `patroni/patroni:latest`, `bitnami/etcd:latest` |
| Network | `pg-patroni-net` |
| Topology | 1 etcd node + 3 Patroni nodes (PostgreSQL) |
| External ports | etcd: 2379; Patroni REST: 8008–8010; PG: 5432–5434 |

#### What it tests

Patroni-managed PostgreSQL HA with etcd as distributed consensus store.
Validates leader election, replica registration, and REST health endpoints.

#### Commands

```bash
cd docker/tests

# Start cluster
bash test-pg-patroni.sh start

# Probe node roles and health
bash test-pg-patroni.sh probe

# Stop and remove volumes
bash test-pg-patroni.sh stop
```

#### Expected output (probe)

```
=== Patroni Cluster Status ===
  ● pg-patroni-1 (port 5432): PRIMARY - running
  ○ pg-patroni-2 (port 5433): REPLICA - streaming
  ○ pg-patroni-3 (port 5434): REPLICA - streaming

=== REST Health Checks ===
  pg-patroni-1 (8008): leader
  pg-patroni-2 (8009): replica
  pg-patroni-3 (8010): replica
```

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Timeout waiting for Patroni` | Slow initialisation | Increase attempts or wait 30 s and re-probe |
| `etcd not ready` | etcd container not started | `docker compose logs etcd` to diagnose |
| All nodes show `REPLICA` | Leader election still in progress | Wait 15 s and re-probe |

---

### 3. PostgreSQL — Full E2E with pgbench

| Item | Value |
|------|-------|
| Script | `docker/tests/test-pg-e2e-full.sh` |
| Compose file | `docker/compose/pg-e2e.yml` |
| Docker images | `postgres:15` (DB nodes) + KEEL built from source |
| Network | `e2e-net` |
| Topology | 1 primary (`pgsql-01`) + 2 replicas (`pgsql-02`, `pgsql-03`) + KEEL proxy + pgbench runner |
| KEEL port | 6432 (external) |

#### What it tests

Complete end-to-end validation: cluster formation → KEEL proxy startup →
read/write split detection → pgbench stress at configurable concurrency.

#### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PGBENCH_CLIENTS` | `100` | Concurrent pgbench clients |
| `PGBENCH_DURATION` | `60` | Benchmark duration in seconds |
| `PGBENCH_SCALE` | `10` | pgbench scale factor |

#### Commands

```bash
cd docker/tests

# Default run (100 clients, 60s)
bash test-pg-e2e-full.sh

# Quick validation run (30s stress, 50 clients)
PGBENCH_DURATION=30 PGBENCH_CLIENTS=50 bash test-pg-e2e-full.sh

# Extended stress run (120s, 200 clients)
PGBENCH_DURATION=120 PGBENCH_CLIENTS=200 bash test-pg-e2e-full.sh
```

#### Expected output

```
[✓] Docker found
[✓] Docker Compose found
[✓] Compose file found
[✓] Previous containers removed
[✓] KEEL build complete
[✓] Primary is ready
[✓] All replicas are ready
[✓] pgbench initialized
e2e-keel-proxy  | INFO  engine: started with 4 workers
e2e-keel-proxy  | INFO  probe: server[2] pgsql-01:5432 detected as RW
e2e-keel-proxy  | INFO  probe: server[0] pgsql-03:5432 detected as RO
e2e-keel-proxy  | INFO  probe: server[1] pgsql-02:5432 detected as RO
e2e-pgbench     | progress: 5.0 s, 2915.3 tps, lat 34.2 ms stddev 38.1, 0 failed
...
e2e-pgbench     | latency average = 34.317 ms
e2e-pgbench     | tps = 2898.076434 (without initial connection time)
  ✓ END-TO-END TEST PASSED
    - pgbench stress test (50 clients, 30s)
[✓] Cleanup complete
```

#### Verified results (50 clients, 30 s, io_uring reactor)

| Metric | Value |
|--------|-------|
| TPS (avg) | ~2,900 |
| Latency avg | ~34 ms |
| Latency stddev | ~38 ms |
| Failed transactions | 0 |
| KEEL workers | 4 |
| Backend connections | 25 per worker |

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `KEEL build complete` never appears | Docker build fails | Check `docker/Dockerfile.linux`; ensure buildx available |
| pgbench reports `FATAL: terminating connection due to conflict` | Replica promotion during run | Normal in failover scenarios; restart test |
| Proxy exits with non-zero code | Binary not found in `$PATH` | `export PATH="$HOME/.local/bin:$PATH"` |
| `pool warmup` warnings | Backend unhealthy at startup | Wait for full healthcheck pass before running |

---

### 4. PostgreSQL — Horizontal Sharding

| Item | Value |
|------|-------|
| Script | `docker/tests/test-sharding.sh` |
| Compose file | `docker/compose/pg-sharding.yml` |
| Docker images | `postgres:15` |
| Network | `pg-sharding-net` |
| Topology | 2 PostgreSQL shards (`pg-shard0`, `pg-shard1`) + KEEL |
| KEEL proxy port | 16432 (external) |
| Prometheus port | 19101 (external) |

#### What it tests

Hash-based shard routing, data distribution verification, single-shard SELECT,
Prometheus metrics exposure, and SIGHUP hot-reload of shard rules.

#### Commands

```bash
cd docker/tests

# Run all 6 tests (tears down on exit)
bash test-sharding.sh

# Keep containers running after test (for manual inspection)
bash test-sharding.sh --keep
# Tear down manually when done:
#   docker compose -f docker/compose/pg-sharding.yml down -v
```

#### Expected output

```
=== KEEL Sharding Integration Test ===

[info] Starting compose stack...
[info] Test 1: Creating schema on both shards...
[PASS] Schema created on both shards
[info] Test 2: INSERT routing via KEEL...
[PASS] INSERTs sent through KEEL without error
[info] Test 3: Verifying shard data distribution...
[info] Shard 0 row count: 2
[info] Shard 1 row count: 2
[PASS] Rows present on both shards
[info] Test 4: Single-shard SELECT via KEEL...
[PASS] Single-shard SELECT returned correct row
[info] Test 5: Prometheus /metrics endpoint...
[PASS] /metrics exposes keel query counters
[info] Test 6: SIGHUP hot-reload (add orders shard rule)...
[PASS] Post-SIGHUP routing works correctly

=== All sharding integration tests passed ===

[info] Tearing down containers...
```

#### Shard routing logic

| Row key | `hash(id) % 2` | Shard |
|---------|----------------|-------|
| `id=0` | 0 | `pg-shard0` |
| `id=1` | 1 | `pg-shard1` |
| `id=2` | 0 | `pg-shard0` |
| `id=3` | 1 | `pg-shard1` |

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Could not fetch /metrics` | Prometheus listener not yet up | Add `sleep 2` or check KEEL startup logs |
| `Shard 0 has no rows` | All inserts routed to one shard | Check shard rule config in compose KEEL config |
| `Expected 'alice', got ''` | SELECT returned empty | Verify shard-key column name matches rule config |

---

### 5. Cloud Auth E2E

| Item | Value |
|------|-------|
| Script | `docker/tests/test-cloud-auth-e2e.sh` |
| Binary | `build-test/tests/test_cloud_auth_e2e` (no Docker) |
| Docker images | None |

#### What it tests

Cloud-native token providers (file-based and environment-variable), token cache
lifecycle, expiry detection, provider fallback, and concurrent access correctness.
This test is self-contained — it invokes a compiled binary directly without Docker.

#### Commands

```bash
cd docker/tests

# Auto-discovers build directory (build-test, build, build-release, build-asan)
bash test-cloud-auth-e2e.sh

# Specify an explicit build directory
bash test-cloud-auth-e2e.sh /path/to/build-test
```

#### Expected output

```
=== KEEL Cloud Auth E2E Test ===

[info] Found binary: /path/to/build-test/tests/test_cloud_auth_e2e
[info] Running test_cloud_auth_e2e...

Test 1: Static file provider
  Token file: /tmp/keel-cloud-auth-test.../token
  Token value: test_token_from_file_12345

Test 2: Static env provider
  Environment variable: TEST_AUTH_TOKEN=test_token_from_env_67890

Test 3: Token cache lifecycle
  - Initial fetch from provider
  - Subsequent fetches from cache (<1 µs latency)
  - Refresh on expiry detection

Test 4: Fallback on provider error
  - Non-existent token file
  - Should fall back to static password

==========================================
Test Results
==========================================
✓ All tests completed successfully

Performance characteristics:
  - Cache hit latency: <1 µs
  - Provider fetch (file): ~10-100 µs
  - Throughput (cached): >20M ops/sec
  - Memory overhead per cache: ~512 bytes

✓ Test directory cleaned up
Test completed successfully!
```

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `[SKIP] test_cloud_auth_e2e binary not built` | Build not done yet | `cmake --build build-test --target test_cloud_auth_e2e` |
| `[FAIL] ... assertion` | Logic regression | Run `ctest -R test_cloud_auth_e2e --output-on-failure` for full details |

---

### 6. MySQL — Streaming Replication

| Item | Value |
|------|-------|
| Script | `docker/tests/test-mysql-replication.sh` |
| Compose file | `docker/compose/mysql-replication.yml` |
| Docker images | `mysql:9` |
| Network | `keel-mysql-replication_mysql-cluster` |
| Topology | 1 primary (`mysql-rep-primary`) + 2 replicas (`mysql-rep-replica1`, `mysql-rep-replica2`) |
| Replication | GTID-based async replication |

#### What it tests

MySQL 9 async GTID replication. Verifies that writes on the primary propagate to
both replicas and that replicas are correctly set `read_only=ON`.

#### Commands

```bash
cd docker/tests

# Start cluster and wait for all nodes healthy
bash test-mysql-replication.sh start

# Run functional tests
bash test-mysql-replication.sh test

# Stop and remove volumes
bash test-mysql-replication.sh stop
```

#### Expected output (test)

```
[TEST]  === Replication Verification ===
[TEST]    Replica 1: row count matches primary (4) ✓
[TEST]    Replica 2: row count matches primary (4) ✓
[TEST]    Replica 1: read_only=ON ✓
[TEST]    Replica 2: read_only=ON ✓

[INFO]  Results: 4 passed, 0 failed
```

#### Expected output (start — replica status)

```
[INFO]  Replica 1 (mysql-rep-replica1):
  server_id  read_only
  2          1
  Replication IO/SQL threads:
        ON
        ON

[INFO]  Replica 2 (mysql-rep-replica2):
  server_id  read_only
  3          1
  Replication IO/SQL threads:
        ON
        ON
```

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Primary not responding` after 60 attempts | MySQL initialising slowly | Increase `attempts` limit or pre-pull `mysql:9` image |
| `Replication IO thread: OFF` | Network or auth issue between nodes | `docker logs mysql-rep-replica1` to check replication errors |
| Row counts diverge | Replication lag | Add `sleep 3` before checking; GTID ensures eventual consistency |

---

### 7. MySQL — Group Replication

| Item | Value |
|------|-------|
| Script | `docker/tests/test-mysql-group.sh` |
| Compose file | `docker/compose/mysql-group.yml` |
| Docker images | `mysql:9` |
| Network | `keel-mysql-group_mysql-mgr` |
| Topology | 3 nodes (`mgr-node1`, `mgr-node2`, `mgr-node3`) — multi-primary mode |

#### What it tests

MySQL Group Replication in multi-primary mode. Bootstraps the GR group on
node1, then joins nodes 2 and 3. Verifies that all three nodes can accept writes
simultaneously and that rows converge across the group.

#### Commands

```bash
cd docker/tests

# Start 3-node multi-primary group
bash test-mysql-group.sh start

# Run multi-primary write tests
bash test-mysql-group.sh test

# Stop and remove volumes
bash test-mysql-group.sh stop
```

#### Expected output (start — group members)

```
[INFO]  Group members (from node1):
mgr-node1       3306    ONLINE  PRIMARY
mgr-node2       3306    ONLINE  PRIMARY
mgr-node3       3306    ONLINE  PRIMARY
```

#### Expected output (test)

```
[TEST]  === Multi-Primary Write Verification ===
[TEST]    Write on node 1 (mgr-node1): ✓
[TEST]    Write on node 2 (mgr-node2): ✓
[TEST]    Write on node 3 (mgr-node3): ✓
[TEST]    Data converged: all nodes have 6 rows ✓

[INFO]  Results: 4 passed, 0 failed
```

#### Group replication startup sequence

The script uses a multi-phase approach to avoid bootstrap races:

1. Start `mgr-node1`, wait for `healthy` healthcheck
2. Bootstrap GR: `SET GLOBAL group_replication_bootstrap_group = ON; START GROUP_REPLICATION …`
3. Wait for `mgr-node1` `MEMBER_STATE = ONLINE` in `performance_schema.replication_group_members`
4. Start `mgr-node2` and `mgr-node3`, run `RESET BINARY LOGS AND GTIDS; START GROUP_REPLICATION …`
5. Wait for cluster to stabilise

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| Bootstrap loop: `ERROR 3092` | GR already running | Safe to ignore; script handles this exit code |
| `MEMBER_STATE = RECOVERING` hangs | IST/SST from donor | Wait; normal during initial join (can take 30–60 s) |
| Node joins as `READ_ONLY` | Single-primary mode detected | Verify compose config has `group_replication_single_primary_mode=OFF` |
| Nodes never reach `ONLINE` | GR communication failure | Check `docker logs mgr-node1` for `Member has left the group` errors; re-run start |

---

### 8. MySQL — MariaDB Galera

| Item | Value |
|------|-------|
| Script | `docker/tests/test-mysql-mariadb.sh` |
| Compose file | `docker/compose/mysql-mariadb.yml` |
| Docker images | `mariadb:latest` |
| Network | `keel-mysql-mariadb_mdb-galera` |
| Topology | 3 nodes (`mdb-node1`, `mdb-node2`, `mdb-node3`) — Galera multi-master |
| SST method | `rsync` |

#### What it tests

MariaDB Galera synchronous multi-master replication. Bootstraps node1 with
`--wsrep-new-cluster`, joins nodes 2 and 3 via SST, then verifies that writes
on all three nodes converge cluster-wide.

#### Commands

```bash
cd docker/tests

# Bootstrap node1, then join nodes 2 and 3
bash test-mysql-mariadb.sh start

# Run multi-master write tests
bash test-mysql-mariadb.sh test

# Stop and remove volumes
bash test-mysql-mariadb.sh stop
```

#### Expected output (start)

```
[INFO]  Node 3 (mdb-node3):
wsrep_cluster_size      3
wsrep_cluster_status    Primary
wsrep_ready     ON
wsrep_local_state_comment       Synced
```

#### Expected output (test)

```
[TEST]  === MariaDB Galera Multi-Master Write Verification ===
[TEST]    Write on node 1 (mdb-node1): ✓
[TEST]    Write on node 2 (mdb-node2): ✓
[TEST]    Write on node 3 (mdb-node3): ✓
[TEST]    Data converged: all nodes have 6 rows ✓

[INFO]  Results: 4 passed, 0 failed
```

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `wsrep_cluster_status = non-Primary` | Node lost quorum | Stop all nodes, restart from `start` |
| `wsrep_local_state_comment = Joiner` hangs | rsync SST in progress | Wait up to 60 s; large clusters may take longer |
| `wsrep_ready = OFF` | Galera not yet initialised | Wait for `wsrep_local_state_comment = Synced` |

---

### 9. MySQL — Percona XtraDB Cluster (PXC)

| Item | Value |
|------|-------|
| Script | `docker/tests/test-mysql-pxc.sh` |
| Compose file | `docker/compose/mysql-pxc.yml` |
| Docker images | `percona/percona-xtradb-cluster:8.4` |
| Network | `keel-mysql-pxc_pxc-net`, subnet `172.29.0.0/24` |
| Topology | 3 nodes: `pxc-node1` (172.29.0.11), `pxc-node2` (172.29.0.12), `pxc-node3` (172.29.0.13) |
| SST method | `xtrabackup-v2` |

#### What it tests

Percona XtraDB Cluster 8.4 — Galera-based synchronous multi-master replication
using xtrabackup for SST. Validates that all three nodes reach `wsrep_local_state_comment = Synced`
with `wsrep_cluster_size = 3`, then verifies multi-master writes converge.

#### Commands

```bash
cd docker/tests

# Start cluster sequentially (required — see startup notes below)
bash test-mysql-pxc.sh start

# Run multi-master write tests
bash test-mysql-pxc.sh test

# Check cluster status without starting/stopping
bash test-mysql-pxc.sh status

# Stop and remove volumes
bash test-mysql-pxc.sh stop
```

#### Expected output (start)

```
[INFO]  Starting Percona XtraDB Cluster...
[INFO]  Waiting for bootstrap node to be ready (wsrep Synced)...
  Waiting for node1 (1/60)...
  ...
[INFO]  Starting node2 (SST from node1)...
[INFO]  Waiting for node2 to sync (xtrabackup SST takes ~30s)...
  Waiting for node2 sync (1/40, state=Donor/Desynced)...
  Waiting for node2 sync (2/40, state=Joiner)...
  ...
[INFO]  Starting node3 (SST from node1 or node2)...
[INFO]  Waiting for cluster formation (PXC IST/SST can take a while)...
[INFO]  Node 1 (pxc-node1):
wsrep_cluster_size      3
wsrep_cluster_status    Primary
wsrep_ready     ON
wsrep_local_state_comment       Synced
...
[INFO]  Node 3 (pxc-node3):
wsrep_cluster_size      3
wsrep_cluster_status    Primary
wsrep_ready     ON
wsrep_local_state_comment       Synced
```

#### Expected output (test)

```
[TEST]  === PXC Multi-Master Write Verification ===
[TEST]    Write on node 1 (pxc-node1): ✓
[TEST]    Write on node 2 (pxc-node2): ✓
[TEST]    Write on node 3 (pxc-node3): ✓
[TEST]    Data converged: all nodes have 6 rows ✓

[INFO]  Results: 4 passed, 0 failed
```

#### PXC startup notes (important)

The PXC entrypoint runs a **3-phase startup**:

1. `mysqld --initialize-insecure` — data directory initialisation
2. `mysqld --skip-networking` — Galera bootstrap under `--skip-networking`; runs
   init SQL (creates users, sets passwords). Galera port 4567 is **briefly
   active** here but loses quorum when phase 2 shuts down.
3. `exec mysqld` — final production start with full Galera.

Because of this sequence, the healthcheck is set to wait for
`wsrep_local_state_comment = Synced` (not just `mysqladmin ping`). This
prevents nodes 2 and 3 from starting their SST during phase 2, which would
cause a quorum failure when phase 2 mysqld exits.

Node2 must be fully `Synced` **before** node3 starts, to prevent two concurrent
xtrabackup SST processes from conflicting on the donor.

#### PXC-specific configuration

| Config | Value | Reason |
|--------|-------|--------|
| `loose-pxc_encrypt_cluster_traffic = OFF` | All node CNFs | Avoids certificate modulus errors when SSL traffic mixing occurs |
| Healthcheck | `wsrep_local_state_comment = Synced` | Only passes after full Galera start (not during phase 2) |
| Node2/3 startup | Sequential | Prevents concurrent xtrabackup SST conflicts |

#### Common issues

| Error | Cause | Fix |
|-------|-------|-----|
| `data too large for modulus: certificate signature failure` | PXC SSL mismatch between Galera phases | Ensure `loose-pxc_encrypt_cluster_traffic = OFF` in all node CNFs |
| `Failed to establish quorum` / `xtrabackup_checkpoints missing` | Node2/3 tried to join during phase 2 | Healthcheck must gate on `wsrep_local_state_comment = Synced`, not `mysqladmin ping` |
| Node2 fails SST with `WSREP: Failed to receive first data chunk` | Concurrent SST from two joiners | Start node2, wait for `Synced`, then start node3 sequentially |
| `safe_to_bootstrap: 0` — node1 won't bootstrap | Node1 `grastate.dat` left in unsafe state by previous crash | `docker compose down -v` to remove all volumes, then `start` fresh |
| `wsrep_cluster_size = 1` after all nodes start | Nodes 2/3 not joining | Check `docker logs pxc-node2` for Galera errors; check network subnet connectivity |
| Image pull timeout | `percona/percona-xtradb-cluster:8.4` is large (~1 GB) | Pre-pull: `docker pull percona/percona-xtradb-cluster:8.4` |

---

## Running pgbench Stress Tests

### Automated Stress Test

The E2E test script (`test-pg-e2e-full.sh`) handles build, cluster setup, and
pgbench in one command:

```bash
cd docker/tests
export PATH="$HOME/.local/bin:$PATH"

# Default settings (100 clients, 60 seconds)
bash test-pg-e2e-full.sh

# Quick validation (50 clients, 30 seconds)
PGBENCH_DURATION=30 PGBENCH_CLIENTS=50 bash test-pg-e2e-full.sh

# Extended stress (200 clients, 120 seconds)
PGBENCH_CLIENTS=200 PGBENCH_DURATION=120 bash test-pg-e2e-full.sh
```

### Manual pgbench Execution

After the E2E stack is running:

```bash
# Initialise pgbench tables (inside the pgbench container)
docker compose -f docker/compose/pg-e2e.yml exec pgbench \
    pgbench -h e2e-keel-proxy -p 6432 -U postgres -d testdb -i -s 10

# Run benchmark
docker compose -f docker/compose/pg-e2e.yml exec pgbench \
    pgbench -h e2e-keel-proxy -p 6432 -U postgres -d testdb \
        --client=100 \
        --jobs=4 \
        --time=60 \
        --progress=5 \
        --report-per-command
```

### pgbench Options

| Option | Description |
|--------|-------------|
| `-c, --client=N` | Number of concurrent clients |
| `-j, --jobs=N` | Number of threads |
| `-T, --time=N` | Duration in seconds |
| `-s, --scale=N` | Scale factor (100,000 rows per scale) |
| `-P, --progress=N` | Progress interval in seconds |
| `--report-per-command` | Per-command latency breakdown |
| `-S, --select-only` | Read-only workload |
| `-M, --protocol=S` | Protocol: `simple`, `extended`, `prepared` |

### Custom Workloads

```bash
# Read-only test (SELECT queries only)
pgbench -h 127.0.0.1 -p 7432 -U postgres -d testdb \
    --client=100 --time=60 --select-only

# Write-heavy test
pgbench -h 127.0.0.1 -p 7432 -U postgres -d testdb \
    --client=50 --time=60 --builtin=tpcb-like

# Custom SQL script
cat > /tmp/custom.sql << 'EOF'
\set aid random(1, 100000 * :scale)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
EOF

pgbench -h 127.0.0.1 -p 7432 -U postgres -d testdb \
    --client=100 --time=60 --file=/tmp/custom.sql
```

### High-Concurrency Stress Tests (3000+ Clients)

KEEL has been validated at 3,000 concurrent reconnecting clients. These tests
require kernel tuning to avoid TCP port exhaustion.

#### Kernel Tuning (Required)

```bash
# Allow reuse of TIME_WAIT sockets
sudo sysctl -w net.ipv4.tcp_tw_reuse=1

# Widen ephemeral port range
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Shorten TIME_WAIT timeout for reconnecting workloads
sudo sysctl -w net.ipv4.tcp_fin_timeout=5

# Cap TIME_WAIT bucket count
sudo sysctl -w net.ipv4.tcp_max_tw_buckets=10000
```

#### Running

```bash
# SELECT-only, 3000 reconnecting clients, 30 seconds
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 7432 -U postgres -d test \
    -c 3000 -C -S -T 30 -P 5

# Expected output: ~6,800 TPS, 0 failures
```

#### Verified Results

| Run | Clients | Duration | TPS | Transactions | Failures |
|-----|---------|----------|-----|-------------|----------|
| 1 | 3,000 | 15s | 6,809 | 102,297 | 0 |
| 2 | 3,000 | 15s | 6,580 | 99,015 | 0 |
| 3 | 3,000 | 15s | 6,451 | 96,924 | 0 |
| 4 | 3,000 | 30s | 6,818 | 204,725 | 0 |

### E2E Stress Test Results (via test-pg-e2e-full.sh)

Validated on Linux with `io_uring` reactor, 3-node PostgreSQL cluster, KEEL
built from source in the Docker compose stack. KEEL proxy: 4 workers, 25 backend
connections per worker, transaction pooling mode.

| Clients | Duration | TPS (avg) | Latency avg | Latency stddev | Failures |
|---------|----------|-----------|-------------|----------------|----------|
| 50 | 30 s | ~2,900 | ~34 ms | ~38 ms | 0 |
| 100 | 60 s | ~2,900 | ~68 ms | ~72 ms | 0 |

Sample progress output (50 clients, 30 s):

```
progress: 5.0 s,  2915.3 tps, lat 34.2 ms stddev 38.1, 0 failed
progress: 10.0 s, 2900.0 tps, lat 34.094 ms stddev 40.286, 0 failed
progress: 15.0 s, 2799.0 tps, lat 35.826 ms stddev 41.152, 0 failed
progress: 20.0 s, 2918.2 tps, lat 33.983 ms stddev 35.095, 0 failed
progress: 25.0 s, 2925.8 tps, lat 34.117 ms stddev 38.547, 0 failed
progress: 30.0 s, 2849.6 tps, lat 34.917 ms stddev 38.991, 0 failed

latency average = 34.317 ms
latency stddev  = 37.961 ms
tps = 2898.076434 (without initial connection time)
```

---

## Running sysbench Stress Tests

### Maintained Script Inventory

The `scripts/` directory contains CI enforcement harnesses and packaging.
The `bench/` directory contains benchmark scripts and workload definitions.
Use these lists as the source of truth for supported script-based testing:

**scripts/** (CI / enforcement / packaging):
- `scripts/hardening-ci.sh` (orchestrated hardening matrix)
- `scripts/hardening-*.sh` (targeted hardening/security/chaos checks)
- `scripts/ci-hard-guarantee.sh` (build + ctest + kTLS gate)
- `scripts/test_rw_split.sh` (read/write split validation)
- `scripts/package-linux.sh` (release packaging)

**bench/** (benchmark / performance):
- `bench/bench-compare.sh` (comparative: Direct vs KEEL vs HAProxy vs PgBouncer)
- `bench/perf-rootcause-3pass.sh` (automated 3-pass latency attribution)
- `bench/perf-lock-artifacts.sh` (lock benchmark artifacts into repo)
- `bench/diag_bench.sh` (quick diagnostic benchmark)
- `bench/run_keel_pgbench.sh` (weighted pgbench harness)
- `bench/tpcc_like_persistent.lua` (sysbench TPC-C workload, persistent conns)
- `bench/tpcc_like_reconnect.lua` (sysbench TPC-C workload, reconnect mode)
- `bench/pgbench_read_rw_split.sql` (shadow-diff / split SQL fixture)
- `bench/schema.sql` (benchmark database schema)
- `bench/scripts/*.sql` (pgbench weighted SQL scripts)

Legacy one-off wrappers and duplicate sysbench launchers were intentionally
removed during cleanup to keep the script surface small and reliable.

The `sysbench oltp_read_write` workload exercises the full PostgreSQL prepared
statement path through KEEL, including **prepared statement virtualization**
(PS replay). Each transaction issues a `BEGIN`, several keyed reads, and an
update followed by a `COMMIT`, using named prepared statements — exactly the
pattern that requires the WAIT_STMT_REPLAY engine state.

### Prerequisites

```bash
# Ubuntu/Debian
sudo apt-get install -y sysbench

# Fedora/RHEL
sudo dnf install sysbench
```

### Prepare the Database

```bash
sysbench oltp_read_write \
    --db-driver=pgsql \
    --pgsql-host=127.0.0.1 \
    --pgsql-port=7432 \
    --pgsql-user=postgres \
    --pgsql-password=postgres \
    --pgsql-db=test \
    --tables=1 \
    --table-size=1000 \
    prepare
```

### Run Benchmark

```bash
sysbench oltp_read_write \
    --db-driver=pgsql \
    --pgsql-host=127.0.0.1 \
    --pgsql-port=7432 \
    --pgsql-user=postgres \
    --pgsql-password=postgres \
    --pgsql-db=test \
    --tables=1 \
    --table-size=1000 \
    --threads=4 \
    --time=20 \
    --report-interval=5 \
    run
```

### Strict AB3 Workflow (KEEL vs Direct)

For the strict AB3 methodology, acceptance criteria, and published 200/300-thread
results, see [PERF_STRICT_AB3_SUMMARY.md](PERF_STRICT_AB3_SUMMARY.md).

After collecting runs, lock benchmark artifacts into the repo with:

```bash
bench/perf-lock-artifacts.sh
```

### 3-Pass Root-Cause Profiling (One Command)

Use this when you need quick attribution of slowness source (pool wait,
backend wait, scheduler delay, backend execution, deferred-send backpressure)
without manually toggling instrumentation.

```bash
OUT_DIR=/tmp/keel_rootcause_$(date +%Y%m%d_%H%M%S) \
    THREADS=300 DURATION=20 \
    bench/perf-rootcause-3pass.sh
```

What it does:

- Runs three passes automatically: `coarse` → `split` → `full`
- Applies hot-path runtime toggles (`KEEL_HOT_INSTR_*`) per pass
- Captures `SHOW STATS` before/after each pass and computes deltas
- Produces a human-readable diagnosis in `result.txt`

Artifacts:

- `<OUT_DIR>/coarse/summary.json`
- `<OUT_DIR>/split/summary.json`
- `<OUT_DIR>/full/summary.json`
- `<OUT_DIR>/result.txt`

Common knobs:

- `THREADS`, `DURATION`, `READ_PCT`, `READ_TXN_MODE`
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- `KEEL_BIN`, `CFG_FILE`, `ADMIN_HOST`, `ADMIN_PORT`

### Verified Results (io_uring reactor, transaction pooling)

All runs through KEEL at localhost; PostgreSQL 17 on the same host.
Lock-conflict failures (`ignored errors`) are row-level lock contention
— identical rate when connecting directly, *not* a proxy artifact.

| Threads | Table Rows | Duration | Transactions | TPS (total) | TPS (avg) | Avg Latency | Ignored Errors |
|---------|-----------|----------|-------------|-------------|-----------|-------------|----------------|
| 1 | 1,000 | 10 s | 2,824 | 2,824 | 282 | 3.54 ms | normal |
| 4 | 1,000 | 20 s | 3,298 | 3,298 | 164 | 24.26 ms | normal |
| 4 | 10,000 | 20 s | 10,094 | 10,094 | 672 | 5.95 ms | normal |

> **Note:** The lower TPS at `table-size=1000` with 4 threads is expected —
> higher contention on the smaller table drives more lock-conflict retries and
> longer wait times. Increasing `table-size` reduces contention and yields
> higher throughput.

### Validating Correctness

Since `oltp_read_write` uses named prepared statements, a zero ignored-errors
run confirms that PS virtualization (replay) is working correctly:

```bash
# A passing run should show:
#   FATAL errors: 0
#   Ignored errors: <same as direct PG — lock conflicts only>
grep -E 'FATAL|ignored' sysbench_output.txt
```

A regression in PS replay (e.g., missing Sync in replay buffer, or undraned
RFQ from replay Sync) will instead produce `FATAL` errors or a proxy hang
within the first few seconds.

---

## Manual Testing

### Testing Without Docker

```bash
# Ensure PostgreSQL is running locally
pg_isready -h 127.0.0.1 -p 5432

# Start KEEL with the test config
./build/src/main/keel -c etc/keel-pg.ini

# In another terminal, test connectivity
PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d test -c "SELECT 1;"
```

### Minimal Test Configuration

```ini
[keel]
log_level = 3    # DEBUG

[worker_group.test]
name = test
protocol = postgres
bind_addr = 127.0.0.1
bind_port = 7432
num_workers = 2
pool_mode = transaction
min_pool_size = 5
max_pool_size = 20
server_user = postgres
server_password = postgres
server_auth = scram-sha-256

[worker_group.test.servers]
primary = host=127.0.0.1 port=5432 dbname=postgres role=RW weight=100

[auth]
auth_type = scram-sha-256
auth_file = etc/userlist.txt
```

### Testing with psql

```bash
# Basic query
PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d test \
    -c "SELECT 'Hello KEEL!';"

# Multi-statement transaction
PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d test << 'EOF'
BEGIN;
CREATE TABLE IF NOT EXISTS test_table (id serial, data text);
INSERT INTO test_table (data) VALUES ('test1'), ('test2'), ('test3');
SELECT * FROM test_table;
COMMIT;
DROP TABLE test_table;
EOF

# Check backend PID (shows pool reuse)
PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d test \
    -c "SELECT pg_backend_pid();"
```

---

## Interpreting Results

### pgbench Output

```
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 10
query mode: simple
number of clients: 100
number of threads: 4
maximum number of tries: 1
duration: 60 s
number of transactions actually processed: 123456
number of failed transactions: 0 (0.000%)
latency average = 48.563 ms
latency stddev = 12.345 ms
initial connection time = 234.567 ms
tps = 2057.93 (without initial connection time)
```

**Key Metrics:**

| Metric | Description | Good Value |
|--------|-------------|------------|
| `tps` | Transactions per second | Higher is better |
| `latency average` | Average query latency | Lower is better |
| `latency stddev` | Latency variability | Lower = more consistent |
| `failed transactions` | Error rate | Should be 0% |

### Pool Efficiency

Monitor KEEL logs for pool behavior:

```bash
# Look for pool hit/miss ratios
grep -E "POOL HIT|POOL MISS|pool-return" keel.log
```

- High **POOL HIT** ratio → good connection reuse
- Many **POOL MISS** → increase `min_pool_size` or `max_pool_size`
- **pool-return** → connections being recycled (transaction pooling working)

### Replication Lag

```bash
psql -h 127.0.0.1 -p 5432 -U postgres -c \
    "SELECT client_addr, state, sent_lsn, replay_lsn,
            pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes
     FROM pg_stat_replication;"
```

---

## Troubleshooting

### Build Failures

**Problem:** `liburing.h not found`
```bash
# Install liburing
sudo apt-get install liburing-dev

# Or disable io_uring (will use epoll fallback)
cmake .. -DKEEL_USE_IOURING=OFF
```

**Problem:** `openssl/sha.h not found`
```bash
sudo apt-get install libssl-dev
```

**Problem:** C23 features not supported
```bash
# Ensure GCC 13+ or Clang 17+
gcc --version
# If too old, install a newer version
sudo apt-get install gcc-13
cmake .. -DCMAKE_C_COMPILER=gcc-13
```

### Test Failures

**Problem:** Unit tests fail with memory errors
```bash
# Build with AddressSanitizer for detailed diagnostics
cmake .. -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_ASAN=ON
make -j$(nproc)
ctest --output-on-failure
```

**Problem:** E2E tests fail to connect
```bash
# Check containers are running
docker compose -f docker/compose/pg-e2e.yml ps

# Check KEEL logs
docker compose -f docker/compose/pg-e2e.yml logs e2e-keel-proxy

# Check PostgreSQL is ready
docker compose -f docker/compose/pg-e2e.yml exec pgsql-01 pg_isready
```

### Connection Issues

**Problem:** Pool exhaustion (queries hang)
```
Sessions queued, pool at max_pool_size
```
- Increase `max_pool_size`
- Ensure `pool_mode = transaction` (not `session`)
- Reduce client count or idle timeout

**Problem:** Proxy hangs at ~900 clients
```
pgbench -c 900 ... produces no output
```
- The default `RLIMIT_NOFILE` soft limit is 1024. With 4 io_uring rings + backends + clients, the proxy runs out of file descriptors at ~900 clients.
- **Solution**: KEEL now auto-raises `RLIMIT_NOFILE` at startup (up to 1M). Verify with:
  ```bash
  cat /proc/$(pgrep keel)/limits | grep "open files"
  # Should show: Max open files  1048576  1048576
  ```

**Problem:** Proxy stops working after recompile / rapid restart
```
TIME_WAIT socket count > 30,000
```
- TCP `TIME_WAIT` sockets from prior runs exhaust ephemeral ports.
- **Solution**: Apply kernel tuning (see [High-Concurrency Stress Tests](#high-concurrency-stress-tests-3000-clients) above).
- Check current TIME_WAIT count: `ss -s | grep TIME-WAIT`

**Problem:** Multiple proxy instances on same port
```
SO_REUSEPORT allows multiple instances; killing the wrong one looks like a crash
```
- **Solution**: Always clean up before starting:
  ```bash
  pkill -9 -f 'keel -c' 2>/dev/null
  sleep 1
  ss -tlnp | grep 7432  # Verify port is clear
  ```

**Problem:** Backend connection refused
```
connection to 127.0.0.1:5432 FAILED
```
- Verify PostgreSQL is running: `pg_isready -h 127.0.0.1 -p 5432`
- Check `pg_hba.conf` allows connections
- Verify credentials in `keel-pg.ini` match PostgreSQL

**Problem:** Authentication failure
```
SCRAM authentication failed
```
- Verify `server_user` / `server_password` in config
- Ensure PostgreSQL uses `scram-sha-256` in `pg_hba.conf`
- Check `userlist.txt` for client auth credentials

**Problem:** Silent crash (no error output)
```
Proxy disappears, no log output
```
- KEEL now installs signal handlers for SIGSEGV, SIGABRT, and SIGBUS that print a diagnostic to stderr before re-raising the signal for a core dump.
- Check stderr output: `FATAL: SIGSEGV received — aborting`
- Enable core dumps: `ulimit -c unlimited`
- Analyze with: `gdb ./build/src/main/keel core`

### Performance Issues

**Problem:** Low TPS or high latency
1. Ensure `pool_mode = transaction` (most efficient)
2. Increase `min_pool_size` (pre-warm more connections)
3. Check `io_backend` — `iouring` is fastest on Linux 5.6+
4. Tune PostgreSQL (`shared_buffers`, `work_mem`, etc.)
5. Check network latency between proxy and backends

**Problem:** 0 TPS after client cancellation
- This was a known bug (fixed). Ensure you're on the latest `redesign` branch
- Verify backend connections are being returned to pool after cancel

### Docker Integration Test Failures

**Problem:** Docker Compose `up` hangs waiting for `healthy`
```
Waiting for node1 (60/60)...
```
- The container started but the healthcheck is failing
- Inspect with: `docker logs <container_name> | tail -40`
- Common cause: port conflict with a previous test run that wasn't cleaned up
- Fix: `docker compose -f docker/compose/<file>.yml down -v --remove-orphans`

**Problem:** `keel` binary not found in compose build
```
Error: KEEL binary not found
```
- The compose stack builds KEEL from source via `docker/Dockerfile.linux`
- Ensure the `build-test/src/main/keel` binary exists: `cmake --build build-test`
- Or use `~/.local/bin/keel` if using the install target:
  ```bash
  export PATH="$HOME/.local/bin:$PATH"
  ```

**Problem:** MySQL tests fail with `Table 'test.t1' doesn't exist`
- The MySQL init SQL (embedded in compose) creates the `test` database and `t1`
  table on startup
- If the healthcheck passed before init completed, the table may not exist yet
- Fix: `stop` the cluster and `start` again; or check init SQL in compose file

**Problem:** MariaDB / PXC node stays in `wsrep_local_state_comment = Joiner`
- SST (State Snapshot Transfer) is still in progress via rsync/xtrabackup
- Normal; can take 30–120 s depending on data size
- Monitor: `docker exec <node> mariadb -uroot -proot -e "SHOW STATUS LIKE 'wsrep_local_state_comment';"`
- If stuck > 2 min: `docker logs <node>` to see SST donor/joiner errors

**Problem:** PXC: `ERROR 3092` or `ER_GROUP_REPLICATION_RUNNING`
- Group Replication already running on a node that was supposed to bootstrap
- This is handled by the script (it treats this as success)
- If still failing: stop all containers, remove volumes, restart from scratch:
  ```bash
  docker compose -f docker/compose/mysql-pxc.yml down -v --remove-orphans
  bash test-mysql-pxc.sh start
  ```

**Problem:** PXC: `data too large for modulus: certificate signature failure`
- Galera SSL is active and certificates don't match between phases
- Ensure all node CNFs contain:
  ```ini
  loose-pxc_encrypt_cluster_traffic = OFF
  ```
- Files: `docker/mysql/config/pxc-node1.cnf`, `pxc-node2.cnf`, `pxc-node3.cnf`

**Problem:** PXC: `safe_to_bootstrap: 0` — bootstrap node refuses to start
- A previous cluster shutdown left `grastate.dat` in an unsafe state
- Fix: remove all PXC volumes to reset `grastate.dat`:
  ```bash
  docker compose -f docker/compose/mysql-pxc.yml down -v
  ```

**Problem:** Sharding test: `Could not fetch /metrics`
```
[FAIL] Could not fetch /metrics
```
- The KEEL Prometheus endpoint on port 19101 is not yet ready
- The `--wait` flag in the compose up should handle this, but if the metrics
  server is slow: add a brief `sleep 3` before the metrics check
- Verify with: `curl -sf http://127.0.0.1:19101/metrics | head`

**Problem:** `test-cloud-auth-e2e.sh` prints `[SKIP]`
```
[SKIP] Skipping cloud-auth E2E test (binary not built)
```
- The `test_cloud_auth_e2e` binary is missing
- Build it: `cmake --build build-test --target test_cloud_auth_e2e`

---

## Summary

### Validated test matrix (all pass)

| Test suite | Script / Command | Result |
|------------|-----------------|--------|
| PG streaming replication | `bash docker/tests/test-pg-streaming.sh start && … stop` | ✓ PASS |
| PG Patroni HA | `bash docker/tests/test-pg-patroni.sh start && … stop` | ✓ PASS |
| PG e2e-full (50 clients, 30 s) | `PGBENCH_DURATION=30 PGBENCH_CLIENTS=50 bash docker/tests/test-pg-e2e-full.sh` | ✓ PASS — 2,898 TPS |
| PG horizontal sharding | `bash docker/tests/test-sharding.sh` | ✓ PASS (6/6 tests) |
| Cloud auth E2E | `bash docker/tests/test-cloud-auth-e2e.sh` | ✓ PASS |
| MySQL replication | `bash docker/tests/test-mysql-replication.sh start && … test && … stop` | ✓ PASS (4/4) |
| MySQL Group Replication | `bash docker/tests/test-mysql-group.sh start && … test && … stop` | ✓ PASS (4/4) |
| MySQL MariaDB Galera | `bash docker/tests/test-mysql-mariadb.sh start && … test && … stop` | ✓ PASS (4/4) |
| MySQL PXC 8.4 | `bash docker/tests/test-mysql-pxc.sh start && … test && … stop` | ✓ PASS (4/4) |
| C unit tests (build-asan) | `cd build-asan && ctest --output-on-failure` | ✓ 100/100 |

### Quick reference

| Test Type | Command | Purpose |
|-----------|---------|---------|
| All unit tests | `cd build-test && ctest --output-on-failure` | Component-level testing |
| Unit tests (ASAN) | `cd build-asan && ctest --output-on-failure` | Memory safety validation |
| PG E2E stress | `PGBENCH_DURATION=30 PGBENCH_CLIENTS=50 bash docker/tests/test-pg-e2e-full.sh` | Full E2E stress test |
| Sharding | `bash docker/tests/test-sharding.sh` | Sharding + metrics + hot-reload |
| Cloud auth | `bash docker/tests/test-cloud-auth-e2e.sh` | Token cache + providers |
| MySQL replication | `bash docker/tests/test-mysql-replication.sh start && test && stop` | MySQL 9 async replication |
| MySQL Group Replication | `bash docker/tests/test-mysql-group.sh start && test && stop` | MGR multi-primary |
| MySQL MariaDB | `bash docker/tests/test-mysql-mariadb.sh start && test && stop` | Galera sync replication |
| MySQL PXC | `bash docker/tests/test-mysql-pxc.sh start && test && stop` | PXC xtrabackup SST |
| sysbench | `sysbench oltp_read_write ... run` | PS virtualization + throughput |
| Performance | `docs/PERF_STRICT_AB3_SUMMARY.md` + `bench/perf-lock-artifacts.sh` | Strict AB3 method/results |
| Hardening matrix | `./scripts/hardening-ci.sh` | ASAN/UBSAN/TSAN + hardening labels |
| Shadow diff | `./scripts/hardening-shadow-diff.sh` | Direct DB vs proxy parity |
| Slow client | `./scripts/hardening-slow-client-fast-server.sh` | Backpressure / partial-write |
| Jepsen-Lite | `./scripts/hardening-jepsen-lite.sh` | Fault injection under load |
| Syscall fault | `./scripts/hardening-syscall-fault-injection.sh` | Kernel syscall failure resilience |
| Netem jitter | `./scripts/hardening-netem-jitter.sh` | Network degradation resilience |
| Zombie backend | `./scripts/hardening-zombie-backend.sh` | Bounded timeout for dead backends |
| Static analysis | `./scripts/hardening-static-analysis.sh` | Path-sensitive null/UB defects |
| Binary hardening | `./scripts/hardening-checksec.sh` | PIE/NX/Canary/RELRO posture |
| TLS scan | `./scripts/hardening-tls-scan.sh` | Weak protocol/cipher detection |
| SQLMap stress | `./scripts/hardening-sqlmap.sh` | Parser stability under malicious SQL |
| Runtime security | `ctest -R test_runtime_security_harness` | Seccomp + privilege drop |

### CI / Release Gate (One Command)

Use this entrypoint to standardize production-readiness checks:

```bash
./scripts/hardening-ci.sh
```

Default behavior:
- Configures/builds `build-linux`
- Runs `ctest -L hardening`
- Skips expensive/integration-dependent steps unless enabled via env

Common CI variants:

```bash
# Full sanitizer matrix + hardening tests
RUN_SANITIZERS=1 ./scripts/hardening-ci.sh

# Include optional MSAN pass in sanitizer matrix (clang required)
RUN_SANITIZERS=1 RUN_MSAN=1 ./scripts/hardening-ci.sh

# Include direct-vs-proxy shadow diff
RUN_SHADOW_DIFF=1 ./scripts/hardening-ci.sh

# Include backpressure smoke (proxy must already be reachable)
RUN_SLOW_CLIENT=1 ./scripts/hardening-ci.sh

# Full gate (longest)
RUN_SANITIZERS=1 RUN_SHADOW_DIFF=1 RUN_SLOW_CLIENT=1 ./scripts/hardening-ci.sh

# Chaos + security expansion
RUN_CHAOS_SYSCALLS=1 RUN_CHAOS_ZOMBIE=1 RUN_SECURITY_CHECKSEC=1 ./scripts/hardening-ci.sh

# Netem + TLS + sqlmap (environment/tooling dependent)
RUN_CHAOS_NETEM=1 RUN_SECURITY_TLS=1 RUN_SECURITY_SQLMAP=1 ./scripts/hardening-ci.sh
```

Triage map:
- `test_pool_audit` fails: inspect pool canary/header tracking path and SIGUSR1 dump output
- `test_fd_tracking` fails: inspect socket close paths and `/proc/<pid>/fd` growth
- `test_state_machine` / `test_dirty_connection` fails: inspect backend reuse and state hash cleanup
- `test_concurrency_stress` fails: inspect queue backpressure, TIME_WAIT behavior, RLIMIT checks
- `test_shadow_diff_harness` fails: inspect `scripts/hardening-shadow-diff.sh` SQL-file validation
- `test_runtime_security_harness` fails: inspect `[security]` config, user/group resolution, and seccomp install path in startup logs
- `test_runtime_security_strict_harness` fails: inspect strict seccomp allowlist coverage for active syscalls on this platform

Note: strict seccomp is intentionally environment-dependent and is not part of the default `hardening` label gate.

For more information, see:
- [docs/MULTIPLEXING.md](MULTIPLEXING.md) — Worker architecture deep-dive
- [docs/SHARDING.md](SHARDING.md) — Sharding architecture and hot-reload
- [etc/keel-pg.ini](../etc/keel-pg.ini) — PostgreSQL configuration reference
- [etc/keel-my.ini](../etc/keel-my.ini) — MySQL configuration reference
- [docker/README.md](../docker/README.md) — Docker setup details

---

## Known Issues and Notes

### Pre-existing CI failures

`test_cluster` (test #68 — cluster multi-backend topology) and
`test_cluster_election` (test #69 — Raft leader election) are known to fail
intermittently in dev-container environments that do not support multiple
loopback listeners or real network isolation.  These failures are **not** caused
by any of the changes in this session and are tracked separately.  They pass
reliably in the dedicated CI `ubuntu-24.04` runner with the staggered-start fix
introduced in `test(cluster): stagger node starts in build_cluster`.

Run the test in isolation to confirm:

```bash
ctest --test-dir build -R "test_cluster$" --output-on-failure
```

### PXC 8.4 — Three-phase entrypoint and sequential startup requirement

Percona XtraDB Cluster 8.4 uses a **three-phase Docker entrypoint**:

1. **Phase 1** — `mysqld --initialize-insecure`: creates the data directory.
2. **Phase 2** — `mysqld --skip-networking`: starts Galera (briefly on port 4567) to
   run init SQL (create users, configure passwords). When this phase exits, the
   Galera view is lost and any joining node loses quorum.
3. **Phase 3** — `exec mysqld`: final production start with full Galera.

**Healthcheck requirement:** The Docker healthcheck must wait for
`wsrep_local_state_comment = Synced` (not just `mysqladmin ping`). If the
healthcheck uses `mysqladmin ping`, it can pass during phase 2 while Galera is
ephemeral, causing nodes 2/3 to start SST during phase 2 — and then fail with
`Failed to establish quorum` or `xtrabackup_checkpoints missing` when phase 2
exits.

**Sequential node startup:** Nodes 2 and 3 must be started sequentially (not
in parallel). Starting both simultaneously causes two concurrent xtrabackup SST
processes to conflict on the donor. The test script starts node2, waits for
`Synced`, then starts node3.

**SSL configuration:** `loose-pxc_encrypt_cluster_traffic = OFF` is required
in all node CNFs (`docker/mysql/config/pxc-node*.cnf`) to avoid
`data too large for modulus: certificate signature failure` errors that arise
when Galera SSL traffic mixes with unsigned Galera messages during the phase
transitions.

### PXC 8.4 — `grastate.dat` and `safe_to_bootstrap`

After a clean cluster shutdown, the node that was primary retains
`safe_to_bootstrap: 1` in `grastate.dat`. After an unclean shutdown (crash,
`docker kill`, power loss), **all nodes** may have `safe_to_bootstrap: 0` —
preventing any node from re-bootstrapping.

To recover: remove all PXC volumes to clear `grastate.dat` state:

```bash
docker compose -f docker/compose/mysql-pxc.yml down -v --remove-orphans
bash docker/tests/test-mysql-pxc.sh start
```

### ASAN build — config memory leak (fixed)

A config-scope memory leak was present in `src/main/main.c`: the
`keel_config_t*` pointer was declared inside an `if (opts.config_file)` block,
so `keel_config_free()` was never called. Fixed by widening the pointer scope
and adding `keel_config_free(config)` at shutdown before the scripting runtime
is torn down. The ASAN build now shows zero leaks (100/100 tests pass).

### io_uring timeout dangling-pointer fix

A production bug was discovered and fixed in `src/arch/linux/reactor_iouring.c`
while writing `test_reactor`.

**Root cause:** `iouring_timeout()` declared `struct __kernel_timespec ts` on the
stack and passed `&ts` to `io_uring_prep_timeout(sqe, &ts, 0, 0)`.  The SQE was
submitted asynchronously; by the time `io_uring_submit_and_wait_timeout()` ran
in the next `reactor_tick()` the stack frame had been destroyed and the kernel
was dereferencing a dangling pointer → garbage timeout values → timeouts never
fired.

**Fix:** A `struct __kernel_timespec ts` field was added directly to
`iouring_op_t`.  The `iouring_op_t` slots live in the heap-allocated
`pending_ops` array (which is never reallocated, ensuring pointer stability
until the CQE is processed), so `&op->ts` remains valid for the entire lifetime
of the in-flight SQE.

This bug would have caused silent infinite hangs on any code path that relied
on the reactor timeout, including connection timeouts, query timeouts, and
health-check probe intervals.
