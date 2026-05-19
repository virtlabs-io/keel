<img src="keel.png" alt="More than a pooler. The missing link between your application and a truly elastic database" width="300" height="200">

# KEEL - Database Connection Pooler

[![CI](https://github.com/virtlabs-io/dbcp-keel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/virtlabs-io/dbcp-keel/actions/workflows/ci.yml)
[![Hardening](https://github.com/virtlabs-io/dbcp-keel/actions/workflows/hardening.yml/badge.svg?branch=main)](https://github.com/virtlabs-io/dbcp-keel/actions/workflows/hardening.yml)
[![codecov](https://codecov.io/gh/virtlabs-io/dbcp-keel/graph/badge.svg)](https://codecov.io/gh/virtlabs-io/dbcp-keel)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

A high-performance, database-agnostic connection pooler written in modern C (C23) with native io_uring support, transaction pooling, intelligent routing, and full TLS support (frontend + backend). Supports both **PostgreSQL** and **MySQL** wire protocols. For `v0.2-alpha`, the recommended production deployment is conservative: **PostgreSQL in `pool` mode**, with higher-level routing and cluster features enabled deliberately rather than assumed by default.

Quick Links: [Docs](docs/) · [Production Readiness](docs/PRODUCTION_READINESS.md) · [Docker](docs/DOCKER.md) · [Testing](docs/TESTING.md) · [Benchmarks](bench/README.md) · [Scatter-Merge](docs/SCATTER_MERGE.md) · [Sharding](docs/SHARDING.md) · [Session Context](docs/SESSION_CONTEXT.md) · [Runtime Modes](docs/RUNTIME_MODES.md) · [Cluster Compression](docs/CLUSTER_WIRE_COMPRESSION.md)

## Overview

KEEL is a lightweight database connection pooler designed for high-throughput, low-latency environments. It supports **PostgreSQL** (v3 wire protocol) and **MySQL** (client/server protocol), including MySQL 9, Percona XtraDB Cluster, MariaDB Galera, and Group Replication topologies. It uses a share-nothing, per-worker architecture where each worker thread owns its own reactor (io_uring / kqueue / epoll), session slab, backend connection pool, and timer wheel — eliminating cross-thread locking in the fast path.

### Key Design Principles

- **Share nothing, scale linearly** — each worker is fully isolated
- **Zero-copy I/O** — Linux splice(2) for client↔backend data transfer
- **Async everything** — backend connect + SCRAM-SHA-256 auth runs entirely on the reactor
- **Multi-protocol** — PostgreSQL and MySQL from the same binary
- **Transaction pooling** — connections returned to the pool after each transaction

## Production Support Status for v0.2-alpha

Recommended deployment mode: `mode = pool` with `prepared_statement = virtualize` and `experimental_features = false`.

| Status | Features |
|--------|----------|
| **Production candidate** | PostgreSQL pool mode, PostgreSQL prepared-statement virtualization after replay validation, admin inspection and basic metrics |
| **Hardening** | Smart routing, SSV, Patroni failover, transaction tracking |
| **Experimental** | Sharding, scatter-merge, multi-shard 2PC, WAL/GTID catch-up probes, cluster compression |
| **Aspirational** | Result cache correctness guarantees |

`smart` and `full` remain useful tiers, but they are not the default production recommendation for `v0.2-alpha`. Promote them only when the corresponding failure-mode and observability gates in [PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md) are satisfied for your deployment.

## Feature Status

KEEL has a broad feature surface. The labels below keep the advertised model
aligned with the implementation state:

| Status | Meaning | Current examples |
|--------|---------|------------------|
| **Stable / implemented** | Production hot paths with focused tests and no known reactor-blocking calls. | Native reactor I/O, async backend connect/auth, transaction pooling, prepared-statement borrowing/replay, session-state sync, sticky-primary read-after-write safety, pool CLEANING reuse gate. |
| **Implemented / hardening** | Code exists and has targeted tests, but feature combinations are still being hardened with invariants and failure-mode coverage. | Session-context virtualization, transaction tracking, commit-in-doubt recovery, NOTIFY/LISTEN proxying, query rules, migration/drain behavior. |
| **Experimental / planned** | Prototype, advanced, or roadmap work that should not be treated as a default production guarantee yet. | Token-based replica catch-up via WAL LSN/GTID probes, sharding scatter-merge, multi-shard 2PC, result cache, multi-proxy cluster compression, fully reactor-owned consistency-token capture. |

For the full maturity inventory, failure-mode matrix, and failover semantics,
see [PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md). New production
claims should be added there before feature docs or UI copy are expanded.

### Production Profiles (v0.2-alpha)

`pool` is now the default runtime tier and only production-safe defaults are enabled by default.

```ini
[keel]
experimental_features = false

[worker_group.main]
mode = pool
prepared_statement = virtualize
result_cache = off
```

Experimental features require explicit opt-in:

```ini
[keel]
experimental_features = true

[worker_group.main]
mode = smart                  # hardening tier, not the default production tier
result_cache = on
scatter_merge = on
wal_lsn_capture = on
```

Startup logs now emit:

`Runtime tier: <tier>. Enabled features: [...]`

### Core
- **Native Reactor I/O** — io_uring on Linux 5.6+ (primary), kqueue on macOS, epoll as Linux fallback
- **PostgreSQL + MySQL** — full wire protocol support for both databases from a single binary
- **Transaction Pooling** — backend connections shared across clients between transactions
- **Per-Worker Backend Pools** — each worker maintains its own pool with async refill
- **Non-Blocking Backend Connect** — TCP connect + auth handshake via reactor state machine (zero `poll()` calls)
- **Connection Multiplexing** — multi-threaded worker groups with SO_REUSEPORT distribution
- **Connection Migration** — idle sessions transferred between workers via Unix socketpair SCM_RIGHTS + SPSC ring buffer for runtime load rebalancing
- **Prepared Statement Pooling** — four strategies for multiplexing PS-heavy applications: `virtualize` (replay), `pinning` (session affinity), `tracking` (simple+extended), `anonymous` (JIT rewrite) — see [PREPARED_STATEMENTS.md](docs/PREPARED_STATEMENTS.md)
- **XID Probe + Commit-in-Doubt Recovery** — instruments COMMIT with `txid_current()` and re-queries `txid_status()` on a fresh connection if the backend dies before `ReadyForQuery` (requires `transaction_tracking = on`)
- **Read-After-Write Safety** — stable sticky-primary routing keeps reads on the primary briefly after writes; WAL LSN/GTID replica catch-up probes are experimental until token capture and replica checks are fully reactor-owned
- **SSV (Semantic State Virtualization)** — atom-layer consistency model for prepared statement and GUC session state across pooled backends: hash-bucket pool index for O(1) backend matching, OPAQUE domain split, CONFIG domain atoms, consistency-token atom storage, and semantic replay — see [SSV_POSTGRES_IMPLEMENTATION.md](docs/SSV_POSTGRES_IMPLEMENTATION.md)
- **io_uring Linked SQEs** — backend send+recv chained as atomic io_uring sequences (IOSQE_IO_LINK), eliminating one syscall per round-trip
- **Registered FDs** — file descriptors pre-registered with the io_uring ring for lower per-operation kernel overhead (enabled by default)
- **Hook Chain Fast-Path** — per-engine `hook_mask` bitmask skips hook fire calls with a single integer comparison when no hooks are registered
- **TCP_QUICKACK** — disabled delayed ACKs on client and backend sockets for lower round-trip latency
- **Per-Worker RR Counters** — replica selection uses per-worker round-robin counters instead of global atomics
- **Single-Shot Accept** — fair connection distribution across workers (avoids multishot starvation)
- **Zero-Copy Splice** — Linux splice(2) for kernel-space data forwarding
- **MSG_PEEK + Splice DataRow Bypass** — `fast_network_path = on`: peeks at backend message headers (5-byte MSG_PEEK) and splices DataRow frames directly from backend socket → kernel pipe → client socket without any userspace copy; terminal frames (ReadyForQuery, ErrorResponse) still go through the full engine protocol path — see [QUERY_FLOW.md](docs/QUERY_FLOW.md)
- **Result Cache Framework** — `result_cache = on|off` config parameter prepares for future query result caching; when enabled, disables the splice bypass so data is captured in userspace
- **TLS + mTLS + kTLS** — frontend TLS termination, backend TLS, optional client-cert verification, kernel TLS acceleration, cipher suite enforcement, cert hot-reload via SIGHUP, and downgrade protection
- **Graceful Drain/Shutdown** — lifecycle state machine (CREATED → ACTIVE → DRAINING → STOPPING → STOPPED), configurable drain timeout, CID-aware force-close that protects commit-in-doubt sessions, PostgreSQL FATAL 57P03 error on drain rejection
- **Pool Allocators** — O(1) free-list allocation for recv contexts (~400 B metadata, heap-backed I/O buffers allocated lazily) and pool waiters
- **Non-Blocking Pool Cleanup** — dirty connection cleanup enters `CLEANING`, sends `DISCARD ALL` with `MSG_DONTWAIT`, and only returns the backend after PostgreSQL `ReadyForQuery('I')`
- **Non-Blocking State Sync** — backend state sync is a pre-query reactor phase; the client query is not forwarded until sync responses are drained through `ReadyForQuery`
- **Reactor Blocking Gate** — `scripts/check_forbidden_blocking.sh` fails CI if forbidden blocking calls appear in worker/engine/pool hot-path files
- **Session-Context Preservation** — transparent SET parameter, search_path, and session variable continuity across backend reassignment via sorted K/V state profiles, XXHash64 fingerprinting, two-pointer merge diff, and 5-tier pool borrow — see [SESSION_CONTEXT.md](docs/SESSION_CONTEXT.md)
- **Runtime Mode Tiers** — four operating tiers (PROXY / POOL / SMART / FULL) that gate features at compile-checked hot-path macros; PROXY tier disables SQL parsing for raw throughput — see [RUNTIME_MODES.md](docs/RUNTIME_MODES.md)
- **Cross-Feature Invariant Model** — formal 12×12 compatibility matrix with runtime checker and 20 violation classes ensuring feature combinations are safe
- **Formal State Machine Model** — 9-domain session state machine (phase, replay, CID, TLS, drain, pool, PS, txn, auth) with contracts, transition matrices, predicates, and event journal — see [STATE_MODEL.md](docs/STATE_MODEL.md)
- **Admission Control** — per-worker frontend/backend connection limits with bounded FIFO wait queue, pressure feedback, peak tracking, and queue timeout — prevents thundering-herd overload
- **Route Cache** — per-worker 1024-entry XXHash64 L1 cache for query→route decisions; 8-probe linear search with LRU eviction avoids redundant SQL analysis
- **Zero-poll Hot Path** — entire connect/auth/query path runs without `poll()` syscalls on io_uring
- **Async Pool Warmup** — pre-connects backend connections asynchronously during startup
- **Auto FD Limit** — raises `RLIMIT_NOFILE` to hard max at startup (up to 1M file descriptors)
- **Crash Signal Handlers** — catches SIGSEGV/SIGABRT/SIGBUS with async-signal-safe diagnostics
- **Modern C23** — written in C23 with strict compiler warnings (`-Wall -Wextra -Werror`)

### SQL & Routing
- **Full SQL Lexer & Parser** — tokenizer → recursive descent parser → query tree (AST)
- **Automatic Read/Write Splitting** — reads to replicas, writes to primary
- **Weighted Load Balancing** — configurable weights per backend server
- **Transaction Pinning** — server affinity maintained during open transactions
- **FOR UPDATE Detection** — locking reads routed to primary
- **Read-After-Write Safety** — sticky-primary override is stable; WAL LSN/GTID token checks are experimental until their capture and replica probes are reactor-owned
- **Cross-Service Read-Your-Writes** — propagate write positions across independent services via `SET keel.read_after_lsn = '<lsn>'` and `SHOW keel.write_lsn`; intercepted at the proxy with no backend round-trip; injects LSN into the session's consistency atoms for routing policy
- **Sticky-Primary Override** — after a write, reads are temporarily pinned to primary (100 ms window) before replica routing resumes
- **NOTIFY/LISTEN Proxying** — transparent proxy of PostgreSQL `NOTIFY` and `LISTEN`/`UNLISTEN` messages; notification payloads forwarded to all subscribed client sessions without exposing backend connection details
- **Declarative Query Rules** — INI-based query routing, blocking, rewriting, and tagging rules; no-code alternative to Lua/Python hooks; supports regex matching, route overrides (`primary`/`replica`/`any`), hard block, and query rewriting with capture groups
- **Online Schema Change Proxying** — detects active OSC tools (gh-ost, pt-online-schema-change, Flyway, Liquibase) and pins their connections to the primary for the duration of the schema migration; connection affinity released automatically when the OSC session closes
- **CTE / Window / Set-Operation Support** — full SQL parser coverage for `WITH`, `OVER`, `UNION`, `INTERSECT`, `EXCEPT`, `MERGE`, `LOCK`, `LISTEN`, `VACUUM`
- **Pluggable Router Plugins** — per-database routing policies via `keel_router_plugin_ops_t`

### Authentication
- **SCRAM-SHA-256** — full client + backend auth for PostgreSQL (OpenSSL)
- **MD5** — legacy PostgreSQL password auth
- **caching_sha2_password** — MySQL default auth plugin (OpenSSL)
- **mysql_native_password** — MySQL legacy auth
- **Trust / Password** — simple auth methods
- **User File Support** — external credential management (`userlist.txt`)
- **Cloud-Native Authentication** — AWS RDS IAM (SigV4 token generation, 14-minute cache), GCP Cloud SQL IAM (service account JWT + metadata server + token file fallback), Azure AD/Entra ID (IMDS managed identity + env var + token file fallback) — see [CLOUD_AUTH.md](docs/CLOUD_AUTH.md)
- **Enterprise Authentication** — PAM (`pam_authenticate()` in separate thread), LDAP (`ldap_bind()` + search with session-level result caching), mTLS certificate identity (extract username from client cert CN/SAN, no password challenge), auth query (validate credentials via a configurable SQL function on the backend) — pluggable `keel_auth_provider_ops_t` vtable

### Health & Failover
- **Pluggable Probe System** — postgres (SQL), patroni (HTTP), mysql, tcp, exec (script)
- **Automatic Role Detection** — `pg_is_in_recovery()` for PostgreSQL, REST API for Patroni
- **Patroni Cluster Discovery** — `GET /cluster` (all members) with `/patroni` fallback; pure-C HTTP/1.0 client, no external library dependencies
- **Failover Handling** — dead servers removed, role changes detected, routing adjusted
- **WAL Position Tracking** — replication lag monitoring via LSN

### Observability
- **Per-Worker Statistics** — queries_total, queries_read, queries_write, queries_tx, bytes counters, migrations_sent, migrations_received
- **Latency Histograms** — P50/P95/P99 per worker merged to Prometheus histogram format
- **TLS/kTLS Metrics** — TLS handshake, kTLS activation/fallback/failure, cert reload, cert reload failure, and downgrade rejection counters via admin `SHOW STATS` and Prometheus `/metrics`
- **Pluggable Log Backend** — stdout, file, and syslog log plugins with hot-loaded configuration
- **Structured NDJSON Logging** — `log_format = json` emits newline-delimited JSON with trace correlation fields (trace_id, span_id) for log aggregation pipelines
- **Audit Logging** — structured security audit log with event-type filtering (auth, admin, query, pool): `[audit]` INI section; NDJSON or text output to file, syslog, or webhook
- **Query Logging** — configurable per-query log with mode filter (all, read, write, none)
- **Prometheus Metrics** — `/metrics` endpoint for Prometheus scraping with pool, session, TLS, shard, and query counters
- **Grafana Dashboard** — pre-built JSON at `etc/grafana/keel-dashboard.json`
- **Admin Console** — PostgreSQL wire protocol on dedicated TCP port; 21+ commands including virtual tables, JSON output, K8s health endpoints — see [ADMIN_SQL.md](docs/ADMIN_SQL.md)
- **Web Management UI** — self-contained dark-themed SPA served at `GET /ui` on the Prometheus port; auto-refreshes every 5 s; shows engine state, workers, uptime, sessions, pool, queries, and errors; no external dependencies
- **JSON Status API** — `GET /api/status.json` returns current engine state and aggregated metrics as JSON; CORS-enabled for direct browser fetch
- **Distributed Tracing (OpenTelemetry)** — per-session W3C `traceparent` injection into SQL block comments, per-query spans with SQL digest and route decision, OTLP/HTTP JSON export, per-worker lock-free span ring buffer, configurable head-based sampling rate — see [TRACING.md](docs/TRACING.md)

### SQL Routing (Advanced)
- **Horizontal Sharding** — transparent shard-key extraction from SQL AST (SELECT/INSERT/UPDATE/DELETE), int64-modulo and xxhash64-string shard mapping, `$N` bound-parameter resolution for prepared statements, SINGLE / SCATTER / UNSUPPORTED routing plans, scatter fan-out with per-shard read/write routing, in-transaction and temp-table primary forcing, per-shard counters, scatter aggregation callbacks, range-based shard maps (INT64 threshold table), multi-shard transaction coordinator (cross-shard write validation), shard migration state (dual-write + read-from-new), admin virtual tables (`SHOW SHARD RULES`, `EXPLAIN SHARD PLAN FOR '<sql>'`), INI-persistent rule registry, SIGHUP hot-reload, Prometheus metrics — see [SHARDING.md](docs/SHARDING.md)
- **Scatter-Merge Queries** — transparent cross-shard aggregation with zero application changes: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT col)` merged globally; `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET` all applied post-merge; built-in LIMIT/OFFSET correctness fix prevents per-shard GROUP truncation (`sc_strip_limit_offset`); 2PC coordinator for scatter writes with deterministic GIDs and terminal-state guards; per-shard `SO_RCVTIMEO = 30 s`; `EXPLAIN SHARD PLAN FOR` returns 7-column plan metadata; Prometheus histogram `keel_router_scatter_merge_duration_seconds` (10 buckets, 1ms–2.5s) — see [SCATTER_MERGE.md](docs/SCATTER_MERGE.md)

  **Quick-start (3 minutes):**
  ```ini
  # keel.ini
  [shard_rule.orders]
  table = orders
  column = user_id
  shard_count = 2
  strategy = hash

  [shard_backend.0]
  host = shard0.local
  port = 5432
  database = mydb
  user = app

  [shard_backend.1]
  host = shard1.local
  port = 5432
  database = mydb
  user = app
  ```
  ```sql
  -- Connect to KEEL, not directly to any shard
  SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
  FROM orders
  GROUP BY status
  ORDER BY total DESC
  LIMIT 5;
  -- KEEL fans out to both shards, merges groups, sorts, and returns top-5 globally.
  ```
- **Declarative Query Rules** — INI `[query_rule.*]` sections for no-code routing, blocking, and rewriting rules; POSIX ERE matching on SQL text, username, and database; `route_to` (primary/replica/any), `rewrite_to` (SQL replacement with capture groups), `block` (synthetic error); evaluated after parse, before normal routing; zero hook code required
- **Online Schema Change Proxying** — detects gh-ost (`_gho`, `_ghc`) and pt-online-schema-change (`_new`, `_old`) shadow-table DML; pins the session to the primary for the migration duration; transparent to the OSC tool — no config changes required
- **Cross-Service Read-Your-Writes** — `SET keel.read_after_lsn = '<lsn>'` stores a client-supplied WAL LSN in the session's consistency atoms; `SHOW keel.write_lsn` returns the latest token known to the protocol context; token-based replica catch-up remains experimental until checks are reactor-owned
- **NOTIFY/LISTEN Transparent Proxying** — `LISTEN`/`UNLISTEN`/`NOTIFY` intercepted at the protocol layer; LISTEN pins the session to a dedicated backend (session-mode semantics for that session only); UNLISTEN releases the pin and returns to transaction-mode pool; `UNLISTEN *` handled; sessions with active subscriptions survive idle timeout and pool drain

### Extensibility
- **Hook/Trigger System** — 4 query pipeline extension points (after_query_read, after_query_parse, before_route, before_send)
- **Lua Hooks** — embedded Lua 5.4/LuaJIT scripting (enabled by default, disable with `-DKEEL_ENABLE_LUA=OFF`)
- **Python Hooks** — embedded CPython 3.x scripting (enabled by default, disable with `-DKEEL_ENABLE_PYTHON=OFF`)
- **Native Plugin Hooks** — `.so` / `.dylib` shared libraries loaded via `dlopen()` (always available)
- **Mutable Context** — hooks can inspect and modify session, query, routing, and flags in-place
- **Abort Control** — hooks return true/false to continue or abort query processing
- **Priority Chains** — hooks execute in priority order; mixing Lua + Python + native at the same point

### Operations
- **Live Configuration Reload** — SIGHUP reloads pool sizes, timeouts, server weights, probe timing, rebalancing config, TLS certificates, log level, shard rules, query rules, throttle rules, and audit config without restart
- **Graceful Drain/Shutdown** — lifecycle state machine (CREATED → ACTIVE → DRAINING → STOPPING → STOPPED), configurable drain timeout, CID-aware force-close that protects commit-in-doubt sessions, PostgreSQL FATAL 57P03 error on drain rejection
- **Multi-Proxy HA Cluster** — 2–3 KEEL instances form a cluster with heartbeat-based peer health monitoring, config gossip (checksum-based reconciliation), transitive peer discovery, and NOTIFY_SERVER event delivery; wire protocol uses magic `0x4B454C43`; transparent **wire-protocol compression** (zlib/zstd) for WAN/cross-region deployments; configured via `[cluster]` INI section or `KEEL_CLUSTER_*` environment variables — see [CLUSTER_WIRE_COMPRESSION.md](docs/CLUSTER_WIRE_COMPRESSION.md)
- **Connection Lifecycle Management** — `max_connection_age_ms` / `max_connection_age_s` closes and replaces backend connections older than a configured maximum; per-user/per-database quotas; idle eviction; declarative pool prefill
- **Query Throttling & Rate Limiting** — per-rule token-bucket rate limiter via `[throttle.N]` INI sections; rules match by user, database, query pattern, or default; configurable burst and sustained rate; returns standard too-many-requests error to the client
- **Release Packaging** — tar.gz, DEB, and RPM packages via CPack with man pages (`keel(1)`, `keel.ini(5)`), systemd unit, post-install user/group creation, logrotate config
- **Kubernetes Native** — Helm chart (`helm/keel/`) with ConfigMap, Secret, StatefulSet, PodMonitor; Go controller-runtime operator (`operator/`) with `KeelPool` CRD, reconcile loop, distroless Dockerfile; HPA guidance on `pool_wait_queue_enqueued` metric
- **Docker Official Images** — multi-arch (`linux/amd64`, `linux/arm64`) images at `ghcr.io/virtlabs/keel:latest`; `KEEL_*` env var config via entrypoint; production Compose templates — see [DOCKER.md](docs/DOCKER.md)
- **Built-in Certificate Authority** — `SHOW CERTIFICATES` lists loaded certs with subject, issuer, validity dates, and SHA-256 fingerprint; `RELOAD CERTS` forces atomic SSL_CTX swap; `tls_auto_renew_days` threshold for automated renewal
- **Hardening CI** — unified CI gate: sanitizer matrix (ASan+UBSan, TSan, MSan), shadow diff, backpressure tests, syscall fault injection, network chaos, checksec verification, TLS audit, sqlmap fuzzing
- **GitHub Actions CI/CD** — build+test on every push/PR; sanitizer matrix on PRs; weekly hardening schedule; automated DEB/RPM/TGZ release packaging on tags

### Memory
- **Arena Allocator** — fast bump allocation for request-scoped memory
- **Slab Allocator** — fixed-size object pools for sessions and connections
- **Ring Buffer** — lock-free circular buffer for protocol data
- **Pool Allocator** — O(1) free-list allocation for recv contexts (~400 B metadata) and pool waiters
- **NUMA-Aware Allocator** — node-local and interleaved allocation via `mmap`+`mbind` (no libnuma dependency)
- **Lazy I/O Buffers** — heap-backed buffers allocated on demand, not at pool creation (300× lower idle memory vs embedded 64 KB arrays)
- **Leak Detection** — debug builds track all allocations with canary validation

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Client Applications                           │
│           (psql, pgbench, mysql, sysbench, app servers)              │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ TCP (port 7432 / 7306)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     Worker Group (SO_REUSEPORT)                      │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐    │
│  │    Worker 0      │  │    Worker 1      │  │    Worker N      │    │
│  │                  │  │                  │  │                  │    │
│  │ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │    │
│  │ │   Reactor    │ │  │ │   Reactor    │ │  │ │   Reactor    │ │    │
│  │ │ (io_uring)   │ │  │ │ (io_uring)   │ │  │ │ (io_uring)   │ │    │
│  │ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │    │
│  │ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │    │
│  │ │ Session Slab │ │  │ │ Session Slab │ │  │ │ Session Slab │ │    │
│  │ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │    │
│  │ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │    │
│  │ │ Backend Pool │ │  │ │ Backend Pool │ │  │ │ Backend Pool │ │    │
│  │ │ (RW/RO/WO    │ │  │ │ (RW/RO/WO    │ │  │ │ (RW/RO/WO    │ │    │
│  │ │  node pools) │ │  │ │  node pools) │ │  │ │  node pools) │ │    │
│  │ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │    │
│  │ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │    │
│  │ │ Timer Wheel  │ │  │ │ Timer Wheel  │ │  │ │ Timer Wheel  │ │    │
│  │ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │    │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            ▼                  ▼                  ▼
     ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
     │  Primary    │   │  Replica 1  │   │  Replica 2  │
     │  (5432)     │   │  (5433)     │   │  (5434)     │
     └─────────────┘   └─────────────┘   └─────────────┘
```

### Request Flow

```
Client → accept (SO_REUSEPORT) → Worker
  → Frontend Protocol (StartupMessage, Auth — SCRAM/MD5/caching_sha2/LDAP/PAM/mTLS/cloud)
  → Session created from slab
  → Query Rules check (declarative INI rules evaluated before parse)
  → SQL Parser (Lexer → Parser → Query Tree) [skipped in PROXY mode]
  → Throttle check (token-bucket rate limiter)
  → OSC / NOTIFY / LISTEN / keel.* GUC intercept
  → Router (read/write split, shard dispatch, hook chain, plugin)
  → Backend Pool borrow (or queue + async refill)
  → Backend Protocol (proxy query, relay results, zero-copy splice)
  → Backend Pool return (transaction complete)
  → Stats update (counters, histogram, tracing span)
```

### Multi-Proxy Cluster Mode

```
                    ┌─────────────────────────────────────────┐
                    │   KEEL Cluster (gossip on TCP 7000)      │
                    │                                          │
               ┌────┤  KEEL Node 0       KEEL Node 1          │
  Clients ─────┤    │  (active)          (active)             │
  (load         └────┤  port 7432         port 7432            │
  balanced)          └─────────────────────────────────────────┘
                                │ heartbeat / config sync
                         Primary + Replicas
```

Each node runs independently; the cluster layer propagates configuration changes and detects peer failures. No shared state in the query path — full share-nothing semantics are preserved.

## Project Structure

```
keel/
├── include/keel/                   # Public API headers (60 files)
│   ├── keel.h                      # Master include
│   ├── reactor.h                  # Platform-agnostic reactor interface
│   ├── worker.h                   # Worker thread & group management
│   ├── engine.h / engine_flow.h   # Session engine & data flow
│   ├── session.h                  # Client session lifecycle
│   ├── pool.h                     # Connection pool management
│   ├── protocol.h                 # Protocol abstraction
│   ├── protocol_vtable.h          # Protocol virtual dispatch
│   ├── protocol_flow.h            # Protocol flow state machine
│   ├── protocol_action.h          # Protocol action types
│   ├── router.h                   # Query routing core
│   ├── router_plugin.h            # Pluggable routing plugins
│   ├── router_metadata.h         # Metadata-aware routing
│   ├── router_discovery.h         # Server discovery & probing
│   ├── sql.h / sql_ast.h          # SQL parser & AST
│   ├── sql_query_tree.h           # Query tree types
│   ├── auth.h / backend_auth.h    # Client & backend authentication
│   ├── mem.h / mem_safety.h       # Memory management
│   ├── ringbuf.h                  # Ring buffer
│   ├── io_splice.h                # Zero-copy splice
│   ├── admission.h                # Connection admission control
│   ├── hardpin.h                  # Hard session pinning
│   ├── route_cache.h              # Route decision cache
│   ├── state_profile.h            # Session state profiling
│   ├── probe.h                    # Health check probes
│   ├── log.h / print.h            # Logging & output
│   ├── ini.h                      # INI config parser
│   ├── string_view.h              # Non-owning string view
│   ├── xxhash.h                   # Fast hashing
│   ├── types.h / error.h / util.h # Core types & utilities
│   └── ...
├── src/
│   ├── arch/                      # Platform-specific reactor backends
│   │   ├── common/reactor_common.c
│   │   ├── linux/
│   │   │   ├── reactor_iouring.c  # io_uring reactor (Linux 5.6+)
│   │   │   ├── reactor_epoll.c    # epoll reactor (Linux fallback)
│   │   │   └── io_splice.c        # Zero-copy splice
│   │   └── macos/
│   │       └── reactor_kqueue.c   # kqueue reactor (macOS/BSD)
│   ├── core/                      # Core services
│   │   ├── auth.c                 # Client authentication (SCRAM, MD5)
│   │   ├── config.c               # INI configuration parser
│   │   ├── pool.c                 # Connection pool logic
│   │   ├── router_weighted.c      # Weighted routing
│   │   ├── router_plugin.c        # Plugin manager
│   │   ├── router_metadata.c      # Metadata cache
│   │   └── router_discovery.c     # Server discovery & probing
│   ├── engine/                    # Session engine
│   │   ├── engine.c               # Engine core
│   │   ├── engine_flow.c          # Data flow (client↔backend)
│   │   └── backend_auth.c         # Backend SCRAM-SHA-256 auth
│   ├── worker/                    # Worker threads
│   │   ├── worker.c               # Per-core worker (reactor loop, timers)
│   │   ├── migration.c/.h         # Worker connection migration (SCM_RIGHTS + SPSC ring)
│   │   ├── backend_pool.c/.h      # Per-worker backend connection pool
│   │   └── backend_connect_async.c/.h  # Async backend connect state machine
│   ├── session/                   # Session management
│   │   ├── session.c              # Session lifecycle
│   │   ├── admission.c            # Admission control
│   │   ├── hardpin.c              # Hard pinning
│   │   ├── residual.c             # Residual data handling
│   │   ├── route_cache.c          # Route decision cache
│   │   └── state_profile.c        # State profiling
│   ├── protocol/                  # Protocol implementations
│   │   ├── postgres/              # PostgreSQL wire protocol (v3)
│   │   │   ├── postgres_proto.c   # Message serialization/parsing
│   │   │   └── postgres_flow.c    # Protocol flow state machine
│   │   ├── mysql/                 # MySQL client/server protocol
│   │   │   ├── mysql_proto.c      # Packet serialization/parsing
│   │   │   ├── mysql_flow.c       # Protocol flow state machine
│   │   │   └── mysql_backend_auth.c  # Backend auth (caching_sha2)
│   │   ├── protocol_registry.c    # Protocol registry
│   │   └── protocol_flow_registry.c
│   ├── sql/                       # SQL analysis
│   │   ├── lexer.c                # SQL tokenizer
│   │   ├── parser.c               # Recursive descent parser
│   │   ├── query_tree.c           # Query tree builder
│   │   └── analyzer.c             # Query classification
│   ├── mem/                       # Memory subsystem
│   │   ├── arena.c                # Arena (bump) allocator
│   │   ├── slab.c                 # Slab allocator
│   │   ├── pool.c                 # Pool allocator
│   │   ├── ringbuf.c              # Ring buffer
│   │   │   ├── mem.c                  # Memory utilities
│   │   ├── mem_safety.c           # Debug leak tracking
│   │   └── numa.c                 # NUMA-aware allocator
│   ├── log/                       # Logging subsystem
│   │   ├── log_plugin_stdout.c    # Console log plugin
│   │   ├── log_plugin_file.c      # File log plugin
│   │   ├── log_plugin_syslog.c    # Syslog log plugin
│   │   ├── log_plugin_loader.c    # Plugin loader
│   │   └── query_log.c            # Per-query logging
│   ├── hook/                       # Hook/trigger system
│   │   ├── hook.c                 # Core dispatch, dlopen plugin loader
│   │   ├── lua_bridge.c           # Lua 5.4/LuaJIT integration
│   │   └── python_bridge.c        # CPython 3.x integration
│   ├── stats/stats.c              # Per-worker statistics
│   ├── admin/admin.c              # Admin console listener
│   ├── util/                      # Utilities
│   │   ├── log.c / print.c        # Logging & formatted output
│   │   ├── hash.c / xxhash.c      # Hash functions
│   │   ├── string.c / string_view.c
│   │   ├── buffer.c               # Buffer management
│   │   ├── error.c                # Error handling
│   │   ├── time.c                 # Time utilities
│   │   └── hashring.c             # Consistent hashing
│   └── main/main.c               # Entry point
├── tests/                         # Test suite (116 tests)
│   ├── test_mem.c                 # Memory allocator tests
│   ├── test_auth.c                # Authentication tests
│   ├── test_parser.c              # SQL parser tests
│   ├── test_router.c              # Query routing tests
│   ├── test_router_plugin.c       # Router plugin tests
│   ├── test_sql.c                 # SQL analysis tests
│   ├── test_config.c              # Configuration tests
│   ├── test_log.c                 # Logging tests
│   ├── test_query_log.c           # Per-query logging tests
│   ├── test_util.c                # Utility tests
│   ├── test_string_view.c         # String view tests
│   ├── test_ringbuf.c             # Ring buffer tests
│   ├── test_xxhash.c              # Hash function tests
│   ├── test_residual.c            # Residual data tests
│   ├── test_session_engine.c      # Session engine tests
│   ├── test_pool_correctness.c    # Pool borrow/return tests
│   ├── test_plugin_contract.c     # Plugin contract tests
│   ├── test_pg_protocol_flow.c    # PostgreSQL protocol flow tests
│   ├── test_mysql_protocol_flow.c # MySQL protocol flow tests
│   ├── test_hooks.c               # Hook system tests
│   ├── test_failover.c            # Failover & probe tests
│   ├── test_migration.c           # Worker connection migration tests
│   ├── test_tls_ktls.c            # TLS handshake + kTLS activation/fallback tests
│   └── ...                        # Integration & E2E test files
├── etc/                           # Configuration
│   ├── keel-pg.ini                 # PostgreSQL configuration
│   ├── keel-my.ini                 # MySQL configuration
│   ├── keel.ini.example            # Annotated config reference
│   ├── certs/                     # Sample CA/server/client test PKI
│   └── userlist.txt               # Client credentials
├── examples/hooks/                # Hook examples & templates
│   ├── lua/                       # Lua hook scripts
│   ├── python/                    # Python hook modules
│   ├── plugins/                   # Native .so plugin sources
│   └── hooks.ini.example          # Hook configuration reference
├── docs/                          # Documentation
│   ├── STARTUP.md                 # Startup flow & memory architecture
│   ├── CONNECTION_FLOW.md         # Connection lifecycle & pooling
│   ├── QUERY_FLOW.md              # Query execution & routing flow
│   ├── MULTIPLEXING.md            # Worker architecture deep-dive
│   ├── HOOKS.md                   # Hook/trigger system
│   ├── PREPARED_STATEMENTS.md     # Prepared statement pooling strategies
│   ├── TRANSACTION_TRACKING.md    # XID probe & read-after-write consistency
│   ├── TESTING.md                 # Testing guide
│   ├── ADMIN_SQL.md               # Admin SQL virtual tables & commands
│   ├── OPERATIONS.md              # Day-2 operations guide
│   ├── SESSION_CONTEXT.md         # Session-context preservation feature guide
│   ├── RUNTIME_MODES.md           # Runtime mode tiers (PROXY/POOL/SMART/FULL)
│   ├── STATE_MODEL.md             # Formal 9-domain session state machine
│   ├── SSV_POSTGRES_IMPLEMENTATION.md # SSV engine implementation
│   └── ROADMAP.md                 # Project roadmap & future plans
├── docker/                        # Docker & E2E testing
│   ├── compose/                   # Docker Compose stacks
│   │   ├── pg-e2e.yml             # PostgreSQL E2E test stack
│   │   ├── pg-patroni.yml         # PostgreSQL Patroni cluster
│   │   ├── pg-streaming.yml       # PostgreSQL streaming replication
│   │   ├── mysql-replication.yml  # MySQL 9 async replication
│   │   ├── mysql-group.yml        # MySQL 9 Group Replication
│   │   ├── mysql-pxc.yml          # Percona XtraDB Cluster 8.4
│   │   └── mysql-mariadb.yml      # MariaDB Galera Cluster
│   ├── tests/                     # E2E test runner scripts
│   │   ├── test-pg-e2e-full.sh    # PostgreSQL full E2E
│   │   ├── test-pg-patroni.sh     # Patroni HA cluster
│   │   ├── test-pg-streaming.sh   # Streaming replication
│   │   ├── test-mysql-replication.sh  # MySQL async replication
│   │   ├── test-mysql-group.sh    # MySQL Group Replication
│   │   ├── test-mysql-pxc.sh      # Percona XtraDB Cluster
│   │   └── test-mysql-mariadb.sh  # MariaDB Galera
│   ├── Dockerfile.build           # Build image
│   └── Dockerfile.e2e             # E2E test image
├── cmake/
│   └── keel_config.h.in            # Generated config header template
├── CMakeLists.txt                 # Top-level build
└── LICENSE                        # AGPL-3.0
```

## When to Use KEEL

### Best Fit

| Scenario | Why KEEL |
|----------|----------|
| **High-concurrency PostgreSQL or MySQL** | Transaction pooling multiplexes hundreds of app threads over a small backend pool. io_uring eliminates syscall overhead at high connection counts. |
| **Read/write splitting with replicas** | Automatic SQL classification routes SELECTs to replicas and writes to primary, with sticky-primary override and configurable weights. |
| **ORM-heavy applications (Hibernate, GORM, SQLAlchemy, Prisma, pgx)** | `prepared_statement = virtualize` transparently replays named prepared statements on any backend; no ORM changes required. |
| **Multi-tenant SaaS with session isolation** | Session-context preservation (SSV) keeps per-session GUCs and search_path consistent across backend reassignment. |
| **Read-after-write safety** | Sticky-primary routing is stable after writes; cross-service LSN/GTID tokens can be stored and surfaced, while replica catch-up probes remain experimental until fully reactor-owned. |
| **Kubernetes / cloud-native deployments** | Helm chart, CRD operator, `KEEL_*` env var config, `ghcr.io/virtlabs/keel:latest` image, K8s health endpoints, HPA integration. |
| **AWS RDS / GCP Cloud SQL / Azure** | Cloud-native auth plugins handle IAM token generation and rotation automatically — no password rotation scripts needed. |
| **Online schema changes (gh-ost, pt-osc)** | OSC proxying pins shadow-table queries to the primary automatically; no separate session-mode listener required. |
| **PostgreSQL NOTIFY/LISTEN pub-sub** | Transparent LISTEN pinning keeps pub-sub working through a transaction-mode pool. |
| **Scripted proxy logic** | Hook chain (Lua, Python, native .so) at 4 pipeline stages; declarative query rules for config-only routing/blocking/rewriting. |
| **Horizontal sharding** | Transparent shard-key extraction from SQL AST, scatter aggregation, admin virtual tables — no application changes for single-shard queries. |
| **Multi-proxy HA** | Two or three KEEL instances gossip configuration and monitor each other; no external load balancer required for HA at the proxy layer. |
| **Compliance / regulated environments** | Audit logging, seccomp syscall filter, privilege drop, mTLS, LDAP/PAM integration, structured NDJSON log output. |

### Consider Alternatives When

| Situation | Alternative / Reason |
|-----------|---------------------|
| **Single long-lived session per application thread** | Overhead of a proxy is net negative. Connect directly to PostgreSQL. |
| **pgAdmin / schema migration tools** | Set `mode = proxy` for these connections — full transaction pooling is unnecessary and can interfere with DDL that spans multiple round-trips. |
| **Applications requiring PostgreSQL COPY streaming** | Large COPY operations must not be interrupted mid-stream by pool rebalancing; use `mode = proxy` or session pinning for the COPY session. |
| **MySQL stored procedures with temporary tables across transactions** | MySQL temporary tables are connection-scoped. Use `mode = proxy` or `prepared_statement = pinning` equivalent for those sessions. |
| **Extremely low traffic (< 10 concurrent clients)** | The overhead of connection pooling is not justified. PgBouncer in session mode is simpler. |
| **Applications that rely on `pg_backend_pid()` for locking** | Backend PID changes on each borrow. Use `mode = proxy` (session affinity) if advisory lock by PID is required. |
| **PostgreSQL LISTEN in high-frequency notification scenarios** | Each LISTEN session holds a dedicated backend connection. If most sessions LISTEN, pool efficiency is lost — a dedicated notification worker is more efficient. |
| **Windows production deployments** | Native Windows support is planned (P3) but not yet delivered. Linux or Linux containers are required for production. |

### Deployment Scenarios

#### Scenario 1: Simple PostgreSQL Pool (replacing PgBouncer)

```ini
[worker_group.myapp]
protocol          = postgresql
bind_addr         = 0.0.0.0
bind_port         = 5432
num_workers       = 4
mode              = pool           # no SQL parse, just pooling
prepared_statement = virtualize    # transparent PS replay
min_pool_size     = 5
max_pool_size     = 50

[worker_group.myapp.servers]
primary = host=db.local port=5432 dbname=app user=app password=secret role=RW weight=100
```

#### Scenario 2: PostgreSQL with Read/Write Splitting + Patroni HA

This profile is in the **hardening** bucket for `v0.2-alpha`. Use it only when
you need read/write routing and have validated failover behavior in your own environment.

```ini
[worker_group.prod]
protocol            = postgresql
bind_port           = 5432
mode                = smart        # hardening, not default production tier
prepared_statement  = virtualize
transaction_tracking = on          # read-after-write consistency
min_pool_size       = 10
max_pool_size       = 200
probe               = patroni:8008

[worker_group.prod.servers]
node1 = host=10.0.0.1 port=5432 dbname=prod user=app password=s role=auto weight=100
node2 = host=10.0.0.2 port=5432 dbname=prod user=app password=s role=auto weight=100
node3 = host=10.0.0.3 port=5432 dbname=prod user=app password=s role=auto weight=100
```

#### Scenario 3: MySQL with Group Replication

```ini
[worker_group.mysql]
protocol        = mysql
bind_port       = 3306
mode            = smart
min_pool_size   = 5
max_pool_size   = 100
probe           = mysql
probe_interval  = 3000

[worker_group.mysql.servers]
node1 = host=10.1.0.1 port=3306 dbname=app user=app password=s role=auto weight=100
node2 = host=10.1.0.2 port=3306 dbname=app user=app password=s role=RO weight=100
node3 = host=10.1.0.3 port=3306 dbname=app user=app password=s role=RO weight=100
```

#### Scenario 4: Kubernetes with Helm

```bash
helm install keel helm/keel/ \
  --set config.workerGroup.bindPort=5432 \
  --set config.workerGroup.servers[0].host=postgres-primary.default.svc \
  --set credentials.serverPassword=secret \
  --set monitoring.enabled=true
```

#### Scenario 5: Multi-Proxy HA Cluster

```ini
[cluster]
enabled       = true
node_id       = keel-0
listen_port   = 7000
initial_peers = 10.0.0.2:7000,10.0.0.3:7000
```

```ini
# On node 1 (10.0.0.2):
[cluster]
enabled       = true
node_id       = keel-1
listen_port   = 7000
initial_peers = 10.0.0.1:7000,10.0.0.3:7000
```

**WAN/cross-region:** enable wire compression to reduce gossip bandwidth:

```ini
[cluster]
enabled                  = true
node_id                  = keel-0
listen_port              = 7000
initial_peers            = 10.1.0.2:7000,10.2.0.3:7000
compress                 = zstd   ; none | zlib | zstd
compress_threshold_bytes = 256
```

See [CLUSTER_WIRE_COMPRESSION.md](docs/CLUSTER_WIRE_COMPRESSION.md) for protocol details, codec selection, and Docker image dependencies.

#### Scenario 6: Horizontal Sharding

```ini
[shard_rule.users]
table      = users
key_column = user_id
strategy   = modulo
shards     = 4

[shard_rule.users.servers]
shard0 = host=shard0.db port=5432 dbname=app
shard1 = host=shard1.db port=5432 dbname=app
shard2 = host=shard2.db port=5432 dbname=app
shard3 = host=shard3.db port=5432 dbname=app
```

## Risks and Tradeoffs

### Known Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **PostgreSQL `pg_backend_pid()` returns pooled PID** | Medium | Advisory locks by PID break | Use `mode = proxy` for sessions that rely on `pg_backend_pid()` for locking |
| **MySQL temp tables lost on pool return** | Medium | Application sees "table doesn't exist" | Use session pinning or `mode = proxy` for sessions with cross-transaction temp tables |
| **COPY FROM STDIN interrupted by rebalancing** | Low | COPY fails mid-stream | Use `mode = proxy` for bulk-load sessions; rebalancing only migrates IDLE sessions |
| **Commit-in-doubt window** | Very Low | Duplicate commit if client retries without XID probe | Enable `transaction_tracking = on` for full CID recovery; without it, the risk is the same as any TCP proxy |
| **Prepared statement replay overhead** | Low | Extra round-trip on first use of a PS on a new backend | `virtualize` mode adds at most one Parse replay per backend assignment; negligible for OLTP |
| **kTLS not available on older kernels** | Medium | Falls back to userspace TLS silently | Check `SHOW STATS` for `ktls_fallback_total`; kTLS requires Linux 4.17+ and supported cipher |
| **io_uring not available** | Low | Falls back to epoll automatically | io_uring requires Linux 5.6+; production deployments should ensure kernel version |
| **SCRAM-SHA-256 overhead vs MD5** | Very Low | Slightly higher CPU per connection | SCRAM is required for security; overhead is sub-millisecond and happens once per session |
| **Large scatter queries** | Medium | Fan-out to all shards for non-keyed queries | Add a route rule or hook to force primary-only for cross-shard aggregation queries |
| **Cluster gossip split-brain** | Low | Two nodes believe they are the leader | KEEL cluster uses fail_threshold to avoid premature failover; no distributed consensus — external load balancer is recommended for strict HA |

### Tradeoffs vs Direct Connection

| Tradeoff | Detail |
|----------|--------|
| **Latency overhead** | Single-worker, single-backend: +0.1–0.5 ms per query (syscall + buffer copy). At high concurrency the pool's reduction in backend connection count more than compensates. |
| **Memory overhead** | ~400 B per pooled backend slot (lazy I/O buffers); each worker holds its own pool slice. |
| **SQL parser CPU** | SQL analysis runs on every query in SMART/FULL mode. Use `mode = pool` or `mode = proxy` to disable for maximum throughput. |
| **Feature combinations** | Not all features compose freely. The 12×12 invariant model enforces safe combinations at startup; incompatible combinations are rejected with a clear error. |

### Corner Cases

- **`SET LOCAL` inside a transaction** — KEEL replays all session state atoms on backend assignment. `SET LOCAL` changes are transaction-scoped; if the connection is returned to the pool mid-transaction (due to error or abort), the SET LOCAL state is lost — identical to direct connection behaviour.
- **Extended query protocol + simple query mixed** — Using `tracking` PS mode handles mixed `PREPARE ... AS` (simple) + `Execute` (extended). Using `virtualize` (default) handles extended-only. Mixing both protocols with ORM libraries that use each for different query types requires `tracking` mode.
- **Long-running queries during drain** — Sessions executing a query when SIGTERM arrives will complete (up to `shutdown_timeout_ms`). CID sessions (commit-in-doubt) are never force-closed during drain.
- **Backend certificate rotation** — SIGHUP triggers atomic `SSL_CTX` swap for frontend certs. Backend certs require pool connection recycling; new connections pick up the new cert automatically as old ones are closed by idle timeout.
- **MySQL GTID vs LSN** — `SET keel.read_after_lsn` accepts both PostgreSQL LSN format and MySQL GTID strings; the proxy stores them opaquely in the session atom and compares them as strings when selecting replicas.

## Compatibility Matrix

The table below shows the database versions, kernel versions, and Linux
distributions that are actively tested in CI or have been validated manually.
Untested combinations may work but are not supported.

### Database Backends

| Database | Version | Status | Notes |
|----------|---------|--------|-------|
| PostgreSQL | 14 | ✅ Tested in CI | Full wire protocol v3; all auth methods |
| PostgreSQL | 15 | ✅ Tested in CI | — |
| PostgreSQL | 16 | ✅ Tested in CI (primary) | Service container in `integration-db` CI job |
| PostgreSQL | 17 | ✅ Tested in CI | — |
| MySQL | 8.0 | ✅ Tested in CI | `caching_sha2_password`, `mysql_native_password` |
| MySQL | 8.4 | ✅ Tested in CI (primary) | Service container in `integration-db` CI job |
| MySQL | 9.x | ✅ Validated | Group Replication, InnoDB Cluster |
| MariaDB | 10.11 | ✅ Validated | Galera cluster topology |
| MariaDB | 11.x | ✅ Tested in CI | Galera cluster; `integration-db` CI service |
| Percona XtraDB Cluster | 8.0 | ✅ Validated | PXC wsrep protocol |

### Linux Kernel Requirements

| Kernel Version | I/O Backend | Notes |
|----------------|-------------|-------|
| ≥ 5.6 | **io_uring** (default) | `io_uring_setup` syscall available; recommended for production |
| ≥ 5.19 | io_uring buf-rings | `KEEL_USE_IOURING=ON` + `use_buf_rings=1`; lowers per-I/O overhead further |
| 5.4 – 5.5 | **epoll** (automatic fallback) | `io_uring` not available; KEEL falls back silently |
| < 5.4 | ❌ Not supported | Missing `epoll_pwait2` and other required interfaces |
| Any (unprivileged) | **epoll** (automatic fallback) | Docker/rootless: `EPERM` on `io_uring_setup` → epoll fallback |

kTLS (kernel TLS offload) requires Linux 4.17+ and a supported cipher suite.
KEEL logs a warning and falls back to userspace TLS silently when kTLS is unavailable.

### Operating Systems

| OS | Arch | Status |
|----|------|--------|
| Ubuntu 22.04 LTS | x86_64, arm64 | ✅ CI + Docker release image |
| Ubuntu 24.04 LTS | x86_64, arm64 | ✅ CI (primary) + Docker release image |
| Debian 12 (Bookworm) | x86_64 | ✅ Validated |
| RHEL / Rocky Linux 9 | x86_64 | ✅ RPM package tested |
| Fedora 40 | x86_64 | ✅ Validated |
| macOS 14+ | arm64 (Apple Silicon) | ✅ Builds and tests pass (epoll → kqueue) |

### Compiler Support

| Compiler | Version | Notes |
|----------|---------|-------|
| GCC | 13+ | Default in CI |
| Clang | 17+ | Required for fuzzing (`-DKEEL_ENABLE_FUZZ=ON`) and MSan |

---

## Building

### Prerequisites

- **CMake** 3.25+
- **C23 Compiler**: GCC 13+ or Clang 17+
- **OpenSSL** (required — used for SCRAM-SHA-256 and TLS/mTLS)
- **liburing** (recommended on Linux — enables io_uring reactor)

#### Ubuntu/Debian

```bash
sudo apt-get install cmake gcc-13 libssl-dev liburing-dev
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

### Build Commands

```bash
# Clone the repository
git clone https://github.com/virtlabs-io/dbcp-keel.git
cd dbcp-keel

# Create build directory
mkdir build && cd build

# Configure (Release build)
cmake -DCMAKE_BUILD_TYPE=Release ..

# Build (with optional ccache for faster incremental rebuilds)
cmake --build . -j$(nproc)

# Run tests (parallel)
ctest --output-on-failure -j$(nproc)
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `KEEL_USE_IOURING` | `ON` (Linux) | Enable io_uring reactor backend |
| `KEEL_USE_EPOLL` | `ON` (Linux) | Enable epoll reactor (fallback) |
| `KEEL_USE_KQUEUE` | `ON` (macOS) | Enable kqueue reactor |
| `KEEL_ENABLE_TESTS` | `ON` | Build test suite |
| `KEEL_ENABLE_BENCHMARKS` | `OFF` | Build benchmark binaries (`-DKEEL_ENABLE_BENCHMARKS=ON`) |
| `KEEL_ENABLE_HARDENING` | `ON` | Stack protection, RELRO, PIE, FORTIFY |
| `KEEL_ENABLE_ASAN` | `OFF` | AddressSanitizer |
| `KEEL_ENABLE_TSAN` | `OFF` | ThreadSanitizer |
| `KEEL_ENABLE_UBSAN` | `OFF` | UndefinedBehaviorSanitizer |
| `KEEL_ENABLE_MSAN` | `OFF` | MemorySanitizer |
| `KEEL_ENABLE_COVERAGE` | `OFF` | Code coverage |
| `KEEL_ENABLE_LUA` | `ON` | Lua hook support (Lua 5.4 / LuaJIT) |
| `KEEL_ENABLE_PYTHON` | `ON` | Python hook support (CPython 3.x) |

### Debug Build with Sanitizers

```bash
cmake -DCMAKE_BUILD_TYPE=Debug \
      -DKEEL_ENABLE_ASAN=ON \
      -DKEEL_ENABLE_UBSAN=ON ..
make -j$(nproc)
```

## Configuration

KEEL uses INI-based configuration with worker group sections. See [`etc/keel-pg.ini`](etc/keel-pg.ini) (PostgreSQL) and [`etc/keel-my.ini`](etc/keel-my.ini) (MySQL) for fully commented references.

### Minimal Configuration

```ini
[keel]
log_level = info   # trace, debug, info, warn, error, fatal

[worker_group.myapp]
name = myapp
protocol = postgresql
bind_addr = 0.0.0.0
bind_port = 7432
num_workers = 4
min_pool_size = 10
max_pool_size = 50

# Server authentication (KEEL → backends)
server_user = postgres
server_password = postgres

# Health checking
probe = query
probe_interval = 5s

[worker_group.myapp.servers]

primary  = host=127.0.0.1 port=5432 dbname=mydb role=RW weight=100
replica1 = host=127.0.0.1 port=5433 dbname=mydb role=RO weight=100
replica2 = host=127.0.0.1 port=5434 dbname=mydb role=RO weight=100
```

### Worker Group Options

| Option | Default | Description |
|--------|---------|-------------|
| `bind_addr` | `0.0.0.0` | Listen address |
| `bind_port` | `6432` | Listen port |
| `mode` | `pool` | Runtime tier: `proxy`, `pool`, `smart`, `full` |
| `num_workers` | `0` (auto) | Worker threads (0 = one per CPU core) |
| `min_pool_size` | `10` | Minimum backend connections per worker |
| `max_pool_size` | `50` | Maximum backend connections per worker |
| `max_conns_per_worker` | `0` | Per-worker session cap (0 = governed by max_pool_size) |
| `idle_timeout_ms` | `300000` | Server-side idle connection timeout (ms) |
| `connect_timeout_ms` | `5000` | Backend connect timeout (ms) |
| `pool_max_waiting` | `100` | Max clients queued waiting for a free backend |
| `listen_backlog` | `128` | TCP listen backlog |
| `rebalance` | `true` | Automatic load rebalancing across workers |
| `rebalance_interval_ms` | `5000` | Rebalance check interval (ms) |
| `rebalance_threshold_pct` | `125` | Imbalance trigger threshold (125 = 1.25× average) |
| `rebalance_max_per_tick` | `4` | Max session migrations per interval |
| `fast_network_path` | `on` | Zero-copy splice bypass for DataRow frames (MSG_PEEK + splice) |
| `result_cache` | `off` | Query result caching (future — disables splice bypass when on) |
| `use_buf_rings` | `0` | io_uring buffer rings (Linux 5.19+) |
| `prepared_statement` | `virtualize` | PS pooling strategy: virtualize, pinning, tracking, anonymous |
| `transaction_tracking` | `off` | XID probe + read-after-write consistency tokens |
| `experimental_features` | `off` | Required to enable experimental feature keys |
| `scatter_merge` | `off` | Enable scatter-merge routing features (experimental) |
| `wal_lsn_capture` | `off` | Enable WAL LSN capture (experimental) |
| `gtid_capture` | `off` | Enable GTID capture (experimental) |

### TLS Configuration (Frontend + Backend)

```ini
[worker_group.myapp]
# Frontend TLS (client -> KEEL)
tls_mode = require                 # disable | prefer | require
tls_cert_file = /keel/etc/certs/frontend/server.crt
tls_key_file = /keel/etc/certs/frontend/server.key
tls_verify_peer = optional         # no | optional | require
tls_ca_file = /keel/etc/certs/ca/ca.crt
ktls_enabled = auto                # off | on | auto

# Cipher policy (optional)
tls_ciphers = ECDHE-RSA-AES256-GCM-SHA384         # TLS 1.2 cipher list
tls_ciphersuites = TLS_AES_256_GCM_SHA384          # TLS 1.3 ciphersuites
tls_min_version = 1.3                               # Minimum TLS version

# Backend TLS (KEEL -> database)
backend_tls = require              # disable | prefer | require
backend_verify_peer = yes          # yes | no
backend_ca_file = /keel/etc/certs/ca/ca.crt
backend_cert_file = /keel/etc/certs/client/client.crt
backend_key_file = /keel/etc/certs/client/client.key
backend_tls_ciphers = ECDHE-RSA-AES256-GCM-SHA384
backend_tls_ciphersuites = TLS_AES_256_GCM_SHA384
backend_tls_min_version = 1.3
```

Sample local certificates are available under `etc/certs/`.

### Server Definition

Servers are defined in `[worker_group.<name>.servers]`:

```ini
[worker_group.myapp.servers]
# Format: name = host=... port=... dbname=... role=... weight=... [user=... password=...]
primary  = host=db1.local port=5432 dbname=app role=RW  weight=100
replica1 = host=db2.local port=5432 dbname=app role=RO  weight=100
replica2 = host=db3.local port=5432 dbname=app role=RO  weight=50
```

Per-server credentials override the group-level `server_user` / `server_password`.

### Probe Types

| Probe | Format | Health Check | Role Detection |
|-------|--------|--------------|----------------|
| `postgres` | `probe = postgres` | `SELECT 1` | `pg_is_in_recovery()` |
| `patroni` | `probe = patroni:8008` | `GET /primary` | REST API role field |
| `mysql` | `probe = mysql` | `SELECT 1` | `@@read_only` |
| `tcp` | `probe = tcp` | Connect/disconnect | None |
| `exec` | `probe = exec:/path/script` | Exit code | Exit code |

### Password Sources

```ini
server_password = secret                     # Plain text
```

### User Authentication File (userlist.txt)

```
"username" "password"
"admin" "supersecret"
"readonly" "readpass"
```

### Security Hardening

Keel can drop privileges and apply a seccomp system-call filter after startup:

```ini
[security]
privilege_drop        = 1        # drop root after bind / RLIMIT setup
run_user              = keel     # setuid target
run_group             = keel     # setgid target
require_privilege_drop = 1       # abort if drop fails
seccomp               = baseline # off | baseline | strict
require_seccomp       = 0        # abort if seccomp fails
no_new_privs          = 1        # PR_SET_NO_NEW_PRIVS
```

See [`etc/keel.ini.example`](etc/keel.ini.example) for the full `[security]` section reference.

## Running
### Start the Proxy

```bash
# PostgreSQL
./build/src/main/keel -c etc/keel-pg.ini

# MySQL
./build/src/main/keel -c etc/keel-my.ini
```

### Command Line Options

```
Usage: keel [OPTIONS]

Options:
  -c, --config <file>    Configuration file path
  -d, --daemon           Run as daemon
  -v, --verbose          Increase verbosity
  -h, --help             Show help message
  --version              Show version
```

### Connect via psql

```bash
# Connect through the proxy (port 7432 in the example config)
PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d mydb

# The proxy will:
# 1. Authenticate the client (SCRAM-SHA-256)
# 2. Assign a session on the accepting worker
# 3. Parse each query through the SQL parser
# 4. Route reads to replicas, writes to primary
# 5. Borrow a backend connection from the pool (or queue & async-refill)
# 6. Return the connection to the pool after transaction completes
```

### Connect via mysql

```bash
# Connect through the proxy (port 7306 in the example config)
mysql -h 127.0.0.1 -P 7306 -u keel -pkeel mydb

# The proxy will:
# 1. Send MySQL greeting with server capabilities
# 2. Authenticate the client (caching_sha2_password)
# 3. Parse COM_QUERY messages through the SQL parser
# 4. Route reads to replicas, writes to primary
# 5. Borrow a backend connection from the pool
# 6. Return the connection to the pool after OK/EOF
```

## Testing

### Unit & Integration Tests (116 tests)

```bash
cd build
ctest --output-on-failure
```

| Test | Description |
|------|-------------|
| `test_mem` | Memory allocators (arena, slab, pool) |
| `test_auth` | Authentication (SCRAM-SHA-256, MD5) |
| `test_parser` | SQL parser & lexer |
| `test_router` | Query routing & load balancing |
| `test_router_plugin` | Router plugin system |
| `test_sql` | SQL analysis & query classification |
| `test_config` | INI configuration parser |
| `test_config_reload` | Config reload infrastructure: INI diff, parameter classification, weight re-parse, probe timing (151 assertions) |
| `test_log` | Logging subsystem |
| `test_query_log` | Per-query logging |
| `test_util` | Utility functions |
| `test_string_view` | String view operations |
| `test_ringbuf` | Ring buffer |
| `test_xxhash` | Hash functions |
| `test_residual` | Residual data handling |
| `test_session_engine` | Session engine lifecycle |
| `test_pool_correctness` | Pool borrow/return correctness |
| `test_plugin_contract` | Plugin contract verification |
| `test_pg_protocol_flow` | PostgreSQL protocol flow |
| `test_mysql_protocol_flow` | MySQL protocol flow |
| `test_hooks` | Hook system (register, fire, abort, priority, stats) |
| `test_failover` | Failover & probe system |
| `test_migration` | Worker connection migration (lifecycle, SCM_RIGHTS FD transfer, eligibility) |
| `test_tls_ktls` | TLS handshake/data path and kTLS activation/fallback accounting |
| `test_tls_security` | TLS cipher policy, cert reload, downgrade protection, mTLS peer info, version enforcement, engine drain API |
| `test_drain_shutdown` | Graceful drain/shutdown lifecycle, state machine, force-close, CID protection |
| `test_pool_audit` | Pool allocator metadata integrity, canary validation |
| `test_fd_tracking` | File descriptor tracking, leak detection |
| `test_state_machine` | Session state machine transitions, invariant enforcement |
| `test_dirty_connection` | Dirty connection cleanup (DISCARD ALL), non-blocking drain |
| `test_concurrency_stress` | Thundering herd, TIME_WAIT, RLIMIT_MEMLOCK, multi-worker stress |
| `test_fuzz_harness` | Protocol fuzz harness: malformed packets, random payloads |
| `test_shadow_diff_harness` | Shadow diff testing against direct PostgreSQL |
| `test_runtime_security_harness` | Seccomp BPF, privilege drop, syscall filtering |
| `test_runtime_security_strict_harness` | Strict seccomp (minimal syscall set) |
| `test_ps_pool_matrix` | Combinatorial: PS modes × pool states × concurrency (729 assertions) |
| `test_tls_splice_matrix` | Combinatorial: TLS modes × splice paths × kTLS states |
| `test_session_hooks_matrix` | Combinatorial: session states × hook points × abort/continue |
| `test_crash_recovery_matrix` | Combinatorial: crash points × recovery modes × txn states |
| `test_dual_protocol_matrix` | Combinatorial: PG × MySQL protocol interactions |
| `test_invariant_model` | Formal invariant model: 12×12 matrix, violation classes, pool invariants |
| `test_runtime_mode` | Runtime mode tiers: tier parsing, gate macros, PROXY overrides (78 assertions) |
| `test_state_context` | Session-context preservation: SET persistence, diff gen, sync SQL, round-trip (55 assertions) |
| `test_stats_alignment` | Stats structure alignment and atomic counter layout |
| `test_state_contracts` | Contract-driven state model: derived enums, transition matrices, invariant checks (218 assertions) |
| `test_sm_sequence_walk` | State machine exhaustive walk: DFS of all transition matrices, illegal edge rejection (289 assertions) |
| `test_sm_fuzz` | State machine fuzz harness: AFL++/libfuzzer compatible, random byte-pair commands (2304+ inputs) |
| `test_sm_stress` | State machine concurrent stress: 64-thread independent lifecycles, journal ring-buffer stress |
| `test_graceful_restart` | Graceful restart lifecycle: in-flight connection handling, worker drain sequencing, SIGHUP vs SIGTERM paths |
| `test_tls_auto` | TLS auto-negotiation: SSLRequest detection, auto-upgrade from plaintext, mixed TLS/plaintext listener |
| `test_cluster` | Cluster management: multi-backend topology updates, primary/replica role transitions, Patroni REST health |
| `test_cancel_forwarding` | PostgreSQL cancel-request forwarding: BackendKeyData extraction, CancelRequest proxy, unknown-PID safety |
| `test_trace` | Tracing subsystem: span lifecycle, attribute tagging, sampling, flush/export, zero-overhead disabled-trace fast path |
| `test_admin_auth` | Admin authentication: password-protected listener, credential validation, session isolation |
| `test_histogram` | Histogram statistics: bucket boundaries, P50/P95/P99, worker merge, Prometheus text format |
| `test_otlp_encoding` | OTLP encoding: protobuf metric/span serialization, attribute value types, batch limits |
| `test_ndjson_log` | NDJSON log format: structured field emission, level filtering, escaping, log-plugin round-trip |
| `test_ssv_core` | SSV core: session-state atom lifecycle, hash-bucket pool index, OPAQUE/CONFIG domain split, WAL LSN, replay ordering |
| `test_ssv_atom` | SSV atom layer: atom create/update/compare, domain classification, diff/merge, serialization |
| `test_alloc_inject` | OOM injection: `keel_mem_set_fail_countdown()` systematically faults router, connpool, registry, session, config subsystems; staircase scan; fail-then-recover state check (15 assertions) |
| `test_connpool_exhaust` | Pool exhaustion: full-pool timeout returns `KEEL_ERR_POOL_TIMEOUT`, stats increment, release unblocks waiter, pool consistent after timeout, 16-thread timeout storm (36 assertions) |
| `test_route_cache_stress` | Route cache adversarial stress: collision chains, LRU eviction, adversarial single-bucket workload, 4-thread × 50K concurrent reads, counter invariant (21 assertions) |
| `test_proto_split` | Split-at-every-byte protocol: `frame_len()` semantics for all PG (startup/Q/P/B/E/H/S/X/Z/R/S/C) and MySQL (COM_QUERY/HandshakeResponse41) messages (26 assertions) |
| `test_ps_failover_tls` | PS replay × failover × GUC hash: TRACKING PREPARE populates replay buf, stmt_count, name presence, hash stability, roundtrip into fresh ctx, GUC hash change/restore, empty replay for PINNING/ANONYMOUS/OFF (56 assertions) |
| `test_listen_notify` | NOTIFY/LISTEN proxying: pin/unpin lifecycle, UNLISTEN *, NOTIFY passthrough, case-insensitivity, non-LISTEN query forwarding (20 assertions) |
| `test_query_rules` | Declarative query rules: creation, matching, INI load, priority, action semantics, edge cases (74 assertions) |
| `test_osc_proxying` | Online schema change: gh-ost DML detection, pt-osc detection, non-shadow pass-through, pin/unpin lifecycle, case-insensitivity (29 assertions) |
| `test_ryw_propagation` | Cross-service read-your-writes: SET parsing, SHOW, notify vtable, wire format, edge cases (64 assertions) |
| `test_web_ui` | Web management UI: HTML structure, JSON API format, route matching, security (no external scripts/eval), SPA behaviour (91 assertions) |
| `test_audit_log` | Audit logging: event filtering, NDJSON/text format, auth/admin/query events, log rotation hook (81 assertions) |
| `test_throttle` | Query throttling: token-bucket rule matching, sustained rate, burst, user/db scoping, INI load (34 assertions) |
| `test_cloud_auth` | Cloud-native auth: AWS SigV4 token, GCP JWT, Azure IMDS; token caching; expiry rotation |
| `test_enterprise_auth` | Enterprise auth: PAM, LDAP bind+search, cert identity CN extraction, auth_query execution |
| `test_sharding` | Horizontal sharding: shard-key extraction, modulo/hash/range dispatch, scatter plan, admin virtual table, hot-reload, Prometheus metrics, multi-shard tx coordinator (344 assertions) |

### Hard Guarantee Gate

```bash
# Build + full ctest + explicit TLS/kTLS regression
bash ./tests/hardening/run_ci_gate.sh

# Strict mode: fail if kTLS cannot be activated in this environment
KEEL_REQUIRE_KTLS=1 bash ./tests/hardening/run_ci_gate.sh
```

### Master Test Coordinator

All test suites (unit, e2e, integration, hardening, chaos, fuzz) can be run
through the unified coordinator:

```bash
# Run all suites:
python3 tests/run_tests.py

# Run specific suites:
python3 tests/run_tests.py --suite unit --suite e2e

# Unit tests only, against a specific build:
python3 tests/run_tests.py --suite unit --build-dir build

# List available suites:
python3 tests/run_tests.py --list
```

### End-to-End Tests

```bash
# Full Python E2E suite (Docker Compose, all features, HTML+JSON report):
cd tests/e2e && ./run_e2e.sh
./run_e2e.sh --no-chaos --no-stress     # fast smoke
./run_e2e.sh --only metrics             # single category

# PostgreSQL E2E with Docker (primary + replicas + pgbench)
tests/integration/test-pg-e2e-full.sh

# PostgreSQL Patroni cluster
tests/integration/test-pg-patroni.sh

# PostgreSQL streaming replication
tests/integration/test-pg-streaming.sh

# MySQL async replication (MySQL 9)
tests/integration/test-mysql-replication.sh

# MySQL Group Replication (MySQL 9)
tests/integration/test-mysql-group.sh

# Percona XtraDB Cluster 8.4
tests/integration/test-mysql-pxc.sh

# MariaDB Galera Cluster
tests/integration/test-mysql-mariadb.sh
```

### Chaos Tests

The `tests/chaos/` directory contains 12 fault-injection scenarios run against a
live KEEL + PostgreSQL stack. Each scenario injects a specific class of failure
and verifies that KEEL recovers correctly.

| Scenario | Injects |
|----------|---------|
| `commit-in-doubt.sh` | Backend killed after COMMIT sent, before ReadyForQuery |
| `flip-primary.sh` | Primary role transfer mid-workload |
| `kill-backend-mid-query.sh` | Backend process killed during active query |
| `partition-replica.sh` | Network partition isolating replica |
| `primary-dies-during-txn.sh` | Primary dies with open transaction |
| `primary-dies-idle.sh` | Primary dies with idle connections in pool |
| `replica-lag-threshold.sh` | Replica lag exceeds read-after-write threshold |
| `role-flapping.sh` | Rapid primary/replica role changes |
| `scatter-backend-mid-scatter.sh` | Shard backend dies during scatter fan-out |
| `scatter-network-partition.sh` | Network partition during scatter-merge |
| `sigkill-during-drain.sh` | SIGKILL sent during graceful drain |
| `timeline-invalidation.sh` | PostgreSQL timeline switch (promotion) |

```bash
# Requires: docker compose -f docker/compose/pg-chaos.yml up -d --wait
tests/chaos/run-chaos.sh                       # all scenarios
tests/chaos/run-chaos.sh kill-backend          # single scenario
```

The chaos suite is also exercised weekly via `.github/workflows/chaos.yml`.

### Sanitizer Builds

```bash
# AddressSanitizer
cmake -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_ASAN=ON ..
make -j$(nproc) && ctest --output-on-failure

# ThreadSanitizer
cmake -DCMAKE_BUILD_TYPE=Debug -DKEEL_ENABLE_TSAN=ON ..
make -j$(nproc) && ctest --output-on-failure
```

## Query Routing Examples

### PostgreSQL

```sql
-- Routed to RO replica (read-only)
SELECT * FROM users WHERE id = 1;
SELECT COUNT(*) FROM orders;

-- Routed to RW node (writes)
INSERT INTO users (name) VALUES ('Alice');
UPDATE orders SET status = 'shipped' WHERE id = 100;
DELETE FROM sessions WHERE expired < NOW();

-- Routed to RW node (DDL)
CREATE TABLE new_table (id INT);
ALTER TABLE users ADD COLUMN email TEXT;

-- Routed to RW node (locking read)
SELECT * FROM users WHERE id = 1 FOR UPDATE;

-- Transaction pinning (stays on same backend)
BEGIN;
INSERT INTO orders (item) VALUES ('widget');
SELECT * FROM orders WHERE id = currval('orders_id_seq');
COMMIT;
-- Connection returned to pool here
```

### MySQL

```sql
-- Routed to RO replica (read-only)
SELECT * FROM users WHERE id = 1;
SHOW TABLES;

-- Routed to RW node (writes)
INSERT INTO users (name) VALUES ('Alice');
UPDATE orders SET status = 'shipped' WHERE id = 100;

-- Routed to PRIMARY (locking read)
SELECT * FROM users WHERE id = 1 FOR UPDATE;

-- Transaction pinning
START TRANSACTION;
INSERT INTO orders (item) VALUES ('widget');
SELECT LAST_INSERT_ID();
COMMIT;
-- Connection returned to pool here
```

## Performance

### Recommended Kernel Tuning

For high-concurrency reconnecting workloads (`-C` flag), tune these sysctls:

```bash
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_fin_timeout=5
sudo sysctl -w net.ipv4.tcp_max_tw_buckets=10000
```

## Roadmap

KEEL has a broad implemented surface, but not every feature has the same
production maturity. The list below is a maturity snapshot; see
[PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md) for the source of truth
and [ROADMAP.md](docs/ROADMAP.md) for longer-term planning.

### Maturity Snapshot

- [x] io_uring share-nothing reactor with linked SQEs, registered FDs, zero-poll hot path
- [x] Dual-protocol support (PostgreSQL v3 + MySQL client/server)
- [x] Transaction pooling with zero-copy splice and MSG_PEEK DataRow bypass
- [x] Full SQL lexer/parser with automatic read/write splitting
- [~] Horizontal sharding: shard-key extraction, modulo/hash/range strategies, scatter aggregation, multi-shard tx coordinator, admin virtual tables, hot-reload, Prometheus metrics are implemented but remain experimental for production rollouts
- [x] Session-context preservation (SET params, search_path across backend reassignment via SSV)
- [x] Prepared statement pooling (4 strategies: virtualize, pinning, tracking, anonymous)
- [x] XID probe + commit-in-doubt recovery, with sticky-primary read-after-write safety
- [x] Cross-service token parsing via `SET keel.read_after_lsn` / `SHOW keel.write_lsn`; reactor-owned replica catch-up probes remain experimental
- [x] TLS + mTLS + kTLS with cipher enforcement, cert hot-reload, downgrade protection, built-in cert inspection
- [x] Hook/trigger system (Lua 5.4, Python 3.x, native .so plugins at 4 pipeline stages)
- [x] Admin console (21+ commands, virtual tables, JSON output, K8s health endpoints, keel-cli)
- [x] Prometheus metrics, Grafana dashboard, latency histograms, web management UI
- [x] Distributed tracing: W3C traceparent injection, OTLP/HTTP export, per-query spans
- [x] Audit logging: structured NDJSON/text security audit log with event filtering
- [x] Query throttling: per-rule token-bucket rate limiting via `[throttle.N]` INI
- [x] Live SIGHUP reload (pool sizes, timeouts, weights, probes, TLS certs, log level, shard rules, query rules)
- [x] Seccomp BPF system-call filter, privilege drop, binary hardening (PIE, Full RELRO, NX)
- [~] Multi-proxy HA cluster: heartbeat, config gossip, peer discovery, wire-protocol compression (zlib/zstd) is implemented but still under production hardening
- [x] Cloud-native auth: AWS SigV4, GCP OAuth2, Azure IMDS with token caching
- [x] Enterprise auth: PAM, LDAP, mTLS certificate identity, auth query
- [x] Connection lifecycle management: max age, idle eviction, per-user quotas, pool prefill
- [x] NOTIFY/LISTEN transparent proxying through transaction-mode pool
- [x] Declarative query rules: INI-driven routing, rewriting, and blocking without hook code
- [x] Online Schema Change proxying: gh-ost and pt-osc transparent primary affinity
- [x] Kubernetes native: Helm chart, CRD operator (`KeelPool`), sidecar mode
- [x] Docker official images: multi-arch, `KEEL_*` env var config, production Compose templates
- [x] 116 unit/integration/combinatorial/fuzz tests + 7 Docker Compose E2E stacks
- [x] Formal 12×12 invariant model + 9-domain state machine with exhaustive verification

### Remaining Planned Work (P3)

| Feature | Description |
|---------|-------------|
| Database aliases | `[database_aliases]` mapping for zero-downtime DB migrations |
| Windows native packages | After kqueue macOS path is complete |

See [ROADMAP.md](docs/ROADMAP.md) for full details, competitive advantages, and implementation notes.

## Documentation

Detailed architecture documentation is available in the [`docs/`](docs/) directory:

| Document | Description |
|----------|-------------|
| [PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md) | Feature maturity levels, required failure-mode matrix, failover semantics, and operator inspection contract |
| [STARTUP.md](docs/STARTUP.md) | Startup flow, configuration parsing, memory architecture, worker initialization |
| [CONNECTION_FLOW.md](docs/CONNECTION_FLOW.md) | Connection lifecycle, accept loop, authentication, backend pool architecture |
| [QUERY_FLOW.md](docs/QUERY_FLOW.md) | Query execution flow, SQL analysis, routing, forwarding, result handling |
| [MULTIPLEXING.md](docs/MULTIPLEXING.md) | Worker architecture deep-dive, SO_REUSEPORT, reactor model, connection migration, automatic rebalancing |
| [PREPARED_STATEMENTS.md](docs/PREPARED_STATEMENTS.md) | Prepared statement pooling strategies (virtualize/pinning/tracking/anonymous), config guide, real-world examples |
| [SSV_POSTGRES_IMPLEMENTATION.md](docs/SSV_POSTGRES_IMPLEMENTATION.md) | Implementation record for PostgreSQL prepared-statement semantic virtualization: architecture, scope boundaries, tradeoffs, diagrams, code excerpts, and validation |
| [TRANSACTION_TRACKING.md](docs/TRANSACTION_TRACKING.md) | XID probe, commit-in-doubt recovery, WAL LSN consistency tokens |
| [SHARDING.md](docs/SHARDING.md) | Horizontal sharding architecture, shard rule configuration, API reference, hot-reload, Prometheus metrics |
| [DOCKER.md](docs/DOCKER.md) | Docker quick-start, multi-arch images, `KEEL_*` env vars, production Compose templates, GitHub Actions publish |
| [TESTING.md](docs/TESTING.md) | Testing guide, E2E setup, benchmark scripts |
| [ADMIN_SQL.md](docs/ADMIN_SQL.md) | Admin SQL interface: virtual tables, DML operations, 21 admin commands, Prometheus metrics, K8s health endpoints |
| [OPERATIONS.md](docs/OPERATIONS.md) | Operations guide: SIGHUP reload, graceful drain, signal reference, upgrade procedures |
| [ROADMAP.md](docs/ROADMAP.md) | Project roadmap: completed features, planned work (P1-P3), and competitive advantages |
| [HOOKS.md](docs/HOOKS.md) | Hook/trigger system — Lua, Python, and native plugin extensibility |
| [SESSION_CONTEXT.md](docs/SESSION_CONTEXT.md) | Session-context preservation: compatibility matrix, architecture, 5-tier pool borrow, diff algorithm, worked examples, competitor comparison |
| [RUNTIME_MODES.md](docs/RUNTIME_MODES.md) | Runtime mode tiers (PROXY/POOL/SMART/FULL): feature matrix, configuration, performance impact, decision guide |
| [STATE_MODEL.md](docs/STATE_MODEL.md) | Formal 9-domain session state machine: transition matrices, contracts, predicates, event journal |
| [CLUSTER_WIRE_COMPRESSION.md](docs/CLUSTER_WIRE_COMPRESSION.md) | Wire-protocol compression (zlib/zstd) for WAN/cross-region cluster gossip: protocol details, codec guide, INI config, Docker setup |

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

- ✅ You can use, modify, and distribute this software
- ✅ You can use it for commercial purposes
- ⚠️ Modified/distributed versions must share source code
- ⚠️ Network use (SaaS) requires source code disclosure
- ⚠️ Derivative works must use the same license

See [LICENSE](LICENSE) for the full license text.

## Acknowledgments

- [PostgreSQL](https://www.postgresql.org/) — The world's most advanced open source database
- [MySQL](https://www.mysql.com/) — The world's most popular open source database
- [io_uring](https://kernel.dk/io_uring.pdf) — Linux's high-performance async I/O interface
- [liburing](https://github.com/axboe/liburing) — io_uring userspace library
- [OpenSSL](https://www.openssl.org/) — Cryptographic library for SCRAM/SHA authentication

---

**KEEL** — Fast, intelligent database connection pooling for PostgreSQL and MySQL.
