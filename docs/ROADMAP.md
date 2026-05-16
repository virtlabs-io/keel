# KEEL Roadmap

> Last updated: 2026-05-05
> Branch: `main`

This document tracks what KEEL has delivered, what is in progress, and what is planned.
Items are organised by status and priority.

---

## Completed

Everything listed below is implemented, tested, and available in the current build.

### Core Engine
- **io_uring Share-Nothing Reactor** — per-worker io_uring rings with linked SQEs, registered FDs, batch sends, submit+wait merging, and zero-poll hot path
- **Dual-Protocol Support** — PostgreSQL v3 wire protocol and MySQL client/server protocol from a single binary (MySQL 9, Percona XtraDB Cluster, MariaDB Galera, Group Replication); MySQL protocol at full feature parity with PostgreSQL: 64-slot PS session map with FNV-1a session hash, `get_stmt_replay` for backend re-preparation, SESSION_TRACK_SCHEMA/GTIDS capture, cross-service RYW via `SET`/`SELECT @keel_write_gtid`, XA distributed-transaction pinning (BEGINS_TX / ENDS_TX / HARD_PIN), CALL/DO classification (340 protocol assertions)
- **Transaction Pooling** — backend connections shared across clients between transactions with non-blocking DISCARD ALL cleanup
- **Connection Multiplexing** — multi-threaded worker groups with SO_REUSEPORT kernel-level distribution
- **Connection Migration** — idle sessions transferred between workers via Unix socketpair SCM_RIGHTS + SPSC ring buffer for runtime load rebalancing
- **Automatic Rebalancing** — timer-based pressure detection with configurable threshold-driven migration
- **Admission Control** — per-worker frontend/backend limits with bounded FIFO wait queue, pressure feedback, peak tracking, and queue timeout
- **Zero-Copy Splice** — Linux splice(2) for kernel-space client↔backend data forwarding
- **MSG_PEEK + Splice DataRow Bypass** — peeks at backend message headers and splices DataRow frames directly via kernel pipe without userspace copy
- **Async Pool Warmup** — pre-connects backend connections asynchronously during startup
- **Auto FD Limit** — raises RLIMIT_NOFILE to hard max at startup (up to 1M file descriptors)
- **Crash Signal Handlers** — catches SIGSEGV/SIGABRT/SIGBUS with async-signal-safe diagnostics
- **Modern C23** — written in C23 with strict compiler warnings (-Wall -Wextra -Werror)

### SQL & Routing
- **Full SQL Lexer & Parser** — tokenizer → recursive descent parser → query tree (AST)
- **Automatic Read/Write Splitting** — reads to replicas, writes to primary
- **Weighted Load Balancing** — configurable weights per backend server
- **Transaction Pinning** — server affinity maintained during open transactions
- **FOR UPDATE Detection** — locking reads routed to primary
- **Sticky-Primary Override** — after a write, reads temporarily pinned to primary (100 ms window)
- **CTE / Window / Set-Operation Support** — WITH, OVER, UNION, INTERSECT, EXCEPT, MERGE, LOCK, LISTEN, VACUUM
- **Route Cache** — per-worker 1024-entry XXHash64 L1 cache with 8-probe linear search and LRU eviction
- **Pluggable Router Plugins** — per-database routing policies via keel_router_plugin_ops_t
- **Horizontal Sharding** — transparent shard-key extraction from SQL AST (SELECT/INSERT/UPDATE/DELETE), deterministic shard mapping (int64 modulo, xxhash64 string hashing), `$N` bound-parameter resolution for prepared statements, unified `keel_shard_plan_t` routing plan API (SINGLE / SCATTER / UNSUPPORTED), in-router shard rule registry with case-insensitive CRUD, `keel_router_plan_sql()` auto-dispatch across all registered rules, scatter fan-out `keel_router_scatter_servers()` with per-shard read/write routing, session-state-aware primary forcing (in-transaction, temp tables), `keel_scatter_plan_t` with per-shard `keel_route_decision_t` and failure accounting, **rule persistence** via `[shard_rule.*]` INI sections + `keel_router_load_shard_rules_from_config()`, **combined dispatch** via `keel_router_dispatch_sql()` / `keel_router_dispatch_sql_timed()`, **scatter result aggregation** via `keel_route_agg_t` + `keel_route_merge_fn`, **per-shard routing counters**, **admin virtual table** (`SELECT * FROM shard_rules`, `SHOW SHARD RULES`, `EXPLAIN SHARD PLAN FOR '<sql>'`), **range-based shard map** (`KEEL_SHARD_STRATEGY_RANGE` with INT64 threshold table), **multi-shard transaction coordinator** (scatter write participation tracking + cross-tx validation returning `KEEL_ERR_SHARD_CROSS_TX`), **shard migration state** (dual-write + read-from-new via `KEEL_SHARD_STATE_MIGRATING`), **connection pool per shard** (`keel_connpool_t` with min/max, idle eviction, health-check), **proxy session** (`keel_client_session_t`), **config hot-reload** (`keel_config_reload_shard_rules()` wired to SIGHUP), **Prometheus metrics** (`keel_router_write_prometheus()` served on `/metrics`), **query timeout** (`keel_router_dispatch_sql_timed()` + `KEEL_ERR_QUERY_TIMEOUT`)
- **Scatter-Merge Aggregation Engine** — transparent cross-shard aggregation of `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT col)` with multi-phase merge pipeline (Phase D scalar agg, Phase E GROUP BY, Phase H HAVING, Phase C ORDER BY merge-sort, Phase L global LIMIT/OFFSET); `sc_strip_limit_offset()` prevents per-shard GROUP BY + LIMIT truncation; 2PC coordinator (`scatter_2pc.c`) with deterministic GIDs `keel_<session>_<seq>_s<shard>`, terminal-state guards (prevents commit-after-rollback / rollback-after-commit), `KEEL_2PC_MAX_PARTICIPANTS = 64`; per-shard direct TCP connections with `SO_RCVTIMEO = 30s`; `EXPLAIN SHARD PLAN FOR` extended to 7 columns (kind, shard_index, shard_count, agg_type, merge_strategy, has_order_by, has_limit); Prometheus histogram `keel_router_scatter_merge_duration_seconds` with 10 buckets (1ms–2.5s); pgbench benchmark scripts (`bench/run_scatter_pgbench.sh`, `bench/measure_scatter_conn_overhead.sh`); 106 tests (14 scatter merge e2e, 27 2PC matrix, 134-assertion SQL fuzz) — see [SCATTER_MERGE.md](SCATTER_MERGE.md)
- **Declarative Query Rules** — INI `[query_rule.*]` sections with POSIX ERE matching, route override (primary/replica/any), hard block (synthetic error), query rewrite with capture groups; evaluated after parse before routing; SIGHUP hot-reload; admin virtual table `SELECT * FROM query_rules`
- **Online Schema Change Proxying** — detects gh-ost (`_gho`, `_ghc`) and pt-online-schema-change (`_new`, `_old`) shadow-table DML patterns; pins session to primary for migration duration; `osc_active` state bit tracks lifetime; 29 unit tests
- **NOTIFY/LISTEN Transparent Proxying** — intercepts LISTEN/UNLISTEN/NOTIFY at protocol layer; LISTEN pins to a dedicated backend (session-mode semantics); UNLISTEN releases the pin; `UNLISTEN *` handled; subscribed sessions survive idle timeout and pool drain; 20 unit tests
- **Cross-Service Read-Your-Writes** — `SET keel.read_after_lsn = '<LSN>'` stores client-supplied LSN in session consistency atoms (no backend round-trip); `SHOW keel.write_lsn` returns most-recent committed write LSN; enables multi-service RYW propagation via SQL; 64 unit tests

### Session & State Management
- **Session-Context Preservation** — transparent SET parameter, search_path, and session variable continuity across backend reassignment via sorted K/V state profiles, XXHash64 fingerprinting, two-pointer merge diff, and 5-tier pool borrow
- **SSV (Semantic State Virtualization)** — atom-layer consistency model for prepared statement and GUC session state: hash-bucket pool index for O(1) backend matching, OPAQUE domain split, CONFIG domain atoms, WAL LSN integration, and semantic replay
- **Prepared Statement Pooling** — four strategies: virtualize (replay), pinning (session affinity), tracking (simple+extended), anonymous (JIT rewrite)
- **XID Probe + Commit-in-Doubt Recovery** — instruments COMMIT with txid_current() and re-queries txid_status() on a fresh connection if the backend dies before ReadyForQuery
- **Read-After-Write Consistency** — WAL LSN tokens ensure replicas have replayed the client's last write before serving reads
- **Runtime Mode Tiers** — four tiers (PROXY / POOL / SMART / FULL) that gate features behind single integer comparisons for zero-overhead when disabled
- **Cross-Feature Invariant Model** — 12×12 compatibility matrix with runtime checker and 20 violation classes
- **Formal State Machine Model** — 9-domain session state machine with transition matrices, contracts, predicates, and event journal

### Security & TLS
- **TLS + mTLS + kTLS** — frontend TLS termination, backend TLS, optional client-cert verification, kernel TLS acceleration, cipher suite enforcement, cert hot-reload via SIGHUP, and downgrade protection
- **Cipher Policy** — TLS 1.2 cipher list, TLS 1.3 ciphersuite list, and minimum version enforcement
- **Certificate Hot-Reload** — SIGHUP triggers atomic SSL_CTX swap without dropping connections
- **mTLS Session Enrichment** — peer cert subject/issuer stored in session metadata for hook inspection
- **Certificate Inspection** — `SHOW CERTIFICATES` lists frontend CA, server cert, and backend CA with subject, issuer, not-before/after, and SHA-256 fingerprint
- **Seccomp System-Call Filter** — baseline and strict BPF profiles with PR_SET_NO_NEW_PRIVS
- **Privilege Drop** — drop root after bind and RLIMIT setup, setuid/setgid to unprivileged user
- **Binary Hardening** — PIE, NX, stack canary (stack-protector-strong), Full RELRO enforced at build time
- **Audit Logging** — structured security audit log with event-type filtering (auth, admin, query, pool); NDJSON or text output; `[audit]` INI section; 81 unit tests
- **Ephemeral Test PKI** — `scripts/generate-test-certs.sh` generates a self-signed CA, server, backend, and client certs for dev/test

### Authentication
- **SCRAM-SHA-256** — full client + backend auth for PostgreSQL (OpenSSL)
- **MD5** — legacy PostgreSQL password auth
- **caching_sha2_password** — MySQL default auth plugin (OpenSSL)
- **mysql_native_password** — MySQL legacy auth
- **Trust / Password** — simple auth methods
- **User File Support** — external credential management (userlist.txt)
- **Cloud-Native Authentication** — AWS RDS IAM (SigV4 token generation, 14-min cache), GCP Cloud SQL IAM (service account JWT + metadata server + token file fallback), Azure AD/Entra (IMDS managed identity + env var + token file fallback)
- **Enterprise Authentication** — PAM (`pam_authenticate()` in separate thread to avoid blocking reactor), LDAP (`ldap_bind()` + search with session-level result caching), mTLS certificate identity (extract username from client cert CN/SAN, no password challenge), auth query (validate credentials via configurable SQL function on backend); pluggable `keel_auth_provider_ops_t` vtable; CMake auto-detects libldap/libpam

### Health & Failover
- **Pluggable Probe System** — postgres (SQL), patroni (HTTP REST), mysql, tcp, exec (custom script)
- **Automatic Role Detection** — pg_is_in_recovery() for PostgreSQL, REST API for Patroni
- **Patroni Cluster Discovery** — GET /cluster (all members) with /patroni fallback; pure-C HTTP/1.0 client
- **Failover Handling** — dead servers removed, role changes detected, routing adjusted
- **WAL Position Tracking** — replication lag monitoring via LSN

### Observability & Admin
- **Admin Console** — PostgreSQL wire protocol on dedicated TCP port; 21+ commands including SHOW, PAUSE, RESUME, DISABLE/ENABLE SERVER, ADD/REMOVE SERVER, KILL CLIENT, DRAIN, SET (13 runtime keys), RELOAD
- **SQL-Syntax Admin Query Language** — full SQL DML against virtual admin tables: SELECT from 14+ tables (pools, servers, clients, config, stats, databases, users, peers, cluster, cluster_stats, tracing, shard_rules, query_rules, certificates, etc.)
- **keel-cli** — standalone command-line binary for scripting (zero library dependencies)
- **JSON Output** — append FORMAT JSON to any admin command
- **Prometheus Metrics** — /metrics endpoint with pool, session, TLS, shard, query, throttle, and error counters
- **Latency Histograms** — P50/P95/P99 per-worker histograms merged to Prometheus histogram format
- **K8s Health Endpoints** — /healthz, /readyz, /livez on the Prometheus HTTP port
- **Grafana Dashboard** — pre-built JSON at etc/grafana/keel-dashboard.json
- **Per-Worker Statistics** — queries_total, queries_read, queries_write, bytes counters, migration stats
- **TLS/kTLS Metrics** — handshake, activation, fallback, failure, cert reload, and downgrade rejection counters
- **Pluggable Log Backend** — stdout, file, and syslog log plugins with hot-loaded configuration
- **Structured NDJSON Logging** — `log_format = json` emits newline-delimited JSON with trace correlation fields
- **Query Logging** — configurable per-query log with mode filter (all, read, write, none)
- **Distributed Tracing (OpenTelemetry)** — per-session W3C `traceparent` SQL comment injection, per-query spans with SQL digest and route decision, OTLP/HTTP JSON export, per-worker lock-free span ring buffer, configurable head-based sampling rate (ppm); 25 unit tests
- **Web Management UI** — self-contained dark-themed SPA at `GET /ui`; auto-refreshes every 5 s; engine state badge, metric cards, `.catch` reconnect handler; 91 unit tests
- **JSON Status API** — `GET /api/status.json` with engine state, session/pool/query/error counters; CORS-enabled
- **Query Throttling** — per-rule token-bucket rate limiting via `[throttle.N]` INI sections; user/db/pattern matching; configurable burst and sustained rate; 34 unit tests

### Extensibility
- **Hook/Trigger System** — 4 query pipeline extension points (after_query_read, after_query_parse, before_route, before_send)
- **Lua Hooks** — embedded Lua 5.4/LuaJIT scripting
- **Python Hooks** — embedded CPython 3.x scripting
- **Native Plugin Hooks** — .so/.dylib shared libraries loaded via dlopen()
- **Mutable Context** — hooks can inspect and modify session, query, routing, and flags in-place
- **Priority Chains** — hooks execute in priority order; mixing Lua + Python + native at the same point

### Memory Architecture
- **Arena Allocator** — fast bump allocation for request-scoped memory
- **Slab Allocator** — fixed-size object pools for sessions and connections
- **Pool Allocator** — O(1) free-list allocation for recv contexts (~400 B metadata) and pool waiters
- **Lazy I/O Buffers** — heap-backed buffers allocated on demand, not at pool creation (300× lower idle memory vs embedded 64 KB arrays)
- **Ring Buffer** — lock-free circular buffer for protocol data
- **NUMA-Aware Allocator** — node-local and interleaved allocation via mmap+mbind (no libnuma dependency)

### Operations & Deployment
- **Live Configuration Reload** — SIGHUP reloads pool sizes, timeouts, server weights, probe timing, rebalancing config, TLS certificates, log level, shard rules, query rules, throttle rules, audit config without restart
- **Graceful Drain/Shutdown** — lifecycle state machine (CREATED → ACTIVE → DRAINING → STOPPING → STOPPED), configurable drain timeout, CID-aware force-close
- **Connection Lifecycle Management** — `max_connection_age_ms` closes/replaces aged backend connections; per-user/per-database connection quotas; idle eviction; declarative pool prefill
- **Multi-Proxy HA Cluster** — 2–3 KEEL instances form a cluster with heartbeat-based peer health monitoring, config gossip (checksum-based reconciliation), transitive peer discovery, NOTIFY_SERVER event delivery; wire protocol magic `0x4B454C43`; `[cluster]` INI section + `KEEL_CLUSTER_*` env vars; 25 unit tests
- **Cluster Wire-Protocol Compression** — transparent zlib/zstd payload compression on the gossip TCP wire for WAN/cross-region deployments; codec flag encoded in top 2 bits of the `payload_len` header field (protocol v4); configurable per-node via `compress = none|zlib|zstd` and `compress_threshold_bytes`; `keel_compress`/`keel_decompress`/`keel_compress_bound` unified API; libzstd auto-detected at build time (`KEEL_HAS_ZSTD`); Dockerfiles updated with `libzstd-dev` (build) and `libzstd1` (runtime); 14 new unit + e2e tests (51 total in election suite); see [CLUSTER_WIRE_COMPRESSION.md](CLUSTER_WIRE_COMPRESSION.md)
- **Kubernetes Native** — Helm chart (`helm/keel/`) with ConfigMap, Secret, StatefulSet, PodMonitor; Go controller-runtime operator (`operator/`) with `KeelPool` CRD, reconcile loop, distroless Dockerfile; HPA guidance on `pool_wait_queue_enqueued` metric
- **Docker Official Images** — multi-arch (`linux/amd64`, `linux/arm64`) at `ghcr.io/virtlabs/keel:latest`; `KEEL_*` env var config via `docker/docker-entrypoint.sh`; production Compose templates; GitHub Actions publish on `v*.*.*` tags
- **Release Packaging** — tar.gz, DEB, and RPM packages via CPack with man pages (`keel(1)`, `keel.ini(5)`), systemd unit, post-install user/group creation, logrotate config, proper dependency declarations
- **Hardening CI** — unified CI gate with sanitizer matrix (ASan+UBSan, TSan, MSan), shadow diff, backpressure tests, syscall fault injection, network chaos, checksec verification, TLS audit, sqlmap fuzzing
- **GitHub Actions CI/CD** — build+test on every push/PR, sanitizer matrix on PRs, weekly hardening schedule, automated DEB/RPM/TGZ release packaging on tags

### Testing
- **90+ Unit/Integration/Combinatorial/Fuzz Tests** — covering all subsystems; 344 sharding assertions across 5 test suites; 729+ combinatorial matrix assertions
- **7 Docker Compose E2E Stacks** — PostgreSQL (E2E, Patroni, streaming), MySQL (replication, group, PXC, MariaDB Galera)
- **Fuzz Harnesses** — protocol fuzzer, state machine AFL++/libfuzzer harness
- **Concurrent Stress Tests** — thundering herd, 64-thread state machine stress, connection churn
- **OOM Injection** — `keel_mem_set_fail_countdown()` faults all major subsystems systematically

---

## In Progress

*Nothing currently in progress.*

---

## Planned — Future (P3)

### 1. Database Aliases
`[database_aliases]` config section mapping logical database names to configured names. Transparent rewrite of the startup message `database` parameter for zero-downtime database migrations.

### 2. Windows / macOS Native Packages
Complete kqueue path on macOS, ARM64 Linux (Graviton) builds, and native macOS binary distribution. Windows via wepoll (low priority).

---

## Summary

| Status | Count | Items |
|--------|-------|-------|
| **Completed** | 120+ features | Core engine, SQL routing, horizontal sharding (Phases 1–6 + Tiers 1–5), **scatter-merge aggregation engine** (COUNT/SUM/AVG/MIN/MAX/COUNT DISTINCT, GROUP BY, HAVING, ORDER BY, LIMIT, 2PC writes, Prometheus histogram, EXPLAIN enhancements), query result caching, cloud-native auth, enterprise auth (PAM/LDAP/mTLS/auth_query), session management, TLS/mTLS/kTLS, health/failover, observability (Prometheus, histograms, OTLP tracing, NDJSON logging, web UI, audit logging, query throttling), extensibility (hooks, declarative rules, OSC, NOTIFY/LISTEN, cross-service RYW), memory architecture, operations (multi-proxy HA cluster, **wire-protocol compression**, Kubernetes, Docker), 106+ tests |
| **In Progress** | 0 | — |
| **P3 (Future)** | 2 | Database aliases, Windows/macOS native packages |

---

## Where KEEL Leads

| Capability | Advantage |
|-----------|-----------|
| Dual-protocol | Only proxy handling both PostgreSQL and MySQL from a single binary |
| io_uring share-nothing reactor | Lowest-overhead async I/O; linear scaling with CPU cores |
| Prepared statement strategies (4 modes) | Most comprehensive PS multiplexing of any proxy |
| XID commit-in-doubt recovery | Unique safety guarantee: no orphaned transactions after backend crash |
| WAL LSN read-after-write consistency | Replicas always caught up to the client's last write; cross-service propagation via `SET keel.read_after_lsn` / `SHOW keel.write_lsn` |
| Scripting hooks (Lua + Python + native) | Hook at 4 pipeline stages, mix runtimes, priority chains |
| Zero-copy splice + DataRow bypass | Kernel-space forwarding; MSG_PEEK + splice for result sets |
| Session-context preservation | Multiplexing + session state — best of both worlds |
| SSV (Semantic State Virtualization) | Atom-layer consistency with hash-bucket pool index — unique |
| Runtime mode tiers | 4-tier feature gating with near-zero cost for disabled features |
| Formal invariant + state machine models | 12×12 matrix, 20-class checker, 9-domain state machine with exhaustive verification |
| Memory architecture | Pool slot ~400 B vs ~131 KB; 300× lower idle memory |
| Seccomp hardening | Runtime syscall filtering — not standard in any competitor |
| Automatic rebalancing | Timer-based with threshold-driven migration — unique among all proxies |
| Declarative query rules | INI-driven routing, rewriting, and blocking without hook code |
| NOTIFY/LISTEN proxying | Full pub/sub through a transaction-mode pool — unique among poolers |
| OSC-aware connection affinity | gh-ost and pt-osc work transparently with no config changes |
| Embedded web management UI | Zero-dependency SPA on the Prometheus port; `GET /ui` + `GET /api/status.json` |
| Multi-proxy HA cluster | Gossip-based config sync and peer monitoring with zero external dependencies |
| Enterprise & cloud auth | PAM, LDAP, mTLS, auth_query, AWS/GCP/Azure IAM in one binary |
| Horizontal sharding + scatter-merge | Transparent shard dispatch from SQL AST; built-in cross-shard COUNT/SUM/AVG/MIN/MAX/COUNT DISTINCT aggregation with GROUP BY, HAVING, ORDER BY, LIMIT; 2PC scatter writes; EXPLAIN SHARD PLAN FOR with full merge metadata |
| Kubernetes native | Helm chart + Go CRD operator with `KeelPool` resource and HPA integration |
| Query throttling | Per-rule token-bucket rate limiting via declarative INI config |
| Audit logging | Structured NDJSON security audit log with event filtering — compliance-ready |

### Core Engine
- **io_uring Share-Nothing Reactor** — per-worker io_uring rings with linked SQEs, registered FDs, batch sends, submit+wait merging, and zero-poll hot path
- **Dual-Protocol Support** — PostgreSQL v3 wire protocol and MySQL client/server protocol from a single binary (MySQL 9, Percona XtraDB Cluster, MariaDB Galera, Group Replication); MySQL protocol at full feature parity with PostgreSQL: 64-slot PS session map with FNV-1a session hash, `get_stmt_replay` for backend re-preparation, SESSION_TRACK_SCHEMA/GTIDS capture, cross-service RYW via `SET`/`SELECT @keel_write_gtid`, XA distributed-transaction pinning (BEGINS_TX / ENDS_TX / HARD_PIN), CALL/DO classification (340 protocol assertions)
- **Transaction Pooling** — backend connections shared across clients between transactions with non-blocking DISCARD ALL cleanup
- **Connection Multiplexing** — multi-threaded worker groups with SO_REUSEPORT kernel-level distribution
- **Connection Migration** — idle sessions transferred between workers via Unix socketpair SCM_RIGHTS + SPSC ring buffer for runtime load rebalancing
- **Automatic Rebalancing** — timer-based pressure detection with configurable threshold-driven migration
- **Admission Control** — per-worker frontend/backend limits with bounded FIFO wait queue, pressure feedback, peak tracking, and queue timeout
- **Zero-Copy Splice** — Linux splice(2) for kernel-space client↔backend data forwarding
- **MSG_PEEK + Splice DataRow Bypass** — peeks at backend message headers and splices DataRow frames directly via kernel pipe without userspace copy
- **Async Pool Warmup** — pre-connects backend connections asynchronously during startup
- **Auto FD Limit** — raises RLIMIT_NOFILE to hard max at startup (up to 1M file descriptors)
- **Crash Signal Handlers** — catches SIGSEGV/SIGABRT/SIGBUS with async-signal-safe diagnostics
- **Modern C23** — written in C23 with strict compiler warnings (-Wall -Wextra -Werror)

### SQL & Routing
- **Full SQL Lexer & Parser** — tokenizer → recursive descent parser → query tree (AST)
- **Automatic Read/Write Splitting** — reads to replicas, writes to primary
- **Weighted Load Balancing** — configurable weights per backend server
- **Transaction Pinning** — server affinity maintained during open transactions
- **FOR UPDATE Detection** — locking reads routed to primary
- **Sticky-Primary Override** — after a write, reads temporarily pinned to primary (100 ms window)
- **CTE / Window / Set-Operation Support** — WITH, OVER, UNION, INTERSECT, EXCEPT, MERGE, LOCK, LISTEN, VACUUM
- **Route Cache** — per-worker 1024-entry XXHash64 L1 cache with 8-probe linear search and LRU eviction
- **Pluggable Router Plugins** — per-database routing policies via keel_router_plugin_ops_t
- **Horizontal Sharding** — transparent shard-key extraction from SQL AST (SELECT/INSERT/UPDATE/DELETE), deterministic shard mapping (int64 modulo, xxhash64 string hashing), `$N` bound-parameter resolution for prepared statements, unified `keel_shard_plan_t` routing plan API (SINGLE / SCATTER / UNSUPPORTED), in-router shard rule registry with case-insensitive CRUD (`keel_router_add_shard_rule`, `keel_router_remove_shard_rule`, `keel_router_get_shard_rule`), `keel_router_plan_sql()` auto-dispatch across all registered rules, scatter fan-out `keel_router_scatter_servers()` with per-shard read/write routing, session-state-aware primary forcing (in-transaction, temp tables), `keel_scatter_plan_t` with per-shard `keel_route_decision_t` and failure accounting, **rule persistence** via `[shard_rule.*]` INI sections + `keel_router_load_shard_rules_from_config()`, **combined dispatch** via `keel_router_dispatch_sql()` / `keel_router_dispatch_sql_timed()`, **scatter result aggregation** via `keel_route_agg_t` + `keel_route_merge_fn`, **per-shard routing counters** (`shard_single_routes[]`, `shard_scatter_hits`, `shard_scatter_failed`), **admin virtual table** (`SELECT * FROM shard_rules`, `SHOW SHARD RULES`, `EXPLAIN SHARD PLAN FOR '<sql>'`), **range-based shard map** (`KEEL_SHARD_STRATEGY_RANGE` with INT64 threshold table), **multi-shard transaction coordinator** (scatter write participation tracking + cross-tx validation returning `KEEL_ERR_SHARD_CROSS_TX`), **shard migration state** (dual-write + read-from-new via `KEEL_SHARD_STATE_MIGRATING`), **connection pool per shard** (`keel_connpool_t` with min/max, idle eviction, health-check), **proxy session** (`keel_client_session_t` — route + acquire + execute in one call), **config hot-reload** (`keel_config_reload_shard_rules()` wired to SIGHUP), **Prometheus metrics** (`keel_router_write_prometheus()` served on `/metrics`), **query timeout** (`keel_router_dispatch_sql_timed()` + `KEEL_ERR_QUERY_TIMEOUT`)

### Session & State Management
- **Session-Context Preservation** — transparent SET parameter, search_path, and session variable continuity across backend reassignment via sorted K/V state profiles, XXHash64 fingerprinting, two-pointer merge diff, and 5-tier pool borrow
- **SSV (Semantic State Virtualization)** — atom-layer consistency model for prepared statement and GUC session state: hash-bucket pool index for O(1) backend matching, OPAQUE domain split, CONFIG domain atoms, WAL LSN integration, and semantic replay
- **Prepared Statement Pooling** — four strategies: virtualize (replay), pinning (session affinity), tracking (simple+extended), anonymous (JIT rewrite)
- **XID Probe + Commit-in-Doubt Recovery** — instruments COMMIT with txid_current() and re-queries txid_status() on a fresh connection if the backend dies before ReadyForQuery
- **Read-After-Write Consistency** — WAL LSN tokens ensure replicas have replayed the client's last write before serving reads
- **Runtime Mode Tiers** — four tiers (PROXY / POOL / SMART / FULL) that gate features behind single integer comparisons for zero-overhead when disabled
- **Cross-Feature Invariant Model** — 12×12 compatibility matrix with runtime checker and 20 violation classes
- **Formal State Machine Model** — 9-domain session state machine with transition matrices, contracts, predicates, and event journal

### Security & TLS
- **TLS + mTLS + kTLS** — frontend TLS termination, backend TLS, optional client-cert verification, kernel TLS acceleration, cipher suite enforcement, cert hot-reload via SIGHUP, and downgrade protection
- **Cipher Policy** — TLS 1.2 cipher list, TLS 1.3 ciphersuite list, and minimum version enforcement
- **Certificate Hot-Reload** — SIGHUP triggers atomic SSL_CTX swap without dropping connections
- **mTLS Session Enrichment** — peer cert subject/issuer stored in session metadata for hook inspection
- **Seccomp System-Call Filter** — baseline and strict BPF profiles with PR_SET_NO_NEW_PRIVS
- **Privilege Drop** — drop root after bind and RLIMIT setup, setuid/setgid to unprivileged user
- **Binary Hardening** — PIE, NX, stack canary (stack-protector-strong), Full RELRO enforced at build time
- **Ephemeral Test PKI** — `scripts/generate-test-certs.sh` generates a self-signed CA, server, backend, and client certs for dev/test (certs not stored in git)

### Authentication
- **SCRAM-SHA-256** — full client + backend auth for PostgreSQL (OpenSSL)
- **MD5** — legacy PostgreSQL password auth
- **caching_sha2_password** — MySQL default auth plugin (OpenSSL)
- **mysql_native_password** — MySQL legacy auth
- **Trust / Password** — simple auth methods
- **User File Support** — external credential management (userlist.txt)
- **Cloud-Native Authentication** — AWS RDS IAM (SigV4 token generation, 14-min cache), GCP Cloud SQL IAM (service account JWT + metadata server + token file fallback), Azure AD/Entra (IMDS managed identity + env var + token file fallback)

### Health & Failover
- **Pluggable Probe System** — postgres (SQL), patroni (HTTP REST), mysql, tcp, exec (custom script)
- **Automatic Role Detection** — pg_is_in_recovery() for PostgreSQL, REST API for Patroni
- **Patroni Cluster Discovery** — GET /cluster (all members) with /patroni fallback; pure-C HTTP/1.0 client
- **Failover Handling** — dead servers removed, role changes detected, routing adjusted
- **WAL Position Tracking** — replication lag monitoring via LSN

### Observability & Admin
- **Admin Console** — PostgreSQL wire protocol on dedicated TCP port; 21 commands including SHOW, PAUSE, RESUME, DISABLE/ENABLE SERVER, ADD/REMOVE SERVER, KILL CLIENT, DRAIN, SET (13 runtime keys), RELOAD
- **SQL-Syntax Admin Query Language** — full SQL DML against virtual admin tables: SELECT from 14 tables (pools, servers, clients, config, stats, databases, users, peers, cluster, cluster_stats, tracing, etc.), UPDATE config/servers, INSERT INTO servers/peers, DELETE FROM servers/clients/peers
- **keel-cli** — standalone command-line binary for scripting (zero library dependencies)
- **JSON Output** — append FORMAT JSON to any admin command
- **Prometheus Metrics** — /metrics endpoint with pool, session, TLS, query, and error counters
- **K8s Health Endpoints** — /healthz, /readyz, /livez on the Prometheus HTTP port
- **Grafana Dashboard** — pre-built JSON at etc/grafana/keel-dashboard.json
- **Per-Worker Statistics** — queries_total, queries_read, queries_write, bytes counters, migration stats
- **TLS/kTLS Metrics** — handshake, activation, fallback, failure, cert reload, and downgrade rejection counters
- **Pluggable Log Backend** — stdout, file, and syslog log plugins with hot-loaded configuration
- **Query Logging** — configurable per-query log with mode filter (all, read, write, none)

### Extensibility
- **Hook/Trigger System** — 4 query pipeline extension points (after_query_read, after_query_parse, before_route, before_send)
- **Lua Hooks** — embedded Lua 5.4/LuaJIT scripting
- **Python Hooks** — embedded CPython 3.x scripting
- **Native Plugin Hooks** — .so/.dylib shared libraries loaded via dlopen()
- **Mutable Context** — hooks can inspect and modify session, query, routing, and flags in-place
- **Priority Chains** — hooks execute in priority order; mixing Lua + Python + native at the same point

### Memory Architecture
- **Arena Allocator** — fast bump allocation for request-scoped memory
- **Slab Allocator** — fixed-size object pools for sessions and connections
- **Pool Allocator** — O(1) free-list allocation for recv contexts (~400 B metadata) and pool waiters
- **Lazy I/O Buffers** — heap-backed buffers allocated on demand, not at pool creation (300× lower idle memory vs embedded 64 KB arrays)
- **Ring Buffer** — lock-free circular buffer for protocol data
- **NUMA-Aware Allocator** — node-local and interleaved allocation via mmap+mbind (no libnuma dependency)

### Operations
- **Live Configuration Reload** — SIGHUP reloads pool sizes, timeouts, server weights, probe timing, rebalancing config, TLS certificates, and log level without restart
- **Graceful Drain/Shutdown** — lifecycle state machine (CREATED → ACTIVE → DRAINING → STOPPING → STOPPED), configurable drain timeout, CID-aware force-close
- **Release Packaging** — tar.gz, DEB, and RPM packages via CPack with man pages (`keel(1)`, `keel.ini(5)`), systemd unit, post-install user/group creation, logrotate config, and proper dependency declarations
- **Hardening CI** — unified CI gate with sanitizer matrix (ASan+UBSan, TSan, MSan), shadow diff, backpressure tests, syscall fault injection, network chaos, checksec verification, TLS audit, and sqlmap fuzzing
- **GitHub Actions CI/CD** — build+test on every push/PR, sanitizer matrix on PRs, weekly hardening schedule, automated DEB/RPM/TGZ release packaging on tags

### Testing
- **86 Unit/Integration Tests** — covering memory, auth, parser, routing, protocol flow, hooks, failover, migration, TLS, drain, state machine, invariant model, SSV, security, observability, cloud auth, SQL admin, horizontal sharding (Phases 1–6 + Tiers 1–5: combined dispatch, scatter aggregation, per-shard counters, range map, transaction coordinator, migration state, connection pool, proxy session, hot-reload, metrics, timeout), OOM/allocation-failure injection (`keel_mem_set_fail_countdown()`), split-at-every-byte protocol generator (all PG and MySQL wire messages), connection pool exhaustion and wait-queue verification, route cache adversarial collision stress, and prepared-statement replay × failover × GUC session-hash stability
- **7 Docker Compose E2E Stacks** — PostgreSQL (E2E, Patroni, streaming), MySQL (replication, group, PXC, MariaDB Galera)
- **Combinatorial Matrix Tests** — PS×pool, TLS×splice, session×hooks, crash×recovery, PG×MySQL (729+ assertions)
- **Fuzz Harnesses** — protocol fuzzer, state machine AFL++/libfuzzer harness
- **Concurrent Stress Tests** — thundering herd, 64-thread state machine stress, connection churn

---

# Roadmap to Zero Failing Tests (M0–M12)

> Companion to [LIMITATIONS.md](LIMITATIONS.md). The section above describes
> *what* has shipped; everything below sequences *how* and *when* to close
> the remaining failing-test backlog, plus the engineering practices to
> apply along the way.

---

## Guiding principles

1. **Stability before features.** The allocator-corruption bug (§4)
   destabilises everything else; nothing else gets a green run while
   it can fire. Fix it first.
2. **Silent wrong results are P0.** Anything that returns a wrong
   number without an error (most of §1) is more dangerous than
   features that error out loudly. Triage by severity, not by test
   count.
3. **Routing correctness > merge breadth.** A query that goes to one
   shard correctly is always preferable to a scatter that *almost*
   works. Therefore §2 (parameter-aware routing) blocks several
   merge limitations from biting users.
4. **Land in narrow, reviewable PRs.** Each milestone below is a
   single PR target. Avoid omnibus changes that mix router, scatter,
   and SSV — they are individually hard to review.
5. **Add a regression test before, or alongside, the fix.** Many of
   the failing tests already exist; for the items that pass in
   isolation today, write the load-mixed reproducer first.

---

## Milestones at a glance

| M | Name | Cumulative tests fixed | Calendar | Risk |
|---|---|---|---|---|
| **M0** | Stability — allocator corruption | (enables M3 wins) | wk 1 | High |
| **M1** | Quick-win merge bugs | +5 (OFFSET, NULLS, GROUP-NULL) | wk 1–2 | Low |
| **M2** | HAVING & expression aggregates | +9 | wk 2–3 | Med |
| **M3** | SSV reliability under load | +25 | wk 3 | Med (depends on M0) |
| **M4** | Parameter-aware routing | +5 (+ unblocks §1.13) | wk 4–5 | High |
| **M5** | CTE inlining | +8 (non-recursive) | wk 5–6 | Med |
| **M6** | Recursive CTE single-shard | +4 | wk 6 | Low |
| **M7** | UNION-ALL with merge | +1 | wk 7 | Med |
| **M8** | JSONB & extra aggregates | +6 | wk 7–8 | Low |
| **M9** | Routing edge cases (IN/BETWEEN/COALESCE) | +3 | wk 8 | Low |
| **M10** | Pool fairness & PS perf | +2 | wk 8–9 | Low |
| **M11** | Window functions v1 | +5 | wk 9–11 | High |
| **M12** | Percentile & ordered-set aggregates | +2 | wk 11–12 | Med |

**Cumulative fixed:** 75 / 76 by end of M12. The remaining test
(`test_concurrent_updates_no_lost_update`) goes green when M4 lands
(it is a downstream effect of §2). Buffer for slip: 2 weeks.

**Total calendar:** ~12 weeks of focused single-engineer work,
or ~6 weeks at 2 engineers running M-pairs in parallel.

---

## M0 — Allocator corruption (stability foundation)

> **Why first:** Every higher-tier improvement is invalidated by 36
> heap-corruption events per run. Without M0, M3 is a guessing game.

### Goals
- Reproduce the corruption deterministically.
- Identify the offending writer.
- Patch and confirm zero `Invalid memory block in free` events in a
  full e2e run.

### Steps
1. **Reproducer harness** (0.5 d).
   Write a `bench/repro_alloc_corruption.sh` that runs
   `test_sharding_comprehensive.py` (heavy scatter) in a loop while
   `test_prepared_statements_ssv.py` runs concurrently. Confirm
   `docker logs e2e-keel | grep -c "Invalid memory"` rises monotonically.
2. **ASan build** (0.5 d).
   Use existing `build-asan/`. Recompile with `-fsanitize=address
   -fno-omit-frame-pointer`. Re-run the harness.
3. **Triage the ASan trace** (1 d).
   Expect a heap-buffer-overflow at the bottom of an
   `engine_scatter.c` or `keel_pg_result_*` write loop. Map back to
   the exact realloc path.
4. **Fix** (0.5–2 d).
   Most likely candidates:
   - Off-by-one in row-buffer growth: writing `len + 1` into a buffer
     sized `len`.
   - A short-tag/long-tag CommandComplete write that miscalculates.
   - A merge-state struct that grows past its initial capacity.
5. **Guard-page CI mode** (0.5 d).
   Add `KEEL_MEM_DEBUG_GUARD_PAGE=1` flag to `src/mem/mem.c` and a
   nightly job. Catches future regressions immediately.

### Exit criteria
- `grep -c "Invalid memory" $(docker logs e2e-keel)` returns 0 after
  a full e2e run.
- ASan run is clean.

### Risks
- ASan timing changes the bug. Mitigation: keep TSan and Valgrind
  alternates ready; if all three hide the bug, instrument
  `keel_alloc_header_t` with a write barrier on neighbour blocks.
- Fix turns out to be in protocol code (Bind buffer reuse). Then
  scope expands by 1–2 days.

### Deliverables
- 1 PR: `src/mem/` + offending caller fix + harness in `bench/`.
- Test: `tests/perf_corruption_smoke.py` runs the harness, asserts
  zero corruption events.

---

## M1 — Quick-win merge bugs

> **Why second:** Lowest-risk PRs that fix silent-wrong-result bugs.
> Each is < 1 day work and unblocks 1–3 tests.

### M1a · OFFSET correctness (§1.6, 3 tests)

- **File:** `src/engine/engine_scatter.c` near `sc_rewrite_limit_offset`
  (or equivalent helper).
- **Change:** Strip OFFSET from per-shard SQL; remember `m`; in the
  merge consumer drop the first `m` post-merge rows.
- **Test:** the 3 existing tests + a new edge-case test for
  `OFFSET larger than total`.

### M1b · NULLS FIRST/LAST comparator (§1.7, 1 test)

- **File:** `src/engine/engine_scatter.c` merge comparator.
- **Change:** Thread `nulls_first` flag from the parsed sort key into
  the comparator; flip the NULL-vs-value verdict accordingly.

### M1c · GROUP BY NULL key equality (§1.8, 1 test)

- **File:** merge group-key hash/equality.
- **Change:** Use IS NOT DISTINCT FROM semantics for grouping
  (NULL ≡ NULL).

### Exit criteria
- 5 named tests pass; full suite passes count rises to 485.

### Risks
- A bad NULLS comparator change can flip ordering for non-null tests
  too. Mitigation: a property-test that verifies the comparator is
  a strict total order with the expected NULLS placement.

---

## M2 — HAVING & expression aggregates

> **Why now:** Combined fix for §1.9 (CASE in aggregate) and §1.12
> (HAVING & bucketing). Both ride on the same expression-pushdown
> mechanism.

### Steps
1. **HAVING strip-and-reapply** (1 d).
   - Walk the parsed HAVING tree; partition predicates into
     "aggregate-dependent" (post-merge) and "scalar-only" (push down).
   - Per-shard SQL retains scalar-only predicates; aggregate
     predicates are evaluated after the merge.
2. **Expression pushdown for projection / GROUP BY** (2–3 d).
   - When the merge classifier sees `SUM(<expr>)`, `<expr>` is pushed
     into the per-shard SQL projection list and the merge column is
     mapped to the (now scalar) intermediate.
   - GROUP BY of an expression: rewrite per-shard SQL to add the
     expression to the projection and key the merge by it.
3. **Volatile-function guard** (0.5 d).
   - If `<expr>` contains a volatile function (`random()`, `now()`,
     `uuid_generate*`), refuse pushdown and either error with a
     clear message or pin to a single shard.

### Exit criteria
- 9 tests fixed (3 in TestComplexQueries, 6 in
  TestScatterAggregatesComprehensive + TestComplexAggregationAtScale).
- Cumulative: 494 passing.

### Risks
- Type inference: per-shard SQL must preserve the original output
  type after pushdown so later merge math is type-stable.
- HAVING with subqueries — out of scope; refuse and pin.

---

## M3 — SSV reliability under load

> **Why now:** With M0 stable and the merge bugs no longer crashing
> sessions, the 25 SSV failures should largely *evaporate*. This
> milestone's real work is auditing the residual 1–5 that don't.

### Steps
1. **Re-run the suite after M0** (0.5 d).
   Many of the 25 will pass without further work. Triage the residual.
2. **SSV reset audit** (1–2 d).
   `src/session/ssv.c`: review every backend-disconnect path and
   confirm the per-session prepared-statement table is invalidated.
   Add asserts in debug build.
3. **Replay timeout** (0.5 d).
   Add `keel.ssv_replay_timeout_ms` (default 5000). On timeout,
   surface a clear error to the client; do not lock the pool.
4. **Pool reservation for SSV replay** (1–2 d).
   When SSV needs a backend, place its acquire at the front of the
   wait queue so long scatters cannot starve it indefinitely.
5. **Stress test in CI** (0.5 d).
   Add `bench/stress_ssv_under_scatter.sh` that mixes the two
   workloads and asserts no connection drops.

### Exit criteria
- `test_prepared_statements_ssv.py` passes 100% in the full suite.
- Cumulative: 519 passing.

### Risks
- Pool reservation can introduce starvation for non-SSV requests.
  Mitigation: cap reserved slots at `min(2, max_size / 4)`.
- A residual bug specific to backend rotation may need protocol-level
  fixes (Parse rewriting on rotation). Budget 2 extra days as buffer.

---

## M4 — Parameter-aware routing (largest single milestone)

> **Why now:** Once SSV is solid, the next correctness priority is
> ensuring every parameterised statement reaches the right shard. This
> is the largest single piece of work in the roadmap.

### Phases

**M4.1 · AST tagging at Parse time** (2 d)
- Extend `keel_router_dispatch_sql()` return type with a deferred
  plan: `{ shard_key_param_index: int, shard_key_role: enum,
  fallback_strategy: scatter|round_robin }`.
- Cache the plan keyed on the prepared-statement name.

**M4.2 · Bind-side resolution** (3 d)
- In `src/protocol/postgres/postgres_flow.c` Bind handler, after
  parsing parameter values, fetch the deferred plan, materialise the
  shard key (`keel_shard_key_t`), call `keel_shard_map_key_rule`
  (range or hash), pick the backend.
- Defer Parse + Bind + Execute forwarding to *after* shard selection.

**M4.3 · Binary parameter formats** (2 d)
- Decode the per-parameter format-codes array from Bind.
- For binary `int8`/`int4`/`int2`/`text`/`uuid`, decode without
  copying when possible.

**M4.4 · Negative-int routing parity** (0.5 d)
- Audit `keel_shard_map_key`: the test contract is
  `abs(key) % shard_count`; current code does
  `(uint64_t)key % shard_count`. For `-1` both happen to land on
  shard 1, but for `-2` they diverge. Decide the contract
  (recommend `abs(key) % shard_count` to match Postgres `hashint8`'s
  intuition and the existing test-suite expectations) and align both
  Python and C.
- Add property tests covering ±100 keys and large negatives.

**M4.5 · Composite & IN-list shard keys** (2 d, scope to define)
- `WHERE id IN ($1,$2,$3)` → enumerate all shards; subset dispatch
  (also serves §1.11).
- Composite keys (multi-column shard rules) — defer to M9 unless
  trivially in scope.

**M4.6 · Re-route on rebind** (1 d)
- A Parse with no Bind, then Bind with a different parameter set,
  must re-route per Bind.
- Once the session is pinned (txn open), ignore deferred routing.

**M4.7 · Tests & docs** (1 d)
- Existing 5 tests + new tests for: binary format `int8` parameter,
  composite key, IN-list parameters, mixed Parse/Describe/Bind order.

### Exit criteria
- 5 named tests pass.
- `test_concurrent_updates_no_lost_update` (§1.13) passes as a side
  effect.
- Cumulative: 525 passing.

### Risks
- **Compatibility.** Some clients (e.g. JDBC with cached
  PreparedStatement on the *server* side) may misbehave when KEEL
  defers the Parse forwarding. Mitigation: forward Parse early to a
  "scratch" backend just for Describe responses; commit the real
  backend on first Bind.
- **Performance.** Deferred Parse adds one round-trip on the cold
  path. Mitigation: prefetch likely shards based on the previous
  Bind for the same statement.
- **Schema drift.** If shards diverge (operational accident), Describe
  on a scratch backend may differ from the eventually-routed backend.
  Mitigation: track a per-shard schema fingerprint, error if they
  diverge.
- **Format codes 0/1 mixed.** Edge-case Bind messages can use text
  format for some params and binary for others. The decoder must
  per-parameter respect its own format.
- **Negative-int parity break.** Changing the C-side hash from
  `(uint64_t)key` to `abs(key)` is a *data migration* for any
  customer running today: rows previously hashed to shard A may need
  to live on shard B after the change. Mitigation: ship the new
  algorithm under a config flag (`shard_key_hash = legacy|abs`),
  default `legacy` for one minor version, document the migration
  procedure (`pg_dump | pg_restore` per shard or reshard tool).

---

## M5 — CTE inlining (non-recursive)

### Steps
1. **Detect non-recursive WITH** in `src/sql/sql_classifier.c`.
2. **Single-reference inlining** (3 d): substitute the CTE body in
   place; rename intermediate aliases to avoid clashes.
3. **Multi-reference materialisation** (push to M11 if scope tight):
   for now, refuse such CTEs and pin the query to a single shard with
   a clear log message.
4. **Side-effect detection**: refuse CTEs containing
   `INSERT/UPDATE/DELETE … RETURNING`.

### Exit criteria
- 8 tests fixed (TestCTEs + TestComplexAggregationAtScale CTE tests).
- Cumulative: 533.

### Risks
- A CTE may shadow an outer table name; rename collisions must be
  resolved correctly.
- LATERAL CTEs change evaluation order; do not inline.

---

## M6 — Recursive CTE single-shard fallback

### Steps
1. **Detect WITH RECURSIVE** in classifier.
2. **Walk recursion body**: if it touches no sharded table, route as
   `KEEL_DISPATCH_SINGLE_ANY` to shard 0 (or pool LRU).
3. **Otherwise refuse** with `ERROR: cross-shard recursive CTEs are
   not supported`.

### Exit criteria
- 4 recursive CTE tests pass for the local-only case.
- Cumulative: 537.

### Risks
- An "ostensibly local" recursive CTE may join sharded tables in the
  outer SELECT; must walk the entire query tree.

---

## M7 — UNION ALL with merge

### Steps
1. **Top-level SetOp dispatcher** (2 d): each leg is dispatched
   independently; results are concatenated in-proxy.
2. **UNION (de-dup)** (1 d): hash-dedup on concatenation; spill above
   threshold.
3. **ORDER BY across UNION ALL** (1 d): k-way merge of the legs.
4. **Outer aggregate over UNION ALL** (0.5 d): re-aggregate after
   concatenation.

### Exit criteria
- 1 named test fixed.
- Cumulative: 538.

### Risks
- INTERSECT / EXCEPT — out of scope; mark as unsupported.

---

## M8 — JSONB scatter & extra aggregates

### Steps
1. **Aggregate function table extension** (1 d): add `array_agg`,
   `jsonb_agg`, `string_agg`, `jsonb_object_agg` with concat-style
   merge ops.
2. **JSONB extractor pushdown** (1 d, depends on M2): allow
   `payload->>'field'` and `payload->'field'->>'sub'` in GROUP BY
   and projection via M2's expression pushdown.
3. **Edge cases**: `array_agg(x ORDER BY y)` and `string_agg(x, sep
   ORDER BY y)` need ordered concat; can defer to M11 (windows).

### Exit criteria
- 6 tests pass.
- Cumulative: 544.

### Risks
- Encoding: JSONB is binary on the wire; ensure shard responses are
  consistently text or binary before merge.

---

## M9 — Routing edge cases

### Steps
1. **IN-list targeted dispatch** (1 d): subset-of-shards dispatch
   for `key IN (lit, …)`. Re-uses M4 infrastructure for parameter
   variant.
2. **COALESCE single-row race** (0.5 d): scatter must wait for all
   shards (or a definitive "row found" signal) before forwarding the
   row.
3. **BETWEEN ordering** (1 d): ensure consistent ORDER BY in
   per-shard SQL and merge comparator.

### Exit criteria
- 3 tests pass.
- Cumulative: 547.

### Risks
- The COALESCE wait can add latency on slow shards. Mitigation: fast
  path when one shard returns a row early *and* the row is unique by
  shard key.

---

## M10 — Pool fairness & PS fast-path TPS

### M10a · Pool burst (§5, 1 test)
- Convert eviction-on-full to queue-on-full with bounded queue.

### M10b · PS fast-path (§6, 1 test)
- Skip Parse-rewrite when backend pinned & no new statements.
- Memoise router output per-connection per-SQL.

### Exit criteria
- 2 tests pass.
- Cumulative: 549.

---

## M11 — Window functions v1

> **Why this late:** Largest piece of unimplemented merge logic.
> Worth taking time to design.

### Phases

**M11.1 · Design doc** (2 d)
- Document supported windows: `OVER ()`, `OVER (PARTITION BY const)`,
  `OVER (PARTITION BY … ORDER BY … ROWS BETWEEN UNBOUNDED PRECEDING
  AND CURRENT ROW)`.
- Out-of-scope v1: `RANGE` frames, `LAG`/`LEAD` with offset, named
  windows, frame exclusion.

**M11.2 · Per-shard collection** (3 d)
- Per-shard SQL must return the partition-by + order-by key columns
  in addition to the projection, so the proxy can re-key.

**M11.3 · In-proxy window evaluator** (5 d)
- New module `src/engine/engine_window.c`.
- Streaming partition iteration with spill-to-disk above
  configurable threshold (`keel.window_max_partition_rows`).

**M11.4 · Function set** (2 d)
- v1 functions: `sum`, `count`, `avg`, `min`, `max`, `first_value`,
  `last_value`, `row_number`, `rank`, `dense_rank`.

**M11.5 · Tests** (1 d)
- 5 named tests plus new property tests.

### Exit criteria
- 5 tests pass.
- Cumulative: 554.

### Risks
- Memory blow-up: partitions must spill. Spill format design adds
  scope.
- Correctness bar is exact; tests should compare against PostgreSQL
  on a single shard with the same data.

---

## M12 — Percentile & ordered-set aggregates

### Phases

**M12.1 · Exact path** (2 d)
- For percentile / mode / ordered-set aggregates, ship the raw input
  column from each shard; merge into a sorted stream; compute centrally.

**M12.2 · Approximate path** (3–5 d, optional)
- Integrate a quantile sketch (e.g. t-digest C lib) behind
  `keel.percentile_mode = exact|approx`.

**M12.3 · Memory caps** (1 d)
- `keel.percentile_max_rows` per query; clear error on exceed.

### Exit criteria
- 2 tests pass.
- Cumulative: 556.

### Risks
- Sketch dependency: vet license, audit code, decide whether to
  vendor or build-time link.
- Approximate answers must be opt-in to avoid silent precision loss.

---

## Cross-cutting workstreams

These run alongside the milestones and have their own owners.

### CC1 · Telemetry (continuous)
- Counter `keel_scatter_unsupported_pattern_total{pattern=…}` —
  emitted by every code path that today silently produces wrong
  results.
- Counter `keel_router_param_routing_deferred_total` — visibility
  into M4 effectiveness.
- Counter `keel_alloc_header_corruption_total` — already exists in
  spirit (the log line); promote to a Prometheus metric so M0
  regressions are caught immediately.

### CC2 · Documentation (continuous)
- New `docs/sql-feature-matrix.md` listing each SQL feature and its
  scatter support level (`full | merge-required | single-shard-only |
  unsupported`). Updated as each milestone ships.
- Update `README.md` "Limitations" section to link here.

### CC3 · CI hygiene (M0–M3)
- Add ASan + TSan + Valgrind matrix builds, nightly.
- Promote `bench/repro_alloc_corruption.sh` to a CI gate after M0.

### CC4 · Test-suite maintenance (continuous)
- Aspirational tests that lag the roadmap should be marked
  `xfail(strict=True, reason="…", raises=…)` referencing the
  relevant LIMITATIONS.md section. Remove the `xfail` as the
  milestone lands.

---

## Risk register

| ID | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Allocator bug elusive under ASan | Med | High | TSan + Valgrind + mprotect guard pages as fallback |
| R2 | M4 negative-int hash change requires customer data migration | High | High | Ship under config flag, default to legacy for 1 release, document migration |
| R3 | M11 spill format is throw-away work if executor architecture changes | Med | Med | Keep spill format internal (no on-disk persistence); guard behind feature flag |
| R4 | M3 residual SSV bug needs protocol-level fix | Med | Med | Reserve 2-day buffer in M3 |
| R5 | M5 inlining breaks complex BI tool queries (alias clashes) | Med | High | Comprehensive snapshot tests against pgbench / dbeaver-style queries before release |
| R6 | M2 expression pushdown changes per-shard SQL in ways triggering planner regressions | Low | Med | Capture per-shard EXPLAIN before/after for the suite |
| R7 | M7 UNION dedup is unbounded-memory by default | Med | High | Hard cap with config; ERROR on overflow |
| R8 | M11 window evaluator becomes a maintenance hotspot | Med | Med | Keep v1 strictly limited; defer extras to v2 |
| R9 | Roadmap lands behind schedule due to operational interrupts | High | Low | Buffer 2 weeks at end; M11/M12 are the natural cut line |

---

## Success metrics

The roadmap is "done" when:

1. `pytest tests/e2e/ -q` reports **586 passed, 0 failed, 0 errors**
   (tolerated: skips for explicitly out-of-scope tests with reason
   strings).
2. ASan + TSan + Valgrind builds run the suite cleanly nightly for
   one full week.
3. `keel_alloc_header_corruption_total` is 0 over a 24-hour stress
   run.
4. `keel_scatter_unsupported_pattern_total` is 0 across a representative
   workload (no silent fallbacks remain).
5. `docs/sql-feature-matrix.md` lists every relevant feature; each
   `unsupported` row links to the issue tracking the next planned
   milestone.

---

## Recommended PR sequence (single-engineer view)

```
PR-1 (M0)  : src/mem/ guard + offending caller fix
PR-2 (M1a) : engine_scatter OFFSET strip-and-reapply
PR-3 (M1b) : NULLS FIRST/LAST comparator
PR-4 (M1c) : GROUP BY NULL key equality
PR-5 (M2)  : HAVING & expression pushdown
PR-6 (M3)  : SSV reset + replay timeout + pool reservation
PR-7 (M4.1+M4.2) : Deferred routing skeleton
PR-8 (M4.3): Binary parameter formats
PR-9 (M4.4): Hash parity (under config flag)
PR-10 (M4.5+M4.6): IN-list + rebind + composite scope
PR-11 (M5) : CTE single-reference inlining
PR-12 (M6) : Recursive CTE single-shard fallback
PR-13 (M7) : UNION ALL with merge
PR-14 (M8) : JSONB & extra aggregates
PR-15 (M9) : Routing edge cases (IN/BETWEEN/COALESCE)
PR-16 (M10): Pool fairness + PS fast-path
PR-17 (M11.1) : Window functions design doc (review only)
PR-18 (M11.2-4): Window evaluator + functions
PR-19 (M12) : Percentile / ordered-set aggregates
PR-20      : docs/sql-feature-matrix.md final pass + xfail removals
```

Each PR carries its own tests + CHANGELOG entry. CC1 (telemetry) is
folded into the relevant feature PR rather than landing standalone.

---

## Definition of done per PR

Every PR in this roadmap must:

1. Pass the full e2e suite locally with no new failures.
2. Pass ASan and TSan builds.
3. Add or unmark at least one targeted regression test.
4. Update `docs/sql-feature-matrix.md` if the supported feature set
   changed.
5. Update `LIMITATIONS.md` to remove the resolved item or move it to
   a "Known issues — historical" section.
6. Update `CHANGELOG.md`.
7. Carry a short design note in the PR description explaining the
   decision tree (especially for M4 deferred routing and M11 window
   model).
