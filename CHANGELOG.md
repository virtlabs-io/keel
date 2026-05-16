# Changelog

All notable changes to KEEL are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
KEEL uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from `alpha-0.3.0` forward.

> **Migration notes** for breaking config key changes appear in each release
> section under a `### Migration` heading.

---

## [Unreleased]

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

[Unreleased]: https://github.com/virtlabs-io/dbcp-keel/compare/alpha-0.3.0...HEAD
[alpha-0.3.0]: https://github.com/virtlabs-io/dbcp-keel/compare/alpha-0.1...alpha-0.3.0
[0.3.0-dev]: https://github.com/virtlabs-io/dbcp-keel/compare/alpha-0.1...alpha-0.3.0
[alpha-0.1]: https://github.com/virtlabs-io/dbcp-keel/releases/tag/alpha-0.1

