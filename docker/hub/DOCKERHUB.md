<img src="https://raw.githubusercontent.com/virtlabs-io/keel/main/keel.png" alt="KEEL — More than a pooler. The missing link between your application and a truly elastic database" width="300">

# KEEL — Database Connection Pooler & Proxy

**KEEL** is a correctness-first PostgreSQL connection pooler and intelligent
database proxy. Its production candidate profile is conservative PostgreSQL
transaction pooling with prepared-statement virtualization. Broader routing,
MySQL, sharding, and cluster features exist, but are labelled by maturity and
should be enabled deliberately.

> [GitHub](https://github.com/virtlabs-io/keel) · [Documentation](https://github.com/virtlabs-io/keel/tree/main/docs) · [Changelog](https://github.com/virtlabs-io/keel/blob/main/CHANGELOG.md) · [License: AGPL-3.0](https://github.com/virtlabs-io/keel/blob/main/LICENSE)
 
---

## Why KEEL?

| Problem | How KEEL solves it |
|---------|-------------------|
| Too many connections overloading the database | Transaction pooling multiplexes hundreds of app threads over a small backend pool |
| Read replicas sitting idle while the primary is overloaded | Smart read/write routing exists in the Hardening bucket and can be enabled deliberately |
| ORM prepared statements breaking pooling (Hibernate, pgx, GORM, SQLAlchemy) | Transparent prepared-statement virtualization — no ORM changes required |
| Session state (SET, search_path, GUCs) lost when connections are reused | Session-context preservation is being hardened for stateful workloads |
| Patroni failover causing application errors | Role observation and conservative routing are in the Hardening bucket |
| Cloud IAM token rotation for RDS/Cloud SQL/Azure | Cloud auth providers are available as Experimental/Hardening features depending on provider and deployment |
| Compliance requirements for audit logging and TLS enforcement | Structured audit log, mTLS, LDAP/PAM integration, privilege drop, seccomp filter |

---

## Available Images

| Tag | Base | Size | Use when |
|-----|------|------|---------|
| Immutable Debian release tags, `debian` | Debian Trixie Slim | ~120 MB | Default base, best library compatibility |
| `ubuntu`, immutable Ubuntu tags | Ubuntu 24.04 LTS | ~140 MB | Ubuntu-centric environments |
| `alpine`, immutable Alpine tags | Alpine 3.20 | ~40 MB | Minimal footprint, musl libc |
| `latest` | Debian base | ~120 MB | Mutable convenience tag for development |

All images are multi-arch: **linux/amd64** and **linux/arm64**.

Production deployments should pin an immutable release tag or digest.

---

## Quick Start

### Minimal PostgreSQL pool (replacing PgBouncer)

```bash
docker run -d \
  -e KEEL_SERVER_HOST=your-postgres-host \
  -e KEEL_SERVER_PORT=5432 \
  -e KEEL_SERVER_USER=app \
  -e KEEL_SERVER_PASSWORD=secret \
  -e KEEL_SERVER_DATABASE=mydb \
  -p 7432:7432 \
  vlbsio/keel:<release-tag>
```

Then connect your app to `localhost:7432` exactly as you would connect to PostgreSQL directly.

### With a config file

```bash
docker run -d \
  -v /path/to/keel.ini:/etc/keel/keel.ini \
  -p 7432:7432 \
  vlbsio/keel:<release-tag>
```

### Docker Compose

```yaml
services:
  keel:
    image: vlbsio/keel:<release-tag>
    ports:
      - "7432:7432"   # PostgreSQL proxy
      - "9101:9101"   # Prometheus metrics
    volumes:
      - ./keel.ini:/etc/keel/keel.ini
    restart: unless-stopped
```

---

## Feature Readiness

KEEL has a broad feature surface. Features are labelled by production maturity.
The canonical maturity matrix lives in
[`docs/PRODUCTION_READINESS.md`](https://github.com/virtlabs-io/keel/blob/main/docs/PRODUCTION_READINESS.md);
the summary below mirrors it.

> **Recommended production deployment:** PostgreSQL in
> `mode = pool` with `prepared_statement = virtualize` and
> `experimental_features = false`. MySQL support, smart routing, SSV,
> Patroni failover, and transaction tracking are in the **Hardening** bucket
> — enable deliberately with monitoring.

### Production candidate

These are the supported production candidate capabilities when configured
conservatively and validated against your workload.

| Feature | What it does |
|---------|-------------|
| **PostgreSQL transaction pooling** | Connections returned to the pool after each transaction; hundreds of app connections share a small backend pool |
| **PostgreSQL prepared-statement virtualization** | Named prepared statements transparently replayed on any backend — works with Hibernate, pgx, GORM, SQLAlchemy, Prisma |
| **Full TLS + mTLS** | Frontend TLS termination, backend TLS, optional client certificate verification (kTLS acceleration is *Hardening* — kernel/cipher support varies) |
| **SCRAM-SHA-256 / MD5 auth** | PostgreSQL authentication; prefer SCRAM-SHA-256 for production |
| **Admin console** | `psql`-compatible admin interface: `SHOW POOLS`, `SHOW STATS`, `SHOW SERVERS`, `RELOAD`, and 20+ commands |
| **Prometheus metrics** | `/metrics` endpoint with pool, session, query, and TLS counters + P50/P95/P99 histograms |
| **Live config reload** | SIGHUP reloads pool sizes, TLS certs, timeouts, and server weights without restart |
| **Graceful drain/shutdown** | In-flight queries complete before shutdown; commit-in-doubt sessions are never force-closed |
| **Privilege drop + seccomp** | Supported hardening controls; enable in production where the runtime supports them |

### 🔶 Hardening (works, needs validation in your environment)

These features are implemented and tested but the full failure-mode surface is still being hardened. Enable deliberately with monitoring.

| Feature | What it does |
|---------|-------------|
| **MySQL transaction pooling** | Wire support, prepared-statement session map, GTID capture, XA pinning — parity with PostgreSQL tracked in `docs/MYSQL_PG_PARITY.md` |
| **Automatic read/write splitting** | SQL parser classifies queries; SELECTs go to replicas, writes to primary — with sticky-primary override after writes |
| **Patroni / health probe failover** | Automatic role detection via `pg_is_in_recovery()`, Patroni REST API, MySQL `@@read_only`; dead servers removed from rotation |
| **Session-context preservation (SSV)** | Keeps `SET` parameters, `search_path`, and session GUCs consistent across pooled backends |
| **Transaction tracking** | XID-based commit-in-doubt recovery; read-after-write consistency tokens |
| **NOTIFY / LISTEN proxying** | Transparent proxy for PostgreSQL pub-sub; LISTEN sessions pinned automatically |
| **Query rules (declarative routing)** | INI-based route overrides, blocking, and SQL rewriting — no scripting required |
| **OSC proxying** | Auto-detects `gh-ost` and `pt-online-schema-change`; pins shadow-table DML to primary |
| **Cloud-native auth** | AWS RDS IAM, GCP Cloud SQL IAM, Azure AD/Entra — validate token renewal and provider-outage behaviour per environment |
| **kTLS acceleration** | Kernel TLS offload — kernel and cipher compatibility vary by deployment |

### 🧪 Experimental (opt-in, not production defaults)

These require `experimental_features = true` in config. Use in non-critical environments or staging only.

| Feature | What it does |
|---------|-------------|
| **Horizontal sharding** | Transparent shard-key extraction from SQL; scatter-merge aggregations across shards |
| **Scatter-merge queries** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY`, `ORDER BY`, `LIMIT` merged globally across shards |
| **WAL LSN / GTID replica catch-up** | Token-based replica lag checks for cross-service read-after-write safety |
| **Multi-proxy HA cluster** | 2–3 KEEL nodes gossip configuration and monitor each other; zlib/zstd wire compression for WAN |
| **Result cache** | Query result caching framework — *Aspirational*: infrastructure hooks exist, correctness/invalidation guarantees are not production-supported |

---

## Database & Platform Support

The supported PostgreSQL version list and database maturity notes live in
[`docs/COMPATIBILITY.md`](https://github.com/virtlabs-io/keel/blob/main/docs/COMPATIBILITY.md).
PostgreSQL is the production candidate baseline; MySQL, MariaDB, and Percona
topologies are in the Hardening bucket.

### Cloud databases

| Cloud | Service | Auth method |
|-------|---------|-------------|
| AWS | RDS, Aurora | IAM token (SigV4, auto-rotated) |
| GCP | Cloud SQL | Service account JWT + metadata server |
| Azure | Database for PostgreSQL/MySQL | Managed identity (IMDS + env var fallback) |

No password rotation scripts needed — KEEL handles token generation and caching automatically.

### Linux kernel requirements

| Kernel | I/O backend | Notes |
|--------|-------------|-------|
| 5.6+ | io_uring (default) | Best performance; recommended for production |
| 5.4–5.5 | epoll (auto-fallback) | Works; silent fallback |
| Any (unprivileged) | epoll (auto-fallback) | Docker rootless: `EPERM` on io_uring → epoll |

---

## Security Hardening

KEEL supports defense-in-depth controls:

- **Seccomp BPF filter** — allowlist-only syscall policy (`baseline` or `strict` mode), opt-in by config
- **Privilege drop** — drops to unprivileged user after binding ports, opt-in by config
- **mTLS** — optional client certificate verification with CN/SAN identity extraction
- **LDAP / PAM integration** — enterprise authentication via `ldap_bind()` or `pam_authenticate()`
- **Structured audit log** — NDJSON audit trail for auth, admin, query, and pool events; filterable by event type
- **Cipher enforcement** — configurable TLS 1.2/1.3 cipher suites and minimum version
- **Certificate hot-reload** — `SIGHUP` triggers atomic TLS context swap without dropping connections
- **Known limits surfaced** — review `docs/LIMITATIONS.md` before enabling Hardening or Experimental features

---

## Configuration Example

```ini
[keel]
log_level = info

[worker_group.myapp]
protocol          = postgresql
bind_addr         = 0.0.0.0
bind_port         = 7432
num_workers       = 4
mode              = pool
prepared_statement = virtualize     # transparent PS replay for ORMs
min_pool_size     = 10
max_pool_size     = 100
auth_method       = scram-sha-256

[worker_group.myapp.servers]
primary  = host=db1.internal port=5432 dbname=myapp user=app password=secret role=RW weight=100
```

For the full configuration reference, see [docs/DOCKER.md](https://github.com/virtlabs-io/keel/blob/main/docs/DOCKER.md).

---

## Exposed Ports

| Port | Purpose |
|------|---------|
| `7432` | PostgreSQL proxy |
| `7306` | MySQL proxy |
| `6433` | Admin console (psql-compatible) |
| `9100` | Cluster peer communication |
| `9101` | Prometheus metrics + web UI |

---

## Environment Variables

Common overrides for container deployments (no config file required for simple setups):

```bash
KEEL_LOG_LEVEL=2              # 0=trace … 5=fatal
KEEL_SERVER_HOST=db.internal
KEEL_SERVER_PORT=5432
KEEL_SERVER_USER=app
KEEL_SERVER_PASSWORD=secret
KEEL_SERVER_DATABASE=mydb
KEEL_POOL_MIN=10
KEEL_POOL_MAX=100
KEEL_BIND_PORT=7432
```

---

## Documentation

| Guide | Description |
|-------|-------------|
| [Quick start & Docker](https://github.com/virtlabs-io/keel/blob/main/docs/DOCKER.md) | Docker quick-start, env vars, Compose templates, Kubernetes |
| [Production readiness](https://github.com/virtlabs-io/keel/blob/main/docs/PRODUCTION_READINESS.md) | Feature maturity matrix, failure-mode guide, operator checklist |
| [Admin SQL console](https://github.com/virtlabs-io/keel/blob/main/docs/ADMIN_SQL.md) | `SHOW POOLS`, `SHOW STATS`, `RELOAD`, health endpoints |
| [Prepared statements](https://github.com/virtlabs-io/keel/blob/main/docs/PREPARED_STATEMENTS.md) | Pooling strategies for ORMs (virtualize / pinning / tracking) |
| [Session context](https://github.com/virtlabs-io/keel/blob/main/docs/SESSION_CONTEXT.md) | GUC and SET preservation across pooled backends |
| [Read/write splitting](https://github.com/virtlabs-io/keel/blob/main/docs/QUERY_FLOW.md) | SQL routing, replica selection, sticky-primary override |
| [Cloud auth](https://github.com/virtlabs-io/keel/blob/main/docs/CLOUD_AUTH.md) | AWS RDS IAM, GCP Cloud SQL, Azure Entra token management |
| [Sharding](https://github.com/virtlabs-io/keel/blob/main/docs/SHARDING.md) | Horizontal sharding, scatter-merge, admin virtual tables |
| [Tracing (OpenTelemetry)](https://github.com/virtlabs-io/keel/blob/main/docs/TRACING.md) | W3C traceparent injection, OTLP export, per-query spans |
| [Runtime modes](https://github.com/virtlabs-io/keel/blob/main/docs/RUNTIME_MODES.md) | PROXY / POOL / SMART / FULL tier guide |
| [Hooks (Lua / Python)](https://github.com/virtlabs-io/keel/blob/main/docs/HOOKS.md) | Scripted routing, query rewriting, and pipeline extensions |
| [Operations](https://github.com/virtlabs-io/keel/blob/main/docs/OPERATIONS.md) | SIGHUP reload, graceful drain, signal reference |
| [Cluster compression](https://github.com/virtlabs-io/keel/blob/main/docs/CLUSTER_WIRE_COMPRESSION.md) | Multi-proxy HA, zlib/zstd WAN compression |

---

## Kubernetes

KEEL ships a Helm chart and a Kubernetes operator:

```bash
helm install keel oci://ghcr.io/virtlabs-io/helm/keel \
  --set config.server.host=postgres-primary.default.svc \
  --set config.server.password=secret \
  --set monitoring.enabled=true
```

The operator (`KeelPool` CRD) handles pool lifecycle, config reconciliation, and HPA integration via the `pool_wait_queue_enqueued` metric.

---

## License

AGPL-3.0 — free to use, modify, and distribute. Network use (SaaS) requires source disclosure.  
See [LICENSE](https://github.com/virtlabs-io/keel/blob/main/LICENSE) for details.

---

*KEEL — the missing link between your application and a truly elastic database.*
