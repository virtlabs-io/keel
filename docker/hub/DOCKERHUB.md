<img src="https://raw.githubusercontent.com/virtlabs-io/keel/main/keel.png" alt="KEEL — More than a pooler. The missing link between your application and a truly elastic database" width="300">

# KEEL — Database Connection Pooler & Proxy

**KEEL** is a high-performance connection pooler and intelligent proxy for **PostgreSQL** and **MySQL**. It sits between your application and your databases, multiplexing thousands of app connections over a small pool of real backend connections — reducing database load, improving tail latency, and adding routing intelligence with zero application changes.

> **v0.3.0** · [GitHub](https://github.com/virtlabs-io/keel) · [Documentation](https://github.com/virtlabs-io/keel/tree/main/docs) · [Changelog](https://github.com/virtlabs-io/keel/blob/main/CHANGELOG.md) · [License: AGPL-3.0](https://github.com/virtlabs-io/keel/blob/main/LICENSE)
 
---

## Why KEEL?

| Problem | How KEEL solves it |
|---------|-------------------|
| Too many connections overloading the database | Transaction pooling multiplexes hundreds of app threads over a small backend pool |
| Read replicas sitting idle while the primary is overloaded | Automatic read/write splitting routes SELECTs to replicas and writes to primary |
| ORM prepared statements breaking pooling (Hibernate, pgx, GORM, SQLAlchemy) | Transparent prepared-statement virtualization — no ORM changes required |
| Session state (SET, search_path, GUCs) lost when connections are reused | Session-context preservation keeps per-session state consistent across backend reassignment |
| Patroni failover causing application errors | Automatic role detection and routing updates on primary/replica changes |
| Cloud IAM token rotation for RDS/Cloud SQL/Azure | Built-in AWS, GCP, and Azure token management — no rotation scripts needed |
| Compliance requirements for audit logging and TLS enforcement | Structured audit log, mTLS, LDAP/PAM integration, privilege drop, seccomp filter |

---

## Available Images

| Tag | Base | Size | Use when |
|-----|------|------|---------|
| `latest`, `X.Y.Z`, `debian` | Debian Trixie Slim | ~120 MB | Default — best library compatibility |
| `ubuntu`, `X.Y.Z-ubuntu` | Ubuntu 24.04 LTS | ~140 MB | Ubuntu-centric environments |
| `alpine`, `X.Y.Z-alpine` | Alpine 3.20 | ~40 MB | Minimal footprint, musl libc |

All images are multi-arch: **linux/amd64** and **linux/arm64**.

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
  vlbsio/keel
```

Then connect your app to `localhost:7432` exactly as you would connect to PostgreSQL directly.

### With a config file

```bash
docker run -d \
  -v /path/to/keel.ini:/etc/keel/keel.ini \
  -p 7432:7432 \
  vlbsio/keel
```

### Docker Compose

```yaml
services:
  keel:
    image: vlbsio/keel
    ports:
      - "7432:7432"   # PostgreSQL proxy
      - "9101:9101"   # Prometheus metrics
    volumes:
      - ./keel.ini:/etc/keel/keel.ini
    restart: unless-stopped
```

---

## Feature Readiness

KEEL has a broad feature surface. Features are labelled by production maturity:

### ✅ Stable (production-ready)

These are safe to use in production today with default settings.

| Feature | What it does |
|---------|-------------|
| **Transaction pooling** | Connections returned to the pool after each transaction; hundreds of app connections share a small backend pool |
| **Prepared statement virtualization** | Named prepared statements transparently replayed on any backend — works with Hibernate, pgx, GORM, SQLAlchemy, Prisma |
| **Full TLS + mTLS** | Frontend TLS termination, backend TLS, optional client certificate verification, kernel TLS acceleration |
| **SCRAM-SHA-256 / MD5 auth** | Full PostgreSQL authentication including `caching_sha2_password` for MySQL |
| **Admin console** | `psql`-compatible admin interface: `SHOW POOLS`, `SHOW STATS`, `SHOW SERVERS`, `RELOAD`, and 20+ commands |
| **Prometheus metrics** | `/metrics` endpoint with pool, session, query, and TLS counters + P50/P95/P99 histograms |
| **Live config reload** | SIGHUP reloads pool sizes, TLS certs, timeouts, and server weights without restart |
| **Graceful drain/shutdown** | In-flight queries complete before shutdown; commit-in-doubt sessions are never force-closed |
| **Privilege drop + seccomp** | Drops root after bind; optional seccomp syscall filter reduces attack surface |

### 🔶 Hardening (works, needs validation in your environment)

These features are implemented and tested but the full failure-mode surface is still being hardened. Enable deliberately with monitoring.

| Feature | What it does |
|---------|-------------|
| **Automatic read/write splitting** | SQL parser classifies queries; SELECTs go to replicas, writes to primary — with sticky-primary override after writes |
| **Patroni / health probe failover** | Automatic role detection via `pg_is_in_recovery()`, Patroni REST API, MySQL `@@read_only`; dead servers removed from rotation |
| **Session-context preservation (SSV)** | Keeps `SET` parameters, `search_path`, and session GUCs consistent across pooled backends |
| **Transaction tracking** | XID-based commit-in-doubt recovery; read-after-write consistency tokens |
| **NOTIFY / LISTEN proxying** | Transparent proxy for PostgreSQL pub-sub; LISTEN sessions pinned automatically |
| **Query rules (declarative routing)** | INI-based route overrides, blocking, and SQL rewriting — no scripting required |
| **OSC proxying** | Auto-detects `gh-ost` and `pt-online-schema-change`; pins shadow-table DML to primary |

### 🧪 Experimental (opt-in, not production defaults)

These require `experimental_features = true` in config. Use in non-critical environments or staging only.

| Feature | What it does |
|---------|-------------|
| **Horizontal sharding** | Transparent shard-key extraction from SQL; scatter-merge aggregations across shards |
| **Scatter-merge queries** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY`, `ORDER BY`, `LIMIT` merged globally across shards |
| **WAL LSN / GTID replica catch-up** | Token-based replica lag checks for cross-service read-after-write safety |
| **Multi-proxy HA cluster** | 2–3 KEEL nodes gossip configuration and monitor each other; zlib/zstd wire compression for WAN |
| **Result cache** | Query result caching framework (infrastructure ready, correctness guarantees in progress) |

---

## Database & Platform Support

### Supported databases

| Database | Versions | Status |
|----------|---------|--------|
| PostgreSQL | 14, 15, 16, 17 | ✅ Tested in CI |
| MySQL | 8.0, 8.4, 9.x | ✅ Tested in CI |
| MariaDB | 10.11, 11.x | ✅ Tested in CI |
| Percona XtraDB Cluster | 8.0 | ✅ Validated |

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

KEEL is built with defense-in-depth:

- **Seccomp BPF filter** — allowlist-only syscall policy (`baseline` or `strict` mode)
- **Privilege drop** — drops to unprivileged user after binding ports
- **mTLS** — optional client certificate verification with CN/SAN identity extraction
- **LDAP / PAM integration** — enterprise authentication via `ldap_bind()` or `pam_authenticate()`
- **Structured audit log** — NDJSON audit trail for auth, admin, query, and pool events; filterable by event type
- **Cipher enforcement** — configurable TLS 1.2/1.3 cipher suites and minimum version
- **Certificate hot-reload** — `SIGHUP` triggers atomic TLS context swap without dropping connections
- **No COPY from untrusted input** — SQL parser blocks dangerous patterns at the proxy layer

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
mode              = pool            # pool | smart | proxy | full
prepared_statement = virtualize     # transparent PS replay for ORMs
min_pool_size     = 10
max_pool_size     = 100

[worker_group.myapp.servers]
primary  = host=db1.internal port=5432 dbname=myapp user=app password=secret role=RW weight=100
replica1 = host=db2.internal port=5432 dbname=myapp user=app password=secret role=RO weight=100
replica2 = host=db3.internal port=5432 dbname=myapp user=app password=secret role=RO weight=100

[worker_group.myapp.probe]
type     = patroni
endpoint = http://db1.internal:8008
interval = 3s
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
