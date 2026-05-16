# KEEL — Docker Test & Build Environments

This directory contains everything needed to build KEEL on Linux and to spin up
database clusters for integration, smoke, and stress testing.

---

## Directory Structure

```
docker/
├── README.md                        ← you are here
│
├── build-linux.sh                   ← macOS → Linux build/test helper
│
├── Dockerfile.linux                 ← multi-stage production image (devenv, builder, tester, runner)
│
├── compose/                         ← all Docker Compose files
│   ├── pg-streaming.yml             ←   PostgreSQL streaming replication (1P + 2R)
│   ├── pg-patroni.yml               ←   PostgreSQL HA via Patroni + etcd
│   ├── pg-e2e.yml                   ←   Full E2E: PG cluster + KEEL proxy + pgbench
│   ├── pg-dev.yml                   ←   Dev environment (PG + devenv bind-mount)
│   ├── keel.yml                     ←   Standalone KEEL proxy
│   ├── mysql-replication.yml        ←   MySQL 9 streaming replication (1P + 2R)
│   ├── mysql-group.yml              ←   MySQL Group Replication (MGR)
│   ├── mysql-pxc.yml                ←   Percona XtraDB Cluster (Galera multi-master)
│   └── mysql-mariadb.yml            ←   MariaDB Galera cluster
│
├── tests/                           ← test runner scripts
│   ├── test-pg-e2e-full.sh          ←   E2E stress test driver
│   ├── test-pg-streaming.sh         ←   Streaming replication smoke test
│   ├── test-pg-patroni.sh           ←   Patroni HA smoke test
│   ├── test-mysql-replication.sh    ←   MySQL replication smoke test
│   ├── test-mysql-group.sh          ←   MySQL Group Replication smoke test
│   ├── test-mysql-pxc.sh            ←   PXC multi-master smoke test
│   └── test-mysql-mariadb.sh        ←   MariaDB Galera smoke test
│
├── postgres/                        ← PostgreSQL-specific assets
│   ├── config/
│   │   └── postgresql-overrides.conf    ←  parameter overrides applied to all PG nodes
│   └── scripts/
│       ├── init-primary.sh              ←  streaming replication primary init
│       └── init-e2e-primary.sh          ←  E2E test primary init (extra users/db)
│
├── mysql/                           ← MySQL-specific assets
│   ├── config/                      ←  per-node my.cnf overrides
│   │   ├── primary.cnf              ←    MySQL replication primary
│   │   ├── replica1.cnf             ←    MySQL replication replica 1
│   │   ├── replica2.cnf             ←    MySQL replication replica 2
│   │   ├── mgr-node{1,2,3}.cnf     ←    MySQL Group Replication nodes
│   │   ├── mdb-node{1,2,3}.cnf     ←    MariaDB Galera nodes
│   │   └── pxc-node{1,2,3}.cnf     ←    Percona XtraDB Cluster nodes
│   └── scripts/                    ←  init SQL/shell scripts
│       ├── init-primary.sh          ←    create replication user on MySQL primary
│       ├── init-primary.sql         ←    DDL run on primary after start
│       ├── init-replica.sh          ←    configure replication on replicas
│       ├── mgr-bootstrap.sql        ←    bootstrap MGR on node 1
│       ├── mgr-init-node1.sql       ←    MGR node 1 init
│       ├── mgr-init-joiner.sql      ←    MGR joiner nodes init
│       ├── mdb-init-node1.sql       ←    MariaDB bootstrap
│       └── pxc-init-node1.sql       ←    PXC bootstrap
│
├── keel/                            ← KEEL proxy config for test environments
│   ├── keel-e2e.ini                 ←  proxy config used by the E2E compose stack
│   ├── keel-dev.ini                 ←  proxy config used by the dev compose stack
│   └── userlist.txt                 ←  client auth credentials for E2E tests
```

---

## Building KEEL on Linux (from macOS or any Docker host)

KEEL uses `io_uring` on Linux.  Use `build-linux.sh` to compile and test inside
a Docker container — no Linux machine required.

```bash
# Compile only
./docker/build-linux.sh build

# Compile + run all unit tests
./docker/build-linux.sh test

# Build a minimal runtime image  (tag: keel:linux)
./docker/build-linux.sh image

# Launch keel from the runtime image
./docker/build-linux.sh run

# Interactive shell inside the build container
./docker/build-linux.sh shell

# Memory-check tests under Valgrind
./docker/build-linux.sh valgrind

# Remove all images and local build-linux/ artefacts
./docker/build-linux.sh clean
```

**Cross-compile for linux/amd64 from Apple Silicon:**
```bash
KEEL_DOCKER_PLATFORM=linux/amd64 ./docker/build-linux.sh test
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `KEEL_DOCKER_PLATFORM` | _(native)_ | Docker `--platform` value |
| `KEEL_BUILD_IMAGE` | `keel-linux-build` | Builder image tag |
| `KEEL_TEST_IMAGE` | `keel-linux-test` | Tester image tag |
| `KEEL_RUNTIME_IMAGE` | `keel:linux` | Runtime image tag |
| `KEEL_CONFIG_FILE` | `./etc/keel-mix.ini` | Config file mounted into `run` |

---

## PostgreSQL Test Environments

All compose files are in `docker/compose/`.  Run them from the **project root**:

```bash
docker compose -f docker/compose/<file>.yml up -d
docker compose -f docker/compose/<file>.yml down -v
```

### Streaming Replication  (`pg-streaming.yml`)

3-node cluster: 1 primary + 2 hot-standby replicas.  No automatic failover.

| Node | Host port |
|---|---|
| primary | 5432 |
| replica1 | 5433 |
| replica2 | 5434 |

```bash
docker compose -f docker/compose/pg-streaming.yml up -d
# connect
psql -h localhost -p 5432 -U postgres -d postgres
```

### Patroni HA  (`pg-patroni.yml`)

3 Patroni nodes + etcd.  Automatic leader election and failover.

| Node | PG port | Patroni API port |
|---|---|---|
| patroni1 | 5432 | 8008 |
| patroni2 | 5433 | 8009 |
| patroni3 | 5434 | 8010 |

```bash
docker compose -f docker/compose/pg-patroni.yml up -d
# check leader
docker exec -it patroni1 patronictl list
# trigger switchover
docker exec -it patroni1 patronictl switchover
# REST API
curl http://localhost:8008/
```

### End-to-End stress test  (`pg-e2e.yml`)

Full stack: PG cluster + KEEL proxy + pgbench.

```bash
# Quick run (uses defaults: 100 clients, 60 s)
tests/integration/test-pg-e2e-full.sh

# Custom run
PGBENCH_CLIENTS=200 PGBENCH_DURATION=120 tests/integration/test-pg-e2e-full.sh
```

| Component | Setting |
|---|---|
| KEEL listen port | 6432 (exposed as 16432 on host) |
| KEEL pool mode | transaction |
| KEEL backend connections | 50 |
| KEEL frontend connections | 1000 |
| pgbench clients | 100 (default) |
| pgbench duration | 60 s (default) |

Manual inspection:
```bash
docker compose -f docker/compose/pg-e2e.yml logs -f keel     # KEEL logs
docker compose -f docker/compose/pg-e2e.yml logs -f pgbench  # benchmark output
docker compose -f docker/compose/pg-e2e.yml down -v          # teardown
```

### Proxy SSV end-to-end validation

For deterministic PostgreSQL semantic statement replay coverage outside the pgbench stress path, run the dedicated C test suite via CTest:

```bash
# Build first (if you don't have an existing build)
cmake -S . -B build-host -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DKEEL_ENABLE_TESTS=ON
cmake --build build-host -j$(nproc)

# Run only the SSV E2E test
ctest --test-dir build-host -R test_proxy_ssv_e2e --output-on-failure
```

The `test_proxy_ssv_e2e` binary exercises:

- `search_path`-driven prepared statement semantic replay
- transaction-local `search_path` overlays reverted by `COMMIT`
- `set_config(..., true)` local overlays reverted by `ROLLBACK`
- `DateStyle`-driven prepared literal replay
- `TimeZone`-driven prepared literal replay
- temp-table shadowing reverted by `ROLLBACK`
- temp-table shadowing reverted by `DISCARD TEMP`

It is also registered in CTest as `test_proxy_ssv_e2e` and can be pulled into the repo's CI gate with:

```bash
RUN_PROXY_SSV_E2E=1 ./tests/hardening/run_all.sh
```

---

## MySQL / MariaDB Test Environments

### MySQL Streaming Replication  (`mysql-replication.yml`)

1 primary + 2 replicas, GTID-based async replication.

| Node | Host port |
|---|---|
| mysql-primary | 3306 |
| mysql-replica1 | 3307 |
| mysql-replica2 | 3308 |

```bash
tests/integration/test-mysql-replication.sh start
tests/integration/test-mysql-replication.sh test
tests/integration/test-mysql-replication.sh failover   # manual failover test
tests/integration/test-mysql-replication.sh stop
```

### MySQL Group Replication (MGR)  (`mysql-group.yml`)

3-node MGR cluster.

| Node | Host port |
|---|---|
| mgr-node1 | 3306 |
| mgr-node2 | 3307 |
| mgr-node3 | 3308 |

```bash
tests/integration/test-mysql-group.sh start
tests/integration/test-mysql-group.sh test
tests/integration/test-mysql-group.sh stop
```

### Percona XtraDB Cluster (PXC)  (`mysql-pxc.yml`)

3-node Galera multi-master cluster.  Writes accepted on all nodes.

| Node | Host port |
|---|---|
| pxc-node1 | 3306 |
| pxc-node2 | 3307 |
| pxc-node3 | 3308 |

```bash
tests/integration/test-mysql-pxc.sh start
tests/integration/test-mysql-pxc.sh test
tests/integration/test-mysql-pxc.sh failure   # node failure & rejoin test
tests/integration/test-mysql-pxc.sh stop
```

### MariaDB Galera  (`mysql-mariadb.yml`)

3-node MariaDB Galera cluster.

| Node | Host port |
|---|---|
| mdb-node1 | 3306 |
| mdb-node2 | 3307 |
| mdb-node3 | 3308 |

```bash
tests/integration/test-mysql-mariadb.sh start
tests/integration/test-mysql-mariadb.sh test
tests/integration/test-mysql-mariadb.sh stop
```

---

## PostgreSQL Configuration

All PostgreSQL nodes load `docker/postgres/config/postgresql-overrides.conf`
at startup via `include_if_exists`.  Only the parameters listed there are
changed — all other settings keep PostgreSQL defaults.

Edit the file freely and restart the cluster to apply:
```bash
docker compose -f docker/compose/pg-streaming.yml down -v   # -v clears data volumes
docker compose -f docker/compose/pg-streaming.yml up -d
```

Key defaults set for benchmarking:
```conf
max_connections = 200
shared_buffers = 1024MB
wal_level = replica
max_wal_senders = 10
```

---

## KEEL Configuration for Tests

`docker/keel/keel-e2e.ini` — KEEL proxy config used by the E2E compose stack.
`docker/keel/userlist.txt` — client auth credentials.

The proxy listens on **port 6432** inside the container (exposed as 16432 on
the host) and connects to `pg-primary`, `pg-replica1`, `pg-replica2` by
container hostname.

---

## Credentials

All test environments use:

| Role | User | Password |
|---|---|---|
| Superuser | `postgres` | `postgres` |
| Replication | `replicator` | `replicator` |
| MySQL root | `root` | `root` |
| MySQL app user | `keel` | `keel` |
