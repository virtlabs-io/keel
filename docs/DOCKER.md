# KEEL Docker Guide

Official multi-arch Docker images for KEEL are published to the GitHub Container Registry. This guide covers pulling, running, and deploying KEEL in containers.

---

## Quick Start

```bash
# Pull the latest image (linux/amd64 and linux/arm64)
docker pull ghcr.io/virtlabs/keel:latest

# Run with a config file
docker run --rm \
  -v ./my-keel.ini:/etc/keel/keel.ini:ro \
  -p 7432:7432 -p 6433:6433 -p 9101:9101 \
  ghcr.io/virtlabs/keel:latest

# Run with environment variables only (no config file needed for cluster)
docker run --rm \
  -e KEEL_CLUSTER_NODE_ID=node-1 \
  -e KEEL_CLUSTER_ENABLED=true \
  -e KEEL_CLUSTER_INITIAL_PEERS="node-2:9100,node-3:9100" \
  -v ./keel.ini:/etc/keel/keel.ini:ro \
  -p 7432:7432 -p 6433:6433 \
  ghcr.io/virtlabs/keel:latest
```

---

## Image Tags

| Tag | Description |
|-----|-------------|
| `latest` | Default published image |
| `<tag>` | Immutable tag listed in the container registry or release notes |
| `sha-abc1234` | Specific commit |

Supported platforms: **linux/amd64**, **linux/arm64**

---

## Environment Variable Configuration

The entrypoint script (`docker-entrypoint.sh`) maps `KEEL_*` environment variables to INI file overrides before launching the binary. Environment variables always win over the INI file.

### Core Settings

| Variable | INI Equivalent | Default | Description |
|----------|---------------|---------|-------------|
| `KEEL_CONFIG` | — | `/etc/keel/keel.ini` | Path to main INI config |
| `KEEL_LOG_LEVEL` | `[keel] log_level` | `info` | Log verbosity: trace/debug/info/warn/error |
| `KEEL_SHUTDOWN_TIMEOUT` | `[keel] shutdown_timeout_ms` | `30000` | Graceful shutdown timeout (ms) |

### Admin Console

| Variable | INI Equivalent | Default | Description |
|----------|---------------|---------|-------------|
| `KEEL_ADMIN_ENABLED` | `[admin] enabled` | `true` | Enable admin console |
| `KEEL_ADMIN_PORT` | `[admin] listen_port` | `6433` | Admin listen port |
| `KEEL_ADMIN_ADDR` | `[admin] listen_addr` | `0.0.0.0` | Admin bind address |

### Prometheus Metrics

| Variable | INI Equivalent | Default | Description |
|----------|---------------|---------|-------------|
| `KEEL_PROM_ENABLED` | `[prometheus] enabled` | `true` | Enable `/metrics` endpoint |
| `KEEL_PROM_PORT` | `[prometheus] port` | `9101` | Prometheus listen port |
| `KEEL_PROM_ADDR` | `[prometheus] listen_addr` | `0.0.0.0` | Prometheus bind address |

### Multi-Proxy HA Cluster

| Variable | INI Equivalent | Default | Description |
|----------|---------------|---------|-------------|
| `KEEL_CLUSTER_ENABLED` | `[cluster] enabled` | `false` | Enable cluster mode |
| `KEEL_CLUSTER_NODE_ID` | `[cluster] node_id` | *(auto)* | Unique node identifier (e.g. `keel-1`) |
| `KEEL_CLUSTER_LISTEN_ADDR` | `[cluster] listen_addr` | `0.0.0.0` | Cluster gossip bind address |
| `KEEL_CLUSTER_LISTEN_PORT` | `[cluster] listen_port` | `9100` | Cluster gossip port |
| `KEEL_CLUSTER_INITIAL_PEERS` | `[cluster] initial_peers` | *(none)* | Comma-separated `host:port` list |
| `KEEL_CLUSTER_HB_INTERVAL` | `[cluster] heartbeat_interval_ms` | `1000` | Heartbeat send interval (ms) |
| `KEEL_CLUSTER_HB_TIMEOUT` | `[cluster] heartbeat_timeout_ms` | `5000` | Heartbeat response timeout (ms) |
| `KEEL_CLUSTER_FAIL_THRESHOLD` | `[cluster] failure_threshold` | `3` | Consecutive failures before DOWN |
| `KEEL_CLUSTER_AUTO_SYNC` | `[cluster] auto_sync` | `true` | Auto-sync config on checksum mismatch |

---

## Exposed Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| `7432` | PostgreSQL wire | PostgreSQL proxy listen |
| `7306` | MySQL wire | MySQL proxy listen |
| `6433` | PostgreSQL wire | Admin console |
| `9100` | TCP | Cluster gossip (internal) |
| `9101` | HTTP | Prometheus metrics (`/metrics`) |

---

## Production Compose Templates

Two ready-made Compose files are shipped in `docker/compose/`.

### PostgreSQL HA (3-node KEEL + 3-node PG)

```bash
# Start the stack
docker compose -f docker/compose/pg-ha-official.yml up -d

# Connect via any KEEL node
psql -h 127.0.0.1 -p 7432 -U postgres postgres  # keel-1
psql -h 127.0.0.1 -p 7433 -U postgres postgres  # keel-2
psql -h 127.0.0.1 -p 7434 -U postgres postgres  # keel-3

# Check cluster status
psql -h 127.0.0.1 -p 6433 -U admin keel -c "SHOW CLUSTER;"
psql -h 127.0.0.1 -p 6433 -U admin keel -c "SHOW CLUSTER STATS;"
```

### MySQL HA (3-node KEEL + 3-node MySQL)

```bash
docker compose -f docker/compose/mysql-ha-official.yml up -d

# Connect via any KEEL node
mysql -h 127.0.0.1 -P 7306 -u root -proot test  # keel-1
mysql -h 127.0.0.1 -P 7307 -u root -proot test  # keel-2
```

### Override ports via env vars

```bash
KEEL1_SQL_PORT=5432 KEEL2_SQL_PORT=5433 \
  docker compose -f docker/compose/pg-ha-official.yml up -d
```

### Override the image tag

```bash
KEEL_IMAGE=ghcr.io/virtlabs/keel:1.2.0 \
  docker compose -f docker/compose/pg-ha-official.yml up -d
```

---

## Building the Image

### Single-arch (local testing)

```bash
docker build -t keel:local .
```

### Multi-arch (requires buildx)

```bash
# Set up a buildx builder once
docker buildx create --use --name keel-builder

# Build + push to registry
KEEL_PUBLISH_TAG=1.2.0 ./docker/build-linux.sh publish

# Dry run (no push)
./docker/build-linux.sh publish-dry
```

### Verify tests pass before shipping

```bash
# Build the tester stage — fails the build if any test fails
docker build --target tester -t keel:test .
```

---

## GitHub Actions — Automated Publish

The workflow in `.github/workflows/docker-publish.yml` runs on every `v*.*.*` tag:

1. Sets up QEMU + buildx for cross-compilation.
2. Builds the `tester` stage for `linux/amd64` and fails fast if tests fail.
3. Builds + pushes the `runner` stage for both `linux/amd64` and `linux/arm64`.
4. Attaches SBOM and provenance attestations.
5. Runs a smoke test: pulls the published image and calls `keel --help`.

Trigger manually from the Actions tab with an optional custom tag.

---

## Security Notes

- The runner image runs as a dedicated non-root `keel` user (`UID`/`GID` from `useradd -r`).
- No compiler, headers, or build tools are present in the runner image.
- Runtime dependencies are pinned to the Ubuntu 24.04 LTS versions of `liburing2`, `libyaml-0-2`, `libssl3`, and `lua5.4`.
- The `/run/keel/` directory for env-override INI fragments is writable by the `keel` user only via `tmpfs` mount (or pre-created with correct permissions).
