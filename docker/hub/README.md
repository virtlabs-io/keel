# KEEL — Production Docker Images

Official Docker Hub images for [KEEL](https://hub.docker.com/r/vlbsio/keel), a
high-performance database connection pooler and proxy.

## Available tags

| Tag | Base image | Size |
|-----|-----------|------|
| `latest`, `X.Y.Z`, `X.Y`, `X`, `debian`, `X.Y.Z-debian` | `debian:bookworm-slim` | ~120 MB |
| `ubuntu`, `X.Y.Z-ubuntu`, `X.Y-ubuntu` | `ubuntu:24.04` | ~140 MB |
| `alpine`, `X.Y.Z-alpine`, `X.Y-alpine` | `alpine:3.20` | ~40 MB |

`latest` always tracks the most recent stable release on the **Debian** base.

## Quick start

```bash
# Debian (default / recommended)
docker pull vlbsio/keel

# Alpine (smallest)
docker pull vlbsio/keel:alpine

# Run with your own config
docker run -v /path/to/keel.ini:/etc/keel/keel.ini -p 7432:7432 vlbsio/keel
```

## Environment variable overrides

All `KEEL_*` variables are translated to INI config at container start:

```bash
docker run \
  -e KEEL_LOG_LEVEL=2 \
  -e KEEL_CLUSTER_ENABLED=false \
  -e KEEL_CLUSTER_NODE_ID=keel-1 \
  -p 7432:7432 \
  vlbsio/keel
```

See [docker/docker-entrypoint.sh](../docker-entrypoint.sh) for the full list.

## Exposed ports

| Port | Purpose |
|------|---------|
| `7432` | PostgreSQL proxy |
| `7306` | MySQL proxy |
| `6433` | Admin console |
| `9100` | Cluster peer communication |
| `9101` | Prometheus metrics |

## Volumes

| Path | Purpose |
|------|---------|
| `/etc/keel/keel.ini` | Main configuration file |
| `/var/log/keel/` | Log output |

## Build locally

```bash
# Debian
docker build -f docker/hub/Dockerfile.debian -t vlbsio/keel:debian .

# Ubuntu
docker build -f docker/hub/Dockerfile.ubuntu -t vlbsio/keel:ubuntu .

# Alpine
docker build -f docker/hub/Dockerfile.alpine -t vlbsio/keel:alpine .
```

## CI / CD

Production images are built and pushed to Docker Hub automatically via
[`.github/workflows/hub-publish.yml`](../../.github/workflows/hub-publish.yml)
on every `vX.Y.Z` git tag.

### Required repository secrets

| Secret | Description |
|--------|-------------|
| `DOCKERHUB_USERNAME` | Docker Hub login (e.g. `vlbsio`) |
| `DOCKERHUB_TOKEN` | Docker Hub access token (read/write/delete scope) |

### Manual trigger

The workflow can also be triggered manually from the GitHub Actions UI, with an
optional extra tag to apply to the Debian image.
