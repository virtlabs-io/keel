# KEEL Package Install Guide

This is the single install guide for packaged Keel builds. Keep release-specific
asset names in release notes; keep this document focused on the install pattern.

## Pick a Build Variant

| Variant | Use when | Notes |
|---------|----------|-------|
| `core` | Default production candidate deployments. | No embedded Lua or Python interpreters. Smaller dependency and attack surface. |
| `full` | You need Lua or Python hook scripting. | Same engine, plus scripting dependencies. Treat hooks as an explicit operational choice. |

Package artifact names include the variant:

- `keel-core-<version>-Linux-<arch>.tar.gz`
- `keel-full-<version>-Linux-<arch>.tar.gz`
- `keel_<version>_<arch>.deb`
- `keel-<version>-<release>.<arch>.rpm`

## Debian and Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates openssl libyaml-0-2 liburing2
sudo dpkg -i ./keel_<version>_<arch>.deb
sudo systemctl enable --now keel
```

After installation:

```bash
keel --version
sudo systemctl status keel
```

The package installs the binary, systemd unit, default directories, and package
documentation. Review `/etc/keel/keel.ini` before exposing a listener.

## RHEL, Rocky Linux, and Fedora

```bash
sudo dnf install -y ca-certificates openssl libyaml liburing
sudo rpm -Uvh ./keel-<version>-<release>.<arch>.rpm
sudo systemctl enable --now keel
```

After installation:

```bash
keel --version
sudo systemctl status keel
```

## Tarball

Use the tarball when you want a relocatable install or are packaging Keel into a
custom base image:

```bash
tar -xzf keel-core-<version>-Linux-<arch>.tar.gz
sudo install -m 0755 keel/bin/keel /usr/local/bin/keel
sudo install -m 0755 keel/bin/keel-cli /usr/local/bin/keel-cli
sudo mkdir -p /etc/keel /var/lib/keel /var/log/keel
```

Create a service unit appropriate for your environment, then run:

```bash
keel --check-config -c /etc/keel/keel.ini
keel -c /etc/keel/keel.ini
```

## Docker

Container images are published to GitHub Container Registry:

```bash
docker pull ghcr.io/virtlabs/keel:<release-tag>
docker run --rm \
  -v ./keel.ini:/etc/keel/keel.ini:ro \
  -p 7432:7432 \
  ghcr.io/virtlabs/keel:<release-tag>
```

Mutable tags are convenient for development, but production deployments should
pin an immutable release tag or digest. See [DOCKER.md](DOCKER.md) for Compose,
multi-arch, and image-build details.

## Recommended Production Candidate Config

```ini
[keel]
experimental_features = false

[worker_group.main]
protocol = postgres
mode = pool
prepared_statement = virtualize
```

Do not enable hardening or experimental features from a package install guide.
Use [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) to decide when a feature
is mature enough for your environment.

## Verify the Install

```bash
keel --version
keel --check-config -c /etc/keel/keel.ini
```

Then connect through the configured listener:

```bash
PGPASSWORD=<password> psql -h 127.0.0.1 -p 7432 -U <user> <database>
```

## Upgrade Notes

1. Read the relevant `CHANGELOG.md` release section.
2. Check [LIMITATIONS.md](LIMITATIONS.md) for known limitations that affect your
   enabled features.
3. Run `keel --check-config -c /etc/keel/keel.ini` with the new binary.
4. Drain old instances before replacing them when transaction pooling is active.

