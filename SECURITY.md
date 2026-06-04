# Security Policy

## Supported Branches

| Branch class | Supported |
|--------------|-----------|
| Current development branch | ✅ Active |
| Historical development branches | ❌ Not supported unless explicitly listed in release notes |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report security issues privately via GitHub's built-in mechanism:

1. Go to the [Security tab](https://github.com/virtlabs-io/keel/security/advisories/new) of this repository.
2. Click **"Report a vulnerability"**.
3. Fill in the details: affected component, reproduction steps, potential impact, and any suggested fix.

You will receive an acknowledgement within **48 hours** and a detailed response within **7 days**. We aim to release a patch within **30 days** of a confirmed vulnerability.

If you are unable to use GitHub's private reporting, you may email the maintainers directly (see commit metadata for contact details).

## Scope

The following are in scope:

- Authentication bypass or privilege escalation in the proxy layer
- SQL injection or query routing bypass
- TLS downgrade or certificate validation bypass
- Memory safety issues (buffer overflows, use-after-free, etc.)
- Seccomp filter bypass or privilege escalation via the proxy process
- Denial of service via the admin console or client-facing ports

Out of scope:

- Vulnerabilities in third-party dependencies (report directly to upstream)
- Issues requiring physical access or local root on the host
- Security issues in benchmark or example scripts

## Operational Security Model

Keel is a network proxy that terminates client database connections and opens
backend database connections. Treat it as part of the trusted database access
path.

Recommended production posture:

- Bind client listeners only where application clients need access.
- Keep admin and metrics listeners on a trusted management network.
- Use TLS or mTLS for untrusted networks.
- Use SCRAM-SHA-256, mTLS, or an external auth provider instead of `trust`.
- Run as a dedicated non-root user after startup.
- Enable seccomp where the host/container runtime supports it.
- Keep `experimental_features = false` for the production candidate profile.

Security boundaries:

- Keel does not make unsafe SQL safe. Application authorization remains a
  database and application responsibility.
- Keel does not hide backend identity for features that explicitly expose it,
  such as `pg_backend_pid()`.
- Keel's admin interface is operator-facing and must not be exposed publicly.
- Experimental hooks and embedded scripting expand the trusted code base; use
  the `core` build unless scripting is required.

## Security Hardening Features

Keel supports these hardening measures:

- **Binary hardening** — PIE, full RELRO, NX, stack canary (`-fstack-protector-strong`)
- **Seccomp filter** — baseline and strict BPF syscall allowlists, opt-in by config
- **Privilege drop** — drops root after bind and `RLIMIT_NOFILE` setup, opt-in by config
- **TLS enforcement** — minimum TLS 1.2, cipher suite policy, cert hot-reload
- **Audit logging** — structured NDJSON security event log
- **Sanitizer CI** — ASan + UBSan, TSan, and MSan run on every PR

See [docs/TESTING.md](docs/TESTING.md) for the full hardening CI matrix and
[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md#security) for security config keys.
