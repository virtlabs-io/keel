# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| alpha-0.3.0 (current) | ✅ Active |
| < alpha-0.3.0 | ❌ Not supported |

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

## Security Hardening Features

KEEL ships with several hardening measures enabled by default:

- **Binary hardening** — PIE, full RELRO, NX, stack canary (`-fstack-protector-strong`)
- **Seccomp filter** — baseline and strict BPF syscall allowlists
- **Privilege drop** — drops root after bind and `RLIMIT_NOFILE` setup
- **TLS enforcement** — minimum TLS 1.2, cipher suite policy, cert hot-reload
- **Audit logging** — structured NDJSON security event log
- **Sanitizer CI** — ASan + UBSan, TSan, and MSan run on every PR

See [docs/TESTING.md](docs/TESTING.md) for the full hardening CI matrix.
