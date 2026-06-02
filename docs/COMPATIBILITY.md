# KEEL Configuration API Compatibility

This document defines the stability tier of every `keel.ini` configuration key,
the deprecation policy, and the location of migration notes for breaking changes.

---

## Stability Tiers

| Tier | Meaning |
|------|---------|
| **Stable** | Will not be renamed or removed without a documented deprecation window. A deprecated key produces a `WARN`-level log message. |
| **Experimental** | Working but the interface (name, semantics, or defaults) may change while the feature is maturing. Not subject to the deprecation policy. |
| **Internal** | Not intended for end users. May be removed or changed without notice. |

---

## Deprecation Policy

1. A key is first marked `[DEPRECATED]` in the changelog or release notes.
2. While deprecated, it still works but logs a warning on every SIGHUP and startup.
3. The key is removed only after the documented compatibility window has elapsed.
4. Migration guides appear in the `### Migration` section of the relevant `CHANGELOG.md` entry.

---

## `[keel]` — Global Settings

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `log_level` | Stable | `info` | ✅ SIGHUP | `error` `warn` `info` `debug` `trace` |
| `log_file` | Stable | stdout | ✅ SIGHUP | Absolute path; empty = stdout |
| `unix_socket_dir` | Stable | none | ❌ restart | Directory for `.s.PGSQL.<port>` socket |
| `pid_file` | Stable | none | ❌ restart | Written on startup; removed on clean exit |
| `shutdown_timeout_ms` | Stable | `30000` | ✅ SIGHUP | Grace period before forced drain |
| `daemonize` | Stable | `false` | ❌ restart | Double-fork into background |
| `experimental_features` | Stable | `false` | ❌ restart | Required opt-in gate for experimental features |

---

## `[worker_group.<name>]` — Per-Group Pool Settings

### Network / Binding

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `bind_addr` | Stable | `0.0.0.0` | ❌ restart | Listen address |
| `bind_port` | Stable | `6432` | ❌ restart | Listen port |
| `mode` | Stable | `pool` | ❌ restart | Runtime tier: `proxy` `pool` `smart` `full` |
| `num_workers` | Stable | `0` (= CPU count) | ❌ restart | Worker threads |
| `listen_backlog` | Stable | `128` | ❌ restart | TCP listen backlog |
| `protocol` | Stable | `postgres` | ❌ restart | `postgres` or `mysql` |

### Pool Sizing

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `min_pool_size` | Stable | `10` | ✅ SIGHUP | Backend connections opened eagerly per worker |
| `max_pool_size` | Stable | `50` | ✅ SIGHUP | Hard cap per worker |
| `max_conns_per_worker` | Stable | `0` (= `max_pool_size`) | ✅ SIGHUP | Per-worker session cap |
| `pool_max_waiting` | Stable | `100` | ✅ SIGHUP | Max clients queued waiting for a free backend |

### Timeouts

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `idle_timeout_ms` | Stable | `300000` | ✅ SIGHUP | Server-side idle connection timeout |
| `connect_timeout_ms` | Stable | `5000` | ✅ SIGHUP | Backend connect timeout |
| `client_idle_timeout` | Stable | `5m` | ✅ SIGHUP | Client-side idle timeout; accepts `s`/`m`/`h` suffix |
| `client_connect_timeout` | Stable | `10s` | ✅ SIGHUP | Client connect timeout |
| `query_timeout_ms` | Stable | `0` (off) | ✅ SIGHUP | Per-query hard timeout; 0 = disabled |

### Authentication (server → backend)

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `server_user` | Stable | — | ❌ restart | Backend username |
| `server_password` | Stable | — | ✅ SIGHUP | Plain text, `env:VAR`, `file:/path`, `vault:path` |
| `auth_method` | Stable | `scram-sha-256` | ❌ restart | `scram-sha-256` `md5` `trust` `cert` |

### Health Probes & Failover

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `probe` | Stable | none | ✅ SIGHUP | `postgres` `patroni:<port>` `mysql` `tcp` `exec:/path` |
| `probe_interval` | Stable | `5s` | ✅ SIGHUP | How often to probe each backend |
| `probe_timeout` | Stable | `3s` | ✅ SIGHUP | Probe response timeout |
| `probe_retries` | Stable | `3` | ✅ SIGHUP | Consecutive failures before marking down |
| `failover_delay` | Stable | `10s` | ✅ SIGHUP | Delay before re-routing after failover |

### TLS (frontend + backend)

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `tls_mode` | Stable | `disable` | ❌ restart | `disable` `prefer` `require` |
| `tls_cert_file` | Stable | none | ✅ SIGHUP | Server certificate (hot-swap via SIGHUP) |
| `tls_key_file` | Stable | none | ✅ SIGHUP | Server key (hot-swap via SIGHUP) |
| `tls_ca_file` | Stable | none | ✅ SIGHUP | CA bundle for client verification |
| `tls_verify_peer` | Stable | `no` | ✅ SIGHUP | `no` `optional` `require` |
| `tls_ciphers` | Stable | OpenSSL default | ✅ SIGHUP | TLS 1.2 cipher list |
| `tls_ciphersuites` | Stable | OpenSSL default | ✅ SIGHUP | TLS 1.3 ciphersuites |
| `tls_min_version` | Stable | `1.2` | ✅ SIGHUP | `1.2` or `1.3` |
| `tls_auto_generate` | Experimental | `0` | ❌ restart | Auto-generate self-signed certs on startup |
| `tls_auto_dir` | Experimental | `/var/lib/keel/certs` | ❌ restart | Directory for auto-generated certs |
| `ktls_enabled` | Experimental | `auto` | ❌ restart | `off` `on` `auto` — kernel TLS offload |
| `backend_tls` | Stable | `prefer` | ❌ restart | TLS mode to backend: `disable` `prefer` `require` |
| `backend_verify_peer` | Stable | `yes` | ❌ restart | Verify backend certificate |
| `backend_ca_file` | Stable | none | ✅ SIGHUP | CA bundle for backend cert verification |
| `backend_cert_file` | Stable | none | ✅ SIGHUP | Client cert for mTLS to backend |
| `backend_key_file` | Stable | none | ✅ SIGHUP | Client key for mTLS to backend |
| `backend_tls_min_version` | Stable | `1.2` | ✅ SIGHUP | — |

### Session & Pooling Behaviour

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `prepared_statement` | Stable | `virtualize` | ❌ restart | `virtualize` `pinning` `tracking` `anonymous` |
| `transaction_tracking` | Hardening | `off` | ❌ restart | XID probe + read-after-write LSN tokens. Tier per [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md). |
| `fast_network_path` | Experimental | `on` | ❌ restart | MSG_PEEK + splice bypass for DataRow frames |
| `result_cache` | Aspirational | `off` | ❌ restart | Query result caching framework hooks; correctness and invalidation are not production guarantees. Tier per [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md). Disables zero-copy splice bypass when `on`. |
| `scatter_merge` | Experimental | `off` | ❌ restart | Enables scatter-merge routing. When `off` (default), scatter-eligible queries are rejected at dispatch with SQLSTATE `0A000` (`feature_not_supported`). Recursive CTEs over sharded tables are always rejected regardless of this flag. |
| `wal_lsn_capture` | Experimental | `off` | ❌ restart | Enables WAL LSN capture |
| `gtid_capture` | Experimental | `off` | ❌ restart | Enables GTID capture |

### Load Rebalancing

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `rebalance` | Stable | `true` | ✅ SIGHUP | Automatic session rebalancing across workers |
| `rebalance_interval_ms` | Stable | `5000` | ✅ SIGHUP | Check interval |
| `rebalance_threshold_pct` | Stable | `125` | ✅ SIGHUP | Imbalance trigger (125 = 1.25× average) |
| `rebalance_max_per_tick` | Stable | `4` | ✅ SIGHUP | Max sessions migrated per interval |

### io_uring

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `use_buf_rings` | Experimental | `0` | ❌ restart | io_uring buffer rings (Linux ≥ 5.19); reduces copy overhead |

---

## `[worker_group.<name>.servers]` — Backend Server Definitions

Format: `name = host=… port=… dbname=… role=… weight=… [user=…] [password=…]`

| Field | Tier | Required | Notes |
|-------|------|----------|-------|
| `host` | Stable | ✅ | Hostname or IP |
| `port` | Stable | ✅ | Database port |
| `dbname` | Stable | ✅ | Target database name |
| `role` | Stable | ✅ | `RW` `RO` `auto` |
| `weight` | Stable | ✅ | Relative traffic weight |
| `user` | Stable | — | Per-server credential override |
| `password` | Stable | — | Per-server credential override |
| `sslmode` | Stable | — | Per-server TLS mode override |

---

## `[logging]` — Log Backend

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `plugin` | Stable | `stdout` | ❌ restart | `stdout` `file` `syslog` or path to `.so` |
| `plugin_path` | Experimental | none | ❌ restart | External log plugin shared library |
| `log_level` | Stable | `info` | ✅ SIGHUP | Overrides `[keel].log_level` for the log plugin |
| `query_log_mode` | Stable | `none` | ✅ SIGHUP | `none` `all` `read` `write` |
| `log_timestamps` | Stable | `true` | ✅ SIGHUP | — |
| `log_source` | Stable | `true` | ✅ SIGHUP | Include client IP/port |
| `log_dest` | Stable | `true` | ✅ SIGHUP | Include backend IP/port |
| `log_username` | Stable | `true` | ✅ SIGHUP | — |
| `log_database` | Stable | `true` | ✅ SIGHUP | — |
| `log_query_tree` | Experimental | `false` | ✅ SIGHUP | Log parsed AST alongside SQL |
| `max_query_len` | Stable | `0` (no limit) | ✅ SIGHUP | Truncate logged SQL at N bytes |
| `use_colors` | Stable | `true` | ✅ SIGHUP | ANSI colours in stdout output |
| `log_file` | Stable | none | ❌ restart | File path (file plugin) |
| `syslog_ident` | Stable | `keel` | ❌ restart | — |
| `syslog_facility` | Stable | `daemon` | ❌ restart | — |

---

## `[stats]`

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `level` | Stable | `basic` | ✅ SIGHUP | `off` `basic` `extended` `system` `trace` `full` |
| `log_interval_ms` | Stable | `0` (off) | ✅ SIGHUP | Periodic stats dump; 0 = only on `SIGUSR1` |

---

## `[admin]`

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `enabled` | Stable | `true` | ❌ restart | — |
| `listen_addr` | Stable | `127.0.0.1` | ❌ restart | — |
| `listen_port` | Stable | `6433` | ❌ restart | — |
| `users` | Stable | `admin` | ✅ SIGHUP | Comma-separated allowed usernames |
| `password` | Stable | none | ✅ SIGHUP | Empty = trust mode |

---

## `[prometheus]`

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `enabled` | Stable | `true` | ❌ restart | — |
| `listen_addr` | Stable | `0.0.0.0` | ❌ restart | — |
| `port` | Stable | `9101` | ❌ restart | — |
| `path` | Stable | `/metrics` | ❌ restart | — |

---

## `[security]`

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `privilege_drop` | Stable | `0` | ❌ restart | Drop root after bind |
| `run_user` | Stable | — | ❌ restart | `setuid` target username |
| `run_group` | Stable | — | ❌ restart | `setgid` target group |
| `require_privilege_drop` | Stable | `0` | ❌ restart | Abort if drop fails |
| `seccomp` | Stable | `off` | ❌ restart | `off` `baseline` `strict` |
| `require_seccomp` | Stable | `0` | ❌ restart | Abort if seccomp fails |
| `no_new_privs` | Stable | `1` | ❌ restart | `PR_SET_NO_NEW_PRIVS` |

---

## `[cluster]`

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `enabled` | Stable | `false` | ❌ restart | — |
| `node_id` | Stable | `hostname:port` | ❌ restart | Auto-generated if empty |
| `listen_addr` | Stable | `0.0.0.0` | ❌ restart | — |
| `listen_port` | Stable | `9100` | ❌ restart | — |
| `heartbeat_interval_ms` | Stable | `1000` | ✅ SIGHUP | — |
| `heartbeat_timeout_ms` | Stable | `5000` | ✅ SIGHUP | — |
| `failure_threshold` | Stable | `3` | ✅ SIGHUP | Consecutive failures before marking DOWN |
| `auto_sync` | Stable | `true` | ✅ SIGHUP | Detect config mismatches via gossip |
| `initial_peers` | Stable | — | ❌ restart | Comma-separated `host:port` list |

---

## `[query_rule.<N>]` — Declarative Query Rules

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `match` | Stable | — | ✅ SIGHUP | POSIX ERE matched against SQL text |
| `action` | Stable | — | ✅ SIGHUP | `route` `block` `rewrite` |
| `route` | Stable | — | ✅ SIGHUP | `primary` `replica` `any` |
| `rewrite` | Experimental | — | ✅ SIGHUP | Replacement SQL text |

---

## `[throttle.<N>]` — Token-Bucket Rate Limits

| Key | Tier | Default | Reloadable | Notes |
|-----|------|---------|------------|-------|
| `match_user` | Stable | `*` | ✅ SIGHUP | Username glob |
| `match_db` | Stable | `*` | ✅ SIGHUP | Database name glob |
| `rate` | Stable | — | ✅ SIGHUP | Queries per second (token refill rate) |
| `burst` | Stable | `= rate` | ✅ SIGHUP | Burst capacity |

---

## Migration Notes

Release-specific migration history belongs in [CHANGELOG.md](../CHANGELOG.md).

### Planned

The following experimental keys may be promoted to Stable or renamed before the stable API milestone:

- `fast_network_path` — may become `splice_bypass` to better reflect the mechanism
- `tls_auto_generate` / `tls_auto_dir` — may be collapsed into a single `tls_auto = /path/to/dir` key
- `use_buf_rings` — may be folded into `io_uring_mode = standard | buf_rings`

Any rename will follow the deprecation policy above.
