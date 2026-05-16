# KEEL Operations Guide

This document covers day-2 operations for running KEEL in production:
zero-downtime upgrades, configuration reload, graceful drain, and what
happens to in-flight database transactions.

---

## Signal Reference

| Signal | Effect |
|--------|--------|
| `SIGTERM` / `SIGINT` | Begin graceful shutdown drain (see [Graceful Shutdown](#graceful-shutdown)) |
| `SIGHUP` | Hot-reload configuration from disk (see [Config Reload](#live-config-reload-sighup)) |
| `SIGUSR1` | Dump a stats snapshot to the log (useful for debugging) |

---

## Live Config Reload (SIGHUP)

KEEL reloads its `keel.ini` without dropping any existing client connections.

### What reloads atomically

The following `keel.ini` keys can be changed while KEEL is running and take effect immediately after `SIGHUP`:

- Pool sizing: `min_pool_size`, `max_pool_size`
- Timeouts: `idle_timeout_ms`, `client_connect_timeout`, `query_timeout_ms`
- Routing weights on existing server entries
- Server health status overrides
- TLS certificates (hot cert swap with atomic `SSL_CTX` pointer exchange)
- Query rules (`[query_rule.*]`)
- Shard rules (`[shard_rule.*]`)
- Query throttle rules (`[throttle.*]`)
- Audit log event filter list
- Log level and log format

### What requires a full restart

- Adding or removing `[worker_group.*]` sections
- Changing `bind_addr` / `bind_port`
- Enabling or disabling `io_uring` / seccomp mode
- Changing the number of workers (`num_workers`)
- Changing `[prometheus]` listen address/port

### How to reload

```bash
# Find the PID (or use the pidfile)
KEEL_PID=$(cat /var/run/keel.pid)

# Edit keel.ini, then:
kill -HUP "$KEEL_PID"

# Or with systemd:
systemctl reload keel
```

KEEL logs a confirmation line:

```
engine: SIGHUP received, reloading configuration
```

Any invalid INI keys are logged as warnings; the change is rejected for
that key while all valid changes take effect.

---

## Graceful Shutdown

When KEEL receives `SIGTERM` or `SIGINT` it enters a **drain phase** before
exiting:

1. **Stop accepting new client connections** — the listen socket is closed immediately; new TCP SYNs are rejected at the kernel level.
2. **Wait for in-flight sessions to complete** — sessions that are mid-query continue to run normally; their database connections are not killed.
3. **Drain timeout** — if sessions have not finished within `shutdown_timeout_ms` (default 30 000 ms = 30 s), they are forcibly closed.
4. **Pool teardown** — all backend connections are closed cleanly with a `Terminate` message (PostgreSQL) or a `COM_QUIT` (MySQL).
5. **Process exits** — exit code 0 on clean drain, 1 if the drain timed out.

### Configuring the drain timeout

```ini
[keel]
# Time in milliseconds to wait for in-flight sessions to finish
# before forcing them closed during shutdown.
# Default: 30000 (30 seconds)
shutdown_timeout_ms = 60000
```

### In-flight transactions during shutdown

| Client state at shutdown | What happens |
|--------------------------|--------------|
| Idle (no active query) | Connection closed immediately |
| Executing a query | Query is allowed to complete; then connection is closed |
| Inside a `BEGIN`/`COMMIT` block | Transaction is allowed to continue up to `shutdown_timeout_ms`; if still open at timeout, KEEL sends `ROLLBACK` then closes |
| Waiting in the pool queue | Wait is aborted; client receives an error |

> **Important:** For safety, configure your load balancer or Kubernetes service
> to stop sending new connections to a draining pod *before* you send SIGTERM.
> This is handled automatically by `preStop` hooks in the Helm chart (see below).

---

## Zero-Downtime Upgrade

### Kubernetes / Helm (rolling update)

The Helm chart defaults to a `RollingUpdate` deployment strategy with
`maxSurge: 1` and `maxUnavailable: 0`, so upgrades are zero-downtime by default.

```bash
# Upgrade to a new version
helm upgrade keel helm/keel/ \
  --set image.tag=alpha-0.4.0 \
  --atomic \
  --timeout 5m
```

The pod spec includes a `preStop` hook that sends `SIGTERM` and waits for
the drain period before the container terminates, giving in-flight connections
time to finish:

```yaml
# (already set in helm/keel/templates/deployment.yaml)
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "sleep 5"]
```

Combined with the readiness probe, Kubernetes only routes traffic to the new
pod once it is fully ready, and only stops routing to the old pod after its
drain completes.

**Checklist:**
1. Verify the new image passes CI.
2. Run `helm upgrade --dry-run` to preview the diff.
3. Run `helm upgrade --atomic` — rolls back automatically on failure.
4. Monitor the rollout: `kubectl rollout status deployment/keel`.
5. Watch for errors in `kubectl logs -f deployment/keel`.

### systemd (binary replacement)

```bash
# 1. Download the new binary to a staging path
curl -Lo /usr/local/bin/keel.new \
  https://github.com/virtlabs-io/dbcp-keel/releases/download/alpha-0.4.0/keel-linux-amd64

# 2. Verify the SHA256 and GPG signature (see docs/RELEASE_SIGNING.md)
sha256sum -c SHA256SUMS

# 3. Atomically replace the binary (atomic rename, never a partial write)
mv /usr/local/bin/keel.new /usr/local/bin/keel

# 4. Issue a graceful restart via systemd
#    - systemd sends SIGTERM, waits for TimeoutStopSec, then starts the new binary
#    - set TimeoutStopSec >= shutdown_timeout_ms in your unit file
systemctl restart keel

# 5. Verify the new version is running
keel --version
systemctl status keel
```

### High-Availability (cluster mode, ≥3 nodes)

In cluster mode, KEEL nodes form a gossip ring with a leader-elected
coordinator. Rolling upgrades work as follows:

1. Restart nodes one at a time, always keeping a quorum healthy.
2. For a 3-node cluster: restart node 3 → wait for it to rejoin → restart node 2 → wait → restart node 1 (leader last).
3. The leader election re-triggers automatically when the current leader goes down; a new leader is elected in < 1 s under normal network conditions.

```bash
# On each node, one at a time:
systemctl restart keel
# Wait for the admin console to report the node as healthy:
psql -h keel-nodeN -p 7433 -c "SHOW KEEL STATUS;"
```

---

## Connection Pool Management

### Pre-warming the pool after restart

KEEL warms the backend connection pool asynchronously at startup.
With `min_pool_size = 10`, it will open 10 connections per worker in the
background before serving clients. No traffic needs to be held.

```ini
[worker_group.myapp]
min_pool_size = 10     # open this many connections eagerly at startup
max_pool_size = 100    # hard cap per worker
```

### Draining a specific backend for maintenance

To remove a backend from the pool without restarting KEEL, use the admin
console:

```sql
-- Connect to the KEEL admin port (default 7433)
psql -h 127.0.0.1 -p 7433 -U admin keeldb

-- Mark a backend as down (no new connections; existing ones drain normally)
UPDATE keel_backends SET health = 'down' WHERE name = 'replica2';

-- Verify routing shifted away from it
SELECT * FROM keel_backends;

-- After maintenance: bring it back
UPDATE keel_backends SET health = 'up' WHERE name = 'replica2';
```

### Forcing idle connection eviction

If you need to recycle stale connections (e.g., after a backend restart):

```bash
# Via admin console
psql -h 127.0.0.1 -p 7433 -c "SELECT keel_evict_idle_connections();"

# Or via SIGHUP if you reduced max_connection_age_seconds in keel.ini:
kill -HUP "$(cat /var/run/keel.pid)"
```

---

## Monitoring During Upgrades

Watch these metrics in Grafana or via the `/metrics` endpoint during a rolling upgrade:

```promql
# Sessions waiting for a connection (should stay near 0)
keel_pool_waiting_sessions

# Pool utilization (expect a brief spike as old pods drain)
keel_pool_utilization_ratio

# Query latency P99 (should not increase significantly)
histogram_quantile(0.99, rate(keel_query_latency_ns_bucket[1m])) / 1e6

# Error rate from the router (should be 0)
rate(keel_router_failover_routes[1m])
```

---

## Log-Level Changes Without Restart

The log level can be changed at runtime via SIGHUP. Edit `keel.ini`:

```ini
[keel]
log_level = 4   # 0=OFF 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=TRACE
```

Then:

```bash
kill -HUP "$(cat /var/run/keel.pid)"
```

---

## Troubleshooting Common Issues

### Pool exhaustion (all connections in use)

```bash
# Check pool stats
psql -h 127.0.0.1 -p 7433 -c "SELECT * FROM keel_pool_stats;"

# Temporary relief: increase max_pool_size via SIGHUP
# (edit keel.ini, then reload)
```

### Backend becomes unreachable during operation

KEEL detects backend failures via health probes (`probe_interval`). When a
backend fails, it is marked down and traffic is redistributed to healthy
backends. No restart is needed.

Manually mark it down/up via the admin console (see [above](#draining-a-specific-backend-for-maintenance)).

### TLS certificate expiry

1. Replace the cert/key files on disk.
2. Send `SIGHUP` — KEEL will atomically hot-swap the TLS context.
3. Verify: check `keel_tls_cert_reloads` increases by 1 in `/metrics`.

Existing TLS sessions continue using the old certificate until they close
naturally. New handshakes use the new certificate immediately.

### High P99 latency after upgrade

1. Check `keel_pool_waiting_sessions` — if elevated, the pool is undersized.
2. Check `keel_backend_latency_ns` histogram — if elevated, the database is slow.
3. Check `keel_connect_latency_ns` — if elevated, pool warmup is not complete yet (wait 30 s after restart).
4. Enable `SIGUSR1` stats dump: `kill -USR1 $(cat /var/run/keel.pid)` and inspect the log.

---

## Scatter-Merge Monitoring and Troubleshooting

This section covers operational aspects of KEEL's scatter-merge aggregation engine.
For architecture and algorithm details, see [SCATTER_MERGE.md](SCATTER_MERGE.md).

### Prerequisites

Scatter-merge is automatically active for any query that:

1. Targets a table with a `[shard_rule.*]` entry in `keel.ini`.
2. Has no shard-key predicate (so the routing plan is SCATTER, not SINGLE).
3. Contains aggregate functions, GROUP BY, HAVING, ORDER BY, or LIMIT.

No additional configuration is required.

### Prometheus Metrics

```promql
# P99 scatter-merge end-to-end latency (fan-out → last byte to client)
histogram_quantile(0.99,
  rate(keel_router_scatter_merge_duration_seconds_bucket[5m])
)

# P95 scatter-merge latency
histogram_quantile(0.95,
  rate(keel_router_scatter_merge_duration_seconds_bucket[5m])
)

# Scatter-merge operations per second
rate(keel_router_scatter_merge_duration_seconds_count[1m])

# Average scatter-merge latency
rate(keel_router_scatter_merge_duration_seconds_sum[1m])
  /
rate(keel_router_scatter_merge_duration_seconds_count[1m])

# Scatter fan-out hits (total scatter queries dispatched)
keel_router_shard_scatter_hits_total

# Scatter failures (at least one shard error)
keel_router_shard_scatter_failed_total
```

**Histogram bucket boundaries:** 1 ms, 5 ms, 10 ms, 25 ms, 50 ms, 100 ms,
250 ms, 500 ms, 1 s, 2.5 s, +Inf.

### Grafana Panel (quick-add JSON)

```json
{
  "title": "Scatter-Merge Latency",
  "type": "graph",
  "targets": [
    {
      "expr": "histogram_quantile(0.99, rate(keel_router_scatter_merge_duration_seconds_bucket[5m]))",
      "legendFormat": "P99"
    },
    {
      "expr": "histogram_quantile(0.95, rate(keel_router_scatter_merge_duration_seconds_bucket[5m]))",
      "legendFormat": "P95"
    },
    {
      "expr": "histogram_quantile(0.50, rate(keel_router_scatter_merge_duration_seconds_bucket[5m]))",
      "legendFormat": "P50"
    }
  ]
}
```

The pre-built dashboard at `etc/grafana/keel-dashboard.json` already includes
this panel.

### Alerting Rules (example)

```yaml
# Prometheus alerting rules
groups:
  - name: keel_scatter
    rules:
      - alert: ScatterMergeHighLatency
        expr: |
          histogram_quantile(0.99,
            rate(keel_router_scatter_merge_duration_seconds_bucket[5m])
          ) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Scatter-merge P99 > 500 ms"

      - alert: ScatterMergeHighFailureRate
        expr: |
          rate(keel_router_shard_scatter_failed_total[5m])
            /
          rate(keel_router_shard_scatter_hits_total[5m]) > 0.05
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "More than 5% of scatter queries failing"
```

### Diagnosing Slow Scatter Queries

**Step 1 — Verify the routing plan**

```sql
-- Admin port (default 7433)
EXPLAIN SHARD PLAN FOR 'SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status';
```

Confirm `kind = SCATTER` and `agg_type` / `merge_strategy` match expectations.

**Step 2 — Check per-shard latency**

Each scatter query opens a new TCP connection per shard. Elevated P99 usually
means:

| Symptom | Likely cause |
|---------|-------------|
| P99 > 200 ms, all shard connections | Network RTT or DNS resolution latency |
| P99 spikes on one shard | That shard's PostgreSQL is slow or under load |
| Timeout errors (30 s) | Shard is unreachable or blocked on a long query |
| Connect errors | `shard_backend.N` host/port misconfigured; check KEEL logs |

**Step 3 — Check KEEL logs**

```bash
# Scatter shard errors appear at WARN level
journalctl -u keel -n 200 | grep -i "scatter\|shard"
```

Example log entries:
```
WARN  scatter: shard 1 returned error: connection refused (host=shard1.local:5432)
WARN  scatter: shard 2 read timeout after 30000ms
```

**Step 4 — Benchmark in isolation**

```bash
# Run the included scatter pgbench script
cd /path/to/keel
KEEL_SCATTER_HOST=127.0.0.1 KEEL_SCATTER_PORT=5432 \
DIRECT_PG_HOST=shard0.local DIRECT_PG_PORT=5432 \
BENCH_DB=mydb BENCH_DURATION=60 BENCH_CLIENTS=8 BENCH_THREADS=4 \
bash bench/run_scatter_pgbench.sh

# Measure connection overhead separately
KEEL_SCATTER_HOST=127.0.0.1 KEEL_SCATTER_PORT=5432 \
DIRECT_PG_HOST=shard0.local DIRECT_PG_PORT=5432 \
bash bench/measure_scatter_conn_overhead.sh
```

### Troubleshooting: Wrong LIMIT Results with GROUP BY

**Symptom:** A query like `SELECT col, SUM(x) FROM t GROUP BY col ORDER BY 2 DESC LIMIT 5`
returns fewer rows than expected, or rows with unexpectedly low aggregates.

**Cause:** An older KEEL version may have forwarded `LIMIT 5` to each shard,
causing per-shard GROUP truncation. Groups with high global aggregates but
low per-shard counts were silently dropped.

**Diagnosis:**

```sql
-- Admin port
EXPLAIN SHARD PLAN FOR 'SELECT col, SUM(x) FROM t GROUP BY col ORDER BY 2 DESC LIMIT 5';
```

If `merge_strategy` shows `GROUP+SORT+LIMIT` (not just `GROUP+LIMIT`), the fix
is in place: LIMIT is applied globally post-merge.

**Fix:** Upgrade to KEEL ≥ the commit that introduced `sc_strip_limit_offset()`
(Phase 4 / commit `051691e`). No config changes are required.

### Scatter Write Failures and 2PC Recovery

If a scatter write fails after some shards have already prepared:

1. KEEL issues `ROLLBACK ALL` across all prepared shards automatically.
2. The client receives a PostgreSQL error.
3. If the KEEL process crashes between PREPARE and COMMIT, in-doubt transactions
   remain on the prepared shards.

**Manual recovery:**

```sql
-- Connect directly to each shard PostgreSQL
SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'keel_%';

-- Rollback if the global transaction did not commit
ROLLBACK PREPARED 'keel_ab12cd34_1_s0';
ROLLBACK PREPARED 'keel_ab12cd34_1_s1';
```

The GID format `keel_<session>_<seq>_s<shard>` identifies the owning KEEL session
and sequence number, which can be correlated with KEEL logs to determine whether
the transaction committed globally.

---

## Query Result Cache

### Enabling the Cache

The query result cache is enabled per `[worker_group.*]` section in `keel.ini`:

```ini
[worker_group.primary]
result_cache = on          # default: off
```

A restart is required when changing `result_cache`.  The worker creates a
per-worker LRU cache instance on startup; each worker is fully independent.

### What Gets Cached

A `SELECT` query result is cached when **all** of the following hold:

- The session is in autocommit mode (outside an explicit `BEGIN`/transaction block)
- The query has no write or DDL effects (`KEEL_QE_WRITE` / `KEEL_QE_DDL` not set)
- `keel_query_cache_is_cacheable()` returns true — rejects: `FOR UPDATE`,
  `FOR SHARE`, `UNION`, `WITH` (CTE), volatile functions (`NOW()`, `RANDOM()`,
  `UUID()`, etc.)

The default TTL is **3 000 ms**.  There is no per-rule TTL override via configuration
at this time.

### Splice Bypass Interaction

When `result_cache = on`, the zero-copy DataRow splice bypass is **disabled** and
all backend responses are copied into userspace for accumulation.  If low latency
on large result sets is the priority and caching is not needed, keep `result_cache = off`.

### Monitoring

Cache statistics are tracked per worker.  Retrieve them programmatically via:

```c
keel_query_cache_stats_t s;
keel_query_cache_stats(worker->query_cache, &s);
/* s.hits, s.misses, s.evictions, s.expirations,
   s.entries_count, s.memory_used_bytes, s.memory_max_bytes */
```

An admin command (`SHOW CACHE STATS`) and corresponding Prometheus metrics are
planned but not yet exposed in this release.

### Manual Flush

To flush all cache entries for a given worker's cache programmatically:

```c
keel_query_cache_flush(worker->query_cache);
```

A `FLUSH QUERY CACHE` admin-socket command is planned for a future release.

### Known Limitations

| # | Description |
|---|-------------|
| 1 | Cache is per-worker — the same backend result may be fetched up to N times across N workers before each worker's cache warms up. |
| 2 | No per-rule TTL — all queries share the same default TTL (3 000 ms). |
| 3 | `FOR SHARE` detection uses the PostgreSQL keyword; MySQL `LOCK IN SHARE MODE` is not yet detected and may be cached. |
