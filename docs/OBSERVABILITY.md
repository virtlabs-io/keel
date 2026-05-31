# Observability — operator quick-start

KEEL exposes operational telemetry on three surfaces:

| Surface | What it is | Always available |
|---|---|---|
| Admin SQL / HTTP / JSON | Interactive inspection (`SHOW STATS`, `SHOW POOLS`, `GET /api/...`). See [docs/ADMIN_SQL.md](ADMIN_SQL.md). | Yes |
| Admin Prometheus endpoint | Scrape `http://<prom_addr>:<prom_port>/metrics`. Emits the full per-worker metric set. | Yes |
| **OTLP/HTTP push exporter** | Background aggregator that snapshots, encodes, and pushes the inventory to any OTLP-compatible collector. | Only when built with `-DKEEL_ENABLE_OTLP=ON` |

This document focuses on the OTLP exporter. For the metric inventory
itself see [docs/METRICS_REFERENCE.md](METRICS_REFERENCE.md).

## 1. When to use OTLP

Prefer the OTLP exporter when:

- You already terminate telemetry at an OpenTelemetry Collector,
  Grafana Agent, Datadog Agent, or any other OTLP/HTTP-compatible
  sidecar.
- You want push delivery rather than scrape (e.g. KEEL runs inside a
  short-lived pod that may be killed before the next scrape interval).
- You want a single transport for metrics across your fleet,
  regardless of whether each instance is reachable from Prometheus.

The Prometheus scrape endpoint stays available alongside OTLP. They
read the same in-process metric registry — there is no separate
counter set — so dashboards built on either surface stay coherent.

## 2. Build and config

Enable at build time:

```bash
cmake -S . -B build-otlp -DKEEL_ENABLE_OTLP=ON
cmake --build build-otlp -j$(nproc)
```

Configure in your `keel.ini`:

```ini
[observability]
otlp_enabled = 1
otlp_endpoint_url = http://otel-collector:4318/v1/metrics
otlp_bearer_token =                       ; optional
otlp_timeout_ms = 5000
otlp_interval_ms = 5000
otlp_max_retries = 2
otlp_queue_capacity = 4
otlp_encode_buf_bytes = 65536
```

All keys default to safe values; see
[etc/keel.ini.example](../etc/keel.ini.example) for the annotated
template. When KEEL is built without OTLP support the section is
ignored — leaving it in your config is harmless.

Verify the configuration without starting workers:

```bash
./build-otlp/src/main/keel --config etc/keel.ini --check-config
```

The output prints the configured endpoint, interval, and queue capacity.

## 3. What KEEL exports

KEEL exports the full snapshot inventory documented in
[docs/METRICS_REFERENCE.md](METRICS_REFERENCE.md) §5 — currently 52
metrics covering sessions, queries, errors, bytes, pool, reactor,
multiplexing safety, reason-coded backend close, commit-in-doubt
resolution, and process metadata.

Resource attributes attached to every export:

- `service.name = keel`
- `service.version = <build version string>`
- `telemetry.sdk.name = keel`
- `telemetry.sdk.language = c`

All counters are exported with **cumulative temporality** (OTel
`AGGREGATION_TEMPORALITY_CUMULATIVE`). Gauges are exported as the
current snapshot value. Histograms are not exported by the current OTLP path
(reserved for `KEEL_TIER_FULL`).

No dynamic per-request labels are emitted — KEEL enforces zero label
cardinality on the hot path (invariant I15). Reason-coded families
(e.g. `keel.backend.close.*`) ship as one counter per reason rather
than a single `reason="..."` label family.

## 4. Verifying export end-to-end

Stand up a quick mock receiver to confirm the exporter is wired:

```bash
python3 - << 'PY'
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n)
        print(f"POST {self.path} len={len(body)} ct={self.headers.get('Content-Type')}")
        self.send_response(200); self.end_headers()
    def log_message(self, *a, **kw): pass
HTTPServer(("127.0.0.1", 14318), H).serve_forever()
PY
```

Point KEEL at it:

```ini
[observability]
otlp_enabled = 1
otlp_endpoint_url = http://127.0.0.1:14318/v1/metrics
otlp_interval_ms = 1000
```

Start KEEL; you should see one POST per `otlp_interval_ms`, each
~2 KB of `application/x-protobuf`.

## 5. Watching the exporter itself

The admin endpoint exposes the exporter's self-stats as JSON:

```bash
curl -s http://127.0.0.1:9100/api/observability/exporter.json | jq
```

Returned fields:

| Field | Meaning |
|---|---|
| `export_attempts_total` | HTTP POSTs attempted (success + failure) |
| `export_success_total` | POSTs that returned 2xx |
| `export_failure_total` | POSTs that returned non-2xx or socket error |
| `export_timeout_total` | POSTs aborted by `otlp_timeout_ms` |
| `export_queue_depth` | Snapshots currently queued for export |
| `export_queue_capacity` | Maximum queue depth (`otlp_queue_capacity`) |
| `export_snapshots_dropped_total` | Snapshots dropped due to full queue |
| `last_export_duration_ns` | Wall-clock duration of the most recent attempt |
| `last_export_status` | `"ok"`, `"error"`, or `"timeout"` |
| `last_export_error` | Last error string, or `null` |
| `last_success_timestamp_ms` | Unix-ms timestamp of last 2xx |
| `last_failure_timestamp_ms` | Unix-ms timestamp of last non-2xx |

These fields are also surfaced as Prometheus counters/gauges on the
admin `/metrics` endpoint (see §6 of the metrics reference).

## 6. Operational guarantees

The exporter is designed to fail safely:

- **Non-blocking submit.** The reactor never waits on the exporter.
  Snapshots are produced on the aggregator thread; if the export
  queue is full, the oldest snapshot is dropped and counted —
  serving paths are not affected.
- **Collector outage isolation.** Verified by
  `tests/test_otlp_fault_injection.c`: with the collector down,
  admin `/healthz` and `/metrics` keep returning 200, and aggregator
  ticks continue to advance.
- **Bounded memory.** Encode scratch is `otlp_encode_buf_bytes` per
  attempt; pending snapshots are bounded by `otlp_queue_capacity`.
- **Clean shutdown.** On SIGTERM the aggregator is stopped before
  the exporter so no snapshot is enqueued after the exporter has
  drained. Verified by repeated launch + SIGTERM cycles.

## 7. Performance budget

Measured by `tests/test_otlp_overhead_bench.c` (60 s timeout, CI
gate):

| Operation | Budget | Measured (Linux x86-64, optimized build) |
|---|---|---|
| Snapshot project + protobuf encode | 200 µs | ~41 µs |
| `keel_otlp_exporter_submit` (queue insert) | 50 µs | 96 ns |
| Aggregator ticks at 20 ms cadence over 600 ms | ≥ 25 | 30 |

The exporter is comfortably within every documented headroom limit.
There is no measurable impact on serving paths from enabling it.

## 8. Disabling at runtime

To turn off OTLP export without rebuilding, set `otlp_enabled = 0`
(or empty `otlp_endpoint_url`) and restart KEEL. The Prometheus
scrape endpoint and admin surfaces are not affected.

## 9. Related documents

- [docs/METRICS_REFERENCE.md](METRICS_REFERENCE.md) — full metric
  inventory (OTLP §5 + admin-only extended §6).
- [docs/ADMIN_SQL.md](ADMIN_SQL.md) — admin SQL/HTTP/JSON surface.
- [docs/TRACING.md](TRACING.md) — distributed tracing pipeline.
- [docs/PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) —
  observability maturity and readiness gates.
