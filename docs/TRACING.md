# Distributed Tracing

Keel includes built-in distributed tracing with W3C Trace Context
propagation and OTLP/HTTP export.

## Architecture

```
  Client (traceparent) ──► Keel worker ──► Backend
          │                    │
          │              span lifecycle
          │                    │
          ▼                    ▼
     session.trace_ctx   per-worker ring buffer
                               │
                          exporter thread
                               │
                               ▼
                     OTLP/HTTP collector
                     (Jaeger, Tempo, etc.)
```

Each session creates a root span that covers its lifetime.  Events are
recorded at key flow points: query classification, pool borrow, backend
I/O, and session close.  Finished spans are written into a lock-free
per-worker ring buffer and drained by a background exporter thread.

## Configuration

Add a `[tracing]` section to `keel.ini`:

```ini
[tracing]
# Master switch — set to true to enable tracing.
# Default: false
enabled = true

# OTLP/HTTP endpoint for span export.
# Default: http://localhost:4318/v1/traces
endpoint = http://localhost:4318/v1/traces

# OTLP resource service.name attribute.
# Default: keel
service_name = keel

# Head-based sampling rate in parts-per-million.
#   1000000 = 100% (sample every request)
#     10000 =   1% (default)
#      1000 = 0.1%
# Default: 10000
sample_rate_ppm = 10000

# Maximum spans per OTLP export batch.
# Default: 256
batch_size = 256

# How often the exporter thread flushes, in milliseconds.
# Default: 5000
flush_interval_ms = 5000

# Per-worker span ring buffer capacity (must be power of 2).
# When full, new spans are dropped (counted in stats).
# Default: 4096
ring_capacity = 4096

# HTTP timeout for OTLP export, in milliseconds.
# Default: 10000
export_timeout_ms = 10000
```

## Sampling

Keel uses **head-based sampling**: the decision is made once at session
creation and applies to all spans within that session.  The rate is
configured via `sample_rate_ppm` (parts per million):

| `sample_rate_ppm` | Effective Rate |
|--------------------|---------------|
| 1000000            | 100%          |
| 100000             | 10%           |
| 10000              | 1% (default)  |
| 1000               | 0.1%          |
| 0                  | disabled      |

If an incoming client sends a `traceparent` header with the sampled flag
set, the session always records.

## Span Events

Each session root span may contain these events:

| Event              | When                             |
|--------------------|----------------------------------|
| `query.classify`   | Query type and route determined   |
| `pool.borrow`      | Backend connection borrowed       |
| `pool.wait`        | Blocked waiting for a connection  |
| `backend.query`    | Query forwarded to backend        |
| `backend.response` | Full response received            |

## Span Attributes

| Attribute            | Type   | Description                    |
|----------------------|--------|--------------------------------|
| `db.system`          | string | `postgresql` or `mysql`        |
| `db.statement`       | string | Query text (first 256 chars)   |
| `keel.session_id`    | int    | Worker-scoped session counter  |
| `keel.worker_id`     | int    | Worker thread index            |
| `keel.route`         | string | Routing decision (read/write)  |
| `keel.pool_wait_ms`  | int    | Milliseconds waiting for conn  |

## Exporter

The OTLP exporter runs as a single background thread.  Key features:

- **HTTP keep-alive**: Persistent TCP connection reused across export
  cycles (eliminates per-batch handshake overhead).
- **Retry with backoff**: Transient failures (HTTP 429, 502, 503, 504,
  network errors) are retried up to 3 times with exponential backoff
  (100ms, 200ms, 400ms).
- **Graceful shutdown**: On SIGTERM, the exporter drains remaining
  spans before exiting.
- **Zero hot-path allocation**: Spans are stored inline in the session
  struct; finished spans are memcpy'd into the ring buffer.

## Log Correlation

When JSON logging is enabled (`log_format = json` in `[logging]`),
every log line includes `trace_id` and `span_id` fields for the
active session on that worker thread.  This allows joining logs and
traces in observability platforms.

```json
{"ts":"2025-01-15T10:30:45.123456Z","level":"INFO","cat":"POOL",
 "file":"engine_flow.c","line":1270,"func":"engine_flow_process",
 "msg":"Pool borrow: acquired backend conn",
 "trace_id":"a1b2c3d4e5f60718a1b2c3d4e5f60718",
 "span_id":"deadbeef12345678"}
```

## Prometheus Metrics

Tracing stats are exposed on the Prometheus `/metrics` endpoint:

| Metric                          | Type    | Description                  |
|---------------------------------|---------|------------------------------|
| `keel_trace_spans_exported`     | counter | Total spans successfully sent|
| `keel_trace_spans_dropped`      | counter | Spans dropped (ring full)    |
| `keel_trace_export_errors`      | counter | OTLP export failures         |
| `keel_trace_export_batches`     | counter | Export batch count           |

## Backends

Tested with:
- **Jaeger** (v1.50+) — `COLLECTOR_OTLP_ENABLED=true`
- **Grafana Tempo** — OTLP/HTTP ingest
- **OpenTelemetry Collector** — `otlpreceiver` with HTTP

## Helm

See the `tracing` section in `values.yaml` for Kubernetes deployment:

```yaml
tracing:
  enabled: false
  endpoint: "http://tempo:4318/v1/traces"
  sampleRatePpm: 10000
  serviceName: "keel"
```
