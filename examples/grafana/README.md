# KEEL Grafana Dashboard

Pre-built Grafana dashboard for monitoring KEEL PostgreSQL connection pooler.

## Import

1. Open Grafana → **Dashboards → Import**
2. Upload `keel-dashboard.json` (or paste its contents)
3. Select your Prometheus data source when prompted

## Requirements

- **Prometheus** scraping the KEEL admin `/metrics` endpoint
- **Grafana 10+** (schema version 39, time series / heatmap panels)
- KEEL `stats_level = extended` for histogram panels (latency, session duration)

## Dashboard Sections

| Row | Panels | Key Metrics |
|-----|--------|-------------|
| **Overview** | 8 stat/gauge tiles | Uptime, workers, active sessions, QPS, error rate, pool util, p99 latency, RSS |
| **Sessions** | Active per worker, create/close rate | `keel_sessions_active`, `keel_sessions_created`, `keel_sessions_closed` |
| **Connection Pool** | Breakdown (active/idle/cleaning/pinned), hit/miss rate, borrow/return | `keel_pool_connections_*`, `keel_pool_hits`, `keel_pool_misses` |
| **Query Routing** | Rate by type (read/write/tx), read/write ratio | `keel_queries_read`, `keel_queries_write`, `keel_queries_tx` |
| **Latency** | Query/backend/connect/wait percentiles (p50/p95/p99), heatmap | `keel_query_latency_ns`, `keel_backend_latency_ns`, etc. |
| **Errors** | Rate by type, proxy safety counters | `keel_errors_*`, `keel_proxy_state_desync_total`, etc. |
| **Throughput** | Client/backend byte rates, io_uring ops | `keel_bytes_*`, `keel_ops_*`, `keel_loop_iterations` |
| **TLS** | Connection rates, kTLS stats | `keel_tls_*`, `keel_ktls_*` |
| **Tracing** | Span throughput, export health | `keel_trace_spans_*`, `keel_trace_export_*` |
| **Rebalancing** | Migrations, rebalance checks | `keel_migrations_*`, `keel_rebalance_*` |
| **System** | CPU, RSS, FDs, buffer pool, session duration | `keel_cpu_*`, `keel_rss_bytes`, `keel_fd_*` |

## Template Variables

- **`$datasource`** — Prometheus data source selector
- **`$instance`** — Filter by KEEL instance (multi-select, default All)
- **`$worker`** — Filter by worker thread (multi-select, default All)

## Annotations

- **TLS cert reloads** — blue markers when certificate rotation is detected
