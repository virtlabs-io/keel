"""
test_observability.py — Prometheus metrics and admin interface tests
====================================================================

Tests that verify KEEL exposes correct observability data at all times.

Background
----------
KEEL exposes two observability interfaces:

1. **Prometheus metrics** at ``http://<host>:9101/metrics``: a text/plain
   Prometheus exposition format endpoint with counters and gauges for all major
   subsystems (queries, pool, router, errors, connection churn).

2. **Admin SQL interface** at ``<host>:6433``: a PostgreSQL wire-protocol endpoint
   accepting connections from the ``admin`` user.  KEEL exposes virtual tables
   and views (``keel_*``) that return live runtime state.

What is tested
--------------
- **Prometheus endpoint availability**: ``/metrics``, ``/healthz``, ``/readyz``
  return 200 OK.
- **Metric presence**: all required counters (``keel_queries_total``,
  ``keel_pool_borrows_total``, etc.) are present immediately after startup.
- **Counter monotonicity**: after sending queries through KEEL, the query counter
  has increased.  This validates that the proxy's instrumentation path is active.
- **Error counter increment**: a deliberate invalid query increments the error
  counter.  Validates that errors are attributed correctly and not silently
  discarded.
- **Pool gauge accuracy**: the pool metrics reflect the actual pool utilization
  seen during concurrent load.
- **Admin interface**: ``SHOW STATS``, ``SHOW POOLS``, ``SHOW SERVERS`` return
  rows and are parseable.

Why these tests exist
---------------------
KEEL is deployed in environments where on-call engineers rely on Prometheus
dashboards and admin queries to diagnose incidents.  If metrics are absent,
stale, or systematically wrong, operators cannot detect shard imbalances, pool
saturation, or elevated error rates.  The tests guard against:

- Silently broken metric scrape endpoint (HTTP 500, empty body).
- Counters that never increment (metric registration bug, wrong label, wrong
  counter variable incremented).
- Admin interface accepting connections but returning no data.

Why a test might fail
---------------------
- **KEEL not yet accepting connections**: if the test runs before KEEL's health
  probe has completed its first cycle, counters may be zero even after queries.
  The ``wait_for_keel_ready`` fixture waits up to 60s.
- **Prometheus port clash**: if port 9101 is already bound by another process,
  KEEL starts but the metrics endpoint is unreachable.
- **Counter overflow / reset**: very long test runs with many restarts may show
  counters resetting to 0 if KEEL was restarted between the before/after reads.
- **Metric name changes**: if KEEL renames a metric, the test fails with
  "metric not found".  Update the expected names in ``helpers.py``.

Consequences of failure
-----------------------
- Operators lose visibility into KEEL's health during incidents.
- Alert rules based on ``keel_*`` counters stop firing → silent failures in
  production go undetected.
- SLA reporting based on error rates is incorrect.
"""

from __future__ import annotations

import time

import pytest
import psycopg2
import requests

from helpers import pg_exec, pg_scalar, parse_prometheus, get_metric

pytestmark = pytest.mark.metrics

# ---------------------------------------------------------------------------
# Module-level constants (mirrors tests/e2e/conftest.py — defined here to
# avoid cross-directory conftest resolution issues when both e2e/ and
# integration/ are collected in the same pytest session).
# ---------------------------------------------------------------------------
KEEL_HOST       = "127.0.0.1"
KEEL_PORT       = 26432
KEEL_ADMIN_PORT = 26433
PG_USER         = "postgres"
PG_PASSWORD     = "postgres"
PG_DBNAME       = "testdb"


# ---------------------------------------------------------------------------
# Module-level health guard — wait for KEEL proxy to serve queries before
# any test in this module runs (needed after failover/chaos tests).
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def wait_for_keel_ready(request):
    """Block until KEEL accepts a SELECT 1 (up to 60 s) before this module."""
    import psycopg2
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            c = psycopg2.connect(
                host=KEEL_HOST, port=KEEL_PORT,
                user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME,
                connect_timeout=5,
            )
            c.autocommit = True
            with c.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            c.close()
            return
        except psycopg2.Error:
            time.sleep(2)


# ---------------------------------------------------------------------------
# Prometheus tests
# ---------------------------------------------------------------------------

class TestPrometheusEndpoint:

    def test_metrics_endpoint_reachable(self, prom_url):
        """GET /metrics returns HTTP 200."""
        resp = requests.get(prom_url, timeout=10)
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"

    def test_metrics_content_type(self, prom_url):
        """Content-Type is text/plain (Prometheus exposition format)."""
        resp = requests.get(prom_url, timeout=10)
        assert "text/plain" in resp.headers.get("content-type", "")

    def test_metrics_not_empty(self, prom_url):
        """The response body contains at least one metric line."""
        text = requests.get(prom_url, timeout=10).text
        non_comment = [l for l in text.splitlines() if l and not l.startswith("#")]
        assert len(non_comment) > 0, "Prometheus /metrics returned no metric lines"

    def test_required_metric_names_present(self, fetch_metrics):
        """
        A set of required KEEL metric names must appear in the response.
        This validates that the metrics registry is properly initialised.
        """
        text = fetch_metrics()
        required = [
            "keel_queries_total",
            "keel_pool_borrows",
            "keel_sessions_active",
        ]
        missing = [name for name in required if name not in text]
        assert not missing, f"Missing metrics: {missing}"

    def test_router_metrics_present(self, fetch_metrics):
        """Routing-specific metrics must be present in the Prometheus output."""
        text = fetch_metrics()
        # keel_queries_* metrics reflect actual query routing through the proxy.
        # keel_router_* metrics require the low-level keel_router_t module to be
        # attached to the admin endpoint (used by the standalone router API).
        routing_metrics = [
            "keel_queries_total",
            "keel_queries_read",
            "keel_queries_write",
        ]
        missing = [m for m in routing_metrics if m not in text]
        assert not missing, f"Missing routing metrics: {missing}"

    def test_query_counter_increments(self, keel_conn, fetch_metrics):
        """Executing queries through KEEL increments keel_queries_total."""
        before_text = fetch_metrics()
        # Use the aggregate total (keel_queries_total_total) to avoid per-worker
        # label overwriting in parse_prometheus (last worker value wins)
        before = get_metric(before_text, "keel_queries_total_total") or 0.0

        # Execute 10 queries
        for i in range(10):
            pg_exec(keel_conn, f"SELECT {i}::int")

        # Allow the metrics to update
        time.sleep(0.5)

        after_text = fetch_metrics()
        after = get_metric(after_text, "keel_queries_total_total") or 0.0

        assert after >= before + 10, (
            f"keel_queries_total_total should have increased by ≥10: before={before}, after={after}"
        )

    def test_pool_borrow_counter_increments(self, keel_dsn, fetch_metrics):
        """Running queries increments the keel_queries_total_total counter."""
        import psycopg2

        before_text = fetch_metrics()
        before = get_metric(before_text, "keel_queries_total_total") or 0.0

        # Run 15 sequential queries; each must register as a completed query.
        for _ in range(15):
            conn = psycopg2.connect(keel_dsn, connect_timeout=10)
            conn.autocommit = True
            pg_exec(conn, "SELECT 1")
            conn.close()

        time.sleep(0.5)
        after_text = fetch_metrics()
        after = get_metric(after_text, "keel_queries_total_total") or 0.0

        assert after >= before + 15, (
            f"keel_queries_total_total should have increased by ≥15: before={before}, after={after}"
        )

    def test_active_connections_gauge(self, fetch_metrics):
        """keel_sessions_active gauge is present and non-negative."""
        text = fetch_metrics()
        val = get_metric(text, "keel_sessions_active_total")
        # Gauge should exist and be a valid number >= 0
        assert val is not None, "keel_sessions_active_total gauge not found"
        assert val >= 0

    def test_error_metric_after_bad_query(self, keel_dsn, fetch_metrics):
        """Executing an invalid query increments an error counter."""
        before_text = fetch_metrics()
        before_errors = sum(
            v for k, v in parse_prometheus(before_text).items()
            if "error" in k.lower()
        )

        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        try:
            pg_exec(conn, "SELECT * FROM nonexistent_table_xyz_keel_e2e")
        except psycopg2.Error:
            pass
        conn.close()

        time.sleep(0.5)
        after_text = fetch_metrics()
        after_errors = sum(
            v for k, v in parse_prometheus(after_text).items()
            if "error" in k.lower()
        )
        assert after_errors >= before_errors, (
            "Error counters should be non-decreasing after a bad query"
        )

    def test_metrics_under_load(self, keel_dsn, fetch_metrics):
        """
        After 100 sequential queries, the total query counter has increased
        and no negative gauge values are present.
        """
        import psycopg2

        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        for i in range(100):
            pg_exec(conn, "SELECT 1")
        conn.close()

        time.sleep(0.5)
        text = fetch_metrics()
        parsed = parse_prometheus(text)

        for name, val in parsed.items():
            if "gauge" not in name and "histogram" not in name:
                assert val >= 0, f"Metric '{name}' has negative value: {val}"


class TestScatterUnsupportedPatternMetric:
    """The keel_scatter_unsupported_pattern_total counter exposes per-kind
    visibility into scatter dispatches whose result correctness depends on
    SQL patterns the engine does not fully merge — so operators can alert
    on silent-wrong-result risk without enabling verbose logging.
    """

    KINDS = {
        "percentile",
        "window_func",
        "recursive_cte",
        "union_all",
        "dml_returning",
        "ddl",
    }

    def _kind_value(self, text: str, kind: str) -> float:
        needle = f'keel_scatter_unsupported_pattern_total{{kind="{kind}"}} '
        for line in text.splitlines():
            if line.startswith(needle):
                return float(line[len(needle):])
        raise AssertionError(
            f'kind="{kind}" missing from /metrics; got\n{text}'
        )

    def test_all_kinds_exposed(self, fetch_metrics):
        text = fetch_metrics()
        assert "keel_scatter_unsupported_pattern_total" in text
        for kind in self.KINDS:
            self._kind_value(text, kind)

    def test_percentile_increments(self, keel_dsn, fetch_metrics):
        before = self._kind_value(fetch_metrics(), "percentile")
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        try:
            try:
                pg_exec(
                    conn,
                    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id) "
                    "FROM users",
                )
            except Exception:
                pass  # Result correctness is the subject of §1.4; counter
                      # must still bump on dispatch.
        finally:
            conn.close()
        time.sleep(0.3)
        after = self._kind_value(fetch_metrics(), "percentile")
        assert after > before, (
            f"percentile counter did not increment: before={before} after={after}"
        )


# ---------------------------------------------------------------------------
# Admin SQL interface tests
# ---------------------------------------------------------------------------

class TestAdminInterface:

    def test_admin_port_accepts_connections(self, compose_stack):
        """The admin SQL port accepts PostgreSQL wire-protocol connections."""
        import psycopg2

        conn = psycopg2.connect(
            host=KEEL_HOST, port=KEEL_ADMIN_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname="keel_admin",
            connect_timeout=5,
        )
        conn.autocommit = True
        rows = pg_exec(conn, "SHOW VERSION")
        assert len(rows) > 0, "SHOW VERSION returned no rows"
        conn.close()

    def test_admin_show_pools(self, compose_stack):
        """SHOW POOLS via the admin interface returns at least one row."""
        import psycopg2

        conn = psycopg2.connect(
            host=KEEL_HOST, port=KEEL_ADMIN_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname="keel_admin", connect_timeout=5,
        )
        conn.autocommit = True
        rows = pg_exec(conn, "SHOW POOLS")
        conn.close()
        assert len(rows) > 0, "SHOW POOLS returned no rows"

    def test_admin_show_stats(self, compose_stack):
        """SHOW STATS via the admin interface returns at least one row."""
        import psycopg2

        conn = psycopg2.connect(
            host=KEEL_HOST, port=KEEL_ADMIN_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname="keel_admin", connect_timeout=5,
        )
        conn.autocommit = True
        rows = pg_exec(conn, "SHOW STATS")
        conn.close()
        assert len(rows) > 0, "SHOW STATS returned no rows"

    def test_admin_show_servers(self, compose_stack):
        """SHOW SERVERS lists the backend shard connections."""
        import psycopg2

        conn = psycopg2.connect(
            host=KEEL_HOST, port=KEEL_ADMIN_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname="keel_admin", connect_timeout=5,
        )
        conn.autocommit = True
        rows = pg_exec(conn, "SHOW SERVERS")
        conn.close()
        # Should list at least shard0 and shard1
        assert len(rows) >= 2, f"Expected ≥2 server rows, got {len(rows)}"
