"""
test_performance.py — Throughput and latency baseline tests for the KEEL E2E suite
===================================================================================

Verifies that the proxy meets minimum throughput and latency targets when
running against the Docker Compose stack.  All numbers are conservative enough
to pass on any reasonable CI runner.

Background
----------
KEEL is designed as a high-performance proxy for sharded PostgreSQL.  Its io_uring
reactor, connection pool, and zero-copy read paths are intended to add minimal
latency over a direct backend connection.  These tests guard against regressions
that would degrade throughput or inflate tail latency.

What is tested
--------------
1. **Single-connection SELECT 1 throughput** — measures the round-trip overhead of
   KEEL's routing and protocol translation.  A regression here indicates added
   latency in the hot path (query parsing, pool borrow, backend dispatch, result
   forwarding).

2. **Tail latency (p99)** — ensures that the 99th-percentile latency stays within
   the budget.  High p99 is often caused by pool contention (wait for backend
   connection), lock contention in KEEL, or GC pauses in the test process.

3. **Parameterized query throughput** (psycopg2 ``%s`` parameters) — exercises the
   Extended Query Protocol (Parse → Bind → Execute).  KEEL must handle the
   multi-message exchange without copying payloads unnecessarily.  The psycopg2
   driver sends these as unnamed prepared statements, which are re-parsed on
   every query by design (``PQexecParams``).

4. **SQL-level PREPARE / EXECUTE throughput** — exercises the Simple Query Protocol
   ``PREPARE name AS ...`` / ``EXECUTE name(...)`` / ``DEALLOCATE name`` path.
   KEEL must track named prepared statements per session and use connection
   affinity (``backend_pool_borrow_with_stmts``) to route ``EXECUTE`` to the
   same backend connection that received ``PREPARE``.  Uses explicit transactions
   to guarantee connection pinning across all three statements in
   ``pool_mode=transaction``.

5. **Concurrent throughput** — 10 simultaneous connections must together exceed the
   parallel TPS target.  Regression here indicates lock contention in KEEL's
   worker pool, backend pool mutex, or io_uring submission queue.

6. **Connection churn** — open + query + close latency p99 must stay below the
   budget.  Regression here indicates slow connection setup (SSL handshake, KEEL
   authentication, pool initialization).

Why these tests exist
---------------------
Performance regressions in a proxy are invisible without explicit benchmarks.
A 2× latency regression in KEEL's hot path translates directly to 2× latency
for all client queries.  These conservative thresholds ensure that obvious
regressions (e.g. re-enabling a debug log call on every query) are caught in CI.

Why a test might fail
---------------------
- **Overloaded CI runner**: all thresholds are set at 20% of typical developer
  machine performance.  If the runner is extremely loaded, thresholds may not
  be met.  Consider skipping with ``-m "not stress"`` on constrained runners.
- **New latency source introduced**: adding a synchronous syscall, mutex, or
  allocation on the hot path will raise p99.  Profile with ``perf record``.
- **Pool contention**: if the pool is undersized for the test concurrency, workers
  will queue for connections.  Check ``SHOW POOL_STATS`` via the admin interface.
- **SQL PREPARE routing failure**: if KEEL's prepared statement tracking misses a
  statement name, ``EXECUTE`` is routed to a backend that does not have it and
  PostgreSQL returns "prepared statement does not exist".

Consequences of failure
-----------------------
- Hidden performance regression shipped to production, causing SLA violations.
- High p99 latency under load → client timeouts → cascading failures.

Markers: ``stress`` (reuses the existing stress marker)

Run selectively::

    pytest tests/e2e/ -m stress -k performance -v
"""

from __future__ import annotations

import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import pytest

from helpers import pg_exec, pg_scalar

pytestmark = pytest.mark.stress

# ---------------------------------------------------------------------------
# Thresholds — tuned for modest CI hardware
# ---------------------------------------------------------------------------
_MIN_TPS_SINGLE      = 50      # SELECT 1 qps on a single connection
_MIN_TPS_PARALLEL    = 200     # aggregate qps across all threads
_MAX_P99_MS          = 500.0   # p99 latency budget (ms)
_MAX_CHURN_P99_S     = 0.5     # max p99 for connect + query + disconnect


def _percentile(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    sv = sorted(vals)
    idx = (p / 100.0) * (len(sv) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sv) - 1)
    return sv[lo] + (sv[hi] - sv[lo]) * (idx - lo)


# ---------------------------------------------------------------------------
# Single-connection baseline
# ---------------------------------------------------------------------------

class TestSingleConnectionPerformance:
    @pytest.mark.timeout(60)
    def test_baseline_tps_single_connection(self, keel_conn):
        """Single-connection SELECT 1 throughput must exceed MIN_TPS_SINGLE."""
        duration = 10.0
        count = 0
        t0 = time.monotonic()
        deadline = t0 + duration
        with keel_conn.cursor() as cur:
            while time.monotonic() < deadline:
                cur.execute("SELECT 1")
                cur.fetchone()
                count += 1

        elapsed = time.monotonic() - t0
        tps = count / elapsed
        assert tps >= _MIN_TPS_SINGLE, (
            f"Single-connection TPS {tps:.1f} < minimum {_MIN_TPS_SINGLE}"
        )

    @pytest.mark.timeout(60)
    def test_latency_percentiles(self, keel_conn):
        """p99 latency must be below the budget."""
        latencies: list[float] = []
        deadline = time.monotonic() + 10.0
        with keel_conn.cursor() as cur:
            while time.monotonic() < deadline:
                t0 = time.monotonic()
                cur.execute("SELECT 1")
                cur.fetchone()
                latencies.append((time.monotonic() - t0) * 1000.0)

        p50  = _percentile(latencies, 50)
        p95  = _percentile(latencies, 95)
        p99  = _percentile(latencies, 99)
        p999 = _percentile(latencies, 99.9)

        assert p99 <= _MAX_P99_MS, (
            f"p99 latency {p99:.1f}ms > budget {_MAX_P99_MS}ms "
            f"(p50={p50:.1f}ms p95={p95:.1f}ms p99.9={p999:.1f}ms)"
        )

    @pytest.mark.timeout(60)
    def test_prepared_statement_tps(self, keel_conn):
        """Prepared-statement throughput must meet the minimum TPS requirement.

        Uses psycopg2 parameterized queries which exercise PostgreSQL's Extended
        Query Protocol (Parse → Bind → Execute).  KEEL must forward the protocol
        messages to the backend and return results transparently.

        See also: test_sql_level_prepare_execute_tps for the SQL-level
        PREPARE / EXECUTE path.
        """
        count = 0
        t0 = time.monotonic()
        deadline = t0 + 10.0
        with keel_conn.cursor() as cur:
            # Use parameterized queries — SQL-level PREPARE is not supported
            # by all proxy modes; psycopg2 parameters exercise the same path.
            while time.monotonic() < deadline:
                cur.execute("SELECT %s::int + %s::int", (count % 500, count % 300))
                cur.fetchone()
                count += 1

        tps = count / (time.monotonic() - t0)
        assert tps >= _MIN_TPS_SINGLE, (
            f"Prepared-statement TPS {tps:.1f} < minimum {_MIN_TPS_SINGLE}"
        )

    @pytest.mark.timeout(60)
    def test_sql_level_prepare_execute_tps(self, keel_dsn):
        """SQL-level PREPARE / EXECUTE / DEALLOCATE throughput through KEEL.

        Tests the Simple Query Protocol path for named server-side prepared
        statements:

          PREPARE stmt_name AS SELECT $1::int + $2::int
          EXECUTE stmt_name(42, 58)
          DEALLOCATE stmt_name

        KEEL tracks prepared statement state per client session and uses
        connection affinity (backend_pool_borrow_with_stmts) to route EXECUTE
        to the same backend connection that received PREPARE.

        Why this test exists
        --------------------
        Applications that use ORM-level or driver-level PREPARE (e.g. PgBouncer
        clients with server_reset_query, or applications using PREPARE directly)
        must not break when routed through KEEL.  The Simple Query Protocol
        PREPARE is distinct from the Extended Query Protocol Parse message —
        both must be handled correctly.

        Why a test might fail
        ---------------------
        - KEEL recycles the backend connection between PREPARE and EXECUTE,
          serving EXECUTE on a different backend that does not have the
          statement. Expected error: "prepared statement does not exist".
        - Statement name conflicts if DEALLOCATE is not tracked correctly.
        - Shard routing for EXECUTE cannot determine the target shard without
          looking up the original SQL template.

        Uses explicit transactions (autocommit=False) to guarantee that PREPARE,
        EXECUTE, and DEALLOCATE all run within the same backend connection
        (pool_mode=transaction pins the backend for the duration of a txn).
        """
        count = 0
        t0 = time.monotonic()
        deadline = t0 + 10.0
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                while time.monotonic() < deadline:
                    stmt = f"perf_ps_{count % 5}"
                    cur.execute(
                        f"PREPARE {stmt} AS SELECT $1::int + $2::int"
                    )
                    cur.execute(
                        f"EXECUTE {stmt}({count % 500}, {count % 300})"
                    )
                    cur.fetchone()
                    cur.execute(f"DEALLOCATE {stmt}")
                    conn.commit()
                    count += 1
        finally:
            conn.close()

        tps = count / (time.monotonic() - t0)
        assert tps >= _MIN_TPS_SINGLE, (
            f"SQL PREPARE/EXECUTE TPS {tps:.1f} < minimum {_MIN_TPS_SINGLE}"
        )


# ---------------------------------------------------------------------------
# Concurrent throughput
# ---------------------------------------------------------------------------

class TestConcurrentThroughput:
    @pytest.mark.timeout(60)
    def test_parallel_throughput_10_clients(self, keel_dsn):
        """10 concurrent connections must together achieve MIN_TPS_PARALLEL."""
        clients   = 10
        duration  = 10.0
        counts    = [0] * clients
        errors    = [0] * clients
        stop      = threading.Event()

        def _worker(idx: int) -> None:
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                cur = conn.cursor()
                while not stop.is_set():
                    try:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                        counts[idx] += 1
                    except Exception:
                        errors[idx] += 1
                conn.close()
            except Exception:
                errors[idx] += 1

        threads = [threading.Thread(target=_worker, args=(i,), daemon=True)
                   for i in range(clients)]
        t0 = time.monotonic()
        for th in threads:
            th.start()
        time.sleep(duration)
        stop.set()
        for th in threads:
            th.join(timeout=5)
        elapsed = time.monotonic() - t0

        total_q = sum(counts)
        total_e = sum(errors)
        tps = total_q / elapsed

        assert tps >= _MIN_TPS_PARALLEL, (
            f"Parallel TPS {tps:.1f} < minimum {_MIN_TPS_PARALLEL} ({clients} clients)"
        )
        error_rate = total_e / max(total_q + total_e, 1)
        assert error_rate <= 0.02, (
            f"Error rate {error_rate*100:.1f}% > 2% under {clients} concurrent clients"
        )

    @pytest.mark.timeout(60)
    def test_connection_churn_latency(self, keel_dsn):
        """Connect + SELECT 1 + disconnect p99 must be within budget."""
        durations: list[float] = []
        for _ in range(50):
            t0 = time.monotonic()
            conn = psycopg2.connect(keel_dsn, connect_timeout=5)
            conn.autocommit = True
            conn.cursor().execute("SELECT 1")
            conn.close()
            durations.append(time.monotonic() - t0)

        p99 = _percentile(durations, 99)
        assert p99 <= _MAX_CHURN_P99_S, (
            f"Connection churn p99 {p99*1000:.1f}ms > budget {_MAX_CHURN_P99_S*1000:.0f}ms"
        )


# ---------------------------------------------------------------------------
# Result set throughput
# ---------------------------------------------------------------------------

class TestResultSetThroughput:
    @pytest.mark.timeout(60)
    def test_large_result_set_row_rate(self, keel_conn):
        """10 000-row result set must be delivered within a reasonable time."""
        n   = 10_000
        t0  = time.monotonic()
        rows = pg_exec(keel_conn, f"SELECT i FROM generate_series(1, {n}) i")
        elapsed = time.monotonic() - t0

        assert len(rows) == n, f"Expected {n} rows, got {len(rows)}"
        rps = n / elapsed
        # Minimum 5 000 rows/s — very conservative for any hardware
        assert rps >= 5_000, f"Row rate {rps:.0f} r/s is too slow"

    @pytest.mark.timeout(30)
    def test_wide_row_throughput(self, keel_conn):
        """Rows with many columns must be transmitted without errors."""
        # Build a query returning 100 columns
        cols = ", ".join(f"md5('{i}'::text) AS c{i}" for i in range(100))
        rows = pg_exec(keel_conn, f"SELECT {cols}")
        assert len(rows) == 1, "Expected 1 row"
        assert len(rows[0]) == 100, f"Expected 100 columns, got {len(rows[0])}"
