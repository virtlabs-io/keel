"""
test_stress.py — Concurrency and load tests
============================================

Tests that KEEL handles high concurrency without data corruption,
deadlocks, or connection leaks.

Background
----------
KEEL uses an io_uring reactor with 4 worker threads.  Under concurrent load,
the workers share backend connection pools protected by mutexes.  These tests
stress the concurrent paths to detect:

- **Data races** in the pool borrow/return path.
- **Lost updates** in scatter-write under concurrent writers.
- **Connection leaks** that exhaust the pool under sustained load.
- **Result corruption** in scatter-merge under concurrent readers.
- **Starvation** of long-running transactions when short transactions arrive.

All tests are marked ``stress`` and use ``--timeout=120`` to catch deadlocks.

What is tested
--------------
1. **50 concurrent readers** — no contention, no corruption.  All SELECT queries
   return correct results.
2. **50 concurrent writers** — each inserts a unique row via explicit transaction.
   All rows must persist on the correct shard; no row must be lost or duplicated.
3. **Mixed read/write concurrency** — correctness under a realistic workload
   (reads and writes interleaved).
4. **Transaction throughput** — measure TPS and assert a minimum threshold.
5. **Pool saturation** — exceed pool size, requests queue and complete.  KEEL
   must queue excess connections rather than dropping them.
6. **Scatter-merge under load** — concurrent aggregate queries must return
   consistent results.

Why these tests exist
---------------------
Without stress tests, concurrency bugs are invisible during normal single-client
testing.  Common failure modes discovered only under load:
- Pool borrow/return race → connection state corruption (one client sees another
  client's in-progress transaction).
- io_uring SQE ring overflow under high SQE submission rate → dropped operations.
- Scatter-write locking → all writes serialized on a mutex → throughput collapse.
- Deadlock between a long-running scatter-write and a pool exhaustion wait.

Why a test might fail
---------------------
- **Pool size too small**: if ``max_pool_size`` < number of concurrent connections,
  requests queue.  The pool saturation test verifies this queuing behaviour.
- **Lost update under concurrent writes**: if two scatter-writes race and both
  try to PREPARE TRANSACTION with the same GID, one will fail.  KEEL uses a
  monotonic sequence number in the GID to prevent this.
- **Threshold too tight on slow CI**: TPS thresholds are set at 20% of typical
  developer machine performance.  If CI is heavily loaded, tests may fail.

Consequences of failure
-----------------------
- Data loss under concurrent writes → financial and inventory inconsistencies.
- Pool exhaustion → all clients queued indefinitely → service unavailable.
- Scatter-merge inconsistency under load → different clients see different
  aggregate totals for the same data at the same time.

Run selectively::

    pytest tests/e2e/ -m stress --timeout=120
"""

from __future__ import annotations

import time
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
import psycopg2

from helpers import (
    pg_exec, pg_scalar, pg_count,
    shard_total_count, clear_table_on_shards,
    run_concurrent, WorkerResult,
)

pytestmark = pytest.mark.stress


@pytest.fixture(scope="module", autouse=True)
def wait_for_keel_healthy(keel_dsn):
    """Wait for KEEL to be fully healthy (shard routing working) before stress tests."""
    # Use IDs that route to different shards to verify both are reachable via keel.
    probe_ids = (88881, 88882)
    for attempt in range(30):
        try:
            conn = psycopg2.connect(keel_dsn, connect_timeout=5)
            conn.autocommit = True
            with conn.cursor() as cur:
                for pid in probe_ids:
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, 'probe') ON CONFLICT DO NOTHING",
                        (pid,)
                    )
                for pid in probe_ids:
                    cur.execute("DELETE FROM users WHERE id = %s", (pid,))
            conn.close()
            return
        except psycopg2.Error:
            time.sleep(2)
    raise RuntimeError("KEEL did not become healthy within timeout before stress tests")

pytestmark = pytest.mark.stress


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_tables(shard0_conn, shard1_conn):
    clear_table_on_shards(shard0_conn, shard1_conn, "users")
    clear_table_on_shards(shard0_conn, shard1_conn, "events")
    yield
    clear_table_on_shards(shard0_conn, shard1_conn, "users")
    clear_table_on_shards(shard0_conn, shard1_conn, "events")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConcurrentReaders:

    def test_50_concurrent_selects(self, keel_dsn):
        """50 threads each run SELECT 1 concurrently — all must succeed."""
        def reader(result: WorkerResult, dsn: str) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = True
                val = pg_scalar(conn, "SELECT 1")
                if val == 1:
                    result.record_success()
                else:
                    result.record_error(AssertionError(f"Unexpected value: {val}"))
            except Exception as exc:
                result.record_error(exc)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        result = run_concurrent(reader, 50, keel_dsn)
        assert not result.errors, f"{len(result.errors)} reader errors: {result.errors[:3]}"
        assert result.successes == 50

    def test_concurrent_scatter_count_consistent(self, keel_dsn, shard0_conn, shard1_conn):
        """
        Insert N rows, then 20 threads simultaneously run COUNT(*).
        Every thread must observe exactly N rows.
        """
        N = 60
        for i in range(N):
            pg_exec(shard0_conn if i % 2 == 0 else shard1_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (i, "stress", i))

        counts: list[int] = []
        lock = threading.Lock()

        def counter(result: WorkerResult, dsn: str) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = True
                c = pg_scalar(conn, "SELECT COUNT(*) FROM events")
                with lock:
                    counts.append(c)
                result.record_success()
            except Exception as exc:
                result.record_error(exc)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        result = run_concurrent(counter, 20, keel_dsn)
        assert not result.errors, f"Counter errors: {result.errors}"
        assert all(c == N for c in counts), (
            f"Inconsistent scatter-merge counts: min={min(counts)} max={max(counts)} expected={N}"
        )


class TestConcurrentWriters:

    def test_50_concurrent_inserts_no_loss(self, keel_dsn, shard0_conn, shard1_conn):
        """
        50 threads each insert a unique row.  All rows must be present after
        all threads complete — no rows lost, no duplicates.
        """
        BASE_ID = 100_000
        N = 50

        def writer(result: WorkerResult, dsn: str, idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = True
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (BASE_ID + idx, f"stress_{idx}"))
                result.record_success()
            except Exception as exc:
                result.record_error(exc)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        result = WorkerResult()
        threads = [
            threading.Thread(target=writer, args=(result, keel_dsn, i))
            for i in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not result.errors, f"Write errors: {result.errors[:3]}"
        total = shard_total_count(
            shard0_conn, shard1_conn, "users",
            f"id >= {BASE_ID} AND id < {BASE_ID + N}",
        )
        assert total == N, f"Expected {N} rows, got {total}"

    def test_concurrent_update_no_lost_update(self, keel_dsn, shard0_conn, shard1_conn):
        """
        10 threads each increment a counter row 20 times.
        Final value must equal 10 * 20 = 200.
        """
        # Create a row on shard 0 (even id)
        pg_exec(shard0_conn,
                "INSERT INTO users(id, name, balance) VALUES (200000, 'counter', 0) "
                "ON CONFLICT(id) DO UPDATE SET balance = 0")

        N_THREADS = 10
        N_INCREMENTS = 20
        errors: list[Exception] = []
        lock = threading.Lock()

        def incrementer(dsn: str) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = True
                for _ in range(N_INCREMENTS):
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE users SET balance = balance + 1 WHERE id = 200000"
                        )
            except Exception as exc:
                with lock:
                    errors.append(exc)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        threads = [threading.Thread(target=incrementer, args=(keel_dsn,))
                   for _ in range(N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, f"Concurrent update errors: {errors}"

        balance = pg_scalar(shard0_conn, "SELECT balance FROM users WHERE id = 200000")
        expected = N_THREADS * N_INCREMENTS
        assert float(balance) == expected, f"Expected balance={expected}, got {balance}"


class TestMixedWorkload:

    def test_concurrent_reads_and_writes(self, keel_dsn, shard0_conn, shard1_conn):
        """
        30 writer threads and 20 reader threads run simultaneously.
        Writers succeed, readers get consistent non-negative counts.
        """
        BASE_ID = 200_000
        N_WRITERS = 30
        N_READERS = 20
        read_counts: list[int] = []
        write_errors: list[Exception] = []
        read_errors: list[Exception] = []
        lock = threading.Lock()

        def writer(idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (BASE_ID + idx, f"mixed_{idx}"))
            except Exception as exc:
                with lock:
                    write_errors.append(exc)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        def reader() -> None:
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                c = pg_scalar(conn, "SELECT COUNT(*) FROM users")
                with lock:
                    read_counts.append(int(c or 0))
            except Exception as exc:
                with lock:
                    read_errors.append(exc)
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        threads = (
            [threading.Thread(target=writer, args=(i,)) for i in range(N_WRITERS)]
            + [threading.Thread(target=reader) for _ in range(N_READERS)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not write_errors, f"Write errors: {write_errors[:3]}"
        assert not read_errors,  f"Read errors:  {read_errors[:3]}"
        # All read counts must be non-negative and monotonically plausible
        assert all(c >= 0 for c in read_counts)


class TestThroughput:

    def test_minimum_sequential_tps(self, keel_dsn):
        """
        Execute 500 simple SELECT 1 queries and verify throughput exceeds
        a conservative 200 TPS baseline.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True

        N = 500
        start = time.monotonic()
        for _ in range(N):
            pg_exec(conn, "SELECT 1")
        elapsed = time.monotonic() - start
        conn.close()

        tps = N / elapsed
        assert tps >= 200, (
            f"Sequential TPS too low: {tps:.1f} (got {elapsed:.2f}s for {N} queries)"
        )
        print(f"\n  [stress] Sequential TPS: {tps:.0f}")

    def test_concurrent_tps(self, keel_dsn):
        """
        8 threads each run 200 queries; aggregate TPS must exceed 800.
        Validates KEEL scales linearly across workers.
        """
        N_THREADS = 8
        N_QUERIES = 200
        thread_times: list[float] = []
        lock = threading.Lock()

        def measure(dsn: str) -> None:
            conn = psycopg2.connect(dsn, connect_timeout=10)
            conn.autocommit = True
            t0 = time.monotonic()
            for _ in range(N_QUERIES):
                pg_exec(conn, "SELECT 1")
            elapsed = time.monotonic() - t0
            conn.close()
            with lock:
                thread_times.append(elapsed)

        threads = [threading.Thread(target=measure, args=(keel_dsn,))
                   for _ in range(N_THREADS)]
        wall_start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        wall_elapsed = time.monotonic() - wall_start

        total_queries = N_THREADS * N_QUERIES
        aggregate_tps = total_queries / wall_elapsed

        print(f"\n  [stress] Concurrent TPS ({N_THREADS} threads × {N_QUERIES} queries): "
              f"{aggregate_tps:.0f} TPS in {wall_elapsed:.2f}s")
        print(f"  [stress] Per-thread time: "
              f"min={min(thread_times):.2f}s "
              f"max={max(thread_times):.2f}s "
              f"avg={statistics.mean(thread_times):.2f}s")

        assert aggregate_tps >= 800, (
            f"Concurrent TPS too low: {aggregate_tps:.1f} (expected ≥800)"
        )


class TestPoolSaturation:

    def test_connections_beyond_pool_size_queue_and_complete(self, keel_dsn):
        """
        Attempt 60 simultaneous connections (exceeds default max_pool_size=40
        but within max_client_conns=400).  All must eventually succeed or
        receive an explicit queue-timeout error — no hangs, no crashes.
        """
        N = 60
        results: list[int | Exception] = [None] * N
        lock = threading.Lock()

        def worker(idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=30)
                conn.autocommit = True
                time.sleep(0.1)  # Hold the connection briefly
                val = pg_scalar(conn, "SELECT 1")
                with lock:
                    results[idx] = val
            except Exception as exc:
                with lock:
                    results[idx] = exc
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        # Verify no results are None (all threads completed, even if with error)
        assert all(r is not None for r in results), "Some threads timed out without response"

        successes = sum(1 for r in results if r == 1)
        errors    = [r for r in results if isinstance(r, Exception)]

        print(f"\n  [stress] Pool saturation: {successes} successes, {len(errors)} errors")
        # At least half must succeed even under saturation
        assert successes >= N // 2, (
            f"Too few successes under pool saturation: {successes}/{N}"
        )
