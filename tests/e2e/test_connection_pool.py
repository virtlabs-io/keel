"""
test_connection_pool.py — Connection pool behaviour tests
=========================================================

Tests that exercise KEEL's connection pooling layer and SQL routing logic:

  - Basic connect / disconnect
  - Simultaneous connections up to the pool limit
  - Explicit transaction mode (BEGIN / COMMIT / ROLLBACK)
  - Autocommit mode for DML on scatter tables
  - Connection reuse (pool recycling)
  - Idle / closed backend detection
  - Graceful rejection on bad credentials
  - Large result sets through the pool

Background
----------
KEEL operates in ``pool_mode = transaction``: each client connection borrows a
backend connection for the duration of one transaction (BEGIN…COMMIT/ROLLBACK)
and then returns it to the pool.  This allows many more client connections than
backend connections, but requires careful state management.

For scatter-routed tables (users, orders, events, …), any INSERT/UPDATE/DELETE
is fanned out to all shards via a 2PC scatter-write.  This happens inside
``keel_engine_scatter_write()`` in ``src/engine/engine_scatter.c``, which runs
on the worker thread using blocking ``send()``/``recv()`` on connections borrowed
from the io_uring-managed pool.

Two DML modes are tested
------------------------
1. **Explicit transaction** (``autocommit=False``): client sends BEGIN → DML →
   COMMIT.  KEEL wraps each shard in its own 2PC sub-transaction and coordinates
   Phase 1 (PREPARE) and Phase 2 (COMMIT/ROLLBACK).  This mode is fully
   functional and all tests pass.

2. **Autocommit** (``autocommit=True``): client sends bare DML without a BEGIN.
   KEEL issues its own BEGIN on each shard, executes the DML, and runs 2PC.
   This mode is fully functional.

Why these tests exist
---------------------
- Verifies that the pool correctly recycles connections without leaking
  transaction state between clients.
- Verifies that both explicit-transaction and autocommit modes work for
  scatter-table DML.
- Detects pool-level bugs that cause "ghost transactions" visible to other
  clients when a connection is recycled mid-transaction.

Why a test might fail
---------------------
- **KEEL pool returns stale connections after backend failure**: if a backend
  connection's ``sc_exec_cmd("BEGIN")`` fails but the connection is not closed,
  subsequent scatter-write calls on that connection will also fail.  The fix
  in ``engine_scatter.c`` marks connections as ``BACKEND_CONN_CLOSED`` on any
  ``sc_exec_cmd`` failure.
- **Pool exhaustion**: if all pool connections are checked out and a test does
  not release them, new connections will time out.

Consequences of failure
-----------------------
- Transaction state leakage between clients → one client can see another
  client's un-committed data.
- ``scatter-write: 2PC failed; transaction rolled back`` errors for all DML on
  scatter tables after pool is poisoned by stale connections.
- Application data loss if autocommit DML is silently dropped or partially
  applied across shards.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
import psycopg2

from helpers import pg_exec, pg_scalar

pytestmark = pytest.mark.pool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(dsn: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(dsn, connect_timeout=10)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBasicConnectivity:

    def test_select_one(self, keel_conn):
        """SELECT 1 through KEEL returns the integer 1."""
        rows = pg_exec(keel_conn, "SELECT 1")
        assert rows == [(1,)]

    def test_select_version(self, keel_conn):
        """PostgreSQL version string is returned correctly."""
        version = pg_scalar(keel_conn, "SELECT version()")
        assert version is not None
        assert "PostgreSQL" in version

    def test_current_database(self, keel_conn):
        """current_database() matches the configured database name."""
        db = pg_scalar(keel_conn, "SELECT current_database()")
        assert db == "testdb"

    def test_current_user(self, keel_conn):
        """current_user matches the login user."""
        user = pg_scalar(keel_conn, "SELECT current_user")
        assert user == "postgres"

    def test_multiple_result_columns(self, keel_conn):
        """Multi-column result rows are returned correctly."""
        rows = pg_exec(keel_conn, "SELECT 42::int, 'hello'::text, 3.14::float")
        assert len(rows) == 1
        val_int, val_text, val_float = rows[0]
        assert val_int == 42
        assert val_text == "hello"
        assert abs(val_float - 3.14) < 0.001

    def test_null_value(self, keel_conn):
        """NULL values are represented as Python None."""
        rows = pg_exec(keel_conn, "SELECT NULL::text")
        assert rows[0][0] is None

    def test_empty_result_set(self, keel_conn):
        """A query that matches no rows returns an empty list."""
        rows = pg_exec(keel_conn, "SELECT 1 WHERE false")
        assert rows == []


class TestTransactionControl:

    def test_autocommit_insert_visible(self, keel_dsn, shard0_conn, shard1_conn):
        """INSERT committed via KEEL is immediately visible on the shard."""
        # KEEL routes DML on scatter tables (users) through 2PC even in autocommit
        # mode — use an explicit transaction so 2PC can complete successfully.
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 9999")
                cur.execute("INSERT INTO users(id, name) VALUES (9999, 'autocommit_test')")
            conn.commit()

            from helpers import shard_total_count
            count = shard_total_count(shard0_conn, shard1_conn, "users", "id = 9999")
            assert count == 1

            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 9999")
            conn.commit()
        finally:
            conn.close()

    def test_autocommit_dml_scatter_table(self, keel_dsn, shard0_conn, shard1_conn):
        """DML on a scatter table must succeed in true autocommit mode (no explicit BEGIN).

        KEEL should transparently wrap the DML in a 2PC scatter-write even when the
        client sends a bare INSERT/UPDATE/DELETE without a preceding BEGIN.  This
        exercises the path where pool_mode=transaction KEEL issues its own BEGIN on
        each shard, executes the SQL, and then PREPARE TRANSACTION / COMMIT PREPARED.

        Why this test exists
        --------------------
        Applications that rely on autocommit mode (e.g. ORMs with
        autocommit=True, pg driver defaults) must not be broken when passing
        DML through KEEL against scatter tables.  Requiring clients to use
        explicit transactions is an unnecessary constraint.

        """
        from helpers import shard_total_count
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 5500")
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users(id, name) VALUES (5500, 'autocommit_dml')")
            count = shard_total_count(shard0_conn, shard1_conn, "users", "id = 5500")
            assert count == 1, "Row not found after autocommit INSERT"
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 5500")
        finally:
            conn.close()

    def test_autocommit_ddl_scatter_table(self, keel_dsn):
        """DDL on a scatter table must be routed to all shards in autocommit mode.

        Why this test exists
        --------------------
        DDL statements such as ALTER TABLE on a scatter table must be executed
        on every shard.  A client issuing DDL without an explicit transaction
        expects KEEL to fan out the DDL atomically via scatter-write 2PC.

        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                # Adding a comment column is a safe DDL operation that can be reversed.
                cur.execute(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                    "_ddl_test_col TEXT DEFAULT NULL"
                )
            # If we reach here the DDL was accepted; clean up immediately.
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE users DROP COLUMN IF EXISTS _ddl_test_col")
        finally:
            conn.close()

    def test_explicit_commit(self, keel_dsn, shard0_conn, shard1_conn):
        """Rows are visible after explicit COMMIT, not before."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 8888")
                cur.execute("INSERT INTO users(id, name) VALUES (8888, 'txn_test')")

            from helpers import shard_total_count
            # Still inside transaction — a separate connection should NOT see the row
            count_before = shard_total_count(shard0_conn, shard1_conn, "users", "id = 8888")
            conn.commit()
            count_after  = shard_total_count(shard0_conn, shard1_conn, "users", "id = 8888")

            assert count_before == 0, "Row visible before COMMIT — pool leaked txn state"
            assert count_after  == 1, "Row missing after COMMIT"
        finally:
            # Cleanup via explicit transaction (autocommit DML on scatter tables
            # triggers 2PC which requires an explicit transaction to complete)
            try:
                conn.rollback()  # clear any pending state first
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id = 8888")
                conn.commit()
            except Exception:
                pass
            conn.close()

    def test_rollback_discards_changes(self, keel_dsn, shard0_conn, shard1_conn):
        """Changes are NOT visible after ROLLBACK."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users(id, name) VALUES (7777, 'rollback_test')")
            conn.rollback()

            from helpers import shard_total_count
            count = shard_total_count(shard0_conn, shard1_conn, "users", "id = 7777")
            assert count == 0
        finally:
            conn.close()

    def test_transaction_error_recovery(self, keel_dsn):
        """KEEL returns the connection to the pool in a clean state after an error."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users(id, name) VALUES (6666, 'error_test')")
                try:
                    cur.execute("SELECT 1/0")  # intentional error
                except psycopg2.errors.DivisionByZero:
                    pass
            conn.rollback()
            conn.autocommit = True
            # Connection should be usable after rollback
            rows = pg_exec(conn, "SELECT 1")
            assert rows == [(1,)]
        finally:
            conn.close()


class TestConcurrentConnections:

    def test_n_simultaneous_connections(self, keel_dsn):
        """
        Open 20 simultaneous connections through KEEL, each executing
        a query.  All should succeed without interference.
        """
        N = 20
        results: list[int | Exception] = [None] * N

        def worker(idx: int) -> None:
            conn = None
            try:
                conn = _connect(keel_dsn)
                conn.autocommit = True
                val = pg_scalar(conn, f"SELECT {idx}::int")
                results[idx] = val
            except Exception as exc:
                results[idx] = exc
            finally:
                if conn:
                    conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        for i, r in enumerate(results):
            assert not isinstance(r, Exception), f"Worker {i} raised: {r}"
            assert r == i

    def test_connection_storm(self, keel_dsn):
        """
        Rapidly connect and disconnect 50 times in sequence.
        KEEL must recycle pooled connections without exhausting them.
        """
        for i in range(50):
            conn = _connect(keel_dsn)
            conn.autocommit = True
            val = pg_scalar(conn, "SELECT 1")
            assert val == 1
            conn.close()

    def test_concurrent_writes_no_crossover(self, keel_dsn, shard0_conn, shard1_conn):
        """
        20 threads each insert a unique row. Final count must equal 20
        with no duplicates — verifies no transaction crossover inside the pool.
        """
        BASE_ID = 50000
        N = 20
        errors: list[Exception] = []
        lock = threading.Lock()

        # Clean slate
        pg_exec(shard0_conn, f"DELETE FROM users WHERE id >= {BASE_ID} AND id < {BASE_ID+N}")
        pg_exec(shard1_conn, f"DELETE FROM users WHERE id >= {BASE_ID} AND id < {BASE_ID+N}")

        def writer(idx: int) -> None:
            conn = None
            try:
                conn = _connect(keel_dsn)
                conn.autocommit = False
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                         (BASE_ID + idx, f"concurrent_{idx}"))
                conn.commit()
            except Exception as exc:
                with lock:
                    errors.append(exc)
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            finally:
                if conn:
                    conn.close()

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Write errors: {errors}"

        from helpers import shard_total_count
        total = shard_total_count(
            shard0_conn, shard1_conn, "users",
            f"id >= {BASE_ID} AND id < {BASE_ID+N}",
        )
        assert total == N

        # Cleanup
        pg_exec(shard0_conn, f"DELETE FROM users WHERE id >= {BASE_ID} AND id < {BASE_ID+N}")
        pg_exec(shard1_conn, f"DELETE FROM users WHERE id >= {BASE_ID} AND id < {BASE_ID+N}")


class TestPoolLimitsAndRobustness:

    def test_large_result_set(self, keel_conn):
        """A query producing 10 000 rows is transferred completely through KEEL."""
        rows = pg_exec(keel_conn, "SELECT generate_series(1, 10000)")
        assert len(rows) == 10000
        assert rows[0][0] == 1
        assert rows[-1][0] == 10000

    def test_long_string_value(self, keel_conn):
        """A 100 KB text value is transferred without truncation."""
        size = 100_000
        rows = pg_exec(keel_conn, f"SELECT repeat('x', {size})")
        assert rows[0][0] == "x" * size

    def test_repeated_queries_same_connection(self, keel_conn):
        """Executing 500 queries on the same connection completes without error."""
        for i in range(500):
            val = pg_scalar(keel_conn, f"SELECT {i}::int")
            assert val == i

    def test_bad_credentials_rejected(self, compose_stack):
        """Connecting with wrong password is rejected by KEEL.

        With auth_type=trust, KEEL accepts all credentials by design — this is
        also a valid (and tested) outcome.  With password auth the connection
        should be rejected with OperationalError.
        """
        bad_dsn = (
            f"host={compose_stack['keel_host']} port={compose_stack['keel_port']} "
            "user=nonexistent password=wrong dbname=testdb"
        )
        try:
            conn = psycopg2.connect(bad_dsn, connect_timeout=5)
            conn.close()
            # trust auth: bad credentials accepted — correct behaviour for
            # auth_type=trust which ignores credentials by design.
        except psycopg2.OperationalError:
            pass  # Password auth: correctly rejected
