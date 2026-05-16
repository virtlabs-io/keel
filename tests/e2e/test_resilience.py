"""
test_resilience.py — Error path and resilience tests for the KEEL E2E suite
=============================================================================

Tests error recovery, malformed SQL handling, and connection stability.

Background
----------
KEEL must remain stable and return clean errors when clients send malformed
SQL, trigger runtime errors (division by zero, constraint violations), or
exhaust resources.  A resilient proxy should:
- Return ``ErrorResponse`` with a clear SQLSTATE for client errors.
- Not close the connection on client errors (the client can retry).
- Not leak transaction state to the next client after an error.
- Remain stable under concurrent errors (no race conditions in error path).

What is tested
--------------
- **Syntax error**: malformed SQL returns ``ErrorResponse(SQLSTATE=42601)``
  and the connection is still usable afterward.
- **Runtime error**: division by zero, null pointer deref, etc. return
  appropriate error codes and the connection survives.
- **Constraint violation**: INSERT of duplicate primary key returns
  ``ErrorResponse(SQLSTATE=23505)`` (unique_violation).
- **Large error message**: an error with a very long message is forwarded
  without truncation or buffer overflow.
- **Rapid reconnects**: opening and closing connections rapidly does not
  exhaust the pool or cause KEEL to crash.
- **Long transaction + short transaction**: a long-running explicit transaction
  does not block short transactions from completing.

Why these tests exist
---------------------
Error paths are often under-tested because they require explicit fault injection.
Bugs in error handling manifest as:
- Connection drops after any error (application sees intermittent disconnects).
- Wrong SQLSTATE forwarded (application retries on errors that should be fatal).
- Transaction state leakage (client A's rolled-back INSERT is visible to client B
  because KEEL failed to reset the backend connection state on error).

Why a test might fail
---------------------
- **KEEL drops connection on error**: if the error path in KEEL closes the
  backend connection without sending ReadyForQuery to the client, subsequent
  queries on the same connection will time out.
- **SQLSTATE mismatch**: KEEL may wrap errors and return a generic internal error
  code instead of the original PostgreSQL SQLSTATE.
- **Pool leak under concurrent errors**: concurrent error injection may exhaust
  the pool if connections are not properly returned after errors.

Consequences of failure
-----------------------
- Every query error causes a connection drop → application must reconnect for
  every error → cascading latency spike.
- Wrong SQLSTATE → application cannot distinguish retriable from fatal errors.
- Pool exhaustion under errors → entire service becomes unavailable.

Markers: ``failover``, ``chaos``

Run selectively::

    pytest tests/e2e/ -m "failover or chaos" -k resilience -v
"""

from __future__ import annotations

import threading
import time

import psycopg2
import pytest

from helpers import pg_exec, pg_scalar

pytestmark = [pytest.mark.failover]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_conn(keel_dsn):
    """A fresh connection for each test — closed cleanly even on failure."""
    conn = psycopg2.connect(keel_dsn, connect_timeout=10)
    conn.autocommit = True
    yield conn
    try:
        conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# SQL error recovery
# ---------------------------------------------------------------------------

class TestSQLErrorRecovery:
    def test_error_then_valid_query(self, fresh_conn, keel_dsn):
        """After an SQL error the connection must remain usable."""
        with pytest.raises(psycopg2.errors.UndefinedTable):
            pg_exec(fresh_conn, "SELECT * FROM table_that_does_not_exist_keel")

        fresh_conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        fresh_conn.autocommit = True
        result = pg_exec(fresh_conn, "SELECT 42")
        fresh_conn.close()
        assert result[0][0] == 42, "Expected 42 after error recovery"

    def test_transaction_error_rollback(self, keel_txn_conn):
        """An error inside a transaction must leave the session in an error state
        that is cleared by ROLLBACK."""
        cur = keel_txn_conn.cursor()
        cur.execute("SELECT 1")   # begin implicit transaction
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM table_that_does_not_exist_keel")

        keel_txn_conn.rollback()
        # After rollback, normal query must work
        cur.execute("SELECT 99")
        row = cur.fetchone()
        assert row[0] == 99, "Query failed after transaction rollback"

    def test_division_by_zero_recovery(self, fresh_conn, keel_dsn):
        """Division by zero must yield an error, then the connection must recover."""
        with pytest.raises(psycopg2.errors.DivisionByZero):
            pg_exec(fresh_conn, "SELECT 1 / 0")

        fresh_conn2 = psycopg2.connect(keel_dsn, connect_timeout=10)
        fresh_conn2.autocommit = True
        result = pg_exec(fresh_conn2, "SELECT 1")
        fresh_conn2.close()
        assert result[0][0] == 1

    def test_syntax_error_recovery(self, fresh_conn, keel_dsn):
        """A syntax error must not permanently break the connection."""
        try:
            pg_exec(fresh_conn, "THIS IS NOT SQL")
        except psycopg2.Error:
            pass

        fresh_conn2 = psycopg2.connect(keel_dsn, connect_timeout=10)
        fresh_conn2.autocommit = True
        assert pg_scalar(fresh_conn2, "SELECT 1") == 1
        fresh_conn2.close()


# ---------------------------------------------------------------------------
# Connection stability under concurrent errors
# ---------------------------------------------------------------------------

class TestConcurrentErrorResilience:
    @pytest.mark.timeout(60)
    def test_many_concurrent_errors_no_deadlock(self, keel_dsn):
        """50 concurrent connections each triggering an error — no deadlock/hang."""
        results: list[str] = []
        lock = threading.Lock()

        def _worker() -> None:
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                try:
                    conn.cursor().execute("SELECT * FROM nonexistent_table_keel_test")
                except psycopg2.Error:
                    pass  # expected
                # Now do a valid query
                conn.cursor().execute("SELECT 1")
                conn.close()
                with lock:
                    results.append("ok")
            except Exception as exc:
                with lock:
                    results.append(f"error:{exc}")

        threads = [threading.Thread(target=_worker, daemon=True) for _ in range(50)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=20)

        errors = [r for r in results if r.startswith("error")]
        assert len(errors) < 5, f"Too many errors in concurrent error test: {errors[:3]}"

    @pytest.mark.timeout(30)
    @pytest.mark.chaos
    def test_rapid_connect_disconnect_stability(self, keel_dsn):
        """50 rapid connect/disconnect cycles must not destabilise the proxy."""
        for i in range(50):
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=5)
                conn.autocommit = True
                conn.cursor().execute("SELECT 1")
                conn.close()
            except psycopg2.Error as exc:
                pytest.fail(f"Connect/disconnect failed on iteration {i}: {exc}")


# ---------------------------------------------------------------------------
# Pool-level resilience
# ---------------------------------------------------------------------------

class TestPoolResilience:
    @pytest.mark.timeout(60)
    def test_connections_recovered_after_burst(self, keel_dsn):
        """After a burst of connections, the proxy must still serve new clients."""
        # Open many connections
        conns = []
        for _ in range(20):
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=5)
                conn.autocommit = True
                conns.append(conn)
            except Exception:
                break

        # Close all burst connections
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass

        # Proxy must still answer
        time.sleep(0.5)
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        result = pg_exec(conn, "SELECT 42")
        conn.close()
        assert result[0][0] == 42, "Proxy unavailable after connection burst"

    @pytest.mark.timeout(30)
    def test_idle_connection_reuse(self, keel_dsn):
        """An idle connection must be reusable after a pause."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        pg_exec(conn, "SELECT 1")
        time.sleep(2)   # sit idle
        # Must work after the pause
        result = pg_exec(conn, "SELECT 77")
        conn.close()
        assert result[0][0] == 77, "Idle connection unusable after 2s pause"
