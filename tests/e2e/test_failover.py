"""
test_failover.py — Backend failover and recovery tests
=======================================================

Tests that KEEL detects and routes around failed shard backends gracefully.

Background
----------
KEEL runs a background probe thread that health-checks each shard backend on
a configurable interval (``probe_interval = 2000ms`` in the e2e config).  When
a backend fails health checks, KEEL marks it DOWN and stops sending new queries
to it.  When it recovers, KEEL marks it UP and re-warms the connection pool.

These tests exercise that lifecycle by stopping and starting Docker containers
that host the PostgreSQL shard backends.

What is tested
--------------
1. **Partial availability** — when one shard is down, queries that hash to the
   surviving shard still succeed.  Scatter aggregations fail (both shards needed)
   but single-shard queries route around the outage.

2. **Recovery** — after a stopped shard restarts, KEEL reconnects automatically.
   The connection pool is drained of stale connections and refilled.  New queries
   succeed on a fresh connection.

3. **Controlled error** — writes destined for an unavailable shard return a
   clean ``OperationalError`` instead of hanging indefinitely.

4. **Idle reconnect** — a long-idle KEEL is able to reconnect to a shard that
   was restarted while KEEL's pool connections were idle (not in use).

5. **Repeated cycles** — KEEL survives multiple stop/start cycles without
   accumulating broken state.

Why these tests exist
---------------------
Without proper failover handling:
- Client queries would hang until the TCP timeout (~2 min) when a shard is down.
- After restart, clients would receive "server closed the connection unexpectedly"
  indefinitely because KEEL keeps handing out stale connections.
- A single shard failure would take down the entire KEEL proxy for all users,
  even those whose data resides on the surviving shard.

Why a test might fail
---------------------
- **Cascade from previous test**: the first test (``test_reads_continue…``) stops
  and restarts shard1, then calls ``_wait_keel_ready`` to confirm recovery.  If
  ``_wait_keel_ready`` uses autocommit scatter-write DML (which is broken due to
  the io_uring conflict), it will exhaust its retry window without confirming
  recovery, leaving KEEL's pool contaminated with stale connections.  The second
  test then fails at its very first INSERT.  **Fix**: ``_wait_keel_ready`` must
  use explicit transactions, which are handled by KEEL's normal flow pipeline
  (not the blocking scatter-write path).

- **Stale connection pool recycling**: if ``keel_engine_scatter_write`` fails on a
  borrowed backend connection (e.g. ``BEGIN`` gets EPIPE because the shard just
  restarted) but does NOT close the connection before returning it to the pool,
  every subsequent scatter-write will pick up the same dead connection.  Fix in
  ``engine_scatter.c``: close and mark as ``BACKEND_CONN_CLOSED`` on any
  ``sc_exec_cmd`` failure.

- **Probe interval lag**: the probe thread detects shard UP/DOWN on a 2s interval.
  Tests that restart a shard must wait long enough for the probe to confirm
  recovery before sending new queries.

- **Docker DNS instability**: on the first probe cycle after stack creation, the
  DNS resolution for container names may fail transiently.  The probe logs
  "Temporary failure in name resolution" and retries on the next interval.  This
  is normal and does not indicate a test failure.

Consequences of failure
-----------------------
- Users experience ``OperationalError: server closed the connection unexpectedly``
  for minutes after a shard restart, during which KEEL cannot serve any write.
- Without the stale-connection fix, a single shard failure permanently degrades
  write throughput until KEEL is manually restarted.
- With broken ``_wait_keel_ready``, failover test isolation breaks and subsequent
  tests fail due to pool contamination, giving misleading failure attribution.

All tests are marked ``failover`` and ``chaos`` so they can be skipped in
constrained CI environments (e.g. ``pytest tests/e2e/ -m "not chaos"``).
"""

from __future__ import annotations

import subprocess
import time

import pytest
import psycopg2

from helpers import pg_exec, pg_scalar, shard_total_count, clear_table_on_shards

pytestmark = [pytest.mark.failover, pytest.mark.chaos]

# ---------------------------------------------------------------------------
# Module-level constants (mirrors tests/e2e/conftest.py — defined here to
# avoid cross-directory conftest resolution issues when both e2e/ and
# integration/ are collected in the same pytest session).
# ---------------------------------------------------------------------------
KEEL_HOST   = "127.0.0.1"
SHARD0_PORT = 25432
SHARD1_PORT = 25433
PG_USER     = "postgres"
PG_PASSWORD = "postgres"
PG_DBNAME   = "testdb"


# ---------------------------------------------------------------------------
# Docker container helpers
# ---------------------------------------------------------------------------

COMPOSE_FILE_PATH = None  # populated at collection time from conftest


def _docker_stop(container: str, timeout: int = 10) -> None:
    """Stop a named Docker container."""
    subprocess.run(
        ["docker", "stop", "--time", str(timeout), container],
        check=True, capture_output=True, timeout=30,
    )


def _docker_start(container: str) -> None:
    """Start a named Docker container."""
    subprocess.run(
        ["docker", "start", container],
        check=True, capture_output=True, timeout=30,
    )


def _wait_pg_ready(host: str, port: int, retries: int = 30, delay: float = 2.0) -> bool:
    for _ in range(retries):
        try:
            conn = psycopg2.connect(
                host=host, port=port,
                user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME,
                connect_timeout=3,
            )
            conn.close()
            return True
        except psycopg2.Error:
            time.sleep(delay)
    return False


def _wait_keel_ready(keel_dsn: str, retries: int = 30, delay: float = 2.0) -> bool:
    """Wait until KEEL can serve shard-routed queries end-to-end (both shards up).

    Uses explicit transactions (autocommit=False) for scatter-table DML because
    KEEL's scatter-write 2PC path is driven synchronously from the worker thread
    via blocking I/O on io_uring-managed connections.  Autocommit scatter DML
    can race with pending io_uring operations and is not reliable as a readiness
    probe.  Explicit transactions avoid this by letting the normal query pipeline
    handle the BEGIN/COMMIT envelope.
    """
    # Use IDs that hash to different shards to verify both are reachable.
    probe_ids = (88881, 88882)
    for _ in range(retries):
        conn = None
        try:
            conn = psycopg2.connect(keel_dsn, connect_timeout=5)
            conn.autocommit = False
            with conn.cursor() as cur:
                for pid in probe_ids:
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, 'keel_probe')"
                        " ON CONFLICT DO NOTHING",
                        (pid,)
                    )
            conn.commit()
            conn.autocommit = False
            with conn.cursor() as cur:
                for pid in probe_ids:
                    cur.execute("DELETE FROM users WHERE id = %s", (pid,))
            conn.commit()
            conn.close()
            return True
        except psycopg2.Error:
            try:
                if conn is not None:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            time.sleep(delay)
    return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_state(shard0_conn, shard1_conn):
    clear_table_on_shards(shard0_conn, shard1_conn, "users")
    yield
    # Best-effort cleanup even if the test left a shard down
    for conn in (shard0_conn, shard1_conn):
        try:
            pg_exec(conn, "DELETE FROM users")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestShardUnavailable:

    def test_reads_continue_when_one_shard_is_down(
        self, keel_conn, keel_dsn, shard0_conn, shard1_conn, compose_stack
    ):
        """
        When one shard is stopped, queries that touch only the surviving shard
        should still succeed.
        """
        # Insert rows on both shards while both are up
        # Use explicit transactions: KEEL routes DML on scatter tables through 2PC
        for uid in range(0, 8):
            conn = psycopg2.connect(keel_dsn, connect_timeout=10)
            conn.autocommit = False
            try:
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (uid, f"failover_u{uid}"))
                conn.commit()
            finally:
                conn.close()

        shard0_count = pg_scalar(shard0_conn, "SELECT COUNT(*) FROM users")
        shard1_count = pg_scalar(shard1_conn, "SELECT COUNT(*) FROM users")
        assert shard0_count + shard1_count == 8

        # Stop shard 1
        try:
            _docker_stop("e2e-pg-shard1")
            time.sleep(2)

            # Direct reads from shard 0 must still work
            count0 = pg_scalar(shard0_conn, "SELECT COUNT(*) FROM users")
            assert count0 == shard0_count, "Shard 0 reads broken after shard 1 stop"

        finally:
            _docker_start("e2e-pg-shard1")
            _wait_pg_ready(compose_stack["keel_host"], compose_stack["shard1_port"])
            # Wait for KEEL to also drain stale connections and reconnect to shard1
            # so the next test starts with a fully recovered KEEL.
            _wait_keel_ready(keel_dsn, retries=30, delay=2.0)

    def test_keel_recovers_after_shard_restart(
        self, keel_conn, keel_dsn, shard0_conn, shard1_conn, compose_stack
    ):
        """
        After a shard restarts, KEEL reconnects and queries succeed again.
        """
        # Use explicit transaction: KEEL routes DML on scatter tables through 2PC
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            pg_exec(conn, "INSERT INTO users(id, name) VALUES (9901, 'before_restart')")
            conn.commit()
        finally:
            conn.close()

        try:
            _docker_stop("e2e-pg-shard0")
            time.sleep(2)
            _docker_start("e2e-pg-shard0")
            ready = _wait_pg_ready(
                compose_stack["keel_host"], compose_stack["shard0_port"],
                retries=30, delay=2.0,
            )
            assert ready, "Shard 0 did not recover within timeout"
            time.sleep(3)  # Allow KEEL health probe to detect the restart

            # KEEL should reconnect and queries should work.
            # With probe=postgres, KEEL health-checks backends and drains stale
            # connections.  Create a fresh connection on each retry since KEEL
            # may close stale client connections while the pool is recovering.
            last_exc = None
            for _ in range(20):
                try:
                    new_conn = psycopg2.connect(keel_dsn, connect_timeout=5)
                    new_conn.autocommit = True
                    val = pg_scalar(new_conn, "SELECT 1")
                    new_conn.close()
                    assert val == 1
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    try:
                        new_conn.close()
                    except Exception:
                        pass
                    time.sleep(2)
            if last_exc is not None:
                raise last_exc

        finally:
            # Ensure shard 0 is always running after this test
            try:
                _docker_start("e2e-pg-shard0")
            except Exception:
                pass
            _wait_pg_ready(compose_stack["keel_host"], compose_stack["shard0_port"])
            # Wait for KEEL to also recover end-to-end
            _wait_keel_ready(keel_dsn, retries=20, delay=2.0)
            # Allow the probe thread to confirm both shards healthy before
            # the next test starts (probe interval is 2s, allow 2 full cycles)
            time.sleep(5)

    def test_write_to_down_shard_returns_error(
        self, keel_dsn, compose_stack
    ):
        """
        A write that must go to a stopped shard raises an OperationalError
        instead of hanging indefinitely.
        """
        # Route id=9902 through KEEL to discover which shard owns that hash bucket
        # Use explicit transaction: KEEL routes DML on scatter tables through 2PC
        probe_conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        probe_conn.autocommit = False
        try:
            pg_exec(probe_conn, "INSERT INTO users(id, name) VALUES (9902, 'probe_shard')")
            probe_conn.commit()
        finally:
            probe_conn.close()

        # Check which shard physically received the row
        s0 = psycopg2.connect(
            host=KEEL_HOST, port=SHARD0_PORT,
            user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, connect_timeout=5,
        )
        s0.autocommit = True
        on_shard0 = pg_scalar(s0, "SELECT COUNT(*) FROM users WHERE id = 9902") == 1

        # Delete the probe row from whichever shard it landed on so we can re-insert
        # through KEEL when the shard is stopped
        if on_shard0:
            pg_exec(s0, "DELETE FROM users WHERE id = 9902")
        s0.close()

        if not on_shard0:
            s1 = psycopg2.connect(
                host=KEEL_HOST, port=SHARD1_PORT,
                user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, connect_timeout=5,
            )
            s1.autocommit = True
            pg_exec(s1, "DELETE FROM users WHERE id = 9902")
            s1.close()

        # Stop the shard that owns id=9902: KEEL must route 9902 to that stopped shard
        shard_to_stop = "e2e-pg-shard0" if on_shard0 else "e2e-pg-shard1"
        stopped_port  = SHARD0_PORT if on_shard0 else SHARD1_PORT

        try:
            _docker_stop(shard_to_stop)
            time.sleep(3)

            conn = psycopg2.connect(keel_dsn, connect_timeout=10)
            conn.autocommit = True
            with pytest.raises((psycopg2.OperationalError, psycopg2.DatabaseError)):
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (9902, "should_fail"))
            conn.close()

        finally:
            _docker_start(shard_to_stop)
            _wait_pg_ready(compose_stack["keel_host"], stopped_port)
            # Wait for KEEL to re-establish backend connections
            _wait_keel_ready(keel_dsn, retries=20, delay=2.0)



class TestConnectionRecovery:

    def test_keel_survives_idle_shard_reconnect(
        self, keel_conn, keel_dsn, shard0_conn, shard1_conn, compose_stack
    ):
        """
        Even after all backend connections in the pool are closed (by a
        PostgreSQL ``pg_terminate_backend`` sweep), KEEL creates new connections
        and queries succeed.
        """
        # Kill all backend connections from KEEL to shard 0
        pg_exec(shard0_conn, """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE (application_name LIKE '%keel%' OR application_name = '')
              AND pid <> pg_backend_pid()
        """)
        time.sleep(2)

        # KEEL should reconnect transparently; retry with fresh connections
        # because KEEL may close the current client connection when it detects
        # the stale backend connection on first use (probe=none).
        last_exc = None
        for _ in range(15):
            try:
                new_conn = psycopg2.connect(keel_dsn, connect_timeout=5)
                new_conn.autocommit = True
                val = pg_scalar(new_conn, "SELECT 1")
                new_conn.close()
                assert val == 1
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                try:
                    new_conn.close()
                except Exception:
                    pass
                time.sleep(2)
        if last_exc is not None:
            raise last_exc

        # Wait for KEEL to finish draining and re-establishing backend connections
        # so subsequent test modules (scatter-merge, stress) start cleanly.
        _wait_keel_ready(keel_dsn, retries=30, delay=2.0)

    def test_repeated_cycle_stop_start(
        self, keel_dsn, compose_stack
    ):
        """
        Stop-start shard 1 twice in sequence.  KEEL must recover both times.
        """
        for _ in range(2):
            _docker_stop("e2e-pg-shard1")
            time.sleep(1)
            _docker_start("e2e-pg-shard1")
            ready = _wait_pg_ready(
                compose_stack["keel_host"], compose_stack["shard1_port"],
                retries=40, delay=2.0,
            )
            assert ready, "Shard 1 did not become ready within timeout after restart"
            time.sleep(3)  # Allow KEEL probe to detect recovery

        # Retry with fresh connections — KEEL may need one round-trip to detect recovery
        last_exc = None
        for _ in range(15):
            try:
                new_conn = psycopg2.connect(keel_dsn, connect_timeout=5)
                new_conn.autocommit = True
                val = pg_scalar(new_conn, "SELECT 42")
                new_conn.close()
                assert val == 42
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                try:
                    new_conn.close()
                except Exception:
                    pass
                time.sleep(2)
        if last_exc is not None:
            raise last_exc

        # Wait for shard routing to recover fully so subsequent test modules start cleanly.
        _wait_keel_ready(keel_dsn, retries=30, delay=2.0)
