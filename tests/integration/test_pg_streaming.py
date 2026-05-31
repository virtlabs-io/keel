"""test_pg_streaming.py — PostgreSQL streaming replication integration tests.

Tests that a 1-primary + 2-replica streaming replication cluster behaves
correctly: roles are assigned, streaming is active, and writes on the primary
propagate to both replicas.

The compose stack (docker/compose/pg-streaming.yml) is started once per
session by the ``pg_streaming_stack`` fixture in conftest.py.

What is tested
--------------
1. **Primary role** — ``pg_is_in_recovery()`` returns ``false`` on port 5432.
2. **Replica role** — ``pg_is_in_recovery()`` returns ``true`` on ports 5433
   and 5434.
3. **Streaming active** — ``pg_stat_replication`` on the primary contains at
   least 2 rows in ``streaming`` state.
4. **Replication lag** — The write-ahead log is not excessively behind on
   either replica (``pg_last_wal_receive_lsn`` is not NULL).
5. **Write propagation** — A row inserted on the primary becomes visible on
   both replicas within a short window.
"""

from __future__ import annotations

import time

import pytest

psycopg2 = pytest.importorskip("psycopg2", reason="psycopg2 not installed — skipping streaming tests")

pytestmark = [pytest.mark.streaming, pytest.mark.timeout(300)]

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

_TEST_TABLE = "keel_stream_probe"


def _scalar(conn: psycopg2.extensions.connection, sql: str, params=()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row else None


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

class TestPgStreamingReplication:
    """Verify the pg-streaming compose cluster is healthy and replicating."""

    def test_primary_is_not_in_recovery(self, primary_conn):
        """Port 5432 must be the primary (pg_is_in_recovery = false)."""
        in_recovery = _scalar(primary_conn, "SELECT pg_is_in_recovery()")
        assert in_recovery is False, (
            f"Expected primary on port 5432 to report pg_is_in_recovery()=false, got {in_recovery!r}"
        )

    def test_replica1_is_in_recovery(self, replica1_conn):
        """Port 5433 must be a standby (pg_is_in_recovery = true)."""
        in_recovery = _scalar(replica1_conn, "SELECT pg_is_in_recovery()")
        assert in_recovery is True, (
            f"Expected replica1 on port 5433 to report pg_is_in_recovery()=true, got {in_recovery!r}"
        )

    def test_replica2_is_in_recovery(self, replica2_conn):
        """Port 5434 must be a standby (pg_is_in_recovery = true)."""
        in_recovery = _scalar(replica2_conn, "SELECT pg_is_in_recovery()")
        assert in_recovery is True, (
            f"Expected replica2 on port 5434 to report pg_is_in_recovery()=true, got {in_recovery!r}"
        )

    def test_streaming_replication_active(self, primary_conn):
        """Primary must have at least 2 replicas in streaming state."""
        count = _scalar(
            primary_conn,
            "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'",
        )
        assert int(count) >= 2, (
            f"Expected ≥ 2 streaming replicas, found {count}"
        )

    def test_replica_wal_receive_lsn_not_null(self, replica1_conn, replica2_conn):
        """Replicas must report a received WAL position (not NULL)."""
        for name, conn in [("replica1", replica1_conn), ("replica2", replica2_conn)]:
            lsn = _scalar(conn, "SELECT pg_last_wal_receive_lsn()")
            assert lsn is not None, (
                f"{name}: pg_last_wal_receive_lsn() is NULL — replica may not be streaming"
            )

    def test_write_propagates_to_replicas(
        self, primary_conn, replica1_conn, replica2_conn
    ):
        """A row written on the primary must be visible on both replicas."""
        # Create a probe table on the primary (idempotent)
        with primary_conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {_TEST_TABLE}"
                " (id serial PRIMARY KEY, val text)"
            )

        try:
            with primary_conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {_TEST_TABLE} (val) VALUES (%s)",
                    ("stream_probe",),
                )

            # Give replicas time to apply the WAL
            time.sleep(2)

            for name, conn in [("replica1", replica1_conn), ("replica2", replica2_conn)]:
                count = _scalar(
                    conn,
                    f"SELECT count(*) FROM {_TEST_TABLE} WHERE val = %s",
                    ("stream_probe",),
                )
                assert int(count) >= 1, (
                    f"{name}: probe row not visible after 2 s — replication lag?"
                )
        finally:
            # Best-effort cleanup; table lives in the ephemeral compose volume anyway
            try:
                with primary_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {_TEST_TABLE}")
            except Exception:
                pass
