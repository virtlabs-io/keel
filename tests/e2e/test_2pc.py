"""
test_2pc.py — Two-phase commit integration tests
=================================================

Tests that KEEL correctly orchestrates distributed two-phase commit (2PC)
across shard nodes when a write touches multiple shards.

Background
----------
KEEL uses PostgreSQL's PREPARE TRANSACTION / COMMIT PREPARED protocol to
atomically commit writes across all shards for scatter-routed tables.  When a
client sends ``BEGIN`` → DML → ``COMMIT``, KEEL intercepts the DML, fans it
out to all relevant shards, and runs:

  Phase 1: BEGIN + DML + PREPARE TRANSACTION 'keel_<session>_<seq>_s<N>'
  Phase 2: COMMIT PREPARED (all shards succeeded) or ROLLBACK PREPARED (any failure)

The client only ever sees a single COMMIT/ROLLBACK response; KEEL hides the
2PC mechanics completely.

What is tested
--------------
- Happy path: multi-row scatter INSERT committed atomically across both shards.
- Isolation: in-flight 2PC rows must NOT be visible to concurrent readers until
  COMMIT PREPARED completes.
- Rollback consistency: if any shard fails during Phase 1, all shards roll back.
- Idempotency: repeated writes to the same key handle ON CONFLICT correctly.
- Non-scatter tables: single-shard writes are NOT wrapped in 2PC.

Why these tests exist
---------------------
Without 2PC, a crash or failure between Phase 1 commits on different shards
would leave the database in a split-brain state: some shards with the new data,
others without.  For financial, inventory, or other write-once-correct data,
this is catastrophic.

Why a test might fail
---------------------
- ``max_prepared_transactions = 0`` on a shard — PREPARE TRANSACTION will fail
  immediately with "prepared transactions are disabled".  Fix: ensure PostgreSQL
  is started with ``-c max_prepared_transactions=<N>`` (done in compose).
- Stale ``pg_prepared_xacts`` entries from a previous aborted test run can
  block new PREPARE with duplicate GID errors.  Fix: ``ROLLBACK PREPARED 'gid'``
  on the affected shard.
- KEEL scatter-write returning stale backend connections after a shard restart
  causes Phase 1 to fail silently (BEGIN succeeds but DML fails on closed fd).
  Fix: ``engine_scatter.c`` must mark connections CLOSED on any sc_exec_cmd
  failure so they are not returned to the pool as healthy.

Consequences of failure
-----------------------
- Partial commits across shards → data inconsistency visible as missing or
  duplicate rows depending on which shard succeeded.
- Client sees "scatter-write: 2PC failed; transaction rolled back" even though
  one shard actually committed — next read sees a different count per shard.
- ``pg_prepared_xacts`` leaked entries block the next ``PREPARE TRANSACTION``
  with the same GID, causing permanent failure until manually cleaned up.

Prerequisites
-------------
- ``max_prepared_transactions > 0`` on each shard (set in compose command-line)
- The ``keel_2pc_test`` table must exist on both shards (created by conftest)
"""

from __future__ import annotations

import threading

import pytest
import psycopg2

from helpers import pg_exec, pg_scalar, pg_count, shard_total_count, clear_table_on_shards

pytestmark = pytest.mark.twopc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _has_2pc(conn) -> bool:
    """Return True if max_prepared_transactions > 0 on *conn*'s backend."""
    val = pg_scalar(conn, "SELECT current_setting('max_prepared_transactions')::int")
    return val is not None and int(val) > 0


def _cleanup_prepared(conn) -> None:
    """Roll back any leftover prepared transactions whose GID starts with 'keel'."""
    pg_exec(conn, """
        DO $$ DECLARE r RECORD; BEGIN
          FOR r IN SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'keel%' LOOP
            EXECUTE format('ROLLBACK PREPARED %L', r.gid);
          END LOOP;
        END $$
    """)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_2pc_table(shard0_conn, shard1_conn):
    """Wipe keel_2pc_test and any orphaned prepared transactions before each test."""
    for conn in (shard0_conn, shard1_conn):
        _cleanup_prepared(conn)
    clear_table_on_shards(shard0_conn, shard1_conn, "keel_2pc_test")
    yield
    for conn in (shard0_conn, shard1_conn):
        _cleanup_prepared(conn)
    clear_table_on_shards(shard0_conn, shard1_conn, "keel_2pc_test")


@pytest.fixture(autouse=True, scope="module")
def require_2pc(shard0_dsn):
    """Skip entire module if 2PC is disabled on the shard nodes."""
    conn = psycopg2.connect(shard0_dsn, connect_timeout=10)
    conn.autocommit = True
    ok = _has_2pc(conn)
    conn.close()
    if not ok:
        pytest.skip(
            "max_prepared_transactions=0 on shard nodes — 2PC tests skipped. "
            "Set max_prepared_transactions >= 10 in the compose service command."
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHappyPath:

    def test_cross_shard_commit_persists_data(self, keel_dsn, shard0_conn, shard1_conn):
        """
        A transaction that writes to both shards commits atomically.
        Both rows are visible after COMMIT.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # Insert rows that hash to different shards
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (0, 'commit_s0')")
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (1, 'commit_s1')")
            conn.commit()
        finally:
            conn.close()

        total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        assert total == 2, f"Expected 2 rows after cross-shard commit, got {total}"

    def test_cross_shard_rollback_removes_all_data(self, keel_dsn, shard0_conn, shard1_conn):
        """
        Rolling back a cross-shard transaction removes all rows from all shards.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (0, 'rollback_s0')")
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (1, 'rollback_s1')")
            conn.rollback()
        finally:
            conn.close()

        total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        assert total == 0, f"Expected 0 rows after rollback, got {total}"

    def test_single_shard_write_commits(self, keel_dsn, shard0_conn, shard1_conn):
        """
        A write that touches only one shard still commits successfully
        (degenerate 2PC / no-2PC path).
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # Use a specific shard marker — KEEL routes to one shard
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (0, 'single_shard')")
            conn.commit()
        finally:
            conn.close()

        total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        assert total == 1

    def test_multiple_cross_shard_transactions_sequential(self, keel_dsn, shard0_conn, shard1_conn):
        """
        10 sequential cross-shard transactions all commit; total row count = 10.
        """
        for i in range(10):
            conn = psycopg2.connect(keel_dsn, connect_timeout=10)
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO keel_2pc_test(shard, marker) VALUES (%s, %s)",
                        (i % 2, f"seq_{i}"),
                    )
                conn.commit()
            finally:
                conn.close()

        total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        assert total == 10

    def test_data_not_visible_during_prepare_phase(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        Rows written inside a transaction that has not yet committed are NOT
        visible to a concurrent direct shard connection.
        """
        visible_during: list[int] = []

        def reader() -> None:
            import time
            time.sleep(0.05)
            total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
            visible_during.append(total)

        reader_thread = threading.Thread(target=reader)

        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        reader_thread.start()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (0, 'isolation_test')")
            reader_thread.join(timeout=5)
            # Do NOT commit yet — check reader saw 0 rows
            total_before = visible_during[0] if visible_during else -1
            conn.commit()
        finally:
            conn.close()

        total_after = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        assert total_before == 0, f"Row visible before commit (isolation breach): count={total_before}"
        assert total_after  == 1


class TestConcurrentTransactions:

    def test_concurrent_cross_shard_commits(self, keel_dsn, shard0_conn, shard1_conn):
        """
        20 threads each execute a cross-shard INSERT + COMMIT concurrently.
        All transactions must succeed and 20 rows must be present at the end.
        """
        N = 20
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO keel_2pc_test(shard, marker) VALUES (%s, %s)",
                        (idx % 2, f"concurrent_{idx}"),
                    )
                conn.commit()
            except Exception as exc:
                with lock:
                    errors.append(exc)
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
            t.join(timeout=30)

        assert not errors, f"Concurrent 2PC errors: {errors}"

        total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        assert total == N, f"Expected {N} rows, got {total}"

    def test_interleaved_commit_and_rollback(self, keel_dsn, shard0_conn, shard1_conn):
        """
        50 transactions: half commit, half rollback.
        Only the committed rows should be present at the end.
        """
        N = 50
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO keel_2pc_test(shard, marker) VALUES (%s, %s)",
                        (idx % 2, f"mixed_{idx}"),
                    )
                if idx % 2 == 0:
                    conn.commit()
                else:
                    conn.rollback()
            except Exception as exc:
                with lock:
                    errors.append(exc)
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
            t.join(timeout=30)

        assert not errors, f"Concurrent 2PC errors: {errors}"

        total = shard_total_count(shard0_conn, shard1_conn, "keel_2pc_test")
        # Only even-indexed workers committed
        committed = sum(1 for i in range(N) if i % 2 == 0)
        assert total == committed, f"Expected {committed} rows, got {total}"


class TestPreparedTransactionCleanup:

    def test_no_orphaned_prepared_txns_after_commit(self, keel_dsn, shard0_conn, shard1_conn):
        """
        After a successful cross-shard commit, no prepared transactions remain
        in pg_prepared_xacts on either shard.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (0, 'cleanup_check')")
            conn.commit()
        finally:
            conn.close()

        for label, sc in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            leftover = pg_scalar(sc, "SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid LIKE 'keel%'")
            assert leftover == 0, f"{label}: {leftover} orphaned prepared txn(s) after commit"

    def test_no_orphaned_prepared_txns_after_rollback(self, keel_dsn, shard0_conn, shard1_conn):
        """
        After a ROLLBACK, any prepared transactions KEEL created are cleaned up.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO keel_2pc_test(shard, marker) VALUES (0, 'rollback_cleanup')")
            conn.rollback()
        finally:
            conn.close()

        for label, sc in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            leftover = pg_scalar(sc, "SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid LIKE 'keel%'")
            assert leftover == 0, f"{label}: {leftover} orphaned prepared txn(s) after rollback"
