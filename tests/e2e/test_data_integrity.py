"""test_data_integrity.py — Data integrity under concurrent load.

Ensures that KEEL correctly preserves data consistency across all operations:
inserts, updates, deletes, transactions, and cross-shard 2PC commits.

What is tested
--------------
1. **Concurrent insert integrity** — 10 threads each insert 100 unique rows.
   After all threads finish, ``SELECT COUNT(*)`` via KEEL (scatter) must equal
   the number of rows visible by summing direct counts on each shard.

2. **Scatter aggregate matches direct sum** — ``SELECT COUNT(*)`` through KEEL
   must equal ``shard0_count + shard1_count`` at all times.

3. **Update integrity** — batch UPDATE changes every target row; subsequent
   scatter SELECT with that filter must return the correct updated count.

4. **Delete integrity** — batch DELETE removes exactly the target rows; scatter
   COUNT afterwards is 0 for those rows.

5. **Rollback leaves no trace** — rows INSERTed inside a rolled-back
   transaction must not appear on any shard.

6. **2PC cross-shard atomicity (commit)** — after PREPARE/COMMIT PREPARED on
   both shards, both shards hold the expected rows.

7. **2PC cross-shard atomicity (rollback)** — after PREPARE/ROLLBACK PREPARED
   on both shards, neither shard holds the rows.

8. **Idempotent double-delete** — deleting an already-absent key must not
   raise an error.

9. **Read-your-writes within a session** — after INSERT via KEEL, the same
   connection must read the row back immediately (no dirty-read or lag issue
   within the proxy).
"""

from __future__ import annotations

import threading
import uuid

import psycopg2
import pytest

from helpers import pg_exec, pg_scalar, pg_count

pytestmark = [pytest.mark.integrity]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE = 9_000_000         # ID namespace for this test module
_THREADS = 10
_ROWS_PER_THREAD = 100
_TOTAL_ROWS = _THREADS * _ROWS_PER_THREAD


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scatter_count(conn, table: str, id_from: int, id_to: int) -> int:
    """COUNT(*) via KEEL (scatter query — no shard key in WHERE)."""
    row = pg_scalar(conn,
                    f"SELECT count(*) FROM {table} "
                    f"WHERE id BETWEEN %s AND %s",
                    (id_from, id_to))
    return int(row or 0)


def _direct_count(conn, table: str, id_from: int, id_to: int) -> int:
    """COUNT(*) directly on one shard."""
    row = pg_scalar(conn,
                    f"SELECT count(*) FROM {table} "
                    f"WHERE id BETWEEN %s AND %s",
                    (id_from, id_to))
    return int(row or 0)


def _cleanup_range(conn, table: str, id_from: int, id_to: int) -> None:
    pg_exec(conn,
            f"DELETE FROM {table} WHERE id BETWEEN %s AND %s",
            (id_from, id_to))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConcurrentInsertIntegrity:
    """Multi-threaded inserts must all land exactly once with correct total."""

    def test_concurrent_inserts_total_matches(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """10 threads × 100 inserts: scatter COUNT via KEEL == 1000."""
        base = _BASE

        # Cleanup any leftover rows from previous runs
        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + _TOTAL_ROWS - 1))

        errors: list[str] = []

        def _insert_batch(thread_id: int) -> None:
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=15)
                conn.autocommit = True
                start = base + thread_id * _ROWS_PER_THREAD
                for i in range(_ROWS_PER_THREAD):
                    uid = start + i
                    pg_exec(conn,
                            "INSERT INTO users(id, name) VALUES (%s, %s)",
                            (uid, f"t{thread_id}_r{i}"))
                conn.close()
            except Exception as exc:
                errors.append(f"thread {thread_id}: {exc}")

        threads = [
            threading.Thread(target=_insert_batch, args=(t,))
            for t in range(_THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, f"Insert errors: {errors}"

        # KEEL scatter count
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        scatter = _scatter_count(conn, "users", base, base + _TOTAL_ROWS - 1)
        conn.close()

        # Direct shard counts
        direct0 = _direct_count(shard0_conn, "users", base, base + _TOTAL_ROWS - 1)
        direct1 = _direct_count(shard1_conn, "users", base, base + _TOTAL_ROWS - 1)
        direct_sum = direct0 + direct1

        # Cleanup
        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + _TOTAL_ROWS - 1))

        assert scatter == _TOTAL_ROWS, (
            f"Scatter COUNT = {scatter}, expected {_TOTAL_ROWS}"
        )
        assert direct_sum == _TOTAL_ROWS, (
            f"Direct shard sum = {direct_sum} (shard0={direct0}, shard1={direct1}), "
            f"expected {_TOTAL_ROWS}"
        )
        assert scatter == direct_sum, (
            f"KEEL scatter ({scatter}) != direct shard sum ({direct_sum})"
        )

    def test_no_duplicate_rows_under_concurrency(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """No row should appear more than once even with concurrent inserts."""
        base = _BASE + 2_000

        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + _TOTAL_ROWS - 1))

        errors: list[str] = []

        def _insert_batch(thread_id: int) -> None:
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=15)
                conn.autocommit = True
                start = base + thread_id * _ROWS_PER_THREAD
                for i in range(_ROWS_PER_THREAD):
                    uid = start + i
                    pg_exec(conn,
                            "INSERT INTO users(id, name) VALUES (%s, %s)",
                            (uid, f"dup_t{thread_id}_r{i}"))
                conn.close()
            except Exception as exc:
                errors.append(f"thread {thread_id}: {exc}")

        threads = [
            threading.Thread(target=_insert_batch, args=(t,))
            for t in range(_THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, f"Insert errors: {errors}"

        # Check for duplicates on each shard
        duplicates: list[str] = []
        for label, conn in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            rows = pg_exec(
                conn,
                "SELECT id, count(*) FROM users "
                "WHERE id BETWEEN %s AND %s GROUP BY id HAVING count(*) > 1",
                (base, base + _TOTAL_ROWS - 1)
            )
            for row in rows:
                duplicates.append(f"{label}: id={row[0]} count={row[1]}")

        # Cleanup
        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + _TOTAL_ROWS - 1))

        assert not duplicates, f"Duplicate rows found: {duplicates}"


class TestScatterAggregateIntegrity:
    """KEEL scatter aggregates must exactly match direct per-shard totals."""

    def test_scatter_count_matches_direct_sum(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """COUNT(*) via KEEL == shard0_count + shard1_count."""
        base = _BASE + 4_000
        n = 200

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))

        for i in range(n):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (base + i, f"scatter_{i}"))

        scatter = _scatter_count(keel_conn, "users", base, base + n - 1)
        d0 = _direct_count(shard0_conn, "users", base, base + n - 1)
        d1 = _direct_count(shard1_conn, "users", base, base + n - 1)

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))

        assert d0 + d1 == n, f"Direct shard sum mismatch: {d0}+{d1}={d0+d1} (expected {n})"
        assert scatter == n, f"Scatter COUNT mismatch: {scatter} (expected {n})"
        assert scatter == d0 + d1, \
            f"Scatter {scatter} != direct sum {d0+d1}"

    def test_scatter_sum_matches_direct_sum(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """SUM(balance) via KEEL scatter must equal shard0+shard1 direct sums."""
        base = _BASE + 5_000
        n = 100
        expected_total = sum(i * 10 for i in range(n))

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))

        for i in range(n):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, balance) VALUES (%s, %s, %s)",
                    (base + i, f"sum_{i}", i * 10))

        scatter_sum = float(pg_scalar(
            keel_conn,
            "SELECT SUM(balance) FROM users WHERE id BETWEEN %s AND %s",
            (base, base + n - 1)
        ) or 0)

        d0_sum = float(pg_scalar(
            shard0_conn,
            "SELECT COALESCE(SUM(balance), 0) FROM users WHERE id BETWEEN %s AND %s",
            (base, base + n - 1)
        ) or 0)
        d1_sum = float(pg_scalar(
            shard1_conn,
            "SELECT COALESCE(SUM(balance), 0) FROM users WHERE id BETWEEN %s AND %s",
            (base, base + n - 1)
        ) or 0)

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))

        assert scatter_sum == pytest.approx(expected_total, abs=0.01), \
            f"Scatter SUM = {scatter_sum}, expected {expected_total}"
        assert scatter_sum == pytest.approx(d0_sum + d1_sum, abs=0.01), \
            f"Scatter SUM {scatter_sum} != direct sum {d0_sum + d1_sum}"


class TestUpdateIntegrity:
    """UPDATE operations must change exactly the targeted rows."""

    def test_update_changes_all_target_rows(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """UPDATE WHERE range must update all rows in that range."""
        base = _BASE + 6_000
        n = 100

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))
        for i in range(n):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (base + i, "original"))

        # Scatter UPDATE (no shard key)
        pg_exec(keel_conn,
                "UPDATE users SET name = %s WHERE id BETWEEN %s AND %s",
                ("updated", base, base + n - 1))

        # Verify via direct shard reads
        for label, conn in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            wrong = int(pg_scalar(
                conn,
                "SELECT count(*) FROM users "
                "WHERE id BETWEEN %s AND %s AND name != %s",
                (base, base + n - 1, "updated")
            ) or 0)
            assert wrong == 0, \
                f"{label}: {wrong} rows still have old name after UPDATE"

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))

    def test_update_leaves_non_target_rows_unchanged(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """UPDATE on a range must not affect rows outside that range."""
        base = _BASE + 7_000
        n = 100

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + 2 * n - 1))

        for i in range(2 * n):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (base + i, "original"))

        # Update only first half
        pg_exec(keel_conn,
                "UPDATE users SET name = %s WHERE id BETWEEN %s AND %s",
                ("updated", base, base + n - 1))

        # Second half must be untouched
        for label, conn in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            wrong = int(pg_scalar(
                conn,
                "SELECT count(*) FROM users "
                "WHERE id BETWEEN %s AND %s AND name != %s",
                (base + n, base + 2 * n - 1, "original")
            ) or 0)
            assert wrong == 0, \
                f"{label}: {wrong} rows outside update range were changed"

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + 2 * n - 1))


class TestDeleteIntegrity:
    """DELETE operations must remove exactly the targeted rows."""

    def test_scatter_delete_removes_all_rows(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """DELETE (scatter) must empty both shards for the target range."""
        base = _BASE + 8_000
        n = 100

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))
        for i in range(n):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (base + i, "to_del"))

        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1))

        for label, conn in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            remaining = int(pg_scalar(
                conn,
                "SELECT count(*) FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1)
            ) or 0)
            assert remaining == 0, \
                f"{label}: {remaining} rows survived DELETE"

    def test_double_delete_is_idempotent(self, keel_conn):
        """Deleting an already-absent key must not raise an error."""
        uid = _BASE + 99_999
        pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (uid,))
        # Second delete — must be silent
        pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (uid,))


class TestTransactionIntegrity:
    """Transactions must be all-or-nothing, even across shards."""

    def test_rollback_leaves_no_rows(
        self, keel_txn_conn, shard0_conn, shard1_conn
    ):
        """Rows inserted in a rolled-back transaction must not persist."""
        base = _BASE + 10_000
        n = 50

        # Cleanup first (outside transaction)
        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + n - 1))

        # Insert inside transaction, then rollback
        for i in range(n):
            pg_exec(keel_txn_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (base + i, "rollback_me"))
        keel_txn_conn.rollback()

        # Verify nothing persisted
        for label, conn in [("shard0", shard0_conn), ("shard1", shard1_conn)]:
            count = int(pg_scalar(
                conn,
                "SELECT count(*) FROM users WHERE id BETWEEN %s AND %s",
                (base, base + n - 1)
            ) or 0)
            assert count == 0, \
                f"{label}: {count} rows survived ROLLBACK"

    def test_commit_persists_rows(
        self, keel_txn_conn, shard0_conn, shard1_conn
    ):
        """Rows inserted in a committed transaction must be visible on shards."""
        base = _BASE + 11_000
        n = 50

        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + n - 1))

        for i in range(n):
            pg_exec(keel_txn_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (base + i, "commit_me"))
        keel_txn_conn.commit()

        d0 = _direct_count(shard0_conn, "users", base, base + n - 1)
        d1 = _direct_count(shard1_conn, "users", base, base + n - 1)

        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM users WHERE id BETWEEN %s AND %s",
                    (base, base + n - 1))

        assert d0 + d1 == n, \
            f"Committed rows: {d0}+{d1}={d0+d1}, expected {n}"

    def test_read_your_writes_within_session(self, keel_conn):
        """INSERT and immediate SELECT in same connection must see the new row."""
        uid = _BASE + 12_001
        pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (uid,))
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                (uid, "ryw"))
        row = pg_exec(keel_conn,
                      "SELECT id, name FROM users WHERE id = %s", (uid,))
        pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (uid,))

        assert len(row) == 1, f"Expected 1 row (read-your-writes), got {len(row)}"
        assert row[0][1] == "ryw"


class TestCrossShardAtomicity:
    """2PC operations must commit or rollback atomically across shards."""

    def test_2pc_commit_both_shards_have_data(
        self, shard0_conn, shard1_conn, shard0_dsn, shard1_dsn
    ):
        """After COMMIT PREPARED on both shards, both shards must hold the rows."""
        xid0 = f"integrity_2pc_commit_s0_{uuid.uuid4().hex[:8]}"
        xid1 = f"integrity_2pc_commit_s1_{uuid.uuid4().hex[:8]}"

        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM keel_2pc_test WHERE marker = %s",
                    ("2pc_commit_test",))

        # Use dedicated connections for the PREPARE phase to avoid toggling
        # autocommit on the shared fixture connections (psycopg2 raises
        # ProgrammingError if autocommit is changed while a transaction is open).
        conn0 = psycopg2.connect(shard0_dsn, connect_timeout=10)
        conn1 = psycopg2.connect(shard1_dsn, connect_timeout=10)
        conn0.autocommit = False
        conn1.autocommit = False
        try:
            pg_exec(conn0,
                    "INSERT INTO keel_2pc_test(shard, marker) VALUES (0, %s)",
                    ("2pc_commit_test",))
            pg_exec(conn1,
                    "INSERT INTO keel_2pc_test(shard, marker) VALUES (1, %s)",
                    ("2pc_commit_test",))
            pg_exec(conn0, f"PREPARE TRANSACTION '{xid0}'")
            pg_exec(conn1, f"PREPARE TRANSACTION '{xid1}'")
        finally:
            conn0.close()
            conn1.close()

        # Phase 2: COMMIT PREPARED on both shards
        pg_exec(shard0_conn, f"COMMIT PREPARED '{xid0}'")
        pg_exec(shard1_conn, f"COMMIT PREPARED '{xid1}'")

        c0 = int(pg_scalar(shard0_conn,
                           "SELECT count(*) FROM keel_2pc_test WHERE marker = %s",
                           ("2pc_commit_test",)) or 0)
        c1 = int(pg_scalar(shard1_conn,
                           "SELECT count(*) FROM keel_2pc_test WHERE marker = %s",
                           ("2pc_commit_test",)) or 0)

        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM keel_2pc_test WHERE marker = %s",
                    ("2pc_commit_test",))

        assert c0 == 1, f"Shard 0: expected 1 row after 2PC commit, got {c0}"
        assert c1 == 1, f"Shard 1: expected 1 row after 2PC commit, got {c1}"

    def test_2pc_rollback_neither_shard_has_data(
        self, shard0_conn, shard1_conn, shard0_dsn, shard1_dsn
    ):
        """After ROLLBACK PREPARED on both shards, neither shard must hold rows."""
        xid0 = f"integrity_2pc_rollback_s0_{uuid.uuid4().hex[:8]}"
        xid1 = f"integrity_2pc_rollback_s1_{uuid.uuid4().hex[:8]}"
        marker = "2pc_rollback_test"

        for conn in [shard0_conn, shard1_conn]:
            pg_exec(conn,
                    "DELETE FROM keel_2pc_test WHERE marker = %s", (marker,))

        # Use dedicated connections for the PREPARE phase
        conn0 = psycopg2.connect(shard0_dsn, connect_timeout=10)
        conn1 = psycopg2.connect(shard1_dsn, connect_timeout=10)
        conn0.autocommit = False
        conn1.autocommit = False
        try:
            pg_exec(conn0,
                    "INSERT INTO keel_2pc_test(shard, marker) VALUES (0, %s)",
                    (marker,))
            pg_exec(conn1,
                    "INSERT INTO keel_2pc_test(shard, marker) VALUES (1, %s)",
                    (marker,))
            pg_exec(conn0, f"PREPARE TRANSACTION '{xid0}'")
            pg_exec(conn1, f"PREPARE TRANSACTION '{xid1}'")
        finally:
            conn0.close()
            conn1.close()

        pg_exec(shard0_conn, f"ROLLBACK PREPARED '{xid0}'")
        pg_exec(shard1_conn, f"ROLLBACK PREPARED '{xid1}'")

        c0 = int(pg_scalar(shard0_conn,
                           "SELECT count(*) FROM keel_2pc_test WHERE marker = %s",
                           (marker,)) or 0)
        c1 = int(pg_scalar(shard1_conn,
                           "SELECT count(*) FROM keel_2pc_test WHERE marker = %s",
                           (marker,)) or 0)

        assert c0 == 0, f"Shard 0: {c0} rows survive after 2PC rollback"
        assert c1 == 0, f"Shard 1: {c1} rows survive after 2PC rollback"
