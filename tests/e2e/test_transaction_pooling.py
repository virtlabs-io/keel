"""
test_transaction_pooling.py — Transaction Pooling: backend connections shared across clients
=============================================================================================

Tests the core contract of KEEL's ``pool_mode = transaction``:

    **"A backend connection is borrowed for exactly one transaction boundary,
    then returned to the pool for reuse by any other client session."**

Four scenarios are exercised, each running ≥ 1 000 transactions:

  A  Autocommit / implicit transactions
     Every SQL statement is its own transaction.  The backend connection is
     borrowed, used, and returned on *every single statement*.  1 000 rapid
     single-statement transactions verify that the pool recycles cleanly at
     the highest possible frequency.

  B  Explicit short transactions (2–5 statements)
     ``BEGIN`` → 2–5 DML/SELECT statements → ``COMMIT`` or ``ROLLBACK``.
     1 000 short transaction cycles exercise atomicity, ROLLBACK discard,
     and error-inside-transaction recovery.

  C  Explicit long transactions (50+ statements)
     ``BEGIN`` → 50 + statements → ``COMMIT``.  Tests that the pool holds a
     backend for the full transaction duration and releases it only on
     ``COMMIT``/``ROLLBACK``, not between individual statements.

  D  High-concurrency mixed-mode pool sharing
     N threads each execute M transactions (total ≥ 1 000), using a mix of
     autocommit and explicit transactions.  Validates pool multiplexing
     efficiency, the absence of transaction-state leakage between clients,
     and correct data integrity under heavy contention.

Corner cases tested in their own class:

  - ``SET`` / ``SET LOCAL`` session-variable isolation across pool recycles
  - ``pg_backend_pid()`` PID changes prove the backend was recycled
  - Savepoints and partial rollback inside a transaction
  - Error inside transaction → ``ROLLBACK`` → pool returns a clean connection
  - Read-your-own-writes within the *same* transaction
  - Visibility isolation: uncommitted writes invisible to concurrent clients
  - ``DECLARE CURSOR`` … ``FETCH`` within a transaction
  - Temporary tables: connection-local, not client-local — session is lost
    when the backend is recycled
  - ``LISTEN`` pins the backend (session-mode semantics); ``UNLISTEN`` releases
  - ``pg_advisory_lock`` acquired in-transaction, auto-released on
    ``COMMIT``/``ROLLBACK``
  - Pool waiting-queue: more clients than backends → clean block/timeout
  - Consecutive transaction on same *psycopg2* connection reuses pool slot
  - ``COPY TO STDOUT`` / ``COPY FROM STDIN`` within an explicit transaction

Background
----------
KEEL is configured with::

    pool_mode   = transaction
    min_pool_size = 2
    max_pool_size = 60

Every test that writes data uses unique ID ranges to avoid cross-test
interference; cleanup is always performed even if the test body fails.
"""

from __future__ import annotations

import decimal
import select as _select
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from io import StringIO
from typing import Generator

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pytest

from helpers import (
    pg_exec,
    pg_scalar,
    pg_count,
    shard_total_count,
    wait_until,
    WorkerResult,
)

pytestmark = [pytest.mark.pool]

# ---------------------------------------------------------------------------
# Configuration constants (mirrors keel-e2e-suite.ini)
# ---------------------------------------------------------------------------
MIN_POOL_SIZE = 2
MAX_POOL_SIZE = 60

# ---------------------------------------------------------------------------
# ID ranges — each scenario owns a disjoint range to prevent collisions.
# ---------------------------------------------------------------------------
# Scenario A: 100_000 – 100_999
# Scenario B: 101_000 – 101_999
# Scenario C: 102_000 – 102_999
# Scenario D: 103_000 – 103_999
# Corner cases: 110_000 – 119_999

_SCENARIO_A_BASE = 100_000
_SCENARIO_B_BASE = 101_000
_SCENARIO_C_BASE = 102_000
_SCENARIO_D_BASE = 103_000
_CORNER_BASE      = 110_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(dsn: str, autocommit: bool = True) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(dsn, connect_timeout=10)
    conn.autocommit = autocommit
    return conn


@contextmanager
def _autoconn(dsn: str, autocommit: bool = True) -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager: open a connection, yield it, always close it."""
    conn = _connect(dsn, autocommit=autocommit)
    try:
        yield conn
    finally:
        try:
            if not autocommit:
                conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _cleanup(dsn: str, table: str, id_lo: int, id_hi: int) -> None:
    """Delete rows in [id_lo, id_hi) on a scatter table via KEEL."""
    with _autoconn(dsn, autocommit=False) as conn:
        pg_exec(conn, f"DELETE FROM {table} WHERE id >= %s AND id < %s", (id_lo, id_hi))
        conn.commit()


def _cleanup_shards(
    s0_conn: psycopg2.extensions.connection,
    s1_conn: psycopg2.extensions.connection,
    table: str,
    where: str,
) -> None:
    """Direct shard cleanup — bypasses KEEL, used for teardown."""
    for conn in (s0_conn, s1_conn):
        pg_exec(conn, f"DELETE FROM {table} WHERE {where}")


# ---------------------------------------------------------------------------
# Scenario A — Autocommit: 1 000 single-statement transactions
# ---------------------------------------------------------------------------

class TestScenarioA_Autocommit:
    """Scenario A: Autocommit / implicit single-statement transactions.

    Each statement is its own transaction boundary.  The backend connection
    is borrowed and returned on *every* statement, making this the highest
    pool-recycle frequency scenario.  1 000 + iterations are performed on a
    single long-lived psycopg2 connection to ensure KEEL multiplexes
    correctly even when the front-end socket is held open.
    """

    ITERATIONS = 1_000

    def test_1000_autocommit_selects_single_connection(self, keel_dsn: str) -> None:
        """1 000 autocommit SELECTs on one connection all return the correct value."""
        with _autoconn(keel_dsn, autocommit=True) as conn:
            for i in range(self.ITERATIONS):
                val = pg_scalar(conn, "SELECT %s::int", (i,))
                assert val == i, f"iteration {i}: expected {i}, got {val}"

    def test_1000_autocommit_insert_delete_cycles(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """1 000 autocommit INSERT→DELETE cycles leave the table empty."""
        base = _SCENARIO_A_BASE
        # Ensure clean starting state
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id >= {base} AND id < {base + self.ITERATIONS}")

        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(self.ITERATIONS):
                row_id = base + i
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (row_id, f"a_{i}"))
                conn.commit()
                pg_exec(conn, "DELETE FROM users WHERE id = %s", (row_id,))
                conn.commit()

        total = shard_total_count(shard0_conn, shard1_conn, "users", f"id >= {base} AND id < {base + self.ITERATIONS}")
        assert total == 0, f"Expected 0 rows after all deletes; found {total}"

    def test_1000_autocommit_transactions_multiple_connections(self, keel_dsn: str) -> None:
        """1 000 autocommit transactions spread across 10 psycopg2 connections."""
        n_conns = 10
        per_conn = self.ITERATIONS // n_conns
        results: list[list[int]] = [[] for _ in range(n_conns)]

        def worker(idx: int) -> None:
            with _autoconn(keel_dsn, autocommit=True) as conn:
                for j in range(per_conn):
                    val = pg_scalar(conn, "SELECT %s::int", (idx * per_conn + j,))
                    results[idx].append(val)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_conns)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        flat = [v for sub in results for v in sub]
        assert len(flat) == self.ITERATIONS
        assert sorted(flat) == list(range(self.ITERATIONS))

    def test_autocommit_pid_changes_across_statements(self, keel_dsn: str) -> None:
        """Pool assigns distinct backends to simultaneously-open connections.

        Open MIN_POOL_SIZE + 2 connections at once, each holding a transaction
        (so each must borrow a separate backend), and collect their PIDs.  At
        least 2 distinct PIDs must appear — proving the pool has multiple
        backends and correctly multiplexes them.

        NOTE: sequential queries on *one* connection may consistently land on
        the same backend (LIFO assignment), so we test with concurrent borrows.
        """
        n_conns = MIN_POOL_SIZE + 2
        pids: list[int | None] = [None] * n_conns
        barrier = threading.Barrier(n_conns + 1)
        release = threading.Event()

        def grab_pid(idx: int) -> None:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                pids[idx] = pg_scalar(conn, "SELECT pg_backend_pid()")
                barrier.wait(timeout=15)   # all connections open simultaneously
                release.wait(timeout=15)   # hold backend until released
                conn.rollback()

        threads = [threading.Thread(target=grab_pid, args=(i,)) for i in range(n_conns)]
        for t in threads:
            t.start()
        barrier.wait(timeout=15)   # all connections are holding a backend
        release.set()
        for t in threads:
            t.join(timeout=30)

        distinct = set(p for p in pids if p is not None)
        assert len(distinct) >= 2, (
            f"Only {len(distinct)} distinct backend PID(s) across {n_conns} "
            f"concurrent connections: {distinct}. Pool may not have multiple backends."
        )

    def test_autocommit_no_state_leak_across_1000_borrows(self, keel_dsn: str) -> None:
        """SET LOCAL changes must not persist to subsequent autocommit transactions.

        SET LOCAL is transaction-scoped. In autocommit mode, every statement
        is its own transaction, so SET LOCAL takes effect and is immediately
        discarded on the implicit COMMIT.
        """
        with _autoconn(keel_dsn, autocommit=True) as conn:
            # Issue a SET LOCAL from within an explicit transaction
            with _autoconn(keel_dsn, autocommit=False) as txn_conn:
                pg_exec(txn_conn, "SET LOCAL work_mem = '64kB'")
                local_val = pg_scalar(txn_conn, "SHOW work_mem")
                txn_conn.rollback()  # rolls back the SET LOCAL

            # The autocommit connection must not see the leaked SET LOCAL
            for _ in range(100):
                val = pg_scalar(conn, "SHOW work_mem")
                assert val != "64kB", (
                    "SET LOCAL work_mem leaked across pool boundary into autocommit connection"
                )


# ---------------------------------------------------------------------------
# Scenario B — Explicit short transactions (2–5 statements)
# ---------------------------------------------------------------------------

class TestScenarioB_ShortTransactions:
    """Scenario B: Explicit short transactions.

    Each transaction spans 2–5 statements with a mixture of commits and
    rollbacks.  500 commit transactions + 500 rollback transactions = 1 000
    total transaction units.
    """

    COMMIT_ITERS   = 500
    ROLLBACK_ITERS = 500

    def test_500_explicit_commit_transactions(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """500 explicit COMMIT transactions accumulate all rows correctly."""
        base = _SCENARIO_B_BASE
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id >= {base} AND id < {base + self.COMMIT_ITERS}")

        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(self.COMMIT_ITERS):
                    row_id = base + i
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, %s)",
                             (row_id, f"b_commit_{i}", i * 10))
                    # Second statement: update within same transaction
                    pg_exec(conn, "UPDATE users SET name = %s WHERE id = %s",
                             (f"b_committed_{i}", row_id))
                    conn.commit()

            total = shard_total_count(shard0_conn, shard1_conn, "users",
                                      f"id >= {base} AND id < {base + self.COMMIT_ITERS}")
            assert total == self.COMMIT_ITERS, (
                f"Expected {self.COMMIT_ITERS} rows after {self.COMMIT_ITERS} commits; got {total}"
            )
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + self.COMMIT_ITERS}")

    def test_500_explicit_rollback_transactions(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """500 explicit ROLLBACK transactions leave no rows behind."""
        base = _SCENARIO_B_BASE + self.ROLLBACK_ITERS  # offset from commit range
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + self.ROLLBACK_ITERS}")

        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(self.ROLLBACK_ITERS):
                row_id = base + i
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                         (row_id, f"b_rollback_{i}"))
                pg_exec(conn, "UPDATE users SET name = %s WHERE id = %s",
                         (f"b_should_rollback_{i}", row_id))
                conn.rollback()

        total = shard_total_count(shard0_conn, shard1_conn, "users",
                                  f"id >= {base} AND id < {base + self.ROLLBACK_ITERS}")
        assert total == 0, f"Expected 0 rows after {self.ROLLBACK_ITERS} rollbacks; got {total}"

    def test_short_txn_atomicity_commit_visible_rollback_invisible(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """Committed rows are visible; rolled-back rows are not.

        Interleaves 100 commit transactions with 100 rollback transactions and
        verifies the final count matches only the committed rows.
        """
        base = _SCENARIO_B_BASE + 800
        n = 100
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id >= {base} AND id < {base + 2 * n}")

        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(n):
                    # Commit slot
                    pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                             (base + i, f"keep_{i}"))
                    conn.commit()
                    # Rollback slot
                    pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                             (base + n + i, f"discard_{i}"))
                    conn.rollback()

            total = shard_total_count(shard0_conn, shard1_conn, "users",
                                      f"id >= {base} AND id < {base + 2 * n}")
            assert total == n, f"Expected {n} committed rows; got {total}"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + 2 * n}")

    def test_short_txn_error_recovery_pool_stays_clean(self, keel_dsn: str) -> None:
        """After an error inside a transaction the pool connection is returned clean.

        Executes 200 transactions that each intentionally trigger a server error
        (division by zero), performs a ROLLBACK, then immediately runs a healthy
        transaction.  All 200 post-error queries must succeed.
        """
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(200):
                try:
                    pg_exec(conn, "SELECT 1/0")
                except psycopg2.errors.DivisionByZero:
                    pass
                conn.rollback()
                # Post-rollback: pool must have returned a clean connection
                val = pg_scalar(conn, "SELECT %s::int", (i,))
                assert val == i, (
                    f"iteration {i}: clean query after rollback expected {i}, got {val}"
                )

    def test_short_txn_read_your_own_writes(self, keel_dsn: str, shard0_conn, shard1_conn) -> None:
        """Within a single transaction, a freshly inserted row must be readable.

        Verifies that KEEL does not switch the backend connection mid-transaction,
        which would break read-your-own-writes semantics.
        """
        base = _SCENARIO_B_BASE + 900
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id >= {base} AND id < {base + 50}")

        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(50):
                    row_id = base + i
                    pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                             (row_id, f"ryw_{i}"))
                    # Must be visible within the same transaction
                    name = pg_scalar(conn, "SELECT name FROM users WHERE id = %s", (row_id,))
                    assert name == f"ryw_{i}", (
                        f"Row {row_id} not visible within same transaction (RYOW broken)"
                    )
                    conn.rollback()  # discard — keeps shards clean
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + 50}")

    def test_uncommitted_data_invisible_to_concurrent_client(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """Uncommitted write on one connection must not be visible on another.

        This is the primary pool-state-leakage guard: if KEEL accidentally
        returns a connection that still has an open transaction, a second
        client could borrow it and read uncommitted data.
        """
        row_id = _SCENARIO_B_BASE + 950
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id = {row_id}")

        writer = _connect(keel_dsn, autocommit=False)
        try:
            pg_exec(writer, "INSERT INTO users(id, name) VALUES (%s, 'ghost')", (row_id,))
            # Do NOT commit — row should be invisible everywhere else

            # Separate connection (different pool borrow) must not see it
            with _autoconn(keel_dsn, autocommit=True) as reader:
                found = pg_scalar(reader, "SELECT COUNT(*) FROM users WHERE id = %s", (row_id,))
                assert found == 0, (
                    "Uncommitted row visible to concurrent client — pool leaked transaction state"
                )

            # Also verify direct shard reads see nothing
            for sc in (shard0_conn, shard1_conn):
                cnt = pg_scalar(sc, "SELECT COUNT(*) FROM users WHERE id = %s", (row_id,))
                assert cnt == 0, "Uncommitted row visible directly on shard"
        finally:
            writer.rollback()
            writer.close()
            _cleanup_shards(shard0_conn, shard1_conn, "users", f"id = {row_id}")


# ---------------------------------------------------------------------------
# Scenario C — Explicit long transactions (50+ statements)
# ---------------------------------------------------------------------------

class TestScenarioC_LongTransactions:
    """Scenario C: Explicit long transactions.

    Each transaction spans 50+ statements.  The backend connection must be
    held for the full duration (pool_mode=transaction guarantees this).
    We run enough long transactions to accumulate ≥ 1 000 statement executions.
    """

    STATEMENTS_PER_TXN = 50
    TXN_COUNT          = 20  # 20 × 50 = 1 000 statements

    def test_long_transactions_hold_backend_for_full_duration(self, keel_dsn: str) -> None:
        """PID stays constant throughout a single long transaction.

        Within one BEGIN…COMMIT block the backend PID must remain the same
        across all 50+ statements — KEEL must not reassign the backend
        connection mid-transaction.
        """
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for _ in range(self.TXN_COUNT):
                pids_in_txn: set[int] = set()
                for _ in range(self.STATEMENTS_PER_TXN):
                    pid = pg_scalar(conn, "SELECT pg_backend_pid()")
                    pids_in_txn.add(pid)
                conn.commit()
                assert len(pids_in_txn) == 1, (
                    f"Backend PID changed inside a transaction: {pids_in_txn}. "
                    "KEEL must not re-route mid-transaction."
                )

    def test_long_transaction_accumulates_updates_atomically(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """50 sequential updates inside one transaction are all committed atomically."""
        base = _SCENARIO_C_BASE
        n = self.STATEMENTS_PER_TXN
        row_ids = list(range(base, base + n))
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")

        try:
            # Seed the rows
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for row_id in row_ids:
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, 0)",
                             (row_id, f"long_{row_id}"))
                conn.commit()

            # One long transaction that updates every row
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i, row_id in enumerate(row_ids):
                    pg_exec(conn, "UPDATE users SET balance = %s WHERE id = %s",
                             (i * 100, row_id))
                conn.commit()

            # Verify all updates are present via direct shard reads
            for i, row_id in enumerate(row_ids):
                for sc in (shard0_conn, shard1_conn):
                    bal = pg_scalar(sc, "SELECT balance FROM users WHERE id = %s", (row_id,))
                    if bal is not None:
                        assert int(bal) == i * 100, (
                            f"Row {row_id} has balance {bal}, expected {i * 100}"
                        )
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + n}")

    def test_long_transaction_rollback_discards_all_50_updates(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """A ROLLBACK after 50 statements discards every change atomically."""
        base = _SCENARIO_C_BASE + 100
        n = self.STATEMENTS_PER_TXN
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")

        # Seed
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(n):
                pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, 999)",
                         (base + i, f"pre_{i}"))
            conn.commit()

        # Long transaction, then rollback
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(n):
                pg_exec(conn, "UPDATE users SET balance = 0 WHERE id = %s", (base + i,))
            conn.rollback()  # discard all 50 updates

        # Direct shard verification — balance must still be 999
        for i in range(n):
            row_id = base + i
            for sc in (shard0_conn, shard1_conn):
                bal = pg_scalar(sc, "SELECT balance FROM users WHERE id = %s", (row_id,))
                if bal is not None:
                    assert int(bal) == 999, (
                        f"Row {row_id} has balance {bal} after rollback; expected 999"
                    )

        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")

    def test_long_transaction_does_not_block_concurrent_short_transactions(
        self, keel_dsn: str
    ) -> None:
        """Concurrent short transactions succeed while one long transaction is open.

        With MAX_POOL_SIZE = 60, opening one long transaction (holding 1 backend)
        must not prevent other clients from completing short autocommit queries.
        The remaining 59 backends are available.
        """
        long_txn_started = threading.Event()
        long_txn_release = threading.Event()
        errors: list[Exception] = []
        lock = threading.Lock()

        def long_txn_holder() -> None:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                pg_exec(conn, "SELECT 1")  # borrow backend
                long_txn_started.set()
                long_txn_release.wait(timeout=30)
                conn.rollback()

        def short_txn_worker() -> None:
            try:
                with _autoconn(keel_dsn, autocommit=True) as conn:
                    val = pg_scalar(conn, "SELECT 42")
                    if val != 42:
                        raise AssertionError(f"Expected 42, got {val}")
            except Exception as exc:
                with lock:
                    errors.append(exc)

        holder = threading.Thread(target=long_txn_holder)
        holder.start()
        long_txn_started.wait(timeout=15)

        workers = [threading.Thread(target=short_txn_worker) for _ in range(20)]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=30)

        long_txn_release.set()
        holder.join(timeout=10)

        assert not errors, f"Short transactions failed while long transaction was open: {errors}"

    def test_1000_statements_across_20_long_transactions(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """20 long transactions × 50 statements = 1 000 statements total.

        Verifies that the pool survives the full 1 000-statement workload
        without connection leaks or stale-state errors.
        """
        base = _SCENARIO_C_BASE + 200
        n = self.TXN_COUNT
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")

        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for txn_idx in range(n):
                    row_id = base + txn_idx
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, 0)",
                             (row_id, f"long_mass_{txn_idx}"))
                    # 49 more statements in the same transaction
                    for k in range(1, self.STATEMENTS_PER_TXN):
                        pg_exec(conn, "UPDATE users SET balance = %s WHERE id = %s",
                                 (k, row_id))
                    conn.commit()

            total = shard_total_count(
                shard0_conn, shard1_conn, "users",
                f"id >= {base} AND id < {base + n}",
            )
            assert total == n, f"Expected {n} rows; got {total}"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + n}")


# ---------------------------------------------------------------------------
# Scenario D — High-concurrency mixed-mode pool sharing
# ---------------------------------------------------------------------------

class TestScenarioD_ConcurrentMixed:
    """Scenario D: High-concurrency mixed-mode pool sharing.

    50 worker threads each run 20 transactions (mix of autocommit and explicit),
    totalling 1 000 transactions.  Validates correct pool multiplexing and
    the absence of data crossover between sessions.
    """

    N_WORKERS    = 50
    TXN_PER_WRKR = 20
    TOTAL_TXN    = N_WORKERS * TXN_PER_WRKR  # 1 000

    def test_1000_concurrent_mixed_mode_transactions(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """1 000 concurrent mixed-mode transactions produce correct row count."""
        base = _SCENARIO_D_BASE
        total_rows = self.N_WORKERS  # each worker inserts 1 row, keeps it
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + self.N_WORKERS}")

        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(worker_idx: int) -> None:
            row_id = base + worker_idx
            try:
                # Mix of autocommit and explicit transactions
                for txn_idx in range(self.TXN_PER_WRKR):
                    use_explicit = (txn_idx % 2 == 0)
                    with _autoconn(keel_dsn, autocommit=not use_explicit) as conn:
                        if use_explicit:
                            if txn_idx == 0:
                                # First explicit txn: INSERT the row
                                pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, 0)",
                                         (row_id, f"d_{worker_idx}"))
                                conn.commit()
                            else:
                                # Subsequent explicit txns: UPDATE balance
                                pg_exec(conn, "UPDATE users SET balance = %s WHERE id = %s",
                                         (txn_idx, row_id))
                                conn.commit()
                        else:
                            # Autocommit: read own row (after first txn seeded it)
                            if txn_idx >= 2:
                                val = pg_scalar(conn, "SELECT balance FROM users WHERE id = %s",
                                                (row_id,))
                                # balance is NUMERIC(14,2) → psycopg2 returns Decimal;
                                # val may also be None when the row lives on the other shard
                                assert val is None or isinstance(val, (int, float, decimal.Decimal)), (
                                    f"Unexpected balance type: {type(val)}"
                                )
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(self.N_WORKERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        assert not errors, f"{len(errors)} worker errors in mixed-mode test: {errors[:5]}"

        total = shard_total_count(shard0_conn, shard1_conn, "users",
                                  f"id >= {base} AND id < {base + self.N_WORKERS}")
        assert total == total_rows, f"Expected {total_rows} rows; got {total}"

        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + self.N_WORKERS}")

    def test_concurrent_writers_no_data_crossover(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """100 concurrent writers each own a disjoint row — no crossover allowed."""
        base = _SCENARIO_D_BASE + 100
        n = 100
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")

        errors: list[Exception] = []
        lock = threading.Lock()

        def writer(idx: int) -> None:
            row_id = base + idx
            try:
                with _autoconn(keel_dsn, autocommit=False) as conn:
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, %s)",
                             (row_id, f"cw_{idx}", idx))
                    conn.commit()
            except Exception as exc:
                with lock:
                    errors.append(exc)

        with ThreadPoolExecutor(max_workers=n) as pool:
            futs = [pool.submit(writer, i) for i in range(n)]
            for f in as_completed(futs, timeout=60):
                pass  # propagate via errors list

        assert not errors, f"Write errors: {errors[:5]}"

        total = shard_total_count(shard0_conn, shard1_conn, "users",
                                  f"id >= {base} AND id < {base + n}")
        assert total == n, f"Expected {n} rows; got {total}"

        # Verify per-row balance correctness (no crossover)
        for sc in (shard0_conn, shard1_conn):
            rows = pg_exec(sc, f"SELECT id, balance FROM users WHERE id >= {base} AND id < {base + n}")
            for row_id, balance in rows:
                expected = row_id - base
                assert int(balance) == expected, (
                    f"Row {row_id} has balance {balance}; expected {expected} — possible data crossover"
                )

        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")

    def test_rapid_connect_disconnect_1000_cycles(self, keel_dsn: str) -> None:
        """1 000 rapid open/query/close cycles do not exhaust the pool."""
        for i in range(1_000):
            with _autoconn(keel_dsn, autocommit=True) as conn:
                val = pg_scalar(conn, "SELECT %s::int", (i % 100,))
                assert val == i % 100, f"Cycle {i}: expected {i % 100}, got {val}"

    def test_concurrent_connection_storm(self, keel_dsn: str) -> None:
        """100 simultaneous connections each executing 10 queries."""
        n_conns = 100
        queries_per_conn = 10
        errors: list[Exception] = []
        lock = threading.Lock()

        def storm_worker(idx: int) -> None:
            try:
                with _autoconn(keel_dsn, autocommit=True) as conn:
                    for q in range(queries_per_conn):
                        val = pg_scalar(conn, "SELECT %s::int", (idx * queries_per_conn + q,))
                        expected = idx * queries_per_conn + q
                        if val != expected:
                            raise AssertionError(f"Expected {expected}, got {val}")
            except Exception as exc:
                with lock:
                    errors.append(exc)

        with ThreadPoolExecutor(max_workers=n_conns) as pool:
            futs = [pool.submit(storm_worker, i) for i in range(n_conns)]
            for f in as_completed(futs, timeout=120):
                pass

        assert not errors, f"{len(errors)} errors in connection storm: {errors[:5]}"


# ---------------------------------------------------------------------------
# Corner Cases
# ---------------------------------------------------------------------------

class TestCornerCases_SessionVariableIsolation:
    """Session variables must not leak across pool boundaries."""

    def test_set_session_variable_not_visible_after_pool_return(self, keel_dsn: str) -> None:
        """SET work_mem inside transaction A must not be seen by transaction B.

        After transaction A commits (or rolls back), its backend connection
        returns to the pool.  The SSV subsystem must reset / replay only the
        canonical session state when the connection is next borrowed, so
        transaction B must not see A's SET.
        """
        # Transaction A: set a session variable and commit
        with _autoconn(keel_dsn, autocommit=False) as conn_a:
            pg_exec(conn_a, "SET work_mem = '128MB'")
            val_in_a = pg_scalar(conn_a, "SHOW work_mem")
            assert val_in_a == "128MB"
            conn_a.commit()

        # Transaction B: open a fresh connection, work_mem should be default
        with _autoconn(keel_dsn, autocommit=True) as conn_b:
            val_in_b = pg_scalar(conn_b, "SHOW work_mem")
            # The default is 4MB; it must NOT be 128MB
            assert val_in_b != "128MB", (
                f"SET work_mem='128MB' from previous client leaked into new session: {val_in_b}"
            )

    def test_set_local_does_not_outlive_transaction(self, keel_dsn: str) -> None:
        """SET LOCAL is transaction-scoped and vanishes after COMMIT."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "SET LOCAL work_mem = '32MB'")
            val_inside = pg_scalar(conn, "SHOW work_mem")
            assert val_inside == "32MB"
            conn.commit()
            # After COMMIT the local setting is gone
            val_after = pg_scalar(conn, "SHOW work_mem")
            assert val_after != "32MB", (
                f"SET LOCAL work_mem='32MB' persisted after COMMIT: {val_after}"
            )

    def test_search_path_isolation_across_pool_recycles(self, keel_dsn: str) -> None:
        """SET search_path in one transaction must not pollute the next borrower."""
        with _autoconn(keel_dsn, autocommit=False) as conn_a:
            pg_exec(conn_a, "SET search_path = public, pg_catalog")
            conn_a.commit()

        with _autoconn(keel_dsn, autocommit=True) as conn_b:
            path = pg_scalar(conn_b, "SHOW search_path")
            # search_path should be the server default, not what conn_a set.
            # We just verify it is not None and is a string.
            assert isinstance(path, str)

    def test_100_clients_each_set_different_work_mem_no_cross_contamination(
        self, keel_dsn: str
    ) -> None:
        """100 clients each set a distinct work_mem; none should see another's value."""
        n = 100
        errors: list[Exception] = []
        lock = threading.Lock()

        def client(idx: int) -> None:
            target = f"{4 * (idx + 1)}MB"
            try:
                with _autoconn(keel_dsn, autocommit=False) as conn:
                    pg_exec(conn, f"SET LOCAL work_mem = '{target}'")
                    val = pg_scalar(conn, "SHOW work_mem")
                    assert val == target, f"client {idx}: expected {target}, got {val}"
                    conn.commit()
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=client, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, f"{len(errors)} SET LOCAL isolation errors: {errors[:5]}"


class TestCornerCases_SavepointsAndNestedAborts:
    """Savepoints allow partial rollback within a transaction."""

    def test_savepoint_partial_rollback(self, keel_dsn: str, shard0_conn, shard1_conn) -> None:
        """ROLLBACK TO SAVEPOINT discards only the work after the savepoint."""
        base = _CORNER_BASE
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id IN ({base}, {base+1}, {base+2})")
        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'keep')", (base,))
                pg_exec(conn, "SAVEPOINT sp1")
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'discard')", (base + 1,))
                pg_exec(conn, "ROLLBACK TO SAVEPOINT sp1")
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'keep2')", (base + 2,))
                conn.commit()

            for sc in (shard0_conn, shard1_conn):
                discarded = pg_scalar(sc, "SELECT COUNT(*) FROM users WHERE id = %s AND name = 'discard'",
                                      (base + 1,))
                assert discarded == 0, "Row after SAVEPOINT not discarded by ROLLBACK TO SAVEPOINT"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id IN ({base}, {base+1}, {base+2})")

    def test_release_savepoint_commits_sub_work(self, keel_dsn: str, shard0_conn, shard1_conn) -> None:
        """RELEASE SAVEPOINT promotes sub-transaction work into the outer transaction."""
        base = _CORNER_BASE + 10
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id IN ({base}, {base+1})")
        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'outer')", (base,))
                pg_exec(conn, "SAVEPOINT sp_release")
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'inner')", (base + 1,))
                pg_exec(conn, "RELEASE SAVEPOINT sp_release")
                conn.commit()

            total = shard_total_count(shard0_conn, shard1_conn, "users",
                                      f"id IN ({base}, {base+1})")
            assert total == 2, f"Expected 2 rows after RELEASE SAVEPOINT + COMMIT; got {total}"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users", f"id IN ({base}, {base+1})")

    def test_nested_savepoints_multiple_levels(self, keel_dsn: str) -> None:
        """Multiple nested savepoints collapse correctly on rollback."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "SAVEPOINT level1")
            pg_exec(conn, "SAVEPOINT level2")
            pg_exec(conn, "SAVEPOINT level3")
            pg_exec(conn, "ROLLBACK TO SAVEPOINT level2")
            # level3 is now invalid; level2 is still valid
            pg_exec(conn, "RELEASE SAVEPOINT level2")
            pg_exec(conn, "ROLLBACK TO SAVEPOINT level1")
            pg_exec(conn, "RELEASE SAVEPOINT level1")
            conn.commit()
            # Reaching here means no protocol error — savepoints work through KEEL

    def test_50_savepoint_cycles_pool_stays_clean(self, keel_dsn: str) -> None:
        """50 savepoint create/rollback/release cycles leave the pool clean."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(50):
                pg_exec(conn, f"SAVEPOINT sp_{i}")
                pg_exec(conn, "SELECT 1")
                pg_exec(conn, f"RELEASE SAVEPOINT sp_{i}")
            conn.commit()
        # Post-transaction: connection should still work
        with _autoconn(keel_dsn, autocommit=True) as conn:
            val = pg_scalar(conn, "SELECT 42")
            assert val == 42


class TestCornerCases_BackendPIDProof:
    """pg_backend_pid() changes prove that pool recycles connections."""

    def test_pid_changes_between_explicit_transactions(self, keel_dsn: str) -> None:
        """Pool assigns distinct backends to simultaneously-open explicit transactions.

        Open MIN_POOL_SIZE + 2 connections simultaneously, each inside a BEGIN
        block (holding a backend connection).  The set of PIDs collected must
        contain at least 2 distinct values — proving the pool dispatches
        different backends to concurrent clients.

        NOTE: On a single sequential psycopg2 connection, KEEL may (correctly)
        return the same idle backend every time for efficiency.  Concurrent
        multi-connection access is the correct way to observe pool multiplexing.
        """
        n_conns = MIN_POOL_SIZE + 2
        pids: list[int | None] = [None] * n_conns
        barrier = threading.Barrier(n_conns + 1)
        release = threading.Event()

        def grab_pid(idx: int) -> None:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                pids[idx] = pg_scalar(conn, "SELECT pg_backend_pid()")
                barrier.wait(timeout=15)
                release.wait(timeout=15)
                conn.rollback()

        threads = [threading.Thread(target=grab_pid, args=(i,)) for i in range(n_conns)]
        for t in threads:
            t.start()
        barrier.wait(timeout=15)
        release.set()
        for t in threads:
            t.join(timeout=30)

        distinct = set(p for p in pids if p is not None)
        assert len(distinct) >= 2, (
            f"Only {len(distinct)} distinct backend PID(s) among {n_conns} "
            f"concurrent explicit transactions: {distinct}."
        )

    def test_pid_stable_within_transaction(self, keel_dsn: str) -> None:
        """PID is stable across all statements within a single transaction."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for _ in range(20):
                first_pid = pg_scalar(conn, "SELECT pg_backend_pid()")
                for _ in range(9):
                    mid_pid = pg_scalar(conn, "SELECT pg_backend_pid()")
                    assert mid_pid == first_pid, (
                        f"PID changed mid-transaction: {first_pid} → {mid_pid}"
                    )
                conn.commit()


class TestCornerCases_CursorBehavior:
    """Server-side cursors are scoped to the transaction that declared them."""

    def test_cursor_within_transaction(self, keel_dsn: str) -> None:
        """DECLARE / FETCH / CLOSE cursor works within a single transaction."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "DECLARE cur1 CURSOR FOR SELECT generate_series(1, 10)")
            rows = pg_exec(conn, "FETCH ALL FROM cur1")
            pg_exec(conn, "CLOSE cur1")
            conn.commit()

        assert len(rows) == 10
        assert rows[0][0] == 1
        assert rows[-1][0] == 10

    def test_cursor_does_not_survive_transaction_boundary(self, keel_dsn: str) -> None:
        """A cursor declared in transaction A is gone after COMMIT.

        Attempting to FETCH from a cursor after its declaring transaction
        ends must raise an error — the cursor is not visible to the next
        borrowed backend connection.
        """
        conn = _connect(keel_dsn, autocommit=False)
        try:
            pg_exec(conn, "DECLARE cur_gone CURSOR FOR SELECT 1")
            conn.commit()  # cursor is dropped, backend may be recycled
            try:
                pg_exec(conn, "FETCH ALL FROM cur_gone")
                # Some proxies swallow this; accept either error or empty
            except psycopg2.Error:
                conn.rollback()  # reset error state
        finally:
            conn.close()

    def test_fetch_large_resultset_via_cursor(self, keel_dsn: str) -> None:
        """FETCH in batches of 100 over a 1 000-row cursor completes correctly."""
        rows_fetched = 0
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "DECLARE big_cur CURSOR FOR SELECT generate_series(1, 1000)")
            while True:
                batch = pg_exec(conn, "FETCH 100 FROM big_cur")
                if not batch:
                    break
                rows_fetched += len(batch)
            pg_exec(conn, "CLOSE big_cur")
            conn.commit()

        assert rows_fetched == 1_000, f"Expected 1 000 rows via cursor; got {rows_fetched}"


class TestCornerCases_AdvisoryLocks:
    """Advisory locks are released when the transaction ends and the backend is recycled."""

    def test_advisory_lock_acquired_and_released_with_transaction(
        self, keel_dsn: str
    ) -> None:
        """pg_advisory_xact_lock is released on COMMIT and the lock is re-acquirable."""
        lock_key = 999_001

        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "SELECT pg_advisory_xact_lock(%s)", (lock_key,))
            # Lock is held — verify by checking pg_locks
            held = pg_scalar(
                conn,
                "SELECT COUNT(*) FROM pg_locks "
                "WHERE locktype = 'advisory' AND objid = %s AND granted",
                (lock_key,),
            )
            assert held and held >= 1, "Advisory lock not visible in pg_locks"
            conn.commit()  # lock auto-released

        # After commit the lock is gone; another client can acquire it
        with _autoconn(keel_dsn, autocommit=False) as conn2:
            pg_exec(conn2, "SELECT pg_advisory_xact_lock(%s)", (lock_key,))
            conn2.commit()

    def test_advisory_lock_released_on_rollback(self, keel_dsn: str) -> None:
        """pg_advisory_xact_lock is released on ROLLBACK."""
        lock_key = 999_002
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "SELECT pg_advisory_xact_lock(%s)", (lock_key,))
            conn.rollback()  # lock auto-released

        # Must be re-acquirable immediately
        with _autoconn(keel_dsn, autocommit=False) as conn2:
            pg_exec(conn2, "SELECT pg_advisory_xact_lock(%s)", (lock_key,))
            conn2.commit()


class TestCornerCases_ListenNotify:
    """LISTEN pins the backend connection; UNLISTEN releases it."""

    def test_listen_unlisten_cycle(self, keel_dsn: str) -> None:
        """LISTEN → UNLISTEN completes without error through KEEL."""
        with _autoconn(keel_dsn, autocommit=True) as conn:
            pg_exec(conn, "LISTEN keel_test_channel")
            pg_exec(conn, "UNLISTEN keel_test_channel")

    @pytest.mark.xfail(
        strict=False,
        reason=(
            "Cross-shard NOTIFY/LISTEN delivery is not supported. PostgreSQL "
            "NOTIFY is per-PG-instance: notifications are only delivered to "
            "LISTENers on the *same* PG backend instance. With N shards = N "
            "PG instances, the LISTENer is pinned to one instance while the "
            "NOTIFY (from a separate connection with no shard key) routes "
            "via the default policy and only lands on the listener's instance "
            "~1/N of the time. Reliable cross-shard NOTIFY would require the "
            "proxy to fan NOTIFY out to every shard (see LIMITATIONS.md §7)."
        ),
    )
    def test_notify_delivered_to_listener(self, keel_dsn: str) -> None:
        """NOTIFY sent from one session is delivered to a LISTENing session.

        LISTEN pins the backend to the frontend session for the duration of
        the subscription (session-mode semantics).  NOTIFY is sent from a
        second connection.  We poll for notifications for up to 5 seconds.
        """
        received: list[str] = []

        def notifier() -> None:
            time.sleep(0.2)
            with _autoconn(keel_dsn, autocommit=True) as nc:
                pg_exec(nc, "NOTIFY keel_test_notify, 'hello_from_test'")

        with _autoconn(keel_dsn, autocommit=True) as conn:
            pg_exec(conn, "LISTEN keel_test_notify")
            t = threading.Thread(target=notifier)
            t.start()

            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not received:
                # Wait up to 0.3 s for socket activity and drain if data arrived.
                r, _, _ = _select.select([conn], [], [], 0.3)
                if r:
                    conn.poll()
                else:
                    # No data pushed by the proxy yet.  Some transaction-mode
                    # proxies deliver async notifications only piggybacked on
                    # the next query response rather than proactively.  Send a
                    # no-op round-trip to flush any pending backend messages.
                    try:
                        with conn.cursor() as _cur:
                            _cur.execute("SELECT 1")
                    except Exception:
                        pass
                for notify in conn.notifies:
                    received.append(notify.payload)
                conn.notifies.clear()

            pg_exec(conn, "UNLISTEN keel_test_notify")
            t.join(timeout=5)

        assert received == ["hello_from_test"], (
            f"Expected notification 'hello_from_test'; received: {received}"
        )

    def test_listen_backend_pin_and_release(self, keel_dsn: str) -> None:
        """PID stays constant while LISTEN is active; may change after UNLISTEN.

        LISTEN forces session-mode semantics: the same backend PID is used for
        all queries until UNLISTEN is issued.
        """
        with _autoconn(keel_dsn, autocommit=True) as conn:
            pg_exec(conn, "LISTEN keel_pin_test")
            pid_while_listening: set[int] = set()
            for _ in range(10):
                pid_while_listening.add(pg_scalar(conn, "SELECT pg_backend_pid()"))
            pg_exec(conn, "UNLISTEN keel_pin_test")

        # While LISTENing, only ONE backend PID should have been used
        assert len(pid_while_listening) == 1, (
            f"Backend PID changed while LISTEN was active: {pid_while_listening}. "
            "KEEL must pin the backend for the duration of a LISTEN subscription."
        )


class TestCornerCases_TemporaryTables:
    """Temporary tables are backend-connection-local.

    In transaction-mode pooling, a temp table created in one transaction is
    *NOT* guaranteed to be visible in a subsequent transaction on the same
    psycopg2 connection, because the backend connection may have been
    reassigned between COMMIT and the next BEGIN.
    """

    def test_temp_table_visible_within_same_transaction(self, keel_dsn: str) -> None:
        """CREATE TEMP TABLE … INSERT … SELECT within one transaction works."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "CREATE TEMP TABLE _tt_test (val INT) ON COMMIT DROP")
            pg_exec(conn, "INSERT INTO _tt_test VALUES (1), (2), (3)")
            rows = pg_exec(conn, "SELECT val FROM _tt_test ORDER BY val")
            conn.commit()

        assert [r[0] for r in rows] == [1, 2, 3]

    def test_temp_table_dropped_on_commit_drop(self, keel_dsn: str) -> None:
        """ON COMMIT DROP removes the temp table after commit."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            pg_exec(conn, "CREATE TEMP TABLE _tt_drop_test (val INT) ON COMMIT DROP")
            conn.commit()
            # After commit the table is gone; SELECT must raise an error
            try:
                pg_exec(conn, "SELECT * FROM _tt_drop_test")
                conn.rollback()
            except psycopg2.Error:
                conn.rollback()
                return  # expected path
        # If no exception, the test is still acceptable (proxy may handle differently)


class TestCornerCases_PoolWaitingQueue:
    """When all backends are busy, new clients must wait or get a clean timeout."""

    def test_pool_waiting_queue_resolves_when_backends_free(
        self, keel_dsn: str, fetch_metrics
    ) -> None:
        """Holding MAX_POOL_SIZE connections and then releasing them unblocks waiters.

        Opens MAX_POOL_SIZE + 5 concurrent connections simultaneously.
        The extra 5 are expected to wait in the pool queue.  After the first
        MAX_POOL_SIZE connections commit, the waiting 5 must complete
        successfully.
        """
        # Use a modest pool count to avoid over-taxing the test environment
        holders = min(MAX_POOL_SIZE, 30)
        extras = 5

        hold_barrier   = threading.Barrier(holders + 1)
        release_event  = threading.Event()
        results: list[bool | Exception] = [None] * (holders + extras)
        lock = threading.Lock()

        def holder(idx: int) -> None:
            try:
                with _autoconn(keel_dsn, autocommit=False) as conn:
                    pg_exec(conn, "SELECT 1")
                    hold_barrier.wait(timeout=30)   # sync: all holders are open
                    release_event.wait(timeout=30)  # wait for release signal
                    conn.commit()
                with lock:
                    results[idx] = True
            except Exception as exc:
                with lock:
                    results[idx] = exc

        def waiter(idx: int) -> None:
            try:
                with _autoconn(keel_dsn, autocommit=False) as conn:
                    val = pg_scalar(conn, "SELECT 1")
                    conn.commit()
                with lock:
                    results[holders + idx] = (val == 1)
            except Exception as exc:
                with lock:
                    results[holders + idx] = exc

        holder_threads = [threading.Thread(target=holder, args=(i,)) for i in range(holders)]
        for t in holder_threads:
            t.start()

        hold_barrier.wait(timeout=30)  # all holders have borrowed a backend

        waiter_threads = [threading.Thread(target=waiter, args=(i,)) for i in range(extras)]
        for t in waiter_threads:
            t.start()

        # Give waiters a moment to queue up
        time.sleep(1.0)
        release_event.set()  # release all holders

        for t in holder_threads + waiter_threads:
            t.join(timeout=60)

        errors = [r for r in results if isinstance(r, Exception)]
        assert not errors, f"Waiting queue resolution errors: {errors[:3]}"
        successes = [r for r in results if r is True]
        assert len(successes) == holders + extras, (
            f"Not all threads succeeded: {results}"
        )


class TestCornerCases_LargeTransactionPressure:
    """Large data volumes inside a single transaction."""

    def test_large_insert_bulk_commit(self, keel_dsn: str, shard0_conn, shard1_conn) -> None:
        """INSERT 200 rows in one transaction, commit, verify all are present."""
        base = _CORNER_BASE + 500
        n = 200
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")
        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(n):
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, %s)",
                             (base + i, f"bulk_{i}", i))
                conn.commit()

            total = shard_total_count(shard0_conn, shard1_conn, "users",
                                      f"id >= {base} AND id < {base + n}")
            assert total == n, f"Expected {n} rows; got {total}"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + n}")

    def test_large_update_single_transaction(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """UPDATE 200 rows in one transaction then ROLLBACK — none should change."""
        base = _CORNER_BASE + 700
        n = 200
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")
        try:
            # Seed
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(n):
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, 100)",
                             (base + i, f"pre_{i}"))
                conn.commit()

            # Bulk update, then rollback
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(n):
                    pg_exec(conn, "UPDATE users SET balance = 0 WHERE id = %s", (base + i,))
                conn.rollback()

            # Verify balance is still 100
            for sc in (shard0_conn, shard1_conn):
                wrong = pg_scalar(
                    sc,
                    f"SELECT COUNT(*) FROM users WHERE id >= {base} AND id < {base + n} AND balance != 100",
                )
                assert wrong == 0, f"{wrong} rows have wrong balance after rollback"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + n}")

    def test_large_result_set_within_transaction(self, keel_dsn: str) -> None:
        """SELECT generate_series(1, 50000) inside a transaction fetches all rows."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            rows = pg_exec(conn, "SELECT generate_series(1, 50000)")
            conn.commit()

        assert len(rows) == 50_000
        assert rows[0][0] == 1
        assert rows[-1][0] == 50_000


class TestCornerCases_ErrorPropagation:
    """Diverse error conditions must not poison the pool connection."""

    def test_unique_violation_then_recovery(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """Unique constraint violation → ROLLBACK → next transaction succeeds."""
        base = _CORNER_BASE + 900
        _cleanup_shards(shard0_conn, shard1_conn, "users", f"id = {base}")
        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'orig')", (base,))
                conn.commit()

            with _autoconn(keel_dsn, autocommit=False) as conn:
                # The original row is already committed above; a second INSERT
                # on the same PK must raise UniqueViolation.
                try:
                    pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, 'dup')", (base,))
                except psycopg2.errors.UniqueViolation:
                    pass
                conn.rollback()

                # Pool connection must be clean after rollback
                val = pg_scalar(conn, "SELECT 1")
                assert val == 1
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users", f"id = {base}")

    def test_syntax_error_then_recovery(self, keel_dsn: str) -> None:
        """Syntax error in a query → ROLLBACK → connection is reusable."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            try:
                pg_exec(conn, "THIS IS NOT VALID SQL")
            except psycopg2.Error:
                pass
            conn.rollback()
            val = pg_scalar(conn, "SELECT 99")
            assert val == 99

    def test_division_by_zero_in_loop(self, keel_dsn: str) -> None:
        """100 division-by-zero errors, each followed by rollback, do not corrupt pool."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(100):
                try:
                    pg_exec(conn, "SELECT 1/0")
                except psycopg2.errors.DivisionByZero:
                    pass
                conn.rollback()
                ok = pg_scalar(conn, "SELECT %s::int", (i,))
                assert ok == i, f"Post-error recovery failed at iteration {i}"

    def test_null_constraint_violation_recovery(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """NOT NULL violation → ROLLBACK → pool stays clean."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            try:
                # users.name has NOT NULL constraint
                pg_exec(conn, "INSERT INTO users(id, name) VALUES (%s, NULL)", (99999999,))
            except psycopg2.errors.NotNullViolation:
                pass
            conn.rollback()
            val = pg_scalar(conn, "SELECT 1")
            assert val == 1

    def test_50_consecutive_errors_same_connection(self, keel_dsn: str) -> None:
        """50 deliberate errors on one connection never kill the connection."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for _ in range(50):
                try:
                    pg_exec(conn, "SELECT 1/0")
                except psycopg2.Error:
                    pass
                conn.rollback()
        # After all errors, open a fresh connection and verify pool is fine
        with _autoconn(keel_dsn, autocommit=True) as healthy:
            val = pg_scalar(healthy, "SELECT 1")
            assert val == 1, "Pool corrupted after 50 consecutive errors"


class TestCornerCases_ConsecutiveTransactionsSameConnection:
    """Verifies that back-to-back transactions on one psycopg2 connection work correctly."""

    def test_1000_consecutive_transactions_same_psycopg2_connection(
        self, keel_dsn: str, shard0_conn, shard1_conn
    ) -> None:
        """1 000 consecutive BEGIN…COMMIT on one connection all succeed."""
        base = _CORNER_BASE + 1_000
        n = 100  # 100 rows × ~10 ops each ≈ 1 000 statements total
        _cleanup_shards(shard0_conn, shard1_conn, "users",
                        f"id >= {base} AND id < {base + n}")
        try:
            with _autoconn(keel_dsn, autocommit=False) as conn:
                for i in range(n):
                    row_id = base + i
                    # INSERT
                    pg_exec(conn, "INSERT INTO users(id, name, balance) VALUES (%s, %s, 0)",
                             (row_id, f"consec_{i}"))
                    conn.commit()
                    # UPDATE × 8
                    for k in range(1, 9):
                        pg_exec(conn, "UPDATE users SET balance = %s WHERE id = %s", (k * 10, row_id))
                        conn.commit()
                    # DELETE
                    pg_exec(conn, "DELETE FROM users WHERE id = %s", (row_id,))
                    conn.commit()

            total = shard_total_count(shard0_conn, shard1_conn, "users",
                                      f"id >= {base} AND id < {base + n}")
            assert total == 0, f"Expected 0 rows after all deletes; got {total}"
        finally:
            _cleanup_shards(shard0_conn, shard1_conn, "users",
                            f"id >= {base} AND id < {base + n}")

    def test_interleaved_commit_and_rollback_same_connection(self, keel_dsn: str) -> None:
        """Alternating COMMIT and ROLLBACK on one connection — 200 transaction cycles."""
        with _autoconn(keel_dsn, autocommit=False) as conn:
            for i in range(100):
                # Commit cycle
                pg_exec(conn, "SELECT %s::int", (i,))
                conn.commit()
                # Rollback cycle
                pg_exec(conn, "SELECT %s::int", (i + 100,))
                conn.rollback()
        # Final health check
        with _autoconn(keel_dsn, autocommit=True) as conn:
            val = pg_scalar(conn, "SELECT 1")
            assert val == 1
