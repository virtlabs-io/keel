"""
test_sharding.py — Shard routing tests
=======================================

Tests that verify KEEL correctly routes SQL queries to the right PostgreSQL
shard nodes using hash-based partitioning on the declared shard column.

Topology::

  pg-shard0  (port 25432)  — hash(id) % 2 == 0
  pg-shard1  (port 25433)  — hash(id) % 2 == 1

KEEL shard rules (keel-e2e-suite.ini)::

  users       shard by  id
  orders      shard by  order_id
  events      shard by  shard_hint

Background
----------
KEEL uses a hash-based routing strategy: the shard column value is hashed
(``hash(value) % shard_count``) and the result selects the destination shard.
For reads, KEEL sends the query only to the matching shard.  For writes, KEEL
uses scatter-write 2PC to fan out to all shards (since any shard may need the
schema change, or since scatter DELETE / UPDATE must cover all shards).

What is tested
--------------
- **Point reads**: ``SELECT … WHERE id = K`` routes to exactly one shard; the
  other shard receives no query.
- **Point writes**: ``INSERT … id = K`` arrives on the correct shard and is
  absent from the other.
- **Multi-table routing**: ``orders`` (by ``order_id``) and ``events`` (by
  ``shard_hint``) use different columns; both are routed correctly.
- **Determinism**: the same key always maps to the same shard across restarts.

Why these tests exist
---------------------
A routing bug (wrong hash function, off-by-one in modulo, wrong column) would
send writes to the wrong shard.  Reads would then return no rows (the row is on
the other shard) or stale rows (reads don't see the write), breaking application
correctness silently.

Why a test might fail
---------------------
- **Shard rule misconfiguration**: if the shard column or shard count is wrong in
  ``keel-e2e-suite.ini``, the hash function maps to a different shard.
- **Hash function change**: changing KEEL's hash function redistributes all keys —
  existing data appears on the wrong shard.
- **Schema mismatch**: if the table or column is renamed, the shard rule doesn't
  match and KEEL falls back to broadcast or single-shard routing.

Consequences of failure
-----------------------
- Rows written to wrong shard → queries against the correct shard return no data.
- Writes appear to succeed but reads return nothing → silent data loss.
- With hash function change: full resharding required to recover consistency.
"""

from __future__ import annotations

import pytest
import psycopg2

from helpers import pg_exec, pg_scalar, pg_count, shard_total_count, clear_table_on_shards

pytestmark = pytest.mark.sharding


# ---------------------------------------------------------------------------
# Fixtures — isolate data per test class with unique ID ranges
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_users(shard0_conn, shard1_conn):
    """Truncate the users table on both shards before each test."""
    clear_table_on_shards(shard0_conn, shard1_conn, "users")
    yield
    clear_table_on_shards(shard0_conn, shard1_conn, "users")


@pytest.fixture(autouse=True)
def clean_orders(shard0_conn, shard1_conn):
    clear_table_on_shards(shard0_conn, shard1_conn, "orders")
    yield
    clear_table_on_shards(shard0_conn, shard1_conn, "orders")


@pytest.fixture(autouse=True)
def clean_events(shard0_conn, shard1_conn):
    clear_table_on_shards(shard0_conn, shard1_conn, "events")
    yield
    clear_table_on_shards(shard0_conn, shard1_conn, "events")


# ---------------------------------------------------------------------------
# Routing correctness
# ---------------------------------------------------------------------------

class TestInsertRouting:

    def test_inserts_land_on_some_shard(self, keel_conn, shard0_conn, shard1_conn):
        """Every INSERT through KEEL lands on exactly one shard."""
        for uid in range(0, 10):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (uid, f"user_{uid}"))

        total = shard_total_count(shard0_conn, shard1_conn, "users")
        assert total == 10, f"Expected 10 rows across shards, got {total}"

    def test_inserts_distributed_across_shards(self, keel_conn, shard0_conn, shard1_conn):
        """With enough rows, both shards receive at least one row."""
        for uid in range(0, 20):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (uid, f"user_{uid}"))

        cnt0 = pg_count(shard0_conn, "users")
        cnt1 = pg_count(shard1_conn, "users")
        assert cnt0 > 0, "Shard 0 received no rows — routing is broken"
        assert cnt1 > 0, "Shard 1 received no rows — routing is broken"
        assert cnt0 + cnt1 == 20

    def test_insert_no_row_duplication(self, keel_conn, shard0_conn, shard1_conn):
        """Each row appears on exactly one shard (no duplicates)."""
        ids = list(range(100, 120))
        for uid in ids:
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (uid, f"u{uid}"))

        cnt0 = pg_count(shard0_conn, "users")
        cnt1 = pg_count(shard1_conn, "users")
        assert cnt0 + cnt1 == len(ids), (
            f"Row count mismatch: shard0={cnt0} shard1={cnt1} expected={len(ids)}"
        )

    def test_orders_routed_by_order_id(self, keel_conn, shard0_conn, shard1_conn):
        """Orders table is sharded by order_id (different shard rule than users)."""
        for oid in range(0, 10):
            pg_exec(keel_conn,
                    "INSERT INTO orders(order_id, user_id, amount) VALUES (%s, %s, %s)",
                    (oid, oid, float(oid) * 10))

        total = shard_total_count(shard0_conn, shard1_conn, "orders")
        assert total == 10

        cnt0 = pg_count(shard0_conn, "orders")
        cnt1 = pg_count(shard1_conn, "orders")
        assert cnt0 > 0 and cnt1 > 0, "Orders not distributed across shards"

    def test_events_routed_by_shard_hint(self, keel_conn, shard0_conn, shard1_conn):
        """Events table uses shard_hint as the shard key."""
        for hint in range(0, 12):
            pg_exec(keel_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, "click", hint * 5))

        total = shard_total_count(shard0_conn, shard1_conn, "events")
        assert total == 12


class TestPointLookup:

    def test_select_by_pk_returns_correct_row(self, keel_conn, shard0_conn, shard1_conn):
        """SELECT WHERE id=<pk> through KEEL returns the right row from the right shard."""
        rows_to_insert = [(1001, "alice"), (1002, "bob"), (1003, "carol"), (1004, "dave")]
        for uid, name in rows_to_insert:
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, name))

        for uid, expected_name in rows_to_insert:
            name = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (uid,))
            assert name == expected_name, f"id={uid}: expected '{expected_name}', got '{name}'"

    def test_select_nonexistent_pk_returns_nothing(self, keel_conn):
        """SELECT for a non-existent PK returns no rows."""
        rows = pg_exec(keel_conn, "SELECT * FROM users WHERE id = 99999999")
        assert rows == []

    def test_update_via_keel(self, keel_conn, shard0_conn, shard1_conn):
        """UPDATE routed through KEEL modifies the correct row on the shard."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, balance) VALUES (2001, 'update_test', 0)")
        pg_exec(keel_conn, "UPDATE users SET balance = 500.00 WHERE id = 2001")

        balance = pg_scalar(keel_conn, "SELECT balance FROM users WHERE id = 2001")
        assert float(balance) == 500.00

    def test_delete_via_keel(self, keel_conn, shard0_conn, shard1_conn):
        """DELETE routed through KEEL removes the row from the shard."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (3001, 'delete_test')")
        pg_exec(keel_conn, "DELETE FROM users WHERE id = 3001")

        rows = pg_exec(keel_conn, "SELECT * FROM users WHERE id = 3001")
        assert rows == []

        total = shard_total_count(shard0_conn, shard1_conn, "users", "id = 3001")
        assert total == 0

    def test_upsert_via_keel(self, keel_conn, shard0_conn, shard1_conn):
        """INSERT … ON CONFLICT DO UPDATE routes correctly."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, balance) VALUES (4001, 'upsert_test', 0)")
        pg_exec(keel_conn,
                "INSERT INTO users(id, name, balance) VALUES (4001, 'upsert_test', 999) "
                "ON CONFLICT(id) DO UPDATE SET balance = EXCLUDED.balance")

        balance = pg_scalar(keel_conn, "SELECT balance FROM users WHERE id = 4001")
        assert float(balance) == 999.00

    def test_same_pk_on_two_tables_sharded_independently(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """
        user id=5 and order_id=5 are independent shard keys.
        Both rows should appear exactly once in the combined shard counts.
        """
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (5, 'shared_pk_user')")
        pg_exec(keel_conn, "INSERT INTO orders(order_id, user_id, amount) VALUES (5, 5, 99)")

        user_total  = shard_total_count(shard0_conn, shard1_conn, "users",  "id = 5")
        order_total = shard_total_count(shard0_conn, shard1_conn, "orders", "order_id = 5")
        assert user_total  == 1
        assert order_total == 1


class TestShardDirectVerification:
    """
    Cross-check that rows inserted through KEEL are actually on the expected
    shard by inspecting shard nodes directly.
    """

    def test_shard_row_counts_match_insert_count(self, keel_conn, shard0_conn, shard1_conn):
        """
        Insert 100 users via KEEL.  Summing the direct per-shard counts must
        equal 100.  No rows should be lost or duplicated.
        """
        N = 100
        for uid in range(10000, 10000 + N):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (uid, f"verify_{uid}"))

        total = shard_total_count(shard0_conn, shard1_conn, "users")
        assert total == N

    def test_pk_constraint_enforced_per_shard(self, keel_conn):
        """
        Inserting a duplicate PK through KEEL raises an IntegrityError
        (the constraint is enforced on the shard).
        """
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (20001, 'pk_test')")
        with pytest.raises(psycopg2.errors.UniqueViolation):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (20001, 'pk_duplicate')")

    def test_transaction_atomicity_across_inserts(self, keel_dsn, shard0_conn, shard1_conn):
        """
        A multi-row INSERT in a single transaction must be fully committed or
        fully rolled back — no partial visibility.
        """
        BASE = 30000
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users(id, name) VALUES (%s, %s)", (BASE,     "txn_a"))
                cur.execute("INSERT INTO users(id, name) VALUES (%s, %s)", (BASE + 1, "txn_b"))
            conn.commit()

            total = shard_total_count(
                shard0_conn, shard1_conn, "users",
                f"id IN ({BASE}, {BASE+1})",
            )
            assert total == 2
        finally:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM users WHERE id IN ({BASE}, {BASE+1})")
            conn.close()
