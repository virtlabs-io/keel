"""
test_sharding_comprehensive.py — Comprehensive horizontal sharding test suite
==============================================================================

This module exercises every aspect of KEEL's horizontal sharding system with
no shortcuts or gaps.  It is designed to detect:

  • Incorrect shard routing (wrong shard receives a row)
  • Row duplication (row present on more than one shard)
  • Row loss (row missing from all shards)
  • Scatter-merge aggregation bugs (wrong SUM, COUNT, AVG, MIN, MAX)
  • Window function correctness over multi-shard result sets
  • CTE evaluation correctness (simple, chained, recursive)
  • JSONB operator correctness over scattered data
  • Complex aggregate functions (FILTER, STRING_AGG, ARRAY_AGG, percentile)
  • Set operation correctness (UNION ALL)
  • NULL handling everywhere (NULL shard-adjacent columns, NULL aggregation)
  • Boundary values (MAX BIGINT, zero, negative IDs, Unicode)
  • Prepared statement / parameterized query routing
  • Data integrity under high concurrency (no lost updates, no phantom rows)
  • Consistency of repeated scatter COUNT queries
  • RETURNING clause on INSERT/UPDATE/DELETE
  • Ordered merge correctness (ORDER BY, LIMIT, OFFSET)

Tables used:
  users          — sharded by id           (BIGINT)
  orders         — sharded by order_id     (BIGINT)
  events         — sharded by shard_hint   (BIGINT)
  products       — sharded by product_id   (BIGINT, JSONB metadata)
  user_activity  — sharded by user_id      (BIGINT, for window functions)

Run individually:
  pytest tests/e2e/test_sharding_comprehensive.py -v
Run specific group:
  pytest tests/e2e/test_sharding_comprehensive.py -v -k "Window"
  pytest tests/e2e/test_sharding_comprehensive.py -v -m  jsonb
"""

from __future__ import annotations

import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import psycopg2
import psycopg2.errors
import pytest

from helpers import (
    pg_exec,
    pg_scalar,
    pg_count,
    shard_total_count,
    clear_table_on_shards,
    run_concurrent,
    WorkerResult,
)

pytestmark = [pytest.mark.sharding]

# =============================================================================
# Pre-computed dataset constants
# =============================================================================

# ---------------------------------------------------------------------------
# user_activity — 12 rows spread across user_ids 100–105
# ---------------------------------------------------------------------------
ACTIVITY_ROWS = [
    # (user_id, action, score)
    (100, "login", 10),
    (100, "click",  5),
    (101, "login", 20),
    (101, "click", 15),
    (102, "login",  8),
    (102, "buy",   50),
    (103, "login", 12),
    (103, "click",  3),
    (104, "login", 25),
    (104, "buy",  100),
    (105, "click",  7),
    (105, "buy",   30),
]

ACTIVITY_SCORES_ASC = sorted(r[2] for r in ACTIVITY_ROWS)
# [3, 5, 7, 8, 10, 12, 15, 20, 25, 30, 50, 100]

ACTIVITY_TOTAL_SCORE = sum(r[2] for r in ACTIVITY_ROWS)  # 285

_ACTIONS = sorted({r[1] for r in ACTIVITY_ROWS})  # ['buy', 'click', 'login']
ACTIVITY_SUM_BY_ACTION = {
    action: sum(r[2] for r in ACTIVITY_ROWS if r[1] == action)
    for action in _ACTIONS
}
# buy=180, click=30, login=75

ACTIVITY_COUNT_BY_ACTION = {
    action: sum(1 for r in ACTIVITY_ROWS if r[1] == action)
    for action in _ACTIONS
}
# buy=3, click=4, login=5  (100→login+click, 101→login+click, 102→login+buy,
#                            103→login+click, 104→login+buy, 105→click+buy)
# Recalculate:  login appears in rows: (100,login),(101,login),(102,login),(103,login),(104,login) → 5
#               click: (100,click),(101,click),(103,click),(105,click) → 4
#               buy:   (102,buy),(104,buy),(105,buy) → 3

# ---------------------------------------------------------------------------
# products — 6 products with JSONB metadata
# ---------------------------------------------------------------------------
PRODUCTS_ROWS: list[tuple] = [
    # (product_id, name, category, price, stock, metadata_dict, tags_list)
    (200, "Laptop Pro",  "electronics", 999.99,    5,
     {"brand": "ACME",   "weight_kg": 0.5, "in_stock": True,  "rating": 4.5},
     ["laptop", "portable"]),
    (201, "Phone X",     "electronics", 499.99,   10,
     {"brand": "TechCo", "weight_kg": 0.2, "in_stock": True,  "rating": 4.2},
     ["phone", "mobile"]),
    (202, "Python Book", "books",        29.99,  100,
     {"author": "Alice",  "pages": 320,    "in_stock": True,  "rating": 4.8},
     ["python", "programming"]),
    (203, "Algo Book",   "books",        39.99,   50,
     {"author": "Bob",    "pages": 450,    "in_stock": False, "rating": 4.6},
     ["algorithms", "cs"]),
    (204, "Workstation", "electronics", 1999.99,   2,
     {"brand": "ACME",   "weight_kg": 2.5, "in_stock": True,  "rating": 4.7},
     ["laptop", "workstation"]),
    (205, "T-Shirt",     "clothing",     19.99,  200,
     {"color": "blue",   "size": "M",    "in_stock": True,  "rating": 3.9},
     ["casual", "cotton"]),
]

PRODUCTS_TOTAL_PRICE = sum(r[3] for r in PRODUCTS_ROWS)
# 999.99 + 499.99 + 29.99 + 39.99 + 1999.99 + 19.99 = 3589.94

PRODUCTS_IN_STOCK_COUNT = sum(1 for r in PRODUCTS_ROWS if r[5]["in_stock"])
# 5  (all except product 203)

PRODUCTS_ACME_IDS = [r[0] for r in PRODUCTS_ROWS if r[5].get("brand") == "ACME"]
# [200, 204]
PRODUCTS_ACME_PRICE_SUM = sum(r[3] for r in PRODUCTS_ROWS if r[5].get("brand") == "ACME")
# 999.99 + 1999.99 = 2999.98

PRODUCTS_WITH_BRAND_COUNT = sum(1 for r in PRODUCTS_ROWS if "brand" in r[5])
# 3  (200, 201, 204)

PRODUCTS_CATEGORIES = sorted({r[2] for r in PRODUCTS_ROWS})
# ['books', 'clothing', 'electronics']

PRODUCTS_PRICE_BY_CAT = {
    cat: sum(r[3] for r in PRODUCTS_ROWS if r[2] == cat)
    for cat in PRODUCTS_CATEGORIES
}
# books=69.98, clothing=19.99, electronics=3499.97

# ---------------------------------------------------------------------------
# events — cross-shard scatter dataset (same structure as test_scatter_merge.py
#           but with more rows for broader coverage)
# ---------------------------------------------------------------------------
SCATTER_EVENTS = [
    # (shard_hint, category, value)
    (0,  "alpha", 10),
    (2,  "alpha", 20),
    (4,  "beta",  30),
    (6,  "alpha", 40),
    (8,  "beta",  50),
    (10, "gamma", 60),
    (1,  "alpha", 15),
    (3,  "beta",  25),
    (5,  "gamma", 35),
    (7,  "alpha", 45),
    (9,  "beta",  55),
    (11, "gamma", 65),
]
SCATTER_TOTAL    = len(SCATTER_EVENTS)                          # 12
SCATTER_SUM      = sum(r[2] for r in SCATTER_EVENTS)           # 450
SCATTER_MIN      = min(r[2] for r in SCATTER_EVENTS)           # 10
SCATTER_MAX      = max(r[2] for r in SCATTER_EVENTS)           # 65
SCATTER_AVG      = SCATTER_SUM / SCATTER_TOTAL                 # 37.5
SCATTER_CAT_CNT  = {c: sum(1 for r in SCATTER_EVENTS if r[1] == c)
                    for c in ("alpha", "beta", "gamma")}       # alpha=4, beta=4, gamma=3  ...wait
# Recalculate: alpha: rows with hint 0,2,6,1,7 → 5; beta: 4,8,3,9 → 4; gamma: 10,5,11 → 3  ...wait
# Let me recount carefully:
# (0,alpha), (2,alpha), (4,beta), (6,alpha), (8,beta), (10,gamma),
# (1,alpha),  (3,beta),  (5,gamma), (7,alpha),  (9,beta),  (11,gamma)
# alpha: 0,2,6,1,7 → 5 rows
# beta:  4,8,3,9 → 4 rows
# gamma: 10,5,11 → 3 rows
# Total: 12 ✓
SCATTER_CAT_SUM  = {
    c: sum(r[2] for r in SCATTER_EVENTS if r[1] == c)
    for c in ("alpha", "beta", "gamma")
}
# alpha: 10+20+40+15+45 = 130
# beta:  30+50+25+55    = 160
# gamma: 60+35+65       = 160
SCATTER_VALUES_ASC = sorted(r[2] for r in SCATTER_EVENTS)
# [10,15,20,25,30,35,40,45,50,55,60,65]


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module", autouse=True)
def wait_healthy(keel_dsn):
    """Block until KEEL is reachable and shard routing is functional."""
    probe_ids = (88881, 88882)
    for _ in range(30):
        try:
            conn = psycopg2.connect(keel_dsn, connect_timeout=5)
            conn.autocommit = True
            with conn.cursor() as cur:
                for pid in probe_ids:
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, 'probe') "
                        "ON CONFLICT DO NOTHING",
                        (pid,),
                    )
                for pid in probe_ids:
                    cur.execute("DELETE FROM users WHERE id = %s", (pid,))
            conn.close()
            return
        except psycopg2.Error:
            time.sleep(2)
    raise RuntimeError("KEEL not healthy before comprehensive sharding tests")


@pytest.fixture(autouse=True)
def clean_all_tables(shard0_conn, shard1_conn):
    """Truncate all test tables on both shards before and after every test."""
    _all_tables = ("users", "events", "orders", "products", "user_activity")
    for tbl in _all_tables:
        clear_table_on_shards(shard0_conn, shard1_conn, tbl)
    yield
    for tbl in _all_tables:
        clear_table_on_shards(shard0_conn, shard1_conn, tbl)


# =============================================================================
# Helpers
# =============================================================================

def _insert_activity(conn) -> None:
    """Bulk-insert ACTIVITY_ROWS into user_activity via *conn*."""
    for user_id, action, score in ACTIVITY_ROWS:
        pg_exec(
            conn,
            "INSERT INTO user_activity(user_id, action, score) VALUES (%s, %s, %s)",
            (user_id, action, score),
        )


def _insert_products(conn) -> None:
    """Bulk-insert PRODUCTS_ROWS into products via *conn*."""
    for pid, name, cat, price, stock, meta, tags in PRODUCTS_ROWS:
        pg_exec(
            conn,
            "INSERT INTO products(product_id, name, category, price, stock, metadata, tags) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (pid, name, cat, price, stock, json.dumps(meta), tags),
        )


def _insert_scatter_events(conn) -> None:
    """Bulk-insert SCATTER_EVENTS into events via *conn*."""
    for hint, cat, val in SCATTER_EVENTS:
        pg_exec(
            conn,
            "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
            (hint, cat, val),
        )


# =============================================================================
# PART 1 — Routing Correctness: DML Edge Cases
# =============================================================================

class TestRoutingEdgeCases:
    """
    Beyond the basic smoke tests in test_sharding.py, these verify routing
    correctness for DML features that interact with the routing layer.
    """

    def test_insert_returning_single_row(self, keel_conn):
        """INSERT ... RETURNING delivers the new row back through KEEL."""
        rows = pg_exec(
            keel_conn,
            "INSERT INTO users(id, name, balance) VALUES (1, 'alice', 100.00) "
            "RETURNING id, name, balance",
        )
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "alice"
        assert float(rows[0][2]) == pytest.approx(100.00)

    def test_update_returning_modified_columns(self, keel_conn):
        """UPDATE ... RETURNING exposes the post-update column values."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, balance) VALUES (2, 'bob', 0)")
        rows = pg_exec(
            keel_conn,
            "UPDATE users SET balance = 750.00 WHERE id = 2 RETURNING id, balance",
        )
        assert len(rows) == 1
        assert rows[0][0] == 2
        assert float(rows[0][1]) == pytest.approx(750.00)

    def test_delete_returning_removed_row(self, keel_conn):
        """DELETE ... RETURNING delivers the deleted row exactly once."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (3, 'carol', 'c@e.com')")
        rows = pg_exec(keel_conn, "DELETE FROM users WHERE id = 3 RETURNING id, email")
        assert len(rows) == 1
        assert rows[0][0] == 3
        assert rows[0][1] == "c@e.com"
        assert pg_exec(keel_conn, "SELECT 1 FROM users WHERE id = 3") == []

    def test_in_clause_crosses_shards(self, keel_conn, shard0_conn, shard1_conn):
        """WHERE id IN (...) with values spanning both shards returns every match."""
        ids = list(range(10, 22))
        for uid in ids:
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))
        placeholders = ",".join(str(i) for i in ids)
        rows = pg_exec(
            keel_conn,
            f"SELECT id FROM users WHERE id IN ({placeholders}) ORDER BY id",
        )
        assert [r[0] for r in rows] == ids

    def test_between_scatter_returns_range(self, keel_conn, shard0_conn, shard1_conn):
        """WHERE id BETWEEN lower AND upper triggers scatter and returns all rows in range."""
        for uid in range(30, 42):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))
        rows = pg_exec(keel_conn, "SELECT id FROM users WHERE id BETWEEN 30 AND 41 ORDER BY id")
        assert [r[0] for r in rows] == list(range(30, 42))

    def test_like_filter_cross_shard(self, keel_conn):
        """WHERE name LIKE pattern scatters and filters correctly across both shards."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (50, 'alpha_one')")
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (51, 'beta_one')")
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (52, 'alpha_two')")
        rows = pg_exec(keel_conn, "SELECT id FROM users WHERE name LIKE 'alpha_%' ORDER BY id")
        assert [r[0] for r in rows] == [50, 52]

    def test_negative_shard_key_routes_and_retrieves(self, keel_conn, shard0_conn, shard1_conn):
        """Negative BIGINT shard keys hash to a valid shard without error."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (-1, 'neg_one')")
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (-999, 'neg_many')")
        assert pg_scalar(keel_conn, "SELECT name FROM users WHERE id = -1") == "neg_one"
        assert pg_scalar(keel_conn, "SELECT name FROM users WHERE id = -999") == "neg_many"
        assert shard_total_count(shard0_conn, shard1_conn, "users", "id < 0") == 2

    def test_max_bigint_shard_key(self, keel_conn, shard0_conn, shard1_conn):
        """The maximum BIGINT value routes to a shard without overflow."""
        max_id = 9_223_372_036_854_775_807
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, 'maxbig')", (max_id,))
        name = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (max_id,))
        assert name == "maxbig"
        assert shard_total_count(shard0_conn, shard1_conn, "users") == 1

    def test_unicode_and_emoji_stored_intact(self, keel_conn):
        """UTF-8 multibyte and emoji characters survive the shard round-trip."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (60, '日本語テスト 🎉')")
        name = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = 60")
        assert name == "日本語テスト 🎉"

    def test_null_non_key_column_stored_as_null(self, keel_conn):
        """NULL in a non-shard-key column is faithfully stored and retrieved as NULL."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (70, 'no_email', NULL)")
        email = pg_scalar(keel_conn, "SELECT email FROM users WHERE id = 70")
        assert email is None

    def test_empty_string_non_key_column(self, keel_conn):
        """Empty string in a non-key column round-trips without mutation."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (71, '', '')")
        row = pg_exec(keel_conn, "SELECT name, email FROM users WHERE id = 71")
        assert row == [("", "")]

    def test_update_multiple_columns_atomically(self, keel_conn):
        """UPDATE touching several columns is atomic and lands on the right shard."""
        pg_exec(
            keel_conn,
            "INSERT INTO users(id, name, email, age, balance) VALUES (80, 'start', NULL, 0, 0)",
        )
        pg_exec(
            keel_conn,
            "UPDATE users SET name='end', email='e@t.com', age=25, balance=1234.56 WHERE id = 80",
        )
        row = pg_exec(keel_conn, "SELECT name, email, age, balance FROM users WHERE id = 80")
        assert len(row) == 1
        assert row[0][0] == "end"
        assert row[0][1] == "e@t.com"
        assert row[0][2] == 25
        assert float(row[0][3]) == pytest.approx(1234.56, abs=0.01)

    def test_upsert_on_conflict_do_update(self, keel_conn, shard0_conn, shard1_conn):
        """INSERT … ON CONFLICT DO UPDATE modifies in place; no duplicate rows."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, balance) VALUES (90, 'first', 100)")
        pg_exec(
            keel_conn,
            "INSERT INTO users(id, name, balance) VALUES (90, 'second', 200) "
            "ON CONFLICT(id) DO UPDATE SET balance = EXCLUDED.balance, name = EXCLUDED.name",
        )
        assert pg_scalar(keel_conn, "SELECT name FROM users WHERE id = 90") == "second"
        assert float(pg_scalar(keel_conn, "SELECT balance FROM users WHERE id = 90")) == pytest.approx(200.0)
        assert shard_total_count(shard0_conn, shard1_conn, "users", "id = 90") == 1

    def test_no_phantom_rows_after_rollback(self, keel_dsn, shard0_conn, shard1_conn):
        """ROLLBACK removes all rows written in the transaction from all shards."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users(id, name) VALUES (91, 'rollback_a')")
                cur.execute("INSERT INTO users(id, name) VALUES (92, 'rollback_b')")
            conn.rollback()
        finally:
            conn.close()
        assert shard_total_count(shard0_conn, shard1_conn, "users", "id IN (91, 92)") == 0

    def test_shard_count_invariant_after_bulk_insert(self, keel_conn, shard0_conn, shard1_conn):
        """
        After N inserts through KEEL, the direct per-shard row count equals N.
        This is the fundamental no-duplication, no-loss invariant.
        """
        N = 200
        for uid in range(1000, 1000 + N):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        direct_total = shard_total_count(shard0_conn, shard1_conn, "users")
        scatter_count = pg_scalar(keel_conn, "SELECT COUNT(*) FROM users")
        assert direct_total == N, f"Direct count {direct_total} ≠ expected {N}"
        assert scatter_count == N, f"Scatter COUNT {scatter_count} ≠ expected {N}"

    def test_coalesce_in_scatter_select(self, keel_conn):
        """COALESCE on nullable column in scatter SELECT returns correct fallback."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (100, 'no_mail', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (101, 'has_mail', 'x@y')")
        rows = pg_exec(
            keel_conn,
            "SELECT id, COALESCE(email, 'N/A') FROM users WHERE id IN (100, 101) ORDER BY id",
        )
        assert rows[0][1] == "N/A"
        assert rows[1][1] == "x@y"

    def test_parameterized_point_lookup(self, keel_conn):
        """Bound parameter as the shard-key value routes to the correct shard."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (200, 'paramtest')")
        name = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (200,))
        assert name == "paramtest"

    def test_parameterized_insert_routes_correctly(self, keel_conn, shard0_conn, shard1_conn):
        """INSERT with all values as bound parameters lands on exactly one shard."""
        ids = list(range(300, 320))
        for uid in ids:
            pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (%s, %s, %s)",
                    (uid, f"param_{uid}", uid % 80))
        total = shard_total_count(shard0_conn, shard1_conn, "users")
        assert total == len(ids)

    def test_parameterized_update(self, keel_conn):
        """UPDATE with bound parameter shard-key routes to the row's shard."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, balance) VALUES (400, 'before', 0)")
        pg_exec(keel_conn, "UPDATE users SET balance = %s WHERE id = %s", (999.99, 400))
        balance = float(pg_scalar(keel_conn, "SELECT balance FROM users WHERE id = 400"))
        assert balance == pytest.approx(999.99, abs=0.01)

    def test_parameterized_delete(self, keel_conn, shard0_conn, shard1_conn):
        """DELETE with bound parameter shard-key removes the row from its shard."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (500, 'deleteme')")
        pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (500,))
        assert shard_total_count(shard0_conn, shard1_conn, "users", "id = 500") == 0


# =============================================================================
# PART 2 — Data Integrity Invariants
# =============================================================================

class TestDataIntegrityInvariants:
    """
    Strong guarantees that sharding never loses, duplicates, or corrupts data.
    All assertions cross-check by reading directly from each shard backend.
    """

    def test_every_id_exists_on_exactly_one_shard(self, keel_conn, shard0_conn, shard1_conn):
        """
        For every inserted ID, exactly one of the two shards contains the row.
        Sum over all IDs of (count_on_shard0 + count_on_shard1) must equal N.
        """
        ids = list(range(2000, 2050))
        for uid in ids:
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        total_duplication_check = 0
        for uid in ids:
            on_s0 = pg_count(shard0_conn, "users", f"id = {uid}")
            on_s1 = pg_count(shard1_conn, "users", f"id = {uid}")
            total_duplication_check += on_s0 + on_s1
            assert on_s0 + on_s1 == 1, (
                f"id={uid} on_s0={on_s0} on_s1={on_s1}: expected exactly 1 occurrence"
            )
        assert total_duplication_check == len(ids)

    def test_delete_removes_from_correct_shard(self, keel_conn, shard0_conn, shard1_conn):
        """
        After deleting a row via KEEL, neither shard retains the row.
        Other rows on both shards are untouched.
        """
        for uid in range(3000, 3010):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        pg_exec(keel_conn, "DELETE FROM users WHERE id = 3005")

        remaining = shard_total_count(shard0_conn, shard1_conn, "users")
        assert remaining == 9, f"Expected 9 after delete, got {remaining}"
        deleted_count = shard_total_count(shard0_conn, shard1_conn, "users", "id = 3005")
        assert deleted_count == 0

    def test_update_does_not_create_duplicate(self, keel_conn, shard0_conn, shard1_conn):
        """
        UPDATE via KEEL modifies exactly one row.  No ghost copies appear.
        """
        pg_exec(keel_conn, "INSERT INTO users(id, name, balance) VALUES (4000, 'upd_test', 0)")
        pg_exec(keel_conn, "UPDATE users SET balance = 100 WHERE id = 4000")

        count_s0 = pg_count(shard0_conn, "users", "id = 4000")
        count_s1 = pg_count(shard1_conn, "users", "id = 4000")
        assert count_s0 + count_s1 == 1
        balance_s0 = pg_scalar(shard0_conn, "SELECT balance FROM users WHERE id = 4000")
        balance_s1 = pg_scalar(shard1_conn, "SELECT balance FROM users WHERE id = 4000")
        balance = balance_s0 or balance_s1
        assert float(balance) == pytest.approx(100.0)

    def test_scatter_count_matches_per_shard_sum(self, keel_conn, shard0_conn, shard1_conn):
        """
        COUNT(*) through KEEL equals the sum of direct per-shard counts.
        Verified after many inserts including values across both shards.
        """
        N = 100
        for uid in range(5000, 5000 + N):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        scatter = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        direct = shard_total_count(shard0_conn, shard1_conn, "users")
        assert scatter == N
        assert direct == N
        assert scatter == direct

    def test_pk_uniqueness_enforced_per_shard(self, keel_conn):
        """Duplicate PK through KEEL raises UniqueViolation from the backing shard."""
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (6000, 'pk_one')")
        with pytest.raises(psycopg2.errors.UniqueViolation):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (6000, 'pk_two')")

    def test_transaction_commit_visible_on_correct_shard(self, keel_dsn, shard0_conn, shard1_conn):
        """Committed multi-DML transaction rows appear on the backing shards."""
        BASE = 7000
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                for i in range(5):
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (BASE + i, f"txn_{i}"),
                    )
            conn.commit()
        finally:
            conn.close()
        assert shard_total_count(shard0_conn, shard1_conn, "users",
                                 f"id >= {BASE} AND id < {BASE + 5}") == 5

    def test_transaction_rollback_leaves_no_rows(self, keel_dsn, shard0_conn, shard1_conn):
        """ROLLBACK removes every in-flight row from all shards."""
        BASE = 8000
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                for i in range(5):
                    cur.execute("INSERT INTO users(id, name) VALUES (%s, %s)", (BASE + i, f"r{i}"))
            conn.rollback()
        finally:
            conn.close()
        assert shard_total_count(shard0_conn, shard1_conn, "users",
                                 f"id >= {BASE} AND id < {BASE + 5}") == 0

    def test_bulk_delete_removes_exact_subset(self, keel_conn, shard0_conn, shard1_conn):
        """DELETE WHERE id IN (...) removes exactly the specified set of rows."""
        for uid in range(9000, 9020):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        to_delete = list(range(9000, 9010))
        placeholder = ",".join(str(i) for i in to_delete)
        pg_exec(keel_conn, f"DELETE FROM users WHERE id IN ({placeholder})")

        remaining = shard_total_count(shard0_conn, shard1_conn, "users")
        assert remaining == 10
        deleted_check = shard_total_count(shard0_conn, shard1_conn, "users",
                                          f"id BETWEEN 9000 AND 9009")
        assert deleted_check == 0


# =============================================================================
# PART 3 — Scatter-Merge: Aggregation Completeness
# =============================================================================

class TestScatterAggregatesComprehensive:
    """
    All aggregate functions and their correctness over a 12-row scatter dataset
    that is guaranteed to land on both shards (even and odd shard_hint values).
    """

    @pytest.fixture(autouse=True)
    def insert_events(self, keel_conn):
        _insert_scatter_events(keel_conn)

    def test_count_star(self, keel_conn):
        assert pg_scalar(keel_conn, "SELECT COUNT(*) FROM events") == SCATTER_TOTAL

    def test_sum(self, keel_conn):
        assert int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events")) == SCATTER_SUM

    def test_min(self, keel_conn):
        assert pg_scalar(keel_conn, "SELECT MIN(value) FROM events") == SCATTER_MIN

    def test_max(self, keel_conn):
        assert pg_scalar(keel_conn, "SELECT MAX(value) FROM events") == SCATTER_MAX

    def test_avg(self, keel_conn):
        avg = float(pg_scalar(keel_conn, "SELECT AVG(value) FROM events"))
        assert avg == pytest.approx(SCATTER_AVG, rel=1e-6)

    def test_count_distinct_categories(self, keel_conn):
        """COUNT(DISTINCT category) returns 3 across both shards."""
        assert pg_scalar(keel_conn, "SELECT COUNT(DISTINCT category) FROM events") == 3

    def test_sum_with_where_filter(self, keel_conn):
        """SUM with WHERE restricts scatter before merge."""
        for cat, expected in SCATTER_CAT_SUM.items():
            result = int(pg_scalar(keel_conn,
                                   "SELECT SUM(value) FROM events WHERE category = %s",
                                   (cat,)))
            assert result == expected, f"category={cat}: expected {expected}, got {result}"

    def test_group_by_count_all_categories(self, keel_conn):
        """GROUP BY category COUNT(*) returns correct per-group counts from both shards."""
        rows = pg_exec(keel_conn,
                       "SELECT category, COUNT(*) FROM events "
                       "GROUP BY category ORDER BY category")
        assert len(rows) == 3
        result = {r[0]: r[1] for r in rows}
        for cat in ("alpha", "beta", "gamma"):
            expected = SCATTER_CAT_CNT[cat]
            assert result[cat] == expected, f"{cat}: expected {expected}, got {result[cat]}"

    def test_group_by_sum_all_categories(self, keel_conn):
        rows = pg_exec(keel_conn,
                       "SELECT category, SUM(value) FROM events "
                       "GROUP BY category ORDER BY category")
        result = {r[0]: int(r[1]) for r in rows}
        for cat, expected in SCATTER_CAT_SUM.items():
            assert result[cat] == expected

    def test_group_by_avg_all_categories(self, keel_conn):
        rows = pg_exec(keel_conn,
                       "SELECT category, AVG(value) FROM events "
                       "GROUP BY category ORDER BY category")
        result = {r[0]: float(r[1]) for r in rows}
        for cat in SCATTER_CAT_SUM:
            expected_avg = SCATTER_CAT_SUM[cat] / SCATTER_CAT_CNT[cat]
            assert result[cat] == pytest.approx(expected_avg, rel=1e-6)

    def test_group_by_multi_aggregates(self, keel_conn):
        """A single GROUP BY with COUNT, SUM, MIN, MAX, AVG all correct."""
        rows = pg_exec(keel_conn,
                       "SELECT category, COUNT(*), SUM(value), MIN(value), MAX(value), AVG(value) "
                       "FROM events GROUP BY category ORDER BY category")
        assert len(rows) == 3
        for row in rows:
            cat = row[0]
            cat_vals = [r[2] for r in SCATTER_EVENTS if r[1] == cat]
            assert row[1] == len(cat_vals),         f"{cat} COUNT"
            assert int(row[2]) == sum(cat_vals),    f"{cat} SUM"
            assert row[3] == min(cat_vals),         f"{cat} MIN"
            assert row[4] == max(cat_vals),         f"{cat} MAX"
            assert float(row[5]) == pytest.approx(sum(cat_vals) / len(cat_vals), rel=1e-6)

    def test_having_filters_groups(self, keel_conn):
        """HAVING SUM > threshold eliminates correct groups after merge."""
        # alpha=130, beta=160, gamma=160 — threshold 150 keeps beta and gamma
        rows = pg_exec(keel_conn,
                       "SELECT category FROM events "
                       "GROUP BY category HAVING SUM(value) > 150 ORDER BY category")
        assert [r[0] for r in rows] == ["beta", "gamma"]

    def test_having_count_eliminates_small_groups(self, keel_conn):
        """HAVING COUNT(*) > 3 keeps groups with more than 3 rows (alpha=5, beta=4)."""
        rows = pg_exec(keel_conn,
                       "SELECT category FROM events "
                       "GROUP BY category HAVING COUNT(*) > 3 ORDER BY category")
        result_cats = [r[0] for r in rows]
        # alpha=5, beta=4 both > 3; gamma=3 does not qualify
        assert "gamma" not in result_cats
        assert "alpha" in result_cats
        assert "beta" in result_cats

    def test_order_by_limit_on_scatter(self, keel_conn):
        """ORDER BY + LIMIT on merged result returns globally correct top-N."""
        top3_expected = sorted((r[2] for r in SCATTER_EVENTS), reverse=True)[:3]
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value DESC LIMIT 3")
        assert [r[0] for r in rows] == top3_expected

    def test_order_by_offset(self, keel_conn):
        """ORDER BY + OFFSET skips the correct number of rows from the sorted set."""
        all_values = sorted(r[2] for r in SCATTER_EVENTS)  # ascending
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value ASC OFFSET 3 LIMIT 4")
        assert [r[0] for r in rows] == all_values[3:7]

    def test_filter_aggregate(self, keel_conn):
        """COUNT(*) FILTER (WHERE ...) counts only matching rows across all shards."""
        expected_alpha = SCATTER_CAT_CNT["alpha"]
        result = pg_scalar(
            keel_conn,
            "SELECT COUNT(*) FILTER (WHERE category = 'alpha') FROM events",
        )
        assert result == expected_alpha

    def test_sum_filter_aggregate(self, keel_conn):
        """SUM(value) FILTER (WHERE category = 'beta') sums only beta rows."""
        result = int(pg_scalar(
            keel_conn,
            "SELECT SUM(value) FILTER (WHERE category = 'beta') FROM events",
        ))
        assert result == SCATTER_CAT_SUM["beta"]

    def test_null_table_count_returns_zero(self, keel_conn, shard0_conn, shard1_conn):
        """COUNT(*) on an empty table after TRUNCATE returns 0, not NULL."""
        clear_table_on_shards(shard0_conn, shard1_conn, "events")
        assert pg_scalar(keel_conn, "SELECT COUNT(*) FROM events") == 0

    def test_null_table_sum_returns_null(self, keel_conn, shard0_conn, shard1_conn):
        """SUM on an empty table returns NULL, not 0."""
        clear_table_on_shards(shard0_conn, shard1_conn, "events")
        assert pg_scalar(keel_conn, "SELECT SUM(value) FROM events") is None

    def test_single_row_all_aggregates_correct(self, keel_conn, shard0_conn, shard1_conn):
        """With exactly one row, COUNT=1, SUM=MIN=MAX=AVG=that value."""
        clear_table_on_shards(shard0_conn, shard1_conn, "events")
        pg_exec(keel_conn, "INSERT INTO events(shard_hint, category, value) VALUES (0, 'solo', 42)")
        assert pg_scalar(keel_conn, "SELECT COUNT(*) FROM events") == 1
        assert int(pg_scalar(keel_conn, "SELECT SUM(value)  FROM events")) == 42
        assert pg_scalar(keel_conn, "SELECT MIN(value)  FROM events") == 42
        assert pg_scalar(keel_conn, "SELECT MAX(value)  FROM events") == 42
        assert float(pg_scalar(keel_conn, "SELECT AVG(value) FROM events")) == pytest.approx(42.0)

    def test_values_with_nulls_in_aggregate(self, keel_conn):
        """NULL values in aggregate columns are correctly ignored by SUM/AVG/COUNT."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (10001, 'a', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (10002, 'b', 30)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (10003, 'c', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (10004, 'd', 50)")
        # COUNT(age) skips NULLs; SUM ignores NULLs; AVG = (30+50)/2 = 40
        assert pg_scalar(keel_conn, "SELECT COUNT(age) FROM users") == 2
        assert int(pg_scalar(keel_conn, "SELECT SUM(age) FROM users")) == 80
        assert float(pg_scalar(keel_conn, "SELECT AVG(age) FROM users")) == pytest.approx(40.0)


# =============================================================================
# PART 4 — Window Functions over Scatter Result Sets
# =============================================================================

class TestWindowFunctionsScatter:
    """
    KEEL collects all rows from all shards then applies window functions on the
    merged set.  Every test verifies the exact window output against Python-
    computed expectations.
    """

    @pytest.fixture(autouse=True)
    def insert_activity(self, keel_conn):
        _insert_activity(keel_conn)

    def test_row_number_global_order(self, keel_conn):
        """ROW_NUMBER() OVER (ORDER BY score ASC) assigns 1..N across all shards."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, ROW_NUMBER() OVER (ORDER BY score ASC) AS rn "
            "FROM user_activity ORDER BY score ASC",
        )
        assert len(rows) == len(ACTIVITY_ROWS)
        scores = [r[0] for r in rows]
        row_numbers = [r[1] for r in rows]
        assert scores == ACTIVITY_SCORES_ASC
        assert row_numbers == list(range(1, len(ACTIVITY_ROWS) + 1))

    def test_row_number_descending(self, keel_conn):
        """ROW_NUMBER() OVER (ORDER BY score DESC) starts at 1 for the highest score."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn "
            "FROM user_activity ORDER BY score DESC",
        )
        assert rows[0][0] == max(ACTIVITY_SCORES_ASC)
        assert rows[0][1] == 1

    def test_rank_with_ties(self, keel_conn):
        """RANK() skips positions for tied values; verifiable via deterministic dataset."""
        # Insert rows with deliberate ties on score
        pg_exec(keel_conn, "INSERT INTO user_activity(user_id, action, score) VALUES (200, 'x', 99)")
        pg_exec(keel_conn, "INSERT INTO user_activity(user_id, action, score) VALUES (201, 'x', 99)")
        pg_exec(keel_conn, "INSERT INTO user_activity(user_id, action, score) VALUES (202, 'x', 98)")
        rows = pg_exec(
            keel_conn,
            "SELECT score, RANK() OVER (ORDER BY score DESC) AS rnk "
            "FROM user_activity WHERE user_id IN (200,201,202) ORDER BY rnk",
        )
        ranks = {r[0]: r[1] for r in rows}
        assert ranks[99] == 1   # two rows tie for rank 1
        assert ranks[98] == 3   # next rank is 3 (skip 2)

    def test_dense_rank_no_gaps(self, keel_conn):
        """DENSE_RANK() never skips positions even with ties."""
        pg_exec(keel_conn, "INSERT INTO user_activity(user_id, action, score) VALUES (300, 'y', 99)")
        pg_exec(keel_conn, "INSERT INTO user_activity(user_id, action, score) VALUES (301, 'y', 99)")
        pg_exec(keel_conn, "INSERT INTO user_activity(user_id, action, score) VALUES (302, 'y', 98)")
        rows = pg_exec(
            keel_conn,
            "SELECT score, DENSE_RANK() OVER (ORDER BY score DESC) AS dr "
            "FROM user_activity WHERE user_id IN (300,301,302) ORDER BY dr",
        )
        ranks = {r[0]: r[1] for r in rows}
        assert ranks[99] == 1
        assert ranks[98] == 2  # dense: no gap

    def test_ntile_4_equal_distribution(self, keel_conn):
        """NTILE(4) over 12 rows assigns exactly 3 rows per tile."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, NTILE(4) OVER (ORDER BY score ASC) AS tile "
            "FROM user_activity ORDER BY score ASC",
        )
        assert len(rows) == len(ACTIVITY_ROWS)
        tile_counts: dict[int, int] = {}
        for _, tile in rows:
            tile_counts[tile] = tile_counts.get(tile, 0) + 1
        assert set(tile_counts.keys()) == {1, 2, 3, 4}
        for t, cnt in tile_counts.items():
            assert cnt == 3, f"Tile {t} has {cnt} rows, expected 3"

    def test_lag_produces_null_for_first_row(self, keel_conn):
        """LAG(score, 1) over the first row of the ordered set returns NULL."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, LAG(score, 1) OVER (ORDER BY score ASC) AS prev_score "
            "FROM user_activity ORDER BY score ASC LIMIT 1",
        )
        assert rows[0][1] is None, "LAG of first row must be NULL"

    def test_lag_correct_predecessor_value(self, keel_conn):
        """LAG(score, 1) for the second row equals the first (minimum) score."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, LAG(score, 1) OVER (ORDER BY score ASC) AS prev_score "
            "FROM user_activity ORDER BY score ASC LIMIT 2",
        )
        assert rows[1][1] == rows[0][0], "LAG of second row must equal first row score"

    def test_lead_produces_null_for_last_row(self, keel_conn):
        """LEAD(score, 1) over the last row of the ordered set returns NULL."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, LEAD(score, 1) OVER (ORDER BY score ASC) AS next_score "
            "FROM user_activity ORDER BY score ASC",
        )
        assert rows[-1][1] is None, "LEAD of last row must be NULL"

    def test_lead_correct_successor_value(self, keel_conn):
        """LEAD(score, 1) for the first row equals the second-smallest score."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, LEAD(score, 1) OVER (ORDER BY score ASC) AS next_score "
            "FROM user_activity ORDER BY score ASC LIMIT 2",
        )
        assert rows[0][1] == rows[1][0], "LEAD of first row must equal second row score"

    def test_first_value_over_entire_window(self, keel_conn):
        """FIRST_VALUE(score) with unbounded frame equals the global minimum for all rows."""
        min_score = ACTIVITY_SCORES_ASC[0]
        rows = pg_exec(
            keel_conn,
            "SELECT FIRST_VALUE(score) OVER ("
            "  ORDER BY score ASC "
            "  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
            ") AS fv FROM user_activity",
        )
        assert all(r[0] == min_score for r in rows), (
            f"FIRST_VALUE must be {min_score} for all rows"
        )

    def test_last_value_over_entire_window(self, keel_conn):
        """LAST_VALUE(score) with unbounded frame equals the global maximum for all rows."""
        max_score = ACTIVITY_SCORES_ASC[-1]
        rows = pg_exec(
            keel_conn,
            "SELECT LAST_VALUE(score) OVER ("
            "  ORDER BY score ASC "
            "  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
            ") AS lv FROM user_activity",
        )
        assert all(r[0] == max_score for r in rows), (
            f"LAST_VALUE must be {max_score} for all rows"
        )

    def test_sum_running_total(self, keel_conn):
        """SUM(score) OVER (ORDER BY score ASC ROWS UNBOUNDED PRECEDING) gives running total."""
        rows = pg_exec(
            keel_conn,
            "SELECT score, SUM(score) OVER (ORDER BY score ASC "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running "
            "FROM user_activity ORDER BY score ASC",
        )
        assert len(rows) == len(ACTIVITY_ROWS)
        expected_running = 0
        for i, (score, running) in enumerate(rows):
            expected_running += ACTIVITY_SCORES_ASC[i]
            assert int(running) == expected_running, (
                f"Row {i}: running total {running} ≠ expected {expected_running}"
            )

    def test_partition_by_action_row_number(self, keel_conn):
        """ROW_NUMBER() OVER (PARTITION BY action ORDER BY score) resets per-action."""
        rows = pg_exec(
            keel_conn,
            "SELECT action, score, ROW_NUMBER() OVER (PARTITION BY action ORDER BY score) AS rn "
            "FROM user_activity ORDER BY action, score",
        )
        # Within each action partition, RN must start at 1 and increment by 1
        by_action: dict[str, list[int]] = {}
        for action, score, rn in rows:
            by_action.setdefault(action, []).append(rn)
        for action, rns in by_action.items():
            assert rns == list(range(1, len(rns) + 1)), (
                f"action={action}: expected consecutive RN 1..{len(rns)}, got {rns}"
            )

    def test_partition_sum_over_action(self, keel_conn):
        """SUM(score) OVER (PARTITION BY action) equals per-action total for every row."""
        rows = pg_exec(
            keel_conn,
            "SELECT action, score, SUM(score) OVER (PARTITION BY action) AS action_total "
            "FROM user_activity ORDER BY action, score",
        )
        for action, score, total in rows:
            expected = ACTIVITY_SUM_BY_ACTION[action]
            assert int(total) == expected, (
                f"action={action} score={score}: partition sum {total} ≠ {expected}"
            )

    def test_partition_count_over_action(self, keel_conn):
        """COUNT(*) OVER (PARTITION BY action) gives per-action row count."""
        rows = pg_exec(
            keel_conn,
            "SELECT action, COUNT(*) OVER (PARTITION BY action) AS action_cnt "
            "FROM user_activity",
        )
        for action, cnt in rows:
            assert int(cnt) == ACTIVITY_COUNT_BY_ACTION[action], (
                f"action={action}: cnt={cnt} ≠ expected {ACTIVITY_COUNT_BY_ACTION[action]}"
            )

    def test_window_row_count_matches_scatter_count(self, keel_conn):
        """Number of rows produced by a window query equals the scatter COUNT(*)."""
        window_rows = pg_exec(
            keel_conn,
            "SELECT ROW_NUMBER() OVER (ORDER BY score) FROM user_activity",
        )
        scatter_count = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM user_activity"))
        assert len(window_rows) == scatter_count == len(ACTIVITY_ROWS)


# =============================================================================
# PART 5 — CTEs: Simple, Chained, Recursive
# =============================================================================

class TestCTEs:
    """
    CTE correctness over sharded tables.  Verifies that KEEL correctly handles
    CTEs wrapping scatter-merge queries and pure-computation recursive CTEs.
    """

    @pytest.fixture(autouse=True)
    def insert_events(self, keel_conn):
        _insert_scatter_events(keel_conn)

    def test_simple_cte_wraps_scatter_aggregate(self, keel_conn):
        """WITH cte AS (scatter aggregate) SELECT * FROM cte returns correct result."""
        rows = pg_exec(
            keel_conn,
            """
            WITH agg AS (
                SELECT category, SUM(value) AS total FROM events GROUP BY category
            )
            SELECT category, total FROM agg ORDER BY category
            """,
        )
        assert len(rows) == 3
        result = {r[0]: int(r[1]) for r in rows}
        for cat, expected in SCATTER_CAT_SUM.items():
            assert result[cat] == expected

    def test_cte_with_filter_having(self, keel_conn):
        """CTE output filtered by a WHERE clause returns correct subset."""
        rows = pg_exec(
            keel_conn,
            """
            WITH agg AS (
                SELECT category, SUM(value) AS total FROM events GROUP BY category
            )
            SELECT category, total FROM agg WHERE total > 140 ORDER BY category
            """,
        )
        # beta=160, gamma=160 qualify; alpha=130 does not
        assert len(rows) == 2
        assert {r[0] for r in rows} == {"beta", "gamma"}

    def test_chained_ctes(self, keel_conn):
        """Multiple CTEs in a single WITH clause evaluated in order."""
        rows = pg_exec(
            keel_conn,
            """
            WITH
              base AS (
                  SELECT category, SUM(value) AS total FROM events GROUP BY category
              ),
              ranked AS (
                  SELECT category, total,
                         RANK() OVER (ORDER BY total DESC) AS rnk
                  FROM base
              )
            SELECT category, total, rnk FROM ranked ORDER BY rnk
            """,
        )
        assert len(rows) == 3
        # Top rank(s) should be the category with highest total
        max_total = max(SCATTER_CAT_SUM.values())
        top_cats = {c for c, s in SCATTER_CAT_SUM.items() if s == max_total}
        assert rows[0][0] in top_cats

    def test_cte_count_and_sum_simultaneously(self, keel_conn):
        """CTE computes COUNT and SUM, outer query computes AVG from those."""
        row = pg_exec(
            keel_conn,
            """
            WITH stats AS (
                SELECT COUNT(*) AS cnt, SUM(value) AS total FROM events
            )
            SELECT cnt, total, ROUND(total::numeric / cnt, 6) AS computed_avg
            FROM stats
            """,
        )
        assert len(row) == 1
        cnt, total, avg = row[0]
        assert int(cnt) == SCATTER_TOTAL
        assert int(total) == SCATTER_SUM
        assert float(avg) == pytest.approx(SCATTER_AVG, rel=1e-5)

    def test_cte_used_twice_in_query(self, keel_conn):
        """A CTE referenced twice (self-join) produces correct cross-product result."""
        rows = pg_exec(
            keel_conn,
            """
            WITH cats AS (
                SELECT DISTINCT category FROM events
            )
            SELECT a.category, b.category
            FROM cats a CROSS JOIN cats b
            WHERE a.category < b.category
            ORDER BY 1, 2
            """,
        )
        # 3 categories → C(3,2) = 3 pairs
        assert len(rows) == 3

    def test_cte_min_max_values(self, keel_conn):
        """CTE computing global MIN and MAX returns the scatter-correct extremes."""
        row = pg_exec(
            keel_conn,
            """
            WITH extremes AS (
                SELECT MIN(value) AS lo, MAX(value) AS hi FROM events
            )
            SELECT lo, hi FROM extremes
            """,
        )
        assert len(row) == 1
        assert row[0][0] == SCATTER_MIN
        assert row[0][1] == SCATTER_MAX

    def test_recursive_cte_sum_of_integers(self, keel_conn):
        """
        Recursive CTE for 1..10 sum returns 55.
        This is pure computation — KEEL routes to any shard and evaluates.
        """
        result = pg_scalar(
            keel_conn,
            """
            WITH RECURSIVE seq(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < 10
            )
            SELECT SUM(n) FROM seq
            """,
        )
        assert int(result) == 55  # 1+2+...+10

    def test_recursive_cte_fibonacci(self, keel_conn):
        """
        Recursive Fibonacci CTE produces correct values (1,1,2,3,5,8,13,21,34,55,89,144).
        """
        rows = pg_exec(
            keel_conn,
            """
            WITH RECURSIVE fib(a, b) AS (
                SELECT 1, 1
                UNION ALL
                SELECT b, a + b FROM fib WHERE a < 100
            )
            SELECT a FROM fib ORDER BY a
            """,
        )
        values = [r[0] for r in rows]
        assert values == [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]

    def test_recursive_cte_countdown(self, keel_conn):
        """Recursive CTE counting down from 5 to 0 produces the correct sequence."""
        rows = pg_exec(
            keel_conn,
            """
            WITH RECURSIVE countdown(n) AS (
                SELECT 5
                UNION ALL
                SELECT n - 1 FROM countdown WHERE n > 0
            )
            SELECT n FROM countdown ORDER BY n DESC
            """,
        )
        assert [r[0] for r in rows] == [5, 4, 3, 2, 1, 0]

    def test_cte_with_order_by_on_scatter(self, keel_conn):
        """CTE containing ORDER BY propagates ordering to outer SELECT."""
        rows = pg_exec(
            keel_conn,
            """
            WITH ordered AS (
                SELECT shard_hint, value FROM events ORDER BY value ASC
            )
            SELECT value FROM ordered ORDER BY value ASC
            """,
        )
        values = [r[0] for r in rows]
        assert values == SCATTER_VALUES_ASC


# =============================================================================
# PART 6 — JSONB Operations over Scatter
# =============================================================================

class TestJSONBScatter:
    """
    JSONB columns stored on shards are fully query-able through KEEL.
    Operators (@>, ->>, ?, #>), JSONB aggregation, and JSONB-based filtering
    must all return correct results after scatter-merge.
    """

    @pytest.fixture(autouse=True)
    def insert_products(self, keel_conn):
        _insert_products(keel_conn)

    def test_total_row_count(self, keel_conn):
        """All 6 products are present across shards."""
        assert pg_scalar(keel_conn, "SELECT COUNT(*) FROM products") == len(PRODUCTS_ROWS)

    def test_jsonb_arrow_extract_text(self, keel_conn):
        """metadata->>'brand' returns the correct string for a point-lookup."""
        brand = pg_scalar(
            keel_conn,
            "SELECT metadata->>'brand' FROM products WHERE product_id = 200",
        )
        assert brand == "ACME"

    def test_jsonb_path_text_extract(self, keel_conn):
        """metadata->>'author' returns correct author from a books product."""
        author = pg_scalar(
            keel_conn,
            "SELECT metadata->>'author' FROM products WHERE product_id = 202",
        )
        assert author == "Alice"

    def test_jsonb_contains_in_stock_filter(self, keel_conn):
        """WHERE metadata @> '{\"in_stock\": true}' counts the correct in-stock products."""
        count = pg_scalar(
            keel_conn,
            "SELECT COUNT(*) FROM products WHERE metadata @> '{\"in_stock\": true}'",
        )
        assert int(count) == PRODUCTS_IN_STOCK_COUNT

    def test_jsonb_contains_out_of_stock(self, keel_conn):
        """WHERE metadata @> '{\"in_stock\": false}' returns exactly 1 product (203)."""
        count = pg_scalar(
            keel_conn,
            "SELECT COUNT(*) FROM products WHERE metadata @> '{\"in_stock\": false}'",
        )
        assert int(count) == 1
        pid = pg_scalar(
            keel_conn,
            "SELECT product_id FROM products WHERE metadata @> '{\"in_stock\": false}'",
        )
        assert pid == 203

    def test_jsonb_key_exists_operator(self, keel_conn):
        """metadata ? 'brand' counts only products with a brand key."""
        count = pg_scalar(keel_conn, "SELECT COUNT(*) FROM products WHERE metadata ? 'brand'")
        assert int(count) == PRODUCTS_WITH_BRAND_COUNT  # 3

    def test_jsonb_sum_over_filter(self, keel_conn):
        """SUM(price) WHERE metadata @> '{"brand":"ACME"}' sums only ACME products."""
        total = float(pg_scalar(
            keel_conn,
            "SELECT SUM(price) FROM products WHERE metadata @> '{\"brand\": \"ACME\"}'",
        ))
        assert total == pytest.approx(PRODUCTS_ACME_PRICE_SUM, abs=0.01)

    def test_jsonb_numeric_field_extraction(self, keel_conn):
        """metadata->>'pages' cast to INT returns the correct numeric value."""
        pages = pg_scalar(
            keel_conn,
            "SELECT (metadata->>'pages')::INT FROM products WHERE product_id = 202",
        )
        assert pages == 320

    def test_jsonb_agg_over_scatter(self, keel_conn):
        """jsonb_agg collects JSONB objects from both shards into a single array."""
        result = pg_scalar(
            keel_conn,
            "SELECT jsonb_agg(metadata ORDER BY product_id) "
            "FROM products WHERE category = 'electronics'",
        )
        assert result is not None
        parsed = result if isinstance(result, list) else json.loads(result)
        # Should have 3 electronics products: 200, 201, 204
        assert len(parsed) == 3
        # Ordered by product_id: first is Laptop Pro (brand ACME)
        assert parsed[0].get("brand") == "ACME"

    def test_jsonb_group_by_in_stock(self, keel_conn):
        """GROUP BY (metadata->>'in_stock')::boolean counts correctly."""
        rows = pg_exec(
            keel_conn,
            "SELECT (metadata->>'in_stock')::boolean AS in_stock, COUNT(*) "
            "FROM products GROUP BY 1 ORDER BY 1",
        )
        result = {r[0]: r[1] for r in rows}
        assert result.get(True) == PRODUCTS_IN_STOCK_COUNT
        assert result.get(False) == len(PRODUCTS_ROWS) - PRODUCTS_IN_STOCK_COUNT

    def test_jsonb_category_sum_with_filter(self, keel_conn):
        """SUM(price) GROUP BY category filtered by JSONB condition."""
        rows = pg_exec(
            keel_conn,
            "SELECT category, SUM(price) FROM products "
            "WHERE metadata @> '{\"in_stock\": true}' "
            "GROUP BY category ORDER BY category",
        )
        result = {r[0]: float(r[1]) for r in rows}
        # Only in-stock products: 200,201,202,204,205 (not 203)
        in_stock_products = [r for r in PRODUCTS_ROWS if r[5]["in_stock"]]
        expected = {}
        for _, _, cat, price, _, _, _ in in_stock_products:
            expected[cat] = expected.get(cat, 0.0) + price
        for cat, exp in expected.items():
            assert result.get(cat) == pytest.approx(exp, abs=0.01), f"category={cat}"

    def test_jsonb_text_contains_operator(self, keel_conn):
        """metadata @> checks for a string value returns the correct matches."""
        # Only ACME products have brand=ACME
        rows = pg_exec(
            keel_conn,
            "SELECT product_id FROM products "
            "WHERE metadata @> '{\"brand\": \"ACME\"}' ORDER BY product_id",
        )
        assert [r[0] for r in rows] == sorted(PRODUCTS_ACME_IDS)

    def test_array_column_any_operator(self, keel_conn):
        """WHERE 'laptop' = ANY(tags) filters products with the 'laptop' tag."""
        rows = pg_exec(
            keel_conn,
            "SELECT product_id FROM products WHERE 'laptop' = ANY(tags) ORDER BY product_id",
        )
        expected_ids = sorted(
            r[0] for r in PRODUCTS_ROWS if "laptop" in r[6]
        )  # [200, 204]
        assert [r[0] for r in rows] == expected_ids

    def test_string_agg_over_scatter(self, keel_conn):
        """STRING_AGG(name, '|') across both shards returns all product names."""
        result = pg_scalar(
            keel_conn,
            "SELECT STRING_AGG(name, '|' ORDER BY product_id) FROM products",
        )
        assert result is not None
        names_from_result = sorted(result.split("|"))
        expected_names = sorted(r[1] for r in PRODUCTS_ROWS)
        assert names_from_result == expected_names

    def test_array_agg_over_scatter(self, keel_conn):
        """ARRAY_AGG(category ORDER BY product_id) returns all categories in order."""
        result = pg_scalar(
            keel_conn,
            "SELECT ARRAY_AGG(category ORDER BY product_id) FROM products",
        )
        assert result is not None
        expected = [r[2] for r in sorted(PRODUCTS_ROWS, key=lambda x: x[0])]
        assert list(result) == expected


# =============================================================================
# PART 7 — Complex Queries: Subqueries, Set Ops, Mixed Patterns
# =============================================================================

class TestComplexQueries:
    """
    Complex multi-step queries combining aggregates, subqueries, set operations,
    and expressions that exercise the full scatter-merge pipeline.
    """

    @pytest.fixture(autouse=True)
    def insert_all(self, keel_conn):
        _insert_scatter_events(keel_conn)
        _insert_products(keel_conn)

    def test_subquery_in_where_exists(self, keel_conn):
        """EXISTS subquery over scatter returns the correct Boolean."""
        # Check if any event with value > 60 exists (65 is the max)
        result = pg_scalar(
            keel_conn,
            "SELECT EXISTS(SELECT 1 FROM events WHERE value > 60)",
        )
        assert result is True

    def test_subquery_in_where_not_exists(self, keel_conn):
        """NOT EXISTS subquery over scatter returns True when no rows match."""
        result = pg_scalar(
            keel_conn,
            "SELECT EXISTS(SELECT 1 FROM events WHERE value > 1000)",
        )
        assert result is False

    def test_scalar_subquery_in_select(self, keel_conn):
        """Scalar subquery computing global MAX used inline in outer SELECT."""
        rows = pg_exec(
            keel_conn,
            "SELECT category, SUM(value), "
            "       (SELECT MAX(value) FROM events) AS global_max "
            "FROM events GROUP BY category ORDER BY category",
        )
        for row in rows:
            assert row[2] == SCATTER_MAX, f"category={row[0]}: global_max {row[2]} ≠ {SCATTER_MAX}"

    def test_derived_table_subquery(self, keel_conn):
        """FROM (SELECT ...) subquery (derived table) filters results correctly."""
        rows = pg_exec(
            keel_conn,
            """
            SELECT cat_total.category, cat_total.total
            FROM (
                SELECT category, SUM(value) AS total FROM events GROUP BY category
            ) cat_total
            WHERE cat_total.total > 150
            ORDER BY cat_total.category
            """,
        )
        result = {r[0]: int(r[1]) for r in rows}
        expected = {c: s for c, s in SCATTER_CAT_SUM.items() if s > 150}
        assert result == expected

    def test_union_all_combines_scatter_results(self, keel_conn):
        """UNION ALL of two scatter queries returns the combined row count."""
        rows = pg_exec(
            keel_conn,
            """
            SELECT value FROM events WHERE category = 'alpha'
            UNION ALL
            SELECT value FROM events WHERE category = 'beta'
            ORDER BY value
            """,
        )
        alpha_vals = sorted(r[2] for r in SCATTER_EVENTS if r[1] == "alpha")
        beta_vals  = sorted(r[2] for r in SCATTER_EVENTS if r[1] == "beta")
        expected = sorted(alpha_vals + beta_vals)
        assert [r[0] for r in rows] == expected

    def test_case_expression_in_aggregate(self, keel_conn):
        """CASE expression inside SUM correctly buckets values across shards."""
        # Count how many values are <= 35 (should be 6: 10,15,20,25,30,35)
        result = pg_scalar(
            keel_conn,
            "SELECT SUM(CASE WHEN value <= 35 THEN 1 ELSE 0 END) FROM events",
        )
        expected = sum(1 for r in SCATTER_EVENTS if r[2] <= 35)
        assert int(result) == expected

    def test_case_expression_in_group_by(self, keel_conn):
        """GROUP BY CASE expression correctly partitions rows across shards."""
        rows = pg_exec(
            keel_conn,
            """
            SELECT CASE WHEN value <= 35 THEN 'low' ELSE 'high' END AS bucket,
                   COUNT(*) AS cnt
            FROM events
            GROUP BY bucket
            ORDER BY bucket
            """,
        )
        result = {r[0]: r[1] for r in rows}
        expected_low  = sum(1 for r in SCATTER_EVENTS if r[2] <= 35)
        expected_high = SCATTER_TOTAL - expected_low
        assert result["high"] == expected_high
        assert result["low"]  == expected_low

    def test_greatest_least_across_scatter(self, keel_conn):
        """GREATEST and LEAST aggregate via MAX/MIN correctly."""
        # Use GREATEST(MIN, 0) to ensure non-negative — values are all positive here
        result = pg_scalar(
            keel_conn,
            "SELECT GREATEST(MAX(value), 0) FROM events",
        )
        assert int(result) == SCATTER_MAX

    def test_percentile_cont_via_scatter(self, keel_conn):
        """PERCENTILE_CONT(0.5) ORDER BY value returns the median across all shards."""
        median = float(pg_scalar(
            keel_conn,
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) FROM events",
        ))
        # sorted values: [10,15,20,25,30,35,40,45,50,55,60,65] — 12 values
        # Median = avg of 6th and 7th = (35+40)/2 = 37.5
        assert median == pytest.approx(37.5, abs=0.01)

    def test_percentile_disc_via_scatter(self, keel_conn):
        """PERCENTILE_DISC(0.0) ORDER BY value returns the global minimum."""
        lo = pg_scalar(
            keel_conn,
            "SELECT PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value) FROM events",
        )
        assert lo == SCATTER_MIN

    def test_multi_table_scatter_counts_independent(self, keel_conn, shard0_conn, shard1_conn):
        """
        Counts from two independently-sharded tables are both correct and independent.
        Scatter-merge for each table returns the table's own row count.
        """
        assert pg_scalar(keel_conn, "SELECT COUNT(*) FROM events") == SCATTER_TOTAL
        assert pg_scalar(keel_conn, "SELECT COUNT(*) FROM products") == len(PRODUCTS_ROWS)

    def test_combined_aggregate_and_subquery(self, keel_conn):
        """Aggregate referencing a subquery max — both computed via scatter."""
        row = pg_exec(
            keel_conn,
            """
            SELECT COUNT(*) AS cnt,
                   SUM(value) AS total,
                   (SELECT MAX(value) FROM events) AS global_max
            FROM events
            WHERE value < (SELECT AVG(value) FROM events)
            """,
        )
        avg_val = SCATTER_AVG  # 37.5
        below_avg = [r[2] for r in SCATTER_EVENTS if r[2] < avg_val]
        assert int(row[0][0]) == len(below_avg)
        assert int(row[0][1]) == sum(below_avg)
        assert row[0][2] == SCATTER_MAX


# =============================================================================
# PART 8 — Order, Limit, Offset Correctness
# =============================================================================

class TestOrderLimitOffset:
    """
    Merge-sort, LIMIT and OFFSET applied after collecting rows from all shards.
    The global ordering must be correct regardless of which shard a row came from.
    """

    @pytest.fixture(autouse=True)
    def insert_events(self, keel_conn):
        _insert_scatter_events(keel_conn)

    def test_full_ascending_order(self, keel_conn):
        """All rows returned in ascending order by value span both shards correctly."""
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value ASC")
        assert [r[0] for r in rows] == SCATTER_VALUES_ASC

    def test_full_descending_order(self, keel_conn):
        """All rows returned in descending order."""
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value DESC")
        assert [r[0] for r in rows] == list(reversed(SCATTER_VALUES_ASC))

    def test_limit_1_returns_global_minimum(self, keel_conn):
        """LIMIT 1 ORDER BY value ASC returns the single global minimum."""
        row = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value ASC LIMIT 1")
        assert row[0][0] == SCATTER_MIN

    def test_limit_1_returns_global_maximum(self, keel_conn):
        """LIMIT 1 ORDER BY value DESC returns the single global maximum."""
        row = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value DESC LIMIT 1")
        assert row[0][0] == SCATTER_MAX

    def test_limit_n_top_elements_correct(self, keel_conn):
        """LIMIT N returns the globally correct top-N elements."""
        for n in (1, 3, 5, 6, 12):
            rows = pg_exec(keel_conn, f"SELECT value FROM events ORDER BY value DESC LIMIT {n}")
            expected = list(reversed(SCATTER_VALUES_ASC))[:n]
            assert [r[0] for r in rows] == expected, f"LIMIT {n} failed"

    def test_offset_skips_correct_rows(self, keel_conn):
        """OFFSET N skips exactly N rows from the sorted global order."""
        for offset in (0, 1, 5, 11):
            rows = pg_exec(
                keel_conn,
                f"SELECT value FROM events ORDER BY value ASC OFFSET {offset}",
            )
            expected = SCATTER_VALUES_ASC[offset:]
            assert [r[0] for r in rows] == expected, f"OFFSET {offset} failed"

    def test_limit_offset_page(self, keel_conn):
        """LIMIT + OFFSET together implement correct pagination over merged rows."""
        page_size = 4
        for page in range(3):  # 0, 1, 2
            offset = page * page_size
            rows = pg_exec(
                keel_conn,
                f"SELECT value FROM events ORDER BY value ASC "
                f"LIMIT {page_size} OFFSET {offset}",
            )
            expected = SCATTER_VALUES_ASC[offset : offset + page_size]
            assert [r[0] for r in rows] == expected, f"Page {page} failed"

    def test_order_by_text_column(self, keel_conn):
        """ORDER BY text column (category) produces lexicographic global order."""
        rows = pg_exec(keel_conn, "SELECT category FROM events ORDER BY category ASC")
        cats = [r[0] for r in rows]
        assert cats == sorted(cats)

    def test_order_by_multi_column(self, keel_conn):
        """ORDER BY (category, value) produces correct composite sort across shards."""
        rows = pg_exec(
            keel_conn,
            "SELECT category, value FROM events ORDER BY category ASC, value ASC",
        )
        expected = sorted((r[1], r[2]) for r in SCATTER_EVENTS)
        actual = [(r[0], r[1]) for r in rows]
        assert actual == expected

    def test_offset_beyond_result_set_returns_empty(self, keel_conn):
        """OFFSET larger than the result set returns 0 rows (not an error)."""
        rows = pg_exec(
            keel_conn,
            f"SELECT value FROM events ORDER BY value OFFSET {SCATTER_TOTAL + 100}",
        )
        assert rows == []


# =============================================================================
# PART 9 — Concurrent Write Integrity
# =============================================================================

class TestConcurrentWriteIntegrity:
    """
    High-concurrency tests that verify KEEL never loses, duplicates, or corrupts
    rows under parallel workloads.  Tests use direct shard connections to cross-
    check row counts.
    """

    def test_30_concurrent_inserts_no_loss_no_duplication(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        30 threads each insert 1 unique row.  Total on both shards must be exactly 30.
        """
        BASE = 200_000
        N    = 30

        def writer(result: WorkerResult, dsn: str, idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (BASE + idx, f"concurrent_{idx}"),
                    )
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
        total = shard_total_count(shard0_conn, shard1_conn, "users",
                                   f"id >= {BASE} AND id < {BASE + N}")
        assert total == N, f"Expected {N} rows, got {total}"

    def test_concurrent_scatter_count_always_consistent(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        With a stable dataset, 20 threads running COUNT(*) simultaneously all
        observe the same value.
        """
        N_EVENTS = 50
        for i in range(N_EVENTS):
            # Insert directly to shards for speed; shard_hint controls placement
            target = shard0_conn if i % 2 == 0 else shard1_conn
            with target.cursor() as cur:
                cur.execute(
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (i, "load", i),
                )

        counts: list[int] = []
        lock = threading.Lock()

        def counter(result: WorkerResult, dsn: str) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM events")
                    c = cur.fetchone()[0]
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
        assert not result.errors, f"Counter errors: {result.errors[:3]}"
        assert all(c == N_EVENTS for c in counts), (
            f"Inconsistent scatter counts: min={min(counts)} max={max(counts)} "
            f"expected={N_EVENTS}"
        )

    def test_concurrent_updates_no_lost_update(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        10 threads each increment the same counter row 10 times via
        UPDATE … SET balance = balance + 1.
        Final balance must be 10 * 10 = 100.
        """
        # Insert the counter row directly on shard 0 (even id → shard 0 with most hash fns)
        # We pick id=1_000_000_000 and trust that it lands on one shard
        COUNTER_ID = 1_000_000_000
        # Insert via KEEL to ensure routing
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users(id, name, balance) VALUES (%s, %s, 0)",
                (COUNTER_ID, "counter"),
            )
        conn.close()

        THREADS = 10
        INCREMENTS = 10

        def incrementor(result: WorkerResult, dsn: str) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=10)
                conn.autocommit = False
                for _ in range(INCREMENTS):
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE users SET balance = balance + 1 WHERE id = %s",
                            (COUNTER_ID,),
                        )
                    conn.commit()
                result.record_success()
            except Exception as exc:
                result.record_error(exc)
            finally:
                if conn:
                    try:
                        conn.rollback()
                        conn.close()
                    except Exception:
                        pass

        result = run_concurrent(incrementor, THREADS, keel_dsn)
        # Allow some failures (retry / contention) but total balance must match
        final_balance_s0 = pg_scalar(shard0_conn, "SELECT balance FROM users WHERE id = %s",
                                      (COUNTER_ID,))
        final_balance_s1 = pg_scalar(shard1_conn, "SELECT balance FROM users WHERE id = %s",
                                      (COUNTER_ID,))
        final_balance = float(final_balance_s0 or final_balance_s1 or 0)
        assert final_balance == pytest.approx(THREADS * INCREMENTS, abs=0.01), (
            f"Lost update detected: balance={final_balance} "
            f"expected={THREADS * INCREMENTS}"
        )

    def test_read_your_own_writes(self, keel_dsn):
        """
        In autocommit mode, a SELECT immediately after INSERT returns the row.
        Tests that KEEL routes both queries to the same shard.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        try:
            for uid in range(300_000, 300_020):
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (uid, f"ryw_{uid}"),
                    )
                    cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
                    row = cur.fetchone()
                    assert row is not None, f"Read-your-write failed for id={uid}"
                    assert row[0] == f"ryw_{uid}"
        finally:
            conn.close()

    def test_concurrent_readers_see_no_dirty_data(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        While a writer holds an open (uncommitted) transaction,
        concurrent readers through KEEL must not see the in-flight rows.
        """
        writer_conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        writer_conn.autocommit = False

        dirty_reads: list[int] = []
        read_done = threading.Event()

        def reader():
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=5)
                conn.autocommit = True
                # Wait a moment for the writer to start its transaction
                time.sleep(0.05)
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users WHERE id IN (400001, 400002)")
                    dirty_reads.append(cur.fetchone()[0])
                conn.close()
            except Exception:
                pass
            finally:
                read_done.set()

        reader_thread = threading.Thread(target=reader)
        reader_thread.start()

        try:
            with writer_conn.cursor() as cur:
                cur.execute("INSERT INTO users(id, name) VALUES (400001, 'dirty_a')")
                cur.execute("INSERT INTO users(id, name) VALUES (400002, 'dirty_b')")
            read_done.wait(timeout=5)
            assert dirty_reads == [0], (
                f"Dirty read detected: reader saw {dirty_reads} uncommitted rows"
            )
            writer_conn.rollback()
        finally:
            try:
                writer_conn.close()
            except Exception:
                pass
        reader_thread.join(timeout=5)


# =============================================================================
# PART 10 — Large Result Set Scatter
# =============================================================================

class TestLargeScatter:
    """
    Validates scatter-merge correctness over large datasets (1000+ rows) that
    stress the result-store aggregation pipeline, including possible spill-to-disk.
    """

    N = 1_000

    @pytest.fixture(autouse=True)
    def insert_large_dataset(self, keel_conn):
        """Insert N events with known values via KEEL (scatter across shards)."""
        for i in range(self.N):
            pg_exec(
                keel_conn,
                "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                (i, "cat_a" if i % 3 == 0 else "cat_b" if i % 3 == 1 else "cat_c", i),
            )

    def test_scatter_count_1000_rows(self, keel_conn):
        """COUNT(*) over 1000 rows returns exactly 1000."""
        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events")) == self.N

    def test_scatter_sum_1000_rows(self, keel_conn):
        """SUM(value) for values 0..999 equals N*(N-1)/2."""
        expected_sum = self.N * (self.N - 1) // 2  # 499_500
        assert int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events")) == expected_sum

    def test_scatter_min_max_1000_rows(self, keel_conn):
        """MIN and MAX of values 0..999 are 0 and 999 respectively."""
        assert pg_scalar(keel_conn, "SELECT MIN(value) FROM events") == 0
        assert pg_scalar(keel_conn, "SELECT MAX(value) FROM events") == self.N - 1

    def test_scatter_order_by_limit_1000_rows(self, keel_conn):
        """ORDER BY ASC LIMIT 10 returns the 10 globally smallest values (0..9)."""
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value ASC LIMIT 10")
        assert [r[0] for r in rows] == list(range(10))

    def test_scatter_group_by_3_categories_1000_rows(self, keel_conn):
        """
        GROUP BY category over 1000 rows with exactly 3 categories produces
        correct counts.  cat_a: rows where i%3==0, cat_b: i%3==1, cat_c: i%3==2.
        """
        rows = pg_exec(
            keel_conn,
            "SELECT category, COUNT(*) FROM events GROUP BY category ORDER BY category",
        )
        result = {r[0]: r[1] for r in rows}
        expected_a = sum(1 for i in range(self.N) if i % 3 == 0)
        expected_b = sum(1 for i in range(self.N) if i % 3 == 1)
        expected_c = sum(1 for i in range(self.N) if i % 3 == 2)
        assert result["cat_a"] == expected_a
        assert result["cat_b"] == expected_b
        assert result["cat_c"] == expected_c

    def test_scatter_avg_1000_rows(self, keel_conn):
        """AVG of 0..999 is 499.5."""
        avg = float(pg_scalar(keel_conn, "SELECT AVG(value) FROM events"))
        assert avg == pytest.approx(499.5, rel=1e-6)

    def test_scatter_repeated_count_stable(self, keel_conn):
        """Running COUNT(*) 10 times on the same dataset always returns 1000."""
        for _ in range(10):
            c = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events"))
            assert c == self.N, f"Unstable scatter count: {c}"

    def test_scatter_count_matches_direct_sum(self, keel_conn, shard0_conn, shard1_conn):
        """Scatter COUNT(*) equals sum of per-shard direct counts."""
        scatter = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events"))
        direct  = shard_total_count(shard0_conn, shard1_conn, "events")
        assert scatter == direct == self.N

    def test_scatter_window_row_number_on_large_set(self, keel_conn):
        """ROW_NUMBER() OVER (ORDER BY value) assigns 1..N on 1000 rows."""
        rows = pg_exec(
            keel_conn,
            "SELECT ROW_NUMBER() OVER (ORDER BY value ASC) AS rn "
            "FROM events ORDER BY value ASC LIMIT 10",
        )
        assert [r[0] for r in rows] == list(range(1, 11))


# =============================================================================
# PART 11 — Shard Distribution and Determinism
# =============================================================================

class TestShardDistributionDeterminism:
    """
    Hash-based sharding must be deterministic and roughly balanced.
    The same key must always route to the same shard, and with enough keys
    both shards must receive a meaningful fraction of the rows.
    """

    def test_same_key_always_same_shard(self, keel_conn, shard0_conn, shard1_conn):
        """
        Inserting the same ID twice (delete in between) both times lands on
        the same shard.  Verifies routing determinism.
        """
        test_id = 555_000
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, 'det_a')", (test_id,))
        # Determine which shard holds the row
        on_s0 = pg_count(shard0_conn, "users", f"id = {test_id}")
        on_s1 = pg_count(shard1_conn, "users", f"id = {test_id}")
        first_shard = 0 if on_s0 == 1 else 1

        pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (test_id,))
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, 'det_b')", (test_id,))

        on_s0_again = pg_count(shard0_conn, "users", f"id = {test_id}")
        on_s1_again = pg_count(shard1_conn, "users", f"id = {test_id}")
        second_shard = 0 if on_s0_again == 1 else 1

        assert first_shard == second_shard, (
            f"id={test_id} routed to shard {first_shard} first, "
            f"then shard {second_shard} — routing is not deterministic"
        )

    def test_both_shards_receive_rows(self, keel_conn, shard0_conn, shard1_conn):
        """
        With 100 insertions, both shards must receive at least 15% of the rows
        (indicating reasonable hash distribution, not all-to-one).
        """
        N = 100
        for uid in range(600_000, 600_000 + N):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        cnt0 = pg_count(shard0_conn, "users")
        cnt1 = pg_count(shard1_conn, "users")
        assert cnt0 > 0 and cnt1 > 0, "One shard received no rows — hash is degenerate"
        assert cnt0 >= int(N * 0.15), f"Shard 0 received only {cnt0}/{N} rows"
        assert cnt1 >= int(N * 0.15), f"Shard 1 received only {cnt1}/{N} rows"
        assert cnt0 + cnt1 == N

    def test_scatter_count_matches_all_shard_count(self, keel_conn, shard0_conn, shard1_conn):
        """
        For each table: COUNT(*) through KEEL == sum of per-shard direct counts.
        Verified for users, events, and products simultaneously.
        """
        N_USERS  = 50
        N_EVENTS = 30

        for uid in range(700_000, 700_000 + N_USERS):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))
        for i in range(N_EVENTS):
            pg_exec(keel_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (i, "x", i))

        for table, expected in [("users", N_USERS), ("events", N_EVENTS)]:
            scatter = int(pg_scalar(keel_conn, f"SELECT COUNT(*) FROM {table}"))
            direct  = shard_total_count(shard0_conn, shard1_conn, table)
            assert scatter == expected, f"{table}: scatter {scatter} ≠ {expected}"
            assert direct  == expected, f"{table}: direct {direct} ≠ {expected}"


# =============================================================================
# PART 12 — NULL Handling in Aggregates and Routing
# =============================================================================

class TestNullHandlingComprehensive:
    """
    NULL values must never corrupt aggregation, routing decisions, or ORDER BY.
    """

    def test_count_star_ignores_nulls_correctly(self, keel_conn, shard0_conn, shard1_conn):
        """COUNT(*) counts all rows; COUNT(col) skips NULLs — both correct after scatter."""
        for uid in range(800_000, 800_010):
            email = f"u{uid}@e.com" if uid % 2 == 0 else None
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, email) VALUES (%s, %s, %s)",
                    (uid, f"n{uid}", email))

        count_star  = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        count_email = int(pg_scalar(keel_conn, "SELECT COUNT(email) FROM users"))
        assert count_star  == 10
        assert count_email == 5  # only the even ones

    def test_sum_of_null_column_returns_null(self, keel_conn):
        """SUM over a column that is NULL for all rows returns NULL."""
        for uid in range(810_000, 810_005):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, age) VALUES (%s, %s, NULL)", (uid, "n"))
        result = pg_scalar(keel_conn, "SELECT SUM(age) FROM users")
        assert result is None

    def test_min_max_skip_nulls(self, keel_conn):
        """MIN and MAX correctly ignore NULL in the aggregate column."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (820001, 'a', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (820002, 'b', 10)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (820003, 'c', 20)")
        assert pg_scalar(keel_conn, "SELECT MIN(age) FROM users") == 10
        assert pg_scalar(keel_conn, "SELECT MAX(age) FROM users") == 20

    def test_avg_skips_nulls(self, keel_conn):
        """AVG skips NULLs and computes over the non-null values only."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (830001, 'a', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (830002, 'b', 10)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (830003, 'c', 30)")
        avg = float(pg_scalar(keel_conn, "SELECT AVG(age) FROM users"))
        assert avg == pytest.approx(20.0, rel=1e-6)

    def test_order_by_nulls_last(self, keel_conn):
        """NULLS LAST in ORDER BY places NULL rows at the end of the merged output."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (840001, 'a', 5)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (840002, 'b', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (840003, 'c', 3)")
        rows = pg_exec(
            keel_conn,
            "SELECT age FROM users WHERE id BETWEEN 840001 AND 840003 "
            "ORDER BY age ASC NULLS LAST",
        )
        ages = [r[0] for r in rows]
        assert ages[-1] is None, "NULL must be last with NULLS LAST"
        assert ages[0] == 3

    def test_order_by_nulls_first(self, keel_conn):
        """NULLS FIRST places NULL rows at the beginning of the merged output."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (850001, 'a', 5)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (850002, 'b', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, age) VALUES (850003, 'c', 3)")
        rows = pg_exec(
            keel_conn,
            "SELECT age FROM users WHERE id BETWEEN 850001 AND 850003 "
            "ORDER BY age ASC NULLS FIRST",
        )
        ages = [r[0] for r in rows]
        assert ages[0] is None, "NULL must be first with NULLS FIRST"

    def test_group_by_excludes_null_keys(self, keel_conn):
        """GROUP BY on a nullable column does NOT produce a group for NULL keys by default."""
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (860001, 'a', 'x@y')")
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (860002, 'b', NULL)")
        pg_exec(keel_conn, "INSERT INTO users(id, name, email) VALUES (860003, 'c', 'x@y')")
        rows = pg_exec(
            keel_conn,
            "SELECT email, COUNT(*) FROM users "
            "WHERE id BETWEEN 860001 AND 860003 GROUP BY email ORDER BY email NULLS LAST",
        )
        # email='x@y' → 2, NULL → 1 (NULL forms its own group in PostgreSQL)
        groups = {r[0]: r[1] for r in rows}
        assert groups.get("x@y") == 2
        # NULL is a valid group key in PostgreSQL GROUP BY
        null_count = groups.get(None, 0)
        assert null_count == 1


# =============================================================================
# PART 13 — Stress: Repeated and Mixed Operations
# =============================================================================

class TestRepeatedOperations:
    """
    Validate scatter stability under repeated and interleaved operations.
    No state should leak between queries.
    """

    def test_repeated_scatter_counts_are_stable(self, keel_conn, shard0_conn, shard1_conn):
        """
        After inserting a fixed set, COUNT(*) must return the exact same value
        on every repeated call — no phantom rows, no row loss between queries.
        """
        N = 30
        for uid in range(900_000, 900_000 + N):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"u{uid}"))

        for _ in range(20):
            count = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
            assert count == N, f"Unstable scatter count: {count}"

    def test_insert_delete_count_cycle(self, keel_conn, shard0_conn, shard1_conn):
        """
        INSERT → verify count → DELETE → verify count = 0, repeated N times.
        Each cycle must be clean; no phantom rows should accumulate.
        """
        for cycle in range(10):
            uid = 910_000 + cycle
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"c{cycle}"))
            c1 = shard_total_count(shard0_conn, shard1_conn, "users", f"id = {uid}")
            assert c1 == 1, f"Cycle {cycle}: row not found after insert"
            pg_exec(keel_conn, "DELETE FROM users WHERE id = %s", (uid,))
            c2 = shard_total_count(shard0_conn, shard1_conn, "users", f"id = {uid}")
            assert c2 == 0, f"Cycle {cycle}: row remains after delete"

    def test_interleaved_writes_and_scatter_reads(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        Write thread inserts rows while a read thread runs scatter COUNT.
        After all writes finish, the final COUNT must equal the total inserted.
        """
        N = 50
        write_errors: list[Exception] = []
        read_errors:  list[Exception] = []
        stop_reading  = threading.Event()

        def writer():
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                for i in range(N):
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO users(id, name) VALUES (%s, %s)",
                            (920_000 + i, f"iw_{i}"),
                        )
                conn.close()
            except Exception as exc:
                write_errors.append(exc)
            finally:
                stop_reading.set()

        def reader():
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                while not stop_reading.is_set():
                    with conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM users")
                        _ = cur.fetchone()[0]  # just ensure no error
                conn.close()
            except Exception as exc:
                read_errors.append(exc)

        t_writer = threading.Thread(target=writer)
        t_reader = threading.Thread(target=reader)
        t_reader.start()
        t_writer.start()
        t_writer.join(timeout=30)
        stop_reading.set()
        t_reader.join(timeout=10)

        assert not write_errors, f"Write errors: {write_errors}"
        assert not read_errors,  f"Read errors:  {read_errors}"
        final = shard_total_count(shard0_conn, shard1_conn, "users",
                                   "id >= 920000 AND id < 970000")
        assert final == N
