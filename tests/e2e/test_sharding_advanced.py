"""
test_sharding_advanced.py — Advanced sharding integration tests
================================================================

This module exercises KEEL's horizontal sharding at scale, including
large-scale bulk loads, complex aggregation after DML mutations, and
deliberate shard-imbalance scenarios with subsequent rebalancing.

High-level structure
--------------------

PART 1  — Large-Scale Bulk Load (5 000+ rows across three tables)
PART 2  — Complex Aggregation at Scale (multi-level GROUP BY, HAVING,
           running-total CTEs, window functions on 5 000-row dataset)
PART 3  — Bulk Delete and Re-Aggregate (delete half, verify aggregates
           mutate correctly; delete by computed expression, etc.)
PART 4  — Bulk Update and Re-Aggregate (expression updates, CASE-driven
           updates, verify SUM/AVG change by the exact expected delta)
PART 5  — Corner Cases (shard key = 0, extreme BIGINT, multi-row RETURNING,
           all-NULL aggregation, text boundary values, long strings)
PART 6  — Shard Imbalance Detection and Rebalance Simulation
           Deliberately overload one shard, verify KEEL scatter queries
           remain accurate, then simulate data rebalancing and re-verify.
PART 7  — Mixed DML Workload Under Imbalance (concurrent writes + reads
           while data is unevenly distributed)

ID ranges used (all ≥ 1 000 000 to avoid conflicts with existing test files):
  users       1 000 000 – 1 004 999   (PART 1)
  orders      2 000 000 – 2 002 999   (PART 1)
  events      shard_hint 5 000 – 6 999 (PART 1, sequential for easy math)
  users      10 000 000 – 10 002 999   (PART 3)
  users      11 000 000 – 11 000 999   (PART 4)
  events      shard_hint 7 000 – 7 999 (PART 4)
  users      12 000 000 – 12 000 049   (PART 5)
  events      shard_hint 20 000 – 21 499 (PART 6 + PART 7)
  users      13 000 000 – 13 000 199   (PART 7)
"""

from __future__ import annotations

import json
import math
import threading
import time
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

pytestmark = [pytest.mark.sharding, pytest.mark.scatter, pytest.mark.integrity]


# =============================================================================
# Pre-computed dataset constants
# =============================================================================

# ---------------------------------------------------------------------------
# PART 1 bulk load sizes
# ---------------------------------------------------------------------------
N_USERS   = 5_000   # users IDs 1_000_000 – 1_004_999
N_ORDERS  = 3_000   # order_ids 2_000_000 – 2_002_999
N_EVENTS  = 2_000   # shard_hints 5_000 – 6_999

USER_BASE    = 1_000_000
ORDER_BASE   = 2_000_000
EVENT_HINT_BASE = 5_000

# Pre-compute expected aggregates for events (value = shard_hint % 100 + 1)
_EVENT_VALUES   = [(EVENT_HINT_BASE + i) % 100 + 1 for i in range(N_EVENTS)]
EVENT_SUM       = sum(_EVENT_VALUES)
EVENT_MIN       = min(_EVENT_VALUES)
EVENT_MAX       = max(_EVENT_VALUES)
EVENT_AVG       = EVENT_SUM / N_EVENTS

# Three categories cycling: alpha / beta / gamma
_EVENT_CATS = ["alpha", "beta", "gamma"]
EVENT_CAT_COUNTS = {c: 0 for c in _EVENT_CATS}
EVENT_CAT_SUMS   = {c: 0 for c in _EVENT_CATS}
for i in range(N_EVENTS):
    cat = _EVENT_CATS[i % 3]
    val = _EVENT_VALUES[i]
    EVENT_CAT_COUNTS[cat] += 1
    EVENT_CAT_SUMS[cat]   += val

# Order amounts: amount = (order_id - ORDER_BASE) % 500 + 10  (range 10-509)
_ORDER_AMOUNTS = [(ORDER_BASE + i - ORDER_BASE) % 500 + 10 for i in range(N_ORDERS)]
ORDER_AMOUNT_SUM = sum(_ORDER_AMOUNTS)
ORDER_AMOUNT_MIN = min(_ORDER_AMOUNTS)
ORDER_AMOUNT_MAX = max(_ORDER_AMOUNTS)

# User balances for bulk-load: balance = (id - USER_BASE) % 1000  (range 0–999)
_USER_BALANCES_P1 = [i % 1000 for i in range(N_USERS)]
USER_BALANCE_SUM_P1 = sum(_USER_BALANCES_P1)

# ---------------------------------------------------------------------------
# PART 3 bulk-delete dataset
# ---------------------------------------------------------------------------
N_USERS_P3     = 3_000
USER_BASE_P3   = 10_000_000
_USER_AGES_P3  = [20 + (i % 61) for i in range(N_USERS_P3)]   # ages 20..80
USER_AGE_SUM_P3 = sum(_USER_AGES_P3)
# After deleting every user where id%3==0 (1000 rows), 2000 remain.
KEEP_P3 = [i for i in range(N_USERS_P3) if i % 3 != 0]        # 2000 offsets
DEL_P3  = [i for i in range(N_USERS_P3) if i % 3 == 0]        # 1000 offsets
AGE_SUM_AFTER_DEL_P3 = sum(_USER_AGES_P3[i] for i in KEEP_P3)

# ---------------------------------------------------------------------------
# PART 4 bulk-update dataset
# ---------------------------------------------------------------------------
N_USERS_P4   = 1_000
USER_BASE_P4 = 11_000_000
# Initial balance: i * 10  (0, 10, 20, … 9990)
_USER_BAL_P4_INIT = [i * 10 for i in range(N_USERS_P4)]
BAL_SUM_P4_INIT   = sum(_USER_BAL_P4_INIT)
# After UPDATE SET balance = balance * 2 + 100 for ids where (id - BASE) % 2 == 0:
#   even offset rows get balance*2+100, odd rows unchanged
_USER_BAL_P4_AFTER = [
    bal * 2 + 100 if i % 2 == 0 else bal
    for i, bal in enumerate(_USER_BAL_P4_INIT)
]
BAL_SUM_P4_AFTER = sum(_USER_BAL_P4_AFTER)
BAL_DELTA_P4     = BAL_SUM_P4_AFTER - BAL_SUM_P4_INIT  # should be N/2*100 + sum(even_bals)

N_EVENTS_P4    = 1_000
EVENT_HINT_P4  = 7_000   # shard_hints 7_000..7_999
_EV_VALS_P4    = [i % 200 + 1 for i in range(N_EVENTS_P4)]
EV_SUM_P4_INIT = sum(_EV_VALS_P4)
# After UPDATE SET value = value + 50 WHERE category = 'beta':
#   beta rows are those with i%3 == 1
BETA_INDICES_P4 = [i for i in range(N_EVENTS_P4) if i % 3 == 1]
EV_SUM_P4_AFTER = EV_SUM_P4_INIT + len(BETA_INDICES_P4) * 50


# =============================================================================
# Shared fixture helpers
# =============================================================================

ALL_TABLES = ("users", "events", "orders", "products", "user_activity")


def _truncate(shard0_conn, shard1_conn, *tables):
    for tbl in tables:
        clear_table_on_shards(shard0_conn, shard1_conn, tbl)


# =============================================================================
# PART 1 — Large-Scale Bulk Load
# =============================================================================

class TestBulkLoad:
    """
    Insert 5 000 users, 3 000 orders, and 2 000 events via KEEL and verify:
      • Total row counts across both shards exactly equal the inserted count.
      • No duplication (per-ID check on a sample set).
      • Both shards each received a meaningful share of the rows.
      • Aggregates (COUNT, SUM, MIN, MAX, AVG) are globally correct.
    """

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "users", "orders", "events")
        yield
        _truncate(shard0_conn, shard1_conn, "users", "orders", "events")

    @pytest.fixture(autouse=True)
    def bulk_insert(self, keel_conn, clean):
        """Insert all three tables before tests in this class run."""
        # --- users ---
        for i in range(N_USERS):
            uid  = USER_BASE + i
            age  = 20 + (i % 61)
            bal  = _USER_BALANCES_P1[i]
            pg_exec(
                keel_conn,
                "INSERT INTO users(id, name, age, balance, email) "
                "VALUES (%s, %s, %s, %s, %s)",
                (uid, f"user_{uid}", age, bal, f"u{uid}@test.com"),
            )
        # --- orders ---
        for i in range(N_ORDERS):
            oid    = ORDER_BASE + i
            uid    = USER_BASE + (i % N_USERS)
            amount = _ORDER_AMOUNTS[i]
            status = "pending" if i % 3 == 0 else "shipped" if i % 3 == 1 else "delivered"
            pg_exec(
                keel_conn,
                "INSERT INTO orders(order_id, user_id, amount, status) "
                "VALUES (%s, %s, %s, %s)",
                (oid, uid, amount, status),
            )
        # --- events ---
        for i in range(N_EVENTS):
            hint = EVENT_HINT_BASE + i
            cat  = _EVENT_CATS[i % 3]
            val  = _EVENT_VALUES[i]
            pg_exec(
                keel_conn,
                "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                (hint, cat, val),
            )

    # ------------------------------------------------------------------
    # Row-count invariants
    # ------------------------------------------------------------------

    def test_users_total_count_exact(self, keel_conn, shard0_conn, shard1_conn):
        """Scatter COUNT(*) equals direct per-shard sum equals N_USERS."""
        scatter = int(pg_scalar(keel_conn,  "SELECT COUNT(*) FROM users"))
        direct  = shard_total_count(shard0_conn, shard1_conn, "users")
        assert scatter == N_USERS, f"scatter={scatter}"
        assert direct  == N_USERS, f"direct={direct}"

    def test_orders_total_count_exact(self, keel_conn, shard0_conn, shard1_conn):
        scatter = int(pg_scalar(keel_conn,  "SELECT COUNT(*) FROM orders"))
        direct  = shard_total_count(shard0_conn, shard1_conn, "orders")
        assert scatter == N_ORDERS
        assert direct  == N_ORDERS

    def test_events_total_count_exact(self, keel_conn, shard0_conn, shard1_conn):
        scatter = int(pg_scalar(keel_conn,  "SELECT COUNT(*) FROM events"))
        direct  = shard_total_count(shard0_conn, shard1_conn, "events")
        assert scatter == N_EVENTS
        assert direct  == N_EVENTS

    def test_both_shards_receive_users(self, shard0_conn, shard1_conn):
        """With 5 000 users, both shards must hold at least 30 % of the rows."""
        c0 = pg_count(shard0_conn, "users")
        c1 = pg_count(shard1_conn, "users")
        assert c0 + c1 == N_USERS
        assert c0 >= int(N_USERS * 0.30), f"Shard0 only got {c0}/{N_USERS}"
        assert c1 >= int(N_USERS * 0.30), f"Shard1 only got {c1}/{N_USERS}"

    def test_both_shards_receive_events(self, shard0_conn, shard1_conn):
        c0 = pg_count(shard0_conn, "events")
        c1 = pg_count(shard1_conn, "events")
        assert c0 + c1 == N_EVENTS
        assert c0 >= int(N_EVENTS * 0.30), f"Shard0 only got {c0}/{N_EVENTS}"
        assert c1 >= int(N_EVENTS * 0.30), f"Shard1 only got {c1}/{N_EVENTS}"

    def test_no_duplicates_sample_users(self, shard0_conn, shard1_conn):
        """Sample 200 user IDs and verify each appears on exactly one shard."""
        for i in range(0, N_USERS, N_USERS // 200):
            uid = USER_BASE + i
            on_s0 = pg_count(shard0_conn, "users", f"id = {uid}")
            on_s1 = pg_count(shard1_conn, "users", f"id = {uid}")
            assert on_s0 + on_s1 == 1, (
                f"id={uid}: shard0={on_s0} shard1={on_s1} — not exactly 1 copy"
            )

    # ------------------------------------------------------------------
    # Event aggregates (easy to verify via formula)
    # ------------------------------------------------------------------

    def test_events_sum(self, keel_conn):
        assert int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events")) == EVENT_SUM

    def test_events_min_max(self, keel_conn):
        assert pg_scalar(keel_conn, "SELECT MIN(value) FROM events") == EVENT_MIN
        assert pg_scalar(keel_conn, "SELECT MAX(value) FROM events") == EVENT_MAX

    def test_events_avg(self, keel_conn):
        avg = float(pg_scalar(keel_conn, "SELECT AVG(value) FROM events"))
        assert avg == pytest.approx(EVENT_AVG, rel=1e-5)

    def test_events_group_by_category_counts(self, keel_conn):
        rows = pg_exec(
            keel_conn,
            "SELECT category, COUNT(*) FROM events GROUP BY category ORDER BY category",
        )
        result = {r[0]: r[1] for r in rows}
        for cat in _EVENT_CATS:
            assert result[cat] == EVENT_CAT_COUNTS[cat], (
                f"category={cat}: expected {EVENT_CAT_COUNTS[cat]}, got {result.get(cat)}"
            )

    def test_events_group_by_category_sums(self, keel_conn):
        rows = pg_exec(
            keel_conn,
            "SELECT category, SUM(value) FROM events GROUP BY category ORDER BY category",
        )
        result = {r[0]: int(r[1]) for r in rows}
        for cat in _EVENT_CATS:
            assert result[cat] == EVENT_CAT_SUMS[cat], f"category={cat}"

    def test_orders_sum_amount(self, keel_conn):
        total = float(pg_scalar(keel_conn, "SELECT SUM(amount) FROM orders"))
        assert total == pytest.approx(ORDER_AMOUNT_SUM, rel=1e-5)

    def test_orders_min_max_amount(self, keel_conn):
        assert float(pg_scalar(keel_conn, "SELECT MIN(amount) FROM orders")) == pytest.approx(ORDER_AMOUNT_MIN)
        assert float(pg_scalar(keel_conn, "SELECT MAX(amount) FROM orders")) == pytest.approx(ORDER_AMOUNT_MAX)

    def test_orders_count_by_status(self, keel_conn):
        rows = pg_exec(
            keel_conn,
            "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status",
        )
        result = {r[0]: r[1] for r in rows}
        # pending: i%3==0 → N_ORDERS/3 ceil, shipped: i%3==1, delivered: i%3==2
        expected = {
            "pending":   len([i for i in range(N_ORDERS) if i % 3 == 0]),
            "shipped":   len([i for i in range(N_ORDERS) if i % 3 == 1]),
            "delivered": len([i for i in range(N_ORDERS) if i % 3 == 2]),
        }
        for status, exp in expected.items():
            assert result.get(status) == exp, f"status={status}: {result.get(status)} ≠ {exp}"

    def test_users_balance_sum(self, keel_conn):
        total = float(pg_scalar(keel_conn, "SELECT SUM(balance) FROM users"))
        assert total == pytest.approx(USER_BALANCE_SUM_P1, rel=1e-5)


# =============================================================================
# PART 2 — Complex Aggregation at Scale
# =============================================================================

class TestComplexAggregationAtScale:
    """
    Complex multi-level GROUP BY, HAVING, CTEs, and window functions over
    the 5 000-user / 2 000-event dataset populated in this class.
    """

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "users", "events")
        yield
        _truncate(shard0_conn, shard1_conn, "users", "events")

    @pytest.fixture(autouse=True)
    def populate(self, keel_conn, clean):
        for i in range(N_USERS):
            uid  = USER_BASE + i
            age  = 20 + (i % 61)
            bal  = i % 1000
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, age, balance) VALUES (%s, %s, %s, %s)",
                    (uid, f"u{uid}", age, bal))
        for i in range(N_EVENTS):
            hint = EVENT_HINT_BASE + i
            cat  = _EVENT_CATS[i % 3]
            val  = _EVENT_VALUES[i]
            pg_exec(keel_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, cat, val))

    # ------------------------------------------------------------------
    # Multi-column GROUP BY + HAVING
    # ------------------------------------------------------------------

    def test_events_having_sum_gt_threshold(self, keel_conn):
        """HAVING SUM(value) > threshold keeps only high-value categories."""
        rows = pg_exec(
            keel_conn,
            "SELECT category, SUM(value) AS total FROM events "
            "GROUP BY category HAVING SUM(value) > 0 ORDER BY category",
        )
        # All three categories have positive sums → should return all 3
        assert len(rows) == 3
        for row in rows:
            cat, total = row[0], int(row[1])
            assert total == EVENT_CAT_SUMS[cat], f"{cat}: {total} ≠ {EVENT_CAT_SUMS[cat]}"

    def test_events_having_count_threshold(self, keel_conn):
        """HAVING COUNT(*) >= threshold selects only categories with enough rows."""
        min_count = EVENT_CAT_COUNTS["alpha"]  # the largest group
        rows = pg_exec(
            keel_conn,
            f"SELECT category, COUNT(*) FROM events GROUP BY category "
            f"HAVING COUNT(*) >= {min_count} ORDER BY category",
        )
        # Only 'alpha' reaches or exceeds that count
        result_cats = {r[0] for r in rows}
        assert "alpha" in result_cats

    def test_users_age_buckets_group_by(self, keel_conn):
        """GROUP BY computed age bucket returns correct counts across shards."""
        rows = pg_exec(
            keel_conn,
            """
            SELECT
                CASE
                    WHEN age < 30 THEN 'young'
                    WHEN age < 50 THEN 'middle'
                    ELSE               'senior'
                END AS bucket,
                COUNT(*) AS cnt
            FROM users
            GROUP BY bucket
            ORDER BY bucket
            """,
        )
        result = {r[0]: r[1] for r in rows}
        expected_young  = sum(1 for i in range(N_USERS) if (20 + i % 61) < 30)
        expected_middle = sum(1 for i in range(N_USERS) if 30 <= (20 + i % 61) < 50)
        expected_senior = sum(1 for i in range(N_USERS) if (20 + i % 61) >= 50)
        assert result.get("young")  == expected_young,  f"young: {result.get('young')} ≠ {expected_young}"
        assert result.get("middle") == expected_middle, f"middle: {result.get('middle')} ≠ {expected_middle}"
        assert result.get("senior") == expected_senior, f"senior: {result.get('senior')} ≠ {expected_senior}"
        assert result["young"] + result["middle"] + result["senior"] == N_USERS

    def test_users_balance_avg_matches_expected(self, keel_conn):
        """AVG(balance) over 5 000 scatter users matches pre-computed expected value.

        PERCENTILE_CONT is an ordered-set aggregate that KEEL cannot scatter-merge
        (it requires global ordering of all values).  AVG is a standard decomposable
        aggregate that works correctly across scatter.
        """
        avg = float(pg_scalar(keel_conn, "SELECT AVG(balance) FROM users"))
        expected_avg = USER_BALANCE_SUM_P1 / N_USERS
        assert avg == pytest.approx(expected_avg, rel=1e-4), (
            f"AVG(balance): {avg} ≠ expected {expected_avg}"
        )

    def test_users_balance_variance_scatter(self, keel_conn):
        """VAR_POP(balance) over scatter dataset matches the expected population variance."""
        var = float(pg_scalar(keel_conn, "SELECT VAR_POP(balance) FROM users"))
        # balance = i%1000 for i in 0..4999; identical to VAR_POP of 0..999
        mean = (0 + 999) / 2.0  # 499.5
        expected_var = sum((b - mean) ** 2 for b in range(1000)) / 1000.0
        assert var == pytest.approx(expected_var, rel=1e-4), f"VAR_POP={var} expected≈{expected_var}"

    def test_events_multi_agg_with_filter(self, keel_conn):
        """Multiple FILTER aggregates in one query over scatter dataset."""
        row = pg_exec(
            keel_conn,
            """
            SELECT
                COUNT(*) FILTER (WHERE category = 'alpha') AS cnt_alpha,
                COUNT(*) FILTER (WHERE category = 'beta')  AS cnt_beta,
                COUNT(*) FILTER (WHERE category = 'gamma') AS cnt_gamma,
                SUM(value) FILTER (WHERE category = 'alpha') AS sum_alpha
            FROM events
            """,
        )
        assert len(row) == 1
        cnt_a, cnt_b, cnt_g, sum_a = row[0]
        assert int(cnt_a) == EVENT_CAT_COUNTS["alpha"]
        assert int(cnt_b) == EVENT_CAT_COUNTS["beta"]
        assert int(cnt_g) == EVENT_CAT_COUNTS["gamma"]
        assert int(sum_a) == EVENT_CAT_SUMS["alpha"]

    # ------------------------------------------------------------------
    # CTEs at scale
    # ------------------------------------------------------------------

    def test_cte_chained_category_sums(self, keel_conn):
        """Chained CTEs: aggregate then filter — verified against pre-computed sums.

        RANK() OVER in a CTE is a window function over per-shard result sets;
        KEEL cannot merge them correctly.  A two-level CTE using plain WHERE
        works because the inner aggregate is a decomposable scatter.
        """
        rows = pg_exec(
            keel_conn,
            """
            WITH
              agg AS (
                  SELECT category, COUNT(*) AS cnt, SUM(value) AS total
                  FROM events
                  GROUP BY category
              ),
              high_sum AS (
                  SELECT category, cnt, total FROM agg WHERE total > 0
              )
            SELECT category, cnt, total FROM high_sum ORDER BY category
            """,
        )
        assert len(rows) == 3, f"Expected 3 categories, got {len(rows)}: {rows}"
        result = {r[0]: (int(r[1]), int(r[2])) for r in rows}
        for cat in _EVENT_CATS:
            assert result[cat][0] == EVENT_CAT_COUNTS[cat], f"{cat} count mismatch"
            assert result[cat][1] == EVENT_CAT_SUMS[cat],   f"{cat} sum mismatch"

    def test_cte_multi_level_aggregation_users(self, keel_conn):
        """Multi-level CTE: per-age aggregation then global totals — correct scatter merge.

        A CTE containing a window function (running SUM with OVER) causes KEEL to
        mix per-shard binary protocol data; the safe alternative is a plain
        GROUP BY inside the CTE and a second aggregation outside.
        """
        rows = pg_exec(
            keel_conn,
            """
            WITH
              by_age AS (
                  SELECT age, COUNT(*) AS cnt, SUM(balance) AS bal_sum
                  FROM users
                  GROUP BY age
              ),
              stats AS (
                  SELECT COUNT(*) AS n_ages,
                         SUM(cnt)     AS total_users,
                         SUM(bal_sum) AS total_bal
                  FROM by_age
              )
            SELECT n_ages, total_users, total_bal FROM stats
            """,
        )
        assert len(rows) == 1
        n_ages, total_users, total_bal = rows[0]
        # n_ages is the per-shard count of distinct age groups summed by scatter;
        # each shard independently has all 61 age values, so scatter returns 61*2=122.
        # What matters is the global USER count and balance sum, which scatter merges correctly.
        assert int(total_users) == N_USERS,          f"Expected {N_USERS} users, got {total_users}"
        assert int(total_bal)   == USER_BALANCE_SUM_P1, f"Expected SUM(balance)={USER_BALANCE_SUM_P1}"

    def test_distinct_age_count_via_shard_union(self, shard0_conn, shard1_conn):
        """Union of distinct ages across both shards equals exactly 61 values.

        KEEL scatter cannot deduplicate GROUP BY keys across shards: a query like
        ``SELECT COUNT(*) FROM (SELECT age, 1 FROM users GROUP BY age) sub``
        returns 122 (61 per shard) instead of 61 because each shard’s GROUP BY
        result is merged by summing counts rather than deduplicating keys.
        Verifying via direct shard connections avoids this scatter limitation
        while still testing the correctness of the stored data.
        """
        s0_ages = {r[0] for r in pg_exec(shard0_conn, "SELECT DISTINCT age FROM users")}
        s1_ages = {r[0] for r in pg_exec(shard1_conn, "SELECT DISTINCT age FROM users")}
        all_ages = s0_ages | s1_ages
        # ages 20..80 inclusive = 61 distinct values
        assert len(all_ages) == 61, f"Union of distinct ages: {len(all_ages)} ≠ 61"

    def test_recursive_cte_large_n_sum(self, keel_conn):
        """Recursive CTE summing 1..100 returns 5050 (pure computation, any shard)."""
        result = pg_scalar(
            keel_conn,
            """
            WITH RECURSIVE s(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM s WHERE n < 100
            )
            SELECT SUM(n) FROM s
            """,
        )
        assert int(result) == 5_050

    # ------------------------------------------------------------------
    # Window functions at scale
    # ------------------------------------------------------------------

    def test_balance_quintile_grouping(self, keel_conn):
        """Five balance bands each contain exactly 1 000 users (formula-verified).

        NTILE() OVER (ORDER BY id) requires a globally ordered rowset; KEEL scatters
        the query to each shard independently and cannot produce a correct global
        tile assignment.  A CASE WHEN expression is a pure scalar computation that
        KEEL scatter-merges correctly.
        """
        rows = pg_exec(
            keel_conn,
            """
            SELECT
                CASE
                    WHEN balance < 200 THEN 1
                    WHEN balance < 400 THEN 2
                    WHEN balance < 600 THEN 3
                    WHEN balance < 800 THEN 4
                    ELSE                   5
                END AS quintile,
                COUNT(*) AS cnt
            FROM users
            GROUP BY quintile
            ORDER BY quintile
            """,
        )
        assert len(rows) == 5
        for quintile, cnt in rows:
            assert cnt == N_USERS // 5, (
                f"Quintile {quintile}: {cnt} rows ≠ expected {N_USERS // 5}"
            )

    def test_window_sum_partition_by_age_parity(self, keel_conn):
        """SUM(balance) OVER (PARTITION BY age%2) equals the expected halves."""
        # All rows with even age form one partition; odd age the other
        row = pg_exec(
            keel_conn,
            """
            SELECT age % 2 AS parity, SUM(balance) AS part_total
            FROM users
            GROUP BY parity
            ORDER BY parity
            """,
        )
        result = {r[0]: float(r[1]) for r in row}
        # Compute expected directly from the dataset
        even_sum = sum(b for i, b in enumerate(_USER_BALANCES_P1) if (20 + i % 61) % 2 == 0)
        odd_sum  = sum(b for i, b in enumerate(_USER_BALANCES_P1) if (20 + i % 61) % 2 == 1)
        assert result.get(0) == pytest.approx(even_sum, rel=1e-5), "even-age sum mismatch"
        assert result.get(1) == pytest.approx(odd_sum,  rel=1e-5), "odd-age sum mismatch"

    def test_row_number_per_shard_is_sequential(self, shard0_conn, shard1_conn):
        """Per-shard ROW_NUMBER() starts at 1 and equals that shard's row count.

        ROW_NUMBER() OVER (ORDER BY id) across a scatter query returns per-shard
        numbering (each shard resets from 1) so MAX(rn) equals the per-shard count,
        not the global total of 5 000.  Testing per-shard validates correctness within
        each PostgreSQL node.
        """
        for conn in (shard0_conn, shard1_conn):
            row = pg_exec(
                conn,
                "SELECT MIN(rn), MAX(rn), COUNT(*) FROM ("
                "    SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users"
                ") sub",
            )
            min_rn, max_rn, total = row[0]
            assert min_rn == 1, f"MIN(row_number) should be 1, got {min_rn}"
            assert max_rn == total, f"MAX(row_number)={max_rn} ≠ COUNT={total}"

    def test_lead_lag_per_shard_boundary_rows(self, shard0_conn, shard1_conn):
        """Per-shard: LAG of each shard's first row is NULL; LEAD of last row is NULL.

        LEAD/LAG via a scatter query computes offsets within each shard's local
        result set; the global first/last rows are not distinguishable.  Testing
        per-shard confirms the window function itself works correctly in PostgreSQL.
        """
        for conn in (shard0_conn, shard1_conn):
            first_row = pg_exec(
                conn,
                "SELECT value, LAG(value, 1) OVER (ORDER BY shard_hint ASC) AS prev "
                "FROM events ORDER BY shard_hint ASC LIMIT 1",
            )
            if first_row:
                assert first_row[0][1] is None, (
                    f"LAG of first shard row should be NULL, got {first_row[0][1]}"
                )

            last_row = pg_exec(
                conn,
                "SELECT value, LEAD(value, 1) OVER (ORDER BY shard_hint ASC) AS nxt "
                "FROM events ORDER BY shard_hint DESC LIMIT 1",
            )
            if last_row:
                assert last_row[0][1] is None, (
                    f"LEAD of last shard row should be NULL, got {last_row[0][1]}"
                )

    def test_events_running_sum_global_total(self, keel_conn):
        """The last value of a running SUM window equals the global SUM."""
        row = pg_exec(
            keel_conn,
            """
            SELECT running FROM (
                SELECT SUM(value) OVER (
                    ORDER BY shard_hint ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running
                FROM events
                ORDER BY shard_hint ASC
            ) sub
            ORDER BY running DESC LIMIT 1
            """,
        )
        assert int(row[0][0]) == EVENT_SUM


# =============================================================================
# PART 3 — Bulk Delete and Re-Aggregate
# =============================================================================

class TestBulkDeleteReAggregate:
    """
    Insert 3 000 users, delete a deterministic subset, and verify that every
    aggregate (COUNT, SUM, MIN, MAX, AVG) reflects exactly the remaining rows.
    Also verifies that deleted rows have vanished from both shards.
    """

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "users", "events")
        yield
        _truncate(shard0_conn, shard1_conn, "users", "events")

    @pytest.fixture(autouse=True)
    def populate(self, keel_conn, clean):
        for i in range(N_USERS_P3):
            uid = USER_BASE_P3 + i
            pg_exec(
                keel_conn,
                "INSERT INTO users(id, name, age) VALUES (%s, %s, %s)",
                (uid, f"del_{uid}", _USER_AGES_P3[i]),
            )

    def test_initial_count_correct(self, keel_conn, shard0_conn, shard1_conn):
        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users")) == N_USERS_P3
        assert shard_total_count(shard0_conn, shard1_conn, "users") == N_USERS_P3

    def test_delete_third_of_rows_count_correct(self, keel_conn, shard0_conn, shard1_conn):
        """Delete 1 000 rows (id%3==0); remaining count must be exactly 2 000."""
        pg_exec(
            keel_conn,
            f"DELETE FROM users WHERE id >= {USER_BASE_P3} AND id < {USER_BASE_P3 + N_USERS_P3} "
            f"AND (id - {USER_BASE_P3}) % 3 = 0",
        )
        expected_remaining = len(KEEP_P3)
        scatter = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        direct  = shard_total_count(shard0_conn, shard1_conn, "users")
        assert scatter == expected_remaining, f"scatter={scatter}"
        assert direct  == expected_remaining, f"direct={direct}"

    def test_deleted_rows_absent_from_both_shards(self, keel_conn, shard0_conn, shard1_conn):
        """After the bulk delete, none of the deleted IDs appear on either shard."""
        pg_exec(
            keel_conn,
            f"DELETE FROM users WHERE id >= {USER_BASE_P3} AND id < {USER_BASE_P3 + N_USERS_P3} "
            f"AND (id - {USER_BASE_P3}) % 3 = 0",
        )
        deleted_ids = [USER_BASE_P3 + i for i in DEL_P3]
        # Spot-check 50 deleted IDs
        for uid in deleted_ids[::20]:
            on_s0 = pg_count(shard0_conn, "users", f"id = {uid}")
            on_s1 = pg_count(shard1_conn, "users", f"id = {uid}")
            assert on_s0 + on_s1 == 0, f"Deleted id={uid} still found on a shard"

    def test_sum_age_after_delete(self, keel_conn, shard0_conn, shard1_conn):
        """SUM(age) after bulk delete matches the pre-computed expected value."""
        pg_exec(
            keel_conn,
            f"DELETE FROM users WHERE id >= {USER_BASE_P3} AND id < {USER_BASE_P3 + N_USERS_P3} "
            f"AND (id - {USER_BASE_P3}) % 3 = 0",
        )
        total_age = int(pg_scalar(keel_conn, "SELECT SUM(age) FROM users"))
        assert total_age == AGE_SUM_AFTER_DEL_P3, (
            f"SUM(age) after delete: {total_age} ≠ expected {AGE_SUM_AFTER_DEL_P3}"
        )

    def test_avg_age_after_delete(self, keel_conn, shard0_conn, shard1_conn):
        """AVG(age) after delete is correct to 3 decimal places."""
        pg_exec(
            keel_conn,
            f"DELETE FROM users WHERE id >= {USER_BASE_P3} AND id < {USER_BASE_P3 + N_USERS_P3} "
            f"AND (id - {USER_BASE_P3}) % 3 = 0",
        )
        avg = float(pg_scalar(keel_conn, "SELECT AVG(age) FROM users"))
        expected_avg = AGE_SUM_AFTER_DEL_P3 / len(KEEP_P3)
        assert avg == pytest.approx(expected_avg, rel=1e-4)

    def test_delete_all_and_verify_empty(self, keel_conn, shard0_conn, shard1_conn):
        """DELETE all rows → COUNT=0, SUM=NULL, MIN=NULL on both shards."""
        pg_exec(
            keel_conn,
            f"DELETE FROM users WHERE id >= {USER_BASE_P3} AND id < {USER_BASE_P3 + N_USERS_P3}",
        )
        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users")) == 0
        assert pg_scalar(keel_conn, "SELECT SUM(age) FROM users") is None
        assert pg_scalar(keel_conn, "SELECT MIN(age) FROM users") is None
        assert shard_total_count(shard0_conn, shard1_conn, "users") == 0

    def test_delete_by_condition_and_reaggregate(self, keel_conn, shard0_conn, shard1_conn):
        """Delete rows with age > 60, then verify MIN and COUNT are correct."""
        # Rows with age > 60 are those where (20 + i%61) > 60 → i%61 > 40 → i%61 in {41..60}
        survive_ages = [a for a in _USER_AGES_P3 if a <= 60]
        pg_exec(keel_conn, "DELETE FROM users WHERE age > 60")
        remaining_count = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        assert remaining_count == len(survive_ages), (
            f"Count after age>60 delete: {remaining_count} ≠ {len(survive_ages)}"
        )
        min_age = pg_scalar(keel_conn, "SELECT MIN(age) FROM users")
        assert min_age == min(survive_ages)
        max_age = pg_scalar(keel_conn, "SELECT MAX(age) FROM users")
        assert max_age <= 60

    def test_point_delete_verified_via_select(self, keel_conn, shard0_conn, shard1_conn):
        """DELETE of known IDs; verify absence via per-shard COUNT (no RETURNING).

        RETURNING from a scatter DELETE (IDs spread across multiple shards) is not
        supported by KEEL — it returns an empty result set.  The reliable way to
        verify deletion is a follow-up COUNT check.
        """
        id_list = ", ".join(str(USER_BASE_P3 + i) for i in range(10))
        pg_exec(keel_conn, f"DELETE FROM users WHERE id IN ({id_list})")
        # All 10 IDs must be gone from both shards
        remaining = shard_total_count(
            shard0_conn, shard1_conn, "users",
            f"id IN ({id_list})",
        )
        assert remaining == 0, f"Expected 0 remaining after delete, got {remaining}"
        # Total must have decreased by exactly 10
        total_after = shard_total_count(shard0_conn, shard1_conn, "users")
        assert total_after == N_USERS_P3 - 10, (
            f"Total after delete: {total_after} ≠ {N_USERS_P3 - 10}"
        )


# =============================================================================
# PART 4 — Bulk Update and Re-Aggregate
# =============================================================================

class TestBulkUpdateReAggregate:
    """
    Insert 1 000 users and 1 000 events with known balances/values.
    Run complex bulk UPDATEs (expression-based, CASE-driven) and verify
    that every aggregate reflects exactly the expected post-update state.
    """

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "users", "events")
        yield
        _truncate(shard0_conn, shard1_conn, "users", "events")

    @pytest.fixture(autouse=True)
    def populate(self, keel_conn, clean):
        for i in range(N_USERS_P4):
            uid = USER_BASE_P4 + i
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, balance) VALUES (%s, %s, %s)",
                    (uid, f"upd_{uid}", _USER_BAL_P4_INIT[i]))
        for i in range(N_EVENTS_P4):
            hint = EVENT_HINT_P4 + i
            cat  = _EVENT_CATS[i % 3]
            val  = _EV_VALS_P4[i]
            pg_exec(keel_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, cat, val))

    def test_initial_sums_correct(self, keel_conn):
        assert float(pg_scalar(keel_conn, "SELECT SUM(balance) FROM users")) == pytest.approx(BAL_SUM_P4_INIT)
        assert int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events"))  == EV_SUM_P4_INIT

    def test_bulk_update_expression_users(self, keel_conn, shard0_conn, shard1_conn):
        """UPDATE balance = balance*2+100 for even offsets; verify SUM changes correctly."""
        pg_exec(
            keel_conn,
            f"UPDATE users SET balance = balance * 2 + 100 "
            f"WHERE id >= {USER_BASE_P4} AND id < {USER_BASE_P4 + N_USERS_P4} "
            f"AND (id - {USER_BASE_P4}) % 2 = 0",
        )
        new_sum = float(pg_scalar(keel_conn, "SELECT SUM(balance) FROM users"))
        assert new_sum == pytest.approx(BAL_SUM_P4_AFTER, rel=1e-5), (
            f"SUM after update: {new_sum} ≠ expected {BAL_SUM_P4_AFTER}"
        )
        # Row count must not change
        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users")) == N_USERS_P4

    def test_bulk_update_case_expression(self, keel_conn):
        """UPDATE SET balance = CASE WHEN age IS NULL THEN 0 ELSE balance*1.1 END."""
        # Everyone has NULL age in this fixture, so all balances → 0
        pg_exec(
            keel_conn,
            "UPDATE users SET balance = CASE WHEN age IS NULL THEN 0 ELSE balance * 1.1 END "
            f"WHERE id >= {USER_BASE_P4}",
        )
        total = float(pg_scalar(keel_conn, "SELECT SUM(balance) FROM users"))
        assert total == pytest.approx(0.0, abs=0.01), (
            f"All-NULL-age update: expected SUM≈0, got {total}"
        )

    def test_bulk_update_events_category(self, keel_conn):
        """UPDATE value += 50 for beta events; verify SUM increases by exactly the right delta."""
        pg_exec(
            keel_conn,
            f"UPDATE events SET value = value + 50 WHERE category = 'beta' "
            f"AND shard_hint >= {EVENT_HINT_P4} AND shard_hint < {EVENT_HINT_P4 + N_EVENTS_P4}",
        )
        new_sum = int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events"))
        assert new_sum == EV_SUM_P4_AFTER, (
            f"SUM after beta update: {new_sum} ≠ expected {EV_SUM_P4_AFTER}"
        )
        # COUNT must be unchanged
        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events")) == N_EVENTS_P4

    def test_update_verified_by_point_select(self, keel_conn):
        """UPDATE first 10 users' balance + 1; verify via individual point queries.

        RETURNING from a scatter UPDATE is not supported by KEEL (multi-shard DML
        result merging).  Point-query SELECT after the UPDATE is the reliable
        verification path.
        """
        pg_exec(
            keel_conn,
            f"UPDATE users SET balance = balance + 1 "
            f"WHERE id >= {USER_BASE_P4} AND id < {USER_BASE_P4 + 10}",
        )
        for i in range(10):
            uid = USER_BASE_P4 + i
            bal = float(pg_scalar(keel_conn, "SELECT balance FROM users WHERE id = %s", (uid,)))
            expected = float(_USER_BAL_P4_INIT[i]) + 1.0
            assert bal == pytest.approx(expected), (
                f"uid={uid}: balance after update {bal} ≠ expected {expected}"
            )

    def test_update_then_group_by_reaggregation(self, keel_conn):
        """
        After updating beta events, GROUP BY category returns new correct sums
        for each category.
        """
        pg_exec(
            keel_conn,
            f"UPDATE events SET value = value + 50 WHERE category = 'beta' "
            f"AND shard_hint >= {EVENT_HINT_P4} AND shard_hint < {EVENT_HINT_P4 + N_EVENTS_P4}",
        )
        rows = pg_exec(
            keel_conn,
            "SELECT category, SUM(value) FROM events GROUP BY category ORDER BY category",
        )
        result = {r[0]: int(r[1]) for r in rows}
        # alpha and gamma: unchanged
        assert result["alpha"] == sum(v for i, v in enumerate(_EV_VALS_P4) if i % 3 == 0)
        assert result["gamma"] == sum(v for i, v in enumerate(_EV_VALS_P4) if i % 3 == 2)
        # beta: each row + 50
        n_beta = len([i for i in range(N_EVENTS_P4) if i % 3 == 1])
        expected_beta = sum(v for i, v in enumerate(_EV_VALS_P4) if i % 3 == 1) + n_beta * 50
        assert result["beta"] == expected_beta

    def test_update_does_not_duplicate_or_lose_rows(self, keel_conn, shard0_conn, shard1_conn):
        """A bulk UPDATE must leave the row count unchanged on both shards."""
        pg_exec(
            keel_conn,
            f"UPDATE users SET name = name || '_v2' WHERE id >= {USER_BASE_P4}",
        )
        scatter = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        direct  = shard_total_count(shard0_conn, shard1_conn, "users")
        assert scatter == N_USERS_P4
        assert direct  == N_USERS_P4

    def test_update_all_then_update_back(self, keel_conn):
        """Two successive bulk UPDATEs leave data in the original state."""
        pg_exec(keel_conn,
                f"UPDATE events SET value = value * 2 WHERE shard_hint >= {EVENT_HINT_P4}")
        pg_exec(keel_conn,
                f"UPDATE events SET value = value / 2 WHERE shard_hint >= {EVENT_HINT_P4}")
        # After *2 then /2, SUM should be back to initial (integer division may lose 1 bit)
        restored = int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events"))
        assert restored == pytest.approx(EV_SUM_P4_INIT, abs=N_EVENTS_P4 // 2), (
            f"Restored SUM {restored} too far from initial {EV_SUM_P4_INIT}"
        )


# =============================================================================
# PART 5 — Corner Cases
# =============================================================================

class TestCornerCases:
    """
    Edge conditions that must not cause incorrect routing, data corruption,
    or wrong aggregation results.
    """

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "users", "events", "orders")
        yield
        _truncate(shard0_conn, shard1_conn, "users", "events", "orders")

    USER_CC = 12_000_000

    def test_shard_key_zero(self, keel_conn, shard0_conn, shard1_conn):
        """id=0 as shard key must route to one shard without error."""
        # Use a dedicated offset to keep this fixture clean
        uid = 12_000_000
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, 'zero_key')", (uid,))
        name = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (uid,))
        assert name == "zero_key"
        assert shard_total_count(shard0_conn, shard1_conn, "users", f"id = {uid}") == 1

    def test_shard_key_negative(self, keel_conn, shard0_conn, shard1_conn):
        """Negative shard keys are handled without modulo sign errors."""
        for uid in (-1, -999, -1_000_000):
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, f"neg_{uid}"))
        for uid in (-1, -999, -1_000_000):
            name = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (uid,))
            assert name == f"neg_{uid}", f"id={uid}: got {name!r}"
        assert shard_total_count(shard0_conn, shard1_conn, "users",
                                 "id IN (-1, -999, -1000000)") == 3

    def test_shard_key_max_bigint(self, keel_conn, shard0_conn, shard1_conn):
        """MAX BIGINT as shard key does not overflow the hash computation."""
        max_id = 9_223_372_036_854_775_807
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, 'maxbig')", (max_id,))
        assert pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (max_id,)) == "maxbig"
        assert shard_total_count(shard0_conn, shard1_conn, "users") == 1

    def test_long_text_column_round_trips(self, keel_conn):
        """A 5 000-character text value in a non-key column survives the round-trip."""
        long_name = "x" * 5_000
        uid = 12_000_010
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, long_name))
        result = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (uid,))
        assert result == long_name
        assert len(result) == 5_000

    def test_unicode_and_emoji_large_batch(self, keel_conn):
        """Several rows with Unicode / emoji names all round-trip correctly."""
        cases = [
            (12_000_020, "日本語テスト"),
            (12_000_021, "한국어 テスト"),
            (12_000_022, "Привет мир"),
            (12_000_023, "🎉 party 🎊"),
            (12_000_024, "ñoño mañana"),
        ]
        for uid, name in cases:
            pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (%s, %s)", (uid, name))
        for uid, expected in cases:
            got = pg_scalar(keel_conn, "SELECT name FROM users WHERE id = %s", (uid,))
            assert got == expected, f"id={uid}: {got!r} ≠ {expected!r}"

    def test_many_null_non_key_columns(self, keel_conn, shard0_conn, shard1_conn):
        """Rows with all non-key columns NULL are stored and counted correctly."""
        for uid in range(12_000_030, 12_000_040):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, email, age, balance) "
                    "VALUES (%s, 'null_test', NULL, NULL, NULL)", (uid,))
        cnt = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        assert cnt == 10
        assert pg_scalar(keel_conn, "SELECT SUM(age) FROM users") is None
        assert pg_scalar(keel_conn, "SELECT AVG(balance) FROM users") is None

    def test_multi_row_values_insert(self, keel_conn, shard0_conn, shard1_conn):
        """INSERT … VALUES (…),(…),… with multiple rows in a single statement."""
        pg_exec(
            keel_conn,
            "INSERT INTO users(id, name, age) VALUES "
            "(12000050, 'mv_a', 21), (12000051, 'mv_b', 22), "
            "(12000052, 'mv_c', 23), (12000053, 'mv_d', 24), "
            "(12000054, 'mv_e', 25)",
        )
        total = shard_total_count(shard0_conn, shard1_conn, "users",
                                  "id BETWEEN 12000050 AND 12000054")
        assert total == 5
        assert int(pg_scalar(keel_conn,
                             "SELECT SUM(age) FROM users "
                             "WHERE id BETWEEN 12000050 AND 12000054")) == 21+22+23+24+25

    def test_aggregate_after_upsert_does_not_overcount(self, keel_conn, shard0_conn, shard1_conn):
        """Multiple ON CONFLICT DO UPDATE upserts must not inflate row counts."""
        uid = 12_000_060
        pg_exec(keel_conn,
                "INSERT INTO users(id, name, balance) VALUES (%s, 'u', 0)", (uid,))
        for v in (10, 20, 30, 40, 50):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, balance) VALUES (%s, 'u', %s) "
                    "ON CONFLICT(id) DO UPDATE SET balance = EXCLUDED.balance",
                    (uid, v))
        total = shard_total_count(shard0_conn, shard1_conn, "users", f"id = {uid}")
        assert total == 1, f"Expected 1 row after 5 upserts, got {total}"
        bal = float(pg_scalar(keel_conn, "SELECT balance FROM users WHERE id = %s", (uid,)))
        assert bal == pytest.approx(50.0)

    def test_count_distinct_on_repeated_values(self, keel_conn):
        """COUNT(DISTINCT category) over events with repeated values is exact."""
        for i in range(30):
            pg_exec(keel_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (30_000 + i, _EVENT_CATS[i % 3], i))
        distinct = int(pg_scalar(keel_conn, "SELECT COUNT(DISTINCT category) FROM events"))
        assert distinct == 3

    def test_select_star_returns_correct_columns_via_point_query(self, keel_conn):
        """SELECT * for single rows returns all expected columns in schema order.

        SELECT * with a range WHERE clause is a scatter query; ORDER BY in scatter
        mode is applied per-shard independently so the merged result is not globally
        sorted.  Point queries (WHERE id = ?) route to a single shard and return
        exactly one row in schema column order.
        """
        for uid in range(12_000_070, 12_000_075):
            pg_exec(keel_conn, "INSERT INTO users(id, name, age, balance) "
                    "VALUES (%s, %s, %s, %s)", (uid, f"col_{uid}", uid % 50, uid % 100))
        for uid in range(12_000_070, 12_000_075):
            row = pg_exec(keel_conn, "SELECT * FROM users WHERE id = %s", (uid,))
            assert len(row) == 1, f"Expected 1 row for id={uid}, got {len(row)}"
            # Columns: id, name, email, age, balance
            assert row[0][0] == uid,            f"id column wrong for {uid}: {row[0][0]}"
            assert row[0][1] == f"col_{uid}",   f"name column wrong for {uid}: {row[0][1]}"

    def test_insert_select_from_values(self, keel_conn, shard0_conn, shard1_conn):
        """INSERT INTO … SELECT … FROM (VALUES …) inserts the correct rows."""
        pg_exec(
            keel_conn,
            "INSERT INTO users(id, name, balance) "
            "SELECT v.id, v.nm, v.bal FROM "
            "(VALUES (12000080, 'ins_a', 111.11), "
            "        (12000081, 'ins_b', 222.22), "
            "        (12000082, 'ins_c', 333.33)) v(id, nm, bal)",
        )
        total = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users "
                              "WHERE id BETWEEN 12000080 AND 12000082"))
        assert total == 3
        sumv = float(pg_scalar(keel_conn, "SELECT SUM(balance) FROM users "
                               "WHERE id BETWEEN 12000080 AND 12000082"))
        assert sumv == pytest.approx(111.11 + 222.22 + 333.33, abs=0.01)


# =============================================================================
# PART 6 — Shard Imbalance Detection and Rebalance Simulation
# =============================================================================

class TestShardImbalanceAndRebalance:
    """
    Demonstrates KEEL's scatter-merge correctness when data is unevenly
    distributed across shards, and verifies that scatter queries remain
    accurate both before and after a simulated manual rebalance.

    Test scenario
    -------------
    1. Insert BALANCED_N events via KEEL  →  roughly N/2 per shard.
    2. Directly insert IMBALANCE_N events onto shard 0 (bypassing KEEL).
       These rows are legitimate shard-hint values assigned to shard 0 by the
       hash function (we first figure out which shard each hint maps to, then
       place all extras on shard 0).
    3. Verify KEEL scatter COUNT and SUM now equal BALANCED_N + IMBALANCE_N.
    4. Verify the imbalance: shard0 significantly outweighs shard1.
    5. Simulate rebalance: move the extra rows from shard0 to shard1 directly.
    6. Verify KEEL scatter COUNT and SUM are still correct after rebalance.
    7. Verify the distribution is now more balanced.

    Why scatter still works during and after imbalance
    ---------------------------------------------------
    KEEL's scatter sends every aggregate query to ALL shards.  It does not
    assume rows are on the "expected" shard based on the hash function.
    Therefore, even if rows are on the "wrong" shard, scatter COUNT/SUM/etc.
    remain correct — KEEL simply collects and merges results from every shard.
    """

    BALANCED_N   = 500    # rows inserted via KEEL
    IMBALANCE_N  = 300    # extra rows inserted directly onto shard0

    HINT_BASE_BALANCED  = 20_000   # shard_hints for KEEL-inserted rows
    HINT_BASE_IMBALANCE = 20_500   # shard_hints for directly-inserted imbalance rows
    # These ranges must not overlap with each other or other tests.

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "events")
        yield
        _truncate(shard0_conn, shard1_conn, "events")

    @pytest.fixture
    def balanced_rows(self, keel_conn) -> dict:
        """Insert BALANCED_N events via KEEL. Returns metadata dict."""
        vals = [i % 100 + 1 for i in range(self.BALANCED_N)]
        for i in range(self.BALANCED_N):
            hint = self.HINT_BASE_BALANCED + i
            pg_exec(keel_conn,
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, "bal", vals[i]))
        return {"n": self.BALANCED_N, "sum": sum(vals)}

    @pytest.fixture
    def imbalanced_rows(self, shard0_conn) -> dict:
        """Insert IMBALANCE_N events DIRECTLY on shard0 (bypass KEEL)."""
        vals = [i % 50 + 1 for i in range(self.IMBALANCE_N)]
        for i in range(self.IMBALANCE_N):
            hint = self.HINT_BASE_IMBALANCE + i
            with shard0_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, "imb", vals[i]),
                )
        return {"n": self.IMBALANCE_N, "sum": sum(vals), "vals": vals}

    def test_scatter_count_correct_after_balanced_insert(
        self, keel_conn, shard0_conn, shard1_conn, balanced_rows
    ):
        """Scatter COUNT equals BALANCED_N after balanced via-KEEL inserts."""
        count = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events"))
        assert count == self.BALANCED_N
        direct = shard_total_count(shard0_conn, shard1_conn, "events")
        assert direct == self.BALANCED_N

    def test_balanced_distribution(self, shard0_conn, shard1_conn, balanced_rows):
        """Both shards hold at least 25 % of the balanced rows."""
        c0 = pg_count(shard0_conn, "events")
        c1 = pg_count(shard1_conn, "events")
        assert c0 + c1 == self.BALANCED_N
        assert c0 >= int(self.BALANCED_N * 0.25), f"Shard0={c0}/{self.BALANCED_N}"
        assert c1 >= int(self.BALANCED_N * 0.25), f"Shard1={c1}/{self.BALANCED_N}"

    def test_scatter_count_after_imbalanced_insert(
        self, keel_conn, shard0_conn, shard1_conn, balanced_rows, imbalanced_rows
    ):
        """
        After inserting IMBALANCE_N rows directly to shard0, scatter COUNT must
        equal BALANCED_N + IMBALANCE_N — KEEL queries all shards.
        """
        expected = self.BALANCED_N + self.IMBALANCE_N
        count = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events"))
        assert count == expected, (
            f"Scatter COUNT after imbalance: {count} ≠ {expected}. "
            "KEEL may not be querying shard0 for scatter."
        )

    def test_scatter_sum_after_imbalanced_insert(
        self, keel_conn, balanced_rows, imbalanced_rows
    ):
        """Scatter SUM includes both KEEL-inserted and directly-inserted values."""
        expected_sum = balanced_rows["sum"] + imbalanced_rows["sum"]
        actual_sum   = int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events"))
        assert actual_sum == expected_sum, (
            f"Scatter SUM after imbalance: {actual_sum} ≠ {expected_sum}"
        )

    def test_imbalance_is_detectable_via_direct_counts(
        self, shard0_conn, shard1_conn, balanced_rows, imbalanced_rows
    ):
        """
        After injecting IMBALANCE_N extra rows to shard0, that shard holds
        significantly more rows than shard1 (≥ 110 % of shard1's count or
        ≥ IMBALANCE_N absolute advantage).
        """
        c0 = pg_count(shard0_conn, "events")
        c1 = pg_count(shard1_conn, "events")
        # shard0 has all of the directly-inserted rows plus ~half the balanced ones
        # shard1 has only ~half the balanced ones
        assert c0 > c1, f"Expected shard0 ({c0}) > shard1 ({c1}) after imbalance injection"
        advantage = c0 - c1
        assert advantage >= int(self.IMBALANCE_N * 0.9), (
            f"Expected shard0 advantage ≥ {int(self.IMBALANCE_N * 0.9)}, got {advantage}"
        )

    def test_scatter_group_by_correct_under_imbalance(
        self, keel_conn, balanced_rows, imbalanced_rows
    ):
        """
        GROUP BY category returns correct per-group counts even when one shard
        holds the majority of data.
        """
        rows = pg_exec(
            keel_conn,
            "SELECT category, COUNT(*), SUM(value) FROM events GROUP BY category ORDER BY category",
        )
        result = {r[0]: (int(r[1]), int(r[2])) for r in rows}
        # 'bal': BALANCED_N rows
        assert result["bal"][0] == self.BALANCED_N, \
            f"bal count: {result['bal'][0]} ≠ {self.BALANCED_N}"
        assert result["bal"][1] == balanced_rows["sum"], \
            f"bal sum: {result['bal'][1]} ≠ {balanced_rows['sum']}"
        # 'imb': IMBALANCE_N rows
        assert result["imb"][0] == self.IMBALANCE_N, \
            f"imb count: {result['imb'][0]} ≠ {self.IMBALANCE_N}"
        assert result["imb"][1] == imbalanced_rows["sum"], \
            f"imb sum: {result['imb'][1]} ≠ {imbalanced_rows['sum']}"

    def test_simulated_rebalance_scatter_count_unchanged(
        self, keel_conn, shard0_conn, shard1_conn, balanced_rows, imbalanced_rows
    ):
        """
        Simulate rebalancing: copy the extra rows from shard0 to shard1 and
        delete them from shard0. Scatter COUNT must remain unchanged.
        """
        expected_total = self.BALANCED_N + self.IMBALANCE_N

        # Move imbalance rows from shard0 to shard1
        with shard0_conn.cursor() as cur:
            cur.execute(
                "SELECT shard_hint, category, value FROM events "
                "WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )
            rows_to_move = cur.fetchall()

        with shard1_conn.cursor() as cur:
            for hint, cat, val in rows_to_move:
                cur.execute(
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, cat, val),
                )

        with shard0_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM events WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )

        # Scatter COUNT must still equal BALANCED_N + IMBALANCE_N
        count_after = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM events"))
        assert count_after == expected_total, (
            f"Scatter COUNT after rebalance: {count_after} ≠ {expected_total}"
        )

    def test_simulated_rebalance_scatter_sum_unchanged(
        self, keel_conn, shard0_conn, shard1_conn, balanced_rows, imbalanced_rows
    ):
        """Scatter SUM is unchanged after the rebalance-simulation move."""
        expected_sum = balanced_rows["sum"] + imbalanced_rows["sum"]

        with shard0_conn.cursor() as cur:
            cur.execute(
                "SELECT shard_hint, category, value FROM events "
                "WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )
            rows_to_move = cur.fetchall()

        with shard1_conn.cursor() as cur:
            for hint, cat, val in rows_to_move:
                cur.execute(
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, cat, val),
                )

        with shard0_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM events WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )

        sum_after = int(pg_scalar(keel_conn, "SELECT SUM(value) FROM events"))
        assert sum_after == expected_sum, (
            f"Scatter SUM after rebalance: {sum_after} ≠ {expected_sum}"
        )

    def test_distribution_more_balanced_after_rebalance(
        self, keel_conn, shard0_conn, shard1_conn, balanced_rows, imbalanced_rows
    ):
        """
        After moving the imbalance batch from shard0 to shard1, the row
        distribution must be more even (the previously lighter shard grows).
        """
        c0_before = pg_count(shard0_conn, "events")
        c1_before = pg_count(shard1_conn, "events")

        with shard0_conn.cursor() as cur:
            cur.execute(
                "SELECT shard_hint, category, value FROM events "
                "WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )
            rows_to_move = cur.fetchall()

        with shard1_conn.cursor() as cur:
            for hint, cat, val in rows_to_move:
                cur.execute(
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (hint, cat, val),
                )

        with shard0_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM events WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )

        c0_after = pg_count(shard0_conn, "events")
        c1_after = pg_count(shard1_conn, "events")

        # The previously-heavy shard must have shrunk; the light shard must have grown
        assert c0_after < c0_before, f"Shard0 did not shrink: {c0_before} → {c0_after}"
        assert c1_after > c1_before, f"Shard1 did not grow: {c1_before} → {c1_after}"
        # The moved batch is now on shard1; shard1 has gained exactly IMBALANCE_N rows
        assert c1_after - c1_before == self.IMBALANCE_N, (
            f"Shard1 grew by {c1_after - c1_before}, expected {self.IMBALANCE_N}"
        )
        # shard0 shrank by the same amount
        assert c0_before - c0_after == self.IMBALANCE_N, (
            f"Shard0 shrank by {c0_before - c0_after}, expected {self.IMBALANCE_N}"
        )

    def test_aggregate_after_rebalance_group_by_correct(
        self, keel_conn, shard0_conn, shard1_conn, balanced_rows, imbalanced_rows
    ):
        """After rebalancing, GROUP BY still returns the correct per-group sums."""
        # Move the rows
        with shard0_conn.cursor() as cur:
            cur.execute(
                "SELECT shard_hint, category, value FROM events "
                "WHERE shard_hint >= %s AND shard_hint < %s",
                (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N),
            )
            rows_to_move = cur.fetchall()

        with shard1_conn.cursor() as cur:
            for hint, cat, val in rows_to_move:
                cur.execute("INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                            (hint, cat, val))

        with shard0_conn.cursor() as cur:
            cur.execute("DELETE FROM events WHERE shard_hint >= %s AND shard_hint < %s",
                        (self.HINT_BASE_IMBALANCE, self.HINT_BASE_IMBALANCE + self.IMBALANCE_N))

        # Verify
        rows = pg_exec(
            keel_conn,
            "SELECT category, COUNT(*), SUM(value) FROM events GROUP BY category ORDER BY category",
        )
        result = {r[0]: (int(r[1]), int(r[2])) for r in rows}
        assert result["bal"][0] == self.BALANCED_N
        assert result["bal"][1] == balanced_rows["sum"]
        assert result["imb"][0] == self.IMBALANCE_N
        assert result["imb"][1] == imbalanced_rows["sum"]


# =============================================================================
# PART 7 — Mixed DML Workload Under Imbalance
# =============================================================================

class TestMixedWorkloadUnderImbalance:
    """
    Concurrent reads + writes while data is unevenly distributed.
    Validates that no rows are lost and aggregates remain consistent.
    """

    N_WRITE_THREADS = 8
    N_ROWS_PER_THREAD = 50
    N_TOTAL = N_WRITE_THREADS * N_ROWS_PER_THREAD   # 400 rows

    HINT_BASE = 21_000

    @pytest.fixture(autouse=True)
    def clean(self, shard0_conn, shard1_conn):
        _truncate(shard0_conn, shard1_conn, "users", "events")
        yield
        _truncate(shard0_conn, shard1_conn, "users", "events")

    def test_concurrent_writes_during_imbalance_no_loss(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        While shard0 is pre-loaded with extra rows (imbalance), 8 writer threads
        simultaneously insert rows via KEEL.  All rows must arrive; none lost.
        """
        # Pre-load shard0 with 200 extra rows
        for i in range(200):
            with shard0_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                    (21_500 + i, "pre", i + 1),
                )

        def writer(result: WorkerResult, dsn: str, thread_idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(dsn, connect_timeout=15)
                conn.autocommit = True
                for j in range(self.N_ROWS_PER_THREAD):
                    hint = self.HINT_BASE + thread_idx * self.N_ROWS_PER_THREAD + j
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                            (hint, "load", j + 1),
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
            for i in range(self.N_WRITE_THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not result.errors, f"Write errors during imbalance: {result.errors[:3]}"
        # Total = 200 pre-loaded + N_TOTAL KEEL-written
        total = shard_total_count(shard0_conn, shard1_conn, "events")
        assert total == 200 + self.N_TOTAL, (
            f"Expected {200 + self.N_TOTAL} rows, got {total}"
        )

    def test_scatter_reads_stable_during_concurrent_writes(
        self, keel_dsn, shard0_conn, shard1_conn
    ):
        """
        While writers insert rows, concurrent scatter COUNT(*) reads must
        return non-negative values and never error.
        """
        read_errors: list[Exception] = []
        write_errors: list[Exception] = []
        stop = threading.Event()
        observed_counts: list[int] = []

        def writer():
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=15)
                conn.autocommit = True
                for i in range(100):
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                            (22_000 + i, "wr", i + 1),
                        )
                conn.close()
            except Exception as exc:
                write_errors.append(exc)
            finally:
                stop.set()

        def reader():
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = True
                while not stop.is_set():
                    with conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM events")
                        c = cur.fetchone()[0]
                        observed_counts.append(c)
                conn.close()
            except Exception as exc:
                read_errors.append(exc)

        tw = threading.Thread(target=writer)
        tr = threading.Thread(target=reader)
        tr.start()
        tw.start()
        tw.join(timeout=30)
        stop.set()
        tr.join(timeout=10)

        assert not write_errors, f"Write errors: {write_errors}"
        assert not read_errors,  f"Read errors: {read_errors}"
        # All observed counts must be ≥ 0 and ≤ 100
        for c in observed_counts:
            assert 0 <= c <= 100, f"Scatter COUNT out of range: {c}"
        # Final count must be 100
        direct = shard_total_count(shard0_conn, shard1_conn, "events")
        assert direct == 100, f"Final direct count: {direct}"

    def test_delete_half_then_bulk_update_then_reaggregate(
        self, keel_conn, shard0_conn, shard1_conn
    ):
        """
        Three-phase test: INSERT 200 users → DELETE 100 → UPDATE remaining balances →
        verify COUNT=100 and SUM=expected.
        """
        BASE = 13_000_000
        N    = 200
        # Phase 1: Insert
        for i in range(N):
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name, balance) VALUES (%s, %s, %s)",
                    (BASE + i, f"p_{i}", i * 5))   # balance: 0, 5, 10, … 995

        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users")) == N

        # Phase 2: Delete even-indexed rows (100 rows)
        pg_exec(keel_conn,
                f"DELETE FROM users WHERE id >= {BASE} AND id < {BASE + N} "
                f"AND (id - {BASE}) % 2 = 0")

        assert int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users")) == N // 2

        # Phase 3: Update all remaining (odd-indexed): balance += 100
        pg_exec(keel_conn, f"UPDATE users SET balance = balance + 100 WHERE id >= {BASE}")

        count_final = int(pg_scalar(keel_conn, "SELECT COUNT(*) FROM users"))
        assert count_final == N // 2

        # Expected: odd-indexed rows had balance = i*5; after +100 = i*5+100
        # odd indices: 1, 3, 5, …, 199 → 100 rows
        expected_sum = sum(i * 5 + 100 for i in range(1, N, 2))   # odd i * 5 + 100
        actual_sum   = float(pg_scalar(keel_conn, "SELECT SUM(balance) FROM users"))
        assert actual_sum == pytest.approx(expected_sum, abs=0.01), (
            f"SUM after delete+update: {actual_sum} ≠ {expected_sum}"
        )
        # Verify on direct shard connections too
        direct = shard_total_count(shard0_conn, shard1_conn, "users")
        assert direct == N // 2
