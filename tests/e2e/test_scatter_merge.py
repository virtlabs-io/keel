"""
test_scatter_merge.py — Scatter-merge aggregation tests
========================================================

Tests that verify KEEL correctly fans out aggregate queries to all shards and
merges partial results into a single correct answer for the client.

Background
----------
KEEL transparently fans out a query to all shards, collects the partial
results, and merges them into a single result set returned to the client.

For aggregate queries (COUNT, SUM, MIN, MAX, AVG, GROUP BY), KEEL must:
1. Send the query to every shard.
2. Collect partial results from each shard.
3. Merge them according to the aggregate semantics:
   - SUM: add all partial sums.
   - COUNT: add all partial counts.
   - MIN/MAX: min/max of partial results.
   - AVG: (sum of all partial sums) / (sum of all partial counts).
   - GROUP BY: merge groups from all shards, aggregating within each group.
   - ORDER BY: merge-sort partial result sets.

What is tested
--------------
- COUNT, SUM, MIN, MAX across shards
- AVG (requires sum/count finalize)
- GROUP BY with hash-merge
- HAVING post-filter
- ORDER BY on merged output
- Mixed aggregates in a single query

Dataset (inserted in setup, deleted in teardown)::

  events table — shard_hint controls routing
  | shard_hint | category | value |
  |------------|----------|-------|
  |  0         | alpha    |  10   |
  |  0         | alpha    |  20   |
  |  0         | beta     |  30   |
  |  1         | alpha    |  40   |
  |  1         | beta     |  50   |
  |  1         | beta     |  60   |

  Total events: 6, SUM=210, AVG=35, MAX=60, MIN=10
  alpha: 3 rows, SUM=70
  beta:  3 rows, SUM=140

Why these tests exist
---------------------
Scatter-merge is one of KEEL's most complex code paths.  Bugs manifest as:
- Wrong aggregate values (off-by-one in merge, partial result not included).
- Missing groups (GROUP BY key on one shard not merged with the same key on
  another shard).
- Wrong ORDER BY after merge (shards return sorted partial sets but the merge
  step doesn't respect the global order).
- NULL handling errors (NULL from one shard not propagated correctly).

Why a test might fail
---------------------
- **Merge algorithm bug**: partial results from shard 1 not included in the
  final merge → aggregates look correct for shard 0 data only.
- **Data setup failure**: if the ``setup_events`` fixture fails to insert rows
  (e.g. 2PC error), the test sees empty tables and all aggregates return NULL
  or 0, making asserts pass vacuously.  Always check row counts first.
- **AVG precision**: floating-point AVG may differ by epsilon across architectures.
  Tests use integer-valued datasets to avoid FP issues.

Consequences of failure
-----------------------
- Application receives wrong aggregate results → incorrect business logic
  (e.g. wrong inventory count, wrong revenue total).
- GROUP BY merge failure → some user segments silently missing from reports.
- ORDER BY merge failure → paginated results skip or duplicate rows.
"""

from __future__ import annotations

import time

import pytest
import psycopg2

from helpers import pg_exec, pg_scalar, clear_table_on_shards

pytestmark = pytest.mark.scatter


@pytest.fixture(scope="module", autouse=True)
def wait_for_keel_healthy(keel_dsn):
    """Wait for KEEL to be fully healthy (shard routing working) before scatter-merge tests."""
    # Use IDs that route to different shards to verify both are reachable via keel.
    probe_ids = (88881, 88882)
    for attempt in range(30):
        try:
            conn = psycopg2.connect(keel_dsn, connect_timeout=5)
            conn.autocommit = True
            with conn.cursor() as cur:
                for pid in probe_ids:
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, 'probe') ON CONFLICT DO NOTHING",
                        (pid,)
                    )
                for pid in probe_ids:
                    cur.execute("DELETE FROM users WHERE id = %s", (pid,))
            conn.close()
            return
        except psycopg2.Error:
            time.sleep(2)
    raise RuntimeError("KEEL did not become healthy within timeout before scatter-merge tests")


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

DATASET = [
    # (shard_hint, category, value)
    (0,  "alpha",  10),
    (2,  "alpha",  20),
    (4,  "beta",   30),
    (1,  "alpha",  40),
    (3,  "beta",   50),
    (5,  "beta",   60),
]

TOTAL_ROWS  = len(DATASET)               # 6
TOTAL_SUM   = sum(r[2] for r in DATASET) # 210
TOTAL_MIN   = min(r[2] for r in DATASET) # 10
TOTAL_MAX   = max(r[2] for r in DATASET) # 60
TOTAL_AVG   = TOTAL_SUM / TOTAL_ROWS     # 35.0

ALPHA_ROWS  = [r for r in DATASET if r[1] == "alpha"]
BETA_ROWS   = [r for r in DATASET if r[1] == "beta"]
ALPHA_SUM   = sum(r[2] for r in ALPHA_ROWS)   # 70
BETA_SUM    = sum(r[2] for r in BETA_ROWS)    # 140
ALPHA_COUNT = len(ALPHA_ROWS)                 # 3
BETA_COUNT  = len(BETA_ROWS)                  # 3


@pytest.fixture(autouse=True)
def populate_events(keel_dsn, keel_conn, shard0_conn, shard1_conn):
    """Load test dataset into events before each test, clean up after."""
    clear_table_on_shards(shard0_conn, shard1_conn, "events")
    # Retry with fresh connections in case the backend is still draining
    # stale connections after failover tests (backend EOF storms).
    last_exc = None
    for _attempt in range(10):
        try:
            conn = psycopg2.connect(keel_dsn, connect_timeout=10)
            conn.autocommit = True
            with conn.cursor() as cur:
                for hint, cat, val in DATASET:
                    cur.execute(
                        "INSERT INTO events(shard_hint, category, value) VALUES (%s, %s, %s)",
                        (hint, cat, val)
                    )
            conn.close()
            last_exc = None
            break
        except psycopg2.Error as exc:
            last_exc = exc
            try:
                conn.close()
            except Exception:
                pass
            clear_table_on_shards(shard0_conn, shard1_conn, "events")
            time.sleep(2)
    if last_exc is not None:
        raise last_exc
    yield
    clear_table_on_shards(shard0_conn, shard1_conn, "events")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestScalarAggregates:

    def test_count_star(self, keel_conn):
        """COUNT(*) aggregates rows from all shards."""
        count = pg_scalar(keel_conn, "SELECT COUNT(*) FROM events")
        assert count == TOTAL_ROWS, f"Expected {TOTAL_ROWS}, got {count}"

    def test_sum(self, keel_conn):
        """SUM(value) merges partial sums from all shards."""
        total = pg_scalar(keel_conn, "SELECT SUM(value) FROM events")
        assert int(total) == TOTAL_SUM

    def test_min(self, keel_conn):
        """MIN(value) returns the global minimum across shards."""
        minimum = pg_scalar(keel_conn, "SELECT MIN(value) FROM events")
        assert minimum == TOTAL_MIN

    def test_max(self, keel_conn):
        """MAX(value) returns the global maximum across shards."""
        maximum = pg_scalar(keel_conn, "SELECT MAX(value) FROM events")
        assert maximum == TOTAL_MAX

    def test_avg(self, keel_conn):
        """AVG(value) computes sum/count correctly across shards."""
        avg = pg_scalar(keel_conn, "SELECT AVG(value) FROM events")
        assert avg is not None
        assert abs(float(avg) - TOTAL_AVG) < 0.01, f"Expected {TOTAL_AVG}, got {avg}"

    def test_count_distinct(self, keel_conn):
        """COUNT(DISTINCT category) returns 2 (alpha, beta) from scattered data."""
        count = pg_scalar(keel_conn, "SELECT COUNT(DISTINCT category) FROM events")
        assert count == 2

    def test_sum_with_filter(self, keel_conn):
        """SUM with WHERE clause filters before scatter-merge."""
        alpha_sum = pg_scalar(keel_conn,
                               "SELECT SUM(value) FROM events WHERE category = 'alpha'")
        assert int(alpha_sum) == ALPHA_SUM


class TestGroupBy:

    def test_group_by_category_count(self, keel_conn):
        """GROUP BY category COUNT(*) returns correct per-group counts."""
        rows = pg_exec(keel_conn,
                       "SELECT category, COUNT(*) FROM events GROUP BY category ORDER BY category")
        assert len(rows) == 2
        cats = {r[0]: r[1] for r in rows}
        assert cats["alpha"] == ALPHA_COUNT
        assert cats["beta"]  == BETA_COUNT

    def test_group_by_category_sum(self, keel_conn):
        """GROUP BY category SUM(value) merges per-group sums from all shards."""
        rows = pg_exec(keel_conn,
                       "SELECT category, SUM(value) FROM events GROUP BY category ORDER BY category")
        sums = {r[0]: int(r[1]) for r in rows}
        assert sums["alpha"] == ALPHA_SUM
        assert sums["beta"]  == BETA_SUM

    def test_group_by_category_avg(self, keel_conn):
        """GROUP BY category AVG(value) finalises per-group averages."""
        rows = pg_exec(keel_conn,
                       "SELECT category, AVG(value) FROM events GROUP BY category ORDER BY category")
        avgs = {r[0]: float(r[1]) for r in rows}
        assert abs(avgs["alpha"] - ALPHA_SUM / ALPHA_COUNT) < 0.01
        assert abs(avgs["beta"]  - BETA_SUM  / BETA_COUNT)  < 0.01

    def test_group_by_multiple_aggregates(self, keel_conn):
        """A single GROUP BY query with COUNT, SUM, MIN, MAX all correct."""
        rows = pg_exec(keel_conn,
                       "SELECT category, COUNT(*), SUM(value), MIN(value), MAX(value) "
                       "FROM events GROUP BY category ORDER BY category")
        assert len(rows) == 2
        alpha = next(r for r in rows if r[0] == "alpha")
        beta  = next(r for r in rows if r[0] == "beta")

        assert alpha[1] == ALPHA_COUNT
        assert int(alpha[2]) == ALPHA_SUM
        assert alpha[3] == min(r[2] for r in ALPHA_ROWS)
        assert alpha[4] == max(r[2] for r in ALPHA_ROWS)

        assert beta[1] == BETA_COUNT
        assert int(beta[2]) == BETA_SUM

    def test_group_by_value_bucket(self, keel_conn):
        """GROUP BY computed expression (value > 30) works correctly."""
        rows = pg_exec(keel_conn,
                       "SELECT (value > 30), COUNT(*) FROM events "
                       "GROUP BY (value > 30) ORDER BY 1")
        counts = {r[0]: r[1] for r in rows}
        low  = sum(1 for _, _, v in DATASET if v <= 30)
        high = sum(1 for _, _, v in DATASET if v >  30)
        assert counts[False] == low
        assert counts[True]  == high


class TestHaving:

    def test_having_basic(self, keel_conn):
        """HAVING COUNT(*) > N filters groups after the merge step."""
        # Both groups have exactly 3 rows; filter for > 2 keeps both
        rows = pg_exec(keel_conn,
                       "SELECT category, COUNT(*) FROM events "
                       "GROUP BY category HAVING COUNT(*) > 2 ORDER BY category")
        assert len(rows) == 2

    def test_having_eliminates_all_groups(self, keel_conn):
        """HAVING with impossible condition returns no rows."""
        rows = pg_exec(keel_conn,
                       "SELECT category, COUNT(*) FROM events "
                       "GROUP BY category HAVING COUNT(*) > 999")
        assert rows == []

    def test_having_sum_filter(self, keel_conn):
        """HAVING SUM(value) > threshold keeps correct groups."""
        # ALPHA_SUM=70, BETA_SUM=140 — threshold 100 keeps only beta
        rows = pg_exec(keel_conn,
                       "SELECT category, SUM(value) FROM events "
                       "GROUP BY category HAVING SUM(value) > 100 ORDER BY category")
        assert len(rows) == 1
        assert rows[0][0] == "beta"
        assert int(rows[0][1]) == BETA_SUM

    def test_having_avg_filter(self, keel_conn):
        """HAVING AVG(value) filters on the finalised average."""
        # alpha AVG = 70/3 ≈ 23.3, beta AVG = 140/3 ≈ 46.7
        rows = pg_exec(keel_conn,
                       "SELECT category, AVG(value) FROM events "
                       "GROUP BY category HAVING AVG(value) > 30 ORDER BY category")
        assert len(rows) == 1
        assert rows[0][0] == "beta"


class TestOrderBy:

    def test_order_by_ascending(self, keel_conn):
        """Merged rows from all shards are sorted ascending by value."""
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value ASC")
        values = [r[0] for r in rows]
        assert values == sorted(values)
        assert values[0] == TOTAL_MIN
        assert values[-1] == TOTAL_MAX

    def test_order_by_descending(self, keel_conn):
        """ORDER BY DESC on merged results returns globally sorted descending list."""
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value DESC")
        values = [r[0] for r in rows]
        assert values == sorted(values, reverse=True)

    def test_order_by_after_group_by(self, keel_conn):
        """GROUP BY + ORDER BY on merged output sorts groups correctly."""
        rows = pg_exec(keel_conn,
                       "SELECT category, SUM(value) as total "
                       "FROM events GROUP BY category ORDER BY total DESC")
        assert len(rows) == 2
        assert rows[0][0] == "beta"   # higher sum first
        assert rows[1][0] == "alpha"

    def test_order_by_limit(self, keel_conn):
        """ORDER BY LIMIT returns the correct top-N rows from the merged set."""
        rows = pg_exec(keel_conn, "SELECT value FROM events ORDER BY value DESC LIMIT 3")
        values = [r[0] for r in rows]
        expected = sorted([r[2] for r in DATASET], reverse=True)[:3]
        assert values == expected


class TestEmptyAndEdgeCases:

    def test_empty_table_count(self, keel_conn, shard0_conn, shard1_conn):
        """COUNT(*) on an empty table returns 0."""
        clear_table_on_shards(shard0_conn, shard1_conn, "events")
        count = pg_scalar(keel_conn, "SELECT COUNT(*) FROM events")
        assert count == 0

    def test_empty_table_sum_is_null(self, keel_conn, shard0_conn, shard1_conn):
        """SUM on an empty table returns NULL (Python None)."""
        clear_table_on_shards(shard0_conn, shard1_conn, "events")
        total = pg_scalar(keel_conn, "SELECT SUM(value) FROM events")
        assert total is None

    def test_single_row(self, keel_conn, shard0_conn, shard1_conn):
        """Scatter-merge with a single row returns correct aggregates."""
        clear_table_on_shards(shard0_conn, shard1_conn, "events")
        pg_exec(keel_conn, "INSERT INTO events(shard_hint, category, value) VALUES (0, 'solo', 42)")
        count = pg_scalar(keel_conn, "SELECT COUNT(*) FROM events")
        total = pg_scalar(keel_conn, "SELECT SUM(value) FROM events")
        assert count == 1
        assert int(total) == 42
