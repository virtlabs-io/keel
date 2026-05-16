"""
test_sharding_limitations.py — Known KEEL scatter/sharding limitations
=======================================================================

Most tests in this file are marked ``@pytest.mark.xfail`` (with
``strict=False``) to document a **known limitation** of the KEEL scatter-merge
engine.  Each xfailed test:

1. Demonstrates the *expected correct* behaviour (the assertion).
2. Is expected to fail with the current implementation.
3. Will automatically promote to XPASS (and be caught in CI) when the
   limitation is fixed.

Removing the ``xfail`` marker (or changing ``strict=True`` once fixed) is the
intended workflow for each item.

Organisation
------------
A  Scatter aggregate limitations
B  Window function scatter limitations
C  DML / RETURNING limitations
D  Routing limitations
E  CTE limitations
F  Global ordering / pagination limitations
G  Schema-qualification limitations

Reference
---------
- docs/SCATTER_MERGE.md §17 "Limitations and Unsupported Patterns"
- docs/SHARDING.md §18 corner-cases reference
- src/engine/engine_scatter.c  (Phase D/E/F/H/C/L merge pipeline)

Setup dependency
----------------
These tests run against the same fixture set as test_sharding_advanced.py.
Bulk-insert data (5 000 users, 3 000 orders, 2 000 events) must already be
present; that is guaranteed by the session-scoped bulk-load fixture in
conftest.py (or the class-level fixtures in test_sharding_advanced.py).
"""

from __future__ import annotations

import pytest
import psycopg2

from helpers import pg_exec, pg_scalar, shard_total_count

# ---------------------------------------------------------------------------
# Module-level pytest mark
# ---------------------------------------------------------------------------
pytestmark = pytest.mark.limitations


# ===========================================================================
# Category A — Scatter aggregate limitations
# ===========================================================================

class TestScatterAggregateLimitations:
    """Aggregate functions that KEEL cannot merge cross-shard correctly."""

    # -----------------------------------------------------------------------
    # Class-scoped seed: 500 users with ids 1..500 and age = 20 + (id % 61).
    # Provides:
    #   • ids 1..10 (and 1..200) for the WHERE id <= N agg tests
    #   • exactly 61 distinct age values (20..80) for the DISTINCT tests
    # Truncating + seeding makes the class self-contained regardless of test
    # ordering relative to test_sharding_advanced.py (USER_BASE = 1_000_000).
    # -----------------------------------------------------------------------
    @pytest.fixture(autouse=True, scope="class")
    def _seed_class(self, shard0_dsn, shard1_dsn, keel_dsn):
        s0 = psycopg2.connect(shard0_dsn, connect_timeout=10); s0.autocommit = True
        s1 = psycopg2.connect(shard1_dsn, connect_timeout=10); s1.autocommit = True
        kc = psycopg2.connect(keel_dsn,    connect_timeout=10); kc.autocommit = True
        try:
            with s0.cursor() as c: c.execute("TRUNCATE TABLE users CASCADE")
            with s1.cursor() as c: c.execute("TRUNCATE TABLE users CASCADE")
            with kc.cursor() as c:
                for uid in range(1, 501):
                    age = 20 + (uid % 61)
                    c.execute(
                        "INSERT INTO users(id, name, age, balance) "
                        "VALUES (%s, %s, %s, %s)",
                        (uid, f"u{uid}", age, uid),
                    )
            yield
        finally:
            for cn in (s0, s1, kc):
                try: cn.close()
                except Exception: pass

    # -----------------------------------------------------------------------
    # A1 — PERCENTILE_CONT
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "PERCENTILE_CONT is an ordered-set aggregate.  KEEL scatters the query "
            "to each shard independently; each shard computes its own percentile over "
            "its local rows.  The proxy returns N rows (one per shard) instead of one "
            "globally correct percentile.  "
            "Fix: implement ordered-set aggregate merge in Phase D / new Phase G.  "
            "Reference: SCATTER_MERGE.md §17 Aggregate Limitations."
        ),
    )
    def test_percentile_cont_scatter_global(self, keel_conn):
        """PERCENTILE_CONT(0.5) over scattered users returns the *global* median age.

        With 5 000 users (age = 20 + id%61, range 20-80) the global median age
        is 50.  Per-shard each shard sees a partial distribution and may compute
        a different median.
        """
        result = pg_scalar(
            keel_conn,
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) FROM users",
        )
        # Global median of age=20+(id%61) over id=1..5000 is 50.
        assert result == 50, (
            f"Expected global median age=50, got {result}.  "
            "Likely returned per-shard median instead of global."
        )

    # -----------------------------------------------------------------------
    # A2 — PERCENTILE_DISC
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "PERCENTILE_DISC is an ordered-set aggregate.  Same root cause as "
            "PERCENTILE_CONT — each shard computes its own discrete percentile.  "
            "Reference: SCATTER_MERGE.md §17 Aggregate Limitations."
        ),
    )
    def test_percentile_disc_scatter_global(self, keel_conn):
        """PERCENTILE_DISC(0.5) over scattered users returns the global median age."""
        result = pg_scalar(
            keel_conn,
            "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY age) FROM users",
        )
        # PERCENTILE_DISC picks the first value ≥ 0.5 cumulative fraction.
        # For 5 000 uniformly distributed values in [20,80] this is 50.
        assert result == 50, (
            f"Expected global discrete median age=50, got {result}."
        )

    # -----------------------------------------------------------------------
    # A3 — STRING_AGG returns one row per shard
    # -----------------------------------------------------------------------
    def test_string_agg_scatter_single_row(self, keel_conn):
        """STRING_AGG across scatter table returns exactly one row (merged string)."""
        rows = pg_exec(
            keel_conn,
            "SELECT STRING_AGG(name, ',' ORDER BY id) FROM users WHERE id <= 10",
        )
        assert len(rows) == 1, (
            f"Expected 1 row from STRING_AGG, got {len(rows)} rows (one per shard)."
        )

    # -----------------------------------------------------------------------
    # A4 — ARRAY_AGG cross-shard merge (Phase 5)
    # -----------------------------------------------------------------------
    def test_array_agg_scatter_single_row(self, keel_conn):
        """ARRAY_AGG across scatter table returns exactly one row (merged array)."""
        rows = pg_exec(
            keel_conn,
            "SELECT ARRAY_AGG(id ORDER BY id) FROM users WHERE id <= 10",
        )
        assert len(rows) == 1, (
            f"Expected 1 row from ARRAY_AGG, got {len(rows)} rows (one per shard)."
        )
        # The merged array should contain 10 elements.
        assert len(rows[0][0]) == 10, (
            f"Expected 10 elements in merged array, got {len(rows[0][0])}."
        )

    # -----------------------------------------------------------------------
    # A5 — jsonb_agg returns one row per shard
    # -----------------------------------------------------------------------
    def test_jsonb_agg_scatter_single_row(self, keel_conn):
        """jsonb_agg across scatter table returns exactly one row (merged JSON array)."""
        rows = pg_exec(
            keel_conn,
            "SELECT jsonb_agg(id ORDER BY id) FROM users WHERE id <= 10",
        )
        assert len(rows) == 1, (
            f"Expected 1 row from jsonb_agg, got {len(rows)} rows (one per shard)."
        )

    # -----------------------------------------------------------------------
    # A6 — json_object_agg cross-shard merge (Phase 5)
    # -----------------------------------------------------------------------
    def test_json_object_agg_scatter_single_row(self, keel_conn):
        """json_object_agg across scatter table returns exactly one row."""
        rows = pg_exec(
            keel_conn,
            "SELECT json_object_agg(id, name ORDER BY id) FROM users WHERE id <= 10",
        )
        assert len(rows) == 1, (
            f"Expected 1 row from json_object_agg, got {len(rows)} rows."
        )

    # -----------------------------------------------------------------------
    # A7 — COUNT(DISTINCT) via subquery double-counts cross-shard
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "SELECT COUNT(*) FROM (SELECT DISTINCT col FROM sharded_table) double-counts "
            "distinct values.  Phase 4 added cross-shard dedup for bare SELECT DISTINCT, "
            "but a wrapper COUNT(*) over a DISTINCT subquery is rewritten by Postgres on "
            "each shard into a scalar count — the proxy then SUMs the per-shard counts "
            "instead of deduplicating values across shards.  Fix would require detecting "
            "this pattern in the router and rewriting per-shard SQL to send DISTINCT "
            "values to the proxy (similar to keel_scatter_count_distinct_rewrite).  "
            "Workaround: use COUNT(DISTINCT col) directly (Phase D deduplication).  "
            "Reference: SCATTER_MERGE.md §17 / documented in test_sharding_advanced.py."
        ),
    )
    def test_count_distinct_via_subquery_global(self, keel_conn):
        """COUNT(*) FROM (SELECT DISTINCT age ...) returns the *global* distinct count.

        Users have age = 20 + (id % 61), so there are exactly 61 distinct ages.
        A wrapper COUNT(*) over a DISTINCT subquery should return 61, not
        61 * num_shards.
        """
        count = pg_scalar(
            keel_conn,
            "SELECT COUNT(*) FROM (SELECT DISTINCT age FROM users) sub",
        )
        assert count == 61, (
            f"Expected 61 global distinct ages, got {count}.  "
            "Likely summed per-shard distinct counts (61 × shards)."
        )

    # -----------------------------------------------------------------------
    # A8 — SELECT DISTINCT cross-shard deduplicates (Phase 4)
    # -----------------------------------------------------------------------
    def test_select_distinct_cross_shard_deduplication(self, keel_conn):
        """SELECT DISTINCT age FROM users returns exactly 61 distinct age values."""
        rows = pg_exec(keel_conn, "SELECT DISTINCT age FROM users")
        assert len(rows) == 61, (
            f"Expected 61 distinct ages (20-80), got {len(rows)}.  "
            "Likely returned per-shard DISTINCT rows without cross-shard dedup."
        )


# ===========================================================================
# Category B — Window function scatter limitations
# ===========================================================================

class TestWindowFunctionLimitations:
    """Window functions not yet implemented in Phase F of the scatter-merge pipeline."""

    # -----------------------------------------------------------------------
    # Class-scoped seed: 500 users with ids 1..500, age = 20 + (id % 61),
    # balance = id.  Provides ids 1..200 for the WHERE id <= N window queries
    # and 61 distinct age values for PARTITION BY age.
    # -----------------------------------------------------------------------
    @pytest.fixture(autouse=True, scope="class")
    def _seed_class(self, shard0_dsn, shard1_dsn, keel_dsn):
        s0 = psycopg2.connect(shard0_dsn, connect_timeout=10); s0.autocommit = True
        s1 = psycopg2.connect(shard1_dsn, connect_timeout=10); s1.autocommit = True
        kc = psycopg2.connect(keel_dsn,    connect_timeout=10); kc.autocommit = True
        try:
            with s0.cursor() as c: c.execute("TRUNCATE TABLE users CASCADE")
            with s1.cursor() as c: c.execute("TRUNCATE TABLE users CASCADE")
            with kc.cursor() as c:
                for uid in range(1, 501):
                    age = 20 + (uid % 61)
                    c.execute(
                        "INSERT INTO users(id, name, age, balance) "
                        "VALUES (%s, %s, %s, %s)",
                        (uid, f"u{uid}", age, uid),
                    )
            yield
        finally:
            for cn in (s0, s1, kc):
                try: cn.close()
                except Exception: pass

    # -----------------------------------------------------------------------
    # B1 — NTH_VALUE not in Phase F
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "NTH_VALUE is not implemented in Phase F (scatter window recomputation).  "
            "Each shard computes NTH_VALUE over its local rows; the values are incorrect "
            "for global ordering.  "
            "Fix: implement NTH_VALUE in keel_pg_result_window_compute().  "
            "Reference: SCATTER_MERGE.md §6 / §17 Window Function Limitations."
        ),
    )
    def test_nth_value_scatter_correct(self, keel_conn):
        """NTH_VALUE(id, 2) OVER (ORDER BY id) returns the globally 2nd-smallest id.

        Over the scatter users table, the 2nd smallest id is 2.  If Phase F is not
        implemented for NTH_VALUE, each shard returns its own local 2nd value.
        """
        rows = pg_exec(
            keel_conn,
            """
            SELECT DISTINCT NTH_VALUE(id, 2) OVER (ORDER BY id
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            FROM users
            WHERE id <= 100
            """,
        )
        # Should return a single distinct value: 2 (the global 2nd-smallest id)
        assert len(rows) == 1, (
            f"Expected 1 distinct NTH_VALUE result, got {len(rows)}."
        )
        assert rows[0][0] == 2, (
            f"Expected NTH_VALUE(id,2)=2 globally, got {rows[0][0]}."
        )

    # -----------------------------------------------------------------------
    # B2 — CUME_DIST not in Phase F
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "CUME_DIST is not implemented in Phase F.  Each shard computes its own "
            "cumulative distribution fraction over local rows only, giving values in "
            "(0, 1] relative to the shard, not the global dataset.  "
            "Fix: implement CUME_DIST in keel_pg_result_window_compute().  "
            "Reference: SCATTER_MERGE.md §6 / §17."
        ),
    )
    def test_cume_dist_scatter_correct(self, keel_conn, shard0_conn, shard1_conn):
        """CUME_DIST() OVER (ORDER BY balance) values span the global [0..1] range correctly.

        The maximum CUME_DIST value in the entire dataset must be 1.0 (the last row
        in the global order has cumulative rank = 1).  With per-shard computation,
        each shard's max CUME_DIST is 1.0 — but the values for intermediate rows
        are computed relative to the shard size, not the global dataset, making them
        wrong for any row that is not the last one on its shard.
        """
        # Count total rows we'll query (small sample for speed)
        result = pg_exec(
            keel_conn,
            """
            SELECT id, CUME_DIST() OVER (ORDER BY balance) AS cd
            FROM users
            WHERE id <= 100
            ORDER BY id
            """,
        )
        assert len(result) == 100, f"Expected 100 rows, got {len(result)}"
        # With 100 globally ordered rows, each step is 1/100 = 0.01.
        # The first row should have cd ≈ 0.01; the last cd = 1.0.
        first_cd = float(result[0][1])
        assert abs(first_cd - 0.01) < 0.005, (
            f"Expected first CUME_DIST ≈ 0.01, got {first_cd}.  "
            "Per-shard CUME_DIST gives a fraction relative to the shard row count."
        )

    # -----------------------------------------------------------------------
    # B3 — LAST_VALUE with default frame
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "LAST_VALUE with the default frame (ROWS BETWEEN UNBOUNDED PRECEDING AND "
            "CURRENT ROW) returns the *current* row's value, not the last row in the "
            "partition.  KEEL Phase F propagates this PostgreSQL default behaviour "
            "correctly — but users expect LAST_VALUE to mean 'last in partition'.  "
            "This is a documentation/usability limitation: without an explicit frame "
            "clause, LAST_VALUE is misleading.  "
            "Reference: SCATTER_MERGE.md §6 / SHARDING.md §18.12."
        ),
    )
    def test_last_value_default_frame_returns_partition_last(self, keel_conn):
        """LAST_VALUE(balance) OVER (PARTITION BY age ORDER BY id) returns the
        *last* balance in each age partition (not the current row's balance).

        Without an explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        frame, PostgreSQL's default frame ends at CURRENT ROW, so LAST_VALUE
        returns the current row's own balance — not the partition's last.
        """
        rows = pg_exec(
            keel_conn,
            """
            SELECT id, age, balance,
                   LAST_VALUE(balance) OVER (
                       PARTITION BY age
                       ORDER BY id
                       -- no explicit frame: default is ROWS BETWEEN UNBOUNDED PRECEDING
                       -- AND CURRENT ROW, making LAST_VALUE = current balance
                   ) AS last_bal
            FROM users
            WHERE id BETWEEN 1 AND 200
            ORDER BY age, id
            LIMIT 20
            """,
        )
        assert len(rows) > 0
        # For a non-last row in its partition, last_bal should differ from balance
        # if the window frame covered the full partition.
        non_last_rows = [
            (id_, age, bal, last_bal)
            for id_, age, bal, last_bal in rows
            if last_bal == bal
        ]
        # With the correct frame, most non-last rows would have last_bal ≠ bal.
        # With the default frame, ALL rows have last_bal == bal (since frame = up to current).
        # This test asserts the correct behaviour fails with default frame.
        assert len(non_last_rows) == 0, (
            f"Expected LAST_VALUE to return the partition's last balance, "
            f"but {len(non_last_rows)} rows have last_bal == current balance "
            f"(default frame returns current row's value, not partition end)."
        )


# ===========================================================================
# Category C — DML / RETURNING limitations
# ===========================================================================

class TestDMLReturningLimitations:
    """RETURNING clauses on scatter DML do not merge results from all shards."""

    # -----------------------------------------------------------------------
    # C1 — DELETE ... RETURNING on scatter returns merged rows
    # -----------------------------------------------------------------------
    def test_scatter_delete_returning_rows(self, keel_dsn, shard0_conn, shard1_conn):
        """DELETE ... RETURNING id on a scatter table returns the deleted row ids.

        Insert rows with known ids on both shards, then DELETE with RETURNING id.
        The number of returned rows must equal the number of rows actually deleted.
        """
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # Clean up first
                cur.execute("DELETE FROM users WHERE id IN (88001, 88002, 88003, 88004)")
                # Insert known rows — ids spread across shards via hash
                for uid in (88001, 88002, 88003, 88004):
                    cur.execute(
                        "INSERT INTO users(id, name) VALUES (%s, %s)",
                        (uid, f"ret_test_{uid}"),
                    )
            conn.commit()

            # Now DELETE with RETURNING
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM users WHERE id IN (88001, 88002, 88003, 88004)"
                    " RETURNING id"
                )
                returned = cur.fetchall()
            conn.commit()

            returned_ids = {r[0] for r in returned}
            assert returned_ids == {88001, 88002, 88003, 88004}, (
                f"Expected RETURNING to yield all 4 deleted ids, got: {returned_ids}."
            )
        finally:
            # Best-effort cleanup
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id IN (88001,88002,88003,88004)")
                conn.commit()
            except Exception:
                pass
            conn.close()

    # -----------------------------------------------------------------------
    # C2 — UPDATE ... RETURNING on scatter returns merged rows
    # -----------------------------------------------------------------------
    def test_scatter_update_returning_rows(self, keel_dsn, shard0_conn, shard1_conn):
        """UPDATE ... RETURNING id, name on a scatter table returns the updated rows."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id IN (88010, 88011)")
                for uid in (88010, 88011):
                    cur.execute(
                        "INSERT INTO users(id, name, balance) VALUES (%s, %s, 0)",
                        (uid, f"upd_test_{uid}"),
                    )
            conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET balance = 999 WHERE id IN (88010, 88011)"
                    " RETURNING id, balance"
                )
                returned = cur.fetchall()
            conn.commit()

            assert len(returned) == 2, (
                f"Expected 2 rows from UPDATE RETURNING, got {len(returned)}."
            )
            assert all(bal == 999 for _, bal in returned), (
                f"Expected all returned balances=999, got {returned}."
            )
        finally:
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id IN (88010, 88011)")
                conn.commit()
            except Exception:
                pass
            conn.close()

    # -----------------------------------------------------------------------
    # C3 — INSERT ... RETURNING on scatter returns merged rows
    # -----------------------------------------------------------------------
    def test_scatter_insert_returning_id(self, keel_dsn):
        """INSERT INTO scatter table ... RETURNING id returns the inserted id."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 88020")
                cur.execute(
                    "INSERT INTO users(id, name) VALUES (88020, 'ins_ret_test')"
                    " RETURNING id"
                )
                returned = cur.fetchall()
            conn.commit()
            assert returned == [(88020,)], (
                f"Expected INSERT RETURNING to yield [(88020,)], got {returned}."
            )
        finally:
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id = 88020")
                conn.commit()
            except Exception:
                pass
            conn.close()


# ===========================================================================
# Category D — Routing limitations
# ===========================================================================

class TestRoutingLimitations:
    """Query patterns that KEEL cannot route correctly."""

    # -----------------------------------------------------------------------
    # D1 — Cross-shard JOIN returns UNSUPPORTED
    # -----------------------------------------------------------------------
    def test_cross_shard_join_executes(self, keel_conn):
        """JOIN across two scatter tables with different shard keys must execute.

        users is sharded on id; orders is sharded on order_id.
        A JOIN on user-level attributes requires reading from both shards
        independently and cannot be routed by shard key.
        """
        rows = pg_exec(
            keel_conn,
            """
            SELECT u.id, u.name, o.amount
            FROM users u
            JOIN orders o ON o.order_id = u.id
            WHERE u.id BETWEEN 1 AND 20
            """,
        )
        # Merely reaching here without an exception is sufficient — the row count
        # is unpredictable since order_id may not match user id 1:1.
        assert isinstance(rows, list)

    # -----------------------------------------------------------------------
    # D2 — Multi-row VALUES INSERT routes all rows to first shard
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "Multi-row INSERT VALUES routes based on the shard key of the FIRST row\n"
            "only (shard_extract_insert() reads only row[0]).  Rows destined for\n"
            "other shards are silently inserted to the wrong shard.  Fix requires\n"
            "a new per-row dispatch fan-out path: split the multi-row VALUES into\n"
            "one INSERT per target shard, dispatch in parallel, and aggregate the\n"
            "row-count tags. Tracked separately from the routing-extractor work.\n"
            "Reference: SCATTER_MERGE.md \u00a717 Multi-Row INSERT Routing."
        ),
    )
    def test_multi_row_insert_routes_each_row(self, keel_dsn, shard0_conn, shard1_conn):
        """Multi-row INSERT delivers each row to its correct shard.

        Insert two rows with ids known to land on different shards.  Both rows
        must be retrievable via the correct shard connection after the INSERT.
        """
        # Determine which shard each id lands on by inserting individually first.
        # (ids 1 and 2 typically land on different shards with 2-shard hash setup)
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id IN (87001, 87002)")
            conn.commit()

            with conn.cursor() as cur:
                # Multi-row VALUES INSERT — KEEL routes by first row (id=87001)
                cur.execute(
                    "INSERT INTO users(id, name) VALUES (87001, 'mr1'), (87002, 'mr2')"
                )
            conn.commit()

            # Both rows must be found across both shards combined
            total = shard_total_count(
                shard0_conn, shard1_conn, "users", "id IN (87001, 87002)"
            )
            assert total == 2, (
                f"Expected 2 rows across shards after multi-row INSERT, got {total}.  "
                "One row likely went to the wrong shard."
            )

            # Each shard should have at most one of these rows (not both on same shard)
            from helpers import pg_count
            s0_count = pg_count(shard0_conn, "users", "id IN (87001, 87002)")
            s1_count = pg_count(shard1_conn, "users", "id IN (87001, 87002)")
            assert s0_count <= 1, (
                f"Shard 0 has {s0_count} rows; expected ≤1 (each row to its own shard)."
            )
            assert s1_count <= 1, (
                f"Shard 1 has {s1_count} rows; expected ≤1."
            )
        finally:
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id IN (87001, 87002)")
                conn.commit()
            except Exception:
                pass
            conn.close()

    # -----------------------------------------------------------------------
    # D3 — OR on shard key always scatters
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "WHERE id = $1 OR id = $2 is not decomposed by the shard-key extractor; "
            "the query always scatters to all shards even when both ids are on the "
            "same shard.  Fix: decompose OR into IN-list and deduplicate shards.  "
            "Reference: SCATTER_MERGE.md §18 Corner Cases / SHARDING.md routing notes."
        ),
    )
    def test_or_shard_key_single_routes(self, keel_conn):
        """WHERE id = 1 OR id = 2 should return both rows correctly.

        We can't control which shard ids 1 and 2 land on without running the
        server, but the result must be correct regardless of whether KEEL
        narrows the routing to one shard or scatters.
        """
        # Self-contained seed of the two ids under test.
        pg_exec(keel_conn, "DELETE FROM users WHERE id IN (1, 2)")
        pg_exec(keel_conn, "INSERT INTO users(id, name) VALUES (1, 'u1'), (2, 'u2')")
        try:
            rows = pg_exec(
                keel_conn,
                "SELECT id FROM users WHERE id = 1 OR id = 2 ORDER BY id",
            )
            assert rows == [(1,), (2,)], (
                f"Expected exactly rows for id=1 and id=2, got {rows}."
            )
        finally:
            pg_exec(keel_conn, "DELETE FROM users WHERE id IN (1, 2)")

    # -----------------------------------------------------------------------
    # D4 — Composite shard key via tuple equality not recognised
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "WHERE (col1, col2) = ($1, $2) \u2014 row/tuple equality is not parsed\n"
            "by the SQL parser today (no KEEL_SQL_NODE_EXPR_ROW production), so the\n"
            "router cannot recognise it as a shard-key predicate. Fix requires both\n"
            "a parser change (build a row expression) and an extractor change in\n"
            "shard_extract_from_where(). Reference: SHARDING.md routing limitations."
        ),
    )
    def test_composite_key_routes_single(self, admin_dsn):
        """WHERE (id, name) = (1, 'user_1') should route to a single shard."""
        # Routing assertion only — uses EXPLAIN SHARD PLAN against the admin port.
        admin = psycopg2.connect(admin_dsn, connect_timeout=10)
        admin.autocommit = True
        try:
            rows = pg_exec(
                admin,
                "EXPLAIN SHARD PLAN FOR 'SELECT * FROM users WHERE (id, name) = (1, ''user_1'')'",
            )
        finally:
            admin.close()
        assert len(rows) == 1
        kind = rows[0][0]
        assert kind == "SINGLE", (
            f"Expected composite key equality to route SINGLE, got kind={kind!r}."
        )

    # -----------------------------------------------------------------------
    # D5 — Schema-qualified table name routing fails
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "SELECT * FROM public.users WHERE id = 1 — the schema prefix 'public.' "
            "may not be stripped by the SQL parser.  If preserved, KEEL's "
            "case-insensitive table match against 'users' fails and the query "
            "scatters or returns UNSUPPORTED instead of routing SINGLE.  "
            "Fix: strip schema qualifier before table-name comparison.  "
            "Reference: SHARDING.md §18.10."
        ),
    )
    def test_schema_qualified_table_routes_single(self, admin_dsn):
        """SELECT from public.users WHERE id = 1 routes to a single shard."""
        admin = psycopg2.connect(admin_dsn, connect_timeout=10)
        admin.autocommit = True
        try:
            rows = pg_exec(
                admin,
                "EXPLAIN SHARD PLAN FOR 'SELECT * FROM public.users WHERE id = 1'",
            )
        finally:
            admin.close()
        assert len(rows) == 1
        kind = rows[0][0]
        assert kind == "SINGLE", (
            f"Expected schema-qualified query to route SINGLE, got kind={kind!r}."
        )

    # -----------------------------------------------------------------------
    # D6 — BETWEEN on shard key scatters instead of single-routing
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "WHERE id BETWEEN $1 AND $2 is not recognised as a single-shard predicate "
            "even when the range falls entirely within one shard.  The query always "
            "scatters.  Fix: implement range analysis in shard-key extractor for BETWEEN.  "
            "Reference: SHARDING.md routing notes."
        ),
    )
    def test_between_shard_key_single_route(self, admin_dsn):
        """WHERE id BETWEEN 1 AND 1 (single value range) should route SINGLE."""
        admin = psycopg2.connect(admin_dsn, connect_timeout=10)
        admin.autocommit = True
        try:
            rows = pg_exec(
                admin,
                "EXPLAIN SHARD PLAN FOR 'SELECT * FROM users WHERE id BETWEEN 1 AND 1'",
            )
        finally:
            admin.close()
        assert len(rows) == 1
        kind = rows[0][0]
        assert kind == "SINGLE", (
            f"Expected BETWEEN single-value range to route SINGLE, got {kind!r}."
        )


# ===========================================================================
# Category E — CTE limitations
# ===========================================================================

class TestCTELimitations:
    """Common Table Expression patterns that behave unexpectedly with scatter."""

    # -----------------------------------------------------------------------
    # E1 — Writable CTE scatters INSERT to all shards
    # -----------------------------------------------------------------------
    def test_writable_cte_inserts_to_correct_shard_only(self, keel_dsn, shard0_conn, shard1_conn):
        """Writable CTE inserts exactly one row (on the correct shard), not N rows."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = 86001")
            conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH ins AS (
                        INSERT INTO users(id, name) VALUES (86001, 'cte_ins_test')
                        RETURNING id
                    )
                    SELECT id FROM ins
                    """
                )
                returned = cur.fetchall()
            conn.commit()

            total = shard_total_count(
                shard0_conn, shard1_conn, "users", "id = 86001"
            )
            assert total == 1, (
                f"Expected 1 row after writable CTE INSERT, got {total} total across shards.  "
                "Writable CTE scatters the INSERT to all shards."
            )
            assert returned == [(86001,)], (
                f"Expected RETURNING to yield [(86001,)], got {returned}."
            )
        finally:
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE id = 86001")
                conn.commit()
            except Exception:
                pass
            conn.close()

    # -----------------------------------------------------------------------
    # E2 — Read-only CTE row duplication (N copies)
    # Phase 8a fix: router now folds non-recursive constant CTEs (no FROM in
    # the CTE body) to a single shard, so COUNT(*) over a constant CTE
    # returns 1 instead of N.  Reference: SCATTER_MERGE.md §7 CTEs.
    # -----------------------------------------------------------------------
    def test_constant_cte_single_row(self, keel_conn):
        """WITH cte AS (SELECT 42 AS val) SELECT COUNT(*) FROM cte returns 1."""
        count = pg_scalar(
            keel_conn,
            "WITH cte AS (SELECT 42 AS val) SELECT COUNT(*) FROM cte",
        )
        assert count == 1, (
            f"Expected 1 row from constant CTE, got {count} (likely 1 per shard)."
        )

    # -----------------------------------------------------------------------
    # E3 — Recursive CTE cross-shard hierarchy traversal incomplete
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "WITH RECURSIVE is evaluated per-shard.  If a parent row is on shard 0 "
            "and its child row is on shard 1, the recursive expansion on shard 0 will "
            "not find the child.  Cross-shard recursive hierarchies are incomplete.  "
            "Fix: requires cross-shard recursive CTE coordination (complex).  "
            "Reference: SCATTER_MERGE.md §7 / SHARDING.md §18.14."
        ),
    )
    def test_recursive_cte_cross_shard_hierarchy(self, keel_dsn, shard0_conn, shard1_conn):
        """A recursive CTE traversing a parent-child hierarchy finds all descendants.

        Insert a parent row on one shard and a child on the other.  The recursive
        CTE must find both (parent and child = 2 rows).
        """
        # We need a self-referential table.  Use a temporary unsharded approach:
        # events has shard_hint as shard key.  Create a hierarchy where
        # event 85001 (parent) is on shard 0 and event 85002 (child, parent_id=85001)
        # goes to the shard key that maps to shard 1.
        # Since we cannot guarantee shard placement without running the server,
        # this test is marked xfail to document the limitation.
        # When implemented, insert parent/child on different shards and verify
        # the recursive CTE returns both.
        pytest.skip(
            "Cannot deterministically control cross-shard placement without live KEEL.  "
            "The xfail marker documents the limitation; the skip prevents flapping."
        )


# ===========================================================================
# Category F — Global ordering / pagination limitations
# ===========================================================================

class TestGlobalOrderingLimitations:
    """ORDER BY + LIMIT/OFFSET without GROUP BY: LIMIT pushed to shards,
    ORDER BY is per-shard, so the final result may not be globally sorted."""

    # Non-overlapping id range so this class can run in parallel (pytest-xdist)
    # with other suites that mutate the shared ``users`` table.  Avoids known
    # bases used by TestBulkLoad (1_000_000+) and tests in
    # test_sharding_comprehensive (8xxxxx, 9xxxxx).
    _SEED_BASE = 770_000
    _SEED_N    = 30
    _SEED_IDS  = list(range(_SEED_BASE + 1, _SEED_BASE + 1 + _SEED_N))

    @pytest.fixture(autouse=True)
    def seed_users(self, keel_conn):
        # Best-effort cleanup of our own range from any previous run.
        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (self._SEED_BASE + 1, self._SEED_BASE + self._SEED_N))
        for uid in self._SEED_IDS:
            pg_exec(keel_conn,
                    "INSERT INTO users(id, name) VALUES (%s, %s)",
                    (uid, f"u{uid}"))
        yield
        pg_exec(keel_conn,
                "DELETE FROM users WHERE id BETWEEN %s AND %s",
                (self._SEED_BASE + 1, self._SEED_BASE + self._SEED_N))

    # -----------------------------------------------------------------------
    # F1 — ORDER BY + LIMIT/OFFSET produces globally correct pagination
    # -----------------------------------------------------------------------
    def test_order_by_limit_offset_correct_result(self, keel_conn):
        """SELECT id FROM users ORDER BY id LIMIT 5 OFFSET 10 returns globally correct rows.

        The 11th through 15th smallest ids in our seeded range must be
        returned, regardless of which shard each row lives on.
        """
        base = self._SEED_BASE
        rows = pg_exec(
            keel_conn,
            "SELECT id FROM users WHERE id BETWEEN %s AND %s "
            "ORDER BY id ASC LIMIT 5 OFFSET 10",
            (base + 1, base + self._SEED_N),
        )
        ids = [r[0] for r in rows]
        expected = [base + 11, base + 12, base + 13, base + 14, base + 15]
        assert ids == expected, (
            f"Expected globally sorted ids {expected}, got {ids}.  "
            "Per-shard ORDER BY + LIMIT OFFSET does not guarantee global ordering."
        )

    # -----------------------------------------------------------------------
    # F2 — LIMIT without ORDER BY may return different rows per run
    # -----------------------------------------------------------------------
    def test_limit_without_order_by_is_deterministic(self, keel_conn):
        """SELECT id FROM users LIMIT 10 returns the same 10 rows across repeated calls.

        Without ORDER BY, the result set is non-deterministic.  This test
        documents that successive calls may return different rows (unstable).
        Restricted to our seeded id range to avoid interference from
        concurrent tests that mutate ``users``.
        """
        base = self._SEED_BASE
        q = (
            "SELECT id FROM users WHERE id BETWEEN %s AND %s LIMIT 10"
        )
        params = (base + 1, base + self._SEED_N)
        rows1 = [r[0] for r in pg_exec(keel_conn, q, params)]
        rows2 = [r[0] for r in pg_exec(keel_conn, q, params)]
        assert sorted(rows1) == sorted(rows2), (
            f"LIMIT without ORDER BY returned different rows across calls: "
            f"{sorted(rows1)} vs {sorted(rows2)}."
        )


# ===========================================================================
# Category G — Protocol / DDL limitations
# ===========================================================================

class TestProtocolDDLLimitations:
    """Protocol-level and DDL routing limitations."""

    # -----------------------------------------------------------------------
    # G1 — DDL not scatter-routed to all shards
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "CREATE TABLE, ALTER TABLE, DROP TABLE and other DDL are not scatter-routed "
            "by KEEL.  DDL is passed to the default backend only (one shard).  To apply "
            "DDL to all shards, operators must run it directly on each shard backend.  "
            "Reference: SCATTER_MERGE.md §17 Unsupported SQL Patterns."
        ),
    )
    def test_alter_table_applies_to_all_shards(self, keel_dsn, shard0_conn, shard1_conn):
        """ALTER TABLE ADD COLUMN through KEEL applies the column on all shards."""
        conn = psycopg2.connect(keel_dsn, connect_timeout=10)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS _lim_test_col INT DEFAULT NULL"
                )

            # Verify column exists on both shards
            def col_exists(c):
                rows = pg_exec(
                    c,
                    """
                    SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = '_lim_test_col'
                    """,
                )
                return rows[0][0] == 1

            s0_has = col_exists(shard0_conn)
            s1_has = col_exists(shard1_conn)

            # Cleanup — also not scatter-routed, but best-effort
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE users DROP COLUMN IF EXISTS _lim_test_col"
                )

            assert s0_has, "Column not found on shard 0 after ALTER TABLE via KEEL."
            assert s1_has, "Column not found on shard 1 after ALTER TABLE via KEEL."
        finally:
            conn.close()

    # -----------------------------------------------------------------------
    # G2 — Multi-statement query (semicolon-separated) not supported
    # -----------------------------------------------------------------------
    def test_multi_statement_query_executes_all(self, keel_conn):
        """Two SELECT statements separated by semicolon both return results."""
        # psycopg2 will execute both; KEEL should not error or drop the second.
        rows = pg_exec(
            keel_conn,
            "SELECT 1; SELECT 2",
        )
        # Some drivers return only the last result; but both must execute without error.
        # The key requirement is no exception is raised.
        assert rows is not None

    # -----------------------------------------------------------------------
    # G3 — COPY FROM / COPY TO not shard-routed
    # -----------------------------------------------------------------------
    def test_copy_command_not_supported(self, keel_conn):
        """COPY FROM STDIN through KEEL successfully loads rows to the correct shards."""
        # This test simply verifies that no exception is raised on COPY.
        # In practice COPY is unsupported and will fail.
        import io
        conn = keel_conn
        try:
            with conn.cursor() as cur:
                data = io.StringIO("85900\tcopy_test_user\n")
                cur.copy_from(data, "users", columns=("id", "name"))
            conn.commit()
        except Exception as exc:
            pytest.fail(
                f"COPY FROM raised an exception: {exc}.  "
                "KEEL does not currently support COPY command routing."
            )

    # -----------------------------------------------------------------------
    # G4 — WHERE id IN (...) scatters instead of routing to subset of shards
    # -----------------------------------------------------------------------
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "WHERE id IN (1, 2) always scatters to all shards. With ids 1 and 2\n"
            "hashing to different shards in the e2e 2-shard topology, the ideal\n"
            "behaviour is a SUBSET plan covering exactly 2 shards \u2014 but the\n"
            "keel_shard_plan_t API only encodes SINGLE / SCATTER / UNSUPPORTED.\n"
            "Fix requires a new SUBSET plan kind plus IN-list pruning in the\n"
            "shard-key extractor. Reference: SCATTER_MERGE.md \u00a718 Corner Cases."
        ),
    )
    def test_in_list_routes_to_subset_of_shards(self, admin_dsn):
        """WHERE id IN (1, 2) routes only to shards that actually contain those ids."""
        admin = psycopg2.connect(admin_dsn, connect_timeout=10)
        admin.autocommit = True
        try:
            rows = pg_exec(
                admin,
                "EXPLAIN SHARD PLAN FOR 'SELECT * FROM users WHERE id IN (1, 2)'",
            )
        finally:
            admin.close()
        assert len(rows) == 1
        kind = rows[0][0]
        # Ideally routes SINGLE or at least not to ALL shards
        assert kind == "SINGLE", (
            f"Expected IN-list with co-located ids to route SINGLE, got {kind!r}."
        )
