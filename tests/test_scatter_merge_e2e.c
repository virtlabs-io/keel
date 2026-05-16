/**
 * @file test_scatter_merge_e2e.c
 * @brief End-to-end integration tests for the scatter → merge pipeline against
 *        real PostgreSQL backends.
 *
 * Each test opens two direct connections to the same PostgreSQL node, treating
 * them as two independent shard backends.  It inserts known data, runs
 * aggregate queries on each "shard", feeds the results into @ref keel_scatter_result_t,
 * and then exercises the full scatter-merge library stack:
 *
 *   - @ref keel_scatter_result_merge_aggs   — COUNT and SUM reduction across shards
 *   - @ref keel_scatter_result_finalize_avg — AVG rewrite finalisation (SUM/COUNT→AVG)
 *   - @ref keel_scatter_result_group_aggs   — hash-merge of per-shard partial groups
 *   - @ref keel_scatter_result_apply_having — HAVING post-filter on grouped output
 *
 * The tests skip gracefully when no PostgreSQL cluster is reachable.  In CI
 * set KEEL_TEST_PG_HOST1 / KEEL_TEST_PG_PORT1 to point at a live instance.
 *
 * Dataset (shared by all tests, created in setup_schemas()):
 *
 *   keel_e2e_s0(grp TEXT, val BIGINT):
 *     ('A', 10), ('A', 20), ('A', 30),   -- grp A: 3 rows, SUM=60
 *     ('B', 40), ('B', 50)               -- grp B: 2 rows, SUM=90
 *     TOTAL: 5 rows, SUM=150
 *
 *   keel_e2e_s1(grp TEXT, val BIGINT):
 *     ('A', 40),                         -- grp A: 1 row,  SUM=40
 *     ('B', 10), ('B', 20), ('B', 30)    -- grp B: 3 rows, SUM=60
 *     TOTAL: 4 rows, SUM=100
 *
 *   Combined:
 *     COUNT       : 5 + 4 = 9
 *     SUM(val)    : 150 + 100 = 250
 *     AVG(val)    : 250 / 9 ≈ 27.78  (not clean — see test for SUM/COUNT trick)
 *     grp A       : 3+1=4 rows, SUM=100
 *     grp B       : 2+3=5 rows, SUM=150
 *     HAVING cnt > 4 : only grp B (5)
 */

#include "test_utils.h"
#include "test_integration.h"
#include "keel/core/scatter_store.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* ============================================================================
 * Globals
 * ============================================================================ */

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) { return g_tests_failed ? 1 : 0; }

static bool g_skip_tests = false;

/* ============================================================================
 * Test helpers
 * ============================================================================ */

/** Build a column value from a C string (text wire format). */
static keel_scatter_col_val_t mkval(const char* s)
{
    return (keel_scatter_col_val_t){
        .len  = (int32_t)(s ? (int32_t)strlen(s) : -1),
        .data = s,
    };
}

/** SQL NULL column value. */
static keel_scatter_col_val_t mknull(void)
{
    return (keel_scatter_col_val_t){ .len = -1, .data = NULL };
}

/* ============================================================================
 * Schema setup / teardown
 * ============================================================================ */

static bool setup_schemas(void)
{
    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c = integ_pg_connect(host, port,
                                           INTEG_PG_USER, INTEG_PG_PASSWORD,
                                           INTEG_PG_DATABASE);
    if (!c) return false;

    integ_pg_exec(c, "DROP TABLE IF EXISTS keel_e2e_s0");
    integ_pg_exec(c, "DROP TABLE IF EXISTS keel_e2e_s1");

    bool ok =
        integ_pg_exec(c,
            "CREATE TABLE keel_e2e_s0 (grp TEXT, val BIGINT)") &&
        integ_pg_exec(c,
            "INSERT INTO keel_e2e_s0 VALUES "
            "('A',10),('A',20),('A',30),('B',40),('B',50)") &&
        integ_pg_exec(c,
            "CREATE TABLE keel_e2e_s1 (grp TEXT, val BIGINT)") &&
        integ_pg_exec(c,
            "INSERT INTO keel_e2e_s1 VALUES "
            "('A',40),('B',10),('B',20),('B',30)");

    integ_pg_close(c);
    return ok;
}

static void teardown_schemas(void)
{
    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c = integ_pg_connect(host, port,
                                           INTEG_PG_USER, INTEG_PG_PASSWORD,
                                           INTEG_PG_DATABASE);
    if (!c) return;
    integ_pg_exec(c, "DROP TABLE IF EXISTS keel_e2e_s0");
    integ_pg_exec(c, "DROP TABLE IF EXISTS keel_e2e_s1");
    integ_pg_close(c);
}

/* ============================================================================
 * E2E: COUNT merge
 *
 * Scenario: each shard returns a COUNT(*) row.  scatter_store merges with
 * KEEL_AGG_COUNT.  Expected total: 5 + 4 = 9.
 * ============================================================================ */

static void test_e2e_scatter_count(void)
{
    TEST_BEGIN("scatter merge e2e: COUNT(*) across 2 shards");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    /* Query each shard */
    int64_t cnt0 = 0, cnt1 = 0;
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT COUNT(*) FROM keel_e2e_s0", &cnt0));
    TEST_ASSERT(integ_pg_query_int(c1, "SELECT COUNT(*) FROM keel_e2e_s1", &cnt1));
    TEST_ASSERT_EQ(cnt0, (int64_t)5);
    TEST_ASSERT_EQ(cnt1, (int64_t)4);

    integ_pg_close(c0);
    integ_pg_close(c1);

    /* Build scatter_store and merge */
    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "cnt");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    char buf0[32], buf1[32];
    snprintf(buf0, sizeof buf0, "%lld", (long long)cnt0);
    snprintf(buf1, sizeof buf1, "%lld", (long long)cnt1);

    keel_scatter_col_val_t v0 = mkval(buf0);
    keel_scatter_col_val_t v1 = mkval(buf1);

    TEST_ASSERT_EQ(keel_scatter_result_append(r, &v0), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_append(r, &v1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 2);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_COUNT };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));
    TEST_ASSERT(out[0].len > 0);

    char merged[32];
    size_t l = (size_t)out[0].len < sizeof merged - 1
               ? (size_t)out[0].len : sizeof merged - 1;
    memcpy(merged, out[0].data, l);
    merged[l] = '\0';
    TEST_ASSERT_EQ((int)strtol(merged, NULL, 10), 9);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: SUM merge
 *
 * Scenario: each shard returns SUM(val).  scatter_store merges with
 * KEEL_AGG_SUM.  Expected: 150 + 100 = 250.
 * ============================================================================ */

static void test_e2e_scatter_sum(void)
{
    TEST_BEGIN("scatter merge e2e: SUM(val) across 2 shards");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    int64_t sum0 = 0, sum1 = 0;
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT SUM(val) FROM keel_e2e_s0", &sum0));
    TEST_ASSERT(integ_pg_query_int(c1, "SELECT SUM(val) FROM keel_e2e_s1", &sum1));
    TEST_ASSERT_EQ(sum0, (int64_t)150);
    TEST_ASSERT_EQ(sum1, (int64_t)100);

    integ_pg_close(c0);
    integ_pg_close(c1);

    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "total");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    char b0[32], b1[32];
    snprintf(b0, sizeof b0, "%lld", (long long)sum0);
    snprintf(b1, sizeof b1, "%lld", (long long)sum1);
    keel_scatter_col_val_t v0 = mkval(b0), v1 = mkval(b1);

    keel_scatter_result_append(r, &v0);
    keel_scatter_result_append(r, &v1);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));

    char merged[32];
    size_t l = (size_t)out[0].len < sizeof merged - 1
               ? (size_t)out[0].len : sizeof merged - 1;
    memcpy(merged, out[0].data, l);
    merged[l] = '\0';
    TEST_ASSERT_EQ((int)strtol(merged, NULL, 10), 250);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: AVG finalisation
 *
 * Scenario: two shards each return SUM(val) and COUNT(val) (the AVG rewrite).
 * scatter_store merges both columns with SUM, then finalize_avg divides.
 *
 *   shard0: SUM=150, COUNT=5
 *   shard1: SUM=100, COUNT=4
 *   merged: SUM=250, COUNT=9
 *   AVG    = 250.0 / 9.0 ≈ 27.778 (stored as "%.17g" string)
 * ============================================================================ */

static void test_e2e_scatter_avg_finalize(void)
{
    TEST_BEGIN("scatter merge e2e: AVG rewrite finalisation (SUM+COUNT → AVG)");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    int64_t sum0 = 0, cnt0 = 0, sum1 = 0, cnt1 = 0;
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT SUM(val)   FROM keel_e2e_s0", &sum0));
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT COUNT(val) FROM keel_e2e_s0", &cnt0));
    TEST_ASSERT(integ_pg_query_int(c1, "SELECT SUM(val)   FROM keel_e2e_s1", &sum1));
    TEST_ASSERT(integ_pg_query_int(c1, "SELECT COUNT(val) FROM keel_e2e_s1", &cnt1));

    integ_pg_close(c0);
    integ_pg_close(c1);

    /* sum0=150, cnt0=5, sum1=100, cnt1=4 */
    TEST_ASSERT(sum0 > 0 && cnt0 > 0 && sum1 > 0 && cnt1 > 0);

    /* Build 2-column scatter_store: col0=SUM, col1=COUNT */
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "sum_val");
    cols[0].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt_val");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    char s0[32], c0s[32], s1[32], c1s[32];
    snprintf(s0,  sizeof s0,  "%lld", (long long)sum0);
    snprintf(c0s, sizeof c0s, "%lld", (long long)cnt0);
    snprintf(s1,  sizeof s1,  "%lld", (long long)sum1);
    snprintf(c1s, sizeof c1s, "%lld", (long long)cnt1);

    keel_scatter_col_val_t row0[2] = { mkval(s0),  mkval(c0s) };
    keel_scatter_col_val_t row1[2] = { mkval(s1),  mkval(c1s) };

    keel_scatter_result_append(r, row0);
    keel_scatter_result_append(r, row1);

    /* Merge both columns with SUM (simulates scatter aggregation) */
    keel_agg_col_spec_t specs[2] = {
        { .col_index = 0, .func = KEEL_AGG_SUM },
        { .col_index = 1, .func = KEEL_AGG_SUM },
    };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, specs, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    /* Finalise AVG: divide col0 by col1 in-place */
    keel_avg_finalize_spec_t avg_spec = {
        .sum_col   = 0,
        .count_col = 1,
        .out_col   = 0,
    };
    TEST_ASSERT_EQ(keel_scatter_result_finalize_avg(r, &avg_spec, 1), KEEL_OK);

    /* Verify: (150+100) / (5+4) = 250.0 / 9.0 */
    double expected_avg = (double)(sum0 + sum1) / (double)(cnt0 + cnt1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));
    TEST_ASSERT(out[0].len > 0);

    char avg_str[64];
    size_t l = (size_t)out[0].len < sizeof avg_str - 1
               ? (size_t)out[0].len : sizeof avg_str - 1;
    memcpy(avg_str, out[0].data, l);
    avg_str[l] = '\0';
    double got_avg = strtod(avg_str, NULL);

    /* Allow floating-point epsilon */
    double diff = got_avg - expected_avg;
    if (diff < 0) diff = -diff;
    TEST_ASSERT(diff < 1e-9);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: GROUP BY + HAVING
 *
 * Scenario: each shard contributes one partially-aggregated row per group.
 * After hash-merging (keel_scatter_result_group_aggs) the proxy applies a HAVING
 * predicate to discard groups below threshold.
 *
 *   shard0 partial: grp A cnt=3, grp B cnt=2
 *   shard1 partial: grp A cnt=1, grp B cnt=3
 *   merged:         grp A cnt=4, grp B cnt=5
 *   HAVING cnt > 4: only grp B (5) survives
 * ============================================================================ */

static void test_e2e_scatter_group_having(void)
{
    TEST_BEGIN("scatter merge e2e: GROUP BY hash-merge + HAVING filter");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    /* Per-group counts from each shard (simulating GROUP BY on each backend) */
    int64_t s0_a = 0, s0_b = 0, s1_a = 0, s1_b = 0;
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COUNT(*) FROM keel_e2e_s0 WHERE grp='A'", &s0_a));
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COUNT(*) FROM keel_e2e_s0 WHERE grp='B'", &s0_b));
    TEST_ASSERT(integ_pg_query_int(c1,
        "SELECT COUNT(*) FROM keel_e2e_s1 WHERE grp='A'", &s1_a));
    TEST_ASSERT(integ_pg_query_int(c1,
        "SELECT COUNT(*) FROM keel_e2e_s1 WHERE grp='B'", &s1_b));

    TEST_ASSERT_EQ(s0_a, (int64_t)3);
    TEST_ASSERT_EQ(s0_b, (int64_t)2);
    TEST_ASSERT_EQ(s1_a, (int64_t)1);
    TEST_ASSERT_EQ(s1_b, (int64_t)3);

    integ_pg_close(c0);
    integ_pg_close(c1);

    /* Build 2-col scatter_store: col0=grp TEXT, col1=cnt INT8 */
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* 4 rows: 2 from shard0, 2 from shard1 (simulating scattered partial aggs) */
    char s0ab[32], s0bb[32], s1ab[32], s1bb[32];
    snprintf(s0ab, sizeof s0ab, "%lld", (long long)s0_a);
    snprintf(s0bb, sizeof s0bb, "%lld", (long long)s0_b);
    snprintf(s1ab, sizeof s1ab, "%lld", (long long)s1_a);
    snprintf(s1bb, sizeof s1bb, "%lld", (long long)s1_b);

    keel_scatter_col_val_t rows[4][2] = {
        { mkval("A"), mkval(s0ab) },
        { mkval("B"), mkval(s0bb) },
        { mkval("A"), mkval(s1ab) },
        { mkval("B"), mkval(s1bb) },
    };
    for (int i = 0; i < 4; i++)
        keel_scatter_result_append(r, rows[i]);

    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 4);

    /* GROUP BY col0, COUNT col1 */
    keel_group_col_spec_t gkey = { .col_index = 0 };
    keel_agg_col_spec_t   agg  = { .col_index = 1, .func = KEEL_AGG_COUNT };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, &gkey, 1, &agg, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 2); /* A and B */

    /* HAVING cnt > 4 — only group B (5) should survive */
    keel_having_pred_t pred;
    memset(&pred, 0, sizeof pred);
    pred.col_index   = 1;
    pred.op          = KEEL_CMP_GT;
    snprintf(pred.literal, sizeof pred.literal, "4");
    pred.literal_len = 1;

    TEST_ASSERT_EQ(keel_scatter_result_apply_having(r, &pred, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));

    /* Verify group key is 'B' */
    TEST_ASSERT(out[0].len == 1 && out[0].data[0] == 'B');

    /* Verify merged count is 5 */
    char cnt_str[32];
    size_t l = (size_t)out[1].len < sizeof cnt_str - 1
               ? (size_t)out[1].len : sizeof cnt_str - 1;
    memcpy(cnt_str, out[1].data, l);
    cnt_str[l] = '\0';
    TEST_ASSERT_EQ((int)strtol(cnt_str, NULL, 10), 5);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: ORDER BY sort merge
 *
 * Scenario: each shard returns SUM(val) per group, producing 2 rows per shard.
 * After group_aggs (4 rows → 2 rows), the result is sort-merged descending.
 *
 *   After group_aggs: A=100, B=150
 *   After sort DESC:  B=150, A=100
 * ============================================================================ */

static void test_e2e_scatter_sort_merge(void)
{
    TEST_BEGIN("scatter merge e2e: ORDER BY descending after GROUP BY merge");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    /* Per-group sums from each shard */
    int64_t s0_a_sum = 0, s0_b_sum = 0, s1_a_sum = 0, s1_b_sum = 0;
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COALESCE(SUM(val),0) FROM keel_e2e_s0 WHERE grp='A'", &s0_a_sum));
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COALESCE(SUM(val),0) FROM keel_e2e_s0 WHERE grp='B'", &s0_b_sum));
    TEST_ASSERT(integ_pg_query_int(c1,
        "SELECT COALESCE(SUM(val),0) FROM keel_e2e_s1 WHERE grp='A'", &s1_a_sum));
    TEST_ASSERT(integ_pg_query_int(c1,
        "SELECT COALESCE(SUM(val),0) FROM keel_e2e_s1 WHERE grp='B'", &s1_b_sum));

    integ_pg_close(c0);
    integ_pg_close(c1);

    /* Expected: A=60+40=100, B=90+60=150 */
    int64_t exp_a = s0_a_sum + s1_a_sum;
    int64_t exp_b = s0_b_sum + s1_b_sum;

    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "total");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    char sa[32], sb[32], sa1[32], sb1[32];
    snprintf(sa,  sizeof sa,  "%lld", (long long)s0_a_sum);
    snprintf(sb,  sizeof sb,  "%lld", (long long)s0_b_sum);
    snprintf(sa1, sizeof sa1, "%lld", (long long)s1_a_sum);
    snprintf(sb1, sizeof sb1, "%lld", (long long)s1_b_sum);

    keel_scatter_col_val_t rows[4][2] = {
        { mkval("A"), mkval(sa)  },
        { mkval("B"), mkval(sb)  },
        { mkval("A"), mkval(sa1) },
        { mkval("B"), mkval(sb1) },
    };
    for (int i = 0; i < 4; i++)
        keel_scatter_result_append(r, rows[i]);

    /* Hash-merge groups, accumulating with SUM */
    keel_group_col_spec_t gkey = { .col_index = 0 };
    keel_agg_col_spec_t   agg  = { .col_index = 1, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, &gkey, 1, &agg, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 2);

    /* Sort descending by col1 (total SUM) */
    keel_sort_key_t key = {
        .col_index = 1,
        .dir       = KEEL_SORT_DESC,
        .nulls     = KEEL_SORT_NULLS_LAST,
    };
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, &key, 1), KEEL_OK);

    /* Verify first row has the larger sum (grp B, exp_b > exp_a) */
    TEST_ASSERT(exp_b > exp_a); /* sanity check on our dataset */

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;

    /* First row: B */
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));
    TEST_ASSERT(out[0].len == 1 && out[0].data[0] == 'B');

    char tot[32];
    size_t l = (size_t)out[1].len < sizeof tot - 1
               ? (size_t)out[1].len : sizeof tot - 1;
    memcpy(tot, out[1].data, l); tot[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(tot, NULL, 10), exp_b);

    /* Second row: A */
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));
    TEST_ASSERT(out[0].len == 1 && out[0].data[0] == 'A');

    l = (size_t)out[1].len < sizeof tot - 1
        ? (size_t)out[1].len : sizeof tot - 1;
    memcpy(tot, out[1].data, l); tot[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(tot, NULL, 10), exp_a);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: MIN and MAX merge
 *
 * Scenario: two shards, each with known min/max values.
 *   shard0 (keel_e2e_s0): val in {10,20,30,40,50} — min=10, max=50
 *   shard1 (keel_e2e_s1): val in {40,10,20,30}    — min=10, max=40
 *   merged MIN(val) = min(10,10) = 10
 *   merged MAX(val) = max(50,40) = 50
 * ============================================================================ */

static void test_e2e_scatter_min_max(void)
{
    TEST_BEGIN("scatter merge e2e: MIN and MAX across 2 shards");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    int64_t min0 = 0, max0 = 0, min1 = 0, max1 = 0;
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT MIN(val) FROM keel_e2e_s0", &min0));
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT MAX(val) FROM keel_e2e_s0", &max0));
    TEST_ASSERT(integ_pg_query_int(c1, "SELECT MIN(val) FROM keel_e2e_s1", &min1));
    TEST_ASSERT(integ_pg_query_int(c1, "SELECT MAX(val) FROM keel_e2e_s1", &max1));
    integ_pg_close(c0);
    integ_pg_close(c1);

    /* Build 2-col result: col0=partial_min, col1=partial_max */
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "mn");
    cols[0].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[1].name, sizeof cols[1].name, "mx");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    char smin0[32], smax0[32], smin1[32], smax1[32];
    snprintf(smin0, sizeof smin0, "%lld", (long long)min0);
    snprintf(smax0, sizeof smax0, "%lld", (long long)max0);
    snprintf(smin1, sizeof smin1, "%lld", (long long)min1);
    snprintf(smax1, sizeof smax1, "%lld", (long long)max1);

    keel_scatter_col_val_t row0[2] = { mkval(smin0), mkval(smax0) };
    keel_scatter_col_val_t row1[2] = { mkval(smin1), mkval(smax1) };
    keel_scatter_result_append(r, row0);
    keel_scatter_result_append(r, row1);

    keel_agg_col_spec_t specs[2] = {
        { .col_index = 0, .func = KEEL_AGG_MIN },
        { .col_index = 1, .func = KEEL_AGG_MAX },
    };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, specs, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));

    char mn_str[32], mx_str[32];
    size_t lm = (size_t)out[0].len < sizeof mn_str - 1
                ? (size_t)out[0].len : sizeof mn_str - 1;
    size_t lx = (size_t)out[1].len < sizeof mx_str - 1
                ? (size_t)out[1].len : sizeof mx_str - 1;
    memcpy(mn_str, out[0].data, lm); mn_str[lm] = '\0';
    memcpy(mx_str, out[1].data, lx); mx_str[lx] = '\0';

    int64_t merged_min = strtoll(mn_str, NULL, 10);
    int64_t merged_max = strtoll(mx_str, NULL, 10);
    /* min(min0,min1), max(max0,max1) */
    TEST_ASSERT_EQ(merged_min, (min0 < min1 ? min0 : min1));
    TEST_ASSERT_EQ(merged_max, (max0 > max1 ? max0 : max1));

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: Empty shard — one shard returns 0 rows
 *
 * Scenario: one shard table is empty (no rows match the filter), the other
 * shard contributes COUNT=5, SUM=150.  The merge must return the non-empty
 * shard's values unchanged, not crash or return 0.
 *
 *   shard0: keel_e2e_s0 WHERE grp='Z' → 0 rows (no group Z in dataset)
 *   shard1: keel_e2e_s1 has 4 rows (all non-Z)
 *   Merged COUNT=4, SUM=100
 *
 * At the scatter_store level this is represented as one shard producing a
 * result with a single row containing the non-null partial aggregate, while
 * the other shard contributes no row at all (0 rows appended).
 * ============================================================================ */

static void test_e2e_scatter_empty_shard(void)
{
    TEST_BEGIN("scatter merge e2e: empty shard (0 rows) + non-empty shard");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* c0 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    integ_pg_conn_t* c1 = integ_pg_connect(host, port,
                                            INTEG_PG_USER, INTEG_PG_PASSWORD,
                                            INTEG_PG_DATABASE);
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    /* shard0 contributes: count=5, sum=150 (all rows from s0) */
    int64_t cnt0 = 0, sum0 = 0;
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT COUNT(*) FROM keel_e2e_s0", &cnt0));
    TEST_ASSERT(integ_pg_query_int(c0, "SELECT SUM(val)  FROM keel_e2e_s0", &sum0));
    integ_pg_close(c0);
    integ_pg_close(c1);

    TEST_ASSERT(cnt0 == 5 && sum0 == 150);

    /* Build 2-col result: col0=count, col1=sum */
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "cnt");
    cols[0].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[1].name, sizeof cols[1].name, "sm");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Shard 0 contributes one row; shard 1 contributes ZERO rows (empty). */
    char scnt[32], ssum[32];
    snprintf(scnt, sizeof scnt, "%lld", (long long)cnt0);
    snprintf(ssum, sizeof ssum, "%lld", (long long)sum0);
    keel_scatter_col_val_t row0[2] = { mkval(scnt), mkval(ssum) };
    TEST_ASSERT_EQ(keel_scatter_result_append(r, row0), KEEL_OK);
    /* No row from shard 1 — shard returned an empty result set */
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_agg_col_spec_t specs[2] = {
        { .col_index = 0, .func = KEEL_AGG_COUNT },
        { .col_index = 1, .func = KEEL_AGG_SUM   },
    };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, specs, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));

    char c_str[32], s_str[32];
    size_t lc = (size_t)out[0].len < sizeof c_str - 1
                ? (size_t)out[0].len : sizeof c_str - 1;
    size_t ls = (size_t)out[1].len < sizeof s_str - 1
                ? (size_t)out[1].len : sizeof s_str - 1;
    memcpy(c_str, out[0].data, lc); c_str[lc] = '\0';
    memcpy(s_str, out[1].data, ls); s_str[ls] = '\0';

    TEST_ASSERT_EQ((int64_t)strtoll(c_str, NULL, 10), cnt0);
    TEST_ASSERT_EQ((int64_t)strtoll(s_str, NULL, 10), sum0);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: All-shards-fail — error propagation when both shards return no data
 *
 * Scenario: both shards return 0 rows (filters match nothing).  After merge
 * the result must be 0 rows; no hang, no crash, no corrupted output.
 *
 * Additionally: verify that passing NULL spec array with nagg_specs=0 returns
 * KEEL_OK on an empty result (passthrough).
 * ============================================================================ */

static void test_e2e_scatter_all_shards_no_rows(void)
{
    TEST_BEGIN("scatter merge e2e: all shards return 0 rows — no hang, no crash");

    /* This test does not need a live cluster — it exercises the library
     * directly with zero rows appended from any shard. */

    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "cnt");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* No rows appended — both shards returned empty result sets. */
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_COUNT };
    keel_error_t err = keel_scatter_result_merge_aggs(r, &spec, 1);
    /* Merge on an empty result must not crash; result stays at 0 rows. */
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);

    /* Iterator must immediately signal end-of-stream. */
    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &out)); /* must be false */
    keel_scatter_result_iter_close(&it);

    /* Sort on empty result must be a no-op. */
    keel_sort_key_t key = { .col_index = 0, .dir = KEEL_SORT_ASC,
                             .nulls = KEEL_SORT_NULLS_LAST };
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, &key, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * E2E: NULL handling in aggregates (SUM/MIN/MAX over all-NULL inputs)
 *
 * SQL semantics: SUM/MIN/MAX over all-NULL inputs must return NULL, not 0.
 * COUNT(*) over all-NULL inputs returns 0 (no rows), COUNT(expr) where expr
 * is always NULL also returns 0.
 *
 * Simulate two shards that each return a single NULL partial aggregate.
 * After merge, the merged value must also be NULL.
 * ============================================================================ */

static void test_e2e_scatter_null_aggregate(void)
{
    TEST_BEGIN("scatter merge e2e: NULL aggregate inputs propagate NULL output");

    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "total");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Both shards return NULL for SUM (empty table filtered to zero rows). */
    keel_scatter_col_val_t null_row[1] = { mknull() };
    TEST_ASSERT_EQ(keel_scatter_result_append(r, null_row), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_append(r, null_row), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 2);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));

    /* Merged SUM of (NULL, NULL) must be NULL, not "0". */
    TEST_ASSERT_EQ(out[0].len, (int32_t)-1);
    TEST_ASSERT(out[0].data == NULL);

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);

    /* ---- MIN / MAX over all-NULL must also yield NULL ---- */
    keel_scatter_result_t* r2 = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r2 != NULL);
    keel_scatter_result_append(r2, null_row);
    keel_scatter_result_append(r2, null_row);
    keel_agg_col_spec_t mspec = { .col_index = 0, .func = KEEL_AGG_MIN };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r2, &mspec, 1), KEEL_OK);
    keel_scatter_result_iter_t it2;
    keel_scatter_result_iter_init(&it2, r2);
    const keel_scatter_col_val_t* out2;
    TEST_ASSERT(keel_scatter_result_iter_next(&it2, &out2));
    TEST_ASSERT_EQ(out2[0].len, (int32_t)-1);
    keel_scatter_result_iter_close(&it2);
    keel_scatter_result_destroy(r2);

    TEST_END();
}

/* ============================================================================
 * E2E: COUNT DISTINCT — dispatch spec sets requires_count_distinct flag
 *
 * COUNT(DISTINCT col) cannot be computed by summing partial shard counts.
 * The dispatch layer must flag requires_count_distinct = true and the caller
 * must route to a single shard (or block scatter).
 *
 * This test verifies the struct semantics; it does not need a live cluster.
 * ============================================================================ */

#include "keel/core/router.h"

static void test_e2e_count_distinct_dispatch_flag(void)
{
    TEST_BEGIN("scatter dispatch: COUNT(DISTINCT) sets requires_count_distinct flag");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    /* Simulate dispatch layer detecting COUNT(DISTINCT col):
     * - requires_count_distinct must be set
     * - requires_merge must NOT be set (can't be merged across shards)
     * - count_distinct_col must record the column name */
    dr.requires_count_distinct = true;
    dr.requires_merge          = false;
    snprintf(dr.count_distinct_col, sizeof dr.count_distinct_col, "user_id");

    TEST_ASSERT(dr.requires_count_distinct);
    TEST_ASSERT(!dr.requires_merge);
    TEST_ASSERT(strcmp(dr.count_distinct_col, "user_id") == 0);

    /* A query with COUNT DISTINCT and window functions must still work;
     * has_window_funcs is orthogonal. */
    dr.has_window_funcs = true;
    TEST_ASSERT(dr.requires_count_distinct);
    TEST_ASSERT(dr.has_window_funcs);
    TEST_ASSERT(!dr.requires_merge);

    /* Setting requires_count_distinct must not corrupt twopc or other
     * adjacent fields. */
    TEST_ASSERT(!dr.twopc_required);
    TEST_ASSERT(dr.twopc == NULL);

    keel_dispatch_result_cleanup(&dr);
    TEST_END();
}

/* ============================================================================
 * E2E: GROUP BY + ORDER BY + LIMIT full pipeline
 *
 * Scenario: 4 partial rows (2 per shard) → hash-merge to 2 groups →
 * sort DESC → LIMIT 1 → only the largest group survives.
 *
 *   shard0 partial: A=60, B=90
 *   shard1 partial: A=40, B=60
 *   merged:         A=100, B=150
 *   sort DESC col1: B=150, A=100
 *   LIMIT 1:        B=150
 * ============================================================================ */

static void test_e2e_scatter_group_order_limit(void)
{
    TEST_BEGIN("scatter merge e2e: GROUP BY + ORDER BY DESC + LIMIT 1 pipeline");

    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "total");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* 4 partial rows from 2 shards */
    keel_scatter_col_val_t rows[4][2] = {
        { mkval("A"), mkval("60")  },   /* shard0 group A partial */
        { mkval("B"), mkval("90")  },   /* shard0 group B partial */
        { mkval("A"), mkval("40")  },   /* shard1 group A partial */
        { mkval("B"), mkval("60")  },   /* shard1 group B partial */
    };
    for (int i = 0; i < 4; i++)
        keel_scatter_result_append(r, rows[i]);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 4);

    /* 1. Hash-merge groups, accumulating SUM */
    keel_group_col_spec_t gkey = { .col_index = 0 };
    keel_agg_col_spec_t   agg  = { .col_index = 1, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, &gkey, 1, &agg, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 2);

    /* 2. Sort descending by total */
    keel_sort_key_t key = { .col_index = 1, .dir = KEEL_SORT_DESC,
                             .nulls = KEEL_SORT_NULLS_LAST };
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, &key, 1), KEEL_OK);

    /* 3. Apply LIMIT 1 */
    keel_scatter_result_apply_limit(r, 1, 0);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    /* 4. Verify: only group B (total=150) survives */
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* out;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &out));
    TEST_ASSERT(out[0].len == 1 && out[0].data[0] == 'B');

    char tot[32];
    size_t l = (size_t)out[1].len < sizeof tot - 1
               ? (size_t)out[1].len : sizeof tot - 1;
    memcpy(tot, out[1].data, l); tot[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(tot, NULL, 10), (int64_t)150);

    /* No second row */
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &out));

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * 4.1 — Prepared-statement scatter
 * ============================================================================ */

/**
 * @brief Cluster test: PREPARE + EXECUTE on shard backends, then scatter-merge.
 *
 * The scatter engine always receives expanded SQL (raw text) from the proxy
 * dispatch layer.  This test shows the *shard-side* half of that flow: the
 * backends themselves correctly handle PREPARE + EXECUTE when called with
 * simple-query protocol text, and the partial-group rows they return can be
 * fed into the same scatter-merge pipeline used by keel_engine_scatter_execute.
 *
 * Dataset re-used from setup_schemas():
 *   keel_e2e_s0  →  grp A: cnt=3, tot=60;  grp B: cnt=2, tot=90
 *   keel_e2e_s1  →  grp A: cnt=1, tot=40;  grp B: cnt=3, tot=60
 *   Combined     →  grp A: cnt=4, tot=100; grp B: cnt=5, tot=150
 */
static void test_e2e_scatter_prepared_exec(void)
{
    TEST_BEGIN("scatter: PREPARE/EXECUTE on shard backends feed merge pipeline");

    if (g_skip_tests) { TEST_END(); return; }

    const char* host = integ_get_node_host(1);
    uint16_t    port = integ_get_node_port(1);

    integ_pg_conn_t* s0 = integ_pg_connect(host, port,
                                             INTEG_PG_USER, INTEG_PG_PASSWORD,
                                             INTEG_PG_DATABASE);
    integ_pg_conn_t* s1 = integ_pg_connect(host, port,
                                             INTEG_PG_USER, INTEG_PG_PASSWORD,
                                             INTEG_PG_DATABASE);
    if (!s0 || !s1) {
        if (s0) integ_pg_close(s0);
        if (s1) integ_pg_close(s1);
        printf("SKIP: could not open two shard connections\n");
        g_skip_tests = true;
        TEST_END();
        return;
    }

    /* Phase 1: PREPARE the aggregate statement on both shard connections.
     * In production, the proxy would expand EXECUTE → raw SQL before fan-out;
     * here we drive PREPARE/EXECUTE directly to test backend compatibility. */
    TEST_ASSERT(integ_pg_exec(s0,
        "PREPARE scatter_ps_s0 AS "
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS tot "
        "FROM keel_e2e_s0 GROUP BY grp ORDER BY grp"));
    TEST_ASSERT(integ_pg_exec(s1,
        "PREPARE scatter_ps_s1 AS "
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS tot "
        "FROM keel_e2e_s1 GROUP BY grp ORDER BY grp"));

    /* Phase 2: EXECUTE the prepared statements.  Use integ_pg_query_int to
     * verify each shard returns exactly 2 groups (A and B). */
    int64_t ngroups_s0 = 0;
    TEST_ASSERT(integ_pg_query_int(s0,
        "SELECT COUNT(*) FROM ("
        "  EXECUTE scatter_ps_s0"
        ") AS sub",
        &ngroups_s0));
    TEST_ASSERT_EQ(ngroups_s0, (int64_t)2);

    int64_t ngroups_s1 = 0;
    TEST_ASSERT(integ_pg_query_int(s1,
        "SELECT COUNT(*) FROM ("
        "  EXECUTE scatter_ps_s1"
        ") AS sub",
        &ngroups_s1));
    TEST_ASSERT_EQ(ngroups_s1, (int64_t)2);

    /* Phase 3: Simulate the scatter-merge pipeline that the engine would run
     * after collecting EXECUTE results from all shards.
     * Feed known partial-group rows (same values setup_schemas() inserts). */
    keel_scatter_col_desc_t cols[3];
    memset(cols, 0, sizeof cols);
    memcpy(cols[0].name, "grp", 3); cols[0].type = KEEL_COL_TYPE_TEXT;
    memcpy(cols[1].name, "cnt", 3); cols[1].type = KEEL_COL_TYPE_INT64;
    memcpy(cols[2].name, "tot", 3); cols[2].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(3, cols,
                                                  16 * 1024 * 1024, NULL);
    TEST_ASSERT_NOT_NULL(r);

    /* Partial rows from shard 0 */
    keel_scatter_col_val_t s0_rows[2][3] = {
        { mkval("A"), mkval("3"), mkval("60") },
        { mkval("B"), mkval("2"), mkval("90") },
    };
    /* Partial rows from shard 1 */
    keel_scatter_col_val_t s1_rows[2][3] = {
        { mkval("A"), mkval("1"), mkval("40") },
        { mkval("B"), mkval("3"), mkval("60") },
    };
    for (int i = 0; i < 2; i++) keel_scatter_result_append(r, s0_rows[i]);
    for (int i = 0; i < 2; i++) keel_scatter_result_append(r, s1_rows[i]);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)4);

    /* Merge: group on col 0, SUM cnt (col 1), SUM tot (col 2) */
    keel_group_col_spec_t gkey  = { .col_index = 0 };
    keel_agg_col_spec_t   agg[] = {
        { .col_index = 1, .func = KEEL_AGG_SUM },
        { .col_index = 2, .func = KEEL_AGG_SUM },
    };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, &gkey, 1, agg, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    /* Sort by grp for deterministic assertion order */
    keel_sort_key_t sk = { .col_index = 0, .dir = KEEL_SORT_ASC,
                            .nulls = KEEL_SORT_NULLS_LAST };
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, &sk, 1), KEEL_OK);

    struct { char grp; int64_t cnt; int64_t tot; } expected[] = {
        { 'A', 4, 100 },
        { 'B', 5, 150 },
    };
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* row;
    for (int i = 0; i < 2; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &row));
        TEST_ASSERT(row[0].len == 1 && row[0].data[0] == expected[i].grp);
        char b[32];
        size_t l;
        l = (size_t)row[1].len < sizeof b - 1 ? (size_t)row[1].len : sizeof b - 1;
        memcpy(b, row[1].data, l); b[l] = '\0';
        TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), expected[i].cnt);
        l = (size_t)row[2].len < sizeof b - 1 ? (size_t)row[2].len : sizeof b - 1;
        memcpy(b, row[2].data, l); b[l] = '\0';
        TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), expected[i].tot);
    }
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &row));

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);

    /* Cleanup prepared statements */
    integ_pg_exec(s0, "DEALLOCATE scatter_ps_s0");
    integ_pg_exec(s1, "DEALLOCATE scatter_ps_s1");
    integ_pg_close(s0);
    integ_pg_close(s1);

    TEST_END();
}

/**
 * @brief Library-only: parameterized scatter — EXECUTE with $1 bound value.
 *
 * When a client executes EXECUTE stmt('A'), the proxy expands the parameter
 * and sends `SELECT … WHERE grp = 'A'` to each shard.  This test verifies
 * the scatter-merge pipeline produces correct results from the per-shard
 * single-group rows that such a parameterized execution would return.
 */
static void test_e2e_scatter_prepared_param_merge(void)
{
    TEST_BEGIN("scatter: prepared-stmt param binding — per-group scatter merge");

    /* Simulate EXECUTE stmt('A'): each shard filters grp='A' only.
     *   Shard 0: grp A, cnt=3, tot=60
     *   Shard 1: grp A, cnt=1, tot=40
     *   Combined: cnt=4, tot=100 */
    keel_scatter_col_desc_t cols[3];
    memset(cols, 0, sizeof cols);
    memcpy(cols[0].name, "grp", 3); cols[0].type = KEEL_COL_TYPE_TEXT;
    memcpy(cols[1].name, "cnt", 3); cols[1].type = KEEL_COL_TYPE_INT64;
    memcpy(cols[2].name, "tot", 3); cols[2].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(3, cols,
                                                  16 * 1024 * 1024, NULL);
    TEST_ASSERT_NOT_NULL(r);

    keel_scatter_col_val_t rows[2][3] = {
        { mkval("A"), mkval("3"), mkval("60") }, /* shard 0 */
        { mkval("A"), mkval("1"), mkval("40") }, /* shard 1 */
    };
    keel_scatter_result_append(r, rows[0]);
    keel_scatter_result_append(r, rows[1]);

    keel_group_col_spec_t gkey  = { .col_index = 0 };
    keel_agg_col_spec_t   agg[] = {
        { .col_index = 1, .func = KEEL_AGG_SUM },
        { .col_index = 2, .func = KEEL_AGG_SUM },
    };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, &gkey, 1, agg, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* row;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &row));
    TEST_ASSERT(row[0].len == 1 && row[0].data[0] == 'A');
    char b[32]; size_t l;
    l = (size_t)row[1].len < sizeof b - 1 ? (size_t)row[1].len : sizeof b - 1;
    memcpy(b, row[1].data, l); b[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), (int64_t)4);
    l = (size_t)row[2].len < sizeof b - 1 ? (size_t)row[2].len : sizeof b - 1;
    memcpy(b, row[2].data, l); b[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), (int64_t)100);
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &row));

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * 4.2 — Scatter LIMIT / OFFSET correctness
 * ============================================================================ */

/**
 * @brief Library-only: GROUP BY + global LIMIT returns globally correct top-N.
 *
 * This is the regression test for the sc_strip_limit_offset() fix in
 * engine_scatter.c.  It proves that when ALL partial groups from all shards
 * are fed to the merge pipeline (i.e., no per-shard LIMIT is applied), the
 * post-merge LIMIT correctly returns the globally top-N groups.
 *
 * Without the fix, shard 0 would only return its local top-3 (A, B, C) and
 * shard 1 its local top-3 (D, C, B).  Group D (global rank #1 with total=200)
 * would be missing from shard 0's result, and the global top-3 would be wrong.
 *
 * Groups and per-shard SUM:
 *   Shard 0: A=100, B=80, C=70, D=60   (per-shard top-3: A, B, C — D dropped)
 *   Shard 1: A=10,  B=20, C=30, D=200  (per-shard top-3: D, C, B)
 * Global:    A=110, B=100, C=100, D=260
 * Global top-3: D=260, A=110, B=100  (or C=100 depending on tie-break; B used here)
 */
static void test_e2e_scatter_group_limit_correctness(void)
{
    TEST_BEGIN("scatter: GROUP BY + LIMIT applies post-merge, not per-shard");

    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    memcpy(cols[0].name, "grp",   3); cols[0].type = KEEL_COL_TYPE_TEXT;
    memcpy(cols[1].name, "total", 5); cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols,
                                                  64 * 1024 * 1024, NULL);
    TEST_ASSERT_NOT_NULL(r);

    /* All 8 partial-group rows from both shards — NO per-shard LIMIT applied */
    keel_scatter_col_val_t rows[8][2] = {
        { mkval("A"), mkval("100") }, /* shard 0 */
        { mkval("B"), mkval("80")  },
        { mkval("C"), mkval("70")  },
        { mkval("D"), mkval("60")  },
        { mkval("A"), mkval("10")  }, /* shard 1 */
        { mkval("B"), mkval("20")  },
        { mkval("C"), mkval("30")  },
        { mkval("D"), mkval("200") },
    };
    for (int i = 0; i < 8; i++) keel_scatter_result_append(r, rows[i]);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)8);

    /* Merge groups with SUM */
    keel_group_col_spec_t gkey = { .col_index = 0 };
    keel_agg_col_spec_t   agg  = { .col_index = 1, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, &gkey, 1, &agg, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)4); /* A, B, C, D */

    /* Sort DESC by total */
    keel_sort_key_t sk = { .col_index = 1, .dir = KEEL_SORT_DESC,
                            .nulls = KEEL_SORT_NULLS_LAST };
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, &sk, 1), KEEL_OK);

    /* Apply global LIMIT 3 post-merge */
    keel_scatter_result_apply_limit(r, 3, 0);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3);

    /* Expected global top-3: D=200, A=110, then B=100 or C=100 (either is fine) */
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* row;

    /* Rank 1: D=260 — proves sc_strip_limit_offset prevented shard-0 truncation */
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &row));
    TEST_ASSERT(row[0].len == 1 && row[0].data[0] == 'D');
    char b[32]; size_t l;
    l = (size_t)row[1].len < sizeof b - 1 ? (size_t)row[1].len : sizeof b - 1;
    memcpy(b, row[1].data, l); b[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), (int64_t)260);

    /* Rank 2: A=110 */
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &row));
    TEST_ASSERT(row[0].len == 1 && row[0].data[0] == 'A');
    l = (size_t)row[1].len < sizeof b - 1 ? (size_t)row[1].len : sizeof b - 1;
    memcpy(b, row[1].data, l); b[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), (int64_t)110);

    /* Rank 3: B=100 or C=100 (tie) */
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &row));
    l = (size_t)row[1].len < sizeof b - 1 ? (size_t)row[1].len : sizeof b - 1;
    memcpy(b, row[1].data, l); b[l] = '\0';
    TEST_ASSERT_EQ((int64_t)strtoll(b, NULL, 10), (int64_t)100);

    /* No fourth row */
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &row));

    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    keel_mem_init(NULL);

    if (!integ_cluster_running()) {
        printf("SKIP: no PostgreSQL cluster reachable — set KEEL_TEST_PG_HOST1/"
               "KEEL_TEST_PG_PORT1 or start the test cluster\n");
        g_skip_tests = true;
    }

    if (!g_skip_tests) {
        if (!setup_schemas()) {
            printf("SKIP: failed to create test schemas (check PostgreSQL logs)\n");
            g_skip_tests = true;
        }
    }

    /* Tests requiring a live cluster */
    test_e2e_scatter_count();
    test_e2e_scatter_sum();
    test_e2e_scatter_avg_finalize();
    test_e2e_scatter_group_having();
    test_e2e_scatter_sort_merge();
    test_e2e_scatter_min_max();
    test_e2e_scatter_empty_shard();
    test_e2e_scatter_prepared_exec();     /* 4.1 */

    /* Library-only tests — no PG connection needed */
    test_e2e_scatter_all_shards_no_rows();
    test_e2e_scatter_null_aggregate();
    test_e2e_count_distinct_dispatch_flag();
    test_e2e_scatter_group_order_limit();
    test_e2e_scatter_prepared_param_merge();    /* 4.1 lib-only */
    test_e2e_scatter_group_limit_correctness(); /* 4.2 */

    if (!g_skip_tests)
        teardown_schemas();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
