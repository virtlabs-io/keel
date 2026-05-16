/**
 * @file test_scatter_window.c
 * @brief Unit tests for Phase F window function global recomputation.
 *
 * Tests keel_scatter_result_window_compute() directly with synthetic in-memory
 * results (no PostgreSQL connection required).
 *
 * Covers:
 *   - ROW_NUMBER: sequential 1..N for unordered and ordered inputs
 *   - RANK: tie groups share the rank of their first row; gaps after ties
 *   - DENSE_RANK: tie groups share rank but no gaps between groups
 *   - NTILE: PostgreSQL bucket distribution semantics
 *   - PERCENT_RANK: (group_start)/(N-1) for each row
 *   - CUME_DIST: end_of_tie_group/N for each row
 *   - Spill-path: forces spill via tiny mem_limit, retests ROW_NUMBER + RANK
 *   - Struct field defaults: nwindow_col_specs==0 → no-op
 */

#include "test_utils.h"
#include "keel/core/scatter_store.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) { return g_tests_failed ? 1 : 0; }

/* ============================================================================
 * Helpers
 * ============================================================================ */

/** Two-column descriptor: col0=INT8 (rank output), col1=INT8 (order key). */
static void make_rank_descs(keel_scatter_col_desc_t out[2])
{
    memset(out, 0, 2 * sizeof *out);
    snprintf(out[0].name, sizeof out[0].name, "rn");
    out[0].type = KEEL_COL_TYPE_INT64;
    out[0].format   = 0; /* text */
    snprintf(out[1].name, sizeof out[1].name, "score");
    out[1].type = KEEL_COL_TYPE_INT64;
    out[1].format   = 0;
}

/** Build a text-format column value from a C string. */
static keel_scatter_col_val_t mkval(const char* s)
{
    return (keel_scatter_col_val_t){
        .len  = s ? (int32_t)strlen(s) : -1,
        .data = s,
    };
}

/**
 * Append a two-column row ("placeholder_rank", "score_str") to r.
 * col0 is a placeholder (will be overwritten by window_compute).
 * col1 is the integer order-key as a decimal string.
 */
static void append_row2(keel_scatter_result_t* r, const char* placeholder,
                         const char* score)
{
    keel_scatter_col_val_t vals[2] = { mkval(placeholder), mkval(score) };
    keel_error_t err = keel_scatter_result_append(r, vals);
    (void)err;
}

/**
 * Read col @p ci of row @p row_idx as a parsed int64.
 * Only valid for in-memory results (no spill).
 */
static int64_t read_int_col(keel_scatter_result_t* r, size_t row_idx, int ci)
{
    if (!r->rows || row_idx >= r->row_count) return -9999;
    keel_scatter_row_t* row = r->rows[row_idx];
    if (!row || ci >= row->ncols) return -9999;
    const keel_scatter_col_val_t* v = &row->cols[ci];
    if (v->len <= 0 || !v->data) return -9999;
    char tmp[32] = {0};
    size_t copy = (size_t)v->len < sizeof(tmp) - 1 ? (size_t)v->len : sizeof(tmp) - 1;
    memcpy(tmp, v->data, copy);
    return (int64_t)strtoll(tmp, NULL, 10);
}

/** Build a simple ROW_NUMBER window spec on col 0, ordered by col 1 ASC. */
static keel_window_col_spec_t make_rn_spec(void)
{
    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_ROW_NUMBER;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;
    return ws;
}

/* ============================================================================
 * ROW_NUMBER
 * ============================================================================ */

static void test_row_number_basic(void)
{
    TEST_BEGIN("window: ROW_NUMBER sequential 1..5");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Append 5 rows with scores 30,10,50,20,40 (unsorted) */
    const char* scores[] = { "30", "10", "50", "20", "40" };
    for (int i = 0; i < 5; i++)
        append_row2(r, "0", scores[i]);

    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 5);

    keel_window_col_spec_t ws = make_rn_spec();
    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 5);

    /* After sort-by-score ASC and ROW_NUMBER, col0 must be 1..5 in order */
    for (size_t i = 0; i < 5; i++) {
        int64_t rn = read_int_col(r, i, 0);
        TEST_ASSERT_EQ(rn, (int64_t)(i + 1));
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_row_number_single_row(void)
{
    TEST_BEGIN("window: ROW_NUMBER single row → 1");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row2(r, "0", "42");

    keel_window_col_spec_t ws = make_rn_spec();
    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(read_int_col(r, 0, 0), 1);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_row_number_zero_rows(void)
{
    TEST_BEGIN("window: ROW_NUMBER zero rows → no-op");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_window_col_spec_t ws = make_rn_spec();
    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * RANK
 * ============================================================================ */

static void test_rank_with_ties(void)
{
    TEST_BEGIN("window: RANK tie groups get same rank, gaps after ties");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* scores: 10, 10, 20, 30, 30  → sorted: 10,10,20,30,30
     * expected RANK:               1, 1,  3,  4,  4           */
    const char* scores[] = { "20", "10", "30", "10", "30" };
    for (int i = 0; i < 5; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_RANK;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* After sort by score ASC: rows 0,1→10; row2→20; rows3,4→30 */
    int64_t expected[] = { 1, 1, 3, 4, 4 };
    for (size_t i = 0; i < 5; i++) {
        int64_t rank = read_int_col(r, i, 0);
        TEST_ASSERT_EQ(rank, expected[i]);
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_rank_no_ties(void)
{
    TEST_BEGIN("window: RANK no ties → sequential");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    const char* scores[] = { "3", "1", "2" };
    for (int i = 0; i < 3; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_RANK;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    int64_t expected[] = { 1, 2, 3 };
    for (size_t i = 0; i < 3; i++)
        TEST_ASSERT_EQ(read_int_col(r, i, 0), expected[i]);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * DENSE_RANK
 * ============================================================================ */

static void test_dense_rank_with_ties(void)
{
    TEST_BEGIN("window: DENSE_RANK tie groups get same rank, no gaps");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* scores: 10,10,20,30,30 → sorted → DENSE_RANK: 1,1,2,3,3 */
    const char* scores[] = { "20", "10", "30", "10", "30" };
    for (int i = 0; i < 5; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_DENSE_RANK;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    int64_t expected[] = { 1, 1, 2, 3, 3 };
    for (size_t i = 0; i < 5; i++)
        TEST_ASSERT_EQ(read_int_col(r, i, 0), expected[i]);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * NTILE
 * ============================================================================ */

static void test_ntile_7_rows_3_buckets(void)
{
    TEST_BEGIN("window: NTILE(3) on 7 rows → buckets [3,2,2]");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 7; i++) {
        char score[8];
        snprintf(score, sizeof score, "%d", i);
        append_row2(r, "0", score);
    }

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_NTILE;
    ws.ntile_n   = 3;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* PostgreSQL NTILE(3) on 7: first 7%3=1 bucket(s) get floor(7/3)+1=3 rows,
     * remaining 2 buckets get floor(7/3)=2 rows.
     * Expected: 1,1,1, 2,2, 3,3 */
    int64_t expected[] = { 1, 1, 1, 2, 2, 3, 3 };
    for (size_t i = 0; i < 7; i++)
        TEST_ASSERT_EQ(read_int_col(r, i, 0), expected[i]);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_ntile_4_rows_4_buckets(void)
{
    TEST_BEGIN("window: NTILE(4) on 4 rows → 1,2,3,4");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 4; i++) {
        char score[8];
        snprintf(score, sizeof score, "%d", i * 10);
        append_row2(r, "0", score);
    }

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_NTILE;
    ws.ntile_n   = 4;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    for (size_t i = 0; i < 4; i++)
        TEST_ASSERT_EQ(read_int_col(r, i, 0), (int64_t)(i + 1));

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_ntile_n_gt_rows(void)
{
    TEST_BEGIN("window: NTILE(10) on 3 rows → 1,2,3 (each its own bucket)");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 3; i++) {
        char score[8];
        snprintf(score, sizeof score, "%d", i);
        append_row2(r, "0", score);
    }

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_NTILE;
    ws.ntile_n   = 10;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* When n > rows, each row gets its own bucket: 1,2,3 */
    int64_t expected[] = { 1, 2, 3 };
    for (size_t i = 0; i < 3; i++)
        TEST_ASSERT_EQ(read_int_col(r, i, 0), expected[i]);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * PERCENT_RANK and CUME_DIST — use double column
 * ============================================================================ */

/** Three-column descriptor: col0=FLOAT8 (window output), col1=INT8 (key),
 *  col2=TEXT (dummy extra column). */
static void make_float_descs(keel_scatter_col_desc_t out[2])
{
    memset(out, 0, 2 * sizeof *out);
    snprintf(out[0].name, sizeof out[0].name, "pct");
    out[0].type = KEEL_COL_TYPE_FLOAT64;
    out[0].format   = 0;
    snprintf(out[1].name, sizeof out[1].name, "score");
    out[1].type = KEEL_COL_TYPE_INT64;
    out[1].format   = 0;
}

static double read_double_col(keel_scatter_result_t* r, size_t row_idx, int ci)
{
    if (!r->rows || row_idx >= r->row_count) return -9999.0;
    keel_scatter_row_t* row = r->rows[row_idx];
    if (!row || ci >= row->ncols) return -9999.0;
    const keel_scatter_col_val_t* v = &row->cols[ci];
    if (v->len <= 0 || !v->data) return -9999.0;
    char tmp[64] = {0};
    size_t copy = (size_t)v->len < sizeof(tmp) - 1 ? (size_t)v->len : sizeof(tmp) - 1;
    memcpy(tmp, v->data, copy);
    return strtod(tmp, NULL);
}

static void test_percent_rank_4_rows(void)
{
    TEST_BEGIN("window: PERCENT_RANK on 4 distinct rows → 0, 1/3, 2/3, 1");

    keel_scatter_col_desc_t cols[2];
    make_float_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* scores 40,10,30,20 → sorted: 10,20,30,40 */
    const char* scores[] = { "40", "10", "30", "20" };
    for (int i = 0; i < 4; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_PERCENT_RANK;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* PERCENT_RANK: (rank-1)/(N-1): 0/3, 1/3, 2/3, 3/3 */
    double expected[] = { 0.0, 1.0/3.0, 2.0/3.0, 1.0 };
    for (size_t i = 0; i < 4; i++) {
        double v = read_double_col(r, i, 0);
        double diff = v - expected[i];
        if (diff < 0) diff = -diff;
        TEST_ASSERT(diff < 1e-9);
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_percent_rank_single_row(void)
{
    TEST_BEGIN("window: PERCENT_RANK single row → 0.0");

    keel_scatter_col_desc_t cols[2];
    make_float_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row2(r, "0", "99");

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_PERCENT_RANK;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    double v = read_double_col(r, 0, 0);
    TEST_ASSERT(v < 1e-15);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_cume_dist_with_ties(void)
{
    TEST_BEGIN("window: CUME_DIST with ties → end-of-group/N");

    keel_scatter_col_desc_t cols[2];
    make_float_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* scores: 10,10,20,30 → sorted: 10,10,20,30
     * CUME_DIST: group 1 ends at j=2 → 2/4=0.5
     *            group 2 ends at j=3 → 3/4=0.75
     *            group 3 ends at j=4 → 4/4=1.0 */
    const char* scores[] = { "20", "10", "30", "10" };
    for (int i = 0; i < 4; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_CUME_DIST;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    double expected[] = { 0.5, 0.5, 0.75, 1.0 };
    for (size_t i = 0; i < 4; i++) {
        double v = read_double_col(r, i, 0);
        double diff = v - expected[i];
        if (diff < 0) diff = -diff;
        TEST_ASSERT(diff < 1e-9);
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Spill-path tests
 * ============================================================================ */

static void test_spill_row_number(void)
{
    TEST_BEGIN("window: ROW_NUMBER via spill path (tiny mem_limit)");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    /* Tiny memory limit to force spill. 1 byte forces spill immediately. */
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 1, NULL);
    if (!r) {
        /* If spill dir not writable, skip gracefully */
        printf("  SKIP: cannot create spill-path result (no temp dir?)\n");
        return;
    }

    const char* scores[] = { "30", "10", "50", "20", "40" };
    for (int i = 0; i < 5; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws = make_rn_spec();
    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 5);

    /* Iterate via iterator and check ROW_NUMBER values */
    keel_scatter_result_iter_t it;
    keel_error_t ierr = keel_scatter_result_iter_init(&it, r);
    TEST_ASSERT_EQ(ierr, KEEL_OK);

    int64_t rn = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        rn++;
        char tmp[32] = {0};
        size_t copy = (size_t)vals[0].len < sizeof(tmp) - 1
                      ? (size_t)vals[0].len : sizeof(tmp) - 1;
        memcpy(tmp, vals[0].data, copy);
        int64_t got = strtoll(tmp, NULL, 10);
        TEST_ASSERT_EQ(got, rn);
    }
    TEST_ASSERT_EQ(rn, 5);
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_spill_rank(void)
{
    TEST_BEGIN("window: RANK with ties via spill path");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 1, NULL);
    if (!r) {
        printf("  SKIP: cannot create spill-path result\n");
        return;
    }

    /* scores: 10,10,20,30,30 → RANK: 1,1,3,4,4 */
    const char* scores[] = { "20", "10", "30", "10", "30" };
    for (int i = 0; i < 5; i++)
        append_row2(r, "0", scores[i]);

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    ws.col_index = 0;
    ws.func      = KEEL_WFUNC_RANK;
    ws.ntile_n   = 1;
    ws.order_keys[0].col_index = 1;
    ws.order_keys[0].dir       = KEEL_SORT_ASC;
    ws.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    ws.norder_keys = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 5);

    keel_scatter_result_iter_t it;
    keel_error_t ierr = keel_scatter_result_iter_init(&it, r);
    TEST_ASSERT_EQ(ierr, KEEL_OK);

    int64_t expected[] = { 1, 1, 3, 4, 4 };
    int64_t idx = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals) && idx < 5) {
        char tmp[32] = {0};
        size_t copy = (size_t)vals[0].len < sizeof(tmp) - 1
                      ? (size_t)vals[0].len : sizeof(tmp) - 1;
        memcpy(tmp, vals[0].data, copy);
        int64_t got = strtoll(tmp, NULL, 10);
        TEST_ASSERT_EQ(got, expected[idx]);
        idx++;
    }
    TEST_ASSERT_EQ(idx, 5);
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * nwindow_col_specs == 0 → no-op
 * ============================================================================ */

static void test_no_specs_is_noop(void)
{
    TEST_BEGIN("window: nwindow_col_specs==0 is a no-op");

    keel_scatter_col_desc_t cols[2];
    make_rank_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 3; i++) {
        char score[8];
        snprintf(score, sizeof score, "%d", i);
        append_row2(r, "99", score);
    }

    keel_window_col_spec_t ws = make_rn_spec();
    /* Pass nspecs=0 — nothing should change */
    keel_error_t err = keel_scatter_result_window_compute(r, &ws, 0);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 3);

    /* col0 should still be "99" (unchanged) */
    TEST_ASSERT_EQ(read_int_col(r, 0, 0), 99);
    TEST_ASSERT_EQ(read_int_col(r, 1, 0), 99);
    TEST_ASSERT_EQ(read_int_col(r, 2, 0), 99);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Window col spec defaults
 * ============================================================================ */

static void test_window_col_spec_defaults(void)
{
    TEST_BEGIN("keel_window_col_spec_t: zero-init has norder_keys==0");

    keel_window_col_spec_t ws;
    memset(&ws, 0, sizeof ws);
    TEST_ASSERT_EQ(ws.norder_keys, 0);
    TEST_ASSERT_EQ(ws.ntile_n, 0);
    TEST_ASSERT_EQ(ws.col_index, 0);
    TEST_ASSERT_EQ(ws.func, KEEL_WFUNC_ROW_NUMBER);

    TEST_END();
}

/* ============================================================================
 * Tier 3: value-access window function tests
 *
 * Layout for most tests:
 *   col 0 = output (LAG/LEAD/FIRST_VALUE/…) — written by window_compute
 *   col 1 = source value (integer string)
 *   col 2 = partition key (integer string)
 *   col 3 = order key (integer string)
 * ============================================================================ */

/**
 * Describe a 4-column result: col0=output, col1=value, col2=part, col3=order.
 */
static void make_t3_descs(keel_scatter_col_desc_t out[4])
{
    memset(out, 0, 4 * sizeof *out);
    snprintf(out[0].name, sizeof out[0].name, "out");
    out[0].type = KEEL_COL_TYPE_INT64; out[0].format = 0;
    snprintf(out[1].name, sizeof out[1].name, "val");
    out[1].type = KEEL_COL_TYPE_INT64; out[1].format = 0;
    snprintf(out[2].name, sizeof out[2].name, "part");
    out[2].type = KEEL_COL_TYPE_INT64; out[2].format = 0;
    snprintf(out[3].name, sizeof out[3].name, "ord");
    out[3].type = KEEL_COL_TYPE_INT64; out[3].format = 0;
}

/** Append a 4-column row to r. */
static void append_row4(keel_scatter_result_t* r,
                         const char* out_ph,
                         const char* val,
                         const char* part,
                         const char* ord)
{
    keel_scatter_col_val_t vals[4] = {
        mkval(out_ph), mkval(val), mkval(part), mkval(ord)
    };
    keel_error_t err = keel_scatter_result_append(r, vals);
    (void)err;
}

/**
 * Read col ci of row row_idx; return NULL_SENTINEL (-9999) when SQL NULL.
 * Positive: returns parsed int64. Negative sentinel: -9999.
 */
#define T3_NULL_SENTINEL (-9999)

static int64_t read_t3_col(keel_scatter_result_t* r, size_t row_idx, int ci)
{
    if (!r->rows || row_idx >= r->row_count) return T3_NULL_SENTINEL;
    keel_scatter_row_t* row = r->rows[row_idx];
    if (!row || ci >= row->ncols) return T3_NULL_SENTINEL;
    const keel_scatter_col_val_t* v = &row->cols[ci];
    if (v->len < 0) return T3_NULL_SENTINEL; /* SQL NULL */
    if (v->len == 0 || !v->data) return 0;
    char tmp[32] = {0};
    size_t n = (size_t)v->len < sizeof(tmp) - 1 ? (size_t)v->len : sizeof(tmp) - 1;
    memcpy(tmp, v->data, n);
    return (int64_t)strtoll(tmp, NULL, 10);
}

/** Build a Tier 3 spec for a single-partition (no partition keys) test. */
static keel_window_col_spec_t make_t3_spec(keel_window_func_t func)
{
    keel_window_col_spec_t sp;
    memset(&sp, 0, sizeof sp);
    sp.col_index       = 0;   /* output column */
    sp.func            = func;
    sp.source_col      = 1;   /* read from col 1 */
    sp.val_offset      = 1;   /* default LAG/LEAD offset */
    sp.default_val_len = -1;  /* SQL NULL default */
    /* order by col 3 ASC */
    sp.order_keys[0].col_index = 3;
    sp.order_keys[0].dir       = KEEL_SORT_ASC;
    sp.order_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    sp.norder_keys = 1;
    /* partition by col 2 */
    sp.partition_keys[0].col_index = 2;
    sp.partition_keys[0].dir       = KEEL_SORT_ASC;
    sp.partition_keys[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
    sp.npartition_keys = 1;
    /* frame: ROWS UNBOUNDED PRECEDING → UNBOUNDED FOLLOWING */
    sp.frame_mode         = KEEL_FRAME_ROWS;
    sp.frame_start.type   = KEEL_FRAME_UNBOUNDED_PRECEDING;
    sp.frame_end.type     = KEEL_FRAME_UNBOUNDED_FOLLOWING;
    return sp;
}

/* ------------------------------------------------------------------ */
/* LAG tests                                                           */
/* ------------------------------------------------------------------ */

static void test_lag_basic(void)
{
    TEST_BEGIN("window: LAG offset=1 single partition 5 rows");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* 5 rows, same partition (part="1"), values 10..50, ordered by col3 1..5 */
    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");
    append_row4(r, "0", "40", "1", "4");
    append_row4(r, "0", "50", "1", "5");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAG);
    sp.val_offset = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ((int)r->row_count, 5);

    /* After sort by (part ASC, ord ASC): rows are already ordered 1..5.
     * LAG(1): row 0 → NULL, row 1 → 10, row 2 → 20, row 3 → 30, row 4 → 40 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), T3_NULL_SENTINEL);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 10);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 20);
    TEST_ASSERT_EQ(read_t3_col(r, 3, 0), 30);
    TEST_ASSERT_EQ(read_t3_col(r, 4, 0), 40);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_lag_offset2(void)
{
    TEST_BEGIN("window: LAG offset=2 — rows before partition start → NULL");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAG);
    sp.val_offset = 2;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* row 0: i-2 = -2 → NULL; row 1: i-2 = -1 → NULL; row 2: i-2 = 0 → val=10 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), T3_NULL_SENTINEL);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), T3_NULL_SENTINEL);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 10);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_lag_with_default(void)
{
    TEST_BEGIN("window: LAG offset=1 with integer default value");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAG);
    sp.val_offset = 1;
    /* default = -1 */
    sp.default_val_len = snprintf(sp.default_val, sizeof sp.default_val, "%d", -1);

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* row 0 offset falls outside → default -1; row 1 → 10; row 2 → 20 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), -1);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 10);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 20);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ------------------------------------------------------------------ */
/* LEAD tests                                                          */
/* ------------------------------------------------------------------ */

static void test_lead_basic(void)
{
    TEST_BEGIN("window: LEAD offset=1 single partition 5 rows");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");
    append_row4(r, "0", "40", "1", "4");
    append_row4(r, "0", "50", "1", "5");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LEAD);
    sp.val_offset = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* LEAD(1): row 0→20, row 1→30, row 2→40, row 3→50, row 4→NULL */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), 20);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 30);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 40);
    TEST_ASSERT_EQ(read_t3_col(r, 3, 0), 50);
    TEST_ASSERT_EQ(read_t3_col(r, 4, 0), T3_NULL_SENTINEL);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_lead_past_partition_end(void)
{
    TEST_BEGIN("window: LEAD does not cross partition boundary");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Partition 1: 3 rows; Partition 2: 2 rows */
    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");
    append_row4(r, "0", "40", "2", "1");
    append_row4(r, "0", "50", "2", "2");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LEAD);
    sp.val_offset = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Partition 1 rows (sorted: 1/1,1/2,1/3): 20, 30, NULL */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), 20);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 30);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), T3_NULL_SENTINEL); /* end of part1 */
    /* Partition 2 rows: 50, NULL */
    TEST_ASSERT_EQ(read_t3_col(r, 3, 0), 50);
    TEST_ASSERT_EQ(read_t3_col(r, 4, 0), T3_NULL_SENTINEL); /* end of part2 */

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ------------------------------------------------------------------ */
/* Two-partition LAG boundary test                                     */
/* ------------------------------------------------------------------ */

static void test_lag_two_partitions(void)
{
    TEST_BEGIN("window: LAG does not cross partition boundary (2 parts × 3 rows)");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Interleaved rows to exercise sort */
    append_row4(r, "0", "11", "1", "1");
    append_row4(r, "0", "21", "2", "1");
    append_row4(r, "0", "12", "1", "2");
    append_row4(r, "0", "22", "2", "2");
    append_row4(r, "0", "13", "1", "3");
    append_row4(r, "0", "23", "2", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAG);
    sp.val_offset = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ((int)r->row_count, 6);

    /* After sort by (part ASC, ord ASC):
     *  idx 0: part=1,ord=1 → NULL    idx 1: part=1,ord=2 → 11
     *  idx 2: part=1,ord=3 → 12
     *  idx 3: part=2,ord=1 → NULL    idx 4: part=2,ord=2 → 21
     *  idx 5: part=2,ord=3 → 22 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), T3_NULL_SENTINEL);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 11);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 12);
    TEST_ASSERT_EQ(read_t3_col(r, 3, 0), T3_NULL_SENTINEL);
    TEST_ASSERT_EQ(read_t3_col(r, 4, 0), 21);
    TEST_ASSERT_EQ(read_t3_col(r, 5, 0), 22);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ------------------------------------------------------------------ */
/* FIRST_VALUE tests                                                   */
/* ------------------------------------------------------------------ */

static void test_first_value_unbounded(void)
{
    TEST_BEGIN("window: FIRST_VALUE ROWS UNBOUNDED PRECEDING → every row sees part first");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_FIRST_VALUE);
    /* frame: ROWS UNBOUNDED PRECEDING → UNBOUNDED FOLLOWING (set by make_t3_spec) */

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* All rows should see the first value in partition: 10 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), 10);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 10);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 10);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ------------------------------------------------------------------ */
/* LAST_VALUE tests                                                    */
/* ------------------------------------------------------------------ */

static void test_last_value_unbounded(void)
{
    TEST_BEGIN("window: LAST_VALUE ROWS UNBOUNDED FOLLOWING → every row sees part last");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAST_VALUE);
    /* frame end = UNBOUNDED FOLLOWING already set by make_t3_spec */

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* All rows see the last value in partition: 30 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), 30);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 30);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 30);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_last_value_current_row_frame(void)
{
    TEST_BEGIN("window: LAST_VALUE ROWS UNBOUNDED PRECEDING → CURRENT ROW = current value");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAST_VALUE);
    /* Override frame end to CURRENT ROW (PostgreSQL default for LAST_VALUE) */
    sp.frame_end.type = KEEL_FRAME_CURRENT_ROW;
    sp.frame_end.n    = 0;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Each row sees its own value */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), 10);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 20);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 30);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ------------------------------------------------------------------ */
/* NTH_VALUE tests                                                     */
/* ------------------------------------------------------------------ */

static void test_nth_value_second(void)
{
    TEST_BEGIN("window: NTH_VALUE(col,2) within UNBOUNDED frame → 2nd partition row");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_NTH_VALUE);
    sp.val_offset = 2; /* n=2 (1-based) */

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* All rows see the 2nd value in partition: 20 */
    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), 20);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), 20);
    TEST_ASSERT_EQ(read_t3_col(r, 2, 0), 20);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_nth_value_out_of_range(void)
{
    TEST_BEGIN("window: NTH_VALUE n > partition size → NULL");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_NTH_VALUE);
    sp.val_offset = 5; /* n=5 but only 2 rows */

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_ASSERT_EQ(read_t3_col(r, 0, 0), T3_NULL_SENTINEL);
    TEST_ASSERT_EQ(read_t3_col(r, 1, 0), T3_NULL_SENTINEL);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ------------------------------------------------------------------ */
/* Spill path for LAG                                                  */
/* ------------------------------------------------------------------ */

static void test_spill_lag(void)
{
    TEST_BEGIN("window: LAG offset=1 via spill path");

    keel_scatter_col_desc_t cols[4];
    make_t3_descs(cols);
    /* Force spill with 1-byte memory limit */
    keel_scatter_result_t* r = keel_scatter_result_create(4, cols, 1, NULL);
    TEST_ASSERT(r != NULL);

    append_row4(r, "0", "10", "1", "1");
    append_row4(r, "0", "20", "1", "2");
    append_row4(r, "0", "30", "1", "3");
    append_row4(r, "0", "40", "1", "4");

    keel_window_col_spec_t sp = make_t3_spec(KEEL_WFUNC_LAG);
    sp.val_offset = 1;

    keel_error_t err = keel_scatter_result_window_compute(r, &sp, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Read back via iterator (spill path) */
    keel_scatter_result_iter_t it;
    err = keel_scatter_result_iter_init(&it, r);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* After sort: row0→NULL, row1→10, row2→20, row3→30 */
    const int64_t expected[] = { T3_NULL_SENTINEL, 10, 20, 30 };
    const keel_scatter_col_val_t* vals;
    size_t row_i = 0;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        int64_t got;
        if (vals[0].len < 0) {
            got = T3_NULL_SENTINEL;
        } else {
            char tmp[32] = {0};
            size_t n = (size_t)vals[0].len < 31 ? (size_t)vals[0].len : 31;
            memcpy(tmp, vals[0].data, n);
            got = (int64_t)strtoll(tmp, NULL, 10);
        }
        if (row_i < 4) {
            TEST_ASSERT_EQ(got, expected[row_i]);
        }
        row_i++;
    }
    keel_scatter_result_iter_close(&it);
    TEST_ASSERT_EQ((int)row_i, 4);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== test_scatter_window ===\n\n");

    /* ROW_NUMBER */
    test_row_number_basic();
    test_row_number_single_row();
    test_row_number_zero_rows();

    /* RANK */
    test_rank_with_ties();
    test_rank_no_ties();

    /* DENSE_RANK */
    test_dense_rank_with_ties();

    /* NTILE */
    test_ntile_7_rows_3_buckets();
    test_ntile_4_rows_4_buckets();
    test_ntile_n_gt_rows();

    /* PERCENT_RANK */
    test_percent_rank_4_rows();
    test_percent_rank_single_row();

    /* CUME_DIST */
    test_cume_dist_with_ties();

    /* Spill path */
    test_spill_row_number();
    test_spill_rank();

    /* No-op */
    test_no_specs_is_noop();

    /* Struct defaults */
    test_window_col_spec_defaults();

    /* Tier 3: LAG */
    test_lag_basic();
    test_lag_offset2();
    test_lag_with_default();

    /* Tier 3: LEAD */
    test_lead_basic();
    test_lead_past_partition_end();

    /* Tier 3: multi-partition boundary */
    test_lag_two_partitions();

    /* Tier 3: FIRST_VALUE */
    test_first_value_unbounded();

    /* Tier 3: LAST_VALUE */
    test_last_value_unbounded();
    test_last_value_current_row_frame();

    /* Tier 3: NTH_VALUE */
    test_nth_value_second();
    test_nth_value_out_of_range();

    /* Tier 3: spill path */
    test_spill_lag();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
