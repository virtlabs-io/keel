/**
 * @file test_scatter_store.c
 * @brief Unit tests for the scatter-merge result store (Phase A).
 *
 * Covers:
 *   - Memory-path: create, append, iterate, destroy (no spill)
 *   - Spill-path: triggers spill mid-append, continues appending to disk,
 *     iterates all rows correctly from disk
 *   - Large-row spill: rows larger than the write-buffer block size
 *   - OID comparator: text and binary formats for all supported type families
 *   - NULL handling: NULLS FIRST ordering in comparator
 *   - Edge cases: zero-row result, single-row result, single-column result
 *   - Config: custom mem_limit_bytes clamping, custom spill_dir
 */

#include "test_utils.h"
#include "keel/core/scatter_store.h"
#include "keel/core/router.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <limits.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) { return g_tests_failed ? 1 : 0; }

/* ============================================================================
 * Test helpers
 * ============================================================================ */

/** Build a simple two-column descriptor (int4, text). */
static void make_two_col_descs(keel_scatter_col_desc_t out[2])
{
    memset(out, 0, 2 * sizeof *out);
    snprintf(out[0].name, sizeof out[0].name, "id");
    out[0].type = KEEL_COL_TYPE_INT32;
    out[0].format   = 0; /* text */
    snprintf(out[1].name, sizeof out[1].name, "val");
    out[1].type = KEEL_COL_TYPE_TEXT;
    out[1].format   = 0; /* text */
}

/** Build a column value from a string literal (text format). */
static keel_scatter_col_val_t mkval(const char* s)
{
    return (keel_scatter_col_val_t){
        .len  = (int32_t)(s ? (int32_t)strlen(s) : -1),
        .data = s,
    };
}

/** Build a SQL NULL column value. */
static keel_scatter_col_val_t mknull(void)
{
    return (keel_scatter_col_val_t){ .len = -1, .data = NULL };
}

/* ============================================================================
 * create / destroy
 * ============================================================================ */

static void test_create_destroy(void)
{
    TEST_BEGIN("scatter_store: create and destroy");

    keel_scatter_col_desc_t cols[2];
    make_two_col_descs(cols);

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 0, NULL);
    TEST_ASSERT(r != NULL);
    TEST_ASSERT_EQ(keel_scatter_result_ncols(r), 2);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);
    TEST_ASSERT(!keel_scatter_result_spilled(r));

    keel_scatter_result_destroy(r);
    /* Destroy of NULL is a no-op */
    keel_scatter_result_destroy(NULL);

    TEST_END();
}

/* ============================================================================
 * memory path: append + iterate
 * ============================================================================ */

static void test_memory_path_basic(void)
{
    TEST_BEGIN("scatter_store: memory path basic append + iterate");

    keel_scatter_col_desc_t cols[2];
    make_two_col_descs(cols);

    /* Large mem limit so we stay in memory */
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols,
                                                  64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Append 3 rows */
    for (int i = 0; i < 3; i++) {
        char id_buf[16], val_buf[32];
        snprintf(id_buf,  sizeof id_buf,  "%d",   i + 1);
        snprintf(val_buf, sizeof val_buf, "row%d", i + 1);
        keel_scatter_col_val_t vals[2] = { mkval(id_buf), mkval(val_buf) };
        keel_error_t err = keel_scatter_result_append(r, vals);
        TEST_ASSERT_EQ(err, KEEL_OK);
    }

    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 3);
    TEST_ASSERT(!keel_scatter_result_spilled(r));

    /* Iterate and verify */
    keel_scatter_result_iter_t it;
    keel_error_t err = keel_scatter_result_iter_init(&it, r);
    TEST_ASSERT_EQ(err, KEEL_OK);

    int count = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        count++;
        /* id column should be a decimal digit string */
        TEST_ASSERT(vals[0].len > 0);
        /* val column should start with "row" */
        TEST_ASSERT(vals[1].len >= 4);
        TEST_ASSERT(memcmp(vals[1].data, "row", 3) == 0);
    }
    TEST_ASSERT_EQ(count, 3);
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_memory_path_null_values(void)
{
    TEST_BEGIN("scatter_store: memory path NULL column values");

    keel_scatter_col_desc_t cols[2];
    make_two_col_descs(cols);
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_scatter_col_val_t vals[2] = { mknull(), mkval("hello") };
    TEST_ASSERT_EQ(keel_scatter_result_append(r, vals), KEEL_OK);

    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* rv;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &rv));
    TEST_ASSERT_EQ(rv[0].len, -1);              /* NULL */
    TEST_ASSERT(rv[1].len == (int32_t)strlen("hello"));
    TEST_ASSERT(memcmp(rv[1].data, "hello", 5) == 0);
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &rv));
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_memory_path_zero_rows(void)
{
    TEST_BEGIN("scatter_store: memory path zero rows");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "x");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 0, NULL);
    TEST_ASSERT(r != NULL);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);

    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* rv;
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &rv));
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * spill path
 * ============================================================================ */

static void test_spill_path_basic(void)
{
    TEST_BEGIN("scatter_store: spill path triggered and all rows read back");

    keel_scatter_col_desc_t cols[2];
    make_two_col_descs(cols);

    /*
     * Use a very small memory limit (1 MiB minimum) to trigger spill
     * after the first few small rows.  Each row is ~50 bytes, so 200
     * rows should comfortably spill.
     */
    keel_scatter_result_t* r = keel_scatter_result_create(2, cols,
                                                  KEEL_SCATTER_MIN_MEM_LIMIT_BYTES,
                                                  "/tmp");
    TEST_ASSERT(r != NULL);

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        char id_buf[16], val_buf[64];
        snprintf(id_buf,  sizeof id_buf,  "%d",    i);
        snprintf(val_buf, sizeof val_buf, "value_%d_abcdefghijklmnopqrst", i);
        keel_scatter_col_val_t vals[2] = { mkval(id_buf), mkval(val_buf) };
        keel_error_t err = keel_scatter_result_append(r, vals);
        TEST_ASSERT_EQ(err, KEEL_OK);
    }

    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)N);

    /* Iterate all rows and count them */
    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);

    const keel_scatter_col_val_t* vals;
    int count = 0;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        TEST_ASSERT(vals[0].len > 0);
        TEST_ASSERT(vals[1].len > 0);
        TEST_ASSERT(memcmp(vals[1].data, "value_", 6) == 0);
        count++;
    }
    TEST_ASSERT_EQ(count, N);
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_spill_second_iterate(void)
{
    TEST_BEGIN("scatter_store: spill path can be re-iterated");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;
    cols[0].format   = 0;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols,
                                                  KEEL_SCATTER_MIN_MEM_LIMIT_BYTES,
                                                  "/tmp");
    TEST_ASSERT(r != NULL);

    const int N = 500;
    for (int i = 0; i < N; i++) {
        char buf[16];
        snprintf(buf, sizeof buf, "%d", i);
        keel_scatter_col_val_t vals[1] = { mkval(buf) };
        TEST_ASSERT_EQ(keel_scatter_result_append(r, vals), KEEL_OK);
    }

    /* First pass */
    {
        keel_scatter_result_iter_t it;
        TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
        const keel_scatter_col_val_t* vals;
        int count = 0;
        while (keel_scatter_result_iter_next(&it, &vals)) count++;
        TEST_ASSERT_EQ(count, N);
        keel_scatter_result_iter_close(&it);
    }

    /* Second pass — same result */
    {
        keel_scatter_result_iter_t it;
        TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
        const keel_scatter_col_val_t* vals;
        int count = 0;
        while (keel_scatter_result_iter_next(&it, &vals)) count++;
        TEST_ASSERT_EQ(count, N);
        keel_scatter_result_iter_close(&it);
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_spill_large_rows(void)
{
    TEST_BEGIN("scatter_store: spill path with rows larger than write buffer");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "blob");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    cols[0].format   = 0;

    /* 1 MiB minimum, but rows are 128 KiB each → each row exceeds the
     * 64 KiB write buffer so the large-row direct-write path is exercised. */
    keel_scatter_result_t* r = keel_scatter_result_create(1, cols,
                                                  KEEL_SCATTER_MIN_MEM_LIMIT_BYTES,
                                                  "/tmp");
    TEST_ASSERT(r != NULL);

    const size_t ROW_DATA = 128 * 1024; /* 128 KiB */
    char* big = (char*)malloc(ROW_DATA);
    TEST_ASSERT(big != NULL);
    memset(big, 'X', ROW_DATA);

    const int N = 10;
    for (int i = 0; i < N; i++) {
        big[0] = (char)('A' + i % 26); /* make each row distinguishable */
        keel_scatter_col_val_t vals[1] = {{ .len = (int32_t)ROW_DATA, .data = big }};
        TEST_ASSERT_EQ(keel_scatter_result_append(r, vals), KEEL_OK);
    }

    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)N);
    TEST_ASSERT(keel_scatter_result_spilled(r));

    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* vals;
    int count = 0;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        TEST_ASSERT_EQ(vals[0].len, (int32_t)ROW_DATA);
        TEST_ASSERT(vals[0].data[0] == (char)('A' + count));
        count++;
    }
    TEST_ASSERT_EQ(count, N);
    keel_scatter_result_iter_close(&it);

    free(big);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * OID comparator — text format
 * ============================================================================ */

static void test_cmp_text_int4(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: INT4 text format");

    keel_scatter_col_val_t a = mkval("10");
    keel_scatter_col_val_t b = mkval("20");
    keel_scatter_col_val_t c = mkval("10");
    keel_scatter_col_val_t n = mknull();

    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 0, &a, &b) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 0, &b, &a) > 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 0, &a, &c) == 0);
    /* NULL sorts before non-NULL */
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 0, &n, &a) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 0, &a, &n) > 0);

    TEST_END();
}

static void test_cmp_text_int8(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: INT8 text format");

    keel_scatter_col_val_t lo = mkval("1000000000");
    keel_scatter_col_val_t hi = mkval("9999999999");
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT64, 0, &lo, &hi) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT64, 0, &hi, &lo) > 0);

    TEST_END();
}

static void test_cmp_text_float8(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: FLOAT8 text format");

    keel_scatter_col_val_t a = mkval("1.5");
    keel_scatter_col_val_t b = mkval("2.5");
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_FLOAT64, 0, &a, &b) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_FLOAT64, 0, &b, &a) > 0);

    TEST_END();
}

static void test_cmp_text_text(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: TEXT text format");

    keel_scatter_col_val_t a = mkval("apple");
    keel_scatter_col_val_t b = mkval("banana");
    keel_scatter_col_val_t c = mkval("apple");
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_TEXT, 0, &a, &b) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_TEXT, 0, &b, &a) > 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_TEXT, 0, &a, &c) == 0);

    TEST_END();
}

static void test_cmp_text_bool(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: BOOL text format");

    keel_scatter_col_val_t t = mkval("t");
    keel_scatter_col_val_t f = mkval("f");
    /* true > false */
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_BOOL, 0, &t, &f) > 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_BOOL, 0, &f, &t) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_BOOL, 0, &t, &t) == 0);

    TEST_END();
}

static void test_cmp_text_timestamp(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: TIMESTAMP text format (ISO lexicographic)");

    keel_scatter_col_val_t a = mkval("2023-01-01 12:00:00");
    keel_scatter_col_val_t b = mkval("2024-06-15 08:30:00");
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_TIMESTAMP, 0, &a, &b) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_TIMESTAMP, 0, &b, &a) > 0);

    TEST_END();
}

static void test_cmp_text_uuid(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: UUID text format");

    keel_scatter_col_val_t a = mkval("00000000-0000-0000-0000-000000000001");
    keel_scatter_col_val_t b = mkval("00000000-0000-0000-0000-000000000002");
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_UUID, 0, &a, &b) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_UUID, 0, &b, &a) > 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_UUID, 0, &a, &a) == 0);

    TEST_END();
}

static void test_cmp_null_null(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: NULL vs NULL is equal");

    keel_scatter_col_val_t n1 = mknull();
    keel_scatter_col_val_t n2 = mknull();
    TEST_ASSERT_EQ(keel_scatter_col_cmp(KEEL_COL_TYPE_TEXT, 0, &n1, &n2), 0);

    TEST_END();
}

/* ============================================================================
 * OID comparator — binary format
 * ============================================================================ */

/** Build a big-endian int32 binary value. */
static keel_scatter_col_val_t mkbinval32(int32_t v, char buf[4])
{
    buf[0] = (char)((uint32_t)v >> 24 & 0xFF);
    buf[1] = (char)((uint32_t)v >> 16 & 0xFF);
    buf[2] = (char)((uint32_t)v >>  8 & 0xFF);
    buf[3] = (char)((uint32_t)v       & 0xFF);
    return (keel_scatter_col_val_t){ .len = 4, .data = buf };
}

/** Build a big-endian int64 binary value. */
static keel_scatter_col_val_t mkbinval64(int64_t v, char buf[8])
{
    uint64_t u = (uint64_t)v;
    for (int i = 7; i >= 0; i--) { buf[i] = (char)(u & 0xFF); u >>= 8; }
    return (keel_scatter_col_val_t){ .len = 8, .data = buf };
}

static void test_cmp_binary_int4(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: INT4 binary format");

    char ba[4], bb[4];
    keel_scatter_col_val_t a = mkbinval32(-1,  ba);
    keel_scatter_col_val_t b = mkbinval32(100, bb);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 1, &a, &b) < 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT32, 1, &b, &a) > 0);

    TEST_END();
}

static void test_cmp_binary_int8(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: INT8 binary format");

    char ba[8], bb[8];
    keel_scatter_col_val_t a = mkbinval64((int64_t)1e15,  ba);
    keel_scatter_col_val_t b = mkbinval64((int64_t)2e15, bb);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_INT64, 1, &a, &b) < 0);

    TEST_END();
}

static void test_cmp_binary_bool(void)
{
    TEST_BEGIN("keel_scatter_col_cmp: BOOL binary format");

    char bt = 1, bf = 0;
    keel_scatter_col_val_t t = { .len = 1, .data = &bt };
    keel_scatter_col_val_t f = { .len = 1, .data = &bf };
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_BOOL, 1, &t, &f) > 0);
    TEST_ASSERT(keel_scatter_col_cmp(KEEL_COL_TYPE_BOOL, 1, &f, &t) < 0);

    TEST_END();
}

/* ============================================================================
 * Config clamping
 * ============================================================================ */

static void test_mem_limit_clamping(void)
{
    TEST_BEGIN("scatter_store: mem_limit_bytes clamped to minimum");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "x");
    cols[0].type = KEEL_COL_TYPE_INT32;

    /* Pass 100 bytes (below KEEL_SCATTER_MIN_MEM_LIMIT_BYTES) */
    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 100, "/tmp");
    TEST_ASSERT(r != NULL);
    /* The store should still work — just with the 1 MiB floor applied */
    keel_scatter_col_val_t vals[1] = { mkval("42") };
    TEST_ASSERT_EQ(keel_scatter_result_append(r, vals), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);
    keel_scatter_result_destroy(r);

    TEST_END();
}

/* ============================================================================
 * Dispatch result struct — requires_merge field present
 * ============================================================================ */

static void test_dispatch_result_requires_merge_field(void)
{
    TEST_BEGIN("keel_dispatch_result_t: requires_merge field exists and defaults false");

    /* Just verify the struct compiles and the field is accessible */
    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);
    TEST_ASSERT(!dr.requires_merge);
    dr.requires_merge = true;
    TEST_ASSERT(dr.requires_merge);

    TEST_END();
}

/* ============================================================================
 * Phase C — keel_scatter_result_sort (memory path)
 * ============================================================================ */

static void test_sort_memory_int4_asc(void)
{
    TEST_BEGIN("keel_scatter_result_sort: INT4 ASC memory path");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;
    cols[0].format   = 0;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Append rows in reverse order: 5, 3, 1, 4, 2 */
    const char* vals_in[] = { "5", "3", "1", "4", "2" };
    for (int i = 0; i < 5; i++) {
        keel_scatter_col_val_t v[1] = { mkval(vals_in[i]) };
        TEST_ASSERT_EQ(keel_scatter_result_append(r, v), KEEL_OK);
    }

    keel_sort_key_t keys[1] = {{ .col_index = 0, .dir = KEEL_SORT_ASC,
                                  .nulls = KEEL_SORT_NULLS_DEFAULT }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, keys, 1), KEEL_OK);

    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* v;
    int expected[] = { 1, 2, 3, 4, 5 };
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
        char buf[8];
        memcpy(buf, v[0].data, (size_t)v[0].len);
        buf[v[0].len] = '\0';
        TEST_ASSERT_EQ(atoi(buf), expected[i]);
    }
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &v));
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_sort_memory_int4_desc(void)
{
    TEST_BEGIN("keel_scatter_result_sort: INT4 DESC memory path");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;
    cols[0].format   = 0;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    const char* vals_in[] = { "2", "5", "1", "3", "4" };
    for (int i = 0; i < 5; i++) {
        keel_scatter_col_val_t v[1] = { mkval(vals_in[i]) };
        keel_scatter_result_append(r, v);
    }

    keel_sort_key_t keys[1] = {{ .col_index = 0, .dir = KEEL_SORT_DESC,
                                  .nulls = KEEL_SORT_NULLS_DEFAULT }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, keys, 1), KEEL_OK);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    int expected[] = { 5, 4, 3, 2, 1 };
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
        char buf[8];
        memcpy(buf, v[0].data, (size_t)v[0].len);
        buf[v[0].len] = '\0';
        TEST_ASSERT_EQ(atoi(buf), expected[i]);
    }
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_sort_memory_text_multi_key(void)
{
    TEST_BEGIN("keel_scatter_result_sort: TEXT secondary key (id ASC, name ASC)");

    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "id");
    cols[0].type = KEEL_COL_TYPE_INT32;
    snprintf(cols[1].name, sizeof cols[1].name, "name");
    cols[1].type = KEEL_COL_TYPE_TEXT;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* rows: (2,"b"), (1,"z"), (1,"a"), (2,"a") */
    struct { const char* id; const char* name; } rows_in[] = {
        {"2","b"}, {"1","z"}, {"1","a"}, {"2","a"}
    };
    for (int i = 0; i < 4; i++) {
        keel_scatter_col_val_t v[2] = { mkval(rows_in[i].id), mkval(rows_in[i].name) };
        keel_scatter_result_append(r, v);
    }

    keel_sort_key_t keys[2] = {
        { .col_index = 0, .dir = KEEL_SORT_ASC, .nulls = KEEL_SORT_NULLS_DEFAULT },
        { .col_index = 1, .dir = KEEL_SORT_ASC, .nulls = KEEL_SORT_NULLS_DEFAULT },
    };
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, keys, 2), KEEL_OK);

    /* Expected: (1,"a"), (1,"z"), (2,"a"), (2,"b") */
    struct { int id; const char* name; } expected[] = {
        {1,"a"}, {1,"z"}, {2,"a"}, {2,"b"}
    };
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    for (int i = 0; i < 4; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
        char idbuf[8];
        memcpy(idbuf, v[0].data, (size_t)v[0].len);
        idbuf[v[0].len] = '\0';
        TEST_ASSERT_EQ(atoi(idbuf), expected[i].id);
        TEST_ASSERT_EQ((int)v[1].len, (int)strlen(expected[i].name));
        TEST_ASSERT(memcmp(v[1].data, expected[i].name, (size_t)v[1].len) == 0);
    }
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_sort_memory_nulls_first(void)
{
    TEST_BEGIN("keel_scatter_result_sort: NULLS FIRST respected");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_scatter_col_val_t n_null[1] = { mknull() };
    keel_scatter_col_val_t n2[1]     = { mkval("2") };
    keel_scatter_col_val_t n1[1]     = { mkval("1") };
    keel_scatter_result_append(r, n2);
    keel_scatter_result_append(r, n_null);
    keel_scatter_result_append(r, n1);

    keel_sort_key_t keys[1] = {{ .col_index = 0, .dir = KEEL_SORT_ASC,
                                  .nulls = KEEL_SORT_NULLS_FIRST }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, keys, 1), KEEL_OK);

    /* Expected: NULL, 1, 2 */
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    TEST_ASSERT_EQ(v[0].len, -1); /* NULL first */
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[8]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoi(buf), 1);
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoi(buf), 2);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_sort_empty_result(void)
{
    TEST_BEGIN("keel_scatter_result_sort: empty result is a no-op");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_sort_key_t keys[1] = {{ .col_index = 0, .dir = KEEL_SORT_ASC }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, keys, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)0);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase C — keel_scatter_result_apply_limit (memory path)
 * ============================================================================ */

static void test_limit_no_offset(void)
{
    TEST_BEGIN("keel_scatter_result_apply_limit: LIMIT 3 no offset");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 10; i++) {
        char buf[8]; snprintf(buf, sizeof buf, "%d", i);
        keel_scatter_col_val_t v[1] = { mkval(buf) };
        keel_scatter_result_append(r, v);
    }
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)10);

    keel_scatter_result_apply_limit(r, 3, 0);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    for (int i = 1; i <= 3; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
        char buf[8]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
        TEST_ASSERT_EQ(atoi(buf), i);
    }
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &v));
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_limit_with_offset(void)
{
    TEST_BEGIN("keel_scatter_result_apply_limit: LIMIT 3 OFFSET 2");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 10; i++) {
        char buf[8]; snprintf(buf, sizeof buf, "%d", i);
        keel_scatter_col_val_t v[1] = { mkval(buf) };
        keel_scatter_result_append(r, v);
    }

    keel_scatter_result_apply_limit(r, 3, 2); /* rows 3, 4, 5 */
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    int expected[] = { 3, 4, 5 };
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
        char buf[8]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
        TEST_ASSERT_EQ(atoi(buf), expected[i]);
    }
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &v));
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_limit_offset_past_end(void)
{
    TEST_BEGIN("keel_scatter_result_apply_limit: OFFSET past end of result");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 3; i++) {
        char buf[8]; snprintf(buf, sizeof buf, "%d", i);
        keel_scatter_col_val_t v[1] = { mkval(buf) };
        keel_scatter_result_append(r, v);
    }

    keel_scatter_result_apply_limit(r, 0, 100); /* offset > rows */
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)0);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_limit_zero_means_no_limit(void)
{
    TEST_BEGIN("keel_scatter_result_apply_limit: limit==0 means unlimited");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    for (int i = 1; i <= 5; i++) {
        char buf[8]; snprintf(buf, sizeof buf, "%d", i);
        keel_scatter_col_val_t v[1] = { mkval(buf) };
        keel_scatter_result_append(r, v);
    }

    keel_scatter_result_apply_limit(r, 0, 2); /* skip 2, no limit */
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3); /* rows 3, 4, 5 */
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_sort_then_limit(void)
{
    TEST_BEGIN("keel_scatter_result_sort + apply_limit: sort DESC then LIMIT 2 OFFSET 1");

    keel_scatter_col_desc_t cols[1];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "n");
    cols[0].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(1, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    const char* vs[] = { "3", "1", "5", "2", "4" };
    for (int i = 0; i < 5; i++) {
        keel_scatter_col_val_t v[1] = { mkval(vs[i]) };
        keel_scatter_result_append(r, v);
    }

    /* Sort DESC → [5, 4, 3, 2, 1] */
    keel_sort_key_t keys[1] = {{ .col_index = 0, .dir = KEEL_SORT_DESC }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, keys, 1), KEEL_OK);

    /* LIMIT 2 OFFSET 1 → [4, 3] */
    keel_scatter_result_apply_limit(r, 2, 1);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    int expected[] = { 4, 3 };
    for (int i = 0; i < 2; i++) {
        TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
        char buf[8]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
        TEST_ASSERT_EQ(atoi(buf), expected[i]);
    }
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &v));
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase C — dispatch result order_keys / norder_keys fields
 * ============================================================================ */

static void test_dispatch_result_order_fields_exist(void)
{
    TEST_BEGIN("keel_dispatch_result_t: order_keys and limit fields accessible");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    TEST_ASSERT_EQ(dr.norder_keys, (uint16_t)0);
    TEST_ASSERT_EQ(dr.limit_count, (uint64_t)0);
    TEST_ASSERT_EQ(dr.limit_offset, (uint64_t)0);

    /* Verify array is writable */
    dr.order_keys[0].col_index = 1;
    dr.order_keys[0].dir       = KEEL_SORT_DESC;
    dr.order_keys[0].nulls     = KEEL_SORT_NULLS_LAST;
    dr.norder_keys  = 1;
    dr.limit_count  = 100;
    dr.limit_offset = 10;

    TEST_ASSERT_EQ(dr.order_keys[0].col_index, (int16_t)1);
    TEST_ASSERT_EQ(dr.order_keys[0].dir,       KEEL_SORT_DESC);
    TEST_ASSERT_EQ(dr.order_keys[0].nulls,     KEEL_SORT_NULLS_LAST);
    TEST_ASSERT_EQ(dr.norder_keys,             (uint16_t)1);
    TEST_ASSERT_EQ(dr.limit_count,             (uint64_t)100);
    TEST_ASSERT_EQ(dr.limit_offset,            (uint64_t)10);

    TEST_END();
}

/* ============================================================================
 * Phase D — keel_scatter_result_merge_aggs
 * ============================================================================ */

/** Build a single-column result with the given text values (one per shard). */
static keel_scatter_result_t* make_single_col_result(keel_col_type_t col_type,
                                                  const char* const* vals,
                                                  int nvals)
{
    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "v");
    col.type   = col_type;
    col.format = KEEL_WIRE_TEXT;
    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    if (!r) return NULL;
    for (int i = 0; i < nvals; i++) {
        keel_scatter_col_val_t v[1] = { vals[i] ? mkval(vals[i]) : mknull() };
        keel_scatter_result_append(r, v);
    }
    return r;
}

static void test_merge_aggs_count(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: COUNT sum across 3 shards");

    const char* parts[] = { "5", "3", "7" };
    keel_scatter_result_t* r = make_single_col_result(KEEL_COL_TYPE_INT64, parts, 3);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_COUNT };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[16]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoll(buf), 15LL);
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &v));
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_sum_int(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: SUM integer across 3 shards");

    const char* parts[] = { "100", "200", "300" };
    keel_scatter_result_t* r = make_single_col_result(KEEL_COL_TYPE_INT64, parts, 3);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[16]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoll(buf), 600LL);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_sum_float(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: SUM float8 across 2 shards");

    const char* parts[] = { "10.5", "20.5" };
    keel_scatter_result_t* r = make_single_col_result(KEEL_COL_TYPE_FLOAT64, parts, 2);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[32]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    double result = strtod(buf, NULL);
    TEST_ASSERT(result > 30.99 && result < 31.01);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_min(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: MIN across 3 shards");

    const char* parts[] = { "50", "30", "70" };
    keel_scatter_result_t* r = make_single_col_result(KEEL_COL_TYPE_INT32, parts, 3);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_MIN };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[16]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoi(buf), 30);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_max(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: MAX across 3 shards");

    const char* parts[] = { "50", "30", "70" };
    keel_scatter_result_t* r = make_single_col_result(KEEL_COL_TYPE_INT32, parts, 3);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_MAX };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[16]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoi(buf), 70);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_mixed(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: COUNT(*) + MAX(amount) across 2 shards");

    /* 2-column result: [count, max_amount] */
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "cnt");
    cols[0].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[1].name, sizeof cols[1].name, "mx");
    cols[1].type = KEEL_COL_TYPE_INT32;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Shard 1: count=500, max=99 */
    keel_scatter_col_val_t row1[2] = { mkval("500"), mkval("99") };
    keel_scatter_result_append(r, row1);

    /* Shard 2: count=300, max=149 */
    keel_scatter_col_val_t row2[2] = { mkval("300"), mkval("149") };
    keel_scatter_result_append(r, row2);

    keel_agg_col_spec_t specs[2] = {
        { .col_index = 0, .func = KEEL_AGG_COUNT },
        { .col_index = 1, .func = KEEL_AGG_MAX   },
    };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, specs, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char b0[16], b1[16];
    memcpy(b0, v[0].data, (size_t)v[0].len); b0[v[0].len] = '\0';
    memcpy(b1, v[1].data, (size_t)v[1].len); b1[v[1].len] = '\0';
    TEST_ASSERT_EQ(atoll(b0), 800LL);
    TEST_ASSERT_EQ(atoi(b1), 149);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_null_inputs(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: NULL from one shard — SUM skips NULL");

    /* 3 shards: NULL, 200, 300 */
    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "s");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_scatter_col_val_t null_v[1] = { mknull() };
    keel_scatter_col_val_t v200[1]   = { mkval("200") };
    keel_scatter_col_val_t v300[1]   = { mkval("300") };
    keel_scatter_result_append(r, null_v);
    keel_scatter_result_append(r, v200);
    keel_scatter_result_append(r, v300);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[16]; memcpy(buf, v[0].data, (size_t)v[0].len); buf[v[0].len] = '\0';
    TEST_ASSERT_EQ(atoll(buf), 500LL);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_all_null_sum(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: all-NULL SUM produces NULL result");

    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "s");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_scatter_col_val_t nv[1] = { mknull() };
    keel_scatter_result_append(r, nv);
    keel_scatter_result_append(r, nv);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_SUM };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    TEST_ASSERT_EQ(v[0].len, -1); /* NULL */
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_avg_rejected(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: KEEL_AGG_AVG returns NOT_SUPPORTED");

    const char* parts[] = { "10.5" };
    keel_scatter_result_t* r = make_single_col_result(KEEL_COL_TYPE_FLOAT64, parts, 1);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_AVG };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1),
                   KEEL_ERR_NOT_SUPPORTED);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_empty_result(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: empty result is a no-op");

    keel_scatter_col_desc_t col;
    memset(&col, 0, sizeof col);
    snprintf(col.name, sizeof col.name, "n");
    col.type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(1, &col, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_agg_col_spec_t spec = { .col_index = 0, .func = KEEL_AGG_COUNT };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, &spec, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)0);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_merge_aggs_passthrough(void)
{
    TEST_BEGIN("keel_scatter_result_merge_aggs: NONE col keeps first shard's value");

    /* 2-column: [label TEXT (NONE), count INT8 (COUNT)] */
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "lbl");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64 * 1024 * 1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_scatter_col_val_t row1[2] = { mkval("hello"), mkval("10") };
    keel_scatter_col_val_t row2[2] = { mkval("world"), mkval("20") };
    keel_scatter_result_append(r, row1);
    keel_scatter_result_append(r, row2);

    keel_agg_col_spec_t specs[1] = {
        { .col_index = 1, .func = KEEL_AGG_COUNT },
    };
    TEST_ASSERT_EQ(keel_scatter_result_merge_aggs(r, specs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    /* label: first shard's "hello" */
    TEST_ASSERT_EQ((int)v[0].len, 5);
    TEST_ASSERT(memcmp(v[0].data, "hello", 5) == 0);
    /* count: 10+20 = 30 */
    char buf[16]; memcpy(buf, v[1].data, (size_t)v[1].len); buf[v[1].len] = '\0';
    TEST_ASSERT_EQ(atoll(buf), 30LL);
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase D — dispatch result agg fields exist
 * ============================================================================ */

static void test_dispatch_agg_fields_exist(void)
{
    TEST_BEGIN("keel_dispatch_result_t: nagg_specs, agg_specs, requires_avg_rewrite");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    TEST_ASSERT_EQ(dr.nagg_specs, (uint16_t)0);
    TEST_ASSERT(!dr.requires_avg_rewrite);

    dr.agg_specs[0].col_index = 2;
    dr.agg_specs[0].func      = KEEL_AGG_SUM;
    dr.nagg_specs             = 1;
    dr.requires_avg_rewrite   = true;

    TEST_ASSERT_EQ(dr.agg_specs[0].col_index, (int16_t)2);
    TEST_ASSERT_EQ(dr.agg_specs[0].func,      KEEL_AGG_SUM);
    TEST_ASSERT_EQ(dr.nagg_specs,             (uint16_t)1);
    TEST_ASSERT(dr.requires_avg_rewrite);

    TEST_END();
}

/* ============================================================================
 * Phase E — keel_scatter_result_group_aggs
 * ============================================================================ */

typedef struct { const char* key; const char* val; } group_kv_t;

/**
 * Build a 2-column result: col0 = key (TEXT), col1 = value (INT8).
 */
static keel_scatter_result_t* make_group_result(const group_kv_t* rows_in, int n)
{
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64*1024*1024, NULL);
    if (!r) return NULL;

    for (int i = 0; i < n; i++) {
        keel_scatter_col_val_t v[2] = {
            rows_in[i].key ? mkval(rows_in[i].key) : mknull(),
            rows_in[i].val ? mkval(rows_in[i].val) : mknull(),
        };
        keel_scatter_result_append(r, v);
    }
    return r;
}

/** Collect all output rows into an array; caller frees each row's key/val strings. */
typedef struct { char key[64]; char val[32]; bool null_key; bool null_val; } group_row_t;

static int collect_group_rows(keel_scatter_result_t* r, group_row_t* out, int cap)
{
    keel_scatter_result_iter_t it;
    if (keel_scatter_result_iter_init(&it, r) != KEEL_OK) return -1;
    const keel_scatter_col_val_t* v;
    int n = 0;
    while (n < cap && keel_scatter_result_iter_next(&it, &v)) {
        if (v[0].len < 0) {
            out[n].null_key = true;
            out[n].key[0] = '\0';
        } else {
            out[n].null_key = false;
            size_t l = v[0].len < 63 ? (size_t)v[0].len : 63;
            memcpy(out[n].key, v[0].data, l); out[n].key[l] = '\0';
        }
        if (v[1].len < 0) {
            out[n].null_val = true;
            out[n].val[0] = '\0';
        } else {
            out[n].null_val = false;
            size_t l = v[1].len < 31 ? (size_t)v[1].len : 31;
            memcpy(out[n].val, v[1].data, l); out[n].val[l] = '\0';
        }
        n++;
    }
    keel_scatter_result_iter_close(&it);
    return n;
}

/** Find a row with key == target_key in out[0..n-1]. Returns index or -1. */
static int find_group_row(const group_row_t* out, int n, const char* key)
{
    for (int i = 0; i < n; i++)
        if (!out[i].null_key && strcmp(out[i].key, key) == 0) return i;
    return -1;
}

static void test_group_single_key_count(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: GROUP BY TEXT, COUNT — 3 shards, 2 groups");

    /* Shards: A=10, B=20, A=30 (shard-level partial COUNTs) */
    group_kv_t rows[] = {
        {"A", "10"}, {"B", "20"}, {"A", "30"}
    };
    keel_scatter_result_t* r = make_group_result(rows, 3);
    TEST_ASSERT(r != NULL);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_COUNT }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    group_row_t out[4];
    int n = collect_group_rows(r, out, 4);
    TEST_ASSERT_EQ(n, 2);

    int ia = find_group_row(out, n, "A");
    int ib = find_group_row(out, n, "B");
    TEST_ASSERT(ia >= 0);
    TEST_ASSERT(ib >= 0);
    TEST_ASSERT_EQ(atoll(out[ia].val), 40LL); /* 10 + 30 */
    TEST_ASSERT_EQ(atoll(out[ib].val), 20LL);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_single_key_sum(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: GROUP BY INT4, SUM(amount)");

    /* 3-column: [id INT4, amount INT8, label TEXT] */
    keel_scatter_col_desc_t cols[3];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "id");
    cols[0].type = KEEL_COL_TYPE_INT32;
    snprintf(cols[1].name, sizeof cols[1].name, "amount");
    cols[1].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[2].name, sizeof cols[2].name, "lbl");
    cols[2].type = KEEL_COL_TYPE_TEXT;

    keel_scatter_result_t* r = keel_scatter_result_create(3, cols, 64*1024*1024, NULL);
    TEST_ASSERT(r != NULL);

    /* Rows: (1, 100, "x"), (2, 200, "y"), (1, 300, "x") */
    struct { const char* id; const char* amt; const char* lbl; } rows_in[] = {
        {"1","100","x"}, {"2","200","y"}, {"1","300","x"}
    };
    for (int i = 0; i < 3; i++) {
        keel_scatter_col_val_t v[3] = {
            mkval(rows_in[i].id), mkval(rows_in[i].amt), mkval(rows_in[i].lbl)
        };
        keel_scatter_result_append(r, v);
    }

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_SUM }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    /* Collect by id */
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    long long sum1 = 0, sum2 = 0;
    while (keel_scatter_result_iter_next(&it, &v)) {
        char id_buf[8], amt_buf[16];
        memcpy(id_buf,  v[0].data, (size_t)v[0].len); id_buf[v[0].len]   = '\0';
        memcpy(amt_buf, v[1].data, (size_t)v[1].len); amt_buf[v[1].len] = '\0';
        if (atoi(id_buf) == 1) sum1 = atoll(amt_buf);
        else                   sum2 = atoll(amt_buf);
    }
    keel_scatter_result_iter_close(&it);
    TEST_ASSERT_EQ(sum1, 400LL); /* 100 + 300 */
    TEST_ASSERT_EQ(sum2, 200LL);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_multi_key(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: GROUP BY dept+region (2 keys), COUNT");

    /* 3-column: [dept TEXT, region TEXT, cnt INT8] */
    keel_scatter_col_desc_t cols[3];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "dept");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "region");
    cols[1].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[2].name, sizeof cols[2].name, "cnt");
    cols[2].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(3, cols, 64*1024*1024, NULL);
    TEST_ASSERT(r != NULL);

    struct { const char* d; const char* rg; const char* c; } rows_in[] = {
        {"eng","us","5"}, {"mkt","eu","3"}, {"eng","us","7"}, {"mkt","eu","2"}
    };
    for (int i = 0; i < 4; i++) {
        keel_scatter_col_val_t v[3] = {
            mkval(rows_in[i].d), mkval(rows_in[i].rg), mkval(rows_in[i].c)
        };
        keel_scatter_result_append(r, v);
    }

    keel_group_col_spec_t gkeys[2] = {{ .col_index = 0 }, { .col_index = 1 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 2, .func = KEEL_AGG_COUNT }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 2, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    /* Verify: eng/us=12, mkt/eu=5 */
    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    long long eng_cnt = 0, mkt_cnt = 0;
    while (keel_scatter_result_iter_next(&it, &v)) {
        char db[16], cb[16];
        memcpy(db, v[0].data, (size_t)v[0].len); db[v[0].len] = '\0';
        memcpy(cb, v[2].data, (size_t)v[2].len); cb[v[2].len] = '\0';
        if (strcmp(db, "eng") == 0) eng_cnt = atoll(cb);
        else                         mkt_cnt = atoll(cb);
    }
    keel_scatter_result_iter_close(&it);
    TEST_ASSERT_EQ(eng_cnt, 12LL); /* 5 + 7 */
    TEST_ASSERT_EQ(mkt_cnt, 5LL);  /* 3 + 2 */

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_null_key_forms_own_group(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: NULL GROUP BY key forms its own group");

    group_kv_t rows[] = {
        {NULL, "10"}, {"A", "20"}, {NULL, "30"}
    };
    keel_scatter_result_t* r = make_group_result(rows, 3);
    TEST_ASSERT(r != NULL);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_SUM }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2); /* NULL group + "A" */

    group_row_t out[4];
    int n = collect_group_rows(r, out, 4);
    TEST_ASSERT_EQ(n, 2);

    /* Find NULL group */
    int null_idx = -1, a_idx = -1;
    for (int i = 0; i < n; i++) {
        if (out[i].null_key) null_idx = i;
        else if (strcmp(out[i].key, "A") == 0) a_idx = i;
    }
    TEST_ASSERT(null_idx >= 0);
    TEST_ASSERT(a_idx   >= 0);
    TEST_ASSERT_EQ(atoll(out[null_idx].val), 40LL); /* 10 + 30 */
    TEST_ASSERT_EQ(atoll(out[a_idx].val),   20LL);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_single_group(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: all rows same group → 1 output row");

    group_kv_t rows[] = {
        {"X","10"}, {"X","20"}, {"X","30"}
    };
    keel_scatter_result_t* r = make_group_result(rows, 3);
    TEST_ASSERT(r != NULL);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_SUM }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    char buf[16]; memcpy(buf, v[1].data, (size_t)v[1].len); buf[v[1].len] = '\0';
    TEST_ASSERT_EQ(atoll(buf), 60LL);
    TEST_ASSERT(!keel_scatter_result_iter_next(&it, &v));
    keel_scatter_result_iter_close(&it);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_empty_result(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: empty input is a no-op");

    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "k"); cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "v"); cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64*1024*1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_COUNT }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)0);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_avg_rejected(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs: KEEL_AGG_AVG returns NOT_SUPPORTED");

    group_kv_t rows[] = { {"A","5"} };
    keel_scatter_result_t* r = make_group_result(rows, 1);
    TEST_ASSERT(r != NULL);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_AVG }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1),
                   KEEL_ERR_NOT_SUPPORTED);
    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_then_sort(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs + sort: groups sorted by key ASC");

    group_kv_t rows[] = {
        {"C","1"}, {"A","2"}, {"B","3"}, {"A","4"}, {"C","5"}
    };
    keel_scatter_result_t* r = make_group_result(rows, 5);
    TEST_ASSERT(r != NULL);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_SUM }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3);

    /* Sort by group key (col 0) ASC */
    keel_sort_key_t sk[1] = {{ .col_index = 0, .dir = KEEL_SORT_ASC }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, sk, 1), KEEL_OK);

    group_row_t out[4];
    int n = collect_group_rows(r, out, 4);
    TEST_ASSERT_EQ(n, 3);
    TEST_ASSERT(strcmp(out[0].key, "A") == 0);
    TEST_ASSERT(strcmp(out[1].key, "B") == 0);
    TEST_ASSERT(strcmp(out[2].key, "C") == 0);
    TEST_ASSERT_EQ(atoll(out[0].val), 6LL);  /* 2+4 */
    TEST_ASSERT_EQ(atoll(out[1].val), 3LL);
    TEST_ASSERT_EQ(atoll(out[2].val), 6LL);  /* 1+5 */

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase E — spill-path stress tests for keel_scatter_result_group_aggs
 *
 * These tests force the scatter store to spill to disk before calling
 * group_aggs so that the iterator's disk-read path is exercised during
 * the first aggregation pass, and the spill-rewrite path during the second.
 * ============================================================================ */

/**
 * Build a spilled 3-column result (grp TEXT, val INT8, pad TEXT) with N rows
 * where grp = group_names[i % ngroups] and val = (i%7)+1.  The 512-byte pad
 * column forces spill to disk after ~1800 rows with KEEL_SCATTER_MIN_MEM_LIMIT_BYTES.
 * Returns NULL on failure.
 */
static keel_scatter_result_t* make_spilled_group_result(
    const char** group_names, int ngroups, int total_rows)
{
    keel_scatter_col_desc_t cols[3];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "val");
    cols[1].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[2].name, sizeof cols[2].name, "pad");
    cols[2].type = KEEL_COL_TYPE_TEXT;

    /* 512-byte pad column makes each row ~578 bytes; spill triggers after
     * ~1800 rows even with the 1 MiB minimum memory limit */
    keel_scatter_result_t* r = keel_scatter_result_create(3, cols,
                                                  KEEL_SCATTER_MIN_MEM_LIMIT_BYTES,
                                                  "/tmp");
    if (!r) return NULL;

    char pad_buf[513];
    memset(pad_buf, 'X', 512);
    pad_buf[512] = '\0';

    for (int i = 0; i < total_rows; i++) {
        const char* key = group_names[i % ngroups];
        char val_buf[32];
        snprintf(val_buf, sizeof val_buf, "%d", (i % 7) + 1);
        keel_scatter_col_val_t v[3] = {
            mkval(key),
            mkval(val_buf),
            { .len = 512, .data = pad_buf },
        };
        if (keel_scatter_result_append(r, v) != KEEL_OK) {
            keel_scatter_result_destroy(r);
            return NULL;
        }
    }
    return r;
}

static void test_group_spill_count(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs spill: COUNT aggregation on spilled input");

    const char* groups[] = {"alpha", "beta", "gamma"};
    const int   total    = 3000; /* enough rows to overflow the 1 MiB limit */
    keel_scatter_result_t* r = make_spilled_group_result(groups, 3, total);
    TEST_ASSERT(r != NULL);
    TEST_ASSERT(keel_scatter_result_spilled(r));
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)total);

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_COUNT }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3);

    /* Each group has total/3 = 1000 rows */
    group_row_t out[4];
    int n = collect_group_rows(r, out, 4);
    TEST_ASSERT_EQ(n, 3);

    /* KEEL_AGG_COUNT sums the val column (same path as SUM).
     * Each val = (i%7)+1 and groups cycle in round-robin, so compute
     * the correct per-group expected sum. */
    long long expected_sum[3] = {0, 0, 0};
    for (int j = 0; j < total; j++) expected_sum[j % 3] += (j % 7) + 1;

    for (int i = 0; i < n; i++) {
        long long got = atoll(out[i].val);
        int g = -1;
        for (int j = 0; j < 3; j++)
            if (strcmp(out[i].key, groups[j]) == 0) { g = j; break; }
        TEST_ASSERT(g >= 0);
        TEST_ASSERT_EQ(got, expected_sum[g]);
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_spill_sum(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs spill: SUM aggregation on spilled input");

    const char* groups[] = {"X", "Y"};
    /* 2000 rows: 1000 per group.  Value for row i = (i % 7 + 1).
     * Sum for group "X" (even i): sum of (i%7)+1 for i in 0,2,4,...,1998
     * Sum for group "Y" (odd  i): sum of (i%7)+1 for i in 1,3,5,...,1999 */
    const int total = 2000;
    keel_scatter_result_t* r = make_spilled_group_result(groups, 2, total);
    TEST_ASSERT(r != NULL);
    TEST_ASSERT(keel_scatter_result_spilled(r));

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_SUM }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    group_row_t out[3];
    int n = collect_group_rows(r, out, 3);
    TEST_ASSERT_EQ(n, 2);

    /* Compute expected sums independently */
    long long exp_x = 0, exp_y = 0;
    for (int i = 0; i < total; i++) {
        long long v = (i % 7) + 1;
        if (i % 2 == 0) exp_x += v; else exp_y += v;
    }

    int ix = find_group_row(out, n, "X");
    int iy = find_group_row(out, n, "Y");
    TEST_ASSERT(ix >= 0);
    TEST_ASSERT(iy >= 0);
    TEST_ASSERT_EQ(atoll(out[ix].val), exp_x);
    TEST_ASSERT_EQ(atoll(out[iy].val), exp_y);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_spill_min_max(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs spill: MIN and MAX across groups");

    /* 4-column result: grp TEXT, lo INT8 (min), hi INT8 (max), pad TEXT
     * 512-byte pad column forces spill after ~1500 rows. */
    keel_scatter_col_desc_t cols[4];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "lo");
    cols[1].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[2].name, sizeof cols[2].name, "hi");
    cols[2].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[3].name, sizeof cols[3].name, "pad");
    cols[3].type = KEEL_COL_TYPE_TEXT;

    keel_scatter_result_t* r = keel_scatter_result_create(4, cols,
                                                  KEEL_SCATTER_MIN_MEM_LIMIT_BYTES,
                                                  "/tmp");
    TEST_ASSERT(r != NULL);

    char pad_buf[513];
    memset(pad_buf, 'X', 512);
    pad_buf[512] = '\0';

    /* 4000 rows: group = "even" or "odd", lo = hi = i */
    const int N = 4000;
    for (int i = 0; i < N; i++) {
        char grp[8], num[16];
        snprintf(grp, sizeof grp, "%s", (i % 2 == 0) ? "even" : "odd");
        snprintf(num, sizeof num, "%d", i);
        keel_scatter_col_val_t v[4] = {
            mkval(grp), mkval(num), mkval(num),
            { .len = 512, .data = pad_buf },
        };
        TEST_ASSERT_EQ(keel_scatter_result_append(r, v), KEEL_OK);
    }
    TEST_ASSERT(keel_scatter_result_spilled(r));

    keel_group_col_spec_t gkeys[1]  = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[2] = {
        { .col_index = 1, .func = KEEL_AGG_MIN },
        { .col_index = 2, .func = KEEL_AGG_MAX },
    };
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 2), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)2);

    /* Collect: find "even" and "odd" rows */
    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* v;
    long long even_min = -1, even_max = -1, odd_min = -1, odd_max = -1;
    while (keel_scatter_result_iter_next(&it, &v)) {
        char grp[8], lo_s[16], hi_s[16];
        size_t gl = v[0].len < 7  ? (size_t)v[0].len : 7;
        size_t ll = v[1].len < 15 ? (size_t)v[1].len : 15;
        size_t hl = v[2].len < 15 ? (size_t)v[2].len : 15;
        memcpy(grp,  v[0].data, gl); grp[gl]  = '\0';
        memcpy(lo_s, v[1].data, ll); lo_s[ll] = '\0';
        memcpy(hi_s, v[2].data, hl); hi_s[hl] = '\0';
        if (strcmp(grp, "even") == 0) { even_min = atoll(lo_s); even_max = atoll(hi_s); }
        else                          { odd_min  = atoll(lo_s); odd_max  = atoll(hi_s); }
    }
    keel_scatter_result_iter_close(&it);

    /* even: 0,2,4,...,3998 → min=0, max=3998
     * odd:  1,3,5,...,3999 → min=1, max=3999 */
    TEST_ASSERT_EQ(even_min, 0LL);
    TEST_ASSERT_EQ(even_max, 3998LL);
    TEST_ASSERT_EQ(odd_min,  1LL);
    TEST_ASSERT_EQ(odd_max,  3999LL);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_spill_many_groups(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs spill: many groups, output also spills");

    /* 200 distinct group keys, 4000 total rows → 20 rows per group.
     * Each row counted: expected COUNT per group = 20. */
#define NGROUPS 200
#define NROWS   4000
    char group_bufs[NGROUPS][16];
    const char* group_ptrs[NGROUPS];
    for (int g = 0; g < NGROUPS; g++) {
        snprintf(group_bufs[g], sizeof group_bufs[g], "grp%03d", g);
        group_ptrs[g] = group_bufs[g];
    }

    /* make_spilled_group_result builds a 3-col result with 512-byte padding;
     * 4000 rows well exceed the ~1800-row spill threshold */
    keel_scatter_result_t* r = make_spilled_group_result(
        (const char**)group_ptrs, NGROUPS, NROWS);
    TEST_ASSERT(r != NULL);
    TEST_ASSERT(keel_scatter_result_spilled(r));

    keel_group_col_spec_t gkeys[1]  = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_COUNT }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)NGROUPS);

    /* Every group must have the correct sum of (i%7+1) values.
     * KEEL_AGG_COUNT = SUM of values; compute expected per group. */
    long long expected_sums[NGROUPS];
    for (int g = 0; g < NGROUPS; g++) expected_sums[g] = 0;
    for (int j = 0; j < NROWS; j++) expected_sums[j % NGROUPS] += (j % 7) + 1;

    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* v;
    int checked = 0;
    while (keel_scatter_result_iter_next(&it, &v)) {
        char key[16], val[16];
        size_t kl = v[0].len < 15 ? (size_t)v[0].len : 15;
        size_t vl = v[1].len < 15 ? (size_t)v[1].len : 15;
        memcpy(key, v[0].data, kl); key[kl] = '\0';
        memcpy(val, v[1].data, vl); val[vl] = '\0';
        /* Recover group index from key name "grpNNN" */
        int g = atoi(key + 3);
        TEST_ASSERT_EQ(atoll(val), expected_sums[g]);
        checked++;
    }
    keel_scatter_result_iter_close(&it);
    TEST_ASSERT_EQ(checked, NGROUPS);
#undef NGROUPS
#undef NROWS

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_spill_then_sort(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs spill + sort: output groups sorted correctly");

    const char* groups[] = {"cherry", "apple", "banana", "date", "elderberry"};
    const int ngroups = 5;
    const int total = 5000; /* 1000 rows per group; spill triggers after ~1800 */

    keel_scatter_result_t* r = make_spilled_group_result(groups, ngroups, total);
    TEST_ASSERT(r != NULL);
    TEST_ASSERT(keel_scatter_result_spilled(r));

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_COUNT }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)ngroups);

    /* Sort by group key (TEXT col 0) ASC */
    keel_sort_key_t sk[1] = {{ .col_index = 0, .dir = KEEL_SORT_ASC }};
    TEST_ASSERT_EQ(keel_scatter_result_sort(r, sk, 1), KEEL_OK);

    /* Expected sorted order: apple, banana, cherry, date, elderberry */
    const char* expected_order[] = {"apple", "banana", "cherry", "date", "elderberry"};
    group_row_t out[6];
    int n = collect_group_rows(r, out, 6);
    TEST_ASSERT_EQ(n, ngroups);

    for (int i = 0; i < n; i++) {
        TEST_ASSERT(strcmp(out[i].key, expected_order[i]) == 0);
        /* Find this group's index in groups[] to compute expected SUM */
        int g = -1;
        for (int j = 0; j < ngroups; j++)
            if (strcmp(groups[j], expected_order[i]) == 0) { g = j; break; }
        TEST_ASSERT(g >= 0);
        /* KEEL_AGG_COUNT = SUM of (i%7+1) for rows where i%ngroups == g */
        long long expected_val = 0;
        for (int j = g; j < total; j += ngroups)
            expected_val += (j % 7) + 1;
        TEST_ASSERT_EQ(atoll(out[i].val), expected_val);
    }

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_group_spill_null_keys(void)
{
    TEST_BEGIN("keel_scatter_result_group_aggs spill: NULL keys form own group on spilled input");

    /* 3-col result: grp TEXT (nullable), val INT8, pad TEXT (512 bytes)
     * Interleave NULL, "A", "B" rows so the spill triggers mid-append. */
    keel_scatter_col_desc_t cols[3];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "grp");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "val");
    cols[1].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[2].name, sizeof cols[2].name, "pad");
    cols[2].type = KEEL_COL_TYPE_TEXT;

    keel_scatter_result_t* r = keel_scatter_result_create(3, cols,
                                                  KEEL_SCATTER_MIN_MEM_LIMIT_BYTES,
                                                  "/tmp");
    TEST_ASSERT(r != NULL);

    char pad_buf[513];
    memset(pad_buf, 'X', 512);
    pad_buf[512] = '\0';

    /* 4000 rows interleaved: i%3==0 → NULL key (val=1),
     *                        i%3==1 → "A" (val=2),
     *                        i%3==2 → "B" (val=3) */
    for (int i = 0; i < 4000; i++) {
        keel_scatter_col_val_t v[3];
        v[2].len  = 512;
        v[2].data = pad_buf;
        if (i % 3 == 0) {
            v[0] = mknull();
            v[1] = mkval("1");
        } else if (i % 3 == 1) {
            v[0] = mkval("A");
            v[1] = mkval("2");
        } else {
            v[0] = mkval("B");
            v[1] = mkval("3");
        }
        TEST_ASSERT_EQ(keel_scatter_result_append(r, v), KEEL_OK);
    }
    TEST_ASSERT(keel_scatter_result_spilled(r));

    keel_group_col_spec_t gkeys[1] = {{ .col_index = 0 }};
    keel_agg_col_spec_t   aspecs[1] = {{ .col_index = 1, .func = KEEL_AGG_SUM }};
    TEST_ASSERT_EQ(keel_scatter_result_group_aggs(r, gkeys, 1, aspecs, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), (size_t)3);

    /* Expected sums: NULL group sums val=1 for each i%3==0 row,
     *               "A" group sums val=2 for each i%3==1 row,
     *               "B" group sums val=3 for each i%3==2 row */
    long long exp_null = 0, exp_a = 0, exp_b = 0;
    for (int i = 0; i < 4000; i++) {
        if      (i % 3 == 0) exp_null += 1;
        else if (i % 3 == 1) exp_a    += 2;
        else                 exp_b    += 3;
    }

    group_row_t out[4];
    int n = collect_group_rows(r, out, 4);
    TEST_ASSERT_EQ(n, 3);

    bool found_null = false, found_a = false, found_b = false;
    for (int i = 0; i < n; i++) {
        if (out[i].null_key) {
            found_null = true;
            TEST_ASSERT_EQ(atoll(out[i].val), exp_null);
        } else if (strcmp(out[i].key, "A") == 0) {
            found_a = true;
            TEST_ASSERT_EQ(atoll(out[i].val), exp_a);
        } else if (strcmp(out[i].key, "B") == 0) {
            found_b = true;
            TEST_ASSERT_EQ(atoll(out[i].val), exp_b);
        }
    }
    TEST_ASSERT(found_null);
    TEST_ASSERT(found_a);
    TEST_ASSERT(found_b);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase E — dispatch result group_key_cols fields
 * ============================================================================ */

static void test_dispatch_group_fields_exist(void)
{
    TEST_BEGIN("keel_dispatch_result_t: group_key_cols and ngroup_key_cols");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    TEST_ASSERT_EQ(dr.ngroup_key_cols, (uint16_t)0);

    dr.group_key_cols[0].col_index = 3;
    dr.group_key_cols[1].col_index = 5;
    dr.ngroup_key_cols = 2;

    TEST_ASSERT_EQ(dr.group_key_cols[0].col_index, (int16_t)3);
    TEST_ASSERT_EQ(dr.group_key_cols[1].col_index, (int16_t)5);
    TEST_ASSERT_EQ(dr.ngroup_key_cols, (uint16_t)2);

    TEST_END();
}

/* ============================================================================
 * Phase F — window function flag
 * ============================================================================ */

static void test_dispatch_window_flag_default_false(void)
{
    TEST_BEGIN("keel_dispatch_result_t: has_window_funcs defaults to false");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    TEST_ASSERT(!dr.has_window_funcs);

    TEST_END();
}

static void test_dispatch_window_flag_set(void)
{
    TEST_BEGIN("keel_dispatch_result_t: has_window_funcs can be set independently");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    dr.has_window_funcs = true;

    TEST_ASSERT(dr.has_window_funcs);
    /* Setting has_window_funcs must not affect requires_merge */
    TEST_ASSERT(!dr.requires_merge);
    /* Setting has_window_funcs must not corrupt adjacent fields */
    TEST_ASSERT_EQ(dr.ngroup_key_cols, (uint16_t)0);
    TEST_ASSERT_EQ(dr.nagg_specs, (uint16_t)0);

    TEST_END();
}

static void test_dispatch_window_does_not_set_requires_merge(void)
{
    TEST_BEGIN("keel_dispatch_result_t: window flag independent from requires_merge");

    /* A query can have has_window_funcs=true and requires_merge=false at the
     * same time — the proxy must not attempt scatter-merge for window queries. */
    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    dr.has_window_funcs = true;
    TEST_ASSERT(dr.has_window_funcs);
    TEST_ASSERT(!dr.requires_merge);

    /* Verify that requires_merge can still be set independently */
    dr.requires_merge = true;
    TEST_ASSERT(dr.has_window_funcs);
    TEST_ASSERT(dr.requires_merge);

    TEST_END();
}

/* ============================================================================
 * Phase H — keel_scatter_result_apply_having
 * ============================================================================ */

/**
 * Build a 2-column result: col0 = label (TEXT), col1 = count (INT8 as text).
 */
static keel_scatter_result_t* make_having_result(void)
{
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "label");
    cols[0].type = KEEL_COL_TYPE_TEXT;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64*1024*1024, NULL);
    if (!r) return NULL;

    /* rows: A=3, B=7, C=5, D=10, E=1 */
    const char* keys[] = {"A","B","C","D","E"};
    const char* vals[] = {"3","7","5","10","1"};
    for (int i = 0; i < 5; i++) {
        keel_scatter_col_val_t v[2] = { mkval(keys[i]), mkval(vals[i]) };
        keel_scatter_result_append(r, v);
    }
    return r;
}

static void test_apply_having_basic(void)
{
    TEST_BEGIN("keel_scatter_result_apply_having: col1 > 5 keeps B=7 and D=10");

    keel_scatter_result_t* r = make_having_result();
    TEST_ASSERT(r != NULL);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 5);

    keel_having_pred_t pred;
    memset(&pred, 0, sizeof pred);
    pred.col_index = 1;
    pred.op        = KEEL_CMP_GT;
    snprintf(pred.literal, sizeof pred.literal, "5");
    pred.literal_len = 1;

    keel_error_t err = keel_scatter_result_apply_having(r, &pred, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 2); /* B=7, D=10 */

    /* Verify the surviving rows are B and D */
    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* v;
    int found_b = 0, found_d = 0;
    while (keel_scatter_result_iter_next(&it, &v)) {
        if (v[0].len == 1 && v[0].data[0] == 'B') found_b = 1;
        if (v[0].len == 1 && v[0].data[0] == 'D') found_d = 1;
    }
    keel_scatter_result_iter_close(&it);
    TEST_ASSERT(found_b);
    TEST_ASSERT(found_d);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_apply_having_eq(void)
{
    TEST_BEGIN("keel_scatter_result_apply_having: col1 = 5 keeps only C=5");

    keel_scatter_result_t* r = make_having_result();
    TEST_ASSERT(r != NULL);

    keel_having_pred_t pred;
    memset(&pred, 0, sizeof pred);
    pred.col_index = 1;
    pred.op        = KEEL_CMP_EQ;
    snprintf(pred.literal, sizeof pred.literal, "5");
    pred.literal_len = 1;

    keel_error_t err = keel_scatter_result_apply_having(r, &pred, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 1);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* v;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &v));
    TEST_ASSERT(v[0].len == 1 && v[0].data[0] == 'C');
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_apply_having_no_match(void)
{
    TEST_BEGIN("keel_scatter_result_apply_having: no rows pass predicate → empty result");

    keel_scatter_result_t* r = make_having_result();
    TEST_ASSERT(r != NULL);

    keel_having_pred_t pred;
    memset(&pred, 0, sizeof pred);
    pred.col_index = 1;
    pred.op        = KEEL_CMP_GT;
    snprintf(pred.literal, sizeof pred.literal, "99");
    pred.literal_len = 2;

    keel_error_t err = keel_scatter_result_apply_having(r, &pred, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 0);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_apply_having_empty_preds(void)
{
    TEST_BEGIN("keel_scatter_result_apply_having: zero predicates keeps all rows");

    keel_scatter_result_t* r = make_having_result();
    TEST_ASSERT(r != NULL);

    keel_error_t err = keel_scatter_result_apply_having(r, NULL, 0);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 5);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase H — keel_scatter_result_finalize_avg
 * ============================================================================ */

/**
 * Build a 2-column result: col0 = SUM (numeric text), col1 = COUNT (int text).
 */
static keel_scatter_result_t* make_avg_result(void)
{
    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "sum_x");
    cols[0].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt_x");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64*1024*1024, NULL);
    if (!r) return NULL;

    /* rows: (30, 3) → avg=10, (100, 4) → avg=25, (0, 5) → avg=0 */
    const char* sums[]   = {"30", "100", "0"};
    const char* counts[] = {"3",   "4",  "5"};
    for (int i = 0; i < 3; i++) {
        keel_scatter_col_val_t v[2] = { mkval(sums[i]), mkval(counts[i]) };
        keel_scatter_result_append(r, v);
    }
    return r;
}

static void test_finalize_avg_basic(void)
{
    TEST_BEGIN("keel_scatter_result_finalize_avg: SUM/COUNT cols → AVG in-place");

    keel_scatter_result_t* r = make_avg_result();
    TEST_ASSERT(r != NULL);
    TEST_ASSERT_EQ(keel_scatter_result_row_count(r), 3);

    keel_avg_finalize_spec_t spec;
    spec.sum_col   = 0;
    spec.count_col = 1;
    spec.out_col   = 0;

    keel_error_t err = keel_scatter_result_finalize_avg(r, &spec, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Verify AVG values: 30/3=10, 100/4=25, 0/5=0 */
    double expected[] = {10.0, 25.0, 0.0};
    keel_scatter_result_iter_t it;
    TEST_ASSERT_EQ(keel_scatter_result_iter_init(&it, r), KEEL_OK);
    const keel_scatter_col_val_t* v;
    int idx = 0;
    while (keel_scatter_result_iter_next(&it, &v)) {
        TEST_ASSERT(v[0].len > 0);
        char buf[64];
        size_t l = (size_t)v[0].len < sizeof buf - 1 ? (size_t)v[0].len : sizeof buf - 1;
        memcpy(buf, v[0].data, l);
        buf[l] = '\0';
        double got = strtod(buf, NULL);
        TEST_ASSERT(got == expected[idx]);
        idx++;
    }
    keel_scatter_result_iter_close(&it);
    TEST_ASSERT_EQ(idx, 3);

    keel_scatter_result_destroy(r);
    TEST_END();
}

static void test_finalize_avg_null_count(void)
{
    TEST_BEGIN("keel_scatter_result_finalize_avg: NULL count → NULL avg");

    keel_scatter_col_desc_t cols[2];
    memset(cols, 0, sizeof cols);
    snprintf(cols[0].name, sizeof cols[0].name, "sum_x");
    cols[0].type = KEEL_COL_TYPE_INT64;
    snprintf(cols[1].name, sizeof cols[1].name, "cnt_x");
    cols[1].type = KEEL_COL_TYPE_INT64;

    keel_scatter_result_t* r = keel_scatter_result_create(2, cols, 64*1024*1024, NULL);
    TEST_ASSERT(r != NULL);

    keel_scatter_col_val_t v[2] = { mkval("50"), mknull() };
    keel_scatter_result_append(r, v);

    keel_avg_finalize_spec_t spec = { .sum_col=0, .count_col=1, .out_col=0 };
    keel_error_t err = keel_scatter_result_finalize_avg(r, &spec, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_scatter_result_iter_t it;
    keel_scatter_result_iter_init(&it, r);
    const keel_scatter_col_val_t* ov;
    TEST_ASSERT(keel_scatter_result_iter_next(&it, &ov));
    /* NULL count → result should be NULL */
    TEST_ASSERT(ov[0].len < 0);
    keel_scatter_result_iter_close(&it);

    keel_scatter_result_destroy(r);
    TEST_END();
}

/* ============================================================================
 * Phase H — keel_dispatch_result_t struct Phase H fields
 * ============================================================================ */

static void test_dispatch_phase_h_fields(void)
{
    TEST_BEGIN("keel_dispatch_result_t: Phase H fields exist and zero-initialise");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    /* window_forced_single */
    TEST_ASSERT(!dr.window_forced_single);

    /* AVG finalize specs */
    TEST_ASSERT_EQ(dr.navg_finalize_specs, (uint16_t)0);
    dr.avg_finalize_specs[0].sum_col   = 0;
    dr.avg_finalize_specs[0].count_col = 2;
    dr.avg_finalize_specs[0].out_col   = 0;
    dr.navg_finalize_specs = 1;
    TEST_ASSERT_EQ(dr.navg_finalize_specs, (uint16_t)1);
    TEST_ASSERT_EQ(dr.avg_finalize_specs[0].count_col, (int16_t)2);

    /* HAVING predicates */
    TEST_ASSERT_EQ(dr.nhaving_preds, (uint16_t)0);
    dr.having_preds[0].col_index    = 1;
    dr.having_preds[0].op           = KEEL_CMP_GT;
    dr.having_preds[0].literal[0]   = '5';
    dr.having_preds[0].literal_len  = 1;
    dr.nhaving_preds = 1;
    TEST_ASSERT_EQ(dr.nhaving_preds, (uint16_t)1);
    TEST_ASSERT_EQ(dr.having_preds[0].op, KEEL_CMP_GT);

    /* 2PC fields */
    TEST_ASSERT(!dr.twopc_required);
    TEST_ASSERT(dr.twopc == NULL);
    dr.twopc_required = true;
    TEST_ASSERT(dr.twopc_required);
    /* Setting twopc_required must not affect existing fields */
    TEST_ASSERT(dr.navg_finalize_specs == 1);
    TEST_ASSERT(dr.nhaving_preds == 1);

    TEST_END();
}

static void test_dispatch_window_forced_single_independent(void)
{
    TEST_BEGIN("keel_dispatch_result_t: window_forced_single independent from has_window_funcs");

    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof dr);

    dr.has_window_funcs   = true;
    dr.window_forced_single = true;

    TEST_ASSERT(dr.has_window_funcs);
    TEST_ASSERT(dr.window_forced_single);
    /* Forcing single must not flip requires_merge */
    TEST_ASSERT(!dr.requires_merge);

    TEST_END();
}

int main(void)
{
    keel_mem_init(NULL);

    /* create / destroy */
    test_create_destroy();

    /* memory path */
    test_memory_path_basic();
    test_memory_path_null_values();
    test_memory_path_zero_rows();

    /* spill path */
    test_spill_path_basic();
    test_spill_second_iterate();
    test_spill_large_rows();

    /* OID comparator — text */
    test_cmp_text_int4();
    test_cmp_text_int8();
    test_cmp_text_float8();
    test_cmp_text_text();
    test_cmp_text_bool();
    test_cmp_text_timestamp();
    test_cmp_text_uuid();
    test_cmp_null_null();

    /* OID comparator — binary */
    test_cmp_binary_int4();
    test_cmp_binary_int8();
    test_cmp_binary_bool();

    /* Config */
    test_mem_limit_clamping();

    /* router.h struct */
    test_dispatch_result_requires_merge_field();

    /* Phase C — keel_scatter_result_sort (memory path) */
    test_sort_memory_int4_asc();
    test_sort_memory_int4_desc();
    test_sort_memory_text_multi_key();
    test_sort_memory_nulls_first();
    test_sort_empty_result();

    /* Phase C — keel_scatter_result_apply_limit (memory path) */
    test_limit_no_offset();
    test_limit_with_offset();
    test_limit_offset_past_end();
    test_limit_zero_means_no_limit();
    test_sort_then_limit();

    /* Phase C — dispatch result struct fields */
    test_dispatch_result_order_fields_exist();

    /* Phase D — keel_scatter_result_merge_aggs */
    test_merge_aggs_count();
    test_merge_aggs_sum_int();
    test_merge_aggs_sum_float();
    test_merge_aggs_min();
    test_merge_aggs_max();
    test_merge_aggs_mixed();
    test_merge_aggs_null_inputs();
    test_merge_aggs_all_null_sum();
    test_merge_aggs_avg_rejected();
    test_merge_aggs_empty_result();
    test_merge_aggs_passthrough();

    /* Phase D — dispatch result agg fields */
    test_dispatch_agg_fields_exist();

    /* Phase E — keel_scatter_result_group_aggs */
    test_group_single_key_count();
    test_group_single_key_sum();
    test_group_multi_key();
    test_group_null_key_forms_own_group();
    test_group_single_group();
    test_group_empty_result();
    test_group_avg_rejected();
    test_group_then_sort();

    /* GROUP BY spill-path stress tests */
    test_group_spill_count();
    test_group_spill_sum();
    test_group_spill_min_max();
    test_group_spill_many_groups();
    test_group_spill_then_sort();
    test_group_spill_null_keys();

    /* Phase E — dispatch result group fields */
    test_dispatch_group_fields_exist();

    /* Phase F — window function has_window_funcs flag */
    test_dispatch_window_flag_default_false();
    test_dispatch_window_flag_set();
    test_dispatch_window_does_not_set_requires_merge();

    /* Phase H — keel_scatter_result_apply_having */
    test_apply_having_basic();
    test_apply_having_eq();
    test_apply_having_no_match();
    test_apply_having_empty_preds();

    /* Phase H — keel_scatter_result_finalize_avg */
    test_finalize_avg_basic();
    test_finalize_avg_null_count();

    /* Phase H — dispatch result Phase H struct fields */
    test_dispatch_phase_h_fields();
    test_dispatch_window_forced_single_independent();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
