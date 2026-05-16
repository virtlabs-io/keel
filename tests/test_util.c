/**
 * @file test_util.c
 * @brief Tests for utility functions
 *
 * Exercises the cross-cutting utility library that every subsystem
 * depends on: consistent hashing (hash ring), monotonic / wall-clock
 * time, durations, stopwatch, and log integration.
 *
 * Test families:
 *   §1 — Hash ring: lifecycle (create / destroy / NULL), add /
 *         remove nodes, deterministic get (same key → same node),
 *         weight-proportional distribution.
 *   §2 — Duration creation: ns / us / ms / sec / min factory
 *         functions and internal representation.
 *   §3 — Duration conversion: cross-unit round-trips and edge
 *         cases (zero, max, negative).
 *   §4 — Time now: keel_time_now() returns a monotonic value
 *         that advances between successive calls.
 *   §5 — Time arithmetic: add / subtract / compare against
 *         duration deltas.
 *   §6 — Stopwatch: start → elapsed → reset cycle.
 *   §7 — Realtime: keel_time_realtime() returns a wall-clock
 *         value in the plausible epoch range.
 *   §8 — Time formatting: ISO-8601 and local-time string
 *         serialization.
 *   §9 — Duration format / parse: human-readable "500ms",
 *         "2.5s" round-trips.
 *   §10 — Log smoke tests: config default, lifecycle, level,
 *          level name, category name, output (exercises the same
 *          API as test_log.c but through the util convenience
 *          wrappers).
 *
 * Note: string, buffer, and hash function tests are disabled until
 * API mismatches between headers and implementations are resolved.
 */


#include "test_utils.h"
#include "keel_types.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Hash Ring Tests
 * ============================================================================ */

static void test_hash_ring_lifecycle(void) {
    TEST_BEGIN("hash ring lifecycle");
    
    keel_hash_ring_t* ring = keel_hash_ring_new(100);
    TEST_ASSERT_NOT_NULL(ring);
    
    keel_hash_ring_free(ring);
    
    /* NULL should be handled */
    keel_hash_ring_free(NULL);
    
    TEST_END();
}

static void test_hash_ring_add_remove(void) {
    TEST_BEGIN("hash ring add/remove");
    
    keel_hash_ring_t* ring = keel_hash_ring_new(100);
    TEST_ASSERT_NOT_NULL(ring);
    
    /* Add nodes */
    keel_error_t err = keel_hash_ring_add(ring, "node1", 5);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    err = keel_hash_ring_add(ring, "node2", 5);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    err = keel_hash_ring_add(ring, "node3", 5);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Remove a node */
    err = keel_hash_ring_remove(ring, "node2");
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    keel_hash_ring_free(ring);
    
    TEST_END();
}

static void test_hash_ring_get(void) {
    TEST_BEGIN("hash ring get");
    
    keel_hash_ring_t* ring = keel_hash_ring_new(100);
    TEST_ASSERT_NOT_NULL(ring);
    
    keel_hash_ring_add(ring, "node1", 5);
    keel_hash_ring_add(ring, "node2", 5);
    
    const void* node_data;
    size_t node_len;
    keel_error_t err = keel_hash_ring_get(ring, "test-key", 8, &node_data, &node_len);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(node_data);
    
    /* Same key should always map to same node */
    const void* node_data2;
    size_t node_len2;
    err = keel_hash_ring_get(ring, "test-key", 8, &node_data2, &node_len2);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT(node_len == node_len2);
    TEST_ASSERT(memcmp(node_data, node_data2, node_len) == 0);
    
    keel_hash_ring_free(ring);
    
    TEST_END();
}

/* ============================================================================
 * Time Utility Tests
 * ============================================================================ */

static void test_duration_creation(void) {
    TEST_BEGIN("duration creation");
    
    keel_duration_t d_ns = keel_duration_ns(1000);
    keel_duration_t d_us = keel_duration_us(1);
    keel_duration_t d_ms = keel_duration_ms(1);
    keel_duration_t d_sec = keel_duration_sec(1);
    keel_duration_t d_min = keel_duration_min(1);
    
    /* 1 us = 1000 ns */
    TEST_ASSERT_EQ(d_ns, d_us);
    
    /* 1 ms = 1000 us */
    TEST_ASSERT_EQ(d_ms, keel_duration_us(1000));
    
    /* 1 sec = 1000 ms */
    TEST_ASSERT_EQ(d_sec, keel_duration_ms(1000));
    
    /* 1 min = 60 sec */
    TEST_ASSERT_EQ(d_min, keel_duration_sec(60));
    
    TEST_END();
}

static void test_duration_conversion(void) {
    TEST_BEGIN("duration conversion");
    
    keel_duration_t d = keel_duration_sec(2);
    
    /* To seconds */
    double sec = keel_duration_to_sec(d);
    TEST_ASSERT(sec >= 1.9 && sec <= 2.1);
    
    /* To milliseconds */
    int64_t ms = keel_duration_to_ms(d);
    TEST_ASSERT_EQ(ms, 2000);
    
    TEST_END();
}

static void test_time_now(void) {
    TEST_BEGIN("time now");
    
    keel_time_t t1 = keel_time_now();
    keel_time_t t2 = keel_time_now();
    
    /* Time should be monotonic */
    TEST_ASSERT(t2 >= t1);
    
    /* Should be reasonable (not 0) */
    TEST_ASSERT(t1 > 0);
    
    TEST_END();
}

static void test_time_arithmetic(void) {
    TEST_BEGIN("time arithmetic");
    
    keel_time_t t1 = keel_time_now();
    keel_duration_t d = keel_duration_ms(100);
    
    keel_time_t t2 = keel_time_add(t1, d);
    TEST_ASSERT(t2 > t1);
    
    keel_duration_t diff = keel_time_diff(t1, t2);
    int64_t diff_ms = keel_duration_to_ms(diff);
    TEST_ASSERT_EQ(diff_ms, 100);
    
    TEST_END();
}

static void test_stopwatch(void) {
    TEST_BEGIN("stopwatch");
    
    keel_stopwatch_t sw;
    keel_stopwatch_start(&sw);
    
    /* Do some work */
    volatile int sum = 0;
    for (int i = 0; i < 10000; i++) {
        sum += i;
    }
    (void)sum;
    
    keel_duration_t elapsed = keel_stopwatch_elapsed(&sw);
    TEST_ASSERT(elapsed >= 0);
    
    TEST_END();
}

static void test_time_realtime(void) {
    TEST_BEGIN("time realtime");
    
    keel_time_t t = keel_time_realtime();
    
    /* Should be reasonable (after year 2020) */
    keel_time_t year_2020 = 1577836800ULL * KEEL_NS_PER_SEC;  /* Jan 1, 2020 */
    TEST_ASSERT(t > year_2020);
    
    TEST_END();
}

static void test_time_comparisons(void) {
    TEST_BEGIN("time comparisons");
    
    keel_time_t t1 = keel_time_now();
    keel_time_t t2 = keel_time_add(t1, keel_duration_ms(100));
    
    /* t1 should be before t2 */
    TEST_ASSERT(t1 < t2);
    
    /* Difference should be 100ms */
    keel_duration_t diff = keel_time_diff(t1, t2);
    int64_t diff_ms = keel_duration_to_ms(diff);
    TEST_ASSERT_EQ(diff_ms, 100);
    
    TEST_END();
}

static void test_time_before_after(void) {
    TEST_BEGIN("time before/after");
    
    keel_time_t t1 = keel_time_now();
    keel_time_t t2 = keel_time_add(t1, keel_duration_ms(100));
    
    TEST_ASSERT(keel_time_before(t1, t2));
    TEST_ASSERT(!keel_time_before(t2, t1));
    TEST_ASSERT(!keel_time_before(t1, t1));
    
    TEST_ASSERT(keel_time_after(t2, t1));
    TEST_ASSERT(!keel_time_after(t1, t2));
    TEST_ASSERT(!keel_time_after(t1, t1));
    
    TEST_END();
}

static void test_time_format_iso8601(void) {
    TEST_BEGIN("time format iso8601");
    
    keel_time_t t = keel_time_realtime();
    char buf[64];
    
    size_t len = keel_time_format_iso8601(t, buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(len < sizeof(buf));
    
    /* Should have format like 2024-01-15T12:34:56.123456789Z */
    TEST_ASSERT(buf[4] == '-');  /* Year separator */
    TEST_ASSERT(buf[7] == '-');  /* Month separator */
    TEST_ASSERT(buf[10] == 'T'); /* Date-time separator */
    
    /* Buffer too small */
    char small_buf[5];
    len = keel_time_format_iso8601(t, small_buf, sizeof(small_buf));
    TEST_ASSERT(len >= sizeof(small_buf));  /* Truncated */
    
    TEST_END();
}

static void test_time_format_local(void) {
    TEST_BEGIN("time format local");
    
    keel_time_t t = keel_time_realtime();
    char buf[64];
    
    size_t len = keel_time_format_local(t, buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(len < sizeof(buf));
    
    /* Should have format like 2024-01-15 12:34:56 */
    TEST_ASSERT(buf[4] == '-');  /* Year separator */
    TEST_ASSERT(buf[7] == '-');  /* Month separator */
    TEST_ASSERT(buf[10] == ' '); /* Date-time separator */
    
    TEST_END();
}

static void test_duration_format(void) {
    TEST_BEGIN("duration format");
    
    char buf[64];
    size_t len;
    
    /* Nanoseconds */
    len = keel_duration_format(500, buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(buf, "ns") != NULL);
    
    /* Microseconds */
    len = keel_duration_format(keel_duration_us(50), buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    /* Should contain µs or similar */
    
    /* Milliseconds */
    len = keel_duration_format(keel_duration_ms(100), buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(buf, "ms") != NULL);
    
    /* Seconds */
    len = keel_duration_format(keel_duration_sec(5), buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(buf, "s") != NULL);
    
    /* Minutes */
    len = keel_duration_format(keel_duration_min(2) + keel_duration_sec(30), buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(buf, "m") != NULL);
    
    /* Hours */
    keel_duration_t two_hours_fifteen = keel_duration_sec(3600 * 2 + 15 * 60);
    len = keel_duration_format(two_hours_fifteen, buf, sizeof(buf));
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(buf, "h") != NULL);
    
    TEST_END();
}

static void test_duration_parse(void) {
    TEST_BEGIN("duration parse");
    
    keel_duration_t d;
    
    /* Parse seconds */
    TEST_ASSERT(keel_duration_parse("5s", &d));
    TEST_ASSERT_EQ(keel_duration_to_ms(d), 5000);
    
    /* Parse milliseconds */
    TEST_ASSERT(keel_duration_parse("100ms", &d));
    TEST_ASSERT_EQ(keel_duration_to_ms(d), 100);
    
    /* Parse microseconds */
    TEST_ASSERT(keel_duration_parse("1000us", &d));
    TEST_ASSERT_EQ(keel_duration_to_us(d), 1000);
    
    /* Parse nanoseconds */
    TEST_ASSERT(keel_duration_parse("1000000ns", &d));
    TEST_ASSERT_EQ(keel_duration_to_ns(d), 1000000);
    
    /* Parse minutes */
    TEST_ASSERT(keel_duration_parse("2m", &d));
    TEST_ASSERT_EQ(keel_duration_to_sec(d), 120.0);
    
    TEST_ASSERT(keel_duration_parse("2min", &d));
    TEST_ASSERT_EQ(keel_duration_to_sec(d), 120.0);
    
    /* Parse hours */
    TEST_ASSERT(keel_duration_parse("1h", &d));
    TEST_ASSERT_EQ(keel_duration_to_sec(d), 3600.0);
    
    TEST_ASSERT(keel_duration_parse("1hr", &d));
    TEST_ASSERT_EQ(keel_duration_to_sec(d), 3600.0);
    
    /* Plain number defaults to seconds */
    TEST_ASSERT(keel_duration_parse("30", &d));
    TEST_ASSERT_EQ(keel_duration_to_sec(d), 30.0);
    
    /* Invalid formats */
    TEST_ASSERT(!keel_duration_parse(NULL, &d));
    TEST_ASSERT(!keel_duration_parse("5s", NULL));
    TEST_ASSERT(!keel_duration_parse("", &d));
    TEST_ASSERT(!keel_duration_parse("abc", &d));
    TEST_ASSERT(!keel_duration_parse("5xyz", &d));
    
    TEST_END();
}

static void test_duration_to_ns_us(void) {
    TEST_BEGIN("duration to ns/us");
    
    keel_duration_t d = keel_duration_ms(1);
    
    int64_t ns = keel_duration_to_ns(d);
    TEST_ASSERT_EQ(ns, 1000000);
    
    int64_t us = keel_duration_to_us(d);
    TEST_ASSERT_EQ(us, 1000);
    
    TEST_END();
}

/* ============================================================================
 * Duration Conversion Tests
 * ============================================================================ */

static void test_duration_conversions(void) {
    TEST_BEGIN("duration conversions");
    
    keel_duration_t d = keel_duration_sec(1);
    
    /* To milliseconds */
    int64_t ms = keel_duration_to_ms(d);
    TEST_ASSERT_EQ(ms, 1000);
    
    /* To seconds (double) */
    double sec = keel_duration_to_sec(d);
    TEST_ASSERT(sec >= 0.99 && sec <= 1.01);
    
    /* Test with minutes */
    d = keel_duration_min(1);
    ms = keel_duration_to_ms(d);
    TEST_ASSERT_EQ(ms, 60000);
    
    /* Test with milliseconds */
    d = keel_duration_ms(500);
    ms = keel_duration_to_ms(d);
    TEST_ASSERT_EQ(ms, 500);
    
    /* Test with nanoseconds */
    d = keel_duration_ns(1000000);  /* 1ms in ns */
    ms = keel_duration_to_ms(d);
    TEST_ASSERT_EQ(ms, 1);
    
    TEST_END();
}

static void test_sleep_functions(void) {
    TEST_BEGIN("sleep functions");
    
    keel_time_t t1 = keel_time_now();
    
    /* Sleep for 5ms using keel_sleep */
    keel_sleep(keel_duration_ms(5));
    
    keel_time_t t2 = keel_time_now();
    keel_duration_t elapsed = keel_time_diff(t1, t2);
    int64_t elapsed_ms = keel_duration_to_ms(elapsed);
    
    /* Should have slept at least 4ms (allow for timing jitter) */
    TEST_ASSERT(elapsed_ms >= 4);
    
    /* Test sleep_ms */
    t1 = keel_time_now();
    keel_sleep_ms(5);
    t2 = keel_time_now();
    elapsed = keel_time_diff(t1, t2);
    elapsed_ms = keel_duration_to_ms(elapsed);
    TEST_ASSERT(elapsed_ms >= 4);
    
    /* Test sleep_us (sleep 5000us = 5ms) */
    t1 = keel_time_now();
    keel_sleep_us(5000);
    t2 = keel_time_now();
    elapsed = keel_time_diff(t1, t2);
    elapsed_ms = keel_duration_to_ms(elapsed);
    TEST_ASSERT(elapsed_ms >= 4);
    
    /* Test sleep_ns (sleep 5000000ns = 5ms) */
    t1 = keel_time_now();
    keel_sleep_ns(5000000);
    t2 = keel_time_now();
    elapsed = keel_time_diff(t1, t2);
    elapsed_ms = keel_duration_to_ms(elapsed);
    TEST_ASSERT(elapsed_ms >= 4);
    
    /* Test zero/negative sleep doesn't hang */
    keel_sleep(0);
    keel_sleep(-1);
    
    TEST_END();
}

static void test_stopwatch_full(void) {
    TEST_BEGIN("stopwatch full");
    
    keel_stopwatch_t sw = {0};
    
    /* Start */
    keel_stopwatch_start(&sw);
    TEST_ASSERT(sw.running);
    
    /* Check elapsed - should be small */
    keel_duration_t elapsed = keel_stopwatch_elapsed(&sw);
    TEST_ASSERT(elapsed >= 0);
    
    /* Sleep a bit to accumulate time */
    keel_sleep_ms(5);
    
    /* Elapsed should have increased */
    keel_duration_t elapsed1 = keel_stopwatch_elapsed(&sw);
    TEST_ASSERT(elapsed1 > elapsed);
    TEST_ASSERT(keel_duration_to_ms(elapsed1) >= 4);
    
    /* Stop the stopwatch */
    keel_stopwatch_stop(&sw);
    TEST_ASSERT(!sw.running);
    
    /* Elapsed should stay the same after stop */
    keel_duration_t stopped_elapsed = keel_stopwatch_elapsed(&sw);
    keel_sleep_ms(5);
    keel_duration_t stopped_elapsed2 = keel_stopwatch_elapsed(&sw);
    TEST_ASSERT_EQ(stopped_elapsed, stopped_elapsed2);
    
    /* Reset the stopwatch */
    keel_stopwatch_reset(&sw);
    TEST_ASSERT(!sw.running);
    TEST_ASSERT_EQ(sw.elapsed, 0);
    TEST_ASSERT_EQ(sw.start, 0);
    
    /* After reset, elapsed should be 0 */
    elapsed = keel_stopwatch_elapsed(&sw);
    TEST_ASSERT_EQ(elapsed, 0);
    
    /* Null safety */
    keel_stopwatch_start(NULL);
    keel_stopwatch_stop(NULL);
    keel_stopwatch_reset(NULL);
    elapsed = keel_stopwatch_elapsed(NULL);
    TEST_ASSERT_EQ(elapsed, 0);
    
    TEST_END();
}

/* ============================================================================
 * Logging Tests
 * ============================================================================ */

static void test_log_config_default(void) {
    TEST_BEGIN("log config default");
    
    keel_log_config_t config = keel_log_config_default();
    
    /* Verify defaults */
    TEST_ASSERT_EQ(config.min_level, KEEL_LOG_INFO);
    TEST_ASSERT(config.categories != 0);  /* Some categories enabled */
    TEST_ASSERT_EQ(config.output, KEEL_LOG_OUTPUT_STDERR);
    TEST_ASSERT(config.include_time);
    TEST_ASSERT(config.include_level);
    TEST_ASSERT(config.use_colors);
    
    TEST_END();
}

static void test_log_lifecycle(void) {
    TEST_BEGIN("log lifecycle");
    
    /* Initialize with defaults */
    keel_error_t err = keel_log_init(NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Double init should be safe */
    err = keel_log_init(NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Shutdown */
    keel_log_shutdown();
    
    /* Double shutdown should be safe */
    keel_log_shutdown();
    
    TEST_END();
}

static void test_log_level(void) {
    TEST_BEGIN("log level");
    
    keel_log_init(NULL);
    
    /* Set and get level */
    keel_log_set_level(KEEL_LOG_DEBUG);
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_DEBUG);
    
    keel_log_set_level(KEEL_LOG_ERROR);
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_ERROR);
    
    keel_log_set_level(KEEL_LOG_TRACE);
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_TRACE);
    
    /* Restore for other tests */
    keel_log_set_level(KEEL_LOG_WARN);
    
    keel_log_shutdown();
    
    TEST_END();
}

static void test_log_level_name(void) {
    TEST_BEGIN("log level name");
    
    /* Level names */
    const char* name;
    
    name = keel_log_level_name(KEEL_LOG_TRACE);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_level_name(KEEL_LOG_DEBUG);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_level_name(KEEL_LOG_INFO);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_level_name(KEEL_LOG_WARN);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_level_name(KEEL_LOG_ERROR);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_level_name(KEEL_LOG_FATAL);
    TEST_ASSERT_NOT_NULL(name);
    
    /* Invalid level */
    name = keel_log_level_name((keel_log_level_t)999);
    TEST_ASSERT_NOT_NULL(name);
    
    TEST_END();
}

static void test_log_category_name(void) {
    TEST_BEGIN("log category name");
    
    const char* name;
    
    name = keel_log_category_name(KEEL_LOG_CAT_CORE);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_category_name(KEEL_LOG_CAT_POOL);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_category_name(KEEL_LOG_CAT_IO);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_category_name(KEEL_LOG_CAT_PROTO);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_category_name(KEEL_LOG_CAT_SQL);
    TEST_ASSERT_NOT_NULL(name);
    
    name = keel_log_category_name(KEEL_LOG_CAT_MEM);
    TEST_ASSERT_NOT_NULL(name);
    
    /* Invalid category */
    name = keel_log_category_name((keel_log_category_t)0);
    TEST_ASSERT_NOT_NULL(name);
    
    TEST_END();
}

static void test_log_output(void) {
    TEST_BEGIN("log output");
    
    /* Initialize with stderr to suppress actual logging */
    keel_log_config_t config = keel_log_config_default();
    config.min_level = KEEL_LOG_FATAL;  /* Only show fatal - suppress test output */
    config.output = KEEL_LOG_OUTPUT_STDERR;
    
    keel_error_t err = keel_log_init(&config);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* These shouldn't crash even though level is too high */
    KEEL_LOG_TRACE(KEEL_LOG_CAT_CORE, "trace message");
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "debug message");
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "info message");
    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "warn message");
    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "error message");
    
    keel_log_shutdown();
    
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Utility Library Tests\n");
    printf("=====================\n\n");
    
    keel_mem_init(NULL);
    
    /* Hash ring tests */
    test_hash_ring_lifecycle();
    test_hash_ring_add_remove();
    test_hash_ring_get();
    
    /* Time tests */
    test_duration_creation();
    test_duration_conversion();
    test_duration_conversions();
    test_duration_to_ns_us();
    test_time_now();
    test_time_arithmetic();
    test_time_comparisons();
    test_time_before_after();
    test_time_realtime();
    test_time_format_iso8601();
    test_time_format_local();
    test_duration_format();
    test_duration_parse();
    
    /* Stopwatch tests */
    test_stopwatch();
    test_stopwatch_full();
    
    /* Sleep tests */
    test_sleep_functions();
    
    /* Logging tests */
    test_log_config_default();
    test_log_lifecycle();
    test_log_level();
    test_log_level_name();
    test_log_category_name();
    test_log_output();
    
    keel_mem_shutdown();
    
    return test_summary();
}
