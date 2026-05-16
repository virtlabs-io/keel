/**
 * @file test_route_cache.c
 * @brief Correctness and eviction-policy tests for the per-session route cache.
 *
 * The route cache is a small hash table that avoids redundant SQL classification
 * for repeated queries. These tests verify insert/lookup, collision handling,
 * LRU-based eviction, and statistics tracking so regressions in the cache do not
 * silently redirect queries to the wrong backend tier.
 */

#include "test_utils.h"
#include "keel/session/route_cache.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/* Route type constants for testing */
enum { RT_READ = 1, RT_WRITE = 2, RT_DDL = 3 };

/* ============================================================================
 * §1 — Init and Flush
 * ============================================================================ */

static void test_init(void)
{
    TEST_BEGIN("route_cache init — zeroed");
    route_cache_t c;
    route_cache_init(&c);
    TEST_ASSERT_EQ(c.hits, 0ULL);
    TEST_ASSERT_EQ(c.misses, 0ULL);
    TEST_ASSERT_EQ(c.inserts, 0ULL);
    TEST_ASSERT_EQ(c.evictions, 0ULL);
    TEST_ASSERT_EQ(c.tick, 0ULL);
    TEST_END();
}

static void test_flush_clears_entries_keeps_stats(void)
{
    TEST_BEGIN("route_cache flush — clears entries, keeps stats");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "SELECT 1", 8, RT_READ);

    uint8_t out = 0;
    TEST_ASSERT(route_cache_lookup(&c, "SELECT 1", 8, &out));
    TEST_ASSERT_EQ(out, RT_READ);
    TEST_ASSERT_EQ(c.hits, 1ULL);
    TEST_ASSERT_EQ(c.inserts, 1ULL);

    route_cache_flush(&c);

    /* After flush, lookup should miss */
    TEST_ASSERT(!route_cache_lookup(&c, "SELECT 1", 8, &out));

    /* Stats preserved */
    TEST_ASSERT_EQ(c.inserts, 1ULL);
    TEST_ASSERT_EQ(c.hits, 1ULL);
    TEST_ASSERT_EQ(c.misses, 1ULL);
    TEST_END();
}

/* ============================================================================
 * §2 — Basic Insert / Lookup
 * ============================================================================ */

static void test_miss_on_empty(void)
{
    TEST_BEGIN("route_cache lookup — miss on empty cache");
    route_cache_t c;
    route_cache_init(&c);

    uint8_t out = 0xFF;
    TEST_ASSERT(!route_cache_lookup(&c, "SELECT 1", 8, &out));
    TEST_ASSERT_EQ(out, 0xFF); /* unchanged on miss */
    TEST_ASSERT_EQ(c.misses, 1ULL);
    TEST_END();
}

static void test_insert_then_hit(void)
{
    TEST_BEGIN("route_cache insert + lookup — cache hit");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "SELECT * FROM users", 19, RT_READ);

    uint8_t out = 0;
    TEST_ASSERT(route_cache_lookup(&c, "SELECT * FROM users", 19, &out));
    TEST_ASSERT_EQ(out, RT_READ);
    TEST_ASSERT_EQ(c.hits, 1ULL);
    TEST_ASSERT_EQ(c.inserts, 1ULL);
    TEST_END();
}

static void test_update_existing(void)
{
    TEST_BEGIN("route_cache insert — update existing entry");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "SELECT 1", 8, RT_READ);
    route_cache_insert(&c, "SELECT 1", 8, RT_WRITE);

    uint8_t out = 0;
    TEST_ASSERT(route_cache_lookup(&c, "SELECT 1", 8, &out));
    TEST_ASSERT_EQ(out, RT_WRITE); /* updated */

    /* Update should not count as new insert */
    TEST_ASSERT_EQ(c.inserts, 1ULL);
    TEST_ASSERT_EQ(c.evictions, 0ULL);
    TEST_END();
}

static void test_multiple_distinct_queries(void)
{
    TEST_BEGIN("route_cache — multiple distinct queries");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "SELECT 1", 8, RT_READ);
    route_cache_insert(&c, "INSERT INTO t VALUES (1)", 24, RT_WRITE);
    route_cache_insert(&c, "CREATE TABLE t (id INT)", 23, RT_DDL);

    uint8_t out;
    TEST_ASSERT(route_cache_lookup(&c, "SELECT 1", 8, &out));
    TEST_ASSERT_EQ(out, RT_READ);

    TEST_ASSERT(route_cache_lookup(&c, "INSERT INTO t VALUES (1)", 24, &out));
    TEST_ASSERT_EQ(out, RT_WRITE);

    TEST_ASSERT(route_cache_lookup(&c, "CREATE TABLE t (id INT)", 23, &out));
    TEST_ASSERT_EQ(out, RT_DDL);

    TEST_ASSERT_EQ(c.inserts, 3ULL);
    TEST_ASSERT_EQ(c.hits, 3ULL);
    TEST_END();
}

/* ============================================================================
 * §3 — Collision and Eviction
 * ============================================================================ */

static void test_probe_chain_exhaustion(void)
{
    TEST_BEGIN("route_cache — probe chain full causes eviction");
    route_cache_t c;
    route_cache_init(&c);

    /* Insert ROUTE_CACHE_MAX_PROBE + 1 queries. Since they may end up in
     * different probe chains (unless they all hash to the same slot),
     * we just verify that after many inserts, some evictions happen. */
    char buf[64];
    for (int i = 0; i < ROUTE_CACHE_SIZE + 100; i++) {
        int len = snprintf(buf, sizeof(buf), "SELECT %d", i);
        route_cache_insert(&c, buf, (size_t)len, RT_READ);
    }

    /* Must have had some evictions */
    TEST_ASSERT(c.evictions > 0);
    /* All inserts counted (minus updates) */
    TEST_ASSERT(c.inserts > 0);
    TEST_END();
}

static void test_eviction_oldest_in_chain(void)
{
    TEST_BEGIN("route_cache — eviction picks oldest entry");
    route_cache_t c;
    route_cache_init(&c);

    /* Insert enough queries that they likely collide on some probe chains.
     * First inserts are older. After inserting many more, lookup the
     * newest ones — they should still be present. */
    char buf[64];
    for (int i = 0; i < 2000; i++) {
        int len = snprintf(buf, sizeof(buf), "SELECT val FROM t%d", i);
        route_cache_insert(&c, buf, (size_t)len, RT_READ);
    }

    /* Recent inserts should still be findable */
    uint8_t out;
    int hits = 0;
    for (int i = 1900; i < 2000; i++) {
        int len = snprintf(buf, sizeof(buf), "SELECT val FROM t%d", i);
        if (route_cache_lookup(&c, buf, (size_t)len, &out)) hits++;
    }
    /* Not all may survive (probe chain limits), but many should */
    TEST_ASSERT(hits > 50);
    TEST_END();
}

/* ============================================================================
 * §4 — LRU Freshness
 * ============================================================================ */

static void test_lru_touch_on_hit(void)
{
    TEST_BEGIN("route_cache — lookup refreshes timestamp (LRU)");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "A", 1, RT_READ);
    route_cache_insert(&c, "B", 1, RT_WRITE);

    uint64_t tick_after_inserts = c.tick;

    /* Lookup A — should bump tick */
    uint8_t out;
    route_cache_lookup(&c, "A", 1, &out);
    TEST_ASSERT(c.tick > tick_after_inserts);
    TEST_END();
}

/* ============================================================================
 * §5 — Edge Cases / Safety
 * ============================================================================ */

static void test_null_safety(void)
{
    TEST_BEGIN("route_cache — NULL pointer safety");
    route_cache_t c;
    route_cache_init(&c);

    /* NULL cache */
    uint8_t out = 0;
    TEST_ASSERT(!route_cache_lookup(NULL, "X", 1, &out));

    /* NULL query */
    TEST_ASSERT(!route_cache_lookup(&c, NULL, 1, &out));

    /* Zero-length query */
    TEST_ASSERT(!route_cache_lookup(&c, "X", 0, &out));

    /* NULL route_out is allowed (just checks hit/miss) */
    route_cache_insert(&c, "X", 1, RT_READ);
    TEST_ASSERT(route_cache_lookup(&c, "X", 1, NULL));

    /* Flush NULL */
    route_cache_flush(NULL); /* should not crash */

    /* Insert with NULL cache */
    route_cache_insert(NULL, "X", 1, RT_READ); /* should not crash */

    TEST_END();
}

static void test_empty_query(void)
{
    TEST_BEGIN("route_cache — zero-length query rejected");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "", 0, RT_READ);
    uint8_t out;
    TEST_ASSERT(!route_cache_lookup(&c, "", 0, &out));
    TEST_ASSERT_EQ(c.inserts, 0ULL);
    TEST_END();
}

/* ============================================================================
 * §6 — Statistics Accuracy
 * ============================================================================ */

static void test_stats_accuracy(void)
{
    TEST_BEGIN("route_cache — statistics correct after mixed ops");
    route_cache_t c;
    route_cache_init(&c);

    route_cache_insert(&c, "Q1", 2, RT_READ);
    route_cache_insert(&c, "Q2", 2, RT_WRITE);

    uint8_t out;
    route_cache_lookup(&c, "Q1", 2, &out);  /* hit */
    route_cache_lookup(&c, "Q2", 2, &out);  /* hit */
    route_cache_lookup(&c, "Q3", 2, &out);  /* miss */

    TEST_ASSERT_EQ(c.inserts, 2ULL);
    TEST_ASSERT_EQ(c.hits, 2ULL);
    TEST_ASSERT_EQ(c.misses, 1ULL);

    double hr = route_cache_hit_rate(&c);
    /* 2 hits / 3 total = 0.6667 */
    TEST_ASSERT(hr > 0.66 && hr < 0.67);
    TEST_END();
}

/* ============================================================================
 * §7 — Large-Scale Insert/Lookup Stress
 * ============================================================================ */

static void test_large_scale_stress(void)
{
    TEST_BEGIN("route_cache — 10K insert + lookup stress");
    route_cache_t c;
    route_cache_init(&c);

    char buf[64];
    /* Insert 10K queries */
    for (int i = 0; i < 10000; i++) {
        int len = snprintf(buf, sizeof(buf), "SELECT * FROM t WHERE id=%d", i);
        route_cache_insert(&c, buf, (size_t)len, (uint8_t)(i % 3 + 1));
    }

    /* Lookup all — cache can hold 1024, so most early ones evicted */
    int hits = 0;
    for (int i = 0; i < 10000; i++) {
        int len = snprintf(buf, sizeof(buf), "SELECT * FROM t WHERE id=%d", i);
        uint8_t out;
        if (route_cache_lookup(&c, buf, (size_t)len, &out)) {
            hits++;
            TEST_ASSERT_EQ(out, (uint8_t)(i % 3 + 1));
        }
    }

    /* With 1024 slots and 8-probe chains, most recent entries should survive */
    TEST_ASSERT(hits > 500);
    TEST_ASSERT(hits <= ROUTE_CACHE_SIZE);

    /* Verify stats match */
    TEST_ASSERT_EQ(c.hits + c.misses, 10000ULL);
    TEST_ASSERT_EQ(c.hits, (uint64_t)hits);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== KEEL Route Cache Tests ===\n\n");

    test_init();
    test_flush_clears_entries_keeps_stats();
    test_miss_on_empty();
    test_insert_then_hit();
    test_update_existing();
    test_multiple_distinct_queries();
    test_probe_chain_exhaustion();
    test_eviction_oldest_in_chain();
    test_lru_touch_on_hit();
    test_null_safety();
    test_empty_query();
    test_stats_accuracy();
    test_large_scale_stress();

    return test_summary();
}
