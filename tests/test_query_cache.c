/**
 * @file test_query_cache.c
 * @brief Unit tests for query result caching.
 *
 * Tests the LRU query result cache:
 *   §1 — Cache lifecycle: create, destroy, basic put/get
 *   §2 — LRU eviction: size limits trigger oldest-entry eviction
 *   §3 — TTL expiry: entries expire on timeout
 *   §4 — Flush: clear all entries
 *   §5 — Digest computation: normalize queries, detect non-cacheable patterns
 *   §6 — Thread safety: concurrent access with RWlock
 *   §7 — Memory management: edge cases, invalid arguments
 *
 * @author KEEL Development Team
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/core/query_cache.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

/* Global counters for test_utils */
int g_tests_run = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) {
    printf("\n=== Query Cache Tests ===\n");
    printf("Total:  %d\n", g_tests_run);
    printf("Passed: %d\n", g_tests_passed);
    printf("Failed: %d\n", g_tests_failed);
    return (g_tests_failed == 0) ? 0 : 1;
}

/* ============================================================================
 * Test 1: Basic Create/Destroy
 * ============================================================================ */


/* §1 — Create/destroy */
static void test_query_cache_create_destroy(void) {
    TEST_BEGIN("cache_create_destroy");

    keel_query_cache_t* cache = NULL;
    keel_error_t err = keel_query_cache_create(&cache, 3000, 5);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(cache);

    keel_query_cache_destroy(cache);
    keel_query_cache_destroy(NULL);  /* NULL-safe */

    TEST_END();
}
/* ============================================================================
 * Test 2: Basic Put/Get
 * ============================================================================ */


/* §2 — Put/Get */
static void test_query_cache_put_get(void) {
    TEST_BEGIN("cache_put_get");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 10);

    uint8_t digest[32];
    memset(digest, 0x42, 32);

    uint8_t result[] = "SELECT * FROM users;";
    size_t result_len = strlen((const char*)result);

    /* Initially miss */
    const uint8_t* retrieved = NULL;
    size_t retrieved_len = 0;
    keel_error_t err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_CACHE_MISS);

    /* Put result */
    err = keel_query_cache_put(cache, digest, result, result_len, 1000);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Now hit */
    err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(retrieved_len, result_len);
    TEST_ASSERT_EQ(memcmp(retrieved, result, result_len), 0);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 3: Multiple Entries
 * ============================================================================ */


/* §3 — Multiple entries */
static void test_query_cache_multiple_entries(void) {
    TEST_BEGIN("cache_multiple_entries");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 100);

    /* Insert 10 different queries */
    for (int i = 0; i < 10; i++) {
        uint8_t digest[32];
        memset(digest, i, 32);

        char result[100];
        snprintf(result, sizeof(result), "Query %d result", i);
        size_t result_len = strlen(result);

        keel_error_t err = keel_query_cache_put(cache, digest, (uint8_t*)result,
                                                 result_len, 1000);
        TEST_ASSERT_EQ(err, KEEL_OK);
    }

    /* Retrieve all 10 */
    for (int i = 0; i < 10; i++) {
        uint8_t digest[32];
        memset(digest, i, 32);

        const uint8_t* retrieved = NULL;
        size_t retrieved_len = 0;

        keel_error_t err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
        TEST_ASSERT_EQ(err, KEEL_OK);

        char expected[100];
        snprintf(expected, sizeof(expected), "Query %d result", i);
        TEST_ASSERT_EQ(retrieved_len, strlen(expected));
        TEST_ASSERT_EQ(memcmp(retrieved, expected, strlen(expected)), 0);
    }

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 4: LRU Eviction on Size Limit
 * ============================================================================ */


/* §4 — LRU eviction */
static void test_query_cache_lru_eviction(void) {
    TEST_BEGIN("cache_lru_eviction");

    /* Create small cache: 1 MB */
    keel_query_cache_t* cache = NULL;
    keel_error_t err = keel_query_cache_create(&cache, 1000, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Insert large entries that will trigger eviction */
    uint8_t large_result[600000];  /* 600 KB > 1 MB cache */
    memset(large_result, 0xAA, sizeof(large_result));

    /* First entry */
    uint8_t digest1[32];
    memset(digest1, 1, 32);
    err = keel_query_cache_put(cache, digest1, large_result,
                               sizeof(large_result), 1000);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Second entry should evict first */
    uint8_t digest2[32];
    memset(digest2, 2, 32);
    err = keel_query_cache_put(cache, digest2, large_result,
                               sizeof(large_result), 1000);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* First entry should be evicted */
    const uint8_t* retrieved = NULL;
    size_t retrieved_len = 0;
    err = keel_query_cache_get(cache, digest1, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_CACHE_MISS);

    /* Second entry should be present */
    err = keel_query_cache_get(cache, digest2, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 5: TTL Expiry
 * ============================================================================ */


/* §5 — TTL expiry */
static void test_query_cache_ttl_expiry(void) {
    TEST_BEGIN("cache_ttl_expiry");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 10);

    uint8_t digest[32];
    memset(digest, 0x55, 32);

    uint8_t result[] = "TTL test result";
    size_t result_len = strlen((const char*)result);

    /* Put with a one-second TTL.
     * NOTE: the cache rounds TTL up to the nearest whole second via
     * ceiling division, so ttl_sec = 1 and expires_at = time(NULL) + 1.
     * time(NULL) has 1-second granularity.  To guarantee the entry is
     * expired regardless of when within a second the test runs, we sleep
     * for 2.1 seconds: the worst case is insertion just after a second
     * boundary, requiring just under 1 second to reach expires_at, so 2s
     * of sleep plus a 0.1s margin is always sufficient. */
    keel_error_t err = keel_query_cache_put(cache, digest, result, result_len, 1000);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Should hit immediately */
    const uint8_t* retrieved = NULL;
    size_t retrieved_len = 0;
    err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Sleep long enough to guarantee expiry at any sub-second insertion time. */
    usleep(2100000);

    /* Should miss now */
    err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_CACHE_MISS);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 6: Flush Operation
 * ============================================================================ */


/* §6 — Flush */
static void test_query_cache_flush(void) {
    TEST_BEGIN("cache_flush");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 100);

    /* Insert 5 entries */
    for (int i = 0; i < 5; i++) {
        uint8_t digest[32];
        memset(digest, i + 10, 32);

        char result[50];
        snprintf(result, sizeof(result), "Flush test %d", i);

        keel_error_t err = keel_query_cache_put(cache, digest, (uint8_t*)result,
                                                 strlen(result), 1000);
        TEST_ASSERT_EQ(err, KEEL_OK);
    }

    /* Flush all */
    keel_error_t err = keel_query_cache_flush(cache);
    TEST_ASSERT_EQ(err, KEEL_OK);
    keel_query_cache_flush(NULL);  /* NULL-safe */

    /* All should miss */
    for (int i = 0; i < 5; i++) {
        uint8_t digest[32];
        memset(digest, i + 10, 32);

        const uint8_t* retrieved = NULL;
        size_t retrieved_len = 0;

        err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
        TEST_ASSERT_EQ(err, KEEL_CACHE_MISS);
    }

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 7: Expire Operation
 * ============================================================================ */


/* §7 — Expire */
static void test_query_cache_expire(void) {
    TEST_BEGIN("cache_expire");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 10);

    uint8_t digest[32];
    memset(digest, 0x77, 32);

    uint8_t result[] = "Expire test result";
    size_t result_len = strlen((const char*)result);

    /* Put with long TTL */
    keel_error_t err = keel_query_cache_put(cache, digest, result, result_len, 10000);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Should hit */
    const uint8_t* retrieved = NULL;
    size_t retrieved_len = 0;
    err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Manually expire */
    err = keel_query_cache_expire(cache, digest);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Should miss now (TTL still has time but entry is marked expired) */
    err = keel_query_cache_get(cache, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_CACHE_MISS);

    /* Expire non-existent entry */
    uint8_t missing_digest[32];
    memset(missing_digest, 0xFF, 32);
    err = keel_query_cache_expire(cache, missing_digest);
    TEST_ASSERT_EQ(err, KEEL_CACHE_MISS);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 8: Statistics
 * ============================================================================ */


/* §8 — Statistics */
static void test_query_cache_stats(void) {
    TEST_BEGIN("cache_stats");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 100);

    uint8_t digest1[32];
    memset(digest1, 0x80, 32);

    uint8_t digest2[32];
    memset(digest2, 0x81, 32);

    uint8_t result[] = "Stats test result";
    size_t result_len = strlen((const char*)result);

    /* Put 2 entries */
    keel_query_cache_put(cache, digest1, result, result_len, 1000);
    keel_query_cache_put(cache, digest2, result, result_len, 1000);

    /* Get 1st (hit) */
    const uint8_t* retrieved = NULL;
    size_t retrieved_len = 0;
    keel_query_cache_get(cache, digest1, &retrieved, &retrieved_len);

    /* Get 1st again (hit) */
    keel_query_cache_get(cache, digest1, &retrieved, &retrieved_len);

    /* Get 2nd (hit) */
    keel_query_cache_get(cache, digest2, &retrieved, &retrieved_len);

    /* Get missing (miss) */
    uint8_t missing_digest[32];
    memset(missing_digest, 0xFF, 32);
    keel_query_cache_get(cache, missing_digest, &retrieved, &retrieved_len);

    keel_query_cache_stats_t stats;
    keel_error_t err = keel_query_cache_stats(cache, &stats);
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_ASSERT_EQ(stats.hits, 3);
    TEST_ASSERT_EQ(stats.misses, 1);
    TEST_ASSERT_EQ(stats.entries_count, 2);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 9: Query Digest Computation
 * ============================================================================ */


/* §9 — Query digest computation */
static void test_query_cache_digest_computation(void) {
    TEST_BEGIN("cache_digest_computation");

    uint8_t digest1[32];
    uint8_t digest2[32];
    uint8_t digest3[32];

    /* Same query, different whitespace -> same digest */
    keel_error_t err = keel_query_cache_digest("SELECT * FROM users", digest1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    err = keel_query_cache_digest("SELECT   *   FROM   users", digest2);
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_ASSERT_EQ(memcmp(digest1, digest2, 32), 0);

    /* Different query -> different digest */
    err = keel_query_cache_digest("SELECT id FROM users", digest3);
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_ASSERT(memcmp(digest1, digest3, 32) != 0);

    TEST_END();
}
/* ============================================================================
 * Test 10: Is Cacheable Query
 * ============================================================================ */


/* §10 — Is cacheable check */
static void test_query_cache_is_cacheable(void) {
    TEST_BEGIN("cache_is_cacheable");

    /* Cacheable */
    TEST_ASSERT(keel_query_cache_is_cacheable("SELECT * FROM users"));
    TEST_ASSERT(keel_query_cache_is_cacheable("SELECT id, name FROM accounts WHERE id = 123"));

    /* Non-cacheable: DML */
    TEST_ASSERT(!keel_query_cache_is_cacheable("INSERT INTO users VALUES (1, 'test')"));
    TEST_ASSERT(!keel_query_cache_is_cacheable("UPDATE users SET name='test'"));
    TEST_ASSERT(!keel_query_cache_is_cacheable("DELETE FROM users"));

    /* Non-cacheable: FOR UPDATE */
    TEST_ASSERT(!keel_query_cache_is_cacheable("SELECT * FROM users FOR UPDATE"));
    TEST_ASSERT(!keel_query_cache_is_cacheable("SELECT * FROM users FOR SHARE"));

    /* Non-cacheable: Volatile functions */
    TEST_ASSERT(!keel_query_cache_is_cacheable("SELECT NOW() FROM users"));
    TEST_ASSERT(!keel_query_cache_is_cacheable("SELECT RANDOM() FROM users"));
    TEST_ASSERT(!keel_query_cache_is_cacheable("SELECT UUID() FROM users"));

    /* Non-cacheable: CTE */
    TEST_ASSERT(!keel_query_cache_is_cacheable("WITH cte AS (SELECT * FROM users) SELECT * FROM cte"));

    TEST_END();
}
/* ============================================================================
 * Test 11: Non-Cacheable Digest Returns Error
 * ============================================================================ */


/* §11 — Non-cacheable digest returns error */
static void test_query_cache_digest_non_cacheable(void) {
    TEST_BEGIN("cache_non_cacheable_digest");

    uint8_t digest[32];

    /* INSERT should return error */
    keel_error_t err = keel_query_cache_digest("INSERT INTO users VALUES (1, 'test')", digest);
    TEST_ASSERT_EQ(err, KEEL_CACHE_NON_CACHEABLE);

    /* NOW() should return error */
    err = keel_query_cache_digest("SELECT NOW() FROM users", digest);
    TEST_ASSERT_EQ(err, KEEL_CACHE_NON_CACHEABLE);

    TEST_END();
}
/* ============================================================================
 * Test 12: Concurrent Access (Basic Thread Safety)
 * ============================================================================ */


/* §12 — Concurrent access */
typedef struct {
    keel_query_cache_t* cache;
    int thread_id;
    int operations;
} thread_context_t;

static void* cache_thread_worker(void* arg) {
    thread_context_t* ctx = (thread_context_t*)arg;

    for (int i = 0; i < ctx->operations; i++) {
        uint8_t digest[32];
        memset(digest, (ctx->thread_id * 100 + i) % 256, 32);

        char result[100];
        snprintf(result, sizeof(result), "Thread %d op %d", ctx->thread_id, i);

        /* Alternate puts and gets */
        if (i % 2 == 0) {
            keel_query_cache_put(ctx->cache, digest, (uint8_t*)result, strlen(result), 1000);
        } else {
            const uint8_t* retrieved = NULL;
            size_t retrieved_len = 0;
            keel_query_cache_get(ctx->cache, digest, &retrieved, &retrieved_len);
        }
    }

    return NULL;
}

static void test_query_cache_concurrent_access(void) {
    TEST_BEGIN("cache_concurrent_access");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 100);

    pthread_t threads[4];
    thread_context_t contexts[4];

    for (int i = 0; i < 4; i++) {
        contexts[i].cache = cache;
        contexts[i].thread_id = i;
        contexts[i].operations = 100;

        TEST_ASSERT_EQ(pthread_create(&threads[i], NULL, cache_thread_worker,
                                        &contexts[i]), 0);
    }

    for (int i = 0; i < 4; i++) {
        TEST_ASSERT_EQ(pthread_join(threads[i], NULL), 0);
    }

    keel_query_cache_stats_t stats;
    keel_query_cache_stats(cache, &stats);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * Test 13: Case-Insensitive Normalization
 * ============================================================================ */


/* §13 — Case-insensitive normalization */
static void test_query_cache_case_insensitive(void) {
    TEST_BEGIN("cache_case_insensitive");

    uint8_t digest1[32];
    uint8_t digest2[32];

    /* Different cases -> same digest (normalized) */
    keel_error_t err = keel_query_cache_digest("SELECT * FROM users", digest1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    err = keel_query_cache_digest("select * from users", digest2);
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_ASSERT_EQ(memcmp(digest1, digest2, 32), 0);

    TEST_END();
}
/* ============================================================================
 * Test 14: Invalid Arguments
 * ============================================================================ */


/* §14 — Invalid arguments */
static void test_query_cache_invalid_args(void) {
    TEST_BEGIN("cache_invalid_args");

    keel_query_cache_t* cache = NULL;
    keel_query_cache_create(&cache, 1000, 10);

    uint8_t digest[32];
    uint8_t result[] = "test";
    const uint8_t* retrieved = NULL;
    size_t retrieved_len = 0;

    /* NULL cache */
    keel_error_t err = keel_query_cache_get(NULL, digest, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* NULL digest */
    err = keel_query_cache_get(cache, NULL, &retrieved, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* NULL result_out */
    err = keel_query_cache_get(cache, digest, NULL, &retrieved_len);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* NULL put */
    err = keel_query_cache_put(cache, digest, NULL, 4, 1000);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* Zero result_len */
    err = keel_query_cache_put(cache, digest, result, 0, 1000);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* NULL stats */
    err = keel_query_cache_stats(cache, NULL);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    keel_query_cache_destroy(cache);
    TEST_END();
}
/* ============================================================================
 * CUnit Test Suite
 * ============================================================================ */


/* ============================================================================
 * Main Test Runner
 * ============================================================================ */

int main(void) {
    printf("\n=== Query Cache Unit Tests ===\n\n");

    test_query_cache_create_destroy();
    test_query_cache_put_get();
    test_query_cache_multiple_entries();
    test_query_cache_lru_eviction();
    test_query_cache_ttl_expiry();
    test_query_cache_flush();
    test_query_cache_expire();
    test_query_cache_stats();
    test_query_cache_digest_computation();
    test_query_cache_is_cacheable();
    test_query_cache_digest_non_cacheable();
    test_query_cache_concurrent_access();
    test_query_cache_case_insensitive();
    test_query_cache_invalid_args();

    return test_summary();
}

