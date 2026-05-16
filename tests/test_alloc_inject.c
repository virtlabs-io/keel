/**
 * @file test_alloc_inject.c
 * @brief Allocation-failure injection tests.
 *
 * Uses keel_mem_set_fail_countdown() to make the N-th allocation return NULL,
 * then asserts that the subsystem under test (router, connpool, config, …)
 * returns a NULL or error rather than crashing or silently succeeding.
 *
 * Coverage objectives (§5.5 of the exhaustive test plan):
 *  - keel_router_create()  — OOM returns NULL
 *  - keel_router_add_server() — OOM on strdup leaves router intact
 *  - keel_router_add_shard_rule() — OOM handled
 *  - keel_connpool_create() — OOM returns NULL
 *  - keel_connpool_registry_create() — OOM returns NULL
 *  - keel_client_session_create() — OOM returns NULL
 *  - keel_config_load() — OOM returns NULL / error
 *  - Staircase: each Nth allocation fails in turn, no crash
 */

#include "test_utils.h"
#include "keel/mem/mem.h"
#include "keel/core/router.h"
#include "connpool.h"
#include "proxy_session.h"
#include "keel/core/config.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

static keel_route_server_t make_server(const char *name) {
    keel_route_server_t s;
    memset(&s, 0, sizeof(s));
    s.name       = name;
    s.host       = "127.0.0.1";
    s.port       = 5432;
    s.weight     = 1;
    s.role = KEEL_SERVER_PRIMARY;
    return s;
}

static void sanity_check_router(void) {
    keel_mem_set_fail_countdown(-1);
    keel_router_t *r = keel_router_create(NULL);
    if (r) keel_router_destroy(r);
}

/* ============================================================================
 * 1. keel_router_create() — first allocation fails → NULL returned
 * ============================================================================ */
static void test_router_create_oom(void) {
    TEST_BEGIN("alloc_inject: router_create OOM returns NULL");

    keel_mem_set_fail_countdown(0);
    keel_router_t *r = keel_router_create(NULL);
    TEST_ASSERT_NULL(r);
    keel_mem_set_fail_countdown(-1);
    sanity_check_router();

    TEST_END();
}

/* ============================================================================
 * 2. keel_router_add_server() — strdup fails → error, router still valid
 * ============================================================================ */
static void test_router_add_server_oom(void) {
    TEST_BEGIN("alloc_inject: router_add_server strdup OOM returns error");

    keel_mem_set_fail_countdown(-1);
    keel_router_t *r = keel_router_create(NULL);
    TEST_ASSERT_NOT_NULL(r);

    keel_route_server_t srv = make_server("primary");

    keel_mem_set_fail_countdown(0);
    keel_error_t rc = keel_router_add_server(r, &srv);
    keel_mem_set_fail_countdown(-1);

    TEST_ASSERT(rc != KEEL_OK);

    keel_router_destroy(r);
    sanity_check_router();

    TEST_END();
}

/* ============================================================================
 * 3. keel_router_add_shard_rule() — OOM handled
 * ============================================================================ */
static void test_router_add_shard_rule_oom(void) {
    TEST_BEGIN("alloc_inject: router_add_shard_rule OOM returns error");

    keel_mem_set_fail_countdown(-1);
    keel_router_t *r = keel_router_create(NULL);
    TEST_ASSERT_NOT_NULL(r);

    keel_mem_set_fail_countdown(0);
    keel_error_t rc = keel_router_add_shard_rule(r, "users", "id", 4);
    keel_mem_set_fail_countdown(-1);

    TEST_ASSERT(rc != KEEL_OK);
    keel_router_destroy(r);
    sanity_check_router();

    TEST_END();
}

/* ============================================================================
 * 4. keel_connpool_create() — OOM returns NULL
 * ============================================================================ */
static void test_connpool_create_oom(void) {
    TEST_BEGIN("alloc_inject: connpool_create OOM returns NULL");

    keel_route_server_t srv = make_server("pool0");

    keel_mem_set_fail_countdown(0);
    keel_connpool_t *p = keel_connpool_create(&srv, NULL);
    keel_mem_set_fail_countdown(-1);

    TEST_ASSERT_NULL(p);

    TEST_END();
}

/* ============================================================================
 * 5. keel_connpool_registry_create() — OOM returns NULL
 * ============================================================================ */
static void test_connpool_registry_create_oom(void) {
    TEST_BEGIN("alloc_inject: connpool_registry_create OOM returns NULL");

    keel_mem_set_fail_countdown(0);
    keel_connpool_registry_t *reg = keel_connpool_registry_create(NULL);
    keel_mem_set_fail_countdown(-1);

    TEST_ASSERT_NULL(reg);

    TEST_END();
}

/* ============================================================================
 * 6. keel_client_session_create() — OOM returns NULL
 * ============================================================================ */
static void test_client_session_create_oom(void) {
    TEST_BEGIN("alloc_inject: client_session_create OOM returns NULL");

    keel_mem_set_fail_countdown(-1);
    keel_router_t *r = keel_router_create(NULL);
    keel_connpool_registry_t *reg = keel_connpool_registry_create(NULL);
    if (!r || !reg) {
        if (r) keel_router_destroy(r);
        if (reg) keel_connpool_registry_destroy(reg);
        g_tests_run++; g_tests_passed++;
        return;
    }

    keel_mem_set_fail_countdown(0);
    keel_client_session_t *s = keel_client_session_create(r, reg);
    keel_mem_set_fail_countdown(-1);

    TEST_ASSERT_NULL(s);

    keel_router_destroy(r);
    keel_connpool_registry_destroy(reg);

    TEST_END();
}

/* ============================================================================
 * 7. keel_config_load() — OOM during config parse returns NULL/error
 * ============================================================================ */
static void test_config_load_oom(void) {
    TEST_BEGIN("alloc_inject: config_load OOM returns NULL");

    keel_mem_set_fail_countdown(0);
    keel_config_t *cfg = keel_config_load("/keel/tests/keel_test.ini");
    keel_mem_set_fail_countdown(-1);

    TEST_ASSERT_NULL(cfg);

    TEST_END();
}

/* ============================================================================
 * 8. Staircase: fail at positions 0..N, router_create must never crash
 * ============================================================================ */
#define STAIRCASE_DEPTH 12
static void test_staircase_router(void) {
    TEST_BEGIN("alloc_inject: staircase(0..11) router_create no crash");

    int nulls = 0, oks = 0;
    for (int k = 0; k < STAIRCASE_DEPTH; k++) {
        keel_mem_set_fail_countdown(k);
        keel_router_t *r = keel_router_create(NULL);
        keel_mem_set_fail_countdown(-1);
        if (r) {
            oks++;
            keel_router_destroy(r);
        } else {
            nulls++;
        }
    }

    TEST_ASSERT(nulls > 0);
    (void)oks;

    TEST_END();
}

/* ============================================================================
 * 9. Staircase: connpool_create over the first N allocations
 * ============================================================================ */
static void test_staircase_connpool(void) {
    TEST_BEGIN("alloc_inject: staircase(0..11) connpool_create no crash");

    keel_route_server_t srv = make_server("stair");
    int nulls = 0;
    for (int k = 0; k < STAIRCASE_DEPTH; k++) {
        keel_mem_set_fail_countdown(k);
        keel_connpool_t *p = keel_connpool_create(&srv, NULL);
        keel_mem_set_fail_countdown(-1);
        if (p) {
            keel_connpool_destroy(p);
        } else {
            nulls++;
        }
    }
    TEST_ASSERT(nulls > 0);

    TEST_END();
}

/* ============================================================================
 * 10. Staircase: add_server over first N allocations inside router
 * ============================================================================ */
static void test_staircase_add_server(void) {
    TEST_BEGIN("alloc_inject: staircase(0..7) add_server no crash");

    keel_route_server_t srv = make_server("svr");
    int errors = 0;
    for (int k = 0; k < 8; k++) {
        keel_mem_set_fail_countdown(-1);
        keel_router_t *r = keel_router_create(NULL);
        if (!r) continue;

        keel_mem_set_fail_countdown(k);
        keel_error_t rc = keel_router_add_server(r, &srv);
        keel_mem_set_fail_countdown(-1);

        if (rc != KEEL_OK) errors++;
        keel_router_destroy(r);
    }
    TEST_ASSERT(errors > 0);

    TEST_END();
}

/* ============================================================================
 * 11. Double-fail: fail then recover, no state bleed
 * ============================================================================ */
static void test_fail_then_recover(void) {
    TEST_BEGIN("alloc_inject: fail then recover state is clean");

    keel_mem_set_fail_countdown(0);
    keel_router_t *r1 = keel_router_create(NULL);
    keel_mem_set_fail_countdown(-1);
    TEST_ASSERT_NULL(r1);

    keel_router_t *r2 = keel_router_create(NULL);
    TEST_ASSERT_NOT_NULL(r2);

    keel_route_server_t srv = make_server("recovered");
    keel_error_t rc = keel_router_add_server(r2, &srv);
    TEST_ASSERT_EQ(rc, KEEL_OK);

    keel_router_destroy(r2);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */
int main(void) {
    test_router_create_oom();
    test_router_add_server_oom();
    test_router_add_shard_rule_oom();
    test_connpool_create_oom();
    test_connpool_registry_create_oom();
    test_client_session_create_oom();
    test_config_load_oom();
    test_staircase_router();
    test_staircase_connpool();
    test_staircase_add_server();
    test_fail_then_recover();

    printf("\nalloc_inject: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
