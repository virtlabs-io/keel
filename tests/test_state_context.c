/**
 * @file test_state_context.c
 * @brief Session-Context Preservation Tests (Spec §5)
 *
 * Tests the state profile system that enables transparent SET parameter
 * continuity across backend connection reassignment:
 *   §1 — Profile operations: init, set, get, reset, clear, copy
 *   §2 — Hash computation: deterministic, canonical ordering
 *   §3 — Diff SQL generation: two-pointer merge, SET/RESET correctness
 *   §4 — Multi-parameter compose: 4+ params, interleaved SET/RESET
 *   §5 — Profile equality: fast hash path + full content verification
 *   §6 — Round-trip: profile → generate_sync_sql → apply → verify equality
 */

#include "test_utils.h"
#include "keel/session/state_profile.h"

#include <string.h>
#include <stdio.h>

int g_tests_run, g_tests_passed, g_tests_failed;

/* ============================================================================
 * §1 — Profile Operations
 * ============================================================================ */

static void test_profile_operations(void) {
    printf("  §1 Profile operations...\n");

    state_profile_t p;
    state_profile_init(&p);

    /* Init → empty */
    TEST_ASSERT_EQ(p.count, 0u);
    TEST_ASSERT_EQ(p.hash, 0ull);

    /* Set a parameter */
    int rc = state_profile_set(&p, "search_path", "public");
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(p.count, 1u);
    TEST_ASSERT(p.hash != 0ull);

    /* Get the parameter back */
    const char* val = state_profile_get(&p, "search_path");
    TEST_ASSERT_NOT_NULL(val);
    TEST_ASSERT_STR_EQ(val, "public");

    /* Get a non-existent key */
    TEST_ASSERT_NULL(state_profile_get(&p, "timezone"));

    /* Update the parameter */
    rc = state_profile_set(&p, "search_path", "myschema,public");
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(p.count, 1u);
    val = state_profile_get(&p, "search_path");
    TEST_ASSERT_STR_EQ(val, "myschema,public");

    /* Add a second parameter */
    rc = state_profile_set(&p, "timezone", "UTC");
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(p.count, 2u);

    /* Reset a parameter */
    rc = state_profile_reset(&p, "search_path");
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(p.count, 1u);
    TEST_ASSERT_NULL(state_profile_get(&p, "search_path"));

    /* Reset non-existent key → -1 */
    rc = state_profile_reset(&p, "no_such_key");
    TEST_ASSERT_EQ(rc, -1);

    /* Clear */
    state_profile_clear(&p);
    TEST_ASSERT_EQ(p.count, 0u);
    TEST_ASSERT_EQ(p.hash, 0ull);

    /* Copy */
    state_profile_t src, dst;
    state_profile_init(&src);
    state_profile_init(&dst);
    state_profile_set(&src, "work_mem", "64MB");
    state_profile_set(&src, "statement_timeout", "5000");
    state_profile_copy(&dst, &src);
    TEST_ASSERT_EQ(dst.count, src.count);
    TEST_ASSERT_EQ(dst.hash, src.hash);
    TEST_ASSERT_STR_EQ(state_profile_get(&dst, "work_mem"), "64MB");
    TEST_ASSERT_STR_EQ(state_profile_get(&dst, "statement_timeout"), "5000");
}

/* ============================================================================
 * §2 — Hash Computation
 * ============================================================================ */

static void test_hash_computation(void) {
    printf("  §2 Hash computation...\n");

    /* Same params in different insertion order → same hash (canonical) */
    state_profile_t a, b;
    state_profile_init(&a);
    state_profile_init(&b);

    state_profile_set(&a, "search_path", "public");
    state_profile_set(&a, "timezone", "UTC");
    state_profile_set(&a, "work_mem", "64MB");

    /* Insert in reverse order */
    state_profile_set(&b, "work_mem", "64MB");
    state_profile_set(&b, "timezone", "UTC");
    state_profile_set(&b, "search_path", "public");

    TEST_ASSERT_EQ(a.hash, b.hash);
    TEST_ASSERT_EQ(a.count, b.count);

    /* Different values → different hash */
    state_profile_t c;
    state_profile_init(&c);
    state_profile_set(&c, "search_path", "public");
    state_profile_set(&c, "timezone", "America/New_York");
    state_profile_set(&c, "work_mem", "64MB");

    TEST_ASSERT(a.hash != c.hash);

    /* Empty profile hash is 0 */
    state_profile_t empty;
    state_profile_init(&empty);
    TEST_ASSERT_EQ(empty.hash, 0ull);

    /* Single param → non-zero hash */
    state_profile_set(&empty, "x", "1");
    TEST_ASSERT(empty.hash != 0ull);
}

/* ============================================================================
 * §3 — Diff SQL Generation
 * ============================================================================ */

static void test_diff_sql_generation(void) {
    printf("  §3 Diff SQL generation...\n");

    state_profile_t from, to;
    state_sync_result_t result;

    /* Identical profiles → no sync needed */
    state_profile_init(&from);
    state_profile_init(&to);
    state_profile_set(&from, "search_path", "public");
    state_profile_set(&to, "search_path", "public");

    int rc = generate_sync_sql(&from, &to, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, false);
    TEST_ASSERT_EQ(result.set_count, 0);
    TEST_ASSERT_EQ(result.reset_count, 0);

    /* Empty from, non-empty to → SET commands */
    state_profile_init(&from);
    state_profile_init(&to);
    state_profile_set(&to, "search_path", "myschema");

    rc = generate_sync_sql(&from, &to, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, true);
    TEST_ASSERT_EQ(result.set_count, 1);
    TEST_ASSERT_EQ(result.reset_count, 0);
    /* search_path is emitted without quotes (PG identifier list, not literal) */
    TEST_ASSERT(strstr(result.sql, "SET search_path = myschema;") != NULL);

    /* Non-empty from, empty to → RESET commands */
    state_profile_init(&from);
    state_profile_init(&to);
    state_profile_set(&from, "timezone", "UTC");

    rc = generate_sync_sql(&from, &to, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, true);
    TEST_ASSERT_EQ(result.set_count, 0);
    TEST_ASSERT_EQ(result.reset_count, 1);
    TEST_ASSERT(strstr(result.sql, "RESET timezone;") != NULL);

    /* Value change → SET with new value */
    state_profile_init(&from);
    state_profile_init(&to);
    state_profile_set(&from, "work_mem", "4MB");
    state_profile_set(&to, "work_mem", "64MB");

    rc = generate_sync_sql(&from, &to, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, true);
    TEST_ASSERT_EQ(result.set_count, 1);
    TEST_ASSERT_EQ(result.reset_count, 0);
    TEST_ASSERT(strstr(result.sql, "SET work_mem = '64MB';") != NULL);

    /* Both empty → no sync */
    state_profile_init(&from);
    state_profile_init(&to);
    rc = generate_sync_sql(&from, &to, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, false);
}

/* ============================================================================
 * §4 — Multi-Parameter Compose
 * ============================================================================ */

static void test_multi_param_compose(void) {
    printf("  §4 Multi-parameter compose...\n");

    state_profile_t from, to;
    state_sync_result_t result;

    state_profile_init(&from);
    state_profile_init(&to);

    /* from: {a=1, b=2, c=3}
     * to:   {b=2, c=99, d=4}
     * Expected: RESET a; SET c='99'; SET d='4'; (b unchanged) */
    state_profile_set(&from, "a", "1");
    state_profile_set(&from, "b", "2");
    state_profile_set(&from, "c", "3");

    state_profile_set(&to, "b", "2");
    state_profile_set(&to, "c", "99");
    state_profile_set(&to, "d", "4");

    int rc = generate_sync_sql(&from, &to, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, true);
    TEST_ASSERT_EQ(result.reset_count, 1);  /* RESET a */
    TEST_ASSERT_EQ(result.set_count, 2);    /* SET c, SET d */
    TEST_ASSERT(strstr(result.sql, "RESET a;") != NULL);
    TEST_ASSERT(strstr(result.sql, "SET c = '99';") != NULL);
    TEST_ASSERT(strstr(result.sql, "SET d = '4';") != NULL);
    /* b should NOT appear in the sync SQL (unchanged) */
    TEST_ASSERT(strstr(result.sql, "b") == NULL);
}

/* ============================================================================
 * §5 — Profile Equality
 * ============================================================================ */

static void test_profile_equality(void) {
    printf("  §5 Profile equality...\n");

    state_profile_t a, b, c;
    state_profile_init(&a);
    state_profile_init(&b);
    state_profile_init(&c);

    /* Empty profiles are equal */
    TEST_ASSERT(state_profile_equal(&a, &b));
    TEST_ASSERT(state_profile_equal_fast(&a, &b));

    /* Same content → equal */
    state_profile_set(&a, "search_path", "public");
    state_profile_set(&a, "timezone", "UTC");
    state_profile_set(&b, "timezone", "UTC");
    state_profile_set(&b, "search_path", "public");
    TEST_ASSERT(state_profile_equal(&a, &b));
    TEST_ASSERT(state_profile_equal_fast(&a, &b));

    /* Different content → not equal */
    state_profile_set(&c, "search_path", "public");
    state_profile_set(&c, "timezone", "America/Chicago");
    TEST_ASSERT(!state_profile_equal(&a, &c));

    /* Copy → equal */
    state_profile_t copy;
    state_profile_copy(&copy, &a);
    TEST_ASSERT(state_profile_equal(&a, &copy));
    TEST_ASSERT(state_profile_equal_fast(&a, &copy));
}

/* ============================================================================
 * §6 — Round-Trip: profile → diff → apply → verify
 * ============================================================================ */

static void test_round_trip(void) {
    printf("  §6 Round-trip...\n");

    /* Build a session profile */
    state_profile_t session;
    state_profile_init(&session);
    state_profile_set(&session, "search_path", "myapp,public");
    state_profile_set(&session, "timezone", "UTC");
    state_profile_set(&session, "work_mem", "64MB");
    state_profile_set(&session, "statement_timeout", "5000");

    /* Backend starts clean */
    state_profile_t backend;
    state_profile_init(&backend);

    /* Generate diff SQL */
    state_sync_result_t result;
    int rc = generate_sync_sql(&backend, &session, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, true);
    TEST_ASSERT_EQ(result.set_count, 4);
    TEST_ASSERT_EQ(result.reset_count, 0);
    TEST_ASSERT(result.sql_len > 0);

    /* "Apply" the sync by copying (simulates backend executing the SQL) */
    state_profile_copy(&backend, &session);
    TEST_ASSERT(state_profile_equal(&backend, &session));

    /* Now diff should produce nothing */
    rc = generate_sync_sql(&backend, &session, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, false);

    /* Session changes one param */
    state_profile_set(&session, "work_mem", "128MB");
    rc = generate_sync_sql(&backend, &session, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(result.needs_sync, true);
    TEST_ASSERT_EQ(result.set_count, 1);
    TEST_ASSERT_EQ(result.reset_count, 0);
    TEST_ASSERT(strstr(result.sql, "SET work_mem = '128MB';") != NULL);
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("=== Session-Context Preservation Tests ===\n\n");

    test_profile_operations();
    test_hash_computation();
    test_diff_sql_generation();
    test_multi_param_compose();
    test_profile_equality();
    test_round_trip();

    printf("\n--- Results: %d/%d passed, %d failed ---\n",
           g_tests_passed, g_tests_run, g_tests_failed);

    return g_tests_failed > 0 ? 1 : 0;
}
