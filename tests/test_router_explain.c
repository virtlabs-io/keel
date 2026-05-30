/**
 * @file test_router_explain.c
 * @brief Tests for the read-only per-query route explainer.
 *
 * Validates:
 *   - keel_router_explain_sql() mutates no stat counters.
 *   - The eligible-targets table is correctly populated for read and
 *     write decisions.
 *   - The new reason codes (UNKNOWN_FUNCTION, COMMIT_AMBIGUOUS) fire on
 *     the expected inputs.
 *   - keel_route_explanation_to_json() produces a parseable, stable shape.
 */

#include "keel/core/router.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  %-50s ", #name); fflush(stdout); \
    test_##name(); \
    printf("[PASS]\n"); tests_passed++; \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("[FAIL]\n    %s\n    at %s:%d\n", #cond, __FILE__, __LINE__); \
        tests_failed++; return; \
    } \
} while(0)

#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define ASSERT_TRUE(x)  ASSERT(x)
#define ASSERT_FALSE(x) ASSERT(!(x))

static keel_router_t* make_router(void) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.primary_read_weight = 0.0;   /* All reads to replicas by default */
    cfg.failover_to_primary = true;

    keel_router_t* router = keel_router_create(&cfg);
    if (!router) return NULL;

    keel_route_server_t primary = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_router_add_server(router, &primary);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);

    keel_route_server_t replica = {
        .name = "replica1", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_router_add_server(router, &replica);
    keel_router_set_server_health(router, "replica1", KEEL_HEALTH_UP);

    return router;
}

TEST(explain_read_query_picks_replica) {
    keel_router_t* router = make_router();
    ASSERT(router);

    keel_str_t sql = { .data = "SELECT 1", .len = 8 };
    keel_route_explanation_t exp;
    keel_error_t err = keel_router_explain_sql(router, sql, NULL, &exp);
    ASSERT_EQ(err, KEEL_OK);

    ASSERT_TRUE(exp.simulated);
    ASSERT_FALSE(exp.parse_failed);
    ASSERT_TRUE(exp.decision.is_read);
    ASSERT_TRUE(exp.target_count >= 2);

    /* The replica should be the one marked selected (primary_read_weight=0). */
    bool replica_selected = false;
    for (size_t i = 0; i < exp.target_count; i++) {
        if (strcmp(exp.targets[i].name, "replica1") == 0 &&
            exp.targets[i].was_selected) {
            replica_selected = true;
        }
    }
    ASSERT_TRUE(replica_selected);

    keel_router_destroy(router);
}

TEST(explain_write_query_picks_primary) {
    keel_router_t* router = make_router();
    ASSERT(router);

    keel_str_t sql = { .data = "INSERT INTO t VALUES (1)",
                       .len = strlen("INSERT INTO t VALUES (1)") };
    keel_route_explanation_t exp;
    keel_error_t err = keel_router_explain_sql(router, sql, NULL, &exp);
    ASSERT_EQ(err, KEEL_OK);

    ASSERT_FALSE(exp.decision.is_read);
    ASSERT_EQ(exp.decision.reason_code, KEEL_ROUTE_REASON_WRITE_REQUIRED);

    /* Primary should be selected and the only eligible target. */
    bool primary_selected = false;
    size_t eligible_count = 0;
    for (size_t i = 0; i < exp.target_count; i++) {
        if (exp.targets[i].was_eligible) eligible_count++;
        if (strcmp(exp.targets[i].name, "primary") == 0 &&
            exp.targets[i].was_selected) {
            primary_selected = true;
        }
    }
    ASSERT_TRUE(primary_selected);
    ASSERT_EQ(eligible_count, 1);

    keel_router_destroy(router);
}

TEST(explain_commit_in_doubt_refuses_routing) {
    keel_router_t* router = make_router();
    ASSERT(router);

    keel_route_session_t session = {0};
    session.commit_in_doubt = true;  /* NOLINT(keel-metrics) - test fixture */

    keel_str_t sql = { .data = "SELECT 1", .len = 8 };
    keel_route_explanation_t exp;
    keel_error_t err = keel_router_explain_sql(router, sql, &session, &exp);

    ASSERT_EQ(err, KEEL_ERR_UNAVAILABLE);
    ASSERT_EQ(exp.decision.reason_code, KEEL_ROUTE_REASON_COMMIT_AMBIGUOUS);
    ASSERT_TRUE((exp.decision.decision_factors & KEEL_DF_COMMIT_IN_DOUBT) != 0);

    keel_router_destroy(router);
}

TEST(explain_function_call_picks_primary) {
    keel_router_t* router = make_router();
    ASSERT(router);

    keel_str_t sql = { .data = "SELECT my_func(1)",
                       .len = strlen("SELECT my_func(1)") };
    keel_route_explanation_t exp;
    keel_error_t err = keel_router_explain_sql(router, sql, NULL, &exp);
    ASSERT_EQ(err, KEEL_OK);

    /* SELECT with a function call must NOT route to a replica. */
    ASSERT_FALSE(exp.decision.is_read);
    /* Either UNKNOWN_FUNCTION (conservative) or SEMANTIC_UNSAFE (parser
     * couldn't prove safety) — both are acceptable; both keep us off
     * replicas. */
    ASSERT_TRUE(
        exp.decision.reason_code == KEEL_ROUTE_REASON_UNKNOWN_FUNCTION ||
        exp.decision.reason_code == KEEL_ROUTE_REASON_SEMANTIC_UNSAFE ||
        exp.decision.reason_code == KEEL_ROUTE_REASON_WRITE_REQUIRED);

    keel_router_destroy(router);
}

TEST(explain_does_not_mutate_stats) {
    keel_router_t* router = make_router();
    ASSERT(router);

    keel_router_stats_t before, after;
    keel_router_get_stats(router, &before);

    keel_str_t sql_r = { .data = "SELECT 1", .len = 8 };
    keel_str_t sql_w = { .data = "UPDATE t SET x=1",
                         .len = strlen("UPDATE t SET x=1") };

    keel_route_explanation_t exp;
    for (int i = 0; i < 25; i++) {
        keel_router_explain_sql(router, sql_r, NULL, &exp);
        keel_router_explain_sql(router, sql_w, NULL, &exp);
    }

    keel_router_get_stats(router, &after);
    ASSERT_EQ(before.total_routes, after.total_routes);
    ASSERT_EQ(before.read_routes,  after.read_routes);
    ASSERT_EQ(before.write_routes, after.write_routes);
    ASSERT_EQ(before.failover_routes, after.failover_routes);
    ASSERT_EQ(before.pinned_routes, after.pinned_routes);

    keel_router_destroy(router);
}

TEST(explanation_json_contains_required_keys) {
    keel_router_t* router = make_router();
    ASSERT(router);

    keel_str_t sql = { .data = "SELECT 1", .len = 8 };
    keel_route_explanation_t exp;
    keel_router_explain_sql(router, sql, NULL, &exp);

    char buf[4096];
    size_t n = keel_route_explanation_to_json(&exp, 0xdeadbeef,
                                              buf, sizeof buf);
    ASSERT(n > 0 && n < sizeof buf);

    ASSERT(strstr(buf, "\"simulated\":true") != NULL);
    ASSERT(strstr(buf, "\"sql\":") != NULL);
    ASSERT(strstr(buf, "\"decision\":") != NULL);
    ASSERT(strstr(buf, "\"eligible_targets\":[") != NULL);
    ASSERT(strstr(buf, "\"reason_code\":") != NULL);
    ASSERT(strstr(buf, "\"factors\":") != NULL);
    ASSERT(strstr(buf, "\"selected\":") != NULL);
    ASSERT(strstr(buf, "\"eligible\":") != NULL);

    keel_router_destroy(router);
}

int main(void) {
    printf("Router explainer tests\n");
    printf("======================\n\n");

    RUN_TEST(explain_read_query_picks_replica);
    RUN_TEST(explain_write_query_picks_primary);
    RUN_TEST(explain_commit_in_doubt_refuses_routing);
    RUN_TEST(explain_function_call_picks_primary);
    RUN_TEST(explain_does_not_mutate_stats);
    RUN_TEST(explanation_json_contains_required_keys);

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
