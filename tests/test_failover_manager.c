/**
 * @file test_failover_manager.c
 * @brief Unit tests for the per-router failover manager.
 *
 * Covers:
 *   - Cluster epoch starts at 0, bumps to 1 on first primary observation.
 *   - Re-observing the same (primary, timeline) is a no-op.
 *   - A primary flip with default `failover.old_primary_fencing_required=true`
 *     marks the previous primary DEMOTED and excludes it from routing.
 *   - With fencing disabled the previous primary becomes DRAINING instead.
 *   - keel_router_observe_primary(NULL) enters degraded mode and a follow-up
 *     read is rejected when `read_during_failover=REJECT`.
 *   - Consistency-token timeline mismatch returns KEEL_ERR_UNAVAILABLE with
 *     reason_code=KEEL_ROUTE_REASON_TIMELINE_STALE.
 *
 * The tests use the public router API only — no engine plumbing — so they
 * are fast and deterministic.
 */

#include "keel/core/router.h"
#include "keel/sql/sql.h"
#include "keel/sql/query_tree.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------------- */
/* Minimal local test harness (consistent with test_router.c style)          */
/* ------------------------------------------------------------------------- */

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do {                                                  \
    printf("  %-60s ", #name);                                               \
    fflush(stdout);                                                          \
    test_##name();                                                           \
    printf("[PASS]\n");                                                      \
    tests_passed++;                                                          \
} while (0)

#define ASSERT(cond) do {                                                    \
    if (!(cond)) {                                                           \
        printf("[FAIL]\n    Assertion failed: %s\n    at %s:%d\n",           \
               #cond, __FILE__, __LINE__);                                   \
        tests_failed++;                                                      \
        return;                                                              \
    }                                                                        \
} while (0)

#define ASSERT_EQ(a, b)     ASSERT((a) == (b))
#define ASSERT_NE(a, b)     ASSERT((a) != (b))
#define ASSERT_TRUE(x)      ASSERT(x)
#define ASSERT_FALSE(x)     ASSERT(!(x))
#define ASSERT_STR_EQ(a, b) ASSERT(strcmp((a), (b)) == 0)

/* ------------------------------------------------------------------------- */
/* Fixture: 1 primary (timeline=1) + 1 replica (timeline=1).                 */
/* ------------------------------------------------------------------------- */

static keel_router_t* make_router_2node(void)
{
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);
    if (!router) return NULL;

    keel_route_server_t primary = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t replica = {
        .name = "replica", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    if (keel_router_add_server(router, &primary) != KEEL_OK ||
        keel_router_add_server(router, &replica) != KEEL_OK) {
        keel_router_destroy(router);
        return NULL;
    }
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);
    keel_router_set_server_health(router, "replica", KEEL_HEALTH_UP);
    return router;
}

/* ------------------------------------------------------------------------- */
/* §1 — Cluster epoch lifecycle                                              */
/* ------------------------------------------------------------------------- */

TEST(epoch_starts_zero) {
    keel_router_t* router = make_router_2node();
    ASSERT_NE(router, NULL);

    keel_cluster_epoch_t e;
    keel_router_get_cluster_epoch(router, &e);
    ASSERT_EQ(e.generation, 0u);
    ASSERT_EQ(e.primary_name[0], '\0');

    keel_router_destroy(router);
}

TEST(observe_primary_first_time_bumps_to_1) {
    keel_router_t* router = make_router_2node();
    ASSERT_NE(router, NULL);

    bool flipped = keel_router_observe_primary(router, "primary", 1);
    ASSERT_TRUE(flipped);

    keel_cluster_epoch_t e;
    keel_router_get_cluster_epoch(router, &e);
    ASSERT_EQ(e.generation, 1u);
    ASSERT_STR_EQ(e.primary_name, "primary");
    ASSERT_EQ(e.timeline_id, 1u);
    ASSERT_EQ(keel_router_get_node_role_state(router, "primary"),
              KEEL_NODE_STATE_PRIMARY);

    keel_router_destroy(router);
}

TEST(observe_same_primary_is_noop) {
    keel_router_t* router = make_router_2node();
    ASSERT_NE(router, NULL);

    ASSERT_TRUE(keel_router_observe_primary(router, "primary", 1));
    ASSERT_FALSE(keel_router_observe_primary(router, "primary", 1));
    ASSERT_FALSE(keel_router_observe_primary(router, "primary", 1));

    keel_cluster_epoch_t e;
    keel_router_get_cluster_epoch(router, &e);
    ASSERT_EQ(e.generation, 1u);

    keel_router_destroy(router);
}

/* ------------------------------------------------------------------------- */
/* §2 — Primary flip and fencing                                             */
/* ------------------------------------------------------------------------- */

TEST(primary_flip_fences_old_primary) {
    keel_router_t* router = make_router_2node();
    ASSERT_NE(router, NULL);

    /* Promote 'primary' first, then flip to 'replica' as the new primary. */
    ASSERT_TRUE(keel_router_observe_primary(router, "primary", 1));
    ASSERT_TRUE(keel_router_observe_primary(router, "replica", 2));

    keel_cluster_epoch_t e;
    keel_router_get_cluster_epoch(router, &e);
    ASSERT_EQ(e.generation, 2u);
    ASSERT_STR_EQ(e.primary_name, "replica");
    ASSERT_EQ(e.timeline_id, 2u);

    /* Default config has fencing required — old primary is DEMOTED. */
    ASSERT_EQ(keel_router_get_node_role_state(router, "primary"),
              KEEL_NODE_STATE_DEMOTED);
    ASSERT_EQ(keel_router_get_node_role_state(router, "replica"),
              KEEL_NODE_STATE_PRIMARY);

    /* The demoted node must not receive new writes. Run several writes and
     * confirm none land on 'primary'. */
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    for (int i = 0; i < 20; i++) {
        keel_qt_query_t* qt = keel_sql_analyze_full(
            KEEL_STR("UPDATE t SET x = 1"), arena);
        ASSERT_NE(qt, NULL);
        keel_route_decision_t d;
        ASSERT_EQ(keel_router_route(router, qt, NULL, &d), KEEL_OK);
        ASSERT_NE(d.server, NULL);
        ASSERT_STR_EQ(d.server->name, "replica");
    }
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(primary_flip_without_fencing_uses_draining) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.failover.old_primary_fencing_required = false;
    keel_router_t* router = keel_router_create(&cfg);
    ASSERT_NE(router, NULL);

    keel_route_server_t a = {
        .name = "a", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t b = {
        .name = "b", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(router, &a), KEEL_OK);
    ASSERT_EQ(keel_router_add_server(router, &b), KEEL_OK);

    ASSERT_TRUE(keel_router_observe_primary(router, "a", 1));
    ASSERT_TRUE(keel_router_observe_primary(router, "b", 2));

    ASSERT_EQ(keel_router_get_node_role_state(router, "a"),
              KEEL_NODE_STATE_DRAINING);
    ASSERT_EQ(keel_router_get_node_role_state(router, "b"),
              KEEL_NODE_STATE_PRIMARY);

    keel_router_destroy(router);
}

/* ------------------------------------------------------------------------- */
/* §3 — Degraded mode                                                        */
/* ------------------------------------------------------------------------- */

TEST(observe_null_enters_degraded_and_rejects_reads) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.failover.read_during_failover = KEEL_FAILOVER_READ_REJECT;
    keel_router_t* router = keel_router_create(&cfg);
    ASSERT_NE(router, NULL);

    keel_route_server_t primary = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t replica = {
        .name = "replica", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(router, &primary), KEEL_OK);
    ASSERT_EQ(keel_router_add_server(router, &replica), KEEL_OK);

    /* Observe primary, then signal "lost". */
    keel_router_observe_primary(router, "primary", 1);
    keel_router_observe_primary(router, NULL, 0);

    /* A read should now be rejected with DEGRADED_MODE. */
    keel_arena_t* arena = keel_arena_create(4096);
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT 1"), arena);
    ASSERT_NE(qt, NULL);
    keel_route_decision_t d;
    keel_error_t err = keel_router_route(router, qt, NULL, &d);
    ASSERT_EQ(err, KEEL_ERR_UNAVAILABLE);
    ASSERT_EQ(d.reason_code, KEEL_ROUTE_REASON_DEGRADED_MODE);
    ASSERT_TRUE((d.decision_factors & KEEL_DF_DEGRADED_MODE) != 0);

    /* Re-confirming the primary should exit degraded mode. */
    keel_router_observe_primary(router, "primary", 1);
    qt = keel_sql_analyze_full(KEEL_STR("SELECT 1"), arena);
    ASSERT_EQ(keel_router_route(router, qt, NULL, &d), KEEL_OK);

    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ------------------------------------------------------------------------- */
/* §4 — Timeline-mismatch rejection on the consistency path                  */
/* ------------------------------------------------------------------------- */

TEST(timeline_mismatch_rejects_consistent_read) {
    /* Use PRIMARY_ONLY consistency: every replica-safe read is forced to
     * primary via `consistency_forces_primary()`, bypassing the early
     * REJECT block. Combined with `stale_read_policy=REJECT`, the
     * timeline-mismatch check fires and rejects the route. */
    keel_router_config_t cfg = keel_router_config_default();
    cfg.consistency_mode    = KEEL_CONSISTENCY_PRIMARY_ONLY;
    cfg.stale_read_policy   = KEEL_STALE_READ_REJECT;
    keel_router_t* router = keel_router_create(&cfg);
    ASSERT_NE(router, NULL);

    keel_route_server_t primary = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(router, &primary), KEEL_OK);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);

    keel_arena_t* arena = keel_arena_create(4096);
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM t WHERE id = 1"), arena);
    ASSERT_NE(qt, NULL);

    /* Session requires a different timeline than what the primary is on. */
    keel_route_session_t s;
    memset(&s, 0, sizeof(s));
    s.required_timeline_id = 5;   /* stale: primary advanced to 7 */
    keel_route_decision_t d;
    keel_error_t err = keel_router_route(router, qt, &s, &d);
    ASSERT_EQ(err, KEEL_ERR_UNAVAILABLE);
    ASSERT_EQ(d.reason_code, KEEL_ROUTE_REASON_TIMELINE_STALE);
    ASSERT_EQ(d.server, NULL);

    /* Same setup but matching timelines: must succeed. */
    s.required_timeline_id = 7;
    keel_qt_query_t* qt2 = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM t WHERE id = 1"), arena);
    ASSERT_EQ(keel_router_route(router, qt2, &s, &d), KEEL_OK);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");

    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ------------------------------------------------------------------------- */
/* §5 — Manual override                                                      */
/* ------------------------------------------------------------------------- */

TEST(manual_set_node_role_state_removes_from_pool) {
    keel_router_t* router = make_router_2node();
    ASSERT_NE(router, NULL);

    keel_router_observe_primary(router, "primary", 1);

    /* Manually drain the replica. */
    ASSERT_EQ(
        keel_router_set_node_role_state(router, "replica",
                                        KEEL_NODE_STATE_DRAINING),
        KEEL_OK);
    ASSERT_EQ(keel_router_get_node_role_state(router, "replica"),
              KEEL_NODE_STATE_DRAINING);

    /* Reads should now never land on the drained replica. */
    keel_arena_t* arena = keel_arena_create(4096);
    for (int i = 0; i < 20; i++) {
        keel_qt_query_t* qt = keel_sql_analyze_full(
            KEEL_STR("SELECT 1"), arena);
        ASSERT_NE(qt, NULL);
        keel_route_decision_t d;
        ASSERT_EQ(keel_router_route(router, qt, NULL, &d), KEEL_OK);
        ASSERT_NE(d.server, NULL);
        ASSERT_STR_EQ(d.server->name, "primary");
    }

    /* Unknown server returns NOT_FOUND. */
    ASSERT_EQ(
        keel_router_set_node_role_state(router, "ghost",
                                        KEEL_NODE_STATE_DEMOTED),
        KEEL_ERR_NOT_FOUND);

    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ------------------------------------------------------------------------- */
/* §6 — stale_read_policy=WAIT (not implemented; documented fallback)        */
/* ------------------------------------------------------------------------- */

/* The proposal lists three stale_read_policy modes: route_primary, wait,
 * reject. Today only route_primary and reject are actually wired into the
 * router; WAIT requires a reactor-owned catch-up loop that is not yet
 * implemented. The router degrades WAIT to ROUTE_PRIMARY at construction
 * time so configurations carrying WAIT do not silently behave as REJECT
 * (fail-closed) or as a no-op. These tests lock in that contract.            */

TEST(wait_policy_degrades_to_route_primary) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.consistency_mode  = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy = KEEL_STALE_READ_WAIT;
    keel_router_t* router = keel_router_create(&cfg);
    ASSERT_NE(router, NULL);

    keel_route_server_t primary = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t replica = {
        .name = "replica", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(router, &primary), KEEL_OK);
    ASSERT_EQ(keel_router_add_server(router, &replica), KEEL_OK);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);
    keel_router_set_server_health(router, "replica", KEEL_HEALTH_UP);

    /* A consistent read under WAIT must NOT be rejected (that is REJECT
     * semantics). It must land somewhere — typically the primary, since
     * we have no proof the replica caught up. */
    keel_arena_t* arena = keel_arena_create(4096);
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM t WHERE id = 1"), arena);
    ASSERT_NE(qt, NULL);

    keel_route_session_t s;
    memset(&s, 0, sizeof(s));
    s.requires_consistent_read = true;

    keel_route_decision_t d;
    ASSERT_EQ(keel_router_route(router, qt, &s, &d), KEEL_OK);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");

    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(wait_policy_does_not_fail_closed_on_missing_replicas) {
    /* Even with no replicas at all, WAIT must route the consistent read
     * to primary — never reject. This confirms WAIT is NOT silently
     * aliased to REJECT (which would fail-closed here). */
    keel_router_config_t cfg = keel_router_config_default();
    cfg.consistency_mode  = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy = KEEL_STALE_READ_WAIT;
    keel_router_t* router = keel_router_create(&cfg);
    ASSERT_NE(router, NULL);

    keel_route_server_t primary = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 1,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(router, &primary), KEEL_OK);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);

    keel_arena_t* arena = keel_arena_create(4096);
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT 1"), arena);
    ASSERT_NE(qt, NULL);

    keel_route_session_t s;
    memset(&s, 0, sizeof(s));
    s.requires_consistent_read = true;

    keel_route_decision_t d;
    keel_error_t err = keel_router_route(router, qt, &s, &d);
    ASSERT_EQ(err, KEEL_OK);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");

    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ------------------------------------------------------------------------- */
/* Driver                                                                    */
/* ------------------------------------------------------------------------- */

int main(void) {
    printf("Failover-manager tests\n");
    printf("======================\n");

    RUN_TEST(epoch_starts_zero);
    RUN_TEST(observe_primary_first_time_bumps_to_1);
    RUN_TEST(observe_same_primary_is_noop);
    RUN_TEST(primary_flip_fences_old_primary);
    RUN_TEST(primary_flip_without_fencing_uses_draining);
    RUN_TEST(observe_null_enters_degraded_and_rejects_reads);
    RUN_TEST(timeline_mismatch_rejects_consistent_read);
    RUN_TEST(manual_set_node_role_state_removes_from_pool);
    RUN_TEST(wait_policy_degrades_to_route_primary);
    RUN_TEST(wait_policy_does_not_fail_closed_on_missing_replicas);

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
