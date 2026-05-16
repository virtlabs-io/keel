/**
 * @file test_session_hooks_matrix.c
 * @brief Combinatorial matrix testing hooks, route mutation, pin state, and
 *        migration eligibility interactions.
 *
 * Hooks are the primary extension point for query-level policy. This suite
 * verifies that hook fire order, context population, abort semantics, route
 * overrides, and pin-state side effects compose correctly across the four hook
 * points. The combinatorial style intentionally mixes axes that are easy to
 * break in isolation when refactoring the engine flow path.
 */

#include "test_utils.h"
#include "keel_hook.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"

#include <string.h>
#include <stdio.h>

/* ---- Globals ---- */
int g_tests_run, g_tests_passed, g_tests_failed;

/* ---- PG vtable ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire-protocol builders
 * ============================================================================ */

static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static size_t build_startup(uint8_t *buf, const char *user, const char *db) {
    uint8_t *p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5);     p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1);  p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1);    p += dl + 1;
    *p++ = '\0';
    wr32(buf, (uint32_t)(p - buf));
    return (size_t)(p - buf);
}

static size_t build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

static size_t build_named_parse(uint8_t *buf, const char *name, const char *query) {
    size_t nl = strlen(name);
    size_t ql = strlen(query);
    size_t body = nl + 1 + ql + 1 + 2;
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body));
    memcpy(buf + 5, name, nl + 1);
    memcpy(buf + 5 + nl + 1, query, ql + 1);
    buf[5 + nl + 1 + ql + 1] = 0;
    buf[5 + nl + 1 + ql + 2] = 0;
    return 1 + 4 + body;
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

static void *create_and_startup(void) {
    void *ctx = VT->create_context(NULL);
    if (!ctx) return NULL;
    uint8_t buf[512];
    size_t len = build_startup(buf, "alice", "testdb");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);
    return ctx;
}

/* Hook callbacks */
static int g_fire_count;
static int g_order_idx;
static int g_order_log[16];
static keel_hook_route_t g_last_route;
static bool g_last_needs_primary;

static void reset_counters(void) {
    g_fire_count = 0;
    g_order_idx = 0;
    g_last_route = 0;
    g_last_needs_primary = false;
    memset(g_order_log, 0, sizeof(g_order_log));
}

static bool hook_count(keel_hook_ctx_t *ctx) {
    (void)ctx;
    g_fire_count++;
    return true;
}

static bool hook_abort(keel_hook_ctx_t *ctx) {
    g_fire_count++;
    snprintf(ctx->error_msg, sizeof(ctx->error_msg), "aborted");
    return false;
}

static bool hook_force_primary(keel_hook_ctx_t *ctx) {
    ctx->route_hint = KEEL_HOOK_ROUTE_PRIMARY;
    ctx->needs_primary = true;
    g_fire_count++;
    return true;
}

static bool hook_record_context(keel_hook_ctx_t *ctx) {
    g_fire_count++;
    g_last_route = ctx->route_hint;
    g_last_needs_primary = ctx->needs_primary;
    return true;
}

static bool hook_check_sql(keel_hook_ctx_t *ctx) {
    g_fire_count++;
    /* Verify SQL text is populated */
    if (ctx->sql_text && ctx->sql_text_len > 0) {
        g_order_log[g_order_idx++] = 1;
    }
    return true;
}

static bool hook_check_intx(keel_hook_ctx_t *ctx) {
    g_fire_count++;
    /* Record whether the hook sees in_transaction */
    g_order_log[g_order_idx++] = ctx->in_transaction ? 1 : 0;
    return true;
}

static bool hook_prio_100(keel_hook_ctx_t *ctx) {
    (void)ctx;
    g_order_log[g_order_idx++] = 100;
    return true;
}

static bool hook_prio_200(keel_hook_ctx_t *ctx) {
    (void)ctx;
    g_order_log[g_order_idx++] = 200;
    return true;
}

static bool hook_prio_300(keel_hook_ctx_t *ctx) {
    (void)ctx;
    g_order_log[g_order_idx++] = 300;
    return true;
}

static bool hook_disable_splice(keel_hook_ctx_t *ctx) {
    ctx->splice_eligible = false;
    g_fire_count++;
    return true;
}

/* ============================================================================
 * 1) Hook fire at all 4 points with SQL context
 * ============================================================================ */

static void test_hook_fire_all_points(void) {
    keel_hook_point_t points[] = {
        KEEL_HOOK_AFTER_QUERY_READ,
        KEEL_HOOK_AFTER_QUERY_PARSE,
        KEEL_HOOK_BEFORE_ROUTE,
        KEEL_HOOK_BEFORE_SEND,
    };
    const char *names[] = { "after_read", "after_parse", "before_route", "before_send" };

    for (size_t i = 0; i < 4; i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "hook_fire_point/%s: SQL context populated", names[i]);
        TEST_BEGIN(desc);

        keel_hook_init();
        reset_counters();

        keel_hook_handle_t *h = keel_hook_register(
            NULL, points[i], "test", hook_check_sql, 100, NULL);
        TEST_ASSERT_NOT_NULL(h);

        keel_hook_ctx_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.session_id = 1;
        ctx.username = "alice";
        ctx.database = "testdb";
        ctx.sql_text = "SELECT 1";
        ctx.sql_text_len = 8;

        bool ok = keel_hook_fire(NULL, points[i], &ctx);
        TEST_ASSERT(ok);
        TEST_ASSERT_EQ(g_fire_count, 1);
        TEST_ASSERT_EQ(g_order_log[0], 1); /* SQL was populated */

        keel_hook_unregister(NULL, h);
        keel_hook_shutdown();
        TEST_END();
    }
}

/* ============================================================================
 * 2) Hook abort stops chain — verify partial execution
 * ============================================================================ */

static void test_hook_abort_stops_chain(void) {
    TEST_BEGIN("hook_abort_chain: abort at prio 20 stops prio 30");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t *h1 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "pass10", hook_count, 10, NULL);
    keel_hook_handle_t *h2 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "abort20", hook_abort, 20, NULL);
    keel_hook_handle_t *h3 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "pass30", hook_count, 30, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.session_id = 1;
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    bool ok = keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT(!ok);
    /* h1 (pass) + h2 (abort) = 2 fires, h3 never reached */
    TEST_ASSERT_EQ(g_fire_count, 2);
    TEST_ASSERT(strlen(ctx.error_msg) > 0);

    keel_hook_unregister(NULL, h1);
    keel_hook_unregister(NULL, h2);
    keel_hook_unregister(NULL, h3);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 3) Hook route mutation — BEFORE_ROUTE changes route_hint
 * ============================================================================ */

static void test_hook_route_mutation(void) {
    TEST_BEGIN("hook_route_mutation: BEFORE_ROUTE forces PRIMARY");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t *h1 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "force_primary", hook_force_primary, 100, NULL);
    keel_hook_handle_t *h2 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "record", hook_record_context, 200, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.session_id = 1;
    ctx.route_hint = KEEL_HOOK_ROUTE_ANY;  /* Initially any */
    ctx.needs_primary = false;

    bool ok = keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT(ok);
    /* After force_primary ran at prio 100, record at 200 should see PRIMARY */
    TEST_ASSERT_EQ(g_last_route, KEEL_HOOK_ROUTE_PRIMARY);
    TEST_ASSERT(g_last_needs_primary);

    /* The context itself should also be mutated */
    TEST_ASSERT_EQ(ctx.route_hint, KEEL_HOOK_ROUTE_PRIMARY);
    TEST_ASSERT(ctx.needs_primary);

    keel_hook_unregister(NULL, h1);
    keel_hook_unregister(NULL, h2);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 4) Hook priority ordering — fire in ascending priority order
 * ============================================================================ */

static void test_hook_priority_ordering(void) {
    TEST_BEGIN("hook_priority: fire order 100 → 200 → 300");

    keel_hook_init();
    reset_counters();

    /* Register in reverse order */
    keel_hook_handle_t *h3 = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "p300", hook_prio_300, 300, NULL);
    keel_hook_handle_t *h1 = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "p100", hook_prio_100, 100, NULL);
    keel_hook_handle_t *h2 = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "p200", hook_prio_200, 200, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx);
    TEST_ASSERT_EQ(g_order_idx, 3);
    TEST_ASSERT_EQ(g_order_log[0], 100);
    TEST_ASSERT_EQ(g_order_log[1], 200);
    TEST_ASSERT_EQ(g_order_log[2], 300);

    keel_hook_unregister(NULL, h1);
    keel_hook_unregister(NULL, h2);
    keel_hook_unregister(NULL, h3);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 5) Hook × transaction context
 *
 * Verify hooks see correct in_transaction state.
 * ============================================================================ */

static void test_hook_tx_context(void) {
    TEST_BEGIN("hook_tx_context: in_transaction propagated to hook");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t *h = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_READ, "tx_check", hook_check_intx, 100, NULL);

    /* Fire with in_transaction = false */
    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.in_transaction = false;
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;
    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);
    TEST_ASSERT_EQ(g_order_log[0], 0); /* false */

    /* Fire with in_transaction = true */
    reset_counters();
    ctx.in_transaction = true;
    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);
    TEST_ASSERT_EQ(g_order_log[0], 1); /* true */

    keel_hook_unregister(NULL, h);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 6) Hook splice_eligible mutation
 *
 * A hook at BEFORE_SEND disables splice eligibility.
 * ============================================================================ */

static void test_hook_splice_mutation(void) {
    TEST_BEGIN("hook_splice_mutation: BEFORE_SEND disables splice");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t *h = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_SEND, "no_splice", hook_disable_splice, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.splice_eligible = true;  /* Initially eligible */
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    bool ok = keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SEND, &ctx);
    TEST_ASSERT(ok);
    TEST_ASSERT(!ctx.splice_eligible); /* Disabled by hook */

    keel_hook_unregister(NULL, h);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 7) Hook stats accumulation
 * ============================================================================ */

static void test_hook_stats_accumulation(void) {
    TEST_BEGIN("hook_stats: fire_count increments across calls");

    keel_hook_init();

    keel_hook_handle_t *h = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_READ, "stats", hook_count, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    for (int i = 0; i < 10; i++) {
        keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);
    }

    keel_hook_stats_t stats = keel_hook_get_stats(NULL, KEEL_HOOK_AFTER_QUERY_READ);
    TEST_ASSERT_EQ(stats.fire_count, (uint64_t)10);
    TEST_ASSERT_EQ(stats.abort_count, (uint64_t)0);
    TEST_ASSERT_EQ(stats.hook_count, (uint32_t)1);

    keel_hook_unregister(NULL, h);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 8) Hook unregister mid-flow — subsequent fires skip removed hook
 * ============================================================================ */

static void test_hook_unregister_midflow(void) {
    TEST_BEGIN("hook_unregister: removed hook not fired");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t *h = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "temp", hook_count, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT_EQ(g_fire_count, 1);

    keel_hook_unregister(NULL, h);
    reset_counters();

    keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT_EQ(g_fire_count, 0); /* Hook removed */

    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 9) Hook user_data across multiple fires
 * ============================================================================ */

static bool hook_user_data_counter(keel_hook_ctx_t *ctx) {
    int *counter = (int *)ctx->user_data;
    if (counter) (*counter)++;
    return true;
}

static void test_hook_user_data(void) {
    TEST_BEGIN("hook_user_data: counter incremented via user_data");

    keel_hook_init();

    int counter = 0;
    keel_hook_handle_t *h = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_READ, "ud", hook_user_data_counter, 100, &counter);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    for (int i = 0; i < 5; i++) {
        keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);
    }
    TEST_ASSERT_EQ(counter, 5);

    keel_hook_unregister(NULL, h);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 10) Hook × PS flow — hook sees SQL from Parse message
 *
 * When processing a named Parse in the protocol flow, the hook context
 * should receive the SQL text from the Parse message.
 * ============================================================================ */

static void test_hook_with_ps_flow(void) {
    TEST_BEGIN("hook_ps_flow: hook context populated during PS");

    /* This test verifies the hook API independently, crossing with
     * protocol flow concepts.  We populate the hook context as the
     * engine would after on_fe_msg returns a QUERY action. */
    keel_hook_init();
    reset_counters();

    keel_hook_handle_t *h = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "ps_sql", hook_check_sql, 100, NULL);

    /* Simulate what the engine does after protocol plugin returns */
    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    size_t len = build_named_parse(buf, "hookstmt", "INSERT INTO t VALUES ($1)");
    VT->on_fe_msg(ctx, buf, len, &act);

    /* The engine would fire hooks with the SQL from the action */
    keel_hook_ctx_t hctx;
    memset(&hctx, 0, sizeof(hctx));
    hctx.session_id = 42;
    hctx.username = "alice";
    hctx.database = "testdb";
    hctx.sql_text = act.sql_view;
    hctx.sql_text_len = act.sql_view_len;
    hctx.effect_flags = act.effect;

    bool ok = keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &hctx);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(g_fire_count, 1);
    /* hook_check_sql sets order_log[0]=1 if sql_text was non-NULL and non-empty */
    TEST_ASSERT_EQ(g_order_log[0], 1);

    VT->destroy_context(ctx);
    keel_hook_unregister(NULL, h);
    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 11) Double init/shutdown safety
 * ============================================================================ */

static void test_hook_double_init(void) {
    TEST_BEGIN("hook_safety: double init/shutdown is safe");

    keel_hook_init();
    keel_hook_init(); /* Second init — should be safe */

    keel_hook_shutdown();
    keel_hook_shutdown(); /* Second shutdown — should be safe */

    TEST_END();
}

/* ============================================================================
 * 12) Fire on empty point — returns true (no abort)
 * ============================================================================ */

static void test_hook_fire_empty(void) {
    TEST_BEGIN("hook_empty: fire on point with no hooks → true");

    keel_hook_init();

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.sql_text = "SELECT 1";
    ctx.sql_text_len = 8;

    bool ok = keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT(ok);

    keel_hook_shutdown();
    TEST_END();
}

/* ============================================================================
 * 13) Hook × query type matrix
 *
 * Fire hooks with different query types and verify context is correct.
 * ============================================================================ */

static void test_hook_query_type_matrix(void) {
    struct { const char *sql; uint32_t expect_effect; } cases[] = {
        { "SELECT 1",                  KEEL_QE_READONLY },
        { "INSERT INTO t VALUES (1)",  KEEL_QE_WRITE },
        { "BEGIN",                     KEEL_QE_BEGINS_TX },
        { "COMMIT",                    KEEL_QE_ENDS_TX },
        { "CREATE TABLE t (id int)",   KEEL_QE_DDL | KEEL_QE_WRITE },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "hook_qtype/%zu: %s effect flags", i, cases[i].sql);
        TEST_BEGIN(desc);

        keel_hook_init();
        reset_counters();

        keel_hook_handle_t *h = keel_hook_register(
            NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "qtype", hook_count, 100, NULL);

        /* Get effect flags from protocol */
        void *ctx = create_and_startup();
        uint8_t buf[256];
        keel_fe_action_t act;
        size_t len = build_query(buf, cases[i].sql);
        VT->on_fe_msg(ctx, buf, len, &act);

        /* Populate hook context with protocol output */
        keel_hook_ctx_t hctx;
        memset(&hctx, 0, sizeof(hctx));
        hctx.sql_text = act.sql_view;
        hctx.sql_text_len = act.sql_view_len;
        hctx.effect_flags = act.effect;

        keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &hctx);

        /* Verify effect flags match */
        TEST_ASSERT(hctx.effect_flags & cases[i].expect_effect);

        VT->destroy_context(ctx);
        keel_hook_unregister(NULL, h);
        keel_hook_shutdown();
        TEST_END();
    }
}

/* ============================================================================
 * 14) Hook × quarantine queries (LISTEN, TEMP TABLE, PREPARE)
 *
 * Verify hooks receive POTENTIALLY_STATEFUL effect flags.
 * ============================================================================ */

static void test_hook_quarantine_context(void) {
    const char *quarantine_queries[] = {
        "LISTEN my_channel",
        "CREATE TEMP TABLE tmp (id int)",
        "DECLARE mycur CURSOR FOR SELECT 1",
    };

    for (size_t i = 0; i < sizeof(quarantine_queries) / sizeof(quarantine_queries[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "hook_quarantine/%zu: %s → POTENTIALLY_STATEFUL", i, quarantine_queries[i]);
        TEST_BEGIN(desc);

        void *ctx = create_and_startup();
        uint8_t buf[256];
        keel_fe_action_t act;
        size_t len = build_query(buf, quarantine_queries[i]);
        VT->on_fe_msg(ctx, buf, len, &act);

        TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * main()
 * ============================================================================ */

int main(void) {
    printf("=== Session × Hooks × Route Combinatorial Matrix ===\n\n");

    test_hook_fire_all_points();
    test_hook_abort_stops_chain();
    test_hook_route_mutation();
    test_hook_priority_ordering();
    test_hook_tx_context();
    test_hook_splice_mutation();
    test_hook_stats_accumulation();
    test_hook_unregister_midflow();
    test_hook_user_data();
    test_hook_with_ps_flow();
    test_hook_double_init();
    test_hook_fire_empty();
    test_hook_query_type_matrix();
    test_hook_quarantine_context();

    printf("\n--- Results: %d run, %d passed, %d failed ---\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return g_tests_failed > 0 ? 1 : 0;
}
