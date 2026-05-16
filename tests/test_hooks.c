/**
 * @file test_hooks.c
 * @brief Unit tests for the hook/trigger system
 *
 * Tests:
 *   - Init / shutdown lifecycle
 *   - Native hook registration at all 10 points
 *   - Hook firing and priority ordering
 *   - Context mutation propagation
 *   - Abort (return false) stops chain
 *   - Statistics tracking
 *   - Unregister
 *   - New traversal hook points: AFTER_ROUTE, BEFORE_SCATTER, AFTER_SCATTER
 *   - New infrastructure hook points: ON_BACKEND_CONNECT, ON_BACKEND_DISCONNECT,
 *     ON_HEALTH_CHANGE
 *   - Extension context (ext pointer) population and access
 *   - KEEL_HOOK_FIRED_FOR zero-cost guard and keel_hook_point_name()
 *   - Full merge plan access: order_keys, agg_specs, group_key_cols, having_preds,
 *     avg_finalize_specs, window_col_specs, limit_count, limit_offset
 *   - 2PC state: twopc_required, participating_shards_mask
 *   - Scatter output: scatter_rows_merged, scatter_spilled (real values)
 *   - Query tree pointer in base context (query_tree field)
 *   - Shard key in per-shard info (shard_key field)
 */

#include "test_utils.h"
#include "keel_hook.h"

#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Test Helpers — counters and sample hooks
 * ============================================================================ */

static int g_fire_count;    /* times our hook was called */
static int g_order_idx;     /* tracks call order */
static int g_order_log[16]; /* records call sequence */

static void reset_counters(void) {
    g_fire_count = 0;
    g_order_idx  = 0;
    memset(g_order_log, 0, sizeof(g_order_log));
}

/** A hook that simply passes. */
static bool hook_pass(keel_hook_ctx_t* ctx) {
    (void)ctx;
    g_fire_count++;
    return true;
}

/** A hook that aborts. */
static bool hook_abort(keel_hook_ctx_t* ctx) {
    (void)ctx;
    g_fire_count++;
    snprintf(ctx->error_msg, sizeof(ctx->error_msg), "aborted by test");
    return false;
}

/** Priority-tracking hooks — each records its tag. */
static bool hook_order_A(keel_hook_ctx_t* ctx) {
    (void)ctx;
    g_order_log[g_order_idx++] = 1;  /* A = 1 */
    return true;
}

static bool hook_order_B(keel_hook_ctx_t* ctx) {
    (void)ctx;
    g_order_log[g_order_idx++] = 2;  /* B = 2 */
    return true;
}

static bool hook_order_C(keel_hook_ctx_t* ctx) {
    (void)ctx;
    g_order_log[g_order_idx++] = 3;  /* C = 3 */
    return true;
}

/** A hook that mutates ctx fields. */
static bool hook_mutate_route(keel_hook_ctx_t* ctx) {
    ctx->route_hint    = KEEL_HOOK_ROUTE_PRIMARY;
    ctx->needs_primary = true;
    ctx->effect_flags  = 0xCAFE;
    ctx->splice_eligible = false;
    return true;
}

/** A hook that reads user_data via ctx. */
static bool hook_check_user_data(keel_hook_ctx_t* ctx) {
    int* counter = (int*)ctx->user_data;
    if (counter) (*counter)++;
    return true;
}

/* ============================================================================
 * Tests
 * ============================================================================ */

static void test_init_shutdown(void) {
    printf("  Testing init/shutdown lifecycle...\n");

    keel_error_t err = keel_hook_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Double init should still succeed or be no-op */
    err = keel_hook_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_hook_shutdown();

    /* Double shutdown should not crash */
    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_register_and_fire(void) {
    printf("  Testing register + fire at the original 4 query-path points...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t* h0 = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_READ, "test-read", hook_pass, 100, NULL);
    keel_hook_handle_t* h1 = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "test-parse", hook_pass, 100, NULL);
    keel_hook_handle_t* h2 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_ROUTE, "test-route", hook_pass, 100, NULL);
    keel_hook_handle_t* h3 = keel_hook_register(
        NULL, KEEL_HOOK_BEFORE_SEND, "test-send", hook_pass, 100, NULL);

    TEST_ASSERT_NOT_NULL(h0);
    TEST_ASSERT_NOT_NULL(h1);
    TEST_ASSERT_NOT_NULL(h2);
    TEST_ASSERT_NOT_NULL(h3);

    /* Build a minimal context */
    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.session_id  = 42;
    ctx.username    = "alice";
    ctx.database    = "testdb";
    ctx.sql_text    = "SELECT 1";
    ctx.sql_text_len = 8;

    /* Fire each point once */
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SEND, &ctx));

    TEST_ASSERT_EQ(g_fire_count, 4);

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_abort_stops_chain(void) {
    printf("  Testing abort stops chain...\n");

    keel_hook_init();
    reset_counters();

    /* Register: pass (priority 10), abort (priority 20), pass (priority 30) */
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_ROUTE, "first-pass", hook_pass, 10, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_ROUTE, "abort-here", hook_abort, 20, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_ROUTE, "never-runs", hook_pass, 30, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.session_id = 1;
    ctx.username   = "bob";
    ctx.database   = "test";

    bool result = keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT(!result);            /* Should be aborted */
    TEST_ASSERT_EQ(g_fire_count, 2); /* first-pass + abort-here; never-runs skipped */
    TEST_ASSERT_STR_EQ(ctx.error_msg, "aborted by test");

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_priority_ordering(void) {
    printf("  Testing priority ordering...\n");

    keel_hook_init();
    reset_counters();

    /* Register in scrambled order; lower priority should fire first */
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_READ, "C-last",  hook_order_C, 300, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_READ, "A-first", hook_order_A, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_READ, "B-mid",   hook_order_B, 200, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);

    TEST_ASSERT_EQ(g_order_idx, 3);
    TEST_ASSERT_EQ(g_order_log[0], 1); /* A first */
    TEST_ASSERT_EQ(g_order_log[1], 2); /* B second */
    TEST_ASSERT_EQ(g_order_log[2], 3); /* C third */

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_context_mutation(void) {
    printf("  Testing context mutation...\n");

    keel_hook_init();

    keel_hook_register(NULL, KEEL_HOOK_BEFORE_ROUTE, "mutator",
                       hook_mutate_route, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.route_hint      = KEEL_HOOK_ROUTE_REPLICA;
    ctx.needs_primary   = false;
    ctx.effect_flags    = 0;
    ctx.splice_eligible = true;

    bool ok = keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx);
    TEST_ASSERT(ok);

    TEST_ASSERT_EQ(ctx.route_hint, KEEL_HOOK_ROUTE_PRIMARY);
    TEST_ASSERT(ctx.needs_primary);
    TEST_ASSERT_EQ(ctx.effect_flags, (uint32_t)0xCAFE);
    TEST_ASSERT(!ctx.splice_eligible);

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_user_data(void) {
    printf("  Testing user_data passthrough...\n");

    keel_hook_init();

    int counter = 0;
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "user-data-hook",
                       hook_check_user_data, 100, &counter);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx);
    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx);
    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx);

    TEST_ASSERT_EQ(counter, 3);

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_stats(void) {
    printf("  Testing hook statistics...\n");

    keel_hook_init();

    keel_hook_register(NULL, KEEL_HOOK_BEFORE_SEND, "stats-hook", hook_pass, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    /* Fire enough times that total_ns is reliably > 0 even on fast
     * hardware where a single trivial hook call rounds to 0 ns.
     * 1000 indirect function-pointer calls always accumulate measurable
     * elapsed time regardless of clock granularity. */
    const int FIRE_COUNT = 1000;
    for (int i = 0; i < FIRE_COUNT; i++) {
        keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SEND, &ctx);
    }

    keel_hook_stats_t st = keel_hook_get_stats(NULL, KEEL_HOOK_BEFORE_SEND);
    TEST_ASSERT_EQ(st.fire_count, (uint64_t)FIRE_COUNT);
    TEST_ASSERT_EQ(st.abort_count, (uint64_t)0);
    TEST_ASSERT_EQ(st.hook_count, (uint32_t)1);
    TEST_ASSERT(st.total_ns > 0);  /* Must have measured something */

    /* Unfired point should be zeroed */
    keel_hook_stats_t st2 = keel_hook_get_stats(NULL, KEEL_HOOK_AFTER_QUERY_READ);
    TEST_ASSERT_EQ(st2.fire_count, (uint64_t)0);
    TEST_ASSERT_EQ(st2.hook_count, (uint32_t)0);

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_unregister(void) {
    printf("  Testing hook unregister...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_handle_t* h = keel_hook_register(
        NULL, KEEL_HOOK_AFTER_QUERY_READ, "removable", hook_pass, 100, NULL);
    TEST_ASSERT_NOT_NULL(h);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);
    TEST_ASSERT_EQ(g_fire_count, 1);

    keel_hook_unregister(NULL, h);

    keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx);
    TEST_ASSERT_EQ(g_fire_count, 1); /* Should NOT have incremented */

    /* Unregister NULL should not crash */
    keel_hook_unregister(NULL, NULL);

    keel_hook_shutdown();

    printf("    PASSED\n");
}

static void test_fire_empty_point(void) {
    printf("  Testing fire with no hooks registered...\n");

    keel_hook_init();

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    /* Firing an empty point should return true (no abort) */
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx));

    keel_hook_shutdown();

    printf("    PASSED\n");
}

/* ============================================================================
 * New hook point coverage — traversal + infrastructure
 * ============================================================================ */

/** Hook that checks for non-NULL ext (keel_hook_shard_ctx_t). */
static bool hook_check_shard_ext(keel_hook_ctx_t* ctx) {
    keel_hook_shard_ctx_t* sctx = (keel_hook_shard_ctx_t*)ctx->ext;
    if (!sctx) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg), "ext is NULL for shard hook");
        return false;
    }
    g_fire_count++;
    return true;
}

/** Hook that vetoes via the shard context. */
static bool hook_veto_via_shard_ctx(keel_hook_ctx_t* ctx) {
    keel_hook_shard_ctx_t* sctx = (keel_hook_shard_ctx_t*)ctx->ext;
    if (sctx) {
        sctx->veto_execution = true;
        snprintf(sctx->veto_reason, sizeof(sctx->veto_reason), "test-veto");
    }
    return true;
}

/** Hook that checks for non-NULL ext (keel_hook_backend_ctx_t). */
static bool hook_check_backend_ext(keel_hook_ctx_t* ctx) {
    keel_hook_backend_ctx_t* bctx = (keel_hook_backend_ctx_t*)ctx->ext;
    if (!bctx) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg), "ext is NULL for backend hook");
        return false;
    }
    g_fire_count++;
    return true;
}

/** Hook that checks for non-NULL ext (keel_hook_health_ctx_t). */
static bool hook_check_health_ext(keel_hook_ctx_t* ctx) {
    keel_hook_health_ctx_t* hctx = (keel_hook_health_ctx_t*)ctx->ext;
    if (!hctx) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg), "ext is NULL for health hook");
        return false;
    }
    g_fire_count++;
    return true;
}

static void test_all_hook_points_register_and_fire(void) {
    printf("  Testing all 10 hook points register and fire...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_READ,      "r1",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_PARSE,     "r2",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_ROUTE,          "r3",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_ROUTE,           "r4",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_SCATTER,        "r5",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_SCATTER,         "r6",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_SEND,           "r7",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_ON_BACKEND_CONNECT,    "r8",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_ON_BACKEND_DISCONNECT, "r9",  hook_pass, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_ON_HEALTH_CHANGE,      "r10", hook_pass, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_ROUTE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SCATTER, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_SCATTER, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SEND, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_ON_BACKEND_CONNECT, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_ON_BACKEND_DISCONNECT, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_ON_HEALTH_CHANGE, &ctx));

    TEST_ASSERT(g_fire_count == 10);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_shard_ext_context(void) {
    printf("  Testing shard extension context (AFTER_ROUTE / BEFORE_SCATTER / AFTER_SCATTER)...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_ROUTE,    "ar-shard", hook_check_shard_ext, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_SCATTER, "bs-shard", hook_check_shard_ext, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_SCATTER,  "as-shard", hook_check_shard_ext, 100, NULL);

    /* Build a minimal shard ctx */
    keel_hook_shard_ctx_t sctx = {
        .dispatch_kind       = KEEL_HOOK_DISPATCH_SINGLE,
        .single_shard_index  = 2,
        .shard_count         = 1,
        .shards[0]           = { .shard_index = 2, .is_write = false, .server_available = true },
    };

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &sctx;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_ROUTE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SCATTER, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_SCATTER, &ctx));
    TEST_ASSERT(g_fire_count == 3);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_shard_veto_via_ctx(void) {
    printf("  Testing veto via keel_hook_shard_ctx_t.veto_execution...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_ROUTE, "vetoer", hook_veto_via_shard_ctx, 100, NULL);

    keel_hook_shard_ctx_t sctx = { .dispatch_kind = KEEL_HOOK_DISPATCH_SCATTER };
    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &sctx;

    /* Hook returns true but sets veto_execution — engine must check sctx */
    bool fired = keel_hook_fire(NULL, KEEL_HOOK_AFTER_ROUTE, &ctx);
    TEST_ASSERT(fired);  /* hook itself returns true */
    TEST_ASSERT(sctx.veto_execution);
    TEST_ASSERT(strcmp(sctx.veto_reason, "test-veto") == 0);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_backend_ext_context(void) {
    printf("  Testing backend extension context (ON_BACKEND_CONNECT/DISCONNECT)...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_ON_BACKEND_CONNECT,    "bc", hook_check_backend_ext, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_ON_BACKEND_DISCONNECT, "bd", hook_check_backend_ext, 100, NULL);

    keel_hook_backend_ctx_t bctx = {
        .host        = "127.0.0.1",
        .port        = 5432,
        .backend_fd  = 7,
        .shard_index = 0,
        .is_tls      = true,
        .is_primary  = true,
    };

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &bctx;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_ON_BACKEND_CONNECT, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_ON_BACKEND_DISCONNECT, &ctx));
    TEST_ASSERT(g_fire_count == 2);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_health_ext_context(void) {
    printf("  Testing health extension context (ON_HEALTH_CHANGE)...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_ON_HEALTH_CHANGE, "hc", hook_check_health_ext, 100, NULL);

    keel_hook_health_ctx_t hctx = {
        .host            = "db-primary",
        .port            = 5432,
        .shard_index     = 0,
        .is_primary      = true,
        .prev_health     = 0,   /* KEEL_HEALTH_UNKNOWN */
        .curr_health     = 1,   /* KEEL_HEALTH_UP */
        .prev_health_str = "UNKNOWN",
        .curr_health_str = "UP",
        .probe_latency_us = 850,
    };

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &hctx;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_ON_HEALTH_CHANGE, &ctx));
    TEST_ASSERT(g_fire_count == 1);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_hook_point_count(void) {
    printf("  Testing KEEL_HOOK_POINT_COUNT == 10...\n");
    TEST_ASSERT(KEEL_HOOK_POINT_COUNT == 10);
    printf("    PASSED\n");
}

static void test_hook_point_names(void) {
    printf("  Testing keel_hook_point_name() for all points...\n");
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_AFTER_QUERY_READ),      "after_query_read")      == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_AFTER_QUERY_PARSE),     "after_query_parse")     == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_BEFORE_ROUTE),          "before_route")          == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_AFTER_ROUTE),           "after_route")           == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_BEFORE_SCATTER),        "before_scatter")        == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_AFTER_SCATTER),         "after_scatter")         == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_BEFORE_SEND),           "before_send")           == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_ON_BACKEND_CONNECT),    "on_backend_connect")    == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_ON_BACKEND_DISCONNECT), "on_backend_disconnect") == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_ON_HEALTH_CHANGE),      "on_health_change")      == 0);
    TEST_ASSERT(strcmp(keel_hook_point_name(KEEL_HOOK_POINT_COUNT),           "unknown")               == 0);
    printf("    PASSED\n");
}

static void test_hook_bit_macro(void) {
    printf("  Testing KEEL_HOOK_BIT() for all points...\n");
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_AFTER_QUERY_READ)      == (1u << 0));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_AFTER_QUERY_PARSE)     == (1u << 1));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_BEFORE_ROUTE)          == (1u << 2));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_AFTER_ROUTE)           == (1u << 3));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_BEFORE_SCATTER)        == (1u << 4));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_AFTER_SCATTER)         == (1u << 5));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_BEFORE_SEND)           == (1u << 6));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_ON_BACKEND_CONNECT)    == (1u << 7));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_ON_BACKEND_DISCONNECT) == (1u << 8));
    TEST_ASSERT(KEEL_HOOK_BIT(KEEL_HOOK_ON_HEALTH_CHANGE)      == (1u << 9));
    printf("    PASSED\n");
}

static void test_active_mask_covers_new_points(void) {
    printf("  Testing active_mask covers new hook points via registry...\n");

    keel_hook_registry_t* reg = keel_hook_registry_create();
    TEST_ASSERT(reg != NULL);

    uint32_t mask = keel_hook_registry_active_mask(reg);
    TEST_ASSERT(mask == 0);

    keel_hook_register(reg, KEEL_HOOK_AFTER_ROUTE,    "ar", hook_pass, 0, NULL);
    keel_hook_register(reg, KEEL_HOOK_BEFORE_SCATTER, "bs", hook_pass, 0, NULL);
    keel_hook_register(reg, KEEL_HOOK_AFTER_SCATTER,  "as", hook_pass, 0, NULL);

    mask = keel_hook_registry_active_mask(reg);
    TEST_ASSERT(mask & KEEL_HOOK_BIT(KEEL_HOOK_AFTER_ROUTE));
    TEST_ASSERT(mask & KEEL_HOOK_BIT(KEEL_HOOK_BEFORE_SCATTER));
    TEST_ASSERT(mask & KEEL_HOOK_BIT(KEEL_HOOK_AFTER_SCATTER));
    /* Points with no hooks must not be set */
    TEST_ASSERT(!(mask & KEEL_HOOK_BIT(KEEL_HOOK_AFTER_QUERY_READ)));
    TEST_ASSERT(!(mask & KEEL_HOOK_BIT(KEEL_HOOK_ON_HEALTH_CHANGE)));

    keel_hook_registry_destroy(reg);
    printf("    PASSED\n");
}

/** Hook that asserts ext == NULL (used for base query-path points). */
static bool hook_assert_null_ext(keel_hook_ctx_t* ctx) {
    if (ctx->ext != NULL) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg), "expected NULL ext");
        return false;
    }
    g_fire_count++;
    return true;
}

static void test_ext_null_for_base_points(void) {
    printf("  Testing ext is NULL for base points (no caller-provided ext)...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_READ,  "null-ext-1", hook_assert_null_ext, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "null-ext-2", hook_assert_null_ext, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_ROUTE,      "null-ext-3", hook_assert_null_ext, 100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_BEFORE_SEND,       "null-ext-4", hook_assert_null_ext, 100, NULL);

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    /* ext is zero-initialised by memset — simulates engine not setting ext */

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_ROUTE, &ctx));
    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SEND, &ctx));
    TEST_ASSERT(g_fire_count == 4);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

/* ============================================================================
 * Main
 * ============================================================================ */

/* --------------------------------------------------------------------------
 * Helpers for new-field tests
 * -------------------------------------------------------------------------- */

/** Hook that reads and validates the full merge plan from sctx */
static bool hook_check_merge_plan(keel_hook_ctx_t* ctx) {
    keel_hook_shard_ctx_t* sctx = (keel_hook_shard_ctx_t*)ctx->ext;
    if (!sctx) return false;

    /* Verify all merge plan fields are visible */
    if (!sctx->requires_merge)                     return false;
    if (sctx->norder_keys       != 2)              return false;
    if (sctx->order_keys[0].col_index != 3)        return false;
    if (sctx->order_keys[1].col_index != 7)        return false;
    if (sctx->limit_count       != 100)            return false;
    if (sctx->limit_offset      != 20)             return false;
    if (sctx->nagg_specs        != 1)              return false;
    if (sctx->agg_specs[0].col_index != 5)         return false;
    if (sctx->ngroup_key_cols   != 1)              return false;
    if (sctx->group_key_cols[0].col_index != 2)    return false;
    if (sctx->nhaving_preds     != 1)              return false;
    if (sctx->having_preds[0].col_index != 5)      return false;
    if (sctx->navg_finalize_specs != 1)            return false;
    if (sctx->avg_finalize_specs[0].sum_col != 1)  return false;
    if (sctx->avg_finalize_specs[0].count_col != 2) return false;
    if (sctx->avg_finalize_specs[0].out_col != 3)  return false;
    if (sctx->nwindow_col_specs != 1)              return false;
    if (sctx->window_col_specs[0].norder_keys != 1) return false;
    if (sctx->requires_avg_rewrite != true)         return false;
    if (sctx->requires_count_distinct != false)     return false;

    g_fire_count++;
    return true;
}

/** Hook that reads 2PC state from sctx */
static bool hook_check_twopc(keel_hook_ctx_t* ctx) {
    keel_hook_shard_ctx_t* sctx = (keel_hook_shard_ctx_t*)ctx->ext;
    if (!sctx)                                      return false;
    if (!sctx->twopc_required)                      return false;
    if (sctx->participating_shards_mask != 0x7)     return false;
    g_fire_count++;
    return true;
}

/** Hook that reads scatter output stats from sctx */
static bool hook_check_scatter_output(keel_hook_ctx_t* ctx) {
    keel_hook_shard_ctx_t* sctx = (keel_hook_shard_ctx_t*)ctx->ext;
    if (!sctx)                                      return false;
    if (sctx->scatter_rows_merged != 42)            return false;
    if (sctx->scatter_spilled     != true)          return false;
    if (sctx->scatter_elapsed_us  != 1500)          return false;
    g_fire_count++;
    return true;
}

/** Hook that reads query_tree from base context */
static bool hook_check_query_tree(keel_hook_ctx_t* ctx) {
    /* query_tree pointer must be non-NULL when caller fills it */
    if (ctx->query_tree == NULL) return false;
    /* The dummy tree should report no error */
    if (ctx->query_tree->has_error) return false;
    g_fire_count++;
    return true;
}

/** Hook that reads query_tree is NULL for AFTER_QUERY_READ */
static bool hook_check_no_query_tree(keel_hook_ctx_t* ctx) {
    if (ctx->query_tree != NULL) return false;
    g_fire_count++;
    return true;
}

/** Hook that reads shard_key from the first shard info */
static bool hook_check_shard_key(keel_hook_ctx_t* ctx) {
    keel_hook_shard_ctx_t* sctx = (keel_hook_shard_ctx_t*)ctx->ext;
    if (!sctx)                                              return false;
    if (sctx->shards[0].shard_key.kind != KEEL_SHARD_KEY_INT64) return false;
    if (sctx->shards[0].shard_key.value.int64_value != 99)  return false;
    g_fire_count++;
    return true;
}

static void test_merge_plan_access(void) {
    printf("  Testing full merge plan access in shard context...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_SCATTER, "merge-plan", hook_check_merge_plan, 100, NULL);

    /* Build a shard ctx with a full merge plan */
    keel_hook_shard_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.dispatch_kind   = KEEL_HOOK_DISPATCH_SCATTER;
    sctx.requires_merge  = true;
    sctx.requires_avg_rewrite = true;

    /* ORDER BY keys */
    sctx.norder_keys = 2;
    sctx.order_keys[0].col_index = 3;
    sctx.order_keys[1].col_index = 7;

    /* LIMIT / OFFSET */
    sctx.limit_count  = 100;
    sctx.limit_offset = 20;

    /* Aggregate specs */
    sctx.nagg_specs = 1;
    sctx.agg_specs[0].col_index = 5;
    sctx.agg_specs[0].func      = KEEL_AGG_SUM;

    /* GROUP BY */
    sctx.ngroup_key_cols = 1;
    sctx.group_key_cols[0].col_index = 2;

    /* HAVING */
    sctx.nhaving_preds = 1;
    sctx.having_preds[0].col_index = 5;
    sctx.having_preds[0].op = KEEL_CMP_GT;
    memcpy(sctx.having_preds[0].literal, "10", 2);
    sctx.having_preds[0].literal_len = 2;

    /* AVG finalize */
    sctx.navg_finalize_specs = 1;
    sctx.avg_finalize_specs[0].sum_col   = 1;
    sctx.avg_finalize_specs[0].count_col = 2;
    sctx.avg_finalize_specs[0].out_col   = 3;

    /* Window */
    sctx.nwindow_col_specs = 1;
    sctx.window_col_specs[0].norder_keys = 1;
    sctx.window_col_specs[0].order_keys[0].col_index = 0;

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &sctx;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_SCATTER, &ctx));
    TEST_ASSERT(g_fire_count == 1);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_twopc_state_access(void) {
    printf("  Testing 2PC state visibility in shard context...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_BEFORE_SCATTER, "twopc-check", hook_check_twopc, 100, NULL);

    keel_hook_shard_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.dispatch_kind              = KEEL_HOOK_DISPATCH_SCATTER;
    sctx.twopc_required             = true;
    sctx.participating_shards_mask  = 0x7; /* shards 0, 1, 2 */

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &sctx;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_BEFORE_SCATTER, &ctx));
    TEST_ASSERT(g_fire_count == 1);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_scatter_output_stats(void) {
    printf("  Testing scatter output stats (rows_merged, spilled) in AFTER_SCATTER...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_SCATTER, "scatter-out", hook_check_scatter_output, 100, NULL);

    keel_hook_shard_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.dispatch_kind         = KEEL_HOOK_DISPATCH_SCATTER;
    sctx.scatter_rows_merged   = 42;
    sctx.scatter_spilled       = true;
    sctx.scatter_elapsed_us    = 1500;

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &sctx;

    keel_hook_fire(NULL, KEEL_HOOK_AFTER_SCATTER, &ctx);
    TEST_ASSERT(g_fire_count == 1);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_query_tree_access(void) {
    printf("  Testing query_tree pointer in base hook context...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, "qt-check",    hook_check_query_tree,    100, NULL);
    keel_hook_register(NULL, KEEL_HOOK_AFTER_QUERY_READ,  "no-qt-check", hook_check_no_query_tree, 100, NULL);

    /* Simulate AFTER_QUERY_PARSE with a populated (dummy) query tree */
    keel_qt_query_t fake_qt;
    memset(&fake_qt, 0, sizeof(fake_qt));
    fake_qt.has_error = false;

    keel_hook_ctx_t ctx_with_qt;
    memset(&ctx_with_qt, 0, sizeof(ctx_with_qt));
    ctx_with_qt.query_tree = &fake_qt;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_PARSE, &ctx_with_qt));
    TEST_ASSERT(g_fire_count == 1);

    /* Simulate AFTER_QUERY_READ with NULL query_tree */
    keel_hook_ctx_t ctx_no_qt;
    memset(&ctx_no_qt, 0, sizeof(ctx_no_qt));
    /* query_tree is NULL (zeroed) */

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_QUERY_READ, &ctx_no_qt));
    TEST_ASSERT(g_fire_count == 2);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

static void test_shard_key_access(void) {
    printf("  Testing shard_key field in keel_hook_shard_info_t...\n");

    keel_hook_init();
    reset_counters();

    keel_hook_register(NULL, KEEL_HOOK_AFTER_ROUTE, "shard-key", hook_check_shard_key, 100, NULL);

    keel_hook_shard_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.dispatch_kind      = KEEL_HOOK_DISPATCH_SINGLE;
    sctx.single_shard_index = 0;
    sctx.shard_count        = 1;
    sctx.shards[0].shard_index     = 0;
    sctx.shards[0].is_write        = true;
    sctx.shards[0].server_available= true;
    sctx.shards[0].is_healthy      = true;
    /* Populate the resolved shard key */
    sctx.shards[0].shard_key.kind               = KEEL_SHARD_KEY_INT64;
    sctx.shards[0].shard_key.value.int64_value  = 99;

    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.ext = &sctx;

    TEST_ASSERT(keel_hook_fire(NULL, KEEL_HOOK_AFTER_ROUTE, &ctx));
    TEST_ASSERT(g_fire_count == 1);

    keel_hook_shutdown();
    printf("    PASSED\n");
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("=== Hook System Tests ===\n\n");

    /* Original tests */
    test_init_shutdown();
    test_register_and_fire();
    test_abort_stops_chain();
    test_priority_ordering();
    test_context_mutation();
    test_user_data();
    test_stats();
    test_unregister();
    test_fire_empty_point();

    /* New traversal subsystem tests */
    test_hook_point_count();
    test_hook_point_names();
    test_hook_bit_macro();
    test_all_hook_points_register_and_fire();
    test_shard_ext_context();
    test_shard_veto_via_ctx();
    test_backend_ext_context();
    test_health_ext_context();
    test_active_mask_covers_new_points();
    test_ext_null_for_base_points();

    /* Full object access tests */
    test_merge_plan_access();
    test_twopc_state_access();
    test_scatter_output_stats();
    test_query_tree_access();
    test_shard_key_access();

    return test_summary();
}
