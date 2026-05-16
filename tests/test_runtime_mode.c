/**
 * @file test_runtime_mode.c
 * @brief Runtime Mode Tier Tests
 *
 * Tests the 4-tier runtime mode system:
 *   §1 — Tier parsing: string→enum mapping (proxy/pool/smart/full/unknown/NULL)
 *   §2 — Tier ordering: PROXY < POOL < SMART < FULL
 *   §3 — Gate macro correctness: each macro true/false at correct tier boundaries
 *   §4 — Tier name round-trip: parse→name→parse identity
 *   §5 — Session flow init: PROXY forces PS_MODE_OFF + txn_tracking=false
 */

#include "test_utils.h"
#include "keel/engine/runtime_mode.h"
#include "keel/engine/engine_flow.h"
#include "keel/engine/worker.h"

#include <string.h>
#include <stdio.h>

int g_tests_run, g_tests_passed, g_tests_failed;

/* ============================================================================
 * §1 — Tier Parsing
 * ============================================================================ */

static void test_tier_parse(void) {
    printf("  §1 Tier parsing...\n");

    /* Exact lowercase */
    TEST_ASSERT_EQ(keel_tier_parse("proxy"), KEEL_TIER_PROXY);
    TEST_ASSERT_EQ(keel_tier_parse("pool"),  KEEL_TIER_POOL);
    TEST_ASSERT_EQ(keel_tier_parse("smart"), KEEL_TIER_SMART);
    TEST_ASSERT_EQ(keel_tier_parse("full"),  KEEL_TIER_FULL);

    /* Case-insensitive (first-char dispatch is lowercase-ORed) */
    TEST_ASSERT_EQ(keel_tier_parse("PROXY"), KEEL_TIER_PROXY);
    TEST_ASSERT_EQ(keel_tier_parse("Pool"),  KEEL_TIER_POOL);
    TEST_ASSERT_EQ(keel_tier_parse("SMART"), KEEL_TIER_SMART);
    TEST_ASSERT_EQ(keel_tier_parse("Full"),  KEEL_TIER_FULL);

    /* NULL and empty → FULL (safest default) */
    TEST_ASSERT_EQ(keel_tier_parse(NULL), KEEL_TIER_FULL);
    TEST_ASSERT_EQ(keel_tier_parse(""),   KEEL_TIER_FULL);

    /* Unknown string → FULL */
    TEST_ASSERT_EQ(keel_tier_parse("turbo"),   KEEL_TIER_FULL);
    TEST_ASSERT_EQ(keel_tier_parse("minimal"), KEEL_TIER_FULL);
}

/* ============================================================================
 * §2 — Tier Ordering
 * ============================================================================ */

static void test_tier_ordering(void) {
    printf("  §2 Tier ordering...\n");

    TEST_ASSERT(KEEL_TIER_PROXY < KEEL_TIER_POOL);
    TEST_ASSERT(KEEL_TIER_POOL  < KEEL_TIER_SMART);
    TEST_ASSERT(KEEL_TIER_SMART < KEEL_TIER_FULL);

    /* Enum values are 0,1,2,3 */
    TEST_ASSERT_EQ((int)KEEL_TIER_PROXY, 0);
    TEST_ASSERT_EQ((int)KEEL_TIER_POOL,  1);
    TEST_ASSERT_EQ((int)KEEL_TIER_SMART, 2);
    TEST_ASSERT_EQ((int)KEEL_TIER_FULL,  3);
    TEST_ASSERT_EQ((int)KEEL_TIER_COUNT, 4);
}

/* ============================================================================
 * §3 — Gate Macro Correctness
 * ============================================================================ */

static void test_gate_macros(void) {
    printf("  §3 Gate macro correctness...\n");

    /* --- POOLING: enabled at POOL and above --- */
    TEST_ASSERT(!KEEL_TIER_HAS_POOLING(KEEL_TIER_PROXY));
    TEST_ASSERT( KEEL_TIER_HAS_POOLING(KEEL_TIER_POOL));
    TEST_ASSERT( KEEL_TIER_HAS_POOLING(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_POOLING(KEEL_TIER_FULL));

    /* --- ROUTING: enabled at SMART and above --- */
    TEST_ASSERT(!KEEL_TIER_HAS_ROUTING(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_ROUTING(KEEL_TIER_POOL));
    TEST_ASSERT( KEEL_TIER_HAS_ROUTING(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_ROUTING(KEEL_TIER_FULL));

    /* --- QUERY_LOG: enabled at SMART and above --- */
    TEST_ASSERT(!KEEL_TIER_HAS_QUERY_LOG(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_QUERY_LOG(KEEL_TIER_POOL));
    TEST_ASSERT( KEEL_TIER_HAS_QUERY_LOG(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_QUERY_LOG(KEEL_TIER_FULL));

    /* --- STATE_SYNC: enabled at SMART and above --- */
    TEST_ASSERT(!KEEL_TIER_HAS_STATE_SYNC(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_STATE_SYNC(KEEL_TIER_POOL));
    TEST_ASSERT( KEEL_TIER_HAS_STATE_SYNC(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_STATE_SYNC(KEEL_TIER_FULL));

    /* --- HOOKS: enabled at FULL only --- */
    TEST_ASSERT(!KEEL_TIER_HAS_HOOKS(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_HOOKS(KEEL_TIER_POOL));
    TEST_ASSERT(!KEEL_TIER_HAS_HOOKS(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_HOOKS(KEEL_TIER_FULL));

    /* --- TXN_TRACK: enabled at FULL only --- */
    TEST_ASSERT(!KEEL_TIER_HAS_TXN_TRACK(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_TXN_TRACK(KEEL_TIER_POOL));
    TEST_ASSERT(!KEEL_TIER_HAS_TXN_TRACK(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_TXN_TRACK(KEEL_TIER_FULL));

    /* --- LSN_CAPTURE: enabled at FULL only --- */
    TEST_ASSERT(!KEEL_TIER_HAS_LSN_CAPTURE(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_LSN_CAPTURE(KEEL_TIER_POOL));
    TEST_ASSERT(!KEEL_TIER_HAS_LSN_CAPTURE(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_LSN_CAPTURE(KEEL_TIER_FULL));

    /* --- FULL_STATS: enabled at SMART and above --- */
    TEST_ASSERT(!KEEL_TIER_HAS_FULL_STATS(KEEL_TIER_PROXY));
    TEST_ASSERT(!KEEL_TIER_HAS_FULL_STATS(KEEL_TIER_POOL));
    TEST_ASSERT( KEEL_TIER_HAS_FULL_STATS(KEEL_TIER_SMART));
    TEST_ASSERT( KEEL_TIER_HAS_FULL_STATS(KEEL_TIER_FULL));
}

/* ============================================================================
 * §4 — Tier Name Round-Trip
 * ============================================================================ */

static void test_tier_name_roundtrip(void) {
    printf("  §4 Tier name round-trip...\n");

    /* name → parse → same tier */
    for (int i = 0; i < (int)KEEL_TIER_COUNT; i++) {
        keel_tier_t t = (keel_tier_t)i;
        const char* name = keel_tier_name(t);
        TEST_ASSERT(name != NULL);
        TEST_ASSERT(name[0] != '\0');
        keel_tier_t parsed = keel_tier_parse(name);
        TEST_ASSERT_EQ(parsed, t);
    }

    /* Out-of-range → "unknown" */
    TEST_ASSERT(strcmp(keel_tier_name((keel_tier_t)99), "unknown") == 0);
}

/* ============================================================================
 * §5 — Session Flow Init: PROXY forces PS_MODE_OFF + txn_tracking=false
 * ============================================================================ */

static void test_proxy_forces_overrides(void) {
    printf("  §5 PROXY mode overrides...\n");

    /* Set up a minimal worker with PROXY mode but PS and txn_tracking on */
    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.runtime_mode = KEEL_TIER_PROXY;
    worker.ps_mode = KEEL_PS_MODE_VIRTUALIZE;
    worker.txn_tracking = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.client_fd = -1;
    session.server_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));

    /* Call the init routine */
    keel_session_flow_init(&sf, NULL, &session);

    /* PROXY must override worker settings */
    TEST_ASSERT_EQ(sf.mode, KEEL_TIER_PROXY);
    TEST_ASSERT_EQ(sf.ps_mode, KEEL_PS_MODE_OFF);
    TEST_ASSERT_EQ(sf.txn_tracking, false);

    /* Now test FULL mode — should inherit worker settings as-is */
    worker.runtime_mode = KEEL_TIER_FULL;
    worker.ps_mode = KEEL_PS_MODE_VIRTUALIZE;
    worker.txn_tracking = true;
    memset(&sf, 0, sizeof(sf));

    keel_session_flow_init(&sf, NULL, &session);

    TEST_ASSERT_EQ(sf.mode, KEEL_TIER_FULL);
    TEST_ASSERT_EQ(sf.ps_mode, KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_EQ(sf.txn_tracking, true);

    /* POOL mode — should not override PS or txn_tracking */
    worker.runtime_mode = KEEL_TIER_POOL;
    worker.ps_mode = KEEL_PS_MODE_VIRTUALIZE;
    worker.txn_tracking = true;
    memset(&sf, 0, sizeof(sf));

    keel_session_flow_init(&sf, NULL, &session);

    TEST_ASSERT_EQ(sf.mode, KEEL_TIER_POOL);
    TEST_ASSERT_EQ(sf.ps_mode, KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_EQ(sf.txn_tracking, true);

    /* SMART mode — same as POOL */
    worker.runtime_mode = KEEL_TIER_SMART;
    memset(&sf, 0, sizeof(sf));

    keel_session_flow_init(&sf, NULL, &session);

    TEST_ASSERT_EQ(sf.mode, KEEL_TIER_SMART);
    TEST_ASSERT_EQ(sf.ps_mode, KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_EQ(sf.txn_tracking, true);
}

/* ============================================================================
 * §6 — Default session flow init (no worker) → FULL tier
 * ============================================================================ */

static void test_default_tier_no_worker(void) {
    printf("  §6 Default tier (no worker)...\n");

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = NULL;
    session.client_fd = -1;
    session.server_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));

    keel_session_flow_init(&sf, NULL, &session);

    /* No worker → default FULL */
    TEST_ASSERT_EQ(sf.mode, KEEL_TIER_FULL);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("test_runtime_mode: Runtime Mode Tier Tests\n");

    test_tier_parse();
    test_tier_ordering();
    test_gate_macros();
    test_tier_name_roundtrip();
    test_proxy_forces_overrides();
    test_default_tier_no_worker();

    printf("\nResults: %d passed, %d failed, %d total\n",
           g_tests_passed, g_tests_failed, g_tests_run);
    return g_tests_failed ? 1 : 0;
}
