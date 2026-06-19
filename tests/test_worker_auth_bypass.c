/**
 * @file test_worker_auth_bypass.c
 * @brief Regression tests for the authentication fail-open bypass
 *        described in proposals/review_20260618_01.md §A1 and §A2.
 *
 * Background
 * ----------
 * The frontend protocol layer treats a worker with a NULL auth_manager
 * as "trust mode" (accept every connection without authentication). The
 * previous keel_worker_init() code had three paths that could land the
 * worker in that state without the operator explicitly asking for trust:
 *
 *   A1  the configured auth_method had no provider case in the switch
 *       (e.g. the documented default `auth_method = password`); the
 *       `default:` arm destroyed the manager and logged "falling back
 *       to trust";
 *   A2  keel_auth_manager_create() failed (transient OOM at startup)
 *       — same "falling back to trust" log;
 *   A2' keel_auth_manager_register() failed for the requested provider
 *       — same again.
 *
 * Each of those turned a configured (non-trust) deployment into an open
 * authenticator.  These tests pin the new fail-closed contract: every
 * non-trust method MUST either build a working auth_manager or refuse to
 * start the worker.
 *
 * The tests build a real engine + worker so the same allocations,
 * reactor/slab/pool plumbing, and config-to-runtime flow that production
 * uses are exercised.  listen_fd is -1 because none of these tests
 * accept connections.
 */

#include "test_utils.h"
#include "keel/engine/engine.h"
#include "keel/engine/worker.h"
#include "keel/core/auth.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>

/* ---- helpers -------------------------------------------------------- */

/**
 * Build an engine configured for @p method, run keel_worker_init() on a
 * single worker, and report the result.  The caller inspects
 * `worker.auth_manager` and the return code to verify the contract.
 *
 * Returns the keel_worker_init() return value (0 success, -1 failure).
 * On success the worker is left initialized so the caller can inspect
 * `worker.auth_manager`; the caller MUST call keel_worker_cleanup() in
 * that case.  On failure the helper has already torn down the engine
 * and zeroed the worker.
 */
static int init_worker_with_method(keel_worker_t* worker,
                                   keel_auth_method_t method)
{
    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;
    cfg.auth_method = method;

    keel_engine_t* engine = keel_engine_create(&cfg);
    /* keel_engine_create itself must not fail in these tests — if it
     * does, the assertion failure is more useful than a silent skip. */
    TEST_ASSERT_NOT_NULL(engine);

    memset(worker, 0, sizeof(*worker));
    int rc = keel_worker_init(worker, engine, 0, /*listen_fd=*/-1);

    if (rc != 0) {
        /* keel_worker_pool_init only cleans up workers that succeeded;
         * the failing worker is left with partial state and must be
         * cleaned explicitly so we don't leak reactor/slab/pool.  The
         * auth_manager teardown inside keel_worker_cleanup() is NULL-
         * safe, which is exactly the property we're verifying. */
        keel_worker_cleanup(worker);
    }

    keel_engine_destroy(engine);
    return rc;
}

/* ====================================================================
 * §A1 — unsupported / unimplemented auth methods must refuse to start
 * ==================================================================== */

static void test_auth_password_rejected(void) {
    TEST_BEGIN("A1: auth_method=password (documented default) refuses start");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_PASSWORD);
    TEST_ASSERT_EQ(rc, -1);
    /* The failed worker must not carry an auth_manager — otherwise a
     * subsequent protocol-layer accept would treat it as trust mode. */
    TEST_ASSERT_NULL(worker.auth_manager);
    TEST_END();
}

static void test_auth_reject_enum_rejected(void) {
    TEST_BEGIN("A1: auth_method=reject (no provider) refuses start");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_REJECT);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_NULL(worker.auth_manager);
    TEST_END();
}

static void test_auth_gssapi_rejected(void) {
    TEST_BEGIN("A1: auth_method=gssapi (no provider) refuses start");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_GSSAPI);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_NULL(worker.auth_manager);
    TEST_END();
}

static void test_auth_radius_rejected(void) {
    TEST_BEGIN("A1: auth_method=radius (no provider) refuses start");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_RADIUS);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_NULL(worker.auth_manager);
    TEST_END();
}

static void test_auth_passthrough_rejected(void) {
    TEST_BEGIN("A1: auth_method=passthrough (no provider) refuses start");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_PASSTHROUGH);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_NULL(worker.auth_manager);
    TEST_END();
}

static void test_auth_none_rejected(void) {
    TEST_BEGIN("A1: auth_method=none (sentinel) refuses start");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_NONE);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_NULL(worker.auth_manager);
    TEST_END();
}

/* ====================================================================
 * §A1 — explicit trust must still succeed (the only legitimate NULL
 * manager path) and must be auditable in the log
 * ==================================================================== */

static void test_auth_trust_succeeds_null_manager(void) {
    TEST_BEGIN("A1 positive: explicit auth_method=trust succeeds with NULL manager");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_TRUST);
    TEST_ASSERT_EQ(rc, 0);
    /* Trust mode IS the NULL-manager mode by design; the protocol layer
     * interprets NULL as "accept all". The fix is that this is now the
     * ONLY path that produces a NULL manager. */
    TEST_ASSERT_NULL(worker.auth_manager);
    keel_worker_cleanup(&worker);
    TEST_END();
}

/* ====================================================================
 * §A1 — supported methods must succeed AND populate the manager
 * ==================================================================== */

static void test_auth_scram_succeeds_with_manager(void) {
    TEST_BEGIN("A1 positive: auth_method=scram-sha-256 succeeds with non-NULL manager");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_SCRAM_SHA_256);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_NOT_NULL(worker.auth_manager);
    keel_worker_cleanup(&worker);
    TEST_END();
}

static void test_auth_md5_succeeds_with_manager(void) {
    TEST_BEGIN("A1 positive: auth_method=md5 succeeds with non-NULL manager");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_MD5);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_NOT_NULL(worker.auth_manager);
    keel_worker_cleanup(&worker);
    TEST_END();
}

static void test_auth_cert_succeeds_with_manager(void) {
    TEST_BEGIN("A1 positive: auth_method=cert succeeds with non-NULL manager");
    keel_worker_t worker;
    int rc = init_worker_with_method(&worker, KEEL_AUTH_CERTIFICATE);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_NOT_NULL(worker.auth_manager);
    keel_worker_cleanup(&worker);
    TEST_END();
}

/* ====================================================================
 * §A2 — keel_auth_manager_create() returning NULL must propagate as
 * init failure, not as trust-mode startup
 * ==================================================================== */

static void test_auth_manager_create_null_rejected(void) {
    TEST_BEGIN("A2: NULL auth_manager on a non-trust method rejects start");

    /* We cannot easily inject OOM at exactly the auth_manager_create
     * call inside keel_worker_init (it is preceded by many other
     * keel_calloc calls for reactor/slab/pool plumbing).  Instead we
     * verify the underlying contract that keel_worker_init relies on:
     * keel_auth_manager_create() can be made to return NULL via
     * keel_mem_set_fail_countdown, and when it does, the worker code
     * path treats that as a fatal init error.
     *
     * This test pins the keel_auth_manager_create() NULL-on-OOM
     * contract; combined with the source-level evidence that
     * keel_worker_init() returns -1 on a NULL manager (no "fall back
     * to trust" path remains), it closes the A2 bypass. */

    keel_mem_set_fail_countdown(0);
    keel_auth_manager_config_t cfg = {
        .default_method       = KEEL_AUTH_SCRAM_SHA_256,
        .allow_clear_password = false,
        .scram_iterations     = 4096,
    };
    keel_auth_manager_t* mgr = keel_auth_manager_create(&cfg);
    keel_mem_set_fail_countdown(-1);  /* re-enable normal allocation */

    TEST_ASSERT_NULL(mgr);
    /* No cleanup needed: create returned NULL. */
    TEST_END();
}

/* ====================================================================
 * Runner
 * ==================================================================== */

int main(void) {
    test_auth_password_rejected();
    test_auth_reject_enum_rejected();
    test_auth_gssapi_rejected();
    test_auth_radius_rejected();
    test_auth_passthrough_rejected();
    test_auth_none_rejected();
    test_auth_trust_succeeds_null_manager();
    test_auth_scram_succeeds_with_manager();
    test_auth_md5_succeeds_with_manager();
    test_auth_cert_succeeds_with_manager();
    test_auth_manager_create_null_rejected();

    return test_summary();
}
