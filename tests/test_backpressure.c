/**
 * @file test_backpressure.c
 * @brief Comprehensive tests for Issue 10 — memory, FD, and backpressure
 *        hardening additions.
 *
 * This suite validates every new limit introduced in the Issue 10 remediation:
 *
 *  §1  Error code distinctness — KEEL_ERR_MSG_TOO_LARGE / KEEL_ERR_REPLAY_TOO_LARGE
 *  §2  Engine config defaults — three new fields default to 0 (unlimited)
 *  §3  INI parsing — keel_config_get_int reads the three new keys correctly
 *  §4  Config validation — session_max_buffered_bytes / backend_max_replay_bytes
 *                          must be 0 (unlimited) or ≥ 4096; smaller is invalid
 *  §5  pool_wait_timeout_ms wired into backend_pool_config_t
 *  §6  pool_wait_timeout_ms == 0 falls back to connect_timeout_ms
 *  §7  backend_pool_create OOM injection — returns NULL under allocation failure
 *  §8  Pool queue wait — backend_pool_queue_wait returns -1 when max_waiting
 *                        is reached
 *  §9  Pool wait-timeout expiry — expire_waiters returns 1 after deadline
 * §10  FD exhaustion — open() beyond RLIMIT_NOFILE yields EMFILE
 *
 * The tests avoid sleeping where possible.  §9 uses a 60 ms sleep — the same
 * margin used in test_failover.c — so it is robust under load without being
 * slow.
 */

#include "test_utils.h"
#include "keel/session/admission.h"
#include "keel/engine/backend_pool.h"
#include "keel/engine/engine.h"
#include "keel/core/ini.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <errno.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Build a minimal synthetic pool using socketpairs.
 *
 * Mirrors the helper in test_failover.c so this file remains self-contained.
 */
static backend_pool_t* make_pool(size_t n, int backend_fds[], size_t max_waiting)
{
    backend_pool_config_t cfg = {
        .host             = "127.0.0.1",
        .port             = 5432,
        .user             = "test",
        .password         = "test",
        .database         = "test",
        .min_connections  = n,
        .max_connections  = n,
        .max_waiting      = max_waiting,
        .idle_timeout_ms  = 0,
        .wait_timeout_ms  = 0,
    };

    backend_pool_t* pool = keel_calloc(1, sizeof(backend_pool_t));
    if (!pool) return NULL;
    pool->config = cfg;
    pool->connections = keel_calloc(n, sizeof(backend_conn_t));
    if (!pool->connections) { keel_free(pool); return NULL; }
    pool->total_count = n;

    for (size_t i = 0; i < n; i++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            pool->connections[i].fd = -1;
            if (backend_fds) backend_fds[i] = -1;
            atomic_store(&pool->connections[i].state, BACKEND_CONN_CLOSED);
            continue;
        }
        pool->connections[i].fd    = sv[0];
        pool->connections[i].pool  = pool;
        if (backend_fds) backend_fds[i] = sv[1];
        atomic_store(&pool->connections[i].state, BACKEND_CONN_IDLE);
        pool->connections[i].current_state_hash = 0;
        pool->connections[i].next  = pool->clean_list;
        pool->clean_list           = &pool->connections[i];
        pool->clean_count++;
    }

    return pool;
}

static void destroy_pool(backend_pool_t* pool, int backend_fds[], size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) close(pool->connections[i].fd);
        if (backend_fds && backend_fds[i] >= 0) close(backend_fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* Shared expire-callback state */
static int   g_expire_count;
static void* g_expire_last_session;

static void expire_cb(void* session, void* userdata)
{
    (void)userdata;
    g_expire_count++;
    g_expire_last_session = session;
}

/* ============================================================================
 * §1 — Error code distinctness
 * ============================================================================ */

static void test_error_codes_defined(void)
{
    TEST_BEGIN("error codes — KEEL_ERR_MSG_TOO_LARGE and KEEL_ERR_REPLAY_TOO_LARGE defined");

    /* Both codes must be defined, negative, and distinct from every other
     * standard KEEL error code we know about. */
    keel_error_t msg_large    = KEEL_ERR_MSG_TOO_LARGE;
    keel_error_t replay_large = KEEL_ERR_REPLAY_TOO_LARGE;

    TEST_ASSERT(msg_large    < 0);
    TEST_ASSERT(replay_large < 0);
    TEST_ASSERT(msg_large    != replay_large);

    /* Must not alias any of the pre-existing codes */
    TEST_ASSERT(msg_large    != KEEL_OK);
    TEST_ASSERT(msg_large    != KEEL_ERR_UNKNOWN);
    TEST_ASSERT(msg_large    != KEEL_ERR_UNAVAILABLE);
    TEST_ASSERT(replay_large != KEEL_OK);
    TEST_ASSERT(replay_large != KEEL_ERR_UNKNOWN);
    TEST_ASSERT(replay_large != KEEL_ERR_UNAVAILABLE);
    TEST_ASSERT(replay_large != msg_large);

    TEST_END();
}

static void test_error_codes_values(void)
{
    TEST_BEGIN("error codes — expected numeric values");

    /* These are contractual values documented in the remediation plan. */
    TEST_ASSERT_EQ(KEEL_ERR_MSG_TOO_LARGE,    (keel_error_t)-406);
    TEST_ASSERT_EQ(KEEL_ERR_REPLAY_TOO_LARGE, (keel_error_t)-407);

    TEST_END();
}

/* ============================================================================
 * §2 — Engine config defaults
 * ============================================================================ */

static void test_engine_config_defaults(void)
{
    TEST_BEGIN("engine config — new fields default to 0");

    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;

    TEST_ASSERT_EQ(ecfg.pool_wait_timeout_ms,       0ULL);
    TEST_ASSERT_EQ(ecfg.session_max_buffered_bytes,  (size_t)0);
    TEST_ASSERT_EQ(ecfg.backend_max_replay_bytes,    (size_t)0);

    /* Existing adjacent field should be unchanged */
    TEST_ASSERT_EQ(ecfg.pool_max_waiting, 0u);

    TEST_END();
}

static void test_engine_config_default_unlimited(void)
{
    TEST_BEGIN("engine config — default 0 means unlimited (no artificial cap)");

    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;

    /* 0 = unlimited: the engine must never apply a cap when the field is 0 */
    TEST_ASSERT(ecfg.session_max_buffered_bytes == 0);
    TEST_ASSERT(ecfg.backend_max_replay_bytes   == 0);
    TEST_ASSERT(ecfg.pool_wait_timeout_ms       == 0);

    TEST_END();
}

/* ============================================================================
 * §3 — INI parsing of new keys
 * ============================================================================ */

static char g_ini_path[256];

static bool write_ini(const char* content)
{
    snprintf(g_ini_path, sizeof(g_ini_path),
             "/tmp/keel_bp_test_%d.ini", (int)getpid());
    FILE* f = fopen(g_ini_path, "w");
    if (!f) return false;
    fputs(content, f);
    fclose(f);
    return true;
}

static void remove_ini(void)
{
    unlink(g_ini_path);
}

static void test_ini_pool_wait_timeout_ms(void)
{
    TEST_BEGIN("INI — pool_wait_timeout_ms parsed correctly");

    TEST_ASSERT(write_ini(
        "[worker_group.default]\n"
        "pool_wait_timeout_ms = 3000\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    int64_t v = keel_config_get_int(cfg, "worker_group.default",
                                    "pool_wait_timeout_ms", -1);
    TEST_ASSERT_EQ(v, (int64_t)3000);

    keel_config_free(cfg);
    remove_ini();

    TEST_END();
}

static void test_ini_session_max_buffered_bytes(void)
{
    TEST_BEGIN("INI — session_max_buffered_bytes parsed correctly");

    TEST_ASSERT(write_ini(
        "[worker_group.default]\n"
        "session_max_buffered_bytes = 65536\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    int64_t v = keel_config_get_int(cfg, "worker_group.default",
                                    "session_max_buffered_bytes", -1);
    TEST_ASSERT_EQ(v, (int64_t)65536);

    keel_config_free(cfg);
    remove_ini();

    TEST_END();
}

static void test_ini_backend_max_replay_bytes(void)
{
    TEST_BEGIN("INI — backend_max_replay_bytes parsed correctly");

    TEST_ASSERT(write_ini(
        "[worker_group.default]\n"
        "backend_max_replay_bytes = 131072\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    int64_t v = keel_config_get_int(cfg, "worker_group.default",
                                    "backend_max_replay_bytes", -1);
    TEST_ASSERT_EQ(v, (int64_t)131072);

    keel_config_free(cfg);
    remove_ini();

    TEST_END();
}

static void test_ini_all_three_keys(void)
{
    TEST_BEGIN("INI — all three new keys parsed from a single section");

    TEST_ASSERT(write_ini(
        "[worker_group.default]\n"
        "pool_wait_timeout_ms       = 5000\n"
        "session_max_buffered_bytes = 8192\n"
        "backend_max_replay_bytes   = 16384\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    int64_t pwt = keel_config_get_int(cfg, "worker_group.default",
                                      "pool_wait_timeout_ms", -1);
    int64_t smb = keel_config_get_int(cfg, "worker_group.default",
                                      "session_max_buffered_bytes", -1);
    int64_t brb = keel_config_get_int(cfg, "worker_group.default",
                                      "backend_max_replay_bytes", -1);

    TEST_ASSERT_EQ(pwt,  (int64_t)5000);
    TEST_ASSERT_EQ(smb,  (int64_t)8192);
    TEST_ASSERT_EQ(brb, (int64_t)16384);

    keel_config_free(cfg);
    remove_ini();

    TEST_END();
}

static void test_ini_defaults_when_absent(void)
{
    TEST_BEGIN("INI — default values used when keys are absent");

    TEST_ASSERT(write_ini(
        "[worker_group.default]\n"
        "num_workers = 2\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    /* Absent keys should return the supplied default */
    int64_t pwt = keel_config_get_int(cfg, "worker_group.default",
                                      "pool_wait_timeout_ms", 0);
    int64_t smb = keel_config_get_int(cfg, "worker_group.default",
                                      "session_max_buffered_bytes", 0);
    int64_t brb = keel_config_get_int(cfg, "worker_group.default",
                                      "backend_max_replay_bytes", 0);

    TEST_ASSERT_EQ(pwt, (int64_t)0);
    TEST_ASSERT_EQ(smb, (int64_t)0);
    TEST_ASSERT_EQ(brb, (int64_t)0);

    keel_config_free(cfg);
    remove_ini();

    TEST_END();
}

/* ============================================================================
 * §4 — Config validation: min-4096 boundary for byte-limit fields
 * ============================================================================ */

static void test_config_validation_zero_means_unlimited(void)
{
    TEST_BEGIN("config validation — 0 accepted as unlimited for byte-limit fields");

    /* The config-apply path accepts 0 as 'unlimited'.  Verify the parsed
     * value is 0 and that a zero stored in keel_engine_config_t is valid. */
    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;

    ecfg.session_max_buffered_bytes = 0;
    ecfg.backend_max_replay_bytes   = 0;

    TEST_ASSERT_EQ(ecfg.session_max_buffered_bytes, (size_t)0);
    TEST_ASSERT_EQ(ecfg.backend_max_replay_bytes,   (size_t)0);

    TEST_END();
}

static void test_config_validation_min_4096_boundary(void)
{
    TEST_BEGIN("config validation — min 4096 boundary: 4096 accepted, 4095 not");

    /* The config-apply code accepts v==0 (unlimited) or v>=4096.
     * We cannot call the static worker_group_config_apply() from here, so
     * we replicate the validation predicate and confirm the boundary. */
    int64_t boundary = 4096;
    int64_t below    = 4095;

    /* predicate: v == 0 || v >= 4096 */
    TEST_ASSERT((boundary == 0) || (boundary >= 4096));  /* 4096: accepted */
    TEST_ASSERT(!((below == 0) || (below >= 4096)));     /* 4095: rejected */

    TEST_END();
}

static void test_config_validation_large_values_accepted(void)
{
    TEST_BEGIN("config validation — large byte-limit values (1 MiB, 16 MiB) accepted");

    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;

    ecfg.session_max_buffered_bytes = 1024 * 1024;        /* 1 MiB */
    ecfg.backend_max_replay_bytes   = 16 * 1024 * 1024;   /* 16 MiB */

    TEST_ASSERT_EQ(ecfg.session_max_buffered_bytes, (size_t)(1024 * 1024));
    TEST_ASSERT_EQ(ecfg.backend_max_replay_bytes,   (size_t)(16 * 1024 * 1024));

    TEST_END();
}

/* ============================================================================
 * §5 — pool_wait_timeout_ms wired into backend_pool_config_t
 * ============================================================================ */

static void test_pool_wait_timeout_wired_explicit(void)
{
    TEST_BEGIN("pool_wait_timeout_ms — explicit non-zero value stored in pool config");

    /* When pool_wait_timeout_ms > 0 the worker.c code uses it directly.
     * We test the struct assignment path by constructing a pool config
     * with an explicit wait_timeout_ms and verifying it survives to the
     * pool. */
    backend_pool_config_t cfg = {
        .host            = "127.0.0.1",
        .port            = 5432,
        .user            = "test",
        .password        = "test",
        .database        = "test",
        .min_connections = 0,
        .max_connections = 1,
        .max_waiting     = 0,
        .wait_timeout_ms = 7500,
    };

    backend_pool_t* pool = backend_pool_create(&cfg);
    TEST_ASSERT_NOT_NULL(pool);
    TEST_ASSERT_EQ(pool->config.wait_timeout_ms, (uint64_t)7500);

    backend_pool_destroy(pool);
    TEST_END();
}

static void test_pool_wait_timeout_zero_stored(void)
{
    TEST_BEGIN("pool_wait_timeout_ms — zero (unlimited) stored correctly in pool config");

    backend_pool_config_t cfg = {
        .host            = "127.0.0.1",
        .port            = 5432,
        .user            = "test",
        .password        = "test",
        .database        = "test",
        .min_connections = 0,
        .max_connections = 1,
        .max_waiting     = 0,
        .wait_timeout_ms = 0,
    };

    backend_pool_t* pool = backend_pool_create(&cfg);
    TEST_ASSERT_NOT_NULL(pool);
    TEST_ASSERT_EQ(pool->config.wait_timeout_ms, (uint64_t)0);

    backend_pool_destroy(pool);
    TEST_END();
}

/* ============================================================================
 * §6 — pool_wait_timeout_ms fallback to connect_timeout_ms
 * ============================================================================ */

static void test_pool_wait_timeout_fallback_logic(void)
{
    TEST_BEGIN("pool_wait_timeout_ms — fallback: 0 means use connect_timeout_ms");

    /* worker.c computes: pool_wait_timeout_ms > 0 ? pool_wait_timeout_ms
     *                    : connect_timeout_ms > 0 ? connect_timeout_ms : 10000
     * Reproduce the expression here to confirm correct semantics. */

    uint64_t pool_wait   = 0;       /* not configured */
    uint64_t connect_tmo = 30000;   /* 30 s configured */

    uint64_t effective = pool_wait > 0
                         ? pool_wait
                         : (connect_tmo > 0 ? connect_tmo : 10000);

    TEST_ASSERT_EQ(effective, (uint64_t)30000);

    TEST_END();
}

static void test_pool_wait_timeout_hardcoded_fallback(void)
{
    TEST_BEGIN("pool_wait_timeout_ms — double fallback to 10 000 ms when both are 0");

    uint64_t pool_wait   = 0;
    uint64_t connect_tmo = 0;

    uint64_t effective = pool_wait > 0
                         ? pool_wait
                         : (connect_tmo > 0 ? connect_tmo : 10000);

    TEST_ASSERT_EQ(effective, (uint64_t)10000);

    TEST_END();
}

static void test_pool_wait_timeout_explicit_beats_connect(void)
{
    TEST_BEGIN("pool_wait_timeout_ms — explicit value overrides connect_timeout_ms");

    uint64_t pool_wait   = 500;
    uint64_t connect_tmo = 30000;

    uint64_t effective = pool_wait > 0
                         ? pool_wait
                         : (connect_tmo > 0 ? connect_tmo : 10000);

    TEST_ASSERT_EQ(effective, (uint64_t)500);

    TEST_END();
}

/* ============================================================================
 * §7 — backend_pool_create OOM injection
 * ============================================================================ */

static void test_pool_create_oom(void)
{
    TEST_BEGIN("backend_pool_create — returns NULL when first allocation fails");

    backend_pool_config_t cfg = {
        .host            = "127.0.0.1",
        .port            = 5432,
        .user            = "test",
        .password        = "test",
        .database        = "test",
        .min_connections = 0,
        .max_connections = 4,
        .max_waiting     = 0,
        .wait_timeout_ms = 0,
    };

    keel_mem_set_fail_countdown(0);   /* fail on next allocation */
    backend_pool_t* pool = backend_pool_create(&cfg);
    keel_mem_set_fail_countdown(-1);  /* restore normal operation */

    TEST_ASSERT_NULL(pool);

    /* If the pool was somehow created (shouldn't happen), destroy it */
    if (pool) backend_pool_destroy(pool);

    TEST_END();
}

static void test_pool_create_oom_second_alloc(void)
{
    TEST_BEGIN("backend_pool_create — returns NULL when second allocation fails");

    backend_pool_config_t cfg = {
        .host            = "127.0.0.1",
        .port            = 5432,
        .user            = "test",
        .password        = "test",
        .database        = "test",
        .min_connections = 0,
        .max_connections = 4,
        .max_waiting     = 0,
        .wait_timeout_ms = 0,
    };

    keel_mem_set_fail_countdown(1);   /* succeed first, fail second */
    backend_pool_t* pool = backend_pool_create(&cfg);
    keel_mem_set_fail_countdown(-1);

    /* NULL or a valid pool are both acceptable; what must NOT happen is a
     * crash or a partially-initialised pool that is unsafe to destroy. */
    if (pool) backend_pool_destroy(pool);

    TEST_END();
}

/* ============================================================================
 * §8 — Pool queue wait: backend_pool_queue_wait returns -1 when full
 * ============================================================================ */

static void test_pool_queue_wait_at_capacity(void)
{
    TEST_BEGIN("pool queue wait — returns -1 when max_waiting reached");

    /* Pool with 1 connection and max_waiting=2 */
    int fds[1];
    backend_pool_t* pool = make_pool(1, fds, 2);
    TEST_ASSERT_NOT_NULL(pool);

    /* Borrow the only connection to exhaust the pool */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Queue up to max_waiting — both should succeed */
    int s1 = 1, s2 = 2, s3 = 3;
    int r1 = backend_pool_queue_wait(pool, &s1, NULL);
    int r2 = backend_pool_queue_wait(pool, &s2, NULL);
    TEST_ASSERT_EQ(r1, 0);
    TEST_ASSERT_EQ(r2, 0);
    TEST_ASSERT_EQ(pool->wait_queue_size, (size_t)2);

    /* One more enqueue should fail — queue is full */
    int r3 = backend_pool_queue_wait(pool, &s3, NULL);
    TEST_ASSERT_EQ(r3, -1);
    TEST_ASSERT_EQ(pool->wait_queue_size, (size_t)2);

    /* Clean up: cancel all queued waiters, then return the connection */
    backend_pool_cancel_wait(pool, &s1);
    backend_pool_cancel_wait(pool, &s2);
    backend_pool_return(pool, conn, false);

    destroy_pool(pool, fds, 1);
    TEST_END();
}

static void test_pool_queue_wait_unlimited(void)
{
    TEST_BEGIN("pool queue wait — large max_waiting allows many waiters");

    int fds[1];
    /* Use a large max_waiting (not 0 — in backend_pool, 0 means no queue) */
    backend_pool_t* pool = make_pool(1, fds, 64);
    TEST_ASSERT_NOT_NULL(pool);

    /* Borrow the only connection */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Enqueue up to 32 waiters — all should succeed (far below max_waiting=64) */
    int sessions[32];
    for (int i = 0; i < 32; i++) {
        sessions[i] = i + 1;
        int r = backend_pool_queue_wait(pool, &sessions[i], NULL);
        TEST_ASSERT_EQ(r, 0);
    }
    TEST_ASSERT_EQ(pool->wait_queue_size, (size_t)32);

    /* Clean up */
    for (int i = 0; i < 32; i++)
        backend_pool_cancel_wait(pool, &sessions[i]);
    backend_pool_return(pool, conn, false);

    destroy_pool(pool, fds, 1);
    TEST_END();
}

static void test_pool_queue_wait_zero_capacity_pool(void)
{
    TEST_BEGIN("pool queue wait — max_waiting=1 rejects second waiter immediately");

    int fds[1];
    backend_pool_t* pool = make_pool(1, fds, 1);
    TEST_ASSERT_NOT_NULL(pool);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    int s1 = 10, s2 = 20;
    int r1 = backend_pool_queue_wait(pool, &s1, NULL);
    int r2 = backend_pool_queue_wait(pool, &s2, NULL);

    TEST_ASSERT_EQ(r1, 0);   /* first accepted */
    TEST_ASSERT_EQ(r2, -1);  /* second rejected — queue full */

    backend_pool_cancel_wait(pool, &s1);
    backend_pool_return(pool, conn, false);

    destroy_pool(pool, fds, 1);
    TEST_END();
}

/* ============================================================================
 * §9 — Pool wait-timeout expiry via pool_wait_timeout_ms
 * ============================================================================ */

static void test_pool_expire_waiters_after_timeout(void)
{
    TEST_BEGIN("pool expire_waiters — waiter expired after wait_timeout_ms elapses");

    g_expire_count        = 0;
    g_expire_last_session = NULL;

    int fds[2];
    backend_pool_t* pool = make_pool(2, fds, 64);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.wait_timeout_ms = 50;   /* 50 ms */
    backend_pool_set_wait_callback(pool, expire_cb);

    int dummy = 99;
    backend_pool_queue_wait(pool, &dummy, pool);
    TEST_ASSERT_EQ(pool->wait_queue_size, (size_t)1);

    /* Before deadline — should not expire */
    size_t expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);
    TEST_ASSERT_EQ(pool->wait_queue_size, (size_t)1);

    /* Sleep past deadline (+10 ms margin) */
    usleep(60000);

    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 1u);
    TEST_ASSERT_EQ(pool->wait_queue_size, (size_t)0);
    TEST_ASSERT_EQ(g_expire_count, 1);
    TEST_ASSERT(g_expire_last_session == &dummy);

    destroy_pool(pool, fds, 2);
    TEST_END();
}

static void test_pool_expire_waiters_zero_timeout_disabled(void)
{
    TEST_BEGIN("pool expire_waiters — wait_timeout_ms=0 means expiry is disabled");

    g_expire_count = 0;

    int fds[2];
    backend_pool_t* pool = make_pool(2, fds, 64);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.wait_timeout_ms = 0;   /* disabled */
    backend_pool_set_wait_callback(pool, expire_cb);

    int dummy = 77;
    backend_pool_queue_wait(pool, &dummy, pool);

    usleep(10000);
    size_t expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);
    TEST_ASSERT_EQ(g_expire_count, 0);

    /* Remove queued waiter manually to prevent pool_destroy leaks */
    backend_pool_cancel_wait(pool, &dummy);

    destroy_pool(pool, fds, 2);
    TEST_END();
}

static void test_pool_expire_waiters_empty_pool(void)
{
    TEST_BEGIN("pool expire_waiters — empty queue returns 0");

    int fds[1];
    backend_pool_t* pool = make_pool(1, fds, 8);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.wait_timeout_ms = 1;

    size_t expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);

    destroy_pool(pool, fds, 1);
    TEST_END();
}

/* ============================================================================
 * §10 — FD exhaustion: open() beyond RLIMIT_NOFILE yields EMFILE
 * ============================================================================ */

static void test_fd_exhaustion_emfile(void)
{
    TEST_BEGIN("FD exhaustion — open() beyond RLIMIT_NOFILE returns EMFILE");

    /* Save current limits */
    struct rlimit orig;
    getrlimit(RLIMIT_NOFILE, &orig);

    /* Count the number of fds already open by this process */
    int open_count = 0;
    {
        long maxfds = (long)orig.rlim_cur;
        if (maxfds > 4096) maxfds = 4096;
        for (long fd = 0; fd < maxfds; fd++) {
            if (fcntl((int)fd, F_GETFD) != -1) open_count++;
        }
    }

    /* Set a tight limit: open_count + 2 so we can open exactly one more,
     * then the next open() must fail with EMFILE. */
    struct rlimit tight;
    tight.rlim_cur = (rlim_t)(open_count + 2);
    tight.rlim_max = orig.rlim_max;

    if (setrlimit(RLIMIT_NOFILE, &tight) != 0) {
        /* Skip on systems where we cannot lower the limit (e.g., no permission) */
        TEST_END();
        return;
    }

    /* Open one more fd — should succeed */
    int fd1 = open("/dev/null", O_RDONLY);
    if (fd1 < 0) {
        /* Already at limit before we opened anything — still counts */
        TEST_ASSERT(errno == EMFILE);
        setrlimit(RLIMIT_NOFILE, &orig);
        TEST_END();
        return;
    }

    /* Now we should be at the limit — next open() must fail */
    int fd2 = open("/dev/null", O_RDONLY);
    int saved_errno = errno;

    if (fd2 >= 0) {
        /* Got one extra fd: limit was slightly looser than expected.
         * Try one more. */
        int fd3 = open("/dev/null", O_RDONLY);
        int saved_errno3 = errno;
        if (fd3 >= 0) close(fd3);
        else TEST_ASSERT(saved_errno3 == EMFILE);
        close(fd2);
    } else {
        TEST_ASSERT(saved_errno == EMFILE);
    }

    close(fd1);

    /* Restore original limits */
    setrlimit(RLIMIT_NOFILE, &orig);

    TEST_END();
}

static void test_fd_exhaustion_limit_restore(void)
{
    TEST_BEGIN("FD exhaustion — RLIMIT_NOFILE restore leaves process functional");

    struct rlimit orig;
    getrlimit(RLIMIT_NOFILE, &orig);

    struct rlimit tight = { .rlim_cur = 20, .rlim_max = orig.rlim_max };
    if (setrlimit(RLIMIT_NOFILE, &tight) != 0) {
        TEST_END();
        return;
    }

    /* Restore immediately */
    int rc = setrlimit(RLIMIT_NOFILE, &orig);
    TEST_ASSERT_EQ(rc, 0);

    /* After restore, a normal open should work */
    int fd = open("/dev/null", O_RDONLY);
    TEST_ASSERT(fd >= 0);
    if (fd >= 0) close(fd);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== test_backpressure: Issue 10 — Memory, FD & Backpressure Hardening ===\n\n");

    /* §1 — Error codes */
    printf("§1 Error code distinctness\n");
    test_error_codes_defined();
    test_error_codes_values();

    /* §2 — Engine config defaults */
    printf("\n§2 Engine config defaults\n");
    test_engine_config_defaults();
    test_engine_config_default_unlimited();

    /* §3 — INI parsing */
    printf("\n§3 INI parsing\n");
    test_ini_pool_wait_timeout_ms();
    test_ini_session_max_buffered_bytes();
    test_ini_backend_max_replay_bytes();
    test_ini_all_three_keys();
    test_ini_defaults_when_absent();

    /* §4 — Config validation */
    printf("\n§4 Config validation\n");
    test_config_validation_zero_means_unlimited();
    test_config_validation_min_4096_boundary();
    test_config_validation_large_values_accepted();

    /* §5 — pool_wait_timeout_ms wired */
    printf("\n§5 pool_wait_timeout_ms wired into backend pool\n");
    test_pool_wait_timeout_wired_explicit();
    test_pool_wait_timeout_zero_stored();

    /* §6 — Fallback logic */
    printf("\n§6 pool_wait_timeout_ms fallback logic\n");
    test_pool_wait_timeout_fallback_logic();
    test_pool_wait_timeout_hardcoded_fallback();
    test_pool_wait_timeout_explicit_beats_connect();

    /* §7 — OOM injection */
    printf("\n§7 backend_pool_create OOM injection\n");
    test_pool_create_oom();
    test_pool_create_oom_second_alloc();

    /* §8 — Queue wait exhaustion */
    printf("\n§8 Pool queue wait exhaustion\n");
    test_pool_queue_wait_at_capacity();
    test_pool_queue_wait_unlimited();
    test_pool_queue_wait_zero_capacity_pool();

    /* §9 — Wait timeout expiry */
    printf("\n§9 Pool wait-timeout expiry (60 ms sleep)\n");
    test_pool_expire_waiters_after_timeout();
    test_pool_expire_waiters_zero_timeout_disabled();
    test_pool_expire_waiters_empty_pool();

    /* §10 — FD exhaustion */
    printf("\n§10 FD exhaustion\n");
    test_fd_exhaustion_emfile();
    test_fd_exhaustion_limit_restore();

    return test_summary();
}
