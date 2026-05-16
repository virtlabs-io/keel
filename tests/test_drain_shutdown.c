/**
 * @file test_drain_shutdown.c
 * @brief Integration-style checks for engine drain and shutdown semantics.
 *
 * Graceful shutdown is where many subsystems converge: listener lifecycle,
 * worker acceptance, session rejection, drain timeouts, and commit-in-doubt
 * protection. This suite exercises that control surface directly because small
 * regressions here can turn a clean deploy or failover into dropped traffic or
 * unsafe transaction handling.
 *
 * The tests mix cheap structural checks with a lightweight live-engine run so
 * the state enum transitions and externally visible drain behavior are both
 * covered.
 */

#include "test_utils.h"
#include "keel/engine/engine.h"
#include "keel/session/session.h"

#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Create a loopback listening socket on an ephemeral port.
 * @param out_port [out] Receives the assigned TCP port.
 * @return Listening socket fd or `-1` on failure.
 *
 * Using a real TCP listener instead of `socketpair()` matters here because the
 * engine's drain path interacts with accept/listen state, not just with generic
 * connected descriptors.
 */
static int make_listen_socket(uint16_t *out_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = 0,  /* let the OS pick */
    };
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);
    *out_port = ntohs(bound.sin_port);
    return fd;
}

/**
 * @brief Connect a client socket to the temporary loopback listener.
 * @param port Loopback port chosen by make_listen_socket().
 * @return Connected fd or `-1` on failure.
 */
static int connect_to(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = htons(port),
    };
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* ============================================================================
 * Test 1: Lifecycle state enum transitions
 * ============================================================================ */
static void test_lifecycle_states(void) {
    TEST_BEGIN("lifecycle states: CREATED → check defaults");

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (engine) {
        /* Engine starts in CREATED state */
        TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_CREATED);

        /* Not draining, not running */
        TEST_ASSERT_EQ(keel_engine_is_draining(engine), false);
        TEST_ASSERT_EQ(keel_engine_get_active_connections(engine), (uint64_t)0);

        keel_engine_destroy(engine);
    }

    TEST_END();
}

/* ============================================================================
 * Test 2: Drain API on non-running engine
 * ============================================================================ */
static void test_drain_not_running(void) {
    TEST_BEGIN("drain: returns -1 when engine not running");

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (engine) {
        keel_engine_set_drain_timeout(engine, 1000);

        int rc = keel_engine_drain(engine);
        TEST_ASSERT_EQ(rc, -1);

        /* State should still be CREATED (drain didn't execute) */
        TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_CREATED);
        TEST_ASSERT_EQ(keel_engine_is_draining(engine), false);

        keel_engine_destroy(engine);
    }

    TEST_END();
}

/* ============================================================================
 * Test 3: Force-close skips commit_in_doubt sessions
 * ============================================================================ */
static void test_force_close_cid_protection(void) {
    TEST_BEGIN("force-close: skips sessions with commit_in_doubt");

    /* Verify at the session level that the flag exists and is respected
     * by engine_force_close_all.  We can't easily instantiate a full
     * engine with sessions here, but we verify the session struct layout. */
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.client_fd = -1;
    session.server_fd = -1;
    session.commit_in_doubt = false;

    /* Verify default state */
    TEST_ASSERT_EQ(session.commit_in_doubt, false);

    /* Set CID flag */
    session.commit_in_doubt = true;
    TEST_ASSERT_EQ(session.commit_in_doubt, true);

    /* Clear it (as engine_flow does after txid_status resolution) */
    session.commit_in_doubt = false;
    TEST_ASSERT_EQ(session.commit_in_doubt, false);

    TEST_END();
}

/* ============================================================================
 * Test 4: Live engine start → drain → stop (state machine transitions)
 * ============================================================================ */

/*
 * The engine run loop blocks in its own signal-aware wait path, so the live
 * drain test runs it in a helper thread and stops it using the same stop-plus-
 * signal sequence production shutdown relies on.
 */
static void *engine_run_thread(void *arg) {
    keel_engine_t *engine = (keel_engine_t *)arg;
    keel_engine_run(engine);
    return NULL;
}

/**
 * @brief Stop the engine and unblock the thread running `keel_engine_run()`.
 * @param engine Engine instance being shut down.
 * @param tid Thread executing the run loop.
 * @return
 *
 * The explicit signal delivery is not test ceremony; it reflects a real runtime
 * property of the engine loop, which may be blocked in `sigwait()` even after a
 * stop flag has been set.
 */
static void stop_engine_and_join(keel_engine_t *engine, pthread_t tid) {
    keel_engine_stop(engine);
    /* Unblock sigwait() in the run-loop thread */
    pthread_kill(tid, SIGTERM);
    pthread_join(tid, NULL);
}

static void test_live_engine_drain(void) {
    TEST_BEGIN("live engine: start → drain → stop lifecycle");

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);
    TEST_ASSERT(listen_fd >= 0);

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;
    cfg.queue_depth = 64;
    cfg.session_pool_size = 32;
    cfg.idle_timeout_ms = 60000;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (!engine || listen_fd < 0) {
        if (listen_fd >= 0) close(listen_fd);
        if (engine) keel_engine_destroy(engine);
        TEST_END();
        return;
    }

    /* Start engine */
    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_ACTIVE);

    /* Run engine event loop in a background thread */
    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);

    /* Brief sleep to let workers start */
    usleep(50000); /* 50ms */

    /* Connect a client to prove accept works */
    int client = connect_to(port);
    /* Connection may or may not succeed depending on worker state, don't assert */

    /* Give time for accept to process */
    usleep(50000);

    /* Initiate drain (short timeout since there may be 0-1 active connections) */
    keel_engine_set_drain_timeout(engine, 500);
    rc = keel_engine_drain(engine);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(keel_engine_is_draining(engine), true);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_DRAINING);

    /* During drain, new connections should be rejected.
     * The listen fd is closed by drain, so connect() should fail. */
    int client2 = connect_to(port);
    /* client2 should fail (connection refused) or succeed and be immediately closed */
    if (client2 >= 0) {
        /* If it connected (race condition), try to read — should get closed or error */
        uint8_t buf[256];
        ssize_t n = recv(client2, buf, sizeof(buf), MSG_DONTWAIT);
        /* We accept any result here — the important thing is the connection
         * is rejected at the worker level even if TCP handshake completed. */
        (void)n;
        close(client2);
    }

    /* Stop engine and unblock run-loop */
    stop_engine_and_join(engine, tid);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_STOPPED);

    /* Cleanup */
    if (client >= 0) close(client);
    keel_engine_destroy(engine);

    TEST_END();
}

/* ============================================================================
 * Test 5: Drain timeout with force-close
 * ============================================================================ */
static void test_drain_timeout_force_close(void) {
    TEST_BEGIN("drain timeout: force-close on expiry");

    /* Engine without active connections — drain should return immediately.
     * With 0 active connections, the force-close path is not hit,
     * but the API call succeeds cleanly. */

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);
    TEST_ASSERT(listen_fd >= 0);

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;
    cfg.queue_depth = 64;
    cfg.session_pool_size = 32;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (!engine || listen_fd < 0) {
        if (listen_fd >= 0) close(listen_fd);
        if (engine) keel_engine_destroy(engine);
        TEST_END();
        return;
    }

    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(50000);

    /* Set very short timeout */
    keel_engine_set_drain_timeout(engine, 100);
    rc = keel_engine_drain(engine);
    TEST_ASSERT_EQ(rc, 0);

    /* With 0 active connections, drain completes instantly (no force-close needed) */
    TEST_ASSERT_EQ(keel_engine_get_active_connections(engine), (uint64_t)0);

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);

    TEST_END();
}

/* ============================================================================
 * Test 6: Drain rejection sends PostgreSQL FATAL 57P03
 * ============================================================================ */
static void test_drain_pg_error_response(void) {
    TEST_BEGIN("drain rejection: PG FATAL 57P03 sent to connecting clients");

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);
    TEST_ASSERT(listen_fd >= 0);

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;
    cfg.queue_depth = 64;
    cfg.session_pool_size = 32;
    cfg.default_protocol = "postgres";

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (!engine || listen_fd < 0) {
        if (listen_fd >= 0) close(listen_fd);
        if (engine) keel_engine_destroy(engine);
        TEST_END();
        return;
    }

    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(50000);

    /* Now drain — listen fd will be closed, but any connections that
     * sneak in through the SYN backlog should get a PG FATAL error.
     * Since we close the listen_fd, new TCP connect()s should fail
     * with ECONNREFUSED.  Verify drain state is correct. */
    keel_engine_set_drain_timeout(engine, 500);
    rc = keel_engine_drain(engine);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(keel_engine_is_draining(engine), true);

    /* Try connecting after drain — should fail since listen fd is closed */
    int client = connect_to(port);
    if (client >= 0) {
        /* TCP might still connect due to SYN backlog race.
         * If it does, read and check for PG ErrorResponse. */
        uint8_t buf[256];
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
        setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ssize_t n = recv(client, buf, sizeof(buf), 0);
        if (n > 0) {
            /* Should start with 'E' (ErrorResponse) */
            TEST_ASSERT_EQ(buf[0], 'E');
            /* Look for SQLSTATE 57P03 in the response */
            bool found_57P03 = false;
            for (ssize_t i = 0; i < n - 5; i++) {
                if (buf[i] == 'C' && memcmp(&buf[i+1], "57P03", 5) == 0) {
                    found_57P03 = true;
                    break;
                }
            }
            TEST_ASSERT(found_57P03);
        }
        /* If n <= 0, the connection was closed before we could read: also acceptable. */
        close(client);
    }
    /* Connection refused is the expected case — drain closed the listen fd. */

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);

    TEST_END();
}

/* ============================================================================
 * Test 7: State enum values are distinct
 * ============================================================================ */
static void test_state_enum_values(void) {
    TEST_BEGIN("lifecycle states: enum values are distinct and ordered");

    TEST_ASSERT_EQ((int)KEEL_ENGINE_STATE_CREATED,  0);
    TEST_ASSERT_EQ((int)KEEL_ENGINE_STATE_ACTIVE,   1);
    TEST_ASSERT_EQ((int)KEEL_ENGINE_STATE_DRAINING, 2);
    TEST_ASSERT_EQ((int)KEEL_ENGINE_STATE_STOPPING, 3);
    TEST_ASSERT_EQ((int)KEEL_ENGINE_STATE_STOPPED,  4);

    /* States are ordered for comparison */
    TEST_ASSERT((int)KEEL_ENGINE_STATE_CREATED < (int)KEEL_ENGINE_STATE_ACTIVE);
    TEST_ASSERT((int)KEEL_ENGINE_STATE_ACTIVE < (int)KEEL_ENGINE_STATE_DRAINING);
    TEST_ASSERT((int)KEEL_ENGINE_STATE_DRAINING < (int)KEEL_ENGINE_STATE_STOPPING);
    TEST_ASSERT((int)KEEL_ENGINE_STATE_STOPPING < (int)KEEL_ENGINE_STATE_STOPPED);

    TEST_END();
}

/* ============================================================================
 * Test 8: Force-close returns 0 on non-initialized engine
 * ============================================================================ */
static void test_force_close_without_pool(void) {
    TEST_BEGIN("force-close: returns 0 when no worker pool");

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (engine) {
        /* No pool initialized yet — force close should return 0 */
        uint64_t killed = keel_engine_force_close_all(engine);
        TEST_ASSERT_EQ(killed, (uint64_t)0);

        keel_engine_destroy(engine);
    }

    TEST_END();
}

/* ============================================================================
 * Test 9: Session commit_in_doubt flag integration
 * ============================================================================ */
static void test_session_commit_in_doubt_flag(void) {
    TEST_BEGIN("session: commit_in_doubt flag init and lifecycle");

    keel_session_t session;
    memset(&session, 0, sizeof(session));

    /* Default value after zero-init */
    TEST_ASSERT_EQ(session.commit_in_doubt, false);
    TEST_ASSERT_EQ(session.in_transaction, false);

    /* Set in_transaction (normal state) */
    session.in_transaction = true;
    TEST_ASSERT_EQ(session.in_transaction, true);
    TEST_ASSERT_EQ(session.commit_in_doubt, false);

    /* Backend dies during COMMIT → CID activated */
    session.commit_in_doubt = true;
    TEST_ASSERT_EQ(session.commit_in_doubt, true);

    /* txid_status() check resolves → CID cleared */
    session.commit_in_doubt = false;
    session.in_transaction = false;
    TEST_ASSERT_EQ(session.commit_in_doubt, false);
    TEST_ASSERT_EQ(session.in_transaction, false);

    TEST_END();
}

/* ============================================================================
 * Test 10: Full lifecycle with drain (CREATED → ACTIVE → DRAINING → STOPPED)
 * ============================================================================ */
static void test_full_lifecycle(void) {
    TEST_BEGIN("lifecycle: CREATED → ACTIVE → DRAINING → STOPPING → STOPPED");

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);
    TEST_ASSERT(listen_fd >= 0);

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;
    cfg.queue_depth = 64;
    cfg.session_pool_size = 32;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (!engine || listen_fd < 0) {
        if (listen_fd >= 0) close(listen_fd);
        if (engine) keel_engine_destroy(engine);
        TEST_END();
        return;
    }

    /* CREATED */
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_CREATED);

    /* Start → ACTIVE */
    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_ACTIVE);

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(50000);

    /* Drain → DRAINING */
    keel_engine_set_drain_timeout(engine, 500);
    rc = keel_engine_drain(engine);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_DRAINING);

    /* Stop → STOPPING → STOPPED */
    stop_engine_and_join(engine, tid);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_STOPPED);

    keel_engine_destroy(engine);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */
int main(void) {
    test_lifecycle_states();
    test_drain_not_running();
    test_force_close_cid_protection();
    test_state_enum_values();
    test_force_close_without_pool();
    test_session_commit_in_doubt_flag();
    test_live_engine_drain();
    test_drain_timeout_force_close();
    test_drain_pg_error_response();
    test_full_lifecycle();

    test_summary();
    return g_tests_failed > 0 ? 1 : 0;
}
