/**
 * @file test_graceful_restart.c
 * @brief Tests for graceful worker restart: drain flag, active sessions,
 *        and live engine restart lifecycle.
 */

#include "test_utils.h"
#include "keel/engine/engine.h"
#include "keel/engine/worker.h"

#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

static int make_listen_socket(uint16_t *out_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = 0,
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

static void *engine_run_thread(void *arg) {
    keel_engine_t *engine = (keel_engine_t *)arg;
    keel_engine_run(engine);
    return NULL;
}

static void stop_engine_and_join(keel_engine_t *engine, pthread_t tid) {
    keel_engine_stop(engine);
    pthread_kill(tid, SIGTERM);
    pthread_join(tid, NULL);
}

/* ============================================================================
 * Test 1: Restart returns -1 when engine not running
 * ============================================================================ */
static void test_restart_not_running(void) {
    TEST_BEGIN("restart: returns -1 when engine not running");

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (engine) {
        int rc = keel_engine_restart_workers(engine, 1000);
        TEST_ASSERT_EQ(rc, -1);
        keel_engine_destroy(engine);
    }

    TEST_END();
}

/* ============================================================================
 * Test 2: Restart returns -1 for NULL engine
 * ============================================================================ */
static void test_restart_null_engine(void) {
    TEST_BEGIN("restart: returns -1 for NULL engine");

    int rc = keel_engine_restart_workers(NULL, 0);
    TEST_ASSERT_EQ(rc, -1);

    TEST_END();
}

/* ============================================================================
 * Test 3: Live engine restart with zero sessions
 * ============================================================================ */
static void test_live_restart(void) {
    TEST_BEGIN("live restart: workers replaced while engine keeps running");

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);
    TEST_ASSERT(listen_fd >= 0);

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 2;
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

    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(100000); /* Let workers come up */

    /* Engine should be ACTIVE */
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine),
                   (int)KEEL_ENGINE_STATE_ACTIVE);

    /* Restart workers — no sessions, should complete quickly */
    rc = keel_engine_restart_workers(engine, 2000);
    TEST_ASSERT_EQ(rc, 0);

    /* Engine should still be running after restart */
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine),
                   (int)KEEL_ENGINE_STATE_ACTIVE);

    /* Clean shutdown */
    stop_engine_and_join(engine, tid);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine),
                   (int)KEEL_ENGINE_STATE_STOPPED);

    keel_engine_destroy(engine);
    TEST_END();
}

/* ============================================================================
 * Test 4: Double restart in quick succession
 * ============================================================================ */
static void test_double_restart(void) {
    TEST_BEGIN("double restart: two consecutive restarts succeed");

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

    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(100000);

    /* First restart */
    rc = keel_engine_restart_workers(engine, 2000);
    TEST_ASSERT_EQ(rc, 0);

    usleep(50000); /* Let new workers settle */

    /* Second restart */
    rc = keel_engine_restart_workers(engine, 2000);
    TEST_ASSERT_EQ(rc, 0);

    /* Still active */
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine),
                   (int)KEEL_ENGINE_STATE_ACTIVE);

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */
int main(void) {
    test_restart_not_running();
    test_restart_null_engine();
    test_live_restart();
    test_double_restart();

    test_summary();
    return g_tests_failed > 0 ? 1 : 0;
}
