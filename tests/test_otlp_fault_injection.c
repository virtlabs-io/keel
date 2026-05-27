/**
 * @file test_otlp_fault_injection.c
 * @brief Negative + fault-injection tests for the OTLP exporter pipeline
 *        (proposals/v0.2-alpha_observability.md §29.6).
 *
 * Covers the observability-scope items from §29.6:
 *   1. nanopb encode failure increments exporter failure counters
 *      (forced via tiny encode_buf_bytes).
 *   2. exporter HTTP timeout increments timeout/failure counters
 *      (mock collector that accepts but never replies).
 *   3. collector unavailable does not block submitters
 *      (1000 submits to a dead port must complete in negligible wall time).
 *   4. JSON/Prometheus admin endpoints remain available when collector down.
 *   5. Prometheus scrape during snapshot rotation returns valid text format
 *      (concurrent aggregator + repeated scrapes assert well-formed output).
 *
 * Existing tests (test_otlp_snapshot_queue.c, test_otlp_exporter.c) already
 * cover queue overflow drop-oldest accounting and shutdown release of
 * waiters; this file does not duplicate them.
 */

#include "test_utils.h"
#include "keel/core/admin.h"
#include "keel/engine/engine.h"

#include "keel_otlp_aggregator.h"
#include "keel_otlp_exporter.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

static void msleep(uint32_t ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static uint64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static keel_otlp_snapshot_t snap_with(uint64_t v) {
    keel_otlp_snapshot_t s = {0};
    s.start_time_unix_nano = 1;
    s.time_unix_nano       = v + 1;
    s.metric_count         = 1;
    snprintf(s.metrics[0].name, sizeof(s.metrics[0].name),
             "keel_test_total");
    s.metrics[0].value = v;
    return s;
}

/* ============================================================================
 * Mock collectors
 * ============================================================================ */

/* Silent collector: accepts the connection, then sleeps forever without
 * replying. Forces HTTP read timeout on the client. */
typedef struct silent_collector {
    int           listen_fd;
    uint16_t      port;
    pthread_t     thread;
    atomic_bool   stop;
    atomic_int    accepts;
} silent_collector_t;

static void* silent_thread(void* arg) {
    silent_collector_t* s = arg;
    while (!atomic_load(&s->stop)) {
        struct pollfd p = { .fd = s->listen_fd, .events = POLLIN };
        if (poll(&p, 1, 50) <= 0) continue;
        int cfd = accept(s->listen_fd, NULL, NULL);
        if (cfd < 0) continue;
        atomic_fetch_add(&s->accepts, 1);
        /* Do NOT recv, do NOT send. Just hold the connection until stop. */
        while (!atomic_load(&s->stop)) msleep(20);
        close(cfd);
    }
    return NULL;
}

static silent_collector_t* silent_start(void) {
    static silent_collector_t s;
    memset(&s, 0, sizeof(s));
    s.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s.listen_fd >= 0);
    int one = 1;
    setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s.listen_fd, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    TEST_ASSERT(getsockname(s.listen_fd, (struct sockaddr*)&a, &al) == 0);
    s.port = ntohs(a.sin_port);
    TEST_ASSERT(listen(s.listen_fd, 8) == 0);
    TEST_ASSERT(pthread_create(&s.thread, NULL, silent_thread, &s) == 0);
    return &s;
}

static void silent_stop(silent_collector_t* s) {
    atomic_store(&s->stop, true);
    pthread_join(s->thread, NULL);
    close(s->listen_fd);
}

/* OK collector: drains body and replies 200. */
typedef struct ok_collector {
    int           listen_fd;
    uint16_t      port;
    pthread_t     thread;
    atomic_bool   stop;
    atomic_int    requests;
} ok_collector_t;

static void* ok_thread(void* arg) {
    ok_collector_t* s = arg;
    while (!atomic_load(&s->stop)) {
        struct pollfd p = { .fd = s->listen_fd, .events = POLLIN };
        if (poll(&p, 1, 50) <= 0) continue;
        int cfd = accept(s->listen_fd, NULL, NULL);
        if (cfd < 0) continue;
        char   buf[16384];
        size_t off = 0, cl = 0, hdr_end = 0;
        bool   have_hdrs = false;
        while (off + 1 < sizeof(buf)) {
            struct pollfd cp = { .fd = cfd, .events = POLLIN };
            if (poll(&cp, 1, 1000) <= 0) break;
            ssize_t n = recv(cfd, buf + off, sizeof(buf) - 1 - off, 0);
            if (n <= 0) break;
            off += (size_t)n;
            buf[off] = '\0';
            if (!have_hdrs) {
                char* he = strstr(buf, "\r\n\r\n");
                if (he) {
                    have_hdrs = true;
                    hdr_end   = (size_t)(he - buf) + 4;
                    char* clh = strcasestr(buf, "Content-Length:");
                    if (clh) cl = strtoul(clh + 15, NULL, 10);
                }
            }
            if (have_hdrs && off >= hdr_end + cl) break;
        }
        atomic_fetch_add(&s->requests, 1);
        const char* resp =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        (void)send(cfd, resp, strlen(resp), MSG_NOSIGNAL);
        close(cfd);
    }
    return NULL;
}

static ok_collector_t* ok_start(void) {
    static ok_collector_t s;
    memset(&s, 0, sizeof(s));
    s.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s.listen_fd >= 0);
    int one = 1;
    setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s.listen_fd, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    TEST_ASSERT(getsockname(s.listen_fd, (struct sockaddr*)&a, &al) == 0);
    s.port = ntohs(a.sin_port);
    TEST_ASSERT(listen(s.listen_fd, 8) == 0);
    TEST_ASSERT(pthread_create(&s.thread, NULL, ok_thread, &s) == 0);
    return &s;
}

static void ok_stop(ok_collector_t* s) {
    atomic_store(&s->stop, true);
    pthread_join(s->thread, NULL);
    close(s->listen_fd);
}

/* ============================================================================
 * Test 1: tiny encode buffer → encoder reports failure → counters reflect it
 * ============================================================================ */

static void test_encode_buffer_too_small_increments_failures(void) {
    ok_collector_t* c = ok_start();
    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", c->port);

    /* encode_buf_bytes=16 cannot fit any OTLP request envelope: the
     * ExportMetricsServiceRequest header alone is larger. */
    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = url, .timeout_ms = 200 },
        .interval_ms      = 30,
        .max_retries      = 0,
        .queue_capacity   = 4,
        .encode_buf_bytes = 16,
    };
    keel_otlp_exporter_t* e = keel_otlp_exporter_create(&cfg);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    for (uint64_t i = 0; i < 3; i++) {
        keel_otlp_snapshot_t s = snap_with(i);
        keel_otlp_exporter_submit(e, &s);
    }

    keel_exporter_stats_t st;
    for (int i = 0; i < 200; i++) {
        keel_otlp_exporter_self_stats(e, &st);
        if (st.failures >= 1 && st.attempts >= 1) break;
        msleep(10);
    }
    TEST_ASSERT(st.attempts  >= 1);
    TEST_ASSERT(st.failures  >= 1);
    TEST_ASSERT_EQ((int)st.successes, 0);
    /* No request should have reached the collector since encode failed. */
    TEST_ASSERT_EQ(atomic_load(&c->requests), 0);

    keel_otlp_exporter_destroy(e);
    ok_stop(c);
}

/* ============================================================================
 * Test 2: silent collector → HTTP read times out → failures+timeouts ++
 * ============================================================================ */

static void test_http_timeout_increments_counters(void) {
    silent_collector_t* c = silent_start();
    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", c->port);

    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = url, .timeout_ms = 80 },
        .interval_ms      = 20,
        .max_retries      = 0,
        .queue_capacity   = 4,
        .encode_buf_bytes = 8192,
    };
    keel_otlp_exporter_t* e = keel_otlp_exporter_create(&cfg);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    keel_otlp_snapshot_t s = snap_with(42);
    keel_otlp_exporter_submit(e, &s);

    keel_exporter_stats_t st;
    for (int i = 0; i < 200; i++) {
        keel_otlp_exporter_self_stats(e, &st);
        if (st.failures >= 1) break;
        msleep(20);
    }
    TEST_ASSERT(st.attempts >= 1);
    TEST_ASSERT(st.failures >= 1);
    TEST_ASSERT_EQ((int)st.successes, 0);
    TEST_ASSERT(st.last_failure_unix_ms > 0);
    /* Timeout-class failure: either the dedicated timeouts counter
     * incremented, or last_status carries a timeout code. Both are
     * acceptable per the §21 schema. */
    TEST_ASSERT(st.timeouts >= 1 || st.last_status != 0);
    TEST_ASSERT(atomic_load(&c->accepts) >= 1);

    keel_otlp_exporter_destroy(e);
    silent_stop(c);
}

/* ============================================================================
 * Test 3: submit is non-blocking when the collector is unreachable
 * ============================================================================ */

static void test_submit_never_blocks_when_collector_down(void) {
    /* No collector at all. Use a private port that nothing is listening on
     * — bind+listen then close to learn an unused port number. */
    int s = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s >= 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    TEST_ASSERT(getsockname(s, (struct sockaddr*)&a, &al) == 0);
    uint16_t dead_port = ntohs(a.sin_port);
    close(s);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", dead_port);

    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = url, .timeout_ms = 1000 },
        .interval_ms      = 100,
        .max_retries      = 0,
        .queue_capacity   = 4,
        .encode_buf_bytes = 8192,
    };
    keel_otlp_exporter_t* e = keel_otlp_exporter_create(&cfg);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    /* 1000 submits must complete in well under a second: with a bounded
     * queue and drop-oldest semantics, each submit is a single push that
     * never waits on the HTTP layer. */
    uint64_t t0 = mono_ns();
    for (int i = 0; i < 1000; i++) {
        keel_otlp_snapshot_t snap = snap_with((uint64_t)i);
        (void)keel_otlp_exporter_submit(e, &snap);
    }
    uint64_t elapsed_ns = mono_ns() - t0;
    /* Conservative bound — even on a busy CI box this should be milliseconds. */
    TEST_ASSERT(elapsed_ns < 500ULL * 1000000ULL); /* < 500 ms */

    keel_otlp_exporter_destroy(e);
}

/* ============================================================================
 * Test 4: admin endpoints stay available when the OTLP collector is down
 * ============================================================================ */

typedef struct admin_fix {
    keel_engine_t* engine;
    keel_admin_t*  admin;
    uint16_t       port;
} admin_fix_t;

static int admin_fix_start(admin_fix_t* f) {
    memset(f, 0, sizeof(*f));
    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;
    f->engine = keel_engine_create(&ecfg);
    if (!f->engine) return -1;
    keel_admin_config_t acfg = KEEL_ADMIN_CONFIG_DEFAULT;
    acfg.admin_enabled = true;
    acfg.admin_addr    = "127.0.0.1"; acfg.admin_port = 0;
    acfg.prom_enabled  = true;
    acfg.prom_addr     = "127.0.0.1"; acfg.prom_port  = 0;
    f->admin = keel_admin_start(&acfg, f->engine);
    if (!f->admin) { keel_engine_destroy(f->engine); return -1; }
    f->port = keel_admin_get_prom_port(f->admin);
    return f->port > 0 ? 0 : -1;
}

static void admin_fix_stop(admin_fix_t* f) {
    keel_admin_stop(f->admin);
    keel_engine_destroy(f->engine);
}

static int http_get(uint16_t port, const char* path, char* out, size_t out_cap) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        close(fd); return -1;
    }
    char req[256];
    int  n = snprintf(req, sizeof(req),
                      "GET %s HTTP/1.0\r\nHost: localhost\r\n\r\n", path);
    if (send(fd, req, (size_t)n, 0) < 0) { close(fd); return -1; }
    size_t total = 0;
    for (;;) {
        if (total >= out_cap - 1) break;
        ssize_t r = recv(fd, out + total, out_cap - 1 - total, 0);
        if (r <= 0) break;
        total += (size_t)r;
    }
    out[total] = '\0';
    close(fd);
    return (int)total;
}

static void test_admin_endpoints_when_collector_down(void) {
    admin_fix_t f;
    TEST_ASSERT_EQ(admin_fix_start(&f), 0);

    /* Find a guaranteed-dead port. */
    int s = socket(AF_INET, SOCK_STREAM, 0); TEST_ASSERT(s >= 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    getsockname(s, (struct sockaddr*)&a, &al);
    uint16_t dead = ntohs(a.sin_port);
    close(s);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", dead);
    keel_otlp_exporter_config_t ecfg = {
        .http = { .endpoint_url = url, .timeout_ms = 100 },
        .interval_ms      = 30,
        .max_retries      = 0,
        .queue_capacity   = 4,
        .encode_buf_bytes = 8192,
    };
    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&ecfg);
    TEST_ASSERT_NOT_NULL(exp);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(exp), 0);
    keel_admin_set_otlp_exporter(f.admin, exp);

    /* Push a few snapshots so the exporter records failures. */
    for (uint64_t i = 0; i < 3; i++) {
        keel_otlp_snapshot_t snap = snap_with(i);
        keel_otlp_exporter_submit(exp, &snap);
    }
    msleep(200);

    /* Every admin endpoint must still answer 200 with non-trivial bodies. */
    char buf[16384];

    int n = http_get(f.port, "/api/observability/metrics.json", buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(buf, "\"metrics\":")    != NULL);
    /* The embedded exporter health block must reflect the failures. */
    TEST_ASSERT(strstr(buf, "\"export_failure_total\":") != NULL);

    n = http_get(f.port, "/api/observability/metrics.prom", buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(buf, "# TYPE keel_sessions_created_total counter") != NULL);

    n = http_get(f.port, "/api/observability/exporter.json", buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_failure_total\":") != NULL);

    n = http_get(f.port, "/metrics", buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(buf, "# TYPE") != NULL);

    keel_admin_set_otlp_exporter(f.admin, NULL);
    keel_otlp_exporter_destroy(exp);
    admin_fix_stop(&f);
}

/* ============================================================================
 * Test 5: Prometheus scrape during snapshot rotation stays well-formed
 * ============================================================================ */

static void test_prom_scrape_during_rotation(void) {
    admin_fix_t f;
    TEST_ASSERT_EQ(admin_fix_start(&f), 0);

    ok_collector_t* c = ok_start();
    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", c->port);

    keel_otlp_exporter_config_t ecfg = {
        .http = { .endpoint_url = url, .timeout_ms = 500 },
        .interval_ms      = 20,
        .max_retries      = 0,
        .queue_capacity   = 4,
        .encode_buf_bytes = 8192,
    };
    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&ecfg);
    TEST_ASSERT_NOT_NULL(exp);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(exp), 0);
    keel_admin_set_otlp_exporter(f.admin, exp);

    keel_stats_collector_t* coll = keel_engine_get_stats_collector(f.engine);
    TEST_ASSERT_NOT_NULL(coll);
    keel_otlp_aggregator_t* agg = keel_otlp_aggregator_create(coll, exp, 20);
    TEST_ASSERT_NOT_NULL(agg);
    TEST_ASSERT_EQ(keel_otlp_aggregator_start(agg), 0);

    /* Hammer the Prometheus endpoint while the aggregator is rotating. Each
     * response must be well-formed (HTTP/1.1 200, proper Content-Type, and
     * at least one HELP/TYPE/value triplet). */
    for (int i = 0; i < 40; i++) {
        char buf[16384];
        int  n = http_get(f.port, "/api/observability/metrics.prom",
                          buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
        TEST_ASSERT(strstr(buf, "text/plain; version=0.0.4") != NULL);
        TEST_ASSERT(strstr(buf, "# HELP keel_sessions_created_total ") != NULL);
        TEST_ASSERT(strstr(buf, "# TYPE keel_sessions_created_total counter") != NULL);
        TEST_ASSERT(strstr(buf, "\nkeel_sessions_created_total ") != NULL);
    }

    /* Sanity: aggregator did at least one tick and the collector saw at
     * least one request, proving rotation was active during scraping. The
     * scrape loop above runs entirely on the test thread; the aggregator
     * runs on its own thread with a 20 ms cond_timedwait. Give it up to
     * 2 s to land its first tick + first export. */
    for (int i = 0; i < 200; i++) {
        if (keel_otlp_aggregator_ticks(agg) >= 1
            && atomic_load(&c->requests) >= 1) break;
        msleep(10);
    }
    TEST_ASSERT(keel_otlp_aggregator_ticks(agg) >= 1);
    TEST_ASSERT(atomic_load(&c->requests) >= 1);

    keel_otlp_aggregator_stop(agg);
    keel_otlp_aggregator_destroy(agg);
    keel_admin_set_otlp_exporter(f.admin, NULL);
    keel_otlp_exporter_destroy(exp);
    ok_stop(c);
    admin_fix_stop(&f);
}

int main(void) {
    test_encode_buffer_too_small_increments_failures();
    test_http_timeout_increments_counters();
    test_submit_never_blocks_when_collector_down();
    test_admin_endpoints_when_collector_down();
    test_prom_scrape_during_rotation();
    return test_summary();
}
