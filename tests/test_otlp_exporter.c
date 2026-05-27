/**
 * @file test_otlp_exporter.c
 * @brief End-to-end exporter loop: submit snapshots → mock collector →
 *        verify self-stats reflect attempts/successes/failures and that
 *        retries kick in on SERVER_RETRY responses.
 */

#include "test_utils.h"
#include "keel_otlp_exporter.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------------- */
/* Mock collector (re-uses pattern from test_otlp_http.c)                     */
/* ------------------------------------------------------------------------- */

typedef struct mock_collector {
    int           listen_fd;
    uint16_t      port;
    int           status_first;   /**< status returned for request #1..fail_count */
    int           status_after;   /**< status after fail_count requests */
    int           fail_count;
    pthread_t     thread;
    atomic_bool   stop;
    atomic_int    requests;
    atomic_size_t total_body_bytes;
} mock_collector_t;

static void* collector_thread(void* arg)
{
    mock_collector_t* s = arg;
    while (!atomic_load(&s->stop)) {
        struct pollfd p = { .fd = s->listen_fd, .events = POLLIN };
        if (poll(&p, 1, 100) <= 0)
            continue;
        int cfd = accept(s->listen_fd, NULL, NULL);
        if (cfd < 0)
            continue;
        char   buf[16384];
        size_t off = 0;
        size_t cl  = 0;
        size_t hdr_end = 0;
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
        int n = atomic_fetch_add(&s->requests, 1) + 1;
        if (have_hdrs)
            atomic_fetch_add(&s->total_body_bytes, off - hdr_end);
        int status = (n <= s->fail_count) ? s->status_first : s->status_after;
        const char* reason =
            status == 200 ? "OK" :
            status == 503 ? "Service Unavailable" :
            status == 400 ? "Bad Request" : "Status";
        char resp[256];
        int  rl = snprintf(resp, sizeof(resp),
            "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            status, reason);
        (void)send(cfd, resp, (size_t)rl, MSG_NOSIGNAL);
        close(cfd);
    }
    return NULL;
}

static mock_collector_t* collector_start(int status_first, int status_after, int fail_count)
{
    static mock_collector_t s;
    memset(&s, 0, sizeof(s));
    s.status_first = status_first;
    s.status_after = status_after;
    s.fail_count   = fail_count;

    s.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s.listen_fd >= 0);
    int one = 1;
    setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in a = {0};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s.listen_fd, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    TEST_ASSERT(getsockname(s.listen_fd, (struct sockaddr*)&a, &al) == 0);
    s.port = ntohs(a.sin_port);
    TEST_ASSERT(listen(s.listen_fd, 8) == 0);
    TEST_ASSERT(pthread_create(&s.thread, NULL, collector_thread, &s) == 0);
    return &s;
}

static void collector_stop(mock_collector_t* s)
{
    atomic_store(&s->stop, true);
    pthread_join(s->thread, NULL);
    close(s->listen_fd);
}

/* ------------------------------------------------------------------------- */

static keel_otlp_exporter_t* make_exporter(uint16_t port, uint32_t interval_ms,
                                           uint32_t max_retries, uint32_t cap)
{
    static char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", (unsigned)port);
    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = url, .timeout_ms = 1000 },
        .interval_ms     = interval_ms,
        .max_retries     = max_retries,
        .queue_capacity  = cap,
        .encode_buf_bytes = 8192,
    };
    return keel_otlp_exporter_create(&cfg);
}

static keel_otlp_snapshot_t snap_with(uint64_t v)
{
    keel_otlp_snapshot_t s = {0};
    s.start_time_unix_nano = 1;
    s.time_unix_nano       = v + 1;
    s.metric_count         = 1;
    snprintf(s.metrics[0].name, sizeof(s.metrics[0].name), "v_%llu",
             (unsigned long long)v);
    s.metrics[0].value = v;
    return s;
}

static void msleep(uint32_t ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------------------- */

static void test_lifecycle_no_submit(void)
{
    /* Exporter starts, ticks at least once, stops cleanly with no work. */
    keel_otlp_exporter_t* e = make_exporter(1, 50, 0, 4);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);
    msleep(60);
    keel_otlp_exporter_stop(e);
    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(e, &st);
    TEST_ASSERT_EQ((int)st.attempts, 0);
    keel_otlp_exporter_destroy(e);
}

static void test_submit_success(void)
{
    mock_collector_t* c = collector_start(200, 200, 0);
    keel_otlp_exporter_t* e = make_exporter(c->port, 100, 0, 4);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    for (uint64_t i = 0; i < 3; ++i) {
        keel_otlp_snapshot_t s = snap_with(i);
        keel_otlp_exporter_submit(e, &s);
    }
    /* Wait for the collector to see 3 requests. */
    for (int i = 0; i < 100 && atomic_load(&c->requests) < 3; ++i)
        msleep(10);

    keel_otlp_exporter_stop(e);
    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(e, &st);
    TEST_ASSERT_EQ((int)st.attempts, 3);
    TEST_ASSERT_EQ((int)st.successes, 3);
    TEST_ASSERT_EQ((int)st.failures, 0);
    TEST_ASSERT_EQ((int)st.queue_capacity, 4);
    TEST_ASSERT(st.last_success_unix_ms > 0);
    TEST_ASSERT(st.last_duration_ns > 0);
    TEST_ASSERT(atomic_load(&c->total_body_bytes) > 0);

    keel_otlp_exporter_destroy(e);
    collector_stop(c);
}

static void test_retry_on_503(void)
{
    /* First 2 requests return 503 (SERVER_RETRY), 3rd returns 200.
     * With max_retries=2 and one submit, exporter retries twice then succeeds.
     * Collector therefore sees exactly 3 HTTP requests for 1 snapshot. */
    mock_collector_t* c = collector_start(503, 200, 2);
    keel_otlp_exporter_t* e = make_exporter(c->port, 100, /*max_retries=*/2, 4);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    keel_otlp_snapshot_t s = snap_with(42);
    keel_otlp_exporter_submit(e, &s);

    for (int i = 0; i < 100 && atomic_load(&c->requests) < 3; ++i)
        msleep(10);

    keel_otlp_exporter_stop(e);
    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(e, &st);
    TEST_ASSERT_EQ((int)atomic_load(&c->requests), 3);
    TEST_ASSERT_EQ((int)st.attempts, 1);
    TEST_ASSERT_EQ((int)st.successes, 1);
    TEST_ASSERT_EQ((int)st.failures, 0);

    keel_otlp_exporter_destroy(e);
    collector_stop(c);
}

static void test_failure_400(void)
{
    /* 400 = SERVER_REJECT (no retry). */
    mock_collector_t* c = collector_start(400, 400, 0);
    keel_otlp_exporter_t* e = make_exporter(c->port, 100, /*max_retries=*/3, 4);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    keel_otlp_snapshot_t s = snap_with(7);
    keel_otlp_exporter_submit(e, &s);

    for (int i = 0; i < 100 && atomic_load(&c->requests) < 1; ++i)
        msleep(10);

    keel_otlp_exporter_stop(e);
    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(e, &st);
    TEST_ASSERT_EQ((int)atomic_load(&c->requests), 1);
    TEST_ASSERT_EQ((int)st.attempts,  1);
    TEST_ASSERT_EQ((int)st.successes, 0);
    TEST_ASSERT_EQ((int)st.failures,  1);
    TEST_ASSERT(st.last_failure_unix_ms > 0);

    keel_otlp_exporter_destroy(e);
    collector_stop(c);
}

static void test_drop_oldest_reported(void)
{
    /* Tiny queue + slow consumer (no collector at all => attempts stall on
     * CONNECT_FAILED). Submit > capacity items quickly. drop counter must
     * surface through self-stats. */
    keel_otlp_exporter_t* e = make_exporter(/*dead port*/ 1, 100, 0, /*cap=*/2);
    TEST_ASSERT_NOT_NULL(e);
    /* Don't even start the thread so submits go straight into the queue
     * and we exercise pure drop-oldest accounting. */
    for (uint64_t i = 0; i < 5; ++i) {
        keel_otlp_snapshot_t s = snap_with(i);
        keel_otlp_exporter_submit(e, &s);
    }
    /* The thread isn't running, so queue_depth/dropped in stats are
     * still zero (the loop never ran). Verify via the public submit
     * return aggregate instead: 5 pushes into cap=2 → 3 drops. */
    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(e, &st);
    TEST_ASSERT_EQ((int)st.dropped, 0);  /* loop hasn't published yet */

    /* Now start the thread; first pop will publish queue_depth + dropped. */
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);
    for (int i = 0; i < 100; ++i) {
        keel_otlp_exporter_self_stats(e, &st);
        if (st.dropped >= 3 && st.attempts >= 2)
            break;
        msleep(20);
    }
    TEST_ASSERT(st.dropped >= 3);
    TEST_ASSERT(st.attempts >= 1);
    /* Every attempt should be a failure since no collector is listening. */
    TEST_ASSERT_EQ((int)st.successes, 0);

    keel_otlp_exporter_destroy(e);
}

int main(void) {
    test_lifecycle_no_submit();
    test_submit_success();
    test_retry_on_503();
    test_failure_400();
    test_drop_oldest_reported();
    return test_summary();
}
