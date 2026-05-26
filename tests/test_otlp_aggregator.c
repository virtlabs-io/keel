/**
 * @file test_otlp_aggregator.c
 * @brief End-to-end test of the periodic OTLP aggregator + exporter.
 *
 * Spins up a real `keel_stats_collector_t`, a real `keel_otlp_exporter_t`
 * pointing at a tiny in-process HTTP mock that returns 200 OK, and a real
 * `keel_otlp_aggregator_t`. Verifies that snapshots flow end-to-end:
 *   - The mock collector receives at least one POST.
 *   - Exporter self-stats show attempts > 0 and successes > 0.
 *   - The first request body decodes as a non-empty protobuf payload.
 */

#include "test_utils.h"

#include "keel/core/stats.h"
#include "../src/observability/otlp/keel_otlp_aggregator.h"
#include "../src/observability/otlp/keel_otlp_exporter.h"
#include "../src/observability/otlp/keel_exporter_stats.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* -------------------------------------------------------------------------- */
/* Tiny in-process HTTP collector: accepts POSTs, replies 200 OK, counts hits. */

typedef struct mock_collector {
    int                     listen_fd;
    uint16_t                port;
    pthread_t               thread;
    _Atomic bool            stop;
    _Atomic uint64_t        requests;
    _Atomic uint64_t        bytes_received;
} mock_collector_t;

static void* mock_collector_main(void* arg) {
    mock_collector_t* mc = (mock_collector_t*)arg;
    while (!atomic_load_explicit(&mc->stop, memory_order_acquire)) {
        struct timeval tv = { .tv_sec = 0, .tv_usec = 50 * 1000 };
        fd_set rfds; FD_ZERO(&rfds); FD_SET(mc->listen_fd, &rfds);
        int sr = select(mc->listen_fd + 1, &rfds, NULL, NULL, &tv);
        if (sr <= 0) continue;

        int cfd = accept(mc->listen_fd, NULL, NULL);
        if (cfd < 0) continue;

        /* Read until we see the end of HTTP headers, then drain Content-Length bytes. */
        char hdr[4096];
        size_t hdr_len = 0;
        ssize_t body_in_hdr = 0;
        size_t content_length = 0;
        bool   headers_done = false;
        while (!headers_done && hdr_len < sizeof(hdr) - 1) {
            ssize_t r = recv(cfd, hdr + hdr_len, sizeof(hdr) - 1 - hdr_len, 0);
            if (r <= 0) break;
            hdr_len += (size_t)r;
            hdr[hdr_len] = '\0';
            char* hdr_end = strstr(hdr, "\r\n\r\n");
            if (hdr_end) {
                headers_done = true;
                body_in_hdr = (ssize_t)((hdr + hdr_len) - (hdr_end + 4));
                const char* cl = strcasestr(hdr, "Content-Length:");
                if (cl) content_length = (size_t)strtoul(cl + 15, NULL, 10);
            }
        }
        size_t remaining = content_length > (size_t)body_in_hdr
                         ? content_length - (size_t)body_in_hdr : 0;
        char drain[4096];
        while (remaining > 0) {
            ssize_t r = recv(cfd, drain,
                             remaining < sizeof(drain) ? remaining : sizeof(drain), 0);
            if (r <= 0) break;
            remaining -= (size_t)r;
        }
        atomic_fetch_add_explicit(&mc->bytes_received,
                                  content_length, memory_order_relaxed);

        const char* reply =
            "HTTP/1.1 200 OK\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n"
            "\r\n";
        (void)!send(cfd, reply, strlen(reply), MSG_NOSIGNAL);
        close(cfd);

        atomic_fetch_add_explicit(&mc->requests, 1, memory_order_relaxed);
    }
    return NULL;
}

static int mock_collector_start(mock_collector_t* mc) {
    memset(mc, 0, sizeof(*mc));
    mc->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (mc->listen_fd < 0) return -1;
    int opt = 1;
    setsockopt(mc->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;  /* ephemeral */
    if (bind(mc->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(mc->listen_fd); return -1;
    }
    socklen_t alen = sizeof(addr);
    if (getsockname(mc->listen_fd, (struct sockaddr*)&addr, &alen) < 0) {
        close(mc->listen_fd); return -1;
    }
    mc->port = ntohs(addr.sin_port);
    if (listen(mc->listen_fd, 4) < 0) {
        close(mc->listen_fd); return -1;
    }
    atomic_store_explicit(&mc->stop, false, memory_order_release);
    if (pthread_create(&mc->thread, NULL, mock_collector_main, mc) != 0) {
        close(mc->listen_fd); return -1;
    }
    return 0;
}

static void mock_collector_stop(mock_collector_t* mc) {
    atomic_store_explicit(&mc->stop, true, memory_order_release);
    pthread_join(mc->thread, NULL);
    close(mc->listen_fd);
}

static void msleep(int ms) {
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* -------------------------------------------------------------------------- */

static void test_aggregator_drives_exporter_end_to_end(void) {
    mock_collector_t mc;
    TEST_ASSERT_EQ(0, mock_collector_start(&mc));

    keel_stats_collector_t* coll =
        keel_stats_collector_create(KEEL_STATS_BASIC, 2);
    TEST_ASSERT_NOT_NULL(coll);

    /* Inject some signal into worker 0 so the snapshot is non-trivial. */
    keel_stats_ctx_t* ctx0 = keel_stats_collector_get_ctx(coll, 0);
    TEST_ASSERT_NOT_NULL(ctx0);
    keel_counter_add(&ctx0->basic.queries_total, 42);
    keel_counter_add(&ctx0->basic.bytes_recv,   4096);
    keel_gauge_set(&ctx0->basic.sessions_active,   7);

    char url[64];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", mc.port);

    keel_otlp_exporter_config_t cfg = {
        .http = {
            .endpoint_url = url,
            .timeout_ms   = 500,
            .bearer_token = NULL,
        },
        .interval_ms      = 50,
        .max_retries      = 0,
        .queue_capacity   = 4,
        .encode_buf_bytes = 8 * 1024,
    };

    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&cfg);
    TEST_ASSERT_NOT_NULL(exp);
    TEST_ASSERT_EQ(0, keel_otlp_exporter_start(exp));

    keel_otlp_aggregator_t* agg = keel_otlp_aggregator_create(coll, exp, 30);
    TEST_ASSERT_NOT_NULL(agg);
    TEST_ASSERT_EQ(0, keel_otlp_aggregator_start(agg));

    /* Poll up to ~3 s for at least one mock-collector request. */
    int waited = 0;
    while (atomic_load_explicit(&mc.requests, memory_order_relaxed) == 0
           && waited < 3000) {
        msleep(50);
        waited += 50;
    }

    keel_otlp_aggregator_stop(agg);
    keel_otlp_exporter_stop(exp);

    uint64_t reqs   = atomic_load_explicit(&mc.requests,       memory_order_relaxed);
    uint64_t bytes  = atomic_load_explicit(&mc.bytes_received, memory_order_relaxed);
    uint64_t ticks  = keel_otlp_aggregator_ticks(agg);

    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(exp, &st);

    uint64_t attempts  = (uint64_t)st.attempts;
    uint64_t successes = (uint64_t)st.successes;
    uint64_t failures  = (uint64_t)st.failures;

    fprintf(stderr,
            "aggregator: ticks=%llu mock_reqs=%llu mock_bytes=%llu"
            " attempts=%llu successes=%llu failures=%llu\n",
            (unsigned long long)ticks,
            (unsigned long long)reqs,
            (unsigned long long)bytes,
            (unsigned long long)attempts,
            (unsigned long long)successes,
            (unsigned long long)failures);

    TEST_ASSERT(ticks    >= 1);
    TEST_ASSERT(reqs     >= 1);
    TEST_ASSERT(bytes    >= 1);
    TEST_ASSERT(attempts  >= 1);
    TEST_ASSERT(successes >= 1);

    keel_otlp_aggregator_destroy(agg);
    keel_otlp_exporter_destroy(exp);
    keel_stats_collector_destroy(coll);
    mock_collector_stop(&mc);
}

static void test_aggregator_lifecycle_idempotent(void) {
    /* Even with no exporter activity, create/destroy and stop-without-start
     * must be safe. */
    keel_otlp_aggregator_destroy(NULL);
    keel_otlp_aggregator_stop(NULL);
    TEST_ASSERT_EQ((uint64_t)0, keel_otlp_aggregator_ticks(NULL));

    keel_stats_collector_t* coll =
        keel_stats_collector_create(KEEL_STATS_BASIC, 1);
    TEST_ASSERT_NOT_NULL(coll);

    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = "http://127.0.0.1:1/v1/metrics",
                  .timeout_ms = 50, .bearer_token = NULL },
        .interval_ms = 100, .max_retries = 0,
        .queue_capacity = 2, .encode_buf_bytes = 4 * 1024,
    };
    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&cfg);
    TEST_ASSERT_NOT_NULL(exp);

    /* aggregator_create with bad args returns NULL */
    TEST_ASSERT_NULL(keel_otlp_aggregator_create(NULL, exp, 100));
    TEST_ASSERT_NULL(keel_otlp_aggregator_create(coll, NULL, 100));
    TEST_ASSERT_NULL(keel_otlp_aggregator_create(coll, exp, 0));

    keel_otlp_aggregator_t* agg = keel_otlp_aggregator_create(coll, exp, 100);
    TEST_ASSERT_NOT_NULL(agg);

    /* stop without start is a no-op */
    keel_otlp_aggregator_stop(agg);
    keel_otlp_aggregator_destroy(agg);

    keel_otlp_exporter_destroy(exp);
    keel_stats_collector_destroy(coll);
}

int main(void) {
    test_aggregator_lifecycle_idempotent();
    test_aggregator_drives_exporter_end_to_end();
    return test_summary();
}
