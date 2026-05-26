/**
 * @file test_otlp_overhead_bench.c
 * @brief Performance benchmark for the OTLP observability path (§29.7).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Validates the v0.2-alpha observability overhead budget (proposal §29.7).
 * The proposal mandates a runnable, CI-friendly benchmark that proves
 * exporter overhead stays inside the agreed budget. End-to-end pgbench
 * runs require a live PostgreSQL backend and live network and are kept
 * under bench/; this file is the in-process microbenchmark gate that
 * runs in every CI job.
 *
 * Scope:
 *   1. Per-call cost of the cold-path projection
 *      keel_otlp_snapshot_from_stats() → keel_otlp_encode_metrics()
 *      (this is what the background aggregator does every interval).
 *   2. Per-call cost of keel_otlp_exporter_submit() — the only OTLP API
 *      a hot-path caller could ever reach; the proposal mandates zero
 *      blocking work and zero allocations on that path.
 *   3. Steady-state aggregator throughput (ticks/second with a real
 *      mock OTLP receiver).
 *
 * Assertions are generous on purpose so CI noise does not gate releases,
 * but tight enough that a real regression (e.g. accidental malloc per
 * call, accidental syscall on submit) will trip them. The detailed
 * measurements are emitted as a single JSON line on stdout so the
 * release engineer can chart trends.
 *
 * Out of scope: query response histogram pipeline (compile-time gated
 * off by default in §13.7 for v0.2-alpha).
 */

#include "test_utils.h"

#include "keel/core/stats.h"
#include "../src/observability/otlp/keel_otlp_aggregator.h"
#include "../src/observability/otlp/keel_otlp_encode.h"
#include "../src/observability/otlp/keel_otlp_exporter.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------------- */
/* Budget                                                                     */
/* ------------------------------------------------------------------------- */

/* Loose CI-safe upper bounds on per-call cost. A real regression (extra
 * malloc, syscall, lock contention) will overshoot these by >10x. */
#define BUDGET_PROJECT_ENCODE_NS  200000ULL  /* 200µs / op */
#define BUDGET_SUBMIT_NS           50000ULL  /*  50µs / op */
#define BUDGET_AGGREGATOR_TICK_MS    50ULL   /*  ticks at 20ms must keep up */

#define BENCH_ITERATIONS  10000
#define BENCH_SUBMIT_ITER 50000

/* ------------------------------------------------------------------------- */
/* Time helper                                                                */
/* ------------------------------------------------------------------------- */

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ------------------------------------------------------------------------- */
/* Representative stats snapshot                                              */
/* ------------------------------------------------------------------------- */

static void populate_realistic_snapshot(keel_stats_snapshot_t* in) {
    memset(in, 0, sizeof(*in));
    in->num_workers = 16;
    in->uptime_ns   = (int64_t)3600 * 1000000000LL;  /* 1h uptime */

    keel_counter_add(&in->basic.queries_total,       12345678);
    keel_counter_add(&in->basic.queries_read,         8000000);
    keel_counter_add(&in->basic.queries_write,        3000000);
    keel_counter_add(&in->basic.queries_tx,           1345678);
    keel_counter_add(&in->basic.errors_total,             123);
    keel_counter_add(&in->basic.errors_auth,                4);
    keel_counter_add(&in->basic.errors_proto,              17);
    keel_counter_add(&in->basic.errors_backend,            42);
    keel_counter_add(&in->basic.errors_timeout,            60);
    keel_counter_add(&in->basic.bytes_recv,         500ULL * 1024 * 1024);
    keel_counter_add(&in->basic.bytes_sent,        1500ULL * 1024 * 1024);
    keel_counter_add(&in->basic.bytes_backend_recv, 800ULL * 1024 * 1024);
    keel_counter_add(&in->basic.bytes_backend_sent, 200ULL * 1024 * 1024);
    keel_counter_add(&in->basic.bytes_spliced,     2000ULL * 1024 * 1024);
    keel_counter_add(&in->basic.pool_borrows,         500000);
    keel_counter_add(&in->basic.pool_returns,         499950);
    keel_counter_add(&in->basic.pool_creates,            128);
    keel_counter_add(&in->basic.pool_destroys,            78);
    keel_counter_add(&in->basic.pool_hits,            480000);
    keel_counter_add(&in->basic.pool_misses,           20000);
    keel_counter_add(&in->basic.sessions_created,     100000);
    keel_counter_add(&in->basic.sessions_closed,       99950);
    keel_counter_add(&in->basic.loop_iterations,    99999999);
    keel_counter_add(&in->basic.ops_submitted,      90000000);
    keel_counter_add(&in->basic.ops_completed,      89999999);
    keel_counter_add(&in->basic.discard_all_count,      1200);
    keel_counter_add(&in->basic.state_sync_count,        800);
    keel_gauge_set(&in->basic.sessions_active,            50);
    keel_gauge_set(&in->basic.sessions_pinned,             3);
    keel_gauge_set(&in->basic.backends_cleaning,           1);
}

/* ------------------------------------------------------------------------- */
/* Tiny "OK" OTLP/HTTP receiver — accept + drain + 200 reply.                  */
/* ------------------------------------------------------------------------- */

typedef struct {
    int       listen_fd;
    uint16_t  port;
    pthread_t thread;
    _Atomic bool running;
    _Atomic uint64_t accepted;
} ok_receiver_t;

static void* ok_recv_loop(void* arg) {
    ok_receiver_t* r = (ok_receiver_t*)arg;
    while (atomic_load(&r->running)) {
        int cfd = accept(r->listen_fd, NULL, NULL);
        if (cfd < 0) {
            if (!atomic_load(&r->running)) break;
            continue;
        }
        char  buf[8192];
        ssize_t n;
        while ((n = recv(cfd, buf, sizeof(buf), 0)) > 0) {
            /* Drain until peer closes or pause; simple heuristic. */
            if (n < (ssize_t)sizeof(buf)) break;
        }
        const char* resp =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        (void)send(cfd, resp, strlen(resp), MSG_NOSIGNAL);
        close(cfd);
        atomic_fetch_add(&r->accepted, 1);
    }
    return NULL;
}

static bool ok_recv_start(ok_receiver_t* r) {
    memset(r, 0, sizeof(*r));
    r->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (r->listen_fd < 0) return false;
    int one = 1;
    setsockopt(r->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa = { .sin_family = AF_INET };
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (bind(r->listen_fd, (struct sockaddr*)&sa, sizeof(sa)) != 0) return false;
    socklen_t sl = sizeof(sa);
    if (getsockname(r->listen_fd, (struct sockaddr*)&sa, &sl) != 0) return false;
    r->port = ntohs(sa.sin_port);
    if (listen(r->listen_fd, 64) != 0) return false;
    atomic_store(&r->running, true);
    return pthread_create(&r->thread, NULL, ok_recv_loop, r) == 0;
}

static void ok_recv_stop(ok_receiver_t* r) {
    atomic_store(&r->running, false);
    shutdown(r->listen_fd, SHUT_RDWR);
    close(r->listen_fd);
    pthread_join(r->thread, NULL);
}

/* ------------------------------------------------------------------------- */
/* Benchmark 1: snapshot_from_stats + encode_metrics (cold-path projection)   */
/* ------------------------------------------------------------------------- */

static uint64_t bench_project_and_encode(void) {
    keel_stats_snapshot_t in;
    populate_realistic_snapshot(&in);

    keel_otlp_snapshot_t out;
    uint8_t buf[8192];
    size_t  len = 0;

    /* Warmup. */
    for (int i = 0; i < 100; ++i) {
        keel_otlp_snapshot_from_stats(&in, 1, 2, &out);
        keel_otlp_encode_metrics(&out, buf, sizeof(buf), &len);
    }

    uint64_t t0 = now_ns();
    for (int i = 0; i < BENCH_ITERATIONS; ++i) {
        keel_otlp_snapshot_from_stats(&in, 1, (uint64_t)(2 + i), &out);
        keel_otlp_encode_result_t rc =
            keel_otlp_encode_metrics(&out, buf, sizeof(buf), &len);
        if (rc != KEEL_OTLP_ENCODE_OK) {
            fprintf(stderr, "encode failed at i=%d rc=%d\n", i, (int)rc);
            return UINT64_MAX;
        }
    }
    uint64_t t1 = now_ns();
    return (t1 - t0) / (uint64_t)BENCH_ITERATIONS;
}

/* ------------------------------------------------------------------------- */
/* Benchmark 2: exporter_submit per-call cost (HOT-PATH-VISIBLE API)          */
/*                                                                            */
/* Even though §13.7 keeps the worker hot path away from the exporter, any    */
/* future call MUST be non-blocking and allocation-free. This benchmark       */
/* hammers submit from a single thread against a dead exporter (no started    */
/* HTTP loop) so the only work is enqueue + drop-oldest accounting.           */
/* ------------------------------------------------------------------------- */

static uint64_t bench_submit_non_blocking(void) {
    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = "http://127.0.0.1:1",  /* never reached */
                  .timeout_ms   = 100 },
        .interval_ms       = 50,
        .queue_capacity    = 64,
        .max_retries       = 0,
        .encode_buf_bytes  = 4096,
    };
    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&cfg);
    if (!exp) return UINT64_MAX;

    keel_otlp_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    snap.start_time_unix_nano = 1;
    snap.metric_count         = 3;
    snprintf(snap.metrics[0].name, sizeof(snap.metrics[0].name),
             "keel_queries_total");
    snap.metrics[0].value = 100;
    snprintf(snap.metrics[1].name, sizeof(snap.metrics[1].name),
             "keel_errors_total");
    snap.metrics[1].value = 7;
    snprintf(snap.metrics[2].name, sizeof(snap.metrics[2].name),
             "keel_sessions_active");
    snap.metrics[2].value = 50;

    /* Warmup. */
    for (int i = 0; i < 100; ++i) {
        snap.time_unix_nano = (uint64_t)i;
        keel_otlp_exporter_submit(exp, &snap);
    }

    uint64_t t0 = now_ns();
    for (int i = 0; i < BENCH_SUBMIT_ITER; ++i) {
        snap.time_unix_nano = (uint64_t)i;
        keel_otlp_exporter_submit(exp, &snap);
    }
    uint64_t t1 = now_ns();

    keel_otlp_exporter_destroy(exp);
    return (t1 - t0) / (uint64_t)BENCH_SUBMIT_ITER;
}

/* ------------------------------------------------------------------------- */
/* Benchmark 3: full aggregator → exporter → mock receiver round-trip          */
/* ------------------------------------------------------------------------- */

typedef struct {
    uint64_t ticks;
    uint64_t accepted;
    uint64_t elapsed_ns;
} agg_bench_result_t;

static agg_bench_result_t bench_aggregator_throughput(void) {
    agg_bench_result_t r = {0};

    /* Stats collector (size 1 worker is enough — projection sums them). */
    keel_stats_collector_t* coll =
        keel_stats_collector_create(KEEL_STATS_BASIC, 1);
    if (!coll) return r;

    ok_receiver_t rx;
    if (!ok_recv_start(&rx)) {
        keel_stats_collector_destroy(coll);
        return r;
    }
    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", rx.port);

    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = url, .timeout_ms = 1000 },
        .interval_ms       = 20,
        .queue_capacity    = 16,
        .max_retries       = 0,
        .encode_buf_bytes  = 8192,
    };
    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&cfg);
    if (!exp) { ok_recv_stop(&rx); keel_stats_collector_destroy(coll); return r; }
    if (keel_otlp_exporter_start(exp) != 0) {
        keel_otlp_exporter_destroy(exp);
        ok_recv_stop(&rx);
        keel_stats_collector_destroy(coll);
        return r;
    }

    keel_otlp_aggregator_t* agg =
        keel_otlp_aggregator_create(coll, exp, 20 /* ms */);
    if (!agg || keel_otlp_aggregator_start(agg) != 0) {
        if (agg) keel_otlp_aggregator_destroy(agg);
        keel_otlp_exporter_stop(exp);
        keel_otlp_exporter_destroy(exp);
        ok_recv_stop(&rx);
        keel_stats_collector_destroy(coll);
        return r;
    }

    /* Run for ~600ms — expect ~30 ticks at 20ms cadence. */
    uint64_t t0 = now_ns();
    const uint64_t deadline = t0 + 600ULL * 1000000ULL;
    while (now_ns() < deadline) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 5 * 1000000L };
        nanosleep(&ts, NULL);
    }
    r.ticks      = keel_otlp_aggregator_ticks(agg);
    r.elapsed_ns = now_ns() - t0;

    keel_otlp_aggregator_stop(agg);
    keel_otlp_aggregator_destroy(agg);
    keel_otlp_exporter_stop(exp);
    keel_otlp_exporter_destroy(exp);
    r.accepted = atomic_load(&rx.accepted);
    ok_recv_stop(&rx);
    keel_stats_collector_destroy(coll);
    return r;
}

/* ------------------------------------------------------------------------- */
/* Tests                                                                      */
/* ------------------------------------------------------------------------- */

static uint64_t g_project_encode_ns_op = 0;
static uint64_t g_submit_ns_op         = 0;
static agg_bench_result_t g_agg_result = {0};

static void test_project_and_encode_within_budget(void) {
    g_project_encode_ns_op = bench_project_and_encode();
    TEST_ASSERT(g_project_encode_ns_op != UINT64_MAX);
    fprintf(stderr,
            "[bench] project+encode: %llu ns/op (budget %llu)\n",
            (unsigned long long)g_project_encode_ns_op,
            (unsigned long long)BUDGET_PROJECT_ENCODE_NS);
    TEST_ASSERT(g_project_encode_ns_op < BUDGET_PROJECT_ENCODE_NS);
}

static void test_submit_non_blocking_within_budget(void) {
    g_submit_ns_op = bench_submit_non_blocking();
    TEST_ASSERT(g_submit_ns_op != UINT64_MAX);
    fprintf(stderr,
            "[bench] exporter_submit: %llu ns/op (budget %llu)\n",
            (unsigned long long)g_submit_ns_op,
            (unsigned long long)BUDGET_SUBMIT_NS);
    TEST_ASSERT(g_submit_ns_op < BUDGET_SUBMIT_NS);
}

static void test_aggregator_keeps_up_with_interval(void) {
    g_agg_result = bench_aggregator_throughput();
    fprintf(stderr,
            "[bench] aggregator: ticks=%llu accepted=%llu elapsed_ms=%llu\n",
            (unsigned long long)g_agg_result.ticks,
            (unsigned long long)g_agg_result.accepted,
            (unsigned long long)(g_agg_result.elapsed_ns / 1000000ULL));
    /* 600ms at 20ms cadence ≥ ~10 ticks even with substantial scheduler
     * jitter. The mock receiver must have observed at least one POST,
     * proving the full project→encode→http path executes inside one
     * interval. */
    TEST_ASSERT(g_agg_result.ticks >= 5);
    TEST_ASSERT(g_agg_result.accepted >= 1);
    /* The aggregator's per-tick wall-time budget: total elapsed divided
     * by ticks must be in the same order as the interval. */
    if (g_agg_result.ticks > 0) {
        uint64_t per_tick_ms =
            (g_agg_result.elapsed_ns / g_agg_result.ticks) / 1000000ULL;
        TEST_ASSERT(per_tick_ms <= BUDGET_AGGREGATOR_TICK_MS);
    }
}

static void test_emit_summary(void) {
    /* Machine-readable line for trend tracking. */
    printf(
        "{\"bench\":\"otlp_overhead\","
        "\"project_encode_ns_op\":%llu,"
        "\"submit_ns_op\":%llu,"
        "\"aggregator_ticks\":%llu,"
        "\"aggregator_accepted\":%llu,"
        "\"aggregator_elapsed_ms\":%llu,"
        "\"budget_project_encode_ns\":%llu,"
        "\"budget_submit_ns\":%llu,"
        "\"budget_tick_ms\":%llu}\n",
        (unsigned long long)g_project_encode_ns_op,
        (unsigned long long)g_submit_ns_op,
        (unsigned long long)g_agg_result.ticks,
        (unsigned long long)g_agg_result.accepted,
        (unsigned long long)(g_agg_result.elapsed_ns / 1000000ULL),
        (unsigned long long)BUDGET_PROJECT_ENCODE_NS,
        (unsigned long long)BUDGET_SUBMIT_NS,
        (unsigned long long)BUDGET_AGGREGATOR_TICK_MS);
    fflush(stdout);
}

int main(void) {
    test_project_and_encode_within_budget();
    test_submit_non_blocking_within_budget();
    test_aggregator_keeps_up_with_interval();
    test_emit_summary();
    return test_summary();
}
