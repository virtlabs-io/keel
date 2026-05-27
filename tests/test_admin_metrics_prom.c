/**
 * @file test_admin_metrics_prom.c
 * @brief Verify GET /api/observability/metrics.prom serves the curated
 *        OTLP-aligned Prometheus exposition (§23.2).
 */

#include "test_utils.h"
#include "keel/core/admin.h"
#include "keel/engine/engine.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int tcp_connect(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port   = htons(port),
    };
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        close(fd); return -1;
    }
    return fd;
}

static int http_get(uint16_t port, const char* path, char* out, size_t out_cap) {
    int fd = tcp_connect(port);
    if (fd < 0) return -1;
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

typedef struct {
    keel_engine_t* engine;
    keel_admin_t*  admin;
    uint16_t       prom_port;
} fix_t;

static int fix_start(fix_t* f) {
    memset(f, 0, sizeof(*f));
    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;
    f->engine = keel_engine_create(&ecfg);
    if (!f->engine) return -1;

    keel_admin_config_t acfg = KEEL_ADMIN_CONFIG_DEFAULT;
    acfg.admin_enabled = true;
    acfg.admin_addr    = "127.0.0.1";
    acfg.admin_port    = 0;
    acfg.prom_enabled  = true;
    acfg.prom_addr     = "127.0.0.1";
    acfg.prom_port     = 0;

    f->admin = keel_admin_start(&acfg, f->engine);
    if (!f->admin) { keel_engine_destroy(f->engine); return -1; }
    f->prom_port = keel_admin_get_prom_port(f->admin);
    return f->prom_port > 0 ? 0 : -1;
}

static void fix_stop(fix_t* f) {
    keel_admin_stop(f->admin);
    keel_engine_destroy(f->engine);
}

static void test_serves_curated_prom(void) {
    fix_t f;
    TEST_ASSERT_EQ(fix_start(&f), 0);

    char body[16384];
    int  n = http_get(f.prom_port, "/api/observability/metrics.prom",
                      body, sizeof(body));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(body, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(body, "text/plain; version=0.0.4") != NULL);

    /* Required HELP + TYPE + value triplets for representative metrics. */
    static const char* expected[] = {
        "# HELP keel_sessions_created_total ",
        "# TYPE keel_sessions_created_total counter",
        "keel_sessions_created_total ",
        "# TYPE keel_sessions_active gauge",
        "# TYPE keel_queries_total counter",
        "# TYPE keel_pool_borrows_total counter",
        "# TYPE keel_loop_iterations_total counter",
        "# TYPE keel_backends_cleaning gauge",
        "# TYPE keel_uptime_seconds gauge",
        "# TYPE keel_workers gauge",
    };
    for (size_t i = 0; i < sizeof(expected)/sizeof(expected[0]); i++) {
        if (!strstr(body, expected[i])) {
            fprintf(stderr, "missing: %s\nbody:\n%s\n", expected[i], body);
        }
        TEST_ASSERT(strstr(body, expected[i]) != NULL);
    }

    fix_stop(&f);
}

int main(void) {
    test_serves_curated_prom();
    return test_summary();
}
