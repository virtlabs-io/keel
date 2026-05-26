/**
 * @file test_admin_metrics_json.c
 * @brief Verify GET /api/observability/metrics.json (§23.3 JSON Admin Exporter)
 *        emits a grouped snapshot of metrics and, optionally, an embedded
 *        OTLP exporter health block.
 *
 * Runs in both the default (no OTLP) and OTLP-enabled builds. Behavior
 * branches on `KEEL_HAS_OTLP`.
 */

#include "test_utils.h"
#include "keel/core/admin.h"
#include "keel/engine/engine.h"

#ifdef KEEL_HAS_OTLP
#include "keel_otlp_exporter.h"
#endif

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ---- HTTP helpers ------------------------------------------------------- */

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

/* ---- Fixture ------------------------------------------------------------ */

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

/* ---- Tests -------------------------------------------------------------- */

static void test_grouped_metrics_present(void) {
    fix_t f;
    TEST_ASSERT_EQ(fix_start(&f), 0);

    char body[8192];
    int  n = http_get(f.prom_port, "/api/observability/metrics.json",
                      body, sizeof(body));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(body, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(body, "application/json") != NULL);

    /* Snapshot block */
    TEST_ASSERT(strstr(body, "\"snapshot\":") != NULL);
    TEST_ASSERT(strstr(body, "\"time_unix_ns\":") != NULL);
    TEST_ASSERT(strstr(body, "\"uptime_seconds\":") != NULL);
    TEST_ASSERT(strstr(body, "\"workers\":") != NULL);
    TEST_ASSERT(strstr(body, "\"level\":") != NULL);

    /* All metric groups */
    static const char* groups[] = {
        "\"metrics\":",
        "\"sessions\":",  "\"created\":",
        "\"queries\":",   "\"total\":",
        "\"errors\":",    "\"auth\":",
        "\"bytes\":",     "\"recv\":",
        "\"pool\":",      "\"borrows\":",
        "\"reactor\":",   "\"loop_iterations\":",
        "\"multiplex\":", "\"discard_all\":",
        "\"exporter\":",
    };
    for (size_t i = 0; i < sizeof(groups)/sizeof(groups[0]); i++) {
        if (!strstr(body, groups[i])) {
            fprintf(stderr, "missing key: %s\nbody:\n%s\n", groups[i], body);
        }
        TEST_ASSERT(strstr(body, groups[i]) != NULL);
    }

    fix_stop(&f);
}

static void test_exporter_block_reflects_state(void) {
    fix_t f;
    TEST_ASSERT_EQ(fix_start(&f), 0);

    char body[8192];
    int  n = http_get(f.prom_port, "/api/observability/metrics.json",
                      body, sizeof(body));
    TEST_ASSERT(n > 0);

#ifdef KEEL_HAS_OTLP
    /* Compiled but not configured initially. */
    TEST_ASSERT(strstr(body, "\"compiled_in\":true") != NULL);
    TEST_ASSERT(strstr(body, "\"configured\":false") != NULL);

    /* Attach an exporter, then re-query: full self-stats block must appear. */
    keel_otlp_exporter_config_t ecfg = {
        .http = { .endpoint_url = "http://127.0.0.1:1/v1/metrics",
                  .timeout_ms = 50, .bearer_token = NULL },
        .interval_ms      = 100,
        .max_retries      = 0,
        .queue_capacity   = 2,
        .encode_buf_bytes = 4096,
    };
    keel_otlp_exporter_t* exp = keel_otlp_exporter_create(&ecfg);
    TEST_ASSERT_NOT_NULL(exp);
    keel_admin_set_otlp_exporter(f.admin, exp);

    n = http_get(f.prom_port, "/api/observability/metrics.json",
                 body, sizeof(body));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(body, "\"export_attempts_total\":") != NULL);
    TEST_ASSERT(strstr(body, "\"export_queue_capacity\":2") != NULL);
    TEST_ASSERT(strstr(body, "\"last_export_status\":")    != NULL);

    keel_admin_set_otlp_exporter(f.admin, NULL);
    keel_otlp_exporter_destroy(exp);
#else
    TEST_ASSERT(strstr(body, "\"compiled_in\":false") != NULL);
#endif

    fix_stop(&f);
}

int main(void) {
    test_grouped_metrics_present();
    test_exporter_block_reflects_state();
    return test_summary();
}
