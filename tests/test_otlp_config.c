/**
 * @file test_otlp_config.c
 * @brief Verify INI-driven configuration of the OTLP exporter.
 */

#include "test_utils.h"
#include "keel_otlp_config.h"
#include "keel/core/ini.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char* write_ini(const char* contents) {
    char* path = strdup("/tmp/keel_otlp_cfg_XXXXXX.ini");
    int   fd   = mkstemps(path, 4);
    if (fd < 0) { free(path); return NULL; }
    size_t n = strlen(contents);
    if (write(fd, contents, n) != (ssize_t)n) {
        close(fd); unlink(path); free(path); return NULL;
    }
    close(fd);
    return path;
}

static void test_section_missing(void)
{
    char* path = write_ini("[other]\nfoo=bar\n");
    keel_config_t* cfg = keel_config_load(path);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_otlp_exporter_config_t out;
    bool enabled = true;
    TEST_ASSERT_EQ(keel_otlp_config_load(cfg, &out, &enabled), 0);
    TEST_ASSERT(!enabled);
    TEST_ASSERT_NULL((void*)out.http.endpoint_url);
    TEST_ASSERT_EQ((int)out.http.timeout_ms, 5000);
    TEST_ASSERT_EQ((int)out.interval_ms,     5000);
    TEST_ASSERT_EQ((int)out.max_retries,        2);
    TEST_ASSERT_EQ((int)out.queue_capacity,     4);
    TEST_ASSERT_EQ((int)out.encode_buf_bytes, 65536);

    keel_config_free(cfg);
    unlink(path); free(path);
}

static void test_enabled_without_url_stays_off(void)
{
    char* path = write_ini(
        "[observability]\n"
        "otlp_enabled = true\n"
    );
    keel_config_t* cfg = keel_config_load(path);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_otlp_exporter_config_t out;
    bool enabled = true;
    TEST_ASSERT_EQ(keel_otlp_config_load(cfg, &out, &enabled), 0);
    TEST_ASSERT(!enabled);  /* missing endpoint disables it */

    keel_config_free(cfg);
    unlink(path); free(path);
}

static void test_fully_populated(void)
{
    char* path = write_ini(
        "[observability]\n"
        "otlp_enabled         = true\n"
        "otlp_endpoint_url    = http://collector.local:4318/v1/metrics\n"
        "otlp_timeout_ms      = 2500\n"
        "otlp_bearer_token    = secret-token-abc\n"
        "otlp_interval_ms     = 1000\n"
        "otlp_max_retries     = 5\n"
        "otlp_queue_capacity  = 16\n"
        "otlp_encode_buf_bytes= 131072\n"
    );
    keel_config_t* cfg = keel_config_load(path);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_otlp_exporter_config_t out;
    bool enabled = false;
    TEST_ASSERT_EQ(keel_otlp_config_load(cfg, &out, &enabled), 0);
    TEST_ASSERT(enabled);
    TEST_ASSERT_STR_EQ(out.http.endpoint_url,
                       "http://collector.local:4318/v1/metrics");
    TEST_ASSERT_STR_EQ(out.http.bearer_token, "secret-token-abc");
    TEST_ASSERT_EQ((int)out.http.timeout_ms, 2500);
    TEST_ASSERT_EQ((int)out.interval_ms,     1000);
    TEST_ASSERT_EQ((int)out.max_retries,        5);
    TEST_ASSERT_EQ((int)out.queue_capacity,    16);
    TEST_ASSERT_EQ((int)out.encode_buf_bytes, 131072);

    keel_config_free(cfg);
    unlink(path); free(path);
}

static void test_empty_token_stays_null(void)
{
    char* path = write_ini(
        "[observability]\n"
        "otlp_enabled      = true\n"
        "otlp_endpoint_url = http://x/v1/metrics\n"
        "otlp_bearer_token =\n"
    );
    keel_config_t* cfg = keel_config_load(path);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_otlp_exporter_config_t out;
    bool enabled = false;
    TEST_ASSERT_EQ(keel_otlp_config_load(cfg, &out, &enabled), 0);
    TEST_ASSERT(enabled);
    TEST_ASSERT_NULL((void*)out.http.bearer_token);

    keel_config_free(cfg);
    unlink(path); free(path);
}

static void test_invalid_args(void)
{
    keel_otlp_exporter_config_t out;
    bool enabled = false;
    TEST_ASSERT_EQ(keel_otlp_config_load(NULL, &out, &enabled), -1);
}

int main(void)
{
    test_section_missing();
    test_enabled_without_url_stays_off();
    test_fully_populated();
    test_empty_token_stays_null();
    test_invalid_args();
    return test_summary();
}
