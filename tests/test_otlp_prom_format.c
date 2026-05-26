/**
 * @file test_otlp_prom_format.c
 * @brief Unit tests for the OTLP snapshot → Prometheus text formatter
 *        (§23.2 — keel_prom_format_snapshot).
 */

#include "test_utils.h"
#include "keel_otlp_encode.h"
#include "keel_prom_format.h"

#include <string.h>

static void seed_snapshot(keel_otlp_snapshot_t* s) {
    memset(s, 0, sizeof(*s));
    s->start_time_unix_nano = 1000;
    s->time_unix_nano       = 2000;

#define ADD(name_, val_) do {                                       \
    keel_otlp_metric_sample_t* m = &s->metrics[s->metric_count++];  \
    strncpy(m->name, (name_), sizeof(m->name)-1);                   \
    m->value = (val_);                                              \
} while (0)

    ADD("keel_sessions_created_total", 42);
    ADD("keel_sessions_active",         7);
    ADD("keel_queries_total",        1000);
    ADD("keel_uptime_seconds",        123);
#undef ADD
}

static void test_invalid_args(void) {
    char buf[64];
    keel_otlp_snapshot_t s = {0};
    TEST_ASSERT_EQ(keel_prom_format_snapshot(NULL, buf, sizeof(buf)), -1);
    TEST_ASSERT_EQ(keel_prom_format_snapshot(&s, NULL, sizeof(buf)), -1);
    TEST_ASSERT_EQ(keel_prom_format_snapshot(&s, buf, 0),            -1);
}

static void test_empty_snapshot(void) {
    keel_otlp_snapshot_t s = {0};
    char buf[64];
    int n = keel_prom_format_snapshot(&s, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, 0);
    TEST_ASSERT_STR_EQ(buf, "");
}

static void test_help_type_and_counter_total(void) {
    keel_otlp_snapshot_t s; seed_snapshot(&s);
    char buf[4096];
    int n = keel_prom_format_snapshot(&s, buf, sizeof(buf));
    TEST_ASSERT(n > 0);

    /* Counter: HELP, TYPE counter, value with _total suffix. */
    TEST_ASSERT(strstr(buf, "# HELP keel_sessions_created_total ") != NULL);
    TEST_ASSERT(strstr(buf, "# TYPE keel_sessions_created_total counter") != NULL);
    TEST_ASSERT(strstr(buf, "\nkeel_sessions_created_total 42\n")  != NULL);

    /* Gauge: no _total suffix → classified as gauge. */
    TEST_ASSERT(strstr(buf, "# TYPE keel_sessions_active gauge") != NULL);
    TEST_ASSERT(strstr(buf, "\nkeel_sessions_active 7\n")        != NULL);

    /* Known metric → curated help string (NOT the fallback that repeats the name). */
    const char* help_line = strstr(buf, "# HELP keel_sessions_active ");
    TEST_ASSERT(help_line != NULL);
    /* The fallback would render: "# HELP keel_sessions_active keel_sessions_active".
     * Make sure that's NOT what we got. */
    TEST_ASSERT(strncmp(help_line,
                        "# HELP keel_sessions_active keel_sessions_active",
                        strlen("# HELP keel_sessions_active keel_sessions_active"))
                != 0);

    /* Counter without _total in raw name wouldn't be classified as counter —
     * verify keel_queries_total IS counter. */
    TEST_ASSERT(strstr(buf, "# TYPE keel_queries_total counter") != NULL);

    /* uptime_seconds is a gauge per §23 (no _total). */
    TEST_ASSERT(strstr(buf, "# TYPE keel_uptime_seconds gauge") != NULL);
}

static void test_buffer_too_small(void) {
    keel_otlp_snapshot_t s; seed_snapshot(&s);
    char tiny[32];
    int n = keel_prom_format_snapshot(&s, tiny, sizeof(tiny));
    TEST_ASSERT_EQ(n, -2);
}

static void test_unknown_metric_fallback(void) {
    keel_otlp_snapshot_t s = {0};
    keel_otlp_metric_sample_t* m = &s.metrics[s.metric_count++];
    strncpy(m->name, "keel_custom_thing_total", sizeof(m->name)-1);
    m->value = 99;

    char buf[512];
    int n = keel_prom_format_snapshot(&s, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    /* Unknown name still classified by suffix and gets HELP using its own name. */
    TEST_ASSERT(strstr(buf, "# TYPE keel_custom_thing_total counter") != NULL);
    TEST_ASSERT(strstr(buf, "\nkeel_custom_thing_total 99\n") != NULL);
}

int main(void) {
    test_invalid_args();
    test_empty_snapshot();
    test_help_type_and_counter_total();
    test_buffer_too_small();
    test_unknown_metric_fallback();
    return test_summary();
}
