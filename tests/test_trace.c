/**
 * @file test_trace.c
 * @brief Unit tests for the distributed tracing module.
 *
 * Exercises W3C traceparent parsing/formatting, span lifecycle, ring buffer
 * SPSC semantics, sampling decisions, and OTLP JSON generation (indirectly
 * through the span API).
 */

#include "test_utils.h"
#include "keel/trace/trace.h"
#include "keel/mem/mem.h"
#include <string.h>
#include <stdint.h>

/* ============================================================================
 * W3C Traceparent Parsing
 * ============================================================================ */

static void test_parse_valid_traceparent(void) {
    TEST_BEGIN("parse valid traceparent");

    keel_trace_ctx_t ctx;
    bool ok = keel_trace_parse_traceparent(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", &ctx);

    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(ctx.version, 0);
    TEST_ASSERT(ctx.trace_id.hi == 0x4bf92f3577b34da6ULL);
    TEST_ASSERT(ctx.trace_id.lo == 0xa3ce929d0e0e4736ULL);
    TEST_ASSERT(ctx.parent_span_id == 0x00f067aa0ba902b7ULL);
    TEST_ASSERT_EQ(ctx.trace_flags, 0x01);

    TEST_END();
}

static void test_parse_unsampled_traceparent(void) {
    TEST_BEGIN("parse unsampled traceparent");

    keel_trace_ctx_t ctx;
    bool ok = keel_trace_parse_traceparent(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00", &ctx);

    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(ctx.trace_flags, 0x00);
    TEST_ASSERT(!(ctx.trace_flags & KEEL_TRACE_FLAG_SAMPLED));

    TEST_END();
}

static void test_parse_invalid_traceparent(void) {
    TEST_BEGIN("parse invalid traceparent");

    keel_trace_ctx_t ctx;

    /* Too short */
    TEST_ASSERT(!keel_trace_parse_traceparent("00-abc", &ctx));

    /* Bad delimiters */
    TEST_ASSERT(!keel_trace_parse_traceparent(
        "00+4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", &ctx));

    /* Zero trace-id is invalid */
    TEST_ASSERT(!keel_trace_parse_traceparent(
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01", &ctx));

    /* Zero span-id is invalid */
    TEST_ASSERT(!keel_trace_parse_traceparent(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", &ctx));

    /* Non-hex characters */
    TEST_ASSERT(!keel_trace_parse_traceparent(
        "00-ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ-00f067aa0ba902b7-01", &ctx));

    /* NULL input */
    TEST_ASSERT(!keel_trace_parse_traceparent(NULL, &ctx));
    TEST_ASSERT(!keel_trace_parse_traceparent("00-abc", NULL));

    TEST_END();
}

/* ============================================================================
 * W3C Traceparent Formatting
 * ============================================================================ */

static void test_format_traceparent(void) {
    TEST_BEGIN("format traceparent");

    keel_trace_ctx_t ctx = {
        .version = 0,
        .trace_id = { .hi = 0x4bf92f3577b34da6ULL, .lo = 0xa3ce929d0e0e4736ULL },
        .parent_span_id = 0x00f067aa0ba902b7ULL,
        .trace_flags = 0x01,
    };

    char buf[64];
    size_t len = keel_trace_format_traceparent(&ctx, buf, sizeof(buf));

    TEST_ASSERT_EQ(len, 55);
    TEST_ASSERT_STR_EQ(buf, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

    TEST_END();
}

static void test_format_traceparent_roundtrip(void) {
    TEST_BEGIN("format/parse traceparent roundtrip");

    const char* original = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    keel_trace_ctx_t ctx;
    TEST_ASSERT(keel_trace_parse_traceparent(original, &ctx));

    char buf[64];
    size_t len = keel_trace_format_traceparent(&ctx, buf, sizeof(buf));
    TEST_ASSERT_EQ(len, 55);
    TEST_ASSERT_STR_EQ(buf, original);

    TEST_END();
}

static void test_format_traceparent_buffer_too_small(void) {
    TEST_BEGIN("format traceparent buffer too small");

    keel_trace_ctx_t ctx = {
        .version = 0,
        .trace_id = { .hi = 1, .lo = 2 },
        .parent_span_id = 3,
        .trace_flags = 0x01,
    };

    char buf[10];
    size_t len = keel_trace_format_traceparent(&ctx, buf, sizeof(buf));
    TEST_ASSERT_EQ(len, 0);

    TEST_END();
}

/* ============================================================================
 * ID Generation
 * ============================================================================ */

static void test_generate_trace_id(void) {
    TEST_BEGIN("generate trace id");

    keel_trace_id_t id1 = keel_trace_generate_trace_id();
    keel_trace_id_t id2 = keel_trace_generate_trace_id();

    /* IDs should not be zero */
    TEST_ASSERT(!keel_trace_id_is_zero(id1));
    TEST_ASSERT(!keel_trace_id_is_zero(id2));

    /* IDs should be different */
    TEST_ASSERT(id1.hi != id2.hi || id1.lo != id2.lo);

    TEST_END();
}

static void test_generate_span_id(void) {
    TEST_BEGIN("generate span id");

    keel_span_id_t id1 = keel_trace_generate_span_id();
    keel_span_id_t id2 = keel_trace_generate_span_id();

    /* IDs should not be zero */
    TEST_ASSERT(id1 != 0);
    TEST_ASSERT(id2 != 0);

    /* IDs should be different */
    TEST_ASSERT(id1 != id2);

    TEST_END();
}

/* ============================================================================
 * Span Lifecycle
 * ============================================================================ */

static void test_span_start_finish(void) {
    TEST_BEGIN("span start/finish");

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.operation", KEEL_SPAN_SERVER, tid, 0);

    TEST_ASSERT(span.trace_id.hi == tid.hi && span.trace_id.lo == tid.lo);
    TEST_ASSERT(span.span_id != 0);
    TEST_ASSERT(span.parent_span_id == 0);
    TEST_ASSERT_EQ(span.kind, KEEL_SPAN_SERVER);
    TEST_ASSERT_STR_EQ(span.name, "test.operation");
    TEST_ASSERT(span.start_time_ns > 0);
    TEST_ASSERT_EQ(span.end_time_ns, 0);
    TEST_ASSERT_EQ(span.event_count, 0);
    TEST_ASSERT_EQ(span.attr_count, 0);

    keel_span_finish(&span);
    TEST_ASSERT(span.end_time_ns >= span.start_time_ns);

    /* Double-finish should be a no-op */
    uint64_t end1 = span.end_time_ns;
    keel_span_finish(&span);
    TEST_ASSERT(span.end_time_ns == end1);

    TEST_END();
}

static void test_span_events(void) {
    TEST_BEGIN("span events");

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.events", KEEL_SPAN_INTERNAL, tid, 0);

    keel_span_add_event(&span, "pool.borrow");
    keel_span_add_event(&span, "backend.query");
    keel_span_add_event(&span, "backend.response");

    TEST_ASSERT_EQ(span.event_count, 3);
    TEST_ASSERT_STR_EQ(span.events[0].name, "pool.borrow");
    TEST_ASSERT_STR_EQ(span.events[1].name, "backend.query");
    TEST_ASSERT_STR_EQ(span.events[2].name, "backend.response");
    TEST_ASSERT(span.events[0].timestamp_ns > 0);

    /* Events should be in chronological order */
    TEST_ASSERT(span.events[1].timestamp_ns >= span.events[0].timestamp_ns);
    TEST_ASSERT(span.events[2].timestamp_ns >= span.events[1].timestamp_ns);

    TEST_END();
}

static void test_span_event_overflow(void) {
    TEST_BEGIN("span event overflow");

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.overflow", KEEL_SPAN_INTERNAL, tid, 0);

    /* Fill all event slots */
    for (int i = 0; i < KEEL_SPAN_MAX_EVENTS; i++) {
        keel_span_add_event(&span, "event");
    }
    TEST_ASSERT_EQ(span.event_count, KEEL_SPAN_MAX_EVENTS);

    /* One more should be silently ignored */
    keel_span_add_event(&span, "overflow");
    TEST_ASSERT_EQ(span.event_count, KEEL_SPAN_MAX_EVENTS);

    TEST_END();
}

static void test_span_attributes(void) {
    TEST_BEGIN("span attributes");

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.attrs", KEEL_SPAN_INTERNAL, tid, 0);

    keel_span_set_attr_str(&span, "db.system", "postgresql");
    keel_span_set_attr_int(&span, "db.query.rows", 42);
    keel_span_set_attr_bool(&span, "db.query.cached", true);

    TEST_ASSERT_EQ(span.attr_count, 3);
    TEST_ASSERT_EQ(span.attrs[0].type, KEEL_ATTR_STR);
    TEST_ASSERT_STR_EQ(span.attrs[0].key, "db.system");
    TEST_ASSERT_STR_EQ(span.attrs[0].str_val, "postgresql");
    TEST_ASSERT_EQ(span.attrs[1].type, KEEL_ATTR_INT);
    TEST_ASSERT(span.attrs[1].int_val == 42);
    TEST_ASSERT_EQ(span.attrs[2].type, KEEL_ATTR_BOOL);
    TEST_ASSERT(span.attrs[2].bool_val == true);

    TEST_END();
}

static void test_span_attribute_overflow(void) {
    TEST_BEGIN("span attribute overflow");

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.attr_overflow", KEEL_SPAN_INTERNAL, tid, 0);

    for (int i = 0; i < KEEL_SPAN_MAX_ATTRIBUTES; i++) {
        keel_span_set_attr_str(&span, "key", "val");
    }
    TEST_ASSERT_EQ(span.attr_count, KEEL_SPAN_MAX_ATTRIBUTES);

    keel_span_set_attr_str(&span, "overflow", "ignored");
    TEST_ASSERT_EQ(span.attr_count, KEEL_SPAN_MAX_ATTRIBUTES);

    TEST_END();
}

static void test_span_status(void) {
    TEST_BEGIN("span status");

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.status", KEEL_SPAN_INTERNAL, tid, 0);

    TEST_ASSERT_EQ(span.status, KEEL_SPAN_STATUS_UNSET);

    keel_span_set_status(&span, KEEL_SPAN_STATUS_ERROR, "connection refused");
    TEST_ASSERT_EQ(span.status, KEEL_SPAN_STATUS_ERROR);
    TEST_ASSERT_STR_EQ(span.status_msg, "connection refused");

    keel_span_set_status(&span, KEEL_SPAN_STATUS_OK, NULL);
    TEST_ASSERT_EQ(span.status, KEEL_SPAN_STATUS_OK);

    TEST_END();
}

/* ============================================================================
 * Span Ring Buffer
 * ============================================================================ */

static void test_ring_init_destroy(void) {
    TEST_BEGIN("ring init/destroy");

    keel_span_ring_t ring;
    int rc = keel_span_ring_init(&ring, 64);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(ring.capacity == 64);
    TEST_ASSERT_NOT_NULL(ring.slots);

    keel_span_ring_destroy(&ring);

    TEST_END();
}

static void test_ring_power_of_two(void) {
    TEST_BEGIN("ring capacity rounds to power of 2");

    keel_span_ring_t ring;
    int rc = keel_span_ring_init(&ring, 100);
    TEST_ASSERT_EQ(rc, 0);
    /* 100 rounds up to 128 */
    TEST_ASSERT(ring.capacity == 128);

    keel_span_ring_destroy(&ring);

    TEST_END();
}

static void test_ring_push_pop(void) {
    TEST_BEGIN("ring push/pop basic");

    keel_span_ring_t ring;
    keel_span_ring_init(&ring, 16);

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.ring", KEEL_SPAN_SERVER, tid, 0);
    keel_span_finish(&span);

    /* Push one span */
    TEST_ASSERT(keel_span_ring_push(&ring, &span));

    /* Pop it back */
    keel_span_t out[4];
    size_t n = keel_span_ring_pop_batch(&ring, out, 4);
    TEST_ASSERT_EQ(n, 1);
    TEST_ASSERT_STR_EQ(out[0].name, "test.ring");
    TEST_ASSERT(out[0].trace_id.hi == tid.hi && out[0].trace_id.lo == tid.lo);

    /* Ring should be empty */
    n = keel_span_ring_pop_batch(&ring, out, 4);
    TEST_ASSERT_EQ(n, 0);

    keel_span_ring_destroy(&ring);

    TEST_END();
}

static void test_ring_fill_and_overflow(void) {
    TEST_BEGIN("ring fill and overflow");

    keel_span_ring_t ring;
    keel_span_ring_init(&ring, 4); /* Capacity = 4, usable = 3 (SPSC) */

    keel_span_t span;
    memset(&span, 0, sizeof(span));
    span.name = "fill";

    /* Fill ring (capacity - 1 slots usable in SPSC) */
    int pushed = 0;
    for (int i = 0; i < 10; i++) {
        if (keel_span_ring_push(&ring, &span))
            pushed++;
    }
    /* Should have pushed 3 (capacity - 1) */
    TEST_ASSERT(pushed == 3);

    /* Drain all */
    keel_span_t out[8];
    size_t n = keel_span_ring_pop_batch(&ring, out, 8);
    TEST_ASSERT_EQ(n, 3);

    keel_span_ring_destroy(&ring);

    TEST_END();
}

static void test_ring_batch_pop(void) {
    TEST_BEGIN("ring batch pop with limit");

    keel_span_ring_t ring;
    keel_span_ring_init(&ring, 16);

    keel_span_t span;
    memset(&span, 0, sizeof(span));
    span.name = "batch";

    /* Push 8 spans */
    for (int i = 0; i < 8; i++) {
        keel_span_ring_push(&ring, &span);
    }

    /* Pop with limit 3 */
    keel_span_t out[8];
    size_t n = keel_span_ring_pop_batch(&ring, out, 3);
    TEST_ASSERT_EQ(n, 3);

    /* Pop remaining */
    n = keel_span_ring_pop_batch(&ring, out, 8);
    TEST_ASSERT_EQ(n, 5);

    /* Empty */
    n = keel_span_ring_pop_batch(&ring, out, 8);
    TEST_ASSERT_EQ(n, 0);

    keel_span_ring_destroy(&ring);

    TEST_END();
}

/* ============================================================================
 * Trace ID Zero Check
 * ============================================================================ */

static void test_trace_id_is_zero(void) {
    TEST_BEGIN("trace_id is_zero");

    keel_trace_id_t zero = {0, 0};
    keel_trace_id_t nonzero = {0, 1};
    keel_trace_id_t hi_only = {1, 0};

    TEST_ASSERT(keel_trace_id_is_zero(zero));
    TEST_ASSERT(!keel_trace_id_is_zero(nonzero));
    TEST_ASSERT(!keel_trace_id_is_zero(hi_only));

    TEST_END();
}

/* ============================================================================
 * Timestamp
 * ============================================================================ */

static void test_now_ns(void) {
    TEST_BEGIN("trace now_ns");

    uint64_t t1 = keel_trace_now_ns();
    uint64_t t2 = keel_trace_now_ns();

    /* Should be non-zero and monotonic */
    TEST_ASSERT(t1 > 0);
    TEST_ASSERT(t2 >= t1);

    TEST_END();
}

/* ============================================================================
 * Tracer Sampling
 * ============================================================================ */

static void test_tracer_create_destroy(void) {
    TEST_BEGIN("tracer create/destroy");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 1000000; /* 100% */
    config.flush_interval_ms = 100;

    keel_tracer_t* tracer = keel_tracer_create(&config, 2);
    TEST_ASSERT_NOT_NULL(tracer);

    const keel_tracer_stats_t* stats = keel_tracer_get_stats(tracer);
    TEST_ASSERT_NOT_NULL(stats);

    keel_tracer_destroy(tracer);

    TEST_END();
}

static void test_tracer_submit_and_stats(void) {
    TEST_BEGIN("tracer submit and stats");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 1000000;
    config.flush_interval_ms = 60000; /* Long interval — we just test queueing */

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    keel_trace_id_t tid = keel_trace_generate_trace_id();
    keel_span_t span;
    keel_span_start(&span, "test.submit", KEEL_SPAN_SERVER, tid, 0);
    keel_span_finish(&span);

    bool ok = keel_tracer_submit(tracer, 0, &span);
    TEST_ASSERT(ok);

    const keel_tracer_stats_t* stats = keel_tracer_get_stats(tracer);
    TEST_ASSERT(atomic_load(&stats->spans_created) >= 1);

    keel_tracer_destroy(tracer);

    TEST_END();
}

static void test_tracer_disabled(void) {
    TEST_BEGIN("tracer disabled returns NULL");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = false;

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT(tracer == NULL);

    TEST_END();
}

static void test_tracer_sampling_rate(void) {
    TEST_BEGIN("tracer sampling rate");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 0; /* Never sample */

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    /* With 0 ppm, should never sample */
    int sampled = 0;
    for (int i = 0; i < 1000; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 0);

    keel_tracer_destroy(tracer);

    /* 100% sampling */
    config.sample_rate_ppm = 1000000;
    tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    sampled = 0;
    for (int i = 0; i < 100; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 100);

    keel_tracer_destroy(tracer);

    TEST_END();
}

/* ============================================================================
 * Runtime Toggle & Dynamic Sample Rate
 * ============================================================================ */

static void test_runtime_toggle_disable(void) {
    TEST_BEGIN("runtime toggle disable stops sampling");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 1000000; /* 100% */
    config.flush_interval_ms = 60000;

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    /* Initially enabled */
    TEST_ASSERT(keel_tracer_is_enabled(tracer));
    TEST_ASSERT(keel_tracer_should_sample(tracer));

    /* Disable at runtime */
    keel_tracer_set_enabled(tracer, false);
    TEST_ASSERT(!keel_tracer_is_enabled(tracer));

    /* Should never sample when disabled */
    int sampled = 0;
    for (int i = 0; i < 100; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 0);

    keel_tracer_destroy(tracer);
    TEST_END();
}

static void test_runtime_toggle_reenable(void) {
    TEST_BEGIN("runtime toggle re-enable restores sampling");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 1000000;
    config.flush_interval_ms = 60000;

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    /* Disable then re-enable */
    keel_tracer_set_enabled(tracer, false);
    TEST_ASSERT(!keel_tracer_is_enabled(tracer));

    keel_tracer_set_enabled(tracer, true);
    TEST_ASSERT(keel_tracer_is_enabled(tracer));

    /* Should sample at 100% again */
    int sampled = 0;
    for (int i = 0; i < 100; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 100);

    keel_tracer_destroy(tracer);
    TEST_END();
}

static void test_runtime_toggle_null_tracer(void) {
    TEST_BEGIN("runtime toggle with NULL tracer is safe");

    /* These should not crash */
    keel_tracer_set_enabled(NULL, true);
    keel_tracer_set_enabled(NULL, false);
    TEST_ASSERT(!keel_tracer_is_enabled(NULL));

    TEST_END();
}

static void test_dynamic_sample_rate(void) {
    TEST_BEGIN("dynamic sample rate change");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 1000000; /* 100% */
    config.flush_interval_ms = 60000;

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    /* Drop to 0% */
    keel_tracer_set_sample_rate(tracer, 0);
    int sampled = 0;
    for (int i = 0; i < 1000; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 0);

    /* Restore to 100% */
    keel_tracer_set_sample_rate(tracer, 1000000);
    sampled = 0;
    for (int i = 0; i < 100; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 100);

    keel_tracer_destroy(tracer);
    TEST_END();
}

static void test_dynamic_sample_rate_clamp(void) {
    TEST_BEGIN("dynamic sample rate clamped at 1000000");

    keel_trace_config_t config = KEEL_TRACE_CONFIG_DEFAULT;
    config.enabled = true;
    config.sample_rate_ppm = 500000;
    config.flush_interval_ms = 60000;

    keel_tracer_t* tracer = keel_tracer_create(&config, 1);
    TEST_ASSERT_NOT_NULL(tracer);

    /* Set beyond max — should clamp */
    keel_tracer_set_sample_rate(tracer, 9999999);

    /* At 100% (clamped), all should sample */
    int sampled = 0;
    for (int i = 0; i < 100; i++) {
        if (keel_tracer_should_sample(tracer))
            sampled++;
    }
    TEST_ASSERT_EQ(sampled, 100);

    /* NULL tracer should not crash */
    keel_tracer_set_sample_rate(NULL, 500000);

    keel_tracer_destroy(tracer);
    TEST_END();
}

/* ============================================================================
 * Test: keel_trace_format_sql_comment
 * ============================================================================ */

static void test_format_sql_comment(void) {
    TEST_BEGIN("format_sql_comment");

    /* Build a sampled trace context */
    keel_trace_ctx_t ctx = {0};
    ctx.trace_flags = 0x01; /* sampled bit set */
    /* Populate a known trace_id and span_id for deterministic output */
    ctx.trace_id.hi     = UINT64_C(0x0102030405060708);
    ctx.trace_id.lo     = UINT64_C(0x090a0b0c0d0e0f10);
    ctx.parent_span_id  = UINT64_C(0x1112131415161718);

    char buf[128] = {0};
    size_t n = keel_trace_format_sql_comment(&ctx, buf, sizeof(buf));

    /* Must produce a non-empty result when sampled */
    TEST_ASSERT(n > 0);

    /* Must start with the traceparent prefix */
    TEST_ASSERT(n >= 74); /* min: prefix(18) + 32 + dash + 16 + "-01 *X/ " */
    TEST_ASSERT(memcmp(buf, "/* traceparent=", 15) == 0);
    TEST_ASSERT(strstr(buf, "traceparent=") != NULL);

    // Must end with " */ " (space-star-slash-space)
    const char *suffix = " */ ";
    TEST_ASSERT(n >= strlen(suffix));
    TEST_ASSERT(memcmp(buf + n - strlen(suffix), suffix, strlen(suffix)) == 0);

    /* Unsampled context: must return 0 */
    keel_trace_ctx_t unsampled = ctx;
    unsampled.trace_flags = 0x00;
    char buf2[128] = {0};
    size_t n2 = keel_trace_format_sql_comment(&unsampled, buf2, sizeof(buf2));
    TEST_ASSERT_EQ(n2, (size_t)0);

    /* NULL context: must return 0 */
    size_t n3 = keel_trace_format_sql_comment(NULL, buf, sizeof(buf));
    TEST_ASSERT_EQ(n3, (size_t)0);

    /* Buffer too small: must return 0 */
    char tiny[16] = {0};
    size_t n4 = keel_trace_format_sql_comment(&ctx, tiny, sizeof(tiny));
    TEST_ASSERT_EQ(n4, (size_t)0);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    keel_mem_init(NULL);

    /* W3C Traceparent */
    test_parse_valid_traceparent();
    test_parse_unsampled_traceparent();
    test_parse_invalid_traceparent();
    test_format_traceparent();
    test_format_traceparent_roundtrip();
    test_format_traceparent_buffer_too_small();

    /* ID generation */
    test_generate_trace_id();
    test_generate_span_id();
    test_trace_id_is_zero();
    test_now_ns();

    /* Span lifecycle */
    test_span_start_finish();
    test_span_events();
    test_span_event_overflow();
    test_span_attributes();
    test_span_attribute_overflow();
    test_span_status();

    /* Ring buffer */
    test_ring_init_destroy();
    test_ring_power_of_two();
    test_ring_push_pop();
    test_ring_fill_and_overflow();
    test_ring_batch_pop();

    /* Tracer */
    test_tracer_create_destroy();
    test_tracer_submit_and_stats();
    test_tracer_disabled();
    test_tracer_sampling_rate();

    /* Runtime toggle */
    test_runtime_toggle_disable();
    test_runtime_toggle_reenable();
    test_runtime_toggle_null_tracer();
    test_dynamic_sample_rate();
    test_dynamic_sample_rate_clamp();

    /* SQL comment injection */
    test_format_sql_comment();

    keel_mem_shutdown();

    return test_summary();
}
