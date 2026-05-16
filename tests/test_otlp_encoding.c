/**
 * @file test_otlp_encoding.c
 * @brief Unit tests for OTLP JSON and protobuf span encoding.
 *
 * Validates that keel_otlp_build_json() produces valid JSON with expected
 * fields, and keel_otlp_build_protobuf() produces valid protobuf wire
 * format with correct field numbers and content.
 */

#include "test_utils.h"
#include "keel/trace/trace.h"
#include "keel/mem/mem.h"
#include <string.h>
#include <stdlib.h>

/* ============================================================================
 * Helper: build a test span with known values
 * ============================================================================ */

static keel_span_t make_test_span(void) {
    keel_span_t span;
    memset(&span, 0, sizeof(span));

    span.trace_id.hi = 0x0123456789abcdefULL;
    span.trace_id.lo = 0xfedcba9876543210ULL;
    span.span_id     = 0xdeadbeefcafe1234ULL;
    span.parent_span_id = 0x1111111111111111ULL;
    span.kind        = KEEL_SPAN_SERVER;
    span.name        = "test.query";
    span.start_time_ns = 1700000000000000000ULL; /* ~Nov 2023 */
    span.end_time_ns   = 1700000000001000000ULL; /* +1ms */
    span.status      = KEEL_SPAN_STATUS_OK;
    span.status_msg  = NULL;

    span.service_name = "keel-test";
    span.node_id      = "node-1";
    span.worker_id    = 0;

    /* Add attributes */
    keel_span_set_attr_str(&span, "db.system", "postgresql");
    keel_span_set_attr_int(&span, "db.query.rows", 42);
    keel_span_set_attr_bool(&span, "db.query.cached", true);

    /* Add events */
    span.events[0].name = "pool.borrow";
    span.events[0].timestamp_ns = 1700000000000100000ULL;
    span.events[1].name = "backend.query";
    span.events[1].timestamp_ns = 1700000000000200000ULL;
    span.event_count = 2;

    return span;
}

/* ============================================================================
 * JSON Encoding Tests
 * ============================================================================ */

static void test_json_basic_structure(void) {
    TEST_BEGIN("JSON basic structure");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel-test", &len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(len > 0);

    /* Must start with resourceSpans envelope */
    TEST_ASSERT(strstr(json, "\"resourceSpans\"") != NULL);
    TEST_ASSERT(strstr(json, "\"scopeSpans\"") != NULL);
    TEST_ASSERT(strstr(json, "\"spans\"") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_service_name(void) {
    TEST_BEGIN("JSON service.name resource attribute");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "my-keel-proxy", &len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"service.name\"") != NULL);
    TEST_ASSERT(strstr(json, "my-keel-proxy") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_trace_ids(void) {
    TEST_BEGIN("JSON trace/span IDs as hex strings");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);

    /* trace_id = 0x0123456789abcdef fedcba9876543210 → 32 hex chars */
    TEST_ASSERT(strstr(json, "0123456789abcdeffedcba9876543210") != NULL);

    /* span_id = 0xdeadbeefcafe1234 → 16 hex chars */
    TEST_ASSERT(strstr(json, "deadbeefcafe1234") != NULL);

    /* parent_span_id = 0x1111111111111111 */
    TEST_ASSERT(strstr(json, "1111111111111111") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_span_name_and_kind(void) {
    TEST_BEGIN("JSON span name and kind");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);

    /* Name */
    TEST_ASSERT(strstr(json, "\"name\":\"test.query\"") != NULL);

    /* Kind: SERVER = 2 in OTLP */
    TEST_ASSERT(strstr(json, "\"kind\":2") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_timestamps(void) {
    TEST_BEGIN("JSON timestamps as nanosecond strings");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"startTimeUnixNano\":\"1700000000000000000\"") != NULL);
    TEST_ASSERT(strstr(json, "\"endTimeUnixNano\":\"1700000000001000000\"") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_attributes(void) {
    TEST_BEGIN("JSON span attributes");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);

    /* String attribute */
    TEST_ASSERT(strstr(json, "\"db.system\"") != NULL);
    TEST_ASSERT(strstr(json, "\"stringValue\":\"postgresql\"") != NULL);

    /* Int attribute */
    TEST_ASSERT(strstr(json, "\"db.query.rows\"") != NULL);
    TEST_ASSERT(strstr(json, "\"intValue\":\"42\"") != NULL);

    /* Bool attribute */
    TEST_ASSERT(strstr(json, "\"db.query.cached\"") != NULL);
    TEST_ASSERT(strstr(json, "\"boolValue\":true") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_events(void) {
    TEST_BEGIN("JSON span events");

    keel_span_t span = make_test_span();
    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"events\"") != NULL);
    TEST_ASSERT(strstr(json, "\"name\":\"pool.borrow\"") != NULL);
    TEST_ASSERT(strstr(json, "\"name\":\"backend.query\"") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_status(void) {
    TEST_BEGIN("JSON span status");

    keel_span_t span = make_test_span();
    span.status = KEEL_SPAN_STATUS_ERROR;
    span.status_msg = "connection refused";

    size_t len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "\"status\"") != NULL);
    TEST_ASSERT(strstr(json, "\"code\":2") != NULL);
    TEST_ASSERT(strstr(json, "connection refused") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_multiple_spans(void) {
    TEST_BEGIN("JSON multiple spans in batch");

    keel_span_t spans[3];
    for (int i = 0; i < 3; i++) {
        spans[i] = make_test_span();
        spans[i].span_id = (keel_span_id_t)(0xAAAA000000000000ULL + (uint64_t)i);
    }
    spans[0].name = "span.one";
    spans[1].name = "span.two";
    spans[2].name = "span.three";

    size_t len = 0;
    char* json = keel_otlp_build_json(spans, 3, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT(strstr(json, "span.one") != NULL);
    TEST_ASSERT(strstr(json, "span.two") != NULL);
    TEST_ASSERT(strstr(json, "span.three") != NULL);

    keel_free(json);
    TEST_END();
}

static void test_json_empty_batch(void) {
    TEST_BEGIN("JSON empty batch");

    keel_span_t dummy;
    memset(&dummy, 0, sizeof(dummy));
    size_t len = 0;
    char* json = keel_otlp_build_json(&dummy, 0, "keel", &len);

    TEST_ASSERT_NOT_NULL(json);
    /* Should still produce valid envelope with empty spans array */
    TEST_ASSERT(strstr(json, "\"resourceSpans\"") != NULL);
    TEST_ASSERT(strstr(json, "\"spans\":[]") != NULL);

    keel_free(json);
    TEST_END();
}

/* ============================================================================
 * Protobuf Encoding Tests
 * ============================================================================ */

/**
 * Helper: decode a protobuf varint from a byte buffer.
 * Returns the number of bytes consumed, or 0 on error.
 */
static size_t decode_varint(const uint8_t* buf, size_t buf_len, uint64_t* out) {
    *out = 0;
    for (size_t i = 0; i < buf_len && i < 10; i++) {
        *out |= ((uint64_t)(buf[i] & 0x7F)) << (7 * i);
        if ((buf[i] & 0x80) == 0)
            return i + 1;
    }
    return 0; /* Error: varint too long or truncated */
}

/**
 * Helper: find a protobuf field by number and wire type in a message.
 * Returns pointer to the field's value (after the tag), or NULL if not found.
 * Sets *field_len for length-delimited (wire type 2) fields.
 */
static const uint8_t* find_pb_field(const uint8_t* msg, size_t msg_len,
                                     uint32_t target_field, uint32_t target_wire,
                                     size_t* field_len) {
    size_t pos = 0;
    while (pos < msg_len) {
        uint64_t tag;
        size_t tag_len = decode_varint(msg + pos, msg_len - pos, &tag);
        if (tag_len == 0) break;
        pos += tag_len;

        uint32_t field_num = (uint32_t)(tag >> 3);
        uint32_t wire_type = (uint32_t)(tag & 0x07);

        if (field_num == target_field && wire_type == target_wire) {
            if (wire_type == 2) { /* Length-delimited */
                uint64_t len;
                size_t len_bytes = decode_varint(msg + pos, msg_len - pos, &len);
                if (len_bytes == 0) break;
                pos += len_bytes;
                if (field_len) *field_len = (size_t)len;
                return msg + pos;
            } else if (wire_type == 0) { /* Varint */
                if (field_len) *field_len = 0;
                return msg + pos;
            } else if (wire_type == 1) { /* Fixed64 */
                if (field_len) *field_len = 8;
                return msg + pos;
            }
        }

        /* Skip this field */
        if (wire_type == 0) { /* Varint */
            uint64_t val;
            size_t n = decode_varint(msg + pos, msg_len - pos, &val);
            if (n == 0) break;
            pos += n;
        } else if (wire_type == 1) { /* Fixed64 */
            pos += 8;
        } else if (wire_type == 2) { /* Length-delimited */
            uint64_t len;
            size_t n = decode_varint(msg + pos, msg_len - pos, &len);
            if (n == 0) break;
            pos += n + (size_t)len;
        } else if (wire_type == 5) { /* Fixed32 */
            pos += 4;
        } else {
            break; /* Unknown wire type */
        }
    }
    return NULL;
}

static void test_protobuf_non_empty(void) {
    TEST_BEGIN("protobuf non-empty output");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel-test", &len);

    TEST_ASSERT_NOT_NULL(pb);
    TEST_ASSERT(len > 0);

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_top_level_structure(void) {
    TEST_BEGIN("protobuf ExportTraceServiceRequest structure");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel-test", &len);

    TEST_ASSERT_NOT_NULL(pb);

    /* ExportTraceServiceRequest has field 1 = ResourceSpans (length-delimited) */
    size_t rs_len = 0;
    const uint8_t* rs = find_pb_field(pb, len, 1, 2, &rs_len);
    TEST_ASSERT_NOT_NULL(rs);
    TEST_ASSERT(rs_len > 0);

    /* ResourceSpans has field 1 = Resource (length-delimited) */
    size_t res_len = 0;
    const uint8_t* res = find_pb_field(rs, rs_len, 1, 2, &res_len);
    TEST_ASSERT_NOT_NULL(res);

    /* ResourceSpans has field 2 = ScopeSpans (length-delimited) */
    size_t ss_len = 0;
    const uint8_t* ss = find_pb_field(rs, rs_len, 2, 2, &ss_len);
    TEST_ASSERT_NOT_NULL(ss);
    TEST_ASSERT(ss_len > 0);

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_trace_id_length(void) {
    TEST_BEGIN("protobuf trace_id is 16 bytes");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel", &len);
    TEST_ASSERT_NOT_NULL(pb);

    /* Navigate: Request.ResourceSpans(1) → ScopeSpans(2) → Span(2) → trace_id(1) */
    size_t rs_len = 0;
    const uint8_t* rs = find_pb_field(pb, len, 1, 2, &rs_len);
    TEST_ASSERT_NOT_NULL(rs);

    size_t ss_len = 0;
    const uint8_t* ss = find_pb_field(rs, rs_len, 2, 2, &ss_len);
    TEST_ASSERT_NOT_NULL(ss);

    size_t span_len = 0;
    const uint8_t* span_data = find_pb_field(ss, ss_len, 2, 2, &span_len);
    TEST_ASSERT_NOT_NULL(span_data);

    /* Span field 1 = trace_id (bytes, should be 16 bytes) */
    size_t tid_len = 0;
    const uint8_t* tid = find_pb_field(span_data, span_len, 1, 2, &tid_len);
    TEST_ASSERT_NOT_NULL(tid);
    TEST_ASSERT_EQ(tid_len, 16);

    /* Verify the actual bytes match our test trace_id (big-endian) */
    TEST_ASSERT_EQ(tid[0], 0x01);
    TEST_ASSERT_EQ(tid[1], 0x23);
    TEST_ASSERT_EQ(tid[7], 0xef);
    TEST_ASSERT_EQ(tid[8], 0xfe);
    TEST_ASSERT_EQ(tid[15], 0x10);

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_span_id_length(void) {
    TEST_BEGIN("protobuf span_id is 8 bytes");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel", &len);
    TEST_ASSERT_NOT_NULL(pb);

    /* Navigate to span */
    size_t rs_len = 0;
    const uint8_t* rs = find_pb_field(pb, len, 1, 2, &rs_len);
    size_t ss_len = 0;
    const uint8_t* ss = find_pb_field(rs, rs_len, 2, 2, &ss_len);
    size_t span_len = 0;
    const uint8_t* span_data = find_pb_field(ss, ss_len, 2, 2, &span_len);
    TEST_ASSERT_NOT_NULL(span_data);

    /* Span field 2 = span_id (bytes, 8 bytes) */
    size_t sid_len = 0;
    const uint8_t* sid = find_pb_field(span_data, span_len, 2, 2, &sid_len);
    TEST_ASSERT_NOT_NULL(sid);
    TEST_ASSERT_EQ(sid_len, 8);

    /* First byte: 0xde (from 0xdeadbeefcafe1234) */
    TEST_ASSERT_EQ(sid[0], 0xde);
    TEST_ASSERT_EQ(sid[1], 0xad);

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_span_name(void) {
    TEST_BEGIN("protobuf span name field");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel", &len);
    TEST_ASSERT_NOT_NULL(pb);

    /* Navigate to span */
    size_t rs_len = 0;
    const uint8_t* rs = find_pb_field(pb, len, 1, 2, &rs_len);
    size_t ss_len = 0;
    const uint8_t* ss = find_pb_field(rs, rs_len, 2, 2, &ss_len);
    size_t span_len = 0;
    const uint8_t* span_data = find_pb_field(ss, ss_len, 2, 2, &span_len);
    TEST_ASSERT_NOT_NULL(span_data);

    /* Span field 5 = name (string) */
    size_t name_len = 0;
    const uint8_t* name = find_pb_field(span_data, span_len, 5, 2, &name_len);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_EQ(name_len, strlen("test.query"));
    TEST_ASSERT(memcmp(name, "test.query", name_len) == 0);

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_span_kind(void) {
    TEST_BEGIN("protobuf span kind = SERVER(2)");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel", &len);
    TEST_ASSERT_NOT_NULL(pb);

    /* Navigate to span */
    size_t rs_len = 0;
    const uint8_t* rs = find_pb_field(pb, len, 1, 2, &rs_len);
    size_t ss_len = 0;
    const uint8_t* ss = find_pb_field(rs, rs_len, 2, 2, &ss_len);
    size_t span_len = 0;
    const uint8_t* span_data = find_pb_field(ss, ss_len, 2, 2, &span_len);
    TEST_ASSERT_NOT_NULL(span_data);

    /* Span field 6 = kind (varint) */
    const uint8_t* kind = find_pb_field(span_data, span_len, 6, 0, NULL);
    TEST_ASSERT_NOT_NULL(kind);
    uint64_t kind_val;
    decode_varint(kind, 10, &kind_val);
    TEST_ASSERT_EQ(kind_val, 2); /* OTLP SERVER = 2 */

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_multiple_spans(void) {
    TEST_BEGIN("protobuf multiple spans in batch");

    keel_span_t spans[3];
    for (int i = 0; i < 3; i++) {
        spans[i] = make_test_span();
        spans[i].span_id = (keel_span_id_t)(0xBBBB000000000000ULL + (uint64_t)i);
    }

    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(spans, 3, "keel", &len);

    TEST_ASSERT_NOT_NULL(pb);
    TEST_ASSERT(len > 100); /* Should be non-trivial */

    /* Navigate to ScopeSpans — should find multiple Span fields (field 2) */
    size_t rs_len = 0;
    const uint8_t* rs = find_pb_field(pb, len, 1, 2, &rs_len);
    size_t ss_len = 0;
    const uint8_t* ss = find_pb_field(rs, rs_len, 2, 2, &ss_len);
    TEST_ASSERT_NOT_NULL(ss);

    /* Count span fields (field 2 in ScopeSpans) */
    int span_count = 0;
    size_t pos = 0;
    while (pos < ss_len) {
        uint64_t tag;
        size_t tag_len = decode_varint(ss + pos, ss_len - pos, &tag);
        if (tag_len == 0) break;
        pos += tag_len;
        uint32_t field = (uint32_t)(tag >> 3);
        uint32_t wire = (uint32_t)(tag & 0x07);

        if (field == 2 && wire == 2) span_count++;

        /* Skip value */
        if (wire == 0) {
            uint64_t val;
            size_t n = decode_varint(ss + pos, ss_len - pos, &val);
            if (n == 0) break;
            pos += n;
        } else if (wire == 1) {
            pos += 8;
        } else if (wire == 2) {
            uint64_t vlen;
            size_t n = decode_varint(ss + pos, ss_len - pos, &vlen);
            if (n == 0) break;
            pos += n + (size_t)vlen;
        } else {
            break;
        }
    }
    TEST_ASSERT_EQ(span_count, 3);

    keel_free(pb);
    TEST_END();
}

static void test_protobuf_service_name(void) {
    TEST_BEGIN("protobuf resource service.name");

    keel_span_t span = make_test_span();
    size_t len = 0;
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "my-proxy", &len);
    TEST_ASSERT_NOT_NULL(pb);

    /* ResourceSpans(1) → Resource(1) → attributes(1) → KeyValue → value → string */
    /* Just check that "my-proxy" bytes appear somewhere in the output */
    int found = 0;
    for (size_t i = 0; i + 8 <= len; i++) {
        if (memcmp(pb + i, "my-proxy", 8) == 0) {
            found = 1;
            break;
        }
    }
    TEST_ASSERT(found);

    keel_free(pb);
    TEST_END();
}

/* ============================================================================
 * Cross-format Consistency
 * ============================================================================ */

static void test_json_vs_protobuf_both_produce_output(void) {
    TEST_BEGIN("JSON and protobuf both produce non-empty output");

    keel_span_t span = make_test_span();

    size_t json_len = 0, pb_len = 0;
    char* json = keel_otlp_build_json(&span, 1, "keel", &json_len);
    uint8_t* pb = keel_otlp_build_protobuf(&span, 1, "keel", &pb_len);

    TEST_ASSERT_NOT_NULL(json);
    TEST_ASSERT_NOT_NULL(pb);
    TEST_ASSERT(json_len > 0);
    TEST_ASSERT(pb_len > 0);

    /* Protobuf should be more compact than JSON */
    TEST_ASSERT(pb_len < json_len);

    keel_free(json);
    keel_free(pb);
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    keel_mem_init(NULL);

    /* JSON encoding */
    test_json_basic_structure();
    test_json_service_name();
    test_json_trace_ids();
    test_json_span_name_and_kind();
    test_json_timestamps();
    test_json_attributes();
    test_json_events();
    test_json_status();
    test_json_multiple_spans();
    test_json_empty_batch();

    /* Protobuf encoding */
    test_protobuf_non_empty();
    test_protobuf_top_level_structure();
    test_protobuf_trace_id_length();
    test_protobuf_span_id_length();
    test_protobuf_span_name();
    test_protobuf_span_kind();
    test_protobuf_multiple_spans();
    test_protobuf_service_name();

    /* Cross-format */
    test_json_vs_protobuf_both_produce_output();

    keel_mem_shutdown();

    return test_summary();
}
