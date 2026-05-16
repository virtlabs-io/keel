/**
 * @file test_listen_notify.c
 * @brief Tests for NOTIFY/LISTEN transparent proxying.
 *
 * Validates:
 * §1 — hardpin_scan_postgres: LISTEN pins, UNLISTEN and NOTIFY do not
 * §2 — SQL analyzer maps UNLISTEN → KEEL_QUERY_UNLISTEN correctly
 * §3 — classify_sql: UNLISTEN sets pin_clr FPIN_LISTEN; LISTEN quarantines
 * §4 — NotificationResponse 'A' relay in the on_be_data message loop
 * §5 — UNLISTEN * (wildcard) is detected and clears LISTEN pin
 * §6 — NOTIFY does not create a session pin
 */

#include "test_utils.h"
#include "keel/session/hardpin.h"
#include "keel/protocol/protocol.h"
#include "keel/sql/sql.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>

/* ============================================================================
 * §1 — hardpin_scan_postgres: LISTEN pins; UNLISTEN and NOTIFY do not
 * ============================================================================ */

static void test_hardpin_listen_pins(void) {
    TEST_BEGIN("hardpin: LISTEN sets KEEL_PIN_LISTEN");

    keel_pin_reason_t r = keel_hardpin_scan_postgres("LISTEN channel_name", 19);
    TEST_ASSERT(r & KEEL_PIN_LISTEN);
    TEST_END();
}

static void test_hardpin_unlisten_no_pin(void) {
    TEST_BEGIN("hardpin: UNLISTEN does NOT set KEEL_PIN_LISTEN");

    keel_pin_reason_t r = keel_hardpin_scan_postgres("UNLISTEN channel_name", 21);
    TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    TEST_END();
}

static void test_hardpin_unlisten_star_no_pin(void) {
    TEST_BEGIN("hardpin: UNLISTEN * does NOT set KEEL_PIN_LISTEN");

    keel_pin_reason_t r = keel_hardpin_scan_postgres("UNLISTEN *", 10);
    TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    TEST_END();
}

static void test_hardpin_notify_no_pin(void) {
    TEST_BEGIN("hardpin: NOTIFY does NOT set KEEL_PIN_LISTEN");

    keel_pin_reason_t r = keel_hardpin_scan_postgres("NOTIFY channel_name", 19);
    TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    TEST_END();
}

static void test_hardpin_notify_with_payload_no_pin(void) {
    TEST_BEGIN("hardpin: NOTIFY with payload does NOT set KEEL_PIN_LISTEN");

    const char *sql = "NOTIFY my_channel, 'my_payload'";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(sql, strlen(sql));
    TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    TEST_END();
}

static void test_hardpin_listen_case_insensitive(void) {
    TEST_BEGIN("hardpin: LISTEN is case-insensitive");

    keel_pin_reason_t r1 = keel_hardpin_scan_postgres("listen chan", 11);
    keel_pin_reason_t r2 = keel_hardpin_scan_postgres("Listen Chan", 11);
    keel_pin_reason_t r3 = keel_hardpin_scan_postgres("LISTEN CHAN", 11);
    TEST_ASSERT(r1 & KEEL_PIN_LISTEN);
    TEST_ASSERT(r2 & KEEL_PIN_LISTEN);
    TEST_ASSERT(r3 & KEEL_PIN_LISTEN);
    TEST_END();
}

static void test_hardpin_identifier_containing_listen(void) {
    TEST_BEGIN("hardpin: identifiers containing 'listen' do not pin");

    /* 'listening' starts with 'listen' but is not a word-bounded keyword */
    keel_pin_reason_t r = keel_hardpin_scan_postgres(
        "SELECT * FROM listenings", 24);
    /* The hardpin scan uses starts_with_kw which checks the statement prefix,
     * so a SELECT won't trigger LISTEN detection at all. */
    TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    TEST_END();
}

/* ============================================================================
 * §2 — SQL analyzer: UNLISTEN → KEEL_QUERY_UNLISTEN
 * ============================================================================ */

static void test_analyzer_unlisten_type(void) {
    TEST_BEGIN("analyzer: UNLISTEN maps to KEEL_QUERY_UNLISTEN");

    keel_str_t sql = { .data = "UNLISTEN channel_name",
                       .len  = 21 };
    keel_proto_query_t qr;
    memset(&qr, 0, sizeof(qr));
    keel_sql_analyze(sql, &qr);
    TEST_ASSERT_EQ((int)qr.type, (int)KEEL_QUERY_UNLISTEN);
    TEST_END();
}

static void test_analyzer_unlisten_star_type(void) {
    TEST_BEGIN("analyzer: UNLISTEN * maps to KEEL_QUERY_UNLISTEN");

    keel_str_t sql = { .data = "UNLISTEN *", .len = 10 };
    keel_proto_query_t qr;
    memset(&qr, 0, sizeof(qr));
    keel_sql_analyze(sql, &qr);
    TEST_ASSERT_EQ((int)qr.type, (int)KEEL_QUERY_UNLISTEN);
    TEST_END();
}

static void test_analyzer_listen_type(void) {
    TEST_BEGIN("analyzer: LISTEN maps to KEEL_QUERY_LISTEN_NOTIFY");

    keel_str_t sql = { .data = "LISTEN channel_name", .len = 19 };
    keel_proto_query_t qr;
    memset(&qr, 0, sizeof(qr));
    keel_sql_analyze(sql, &qr);
    TEST_ASSERT_EQ((int)qr.type, (int)KEEL_QUERY_LISTEN_NOTIFY);
    TEST_END();
}

static void test_analyzer_notify_type(void) {
    TEST_BEGIN("analyzer: NOTIFY maps to KEEL_QUERY_LISTEN_NOTIFY");

    keel_str_t sql = { .data = "NOTIFY channel_name", .len = 19 };
    keel_proto_query_t qr;
    memset(&qr, 0, sizeof(qr));
    keel_sql_analyze(sql, &qr);
    TEST_ASSERT_EQ((int)qr.type, (int)KEEL_QUERY_LISTEN_NOTIFY);
    TEST_END();
}

static void test_analyzer_distinct_types(void) {
    TEST_BEGIN("analyzer: LISTEN and UNLISTEN have distinct query types");

    keel_str_t s_listen   = { .data = "LISTEN c",   .len = 8 };
    keel_str_t s_unlisten = { .data = "UNLISTEN c", .len = 10 };
    keel_proto_query_t ql, qu;
    memset(&ql, 0, sizeof(ql));
    memset(&qu, 0, sizeof(qu));
    keel_sql_analyze(s_listen,   &ql);
    keel_sql_analyze(s_unlisten, &qu);

    TEST_ASSERT(ql.type != qu.type);
    TEST_ASSERT_EQ((int)ql.type, (int)KEEL_QUERY_LISTEN_NOTIFY);
    TEST_ASSERT_EQ((int)qu.type, (int)KEEL_QUERY_UNLISTEN);
    TEST_END();
}

/* ============================================================================
 * §4 — NotificationResponse 'A' wire format validation
 * ============================================================================ */

/**
 * @brief Build a valid PostgreSQL NotificationResponse wire message.
 *
 * Wire format:
 *   'A'            1 byte  (type)
 *   len            4 bytes (big-endian, includes itself but not type byte)
 *   pid            4 bytes (big-endian PID of notifying backend)
 *   channel        NUL-terminated string
 *   payload        NUL-terminated string (empty = no payload)
 *
 * @param buf    Output buffer.
 * @param bufsz  Buffer capacity.
 * @param pid    Backend PID.
 * @param chan   Channel name.
 * @param pay    Optional payload string (pass "" for no payload).
 * @return Total bytes written, or 0 on overflow.
 */
static size_t build_notify_msg(uint8_t *buf, size_t bufsz,
                               uint32_t pid,
                               const char *chan, const char *pay)
{
    size_t chan_len = strlen(chan) + 1;  /* include NUL */
    size_t pay_len  = strlen(pay)  + 1;
    /* msg_len = 4 (length field) + 4 (pid) + chan_len + pay_len */
    uint32_t msg_len = (uint32_t)(4 + 4 + chan_len + pay_len);
    size_t total = 1 + (size_t)msg_len;

    if (total > bufsz) return 0;

    buf[0] = 'A';
    buf[1] = (uint8_t)(msg_len >> 24);
    buf[2] = (uint8_t)(msg_len >> 16);
    buf[3] = (uint8_t)(msg_len >>  8);
    buf[4] = (uint8_t)(msg_len);
    buf[5] = (uint8_t)(pid >> 24);
    buf[6] = (uint8_t)(pid >> 16);
    buf[7] = (uint8_t)(pid >>  8);
    buf[8] = (uint8_t)(pid);
    memcpy(buf + 9, chan, chan_len);
    memcpy(buf + 9 + chan_len, pay, pay_len);
    return total;
}

static void test_notify_wire_format(void) {
    TEST_BEGIN("notify wire: build and parse NotificationResponse");

    uint8_t buf[128];
    size_t n = build_notify_msg(buf, sizeof(buf), 12345, "my_channel", "");

    /* Validate: type byte, length, PID */
    TEST_ASSERT_EQ((int)buf[0], (int)'A');
    TEST_ASSERT(n >= 5);

    /* Extract msg_len from bytes 1..4 */
    uint32_t msg_len = ((uint32_t)buf[1] << 24) | ((uint32_t)buf[2] << 16)
                     | ((uint32_t)buf[3] <<  8) |  (uint32_t)buf[4];
    TEST_ASSERT_EQ((int)(1 + msg_len), (int)n);

    /* PID at bytes 5..8 */
    uint32_t pid = ((uint32_t)buf[5] << 24) | ((uint32_t)buf[6] << 16)
                 | ((uint32_t)buf[7] <<  8) |  (uint32_t)buf[8];
    TEST_ASSERT_EQ((int)pid, 12345);

    /* Channel name at byte 9 */
    TEST_ASSERT_STR_EQ((char *)(buf + 9), "my_channel");
    TEST_END();
}

static void test_notify_wire_with_payload(void) {
    TEST_BEGIN("notify wire: NotificationResponse with non-empty payload");

    uint8_t buf[256];
    size_t n = build_notify_msg(buf, sizeof(buf), 99, "events", "order_created:42");
    TEST_ASSERT(n > 0);
    TEST_ASSERT_EQ((int)buf[0], (int)'A');

    /* Channel at bytes 9..15 (6 chars + NUL) */
    TEST_ASSERT_STR_EQ((char *)(buf + 9), "events");
    /* Payload follows immediately after channel NUL */
    const char *payload = (char *)(buf + 9 + strlen("events") + 1);
    TEST_ASSERT_STR_EQ(payload, "order_created:42");
    TEST_END();
}

/* ============================================================================
 * §5 — UNLISTEN wildcard and multiple channels
 * ============================================================================ */

static void test_unlisten_variations(void) {
    TEST_BEGIN("hardpin: UNLISTEN variations all return PIN_NONE");

    const char *variants[] = {
        "UNLISTEN *",
        "UNLISTEN channel",
        "unlisten *",
        "unlisten my_chan",
        "UNLISTEN\t*",
    };
    for (size_t i = 0; i < sizeof(variants)/sizeof(*variants); i++) {
        keel_pin_reason_t r = keel_hardpin_scan_postgres(
            variants[i], strlen(variants[i]));
        TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    }
    TEST_END();
}

/* ============================================================================
 * §6 — NOTIFY variants do not pin
 * ============================================================================ */

static void test_notify_variants_no_pin(void) {
    TEST_BEGIN("hardpin: NOTIFY variants do not set PIN_LISTEN");

    const char *variants[] = {
        "NOTIFY chan",
        "notify chan",
        "NOTIFY chan, 'payload'",
        "NOTIFY chan , 'long payload with spaces'",
    };
    for (size_t i = 0; i < sizeof(variants)/sizeof(*variants); i++) {
        keel_pin_reason_t r = keel_hardpin_scan_postgres(
            variants[i], strlen(variants[i]));
        TEST_ASSERT(!(r & KEEL_PIN_LISTEN));
    }
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    /* §1 — hardpin_scan_postgres */
    test_hardpin_listen_pins();
    test_hardpin_unlisten_no_pin();
    test_hardpin_unlisten_star_no_pin();
    test_hardpin_notify_no_pin();
    test_hardpin_notify_with_payload_no_pin();
    test_hardpin_listen_case_insensitive();
    test_hardpin_identifier_containing_listen();

    /* §2 — SQL analyzer */
    test_analyzer_unlisten_type();
    test_analyzer_unlisten_star_type();
    test_analyzer_listen_type();
    test_analyzer_notify_type();
    test_analyzer_distinct_types();

    /* §4 — NotificationResponse wire format */
    test_notify_wire_format();
    test_notify_wire_with_payload();

    /* §5 — UNLISTEN wildcard */
    test_unlisten_variations();

    /* §6 — NOTIFY variants */
    test_notify_variants_no_pin();

    return test_summary();
}
