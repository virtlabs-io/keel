/**
 * @file test_catchup_my_unit.c
 * @brief Unit tests for the MySQL catch-up probe state machine —
 *        pure helpers (Phase 2c).
 *
 * Mirrors test_catchup_pg_unit.c for the MySQL helper layer in
 * worker_catchup_my_helpers.h:
 *   - `my_gtid_token_is_safe`   — defends the COM_QUERY against injection
 *   - `my_token_compare`        — stable total order on GTID-set tokens
 *   - `my_token_satisfied_by`   — exact-match release predicate
 *   - `my_probe_encode_query`   — COM_QUERY framing
 *   - `my_probe_parse_response` — result-set packet parser
 *
 * The full QUERY round (send/recv via the reactor) is exercised in
 * test_catchup_my_mock.c via a socketpair; the live-MySQL e2e lives in
 * test_catchup_my_e2e.c.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

#include "../src/worker/worker_catchup_my_helpers.h"

#include "keel/plugin/plugin_types.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

static keel_consistency_token_t make_token(const char* v, uint32_t tl)
{
    keel_consistency_token_t t = {0};
    if (v) strncpy(t.value, v, sizeof t.value - 1);
    t.timeline_id = tl;
    return t;
}

/* ==========================================================================
 * my_gtid_token_is_safe — the injection-defense gate
 * ==========================================================================*/
static void test_my_gtid_token_is_safe(void)
{
    TEST_BEGIN("my_gtid_token_is_safe: accepts GTID-set grammar, rejects injection");

    /* Accept — typical MySQL GTID sets. */
    TEST_ASSERT(my_gtid_token_is_safe("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42"));
    TEST_ASSERT(my_gtid_token_is_safe("11111111-2222-3333-4444-555555555555:1-10,"
                                      "66666666-7777-8888-9999-aaaaaaaaaaaa:1-5"));
    TEST_ASSERT(my_gtid_token_is_safe("0123456789abcdefABCDEF:1"));
    TEST_ASSERT(my_gtid_token_is_safe("0:1"));
    /* Whitespace around set is tolerated (some servers emit space-padded). */
    TEST_ASSERT(my_gtid_token_is_safe("a:1 , b:2"));

    /* Reject. */
    TEST_ASSERT(!my_gtid_token_is_safe(NULL));
    TEST_ASSERT(!my_gtid_token_is_safe(""));
    TEST_ASSERT(!my_gtid_token_is_safe("aa:1'; DROP TABLE t"));   /* quote */
    TEST_ASSERT(!my_gtid_token_is_safe("aa:1; DROP"));            /* semicolon */
    TEST_ASSERT(!my_gtid_token_is_safe("aa:1) OR 1=1"));          /* paren */
    TEST_ASSERT(!my_gtid_token_is_safe("aa/bb"));                 /* slash */
    TEST_ASSERT(!my_gtid_token_is_safe("aa\\bb"));                /* backslash */

    /* Length cap (>500 chars). */
    char too_long[520];
    memset(too_long, 'a', sizeof too_long - 1);
    too_long[sizeof too_long - 1] = '\0';
    TEST_ASSERT(!my_gtid_token_is_safe(too_long));
    TEST_END();
}

/* ==========================================================================
 * my_token_compare — stable total order
 * ==========================================================================*/
static void test_my_token_compare(void)
{
    TEST_BEGIN("my_token_compare: total ordering");

    keel_consistency_token_t a = make_token("aabb:1-10",   1);
    keel_consistency_token_t b = make_token("aabb:1-100",  1);  /* longer */
    keel_consistency_token_t c = make_token("aabb:1-10",   1);
    keel_consistency_token_t d = make_token("aabb:1-10",   2);  /* later tl */

    /* Longer string > shorter. */
    TEST_ASSERT_EQ(my_token_compare(&a, &b), -1);
    TEST_ASSERT_EQ(my_token_compare(&b, &a),  1);
    TEST_ASSERT_EQ(my_token_compare(&a, &c),  0);

    /* Timeline takes precedence. */
    TEST_ASSERT_EQ(my_token_compare(&a, &d), -1);
    TEST_ASSERT_EQ(my_token_compare(&d, &a),  1);

    /* Same length, different bytes → lex tie-break. */
    keel_consistency_token_t x = make_token("aaa:1", 1);
    keel_consistency_token_t y = make_token("bbb:1", 1);
    TEST_ASSERT_EQ(my_token_compare(&x, &y), -1);
    TEST_ASSERT_EQ(my_token_compare(&y, &x),  1);
    TEST_END();
}

/* ==========================================================================
 * my_token_satisfied_by — exact-match predicate (conservative)
 * ==========================================================================*/
static void test_my_token_satisfied_by(void)
{
    TEST_BEGIN("my_token_satisfied_by: exact string + same timeline");

    keel_consistency_token_t reached = make_token("aabb:1-10", 1);

    keel_consistency_token_t same     = make_token("aabb:1-10", 1);
    keel_consistency_token_t shorter  = make_token("aabb:1-5",  1);
    keel_consistency_token_t newtl    = make_token("aabb:1-10", 2);
    keel_consistency_token_t empty    = make_token("",          1);

    TEST_ASSERT(my_token_satisfied_by(&same, &reached));
    /* Conservative: even though "uuid:1-5" is a subset of "uuid:1-10",
     * the helper requires exact equality — the server-side
     * WAIT_FOR_EXECUTED_GTID_SET is the authoritative oracle. */
    TEST_ASSERT(!my_token_satisfied_by(&shorter, &reached));
    /* Different timeline never satisfies. */
    TEST_ASSERT(!my_token_satisfied_by(&newtl, &reached));
    /* Empty waiter token is trivially satisfied. */
    TEST_ASSERT(my_token_satisfied_by(&empty, &reached));
    TEST_END();
}

/* ==========================================================================
 * my_probe_encode_query — wire bytes for COM_QUERY
 * ==========================================================================*/
static void test_my_probe_encode_query(void)
{
    TEST_BEGIN("my_probe_encode_query: framing + safety gate");

    uint8_t buf[1024];
    size_t  len = 0;
    const char* gtid = "aabbccdd-eeff-0011-2233-445566778899:1-42";

    /* Happy path. */
    TEST_ASSERT_EQ(my_probe_encode_query(buf, sizeof buf, gtid, &len), 0);
    /* 4-byte header. */
    uint32_t payload_len = (uint32_t)buf[0]
                         | ((uint32_t)buf[1] << 8)
                         | ((uint32_t)buf[2] << 16);
    TEST_ASSERT_EQ((size_t)(4 + payload_len), len);
    TEST_ASSERT_EQ(buf[3], (uint8_t)0);       /* seq_id = 0 */
    TEST_ASSERT_EQ(buf[4], (uint8_t)0x03);    /* COM_QUERY */
    /* SQL contains the GTID literally and the WAIT call. */
    char sql[1024];
    size_t sql_len = payload_len - 1;
    memcpy(sql, buf + 5, sql_len);
    sql[sql_len] = '\0';
    TEST_ASSERT(strstr(sql, "WAIT_FOR_EXECUTED_GTID_SET") != NULL);
    TEST_ASSERT(strstr(sql, "'aabbccdd-eeff-0011-2233-445566778899:1-42'") != NULL);
    TEST_ASSERT(strstr(sql, ", 0)") != NULL);

    /* Rejects unsafe GTID. */
    TEST_ASSERT_EQ(my_probe_encode_query(buf, sizeof buf, "x';DROP", &len), -1);
    TEST_ASSERT_EQ(my_probe_encode_query(buf, sizeof buf, "", &len), -1);
    TEST_ASSERT_EQ(my_probe_encode_query(buf, sizeof buf, NULL, &len), -1);

    /* Rejects truncation when cap is tiny. */
    uint8_t tiny[8];
    TEST_ASSERT_EQ(my_probe_encode_query(tiny, sizeof tiny, gtid, &len), -1);

    /* NULL outputs. */
    TEST_ASSERT_EQ(my_probe_encode_query(NULL, sizeof buf, gtid, &len), -1);
    TEST_ASSERT_EQ(my_probe_encode_query(buf, sizeof buf, gtid, NULL), -1);
    TEST_END();
}

/* ==========================================================================
 * my_probe_parse_response — packet sequence parser
 * ==========================================================================*/

/** Build a MySQL packet header into @p out (4 bytes). */
static size_t write_pkt(uint8_t* out, uint8_t seq, const uint8_t* body, size_t body_len)
{
    out[0] = (uint8_t)(body_len      );
    out[1] = (uint8_t)(body_len >>  8);
    out[2] = (uint8_t)(body_len >> 16);
    out[3] = seq;
    if (body && body_len) memcpy(out + 4, body, body_len);
    return 4 + body_len;
}

/** Build a 1-column result-set: col_count(1), col_def, EOF, row, EOF. */
static size_t make_resultset(uint8_t* out, char value)
{
    size_t off = 0;
    /* pkt 1: column count = 1 (single varint byte 0x01) */
    uint8_t cc = 0x01;
    off += write_pkt(out + off, 1, &cc, 1);
    /* pkt 2: column definition — a minimal stub: catalog/schema/table/...
     * we just need it to be non-EOF/non-ERR and non-zero-length. */
    uint8_t col_def[16] = "def\0\0\0\0c\0\0\0\0\0\0\0\0";
    off += write_pkt(out + off, 2, col_def, sizeof col_def);
    /* pkt 3: EOF1 — payload starts with 0xFE, len <= 9 (5 bytes typical). */
    uint8_t eof1[5] = {0xFE, 0x00, 0x00, 0x02, 0x00};
    off += write_pkt(out + off, 3, eof1, sizeof eof1);
    /* pkt 4: row — single length-encoded string of length 1. */
    uint8_t row[2] = {0x01, (uint8_t)value};
    off += write_pkt(out + off, 4, row, sizeof row);
    /* pkt 5: EOF2. */
    uint8_t eof2[5] = {0xFE, 0x00, 0x00, 0x02, 0x00};
    off += write_pkt(out + off, 5, eof2, sizeof eof2);
    return off;
}

static void test_parse_response_reached(void)
{
    TEST_BEGIN("my_probe_parse_response: '0' row drives DONE+result=true");

    uint8_t buf[256];
    size_t total = make_resultset(buf, '0');

    int    pkt_idx = 0;
    bool   result = false, valid = false;
    my_probe_parse_status_t st;
    size_t offset = 0;
    int    iters  = 0;

    while (offset < total && iters++ < 20) {
        size_t n = my_probe_parse_response(buf + offset, total - offset,
                                           &pkt_idx, &st, &result, &valid);
        TEST_ASSERT(st != MY_PARSE_ERROR);
        TEST_ASSERT(st != MY_PARSE_NEED_MORE);
        offset += n;
        if (st == MY_PARSE_DONE) break;
    }
    TEST_ASSERT_EQ(st, MY_PARSE_DONE);
    TEST_ASSERT(valid);
    TEST_ASSERT(result);
    TEST_END();
}

static void test_parse_response_not_reached(void)
{
    TEST_BEGIN("my_probe_parse_response: '1' row drives DONE+result=false");

    uint8_t buf[256];
    size_t total = make_resultset(buf, '1');

    int    pkt_idx = 0;
    bool   result = true, valid = false;
    my_probe_parse_status_t st = MY_PARSE_NEED_MORE;
    size_t offset = 0;
    while (offset < total) {
        size_t n = my_probe_parse_response(buf + offset, total - offset,
                                           &pkt_idx, &st, &result, &valid);
        TEST_ASSERT(st != MY_PARSE_ERROR);
        offset += n;
        if (st == MY_PARSE_DONE) break;
    }
    TEST_ASSERT_EQ(st, MY_PARSE_DONE);
    TEST_ASSERT(valid);
    TEST_ASSERT(!result);
    TEST_END();
}

static void test_parse_response_err_packet(void)
{
    TEST_BEGIN("my_probe_parse_response: ERR packet → MY_PARSE_ERROR");

    /* A single ERR packet (0xFF + sqlstate + msg) terminates immediately. */
    uint8_t buf[64];
    uint8_t err_body[16] = {0xFF, 0x34, 0x12, '#', 'H', 'Y', '0', '0', '0', 'o', 'o', 'p', 's'};
    size_t  total = write_pkt(buf, 1, err_body, sizeof err_body);

    int    pkt_idx = 0;
    bool   result = false, valid = false;
    my_probe_parse_status_t st;
    size_t n = my_probe_parse_response(buf, total, &pkt_idx, &st,
                                       &result, &valid);
    TEST_ASSERT_EQ(st, MY_PARSE_ERROR);
    TEST_ASSERT_EQ(n, total);
    TEST_END();
}

static void test_parse_response_need_more(void)
{
    TEST_BEGIN("my_probe_parse_response: truncated header → NEED_MORE");

    uint8_t buf[256];
    size_t  total = make_resultset(buf, '0');
    int    pkt_idx = 0;
    bool   result = false, valid = false;
    my_probe_parse_status_t st;

    /* Less than 4 bytes — pure header truncation. */
    size_t n = my_probe_parse_response(buf, 3, &pkt_idx, &st, &result, &valid);
    TEST_ASSERT_EQ(st, MY_PARSE_NEED_MORE);
    TEST_ASSERT_EQ(n, (size_t)0);

    /* Header present but body short. */
    n = my_probe_parse_response(buf, 4, &pkt_idx, &st, &result, &valid);
    /* pkt 1 is the col-count packet (1-byte body). 4 header bytes alone
     * means body not yet read, so still NEED_MORE. */
    TEST_ASSERT_EQ(st, MY_PARSE_NEED_MORE);

    /* Just enough to parse the first packet. */
    n = my_probe_parse_response(buf, 5, &pkt_idx, &st, &result, &valid);
    TEST_ASSERT_EQ(st, MY_PARSE_CONSUMED);
    TEST_ASSERT_EQ(n, (size_t)5);
    TEST_ASSERT_EQ(pkt_idx, 1);
    (void)total;
    TEST_END();
}

static void test_parse_response_null_value(void)
{
    TEST_BEGIN("my_probe_parse_response: NULL row → result=false, DONE");

    /* Build a result-set where the row's first byte is 0xFB (NULL). */
    uint8_t buf[256];
    size_t  off = 0;
    uint8_t cc = 0x01;
    off += write_pkt(buf + off, 1, &cc, 1);
    uint8_t col_def[16] = "def\0\0\0\0c\0\0\0\0\0\0\0\0";
    off += write_pkt(buf + off, 2, col_def, sizeof col_def);
    uint8_t eof1[5] = {0xFE, 0, 0, 2, 0};
    off += write_pkt(buf + off, 3, eof1, sizeof eof1);
    uint8_t row_null[1] = {0xFB};
    off += write_pkt(buf + off, 4, row_null, sizeof row_null);
    uint8_t eof2[5] = {0xFE, 0, 0, 2, 0};
    off += write_pkt(buf + off, 5, eof2, sizeof eof2);

    int    pkt_idx = 0;
    bool   result = true, valid = false;
    my_probe_parse_status_t st = MY_PARSE_NEED_MORE;
    size_t offset = 0;
    while (offset < off) {
        size_t n = my_probe_parse_response(buf + offset, off - offset,
                                           &pkt_idx, &st, &result, &valid);
        TEST_ASSERT(st != MY_PARSE_ERROR);
        offset += n;
        if (st == MY_PARSE_DONE) break;
    }
    TEST_ASSERT_EQ(st, MY_PARSE_DONE);
    TEST_ASSERT(valid);
    TEST_ASSERT(!result);
    TEST_END();
}

int main(void)
{
    test_my_gtid_token_is_safe();
    test_my_token_compare();
    test_my_token_satisfied_by();
    test_my_probe_encode_query();
    test_parse_response_reached();
    test_parse_response_not_reached();
    test_parse_response_err_packet();
    test_parse_response_need_more();
    test_parse_response_null_value();
    return test_summary();
}
