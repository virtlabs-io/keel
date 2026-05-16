/**
 * @file test_encoding.c
 * @brief Unit tests for keel_hex_encode() and keel_json_escape().
 *
 * Coverage:
 *   §1  hex_encode: empty, single byte, known multi-byte, all 256 bytes.
 *   §2  hex_encode: output is lowercase, no NUL written into output.
 *   §3  json_escape: plain ASCII pass-through.
 *   §4  json_escape: the five JSON special chars (" \ / \b \f \n \r \t).
 *   §5  json_escape: C0 control characters (0x00-0x1F excluding handled ones).
 *   §6  json_escape: NULL src → empty output.
 *   §7  json_escape: dst_size=0 → no write, return 0.
 *   §8  json_escape: dst_size=1 → NUL-only string.
 *   §9  json_escape: long input truncated safely within dst_size.
 *   §10 json_escape: output is always NUL-terminated within dst_size.
 *   §11 Fuzz: random binary payloads through both helpers without crash.
 *
 * @author Keel test suite
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/util/encoding.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

/* ============================================================================
 * §1–2  keel_hex_encode
 * ============================================================================ */

static void test_hex_encode_empty(void) {
    TEST_BEGIN("hex_encode: empty input produces no output bytes");

    char dst[1] = {(char)0xAB}; /* sentinel */
    keel_hex_encode(NULL, dst, 0);
    /* No bytes written, sentinel unchanged */
    TEST_ASSERT_EQ(dst[0], (char)0xAB);

    TEST_END();
}

static void test_hex_encode_single_byte(void) {
    TEST_BEGIN("hex_encode: single-byte known values");

    char dst[3] = {0};

    /* 0x00 → "00" */
    uint8_t b00 = 0x00;
    keel_hex_encode(&b00, dst, 1);
    dst[2] = '\0';
    TEST_ASSERT_STR_EQ(dst, "00");

    /* 0xFF → "ff" */
    uint8_t bff = 0xFF;
    keel_hex_encode(&bff, dst, 1);
    dst[2] = '\0';
    TEST_ASSERT_STR_EQ(dst, "ff");

    /* 0xAB → "ab" */
    uint8_t bab = 0xAB;
    keel_hex_encode(&bab, dst, 1);
    dst[2] = '\0';
    TEST_ASSERT_STR_EQ(dst, "ab");

    TEST_END();
}

static void test_hex_encode_multi_byte(void) {
    TEST_BEGIN("hex_encode: multi-byte known payload");

    uint8_t src[4] = {0xDE, 0xAD, 0xBE, 0xEF};
    char dst[9] = {0};
    keel_hex_encode(src, dst, 4);
    dst[8] = '\0';
    TEST_ASSERT_STR_EQ(dst, "deadbeef");

    TEST_END();
}

static void test_hex_encode_lowercase(void) {
    TEST_BEGIN("hex_encode: output is lowercase hex digits");

    uint8_t src[16];
    for (int i = 0; i < 16; i++) src[i] = (uint8_t)(i * 16 + i); /* 0x00,0x11,...0xFF */

    char dst[33] = {0};
    keel_hex_encode(src, dst, 16);
    dst[32] = '\0';

    for (int i = 0; dst[i]; i++) {
        char c = dst[i];
        TEST_ASSERT(
            (c >= '0' && c <= '9') ||
            (c >= 'a' && c <= 'f')
        );
    }

    TEST_END();
}

static void test_hex_encode_all_bytes(void) {
    TEST_BEGIN("hex_encode: all 256 byte values produce valid output");

    uint8_t all[256];
    for (int i = 0; i < 256; i++) all[i] = (uint8_t)i;

    char dst[513] = {0};
    keel_hex_encode(all, dst, 256);
    dst[512] = '\0';

    TEST_ASSERT_EQ(strlen(dst), (size_t)512);

    /* Spot-check first and last */
    TEST_ASSERT(dst[0] == '0' && dst[1] == '0');   /* 0x00 → "00" */
    TEST_ASSERT(dst[510] == 'f' && dst[511] == 'f'); /* 0xFF → "ff" */

    TEST_END();
}

/* ============================================================================
 * §3  keel_json_escape — plain ASCII pass-through
 * ============================================================================ */

static void test_json_escape_plain_ascii(void) {
    TEST_BEGIN("json_escape: plain ASCII strings pass through unchanged");

    char out[256];
    size_t written = keel_json_escape(out, sizeof(out), "hello world");
    TEST_ASSERT_STR_EQ(out, "hello world");
    TEST_ASSERT_EQ(written, strlen("hello world"));

    /* Digits and punctuation that don't need escaping */
    written = keel_json_escape(out, sizeof(out), "abc123!@#$%^&*()_+-=[]{}|;':,./<>?");
    TEST_ASSERT_EQ(strlen(out), written);

    TEST_END();
}

/* ============================================================================
 * §4  JSON special characters
 * ============================================================================ */

static void test_json_escape_special_chars(void) {
    TEST_BEGIN("json_escape: JSON special chars produce correct escapes");

    char out[64];

    /* Double quote */
    keel_json_escape(out, sizeof(out), "\"");
    TEST_ASSERT_STR_EQ(out, "\\\"");

    /* Backslash */
    keel_json_escape(out, sizeof(out), "\\");
    TEST_ASSERT_STR_EQ(out, "\\\\");

    /* Newline */
    keel_json_escape(out, sizeof(out), "\n");
    TEST_ASSERT_STR_EQ(out, "\\n");

    /* Carriage return */
    keel_json_escape(out, sizeof(out), "\r");
    TEST_ASSERT_STR_EQ(out, "\\r");

    /* Tab */
    keel_json_escape(out, sizeof(out), "\t");
    TEST_ASSERT_STR_EQ(out, "\\t");

    /* Backspace — JSON allows both \b and \u0008 */
    keel_json_escape(out, sizeof(out), "\b");
    TEST_ASSERT(strcmp(out, "\\b") == 0 || strcmp(out, "\\u0008") == 0);

    /* Form feed — JSON allows both \f and \u000c */
    keel_json_escape(out, sizeof(out), "\f");
    TEST_ASSERT(strcmp(out, "\\f") == 0 || strcmp(out, "\\u000c") == 0 ||
                strcmp(out, "\\u000C") == 0);

    TEST_END();
}

static void test_json_escape_forward_slash(void) {
    TEST_BEGIN("json_escape: forward slash may be escaped or passed through");

    char out[16];
    keel_json_escape(out, sizeof(out), "/");
    /* JSON spec allows both "/" and "\/" — just require it's valid */
    TEST_ASSERT(out[0] == '/' || (out[0] == '\\' && out[1] == '/'));

    TEST_END();
}

static void test_json_escape_combined(void) {
    TEST_BEGIN("json_escape: combined string with specials");

    char out[256];
    size_t n = keel_json_escape(out, sizeof(out), "line1\nline2\ttab\"quote");
    TEST_ASSERT(n > 0);
    TEST_ASSERT(out[n] == '\0');

    /* Must contain escaped sequences */
    TEST_ASSERT(strstr(out, "\\n")  != NULL);
    TEST_ASSERT(strstr(out, "\\t")  != NULL);
    TEST_ASSERT(strstr(out, "\\\"") != NULL);

    TEST_END();
}

/* ============================================================================
 * §5  C0 control characters
 * ============================================================================ */

static void test_json_escape_c0_controls(void) {
    TEST_BEGIN("json_escape: C0 control chars produce \\uXXXX or known escapes");

    char out[32];
    /* Cover 0x01 through 0x1F (0x00 is tricky as NUL terminator) */
    for (int c = 1; c <= 0x1F; c++) {
        /* Skip the ones with dedicated escapes */
        if (c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t')
            continue;

        char src[2] = {(char)c, '\0'};
        size_t n = keel_json_escape(out, sizeof(out), src);
        TEST_ASSERT(n > 0);
        TEST_ASSERT(out[n] == '\0');
        /* Must be escaped: either \uXXXX or some other form — never raw ctrl char */
        TEST_ASSERT((uint8_t)out[0] > 0x1F || out[0] == '\\');
    }

    TEST_END();
}

/* ============================================================================
 * §6  NULL source
 * ============================================================================ */

static void test_json_escape_null_src(void) {
    TEST_BEGIN("json_escape: NULL src produces empty string");

    char out[16] = {(char)0xFF};
    size_t n = keel_json_escape(out, sizeof(out), NULL);
    TEST_ASSERT_EQ(n, (size_t)0);
    TEST_ASSERT_EQ(out[0], '\0');

    TEST_END();
}

/* ============================================================================
 * §7  dst_size = 0
 * ============================================================================ */

static void test_json_escape_zero_dst(void) {
    TEST_BEGIN("json_escape: dst_size=0 writes nothing and returns 0");

    /* dst may be NULL when size is 0 — must not crash */
    char sentinel = (char)0xFF;
    size_t n = keel_json_escape(&sentinel, 0, "hello");
    TEST_ASSERT_EQ(n, (size_t)0);
    /* sentinel must not be touched */
    TEST_ASSERT_EQ(sentinel, (char)0xFF);

    TEST_END();
}

/* ============================================================================
 * §8  dst_size = 1 → NUL-only
 * ============================================================================ */

static void test_json_escape_size_one(void) {
    TEST_BEGIN("json_escape: dst_size=1 produces empty NUL-terminated string");

    char out[1] = {(char)0xFF};
    size_t n = keel_json_escape(out, 1, "hello");
    TEST_ASSERT_EQ(n, (size_t)0);
    TEST_ASSERT_EQ(out[0], '\0');

    TEST_END();
}

/* ============================================================================
 * §9  Truncation
 * ============================================================================ */

static void test_json_escape_truncation(void) {
    TEST_BEGIN("json_escape: long input truncated within dst_size");

    /* 8 chars output buffer */
    char out[8];
    size_t n = keel_json_escape(out, sizeof(out), "abcdefghijklmnopqrstuvwxyz");
    TEST_ASSERT(n < sizeof(out));
    TEST_ASSERT_EQ(out[n], '\0'); /* always NUL-terminated */
    TEST_ASSERT_EQ(strlen(out), n);

    TEST_END();
}

/* ============================================================================
 * §10  NUL termination guaranteed
 * ============================================================================ */

static void test_json_escape_nul_termination(void) {
    TEST_BEGIN("json_escape: output always NUL-terminated");

    char out[32];
    const char* inputs[] = {
        "", "x", "hello\nworld", "\"quoted\"", "\t\r\n",
        "\x01\x02\x03", NULL
    };
    for (int i = 0; inputs[i]; i++) {
        memset(out, 0xAB, sizeof(out));
        size_t n = keel_json_escape(out, sizeof(out), inputs[i]);
        TEST_ASSERT(n < sizeof(out));
        TEST_ASSERT_EQ(out[n], '\0');
    }

    TEST_END();
}

/* ============================================================================
 * §11  Fuzz: random binary payloads
 * ============================================================================ */

static void test_encoding_fuzz(void) {
    TEST_BEGIN("encoding fuzz: random binary payloads don't crash");

    unsigned seed = 0xBEEFCAFE;
    uint8_t src[64];
    char hex_dst[129];
    char json_dst[512];

    for (int iter = 0; iter < 1000; iter++) {
        size_t src_len = (size_t)(rand_r(&seed) % 64);
        for (size_t i = 0; i < src_len; i++) {
            src[i] = (uint8_t)(rand_r(&seed) & 0xFF);
        }

        /* hex_encode: must not crash */
        if (src_len > 0) {
            keel_hex_encode(src, hex_dst, src_len);
            hex_dst[src_len * 2] = '\0';
            TEST_ASSERT_EQ(strlen(hex_dst), src_len * 2);
        }

        /* json_escape on a NUL-terminated substring of the binary data:
         * we use only the printable prefix to avoid NUL confusion */
        char printable[65];
        for (size_t i = 0; i < src_len; i++) {
            printable[i] = (char)((src[i] % 94) + 33); /* printable ASCII */
        }
        printable[src_len] = '\0';

        size_t n = keel_json_escape(json_dst, sizeof(json_dst), printable);
        TEST_ASSERT(n < sizeof(json_dst));
        TEST_ASSERT_EQ(json_dst[n], '\0');
    }

    TEST_ASSERT(true); /* Survived 1000 random iterations */

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Encoding Tests (hex_encode + json_escape)\n");
    printf("==========================================\n\n");

    keel_mem_init(NULL);

    /* hex_encode */
    test_hex_encode_empty();
    test_hex_encode_single_byte();
    test_hex_encode_multi_byte();
    test_hex_encode_lowercase();
    test_hex_encode_all_bytes();

    /* json_escape */
    test_json_escape_plain_ascii();
    test_json_escape_special_chars();
    test_json_escape_forward_slash();
    test_json_escape_combined();
    test_json_escape_c0_controls();
    test_json_escape_null_src();
    test_json_escape_zero_dst();
    test_json_escape_size_one();
    test_json_escape_truncation();
    test_json_escape_nul_termination();

    /* Fuzz */
    test_encoding_fuzz();

    keel_mem_shutdown();

    return test_summary();
}
