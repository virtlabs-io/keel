/**
 * @file test_compress.c
 * @brief Unit tests for the compression codec and HTTP helpers.
 *
 * Tests cover:
 * §1 — keel_compress_gzip_bound()
 * §2 — keel_compress_gzip() — happy paths and error paths
 * §3 — keel_decompress_gzip() — round-trip and error paths
 * §4 — gzip round-trip (compress → decompress) for various inputs
 * §5 — keel_http_accepts_gzip() — request header parsing
 * §6 — keel_http_send_response() — plain and compressed responses
 */

#include "test_utils.h"

#include "keel/core/compress.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

/* ============================================================================
 * §1 — keel_compress_gzip_bound()
 * ============================================================================ */

static void test_bound_zero(void)
{
    TEST_BEGIN("compress bound: bound(0) is positive");
    size_t b = keel_compress_gzip_bound(0);
    TEST_ASSERT(b > 0);
    TEST_END();
}

static void test_bound_small(void)
{
    TEST_BEGIN("compress bound: bound(100) > 100");
    size_t b = keel_compress_gzip_bound(100);
    TEST_ASSERT(b > 100);
    TEST_END();
}

static void test_bound_large(void)
{
    TEST_BEGIN("compress bound: bound(1MB) is sufficient for compressed output");
    size_t src_len = 1024 * 1024;
    size_t b = keel_compress_gzip_bound(src_len);
    /* Must at minimum be able to hold the original (worst-case incompressible) */
    TEST_ASSERT(b >= src_len);
    TEST_END();
}

/* ============================================================================
 * §2 — keel_compress_gzip()
 * ============================================================================ */

static void test_compress_basic(void)
{
    TEST_BEGIN("compress gzip: basic compression returns positive length");
    const char *src = "Hello, World! This is a test of gzip compression.";
    size_t src_len  = strlen(src);
    size_t bound    = keel_compress_gzip_bound(src_len);
    void  *dst      = malloc(bound);
    TEST_ASSERT_NOT_NULL(dst);

    ssize_t n = keel_compress_gzip(src, src_len, dst, bound);
    TEST_ASSERT(n > 0);
    free(dst);
    TEST_END();
}

static void test_compress_empty(void)
{
    TEST_BEGIN("compress gzip: empty input produces valid gzip");
    size_t bound = keel_compress_gzip_bound(0);
    void  *dst   = malloc(bound);
    TEST_ASSERT_NOT_NULL(dst);

    ssize_t n = keel_compress_gzip("", 0, dst, bound);
    TEST_ASSERT(n > 0);   /* gzip header/trailer overhead present */
    free(dst);
    TEST_END();
}

static void test_compress_repetitive_is_smaller(void)
{
    TEST_BEGIN("compress gzip: repetitive data compresses below original size");
    char src[4096];
    memset(src, 'A', sizeof(src));
    size_t bound = keel_compress_gzip_bound(sizeof(src));
    void  *dst   = malloc(bound);
    TEST_ASSERT_NOT_NULL(dst);

    ssize_t n = keel_compress_gzip(src, sizeof(src), dst, bound);
    TEST_ASSERT(n > 0);
    TEST_ASSERT((size_t)n < sizeof(src));  /* repetitive data should compress well */
    free(dst);
    TEST_END();
}

static void test_compress_output_too_small(void)
{
    TEST_BEGIN("compress gzip: buffer too small returns -1");
    const char *src = "Hello World";
    void *tiny = malloc(4);   /* 4 bytes — too small for gzip output */
    TEST_ASSERT_NOT_NULL(tiny);

    ssize_t n = keel_compress_gzip(src, strlen(src), tiny, 4);
    TEST_ASSERT(n == -1);
    free(tiny);
    TEST_END();
}

static void test_compress_null_src(void)
{
    TEST_BEGIN("compress gzip: NULL src returns -1");
    char dst[64];
    ssize_t n = keel_compress_gzip(NULL, 10, dst, sizeof(dst));
    TEST_ASSERT(n == -1);
    TEST_END();
}

static void test_compress_null_dst(void)
{
    TEST_BEGIN("compress gzip: NULL dst returns -1");
    ssize_t n = keel_compress_gzip("hello", 5, NULL, 100);
    TEST_ASSERT(n == -1);
    TEST_END();
}

static void test_compress_zero_dst_len(void)
{
    TEST_BEGIN("compress gzip: zero dst_len returns -1");
    char dst[1];
    ssize_t n = keel_compress_gzip("hello", 5, dst, 0);
    TEST_ASSERT(n == -1);
    TEST_END();
}

static void test_compress_large_input(void)
{
    TEST_BEGIN("compress gzip: 64KB input compresses successfully");
    size_t src_len = 65536;
    char  *src = malloc(src_len);
    TEST_ASSERT_NOT_NULL(src);
    /* Fill with pattern */
    for (size_t i = 0; i < src_len; i++) src[i] = (char)(i & 0xFF);

    size_t bound = keel_compress_gzip_bound(src_len);
    void  *dst   = malloc(bound);
    TEST_ASSERT_NOT_NULL(dst);

    ssize_t n = keel_compress_gzip(src, src_len, dst, bound);
    TEST_ASSERT(n > 0);
    free(src);
    free(dst);
    TEST_END();
}

/* ============================================================================
 * §3 — keel_decompress_gzip()
 * ============================================================================ */

static void test_decompress_null_src(void)
{
    TEST_BEGIN("decompress gzip: NULL src returns -1");
    char out[64];
    ssize_t n = keel_decompress_gzip(NULL, 10, out, sizeof(out));
    TEST_ASSERT(n == -1);
    TEST_END();
}

static void test_decompress_null_dst(void)
{
    TEST_BEGIN("decompress gzip: NULL dst returns -1");
    const char fake[] = "\x1f\x8b";
    ssize_t n = keel_decompress_gzip(fake, sizeof(fake), NULL, 64);
    TEST_ASSERT(n == -1);
    TEST_END();
}

static void test_decompress_invalid_data(void)
{
    TEST_BEGIN("decompress gzip: invalid gzip data returns -1");
    const char junk[] = "this is not gzip data at all !!";
    char out[128];
    ssize_t n = keel_decompress_gzip(junk, sizeof(junk), out, sizeof(out));
    TEST_ASSERT(n == -1);
    TEST_END();
}

static void test_decompress_dst_too_small(void)
{
    TEST_BEGIN("decompress gzip: output buffer too small returns -1");
    const char *src = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    size_t src_len  = strlen(src);
    size_t bound    = keel_compress_gzip_bound(src_len);
    void  *comp     = malloc(bound);
    TEST_ASSERT_NOT_NULL(comp);

    ssize_t clen = keel_compress_gzip(src, src_len, comp, bound);
    TEST_ASSERT(clen > 0);

    char tiny[2];
    ssize_t n = keel_decompress_gzip(comp, (size_t)clen, tiny, sizeof(tiny));
    TEST_ASSERT(n == -1);  /* output too small */
    free(comp);
    TEST_END();
}

/* ============================================================================
 * §4 — Round-trip tests
 * ============================================================================ */

static void test_roundtrip_short_string(void)
{
    TEST_BEGIN("compress roundtrip: short string");
    const char *orig     = "Hello, KEEL!";
    size_t      orig_len = strlen(orig);
    size_t      bound    = keel_compress_gzip_bound(orig_len);

    void *comp = malloc(bound);
    TEST_ASSERT_NOT_NULL(comp);

    ssize_t clen = keel_compress_gzip(orig, orig_len, comp, bound);
    TEST_ASSERT(clen > 0);

    char *out = malloc(orig_len + 1);
    TEST_ASSERT_NOT_NULL(out);
    ssize_t dlen = keel_decompress_gzip(comp, (size_t)clen, out, orig_len + 1);
    TEST_ASSERT(dlen == (ssize_t)orig_len);
    out[dlen] = '\0';
    TEST_ASSERT_STR_EQ(out, orig);

    free(comp);
    free(out);
    TEST_END();
}

static void test_roundtrip_empty(void)
{
    TEST_BEGIN("compress roundtrip: empty input");
    size_t bound = keel_compress_gzip_bound(0);
    void  *comp  = malloc(bound);
    TEST_ASSERT_NOT_NULL(comp);

    ssize_t clen = keel_compress_gzip("", 0, comp, bound);
    TEST_ASSERT(clen > 0);

    char out[16];
    ssize_t dlen = keel_decompress_gzip(comp, (size_t)clen, out, sizeof(out));
    TEST_ASSERT(dlen == 0);

    free(comp);
    TEST_END();
}

static void test_roundtrip_binary_data(void)
{
    TEST_BEGIN("compress roundtrip: binary data with null bytes");
    unsigned char orig[256];
    for (int i = 0; i < 256; i++) orig[i] = (unsigned char)i;
    size_t orig_len = sizeof(orig);
    size_t bound    = keel_compress_gzip_bound(orig_len);

    void *comp = malloc(bound);
    TEST_ASSERT_NOT_NULL(comp);
    ssize_t clen = keel_compress_gzip(orig, orig_len, comp, bound);
    TEST_ASSERT(clen > 0);

    unsigned char *out = malloc(orig_len);
    TEST_ASSERT_NOT_NULL(out);
    ssize_t dlen = keel_decompress_gzip(comp, (size_t)clen, out, orig_len);
    TEST_ASSERT(dlen == (ssize_t)orig_len);
    TEST_ASSERT(memcmp(orig, out, orig_len) == 0);

    free(comp);
    free(out);
    TEST_END();
}

static void test_roundtrip_prometheus_like(void)
{
    TEST_BEGIN("compress roundtrip: Prometheus-style text body");
    const char *body =
        "# HELP keel_connections_total Total connections\n"
        "# TYPE keel_connections_total counter\n"
        "keel_connections_total 12345\n"
        "# HELP keel_queries_total Total queries\n"
        "# TYPE keel_queries_total counter\n"
        "keel_queries_total{db=\"postgres\"} 98765\n";
    size_t body_len = strlen(body);
    size_t bound    = keel_compress_gzip_bound(body_len);

    void *comp = malloc(bound);
    TEST_ASSERT_NOT_NULL(comp);
    ssize_t clen = keel_compress_gzip(body, body_len, comp, bound);
    TEST_ASSERT(clen > 0);

    char *out = malloc(body_len + 1);
    TEST_ASSERT_NOT_NULL(out);
    ssize_t dlen = keel_decompress_gzip(comp, (size_t)clen, out, body_len + 1);
    TEST_ASSERT(dlen == (ssize_t)body_len);
    out[dlen] = '\0';
    TEST_ASSERT_STR_EQ(out, body);

    free(comp);
    free(out);
    TEST_END();
}

static void test_roundtrip_large(void)
{
    TEST_BEGIN("compress roundtrip: 256KB structured data");
    size_t src_len = 256 * 1024;
    char  *src     = malloc(src_len);
    TEST_ASSERT_NOT_NULL(src);
    /* Structured repeated pattern — compresses well */
    const char *pat = "keel_pool_queries_total{db=\"app\",user=\"app\"} ";
    size_t plen = strlen(pat);
    for (size_t i = 0; i < src_len; i++) src[i] = pat[i % plen];

    size_t bound = keel_compress_gzip_bound(src_len);
    void  *comp  = malloc(bound);
    TEST_ASSERT_NOT_NULL(comp);
    ssize_t clen = keel_compress_gzip(src, src_len, comp, bound);
    TEST_ASSERT(clen > 0);
    TEST_ASSERT((size_t)clen < src_len);  /* must compress smaller */

    char *out = malloc(src_len);
    TEST_ASSERT_NOT_NULL(out);
    ssize_t dlen = keel_decompress_gzip(comp, (size_t)clen, out, src_len);
    TEST_ASSERT(dlen == (ssize_t)src_len);
    TEST_ASSERT(memcmp(src, out, src_len) == 0);

    free(src);
    free(comp);
    free(out);
    TEST_END();
}

/* ============================================================================
 * §5 — keel_http_accepts_gzip()
 * ============================================================================ */

static void test_accepts_gzip_yes(void)
{
    TEST_BEGIN("http accepts_gzip: standard header");
    const char *req =
        "GET /metrics HTTP/1.1\r\n"
        "Host: localhost:9191\r\n"
        "Accept-Encoding: gzip, deflate\r\n"
        "Connection: close\r\n"
        "\r\n";
    TEST_ASSERT(keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_no(void)
{
    TEST_BEGIN("http accepts_gzip: no gzip in Accept-Encoding");
    const char *req =
        "GET /metrics HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Accept-Encoding: identity\r\n"
        "\r\n";
    TEST_ASSERT(!keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_missing_header(void)
{
    TEST_BEGIN("http accepts_gzip: no Accept-Encoding header");
    const char *req =
        "GET /metrics HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Connection: close\r\n"
        "\r\n";
    TEST_ASSERT(!keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_case_insensitive_header(void)
{
    TEST_BEGIN("http accepts_gzip: header name is case-insensitive");
    const char *req =
        "GET / HTTP/1.1\r\n"
        "ACCEPT-ENCODING: gzip\r\n"
        "\r\n";
    TEST_ASSERT(keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_gzip_only(void)
{
    TEST_BEGIN("http accepts_gzip: gzip-only Accept-Encoding");
    const char *req =
        "GET / HTTP/1.1\r\n"
        "Accept-Encoding: gzip\r\n"
        "\r\n";
    TEST_ASSERT(keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_with_quality(void)
{
    TEST_BEGIN("http accepts_gzip: gzip with quality value");
    const char *req =
        "GET / HTTP/1.1\r\n"
        "Accept-Encoding: deflate, gzip;q=1.0, *;q=0.5\r\n"
        "\r\n";
    TEST_ASSERT(keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_null_safe(void)
{
    TEST_BEGIN("http accepts_gzip: NULL is safe → false");
    TEST_ASSERT(!keel_http_accepts_gzip(NULL, 0));
    TEST_ASSERT(!keel_http_accepts_gzip(NULL, 100));
    TEST_ASSERT(!keel_http_accepts_gzip("GET /\r\n\r\n", 0));
    TEST_END();
}

static void test_accepts_gzip_partial_header(void)
{
    TEST_BEGIN("http accepts_gzip: truncated request — no crash");
    const char *req = "GET /metrics HTTP/1.1\r\nAcce";   /* truncated */
    TEST_ASSERT(!keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

static void test_accepts_gzip_body_not_scanned(void)
{
    TEST_BEGIN("http accepts_gzip: gzip in body not mistaken for header");
    const char *req =
        "POST /api HTTP/1.1\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        "Accept-Encoding: gzip\r\n";  /* in body, not headers */
    TEST_ASSERT(!keel_http_accepts_gzip(req, strlen(req)));
    TEST_END();
}

/* ============================================================================
 * §6 — keel_http_send_response() via pipe
 * ============================================================================ */

/** Read all available data from fd into a malloc'd buffer. Returns length. */
static ssize_t drain_fd(int fd, char **buf_out)
{
    char  tmp[8192];
    char *buf    = NULL;
    size_t total = 0;
    ssize_t n;
    while ((n = read(fd, tmp, sizeof(tmp))) > 0) {
        char *nb = realloc(buf, total + (size_t)n + 1);
        if (!nb) break;
        buf = nb;
        memcpy(buf + total, tmp, (size_t)n);
        total += (size_t)n;
        buf[total] = '\0';
    }
    *buf_out = buf;
    return (ssize_t)total;
}

static void test_send_response_plain(void)
{
    TEST_BEGIN("http send_response: plain (no compression) response");
    int fds[2];
    if (pipe(fds) != 0) { TEST_ASSERT(0 && "pipe() failed"); return; }

    const char *body = "test body data";
    int rc = keel_http_send_response(fds[1], "text/plain", body, strlen(body), false);
    close(fds[1]);
    TEST_ASSERT_EQ(rc, 0);

    char *resp = NULL;
    ssize_t len = drain_fd(fds[0], &resp);
    close(fds[0]);
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(resp, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(resp, "Content-Type: text/plain") != NULL);
    TEST_ASSERT(strstr(resp, "test body data") != NULL);
    TEST_ASSERT(strstr(resp, "Content-Encoding") == NULL);
    free(resp);
    TEST_END();
}

static void test_send_response_compressed(void)
{
    TEST_BEGIN("http send_response: compressed response has gzip encoding header");
    int fds[2];
    if (pipe(fds) != 0) { TEST_ASSERT(0 && "pipe() failed"); return; }

    /* Long enough body to trigger compression (>128 bytes) */
    char body[512];
    memset(body, 'X', sizeof(body) - 1);
    body[sizeof(body) - 1] = '\0';

    int rc = keel_http_send_response(fds[1], "text/plain", body, sizeof(body) - 1, true);
    close(fds[1]);
    TEST_ASSERT_EQ(rc, 0);

    char *resp = NULL;
    ssize_t len = drain_fd(fds[0], &resp);
    close(fds[0]);
    TEST_ASSERT(len > 0);
    TEST_ASSERT(strstr(resp, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(resp, "Content-Encoding: gzip") != NULL);
    free(resp);
    TEST_END();
}

static void test_send_response_small_body_not_compressed(void)
{
    TEST_BEGIN("http send_response: body <=128 bytes not compressed even if requested");
    int fds[2];
    if (pipe(fds) != 0) { TEST_ASSERT(0 && "pipe() failed"); return; }

    const char *body = "tiny";
    int rc = keel_http_send_response(fds[1], "text/plain", body, strlen(body), true);
    close(fds[1]);
    TEST_ASSERT_EQ(rc, 0);

    char *resp = NULL;
    ssize_t len = drain_fd(fds[0], &resp);
    close(fds[0]);
    TEST_ASSERT(len > 0);
    /* Small body: no Content-Encoding */
    TEST_ASSERT(strstr(resp, "Content-Encoding") == NULL);
    TEST_ASSERT(strstr(resp, "tiny") != NULL);
    free(resp);
    TEST_END();
}

static void test_send_response_content_length_correct(void)
{
    TEST_BEGIN("http send_response: Content-Length matches body in response");
    int fds[2];
    if (pipe(fds) != 0) { TEST_ASSERT(0 && "pipe() failed"); return; }

    const char *body = "Hello World";
    size_t blen = strlen(body);
    keel_http_send_response(fds[1], "text/plain", body, blen, false);
    close(fds[1]);

    char *resp = NULL;
    drain_fd(fds[0], &resp);
    close(fds[0]);

    char expected_cl[64];
    snprintf(expected_cl, sizeof(expected_cl), "Content-Length: %zu", blen);
    TEST_ASSERT(strstr(resp, expected_cl) != NULL);
    free(resp);
    TEST_END();
}

static void test_send_response_connection_close(void)
{
    TEST_BEGIN("http send_response: Connection: close header present");
    int fds[2];
    if (pipe(fds) != 0) { TEST_ASSERT(0 && "pipe() failed"); return; }

    keel_http_send_response(fds[1], "text/plain", "hi", 2, false);
    close(fds[1]);

    char *resp = NULL;
    drain_fd(fds[0], &resp);
    close(fds[0]);
    TEST_ASSERT(strstr(resp, "Connection: close") != NULL);
    free(resp);
    TEST_END();
}

static void test_send_response_content_type_set(void)
{
    TEST_BEGIN("http send_response: custom content-type is set");
    int fds[2];
    if (pipe(fds) != 0) { TEST_ASSERT(0 && "pipe() failed"); return; }

    keel_http_send_response(fds[1],
        "application/json; charset=utf-8",
        "{}", 2, false);
    close(fds[1]);

    char *resp = NULL;
    drain_fd(fds[0], &resp);
    close(fds[0]);
    TEST_ASSERT(strstr(resp, "Content-Type: application/json; charset=utf-8") != NULL);
    free(resp);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    /* §1 Bound */
    test_bound_zero();
    test_bound_small();
    test_bound_large();

    /* §2 Compress */
    test_compress_basic();
    test_compress_empty();
    test_compress_repetitive_is_smaller();
    test_compress_output_too_small();
    test_compress_null_src();
    test_compress_null_dst();
    test_compress_zero_dst_len();
    test_compress_large_input();

    /* §3 Decompress */
    test_decompress_null_src();
    test_decompress_null_dst();
    test_decompress_invalid_data();
    test_decompress_dst_too_small();

    /* §4 Round-trips */
    test_roundtrip_short_string();
    test_roundtrip_empty();
    test_roundtrip_binary_data();
    test_roundtrip_prometheus_like();
    test_roundtrip_large();

    /* §5 Accept-Encoding parsing */
    test_accepts_gzip_yes();
    test_accepts_gzip_no();
    test_accepts_gzip_missing_header();
    test_accepts_gzip_case_insensitive_header();
    test_accepts_gzip_gzip_only();
    test_accepts_gzip_with_quality();
    test_accepts_gzip_null_safe();
    test_accepts_gzip_partial_header();
    test_accepts_gzip_body_not_scanned();

    /* §6 HTTP send_response */
    test_send_response_plain();
    test_send_response_compressed();
    test_send_response_small_body_not_compressed();
    test_send_response_content_length_correct();
    test_send_response_connection_close();
    test_send_response_content_type_set();

    return test_summary();
}
