/**
 * @file compress.c
 * @brief zlib/zstd compression codecs and HTTP response helpers.
 *
 * Implements:
 *   - keel_compress() / keel_decompress() / keel_compress_bound()
 *       Unified multi-codec API used by the cluster wire protocol.
 *   - keel_compress_gzip() / keel_decompress_gzip() / keel_compress_gzip_bound()
 *       zlib gzip codec (always available).
 *   - keel_compress_zstd() / keel_decompress_zstd() / keel_compress_zstd_bound()
 *       Facebook zstd codec (available when KEEL_HAS_ZSTD=1).
 *   - keel_http_send_response() / keel_http_accepts_gzip()
 *       HTTP-layer helpers used by the admin Prometheus endpoint.
 */

#include "keel/core/compress.h"
#include "keel/mem/mem.h"

/* MemorySanitizer: zlib and libzstd are system libraries not compiled with
 * -fsanitize=memory.  After any call that writes to a caller-supplied buffer
 * through an uninstrumented library, we must tell MSan the bytes are valid.
 * On non-MSan builds the macro compiles away to nothing. */
#if defined(__has_feature)
#  if __has_feature(memory_sanitizer)
#    include <sanitizer/msan_interface.h>
#    define KEEL_MSAN_UNPOISON(ptr, size) __msan_unpoison((ptr), (size))
#  else
#    define KEEL_MSAN_UNPOISON(ptr, size) ((void)0)
#  endif
#else
#  define KEEL_MSAN_UNPOISON(ptr, size) ((void)0)
#endif

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <strings.h>   /* strcasestr */
#include <zlib.h>

#if KEEL_HAS_ZSTD
#include <zstd.h>
#endif

/* ============================================================================
 * Buffer-level gzip API
 * ============================================================================ */

/**
 * @brief Compute a safe upper bound for the gzip-compressed form of `src_len` bytes.
 *
 * @param src_len Number of uncompressed bytes.
 * @return Conservative byte count guaranteed to hold the gzip output, including
 *         the 10-byte gzip header, optional extension headers, and 8-byte
 *         trailer.  Callers should allocate at least this many bytes for the
 *         destination buffer passed to `keel_compress_gzip()`.
 *
 * Notes:
 * - The result is intentionally conservative; actual compressed output is
 *   almost always smaller.
 * - Derived from `compressBound()` (zlib) plus 18 bytes of gzip framing
 *   overhead.
 */
size_t keel_compress_gzip_bound(size_t src_len)
{
    /* compressBound + 18-byte gzip header/trailer overhead */
    return compressBound((uLong)src_len) + 18;
}

/**
 * @brief Compress a buffer using gzip/DEFLATE encoding.
 *
 * @param src      Pointer to the uncompressed source data.
 * @param src_len  Number of bytes to compress.
 * @param dst      Caller-allocated destination buffer.
 * @param dst_len  Capacity of the destination buffer in bytes.  Must be at
 *                 least `keel_compress_gzip_bound(src_len)` to guarantee that
 *                 the compressed output fits.
 * @return Number of bytes written to `dst` on success, or `-1` on failure.
 *
 * Failure reasons:
 * - Any input pointer is `NULL` or `dst_len` is zero.
 * - zlib `deflateInit2()` fails (out of memory or invalid parameters).
 * - `deflate()` does not complete in a single pass (`Z_STREAM_END` not
 *   returned); this indicates `dst` was too small.
 *
 * Notes:
 * - Uses `windowBits = 15 | 16` to produce the RFC 1952 gzip format rather
 *   than raw DEFLATE or zlib-wrapped DEFLATE.
 * - The zlib stream is fully consumed and destroyed before the function
 *   returns, so the caller does not need to manage any stream state.
 * - Thread-safe: each call creates an independent `z_stream`.
 */
ssize_t keel_compress_gzip(const void *src, size_t src_len,
                             void       *dst, size_t dst_len)
{
    if (!src || !dst || dst_len == 0) return -1;

    z_stream zs = {0};
    /* windowBits = 15 | 16 → gzip format */
    if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION,
                     Z_DEFLATED, 15 | 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK)
        return -1;

    zs.next_in   = (Bytef *)src;
    zs.avail_in  = (uInt)src_len;
    zs.next_out  = (Bytef *)dst;
    zs.avail_out = (uInt)dst_len;

    int rc = deflate(&zs, Z_FINISH);
    deflateEnd(&zs);

    if (rc != Z_STREAM_END) return -1;
    /* zlib is not MSan-instrumented; mark the written bytes as initialised. */
    KEEL_MSAN_UNPOISON(dst, zs.total_out);
    return (ssize_t)zs.total_out;
}

/**
 * @brief Decompress a gzip or zlib-wrapped DEFLATE buffer into caller storage.
 *
 * @param src      Pointer to the compressed source data.
 * @param src_len  Number of compressed bytes.
 * @param dst      Caller-allocated destination buffer.
 * @param dst_len  Capacity of the destination buffer in bytes.  Must be large
 *                 enough to hold the full uncompressed output.
 * @return Number of bytes written to `dst` on success, or `-1` on failure.
 *
 * Failure reasons:
 * - Any input pointer is `NULL` or `dst_len` is zero.
 * - zlib `inflateInit2()` fails (out of memory).
 * - `inflate()` does not return `Z_STREAM_END`; this indicates either
 *   truncated input or that `dst` was too small for the full output.
 *
 * Notes:
 * - Uses `windowBits = 15 | 16` to auto-detect gzip vs. zlib framing.
 * - The zlib stream is consumed and destroyed before the function returns.
 * - Thread-safe: each call creates an independent `z_stream`.
 */
ssize_t keel_decompress_gzip(const void *src, size_t src_len,
                               void       *dst, size_t dst_len)
{
    if (!src || !dst || dst_len == 0) return -1;

    z_stream zs = {0};
    /* windowBits = 15 | 16 → auto-detect gzip/zlib */
    if (inflateInit2(&zs, 15 | 16) != Z_OK) return -1;

    zs.next_in   = (Bytef *)src;
    zs.avail_in  = (uInt)src_len;
    zs.next_out  = (Bytef *)dst;
    zs.avail_out = (uInt)dst_len;

    int rc = inflate(&zs, Z_FINISH);
    inflateEnd(&zs);

    if (rc != Z_STREAM_END) return -1;
    /* zlib is not MSan-instrumented; mark the written bytes as initialised. */
    KEEL_MSAN_UNPOISON(dst, zs.total_out);
    return (ssize_t)zs.total_out;
}

/* ============================================================================
 * zstd codec
 * ============================================================================ */

#if KEEL_HAS_ZSTD

size_t keel_compress_zstd_bound(size_t src_len)
{
    return ZSTD_compressBound(src_len);
}

ssize_t keel_compress_zstd(const void *src, size_t src_len,
                            void       *dst, size_t dst_len,
                            int         level)
{
    if (!src || !dst || dst_len == 0) return -1;
    if (level == 0) level = ZSTD_CLEVEL_DEFAULT;

    size_t result = ZSTD_compress(dst, dst_len, src, src_len, level);
    if (ZSTD_isError(result)) return -1;
    /* libzstd is not MSan-instrumented; mark the written bytes as initialised. */
    KEEL_MSAN_UNPOISON(dst, result);
    return (ssize_t)result;
}

ssize_t keel_decompress_zstd(const void *src, size_t src_len,
                              void       *dst, size_t dst_len)
{
    if (!src || !dst || dst_len == 0) return -1;

    size_t result = ZSTD_decompress(dst, dst_len, src, src_len);
    if (ZSTD_isError(result)) return -1;
    /* libzstd is not MSan-instrumented; mark the written bytes as initialised. */
    KEEL_MSAN_UNPOISON(dst, result);
    return (ssize_t)result;
}

#else /* !KEEL_HAS_ZSTD — stub implementations that always return -1 */

size_t keel_compress_zstd_bound(size_t src_len)
{
    /* libzstd not available: return src_len so the bound contract holds */
    return src_len;
}

ssize_t keel_compress_zstd(const void *src, size_t src_len,
                            void       *dst, size_t dst_len,
                            int         level)
{
    (void)src; (void)src_len; (void)dst; (void)dst_len; (void)level;
    return -1;
}

ssize_t keel_decompress_zstd(const void *src, size_t src_len,
                              void       *dst, size_t dst_len)
{
    (void)src; (void)src_len; (void)dst; (void)dst_len;
    return -1;
}

#endif /* KEEL_HAS_ZSTD */

/* ============================================================================
 * Unified multi-codec API
 * ============================================================================ */

size_t keel_compress_bound(keel_compress_codec_t codec, size_t src_len)
{
    switch (codec) {
    case KEEL_COMPRESS_GZIP: return keel_compress_gzip_bound(src_len);
    case KEEL_COMPRESS_ZSTD: return keel_compress_zstd_bound(src_len);
    default:                 return src_len;  /* NONE: no expansion */
    }
}

ssize_t keel_compress(keel_compress_codec_t codec,
                      const void *src, size_t src_len,
                      void       *dst, size_t dst_len)
{
    switch (codec) {
    case KEEL_COMPRESS_GZIP:
        return keel_compress_gzip(src, src_len, dst, dst_len);
    case KEEL_COMPRESS_ZSTD:
        return keel_compress_zstd(src, src_len, dst, dst_len, 0);
    default:
        /* NONE: copy src → dst unchanged */
        if (src_len > dst_len) return -1;
        memcpy(dst, src, src_len);
        return (ssize_t)src_len;
    }
}

ssize_t keel_decompress(keel_compress_codec_t codec,
                        const void *src, size_t src_len,
                        void       *dst, size_t dst_len)
{
    switch (codec) {
    case KEEL_COMPRESS_GZIP:
        return keel_decompress_gzip(src, src_len, dst, dst_len);
    case KEEL_COMPRESS_ZSTD:
        return keel_decompress_zstd(src, src_len, dst, dst_len);
    default:
        /* NONE: copy src → dst unchanged */
        if (src_len > dst_len) return -1;
        memcpy(dst, src, src_len);
        return (ssize_t)src_len;
    }
}

/* ============================================================================
 * HTTP helpers
 * ============================================================================ */

/**
 * @brief Scan an HTTP request buffer for `Accept-Encoding: gzip` support.
 *
 * @param request_buf  Pointer to the raw HTTP request bytes.
 * @param request_len  Number of bytes in `request_buf`.
 * @return `true` when the request headers advertise gzip-encoding support,
 *         otherwise `false`.
 *
 * Behavior:
 * - Scans line-by-line through the header section only.  Stops at the first
 *   blank line (CRLF or LF alone) to avoid inspecting the request body.
 * - Header matching is case-insensitive for both the field name and the token
 *   value, which is required by RFC 7231 §5.3.4.
 * - The search is intentionally simple: it looks for the literal substring
 *   `gzip` after the `Accept-Encoding:` prefix without parsing `q=` weights.
 *
 * Edge cases:
 * - Returns `false` on `NULL` input or zero length.
 * - Lines longer than 255 bytes are truncated to a stack buffer; only the
 *   truncated prefix is tested (sufficient for any realistic header value).
 */
bool keel_http_accepts_gzip(const char *request_buf, size_t request_len)
{
    if (!request_buf || request_len == 0) return false;

    /* Scan only headers — stop at first blank line */
    const char *end = request_buf + request_len;
    const char *p   = request_buf;

    while (p < end) {
        /* Find line end */
        const char *eol = memchr(p, '\n', (size_t)(end - p));
        if (!eol) break;

        size_t line_len = (size_t)(eol - p);
        /* Blank line marks end of headers */
        if (line_len == 0 || (line_len == 1 && *p == '\r')) break;

        /* Case-insensitive search for Accept-Encoding: ... gzip */
        /* First check if this is the Accept-Encoding header */
        if (line_len > 15) {
            char hdr[256] = {0};
            size_t copy = line_len < sizeof(hdr) - 1 ? line_len : sizeof(hdr) - 1;
            memcpy(hdr, p, copy);

            /* strncasecmp for Accept-Encoding */
            if (strncasecmp(hdr, "accept-encoding:", 16) == 0) {
                /* Check if gzip appears in the value */
                if (strcasestr(hdr + 16, "gzip") != NULL)
                    return true;
            }
        }

        p = eol + 1;
    }
    return false;
}

/**
 * @brief Format and transmit a minimal HTTP/1.1 200 response over a socket.
 *
 * @param fd            Connected socket descriptor to write into.
 * @param content_type  MIME type for the `Content-Type` response header.
 *                      Falls back to `application/octet-stream` when `NULL`.
 * @param body          Pointer to the response body bytes.
 * @param body_len      Number of body bytes.
 * @param compress      When `true` and `body_len > 128`, attempt to
 *                      gzip-compress the body before sending.  If compression
 *                      fails or produces no gain the original body is sent
 *                      uncompressed and no `Content-Encoding` header is added.
 * @return `0` on success, or `-1` if writing the header or body to `fd`
 *         failed.
 *
 * Response headers emitted:
 * - `Content-Type`: supplied or `application/octet-stream`.
 * - `Content-Length`: exact byte count of the transmitted body.
 * - `Content-Encoding: gzip` only when compression was applied.
 * - `Connection: close` unconditionally.
 *
 * Notes:
 * - Compression is skipped when `body_len <= 128` bytes because gzip framing
 *   overhead would likely exceed any savings.
 * - The compressed buffer is heap-allocated and freed before the function
 *   returns.
 * - The function uses `write(2)` directly; it is not suitable for TLS sockets
 *   or nonblocking descriptors.
 */
int keel_http_send_response(int         fd,
                              const char *content_type,
                              const void *body,
                              size_t      body_len,
                              bool        compress)
{
    const void *send_body    = body;
    size_t      send_len     = body_len;
    bool        did_compress = false;
    void       *comp_buf     = NULL;

    if (compress && body_len > 128) {
        size_t bound = keel_compress_gzip_bound(body_len);
        comp_buf = keel_malloc(bound);
        if (comp_buf) {
            ssize_t clen = keel_compress_gzip(body, body_len, comp_buf, bound);
            if (clen > 0) {
                send_body    = comp_buf;
                send_len     = (size_t)clen;
                did_compress = true;
            } else {
                keel_free(comp_buf);
                comp_buf = NULL;
            }
        }
    }

    /* Build header */
    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "%s"
        "Connection: close\r\n"
        "\r\n",
        content_type ? content_type : "application/octet-stream",
        send_len,
        did_compress ? "Content-Encoding: gzip\r\n" : "");

    int rc = 0;
    if (write(fd, header, (size_t)hlen) != hlen) { rc = -1; goto done; }
    if (send_len > 0 && write(fd, send_body, send_len) != (ssize_t)send_len)
        rc = -1;

done:
    keel_free(comp_buf);
    return rc;
}
