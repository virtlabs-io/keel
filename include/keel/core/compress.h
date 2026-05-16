/**
 * @file compress.h
 * @brief Wire-protocol and HTTP response compression for KEEL.
 *
 * Provides:
 *   1. A multi-codec compression API supporting zlib (gzip/deflate) and
 *      zstd for cluster wire-protocol payloads.
 *   2. HTTP gzip response helpers used by the Prometheus / management HTTP
 *      server (admin.c).  When a client sends "Accept-Encoding: gzip" the
 *      response body is compressed with gzip, reducing payload size for large
 *      /metrics dumps by 80–90%.
 *
 * Codec selection:
 * ================
 *   KEEL_COMPRESS_NONE  — passthrough (no compression)
 *   KEEL_COMPRESS_GZIP  — zlib gzip format (RFC 1952), always available
 *   KEEL_COMPRESS_ZSTD  — Facebook zstd (preferred for WAN/cross-region);
 *                         only available when KEEL_HAS_ZSTD=1
 *
 * Usage:
 * ======
 *   ssize_t n = keel_compress(codec, src, src_len, dst, dst_len);
 *   ssize_t m = keel_decompress(codec, src, src_len, dst, dst_len);
 *   size_t  b = keel_compress_bound(codec, src_len);
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_COMPRESS_H
#define KEEL_COMPRESS_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Codec type
 * ============================================================================ */

typedef enum keel_compress_codec {
    KEEL_COMPRESS_NONE = 0, /**< No compression                             */
    KEEL_COMPRESS_GZIP,     /**< zlib gzip format (RFC 1952)                */
    KEEL_COMPRESS_ZSTD,     /**< Facebook zstd (fast WAN compression)       */
} keel_compress_codec_t;

/* ============================================================================
 * Unified multi-codec API
 * ============================================================================ */

/**
 * @brief Compress @p src_len bytes using the specified codec.
 *
 * @param codec    Compression codec (GZIP or ZSTD; NONE is a passthrough).
 * @param src      Input buffer.
 * @param src_len  Input length in bytes.
 * @param dst      Output buffer.
 * @param dst_len  Capacity of @p dst.  Must be >= keel_compress_bound().
 * @return         Bytes written to @p dst, or -1 on error.
 */
ssize_t keel_compress(keel_compress_codec_t codec,
                      const void *src, size_t src_len,
                      void       *dst, size_t dst_len);

/**
 * @brief Decompress @p src_len bytes using the specified codec.
 *
 * @param codec    Compression codec used when the data was compressed.
 * @param src      Compressed input buffer.
 * @param src_len  Input length in bytes.
 * @param dst      Output buffer.
 * @param dst_len  Capacity of @p dst.
 * @return         Bytes written to @p dst, or -1 on error.
 */
ssize_t keel_decompress(keel_compress_codec_t codec,
                        const void *src, size_t src_len,
                        void       *dst, size_t dst_len);

/**
 * @brief Upper bound on compressed output size for a given codec and input.
 *
 * @param codec    Target codec.
 * @param src_len  Uncompressed input length.
 * @return         Safe upper bound for the compressed output buffer.
 */
size_t keel_compress_bound(keel_compress_codec_t codec, size_t src_len);

/* ============================================================================
 * Buffer-level compression API
 * ============================================================================ */

/**
 * @brief Compress @p src_len bytes from @p src into @p dst using gzip.
 *
 * The destination buffer must be pre-allocated by the caller.  A safe upper
 * bound for @p dst_len is keel_compress_gzip_bound(@p src_len).
 *
 * @param src      Input buffer.
 * @param src_len  Input length in bytes.
 * @param dst      Output buffer.
 * @param dst_len  Capacity of @p dst in bytes.
 * @return         Number of bytes written to @p dst, or -1 on error.
 */
ssize_t keel_compress_gzip(const void *src, size_t src_len,
                            void       *dst, size_t dst_len);

/**
 * @brief Decompress gzip-encoded @p src_len bytes from @p src into @p dst.
 *
 * @param src      Compressed input buffer.
 * @param src_len  Input length in bytes.
 * @param dst      Output buffer.
 * @param dst_len  Capacity of @p dst in bytes.
 * @return         Number of bytes written to @p dst, or -1 on error.
 */
ssize_t keel_decompress_gzip(const void *src, size_t src_len,
                              void       *dst, size_t dst_len);

/**
 * @brief Return the maximum compressed size for an @p src_len-byte input.
 *
 * Use this to pre-allocate @p dst for keel_compress_gzip().
 *
 * @param src_len  Uncompressed input length.
 * @return         Safe upper bound for compressed output size.
 */
size_t keel_compress_gzip_bound(size_t src_len);

/* ============================================================================
 * zstd codec (available when KEEL_HAS_ZSTD=1)
 * ============================================================================ */

/**
 * @brief Compress @p src_len bytes from @p src into @p dst using zstd.
 *
 * @param src         Input buffer.
 * @param src_len     Input length in bytes.
 * @param dst         Output buffer (must hold >= keel_compress_zstd_bound() bytes).
 * @param dst_len     Capacity of @p dst.
 * @param level       Compression level (1=fastest … 22=best; 0 = default=3).
 * @return            Bytes written to @p dst, or -1 on error.
 */
ssize_t keel_compress_zstd(const void *src, size_t src_len,
                            void       *dst, size_t dst_len,
                            int         level);

/**
 * @brief Decompress zstd-encoded @p src_len bytes from @p src into @p dst.
 *
 * @param src      Compressed input buffer.
 * @param src_len  Input length in bytes.
 * @param dst      Output buffer.
 * @param dst_len  Capacity of @p dst.
 * @return         Bytes written to @p dst, or -1 on error.
 */
ssize_t keel_decompress_zstd(const void *src, size_t src_len,
                              void       *dst, size_t dst_len);

/**
 * @brief Return the maximum compressed size for a zstd-encoded @p src_len input.
 *
 * @param src_len  Uncompressed input length.
 * @return         Safe upper bound for the compressed output buffer.
 */
size_t keel_compress_zstd_bound(size_t src_len);

/* ============================================================================
 * HTTP helper
 * ============================================================================ */

/**
 * @brief Check whether an HTTP request advertises gzip support.
 *
 * Scans @p request_buf for an @c Accept-Encoding header that contains
 * @c gzip.  The scan is case-insensitive and stops at the first blank line
 * (end of headers).
 *
 * @param request_buf   Raw HTTP request bytes (may be nul-terminated or not).
 * @param request_len   Number of bytes to scan.
 * @return              true if the client accepts gzip encoding.
 */
bool keel_http_accepts_gzip(const char *request_buf, size_t request_len);

/**
 * @brief Build an HTTP/1.1 200 response with an optionally gzip-compressed body.
 *
 * If @p compress is true and the body can be compressed, the function writes
 * a gzip-encoded body with @c Content-Encoding: gzip.  Otherwise the body is
 * written uncompressed.
 *
 * The function writes the complete response (headers + body) to @p fd.
 *
 * @param fd           Destination socket file descriptor.
 * @param content_type MIME type string (e.g. "text/plain; version=0.0.4").
 * @param body         Uncompressed response body.
 * @param body_len     Length of @p body in bytes.
 * @param compress     Whether to attempt gzip compression.
 * @return             0 on success, -1 on I/O error.
 */
int keel_http_send_response(int         fd,
                             const char *content_type,
                             const void *body,
                             size_t      body_len,
                             bool        compress);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_COMPRESS_H */
