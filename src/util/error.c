/**
 * @file error.c
 * @brief Thread-local error-context storage and formatting helpers.
 *
 * KEEL uses a small thread-local error record to carry richer diagnostic detail
 * than a bare enum return code can express. This lets low-level utility code
 * preserve the cheap explicit return-code style while still capturing source
 * location, `errno`, and a formatted message for later reporting.
 *
 * The model is intentionally simple: each thread has one current context and
 * later failures overwrite earlier ones. That avoids lifetime management and
 * synchronization overhead at the cost of not retaining a historical stack of
 * nested errors.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel_error.h"

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

/* Thread-local error context */
static _Thread_local keel_error_ctx_t tls_error_ctx;

/* ============================================================================
 * Error Context
 * ============================================================================ */

const keel_error_ctx_t* keel_error_get(void) {
    return &tls_error_ctx;
}

void keel_error_clear(void) {
    memset(&tls_error_ctx, 0, sizeof(tls_error_ctx));
}

/* ============================================================================
 * Error Name/Description
 * ============================================================================ */

const char* keel_error_name(keel_error_t err) {
    switch (err) {
        case KEEL_OK:                      return "OK";
        case KEEL_ERR_UNKNOWN:             return "UNKNOWN";
        case KEEL_ERR_NOMEM:               return "NOMEM";
        case KEEL_ERR_INVALID_ARG:         return "INVALID_ARG";
        case KEEL_ERR_NULL_PTR:            return "NULL_PTR";
        case KEEL_ERR_BUFFER_TOO_SMALL:    return "BUFFER_TOO_SMALL";
        case KEEL_ERR_NOT_FOUND:           return "NOT_FOUND";
        case KEEL_ERR_ALREADY_EXISTS:      return "ALREADY_EXISTS";
        case KEEL_ERR_TIMEOUT:             return "TIMEOUT";
        case KEEL_ERR_CANCELLED:           return "CANCELLED";
        case KEEL_ERR_NOT_SUPPORTED:       return "NOT_SUPPORTED";
        case KEEL_ERR_NOT_INITIALIZED:     return "NOT_INITIALIZED";
        case KEEL_ERR_ALREADY_INITIALIZED: return "ALREADY_INITIALIZED";
        case KEEL_ERR_BUSY:                return "BUSY";
        case KEEL_ERR_OVERFLOW:            return "OVERFLOW";
        case KEEL_ERR_UNDERFLOW:           return "UNDERFLOW";
        case KEEL_ERR_INVALID_STATE:       return "INVALID_STATE";
        case KEEL_ERR_WOULD_BLOCK:         return "WOULD_BLOCK";
        case KEEL_ERR_IO:                  return "IO";
        case KEEL_ERR_IO_READ:             return "IO_READ";
        case KEEL_ERR_IO_WRITE:            return "IO_WRITE";
        case KEEL_ERR_IO_CLOSED:           return "IO_CLOSED";
        case KEEL_ERR_IO_RESET:            return "IO_RESET";
        case KEEL_ERR_IO_REFUSED:          return "IO_REFUSED";
        case KEEL_ERR_IO_EOF:              return "IO_EOF";
        case KEEL_ERR_IO_AGAIN:            return "IO_AGAIN";
        case KEEL_ERR_SOCKET:              return "SOCKET";
        case KEEL_ERR_BIND:                return "BIND";
        case KEEL_ERR_LISTEN:              return "LISTEN";
        case KEEL_ERR_CONNECT:             return "CONNECT";
        case KEEL_ERR_ACCEPT:              return "ACCEPT";
        case KEEL_ERR_DNS:                 return "DNS";
        case KEEL_ERR_PROTOCOL:            return "PROTOCOL";
        case KEEL_ERR_PROTOCOL_PARSE:      return "PROTOCOL_PARSE";
        case KEEL_ERR_PROTOCOL_VERSION:    return "PROTOCOL_VERSION";
        case KEEL_ERR_PROTOCOL_INVALID:    return "PROTOCOL_INVALID";
        case KEEL_ERR_PROTOCOL_UNEXPECTED: return "PROTOCOL_UNEXPECTED";
        case KEEL_ERR_PROTOCOL_INCOMPLETE: return "PROTOCOL_INCOMPLETE";
        case KEEL_ERR_AUTH:                return "AUTH";
        case KEEL_ERR_AUTH_FAILED:         return "AUTH_FAILED";
        case KEEL_ERR_AUTH_DENIED:         return "AUTH_DENIED";
        case KEEL_ERR_AUTH_EXPIRED:        return "AUTH_EXPIRED";
        case KEEL_ERR_AUTH_METHOD:         return "AUTH_METHOD";
        case KEEL_ERR_POOL:                return "POOL";
        case KEEL_ERR_POOL_EXHAUSTED:      return "POOL_EXHAUSTED";
        case KEEL_ERR_POOL_TIMEOUT:        return "POOL_TIMEOUT";
        case KEEL_ERR_POOL_CLOSED:         return "POOL_CLOSED";
        case KEEL_ERR_POOL_CONFIG:         return "POOL_CONFIG";
        case KEEL_ERR_UNAVAILABLE:         return "UNAVAILABLE";
        case KEEL_ERR_SQL:                 return "SQL";
        case KEEL_ERR_SQL_PARSE:           return "SQL_PARSE";
        case KEEL_ERR_SQL_SYNTAX:          return "SQL_SYNTAX";
        case KEEL_ERR_DB:                  return "DB";
        case KEEL_ERR_DB_CONNECT:          return "DB_CONNECT";
        case KEEL_ERR_DB_QUERY:            return "DB_QUERY";
        case KEEL_ERR_DB_TRANSACTION:      return "DB_TRANSACTION";
        case KEEL_ERR_TLS:                 return "TLS";
        case KEEL_ERR_TLS_HANDSHAKE:       return "TLS_HANDSHAKE";
        case KEEL_ERR_TLS_CERT:            return "TLS_CERT";
        case KEEL_ERR_TLS_KEY:             return "TLS_KEY";
        default:                          return "UNKNOWN_ERROR";
    }
}

/**
 * @brief Return a human-readable description string for a keel_error_t value.
 *
 * Similar to strerror(3) but for KEEL error codes.  Always returns a
 * non-NULL, statically-allocated string.
 *
 * @param err  Error code to describe.
 * @return NUL-terminated description string.
 */
const char* keel_error_desc(keel_error_t err) {
    switch (err) {
        case KEEL_OK:                      return "Success";
        case KEEL_ERR_UNKNOWN:             return "Unknown error";
        case KEEL_ERR_NOMEM:               return "Out of memory";
        case KEEL_ERR_INVALID_ARG:         return "Invalid argument";
        case KEEL_ERR_NULL_PTR:            return "Null pointer";
        case KEEL_ERR_BUFFER_TOO_SMALL:    return "Buffer too small";
        case KEEL_ERR_NOT_FOUND:           return "Not found";
        case KEEL_ERR_ALREADY_EXISTS:      return "Already exists";
        case KEEL_ERR_TIMEOUT:             return "Operation timed out";
        case KEEL_ERR_CANCELLED:           return "Operation cancelled";
        case KEEL_ERR_NOT_SUPPORTED:       return "Not supported";
        case KEEL_ERR_NOT_INITIALIZED:     return "Not initialized";
        case KEEL_ERR_ALREADY_INITIALIZED: return "Already initialized";
        case KEEL_ERR_BUSY:                return "Resource busy";
        case KEEL_ERR_OVERFLOW:            return "Overflow";
        case KEEL_ERR_UNDERFLOW:           return "Underflow";
        case KEEL_ERR_INVALID_STATE:       return "Invalid state";
        case KEEL_ERR_WOULD_BLOCK:         return "Operation would block";
        case KEEL_ERR_IO:                  return "I/O error";
        case KEEL_ERR_IO_READ:             return "Read error";
        case KEEL_ERR_IO_WRITE:            return "Write error";
        case KEEL_ERR_IO_CLOSED:           return "Connection closed";
        case KEEL_ERR_IO_RESET:            return "Connection reset";
        case KEEL_ERR_IO_REFUSED:          return "Connection refused";
        case KEEL_ERR_IO_EOF:              return "End of file";
        case KEEL_ERR_IO_AGAIN:            return "Try again";
        case KEEL_ERR_SOCKET:              return "Socket error";
        case KEEL_ERR_BIND:                return "Bind failed";
        case KEEL_ERR_LISTEN:              return "Listen failed";
        case KEEL_ERR_CONNECT:             return "Connect failed";
        case KEEL_ERR_ACCEPT:              return "Accept failed";
        case KEEL_ERR_DNS:                 return "DNS resolution failed";
        case KEEL_ERR_PROTOCOL:            return "Protocol error";
        case KEEL_ERR_PROTOCOL_PARSE:      return "Protocol parse error";
        case KEEL_ERR_PROTOCOL_VERSION:    return "Protocol version mismatch";
        case KEEL_ERR_PROTOCOL_INVALID:    return "Invalid protocol message";
        case KEEL_ERR_PROTOCOL_UNEXPECTED: return "Unexpected protocol message";
        case KEEL_ERR_PROTOCOL_INCOMPLETE: return "Incomplete protocol message";
        case KEEL_ERR_AUTH:                return "Authentication error";
        case KEEL_ERR_AUTH_FAILED:         return "Authentication failed";
        case KEEL_ERR_AUTH_DENIED:         return "Access denied";
        case KEEL_ERR_AUTH_EXPIRED:        return "Authentication expired";
        case KEEL_ERR_AUTH_METHOD:         return "Unsupported auth method";
        case KEEL_ERR_POOL:                return "Pool error";
        case KEEL_ERR_POOL_EXHAUSTED:      return "Pool exhausted";
        case KEEL_ERR_POOL_TIMEOUT:        return "Pool timeout";
        case KEEL_ERR_POOL_CLOSED:         return "Pool closed";
        case KEEL_ERR_POOL_CONFIG:         return "Pool configuration error";
        case KEEL_ERR_UNAVAILABLE:         return "Resource unavailable";
        case KEEL_ERR_SQL:                 return "SQL error";
        case KEEL_ERR_SQL_PARSE:           return "SQL parse error";
        case KEEL_ERR_SQL_SYNTAX:          return "SQL syntax error";
        case KEEL_ERR_DB:                  return "Database error";
        case KEEL_ERR_DB_CONNECT:          return "Database connection error";
        case KEEL_ERR_DB_QUERY:            return "Query error";
        case KEEL_ERR_DB_TRANSACTION:      return "Transaction error";
        case KEEL_ERR_TLS:                 return "TLS error";
        case KEEL_ERR_TLS_HANDSHAKE:       return "TLS handshake failed";
        case KEEL_ERR_TLS_CERT:            return "Certificate error";
        case KEEL_ERR_TLS_KEY:             return "Key error";
        default:                          return "Unknown error";
    }
}

/* ============================================================================
 * Error Formatting
 * ============================================================================ */

/**
 * @brief Render an error context into a caller-provided buffer.
 *
 * The output intentionally stays short and log-friendly: a symbolic error name
 * followed by either the stored custom message or the enum's default
 * description.
 *
 * @param ctx Error context to format.
 * @param[out] buf Destination character buffer.
 * @param size Capacity of `buf`.
 * @return Number of characters written, excluding the terminating null byte.
 */
size_t keel_error_format(const keel_error_ctx_t* ctx, char* buf, size_t size) {
    if (!ctx || !buf || size == 0) {
        return 0;
    }
    
    int written = snprintf(buf, size, "[%s] %s",
                           keel_error_name(ctx->code),
                           ctx->message[0] ? ctx->message : keel_error_desc(ctx->code));
    
    if (written < 0) {
        return 0;
    }
    
    return (size_t)written < size ? (size_t)written : size - 1;
}

/* ============================================================================
 * Error Setting (Internal)
 * ============================================================================ */

/**
 * @brief Store a formatted error context in thread-local storage.
 *
 * This helper underpins the public error-setting macros so call sites capture
 * file, line, and function automatically without repeated boilerplate.
 *
 * @param code Error code to store.
 * @param file Source file where the error originated.
 * @param line Source line.
 * @param func Source function.
 * @param fmt Optional `printf`-style message format.
 * @param ... Format arguments for `fmt`.
 * @return
 */
void keel_error_set_internal(
    keel_error_t code,
    const char* file,
    int         line,
    const char* func,
    const char* fmt,
    ...)
{
    tls_error_ctx.code = code;
    tls_error_ctx.os_errno = errno;
    tls_error_ctx.file = file;
    tls_error_ctx.line = line;
    tls_error_ctx.func = func;
    
    if (fmt) {
        va_list args;
        va_start(args, fmt);
        vsnprintf(tls_error_ctx.message, sizeof(tls_error_ctx.message), fmt, args);
        va_end(args);
    } else {
        tls_error_ctx.message[0] = '\0';
    }
}

/**
 * @brief Store an error context whose message is derived from `errno`.
 *
 * `errno` is captured immediately so later library calls cannot clobber the OS
 * failure reason before it is preserved in thread-local state.
 *
 * @param code Error code to store.
 * @param file Source file where the error originated.
 * @param line Source line.
 * @param func Source function.
 * @return
 */
void keel_error_set_errno_internal(
    keel_error_t code,
    const char* file,
    int         line,
    const char* func)
{
    int saved_errno = errno;
    
    tls_error_ctx.code = code;
    tls_error_ctx.os_errno = saved_errno;
    tls_error_ctx.file = file;
    tls_error_ctx.line = line;
    tls_error_ctx.func = func;
    
    snprintf(tls_error_ctx.message, sizeof(tls_error_ctx.message),
             "%s", strerror(saved_errno));
}
