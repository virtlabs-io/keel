/**
 * @file error.h
 * @brief Error handling for KEEL
 *
 * KEEL uses a consistent error handling approach:
 * - Functions return keel_error_t (error code)
 * - KEEL_OK (0) indicates success
 * - Negative values indicate errors
 * - Error context can be retrieved per-thread
 */

#ifndef KEEL_ERROR_H
#define KEEL_ERROR_H

#include "keel_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Error Codes
 * ============================================================================ */

/**
 * @brief Error code type
 *
 * All KEEL functions that can fail return this type.
 * Zero (KEEL_OK) indicates success, negative values are errors.
 */
typedef int32_t keel_error_t;

/** Success */
#define KEEL_OK                      ((keel_error_t)0)

/* General errors (-1 to -99) */
#define KEEL_ERR_UNKNOWN             ((keel_error_t)-1)
#define KEEL_ERR_NOMEM               ((keel_error_t)-2)
#define KEEL_ERR_INVALID_ARG         ((keel_error_t)-3)
#define KEEL_ERR_NULL_PTR            ((keel_error_t)-4)
#define KEEL_ERR_BUFFER_TOO_SMALL    ((keel_error_t)-5)
#define KEEL_ERR_NOT_FOUND           ((keel_error_t)-6)
#define KEEL_ERR_ALREADY_EXISTS      ((keel_error_t)-7)
#define KEEL_ERR_TIMEOUT             ((keel_error_t)-8)
#define KEEL_ERR_CANCELLED           ((keel_error_t)-9)
#define KEEL_ERR_NOT_SUPPORTED       ((keel_error_t)-10)
#define KEEL_ERR_NOT_INITIALIZED     ((keel_error_t)-11)
#define KEEL_ERR_ALREADY_INITIALIZED ((keel_error_t)-12)
#define KEEL_ERR_BUSY                ((keel_error_t)-13)
#define KEEL_ERR_OVERFLOW            ((keel_error_t)-14)
#define KEEL_ERR_UNDERFLOW           ((keel_error_t)-15)
#define KEEL_ERR_INVALID_STATE       ((keel_error_t)-16)
#define KEEL_ERR_WOULD_BLOCK         ((keel_error_t)-17)

/* I/O errors (-100 to -199) */
#define KEEL_ERR_IO                  ((keel_error_t)-100)
#define KEEL_ERR_IO_READ             ((keel_error_t)-101)
#define KEEL_ERR_IO_WRITE            ((keel_error_t)-102)
#define KEEL_ERR_IO_CLOSED           ((keel_error_t)-103)
#define KEEL_ERR_IO_RESET            ((keel_error_t)-104)
#define KEEL_ERR_IO_REFUSED          ((keel_error_t)-105)
#define KEEL_ERR_IO_EOF              ((keel_error_t)-106)
#define KEEL_ERR_IO_AGAIN            ((keel_error_t)-107)
#define KEEL_ERR_SOCKET              ((keel_error_t)-108)
#define KEEL_ERR_BIND                ((keel_error_t)-109)
#define KEEL_ERR_LISTEN              ((keel_error_t)-110)
#define KEEL_ERR_CONNECT             ((keel_error_t)-111)
#define KEEL_ERR_ACCEPT              ((keel_error_t)-112)
#define KEEL_ERR_DNS                 ((keel_error_t)-113)

/* Protocol errors (-200 to -299) */
#define KEEL_ERR_PROTOCOL            ((keel_error_t)-200)
#define KEEL_ERR_PROTOCOL_PARSE      ((keel_error_t)-201)
#define KEEL_ERR_PROTOCOL_VERSION    ((keel_error_t)-202)
#define KEEL_ERR_PROTOCOL_INVALID    ((keel_error_t)-203)
#define KEEL_ERR_PROTOCOL_UNEXPECTED ((keel_error_t)-204)
#define KEEL_ERR_PROTOCOL_INCOMPLETE ((keel_error_t)-205)

/* Authentication errors (-300 to -399) */
#define KEEL_ERR_AUTH                ((keel_error_t)-300)
#define KEEL_ERR_AUTH_FAILED         ((keel_error_t)-301)
#define KEEL_ERR_AUTH_DENIED         ((keel_error_t)-302)
#define KEEL_ERR_AUTH_EXPIRED        ((keel_error_t)-303)
#define KEEL_ERR_AUTH_METHOD         ((keel_error_t)-304)

/* Pool errors (-400 to -499) */
#define KEEL_ERR_POOL                ((keel_error_t)-400)
#define KEEL_ERR_POOL_EXHAUSTED      ((keel_error_t)-401)
#define KEEL_ERR_POOL_TIMEOUT        ((keel_error_t)-402)
#define KEEL_ERR_POOL_CLOSED         ((keel_error_t)-403)
#define KEEL_ERR_POOL_CONFIG         ((keel_error_t)-404)
#define KEEL_ERR_UNAVAILABLE         ((keel_error_t)-405)

/* SQL errors (-500 to -599) */
#define KEEL_ERR_SQL                 ((keel_error_t)-500)
#define KEEL_ERR_SQL_PARSE           ((keel_error_t)-501)
#define KEEL_ERR_SQL_SYNTAX          ((keel_error_t)-502)

/* Database errors (-600 to -699) */
#define KEEL_ERR_DB                  ((keel_error_t)-600)
#define KEEL_ERR_DB_CONNECT          ((keel_error_t)-601)
#define KEEL_ERR_DB_QUERY            ((keel_error_t)-602)
#define KEEL_ERR_DB_TRANSACTION      ((keel_error_t)-603)

/* TLS errors (-700 to -799) */
#define KEEL_ERR_TLS                 ((keel_error_t)-700)
#define KEEL_ERR_TLS_HANDSHAKE       ((keel_error_t)-701)
#define KEEL_ERR_TLS_CERT            ((keel_error_t)-702)
#define KEEL_ERR_TLS_KEY             ((keel_error_t)-703)

/* Routing errors */
#define KEEL_ERR_ROUTE               ((keel_error_t)-800)
#define KEEL_ERR_SHARD_CROSS_TX      ((keel_error_t)-801) /**< Single-shard query touches non-participating shard in scatter transaction */

/* Cache errors (-800 to -899) */
#define KEEL_CACHE_MISS              ((keel_error_t)-800)
#define KEEL_CACHE_NON_CACHEABLE     ((keel_error_t)-801)

/* Tier 5 routing / pool errors (-900 to -999) */
#define KEEL_ERR_QUERY_TIMEOUT       ((keel_error_t)-901) /**< Query exceeded configured timeout */

/* ============================================================================
 * Error Handling Macros
 * ============================================================================ */

/** Check if error code indicates success */
#define KEEL_IS_OK(err)    ((err) == KEEL_OK)

/** Check if error code indicates failure */
#define KEEL_IS_ERR(err)   ((err) < KEEL_OK)

/** Return early if expression returns error */
#define KEEL_TRY(expr) \
    do { \
        keel_error_t _err = (expr); \
        if (KEEL_IS_ERR(_err)) { \
            return _err; \
        } \
    } while (0)

/** Goto cleanup label if expression returns error */
#define KEEL_TRY_GOTO(expr, label) \
    do { \
        keel_error_t _err = (expr); \
        if (KEEL_IS_ERR(_err)) { \
            goto label; \
        } \
    } while (0)

/** Set result variable and goto cleanup if expression returns error */
#define KEEL_TRY_SET(expr, result_var, label) \
    do { \
        result_var = (expr); \
        if (KEEL_IS_ERR(result_var)) { \
            goto label; \
        } \
    } while (0)

/* ============================================================================
 * Error Context
 * ============================================================================ */

/**
 * @brief Extended error information
 *
 * This structure provides detailed error context beyond the error code.
 * It is stored per-thread and can be retrieved after a function returns
 * an error.
 */
typedef struct keel_error_ctx {
    keel_error_t code;           /**< Error code */
    int         os_errno;       /**< OS errno at time of error */
    const char* file;           /**< Source file where error occurred */
    int         line;           /**< Line number where error occurred */
    const char* func;           /**< Function where error occurred */
    char        message[256];   /**< Human-readable error message */
} keel_error_ctx_t;

/**
 * @brief Get current thread's error context
 *
 * @return Pointer to thread-local error context (never NULL)
 */
const keel_error_ctx_t* keel_error_get(void);

/**
 * @brief Clear current thread's error context
 */
void keel_error_clear(void);

/**
 * @brief Get human-readable name for error code
 *
 * @param err Error code
 * @return Static string describing the error (never NULL)
 */
KEEL_PURE const char* keel_error_name(keel_error_t err);

/**
 * @brief Get human-readable description for error code
 *
 * @param err Error code
 * @return Static string with error description (never NULL)
 */
KEEL_PURE const char* keel_error_desc(keel_error_t err);

/**
 * @brief Format error context to string
 *
 * @param ctx   Error context
 * @param buf   Output buffer
 * @param size  Buffer size
 * @return Number of characters written (excluding null terminator)
 */
size_t keel_error_format(const keel_error_ctx_t* ctx, char* buf, size_t size);

/* ============================================================================
 * Internal Error Setting (used by implementation)
 * ============================================================================ */

/**
 * @brief Set error context (internal use)
 *
 * This macro captures file/line/function information automatically.
 */
#define keel_error_set(code, ...) \
    keel_error_set_internal((code), __FILE__, __LINE__, __func__, __VA_ARGS__)

/**
 * @brief Set error context with printf-style message (internal)
 */
KEEL_PRINTF_FMT(5, 6)
void keel_error_set_internal(
    keel_error_t code,
    const char* file,
    int         line,
    const char* func,
    const char* fmt,
    ...
);

/**
 * @brief Set error from OS errno (internal)
 */
#define keel_error_set_errno(code) \
    keel_error_set_errno_internal((code), __FILE__, __LINE__, __func__)

/**
 * @brief Set error context from the current OS errno (internal implementation).
 *
 * Captures the calling thread's errno, maps @p code to its string name, and
 * stores both in the thread-local error context.  Normally invoked through the
 * keel_error_set_errno() macro which supplies file/line/func automatically.
 *
 * @param code  KEEL error code to record.
 * @param file  Source file (supplied by macro).
 * @param line  Source line (supplied by macro).
 * @param func  Function name (supplied by macro).
 */
void keel_error_set_errno_internal(
    keel_error_t code,
    const char* file,
    int         line,
    const char* func
);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ERROR_H */
