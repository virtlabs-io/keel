/**
 * @file log.h
 * @brief Public API for KEEL's structured logging core, filters, and emission macros.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * The logging core is the lowest common denominator used across the rest of
 * KEEL. It provides category filtering, severity filtering, source-location
 * capture, and a macro layer that avoids formatting costs when messages are
 * disabled. Higher-level components such as query logging and pluggable sinks
 * build on top of this API rather than bypassing it.
 *
 * Design choices:
 *   - Severity/category checks are cheap and intended to guard expensive
 *     formatting or SQL serialization work in hot paths.
 *   - File, line, and function are captured by macros so call sites remain
 *     compact while retaining debugging value.
 *   - The logging core separates policy from transport: `log.h` defines what a
 *     record means and when to emit it; plugin interfaces define where it goes.
 */

#ifndef KEEL_LOG_H
#define KEEL_LOG_H

#include "keel_types.h"
#include "keel_error.h"

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Log Levels
 * ============================================================================ */

/**
 * @brief Log severity levels
 */
typedef enum keel_log_level {
    KEEL_LOG_TRACE = 0,      /**< Detailed trace information */
    KEEL_LOG_DEBUG = 1,      /**< Debug information */
    KEEL_LOG_INFO  = 2,      /**< Informational messages */
    KEEL_LOG_WARN  = 3,      /**< Warning conditions */
    KEEL_LOG_ERROR = 4,      /**< Error conditions */
    KEEL_LOG_FATAL = 5,      /**< Fatal errors (application will exit) */
    KEEL_LOG_OFF   = 6,      /**< Logging disabled */
} keel_log_level_t;

/* ============================================================================
 * Log Categories
 * ============================================================================ */

/**
 * @brief Log categories for filtering
 */
typedef enum keel_log_category {
    KEEL_LOG_CAT_CORE    = (1 << 0),     /**< Core pooler logic */
    KEEL_LOG_CAT_POOL    = (1 << 1),     /**< Connection pool */
    KEEL_LOG_CAT_CONN    = (1 << 2),     /**< Connection management */
    KEEL_LOG_CAT_IO      = (1 << 3),     /**< I/O layer */
    KEEL_LOG_CAT_PROTO   = (1 << 4),     /**< Protocol layer */
    KEEL_LOG_CAT_AUTH    = (1 << 5),     /**< Authentication */
    KEEL_LOG_CAT_SQL     = (1 << 6),     /**< SQL parsing */
    KEEL_LOG_CAT_MEM     = (1 << 7),     /**< Memory management */
    KEEL_LOG_CAT_CONFIG  = (1 << 8),     /**< Configuration */
    KEEL_LOG_CAT_STATS   = (1 << 9),     /**< Statistics */
    KEEL_LOG_CAT_TLS     = (1 << 10),    /**< TLS/SSL */    KEEL_LOG_CAT_PROBE   = (1 << 11),    /**< Health probes & failover */    KEEL_LOG_CAT_ADMIN   = (1 << 12),    /**< Admin console */    KEEL_LOG_CAT_TRACE   = (1 << 13),    /**< Distributed tracing */    KEEL_LOG_CAT_ALL     = 0x7FFFFFFF,   /**< All categories (max signed int for portability) */
} keel_log_category_t;

/* ============================================================================
 * Log Configuration
 * ============================================================================ */

/**
 * @brief Log output types
 */
typedef enum keel_log_output {
    KEEL_LOG_OUTPUT_STDERR = 0,  /**< Standard error */
    KEEL_LOG_OUTPUT_FILE,        /**< File output */
    KEEL_LOG_OUTPUT_SYSLOG,      /**< System log (syslog on Unix) */
    KEEL_LOG_OUTPUT_CALLBACK,    /**< Custom callback */
} keel_log_output_t;

/**
 * @brief Log callback function type
 */
typedef void (*keel_log_callback_t)(
    keel_log_level_t    level,
    keel_log_category_t category,
    const char*        file,
    int                line,
    const char*        func,
    const char*        message,
    void*              user_data
);

/**
 * @brief Log configuration
 */
typedef struct keel_log_config {
    keel_log_level_t    min_level;       /**< Minimum log level */
    uint32_t           categories;      /**< Enabled categories (bitmask) */
    keel_log_output_t   output;          /**< Output type */
    const char*        file_path;       /**< File path (if output is FILE) */
    keel_log_callback_t callback;        /**< Callback (if output is CALLBACK) */
    void*              callback_data;   /**< User data for callback */
    bool               include_time;    /**< Include timestamp */
    bool               include_level;   /**< Include level name */
    bool               include_category;/**< Include category name */
    bool               include_location;/**< Include file:line:func */
    bool               use_colors;      /**< Use ANSI colors (stderr only) */
    bool               json_format;     /**< Emit JSON lines (NDJSON) */
} keel_log_config_t;

/**
 * @brief Construct the default logging configuration.
 *
 * @return Default configuration using INFO level, default output, and all categories.
 */
keel_log_config_t keel_log_config_default(void);

/**
 * @brief Initialize logging subsystem
 *
 * @param config Configuration, or `NULL` for defaults.
 * @return `KEEL_OK` on success, or an error code otherwise.
 */
keel_error_t keel_log_init(const keel_log_config_t* config);

/**
 * @brief Shutdown the logging subsystem.
 *
 * @return
 */
void keel_log_shutdown(void);

/**
 * @brief Set log level at runtime
 *
 * @param level New minimum log level.
 * @return
 */
void keel_log_set_level(keel_log_level_t level);

/**
 * @brief Enable or disable JSON (NDJSON) log format at runtime.
 *
 * @param enable  true to emit JSON lines, false for text.
 */
void keel_log_set_json_format(bool enable);

/**
 * @brief Get current log level
 *
 * @return Current minimum log level.
 */
keel_log_level_t keel_log_get_level(void);

/**
 * @brief Enable/disable log categories
 *
 * @param categories Bitmask of categories to enable.
 * @return
 */
void keel_log_set_categories(uint32_t categories);

/**
 * @brief Get current log categories
 *
 * @return Bitmask of enabled categories.
 */
uint32_t keel_log_get_categories(void);

/* ============================================================================
 * Logging Functions
 * ============================================================================ */

/**
 * @brief Check if a log message would be emitted
 *
 * Use this to avoid expensive formatting when logging is disabled.
 *
 * @param level Log level.
 * @param category Log category.
 * @return `true` if a message at this level/category would be emitted.
 */
bool keel_log_enabled(keel_log_level_t level, keel_log_category_t category);

/**
 * @brief Format and emit a log message through the active logging backend.
 *
 * This is the internal implementation entry point; normal callers should use
 * the level macros so disabled messages short-circuit before varargs formatting.
 */
KEEL_PRINTF_FMT(6, 7)
void keel_log_internal(
    keel_log_level_t    level,
    keel_log_category_t category,
    const char*        file,
    int                line,
    const char*        func,
    const char*        fmt,
    ...
);

/* ============================================================================
 * Logging Macros
 * ============================================================================ */

/** Log at TRACE level */
#define KEEL_LOG_TRACE(cat, ...) \
    do { \
        if (keel_log_enabled(KEEL_LOG_TRACE, (cat))) { \
            keel_log_internal(KEEL_LOG_TRACE, (cat), __FILE__, __LINE__, __func__, __VA_ARGS__); \
        } \
    } while (0)

/** Log at DEBUG level */
#define KEEL_LOG_DEBUG(cat, ...) \
    do { \
        if (keel_log_enabled(KEEL_LOG_DEBUG, (cat))) { \
            keel_log_internal(KEEL_LOG_DEBUG, (cat), __FILE__, __LINE__, __func__, __VA_ARGS__); \
        } \
    } while (0)

/** Log at INFO level */
#define KEEL_LOG_INFO(cat, ...) \
    do { \
        if (keel_log_enabled(KEEL_LOG_INFO, (cat))) { \
            keel_log_internal(KEEL_LOG_INFO, (cat), __FILE__, __LINE__, __func__, __VA_ARGS__); \
        } \
    } while (0)

/** Log at WARN level */
#define KEEL_LOG_WARN(cat, ...) \
    do { \
        if (keel_log_enabled(KEEL_LOG_WARN, (cat))) { \
            keel_log_internal(KEEL_LOG_WARN, (cat), __FILE__, __LINE__, __func__, __VA_ARGS__); \
        } \
    } while (0)

/** Log at ERROR level */
#define KEEL_LOG_ERROR(cat, ...) \
    do { \
        if (keel_log_enabled(KEEL_LOG_ERROR, (cat))) { \
            keel_log_internal(KEEL_LOG_ERROR, (cat), __FILE__, __LINE__, __func__, __VA_ARGS__); \
        } \
    } while (0)

/** Log at FATAL level */
#define KEEL_LOG_FATAL(cat, ...) \
    do { \
        if (keel_log_enabled(KEEL_LOG_FATAL, (cat))) { \
            keel_log_internal(KEEL_LOG_FATAL, (cat), __FILE__, __LINE__, __func__, __VA_ARGS__); \
        } \
    } while (0)

/**
 * @brief Return a stable string name for a log level.
 *
 * @param level Log level.
 * @return Static string naming the level.
 */
KEEL_PURE const char* keel_log_level_name(keel_log_level_t level);

/**
 * @brief Return a stable string name for a log category.
 *
 * @param category Log category.
 * @return Static string naming the category.
 */
KEEL_PURE const char* keel_log_category_name(keel_log_category_t category);

/* ============================================================================
 * Trace Correlation (thread-local)
 * ============================================================================ */

/**
 * @brief Set the trace context for the calling thread's log messages.
 *
 * When JSON format is enabled, these IDs are included in every log line
 * emitted by the current thread until cleared.
 *
 * @param trace_id_hex   32-char hex trace ID (or NULL to clear).
 * @param span_id_hex    16-char hex span ID (or NULL to clear).
 */
void keel_log_set_trace_context(const char* trace_id_hex, const char* span_id_hex);

/** Clear the trace context for the calling thread. */
void keel_log_clear_trace_context(void);

/**
 * @brief Get the current thread's trace ID (32 hex chars or empty string).
 */
const char* keel_log_get_trace_id(void);

/**
 * @brief Get the current thread's span ID (16 hex chars or empty string).
 */
const char* keel_log_get_span_id(void);

/* ============================================================================
 * JSON Record Formatter (shared by log plugins)
 * ============================================================================ */

/* Forward declaration — full definition in log_plugin.h */
struct keel_log_record;

/**
 * @brief Write one log record as a JSON line (NDJSON) to a FILE stream.
 *
 * Includes all structured fields from the record plus the thread-local
 * trace_id and span_id when set.  Output is one self-contained JSON object
 * followed by a newline, suitable for ingestion by ELK, Loki, Datadog, etc.
 *
 * @param out    Destination FILE stream (caller holds any needed lock).
 * @param rec    Structured log record.
 */
void keel_log_record_write_json(FILE* out, const struct keel_log_record* rec);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_LOG_H */
