/**
 * @file query_log.h
 * @brief Public API for SQL query logging and structured query-record emission.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Provides configurable SQL query logging with per-query metadata.
 * Queries can be filtered by type (read, write, all) and are emitted
 * through the active log plugin.
 *
 * Configuration (via [logging] INI section):
 * ===========================================
 *   query_log_mode   = none | all | read | write   (default: none)
 *   log_level        = error | warn | info | debug | all  (default: info)
 *   log_timestamps   = true | false                 (default: true)
 *   log_source_addr  = true | false                 (default: true)
 *   log_dest_addr    = true | false                 (default: true)
 *   log_username     = true | false                 (default: true)
 *   log_database     = true | false                 (default: true)
 *
 * Usage:
 * ======
 * @code
 * // In engine/session hot-path after SQL is classified:
 * keel_query_log_emit(&g_query_log, session, &parsed_query);
 * @endcode
 */

#ifndef KEEL_QUERY_LOG_H
#define KEEL_QUERY_LOG_H

#include "keel/log/log_plugin.h"
#include "keel/protocol/protocol.h"
#include "keel_error.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
typedef struct keel_session keel_session_t;

/* ============================================================================
 * Query Log Filter Mode
 * ============================================================================ */

/**
 * @brief Which queries to log
 */
typedef enum keel_query_log_mode {
    KEEL_QUERY_LOG_NONE  = 0,    /**< No query logging              */
    KEEL_QUERY_LOG_ALL   = 1,    /**< Log every query               */
    KEEL_QUERY_LOG_READ  = 2,    /**< Log read queries only         */
    KEEL_QUERY_LOG_WRITE = 3,    /**< Log write/DDL queries only    */
} keel_query_log_mode_t;

/* ============================================================================
 * Query Log Configuration
 * ============================================================================ */

/**
 * @brief Query log runtime configuration
 */
typedef struct keel_query_log_config {
    keel_query_log_mode_t    mode;           /**< Filter mode               */
    keel_log_level_t         min_level;      /**< Minimum severity to emit  */
    bool                    log_timestamps; /**< Include timestamp          */
    bool                    log_source;     /**< Include client IP/host     */
    bool                    log_dest;       /**< Include backend IP/host    */
    bool                    log_username;   /**< Include username           */
    bool                    log_database;   /**< Include database name      */
    bool                    log_query_tree; /**< Include parsed query tree  */
    size_t                  max_query_len;  /**< Truncate queries longer than this (0 = no limit) */
} keel_query_log_config_t;

/**
 * @brief Return a default query log configuration.
 *
 * @return Default query-log configuration.
 */
static inline keel_query_log_config_t keel_query_log_config_default(void) {
    return (keel_query_log_config_t){
        .mode           = KEEL_QUERY_LOG_NONE,
        .min_level      = KEEL_LOG_INFO,
        .log_timestamps = true,
        .log_source     = true,
        .log_dest       = true,
        .log_username   = true,
        .log_database   = true,
        .log_query_tree = false,
        .max_query_len  = 0,
    };
}

/* ============================================================================
 * Query Logger Handle
 * ============================================================================ */

/**
 * @brief Query logger instance
 *
 * One global instance is typically held by the engine and shared
 * (read-only after init) across all worker threads.
 */
typedef struct keel_query_log {
    keel_query_log_config_t  config;
    keel_log_plugin_t*       plugin;     /**< Output plugin            */
    bool                    enabled;    /**< Quick check for hot-path */
} keel_query_log_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialise one query logger instance.
 *
 * `qlog` is a caller-owned output object. The function writes the resolved
 * configuration and enablement state into it, but does not take ownership of
 * the sink plugin pointer.
 *
 * @param[out] qlog Query logger instance to initialise.
 * @param config Configuration to copy, or `NULL` for defaults.
 * @param plugin Log plugin to write through; ownership remains with the caller.
 * @return `KEEL_OK` on success.
 */
keel_error_t keel_query_log_init(
    keel_query_log_t*              qlog,
    const keel_query_log_config_t* config,
    keel_log_plugin_t*             plugin
);

/**
 * @brief Shut down the query logger.
 *
 * @param qlog Query logger instance.
 * @return
 */
void keel_query_log_shutdown(keel_query_log_t* qlog);

/* ============================================================================
 * Emit
 * ============================================================================ */

/**
 * @brief Log a query if it matches the current filter
 *
 * This is the primary entry point called from the session hot-path.
 * It is safe to call from any thread.
 *
 * @param qlog Query logger.
 * @param session Session providing connection and identity metadata.
 * @param query Parsed query information providing SQL text and classification.
 * @return
 */
void keel_query_log_emit(
    keel_query_log_t*            qlog,
    const keel_session_t*        session,
    const keel_proto_query_t*    query
);

/**
 * @brief Log a raw message through the query log plugin
 *
 * For cases where you need to log something that is not a SQL query
 * but still want it routed through the same plugin pipeline.
 *
 * @param qlog Query logger.
 * @param level Log level.
 * @param cat Log category.
 * @param fmt `printf`-style format string.
 */
KEEL_PRINTF_FMT(4, 5)
void keel_query_log_message(
    keel_query_log_t*    qlog,
    keel_log_level_t     level,
    keel_log_category_t  cat,
    const char*         fmt,
    ...
);

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Parse a query_log_mode string from config
 *
 * @param str One of `none`, `all`, `read`, or `write`.
 * @return Matching enum value, defaulting to `KEEL_QUERY_LOG_NONE`.
 */
keel_query_log_mode_t keel_query_log_mode_from_string(const char* str);

/**
 * @brief Get the name string for a query-log mode.
 *
 * @param mode Query-log mode.
 * @return Static string naming the mode.
 */
const char* keel_query_log_mode_name(keel_query_log_mode_t mode);

/**
 * @brief Parse a log_level string from config
 *
 * Accepts: "error", "warn", "warning", "info", "debug", "trace",
 *          "all", "full", "off", "none"
 *
 * @param str Level string.
 * @return Matching log level, defaulting to `KEEL_LOG_INFO`.
 */
keel_log_level_t keel_log_level_from_string(const char* str);

/* ============================================================================
 * Global Query Logger Accessor
 * ============================================================================
 * The main module owns the global query logger instance. These accessors
 * let engine / worker code reach it without needing to #include main.c.
 */

/**
 * @brief Publish the global query logger pointer used by worker/engine code.
 *
 * @param qlog Global query logger instance.
 * @return
 */
void keel_query_log_set_global(keel_query_log_t* qlog);

/**
 * @brief Get the global query logger.
 *
 * @return Global query logger pointer, or `NULL` if none was registered.
 */
keel_query_log_t* keel_query_log_get_global(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_QUERY_LOG_H */
