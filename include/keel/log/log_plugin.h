/**
 * @file log_plugin.h
 * @brief Public API for pluggable log sinks and structured log records.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Defines the vtable that every log output plugin must implement.
 * KEEL ships with three built-in plugins:
 *
 *   stdout  – writes to stdout/stderr (default)
 *   file    – appends to a user-specified log file
 *   syslog  – sends messages via syslog(3)
 *
 * Third-party plugins can be compiled as shared libraries (.so / .dylib)
 * and loaded at runtime via the [logging] plugin_path configuration key.
 *
 * Plugin Contract
 * ===============
 *   1. Export a function: keel_log_plugin_t* keel_log_plugin_create(void)
 *   2. Populate the vtable completely (all function pointers non-NULL).
 *   3. open()  is called once at startup with plugin-specific config.
 *   4. write() may be called from any thread – plugin must be thread-safe.
 *   5. flush() is called when the engine needs output guaranteed on disk.
 *   6. close() is called once at shutdown.
 *
 * Thread Safety
 * =============
 * The engine guarantees that open() and close() are called from the main
 * thread only. write() and flush() may be called from any worker thread
 * concurrently. The plugin must handle its own synchronisation.
 */

#ifndef KEEL_LOG_PLUGIN_H
#define KEEL_LOG_PLUGIN_H

#include "keel/log/log.h"
#include "keel_error.h"

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Plugin Configuration (passed to open)
 * ============================================================================ */

/**
 * @brief Key-value pair for plugin-specific configuration
 *
 * The engine passes all keys from the [logging] INI section that are
 * not consumed by the core (e.g. log_level, query_log_mode) through
 * to the plugin's open() call.
 */
typedef struct keel_log_plugin_opt {
    const char* key;
    const char* value;
} keel_log_plugin_opt_t;

/**
 * @brief Configuration bundle passed to plugin open()
 */
typedef struct keel_log_plugin_config {
    const keel_log_plugin_opt_t* opts;   /**< Array of key-value options     */
    size_t                      nopts;  /**< Number of options              */
    const char*                 file_path; /**< Shortcut: log file path (file plugin) */
    const char*                 ident;  /**< Shortcut: syslog ident         */
} keel_log_plugin_config_t;

/* ============================================================================
 * Log Record (what is passed to write)
 * ============================================================================ */

/**
 * @brief A single log record ready for output
 */
typedef struct keel_log_record {
    /* Severity / filtering */
    keel_log_level_t     level;
    keel_log_category_t  category;

    /* Timestamp (epoch seconds + nanosecond fraction) */
    int64_t             ts_sec;
    int64_t             ts_nsec;

    /* Source location */
    const char*         file;
    int                 line;
    const char*         func;

    /* Pre-formatted message body (null-terminated) */
    const char*         message;
    size_t              message_len;

    /* Optional structured fields — may be NULL / 0 */
    const char*         src_addr;       /**< Client IP or hostname  */
    uint16_t            src_port;       /**< Client port            */
    const char*         dst_addr;       /**< Backend IP or hostname */
    uint16_t            dst_port;       /**< Backend port           */
    const char*         username;       /**< Database user          */
    const char*         database;       /**< Target database        */
    const char*         query;          /**< SQL query text         */
    size_t              query_len;      /**< Length of query        */

    /* Optional parsed tree representation */
    const char*         query_tree;     /**< Serialised parse tree  */
    size_t              query_tree_len; /**< Length of tree string  */
} keel_log_record_t;

/* ============================================================================
 * Plugin VTable
 * ============================================================================ */

/**
 * @brief Log output plugin vtable
 *
 * Every log plugin (built-in or external .so) must provide an instance
 * of this structure.
 */
typedef struct keel_log_plugin {
    /** Human-readable plugin name (e.g. "stdout", "file", "syslog") */
    const char* name;

    /**
     * @brief Open / initialise the plugin
     *
     * Called once from the main thread before any write() calls.
     *
     * @param plugin    This plugin instance
     * @param config    Configuration options from [logging] section
     * @return KEEL_OK on success, error code otherwise
     */
    keel_error_t (*open)(struct keel_log_plugin* plugin,
                        const keel_log_plugin_config_t* config);

    /**
     * @brief Write a log record
     *
     * Must be thread-safe. May be called from any worker thread.
     *
     * @param plugin    This plugin instance
     * @param record    The log record to output
     * @return KEEL_OK on success, error code otherwise
     */
    keel_error_t (*write)(struct keel_log_plugin* plugin,
                         const keel_log_record_t* record);

    /**
     * @brief Flush buffered output
     *
     * Called when the engine needs output guaranteed on storage.
     *
     * @param plugin    This plugin instance
     * @return KEEL_OK on success, error code otherwise
     */
    keel_error_t (*flush)(struct keel_log_plugin* plugin);

    /**
     * @brief Close the plugin and release resources
     *
     * Called once from the main thread during shutdown.
     *
     * @param plugin    This plugin instance
     */
    void (*close)(struct keel_log_plugin* plugin);

    /**
     * @brief Destroy and free the plugin instance
     *
     * Called after close(). The plugin should free its own memory.
     *
     * @param plugin    This plugin instance
     */
    void (*destroy)(struct keel_log_plugin* plugin);

    /** Opaque plugin-private data (the plugin owns this pointer). */
    void* priv;
} keel_log_plugin_t;

/* ============================================================================
 * Built-in Plugin Constructors
 * ============================================================================ */

/**
 * @brief Create the built-in stdout/stderr log plugin.
 *
 * Writes formatted log lines to stdout (INFO and below) or stderr
 * (WARN and above). Supports optional ANSI colour output.
 * @return Plugin instance, or `NULL` on allocation failure.
 */
keel_log_plugin_t* keel_log_plugin_stdout_create(void);

/**
 * @brief Create the built-in file log plugin.
 *
 * Appends log lines to the file specified in config->file_path.
 * The file is opened in append mode and is created if it does not exist.
 * @return Plugin instance, or `NULL` on allocation failure.
 */
keel_log_plugin_t* keel_log_plugin_file_create(void);

/**
 * @brief Create the built-in syslog log plugin.
 *
 * Sends log messages to the local syslog daemon using syslog(3).
 * Uses config->ident as the syslog identifier (defaults to "keel").
 * @return Plugin instance, or `NULL` on allocation failure.
 */
keel_log_plugin_t* keel_log_plugin_syslog_create(void);

/* ============================================================================
 * Dynamic Plugin Loader
 * ============================================================================ */

/**
 * @brief Load a log plugin from a shared library
 *
 * The library must export a function with the following signature:
 *   keel_log_plugin_t* keel_log_plugin_create(void);
 *
 * @param path Path to the shared library (`.so` or `.dylib`).
 * @return Plugin instance, or `NULL` on failure.
 */
keel_log_plugin_t* keel_log_plugin_load(const char* path);

/**
 * @brief Unload a dynamically loaded plugin
 *
 * Calls plugin->close() and plugin->destroy(), then dlclose()s the
 * shared library handle. Do NOT call this on built-in plugins.
 *
 * @param plugin Plugin to unload.
 * @return
 */
void keel_log_plugin_unload(keel_log_plugin_t* plugin);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_LOG_PLUGIN_H */
