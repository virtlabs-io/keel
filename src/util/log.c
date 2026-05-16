/**
 * @file log.c
 * @brief Logging subsystem implementation
 *
 * Provides structured logging with levels, categories, and multiple outputs.
 *
 * Log Levels (from least to most severe):
 * =======================================
 *   TRACE  - Very detailed debugging info
 *   DEBUG  - Developer-relevant information
 *   INFO   - Normal operational messages
 *   WARN   - Potential issues (still operational)
 *   ERROR  - Errors (operation failed)
 *   FATAL  - Critical errors (cannot continue)
 *
 * Categories:
 * ===========
 * Messages can be filtered by category:
 *   CORE, POOL, CONN, IO, PROTO, AUTH, SQL, MEM, CONFIG, STATS, TLS
 *
 * Usage:
 * ======
 * @code
 * // Using macros (preferred)
 * KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, "Connection acquired: %s", conn_id);
 * KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Read failed: %s", strerror(errno));
 *
 * // Check if logging is enabled before expensive formatting
 * if (KEEL_LOG_ENABLED(KEEL_LOG_DEBUG, KEEL_LOG_CAT_MEM)) {
 *     char* dump = expensive_memory_dump();
 *     KEEL_LOG_DEBUG(KEEL_LOG_CAT_MEM, "%s", dump);
 * }
 * @endcode
 *
 * Output Options:
 * ===============
 *   - stderr (default)
 *   - File
 *   - Custom callback
 *
 * Colors:
 * =======
 * When outputting to a terminal, ANSI colors are used:
 *   TRACE=cyan, DEBUG=blue, INFO=green, WARN=yellow, ERROR=red, FATAL=magenta
 */

#include "keel/log/log.h"
#include "keel/log/log_plugin.h"
#include "keel/util/encoding.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

/* ============================================================================
 * Thread-local Trace Context
 * ============================================================================ */

static _Thread_local char tl_trace_id[33];  /* 32 hex + NUL */
static _Thread_local char tl_span_id[17];   /* 16 hex + NUL */

void keel_log_set_trace_context(const char* trace_id_hex, const char* span_id_hex) {
    if (trace_id_hex) {
        snprintf(tl_trace_id, sizeof(tl_trace_id), "%s", trace_id_hex);
    } else {
        tl_trace_id[0] = '\0';
    }
    if (span_id_hex) {
        snprintf(tl_span_id, sizeof(tl_span_id), "%s", span_id_hex);
    } else {
        tl_span_id[0] = '\0';
    }
}

void keel_log_clear_trace_context(void) {
    tl_trace_id[0] = '\0';
    tl_span_id[0] = '\0';
}

const char* keel_log_get_trace_id(void) { return tl_trace_id; }
const char* keel_log_get_span_id(void)  { return tl_span_id; }

/* ============================================================================
 * Global State
 *
 * The logging subsystem uses global state for simplicity.
 * Thread-safe for reading; writes should only happen at startup.
 * ============================================================================ */

static struct {
    bool              initialized;    /**< Whether keel_log_init() was called */
    keel_log_config_t  config;         /**< Current configuration */
    FILE*             file;           /**< Log file handle (if file output) */
} g_log = {0};

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @brief Get default logging configuration
 *
 * Defaults:
 *   - Level: INFO (TRACE and DEBUG disabled)
 *   - Categories: All enabled
 *   - Output: stderr
 *   - Format: timestamp + level, no location
 *   - Colors: enabled
 *
 * @return Default configuration
 */
keel_log_config_t keel_log_config_default(void) {
    return (keel_log_config_t){
        .min_level = KEEL_LOG_INFO,
        .categories = KEEL_LOG_CAT_ALL,
        .output = KEEL_LOG_OUTPUT_STDERR,
        .file_path = NULL,
        .callback = NULL,
        .callback_data = NULL,
        .include_time = true,
        .include_level = true,
        .include_category = false,
        .include_location = false,
        .use_colors = true,
    };
}

/**
 * @brief Initialize the logging subsystem
 *
 * Must be called before any logging. If not called, logging still
 * works with defaults (INFO level, stderr output).
 *
 * @param config  Configuration (NULL for defaults)
 * @return KEEL_OK on success, KEEL_ERR_IO if file open fails
 */
keel_error_t keel_log_init(const keel_log_config_t* config) {
    if (g_log.initialized) {
        return KEEL_OK;
    }
    
    if (config) {
        g_log.config = *config;
    } else {
        g_log.config = keel_log_config_default();
    }
    
    if (g_log.config.output == KEEL_LOG_OUTPUT_FILE && g_log.config.file_path) {
        g_log.file = fopen(g_log.config.file_path, "a");
        if (!g_log.file) {
            return KEEL_ERR_IO;
        }
    }
    
    g_log.initialized = true;
    return KEEL_OK;
}

/**
 * @brief Shutdown the logging subsystem
 *
 * Closes log file if open. Safe to call multiple times.
 */
void keel_log_shutdown(void) {
    if (g_log.file) {
        fclose(g_log.file);
        g_log.file = NULL;
    }
    g_log.initialized = false;
}

/** @brief Set minimum log level at runtime */
void keel_log_set_level(keel_log_level_t level) {
    g_log.config.min_level = level;
}

/** @brief Enable or disable JSON log format at runtime */
void keel_log_set_json_format(bool enable) {
    g_log.config.json_format = enable;
    /* Ensure subsystem is marked initialized so json_format takes effect */
    if (!g_log.initialized) {
        g_log.config = keel_log_config_default();
        g_log.config.json_format = enable;
        g_log.initialized = true;
    }
}

/** @brief Get current minimum log level */
keel_log_level_t keel_log_get_level(void) {
    return g_log.config.min_level;
}

/** @brief Set enabled categories (bitmask) */
void keel_log_set_categories(uint32_t categories) {
    g_log.config.categories = categories;
}

/** @brief Get current enabled categories */
uint32_t keel_log_get_categories(void) {
    return g_log.config.categories;
}

/* ============================================================================
 * Logging
 * ============================================================================ */

/**
 * @brief Check if a log message would be output
 *
 * Use before expensive message formatting.
 *
 * @param level     Log level of the message
 * @param category  Category of the message
 * @return true if message would be logged
 */
bool keel_log_enabled(keel_log_level_t level, keel_log_category_t category) {
    if (!g_log.initialized) {
        /* Default: enable INFO and above */
        return level >= KEEL_LOG_INFO;
    }
    
    if (level < g_log.config.min_level) {
        return false;
    }
    
    if (!(g_log.config.categories & (uint32_t)category)) {
        return false;
    }
    
    return true;
}

/** ANSI color codes for each log level */
static const char* level_colors[] = {
    "\033[36m",  /* TRACE: cyan */
    "\033[34m",  /* DEBUG: blue */
    "\033[32m",  /* INFO:  green */
    "\033[33m",  /* WARN:  yellow */
    "\033[31m",  /* ERROR: red */
    "\033[35m",  /* FATAL: magenta */
};

/** Human-readable level names */
static const char* level_names[] = {
    "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
};

/** Escape a string for JSON output; writes at most dst_size-1 chars + NUL. */
static size_t json_escape(char* dst, size_t dst_size, const char* src) {
    return keel_json_escape(dst, dst_size, src);
}

/**
 * @brief Internal logging function (called by macros)
 *
 * Formats and outputs a log message. Use the KEEL_LOG_* macros instead
 * of calling this directly.
 *
 * @param level     Log level
 * @param category  Log category
 * @param file      Source file name
 * @param line      Source line number
 * @param func      Function name
 * @param fmt       Printf-style format string
 * @param ...       Format arguments
 */
void keel_log_internal(
    keel_log_level_t    level,
    keel_log_category_t category,
    const char*        file,
    int                line,
    const char*        func,
    const char*        fmt,
    ...
) {
    FILE* out = g_log.file ? g_log.file : stderr;

    /* Format the user message first (shared by both paths) */
    char msg_buf[2048];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msg_buf, sizeof(msg_buf), fmt, args);
    va_end(args);

    /* === JSON format (NDJSON: one JSON object per line) === */
    if (g_log.initialized && g_log.config.json_format) {
        char ts_buf[32];
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        struct tm tm_storage;
        gmtime_r(&ts.tv_sec, &tm_storage);
        strftime(ts_buf, sizeof(ts_buf), "%Y-%m-%dT%H:%M:%S", &tm_storage);

        char escaped_msg[4096];
        json_escape(escaped_msg, sizeof(escaped_msg), msg_buf);

        const char* basename = strrchr(file, '/');
        basename = basename ? basename + 1 : file;

        const char* cat_name = keel_log_category_name(category);

        fprintf(out,
            "{\"ts\":\"%s.%06ldZ\",\"level\":\"%s\",\"cat\":\"%s\","
            "\"file\":\"%s\",\"line\":%d,\"func\":\"%s\","
            "\"msg\":\"%s\"",
            ts_buf, ts.tv_nsec / 1000,
            (level < 6) ? level_names[level] : "UNKNOWN",
            cat_name, basename, line, func, escaped_msg);

        /* Append trace context if set */
        if (tl_trace_id[0] != '\0')
            fprintf(out, ",\"trace_id\":\"%s\"", tl_trace_id);
        if (tl_span_id[0] != '\0')
            fprintf(out, ",\"span_id\":\"%s\"", tl_span_id);

        fprintf(out, "}\n");
        fflush(out);
        return;
    }

    /* === Text format (original) === */
    bool use_colors = g_log.config.use_colors && !g_log.file;
    
    /* Timestamp */
    if (g_log.config.include_time || !g_log.initialized) {
        time_t now = time(NULL);
        struct tm tm_storage;
        struct tm* tm_info = localtime_r(&now, &tm_storage);
        char time_buf[32];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
        fprintf(out, "%s ", time_buf);
    }
    
    /* Level */
    if (g_log.config.include_level || !g_log.initialized) {
        if (use_colors && level < 6) {
            fprintf(out, "%s%-5s\033[0m ", level_colors[level], level_names[level]);
        } else if (level < 6) {
            fprintf(out, "%-5s ", level_names[level]);
        }
    }
    
    /* Location */
    if (g_log.config.include_location) {
        /* Get just the filename, not full path */
        const char* basename = strrchr(file, '/');
        if (!basename) {
            basename = file;
        } else {
            basename++;
        }
        fprintf(out, "[%s:%d:%s] ", basename, line, func);
    }
    
    /* Message */
    fputs(msg_buf, out);
    
    fprintf(out, "\n");
    fflush(out);
}

/** @brief Get the string name of a log level */
const char* keel_log_level_name(keel_log_level_t level) {
    if (level >= 0 && level < 6) {
        return level_names[level];
    }
    return "UNKNOWN";
}

/** @brief Get the string name of a log category */
const char* keel_log_category_name(keel_log_category_t category) {
    switch (category) {
    case KEEL_LOG_CAT_CORE:   return "CORE";
    case KEEL_LOG_CAT_POOL:   return "POOL";
    case KEEL_LOG_CAT_CONN:   return "CONN";
    case KEEL_LOG_CAT_IO:     return "IO";
    case KEEL_LOG_CAT_PROTO:  return "PROTO";
    case KEEL_LOG_CAT_AUTH:   return "AUTH";
    case KEEL_LOG_CAT_SQL:    return "SQL";
    case KEEL_LOG_CAT_MEM:    return "MEM";
    case KEEL_LOG_CAT_CONFIG: return "CONFIG";
    case KEEL_LOG_CAT_STATS:  return "STATS";
    case KEEL_LOG_CAT_TLS:    return "TLS";
    case KEEL_LOG_CAT_PROBE:  return "PROBE";
    case KEEL_LOG_CAT_ADMIN:  return "ADMIN";
    default:                 return "ALL";
    }
}

/* ============================================================================
 * JSON Record Formatter (shared by log plugins)
 * ============================================================================ */

/** Escape src into dst for JSON output; returns bytes written (excluding NUL). */
static size_t json_escape_str(char* dst, size_t dst_size, const char* src, size_t src_len) {
    size_t w = 0;
    for (size_t i = 0; i < src_len && src[i] && w + 2 < dst_size; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '"' || c == '\\') {
            dst[w++] = '\\'; dst[w++] = (char)c;
        } else if (c == '\n') {
            dst[w++] = '\\'; dst[w++] = 'n';
        } else if (c == '\r') {
            dst[w++] = '\\'; dst[w++] = 'r';
        } else if (c == '\t') {
            dst[w++] = '\\'; dst[w++] = 't';
        } else if (c < 0x20) {
            /* skip other control chars */
        } else {
            dst[w++] = (char)c;
        }
    }
    dst[w] = '\0';
    return w;
}

void keel_log_record_write_json(FILE* out, const keel_log_record_t* rec) {
    /* Timestamp */
    char ts_buf[32] = {0};
    long usec = 0;
    if (rec->ts_sec > 0) {
        time_t t = (time_t)rec->ts_sec;
        struct tm tm_buf;
        gmtime_r(&t, &tm_buf);
        strftime(ts_buf, sizeof(ts_buf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
        usec = rec->ts_nsec / 1000;
    } else {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        struct tm tm_buf;
        gmtime_r(&ts.tv_sec, &tm_buf);
        strftime(ts_buf, sizeof(ts_buf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
        usec = ts.tv_nsec / 1000;
    }

    fprintf(out, "{\"ts\":\"%s.%06ldZ\",\"level\":\"%s\",\"cat\":\"%s\"",
            ts_buf, usec,
            keel_log_level_name(rec->level),
            keel_log_category_name(rec->category));

    /* Source location */
    if (rec->file && rec->line > 0) {
        const char* base = strrchr(rec->file, '/');
        base = base ? base + 1 : rec->file;
        fprintf(out, ",\"file\":\"%s\",\"line\":%d", base, rec->line);
        if (rec->func)
            fprintf(out, ",\"func\":\"%s\"", rec->func);
    }

    /* Message */
    if (rec->message && rec->message_len > 0) {
        char escaped[4096];
        json_escape_str(escaped, sizeof(escaped), rec->message, rec->message_len);
        fprintf(out, ",\"msg\":\"%s\"", escaped);
    }

    /* Structured fields */
    if (rec->src_addr)
        fprintf(out, ",\"src_addr\":\"%s\",\"src_port\":%u", rec->src_addr, (unsigned)rec->src_port);
    if (rec->dst_addr)
        fprintf(out, ",\"dst_addr\":\"%s\",\"dst_port\":%u", rec->dst_addr, (unsigned)rec->dst_port);
    if (rec->username)
        fprintf(out, ",\"user\":\"%s\"", rec->username);
    if (rec->database)
        fprintf(out, ",\"db\":\"%s\"", rec->database);

    /* Query text */
    if (rec->query && rec->query_len > 0) {
        char escaped[8192];
        json_escape_str(escaped, sizeof(escaped), rec->query, rec->query_len);
        fprintf(out, ",\"query\":\"%s\"", escaped);
    }

    /* Query tree */
    if (rec->query_tree && rec->query_tree_len > 0) {
        char escaped[8192];
        json_escape_str(escaped, sizeof(escaped), rec->query_tree, rec->query_tree_len);
        fprintf(out, ",\"query_tree\":\"%s\"", escaped);
    }

    /* Trace correlation (thread-local) */
    const char* tid = keel_log_get_trace_id();
    const char* sid = keel_log_get_span_id();
    if (tid[0] != '\0')
        fprintf(out, ",\"trace_id\":\"%s\"", tid);
    if (sid[0] != '\0')
        fprintf(out, ",\"span_id\":\"%s\"", sid);

    fprintf(out, "}\n");
    fflush(out);
}
