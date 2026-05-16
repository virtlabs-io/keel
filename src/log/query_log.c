/**
 * @file query_log.c
 * @brief Query-log filtering, record assembly, and plugin emission.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file turns classified SQL queries into structured `keel_log_record_t`
 * objects and sends them through the active log plugin. It deliberately sits
 * above the core logging macros and below the engine/session path, acting as a
 * translation layer between query semantics and sink-oriented log records.
 *
 * Implementation strategy:
 *   - Filtering happens first and cheaply using the already-computed query type
 *     and flags so disabled query logging has minimal hot-path cost.
 *   - Address enrichment is opportunistic: the logger asks the kernel for peer
 *     and local socket addresses only when the relevant config flags are on.
 *   - Query-tree serialization is optional because it can require a full parse
 *     and scratch arena allocation. This is intentionally off by default to
 *     avoid making normal query logging pay for debug-heavy introspection.
 *   - Records are stack-built and sink-agnostic. The plugin decides how to
 *     format or persist them.
 */

#include "keel/log/query_log.h"
#include "keel/session/session.h"
#include "keel/log/log.h"
#include "keel/sql/sql.h"
#include "keel/sql/query_tree.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ============================================================================
 * Helpers — resolve sockaddr to printable string
 * ============================================================================ */

/**
 * @brief Resolve a socket peer address into printable host/port outputs.
 *
 * `buf` and `port_out` are caller-owned output parameters that receive the
 * stringified address and numeric port when resolution succeeds.
 *
 * @param fd Socket file descriptor.
 * @param[out] buf Caller-owned destination buffer for the address string.
 * @param bufsz Size of `buf` in bytes.
 * @param[out] port_out Caller-owned port output.
 * @return
 */
static void peer_addr_str(int fd, char* buf, size_t bufsz, uint16_t* port_out)
{
    buf[0] = '\0';
    *port_out = 0;
    if (fd < 0) return;

    struct sockaddr_storage ss;
    socklen_t len = sizeof(ss);
    if (getpeername(fd, (struct sockaddr*)&ss, &len) != 0) return;

    if (ss.ss_family == AF_INET) {
        struct sockaddr_in* sin = (struct sockaddr_in*)&ss;
        inet_ntop(AF_INET, &sin->sin_addr, buf, (socklen_t)bufsz);
        *port_out = ntohs(sin->sin_port);
    } else if (ss.ss_family == AF_INET6) {
        struct sockaddr_in6* sin6 = (struct sockaddr_in6*)&ss;
        inet_ntop(AF_INET6, &sin6->sin6_addr, buf, (socklen_t)bufsz);
        *port_out = ntohs(sin6->sin6_port);
    }
}

/**
 * @brief Resolve a socket's local bound address into printable host/port outputs.
 *
 * @param fd Socket file descriptor.
 * @param[out] buf Caller-owned destination buffer for the address string.
 * @param bufsz Size of `buf` in bytes.
 * @param[out] port_out Caller-owned port output.
 * @return
 */
static void local_addr_str(int fd, char* buf, size_t bufsz, uint16_t* port_out)
{
    buf[0] = '\0';
    *port_out = 0;
    if (fd < 0) return;

    struct sockaddr_storage ss;
    socklen_t len = sizeof(ss);
    if (getsockname(fd, (struct sockaddr*)&ss, &len) != 0) return;

    if (ss.ss_family == AF_INET) {
        struct sockaddr_in* sin = (struct sockaddr_in*)&ss;
        inet_ntop(AF_INET, &sin->sin_addr, buf, (socklen_t)bufsz);
        *port_out = ntohs(sin->sin_port);
    } else if (ss.ss_family == AF_INET6) {
        struct sockaddr_in6* sin6 = (struct sockaddr_in6*)&ss;
        inet_ntop(AF_INET6, &sin6->sin6_addr, buf, (socklen_t)bufsz);
        *port_out = ntohs(sin6->sin6_port);
    }
}

/* ============================================================================
 * String ↔ Enum Converters
 * ============================================================================ */

/**
 * @brief Parse a query-log mode string.
 *
 * @param str Mode string.
 * @return Parsed query-log mode, defaulting to `KEEL_QUERY_LOG_NONE`.
 */
keel_query_log_mode_t keel_query_log_mode_from_string(const char* str)
{
    if (!str) return KEEL_QUERY_LOG_NONE;
    if (strcasecmp(str, "all")   == 0) return KEEL_QUERY_LOG_ALL;
    if (strcasecmp(str, "read")  == 0) return KEEL_QUERY_LOG_READ;
    if (strcasecmp(str, "write") == 0) return KEEL_QUERY_LOG_WRITE;
    return KEEL_QUERY_LOG_NONE;
}

/**
 * @brief Return a stable string name for a query-log mode.
 *
 * @param mode Query-log mode.
 * @return Static string naming the mode.
 */
const char* keel_query_log_mode_name(keel_query_log_mode_t mode)
{
    switch (mode) {
    case KEEL_QUERY_LOG_ALL:   return "all";
    case KEEL_QUERY_LOG_READ:  return "read";
    case KEEL_QUERY_LOG_WRITE: return "write";
    default:                  return "none";
    }
}

/**
 * @brief Parse a textual log-level name into the enum used by log records.
 *
 * @param str Level string.
 * @return Parsed log level, defaulting to `KEEL_LOG_INFO`.
 */
keel_log_level_t keel_log_level_from_string(const char* str)
{
    if (!str) return KEEL_LOG_INFO;
    if (strcasecmp(str, "trace")   == 0) return KEEL_LOG_TRACE;
    if (strcasecmp(str, "debug")   == 0) return KEEL_LOG_DEBUG;
    if (strcasecmp(str, "info")    == 0) return KEEL_LOG_INFO;
    if (strcasecmp(str, "warn")    == 0) return KEEL_LOG_WARN;
    if (strcasecmp(str, "warning") == 0) return KEEL_LOG_WARN;
    if (strcasecmp(str, "error")   == 0) return KEEL_LOG_ERROR;
    if (strcasecmp(str, "fatal")   == 0) return KEEL_LOG_FATAL;
    if (strcasecmp(str, "all")     == 0) return KEEL_LOG_TRACE;
    if (strcasecmp(str, "full")    == 0) return KEEL_LOG_TRACE;
    if (strcasecmp(str, "off")     == 0) return KEEL_LOG_OFF;
    if (strcasecmp(str, "none")    == 0) return KEEL_LOG_OFF;
    return KEEL_LOG_INFO;
}

/* ============================================================================
 * Query type name for log output
 * ============================================================================ */

/**
 * @brief Map parsed query types to short human-readable labels.
 *
 * @param type Parsed query type.
 * @return Static string label for log messages.
 */
static const char* query_type_label(keel_query_type_t type)
{
    switch (type) {
    case KEEL_QUERY_SELECT:     return "SELECT";
    case KEEL_QUERY_SHOW:       return "SHOW";
    case KEEL_QUERY_EXPLAIN:    return "EXPLAIN";
    case KEEL_QUERY_INSERT:     return "INSERT";
    case KEEL_QUERY_UPDATE:     return "UPDATE";
    case KEEL_QUERY_DELETE:     return "DELETE";
    case KEEL_QUERY_TRUNCATE:   return "TRUNCATE";
    case KEEL_QUERY_CREATE:     return "CREATE";
    case KEEL_QUERY_ALTER:      return "ALTER";
    case KEEL_QUERY_DROP:       return "DROP";
    case KEEL_QUERY_BEGIN:      return "BEGIN";
    case KEEL_QUERY_COMMIT:     return "COMMIT";
    case KEEL_QUERY_ROLLBACK:   return "ROLLBACK";
    case KEEL_QUERY_SAVEPOINT:  return "SAVEPOINT";
    case KEEL_QUERY_SET:        return "SET";
    case KEEL_QUERY_RESET:      return "RESET";
    case KEEL_QUERY_DISCARD:    return "DISCARD";
    case KEEL_QUERY_PREPARE:    return "PREPARE";
    case KEEL_QUERY_EXECUTE:    return "EXECUTE";
    case KEEL_QUERY_DEALLOCATE: return "DEALLOCATE";
    case KEEL_QUERY_COPY:       return "COPY";
    case KEEL_QUERY_CALL:       return "CALL";
    case KEEL_QUERY_DO:         return "DO";
    case KEEL_QUERY_MERGE:      return "MERGE";
    case KEEL_QUERY_MAINTENANCE: return "MAINTENANCE";
    case KEEL_QUERY_LOCK:       return "LOCK";
    case KEEL_QUERY_LISTEN_NOTIFY: return "LISTEN/NOTIFY";
    case KEEL_QUERY_UNLISTEN:       return "UNLISTEN";
    default:                   return "UNKNOWN";
    }
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialize a query logger instance.
 *
 * `qlog` is a caller-owned output object populated in-place. The function
 * copies configuration and computes whether the logger is effectively enabled.
 *
 * @param[out] qlog Query logger instance to initialize.
 * @param config Configuration to copy, or `NULL` for defaults.
 * @param plugin Sink plugin used for emission.
 * @return `KEEL_OK` on success or `KEEL_ERR_INVALID_ARG` if `qlog` is `NULL`.
 */
keel_error_t keel_query_log_init(keel_query_log_t* qlog,
                               const keel_query_log_config_t* config,
                               keel_log_plugin_t* plugin)
{
    if (!qlog) return KEEL_ERR_INVALID_ARG;

    memset(qlog, 0, sizeof(*qlog));

    if (config) {
        qlog->config = *config;
    } else {
        qlog->config = keel_query_log_config_default();
    }

    qlog->plugin  = plugin;
    qlog->enabled = (qlog->config.mode != KEEL_QUERY_LOG_NONE) && (plugin != NULL);

    return KEEL_OK;
}

/**
 * @brief Disable a query logger instance.
 *
 * @param qlog Query logger instance.
 * @return
 */
void keel_query_log_shutdown(keel_query_log_t* qlog)
{
    if (!qlog) return;
    qlog->enabled = false;
    /* We do NOT close or destroy the plugin here — the caller owns it */
}

/* ============================================================================
 * Filter check
 * ============================================================================ */

/**
 * @brief Check whether the current query passes the configured filter.
 *
 * @param qlog Query logger configuration.
 * @param query Parsed query metadata.
 * @return `true` if the query should be emitted.
 */
static bool should_log(const keel_query_log_t* qlog,
                       const keel_proto_query_t* query)
{
    switch (qlog->config.mode) {
    case KEEL_QUERY_LOG_ALL:
        return true;
    case KEEL_QUERY_LOG_READ:
        return (query->flags & KEEL_QUERY_FLAG_READ_ONLY) != 0;
    case KEEL_QUERY_LOG_WRITE:
        return (query->flags & (KEEL_QUERY_FLAG_WRITE | KEEL_QUERY_FLAG_DDL)) != 0;
    default:
        return false;
    }
}

/* ============================================================================
 * Emit
 * ============================================================================ */

/**
 * @brief Emit one structured query log record through the active sink plugin.
 *
 * The function assembles a stack-local record with optional socket metadata,
 * truncated SQL text, and optional parsed query-tree serialization before
 * passing it to the configured plugin.
 *
 * @param qlog Query logger.
 * @param session Session metadata source.
 * @param query Parsed query metadata source.
 * @return
 */
void keel_query_log_emit(keel_query_log_t* qlog,
                        const keel_session_t* session,
                        const keel_proto_query_t* query)
{
    if (!qlog || !qlog->enabled || !qlog->plugin) return;
    if (!query || query->sql.len == 0) return;
    if (!should_log(qlog, query)) return;

    /* Build the log record */
    keel_log_record_t rec;
    memset(&rec, 0, sizeof(rec));

    rec.level    = qlog->config.min_level;
    rec.category = KEEL_LOG_CAT_SQL;

    /* Timestamp */
    if (qlog->config.log_timestamps) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        rec.ts_sec  = ts.tv_sec;
        rec.ts_nsec = ts.tv_nsec;
    }

    /* Source (client) and destination (backend) addresses */
    char src_buf[INET6_ADDRSTRLEN] = {0};
    char dst_buf[INET6_ADDRSTRLEN] = {0};
    uint16_t src_port = 0, dst_port = 0;

    if (session && qlog->config.log_source) {
        peer_addr_str(session->client_fd, src_buf, sizeof(src_buf), &src_port);
        if (src_buf[0]) {
            rec.src_addr = src_buf;
            rec.src_port = src_port;
        }
    }
    if (session && qlog->config.log_dest) {
        peer_addr_str(session->server_fd, dst_buf, sizeof(dst_buf), &dst_port);
        if (dst_buf[0]) {
            rec.dst_addr = dst_buf;
            rec.dst_port = dst_port;
        }
    }

    /* User / database */
    if (session && qlog->config.log_username && session->username[0]) {
        rec.username = session->username;
    }
    if (session && qlog->config.log_database && session->database[0]) {
        rec.database = session->database;
    }

    /* Query text */
    rec.query     = query->sql.data;
    rec.query_len = query->sql.len;
    if (qlog->config.max_query_len > 0 &&
        rec.query_len > qlog->config.max_query_len) {
        rec.query_len = qlog->config.max_query_len;
    }

    /* Message: a short label like "QUERY SELECT" */
    char msg[128];
    int n = snprintf(msg, sizeof(msg), "QUERY %s%s",
                     query_type_label(query->type),
                     query->needs_primary ? " [primary]" : "");
    rec.message     = msg;
    rec.message_len = (size_t)(n > 0 ? n : 0);

    /* Query tree serialization (optional — requires full parse) */
    char tree_buf[4096];
    keel_arena_t* tree_arena = NULL;

    if (qlog->config.log_query_tree && query->sql.len > 0) {
        tree_arena = keel_arena_create(4096);
        if (tree_arena) {
            keel_qt_query_t* qt = keel_sql_analyze_full(query->sql, tree_arena);
            if (qt) {
                int tlen = keel_qt_snprint(qt, tree_buf, sizeof(tree_buf));
                rec.query_tree     = tree_buf;
                rec.query_tree_len = (size_t)(tlen > 0 ? tlen : 0);
            }
        }
    }

    /* Emit through the plugin */
    qlog->plugin->write(qlog->plugin, &rec);

    /* Dispose of the arena used for the tree parse */
    if (tree_arena) {
        keel_arena_destroy(tree_arena);
    }
}

/**
 * @brief Emit a non-query message through the query-log sink pipeline.
 *
 * @param qlog Query logger.
 * @param level Log level.
 * @param cat Log category.
 * @param fmt Format string.
 * @return
 */
void keel_query_log_message(keel_query_log_t* qlog,
                           keel_log_level_t level,
                           keel_log_category_t cat,
                           const char* fmt, ...)
{
    if (!qlog || !qlog->plugin) return;

    char buf[2048];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    keel_log_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.level       = level;
    rec.category    = cat;
    rec.message     = buf;
    rec.message_len = (size_t)(n > 0 ? n : 0);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    rec.ts_sec  = ts.tv_sec;
    rec.ts_nsec = ts.tv_nsec;

    qlog->plugin->write(qlog->plugin, &rec);
}

/* ============================================================================
 * Global Query Logger Accessor
 * ============================================================================ */

static keel_query_log_t* s_global_qlog = NULL;

/**
 * @brief Publish the process-global query logger pointer.
 *
 * @param qlog Query logger instance.
 * @return
 */
void keel_query_log_set_global(keel_query_log_t* qlog)
{
    s_global_qlog = qlog;
}

/**
 * @brief Return the process-global query logger pointer.
 *
 * @return Global query logger instance, or `NULL`.
 */
keel_query_log_t* keel_query_log_get_global(void)
{
    return s_global_qlog;
}