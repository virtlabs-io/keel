/**
 * @file query_logger_plugin.c
 * @brief Native .so hook plugin example — query logger
 *
 * Demonstrates how to write a native shared-library plugin for the
 * Keel hook system.  This plugin registers hooks at two points:
 *
 *   1. after_query_parse  — logs every query
 *   2. before_route       — logs routing decisions
 *
 * Build:
 *   gcc -shared -fPIC -o query_logger_plugin.so query_logger_plugin.c \
 *       -I../../include
 *
 * Configuration (inside a worker group):
 *   [worker_group.<group>.hooks]
 *   hook.plugin.query_logger = examples/hooks/plugins/query_logger_plugin.so
 */

#include "keel_hook.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* -------------------------------------------------------------------------- */
/* Internal state                                                              */
/* -------------------------------------------------------------------------- */

/**
 * Global log file handle.
 *
 * Opened once during keel_hook_plugin_init() and shared by both hook
 * callbacks.  Falls back to stderr if the configured path cannot be opened.
 * Closed in plugin_shutdown().
 *
 * Thread safety: Keel dispatches hooks on the worker thread that owns the
 * session, so concurrent writes from different workers may interleave at
 * the line level.  This is acceptable for a logging plugin — lines are
 * self-contained and fflush() after each write prevents partial lines.
 */
static FILE* g_logfile = NULL;

/**
 * Map a numeric keel_query_type_t value to a human-readable string.
 *
 * The table is ordered to match the keel_query_type_t enum so that a
 * simple array index lookup suffices.  Returns "UNKNOWN" for out-of-range
 * values.
 *
 * @param qt  Numeric query type from ctx->query_type.
 * @return    Static string — never freed.
 */
static const char* query_type_name(uint32_t qt) {
    static const char* names[] = {
        "UNKNOWN",
        "SELECT",   "SHOW",       "EXPLAIN",
        "INSERT",   "UPDATE",     "DELETE",    "TRUNCATE",
        "CREATE",   "ALTER",      "DROP",
        "BEGIN",    "COMMIT",     "ROLLBACK",  "SAVEPOINT",
        "SET",      "RESET",      "DISCARD",
        "PREPARE",  "EXECUTE",    "DEALLOCATE",
        "COPY",
    };
    if (qt < sizeof(names) / sizeof(names[0]))
        return names[qt];
    return "UNKNOWN";
}

/* -------------------------------------------------------------------------- */
/* Hook callbacks                                                              */
/* -------------------------------------------------------------------------- */

/**
 * Hook callback: log parsed query details (AFTER_QUERY_PARSE).
 *
 * Writes a single pipe-delimited line per query containing the timestamp,
 * session ID, user@database, read/write classification, query type name,
 * effect bitmask, cumulative query count, and the first 512 characters of
 * the SQL text.  The line is immediately flushed to avoid buffering delays.
 *
 * @param ctx  Hook context populated by the SQL parser.  Fields used:
 *             session_id, needs_primary, query_type, sql_text, username,
 *             database, effect_flags, query_count.
 * @return     Always true — this is a logging-only hook, never aborts.
 */
static bool hook_log_query(keel_hook_ctx_t* ctx) {
    if (!g_logfile) return true;

    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);

    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm);

    const char* rw   = ctx->needs_primary ? "WRITE" : "READ";
    const char* qt   = query_type_name(ctx->query_type);
    const char* sql  = ctx->sql_text ? ctx->sql_text : "";
    const char* user = ctx->username ? ctx->username : "?";
    const char* db   = ctx->database ? ctx->database : "?";

    fprintf(g_logfile,
            "%s | sid=%-6lu | %s@%s | %-5s | %-10s | effects=0x%04X | #%u | %.512s\n",
            ts, (unsigned long)ctx->session_id,
            user, db, rw, qt,
            ctx->effect_flags, ctx->query_count, sql);
    fflush(g_logfile);

    return true;  /* always continue */
}

/**
 * Hook callback: log routing decision (BEFORE_ROUTE).
 *
 * Appended as an indented continuation line beneath the query log entry,
 * showing the routing hint (PRIMARY / REPLICA / ANY), the needs_primary
 * flag, and the splice eligibility flag.  Useful for verifying that the
 * router's classification matches expectations.
 *
 * @param ctx  Hook context.  Fields used: session_id, route_hint,
 *             needs_primary, splice_eligible.
 * @return     Always true — logging only.
 */
static bool hook_log_route(keel_hook_ctx_t* ctx) {
    if (!g_logfile) return true;

    static const char* route_names[] = { "PRIMARY", "REPLICA", "ANY" };
    const char* route = (ctx->route_hint < 3)
                            ? route_names[ctx->route_hint]
                            : "UNKNOWN";

    fprintf(g_logfile,
            "  → route: sid=%-6lu → %s (needs_primary=%d, splice=%d)\n",
            (unsigned long)ctx->session_id,
            route, ctx->needs_primary, ctx->splice_eligible);
    fflush(g_logfile);

    return true;
}

/* -------------------------------------------------------------------------- */
/* Plugin shutdown                                                             */
/* -------------------------------------------------------------------------- */

/**
 * Release the log file handle.
 *
 * Called by Keel's hook subsystem during graceful shutdown or plugin
 * unload.  Skips closing stderr (the fallback handle) to avoid breaking
 * the process's standard error stream.
 */
static void plugin_shutdown(void) {
    if (g_logfile && g_logfile != stderr) {
        fclose(g_logfile);
        g_logfile = NULL;
    }
}

/* -------------------------------------------------------------------------- */
/* Hook definitions                                                            */
/* -------------------------------------------------------------------------- */

/**
 * Hook definition table — registers two hooks at different pipeline points.
 *
 * Both hooks run at priority 200 (the recommended range for logging/auditing).
 * They share a single log file and produce complementary lines:
 * hook_log_query emits the query line, hook_log_route emits the routing line.
 */
static const keel_hook_plugin_def_t s_hooks[] = {
    {
        .point    = KEEL_HOOK_AFTER_QUERY_PARSE,
        .name     = "native-query-logger",
        .fn       = hook_log_query,
        .priority = 200,
    },
    {
        .point    = KEEL_HOOK_BEFORE_ROUTE,
        .name     = "native-route-logger",
        .fn       = hook_log_route,
        .priority = 200,
    },
};

/**
 * Plugin metadata returned to Keel's hook loader.
 *
 * The api_version field must match KEEL_HOOK_PLUGIN_API_VERSION at the
 * time the plugin was compiled.  Keel rejects plugins with a mismatched
 * version to prevent ABI incompatibilities.
 */
static const keel_hook_plugin_info_t s_plugin_info = {
    .api_version = KEEL_HOOK_PLUGIN_API_VERSION,
    .name        = "query_logger_plugin",
    .version     = "1.0.0",
    .description = "Logs every query and routing decision to a file",
    .hooks       = s_hooks,
    .hook_count  = sizeof(s_hooks) / sizeof(s_hooks[0]),
    .shutdown    = plugin_shutdown,
};

/* -------------------------------------------------------------------------- */
/* Plugin entry point — called by keel_hook_load_plugin() via dlsym()         */
/* -------------------------------------------------------------------------- */

/**
 * Plugin entry point — called by keel_hook_load_plugin() via dlsym().
 *
 * Opens the log file (path from KEEL_PLUGIN_LOG env var, default
 * /tmp/keel_plugin_queries.log) in append mode.  Falls back to stderr
 * if the file cannot be opened.
 *
 * @return  Pointer to the static plugin info struct.  Keel reads the
 *          hooks array and registers each callback in priority order.
 */
const keel_hook_plugin_info_t* keel_hook_plugin_init(void) {
    const char* path = getenv("KEEL_PLUGIN_LOG");
    if (!path) path = "/tmp/keel_plugin_queries.log";

    g_logfile = fopen(path, "a");
    if (!g_logfile) {
        g_logfile = stderr;  /* fallback */
    }

    return &s_plugin_info;
}
