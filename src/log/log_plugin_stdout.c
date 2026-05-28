/**
 * @file log_plugin_stdout.c
 * @brief Built-in stdout/stderr sink with human-friendly formatting.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Writes formatted log lines to stdout (INFO and below) or stderr
 * (WARN and above). Supports optional ANSI colour codes when
 * the output is a terminal.
 *
 * This is the default plugin used when no [logging] plugin is
 * configured or when plugin = stdout.
 *
 * Tradeoffs:
 *   - This sink is optimized for operator readability rather than machine
 *     parsing. It emits one formatted line per record and includes selected
 *     structured fields inline.
 *   - A mutex serializes writes so concurrent worker threads cannot interleave
 *     partial lines. That costs a lock per record, but stdout/stderr sinks are
 *     typically used for development and interactive operations rather than
 *     maximum-throughput archival logging.
 */

#include "keel/log/log_plugin.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

/* ============================================================================
 * Private State
 * ============================================================================ */

typedef struct {
    bool            use_colors;
    bool            json_format;
    pthread_mutex_t mtx;
} stdout_priv_t;

/* ANSI colour codes indexed by keel_log_level_t */
static const char* const s_colors[] = {
    "\033[36m",     /* TRACE: cyan    */
    "\033[34m",     /* DEBUG: blue    */
    "\033[32m",     /* INFO:  green   */
    "\033[33m",     /* WARN:  yellow  */
    "\033[31m",     /* ERROR: red     */
    "\033[35m",     /* FATAL: magenta */
};

static const char* const s_reset = "\033[0m";

/* ============================================================================
 * VTable Implementation
 * ============================================================================ */

/**
 * @brief Initialize the stdout/stderr sink state.
 *
 * @param p Plugin instance.
 * @param config Sink configuration.
 * @return `KEEL_OK` on success or `KEEL_ERR_NOMEM` on allocation failure.
 */
static keel_error_t stdout_open(keel_log_plugin_t* p,
                               const keel_log_plugin_config_t* cfg)
{
    stdout_priv_t* priv = (stdout_priv_t*)keel_calloc(1, sizeof(*priv));
    if (!priv) return KEEL_ERR_NOMEM;

    pthread_mutex_init(&priv->mtx, NULL);

    /* Enable colours when stderr is a terminal, unless explicitly disabled */
    priv->use_colors = isatty(STDERR_FILENO);

    if (cfg) {
        for (size_t i = 0; i < cfg->nopts; i++) {
            if (strcmp(cfg->opts[i].key, "use_colors") == 0 ||
                strcmp(cfg->opts[i].key, "use_colours") == 0) {
                priv->use_colors =
                    (strcmp(cfg->opts[i].value, "true") == 0 ||
                     strcmp(cfg->opts[i].value, "yes") == 0 ||
                     strcmp(cfg->opts[i].value, "1") == 0);
            }
            if (strcmp(cfg->opts[i].key, "json_format") == 0) {
                priv->json_format =
                    (strcmp(cfg->opts[i].value, "true") == 0 ||
                     strcmp(cfg->opts[i].value, "yes") == 0 ||
                     strcmp(cfg->opts[i].value, "1") == 0);
            }
        }
    }

    p->priv = priv;
    return KEEL_OK;
}

/**
 * @brief Format and write one log record to stdout or stderr.
 *
 * @param p Plugin instance.
 * @param rec Structured log record.
 * @return `KEEL_OK` on success or an error code when the sink is uninitialized.
 */
static keel_error_t stdout_write(keel_log_plugin_t* p,
                                const keel_log_record_t* rec)
{
    stdout_priv_t* priv = (stdout_priv_t*)p->priv;
    if (!priv) return KEEL_ERR_INVALID_STATE;

    FILE* out = (rec->level >= KEEL_LOG_WARN) ? stderr : stdout;

    /* === JSON format (NDJSON) === */
    if (priv->json_format) {
        pthread_mutex_lock(&priv->mtx);
        keel_log_record_write_json(out, rec);
        pthread_mutex_unlock(&priv->mtx);
        return KEEL_OK;
    }

    bool  colors = priv->use_colors && isatty(fileno(out));

    pthread_mutex_lock(&priv->mtx);

    /* Timestamp */
    if (rec->ts_sec > 0) {
        time_t t = (time_t)rec->ts_sec;
        struct tm tm_buf;
        struct tm* tm = localtime_r(&t, &tm_buf);
        if (tm) {
            char ts[32];
            strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm);
            fprintf(out, "%s.%03d ", ts, (int)(rec->ts_nsec / 1000000));
        }
    }

    /* Level */
    const char* lvl = keel_log_level_name(rec->level);
    if (colors && rec->level < KEEL_LOG_OFF) {
        fprintf(out, "%s%-5s%s ", s_colors[rec->level], lvl, s_reset);
    } else {
        fprintf(out, "%-5s ", lvl);
    }

    /* Source location (if available) */
    if (rec->file && rec->line > 0) {
        const char* base = strrchr(rec->file, '/');
        base = base ? base + 1 : rec->file;
        fprintf(out, "[%s:%d] ", base, rec->line);
    }

    /* Structured fields */
    if (rec->src_addr) {
        fprintf(out, "src=%s:%u ", rec->src_addr, (unsigned)rec->src_port);
    }
    if (rec->dst_addr) {
        fprintf(out, "dst=%s:%u ", rec->dst_addr, (unsigned)rec->dst_port);
    }
    if (rec->username) {
        fprintf(out, "user=%s ", rec->username);
    }
    if (rec->database) {
        fprintf(out, "db=%s ", rec->database);
    }
    if (rec->route_reason) {
        fprintf(out, "route=%s ", rec->route_reason);
    }
    if (rec->latency_us > 0) {
        fprintf(out, "lat=%lluus ", (unsigned long long)rec->latency_us);
    }

    /* Query text */
    if (rec->query && rec->query_len > 0) {
        fprintf(out, "query=\"%.*s\" ", (int)rec->query_len, rec->query);
    }

    /* Parsed query tree */
    if (rec->query_tree && rec->query_tree_len > 0) {
        fprintf(out, "tree=\"%.*s\" ", (int)rec->query_tree_len, rec->query_tree);
    }

    /* Main message */
    if (rec->message && rec->message_len > 0) {
        fwrite(rec->message, 1, rec->message_len, out);
    }

    fputc('\n', out);

    pthread_mutex_unlock(&priv->mtx);
    return KEEL_OK;
}

/**
 * @brief Flush stdout and stderr.
 *
 * @param p Plugin instance.
 * @return `KEEL_OK`.
 */
static keel_error_t stdout_flush(keel_log_plugin_t* p)
{
    (void)p;
    fflush(stdout);
    fflush(stderr);
    return KEEL_OK;
}

/**
 * @brief Flush but do not close stdout/stderr.
 *
 * @param p Plugin instance.
 * @return
 */
static void stdout_close(keel_log_plugin_t* p)
{
    /* stdout/stderr are not ours to close — just flush */
    (void)p;
    fflush(stdout);
    fflush(stderr);
}

/**
 * @brief Destroy the stdout/stderr sink instance.
 *
 * @param p Plugin instance.
 * @return
 */
static void stdout_destroy(keel_log_plugin_t* p)
{
    if (p) {
        if (p->priv) {
            stdout_priv_t* priv = (stdout_priv_t*)p->priv;
            pthread_mutex_destroy(&priv->mtx);
            keel_free(priv);
        }
        keel_free(p);
    }
}

/* ============================================================================
 * Constructor
 * ============================================================================ */

/**
 * @brief Construct the built-in stdout/stderr sink plugin.
 *
 * @return Plugin instance, or `NULL` on allocation failure.
 */
keel_log_plugin_t* keel_log_plugin_stdout_create(void)
{
    keel_log_plugin_t* p = (keel_log_plugin_t*)keel_calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->name    = "stdout";
    p->open    = stdout_open;
    p->write   = stdout_write;
    p->flush   = stdout_flush;
    p->close   = stdout_close;
    p->destroy = stdout_destroy;

    return p;
}
