/**
 * @file log_plugin_file.c
 * @brief Built-in file sink with append-mode durability-oriented behavior.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Appends formatted log lines to a file. The file is opened in
 * append mode and created if it does not exist.
 *
 * Plugin-specific config keys (in [logging] section):
 *   log_file         – Path to the log file (required)
 *   log_file_rotate  – true/false: reopen on SIGHUP (default: true)
 *   log_file_mode    – File permissions as octal (default: 0644)
 *
 * Design notes:
 *   - The sink opens files in append mode so concurrent writers within one
 *     process preserve record boundaries more reliably and restart behavior is
 *     simple.
 *   - Output is line-buffered by default to balance latency and syscall count.
 *   - Path expansion and parent-directory creation are done once at open time
 *     so steady-state writes stay focused on formatting and `fwrite()`.
 */

#include "keel/log/log_plugin.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>

/* ============================================================================
 * Private State
 * ============================================================================ */

typedef struct {
    FILE*           fp;
    char            path[1024];
    bool            json_format;
    pthread_mutex_t mtx;
} file_priv_t;

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Expand a leading `~` to the current user's `HOME` directory.
 *
 * @param in Input path.
 * @param[out] out Output buffer receiving the expanded path.
 * @param outsz Size of `out`.
 * @return
 */
static void expand_path(const char* in, char* out, size_t outsz)
{
    if (in[0] == '~' && (in[1] == '/' || in[1] == '\0')) {
        const char* home = getenv("HOME");
        if (home) {
            snprintf(out, outsz, "%s%s", home, in + 1);
            return;
        }
    }
    snprintf(out, outsz, "%s", in);
}

/**
 * @brief Create parent directories for a file path, similar to `mkdir -p`.
 *
 * @param path Target file path whose parent directories should exist.
 * @return
 */
static void mkdirs(const char* path)
{
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);

    /* Walk backwards to find the last '/' */
    char* slash = strrchr(tmp, '/');
    if (!slash || slash == tmp) return;
    *slash = '\0';

    /* Recursively create */
    for (char* p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

/* ============================================================================
 * VTable Implementation
 * ============================================================================ */

/**
 * @brief Open the file sink and prepare its private state.
 *
 * @param p Plugin instance.
 * @param cfg Sink configuration.
 * @return `KEEL_OK` on success or an error code on validation or I/O failure.
 */
static keel_error_t file_open(keel_log_plugin_t* p,
                             const keel_log_plugin_config_t* cfg)
{
    file_priv_t* priv = (file_priv_t*)keel_calloc(1, sizeof(*priv));
    if (!priv) return KEEL_ERR_NOMEM;

    pthread_mutex_init(&priv->mtx, NULL);

    /* Determine file path */
    const char* raw_path = NULL;
    if (cfg && cfg->file_path) {
        raw_path = cfg->file_path;
    }
    if (cfg) {
        for (size_t i = 0; i < cfg->nopts; i++) {
            if (strcmp(cfg->opts[i].key, "log_file") == 0) {
                raw_path = cfg->opts[i].value;
            }
        }
    }

    if (!raw_path || raw_path[0] == '\0') {
        keel_free(priv);
        return KEEL_ERR_INVALID_ARG;
    }

    /* Check for JSON format option */
    if (cfg) {
        for (size_t i = 0; i < cfg->nopts; i++) {
            if (strcmp(cfg->opts[i].key, "json_format") == 0) {
                priv->json_format =
                    (strcmp(cfg->opts[i].value, "true") == 0 ||
                     strcmp(cfg->opts[i].value, "yes") == 0 ||
                     strcmp(cfg->opts[i].value, "1") == 0);
            }
        }
    }

    expand_path(raw_path, priv->path, sizeof(priv->path));
    mkdirs(priv->path);

    priv->fp = fopen(priv->path, "a");
    if (!priv->fp) {
        int saved = errno;
        fprintf(stderr, "log_plugin_file: failed to open %s: %s\n",
                priv->path, strerror(saved));
        pthread_mutex_destroy(&priv->mtx);
        keel_free(priv);
        return KEEL_ERR_IO;
    }

    /* Line-buffer the file for reasonable latency */
    setvbuf(priv->fp, NULL, _IOLBF, 0);

    p->priv = priv;
    return KEEL_OK;
}

/**
 * @brief Format and append one log record to the configured file.
 *
 * @param p Plugin instance.
 * @param rec Structured log record.
 * @return `KEEL_OK` on success or an error code when the file sink is not open.
 */
static keel_error_t file_write(keel_log_plugin_t* p,
                              const keel_log_record_t* rec)
{
    file_priv_t* priv = (file_priv_t*)p->priv;
    if (!priv || !priv->fp) return KEEL_ERR_INVALID_STATE;

    /* === JSON format (NDJSON) === */
    if (priv->json_format) {
        pthread_mutex_lock(&priv->mtx);
        keel_log_record_write_json(priv->fp, rec);
        pthread_mutex_unlock(&priv->mtx);
        return KEEL_OK;
    }

    pthread_mutex_lock(&priv->mtx);

    /* Timestamp */
    if (rec->ts_sec > 0) {
        time_t t = (time_t)rec->ts_sec;
        struct tm tm_buf;
        struct tm* tm = localtime_r(&t, &tm_buf);
        if (tm) {
            char ts[32];
            strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm);
            fprintf(priv->fp, "%s.%03d ", ts, (int)(rec->ts_nsec / 1000000));
        }
    }

    /* Level */
    fprintf(priv->fp, "%-5s ", keel_log_level_name(rec->level));

    /* Source location */
    if (rec->file && rec->line > 0) {
        const char* base = strrchr(rec->file, '/');
        base = base ? base + 1 : rec->file;
        fprintf(priv->fp, "[%s:%d] ", base, rec->line);
    }

    /* Structured fields */
    if (rec->src_addr)  fprintf(priv->fp, "src=%s:%u ", rec->src_addr, (unsigned)rec->src_port);
    if (rec->dst_addr)  fprintf(priv->fp, "dst=%s:%u ", rec->dst_addr, (unsigned)rec->dst_port);
    if (rec->username)  fprintf(priv->fp, "user=%s ", rec->username);
    if (rec->database)  fprintf(priv->fp, "db=%s ", rec->database);

    /* Query text */
    if (rec->query && rec->query_len > 0) {
        fprintf(priv->fp, "query=\"%.*s\" ", (int)rec->query_len, rec->query);
    }

    /* Parsed query tree */
    if (rec->query_tree && rec->query_tree_len > 0) {
        fprintf(priv->fp, "tree=\"%.*s\" ", (int)rec->query_tree_len, rec->query_tree);
    }

    /* Message */
    if (rec->message && rec->message_len > 0) {
        fwrite(rec->message, 1, rec->message_len, priv->fp);
    }

    fputc('\n', priv->fp);

    pthread_mutex_unlock(&priv->mtx);
    return KEEL_OK;
}

/**
 * @brief Flush the file sink.
 *
 * @param p Plugin instance.
 * @return `KEEL_OK`.
 */
static keel_error_t file_flush(keel_log_plugin_t* p)
{
    file_priv_t* priv = (file_priv_t*)p->priv;
    if (priv && priv->fp) {
        fflush(priv->fp);
    }
    return KEEL_OK;
}

/**
 * @brief Close the file sink.
 *
 * @param p Plugin instance.
 * @return
 */
static void file_close(keel_log_plugin_t* p)
{
    file_priv_t* priv = (file_priv_t*)p->priv;
    if (priv && priv->fp) {
        fflush(priv->fp);
        fclose(priv->fp);
        priv->fp = NULL;
    }
}

/**
 * @brief Destroy the file sink instance and free its private state.
 *
 * @param p Plugin instance.
 * @return
 */
static void file_destroy(keel_log_plugin_t* p)
{
    if (p) {
        if (p->priv) {
            file_priv_t* priv = (file_priv_t*)p->priv;
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
 * @brief Construct the built-in file sink plugin.
 *
 * @return Plugin instance, or `NULL` on allocation failure.
 */
keel_log_plugin_t* keel_log_plugin_file_create(void)
{
    keel_log_plugin_t* p = (keel_log_plugin_t*)keel_calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->name    = "file";
    p->open    = file_open;
    p->write   = file_write;
    p->flush   = file_flush;
    p->close   = file_close;
    p->destroy = file_destroy;

    return p;
}
