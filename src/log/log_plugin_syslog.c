/**
 * @file log_plugin_syslog.c
 * @brief Built-in syslog sink for integration with host log daemons.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Sends log messages to the local syslog daemon. Maps KEEL log levels
 * to syslog priorities:
 *
 *   TRACE / DEBUG  →  LOG_DEBUG
 *   INFO           →  LOG_INFO
 *   WARN           →  LOG_WARNING
 *   ERROR          →  LOG_ERR
 *   FATAL          →  LOG_CRIT
 *
 * Plugin-specific config keys (in [logging] section):
 *   syslog_ident    – openlog ident string (default: "keel")
 *   syslog_facility – LOG_LOCAL0 … LOG_LOCAL7 | LOG_DAEMON (default: LOG_DAEMON)
 *
 * Tradeoffs:
 *   - syslog is a good operational integration point when the host already has
 *     centralized collection, rotation, and forwarding policy.
 *   - The sink sacrifices some fidelity compared with file/stdout sinks because
 *     records are flattened into one message string before submission.
 */

#include "keel/log/log_plugin.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <pthread.h>

/* ============================================================================
 * Private State
 * ============================================================================ */

typedef struct {
    char    ident[64];
    int     facility;
    bool    opened;
    pthread_mutex_t mtx;
} syslog_priv_t;

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Map KEEL log severity to syslog priority.
 *
 * @param level KEEL log level.
 * @return Matching syslog priority constant.
 */
static int level_to_priority(keel_log_level_t level)
{
    switch (level) {
    case KEEL_LOG_TRACE:  return LOG_DEBUG;
    case KEEL_LOG_DEBUG:  return LOG_DEBUG;
    case KEEL_LOG_INFO:   return LOG_INFO;
    case KEEL_LOG_WARN:   return LOG_WARNING;
    case KEEL_LOG_ERROR:  return LOG_ERR;
    case KEEL_LOG_FATAL:  return LOG_CRIT;
    default:             return LOG_NOTICE;
    }
}

/**
 * @brief Parse a facility name into a syslog facility constant.
 *
 * @param s Facility string.
 * @return Syslog facility constant.
 */
static int parse_facility(const char* s)
{
    if (!s) return LOG_DAEMON;
    if (strcasecmp(s, "daemon")  == 0) return LOG_DAEMON;
    if (strcasecmp(s, "local0")  == 0) return LOG_LOCAL0;
    if (strcasecmp(s, "local1")  == 0) return LOG_LOCAL1;
    if (strcasecmp(s, "local2")  == 0) return LOG_LOCAL2;
    if (strcasecmp(s, "local3")  == 0) return LOG_LOCAL3;
    if (strcasecmp(s, "local4")  == 0) return LOG_LOCAL4;
    if (strcasecmp(s, "local5")  == 0) return LOG_LOCAL5;
    if (strcasecmp(s, "local6")  == 0) return LOG_LOCAL6;
    if (strcasecmp(s, "local7")  == 0) return LOG_LOCAL7;
    if (strcasecmp(s, "user")    == 0) return LOG_USER;
    return LOG_DAEMON;
}

/* ============================================================================
 * VTable Implementation
 * ============================================================================ */

/**
 * @brief Open the syslog sink and call `openlog()`.
 *
 * @param p Plugin instance.
 * @param cfg Sink configuration.
 * @return `KEEL_OK` on success or `KEEL_ERR_NOMEM` on allocation failure.
 */
static keel_error_t syslog_plugin_open(keel_log_plugin_t* p,
                                      const keel_log_plugin_config_t* cfg)
{
    syslog_priv_t* priv = (syslog_priv_t*)keel_calloc(1, sizeof(*priv));
    if (!priv) return KEEL_ERR_NOMEM;

    pthread_mutex_init(&priv->mtx, NULL);

    /* Defaults */
    snprintf(priv->ident, sizeof(priv->ident), "keel");
    priv->facility = LOG_DAEMON;

    /* Apply config */
    if (cfg) {
        if (cfg->ident && cfg->ident[0]) {
            snprintf(priv->ident, sizeof(priv->ident), "%s", cfg->ident);
        }
        for (size_t i = 0; i < cfg->nopts; i++) {
            if (strcmp(cfg->opts[i].key, "syslog_ident") == 0) {
                snprintf(priv->ident, sizeof(priv->ident), "%s", cfg->opts[i].value);
            } else if (strcmp(cfg->opts[i].key, "syslog_facility") == 0) {
                priv->facility = parse_facility(cfg->opts[i].value);
            }
        }
    }

    openlog(priv->ident, LOG_PID | LOG_NDELAY, priv->facility);
    priv->opened = true;

    p->priv = priv;
    return KEEL_OK;
}

/**
 * @brief Flatten one structured record into a syslog message.
 *
 * @param p Plugin instance.
 * @param rec Structured log record.
 * @return `KEEL_OK` on success or an error code when the sink is not open.
 */
static keel_error_t syslog_plugin_write(keel_log_plugin_t* p,
                                       const keel_log_record_t* rec)
{
    syslog_priv_t* priv = (syslog_priv_t*)p->priv;
    if (!priv || !priv->opened) return KEEL_ERR_INVALID_STATE;

    int prio = level_to_priority(rec->level);

    /* Build a single message string with structured fields */
    char buf[4096];
    int  off = 0;

    /* Structured fields */
    if (rec->src_addr) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                        "src=%s:%u ", rec->src_addr, (unsigned)rec->src_port);
    }
    if (rec->dst_addr) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                        "dst=%s:%u ", rec->dst_addr, (unsigned)rec->dst_port);
    }
    if (rec->username) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                        "user=%s ", rec->username);
    }
    if (rec->database) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                        "db=%s ", rec->database);
    }
    if (rec->query && rec->query_len > 0) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                        "query=\"%.*s\" ", (int)rec->query_len, rec->query);
    }
    if (rec->query_tree && rec->query_tree_len > 0) {
        off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                        "tree=\"%.*s\" ", (int)rec->query_tree_len, rec->query_tree);
    }

    /* Main message */
    if (rec->message && rec->message_len > 0) {
        size_t left = sizeof(buf) - (size_t)off;
        size_t copy = rec->message_len < left ? rec->message_len : left - 1;
        memcpy(buf + off, rec->message, copy);
        off += (int)copy;
    }
    buf[off] = '\0';

    /* syslog is thread-safe by POSIX, but we serialise anyway to
     * avoid interleaved structured fields from concurrent threads. */
    pthread_mutex_lock(&priv->mtx);
    syslog(prio, "%s", buf);
    pthread_mutex_unlock(&priv->mtx);

    return KEEL_OK;
}

/**
 * @brief Flush the syslog sink.
 *
 * Syslog itself is not buffered at the plugin layer, so this is a no-op.
 *
 * @param p Plugin instance.
 * @return `KEEL_OK`.
 */
static keel_error_t syslog_plugin_flush(keel_log_plugin_t* p)
{
    /* syslog does not buffer — nothing to do */
    (void)p;
    return KEEL_OK;
}

/**
 * @brief Close the syslog sink with `closelog()`.
 *
 * @param p Plugin instance.
 * @return
 */
static void syslog_plugin_close(keel_log_plugin_t* p)
{
    syslog_priv_t* priv = (syslog_priv_t*)p->priv;
    if (priv && priv->opened) {
        closelog();
        priv->opened = false;
    }
}

/**
 * @brief Destroy the syslog sink instance.
 *
 * @param p Plugin instance.
 * @return
 */
static void syslog_plugin_destroy(keel_log_plugin_t* p)
{
    if (p) {
        if (p->priv) {
            syslog_priv_t* priv = (syslog_priv_t*)p->priv;
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
 * @brief Construct the built-in syslog sink plugin.
 *
 * @return Plugin instance, or `NULL` on allocation failure.
 */
keel_log_plugin_t* keel_log_plugin_syslog_create(void)
{
    keel_log_plugin_t* p = (keel_log_plugin_t*)keel_calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->name    = "syslog";
    p->open    = syslog_plugin_open;
    p->write   = syslog_plugin_write;
    p->flush   = syslog_plugin_flush;
    p->close   = syslog_plugin_close;
    p->destroy = syslog_plugin_destroy;

    return p;
}
