/**
 * @file audit_log.c
 * @brief Structured security audit log implementation.
 *
 * Writes NDJSON or text audit events to a configurable sink (file, stdout,
 * or syslog).  All public functions are thread-safe via a per-instance mutex.
 */

#define _POSIX_C_SOURCE 200809L
#include "keel/log/audit_log.h"
#include "keel/core/ini.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <inttypes.h>
#include <syslog.h>
#include <sys/time.h>

/* ============================================================================
 * Internal helpers
 * ============================================================================ */

/** Return current Unix time in microseconds. */
static int64_t audit_now_us(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000000LL + (int64_t)tv.tv_usec;
}

/** Escape a string for JSON output into buf.  Returns bytes written. */
static size_t json_escape(char *buf, size_t cap, const char *s)
{
    if (!s) {
        if (cap >= 4) { memcpy(buf, "null", 4); return 4; }
        return 0;
    }
    size_t n = 0;
    if (n < cap) buf[n++] = '"';
    for (const unsigned char *p = (const unsigned char *)s; *p && n + 4 < cap; p++) {
        switch (*p) {
        case '"':  buf[n++] = '\\'; buf[n++] = '"';  break;
        case '\\': buf[n++] = '\\'; buf[n++] = '\\'; break;
        case '\n': buf[n++] = '\\'; buf[n++] = 'n';  break;
        case '\r': buf[n++] = '\\'; buf[n++] = 'r';  break;
        case '\t': buf[n++] = '\\'; buf[n++] = 't';  break;
        default:
            if (*p < 0x20) {
                n += (size_t)snprintf(buf + n, cap - n, "\\u%04x", *p);
            } else {
                buf[n++] = (char)*p;
            }
            break;
        }
    }
    if (n < cap) buf[n++] = '"';
    return n;
}

/** Check whether event type is enabled in the mask. */
static inline bool event_enabled(const keel_audit_log_t *al,
                                  keel_audit_event_type_t t)
{
    return al->enabled && (al->config.event_mask & (uint32_t)t) != 0;
}

/* ============================================================================
 * Internal emit
 * ============================================================================ */

typedef struct audit_field {
    const char *key;
    const char *value;  /* NULL → omit field */
} audit_field_t;

/**
 * @brief Format and emit one audit event as an NDJSON line.
 *
 * Serialises @p event_name, @p ts_us, and the provided key/value @p fields
 * into a single JSON object followed by a newline, capped at 2048 bytes.
 * `NULL`-valued fields are silently omitted from the output.
 *
 * Output is sent to syslog (LOG_INFO | LOG_LOCAL5) or flushed to `al->fp`.
 * If the write fails, `al->events_dropped` is incremented instead of
 * `al->events_emitted`.
 *
 * @param al         Audit log handle.
 * @param event_name Event type string (e.g. `"AUTH_OK"`).
 * @param ts_us      Event timestamp in microseconds since epoch.
 * @param fields     Array of key/value pairs to include.
 * @param nfields    Number of entries in @p fields.
 */
static void emit_ndjson(keel_audit_log_t *al,
                         const char *event_name,
                         int64_t ts_us,
                         const audit_field_t *fields,
                         size_t nfields)
{
    char line[2048];
    size_t n = 0;
    size_t cap = sizeof(line);

    n += (size_t)snprintf(line + n, cap - n,
        "{\"ts\":%lld,\"event\":\"%s\"",
        (long long)ts_us, event_name);

    for (size_t i = 0; i < nfields && n + 256 < cap; i++) {
        if (!fields[i].value) continue;
        n += (size_t)snprintf(line + n, cap - n, ",\"%s\":", fields[i].key);
        n += json_escape(line + n, cap - n, fields[i].value);
    }

    if (n + 2 < cap) {
        line[n++] = '}';
        line[n++] = '\n';
    }
    line[n] = '\0';

    if (al->sink == KEEL_AUDIT_SINK_SYSLOG) {
        syslog(LOG_INFO | LOG_LOCAL5, "%s", line);
    } else if (al->fp) {
        if (fputs(line, al->fp) < 0) {
            al->events_dropped++;
            return;
        }
        fflush(al->fp);
    }
    al->events_emitted++;
}

/**
 * @brief Format and emit one audit event as a plain-text log line.
 *
 * Produces a space-delimited line of the form
 * `[AUDIT] ts=<us> event=<name> key1=val1 key2=val2 ...`.
 * `NULL`-valued fields are omitted.  Output is sent to syslog or flushed to
 * `al->fp`; write failures increment `al->events_dropped`.
 *
 * @param al         Audit log handle.
 * @param event_name Event type string (e.g. `"CONNECT"`).
 * @param ts_us      Event timestamp in microseconds since epoch.
 * @param fields     Array of key/value pairs to include.
 * @param nfields    Number of entries in @p fields.
 */
static void emit_text(keel_audit_log_t *al,
                       const char *event_name,
                       int64_t ts_us,
                       const audit_field_t *fields,
                       size_t nfields)
{
    char line[2048];
    size_t n = 0;
    size_t cap = sizeof(line);

    n += (size_t)snprintf(line + n, cap - n,
        "[AUDIT] ts=%lld event=%s", (long long)ts_us, event_name);

    for (size_t i = 0; i < nfields && n + 256 < cap; i++) {
        if (!fields[i].value) continue;
        n += (size_t)snprintf(line + n, cap - n,
            " %s=%s", fields[i].key, fields[i].value);
    }
    if (n + 1 < cap) line[n++] = '\n';
    line[n] = '\0';

    if (al->sink == KEEL_AUDIT_SINK_SYSLOG) {
        syslog(LOG_INFO | LOG_LOCAL5, "%s", line);
    } else if (al->fp) {
        if (fputs(line, al->fp) < 0) {
            al->events_dropped++;
            return;
        }
        fflush(al->fp);
    }
    al->events_emitted++;
}

/**
 * @brief Dispatch one audit event through the configured formatter.
 *
 * Takes the current wall-clock timestamp, acquires the audit log mutex, and
 * delegates to `emit_text()` or `emit_ndjson()` according to
 * `al->config.format`.  All public `keel_audit_emit_*` helpers funnel through
 * this function to guarantee consistent locking and timestamp semantics.
 *
 * @param al         Audit log handle.
 * @param event_name Event type string.
 * @param fields     Key/value field array.
 * @param nfields    Number of entries in @p fields.
 */
static void audit_emit(keel_audit_log_t *al,
                        const char *event_name,
                        const audit_field_t *fields,
                        size_t nfields)
{
    int64_t ts = audit_now_us();
    pthread_mutex_lock(&al->mutex);
    if (al->config.format == KEEL_AUDIT_FORMAT_TEXT)
        emit_text(al, event_name, ts, fields, nfields);
    else
        emit_ndjson(al, event_name, ts, fields, nfields);
    pthread_mutex_unlock(&al->mutex);
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

int keel_audit_log_init(keel_audit_log_t *al, const keel_audit_config_t *cfg)
{
    if (!al || !cfg) return -1;
    memset(al, 0, sizeof(*al));
    al->config = *cfg;
    al->enabled = false;
    pthread_mutex_init(&al->mutex, NULL);

    if (!cfg->enabled) return 0;

    if (strcmp(cfg->path, "syslog") == 0) {
        al->sink = KEEL_AUDIT_SINK_SYSLOG;
        openlog("keel-audit", LOG_PID | LOG_NDELAY, LOG_LOCAL5);
    } else if (strcmp(cfg->path, "stdout") == 0) {
        al->sink = KEEL_AUDIT_SINK_FILE;
        al->fp = stdout;
    } else {
        al->sink = KEEL_AUDIT_SINK_FILE;
        al->fp = fopen(cfg->path, "ae");   /* append, close-on-exec */
        if (!al->fp) {
            fprintf(stderr, "keel-audit: cannot open %s: %s\n",
                    cfg->path, strerror(errno));
            return -1;
        }
    }

    al->enabled = true;
    return 0;
}

/**
 * @brief Initialise an audit log from a `keel_config_t` INI configuration.
 *
 * Reads the `[audit]` section for `enabled`, `path`, `format`, and `events`
 * keys, merging them onto `keel_audit_config_default()`, then delegates to
 * `keel_audit_log_init()`.  When @p config is `NULL` the defaults are used
 * (audit disabled, stdout, NDJSON, all events).
 *
 * @param al     Audit log handle to initialise.
 * @param config Opaque `keel_config_t *` INI tree, or `NULL` for defaults.
 * @return 0 on success, -1 on invalid arguments or log-file open failure.
 */
int keel_audit_log_init_from_config(keel_audit_log_t *al, const void *config)
{
    if (!al) return -1;
    keel_audit_config_t cfg = keel_audit_config_default();

    if (config) {
        const keel_config_t *ini = (const keel_config_t *)config;

        const char *en = keel_config_get_string(ini, "audit", "enabled", "false");
        cfg.enabled = (strcmp(en, "true") == 0 || strcmp(en, "1") == 0);

        const char *path = keel_config_get_string(ini, "audit", "path", "stdout");
        strncpy(cfg.path, path, sizeof(cfg.path) - 1);

        const char *fmt = keel_config_get_string(ini, "audit", "format", "ndjson");
        cfg.format = (strcmp(fmt, "text") == 0)
                     ? KEEL_AUDIT_FORMAT_TEXT
                     : KEEL_AUDIT_FORMAT_NDJSON;

        const char *evs = keel_config_get_string(ini, "audit", "events", "all");
        cfg.event_mask = keel_audit_parse_events(evs);
    }

    return keel_audit_log_init(al, &cfg);
}

/**
 * @brief Close the audit log and release its resources.
 *
 * Flushes and closes the backing file (if it is not `stdout`), calls
 * `closelog()` when the sink is syslog, marks the handle disabled, and
 * destroys the mutex.  After this call the handle may be reinitialised with
 * `keel_audit_log_init()` if needed.
 *
 * @param al Audit log handle to close. `NULL` is a safe no-op.
 */
void keel_audit_log_close(keel_audit_log_t *al)
{
    if (!al) return;
    if (al->fp && al->fp != stdout) {
        fclose(al->fp);
        al->fp = NULL;
    }
    if (al->sink == KEEL_AUDIT_SINK_SYSLOG)
        closelog();
    al->enabled = false;
    pthread_mutex_destroy(&al->mutex);
}

/* ============================================================================
 * Emission
 * ============================================================================ */

void keel_audit_emit_connect(keel_audit_log_t *al,
                              keel_audit_event_type_t type,
                              const char *client_addr,
                              uint16_t    client_port,
                              const char *username,
                              const char *database)
{
    if (!al || !event_enabled(al, type)) return;

    char port_s[8];
    snprintf(port_s, sizeof(port_s), "%u", client_port);

    const audit_field_t fields[] = {
        { "user",   username    },
        { "db",     database    },
        { "client", client_addr },
        { "port",   client_port ? port_s : NULL },
    };
    audit_emit(al, keel_audit_event_name(type),
               fields, sizeof(fields) / sizeof(fields[0]));
}

/**
 * @brief Emit an authentication-outcome audit event.
 *
 * Records the result of a client authentication attempt.  @p type should be
 * `KEEL_AUDIT_AUTH_OK` or `KEEL_AUDIT_AUTH_FAIL`.  The optional @p detail
 * string carries a human-readable reason (e.g. `"wrong password"`).
 *
 * @param al          Audit log handle.
 * @param type        Event type (`KEEL_AUDIT_AUTH_OK` or `KEEL_AUDIT_AUTH_FAIL`).
 * @param username    Authenticating user (may be `NULL`).
 * @param database    Target database (may be `NULL`).
 * @param client_addr Client IP address string (may be `NULL`).
 * @param client_port Client port, or 0 if unknown.
 * @param detail      Optional extra detail string (may be `NULL`).
 */
void keel_audit_emit_auth(keel_audit_log_t *al,
                           keel_audit_event_type_t type,
                           const char *username,
                           const char *database,
                           const char *client_addr,
                           uint16_t    client_port,
                           const char *detail)
{
    if (!al || !event_enabled(al, type)) return;

    char port_s[8];
    snprintf(port_s, sizeof(port_s), "%u", client_port);

    const audit_field_t fields[] = {
        { "user",   username    },
        { "db",     database    },
        { "client", client_addr },
        { "port",   client_port ? port_s : NULL },
        { "detail", detail      },
    };
    audit_emit(al, keel_audit_event_name(type),
               fields, sizeof(fields) / sizeof(fields[0]));
}

/**
 * @brief Emit a DDL statement audit event.
 *
 * Records a data-definition query (`CREATE`, `ALTER`, `DROP`, etc.).
 * The query is truncated to 512 bytes in the log output.
 *
 * @param al          Audit log handle.
 * @param username    Executing user (may be `NULL`).
 * @param database    Target database (may be `NULL`).
 * @param client_addr Client IP address string (may be `NULL`).
 * @param client_port Client port, or 0 if unknown.
 * @param query       SQL text to record (may be `NULL`; truncated to 512 bytes).
 */
void keel_audit_emit_ddl(keel_audit_log_t *al,
                          const char *username,
                          const char *database,
                          const char *client_addr,
                          uint16_t    client_port,
                          const char *query)
{
    if (!al || !event_enabled(al, KEEL_AUDIT_DDL)) return;

    /* Truncate query to 512 bytes in the output by using a temp buffer. */
    char q_trunc[513] = {0};
    if (query) {
        strncpy(q_trunc, query, 512);
        q_trunc[512] = '\0';
    }

    char port_s[8];
    snprintf(port_s, sizeof(port_s), "%u", client_port);

    const audit_field_t fields[] = {
        { "user",   username    },
        { "db",     database    },
        { "client", client_addr },
        { "port",   client_port ? port_s : NULL },
        { "query",  query ? q_trunc : NULL },
    };
    audit_emit(al, "DDL",
               fields, sizeof(fields) / sizeof(fields[0]));
}

/**
 * @brief Emit an administrative command audit event.
 *
 * Records a command issued through the admin interface (e.g. `RELOAD`,
 * `PAUSE`, `SHOW POOLS`).
 *
 * @param al          Audit log handle.
 * @param client_addr Admin client IP address string (may be `NULL`).
 * @param client_port Admin client port, or 0 if unknown.
 * @param command     Command text (may be `NULL`).
 */
void keel_audit_emit_admin_cmd(keel_audit_log_t *al,
                                const char *client_addr,
                                uint16_t    client_port,
                                const char *command)
{
    if (!al || !event_enabled(al, KEEL_AUDIT_ADMIN_CMD)) return;

    char port_s[8];
    snprintf(port_s, sizeof(port_s), "%u", client_port);

    const audit_field_t fields[] = {
        { "client",  client_addr },
        { "port",    client_port ? port_s : NULL },
        { "command", command     },
    };
    audit_emit(al, "ADMIN_CMD",
               fields, sizeof(fields) / sizeof(fields[0]));
}

/**
 * @brief Emit an audit event triggered by a query-rules match.
 *
 * Used by the throttle and block rule engines to record that a rule
 * was applied to a client query.  @p type is typically
 * `KEEL_AUDIT_RULE_BLOCK` or `KEEL_AUDIT_RULE_THROTTLE`.  The query is
 * truncated to 256 bytes in the log output.
 *
 * @param al          Audit log handle.
 * @param type        Event type (block or throttle variant).
 * @param username    Executing user (may be `NULL`).
 * @param database    Target database (may be `NULL`).
 * @param client_addr Client IP address string (may be `NULL`).
 * @param client_port Client port, or 0 if unknown.
 * @param query       SQL text that triggered the rule (may be `NULL`; truncated
 *                    to 256 bytes).
 * @param rule_name   Name of the matched rule (may be `NULL`).
 */
void keel_audit_emit_rule_event(keel_audit_log_t *al,
                                 keel_audit_event_type_t type,
                                 const char *username,
                                 const char *database,
                                 const char *client_addr,
                                 uint16_t    client_port,
                                 const char *query,
                                 const char *rule_name)
{
    if (!al || !event_enabled(al, type)) return;

    char q_trunc[257] = {0};
    if (query) { strncpy(q_trunc, query, 256); q_trunc[256] = '\0'; }

    char port_s[8];
    snprintf(port_s, sizeof(port_s), "%u", client_port);

    const audit_field_t fields[] = {
        { "user",   username    },
        { "db",     database    },
        { "client", client_addr },
        { "port",   client_port ? port_s : NULL },
        { "query",  query ? q_trunc : NULL },
        { "rule",   rule_name   },
    };
    audit_emit(al, keel_audit_event_name(type),
               fields, sizeof(fields) / sizeof(fields[0]));
}

void keel_audit_emit_scatter(keel_audit_log_t *al,
                              const char *username,
                              const char *database,
                              const char *query,
                              size_t      shard_count,
                              size_t      failed_shards,
                              uint64_t    elapsed_us)
{
    if (!al || !event_enabled(al, KEEL_AUDIT_SCATTER)) return;

    char q_trunc[257] = {0};
    if (query) { strncpy(q_trunc, query, 256); q_trunc[256] = '\0'; }

    char shards_s[16], failed_s[16], elapsed_s[32];
    snprintf(shards_s,  sizeof(shards_s),  "%zu",  shard_count);
    snprintf(failed_s,  sizeof(failed_s),  "%zu",  failed_shards);
    snprintf(elapsed_s, sizeof(elapsed_s), "%" PRIu64, elapsed_us);

    const audit_field_t fields[] = {
        { "user",         username          },
        { "db",           database          },
        { "query",        query ? q_trunc : NULL },
        { "shards",       shards_s          },
        { "failed_shards",failed_shards ? failed_s : NULL },
        { "elapsed_us",   elapsed_s         },
    };
    audit_emit(al, "SCATTER",
               fields, sizeof(fields) / sizeof(fields[0]));
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

const char *keel_audit_event_name(keel_audit_event_type_t type)
{
    switch (type) {
    case KEEL_AUDIT_CONNECT:       return "CONNECT";
    case KEEL_AUDIT_DISCONNECT:    return "DISCONNECT";
    case KEEL_AUDIT_AUTH_OK:       return "AUTH_OK";
    case KEEL_AUDIT_AUTH_FAIL:     return "AUTH_FAIL";
    case KEEL_AUDIT_DDL:           return "DDL";
    case KEEL_AUDIT_ADMIN_CMD:     return "ADMIN_CMD";
    case KEEL_AUDIT_RULE_BLOCK:    return "RULE_BLOCK";
    case KEEL_AUDIT_RULE_THROTTLE: return "RULE_THROTTLE";
    case KEEL_AUDIT_SCATTER:       return "SCATTER";
    default:                       return "UNKNOWN";
    }
}

/**
 * @brief Parse a comma-separated event list into a bitmask.
 *
 * Recognises the tokens: `connect`, `disconnect`, `auth`, `auth_ok`,
 * `auth_fail`, `ddl`, `admin`, `rules`, `throttle`, `all`.  Unknown tokens
 * are silently skipped.  The special value `"all"` (or a `NULL` input)
 * returns `KEEL_AUDIT_ALL_EVENTS`.
 *
 * @param str Comma-delimited event name string, or `NULL`.
 * @return Bitmask of `KEEL_AUDIT_*` flags.  Never returns 0; falls back to
 *         `KEEL_AUDIT_ALL_EVENTS` when no token is recognised.
 */
uint32_t keel_audit_parse_events(const char *str)
{
    /* NULL or empty → default (all security events, no scatter) */
    if (!str || str[0] == '\0') return KEEL_AUDIT_ALL_EVENTS;

    uint32_t mask = 0;
    char tmp[256];
    strncpy(tmp, str, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    char *tok = strtok(tmp, ",");
    while (tok) {
        /* trim leading whitespace */
        while (*tok == ' ') tok++;

        if      (strcmp(tok, "connect")    == 0)
            mask |= KEEL_AUDIT_CONNECT | KEEL_AUDIT_DISCONNECT;
        else if (strcmp(tok, "disconnect") == 0)
            mask |= KEEL_AUDIT_DISCONNECT;
        else if (strcmp(tok, "auth")       == 0)
            mask |= KEEL_AUDIT_AUTH_OK | KEEL_AUDIT_AUTH_FAIL;
        else if (strcmp(tok, "auth_ok")    == 0)
            mask |= KEEL_AUDIT_AUTH_OK;
        else if (strcmp(tok, "auth_fail")  == 0)
            mask |= KEEL_AUDIT_AUTH_FAIL;
        else if (strcmp(tok, "ddl")        == 0)
            mask |= KEEL_AUDIT_DDL;
        else if (strcmp(tok, "admin")      == 0)
            mask |= KEEL_AUDIT_ADMIN_CMD;
        else if (strcmp(tok, "rules")      == 0)
            mask |= KEEL_AUDIT_RULE_BLOCK | KEEL_AUDIT_RULE_THROTTLE;
        else if (strcmp(tok, "throttle")   == 0)
            mask |= KEEL_AUDIT_RULE_THROTTLE;
        else if (strcmp(tok, "scatter")    == 0)
            mask |= KEEL_AUDIT_SCATTER;
        else if (strcmp(tok, "all")        == 0)
            mask |= KEEL_AUDIT_ALL_EVENTS;  /* OR in, don't replace — allows "all,scatter" */

        tok = strtok(NULL, ",");
    }
    return mask ? mask : KEEL_AUDIT_ALL_EVENTS;
}

/**
 * @brief Read emission and drop counters from an audit log.
 *
 * Provides a lightweight snapshot of `al->events_emitted` and
 * `al->events_dropped`.  Either output pointer may be `NULL` if that
 * counter is not needed.
 *
 * @param al      Audit log handle (may be `NULL`; outputs are zeroed).
 * @param[out] emitted Number of events successfully written since init.
 * @param[out] dropped Number of events dropped due to write failures.
 */
void keel_audit_stats(const keel_audit_log_t *al,
                      uint64_t *emitted,
                      uint64_t *dropped)
{
    if (!al) { if (emitted) *emitted = 0; if (dropped) *dropped = 0; return; }
    if (emitted) *emitted = al->events_emitted;
    if (dropped) *dropped = al->events_dropped;
}
