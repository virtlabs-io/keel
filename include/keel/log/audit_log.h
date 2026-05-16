/**
 * @file audit_log.h
 * @brief Structured security audit log for KEEL.
 *
 * Provides an audit trail of security-relevant events:
 *   - Client connect / disconnect
 *   - Authentication success / failure
 *   - DDL queries (CREATE, DROP, ALTER, TRUNCATE, etc.)
 *   - Admin console commands
 *   - Query-rule blocks (query blocked by declarative rule)
 *   - Query throttle rejections
 *
 * Output is NDJSON (one JSON object per line) or plain text, written to a
 * configurable sink: a file path, "stdout", or "syslog".
 *
 * Configuration (via [audit] INI section):
 * =========================================
 *   enabled = true | false          (default: false)
 *   path    = /var/log/keel/audit.log | stdout | syslog
 *   format  = ndjson | text         (default: ndjson)
 *   events  = auth,connect,ddl,admin,rules
 *             (comma-separated; "all" enables everything; default: all)
 *
 * Thread Safety:
 * ==============
 * All emit functions are thread-safe. A per-instance mutex serialises writes.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_AUDIT_LOG_H
#define KEEL_AUDIT_LOG_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Event type bitmask
 * ============================================================================ */

/**
 * @brief Audit event categories.
 *
 * Each constant is a single bit so they can be ORed together in the event
 * mask stored in @c keel_audit_config_t.event_mask.
 */
typedef enum keel_audit_event_type {
    KEEL_AUDIT_CONNECT       = (1 << 0), /**< New client connection accepted  */
    KEEL_AUDIT_DISCONNECT    = (1 << 1), /**< Client connection closed        */
    KEEL_AUDIT_AUTH_OK       = (1 << 2), /**< Authentication succeeded        */
    KEEL_AUDIT_AUTH_FAIL     = (1 << 3), /**< Authentication failed           */
    KEEL_AUDIT_DDL           = (1 << 4), /**< DDL statement routed            */
    KEEL_AUDIT_ADMIN_CMD     = (1 << 5), /**< Admin console command received  */
    KEEL_AUDIT_RULE_BLOCK    = (1 << 6), /**< Query blocked by declarative rule */
    KEEL_AUDIT_RULE_THROTTLE = (1 << 7), /**< Query rejected by throttle rule */
    KEEL_AUDIT_SCATTER       = (1 << 8), /**< Scatter-merge fan-out executed  */
} keel_audit_event_type_t;

/**
 * @brief Enable all security-relevant event types (default mask).
 *
 * KEEL_AUDIT_SCATTER is intentionally excluded from this mask.  Scatter
 * events fire once per fan-out query; on scatter-heavy workloads this can
 * produce thousands of records per second, overwhelming the audit log and
 * obscuring the higher-priority security events (auth failures, DDL, rules).
 * Enable SCATTER explicitly via  events = scatter  or  events = all,scatter.
 */
#define KEEL_AUDIT_ALL_EVENTS  0x0FFU   /* all classes except SCATTER (1<<8) */

/**
 * @brief Bitmask that enables every event class, including SCATTER.
 *
 * Use this only when scatter-level audit coverage is explicitly needed,
 * e.g. compliance environments that require full query tracing.
 */
#define KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER  0x1FFU

/* ============================================================================
 * Output format
 * ============================================================================ */

typedef enum keel_audit_format {
    KEEL_AUDIT_FORMAT_NDJSON = 0, /**< One JSON object per line (default) */
    KEEL_AUDIT_FORMAT_TEXT,       /**< Human-readable key=value line       */
} keel_audit_format_t;

/* ============================================================================
 * Sink type
 * ============================================================================ */

typedef enum keel_audit_sink {
    KEEL_AUDIT_SINK_FILE   = 0, /**< Write to a file (or stdout)          */
    KEEL_AUDIT_SINK_SYSLOG,     /**< Write via syslog(3)                  */
} keel_audit_sink_t;

/* ============================================================================
 * Configuration
 * ============================================================================ */

typedef struct keel_audit_config {
    bool                  enabled;    /**< Master on/off switch              */
    char                  path[256];  /**< File path, "stdout", or "syslog"  */
    keel_audit_format_t   format;     /**< Output format                     */
    uint32_t              event_mask; /**< Which event types to emit         */
} keel_audit_config_t;

/**
 * @brief Return the default audit log configuration (disabled).
 */
static inline keel_audit_config_t keel_audit_config_default(void)
{
    return (keel_audit_config_t){
        .enabled    = false,
        .path       = "stdout",
        .format     = KEEL_AUDIT_FORMAT_NDJSON,
        .event_mask = KEEL_AUDIT_ALL_EVENTS,
    };
}

/* ============================================================================
 * Audit log handle
 * ============================================================================ */

typedef struct keel_audit_log {
    keel_audit_config_t  config;
    keel_audit_sink_t    sink;        /**< Resolved sink type               */
    FILE                *fp;          /**< FILE* for file/stdout sink        */
    bool                 enabled;     /**< Quick hot-path check              */
    pthread_mutex_t      mutex;       /**< Serialises concurrent writes      */

    /* Stats */
    uint64_t events_emitted;          /**< Total events written              */
    uint64_t events_dropped;          /**< Events dropped (write error)      */
} keel_audit_log_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialise an audit log instance.
 *
 * Opens the configured sink.  On failure the instance is left disabled so
 * that callers can check @c al->enabled rather than aborting.
 *
 * @param al  Caller-owned instance to initialise.
 * @param cfg Configuration to apply.  Copied by value.
 * @return    0 on success, -1 on error (sink open failed).
 */
int  keel_audit_log_init(keel_audit_log_t *al, const keel_audit_config_t *cfg);

/**
 * @brief Load audit configuration from INI config and initialise.
 *
 * Reads the [audit] section.  On absence, al is initialised with defaults
 * (disabled).
 *
 * @param al      Instance to populate.
 * @param config  Parsed INI config handle.
 * @return 0 on success, -1 on error.
 */
int  keel_audit_log_init_from_config(keel_audit_log_t *al, const void *config);

/**
 * @brief Flush and close the audit log sink.
 *
 * Safe to call on a partially-initialised or disabled instance.
 *
 * @param al  Instance to close.
 */
void keel_audit_log_close(keel_audit_log_t *al);

/* ============================================================================
 * Emission — one function per event class for type safety
 * ============================================================================ */

/**
 * @brief Emit a connect or disconnect event.
 *
 * @param al          Audit log instance.
 * @param type        Must be KEEL_AUDIT_CONNECT or KEEL_AUDIT_DISCONNECT.
 * @param client_addr Client IP address string (may be NULL).
 * @param client_port Client TCP port.
 * @param username    Announced username (may be NULL before auth).
 * @param database    Announced database (may be NULL before auth).
 */
void keel_audit_emit_connect(keel_audit_log_t *al,
                             keel_audit_event_type_t type,
                             const char *client_addr,
                             uint16_t    client_port,
                             const char *username,
                             const char *database);

/**
 * @brief Emit an authentication event (success or failure).
 *
 * @param al          Audit log instance.
 * @param type        KEEL_AUDIT_AUTH_OK or KEEL_AUDIT_AUTH_FAIL.
 * @param username    Authenticating username.
 * @param database    Target database.
 * @param client_addr Client address string.
 * @param client_port Client port.
 * @param detail      Optional error detail (NULL on success).
 */
void keel_audit_emit_auth(keel_audit_log_t *al,
                          keel_audit_event_type_t type,
                          const char *username,
                          const char *database,
                          const char *client_addr,
                          uint16_t    client_port,
                          const char *detail);

/**
 * @brief Emit a DDL query event.
 *
 * @param al          Audit log instance.
 * @param username    Client username.
 * @param database    Target database.
 * @param client_addr Client address.
 * @param client_port Client port.
 * @param query       SQL text (truncated to 512 bytes in output).
 */
void keel_audit_emit_ddl(keel_audit_log_t *al,
                         const char *username,
                         const char *database,
                         const char *client_addr,
                         uint16_t    client_port,
                         const char *query);

/**
 * @brief Emit an admin console command event.
 *
 * @param al         Audit log instance.
 * @param client_addr Admin client address.
 * @param client_port Admin client port.
 * @param command    Command text.
 */
void keel_audit_emit_admin_cmd(keel_audit_log_t *al,
                               const char *client_addr,
                               uint16_t    client_port,
                               const char *command);

/**
 * @brief Emit a query-rule block or throttle rejection event.
 *
 * @param al          Audit log instance.
 * @param type        KEEL_AUDIT_RULE_BLOCK or KEEL_AUDIT_RULE_THROTTLE.
 * @param username    Client username.
 * @param database    Target database.
 * @param client_addr Client address.
 * @param client_port Client port.
 * @param query       SQL text.
 * @param rule_name   Name of the matching rule (INI section).
 */
void keel_audit_emit_rule_event(keel_audit_log_t *al,
                                keel_audit_event_type_t type,
                                const char *username,
                                const char *database,
                                const char *client_addr,
                                uint16_t    client_port,
                                const char *query,
                                const char *rule_name);

/**
 * @brief Emit a scatter-merge fan-out audit event.
 *
 * Records that a scatter-merge query was dispatched across @p shard_count
 * shards.  @p failed_shards indicates how many shards returned an error.
 * The @p query is truncated to 512 bytes in the output.
 *
 * @param al            Audit log instance.
 * @param username      Client username.
 * @param database      Target database.
 * @param query         SQL text fanned out to all shards.
 * @param shard_count   Total number of shards targeted.
 * @param failed_shards Number of shards that returned an error.
 * @param elapsed_us    Total wall-clock time of the scatter in microseconds.
 */
void keel_audit_emit_scatter(keel_audit_log_t *al,
                             const char *username,
                             const char *database,
                             const char *query,
                             size_t      shard_count,
                             size_t      failed_shards,
                             uint64_t    elapsed_us);

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Return the canonical string name of an event type.
 *
 * @param type  Audit event type.
 * @return      Static string, never NULL.
 */
const char *keel_audit_event_name(keel_audit_event_type_t type);

/**
 * @brief Parse a comma-separated event list string into a bitmask.
 *
 * Accepts tokens: "connect", "disconnect", "auth", "ddl", "admin",
 * "rules", "throttle", "scatter", "all".  Unknown tokens are ignored.
 *
 * @param str  Input string (e.g. "auth,ddl,admin").
 * @return     Populated event bitmask.
 */
uint32_t keel_audit_parse_events(const char *str);

/**
 * @brief Return current stats (events emitted / dropped) atomically.
 *
 * @param al        Audit log instance.
 * @param emitted   Output: total events written.
 * @param dropped   Output: total events dropped.
 */
void keel_audit_stats(const keel_audit_log_t *al,
                      uint64_t *emitted,
                      uint64_t *dropped);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_AUDIT_LOG_H */
