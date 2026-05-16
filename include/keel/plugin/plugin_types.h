/**
 * @file plugin_types.h
 * @brief Core ↔ Plugin API Types
 *
 * Defines the data types shared between the KEEL core engine and protocol
 * plugins.  These types are DB-agnostic; plugins map their protocol-specific
 * concepts into these canonical forms.
 *
 * OWNERSHIP RULES:
 *   - Core OWNS: state_profile storage, pin enforcement, pool lifecycle
 *   - Plugin OWNS: protocol parsing, consistency_token memory, capability bits
 *   - Shared: kv_list_t entries are plugin-allocated, core-freed on profile clear
 */

#ifndef KEEL_PLUGIN_TYPES_H
#define KEEL_PLUGIN_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Plugin Capability Flags
 * ============================================================================
 *
 * Returned by plugin_get_capabilities().  Core adapts its behavior based on
 * which features the plugin supports.
 */

typedef enum keel_plugin_cap {
    KEEL_PCAP_NONE                   = 0,

    /* Protocol modes */
    KEEL_PCAP_TEXT_PROTOCOL          = (1 << 0),   /**< SQL text protocol */
    KEEL_PCAP_BINARY_PROTOCOL        = (1 << 1),   /**< Binary/prepared protocol */
    KEEL_PCAP_EXTENDED_QUERY         = (1 << 2),   /**< PG extended query / MySQL COM_STMT */

    /* Prepared statement support */
    KEEL_PCAP_PREPARED_DETECT        = (1 << 3),   /**< Can detect PREPARE/EXECUTE */
    KEEL_PCAP_PREPARED_REPREPARE     = (1 << 4),   /**< Can re-prepare on new backend */

    /* Replication position */
    KEEL_PCAP_CONSISTENCY_TOKEN      = (1 << 5),   /**< Can capture LSN/GTID after write */
    KEEL_PCAP_POSITION_WAIT          = (1 << 6),   /**< Can wait for replica to reach pos */
    KEEL_PCAP_GTID                   = (1 << 7),   /**< Supports GTID-based tracking */

    /* Streaming */
    KEEL_PCAP_STREAMING_COPY         = (1 << 8),   /**< COPY IN/OUT (PG) */
    KEEL_PCAP_STREAMING_LOAD         = (1 << 9),   /**< LOAD DATA (MySQL) */

    /* Auth */
    KEEL_PCAP_AUTH_SCRAM             = (1 << 10),  /**< SCRAM-SHA-256 */
    KEEL_PCAP_AUTH_MD5               = (1 << 11),  /**< MD5 auth */
    KEEL_PCAP_AUTH_NATIVE            = (1 << 12),  /**< mysql_native_password */
    KEEL_PCAP_AUTH_CACHING_SHA2      = (1 << 13),  /**< caching_sha2_password */

    /* State management */
    KEEL_PCAP_STATE_PROFILE          = (1 << 14),  /**< Can track SET/session vars */
    KEEL_PCAP_SELECTIVE_RESET        = (1 << 15),  /**< Can do selective SET/RESET */
    KEEL_PCAP_DISCARD_ALL            = (1 << 16),  /**< Has DISCARD ALL equivalent */

    /* Misc */
    KEEL_PCAP_CANCEL_REQUEST         = (1 << 17),  /**< Supports cancel/kill */
    KEEL_PCAP_SSL                    = (1 << 18),  /**< Supports SSL/TLS negotiation */
    KEEL_PCAP_PROBE_HEALTH           = (1 << 19),  /**< Can health-probe backends */
} keel_plugin_cap_t;

typedef uint32_t keel_plugin_caps_t;  /**< Bitwise OR of keel_plugin_cap_t */

/* ============================================================================
 * Plugin Version Info
 * ============================================================================ */

typedef struct keel_plugin_info {
    const char*         name;           /**< Plugin name (e.g., "postgres", "mysql") */
    uint16_t            default_port;   /**< Default listen port */
    uint32_t            api_version;    /**< Plugin API version (KEEL_PLUGIN_API_V1) */
    keel_plugin_caps_t   capabilities;   /**< Supported capabilities bitmask */
} keel_plugin_info_t;

#define KEEL_PLUGIN_API_V1  1

/* ============================================================================
 * Error Classification
 * ============================================================================
 *
 * Plugins categorize server errors into core-understandable classes.
 * Core uses these to decide: close slot, retry, propagate, etc.
 */

typedef enum keel_error_class {
    KEEL_ERR_SQL_ERROR = 0,      /**< SQL error — return to client, backend ok */
    KEEL_ERR_BACKEND_FATAL,      /**< Backend connection is dead (close slot) */
    KEEL_ERR_TRANSIENT,          /**< Transient error — retry candidate */
    KEEL_ERR_AUTH_FAILURE,       /**< Authentication failure */
    KEEL_ERR_PROTO_VIOLATION,    /**< Protocol violation */
    KEEL_ERR_RESOURCE_LIMIT,     /**< too_many_connections, out of memory, etc. */
    KEEL_ERR_IDEMPOTENT_SAFE,    /**< Transient AND safe to retry (read-only) */
} keel_error_class_t;

typedef struct keel_error_info {
    keel_error_class_t   error_class;
    const char*         sqlstate;       /**< 5-char SQLSTATE (PG) or NULL */
    uint32_t            error_code;     /**< MySQL errno or 0 */
    const char*         message;        /**< Human-readable message (plugin-owned) */
    bool                connection_ok;  /**< true if backend is still usable */
} keel_error_info_t;

/* ============================================================================
 * Consistency Token
 * ============================================================================
 *
 * Opaque to core.  Plugin allocates; core stores in session profile and
 * passes back to plugin_replica_reached_token().
 *
 * PG example:  "0/16B3740" (WAL LSN)
 * MySQL example: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5" (GTID set)
 */

#define KEEL_CONSISTENCY_TOKEN_MAX  128

typedef struct keel_consistency_token {
    char        value[KEEL_CONSISTENCY_TOKEN_MAX];  /**< Null-terminated string */
    uint64_t    captured_at_ns;                     /**< Monotonic timestamp */
} keel_consistency_token_t;

/* ============================================================================
 * Streaming Context
 * ============================================================================
 *
 * Returned by plugin_begin_stream().  Plugin owns the opaque ctx pointer;
 * core calls stream_write() and end_stream() through it.
 */

typedef struct keel_stream_ctx {
    void*       plugin_data;    /**< Plugin-owned opaque state */
    bool        is_inbound;     /**< true=client→backend, false=backend→client */
} keel_stream_ctx_t;

/* ============================================================================
 * Backend Probe Result
 * ============================================================================ */

typedef struct keel_probe_result {
    bool        alive;          /**< Backend responded */
    bool        is_primary;     /**< Is this a primary/master? */
    bool        is_replica;     /**< Is this a replica? */
    bool        accepting_rw;   /**< Accepting read-write queries? */
    int32_t     replication_lag_ms;  /**< Estimated replication lag (-1 = unknown) */
    char        server_version[64];  /**< Server version string */
} keel_probe_result_t;

/* ============================================================================
 * Backend Metadata
 * ============================================================================ */

typedef struct keel_backend_meta {
    char        server_version[64];
    char        database[128];
    char        user[128];
    uint32_t    backend_pid;      /**< PG backend PID or MySQL conn ID */
    bool        in_recovery;      /**< PG: pg_is_in_recovery() */
    bool        read_only;        /**< Server-level read_only */
} keel_backend_meta_t;

/* ============================================================================
 * Apply-State Options & Handle
 * ============================================================================
 *
 * Core passes options to plugin_apply_state(); plugin may return an async
 * handle if the operation requires multiple round-trips.
 */

typedef enum keel_apply_mode {
    KEEL_APPLY_SYNC = 0,        /**< Plugin executes synchronously */
    KEEL_APPLY_PRODUCE_FRAMES,  /**< Plugin returns wire frames for core to send */
} keel_apply_mode_t;

typedef struct keel_apply_opts {
    keel_apply_mode_t    mode;
    uint32_t            timeout_ms;     /**< 0 = default (50ms) */
} keel_apply_opts_t;

/** Async handle — core polls until done */
typedef struct keel_apply_handle {
    bool        complete;       /**< Set by plugin when apply is done */
    int         result;         /**< 0 = success, <0 = error */
    const uint8_t* frames;     /**< Wire frames to send (PRODUCE_FRAMES mode) */
    size_t      frames_len;
} keel_apply_handle_t;

/* ============================================================================
 * Cleanup Options
 * ============================================================================ */

typedef enum keel_cleanup_mode {
    KEEL_CLEANUP_SELECTIVE = 0,  /**< Plugin sends minimal SET/RESET based on delta */
    KEEL_CLEANUP_FULL,           /**< Plugin sends DISCARD ALL or equivalent */
    KEEL_CLEANUP_DESTROY,        /**< Close the slot — don't bother cleaning */
} keel_cleanup_mode_t;

typedef struct keel_cleanup_opts {
    keel_cleanup_mode_t  mode;
    uint32_t            timeout_ms;     /**< 0 = default */
} keel_cleanup_opts_t;

/* ============================================================================
 * Plugin Metrics (optional)
 * ============================================================================ */

typedef struct keel_plugin_metrics {
    uint64_t    state_changes;              /**< temp/prepared/listen creates */
    uint64_t    consistency_token_fetches;
    uint64_t    consistency_token_lat_ns;   /**< Last fetch latency */
    uint64_t    cleanup_count;
    uint64_t    cleanup_lat_ns;             /**< Last cleanup latency */
    uint64_t    classify_count;
    uint64_t    errors_classified;
} keel_plugin_metrics_t;

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PLUGIN_TYPES_H */
