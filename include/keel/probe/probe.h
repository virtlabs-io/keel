/**
 * @file probe.h
 * @brief Public API for backend health probing, role detection, and failover orchestration.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The probe subsystem is KEEL's control-plane view of backend health. Its job is
 * not merely to say "socket reachable"; it must determine whether a backend is
 * usable for routing and, when possible, whether it currently acts as read-write
 * primary or read-only replica. That distinction matters because the routing layer
 * and backend pools maintain role-indexed views of the server set.
 *
 * Architecturally, the subsystem splits into three layers:
 *
 * - a small registry that maps configured probe names to implementations;
 * - probe implementations that know how to interrogate one protocol or control
 *   plane, such as PostgreSQL SQL, MySQL SQL, or Patroni HTTP;
 * - a manager thread that periodically executes probes, tracks transitions, and
 *   updates server-pool metadata so workers observe topology changes.
 *
 * This separation keeps the failover policy generic while allowing the wire-level
 * logic to remain protocol-specific.
 */

#ifndef KEEL_PROBE_H
#define KEEL_PROBE_H

#include "keel_types.h"
#include "keel_error.h"

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
typedef struct keel_engine keel_engine_t;
typedef struct keel_backend_server keel_backend_server_t;
typedef struct keel_server_pool   keel_server_pool_t;

/* ============================================================================
 * Health & Role Enums
 * ============================================================================ */

typedef enum keel_health_status {
    KEEL_HEALTH_UNKNOWN  = 0,    /**< Not yet checked */
    KEEL_HEALTH_UP       = 1,    /**< Server is healthy */
    KEEL_HEALTH_DOWN     = 2,    /**< Server is unreachable */
    KEEL_HEALTH_DEGRADED = 3,    /**< Responding but with issues */
} keel_health_status_t;
#define KEEL_SERVER_HEALTH_DEFINED 1

/* Note: server role enum lives in engine.h (KEEL_SERVER_ROLE_PRIMARY/REPLICA/AUTO).
 * The probe uses the same enum. */

/* ============================================================================
 * Probe Result
 * ============================================================================ */

typedef struct keel_probe_check {
    keel_health_status_t health;         /**< Observed health */
    int                 detected_role;  /**< keel_server_role_t from engine.h */
    uint64_t            latency_us;     /**< Round-trip probe latency (µs) */
    keel_error_t         error;          /**< Error code (KEEL_OK when healthy) */
    char                message[256];   /**< Human-readable status */
} keel_probe_check_t;

/* ============================================================================
 * Per-Server State (tracked by probe manager)
 * ============================================================================ */

typedef struct keel_server_state {
    _Atomic int         health;                 /**< keel_health_status_t */
    _Atomic int         detected_role;          /**< keel_server_role_t */
    _Atomic uint32_t    consecutive_failures;
    _Atomic uint64_t    last_check_ns;          /**< Clock monotonic ns */
    _Atomic uint64_t    last_latency_us;
    _Atomic uint64_t    total_checks;
    _Atomic uint64_t    total_failures;
    char                last_error[256];
} keel_server_state_t;

/* ============================================================================
 * Probe Plugin Vtable
 * ============================================================================ */

/**
 * Every probe type implements this vtable.
 *
 * Lifecycle:  create() → check() ... check() → destroy()
 *
 * check() is called from the probe-manager thread, so it must be
 * thread-safe but doesn't need to be reentrant.  It may block
 * (TCP connect, SQL query, HTTP request) up to the configured timeout.
 */
typedef struct keel_probe_ops {
    /** Human-readable name (e.g. "postgres", "patroni") */
    const char* name;

    /**
     * Allocate probe-specific context.
     * @param server  The backend being probed
     * @param extra   Implementation-specific config string (e.g. "8008" for patroni port)
     * @return opaque context, or NULL on error
     */
    void* (*create)(const keel_backend_server_t* server, const char* extra);

    /**
     * Execute one health + role check.
     * @param ctx     Context from create()
     * @param server  Current server config (host/port/user/pass may change)
     * @param result  Output: health, role, latency
     * @return KEEL_OK on successful execution (result.health may still be DOWN)
     */
    keel_error_t (*check)(void* ctx, const keel_backend_server_t* server,
                         keel_probe_check_t* result);

    /**
     * Release probe-specific context.
     */
    void (*destroy)(void* ctx);
} keel_probe_ops_t;

/* ============================================================================
 * Probe Configuration (parsed from INI)
 * ============================================================================ */

typedef struct keel_probe_config {
    const char*             probe_type;     /**< "postgres", "patroni", "tcp", ... */
    const char*             probe_extra;    /**< After ':', e.g. "8008" */
    const char*             probe_user;     /**< Optional user for probes */
    const char*             probe_password; /**< Optional password for probes */
    const char*             probe_auth;     /**< Auth mode: auto|trust|password|md5|scram */
    uint32_t                interval_ms;    /**< Check interval (default 5000) */
    uint32_t                timeout_ms;     /**< Per-check timeout (default 3000) */
    uint32_t                retries;        /**< Failures before DOWN (default 3) */
    uint32_t                failover_delay_ms; /**< Wait before re-routing (default 10000) */

    /**
     * Optional hook registry for firing ON_HEALTH_CHANGE events.
     *
     * When non-NULL and at least one hook is registered for
     * KEEL_HOOK_ON_HEALTH_CHANGE, the probe manager fires the hook on every
     * health state transition.  NULL disables the hook (zero overhead).
     * The registry must outlive the probe manager.
     */
    struct keel_hook_registry* hook_registry; /**< Hook registry (NULL = disabled) */
} keel_probe_config_t;

#define KEEL_PROBE_CONFIG_DEFAULT { \
    .probe_type         = "postgres",   \
    .probe_extra        = NULL,         \
    .probe_user         = NULL,         \
    .probe_password     = NULL,         \
    .probe_auth         = "auto",      \
    .interval_ms        = 5000,         \
    .timeout_ms         = 3000,         \
    .retries            = 3,            \
    .failover_delay_ms  = 10000,        \
    .hook_registry      = NULL,         \
}

/* ============================================================================
 * Probe Registry
 * ============================================================================ */

#define KEEL_MAX_PROBE_TYPES 16

/**
 * @brief Register a probe implementation under a symbolic name.
 *
 * Registration is intended for single-threaded startup before any probe manager
 * is created.
 *
 * @param name Probe type name.
 * @param ops Vtable implementing the probe.
 * @return `KEEL_OK` on success, or an error describing invalid input, capacity
 *         exhaustion, or duplicate registration.
 */
keel_error_t keel_probe_register(const char* name, const keel_probe_ops_t* ops);

/**
 * @brief Look up a registered probe implementation.
 *
 * @param name Probe type name.
 * @return Matching vtable, or `NULL` when the name is unknown.
 */
const keel_probe_ops_t* keel_probe_lookup(const char* name);

/**
 * @brief Register all built-in probe implementations.
 *
 * @return
 */
void keel_probe_register_builtins(void);

/* ============================================================================
 * Probe Manager
 * ============================================================================
 *
 * Owns a dedicated pthread that periodically checks all servers in the
 * server pool.  On role change (e.g. primary ↔ replica), it swaps the
 * pool pointers in every worker thread so that routing immediately
 * reflects the new topology.
 */

typedef struct keel_probe_manager keel_probe_manager_t;

/**
 * @brief Create a probe manager for one server pool.
 *
 * @param config Probe timing and retry policy.
 * @param pool Server pool to monitor.
 * @param engine Engine handle used to reach worker-owned backend pools.
 * @return New manager, or `NULL` on allocation or configuration failure.
 */
keel_probe_manager_t* keel_probe_manager_create(
    const keel_probe_config_t* config,
    keel_server_pool_t*        pool,
    keel_engine_t*             engine
);

/**
 * Start the probe thread.
 */
keel_error_t keel_probe_manager_start(keel_probe_manager_t* mgr);

/**
 * Signal the probe thread to stop and join it.
 */
void keel_probe_manager_stop(keel_probe_manager_t* mgr);

/**
 * Free all resources.
 */
void keel_probe_manager_destroy(keel_probe_manager_t* mgr);

/**
 * @brief Update probe timing parameters on a running manager.
 *
 * Safe to call while the probe thread is running — values are read on
 * the next probe cycle (no locking needed, single-writer from main thread).
 *
 * @param mgr          Manager to update
 * @param interval_ms  New probe interval (0 = no change)
 * @param timeout_ms   New probe timeout (0 = no change)
 * @param retries      New failure retries (0 = no change)
 */
void keel_probe_manager_update_timing(keel_probe_manager_t* mgr,
                                       uint32_t interval_ms,
                                       uint32_t timeout_ms,
                                       uint32_t retries);

/**
 * @brief Access per-server probe state.
 *
 * @param mgr Manager instance.
 * @param server_idx Server index in the monitored pool.
 * @return Pointer to immutable per-server state, or `NULL` when the index is out
 *         of range.
 */
const keel_server_state_t* keel_probe_manager_get_state(
    const keel_probe_manager_t* mgr, size_t server_idx);

/* ============================================================================
 * Utility
 * ============================================================================ */

/**
 * @brief Convert a health enum to a stable printable string.
 *
 * @param s Health-status enum value.
 * @return Static string representation.
 */
const char* keel_health_status_str(keel_health_status_t s);

/* ============================================================================
 * Built-in Probe Externs
 * ============================================================================ */

extern const keel_probe_ops_t keel_probe_postgres_ops;
extern const keel_probe_ops_t keel_probe_patroni_ops;
extern const keel_probe_ops_t keel_probe_mysql_ops;

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PROBE_H */
