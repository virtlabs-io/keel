/**
 * @file worker_group.h
 * @brief Worker-group runtime descriptor, defaults, and configuration helpers.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "keel/engine/engine.h"
#include "keel/engine/runtime_mode.h"
#include "keel/engine/backend_pool.h"
#include "keel/probe/probe.h"
#include "keel/failover/failover_manager.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/tls_auto.h"
#include "keel/core/auth.h"
#include "keel/core/router.h"
#include "keel/core/router_plugin.h"
#include "keel/core/throttle.h"
#include "keel/core/router_discovery.h"
#include "keel/core/ini.h"
#include "keel_hook.h"

/* ============================================================================
 * Constants
 * ============================================================================ */

#define KEEL_MAX_WORKER_GROUPS 64

/* ============================================================================
 * Per-Worker-Group Runtime State
 * ============================================================================ */

/**
 * Each worker group gets its own listen socket, engine, server pool, and probe.
 */
typedef struct worker_group {
    /* INI section names */
    char            section[256];
    char            servers_section[280];

    /* Per-group proxy settings */
    const char*     name;
    const char*     listen_addr;
    uint16_t        listen_port;
    const char*     default_protocol;
    uint32_t        num_workers;
    bool            pin_workers;
    size_t          pool_min_size;
    size_t          pool_max_size;
    size_t          session_pool_size;
    size_t          buffer_size;
    uint32_t        max_clients;              /**< Total max frontend connections (0=unlimited) */
    uint64_t        idle_timeout_ms;
    uint64_t        connect_timeout_ms;
    uint32_t        pool_prune_interval_ms;   /**< How often to drop idle pool conns */
    uint32_t        pool_refill_interval_ms;  /**< Pool reconnect poll period */
    uint32_t        pool_refill_backoff_ms;   /**< Slower poll when pool is full */
    uint32_t        pool_max_waiting;         /**< Max queued sessions (0=auto) */
    uint64_t        pool_wait_timeout_ms;     /**< Max wait for pool slot — 0=use connect_timeout_ms */
    size_t          session_max_buffered_bytes; /**< Per-session buffer cap — 0=unlimited */
    size_t          backend_max_replay_bytes;   /**< Per-backend PS replay cap — 0=unlimited */
    uint32_t        listen_backlog;           /**< TCP listen() queue depth */
    bool            use_buf_rings;            /**< io_uring buf rings for recv */
    uint32_t        buf_ring_size;            /**< Buf ring slots (0 = queue_depth) */
    bool            sqpoll;                   /**< io_uring SQ polling (kernel thread) */
    uint32_t        sqpoll_idle_ms;           /**< SQ poll thread idle timeout */
    const char*     backend_host;
    uint16_t        backend_port;
    const char*     backend_user;
    const char*     backend_password;
    const char*     backend_database;

    /* Server pool for R/W splitting */
    keel_server_pool_t   server_pool;

    /* Probe config */
    keel_probe_config_t  probe_cfg;
    char                probe_type_buf[64];
    char                probe_extra_buf[64];
    char                probe_user_buf[128];
    char                probe_password_buf[128];
    char                probe_auth_buf[32];

    /* Runtime handles */
    int                  listen_fd;
    keel_engine_t*       engine;
    keel_probe_manager_t* probe_mgr;
    keel_failover_manager_t* failover_mgr;
    keel_hook_registry_t* hook_registry;

    /* Prepared-statement pooling strategy */
    keel_ps_mode_t       ps_mode;      /**< virtualize / pinning / tracking / anonymous */

    /* Runtime mode tier */
    keel_tier_t          runtime_mode; /**< proxy / pool / smart / full */
    bool                 experimental_features;

    /* Replication uncertainty tracking */
    bool                 txn_tracking;     /**< transaction_tracking = on|off */
    bool                 wal_lsn_capture;  /**< wal_lsn_capture = on|off */
    bool                 gtid_capture;     /**< gtid_capture = on|off */

    /* Zero-copy fast network path */
    bool                 fast_network_path; /**< fast_network_path = on|off */
    bool                 result_cache;      /**< result_cache = on|off */
    bool                 scatter_merge_enabled; /**< scatter_merge = on|off */
    bool                 sharding_enabled;      /**< shard routing enabled */
    bool                 hooks_enabled;         /**< any hook chain enabled */

    /* Sticky-primary TTL (0 = disabled) */
    uint32_t             sticky_primary_ttl_ms;

    /* Failover-manager policy */
    keel_failover_config_t failover_cfg;

    /* Connection rebalancing */
    bool                 rebalance_enabled;
    uint32_t             rebalance_interval_ms;
    uint32_t             rebalance_threshold_pct;
    uint32_t             rebalance_max_per_tick;

    /* Scatter-merge memory budget and spill directory */
    size_t               scatter_merge_max_mem_bytes;
    char                 scatter_merge_spill_dir_buf[512];

    /* TLS configuration for client connections */
    keel_tls_config_t    tls_config;
    char                 tls_cert_file_buf[512];
    char                 tls_key_file_buf[512];
    char                 tls_ca_file_buf[512];
    char                 tls_mode_buf[32];
    char                 tls_verify_buf[32];

    /* TLS configuration for backend connections */
    keel_tls_config_t    backend_tls_config;
    char                 backend_tls_cert_file_buf[512];
    char                 backend_tls_key_file_buf[512];
    char                 backend_tls_ca_file_buf[512];
    char                 backend_tls_mode_buf[32];
    char                 backend_tls_verify_buf[32];

    /* Auto-generated TLS certificates */
    bool                 tls_auto_generate;
    char                 tls_auto_dir_buf[512];
    keel_tls_auto_result_t tls_auto_result;

    /* Enterprise authentication configuration */
    keel_auth_method_t   auth_method;
    char                 auth_method_buf[32];
    /* LDAP */
    char                 auth_ldap_url_buf[256];
    char                 auth_ldap_base_dn_buf[256];
    char                 auth_ldap_bind_dn_buf[256];
    char                 auth_ldap_bind_password_buf[128];
    char                 auth_ldap_search_filter_buf[256];
    char                 auth_ldap_dn_suffix_buf[256];
    bool                 auth_ldap_start_tls;
    int                  auth_ldap_timeout_s;
    /* PAM */
    char                 auth_pam_service_buf[64];
    /* auth_query */
    char                 auth_query_buf[512];
    char                 auth_query_conn_buf[512];
    /* userlist */
    char                 auth_userlist_file_buf[512];

    /* Connection lifecycle */
    uint64_t             pool_max_connection_age_ms;

    /* Static storage for server connection-string fields */
    char server_bufs[KEEL_MAX_SERVERS][7][256];

    /* Shard router */
    keel_router_t*          router;

    /* Router plugin manager */
    keel_router_mgr_t*      router_mgr;

    /* Throttle rules */
    keel_throttle_rules_t*  throttle_rules;

    /* Background topology discovery */
    keel_discovery_t*       discovery;
} worker_group_t;

/* ============================================================================
 * Global Group State
 * ============================================================================ */

extern size_t         g_num_groups;
extern worker_group_t g_groups[KEEL_MAX_WORKER_GROUPS];
extern bool           g_experimental_features_enabled;
extern size_t         g_query_rule_count;
extern size_t         g_throttle_rule_count;
extern size_t         g_shard_rule_count;

/* ============================================================================
 * Callback / Collector Types
 * ============================================================================ */

/** Accumulator used by reload_collect_server_keys(). */
typedef struct {
    struct { const char* key; const char* val; } entries[KEEL_MAX_SERVERS];
    size_t nentries;
} reload_srv_ctx_t;

typedef struct {
    size_t* count;
    size_t  max;
} wg_collect_ctx_t;

typedef struct {
    const char** keys;
    const char** vals;
    size_t       count;
    size_t       cap;
} srv_collect_ctx_t;

/* ============================================================================
 * Functions
 * ============================================================================ */

/**
 * @brief Initialize a worker-group descriptor with conservative runtime defaults.
 */
void worker_group_defaults(worker_group_t* g);

/**
 * @brief Config-section iterator callback: discovers top-level worker_group.* sections.
 */
void collect_worker_groups(const char* sec, void* ctx);

/**
 * @brief Config-section iterator callback: copies key/value pairs into a bounded buffer.
 */
void collect_srv_keys(const char* key, const char* value, void* ctx);

/**
 * @brief Read a boolean key from a config section with a caller-supplied default.
 */
bool config_bool_enabled(const keel_config_t* config,
                         const char* section,
                         const char* key,
                         bool default_val);

/**
 * @brief Append a feature name to a comma-separated buffer.
 */
void append_feature_name(char* buf, size_t cap, const char* name, bool* first);

/**
 * @brief Build the enabled-feature list string for a worker group.
 */
void build_runtime_feature_list(const worker_group_t* wg,
                                bool cluster_compression_enabled,
                                char* out,
                                size_t out_cap);

/**
 * @brief Validate that all experimental features have the required opt-in flag set.
 *
 * @return true when all gates pass, false when any violation is detected.
 */
bool validate_experimental_feature_gates(bool cluster_compression_enabled);

/**
 * @brief Config-iterator callback: accumulates server key/value pairs for reload.
 */
void reload_collect_server_keys(const char* key, const char* value, void* ctx);

/**
 * @brief Reload a single worker group's live-tunable parameters from a fresh config snapshot.
 *
 * @param config Freshly loaded configuration tree.
 * @param wg     Worker-group descriptor to inspect and mutate.
 * @return Number of live settings actually changed and applied.
 */
int reload_worker_group(keel_config_t* config, worker_group_t* wg);
