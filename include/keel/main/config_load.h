/**
 * @file config_load.h
 * @brief Configuration loading, global runtime instances, logging plugin init,
 *        cluster callbacks, and startup-failure cleanup.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#include "keel/core/ini.h"
#include "keel/core/admin.h"
#include "keel/core/cluster.h"
#include "keel/core/stats.h"
#include "keel/core/query_rules.h"
#include "keel/trace/trace.h"
#include "keel/log/audit_log.h"
#include "keel/log/log_plugin.h"
#include "keel/log/query_log.h"
#include "keel/engine/engine.h"
/* worker_group_t needed for cluster_setup_callbacks prototype */
#include "keel/main/worker_group.h"

/* ============================================================================
 * Configuration Structs
 * ============================================================================ */

typedef struct keel_proxy_config {
    const char* listen_addr;
    uint16_t    listen_port;
    const char* backend_host;
    uint16_t    backend_port;
    const char* backend_user;
    const char* backend_password;
    const char* backend_database;

    /* Pool settings */
    size_t      pool_min_size;
    size_t      pool_max_size;
    uint64_t    idle_timeout_ms;
    uint64_t    connect_timeout_ms;

    /* Worker settings */
    uint32_t    num_workers;
    bool        pin_workers;

    /* Session settings */
    size_t      session_pool_size;
    size_t      buffer_size;

    /* Protocol */
    const char* default_protocol;

    /* Logging */
    int         log_level;

    /* Stats / Instrumentation */
    const char* stats_level_str;
    uint32_t    stats_interval_ms;
    uint32_t    hotpath_instr_mask;
    uint32_t    instr_mask;

    /* Config file path (for SIGHUP reload) */
    const char* config_file;
    bool        experimental_features;

    /* Graceful shutdown drain timeout */
    uint32_t    shutdown_timeout_ms;

    /* Declarative query rules (BEFORE_ROUTE hook) */
    keel_query_rules_t* query_rules;
} keel_proxy_config_t;

typedef struct keel_logging_config {
    const char* plugin_name;
    const char* plugin_path;
    const char* log_file;
    const char* syslog_ident;
    const char* syslog_facility;
    const char* log_level_str;
    const char* query_log_mode_str;
    bool        log_timestamps;
    bool        log_source;
    bool        log_dest;
    bool        log_username;
    bool        log_database;
    bool        log_query_tree;
    size_t      max_query_len;
    bool        use_colors;
    bool        json_format;
} keel_logging_config_t;

/* ============================================================================
 * Global Instances (definitions live in config_load.c)
 * ============================================================================ */

extern keel_proxy_config_t      g_config;
extern keel_logging_config_t    g_log_cfg;
extern keel_admin_config_t      g_admin_cfg;
extern keel_admin_t*            g_admin;
extern keel_cluster_config_t    g_cluster_cfg;
extern keel_cluster_t*          g_cluster;
extern keel_trace_config_t      g_trace_cfg;
extern keel_tracer_t*           g_tracer;
extern keel_audit_log_t         g_audit_log;
extern bool                     g_audit_log_initialized;
extern keel_log_plugin_t*       g_log_plugin;
extern keel_query_log_t         g_query_log;
extern keel_engine_t*           g_engine;  /**< First (or only) engine; used for stats/admin */

#ifdef KEEL_HAS_OTLP
#include "../observability/otlp/keel_otlp_aggregator.h"
#include "../observability/otlp/keel_otlp_config.h"
#include "../observability/otlp/keel_otlp_exporter.h"
extern keel_otlp_exporter_config_t g_otlp_cfg;
extern bool                        g_otlp_enabled;
extern keel_otlp_exporter_t*       g_otlp_exporter;
extern keel_otlp_aggregator_t*     g_otlp_agg;
#endif

/* ============================================================================
 * Functions
 * ============================================================================ */

/**
 * @brief Load, parse, and materialize all configuration sections.
 *
 * Loads @p path (INI or YAML), validates the schema version, parses all
 * [worker_group.*], [logging], [stats], [instrument], [query_rule.*],
 * [admin], [prometheus], [observability], [cluster], [security], and
 * [tracing] sections into their respective globals.  Applies KEEL_CLUSTER_*
 * environment-variable overrides at the end.
 *
 * @param path       Config file path.  When NULL only env overrides are applied.
 * @param[out] fatal Set to true when the error is unrecoverable (schema mismatch,
 *                   etc.).  False on a non-fatal file-load warning.  May be NULL.
 * @return Loaded keel_config_t*, or NULL on failure.  The caller owns the pointer
 *         and must call keel_config_free() after all engines have stopped.
 */
keel_config_t* config_load(const char* path, bool* fatal);

/**
 * @brief Initialise the logging plugin and query logger from g_log_cfg.
 *
 * Must be called after config_load() and before any subsystem that logs.
 */
void log_init(void);

/**
 * @brief Wire cluster notification and stats callbacks.
 *
 * Must be called after cluster_start() succeeds.
 *
 * @param cluster  Running cluster manager.
 * @param engine   First (primary) engine for stats reporting.
 * @param groups   Worker-group array (g_groups).
 */
void cluster_setup_callbacks(keel_cluster_t* cluster,
                              keel_engine_t*  engine,
                              worker_group_t* groups);

/**
 * @brief Tear down all subsystems that were successfully started before a
 * startup failure.
 *
 * @param config        Config tree to free (may be NULL).
 * @param tls_initialized  True when keel_tls_init() succeeded.
 */
void cleanup_startup_failure(keel_config_t* config, bool tls_initialized);
