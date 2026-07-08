/**
 * @file config_load.c
 * @brief Global configuration instances, config loading, logging init,
 *        cluster callbacks, and startup-failure cleanup.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Extracted from src/main/main.c — no logic changes, only relocation.
 * Exception: strict_auth validation moved to main() (after config_load returns).
 */

#include "keel/main/config_load.h"
#include "keel/main/worker_group.h"
#include "keel/main/security.h"
#include "keel/main/stats_display.h"

#include "keel/engine/engine.h"
#include "keel/engine/worker.h"
#include "keel/engine/backend_pool.h"
#include "keel/core/ini.h"
#include "keel/core/config_reload.h"
#include "keel/core/config_migrate.h"
#include "keel/core/config_yaml.h"
#include "keel/core/ini.h"
#include "keel/core/admin.h"
#include "keel/core/cluster.h"
#include "keel/core/stats.h"
#include "keel/core/query_rules.h"
#include "keel/core/throttle.h"
#include "keel/core/sharding.h"
#include "keel/core/auth.h"
#include "keel/core/router.h"
#include "keel/core/router_plugin.h"
#include "keel/core/router_discovery.h"
#include "keel/trace/trace.h"
#include "keel/log/log.h"
#include "keel/log/log_plugin.h"
#include "keel/log/query_log.h"
#include "keel/log/audit_log.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/tls_auto.h"
#include "keel/mem/mem.h"
#include "keel/probe/probe.h"
#include "keel/failover/failover_manager.h"
#include "keel_hook.h"
#include "keel_error.h"

#ifdef KEEL_HAS_OTLP
#include "../observability/otlp/keel_otlp_aggregator.h"
#include "../observability/otlp/keel_otlp_config.h"
#include "../observability/otlp/keel_otlp_exporter.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

/* ============================================================================
 * Global Variable Definitions
 * ============================================================================ */

keel_proxy_config_t g_config = {
    .listen_addr         = "0.0.0.0",
    .listen_port         = 6432,
    .backend_host        = "127.0.0.1",
    .backend_port        = 5432,
    .backend_user        = "postgres",
    .backend_password    = NULL,
    .backend_database    = "postgres",
    .pool_min_size       = 2,
    .pool_max_size       = 100,
    .idle_timeout_ms     = 300000,
    .connect_timeout_ms  = 10000,
    .num_workers         = 0,
    .pin_workers         = true,
    .session_pool_size   = 1024,
    .buffer_size         = 65536,
    .default_protocol    = "postgres",
    .log_level           = 2,
    .stats_level_str     = "basic",
    .stats_interval_ms   = 0,
    .hotpath_instr_mask  = KEEL_HOT_INSTR_ALL,
    .instr_mask          = KEEL_INSTR_CAT_NONE,
    .config_file         = NULL,
    .experimental_features = false,
    .shutdown_timeout_ms = 30000,
};

keel_logging_config_t g_log_cfg = {
    .plugin_name       = "stdout",
    .plugin_path       = NULL,
    .log_file          = NULL,
    .syslog_ident      = "keel",
    .syslog_facility   = NULL,
    .log_level_str     = "info",
    .query_log_mode_str = "none",
    .log_timestamps    = true,
    .log_source        = true,
    .log_dest          = true,
    .log_username      = true,
    .log_database      = true,
    .log_query_tree    = false,
    .max_query_len     = 0,
    .use_colors        = true,
    .json_format       = false,
};

keel_admin_config_t  g_admin_cfg = KEEL_ADMIN_CONFIG_DEFAULT;
keel_admin_t*        g_admin     = NULL;

keel_cluster_config_t g_cluster_cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
keel_cluster_t*       g_cluster     = NULL;

keel_trace_config_t g_trace_cfg = KEEL_TRACE_CONFIG_DEFAULT;
keel_tracer_t*      g_tracer    = NULL;

keel_audit_log_t    g_audit_log;
bool                g_audit_log_initialized = false;

keel_log_plugin_t*  g_log_plugin = NULL;
keel_query_log_t    g_query_log  = {0};

keel_engine_t*      g_engine = NULL;

#ifdef KEEL_HAS_OTLP
keel_otlp_exporter_config_t g_otlp_cfg;
bool                        g_otlp_enabled  = false;
keel_otlp_exporter_t*       g_otlp_exporter = NULL;
keel_otlp_aggregator_t*     g_otlp_agg      = NULL;
#endif

/* ============================================================================
 * Startup Failure Cleanup
 * ============================================================================ */

void cleanup_startup_failure(keel_config_t* config, bool tls_initialized) {
#ifdef KEEL_HAS_OTLP
    if (g_otlp_agg) {
        keel_otlp_aggregator_stop(g_otlp_agg);
        keel_otlp_aggregator_destroy(g_otlp_agg);
        g_otlp_agg = NULL;
    }
    if (g_otlp_exporter) {
        keel_admin_set_otlp_exporter(g_admin, NULL);
        keel_otlp_exporter_stop(g_otlp_exporter);
        keel_otlp_exporter_destroy(g_otlp_exporter);
        g_otlp_exporter = NULL;
    }
#endif
    keel_admin_stop(g_admin);
    g_admin = NULL;

    if (g_cluster) {
        keel_cluster_destroy(g_cluster);
        g_cluster = NULL;
    }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];

        if (wg->failover_mgr) {
            keel_failover_manager_destroy(wg->failover_mgr);
            wg->failover_mgr = NULL;
        }
        if (wg->probe_mgr) {
            keel_probe_manager_destroy(wg->probe_mgr);
            wg->probe_mgr = NULL;
        }
        if (wg->discovery) {
            keel_discovery_stop(wg->discovery);
            keel_discovery_destroy(wg->discovery);
            wg->discovery = NULL;
        }
        if (wg->throttle_rules) {
            keel_throttle_rules_replace(&wg->throttle_rules, NULL);
        }
        if (wg->router_mgr) {
            keel_router_mgr_destroy(wg->router_mgr);
            wg->router_mgr = NULL;
        }
        if (wg->engine) {
            keel_engine_destroy(wg->engine);
            wg->engine = NULL;
        }
        if (wg->listen_fd >= 0) {
            close(wg->listen_fd);
            wg->listen_fd = -1;
        }
        if (wg->hook_registry) {
            keel_hook_registry_destroy(wg->hook_registry);
            wg->hook_registry = NULL;
        }
    }
    g_engine = NULL;

    if (g_tracer) {
        keel_tracer_destroy(g_tracer);
        g_tracer = NULL;
    }
    if (g_audit_log_initialized) {
        keel_audit_log_close(&g_audit_log);
        g_audit_log_initialized = false;
    }

    keel_config_free(config);

    keel_lua_shutdown();
    keel_python_shutdown();

    keel_query_log_shutdown(&g_query_log);
    if (g_log_plugin) {
        g_log_plugin->flush(g_log_plugin);
        g_log_plugin->close(g_log_plugin);
        g_log_plugin->destroy(g_log_plugin);
        g_log_plugin = NULL;
    }

    if (tls_initialized)
        keel_tls_cleanup();

    keel_mem_shutdown();
}

/* ============================================================================
 * Cluster Callbacks
 * ============================================================================ */

/**
 * @brief Cluster server-topology-change callback (H2).
 */
static void cluster_server_notify_handler(void* user_data,
                                          uint8_t action,
                                          const char* host,
                                          uint16_t port,
                                          uint8_t role,
                                          uint32_t weight)
{
    (void)role; (void)weight;
    if (!user_data || !host) return;

    worker_group_t* groups = (worker_group_t*)user_data;

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &groups[gi];
        if (!wg->engine) continue;

        uint32_t nw = keel_engine_get_num_workers(wg->engine);
        if (nw == 0) continue;

        keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, 0);
        if (!w || !w->server_pool || !w->server_pools) continue;

        for (size_t si = 0; si < w->server_pool->count; si++) {
            const keel_backend_server_t* srv = &w->server_pool->servers[si];
            if (!srv->host) continue;
            if (strcmp(srv->host, host) != 0 || srv->port != port) continue;

            if (action == 1 /* remove */) {
                if (w->server_pools[si])
                    backend_pool_update_target(w->server_pools[si], NULL, 0);
            } else {
                if (w->server_pools[si])
                    backend_pool_update_target(w->server_pools[si], host, port);
            }
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "cluster notify: group=%zu action=%u server=%s:%u",
                gi, (unsigned)action, host, (unsigned)port);
            break;
        }
    }
}

/**
 * @brief Engine stats callback registered with the cluster manager.
 */
static void cluster_engine_stats_cb(void* user_data,
                                     uint32_t* out_clients,
                                     uint32_t* out_backends,
                                     uint32_t* out_servers) {
    keel_engine_t* eng = (keel_engine_t*)user_data;
    *out_clients = (uint32_t)keel_engine_get_active_connections(eng);

    uint64_t backends = 0;
    uint32_t nw = keel_engine_get_num_workers(eng);
    for (uint32_t wi = 0; wi < nw; wi++) {
        const keel_worker_t* w = keel_engine_get_worker(eng, wi);
        if (!w || !w->server_pools) continue;
        for (size_t si = 0; si < w->server_pool_count; si++) {
            backend_pool_t* pool = w->server_pools[si];
            if (!pool) continue;
            backend_pool_stats_t st;
            backend_pool_get_stats(pool, &st);
            backends += st.active_connections;
        }
    }
    *out_backends = (uint32_t)(backends > UINT32_MAX ? UINT32_MAX : backends);

    const keel_server_pool_t* sp = keel_engine_get_server_pool(eng);
    *out_servers = sp ? (uint32_t)sp->count : 0;
}

/* ============================================================================
 * Logging Initialisation
 * ============================================================================ */

void log_init(void) {
    /* Choose plugin: explicit .so path > named built-in */
    if (g_log_cfg.plugin_path && g_log_cfg.plugin_path[0]) {
        g_log_plugin = keel_log_plugin_load(g_log_cfg.plugin_path);
        if (!g_log_plugin) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG, "Failed to load log plugin from %s, "
                            "falling back to stdout", g_log_cfg.plugin_path);
        }
    }

    if (!g_log_plugin) {
        const char* pname = g_log_cfg.plugin_name ? g_log_cfg.plugin_name : "stdout";
        if (strcmp(pname, "file") == 0) {
            g_log_plugin = keel_log_plugin_file_create();
        } else if (strcmp(pname, "syslog") == 0) {
            g_log_plugin = keel_log_plugin_syslog_create();
        } else {
            g_log_plugin = keel_log_plugin_stdout_create();
        }
    }

    if (g_log_plugin) {
        keel_log_plugin_opt_t opts[8];
        size_t nopts = 0;

        if (g_log_cfg.log_file) {
            opts[nopts++] = (keel_log_plugin_opt_t){ "log_file", g_log_cfg.log_file };
        }
        if (g_log_cfg.syslog_facility) {
            opts[nopts++] = (keel_log_plugin_opt_t){ "syslog_facility", g_log_cfg.syslog_facility };
        }
        opts[nopts++] = (keel_log_plugin_opt_t){
            "use_colors", g_log_cfg.use_colors ? "true" : "false"
        };
        opts[nopts++] = (keel_log_plugin_opt_t){
            "json_format", g_log_cfg.json_format ? "true" : "false"
        };

        keel_log_plugin_config_t pcfg = {
            .opts      = opts,
            .nopts     = nopts,
            .file_path = g_log_cfg.log_file,
            .ident     = g_log_cfg.syslog_ident,
        };

        keel_error_t perr = g_log_plugin->open(g_log_plugin, &pcfg);
        if (perr != KEEL_OK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG, "Log plugin open() failed (%d), "
                            "falling back to stderr", perr);
            g_log_plugin->destroy(g_log_plugin);
            g_log_plugin = keel_log_plugin_stdout_create();
            if (g_log_plugin) {
                g_log_plugin->open(g_log_plugin, NULL);
            }
        }
    }

    /* Set up query logger */
    keel_query_log_config_t qlcfg = keel_query_log_config_default();
    qlcfg.mode           = keel_query_log_mode_from_string(g_log_cfg.query_log_mode_str);
    qlcfg.min_level      = keel_log_level_from_string(g_log_cfg.log_level_str);
    qlcfg.log_timestamps = g_log_cfg.log_timestamps;
    qlcfg.log_source     = g_log_cfg.log_source;
    qlcfg.log_dest       = g_log_cfg.log_dest;
    qlcfg.log_username   = g_log_cfg.log_username;
    qlcfg.log_database   = g_log_cfg.log_database;
    qlcfg.log_query_tree = g_log_cfg.log_query_tree;
    qlcfg.max_query_len  = g_log_cfg.max_query_len;

    keel_query_log_init(&g_query_log, &qlcfg, g_log_plugin);
    keel_query_log_set_global(&g_query_log);

    keel_log_set_level(keel_log_level_from_string(g_log_cfg.log_level_str));

    if (g_log_cfg.json_format)
        keel_log_set_json_format(true);

    printf("  Logging:\n");
    printf("    Plugin:     %s\n", g_log_plugin ? g_log_plugin->name : "(none)");
    printf("    Level:      %s\n", g_log_cfg.log_level_str);
    printf("    Query log:  %s\n", keel_query_log_mode_name(qlcfg.mode));
    if (g_log_cfg.log_query_tree)
        printf("    Query tree: enabled\n");
    if (g_log_cfg.log_file)
        printf("    Log file:   %s\n", g_log_cfg.log_file);
    if (g_log_cfg.plugin_path)
        printf("    Plugin .so: %s\n", g_log_cfg.plugin_path);
    printf("\n");
}

/* ============================================================================
 * Cluster Callback Wrapper
 * ============================================================================ */

void cluster_setup_callbacks(keel_cluster_t* cluster,
                              keel_engine_t*  engine,
                              worker_group_t* groups)
{
    keel_cluster_set_stats_cb(cluster, cluster_engine_stats_cb, engine);
    keel_cluster_set_server_notify_cb(cluster, cluster_server_notify_handler, groups);
}

/* ============================================================================
 * Configuration Loading
 * ============================================================================ */

keel_config_t* config_load(const char* path, bool* fatal)
{
    if (fatal) *fatal = false;

    keel_config_t* config = NULL;

    if (path) {
        printf("Configuration file: %s\n", path);
        g_config.config_file = path;

        config = keel_config_load_auto(path);
        if (!config) {
            fprintf(stderr, "Warning: Failed to load config file: %s\n", path);
            /* Non-fatal — fall through to env-override pass. */
            goto apply_env_overrides;
        }

        /* Schema version gate */
        int64_t cfg_ver = keel_config_get_int(config, "keel", "config_version", 0);
        if (cfg_ver != KEEL_CONFIG_SCHEMA_VERSION) {
            fprintf(stderr,
                    "keel: configuration schema mismatch in '%s'\n"
                    "  found:    config_version = %lld\n"
                    "  required: config_version = %d (in [keel] section)\n"
                    "\n"
                    "v2 renames unit-suffixed INI keys (idle_timeout_ms ->\n"
                    "idle_timeout, session_max_buffered_bytes ->\n"
                    "session_max_buffered, etc.) and moves the unit into the\n"
                    "value (e.g. idle_timeout = 5m, session_max_buffered = 64KiB).\n"
                    "\n"
                    "To migrate an existing v1 config in place:\n"
                    "  keel --migrate-config %s -o %s.v2\n",
                    path,
                    (long long)cfg_ver,
                    KEEL_CONFIG_SCHEMA_VERSION,
                    path, path);
            keel_config_free(config);
            if (fatal) *fatal = true;
            return NULL;
        }

        /* Compute config file's directory for relative hook script= paths. */
        char config_dir[PATH_MAX] = {0};
        {
            char abs_path[PATH_MAX];
            if (realpath(path, abs_path)) {
                strncpy(config_dir, abs_path, sizeof(config_dir) - 1);
                char* last_slash = strrchr(config_dir, '/');
                if (last_slash) *last_slash = '\0';
            }
        }

        /* ----------------------------------------------------------------
         * [keel] section — global settings
         * ---------------------------------------------------------------- */
        g_config.log_level = (int)keel_config_get_int(config, "keel", "log_level", g_config.log_level);
        g_config.experimental_features = config_bool_enabled(
            config, "keel", "experimental_features", false);
        g_experimental_features_enabled = g_config.experimental_features;

        /* Shared-buffers pool (Phase 2 memory init) */
        {
            int64_t sb = keel_config_get_bytes(config, "keel", "shared_buffers", 0);
            if (sb > 0) {
                bool do_mlock = config_bool_enabled(config, "keel", "mlock_pool", false);
                bool do_huge  = config_bool_enabled(config, "keel", "use_huge_pages", false);
                keel_error_t perr = keel_mem_pool_attach((size_t)sb, do_mlock, do_huge);
                if (perr != KEEL_OK) {
                    fprintf(stderr,
                        "keel: shared_buffers=%lld: pool attach failed (%d). "
                        "Ensure the system has enough virtual address space "
                        "and, if mlock_pool=1, sufficient RLIMIT_MEMLOCK.\n",
                        (long long)sb, (int)perr);
                    keel_config_free(config);
                    if (fatal) *fatal = true;
                    return NULL;
                }
            }
        }

        /* Graceful shutdown drain timeout */
        {
            int64_t dt = keel_config_get_duration_ms(config, "keel", "shutdown_timeout", 30000);
            if (dt >= 0) g_config.shutdown_timeout_ms = (uint32_t)dt;
        }

        /* Shard-key hash mode */
        {
            const char* env = getenv("KEEL_SHARD_KEY_HASH");
            const char* val = env && env[0]
                              ? env
                              : keel_config_get_string(config, "keel", "shard_key_hash", "legacy");
            keel_shard_hash_mode_t mode = KEEL_SHARD_HASH_LEGACY;
            if (keel_shard_parse_hash_mode(val, &mode) == KEEL_OK) {
                keel_shard_set_hash_mode(mode);
                KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG,
                              "shard_key_hash mode = %s (%s)", val,
                              env ? "from env KEEL_SHARD_KEY_HASH"
                                  : "from [keel] section");
            } else {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                              "unknown shard_key_hash value '%s' "
                              "(expected: legacy|abs); keeping legacy", val);
            }
        }

        g_query_rule_count    = keel_config_count_sections_prefix(config, "query_rule.");
        g_throttle_rule_count = keel_config_count_sections_prefix(config, "throttle.");
        g_shard_rule_count    = keel_config_count_sections_prefix(config, "shard_rule.");

        /* Discover all worker_group.* sections */
        {
            wg_collect_ctx_t cctx = { &g_num_groups, KEEL_MAX_WORKER_GROUPS };
            keel_config_iter_sections_prefix(config, "worker_group.",
                                            collect_worker_groups, &cctx);
        }

        /* ----------------------------------------------------------------
         * Parse each worker group
         * ---------------------------------------------------------------- */
        for (size_t gi = 0; gi < g_num_groups; gi++) {
            worker_group_t* wg = &g_groups[gi];
            worker_group_defaults(wg);
            const char* section = wg->section;

            wg->name = keel_config_get_string(config, section, "name", section + 13);

            const char* addr = keel_config_get_string(config, section, "bind_addr", NULL);
            if (addr) wg->listen_addr = addr;

            int64_t port = keel_config_get_int(config, section, "bind_port", 0);
            if (port > 0 && port <= 65535) wg->listen_port = (uint16_t)port;

            int64_t workers = keel_config_get_int(config, section, "num_workers", 0);
            if (workers > 0) wg->num_workers = (uint32_t)workers;

            const char* protocol = keel_config_get_string(config, section, "protocol", NULL);
            if (protocol) wg->default_protocol = protocol;

            const char* database = keel_config_get_string(config, section, "default_database", NULL);
            if (database) wg->backend_database = database;

            const char* server_user = keel_config_get_string(config, section, "server_user", NULL);
            if (server_user) wg->backend_user = server_user;

            const char* server_password = keel_config_get_string(config, section, "server_password", NULL);
            if (server_password) wg->backend_password = server_password;

            int64_t min_ps = keel_config_get_int(config, section, "min_pool_size", (int64_t)wg->pool_min_size);
            if (min_ps > 0) wg->pool_min_size = (size_t)min_ps;

            int64_t max_ps = keel_config_get_int(config, section, "max_pool_size", (int64_t)wg->pool_max_size);
            if (max_ps > 0) wg->pool_max_size = (size_t)max_ps;

            int64_t mcpw = keel_config_get_int(config, section, "max_conns_per_worker", (int64_t)wg->session_pool_size);
            if (mcpw > 0) wg->session_pool_size = (size_t)mcpw;

            int64_t mc = keel_config_get_int(config, section, "max_clients", (int64_t)wg->max_clients);
            if (mc >= 0) wg->max_clients = (uint32_t)mc;

            /* Optional operational tuning */
            {
                int64_t v;
                v = keel_config_get_duration_ms(config, section, "idle_timeout",
                                        (int64_t)wg->idle_timeout_ms);
                if (v > 0) wg->idle_timeout_ms = (uint64_t)v;
                {
                    const char* ts = keel_config_get_string(config, section, "client_idle_timeout", NULL);
                    if (ts) {
                        int tv = atoi(ts);
                        if (tv > 0) {
                            if      (strchr(ts, 'm')) tv *= 60000;
                            else if (strchr(ts, 's')) tv *= 1000;
                            wg->idle_timeout_ms = (uint64_t)tv;
                        }
                    }
                }
                v = keel_config_get_duration_ms(config, section, "connect_timeout",
                                        (int64_t)wg->connect_timeout_ms);
                if (v > 0) wg->connect_timeout_ms = (uint64_t)v;
                {
                    const char* ts = keel_config_get_string(config, section, "client_connect_timeout", NULL);
                    if (ts) {
                        int tv = atoi(ts);
                        if (tv > 0) {
                            if      (strchr(ts, 'm')) tv *= 60000;
                            else if (strchr(ts, 's')) tv *= 1000;
                            wg->connect_timeout_ms = (uint64_t)tv;
                        }
                    }
                }
                v = keel_config_get_duration_ms(config, section, "pool_prune_interval",
                                        (int64_t)wg->pool_prune_interval_ms);
                if (v > 0) wg->pool_prune_interval_ms = (uint32_t)v;

                v = keel_config_get_duration_ms(config, section, "pool_refill_interval",
                                        (int64_t)wg->pool_refill_interval_ms);
                if (v >= 100) wg->pool_refill_interval_ms = (uint32_t)v;

                v = keel_config_get_duration_ms(config, section, "pool_refill_backoff",
                                        (int64_t)wg->pool_refill_backoff_ms);
                if (v > 0) wg->pool_refill_backoff_ms = (uint32_t)v;

                v = keel_config_get_int(config, section, "pool_max_waiting", 0);
                if (v >= 0) wg->pool_max_waiting = (uint32_t)v;

                v = keel_config_get_duration_ms(config, section, "pool_wait_timeout", 0);
                if (v >= 0) wg->pool_wait_timeout_ms = (uint64_t)v;

                v = keel_config_get_bytes(config, section, "session_max_buffered", 0);
                if (v == 0 || v >= 4096)
                    wg->session_max_buffered_bytes = (size_t)v;
                else if (v > 0)
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                        "[%s] session_max_buffered=%lld below minimum 4096 — ignored",
                        wg->name, (long long)v);

                v = keel_config_get_bytes(config, section, "backend_max_replay", 0);
                if (v == 0 || v >= 4096)
                    wg->backend_max_replay_bytes = (size_t)v;
                else if (v > 0)
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                        "[%s] backend_max_replay=%lld below minimum 4096 — ignored",
                        wg->name, (long long)v);

                v = keel_config_get_int(config, section, "listen_backlog",
                                        (int64_t)wg->listen_backlog);
                if (v > 0) wg->listen_backlog = (uint32_t)v;

                int64_t ubr = keel_config_get_int(config, section, "use_buf_rings", 0);
                wg->use_buf_rings = (ubr != 0);
                v = keel_config_get_int(config, section, "buf_ring_size", 0);
                if (v >= 0) wg->buf_ring_size = (uint32_t)v;

                int64_t sqp = keel_config_get_int(config, section, "sqpoll", 0);
                wg->sqpoll = (sqp != 0);
                v = keel_config_get_duration_ms(config, section, "sqpoll_idle", 1000);
                if (v > 0) wg->sqpoll_idle_ms = (uint32_t)v;

                /* Prepared-statement pooling mode */
                {
                    const char* ps_str = keel_config_get_string(
                        config, section, "prepared_statement", "virtualize");
                    if      (ps_str && strcmp(ps_str, "pinning")   == 0)
                        wg->ps_mode = KEEL_PS_MODE_PINNING;
                    else if (ps_str && strcmp(ps_str, "tracking")  == 0)
                        wg->ps_mode = KEEL_PS_MODE_TRACKING;
                    else if (ps_str && strcmp(ps_str, "anonymous") == 0)
                        wg->ps_mode = KEEL_PS_MODE_ANONYMOUS;
                    else if (ps_str && strcmp(ps_str, "off")       == 0)
                        wg->ps_mode = KEEL_PS_MODE_OFF;
                    else
                        wg->ps_mode = KEEL_PS_MODE_VIRTUALIZE;
                }

                /* Runtime mode tier */
                {
                    const char* mode_str = keel_config_get_string(
                        config, section, "mode", "pool");
                    wg->runtime_mode = keel_tier_parse(mode_str);
                }

                wg->experimental_features = config_bool_enabled(
                    config, section, "experimental_features",
                    g_config.experimental_features);

                {
                    const char* tt = keel_config_get_string(
                        config, section, "transaction_tracking", "off");
                    wg->txn_tracking = (tt && strcmp(tt, "on") == 0);
                }

                wg->fast_network_path = keel_config_get_bool(
                    config, section, "fast_network_path", true);
                wg->result_cache = keel_config_get_bool(
                    config, section, "result_cache", false);
                wg->scatter_merge_enabled = config_bool_enabled(
                    config, section, "scatter_merge", false);
                wg->wal_lsn_capture = config_bool_enabled(
                    config, section, "wal_lsn_capture", false);
                wg->gtid_capture = config_bool_enabled(
                    config, section, "gtid_capture", false);

                {
                    int64_t spt = keel_config_get_duration_ms(config, section, "sticky_primary_ttl", -1);
                    if (spt >= 0)
                        wg->sticky_primary_ttl_ms = (uint32_t)spt;
                }

                /* Failover-manager policy */
                {
                    const char* p = keel_config_get_string(
                        config, section, "failover_provider", NULL);
                    if (p && *p) {
                        if      (strcmp(p, "patroni") == 0) wg->failover_cfg.provider = KEEL_FAILOVER_PROVIDER_PATRONI;
                        else if (strcmp(p, "static")  == 0) wg->failover_cfg.provider = KEEL_FAILOVER_PROVIDER_STATIC;
                        else if (strcmp(p, "consul")  == 0) wg->failover_cfg.provider = KEEL_FAILOVER_PROVIDER_CONSUL;
                        else if (strcmp(p, "etcd")    == 0) wg->failover_cfg.provider = KEEL_FAILOVER_PROVIDER_ETCD;
                        else if (strcmp(p, "custom")  == 0) wg->failover_cfg.provider = KEEL_FAILOVER_PROVIDER_CUSTOM;
                        else if (strcmp(p, "none")    == 0) wg->failover_cfg.provider = KEEL_FAILOVER_PROVIDER_NONE;
                        else KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "[%s] unknown failover_provider='%s' (keeping default)",
                            wg->name, p);
                    }
                    int64_t fv;
                    fv = keel_config_get_duration_ms(config, section,
                            "failover_detection_interval",
                            (int64_t)wg->failover_cfg.detection_interval_ms);
                    if (fv > 0) wg->failover_cfg.detection_interval_ms = (uint32_t)fv;

                    fv = keel_config_get_int(config, section,
                            "failover_failure_threshold",
                            (int64_t)wg->failover_cfg.failure_threshold);
                    if (fv > 0) wg->failover_cfg.failure_threshold = (uint32_t)fv;

                    fv = keel_config_get_duration_ms(config, section,
                            "failover_promotion_grace",
                            (int64_t)wg->failover_cfg.promotion_grace_ms);
                    if (fv >= 0) wg->failover_cfg.promotion_grace_ms = (uint32_t)fv;

                    wg->failover_cfg.old_primary_fencing_required =
                        keel_config_get_bool(config, section,
                            "failover_old_primary_fencing_required",
                            wg->failover_cfg.old_primary_fencing_required);

                    wg->failover_cfg.allow_ambiguous_write_retry =
                        keel_config_get_bool(config, section,
                            "failover_allow_ambiguous_write_retry",
                            wg->failover_cfg.allow_ambiguous_write_retry);

                    const char* rdf = keel_config_get_string(
                        config, section, "failover_read_during_failover", NULL);
                    if (rdf && *rdf) {
                        if      (strcmp(rdf, "primary_only")             == 0) wg->failover_cfg.read_during_failover = KEEL_FAILOVER_READ_PRIMARY_ONLY;
                        else if (strcmp(rdf, "reject")                   == 0) wg->failover_cfg.read_during_failover = KEEL_FAILOVER_READ_REJECT;
                        else if (strcmp(rdf, "allow_caught_up_replicas") == 0) wg->failover_cfg.read_during_failover = KEEL_FAILOVER_READ_ALLOW_CAUGHT_UP_REPLICAS;
                        else KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "[%s] unknown failover_read_during_failover='%s' (keeping default)",
                            wg->name, rdf);
                    }

                    const char* tdf = keel_config_get_string(
                        config, section, "failover_transaction_during_failover", NULL);
                    if (tdf && *tdf) {
                        if      (strcmp(tdf, "fail") == 0) wg->failover_cfg.transaction_during_failover = KEEL_FAILOVER_TXN_FAIL;
                        else if (strcmp(tdf, "wait") == 0) wg->failover_cfg.transaction_during_failover = KEEL_FAILOVER_TXN_WAIT;
                        else KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "[%s] unknown failover_transaction_during_failover='%s' (keeping default)",
                            wg->name, tdf);
                    }

                    if (wg->failover_cfg.allow_ambiguous_write_retry) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "[%s] failover_allow_ambiguous_write_retry=on — "
                            "writes may be retried after an ambiguous COMMIT; "
                            "this can DUPLICATE rows on failover. "
                            "See proposals/keel-v.05-alpha-consistent_read-failover-pstmt.md",
                            wg->name);
                    }
                    if (!wg->failover_cfg.old_primary_fencing_required) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "[%s] failover_old_primary_fencing_required=off — "
                            "old primary may still accept writes after promotion; "
                            "split-brain risk", wg->name);
                    }
                }

                /* Connection rebalancing */
                {
                    const char* rb = keel_config_get_string(
                        config, section, "rebalance", "on");
                    wg->rebalance_enabled = (!rb || strcmp(rb, "off") != 0);

                    int64_t rv;
                    rv = keel_config_get_duration_ms(config, section, "rebalance_interval",
                                            (int64_t)wg->rebalance_interval_ms);
                    if (rv > 0) wg->rebalance_interval_ms = (uint32_t)rv;

                    rv = keel_config_get_int(config, section, "rebalance_threshold_pct",
                                            (int64_t)wg->rebalance_threshold_pct);
                    if (rv > 100) wg->rebalance_threshold_pct = (uint32_t)rv;

                    rv = keel_config_get_int(config, section, "rebalance_max_per_tick",
                                            (int64_t)wg->rebalance_max_per_tick);
                    if (rv > 0) wg->rebalance_max_per_tick = (uint32_t)rv;
                }

                /* Scatter-merge */
                {
                    int64_t smm = keel_config_get_bytes(config, section, "scatter_merge_max_mem", 0);
                    if (smm > 0)
                        wg->scatter_merge_max_mem_bytes = (size_t)smm;
                    if (smm > 0)
                        wg->scatter_merge_enabled = true;

                    const char* ssd = keel_config_get_string(config, section,
                                                              "scatter_merge_spill_dir", NULL);
                    if (ssd && ssd[0] != '\0')
                        snprintf(wg->scatter_merge_spill_dir_buf,
                                 sizeof wg->scatter_merge_spill_dir_buf, "%s", ssd);
                    if (ssd && ssd[0] != '\0')
                        wg->scatter_merge_enabled = true;
                }

                /* Frontend TLS */
                {
                    const char* tls_mode_str = keel_config_get_string(
                        config, section, "tls_mode", "disable");
                    if (tls_mode_str) {
                        snprintf(wg->tls_mode_buf, sizeof(wg->tls_mode_buf), "%s", tls_mode_str);
                        if      (strcmp(tls_mode_str, "prefer") == 0)  wg->tls_config.mode = KEEL_TLS_PREFER;
                        else if (strcmp(tls_mode_str, "require") == 0) wg->tls_config.mode = KEEL_TLS_REQUIRE;
                        else                                            wg->tls_config.mode = KEEL_TLS_DISABLE;
                    }
                    const char* cert_file = keel_config_get_string(config, section, "tls_cert_file", NULL);
                    if (cert_file) {
                        snprintf(wg->tls_cert_file_buf, sizeof(wg->tls_cert_file_buf), "%s", cert_file);
                        wg->tls_config.cert_file = wg->tls_cert_file_buf;
                    }
                    const char* key_file = keel_config_get_string(config, section, "tls_key_file", NULL);
                    if (key_file) {
                        snprintf(wg->tls_key_file_buf, sizeof(wg->tls_key_file_buf), "%s", key_file);
                        wg->tls_config.key_file = wg->tls_key_file_buf;
                    }
                    const char* ca_file = keel_config_get_string(config, section, "tls_ca_file", NULL);
                    if (ca_file) {
                        snprintf(wg->tls_ca_file_buf, sizeof(wg->tls_ca_file_buf), "%s", ca_file);
                        wg->tls_config.ca_file = wg->tls_ca_file_buf;
                    }
                    const char* verify_str = keel_config_get_string(
                        config, section, "tls_verify_peer", "require");
                    if (verify_str) {
                        snprintf(wg->tls_verify_buf, sizeof(wg->tls_verify_buf), "%s", verify_str);
                        if      (strcmp(verify_str, "optional") == 0) wg->tls_config.verify_peer = KEEL_TLS_VERIFY_OPTIONAL;
                        else if (strcmp(verify_str, "require") == 0 || strcmp(verify_str, "required") == 0)
                            wg->tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
                        else {
                            wg->tls_config.verify_peer = KEEL_TLS_VERIFY_NONE;
                            if (wg->tls_config.mode != KEEL_TLS_DISABLE) {
                                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                    "SECURITY: [%s] tls_verify_peer=none disables client certificate "
                                    "verification — connections are vulnerable to MITM attacks. "
                                    "Set tls_verify_peer=require for production deployments.",
                                    section);
                            }
                        }
                    }
                    int64_t ktls = keel_config_get_int(config, section, "ktls_enabled", 1);
                    wg->tls_config.ktls_enabled = (ktls != 0);
                    const char* tls_ciphers = keel_config_get_string(config, section, "tls_ciphers", NULL);
                    if (tls_ciphers) wg->tls_config.ciphers = (char*)tls_ciphers;
                    const char* tls_ciphersuites = keel_config_get_string(config, section, "tls_ciphersuites", NULL);
                    if (tls_ciphersuites) wg->tls_config.ciphersuites = (char*)tls_ciphersuites;
                    const char* tls_min_ver = keel_config_get_string(config, section, "tls_min_version", NULL);
                    if (tls_min_ver) {
                        if (strcmp(tls_min_ver, "1.3") == 0 || strcmp(tls_min_ver, "TLSv1.3") == 0)
                            wg->tls_config.min_version = KEEL_TLS_VERSION_1_3;
                        else if (strcmp(tls_min_ver, "1.2") == 0 || strcmp(tls_min_ver, "TLSv1.2") == 0)
                            wg->tls_config.min_version = KEEL_TLS_VERSION_1_2;
                    }
                    int64_t hs_timeout = keel_config_get_duration_ms(config, section, "tls_handshake_timeout", 10000);
                    if (hs_timeout > 0) wg->tls_config.handshake_timeout_ms = (size_t)hs_timeout;
                    int64_t read_timeout = keel_config_get_duration_ms(config, section, "tls_read_timeout", 30000);
                    if (read_timeout > 0) wg->tls_config.read_timeout_ms = (size_t)read_timeout;
                }

                /* Backend TLS */
                {
                    const char* backend_tls_mode = keel_config_get_string(
                        config, section, "backend_tls_mode", "disable");
                    if (backend_tls_mode) {
                        snprintf(wg->backend_tls_mode_buf, sizeof(wg->backend_tls_mode_buf), "%s", backend_tls_mode);
                        if      (strcmp(backend_tls_mode, "prefer") == 0)  wg->backend_tls_config.mode = KEEL_TLS_PREFER;
                        else if (strcmp(backend_tls_mode, "require") == 0) wg->backend_tls_config.mode = KEEL_TLS_REQUIRE;
                        else                                               wg->backend_tls_config.mode = KEEL_TLS_DISABLE;
                    }
                    const char* backend_cert = keel_config_get_string(config, section, "backend_tls_cert_file", NULL);
                    if (backend_cert) {
                        snprintf(wg->backend_tls_cert_file_buf, sizeof(wg->backend_tls_cert_file_buf), "%s", backend_cert);
                        wg->backend_tls_config.cert_file = wg->backend_tls_cert_file_buf;
                    }
                    const char* backend_key = keel_config_get_string(config, section, "backend_tls_key_file", NULL);
                    if (backend_key) {
                        snprintf(wg->backend_tls_key_file_buf, sizeof(wg->backend_tls_key_file_buf), "%s", backend_key);
                        wg->backend_tls_config.key_file = wg->backend_tls_key_file_buf;
                    }
                    const char* backend_ca = keel_config_get_string(config, section, "backend_tls_ca_file", NULL);
                    if (backend_ca) {
                        snprintf(wg->backend_tls_ca_file_buf, sizeof(wg->backend_tls_ca_file_buf), "%s", backend_ca);
                        wg->backend_tls_config.ca_file = wg->backend_tls_ca_file_buf;
                    }
                    const char* backend_verify = keel_config_get_string(
                        config, section, "backend_tls_verify_peer", "require");
                    if (backend_verify) {
                        snprintf(wg->backend_tls_verify_buf, sizeof(wg->backend_tls_verify_buf), "%s", backend_verify);
                        if      (strcmp(backend_verify, "optional") == 0) wg->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_OPTIONAL;
                        else if (strcmp(backend_verify, "require") == 0 || strcmp(backend_verify, "required") == 0)
                            wg->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
                        else {
                            wg->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_NONE;
                            if (wg->backend_tls_config.mode != KEEL_TLS_DISABLE) {
                                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                    "SECURITY: [%s] backend_tls_verify_peer=none disables backend "
                                    "certificate verification — connections to PostgreSQL backends are "
                                    "vulnerable to MITM attacks. "
                                    "Set backend_tls_verify_peer=require for production deployments.",
                                    section);
                            }
                        }
                    }
                    int64_t backend_ktls = keel_config_get_int(config, section, "backend_ktls_enabled", 1);
                    wg->backend_tls_config.ktls_enabled = (backend_ktls != 0);
                    const char* be_ciphers = keel_config_get_string(config, section, "backend_tls_ciphers", NULL);
                    if (be_ciphers) wg->backend_tls_config.ciphers = (char*)be_ciphers;
                    const char* be_ciphersuites = keel_config_get_string(config, section, "backend_tls_ciphersuites", NULL);
                    if (be_ciphersuites) wg->backend_tls_config.ciphersuites = (char*)be_ciphersuites;
                    const char* be_min_ver = keel_config_get_string(config, section, "backend_tls_min_version", NULL);
                    if (be_min_ver) {
                        if (strcmp(be_min_ver, "1.3") == 0 || strcmp(be_min_ver, "TLSv1.3") == 0)
                            wg->backend_tls_config.min_version = KEEL_TLS_VERSION_1_3;
                        else if (strcmp(be_min_ver, "1.2") == 0 || strcmp(be_min_ver, "TLSv1.2") == 0)
                            wg->backend_tls_config.min_version = KEEL_TLS_VERSION_1_2;
                    }
                    int64_t backend_hs_timeout = keel_config_get_duration_ms(config, section, "backend_tls_handshake_timeout", 10000);
                    if (backend_hs_timeout > 0) wg->backend_tls_config.handshake_timeout_ms = (size_t)backend_hs_timeout;
                    int64_t backend_read_timeout = keel_config_get_duration_ms(config, section, "backend_tls_read_timeout", 30000);
                    if (backend_read_timeout > 0) wg->backend_tls_config.read_timeout_ms = (size_t)backend_read_timeout;
                }
            } /* end operational tuning block */

            /* Auto-generate TLS certificates */
            {
                int64_t auto_gen = keel_config_get_int(config, section, "tls_auto_generate", 0);
                wg->tls_auto_generate = (auto_gen != 0);
                const char* auto_dir = keel_config_get_string(
                    config, section, "tls_auto_dir", "/var/lib/keel/certs");
                if (auto_dir)
                    snprintf(wg->tls_auto_dir_buf, sizeof(wg->tls_auto_dir_buf), "%s", auto_dir);
            }

            /* Parse backend servers */
            if (keel_config_has_section(config, wg->servers_section)) {
                const char* srv_keys[KEEL_MAX_SERVERS];
                const char* srv_vals[KEEL_MAX_SERVERS];
                srv_collect_ctx_t srv_ctx = { srv_keys, srv_vals, 0, KEEL_MAX_SERVERS };
                keel_config_iter_keys(config, wg->servers_section, collect_srv_keys, &srv_ctx);

                for (size_t i = 0; i < srv_ctx.count && wg->server_pool.count < KEEL_MAX_SERVERS; i++) {
                    const char* server_def = srv_ctx.vals[i];
                    if (!server_def) continue;

                    keel_backend_server_t* server = &wg->server_pool.servers[wg->server_pool.count];
                    char* host_buf  = wg->server_bufs[wg->server_pool.count][0];
                    char* user_buf  = wg->server_bufs[wg->server_pool.count][1];
                    char* pass_buf  = wg->server_bufs[wg->server_pool.count][2];
                    char* db_buf    = wg->server_bufs[wg->server_pool.count][3];
                    char* puser_buf = wg->server_bufs[wg->server_pool.count][4];
                    char* ppass_buf = wg->server_bufs[wg->server_pool.count][5];
                    char* pauth_buf = wg->server_bufs[wg->server_pool.count][6];

                    server->host          = "127.0.0.1";
                    server->port          = 5432;
                    server->user          = "postgres";
                    server->password      = NULL;
                    server->database      = "postgres";
                    server->probe_user    = wg->probe_cfg.probe_user;
                    server->probe_password = wg->probe_cfg.probe_password;
                    server->probe_auth    = wg->probe_cfg.probe_auth;
                    server->role          = KEEL_SERVER_ROLE_AUTO;
                    server->weight        = 100;
                    server->healthy       = true;

                    const char* p = server_def;
                    while (*p) {
                        while (*p && isspace((unsigned char)*p)) p++;
                        if (!*p) break;

                        if (strncmp(p, "host=", 5) == 0) {
                            p += 5;
                            char* end = host_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->host = host_buf;
                        } else if (strncmp(p, "port=", 5) == 0) {
                            p += 5; server->port = (uint16_t)atoi(p);
                            while (*p && !isspace((unsigned char)*p)) p++;
                        } else if (strncmp(p, "user=", 5) == 0) {
                            p += 5;
                            char* end = user_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->user = user_buf;
                        } else if (strncmp(p, "password=", 9) == 0) {
                            p += 9;
                            char* end = pass_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->password = pass_buf;
                        } else if (strncmp(p, "probe_user=", 11) == 0) {
                            p += 11;
                            char* end = puser_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->probe_user = puser_buf;
                        } else if (strncmp(p, "probe_password=", 15) == 0) {
                            p += 15;
                            char* end = ppass_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->probe_password = ppass_buf;
                        } else if (strncmp(p, "probe_auth=", 11) == 0) {
                            p += 11;
                            char* end = pauth_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->probe_auth = pauth_buf;
                        } else if (strncmp(p, "dbname=", 7) == 0) {
                            p += 7;
                            char* end = db_buf;
                            while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                            *end = '\0'; server->database = db_buf;
                        } else if (strncmp(p, "role=", 5) == 0) {
                            p += 5;
                            if      (strncmp(p, "RW", 2) == 0 || strncmp(p, "rw", 2) == 0 || strncmp(p, "primary", 7) == 0)
                                server->role = KEEL_SERVER_ROLE_RW;
                            else if (strncmp(p, "RO", 2) == 0 || strncmp(p, "ro", 2) == 0 || strncmp(p, "replica", 7) == 0)
                                server->role = KEEL_SERVER_ROLE_RO;
                            else if (strncmp(p, "WO", 2) == 0 || strncmp(p, "wo", 2) == 0)
                                server->role = KEEL_SERVER_ROLE_WO;
                            else if (strncmp(p, "auto", 4) == 0 || strncmp(p, "AUTO", 4) == 0)
                                server->role = KEEL_SERVER_ROLE_AUTO;
                            while (*p && !isspace((unsigned char)*p)) p++;
                        } else if (strncmp(p, "weight=", 7) == 0) {
                            p += 7; server->weight = (uint32_t)atoi(p);
                            while (*p && !isspace((unsigned char)*p)) p++;
                        } else if (strncmp(p, "shard_id=", 9) == 0) {
                            p += 9; server->shard_id = (uint32_t)atoi(p);
                            if (server->shard_id > 0) wg->sharding_enabled = true;
                            while (*p && !isspace((unsigned char)*p)) p++;
                        } else {
                            while (*p && !isspace((unsigned char)*p)) p++;
                        }
                    }

                    wg->server_pool.count++;

                    if (server->role == KEEL_SERVER_ROLE_RW && wg->backend_host == NULL) {
                        wg->backend_host     = server->host;
                        wg->backend_port     = server->port;
                        wg->backend_user     = server->user;
                        wg->backend_password = server->password;
                        wg->backend_database = server->database;
                    }
                }

                keel_server_pool_rebuild_indices(&wg->server_pool);

                printf("  [%s] Parsed %zu backend servers (RW=%zu, RO=%zu, WO=%zu)\n",
                       wg->name, wg->server_pool.count,
                       wg->server_pool.rw_count,
                       wg->server_pool.ro_count,
                       wg->server_pool.wo_count);
            }

            /* Parse probe config */
            {
                const char* probe_str = keel_config_get_string(config, section, "probe", NULL);
                if (probe_str) {
                    const char* colon = strchr(probe_str, ':');
                    if (colon) {
                        size_t tlen = (size_t)(colon - probe_str);
                        if (tlen >= sizeof(wg->probe_type_buf)) tlen = sizeof(wg->probe_type_buf) - 1;
                        memcpy(wg->probe_type_buf, probe_str, tlen);
                        wg->probe_type_buf[tlen] = '\0';
                        strncpy(wg->probe_extra_buf, colon + 1, sizeof(wg->probe_extra_buf) - 1);
                        wg->probe_extra_buf[sizeof(wg->probe_extra_buf) - 1] = '\0';
                        wg->probe_cfg.probe_type  = wg->probe_type_buf;
                        wg->probe_cfg.probe_extra = wg->probe_extra_buf;
                    } else {
                        strncpy(wg->probe_type_buf, probe_str, sizeof(wg->probe_type_buf) - 1);
                        wg->probe_type_buf[sizeof(wg->probe_type_buf) - 1] = '\0';
                        wg->probe_cfg.probe_type = wg->probe_type_buf;
                    }
                }

                const char* pv;
                pv = keel_config_get_string(config, section, "probe_interval", NULL);
                if (pv) { int val = atoi(pv); if (val > 0) { if (strchr(pv, 's')) val *= 1000; wg->probe_cfg.interval_ms = (uint32_t)val; } }

                pv = keel_config_get_string(config, section, "probe_timeout", NULL);
                if (pv) { int val = atoi(pv); if (val > 0) { if (strchr(pv, 's')) val *= 1000; wg->probe_cfg.timeout_ms = (uint32_t)val; } }

                int64_t retries = keel_config_get_int(config, section, "probe_retries",
                                                     (int64_t)wg->probe_cfg.retries);
                if (retries > 0) wg->probe_cfg.retries = (uint32_t)retries;

                pv = keel_config_get_string(config, section, "probe_user", NULL);
                if (pv && *pv) { strncpy(wg->probe_user_buf, pv, sizeof(wg->probe_user_buf) - 1); wg->probe_user_buf[sizeof(wg->probe_user_buf)-1]='\0'; wg->probe_cfg.probe_user = wg->probe_user_buf; }

                pv = keel_config_get_string(config, section, "probe_password", NULL);
                if (pv && *pv) { strncpy(wg->probe_password_buf, pv, sizeof(wg->probe_password_buf) - 1); wg->probe_password_buf[sizeof(wg->probe_password_buf)-1]='\0'; wg->probe_cfg.probe_password = wg->probe_password_buf; }

                pv = keel_config_get_string(config, section, "probe_auth", NULL);
                if (pv && *pv) { strncpy(wg->probe_auth_buf, pv, sizeof(wg->probe_auth_buf) - 1); wg->probe_auth_buf[sizeof(wg->probe_auth_buf)-1]='\0'; wg->probe_cfg.probe_auth = wg->probe_auth_buf; }

                pv = keel_config_get_string(config, section, "failover_delay", NULL);
                if (pv) { int val = atoi(pv); if (val > 0) { if (strchr(pv, 's')) val *= 1000; wg->probe_cfg.failover_delay_ms = (uint32_t)val; } }

                /* Apply group-level probe auth defaults to servers */
                for (size_t si = 0; si < wg->server_pool.count; si++) {
                    keel_backend_server_t* s = &wg->server_pool.servers[si];
                    if (!s->probe_user)     s->probe_user     = wg->probe_cfg.probe_user;
                    if (!s->probe_password) s->probe_password = wg->probe_cfg.probe_password;
                    if (!s->probe_auth)     s->probe_auth     = wg->probe_cfg.probe_auth;
                }
            }

            /* Parse enterprise authentication */
            {
                const char* v = keel_config_get_string(config, section, "auth_method", NULL);
                if (v && *v) {
                    strncpy(wg->auth_method_buf, v, sizeof(wg->auth_method_buf) - 1);
                    wg->auth_method_buf[sizeof(wg->auth_method_buf) - 1] = '\0';
                    if (strcasecmp(v, "scram-sha-256") == 0 || strcasecmp(v, "scram") == 0)
                        wg->auth_method = KEEL_AUTH_SCRAM_SHA_256;
                    else if (strcasecmp(v, "md5") == 0) {
                        wg->auth_method = KEEL_AUTH_MD5;
                        /* Strict-auth check is performed in main() after config_load() returns */
                        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                            "SECURITY: [%s] auth_method=md5 is deprecated (RFC 6151) and vulnerable "
                            "to offline brute-force attacks. Migrate to scram-sha-256. "
                            "Use --strict-auth to reject md5 at startup.",
                            wg->name);
                    }
                    else if (strcasecmp(v, "trust") == 0) {
                        wg->auth_method = KEEL_AUTH_TRUST;
                        /* Strict-auth check is performed in main() after config_load() returns */
                        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                            "SECURITY: [%s] auth_method=trust accepts all connections without "
                            "authentication. This is only safe on loopback/private networks. "
                            "Use --strict-auth to reject trust at startup.",
                            wg->name);
                    }
                    else if (strcasecmp(v, "password") == 0)
                        wg->auth_method = KEEL_AUTH_PASSWORD;
                    else if (strcasecmp(v, "cert") == 0 || strcasecmp(v, "certificate") == 0)
                        wg->auth_method = KEEL_AUTH_CERTIFICATE;
                    else if (strcasecmp(v, "ldap") == 0)
                        wg->auth_method = KEEL_AUTH_LDAP;
                    else if (strcasecmp(v, "pam") == 0)
                        wg->auth_method = KEEL_AUTH_PAM;
                    else if (strcasecmp(v, "auth_query") == 0 || strcasecmp(v, "authquery") == 0)
                        wg->auth_method = KEEL_AUTH_SCRAM_SHA_256;
                    else {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                            "[%s] Unknown auth_method '%s', defaulting to scram-sha-256",
                            wg->name, v);
                        wg->auth_method = KEEL_AUTH_SCRAM_SHA_256;
                    }
                    KEEL_LOG_INFO(KEEL_LOG_CAT_AUTH,
                        "[%s] auth_method = %s", wg->name, wg->auth_method_buf);
                }

                /* LDAP config */
#define COPY_BUF(field, key) do { \
    const char* _v = keel_config_get_string(config, section, (key), NULL); \
    if (_v && *_v) { strncpy(wg->field, _v, sizeof(wg->field)-1); wg->field[sizeof(wg->field)-1]='\0'; } \
} while(0)
                COPY_BUF(auth_ldap_url_buf,           "ldap_url");
                COPY_BUF(auth_ldap_base_dn_buf,       "ldap_base_dn");
                COPY_BUF(auth_ldap_bind_dn_buf,       "ldap_bind_dn");
                COPY_BUF(auth_ldap_bind_password_buf, "ldap_bind_password");
                COPY_BUF(auth_ldap_search_filter_buf, "ldap_search_filter");
                COPY_BUF(auth_ldap_dn_suffix_buf,     "ldap_dn_suffix");
                COPY_BUF(auth_pam_service_buf,        "pam_service_name");
                COPY_BUF(auth_query_buf,              "auth_query");
                COPY_BUF(auth_query_conn_buf,         "auth_query_connection");
                COPY_BUF(auth_userlist_file_buf,      "userlist_file");
#undef COPY_BUF
                {
                    int64_t ltls = keel_config_get_int(config, section, "ldap_start_tls", 0);
                    wg->auth_ldap_start_tls = (ltls != 0);
                }
                {
                    int64_t lto = keel_config_get_int(config, section, "ldap_timeout", 5);
                    wg->auth_ldap_timeout_s = (int)lto;
                }

                /* Connection lifecycle */
                {
                    const char* age_s = keel_config_get_string(config, section, "max_connection_age_s", NULL);
                    if (age_s && *age_s) {
                        double secs = atof(age_s);
                        if (secs > 0)
                            wg->pool_max_connection_age_ms = (uint64_t)(secs * 1000.0);
                    }
                    int64_t mcams = keel_config_get_duration_ms(config, section, "max_connection_age", 0);
                    if (mcams > 0)
                        wg->pool_max_connection_age_ms = (uint64_t)mcams;
                }
            }

            /* Parse [worker_group.X.hooks] section */
            {
                char hooks_section[320];
                snprintf(hooks_section, sizeof(hooks_section), "%.255s.hooks", section);

                if (keel_config_has_section(config, hooks_section)) {
                    wg->hooks_enabled = true;
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "[%s] Found hooks section [%s]", wg->name, hooks_section);

                    keel_hook_registry_t* reg = keel_hook_registry_create();
                    if (!reg) {
                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                            "[%s] Failed to create hook registry (out of memory)", wg->name);
                    } else {
                        bool need_lua    = false;
                        bool need_python = false;
                        bool python_inited = false;
                        size_t lua_count = 0, python_count = 0, plugin_count = 0;

                        const char* hk_keys[64];
                        const char* hk_vals[64];
                        srv_collect_ctx_t hk_ctx = { hk_keys, hk_vals, 0, 64 };
                        keel_config_iter_keys(config, hooks_section, collect_srv_keys, &hk_ctx);

                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] Parsing %zu hook entries from [%s]",
                            wg->name, hk_ctx.count, hooks_section);

                        for (size_t hi = 0; hi < hk_ctx.count; hi++) {
                            const char* key = hk_keys[hi];
                            const char* val = hk_vals[hi];
                            if (!key || !val) continue;

                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[%s]   Hook entry: %s = %s", wg->name, key, val);

                            if (strncmp(key, "hook.plugin.", 12) == 0) {
                                const char* plugin_name = key + 12;
                                if (access(val, R_OK) != 0) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                        "[%s] Hook plugin file not found or not readable: '%s' (for hook.plugin.%s)",
                                        wg->name, val, plugin_name);
                                    continue;
                                }
                                keel_error_t err = keel_hook_load_plugin(reg, val);
                                if (err != KEEL_OK) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                        "[%s] Failed to load hook plugin '%s' from '%s': error %d",
                                        wg->name, plugin_name, val, err);
                                } else {
                                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                        "[%s] Loaded native plugin '%s' from '%s'", wg->name, plugin_name, val);
                                    plugin_count++;
                                }
                            } else if (strncmp(key, "hook.lua.", 9) == 0) {
                                need_lua = true;
                                const char* rest = key + 9;
                                const char* dot = strchr(rest, '.');
                                if (!dot) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Invalid Lua hook key: '%s'", wg->name, key);
                                    continue;
                                }
                                char point_str[64];
                                size_t plen = (size_t)(dot - rest);
                                if (plen >= sizeof(point_str)) plen = sizeof(point_str) - 1;
                                memcpy(point_str, rest, plen); point_str[plen] = '\0';
                                const char* hook_name = dot + 1;

                                keel_hook_point_t point = KEEL_HOOK_POINT_COUNT;
                                if      (strcmp(point_str, "after_query_read")  == 0) point = KEEL_HOOK_AFTER_QUERY_READ;
                                else if (strcmp(point_str, "after_query_parse") == 0) point = KEEL_HOOK_AFTER_QUERY_PARSE;
                                else if (strcmp(point_str, "before_route")      == 0) point = KEEL_HOOK_BEFORE_ROUTE;
                                else if (strcmp(point_str, "before_send")       == 0) point = KEEL_HOOK_BEFORE_SEND;

                                if (point == KEEL_HOOK_POINT_COUNT) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Unknown hook point '%s' in key '%s'",
                                        wg->name, point_str, key);
                                    continue;
                                }

                                char lua_script[256] = {0};
                                char lua_func[128]   = {0};
                                int priority = 100;
                                const char* pp = val;
                                while (*pp) {
                                    while (*pp && isspace((unsigned char)*pp)) pp++;
                                    if (!*pp) break;
                                    if      (strncmp(pp, "script=",   7) == 0) { pp += 7; char* e = lua_script; while (*pp && !isspace((unsigned char)*pp)) *e++ = *pp++; *e = '\0'; }
                                    else if (strncmp(pp, "func=",     5) == 0) { pp += 5; char* e = lua_func;   while (*pp && !isspace((unsigned char)*pp)) *e++ = *pp++; *e = '\0'; }
                                    else if (strncmp(pp, "priority=", 9) == 0) { pp += 9; priority = atoi(pp); while (*pp && !isspace((unsigned char)*pp)) pp++; }
                                    else { while (*pp && !isspace((unsigned char)*pp)) pp++; }
                                }

                                if (!lua_script[0] || !lua_func[0]) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Lua hook '%s': missing script= or func= in '%s'", wg->name, key, val);
                                    continue;
                                }
                                if (access(lua_script, R_OK) != 0) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Lua script not found: '%s'", wg->name, lua_script);
                                    continue;
                                }
                                if (!keel_lua_available()) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Lua hook '%s' requires -DKEEL_ENABLE_LUA=ON", wg->name, hook_name);
                                    continue;
                                }

                                keel_hook_handle_t* h = keel_hook_register_lua(reg, point, hook_name, lua_script, lua_func, priority);
                                if (h) {
                                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                        "[%s] Registered Lua hook '%s' at %s (script=%s func=%s priority=%d)",
                                        wg->name, hook_name, keel_hook_point_name(point), lua_script, lua_func, priority);
                                    lua_count++;
                                } else {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Failed to register Lua hook '%s'", wg->name, hook_name);
                                }
                            } else if (strncmp(key, "hook.python.", 12) == 0) {
                                need_python = true;
                                const char* rest = key + 12;
                                const char* dot = strchr(rest, '.');
                                if (!dot) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Invalid Python hook key: '%s'", wg->name, key);
                                    continue;
                                }
                                char point_str[64];
                                size_t plen = (size_t)(dot - rest);
                                if (plen >= sizeof(point_str)) plen = sizeof(point_str) - 1;
                                memcpy(point_str, rest, plen); point_str[plen] = '\0';
                                const char* hook_name = dot + 1;

                                keel_hook_point_t point = KEEL_HOOK_POINT_COUNT;
                                if      (strcmp(point_str, "after_query_read")  == 0) point = KEEL_HOOK_AFTER_QUERY_READ;
                                else if (strcmp(point_str, "after_query_parse") == 0) point = KEEL_HOOK_AFTER_QUERY_PARSE;
                                else if (strcmp(point_str, "before_route")      == 0) point = KEEL_HOOK_BEFORE_ROUTE;
                                else if (strcmp(point_str, "before_send")       == 0) point = KEEL_HOOK_BEFORE_SEND;

                                if (point == KEEL_HOOK_POINT_COUNT) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Unknown hook point '%s'", wg->name, point_str);
                                    continue;
                                }

                                char py_module[256]    = {0};
                                char py_script[PATH_MAX] = {0};
                                char py_func[128]      = {0};
                                int priority = 100;
                                const char* pp = val;
                                while (*pp) {
                                    while (*pp && isspace((unsigned char)*pp)) pp++;
                                    if (!*pp) break;
                                    if      (strncmp(pp, "module=",  7) == 0) { pp += 7; char* e = py_module; while (*pp && !isspace((unsigned char)*pp)) *e++ = *pp++; *e = '\0'; }
                                    else if (strncmp(pp, "script=",  7) == 0) { pp += 7; char* e = py_script; while (*pp && !isspace((unsigned char)*pp)) *e++ = *pp++; *e = '\0'; }
                                    else if (strncmp(pp, "func=",    5) == 0) { pp += 5; char* e = py_func;   while (*pp && !isspace((unsigned char)*pp)) *e++ = *pp++; *e = '\0'; }
                                    else if (strncmp(pp, "priority=",9) == 0) { pp += 9; priority = atoi(pp); while (*pp && !isspace((unsigned char)*pp)) pp++; }
                                    else { while (*pp && !isspace((unsigned char)*pp)) pp++; }
                                }

                                /* Resolve relative script path */
                                char script_dir[PATH_MAX] = {0};
                                if (py_script[0] && !py_module[0]) {
                                    if (py_script[0] != '/' && config_dir[0]) {
                                        char resolved[PATH_MAX * 2];
                                        snprintf(resolved, sizeof(resolved), "%s/%s", config_dir, py_script);
                                        strncpy(py_script, resolved, sizeof(py_script) - 1);
                                        py_script[sizeof(py_script) - 1] = '\0';
                                    }
                                    char dir_buf[PATH_MAX];
                                    strncpy(dir_buf, py_script, sizeof(dir_buf) - 1);
                                    dir_buf[sizeof(dir_buf) - 1] = '\0';
                                    char* last_slash = strrchr(dir_buf, '/');
                                    if (last_slash) {
                                        *last_slash = '\0';
                                        strncpy(script_dir, dir_buf, sizeof(script_dir) - 1);
                                        const char* basename = last_slash + 1;
                                        strncpy(py_module, basename, sizeof(py_module) - 1);
                                    } else {
                                        strncpy(py_module, py_script, sizeof(py_module) - 1);
                                    }
                                    size_t mlen = strlen(py_module);
                                    if (mlen > 3 && strcmp(py_module + mlen - 3, ".py") == 0)
                                        py_module[mlen - 3] = '\0';
                                }

                                if (!py_module[0] || !py_func[0]) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Python hook '%s': missing script=/module= or func=", wg->name, key);
                                    continue;
                                }
                                if (!keel_python_available()) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Python hook '%s' requires -DKEEL_ENABLE_PYTHON=ON", wg->name, hook_name);
                                    continue;
                                }

                                if (!python_inited && keel_python_available()) {
                                    keel_error_t pyerr = keel_python_init();
                                    if (pyerr != KEEL_OK) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Failed to initialize Python: error %d", wg->name, pyerr);
                                        continue;
                                    }
                                    python_inited = true;
                                }

                                if (script_dir[0])
                                    keel_python_add_script_dir(script_dir);

                                keel_hook_handle_t* h = keel_hook_register_python(reg, point, hook_name, py_module, py_func, priority);
                                if (h) {
                                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                        "[%s] Registered Python hook '%s' at %s (module=%s func=%s priority=%d)",
                                        wg->name, hook_name, keel_hook_point_name(point), py_module, py_func, priority);
                                    python_count++;
                                } else {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Failed to register Python hook '%s'", wg->name, hook_name);
                                }
                            } else {
                                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                    "[%s] Unknown hook key prefix: '%s' (expected hook.plugin.*, hook.lua.*, or hook.python.*)",
                                    wg->name, key);
                            }
                        } /* for each hook entry */

                        if (need_lua && keel_lua_available() && lua_count > 0) {
                            keel_error_t err = keel_lua_init();
                            if (err != KEEL_OK)
                                KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "[%s] Failed to initialize Lua: error %d", wg->name, err);
                            else
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[%s] Lua interpreter initialized", wg->name);
                        }
                        if (python_inited)
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[%s] Python interpreter initialized", wg->name);

                        wg->hook_registry = reg;

                        size_t total = lua_count + python_count + plugin_count;
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] Hook summary: %zu hooks registered (%zu native plugin, %zu Lua, %zu Python)",
                            wg->name, total, plugin_count, lua_count, python_count);

                        for (int pt = 0; pt < KEEL_HOOK_POINT_COUNT; pt++) {
                            keel_hook_stats_t st = keel_hook_get_stats(reg, (keel_hook_point_t)pt);
                            if (st.hook_count > 0)
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[%s]   %-20s: %u hook(s)",
                                    wg->name, keel_hook_point_name((keel_hook_point_t)pt), st.hook_count);
                        }
                        (void)need_python; /* suppress unused-variable warning */
                    }
                } else {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "[%s] No hooks section [%s] found (hooks disabled for this group)",
                        wg->name, hooks_section);
                }
            }
        } /* end for each worker group */

        /* ----------------------------------------------------------------
         * [logging] section
         * ---------------------------------------------------------------- */
        if (keel_config_has_section(config, "logging")) {
            const char* v;
            v = keel_config_get_string(config, "logging", "plugin", NULL);           if (v) g_log_cfg.plugin_name = v;
            v = keel_config_get_string(config, "logging", "plugin_path", NULL);      if (v) g_log_cfg.plugin_path = v;
            v = keel_config_get_string(config, "logging", "log_file", NULL);         if (v) g_log_cfg.log_file = v;
            v = keel_config_get_string(config, "logging", "log_level", NULL);        if (v) g_log_cfg.log_level_str = v;
            v = keel_config_get_string(config, "logging", "query_log_mode", NULL);   if (v) g_log_cfg.query_log_mode_str = v;
            v = keel_config_get_string(config, "logging", "syslog_ident", NULL);     if (v) g_log_cfg.syslog_ident = v;
            v = keel_config_get_string(config, "logging", "syslog_facility", NULL);  if (v) g_log_cfg.syslog_facility = v;
            g_log_cfg.log_timestamps = keel_config_get_bool(config, "logging", "log_timestamps", true);
            g_log_cfg.log_source     = keel_config_get_bool(config, "logging", "log_source",     true);
            g_log_cfg.log_dest       = keel_config_get_bool(config, "logging", "log_dest",       true);
            g_log_cfg.log_username   = keel_config_get_bool(config, "logging", "log_username",   true);
            g_log_cfg.log_database   = keel_config_get_bool(config, "logging", "log_database",   true);
            g_log_cfg.log_query_tree = keel_config_get_bool(config, "logging", "log_query_tree", false);
            g_log_cfg.use_colors     = keel_config_get_bool(config, "logging", "use_colors",     true);
            v = keel_config_get_string(config, "logging", "log_format", NULL);
            if (v && strcmp(v, "json") == 0) g_log_cfg.json_format = true;
            int64_t mqlen = keel_config_get_int(config, "logging", "max_query_len", 0);
            if (mqlen > 0) g_log_cfg.max_query_len = (size_t)mqlen;
        }

        /* ----------------------------------------------------------------
         * [stats] section
         * ---------------------------------------------------------------- */
        if (keel_config_has_section(config, "stats")) {
            const char* v = keel_config_get_string(config, "stats", "level", NULL);
            if (v) g_config.stats_level_str = v;
            int64_t interval = keel_config_get_duration_ms(config, "stats", "log_interval", 0);
            if (interval > 0) g_config.stats_interval_ms = (uint32_t)interval;
            g_config.hotpath_instr_mask =
                apply_hotpath_instr_mask_from_config(config, g_config.hotpath_instr_mask);
        }

        /* [instrument] section */
        g_config.instr_mask = apply_instr_mask_from_config(config, g_config.instr_mask);

        /* [query_rule.N] sections */
        {
            keel_query_rules_t* qr = NULL;
            keel_error_t qr_err = keel_query_rules_load(config, &qr);
            if (qr_err != KEEL_OK) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "Failed to load query rules (err=%d); query routing rules disabled", (int)qr_err);
            } else {
                keel_query_rules_replace(&g_config.query_rules, qr);
                if (qr && qr->count > 0)
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Loaded %zu query rule(s)", qr->count);
            }
        }

        /* [admin] section */
        if (keel_config_has_section(config, "admin")) {
            const char* v = keel_config_get_string(config, "admin", "enabled", "false");
            g_admin_cfg.admin_enabled = (v && (strcmp(v,"true")==0||strcmp(v,"1")==0||strcmp(v,"yes")==0));
            v = keel_config_get_string(config, "admin", "listen_addr", NULL);
            if (v) g_admin_cfg.admin_addr = v;
            int64_t aport = keel_config_get_int(config, "admin", "listen_port", 6433);
            if (aport > 0 && aport <= 65535) g_admin_cfg.admin_port = (uint16_t)aport;
            v = keel_config_get_string(config, "admin", "users", NULL);    if (v) g_admin_cfg.admin_users = v;
            v = keel_config_get_string(config, "admin", "password", NULL); if (v) g_admin_cfg.admin_password = v;
        }

        /* [prometheus] section */
        if (keel_config_has_section(config, "prometheus")) {
            const char* v = keel_config_get_string(config, "prometheus", "enabled", "false");
            g_admin_cfg.prom_enabled = (v && (strcmp(v,"true")==0||strcmp(v,"1")==0||strcmp(v,"yes")==0));
            v = keel_config_get_string(config, "prometheus", "listen_addr", NULL);
            if (v) g_admin_cfg.prom_addr = v;
            int64_t pport = keel_config_get_int(config, "prometheus", "port", 9101);
            if (pport > 0 && pport <= 65535) g_admin_cfg.prom_port = (uint16_t)pport;
            v = keel_config_get_string(config, "prometheus", "path", NULL);
            if (v) g_admin_cfg.prom_path = v;
        }

        /* [observability] section — OTLP */
#ifdef KEEL_HAS_OTLP
        (void)keel_otlp_config_load(config, &g_otlp_cfg, &g_otlp_enabled);
        if (g_otlp_enabled) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "OTLP exporter configured: endpoint=%s interval=%ums queue_cap=%u",
                g_otlp_cfg.http.endpoint_url, g_otlp_cfg.interval_ms, g_otlp_cfg.queue_capacity);
        }
#endif

        /* [cluster] section */
        if (keel_config_has_section(config, "cluster")) {
            const char* v;
            v = keel_config_get_string(config, "cluster", "enabled", "false");
            g_cluster_cfg.enabled = (v && (strcmp(v,"true")==0||strcmp(v,"1")==0||strcmp(v,"yes")==0));

            v = keel_config_get_string(config, "cluster", "node_id", NULL);
            if (v) { size_t len = strlen(v); if (len >= KEEL_CLUSTER_MAX_NODE_ID) len = KEEL_CLUSTER_MAX_NODE_ID - 1; memcpy(g_cluster_cfg.node_id, v, len); g_cluster_cfg.node_id[len] = '\0'; }

            v = keel_config_get_string(config, "cluster", "listen_addr", NULL);
            if (v) { size_t len = strlen(v); if (len >= KEEL_CLUSTER_MAX_ADDR) len = KEEL_CLUSTER_MAX_ADDR - 1; memcpy(g_cluster_cfg.listen_addr, v, len); g_cluster_cfg.listen_addr[len] = '\0'; }

            int64_t cport = keel_config_get_int(config, "cluster", "listen_port", 9100);
            if (cport > 0 && cport <= 65535) g_cluster_cfg.listen_port = (uint16_t)cport;

            int64_t hb_int = keel_config_get_duration_ms(config, "cluster", "heartbeat_interval", 1000);
            if (hb_int > 0) g_cluster_cfg.heartbeat_interval_ms = (uint32_t)hb_int;
            int64_t hb_to = keel_config_get_duration_ms(config, "cluster", "heartbeat_timeout", 5000);
            if (hb_to > 0) g_cluster_cfg.heartbeat_timeout_ms = (uint32_t)hb_to;
            int64_t ft = keel_config_get_int(config, "cluster", "failure_threshold", 3);
            if (ft > 0) g_cluster_cfg.failure_threshold = (uint32_t)ft;

            g_cluster_cfg.auto_sync        = keel_config_get_bool(config, "cluster", "auto_sync",        true);
            g_cluster_cfg.election_enabled = keel_config_get_bool(config, "cluster", "election_enabled", true);

            v = keel_config_get_string(config, "cluster", "election_state_path", NULL);
            if (v && *v) { size_t len = strlen(v); if (len >= sizeof(g_cluster_cfg.election_state_path)) len = sizeof(g_cluster_cfg.election_state_path)-1; memcpy(g_cluster_cfg.election_state_path, v, len); g_cluster_cfg.election_state_path[len]='\0'; }

            v = keel_config_get_string(config, "cluster", "vip", NULL);
            if (v && *v) { size_t len = strlen(v); if (len >= sizeof(g_cluster_cfg.vip)) len = sizeof(g_cluster_cfg.vip)-1; memcpy(g_cluster_cfg.vip, v, len); g_cluster_cfg.vip[len]='\0'; }

            v = keel_config_get_string(config, "cluster", "vip_interface", NULL);
            if (v && *v) { size_t len = strlen(v); if (len >= sizeof(g_cluster_cfg.vip_interface)) len = sizeof(g_cluster_cfg.vip_interface)-1; memcpy(g_cluster_cfg.vip_interface, v, len); g_cluster_cfg.vip_interface[len]='\0'; }

            {
                const char* codec = keel_config_get_string(config, "cluster", "compress", "none");
                if      (codec && strcasecmp(codec, "zlib") == 0) g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
                else if (codec && strcasecmp(codec, "zstd") == 0) g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;
                else                                              g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_NONE;
            }
            {
                int64_t thr = keel_config_get_bytes(config, "cluster", "compress_threshold", 256);
                if (thr > 0 && thr <= KEEL_CLUSTER_MAX_PAYLOAD)
                    g_cluster_cfg.compress_threshold_bytes = (uint32_t)thr;
            }

            v = keel_config_get_string(config, "cluster", "initial_peers", NULL);
            if (v && *v) {
                char buf[2048];
                size_t vlen = strlen(v);
                if (vlen >= sizeof(buf)) vlen = sizeof(buf) - 1;
                memcpy(buf, v, vlen); buf[vlen] = '\0';
                char* saveptr = NULL;
                char* tok = strtok_r(buf, ",", &saveptr);
                while (tok && g_cluster_cfg.initial_peer_count < KEEL_CLUSTER_MAX_PEERS) {
                    while (*tok == ' ') tok++;
                    char* end = tok + strlen(tok) - 1;
                    while (end > tok && *end == ' ') *end-- = '\0';
                    char* colon = strrchr(tok, ':');
                    if (colon && colon != tok) {
                        *colon = '\0';
                        size_t idx = g_cluster_cfg.initial_peer_count;
                        size_t alen = strlen(tok);
                        if (alen >= KEEL_CLUSTER_MAX_ADDR) alen = KEEL_CLUSTER_MAX_ADDR - 1;
                        memcpy(g_cluster_cfg.initial_peers[idx].addr, tok, alen);
                        g_cluster_cfg.initial_peers[idx].addr[alen] = '\0';
                        g_cluster_cfg.initial_peers[idx].port = (uint16_t)atoi(colon + 1);
                        g_cluster_cfg.initial_peer_count++;
                    }
                    tok = strtok_r(NULL, ",", &saveptr);
                }
            }
        }

        /* [security] section — delegated to security module */
        security_config_from_ini(config);

        /* [tracing] section */
        if (keel_config_has_section(config, "tracing")) {
            const char* v;
            g_trace_cfg.enabled = keel_config_get_bool(config, "tracing", "enabled", g_trace_cfg.enabled);
            v = keel_config_get_string(config, "tracing", "endpoint", NULL);
            if (v) { size_t len = strlen(v); if (len >= sizeof(g_trace_cfg.endpoint)) len = sizeof(g_trace_cfg.endpoint)-1; memcpy(g_trace_cfg.endpoint, v, len); g_trace_cfg.endpoint[len]='\0'; }
            v = keel_config_get_string(config, "tracing", "service_name", NULL);
            if (v) { size_t len = strlen(v); if (len >= sizeof(g_trace_cfg.service_name)) len = sizeof(g_trace_cfg.service_name)-1; memcpy(g_trace_cfg.service_name, v, len); g_trace_cfg.service_name[len]='\0'; }
            int64_t sr = keel_config_get_int(config, "tracing", "sample_rate_ppm", g_trace_cfg.sample_rate_ppm);
            if (sr >= 0 && sr <= 1000000) g_trace_cfg.sample_rate_ppm = (uint32_t)sr;
            int64_t bs = keel_config_get_int(config, "tracing", "batch_size", g_trace_cfg.batch_size);
            if (bs > 0) g_trace_cfg.batch_size = (uint32_t)bs;
            int64_t fi = keel_config_get_duration_ms(config, "tracing", "flush_interval", g_trace_cfg.flush_interval_ms);
            if (fi > 0) g_trace_cfg.flush_interval_ms = (uint32_t)fi;
            int64_t rc = keel_config_get_int(config, "tracing", "ring_capacity", g_trace_cfg.ring_capacity);
            if (rc > 0) g_trace_cfg.ring_capacity = (uint32_t)rc;
            int64_t et = keel_config_get_duration_ms(config, "tracing", "export_timeout", g_trace_cfg.export_timeout_ms);
            if (et > 0) g_trace_cfg.export_timeout_ms = (uint32_t)et;
            const char* proto = keel_config_get_string(config, "tracing", "protocol", NULL);
            if (proto) {
                if (strcmp(proto, "http/protobuf") == 0) g_trace_cfg.protocol = KEEL_OTLP_HTTP_PROTOBUF;
                else                                      g_trace_cfg.protocol = KEEL_OTLP_HTTP_JSON;
            }
        }

        /* Note: don't free config — strings are referenced by engine config */
    } /* end if (path) */

apply_env_overrides:
    /* ----------------------------------------------------------------
     * Apply KEEL_CLUSTER_* environment variable overrides.
     * Environment variables always win over INI file settings.
     * ---------------------------------------------------------------- */
    {
        const char* ev;

        ev = getenv("KEEL_CLUSTER_ENABLED");
        if (ev) g_cluster_cfg.enabled = (strcmp(ev,"true")==0||strcmp(ev,"1")==0||strcmp(ev,"yes")==0);

        ev = getenv("KEEL_CLUSTER_NODE_ID");
        if (ev) { size_t len = strlen(ev); if (len >= KEEL_CLUSTER_MAX_NODE_ID) len = KEEL_CLUSTER_MAX_NODE_ID-1; memcpy(g_cluster_cfg.node_id, ev, len); g_cluster_cfg.node_id[len]='\0'; }

        ev = getenv("KEEL_CLUSTER_LISTEN_ADDR");
        if (ev) { size_t len = strlen(ev); if (len >= KEEL_CLUSTER_MAX_ADDR) len = KEEL_CLUSTER_MAX_ADDR-1; memcpy(g_cluster_cfg.listen_addr, ev, len); g_cluster_cfg.listen_addr[len]='\0'; }

        ev = getenv("KEEL_CLUSTER_LISTEN_PORT");
        if (ev) { long p = strtol(ev, NULL, 10); if (p > 0 && p <= 65535) g_cluster_cfg.listen_port = (uint16_t)p; }

        ev = getenv("KEEL_CLUSTER_HB_INTERVAL");
        if (ev) { long v = strtol(ev, NULL, 10); if (v > 0) g_cluster_cfg.heartbeat_interval_ms = (uint32_t)v; }

        ev = getenv("KEEL_CLUSTER_HB_TIMEOUT");
        if (ev) { long v = strtol(ev, NULL, 10); if (v > 0) g_cluster_cfg.heartbeat_timeout_ms = (uint32_t)v; }

        ev = getenv("KEEL_CLUSTER_FAIL_THRESHOLD");
        if (ev) { long v = strtol(ev, NULL, 10); if (v > 0) g_cluster_cfg.failure_threshold = (uint32_t)v; }

        ev = getenv("KEEL_CLUSTER_AUTO_SYNC");
        if (ev) g_cluster_cfg.auto_sync = (strcmp(ev,"true")==0||strcmp(ev,"1")==0||strcmp(ev,"yes")==0);

        ev = getenv("KEEL_CLUSTER_ELECTION_ENABLED");
        if (ev) g_cluster_cfg.election_enabled = (strcmp(ev,"true")==0||strcmp(ev,"1")==0||strcmp(ev,"yes")==0);

        ev = getenv("KEEL_CLUSTER_ELECTION_STATE_PATH");
        if (ev && ev[0]) { size_t len = strlen(ev); if (len >= sizeof(g_cluster_cfg.election_state_path)) len = sizeof(g_cluster_cfg.election_state_path)-1; memcpy(g_cluster_cfg.election_state_path, ev, len); g_cluster_cfg.election_state_path[len]='\0'; }

        ev = getenv("KEEL_CLUSTER_VIP");
        if (ev && ev[0]) { size_t len = strlen(ev); if (len >= sizeof(g_cluster_cfg.vip)) len = sizeof(g_cluster_cfg.vip)-1; memcpy(g_cluster_cfg.vip, ev, len); g_cluster_cfg.vip[len]='\0'; }

        ev = getenv("KEEL_CLUSTER_VIP_INTERFACE");
        if (ev && ev[0]) { size_t len = strlen(ev); if (len >= sizeof(g_cluster_cfg.vip_interface)) len = sizeof(g_cluster_cfg.vip_interface)-1; memcpy(g_cluster_cfg.vip_interface, ev, len); g_cluster_cfg.vip_interface[len]='\0'; }

        ev = getenv("KEEL_CLUSTER_COMPRESS");
        if (ev && ev[0]) {
            if      (strcasecmp(ev, "zlib") == 0) g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
            else if (strcasecmp(ev, "zstd") == 0) g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;
            else                                  g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_NONE;
        }

        ev = getenv("KEEL_CLUSTER_INITIAL_PEERS");
        if (ev && ev[0]) {
            char peers_buf[4096];
            size_t plen = strlen(ev);
            if (plen >= sizeof(peers_buf)) plen = sizeof(peers_buf) - 1;
            memcpy(peers_buf, ev, plen); peers_buf[plen] = '\0';
            g_cluster_cfg.initial_peer_count = 0;
            char* saveptr2 = NULL;
            char* tok = strtok_r(peers_buf, ",", &saveptr2);
            while (tok && g_cluster_cfg.initial_peer_count < KEEL_CLUSTER_MAX_PEERS) {
                while (*tok == ' ') tok++;
                char* colon = strrchr(tok, ':');
                if (colon && colon > tok) {
                    *colon = '\0';
                    size_t idx = g_cluster_cfg.initial_peer_count;
                    size_t alen = strlen(tok);
                    if (alen >= KEEL_CLUSTER_MAX_ADDR) alen = KEEL_CLUSTER_MAX_ADDR - 1;
                    memcpy(g_cluster_cfg.initial_peers[idx].addr, tok, alen);
                    g_cluster_cfg.initial_peers[idx].addr[alen] = '\0';
                    g_cluster_cfg.initial_peers[idx].port = (uint16_t)atoi(colon + 1);
                    g_cluster_cfg.initial_peer_count++;
                }
                tok = strtok_r(NULL, ",", &saveptr2);
            }
        }
    }

    return config;
}
