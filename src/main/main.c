/**
 * @file main.c
 * @brief Process bootstrap and runtime orchestration for KEEL.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 * Orchestrates the bootstrap pipeline: CLI parsing, config loading, engine
 * creation, signal handling, and graceful shutdown.  All subsystem
 * implementations live in the accompanying module files.
 */

/* ---- Module headers (extracted subsystems) ---- */
#include "keel/main/cli.h"
#include "keel/main/process.h"
#include "keel/main/security.h"
#include "keel/main/worker_group.h"
#include "keel/main/config_load.h"
#include "keel/main/stats_display.h"

/* ---- KEEL subsystem headers needed directly in main() ---- */
#include "keel/engine/engine.h"
#include "keel/engine/backend_pool.h"
#include "keel/reactor/reactor.h"
#include "keel/core/stats.h"
#include "keel/core/admin.h"
#include "keel/core/cluster.h"
#include "keel/core/config.h"
#include "keel/core/config_reload.h"
#include "keel/core/query_rules.h"
#include "keel/core/throttle.h"
#include "keel/core/router_discovery.h"
#include "keel/core/router.h"
#include "keel/core/router_plugin.h"
#include "keel/core/sharding.h"
#include "keel/core/auth.h"
#include "keel/probe/probe.h"
#include "keel/failover/failover_manager.h"
#include "keel/mem/mem.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/tls_auto.h"
#include "keel/log/log.h"
#include "keel/log/audit_log.h"
#include "keel/trace/trace.h"
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
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <stdatomic.h>
#include <sys/resource.h>
#include "keel/core/config_yaml.h"
#include "keel/core/config_migrate.h"
#include "keel/util/platform_compat.h"

/* ============================================================================
 * Signal State
 * ============================================================================ */

static volatile sig_atomic_t g_should_stop = 0;

static void signal_handler(int sig) {
    (void)sig;
    g_should_stop = 1;
}

static void stats_signal_handler(int sig) {
    (void)sig;
    /* Handled via sigwait in the main loop */
}

/* ============================================================================
 * Entry Point
 * ============================================================================ */

int main(int argc, char** argv) {
    /* Disable buffering for consistent output */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    /* Initialize memory subsystem */
    keel_mem_init(NULL);

    /* Raise file descriptor limit to the hard maximum */
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
            rlim_t target = rl.rlim_max;
            if (target == RLIM_INFINITY || target > 1048576)
                target = 1048576;
            if (rl.rlim_cur < target) {
                rl.rlim_cur = target;
                if (setrlimit(RLIMIT_NOFILE, &rl) < 0) {
                    fprintf(stderr, "Warning: could not raise NOFILE limit to %lu: %s\n",
                            (unsigned long)target, strerror(errno));
                }
            }
        }
    }

    /* Parse command line */
    options_t opts;
    if (cli_parse(argc, argv, &opts) < 0) {
        print_usage(argv[0]);
        return 1;
    }

    if (opts.help) {
        print_usage(argv[0]);
        return 0;
    }

    if (opts.version) {
        print_version();
        return 0;
    }

    if (opts.migrate_in) {
        keel_error_t rc = keel_config_migrate_file(opts.migrate_in, opts.migrate_out);
        if (rc != KEEL_OK) {
            fprintf(stderr, "keel: --migrate-config: migration failed (%d)\n", (int)rc);
            return 1;
        }
        if (opts.migrate_out && strcmp(opts.migrate_out, "-") != 0) {
            fprintf(stderr, "keel: migrated %s -> %s (schema v%d)\n",
                    opts.migrate_in, opts.migrate_out, KEEL_CONFIG_SCHEMA_VERSION);
        }
        return 0;
    }

    if (opts.convert_in) {
        if (!opts.convert_out) {
            fprintf(stderr,
                    "keel: --convert-config requires --output (-o) FILE; the target\n"
                    "      format is inferred from the output file's extension\n"
                    "      (.yaml/.yml -> YAML, anything else -> INI).\n");
            return 1;
        }
        keel_config_format_t in_fmt  = keel_config_detect_format(opts.convert_in);
        keel_config_format_t out_fmt = keel_config_detect_format(opts.convert_out);
        if (in_fmt == out_fmt) {
            fprintf(stderr,
                    "keel: --convert-config: input and output have the same format "
                    "(%s); nothing to do.\n",
                    in_fmt == KEEL_CONFIG_FORMAT_YAML ? "YAML" : "INI");
            return 1;
        }
        keel_error_t rc = (in_fmt == KEEL_CONFIG_FORMAT_INI)
            ? keel_config_convert_ini_to_yaml(opts.convert_in, opts.convert_out)
            : keel_config_convert_yaml_to_ini(opts.convert_in, opts.convert_out);
        if (rc != KEEL_OK) {
            fprintf(stderr, "keel: --convert-config: failed (%d)\n", (int)rc);
            return 1;
        }
        fprintf(stderr, "keel: converted %s -> %s\n", opts.convert_in, opts.convert_out);
        return 0;
    }

    /* Seed hot-path mask from environment before config overrides it */
    g_config.hotpath_instr_mask = build_hotpath_instr_mask_from_env();

    /* Apply CLI security overrides */
    if (opts.strict_auth)
        g_security_cfg.strict_auth = true;

    /* Accept positional argument as config file if -c was not given */
    if (!opts.config_file && optind < argc)
        opts.config_file = argv[optind];

    /* Load configuration file (populates all g_* globals) */
    bool config_fatal = false;
    keel_config_t* config = config_load(opts.config_file, &config_fatal);
    if (config_fatal) {
        cleanup_startup_failure(NULL, false);
        return 1;
    }

    /* Strict-auth validation (moved out of config_load for clean separation) */
    if (g_security_cfg.strict_auth) {
        for (size_t gi = 0; gi < g_num_groups; gi++) {
            worker_group_t* wg = &g_groups[gi];
            if (wg->auth_method == KEEL_AUTH_MD5) {
                fprintf(stderr,
                    "FATAL: [%s] auth_method=md5 is rejected by --strict-auth. "
                    "Switch to scram-sha-256 for production deployments.\n",
                    wg->name);
                cleanup_startup_failure(config, false);
                return 1;
            }
            if (wg->auth_method == KEEL_AUTH_TRUST) {
                fprintf(stderr,
                    "FATAL: [%s] auth_method=trust is rejected by --strict-auth. "
                    "Trust authentication accepts any connection without a password.\n",
                    wg->name);
                cleanup_startup_failure(config, false);
                return 1;
            }
        }
    }

    /* Validate experimental feature gates */
    {
        bool cluster_compression_enabled =
            (g_cluster_cfg.compress_codec != KEEL_CLUSTER_COMPRESS_NONE);
        if (!validate_experimental_feature_gates(cluster_compression_enabled)) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CONFIG,
                "Configuration rejected: experimental features are disabled. "
                "Set [keel] experimental_features=true to opt in.");
            cleanup_startup_failure(config, false);
            return 1;
        }
    }

    /* --check-config: validate and exit */
    if (opts.check_config) {
        printf("Configuration OK\n");
        if (config) keel_config_free(config);
        keel_mem_shutdown();
        return 0;
    }

    /* Apply CLI log-level override */
    g_config.log_level = opts.log_level;

    /* If no worker groups were parsed, create a single implicit default group */
    if (g_num_groups == 0) {
        worker_group_t* wg = &g_groups[0];
        worker_group_defaults(wg);
        snprintf(wg->section, sizeof(wg->section), "default");
        wg->name             = "default";
        wg->listen_addr      = g_config.listen_addr;
        wg->listen_port      = g_config.listen_port;
        wg->default_protocol = g_config.default_protocol;
        wg->num_workers      = g_config.num_workers;
        wg->pool_min_size    = g_config.pool_min_size;
        wg->pool_max_size    = g_config.pool_max_size;
        wg->backend_host     = g_config.backend_host;
        wg->backend_port     = g_config.backend_port;
        wg->backend_user     = g_config.backend_user;
        wg->backend_password = g_config.backend_password;
        wg->backend_database = g_config.backend_database;
        g_num_groups = 1;
    }

    /* Apply CLI overrides to the first (or only) group */
    if (opts.listen_addr && strcmp(opts.listen_addr, "0.0.0.0") != 0)
        g_groups[0].listen_addr = opts.listen_addr;
    if (opts.listen_port != 6432)
        g_groups[0].listen_port = opts.listen_port;
    if (opts.backend_host && strcmp(opts.backend_host, "127.0.0.1") != 0)
        g_groups[0].backend_host = opts.backend_host;
    if (opts.backend_port != 5432)
        g_groups[0].backend_port = opts.backend_port;
    if (opts.num_workers > 0)
        g_groups[0].num_workers = opts.num_workers;

    /* Print startup banner */
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║   KEEL - High-Performance Database Proxy  v%d.%d.%d              ║\n",
           KEEL_VERSION_MAJOR, KEEL_VERSION_MINOR, KEEL_VERSION_PATCH);
    printf("╚═══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];
        char enabled_features[512];
        bool cluster_compression_enabled =
            (g_cluster_cfg.compress_codec != KEEL_CLUSTER_COMPRESS_NONE);
        build_runtime_feature_list(wg, cluster_compression_enabled,
                                   enabled_features, sizeof(enabled_features));
        printf("  [%s]\n", wg->name);
        printf("    Listen:   %s:%d\n", wg->listen_addr, wg->listen_port);
        printf("    Backend:  %s:%d\n", wg->backend_host, wg->backend_port);
        printf("    Workers:  %s\n", wg->num_workers == 0 ? "auto (one per CPU)" : "specified");
        printf("    Pool:     %zu min / %zu max (per server)\n", wg->pool_min_size, wg->pool_max_size);
        printf("    Protocol: %s\n", wg->default_protocol);
        printf("    Runtime tier: %s. Enabled features: [%s]\n",
               keel_tier_name(wg->runtime_mode), enabled_features);
        if (KEEL_TIER_IS_EXPERIMENTAL(wg->runtime_mode)) {
            printf("    WARNING: mode=full enables hardening/experimental subsystems "
                   "(hooks, transaction tracking, LSN capture) and is not the "
                   "recommended production default.\n");
        }
    }
    printf("  Stats:    level=%s", g_config.stats_level_str);
    if (g_config.stats_interval_ms > 0)
        printf("  interval=%ums", g_config.stats_interval_ms);
    printf("\n");
    printf("  HotPath:  pool=%s backend=%s query_split=%s deferred_send=%s\n",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_POOL) ? "on" : "off",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND) ? "on" : "off",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT) ? "on" : "off",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_DEFERRED_SEND) ? "on" : "off");
    printf("  Security: priv_drop=%s user=%s group=%s seccomp=%s\n",
        g_security_cfg.privilege_drop ? "on" : "off",
        g_security_cfg.run_user  ? g_security_cfg.run_user  : "(none)",
        g_security_cfg.run_group ? g_security_cfg.run_group : "(none)",
        g_security_cfg.seccomp_mode == KEEL_SECCOMP_STRICT   ? "strict"   :
        g_security_cfg.seccomp_mode == KEEL_SECCOMP_BASELINE ? "baseline" : "off");
    printf("\n");

    /* Initialise log plugin and query logger */
    log_init();

    /* Daemonize if requested */
    if (opts.daemonize) {
        printf("Daemonizing...\n");
        if (daemonize_process() < 0) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "Failed to daemonize");
            cleanup_startup_failure(config, false);
            return 1;
        }
    }

    /* Set up signal handlers */
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGUSR1, stats_signal_handler);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGSEGV, crash_handler);
    signal(SIGABRT, crash_handler);
    signal(SIGBUS,  crash_handler);

    /* Initialize TLS subsystem */
    bool tls_initialized = false;
    if (keel_tls_init() != KEEL_OK) {
        KEEL_LOG_FATAL(KEEL_LOG_CAT_TLS, "Failed to initialize TLS subsystem");
        cleanup_startup_failure(config, false);
        return 1;
    }
    tls_initialized = true;
    KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "TLS subsystem initialized");

    uint32_t sys_instr_mask = build_system_instr_mask_from_env();

    /* ====================================================================
     * Phase 1: Create listen sockets and engines (no worker threads yet).
     * ==================================================================== */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];

        wg->listen_fd = create_listen_socket(wg->listen_addr, wg->listen_port,
                                              wg->listen_backlog);
        if (wg->listen_fd < 0) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "[%s] Failed to create listen socket on %s:%d",
                          wg->name, wg->listen_addr, wg->listen_port);
            cleanup_startup_failure(config, tls_initialized);
            return 1;
        }
        printf("  [%s] Socket: fd=%d (listening on %s:%d)\n",
               wg->name, wg->listen_fd, wg->listen_addr, wg->listen_port);
        printf("  [%s] Mode:   %s\n", wg->name, keel_tier_name(wg->runtime_mode));
        {
            char enabled_features[512];
            bool cluster_compression_enabled =
                (g_cluster_cfg.compress_codec != KEEL_CLUSTER_COMPRESS_NONE);
            build_runtime_feature_list(wg, cluster_compression_enabled,
                                       enabled_features, sizeof(enabled_features));
            KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG,
                "[%s] Runtime tier: %s. Enabled features: [%s]",
                wg->name, keel_tier_name(wg->runtime_mode), enabled_features);
        }

        /* Build per-group engine config */
        keel_engine_config_t engine_cfg = KEEL_ENGINE_CONFIG_DEFAULT;
        engine_cfg.num_workers       = wg->num_workers;
        engine_cfg.pin_workers       = wg->pin_workers;
        engine_cfg.session_pool_size = wg->session_pool_size;
        engine_cfg.slab_buffer_size  = wg->buffer_size;
        if (wg->max_clients > 0) {
            uint32_t nw = wg->num_workers > 0 ? wg->num_workers : 1;
            engine_cfg.max_clients_per_worker = (wg->max_clients + nw - 1) / nw;
        }
        engine_cfg.idle_timeout_ms          = (uint32_t)wg->idle_timeout_ms;
        engine_cfg.connect_timeout_ms       = (uint32_t)wg->connect_timeout_ms;
        engine_cfg.pool_idle_timeout_ms     = wg->idle_timeout_ms;
        engine_cfg.pool_prune_interval_ms   = wg->pool_prune_interval_ms;
        engine_cfg.pool_refill_interval_ms  = wg->pool_refill_interval_ms;
        engine_cfg.pool_refill_backoff_ms   = wg->pool_refill_backoff_ms;
        engine_cfg.pool_max_waiting         = wg->pool_max_waiting;
        engine_cfg.default_protocol         = wg->default_protocol;
        engine_cfg.pool_min_size            = wg->pool_min_size;
        engine_cfg.pool_max_size            = wg->pool_max_size;
        engine_cfg.stats_level              = (int)keel_stats_level_from_str(g_config.stats_level_str);
        engine_cfg.stats_interval_ms        = g_config.stats_interval_ms;
        engine_cfg.hotpath_instr_mask       = g_config.hotpath_instr_mask;
        engine_cfg.instr_mask               = g_config.instr_mask;
        engine_cfg.backend_host             = wg->backend_host;
        engine_cfg.backend_port             = wg->backend_port;
        engine_cfg.backend_user             = wg->backend_user;
        engine_cfg.backend_password         = wg->backend_password;
        engine_cfg.backend_database         = wg->backend_database;
        engine_cfg.server_pool              = wg->server_pool;
        engine_cfg.hook_registry            = wg->hook_registry;

        /* Register declarative query rules as a BEFORE_ROUTE hook */
        if (g_config.query_rules && g_config.query_rules->count > 0) {
            if (!engine_cfg.hook_registry)
                engine_cfg.hook_registry = keel_hook_registry_create();
            if (engine_cfg.hook_registry) {
                keel_hook_register(engine_cfg.hook_registry,
                                   KEEL_HOOK_BEFORE_ROUTE,
                                   "query_rules",
                                   keel_query_rules_hook,
                                   10,
                                   g_config.query_rules);
                wg->hook_registry = engine_cfg.hook_registry;
            }
        }

        /* Register throttle rules as a BEFORE_ROUTE hook */
        {
            keel_throttle_rules_t* tr = NULL;
            if (keel_throttle_rules_load(config, &tr) == KEEL_OK && tr) {
                if (!engine_cfg.hook_registry)
                    engine_cfg.hook_registry = keel_hook_registry_create();
                if (engine_cfg.hook_registry) {
                    keel_throttle_rules_register_hook(engine_cfg.hook_registry, tr);
                    wg->hook_registry = engine_cfg.hook_registry;
                }
                wg->throttle_rules = tr;
            }
        }

        /* Start background topology discovery */
        if (wg->router) {
            keel_discovery_config_t disc_cfg = keel_discovery_config_default();
            keel_discovery_t* disc = keel_discovery_create(&disc_cfg);
            if (disc) {
                if (keel_discovery_start(disc, wg->router) == KEEL_OK) {
                    wg->discovery = disc;
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "worker group %zu: topology discovery started", gi);
                } else {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                        "worker group %zu: topology discovery start failed", gi);
                    keel_discovery_destroy(disc);
                }
            }
        }

        engine_cfg.use_buf_rings     = wg->use_buf_rings;
        engine_cfg.buf_ring_size     = wg->buf_ring_size;
        engine_cfg.sqpoll            = wg->sqpoll;
        engine_cfg.sqpoll_idle_ms    = wg->sqpoll_idle_ms;
        engine_cfg.ps_mode           = wg->ps_mode;
        engine_cfg.runtime_mode      = wg->runtime_mode;
        engine_cfg.txn_tracking      = wg->txn_tracking;
        engine_cfg.fast_network_path = wg->fast_network_path;
        engine_cfg.result_cache      = wg->result_cache;
        engine_cfg.sticky_primary_ttl_ms        = wg->sticky_primary_ttl_ms;
        engine_cfg.scatter_merge_max_mem_bytes  = wg->scatter_merge_max_mem_bytes;
        engine_cfg.scatter_merge_spill_dir      =
            wg->scatter_merge_spill_dir_buf[0] ? wg->scatter_merge_spill_dir_buf : NULL;
        engine_cfg.rebalance_enabled       = wg->rebalance_enabled;
        engine_cfg.rebalance_interval_ms   = wg->rebalance_interval_ms;
        engine_cfg.rebalance_threshold_pct = wg->rebalance_threshold_pct;
        engine_cfg.rebalance_max_per_tick  = wg->rebalance_max_per_tick;
        engine_cfg.tls_config              = wg->tls_config;
        engine_cfg.backend_tls_config      = wg->backend_tls_config;
        engine_cfg.trace_config            = g_trace_cfg;

        /* Enterprise auth wiring */
        engine_cfg.auth_method              = wg->auth_method;
        engine_cfg.auth_ldap_url            = wg->auth_ldap_url_buf[0]    ? wg->auth_ldap_url_buf    : NULL;
        engine_cfg.auth_ldap_base_dn        = wg->auth_ldap_base_dn_buf[0]? wg->auth_ldap_base_dn_buf: NULL;
        engine_cfg.auth_ldap_bind_dn        = wg->auth_ldap_bind_dn_buf[0]? wg->auth_ldap_bind_dn_buf: NULL;
        engine_cfg.auth_ldap_bind_password  = wg->auth_ldap_bind_password_buf[0]  ? wg->auth_ldap_bind_password_buf  : NULL;
        engine_cfg.auth_ldap_search_filter  = wg->auth_ldap_search_filter_buf[0]  ? wg->auth_ldap_search_filter_buf  : NULL;
        engine_cfg.auth_ldap_dn_suffix      = wg->auth_ldap_dn_suffix_buf[0]      ? wg->auth_ldap_dn_suffix_buf      : NULL;
        engine_cfg.auth_ldap_start_tls      = wg->auth_ldap_start_tls;
        engine_cfg.auth_ldap_timeout_s      = wg->auth_ldap_timeout_s;
        engine_cfg.auth_pam_service_name    = wg->auth_pam_service_buf[0]  ? wg->auth_pam_service_buf  : NULL;
        engine_cfg.auth_query               = wg->auth_query_buf[0]        ? wg->auth_query_buf        : NULL;
        engine_cfg.auth_query_conn_string   = wg->auth_query_conn_buf[0]   ? wg->auth_query_conn_buf   : NULL;
        engine_cfg.auth_userlist_file       = wg->auth_userlist_file_buf[0]? wg->auth_userlist_file_buf: NULL;

        engine_cfg.pool_max_connection_age_ms = wg->pool_max_connection_age_ms;

        /* Auto-generate TLS certificates if requested */
        if (wg->tls_auto_generate) {
            keel_tls_auto_config_t auto_cfg = {
                .output_dir    = wg->tls_auto_dir_buf,
                .key_bits      = 2048,
                .validity_days = 365,
                .cn            = "Keel CA",
                .server_cn     = "keel-server",
                .server_san    = "localhost,127.0.0.1,::1",
            };
            if (keel_tls_auto_generate(&auto_cfg, &wg->tls_auto_result) != 0) {
                KEEL_LOG_FATAL(KEEL_LOG_CAT_TLS,
                    "[%s] Failed to auto-generate TLS certificates in %s",
                    wg->name, wg->tls_auto_dir_buf);
                cleanup_startup_failure(config, tls_initialized);
                return 1;
            }
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                "[%s] Auto-generated TLS certificates in %s", wg->name, wg->tls_auto_dir_buf);
            if (!engine_cfg.tls_config.cert_file) engine_cfg.tls_config.cert_file = wg->tls_auto_result.server_cert;
            if (!engine_cfg.tls_config.key_file)  engine_cfg.tls_config.key_file  = wg->tls_auto_result.server_key;
            if (!engine_cfg.tls_config.ca_file)   engine_cfg.tls_config.ca_file   = wg->tls_auto_result.ca_cert;
            if (engine_cfg.tls_config.mode == KEEL_TLS_DISABLE) {
                engine_cfg.tls_config.mode = KEEL_TLS_PREFER;
                KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "[%s] TLS mode upgraded to 'prefer' (auto-generated certs)", wg->name);
            }
        }

        /* Create shard router when the server pool has sharded servers */
        {
            bool has_shard_ids = false;
            for (size_t si = 0; si < wg->server_pool.count; si++) {
                if (wg->server_pool.servers[si].shard_id != 0) { has_shard_ids = true; break; }
            }
            if (has_shard_ids) {
                keel_router_config_t rcfg = keel_router_config_default();
                rcfg.scatter_merge_enabled = wg->scatter_merge_enabled;
                rcfg.failover = wg->failover_cfg;
                wg->router = keel_router_create(&rcfg);
                if (wg->router) {
                    for (size_t si = 0; si < wg->server_pool.count; si++) {
                        const keel_backend_server_t* srv = &wg->server_pool.servers[si];
                        char srv_name[64];
                        snprintf(srv_name, sizeof(srv_name), "shard%u", srv->shard_id);
                        keel_route_server_t rs = {
                            .name     = srv_name,
                            .host     = srv->host,
                            .port     = srv->port,
                            .role     = srv->role,
                            .weight   = (int)(srv->weight > 0 ? srv->weight : 100),
                            .shard_id = (size_t)srv->shard_id,
                            .health   = KEEL_HEALTH_UP,
                        };
                        keel_router_add_server(wg->router, &rs);
                    }
                    keel_reload_result_t rr = {0};
                    if (config) {
                        keel_config_reload_shard_rules(config, wg->name, wg->router, &rr);
                        if (rr.applied > 0)
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "[%s] Loaded %d shard rule(s) into router", wg->name, rr.applied);
                    }
                    engine_cfg.router = wg->router;

                    keel_router_mgr_config_t mgr_cfg = keel_router_mgr_config_default();
                    wg->router_mgr = keel_router_mgr_create(&mgr_cfg, wg->router);
                    if (!wg->router_mgr)
                        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "[%s] router plugin manager creation failed", wg->name);
                    else
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[%s] router plugin manager created", wg->name);
                }
            }
        }

        wg->engine = keel_engine_create(&engine_cfg);
        if (!wg->engine) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "[%s] Failed to create engine", wg->name);
            cleanup_startup_failure(config, tls_initialized);
            return 1;
        }

        keel_engine_set_drain_timeout(wg->engine, g_config.shutdown_timeout_ms);

        {
            keel_stats_collector_t* sc = keel_engine_get_stats_collector(wg->engine);
            if (sc) {
                uint32_t mask = sys_instr_mask;
                keel_stats_set_system_probe_mask(sc, mask);
                KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                    "[%s] System instrumentation mask: CPU=%s MEM=%s FD=%s DISK=%s NET=%s",
                    wg->name,
                    (mask & KEEL_STAT_SYS_CPU)     ? "on" : "off",
                    (mask & KEEL_STAT_SYS_MEMORY)  ? "on" : "off",
                    (mask & KEEL_STAT_SYS_FD)      ? "on" : "off",
                    (mask & KEEL_STAT_SYS_DISK)    ? "on" : "off",
                    (mask & KEEL_STAT_SYS_NETWORK) ? "on" : "off");
            }
        }

        if (gi == 0) {
            keel_reactor_type_t rt = keel_engine_get_reactor_type(wg->engine);
            printf("  Reactor:  %s\n",
                   rt == KEEL_REACTOR_IOURING ? "io_uring" :
                   rt == KEEL_REACTOR_KQUEUE  ? "kqueue"   :
                   rt == KEEL_REACTOR_EPOLL   ? "epoll"    : "unknown");
        }
    } /* end for each worker group */

    /* Keep g_engine pointing at the first engine for stats/admin */
    g_engine = g_groups[0].engine;

    /* Create tracer (shared across all worker groups) */
    if (g_trace_cfg.enabled) {
        uint32_t total_workers = 0;
        for (size_t gi = 0; gi < g_num_groups; gi++)
            total_workers += keel_engine_get_num_workers(g_groups[gi].engine);
        g_tracer = keel_tracer_create(&g_trace_cfg, total_workers);
        if (g_tracer) {
            for (size_t gi = 0; gi < g_num_groups; gi++)
                keel_engine_set_tracer(g_groups[gi].engine, g_tracer);
            KEEL_LOG_INFO(KEEL_LOG_CAT_TRACE,
                "Tracing enabled: endpoint=%s sample_rate=%u ppm workers=%u",
                g_trace_cfg.endpoint, g_trace_cfg.sample_rate_ppm, total_workers);
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TRACE, "Failed to create tracer");
        }
    }

    /* Initialise audit log */
    if (keel_audit_log_init_from_config(&g_audit_log, config) == 0) {
        g_audit_log_initialized = true;
        if (g_audit_log.enabled) {
            for (size_t gi = 0; gi < g_num_groups; gi++)
                keel_engine_set_audit_log(g_groups[gi].engine,
                                         (struct keel_audit_log*)&g_audit_log);
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "Audit log enabled: path=%s format=%s",
                g_audit_log.config.path,
                g_audit_log.config.format == KEEL_AUDIT_FORMAT_NDJSON ? "ndjson" : "text");
        }
    } else {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to initialise audit log");
    }

    /* Print "Ready" banner */
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║   Ready to accept connections                                  ║\n");
    printf("╚═══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];
        bool is_mysql = (strcmp(wg->default_protocol, "mysql") == 0);
        if (is_mysql)
            printf("  Test with: mysql -h 127.0.0.1 -P %d -u <user> -p <database>\n", wg->listen_port);
        else
            printf("  Test with: psql -h 127.0.0.1 -p %d -U <user> -d <database>\n", wg->listen_port);
    }
    printf("  Dump stats: kill -USR1 <pid>\n");

    /* Start admin console + Prometheus */
    if (g_admin_cfg.admin_enabled || g_admin_cfg.prom_enabled) {
        g_admin = keel_admin_start(&g_admin_cfg, g_engine);
        if (g_admin) {
            if (g_admin_cfg.admin_enabled)
                printf("  Admin:    psql -h %s -p %u -U admin -c 'SHOW STATS'\n",
                       g_admin_cfg.admin_addr, g_admin_cfg.admin_port);
            if (g_admin_cfg.prom_enabled)
                printf("  Metrics:  curl http://%s:%u/metrics\n",
                       g_admin_cfg.prom_addr, g_admin_cfg.prom_port);
            if (g_config.query_rules)
                keel_admin_set_query_rules(g_admin, g_config.query_rules);
            for (size_t gi = 0; gi < g_num_groups; gi++) {
                if (g_groups[gi].throttle_rules)
                    keel_admin_set_throttle_rules(g_admin, g_groups[gi].throttle_rules);
                if (g_groups[gi].discovery)
                    keel_admin_set_discovery(g_admin, g_groups[gi].discovery);
            }
            for (size_t gi = 0; gi < g_num_groups; gi++) {
                if (g_groups[gi].router) {
                    keel_admin_set_router(g_admin, g_groups[gi].router);
                    break;
                }
            }
        }
    }

#ifdef KEEL_HAS_OTLP
    if (g_otlp_enabled && g_engine) {
        g_otlp_exporter = keel_otlp_exporter_create(&g_otlp_cfg);
        if (g_otlp_exporter && keel_otlp_exporter_start(g_otlp_exporter) == 0) {
            keel_stats_collector_t* coll = keel_engine_get_stats_collector(g_engine);
            if (coll) {
                g_otlp_agg = keel_otlp_aggregator_create(coll, g_otlp_exporter, g_otlp_cfg.interval_ms);
                if (g_otlp_agg && keel_otlp_aggregator_start(g_otlp_agg) == 0) {
                    keel_admin_set_otlp_exporter(g_admin, g_otlp_exporter);
                    printf("  OTLP:     %s (interval=%ums)\n",
                           g_otlp_cfg.http.endpoint_url, g_otlp_cfg.interval_ms);
                } else {
                    fprintf(stderr, "Warning: failed to start OTLP aggregator\n");
                    if (g_otlp_agg) { keel_otlp_aggregator_destroy(g_otlp_agg); g_otlp_agg = NULL; }
                    keel_otlp_exporter_stop(g_otlp_exporter);
                    keel_otlp_exporter_destroy(g_otlp_exporter);
                    g_otlp_exporter = NULL;
                }
            } else {
                fprintf(stderr, "Warning: stats collector unavailable; disabling OTLP\n");
                keel_otlp_exporter_stop(g_otlp_exporter);
                keel_otlp_exporter_destroy(g_otlp_exporter);
                g_otlp_exporter = NULL;
            }
        } else {
            fprintf(stderr, "Warning: failed to start OTLP exporter\n");
            if (g_otlp_exporter) { keel_otlp_exporter_destroy(g_otlp_exporter); g_otlp_exporter = NULL; }
        }
    }
#endif

    /* Start cluster manager */
    if (g_cluster_cfg.enabled) {
        g_cluster = keel_cluster_create(&g_cluster_cfg);
        if (g_cluster) {
            if (g_engine)
                cluster_setup_callbacks(g_cluster, g_engine, g_groups);
            if (keel_cluster_start(g_cluster) == 0) {
                printf("  Cluster:  %s:%u (node=%s, peers=%zu)\n",
                       g_cluster_cfg.listen_addr, g_cluster_cfg.listen_port,
                       g_cluster_cfg.node_id, g_cluster_cfg.initial_peer_count);
                cluster_setup_callbacks(g_cluster, g_engine, g_groups);
                if (g_admin)
                    keel_admin_set_cluster(g_admin, g_cluster);
            } else {
                fprintf(stderr, "Warning: failed to start cluster manager\n");
                keel_cluster_destroy(g_cluster);
                g_cluster = NULL;
            }
        }
    }

    /* Create probe managers */
    {
        bool probes_registered = false;
        for (size_t gi = 0; gi < g_num_groups; gi++) {
            worker_group_t* wg = &g_groups[gi];
            if (wg->server_pool.count == 0) continue;
            if (!probes_registered) {
                keel_probe_register_builtins();
                probes_registered = true;
            }
            wg->probe_mgr = keel_probe_manager_create(
                &wg->probe_cfg,
                keel_engine_get_server_pool(wg->engine),
                wg->engine);
            if (wg->probe_mgr) {
                printf("  [%s] Probe: type=%s interval=%ums timeout=%ums retries=%u\n",
                       wg->name, wg->probe_cfg.probe_type,
                       wg->probe_cfg.interval_ms, wg->probe_cfg.timeout_ms,
                       wg->probe_cfg.retries);
            }

            if (wg->router) {
                keel_failover_manager_config_t fmcfg = {
                    .detection_interval_ms = wg->failover_cfg.detection_interval_ms
                                              ? wg->failover_cfg.detection_interval_ms : 500,
                    .failure_threshold     = wg->failover_cfg.failure_threshold
                                              ? wg->failover_cfg.failure_threshold : 3,
                    .promotion_grace_ms    = wg->failover_cfg.promotion_grace_ms
                                              ? wg->failover_cfg.promotion_grace_ms : 3000,
                    .old_primary_fencing   = wg->failover_cfg.old_primary_fencing_required,
                };
                wg->failover_mgr = keel_failover_manager_create(
                    &fmcfg,
                    wg->probe_mgr,
                    keel_engine_get_server_pool(wg->engine),
                    wg->engine,
                    wg->router);
                if (wg->failover_mgr)
                    printf("  [%s] Failover-mgr: interval=%ums threshold=%u grace=%ums\n",
                           wg->name, fmcfg.detection_interval_ms,
                           fmcfg.failure_threshold, fmcfg.promotion_grace_ms);
            }
        }
    }

    printf("  Press Ctrl+C to stop\n");
    printf("\n");
    fflush(stdout);

    /* Apply runtime security hardening */
    if (apply_runtime_security_policy() < 0) {
        KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "security: failed to apply runtime security policy");
        cleanup_startup_failure(config, tls_initialized);
        return 1;
    }

    /* ====================================================================
     * Phase 2: Start engines (spawns worker threads) and probe threads.
     * ==================================================================== */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];
        if (keel_engine_start(wg->engine, wg->listen_fd) < 0) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "[%s] Failed to start engine", wg->name);
            keel_engine_destroy(wg->engine);
            wg->engine = NULL;
            close(wg->listen_fd);
            wg->listen_fd = -1;
            cleanup_startup_failure(config, tls_initialized);
            return 1;
        }
    }

    /* Start probe threads */
    for (size_t gi = 0; gi < g_num_groups; gi++)
        if (g_groups[gi].probe_mgr)
            keel_probe_manager_start(g_groups[gi].probe_mgr);

    /* Start failover-manager detector threads */
    for (size_t gi = 0; gi < g_num_groups; gi++)
        if (g_groups[gi].failover_mgr)
            keel_failover_manager_start(g_groups[gi].failover_mgr);

    /* Register stats dump callback on first engine */
    keel_engine_set_periodic_callback(g_engine, (void(*)(void*))stats_dump, NULL);

    /* ====================================================================
     * Main Loop — wait for signals.
     * ==================================================================== */
    {
        sigset_t sigset;
        sigemptyset(&sigset);
        sigaddset(&sigset, SIGINT);
        sigaddset(&sigset, SIGTERM);
        sigaddset(&sigset, SIGUSR1);
        sigaddset(&sigset, SIGHUP);
        pthread_sigmask(SIG_BLOCK, &sigset, NULL);

        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: entering main loop");

        uint32_t interval_ms = g_config.stats_interval_ms;

        while (!g_should_stop) {
            int sig;

            if (interval_ms > 0) {
                struct timespec ts = {
                    .tv_sec  = interval_ms / 1000,
                    .tv_nsec = (long)(interval_ms % 1000) * 1000000L,
                };
                sig = keel_sigtimedwait(&sigset, &ts);
                if (sig < 0) {
                    if (errno == EAGAIN) { stats_dump(); continue; }
                    continue;
                }
            } else {
                if (sigwait(&sigset, &sig) != 0) continue;
            }

            if (sig == SIGUSR1) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_STATS, "engine: SIGUSR1 received, stats dump requested");
                stats_dump();
                keel_pool_dump_active_allocations();
                continue;
            }

            if (sig == SIGHUP) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: SIGHUP received, reloading configuration");

                /* Reload TLS certificates */
                for (size_t gi = 0; gi < g_num_groups; gi++) {
                    worker_group_t* wg = &g_groups[gi];
                    keel_error_t rc = keel_tls_reload_certs(
                        wg->tls_config.mode != KEEL_TLS_DISABLE ? &wg->tls_config : NULL,
                        wg->backend_tls_config.mode != KEEL_TLS_DISABLE ? &wg->backend_tls_config : NULL);
                    if (rc == KEEL_OK)
                        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "[%s] TLS certificates reloaded", wg->name);
                    else if (wg->tls_config.mode != KEEL_TLS_DISABLE ||
                             wg->backend_tls_config.mode != KEEL_TLS_DISABLE)
                        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "[%s] TLS certificate reload failed", wg->name);
                }

                /* Re-parse config and apply live-tunable parameters */
                if (g_config.config_file) {
                    keel_config_t* reload_cfg = keel_config_load(g_config.config_file);
                    if (reload_cfg) {
                        const char* new_level = keel_config_get_string(reload_cfg, "logging", "log_level", NULL);
                        if (new_level) {
                            int lvl = keel_log_level_from_string(new_level);
                            if (lvl >= 0) {
                                keel_log_set_level(lvl);
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Log level changed to: %s", new_level);
                            }
                        }

                        int total_applied = 0;
                        for (size_t gi = 0; gi < g_num_groups; gi++) {
                            int n = reload_worker_group(reload_cfg, &g_groups[gi]);
                            total_applied += n;
                        }
                        if (total_applied > 0)
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "Live reload: %d parameter(s) applied across %zu group(s)",
                                total_applied, g_num_groups);

                        for (size_t gi = 0; gi < g_num_groups; gi++) {
                            worker_group_t* wg = &g_groups[gi];
                            if (!wg->router) continue;
                            keel_reload_result_t rr = {0};
                            keel_config_reload_shard_rules(reload_cfg, wg->name, wg->router, &rr);
                            if (rr.applied > 0 || rr.errors > 0)
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "[%s] Shard rules reloaded: applied=%d skipped=%d unchanged=%d errors=%d",
                                    wg->name, rr.applied, rr.skipped, rr.unchanged, rr.errors);
                        }

                        {
                            keel_query_rules_t* new_qr = NULL;
                            if (keel_query_rules_load(reload_cfg, &new_qr) == KEEL_OK) {
                                size_t old_count = g_config.query_rules ? g_config.query_rules->count : 0;
                                keel_query_rules_replace(&g_config.query_rules, new_qr);
                                if (g_admin) keel_admin_set_query_rules(g_admin, g_config.query_rules);
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "Query rules reloaded: %zu rule(s) (was %zu)",
                                    new_qr ? new_qr->count : 0, old_count);
                            } else {
                                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                    "Query rules reload failed; previous rules still active");
                            }
                        }

                        for (size_t gi = 0; gi < g_num_groups; gi++) {
                            worker_group_t* wg = &g_groups[gi];
                            keel_throttle_rules_t* new_tr = NULL;
                            if (keel_throttle_rules_load(reload_cfg, &new_tr) == KEEL_OK) {
                                keel_throttle_rules_replace(&wg->throttle_rules, new_tr);
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[%s] Throttle rules reloaded", wg->name);
                            }
                        }

                        keel_config_free(reload_cfg);
                    }
                }

                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Configuration reload complete");
                continue;
            }

            /* SIGINT / SIGTERM */
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                         "engine: received signal %d, initiating shutdown", sig);
            g_should_stop = 1;
        }
    }

    /* ====================================================================
     * Shutdown
     * ==================================================================== */
    printf("\n");
    printf("Shutting down...\n");

    /* Phase 1: Drain */
    for (size_t gi = 0; gi < g_num_groups; gi++)
        if (g_groups[gi].engine)
            keel_engine_drain(g_groups[gi].engine);

    /* Phase 2: Stop engines */
    for (size_t gi = 0; gi < g_num_groups; gi++)
        if (g_groups[gi].engine)
            keel_engine_stop(g_groups[gi].engine);

    /* Print final statistics */
    {
        uint64_t total_conns  = keel_engine_get_total_connections(g_engine);
        uint64_t active_conns = keel_engine_get_active_connections(g_engine);
        printf("\n");
        printf("Final Statistics:\n");
        printf("  Total connections:  %llu\n", (unsigned long long)total_conns);
        printf("  Active connections: %llu\n", (unsigned long long)active_conns);
    }

    stats_dump();

#ifdef KEEL_HAS_OTLP
    if (g_otlp_agg) { keel_otlp_aggregator_stop(g_otlp_agg); keel_otlp_aggregator_destroy(g_otlp_agg); g_otlp_agg = NULL; }
    if (g_otlp_exporter) {
        keel_admin_set_otlp_exporter(g_admin, NULL);
        keel_otlp_exporter_stop(g_otlp_exporter);
        keel_otlp_exporter_destroy(g_otlp_exporter);
        g_otlp_exporter = NULL;
    }
#endif

    keel_admin_stop(g_admin); g_admin = NULL;

    if (g_cluster) { keel_cluster_destroy(g_cluster); g_cluster = NULL; }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].failover_mgr) { keel_failover_manager_destroy(g_groups[gi].failover_mgr); g_groups[gi].failover_mgr = NULL; }
    }
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        keel_probe_manager_destroy(g_groups[gi].probe_mgr);
        g_groups[gi].probe_mgr = NULL;
    }
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].discovery) {
            keel_discovery_stop(g_groups[gi].discovery);
            keel_discovery_destroy(g_groups[gi].discovery);
            g_groups[gi].discovery = NULL;
        }
    }
    for (size_t gi = 0; gi < g_num_groups; gi++)
        if (g_groups[gi].throttle_rules)
            keel_throttle_rules_replace(&g_groups[gi].throttle_rules, NULL);

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].router_mgr) { keel_router_mgr_destroy(g_groups[gi].router_mgr); g_groups[gi].router_mgr = NULL; }
    }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        keel_engine_destroy(g_groups[gi].engine);
        g_groups[gi].engine = NULL;
    }
    g_engine = NULL;

    if (g_tracer) { keel_tracer_destroy(g_tracer); g_tracer = NULL; }
    if (g_audit_log_initialized) { keel_audit_log_close(&g_audit_log); g_audit_log_initialized = false; }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].hook_registry) {
            keel_hook_registry_destroy(g_groups[gi].hook_registry);
            g_groups[gi].hook_registry = NULL;
        }
    }

    keel_config_free(config);
    config = NULL;

    keel_lua_shutdown();
    keel_python_shutdown();

    keel_query_log_shutdown(&g_query_log);
    if (g_log_plugin) {
        g_log_plugin->flush(g_log_plugin);
        g_log_plugin->close(g_log_plugin);
        g_log_plugin->destroy(g_log_plugin);
        g_log_plugin = NULL;
    }

    keel_mem_shutdown();
    keel_tls_cleanup();

    printf("Goodbye!\n");
    return 0;
}
