/**
 * @file stats_display.c
 * @brief Stats snapshot printer, instrumentation mask builders, and
 *        boolean environment-variable helpers.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Extracted from src/main/main.c — no logic changes, only relocation.
 */

#include "keel/main/stats_display.h"

#include "keel/engine/engine.h"
#include "keel/core/stats.h"
#include "keel/core/ini.h"
#include "keel/log/log.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

/* g_engine is defined in config_load.c */
extern struct keel_engine* g_engine;

/* ============================================================================
 * Boolean / Environment Helpers
 * ============================================================================ */

bool env_enabled_default_true(const char *name)
{
    const char *v = getenv(name);
    if (!v || !*v) return true;
    if (strcasecmp(v, "0") == 0 ||
        strcasecmp(v, "false") == 0 ||
        strcasecmp(v, "off") == 0 ||
        strcasecmp(v, "no") == 0) {
        return false;
    }
    return true;
}

bool env_enabled_default_false(const char *name)
{
    const char *v = getenv(name);
    if (!v || !*v) return false;
    if (strcasecmp(v, "1") == 0 ||
        strcasecmp(v, "true") == 0 ||
        strcasecmp(v, "on") == 0 ||
        strcasecmp(v, "yes") == 0) {
        return true;
    }
    return false;
}

bool parse_bool_string(const char *v, bool *out)
{
    if (!v || !*v || !out)
        return false;

    if (strcasecmp(v, "1") == 0 ||
        strcasecmp(v, "true") == 0 ||
        strcasecmp(v, "on") == 0 ||
        strcasecmp(v, "yes") == 0) {
        *out = true;
        return true;
    }

    if (strcasecmp(v, "0") == 0 ||
        strcasecmp(v, "false") == 0 ||
        strcasecmp(v, "off") == 0 ||
        strcasecmp(v, "no") == 0) {
        *out = false;
        return true;
    }

    return false;
}

/* ============================================================================
 * Instrumentation Mask Builders
 * ============================================================================ */

uint32_t build_system_instr_mask_from_env(void)
{
    bool default_all = env_enabled_default_true("KEEL_INSTR_ALL");

    uint32_t mask = 0;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_CPU")
                    : env_enabled_default_false("KEEL_INSTR_CPU"))
        mask |= KEEL_STAT_SYS_CPU;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_MEMORY")
                    : env_enabled_default_false("KEEL_INSTR_MEMORY"))
        mask |= KEEL_STAT_SYS_MEMORY;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_FD")
                    : env_enabled_default_false("KEEL_INSTR_FD"))
        mask |= KEEL_STAT_SYS_FD;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_DISK")
                    : env_enabled_default_false("KEEL_INSTR_DISK"))
        mask |= KEEL_STAT_SYS_DISK;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_NETWORK")
                    : env_enabled_default_false("KEEL_INSTR_NETWORK"))
        mask |= KEEL_STAT_SYS_NETWORK;

    return mask;
}

uint32_t build_hotpath_instr_mask_from_env(void)
{
    bool default_all = env_enabled_default_true("KEEL_HOT_INSTR_ALL");

    uint32_t mask = 0;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_WAIT_POOL")
                    : env_enabled_default_false("KEEL_HOT_INSTR_WAIT_POOL"))
        mask |= KEEL_HOT_INSTR_WAIT_POOL;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_WAIT_BACKEND")
                    : env_enabled_default_false("KEEL_HOT_INSTR_WAIT_BACKEND"))
        mask |= KEEL_HOT_INSTR_WAIT_BACKEND;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT")
                    : env_enabled_default_false("KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT"))
        mask |= KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_DEFERRED_SEND")
                    : env_enabled_default_false("KEEL_HOT_INSTR_DEFERRED_SEND"))
        mask |= KEEL_HOT_INSTR_DEFERRED_SEND;

    return mask;
}

uint32_t apply_hotpath_instr_mask_from_config(const keel_config_t *cfg,
                                              uint32_t current_mask)
{
    if (!cfg || !keel_config_has_section(cfg, "stats"))
        return current_mask;

    uint32_t mask = current_mask;
    bool enabled = false;

    const char *v = keel_config_get_string(cfg, "stats", "hotpath_all", NULL);
    if (parse_bool_string(v, &enabled)) {
        mask = enabled ? KEEL_HOT_INSTR_ALL : 0;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_wait_pool", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_WAIT_POOL;
        else mask &= ~KEEL_HOT_INSTR_WAIT_POOL;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_wait_backend", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_WAIT_BACKEND;
        else mask &= ~KEEL_HOT_INSTR_WAIT_BACKEND;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_wait_backend_query_split", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT;
        else mask &= ~KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_deferred_send", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_DEFERRED_SEND;
        else mask &= ~KEEL_HOT_INSTR_DEFERRED_SEND;
    }

    return mask;
}

uint32_t apply_instr_mask_from_config(const keel_config_t *cfg,
                                      uint32_t current_mask)
{
    if (!cfg || !keel_config_has_section(cfg, "instrument"))
        return current_mask;

    uint32_t mask = current_mask;
    bool enabled = false;

    /* Master switch */
    const char *v = keel_config_get_string(cfg, "instrument", "enabled", NULL);
    if (parse_bool_string(v, &enabled)) {
        mask = enabled ? KEEL_INSTR_CAT_ALL : KEEL_INSTR_CAT_NONE;
    }

    /* Per-category overrides */
    static const struct { const char *key; uint32_t bit; } cats[] = {
        { "cat_engine", KEEL_INSTR_CAT_ENGINE },
        { "cat_pool",   KEEL_INSTR_CAT_POOL },
        { "cat_proto",  KEEL_INSTR_CAT_PROTO },
        { "cat_io",     KEEL_INSTR_CAT_IO },
        { "cat_hook",   KEEL_INSTR_CAT_HOOK },
        { "cat_route",  KEEL_INSTR_CAT_ROUTE },
        { "cat_ps",     KEEL_INSTR_CAT_PS },
        { "cat_state",  KEEL_INSTR_CAT_STATE },
    };

    for (size_t i = 0; i < sizeof(cats) / sizeof(cats[0]); i++) {
        v = keel_config_get_string(cfg, "instrument", cats[i].key, NULL);
        if (parse_bool_string(v, &enabled)) {
            if (enabled) mask |= cats[i].bit;
            else mask &= ~cats[i].bit;
        }
    }

    return mask;
}

/* ============================================================================
 * Stats Dump
 * ============================================================================ */

void stats_dump(void) {
    if (!g_engine) return;

    keel_stats_collector_t *sc = keel_engine_get_stats_collector(g_engine);
    if (!sc) return;

    /* Refresh system sample immediately before snapshot on demand/interval. */
    keel_stats_sample_system(sc);

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(sc, &snap);

    double uptime_s = (double)snap.uptime_ns / 1.0e9;

    printf("\n");
    printf("╔═══════════════════════ KEEL Stats ════════════════════════════╗\n");
    printf("║ Level: %-10s  Uptime: %.1fs  Workers: %zu              \n",
           keel_stats_level_to_str(snap.level), uptime_s, snap.num_workers);
    printf("╠══════════════════════════════════════════════════════════════╣\n");

    if (snap.level >= KEEL_STATS_BASIC) {
        printf("║ Sessions:  created=%-8llu  closed=%-8llu  active=%-6lld  peak=%-6llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.sessions_created),
               (unsigned long long)keel_counter_get(&snap.basic.sessions_closed),
               (long long)keel_gauge_get(&snap.basic.sessions_active),
               (unsigned long long)snap.basic.sessions_peak);
        printf("║ Pool:      borrow=%-8llu  return=%-8llu  hit=%-8llu  miss=%-7llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.pool_borrows),
               (unsigned long long)keel_counter_get(&snap.basic.pool_returns),
               (unsigned long long)keel_counter_get(&snap.basic.pool_hits),
               (unsigned long long)keel_counter_get(&snap.basic.pool_misses));
        printf("║ Pool:      create=%-8llu  destroy=%-7llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.pool_creates),
               (unsigned long long)keel_counter_get(&snap.basic.pool_destroys));
        printf("║ Queries:   total=%-9llu  read=%-9llu  write=%-8llu  tx=%-8llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.queries_total),
               (unsigned long long)keel_counter_get(&snap.basic.queries_read),
               (unsigned long long)keel_counter_get(&snap.basic.queries_write),
               (unsigned long long)keel_counter_get(&snap.basic.queries_tx));
        printf("║ Errors:    total=%-9llu  auth=%-9llu  proto=%-8llu  backend=%-5llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.errors_total),
               (unsigned long long)keel_counter_get(&snap.basic.errors_auth),
               (unsigned long long)keel_counter_get(&snap.basic.errors_proto),
               (unsigned long long)keel_counter_get(&snap.basic.errors_backend));
        printf("║ I/O:       recv=%-10llu  sent=%-10llu  spliced=%-8llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.bytes_recv),
               (unsigned long long)keel_counter_get(&snap.basic.bytes_sent),
               (unsigned long long)keel_counter_get(&snap.basic.bytes_spliced));
        printf("║ Reactor:   loops=%-9llu  submit=%-8llu  complete=%-7llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.loop_iterations),
               (unsigned long long)keel_counter_get(&snap.basic.ops_submitted),
               (unsigned long long)keel_counter_get(&snap.basic.ops_completed));
        {
            uint64_t pool_ev = keel_counter_get(&snap.basic.flow_wait_pool_events);
            uint64_t pool_ns = keel_counter_get(&snap.basic.flow_wait_pool_ns_total);
            uint64_t be_ev   = keel_counter_get(&snap.basic.flow_wait_backend_events);
            uint64_t be_ns   = keel_counter_get(&snap.basic.flow_wait_backend_ns_total);
            printf("║ FlowWait:  pool_ev=%-7llu pool_ms=%-10.2f be_ev=%-7llu be_ms=%-10.2f\n",
                (unsigned long long)pool_ev,
                (double)pool_ns / 1000000.0,
                (unsigned long long)be_ev,
                (double)be_ns / 1000000.0);
        }
    }

    if (snap.level >= KEEL_STATS_EXTENDED) {
        /* Show p50/p95/p99 latencies */
        uint64_t qp50 = keel_histogram_percentile(&snap.extended.query_latency_ns, 0.50);
        uint64_t qp95 = keel_histogram_percentile(&snap.extended.query_latency_ns, 0.95);
        uint64_t qp99 = keel_histogram_percentile(&snap.extended.query_latency_ns, 0.99);
        uint64_t bp50 = keel_histogram_percentile(&snap.extended.backend_latency_ns, 0.50);
        uint64_t bp95 = keel_histogram_percentile(&snap.extended.backend_latency_ns, 0.95);
        uint64_t bp99 = keel_histogram_percentile(&snap.extended.backend_latency_ns, 0.99);
        printf("╠══════════════════════════════════════════════════════════════╣\n");
        printf("║ Query latency (ns):   p50=%-10llu  p95=%-10llu  p99=%-10llu\n",
               (unsigned long long)qp50, (unsigned long long)qp95, (unsigned long long)qp99);
        printf("║ Backend latency (ns): p50=%-10llu  p95=%-10llu  p99=%-10llu\n",
               (unsigned long long)bp50, (unsigned long long)bp95, (unsigned long long)bp99);
    }

    if (snap.level >= KEEL_STATS_SYSTEM) {
        uint32_t m = snap.system.probe_mask;
        printf("╠══════════════════════════════════════════════════════════════╣\n");
        if (m & KEEL_STAT_SYS_CPU) {
            printf("║ CPU:  user=%.1f%%  sys=%.1f%%\n",
                snap.system.cpu_user_pct, snap.system.cpu_sys_pct);
            printf("║ Ctx switches:  voluntary=%llu  involuntary=%llu\n",
                (unsigned long long)snap.system.ctx_switches_vol,
                (unsigned long long)snap.system.ctx_switches_inv);
        }
        if (m & KEEL_STAT_SYS_MEMORY) {
            printf("║ Memory: RSS=%.1f MB  VM=%.1f MB\n",
                (double)snap.system.rss_bytes / (1024.0 * 1024.0),
                (double)snap.system.vm_bytes  / (1024.0 * 1024.0));
        }
        if (m & KEEL_STAT_SYS_FD) {
            printf("║ FDs: open=%u  limit=%u\n",
                snap.system.fd_open, snap.system.fd_limit);
        }
        if (m & KEEL_STAT_SYS_DISK) {
            printf("║ Disk IO: read=%llu B  write=%llu B\n",
                (unsigned long long)snap.system.disk_read_bytes,
                (unsigned long long)snap.system.disk_write_bytes);
        }
        if (m & KEEL_STAT_SYS_NETWORK) {
            printf("║ Net IO: rx=%llu B  tx=%llu B\n",
                (unsigned long long)snap.system.net_rx_bytes,
                (unsigned long long)snap.system.net_tx_bytes);
        }
    }

    /* Function-level instrumentation probes */
    {
        bool any_active = false;
        for (int p = 0; p < KEEL_INSTR__COUNT; p++) {
            if (snap.instr.probes[p].call_count > 0) {
                any_active = true;
                break;
            }
        }
        if (any_active) {
            printf("╠══════════════════════════════════════════════════════════════╣\n");
            printf("║ Instrumentation Probes:                                     \n");
            for (int p = 0; p < KEEL_INSTR__COUNT; p++) {
                const keel_instr_probe_t *pr = &snap.instr.probes[p];
                if (pr->call_count == 0) continue;
                uint64_t avg_ns = pr->total_ns / pr->call_count;
                double total_ms = (double)pr->total_ns / 1000000.0;
                printf("║   %-20s calls=%-10llu total=%8.2fms  avg=%-7lluns  min=%-7lluns  max=%-7lluns\n",
                    keel_instr_probe_name((keel_instr_id_t)p),
                    (unsigned long long)pr->call_count,
                    total_ms,
                    (unsigned long long)avg_ns,
                    (unsigned long long)(pr->min_ns == UINT64_MAX ? 0 : pr->min_ns),
                    (unsigned long long)pr->max_ns);
            }
        }
    }

    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    fflush(stdout);
}
