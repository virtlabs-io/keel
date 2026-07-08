/**
 * @file worker_group.c
 * @brief Worker-group runtime descriptor, defaults, and configuration helpers.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Extracted from src/main/main.c — no logic changes, only relocation.
 */

#include "keel/main/worker_group.h"
#include "keel/main/stats_display.h"  /* parse_bool_string */

#include "keel/engine/worker.h"
#include "keel/engine/backend_pool.h"
#include "keel/core/ini.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

/* ============================================================================
 * Global State
 * ============================================================================ */

size_t         g_num_groups                 = 0;
worker_group_t g_groups[KEEL_MAX_WORKER_GROUPS];
bool           g_experimental_features_enabled = false;
size_t         g_query_rule_count           = 0;
size_t         g_throttle_rule_count        = 0;
size_t         g_shard_rule_count           = 0;

/* ============================================================================
 * Reload Helpers
 * ============================================================================ */

void reload_collect_server_keys(const char* key, const char* value, void* ctx) {
    reload_srv_ctx_t* sc = ctx;
    if (sc->nentries < KEEL_MAX_SERVERS) {
        sc->entries[sc->nentries].key = key;
        sc->entries[sc->nentries].val = value;
        sc->nentries++;
    }
}

int reload_worker_group(keel_config_t* config, worker_group_t* wg) {
    const char* section = wg->section;
    int applied = 0;
    int restart_needed = 0;

    /* ------------------------------------------------------------------
     * §1 — Restart-required parameters: detect and warn
     * ------------------------------------------------------------------ */

    /* bind_port */
    int64_t new_port = keel_config_get_int(config, section, "bind_port",
                                            (int64_t)wg->listen_port);
    if (new_port > 0 && (uint16_t)new_port != wg->listen_port) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[%s] bind_port changed (%u -> %lld) — requires restart",
            wg->name, wg->listen_port, (long long)new_port);
        restart_needed++;
    }

    /* num_workers */
    int64_t new_workers = keel_config_get_int(config, section, "num_workers",
                                               (int64_t)wg->num_workers);
    if (new_workers > 0 && (uint32_t)new_workers != wg->num_workers) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[%s] num_workers changed (%u -> %lld) — requires restart",
            wg->name, wg->num_workers, (long long)new_workers);
        restart_needed++;
    }

    /* prepared_statement mode */
    {
        const char* ps_str = keel_config_get_string(config, section,
                                "prepared_statement", NULL);
        if (ps_str) {
            keel_ps_mode_t new_ps = KEEL_PS_MODE_VIRTUALIZE;
            if      (strcmp(ps_str, "pinning")   == 0) new_ps = KEEL_PS_MODE_PINNING;
            else if (strcmp(ps_str, "tracking")  == 0) new_ps = KEEL_PS_MODE_TRACKING;
            else if (strcmp(ps_str, "anonymous") == 0) new_ps = KEEL_PS_MODE_ANONYMOUS;
            else if (strcmp(ps_str, "off")       == 0) new_ps = KEEL_PS_MODE_OFF;
            if (new_ps != wg->ps_mode) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "[%s] prepared_statement changed (%d -> %d) — requires restart",
                    wg->name, (int)wg->ps_mode, (int)new_ps);
                restart_needed++;
            }
        }
    }

    /* runtime mode tier */
    {
        const char* mode_str = keel_config_get_string(config, section, "mode", NULL);
        if (mode_str) {
            keel_tier_t new_tier = keel_tier_parse(mode_str);
            if (new_tier != wg->runtime_mode) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "[%s] mode changed (%d -> %d) — requires restart",
                    wg->name, (int)wg->runtime_mode, (int)new_tier);
                restart_needed++;
            }
        }
    }

    /* transaction_tracking */
    {
        const char* tt = keel_config_get_string(config, section,
                            "transaction_tracking", NULL);
        if (tt) {
            bool new_tt = (strcmp(tt, "on") == 0);
            if (new_tt != wg->txn_tracking) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "[%s] transaction_tracking changed — requires restart",
                    wg->name);
                restart_needed++;
            }
        }
    }

    if (restart_needed > 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[%s] %d parameter(s) require restart to take effect",
            wg->name, restart_needed);
    }

    /* ------------------------------------------------------------------
     * §2 — Pool sizing (safe to change)
     * ------------------------------------------------------------------ */

    keel_engine_config_t* ecfg = wg->engine
                                 ? keel_engine_get_config_mut(wg->engine)
                                 : NULL;

    {
        int64_t new_min = keel_config_get_int(config, section, "min_pool_size",
                                               (int64_t)wg->pool_min_size);
        int64_t new_max = keel_config_get_int(config, section, "max_pool_size",
                                               (int64_t)wg->pool_max_size);
        if (new_min < 1) new_min = 1;
        if (new_max < new_min) new_max = new_min;

        bool pool_changed = false;
        if ((size_t)new_min != wg->pool_min_size) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] min_pool_size: %zu -> %lld",
                wg->name, wg->pool_min_size, (long long)new_min);
            wg->pool_min_size = (size_t)new_min;
            pool_changed = true;
        }
        if ((size_t)new_max != wg->pool_max_size) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] max_pool_size: %zu -> %lld",
                wg->name, wg->pool_max_size, (long long)new_max);
            wg->pool_max_size = (size_t)new_max;
            pool_changed = true;
        }

        if (pool_changed && ecfg) {
            ecfg->pool_min_size = wg->pool_min_size;
            ecfg->pool_max_size = wg->pool_max_size;

            uint32_t nw = ecfg->num_workers > 0 ? ecfg->num_workers : 4;
            size_t per_min = wg->pool_min_size / nw;
            size_t per_max = wg->pool_max_size / nw;
            if (per_min < 1) per_min = 1;
            if (per_max < per_min) per_max = per_min;

            uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
            for (uint32_t wi = 0; wi < nworkers; wi++) {
                keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                if (!w) continue;
                for (size_t si = 0; si < w->server_pool_count; si++) {
                    if (w->server_pools[si]) {
                        w->server_pools[si]->config.min_connections = per_min;
                        w->server_pools[si]->config.max_connections = per_max;
                    }
                }
            }
            applied++;
        }
    }

    /* pool_max_waiting */
    {
        int64_t new_mw = keel_config_get_int(config, section, "pool_max_waiting",
                                              (int64_t)wg->pool_max_waiting);
        if (new_mw >= 0 && (uint32_t)new_mw != wg->pool_max_waiting) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_max_waiting: %u -> %lld",
                wg->name, wg->pool_max_waiting, (long long)new_mw);
            wg->pool_max_waiting = (uint32_t)new_mw;
            if (ecfg) ecfg->pool_max_waiting = wg->pool_max_waiting;

            if (ecfg) {
                uint32_t nw = ecfg->num_workers > 0 ? ecfg->num_workers : 4;
                size_t per_mw = wg->pool_max_waiting > 0
                    ? wg->pool_max_waiting / (nw > 0 ? nw : 1) : 0;
                if (per_mw < 1 && wg->pool_max_waiting > 0) per_mw = 1;

                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (!w) continue;
                    for (size_t si = 0; si < w->server_pool_count; si++) {
                        if (w->server_pools[si])
                            w->server_pools[si]->config.max_waiting = per_mw;
                    }
                }
            }
            applied++;
        }
    }

    /* pool_wait_timeout_ms */
    {
        int64_t new_wt = keel_config_get_duration_ms(config, section, "pool_wait_timeout",
                                              (int64_t)wg->pool_wait_timeout_ms);
        if (new_wt >= 0 && (uint64_t)new_wt != wg->pool_wait_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_wait_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->pool_wait_timeout_ms, (long long)new_wt);
            wg->pool_wait_timeout_ms = (uint64_t)new_wt;
            if (ecfg) ecfg->pool_wait_timeout_ms = wg->pool_wait_timeout_ms;
            applied++;
        }
    }

    /* session_max_buffered_bytes */
    {
        int64_t new_smb = keel_config_get_bytes(config, section, "session_max_buffered",
                                               (int64_t)wg->session_max_buffered_bytes);
        if (new_smb == 0 || new_smb >= 4096) {
            if ((size_t)new_smb != wg->session_max_buffered_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] session_max_buffered: %zu -> %lld",
                    wg->name, wg->session_max_buffered_bytes, (long long)new_smb);
                wg->session_max_buffered_bytes = (size_t)new_smb;
                if (ecfg) ecfg->session_max_buffered_bytes = wg->session_max_buffered_bytes;
                applied++;
            }
        } else if (new_smb > 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] session_max_buffered=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)new_smb);
        }
    }

    /* backend_max_replay_bytes */
    {
        int64_t new_mrb = keel_config_get_bytes(config, section, "backend_max_replay",
                                               (int64_t)wg->backend_max_replay_bytes);
        if (new_mrb == 0 || new_mrb >= 4096) {
            if ((size_t)new_mrb != wg->backend_max_replay_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] backend_max_replay: %zu -> %lld",
                    wg->name, wg->backend_max_replay_bytes, (long long)new_mrb);
                wg->backend_max_replay_bytes = (size_t)new_mrb;
                if (ecfg) ecfg->backend_max_replay_bytes = wg->backend_max_replay_bytes;
                applied++;
            }
        } else if (new_mrb > 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] backend_max_replay=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)new_mrb);
        }
    }

    /* ------------------------------------------------------------------
     * §3 — Timeouts (safe to change)
     * ------------------------------------------------------------------ */

    {
        int64_t v;

        /* idle_timeout_ms */
        v = keel_config_get_duration_ms(config, section, "idle_timeout",
                                 (int64_t)wg->idle_timeout_ms);
        const char* ts = keel_config_get_string(config, section,
                            "client_idle_timeout", NULL);
        if (ts) {
            int tv = atoi(ts);
            if (tv > 0) {
                if      (strchr(ts, 'm')) tv *= 60000;
                else if (strchr(ts, 's')) tv *= 1000;
                v = tv;
            }
        }
        if (v > 0 && (uint64_t)v != wg->idle_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] idle_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->idle_timeout_ms, (long long)v);
            wg->idle_timeout_ms = (uint64_t)v;
            if (ecfg) ecfg->idle_timeout_ms = (uint32_t)v;

            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->idle_timeout_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* connect_timeout_ms */
        v = keel_config_get_duration_ms(config, section, "connect_timeout",
                                 (int64_t)wg->connect_timeout_ms);
        ts = keel_config_get_string(config, section, "client_connect_timeout", NULL);
        if (ts) {
            int tv = atoi(ts);
            if (tv > 0) {
                if      (strchr(ts, 'm')) tv *= 60000;
                else if (strchr(ts, 's')) tv *= 1000;
                v = tv;
            }
        }
        if (v > 0 && (uint64_t)v != wg->connect_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] connect_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->connect_timeout_ms, (long long)v);
            wg->connect_timeout_ms = (uint64_t)v;
            if (ecfg) ecfg->connect_timeout_ms = (uint32_t)v;
            applied++;
        }

        /* pool_prune_interval_ms */
        v = keel_config_get_duration_ms(config, section, "pool_prune_interval",
                                 (int64_t)wg->pool_prune_interval_ms);
        if (v > 0 && (uint32_t)v != wg->pool_prune_interval_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_prune_interval: %u -> %lld",
                wg->name, wg->pool_prune_interval_ms, (long long)v);
            wg->pool_prune_interval_ms = (uint32_t)v;
            if (ecfg) ecfg->pool_prune_interval_ms = (uint32_t)v;

            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->pool_prune_interval_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* pool_refill_interval_ms */
        v = keel_config_get_duration_ms(config, section, "pool_refill_interval",
                                 (int64_t)wg->pool_refill_interval_ms);
        if (v >= 100 && (uint32_t)v != wg->pool_refill_interval_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_refill_interval: %u -> %lld",
                wg->name, wg->pool_refill_interval_ms, (long long)v);
            wg->pool_refill_interval_ms = (uint32_t)v;
            if (ecfg) ecfg->pool_refill_interval_ms = (uint32_t)v;

            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->pool_refill_interval_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* pool_refill_backoff_ms */
        v = keel_config_get_duration_ms(config, section, "pool_refill_backoff",
                                 (int64_t)wg->pool_refill_backoff_ms);
        if (v > 0 && (uint32_t)v != wg->pool_refill_backoff_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_refill_backoff: %u -> %lld",
                wg->name, wg->pool_refill_backoff_ms, (long long)v);
            wg->pool_refill_backoff_ms = (uint32_t)v;
            if (ecfg) ecfg->pool_refill_backoff_ms = (uint32_t)v;

            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->pool_refill_backoff_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* pool_wait_timeout_ms */
        v = keel_config_get_duration_ms(config, section, "pool_wait_timeout",
                                 (int64_t)wg->pool_wait_timeout_ms);
        if (v >= 0 && (uint64_t)v != wg->pool_wait_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_wait_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->pool_wait_timeout_ms, (long long)v);
            wg->pool_wait_timeout_ms = (uint64_t)v;
            if (ecfg) ecfg->pool_wait_timeout_ms = (uint64_t)v;
            applied++;
        }

        /* session_max_buffered_bytes */
        v = keel_config_get_bytes(config, section, "session_max_buffered",
                                 (int64_t)wg->session_max_buffered_bytes);
        if (v == 0 || v >= 4096) {
            if ((size_t)v != wg->session_max_buffered_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] session_max_buffered: %zu -> %lld",
                    wg->name, wg->session_max_buffered_bytes, (long long)v);
                wg->session_max_buffered_bytes = (size_t)v;
                if (ecfg) ecfg->session_max_buffered_bytes = (size_t)v;
                applied++;
            }
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] session_max_buffered=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)v);
        }

        /* backend_max_replay_bytes */
        v = keel_config_get_bytes(config, section, "backend_max_replay",
                                 (int64_t)wg->backend_max_replay_bytes);
        if (v == 0 || v >= 4096) {
            if ((size_t)v != wg->backend_max_replay_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] backend_max_replay: %zu -> %lld",
                    wg->name, wg->backend_max_replay_bytes, (long long)v);
                wg->backend_max_replay_bytes = (size_t)v;
                if (ecfg) ecfg->backend_max_replay_bytes = (size_t)v;
                applied++;
            }
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] backend_max_replay=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)v);
        }
    }

    /* ------------------------------------------------------------------
     * §4 — Server weights (safe to change)
     * ------------------------------------------------------------------ */

    if (ecfg && keel_config_has_section(config, wg->servers_section)) {
        keel_server_pool_t* sp = &ecfg->server_pool;

        reload_srv_ctx_t wctx = { .nentries = 0 };
        keel_config_iter_keys(config, wg->servers_section,
            reload_collect_server_keys, &wctx);

        for (size_t ei = 0; ei < wctx.nentries; ei++) {
            const char* val = wctx.entries[ei].val;
            char h[256] = {0};
            uint16_t pt = 0;
            uint32_t wt = 100;

            const char* p = val;
            while (*p) {
                while (*p && (*p == ' ' || *p == '\t')) p++;
                if (!*p) break;

                if (strncmp(p, "host=", 5) == 0) {
                    p += 5;
                    char* d = h;
                    while (*p && *p != ' ' && *p != '\t' && (size_t)(d - h) < sizeof(h) - 1)
                        *d++ = *p++;
                    *d = '\0';
                } else if (strncmp(p, "port=", 5) == 0) {
                    pt = (uint16_t)atoi(p + 5);
                    while (*p && *p != ' ' && *p != '\t') p++;
                } else if (strncmp(p, "weight=", 7) == 0) {
                    wt = (uint32_t)atoi(p + 7);
                    while (*p && *p != ' ' && *p != '\t') p++;
                } else {
                    while (*p && *p != ' ' && *p != '\t') p++;
                }
            }

            for (size_t si = 0; si < sp->count; si++) {
                if (sp->servers[si].host && strcmp(sp->servers[si].host, h) == 0 &&
                    sp->servers[si].port == pt) {
                    if (sp->servers[si].weight != wt) {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] server %s:%u weight: %u -> %u",
                            wg->name, h, pt, sp->servers[si].weight, wt);
                        sp->servers[si].weight = wt;

                        if (si < wg->server_pool.count)
                            wg->server_pool.servers[si].weight = wt;
                        applied++;
                    }
                    break;
                }
            }
        }
    }

    /* ------------------------------------------------------------------
     * §5 — Probe configuration (safe to change)
     * ------------------------------------------------------------------ */

    if (wg->probe_mgr) {
        const char* v;
        uint32_t new_interval = 0, new_timeout = 0, new_retries = 0;

        v = keel_config_get_string(config, section, "probe_interval", NULL);
        if (v) {
            int val = atoi(v);
            if (val > 0) {
                if (strchr(v, 's')) val *= 1000;
                if ((uint32_t)val != wg->probe_cfg.interval_ms) {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "[%s] probe_interval: %ums -> %dms",
                        wg->name, wg->probe_cfg.interval_ms, val);
                    wg->probe_cfg.interval_ms = (uint32_t)val;
                    new_interval = (uint32_t)val;
                }
            }
        }

        v = keel_config_get_string(config, section, "probe_timeout", NULL);
        if (v) {
            int val = atoi(v);
            if (val > 0) {
                if (strchr(v, 's')) val *= 1000;
                if ((uint32_t)val != wg->probe_cfg.timeout_ms) {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "[%s] probe_timeout: %ums -> %dms",
                        wg->name, wg->probe_cfg.timeout_ms, val);
                    wg->probe_cfg.timeout_ms = (uint32_t)val;
                    new_timeout = (uint32_t)val;
                }
            }
        }

        int64_t retries = keel_config_get_int(config, section, "probe_retries",
                                               (int64_t)wg->probe_cfg.retries);
        if (retries > 0 && (uint32_t)retries != wg->probe_cfg.retries) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] probe_retries: %u -> %lld",
                wg->name, wg->probe_cfg.retries, (long long)retries);
            wg->probe_cfg.retries = (uint32_t)retries;
            new_retries = (uint32_t)retries;
        }

        if (new_interval || new_timeout || new_retries) {
            keel_probe_manager_update_timing(wg->probe_mgr,
                                              new_interval, new_timeout, new_retries);
            applied++;
        }
    }

    /* ------------------------------------------------------------------
     * §6 — Rebalancing (safe to change)
     * ------------------------------------------------------------------ */

    if (ecfg) {
        const char* rb = keel_config_get_string(config, section, "rebalance", NULL);
        if (rb) {
            bool new_rb = (strcmp(rb, "off") != 0);
            if (new_rb != wg->rebalance_enabled) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] rebalance: %s -> %s",
                    wg->name, wg->rebalance_enabled ? "on" : "off",
                    new_rb ? "on" : "off");
                wg->rebalance_enabled = new_rb;
                ecfg->rebalance_enabled = new_rb;
                applied++;
            }
        }

        int64_t v;
        v = keel_config_get_duration_ms(config, section, "rebalance_interval",
                                 (int64_t)wg->rebalance_interval_ms);
        if (v > 0 && (uint32_t)v != wg->rebalance_interval_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] rebalance_interval: %u -> %lld",
                wg->name, wg->rebalance_interval_ms, (long long)v);
            wg->rebalance_interval_ms = (uint32_t)v;
            ecfg->rebalance_interval_ms = (uint32_t)v;
            applied++;
        }

        v = keel_config_get_int(config, section, "rebalance_threshold_pct",
                                 (int64_t)wg->rebalance_threshold_pct);
        if (v > 100 && (uint32_t)v != wg->rebalance_threshold_pct) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] rebalance_threshold_pct: %u -> %lld",
                wg->name, wg->rebalance_threshold_pct, (long long)v);
            wg->rebalance_threshold_pct = (uint32_t)v;
            ecfg->rebalance_threshold_pct = (uint32_t)v;
            applied++;
        }

        v = keel_config_get_int(config, section, "rebalance_max_per_tick",
                                 (int64_t)wg->rebalance_max_per_tick);
        if (v > 0 && (uint32_t)v != wg->rebalance_max_per_tick) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] rebalance_max_per_tick: %u -> %lld",
                wg->name, wg->rebalance_max_per_tick, (long long)v);
            wg->rebalance_max_per_tick = (uint32_t)v;
            ecfg->rebalance_max_per_tick = (uint32_t)v;
            applied++;
        }
    }

    return applied;
}

/* ============================================================================
 * Worker Group Defaults and Helpers
 * ============================================================================ */

void worker_group_defaults(worker_group_t* g) {
    g->name              = NULL;
    g->listen_addr       = "0.0.0.0";
    g->listen_port       = 6432;
    g->default_protocol  = "postgres";
    g->num_workers       = 0;
    g->pin_workers       = true;
    g->pool_min_size     = 2;
    g->pool_max_size     = 100;
    g->session_pool_size = 1024;
    g->buffer_size       = 65536;
    g->max_clients       = 0;
    g->idle_timeout_ms            = 300000;
    g->connect_timeout_ms         = 10000;
    g->pool_prune_interval_ms     = 30000;
    g->pool_refill_interval_ms    = 100;
    g->pool_refill_backoff_ms     = 5000;
    g->pool_max_waiting           = 0;
    g->pool_wait_timeout_ms       = 0;
    g->session_max_buffered_bytes = 0;
    g->backend_max_replay_bytes   = 0;
    g->listen_backlog             = 4096;
    g->use_buf_rings              = false;
    g->buf_ring_size              = 0;
    g->sqpoll                     = false;
    g->sqpoll_idle_ms             = 1000;
    g->rebalance_enabled          = true;
    g->rebalance_interval_ms      = 5000;
    g->rebalance_threshold_pct    = 125;
    g->rebalance_max_per_tick     = 4;
    g->scatter_merge_max_mem_bytes = 0;
    snprintf(g->scatter_merge_spill_dir_buf,
             sizeof g->scatter_merge_spill_dir_buf, "/tmp");
    g->backend_host      = "127.0.0.1";
    g->backend_port      = 5432;
    g->backend_user      = "postgres";
    g->backend_password  = NULL;
    g->backend_database  = "postgres";
    memset(&g->server_pool, 0, sizeof(g->server_pool));
    g->probe_cfg = (keel_probe_config_t)KEEL_PROBE_CONFIG_DEFAULT;
    snprintf(g->probe_auth_buf, sizeof(g->probe_auth_buf), "auto");
    g->probe_cfg.probe_auth = g->probe_auth_buf;
    g->listen_fd  = -1;
    g->engine     = NULL;
    g->probe_mgr  = NULL;
    g->failover_mgr = NULL;
    g->ps_mode    = KEEL_PS_MODE_VIRTUALIZE;
    g->runtime_mode = KEEL_TIER_POOL;
    g->experimental_features = false;
    g->txn_tracking = false;
    g->wal_lsn_capture = false;
    g->gtid_capture = false;
    g->fast_network_path = true;
    g->result_cache = false;
    g->scatter_merge_enabled = false;
    g->sharding_enabled = false;
    g->hooks_enabled = false;
    g->sticky_primary_ttl_ms = 100U;

    g->failover_cfg = keel_failover_config_default();

    /* Initialize TLS configuration to disabled with safe defaults */
    memset(&g->tls_config, 0, sizeof(g->tls_config));
    g->tls_config.mode = KEEL_TLS_DISABLE;
    g->tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
    g->tls_config.min_version = KEEL_TLS_VERSION_AUTO;
    g->tls_config.max_version = 0;
    g->tls_config.ktls_enabled = true;
    g->tls_config.read_timeout_ms = 30000;
    g->tls_config.handshake_timeout_ms = 10000;
    g->tls_config.cert_file = NULL;
    g->tls_config.key_file = NULL;
    g->tls_config.ca_file = NULL;
    memset(g->tls_cert_file_buf, 0, sizeof(g->tls_cert_file_buf));
    memset(g->tls_key_file_buf, 0, sizeof(g->tls_key_file_buf));
    memset(g->tls_ca_file_buf, 0, sizeof(g->tls_ca_file_buf));
    memset(g->tls_mode_buf, 0, sizeof(g->tls_mode_buf));
    memset(g->tls_verify_buf, 0, sizeof(g->tls_verify_buf));

    /* Initialize backend TLS configuration */
    memset(&g->backend_tls_config, 0, sizeof(g->backend_tls_config));
    g->backend_tls_config.mode = KEEL_TLS_DISABLE;
    g->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
    g->backend_tls_config.min_version = KEEL_TLS_VERSION_AUTO;
    g->backend_tls_config.max_version = 0;
    g->backend_tls_config.ktls_enabled = true;
    g->backend_tls_config.read_timeout_ms = 30000;
    g->backend_tls_config.handshake_timeout_ms = 10000;
    g->backend_tls_config.cert_file = NULL;
    g->backend_tls_config.key_file = NULL;
    g->backend_tls_config.ca_file = NULL;
    memset(g->backend_tls_cert_file_buf, 0, sizeof(g->backend_tls_cert_file_buf));
    memset(g->backend_tls_key_file_buf, 0, sizeof(g->backend_tls_key_file_buf));
    memset(g->backend_tls_ca_file_buf, 0, sizeof(g->backend_tls_ca_file_buf));
    memset(g->backend_tls_mode_buf, 0, sizeof(g->backend_tls_mode_buf));
    memset(g->backend_tls_verify_buf, 0, sizeof(g->backend_tls_verify_buf));

    /* Enterprise authentication defaults */
    g->auth_method = KEEL_AUTH_SCRAM_SHA_256;
    strncpy(g->auth_method_buf, "scram-sha-256", sizeof(g->auth_method_buf) - 1);
    g->auth_method_buf[sizeof(g->auth_method_buf) - 1] = '\0';
    memset(g->auth_ldap_url_buf, 0, sizeof(g->auth_ldap_url_buf));
    memset(g->auth_ldap_base_dn_buf, 0, sizeof(g->auth_ldap_base_dn_buf));
    memset(g->auth_ldap_bind_dn_buf, 0, sizeof(g->auth_ldap_bind_dn_buf));
    memset(g->auth_ldap_bind_password_buf, 0, sizeof(g->auth_ldap_bind_password_buf));
    memset(g->auth_ldap_search_filter_buf, 0, sizeof(g->auth_ldap_search_filter_buf));
    memset(g->auth_ldap_dn_suffix_buf, 0, sizeof(g->auth_ldap_dn_suffix_buf));
    g->auth_ldap_start_tls = false;
    g->auth_ldap_timeout_s = 5;
    memset(g->auth_pam_service_buf, 0, sizeof(g->auth_pam_service_buf));
    memset(g->auth_query_buf, 0, sizeof(g->auth_query_buf));
    memset(g->auth_query_conn_buf, 0, sizeof(g->auth_query_conn_buf));
    memset(g->auth_userlist_file_buf, 0, sizeof(g->auth_userlist_file_buf));
    g->pool_max_connection_age_ms = 0;
}

void collect_worker_groups(const char* sec, void* ctx) {
    wg_collect_ctx_t* c = (wg_collect_ctx_t*)ctx;
    /* Match "worker_group.X" but not "worker_group.X.servers" or ".probe" */
    const char* after = sec + 13; /* skip "worker_group." */
    if (*after && strchr(after, '.') == NULL && *c->count < c->max) {
        worker_group_t* g = &g_groups[*c->count];
        snprintf(g->section, sizeof(g->section), "%s", sec);
        snprintf(g->servers_section, sizeof(g->servers_section), "%s.servers", sec);
        (*c->count)++;
    }
}

void collect_srv_keys(const char* key, const char* value, void* ctx) {
    srv_collect_ctx_t* c = (srv_collect_ctx_t*)ctx;
    if (c->count < c->cap) {
        c->keys[c->count] = key;
        c->vals[c->count] = value;
        c->count++;
    }
}

bool config_bool_enabled(const keel_config_t* config,
                         const char* section,
                         const char* key,
                         bool default_val)
{
    const char* val = keel_config_get_string(config, section, key, NULL);
    if (!val) return default_val;
    return (strcasecmp(val, "true") == 0 ||
            strcmp(val, "1") == 0 ||
            strcasecmp(val, "yes") == 0 ||
            strcasecmp(val, "on") == 0);
}

void append_feature_name(char* buf, size_t cap, const char* name, bool* first)
{
    size_t len;
    if (!buf || cap == 0 || !name || !first) return;
    len = strlen(buf);
    if (len >= cap - 1) return;
    if (!*first) {
        snprintf(buf + len, cap - len, ", ");
        len = strlen(buf);
        if (len >= cap - 1) return;
    }
    snprintf(buf + len, cap - len, "%s", name);
    *first = false;
}

void build_runtime_feature_list(const worker_group_t* wg,
                                bool cluster_compression_enabled,
                                char* out,
                                size_t out_cap)
{
    bool first = true;
    if (!wg || !out || out_cap == 0) return;
    out[0] = '\0';

    if (KEEL_TIER_HAS_POOLING(wg->runtime_mode))
        append_feature_name(out, out_cap, "pooling", &first);
    if (KEEL_TIER_HAS_ROUTING(wg->runtime_mode))
        append_feature_name(out, out_cap, "routing", &first);
    if (KEEL_TIER_HAS_STATE_SYNC(wg->runtime_mode))
        append_feature_name(out, out_cap, "state_sync", &first);
    if (wg->txn_tracking)
        append_feature_name(out, out_cap, "transaction_tracking", &first);
    if (wg->result_cache)
        append_feature_name(out, out_cap, "result_cache", &first);
    if (wg->hooks_enabled || g_query_rule_count > 0 || g_throttle_rule_count > 0)
        append_feature_name(out, out_cap, "hooks", &first);
    if (wg->sharding_enabled || g_shard_rule_count > 0)
        append_feature_name(out, out_cap, "sharding", &first);
    if (wg->scatter_merge_enabled)
        append_feature_name(out, out_cap, "scatter_merge", &first);
    if (cluster_compression_enabled)
        append_feature_name(out, out_cap, "cluster_compression", &first);
    if (wg->wal_lsn_capture)
        append_feature_name(out, out_cap, "wal_lsn_capture", &first);
    if (wg->gtid_capture)
        append_feature_name(out, out_cap, "gtid_capture", &first);

    if (first)
        snprintf(out, out_cap, "none");
}

bool validate_experimental_feature_gates(bool cluster_compression_enabled)
{
    bool valid = true;
    if (!g_experimental_features_enabled && g_query_rule_count > 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: query_rule.*");
        valid = false;
    }
    if (!g_experimental_features_enabled && g_throttle_rule_count > 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: throttle.*");
        valid = false;
    }
    if (!g_experimental_features_enabled && g_shard_rule_count > 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: shard_rule.*");
        valid = false;
    }
    if (!g_experimental_features_enabled && cluster_compression_enabled) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: [cluster] compress");
        valid = false;
    }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        const worker_group_t* wg = &g_groups[gi];
        const char* section = wg->section[0] ? wg->section : "(worker_group)";
        bool allow_group_experimental =
            g_experimental_features_enabled || wg->experimental_features;
        if (KEEL_TIER_IS_EXPERIMENTAL(wg->runtime_mode) && !allow_group_experimental) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] mode=full. "
                "mode=full enables hardening/experimental subsystems (hooks, transaction "
                "tracking, LSN capture) and is not the recommended production default for "
                "production candidate deployments. Use mode=pool or mode=smart instead.",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->result_cache) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] result_cache=on",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->hooks_enabled) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s.hooks] section",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->sharding_enabled) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s.servers] shard_id",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->scatter_merge_enabled) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] scatter_merge*",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->wal_lsn_capture) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] wal_lsn_capture=on",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->gtid_capture) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] gtid_capture=on",
                section);
            valid = false;
        }
    }

    return valid;
}
