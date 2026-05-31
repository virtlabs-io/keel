/**
 * @file failover_manager.c
 * @brief Active failover-manager detector loop — implementation.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Implementation note (current scope): the detector thread observes the
 * authoritative pool->servers[].role and reconciles role changes into the
 * router's cluster-epoch state via keel_router_observe_primary(). The
 * probe manager remains the mutator of pool roles (see probe_manager.c)
 * because that path is well-tested and stable. A future refactor will
 * migrate the mutation responsibility into this component.
 *
 * Today the detector closes the gap that previously caused router epoch
 * to stay stale on probe-driven failovers (the Patroni and SQL discovery
 * paths already called observe_primary on their flips). With this in
 * place every flip — regardless of source — converges through the same
 * router-epoch state machine: degraded-mode exit, session timeline
 * checks, and node fencing.
 */

#include "keel/failover/failover_manager.h"
#include "keel/probe/probe.h"
#include "keel/core/router.h"
#include "keel/engine/engine.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ============================================================================
 * Internal state
 * ============================================================================ */

typedef struct fm_server_state {
    int      last_role;          /* keel_server_role_t snapshot from pool */
    uint32_t same_role_streak;   /* consecutive ticks role unchanged from last_role */
    bool     reported;           /* observe_primary already called for this role? */
} fm_server_state_t;

struct keel_failover_manager {
    keel_failover_manager_config_t cfg;
    keel_probe_manager_t*          probe_mgr;     /* may be NULL */
    keel_server_pool_t*            pool;
    keel_engine_t*                 engine;
    keel_router_t*                 router;        /* may be NULL */

    fm_server_state_t*             states;        /* [pool->count] */

    pthread_t                      thread;
    volatile bool                  running;
    volatile bool                  should_stop;

    uint64_t                       last_flip_ns;  /* monotonic ns of last epoch bump */
    _Atomic uint64_t               flip_count;
    _Atomic uint32_t               timeline;
};

/* ============================================================================
 * Helpers
 * ============================================================================ */

static uint64_t mono_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static const char* role_str(int r)
{
    switch (r) {
        case KEEL_SERVER_ROLE_RW:   return "RW";
        case KEEL_SERVER_ROLE_RO:   return "RO";
        case KEEL_SERVER_ROLE_WO:   return "WO";
        case KEEL_SERVER_ROLE_AUTO: return "AUTO";
        default:                    return "?";
    }
}

/**
 * @brief Reconcile a confirmed RW assignment into the router epoch.
 *
 * Builds a synthetic "host:port" identifier for the new primary and
 * invokes keel_router_observe_primary(). When the router has a
 * matching server registered by the same name, fencing/draining of
 * the prior primary takes effect at the routing layer; otherwise the
 * epoch counter and degraded_mode flag are still updated, which is
 * sufficient for sessions to detect topology drift.
 */
static void publish_primary(keel_failover_manager_t* mgr, size_t idx)
{
    if (!mgr->router) return;

    keel_backend_server_t* srv = &mgr->pool->servers[idx];
    char ident[128];
    snprintf(ident, sizeof(ident), "%s:%u",
             srv->host ? srv->host : "?", (unsigned)srv->port);

    uint32_t new_tl = atomic_fetch_add(&mgr->timeline, 1) + 1;
    bool flipped = keel_router_observe_primary(mgr->router, ident, new_tl);
    if (flipped) {
        atomic_fetch_add(&mgr->flip_count, 1);
        mgr->last_flip_ns = mono_ns();
        KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
                "failover-mgr: epoch bumped — primary=%s timeline=%u",
                ident, new_tl);
    }
}

/* ============================================================================
 * Detector tick
 * ============================================================================ */

/**
 * @brief Walk every server, detect debounced role changes, publish epoch.
 *
 * A change is acted on only after the same role has been observed for
 * @c failure_threshold consecutive ticks (debouncing flapping probes)
 * AND @c promotion_grace_ms has elapsed since the last published flip
 * (cooldown to prevent ping-pong).
 */
static void detector_tick(keel_failover_manager_t* mgr)
{
    keel_server_pool_t* pool = mgr->pool;
    uint64_t now = mono_ns();
    bool cooldown_ok = (now - mgr->last_flip_ns) >=
                       (uint64_t)mgr->cfg.promotion_grace_ms * 1000000ULL;

    for (size_t i = 0; i < pool->count; i++) {
        int cur_role = (int)pool->servers[i].role;
        fm_server_state_t* st = &mgr->states[i];

        if (cur_role == st->last_role) {
            if (st->same_role_streak < UINT32_MAX) st->same_role_streak++;
        } else {
            KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
                    "failover-mgr: server[%zu] %s:%u role change observed: %s → %s",
                    i, pool->servers[i].host, pool->servers[i].port,
                    role_str(st->last_role), role_str(cur_role));
            st->last_role = cur_role;
            st->same_role_streak = 1;
            st->reported = false;
        }

        /* Only act on RW: that's what observe_primary tracks. RO/WO are
         * uninteresting for cluster-epoch purposes. */
        if (cur_role != KEEL_SERVER_ROLE_RW) continue;
        if (st->reported) continue;
        if (st->same_role_streak < mgr->cfg.failure_threshold) continue;
        if (!cooldown_ok) continue;

        publish_primary(mgr, i);
        st->reported = true;
    }
}

/* ============================================================================
 * Thread loop
 * ============================================================================ */

static void* detector_thread(void* arg)
{
    keel_failover_manager_t* mgr = (keel_failover_manager_t*)arg;

#ifdef __linux__
    pthread_setname_np(pthread_self(), "keel-failover");
#endif

    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
            "failover-mgr: detector started — interval=%ums threshold=%u grace=%ums router=%s",
            mgr->cfg.detection_interval_ms, mgr->cfg.failure_threshold,
            mgr->cfg.promotion_grace_ms, mgr->router ? "yes" : "no");

    mgr->running = true;

    while (!mgr->should_stop) {
        uint64_t cycle_start = mono_ns();
        detector_tick(mgr);

        uint64_t elapsed_ms = (mono_ns() - cycle_start) / 1000000ULL;
        if (elapsed_ms < mgr->cfg.detection_interval_ms && !mgr->should_stop) {
            uint32_t sleep_ms = mgr->cfg.detection_interval_ms - (uint32_t)elapsed_ms;
            struct timespec ts = {
                .tv_sec  = sleep_ms / 1000,
                .tv_nsec = (long)(sleep_ms % 1000) * 1000000L,
            };
            while (nanosleep(&ts, &ts) < 0 && !mgr->should_stop) {
                /* EINTR — retry */
            }
        }
    }

    mgr->running = false;
    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE, "failover-mgr: detector stopped");
    return NULL;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

keel_failover_manager_t* keel_failover_manager_create(
    const keel_failover_manager_config_t* cfg,
    keel_probe_manager_t*                  probe_mgr,
    keel_server_pool_t*                    pool,
    keel_engine_t*                         engine,
    keel_router_t*                         router)
{
    if (!pool || !engine || pool->count == 0) return NULL;

    keel_failover_manager_t* mgr = keel_calloc(1, sizeof(*mgr));
    if (!mgr) return NULL;

    static const keel_failover_manager_config_t defaults = KEEL_FAILOVER_MANAGER_CONFIG_DEFAULT;
    mgr->cfg = cfg ? *cfg : defaults;
    if (mgr->cfg.detection_interval_ms == 0) mgr->cfg.detection_interval_ms = 500;
    if (mgr->cfg.failure_threshold     == 0) mgr->cfg.failure_threshold     = 1;

    mgr->probe_mgr = probe_mgr;
    mgr->pool      = pool;
    mgr->engine    = engine;
    mgr->router    = router;

    mgr->states = keel_calloc(pool->count, sizeof(fm_server_state_t));
    if (!mgr->states) {
        keel_free(mgr);
        return NULL;
    }
    for (size_t i = 0; i < pool->count; i++) {
        mgr->states[i].last_role = KEEL_SERVER_ROLE_AUTO;
        mgr->states[i].same_role_streak = 0;
        mgr->states[i].reported = false;
    }

    atomic_store(&mgr->flip_count, 0);
    atomic_store(&mgr->timeline, 0);
    mgr->last_flip_ns = 0;

    return mgr;
}

keel_error_t keel_failover_manager_start(keel_failover_manager_t* mgr)
{
    if (!mgr) return KEEL_ERR_INVALID_ARG;
    if (mgr->running) return KEEL_OK;

    mgr->should_stop = false;
    int rc = pthread_create(&mgr->thread, NULL, detector_thread, mgr);
    if (rc != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_PROBE,
                "failover-mgr: pthread_create failed: %s", strerror(rc));
        return KEEL_ERR_UNKNOWN;
    }
    return KEEL_OK;
}

void keel_failover_manager_stop(keel_failover_manager_t* mgr)
{
    if (!mgr) return;
    mgr->should_stop = true;
    if (mgr->running) {
        pthread_join(mgr->thread, NULL);
    }
}

void keel_failover_manager_destroy(keel_failover_manager_t* mgr)
{
    if (!mgr) return;
    keel_failover_manager_stop(mgr);
    keel_free(mgr->states);
    keel_free(mgr);
}

uint64_t keel_failover_manager_flip_count(const keel_failover_manager_t* mgr)
{
    if (!mgr) return 0;
    return atomic_load_explicit(&mgr->flip_count, memory_order_relaxed);
}

uint32_t keel_failover_manager_current_timeline(const keel_failover_manager_t* mgr)
{
    if (!mgr) return 0;
    return atomic_load_explicit(&mgr->timeline, memory_order_relaxed);
}
