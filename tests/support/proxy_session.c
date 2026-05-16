/**
 * @file proxy_session.c
 * @brief Sharding-aware proxy session implementation.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "proxy_session.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"
#include "keel/log/log.h"

#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Internal structure
 * ============================================================================ */

struct keel_client_session {
    keel_router_t*             router;
    keel_connpool_registry_t*  registry;

    /* PG-wire-level routing state passed to every dispatch call */
    keel_route_session_t       routing_state;

    /* Lifetime counters */
    keel_client_session_stats_t stats;
};

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

keel_client_session_t* keel_client_session_create(
    keel_router_t*            router,
    keel_connpool_registry_t* reg) {
    if (!router) return NULL;

    keel_client_session_t* cs = keel_calloc(1, sizeof(keel_client_session_t));
    if (!cs) return NULL;

    cs->router   = router;
    cs->registry = reg;
    /* routing_state and stats are zero-initialised by calloc */
    return cs;
}

void keel_client_session_destroy(keel_client_session_t* cs) {
    if (!cs) return;
    /* Clear scatter tracking before freeing */
    keel_router_clear_scatter_participation(&cs->routing_state);
    cs->routing_state.pinned_server = NULL;
    keel_free(cs);
}

/* ============================================================================
 * Dispatch
 * ============================================================================ */

keel_error_t keel_client_session_dispatch(
    keel_client_session_t*            cs,
    keel_str_t                        sql,
    const keel_shard_bound_params_t*  params,
    bool                              is_write,
    keel_dispatch_result_t*           out,
    keel_query_timing_t*              timing) {
    if (!cs || !out) return KEEL_ERR_INVALID_ARG;

    keel_time_t t_start = keel_time_now();

    keel_error_t err = keel_router_dispatch_sql(
        cs->router, sql, &cs->routing_state, params, is_write, out);

    keel_time_t t_dispatch = keel_time_now();

    if (err != KEEL_OK) {
        if (err == KEEL_ERR_SHARD_CROSS_TX) {
            cs->stats.cross_tx_rejected++;
        }
        return err;
    }

    cs->stats.queries_total++;
    if (out->kind == KEEL_DISPATCH_SINGLE) {
        cs->stats.queries_single++;
    } else if (out->kind == KEEL_DISPATCH_SCATTER) {
        cs->stats.queries_scatter++;
    }

    if (timing) {
        timing->dispatch_ns = (uint64_t)keel_time_diff(t_start, t_dispatch);
        timing->acquire_ns  = 0;   /* populated by caller after pool acquire */
        timing->execute_ns  = 0;   /* populated by caller after query        */
        timing->total_ns    = 0;   /* populated by caller at query end       */
    }

    return KEEL_OK;
}

void keel_client_session_release_conn(keel_client_session_t*    cs,
                                       const keel_route_server_t* server,
                                       keel_conn_t*               conn,
                                       bool                       reusable) {
    if (!conn) return;

    if (cs && cs->registry && server) {
        keel_connpool_t* pool = keel_connpool_registry_get(cs->registry, server);
        if (pool) {
            keel_connpool_release(pool, conn, reusable);
            return;
        }
    }
    /* Fallback: close the fd directly if we can't return to the pool */
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd    = -1;
        conn->state = KEEL_CONN_CLOSED;
    }
}

/* ============================================================================
 * Transaction control
 * ============================================================================ */

void keel_client_session_begin_tx(keel_client_session_t* cs) {
    if (!cs) return;
    cs->routing_state.in_transaction = true;
    cs->stats.tx_count++;
}

void keel_client_session_end_tx(keel_client_session_t* cs, bool aborted) {
    if (!cs) return;
    cs->routing_state.in_transaction = false;
    keel_router_clear_scatter_participation(&cs->routing_state);
    cs->routing_state.pinned_server = NULL;
    if (aborted) {
        cs->stats.tx_aborted++;
    }
}

void keel_client_session_record_scatter_write(keel_client_session_t* cs,
                                               const keel_scatter_plan_t* plan) {
    if (!cs || !plan) return;
    keel_router_record_scatter_write(&cs->routing_state, plan);
    cs->stats.queries_scatter++;
}

void keel_client_session_pin(keel_client_session_t*    cs,
                              const keel_route_server_t* server) {
    if (!cs) return;
    cs->routing_state.pinned_server = (keel_route_server_t*)(uintptr_t)server;
}

void keel_client_session_unpin(keel_client_session_t* cs) {
    if (!cs) return;
    cs->routing_state.pinned_server = NULL;
}

/* ============================================================================
 * Accessors
 * ============================================================================ */

const keel_route_session_t* keel_client_session_routing_state(
    const keel_client_session_t* cs) {
    if (!cs) return NULL;
    return &cs->routing_state;
}

void keel_client_session_get_stats(const keel_client_session_t*  cs,
                                    keel_client_session_stats_t*  stats) {
    if (!cs || !stats) return;
    *stats = cs->stats;
}
