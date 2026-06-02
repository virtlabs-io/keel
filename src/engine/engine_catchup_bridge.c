/**
 * @file engine_catchup_bridge.c
 * @brief Implementation of the engine ↔ catch-up manager bridge.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/engine/catchup_bridge.h"

#include <string.h>

keel_flow_result_t keel_engine_consult_catchup(
    keel_router_t* router,
    keel_catchup_manager_t* catchup,
    struct keel_session* session,
    const keel_route_session_t* route_session,
    const struct keel_qt_query* qt,
    keel_catchup_resume_cb resume_cb,
    void* resume_userdata,
    keel_route_decision_t* out_decision)
{
    if (router == NULL || catchup == NULL || route_session == NULL) {
        return KEEL_FLOW_OK;
    }

    keel_route_decision_t local_decision;
    memset(&local_decision, 0, sizeof(local_decision));

    keel_error_t err = keel_router_route(router, qt, route_session, &local_decision);

    if (out_decision != NULL) {
        *out_decision = local_decision;
    }

    if (err != KEEL_OK) {
        return KEEL_FLOW_OK;
    }
    if (local_decision.reason_code != KEEL_ROUTE_REASON_WAIT_CATCHUP) {
        return KEEL_FLOW_OK;
    }

    /* Router asked us to park the session. Enqueue a waiter on the
     * already-chosen replica (wait_server_index); the catch-up manager
     * tick will probe the replica and fire `resume_cb` with the outcome. */
    keel_catchup_waiter_t* w = keel_catchup_enqueue(
        catchup,
        session,
        local_decision.wait_server_index,
        &local_decision.wait_token,
        local_decision.wait_max_ms,
        resume_cb,
        resume_userdata);

    if (w == NULL) {
        /* Wait list rejected the enqueue (typically `max_waiters` reached).
         * Spec for `stale_read_policy = wait` says the read must still be
         * served — caller should fall back to the primary. */
        return KEEL_FLOW_ERROR;
    }

    return KEEL_FLOW_WAIT_CATCHUP;
}
