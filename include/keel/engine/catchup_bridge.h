/**
 * @file catchup_bridge.h
 * @brief Engine ↔ catch-up manager bridge (Patch 2d-3).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * # Purpose
 *
 * Patch 2d-2 made `keel_router_route()` emit `KEEL_ROUTE_REASON_WAIT_CATCHUP`
 * whenever `stale_read_policy = wait` would otherwise have forced the read
 * onto the primary. This header exposes the one helper the engine flow uses
 * to act on that signal: consult the router, and — if the verdict is WAIT —
 * park the session in the per-worker `keel_catchup_manager_t`.
 *
 * The helper is intentionally protocol-agnostic and decoupled from
 * `keel_worker_t`/`keel_session_flow_t` so it can be unit-tested in
 * isolation (no reactor required). The engine call site builds the
 * `keel_route_session_t` from its own state, supplies a resume callback
 * that knows how to continue the FE pipeline, and forwards the return
 * code to the worker's flow-result switch.
 */

#ifndef KEEL_ENGINE_CATCHUP_BRIDGE_H
#define KEEL_ENGINE_CATCHUP_BRIDGE_H

#include "keel/core/router.h"
#include "keel/engine/catchup.h"
#include "keel/engine/engine_flow.h"

#ifdef __cplusplus
extern "C" {
#endif

struct keel_qt_query;
struct keel_session;

/**
 * @brief Consult the router; if it asks to wait, enqueue a catch-up waiter.
 *
 * Decision logic:
 *   1. If @p router or @p catchup is NULL, returns `KEEL_FLOW_OK` (the
 *      caller proceeds with its normal routing path; nothing was parked).
 *   2. Calls `keel_router_route(router, qt, route_session, &decision)`.
 *   3. If the router returns a non-WAIT_CATCHUP verdict (or fails),
 *      returns `KEEL_FLOW_OK` and leaves `*out_decision` populated with
 *      whatever the router produced (if @p out_decision is non-NULL).
 *   4. If the verdict is `KEEL_ROUTE_REASON_WAIT_CATCHUP`, calls
 *      `keel_catchup_enqueue()` with the decision's `wait_server_index`,
 *      `wait_token`, and `wait_max_ms`. On enqueue success returns
 *      `KEEL_FLOW_WAIT_CATCHUP` — the caller MUST surface this to the
 *      worker (which leaves the FE recv un-armed) and MUST NOT touch
 *      @p session again until @p resume_cb fires.
 *   5. If enqueue fails (manager full, max_waiters exceeded), returns
 *      `KEEL_FLOW_ERROR` — the caller is expected to fall back to the
 *      primary per the spirit of `stale_read_policy = wait` (a parked
 *      slot was unavailable, but the read MUST still be served).
 *
 * @param router          Router handle; NULL → no-op.
 * @param catchup         Catch-up manager (per-worker); NULL → no-op.
 * @param session         Session pointer captured for the resume callback.
 * @param route_session   Router input (consistency token, txn state, …).
 *                        Must remain valid for the duration of this call.
 * @param qt              Pre-parsed Query Tree, or NULL.
 * @param resume_cb       Invoked when the waiter is released (REACHED,
 *                        TIMEOUT, PROBE_FAILED, or CANCELLED).
 * @param resume_userdata Opaque pointer forwarded to @p resume_cb.
 * @param out_decision    Optional out-param receiving the router's
 *                        decision (useful for callers that want to take
 *                        the non-WAIT path themselves).
 * @return                Flow result as described above.
 */
keel_flow_result_t keel_engine_consult_catchup(
    keel_router_t* router,
    keel_catchup_manager_t* catchup,
    struct keel_session* session,
    const keel_route_session_t* route_session,
    const struct keel_qt_query* qt,
    keel_catchup_resume_cb resume_cb,
    void* resume_userdata,
    keel_route_decision_t* out_decision);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_CATCHUP_BRIDGE_H */
