/**
 * @file catchup_consult.h
 * @brief Engine-side wait-catchup consultation helper (Patch 2d-4).
 *
 * Wraps `keel_router_route` for the engine hot path: when a router is
 * configured with `consistency_mode=read_your_writes` and
 * `stale_read_policy=wait`, and the session holds a non-empty consistency
 * token, this helper asks the router whether it would emit
 * `KEEL_ROUTE_REASON_WAIT_CATCHUP` for a replica-eligible read.
 *
 * v0.5-alpha scope: this helper is a *decision* helper only. The caller
 * is expected to degrade to the primary on a WAIT verdict — the full
 * async park + re-dispatch continuation lives in
 * `keel_engine_consult_catchup` (catchup_bridge.h) and will be wired in
 * v0.5-beta once the engine has a resume continuation. Keeping the
 * decision logic in its own translation unit makes it directly unit-
 * testable without standing up a real worker + reactor.
 */

#ifndef KEEL_ENGINE_CATCHUP_CONSULT_H
#define KEEL_ENGINE_CATCHUP_CONSULT_H

#include <stdbool.h>

#include "keel/core/router.h"
#include "keel/sql/query_tree.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Consult the router for stale_read_policy=wait verdict.
 *
 * @param router            Router to consult; NULL → returns false.
 * @param qt                Parsed query tree for the read in question;
 *                          NULL → returns false. The router needs this to
 *                          confirm the query is a replica-safe read; the
 *                          engine's own `KEEL_FROUTE_READ` classification
 *                          is not visible to the router.
 * @param token             Session's latest consistency token; NULL or
 *                          empty (`value[0]=='\0'`) → returns false.
 * @param in_transaction    Whether the session is inside an open
 *                          transaction (router skips WAIT inside txns).
 * @param out_decision      Optional output: receives the full router
 *                          decision (factors, reason_code, wait_*) when
 *                          the router was consulted successfully. May
 *                          be NULL.
 *
 * @return true  iff the router emitted `KEEL_ROUTE_REASON_WAIT_CATCHUP`
 *               and the caller should degrade to the primary (v0.5-alpha
 *               semantics).
 * @return false in all other cases (router=NULL, no token, route err,
 *               or router chose a server normally).
 */
bool keel_engine_should_degrade_to_primary_on_wait(
    keel_router_t* router,
    const keel_qt_query_t* qt,
    const keel_consistency_token_t* token,
    bool in_transaction,
    keel_route_decision_t* out_decision);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_CATCHUP_CONSULT_H */
