/**
 * @file engine_catchup_consult.c
 * @brief Engine-side wait-catchup consultation helper (Patch 2d-4).
 *
 * See keel/engine/catchup_consult.h for design rationale.
 */

#include "keel/engine/catchup_consult.h"

#include <string.h>

bool keel_engine_should_degrade_to_primary_on_wait(
    keel_router_t* router,
    const keel_qt_query_t* qt,
    const keel_consistency_token_t* token,
    bool in_transaction,
    keel_route_decision_t* out_decision)
{
    if (router == NULL) return false;
    if (qt     == NULL) return false;
    if (token  == NULL) return false;
    if (token->value[0] == '\0') return false;

    keel_route_session_t rs;
    memset(&rs, 0, sizeof(rs));
    rs.in_transaction              = in_transaction;
    rs.requires_consistent_read    = true;
    rs.required_consistency_token  = *token;

    keel_route_decision_t rd;
    memset(&rd, 0, sizeof(rd));
    if (keel_router_route(router, qt, &rs, &rd) != KEEL_OK) {
        return false;
    }
    if (out_decision) *out_decision = rd;
    return rd.reason_code == KEEL_ROUTE_REASON_WAIT_CATCHUP;
}
