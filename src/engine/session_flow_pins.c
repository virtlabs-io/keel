/**
 * @file session_flow_pins.c
 * @brief Centralised pin-mask mutation + observation for keel_session_flow_t.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Lives in its own translation unit so callers that only need the pin
 * helper (e.g. state_machine.c, test binaries) don't transitively pull
 * in the rest of engine_flow.c — which depends on backend_pool symbols.
 */

#include "keel/engine/engine_flow.h"
#include "keel/session/session.h"
#include "keel/util/util.h"
#include "keel/session/ssv_atom.h"
#include "keel/session/ssv.h"
#include "keel/core/stats.h"

static inline void sync_session_ssv_state(keel_session_t* session,
                                          keel_session_flow_t* sf)
{
    session->pin_reason = (uint32_t)keel_ssv_pin_reason_from_flow_pins(sf->pins);
}

static inline void observe_pin_transition(struct keel_stats_ctx* stats,
                                          keel_flow_pin_reason_t prev_pins,
                                          keel_flow_pin_reason_t next_pins)
{
    if (!stats)
        return;

    if (prev_pins == KEEL_FPIN_NONE && next_pins != KEEL_FPIN_NONE)
        KEEL_STAT_GAUGE_INC(stats, sessions_pinned);
    else if (prev_pins != KEEL_FPIN_NONE && next_pins == KEEL_FPIN_NONE)
        KEEL_STAT_GAUGE_DEC(stats, sessions_pinned);

    keel_flow_pin_reason_t added = next_pins & ~prev_pins;
    if (added & KEEL_FPIN_TRANSACTION) {
        KEEL_STAT_INC(stats, pin_reason_transaction);
        KEEL_STAT_GAUGE_INC(stats, sessions_pinned_transaction);
    }
    if ((prev_pins & KEEL_FPIN_TRANSACTION) &&
        !(next_pins & KEEL_FPIN_TRANSACTION))
        KEEL_STAT_GAUGE_DEC(stats, sessions_pinned_transaction);

    if (added & KEEL_FPIN_EXTENDED_PROTO) {
        KEEL_STAT_INC(stats, pin_reason_extended_protocol);
        KEEL_STAT_GAUGE_INC(stats, sessions_pinned_extended_protocol);
    }
    if ((prev_pins & KEEL_FPIN_EXTENDED_PROTO) &&
        !(next_pins & KEEL_FPIN_EXTENDED_PROTO))
        KEEL_STAT_GAUGE_DEC(stats, sessions_pinned_extended_protocol);

    if (added & KEEL_FPIN_PREPARED_STMT) {
        KEEL_STAT_INC(stats, pin_reason_prepared_stmt);
        KEEL_STAT_GAUGE_INC(stats, sessions_pinned_prepared_stmt);
    }
    if ((prev_pins & KEEL_FPIN_PREPARED_STMT) &&
        !(next_pins & KEEL_FPIN_PREPARED_STMT))
        KEEL_STAT_GAUGE_DEC(stats, sessions_pinned_prepared_stmt);

    const keel_flow_pin_reason_t tracked =
        KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO |
        KEEL_FPIN_PREPARED_STMT;
    if (added & ~tracked)
        KEEL_STAT_INC(stats, pin_reason_other);
}

void keel_session_flow_apply_pin_change(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_worker_t* worker,
    keel_flow_pin_reason_t add,
    keel_flow_pin_reason_t clear)
{
    if (!sf) return;
    keel_flow_pin_reason_t prev = sf->pins;
    sf->pins |= add;
    sf->pins &= ~clear;
    if (session)
        sync_session_ssv_state(session, sf);
    observe_pin_transition(worker ? worker->stats_ctx : NULL, prev, sf->pins);
}
