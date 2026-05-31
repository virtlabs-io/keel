/**
 * @file worker_catchup_pg_helpers.h
 * @brief Pure, transport-free helpers used by the PostgreSQL catch-up
 *        probe state machine.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * These are deliberately header-only (`static inline`) so that:
 *   - the production probe SM (worker_catchup_pg.c) can call them
 *     with full inlining; and
 *   - unit tests can include just this header without dragging in
 *     the reactor / backend_async_start / log dependencies of the
 *     full state machine.
 *
 * Nothing here touches sockets, allocations, the manager, or the
 * worker.  They take `keel_consistency_token_t` by const pointer and
 * are pure functions of their inputs.
 */

#ifndef KEEL_WORKER_CATCHUP_PG_HELPERS_H
#define KEEL_WORKER_CATCHUP_PG_HELPERS_H

#include "keel/plugin/plugin_types.h"

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Parse a PostgreSQL LSN string ("HHHH/LLLLLLLL") into a uint64_t.
 * @return true on success; false on NULL / empty / malformed input.
 */
static inline bool pg_lsn_parse(const char* s, uint64_t* out)
{
    if (!s || !*s || !out) return false;
    uint32_t hi = 0, lo = 0;
    int n = sscanf(s, "%x/%x", &hi, &lo);
    if (n != 2) return false;
    *out = ((uint64_t)hi << 32) | lo;
    return true;
}

/**
 * @brief Defensive check that an LSN token string is safe to interpolate
 *        into the probe's SQL Query message.
 *
 * Accepts only the strict PostgreSQL LSN grammar `HEX+ '/' HEX+` and
 * rejects everything else, including embedded quotes, semicolons,
 * whitespace, and multiple slashes. The probe additionally casts the
 * value with `::pg_lsn`, so any string that slips past this check would
 * still fail to parse server-side — but we belt-and-braces it here.
 */
static inline bool pg_lsn_token_is_safe(const char* value)
{
    if (!value || value[0] == '\0') return false;
    bool saw_slash = false, saw_left = false, saw_right = false;
    for (const char* p = value; *p; p++) {
        unsigned char ch = (unsigned char)*p;
        if (*p == '/') {
            if (saw_slash || !saw_left) return false;
            saw_slash = true;
            continue;
        }
        if (!isxdigit(ch)) return false;
        if (saw_slash) saw_right = true;
        else           saw_left  = true;
    }
    return saw_slash && saw_left && saw_right;
}

/**
 * @brief Total order on (timeline_id, LSN) pairs.
 *
 * Different timelines are ordered by their numeric `timeline_id` so the
 * picker is deterministic. Within a timeline, ordering is by parsed LSN.
 * Unparseable LSN strings are treated as 0 so the comparator is total.
 *
 * @return -1 if a<b, 0 if equal, +1 if a>b.
 */
static inline int pg_token_compare(const keel_consistency_token_t* a,
                                   const keel_consistency_token_t* b)
{
    if (a->timeline_id != b->timeline_id) {
        return (a->timeline_id < b->timeline_id) ? -1 : 1;
    }
    uint64_t la = 0, lb = 0;
    pg_lsn_parse(a->value, &la);
    pg_lsn_parse(b->value, &lb);
    if (la == lb) return 0;
    return (la < lb) ? -1 : 1;
}

/**
 * @brief Decide whether a parked waiter token is satisfied by a token
 *        the probe just confirmed reached.
 *
 * Requires (a) same `timeline_id` — crossing a promotion invalidates
 * old LSNs; and (b) `waiter_lsn <= reached_lsn`.
 */
static inline bool pg_token_satisfied_by(
    const keel_consistency_token_t* waiter,
    const keel_consistency_token_t* reached)
{
    if (waiter->timeline_id != reached->timeline_id) return false;
    uint64_t lw = 0, lr = 0;
    if (!pg_lsn_parse(waiter->value, &lw))  return false;
    if (!pg_lsn_parse(reached->value, &lr)) return false;
    return lw <= lr;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_WORKER_CATCHUP_PG_HELPERS_H */
