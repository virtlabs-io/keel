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
#include "keel/util/endian.h"

#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

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

/* ============================================================================
 * Wire encoding / decoding
 * ============================================================================ */

/**
 * @brief Encode the catch-up probe's SELECT into a PG 'Q' (Query)
 *        message.
 *
 * The SQL is:
 *
 *     SELECT (pg_last_wal_replay_lsn() IS NULL)
 *         OR (pg_last_wal_replay_lsn() >= '$lsn'::pg_lsn);
 *
 * The NULL branch makes the probe pass transparently against a primary
 * (where `pg_last_wal_replay_lsn()` is NULL). The `$lsn` interpolation
 * is gated by `pg_lsn_token_is_safe()` to defend against injection.
 *
 * @param out       Destination buffer for the framed wire bytes.
 * @param cap       Capacity of @p out.
 * @param lsn_value LSN string from a `keel_consistency_token_t`.
 * @param out_len   Receives the number of bytes written on success.
 * @return 0 on success; -1 if @p lsn_value fails the safety gate or
 *         the encoded message would exceed @p cap.
 */
static inline int pg_probe_encode_query(uint8_t* out, size_t cap,
                                        const char* lsn_value,
                                        size_t* out_len)
{
    if (!out || !lsn_value || !out_len) return -1;
    if (!pg_lsn_token_is_safe(lsn_value)) return -1;

    char sql[256];
    int n = snprintf(sql, sizeof sql,
                     "SELECT (pg_last_wal_replay_lsn() IS NULL)"
                     " OR (pg_last_wal_replay_lsn() >= '%s'::pg_lsn);",
                     lsn_value);
    if (n < 0 || (size_t)n >= sizeof sql) return -1;

    size_t sql_len = (size_t)n;
    size_t total   = 1 + 4 + sql_len + 1;
    if (total > cap) return -1;

    out[0] = 'Q';
    keel_be32_put(out + 1, (uint32_t)(4 + sql_len + 1));
    memcpy(out + 5, sql, sql_len);
    out[5 + sql_len] = '\0';
    *out_len = total;
    return 0;
}

/** Outcome of `pg_probe_parse_message`. */
typedef enum {
    PG_PARSE_NEED_MORE = 0,  /**< Not enough bytes yet — caller must recv. */
    PG_PARSE_CONSUMED  = 1,  /**< One full message consumed; may continue. */
    PG_PARSE_DONE      = 2,  /**< ReadyForQuery seen — probe round complete. */
    PG_PARSE_ERROR     = -1, /**< Protocol error — caller must fail probe. */
} pg_probe_parse_status_t;

/**
 * @brief Try to consume one framed PG backend message from the head of a
 *        receive buffer.
 *
 * Recognises the small subset the probe cares about:
 *   - 'T' RowDescription, 'C' CommandComplete, 'N' NoticeResponse,
 *     'S' ParameterStatus — ignored.
 *   - 'D' DataRow — extracts the single boolean column (`'t'`/`'f'`) into
 *     @p out_result; @p out_result_valid is set to true.
 *     A SQL NULL value is treated as `false`.
 *   - 'Z' ReadyForQuery — sets `*status = PG_PARSE_DONE`.
 *   - 'E' ErrorResponse — returns 0 with `*status = PG_PARSE_ERROR`.
 *   - Anything else — tolerated and skipped.
 *
 * Does NOT mutate the buffer. The caller is responsible for sliding the
 * buffer down by the returned byte count.
 *
 * @param buf               Buffer to parse.
 * @param have              Bytes available in @p buf.
 * @param[out] status       Parse outcome.
 * @param[out] out_result   Receives the DataRow boolean (only meaningful
 *                          when *out_result_valid is set).
 * @param[in,out] out_result_valid Sticky flag — set to true when a
 *                          DataRow is parsed; never reset by this fn.
 * @return Number of bytes consumed from the front of @p buf
 *         (0 when status==PG_PARSE_NEED_MORE).
 */
static inline size_t pg_probe_parse_message(const uint8_t* buf, size_t have,
                                            pg_probe_parse_status_t* status,
                                            bool* out_result,
                                            bool* out_result_valid)
{
    *status = PG_PARSE_NEED_MORE;
    if (have < 5) return 0;

    uint8_t  type     = buf[0];
    uint32_t body_len = keel_be32_get(buf + 1);
    if (body_len < 4) { *status = PG_PARSE_ERROR; return 0; }
    size_t total = (size_t)1 + body_len;
    if (have < total) return 0;

    const uint8_t* body = buf + 5;
    uint32_t body_payload_len = body_len - 4;

    switch (type) {
    case 'T':  /* RowDescription */
    case 'C':  /* CommandComplete */
    case 'N':  /* NoticeResponse */
    case 'S':  /* ParameterStatus */
        *status = PG_PARSE_CONSUMED;
        break;

    case 'D': {  /* DataRow — extract bool. */
        if (body_payload_len < 2) { *status = PG_PARSE_ERROR; return 0; }
        uint16_t ncols = (uint16_t)((body[0] << 8) | body[1]);
        if (ncols != 1) { *status = PG_PARSE_ERROR; return 0; }
        if (body_payload_len < 6) { *status = PG_PARSE_ERROR; return 0; }
        int32_t col_len = (int32_t)keel_be32_get(body + 2);
        if (col_len < 0 || (uint32_t)(6 + col_len) > body_payload_len) {
            /* SQL NULL — treat as not-reached. */
            *out_result        = false;
            *out_result_valid  = true;
        } else if (col_len >= 1) {
            *out_result        = (body[6] == 't');
            *out_result_valid  = true;
        }
        *status = PG_PARSE_CONSUMED;
        break;
    }

    case 'Z':  /* ReadyForQuery — round complete. */
        *status = PG_PARSE_DONE;
        break;

    case 'E':  /* ErrorResponse. */
        *status = PG_PARSE_ERROR;
        return 0;

    default:
        /* Unknown but well-framed — tolerate. */
        *status = PG_PARSE_CONSUMED;
        break;
    }
    return total;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_WORKER_CATCHUP_PG_HELPERS_H */
