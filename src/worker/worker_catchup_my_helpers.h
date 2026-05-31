/**
 * @file worker_catchup_my_helpers.h
 * @brief Pure, transport-free helpers used by the MySQL catch-up
 *        probe state machine.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Header-only `static inline` so:
 *   - the production probe SM (worker_catchup_my.c) inlines them; and
 *   - unit tests include them without dragging in the reactor /
 *     backend_async_start / log dependencies.
 *
 * Mirrors worker_catchup_pg_helpers.h. The MySQL token is a GTID set
 * string ("uuid:1-N[,uuid:1-M]..."); the catch-up probe uses
 * `WAIT_FOR_EXECUTED_GTID_SET(@gtid, 0)` which returns:
 *   '0' — replica has applied the entire set
 *   '1' — timeout (not reached yet)
 *   NULL — error (e.g. GTID mode disabled)
 *
 * We treat MySQL GTID sets as an *opaque* token: we cannot order two
 * unrelated GTID sets, so the comparator picks the longest one as a
 * proxy for "most strict" and `satisfied_by` simply requires the
 * waiter token to be `==` the reached token (string equality). The
 * server-side `WAIT_FOR_EXECUTED_GTID_SET` does the real ordering
 * (set inclusion); we just gate which waiter is satisfied by which
 * probe round here.
 */

#ifndef KEEL_WORKER_CATCHUP_MY_HELPERS_H
#define KEEL_WORKER_CATCHUP_MY_HELPERS_H

#include "keel/plugin/plugin_types.h"

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
 * @brief Defensive charset gate for a MySQL GTID-set string before it
 *        is interpolated into the probe's SQL literal.
 *
 * Accepts only the characters that legitimately appear in a GTID set:
 *   - hex digits (UUID parts and intervals are decimal but the latter
 *     are a strict subset of hex digits)
 *   - ':' (uuid:interval separator)
 *   - ',' (set separator)
 *   - '-' (interval range)
 *   - whitespace (servers sometimes emit space-separated sets)
 *
 * Rejects empty strings, quotes, semicolons, and everything else. Cap
 * at 500 bytes so the snprintf into the 576-byte SQL buffer in
 * `my_probe_encode_query` cannot truncate inside a quoted literal.
 */
static inline bool my_gtid_token_is_safe(const char* value)
{
    if (!value || value[0] == '\0') return false;
    size_t n = 0;
    for (const char* p = value; *p; p++, n++) {
        unsigned char c = (unsigned char)*p;
        bool ok = (c >= '0' && c <= '9') ||
                  (c >= 'a' && c <= 'f') ||
                  (c >= 'A' && c <= 'F') ||
                  c == ':' || c == ',' || c == '-' ||
                  c == ' ' || c == '\t' || c == '\n' || c == '\r';
        if (!ok) return false;
        if (n > 500) return false;
    }
    return true;
}

/**
 * @brief Total order over GTID-set tokens for the probe picker.
 *
 * MySQL GTID sets are partially ordered (set inclusion) — we cannot
 * pick a unique "highest" without doing real set arithmetic. The
 * catch-up probe doesn't actually need the picked token to be the
 * supremum: it only needs *some* parked token to probe, and the
 * server will then satisfy every waiter with the same string via
 * `WAIT_FOR_EXECUTED_GTID_SET`.
 *
 * So we use a stable, simple total order: longer string > shorter,
 * tie-break by lexicographic `strcmp` over `value`. timeline_id is
 * unused on MySQL (no concept of timelines) but included in the
 * compare to keep the comparator total over the token type.
 */
static inline int my_token_compare(const keel_consistency_token_t* a,
                                   const keel_consistency_token_t* b)
{
    if (a->timeline_id != b->timeline_id) {
        return (a->timeline_id < b->timeline_id) ? -1 : 1;
    }
    size_t la = strnlen(a->value, sizeof a->value);
    size_t lb = strnlen(b->value, sizeof b->value);
    if (la != lb) return (la < lb) ? -1 : 1;
    int c = strncmp(a->value, b->value, sizeof a->value);
    if (c == 0) return 0;
    return (c < 0) ? -1 : 1;
}

/**
 * @brief Decide whether a parked waiter token is satisfied by a token
 *        the probe just confirmed reached.
 *
 * For MySQL we conservatively require exact string equality (same
 * timeline_id, same `value`). A more sophisticated implementation
 * could test set inclusion client-side, but that would duplicate
 * server logic and risk drift. The server-side
 * `WAIT_FOR_EXECUTED_GTID_SET` is the authoritative oracle; this
 * gate just decides which queued waiter the probe answers for.
 *
 * Empty tokens are trivially satisfied (mirrors the vtable hook
 * `myf_replica_reached_token`).
 */
static inline bool my_token_satisfied_by(
    const keel_consistency_token_t* waiter,
    const keel_consistency_token_t* reached)
{
    if (waiter->timeline_id != reached->timeline_id) return false;
    if (waiter->value[0] == '\0') return true;
    return strncmp(waiter->value, reached->value, sizeof waiter->value) == 0;
}

/* ============================================================================
 * Wire encoding / decoding
 * ============================================================================ */

/**
 * @brief Encode the catch-up probe's SELECT into a MySQL COM_QUERY
 *        packet.
 *
 * The SQL is:
 *
 *     SELECT WAIT_FOR_EXECUTED_GTID_SET('$gtid', 0)
 *
 * `0` is the timeout in seconds — we want an immediate check; the
 * probe will be retried on the next manager tick if not yet reached.
 *
 * Packet layout (MySQL protocol):
 *   [0..2]  payload_len (3-byte LE)
 *   [3]     seq_id = 0 (new command)
 *   [4]     0x03 (COM_QUERY)
 *   [5..]   SQL bytes (no terminator)
 *
 * @param out         Destination buffer for the framed wire bytes.
 * @param cap         Capacity of @p out.
 * @param gtid_value  GTID-set string from a `keel_consistency_token_t`.
 * @param out_len     Receives the number of bytes written on success.
 * @return 0 on success; -1 if @p gtid_value fails the safety gate or
 *         the encoded packet would exceed @p cap.
 */
static inline int my_probe_encode_query(uint8_t* out, size_t cap,
                                        const char* gtid_value,
                                        size_t* out_len)
{
    if (!out || !gtid_value || !out_len) return -1;
    if (!my_gtid_token_is_safe(gtid_value)) return -1;

    char sql[576];
    int n = snprintf(sql, sizeof sql,
                     "SELECT WAIT_FOR_EXECUTED_GTID_SET('%s', 0)",
                     gtid_value);
    if (n < 0 || (size_t)n >= sizeof sql) return -1;

    size_t sql_len     = (size_t)n;
    size_t payload_len = 1 + sql_len;            /* COM_QUERY (1) + sql */
    size_t total       = 4 + payload_len;        /* 4-byte header */
    if (total > cap) return -1;

    out[0] = (uint8_t)(payload_len      );
    out[1] = (uint8_t)(payload_len >>  8);
    out[2] = (uint8_t)(payload_len >> 16);
    out[3] = 0;                                  /* seq_id = 0 */
    out[4] = 0x03;                               /* COM_QUERY */
    memcpy(out + 5, sql, sql_len);
    *out_len = total;
    return 0;
}

/** Outcome of `my_probe_parse_response`. */
typedef enum {
    MY_PARSE_NEED_MORE = 0,  /**< Not enough bytes yet — caller must recv. */
    MY_PARSE_CONSUMED  = 1,  /**< One full packet consumed; may continue. */
    MY_PARSE_DONE      = 2,  /**< Response complete (final EOF/OK seen). */
    MY_PARSE_ERROR     = -1, /**< Protocol error — caller must fail probe. */
} my_probe_parse_status_t;

/**
 * @brief Try to consume one framed MySQL backend packet from the head
 *        of a receive buffer.
 *
 * Recognises the result-set sequence for a `SELECT <scalar>` query
 * (with CLIENT_DEPRECATE_EOF disabled, which matches KEEL's backend
 * handshake):
 *
 *   pkt 1 (seq=1): column count   (varint)
 *   pkt 2 (seq=2): column definition (skipped)
 *   pkt 3 (seq=3): EOF1            (skipped)
 *   pkt 4 (seq=4): row data       — extract first column as ASCII '0'/'1'
 *   pkt 5 (seq=5): EOF2            → DONE
 *
 * An ERR packet at any point → ERROR. An OK packet (first byte 0x00)
 * before the row-data packet is allowed but unexpected and treated as
 * "not reached" (out_result=false) + DONE on the next EOF.
 *
 * Does NOT mutate the buffer. The caller is responsible for sliding
 * the buffer down by the returned byte count.
 *
 * @param buf                       Buffer to parse.
 * @param have                      Bytes available in @p buf.
 * @param[in,out] pkt_index         Packet counter for this round (start at 0).
 * @param[out] status               Parse outcome.
 * @param[out] out_result           Receives the row's boolean answer
 *                                  ('0' from WAIT_FOR_EXECUTED_GTID_SET ⇒ true).
 * @param[in,out] out_result_valid  Sticky flag — set true when the row
 *                                  data packet has been observed.
 * @return Bytes consumed from @p buf (0 when status is NEED_MORE).
 */
static inline size_t my_probe_parse_response(const uint8_t* buf, size_t have,
                                             int* pkt_index,
                                             my_probe_parse_status_t* status,
                                             bool* out_result,
                                             bool* out_result_valid)
{
    *status = MY_PARSE_NEED_MORE;
    if (!buf || have < 4 || !pkt_index || !status) return 0;

    uint32_t pkt_len = (uint32_t)buf[0]
                     | ((uint32_t)buf[1] <<  8)
                     | ((uint32_t)buf[2] << 16);
    /* seq_id at buf[3] — not asserted; servers occasionally renumber. */
    size_t total = 4 + (size_t)pkt_len;
    if (have < total) return 0;

    const uint8_t* payload = buf + 4;
    uint8_t first = (pkt_len > 0) ? payload[0] : 0xFE;
    (*pkt_index)++;

    if (first == 0xFF) {                /* ERR packet — fail probe */
        *status = MY_PARSE_ERROR;
        return total;
    }

    /* EOF marker: first=0xFE AND payload length <= 9 (length-encoded
     * NULL in a row body would also start with 0xFE but those packets
     * are much longer). */
    bool is_eof = (first == 0xFE && pkt_len <= 9);

    /* Header packet (column count) and the column definition packet
     * we don't care about — just skip. */
    if (*pkt_index <= 2) {
        *status = MY_PARSE_CONSUMED;
        return total;
    }

    /* After col-count + col-def: first EOF closes the column block,
     * then row data, then final EOF. */
    if (is_eof) {
        if (!*out_result_valid && *pkt_index == 3) {
            /* EOF closing the column block — keep going. */
            *status = MY_PARSE_CONSUMED;
            return total;
        }
        /* Either the final EOF (after a row) or an EOF where we
         * expected a row (treat as not-reached). */
        *status = MY_PARSE_DONE;
        return total;
    }

    /* Could also be an OK packet here (header-driven query). */
    if (first == 0x00) {
        *out_result = false;
        *out_result_valid = true;
        *status = MY_PARSE_DONE;
        return total;
    }

    /* Row data: length-encoded string at the start of the payload.
     * For our single boolean-ish column the value is one ASCII byte
     * ('0' = reached, '1' = timeout) or NULL (0xFB). */
    if (first == 0xFB) {                /* SQL NULL */
        *out_result = false;
        *out_result_valid = true;
    } else if (first < 0xFB && pkt_len >= 1 + (uint32_t)first) {
        if (first == 0) {               /* empty string */
            *out_result = false;
        } else {
            char v = (char)payload[1];
            *out_result = (v == '0');
        }
        *out_result_valid = true;
    } else {
        /* Multi-byte length encoding for our 1-byte expected value is
         * unexpected; treat as protocol error. */
        *status = MY_PARSE_ERROR;
        return total;
    }
    *status = MY_PARSE_CONSUMED;
    return total;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_WORKER_CATCHUP_MY_HELPERS_H */
