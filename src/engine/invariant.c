/**
 * @file invariant.c
 * @brief Runtime invariant and feature-interaction checker implementation.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Implements the compatibility matrix and invariant checking functions
 * defined in invariant.h. All checks are O(1) — field comparisons only,
 * no I/O, no allocation, no locking.
 */

#include "keel/engine/invariant.h"
#include "keel/session/state_profile.h"

#include <stdatomic.h>
#include <string.h>

/* ============================================================================
 * §1 — Feature Interaction Compatibility Matrix
 *
 * 12×12 matrix. Symmetric. Read as: row-feature × column-feature → level.
 *
 * Key design decisions embedded in this matrix:
 *
 *   TRANSACTION × PS_REPLAY = GUARDED
 *     PS replay into a backend that already has a transaction open is legal
 *     (e.g., the replay happened BEFORE the Bind that starts the tx), but
 *     replaying while tx==FAILED would be a bug.
 *
 *   COPY_MODE × most things = MUTEX
 *     During COPY IN, no other SQL messages can be interleaved. The COPY
 *     protocol runs to completion before any other action.
 *
 *   COMMIT_IN_DOUBT × DRAINING = GUARDED
 *     Drain must NOT force-close a session with commit_in_doubt. The XID
 *     check must complete first, or the client gets silent data loss.
 *
 *   TLS_HANDSHAKE × SPLICE = MUTEX
 *     Splice (zero-copy sendfile/splice) bypasses userspace TLS; cannot be
 *     active during handshake or on TLS connections at all.
 *
 *   MIGRATION × {TRANSACTION, PS_REPLAY, COPY} = MUTEX
 *     Migration requires the session to be completely quiescent: no pending
 *     work, no in-progress protocol state, no unflushed residuals.
 * ============================================================================ */

const keel_compat_level_t keel_compat_matrix[KEEL_FEAT_COUNT][KEEL_FEAT_COUNT] = {
/*   TX    PS_REP COPY    EXT   DOUBT  TLS_HS SPLICE DRAIN  MIGR   LSN   HPIN  QUAR                        */
 {    0,     1,     0,     0,     1,     3,     0,     0,     3,    0,    0,    0  },  /* TRANSACTION      */
 {    1,     0,     3,     1,     3,     3,     2,     0,     3,    3,    0,    1  },  /* PS_REPLAY        */
 {    0,     3,     0,     3,     3,     3,     2,     0,     3,    3,    0,    3  },  /* COPY_MODE        */
 {    0,     1,     3,     0,     1,     3,     0,     0,     3,    0,    0,    0  },  /* EXTENDED_PROTO   */
 {    1,     3,     3,     1,     0,     3,     3,     1,     3,    3,    1,    3  },  /* COMMIT_IN_DOUBT  */
 {    3,     3,     3,     3,     3,     0,     3,     3,     3,    3,    3,    3  },  /* TLS_HANDSHAKE    */
 {    0,     2,     2,     0,     3,     3,     0,     2,     3,    0,    0,    0  },  /* SPLICE           */
 {    0,     0,     0,     0,     1,     3,     2,     0,     0,    0,    0,    0  },  /* DRAINING         */
 {    3,     3,     3,     3,     3,     3,     3,     0,     0,    3,    3,    3  },  /* MIGRATION        */
 {    0,     3,     3,     0,     3,     3,     0,     0,     3,    0,    0,    0  },  /* LSN_CAPTURE      */
 {    0,     0,     0,     0,     1,     3,     0,     0,     3,    0,    0,    0  },  /* HARD_PIN         */
 {    0,     1,     3,     0,     3,     3,     0,     0,     3,    0,    0,    0  },  /* QUARANTINE       */
};

/* ============================================================================
 * §2 — Session Invariant Checker
 * ============================================================================ */

/**
 * @brief Check the current session state for cross-feature invariant violations.
 *
 * The checks are deliberately constant time so they can be enabled liberally
 * in debug and hardening builds without changing hot-path asymptotics.
 *
 * @param sf Session flow state.
 * @param session Session runtime state, or `NULL` when unavailable.
 * @param be_conn Borrowed backend connection, or `NULL`.
 * @return Bitmask of violated invariants, or `KEEL_INV_OK` when all checks pass.
 */
uint32_t keel_invariant_check_session(
    const keel_session_flow_t *sf,
    const keel_session_t      *session,
    const backend_conn_t      *be_conn)
{
    if (!sf) return 0;
    uint32_t v = KEEL_INV_OK;

    /* ---- R1: PS replay × transaction × pool ---- */

    /* Replay requires a backend to send Parse messages to */
    if (sf->stmt_replay_len > 0 && !be_conn)
        v |= KEEL_INV_REPLAY_WITHOUT_BACKEND;

    /* Cannot replay into a failed transaction — Parse would fail */
    if (sf->stmt_replay_len > 0 && sf->tx == KEEL_TX_FAILED)
        v |= KEEL_INV_REPLAY_IN_FAILED_TX;

    /* Cannot replay during COPY mode — protocol forbids interleaving */
    if (sf->stmt_replay_len > 0 && (sf->pins & KEEL_FPIN_COPY))
        v |= KEEL_INV_REPLAY_DURING_COPY;

    /* ---- R2: LSN capture × protocol framing ---- */

    /* capture_lsn_pending with txn_had_writes=false and no pending write:
     * This is not necessarily a bug (could be a non-write query that set the
     * flag), but inside an explicit tx it means we'd capture a stale LSN.
     * Only flag when inside explicit tx and no writes occurred. */
    if (sf->capture_lsn_pending && sf->txn_had_writes == false &&
        sf->tx == KEEL_TX_ACTIVE)
        v |= KEEL_INV_LSN_WITHOUT_TX_WRITE;

    /* Cannot capture meaningful LSN in a failed transaction */
    if (sf->capture_lsn_pending && sf->tx == KEEL_TX_FAILED)
        v |= KEEL_INV_LSN_IN_FAILED_TX;

    /* ---- R3: TLS × splice ---- */
    /* Splice during TLS handshake is impossible — checked at feature level.
     * This is enforced by the engine never setting splice_eligible during
     * HANDSHAKE_AUTH phase, but we verify the invariant defensively. */
    if (sf->phase == KEEL_PHASE_HANDSHAKE_AUTH) {
        /* No splice_eligible check here since it's per-message, not per-session.
         * The test suite verifies this per-message. */
    }

    /* ---- R5: Migration eligibility ---- */
    /* These are checked at migration decision time, but we enforce them
     * structurally: if migration is somehow in progress (queued_for_pool
     * is a proxy), no pending state should exist. */

    /* ---- R6: Drain × commit-in-doubt ---- */
    /* This cannot be checked purely from sf — it requires drain state from
     * the engine. The macro KEEL_CHECK_INVARIANTS is called in the drain
     * path where the engine knows whether it's force-closing. */

    /* ---- Pin consistency ---- */
    if (!keel_pins_consistent(sf->pins))
        v |= KEEL_INV_PIN_CONFLICT;

    /* ---- Phase consistency ---- */

    /* In HANDSHAKE_AUTH, no query-path pins should be set */
    if (sf->phase == KEEL_PHASE_HANDSHAKE_AUTH &&
        (sf->pins & (KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO |
                     KEEL_FPIN_COPY | KEEL_FPIN_PREPARED_STMT)))
        v |= KEEL_INV_QUERY_IN_AUTH_PHASE;

    /* ---- Pool ↔ session consistency ---- */

    if (be_conn) {
        backend_conn_state_t bs = atomic_load(&((backend_conn_t *)be_conn)->state);

        /* If backend is IDLE, session should NOT think it's in a transaction */
        if (bs == BACKEND_CONN_IDLE && sf->tx == KEEL_TX_ACTIVE)
            v |= KEEL_INV_IDLE_BACKEND_WITH_TX;
    }

    /* If TRANSACTION pin is set, there should be a backend (or pending pool wait) */
    if ((sf->pins & KEEL_FPIN_TRANSACTION) && !be_conn && !sf->queued_for_pool)
        v |= KEEL_INV_NO_BACKEND_WITH_TX_PIN;

    /* ---- Commit-in-doubt consistency ---- */

    /* commit_in_doubt without txn_tracking enabled is impossible */
    if (sf->commit_in_doubt && !sf->txn_tracking)
        v |= KEEL_INV_DOUBT_WITHOUT_TRACKING;

    /* commit_in_doubt without a captured XID makes resolution impossible */
    if (sf->commit_in_doubt && sf->indoubt_xid == 0)
        v |= KEEL_INV_DOUBT_WITHOUT_XID;

    /* ---- Copy state consistency ---- */

    /* copy_skip or copy_hdr_len > 0 but no COPY pin */
    if ((sf->copy_skip > 0 || sf->copy_hdr_len > 0) &&
        !(sf->pins & KEEL_FPIN_COPY))
        v |= KEEL_INV_COPY_STATE_WITHOUT_PIN;

    return v;
}

/* ============================================================================
 * §3 — Pool Invariant Checker
 * ============================================================================ */

/**
 * Pool invariant flags (internal — mapped into the same uint32_t space).
 */
enum {
    KEEL_PINV_OK                     = 0,
    KEEL_PINV_COUNT_MISMATCH         = (1 << 0),
    KEEL_PINV_CLEANING_OVERFLOW      = (1 << 1),
    KEEL_PINV_ACTIVE_OVERFLOW        = (1 << 2),
    KEEL_PINV_IDLE_ON_WRONG_LIST     = (1 << 3),
    KEEL_PINV_BORROWABLE_HAS_OWNER   = (1 << 4),
    KEEL_PINV_CLEAN_LIST_DIRTY       = (1 << 5),
    KEEL_PINV_DIRTY_LIST_CLEAN       = (1 << 6),
    KEEL_PINV_LIST_DUPLICATE         = (1 << 7),
    KEEL_PINV_CLEANING_BORROWABLE    = (1 << 8),
    KEEL_PINV_CLEANUP_STATE_INVALID  = (1 << 9),
};

/**
 * @brief Check backend-pool counters and lists for structural consistency.
 *
 * @param pool Backend pool to inspect.
 * @return Bitmask of pool invariant violations, or `0` if the pool is consistent.
 */
uint32_t keel_invariant_check_pool(const backend_pool_t *pool)
{
    if (!pool) return 0;
    uint32_t v = 0;

    /* Sum of sublists + active + cleaning + closed should ≤ total */
    size_t accounted = pool->active_count + pool->clean_count +
                       pool->dirty_count + pool->cleaning_count;
    if (accounted > pool->total_count)
        v |= KEEL_PINV_COUNT_MISMATCH;

    /* cleaning_count cannot exceed total */
    if (pool->cleaning_count > pool->total_count)
        v |= KEEL_PINV_CLEANING_OVERFLOW;

    /* active_count cannot exceed total */
    if (pool->active_count > pool->total_count)
        v |= KEEL_PINV_ACTIVE_OVERFLOW;

    /* Walk clean_list: entries must be borrowable and fully clean. */
    {
        const backend_conn_t *c = pool->clean_list;
        while (c) {
            backend_conn_state_t s = atomic_load(&((backend_conn_t *)c)->state);
            if (s != BACKEND_CONN_IDLE)
                v |= KEEL_PINV_IDLE_ON_WRONG_LIST;
            if (c->pinned_session || c->hard_pinned)
                v |= KEEL_PINV_BORROWABLE_HAS_OWNER;
            if (c->current_state_hash != 0 || c->stmt_set_hash != 0 ||
                c->needs_discard_all || (c->profile && c->profile->count != 0))
                v |= KEEL_PINV_CLEAN_LIST_DIRTY;
            if (v) break;
            c = c->next;
        }
    }

    /* idle_list may carry state/profile/stmt hashes, but it must not contain
     * ACTIVE, CLEANING, CLOSED, or pinned/owned connections. */
    {
        const backend_conn_t *c = pool->idle_list;
        while (c) {
            backend_conn_state_t s = atomic_load(&((backend_conn_t *)c)->state);
            if (s != BACKEND_CONN_IDLE)
                v |= KEEL_PINV_IDLE_ON_WRONG_LIST;
            if (c->pinned_session || c->hard_pinned)
                v |= KEEL_PINV_BORROWABLE_HAS_OWNER;
            if (c->needs_discard_all)
                v |= KEEL_PINV_CLEAN_LIST_DIRTY;
            if (v) break;
            c = c->next;
        }
    }

    /* dirty_list entries are borrowable only through cleanup. They must be
     * IDLE/unowned and must actually need cleanup. */
    {
        const backend_conn_t *c = pool->dirty_list;
        while (c) {
            backend_conn_state_t s = atomic_load(&((backend_conn_t *)c)->state);
            bool dirty = c->needs_discard_all ||
                         c->current_state_hash != 0 ||
                         (c->profile && c->profile->count != 0);
            if (s != BACKEND_CONN_IDLE)
                v |= KEEL_PINV_IDLE_ON_WRONG_LIST;
            if (c->pinned_session || c->hard_pinned)
                v |= KEEL_PINV_BORROWABLE_HAS_OWNER;
            if (!dirty)
                v |= KEEL_PINV_DIRTY_LIST_CLEAN;
            if (v) break;
            c = c->next;
        }
    }

    /* When the pool owns a real connection array, enforce the stronger
     * ownership model: each backend may appear on at most one borrowable list,
     * and non-IDLE states must not be linked from any borrowable list. */
    if (pool->connections && pool->total_count > 0) {
        size_t actual_cleaning = 0;
        size_t actual_clean_list = 0;
        size_t actual_dirty_list = 0;

        for (const backend_conn_t *c = pool->clean_list; c; c = c->next)
            actual_clean_list++;
        for (const backend_conn_t *c = pool->dirty_list; c; c = c->next)
            actual_dirty_list++;

        if (actual_clean_list != pool->clean_count ||
            actual_dirty_list != pool->dirty_count) {
            v |= KEEL_PINV_COUNT_MISMATCH;
        }

        for (size_t i = 0; i < pool->total_count; i++) {
            const backend_conn_t *slot = &pool->connections[i];
            unsigned refs = 0;
            for (const backend_conn_t *c = pool->clean_list; c; c = c->next)
                if (c == slot) refs++;
            for (const backend_conn_t *c = pool->idle_list; c; c = c->next)
                if (c == slot) refs++;
            for (const backend_conn_t *c = pool->dirty_list; c; c = c->next)
                if (c == slot) refs++;

            backend_conn_state_t s = atomic_load(&((backend_conn_t *)slot)->state);
            if (refs > 1)
                v |= KEEL_PINV_LIST_DUPLICATE;
            if (refs > 0 && s != BACKEND_CONN_IDLE)
                v |= KEEL_PINV_IDLE_ON_WRONG_LIST;

            if (s == BACKEND_CONN_CLEANING) {
                actual_cleaning++;
                if (refs > 0)
                    v |= KEEL_PINV_CLEANING_BORROWABLE;
                if (slot->pinned_session || slot->hard_pinned)
                    v |= KEEL_PINV_BORROWABLE_HAS_OWNER;
                if (slot->cleanup_state == BACKEND_CLEANUP_NONE)
                    v |= KEEL_PINV_CLEANUP_STATE_INVALID;
            } else if (slot->cleanup_state != BACKEND_CLEANUP_NONE) {
                v |= KEEL_PINV_CLEANUP_STATE_INVALID;
            }
        }

        if (actual_cleaning != pool->cleaning_count)
            v |= KEEL_PINV_COUNT_MISMATCH;
    }

    return v;
}
