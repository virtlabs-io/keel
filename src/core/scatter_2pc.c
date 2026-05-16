/**
 * @file scatter_2pc.c
 * @brief Two-phase commit coordinator for scatter (multi-shard) write transactions.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/core/scatter_2pc.h"

#include <string.h>
#include <stdio.h>
#include <stdint.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/** Find the participant slot for @p shard_idx, or NULL. */
static keel_2pc_participant_t*
find_participant(keel_2pc_coord_t* coord, size_t shard_idx)
{
    for (size_t i = 0; i < coord->count; i++) {
        if (coord->participants[i].shard_index == shard_idx)
            return &coord->participants[i];
    }
    return NULL;
}

static const keel_2pc_participant_t*
find_participant_c(const keel_2pc_coord_t* coord, size_t shard_idx)
{
    for (size_t i = 0; i < coord->count; i++) {
        if (coord->participants[i].shard_index == shard_idx)
            return &coord->participants[i];
    }
    return NULL;
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

void keel_2pc_coord_init(keel_2pc_coord_t* coord,
                         uint64_t          session_id,
                         uint64_t          seq)
{
    if (!coord) return;
    memset(coord, 0, sizeof *coord);
    snprintf(coord->gid_prefix, sizeof coord->gid_prefix,
             "keel_%"PRIu64"_%"PRIu64, session_id, seq);
    coord->state = KEEL_2PC_IDLE;
}

void keel_2pc_coord_reset(keel_2pc_coord_t* coord)
{
    if (!coord) return;
    char saved[sizeof coord->gid_prefix];
    memcpy(saved, coord->gid_prefix, sizeof saved);
    memset(coord, 0, sizeof *coord);
    memcpy(coord->gid_prefix, saved, sizeof saved);
    coord->state = KEEL_2PC_IDLE;
}

/* ============================================================================
 * Protocol phases
 * ============================================================================ */

keel_error_t keel_2pc_coord_begin(keel_2pc_coord_t*          coord,
                                   const keel_scatter_plan_t* plan)
{
    if (!coord || !plan) return KEEL_ERR_INVALID_ARG;
    if (coord->state != KEEL_2PC_IDLE) return KEEL_ERR_INVALID_ARG;

    uint64_t mask = plan->participating_shards_mask;

    /* Count bits to check capacity before writing anything. */
    size_t nbits = 0;
    for (uint64_t m = mask; m; m &= m - 1) nbits++;
    if (nbits > KEEL_2PC_MAX_PARTICIPANTS) return KEEL_ERR_OVERFLOW;

    coord->count = 0;
    for (size_t i = 0; i < 64; i++) {
        if (!(mask & ((uint64_t)1 << i))) continue;

        keel_2pc_participant_t* p = &coord->participants[coord->count++];
        p->shard_index = i;
        p->state       = KEEL_2PC_ACTIVE;
        snprintf(p->gid, sizeof p->gid,
                 "%s_s%zu", coord->gid_prefix, i);
    }

    coord->state = KEEL_2PC_ACTIVE;
    return KEEL_OK;
}

keel_error_t keel_2pc_coord_prepare(keel_2pc_coord_t* coord, size_t shard_idx)
{
    if (!coord) return KEEL_ERR_INVALID_ARG;
    keel_2pc_participant_t* p = find_participant(coord, shard_idx);
    if (!p)                              return KEEL_ERR_NOT_FOUND;
    if (p->state != KEEL_2PC_ACTIVE)    return KEEL_ERR_INVALID_ARG;
    p->state = KEEL_2PC_PREPARED;
    return KEEL_OK;
}

keel_error_t keel_2pc_coord_prepare_failed(keel_2pc_coord_t* coord, size_t shard_idx)
{
    if (!coord) return KEEL_ERR_INVALID_ARG;
    keel_2pc_participant_t* p = find_participant(coord, shard_idx);
    if (!p)                              return KEEL_ERR_NOT_FOUND;
    if (p->state != KEEL_2PC_ACTIVE)    return KEEL_ERR_INVALID_ARG;
    p->state = KEEL_2PC_ABORTED;
    return KEEL_OK;
}

keel_error_t keel_2pc_coord_commit_all(keel_2pc_coord_t* coord)
{
    if (!coord) return KEEL_ERR_INVALID_ARG;

    /* Reject calls on terminal coordinators (idempotent safety) */
    if (coord->state == KEEL_2PC_COMMITTED ||
        coord->state == KEEL_2PC_ROLLED_BACK)
        return KEEL_ERR_INVALID_ARG;

    /* All must be PREPARED. */
    for (size_t i = 0; i < coord->count; i++) {
        if (coord->participants[i].state != KEEL_2PC_PREPARED)
            return KEEL_ERR_INVALID_ARG;
    }
    for (size_t i = 0; i < coord->count; i++)
        coord->participants[i].state = KEEL_2PC_COMMITTED;
    coord->state = KEEL_2PC_COMMITTED;
    return KEEL_OK;
}

keel_error_t keel_2pc_coord_rollback_all(keel_2pc_coord_t* coord)
{
    if (!coord) return KEEL_ERR_INVALID_ARG;

    /* A coordinator that has already reached a terminal state (COMMITTED or
     * ROLLED_BACK) must not be rolled back again.  In crash-recovery the caller
     * reconstructs a fresh coordinator from durable storage; double-rollback of
     * an in-memory coordinator is always a caller bug. */
    if (coord->state == KEEL_2PC_COMMITTED ||
        coord->state == KEEL_2PC_ROLLED_BACK)
        return KEEL_ERR_INVALID_ARG;

    for (size_t i = 0; i < coord->count; i++) {
        keel_2pc_state_t s = coord->participants[i].state;
        if (s == KEEL_2PC_PREPARED || s == KEEL_2PC_ACTIVE)
            coord->participants[i].state = KEEL_2PC_ROLLED_BACK;
        /* ABORTED stays ABORTED (already reverted via normal ROLLBACK). */
    }
    coord->state = KEEL_2PC_ROLLED_BACK;
    return KEEL_OK;
}

/* ============================================================================
 * Inspection
 * ============================================================================ */

const char* keel_2pc_coord_gid(const keel_2pc_coord_t* coord, size_t shard_idx)
{
    if (!coord) return NULL;
    const keel_2pc_participant_t* p = find_participant_c(coord, shard_idx);
    return p ? p->gid : NULL;
}

keel_2pc_state_t keel_2pc_coord_shard_state(const keel_2pc_coord_t* coord,
                                              size_t shard_idx)
{
    if (!coord) return KEEL_2PC_IDLE;
    const keel_2pc_participant_t* p = find_participant_c(coord, shard_idx);
    return p ? p->state : KEEL_2PC_IDLE;
}

bool keel_2pc_coord_all_prepared(const keel_2pc_coord_t* coord)
{
    if (!coord || coord->count == 0) return false;
    for (size_t i = 0; i < coord->count; i++) {
        if (coord->participants[i].state != KEEL_2PC_PREPARED) return false;
    }
    return true;
}
