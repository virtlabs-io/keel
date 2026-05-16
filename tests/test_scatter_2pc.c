/**
 * @file test_scatter_2pc.c
 * @brief Unit tests for the two-phase commit coordinator (Phase G).
 *
 * Covers:
 *   - Init: gid_prefix format, idle state, zero participants
 *   - Begin: participants populated from mask, GIDs assigned, state → ACTIVE
 *   - Prepare: single participant ACTIVE→PREPARED, error on wrong shard/state
 *   - prepare_failed: ACTIVE→ABORTED
 *   - all_prepared: false until all prepared, true when all prepared
 *   - commit_all: PREPARED→COMMITTED happy path; error on mixed states
 *   - rollback_all: PREPARED/ACTIVE→ROLLED_BACK; ABORTED unchanged
 *   - GID format: "keel_<session>_<seq>_s<shard>"
 *   - reset: clears participants/state, preserves prefix
 *   - Edge cases: zero-shard mask (empty begin), NULL safety, full rollback
 */

#include "test_utils.h"
#include "keel/core/scatter_2pc.h"
#include "keel/core/router.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) { return g_tests_failed ? 1 : 0; }

/* ============================================================================
 * Helpers
 * ============================================================================ */

/** Build a scatter plan with exactly the given shard-index bits set. */
static keel_scatter_plan_t make_plan(uint64_t mask)
{
    keel_scatter_plan_t p;
    memset(&p, 0, sizeof p);
    p.participating_shards_mask = mask;
    /* populate count so the caller can use p.count if needed */
    size_t cnt = 0;
    for (uint64_t m = mask; m; m &= m - 1) cnt++;
    p.count = cnt;
    return p;
}

/* ============================================================================
 * Tests
 * ============================================================================ */

static void test_2pc_init_state(void)
{
    TEST_BEGIN("keel_2pc_coord_init: idle state, no participants, gid_prefix set");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 42ULL, 1ULL);

    TEST_ASSERT_EQ(c.state, KEEL_2PC_IDLE);
    TEST_ASSERT_EQ(c.count, (size_t)0);
    TEST_ASSERT(strncmp(c.gid_prefix, "keel_42_1", 9) == 0);

    TEST_END();
}

static void test_2pc_gid_prefix_format(void)
{
    TEST_BEGIN("keel_2pc_coord_init: gid_prefix format keel_<session>_<seq>");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 12345ULL, 7ULL);

    TEST_ASSERT_STR_EQ(c.gid_prefix, "keel_12345_7");

    TEST_END();
}

static void test_2pc_begin_populates_participants(void)
{
    TEST_BEGIN("keel_2pc_coord_begin: participants from mask, state ACTIVE");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);

    /* bits 0, 2, 5 set → 3 participants */
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 2) | (1ULL << 5));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_OK);

    TEST_ASSERT_EQ(c.count, (size_t)3);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ACTIVE);

    /* Each participant is ACTIVE and has a non-empty GID */
    for (size_t i = 0; i < c.count; i++) {
        TEST_ASSERT_EQ(c.participants[i].state, KEEL_2PC_ACTIVE);
        TEST_ASSERT(c.participants[i].gid[0] != '\0');
    }

    TEST_END();
}

static void test_2pc_begin_gid_contains_shard(void)
{
    TEST_BEGIN("keel_2pc_coord_begin: each GID encodes its shard index");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 99ULL, 2ULL);

    keel_scatter_plan_t plan = make_plan((1ULL << 3) | (1ULL << 7));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_OK);

    /* GIDs should contain "_s3" and "_s7" */
    const char* gid3 = keel_2pc_coord_gid(&c, 3);
    const char* gid7 = keel_2pc_coord_gid(&c, 7);
    TEST_ASSERT(gid3 != NULL);
    TEST_ASSERT(gid7 != NULL);
    TEST_ASSERT(strstr(gid3, "_s3") != NULL);
    TEST_ASSERT(strstr(gid7, "_s7") != NULL);

    TEST_END();
}

static void test_2pc_begin_twice_fails(void)
{
    TEST_BEGIN("keel_2pc_coord_begin: second call returns INVALID_ARG");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan(1ULL);
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_ERR_INVALID_ARG);

    TEST_END();
}

static void test_2pc_prepare_happy(void)
{
    TEST_BEGIN("keel_2pc_coord_prepare: ACTIVE → PREPARED");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);

    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 0), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_PREPARED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_ACTIVE); /* untouched */

    TEST_END();
}

static void test_2pc_prepare_unknown_shard(void)
{
    TEST_BEGIN("keel_2pc_coord_prepare: unknown shard → NOT_FOUND");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan(1ULL << 2);
    keel_2pc_coord_begin(&c, &plan);

    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 9), KEEL_ERR_NOT_FOUND);

    TEST_END();
}

static void test_2pc_prepare_twice_fails(void)
{
    TEST_BEGIN("keel_2pc_coord_prepare: preparing same shard twice → INVALID_ARG");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan(1ULL);
    keel_2pc_coord_begin(&c, &plan);

    keel_2pc_coord_prepare(&c, 0);
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 0), KEEL_ERR_INVALID_ARG);

    TEST_END();
}

static void test_2pc_prepare_failed_marks_aborted(void)
{
    TEST_BEGIN("keel_2pc_coord_prepare_failed: ACTIVE → ABORTED");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);

    TEST_ASSERT_EQ(keel_2pc_coord_prepare_failed(&c, 1), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_ABORTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_ACTIVE);

    TEST_END();
}

static void test_2pc_all_prepared_false_initially(void)
{
    TEST_BEGIN("keel_2pc_coord_all_prepared: false when no participants");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&c));

    TEST_END();
}

static void test_2pc_all_prepared_partial(void)
{
    TEST_BEGIN("keel_2pc_coord_all_prepared: false when only some prepared");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);

    TEST_ASSERT(!keel_2pc_coord_all_prepared(&c)); /* shard 1 still ACTIVE */

    TEST_END();
}

static void test_2pc_all_prepared_true(void)
{
    TEST_BEGIN("keel_2pc_coord_all_prepared: true when all prepared");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);
    keel_2pc_coord_prepare(&c, 1);

    TEST_ASSERT(keel_2pc_coord_all_prepared(&c));

    TEST_END();
}

static void test_2pc_commit_all_happy(void)
{
    TEST_BEGIN("keel_2pc_coord_commit_all: PREPARED → COMMITTED");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1) | (1ULL << 2));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);
    keel_2pc_coord_prepare(&c, 1);
    keel_2pc_coord_prepare(&c, 2);

    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_COMMITTED);
    for (size_t i = 0; i < c.count; i++)
        TEST_ASSERT_EQ(c.participants[i].state, KEEL_2PC_COMMITTED);

    TEST_END();
}

static void test_2pc_commit_all_requires_all_prepared(void)
{
    TEST_BEGIN("keel_2pc_coord_commit_all: INVALID_ARG if not all prepared");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0); /* shard 1 still ACTIVE */

    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_ERR_INVALID_ARG);
    /* State unchanged */
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ACTIVE);

    TEST_END();
}

static void test_2pc_rollback_all_from_prepared(void)
{
    TEST_BEGIN("keel_2pc_coord_rollback_all: PREPARED → ROLLED_BACK");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);
    keel_2pc_coord_prepare(&c, 1);

    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_ROLLED_BACK);

    TEST_END();
}

static void test_2pc_rollback_all_mixed_states(void)
{
    TEST_BEGIN("keel_2pc_coord_rollback_all: handles PREPARED+ACTIVE+ABORTED mix");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    /* 3 shards: s0=PREPARED, s1=ACTIVE, s2=ABORTED */
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1) | (1ULL << 2));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);
    keel_2pc_coord_prepare_failed(&c, 2);
    /* shard 1 stays ACTIVE */

    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 2), KEEL_2PC_ABORTED); /* unchanged */

    TEST_END();
}

static void test_2pc_gid_not_found(void)
{
    TEST_BEGIN("keel_2pc_coord_gid: NULL for unknown shard");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan(1ULL << 4);
    keel_2pc_coord_begin(&c, &plan);

    TEST_ASSERT(keel_2pc_coord_gid(&c, 4) != NULL);  /* known */
    TEST_ASSERT(keel_2pc_coord_gid(&c, 9) == NULL);  /* unknown */

    TEST_END();
}

static void test_2pc_shard_state_unknown(void)
{
    TEST_BEGIN("keel_2pc_coord_shard_state: IDLE for unknown shard");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);

    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 99), KEEL_2PC_IDLE);

    TEST_END();
}

static void test_2pc_reset_clears_participants(void)
{
    TEST_BEGIN("keel_2pc_coord_reset: clears count/state, preserves prefix");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 7ULL, 3ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);

    char saved_prefix[sizeof c.gid_prefix];
    memcpy(saved_prefix, c.gid_prefix, sizeof saved_prefix);

    keel_2pc_coord_reset(&c);

    TEST_ASSERT_EQ(c.count, (size_t)0);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_IDLE);
    TEST_ASSERT(memcmp(c.gid_prefix, saved_prefix, sizeof saved_prefix) == 0);

    TEST_END();
}

static void test_2pc_empty_mask(void)
{
    TEST_BEGIN("keel_2pc_coord_begin: zero mask → 0 participants, ACTIVE");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan(0ULL);
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_OK);
    TEST_ASSERT_EQ(c.count, (size_t)0);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ACTIVE);

    TEST_END();
}

static void test_2pc_null_safety(void)
{
    TEST_BEGIN("keel_2pc_coord: NULL coord/plan handled gracefully");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);

    keel_2pc_coord_init(NULL, 1, 1);           /* no crash */
    keel_2pc_coord_reset(NULL);                /* no crash */

    TEST_ASSERT_EQ(keel_2pc_coord_begin(NULL, NULL),    KEEL_ERR_INVALID_ARG);
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(NULL, 0),     KEEL_ERR_INVALID_ARG);
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(NULL),     KEEL_ERR_INVALID_ARG);
    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(NULL),   KEEL_ERR_INVALID_ARG);
    TEST_ASSERT(keel_2pc_coord_gid(NULL, 0) == NULL);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(NULL, 0), KEEL_2PC_IDLE);
    TEST_ASSERT(!keel_2pc_coord_all_prepared(NULL));
    TEST_ASSERT_EQ(keel_2pc_coord_overall_state(NULL),  KEEL_2PC_IDLE);
    TEST_ASSERT_EQ(keel_2pc_coord_participant_count(NULL), (size_t)0);

    TEST_END();
}

static void test_2pc_full_happy_path(void)
{
    TEST_BEGIN("keel_2pc_coord: full 3-shard prepare→commit happy path");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1000ULL, 42ULL);

    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1) | (1ULL << 2));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_participant_count(&c), (size_t)3);

    /* Phase 1: prepare each shard */
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&c));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 0), KEEL_OK);
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&c));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 1), KEEL_OK);
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&c));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 2), KEEL_OK);
    TEST_ASSERT(keel_2pc_coord_all_prepared(&c));

    /* Verify GIDs are stable across calls */
    const char* g0a = keel_2pc_coord_gid(&c, 0);
    const char* g0b = keel_2pc_coord_gid(&c, 0);
    TEST_ASSERT(g0a != NULL);
    TEST_ASSERT_STR_EQ(g0a, g0b);

    /* Phase 2: commit */
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_overall_state(&c), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 2), KEEL_2PC_COMMITTED);

    TEST_END();
}

/* ============================================================================
 * Phase 4.3 — Full commit/abort/crash-recovery matrix additions
 * ============================================================================ */

/**
 * Verify that once a coordinator has been fully committed, a subsequent
 * rollback_all call returns an error and does not disturb COMMITTED state.
 * Crash-recovery relevance: the recovery agent should only issue COMMIT or
 * ROLLBACK PREPARED once; a double-call must be harmless in the state machine.
 */
static void test_2pc_rollback_after_commit_is_noop_or_error(void)
{
    TEST_BEGIN("keel_2pc_coord_rollback_all: after COMMITTED returns INVALID_ARG, "
               "state unchanged");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);
    keel_2pc_coord_prepare(&c, 1);
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_COMMITTED);

    /* A second commit or rollback on an already-COMMITTED coordinator must not
     * corrupt state.  The implementation returns INVALID_ARG. */
    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&c), KEEL_ERR_INVALID_ARG);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_COMMITTED);

    TEST_END();
}

/**
 * Symmetric to the above: after a full rollback, commit_all must fail.
 */
static void test_2pc_commit_after_rollback_fails(void)
{
    TEST_BEGIN("keel_2pc_coord_commit_all: after ROLLED_BACK returns INVALID_ARG");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    keel_2pc_coord_begin(&c, &plan);
    keel_2pc_coord_prepare(&c, 0);
    keel_2pc_coord_prepare(&c, 1);
    keel_2pc_coord_rollback_all(&c);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ROLLED_BACK);

    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_ERR_INVALID_ARG);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ROLLED_BACK);

    TEST_END();
}

/**
 * Abort-then-rollback recovery matrix:
 *
 *   shard 0 → PREPARE succeeds  (PREPARED)
 *   shard 1 → PREPARE fails     (ABORTED)
 *
 * Because not all shards are PREPARED, commit_all must refuse.  The recovery
 * path is rollback_all: shard 0 (PREPARED) → ROLLED_BACK; shard 1 (ABORTED)
 * stays ABORTED because the shard's backend already rolled back normally.
 *
 * After rollback_all the coordinator overall state is ROLLED_BACK.  A second
 * rollback_all must also return INVALID_ARG (already done).
 */
static void test_2pc_abort_partial_then_rollback(void)
{
    TEST_BEGIN("2pc: partial prepare failure — commit refused, rollback completes");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 5ULL, 1ULL);

    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1) | (1ULL << 2));
    keel_2pc_coord_begin(&c, &plan);

    /* Shard 0 prepares OK */
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 0), KEEL_OK);
    /* Shard 1 prepare fails */
    TEST_ASSERT_EQ(keel_2pc_coord_prepare_failed(&c, 1), KEEL_OK);
    /* Shard 2 prepares OK */
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, 2), KEEL_OK);

    /* Not all prepared → commit must fail */
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&c));
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_ERR_INVALID_ARG);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ACTIVE); /* unchanged */

    /* Rollback path */
    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ROLLED_BACK);
    /* PREPARED shards get ROLLED_BACK */
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 0), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 2), KEEL_2PC_ROLLED_BACK);
    /* ABORTED shard stays ABORTED (backend already rolled back normally) */
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&c, 1), KEEL_2PC_ABORTED);

    /* Rollback is terminal — another rollback is rejected */
    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&c), KEEL_ERR_INVALID_ARG);

    TEST_END();
}

/**
 * GID determinism — crash-recovery contract.
 *
 * If the coordinator crashes between PREPARE TRANSACTION and COMMIT PREPARED,
 * a recovery agent can reconstruct the decision by re-initialising the
 * coordinator with the same (session_id, seq) pair and re-running begin() with
 * the same shard mask.  This test verifies that the GIDs produced are
 * identical regardless of when init() is called, so a recovery agent can
 * safely reissue COMMIT PREPARED '<gid>' for each prepared shard.
 */
static void test_2pc_gid_deterministic(void)
{
    TEST_BEGIN("keel_2pc_coord GID is deterministic — crash recovery guarantee");

    uint64_t session_id = 999ULL;
    uint64_t seq        = 7ULL;
    uint64_t mask       = (1ULL << 3) | (1ULL << 11) | (1ULL << 42);

    /* First initialisation (before "crash") */
    keel_2pc_coord_t c1;
    keel_2pc_coord_init(&c1, session_id, seq);
    keel_scatter_plan_t plan = make_plan(mask);
    keel_2pc_coord_begin(&c1, &plan);

    const char* gid3_before  = keel_2pc_coord_gid(&c1, 3);
    const char* gid11_before = keel_2pc_coord_gid(&c1, 11);
    const char* gid42_before = keel_2pc_coord_gid(&c1, 42);
    TEST_ASSERT(gid3_before  != NULL);
    TEST_ASSERT(gid11_before != NULL);
    TEST_ASSERT(gid42_before != NULL);

    /* Capture GID values before the "crash" */
    char saved3[KEEL_2PC_GID_MAX], saved11[KEEL_2PC_GID_MAX], saved42[KEEL_2PC_GID_MAX];
    memcpy(saved3,  gid3_before,  KEEL_2PC_GID_MAX);
    memcpy(saved11, gid11_before, KEEL_2PC_GID_MAX);
    memcpy(saved42, gid42_before, KEEL_2PC_GID_MAX);

    /* Second initialisation (after "crash" — same params) */
    keel_2pc_coord_t c2;
    keel_2pc_coord_init(&c2, session_id, seq);
    keel_2pc_coord_begin(&c2, &plan);

    /* GIDs must be byte-for-byte identical */
    TEST_ASSERT_STR_EQ(keel_2pc_coord_gid(&c2,  3), saved3);
    TEST_ASSERT_STR_EQ(keel_2pc_coord_gid(&c2, 11), saved11);
    TEST_ASSERT_STR_EQ(keel_2pc_coord_gid(&c2, 42), saved42);

    /* Different seq → different GIDs (no collision between concurrent txns) */
    keel_2pc_coord_t c3;
    keel_2pc_coord_init(&c3, session_id, seq + 1);
    keel_2pc_coord_begin(&c3, &plan);
    TEST_ASSERT(strcmp(keel_2pc_coord_gid(&c3, 3), saved3) != 0);

    TEST_END();
}

/**
 * Boundary: KEEL_2PC_MAX_PARTICIPANTS shards — all bits set in the mask up to
 * the maximum.  This exercises the capacity check and verifies that GIDs are
 * assigned to every shard without overflow.
 */
static void test_2pc_max_shards_boundary(void)
{
    TEST_BEGIN("keel_2pc_coord_begin: KEEL_2PC_MAX_PARTICIPANTS shards — no overflow");

    keel_2pc_coord_t c;
    keel_2pc_coord_init(&c, 1ULL, 1ULL);

    /* Build a mask with exactly KEEL_2PC_MAX_PARTICIPANTS bits set.
     * KEEL_2PC_MAX_PARTICIPANTS == KEEL_SCATTER_MAX_SHARDS == 64, so all bits. */
    uint64_t full_mask = (KEEL_2PC_MAX_PARTICIPANTS == 64)
                         ? UINT64_MAX
                         : ((uint64_t)1 << KEEL_2PC_MAX_PARTICIPANTS) - 1ULL;

    keel_scatter_plan_t plan = make_plan(full_mask);
    plan.count = KEEL_2PC_MAX_PARTICIPANTS;

    TEST_ASSERT_EQ(keel_2pc_coord_begin(&c, &plan), KEEL_OK);
    TEST_ASSERT_EQ(c.count, (size_t)KEEL_2PC_MAX_PARTICIPANTS);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_ACTIVE);

    /* Every participant has a valid GID */
    for (size_t i = 0; i < c.count; i++) {
        TEST_ASSERT(c.participants[i].gid[0] != '\0');
        TEST_ASSERT_EQ(c.participants[i].state, KEEL_2PC_ACTIVE);
    }

    /* Prepare all, commit all */
    for (size_t i = 0; i < c.count; i++)
        TEST_ASSERT_EQ(keel_2pc_coord_prepare(&c, c.participants[i].shard_index), KEEL_OK);
    TEST_ASSERT(keel_2pc_coord_all_prepared(&c));
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&c), KEEL_OK);
    TEST_ASSERT_EQ(c.state, KEEL_2PC_COMMITTED);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    keel_mem_init(NULL);

    printf("\n=== scatter_2pc tests (Phase G) ===\n\n");

    test_2pc_init_state();
    test_2pc_gid_prefix_format();
    test_2pc_begin_populates_participants();
    test_2pc_begin_gid_contains_shard();
    test_2pc_begin_twice_fails();
    test_2pc_prepare_happy();
    test_2pc_prepare_unknown_shard();
    test_2pc_prepare_twice_fails();
    test_2pc_prepare_failed_marks_aborted();
    test_2pc_all_prepared_false_initially();
    test_2pc_all_prepared_partial();
    test_2pc_all_prepared_true();
    test_2pc_commit_all_happy();
    test_2pc_commit_all_requires_all_prepared();
    test_2pc_rollback_all_from_prepared();
    test_2pc_rollback_all_mixed_states();
    test_2pc_gid_not_found();
    test_2pc_shard_state_unknown();
    test_2pc_reset_clears_participants();
    test_2pc_empty_mask();
    test_2pc_null_safety();
    test_2pc_full_happy_path();

    /* Phase 4.3 — full commit/abort/crash-recovery matrix */
    test_2pc_rollback_after_commit_is_noop_or_error();
    test_2pc_commit_after_rollback_fails();
    test_2pc_abort_partial_then_rollback();
    test_2pc_gid_deterministic();
    test_2pc_max_shards_boundary();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
