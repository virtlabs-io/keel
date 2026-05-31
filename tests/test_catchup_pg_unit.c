/**
 * @file test_catchup_pg_unit.c
 * @brief Unit tests for the PostgreSQL catch-up probe state machine —
 *        pure helpers and manager-helper integration (Phase 2b-test).
 *
 * Covers two layers without spinning a worker, a reactor, or a real
 * backend:
 *
 *   1. The transport-free PG helpers in worker_catchup_pg_helpers.h:
 *      - `pg_lsn_parse`           — LSN string → uint64_t
 *      - `pg_lsn_token_is_safe`   — defends the Q-message against injection
 *      - `pg_token_compare`       — total order on (timeline, LSN)
 *      - `pg_token_satisfied_by`  — release predicate
 *
 *   2. The manager helpers exposed to the per-protocol drivers via
 *      worker_catchup_internal.h:
 *      - `keel_catchup_release_satisfied` — batched release of parked waiters
 *      - `keel_catchup_pick_probe_token`  — picks the strictest token
 *      - `keel_catchup_apply_backoff`     — exponential escalation up to cap
 *      - `keel_catchup_cache_put`         — short-circuits future enqueues
 *
 * The full state-machine handshake (CONNECT → SCRAM → QUERY round) is
 * exercised in a separate socketpair-mock test (test_catchup_pg_mock.c)
 * and end-to-end against real Postgres in tests/integration/.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

#include "../src/worker/worker_catchup_internal.h"
#include "../src/worker/worker_catchup_pg_helpers.h"

#include "keel/engine/catchup.h"
#include "keel/plugin/plugin_types.h"
#include "keel/util/util.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* The manager never dereferences the session pointer in unit-test mode. */
static int dummy_session_sentinel;

typedef struct {
    keel_catchup_outcome_t outcome;
    int                    call_count;
} resume_capture_t;

static void capture_cb(struct keel_session* s,
                       keel_catchup_outcome_t outcome,
                       void* userdata)
{
    (void)s;
    resume_capture_t* cap = (resume_capture_t*)userdata;
    cap->outcome = outcome;
    cap->call_count++;
}

static keel_consistency_token_t make_token(const char* v, uint32_t tl)
{
    keel_consistency_token_t t = {0};
    if (v) strncpy(t.value, v, sizeof t.value - 1);
    t.timeline_id = tl;
    return t;
}

/* ==========================================================================
 * pg_lsn_parse
 * ==========================================================================*/
static void test_pg_lsn_parse(void)
{
    TEST_BEGIN("pg_lsn_parse: valid and invalid inputs");
    uint64_t v = 0;

    TEST_ASSERT(pg_lsn_parse("0/16B3740", &v));
    TEST_ASSERT_EQ(v, (uint64_t)0x16B3740ULL);

    TEST_ASSERT(pg_lsn_parse("1A/0", &v));
    TEST_ASSERT_EQ(v, ((uint64_t)0x1A << 32));

    TEST_ASSERT(pg_lsn_parse("FFFFFFFF/FFFFFFFF", &v));
    TEST_ASSERT_EQ(v, (uint64_t)0xFFFFFFFFFFFFFFFFULL);

    /* Rejects. */
    TEST_ASSERT(!pg_lsn_parse(NULL, &v));
    TEST_ASSERT(!pg_lsn_parse("", &v));
    TEST_ASSERT(!pg_lsn_parse("0/16B3740", NULL));
    TEST_ASSERT(!pg_lsn_parse("nope", &v));
    /* `sscanf("%x")` is lenient — "0" alone parses one number, not two,
     * so we should reject it. */
    TEST_ASSERT(!pg_lsn_parse("0", &v));
    TEST_END();
}

/* ==========================================================================
 * pg_lsn_token_is_safe — the injection-defense gate
 * ==========================================================================*/
static void test_pg_lsn_token_is_safe(void)
{
    TEST_BEGIN("pg_lsn_token_is_safe: accepts grammar, rejects injection");

    /* Accept. */
    TEST_ASSERT(pg_lsn_token_is_safe("0/0"));
    TEST_ASSERT(pg_lsn_token_is_safe("0/16B3740"));
    TEST_ASSERT(pg_lsn_token_is_safe("ABCDEF/abcdef"));
    TEST_ASSERT(pg_lsn_token_is_safe("FFFFFFFF/FFFFFFFF"));

    /* Reject. */
    TEST_ASSERT(!pg_lsn_token_is_safe(NULL));
    TEST_ASSERT(!pg_lsn_token_is_safe(""));
    TEST_ASSERT(!pg_lsn_token_is_safe("0"));               /* no slash */
    TEST_ASSERT(!pg_lsn_token_is_safe("/0"));              /* empty left */
    TEST_ASSERT(!pg_lsn_token_is_safe("0/"));              /* empty right */
    TEST_ASSERT(!pg_lsn_token_is_safe("0/0/0"));           /* double slash */
    TEST_ASSERT(!pg_lsn_token_is_safe("0 / 0"));           /* whitespace */
    TEST_ASSERT(!pg_lsn_token_is_safe("0/0;DROP TABLE t"));/* injection */
    TEST_ASSERT(!pg_lsn_token_is_safe("0/0'--"));          /* quote */
    TEST_ASSERT(!pg_lsn_token_is_safe("0/0\n"));           /* newline */
    TEST_ASSERT(!pg_lsn_token_is_safe("g/0"));             /* non-hex */
    TEST_ASSERT(!pg_lsn_token_is_safe("0/g"));             /* non-hex right */
    TEST_END();
}

/* ==========================================================================
 * pg_token_compare — totality, antisymmetry, timeline ordering
 * ==========================================================================*/
static void test_pg_token_compare(void)
{
    TEST_BEGIN("pg_token_compare: total ordering");

    keel_consistency_token_t a = make_token("0/100", 1);
    keel_consistency_token_t b = make_token("0/200", 1);
    keel_consistency_token_t c = make_token("0/100", 1);
    keel_consistency_token_t d = make_token("0/100", 2);  /* later timeline */

    TEST_ASSERT_EQ(pg_token_compare(&a, &b), -1);
    TEST_ASSERT_EQ(pg_token_compare(&b, &a),  1);
    TEST_ASSERT_EQ(pg_token_compare(&a, &c),  0);

    /* Timeline takes precedence over LSN. */
    TEST_ASSERT_EQ(pg_token_compare(&a, &d), -1);
    TEST_ASSERT_EQ(pg_token_compare(&d, &b),  1);

    /* Unparseable LSN treated as 0 (still total). */
    keel_consistency_token_t bad = make_token("garbage", 1);
    TEST_ASSERT_EQ(pg_token_compare(&bad, &a), -1);
    TEST_ASSERT_EQ(pg_token_compare(&bad, &bad), 0);
    TEST_END();
}

/* ==========================================================================
 * pg_token_satisfied_by — the release predicate
 * ==========================================================================*/
static void test_pg_token_satisfied_by(void)
{
    TEST_BEGIN("pg_token_satisfied_by: same timeline + LSN <= reached");

    keel_consistency_token_t reached = make_token("0/200", 1);

    keel_consistency_token_t lower  = make_token("0/100", 1);
    keel_consistency_token_t equal  = make_token("0/200", 1);
    keel_consistency_token_t higher = make_token("0/300", 1);
    keel_consistency_token_t newtl  = make_token("0/100", 2);
    keel_consistency_token_t bad    = make_token("garbage", 1);

    TEST_ASSERT(pg_token_satisfied_by(&lower,  &reached));
    TEST_ASSERT(pg_token_satisfied_by(&equal,  &reached));
    TEST_ASSERT(!pg_token_satisfied_by(&higher, &reached));
    /* Different timeline never satisfies — must wait for fresh probe. */
    TEST_ASSERT(!pg_token_satisfied_by(&newtl, &reached));
    /* Unparseable waiter token never released (fail-safe). */
    TEST_ASSERT(!pg_token_satisfied_by(&bad, &reached));
    TEST_END();
}

/* ==========================================================================
 * keel_catchup_pick_probe_token — picks the strictest parked token
 * ==========================================================================*/
static void test_pick_probe_token(void)
{
    TEST_BEGIN("pick_probe_token: returns highest LSN across waiters");

    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);
    TEST_ASSERT_NOT_NULL(m);

    keel_consistency_token_t out;
    /* Empty manager — picker reports no waiter. */
    TEST_ASSERT(!keel_catchup_pick_probe_token(m, 0, pg_token_compare, &out));

    resume_capture_t caps[3] = {0};
    keel_consistency_token_t t_lo = make_token("0/100", 1);
    keel_consistency_token_t t_md = make_token("0/200", 1);
    keel_consistency_token_t t_hi = make_token("0/300", 1);

    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &t_md,
        60000, capture_cb, &caps[0]));
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &t_hi,
        60000, capture_cb, &caps[1]));
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &t_lo,
        60000, capture_cb, &caps[2]));
    /* Different server — must NOT influence the pick for server 0. */
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 1,
        &(keel_consistency_token_t){.value = "FF/FF", .timeline_id = 1},
        60000, capture_cb, &caps[2]));

    TEST_ASSERT(keel_catchup_pick_probe_token(m, 0, pg_token_compare, &out));
    TEST_ASSERT_STR_EQ(out.value, "0/300");
    TEST_ASSERT_EQ(out.timeline_id, (uint32_t)1);

    /* Nothing on a server with no parked waiter. */
    TEST_ASSERT(!keel_catchup_pick_probe_token(m, 99, pg_token_compare, &out));

    /* Defensive NULLs. */
    TEST_ASSERT(!keel_catchup_pick_probe_token(NULL, 0, pg_token_compare, &out));
    TEST_ASSERT(!keel_catchup_pick_probe_token(m, 0, NULL, &out));
    TEST_ASSERT(!keel_catchup_pick_probe_token(m, 0, pg_token_compare, NULL));

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* ==========================================================================
 * keel_catchup_release_satisfied — batched fulfilment
 * ==========================================================================*/
static void test_release_satisfied(void)
{
    TEST_BEGIN("release_satisfied: only waiters with LSN <= reached fire");

    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);
    TEST_ASSERT_NOT_NULL(m);

    resume_capture_t caps[5] = {0};
    const char* lsns[5] = { "0/100", "0/150", "0/200", "0/250", "0/300" };
    for (int i = 0; i < 5; i++) {
        keel_consistency_token_t tok = make_token(lsns[i], 1);
        TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
            m, (struct keel_session*)&dummy_session_sentinel, 3, &tok,
            60000, capture_cb, &caps[i]));
    }
    /* Plus one waiter on a different server that must never be released. */
    resume_capture_t other = {0};
    keel_consistency_token_t other_tok = make_token("0/050", 1);
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 7, &other_tok,
        60000, capture_cb, &other));

    /* Probe reports 0/200 reached on server 3. */
    keel_consistency_token_t reached = make_token("0/200", 1);
    size_t n = keel_catchup_release_satisfied(
        m, 3, &reached, (uint64_t)keel_time_now(), pg_token_satisfied_by);
    TEST_ASSERT_EQ(n, (size_t)3);
    TEST_ASSERT_EQ(caps[0].call_count, 1);
    TEST_ASSERT_EQ(caps[0].outcome, KEEL_CATCHUP_REACHED);
    TEST_ASSERT_EQ(caps[1].call_count, 1);
    TEST_ASSERT_EQ(caps[2].call_count, 1);
    TEST_ASSERT_EQ(caps[3].call_count, 0);  /* 0/250 still parked */
    TEST_ASSERT_EQ(caps[4].call_count, 0);  /* 0/300 still parked */
    TEST_ASSERT_EQ(other.call_count, 0);    /* other server */

    /* Subsequent probe at 0/300 must release the remaining two. */
    keel_consistency_token_t reached2 = make_token("0/300", 1);
    n = keel_catchup_release_satisfied(
        m, 3, &reached2, (uint64_t)keel_time_now(), pg_token_satisfied_by);
    TEST_ASSERT_EQ(n, (size_t)2);
    TEST_ASSERT_EQ(caps[3].call_count, 1);
    TEST_ASSERT_EQ(caps[4].call_count, 1);

    /* Promoted-primary scenario: a probe on a newer timeline must
     * release nothing (waiter timelines no longer match). */
    keel_consistency_token_t new_tl = make_token("0/100", 2);
    n = keel_catchup_release_satisfied(
        m, 7, &new_tl, (uint64_t)keel_time_now(), pg_token_satisfied_by);
    TEST_ASSERT_EQ(n, (size_t)0);
    TEST_ASSERT_EQ(other.call_count, 0);

    /* Defensive NULLs. */
    TEST_ASSERT_EQ(keel_catchup_release_satisfied(
        NULL, 0, &reached2, 0, pg_token_satisfied_by), (size_t)0);
    TEST_ASSERT_EQ(keel_catchup_release_satisfied(
        m, 0, NULL, 0, pg_token_satisfied_by), (size_t)0);
    TEST_ASSERT_EQ(keel_catchup_release_satisfied(
        m, 0, &reached2, 0, NULL), (size_t)0);

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* ==========================================================================
 * keel_catchup_apply_backoff — exponential escalation up to cap
 * ==========================================================================*/
static void test_apply_backoff_escalation(void)
{
    TEST_BEGIN("apply_backoff: 50 → 100 → 200 → ... → cap, monotonic");

    keel_catchup_config_t cfg = KEEL_CATCHUP_CONFIG_DEFAULT;
    cfg.probe_backoff_initial_ms = 50;
    cfg.probe_backoff_max_ms     = 800;  /* small cap so we hit it fast */
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, &cfg);
    TEST_ASSERT_NOT_NULL(m);

    /* Drive successive failures and observe the deadline grow then clamp.
     * Use a virtual clock so we can inspect the absolute deadline. */
    const uint64_t T0 = 1000ULL * 1000000ULL;  /* arbitrary 1 second base */
    const uint32_t expected_ms[] = { 50, 100, 200, 400, 800, 800, 800 };
    uint64_t prev_until = 0;

    for (size_t i = 0; i < sizeof expected_ms / sizeof *expected_ms; i++) {
        keel_catchup_apply_backoff(m, /*server*/ 2, T0);
        /* Read the slot through a public-ish accessor: we know the cache
         * helper has no read API, so we just probe the manager's stats —
         * the failure counter must climb by one each call. */
        keel_catchup_stats_snapshot_t snap;
        keel_catchup_manager_snapshot(m, &snap);
        TEST_ASSERT_EQ(snap.probes_failed_total, (uint64_t)(i + 1));

        /* Inspect the backoff state via the internal header. */
        keel_catchup_probe_socket_t* slot = &m->sockets[2];
        TEST_ASSERT_EQ(slot->backoff_current_ms, expected_ms[i]);
        TEST_ASSERT_EQ(slot->backoff_until_ns,
                       T0 + (uint64_t)expected_ms[i] * 1000000ULL);
        TEST_ASSERT(slot->backoff_until_ns >= prev_until);
        prev_until = slot->backoff_until_ns;
    }

    /* Defensive NULL / out-of-range. */
    keel_catchup_apply_backoff(NULL, 0, T0);
    keel_catchup_apply_backoff(m, (size_t)-1, T0);

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* ==========================================================================
 * keel_catchup_cache_put — fast-path short-circuit
 * ==========================================================================*/
static void test_cache_put_short_circuits_enqueue(void)
{
    TEST_BEGIN("cache_put: subsequent enqueue with reached=true returns inline");

    keel_catchup_config_t cfg = KEEL_CATCHUP_CONFIG_DEFAULT;
    cfg.cache_ttl_ms = 100;
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, &cfg);
    TEST_ASSERT_NOT_NULL(m);

    keel_consistency_token_t tok = make_token("0/16B3740", 1);
    keel_catchup_cache_put(m, /*server*/ 4, &tok, /*reached*/ true,
                           (uint64_t)keel_time_now());

    /* Re-enqueue same (server, token) — must NOT park; the public API
     * is `keel_catchup_enqueue` which is documented to consult the cache
     * and fire the callback inline on a hit. */
    resume_capture_t cap = {0};
    keel_catchup_waiter_t* w = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 4, &tok,
        60000, capture_cb, &cap);
    /* A NULL return means "did not park" — the callback should already
     * have fired with KEEL_CATCHUP_REACHED. */
    TEST_ASSERT_NULL(w);
    TEST_ASSERT_EQ(cap.call_count, 1);
    TEST_ASSERT_EQ(cap.outcome, KEEL_CATCHUP_REACHED);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.cache_hits_total, (uint64_t)1);
    TEST_ASSERT_EQ(snap.waiters_active,    (size_t)0);

    /* Defensive NULLs. */
    keel_catchup_cache_put(NULL, 0, &tok, true, 0);
    keel_catchup_cache_put(m, 0, NULL, true, 0);
    keel_catchup_cache_put(m, (size_t)-1, &tok, true, 0);

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* ==========================================================================
 * main
 * ==========================================================================*/
int main(void)
{
    test_pg_lsn_parse();
    test_pg_lsn_token_is_safe();
    test_pg_token_compare();
    test_pg_token_satisfied_by();
    test_pick_probe_token();
    test_release_satisfied();
    test_apply_backoff_escalation();
    test_cache_put_short_circuits_enqueue();
    return test_summary();
}
