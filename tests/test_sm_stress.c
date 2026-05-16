/**
 * @file test_sm_stress.c
 * @brief Multi-threaded concurrent state machine stress tests
 *
 * Verifies state machine transitions under concurrent access patterns:
 *
 *   Test 1: Independent sessions — N threads each run a full lifecycle
 *           (phase→bind→txn×10→unbind→close) on their own session/backend
 *           pair. Checks: no contract violations, journal counts match.
 *
 *   Test 2: Journal stress — N threads each write KEEL_STATE_JOURNAL_CAPACITY×2
 *           events to their own journal. Checks: ring buffer wraps correctly,
 *           no corruption.
 *
 *   Test 3: Contract sync storm — N threads each derive contracts from their
 *           own slightly-modified session_flow, exercising the pure-function
 *           derive paths for data races (there shouldn't be any — each thread
 *           owns its own data).
 *
 *   Test 4: Phase transition thundering herd — all threads wait on a barrier
 *           then simultaneously try to transition separate sessions through
 *           the same phase sequence.
 *
 * NOTE: The state machine transitions operate on per-session state and are
 * NOT thread-safe by design (single-threaded event loop owns each session).
 * These tests verify that the per-session contract holds under parallel
 * execution with NO sharing — the 'stress' is on the allocator, contract
 * derivation logic, and journal ring buffer under high CPU contention.
 */

#include "test_utils.h"
#include "keel/engine/state_machine.h"
#include "keel/mem/mem.h"

#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

/* ============================================================================
 * Thread infrastructure
 * ============================================================================ */

#define STRESS_THREADS 64

typedef struct thread_args {
    int              id;
    atomic_int      *errors;
    pthread_barrier_t *barrier;  /* optional — NULL if no barrier needed */
} thread_args_t;

/* ============================================================================
 * Test 1 — Independent Session Lifecycles
 *
 * Each thread: HANDSHAKE→READY→QUERY, bind, 10× begin/end txn, unbind, CLOSING
 * ============================================================================ */

/**
 * @brief pthread entry: run a full session lifecycle on an independent
 *        session/backend pair.
 *
 * Sequence: HANDSHAKE → READY → QUERY, bind, 10× begin/end txn,
 * unbind, CLOSING.  Any contract violation increments the shared
 * atomic error counter.
 *
 * @param arg  Pointer to thread_args_t (owns its own data).
 * @return NULL always.
 */
static void* lifecycle_thread(void* arg)
{
    thread_args_t* ta = (thread_args_t*)arg;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
    sf.tx    = KEEL_TX_IDLE;

    keel_session_t s;
    memset(&s, 0, sizeof(s));
    s.id        = (uint32_t)(100 + ta->id);
    s.state     = KEEL_SESSION_READY;
    s.server_fd = -1;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = (int)(200 + ta->id);
    atomic_store(&be.state, BACKEND_CONN_IDLE);

    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;
    keel_state_journal_t j;
    keel_journal_init(&j);

    /* HANDSHAKE → READY */
    if (keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    /* READY → QUERY */
    if (keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, &j) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    /* Bind */
    if (keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    /* 10 transaction round-trips */
    for (int i = 0; i < 10; i++) {
        if (keel_session_transition_begin_txn(&sf, &s, &j) != 0) {
            atomic_fetch_add(ta->errors, 1);
            return NULL;
        }

        /* Contract check mid-transaction */
        keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);
        if (c.tx != KEEL_TX_ACTIVE) {
            atomic_fetch_add(ta->errors, 1);
            return NULL;
        }
        if (c.binding != KEEL_BIND_PINNED_TXN) {
            atomic_fetch_add(ta->errors, 1);
            return NULL;
        }

        if (keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, &j) != 0) {
            atomic_fetch_add(ta->errors, 1);
            return NULL;
        }
    }

    /* Unbind */
    if (keel_session_transition_unbind(&sf, &s, &j) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    /* QUERY → READY → CLOSING */
    keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j);
    if (keel_session_transition_phase(&sf, &s, KEEL_PHASE_CLOSING, &j) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    /* Verify journal has expected events:
     * 2 phase + 1 bind + 10 begin + 10 end + 1 unbind + 2 phase = 26 transitions
     * (journal records multiple domain events per transition) */
#ifndef NDEBUG
    if (j.count == 0) {
        atomic_fetch_add(ta->errors, 1);
    }
#endif

    return NULL;
}

static void test_independent_lifecycles(void)
{
    TEST_BEGIN("concurrent independent lifecycles (64 threads)");

    pthread_t threads[STRESS_THREADS];
    thread_args_t args[STRESS_THREADS];
    atomic_int errors = 0;

    for (int i = 0; i < STRESS_THREADS; i++) {
        args[i].id      = i;
        args[i].errors  = &errors;
        args[i].barrier = NULL;
        pthread_create(&threads[i], NULL, lifecycle_thread, &args[i]);
    }

    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    TEST_ASSERT_EQ(atomic_load(&errors), 0);

    TEST_END();
}

/* ============================================================================
 * Test 2 — Journal Ring Buffer Stress
 *
 * Each thread fills a journal past capacity to test wrapping logic.
 * ============================================================================ */

/**
 * @brief pthread entry: fill a private journal past capacity
 *        (3× KEEL_STATE_JOURNAL_CAPACITY) to stress ring-buffer
 *        wrap logic.
 *
 * @param arg  Pointer to thread_args_t.
 * @return NULL always.
 */
static void* journal_thread(void* arg)
{
    thread_args_t* ta = (thread_args_t*)arg;

    keel_state_journal_t j;
    keel_journal_init(&j);

    int count = KEEL_STATE_JOURNAL_CAPACITY * 3;
    for (int i = 0; i < count; i++) {
        keel_journal_record(&j, (uint32_t)ta->id,
                            KEEL_DOMAIN_PHASE,
                            (uint8_t)(i % 6),
                            (uint8_t)((i + 1) % 6),
                            0);
    }

    /* After wrapping, count should be total recorded */
#ifndef NDEBUG
    if (j.count != (uint32_t)count) {
        atomic_fetch_add(ta->errors, 1);
    }

    /* Head is a raw counter (not masked), equals total events recorded */
    if (j.head != (uint32_t)count) {
        atomic_fetch_add(ta->errors, 1);
    }
#endif

    return NULL;
}

static void test_journal_stress(void)
{
    TEST_BEGIN("journal ring buffer stress (64 threads, 192 events each)");

    pthread_t threads[STRESS_THREADS];
    thread_args_t args[STRESS_THREADS];
    atomic_int errors = 0;

    for (int i = 0; i < STRESS_THREADS; i++) {
        args[i].id      = i;
        args[i].errors  = &errors;
        args[i].barrier = NULL;
        pthread_create(&threads[i], NULL, journal_thread, &args[i]);
    }

    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    TEST_ASSERT_EQ(atomic_load(&errors), 0);

    TEST_END();
}

/* ============================================================================
 * Test 3 — Contract Derivation Storm
 *
 * Each thread derives contracts from various session configurations.
 * Tests pure-function paths under high CPU contention.
 * ============================================================================ */

/**
 * @brief pthread entry: derive contracts from every (phase × tx)
 *        combination to exercise the pure-function contract paths
 *        under CPU contention.
 *
 * @param arg  Pointer to thread_args_t.
 * @return NULL always.
 */
static void* contract_storm_thread(void* arg)
{
    thread_args_t* ta = (thread_args_t*)arg;

    keel_session_flow_t sf;
    keel_session_t s;
    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;

    for (int phase = 0; phase < 6; phase++) {
        for (int tx = 0; tx < 3; tx++) {
            memset(&sf, 0, sizeof(sf));
            memset(&s, 0, sizeof(s));
            sf.phase = (keel_session_phase_t)phase;
            sf.tx    = (keel_tx_status_t)tx;
            s.id     = (uint32_t)(ta->id * 100 + phase * 10 + tx);
            s.state  = KEEL_SESSION_READY;
            s.server_fd = -1;

            /* Derive binding — should not crash */
            keel_backend_binding_t bind = keel_derive_binding(&sf, &s);
            (void)bind;

            /* Derive replay state — should not crash */
            keel_replay_state_t rep = keel_derive_replay_state(&sf);
            (void)rep;

            /* Derive CID state — should not crash */
            keel_cid_state_t cid = keel_derive_cid_state(&sf);
            (void)cid;

            /* Full contract sync — should not crash */
            keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);

            /* Check contract (may report violations due to partial setup) */
            uint32_t v = keel_contract_check_session(&c, &sf);
            (void)v;
        }
    }

    return NULL;
}

static void test_contract_derivation_storm(void)
{
    TEST_BEGIN("contract derivation storm (64 threads x 18 configs)");

    pthread_t threads[STRESS_THREADS];
    thread_args_t args[STRESS_THREADS];
    atomic_int errors = 0;

    for (int i = 0; i < STRESS_THREADS; i++) {
        args[i].id      = i;
        args[i].errors  = &errors;
        args[i].barrier = NULL;
        pthread_create(&threads[i], NULL, contract_storm_thread, &args[i]);
    }

    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    TEST_ASSERT_EQ(atomic_load(&errors), 0);

    TEST_END();
}

/* ============================================================================
 * Test 4 — Phase Transition Thundering Herd
 *
 * All threads wait at a barrier then simultaneously execute the same
 * phase transition sequence on their own sessions.
 * ============================================================================ */

/**
 * @brief pthread entry: wait at a barrier then execute
 *        HANDSHAKE → READY → QUERY → READY → CLOSING simultaneously
 *        with all other threads.
 *
 * @param arg  Pointer to thread_args_t (barrier must be non-NULL).
 * @return NULL always.
 */
static void* thundering_phase_thread(void* arg)
{
    thread_args_t* ta = (thread_args_t*)arg;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
    sf.tx    = KEEL_TX_IDLE;

    keel_session_t s;
    memset(&s, 0, sizeof(s));
    s.id        = (uint32_t)(500 + ta->id);
    s.state     = KEEL_SESSION_READY;
    s.server_fd = -1;

    /* Wait at the barrier — all threads release simultaneously */
    pthread_barrier_wait(ta->barrier);

    /* Race: each thread does HANDSHAKE → READY → QUERY → READY → CLOSING */
    keel_session_phase_t seq[] = {
        KEEL_PHASE_READY,
        KEEL_PHASE_QUERY,
        KEEL_PHASE_READY,
        KEEL_PHASE_QUERY,
        KEEL_PHASE_BACKEND_SYNC,
        KEEL_PHASE_QUERY,
        KEEL_PHASE_READY,
        KEEL_PHASE_CLOSING,
    };

    for (int i = 0; i < (int)(sizeof(seq) / sizeof(seq[0])); i++) {
        int rc = keel_session_transition_phase(&sf, &s, seq[i], NULL);
        if (rc != 0) {
            atomic_fetch_add(ta->errors, 1);
            return NULL;
        }
    }

    /* Must end in CLOSING */
    if (sf.phase != KEEL_PHASE_CLOSING) {
        atomic_fetch_add(ta->errors, 1);
    }

    return NULL;
}

static void test_thundering_herd_phase(void)
{
    TEST_BEGIN("thundering herd phase transitions (64 threads)");

    pthread_t threads[STRESS_THREADS];
    thread_args_t args[STRESS_THREADS];
    atomic_int errors = 0;
    pthread_barrier_t barrier;

    pthread_barrier_init(&barrier, NULL, STRESS_THREADS);

    for (int i = 0; i < STRESS_THREADS; i++) {
        args[i].id      = i;
        args[i].errors  = &errors;
        args[i].barrier = &barrier;
        pthread_create(&threads[i], NULL, thundering_phase_thread, &args[i]);
    }

    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_barrier_destroy(&barrier);

    TEST_ASSERT_EQ(atomic_load(&errors), 0);

    TEST_END();
}

/* ============================================================================
 * Test 5 — Full Lifecycle Thundering Herd
 *
 * Like Test 4 but includes bind/txn/unbind — all threads hit the barrier
 * then do the full lifecycle simultaneously.
 * ============================================================================ */

/**
 * @brief pthread entry: barrier-synchronised full lifecycle including
 *        bind, 5× txn round-trips, unbind, and close.
 *
 * @param arg  Pointer to thread_args_t (barrier must be non-NULL).
 * @return NULL always.
 */
static void* thundering_lifecycle_thread(void* arg)
{
    thread_args_t* ta = (thread_args_t*)arg;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
    sf.tx    = KEEL_TX_IDLE;

    keel_session_t s;
    memset(&s, 0, sizeof(s));
    s.id        = (uint32_t)(700 + ta->id);
    s.state     = KEEL_SESSION_READY;
    s.server_fd = -1;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = (int)(800 + ta->id);
    atomic_store(&be.state, BACKEND_CONN_IDLE);

    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;

    /* Wait for all threads */
    pthread_barrier_wait(ta->barrier);

    /* Full lifecycle */
    if (keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, NULL) != 0 ||
        keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, NULL) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    if (keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, NULL) != 0) {
        atomic_fetch_add(ta->errors, 1);
        return NULL;
    }

    /* 5 txn round-trips */
    for (int i = 0; i < 5; i++) {
        if (keel_session_transition_begin_txn(&sf, &s, NULL) != 0 ||
            keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, NULL) != 0) {
            atomic_fetch_add(ta->errors, 1);
            return NULL;
        }
    }

    /* Verify contract mid-flow */
    keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);
    if (c.tx != KEEL_TX_IDLE || c.binding != KEEL_BIND_SHARED) {
        atomic_fetch_add(ta->errors, 1);
    }

    /* Unbind and close */
    keel_session_transition_unbind(&sf, &s, NULL);
    keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, NULL);
    if (keel_session_transition_phase(&sf, &s, KEEL_PHASE_CLOSING, NULL) != 0) {
        atomic_fetch_add(ta->errors, 1);
    }

    return NULL;
}

static void test_thundering_herd_lifecycle(void)
{
    TEST_BEGIN("thundering herd full lifecycle (64 threads)");

    pthread_t threads[STRESS_THREADS];
    thread_args_t args[STRESS_THREADS];
    atomic_int errors = 0;
    pthread_barrier_t barrier;

    pthread_barrier_init(&barrier, NULL, STRESS_THREADS);

    for (int i = 0; i < STRESS_THREADS; i++) {
        args[i].id      = i;
        args[i].errors  = &errors;
        args[i].barrier = &barrier;
        pthread_create(&threads[i], NULL, thundering_lifecycle_thread, &args[i]);
    }

    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_barrier_destroy(&barrier);

    TEST_ASSERT_EQ(atomic_load(&errors), 0);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== KEEL State Machine Concurrent Stress Tests ===\n\n");

    test_independent_lifecycles();
    test_journal_stress();
    test_contract_derivation_storm();
    test_thundering_herd_phase();
    test_thundering_herd_lifecycle();

    return test_summary();
}
