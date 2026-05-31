/**
 * @file worker_catchup_internal.h
 * @brief Internal types shared between the catch-up manager and the
 *        per-protocol probe state machines.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Not a public header: the layout of `keel_catchup_manager`, the wait
 * list, the probe-socket cache, and the result cache is an implementation
 * detail. The PG and MySQL probe state machines (worker_catchup_pg.c,
 * worker_catchup_my.c) need direct access to those fields so the manager
 * can stay free of protocol headers.
 */

#ifndef KEEL_WORKER_CATCHUP_INTERNAL_H
#define KEEL_WORKER_CATCHUP_INTERNAL_H

#include "keel/engine/catchup.h"
#include "keel/engine/engine.h"   /* KEEL_MAX_SERVERS */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef KEEL_CATCHUP_CACHE_SLOTS
#define KEEL_CATCHUP_CACHE_SLOTS 128
#endif

#ifdef __cplusplus
extern "C" {
#endif

/** One parked session. Owned by the manager's intrusive doubly-linked list. */
struct keel_catchup_waiter {
    keel_catchup_waiter_t*   next;
    keel_catchup_waiter_t*   prev;

    struct keel_session*     session;
    size_t                   server_index;
    keel_consistency_token_t token;

    uint64_t                 enqueued_ns;
    uint64_t                 deadline_ns;

    keel_catchup_resume_cb   resume;
    void*                    userdata;

    bool                     released;
};

/** Cached probe result for "did server S reach token T?" with a TTL. */
typedef struct keel_catchup_cache_entry {
    /* `server_index == SIZE_MAX` means slot is empty. */
    size_t                   server_index;
    keel_consistency_token_t token;
    bool                     reached;
    uint64_t                 expires_ns;
} keel_catchup_cache_entry_t;

/** Per-server probe socket state.
 *
 *  `probe_state` is owned by the per-protocol state machine
 *  (e.g. `pg_probe_ctx_t` in worker_catchup_pg.c). The manager treats
 *  it as opaque and only frees it via the `probe_state_free` callback.
 */
typedef struct keel_catchup_probe_socket {
    int      fd;                     /**< -1 = not currently open */
    uint64_t opened_ns;
    uint64_t last_use_ns;
    uint64_t backoff_until_ns;       /**< Exponential-backoff window. */
    uint32_t backoff_current_ms;     /**< 0 until first failure. */
    void*    probe_state;            /**< pg_probe_ctx_t / my_probe_ctx_t */
    void   (*probe_state_free)(void*);
} keel_catchup_probe_socket_t;

struct keel_catchup_manager {
    struct keel_worker*           worker;       /* may be NULL (unit tests) */
    keel_catchup_config_t         cfg;

    keel_catchup_waiter_t*        head;
    keel_catchup_waiter_t*        tail;
    size_t                        waiters_active;
    size_t                        waiters_high_water;

    keel_catchup_probe_socket_t   sockets[KEEL_MAX_SERVERS];

    keel_catchup_cache_entry_t    cache[KEEL_CATCHUP_CACHE_SLOTS];
    size_t                        cache_next_evict;

    /* Aggregate counters mirrored to the worker's stats_ctx. */
    uint64_t enqueued_total;
    uint64_t fulfilled_total;
    uint64_t timeout_total;
    uint64_t cancelled_total;
    uint64_t probes_issued_total;
    uint64_t probes_succeeded_total;
    uint64_t probes_failed_total;
    uint64_t cache_hits_total;
};

/* ============================================================================
 * Helpers exported to probe state machines
 * ============================================================================ */

/**
 * @brief Iterate parked waiters for one server, releasing any whose token
 *        is satisfied by @p reached_token (assuming monotonic LSN order).
 *
 * Called by the PG/MySQL probe state machines after a successful probe
 * reply has been parsed. The probe SM passes the highest token it just
 * confirmed reached on @p server_index; this function then walks the
 * wait list and fires `KEEL_CATCHUP_REACHED` for every waiter whose
 * token is `<=` it under the protocol-specific comparator
 * `is_satisfied_by(waiter_token, reached_token)`.
 *
 * @param m              Manager.
 * @param server_index   Replica whose probe just succeeded.
 * @param reached_token  Token the probe confirmed reached on this replica.
 * @param now_ns         Caller-supplied monotonic timestamp.
 * @param is_satisfied_by  Protocol comparator. Returns true when @p
 *                       waiter is satisfied by @p reached.
 * @return Number of waiters released.
 */
size_t keel_catchup_release_satisfied(
    struct keel_catchup_manager* m,
    size_t server_index,
    const keel_consistency_token_t* reached_token,
    uint64_t now_ns,
    bool (*is_satisfied_by)(const keel_consistency_token_t* waiter,
                            const keel_consistency_token_t* reached));

/**
 * @brief Insert (or refresh) a probe-result cache entry.
 *
 * The TTL comes from `m->cfg.cache_ttl_ms`.
 */
void keel_catchup_cache_put(struct keel_catchup_manager* m,
                            size_t server_index,
                            const keel_consistency_token_t* token,
                            bool reached,
                            uint64_t now_ns);

/**
 * @brief Find the oldest parked waiter for one server.
 *
 * Returns NULL if no waiter for that server is parked. Does not release.
 */
struct keel_catchup_waiter* keel_catchup_first_waiter_for(
    struct keel_catchup_manager* m,
    size_t server_index);

/**
 * @brief Pick the "highest" (most strict) token across all parked waiters
 *        for one server using @p compare.
 *
 * @return true if at least one waiter exists for @p server_index, with
 *         @p out_token populated. false otherwise.
 */
bool keel_catchup_pick_probe_token(
    struct keel_catchup_manager* m,
    size_t server_index,
    int (*compare)(const keel_consistency_token_t* a,
                   const keel_consistency_token_t* b),
    keel_consistency_token_t* out_token);

/**
 * @brief Apply exponential backoff to a probe socket after a failure.
 *
 * Doubles the current window up to `cfg.probe_backoff_max_ms`. Should be
 * called by the probe SM when it transitions to a FAILED state.
 */
void keel_catchup_apply_backoff(struct keel_catchup_manager* m,
                                size_t server_index,
                                uint64_t now_ns);

/* ============================================================================
 * Per-protocol drivers (called from keel_catchup_manager_tick)
 * ============================================================================ */

/**
 * @brief Drive the PostgreSQL probe state machine for one server.
 *
 * Idempotent and non-blocking: if a probe is already in flight, in
 * backoff, or there are no parked waiters, returns immediately.
 *
 * Defined in worker_catchup_pg.c.
 */
void keel_catchup_pg_drive(struct keel_catchup_manager* m,
                           size_t server_index,
                           uint64_t now_ns);

/**
 * @brief Tear down the PostgreSQL probe socket for one server.
 *
 * Closes the probe connection, frees the per-server probe state, and
 * destroys the synthetic mini-pool. Safe to call when nothing is
 * allocated. Defined in worker_catchup_pg.c.
 */
void keel_catchup_pg_close(struct keel_catchup_manager* m,
                           size_t server_index);

/**
 * @brief Test-only hook: inject a pre-authenticated probe socket for
 *        one server, bypassing CONNECT + SCRAM.
 *
 * Allocates the per-server probe context, takes ownership of @p fd
 * (the manager will close it on destroy), registers it with the
 * worker's reactor, and parks the SM in `READY` so the very next
 * `keel_catchup_pg_drive` call goes straight to QUERY_SEND.
 *
 * Intended exclusively for socketpair-based unit tests of the PG
 * probe round (see tests/test_catchup_pg_mock.c). Production code
 * MUST NOT call this — it skips authentication.
 *
 * @return 0 on success, -1 on allocation failure or bad inputs.
 *
 * Defined in worker_catchup_pg.c.
 */
int keel_catchup_pg_test_inject_ready(struct keel_catchup_manager* m,
                                      size_t server_index,
                                      int fd);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_WORKER_CATCHUP_INTERNAL_H */
