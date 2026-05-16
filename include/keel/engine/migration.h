/**
 * @file migration.h
 * @brief Public API for cross-worker session migration and handoff.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Implements connection migration between worker threads.  When a session
 * is idle (KEEL_SESSION_READY, not in transaction, not pinned) it can be
 * handed from one worker to another without interrupting the client.
 *
 * ## Design
 *
 * Workers share nothing at runtime.  Migration bridges two workers using:
 *
 *   1. A **Unix socketpair** created once at engine init, one pair per
 *      (src, dst) direction.  The source worker calls `sendmsg()`+SCM_RIGHTS
 *      to pass the client FD to the destination worker.
 *
 *   2. An **SPSC ring buffer** (one per worker, receiving side) carries the
 *      serialised session state alongside the FD transfer.  Push is done by
 *      the source worker; drain is done by the owner (destination) worker.
 *
 *   3. An **eventfd** write wakes the destination worker's reactor so it
 *      drains its inbox promptly.
 *
 * ## Migration Window
 *
 * A session is eligible for migration only when ALL of the following hold:
 *   - state == KEEL_SESSION_READY
 *   - !session->in_transaction
 *   - pin_reason == KEEL_FPIN_NONE (no hard or soft pins)
 *   - no residual data pending on the client socket
 *   - backend_conn == NULL (connection returned to pool)
 *
 * ## Thread Safety
 *
 * keel_migration_send()  — called by source worker thread only
 * keel_migration_recv()  — called by destination worker thread only
 *
 * The ring buffer is SPSC; concurrent producers require higher-level
 * serialisation (not needed for single-threaded source workers).
 */

#ifndef KEEL_MIGRATION_H
#define KEEL_MIGRATION_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "keel/session/session.h"
#include "keel/mem/ringbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Configuration
 * ============================================================================ */

/** Maximum sessions queued in a single worker's migration inbox. */
#define KEEL_MIGRATION_INBOX_CAPACITY  64

/** Maximum residual bytes preserved across migration (abort if larger). */
#define KEEL_MIGRATION_MAX_RESIDUAL    (KEEL_RESIDUAL_INLINE_SIZE)

/* ============================================================================
 * Serialised Session State
 * ============================================================================
 * Carried through the ring buffer.  The client FD itself is transferred
 * out-of-band via SCM_RIGHTS on the socketpair; here we store everything
 * that the destination worker needs to reconstruct the session.
 */

typedef struct keel_migration_msg {
    /* ------------------------------------------------------------------
     * Identity & state
     * ------------------------------------------------------------------ */
    uint64_t            session_id;         /**< Original session ID */
    keel_session_state_t state;             /**< Must be KEEL_SESSION_READY */

    /* ------------------------------------------------------------------
     * User info
     * ------------------------------------------------------------------ */
    char                username[64];
    char                database[64];
    char                client_password[256];

    /* ------------------------------------------------------------------
     * Flags / timing
     * ------------------------------------------------------------------ */
    uint32_t            flags;              /**< KEEL_SESSION_FLAG_* */
    uint64_t            created_at;         /**< Original creation time (ns) */
    uint64_t            last_activity;      /**< Last I/O time (ns) */
    uint32_t            query_count;        /**< Queries processed so far */

    /* ------------------------------------------------------------------
     * Residual client data (bytes not yet processed)
     * ------------------------------------------------------------------ */
    uint8_t             residual[KEEL_MIGRATION_MAX_RESIDUAL];
    size_t              residual_len;

    /* ------------------------------------------------------------------
     * Hash of SET parameters (for backend state matching)
     * ------------------------------------------------------------------ */
    uint64_t            state_hash;

    /* ------------------------------------------------------------------
     * Source worker index (for diagnostics)
     * ------------------------------------------------------------------ */
    uint32_t            src_worker_id;
} keel_migration_msg_t;

/* ============================================================================
 * Per-Worker Migration State
 * ============================================================================ */

/**
 * @brief Migration channel — one per worker, owned by that worker.
 *
 * sock[0] is the receive end (this worker reads FDs from here).
 * sock[1] is kept by each sending worker to push FDs to this worker.
 *
 * Other workers call keel_migration_send() with a pointer to this
 * structure; they write to sock_send (= sock[1]).
 */
typedef struct keel_worker_migration {
    /* Inbound FD-transfer socket — recvmsg() end */
    int                  sock_recv;         /**< recvmsg() side (owner worker) */
    int                  sock_send;         /**< sendmsg() side (remote workers) */

    /* Inbound message queue (SPSC: many->one, but serialised per sender) */
    keel_spsc_ringbuf_t* inbox;             /**< Ring buffer of keel_migration_msg_t */

    /* Back-pointer to the owning worker (for eventfd wakeup) */
    int                  eventfd;           /**< Copy of owner's eventfd */
    uint32_t             worker_id;         /**< Owner worker index */

    /* Counters */
    uint64_t             received;          /**< Sessions received */
    uint64_t             sent;              /**< Sessions dispatched away */
    uint64_t             rejected;          /**< Refused (e.g., inbox full) */
} keel_worker_migration_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialise migration channel for a worker.
 *
 * Creates the socketpair and inbox ring buffer.
 * Called during worker init, before threads are started.
 *
 * @param mig       Migration channel to initialise.
 * @param worker_id Owning worker index.
 * @param eventfd   Worker's eventfd (for wakeup after push).
 * @return 0 on success, -1 on error.
 */
int keel_migration_init(keel_worker_migration_t* mig,
                        uint32_t worker_id,
                        int eventfd);

/**
 * @brief Destroy migration channel and release resources.
 *
 * Called during worker cleanup.
 */
void keel_migration_destroy(keel_worker_migration_t* mig);

/* ============================================================================
 * Send-Side (source worker)
 * ============================================================================ */

/**
 * @brief Check whether a session is eligible for migration.
 *
 * @param session Session to evaluate.
 * @return true if the session may be migrated.
 */
bool keel_migration_can_migrate(const keel_session_t* session);

/**
 * @brief Serialise and send a session to another worker.
 *
 * Transfers the client FD via SCM_RIGHTS on the destination's socketpair,
 * pushes the serialised state into the destination's inbox ring buffer, and
 * writes to the destination's eventfd to wake it.
 *
 * On success the caller must:
 *   1. Remove the session from the reactor (keel_reactor_unregister_fd).
 *   2. Return the backend connection to the pool (if any).
 *   3. Free the session slot back to the slab.
 *
 * @param session  Session to migrate (must satisfy keel_migration_can_migrate).
 * @param dst      Destination worker's migration channel.
 * @return 0 on success, -1 on error (session not migrated; caller keeps it).
 */
int keel_migration_send(keel_session_t*          session,
                        keel_worker_migration_t* dst);

/* ============================================================================
 * Receive-Side (destination worker)
 * ============================================================================ */

/**
 * @brief Drain the migration inbox and restore sessions.
 *
 * Called from the worker main loop when the eventfd fires or after each
 * reactor wait.  For each pending message, receives the FD via recvmsg(),
 * allocates a new session slot, restores state, and arms a recv on the
 * worker's reactor.
 *
 * @param mig      This worker's migration channel.
 * @param worker   Owning worker (for slab + reactor).
 * @return Number of sessions successfully adopted, -1 on fatal error.
 */
int keel_migration_drain(keel_worker_migration_t* mig,
                         struct keel_worker*      worker);

/* ============================================================================
 * Load Balancing Hint
 * ============================================================================ */

/**
 * @brief Select the least-loaded worker index for migration.
 *
 * Scans the worker pool (via engine) and returns the index of the worker
 * with the fewest active sessions.  Returns UINT32_MAX if no suitable
 * target is found (e.g., all workers equally loaded or pool not available).
 *
 * @param engine  Engine handle (for worker pool access).
 * @param src_id  Source worker index (excluded from candidates).
 * @return Target worker index, or UINT32_MAX.
 */
uint32_t keel_migration_find_target(struct keel_engine* engine,
                                    uint32_t src_id);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_MIGRATION_H */
