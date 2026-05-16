/**
 * @file engine_flow.h
 * @brief Protocol-agnostic session flow engine API and state container.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * The engine flow sits between the worker's I/O callbacks and the protocol
 * plugins. It never guesses framing or auth semantics — it asks the protocol.
 *
 * DATA PATH:
 *   worker recv callback → engine_flow_on_fe_data()
 *     → frame_len() loop → on_fe_msg() → dispatch action → I/O
 *   worker be recv callback → engine_flow_on_be_data()
 *     → frame_len() loop → on_be_msg() → dispatch action → I/O
 */

#ifndef KEEL_ENGINE_FLOW_H
#define KEEL_ENGINE_FLOW_H

#include "keel/protocol/protocol_flow.h"
#include "keel/plugin/plugin_types.h"
#include "keel/session/session.h"
#include "keel/session/ssv_atom.h"
#include "keel/engine/worker.h"
#include "keel/core/config.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward */
struct backend_pool;
struct backend_conn;

/* ============================================================================
 * Engine Flow Result
 * ============================================================================ */

typedef enum keel_flow_result {
    KEEL_FLOW_OK = 0,            /**< Continue (re-arm recv) */
    KEEL_FLOW_WAIT_BACKEND,      /**< Waiting for backend data — don't re-arm FE recv */
    KEEL_FLOW_WAIT_POOL,         /**< Queued waiting for backend from pool */
    KEEL_FLOW_SEND_PENDING,      /**< Async send needed — worker must flush pending_send via io_uring */
    KEEL_FLOW_LINKED_SEND,       /**< io_uring linked send+recv — worker chains SQEs for zero-syscall I/O */
    KEEL_FLOW_LINKED_FE_RESPONSE,/**< io_uring linked send(FE)+recv(FE) — deferred response + rearm */
    KEEL_FLOW_WAIT_STMT_REPLAY,  /**< Replaying prepared stmts to new backend before forwarding client msg */
    KEEL_FLOW_WAIT_COMMIT_CHECK, /**< Replication-tracking: checking txid_status() on new primary */
    KEEL_FLOW_SPLICE_BYPASS,     /**< Zero-copy: worker should peek+splice BE→FE until non-DataRow */
    KEEL_FLOW_WAIT_AUTH,         /**< Auth offloaded to thread — reactor must arm auth_notify_fd */
    KEEL_FLOW_CLOSED,            /**< Session closed — don't touch session anymore */
    KEEL_FLOW_ERROR,             /**< Error — session will be closed */
    KEEL_FLOW_TLS_HANDSHAKE,     /**< TLS accepted ('S' sent) — worker must drive handshake */
} keel_flow_result_t;

/* ============================================================================
 * Session Flow State (stored alongside session)
 * ============================================================================ */

typedef struct keel_session_flow {
    const keel_proto_flow_vtable_t* flow;    /**< Protocol flow vtable */
    void*                          ctx;     /**< Protocol flow context */
    keel_session_phase_t            phase;   /**< Current phase */
    keel_flow_pin_reason_t          pins;    /**< Active pin reasons */
    keel_tx_status_t                tx;      /**< Last tx status from backend */

    /** Prepared-statement pooling strategy for this session.
     *  Copied from worker config at flow init time. */
    keel_ps_mode_t                  ps_mode;

    /** Runtime mode tier — controls hot-path feature gating.
     *  Compared as (mode >= KEEL_MODE_X) for branch-free tier checks. */
    keel_tier_t                      mode;

    /* Pending message when waiting for pool (bounded queue support) */
    const uint8_t*                 pending_msg;     /**< Message waiting for backend */
    size_t                         pending_msg_len; /**< Pending message length */
    bool                           queued_for_pool; /**< True if waiting in pool queue */

    /* COPY IN fast-path state: tracks message boundaries across recv buffers
     * so the CopyDone/CopyFail scanner stays in sync.
     *
     * copy_skip:      Payload bytes to skip in next buffer before the next
     *                 message boundary (from a partial message tail).
     * copy_hdr_len:   Number of saved header bytes (0-4) when a message
     *                 header was split across buffer boundaries.
     * copy_hdr:       Saved partial header bytes (type + up to 3 length bytes).
     */
    size_t                         copy_skip;
    uint8_t                        copy_hdr_len;
    uint8_t                        copy_hdr[5];

    /* Sticky primary affinity: after a write, reads are routed to primary
     * for sticky_primary_ttl_ms to ensure read-after-write consistency */
    uint64_t                       last_write_ns;          /**< Timestamp of last write (monotonic ns) */
    uint32_t                       sticky_primary_ttl_ms;  /**< TTL for sticky affinity (0 = disabled) */

    /* Consistency token: WAL LSN (PG) or GTID (MySQL) captured after last
     * write, used for optional read-after-write consistency checks on replicas */
    keel_consistency_token_t        last_write_token;

    /* SSV consistency atoms: structured semantic state for the CONSISTENCY
     * domain.  Indexes match keel_ssv_consistency_key_t:
     *   [0] WRITE_LSN   — LSN string from last committed write
     *   [1] WRITE_LSN_TS — capture timestamp (monotonic ns)
     * These are per-session, per-worker (no locking needed). */
    keel_ssv_atom_t                 consistency_atoms[KEEL_SSV_CK__COUNT];

    /* SSV opaque atoms: unmodelled semantic dirtiness (DOMAIN_OPAQUE).
     * Deliberately separate from CONSISTENCY to keep domain semantics clean.
     *   [0] UNKNOWN_STATE — forces DISCARD ALL on pool return */
    keel_ssv_atom_t                 opaque_atoms[KEEL_SSV_OK__COUNT];

    /* SSV config atoms: SET variable state profile tracking (DOMAIN_CONFIG).
     *   [0] PROFILE_HASH — XXHash64 digest of current GUC k/v pairs */
    keel_ssv_atom_t                 config_atoms[KEEL_SSV_CFG__COUNT];

    /* Quarantine tracking: when a statement is POTENTIALLY_STATEFUL,
     * we hold the backend pinned until we confirm via backend response */
    keel_flow_pin_reason_t          quarantine_pending;     /**< Hard-pin reasons pending confirmation */

    /* Large message streaming: when a protocol message exceeds the recv
     * buffer, we forward the available portion and record the remaining
     * byte count.  On the next recv, the continuation data is forwarded
     * directly (no framing parse) until the counter reaches zero.
     *
     * fe_fwd_remaining:  bytes still to forward from FE → BE for current msg
     * be_fwd_remaining:  bytes still to forward from BE → FE for current msg
     * fe_fwd_wait_be:    true if we should return WAIT_BACKEND after draining
     *                    the FE continuation (i.e. the partial msg was a query)
     */
    size_t                         fe_fwd_remaining;
    size_t                         be_fwd_remaining;
    bool                           fe_fwd_wait_be;

    /* Prepared-statement replay state (spec §17 — PS Virtualization).
     *
     * When a session with named prepared statements gets a clean backend
     * (stmt_set_hash mismatch), the engine cannot forward the client's
     * Bind/Execute until the backend has parsed all the session's prepared
     * statements.  These fields track the in-progress replay:
     *
     *   stmt_replay_buf      Heap-alloc'd buffer: all Parse wire msgs concatenated
     *   stmt_replay_len      Total bytes in replay buf (0 = no replay in progress)
     *   stmt_replay_count    Number of ParseComplete responses still expected
     *   stmt_replay_orig_msg Original client message to forward after replay
     *   stmt_replay_orig_len Length of original client message
     *   stmt_replay_hash     Expected stmt_set_hash to stamp on backend after replay
     */
    uint8_t*                       stmt_replay_buf;
    size_t                         stmt_replay_len;
    uint32_t                       stmt_replay_count;
    const uint8_t*                 stmt_replay_orig_msg;
    size_t                         stmt_replay_orig_len;
    uint64_t                       stmt_replay_hash;
    bool                           stmt_replay_needs_discard; /**< True: waiting for DISCARD ALL ReadyForQuery
                                                               *   before sending replay Parse msgs.  Set when
                                                               *   backend was borrowed with a different stmt hash
                                                               *   (needs_discard_all), cleared after 'Z' arrives. */
    /* ---- Replication uncertainty tracking (spec §TXN-TRACK) ----
     *
     * When transaction_tracking = on, KEEL intercepts COMMIT queries and
     * rewrites them to "SELECT txid_current() AS _keel_txid; COMMIT;".
     * The back-end XID is captured before COMMIT executes, so that if the
     * backend connection dies between sending COMMIT and receiving
     * CommandComplete(COMMIT), KEEL can borrow a clean connection to the
     * new primary and call txid_status(xid) to determine the outcome.
     *
     * Happy path:  rewrite sent → DataRow(xid) absorbed → C(COMMIT) seen
     *              → commit_in_flight cleared.  No uncertainty.
     * Doubt path:  backend dies while commit_in_flight == true
     *              → commit_in_doubt = true → KEEL_FLOW_WAIT_COMMIT_CHECK
     *              → borrow primary connection → SELECT txid_status(xid)
     *              → synthesize COMMIT or error response to client.
     */
    bool     txn_tracking;          /**< transaction_tracking config enabled */
    bool     commit_in_flight;      /**< COMMIT forwarded; C(COMMIT) not yet seen */
    uint64_t pending_commit_xid;    /**< txid_current() captured before COMMIT (0=unknown) */
    bool     commit_in_doubt;       /**< backend died while commit_in_flight */
    uint64_t indoubt_xid;           /**< XID to check on new primary (0=unknown) */
    struct backend_conn* xid_check_conn; /**< borrowed pool conn for txid_status() check */
    /** Parsed result of txid_status() check:
     *  0=unknown/in_progress/NULL, 1=committed, 2=aborted */
    uint8_t  indoubt_check_result;

    /** When a write/DDL query is forwarded to the backend, set this flag
     *  instead of immediately calling capture_consistency_token() on the
     *  pool socket.  The pool socket is non-blocking at that point: recv()
     *  would return EAGAIN and the SELECT response would later leak into the
     *  client data stream causing protocol corruption.
     *
     *  The engine_flow BE path checks this flag after query_complete fires
     *  (backend is idle), temporarily sets the socket to blocking, calls
     *  capture_consistency_token(), and restores non-blocking. */
    bool     capture_lsn_pending;  /**< Capture LSN/GTID after next query_complete */
    bool     txn_had_writes;       /**< True if a write/DDL was forwarded inside an
                                    *   explicit BEGIN…COMMIT transaction.  Defers LSN
                                    *   capture to the COMMIT's query_complete so that
                                    *   the captured WAL position reflects committed data
                                    *   (not an uncommitted mid-transaction LSN).
                                    *   Cleared on BEGINS_TX and on capture. */

    bool                           stmt_replay_rfq_pending;   /**< True: all ParseCompletes for the replay have
                                                               *   been received but the ReadyForQuery generated
                                                               *   by the Sync appended to the replay buffer has
                                                               *   not yet been consumed.  We MUST drain this 'Z'
                                                               *   before forwarding orig_msg; otherwise it leaks
                                                               *   into the WAIT_BACKEND response stream and keel
                                                               *   erroneously treats it as an early end-of-txn. */
    uint32_t                       discard_skip_bytes;        /**< Bytes remaining in the current partially-
                                                               *   consumed backend message during DISCARD ALL
                                                               *   scanning.  Non-zero means: skip this many bytes
                                                               *   (continuation of a message whose header was seen
                                                               *   in a prior recv) before resuming message parsing. */

    /* Deferred send state: when a non-blocking send can't complete
     * immediately, the unsent data is saved here and the worker
     * flushes it via io_uring before following send_resume. */
    uint8_t                       *pending_send_buf;    /**< Heap-allocated unsent data (NULL = none) */
    size_t                         pending_send_len;    /**< Bytes remaining to send */
    size_t                         pending_send_off;    /**< Bytes already sent from pending_send_buf */
    size_t                         pending_send_cap;    /**< Allocated capacity of pending_send_buf */
    int                            pending_send_fd;     /**< Target fd (client_fd or server_fd) */
    keel_flow_result_t              pending_send_resume;  /**< Flow result to act on after send completes */

    /* io_uring linked send state: when the hot path wants to chain
     * send+recv as linked io_uring SQEs (zero inline send() syscalls),
     * these fields carry the payload.  The buffer points into the recv
     * buffer (valid until re-armed) — no copy needed. */
    const uint8_t*                 linked_send_buf;     /**< Payload to send (points into recv buffer) */
    size_t                         linked_send_len;     /**< Payload length */
    int                            linked_send_fd;      /**< Target fd (server_fd or client_fd) */
    keel_flow_result_t              linked_send_resume;  /**< What to do after linked send+recv completes */

    /* Protocol-path wait timing anchors (worker-side instrumentation). */
    uint64_t                       wait_pool_start_ns;    /**< 0 when not currently waiting for pool */
    uint64_t                       wait_backend_start_ns; /**< 0 when not currently waiting for backend */
    uint8_t                        wait_backend_kind;     /**< 0=none, 1=query, 2=replay, 3=discard */
    uint64_t                       wait_backend_query_recv_armed_ns; /**< Query wait: recv-arm timestamp (0 when immediate byte path) */
    uint64_t                       wait_backend_query_send_start_ns; /**< Query wait: deferred FE→BE send begin timestamp (0 when none) */

    /* Query result cache integration.
     * cache_pending is set by on_fe_data when a cacheable SELECT misses the
     * cache; cleared by on_be_data when the complete response arrives.
     * cache_capture_buf accumulates raw backend wire bytes across on_be_data
     * calls; stored into the cache when query_complete fires. */
    bool                           cache_pending;         /**< true: accumulate BE response for caching */
    uint8_t                        cache_digest[32];      /**< SHA-256 digest of current cacheable query */
    uint8_t*                       cache_capture_buf;     /**< heap-alloc'd accumulation buffer */
    size_t                         cache_capture_len;     /**< bytes captured so far */
    size_t                         cache_capture_cap;     /**< allocated capacity of cache_capture_buf */

    /* Cache write invalidation.
     * cache_inval_pending is set by on_fe_data when a write/DDL is dispatched;
     * at query_complete the write SQL is re-parsed to extract affected tables,
     * which are evicted from the query cache. */
    bool                           cache_inval_pending;   /**< Write/DDL dispatched; invalidate tables on query_complete */
    char*                          cache_inval_sql;       /**< Heap copy of write query text (NUL-terminated) */

    /* Deferred-BEGIN: when sharding is active and an explicit BEGIN arrives
     * before the target shard is known, KEEL synthesises the client response
     * immediately (CommandComplete/ReadyForQuery 'T') without acquiring a
     * backend.  The wire payload is buffered here.  On the next routable
     * DML/SELECT the buffered BEGIN is sent to the correct shard backend
     * first (blocking inline drain), then the actual query follows. */
    bool    begin_deferred;                  /**< true: a BEGIN is pending forwarding */
    uint8_t begin_deferred_payload[512];     /**< raw 'Q: BEGIN...' wire bytes */
    size_t  begin_deferred_payload_len;      /**< byte count (0 when begin_deferred=false) */

    /* Async pre-query replay (PR #4 — see docs/REACTOR_BLOCKING_INVENTORY.md
     * category A).  When set, the BE-side handler absorbs backend bytes until
     * it sees ReadyForQuery ('Z'), then forwards `pending_pre_query_buf` to
     * the backend before resuming normal flow.  This replaces the old
     * blocking BEGIN+drain inline loops in resume_from_pool and on_fe_data. */
    enum {
        KEEL_PRE_QUERY_NONE         = 0,
        KEEL_PRE_QUERY_BEGIN_REPLAY = 1,
    } pending_pre_query;
    uint8_t pending_pre_query_buf[KEEL_PRE_QUERY_REPLAY_BUFSZ]; /**< stashed FE payload */
    size_t  pending_pre_query_len;           /**< bytes valid in stash buffer */
    size_t  pending_pre_query_absorbed;      /**< BE bytes absorbed; runaway-cap */

    /** eventfd for async auth (KEEL_FLOW_WAIT_AUTH).
     *  Set to ≥0 while an off-thread auth operation is in flight;
     *  the reactor arms a read on this fd.  Reset to -1 after the
     *  result is consumed.  Owned by the auth context (closed there). */
    int     auth_notify_fd;
} keel_session_flow_t;

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Initialize session flow state.
 * Called when a new session is accepted.
 */
int keel_session_flow_init(keel_session_flow_t* sf,
                          const keel_proto_flow_vtable_t* flow,
                          keel_session_t* session);

/**
 * @brief Destroy session flow state.
 */
void keel_session_flow_destroy(keel_session_flow_t* sf);

/**
 * @brief Handle commit-in-doubt scenario.
 *
 * Called by the worker when the backend dies while a COMMIT is in-flight
 * (commit_in_flight == true && txn_tracking == true).
 * Sets commit_in_doubt, borrows a clean primary connection, sends
 * SELECT txid_status(xid::xid8)::text, and returns KEEL_FLOW_WAIT_COMMIT_CHECK.
 * On error (no pool connection, missing XID) synthesizes an error response
 * and returns KEEL_FLOW_ERROR.
 */
keel_flow_result_t keel_engine_flow_handle_commit_doubt(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_worker_t* worker);

/**
 * @brief Process frontend data through the flow engine.
 *
 * Loops frame_len + on_fe_msg over the buffer, dispatches actions.
 * May send data to FE, forward to BE, borrow backend, etc.
 *
 * @param sf      Session flow state
 * @param session Session
 * @param data    Received bytes
 * @param len     Number of bytes
 * @return Flow result
 */
keel_flow_result_t keel_engine_flow_on_fe_data(
    keel_session_flow_t* sf,
    keel_session_t* session,
    const uint8_t* data,
    size_t len);

/**
 * @brief Process backend data through the flow engine.
 *
 * Loops frame_len + on_be_msg over the buffer, dispatches actions.
 * Forwards responses to FE, updates tx state, returns backends to pool.
 *
 * @param sf      Session flow state
 * @param session Session
 * @param data    Received bytes
 * @param len     Number of bytes
 * @return Flow result
 */
keel_flow_result_t keel_engine_flow_on_be_data(
    keel_session_flow_t* sf,
    keel_session_t* session,
    const uint8_t* data,
    size_t len);

/**
 * @brief Resume a session that was waiting for a backend from the pool.
 *
 * Called by the pool's wait callback when a backend connection becomes
 * available. Re-sends the pending message and transitions to WAIT_BACKEND.
 *
 * @param sf       Session flow state
 * @param session  Session
 * @param be_conn  The backend connection that became available
 * @return Flow result (typically WAIT_BACKEND)
 */
keel_flow_result_t keel_engine_flow_resume_from_pool(
    keel_session_flow_t* sf,
    keel_session_t* session,
    struct backend_conn* be_conn);

/**
 * @brief Resume a session whose async auth operation has completed.
 *
 * Called by the worker when the auth_notify_fd eventfd fires after an
 * off-thread LDAP/PAM verify.  Drains the eventfd, then re-invokes the
 * protocol's on_fe_msg handler with an empty message so the protocol can
 * pick up the completed auth result from ctx->auth_ctx->state.
 *
 * @param sf       Session flow state
 * @param session  Session (client_fd used for sending error/success)
 * @return KEEL_FLOW_OK (auth complete), KEEL_FLOW_CLOSED (auth rejected
 *         or connection dropped), or KEEL_FLOW_ERROR on internal failure.
 */
keel_flow_result_t keel_engine_flow_resume_auth(
    keel_session_flow_t* sf,
    keel_session_t* session);

/* ============================================================================
 * Non-blocking send helper (shared between engine_flow.c and worker.c)
 * ============================================================================ */

/**
 * @brief Non-blocking send — tries to drain the buffer without blocking.
 *
 * Loops send(MSG_NOSIGNAL) until the full buffer is sent or the kernel
 * returns EAGAIN.  Never calls poll/epoll — if the socket buffer is full,
 * the caller must defer the remainder to io_uring.
 *
 * @return bytes actually sent (may be < len), or -1 on hard error.
 */
ssize_t keel_try_send_nb(int fd, const void* buf, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_FLOW_H */
