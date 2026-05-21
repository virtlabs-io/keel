/**
 * @file session.h
 * @brief Public session object, lifecycle states, and residual-buffer primitives.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The session subsystem is the runtime boundary where transport state, protocol
 * state, backend-pool ownership, and multiplexing safety all meet. A
 * `keel_session_t` starts life as a pure frontend connection, accumulates
 * protocol-specific context during startup and authentication, optionally borrows
 * a backend connection from a pool, and then carries enough metadata for the
 * engine to decide whether that backend can later be returned safely.
 *
 * Several other subsystems feed into this object:
 *
 * - protocol flows store opaque per-session state in `plugin_state`;
 * - the engine updates `mode`, `state`, and transaction metadata to select the
 *   cheapest safe I/O path;
 * - state-profile, hard-pin, and SSV helpers feed `state_hash`, `state_profile`,
 *   `pin_reason`, and related flags so pool-return decisions are based on
 *   semantic cleanliness rather than socket idleness alone;
 * - worker-local slab allocation keeps session allocation deterministic and cheap
 *   on the accept path.
 *
 * This header therefore documents both a data structure and a set of invariants:
 * sessions are worker-affine, residual buffers preserve partially parsed frames,
 * and cleanup must reset all borrow-state so a recycled slab entry cannot leak
 * stale protocol or backend ownership into the next client.
 */

#ifndef KEEL_SESSION_H
#define KEEL_SESSION_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>
#include <sys/types.h>

#include "keel/engine/engine.h"
#include "keel/trace/trace.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct keel_proto_flow_vtable;
struct keel_worker;
struct keel_pool;
struct state_profile;
struct keel_admission;

/* ============================================================================
 * Session States
 * ============================================================================ */

typedef enum keel_session_state {
    KEEL_SESSION_INIT = 0,       /* Initial state, awaiting first packet */
    KEEL_SESSION_STARTUP,        /* Processing startup message */
    KEEL_SESSION_AUTH,           /* Authentication in progress */
    KEEL_SESSION_BACKEND_CONNECT,/* Connecting to backend server */
    KEEL_SESSION_READY,          /* Ready for queries (idle) */
    KEEL_SESSION_QUERY,          /* Processing a query */
    KEEL_SESSION_COPY,           /* COPY data transfer */
    KEEL_SESSION_CLOSING,        /* Graceful close in progress */
    KEEL_SESSION_CLOSED,         /* Session is closed, ready for reuse */

    /* Spec §8 — Granular frontend/backend states */
    KEEL_SESSION_FE_READ,        /* Reading from frontend (recv/io_uring) */
    KEEL_SESSION_FE_CLASSIFY,    /* Protocol classify + action generation */
    KEEL_SESSION_FE_WAIT_BACKEND,/* Waiting for backend from pool */
    KEEL_SESSION_BE_SYNC,        /* Sending state-sync SQL to backend */
    KEEL_SESSION_STREAM_COPY,    /* Streaming via read/write (userspace) */
    KEEL_SESSION_STREAM_SPLICE,  /* Streaming via splice (zero-copy) */
    KEEL_SESSION_HARD_PIN,       /* Hard-pinned — exclusive backend */

    KEEL_SESSION_STATE_COUNT     /* Sentinel — must be last */
} keel_session_state_t;

/**
 * @brief Return a stable string name for a session lifecycle state.
 *
 * The names are used mainly for diagnostics, tracing, and invariant failures.
 * They intentionally mirror the internal state machine so log output can be read
 * against engine state-transition code without a translation table.
 *
 * @param state Session state enum value.
 * @return Pointer to a static string for known states, or `"UNKNOWN"`.
 */
const char* keel_session_state_name(keel_session_state_t state);

/* ============================================================================
 * Residual Buffer
 * ============================================================================
 * Residual buffers preserve bytes that have been read from a socket but cannot
 * yet be consumed by the protocol engine. That usually happens when a frame
 * header arrives before the full body, when a parser intentionally stops at a
 * packet boundary, or when the core must park unread bytes while switching
 * between inspect/copy/stream paths.
 *
 * The implementation is intentionally hybrid:
 *
 * - small residuals stay inline in the session object, avoiding heap traffic on
 *   the common path where only a header or a few trailing bytes remain;
 * - larger tails spill into a singly linked chain of heap chunks so the core can
 *   keep appending without repeatedly reallocating contiguous storage;
 * - consumers may linearize or compact only when a downstream API truly needs a
 *   contiguous view, keeping steady-state receive paths allocation-light.
 */

#define KEEL_RESIDUAL_INLINE_SIZE 256

typedef struct keel_residual_chunk {
    struct keel_residual_chunk*  next;
    size_t                      size;
    size_t                      used;
    uint8_t                     data[];
} keel_residual_chunk_t;

typedef struct keel_residual {
    /* Inline storage for small residuals */
    uint8_t     inline_buf[KEEL_RESIDUAL_INLINE_SIZE];
    size_t      inline_used;
    
    /* Overflow chain for large residuals */
    keel_residual_chunk_t*   head;
    keel_residual_chunk_t*   tail;
    size_t                  total_size;     /* Total bytes across all chunks */
    
    /* Parser state */
    size_t      expected_len;               /* Bytes needed for complete packet */
    bool        header_complete;            /* Have we parsed the header? */
} keel_residual_t;

/**
 * @brief Reset a residual buffer to its empty initial state.
 *
 * This clears parser bookkeeping as well as any inline-byte count, but it does
 * not free heap chunks because no chunks should exist at initialization time.
 * Callers normally use this during session init or after a full clear.
 *
 * @param res Residual buffer to initialize.
 * @return
 */
void keel_residual_init(keel_residual_t* res);

/**
 * @brief Append newly read bytes to the tail of a residual buffer.
 *
 * The function first tries to consume remaining inline capacity and only falls
 * back to heap chunks when the residual outgrows the fixed inline region. This
 * keeps the common partial-frame case allocation-free while still supporting
 * arbitrarily large spillover during streaming and COPY-style exchanges.
 *
 * @param res Residual buffer receiving the bytes.
 * @param data Pointer to input bytes to append.
 * @param len Number of bytes to append.
 * @return `0` on success, or `-1` if heap allocation for overflow chunks fails.
 */
int keel_residual_append(keel_residual_t* res, const void* data, size_t len);

/**
 * @brief Remove bytes from the front of the residual buffer.
 *
 * Consumption is FIFO. Inline bytes are drained first, then overflow chunks.
 * Passing `NULL` for `dest` turns the operation into a discard, which is useful
 * when protocol framing has already inspected the bytes and simply needs to drop
 * them from the pending window.
 *
 * @param res Residual buffer to consume from.
 * @param dest [out] Optional destination for copied bytes, or `NULL` to discard.
 * @param len Maximum number of bytes to consume.
 * @return Number of bytes actually removed from the residual.
 */
size_t keel_residual_consume(keel_residual_t* res, void* dest, size_t len);

/**
 * @brief Expose the first contiguous residual segment without consuming it.
 *
 * This is a zero-copy convenience for parsers that can operate on the current
 * head segment directly. If the residual spans multiple storage regions, the
 * function returns only the leading contiguous portion and reports its length via
 * `len`; callers that need the whole residual contiguously should linearize.
 *
 * @param res Residual buffer to inspect.
 * @param len [out] Receives the contiguous byte count available at the head.
 * @return Pointer to the current contiguous head segment, or `NULL` when empty.
 */
const void* keel_residual_peek(keel_residual_t* res, size_t* len);

/**
 * @brief Return the total byte count currently buffered as residual data.
 */
static inline size_t keel_residual_len(const keel_residual_t* res) {
    return res->inline_used + res->total_size;
}

/**
 * @brief Test whether a residual buffer currently holds any unread bytes.
 */
static inline bool keel_residual_empty(const keel_residual_t* res) {
    return res->inline_used == 0 && res->total_size == 0;
}

/**
 * @brief Free all heap-backed residual storage and reset parser bookkeeping.
 *
 * This is the destructive reset used during session teardown or when a protocol
 * path decides buffered bytes are no longer relevant. After it returns, the
 * residual is equivalent to a fresh `keel_residual_init()` state.
 *
 * @param res Residual buffer to clear.
 * @return
 */
void keel_residual_clear(keel_residual_t* res);

/**
 * @brief Materialize the entire residual buffer into caller-provided storage.
 *
 * This is intentionally a slower path used when a downstream consumer requires a
 * contiguous view. It does not mutate the residual and therefore can be used for
 * inspection before a subsequent consume.
 *
 * @param res Residual buffer to copy from.
 * @param buf [out] Destination buffer that receives the contiguous bytes.
 * @param buf_len Capacity of `buf` in bytes.
 * @return Number of bytes copied, or `-1` if `buf` is too small.
 */
ssize_t keel_residual_linearize(const keel_residual_t* res, void* buf, size_t buf_len);

/**
 * @brief Pull overflow bytes back into inline storage when room is available.
 *
 * Compaction is a cache-locality optimization. It trades a small copy now for a
 * cheaper future parse path by shrinking or eliminating heap chunks once the
 * buffered tail has fallen back under the inline threshold.
 *
 * @param res Residual buffer to compact.
 * @return
 */
void keel_residual_compact(keel_residual_t* res);

/* ============================================================================
 * Pipe Handle (for splice on Linux)
 * ============================================================================ */

typedef struct keel_pipe {
    int     read_fd;
    int     write_fd;
    size_t  capacity;   /* Pipe capacity (F_GETPIPE_SZ) */
    size_t  pending;    /* Bytes currently in pipe */
} keel_pipe_t;

/* ============================================================================
 * Session Structure
 * ============================================================================ */

typedef struct keel_session {
    /* Identity */
    uint64_t            id;             /* Unique session ID */
    keel_session_state_t state;          /* Current state */
    keel_mode_t          mode;           /* Current I/O mode (session-global) */

    /* Per-direction transport modes.  When non-zero (i.e. not STARTUP) these
     * override the global `mode` for the respective traffic direction, enabling
     * asymmetric policies such as ANALYZE client→server while SPLICE
     * server→client result data. */
    keel_mode_t          mode_c2s;       /* Client-to-server I/O mode override */
    keel_mode_t          mode_s2c;       /* Server-to-client I/O mode override */

    /* Cancel key (synthetic pid/secret sent to frontend in BackendKeyData).
     * Encodes worker_id + slab_index so the cancel handler can route back
     * to the correct session without a global map. */
    uint32_t            cancel_pid;     /* (worker_id << 16) | slab_index */
    uint32_t            cancel_secret;  /* Random 32-bit secret */
    
    /* File descriptors */
    int                 client_fd;      /* Client connection */
    int                 server_fd;      /* Backend server connection */
    
    /* Zero-copy resources (Linux only) */
    keel_pipe_t*         c2s_pipe;       /* Client-to-server pipe */
    keel_pipe_t*         s2c_pipe;       /* Server-to-client pipe */
    
    /* Residual buffers for incomplete packets */
    keel_residual_t      client_residual;
    keel_residual_t      server_residual;
    
    /* Plugin interface — opaque state owned by the active protocol plugin.
     * Set by the core after keel_session_flow_init(); valid for the session
     * lifetime.  The plugin reads/writes this via the flow vtable callbacks;
     * core treats it as completely opaque. */
    void*               plugin_state;

    /* Fast-forward (splice) mode.
     * When 1, the core bypasses the plugin on_be_msg callback and splices
     * backend→client data directly.  Cleared by the core when the plugin
     * signals query_complete on a prior frame. */
    uint8_t             fast_forward_mode;

    /* Worker affinity */
    struct keel_worker*  worker;
    
    /* Connection pool (if pooled connection) */
    struct keel_pool*    pool;
    
    /* Backend pool connection (for multiplexing) */
    struct backend_conn* backend_conn;  /* Borrowed connection from pool */
    uint64_t            backend_generation; /* Snapshot of backend_conn->generation at bind time */
    bool                in_transaction; /* Inside BEGIN...COMMIT */
    bool                commit_in_doubt; /* CID recovery in progress — do not force-close */
    uint64_t            indoubt_xid;    /* XID being checked via txid_status() (0=unknown) */
    uint64_t            state_hash;     /* Hash of SET variables */

    /* State profile (spec §5) */
    struct state_profile* state_profile; /* Session's SET parameters */

    /* Hard-pin state (spec §16) */
    uint32_t            pin_reason;     /* Bitmask of keel_pin_reason_t */
    bool                hard_pinned;    /* True if backend is exclusively owned */
    
    /* Timing */
    uint64_t            created_at;     /* Creation timestamp (ns) */
    uint64_t            last_activity;  /* Last I/O timestamp (ns) */
    uint32_t            query_count;    /* Queries processed */
    uint64_t            query_start_ns; /* Monotonic ns when current query started */
    uint64_t            be_send_ns;     /* Monotonic ns when query was sent to backend */
    uint64_t            pool_wait_ns;   /* Monotonic ns when pool borrow started */
    
    /* Flags */
    uint32_t            flags;
    
    /* User info (after auth) */
    char                username[64];
    char                database[64];
    char                client_password[256]; /* For passthrough auth mode */
    
    /* Error info */
    int                 last_error;
    char                error_msg[256];

    /* TLS peer info (mTLS) — populated after TLS handshake if peer cert present */
    bool                tls_peer_has_cert;
    char                tls_peer_subject[256];
    char                tls_peer_issuer[256];
    
    /* Userdata for worker callbacks (e.g., pool wait resume) */
    void*               userdata;

    /* Distributed tracing — populated when trace sampling is active */
    keel_trace_ctx_t    trace_ctx;       /* W3C trace context (propagated or generated) */
    keel_span_t         trace_span;      /* Root span for this session's lifetime */
    bool                trace_sampled;   /* True if this session is being traced */

    /* Early-cancel flag: set atomically by the cancel handler when a
     * CancelRequest arrives before the session has borrowed a backend
     * connection.  The FE query handler synthesises E(57014) and skips
     * backend borrow when this is true. */
    _Atomic bool        cancel_pending;

    /* Slab management - DO NOT MOVE */
    struct keel_session* next_free;      /* Free list link */
    uint32_t            slab_index;     /* Index in slab array */
} keel_session_t;

/* Session flags */
#define KEEL_SESSION_FLAG_SSL            (1 << 0)  /* SSL/TLS enabled */
#define KEEL_SESSION_FLAG_AUTHENTICATED  (1 << 1)  /* Auth completed */
#define KEEL_SESSION_FLAG_POOLED         (1 << 2)  /* From connection pool */
#define KEEL_SESSION_FLAG_READONLY       (1 << 3)  /* Read-only transaction */
#define KEEL_SESSION_FLAG_SPLICE         (1 << 4)  /* Zero-copy mode active */
#define KEEL_SESSION_FLAG_PASSTHROUGH    (1 << 5)  /* Passthrough auth mode */

/* ============================================================================
 * Session Slab Allocator
 * ============================================================================ */

typedef struct keel_session_slab {
    keel_session_t*  sessions;       /* Array of sessions */
    size_t          capacity;       /* Total slots */
    size_t          allocated;      /* Currently allocated */
    keel_session_t*  free_list;      /* Free session list */
    
    /* Stats */
    uint64_t        alloc_count;
    uint64_t        free_count;
    uint64_t        reuse_count;
} keel_session_slab_t;

/**
 * @brief Pre-allocate and initialize a worker-local slab of reusable sessions.
 *
 * Slab allocation keeps the accept/close path deterministic by avoiding a fresh
 * heap allocation for each connection. Each slot is a full `keel_session_t`
 * whose mutable fields are reset on reuse while slab metadata stays stable.
 *
 * @param slab Slab allocator instance to initialize.
 * @param capacity Number of reusable session objects to pre-allocate.
 * @return `0` on success, or `-1` on allocation/setup failure.
 */
int keel_session_slab_init(keel_session_slab_t* slab, size_t capacity);

/**
 * @brief Borrow one reusable session object from the slab free list.
 *
 * The returned object still needs logical initialization with
 * `keel_session_init()`. Exhaustion means the worker has reached its configured
 * concurrency envelope and must reject or defer new frontend work elsewhere.
 *
 * @param slab Slab allocator to borrow from.
 * @return Pointer to a reusable session object, or `NULL` if no slot is free.
 */
keel_session_t* keel_session_slab_alloc(keel_session_slab_t* slab);

/**
 * @brief Return a previously borrowed session object to the slab free list.
 *
 * Callers are expected to have completed logical cleanup before freeing so the
 * next borrower sees a fully reset session, not a half-detached backend or stale
 * protocol state.
 *
 * @param slab Slab allocator receiving the object back.
 * @param session Session object being recycled.
 * @return
 */
void keel_session_slab_free(keel_session_slab_t* slab, keel_session_t* session);

/**
 * @brief Tear down a session slab and free all pre-allocated storage.
 *
 * This is normally a worker-shutdown operation and assumes no live sessions are
 * still using slab entries.
 *
 * @param slab Slab allocator to destroy.
 * @return
 */
void keel_session_slab_destroy(keel_session_slab_t* slab);

/* ============================================================================
 * Session Operations
 * ============================================================================ */

/**
 * @brief Prepare a recycled or fresh session object for a new frontend socket.
 *
 * Initialization wipes all per-connection runtime state while preserving slab
 * bookkeeping fields that tie the object back to the allocator. Backend
 * ownership, transaction state, residual buffers, timestamps, and plugin state
 * all return to a clean baseline so no information leaks across clients.
 *
 * The protocol plugin context itself is created later by the engine once it has
 * selected a flow implementation and called the flow's init hook.
 *
 * @param session Session object to initialize.
 * @param client_fd Accepted frontend socket descriptor.
 * @return `0` on success, or `-1` if arguments are invalid.
 */
int keel_session_init(keel_session_t* session, int client_fd);

/**
 * @brief Perform a validated lifecycle-state transition.
 *
 * The transition table intentionally rejects impossible jumps so log messages,
 * metrics, and engine invariants all observe the same lifecycle. Invalid moves
 * indicate a bug or unexpected control-flow edge and are rejected rather than
 * silently normalised.
 *
 * @param session Session whose state machine is advancing.
 * @param state Target state.
 * @return `0` on success, or `-1` if `session` is `NULL` or the transition is invalid.
 */
int keel_session_set_state(keel_session_t* session, keel_session_state_t state);

/**
 * @brief Overwrite the engine-selected I/O mode for the session.
 *
 * This is a lightweight setter used when the protocol flow has already decided
 * whether subsequent traffic should be analyzed, peeked, copied, streamed, or
 * spliced.
 *
 * @param session Session to update.
 * @param mode New transport mode.
 * @return
 */
void keel_session_set_mode(keel_session_t* session, keel_mode_t mode);

/**
 * @brief Set the I/O transport mode for one traffic direction only.
 *
 * Allows asymmetric per-direction policies (e.g. ANALYZE client→server while
 * splicing server→client result data).  Pass `KEEL_MODE_STARTUP` to clear a
 * direction override and fall back to the session-global mode.
 *
 * @param session   Session to update.
 * @param direction KEEL_DIR_CLIENT_TO_SERVER or KEEL_DIR_SERVER_TO_CLIENT.
 * @param mode      Transport mode override for this direction.
 */
void keel_session_set_mode_dir(keel_session_t* session,
                                keel_direction_t direction,
                                keel_mode_t mode);

/**
 * @brief Refresh the session's last-activity timestamp to the current monotonic time.
 *
 * This is used by idle-timeout logic and worker observability. It does not alter
 * any protocol state beyond the timestamp field itself.
 *
 * @param session Session to touch.
 * @return
 */
void keel_session_touch(keel_session_t* session);

/**
 * @brief Check whether a session has exceeded an idle deadline.
 *
 * The comparison uses monotonic timestamps so it is immune to wall-clock jumps.
 * It is intentionally conservative: if `session` is `NULL`, the function returns
 * `false` rather than expiring an unknown object.
 *
 * @param session Session to inspect.
 * @param timeout_ns Idle threshold in nanoseconds.
 * @return `true` if the session has been idle longer than `timeout_ns`.
 */
bool keel_session_is_idle(keel_session_t* session, uint64_t timeout_ns);

/**
 * @brief Record a session-scoped error code and human-readable message.
 *
 * This is diagnostic state, not a recovery mechanism: callers still need to
 * decide whether to fail the query, close the session, or attempt fallback. The
 * stored message is truncated safely to fit the fixed buffer.
 *
 * @param session Session receiving the error metadata.
 * @param code Subsystem-specific error code.
 * @param msg Optional NUL-terminated error string, or `NULL` to clear the message.
 * @return
 */
void keel_session_set_error(keel_session_t* session, int code, const char* msg);

/**
 * @brief Close frontend/backend descriptors and mark the session closed.
 *
 * The close path intentionally nulls splice pipes rather than freeing them
 * directly because those resources are owned by the worker or pipe pool. This
 * function is idempotent enough for teardown code: repeated calls after the first
 * become mostly no-ops.
 *
 * @param session Session to close.
 * @return
 */
void keel_session_close(keel_session_t* session);

/**
 * @brief Reset all per-client runtime state before a session object is recycled.
 *
 * Cleanup is stricter than close: it ensures residual buffers, profile state,
 * backend references, user metadata, and pin/transaction flags are all cleared so
 * the slab allocator can safely hand the object to a different client.
 *
 * @param session Session to clean up.
 * @return
 */
void keel_session_cleanup(keel_session_t* session);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SESSION_H */
