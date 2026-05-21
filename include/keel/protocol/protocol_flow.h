/**
 * @file protocol_flow.h
 * @brief Primary protocol-plugin contract between the engine and wire-protocol implementations.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This is the central interface that lets KEEL remain protocol-agnostic in the
 * engine while still supporting rich wire-level behavior. A protocol plugin owns
 * message framing, handshake semantics, query classification, transaction-state
 * interpretation, and backend-reuse judgments. The engine, in turn, owns I/O,
 * routing, borrowing, cleanup orchestration, and lifecycle enforcement.
 *
 * That split is deliberate. It prevents protocol code from quietly reaching into
 * transport or routing internals, and it prevents the engine from guessing at wire
 * semantics it cannot safely infer. The result is a contract where protocol code
 * declares intent and state, while the engine remains the sole executor of system
 * actions.
 */

#ifndef KEEL_PROTOCOL_FLOW_H
#define KEEL_PROTOCOL_FLOW_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#include "keel/plugin/plugin_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

typedef struct keel_session     keel_session_t;
struct state_profile;

/* ============================================================================
 * Session Phase (Engine-level lifecycle)
 * ============================================================================ */

typedef enum keel_session_phase {
    KEEL_PHASE_HANDSHAKE_AUTH = 0,   /**< Transport handshake + authentication */
    KEEL_PHASE_READY,                /**< Authenticated, waiting for queries */
    KEEL_PHASE_QUERY,                /**< Processing a query cycle */
    KEEL_PHASE_BACKEND_SYNC,         /**< Syncing backend state before query */
    KEEL_PHASE_BACKEND_CLEANING,     /**< Cleanup state machine on backend */
    KEEL_PHASE_CLOSING,              /**< Session is shutting down */
} keel_session_phase_t;

/* ============================================================================
 * Frontend Message Action (returned by proto->on_fe_msg)
 * ============================================================================
 *
 * The protocol examines a frontend message and tells the engine what to do.
 * The protocol NEVER performs I/O — it only declares intent.
 */

typedef enum keel_fe_action_type {
    KEEL_FE_ACT_NONE = 0,           /**< No action needed (absorbed by protocol) */
    KEEL_FE_ACT_SEND_FE,            /**< Send response to frontend (challenge, handshake) */
    KEEL_FE_ACT_NEED_BACKEND_AUTH,   /**< Need backend for auth relay */
    KEEL_FE_ACT_FORWARD_TO_BACKEND,  /**< Forward message to backend */
    KEEL_FE_ACT_AUTH_COMPLETE,       /**< Authentication is complete */
    KEEL_FE_ACT_QUERY,              /**< Query message — needs routing + backend */
    KEEL_FE_ACT_TERMINATE,          /**< Client wants to disconnect */
    KEEL_FE_ACT_ERROR,              /**< Protocol error, close session */
    KEEL_FE_ACT_SSL_REQUEST,        /**< SSL/TLS request */
    KEEL_FE_ACT_CANCEL_REQUEST,     /**< Cancel request (16-byte one-shot) */
    KEEL_FE_ACT_AUTH_REJECT,        /**< Authentication failed — send error then close */
    KEEL_FE_ACT_WAIT_AUTH,          /**< Auth offloaded to worker thread — arm notify_fd in reactor */
} keel_fe_action_type_t;

/**
 * @brief Message kind classification (for cache/route decisions)
 */
typedef enum keel_msg_kind {
    KEEL_MSG_KIND_SQL = 0,           /**< Contains SQL text */
    KEEL_MSG_KIND_TX_CONTROL,        /**< BEGIN/COMMIT/ROLLBACK/SAVEPOINT */
    KEEL_MSG_KIND_STATE_CHANGE,      /**< SET/RESET/DISCARD */
    KEEL_MSG_KIND_COPY,              /**< COPY data stream */
    KEEL_MSG_KIND_EXTENDED,          /**< Extended protocol (Parse/Bind/Execute) */
    KEEL_MSG_KIND_OTHER,             /**< Everything else */
} keel_msg_kind_t;

/**
 * @brief Query effect flags — protocol-extracted semantic information
 */
typedef enum keel_query_effect_flags {
    KEEL_QE_NONE             = 0,
    KEEL_QE_READONLY         = (1 << 0),    /**< Provably read-only */
    KEEL_QE_WRITE            = (1 << 1),    /**< Modifies data */
    KEEL_QE_DDL              = (1 << 2),    /**< DDL operation */
    KEEL_QE_BEGINS_TX        = (1 << 3),    /**< Starts a transaction */
    KEEL_QE_ENDS_TX          = (1 << 4),    /**< Commits or rolls back */
    KEEL_QE_HARD_PIN         = (1 << 5),    /**< Requires hard pin (LISTEN, TEMP, etc.) */
    KEEL_QE_SETS_STATE       = (1 << 6),    /**< Changes session state (SET) */
    KEEL_QE_UNSAFE_READ      = (1 << 7),    /**< SELECT FOR UPDATE, volatile funcs */
    KEEL_QE_MULTI_STMT       = (1 << 8),    /**< Multiple statements in one message */
    KEEL_QE_COPY_IN          = (1 << 9),    /**< COPY FROM STDIN */
    KEEL_QE_COPY_OUT         = (1 << 10),   /**< COPY TO STDOUT */
    KEEL_QE_POTENTIALLY_STATEFUL = (1 << 11), /**< May create server-side state (temp, cursor, prepared) */
    KEEL_QE_UNKNOWN_STATE    = (1 << 12),   /**< Unmodellable command — mark session unknown on pool return */
} keel_query_effect_flags_t;

/**
 * @brief Pin reason bitmask (why session is pinned to backend)
 */
typedef enum keel_flow_pin_reason {
    KEEL_FPIN_NONE               = 0,
    KEEL_FPIN_TRANSACTION        = (1 << 0),    /**< Inside BEGIN..COMMIT */
    KEEL_FPIN_EXTENDED_PROTO     = (1 << 1),    /**< Extended query protocol */
    KEEL_FPIN_TEMP_TABLE         = (1 << 2),    /**< Created temp table */
    KEEL_FPIN_LISTEN             = (1 << 3),    /**< LISTEN active */
    KEEL_FPIN_CURSOR             = (1 << 4),    /**< Cursor active */
    KEEL_FPIN_COPY               = (1 << 5),    /**< COPY mode active */
    KEEL_FPIN_SET_ROLE           = (1 << 6),    /**< SET ROLE active */
    KEEL_FPIN_ADVISORY_LOCK      = (1 << 7),    /**< Advisory lock held */
    KEEL_FPIN_PREPARED_STMT      = (1 << 8),    /**< Server-side prepared stmt */
    KEEL_FPIN_GET_LOCK           = (1 << 9),    /**< MySQL GET_LOCK */
    KEEL_FPIN_USER_VARIABLE      = (1 << 10),   /**< MySQL @user variable */
    KEEL_FPIN_LOCK_TABLE         = (1 << 11),   /**< LOCK TABLES (MySQL) */
    KEEL_FPIN_FAILED_TX          = (1 << 12),   /**< Transaction in error state */
    KEEL_FPIN_STREAMING          = (1 << 13),   /**< Streaming replication */
    KEEL_FPIN_AUTH               = (1 << 14),   /**< Auth-pinned during handshake */
    KEEL_FPIN_QUARANTINE         = (1 << 15),   /**< Quarantined: potentially stateful, awaiting confirmation */
    KEEL_FPIN_OSC                = (1 << 16),   /**< Online Schema Change tool session (gh-ost / pt-osc) */
} keel_flow_pin_reason_t;

/**
 * @brief Routing hint (protocol advisory, engine decides)
 */
typedef enum keel_flow_route {
    KEEL_FROUTE_WRITE = 0,      /**< Must go to a writable node (RW or WO) */
    KEEL_FROUTE_READ,           /**< Can go to a readable node (RW or RO) */
    KEEL_FROUTE_ANY,            /**< Any available backend */
} keel_flow_route_t;

/* Backward-compatible aliases */
#define KEEL_FROUTE_PRIMARY  KEEL_FROUTE_WRITE
#define KEEL_FROUTE_REPLICA  KEEL_FROUTE_READ

/**
 * @brief Frontend message action — returned by on_fe_msg()
 *
 * This is the central return type. The protocol fills this out to tell
 * the engine exactly what needs to happen next.
 */
typedef struct keel_fe_action {
    keel_fe_action_type_t    type;               /**< Action type */

    /* Response to send to frontend (if type == SEND_FE) */
    const uint8_t*          fe_response;        /**< Response bytes */
    size_t                  fe_response_len;    /**< Response length */

    /* Data to forward to backend (if type == FORWARD_TO_BACKEND or QUERY) */
    const uint8_t*          be_payload;         /**< Backend payload */
    size_t                  be_payload_len;     /**< Backend payload length */

    /* Query semantics (if type == QUERY) */
    keel_msg_kind_t          msg_kind;           /**< Message kind */
    keel_query_effect_flags_t effect;            /**< Query effect flags */
    keel_flow_pin_reason_t   pin_update;         /**< Pin flags to set */
    keel_flow_pin_reason_t   pin_clear;          /**< Pin flags to clear */
    keel_flow_route_t        route_hint;         /**< Routing hint */

    /* SQL view for cache/fingerprint (optional, points into recv buffer) */
    const char*             sql_view;           /**< SQL text */
    size_t                  sql_view_len;       /**< SQL text length */

    /* State delta (SET/RESET) */
    bool                    has_state_delta;     /**< state_delta is valid */
    const char*             state_key;           /**< SET parameter name */
    size_t                  state_key_len;
    const char*             state_value;         /**< SET parameter value */
    size_t                  state_value_len;

    /* Query classification (set by classify_sql) */
    uint32_t                query_type;         /**< keel_query_type_t */

    /* Client identity (set during AUTH_COMPLETE) */
    const char*             client_username;    /**< Username from startup message */
    const char*             client_database;    /**< Database from startup message */

    /** Notification fd for async auth (KEEL_FE_ACT_WAIT_AUTH only).
     *  The auth worker thread writes 1 uint64 to this eventfd when done.
     *  The reactor arms a read on this fd; when it fires, the engine calls
     *  keel_engine_flow_resume_auth() which re-invokes pgf_on_fe_msg with
     *  an empty message to pick up the result from ctx->auth_ctx->state. */
    int                     auth_notify_fd;     /**< eventfd for async auth completion (-1 = none) */

    /* Eligibility flags */
    bool                    cache_eligible;     /**< Can use query effect cache */
    bool                    splice_eligible;    /**< Can use zero-copy splice */
    bool                    no_response;        /**< Backend won't respond (e.g., COM_STMT_CLOSE) */

    /** For extended-protocol messages that enter the send batch before a Flush
     *  terminal: how many backend control-message "groups" this FE message
     *  expects in response (used by flush_pending_count tracking).
     *  Parse/Bind/Execute/Close → 1, Describe → 1 (RowDescription/NoData is
     *  the terminal; ParameterDescription is not counted), Flush/Sync → 0. */
    uint8_t                 ext_response_count;

    /* Cross-service Read-Your-Writes: injected consistency position.
     * When non-empty (first byte != '\0'), the engine stores this LSN in
     * the session's WRITE_LSN consistency atom so that subsequent replica
     * reads are gated on the replica reaching at least this position.
     * Set by the protocol when it intercepts SET keel.read_after_lsn. */
    char                    inject_consistency_lsn[128];
} keel_fe_action_t;

/* ============================================================================
 * Backend Message Action (returned by proto->on_be_msg)
 * ============================================================================
 *
 * The protocol examines a backend response and tells the engine about
 * state transitions, tx status, profile changes, and reuse eligibility.
 */

typedef enum keel_be_action_type {
    KEEL_BE_ACT_FORWARD_FE = 0,     /**< Forward this message to frontend */
    KEEL_BE_ACT_ABSORB,             /**< Don't forward (protocol internal) */
    KEEL_BE_ACT_AUTH_PROGRESS,       /**< Auth handshake progress */
    KEEL_BE_ACT_AUTH_COMPLETE,       /**< Auth handshake done, backend ready */
    KEEL_BE_ACT_ERROR,              /**< Backend error */
    KEEL_BE_ACT_DISCONNECT,         /**< Backend disconnected */
} keel_be_action_type_t;

/**
 * @brief Transaction status (from backend response)
 */
typedef enum keel_tx_status {
    KEEL_TX_IDLE = 0,        /**< No transaction (PG: 'I', MySQL: !SERVER_STATUS_IN_TRANS) */
    KEEL_TX_ACTIVE,          /**< In transaction (PG: 'T', MySQL: SERVER_STATUS_IN_TRANS) */
    KEEL_TX_FAILED,          /**< Failed transaction (PG: 'E') */
} keel_tx_status_t;

typedef struct keel_be_action {
    keel_be_action_type_t    type;               /**< Action type */

    /* Data to forward to frontend (if type == FORWARD_FE) */
    const uint8_t*          fe_payload;         /**< Frontend payload */
    size_t                  fe_payload_len;     /**< Payload length */

    /* Transaction state update */
    bool                    tx_state_changed;   /**< tx_status is valid */
    keel_tx_status_t         tx_status;          /**< New tx status */

    /* Reuse gate — AUTHORITATIVE from protocol */
    bool                    backend_reusable;   /**< Backend can be returned to pool */

    /* Pin updates */
    keel_flow_pin_reason_t   pin_update;         /**< Pin flags to set */
    keel_flow_pin_reason_t   pin_clear;          /**< Pin flags to clear */

    /* Profile update (ParameterStatus / server info) */
    bool                    has_profile_update;  /**< profile key/value are valid */
    const char*             profile_key;
    size_t                  profile_key_len;
    const char*             profile_value;
    size_t                  profile_value_len;

    /* Copy mode transitions */
    bool                    enters_copy_mode;   /**< Entering COPY IN/OUT */
    bool                    exits_copy_mode;    /**< Exiting COPY mode */

    /* Query cycle complete — backend has sent all results for this query */
    bool                    query_complete;     /**< ReadyForQuery / OK seen */

    /* Prepared-statement replay progress.
     * Set when this backend message acknowledges one replayed statement. */
    bool                    stmt_replay_accepted; /**< Replay acknowledgement for one statement */

    /* Zero-copy eligibility */
    bool                    splice_eligible;    /**< Can use zero-copy splice */

    /* Replication-tracking: XID captured from txid_current() probe.
     * Set by the protocol plugin when it has absorbed the DataRow response
     * for the pre-COMMIT "SELECT txid_current() AS _keel_txid" query.
     * The engine stores this in keel_session_flow_t.pending_commit_xid so
     * that, if the backend dies before CommandComplete(COMMIT), KEEL can
     * query txid_status() on the new primary and resolve the uncertainty. */
    bool                    commit_xid_captured; /**< pending_commit_xid is valid */
    uint64_t                commit_xid;          /**< PostgreSQL txid_current() for the in-flight COMMIT */

    /* Commit-in-doubt check stream outcome update.
     * Plugins set this while draining their protocol-specific "outcome check"
     * response stream (e.g. PostgreSQL txid_status). */
    bool                    commit_doubt_outcome_changed;
    uint8_t                 commit_doubt_outcome; /**< 0=unknown, 1=committed, 2=aborted */

    /* True when this message is a backend error response (ErrorResponse in
     * PostgreSQL, ERR packet in MySQL).  Set regardless of the action type so
     * that pre-query absorbers (state sync, deferred BEGIN, PS replay) can
     * detect and reject backend-reported failures without relying on the
     * action type, which may vary per-protocol or per-context. */
    bool                    is_error_response;
} keel_be_action_t;

#define KEEL_PROTO_DRAIN_STATE_BYTES 64

typedef enum keel_proto_drain_result {
    KEEL_PROTO_DRAIN_ERROR = -1,      /**< Protocol error or unsafe stream */
    KEEL_PROTO_DRAIN_MORE = 0,        /**< Valid stream so far; wait for more bytes */
    KEEL_PROTO_DRAIN_COMPLETE = 1,    /**< Terminal reusable boundary reached */
} keel_proto_drain_result_t;

typedef struct keel_proto_drain_state {
    uint8_t opaque[KEEL_PROTO_DRAIN_STATE_BYTES];
} keel_proto_drain_state_t;

typedef enum keel_commit_doubt_reason {
    KEEL_CIDR_NO_XID = 0,         /**< Lost commit confirmation before capturing commit token */
    KEEL_CIDR_NO_RW_POOL,         /**< No writable pool available for outcome check */
    KEEL_CIDR_NO_CHECK_CONN,      /**< Could not borrow a check connection */
    KEEL_CIDR_CHECK_BUILD_FAIL,   /**< Plugin failed to build check query */
    KEEL_CIDR_CHECK_SEND_FAIL,    /**< Check query send failed */
    KEEL_CIDR_RESOLVED_COMMITTED, /**< Outcome check says committed */
    KEEL_CIDR_RESOLVED_ABORTED,   /**< Outcome check says aborted */
    KEEL_CIDR_RESOLVED_UNKNOWN,   /**< Outcome check inconclusive */
} keel_commit_doubt_reason_t;

/**
 * @brief Prepared-statement semantic compatibility profile.
 *
 * This profile separates statement identity (stmt_set_hash) from semantic
 * execution context (search_path, role/auth, GUCs, schema epoch). Core code
 * uses it to decide whether backend statement reuse is safe.
 */
typedef struct keel_stmt_compat_profile {
    uint64_t stmt_set_hash;          /**< Hash of confirmed named statement set */
    uint64_t semantic_profile_hash;  /**< Combined semantic context hash */
    uint64_t schema_epoch;           /**< Monotonic schema/DDL epoch */
    uint64_t role_hash;              /**< Role/session-auth hash */
    uint64_t search_path_hash;       /**< search_path hash */
    uint64_t guc_hash;               /**< Replay-relevant GUC hash */
    bool     semantic_unknown;       /**< Conservative "do not reuse" marker */
} keel_stmt_compat_profile_t;

/* ============================================================================
 * Backend Cleanup Reason
 * ============================================================================ */

typedef enum keel_cleanup_reason {
    KEEL_CLEANUP_FE_DISCONNECT = 0,  /**< Frontend disconnected */
    KEEL_CLEANUP_TX_NOT_IDLE,        /**< Transaction left open */
    KEEL_CLEANUP_FAILED_TX,          /**< Transaction in error state */
    KEEL_CLEANUP_UNKNOWN_STATE,      /**< Unknown session state changes */
    KEEL_CLEANUP_HARD_TAINT,         /**< Hard-pinned taint (LISTEN etc.) */
    KEEL_CLEANUP_TIMEOUT,            /**< Session/query timeout */
} keel_cleanup_reason_t;

/* ============================================================================
 * Prepared-Statement Pooling Mode
 * ============================================================================
 *
 * Controls how the proxy handles named prepared statements when a session
 * is returned to the connection pool.
 *
 *  VIRTUALIZE  (default) — spec §17 replay.  Named prepared statements are
 *              shadowed in the session; when a new backend is borrowed the
 *              engine replays all Parse messages before forwarding the client's
 *              Bind/Execute.  Transparent to the application.
 *
 *  PINNING     — Hard-pin the backend connection the moment a PREPARE is
 *              detected.  The connection is not returned to the pool until
 *              the client issues DEALLOCATE ALL / DISCARD ALL or
 *              disconnects.  Zero overhead at Execute time; wastes a backend
 *              connection per client session that uses prepared statements.
 *
 *  TRACKING    — Same as VIRTUALIZE but the session-level statement cache is
 *              also maintained for Simple Query PREPARE ... AS / EXECUTE
 *              syntax, not only the extended-protocol Parse message path.
 *              Suitable for clients that mix simple and extended query
 *              protocols.
 *
 *  ANONYMOUS   — Intercept the extended-protocol Parse message: store the
 *              statement name → SQL mapping locally, suppress the Parse to the
 *              backend, synthesize a ParseComplete response to the client.
 *              On Bind, JIT-rewrite the named portal into an anonymous
 *              (unnamed-statement) Parse+Bind pair so the backend sees only
 *              stateless queries.  Backend connections remain perfectly
 *              poolable; no replay overhead.  Requires the SQL text to be
 *              fully available at Parse time (the rewrite cannot be deferred).
 */
typedef enum keel_ps_mode {
    KEEL_PS_MODE_VIRTUALIZE = 0,  /**< Default: PS replay (spec §17) */
    KEEL_PS_MODE_PINNING,         /**< Hard-pin backend on first PREPARE */
    KEEL_PS_MODE_TRACKING,        /**< Shadow + replay incl. simple-query PREPARE */
    KEEL_PS_MODE_ANONYMOUS,       /**< JIT rewrite Parse→anon + Bind→inline */
    KEEL_PS_MODE_OFF,             /**< No PS interception: hard-pin on named Parse,
                                   *   skip all stmt tracking / DISCARD ALL / replay.
                                   *   Use when clients don't need cross-connection
                                   *   PS sharing and want minimal proxy overhead. */
} keel_ps_mode_t;

/* ============================================================================
 * Protocol Flow VTable
 * ============================================================================
 *
 * This is the main contract between Engine and Protocol.
 * Each database protocol implements these methods.
 * Methods are grouped by flow phase.
 */

typedef struct keel_proto_flow_vtable {
    /* ---- Identity ---- */
    const char*     name;               /**< "postgres", "mysql" */
    uint16_t        default_port;       /**< 5432, 3306 */

    /* ---- Server-Speaks-First Greeting ---- */

    /**
     * @brief Generate initial greeting to send to client on accept.
     *
     * MySQL requires the server to send an Initial Handshake Packet
     * before the client sends anything.  PostgreSQL returns 0 (client
     * speaks first).
     *
     * @param ctx       Protocol context (freshly created)
     * @param buf       Output buffer
     * @param buf_len   Buffer size
     * @return Bytes written (>0 means send before recv), 0 = client speaks first, -1 on error
     */
    ssize_t (*generate_greeting)(
        void* ctx,
        uint8_t* buf,
        size_t buf_len
    );

    /* ---- Context Management ---- */

    /**
     * Create protocol-specific context for a session.
     * Called once at session init.
     */
    void* (*create_context)(keel_session_t* session);

    /**
     * Destroy protocol context.
     */
    void  (*destroy_context)(void* ctx);

    /* ---- Framing (Phase A/B/C) ---- */

    /**
     * @brief Can we extract a complete message from the buffer?
     *
     * Returns the total message length if a complete message is available,
     * 0 if more data is needed, -1 on protocol error.
     *
     * @param ctx     Protocol context
     * @param data    Buffer data
     * @param len     Available bytes
     * @param dir     Direction (FE→BE or BE→FE)
     * @return Total message length, 0 (need more), or -1 (error)
     */
    ssize_t (*frame_len)(
        void* ctx,
        const uint8_t* data,
        size_t len,
        int dir  /* 0 = FE→BE, 1 = BE→FE */
    );

    /* ---- Phase B: Handshake / Auth ---- */

    /**
     * @brief Process a frontend message during handshake/auth phase.
     *
     * The protocol examines the message and returns an action that tells
     * the engine what to do (send challenge, need backend for relay, etc.)
     *
     * @param ctx     Protocol context
     * @param data    Complete message bytes
     * @param len     Message length
     * @param action  Output: action for engine
     * @return 0 on success, -1 on error
     */
    int (*on_fe_msg)(
        void* ctx,
        const uint8_t* data,
        size_t len,
        keel_fe_action_t* action
    );

    /**
     * @brief Process a backend message (during auth relay or query response).
     *
     * The protocol examines the backend response and returns an action
     * with tx state updates, reuse gate status, etc.
     *
     * @param ctx     Protocol context
     * @param data    Complete message bytes
     * @param len     Message length
     * @param action  Output: action for engine
     * @return 0 on success, -1 on error
     */
    int (*on_be_msg)(
        void* ctx,
        const uint8_t* data,
        size_t len,
        keel_be_action_t* action
    );

    /**
     * @brief Lightweight frame classifier for the fast-forward hot path.
     *
     * Called only when session->fast_forward_mode == 1.  If this returns
     * true the engine forwards the frame directly to the client without
     * invoking on_be_msg — this is the "L4 speed" inner loop for result sets.
     *
     * Contract:
     *   - Return true ONLY for pure data/result-row frames that carry no
     *     state-change information (no tx updates, no query_complete signal).
     *   - Return false for ALL terminal/control frames (ReadyForQuery, EOF,
     *     OK, ERR, CommandComplete, ParameterStatus, etc.).
     *   - Must be O(1) with no allocation or system calls.
     *
     * @param ctx      Protocol flow context (may be NULL; implementors must guard)
     * @param hdr      Start of the raw frame (same pointer passed to on_be_msg)
     * @param hdr_len  Bytes available (at least 1 byte, up to 9 bytes peeked)
     * @return true if on_be_msg can be skipped for this frame
     */
    bool (*is_data_frame)(void* ctx, const uint8_t* hdr, size_t hdr_len);

    /* ---- Phase C: Query Routing ---- */

    /**
     * @brief Compute a fingerprint for SQL text (for cache lookup).
     *
     * Returns a 64-bit hash of the normalized SQL. Two queries with the
     * same fingerprint are assumed to have the same routing characteristics.
     *
     * @param ctx       Protocol context
     * @param sql       SQL text
     * @param sql_len   SQL text length
     * @return 64-bit fingerprint
     */
    uint64_t (*fingerprint)(
        void* ctx,
        const char* sql,
        size_t sql_len
    );

    /* ---- Phase C: Backend Sync ---- */

    /**
     * @brief Build state synchronization commands.
     *
     * When a backend's profile differs from the session's required profile,
     * this method generates the SQL commands to sync them.
     * PG: SET x = 'y'; RESET z;
     * MySQL: SET x = y;
     *
     * @param ctx           Protocol context
     * @param be_profile    Backend's current profile
     * @param se_profile    Session's required profile
     * @param buf           Output buffer for SQL commands
     * @param buf_len       Buffer size
     * @return Bytes written, or -1 on error, or 0 if no sync needed
     */
    ssize_t (*build_state_sync)(
        void* ctx,
        const struct state_profile* be_profile,
        const struct state_profile* se_profile,
        uint8_t* buf,
        size_t buf_len
    );

    /* ---- Phase D: Cleanup ---- */

    /**
     * @brief Build cleanup command for a backend connection.
     *
     * Generates the protocol-specific cleanup command (DISCARD ALL / RESET
     * / ROLLBACK / COM_RESET_CONNECTION / COM_CHANGE_USER).
     *
     * @param ctx       Protocol context
     * @param reason    Why cleanup is needed
     * @param buf       Output buffer
     * @param buf_len   Buffer size
     * @return Bytes written, or -1 on error
     */
    ssize_t (*build_cleanup)(
        void* ctx,
        keel_cleanup_reason_t reason,
        uint8_t* buf,
        size_t buf_len
    );

    /**
     * @brief Check if backend has reached the reusable gate.
     *
     * This is the AUTHORITATIVE check. The engine calls this after each
     * backend message during cleanup or query completion to determine
     * if the connection can be safely returned to the pool.
     *
     * PG:    true when ReadyForQuery with tx_status='I' is seen
     * MySQL: true when OK with !SERVER_STATUS_IN_TRANS and fully drained
     *
     * @param ctx     Protocol context
     * @return true if backend is reusable, false if still draining
     */
    bool (*backend_reuse_gate)(void* ctx);

    /* ---- Startup Message Generation ---- */

    /**
     * @brief Generate startup message for backend connection.
     *
     * When connecting to a new backend, generate the appropriate
     * startup/handshake message.
     *
     * @param ctx       Protocol context
     * @param user      Username
     * @param database  Database name
     * @param buf       Output buffer
     * @param buf_len   Buffer size
     * @return Bytes written, or -1 on error
     */
    ssize_t (*generate_startup)(
        void* ctx,
        const char* user,
        const char* database,
        uint8_t* buf,
        size_t buf_len
    );

    /**
     * @brief Generate a protocol-specific error message to send to client.
     *
     * @param ctx       Protocol context
     * @param code      Error code string (SQLSTATE for PG, errno for MySQL)
     * @param message   Error message text
     * @param buf       Output buffer
     * @param buf_len   Buffer size
     * @return Bytes written, or -1 on error
     */
    ssize_t (*generate_error)(
        void* ctx,
        const char* code,
        const char* message,
        uint8_t* buf,
        size_t buf_len
    );

    /* ---- Backend Connection Auth ---- */

    /**
     * @brief Synchronous TCP connect to a backend server.
     *
     * Creates a socket, connects with timeout, sets TCP_NODELAY.
     * Protocol-agnostic but included in vtable for uniformity.
     */


    /**
     * @brief Generate a ReadyForQuery / idle-state message for the client.
     *
     * Used by the engine to send an idle-state indicator after errors
     * without hardcoding protocol-specific bytes.
     *
     *   PG:    ReadyForQuery('Z') with tx_status='I'
     *   MySQL: returns 0 (MySQL OK already sent by generate_error)
     *
     * @param ctx       Protocol context (may be NULL)
     * @param buf       Output buffer
     * @param buf_len   Buffer size
     * @return Bytes written (>0 means send), 0 = nothing needed, -1 on error
     */
    ssize_t (*generate_ready_for_query)(
        void* ctx,
        uint8_t* buf,
        size_t buf_len
    );

    /* ================================================================
     * Optional Plugin API Extensions (Phase 5)
     *
     * All pointers below may be NULL.  Core checks before calling and
     * falls back to safe defaults when a callback is absent.
     * ================================================================ */

    /**
     * @brief Return plugin identity and capabilities.
     *
     * Core calls this once per vtable to adapt behavior (e.g., skip
     * consistency token tracking if CONSISTENCY_TOKEN cap is absent).
     *
     * @param out  Output plugin info struct
     */
    void (*get_info)(keel_plugin_info_t* out);

    /**
     * @brief Classify a backend error frame into a core-understandable class.
     *
     * @param ctx   Protocol context
     * @param data  Raw backend error frame bytes
     * @param len   Frame length
     * @param out   Output error info
     * @return 0 on success, -1 on parse failure
     */
    int (*classify_error)(
        void* ctx,
        const uint8_t* data,
        size_t len,
        keel_error_info_t* out
    );

    /**
     * @brief Capture replication position after a write completes.
     *
     * PG: SELECT pg_current_wal_lsn()
     * MySQL: SELECT @@gtid_executed
     *
     * Must be cheap (<1 ms).  Plugin may cache the latest position.
     *
     * @param ctx    Protocol context
     * @param be_fd  Backend file descriptor (for issuing inline query)
     * @param out    Output consistency token
     * @return 0 on success, -1 on error or not supported
     */
    int (*capture_consistency_token)(
        void* ctx,
        int be_fd,
        keel_consistency_token_t* out
    );

    /**
     * @brief Check if a replica has reached a given replication position.
     *
     * PG: SELECT pg_last_wal_replay_lsn() >= token
     * MySQL: SELECT WAIT_FOR_EXECUTED_GTID_SET(token, timeout_ms / 1000)
     *
     * @param ctx         Protocol context
     * @param replica_fd  Replica backend file descriptor
     * @param token       Token captured after a write on primary
     * @param timeout_ms  Wait timeout (0 = just check, don't block)
     * @param out_reached Output: true if replica has reached or passed token
     * @return 0 on success, -1 on error
     */
    int (*replica_reached_token)(
        void* ctx,
        int replica_fd,
        const keel_consistency_token_t* token,
        int timeout_ms,
        bool* out_reached
    );

    /**
     * @brief Build protocol-specific commit-in-doubt outcome check payload.
     *
     * Called when the engine lost the original backend before COMMIT
     * confirmation and needs a fresh backend to determine outcome.
     *
     * Example implementations:
     *   PG:    "Q('SELECT txid_status(<xid>)')"
     *   MySQL: "COM_QUERY('SELECT ...')" / GTID-specific check
     *
     * @param ctx      Protocol context.
     * @param xid      Captured commit token (protocol-specific numeric id).
     * @param out_buf  Output payload buffer.
     * @param out_cap  Output buffer capacity.
     * @return Bytes written (>0), 0 unsupported, -1 error.
     */
    ssize_t (*build_commit_doubt_check)(
        void* ctx,
        uint64_t xid,
        uint8_t* out_buf,
        size_t out_cap
    );

    /**
     * @brief Generate a client-facing response for commit-in-doubt outcomes.
     *
     * Implementations should include any protocol-specific terminator/state
     * frame needed by clients (e.g. PostgreSQL ReadyForQuery).
     *
     * @param ctx      Protocol context.
     * @param reason   Commit-in-doubt stage/outcome reason.
     * @param xid      Captured commit token (0 when unavailable).
     * @param out_buf  Output response buffer.
     * @param out_cap  Output buffer capacity.
     * @return Bytes written (>0), 0 unsupported, -1 error.
     */
    ssize_t (*generate_commit_doubt_response)(
        void* ctx,
        keel_commit_doubt_reason_t reason,
        uint64_t xid,
        uint8_t* out_buf,
        size_t out_cap
    );

    /**
     * @brief Begin a streaming operation (COPY IN/OUT, LOAD DATA).
     *
     * @param ctx      Protocol context
     * @param session  Session pointer (for buffer access)
     * @param be_fd    Backend file descriptor
     * @param out      Output: plugin-owned stream context
     * @return 0 on success, -1 on error
     */
    int (*begin_stream)(
        void* ctx,
        void* session,
        int be_fd,
        keel_stream_ctx_t** out
    );

    /**
     * @brief Write data into an active stream.
     *
     * @param sctx  Stream context from begin_stream()
     * @param buf   Data buffer
     * @param n     Data length
     * @return 0 on success, -1 on error
     */
    int (*stream_write)(
        keel_stream_ctx_t* sctx,
        const void* buf,
        size_t n
    );

    /**
     * @brief End a streaming operation (send terminator).
     *
     * @param sctx  Stream context from begin_stream()
     * @return 0 on success, -1 on error
     */
    int (*end_stream)(keel_stream_ctx_t* sctx);

    /**
     * @brief Selective cleanup of a backend connection slot.
     *
     * Unlike build_cleanup() which always sends DISCARD ALL, this method
     * can generate minimal SET/RESET commands based on the actual state
     * delta captured in the profile.
     *
     * @param ctx      Protocol context
     * @param be_fd    Backend file descriptor
     * @param profile  Backend's current state profile
     * @param opts     Cleanup options (mode, timeout)
     * @param buf      Output buffer for wire commands
     * @param buf_len  Buffer size
     * @return Bytes written, 0 if no cleanup needed, -1 on error
     */
    ssize_t (*cleanup_slot)(
        void* ctx,
        int be_fd,
        const struct state_profile* profile,
        keel_cleanup_opts_t opts,
        uint8_t* buf,
        size_t buf_len
    );

    /**
     * @brief Drain and validate a protocol-specific cleanup response stream.
     *
     * Core owns the socket and passes arbitrary recv() chunks. The plugin owns
     * wire parsing, partial-frame state in @p state, and reusable-boundary
     * validation.
     *
     * @param ctx          Protocol flow context; may be NULL for pool-owned cleanup.
     * @param state        Opaque caller-owned state, zeroed before the first chunk.
     * @param data         Backend bytes from recv().
     * @param len          Number of bytes in @p data.
     * @param consumed_out Optional output: bytes consumed from @p data.
     * @return COMPLETE when cleanup is fully drained and reusable, MORE when
     *         more bytes are required, or ERROR when the stream is unsafe.
     */
    keel_proto_drain_result_t (*drain_cleanup_response)(
        void* ctx,
        keel_proto_drain_state_t* state,
        const uint8_t* data,
        size_t len,
        size_t* consumed_out
    );

    /**
     * @brief Health-probe a backend connection.
     *
     * PG: issue "SELECT 1" + parse pg_is_in_recovery()
     * MySQL: COM_PING or "SELECT 1"
     *
     * @param ctx    Protocol context
     * @param be_fd  Backend file descriptor
     * @param out    Output probe result
     * @return 0 on success, -1 on error
     */
    int (*probe_backend)(
        void* ctx,
        int be_fd,
        keel_probe_result_t* out
    );

    /**
     * @brief Retrieve metadata about a backend connection.
     *
     * @param ctx    Protocol context
     * @param be_fd  Backend file descriptor
     * @param out    Output metadata
     * @return 0 on success, -1 on error
     */
    int (*get_backend_metadata)(
        void* ctx,
        int be_fd,
        keel_backend_meta_t* out
    );

    /**
     * @brief Retrieve plugin instrumentation counters.
     *
     * @param ctx   Protocol context
     * @param out   Output metrics
     * @return 0 on success, -1 if not tracked
     */
    int (*get_metrics)(void* ctx, keel_plugin_metrics_t* out);

    /**
     * @brief Notify the protocol context of the latest captured write LSN.
     *
     * Called by the engine after a successful capture_consistency_token()
     * so the protocol can serve SHOW keel.write_lsn without a vtable roundtrip.
     *
     * OPTIONAL — may be NULL.  The engine will call it only when present.
     *
     * @param ctx  Protocol flow context (session-scoped).
     * @param lsn  NUL-terminated LSN string (PG: "A/BCDABCD", MySQL: GTID).
     */
    void (*notify_write_lsn)(void* ctx, const char* lsn);

    /**
     * @brief Get prepared-statement replay data for PS virtualization (spec §17).
     *
     * OPTIONAL — may be NULL.  When NULL the engine falls back to hard-pin
     * behaviour for sessions with named prepared statements.
     *
     * Called by the engine when a session with named prepared statements is
     * assigned a backend whose stmt_set_hash doesn't match the session's.
     * The protocol fills a heap-alloc'd buffer with all Parse wire messages
     * concatenated (one per active named prepared statement), so the engine
     * can replay them to the new backend before forwarding the client's Bind.
     *
     * The engine owns the returned buffer and frees it with keel_free().
     *
     * @param ctx            Protocol flow context (session-scoped)
     * @param replay_buf_out Set to malloc'd buffer of concatenated Parse msgs (caller frees).
     *                       NULL-safe — if no stmts exist, *replay_buf_out is set to NULL.
     * @param replay_len_out Set to total byte length of *replay_buf_out.
     * @param stmt_count_out Set to number of Parse messages in the buffer
     *                       (= number of ParseComplete responses to await).
     * @param hash_out       Set to the session's current stmt_set_hash.
     * @return 0 on success, -1 on error (engine falls back to hard-pin).
     */
    int (*get_stmt_replay)(
        void*     ctx,
        uint8_t** replay_buf_out,
        size_t*   replay_len_out,
        uint32_t* stmt_count_out,
        uint64_t* hash_out
    );

    /**
     * @brief Retrieve the current prepared-statement compatibility profile.
     *
     * OPTIONAL — may be NULL. When absent, the engine must conservatively
     * avoid semantic exact-match reuse and fall back to clean-replay paths.
     *
     * @param ctx    Protocol flow context (session-scoped)
     * @param out    Output profile
     * @return 0 on success, -1 on error/unsupported.
     */
    int (*get_stmt_compat_profile)(
        void* ctx,
        keel_stmt_compat_profile_t* out
    );

    /**
     * @brief (Optional) Rewrite a named-statement Bind into an anonymous
     *        Parse+Bind+Execute triplet.  Used by ANONYMOUS PS mode only.
     *
     * The implementation looks up @p stmt_name in its local anonymous-statement
     * map, reconstructs a one-shot Parse('') for the stored SQL, then writes
     * the resulting wire bytes into @p out_buf.
     *
     * @param ctx          Protocol flow context.
     * @param stmt_name    Name declared by the client in the original
     *                     Parse message.
     * @param stmt_name_len Length of @p stmt_name (bytes, excl. NUL).
     * @param bind_msg     Raw Bind wire message from the client
     *                     (includes the leading 'B' type byte and length).
     * @param bind_len     Length of @p bind_msg in bytes.
     * @param out_buf      Caller-supplied output buffer.
     * @param out_buf_len  Capacity of @p out_buf in bytes.
     * @return             Number of bytes written to @p out_buf on success;
     *                     -1 if the statement name is unknown or the output
     *                     buffer is too small (engine falls back to hard-pin).
     */
    ssize_t (*rewrite_execute_anonymous)(
        void*          ctx,
        const char*    stmt_name,
        size_t         stmt_name_len,
        const uint8_t* bind_msg,
        size_t         bind_len,
        uint8_t*       out_buf,
        size_t         out_buf_len
    );

    /**
     * @brief Apply protocol-specific pin effects from a captured client payload.
     *
     * The engine sometimes has to defer a complete frontend payload while it
     * drains setup traffic such as state sync, cleanup, or prepared-statement
     * replay. Those deferred bytes bypass the normal frame-by-frame
     * on_fe_msg() loop, so protocol plugins may need to inspect them and report
     * pin updates that would otherwise be missed.
     *
     * OPTIONAL — may be NULL. Implementations must tolerate partial trailing
     * frames and only report effects from complete messages.
     *
     * @param ctx        Protocol flow context.
     * @param data       Captured frontend bytes.
     * @param len        Number of bytes in @p data.
     * @param pin_update Output pin bits to set.
     * @param pin_clear  Output pin bits to clear.
     */
    void (*captured_fe_pin_effects)(
        void*                 ctx,
        const uint8_t*        data,
        size_t                len,
        keel_flow_pin_reason_t* pin_update,
        keel_flow_pin_reason_t* pin_clear
    );

    /**
     * @brief Send an asynchronous (fire-and-forget) cancel for a backend connection.
     *
     * Called by the engine when a CANCEL_REQUEST targets a connection owned by
     * this protocol.  The implementation is responsible for opening a new
     * transport-level connection, authenticating if required, sending the
     * appropriate cancel command, and closing the connection — all without
     * blocking the reactor event loop.
     *
     * Implementations SHOULD use a detached thread or reactor-driven I/O.
     *
     * @param host          Backend host address.
     * @param port          Backend port.
     * @param user          Username for authentication (may be NULL).
     * @param password      Password for authentication (may be NULL).
     * @param database      Database name (may be NULL).
     * @param connection_id Protocol-level connection ID to kill.
     * @return 0 if the cancel was successfully queued, -1 on immediate failure.
     */
    int (*cancel_async)(
        const char* host,
        uint16_t    port,
        const char* user,
        const char* password,
        const char* database,
        uint32_t    connection_id
    );

} keel_proto_flow_vtable_t;

/* ============================================================================
 * Default Initializers
 * ============================================================================ */

static inline keel_fe_action_t keel_fe_action_default(void) {
    return (keel_fe_action_t){
        .type = KEEL_FE_ACT_NONE,
        .fe_response = NULL, .fe_response_len = 0,
        .be_payload = NULL, .be_payload_len = 0,
        .msg_kind = KEEL_MSG_KIND_OTHER,
        .effect = KEEL_QE_NONE,
        .pin_update = KEEL_FPIN_NONE, .pin_clear = KEEL_FPIN_NONE,
        .route_hint = KEEL_FROUTE_PRIMARY,
        .sql_view = NULL, .sql_view_len = 0,
        .query_type = 0,
        .client_username = NULL,
        .client_database = NULL,
        .has_state_delta = false,
        .cache_eligible = false,
        .splice_eligible = false,
        .no_response = false,
        .inject_consistency_lsn = {0},
    };
}

static inline keel_be_action_t keel_be_action_default(void) {
    return (keel_be_action_t){
        .type = KEEL_BE_ACT_FORWARD_FE,
        .fe_payload = NULL, .fe_payload_len = 0,
        .tx_state_changed = false,
        .tx_status = KEEL_TX_IDLE,
        .backend_reusable = false,
        .pin_update = KEEL_FPIN_NONE, .pin_clear = KEEL_FPIN_NONE,
        .has_profile_update = false,
        .enters_copy_mode = false, .exits_copy_mode = false,
        .query_complete = false,
        .stmt_replay_accepted = false,
        .splice_eligible = false,
        .commit_doubt_outcome_changed = false,
        .commit_doubt_outcome = 0,
    };
}

/* ============================================================================
 * Built-in Protocol Flow Declarations
 * ============================================================================ */

extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
extern const keel_proto_flow_vtable_t keel_proto_flow_mysql;

/* ============================================================================
 * Protocol Flow Registry
 * ============================================================================ */

/**
 * @brief Register a protocol flow implementation
 */
int keel_proto_flow_register(const keel_proto_flow_vtable_t* vtable);

/**
 * @brief Get protocol flow by name
 */
const keel_proto_flow_vtable_t* keel_proto_flow_get(const char* name);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PROTOCOL_FLOW_H */
