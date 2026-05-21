/**
 * @file postgres_flow_internal.h
 * @brief Internal PostgreSQL flow-plugin state shared with white-box tests.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This header exposes the internal session model used by the PostgreSQL flow
 * plugin. It is intentionally not part of the public protocol API because it
 * couples directly to the implementation's prepared-statement virtualization,
 * transaction-state tracking, replication-consistency probes, and session-profile
 * bookkeeping. Tests include it so they can validate the invariants of those
 * mechanisms directly.
 */

#ifndef KEEL_POSTGRES_FLOW_INTERNAL_H
#define KEEL_POSTGRES_FLOW_INTERNAL_H

#include "keel/protocol/protocol_flow.h"   /* keel_ps_mode_t, effect/route/pin types */
#include "keel/core/auth.h"                /* keel_auth_manager_t, keel_auth_context_t */
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* ============================================================================
 * Named Prepared Statement Cache
 * ============================================================================ */

/** Maximum number of named prepared statements tracked per session.
 *  sysbench: 8 query types × 10 tables = 80 statements; 128 gives headroom. */
#define PG_STMT_CACHE_SIZE 128

/** Maximum Parse wire message length stored for replay (§17).
 *  Messages larger than this cause the session to fall back to hard-pin. */
#define PG_STMT_MAX_WIRE 4096

typedef struct pg_stmt_entry {
    char     name[64];           /**< Statement name (empty for unnamed) */
    char     sql[256];           /**< Truncated SQL for hook context display */
    size_t   sql_len;
    uint32_t query_type;         /**< keel_query_type_t */
    keel_query_effect_flags_t effect;
    keel_flow_route_t         route;
    keel_flow_pin_reason_t    pin_set;
    keel_flow_pin_reason_t    pin_clr;
    bool     valid;
    /** Full Parse wire message for prepared-statement replay (spec §17).
     *  Heap-alloc'd; NULL if the message exceeded PG_STMT_MAX_WIRE bytes. */
    uint8_t* wire_msg;
    size_t   wire_msg_len;
    /** Per-stmt hash (fnv1a of name+sql) contributed to session_stmt_hash via XOR. */
    uint64_t hash;
    /** Semantic context signature for this statement definition. */
    uint64_t context_sig;
    /** true once ParseComplete has been received — only then is the stmt
     *  included in session_stmt_hash and replay buffers. */
    bool     confirmed;
} pg_stmt_entry_t;

/* ============================================================================
 * Anonymous Mode: stmt_name → SQL text map
 * ============================================================================ */

/** Capacity matches PG_STMT_CACHE_SIZE so both structures stay in sync. */
#define PG_ANON_MAP_SIZE PG_STMT_CACHE_SIZE

typedef struct pg_anon_entry {
    char   name[64];     /**< Statement name; empty name means unused slot. */
    char   sql[2048];    /**< Full SQL text of the prepared statement body. */
    size_t sql_len;
} pg_anon_entry_t;

/* ============================================================================
 * Session Context
 * ============================================================================ */

typedef struct pg_flow_ctx {
    bool     startup_complete;
    char     username[64];
    char     database[64];
    char     application_name[64];
    char     txn_status;          /**< 'I' idle, 'T' in-transaction, 'E' error */
    bool     in_transaction;
    bool     in_copy;
    bool     reusable;
    uint8_t  handshake_buf[1024];
    size_t   handshake_len;
    uint32_t backend_pid;
    uint32_t backend_secret;
    uint32_t named_stmt_count;    /**< Named prepared statements on backend. */
    char     stmt_search_path[256]; /**< Current stmt-relevant search_path. */
    char     stmt_search_path_session[256]; /**< Session-level search_path baseline. */
    char     stmt_search_path_local[256]; /**< Transaction-local search_path override. */
    bool     stmt_search_path_local_active; /**< true when a tx-local search_path is active. */
    char     stmt_timezone[256]; /**< Current stmt-relevant TimeZone. */
    char     stmt_timezone_session[256]; /**< Session-level TimeZone baseline. */
    char     stmt_timezone_local[256]; /**< Transaction-local TimeZone override. */
    bool     stmt_timezone_local_active; /**< true when a tx-local TimeZone is active. */
    char     stmt_datestyle[256]; /**< Current stmt-relevant DateStyle. */
    char     stmt_datestyle_session[256]; /**< Session-level DateStyle baseline. */
    char     stmt_datestyle_local[256]; /**< Transaction-local DateStyle override. */
    bool     stmt_datestyle_local_active; /**< true when a tx-local DateStyle is active. */
    char     stmt_intervalstyle[256]; /**< Current stmt-relevant IntervalStyle. */
    char     stmt_intervalstyle_session[256]; /**< Session-level IntervalStyle baseline. */
    char     stmt_intervalstyle_local[256]; /**< Transaction-local IntervalStyle override. */
    bool     stmt_intervalstyle_local_active; /**< true when a tx-local IntervalStyle is active. */
    char     stmt_standard_conforming_strings[256]; /**< Current stmt-relevant standard_conforming_strings. */
    char     stmt_standard_conforming_strings_session[256]; /**< Session-level standard_conforming_strings baseline. */
    char     stmt_standard_conforming_strings_local[256]; /**< Transaction-local standard_conforming_strings override. */
    bool     stmt_standard_conforming_strings_local_active; /**< true when a tx-local standard_conforming_strings is active. */
    char     stmt_backslash_quote[256]; /**< Current stmt-relevant backslash_quote. */
    char     stmt_backslash_quote_session[256]; /**< Session-level backslash_quote baseline. */
    char     stmt_backslash_quote_local[256]; /**< Transaction-local backslash_quote override. */
    bool     stmt_backslash_quote_local_active; /**< true when a tx-local backslash_quote is active. */
    char     stmt_escape_string_warning[256]; /**< Current stmt-relevant escape_string_warning. */
    char     stmt_escape_string_warning_session[256]; /**< Session-level escape_string_warning baseline. */
    char     stmt_escape_string_warning_local[256]; /**< Transaction-local escape_string_warning override. */
    bool     stmt_escape_string_warning_local_active; /**< true when a tx-local escape_string_warning is active. */
    char     stmt_default_tablespace[256]; /**< Current stmt-relevant default_tablespace. */
    char     stmt_default_tablespace_session[256]; /**< Session-level default_tablespace baseline. */
    char     stmt_default_tablespace_local[256]; /**< Transaction-local default_tablespace override. */
    bool     stmt_default_tablespace_local_active; /**< true when a tx-local default_tablespace is active. */
    char     stmt_temp_tablespaces[256]; /**< Current stmt-relevant temp_tablespaces. */
    char     stmt_temp_tablespaces_session[256]; /**< Session-level temp_tablespaces baseline. */
    char     stmt_temp_tablespaces_local[256]; /**< Transaction-local temp_tablespaces override. */
    bool     stmt_temp_tablespaces_local_active; /**< true when a tx-local temp_tablespaces is active. */
    char     stmt_default_table_access_method[256]; /**< Current stmt-relevant default_table_access_method. */
    char     stmt_default_table_access_method_session[256]; /**< Session-level default_table_access_method baseline. */
    char     stmt_default_table_access_method_local[256]; /**< Transaction-local default_table_access_method override. */
    bool     stmt_default_table_access_method_local_active; /**< true when a tx-local default_table_access_method is active. */
    char     stmt_row_security[256]; /**< Current stmt-relevant row_security. */
    char     stmt_row_security_session[256]; /**< Session-level row_security baseline. */
    char     stmt_row_security_local[256]; /**< Transaction-local row_security override. */
    bool     stmt_row_security_local_active; /**< true when a tx-local row_security is active. */
    char     stmt_work_mem[256]; /**< Current stmt-relevant work_mem. */
    char     stmt_work_mem_session[256]; /**< Session-level work_mem baseline. */
    char     stmt_work_mem_local[256]; /**< Transaction-local work_mem override. */
    bool     stmt_work_mem_local_active; /**< true when a tx-local work_mem is active. */
    char     stmt_statement_timeout[256]; /**< Current stmt-relevant statement_timeout. */
    char     stmt_statement_timeout_session[256]; /**< Session-level statement_timeout baseline. */
    char     stmt_statement_timeout_local[256]; /**< Transaction-local statement_timeout override. */
    bool     stmt_statement_timeout_local_active; /**< true when a tx-local statement_timeout is active. */
    char     stmt_lock_timeout[256]; /**< Current stmt-relevant lock_timeout. */
    char     stmt_lock_timeout_session[256]; /**< Session-level lock_timeout baseline. */
    char     stmt_lock_timeout_local[256]; /**< Transaction-local lock_timeout override. */
    bool     stmt_lock_timeout_local_active; /**< true when a tx-local lock_timeout is active. */
    char     stmt_client_encoding[256]; /**< Current stmt-relevant client_encoding. */
    char     stmt_client_encoding_session[256]; /**< Session-level client_encoding baseline. */
    char     stmt_client_encoding_local[256]; /**< Transaction-local client_encoding override. */
    bool     stmt_client_encoding_local_active; /**< true when a tx-local client_encoding is active. */
    char     stmt_role[64];       /**< Current stmt-relevant effective role. */
    char     stmt_session_auth[64]; /**< Current stmt-relevant session auth. */
    uint64_t stmt_temp_epoch;     /**< Conservative temp-object context epoch. */
    uint64_t stmt_schema_epoch;   /**< Schema/DDL epoch for PS semantic invalidation. */
    uint64_t stmt_role_hash;      /**< Cached role/session-auth hash component. */
    uint64_t stmt_search_path_hash; /**< Cached search_path hash component. */
    uint64_t stmt_guc_hash;       /**< Cached tracked-GUC hash component. */
    bool     stmt_semantic_unknown; /**< Conservative semantic-unknown marker. */
    bool     stmt_temp_tx_reset_pending; /**< Temp context changes again when txn ends. */
    bool     stmt_temp_tx_rollback_reset_pending; /**< Temp context changes again if the txn rolls back. */
    bool     stmt_last_tx_end_was_rollback; /**< Most recent tx-end statement was ROLLBACK. */
    uint64_t stmt_context_sig;    /**< Semantic signature for current stmt context. */

    /* Extended Query: per-statement classification cache.
     * Populated by Parse, looked up by Bind to set active classification
     * for the next Execute.  Indexed by statement name. */
    pg_stmt_entry_t stmt_cache[PG_STMT_CACHE_SIZE];
    uint32_t         stmt_evict_next;    /**< Round-robin eviction index. */
    uint64_t         session_stmt_hash; /**< XOR of each confirmed named-stmt .hash. */

    /* In-flight Parse: name/hash of the stmt awaiting ParseComplete.
     * XOR'd into session_stmt_hash only when ParseComplete arrives. */
    char             pending_parse_name[64];
    uint64_t         pending_parse_hash;
    bool             pending_parse_valid;

    /* Tracking-mode: pending simple-query PREPARE state for rollback on backend
     * rejection.  Set in the 'Q' PREPARE handler; cleared by CommandComplete("PREPARE")
     * or ErrorResponse.  pending_track_prior.wire_msg is heap-owned here — must be
     * freed before it is overwritten or on context destroy. */
    char            pending_track_name[64];
    bool            pending_track_valid;
    bool            pending_track_had_prior;
    pg_stmt_entry_t pending_track_prior;

    /* Tracking-mode: pending by-name DEALLOCATE state.
     * Set when DEALLOCATE <name> is forwarded to the backend; cleared on ReadyForQuery.
     * If the backend returns "prepared statement does not exist" the error is absorbed
     * and a synthetic CommandComplete("DEALLOCATE")+ReadyForQuery is injected so the
     * client sees session-level success. */
    bool            pending_deallocate_valid;
    bool            pending_deallocate_absorbed_error;
    bool            pending_deallocate_complete;

    /* Active classification for the next Execute message.
     * Set by Parse (unnamed stmt) or by Bind (looks up named stmt). */
    char     cached_sql[2048];    /**< SQL from active stmt (NUL-terminated). */
    size_t   cached_sql_len;
    uint32_t cached_query_type;   /**< keel_query_type_t */
    keel_query_effect_flags_t cached_effect;
    keel_flow_route_t         cached_route;
    keel_flow_pin_reason_t    cached_pin_set;
    keel_flow_pin_reason_t    cached_pin_clr;
    bool     cached_valid;        /**< true after a valid Parse/Bind. */

    /** Prepared-statement pooling mode (copied from worker config).
     *  Controls behaviour of PREPARE/EXECUTE in Simple Query path. */
    keel_ps_mode_t ps_mode;

    /* ---- Replication-tracking (spec §TXN-TRACK) ----
     *
     * When txn_tracking is true, a COMMIT simple query is rewritten to
     *   SELECT txid_current() AS _keel_txid; COMMIT;
     * The DataRow / CommandComplete("SELECT") responses are absorbed here;
     * the captured XID is signalled to the engine via keel_be_action_t
     * .commit_xid_captured / .commit_xid.
     */
    bool     txn_tracking;       /**< transaction_tracking config enabled */
    bool     xid_probe_active;   /**< Rewritten COMMIT in flight; absorb SELECT results */
    uint64_t xid_probe_result;   /**< txid_current() captured from DataRow */
    bool     commit_doubt_check_active; /**< txid_status() check stream in progress */
    uint8_t  commit_doubt_outcome;      /**< 0=unknown, 1=committed, 2=aborted */

    /* Anonymous mode: stmt_name → full SQL text mapping.
     * Parse messages are intercepted and stored here rather than sent to the
     * backend; Bind messages look up the SQL and build a one-shot anon Parse.
     *
     * Heap-allocated on first use (only in ANONYMOUS ps_mode) to avoid
     * paying ~271 KB per session in non-anonymous modes. */
    pg_anon_entry_t* anon_map;         /**< NULL unless ps_mode == ANONYMOUS */

    /* Anonymous mode: synthetic response buffer.
     * Holds ParseComplete / NoData sent directly back to the client.
     * ParseComplete is 5 bytes; 32 bytes is generous. */
    uint8_t anon_resp_buf[32];
    size_t  anon_resp_len;

    /* Anonymous mode: rewritten Parse+Bind buffer forwarded to backend.
     * Allocated on demand; freed in pgf_destroy(). */
    uint8_t* anon_rewrite_buf;
    size_t   anon_rewrite_cap;

    /* Tracking mode: after transaction-local temp context changes, force the
     * next SQL EXECUTE through DISCARD PLANS + EXECUTE and absorb the DISCARD
     * command tag so PostgreSQL replans against the temp namespace. */
    bool     stmt_discard_plans_before_execute;
    bool     stmt_discard_plans_absorb_pending;
    uint8_t* stmt_discard_plans_rewrite_buf;
    size_t   stmt_discard_plans_rewrite_cap;

    /* Cross-service RYW: latest captured write LSN (from notify_write_lsn).
     * Returned verbatim in response to SHOW keel.write_lsn queries. */
    char     keel_write_lsn[128];

    /* Cross-service RYW: synthetic response buffer for SET/SHOW keel.* intercepts.
     * Holds CommandComplete + ReadyForQuery (or a small result set).
     * 512 bytes fits RowDescription + DataRow(128-byte LSN) + C + Z comfortably. */
    uint8_t  ryw_resp_buf[512];
    size_t   ryw_resp_len;

    /* Plugin metrics counters (surfaced via pgf_get_metrics) */
    uint64_t metrics_state_changes;          /**< Quarantine promotions, TEMP TABLE etc. */
    uint64_t metrics_consistency_fetches;    /**< WAL LSN / GTID probes issued */
    uint64_t metrics_consistency_token_lat_ns; /**< Last consistency-token fetch latency (ns) */
    uint64_t metrics_cleanup_count;          /**< DISCARD ALL / cleanup round-trips */
    uint64_t metrics_cleanup_lat_ns;         /**< Last cleanup-slot latency (ns) */
    uint64_t metrics_classify_count;         /**< SQL classifications performed */
    uint64_t metrics_errors_classified;      /**< Classifications that hit error path */
    /* ---- Enterprise authentication state ----
     *
     * auth_manager:  Non-owning pointer to the worker's shared auth manager.
     *                Set by pgf_create() from session->worker->auth_manager.
     *                NULL in trust mode (backwards-compatible default).
     *
     * auth_ctx:      Per-connection auth context allocated by the provider
     *                during start() and freed when auth completes or fails.
     *
     * auth_pending:  True while a challenge has been sent to the client and
     *                the provider is waiting for the client's response (or
     *                next response in a multi-round exchange like SCRAM).
     */
    keel_auth_manager_t*  auth_manager;      /**< Non-owning ptr to worker auth manager */
    keel_auth_context_t*  auth_ctx;          /**< In-progress per-connection auth context */
    bool                  auth_pending;      /**< Challenge sent; awaiting client response */
    int                   auth_round;        /**< 0-based count of 'p' messages received (for SASL) */
    /* Cancel-RFQ suppression: set when a query_canceled (SQLSTATE 57014)
     * ErrorResponse is received; cleared after the subsequent ReadyForQuery
     * is absorbed.  Prevents a stale Z from landing in the client socket
     * buffer between the cancel error and the next query's response. */
    bool                  cancel_rfq_suppress;
} pg_flow_ctx_t;

#endif /* KEEL_POSTGRES_FLOW_INTERNAL_H */
