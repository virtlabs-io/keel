/**
 * @file mysql_flow.c
 * @brief MySQL/MariaDB wire-protocol flow plugin and session-state interpreter.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The MySQL flow plugin translates MySQL-family command packets into engine actions
 * while tracking enough session semantics to keep connection pooling safe. Compared
 * with PostgreSQL, the wire protocol is simpler in some areas, but the session model
 * still contains many pool-hostile features such as user variables, explicit locks,
 * prepared statements, `LOAD DATA LOCAL INFILE`, and multi-result command sequences.
 * This file encodes those semantics so the engine can route, pin, clean up, or reject
 * reuse based on authoritative protocol knowledge rather than heuristic guesses.
 */

#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/protocol.h"
#include "keel/protocol/mysql_backend_auth.h"
#include "keel/sql/sql.h"
#include "keel_types.h"
#include "keel/plugin/plugin_types.h"
#include "keel/session/state_profile.h"
#include "keel/session/hardpin.h"
#include "keel/engine/worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>     /* struct timeval — needed explicitly on musl libc */
#include "keel/util/platform_compat.h"

/* ============================================================================
 * MySQL Wire Protocol Constants
 * ============================================================================ */

#define MY_HDR 4             /* 3-byte LE length + 1-byte seq_id */
#define MY_MAX_PKT 0xFFFFFF  /* 16 MB - 1 */

/* Client command bytes */
#define MY_COM_QUIT             0x01
#define MY_COM_INIT_DB          0x02
#define MY_COM_QUERY            0x03
#define MY_COM_FIELD_LIST       0x04
#define MY_COM_PING             0x0e
#define MY_COM_CHANGE_USER      0x11
#define MY_COM_STMT_PREPARE     0x16
#define MY_COM_STMT_EXECUTE     0x17
#define MY_COM_STMT_SEND_LONG   0x18
#define MY_COM_STMT_CLOSE       0x19
#define MY_COM_STMT_RESET       0x1a
#define MY_COM_SET_OPTION       0x1b
#define MY_COM_STMT_FETCH       0x1c
#define MY_COM_RESET_CONNECTION 0x1f
#define MY_COM_PROCESS_KILL    0x0c   /**< Cancel a query by connection ID */

/* Server response markers */
#define MY_OK  0x00
#define MY_ERR 0xFF
#define MY_EOF 0xFE
#define MY_LOCAL_INFILE 0xFB

/* SERVER_STATUS flags (bit positions in status_flags LE16) */
#define MY_SERVER_STATUS_IN_TRANS            (1U << 0)
#define MY_SERVER_STATUS_AUTOCOMMIT          (1U << 1)
#define MY_SERVER_MORE_RESULTS_EXISTS        (1U << 3)
#define MY_SERVER_SESSION_STATE_CHANGED      (1U << 14)

/* SESSION_TRACK data types (MySQL 5.7+, following info string in OK packet) */
#define MY_SESSION_TRACK_SYSTEM_VARIABLES    0
#define MY_SESSION_TRACK_SCHEMA              1
#define MY_SESSION_TRACK_STATE_CHANGE        2
#define MY_SESSION_TRACK_GTIDS               3
#define MY_SESSION_TRACK_TRANSACTION_CHARACTERISTICS 4
#define MY_SESSION_TRACK_TRANSACTION_STATE   5

/* Capability flags */
#define MY_CLIENT_CONNECT_WITH_DB     (1U << 3)
#define MY_CLIENT_PROTOCOL_41         (1U << 9)
#define MY_CLIENT_MULTI_STATEMENTS    (1U << 16)
#define MY_CLIENT_MULTI_RESULTS       (1U << 17)
#define MY_CLIENT_PLUGIN_AUTH         (1U << 19)

/* ----------------------------------------------------------------------------
 * Note on MySQL commit-in-doubt resolution.
 *
 * A previous iteration ("Phase B") rewrote bare COMMIT into the multi-statement
 * payload `SELECT @@gtid_executed AS _keel_token;COMMIT` to harvest a GTID set
 * BEFORE the COMMIT executed.  That design was unsound: the captured token is
 * the PRE-COMMIT @@global.gtid_executed set, which is by definition already a
 * subset of any future @@global.gtid_executed.  Resolving a lost COMMIT via
 * `GTID_SUBSET(<pre-commit-set>, @@global.gtid_executed)` therefore always
 * returns 1 and would falsely report a lost commit as committed.
 *
 * MySQL commit-in-doubt resolution now follows a single sound path:
 *   - The plugin only sets `commit_xid_captured` on the COMMIT's OK packet,
 *     and only when that OK actually carried a SESSION_TRACK_GTIDS entry
 *     (requires `session_track_gtids = OWN_GTID` on the server).
 *   - If the client disconnects before that OK arrives, no token is captured
 *     and the engine reports the outcome as UNKNOWN — never as committed.
 * ---------------------------------------------------------------------------- */

/* ============================================================================
 * Result-Set State Machine
 * ============================================================================
 *
 * After COM_QUERY returns a result set (column_count > 0):
 *   1) Server sends column_count column definition packets
 *   2) Server sends an EOF packet (end-of-column-definitions)
 *   3) Server sends data rows (text ResultsetRow packets)
 *   4) Server sends an EOF packet (end-of-data, with status flags)
 *
 * We track this to know when query_complete should be set.
 */
typedef enum my_result_state {
    MY_RS_IDLE = 0,        /**< Not expecting result set */
    MY_RS_COLUMNS,         /**< Waiting for column definitions + EOF */
    MY_RS_ROWS,            /**< Waiting for data rows + EOF */
    MY_RS_STMT_PARAM_DEFS, /**< PREPARE_OK: waiting for param defs + EOF */
    MY_RS_STMT_COL_DEFS,   /**< PREPARE_OK: waiting for column defs + EOF */
} my_result_state_t;

/* ============================================================================
 * Prepared Statement Map
 * ============================================================================
 *
 * MySQL prepared statements are identified by a server-assigned uint32 stmt_id
 * returned in the PREPARE_OK packet.  We track up to MY_STMT_MAP_SIZE live
 * statements per session so that:
 *   1. KEEL_FPIN_PREPARED_STMT can be cleared when the last statement is closed.
 *   2. get_stmt_replay() can replay all active COM_STMT_PREPARE messages when
 *      a backend connection is reused across a different frontend session.
 */
#define MY_STMT_MAP_SIZE  64    /**< Max tracked prepared stmts per session */
#define MY_STMT_SQL_MAX  1024   /**< Max SQL length stored for replay */

typedef struct my_stmt_entry {
    uint32_t stmt_id;           /**< Backend-assigned statement identifier */
    bool     valid;
    char     sql[MY_STMT_SQL_MAX]; /**< Original SQL text for replay */
    size_t   sql_len;
} my_stmt_entry_t;

/* ============================================================================
 * Flow Context
 * ============================================================================ */

typedef struct my_flow_ctx {
    /* ---- Connection state ---- */
    bool             handshake_complete;
    bool             in_transaction;
    bool             in_copy;        /**< LOAD DATA LOCAL INFILE active */
    bool             reusable;
    uint8_t          seq_id;
    uint8_t          last_command;

    /* ---- Client identity ---- */
    char             username[64];
    char             database[64];

    /* ---- Synthetic greeting / auth-ok buffers ---- */
    uint8_t          greeting_buf[256];
    size_t           greeting_len;
    uint8_t          ok_buf[16];
    size_t           ok_len;

    /* ---- Result-set tracking ---- */
    my_result_state_t result_state;
    uint32_t         columns_remaining;
    uint32_t         stmt_columns_total;  /**< Saved num_columns from PREPARE_OK */

    /* ---- Prepared statement tracking ---- */
    my_stmt_entry_t  stmt_map[MY_STMT_MAP_SIZE];
    uint32_t         stmt_active_count;   /**< Live entries in stmt_map */
    uint64_t         session_stmt_hash;   /**< FNV-1a XOR of all active stmt_id hashes */
    char             pending_prepare_sql[MY_STMT_SQL_MAX];  /**< SQL from COM_STMT_PREPARE (awaiting PREPARE_OK) */
    size_t           pending_prepare_sql_len;

    /* ---- Cross-service Read-Your-Writes ---- */
    char             keel_write_gtid[512]; /**< Latest captured write GTID (GTID_EXECUTED) */
    uint8_t          ryw_resp_buf[1024];   /**< Synthetic response buffer for RYW intercepts */
    size_t           ryw_resp_len;

    /* ---- Metrics (Phase 5) ---- */
    uint64_t         state_changes;
    uint64_t         cleanup_count;

    /* ---- Cancel support ---- */
    uint32_t         synthetic_conn_id;  /**< (worker_id << 16 | slab_index) for KILL QUERY routing */

    /* ---- Commit-in-doubt: post-COMMIT GTID-token capture ----
     *
     * `commit_pending` flips to true when the FE side sees a COMMIT
     * issued via COM_QUERY (qtype == KEEL_QUERY_COMMIT) and stays set
     * until the BE side observes the corresponding OK / ERR packet.
     * On the OK, if `keel_write_gtid` was refreshed via the OK packet's
     * SESSION_TRACK_GTIDS payload, the BE handler hashes it into
     * `keel_be_action_t::commit_xid` so the engine can resolve a later
     * connection drop via the GTID_SUBSET probe built by
     * `myf_build_commit_doubt_check`. */
    bool             commit_pending;

    /* ---- Statement-compat profile hashes (mirror PG) ----
     *
     * Populated lazily as we observe handshake completion and tracked
     * SET / USE statements. Surfaced via `myf_get_stmt_compat_profile`
     * so the engine can hash-match a returning backend with a
     * compatible session profile before reuse. */
    uint64_t         stmt_role_hash;    /**< Hash of authenticated username */
    uint64_t         stmt_db_hash;       /**< Hash of current database (USE / COM_INIT_DB) */
    uint64_t         stmt_guc_hash;      /**< XOR-fold of tracked GUC kv hashes (sql_mode, time_zone, ...) */
    bool             stmt_semantic_unknown; /**< True after an untracked SET we cannot model */
} my_flow_ctx_t;

/* ============================================================================
 * Byte Helpers
 * ============================================================================ */

/**
 * @brief Read a 24-bit little-endian integer from a 3-byte buffer.
 * @param p Pointer to at least 3 bytes.
 * @return Decoded unsigned 32-bit value.
 */
static inline uint32_t rdle24(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}

/**
 * @brief Write a 24-bit little-endian integer into a 3-byte buffer.
 * @param p Destination buffer (must hold at least 3 bytes).
 * @param v Value to encode; only the lower 24 bits are written.
 */
static inline void wrle24(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16);
}

/**
 * @brief Read a MySQL length-encoded integer
 *
 * Returns the decoded value and advances *pos past the encoding.
 * MySQL encoding:
 *   0x00-0xFB  → 1-byte (value itself)
 *   0xFC       → 2-byte LE follows
 *   0xFD       → 3-byte LE follows
 *   0xFE       → 8-byte LE follows
 *
 * @param data  Packet data
 * @param len   Total packet length
 * @param pos   Current offset (updated on return)
 * @return Decoded integer value, or (uint64_t)-1 on error
 */
static uint64_t read_lenenc(const uint8_t* data, size_t len, size_t* pos) {
    if (*pos >= len) return (uint64_t)-1;
    uint8_t first = data[*pos];
    if (first < 0xFB) {
        (*pos)++;
        return first;
    }
    if (first == 0xFC) {
        if (*pos + 3 > len) return (uint64_t)-1;
        uint64_t v = (uint64_t)data[*pos + 1] | ((uint64_t)data[*pos + 2] << 8);
        *pos += 3;
        return v;
    }
    if (first == 0xFD) {
        if (*pos + 4 > len) return (uint64_t)-1;
        uint64_t v = (uint64_t)data[*pos + 1] |
                     ((uint64_t)data[*pos + 2] << 8) |
                     ((uint64_t)data[*pos + 3] << 16);
        *pos += 4;
        return v;
    }
    if (first == 0xFE) {
        if (*pos + 9 > len) return (uint64_t)-1;
        uint64_t v = 0;
        for (int i = 0; i < 8; i++)
            v |= (uint64_t)data[*pos + 1 + i] << (i * 8);
        *pos += 9;
        return v;
    }
    /* 0xFF is not a valid lenenc prefix */
    return (uint64_t)-1;
}

/**
 * @brief Read a MySQL length-encoded string.
 *
 * Decodes the length prefix, then returns a pointer into @p data and the
 * decoded string length.  Advances @p pos past the complete encoding.
 *
 * @param data   Packet data.
 * @param len    Total packet length.
 * @param pos    Current offset (updated on return).
 * @param out    [out] Points into @p data at the start of the string content.
 * @param out_len [out] Decoded string length.
 * @return `0` on success, `-1` on truncation / decode error.
 */
static int read_lenenc_str(const uint8_t* data, size_t len, size_t* pos,
                           const char** out, size_t* out_len) {
    uint64_t slen = read_lenenc(data, len, pos);
    if (slen == (uint64_t)-1) return -1;
    if (*pos + slen > len)     return -1;
    *out     = (const char*)(data + *pos);
    *out_len = (size_t)slen;
    *pos += (size_t)slen;
    return 0;
}

/* ============================================================================
 * Synthetic Greeting Builder
 * ============================================================================ */

/* ============================================================================
 * Prepared Statement Map Helpers
 * ============================================================================ */

/**
 * @brief Compute a per-statement hash contribution for the session hash.
 *
 * Uses FNV-1a over the statement ID bytes so that different IDs produce
 * distinct hash values that can be XOR-folded into session_stmt_hash.
 */
static uint64_t my_stmt_id_hash(uint32_t stmt_id) {
    uint64_t h = 14695981039346656037ULL;
    for (int i = 0; i < 4; i++) {
        h ^= (uint64_t)((stmt_id >> (i * 8)) & 0xFF);
        h *= 1099511628211ULL;
    }
    return h;
}

/**
 * @brief FNV-1a 64 hash over an arbitrary byte range (case-sensitive).
 *
 * Used for session-profile hashes (role / db / GUC kv) so that distinct
 * inputs produce distinct uint64 values suitable for XOR-folding or
 * equality comparison by the engine's session reuse path.
 */
static uint64_t my_fnv64(const void* data, size_t len) {
    const uint8_t* p = (const uint8_t*)data;
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint64_t)p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/**
 * @brief Lowercase-folded FNV-1a 64 for case-insensitive GUC name match.
 */
static uint64_t my_fnv64_ci(const void* data, size_t len) {
    const uint8_t* p = (const uint8_t*)data;
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        uint8_t c = p[i];
        if (c >= 'A' && c <= 'Z') c = (uint8_t)(c + 32);
        h ^= (uint64_t)c;
        h *= 1099511628211ULL;
    }
    return h;
}

/**
 * @brief Return true if @p name (length @p nl) is a session-GUC we track
 *        deterministically in `stmt_guc_hash`.
 *
 * The set mirrors the MySQL session variables that materially change
 * statement semantics on reuse: SQL mode, time zone, autocommit,
 * character-set / collation pinning. Any SET that is NOT in this list
 * forces `stmt_semantic_unknown` to true so the engine treats the
 * session as opaque for reuse purposes.
 */
static bool my_is_tracked_guc(const char* name, size_t nl) {
    static const char* kTracked[] = {
        "sql_mode", "time_zone", "autocommit",
        "character_set_client", "character_set_results",
        "character_set_connection", "collation_connection",
        "transaction_isolation", "tx_isolation",
        "transaction_read_only", "tx_read_only",
        "foreign_key_checks", "unique_checks",
    };
    for (size_t i = 0; i < sizeof(kTracked) / sizeof(kTracked[0]); i++) {
        size_t kl = strlen(kTracked[i]);
        if (kl != nl) continue;
        bool match = true;
        for (size_t j = 0; j < nl; j++) {
            char a = name[j];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (a != kTracked[i][j]) { match = false; break; }
        }
        if (match) return true;
    }
    return false;
}

/**
 * @brief Register a newly-prepared statement in the session map.
 *
 * Called from the BE message handler when PREPARE_OK (0x00) is received
 * after a COM_STMT_PREPARE.  Finds a free slot, stores the stmt_id and
 * SQL text, increments stmt_active_count, and XOR-folds the new entry's
 * hash into session_stmt_hash.
 *
 * @param ctx      Flow context.
 * @param stmt_id  Backend-assigned statement identifier.
 * @param sql      SQL text from the pending_prepare_sql buffer.
 * @param sql_len  Length of @p sql in bytes.
 */
static void my_stmt_add(my_flow_ctx_t* ctx, uint32_t stmt_id,
                        const char* sql, size_t sql_len) {
    /* Find a free slot */
    for (int i = 0; i < MY_STMT_MAP_SIZE; i++) {
        if (!ctx->stmt_map[i].valid) {
            ctx->stmt_map[i].stmt_id = stmt_id;
            ctx->stmt_map[i].valid   = true;
            size_t copy_len = sql_len < MY_STMT_SQL_MAX - 1 ? sql_len : MY_STMT_SQL_MAX - 1;
            memcpy(ctx->stmt_map[i].sql, sql, copy_len);
            ctx->stmt_map[i].sql[copy_len] = '\0';
            ctx->stmt_map[i].sql_len = copy_len;
            ctx->stmt_active_count++;
            ctx->session_stmt_hash ^= my_stmt_id_hash(stmt_id);
            return;
        }
    }
    /* Map full — increment count only; stmt cannot be replayed */
    ctx->stmt_active_count++;
}

/**
 * @brief Remove a prepared statement from the session map on COM_STMT_CLOSE.
 *
 * Locates the entry by stmt_id, clears it, decrements stmt_active_count,
 * and removes the entry's hash contribution from session_stmt_hash.
 *
 * @param ctx     Flow context.
 * @param stmt_id Statement ID from the COM_STMT_CLOSE payload.
 */
static void my_stmt_remove(my_flow_ctx_t* ctx, uint32_t stmt_id) {
    for (int i = 0; i < MY_STMT_MAP_SIZE; i++) {
        if (ctx->stmt_map[i].valid && ctx->stmt_map[i].stmt_id == stmt_id) {
            ctx->session_stmt_hash ^= my_stmt_id_hash(stmt_id);
            memset(&ctx->stmt_map[i], 0, sizeof(ctx->stmt_map[i]));
            if (ctx->stmt_active_count > 0)
                ctx->stmt_active_count--;
            return;
        }
    }
    /* Not found — may have been evicted when map was full; still decrement */
    if (ctx->stmt_active_count > 0)
        ctx->stmt_active_count--;
}

/**
 * @brief Clear all prepared statement entries (e.g., after COM_RESET_CONNECTION).
 *
 * @param ctx Flow context.
 */
static void my_stmt_clear_all(my_flow_ctx_t* ctx) {
    memset(ctx->stmt_map, 0, sizeof(ctx->stmt_map));
    ctx->stmt_active_count  = 0;
    ctx->session_stmt_hash  = 0;
    ctx->pending_prepare_sql_len = 0;
}

/* ============================================================================
 * Cross-Service Read-Your-Writes (RYW) Helpers
 * ============================================================================
 *
 * MySQL RYW works via GTID sets.  The proxy intercepts two pseudo-commands:
 *
 *   SET @keel_write_gtid = '<gtid-set>'
 *       Client tells the proxy "I just performed a write at this GTID position;
 *       subsequent reads must reach at least this point."  The proxy absorbs the
 *       SET, stores the GTID in inject_consistency_lsn (engine routes it to the
 *       session's consistency atom), and sends a synthetic OK to the client.
 *
 *   SELECT @keel_write_gtid
 *       Client wants to inspect the stored write GTID, typically to pass it to
 *       another connection.  The proxy builds a one-row result set and returns
 *       it without a backend round-trip.
 */

/**
 * @brief Return true if @p sql is a SET @keel_write_gtid assignment.
 *
 * Extracts the GTID value into @p out (NUL-terminated, max @p outsz bytes)
 * when the function returns true.
 *
 * Accepted forms (case-insensitive):
 *   SET @keel_write_gtid = '<gtid>'
 *   SET @keel_write_gtid='<gtid>'
 */
static bool my_try_parse_set_keel_write_gtid(const char* sql, size_t sl,
                                              char* out, size_t outsz) {
    const char* p = sql;
    const char* end = sql + sl;

    /* Skip whitespace */
    while (p < end && (*p == ' ' || *p == '\t')) p++;

    /* "SET " */
    if (end - p < 4) return false;
    if ((p[0]!='S'&&p[0]!='s') || (p[1]!='E'&&p[1]!='e') ||
        (p[2]!='T'&&p[2]!='t') || p[3]!=' ') return false;
    p += 4;
    while (p < end && *p == ' ') p++;

    /* "@keel_write_gtid" */
    const char* key = "@keel_write_gtid";
    size_t kl = 16;
    if ((size_t)(end - p) < kl) return false;
    for (size_t i = 0; i < kl; i++) {
        char c = p[i]; if (c >= 'A' && c <= 'Z') c += 32;
        if (c != key[i]) return false;
    }
    p += kl;
    while (p < end && (*p == ' ' || *p == '\t')) p++;
    if (p >= end || *p != '=') return false;
    p++;
    while (p < end && (*p == ' ' || *p == '\t')) p++;

    /* Strip optional enclosing quotes */
    const char* vstart = p;
    const char* vend   = end;
    while (vend > vstart && (vend[-1] == ' ' || vend[-1] == ';' ||
                              vend[-1] == '\n' || vend[-1] == '\r'))
        vend--;
    if (vend - vstart >= 2 &&
        ((vstart[0] == '\'' && vend[-1] == '\'') ||
         (vstart[0] == '"'  && vend[-1] == '"')))
        { vstart++; vend--; }

    size_t vlen = (size_t)(vend - vstart);
    if (vlen == 0) return false;
    size_t copy = vlen < outsz - 1 ? vlen : outsz - 1;
    memcpy(out, vstart, copy);
    out[copy] = '\0';
    return true;
}

/**
 * @brief Return true if @p sql is a SELECT @keel_write_gtid query.
 *
 * Accepts both `SELECT @keel_write_gtid` and `SELECT @@keel_write_gtid`
 * (case-insensitive), optionally followed by whitespace or semicolons.
 */
static bool my_is_select_keel_write_gtid(const char* sql, size_t sl) {
    const char* p = sql;
    const char* end = sql + sl;

    while (p < end && (*p == ' ' || *p == '\t')) p++;
    if (end - p < 7) return false;
    const char* sel = "select";
    for (int i = 0; i < 6; i++) {
        char c = p[i]; if (c >= 'A' && c <= 'Z') c += 32;
        if (c != sel[i]) return false;
    }
    p += 6;
    while (p < end && (*p == ' ' || *p == '\t')) p++;

    /* skip optional @@ */
    if (end - p >= 2 && p[0] == '@' && p[1] == '@') p += 2;
    else if (end - p >= 1 && p[0] == '@') p++;

    const char* key = "keel_write_gtid";
    size_t kl = 15;
    if ((size_t)(end - p) < kl) return false;
    for (size_t i = 0; i < kl; i++) {
        char c = p[i]; if (c >= 'A' && c <= 'Z') c += 32;
        if (c != key[i]) return false;
    }
    return true;
}

/**
 * @brief Build a synthetic MySQL result set for `SELECT @keel_write_gtid`.
 *
 * Emits the minimal MySQL text-protocol result set:
 *   column_count(1) → column_def → EOF → row_data → EOF
 *
 * @param value  The GTID string to return (may be empty).
 * @param buf    Output buffer.
 * @param bufsz  Buffer capacity in bytes.
 * @return Bytes written, or -1 if @p buf is too small.
 */
static ssize_t my_build_keel_select_gtid_response(const char* value,
                                                   uint8_t* buf, size_t bufsz,
                                                   uint8_t base_seq) {
    size_t val_len = strlen(value);
    const char* col_name = "@keel_write_gtid";
    size_t col_name_len  = 16;

    /* Estimate total: col_count(5) + col_def(~50) + eof(9) + row(5+val_len) + eof(9) */
    if (bufsz < 100 + val_len) return -1;

    uint8_t* p   = buf;
    uint8_t  seq = base_seq + 1;

    /* ---- Packet 1: column count = 1 ---- */
    wrle24(p, 1); p[3] = seq++; p[MY_HDR] = 0x01; p += MY_HDR + 1;

    /* ---- Packet 2: minimal column definition ----
     * catalog="def" schema="" table="" org_table="" name col_name org_name=""
     * filler(0x0c) charset(2) col_len(4) type(1=0xFD) flags(2) decimals(1) filler(2)
     */
    {
        uint8_t* pkt  = p;
        uint8_t* pay  = pkt + MY_HDR;
        uint8_t* pp   = pay;
        /* catalog lenenc "def" */
        *pp++ = 3; memcpy(pp, "def", 3); pp += 3;
        /* schema */
        *pp++ = 0;
        /* table */
        *pp++ = 0;
        /* org_table */
        *pp++ = 0;
        /* name */
        *pp++ = (uint8_t)col_name_len;
        memcpy(pp, col_name, col_name_len); pp += col_name_len;
        /* org_name */
        *pp++ = 0;
        /* filler = 0x0c */
        *pp++ = 0x0c;
        /* charset: utf8mb4 (33 = 0x21, 0x00) */
        pp[0] = 0x21; pp[1] = 0x00; pp += 2;
        /* column length: 512 */
        pp[0] = 0x00; pp[1] = 0x02; pp[2] = 0x00; pp[3] = 0x00; pp += 4;
        /* type: VAR_STRING = 0xFD */
        *pp++ = 0xFD;
        /* flags */
        pp[0] = 0x00; pp[1] = 0x00; pp += 2;
        /* decimals */
        *pp++ = 0x00;
        /* filler */
        pp[0] = 0x00; pp[1] = 0x00; pp += 2;

        size_t payload_len = (size_t)(pp - pay);
        wrle24(pkt, (uint32_t)payload_len);
        pkt[3] = seq++;
        p = pp;
    }

    /* ---- Packet 3: EOF (end of column definitions) ---- */
    wrle24(p, 5); p[3] = seq++;
    p[MY_HDR+0] = 0xFE; p[MY_HDR+1] = 0x00; p[MY_HDR+2] = 0x00;
    p[MY_HDR+3] = 0x02; p[MY_HDR+4] = 0x00;  /* status: SERVER_STATUS_AUTOCOMMIT */
    p += MY_HDR + 5;

    /* ---- Packet 4: row data (lenenc string) ---- */
    {
        size_t row_payload = (val_len < 0xFB) ? 1 + val_len : 3 + val_len;
        wrle24(p, (uint32_t)row_payload); p[3] = seq++;
        p += MY_HDR;
        if (val_len < 0xFB) {
            *p++ = (uint8_t)val_len;
        } else {
            *p++ = 0xFC;
            *p++ = (uint8_t)val_len;
            *p++ = (uint8_t)(val_len >> 8);
        }
        memcpy(p, value, val_len); p += val_len;
    }

    /* ---- Packet 5: EOF (end of row data) ---- */
    wrle24(p, 5); p[3] = seq++;
    p[MY_HDR+0] = 0xFE; p[MY_HDR+1] = 0x00; p[MY_HDR+2] = 0x00;
    p[MY_HDR+3] = 0x02; p[MY_HDR+4] = 0x00;
    p += MY_HDR + 5;

    return (ssize_t)(p - buf);
}

/**
 * @brief Construct a MySQL HandshakeV10 greeting packet in the flow context.
 *
 * Fills @c ctx->greeting_buf with the complete initial handshake packet and
 * sets @c ctx->greeting_len to the encoded length.  The synthetic connection
 * ID embedded in the packet allows `COM_PROCESS_KILL` to be routed without
 * contacting the real backend.
 *
 * @param ctx Flow context for the new frontend connection.
 */
static void build_greeting(my_flow_ctx_t* ctx) {
    uint8_t* p = ctx->greeting_buf;
    uint8_t* payload = p + MY_HDR;
    uint8_t* pp = payload;

    *pp++ = 10; /* protocol version */
    const char* ver = "8.0.0-keel";
    size_t vl = strlen(ver) + 1;
    memcpy(pp, ver, vl); pp += vl;
    /* connection id (4 bytes LE) — unique per session for KILL QUERY routing */
    pp[0] = (uint8_t)(ctx->synthetic_conn_id);
    pp[1] = (uint8_t)(ctx->synthetic_conn_id >> 8);
    pp[2] = (uint8_t)(ctx->synthetic_conn_id >> 16);
    pp[3] = (uint8_t)(ctx->synthetic_conn_id >> 24);
    pp += 4;
    /* scramble part 1 (8 bytes) + filler */
    memset(pp, 0x41, 8); pp += 8;
    *pp++ = 0; /* filler */
    /* capability flags lower 2 bytes */
    pp[0] = 0xff; pp[1] = 0xf7; pp += 2;
    /* charset: utf8mb4 (0x2d = 45) */
    *pp++ = 0x2d;
    /* status flags: SERVER_STATUS_AUTOCOMMIT */
    pp[0] = 0x02; pp[1] = 0x00; pp += 2;
    /* capability flags upper 2 bytes
     * NB: do NOT advertise CLIENT_DEPRECATE_EOF (0x0100) — we forward raw
     * backend packets that contain legacy EOF markers.  Advertising it
     * makes MySQL 8+ clients expect OK-instead-of-EOF → "Malformed packet". */
    pp[0] = 0xff; pp[1] = 0x00; pp += 2;
    /* auth plugin data length */
    *pp++ = 21;
    /* reserved (10 zeros) */
    memset(pp, 0, 10); pp += 10;
    /* scramble part 2 (12 bytes + NUL) */
    memset(pp, 0x41, 12); pp += 12; *pp++ = 0;
    /* auth plugin name */
    const char* plugin = "mysql_native_password";
    size_t pl = strlen(plugin) + 1;
    memcpy(pp, plugin, pl); pp += pl;

    size_t payload_len = (size_t)(pp - payload);
    wrle24(p, (uint32_t)payload_len);
    p[3] = 0; /* seq_id = 0 */
    ctx->greeting_len = MY_HDR + payload_len;
}

/**
 * @brief Encode a minimal MySQL OK packet into a caller-supplied buffer.
 *
 * Produces a Protocol 4.1 OK layout:
 * `[hdr(4)][0x00][affected_rows=0][last_insert_id=0][status=0x0002][warnings=0]`
 *
 * @param buf Destination buffer (must be at least 11 bytes).
 * @param len [out] Number of bytes written.
 * @param seq Sequence ID to embed in the 4-byte MySQL packet header.
 */
static void build_ok_packet(uint8_t* buf, size_t* len, uint8_t seq) {
    /* OK: [hdr][0x00][0=affected_rows][0=last_insert_id][status=0x0002][warnings=0] */
    uint8_t payload[] = {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00};
    size_t pl = sizeof(payload);
    wrle24(buf, (uint32_t)pl);
    buf[3] = seq;
    memcpy(buf + MY_HDR, payload, pl);
    *len = MY_HDR + pl;
}

/* ============================================================================
 * SQL Classification — MySQL-Aware
 * ============================================================================
 *
 * Uses the shared keel_sql_analyze() for query type, then applies
 * MySQL-specific hardpin scanning for features that prevent backend reuse.
 */

/**
 * @brief Classify a MySQL SQL string and derive routing, pin, and effect metadata.
 *
 * Calls the shared @c keel_sql_analyze() for query type, then applies
 * MySQL-specific hardpin scanning for features such as @c GET_LOCK, user
 * variables, temporary tables, and prepared statements.
 *
 * @param sql            SQL text (not NUL-terminated).
 * @param sql_len        Length of @p sql in bytes.
 * @param eff            [out] Accumulated query-effect flags.
 * @param route          [out] Suggested routing target.
 * @param pin_set        [out] Pin reasons to set on the session.
 * @param pin_clr        [out] Pin reasons to clear from the session.
 * @param kind           [out] Message kind classification.
 * @param out_query_type [out] Raw numeric query type from the SQL analyser.
 */
static void classify_sql_mysql(const char* sql, size_t sql_len,
                                keel_query_effect_flags_t* eff,
                                keel_flow_route_t* route,
                                keel_flow_pin_reason_t* pin_set,
                                keel_flow_pin_reason_t* pin_clr,
                                keel_msg_kind_t* kind,
                                uint32_t* out_query_type) {
    *eff = KEEL_QE_NONE;
    *route = KEEL_FROUTE_PRIMARY;
    *pin_set = KEEL_FPIN_NONE;
    *pin_clr = KEEL_FPIN_NONE;
    *kind = KEEL_MSG_KIND_SQL;
    if (out_query_type) *out_query_type = 0;
    if (!sql || sql_len == 0) return;

    keel_str_t str = { .data = sql, .len = sql_len };
    keel_proto_query_t qr;
    memset(&qr, 0, sizeof(qr));
    keel_sql_analyze(str, &qr);
    if (out_query_type) *out_query_type = (uint32_t)qr.type;

    switch (qr.type) {
    case KEEL_QUERY_SELECT: case KEEL_QUERY_SHOW: case KEEL_QUERY_EXPLAIN:
        *eff |= KEEL_QE_READONLY;
        *route = KEEL_FROUTE_REPLICA;
        break;
    case KEEL_QUERY_INSERT: case KEEL_QUERY_UPDATE:
    case KEEL_QUERY_DELETE: case KEEL_QUERY_TRUNCATE:
        *eff |= KEEL_QE_WRITE;
        break;
    case KEEL_QUERY_CREATE: case KEEL_QUERY_ALTER: case KEEL_QUERY_DROP:
        *eff |= KEEL_QE_DDL | KEEL_QE_WRITE;
        break;
    case KEEL_QUERY_BEGIN:
        *eff |= KEEL_QE_BEGINS_TX;
        *pin_set |= KEEL_FPIN_TRANSACTION;
        *kind = KEEL_MSG_KIND_TX_CONTROL;
        break;
    case KEEL_QUERY_COMMIT: case KEEL_QUERY_ROLLBACK:
        *eff |= KEEL_QE_ENDS_TX;
        *pin_clr |= KEEL_FPIN_TRANSACTION;
        *kind = KEEL_MSG_KIND_TX_CONTROL;
        break;
    case KEEL_QUERY_SAVEPOINT:
        *kind = KEEL_MSG_KIND_TX_CONTROL;
        break;
    case KEEL_QUERY_SET:
        *eff |= KEEL_QE_SETS_STATE;
        *kind = KEEL_MSG_KIND_STATE_CHANGE;
        break;
    case KEEL_QUERY_RESET: case KEEL_QUERY_DISCARD:
        *eff |= KEEL_QE_SETS_STATE;
        *kind = KEEL_MSG_KIND_STATE_CHANGE;
        break;
    case KEEL_QUERY_PREPARE:
        *pin_set |= KEEL_FPIN_PREPARED_STMT;
        *eff |= KEEL_QE_HARD_PIN;
        break;
    case KEEL_QUERY_CALL:
        /* Stored procedures can emit multiple result sets, modify user variables,
         * and alter session state in ways the proxy cannot model.
         * Mark UNKNOWN_STATE so the engine quarantines the backend on return. */
        *eff |= KEEL_QE_WRITE | KEEL_QE_UNKNOWN_STATE;
        *pin_set |= KEEL_FPIN_QUARANTINE;
        break;
    case KEEL_QUERY_DO:
        /* DO executes an expression with side effects but returns no rows. */
        *eff |= KEEL_QE_WRITE;
        break;
    case KEEL_QUERY_COPY:
        *eff |= KEEL_QE_COPY_IN;
        *kind = KEEL_MSG_KIND_COPY;
        break;
    default:
        break;
    }

    /* XA transaction detection (MySQL/InnoDB distributed transactions).
     *
     * XA transactions span multiple connections and backends; session state
     * becomes unpredictable once an XA branch is active.  We hard-pin the
     * backend for the XA lifetime and release it only on XA COMMIT/ROLLBACK.
     *
     * XA verb forms (case-insensitive):
     *   XA START 'xid' / XA BEGIN 'xid'  → begin branch
     *   XA END 'xid'                      → end branch work
     *   XA PREPARE 'xid'                  → two-phase prepare
     *   XA COMMIT 'xid'                   → commit branch
     *   XA ROLLBACK 'xid'                 → rollback branch
     *   XA RECOVER                        → list prepared XA transactions
     */
    {
        const char* p   = sql;
        const char* end = sql + sql_len;
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
        if (end - p >= 3 &&
            (p[0] == 'X' || p[0] == 'x') &&
            (p[1] == 'A' || p[1] == 'a') &&
            (p[2] == ' ' || p[2] == '\t')) {
            const char* xasub = p + 2;
            while (xasub < end && (*xasub == ' ' || *xasub == '\t')) xasub++;
            size_t xalen = (size_t)(end - xasub);

            /* XA START / XA BEGIN → begin distributed transaction */
            bool is_start  = xalen >= 5 &&
                             (xasub[0]=='S'||xasub[0]=='s') &&
                             (xasub[1]=='T'||xasub[1]=='t') &&
                             (xasub[2]=='A'||xasub[2]=='a') &&
                             (xasub[3]=='R'||xasub[3]=='r') &&
                             (xasub[4]=='T'||xasub[4]=='t');
            bool is_begin  = xalen >= 5 &&
                             (xasub[0]=='B'||xasub[0]=='b') &&
                             (xasub[1]=='E'||xasub[1]=='e') &&
                             (xasub[2]=='G'||xasub[2]=='g') &&
                             (xasub[3]=='I'||xasub[3]=='i') &&
                             (xasub[4]=='N'||xasub[4]=='n');
            bool is_commit = xalen >= 6 &&
                             (xasub[0]=='C'||xasub[0]=='c') &&
                             (xasub[5]=='T'||xasub[5]=='t');
            bool is_rollback = xalen >= 8 &&
                               (xasub[0]=='R'||xasub[0]=='r') &&
                               (xasub[1]=='O'||xasub[1]=='o');

            if (is_start || is_begin) {
                *eff     |= KEEL_QE_BEGINS_TX | KEEL_QE_HARD_PIN;
                *pin_set |= KEEL_FPIN_TRANSACTION;
                *kind     = KEEL_MSG_KIND_TX_CONTROL;
            } else if (is_commit || is_rollback) {
                *eff     |= KEEL_QE_ENDS_TX | KEEL_QE_HARD_PIN;
                *pin_clr |= KEEL_FPIN_TRANSACTION;
                *kind     = KEEL_MSG_KIND_TX_CONTROL;
            } else {
                /* XA END, XA PREPARE, XA RECOVER — keep hard-pin */
                *eff |= KEEL_QE_HARD_PIN;
            }
        }
    }

    /* MySQL-specific hard-pin scanning:
     * GET_LOCK, @user_variables, CREATE TEMPORARY TABLE, LOCK TABLES,
     * SQL_CALC_FOUND_ROWS, PREPARE/EXECUTE/DEALLOCATE, OSC shadow tables */
    {
        keel_pin_reason_t hp = keel_hardpin_scan_mysql(sql, sql_len);
        if (hp & (KEEL_PIN_GET_LOCK | KEEL_PIN_USER_VARIABLE |
                  KEEL_PIN_TEMP_TABLE | KEEL_PIN_LOCK_TABLE |
                  KEEL_PIN_FOUND_ROWS | KEEL_PIN_PREPARED_STMT |
                  KEEL_PIN_SESSION_SET | KEEL_PIN_OSC)) {
            *eff |= KEEL_QE_POTENTIALLY_STATEFUL;
            *pin_set |= KEEL_FPIN_QUARANTINE;
        }
        /* OSC: force primary routing + exclusive backend affinity */
        if (hp & KEEL_PIN_OSC) {
            *pin_set |= KEEL_FPIN_OSC;
            *route    = KEEL_FROUTE_PRIMARY;
            *eff     |= KEEL_QE_HARD_PIN;
        }
    }

    /* Multi-statement detection */
    if (keel_sql_count_statements(str) > 1) {
        *eff |= KEEL_QE_MULTI_STMT;
        if (keel_sql_contains_transaction_start(str))
            *pin_set |= KEEL_FPIN_TRANSACTION;
    }
}

/* ============================================================================
 * VTable: Context Create / Destroy
 * ============================================================================ */

/**
 * @brief Allocate and initialise a per-connection MySQL flow context.
 *
 * Assigns a synthetic connection ID derived from the worker and slab index
 * so that `COM_PROCESS_KILL` routing works without reaching the backend.
 *
 * @param s Owning session (may be `NULL` in unit tests).
 * @return Pointer to the new flow context, or `NULL` on allocation failure.
 */
static void* myf_create(keel_session_t* s) {
    my_flow_ctx_t* ctx = keel_calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;

    /* Generate unique synthetic connection ID for KILL QUERY routing.
     * Encodes (worker_id << 16 | slab_index), same scheme as PG cancel. */
    if (s && s->worker) {
        ctx->synthetic_conn_id = ((uint32_t)s->worker->id << 16)
                               | (s->slab_index & 0xFFFF);
        s->cancel_pid = ctx->synthetic_conn_id;
        s->cancel_secret = (uint32_t)((uintptr_t)s ^ 0x5A5A5A5A) ^ (uint32_t)s->id;
    } else {
        ctx->synthetic_conn_id = 1;
    }

    return ctx;
}

/* ============================================================================
 * VTable: generate_greeting — MySQL server-speaks-first
 * ============================================================================ */

/**
 * @brief Vtable hook: copy the pre-built MySQL HandshakeV10 greeting into @p buf.
 *
 * @param vctx  Flow context cast to `void *`.
 * @param buf   Destination buffer.
 * @param blen  Capacity of @p buf in bytes.
 * @return Number of bytes written, or `-1` if the buffer is too small.
 */
static ssize_t myf_generate_greeting(void* vctx, uint8_t* buf, size_t blen) {
    my_flow_ctx_t* ctx = vctx;
    if (!ctx) return -1;
    build_greeting(ctx);
    if (ctx->greeting_len > blen) return -1;
    memcpy(buf, ctx->greeting_buf, ctx->greeting_len);
    return (ssize_t)ctx->greeting_len;
}

/** @brief Vtable hook: free the per-connection flow context allocated by myf_create().
 *  @param v Flow context pointer.
 */
static void myf_destroy(void* v) { keel_free(v); }

/* ============================================================================
 * VTable: frame_len
 * ============================================================================ */

/**
 * @brief Vtable hook: return the total byte length of the MySQL packet at @p data.
 *
 * Reads the 3-byte little-endian payload length from the MySQL packet header
 * and returns `MY_HDR + payload_len`.
 *
 * @param v    Flow context (unused).
 * @param data Buffer start.
 * @param len  Number of bytes currently buffered.
 * @param dir  Direction flag (unused for MySQL).
 * @return Full packet length, `0` if the header is incomplete, or `-1` on
 *         protocol error (payload exceeds `MY_MAX_PKT`).
 */
static ssize_t myf_frame_len(void* v, const uint8_t* data, size_t len, int dir) {
    (void)v; (void)dir;
    if (len < MY_HDR) return 0;
    uint32_t pl = rdle24(data);
    if (pl > MY_MAX_PKT) return -1;
    return (ssize_t)(MY_HDR + pl);
}

/* ===================================================================== */
static int myf_on_fe_msg(void* vctx, const uint8_t* data, size_t len,
                          keel_fe_action_t* act) {
    my_flow_ctx_t* ctx = vctx;
    *act = keel_fe_action_default();

    /* -------- Handshake phase -------- */
    if (!ctx->handshake_complete) {
        if (len < MY_HDR + 32) { act->type = KEEL_FE_ACT_ERROR; return -1; }
        ctx->seq_id = data[3];

        /* Parse caps(4) + max_pkt(4) + charset(1) + reserved(23) = 32 */
        size_t pos = MY_HDR + 32;

        /* Username (NUL-terminated) */
        if (pos < len) {
            size_t ul = strnlen((const char*)(data + pos), len - pos);
            if (ul < sizeof(ctx->username))
                memcpy(ctx->username, data + pos, ul);
            pos += ul + 1;
        }
        /* Skip auth response (length-prefixed) */
        if (pos < len) {
            uint8_t alen = data[pos];
            if (pos + 1 + alen > len) { act->type = KEEL_FE_ACT_ERROR; return -1; }
            pos += 1 + alen;
        }
        /* Database (NUL-terminated, if CONNECT_WITH_DB) */
        if (pos < len) {
            size_t dl = strnlen((const char*)(data + pos), len - pos);
            if (dl < sizeof(ctx->database))
                memcpy(ctx->database, data + pos, dl);
        }

        ctx->handshake_complete = true;
        ctx->reusable = true;
        ctx->result_state = MY_RS_IDLE;

        /* Seed the stmt-compat profile from the authenticated identity.
         * Username → role_hash, database → db_hash. semantic_unknown starts
         * false: only an untracked SET flips it. */
        ctx->stmt_role_hash       = ctx->username[0]
                                  ? my_fnv64(ctx->username, strlen(ctx->username))
                                  : 0;
        ctx->stmt_db_hash         = ctx->database[0]
                                  ? my_fnv64(ctx->database, strlen(ctx->database))
                                  : 0;
        ctx->stmt_guc_hash        = 0;
        ctx->stmt_semantic_unknown = false;
        ctx->commit_pending       = false;

        /* Build OK response (AuthOk) into per-context buffer */
        build_ok_packet(ctx->ok_buf, &ctx->ok_len, ctx->seq_id + 1);

        act->type = KEEL_FE_ACT_AUTH_COMPLETE;
        act->fe_response = ctx->ok_buf;
        act->fe_response_len = ctx->ok_len;
        act->client_username = ctx->username;
        act->client_database = ctx->database;
        return 0;
    }

    /* -------- Regular command phase -------- */
    if (len < MY_HDR + 1) { act->type = KEEL_FE_ACT_ERROR; return -1; }
    uint8_t cmd = data[MY_HDR];
    ctx->seq_id = data[3];
    ctx->last_command = cmd;

    switch (cmd) {

    /* ---- COM_QUERY: full SQL classification + hardpin scanning ---- */
    case MY_COM_QUERY: {
        const char* sql = (const char*)(data + MY_HDR + 1);
        size_t sl = len - MY_HDR - 1;

        keel_query_effect_flags_t eff;
        keel_flow_route_t route;
        keel_flow_pin_reason_t pin_set, pin_clr;
        keel_msg_kind_t kind;
        uint32_t qtype = 0;
        classify_sql_mysql(sql, sl, &eff, &route, &pin_set, &pin_clr, &kind, &qtype);

        /* Commit-in-doubt: arm the post-COMMIT capture so that the
         * matching OK packet (BE side) emits commit_xid_captured. The flag
         * is also cleared by ROLLBACK and by ERR packets so a failed
         * COMMIT does not poison subsequent transactions. */
        if (qtype == KEEL_QUERY_COMMIT)
            ctx->commit_pending = true;
        else if (qtype == KEEL_QUERY_ROLLBACK)
            ctx->commit_pending = false;

        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = kind;
        act->effect = eff;
        act->route_hint = route;
        act->pin_update = pin_set;
        act->pin_clear = pin_clr;
        act->query_type = qtype;
        act->be_payload = data;
        act->be_payload_len = len;
        act->sql_view = sql;
        act->sql_view_len = sl;

        /* Cache eligibility: only for pure reads without side effects */
        act->cache_eligible = !(eff & (KEEL_QE_WRITE | KEEL_QE_DDL |
            KEEL_QE_BEGINS_TX | KEEL_QE_ENDS_TX | KEEL_QE_HARD_PIN |
            KEEL_QE_SETS_STATE | KEEL_QE_MULTI_STMT));

        /* Splice eligibility for large payloads */
        act->splice_eligible = (len > 8192);

        /* Track state changes and extract SET key=value for SSV.
         *
         * MySQL SET forms we handle:
         *   SET @@session.var = value
         *   SET @@global.var  = value
         *   SET @@var = value
         *   SET var = value
         *   SET NAMES charset
         *   SET CHARACTER SET charset
         *
         * We extract the first variable name and value from a simple SET.
         * Multi-SET statements (SET var1=val1, var2=val2) only capture the
         * first pair; the backend SESSION_TRACK response provides the
         * authoritative state regardless. */
        if (eff & KEEL_QE_SETS_STATE) {
            ctx->state_changes++;

            /* Attempt to extract key=value from the SQL text */
            const char* p = sql;
            const char* end = sql + sl;

            /* Skip leading whitespace and "SET " keyword */
            while (p < end && (*p == ' ' || *p == '\t')) p++;
            if (p + 4 <= end &&
                (p[0] == 'S' || p[0] == 's') &&
                (p[1] == 'E' || p[1] == 'e') &&
                (p[2] == 'T' || p[2] == 't') &&
                p[3] == ' ') {
                p += 4;
                while (p < end && *p == ' ') p++;

                /* "SET NAMES ..." → key="character_set_client", value=charset */
                if (p + 5 <= end &&
                    (p[0] == 'N' || p[0] == 'n') &&
                    (p[1] == 'A' || p[1] == 'a') &&
                    (p[2] == 'M' || p[2] == 'm') &&
                    (p[3] == 'E' || p[3] == 'e') &&
                    (p[4] == 'S' || p[4] == 's') &&
                    (p + 5 >= end || p[5] == ' ')) {
                    p += 5;
                    while (p < end && *p == ' ') p++;
                    act->has_state_delta = true;
                    act->state_key = "character_set_client";
                    act->state_key_len = 20;
                    act->state_value = p;
                    /* value extends to end, trimming trailing whitespace/semicolons */
                    const char* ve = end;
                    while (ve > p && (ve[-1] == ' ' || ve[-1] == ';' ||
                                      ve[-1] == '\n' || ve[-1] == '\r'))
                        ve--;
                    act->state_value_len = (size_t)(ve - p);
                } else {
                    /* Generic "SET [@@[session.|global.]]var = value" */
                    /* Skip @@ prefix and optional scope qualifier */
                    if (p + 2 <= end && p[0] == '@' && p[1] == '@') {
                        p += 2;
                        /* Skip "session." or "global." */
                        if (p + 8 <= end && (
                            (p[0]=='s'||p[0]=='S') && (p[7]=='.') &&
                            (p[1]=='e'||p[1]=='E') && (p[2]=='s'||p[2]=='S') &&
                            (p[3]=='s'||p[3]=='S') && (p[4]=='i'||p[4]=='I') &&
                            (p[5]=='o'||p[5]=='O') && (p[6]=='n'||p[6]=='N')))
                            p += 8;
                        else if (p + 7 <= end && (
                            (p[0]=='g'||p[0]=='G') && (p[6]=='.') &&
                            (p[1]=='l'||p[1]=='L') && (p[2]=='o'||p[2]=='O') &&
                            (p[3]=='b'||p[3]=='B') && (p[4]=='a'||p[4]=='A') &&
                            (p[5]=='l'||p[5]=='L')))
                            p += 7;
                    }

                    /* p now points at the variable name */
                    const char* key_start = p;
                    while (p < end && *p != '=' && *p != ' ' && *p != '\t')
                        p++;
                    size_t key_len = (size_t)(p - key_start);

                    if (key_len > 0) {
                        /* Skip whitespace and '=' */
                        while (p < end && (*p == ' ' || *p == '\t')) p++;
                        if (p < end && *p == '=') p++;
                        while (p < end && (*p == ' ' || *p == '\t')) p++;

                        /* Value: strip quotes and trailing semicolons */
                        const char* val_start = p;
                        const char* val_end = end;
                        while (val_end > val_start &&
                               (val_end[-1] == ' ' || val_end[-1] == ';' ||
                                val_end[-1] == '\n' || val_end[-1] == '\r'))
                            val_end--;
                        /* Strip matching quotes */
                        if (val_end - val_start >= 2 &&
                            ((val_start[0] == '\'' && val_end[-1] == '\'') ||
                             (val_start[0] == '"' && val_end[-1] == '"'))) {
                            val_start++;
                            val_end--;
                        }

                        act->has_state_delta = true;
                        act->state_key = key_start;
                        act->state_key_len = key_len;
                        act->state_value = val_start;
                        act->state_value_len = (size_t)(val_end - val_start);
                    }
                }
            }

            /* Stmt-compat profile: fold tracked GUC kv into guc_hash, or
             * flip semantic_unknown for anything we cannot model. The
             * SET NAMES branch above always falls into the tracked path
             * (key="character_set_client"). User-variable SETs (@var=...)
             * and bare "SET ROLE" / "SET PASSWORD" / "SET TRANSACTION"
             * fall through to semantic_unknown so reuse is suppressed. */
            if (act->has_state_delta &&
                my_is_tracked_guc(act->state_key, act->state_key_len)) {
                uint64_t kh = my_fnv64_ci(act->state_key, act->state_key_len);
                uint64_t vh = act->state_value_len
                            ? my_fnv64(act->state_value, act->state_value_len)
                            : 0;
                /* XOR-fold so re-setting the same kv is a no-op for the hash. */
                ctx->stmt_guc_hash ^= kh ^ (vh + 0x9E3779B97F4A7C15ULL);
            } else {
                ctx->stmt_semantic_unknown = true;
            }
        }

        /* Cross-service RYW: intercept SET @keel_write_gtid and SELECT @keel_write_gtid
         * before forwarding to the backend.  Both are fully synthetic — the proxy
         * handles them without a backend round-trip.
         *
         * SET @keel_write_gtid = '<gtid>'
         *   → store the GTID in inject_consistency_lsn so the engine records it in the
         *     session's consistency atom, then send a synthetic OK to the client.
         *
         * SELECT @keel_write_gtid
         *   → return the last stored write GTID as a single-row result set. */
        if (eff & KEEL_QE_SETS_STATE) {
            char gtid_val[512];
            if (my_try_parse_set_keel_write_gtid(sql, sl, gtid_val, sizeof(gtid_val))) {
                /* Store the GTID locally for SHOW/SELECT responses */
                size_t gvlen = strlen(gtid_val);
                if (gvlen >= sizeof(ctx->keel_write_gtid))
                    gvlen = sizeof(ctx->keel_write_gtid) - 1;
                memcpy(ctx->keel_write_gtid, gtid_val, gvlen);
                ctx->keel_write_gtid[gvlen] = '\0';
                /* Build a synthetic OK response */
                build_ok_packet(ctx->ryw_resp_buf, &ctx->ryw_resp_len, ctx->seq_id + 1);
                act->type             = KEEL_FE_ACT_SEND_FE;
                act->fe_response      = ctx->ryw_resp_buf;
                act->fe_response_len  = ctx->ryw_resp_len;
                /* Signal the engine to update the session's consistency atom */
                size_t copy = gvlen < sizeof(act->inject_consistency_lsn) - 1
                            ? gvlen : sizeof(act->inject_consistency_lsn) - 1;
                memcpy(act->inject_consistency_lsn, gtid_val, copy);
                act->inject_consistency_lsn[copy] = '\0';
                return 0;
            }
        } else if (!(eff & (KEEL_QE_WRITE | KEEL_QE_DDL)) &&
                   my_is_select_keel_write_gtid(sql, sl)) {
            const char* gtid = ctx->keel_write_gtid;
            ssize_t resp_len = my_build_keel_select_gtid_response(
                gtid, ctx->ryw_resp_buf, sizeof(ctx->ryw_resp_buf),
                ctx->seq_id);
            if (resp_len > 0) {
                ctx->ryw_resp_len     = (size_t)resp_len;
                act->type             = KEEL_FE_ACT_SEND_FE;
                act->fe_response      = ctx->ryw_resp_buf;
                act->fe_response_len  = ctx->ryw_resp_len;
                return 0;
            }
        }

        return 0;
    }

    /* ---- COM_QUIT ---- */
    case MY_COM_QUIT:
        act->type = KEEL_FE_ACT_TERMINATE;
        return 0;

    /* ---- COM_PING ---- */
    case MY_COM_PING:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;

    /* ---- COM_INIT_DB: switch database ---- */
    case MY_COM_INIT_DB: {
        /* Update tracked database name */
        if (len > MY_HDR + 1) {
            size_t dl = len - MY_HDR - 1;
            if (dl >= sizeof(ctx->database)) dl = sizeof(ctx->database) - 1;
            memcpy(ctx->database, data + MY_HDR + 1, dl);
            ctx->database[dl] = '\0';
            /* Refresh the schema/db hash for the stmt-compat profile. */
            ctx->stmt_db_hash = dl ? my_fnv64(ctx->database, dl) : 0;
        }
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;
    }

    /* ---- COM_CHANGE_USER: re-auth ---- */
    case MY_COM_CHANGE_USER:
        act->type = KEEL_FE_ACT_NEED_BACKEND_AUTH;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;

    /* ---- COM_STMT_PREPARE: text goes to backend, pin for prepared ---- */
    case MY_COM_STMT_PREPARE:
        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = KEEL_MSG_KIND_SQL;
        act->pin_update = KEEL_FPIN_PREPARED_STMT;
        act->be_payload = data;
        act->be_payload_len = len;
        /* Extract SQL for fingerprinting and stash for PREPARE_OK registration */
        if (len > MY_HDR + 1) {
            act->sql_view = (const char*)(data + MY_HDR + 1);
            act->sql_view_len = len - MY_HDR - 1;
            size_t sl = act->sql_view_len;
            if (sl >= sizeof(ctx->pending_prepare_sql))
                sl = sizeof(ctx->pending_prepare_sql) - 1;
            memcpy(ctx->pending_prepare_sql, act->sql_view, sl);
            ctx->pending_prepare_sql[sl] = '\0';
            ctx->pending_prepare_sql_len = sl;
        }
        return 0;

    /* ---- COM_STMT_EXECUTE: binary protocol, pinned ---- */
    case MY_COM_STMT_EXECUTE:
        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = KEEL_MSG_KIND_OTHER;
        act->pin_update = KEEL_FPIN_PREPARED_STMT;
        act->be_payload = data;
        act->be_payload_len = len;
        act->splice_eligible = (len > 4096);
        return 0;

    /* ---- COM_STMT_CLOSE: release prepared statement (no response from server) ---- */
    case MY_COM_STMT_CLOSE:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        act->no_response = true;
        /* Decrement the active statement count and clear the pin once the last
         * statement is closed.  stmt_id is 4 bytes LE at payload offset 0. */
        if (len >= MY_HDR + 5) {
            uint32_t stmt_id = (uint32_t)data[MY_HDR + 1]
                             | ((uint32_t)data[MY_HDR + 2] << 8)
                             | ((uint32_t)data[MY_HDR + 3] << 16)
                             | ((uint32_t)data[MY_HDR + 4] << 24);
            my_stmt_remove(ctx, stmt_id);
        } else if (ctx->stmt_active_count > 0) {
            ctx->stmt_active_count--;
        }
        if (ctx->stmt_active_count == 0)
            act->pin_clear |= KEEL_FPIN_PREPARED_STMT;
        return 0;

    /* ---- COM_STMT_RESET / COM_STMT_SEND_LONG / COM_STMT_FETCH ---- */
    case MY_COM_STMT_SEND_LONG:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        act->no_response = true;  /* No response from server */
        return 0;
    case MY_COM_STMT_RESET:
    case MY_COM_STMT_FETCH:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;

    /* ---- COM_RESET_CONNECTION: reset session state ---- */
    case MY_COM_RESET_CONNECTION:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        /* After reset completes, session state is clean */
        act->pin_clear = KEEL_FPIN_TRANSACTION | KEEL_FPIN_PREPARED_STMT |
                         KEEL_FPIN_QUARANTINE;
        /* Clear the prepared statement map — all server-side stmts are gone */
        my_stmt_clear_all(ctx);
        return 0;

    /* ---- COM_SET_OPTION ---- */
    case MY_COM_SET_OPTION:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;

    /* ---- COM_PROCESS_KILL: cancel a query by connection ID ---- */
    case MY_COM_PROCESS_KILL: {
        /* MySQL COM_PROCESS_KILL sends a 4-byte LE connection_id.
         * We pass it through to the engine as CANCEL_REQUEST; the engine
         * will map synthetic→real and forward appropriately. */
        act->type = KEEL_FE_ACT_CANCEL_REQUEST;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;
    }

    /* ---- COM_FIELD_LIST (deprecated but still used) ---- */
    case MY_COM_FIELD_LIST:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;

    /* ---- Unknown / other commands: pass through ---- */
    default:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data;
        act->be_payload_len = len;
        return 0;
    }
}

/**
 * @brief Vtable hook: return whether @p hdr begins a spliceable result-set data row.
 *
 * Returns `true` for text-protocol result rows (first payload byte is not
 * `0x00` OK, `0xFF` ERR, or `0xFE` EOF) so the engine can bypass per-byte
 * inspection for large result sets.
 *
 * @param vctx     Flow context (unused).
 * @param hdr      Packet buffer (header + at least the first payload byte).
 * @param hdr_len  Number of bytes in @p hdr.
 * @return `true` if this packet is a raw data row, `false` otherwise.
 */
static bool myf_is_data_frame(void* vctx, const uint8_t* hdr, size_t hdr_len) {
    (void)vctx;
    /* MySQL packet layout: 3-byte LE length + 1-byte sequence id + payload.
     * Control packets are identified by their first payload byte (hdr[MY_HDR]):
     *   0x00 = OK packet   — end of non-result command, carries tx state flags
     *   0xFF = ERR packet  — error, must be processed by on_be_msg
     *   0xFE = EOF packet  — end-of-columns or end-of-rows marker
     * Anything else is a result-set text-protocol row (length-encoded fields).
     * 0xFE is treated conservatively (not skipped) to avoid the edge case
     * where a column value >= 254 bytes would also start with 0xFE as a
     * length-encoded integer prefix — in that context on_be_msg still
     * correctly returns KEEL_BE_ACT_FORWARD_FE with no state change. */
    if (hdr_len < MY_HDR + 1) return false;
    uint8_t fb = hdr[MY_HDR];
    return (fb != 0x00 && fb != 0xFF && fb != 0xFE);
}

/**
 * @brief Process a complete backend (server-to-proxy) MySQL message.
 *
 * Parses OK, ERR, EOF, LOCAL INFILE request, and result-set packets.
 * Updates transaction state, reusability flag, and query-complete indicator.
 * Uses proper length-encoded integer decoding for OK packets to extract
 * transaction status flags.
 *
 * @param vctx  Flow context (my_flow_ctx_t*).
 * @param data  Exactly one complete MySQL packet.
 * @param len   Packet length in bytes.
 * @param[out] act  Action for the engine to perform.
 * @return 0 on success, -1 on protocol error.
 */

/**
 * @brief Vtable hook: process one complete MySQL packet received from the backend.
 *
 * Interprets OK, ERR, EOF, LOCAL INFILE, and result-set column-count packets.
 * Tracks the result-set state machine, extracts transaction status flags from
 * OK/EOF, and propagates SESSION_TRACK variable changes when available.
 *
 * @param vctx Flow context.
 * @param data Complete MySQL packet (header + payload).
 * @param len  Total packet length in bytes.
 * @param act  [out] Engine action descriptor filled by this function.
 * @return `0` on success, `-1` on fatal protocol error.
 */
static int myf_on_be_msg(void* vctx, const uint8_t* data, size_t len,
                          keel_be_action_t* act) {
    my_flow_ctx_t* ctx = vctx;
    *act = keel_be_action_default();
    if (len < MY_HDR + 1) { act->type = KEEL_BE_ACT_ERROR; return -1; }

    act->type = KEEL_BE_ACT_FORWARD_FE;
    act->fe_payload = data;
    act->fe_payload_len = len;

    uint8_t marker = data[MY_HDR];

    /* -------- ERR Packet (0xFF) — always checked, can abort any state -------- */
    if (marker == MY_ERR) {
        act->type = KEEL_BE_ACT_ERROR;
        act->is_error_response = true;
        act->query_complete = true;
        ctx->result_state = MY_RS_IDLE;
        /* A failed COMMIT must not leave the post-commit capture armed. */
        ctx->commit_pending = false;

        /* LOAD DATA aborted: ERR during INFILE means streaming is done */
        if (ctx->in_copy) {
            ctx->in_copy = false;
            act->exits_copy_mode = true;
            act->pin_clear |= KEEL_FPIN_COPY;
        }
        return 0;
    }

    /* ========================================================================
     * Result-set in progress — only EOF and ERR (above) are special.
     * Everything else (column defs, data rows, binary rows with 0x00
     * marker) is just forwarded.  This prevents binary ResultSetRow
     * packets (starting with 0x00) from being misidentified as OK.
     * ======================================================================== */
    if (ctx->result_state != MY_RS_IDLE) {

        /* ---- EOF Packet inside result set ---- */
        if (marker == MY_EOF && (len - MY_HDR) < 9) {
            uint16_t status = 0;
            if (len >= MY_HDR + 5) {
                status = (uint16_t)data[MY_HDR + 3] |
                         ((uint16_t)data[MY_HDR + 4] << 8);
            }
            bool in_tx = (status & MY_SERVER_STATUS_IN_TRANS) != 0;
            bool more_results = (status & MY_SERVER_MORE_RESULTS_EXISTS) != 0;

            /* Update transaction state from EOF */
            act->tx_state_changed = true;
            if (in_tx) {
                act->tx_status = KEEL_TX_ACTIVE;
                ctx->in_transaction = true;
                ctx->reusable = false;
                act->pin_update = KEEL_FPIN_TRANSACTION;
            } else {
                act->tx_status = KEEL_TX_IDLE;
                ctx->in_transaction = false;
                ctx->reusable = true;
                act->backend_reusable = true;
                act->pin_clear = KEEL_FPIN_TRANSACTION;
            }

            switch (ctx->result_state) {
            case MY_RS_COLUMNS:
                /* End of column definitions → expect data rows */
                ctx->result_state = MY_RS_ROWS;
                break;
            case MY_RS_ROWS:
                /* End of data rows → query might be complete */
                if (!more_results) {
                    act->query_complete = true;
                    ctx->result_state = MY_RS_IDLE;
                } else {
                    ctx->result_state = MY_RS_IDLE;
                }
                break;
            case MY_RS_STMT_PARAM_DEFS:
                /* End of param definitions in PREPARE_OK response */
                if (ctx->stmt_columns_total > 0) {
                    ctx->result_state = MY_RS_STMT_COL_DEFS;
                    ctx->columns_remaining = ctx->stmt_columns_total;
                } else {
                    act->query_complete = true;
                    ctx->result_state = MY_RS_IDLE;
                }
                break;
            case MY_RS_STMT_COL_DEFS:
                /* End of column definitions in PREPARE_OK response */
                act->query_complete = true;
                ctx->result_state = MY_RS_IDLE;
                break;
            default:
                act->query_complete = true;
                ctx->result_state = MY_RS_IDLE;
                break;
            }
            return 0;
        }

        /* Column definition or data row — just forward, decrement counter */
        if (ctx->result_state == MY_RS_COLUMNS ||
            ctx->result_state == MY_RS_STMT_PARAM_DEFS ||
            ctx->result_state == MY_RS_STMT_COL_DEFS) {
            if (ctx->columns_remaining > 0)
                ctx->columns_remaining--;
        }
        /* MY_RS_ROWS: data row packets are just forwarded */
        return 0;
    }

    /* ========================================================================
     * IDLE state — handle OK, INFILE, EOF, result-set start
     * ======================================================================== */

    /* ---- COM_STMT_PREPARE response (PREPARE_OK, marker 0x00) ---- */
    if (marker == MY_OK && ctx->last_command == MY_COM_STMT_PREPARE) {
        /* PREPARE_OK: [00][stmt_id(4)][num_cols(2)][num_params(2)][00][warnings(2)]
         * Total minimum: MY_HDR + 1 + 4 + 2 + 2 + 1 + 2 = MY_HDR + 12 */
        if (len >= MY_HDR + 12) {
            uint32_t stmt_id    = (uint32_t)data[MY_HDR+1]
                                | ((uint32_t)data[MY_HDR+2] << 8)
                                | ((uint32_t)data[MY_HDR+3] << 16)
                                | ((uint32_t)data[MY_HDR+4] << 24);
            uint16_t num_cols   = (uint16_t)data[MY_HDR+5] | ((uint16_t)data[MY_HDR+6] << 8);
            uint16_t num_params = (uint16_t)data[MY_HDR+7] | ((uint16_t)data[MY_HDR+8] << 8);

            /* Register the prepared statement in the session map so that
             * get_stmt_replay() can rebuild it on a new backend. */
            my_stmt_add(ctx, stmt_id,
                        ctx->pending_prepare_sql, ctx->pending_prepare_sql_len);
            ctx->pending_prepare_sql_len = 0;

            if (num_params > 0) {
                ctx->result_state = MY_RS_STMT_PARAM_DEFS;
                ctx->columns_remaining = num_params;
                ctx->stmt_columns_total = num_cols;
            } else if (num_cols > 0) {
                ctx->result_state = MY_RS_STMT_COL_DEFS;
                ctx->columns_remaining = num_cols;
                ctx->stmt_columns_total = 0;
            } else {
                act->query_complete = true;
            }
        } else {
            /* Short PREPARE_OK — treat as complete; cannot register without stmt_id */
            ctx->pending_prepare_sql_len = 0;
            act->query_complete = true;
        }
        return 0;
    }

    /* ---- OK Packet (0x00) ---- */
    if (marker == MY_OK) {
        /* OK: [0x00][affected_rows_lenenc][last_insert_id_lenenc]
         *     [status_flags(2)][warnings(2)][info_lenenc][session_state_lenenc]
         *
         * The session_state_changes block is present only when the
         * SERVER_SESSION_STATE_CHANGED bit (1<<14) is set in status_flags
         * and the client negotiated CLIENT_SESSION_TRACK.  We always attempt
         * to parse it when the flag is set — the worst case is a truncated
         * packet that we simply skip. */
        size_t pos = MY_HDR + 1;  /* skip 0x00 marker */

        /* True iff this OK packet actually delivered a SESSION_TRACK_GTIDS
         * entry.  Required to keep commit-in-doubt resolution sound: a
         * stale ctx->keel_write_gtid (populated by a previous round-trip
         * or by notify_write_lsn for RYW) must NOT be promoted into a
         * commit token for the current COMMIT. */
        bool gtid_refreshed_this_packet = false;

        /* Skip affected_rows (length-encoded integer) */
        uint64_t affected = read_lenenc(data, len, &pos);
        if (affected == (uint64_t)-1) goto ok_fallback;

        /* Skip last_insert_id (length-encoded integer) */
        uint64_t last_id = read_lenenc(data, len, &pos);
        (void)last_id;
        if (last_id == (uint64_t)-1) goto ok_fallback;

        /* Read status_flags (2 bytes LE) */
        if (pos + 2 > len) goto ok_fallback;
        uint16_t status = (uint16_t)data[pos] | ((uint16_t)data[pos + 1] << 8);
        pos += 2;

        /* Skip warnings (2 bytes LE) */
        if (pos + 2 <= len) pos += 2;

        bool in_tx = (status & MY_SERVER_STATUS_IN_TRANS) != 0;
        bool more_results = (status & MY_SERVER_MORE_RESULTS_EXISTS) != 0;

        act->tx_state_changed = true;

        if (in_tx) {
            act->tx_status = KEEL_TX_ACTIVE;
            ctx->in_transaction = true;
            ctx->reusable = false;
            act->pin_update = KEEL_FPIN_TRANSACTION;
        } else {
            act->tx_status = KEEL_TX_IDLE;
            ctx->in_transaction = false;
            ctx->reusable = true;
            act->backend_reusable = true;
            act->pin_clear = KEEL_FPIN_TRANSACTION;
        }

        /* LOAD DATA LOCAL INFILE exit: OK after INFILE means data is done */
        if (ctx->in_copy) {
            ctx->in_copy = false;
            act->exits_copy_mode = true;
            act->pin_clear |= KEEL_FPIN_COPY;
        }

        /* ---- SESSION_TRACK parsing (MySQL 5.7+ / MariaDB 10.2+) ----
         *
         * When SERVER_SESSION_STATE_CHANGED is set the OK packet extends
         * with:
         *   [info_string  (lenenc)]      — human-readable status info
         *   [session_state (lenenc)]      — block of session-track entries
         *
         * Each entry inside the session_state block:
         *   [type (1 byte)]
         *   [data (lenenc string)]
         *
         * For type == SESSION_TRACK_SYSTEM_VARIABLES (0) the data is:
         *   [name  (lenenc string)]
         *   [value (lenenc string)]
         *
         * We extract the first SYSTEM_VARIABLES entry and propagate it via
         * has_profile_update so the engine can update the session's state
         * profile.  Future iterations can accumulate multiple variables. */
        if ((status & MY_SERVER_SESSION_STATE_CHANGED) && pos < len) {
            /* Skip info string (lenenc) */
            const char* info_str; size_t info_len;
            if (read_lenenc_str(data, len, &pos, &info_str, &info_len) == 0) {
                /* Read session_state_changes block (lenenc-prefixed) */
                uint64_t ss_total = read_lenenc(data, len, &pos);
                if (ss_total != (uint64_t)-1 && ss_total > 0) {
                    size_t ss_end = pos + (size_t)ss_total;
                    if (ss_end > len) ss_end = len;

                    while (pos + 1 < ss_end) {
                        uint8_t track_type = data[pos++];

                        /* Read per-entry data block (lenenc-prefixed) */
                        uint64_t entry_len = read_lenenc(data, len, &pos);
                        if (entry_len == (uint64_t)-1) break;
                        size_t entry_end = pos + (size_t)entry_len;
                        if (entry_end > ss_end) break;

                        if (track_type == MY_SESSION_TRACK_SYSTEM_VARIABLES) {
                            const char* var_name;  size_t var_name_len;
                            const char* var_value; size_t var_value_len;

                            if (read_lenenc_str(data, len, &pos,
                                                &var_name, &var_name_len) == 0 &&
                                read_lenenc_str(data, len, &pos,
                                                &var_value, &var_value_len) == 0) {
                                /* Propagate the first variable via profile update.
                                 * The engine accumulates these into the session's
                                 * state_profile via state_profile_set(). */
                                if (!act->has_profile_update) {
                                    act->has_profile_update = true;
                                    act->profile_key       = var_name;
                                    act->profile_key_len   = var_name_len;
                                    act->profile_value     = var_value;
                                    act->profile_value_len = var_value_len;
                                }
                            }
                        } else if (track_type == MY_SESSION_TRACK_SCHEMA) {
                            /* SESSION_TRACK_SCHEMA (type 1): schema name as
                             * a length-encoded string.  Update the tracked
                             * database and propagate as a profile update. */
                            const char* schema_name; size_t schema_len;
                            if (read_lenenc_str(data, len, &pos,
                                                &schema_name, &schema_len) == 0) {
                                size_t copy_len = schema_len < sizeof(ctx->database) - 1
                                                ? schema_len : sizeof(ctx->database) - 1;
                                memcpy(ctx->database, schema_name, copy_len);
                                ctx->database[copy_len] = '\0';
                                if (!act->has_profile_update) {
                                    act->has_profile_update  = true;
                                    act->profile_key         = "database";
                                    act->profile_key_len     = 8;
                                    act->profile_value       = ctx->database;
                                    act->profile_value_len   = copy_len;
                                }
                            }
                        } else if (track_type == MY_SESSION_TRACK_GTIDS) {
                            /* SESSION_TRACK_GTIDS (type 3): encoding_spec (1 byte)
                             * followed by a length-encoded GTID set string.  The
                             * server emits this after every write so we can capture
                             * the latest executed GTID without a separate query. */
                            if (pos < entry_end) pos++;  /* skip encoding spec */
                            const char* gtid_val; size_t gtid_len;
                            if (read_lenenc_str(data, len, &pos,
                                                &gtid_val, &gtid_len) == 0) {
                                size_t copy_len = gtid_len < sizeof(ctx->keel_write_gtid) - 1
                                                ? gtid_len : sizeof(ctx->keel_write_gtid) - 1;
                                memcpy(ctx->keel_write_gtid, gtid_val, copy_len);
                                ctx->keel_write_gtid[copy_len] = '\0';
                                gtid_refreshed_this_packet = true;
                            }
                        }
                        pos = entry_end;  /* advance past this entry */
                    }
                }
            }
        }

        /* Query is complete unless MORE_RESULTS_EXISTS is set */
        if (!more_results) {
            act->query_complete = true;
            ctx->result_state = MY_RS_IDLE;
        }

        /* ---- Commit-in-doubt: post-COMMIT GTID-token capture ----
         *
         * Only resolve a commit token when this very OK packet delivered a
         * fresh SESSION_TRACK_GTIDS entry.  Using any non-empty
         * `ctx->keel_write_gtid` would be unsound: the field can be carrying
         * a stale GTID from a prior round-trip or from notify_write_lsn (RYW
         * intercept), in which case a lost-COMMIT scenario would falsely
         * resolve as committed via GTID_SUBSET.  See the file-top comment
         * "Note on MySQL commit-in-doubt resolution".
         *
         * If the server did not emit SESSION_TRACK_GTIDS (typically because
         * `session_track_gtids != OWN_GTID`), we leave commit_xid_captured
         * unset; the engine then reports a later disconnect as UNKNOWN. */
        if (ctx->commit_pending && act->query_complete) {
            if (gtid_refreshed_this_packet && ctx->keel_write_gtid[0] != '\0') {
                uint64_t h = my_fnv64(ctx->keel_write_gtid,
                                      strlen(ctx->keel_write_gtid));
                /* Never report a zero token: the engine treats xid==0 as
                 * "no token captured" and short-circuits to NO_XID. */
                if (h == 0) h = 1;
                act->commit_xid_captured = true;
                act->commit_xid          = h;
            }
            ctx->commit_pending = false;
        }

        return 0;

    ok_fallback:
        /* Packet too short for proper parsing — still forward */
        act->query_complete = true;
        return 0;
    }

    /* ---- LOCAL INFILE Request (0xFB) ---- */
    if (marker == MY_LOCAL_INFILE) {
        ctx->in_copy = true;
        act->enters_copy_mode = true;
        act->pin_update = KEEL_FPIN_COPY;
        return 0;
    }

    /* ---- EOF Packet (0xFE < 9 bytes payload) in IDLE state ---- */
    if (marker == MY_EOF && (len - MY_HDR) < 9) {
        uint16_t status = 0;
        if (len >= MY_HDR + 5) {
            status = (uint16_t)data[MY_HDR + 3] |
                     ((uint16_t)data[MY_HDR + 4] << 8);
        }
        bool in_tx = (status & MY_SERVER_STATUS_IN_TRANS) != 0;

        act->tx_state_changed = true;
        if (in_tx) {
            act->tx_status = KEEL_TX_ACTIVE;
            ctx->in_transaction = true;
            ctx->reusable = false;
            act->pin_update = KEEL_FPIN_TRANSACTION;
        } else {
            act->tx_status = KEEL_TX_IDLE;
            ctx->in_transaction = false;
            ctx->reusable = true;
            act->backend_reusable = true;
            act->pin_clear = KEEL_FPIN_TRANSACTION;
        }

        act->query_complete = true;
        ctx->result_state = MY_RS_IDLE;
        return 0;
    }

    /* ---- Column Count — start of result set ---- */
    if (ctx->last_command != MY_COM_FIELD_LIST) {
        /* First non-OK/ERR/EOF/INFILE packet after a query = column_count.
         * COM_FIELD_LIST responses have NO column_count header — just
         * column definition packets terminated by EOF.  We leave those in
         * MY_RS_IDLE so the EOF above sets query_complete. */
        size_t pos = MY_HDR;
        uint64_t col_count = read_lenenc(data, len, &pos);
        if (col_count != (uint64_t)-1 && col_count > 0) {
            ctx->result_state = MY_RS_COLUMNS;
            ctx->columns_remaining = (uint32_t)col_count;
        }
    }

    return 0;
}

/* ===================================================================== */
static uint64_t myf_fingerprint(void* v, const char* s, size_t n) {
    (void)v;
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c >= 'A' && c <= 'Z') c += 32;
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
        h ^= (uint64_t)(uint8_t)c;
        h *= 1099511628211ULL;
    }
    return h;
}

/* ===================================================================== */
static ssize_t myf_build_state_sync(void* v,
    const struct state_profile* bp, const struct state_profile* sp,
    uint8_t* buf, size_t blen) {
    (void)v;

    state_sync_result_t result;
    if (generate_sync_sql(bp, sp, &result) != 0)
        return -1;
    if (!result.needs_sync || result.sql_len == 0)
        return 0;

    /*
     * MySQL COM_QUERY packet layout:
     *   [3-byte LE payload_len][1-byte seq_id=0][0x03 COM_QUERY][sql bytes]
     * payload_len = 1 (cmd byte) + sql_len
     */
    size_t pl    = 1 + result.sql_len;
    size_t total = MY_HDR + pl;
    if (blen < total)
        return -1;

    wrle24(buf, (uint32_t)pl);
    buf[3]       = 0;              /* seq_id */
    buf[MY_HDR]  = MY_COM_QUERY;
    memcpy(buf + MY_HDR + 1, result.sql, result.sql_len);

    return (ssize_t)total;
}

/**
 * @brief Build a cleanup COM_QUERY or COM_RESET_CONNECTION packet.
 *
 * Selects the cleanup command based on the reason:
 *  - `KEEL_CLEANUP_TX_NOT_IDLE` / `KEEL_CLEANUP_FAILED_TX`: COM_QUERY "ROLLBACK"
 *  - All other reasons: COM_RESET_CONNECTION (0x1F)
 *
 * @param v    Flow context (unused).
 * @param r    Cleanup reason enum.
 * @param buf  Output buffer.
 * @param blen Capacity of @p buf.
 * @return Bytes written, or -1 on overflow.
 */

/**
 * @brief Vtable hook: build a cleanup command packet for a connection being
 *        returned to the pool.
 *
 * Emits `COM_QUERY "ROLLBACK"` for `KEEL_CLEANUP_TX_NOT_IDLE` /
 * `KEEL_CLEANUP_FAILED_TX` and `COM_RESET_CONNECTION` for all other reasons.
 *
 * @param v    Flow context (unused).
 * @param r    Reason for the cleanup.
 * @param buf  Destination buffer.
 * @param blen Capacity of @p buf in bytes.
 * @return Number of bytes written, or `-1` if the buffer is too small.
 */
static ssize_t myf_build_cleanup(void* v, keel_cleanup_reason_t r,
                                  uint8_t* buf, size_t blen) {
    (void)v;

    if (r == KEEL_CLEANUP_TX_NOT_IDLE || r == KEEL_CLEANUP_FAILED_TX) {
        /* Need to ROLLBACK first, then COM_RESET_CONNECTION.
         * Send COM_QUERY "ROLLBACK" for simplicity. */
        const char* sql = "ROLLBACK";
        size_t sl = strlen(sql);
        size_t pl = 1 + sl;         /* cmd_byte + sql text */
        size_t total = MY_HDR + pl;
        if (blen < total) return -1;
        wrle24(buf, (uint32_t)pl);
        buf[3] = 0;                  /* seq_id */
        buf[MY_HDR] = MY_COM_QUERY;
        memcpy(buf + MY_HDR + 1, sql, sl);
        return (ssize_t)total;
    }

    /* General cleanup: COM_RESET_CONNECTION (0x1f) */
    if (blen < 5) return -1;
    wrle24(buf, 1);          /* payload_len = 1 */
    buf[3] = 0;              /* seq_id */
    buf[4] = MY_COM_RESET_CONNECTION;
    return 5;
}

/* ===================================================================== */
static bool myf_reuse_gate(void* v) {
    my_flow_ctx_t* c = v;
    return c->reusable && !c->in_transaction && !c->in_copy;
}

/* ===================================================================== */
static ssize_t myf_gen_startup(void* v, const char* user,
                                const char* db, uint8_t* buf, size_t blen) {
    (void)v;
    uint8_t payload[512];
    size_t pos = 0;

    /* We do NOT advertise CLIENT_MULTI_STATEMENTS or CLIENT_MULTI_RESULTS:
     * the proxy itself does not issue multi-statement payloads (the unsound
     * Phase B pre-COMMIT probe that required them has been removed), and
     * leaving them off preserves the upstream MySQL default of rejecting
     * client-supplied multi-statement bundles — which has SQL-injection
     * surface implications callers should opt into explicitly, not inherit
     * from a proxy. */
    uint32_t caps = MY_CLIENT_PROTOCOL_41 | MY_CLIENT_PLUGIN_AUTH | 0xf7ff;
    caps &= ~(MY_CLIENT_MULTI_STATEMENTS | MY_CLIENT_MULTI_RESULTS);
    if (db && db[0]) caps |= MY_CLIENT_CONNECT_WITH_DB;

    /* Capability flags (4 bytes LE) */
    payload[pos++] = (uint8_t)caps;
    payload[pos++] = (uint8_t)(caps >> 8);
    payload[pos++] = (uint8_t)(caps >> 16);
    payload[pos++] = (uint8_t)(caps >> 24);
    /* Max packet = 16MB */
    payload[pos++] = 0xFF; payload[pos++] = 0xFF;
    payload[pos++] = 0xFF; payload[pos++] = 0x00;
    /* Charset: utf8mb4 */
    payload[pos++] = 0x2d;
    /* Reserved (23 zeros) */
    memset(payload + pos, 0, 23); pos += 23;
    /* Username (NUL-terminated) */
    size_t ul = strlen(user);
    memcpy(payload + pos, user, ul); pos += ul;
    payload[pos++] = 0;
    /* Auth response length = 0 (no scramble for now) */
    payload[pos++] = 0;
    /* Database (NUL-terminated, if set) */
    if (caps & MY_CLIENT_CONNECT_WITH_DB) {
        size_t dl = strlen(db);
        memcpy(payload + pos, db, dl); pos += dl;
        payload[pos++] = 0;
    }
    /* Auth plugin name */
    const char* plugin = "mysql_native_password";
    size_t pl = strlen(plugin) + 1;
    memcpy(payload + pos, plugin, pl); pos += pl;

    size_t total = MY_HDR + pos;
    if (total > blen) return -1;
    wrle24(buf, (uint32_t)pos);
    buf[3] = 1; /* seq_id = 1 (response to greeting seq=0) */
    memcpy(buf + MY_HDR, payload, pos);
    return (ssize_t)total;
}

/* ===================================================================== */
static ssize_t myf_gen_error(void* v, const char* code, const char* msg,
                              uint8_t* buf, size_t blen) {
    my_flow_ctx_t* ctx = v;
    size_t ml = strlen(msg);
    size_t pl = 1 + 2 + 1 + 5 + ml;  /* 0xFF + errno(2) + '#' + SQLSTATE(5) + msg */
    size_t total = MY_HDR + pl;
    if (total > blen) return -1;

    wrle24(buf, (uint32_t)pl);
    buf[3] = ctx->seq_id + 1;

    size_t p = MY_HDR;
    buf[p++] = MY_ERR;
    buf[p++] = 0x51; buf[p++] = 0x04;  /* errno 1105 (ER_UNKNOWN_ERROR) */
    buf[p++] = '#';
    if (code && strlen(code) >= 5)
        memcpy(buf + p, code, 5);
    else
        memcpy(buf + p, "HY000", 5);
    p += 5;
    memcpy(buf + p, msg, ml);
    return (ssize_t)total;
}

/* ============================================================================
 * Plugin API Extensions (Phase 5)
 * ============================================================================ */

/**
 * @brief Vtable hook: populate the plugin information descriptor for MySQL.
 *
 * Reports the plugin name, default port, API version, and the full set of
 * capabilities supported by the MySQL flow plugin.
 *
 * @param out [out] Plugin information structure to fill.
 */
static void myf_get_info(keel_plugin_info_t* out) {
    out->name         = "mysql";
    out->default_port = 3306;
    out->api_version  = KEEL_PLUGIN_API_V1;
    out->capabilities =
        KEEL_PCAP_TEXT_PROTOCOL     |
        KEEL_PCAP_BINARY_PROTOCOL   |
        KEEL_PCAP_EXTENDED_QUERY    |
        KEEL_PCAP_PREPARED_DETECT   |
        KEEL_PCAP_CONSISTENCY_TOKEN |
        KEEL_PCAP_POSITION_WAIT     |
        KEEL_PCAP_GTID              |
        KEEL_PCAP_STREAMING_LOAD    |
        KEEL_PCAP_AUTH_NATIVE       |
        KEEL_PCAP_AUTH_CACHING_SHA2 |
        KEEL_PCAP_DISCARD_ALL       |
        KEEL_PCAP_CANCEL_REQUEST    |
        KEEL_PCAP_SSL               |
        KEEL_PCAP_PROBE_HEALTH;
}

/**
 * @brief Vtable hook: parse a MySQL ERR packet and classify the error.
 *
 * Decodes the Protocol 4.1 ERR layout:
 * `[hdr(4)][0xFF][errno_le16][#][sqlstate(5)][message...]`
 * and maps known MySQL error numbers to KEEL error classes.
 *
 * @param vctx Flow context (unused).
 * @param data Complete ERR packet.
 * @param len  Packet length in bytes.
 * @param out  [out] Filled error information descriptor.
 * @return `0` on success, `-1` if @p data is not a valid ERR packet.
 */
static int myf_classify_error(void* vctx, const uint8_t* data, size_t len,
                               keel_error_info_t* out) {
    (void)vctx;
    memset(out, 0, sizeof(*out));
    out->error_class  = KEEL_ERR_SQL_ERROR;
    out->connection_ok = true;

    /* Minimum ERR: hdr(4) + 0xFF(1) + errno(2) = 7 bytes */
    if (!data || len < MY_HDR + 3) return -1;
    if (data[MY_HDR] != MY_ERR) return -1;

    uint16_t errn = (uint16_t)data[MY_HDR + 1] |
                    ((uint16_t)data[MY_HDR + 2] << 8);
    out->error_code = errn;

    /* Protocol 4.1+ has '#' + 5-char SQLSTATE */
    size_t pos = MY_HDR + 3;
    if (pos < len && data[pos] == '#') {
        pos++; /* skip '#' */
        if (pos + 5 <= len) {
            static _Thread_local char state_buf[6];
            memcpy(state_buf, data + pos, 5);
            state_buf[5] = '\0';
            out->sqlstate = state_buf;
            pos += 5;
        }
    }
    if (pos < len)
        out->message = (const char*)(data + pos);

    /* Classify by errno */
    switch (errn) {
    case 1040:   /* Too many connections */
    case 1203:   /* Max user connections exceeded */
        out->error_class = KEEL_ERR_RESOURCE_LIMIT;
        break;
    case 1045:   /* Access denied */
        out->error_class = KEEL_ERR_AUTH_FAILURE;
        break;
    case 1053:   /* Server shutdown in progress */
    case 1077:   /* Normal shutdown */
    case 1079:   /* Shutdown complete */
    case 1080:   /* Forced close */
        out->error_class = KEEL_ERR_BACKEND_FATAL;
        out->connection_ok = false;
        break;
    case 1205:   /* Lock wait timeout */
    case 1213:   /* Deadlock */
        out->error_class = KEEL_ERR_TRANSIENT;
        break;
    case 1047:   /* Unknown COM */
    case 1156:   /* Got packets out of order */
    case 1157:   /* Couldn't uncompress packet */
        out->error_class = KEEL_ERR_PROTO_VIOLATION;
        out->connection_ok = false;
        break;
    default:
        break;
    }
    return 0;
}

/**
 * @brief Vtable hook: build a mode-specific cleanup packet for a returning
 *        connection slot.
 *
 * - `KEEL_CLEANUP_SELECTIVE`: generates `SET @@session.x = DEFAULT` for each
 *   variable tracked in @p profile.
 * - `KEEL_CLEANUP_FULL`: generates `COM_RESET_CONNECTION`.
 * - `KEEL_CLEANUP_DESTROY`: returns `0` (caller will close the socket).
 *
 * @param vctx    Flow context.
 * @param be_fd   Backend file descriptor (unused).
 * @param profile Session state profile describing variables to reset.
 * @param opts    Cleanup options selecting the mode.
 * @param buf     Destination buffer for the encoded packet.
 * @param buf_len Capacity of @p buf in bytes.
 * @return Bytes written, `0` if nothing needs to be sent, or `-1` on error.
 */
static ssize_t myf_cleanup_slot(void* vctx, int be_fd,
                                 const struct state_profile* profile,
                                 keel_cleanup_opts_t opts,
                                 uint8_t* buf, size_t buf_len) {
    my_flow_ctx_t* ctx = vctx;
    (void)be_fd;

    switch (opts.mode) {
    case KEEL_CLEANUP_SELECTIVE:
        /* If no state was changed, nothing to clean */
        if (!profile || profile->count == 0)
            return 0;
        /* For MySQL: build SET @@session.x = DEFAULT for each tracked var.
         * Wrap in COM_QUERY. */
        {
            char sql_buf[2048];
            size_t pos = 0;
            for (size_t i = 0; i < profile->count && pos < sizeof(sql_buf) - 64; i++) {
                int written = snprintf(sql_buf + pos, sizeof(sql_buf) - pos,
                    "SET @@session.%s = DEFAULT; ",
                    profile->sorted_params[i].key);
                if (written > 0) pos += (size_t)written;
            }
            if (pos == 0) return 0;
            /* Wrap in COM_QUERY packet */
            size_t total = MY_HDR + 1 + pos;
            if (buf_len < total) return -1;
            wrle24(buf, (uint32_t)(1 + pos));
            buf[3] = 0;
            buf[MY_HDR] = MY_COM_QUERY;
            memcpy(buf + MY_HDR + 1, sql_buf, pos);
            if (ctx) ctx->cleanup_count++;
            return (ssize_t)total;
        }
    case KEEL_CLEANUP_FULL:
        /* COM_RESET_CONNECTION resets all session state */
        if (buf_len < 5) return -1;
        wrle24(buf, 1);
        buf[3] = 0;
        buf[4] = MY_COM_RESET_CONNECTION;
        if (ctx) ctx->cleanup_count++;
        return 5;
    case KEEL_CLEANUP_DESTROY:
    default:
        return 0;  /* Caller will close the connection */
    }
}

typedef struct my_cleanup_drain_state {
    uint8_t  hdr[MY_HDR];
    uint8_t  hdr_len;
    uint32_t payload_len;
    uint32_t payload_seen;
    uint8_t  marker;
    bool     marker_seen;
} my_cleanup_drain_state_t;

static void my_cleanup_reset_packet(my_cleanup_drain_state_t* st)
{
    memset(st->hdr, 0, sizeof(st->hdr));
    st->hdr_len = 0;
    st->payload_len = 0;
    st->payload_seen = 0;
    st->marker = 0;
    st->marker_seen = false;
}

static keel_proto_drain_result_t myf_drain_cleanup_response(
    void* vctx,
    keel_proto_drain_state_t* state,
    const uint8_t* data,
    size_t len,
    size_t* consumed_out)
{
    (void)vctx;
    if (consumed_out)
        *consumed_out = 0;
    if (!state || (!data && len > 0))
        return KEEL_PROTO_DRAIN_ERROR;

    my_cleanup_drain_state_t* st = (my_cleanup_drain_state_t*)state->opaque;
    size_t pos = 0;

    while (pos < len) {
        if (st->hdr_len < MY_HDR) {
            size_t need = MY_HDR - st->hdr_len;
            size_t take = (len - pos < need) ? (len - pos) : need;
            memcpy(st->hdr + st->hdr_len, data + pos, take);
            st->hdr_len += (uint8_t)take;
            pos += take;

            if (st->hdr_len < MY_HDR) {
                if (consumed_out)
                    *consumed_out = pos;
                return KEEL_PROTO_DRAIN_MORE;
            }

            st->payload_len = rdle24(st->hdr);
            st->payload_seen = 0;
            st->marker_seen = false;
            if (st->payload_len == 0 || st->payload_len > MY_MAX_PKT) {
                if (consumed_out)
                    *consumed_out = pos;
                return KEEL_PROTO_DRAIN_ERROR;
            }
        }

        uint32_t remaining = st->payload_len - st->payload_seen;
        size_t take = (len - pos < remaining) ? (len - pos) : remaining;
        if (!st->marker_seen && take > 0) {
            st->marker = data[pos];
            st->marker_seen = true;
        }

        pos += take;
        st->payload_seen += (uint32_t)take;

        if (st->payload_seen < st->payload_len) {
            if (consumed_out)
                *consumed_out = pos;
            return KEEL_PROTO_DRAIN_MORE;
        }

        bool ok = st->marker_seen && st->marker == MY_OK;
        my_cleanup_reset_packet(st);
        if (consumed_out)
            *consumed_out = pos;
        if (!ok)
            return KEEL_PROTO_DRAIN_ERROR;
        return KEEL_PROTO_DRAIN_COMPLETE;
    }

    if (consumed_out)
        *consumed_out = pos;
    return KEEL_PROTO_DRAIN_MORE;
}

/**
 * @brief Vtable hook: verify backend liveness by exchanging a `COM_PING` packet.
 *
 * Sends a 5-byte `COM_PING` and reads the response.  Sets `out->alive` to
 * `true` on a valid OK reply.
 *
 * @param vctx  Flow context (unused).
 * @param be_fd Connected backend file descriptor.
 * @param out   [out] Probe result descriptor.
 * @return `0` on success, `-1` on I/O error.
 */
static int myf_probe_backend(void* vctx, int be_fd,
                              keel_probe_result_t* out) {
    (void)vctx;
    memset(out, 0, sizeof(*out));
    out->replication_lag_ms = -1;

    /* Build COM_PING packet */
    uint8_t ping[5];
    wrle24(ping, 1);       /* payload_len = 1 */
    ping[3] = 0;           /* seq_id */
    ping[4] = MY_COM_PING;

    ssize_t w = send(be_fd, ping, 5, MSG_NOSIGNAL);
    if (w != 5) return -1;

    /* Read response: expect OK packet */
    uint8_t rbuf[64];
    ssize_t r = recv(be_fd, rbuf, sizeof(rbuf), 0);
    if (r < (ssize_t)(MY_HDR + 1)) return -1;

    if (rbuf[MY_HDR] == MY_OK) {
        out->alive = true;
        return 0;
    }

    return -1;
}

/**
 * @brief Vtable hook: populate the per-connection plugin metrics snapshot.
 *
 * @param vctx Flow context.
 * @param out  [out] Metrics structure to fill.
 * @return Always `0`.
 */
static int myf_get_metrics(void* vctx, keel_plugin_metrics_t* out) {
    my_flow_ctx_t* ctx = vctx;
    memset(out, 0, sizeof(*out));
    if (ctx) {
        out->state_changes = ctx->state_changes;
        out->cleanup_count = ctx->cleanup_count;
    }
    return 0;
}

/**
 * @brief Issue a `COM_QUERY` to the backend and return the first column of
 *        the first result row as a NUL-terminated string.
 *
 * The proxy does not negotiate `CLIENT_DEPRECATE_EOF`, so the expected
 * response sequence is:
 *   `[col_count][col_def_1][EOF][row_data][EOF]`
 *
 * Length-encoded string values up to the two-byte prefix encoding are
 * handled; larger values or unexpected layouts return an error.
 *
 * Blocking send()/recv() are intentional and mirror the equivalent PG
 * helpers (pgf_query_single_value / pgf_capture_consistency_token):
 * these vtable hooks are invoked off the data hot path (backend probe,
 * metadata fetch on acquire, RYW gate when a session has tracked a
 * write), where a brief synchronous round-trip is acceptable. Callers
 * that need a hard upper bound on wall-clock time apply SO_RCVTIMEO /
 * SO_SNDTIMEO around the call (see myf_replica_reached_token below).
 *
 * @param be_fd      Connected backend file descriptor.
 * @param sql        NUL-terminated SQL statement.
 * @param value_buf  Buffer to receive the result (NUL-terminated on success).
 * @param value_max  Size of @p value_buf including the NUL terminator.
 * @return `0` on success, `-1` on protocol error, ERR response, or buffer too small.
 */
static int myf_query_single_value(int be_fd, const char* sql,
                                   char* value_buf, size_t value_max)
{
    size_t  sql_len     = strlen(sql);
    size_t  payload_len = 1 + sql_len;          /* COM_QUERY (1) + sql */
    size_t  pkt_total   = MY_HDR + payload_len;

    /* Stack-allocate query buffer (sql <= 511 bytes for our callers) */
    uint8_t qbuf[MY_HDR + 1 + 512];
    if (pkt_total > sizeof(qbuf)) return -1;
    wrle24(qbuf, (uint32_t)payload_len);
    qbuf[3] = 0;                    /* seq_id = 0 for new command */
    qbuf[4] = MY_COM_QUERY;
    memcpy(qbuf + MY_HDR + 1, sql, sql_len);

    if (send(be_fd, qbuf, pkt_total, MSG_NOSIGNAL) != (ssize_t)pkt_total)
        return -1;

    /* Read response — 8KB covers even large GTID sets */
    uint8_t  rbuf[8192];
    ssize_t  total = 0;
    /* Non-blocking peek loop: keep reading until we have ≥2 EOF/ERR or enough data */
    for (int pass = 0; pass < 4; pass++) {
        ssize_t r = recv(be_fd, rbuf + total, sizeof(rbuf) - (size_t)total, 0);
        if (r <= 0) break;
        total += r;
        /* Stop if we see what looks like the final EOF (seq=5 for 1-col query) */
        if (total > MY_HDR * 5) break;
    }
    if (total <= (ssize_t)MY_HDR) return -1;

    /* Scan packets:
     *   pkt 1 (seq=1): column count  — skip
     *   pkt 2 (seq=2): column def    — skip
     *   pkt 3 (seq=3): EOF1          — skip
     *   pkt 4 (seq=4): row data      — extract
     *   pkt 5 (seq=5): EOF2          — done
     */
    ssize_t  pos       = 0;
    int      pkt_index = 0;

    while (pos + (ssize_t)MY_HDR <= total) {
        uint32_t       pkt_len = rdle24(rbuf + pos);
        const uint8_t* payload = rbuf + pos + MY_HDR;

        /* Guard: ensure we have the full packet */
        if (pos + (ssize_t)MY_HDR + (ssize_t)pkt_len > total) break;

        uint8_t first = (pkt_len > 0) ? payload[0] : 0xFE;
        pkt_index++;

        if (first == MY_ERR) return -1;         /* server error */

        if (first == MY_EOF && pkt_len <= 9) {
            /* EOF marker — not the row data */
            pos += MY_HDR + pkt_len;
            continue;
        }

        if (pkt_index <= 2) {
            /* pkt 1 = column count, pkt 2 = column definition — skip */
            pos += MY_HDR + pkt_len;
            continue;
        }

        /* pkt 3+ after skipping col count + col def: this is a row packet */
        if (first == MY_LOCAL_INFILE) {
            /* NULL value */
            value_buf[0] = '\0';
            return 0;
        }

        /* LengthEncodedString: first byte = length (for values < 251 bytes) */
        if (first < 0xFB) {
            size_t str_len  = (size_t)first;
            size_t copy_len = str_len < value_max - 1 ? str_len : value_max - 1;
            if ((ssize_t)(MY_HDR + 1 + str_len) > (ssize_t)(MY_HDR + pkt_len)) return -1;
            memcpy(value_buf, payload + 1, copy_len);
            value_buf[copy_len] = '\0';
            return 0;
        }
        /* str_len >= 251 — multi-byte length encoding */
        if (first == 0xFC && pkt_len >= 3) {
            /* 2-byte LE length follows */
            size_t str_len  = (size_t)payload[1] | ((size_t)payload[2] << 8);
            /* Reject truncated packets: the str_len bytes that follow the
             * 3-byte prefix must actually be inside the packet body. Without
             * this check a malicious or corrupt server could declare a
             * length larger than the packet and we would memcpy past it. */
            if ((size_t)pkt_len < 3 + str_len) return -1;
            size_t copy_len = (str_len < value_max - 1) ? str_len : value_max - 1;
            memcpy(value_buf, payload + 3, copy_len);
            value_buf[copy_len] = '\0';
            return 0;
        }
        /* Unsupported encoding or unexpected packet */
        return -1;
    }
    return -1;
}

/**
 * @brief Vtable hook: query backend metadata via `SELECT` statements.
 *
 * Issues `SELECT @@read_only`, `SELECT @@version`, and
 * `SELECT connection_id()` and populates the corresponding fields in @p out.
 *
 * @param vctx  Flow context (provides cached username and database).
 * @param be_fd Connected backend file descriptor.
 * @param out   [out] Backend metadata descriptor.
 * @return `0` on success, `-1` if any query fails.
 */
static int myf_get_backend_metadata(void* vctx, int be_fd,
                                      keel_backend_meta_t* out)
{
    my_flow_ctx_t* ctx = (my_flow_ctx_t*)vctx;
    memset(out, 0, sizeof(*out));

    /* Populate fields already known from session context */
    if (ctx && ctx->database[0])
        snprintf(out->database, sizeof(out->database), "%s", ctx->database);
    if (ctx && ctx->username[0])
        snprintf(out->user, sizeof(out->user), "%s", ctx->username);

    /* @@read_only */
    char read_only_str[8] = {0};
    if (myf_query_single_value(be_fd, "SELECT @@read_only",
                                read_only_str, sizeof(read_only_str)) != 0)
        return -1;
    out->read_only   = (read_only_str[0] == '1');
    out->in_recovery = out->read_only;

    /* @@version */
    if (myf_query_single_value(be_fd, "SELECT @@version",
                                out->server_version,
                                sizeof(out->server_version)) != 0)
        return -1;

    /* connection_id() → backend_pid analogue */
    char conn_id_str[16] = {0};
    if (myf_query_single_value(be_fd, "SELECT connection_id()",
                                conn_id_str, sizeof(conn_id_str)) == 0) {
        out->backend_pid = (uint32_t)strtoul(conn_id_str, NULL, 10);
    }

    return 0;
}

/**
 * @brief Vtable hook: capture the current GTID execution position from the backend.
 *
 * Issues `SELECT @@gtid_executed` and records the result along with a
 * monotonic capture timestamp in @p out.
 *
 * @param vctx  Flow context (unused).
 * @param be_fd Connected backend file descriptor.
 * @param out   [out] Consistency token to populate.
 * @return `0` on success, `-1` if the query fails.
 */
static int myf_capture_consistency_token(void* vctx, int be_fd,
                                          keel_consistency_token_t* out)
{
    (void)vctx;
    memset(out, 0, sizeof(*out));

    if (myf_query_single_value(be_fd,
                                "SELECT @@gtid_executed",
                                out->value,
                                KEEL_CONSISTENCY_TOKEN_MAX) != 0)
        return -1;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    out->captured_at_ns = (uint64_t)ts.tv_sec * 1000000000ULL
                         + (uint64_t)ts.tv_nsec;
    return 0;
}

/**
 * @brief Vtable hook: check whether a replica has applied a GTID token.
 *
 * Calls `WAIT_FOR_EXECUTED_GTID_SET(token, timeout_sec)` on the replica.
 * An empty token is treated as trivially satisfied.
 *
 * @param vctx        Flow context (unused).
 * @param replica_fd  Connected replica file descriptor.
 * @param token       Consistency token containing the GTID set to wait for.
 * @param timeout_ms  Maximum wait in milliseconds; `0` means an immediate check.
 * @param out_reached [out] Set to `true` if the replica has caught up.
 * @return `0` on success, `-1` on I/O or protocol error.
 */
static int myf_replica_reached_token(void* vctx, int replica_fd,
                                      const keel_consistency_token_t* token,
                                      int timeout_ms,
                                      bool* out_reached)
{
    (void)vctx;
    *out_reached = false;

    /* Empty token means no write tracked — replica trivially OK */
    if (!token || token->value[0] == '\0') {
        *out_reached = true;
        return 0;
    }

    /* Reject anything that is not a plain GTID set before it ever reaches
     * the SQL string. A GTID set is
     *   <uuid>:<n>[-<m>][:<n>[-<m>]]*[,<uuid>:...]*
     * which is exclusively [0-9a-fA-F:,\-] plus whitespace. Anything else
     * (notably a single quote) would let a compromised primary inject SQL
     * into the replica session via WAIT_FOR_EXECUTED_GTID_SET(). */
    for (const char* p = token->value; *p; ++p) {
        unsigned char c = (unsigned char)*p;
        bool ok = (c >= '0' && c <= '9') ||
                  (c >= 'a' && c <= 'f') ||
                  (c >= 'A' && c <= 'F') ||
                  c == ':' || c == ',' || c == '-' ||
                  c == ' ' || c == '\t' || c == '\n' || c == '\r';
        if (!ok) return -1;
    }
    /* Defence in depth: cap the length to what the SQL buffer can hold so
     * the snprintf below cannot silently truncate inside a quoted literal. */
    size_t token_len = strlen(token->value);
    if (token_len == 0 || token_len > 500) return -1;

    /* Bound the synchronous wait so a slow/hung replica cannot stall the
     * worker thread indefinitely. Save and restore the previous socket
     * timeouts so we do not leak settings back to the pool. */
    struct timeval saved_rcv = {0, 0}, saved_snd = {0, 0};
    bool timeout_set = false;
    if (timeout_ms > 0) {
        socklen_t optlen = sizeof(saved_rcv);
        getsockopt(replica_fd, SOL_SOCKET, SO_RCVTIMEO, &saved_rcv, &optlen);
        optlen = sizeof(saved_snd);
        getsockopt(replica_fd, SOL_SOCKET, SO_SNDTIMEO, &saved_snd, &optlen);

        struct timeval tv = {
            .tv_sec  = (time_t)(timeout_ms / 1000),
            .tv_usec = (suseconds_t)((timeout_ms % 1000) * 1000),
        };
        setsockopt(replica_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(replica_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        timeout_set = true;
    }

    /* timeout_ms=0 → immediate snapshot check (0.001 s for MySQL) */
    double timeout_sec = (timeout_ms > 0) ? (double)timeout_ms / 1000.0 : 0.001;

    char sql[576];
    snprintf(sql, sizeof(sql),
             "SELECT WAIT_FOR_EXECUTED_GTID_SET('%s', %.3f)",
             token->value, timeout_sec);

    char result[8];
    int rc = myf_query_single_value(replica_fd, sql, result, sizeof(result));

    if (timeout_set) {
        setsockopt(replica_fd, SOL_SOCKET, SO_RCVTIMEO, &saved_rcv, sizeof(saved_rcv));
        setsockopt(replica_fd, SOL_SOCKET, SO_SNDTIMEO, &saved_snd, sizeof(saved_snd));
    }

    if (rc != 0) return -1;

    /* WAIT_FOR_EXECUTED_GTID_SET returns 0 = reached, 1 = timeout */
    *out_reached = (result[0] == '0');
    return 0;
}

/* ============================================================================
 * VTable: notify_write_lsn
 * ============================================================================ */

/**
 * @brief Vtable hook: store the latest captured write GTID for RYW intercepts.
 *
 * Called by the engine after a successful capture_consistency_token() so that
 * `SELECT @keel_write_gtid` can be served without a vtable round-trip.
 *
 * @param vctx  Protocol flow context (session-scoped).
 * @param lsn   NUL-terminated GTID string (e.g. "aaaaaaaa-...:1-42").
 */
static void myf_notify_write_lsn(void* vctx, const char* lsn) {
    my_flow_ctx_t* ctx = vctx;
    if (!ctx || !lsn) return;
    size_t len = strlen(lsn);
    if (len >= sizeof(ctx->keel_write_gtid))
        len = sizeof(ctx->keel_write_gtid) - 1;
    memcpy(ctx->keel_write_gtid, lsn, len);
    ctx->keel_write_gtid[len] = '\0';
}

/* ============================================================================
 * VTable: get_stmt_replay
 * ============================================================================ */

/**
 * @brief Vtable hook: build a replay buffer of COM_STMT_PREPARE messages for all
 *        active prepared statements.
 *
 * Called by the engine when a session with active prepared statements is assigned
 * a new backend whose stmt_set_hash doesn't match the session's.  The engine
 * replays these packets to the new backend before forwarding the client's
 * COM_STMT_EXECUTE.
 *
 * The returned buffer is heap-allocated via keel_malloc(); the caller (engine)
 * owns it and must free it with keel_free().
 *
 * @note After replay the server will assign new stmt_ids.  The engine is
 *       responsible for updating the session's stmt_id mapping if required.
 *
 * @param vctx            Protocol flow context.
 * @param replay_buf_out  Set to malloc'd buffer of concatenated COM_STMT_PREPARE
 *                        packets; NULL when no active statements exist.
 * @param replay_len_out  Total byte count of *replay_buf_out.
 * @param stmt_count_out  Number of COM_STMT_PREPARE packets in the buffer.
 * @param hash_out        Current session_stmt_hash (for backend matching).
 * @return 0 on success, -1 on allocation failure.
 */
static int myf_get_stmt_replay(void*     vctx,
                                uint8_t** replay_buf_out,
                                size_t*   replay_len_out,
                                uint32_t* stmt_count_out,
                                uint64_t* hash_out) {
    my_flow_ctx_t* ctx = vctx;
    /* Support hash-only probe: callers (e.g. pool_wait_resume_cb in
     * src/worker/worker.c) may pass NULL for buf/len/count when they
     * only need the session_stmt_hash for backend selection. Matches the
     * PG implementation's contract (see pgf_get_stmt_replay). */
    if (replay_buf_out)  *replay_buf_out  = NULL;
    if (replay_len_out)  *replay_len_out  = 0;
    if (stmt_count_out)  *stmt_count_out  = 0;
    if (hash_out)        *hash_out        = ctx ? ctx->session_stmt_hash : 0;

    if (!ctx || ctx->stmt_active_count == 0) return 0;
    if (!replay_buf_out) return 0;  /* hash-only probe — done */

    /* First pass: measure total buffer size */
    size_t   total_size = 0;
    uint32_t count      = 0;
    for (int i = 0; i < MY_STMT_MAP_SIZE; i++) {
        if (!ctx->stmt_map[i].valid) continue;
        total_size += MY_HDR + 1 + ctx->stmt_map[i].sql_len;
        count++;
    }
    if (count == 0) return 0;

    uint8_t* buf = keel_malloc(total_size);
    if (!buf) return -1;

    /* Second pass: build COM_STMT_PREPARE packets */
    size_t pos = 0;
    for (int i = 0; i < MY_STMT_MAP_SIZE; i++) {
        if (!ctx->stmt_map[i].valid) continue;
        size_t sql_len = ctx->stmt_map[i].sql_len;
        size_t pl      = 1 + sql_len;  /* cmd(1) + sql */
        wrle24(buf + pos, (uint32_t)pl);
        buf[pos + 3]       = 0;                 /* seq_id = 0 per command */
        buf[pos + MY_HDR]  = MY_COM_STMT_PREPARE;
        memcpy(buf + pos + MY_HDR + 1, ctx->stmt_map[i].sql, sql_len);
        pos += MY_HDR + pl;
    }

    *replay_buf_out = buf;
    *replay_len_out = pos;
    *stmt_count_out = count;
    return 0;
}

/* ============================================================================
 * Async Cancel (COM_PROCESS_KILL)
 * ============================================================================ */

/**
 * @brief Arguments passed to the cancel background thread.
 *
 * All strings are copied in myf_cancel_async() before the thread is spawned,
 * so the caller's buffers are not touched after the function returns.
 */
typedef struct myf_cancel_args {
    char     host[256];
    uint16_t port;
    char     user[128];
    char     password[128];
    char     database[128];
    uint32_t connection_id;
} myf_cancel_args_t;

/**
 * @brief Background thread: connect, authenticate, send COM_PROCESS_KILL.
 *
 * Runs in a detached thread to avoid blocking the reactor loop.  The thread
 * opens a new blocking TCP connection, performs the MySQL handshake with the
 * stored credentials, sends COM_PROCESS_KILL, and closes the socket.  All
 * failure paths are silent — cancel is best-effort.
 */
static void* myf_cancel_thread(void* arg)
{
    myf_cancel_args_t* a = (myf_cancel_args_t*)arg;

    /* ------------------------------------------------------------------
     * 1. Connect (blocking)
     * ------------------------------------------------------------------ */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { keel_free(a); return NULL; }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /* Set a generous overall timeout so we don't block forever. */
    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(a->port);
    if (inet_pton(AF_INET, a->host, &addr.sin_addr) != 1) {
        close(fd); keel_free(a); return NULL;
    }

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        close(fd); keel_free(a); return NULL;
    }

    /* ------------------------------------------------------------------
     * 2. Read MySQL server greeting
     * ------------------------------------------------------------------ */
    uint8_t gbuf[512];
    ssize_t glen = recv(fd, gbuf, sizeof(gbuf), 0);
    if (glen < (ssize_t)(MY_HDR + 1)) { close(fd); keel_free(a); return NULL; }

    my_handshake_info_t hs;
    if (my_parse_greeting(gbuf, (size_t)glen, &hs) != 0) {
        close(fd); keel_free(a); return NULL;
    }

    /* ------------------------------------------------------------------
     * 3. Send HandshakeResponse (authenticate)
     * ------------------------------------------------------------------ */
    uint8_t resp_buf[512];
    ssize_t resp_len = my_build_handshake_response(
        &hs,
        a->user[0] ? a->user : NULL,
        a->database[0] ? a->database : NULL,
        a->password,
        resp_buf, sizeof(resp_buf));
    if (resp_len <= 0) { close(fd); keel_free(a); return NULL; }

    if (send(fd, resp_buf, (size_t)resp_len, MSG_NOSIGNAL) != resp_len) {
        close(fd); keel_free(a); return NULL;
    }

    /* ------------------------------------------------------------------
     * 4. Read auth result; handle AuthSwitchRequest if needed
     * ------------------------------------------------------------------ */
    uint8_t auth_buf[512];
    ssize_t auth_len = recv(fd, auth_buf, sizeof(auth_buf), 0);
    if (auth_len < (ssize_t)(MY_HDR + 1)) { close(fd); keel_free(a); return NULL; }

    my_auth_result_t ar;
    if (my_parse_auth_result(auth_buf, (size_t)auth_len, &ar) != 0) {
        close(fd); keel_free(a); return NULL;
    }

    if (ar.type == MY_AUTH_SWITCH) {
        /* Handle one AuthSwitchRequest round-trip */
        uint8_t sw_resp[256];
        ssize_t sw_len = my_build_auth_switch_response(
            ar.switch_plugin,
            ar.switch_scramble, ar.switch_scramble_len,
            a->password,
            (uint8_t)(hs.seq_id + 2),
            sw_resp, sizeof(sw_resp));
        if (sw_len > 0)
            send(fd, sw_resp, (size_t)sw_len, MSG_NOSIGNAL);

        /* Read final OK or ERR */
        uint8_t final_buf[64];
        ssize_t final_len = recv(fd, final_buf, sizeof(final_buf), 0);
        if (final_len < (ssize_t)(MY_HDR + 1)) { close(fd); keel_free(a); return NULL; }
        if (my_parse_auth_result(final_buf, (size_t)final_len, &ar) != 0) {
            close(fd); keel_free(a); return NULL;
        }
    }

    if (ar.type != MY_AUTH_OK) { close(fd); keel_free(a); return NULL; }

    /* ------------------------------------------------------------------
     * 5. Send COM_PROCESS_KILL <connection_id> (4-byte LE uint32)
     * ------------------------------------------------------------------ */
    uint8_t kill_pkt[9];
    /* 3-byte LE length = 5 (1 cmd + 4 id) */
    kill_pkt[0] = 5; kill_pkt[1] = 0; kill_pkt[2] = 0;
    kill_pkt[3] = 0;                /* seq_id */
    kill_pkt[4] = MY_COM_PROCESS_KILL;
    kill_pkt[5] = (uint8_t)(a->connection_id);
    kill_pkt[6] = (uint8_t)(a->connection_id >> 8);
    kill_pkt[7] = (uint8_t)(a->connection_id >> 16);
    kill_pkt[8] = (uint8_t)(a->connection_id >> 24);

    send(fd, kill_pkt, sizeof(kill_pkt), MSG_NOSIGNAL);

    /* Drain the OK/ERR response so the server doesn't see RST */
    uint8_t drain[64];
    recv(fd, drain, sizeof(drain), 0);

    close(fd);
    keel_free(a);
    return NULL;
}

/**
 * @brief Vtable hook: queue an async COM_PROCESS_KILL cancel.
 *
 * Spawns a detached thread to perform the full MySQL authenticate-and-kill
 * sequence without blocking the reactor event loop.
 */
static int myf_cancel_async(const char* host, uint16_t port,
                             const char* user, const char* password,
                             const char* database, uint32_t connection_id)
{
    myf_cancel_args_t* a = keel_malloc(sizeof(*a));
    if (!a) return -1;

    memset(a, 0, sizeof(*a));
    if (host)     { strncpy(a->host,     host,     sizeof(a->host)     - 1); }
    if (user)     { strncpy(a->user,     user,     sizeof(a->user)     - 1); }
    if (password) { strncpy(a->password, password, sizeof(a->password) - 1); }
    if (database) { strncpy(a->database, database, sizeof(a->database) - 1); }
    a->port          = port;
    a->connection_id = connection_id;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_t tid;
    int rc = pthread_create(&tid, &attr, myf_cancel_thread, a);
    pthread_attr_destroy(&attr);

    if (rc != 0) { keel_free(a); return -1; }
    return 0;
}

/* ============================================================================
 * Parity hooks: get_stmt_compat_profile / captured_fe_pin_effects /
 * build_commit_doubt_check / generate_commit_doubt_response
 * ============================================================================
 *
 * These four hooks bring the MySQL vtable to feature parity with the
 * PostgreSQL plugin so the engine does not have to special-case the
 * protocol when handling backend reuse, deferred client payloads, or
 * commit-in-doubt recovery.
 */

/**
 * @brief Vtable hook: report the session’s prepared-statement compatibility
 *        profile so the router can decide whether a borrowed backend is
 *        semantically reusable.
 *
 * MySQL tracks the live named-statement set in ctx->session_stmt_hash
 * (XOR-fold of per-stmt_id hashes; see my_stmt_add/my_stmt_remove). The
 * remaining semantic fields — search_path / role / GUC equivalents — are
 * NOT tracked yet for MySQL: things like SET @@session.*, user variables
 * (KEEL_FPIN_USER_VARIABLE), and active LOCK TABLES are surfaced through
 * pins rather than a profile hash. To stay correct in the face of that
 * gap we set @c semantic_unknown so the engine takes the conservative
 * clean-replay path instead of trusting a zero hash to mean “identical”.
 */
static int myf_get_stmt_compat_profile(void* vctx,
                                       keel_stmt_compat_profile_t* out)
{
    my_flow_ctx_t* ctx = (my_flow_ctx_t*)vctx;
    if (!ctx || !out) return -1;

    memset(out, 0, sizeof(*out));
    out->stmt_set_hash        = ctx->session_stmt_hash;
    out->role_hash            = ctx->stmt_role_hash;
    /* MySQL has no Postgres-style search_path; current database is the
     * closest analog for hash-based reuse gating. */
    out->search_path_hash     = ctx->stmt_db_hash;
    out->guc_hash             = ctx->stmt_guc_hash;
    /* schema_epoch bumps whenever the tracked database changes; reuse the
     * db_hash as the epoch identity so a USE flips it. */
    out->schema_epoch         = ctx->stmt_db_hash;
    out->semantic_profile_hash = ctx->stmt_role_hash
                              ^ ctx->stmt_db_hash
                              ^ ctx->stmt_guc_hash;
    /* Only mark the profile opaque if we actually saw an unmodellable SET. */
    out->semantic_unknown     = ctx->stmt_semantic_unknown;
    return 0;
}

/**
 * @brief Vtable hook: walk a captured frontend payload and report pin
 *        bits the engine would otherwise miss.
 *
 * The engine calls this when it has had to defer a whole FE chunk (e.g.
 * during state_sync drain, slot cleanup, or PS replay) so the frame-by-
 * frame on_fe_msg() loop does not run on those bytes. MySQL has no
 * pipeline marker analogous to the Postgres extended-protocol Sync ('S'),
 * so today this hook is a frame-structure validator only — it iterates
 * complete MySQL packets and reports no pin transitions. The walk is kept
 * (rather than returning immediately) so we tolerate partial trailing
 * frames as the contract requires, and so that future per-frame pin
 * effects (e.g. clearing KEEL_FPIN_LOCK_TABLE on UNLOCK TABLES) can be
 * slotted in without further engine-side changes.
 */
static void myf_captured_fe_pin_effects(void* vctx,
                                         const uint8_t* data,
                                         size_t len,
                                         keel_flow_pin_reason_t* pin_update,
                                         keel_flow_pin_reason_t* pin_clear)
{
    (void)vctx;
    if (pin_update) *pin_update = KEEL_FPIN_NONE;
    if (pin_clear)  *pin_clear  = KEEL_FPIN_NONE;
    if (!data) return;

    size_t pos = 0;
    while (pos + MY_HDR <= len) {
        uint32_t pkt_len = (uint32_t)data[pos]
                         | ((uint32_t)data[pos + 1] << 8)
                         | ((uint32_t)data[pos + 2] << 16);
        size_t frame_len = (size_t)MY_HDR + pkt_len;
        if (pos + frame_len > len) return;          /* partial trailing frame */
        /* No per-frame pin transitions are reported here. */
        pos += frame_len;
    }
}

/**
 * @brief Vtable hook: build a COM_QUERY that checks the outcome of a
 *        commit that was lost mid-flight.
 *
 * MySQL does NOT have a direct equivalent of PostgreSQL's txid_status():
 * once the original connection is gone the only way to tell whether the
 * transaction landed is to compare the GTID set we captured around the
 * COMMIT against @@global.gtid_executed on a fresh backend.
 *
 * The token plumbing relies on the server emitting
 * SESSION_TRACK_GTIDS in the OK packet for COMMIT (enabled via
 * @@SESSION.session_track_gtids='OWN_GTID'). myf_on_be_msg parses that
 * field into ctx->keel_write_gtid and signals capture by setting
 * keel_be_action_t::commit_xid_captured on the COMMIT's OK packet. The
 * engine forwards the captured token here as @p xid; we use it only as
 * a "token is valid" signal and source the actual GTID string from the
 * context, since the engine's uint64 slot cannot round-trip the full
 * GTID set.
 *
 * When @p xid is non-zero and a GTID was captured we emit:
 *   `SELECT GTID_SUBSET('<gtid>', @@global.gtid_executed)`
 * \u2014 result `1` means the original commit landed, `0` means it did not.
 */
static ssize_t myf_build_commit_doubt_check(void* vctx,
                                            uint64_t xid,
                                            uint8_t* out_buf,
                                            size_t out_cap)
{
    my_flow_ctx_t* ctx = (my_flow_ctx_t*)vctx;
    if (!out_buf || out_cap < MY_HDR + 1)
        return -1;
    /* No commit-token captured yet — tell the engine we cannot probe. */
    if (xid == 0 || !ctx || ctx->keel_write_gtid[0] == '\0')
        return 0;

    /* Defence in depth: same GTID-charset gate as myf_replica_reached_token
     * so we never embed an attacker-controlled byte in the SQL literal. */
    size_t gtid_len = strlen(ctx->keel_write_gtid);
    if (gtid_len == 0 || gtid_len > 500) return -1;
    for (size_t i = 0; i < gtid_len; ++i) {
        unsigned char c = (unsigned char)ctx->keel_write_gtid[i];
        bool ok = (c >= '0' && c <= '9') ||
                  (c >= 'a' && c <= 'f') ||
                  (c >= 'A' && c <= 'F') ||
                  c == ':' || c == ',' || c == '-' ||
                  c == ' ' || c == '\t' || c == '\n' || c == '\r';
        if (!ok) return -1;
    }

    char sql[576];
    int sql_len = snprintf(sql, sizeof(sql),
                           "SELECT GTID_SUBSET('%s', @@global.gtid_executed)",
                           ctx->keel_write_gtid);
    if (sql_len < 0 || (size_t)sql_len >= sizeof(sql))
        return -1;

    size_t payload_len = 1u + (size_t)sql_len;          /* COM_QUERY + SQL */
    size_t pkt_total   = (size_t)MY_HDR + payload_len;
    if (pkt_total > out_cap) return -1;

    wrle24(out_buf, (uint32_t)payload_len);
    out_buf[3] = 0;                                     /* seq_id = 0 */
    out_buf[MY_HDR] = MY_COM_QUERY;
    memcpy(out_buf + MY_HDR + 1, sql, (size_t)sql_len);
    return (ssize_t)pkt_total;
}

/**
 * @brief Vtable hook: build the client-facing response after a
 *        commit-in-doubt episode resolves (or fails to resolve).
 *
 * Mirrors pgf_generate_commit_doubt_response but emits MySQL packets:
 *   - RESOLVED_COMMITTED → OK packet (affected_rows=0, autocommit).
 *   - everything else    → ERR packet with a SQLSTATE that matches the
 *                          severity (‘40000’ for ABORTED, ‘08006’ for
 *                          connection-level loss-of-confirmation cases).
 *
 * Without this hook the engine falls back to writing a Postgres-style 'E'
 * frame onto the MySQL client socket, which breaks the protocol stream.
 */
static ssize_t myf_generate_commit_doubt_response(void* vctx,
                                                  keel_commit_doubt_reason_t reason,
                                                  uint64_t xid,
                                                  uint8_t* out_buf,
                                                  size_t out_cap)
{
    my_flow_ctx_t* ctx = (my_flow_ctx_t*)vctx;
    if (!out_buf || out_cap < MY_HDR + 16) return -1;

    /* ---- Success: OK packet (affected_rows=0, last_insert_id=0,
     *      status=SERVER_STATUS_AUTOCOMMIT, warnings=0). ---- */
    if (reason == KEEL_CIDR_RESOLVED_COMMITTED) {
        static const uint8_t kOkPayload[] = {
            0x00,           /* OK header */
            0x00,           /* affected_rows (lenenc=0) */
            0x00,           /* last_insert_id (lenenc=0) */
            0x02, 0x00,     /* status_flags = SERVER_STATUS_AUTOCOMMIT */
            0x00, 0x00,     /* warnings = 0 */
        };
        size_t total = (size_t)MY_HDR + sizeof(kOkPayload);
        if (total > out_cap) return -1;
        wrle24(out_buf, (uint32_t)sizeof(kOkPayload));
        out_buf[3] = ctx ? (uint8_t)(ctx->seq_id + 1) : 1;
        memcpy(out_buf + MY_HDR, kOkPayload, sizeof(kOkPayload));
        return (ssize_t)total;
    }

    /* ---- Failure: build a MySQL ERR packet with a human-readable msg. ---- */
    const char* sqlstate = "08006";
    uint16_t    err_no   = 1105;        /* ER_UNKNOWN_ERROR */
    char        msg[256];
    switch (reason) {
    case KEEL_CIDR_NO_XID:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: transaction outcome unknown (no commit token captured)");
        break;
    case KEEL_CIDR_NO_RW_POOL:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: no RW pool \u2014 compare captured GTID against @@global.gtid_executed to resolve (token=%llu)",
                 (unsigned long long)xid);
        break;
    case KEEL_CIDR_NO_CHECK_CONN:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: pool unavailable \u2014 verify GTID_SUBSET against @@global.gtid_executed (token=%llu)",
                 (unsigned long long)xid);
        break;
    case KEEL_CIDR_CHECK_BUILD_FAIL:
    case KEEL_CIDR_CHECK_SEND_FAIL:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: outcome-check failed \u2014 verify GTID_SUBSET manually (token=%llu)",
                 (unsigned long long)xid);
        break;
    case KEEL_CIDR_RESOLVED_ABORTED:
        sqlstate = "40000";
        err_no   = 1213;    /* ER_LOCK_DEADLOCK — closest “txn rolled back” code */
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: transaction was rolled back");
        break;
    case KEEL_CIDR_RESOLVED_UNKNOWN:
    default:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: outcome uncertain for token=%llu \u2014 verify GTID_SUBSET manually",
                 (unsigned long long)xid);
        break;
    }

    size_t ml    = strlen(msg);
    size_t pl    = 1 + 2 + 1 + 5 + ml;        /* 0xFF + errno + '#' + SQLSTATE + msg */
    size_t total = (size_t)MY_HDR + pl;
    if (total > out_cap) return -1;

    wrle24(out_buf, (uint32_t)pl);
    out_buf[3] = ctx ? (uint8_t)(ctx->seq_id + 1) : 1;

    size_t p = MY_HDR;
    out_buf[p++] = MY_ERR;
    out_buf[p++] = (uint8_t)(err_no & 0xFF);
    out_buf[p++] = (uint8_t)((err_no >> 8) & 0xFF);
    out_buf[p++] = '#';
    memcpy(out_buf + p, sqlstate, 5); p += 5;
    memcpy(out_buf + p, msg, ml);
    return (ssize_t)total;
}

/* ============================================================================
 * VTable Definition
 * ============================================================================ */

const keel_proto_flow_vtable_t keel_proto_flow_mysql = {
    .name             = "mysql",
    .default_port     = 3306,
    .generate_greeting = myf_generate_greeting,
    .create_context   = myf_create,
    .destroy_context  = myf_destroy,
    .frame_len        = myf_frame_len,
    .on_fe_msg        = myf_on_fe_msg,
    .on_be_msg        = myf_on_be_msg,
    .is_data_frame    = myf_is_data_frame,
    .fingerprint      = myf_fingerprint,
    .build_state_sync = myf_build_state_sync,
    .build_cleanup    = myf_build_cleanup,
    .backend_reuse_gate = myf_reuse_gate,
    .generate_startup = myf_gen_startup,
    .generate_error   = myf_gen_error,
    .generate_ready_for_query = NULL,   /* MySQL: not needed (OK packet suffices) */
    /* Commit-in-doubt resolution (PG parity) */
    .build_commit_doubt_check        = myf_build_commit_doubt_check,
    .generate_commit_doubt_response  = myf_generate_commit_doubt_response,
    /* Phase 5 optional extensions */
    .get_info              = myf_get_info,
    .classify_error        = myf_classify_error,
    .capture_consistency_token = myf_capture_consistency_token,
    .replica_reached_token = myf_replica_reached_token,
    .begin_stream          = NULL,      /* LOAD DATA handled via on_be_msg */
    .stream_write          = NULL,
    .end_stream            = NULL,
    .cleanup_slot          = myf_cleanup_slot,
    .drain_cleanup_response = myf_drain_cleanup_response,
    .probe_backend         = myf_probe_backend,
    .get_backend_metadata  = myf_get_backend_metadata,
    .get_metrics           = myf_get_metrics,
    .notify_write_lsn      = myf_notify_write_lsn,
    .get_stmt_replay       = myf_get_stmt_replay,
    .get_stmt_compat_profile   = myf_get_stmt_compat_profile,
    .rewrite_execute_anonymous = NULL,  /* MySQL has no anonymous PS mode */
    .captured_fe_pin_effects   = myf_captured_fe_pin_effects,
    .cancel_async              = myf_cancel_async,
};
