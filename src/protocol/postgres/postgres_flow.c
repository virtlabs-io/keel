/**
 * @file postgres_flow.c
 * @brief PostgreSQL wire-protocol flow plugin and session-state model.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This is one of the most semantically dense translation units in KEEL. It does far
 * more than frame messages: it interprets PostgreSQL startup packets, models session
 * and transaction state, classifies SQL for routing and pinning decisions, virtualizes
 * prepared statements, tracks statement-relevant GUC changes, and helps the engine
 * decide when a backend can safely return to the pool. The implementation is large
 * because PostgreSQL offers multiple protocol modes and many ways for a client to
 * create backend-local state that a pooler must either replay, clean up, or pin.
 */

#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/protocol.h"
#include "keel/protocol/pg_backend_auth.h"
#include "keel/sql/sql.h"
#include "keel_types.h"
#include "keel/session/state_profile.h"
#include "keel/session/hardpin.h"
#include "keel/plugin/plugin_types.h"
#include "keel/engine/worker.h"
#include "keel/core/auth.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/util/endian.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include "keel/util/platform_compat.h"
#include "keel/util/util.h"   /* keel_hash_fnv1a_64 */
#include "keel/log/log.h"
#include <ctype.h>             /* isalnum, isspace */

/* ---- Constants ---- */
#define PG_SSL_REQUEST_CODE    80877103
#define PG_CANCEL_REQUEST_CODE 80877102
#define PG_PROTOCOL_V3         0x00030000

/* ---- Statement cache helpers ---- */

/* Forward declaration (defined after pg_stmt_context_sig / pg_stmt_entry_hash) */
static void pg_stmt_recompute_session_hash(pg_flow_ctx_t* ctx);
static uint64_t pg_stmt_entry_hash(const pg_stmt_entry_t* entry,
                                   uint64_t context_sig);

/** Find a stmt_cache entry by name; returns NULL if not found. */
static pg_stmt_entry_t* pg_stmt_find(pg_flow_ctx_t* ctx, const char* name) {
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (ctx->stmt_cache[i].valid &&
            strcmp(ctx->stmt_cache[i].name, name) == 0)
            return &ctx->stmt_cache[i];
    }
    return NULL;
}

/** Find or allocate a stmt_cache slot for the given name.
 *  If the cache is full, evict the oldest slot (round-robin). */
static pg_stmt_entry_t* pg_stmt_upsert(pg_flow_ctx_t* ctx, const char* name) {
    /* Existing entry? Return it (caller will overwrite fields) */
    pg_stmt_entry_t* e = pg_stmt_find(ctx, name);
    if (e) {
        /* Old entry cleared; session hash will be rebuilt by caller via
         * pg_stmt_recompute_session_hash() after the new entry is set up. */
        if (e->wire_msg) { keel_free(e->wire_msg); e->wire_msg = NULL; e->wire_msg_len = 0; }
        e->hash = 0;
        e->confirmed = false;
        return e;
    }
    /* Free slot? */
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (!ctx->stmt_cache[i].valid) {
            strncpy(ctx->stmt_cache[i].name, name, sizeof(ctx->stmt_cache[i].name) - 1);
            ctx->stmt_cache[i].name[sizeof(ctx->stmt_cache[i].name) - 1] = '\0';
            ctx->stmt_cache[i].confirmed = false;
            return &ctx->stmt_cache[i];
        }
    }
    /* Full — evict using round-robin; only remove confirmed hash contribution */
    uint32_t idx = ctx->stmt_evict_next % PG_STMT_CACHE_SIZE;
    ctx->stmt_evict_next++;
    pg_stmt_entry_t* slot = &ctx->stmt_cache[idx];
    if (slot->valid) {
        /* Evicted entry cleared; session hash rebuilt by caller. */
        if (slot->wire_msg) { keel_free(slot->wire_msg); slot->wire_msg = NULL; }
    }
    memset(slot, 0, sizeof(*slot));
    strncpy(slot->name, name, sizeof(slot->name) - 1);
    return slot;
}

/** Remove a stmt_cache entry by name (called on Close or DEALLOCATE). */
static void pg_stmt_remove(pg_flow_ctx_t* ctx, const char* name) {
    pg_stmt_entry_t* e = pg_stmt_find(ctx, name);
    if (!e) return;
    /* Free heap-allocated wire message */
    if (e->wire_msg) { keel_free(e->wire_msg); e->wire_msg = NULL; e->wire_msg_len = 0; }
    e->valid = false; e->name[0] = '\0'; e->hash = 0; e->confirmed = false;
    /* Rebuild session hash from scratch after entry removal so the
     * context_sig seed is always correctly included. */
    pg_stmt_recompute_session_hash(ctx);
}

/** Clear all tracked named prepared statements and pending tracking state. */
static void pg_stmt_clear_all(pg_flow_ctx_t* ctx)
{
    if (!ctx) return;

    if (ctx->pending_track_valid) {
        if (ctx->pending_track_prior.wire_msg) {
            keel_free(ctx->pending_track_prior.wire_msg);
            ctx->pending_track_prior.wire_msg = NULL;
        }
        ctx->pending_track_valid = false;
        ctx->pending_track_had_prior = false;
    }

    ctx->pending_parse_valid = false;
    ctx->pending_parse_hash = 0;
    ctx->pending_parse_name[0] = '\0';
    ctx->pending_deallocate_valid = false;
    ctx->pending_deallocate_absorbed_error = false;
    ctx->stmt_discard_plans_before_execute = false;
    ctx->stmt_discard_plans_absorb_pending = false;
    ctx->named_stmt_count = 0;

    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (ctx->stmt_cache[i].wire_msg) {
            keel_free(ctx->stmt_cache[i].wire_msg);
            ctx->stmt_cache[i].wire_msg = NULL;
        }
        memset(&ctx->stmt_cache[i], 0, sizeof(ctx->stmt_cache[i]));
    }

    if (ctx->anon_map) {
        keel_free(ctx->anon_map);
        ctx->anon_map = NULL;
    }

    ctx->session_stmt_hash = 0;
}

/** Return true if the session has any confirmed prepared statement entries. */
static bool pg_stmt_has_confirmed_entries(const pg_flow_ctx_t* ctx)
{
    if (!ctx) return false;

    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        const pg_stmt_entry_t* e = &ctx->stmt_cache[i];
        if (e->valid && e->confirmed)
            return true;
    }

    return false;
}

/** Copy stmt_cache entry classification into the active cached_* fields. */
static void pg_stmt_activate(pg_flow_ctx_t* ctx, const pg_stmt_entry_t* e) {
    size_t copy_len = e->sql_len < sizeof(ctx->cached_sql) - 1
                    ? e->sql_len : sizeof(ctx->cached_sql) - 1;
    memcpy(ctx->cached_sql, e->sql, copy_len);
    ctx->cached_sql[copy_len] = '\0';
    ctx->cached_sql_len = copy_len;
    ctx->cached_query_type = e->query_type;
    ctx->cached_effect = e->effect;
    ctx->cached_route = e->route;
    ctx->cached_pin_set = e->pin_set;
    ctx->cached_pin_clr = e->pin_clr;
    ctx->cached_valid = true;
}

/* ---- Byte helpers (rd32/wr32) use keel/util/endian.h ---- */
#define rd32(p)   keel_be32_get(p)
#define wr32(p,v) keel_be32_put((p),(v))

typedef enum pg_stmt_guc_kind {
    PG_STMT_GUC_INVALID = -1,
    PG_STMT_GUC_SEARCH_PATH = 0,
    PG_STMT_GUC_TIMEZONE,
    PG_STMT_GUC_DATESTYLE,
    PG_STMT_GUC_INTERVALSTYLE,
    PG_STMT_GUC_STANDARD_CONFORMING_STRINGS,
    PG_STMT_GUC_BACKSLASH_QUOTE,
    PG_STMT_GUC_ESCAPE_STRING_WARNING,
    PG_STMT_GUC_DEFAULT_TABLESPACE,
    PG_STMT_GUC_TEMP_TABLESPACES,
    PG_STMT_GUC_DEFAULT_TABLE_ACCESS_METHOD,
    PG_STMT_GUC_ROW_SECURITY,
    PG_STMT_GUC_WORK_MEM,
    PG_STMT_GUC_STATEMENT_TIMEOUT,
    PG_STMT_GUC_LOCK_TIMEOUT,
    PG_STMT_GUC_CLIENT_ENCODING,
    PG_STMT_GUC_COUNT
} pg_stmt_guc_kind_t;

typedef struct pg_stmt_guc_change {
    bool               valid;
    bool               is_local;
    bool               is_reset_all;
    pg_stmt_guc_kind_t kind;
    char               value[256];
} pg_stmt_guc_change_t;

static void pg_stmt_restamp_context(pg_flow_ctx_t* ctx);

/**
 * @brief Return the context-signature label string for a GUC kind.
 *
 * Labels are used as separators when building the FNV-1a context
 * signature string (e.g. "|search_path=", "|timezone=").  The leading
 * pipe makes each key-value pair distinct in the concatenated hash input.
 *
 * @param kind  GUC parameter kind enum value.
 * @return Static label string; empty string for unknown kinds.
 */
static const char* pg_stmt_guc_context_label(pg_stmt_guc_kind_t kind)
{
    switch (kind) {
    case PG_STMT_GUC_SEARCH_PATH:
        return "|search_path=";
    case PG_STMT_GUC_TIMEZONE:
        return "|timezone=";
    case PG_STMT_GUC_DATESTYLE:
        return "|datestyle=";
    case PG_STMT_GUC_INTERVALSTYLE:
        return "|intervalstyle=";
    case PG_STMT_GUC_STANDARD_CONFORMING_STRINGS:
        return "|standard_conforming_strings=";
    case PG_STMT_GUC_BACKSLASH_QUOTE:
        return "|backslash_quote=";
    case PG_STMT_GUC_ESCAPE_STRING_WARNING:
        return "|escape_string_warning=";
    case PG_STMT_GUC_DEFAULT_TABLESPACE:
        return "|default_tablespace=";
    case PG_STMT_GUC_TEMP_TABLESPACES:
        return "|temp_tablespaces=";
    case PG_STMT_GUC_DEFAULT_TABLE_ACCESS_METHOD:
        return "|default_table_access_method=";
    case PG_STMT_GUC_ROW_SECURITY:
        return "|row_security=";
    case PG_STMT_GUC_WORK_MEM:
        return "|work_mem=";
    case PG_STMT_GUC_STATEMENT_TIMEOUT:
        return "|statement_timeout=";
    case PG_STMT_GUC_LOCK_TIMEOUT:
        return "|lock_timeout=";
    case PG_STMT_GUC_CLIENT_ENCODING:
        return "|client_encoding=";
    default:
        return "";
    }
}

/**
 * @brief Return the SQL GUC parameter name string for a GUC kind.
 *
 * These are the names used in `SET <name> = <value>` statements
 * (e.g. "search_path", "timezone").
 *
 * @param kind  GUC parameter kind enum value.
 * @return Static SQL name string; empty string for unknown kinds.
 */
static const char* pg_stmt_guc_sql_name(pg_stmt_guc_kind_t kind)
{
    switch (kind) {
    case PG_STMT_GUC_SEARCH_PATH:
        return "search_path";
    case PG_STMT_GUC_TIMEZONE:
        return "timezone";
    case PG_STMT_GUC_DATESTYLE:
        return "datestyle";
    case PG_STMT_GUC_INTERVALSTYLE:
        return "intervalstyle";
    case PG_STMT_GUC_STANDARD_CONFORMING_STRINGS:
        return "standard_conforming_strings";
    case PG_STMT_GUC_BACKSLASH_QUOTE:
        return "backslash_quote";
    case PG_STMT_GUC_ESCAPE_STRING_WARNING:
        return "escape_string_warning";
    case PG_STMT_GUC_DEFAULT_TABLESPACE:
        return "default_tablespace";
    case PG_STMT_GUC_TEMP_TABLESPACES:
        return "temp_tablespaces";
    case PG_STMT_GUC_DEFAULT_TABLE_ACCESS_METHOD:
        return "default_table_access_method";
    case PG_STMT_GUC_ROW_SECURITY:
        return "row_security";
    case PG_STMT_GUC_WORK_MEM:
        return "work_mem";
    case PG_STMT_GUC_STATEMENT_TIMEOUT:
        return "statement_timeout";
    case PG_STMT_GUC_LOCK_TIMEOUT:
        return "lock_timeout";
    case PG_STMT_GUC_CLIENT_ENCODING:
        return "client_encoding";
    default:
        return "";
    }
}

/**
 * @brief Resolve the storage pointers for a tracked GUC parameter.
 *
 * Populates the effective/session/local buffer pointers and `local_active`
 * flag for @p kind from the pg_flow_ctx_t fields.  Used by the GUC change
 * applicator to avoid a separate switch statement per operation.
 *
 * @param ctx          Flow context containing the GUC buffers.
 * @param kind         GUC parameter to resolve.
 * @param[out] effective   Pointer to the active value buffer.
 * @param[out] session     Pointer to the session-level value buffer.
 * @param[out] local       Pointer to the transaction-local value buffer.
 * @param[out] local_active Pointer to the local-active flag.
 * @param[out] cap         Buffer capacity (always 256 for current GUCs).
 * @return true on success; false if @p kind is unknown.
 */
static bool pg_stmt_guc_storage(pg_flow_ctx_t* ctx,
                                pg_stmt_guc_kind_t kind,
                                char** effective,
                                char** session,
                                char** local,
                                bool** local_active,
                                size_t* cap)
{
    *cap = 256;
    switch (kind) {
    case PG_STMT_GUC_SEARCH_PATH:
        *effective = ctx->stmt_search_path;
        *session = ctx->stmt_search_path_session;
        *local = ctx->stmt_search_path_local;
        *local_active = &ctx->stmt_search_path_local_active;
        return true;
    case PG_STMT_GUC_TIMEZONE:
        *effective = ctx->stmt_timezone;
        *session = ctx->stmt_timezone_session;
        *local = ctx->stmt_timezone_local;
        *local_active = &ctx->stmt_timezone_local_active;
        return true;
    case PG_STMT_GUC_DATESTYLE:
        *effective = ctx->stmt_datestyle;
        *session = ctx->stmt_datestyle_session;
        *local = ctx->stmt_datestyle_local;
        *local_active = &ctx->stmt_datestyle_local_active;
        return true;
    case PG_STMT_GUC_INTERVALSTYLE:
        *effective = ctx->stmt_intervalstyle;
        *session = ctx->stmt_intervalstyle_session;
        *local = ctx->stmt_intervalstyle_local;
        *local_active = &ctx->stmt_intervalstyle_local_active;
        return true;
    case PG_STMT_GUC_STANDARD_CONFORMING_STRINGS:
        *effective = ctx->stmt_standard_conforming_strings;
        *session = ctx->stmt_standard_conforming_strings_session;
        *local = ctx->stmt_standard_conforming_strings_local;
        *local_active = &ctx->stmt_standard_conforming_strings_local_active;
        return true;
    case PG_STMT_GUC_BACKSLASH_QUOTE:
        *effective = ctx->stmt_backslash_quote;
        *session = ctx->stmt_backslash_quote_session;
        *local = ctx->stmt_backslash_quote_local;
        *local_active = &ctx->stmt_backslash_quote_local_active;
        return true;
    case PG_STMT_GUC_ESCAPE_STRING_WARNING:
        *effective = ctx->stmt_escape_string_warning;
        *session = ctx->stmt_escape_string_warning_session;
        *local = ctx->stmt_escape_string_warning_local;
        *local_active = &ctx->stmt_escape_string_warning_local_active;
        return true;
    case PG_STMT_GUC_DEFAULT_TABLESPACE:
        *effective = ctx->stmt_default_tablespace;
        *session = ctx->stmt_default_tablespace_session;
        *local = ctx->stmt_default_tablespace_local;
        *local_active = &ctx->stmt_default_tablespace_local_active;
        return true;
    case PG_STMT_GUC_TEMP_TABLESPACES:
        *effective = ctx->stmt_temp_tablespaces;
        *session = ctx->stmt_temp_tablespaces_session;
        *local = ctx->stmt_temp_tablespaces_local;
        *local_active = &ctx->stmt_temp_tablespaces_local_active;
        return true;
    case PG_STMT_GUC_DEFAULT_TABLE_ACCESS_METHOD:
        *effective = ctx->stmt_default_table_access_method;
        *session = ctx->stmt_default_table_access_method_session;
        *local = ctx->stmt_default_table_access_method_local;
        *local_active = &ctx->stmt_default_table_access_method_local_active;
        return true;
    case PG_STMT_GUC_ROW_SECURITY:
        *effective = ctx->stmt_row_security;
        *session = ctx->stmt_row_security_session;
        *local = ctx->stmt_row_security_local;
        *local_active = &ctx->stmt_row_security_local_active;
        return true;
    case PG_STMT_GUC_WORK_MEM:
        *effective = ctx->stmt_work_mem;
        *session = ctx->stmt_work_mem_session;
        *local = ctx->stmt_work_mem_local;
        *local_active = &ctx->stmt_work_mem_local_active;
        return true;
    case PG_STMT_GUC_STATEMENT_TIMEOUT:
        *effective = ctx->stmt_statement_timeout;
        *session = ctx->stmt_statement_timeout_session;
        *local = ctx->stmt_statement_timeout_local;
        *local_active = &ctx->stmt_statement_timeout_local_active;
        return true;
    case PG_STMT_GUC_LOCK_TIMEOUT:
        *effective = ctx->stmt_lock_timeout;
        *session = ctx->stmt_lock_timeout_session;
        *local = ctx->stmt_lock_timeout_local;
        *local_active = &ctx->stmt_lock_timeout_local_active;
        return true;
    case PG_STMT_GUC_CLIENT_ENCODING:
        *effective = ctx->stmt_client_encoding;
        *session = ctx->stmt_client_encoding_session;
        *local = ctx->stmt_client_encoding_local;
        *local_active = &ctx->stmt_client_encoding_local_active;
        return true;
    default:
        return false;
    }
}

/**
 * @brief Copy a GUC value string into a fixed-size buffer (safe strncpy).
 *
 * Treats NULL @p src as empty string.  Always NUL-terminates @p dst.
 *
 * @param dst      Destination buffer.
 * @param dst_len  Capacity of @p dst in bytes (0 = no-op).
 * @param src      Source string (NULL treated as "").
 */
static void pg_stmt_copy_value(char* dst, size_t dst_len, const char* src)
{
    if (dst_len == 0)
        return;
    if (!src)
        src = "";
    strncpy(dst, src, dst_len - 1);
    dst[dst_len - 1] = '\0';
}

/**
 * @brief Copy a SQL token fragment, normalising whitespace and stripping quotes.
 *
 * Trims leading/trailing whitespace and trailing semicolons.  If the fragment
 * is a single-quoted literal, dequotes it (unescaping doubled single-quotes).
 * Result is NUL-terminated and at most @p out_len - 1 characters.
 *
 * @param begin    Start of fragment (pointer into SQL text).
 * @param end      One-past-end of fragment.
 * @param out      Destination buffer.
 * @param out_len  Capacity of @p out (0 = no-op).
 */
static void pg_stmt_copy_normalized_fragment(const char* begin, const char* end,
                                             char* out, size_t out_len)
{
    if (out_len == 0)
        return;

    while (begin < end && isspace((unsigned char)*begin))
        begin++;
    while (end > begin && isspace((unsigned char)end[-1]))
        end--;
    if (end > begin && end[-1] == ';')
        end--;
    while (end > begin && isspace((unsigned char)end[-1]))
        end--;

    if (end > begin + 1 && begin[0] == '\'' && end[-1] == '\'') {
        begin++;
        end--;
        size_t idx = 0;
        while (begin < end && idx + 1 < out_len) {
            if (begin + 1 < end && begin[0] == '\'' && begin[1] == '\'') {
                out[idx++] = '\'';
                begin += 2;
            } else {
                out[idx++] = *begin++;
            }
        }
        out[idx] = '\0';
        return;
    }

    size_t copy_len = (size_t)(end - begin);
    if (copy_len >= out_len)
        copy_len = out_len - 1;
    memcpy(out, begin, copy_len);
    out[copy_len] = '\0';
}

/**
 * @brief Find the first word-boundary occurrence of @p word in @p sql
 *        using case-insensitive comparison.
 *
 * Only returns a match when @p word is preceded and followed by non-alphanumeric
 * characters (or the start/end of the string).
 *
 * @param sql      SQL text to search.
 * @param sql_len  Length of @p sql.
 * @param word     Word to search for (NUL-terminated).
 * @return Pointer to first match within @p sql, or NULL.
 */
static const char* pg_sql_find_word_ci(const char* sql, size_t sql_len,
                                       const char* word)
{
    size_t word_len = strlen(word);
    if (word_len == 0 || sql_len < word_len)
        return NULL;

    for (size_t i = 0; i + word_len <= sql_len; i++) {
        if (i > 0 && isalnum((unsigned char)sql[i - 1]))
            continue;

        size_t j = 0;
        while (j < word_len &&
               tolower((unsigned char)sql[i + j]) ==
               tolower((unsigned char)word[j])) {
            j++;
        }
        if (j != word_len)
            continue;

        if (i + word_len < sql_len &&
            isalnum((unsigned char)sql[i + word_len]))
            continue;

        return sql + i;
    }

    return NULL;
}

/**
 * @brief Case-insensitive match of @p name against all tracked GUC parameter names.
 *
 * Iterates all `pg_stmt_guc_kind_t` values and returns the first whose SQL
 * name matches @p name (case-insensitively, exact length).
 *
 * @param name      GUC parameter name to look up.
 * @param name_len  Byte length of @p name.
 * @return Matching `pg_stmt_guc_kind_t`, or `PG_STMT_GUC_INVALID` if not found.
 */
static pg_stmt_guc_kind_t pg_stmt_match_guc_name_ci(const char* name, size_t name_len)
{
    for (int kind = 0; kind < PG_STMT_GUC_COUNT; kind++) {
        const char* candidate = pg_stmt_guc_sql_name((pg_stmt_guc_kind_t)kind);
        if (strlen(candidate) == name_len &&
            strncasecmp(name, candidate, name_len) == 0) {
            return (pg_stmt_guc_kind_t)kind;
        }
    }
    return PG_STMT_GUC_INVALID;
}

/**
 * @brief Consume a single-quoted SQL literal from position @p *p.
 *
 * Advances @p *p past the closing quote, dequoting doubled single-quotes.
 * Result is NUL-terminated and written into @p out.  Returns false if @p *p
 * does not point to a opening single-quote.
 *
 * @param p       Position pointer (advanced on success).
 * @param end     One-past-end of input.
 * @param out     Destination buffer.
 * @param out_len Capacity of @p out.
 * @return true if a literal was consumed successfully; false otherwise.
 */
static bool pg_stmt_parse_single_quoted_literal(const char** p,
                                                const char* end,
                                                char* out,
                                                size_t out_len)
{
    if (*p >= end || **p != '\'')
        return false;

    (*p)++;
    size_t idx = 0;
    while (*p < end) {
        if (**p == '\'') {
            if (*p + 1 < end && (*p)[1] == '\'') {
                if (idx + 1 < out_len)
                    out[idx++] = '\'';
                *p += 2;
                continue;
            }
            (*p)++;
            out[idx] = '\0';
            return true;
        }
        if (idx + 1 < out_len)
            out[idx++] = **p;
        (*p)++;
    }

    return false;
}

/**
 * @brief Parse a boolean GUC token from the substring [begin, end).
 *
 * Trims whitespace and trailing semicolons, then accepts:
 *  true values: "true", "on", "1", "t", "T"
 *  false values: "false", "off", "0", "f", "F"
 *
 * @param begin  Start of token.
 * @param end    One-past-end of token.
 * @param[out] value  Parsed boolean result.
 * @return true if the token was a recognisable boolean; false otherwise.
 */
static bool pg_stmt_parse_bool_token(const char* begin, const char* end, bool* value)
{
    while (begin < end && isspace((unsigned char)*begin))
        begin++;
    while (end > begin && isspace((unsigned char)end[-1]))
        end--;
    if (end > begin && end[-1] == ';')
        end--;
    while (end > begin && isspace((unsigned char)end[-1]))
        end--;

    size_t len = (size_t)(end - begin);
    if ((len == 4 && strncasecmp(begin, "true", 4) == 0) ||
        (len == 2 && strncasecmp(begin, "on", 2) == 0) ||
        (len == 1 && (*begin == '1' || *begin == 't' || *begin == 'T'))) {
        *value = true;
        return true;
    }
    if ((len == 5 && strncasecmp(begin, "false", 5) == 0) ||
        (len == 3 && strncasecmp(begin, "off", 3) == 0) ||
        (len == 1 && (*begin == '0' || *begin == 'f' || *begin == 'F'))) {
        *value = false;
        return true;
    }
    return false;
}

/**
 * @brief Parse a SET/RESET statement and extract a GUC change record.
 *
 * Recognises:
 *  - `SET [SESSION|LOCAL] guc_name = value` (quoted or unquoted)
 *  - `SET [SESSION|LOCAL] guc_name TO value`
 *  - `RESET guc_name` (fills change.value with "")
 *  - `RESET ALL` (sets change.is_reset_all)
 *
 * @param sql        SQL text (not necessarily NUL-terminated).
 * @param sql_len    Byte length of @p sql.
 * @param[out] change  Output GUC change record; set change.valid=true on success.
 * @return true if a tracked GUC change was found; false if unrecognised.
 */
static bool pg_try_extract_stmt_guc_change(const char* sql, size_t sql_len,
                                           pg_stmt_guc_change_t* change)
{
    const char* p = sql;
    const char* end = sql + sql_len;

    memset(change, 0, sizeof(*change));
    change->kind = PG_STMT_GUC_INVALID;

    while (p < end && isspace((unsigned char)*p))
        p++;

    if ((size_t)(end - p) >= 5 && strncasecmp(p, "RESET", 5) == 0) {
        p += 5;
        while (p < end && isspace((unsigned char)*p))
            p++;
        if ((size_t)(end - p) >= 3 && strncasecmp(p, "ALL", 3) == 0) {
            change->valid = true;
            change->is_reset_all = true;
            change->value[0] = '\0';
            return true;
        }

        const char* name = p;
        while (p < end && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        change->kind = pg_stmt_match_guc_name_ci(name, (size_t)(p - name));
        if (change->kind == PG_STMT_GUC_INVALID)
            return false;
        change->valid = true;
        change->value[0] = '\0';
        return true;
    }

    if ((size_t)(end - p) >= 3 && strncasecmp(p, "SET", 3) == 0) {
        p += 3;
        while (p < end && isspace((unsigned char)*p))
            p++;

        if ((size_t)(end - p) >= 5 && strncasecmp(p, "LOCAL", 5) == 0 &&
            (p + 5 == end || isspace((unsigned char)p[5]))) {
            change->is_local = true;
            p += 5;
            while (p < end && isspace((unsigned char)*p))
                p++;
        } else if ((size_t)(end - p) >= 7 && strncasecmp(p, "SESSION", 7) == 0 &&
                   (p + 7 == end || isspace((unsigned char)p[7]))) {
            p += 7;
            while (p < end && isspace((unsigned char)*p))
                p++;
        }

        const char* name = p;
        while (p < end && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        change->kind = pg_stmt_match_guc_name_ci(name, (size_t)(p - name));
        if (change->kind == PG_STMT_GUC_INVALID)
            return false;

        while (p < end && isspace((unsigned char)*p))
            p++;
        if (p < end && *p == '=') {
            p++;
        } else if ((size_t)(end - p) >= 2 && strncasecmp(p, "TO", 2) == 0) {
            p += 2;
        } else {
            return false;
        }

        while (p < end && isspace((unsigned char)*p))
            p++;
        pg_stmt_copy_normalized_fragment(p, end, change->value, sizeof(change->value));
        change->valid = true;
        return true;
    }

    /* Fast reject: set_config contains '_', rare in typical SQL.
     * Skip the expensive word-boundary scan if no underscore present. */
    if (!memchr(sql, '_', sql_len))
        return false;

    const char* call = pg_sql_find_word_ci(sql, sql_len, "set_config");
    if (!call)
        return false;

    p = call + strlen("set_config");
    while (p < end && isspace((unsigned char)*p))
        p++;
    if (p >= end || *p != '(')
        return false;
    p++;
    while (p < end && isspace((unsigned char)*p))
        p++;

    char guc_name[64];
    if (!pg_stmt_parse_single_quoted_literal(&p, end, guc_name, sizeof(guc_name)))
        return false;
    while (p < end && isspace((unsigned char)*p))
        p++;
    if (p >= end || *p != ',')
        return false;
    p++;
    while (p < end && isspace((unsigned char)*p))
        p++;
    if (!pg_stmt_parse_single_quoted_literal(&p, end, change->value, sizeof(change->value)))
        return false;
    while (p < end && isspace((unsigned char)*p))
        p++;
    if (p >= end || *p != ',')
        return false;
    p++;

    const char* bool_begin = p;
    while (p < end && *p != ')' && *p != ';')
        p++;
    bool is_local = false;
    if (!pg_stmt_parse_bool_token(bool_begin, p, &is_local))
        return false;

    change->kind = pg_stmt_match_guc_name_ci(guc_name, strlen(guc_name));
    if (change->kind == PG_STMT_GUC_INVALID)
        return false;

    change->valid = true;
    change->is_local = is_local;
    return true;
}

/**
 * @brief Apply a session-level GUC change (SET name = value).
 *
 * Updates both the effective value and the session-scoped copy, and clears
 * any active transaction-local override for this parameter.
 *
 * @param effective    Effective-value buffer.
 * @param session      Session-level value buffer.
 * @param local        Transaction-local value buffer.
 * @param local_active Transaction-local active flag.
 * @param cap          Buffer capacity in bytes.
 * @param value        New value string (NULL treated as "").
 * @return true if the effective value changed.
 */
static bool pg_stmt_set_guc_session(char* effective,
                                    char* session,
                                    char* local,
                                    bool* local_active,
                                    size_t cap,
                                    const char* value)
{
    bool changed = strcmp(effective, value ? value : "") != 0 ||
                   strcmp(session, value ? value : "") != 0 ||
                   *local_active;
    pg_stmt_copy_value(session, cap, value);
    pg_stmt_copy_value(effective, cap, value);
    if (local)
        local[0] = '\0';
    *local_active = false;
    return changed;
}

/**
 * @brief Apply a transaction-local GUC change (SET LOCAL name = value).
 *
 * Updates the effective value and the local buffer, and sets local_active.
 * The session-level value is unaffected; it will be restored on transaction end.
 *
 * @param effective    Effective-value buffer.
 * @param local        Transaction-local value buffer.
 * @param local_active Transaction-local active flag.
 * @param cap          Buffer capacity in bytes.
 * @param value        New value string (NULL treated as "").
 * @return true if the effective value changed.
 */
static bool pg_stmt_set_guc_local(char* effective,
                                  char* local,
                                  bool* local_active,
                                  size_t cap,
                                  const char* value)
{
    bool changed = strcmp(effective, value ? value : "") != 0 || !*local_active;
    pg_stmt_copy_value(local, cap, value);
    pg_stmt_copy_value(effective, cap, value);
    *local_active = true;
    return changed;
}

/**
 * @brief Restore the effective GUC value to the session-level value.
 *
 * Called at transaction end to roll back SET LOCAL changes.  No-op if
 * local_active is false.  Clears the local buffer and local_active flag.
 *
 * @param effective    Effective-value buffer.
 * @param session      Session-level value buffer.
 * @param local        Transaction-local value buffer.
 * @param local_active Transaction-local active flag.
 * @param cap          Buffer capacity in bytes.
 * @return true if the effective value changed (i.e. a local was active).
 */
static bool pg_stmt_reset_tx_local_guc(char* effective,
                                       char* session,
                                       char* local,
                                       bool* local_active,
                                       size_t cap)
{
    if (!*local_active)
        return false;
    bool changed = strcmp(effective, session) != 0;
    pg_stmt_copy_value(effective, cap, session);
    if (local)
        local[0] = '\0';
    *local_active = false;
    return changed;
}

/**
 * @brief Apply a parsed GUC change to the flow context.
 *
 * Dispatches to pg_stmt_set_guc_session(), pg_stmt_set_guc_local(), or
 * the reset-all handler.  Calls pg_stmt_restamp_context() if any value changed.
 *
 * @param ctx     Flow context.
 * @param change  Parsed GUC change record (must have change->valid == true).
 * @return true if any GUC value changed and the context was restamped.
 */
static bool pg_stmt_apply_guc_change(pg_flow_ctx_t* ctx,
                                     const pg_stmt_guc_change_t* change)
{
    bool changed = false;

    if (!change || !change->valid)
        return false;

    if (change->is_reset_all) {
        for (int kind = 0; kind < PG_STMT_GUC_COUNT; kind++) {
            char* effective = NULL;
            char* session = NULL;
            char* local = NULL;
            bool* local_active = NULL;
            size_t cap = 0;
            if (!pg_stmt_guc_storage(ctx, (pg_stmt_guc_kind_t)kind,
                                     &effective, &session, &local,
                                     &local_active, &cap)) {
                continue;
            }
            changed |= pg_stmt_set_guc_session(effective, session, local,
                                               local_active, cap, "");
        }
    } else {
        char* effective = NULL;
        char* session = NULL;
        char* local = NULL;
        bool* local_active = NULL;
        size_t cap = 0;
        if (!pg_stmt_guc_storage(ctx, change->kind,
                                 &effective, &session, &local,
                                 &local_active, &cap)) {
            return false;
        }

        if (change->is_local) {
            if (ctx->in_transaction) {
                changed = pg_stmt_set_guc_local(effective, local, local_active,
                                                cap, change->value);
            }
        } else {
            changed = pg_stmt_set_guc_session(effective, session, local,
                                              local_active, cap, change->value);
        }
    }

    if (changed)
        pg_stmt_restamp_context(ctx);
    return changed;
}

/**
 * @brief Restore all transaction-local GUC overrides to their session values.
 *
 * Iterates all tracked GUC kinds, calling pg_stmt_reset_tx_local_guc() for
 * each.  Calls pg_stmt_restamp_context() if any value changed.
 * Called when a transaction ends (ReadyForQuery with status 'I').
 *
 * @param ctx  Flow context.
 * @return true if at least one GUC was restored and the context was restamped.
 */
static bool pg_stmt_restore_tx_local_gucs(pg_flow_ctx_t* ctx)
{
    bool changed = false;

    for (int kind = 0; kind < PG_STMT_GUC_COUNT; kind++) {
        char* effective = NULL;
        char* session = NULL;
        char* local = NULL;
        bool* local_active = NULL;
        size_t cap = 0;
        if (!pg_stmt_guc_storage(ctx, (pg_stmt_guc_kind_t)kind,
                                 &effective, &session, &local,
                                 &local_active, &cap)) {
            continue;
        }
        changed |= pg_stmt_reset_tx_local_guc(effective, session, local,
                                              local_active, cap);
    }

    if (changed)
        pg_stmt_restamp_context(ctx);
    return changed;
}

static uint64_t pg_stmt_hash_search_path(const pg_flow_ctx_t* ctx)
{
    return keel_hash_fnv1a_64(ctx->stmt_search_path,
                              strlen(ctx->stmt_search_path));
}

static uint64_t pg_stmt_hash_role(const pg_flow_ctx_t* ctx)
{
    uint64_t h = keel_hash_fnv1a_64(ctx->stmt_role, strlen(ctx->stmt_role));
    h ^= keel_hash_fnv1a_64("|sauth=", 7);
    h ^= keel_hash_fnv1a_64(ctx->stmt_session_auth,
                            strlen(ctx->stmt_session_auth));
    return h;
}

static uint64_t pg_stmt_hash_gucs(const pg_flow_ctx_t* ctx)
{
    uint64_t hash = 0;
    hash ^= keel_hash_fnv1a_64("|timezone=", 10);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_timezone, strlen(ctx->stmt_timezone));
    hash ^= keel_hash_fnv1a_64("|datestyle=", 11);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_datestyle, strlen(ctx->stmt_datestyle));
    hash ^= keel_hash_fnv1a_64("|intervalstyle=", 15);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_intervalstyle,
                               strlen(ctx->stmt_intervalstyle));
    hash ^= keel_hash_fnv1a_64("|standard_conforming_strings=", 30);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_standard_conforming_strings,
                               strlen(ctx->stmt_standard_conforming_strings));
    hash ^= keel_hash_fnv1a_64("|backslash_quote=", 17);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_backslash_quote,
                               strlen(ctx->stmt_backslash_quote));
    hash ^= keel_hash_fnv1a_64("|escape_string_warning=", 24);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_escape_string_warning,
                               strlen(ctx->stmt_escape_string_warning));
    hash ^= keel_hash_fnv1a_64("|default_tablespace=", 20);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_default_tablespace,
                               strlen(ctx->stmt_default_tablespace));
    hash ^= keel_hash_fnv1a_64("|temp_tablespaces=", 18);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_temp_tablespaces,
                               strlen(ctx->stmt_temp_tablespaces));
    hash ^= keel_hash_fnv1a_64("|default_table_access_method=", 29);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_default_table_access_method,
                               strlen(ctx->stmt_default_table_access_method));
    hash ^= keel_hash_fnv1a_64("|row_security=", 14);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_row_security,
                               strlen(ctx->stmt_row_security));
    hash ^= keel_hash_fnv1a_64("|work_mem=", 10);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_work_mem,
                               strlen(ctx->stmt_work_mem));
    hash ^= keel_hash_fnv1a_64("|statement_timeout=", 19);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_statement_timeout,
                               strlen(ctx->stmt_statement_timeout));
    hash ^= keel_hash_fnv1a_64("|lock_timeout=", 14);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_lock_timeout,
                               strlen(ctx->stmt_lock_timeout));
    hash ^= keel_hash_fnv1a_64("|client_encoding=", 17);
    hash ^= keel_hash_fnv1a_64(ctx->stmt_client_encoding,
                               strlen(ctx->stmt_client_encoding));
    return hash;
}

/**
 * @brief Compute a 64-bit FNV-1a signature of all tracked GUC values.
 *
 * The signature is built by XOR-folding `|key=value` strings for all
 * 15 tracked GUC parameters plus role, session_auth, and temp_epoch.
 * Used to detect GUC state changes that affect prepared-statement
 * compatibility between sessions.
 *
 * @param ctx  Flow context (read-only).
 * @return 64-bit context signature.
 */
static uint64_t pg_stmt_context_sig(const pg_flow_ctx_t* ctx)
{
    uint64_t hash = pg_stmt_hash_search_path(ctx);
    hash ^= keel_hash_fnv1a_64("|gucs=", 6);
    hash ^= pg_stmt_hash_gucs(ctx);
    hash ^= keel_hash_fnv1a_64("|role=", 6);
    hash ^= pg_stmt_hash_role(ctx);
    hash ^= keel_hash_fnv1a_64("|sepoch=", 8);
    hash ^= keel_hash_fnv1a_64(&ctx->stmt_schema_epoch,
                               sizeof(ctx->stmt_schema_epoch));
    hash ^= keel_hash_fnv1a_64("|tepoch=", 8);
    hash ^= keel_hash_fnv1a_64(&ctx->stmt_temp_epoch,
                               sizeof(ctx->stmt_temp_epoch));
    return hash;
}

/**
 * @brief Compute the per-entry hash from the statement name and SQL text.
 *
 * The context_sig parameter is accepted for API symmetry but is NOT folded
 * into the per-entry hash (it is applied once at the session level by
 * pg_stmt_recompute_session_hash() to prevent XOR cancellation).
 *
 * @param entry        Prepared-statement cache entry.
 * @param context_sig  Session context signature (unused).
 * @return 64-bit entry hash.
 */
static uint64_t pg_stmt_entry_hash(const pg_stmt_entry_t* entry,
                                   uint64_t context_sig)
{
    /* Entry hash captures the stmt *identity* (name + SQL) only.
     * The session-level context_sig is folded in once by
     * pg_stmt_recompute_session_hash(), not per-entry — this prevents
     * XOR cancellation of the context when an even number of entries
     * is combined via XOR in the session hash. */
    (void)context_sig;
    uint64_t hash = keel_hash_fnv1a_64(entry->name, strlen(entry->name));
    hash ^= keel_hash_fnv1a_64("|", 1);
    hash ^= keel_hash_fnv1a_64(entry->sql, entry->sql_len);
    return hash;
}

/**
 * @brief Recompute the session-level prepared-statement hash.
 *
 * XORs the per-entry hashes of all CONFIRMED prepared statements, then
 * folds in the current context_sig exactly once.  Sessions with identical
 * statement sets and GUC context will produce the same hash, enabling
 * zero-replay backend reuse.
 *
 * When there are no confirmed statements the hash is 0, allowing the
 * engine fast-path to skip replay entirely.
 *
 * @param ctx  Flow context; session_stmt_hash and stmt_context_sig are updated.
 */
static void pg_stmt_recompute_session_hash(pg_flow_ctx_t* ctx)
{
    ctx->session_stmt_hash = 0;
    bool has_entries = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        pg_stmt_entry_t* entry = &ctx->stmt_cache[i];
        if (!entry->valid || !entry->confirmed)
            continue;
        ctx->session_stmt_hash ^= entry->hash;
        has_entries = true;
    }
    /* Include context_sig exactly once so GUC changes always alter the
     * session hash, regardless of how many prepared statements exist.
     * When there are zero entries, keep hash == 0 so the "no stmts"
     * fast path in get_stmt_replay works correctly. */
    if (has_entries)
        ctx->session_stmt_hash ^= ctx->stmt_context_sig;
}

/**
 * @brief Recompute context_sig and update all cache entry hashes.
 *
 * Calls pg_stmt_context_sig() to refresh ctx->stmt_context_sig, then
 * updates every valid cache entry's context_sig and hash.  Finishes by
 * calling pg_stmt_recompute_session_hash().
 *
 * Must be called after any GUC change or temp-epoch bump that could
 * invalidate cached prepared-statement identity.
 *
 * @param ctx  Flow context.
 */
static void pg_stmt_restamp_context(pg_flow_ctx_t* ctx)
{
    ctx->stmt_search_path_hash = pg_stmt_hash_search_path(ctx);
    ctx->stmt_role_hash = pg_stmt_hash_role(ctx);
    ctx->stmt_guc_hash = pg_stmt_hash_gucs(ctx);
    ctx->stmt_context_sig = pg_stmt_context_sig(ctx);
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        pg_stmt_entry_t* entry = &ctx->stmt_cache[i];
        if (!entry->valid)
            continue;
        entry->context_sig = ctx->stmt_context_sig;
        entry->hash = pg_stmt_entry_hash(entry, ctx->stmt_context_sig);
    }
    pg_stmt_recompute_session_hash(ctx);
}

/**
 * @brief Word-boundary case-insensitive substring search.
 *
 * Returns true iff @p word appears in @p sql as a complete word
 * (surrounded by non-alphanumeric characters or start/end of string).
 *
 * @param sql      SQL text.
 * @param sql_len  Byte length of @p sql.
 * @param word     NUL-terminated word to search for.
 * @return true if found; false otherwise.
 */
static bool pg_sql_contains_word_ci(const char* sql, size_t sql_len,
                                    const char* word)
{
    size_t word_len = strlen(word);
    if (word_len == 0 || sql_len < word_len)
        return false;

    for (size_t i = 0; i + word_len <= sql_len; i++) {
        if (i > 0 && isalnum((unsigned char)sql[i - 1]))
            continue;

        size_t j = 0;
        while (j < word_len &&
               tolower((unsigned char)sql[i + j]) ==
               tolower((unsigned char)word[j])) {
            j++;
        }
        if (j != word_len)
            continue;

        if (i + word_len < sql_len &&
            isalnum((unsigned char)sql[i + word_len]))
            continue;

        return true;
    }

    return false;
}

/**
 * @brief Detect whether a SQL statement creates server-side temporary context.
 *
 * Combines the hardpin scanner (KEEL_PIN_TEMP_TABLE) with SQL text heuristics
 * to detect CREATE TEMP TABLE, DISCARD TEMP/ALL, and DROP TABLE on sessions
 * that already have a non-zero temp_epoch.
 *
 * @param ctx        Flow context (used for temp_epoch check), may be NULL.
 * @param sql        SQL text.
 * @param sql_len    Byte length of @p sql.
 * @param query_type Pre-classified query type (0 = unknown).
 * @return true if the statement likely creates or destroys temp context.
 */
static bool pg_stmt_is_temp_context_change(const pg_flow_ctx_t* ctx,
                                           const char* sql, size_t sql_len,
                                           uint32_t query_type)
{
    if (!sql || sql_len == 0)
        return false;

    if (keel_hardpin_scan_postgres(sql, sql_len) & KEEL_PIN_TEMP_TABLE)
        return true;

    if (pg_sql_contains_word_ci(sql, sql_len, "discard") &&
        (pg_sql_contains_word_ci(sql, sql_len, "temp") ||
         pg_sql_contains_word_ci(sql, sql_len, "all"))) {
        return true;
    }

    if (query_type == (uint32_t)KEEL_QUERY_DISCARD &&
        (pg_sql_contains_word_ci(sql, sql_len, "temp") ||
         pg_sql_contains_word_ci(sql, sql_len, "all"))) {
        return true;
    }

    if (ctx && ctx->stmt_temp_epoch != 0 &&
        query_type == (uint32_t)KEEL_QUERY_DROP &&
        pg_sql_contains_word_ci(sql, sql_len, "table")) {
        return true;
    }

    return false;
}

/**
 * @brief Check whether a temp-table creation resets automatically at transaction end.
 *
 * Returns true if the SQL creates a temporary table with
 * `ON COMMIT DROP` or `ON COMMIT DELETE ROWS`, meaning the temp table
 * is gone by the time the backend returns to idle and can be reused.
 *
 * @param sql      SQL text.
 * @param sql_len  Byte length of @p sql.
 * @return true if the temp change self-cleans at transaction end.
 */
static bool pg_stmt_temp_change_resets_on_tx_end(const char* sql, size_t sql_len)
{
    if (!sql || sql_len == 0)
        return false;

    if (!(keel_hardpin_scan_postgres(sql, sql_len) & KEEL_PIN_TEMP_TABLE))
        return false;

    return pg_sql_contains_word_ci(sql, sql_len, "on") &&
           pg_sql_contains_word_ci(sql, sql_len, "commit") &&
           (pg_sql_contains_word_ci(sql, sql_len, "drop") ||
            pg_sql_contains_word_ci(sql, sql_len, "delete rows"));
}

/**
 * @brief Increment the temp-change epoch counter and restamp the context.
 *
 * Called when a statement that creates or destroys session-scoped temporary
 * state executes successfully.  A changed epoch makes the session's
 * prepared-statement hash differ from any backend that has not seen the
 * same temp-state changes, preventing incorrect replay.
 *
 * @param ctx  Flow context.
 */
static void pg_stmt_bump_temp_epoch(pg_flow_ctx_t* ctx)
{
    ctx->stmt_temp_epoch++;
    ctx->metrics_state_changes++;
    pg_stmt_restamp_context(ctx);
}

/**
 * @brief Extract a search_path value from a SET search_path statement.
 *
 * Recognises `SET [SESSION|LOCAL] search_path = value` and
 * `SET [SESSION|LOCAL] search_path TO value` forms.  The extracted
 * value is written to @p out as a NUL-terminated string.
 *
 * @param sql      SQL text.
 * @param sql_len  Byte length of @p sql.
 * @param[out] out     Destination buffer for the extracted value.
 * @param out_len  Capacity of @p out.
 * @return true if a search_path change was extracted; false otherwise.
 */
static bool pg_try_extract_search_path_change(const char* sql, size_t sql_len,
                                              char* out, size_t out_len)
{
    const char* p = sql;
    const char* end = sql + sql_len;

    while (p < end && isspace((unsigned char)*p))
        p++;

    if ((size_t)(end - p) < 3)
        return false;

    if (strncasecmp(p, "RESET", 5) == 0) {
        p += 5;
        while (p < end && isspace((unsigned char)*p))
            p++;
        if ((size_t)(end - p) >= 11 && strncasecmp(p, "search_path", 11) == 0) {
            out[0] = '\0';
            return true;
        }
        if ((size_t)(end - p) >= 3 && strncasecmp(p, "ALL", 3) == 0) {
            out[0] = '\0';
            return true;
        }
        return false;
    }

    if (strncasecmp(p, "SET", 3) != 0)
        return false;
    p += 3;
    while (p < end && isspace((unsigned char)*p))
        p++;
    if ((size_t)(end - p) >= 7 && strncasecmp(p, "SESSION", 7) == 0) {
        p += 7;
        while (p < end && isspace((unsigned char)*p))
            p++;
    }
    if ((size_t)(end - p) < 11 || strncasecmp(p, "search_path", 11) != 0)
        return false;
    p += 11;
    while (p < end && isspace((unsigned char)*p))
        p++;

    if (p < end && *p == '=') {
        p++;
    } else if ((size_t)(end - p) >= 2 && strncasecmp(p, "TO", 2) == 0) {
        p += 2;
    } else {
        return false;
    }

    while (p < end && isspace((unsigned char)*p))
        p++;

    const char* value = p;
    const char* value_end = end;
    while (value_end > value && isspace((unsigned char)value_end[-1]))
        value_end--;
    if (value_end > value && value_end[-1] == ';')
        value_end--;
    while (value_end > value && isspace((unsigned char)value_end[-1]))
        value_end--;

    size_t value_len = (size_t)(value_end - value);
    if (value_len >= out_len)
        value_len = out_len - 1;
    memcpy(out, value, value_len);
    out[value_len] = '\0';
    return true;
}

/**
 * @brief Extract a role or session-authorization change from a SET statement.
 *
 * Recognises:
 *  - `SET [SESSION] ROLE <rolename>`
 *  - `SET SESSION AUTHORIZATION <user>`
 *  - `RESET ROLE` / `RESET SESSION AUTHORIZATION`
 *
 * @param sql                SQL text.
 * @param sql_len            Byte length of @p sql.
 * @param[out] role_out           Buffer for the extracted role name.
 * @param role_out_len       Capacity of @p role_out.
 * @param[out] session_auth_out   Buffer for the extracted session-auth name.
 * @param session_auth_out_len    Capacity of @p session_auth_out.
 * @return true if a role or session-auth change was extracted; false otherwise.
 */
static bool pg_try_extract_role_change(const char* sql, size_t sql_len,
                                       char* role_out, size_t role_out_len,
                                       char* session_auth_out, size_t session_auth_out_len)
{
    const char* p = sql;
    const char* end = sql + sql_len;

    while (p < end && isspace((unsigned char)*p))
        p++;

    if (strncasecmp(p, "RESET ROLE", 10) == 0) {
        role_out[0] = '\0';
        return true;
    }
    if (strncasecmp(p, "RESET SESSION AUTHORIZATION", 27) == 0) {
        session_auth_out[0] = '\0';
        return true;
    }
    if (strncasecmp(p, "SET ROLE", 8) == 0) {
        p += 8;
        while (p < end && isspace((unsigned char)*p))
            p++;
        const char* value = p;
        const char* value_end = end;
        while (value_end > value && isspace((unsigned char)value_end[-1]))
            value_end--;
        if (value_end > value && value_end[-1] == ';')
            value_end--;
        while (value_end > value && isspace((unsigned char)value_end[-1]))
            value_end--;
        size_t value_len = (size_t)(value_end - value);
        if (value_len >= role_out_len)
            value_len = role_out_len - 1;
        memcpy(role_out, value, value_len);
        role_out[value_len] = '\0';
        return true;
    }
    if (strncasecmp(p, "SET SESSION AUTHORIZATION", 25) == 0) {
        p += 25;
        while (p < end && isspace((unsigned char)*p))
            p++;
        const char* value = p;
        const char* value_end = end;
        while (value_end > value && isspace((unsigned char)value_end[-1]))
            value_end--;
        if (value_end > value && value_end[-1] == ';')
            value_end--;
        while (value_end > value && isspace((unsigned char)value_end[-1]))
            value_end--;
        size_t value_len = (size_t)(value_end - value);
        if (value_len >= session_auth_out_len)
            value_len = session_auth_out_len - 1;
        memcpy(session_auth_out, value, value_len);
        session_auth_out[value_len] = '\0';
        return true;
    }

    return false;
}

/**
 * @brief Parse a `PREPARE name AS body` SQL statement.
 *
 * Extracts stmt_name and body_sql from a Simple Query PREPARE clause.
 * Writes NUL-terminated strings into the caller-supplied buffers.
 *
 * @param sql           Input SQL text (not required to be NUL-terminated).
 * @param sql_len       Length of @p sql in bytes.
 * @param[out] name_out Buffer to receive the statement name (≥64 bytes).
 * @param name_buf      Capacity of @p name_out.
 * @param[out] body_out Buffer to receive the statement body (≥2048 bytes).
 * @param body_buf      Capacity of @p body_out.
 * @param[out] body_len_out Set to body length on success.
 * @return true on success; false if text is not a recognisable PREPARE form.
 */
static bool tracking_parse_prepare(const char* sql, size_t sql_len,
                                   char*  name_out,  size_t name_buf,
                                   char*  body_out,  size_t body_buf,
                                   size_t* body_len_out)
{
    /* Skip leading whitespace */
    const char* p   = sql;
    const char* end = sql + sql_len;
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;

    /* Match "PREPARE" keyword (case-insensitive) */
    if ((size_t)(end - p) < 8) return false;
    if (strncasecmp(p, "PREPARE", 7) != 0) return false;
    p += 7;
    if (p >= end || !(*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
        return false;
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;

    /* Read statement name: identifier chars (letters, digits, underscore, $) */
    const char* name_start = p;
    /* Allow quoted identifiers: "name" */
    if (p < end && *p == '"') {
        p++;  /* skip opening quote */
        const char* qs = p;
        while (p < end && *p != '"') p++;
        if (p >= end) return false;
        size_t nl = (size_t)(p - qs);
        if (nl == 0 || nl >= name_buf) return false;
        memcpy(name_out, qs, nl);
        name_out[nl] = '\0';
        p++;  /* skip closing quote */
    } else {
        while (p < end && (isalnum((unsigned char)*p) || *p == '_' || *p == '$'))
            p++;
        size_t nl = (size_t)(p - name_start);
        if (nl == 0 || nl >= name_buf) return false;
        memcpy(name_out, name_start, nl);
        name_out[nl] = '\0';
    }

    /* Skip optional parameter-type list: ( type1, type2 ) */
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (p < end && *p == '(') {
        int depth = 1; p++;
        while (p < end && depth > 0) {
            if (*p == '(') depth++;
            else if (*p == ')') depth--;
            p++;
        }
    }

    /* Match "AS" keyword */
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if ((size_t)(end - p) < 2) return false;
    if (strncasecmp(p, "AS", 2) != 0) return false;
    p += 2;
    if (p < end && (isalnum((unsigned char)*p) || *p == '_')) return false; /* not a keyword boundary */
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;

    /* Rest is the body SQL */
    size_t bl = (size_t)(end - p);
    if (bl == 0) return false;
    if (bl >= body_buf) bl = body_buf - 1;
    memcpy(body_out, p, bl);
    body_out[bl] = '\0';
    *body_len_out = bl;
    return true;
}

/**
 * @brief Parse a `DEALLOCATE [PREPARE] name` statement and extract the name.
 *
 * Accepts:
 *  - `DEALLOCATE name`
 *  - `DEALLOCATE PREPARE name`
 *  - `DEALLOCATE ALL` / `DEALLOCATE PREPARE ALL`  → name_out = "ALL"
 *
 * @param sql       Input SQL text (not required to be NUL-terminated).
 * @param sql_len   Length of @p sql in bytes.
 * @param name_out  Buffer to receive the extracted name (≥64 bytes recommended).
 * @param name_buf  Capacity of @p name_out.
 * @return true on success; false if text is not a recognisable DEALLOCATE form.
 */
static bool tracking_parse_deallocate(const char* sql, size_t sql_len,
                                       char* name_out, size_t name_buf)
{
    const char* p   = sql;
    const char* end = sql + sql_len;

    /* Skip leading whitespace */
    while (p < end && isspace((unsigned char)*p)) p++;

    /* Match "DEALLOCATE" */
    if ((size_t)(end - p) < 10) return false;
    if (strncasecmp(p, "DEALLOCATE", 10) != 0) return false;
    p += 10;
    if (p < end && isalnum((unsigned char)*p)) return false;
    while (p < end && isspace((unsigned char)*p)) p++;

    /* Skip optional "PREPARE" keyword */
    if ((size_t)(end - p) >= 7 &&
        strncasecmp(p, "PREPARE", 7) == 0 &&
        (p + 7 >= end || !isalnum((unsigned char)p[7]))) {
        p += 7;
        while (p < end && isspace((unsigned char)*p)) p++;
    }

    /* Read the statement name (identifier or "ALL") */
    const char* name_start = p;
    while (p < end && (isalnum((unsigned char)*p) || *p == '_' || *p == '$'))
        p++;

    size_t nl = (size_t)(p - name_start);
    if (nl == 0 || nl >= name_buf) return false;
    memcpy(name_out, name_start, nl);
    name_out[nl] = '\0';
    return true;
}

/* ---- Anonymous mode: stmt_name → SQL map ----
 *
 * The anonymous-statement map holds the (stmt_name → sql_text) pairs
 * intercepted from extended-protocol Parse messages.  A fixed-size open-
 * addressing table with linear probing is used.  Collisions on the same
 * name overwrite the existing entry (PREPARE is idempotent by spec).
 *
 * Map capacity matches PG_STMT_CACHE_SIZE so both structures stay in sync.
 * (pg_anon_entry_t and PG_ANON_MAP_SIZE are defined in postgres_flow_internal.h.)
 */

/* ---- Anonymous map helpers ---- */

/** Lazily allocate the anon_map on first use (~271 KB). */
static pg_anon_entry_t* pg_anon_ensure(pg_flow_ctx_t* ctx) {
    if (ctx->anon_map) return ctx->anon_map;
    ctx->anon_map = keel_calloc(PG_ANON_MAP_SIZE, sizeof(pg_anon_entry_t));
    return ctx->anon_map;  /* NULL on OOM */
}

/** Find an entry by name.  Returns NULL if not found or map not allocated. */
static pg_anon_entry_t* pg_anon_find(pg_anon_entry_t* map, const char* name) {
    if (!map) return NULL;
    uint64_t h = keel_hash_fnv1a_64(name, strlen(name));
    uint32_t idx = (uint32_t)(h % PG_ANON_MAP_SIZE);
    for (uint32_t i = 0; i < PG_ANON_MAP_SIZE; i++) {
        uint32_t slot = (idx + i) % PG_ANON_MAP_SIZE;
        if (map[slot].name[0] == '\0') return NULL;
        if (strcmp(map[slot].name, name) == 0) return &map[slot];
    }
    return NULL;
}

/** Insert or update an entry.  Drops the entry if the map is full. */
static void pg_anon_upsert(pg_anon_entry_t* map, const char* name,
                           const char* sql, size_t sql_len) {
    uint64_t h = keel_hash_fnv1a_64(name, strlen(name));
    uint32_t idx = (uint32_t)(h % PG_ANON_MAP_SIZE);
    for (uint32_t i = 0; i < PG_ANON_MAP_SIZE; i++) {
        uint32_t slot = (idx + i) % PG_ANON_MAP_SIZE;
        if (map[slot].name[0] == '\0' || strcmp(map[slot].name, name) == 0) {
            strncpy(map[slot].name, name, sizeof(map[slot].name) - 1);
            size_t copy = sql_len < sizeof(map[slot].sql) - 1
                        ? sql_len : sizeof(map[slot].sql) - 1;
            memcpy(map[slot].sql, sql, copy);
            map[slot].sql[copy] = '\0';
            map[slot].sql_len   = copy;
            return;
        }
    }
    /* Map full: silently overwrite the home slot */
    strncpy(map[idx].name, name, sizeof(map[idx].name) - 1);
    size_t copy = sql_len < sizeof(map[idx].sql) - 1
                ? sql_len : sizeof(map[idx].sql) - 1;
    memcpy(map[idx].sql, sql, copy);
    map[idx].sql[copy] = '\0';
    map[idx].sql_len   = copy;
}

/** Remove an entry by name (DEALLOCATE). */
static void pg_anon_remove(pg_anon_entry_t* map, const char* name) {
    if (!map) return;
    uint64_t h = keel_hash_fnv1a_64(name, strlen(name));
    uint32_t idx = (uint32_t)(h % PG_ANON_MAP_SIZE);
    for (uint32_t i = 0; i < PG_ANON_MAP_SIZE; i++) {
        uint32_t slot = (idx + i) % PG_ANON_MAP_SIZE;
        if (map[slot].name[0] == '\0') return;
        if (strcmp(map[slot].name, name) == 0) {
            /* Shift subsequent entries to maintain probing invariant */
            uint32_t hole = slot;
            for (uint32_t j = 1; j < PG_ANON_MAP_SIZE; j++) {
                uint32_t next = (slot + j) % PG_ANON_MAP_SIZE;
                if (map[next].name[0] == '\0') break;
                uint32_t natural = (uint32_t)(
                    keel_hash_fnv1a_64(map[next].name, strlen(map[next].name))
                    % PG_ANON_MAP_SIZE);
                /* Re-home if natural slot ≤ hole (mod wrap handled) */
                bool displaced = (natural != next);
                (void)displaced;
                map[hole] = map[next];
                memset(&map[next], 0, sizeof(map[next]));
                hole = next;
            }
            memset(&map[hole], 0, sizeof(map[hole]));
            return;
        }
    }
}

/**
 * @brief Build the PG protocol handshake response for the client.
 *
 * Writes AuthenticationOk, ParameterStatus messages for standard server
 * parameters, BackendKeyData (using the per-session cancel key), and
 * ReadyForQuery into ctx->handshake_buf.
 *
 * @param ctx  Flow context whose handshake_buf and handshake_len are populated.
 */
/* ============================================================================
 * Auth helpers — build PostgreSQL wire messages for client auth exchange
 * ============================================================================ */

/**
 * @brief Write a single PostgreSQL AuthenticationRequest message into
 *        handshake_buf.
 *
 * @param ctx       Flow context whose handshake_buf receives the packet.
 * @param auth_msg  Provider message: first 4 bytes are int32(auth_type) in
 *                  big-endian, remaining bytes are the challenge payload.
 * @param auth_msg_len  Total byte count of @p auth_msg.
 */
static void build_auth_challenge(pg_flow_ctx_t* ctx,
                                 const void* auth_msg, size_t auth_msg_len)
{
    uint8_t* p = ctx->handshake_buf;
    *p++ = 'R';
    wr32(p, (uint32_t)(4 + auth_msg_len)); p += 4;
    if (auth_msg && auth_msg_len > 0)
        memcpy(p, auth_msg, auth_msg_len);
    ctx->handshake_len = (size_t)(1 + 4 + auth_msg_len);
}

/**
 * @brief Write an auth success packet into handshake_buf.
 *
 * Optionally prepends a server-final message (SCRAM AuthenticationSASLFinal,
 * type 12) before the standard AuthenticationOk + ParameterStatus chain +
 * BackendKeyData + ReadyForQuery.
 *
 * @param ctx        Flow context.
 * @param final_msg  Provider final message (may be NULL).
 * @param final_len  Length of @p final_msg.
 */
static void build_auth_success_response(pg_flow_ctx_t* ctx,
                                        const void* final_msg, size_t final_len)
{
    uint8_t* p = ctx->handshake_buf;

    /* Optional server-final message (e.g. SCRAM ServerFinal, type 12) */
    if (final_msg && final_len > 0) {
        *p++ = 'R';
        wr32(p, (uint32_t)(4 + final_len)); p += 4;
        memcpy(p, final_msg, final_len); p += final_len;
    }

    /* AuthenticationOk */
    *p++ = 'R'; wr32(p, 8); p += 4; wr32(p, 0); p += 4;

    /* ParameterStatus messages */
    static const struct { const char* k; const char* v; } ps[] = {
        {"server_version","16.0"}, {"server_encoding","UTF8"},
        {"client_encoding","UTF8"}, {"DateStyle","ISO, MDY"},
        {"integer_datetimes","on"}, {"is_superuser","on"},
        {"standard_conforming_strings","on"}, {"TimeZone","UTC"},
    };
    for (size_t i = 0; i < sizeof(ps)/sizeof(ps[0]); i++) {
        size_t kl = strlen(ps[i].k)+1, vl = strlen(ps[i].v)+1;
        *p++ = 'S'; wr32(p,(uint32_t)(4+kl+vl)); p+=4;
        memcpy(p,ps[i].k,kl); p+=kl;
        memcpy(p,ps[i].v,vl); p+=vl;
    }
    /* BackendKeyData */
    *p++='K'; wr32(p,12); p+=4;
    wr32(p,ctx->backend_pid); p+=4;
    wr32(p,ctx->backend_secret); p+=4;
    /* ReadyForQuery */
    *p++='Z'; wr32(p,5); p+=4; *p++='I';

    ctx->handshake_len = (size_t)(p - ctx->handshake_buf);
}

/**
 * @brief Write a PostgreSQL ErrorResponse for an authentication failure into
 *        handshake_buf.
 *
 * Uses SQLSTATE 28P01 (invalid_password) — standard for auth failures.
 *
 * @param ctx       Flow context.
 * @param username  Username being authenticated (for the error message).
 */
static void build_auth_error(pg_flow_ctx_t* ctx, const char* username)
{
    char errmsg[256];
    snprintf(errmsg, sizeof(errmsg),
             "password authentication failed for user \"%s\"",
             username ? username : "unknown");

    uint8_t* p  = ctx->handshake_buf;
    *p++ = 'E';
    uint8_t* lenp = p; p += 4;          /* length placeholder */
    *p++ = 'S'; memcpy(p,"FATAL\0",6);  p+=6;
    *p++ = 'V'; memcpy(p,"FATAL\0",6);  p+=6;
    *p++ = 'C'; memcpy(p,"28P01\0",6);  p+=6;
    *p++ = 'M';
    size_t ml = strlen(errmsg)+1;
    memcpy(p, errmsg, ml); p+=ml;
    *p++ = 0;                           /* message terminator */

    wr32(lenp, (uint32_t)(p - lenp));
    ctx->handshake_len = (size_t)(p - ctx->handshake_buf);
}

static void build_handshake_response(pg_flow_ctx_t* ctx) {
    uint8_t* p = ctx->handshake_buf;
    /* AuthenticationOk */
    *p++ = 'R'; wr32(p,8); p+=4; wr32(p,0); p+=4;
    /* ParameterStatus */
    static const struct { const char* k; const char* v; } ps[] = {
        {"server_version","16.0"}, {"server_encoding","UTF8"},
        {"client_encoding","UTF8"}, {"DateStyle","ISO, MDY"},
        {"integer_datetimes","on"}, {"is_superuser","on"},
        {"standard_conforming_strings","on"}, {"TimeZone","UTC"},
    };
    for (size_t i = 0; i < sizeof(ps)/sizeof(ps[0]); i++) {
        size_t kl = strlen(ps[i].k)+1, vl = strlen(ps[i].v)+1;
        *p++ = 'S'; wr32(p,(uint32_t)(4+kl+vl)); p+=4;
        memcpy(p,ps[i].k,kl); p+=kl;
        memcpy(p,ps[i].v,vl); p+=vl;
    }
    /* BackendKeyData — use the per-session cancel key set by pgf_create() */
    *p++='K'; wr32(p,12); p+=4; wr32(p,ctx->backend_pid); p+=4; wr32(p,ctx->backend_secret); p+=4;
    /* ReadyForQuery */
    *p++='Z'; wr32(p,5); p+=4; *p++='I';
    ctx->handshake_len = (size_t)(p - ctx->handshake_buf);
}

/**
 * @brief Fast case-insensitive word search without full hardpin scan.
 *
 * Checks whether @p needle appears anywhere in @p haystack as a case-
 * insensitive substring.  No word-boundary check — use pg_sql_find_word_ci()
 * if boundary semantics are required.
 *
 * @param haystack  Text to search.
 * @param hlen      Length of @p haystack.
 * @param needle    Substring to find.
 * @param nlen      Length of @p needle.
 * @return true if found; false otherwise.
 */
static bool ci_contains_word_fast(const char* haystack, size_t hlen,
                                  const char* needle, size_t nlen)
{
    if (nlen == 0 || nlen > hlen) return false;
    for (size_t i = 0; i <= hlen - nlen; i++) {
        if (tolower((unsigned char)haystack[i]) !=
            tolower((unsigned char)needle[0])) continue;
        bool match = true;
        for (size_t j = 1; j < nlen; j++) {
            if (tolower((unsigned char)haystack[i + j]) !=
                tolower((unsigned char)needle[j])) {
                match = false; break;
            }
        }
        if (!match) continue;
        bool left_ok  = (i == 0) || !isalnum((unsigned char)haystack[i - 1]);
        bool right_ok = (i + nlen >= hlen) ||
                        !isalnum((unsigned char)haystack[i + nlen]);
        if (left_ok && right_ok) return true;
    }
    return false;
}

/**
 * @brief Classify a SQL statement and determine routing and pin instructions.
 *
 * Uses the keel_sql API to detect transactions, prepared statements, and
 * special statements, then applies PG-specific heuristics (temp tables,
 * COPY, LOCK, CURSOR, DISCARD) to set effect flags, routing preference,
 * and pin-set/pin-clear reasons.
 *
 * @param sql_text      SQL text.
 * @param sql_len       Byte length of @p sql_text.
 * @param[out] eff      Query effect flags (read/write/transaction/etc.).
 * @param[out] route    Routing preference (primary/replica/any).
 * @param[out] pin_set  Reason to pin the session to a backend.
 * @param[out] pin_clr  Reason to clear an existing pin.
 * @param[out] out_query_type  Numeric query type ID.
 */
static void classify_sql(const char* sql_text, size_t sql_len,
                         keel_query_effect_flags_t* eff,
                         keel_flow_route_t* route,
                         keel_flow_pin_reason_t* pin_set,
                         keel_flow_pin_reason_t* pin_clr,
                         uint32_t* out_query_type) {
    *eff = KEEL_QE_NONE;
    *route = KEEL_FROUTE_PRIMARY;
    *pin_set = KEEL_FPIN_NONE;
    *pin_clr = KEEL_FPIN_NONE;
    if (out_query_type) *out_query_type = 0;
    if (!sql_text || sql_len == 0) return;

    keel_str_t sql = { .data = sql_text, .len = sql_len };
    keel_proto_query_t qr;
    memset(&qr, 0, sizeof(qr));
    keel_sql_analyze(sql, &qr);
    if (out_query_type) *out_query_type = (uint32_t)qr.type;

    switch (qr.type) {
    case KEEL_QUERY_SELECT: case KEEL_QUERY_SHOW: case KEEL_QUERY_EXPLAIN:
        *eff |= KEEL_QE_READONLY;
        *route = KEEL_FROUTE_REPLICA;
        break;
    case KEEL_QUERY_INSERT: case KEEL_QUERY_UPDATE:
    case KEEL_QUERY_DELETE: case KEEL_QUERY_TRUNCATE:
        *eff |= KEEL_QE_WRITE; break;
    case KEEL_QUERY_CREATE: case KEEL_QUERY_ALTER: case KEEL_QUERY_DROP:
        *eff |= KEEL_QE_DDL | KEEL_QE_WRITE; break;
    case KEEL_QUERY_BEGIN:
        *eff |= KEEL_QE_BEGINS_TX; *pin_set |= KEEL_FPIN_TRANSACTION;
        /* BEGIN READ ONLY / START TRANSACTION READ ONLY → replica pool.
         * Scan for "READ" followed by "ONLY" anywhere after the opening keyword.
         * Uses a short linear scan; sql_len is small for DDL/control statements. */
        for (size_t _i = 5; _i + 9 <= sql_len; _i++) {
            if ((sql_text[_i] == 'r' || sql_text[_i] == 'R') &&
                strncasecmp(sql_text + _i, "READ", 4) == 0) {
                size_t _j = _i + 4;
                while (_j < sql_len && (sql_text[_j] == ' ' ||
                       sql_text[_j] == '\t' || sql_text[_j] == '\n')) _j++;
                if (_j + 4 <= sql_len &&
                    strncasecmp(sql_text + _j, "ONLY", 4) == 0) {
                    *route  = KEEL_FROUTE_REPLICA;
                    *eff   |= KEEL_QE_READONLY;   /* transaction cannot write */
                    break;
                }
            }
        }
        break;
    case KEEL_QUERY_COMMIT: case KEEL_QUERY_ROLLBACK:
        *eff |= KEEL_QE_ENDS_TX; *pin_clr |= KEEL_FPIN_TRANSACTION; break;
    case KEEL_QUERY_SET:
        *eff |= KEEL_QE_SETS_STATE; break;
    case KEEL_QUERY_RESET: case KEEL_QUERY_DISCARD:
        *eff |= KEEL_QE_SETS_STATE;
        *pin_clr |= KEEL_FPIN_PREPARED_STMT;  /* DISCARD ALL / RESET ALL wipes stmts */
        break;
    case KEEL_QUERY_DEALLOCATE:
        *pin_clr |= KEEL_FPIN_PREPARED_STMT; break;
    case KEEL_QUERY_PREPARE:
        *pin_set |= KEEL_FPIN_PREPARED_STMT; *eff |= KEEL_QE_HARD_PIN; break;

    /* UNLISTEN — releases the LISTEN hard-pin once the backend confirms.
     * The quarantine heuristic below will NOT add KEEL_PIN_LISTEN for
     * UNLISTEN (hardpin_scan_postgres no longer sets it for UNLISTEN). */
    case KEEL_QUERY_UNLISTEN:
        *pin_clr |= KEEL_FPIN_LISTEN; break;

    /* MERGE / UPSERT is DML — treat like INSERT/UPDATE/DELETE */
    case KEEL_QUERY_MERGE:
        *eff |= KEEL_QE_WRITE; break;

    /* VACUUM / REINDEX / CLUSTER — maintenance writes, no session state */
    case KEEL_QUERY_MAINTENANCE:
        *eff |= KEEL_QE_WRITE; break;

    /* DO $$ ... $$ and CALL — arbitrary procedural code may create temp
     * tables, change session variables, acquire advisory locks, etc.
     * Mark as unmodellable so the engine forces DISCARD ALL on return. */
    case KEEL_QUERY_DO:
    case KEEL_QUERY_CALL:
        *eff |= KEEL_QE_WRITE | KEEL_QE_UNKNOWN_STATE; break;

    default: break;
    }

    /* Conservative quarantine heuristic: if the statement text contains
     * keywords that create server-side session state, mark it as
     * POTENTIALLY_STATEFUL.  The engine will hold the backend in quarantine
     * (pinned) until the backend response confirms no persistent state was
     * created.  False positives are safe (over-pin); false negatives would
     * leak state.
     *
     * Fast-path: SELECT/SHOW/EXPLAIN cannot create session state except
     * for advisory locks in SELECT.  Skip the full hardpin scan for these
     * query types — saves ~10 ci_contains_word calls per query. */
    {
        keel_pin_reason_t hp = KEEL_PIN_NONE;
        if (qr.type == KEEL_QUERY_SELECT) {
            /* SELECT can contain pg_advisory_lock() — check only that */
            if (ci_contains_word_fast(sql_text, sql_len,
                                      "pg_advisory_lock", 16)) {
                hp |= KEEL_PIN_ADVISORY_LOCK;
            }
        } else if (qr.type != KEEL_QUERY_SHOW &&
                   qr.type != KEEL_QUERY_EXPLAIN) {
            hp = keel_hardpin_scan_postgres(sql_text, sql_len);
        }
        if (hp & (KEEL_PIN_TEMP_TABLE | KEEL_PIN_CURSOR | KEEL_PIN_LISTEN |
                  KEEL_PIN_ADVISORY_LOCK | KEEL_PIN_SET_ROLE |
                  KEEL_PIN_PREPARED_STMT | KEEL_PIN_COPY | KEEL_PIN_OSC)) {
            *eff |= KEEL_QE_POTENTIALLY_STATEFUL;
            *pin_set |= KEEL_FPIN_QUARANTINE;
        }
        /* Translate detected hard-pin reasons to explicit flow pins so the
         * quarantine resolution can distinguish SSV-virtualizable state
         * (PREPARED_STMT) from non-virtualizable state (TEMP_TABLE, CURSOR,
         * LISTEN, ADVISORY_LOCK).  Without this, QUARANTINE is the only
         * pin for temp tables, and the SSV quarantine resolution would
         * incorrectly clear it when PREPARED_STMT is also present. */
        if (hp & KEEL_PIN_TEMP_TABLE)
            *pin_set |= KEEL_FPIN_TEMP_TABLE;
        if (hp & KEEL_PIN_CURSOR)
            *pin_set |= KEEL_FPIN_CURSOR;
        if (hp & KEEL_PIN_LISTEN)
            *pin_set |= KEEL_FPIN_LISTEN;
        if (hp & KEEL_PIN_ADVISORY_LOCK)
            *pin_set |= KEEL_FPIN_ADVISORY_LOCK;
        /* OSC session: force primary routing + exclusive backend affinity */
        if (hp & KEEL_PIN_OSC) {
            *pin_set |= KEEL_FPIN_OSC;
            *route    = KEEL_FROUTE_PRIMARY;
            *eff     |= KEEL_QE_HARD_PIN;
        }
    }

    /* Fast pre-check: only invoke the full lexer-based statement counter
     * when the SQL actually contains a semicolon. */
    if (memchr(sql_text, ';', sql_len) &&
        keel_sql_count_statements(sql) > 1) {
        *eff |= KEEL_QE_MULTI_STMT;
        if (keel_sql_contains_transaction_start(sql))
            *pin_set |= KEEL_FPIN_TRANSACTION;
    }
}

/* Replication tracking (spec §TXN-TRACK).
 * When transaction_tracking = on, a simple COMMIT query is rewritten to
 *   SELECT txid_current() AS _keel_txid; COMMIT;
 * so the engine can record the PostgreSQL XID before the COMMIT executes.
 * If the backend dies between writing COMMIT and receiving the response,
 * the engine can use txid_status(xid) on the new primary to determine the
 * outcome.
 *
 * Wire layout (50 bytes):
 *   'Q'                           1
 *   0x00 0x00 0x00 0x31           4  (length = 4 + 45 = 49)
 *   "SELECT txid_current() AS _keel_txid; COMMIT;\0"  45
 */
static const uint8_t kPgXidCommitMsg[] = {
    'Q', 0x00, 0x00, 0x00, 0x31,
    'S','E','L','E','C','T',' ',
    't','x','i','d','_','c','u','r','r','e','n','t','(',')',' ',
    'A','S',' ','_','k','e','e','l','_','t','x','i','d',';',' ',
    'C','O','M','M','I','T',';','\0'
};

/**
 * @brief Create a PostgreSQL protocol flow context for a new session.
 *
 * Allocates and zero-initialises a pg_flow_ctx_t, sets the initial
 * transaction status to 'I' (idle), and generates a random cancel key
 * for the BackendKeyData handshake message.  Returns NULL on allocation
 * failure.
 *
 * @param s  Session slot for this connection.
 * @return Opaque pointer to the new context, or NULL on failure.
 */
static void* pgf_create(keel_session_t* s) {
    pg_flow_ctx_t* ctx = keel_calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    ctx->txn_status = 'I';
    ctx->stmt_search_path[0] = '\0';
    ctx->stmt_search_path_session[0] = '\0';
    ctx->stmt_search_path_local[0] = '\0';
    ctx->stmt_timezone[0] = '\0';
    ctx->stmt_timezone_session[0] = '\0';
    ctx->stmt_timezone_local[0] = '\0';
    ctx->stmt_datestyle[0] = '\0';
    ctx->stmt_datestyle_session[0] = '\0';
    ctx->stmt_datestyle_local[0] = '\0';
    ctx->stmt_intervalstyle[0] = '\0';
    ctx->stmt_intervalstyle_session[0] = '\0';
    ctx->stmt_intervalstyle_local[0] = '\0';
    ctx->stmt_standard_conforming_strings[0] = '\0';
    ctx->stmt_standard_conforming_strings_session[0] = '\0';
    ctx->stmt_standard_conforming_strings_local[0] = '\0';
    ctx->stmt_backslash_quote[0] = '\0';
    ctx->stmt_backslash_quote_session[0] = '\0';
    ctx->stmt_backslash_quote_local[0] = '\0';
    ctx->stmt_escape_string_warning[0] = '\0';
    ctx->stmt_escape_string_warning_session[0] = '\0';
    ctx->stmt_escape_string_warning_local[0] = '\0';
    ctx->stmt_default_tablespace[0] = '\0';
    ctx->stmt_default_tablespace_session[0] = '\0';
    ctx->stmt_default_tablespace_local[0] = '\0';
    ctx->stmt_temp_tablespaces[0] = '\0';
    ctx->stmt_temp_tablespaces_session[0] = '\0';
    ctx->stmt_temp_tablespaces_local[0] = '\0';
    ctx->stmt_default_table_access_method[0] = '\0';
    ctx->stmt_default_table_access_method_session[0] = '\0';
    ctx->stmt_default_table_access_method_local[0] = '\0';
    ctx->stmt_row_security[0] = '\0';
    ctx->stmt_row_security_session[0] = '\0';
    ctx->stmt_row_security_local[0] = '\0';
    ctx->stmt_role[0] = '\0';
    ctx->stmt_session_auth[0] = '\0';
    ctx->stmt_temp_epoch = 0;
    ctx->stmt_schema_epoch = 0;
    ctx->stmt_semantic_unknown = false;
    ctx->stmt_temp_tx_reset_pending = false;
    ctx->stmt_temp_tx_rollback_reset_pending = false;
    ctx->stmt_last_tx_end_was_rollback = false;
    ctx->stmt_context_sig = pg_stmt_context_sig(ctx);
    /* Inherit PS pooling mode from the worker config */
    if (s && s->worker)
        ctx->ps_mode = s->worker->ps_mode;
    /* Inherit replication uncertainty tracking */
    if (s && s->worker)
        ctx->txn_tracking = s->worker->txn_tracking;

    /* Generate unique per-session cancel key for cancel forwarding.
     * Encode worker_id (high 16 bits) + slab_index (low 16 bits) in the
     * synthetic PID so the cancel handler can route back to this session
     * without a global hash map.  The secret is random for security. */
    if (s && s->worker) {
        uint32_t cpid = ((uint32_t)s->worker->id << 16) | (s->slab_index & 0xFFFF);
        uint32_t csec = (uint32_t)((uintptr_t)s ^ 0x5A5A5A5A) ^ (uint32_t)s->id;
        ctx->backend_pid    = cpid;
        ctx->backend_secret = csec;
        s->cancel_pid    = cpid;
        s->cancel_secret = csec;

        /* Copy the worker's auth manager pointer (non-owning).
         * NULL means trust mode — no challenge will be issued. */
        ctx->auth_manager = s->worker->auth_manager;
    }

    return ctx;
}
/**
 * @brief Destroy a PostgreSQL protocol context created by pgf_create().
 *
 * Frees all heap memory owned by the context:
 *  - `wire_msg` buffers in `stmt_cache[]` — raw Parse wire messages for
 *    prepared-statement replay (PS virtualization spec §17).
 *  - `anon_rewrite_buf` — scratch buffer used by pgf_rewrite_execute_anonymous()
 *    to rewrite Execute(name) → Execute("") for anonymous-mode backends.
 * Finally frees the context slab itself.
 *
 * Safe to call with v==NULL.
 */
static void pgf_destroy(void* v) {
    if (!v) return;
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)v;
    /* Free heap-allocated wire messages for all cached prepared statements */
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (ctx->stmt_cache[i].wire_msg) {
            keel_free(ctx->stmt_cache[i].wire_msg);
            ctx->stmt_cache[i].wire_msg = NULL;
        }
    }
    /* Free tracking-mode pending PREPARE rollback wire_msg (if any) */
    if (ctx->pending_track_prior.wire_msg) {
        keel_free(ctx->pending_track_prior.wire_msg);
        ctx->pending_track_prior.wire_msg = NULL;
    }
    /* Free anonymous mode rewrite buffer */
    if (ctx->anon_rewrite_buf) {
        keel_free(ctx->anon_rewrite_buf);
        ctx->anon_rewrite_buf = NULL;
    }
    if (ctx->stmt_discard_plans_rewrite_buf) {
        keel_free(ctx->stmt_discard_plans_rewrite_buf);
        ctx->stmt_discard_plans_rewrite_buf = NULL;
    }
    /* Free anonymous mode map */
    if (ctx->anon_map) {
        keel_free(ctx->anon_map);
        ctx->anon_map = NULL;
    }
    /* Free any in-progress auth context (e.g. connection dropped mid-auth) */
    if (ctx->auth_ctx) {
        keel_auth_context_free(ctx->auth_ctx);
        ctx->auth_ctx = NULL;
    }
    keel_free(v);
}

/* ---- vtable: frame_len ---- */

/**
 * @brief Return the total byte length of the next protocol message in buf.
 *
 * PostgreSQL uses two distinct wire framing formats:
 *
 *  **Pre-startup** (startup packet, SSL request, cancel request):
 *    First 4 bytes are a big-endian uint32 giving the total message length
 *    INCLUDING the 4-byte length field itself.  There is no type byte.
 *    This format is used for the very first message on a new connection.
 *
 *  **Post-startup** (all normal messages):
 *    Byte 0   = message type tag (1 byte)
 *    Bytes 1-4 = big-endian uint32 body length INCLUDING the 4-byte length
 *               field itself but NOT the type byte.
 *    Total wire length = 1 + body_length.
 *
 * The `dir` parameter is 0 for frontend-to-backend (FE) messages and 1 for
 * backend-to-frontend (BE) messages.  Backend messages always use post-startup
 * framing.
 *
 * @return Number of bytes in the complete message (≥ 5 for normal,
 *         ≥ 8 for startup), 0 if more bytes are needed to determine
 *         the length, -1 if the message is malformed or impossibly large.
 */
static ssize_t pgf_frame_len(void* vctx, const uint8_t* data, size_t len, int dir) {
    pg_flow_ctx_t* ctx = vctx;
    if (!ctx) {
        if (dir == 0 && len < 4)
            return 0;
        if (len < 5)
            return 0;
        uint32_t bl = rd32(data + 1);
        if (bl < 4 || bl > 1073741824)
            return -1;
        return (ssize_t)(1 + bl);
    }
    if (!ctx->startup_complete && dir == 0) {
        /* During auth challenge phase (auth_pending=true), the client sends
         * 'p' PasswordMessage / SASLInitialResponse / SASLResponse, which use
         * post-startup framing (type byte + length).  Only the very first
         * message on a fresh connection uses pre-startup framing. */
        if (ctx->auth_pending) {
            if (len < 5) return 0;
            uint32_t bl = rd32(data + 1);
            if (bl < 4 || bl > 1073741824) return -1;
            return (ssize_t)(1 + bl);
        }
        if (len < 4) return 0;
        uint32_t ml = rd32(data);
        if (ml < 8 || ml > 100000) return -1;
        return (ssize_t)ml;
    }
    if (len < 5) return 0;
    uint32_t bl = rd32(data + 1);
    if (bl < 4 || bl > 1073741824) return -1;
    return (ssize_t)(1 + bl);
}

/* ---- Cross-service RYW: keel.* GUC intercept helpers ---- */

/**
 * @brief Try to parse "SET keel.read_after_lsn = 'VALUE'" (case-insensitive).
 *
 * Accepts both single-quoted and unquoted values.  Returns true and fills
 * @p lsn_out (NUL-terminated, up to 127 chars) when the command matches.
 */
static bool pg_try_parse_set_keel_lsn(const char* sql, size_t sql_len,
                                       char* lsn_out, size_t lsn_max)
{
    /* Minimum: "SET keel.read_after_lsn=x" — 27 chars */
    if (sql_len < 27) return false;

    /* Skip leading whitespace */
    size_t i = 0;
    while (i < sql_len && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' ||
                            sql[i] == '\r')) i++;

    /* Match "SET " */
    if (i + 4 > sql_len) return false;
    if (strncasecmp(sql + i, "SET", 3) != 0) return false;
    i += 3;
    if (sql[i] != ' ' && sql[i] != '\t') return false;
    while (i < sql_len && (sql[i] == ' ' || sql[i] == '\t')) i++;

    /* Match "keel.read_after_lsn" */
    const char* kvar = "keel.read_after_lsn";
    size_t kvar_len = 19;
    if (i + kvar_len > sql_len) return false;
    if (strncasecmp(sql + i, kvar, kvar_len) != 0) return false;
    i += kvar_len;

    /* Optional whitespace then '=' */
    while (i < sql_len && (sql[i] == ' ' || sql[i] == '\t')) i++;
    if (i >= sql_len || sql[i] != '=') return false;
    i++;
    while (i < sql_len && (sql[i] == ' ' || sql[i] == '\t')) i++;
    if (i >= sql_len) return false;

    /* Extract value: single-quoted or unquoted */
    size_t val_start, val_end;
    if (sql[i] == '\'') {
        i++;
        val_start = i;
        while (i < sql_len && sql[i] != '\'') i++;
        val_end = i;
    } else {
        val_start = i;
        while (i < sql_len && sql[i] != ' ' && sql[i] != '\t' &&
               sql[i] != '\n' && sql[i] != '\r' && sql[i] != ';') i++;
        val_end = i;
    }

    size_t val_len = val_end - val_start;
    if (val_len == 0 || val_len >= lsn_max) return false;
    memcpy(lsn_out, sql + val_start, val_len);
    lsn_out[val_len] = '\0';
    return true;
}

/**
 * @brief Test whether the SQL is "SHOW keel.write_lsn" (case-insensitive).
 */
static bool pg_is_show_keel_write_lsn(const char* sql, size_t sql_len)
{
    const char* target = "show keel.write_lsn";
    const size_t tlen = 19;
    if (sql_len < tlen) return false;
    size_t i = 0;
    while (i < sql_len && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' ||
                            sql[i] == '\r')) i++;
    if (i + tlen > sql_len) return false;
    if (strncasecmp(sql + i, target, tlen) != 0) return false;
    /* Allow trailing whitespace / semicolons */
    i += tlen;
    while (i < sql_len && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' ||
                            sql[i] == '\r' || sql[i] == ';')) i++;
    return i == sql_len;
}

/**
 * @brief Build a synthetic PostgreSQL result set in @p buf for "SHOW keel.write_lsn".
 *
 * Emits: RowDescription(1 col "write_lsn") + DataRow(value) + CommandComplete("SHOW") + ReadyForQuery('I').
 * Returns total bytes written, or -1 if @p buf is too small.
 */
static ssize_t pg_build_keel_show_response(const char* value, uint8_t* buf, size_t bufsz)
{
    size_t val_len = strlen(value);  /* bytes in the LSN string */
    /* Compute sizes:
     *  RowDescription: 'T' + int32(len) + int16(1 col) + col_name\0 + 6*int32 + 2*int16
     *  DataRow:        'D' + int32(len) + int16(1 col) + int32(val_len) + val_len bytes
     *  CommandComplete:'C' + int32(len) + "SHOW\0"
     *  ReadyForQuery:  'Z' + int32(5) + 'I'
     */
    const char* col_name = "write_lsn";
    size_t col_name_len = 9; /* strlen("write_lsn") */

    /* RowDescription body: int16(numCols=1) + col_name\0(10) + int32(0)+int32(0)+int16(0)+int32(0)+int16(0)+int32(0)+int16(0) = 18 bytes after col name */
    size_t rd_body = 2 + (col_name_len + 1) + 18;
    size_t rd_total = 1 + 4 + rd_body;  /* 'T' + int32 + body */

    /* DataRow body: int16(1) + int32(val_len) + val_len */
    size_t dr_body = 2 + 4 + val_len;
    size_t dr_total = 1 + 4 + dr_body;

    /* CommandComplete: "SHOW\0" = 5 bytes */
    size_t cc_total = 1 + 4 + 5;

    /* ReadyForQuery: 1 byte status */
    size_t rfq_total = 1 + 4 + 1;

    size_t total = rd_total + dr_total + cc_total + rfq_total;
    if (bufsz < total) return -1;

    uint8_t* p = buf;

    /* RowDescription */
    *p++ = 'T';
    wr32(p, (uint32_t)(rd_body + 4)); p += 4;
    p[0] = 0; p[1] = 1; p += 2;  /* numFields = 1 */
    memcpy(p, col_name, col_name_len + 1); p += col_name_len + 1;
    /* tableOID=0, colAttrNum=0, typeOID=25 (text), typeSize=-1, typeMod=-1, format=0 */
    wr32(p, 0); p += 4;          /* tableOID */
    p[0] = 0; p[1] = 0; p += 2; /* colAttrNum */
    wr32(p, 25); p += 4;         /* typeOID: text */
    p[0] = 0xff; p[1] = 0xff; p += 2;  /* typeSize: -1 */
    wr32(p, 0xffffffff); p += 4; /* typeMod: -1 (as uint = 0xffffffff) */
    p[0] = 0; p[1] = 0; p += 2; /* format: text */

    /* DataRow */
    *p++ = 'D';
    wr32(p, (uint32_t)(dr_body + 4)); p += 4;
    p[0] = 0; p[1] = 1; p += 2;           /* numColumns = 1 */
    wr32(p, (uint32_t)val_len); p += 4;    /* column data length */
    memcpy(p, value, val_len); p += val_len;

    /* CommandComplete */
    *p++ = 'C';
    wr32(p, 4 + 5); p += 4;
    memcpy(p, "SHOW\0", 5); p += 5;

    /* ReadyForQuery */
    *p++ = 'Z';
    wr32(p, 5); p += 4;
    *p++ = 'I';

    return (ssize_t)(p - buf);
}

/**
 * @brief Build a synthetic PostgreSQL CommandComplete("SET") + ReadyForQuery('I')
 *        for intercepted SET commands that the proxy handles without backend.
 */
static ssize_t pg_build_keel_set_response(uint8_t* buf, size_t bufsz)
{
    /* CommandComplete("SET\0") = 1+4+4 = 9, ReadyForQuery = 1+4+1 = 6 */
    if (bufsz < 15) return -1;
    uint8_t* p = buf;
    *p++ = 'C'; wr32(p, 4 + 4); p += 4; memcpy(p, "SET\0", 4); p += 4;
    *p++ = 'Z'; wr32(p, 5); p += 4; *p++ = 'I';
    return (ssize_t)(p - buf);
}

/* ---- vtable: on_fe_msg ---- */

/**
 * @brief Parse and classify a single complete frontend message.
 *
 * This is the main protocol state machine for client-to-proxy traffic.
 * On entry `data[0..len-1]` contains EXACTLY one complete message as
 * returned by pgf_frame_len().  The function fills `act` with the
 * action the engine should take.
 *
 * **Startup phase** (ctx->startup_complete == false):
 *   Parses the startup packet (length-only, no type byte):
 *    - Protocol version 80877103  → SSL request: act=SSL_REQUEST
 *    - Protocol version 80877102  → Cancel request: fan-out cancel
 *    - Protocol version 196608 (3.0) → read user/database/application_name
 *      parameters, build AuthenticationRequest(SCRAM-SHA-256), set
 *      act=AUTH_COMPLETE to send the handshake buffer to the FE.
 *
 * **SCRAM auth exchange** (startup_complete==false, auth step 1/2):
 *   Handles 'p' (PasswordMessage) carrying SASLInitialResponse and
 *   SASLResponse.  Validates SCRAM-SHA-256 steps using
 *   `keel_scram_step()`; on success emits AuthenticationOK + backend
 *   parameter messages + ReadyForQuery.
 *
 * **Normal query phase** (startup_complete == true):
 *   Dispatches on the 1-byte message type tag:
 *    - 'Q' SimpleQuery       → KEEL_FE_ACT_QUERY (classify_sql, stats)
 *    - 'P' Parse             → track prepared stmt, route to backend
 *    - 'B' Bind              → update active_portal
 *    - 'D' Describe          → transparent forward
 *    - 'E' Execute           → anon-mode rewrite if needed
 *    - 'S' Sync              → KEEL_FE_ACT_FORWARD
 *    - 'H' Flush             → KEEL_FE_ACT_FORWARD
 *    - 'C' Close             → remove stmt/portal from cache
 *    - 'X' Terminate         → KEEL_FE_ACT_TERMINATE
 *    - 'd' CopyData          → KEEL_FE_ACT_FORWARD
 *    - 'c' CopyDone          → clear COPY pin, FORWARD
 *    - 'f' CopyFail          → clear COPY pin, FORWARD
 *    - 'p' PasswordMessage   → post-auth SASL continuation
 *
 * **PS virtualization** (ps_mode == KEEL_PS_MODE_VIRTUALIZE): Named Parse
 *   messages are tracked: the wire bytes are duplicated into stmt_cache,
 *   a synthetic ParseComplete is returned immediately to the client
 *   (act=SEND_FE), and the actual Parse is deferred to pool-borrow time
 *   (replay).  Anonymous Parses ('') bypass virtualization.
 *
 * @param vctx  Protocol context (pg_flow_ctx_t*).
 * @param data  Exactly one complete FE message.
 * @param len   Length of message in bytes.
 * @param act   OUTPUT: action for the engine to perform.
 * @return 0 on success, -1 on parse error.
 */
static int pgf_on_fe_msg(void* vctx, const uint8_t* data, size_t len,
                          keel_fe_action_t* act) {
    pg_flow_ctx_t* ctx = vctx;
    *act = keel_fe_action_default();

    /* Startup / auth phase — startup_complete is false until the full
     * handshake succeeds.  Two sub-phases exist:
     *
     *  Phase A (auth_pending == false): waiting for StartupMessage.
     *  Phase B (auth_pending == true):  challenge sent; waiting for 'p'.
     */
    if (!ctx->startup_complete) {

        /* ---- Phase B: auth challenge response ('p' PasswordMessage) ---- */
        if (ctx->auth_pending) {
            /*
             * Two entry points:
             *  (a) Normal: client sent a 'p' password message (len >= 5).
             *  (b) Async resume: keel_engine_flow_resume_auth() called us
             *      with len==0 after the LDAP/PAM thread completed its bind.
             *      In that case the auth state is already VERIFY→SUCCESS/FAILED,
             *      so we skip re-calling keel_auth_process().
             */
            bool async_resume = (len == 0);

            keel_auth_state_t st;
            if (async_resume) {
                /* Poll the completed result from the auth context */
                st = keel_auth_get_state(ctx->auth_ctx);
            } else {
                if (len < 5 || data[0] != 'p') {
                    act->type = KEEL_FE_ACT_ERROR;
                    return -1;
                }
                /* data[1..4] = length, data[5..] = password / SASL payload */
                const void* rdata  = data + 5;
                size_t       rlen   = len > 5 ? len - 5 : 0;

                /* For SASL (round 0), the first 'p' is SASLInitialResponse:
                 *   mechanism-name '\0' int32(len) initial-response
                 * Strip the mechanism prefix so the SCRAM handler only sees
                 * the client-first-message bytes. */
                if (ctx->auth_round == 0 && rlen > 0) {
                    const char* mech = (const char*)rdata;
                    size_t mech_len = strnlen(mech, rlen);
                    if (mech_len < rlen) {
                        /* Skip: mechanism-name + '\0' + int32(data-len) */
                        size_t skip = mech_len + 1 + 4;
                        if (skip <= rlen) {
                            /* int32 data length in the next 4 bytes */
                            const uint8_t* lp = (const uint8_t*)rdata + mech_len + 1;
                            int32_t dlen = (int32_t)(((uint32_t)lp[0] << 24) |
                                                      ((uint32_t)lp[1] << 16) |
                                                      ((uint32_t)lp[2] <<  8) |
                                                       (uint32_t)lp[3]);
                            if (dlen >= 0 && skip + (size_t)dlen <= rlen) {
                                rdata = (const uint8_t*)rdata + skip;
                                rlen  = (size_t)dlen;
                            } else if (dlen < 0) {
                                /* -1 means no initial response */
                                rdata = NULL;
                                rlen  = 0;
                            }
                        }
                    }
                }
                ctx->auth_round++;

                /* Strip trailing NUL for cleartext passwords */
                if (rlen > 0 && ((const char*)rdata)[rlen - 1] == '\0')
                    rlen--;

                /* Make a null-terminated copy so auth providers can safely use
                 * C string functions (strchr/strstr/strlen) on network data. */
                void* rdata_safe = NULL;
                if (rlen > 0) {
                    rdata_safe = keel_malloc(rlen + 1);
                    if (!rdata_safe) {
                        act->type = KEEL_FE_ACT_ERROR;
                        return -1;
                    }
                    memcpy(rdata_safe, rdata, rlen);
                    ((char*)rdata_safe)[rlen] = '\0';
                    rdata = rdata_safe;
                }

                st = keel_auth_process(ctx->auth_ctx, rdata, rlen);
                keel_free(rdata_safe);
            }

            if (!async_resume && st == KEEL_AUTH_STATE_CHALLENGE) {
                /* Multi-round exchange (e.g. SCRAM step 2): send next challenge */
                void*  next_msg  = NULL;
                size_t next_len  = 0;
                int    next_type = 0;
                keel_auth_get_message(ctx->auth_ctx, &next_msg, &next_len, &next_type);
                build_auth_challenge(ctx, next_msg, next_len);
                keel_free(next_msg);
                act->type            = KEEL_FE_ACT_SEND_FE;
                act->fe_response     = ctx->handshake_buf;
                act->fe_response_len = ctx->handshake_len;
                return 0;
            }

            if (!async_resume && st == KEEL_AUTH_STATE_VERIFY) {
                /* Auth offloaded to an async worker thread (LDAP/PAM).
                 * Hand the notify fd to the engine so the reactor can arm
                 * a read on it.  When it fires, engine calls resume_auth()
                 * which will come back here with async_resume=true. */
                int nfd = keel_auth_get_verify_fd(ctx->auth_ctx);
                if (nfd < 0) {
                    /* Couldn't create eventfd — fall back to reject */
                    keel_auth_context_free(ctx->auth_ctx);
                    ctx->auth_ctx     = NULL;
                    ctx->auth_pending = false;
                    build_auth_error(ctx, ctx->username);
                    act->type            = KEEL_FE_ACT_AUTH_REJECT;
                    act->fe_response     = ctx->handshake_buf;
                    act->fe_response_len = ctx->handshake_len;
                    act->client_username = ctx->username;
                    act->client_database = ctx->database;
                    return 0;
                }
                act->type            = KEEL_FE_ACT_WAIT_AUTH;
                act->auth_notify_fd  = nfd;
                return 0;
            }

            if (st == KEEL_AUTH_STATE_SUCCESS) {
                /* Get optional final server message (SCRAM ServerFinal, type 12) */
                void*  final_msg  = NULL;
                size_t final_len  = 0;
                int    final_type = 0;
                keel_auth_get_message(ctx->auth_ctx, &final_msg, &final_len, &final_type);
                (void)final_type;

                keel_auth_context_free(ctx->auth_ctx);
                ctx->auth_ctx     = NULL;
                ctx->auth_pending = false;

                build_auth_success_response(ctx, final_msg, final_len);
                keel_free(final_msg);

                ctx->startup_complete = true;
                pg_stmt_restamp_context(ctx);
                ctx->reusable         = true;
                act->type             = KEEL_FE_ACT_AUTH_COMPLETE;
                act->fe_response      = ctx->handshake_buf;
                act->fe_response_len  = ctx->handshake_len;
                act->client_username  = ctx->username;
                act->client_database  = ctx->database;
                return 0;
            }

            /* Auth failed (FAILED or ERROR) */
            keel_auth_context_free(ctx->auth_ctx);
            ctx->auth_ctx     = NULL;
            ctx->auth_pending = false;
            build_auth_error(ctx, ctx->username);
            act->type            = KEEL_FE_ACT_AUTH_REJECT;
            act->fe_response     = ctx->handshake_buf;
            act->fe_response_len = ctx->handshake_len;
            act->client_username = ctx->username;
            act->client_database = ctx->database;
            return 0;
        }

        /* ---- Phase A: StartupMessage / SSL / Cancel ---- */
        if (len < 8) { act->type = KEEL_FE_ACT_ERROR; return -1; }
        uint32_t ver = rd32(data + 4);

        if (ver == PG_SSL_REQUEST_CODE) {
            static const uint8_t n = 'N';
            act->type = KEEL_FE_ACT_SSL_REQUEST;
            act->fe_response = &n;
            act->fe_response_len = 1;
            return 0;
        }
        if (ver == PG_CANCEL_REQUEST_CODE) {
            act->type = KEEL_FE_ACT_CANCEL_REQUEST;
            act->be_payload = data;
            act->be_payload_len = len;
            return 0;
        }
        if (ver != PG_PROTOCOL_V3) { act->type = KEEL_FE_ACT_ERROR; return -1; }

        /* Parse StartupMessage parameters */
        uint32_t ml = rd32(data);
        if (ml > len) { act->type = KEEL_FE_ACT_ERROR; return -1; }
        const char* p = (const char*)(data + 8);
        size_t rem = ml - 8;
        while (rem > 1) {
            const char* k = p; size_t kl = strnlen(k, rem);
            if (kl >= rem) break; p += kl+1; rem -= kl+1;
            const char* v = p; size_t vl = strnlen(v, rem);
            if (vl >= rem) break; p += vl+1; rem -= vl+1;
            if (strcmp(k,"user")==0)
                strncpy(ctx->username, v, sizeof(ctx->username)-1);
            else if (strcmp(k,"database")==0)
                strncpy(ctx->database, v, sizeof(ctx->database)-1);
            else if (strcmp(k,"application_name")==0)
                strncpy(ctx->application_name, v, sizeof(ctx->application_name)-1);
        }
        if (!ctx->database[0])
            strncpy(ctx->database, ctx->username, sizeof(ctx->database)-1);

        /* ---- Auth dispatch ----
         *
         * If an auth manager is configured, start the exchange for this user.
         * For cert auth (immediate SUCCESS) and trust mode (NULL manager),
         * skip straight to success.  For challenge-based methods, build the
         * first challenge message and wait for a 'p' response.
         */
        if (ctx->auth_manager) {
            keel_auth_context_t* ach = NULL;
            keel_error_t aerr = keel_auth_manager_start(
                ctx->auth_manager, ctx->username, &ach);
            if (aerr != KEEL_OK || !ach) {
                build_auth_error(ctx, ctx->username);
                act->type            = KEEL_FE_ACT_AUTH_REJECT;
                act->fe_response     = ctx->handshake_buf;
                act->fe_response_len = ctx->handshake_len;
                act->client_username = ctx->username;
                act->client_database = ctx->database;
                return 0;
            }
            ctx->auth_ctx = ach;

            keel_auth_state_t st = keel_auth_get_state(ach);
            if (st == KEEL_AUTH_STATE_SUCCESS) {
                /* cert / trust immediate success */
                keel_auth_context_free(ctx->auth_ctx);
                ctx->auth_ctx = NULL;
                build_auth_success_response(ctx, NULL, 0);
                ctx->startup_complete = true;
                pg_stmt_restamp_context(ctx);
                ctx->reusable         = true;
                act->type             = KEEL_FE_ACT_AUTH_COMPLETE;
                act->fe_response      = ctx->handshake_buf;
                act->fe_response_len  = ctx->handshake_len;
                act->client_username  = ctx->username;
                act->client_database  = ctx->database;
                return 0;
            }

            /* Challenge-based: get and send the first challenge */
            void*  chal_msg  = NULL;
            size_t chal_len  = 0;
            int    chal_type = 0;
            if (keel_auth_get_message(ctx->auth_ctx, &chal_msg,
                                      &chal_len, &chal_type) != KEEL_OK) {
                keel_auth_context_free(ctx->auth_ctx);
                ctx->auth_ctx = NULL;
                build_auth_error(ctx, ctx->username);
                act->type            = KEEL_FE_ACT_AUTH_REJECT;
                act->fe_response     = ctx->handshake_buf;
                act->fe_response_len = ctx->handshake_len;
                act->client_username = ctx->username;
                act->client_database = ctx->database;
                return 0;
            }
            build_auth_challenge(ctx, chal_msg, chal_len);
            keel_free(chal_msg);

            ctx->auth_pending    = true;
            act->type            = KEEL_FE_ACT_SEND_FE;
            act->fe_response     = ctx->handshake_buf;
            act->fe_response_len = ctx->handshake_len;
            return 0;
        }

        /* Trust mode (no auth manager): send handshake immediately */
        build_handshake_response(ctx);
        ctx->startup_complete = true;
        pg_stmt_restamp_context(ctx);
        ctx->reusable = true;
        act->type = KEEL_FE_ACT_AUTH_COMPLETE;
        act->fe_response = ctx->handshake_buf;
        act->fe_response_len = ctx->handshake_len;
        act->client_username = ctx->username;
        act->client_database = ctx->database;
        return 0;
    }

    /* Regular messages */
    if (len < 5) { act->type = KEEL_FE_ACT_ERROR; return -1; }
    uint8_t t = data[0];

    switch (t) {
    case 'Q': { /* Simple Query */
        const char* sql = (const char*)(data+5);
        size_t sl = len - 5;
        if (sl > 0 && sql[sl-1] == '\0') sl--;
        keel_query_effect_flags_t eff; keel_flow_route_t rt;
        keel_flow_pin_reason_t ps, pc;
        uint32_t qtype = 0;
        classify_sql(sql, sl, &eff, &rt, &ps, &pc, &qtype);
        ctx->metrics_classify_count++;
        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = KEEL_MSG_KIND_SQL;        act->effect = eff; act->route_hint = rt;
        act->pin_update = ps; act->pin_clear = pc;
        act->query_type = qtype;
        act->be_payload = data; act->be_payload_len = len;
        act->sql_view = sql; act->sql_view_len = sl;
        act->cache_eligible = !(eff & (KEEL_QE_WRITE|KEEL_QE_DDL|
            KEEL_QE_BEGINS_TX|KEEL_QE_ENDS_TX|KEEL_QE_HARD_PIN|
            KEEL_QE_SETS_STATE|KEEL_QE_MULTI_STMT));
        act->splice_eligible = (len > 8192);

        if (qtype == KEEL_QUERY_COMMIT) {
            ctx->stmt_last_tx_end_was_rollback = false;
        } else if (qtype == KEEL_QUERY_ROLLBACK) {
            ctx->stmt_last_tx_end_was_rollback = true;
        }

        /* Conservative semantic invalidation:
         * - Unknown/unmodellable utility semantics => never reuse stmt set.
         * - DDL or DISCARD PLANS/ALL => invalidate prepared-stmt set and
         *   bump schema epoch so semantic compatibility changes deterministically.
         */
        if (qtype == KEEL_QUERY_UNKNOWN || (eff & KEEL_QE_UNKNOWN_STATE)) {
            ctx->stmt_semantic_unknown = true;
            pg_stmt_clear_all(ctx);
            act->pin_clear |= KEEL_FPIN_PREPARED_STMT;
            pg_stmt_restamp_context(ctx);
        } else if ((qtype == KEEL_QUERY_CREATE ||
                    qtype == KEEL_QUERY_ALTER ||
                    qtype == KEEL_QUERY_DROP) &&
                   !pg_stmt_is_temp_context_change(ctx, sql, sl, qtype)) {
            ctx->stmt_schema_epoch++;
            ctx->stmt_semantic_unknown = false;
            pg_stmt_clear_all(ctx);
            act->pin_clear |= KEEL_FPIN_PREPARED_STMT;
            pg_stmt_restamp_context(ctx);
        } else if ((qtype == KEEL_QUERY_DISCARD ||
                    (qtype == KEEL_QUERY_RESET &&
                     pg_sql_contains_word_ci(sql, sl, "discard"))) &&
                   (pg_sql_contains_word_ci(sql, sl, "all") ||
                    pg_sql_contains_word_ci(sql, sl, "plans"))) {
            ctx->stmt_schema_epoch++;
            ctx->stmt_semantic_unknown = false;
            pg_stmt_clear_all(ctx);
            act->pin_clear |= KEEL_FPIN_PREPARED_STMT;
            /* DISCARD ALL resets all GUC settings and role back to session
             * defaults (equivalent to RESET ALL + SET SESSION AUTHORIZATION
             * DEFAULT).  DISCARD PLANS only drops cached plans; GUC values
             * are unaffected.  Reset the context fields so the compatibility
             * signature and replay buffer reflect the cleaned-up state. */
            if (pg_sql_contains_word_ci(sql, sl, "all")) {
                pg_stmt_guc_change_t reset_all = {
                    .valid = true, .is_reset_all = true
                };
                pg_stmt_apply_guc_change(ctx, &reset_all);
                ctx->stmt_role[0]         = '\0';
                ctx->stmt_session_auth[0] = '\0';
            }
            pg_stmt_restamp_context(ctx);
        }

        /* Replication tracking: rewrite COMMIT to capture XID first.
         * When txn_tracking is enabled and the FE sends a bare COMMIT,
         * replace the payload with "SELECT txid_current() AS _keel_txid; COMMIT;"
         * The XID is absorbed from the DataRow and signalled to the engine
         * via keel_be_action_t.commit_xid_captured. */
        if (ctx->txn_tracking && (eff & KEEL_QE_ENDS_TX) &&
            qtype == KEEL_QUERY_COMMIT && !ctx->xid_probe_active) {
            act->be_payload    = kPgXidCommitMsg;
            act->be_payload_len = sizeof(kPgXidCommitMsg);
            ctx->xid_probe_active  = true;
            ctx->xid_probe_result  = 0;
            KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[XID-PROBE] SET active ctx=%p (Simple Query COMMIT)", (void*)ctx);
        }
        /* DEALLOCATE / DISCARD wipes all named stmts on the backend */
        if (pc & KEEL_FPIN_PREPARED_STMT) {
            /* Clear any in-flight pending tracking PREPARE — the DISCARD/DEALLOCATE
             * supersedes it.  Free the saved prior wire_msg to avoid a leak. */
            if (ctx->pending_track_valid) {
                if (ctx->pending_track_prior.wire_msg) {
                    keel_free(ctx->pending_track_prior.wire_msg);
                    ctx->pending_track_prior.wire_msg = NULL;
                }
                ctx->pending_track_valid = false;
                ctx->pending_track_had_prior = false;
            }

            /* Tracking mode DEALLOCATE by name: virtualise at session level.
             * Only remove the specific named entry; do not wipe the whole cache.
             * Set pending_deallocate_valid so on_be_msg can absorb a "prepared
             * statement does not exist" error if the backend changed since the
             * statement was created (transaction pooling). */
            bool handled_by_name = false;

            /* PINNING mode: individual DEALLOCATE (not ALL) does NOT release
             * the backend pin.  The backend is hard-pinned to this session
             * for its lifetime; only DEALLOCATE ALL, DISCARD ALL, or client
             * disconnect may release it. */
            if (!handled_by_name &&
                ctx->ps_mode == KEEL_PS_MODE_PINNING &&
                qtype == (uint32_t)KEEL_QUERY_DEALLOCATE) {
                char dealloc_name[64] = "";
                if (tracking_parse_deallocate(sql, sl,
                                              dealloc_name, sizeof(dealloc_name)) &&
                    dealloc_name[0] != '\0' &&
                    strcasecmp(dealloc_name, "ALL") != 0) {
                    /* Keep PREPARED_STMT pin — backend stays pinned. */
                    act->pin_clear &= ~(keel_flow_pin_reason_t)KEEL_FPIN_PREPARED_STMT;
                    handled_by_name = true;
                }
            }

            if (ctx->ps_mode == KEEL_PS_MODE_TRACKING &&
                qtype == (uint32_t)KEEL_QUERY_DEALLOCATE) {
                char dealloc_name[64] = "";
                if (tracking_parse_deallocate(sql, sl,
                                              dealloc_name, sizeof(dealloc_name)) &&
                    dealloc_name[0] != '\0' &&
                    strcasecmp(dealloc_name, "ALL") != 0) {
                    /* Only absorb a "prepared statement does not exist" error from
                     * the backend if the entry was confirmed in our session cache.
                     * A confirmed entry means Keel accepted the PREPARE, so from
                     * the client's perspective DEALLOCATE should succeed even if
                     * the backend changed (transaction-pool hop).
                     * If the entry was NOT confirmed (or never existed), forward
                     * the backend error to the client unchanged. */
                    pg_stmt_entry_t* dentry = pg_stmt_find(ctx, dealloc_name);
                    bool was_confirmed = (dentry != NULL &&
                                         dentry->valid &&
                                         dentry->confirmed);
                    pg_stmt_remove(ctx, dealloc_name);
                    /* Re-evaluate pin_clear: if no confirmed stmts remain, clear pin */
                    if (ctx->session_stmt_hash == 0)
                        act->pin_clear |= KEEL_FPIN_PREPARED_STMT;
                    if (was_confirmed) {
                        ctx->pending_deallocate_valid = true;
                        ctx->pending_deallocate_absorbed_error = false;
                    }
                    handled_by_name = true;
                }
            }

            if (!handled_by_name) {
                pg_stmt_clear_all(ctx);
            }
        }

        /* GUC/role/temp extraction: skip components that cannot apply.
         * - SHOW/EXPLAIN: skip everything (read-only, no side-effects)
         * - SELECT: can call set_config() so GUC extraction needed,
         *   but cannot CREATE/DROP temp, DISCARD, or SET ROLE. */
        if (ctx->ps_mode != KEEL_PS_MODE_ANONYMOUS &&
            qtype != KEEL_QUERY_SHOW &&
            qtype != KEEL_QUERY_EXPLAIN) {
            /* Temp context: SELECT cannot create/drop temp tables
             * or run DISCARD — skip the expensive hardpin + word scans. */
            if (qtype != KEEL_QUERY_SELECT &&
                pg_stmt_is_temp_context_change(ctx, sql, sl, qtype)) {
                pg_stmt_bump_temp_epoch(ctx);
                if (ctx->ps_mode == KEEL_PS_MODE_TRACKING &&
                    ctx->in_transaction &&
                    pg_stmt_has_confirmed_entries(ctx)) {
                    ctx->stmt_discard_plans_before_execute = true;
                }
                if (pg_stmt_temp_change_resets_on_tx_end(sql, sl))
                    ctx->stmt_temp_tx_reset_pending = true;
                if (ctx->in_transaction)
                    ctx->stmt_temp_tx_rollback_reset_pending = true;
            }

            /* GUC extraction: needed for SELECT too (set_config()). */
            pg_stmt_guc_change_t guc_change;
            pg_stmt_apply_guc_change(ctx,
                pg_try_extract_stmt_guc_change(sql, sl, &guc_change)
                    ? &guc_change
                    : NULL);

            /* Role extraction: SELECT cannot SET ROLE / SET SESSION
             * AUTHORIZATION — skip the expensive text scan. */
            if (qtype != KEEL_QUERY_SELECT) {
                char new_role[sizeof(ctx->stmt_role)];
                char new_session_auth[sizeof(ctx->stmt_session_auth)];
                strncpy(new_role, ctx->stmt_role, sizeof(new_role) - 1);
                new_role[sizeof(new_role) - 1] = '\0';
                strncpy(new_session_auth, ctx->stmt_session_auth,
                    sizeof(new_session_auth) - 1);
                new_session_auth[sizeof(new_session_auth) - 1] = '\0';
                if (pg_try_extract_role_change(sql, sl,
                               new_role, sizeof(new_role),
                               new_session_auth, sizeof(new_session_auth)) &&
                (strcmp(ctx->stmt_role, new_role) != 0 ||
                 strcmp(ctx->stmt_session_auth, new_session_auth) != 0)) {
                    strncpy(ctx->stmt_role, new_role, sizeof(ctx->stmt_role) - 1);
                    ctx->stmt_role[sizeof(ctx->stmt_role) - 1] = '\0';
                    strncpy(ctx->stmt_session_auth, new_session_auth,
                        sizeof(ctx->stmt_session_auth) - 1);
                    ctx->stmt_session_auth[sizeof(ctx->stmt_session_auth) - 1] = '\0';
                    pg_stmt_restamp_context(ctx);
                }
            }
        }

        /* Tracking mode: intercept "PREPARE name AS body" Simple Query.
         *
         * When ps_mode == KEEL_PS_MODE_TRACKING we shadow the named prepared
         * statement in the local stmt_cache (same as the extended-protocol
         * Parse path) and strip the KEEL_QE_HARD_PIN flag so the engine
         * uses the KEEL_FPIN_PREPARED_STMT replay path instead of hard-pin.
         * This keeps backends poolable while transparently replaying Parse
         * messages on new connections.
         *
         * The query is still forwarded to the backend as-is (act->be_payload
         * is unchanged); we only add the replay entry.
         *
         * IMPORTANT: the entry is staged with confirmed=false and a pending
         * state is recorded.  Confirmation (confirmed=true, hash XOR'd into
         * session_stmt_hash) only happens when the backend sends
         * CommandComplete("PREPARE").  On ErrorResponse the staged entry is
         * rolled back to the prior state, preserving duplicate-PREPARE semantics
         * that match PostgreSQL behaviour exactly.
         */
        if ((ps & KEEL_FPIN_PREPARED_STMT)
            && ctx->ps_mode == KEEL_PS_MODE_TRACKING
            && qtype == (uint32_t)KEEL_QUERY_PREPARE) {
            /* Strip HARD_PIN: TRACKING mode virtualises PREPARE via the
             * session-hash replay path; the backend is not hard-pinned. */
            act->effect &= ~(keel_query_effect_flags_t)KEEL_QE_HARD_PIN;
            char  trk_name[64];   trk_name[0]  = '\0';
            char  trk_body[2048]; trk_body[0]  = '\0';
            size_t trk_body_len = 0;
            if (tracking_parse_prepare(sql, sl,
                                       trk_name, sizeof(trk_name),
                                       trk_body, sizeof(trk_body),
                                       &trk_body_len)
                && trk_name[0] != '\0') {
                /* Preserve SQL-level PREPARE for replay.  PostgreSQL's SQL
                 * PREPARE and extended Parse share a namespace, but SQL
                 * PREPARE has subtly different invalidation behavior around
                 * transaction-local temp table shadowing. */
                size_t wire_len = len;
                if (wire_len <= PG_STMT_MAX_WIRE) {
                    uint8_t* wire = keel_malloc(wire_len);
                    if (wire) {
                        memcpy(wire, data, wire_len);

                        /* Save prior entry (if any confirmed entry exists for this
                         * name) so we can restore it if the backend rejects the PREPARE.
                         * Detach wire_msg from the existing entry before calling
                         * pg_stmt_upsert() to prevent the upsert from freeing it. */
                        pg_stmt_entry_t* existing = pg_stmt_find(ctx, trk_name);
                        bool had_prior = (existing != NULL &&
                                          existing->valid &&
                                          existing->confirmed);
                        pg_stmt_entry_t prior_entry;
                        if (had_prior) {
                            prior_entry = *existing;
                            existing->wire_msg     = NULL; /* detach: we now own it */
                            existing->wire_msg_len = 0;
                        }

                        pg_stmt_entry_t* e = pg_stmt_upsert(ctx, trk_name);
                        if (e) {
                            size_t sc = trk_body_len < sizeof(e->sql) - 1
                                      ? trk_body_len
                                      : sizeof(e->sql) - 1;
                            memcpy(e->sql, trk_body, sc);
                            e->sql[sc] = '\0';
                            e->sql_len = sc;

                            keel_query_effect_flags_t be;
                            keel_flow_route_t br;
                            keel_flow_pin_reason_t bps, bpc;
                            uint32_t bqt = 0;
                            classify_sql(trk_body, trk_body_len,
                                         &be, &br, &bps, &bpc, &bqt);
                            ctx->metrics_classify_count++;

                            e->query_type = bqt;
                            e->effect = be;
                            e->route = br;
                            e->pin_set = bps;
                            e->pin_clr = bpc;

                            e->wire_msg = wire;
                            e->wire_msg_len = wire_len;
                            e->valid = true;
                            /* Stage as unconfirmed: confirmation happens in on_be_msg
                             * when CommandComplete("PREPARE") arrives. */
                            e->confirmed = false;
                            e->context_sig = ctx->stmt_context_sig;
                            e->hash = pg_stmt_entry_hash(e, e->context_sig);

                            /* Record pending state for rollback */
                            strncpy(ctx->pending_track_name, trk_name,
                                    sizeof(ctx->pending_track_name) - 1);
                            ctx->pending_track_name[sizeof(ctx->pending_track_name)-1] = '\0';
                            ctx->pending_track_valid     = true;
                            ctx->pending_track_had_prior = had_prior;
                            if (had_prior)
                                ctx->pending_track_prior = prior_entry;

                            /* Recompute session hash: the prior confirmed entry (if
                             * any) is no longer included since it is now unconfirmed. */
                            pg_stmt_recompute_session_hash(ctx);
                        } else {
                            /* Upsert failed (OOM): restore prior state if needed */
                            if (had_prior && prior_entry.wire_msg)
                                keel_free(prior_entry.wire_msg);
                            keel_free(wire);
                        }
                    }
                }
            }
        }

        if (ctx->stmt_discard_plans_before_execute &&
            ctx->ps_mode == KEEL_PS_MODE_TRACKING &&
            qtype == (uint32_t)KEEL_QUERY_EXECUTE &&
            act->be_payload == data) {
            static const char discard_prefix[] = "DISCARD PLANS;";
            size_t prefix_len = sizeof(discard_prefix) - 1;
            size_t new_sql_len = prefix_len + sl;
            size_t total = 1 + 4 + new_sql_len + 1;
            if (total <= UINT32_MAX) {
                if (ctx->stmt_discard_plans_rewrite_cap < total) {
                    keel_free(ctx->stmt_discard_plans_rewrite_buf);
                    ctx->stmt_discard_plans_rewrite_buf =
                        (uint8_t*)keel_malloc(total);
                    ctx->stmt_discard_plans_rewrite_cap =
                        ctx->stmt_discard_plans_rewrite_buf ? total : 0;
                }

                if (ctx->stmt_discard_plans_rewrite_buf &&
                    ctx->stmt_discard_plans_rewrite_cap >= total) {
                    uint8_t* wp = ctx->stmt_discard_plans_rewrite_buf;
                    *wp++ = 'Q';
                    wr32(wp, (uint32_t)(4 + new_sql_len + 1));
                    wp += 4;
                    memcpy(wp, discard_prefix, prefix_len);
                    wp += prefix_len;
                    memcpy(wp, sql, sl);
                    wp += sl;
                    *wp = '\0';

                    act->be_payload = ctx->stmt_discard_plans_rewrite_buf;
                    act->be_payload_len = total;
                    ctx->stmt_discard_plans_before_execute = false;
                    ctx->stmt_discard_plans_absorb_pending = true;
                }
            }
        }

        /* Cross-service RYW: intercept SET keel.read_after_lsn and SHOW keel.write_lsn
         * before forwarding to the backend.  Both are synthetic — the proxy handles
         * them entirely without a backend round-trip. */
        if (qtype == KEEL_QUERY_SET) {
            char lsn_val[128];
            if (pg_try_parse_set_keel_lsn(sql, sl, lsn_val, sizeof(lsn_val))) {
                ssize_t resp_len = pg_build_keel_set_response(
                    ctx->ryw_resp_buf, sizeof(ctx->ryw_resp_buf));
                if (resp_len > 0) {
                    ctx->ryw_resp_len = (size_t)resp_len;
                    act->type             = KEEL_FE_ACT_SEND_FE;
                    act->fe_response      = ctx->ryw_resp_buf;
                    act->fe_response_len  = ctx->ryw_resp_len;
                    /* Signal the engine to update the session's consistency atom */
                    strncpy(act->inject_consistency_lsn, lsn_val,
                            sizeof(act->inject_consistency_lsn) - 1);
                    act->inject_consistency_lsn[sizeof(act->inject_consistency_lsn) - 1] = '\0';
                }
                return 0;
            }
        } else if (qtype == KEEL_QUERY_SHOW) {
            if (pg_is_show_keel_write_lsn(sql, sl)) {
                const char* lsn = ctx->keel_write_lsn[0] ? ctx->keel_write_lsn : "";
                ssize_t resp_len = pg_build_keel_show_response(
                    lsn, ctx->ryw_resp_buf, sizeof(ctx->ryw_resp_buf));
                if (resp_len > 0) {
                    ctx->ryw_resp_len = (size_t)resp_len;
                    act->type            = KEEL_FE_ACT_SEND_FE;
                    act->fe_response     = ctx->ryw_resp_buf;
                    act->fe_response_len = ctx->ryw_resp_len;
                }
                return 0;
            }
        }

        return 0;
    }
    case 'P': { /* Parse — contains SQL text for classification */
        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = KEEL_MSG_KIND_EXTENDED;
        act->route_hint = KEEL_FROUTE_PRIMARY;
        act->pin_update = KEEL_FPIN_EXTENDED_PROTO;
        act->be_payload = data; act->be_payload_len = len;
        /* Parse: 'P' + int32 len + string stmt_name + string query + ...
         * If stmt_name is non-empty, this creates a named prepared statement
         * that persists across transactions → pin to backend.
         * Exception: ANONYMOUS mode intercepts the Parse and never sends it
         * to the backend, so the backend acquires no named-statement state. */
        if (len > 5 && data[5] != '\0'
            && ctx->ps_mode != KEEL_PS_MODE_ANONYMOUS) {
            ctx->named_stmt_count++;
            act->pin_update |= KEEL_FPIN_PREPARED_STMT;
        }
        /* Extract SQL text from Parse message:
         * offset 5 = start of stmt_name (NUL-terminated)
         * query follows immediately after stmt_name's NUL */
        if (len > 5) {
            const char* stmt_name = (const char*)(data + 5);
            size_t name_len = strnlen(stmt_name, len - 5);

            /* For unnamed statements, invalidate the active cache so a
             * stale classification isn't accidentally used by Execute.
             * Named statements don't touch cached_valid — they rely on
             * Bind to activate their classification from the stmt_cache. */
            if (stmt_name[0] == '\0')
                ctx->cached_valid = false;
            size_t query_off = 5 + name_len + 1;      /* skip stmt_name + NUL */
            if (query_off < len) {
                const char* sql = (const char*)(data + query_off);
                size_t max_sql = len - query_off;
                size_t sql_len = strnlen(sql, max_sql);
                if (sql_len > 0) {
                    keel_query_effect_flags_t eff; keel_flow_route_t rt;
                    keel_flow_pin_reason_t ps, pc;
                    uint32_t qtype = 0;
                    classify_sql(sql, sql_len, &eff, &rt, &ps, &pc, &qtype);
                    ctx->metrics_classify_count++;
                    act->effect = eff;
                    act->route_hint = rt;
                    act->pin_update |= ps;
                    act->pin_clear = pc;
                    act->query_type = qtype;
                    act->sql_view = sql;
                    act->sql_view_len = sql_len;
                    act->cache_eligible = !(eff & (KEEL_QE_WRITE|KEEL_QE_DDL|
                        KEEL_QE_BEGINS_TX|KEEL_QE_ENDS_TX|KEEL_QE_HARD_PIN|
                        KEEL_QE_SETS_STATE|KEEL_QE_MULTI_STMT));

                    /* OFF mode: hard-pin is already armed above (KEEL_FPIN_PREPARED_STMT).
                     * Skip all stmt tracking — no cache, no hash, no replay buffer.
                     * The named PS lives only on the pinned backend connection and is
                     * never replayed to a different backend. */
                    if (ctx->ps_mode == KEEL_PS_MODE_OFF)
                        return 0;

                    /* Store in named statement cache */
                    pg_stmt_entry_t* entry = pg_stmt_upsert(ctx, stmt_name);
                    size_t scopy = sql_len < sizeof(entry->sql) - 1
                                 ? sql_len : sizeof(entry->sql) - 1;
                    memcpy(entry->sql, sql, scopy);
                    entry->sql[scopy] = '\0';
                    entry->sql_len = scopy;
                    entry->query_type = qtype;
                    entry->effect = eff;
                    entry->route = rt;
                    entry->pin_set = ps;
                    entry->pin_clr = pc;
                    entry->valid = true;
                    entry->context_sig = ctx->stmt_context_sig;

                    /* Anonymous mode: intercept named Parse messages.
                     *
                     * Store the statement name→SQL in anon_map for JIT
                     * rewrite at Bind time.  Return a synthetic ParseComplete
                     * directly to the client and absorb the Parse message
                     * (do NOT send to backend).  Backend connections never
                     * acquire named prepared-statement state, so they stay
                     * freely poolable.
                     *
                     * KEEL_FPIN_PREPARED_STMT is NOT set so the engine will
                     * not pin the backend or add the stmt to the replay buffer.
                     */
                    if (stmt_name[0] != '\0'
                        && ctx->ps_mode == KEEL_PS_MODE_ANONYMOUS) {
                        /* Store in anonymous map (lazily allocated) */
                        pg_anon_upsert(pg_anon_ensure(ctx), stmt_name, sql, sql_len);

                        /* Synthesize ParseComplete ('1') response */
                        ctx->anon_resp_buf[0] = '1';
                        wr32(ctx->anon_resp_buf + 1, 4);  /* length = 4 */
                        ctx->anon_resp_len = 5;

                        /* Override act: send response to client, no backend */
                        act->type          = KEEL_FE_ACT_SEND_FE;
                        act->be_payload    = NULL;
                        act->be_payload_len = 0;
                        act->fe_response     = ctx->anon_resp_buf;
                        act->fe_response_len = ctx->anon_resp_len;
                        /* Clear PREPARED_STMT pin — backend stays clean */
                        act->pin_update &= ~(keel_flow_pin_reason_t)KEEL_FPIN_PREPARED_STMT;
                        act->pin_clear  |= KEEL_FPIN_PREPARED_STMT;
                        return 0;
                    }

                    /* For named statements: store the full Parse wire message
                     * for prepared-statement replay (spec §17). */
                    if (stmt_name[0] != '\0') {
                        if (len <= PG_STMT_MAX_WIRE) {
                            entry->wire_msg = (uint8_t*)keel_malloc(len);
                            if (entry->wire_msg) {
                                memcpy(entry->wire_msg, data, len);
                                entry->wire_msg_len = len;
                            }
                        }
                        /* Compute per-stmt hash: fnv1a(name + "|" + sql).
                         * Do NOT XOR into session_stmt_hash yet — wait for
                         * ParseComplete ('1') to confirm the stmt was accepted.
                         * Track the pending stmt in pending_parse_* fields. */
                        uint64_t h = pg_stmt_entry_hash(entry, ctx->stmt_context_sig);
                        entry->hash = h;
                        entry->confirmed = false;
                        strncpy(ctx->pending_parse_name, stmt_name,
                                sizeof(ctx->pending_parse_name) - 1);
                        ctx->pending_parse_name[sizeof(ctx->pending_parse_name)-1] = '\0';
                        ctx->pending_parse_hash = h;
                        ctx->pending_parse_valid = true;
                    }

                    /* For unnamed statements ("" name), also set
                     * active classification directly — Execute may
                     * follow without a Bind for simple pipelines. */
                    if (stmt_name[0] == '\0') {
                        pg_stmt_activate(ctx, entry);
                    }
                }
            }
        }
        return 0;
    }
    case 'B': { /* Bind — look up named stmt and activate its classification */
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->msg_kind = KEEL_MSG_KIND_EXTENDED;
        act->pin_update = KEEL_FPIN_EXTENDED_PROTO;
        act->be_payload = data; act->be_payload_len = len;
        /* Bind: 'B' + int32 len + string portal_name + string stmt_name + ...
         * Look up stmt_name in the cache and activate it for the next Execute. */
        if (len > 5) {
            const char* portal = (const char*)(data + 5);
            size_t portal_len = strnlen(portal, len - 5);
            size_t stmt_off = 5 + portal_len + 1;
            if (stmt_off < len) {
                const char* stmt_name = (const char*)(data + stmt_off);
                pg_stmt_entry_t* entry = pg_stmt_find(ctx, stmt_name);
                if (entry) {
                    pg_stmt_activate(ctx, entry);
                }

                /* Anonymous mode: JIT-rewrite named Bind → anonymous Parse+Bind.
                 *
                 * When pos_mode == KEEL_PS_MODE_ANONYMOUS and the stmt_name
                 * refers to an anonymously shadowed statement, we construct:
                 *   1. Parse('', sql, 0 params)  — anonymous, one-shot
                 *   2. Bind(portal, '', client's params...)
                 * and forward the pair to the backend instead of the original
                 * Bind.  The backend never holds named prepared-statement state.
                 *
                 * The rewrite buffer is owned by `ctx->anon_rewrite_buf` and
                 * lazily allocated / grown as needed (capacity tracked by
                 * ctx->anon_rewrite_cap).
                 */
                if (stmt_name[0] != '\0'
                    && ctx->ps_mode == KEEL_PS_MODE_ANONYMOUS) {
                    pg_anon_entry_t* ae = pg_anon_find(ctx->anon_map, stmt_name);
                    if (ae) {
                        /* Calculate the one-shot Parse message size:
                         * 'P' + int32(len) + '\0' (anon name) + sql\0 + int16 0
                         */
                        size_t parse_len = 1 + 4 + 1 + ae->sql_len + 1 + 2;

                        /* Rewrite the Bind to reference the unnamed statement:
                         * Original Bind: 'B'|len4|portal\0|stmt_name\0|...
                         * Rewritten:     'B'|len4|portal\0|'\0'      |...
                         * The rest of the Bind payload (param formats, values,
                         * result formats) is copied verbatim.
                         *
                         * We know stmt_name starts at data+stmt_off.  The tail
                         * after stmt_name\0 is at data+stmt_off+strlen+1. */
                        size_t tail_off = stmt_off + strlen(stmt_name) + 1;
                        size_t tail_len = len - tail_off;

                        /* Rewritten Bind: 'B'|len4|portal\0|'\0'|tail */
                        size_t new_bind_body = portal_len + 1  /* portal\0 */
                                             + 1               /* anon name '\0' */
                                             + tail_len;
                        size_t new_bind_len = 1 + 4 + new_bind_body;

                        size_t total = parse_len + new_bind_len;

                        /* (Re-)allocate rewrite buffer if needed */
                        if (ctx->anon_rewrite_cap < total) {
                            keel_free(ctx->anon_rewrite_buf);
                            ctx->anon_rewrite_buf = (uint8_t*)keel_malloc(total);
                            ctx->anon_rewrite_cap = ctx->anon_rewrite_buf
                                                  ? total : 0;
                        }

                        if (ctx->anon_rewrite_buf && ctx->anon_rewrite_cap >= total) {
                            uint8_t* wp = ctx->anon_rewrite_buf;

                            /* 1. Anonymous Parse: 'P' len4 '\0' sql '\0' int16(0) */
                            *wp++ = 'P';
                            wr32(wp, (uint32_t)(parse_len - 1)); wp += 4;
                            *wp++ = '\0';                         /* anon stmt name */
                            memcpy(wp, ae->sql, ae->sql_len); wp += ae->sql_len;
                            *wp++ = '\0';                         /* sql NUL */
                            *wp++ = 0; *wp++ = 0;                 /* num_params=0 */

                            /* 2. Rewritten Bind: 'B' len4 portal '\0' '\0' tail */
                            *wp++ = 'B';
                            wr32(wp, (uint32_t)(new_bind_len - 1)); wp += 4;
                            memcpy(wp, portal, portal_len); wp += portal_len;
                            *wp++ = '\0';                         /* portal NUL */
                            *wp++ = '\0';                         /* anon stmt name */
                            memcpy(wp, data + tail_off, tail_len);

                            /* Forward the rewritten Parse+Bind pair */
                            act->be_payload     = ctx->anon_rewrite_buf;
                            act->be_payload_len = total;
                        }
                        /* else: allocation failed — fall through to original Bind */
                    }
                }
            }
        }
        return 0;
    }
    case 'D': /* Describe — pipeline, no SQL */
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->msg_kind = KEEL_MSG_KIND_EXTENDED;
        act->pin_update = KEEL_FPIN_EXTENDED_PROTO;
        act->be_payload = data; act->be_payload_len = len;
        return 0;
    case 'E': { /* Execute — replay cached Parse classification for hooks */
        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = KEEL_MSG_KIND_EXTENDED;
        act->pin_update = KEEL_FPIN_EXTENDED_PROTO;
        act->be_payload = data; act->be_payload_len = len;
        if (ctx->cached_valid) {
            act->sql_view = ctx->cached_sql;
            act->sql_view_len = ctx->cached_sql_len;
            act->query_type = ctx->cached_query_type;
            act->effect = ctx->cached_effect;
            act->route_hint = ctx->cached_route;
            act->pin_update |= ctx->cached_pin_set;
            act->pin_clear = ctx->cached_pin_clr;
            act->cache_eligible = !(ctx->cached_effect & (KEEL_QE_WRITE|KEEL_QE_DDL|
                KEEL_QE_BEGINS_TX|KEEL_QE_ENDS_TX|KEEL_QE_HARD_PIN|
                KEEL_QE_SETS_STATE|KEEL_QE_MULTI_STMT));

            if (ctx->cached_query_type == KEEL_QUERY_COMMIT) {
                ctx->stmt_last_tx_end_was_rollback = false;
            } else if (ctx->cached_query_type == KEEL_QUERY_ROLLBACK) {
                ctx->stmt_last_tx_end_was_rollback = true;
            }

            /* Semantic invalidation for DDL, DISCARD ALL/PLANS, and unknown
             * utility — mirrors the Simple Query path so protocol context
             * stays consistent regardless of whether extended protocol is
             * used.  Must run before the temp-epoch and GUC blocks below. */
            if (ctx->ps_mode != KEEL_PS_MODE_ANONYMOUS) {
                uint32_t cqt  = ctx->cached_query_type;
                const char* csql = ctx->cached_sql;
                size_t csl    = ctx->cached_sql_len;
                keel_query_effect_flags_t ceff = ctx->cached_effect;

                if (cqt == KEEL_QUERY_UNKNOWN || (ceff & KEEL_QE_UNKNOWN_STATE)) {
                    ctx->stmt_semantic_unknown = true;
                    pg_stmt_clear_all(ctx);
                    pg_stmt_restamp_context(ctx);
                } else if ((cqt == KEEL_QUERY_CREATE ||
                            cqt == KEEL_QUERY_ALTER  ||
                            cqt == KEEL_QUERY_DROP) &&
                           !pg_stmt_is_temp_context_change(ctx, csql, csl, cqt)) {
                    ctx->stmt_schema_epoch++;
                    ctx->stmt_semantic_unknown = false;
                    pg_stmt_clear_all(ctx);
                    pg_stmt_restamp_context(ctx);
                } else if ((cqt == KEEL_QUERY_DISCARD ||
                            (cqt == KEEL_QUERY_RESET &&
                             pg_sql_contains_word_ci(csql, csl, "discard"))) &&
                           (pg_sql_contains_word_ci(csql, csl, "all") ||
                            pg_sql_contains_word_ci(csql, csl, "plans"))) {
                    ctx->stmt_schema_epoch++;
                    ctx->stmt_semantic_unknown = false;
                    pg_stmt_clear_all(ctx);
                    if (pg_sql_contains_word_ci(csql, csl, "all")) {
                        pg_stmt_guc_change_t reset_all = {
                            .valid = true, .is_reset_all = true
                        };
                        pg_stmt_apply_guc_change(ctx, &reset_all);
                        ctx->stmt_role[0]         = '\0';
                        ctx->stmt_session_auth[0] = '\0';
                    }
                    pg_stmt_restamp_context(ctx);
                }
            }

            if (ctx->ps_mode != KEEL_PS_MODE_ANONYMOUS &&
                pg_stmt_is_temp_context_change(ctx,
                                               ctx->cached_sql,
                                               ctx->cached_sql_len,
                                               ctx->cached_query_type)) {
                pg_stmt_bump_temp_epoch(ctx);
                if (ctx->ps_mode == KEEL_PS_MODE_TRACKING &&
                    ctx->in_transaction &&
                    pg_stmt_has_confirmed_entries(ctx)) {
                    ctx->stmt_discard_plans_before_execute = true;
                }
                if (pg_stmt_temp_change_resets_on_tx_end(ctx->cached_sql,
                                                         ctx->cached_sql_len)) {
                    ctx->stmt_temp_tx_reset_pending = true;
                }
                if (ctx->in_transaction)
                    ctx->stmt_temp_tx_rollback_reset_pending = true;
            }

            if (ctx->ps_mode != KEEL_PS_MODE_ANONYMOUS) {
                pg_stmt_guc_change_t guc_change;
                pg_stmt_apply_guc_change(ctx,
                    pg_try_extract_stmt_guc_change(ctx->cached_sql,
                                                   ctx->cached_sql_len,
                                                   &guc_change)
                        ? &guc_change
                        : NULL);

                char new_role[sizeof(ctx->stmt_role)];
                char new_session_auth[sizeof(ctx->stmt_session_auth)];
                strncpy(new_role, ctx->stmt_role, sizeof(new_role) - 1);
                new_role[sizeof(new_role) - 1] = '\0';
                strncpy(new_session_auth, ctx->stmt_session_auth,
                        sizeof(new_session_auth) - 1);
                new_session_auth[sizeof(new_session_auth) - 1] = '\0';
                if (pg_try_extract_role_change(ctx->cached_sql,
                                               ctx->cached_sql_len,
                                               new_role, sizeof(new_role),
                                               new_session_auth,
                                               sizeof(new_session_auth)) &&
                    (strcmp(ctx->stmt_role, new_role) != 0 ||
                     strcmp(ctx->stmt_session_auth, new_session_auth) != 0)) {
                    strncpy(ctx->stmt_role, new_role, sizeof(ctx->stmt_role) - 1);
                    ctx->stmt_role[sizeof(ctx->stmt_role) - 1] = '\0';
                    strncpy(ctx->stmt_session_auth, new_session_auth,
                            sizeof(ctx->stmt_session_auth) - 1);
                    ctx->stmt_session_auth[sizeof(ctx->stmt_session_auth) - 1] = '\0';
                    pg_stmt_restamp_context(ctx);
                }
            }
        }
        return 0;
    }
    case 'S': /* Sync — end of extended query pipeline */
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->msg_kind = KEEL_MSG_KIND_EXTENDED;
        act->pin_clear = KEEL_FPIN_EXTENDED_PROTO;
        act->be_payload = data; act->be_payload_len = len;
        return 0;
    case 'H': /* Flush */
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data; act->be_payload_len = len;
        return 0;
    case 'C': { /* Close — may destroy a named prepared statement */
        act->type = KEEL_FE_ACT_QUERY;
        act->msg_kind = KEEL_MSG_KIND_EXTENDED;
        act->pin_update = KEEL_FPIN_EXTENDED_PROTO;
        act->be_payload = data; act->be_payload_len = len;
        /* Close: 'C' + int32 len + byte type ('S'=stmt/'P'=portal) + string name
         * If closing a named statement, decrement count; unpin when zero.
         * Also remove from the prepared statement classification cache. */
        if (len > 6 && data[5] == 'S' && data[6] != '\0') {
            const char* stmt_name = (const char*)(data + 6);
            pg_stmt_remove(ctx, stmt_name);
            /* Anonymous mode: also remove from the anon map */
            if (ctx->ps_mode == KEEL_PS_MODE_ANONYMOUS)
                pg_anon_remove(ctx->anon_map, stmt_name);
            if (ctx->named_stmt_count > 0)
                ctx->named_stmt_count--;
            if (ctx->named_stmt_count == 0)
                act->pin_clear = KEEL_FPIN_PREPARED_STMT;
        }
        return 0;
    }
    case 'X': /* Terminate */
        act->type = KEEL_FE_ACT_TERMINATE;
        return 0;
    case 'p': /* Password */
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data; act->be_payload_len = len;
        return 0;
    case 'd': case 'c': case 'f': /* COPY data/done/fail */
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->msg_kind = KEEL_MSG_KIND_COPY;
        act->be_payload = data; act->be_payload_len = len;
        act->splice_eligible = true;
        return 0;
    default:
        act->type = KEEL_FE_ACT_FORWARD_TO_BACKEND;
        act->be_payload = data; act->be_payload_len = len;
        return 0;
    }
}

/* ---- vtable: is_data_frame ---- */

/**
 * @brief Return true if the message starting at hdr is a pure DataRow ('D').
 *
 * The engine uses this to bypass `on_be_msg()` and forward raw bytes
 * directly to the client at full wire speed (fast-forward / splice path).
 * Only 'D' DataRow messages qualify — they carry no session state and
 * have no side effects on the proxy state machine.
 *
 * **XID probe exception**: when `ctx->xid_probe_active` is set, the
 * backend is responding to the injected `SELECT txid_current()` probe.
 * The upcoming DataRow carries the XID we need to capture, so we MUST
 * route it through `on_be_msg()` instead of fast-forwarding.  Returning
 * false here forces the engine to call on_be_msg.
 *
 * @param hdr      Pointer to the start of the message (type byte).
 * @param hdr_len  Number of available header bytes (at least 1).
 * @return true if the message can be forwarded without parsing.
 */
static bool pgf_is_data_frame(void* vctx, const uint8_t* hdr, size_t hdr_len) {
    /* PG wire format: every message starts with a 1-byte type tag.
     * 'D' (0x44) = DataRow — pure row data, no state transitions.
     * All other tags ('Z' ReadyForQuery, 'C' CommandComplete, 'E' ErrorResponse,
     * 'S' ParameterStatus, 'K' BackendKeyData, etc.) must go through on_be_msg.
     *
     * Exception: when xid_probe_active is true the DataRow carries the XID
     * from the injected "SELECT txid_current()" and MUST be absorbed by
     * on_be_msg — never forwarded to the client.  Returning false here forces
     * the engine to call on_be_msg for that single DataRow. */
    if (vctx) {
        pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
        if (ctx->xid_probe_active)
            return false;
    }
    return hdr_len >= 1 && hdr[0] == 'D';
}

/* ---- vtable: on_be_msg ---- */

/**
 * @brief Parse and classify a single complete backend message.
 *
 * Called by the engine for every BE message NOT short-cut by is_data_frame().
 * On entry `data[0..len-1]` contains exactly one complete message.  The
 * function fills `act`.
 *
 * **XID probe absorption** (ctx->xid_probe_active == true):
 *   The backend is responding to the `kPgXidCommitMsg` rewrite of COMMIT.
 *   Messages are classified:
 *    - 'T' RowDescription  → ABSORB (examine column names for REASSERT)
 *    - 'D' DataRow         → ABSORB + extract XID from column 0
 *    - 'C' CommandComplete
 *         - "SELECT\0"     → ABSORB (from txid_current() SELECT)
 *         - otherwise      → FORWARD (COMMIT CommandComplete, clears probe)
 *    - 'Z' ReadyForQuery   → FORWARD + act.query_complete=true
 *
 *   **Content REASSERT**: If a 'T' RowDescription is received with a first
 *   column name of `_keel_txid`, the probe IS active despite xid_probe_active
 *   being clear (e.g. the original COMMIT was forwarded un-rewritten on the
 *   resume path).  xid_probe_active is reinstated and the message is absorbed.
 *
 * **Normal messages**:
 *    - 'Z' ReadyForQuery  → FORWARD + act.query_complete, update tx_status
 *    - 'E' ErrorResponse  → FORWARD, may synthesise error classification
 *    - 'K' BackendKeyData → capture ctx->backend_pid / cancel_secret
 *    - '1' ParseComplete  → FORWARD (counted by WAIT_STMT_REPLAY)
 *    - 'A' NotifyResponse → FORWARD as async notifications
 *    - all others         → FORWARD
 *
 * @param vctx  Protocol context (pg_flow_ctx_t*).
 * @param data  Exactly one complete BE message.
 * @param len   Length of message in bytes.
 * @param act   OUTPUT: action for the engine to perform.
 * @return 0 on success, -1 on parse error.
 */
static int pgf_on_be_msg(void* vctx, const uint8_t* data, size_t len,
                          keel_be_action_t* act) {
    pg_flow_ctx_t* ctx = vctx;
    *act = keel_be_action_default();
    if (len < 5) { act->type = KEEL_BE_ACT_ERROR; return -1; }
    act->type = KEEL_BE_ACT_FORWARD_FE;
    act->fe_payload = data; act->fe_payload_len = len;
    uint8_t t = data[0];
    if (!ctx) {
        switch (t) {
        case '1':
            act->stmt_replay_accepted = true;
            return 0;
        case 'E':
            act->type = KEEL_BE_ACT_ERROR;
            act->is_error_response = true;
            return 0;
        case 'Z':
            if (len >= 6) {
                char s = (char)data[5];
                act->tx_state_changed = true;
                act->query_complete = true;
                if (s == 'I') {
                    act->tx_status = KEEL_TX_IDLE;
                    act->backend_reusable = true;
                    act->pin_clear |= KEEL_FPIN_TRANSACTION|KEEL_FPIN_FAILED_TX;
                } else if (s == 'T') {
                    act->tx_status = KEEL_TX_ACTIVE;
                    act->pin_update |= KEEL_FPIN_TRANSACTION;
                } else {
                    act->tx_status = KEEL_TX_FAILED;
                    act->pin_update |= KEEL_FPIN_TRANSACTION|KEEL_FPIN_FAILED_TX;
                }
            }
            return 0;
        default:
            return 0;
        }
    }

    if (ctx->commit_doubt_check_active) {
        /* Commit-in-doubt outcome stream is protocol-owned and absorbed here.
         * Expected sequence: RowDescription/DataRow/CommandComplete/RFQ. */
        act->type = KEEL_BE_ACT_ABSORB;
        act->fe_payload = NULL;
        act->fe_payload_len = 0;
        if (t == 'D' && len >= 11) {
            uint16_t ncols = (uint16_t)(((uint16_t)data[5] << 8) | data[6]);
            if (ncols >= 1) {
                int32_t vcl = (int32_t)(((uint32_t)data[7]  << 24)
                                      | ((uint32_t)data[8]  << 16)
                                      | ((uint32_t)data[9]  <<  8)
                                      |  (uint32_t)data[10]);
                if (vcl > 0 && len >= (size_t)(11 + vcl)) {
                    const char* val = (const char*)(data + 11);
                    if (strncmp(val, "committed", 9) == 0) {
                        ctx->commit_doubt_outcome = 1;
                        act->commit_doubt_outcome_changed = true;
                        act->commit_doubt_outcome = 1;
                    } else if (strncmp(val, "aborted", 7) == 0) {
                        ctx->commit_doubt_outcome = 2;
                        act->commit_doubt_outcome_changed = true;
                        act->commit_doubt_outcome = 2;
                    }
                }
            }
            return 0;
        }
        if (t == 'E') {
            /* Keep unknown outcome and wait for RFQ boundary. */
            act->is_error_response = true;
            return 0;
        }
        if (t == 'Z' && len >= 6) {
            char s = (char)data[5];
            act->tx_state_changed = true;
            act->query_complete = true;
            if (s == 'I') {
                act->tx_status = KEEL_TX_IDLE;
                act->backend_reusable = true;
            } else if (s == 'T') {
                act->tx_status = KEEL_TX_ACTIVE;
            } else {
                act->tx_status = KEEL_TX_FAILED;
            }
            ctx->commit_doubt_check_active = false;
            return 0;
        }
        return 0;
    }

    /* Replication tracking: absorb the RowDescription / DataRow / SELECT
     * CommandComplete that come from the "SELECT txid_current()" prepended
     * to COMMIT.  Only the DataRow response is interesting — it contains the
     * XID we need to record.  Everything else is silently discarded.
     *
     * Content-based probe detection: some code paths (pool-wait resume,
     * no-PS DISCARD ALL) may have forwarded the original raw COMMIT bytes
     * to the backend rather than the rewritten kPgXidCommitMsg.  In that
     * case the backend never ran the SELECT probe and xid_probe_active should
     * stay clear.  HOWEVER, if the probe WAS sent but xid_probe_active was
     * cleared prematurely — e.g. a stale CommandComplete from a pipelined
     * interior query tripped the non-SELECT-C defensive clear before T arrived
     * — the T RowDescription from "SELECT txid_current()" would be forwarded
     * to the client despite the probe having been sent correctly.
     *
     * Defence: check the RowDescription column name.  Our probe always
     * aliases the result as "_keel_txid".  If we see that name, re-assert
     * xid_probe_active so all subsequent probe messages are absorbed. */
    if (t == 'T' || t == 'D') {
        char colname[32] = "?";
        if (t == 'T' && len >= 9) {
            /* Extract first column name from RowDescription */
            size_t off = 7; /* T(1)+len(4)+ncols(2) */
            size_t nm = 0;
            while (off + nm < len - 1 && nm < sizeof(colname)-1 && data[off+nm])
                nm++;
            memcpy(colname, data+7, nm < sizeof(colname)-1 ? nm : sizeof(colname)-1);
            colname[nm < sizeof(colname)-1 ? nm : sizeof(colname)-1] = '\0';
        }
        KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[BE-MSG] type='%c' len=%zu xid_probe=%d ctx=%p col0='%s'",
                (char)t, len, (int)ctx->xid_probe_active, (void*)ctx,
                t == 'T' ? colname : "");
    }

    if (t == 'T' && !ctx->xid_probe_active && len >= 18) {
        /* RowDescription: T(1) + len(4) + ncols(2) + col1_name(≥1) + \0
         * "_keel_txid" is 10 chars; minimum frame size: 1+4+2+10+1 = 18. */
        uint16_t ncols = (uint16_t)(((uint16_t)data[5] << 8) | data[6]);
        if (ncols >= 1 &&
            data[17] == '\0' &&
            memcmp(data + 7, "_keel_txid", 10) == 0) {
            /* This T came from our internal XID probe — re-assert the flag
             * so the following D and C(SELECT) are also absorbed. */
            ctx->xid_probe_active = true;
            KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[XID-PROBE] content-detect REASSERT ctx=%p", (void*)ctx);
        }
    }

    if (ctx->xid_probe_active) {
        if (t == 'T') {
            /* RowDescription — absorb, no payload to client */
            KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[XID-PROBE] ABSORB T ctx=%p", (void*)ctx);
            act->type = KEEL_BE_ACT_ABSORB;
            act->fe_payload = NULL; act->fe_payload_len = 0;
            return 0;
        }
        if (t == 'D') {
            KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[XID-PROBE] ABSORB D ctx=%p", (void*)ctx);
            /* DataRow: D | int32(len) | int16(ncols) | int32(col0_len) | col0_data */
            if (len >= 11) {
                uint16_t ncols = (uint16_t)(((uint16_t)data[5] << 8) | data[6]);
                if (ncols >= 1) {
                    int32_t clen = (int32_t)(((uint32_t)data[7]  << 24) |
                                             ((uint32_t)data[8]  << 16) |
                                             ((uint32_t)data[9]  <<  8) |
                                              (uint32_t)data[10]);
                    if (clen > 0 && (size_t)(11 + clen) <= len) {
                        char xid_buf[24] = {0};
                        size_t cplen = (size_t)clen < sizeof(xid_buf)-1
                                        ? (size_t)clen : sizeof(xid_buf)-1;
                        memcpy(xid_buf, data + 11, cplen);
                        ctx->xid_probe_result = (uint64_t)strtoull(xid_buf, NULL, 10);
                    }
                }
            }
            act->type = KEEL_BE_ACT_ABSORB;
            act->fe_payload = NULL; act->fe_payload_len = 0;
            act->commit_xid_captured = true;
            act->commit_xid = ctx->xid_probe_result;
            return 0;
        }
        if (t == 'C') {
            /* CommandComplete: check tag prefix */
            const char* tag = (len > 5) ? (const char*)(data + 5) : "";
            if (strncmp(tag, "SELECT", 6) == 0) {
                /* Absorb CommandComplete(SELECT) — the COMMIT result follows */
                KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[XID-PROBE] CLEAR on C(SELECT) xid=%llu ctx=%p",
                        (unsigned long long)ctx->xid_probe_result, (void*)ctx);
                ctx->xid_probe_active = false;
                act->type = KEEL_BE_ACT_ABSORB;
                act->fe_payload = NULL; act->fe_payload_len = 0;
                return 0;
            }
            /* Non-SELECT CommandComplete (e.g. "COMMIT") while the probe is
             * still active.  This happens when stmt_replay sent the original
             * bare COMMIT instead of the rewritten "SELECT txid_current();
             * COMMIT;" (the engine captured data+pos rather than
             * act->be_payload as the orig_msg).  Without this guard,
             * xid_probe_active would stay true for the rest of the session and
             * every subsequent RowDescription ('T') would be silently absorbed,
             * causing libpq to see DataRows without a prior RowDescription.
             * Clear the stale probe here; fall through so the COMMIT result is
             * forwarded to the client as normal. */
            KEEL_LOG_TRACE(KEEL_LOG_CAT_PROTO, "[XID-PROBE] defensive CLEAR on non-SELECT C tag='%s' ctx=%p",
                    (len > 5) ? (const char*)(data + 5) : "?", (void*)ctx);
            ctx->xid_probe_active = false;
            /* fall through to default forward */
        }
    }

    switch (t) {
    case 'R': /* Auth */
        if (len >= 9 && rd32(data+5) == 0)
            act->type = KEEL_BE_ACT_AUTH_PROGRESS;
        return 0;
    case '1': /* ParseComplete — confirm the in-flight prepared statement */
        act->stmt_replay_accepted = true;
        if (ctx->pending_parse_valid && ctx->pending_parse_hash != 0) {
            /* Mark the cache entry as confirmed and rebuild the session
             * hash from scratch so context_sig is always included. */
            pg_stmt_entry_t* pe = pg_stmt_find(ctx, ctx->pending_parse_name);
            if (pe) pe->confirmed = true;
            pg_stmt_recompute_session_hash(ctx);
            ctx->pending_parse_valid = false;
            ctx->pending_parse_hash  = 0;
        }
        return 0;
    case 'C': /* CommandComplete */
        /* Tracking mode: confirm a staged simple-query PREPARE on success. */
    {
        const char* tag = (len > 5) ? (const char*)(data + 5) : "";
        if (ctx->stmt_discard_plans_absorb_pending &&
            strncmp(tag, "DISCARD PLANS", 13) == 0) {
            ctx->stmt_discard_plans_absorb_pending = false;
            act->type = KEEL_BE_ACT_ABSORB;
            act->fe_payload = NULL;
            act->fe_payload_len = 0;
            return 0;
        }
        if (ctx->pending_track_valid) {
            if (strncmp(tag, "PREPARE", 7) == 0 &&
                (tag[7] == '\0' || !isalnum((unsigned char)tag[7]))) {
                pg_stmt_entry_t* pe = pg_stmt_find(ctx, ctx->pending_track_name);
                if (pe) {
                    pe->confirmed = true;
                    pg_stmt_recompute_session_hash(ctx);
                }
                /* The prior entry's wire_msg is now superseded; free it. */
                if (ctx->pending_track_had_prior &&
                    ctx->pending_track_prior.wire_msg) {
                    keel_free(ctx->pending_track_prior.wire_msg);
                    ctx->pending_track_prior.wire_msg = NULL;
                }
                ctx->pending_track_valid     = false;
                ctx->pending_track_had_prior = false;
            }
        }
        if (strncmp(tag, "PREPARE", 7) == 0 &&
            (tag[7] == '\0' || !isalnum((unsigned char)tag[7]))) {
            act->stmt_replay_accepted = true;
        }
        return 0;
    }
    case 'E': /* ErrorResponse */
        act->is_error_response = true;
        ctx->stmt_discard_plans_absorb_pending = false;
        /* Tracking mode: backend rejected the staged PREPARE — roll back the
         * cache entry to the prior confirmed state (or remove it if there was
         * no prior entry).  The ErrorResponse is still forwarded to the client
         * so it receives the correct PostgreSQL error (e.g. "prepared statement
         * already exists"). */
        if (ctx->pending_track_valid) {
            pg_stmt_entry_t* e = pg_stmt_find(ctx, ctx->pending_track_name);
            if (ctx->pending_track_had_prior) {
                if (e) {
                    /* Free the new (rejected) wire_msg; restore the prior entry. */
                    uint8_t* new_wire = e->wire_msg;
                    *e = ctx->pending_track_prior;
                    keel_free(new_wire);
                    ctx->pending_track_prior.wire_msg = NULL;
                } else {
                    /* Entry evicted from cache; just free the saved prior. */
                    keel_free(ctx->pending_track_prior.wire_msg);
                    ctx->pending_track_prior.wire_msg = NULL;
                }
            } else if (e) {
                /* No prior entry: remove the staged (now-invalid) entry. */
                pg_stmt_remove(ctx, ctx->pending_track_name);
            }
            pg_stmt_recompute_session_hash(ctx);
            ctx->pending_track_valid     = false;
            ctx->pending_track_had_prior = false;
            /* act->type stays KEEL_BE_ACT_FORWARD_FE — the error is forwarded. */
        }
        /* Tracking mode: backend returned "prepared statement does not exist"
         * for a DEALLOCATE by name when the backend had changed since the
         * PREPARE (transaction pooling hop).  Absorb the error; the session
         * cache was already updated and will inject a synthetic success on the
         * following ReadyForQuery. */
        if (ctx->pending_deallocate_valid) {
            act->type           = KEEL_BE_ACT_ABSORB;
            act->fe_payload     = NULL;
            act->fe_payload_len = 0;
            ctx->pending_deallocate_absorbed_error = true;
        }
        return 0;
    case 'Z': /* ReadyForQuery */
        if (len >= 6) {
            char s = (char)data[5];
            char prev = ctx->txn_status;
            ctx->txn_status = s;
            ctx->in_transaction = (s=='T'||s=='E');
            act->tx_state_changed = true;
            act->query_complete = true;

            /* COPY mode exit: ReadyForQuery after COPY means the COPY is done.
             * Clear the copy pin so the backend can be returned to the pool. */
            if (ctx->in_copy) {
                ctx->in_copy = false;
                act->exits_copy_mode = true;
                act->pin_clear |= KEEL_FPIN_COPY;
            }

            if (s == 'I') {
                act->tx_status = KEEL_TX_IDLE;
                act->backend_reusable = true; ctx->reusable = true;
                act->pin_clear |= KEEL_FPIN_TRANSACTION|KEEL_FPIN_FAILED_TX;
                if (ctx->stmt_temp_tx_reset_pending && prev != 'I') {
                    pg_stmt_bump_temp_epoch(ctx);
                    ctx->stmt_temp_tx_reset_pending = false;
                }
                if (ctx->stmt_temp_tx_rollback_reset_pending && prev != 'I' &&
                    ctx->stmt_last_tx_end_was_rollback) {
                    pg_stmt_bump_temp_epoch(ctx);
                }
                if (prev != 'I')
                    pg_stmt_restore_tx_local_gucs(ctx);
                ctx->stmt_discard_plans_before_execute = false;
                ctx->stmt_discard_plans_absorb_pending = false;
                ctx->stmt_temp_tx_rollback_reset_pending = false;
                ctx->stmt_last_tx_end_was_rollback = false;
            } else if (s == 'T') {
                act->tx_status = KEEL_TX_ACTIVE;
                ctx->reusable = false;
                act->pin_update = KEEL_FPIN_TRANSACTION;
            } else {
                act->tx_status = KEEL_TX_FAILED;
                ctx->reusable = false;
                act->pin_update = KEEL_FPIN_TRANSACTION|KEEL_FPIN_FAILED_TX;
            }

            /* Tracking-mode DEALLOCATE by name: if the backend returned an error
             * (absorbed in the 'E' handler above), inject a synthetic
             * CommandComplete("DEALLOCATE") before this ReadyForQuery so the
             * client receives a well-formed success response. */
            if (ctx->pending_deallocate_valid) {
                if (ctx->pending_deallocate_absorbed_error) {
                    uint8_t* p = ctx->ryw_resp_buf;
                    /* CommandComplete("DEALLOCATE") */
                    *p++ = 'C';
                    wr32(p, 4 + 11); p += 4;
                    memcpy(p, "DEALLOCATE\0", 11); p += 11;
                    /* Append the original ReadyForQuery message verbatim */
                    size_t used = (size_t)(p - ctx->ryw_resp_buf);
                    if (len <= sizeof(ctx->ryw_resp_buf) - used) {
                        memcpy(p, data, len);
                        p += len;
                    }
                    ctx->ryw_resp_len = (size_t)(p - ctx->ryw_resp_buf);
                    act->fe_payload     = ctx->ryw_resp_buf;
                    act->fe_payload_len = ctx->ryw_resp_len;
                }
                ctx->pending_deallocate_valid          = false;
                ctx->pending_deallocate_absorbed_error = false;
            }

            /* Safety guard: clear any stale pending tracking PREPARE state.
             * In well-formed simple-query protocol this cannot happen (the
             * 'C' or 'E' handler always fires before 'Z'), but guard against
             * unexpected protocol sequences to prevent session corruption. */
            if (ctx->pending_track_valid) {
                if (ctx->pending_track_had_prior) {
                    pg_stmt_entry_t* e = pg_stmt_find(ctx, ctx->pending_track_name);
                    if (e) {
                        uint8_t* new_wire = e->wire_msg;
                        *e = ctx->pending_track_prior;
                        keel_free(new_wire);
                        ctx->pending_track_prior.wire_msg = NULL;
                    } else {
                        keel_free(ctx->pending_track_prior.wire_msg);
                        ctx->pending_track_prior.wire_msg = NULL;
                    }
                } else {
                    pg_stmt_remove(ctx, ctx->pending_track_name);
                }
                pg_stmt_recompute_session_hash(ctx);
                ctx->pending_track_valid     = false;
                ctx->pending_track_had_prior = false;
            }
        }
        return 0;
    case 'S': /* ParameterStatus */
        if (len > 5) {
            const char* payload = (const char*)(data+5);
            size_t plen = len-5;
            const char* k = payload; size_t kl = strnlen(k, plen);
            if (kl < plen) {
                act->has_profile_update = true;
                act->profile_key = k; act->profile_key_len = kl;
                const char* v = k+kl+1;
                act->profile_value = v;
                act->profile_value_len = strnlen(v, plen-kl-1);
                if (!ctx->in_transaction) {
                    for (int kind = 0; kind < PG_STMT_GUC_COUNT; kind++) {
                        const char* name = pg_stmt_guc_sql_name((pg_stmt_guc_kind_t)kind);
                        if (strcasecmp(k, name) == 0) {
                            pg_stmt_guc_change_t change;
                            memset(&change, 0, sizeof(change));
                            change.valid = true;
                            change.kind = (pg_stmt_guc_kind_t)kind;
                            size_t copy_len = act->profile_value_len;
                            if (copy_len >= sizeof(change.value))
                                copy_len = sizeof(change.value) - 1;
                            memcpy(change.value, v, copy_len);
                            change.value[copy_len] = '\0';
                            pg_stmt_apply_guc_change(ctx, &change);
                            break;
                        }
                    }
                }
            }
        }
        return 0;
    case 'K': /* BackendKeyData */
        if (len >= 13) {
            ctx->backend_pid = rd32(data+5);
            ctx->backend_secret = rd32(data+9);
        }
        return 0;
    case 'G': case 'H': case 'W': /* CopyIn/Out/Both */
        act->enters_copy_mode = true;
        act->pin_update = KEEL_FPIN_COPY;
        ctx->in_copy = true;
        return 0;
    default:
        return 0;
    }
}

/**
 * @brief Compute an FNV-1a fingerprint of a SQL string.
 *
 * Case-folds uppercase A–Z to lowercase and normalises newlines, carriage
 * returns, and tabs to spaces before hashing, so functionally identical
 * queries with differing case or whitespace produce the same fingerprint.
 *
 * @param v  Flow context (unused).
 * @param s  SQL text.
 * @param n  Byte length of @p s.
 * @return 64-bit FNV-1a fingerprint.
 */
static uint64_t pgf_fingerprint(void* v, const char* s, size_t n) {
    (void)v;
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c>='A'&&c<='Z') c+=32;
        if (c=='\n'||c=='\r'||c=='\t') c=' ';
        h ^= (uint64_t)(uint8_t)c;
        h *= 1099511628211ULL;
    }
    return h;
}

/* ---- vtable: build_state_sync ---- */

/**
 * @brief Generate a Simple Query (`Q`) containing SET statements to
 *        synchronise the backend session to the client's expected state.
 *
 * Computes the diff between the backend's current state profile (`bp`)
 * and the client session's desired profile (`sp`) using `generate_sync_sql()`.
 * If the profiles match (or sp is empty), returns 0 and writes nothing.
 *
 * The output is a single PostgreSQL 'Q' message:
 *   'Q' | uint32_be(4+sql_len+1) | sql NUL-terminated
 *
 * The SQL is a semicolon-separated list of `SET key = value` statements.
 * PostgreSQL executes them as a multi-statement simple query and returns
 * one CommandComplete + one ReadyForQuery.  The engine drains the response
 * on the BE path before forwarding the original client message.
 *
 * @param v     Protocol context (unused; may be NULL).
 * @param bp    Backend current state profile.
 * @param sp    Session desired state profile.
 * @param buf   Output buffer.
 * @param blen  Buffer capacity.
 * @return Bytes written (0 = no sync needed, -1 = buffer too small).
 */
static ssize_t pgf_build_state_sync(void* v,
    const struct state_profile* bp, const struct state_profile* sp,
    uint8_t* buf, size_t blen) {
    (void)v;

    /* Generate diff SQL from backend profile (bp) to session profile (sp) */
    state_sync_result_t result;
    if (generate_sync_sql(bp, sp, &result) != 0) {
        return -1;  /* Buffer overflow in SQL generation */
    }

    if (!result.needs_sync || result.sql_len == 0) {
        return 0;  /* No sync needed */
    }

    /* Wrap in PostgreSQL Simple Query message: 'Q' | len | sql\0 */
    size_t sql_len_with_nul = result.sql_len + 1;
    size_t total = 1 + 4 + sql_len_with_nul;
    if (blen < total) return -1;

    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sql_len_with_nul));
    memcpy(buf + 5, result.sql, result.sql_len);
    buf[5 + result.sql_len] = '\0';

    return (ssize_t)total;
}

/* ---- vtable: build_cleanup ---- */

/**
 * @brief Generate a Simple Query (`Q`) to clean up a backend connection
 *        before returning it to the pool or closing it.
 *
 * The SQL emitted depends on the cleanup reason:
 *
 *  FAILED_TX          → `ROLLBACK`
 *    The transaction is in error state.  A plain ROLLBACK returns the
 *    backend to idle.  DISCARD ALL is intentionally skipped because:
 *      (a) the connection may be reusable without full discard
 *      (b) DISCARD ALL after ROLLBACK emits an extra CommandComplete
 *          which is harder to drain correctly.
 *
 *  FE_DISCONNECT / TX_NOT_IDLE / UNKNOWN_STATE / HARD_TAINT / TIMEOUT
 *    → `ROLLBACK; DISCARD ALL`
 *    Two-statement query: abort any open transaction first, then discard
 *    all session state (prepared stmts, temp tables, SET params, notify
 *    subscriptions).  The backend connection is being discarded after
 *    this, so the isTopLevel restriction on DISCARD ALL is moot.
 *
 * The output message is a single 'Q' Simple Query wire frame.
 *
 * @return Bytes written, or -1 if buf is too small.
 */
static ssize_t pgf_build_cleanup(void* v, keel_cleanup_reason_t r,
                                  uint8_t* buf, size_t blen) {
    (void)v;
    /* Choose the cleanup SQL based on the reason:
     *
     *  FE_DISCONNECT / UNKNOWN_STATE / HARD_TAINT / TIMEOUT
     *      — full cleanup: ROLLBACK; DISCARD ALL clears all session state.
     *        The backend connection is being discarded anyway, so the
     *        isTopLevel restriction on multi-statement DISCARD ALL is
     *        irrelevant here (we don't care if DISCARD ALL errors out
     *        inside an implicit transaction; the connection is going away).
     *
     *  FAILED_TX
     *      — ROLLBACK only: transaction is in error state; DISCARD ALL
     *        must be preceded by a successful ROLLBACK to exit the aborted
     *        transaction, but since the connection is being closed we just
     *        roll back to leave the backend clean before return.
     *
     *  TX_NOT_IDLE
     *      — ROLLBACK; DISCARD ALL: same as FE_DISCONNECT.
     */
    const char* sql;
    switch (r) {
    case KEEL_CLEANUP_FAILED_TX:
        sql = "ROLLBACK";
        break;
    case KEEL_CLEANUP_FE_DISCONNECT:
    case KEEL_CLEANUP_TX_NOT_IDLE:
    case KEEL_CLEANUP_UNKNOWN_STATE:
    case KEEL_CLEANUP_HARD_TAINT:
    case KEEL_CLEANUP_TIMEOUT:
    default:
        sql = "ROLLBACK; DISCARD ALL";
        break;
    }

    size_t sl    = strlen(sql) + 1u;   /* include NUL terminator */
    size_t total = 1u + 4u + sl;
    if (blen < total) return -1;
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4u + sl));
    memcpy(buf + 5, sql, sl);
    return (ssize_t)total;
}

/* ---- vtable: backend_reuse_gate ---- */

/**
 * @brief Return true if the backend connection is safe to return to the pool.
 *
 * A backend is reusable only when ALL three conditions hold:
 *  1. `ctx->reusable`      — no fatal error or hard taint occurred
 *  2. `!ctx->in_transaction` — not in an open transaction (tx_status == 'I')
 *  3. `!ctx->in_copy`      — not mid-COPY data stream
 *
 * Called by the engine after `query_complete` before calling
 * backend_pool_return().  If this returns false the connection is
 * discarded (closed) rather than recycled.
 */
static bool pgf_reuse_gate(void* v) {
    pg_flow_ctx_t* c = v;
    return c->reusable && !c->in_transaction && !c->in_copy;
}

/* ---- vtable: generate_startup ---- */

/**
 * @brief Build a PostgreSQL protocol-version-3 startup packet.
 *
 * Format (no type byte, total length at offset 0):
 *   uint32_be  total_length
 *   uint32_be  protocol_version (196608 = 3.0)
 *   "user\0"   key\0   (5 bytes)
 *   user\0     value\0 (ul+1 bytes)
 *   "database\0" key   (9 bytes)
 *   db\0       value\0 (dl+1 bytes)
 *   \0         terminator
 *
 * @param user  Database username (NUL-terminated).
 * @param db    Database name (NUL-terminated).
 * @param buf   Output buffer.
 * @param blen  Buffer capacity.
 * @return Total bytes written, or -1 if the buffer is too small.
 */
static ssize_t pgf_gen_startup(void* v, const char* user,
                                const char* db, uint8_t* buf, size_t blen) {
    (void)v;
    size_t ul = strlen(user), dl = strlen(db);
    size_t ml = 4+4+5+ul+1+9+dl+1+1;
    if (blen < ml) return -1;
    uint8_t* p = buf;
    wr32(p,(uint32_t)ml); p+=4; wr32(p,PG_PROTOCOL_V3); p+=4;
    memcpy(p,"user",5); p+=5; memcpy(p,user,ul+1); p+=ul+1;
    memcpy(p,"database",9); p+=9; memcpy(p,db,dl+1); p+=dl+1;
    *p++='\0';
    return (ssize_t)ml;
}

/* ---- vtable: generate_error ---- */

/**
 * @brief Build a minimal PostgreSQL ErrorResponse wire message.
 *
 * Generates an 'E' message with fields:
 *   'S' SEVERITY    = "ERROR"
 *   'V' SEVERITY_NP = "ERROR"
 *   'C' SQLSTATE    = code
 *   'M' MESSAGE     = msg
 *   \0  terminator
 *
 * Used by the engine to synthesise a client-facing error response when
 * the backend is unavailable or when the engine itself detects an error
 * (e.g. pool exhaustion, auth failure, route policy rejection).
 *
 * @param code  5-character SQLSTATE code string (e.g. "08006").
 * @param msg   Human-readable error message.
 * @param buf   Output buffer.
 * @param blen  Buffer capacity.
 * @return Total bytes written, or -1 if the buffer is too small.
 */
static ssize_t pgf_gen_error(void* v, const char* code, const char* msg,
                              uint8_t* buf, size_t blen) {
    (void)v;
    size_t cl=strlen(code), ml=strlen(msg);
    size_t total = 1+4+1+6+1+6+1+cl+1+1+ml+1+1;
    if (blen < total) return -1;
    uint8_t* p = buf;
    *p++='E'; wr32(p,(uint32_t)(total-1)); p+=4;
    *p++='S'; memcpy(p,"ERROR",6); p+=6;
    *p++='V'; memcpy(p,"ERROR",6); p+=6;
    *p++='C'; memcpy(p,code,cl+1); p+=cl+1;
    *p++='M'; memcpy(p,msg,ml+1); p+=ml+1;
    *p++='\0';
    return (ssize_t)total;
}

/* ============================================================================
 * Plugin API Extensions (Phase 5)
 * ============================================================================ */

/**
 * @brief Fill the plugin info struct with PostgreSQL capabilities.
 *
 * Populates @p out with the plugin name, default port (5432), API version,
 * and all supported capability flags (text protocol, extended query,
 * prepared-statement detection, consistency tokens, SCRAM/MD5 auth, etc.).
 *
 * @param[out] out  Plugin info struct to fill.
 */
static void pgf_get_info(keel_plugin_info_t* out) {
    out->name        = "postgres";
    out->default_port = 5432;
    out->api_version = KEEL_PLUGIN_API_V1;
    out->capabilities =
        KEEL_PCAP_TEXT_PROTOCOL     |
        KEEL_PCAP_EXTENDED_QUERY    |
        KEEL_PCAP_PREPARED_DETECT   |
        KEEL_PCAP_CONSISTENCY_TOKEN |
        KEEL_PCAP_POSITION_WAIT     |
        KEEL_PCAP_STREAMING_COPY    |
        KEEL_PCAP_AUTH_SCRAM        |
        KEEL_PCAP_AUTH_MD5          |
        KEEL_PCAP_STATE_PROFILE     |
        KEEL_PCAP_SELECTIVE_RESET   |
        KEEL_PCAP_DISCARD_ALL       |
        KEEL_PCAP_CANCEL_REQUEST    |
        KEEL_PCAP_SSL               |
        KEEL_PCAP_PROBE_HEALTH;
}

/* ---- classify_error: parse PG 'E' (ErrorResponse) ---- */

/**
 * @brief Classify a PostgreSQL ErrorResponse ('E') message.
 *
 * Parses the 'E' wire message and extracts the SQLSTATE code, message
 * text, and severity, then maps them to the keel error taxonomy:
 *
 *  SQLSTATE class 08xxx (connection_exception)
 *    → KEEL_ERR_BACKEND_FATAL, connection_ok = false
 *
 *  SQLSTATE class 53xxx (insufficient_resources, e.g. 53200 out_of_memory)
 *    → KEEL_ERR_RESOURCE_LIMIT
 *
 *  SQLSTATE class 57xxx (operator_intervention, e.g. 57014 query_canceled)
 *    → KEEL_ERR_BACKEND_FATAL, connection_ok = false
 *
 *  SQLSTATE class 28xxx (invalid_authorization_specification)
 *    → KEEL_ERR_AUTH_FAILURE
 *
 *  SQLSTATE 40001 (serialization_failure)
 *    → KEEL_ERR_IDEMPOTENT_SAFE (safe to retry on a read replica)
 *
 *  SQLSTATE class 40xxx (transaction_rollback, excluding 40001)
 *    → KEEL_ERR_TRANSIENT
 *
 *  Severity FATAL / PANIC
 *    → KEEL_ERR_BACKEND_FATAL, connection_ok = false (overrides class)
 *
 *  All other errors → KEEL_ERR_SQL_ERROR (recoverable, forward to client)
 *
 * @param data  Raw 'E' message bytes.
 * @param len   Length of message.
 * @param out   OUTPUT: populated error info struct.
 * @return 0 on success, -1 if message is not a valid 'E' message.
 */
static int pgf_classify_error(void* vctx, const uint8_t* data, size_t len,
                               keel_error_info_t* out) {
    (void)vctx;
    if (!data || len < 6 || data[0] != 'E') return -1;

    /* Walk ErrorResponse fields: tag(1) len(4) { field_type(1) value\0 }* \0 */
    memset(out, 0, sizeof(*out));
    out->error_class  = KEEL_ERR_SQL_ERROR;
    out->connection_ok = true;

    const uint8_t* p   = data + 5;
    const uint8_t* end = data + len;

    while (p < end && *p != '\0') {
        uint8_t field = *p++;
        const char* val = (const char*)p;
        size_t vl = strnlen(val, (size_t)(end - p));
        p += vl + 1;

        switch (field) {
        case 'C':  /* SQLSTATE code */
            out->sqlstate = val;
            /* Classify by first two characters (class) */
            if (vl >= 2) {
                char c0 = val[0], c1 = val[1];
                if (c0 == '0' && c1 == '8')       /* 08xxx = connection_exception */
                    { out->error_class = KEEL_ERR_BACKEND_FATAL; out->connection_ok = false; }
                else if (c0 == '5' && c1 == '3')   /* 53xxx = insufficient_resources */
                    out->error_class = KEEL_ERR_RESOURCE_LIMIT;
                else if (c0 == '5' && c1 == '7')   /* 57xxx = operator_intervention */
                    { out->error_class = KEEL_ERR_BACKEND_FATAL; out->connection_ok = false; }
                else if (c0 == '2' && c1 == '8')   /* 28xxx = invalid_auth */
                    out->error_class = KEEL_ERR_AUTH_FAILURE;
                else if (c0 == '4' && c1 == '0') { /* 40xxx = transaction_rollback */
                    /* 40001 = serialization_failure → idempotent-safe if read */
                    if (vl >= 5 && val[2] == '0' && val[3] == '0' && val[4] == '1')
                        out->error_class = KEEL_ERR_IDEMPOTENT_SAFE;
                    else
                        out->error_class = KEEL_ERR_TRANSIENT;
                }
                else if (c0 == '0' && c1 == '0')   /* 00xxx = successful_completion */
                    out->error_class = KEEL_ERR_SQL_ERROR;  /* shouldn't be in 'E' */
            }
            break;
        case 'M':  /* message */
            out->message = val;
            break;
        case 'S':  /* severity */
            if (vl >= 5 && (memcmp(val, "FATAL", 5) == 0 ||
                            memcmp(val, "PANIC", 5) == 0)) {
                out->error_class = KEEL_ERR_BACKEND_FATAL;
                out->connection_ok = false;
            }
            break;
        default:
            break;
        }
    }
    return 0;
}

/**
 * @brief Issue a cleanup SQL command on an existing backend connection.
 *
 * Unlike `build_cleanup()` which generates a cleanup query for a
 * connection being RETURNED to the pool, `cleanup_slot()` is called
 * synchronously with a live fd.  It sends the SQL and drains the
 * response before returning.
 *
 * Three modes:
 *
 *  KEEL_CLEANUP_SELECTIVE: generates `RESET key` for each parameter in
 *    `profile`.  Used for lightweight per-connection state reconciliation
 *    when only a few SET parameters differ.
 *
 *  KEEL_CLEANUP_FULL: sends `DISCARD ALL` to fully reset session state.
 *    Must NOT be combined with ROLLBACK in the same Simple Query because
 *    PostgreSQL requires DISCARD ALL to run at top-level (isTopLevel=true);
 *    a multi-statement query implies isTopLevel=false and causes a PG error.
 *
 *  KEEL_CLEANUP_DESTROY: returns 0 immediately.  Caller will close the fd.
 *
 * @return Bytes written to buf (0 = nothing to do, -1 = error/overflow).
 */
static ssize_t pgf_cleanup_slot(void* vctx, int be_fd,
                                 const struct state_profile* profile,
                                 keel_cleanup_opts_t opts,
                                 uint8_t* buf, size_t buf_len) {
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    (void)be_fd;

    keel_time_t t_start = keel_time_now();

    const char* sql;
    switch (opts.mode) {
    case KEEL_CLEANUP_SELECTIVE:
        /* If profile is NULL or empty, nothing to clean */
        if (!profile || profile->count == 0)
            return 0;
        /* Generate selective RESET: "RESET param1; RESET param2; ..." */
        {
            char sql_buf[2048];
            size_t pos = 0;
            for (size_t i = 0; i < profile->count && pos < sizeof(sql_buf) - 32; i++) {
                int written = snprintf(sql_buf + pos, sizeof(sql_buf) - pos,
                                       "RESET %s; ", profile->sorted_params[i].key);
                if (written > 0) pos += (size_t)written;
            }
            if (pos == 0) return 0;
            /* Wrap in Simple Query message */
            size_t sql_len = pos;
            size_t total = 1 + 4 + sql_len + 1;
            if (buf_len < total) return -1;
            buf[0] = 'Q';
            wr32(buf + 1, (uint32_t)(4 + sql_len + 1));
            memcpy(buf + 5, sql_buf, sql_len);
            buf[5 + sql_len] = '\0';
            return (ssize_t)total;
        }
    case KEEL_CLEANUP_FULL:
        /* DISCARD ALL clears prepared statements, temp tables, SET params.
         * Must be sent as a single-statement query (not combined with
         * ROLLBACK) because multi-statement queries cause isTopLevel=false
         * which makes PostgreSQL reject DISCARD ALL. */
        sql = "DISCARD ALL";
        break;
    case KEEL_CLEANUP_DESTROY:
    default:
        return 0;  /* Caller will close the connection */
    }

    /* Wrap sql in Simple Query message */
    {
        size_t sl = strlen(sql) + 1;
        size_t total = 1 + 4 + sl;
        if (buf_len < total) return -1;
        buf[0] = 'Q';
        wr32(buf + 1, (uint32_t)(4 + sl));
        memcpy(buf + 5, sql, sl);
        if (ctx) {
            ctx->metrics_cleanup_count++;
            ctx->metrics_cleanup_lat_ns = (uint64_t)(keel_time_now() - t_start);
        }
        return (ssize_t)total;
    }
}

typedef struct pg_cleanup_drain_state {
    uint8_t  hdr[5];
    uint8_t  hdr_len;
    uint8_t  msg_type;
    uint8_t  rfq_status;
    uint32_t msg_len;
    uint32_t msg_seen;
} pg_cleanup_drain_state_t;

static int pg_cleanup_finish_msg(const pg_cleanup_drain_state_t* st)
{
    switch (st->msg_type) {
    case 'C': /* CommandComplete */
    case 'N': /* NoticeResponse */
    case 'S': /* ParameterStatus */
        return 0;
    case 'Z': /* ReadyForQuery */
        if (st->msg_len != 5)
            return -1;
        return st->rfq_status == 'I' ? 1 : -1;
    case 'E': /* ErrorResponse */
    case 'A': /* NotificationResponse: async payload has no session owner */
        return -1;
    default:
        return -1;
    }
}

static void pg_cleanup_reset_msg(pg_cleanup_drain_state_t* st)
{
    memset(st->hdr, 0, sizeof(st->hdr));
    st->hdr_len = 0;
    st->msg_type = 0;
    st->rfq_status = 0;
    st->msg_len = 0;
    st->msg_seen = 0;
}

static keel_proto_drain_result_t pgf_drain_cleanup_response(
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

    pg_cleanup_drain_state_t* st = (pg_cleanup_drain_state_t*)state->opaque;
    size_t pos = 0;

    while (pos < len) {
        if (st->hdr_len < sizeof(st->hdr)) {
            size_t need = sizeof(st->hdr) - st->hdr_len;
            size_t take = (len - pos < need) ? (len - pos) : need;
            memcpy(st->hdr + st->hdr_len, data + pos, take);
            st->hdr_len += (uint8_t)take;
            pos += take;

            if (st->hdr_len < sizeof(st->hdr)) {
                if (consumed_out)
                    *consumed_out = pos;
                return KEEL_PROTO_DRAIN_MORE;
            }

            st->msg_type = st->hdr[0];
            st->msg_len = rd32(st->hdr + 1);
            st->msg_seen = 0;
            st->rfq_status = 0;
            if (st->msg_len < 4) {
                if (consumed_out)
                    *consumed_out = pos;
                return KEEL_PROTO_DRAIN_ERROR;
            }
        }

        uint32_t payload_len = st->msg_len - 4;
        uint32_t remaining = payload_len - st->msg_seen;
        size_t take = (len - pos < remaining) ? (len - pos) : remaining;

        if (st->msg_type == 'Z' && st->msg_seen == 0 && take > 0)
            st->rfq_status = data[pos];

        pos += take;
        st->msg_seen += (uint32_t)take;

        if (st->msg_seen < payload_len) {
            if (consumed_out)
                *consumed_out = pos;
            return KEEL_PROTO_DRAIN_MORE;
        }

        int gate = pg_cleanup_finish_msg(st);
        pg_cleanup_reset_msg(st);
        if (gate == 1) {
            if (consumed_out)
                *consumed_out = pos;
            return KEEL_PROTO_DRAIN_COMPLETE;
        }
        if (gate < 0) {
            if (consumed_out)
                *consumed_out = pos;
            return KEEL_PROTO_DRAIN_ERROR;
        }
    }

    if (consumed_out)
        *consumed_out = pos;
    return KEEL_PROTO_DRAIN_MORE;
}

/* ---- probe_backend: health check via PG ---- */

/**
 * @brief Issue a lightweight health-check query to a backend connection.
 *
 * Sends `SELECT 1` as a Simple Query ('Q') and reads the response.
 * Scans the response buffer for a ReadyForQuery ('Z') message to confirm
 * the backend is alive and not in an error state.
 *
 * This is called synchronously on a pool connection before it is returned
 * to a session, and also periodically by the health-check timer to evict
 * dead backends from the pool before clients notice them.
 *
 * @param vctx   Protocol context (unused; may be NULL).
 * @param be_fd  Connected backend socket fd (blocking send+recv).
 * @param out    OUTPUT: populated probe result (out->alive = true on success).
 * @return 0 on success (even if backend has errors), -1 on I/O failure.
 */
static int pgf_probe_backend(void* vctx, int be_fd,
                              keel_probe_result_t* out) {
    (void)vctx;
    memset(out, 0, sizeof(*out));
    out->replication_lag_ms = -1;

    /* Build: SELECT 1 */
    const char* sql = "SELECT 1";
    uint8_t qbuf[32];
    size_t sl = strlen(sql) + 1;
    size_t total = 1 + 4 + sl;
    qbuf[0] = 'Q';
    wr32(qbuf + 1, (uint32_t)(4 + sl));
    memcpy(qbuf + 5, sql, sl);

    ssize_t w = send(be_fd, qbuf, total, MSG_NOSIGNAL);
    if (w != (ssize_t)total) return -1;

    /* Read response: expect at least RowDescription + DataRow + CommandComplete + ReadyForQuery */
    uint8_t rbuf[512];
    ssize_t r = recv(be_fd, rbuf, sizeof(rbuf), 0);
    if (r <= 0) return -1;

    out->alive = true;

    /* Scan for ReadyForQuery ('Z') to confirm success */
    for (ssize_t i = 0; i < r; i++) {
        if (rbuf[i] == 'Z' && (i + 5) <= r) {
            out->alive = true;
            /* Return success — caller can use get_backend_metadata for
             * is_primary/is_replica if needed */
            return 0;
        }
    }
    return 0;
}

/* ---- get_backend_metadata: issue SELECT pg_is_in_recovery(), version() ---- */

/**
 * @brief Query basic backend metadata needed for pool routing decisions.
 *
 * Sends:
 *   `SELECT pg_is_in_recovery()::int, version()`
 *
 * Parses the DataRow response to populate:
 *  - out->in_recovery  — true if backend is a replica (standby)
 *  - out->read_only    — same as in_recovery
 *  - out->server_version — full PostgreSQL version string
 *  - out->database, out->user, out->backend_pid — from ctx (already known)
 *
 * This function is called once per backend connection, immediately after
 * pool handshake, to tag the connection as primary-eligible or
 * replica-only.  The result is cached in keel_backend_conn_t and never
 * re-queried.
 *
 * @param vctx   Protocol context for the backend connection.
 * @param be_fd  Connected backend socket fd (blocking send+recv).
 * @param out    OUTPUT: populated metadata struct.
 * @return 0 on success, -1 if I/O failed or DataRow was not parseable.
 */
static int pgf_get_backend_metadata(void* vctx, int be_fd,
                                     keel_backend_meta_t* out)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    memset(out, 0, sizeof(*out));

    /* Populate fields that are already known from the session context */
    if (ctx) {
        if (ctx->database[0])
            snprintf(out->database, sizeof(out->database), "%s", ctx->database);
        if (ctx->username[0])
            snprintf(out->user, sizeof(out->user), "%s", ctx->username);
        out->backend_pid = ctx->backend_pid;
    }

    /* Build: SELECT pg_is_in_recovery()::int, version() */
    const char* sql = "SELECT pg_is_in_recovery()::int, version()";
    uint8_t qbuf[128];
    size_t sl    = strlen(sql) + 1;
    size_t total = 1 + 4 + sl;
    qbuf[0] = 'Q';
    wr32(qbuf + 1, (uint32_t)(4 + sl));
    memcpy(qbuf + 5, sql, sl);

    if (send(be_fd, qbuf, total, MSG_NOSIGNAL) != (ssize_t)total)
        return -1;

    /* Read response — RowDescription + DataRow + CommandComplete + ReadyForQuery */
    uint8_t rbuf[1024];
    ssize_t r = recv(be_fd, rbuf, sizeof(rbuf), 0);
    if (r <= 0) return -1;

    /* Scan for DataRow ('D'):
     *   'D'  type (1)
     *   int32_be length (4)
     *   int16_be ncols (2)
     *   col 0: int32_be len (4) + data    ← pg_is_in_recovery()::int  ('0'/'1')
     *   col 1: int32_be len (4) + data    ← version() string           */
    for (ssize_t i = 0; i + 7 < r; i++) {
        if (rbuf[i] != 'D') continue;

        uint16_t ncols = (uint16_t)(((uint16_t)rbuf[i + 5] << 8) | rbuf[i + 6]);
        if (ncols < 1) continue;

        ssize_t pos = i + 7;   /* start of first column */

        /* col 0: pg_is_in_recovery()::int */
        if (pos + 4 > r) break;
        int32_t c0_len = (int32_t)rd32(rbuf + pos);
        pos += 4;
        if (c0_len > 0 && pos + c0_len <= r) {
            out->in_recovery = (rbuf[pos] == '1');
            out->read_only   = out->in_recovery;
            pos += c0_len;
        } else if (c0_len > 0) {
            break;
        }

        /* col 1: version() */
        if (ncols < 2 || pos + 4 > r) break;
        int32_t c1_len = (int32_t)rd32(rbuf + pos);
        pos += 4;
        if (c1_len > 0 && pos + c1_len <= r) {
            size_t copy = (size_t)c1_len < sizeof(out->server_version) - 1
                        ? (size_t)c1_len : sizeof(out->server_version) - 1;
            memcpy(out->server_version, rbuf + pos, copy);
            out->server_version[copy] = '\0';
        }
        return 0;
    }
    return -1;
}

/* ---- get_metrics: return plugin instrumentations ---- */

/**
 * @brief Return protocol-level performance metrics for this session context.
 *
 * Copies per-session counters from pg_flow_ctx_t into the generic
 * keel_plugin_metrics_t struct for aggregation by the stats subsystem.
 *
 * Counters tracked:
 *  - state_changes            — number of successful SET-based state syncs
 *  - consistency_token_fetches— times pgf_capture_consistency_token() ran
 *  - consistency_token_lat_ns — last successful token-fetch round-trip (ns)
 *  - cleanup_count            — times pgf_cleanup_slot() was called
 *  - cleanup_lat_ns           — last cleanup SQL-build latency (ns)
 *  - classify_count           — times pgf_classify_error() was called
 *  - errors_classified        — times classify found a non-OK error class
 *
 * @return Always 0.
 */
static int pgf_get_metrics(void* vctx, keel_plugin_metrics_t* out) {
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    memset(out, 0, sizeof(*out));
    if (!ctx) return 0;
    out->state_changes            = ctx->metrics_state_changes;
    out->consistency_token_fetches= ctx->metrics_consistency_fetches;
    out->consistency_token_lat_ns = ctx->metrics_consistency_token_lat_ns;
    out->cleanup_count            = ctx->metrics_cleanup_count;
    out->cleanup_lat_ns           = ctx->metrics_cleanup_lat_ns;
    out->classify_count           = ctx->metrics_classify_count;
    out->errors_classified        = ctx->metrics_errors_classified;
    return 0;
}

/* ---- pgf_notify_write_lsn: store the latest captured write LSN ---- */

static void pgf_notify_write_lsn(void* vctx, const char* lsn) {
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    if (!ctx || !lsn) return;
    strncpy(ctx->keel_write_lsn, lsn, sizeof(ctx->keel_write_lsn) - 1);
    ctx->keel_write_lsn[sizeof(ctx->keel_write_lsn) - 1] = '\0';
}

/* ---- get_stmt_replay: build replay buffer for PS virtualization (spec §17) ---- */

/**
 * @brief Build the prepared-statement replay wire buffer for a new backend.
 *
 * When the PS virtualization mode is active (ps_mode == VIRTUALIZE) and a
 * session is assigned a new backend connection that has a different
 * stmt_set_hash, all named prepared statements must be replayed
 * (re-parsed) on the new connection before the pending client message can
 * be forwarded.
 *
 * The function performs a **two-pass** build over `ctx->stmt_cache[]`:
 *
 *  **Pass 1**: Iterate over all cache slots.  Count the total byte length
 *    of all CONFIRMED (e->confirmed == true) named prepared statement
 *    Parse wire messages stored in e->wire_msg.  Unconfirmed statements
 *    (in-flight awaiting ParseComplete) are excluded — they will be
 *    forwarded via the normal query path when they arrive.
 *
 *  **Pass 2**: Allocate a buffer and copy all Parse messages into it in
 *    cache-slot order.  Then append a Sync ('S' + uint32_be 4) at the end.
 *
 * **Why Sync is required**: PostgreSQL Extended Query protocol does not
 * flush ParseComplete responses until a Sync message is received.  Without
 * Sync, the backend queues all responses indefinitely.  The engine's
 * WAIT_STMT_REPLAY path counts '1' (ParseComplete) responses; without Sync
 * it would never receive them and would deadlock on io_uring recv.
 *
 * **Hash-only probe**: If replay_buf_out is NULL, the function sets only
 * `*hash_out` and returns immediately.  This is used for fast pool-borrow
 * decisions (is this backend's stmt_set_hash a match?  If so skip replay).
 *
 * @param replay_buf_out  OUTPUT: heap-allocated replay buffer (caller frees).
 * @param replay_len_out  OUTPUT: length of replay buffer in bytes.
 * @param stmt_count_out  OUTPUT: number of Parse messages in the buffer.
 * @param hash_out        OUTPUT: current session stmt-set hash.
 * @return 0 on success, -1 on malloc failure.
 */
static int pgf_get_stmt_replay(void* vctx,
                                uint8_t** replay_buf_out,
                                size_t*   replay_len_out,
                                uint32_t* stmt_count_out,
                                uint64_t* hash_out)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;

    /* Support hash-only probe: callers may pass NULL for buf/len/count. */
    if (replay_buf_out) *replay_buf_out = NULL;
    if (replay_len_out) *replay_len_out = 0;
    if (stmt_count_out) *stmt_count_out = 0;
    if (hash_out)       *hash_out       = ctx->session_stmt_hash;

    fprintf(stderr, "[DBG-GETSRP] session_stmt_hash=%016llx full=%d semantic_unknown=%d datestyle='%s'\n",
        (unsigned long long)ctx->session_stmt_hash, (int)(replay_buf_out != NULL),
        (int)ctx->stmt_semantic_unknown, ctx->stmt_datestyle);

    if (ctx->stmt_semantic_unknown) {
        if (hash_out) *hash_out = 0;
        return 0;
    }

    /* If caller only wants the hash, stop here. */
    if (!replay_buf_out)
        return 0;

    if (ctx->session_stmt_hash == 0)
        return 0;   /* No named prepared statements — nothing to replay */

    /* Build a simple-query semantic context prefix so parse-time-sensitive
     * GUCs (DateStyle, TimeZone, search_path, etc.) are restored on the
     * fresh backend BEFORE the Parse messages are processed.
     *
     * The Q(SET ...) message produces CommandComplete('C') + ReadyForQuery('Z')
     * responses that arrive before the ParseComplete stream.  The engine's
     * WAIT_STMT_REPLAY drain loop already handles this correctly: the
     * ParseComplete counter only decrements on '1' messages, so 'C' and 'Z'
     * are silently skipped during counting.  The final 'Z' from the Sync
     * message is caught by the rfq_pending scan. */
    char     sync_sql[2048];
    size_t   sync_sql_len = 0;
#define APPEND_SYNC_SQL(...) \
    do { \
        int _n = snprintf(sync_sql + sync_sql_len, \
                          sizeof(sync_sql) - sync_sql_len, __VA_ARGS__); \
        if (_n > 0 && (size_t)_n < sizeof(sync_sql) - sync_sql_len) \
            sync_sql_len += (size_t)_n; \
    } while (0)

    if (ctx->stmt_search_path[0] != '\0')
        APPEND_SYNC_SQL("SET search_path = %s;", ctx->stmt_search_path);
    if (ctx->stmt_timezone[0] != '\0')
        APPEND_SYNC_SQL("SET timezone = '%s';", ctx->stmt_timezone);
    if (ctx->stmt_datestyle[0] != '\0')
        APPEND_SYNC_SQL("SET datestyle = '%s';", ctx->stmt_datestyle);
    if (ctx->stmt_intervalstyle[0] != '\0')
        APPEND_SYNC_SQL("SET intervalstyle = '%s';", ctx->stmt_intervalstyle);
    if (ctx->stmt_standard_conforming_strings[0] != '\0')
        APPEND_SYNC_SQL("SET standard_conforming_strings = '%s';",
                        ctx->stmt_standard_conforming_strings);
    if (ctx->stmt_backslash_quote[0] != '\0')
        APPEND_SYNC_SQL("SET backslash_quote = '%s';", ctx->stmt_backslash_quote);
    if (ctx->stmt_escape_string_warning[0] != '\0')
        APPEND_SYNC_SQL("SET escape_string_warning = '%s';",
                        ctx->stmt_escape_string_warning);
    if (ctx->stmt_default_tablespace[0] != '\0')
        APPEND_SYNC_SQL("SET default_tablespace = '%s';", ctx->stmt_default_tablespace);
    if (ctx->stmt_temp_tablespaces[0] != '\0')
        APPEND_SYNC_SQL("SET temp_tablespaces = '%s';", ctx->stmt_temp_tablespaces);
    if (ctx->stmt_default_table_access_method[0] != '\0')
        APPEND_SYNC_SQL("SET default_table_access_method = '%s';",
                        ctx->stmt_default_table_access_method);
    if (ctx->stmt_row_security[0] != '\0')
        APPEND_SYNC_SQL("SET row_security = '%s';", ctx->stmt_row_security);
#undef APPEND_SYNC_SQL

    if (sync_sql_len > 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
            "stmt replay semantic prefix: '%.*s'",
            (int)sync_sql_len, sync_sql);
    }

    /* Pass 1: count total bytes and number of replayable stmts */
    size_t   total_len  = 0;
    uint32_t stmt_count = 0;
    bool     has_parse_replay = false;
    size_t   sync_msg_len = 0;
    if (sync_sql_len > 0)
        sync_msg_len = 1 + 4 + sync_sql_len + 1;  /* 'Q' + len + sql + NUL */
    total_len += sync_msg_len;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        pg_stmt_entry_t* e = &ctx->stmt_cache[i];
        /* Only replay CONFIRMED stmts — unconfirmed ones are in-flight and
         * will be forwarded via stmt_replay_orig_msg, not the replay buf. */
        if (!e->valid || !e->confirmed || e->name[0] == '\0' ||
            !e->wire_msg || e->wire_msg_len == 0)
            continue;
        total_len  += e->wire_msg_len;
        stmt_count++;
        if (e->wire_msg[0] == 'P')
            has_parse_replay = true;
    }

    if (stmt_count == 0 || total_len == 0)
        return 0;

    if (has_parse_replay)
        total_len += 5;  /* Sync */

    /* Pass 2: build concatenated buffer */
    uint8_t* buf = (uint8_t*)keel_malloc(total_len);
    if (!buf) return -1;

    size_t offset = 0;

    /* Write the semantic context Q(SET...) prefix if any GUCs need restoration */
    if (sync_msg_len > 0) {
        buf[offset++] = 'Q';
        wr32(buf + offset, (uint32_t)(4 + sync_sql_len + 1));
        offset += 4;
        memcpy(buf + offset, sync_sql, sync_sql_len);
        offset += sync_sql_len;
        buf[offset++] = '\0';
    }

    for (int pass = 0; pass < 2; pass++) {
        uint8_t replay_kind = pass == 0 ? 'Q' : 'P';
        for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
            pg_stmt_entry_t* e = &ctx->stmt_cache[i];
            if (!e->valid || !e->confirmed || e->name[0] == '\0' ||
                !e->wire_msg || e->wire_msg_len == 0 ||
                e->wire_msg[0] != replay_kind)
                continue;
            memcpy(buf + offset, e->wire_msg, e->wire_msg_len);
            offset += e->wire_msg_len;
        }
    }

    if (has_parse_replay) {
        static const uint8_t pg_sync[] = { 'S', 0, 0, 0, 4 };
        memcpy(buf + offset, pg_sync, sizeof(pg_sync));
        offset += sizeof(pg_sync);
    }

    *replay_buf_out = buf;
    *replay_len_out = offset;
    *stmt_count_out = stmt_count;
    return 0;
}

static int pgf_get_stmt_compat_profile(void* vctx,
                                       keel_stmt_compat_profile_t* out)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    if (!ctx || !out)
        return -1;

    memset(out, 0, sizeof(*out));
    out->stmt_set_hash = ctx->session_stmt_hash;
    out->semantic_profile_hash = ctx->stmt_context_sig;
    out->schema_epoch = ctx->stmt_schema_epoch;
    out->role_hash = ctx->stmt_role_hash;
    out->search_path_hash = ctx->stmt_search_path_hash;
    out->guc_hash = ctx->stmt_guc_hash;
    out->semantic_unknown = ctx->stmt_semantic_unknown;
    return 0;
}

/* ---- ReadyForQuery generator (replaces hardcoded 'Z' in engine) ---- */

/**
 * @brief Build a ReadyForQuery ('Z') message indicating idle transaction status.
 *
 * The ReadyForQuery message is sent to the client after every query cycle
 * to signal that the session is ready for the next command.  The 'I'
 * transaction status byte means the backend is in idle state (outside any
 * transaction block).
 *
 * Wire format:
 *   'Z'  type (1 byte)
 *   0x00 0x00 0x00 0x05  length = 5 (includes 4-byte length field)
 *   'I'  tx_status  ('I'=idle, 'T'=in transaction, 'E'=error)
 *
 * @return 6 on success, -1 if buf_len < 6.
 */
static ssize_t pgf_gen_ready_for_query(void* ctx, uint8_t* buf, size_t buf_len) {
    (void)ctx;
    if (buf_len < 6) return -1;
    /* ReadyForQuery: tag='Z', length=5, tx_status='I' (idle) */
    buf[0] = 'Z';
    buf[1] = 0; buf[2] = 0; buf[3] = 0; buf[4] = 5;
    buf[5] = 'I';
    return 6;
}

static ssize_t pgf_build_commit_doubt_check(void* vctx,
                                            uint64_t xid,
                                            uint8_t* out_buf,
                                            size_t out_cap)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    if (!out_buf || out_cap < 32 || xid == 0)
        return -1;

    char sql[80];
    int sql_len = snprintf(sql, sizeof(sql), "SELECT txid_status(%llu)",
                           (unsigned long long)xid);
    if (sql_len < 0 || (size_t)sql_len >= sizeof(sql))
        return -1;

    uint32_t qlen = (uint32_t)(4 + (size_t)sql_len + 1);
    size_t msg_len = 1 + 4 + (size_t)sql_len + 1;
    if (msg_len > out_cap)
        return -1;

    out_buf[0] = 'Q';
    out_buf[1] = (uint8_t)(qlen >> 24);
    out_buf[2] = (uint8_t)(qlen >> 16);
    out_buf[3] = (uint8_t)(qlen >> 8);
    out_buf[4] = (uint8_t)(qlen);
    memcpy(out_buf + 5, sql, (size_t)sql_len);
    out_buf[5 + (size_t)sql_len] = '\0';

    if (ctx) {
        ctx->commit_doubt_check_active = true;
        ctx->commit_doubt_outcome = 0;
    }
    return (ssize_t)msg_len;
}

static ssize_t pgf_generate_commit_doubt_response(void* vctx,
                                                  keel_commit_doubt_reason_t reason,
                                                  uint64_t xid,
                                                  uint8_t* out_buf,
                                                  size_t out_cap)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    if (!out_buf || out_cap < 16)
        return -1;

    if (reason == KEEL_CIDR_RESOLVED_COMMITTED) {
        static const uint8_t kCommitOk[] = {
            'C', 0x00, 0x00, 0x00, 0x0b, 'C','O','M','M','I','T','\0',
            'Z', 0x00, 0x00, 0x00, 0x05, 'I'
        };
        if (sizeof(kCommitOk) > out_cap)
            return -1;
        memcpy(out_buf, kCommitOk, sizeof(kCommitOk));
        return (ssize_t)sizeof(kCommitOk);
    }

    const char* code = "08006";
    char msg[256];
    switch (reason) {
    case KEEL_CIDR_NO_XID:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: transaction outcome unknown (no XID captured)");
        break;
    case KEEL_CIDR_NO_RW_POOL:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: no RW pool — check txid_status(%llu) to resolve",
                 (unsigned long long)xid);
        break;
    case KEEL_CIDR_NO_CHECK_CONN:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: pool unavailable — check txid_status(%llu) to resolve",
                 (unsigned long long)xid);
        break;
    case KEEL_CIDR_CHECK_BUILD_FAIL:
    case KEEL_CIDR_CHECK_SEND_FAIL:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: XID-check failed — verify txid_status(%llu) manually",
                 (unsigned long long)xid);
        break;
    case KEEL_CIDR_RESOLVED_ABORTED:
        code = "40000";
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: transaction was rolled back");
        break;
    case KEEL_CIDR_RESOLVED_UNKNOWN:
    default:
        snprintf(msg, sizeof(msg),
                 "connection lost before COMMIT confirmation: outcome uncertain for XID %llu — check txid_status() manually",
                 (unsigned long long)xid);
        break;
    }

    ssize_t el = pgf_gen_error(ctx, code, msg, out_buf, out_cap);
    if (el <= 0)
        return -1;

    ssize_t zl = pgf_gen_ready_for_query(ctx, out_buf + (size_t)el, out_cap - (size_t)el);
    if (zl < 0)
        return -1;
    return el + zl;
}

/* ---- Anonymous mode vtable hook: rewrite_execute_anonymous ----
 *
 * Called by the engine when ps_mode == KEEL_PS_MODE_ANONYMOUS and a named
 * Bind message arrives.  Looks up stmt_name in the context's anon_map,
 * builds a one-shot anonymous Parse('', sql) followed by the original Bind
 * rewritten to reference the unnamed statement, and writes the result into
 * out_buf.
 *
 * This vtable hook exists as a public API for callers outside
 * postgres_flow.c; the JIT rewrite is also performed inline in
 * pgf_on_fe_msg for the common case.
 */
static ssize_t pgf_rewrite_execute_anonymous(
    void*          vctx,
    const char*    stmt_name,
    size_t         stmt_name_len,
    const uint8_t* bind_msg,
    size_t         bind_len,
    uint8_t*       out_buf,
    size_t         out_buf_len)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    if (!ctx || !stmt_name || !bind_msg || !out_buf) return -1;

    pg_anon_entry_t* ae = pg_anon_find(ctx->anon_map, stmt_name);
    if (!ae) return -1;   /* Unknown statement — caller falls back to hard-pin */

    /* Parse the Bind message: 'B' | int32 len | portal\0 | stmt_name\0 | tail */
    if (bind_len < 5) return -1;
    const char* portal     = (const char*)(bind_msg + 5);
    size_t portal_len      = strnlen(portal, bind_len - 5);
    size_t orig_stmt_off   = 5 + portal_len + 1;
    if (orig_stmt_off >= bind_len) return -1;
    const char* orig_stmt  = (const char*)(bind_msg + orig_stmt_off);
    size_t orig_stmt_len   = strnlen(orig_stmt, bind_len - orig_stmt_off);
    size_t tail_off        = orig_stmt_off + orig_stmt_len + 1;
    size_t tail_len        = (tail_off < bind_len) ? (bind_len - tail_off) : 0;
    (void)stmt_name_len;

    /* One-shot Parse: 'P' | len4 | '\0' | sql\0 | int16(0) */
    size_t parse_msg_len = 1 + 4 + 1 + ae->sql_len + 1 + 2;
    /* Rewritten Bind: 'B' | len4 | portal\0 | '\0' | tail */
    size_t bind_body     = portal_len + 1 + 1 + tail_len;
    size_t bind_msg_len  = 1 + 4 + bind_body;
    size_t total         = parse_msg_len + bind_msg_len;

    if (out_buf_len < total) return -1;

    uint8_t* wp = out_buf;

    /* Write anonymous Parse */
    *wp++ = 'P';
    wr32(wp, (uint32_t)(parse_msg_len - 1)); wp += 4;
    *wp++ = '\0';
    memcpy(wp, ae->sql, ae->sql_len); wp += ae->sql_len;
    *wp++ = '\0';
    *wp++ = 0; *wp++ = 0;  /* num_params = 0 */

    /* Write rewritten Bind */
    *wp++ = 'B';
    wr32(wp, (uint32_t)(bind_msg_len - 1)); wp += 4;
    memcpy(wp, portal, portal_len); wp += portal_len;
    *wp++ = '\0';
    *wp++ = '\0';           /* anonymous statement name */
    if (tail_len > 0) {
        memcpy(wp, bind_msg + tail_off, tail_len);
    }

    return (ssize_t)total;
}

static void pgf_captured_fe_pin_effects(void* vctx,
                                        const uint8_t* data,
                                        size_t len,
                                        keel_flow_pin_reason_t* pin_update,
                                        keel_flow_pin_reason_t* pin_clear)
{
    (void)vctx;
    if (pin_update)
        *pin_update = KEEL_FPIN_NONE;
    if (pin_clear)
        *pin_clear = KEEL_FPIN_NONE;
    if (!data || !pin_clear)
        return;

    size_t pos = 0;
    while (pos + 5 <= len) {
        uint8_t type = data[pos];
        uint32_t msg_len = rd32(data + pos + 1);
        if (msg_len < 4)
            return;

        size_t frame_len = 1u + (size_t)msg_len;
        if (pos + frame_len > len)
            return;

        if (type == 'S')
            *pin_clear |= KEEL_FPIN_EXTENDED_PROTO;

        pos += frame_len;
    }
}

/* ---- capture_consistency_token: inline SELECT pg_current_wal_lsn() ---- */

/**
 * @brief Capture the primary's current WAL LSN as a replication consistency token.
 *
 * Called by the engine on the BE path AFTER a WRITE/DDL query's ReadyForQuery
 * is received and the backend socket has been temporarily set to BLOCKING mode
 * (via fcntl F_SETFL ~O_NONBLOCK).  This ensures the synchronous send+recv
 * cycle does not leave stale bytes on the non-blocking io_uring event loop.
 *
 * Protocol:
 *   1. Send `SELECT pg_current_wal_lsn()` as a Simple Query on `be_fd`.
 *   2. Drain ALL response messages in a loop until a 'Z' ReadyForQuery
 *      is received.  Each message is individually framed:
 *        'T' RowDescription    → skip
 *        'D' DataRow           → extract column-0 text = LSN string ("A/B0000000")
 *        'C' CommandComplete   → skip
 *        'E' ErrorResponse     → log and return -1
 *        'Z' ReadyForQuery     → end of response, break loop
 *   3. Copy the LSN string into out->value.
 *
 * **Deferred capture pattern**: The caller (keel_engine_flow_on_be_data) sets
 * `capture_lsn_pending` during forward processing of the FE message; the
 * actual capture only runs here after query_complete.  This avoids the race
 * where the socket is non-blocking and a partial write of the SELECT query
 * would corrupt the message stream.
 *
 * @param vctx   Protocol context for the primary connection.
 * @param be_fd  Connected primary backend fd (MUST be blocking on entry).
 * @param out    OUTPUT: consistency token struct; out->value set to LSN string.
 * @return 0 on success; -1 on send/recv failure or LSN not found in response.
 */
static int pgf_capture_consistency_token(void* vctx, int be_fd,
                                          keel_consistency_token_t* out)
{
    pg_flow_ctx_t* ctx = (pg_flow_ctx_t*)vctx;
    memset(out, 0, sizeof(*out));
    if (ctx) ctx->metrics_consistency_fetches++;

    keel_time_t t_start = keel_time_now();

    /* Build simple query: SELECT pg_current_wal_lsn() */
    const char* sql = "SELECT pg_current_wal_lsn()";
    uint8_t qbuf[64];
    size_t sl    = strlen(sql) + 1;
    size_t total = 1 + 4 + sl;
    qbuf[0] = 'Q';
    wr32(qbuf + 1, (uint32_t)(4 + sl));
    memcpy(qbuf + 5, sql, sl);

    if (send(be_fd, qbuf, total, MSG_NOSIGNAL) != (ssize_t)total)
        return -1;

    /* Drain ALL backend messages until ReadyForQuery ('Z').
     *
     * This function is called with the socket in blocking mode (engine_flow
     * temporarily clears O_NONBLOCK after query_complete fires, when the
     * backend is known to be idle).  We MUST consume every byte of the
     * T+D+CommandComplete+Z response before returning, so that nothing
     * leaks into the next on_be_data call as a spurious result set.
     *
     * We read message-by-message (5-byte header + body) to correctly handle
     * cases where the response spans multiple TCP segments. */
    bool lsn_found = false;
    for (;;) {
        /* Read 5-byte message header: type(1) + length_be32(4) */
        uint8_t hdr[5];
        size_t  hdr_got = 0;
        while (hdr_got < 5) {
            ssize_t r = recv(be_fd, hdr + hdr_got, 5 - hdr_got, 0);
            if (r <= 0) return lsn_found ? 0 : -1;
            hdr_got += (size_t)r;
        }

        uint8_t  mtype  = hdr[0];
        uint32_t msglen = rd32(hdr + 1);   /* includes the 4-byte length field */
        if (msglen < 4) return -1;         /* malformed */
        uint32_t body_len = msglen - 4;

        /* Read body; split into normal path (fits in buffer) and drain-only
         * path (oversized — unlikely for wal_lsn responses, but be safe). */
        uint8_t body[4096];
        if (body_len <= (uint32_t)sizeof(body)) {
            /* Normal: read directly into body[] */
            uint32_t body_got = 0;
            while (body_got < body_len) {
                ssize_t r = recv(be_fd, body + body_got,
                                 body_len - body_got, 0);
                if (r <= 0) return lsn_found ? 0 : -1;
                body_got += (uint32_t)r;
            }
        } else {
            /* Oversized body: drain without storing */
            uint8_t drain[512];
            uint32_t left = body_len;
            while (left > 0) {
                uint32_t batch = left < (uint32_t)sizeof(drain)
                                     ? left : (uint32_t)sizeof(drain);
                ssize_t r = recv(be_fd, drain, batch, 0);
                if (r <= 0) return lsn_found ? 0 : -1;
                left -= (uint32_t)r;
            }
            /* body contents unavailable (drained) — skip field extraction */
            if (mtype == 'Z') return lsn_found ? 0 : -1;
            continue;
        }

        if (mtype == 'D' && !lsn_found && body_len >= 7) {
            /* DataRow: int16 ncols | per-col: int32 col_len + col_data */
            uint16_t ncols = (uint16_t)(((uint16_t)body[0] << 8) | body[1]);
            if (ncols == 1) {
                int32_t col_len = (int32_t)rd32(body + 2);
                if (col_len > 0 &&
                    col_len < (int32_t)KEEL_CONSISTENCY_TOKEN_MAX &&
                    (uint32_t)(6 + col_len) <= body_len) {
                    memcpy(out->value, body + 6, (size_t)col_len);
                    out->value[col_len] = '\0';
                    struct timespec ts;
                    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
                    out->captured_at_ns =
                        (uint64_t)ts.tv_sec * 1000000000ULL
                        + (uint64_t)ts.tv_nsec;
                    lsn_found = true;
                }
            }
        }

        if (mtype == 'Z') {
            /* ReadyForQuery — all messages drained, backend is idle */
            if (ctx && lsn_found) {
                ctx->metrics_consistency_token_lat_ns =
                    (uint64_t)(keel_time_now() - t_start);
            }
            return lsn_found ? 0 : -1;
        }
    }
}

/* ---- replica_reached_token: inline LSN comparison against replica ---- */

/**
 * @brief Check whether a replica has replayed at least up to a given WAL LSN.
 *
 * Sends:
 *   `SELECT (pg_last_wal_replay_lsn() >= $lsn::pg_lsn) OR
 *           pg_last_wal_replay_lsn() IS NULL`
 *
 * where `$lsn` is the string in `token->value` (e.g. "A/B0000000").
 * The COALESCE / IS NULL guard handles replicas where
 * `pg_last_wal_replay_lsn()` returns NULL (e.g. during the initial
 * connect window before the first log record is applied).
 *
 * Response parsing:
 *   Scans the recv buffer for a DataRow ('D') and reads column 0:
 *    - 't' (boolean text "true")  → *out_reached = true
 *    - 'f' (boolean text "false") → *out_reached = false
 *
 * **Empty token short-circuit**: If `token->value[0] == '\0'` (no write was
 * tracked), the function immediately sets *out_reached = true and returns 0.
 * This preserves zero overhead for read-only sessions.
 *
 * **timeout_ms** is accepted for API compatibility but is not yet honoured;
 * the function performs a single synchronous blocking send+recv.  A
 * non-blocking / timeout path is planned for a future milestone.
 *
 * @param vctx        Protocol context (unused; may be NULL).
 * @param replica_fd  Connected replica socket fd (blocking send+recv).
 * @param token       Consistency token from pgf_capture_consistency_token().
 * @param timeout_ms  Maximum wait time (currently unused).
 * @param out_reached OUTPUT: true if replica LSN ≥ token LSN.
 * @return 0 on success, -1 on I/O error or parse failure.
 */
static int pgf_replica_reached_token(void* vctx, int replica_fd,
                                      const keel_consistency_token_t* token,
                                      int timeout_ms,
                                      bool* out_reached)
{
    (void)vctx;
    *out_reached = false;

    /* Empty token means no write has been tracked — replica is trivially OK. */
    if (!token || token->value[0] == '\0') {
        *out_reached = true;
        return 0;
    }

    /* Apply socket-level send/recv timeouts so a slow replica cannot block
     * the calling thread indefinitely.  timeout_ms == 0 means "use the
     * system default" (i.e. we do not override it). */
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

#define RESTORE_TIMEOUTS() do { \
    if (timeout_set) { \
        setsockopt(replica_fd, SOL_SOCKET, SO_RCVTIMEO, &saved_rcv, sizeof(saved_rcv)); \
        setsockopt(replica_fd, SOL_SOCKET, SO_SNDTIMEO, &saved_snd, sizeof(saved_snd)); \
    } \
} while (0)

    /* Use COALESCE so this also works on the primary (where
     * pg_last_wal_replay_lsn() returns NULL because it is not in recovery). */
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT COALESCE(pg_last_wal_replay_lsn(),"
             " pg_current_wal_lsn()) >= '%s'::pg_lsn",
             token->value);

    uint8_t qbuf[576];
    size_t sl    = strlen(sql) + 1;
    size_t total = 1 + 4 + sl;
    qbuf[0] = 'Q';
    wr32(qbuf + 1, (uint32_t)(4 + sl));
    memcpy(qbuf + 5, sql, sl);

    if (send(replica_fd, qbuf, total, MSG_NOSIGNAL) != (ssize_t)total) {
        RESTORE_TIMEOUTS();
        return -1;
    }

    /* Drain ALL messages until ReadyForQuery ('Z') — same rationale as
     * pgf_capture_consistency_token: must not leave bytes in the socket. */
    bool result_found = false;
    for (;;) {
        uint8_t hdr[5];
        size_t  hdr_got = 0;
        while (hdr_got < 5) {
            ssize_t r = recv(replica_fd, hdr + hdr_got, 5 - hdr_got, 0);
            if (r <= 0) { RESTORE_TIMEOUTS(); return result_found ? 0 : -1; }
            hdr_got += (size_t)r;
        }

        uint8_t  mtype  = hdr[0];
        uint32_t msglen = rd32(hdr + 1);
        if (msglen < 4) { RESTORE_TIMEOUTS(); return -1; }
        uint32_t body_len = msglen - 4;

        uint8_t body[4096];
        if (body_len <= (uint32_t)sizeof(body)) {
            uint32_t body_got = 0;
            while (body_got < body_len) {
                ssize_t r = recv(replica_fd, body + body_got,
                                 body_len - body_got, 0);
                if (r <= 0) { RESTORE_TIMEOUTS(); return result_found ? 0 : -1; }
                body_got += (uint32_t)r;
            }
        } else {
            uint8_t drain[512];
            uint32_t left = body_len;
            while (left > 0) {
                uint32_t batch = left < (uint32_t)sizeof(drain)
                                     ? left : (uint32_t)sizeof(drain);
                ssize_t r = recv(replica_fd, drain, batch, 0);
                if (r <= 0) { RESTORE_TIMEOUTS(); return result_found ? 0 : -1; }
                left -= (uint32_t)r;
            }
            if (mtype == 'Z') { RESTORE_TIMEOUTS(); return result_found ? 0 : -1; }
            continue;
        }

        if (mtype == 'D' && !result_found && body_len >= 7) {
            uint16_t ncols = (uint16_t)(((uint16_t)body[0] << 8) | body[1]);
            if (ncols == 1) {
                int32_t col_len = (int32_t)rd32(body + 2);
                if (col_len >= 1 && (uint32_t)(6 + col_len) <= body_len) {
                    *out_reached = (body[6] == 't');
                    result_found = true;
                }
            }
        }

        if (mtype == 'Z') {
            RESTORE_TIMEOUTS();
            return result_found ? 0 : -1;
        }
    }
}

#undef RESTORE_TIMEOUTS

/* ============================================================================
 * COPY streaming vtable (M1)
 *
 * Implements begin_stream / stream_write / end_stream for PostgreSQL COPY
 * IN streaming.  The caller (engine) enters COPY mode, then calls
 * stream_write() for each data chunk and end_stream() when done.
 * ============================================================================ */

typedef struct pgf_stream_state {
    int      be_fd;      /**< Backend file descriptor to write COPY data to */
    bool     is_inbound; /**< true = COPY IN (client→backend) */
} pgf_stream_state_t;

static int pgf_begin_stream(void* vctx, void* session, int be_fd,
                             keel_stream_ctx_t** out)
{
    (void)vctx; (void)session;
    if (!out) return -1;

    keel_stream_ctx_t* sctx = keel_malloc(sizeof *sctx);
    if (!sctx) return -1;

    pgf_stream_state_t* st = keel_malloc(sizeof *st);
    if (!st) { keel_free(sctx); return -1; }

    st->be_fd      = be_fd;
    st->is_inbound = true; /* COPY IN: data flows from client to backend */

    sctx->plugin_data = st;
    sctx->is_inbound  = true;
    *out = sctx;
    return 0;
}

/**
 * @brief Write a COPY data chunk to the backend.
 *
 * Wraps @p buf in a PostgreSQL CopyData message ('d') and sends it to the
 * backend file descriptor stored in the stream context.
 */
static int pgf_stream_write(keel_stream_ctx_t* sctx, const void* buf, size_t n)
{
    if (!sctx || !sctx->plugin_data || !buf || n == 0) return -1;
    pgf_stream_state_t* st = (pgf_stream_state_t*)sctx->plugin_data;
    if (st->be_fd < 0) return -1;

    /* Build CopyData frame: 'd' + int32(4+n) + data */
    size_t frame_len = 1 + 4 + n;
    uint8_t* frame = keel_malloc(frame_len);
    if (!frame) return -1;

    frame[0] = 'd';
    uint32_t ml = (uint32_t)(4 + n);
    frame[1] = (uint8_t)(ml >> 24);
    frame[2] = (uint8_t)(ml >> 16);
    frame[3] = (uint8_t)(ml >> 8);
    frame[4] = (uint8_t)(ml);
    memcpy(frame + 5, buf, n);

    ssize_t sent = 0;
    size_t  rem  = frame_len;
    while (rem > 0) {
        ssize_t r = send(st->be_fd, frame + sent, rem, MSG_NOSIGNAL);
        if (r > 0) { sent += r; rem -= (size_t)r; }
        else if (r < 0 && errno == EINTR) continue;
        else { keel_free(frame); return -1; }
    }
    keel_free(frame);
    return 0;
}

/**
 * @brief Terminate a COPY IN stream with a CopyDone message.
 *
 * Sends 'c' (CopyDone) to the backend to signal end-of-data, then
 * frees the stream context.
 */
static int pgf_end_stream(keel_stream_ctx_t* sctx)
{
    if (!sctx) return -1;
    pgf_stream_state_t* st = (pgf_stream_state_t*)sctx->plugin_data;
    int rc = 0;

    if (st && st->be_fd >= 0) {
        /* CopyDone: 'c' int32(4) */
        uint8_t done[5] = { 'c', 0, 0, 0, 4 };
        ssize_t s = send(st->be_fd, done, sizeof done, MSG_NOSIGNAL);
        if (s != (ssize_t)sizeof done) rc = -1;
    }

    keel_free(sctx->plugin_data);
    keel_free(sctx);
    return rc;
}

/* ---- VTable ---- */
const keel_proto_flow_vtable_t keel_proto_flow_postgres = {
    .name             = "postgres",
    .default_port     = 5432,
    .generate_greeting = NULL,  /* PG: client speaks first */
    .create_context   = pgf_create,
    .destroy_context  = pgf_destroy,
    .frame_len        = pgf_frame_len,
    .on_fe_msg        = pgf_on_fe_msg,
    .on_be_msg        = pgf_on_be_msg,
    .is_data_frame    = pgf_is_data_frame,
    .fingerprint      = pgf_fingerprint,
    .build_state_sync = pgf_build_state_sync,
    .build_cleanup    = pgf_build_cleanup,
    .backend_reuse_gate = pgf_reuse_gate,
    .generate_startup = pgf_gen_startup,
    .generate_error   = pgf_gen_error,
    .generate_ready_for_query = pgf_gen_ready_for_query,
    /* Phase 5 optional extensions */
    .get_info              = pgf_get_info,
    .classify_error        = pgf_classify_error,
    .capture_consistency_token = pgf_capture_consistency_token,
    .replica_reached_token = pgf_replica_reached_token,
    .begin_stream          = pgf_begin_stream,  /* COPY IN streaming */
    .stream_write          = pgf_stream_write,
    .end_stream            = pgf_end_stream,
    .cleanup_slot          = pgf_cleanup_slot,
    .drain_cleanup_response = pgf_drain_cleanup_response,
    .probe_backend         = pgf_probe_backend,
    .get_backend_metadata  = pgf_get_backend_metadata,
    .get_metrics           = pgf_get_metrics,
    .notify_write_lsn      = pgf_notify_write_lsn,
    .build_commit_doubt_check = pgf_build_commit_doubt_check,
    .generate_commit_doubt_response = pgf_generate_commit_doubt_response,
    .get_stmt_replay           = pgf_get_stmt_replay,
    .get_stmt_compat_profile   = pgf_get_stmt_compat_profile,
    .rewrite_execute_anonymous = pgf_rewrite_execute_anonymous,
    .captured_fe_pin_effects   = pgf_captured_fe_pin_effects,
};
