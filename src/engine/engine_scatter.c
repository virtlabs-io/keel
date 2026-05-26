/**
 * @file engine_scatter.c
 * @brief Scatter-merge query execution for sharded KEEL deployments.
 *
 * This module implements synchronous scatter-merge by opening a blocking
 * TCP connection to each shard backend, executing the query, collecting
 * all rows into a keel_scatter_result_t, applying merge operations (agg,
 * group, sort, limit), and encoding the final result as PostgreSQL wire
 * protocol back to the client.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * GNU Affero General Public License v3.0
 */

#include "engine_scatter.h"
#include "keel/engine/backend_pool.h"
#include "keel/probe/probe_common.h"
#include "keel/protocol/postgres/pg_scatter.h"
#include "keel/log/log.h"
#include "keel/log/audit_log.h"
#include "keel/trace/trace.h"
#include "keel/mem/mem.h"
#include "keel/core/scatter_2pc.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pthread.h>
#include <inttypes.h>
#include <ctype.h>

/* ============================================================================
 * Configuration constants
 * ============================================================================ */

#define SCATTER_CONNECT_TIMEOUT_MS  5000
#define SCATTER_READ_TIMEOUT_MS    30000
#define SCATTER_BUF_SIZE           65536
#define SCATTER_SEND_BUF           65536
#define SCATTER_DEFAULT_MEM        (16 * 1024 * 1024)  /* 16 MB */

/* ============================================================================
 * SQL rewriting helpers
 * ============================================================================ */

/**
 * @brief Rewrite SELECT COUNT(DISTINCT col) ... → SELECT DISTINCT col ...
 *
 * Finds "COUNT(DISTINCT col)" at the beginning of the SELECT list and
 * rewrites to "DISTINCT col", so each shard returns its distinct values.
 * The FROM/WHERE/etc clauses are preserved as-is.
 * Returns 0 on success, -1 on failure.
 */
static int sc_rewrite_count_distinct_sql(const char* sql, char* out, size_t outsz,
                                          const char* col_name)
{
    /* Find "SELECT" */
    const char* p = sql;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (strncasecmp(p, "select", 6) != 0) return -1;
    p += 6;
    while (*p == ' ' || *p == '\t') p++;

    /* Find "COUNT(" */
    const char* count_start = p;
    if (strncasecmp(p, "count(", 6) != 0) return -1;
    p += 6;
    while (*p == ' ' || *p == '\t') p++;

    /* Expect "DISTINCT" */
    if (strncasecmp(p, "distinct", 8) != 0) return -1;
    p += 8;
    while (*p == ' ' || *p == '\t') p++;

    /* Skip the column name and closing paren */
    while (*p && *p != ')') p++;
    if (*p == ')') p++;

    /* Now p points to the rest of the query (FROM ...) */
    /* Build: SELECT DISTINCT col <rest> */
    int n = snprintf(out, outsz, "SELECT DISTINCT %s%s", col_name, p);
    if (n < 0 || (size_t)n >= outsz) return -1;
    return 0;
    (void)count_start;
}

/**
 * @brief Rewrite AVG(expr) → SUM(expr), COUNT(expr) in SQL text.
 *
 * Does a case-insensitive scan for AVG( and replaces each occurrence with
 * SUM(...), COUNT(...) where ... is the original argument (paren-balanced).
 * Also strips HAVING clauses that reference AVG — they will be re-applied
 * post-merge by the caller using keel_scatter_result_apply_having().
 *
 * Writes the result to @p out (NUL-terminated).  Returns 0 on success, -1
 * if the output buffer was too small.
 */
static int sc_rewrite_avg_sql(const char* sql, char* out, size_t outsz)
{
    const char* p = sql;
    char* dst = out;
    char* end = out + outsz - 1; /* leave room for NUL */

#define DST_APPEND(c) do { if (dst >= end) return -1; *dst++ = (c); } while(0)
#define DST_WRITE(s, n) do { if (dst + (n) > end) return -1; memcpy(dst, s, n); dst += (n); } while(0)

    while (*p) {
        /* Check for AVG( (case-insensitive) */
        if ((p[0] == 'A' || p[0] == 'a') &&
            (p[1] == 'V' || p[1] == 'v') &&
            (p[2] == 'G' || p[2] == 'g') &&
             p[3] == '(') {
            /* Check it's not part of a larger identifier */
            bool boundary = (p == sql) || !( (p[-1] >= 'a' && p[-1] <= 'z') ||
                                              (p[-1] >= 'A' && p[-1] <= 'Z') ||
                                              (p[-1] >= '0' && p[-1] <= '9') ||
                                               p[-1] == '_' );
            if (!boundary) { DST_APPEND(*p++); continue; }

            /* Find the matching closing paren */
            const char* arg_start = p + 4; /* skip "AVG(" */
            const char* q = arg_start;
            int depth = 1;
            while (*q && depth > 0) {
                if (*q == '(') depth++;
                else if (*q == ')') depth--;
                if (depth > 0) q++;
                else break;
            }
            if (depth != 0) { DST_APPEND(*p++); continue; } /* unbalanced */

            size_t arg_len = (size_t)(q - arg_start);

            /* Write SUM(arg), COUNT(arg) */
            DST_WRITE("SUM(", 4);
            DST_WRITE(arg_start, arg_len);
            DST_WRITE("), COUNT(", 9);
            DST_WRITE(arg_start, arg_len);
            DST_APPEND(')');

            p = q + 1; /* skip past AVG(...) */
            continue;
        }
        DST_APPEND(*p++);
    }
    *dst = '\0';
    return 0;

#undef DST_APPEND
#undef DST_WRITE
}

/**
 * @brief Strip a HAVING clause from SQL text.
 *
 * The HAVING clause will be re-evaluated post-merge by the scatter executor.
 * We remove it from the per-shard query so that the backends return all
 * partial groups (not filtered).
 *
 * Writes the result to @p out (NUL-terminated). Returns 0 on success.
 */
static int sc_strip_having(const char* sql, char* out, size_t outsz)
{
    /* Simple scan: find "HAVING" at word boundary, then skip everything
     * up to ORDER BY, LIMIT, or end-of-string. */
    const char* p = sql;
    size_t slen = strlen(sql);

    /* Find HAVING keyword */
    const char* having_pos = NULL;
    for (const char* s = p; s < sql + slen - 6; s++) {
        if ((s[0] == 'H' || s[0] == 'h') &&
            (s[1] == 'A' || s[1] == 'a') &&
            (s[2] == 'V' || s[2] == 'v') &&
            (s[3] == 'I' || s[3] == 'i') &&
            (s[4] == 'N' || s[4] == 'n') &&
            (s[5] == 'G' || s[5] == 'g') &&
            (s == sql || s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n') &&
            (s[6] == ' ' || s[6] == '\t' || s[6] == '\n')) {
            having_pos = s;
            break;
        }
    }

    if (!having_pos) {
        /* No HAVING — copy as-is */
        size_t n = slen < outsz - 1 ? slen : outsz - 1;
        memcpy(out, sql, n);
        out[n] = '\0';
        return 0;
    }

    /* Copy everything before HAVING */
    size_t before = (size_t)(having_pos - sql);
    if (before >= outsz) { out[0] = '\0'; return -1; }
    memcpy(out, sql, before);

    /* Find where HAVING clause ends: ORDER BY, LIMIT, OFFSET, or end */
    const char* after = having_pos + 6;
    const char* end_having = sql + slen; /* default: end of string */

    for (const char* s = after; s < sql + slen; s++) {
        if (s[0] == ' ' || s[0] == '\t' || s[0] == '\n') {
            const char* w = s + 1;
            while (*w == ' ' || *w == '\t' || *w == '\n') w++;
            if ((w[0]=='O'||w[0]=='o') && (w[1]=='R'||w[1]=='r') &&
                (w[2]=='D'||w[2]=='d') && (w[3]=='E'||w[3]=='e') &&
                (w[4]=='R'||w[4]=='r')) { end_having = s; break; }
            if ((w[0]=='L'||w[0]=='l') && (w[1]=='I'||w[1]=='i') &&
                (w[2]=='M'||w[2]=='m') && (w[3]=='I'||w[3]=='i') &&
                (w[4]=='T'||w[4]=='t')) { end_having = s; break; }
            if ((w[0]=='O'||w[0]=='o') && (w[1]=='F'||w[1]=='f') &&
                (w[2]=='F'||w[2]=='f') && (w[3]=='S'||w[3]=='s') &&
                (w[4]=='E'||w[4]=='e') && (w[5]=='T'||w[5]=='t')) { end_having = s; break; }
        }
    }

    size_t tail = (size_t)(sql + slen - end_having);
    if (before + tail >= outsz) { out[before] = '\0'; return -1; }
    memcpy(out + before, end_having, tail);
    out[before + tail] = '\0';
    return 0;
}

/**
 * @brief Strip trailing LIMIT [n] [OFFSET m] clauses from a SQL string.
 *
 * When GROUP BY is present in a scatter query, the LIMIT must **not** be
 * forwarded to individual shards.  Reason: each shard would independently
 * truncate its partial-group result to the top-N rows; groups with high
 * global aggregates but low per-shard row counts could be dropped before the
 * proxy has a chance to merge them.
 *
 * Example: `SELECT grp, SUM(val) … GROUP BY grp ORDER BY SUM(val) DESC LIMIT 3`
 *   - Shard 0: A=100, B=80, C=70, D=60  → shard top-3: A, B, C  (D dropped!)
 *   - Shard 1: A=10,  B=20, C=30, D=200 → shard top-3: D, C, B
 *   - Global merge: A=110, B=100, C=100, D=200 → global top-3: D, A, B
 *   - Without LIMIT strip: D is absent from shard-0 result → proxy never sees it.
 *
 * This function removes the LAST occurrence of LIMIT or OFFSET (whichever comes
 * first) at a word boundary, preserving the ORDER BY clause intact.  Both
 * keywords are always last in a top-level SELECT.
 *
 * @note This shares the same "no subquery tracking" simplification as
 *       sc_strip_having — consistent with the rest of this module.
 *
 * Returns 0 on success (even when nothing was stripped), -1 if the output
 * buffer is too small.
 */
static int sc_strip_limit_offset(const char* sql, char* out, size_t outsz)
{
    size_t slen = strlen(sql);
    const char* limit_pos  = NULL;
    const char* offset_pos = NULL;

    for (const char* s = sql; s < sql + slen; s++) {
        /* Require a word-start boundary */
        bool at_word = (s == sql ||
                        s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n');
        if (!at_word) continue;

        size_t left = (size_t)(sql + slen - s);

        /* LIMIT */
        if (left >= 5 &&
            (s[0]=='L'||s[0]=='l') && (s[1]=='I'||s[1]=='i') &&
            (s[2]=='M'||s[2]=='m') && (s[3]=='I'||s[3]=='i') && (s[4]=='T'||s[4]=='t') &&
            (left == 5 || s[5]==' '||s[5]=='\t'||s[5]=='\n')) {
            limit_pos = s;
        }
        /* OFFSET */
        else if (left >= 6 &&
            (s[0]=='O'||s[0]=='o') && (s[1]=='F'||s[1]=='f') && (s[2]=='F'||s[2]=='f') &&
            (s[3]=='S'||s[3]=='s') && (s[4]=='E'||s[4]=='e') && (s[5]=='T'||s[5]=='t') &&
            (left == 6 || s[6]==' '||s[6]=='\t'||s[6]=='\n')) {
            offset_pos = s;
        }
    }

    /* Cut at the earliest of the two detected keywords (removes both) */
    const char* cut = NULL;
    if (limit_pos && offset_pos)
        cut = (limit_pos < offset_pos) ? limit_pos : offset_pos;
    else if (limit_pos)
        cut = limit_pos;
    else if (offset_pos)
        cut = offset_pos;

    if (!cut) {
        /* Nothing to strip */
        if (slen + 1 > outsz) return -1;
        memcpy(out, sql, slen + 1);
        return 0;
    }

    /* Trim trailing whitespace before the cut point */
    while (cut > sql && (cut[-1]==' '||cut[-1]=='\t'||cut[-1]=='\n'))
        cut--;

    size_t keep = (size_t)(cut - sql);
    if (keep + 1 > outsz) return -1;
    memcpy(out, sql, keep);
    out[keep] = '\0';
    return 0;
}

/**
 * @brief Add extra aggregate expressions to the SELECT clause of an SQL query.
 *
 * Inserts ", expr1, expr2, ..." before the FROM keyword so the shard query
 * returns additional columns needed for HAVING evaluation post-merge.
 *
 * Returns 0 on success, -1 if the output buffer is too small or FROM not found.
 */
static int sc_add_extra_select_cols(const char* sql,
                                     const char (*exprs)[128], uint16_t nexpr,
                                     char* out, size_t outsz) {
    if (!nexpr || !exprs) {
        size_t n = strlen(sql);
        if (n >= outsz) return -1;
        memcpy(out, sql, n + 1);
        return 0;
    }
    size_t slen = strlen(sql);
    /* Find "FROM" at a word boundary */
    const char* from_pos = NULL;
    for (const char* s = sql; s < sql + slen - 4; s++) {
        bool bound = (s == sql || s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n');
        if (bound &&
            (s[0]=='F'||s[0]=='f') && (s[1]=='R'||s[1]=='r') &&
            (s[2]=='O'||s[2]=='o') && (s[3]=='M'||s[3]=='m') &&
            (s[4]==' '||s[4]=='\t'||s[4]=='\n')) {
            from_pos = s;
            break;
        }
    }
    if (!from_pos) return -1;

    /* Copy everything before FROM */
    size_t before = (size_t)(from_pos - sql);
    if (before >= outsz) return -1;
    memcpy(out, sql, before);
    char* dst = out + before;
    size_t rem = outsz - before;

    /* Append extra expressions */
    for (uint16_t i = 0; i < nexpr; i++) {
        int n = snprintf(dst, rem, ", %s", exprs[i]);
        if (n < 0 || (size_t)n >= rem) return -1;
        dst += n; rem -= (size_t)n;
    }

    /* Append FROM ... rest */
    size_t rest = slen - before;
    if (rest >= rem) return -1;
    memcpy(dst, from_pos, rest + 1);
    return 0;
}

/* ============================================================================
 * Outer-WHERE strip and ordered-aggregate SQL rewrite helpers
 * ============================================================================ */

/**
 * @brief Strip the outermost WHERE clause from a CTE or derived-table query.
 * Scans at paren-depth 0, removes WHERE … up to ORDER/LIMIT/GROUP/HAVING etc.
 */
static int sc_strip_outer_where(const char* sql, char* out, size_t outsz)
{
    const char* s      = sql;
    size_t      slen   = strlen(sql);
    int         depth  = 0;
    const char* wstart = NULL;
    const char* wend   = NULL;

    for (; *s; s++) {
        if (*s == '(') { depth++; continue; }
        if (*s == ')') { depth--; continue; }
        if (*s == '\'') { s++; while (*s && *s != '\'') s++; continue; }
        if (depth != 0) continue;

        if (!wstart &&
            (*s == 'W' || *s == 'w') &&
            (s == sql || s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n' || s[-1] == ')') &&
            s + 5 <= sql + slen &&
            (s[1]=='H'||s[1]=='h') && (s[2]=='E'||s[2]=='e') &&
            (s[3]=='R'||s[3]=='r') && (s[4]=='E'||s[4]=='e') &&
            (s[5]==' '||s[5]=='\t'||s[5]=='\n'||s[5]=='(')) {
            wstart = s;
            continue;
        }
        if (wstart) {
            static const char* const stops[] = {
                "ORDER","LIMIT","OFFSET","GROUP","HAVING",
                "UNION","INTERSECT","EXCEPT","FETCH",NULL
            };
            for (int i = 0; stops[i]; i++) {
                size_t kl = strlen(stops[i]);
                if (s + kl <= sql + slen &&
                    strncasecmp(s, stops[i], kl) == 0 &&
                    (s == sql || s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n') &&
                    (s[kl] == ' ' || s[kl] == '\t' || s[kl] == '\n' || s[kl] == '\0')) {
                    wend = s;
                    goto sc_where_found;
                }
            }
        }
    }
sc_where_found:;
    if (!wstart) {
        size_t n = slen < outsz - 1 ? slen : outsz - 1;
        memcpy(out, sql, n); out[n] = '\0';
        return 0;
    }
    if (!wend) wend = sql + slen;
    size_t pre = (size_t)(wstart - sql);
    while (pre > 0 && (sql[pre-1] == ' ' || sql[pre-1] == '\t' || sql[pre-1] == '\n'))
        pre--;
    size_t post = (size_t)(sql + slen - wend);
    if (pre + post + 2 >= outsz) { out[0] = '\0'; return -1; }
    memcpy(out, sql, pre);
    if (post > 0) {
        out[pre] = ' ';
        memcpy(out + pre + 1, wend, post);
        out[pre + 1 + post] = '\0';
    } else {
        out[pre] = '\0';
    }
    return 0;
}

/**
 * @brief Find the byte range [*start, *end) of a function name in sql at
 *        paren-depth 0, including its arguments and WITHIN GROUP (...).
 */
static int sc_find_func_range(const char* sql,
                               const char* fn_name, size_t fn_len,
                               size_t* fn_start_out, size_t* fn_end_out)
{
    const char* s    = sql;
    size_t      slen = strlen(sql);
    int         depth = 0;

    while (*s) {
        if (*s == '(') { depth++; s++; continue; }
        if (*s == ')') { depth--; s++; continue; }
        if (*s == '\'') { s++; while (*s && *s != '\'') s++; if (*s) s++; continue; }
        if (depth != 0) { s++; continue; }

        if ((s == sql || !(isalnum((unsigned char)s[-1]) || s[-1] == '_')) &&
            (size_t)(sql + slen - s) > fn_len &&
            strncasecmp(s, fn_name, fn_len) == 0 &&
            s[fn_len] == '(') {
            *fn_start_out = (size_t)(s - sql);
            s += fn_len; /* point to '(' */
            int inner = 0;
            while (*s) {
                if (*s == '(') inner++;
                else if (*s == ')') { inner--; if (inner == 0) { s++; break; } }
                else if (*s == '\'') { s++; while (*s && *s != '\'') s++; }
                s++;
            }
            /* Consume optional WITHIN GROUP (...) */
            const char* ws = s;
            while (*ws == ' ' || *ws == '\t' || *ws == '\n') ws++;
            if ((size_t)(sql + slen - ws) > 6 &&
                strncasecmp(ws, "within", 6) == 0 &&
                (ws[6] == ' ' || ws[6] == '\t' || ws[6] == '\n')) {
                ws += 6;
                while (*ws == ' ' || *ws == '\t' || *ws == '\n') ws++;
                if (strncasecmp(ws, "group", 5) == 0) {
                    ws += 5;
                    while (*ws == ' ' || *ws == '\t' || *ws == '\n') ws++;
                    if (*ws == '(') {
                        int gi = 1; ws++;
                        while (*ws && gi > 0) {
                            if (*ws == '(') gi++;
                            else if (*ws == ')') gi--;
                            ws++;
                        }
                        s = ws;
                    }
                }
            }
            *fn_end_out = (size_t)(s - sql);
            return 0;
        }
        s++;
    }
    return -1;
}

/**
 * @brief Rewrite ordered-aggregate function(s) in a scatter query by
 *        replacing each aggregate with its raw expression + ORDER BY key.
 */
static int sc_rewrite_ord_agg_sql(const char* sql, char* out, size_t outsz,
                                   const keel_ord_agg_spec_t* specs,
                                   uint16_t nspecs)
{
    if (nspecs == 0) {
        size_t slen = strlen(sql);
        if (slen + 1 > outsz) return -1;
        memcpy(out, sql, slen + 1);
        return 0;
    }

    const char* src = sql;
    char*       dst = out;
    size_t      rem = outsz;

    for (uint16_t si = 0; si < nspecs; si++) {
        const keel_ord_agg_spec_t* sp = &specs[si];
        const char* fn_name = NULL;
        size_t fn_len = 0;
        switch (sp->func) {
        case KEEL_ORD_AGG_STRING_AGG:      fn_name = "string_agg";      fn_len = 10; break;
        case KEEL_ORD_AGG_ARRAY_AGG:       fn_name = "array_agg";       fn_len =  9; break;
        case KEEL_ORD_AGG_JSONB_AGG:       fn_name = "jsonb_agg";       fn_len =  9; break;
        case KEEL_ORD_AGG_PERCENTILE_CONT: fn_name = "percentile_cont"; fn_len = 15; break;
        case KEEL_ORD_AGG_PERCENTILE_DISC: fn_name = "percentile_disc"; fn_len = 15; break;
        /* json_object_agg is computed natively per-shard and merged in-proxy;
         * do not rewrite the SQL. */
        case KEEL_ORD_AGG_JSON_OBJECT_AGG: continue;
        default: continue;
        }

        size_t fn_start = 0, fn_end = 0;
        if (sc_find_func_range(src, fn_name, fn_len, &fn_start, &fn_end) != 0)
            continue;

        if (fn_start >= rem) return -1;
        memcpy(dst, src, fn_start);
        dst += fn_start; rem -= fn_start;

        size_t rlen = strlen(sp->replacement_sql);
        if (rlen + 1 >= rem) return -1;
        memcpy(dst, sp->replacement_sql, rlen);
        dst += rlen; rem -= rlen;

        src += fn_end;
    }

    size_t tail = strlen(src);
    if (tail + 1 >= rem) return -1;
    memcpy(dst, src, tail + 1);
    return 0;
}

/* ============================================================================
 * Wire-protocol helpers (portable big-endian)
 * ============================================================================ */

static inline uint32_t sc_get_u32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) | (uint32_t)p[3];
}
static inline uint16_t sc_get_u16(const uint8_t* p) {
    return (uint16_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
}
static inline int32_t sc_get_i32(const uint8_t* p) {
    return (int32_t)sc_get_u32(p);
}
static inline void sc_put_u32(uint8_t* p, uint32_t v) {
    p[0]=(uint8_t)(v>>24); p[1]=(uint8_t)(v>>16); p[2]=(uint8_t)(v>>8); p[3]=(uint8_t)v;
}
static inline void sc_put_u16(uint8_t* p, uint16_t v) {
    p[0]=(uint8_t)(v>>8); p[1]=(uint8_t)v;
}
static inline void sc_put_i32(uint8_t* p, int32_t v) { sc_put_u32(p,(uint32_t)v); }

/* ============================================================================
 * Blocking I/O helpers
 * ============================================================================ */

static int sc_read_full(int fd, void* buf, size_t len) {
    uint8_t* p = (uint8_t*)buf;
    size_t done = 0;
    while (done < len) {
        ssize_t n = recv(fd, p + done, len - done, 0); /* NOLINT(keel-blocking): dedicated scatter worker thread */
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

static int sc_write_full(int fd, const void* buf, size_t len) {
    const uint8_t* p = (const uint8_t*)buf;
    size_t done = 0;
    while (done < len) {
        ssize_t n = send(fd, p + done, len - done, MSG_NOSIGNAL); /* NOLINT(keel-blocking): dedicated scatter worker thread */
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

/* ============================================================================
 * PG simple Query framer
 *
 * sc_build_startup and sc_auth_until_ready have been removed: those layers
 * are now handled transparently by backend_pool_borrow().
 * ============================================================================ */

/**
 * @brief Frame a SQL string as a PostgreSQL FE simple Query message ('Q').
 *
 * This is the only PG wire-protocol framer still needed by scatter: pool
 * borrow guarantees the connection is already authenticated and in
 * ReadyForQuery state, so all scatter must do is send the query and read
 * the response.
 */
static size_t sc_build_query(uint8_t* buf, size_t cap, const char* sql)
{
    size_t slen = strlen(sql);
    size_t total = 1 + 4 + slen + 1;
    if (total > cap) return 0;
    buf[0] = 'Q';
    sc_put_u32(buf + 1, (uint32_t)(4 + slen + 1));
    memcpy(buf + 5, sql, slen + 1);
    return total;
}

/* ============================================================================
 * Per-shard query execution using the backend connection pool
 * ============================================================================ */

/**
 * Execute @p sql on one shard using a pooled, authenticated, TLS-wrapped
 * backend connection.
 *
 * Borrows a connection from @p pool, sends the query, collects all rows into
 * @p result_inout, then returns the connection to the pool.  If the
 * connection is broken mid-response it is closed and its slot is marked
 * CLOSED so the pool's refill timer can reclaim it.
 *
 * On the first call (ncols_out == 0) also populates col_descs and ncols_out.
 */
static int sc_exec_shard_pooled(
    backend_pool_t*     pool,
    const char*         sql,
    keel_scatter_result_t**  result_inout,
    keel_scatter_col_desc_t* col_descs,
    uint16_t*           ncols_out,
    size_t              max_mem_bytes,
    const char*         spill_dir,
    char*               cmd_tag_out,   /* may be NULL; receives CommandComplete tag */
    size_t              cmd_tag_cap,
    char*               errbuf,
    size_t              errlen)
{
    if (cmd_tag_out && cmd_tag_cap > 0) cmd_tag_out[0] = '\0';
    /* Borrow an authenticated, pooled (and TLS-wrapped when configured)
     * connection.  The connection is in ReadyForQuery state on return. */
    backend_conn_t* be = backend_pool_borrow(pool, 0);
    if (!be) {
        snprintf(errbuf, errlen, "scatter: pool exhausted — no connection available");
        return -1;
    }

    int fd = be->fd;
    bool conn_broken = false;

    /* Backend connections are created with O_NONBLOCK for the io_uring reactor.
     * Scatter threads run in dedicated pthreads and do synchronous blocking I/O
     * (sc_read_full / sc_write_full).  EAGAIN from recv() would be treated as an
     * error, making every shard fail.  Temporarily clear O_NONBLOCK so that
     * recv() blocks until data arrives; restored at the 'done:' label below. */
    int orig_flags = fcntl(fd, F_GETFL);
    if (orig_flags < 0 || fcntl(fd, F_SETFL, orig_flags & ~O_NONBLOCK) < 0) { /* NOLINT(keel-blocking): dedicated scatter worker thread */
        snprintf(errbuf, errlen, "scatter: fcntl(O_NONBLOCK) failed: %s",
                 strerror(errno));
        conn_broken = true;
        goto done;
    }

    /* Apply read deadline.  Now that the socket is blocking, SO_RCVTIMEO is
     * honoured and prevents an unresponsive shard from blocking this thread
     * indefinitely. */
    {
        struct timeval tv;
        tv.tv_sec  = SCATTER_READ_TIMEOUT_MS / 1000;
        tv.tv_usec = (SCATTER_READ_TIMEOUT_MS % 1000) * 1000;
        if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
            snprintf(errbuf, errlen, "scatter: setsockopt SO_RCVTIMEO failed: %s",
                     strerror(errno));
            conn_broken = true;
            goto done;
        }
    }

    /* Send query — the pooled connection is already in ReadyForQuery state. */
    uint8_t qbuf[8192];
    size_t qlen = sc_build_query(qbuf, sizeof(qbuf), sql);
    if (qlen == 0 || sc_write_full(fd, qbuf, qlen) < 0) {
        snprintf(errbuf, errlen, "scatter: query send failed");
        conn_broken = true;
        goto done;
    }

    /* Read response: RowDescription + DataRows + CommandComplete + ReadyForQuery */
    uint8_t hdr[5];
    bool got_row_desc = (*ncols_out > 0);
    uint16_t ncols = *ncols_out;
    int rc = 0;

    for (;;) {
        if (sc_read_full(fd, hdr, 5) < 0) {
            snprintf(errbuf, errlen, "scatter: read response header failed");
            rc = -1; conn_broken = true; break;
        }
        char type = (char)hdr[0];
        uint32_t msglen = sc_get_u32(hdr + 1);
        if (msglen < 4 || msglen > (uint32_t)SCATTER_BUF_SIZE) {
            snprintf(errbuf, errlen, "scatter: bad msg len %u type='%c'", msglen, type);
            rc = -1; conn_broken = true; break;
        }
        uint32_t body_len = msglen - 4;
        /* Dynamic allocation for large bodies */
        uint8_t* body = NULL;
        uint8_t static_body[4096];
        bool dynamic = (body_len > sizeof(static_body));
        if (dynamic) {
            body = (uint8_t*)keel_malloc(body_len + 1);
            if (!body) { rc = -1; conn_broken = true; break; }
        } else {
            body = static_body;
        }
        if (body_len > 0 && sc_read_full(fd, body, body_len) < 0) {
            snprintf(errbuf, errlen, "scatter: read response body failed");
            if (dynamic) keel_free(body);
            rc = -1; conn_broken = true; break;
        }

        switch (type) {
        case 'T': { /* RowDescription */
            if (!got_row_desc) {
                /* Parse column descriptors */
                if (body_len < 2) { rc = -1; conn_broken = true; break; }
                ncols = sc_get_u16(body);
                if (ncols > 128) ncols = 128; /* safety cap */
                *ncols_out = ncols;
                const uint8_t* p = body + 2;
                const uint8_t* end = body + body_len;
                for (uint16_t ci = 0; ci < ncols && p < end; ci++) {
                    /* name (null-terminated) */
                    const char* name = (const char*)p;
                    size_t nlen = strnlen(name, (size_t)(end - p));
                    size_t copy = nlen < (KEEL_SCATTER_COL_NAME_MAX - 1) ? nlen : (KEEL_SCATTER_COL_NAME_MAX - 1);
                    memcpy(col_descs[ci].name, name, copy);
                    col_descs[ci].name[copy] = '\0';
                    p += nlen + 1;
                    if (p + 18 > end) break;
                    col_descs[ci].table_id   = sc_get_u32(p); p += 4;
                    col_descs[ci].col_num    = (int16_t)sc_get_u16(p); p += 2;
                    col_descs[ci].type       = keel_pg_oid_to_col_type(sc_get_u32(p)); p += 4;
                    col_descs[ci].type_len   = (int16_t)sc_get_u16(p); p += 2;
                    col_descs[ci].type_mod   = sc_get_i32(p); p += 4;
                    col_descs[ci].format     = (keel_wire_format_t)sc_get_u16(p); p += 2;
                }
                got_row_desc = true;
                /* Create the result store if not yet done */
                if (!*result_inout) {
                    size_t mem = max_mem_bytes > 0 ? max_mem_bytes : SCATTER_DEFAULT_MEM;
                    *result_inout = keel_scatter_result_create(ncols, col_descs, mem, spill_dir);
                    if (!*result_inout) {
                        snprintf(errbuf, errlen, "scatter: result create failed");
                        if (dynamic) keel_free(body);
                        rc = -1;
                        conn_broken = true; /* response not fully consumed */
                        goto done;
                    }
                }
            }
            break;
        }
        case 'D': { /* DataRow */
            if (!*result_inout || ncols == 0) break;
            if (body_len < 2) break;
            uint16_t rcols = sc_get_u16(body);
            if (rcols != ncols) { /* column count mismatch — skip row */ break; }
            keel_scatter_col_val_t vals[128];
            const uint8_t* p = body + 2;
            const uint8_t* end = body + body_len;
            for (uint16_t ci = 0; ci < ncols && p + 4 <= end; ci++) {
                int32_t clen = sc_get_i32(p); p += 4;
                if (clen < 0) {
                    vals[ci].len = -1;
                    vals[ci].data = NULL;
                } else {
                    vals[ci].len = clen;
                    vals[ci].data = (clen > 0 && p + (size_t)clen <= end) ? (const char*)p : "";
                    p += (size_t)(clen > 0 ? clen : 0);
                }
            }
            keel_scatter_result_append(*result_inout, vals);
            break;
        }
        case 'E': { /* ErrorResponse */
            const uint8_t* p = body;
            const uint8_t* end = body + body_len;
            while (p < end && *p != '\0') {
                char ftype = (char)*p++;
                const char* fval = (const char*)p;
                size_t flen = strnlen(fval, (size_t)(end - p));
                p += flen + 1;
                if (ftype == 'M') {
                    snprintf(errbuf, errlen, "scatter: backend error: %s", fval);
                    rc = -1;
                }
            }
            break;
        }
        case 'Z': /* ReadyForQuery — connection is back to idle protocol state */
            if (dynamic) keel_free(body);
            goto done;
        case 'C': /* CommandComplete — capture tag (e.g. "UPDATE 50") */
            if (cmd_tag_out && cmd_tag_cap > 0 && body_len > 0) {
                size_t tlen = strnlen((const char*)body, body_len);
                if (tlen >= cmd_tag_cap) tlen = cmd_tag_cap - 1;
                memcpy(cmd_tag_out, body, tlen);
                cmd_tag_out[tlen] = '\0';
            }
            break;
        case 'S': /* ParameterStatus */
        case 'N': /* NoticeResponse */
        default:
            break;
        }
        if (dynamic) keel_free(body);
        if (rc != 0) break;
    }
done:
    /* Restore original socket flags (O_NONBLOCK) before returning to pool so
     * the io_uring reactor can manage the connection normally. */
    if (!conn_broken && orig_flags >= 0)
        fcntl(fd, F_SETFL, orig_flags);

    /* Clear the read timeout before parking the connection back in the pool. */
    {
        struct timeval tv_clear = {0, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv_clear, sizeof(tv_clear));
    }

    if (conn_broken) {
        /* Response not fully consumed — connection is in an unknown protocol
         * state.  Close the fd and mark the pool slot CLOSED so the pool's
         * background refill timer can reconnect it. */
        backend_pool_close_connection(pool, be, BACKEND_CLOSE_REASON_PROTOCOL_ERROR);
        return rc;
    }
    backend_pool_return(pool, be, false);
    return rc;
}

/* ============================================================================
 * Wire-protocol encoder: send keel_scatter_result_t to client
 * ============================================================================ */

/* Dynamic send buffer that grows as needed */
typedef struct sendbuf {
    uint8_t* data;
    size_t   cap;
    size_t   len;
} sendbuf_t;

static int sb_ensure(sendbuf_t* sb, size_t need) {
    if (sb->len + need <= sb->cap) return 0;
    size_t newcap = sb->cap * 2;
    if (newcap < sb->len + need) newcap = sb->len + need + 4096;
    uint8_t* np = (uint8_t*)keel_realloc(sb->data, newcap);
    if (!np) return -1;
    sb->data = np; sb->cap = newcap;
    return 0;
}

static int sc_flush(sendbuf_t* sb, int fd) {
    if (sb->len == 0) return 0;
    int rc = 0;
    size_t done = 0;
    while (done < sb->len) {
        ssize_t n = send(fd, sb->data + done, sb->len - done, MSG_NOSIGNAL); /* NOLINT(keel-blocking): dedicated scatter worker thread */
        if (n <= 0) { rc = -1; break; }
        done += (size_t)n;
    }
    sb->len = 0;
    return rc;
}

static int sc_send_row_description(sendbuf_t* sb, int fd,
                                    uint16_t ncols,
                                    const keel_scatter_col_desc_t* cols)
{
    /* Calculate size */
    size_t payload = 2; /* num_cols */
    for (uint16_t i = 0; i < ncols; i++) {
        payload += strlen(cols[i].name) + 1; /* name + NUL */
        payload += 4 + 2 + 4 + 2 + 4 + 2;   /* table_id + col_num + type_oid + type_len + type_mod + format */
    }
    size_t total = 1 + 4 + payload;
    if (sb_ensure(sb, total) < 0) return -1;

    uint8_t* p = sb->data + sb->len;
    *p++ = 'T';
    sc_put_u32(p, (uint32_t)(4 + payload)); p += 4;
    sc_put_u16(p, ncols); p += 2;
    for (uint16_t i = 0; i < ncols; i++) {
        size_t nlen = strlen(cols[i].name);
        memcpy(p, cols[i].name, nlen + 1); p += nlen + 1;
        sc_put_u32(p, cols[i].table_id);                          p += 4;
        sc_put_u16(p, (uint16_t)cols[i].col_num);                 p += 2;
        sc_put_u32(p, keel_pg_col_type_to_oid(cols[i].type));     p += 4;
        sc_put_u16(p, (uint16_t)cols[i].type_len);                p += 2;
        sc_put_i32(p, cols[i].type_mod);                          p += 4;
        sc_put_u16(p, (uint16_t)cols[i].format);                  p += 2;
    }
    sb->len += total;
    return sc_flush(sb, fd);
}

static int sc_send_data_rows(sendbuf_t* sb, int fd,
                              keel_scatter_result_t* result,
                              uint16_t ncols)
{
    keel_scatter_result_iter_t it;
    if (keel_scatter_result_iter_init(&it, result) != KEEL_OK) return -1;

    const keel_scatter_col_val_t* vals = NULL;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        /* Calculate DataRow message size */
        size_t payload = 2; /* num_cols */
        for (uint16_t i = 0; i < ncols; i++) {
            payload += 4; /* value length */
            if (vals[i].len > 0) payload += (size_t)vals[i].len;
        }
        size_t total = 1 + 4 + payload;
        if (sb_ensure(sb, total) < 0) { keel_scatter_result_iter_close(&it); return -1; }

        uint8_t* p = sb->data + sb->len;
        *p++ = 'D';
        sc_put_u32(p, (uint32_t)(4 + payload)); p += 4;
        sc_put_u16(p, ncols); p += 2;
        for (uint16_t i = 0; i < ncols; i++) {
            if (vals[i].len < 0) {
                sc_put_i32(p, -1); p += 4; /* NULL */
            } else {
                sc_put_i32(p, vals[i].len); p += 4;
                if (vals[i].len > 0 && vals[i].data) {
                    memcpy(p, vals[i].data, (size_t)vals[i].len);
                    p += (size_t)vals[i].len;
                }
            }
        }
        sb->len += total;

        /* Flush periodically to avoid giant buffers */
        if (sb->len > 64 * 1024) {
            if (sc_flush(sb, fd) < 0) { keel_scatter_result_iter_close(&it); return -1; }
        }
    }
    keel_scatter_result_iter_close(&it);
    return sc_flush(sb, fd);
}

static int sc_send_command_complete(sendbuf_t* sb, int fd, uint64_t nrows)
{
    char tag[64];
    int tlen = snprintf(tag, sizeof(tag), "SELECT %llu", (unsigned long long)nrows);
    size_t total = 1 + 4 + (size_t)(tlen + 1);
    if (sb_ensure(sb, total) < 0) return -1;
    uint8_t* p = sb->data + sb->len;
    *p++ = 'C';
    sc_put_u32(p, (uint32_t)(4 + tlen + 1)); p += 4;
    memcpy(p, tag, (size_t)(tlen + 1));
    sb->len += total;
    return sc_flush(sb, fd);
}

/* Send a CommandComplete with an arbitrary tag string (e.g. "UPDATE 50",
 * "DELETE 12", "INSERT 0 1").  Used for scatter DML where shards return no
 * rows and the proxy must aggregate per-shard row counts. */
static int sc_send_command_complete_str(int fd, const char* tag)
{
    size_t tlen = strlen(tag);
    size_t total = 1 + 4 + tlen + 1;
    uint8_t* buf = (uint8_t*)keel_malloc(total);
    if (!buf) return -1;
    uint8_t* p = buf;
    *p++ = 'C';
    sc_put_u32(p, (uint32_t)(4 + tlen + 1)); p += 4;
    memcpy(p, tag, tlen + 1);
    int rc = sc_write_full(fd, buf, total);
    keel_free(buf);
    return rc;
}

static int sc_send_ready_for_query(int fd)
{
    uint8_t rfq[6] = { 'Z', 0, 0, 0, 5, 'I' };
    return sc_write_full(fd, rfq, 6);
}

static int sc_send_error(int fd, const char* msg)
{
    size_t mlen = strlen(msg);
    /* ErrorResponse: 'E' int32 'M' message '\0' '\0' */
    size_t payload = 1 + mlen + 1 + 1;
    size_t total = 1 + 4 + payload;
    uint8_t* buf = (uint8_t*)keel_malloc(total);
    if (!buf) return -1;
    uint8_t* p = buf;
    *p++ = 'E';
    sc_put_u32(p, (uint32_t)(4 + payload)); p += 4;
    *p++ = 'M';
    memcpy(p, msg, mlen + 1); p += mlen + 1;
    *p++ = '\0';
    int rc = sc_write_full(fd, buf, total);
    keel_free(buf);
    return rc;
}

/* ============================================================================
 * Parallel shard dispatch
 * ============================================================================ */

/** Maximum number of shards supported in a single scatter fan-out. */
#define SC_MAX_SHARDS 64

/**
 * @brief Per-shard task descriptor for parallel execution.
 *
 * Each eligible shard gets one task.  A POSIX thread is spawned per task;
 * after all threads are joined the results are merged into a single
 * keel_scatter_result_t before the merge pipeline (agg/group/sort/limit) runs.
 */
typedef struct scatter_shard_task {
    /* Input (set before thread spawn) */
    backend_pool_t*  pool;
    const char*      sql;
    size_t           max_mem_bytes;
    const char*      spill_dir;
    size_t           shard_idx;
    const char*      host;           /* for log messages */
    uint16_t         port;

    /* Output (written by thread, read after join) */
    keel_scatter_result_t*   result;
    keel_scatter_col_desc_t  col_descs[128];
    uint16_t            ncols;
    char                cmd_tag[64];   /* CommandComplete tag, e.g. "UPDATE 50" */
    char                errbuf[256];
    int                 rc;
} scatter_shard_task_t;

static void* scatter_shard_thread(void* arg)
{
    scatter_shard_task_t* task = (scatter_shard_task_t*)arg;
    task->result = NULL;
    task->ncols  = 0;
    task->cmd_tag[0] = '\0';
    task->rc = sc_exec_shard_pooled(
        task->pool, task->sql,
        &task->result, task->col_descs, &task->ncols,
        task->max_mem_bytes, task->spill_dir,
        task->cmd_tag, sizeof(task->cmd_tag),
        task->errbuf, sizeof(task->errbuf));
    return NULL;
}

/* ============================================================================
 * Monotonic timestamp helper
 * ============================================================================ */

static uint64_t sc_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ============================================================================
 * Public entry point
 * ============================================================================ */

int keel_engine_scatter_execute(
    const keel_server_pool_t*      server_pool,
    struct backend_pool**          server_pools,
    size_t                         server_pool_count,
    const char*                    sql,
    const keel_dispatch_result_t*  dr,
    int                            client_fd,
    size_t                         max_mem_bytes,
    const char*                    spill_dir,
    keel_scatter_obs_ctx_t*  obs)
{
    if (!server_pool || !sql || !dr || client_fd < 0) return -1;

    uint64_t t_start_us = sc_now_us();

    /* Prepare the SQL to send to each shard.
     * If AVG rewrite is needed, replace AVG(x) with SUM(x), COUNT(x).
     * If HAVING predicates exist, strip HAVING from the shard query
     * so all partial groups are returned (HAVING is re-applied post-merge). */
    char rewritten_sql[8192];
    const char* exec_sql = sql;
    if (dr->requires_count_distinct) {
        if (sc_rewrite_count_distinct_sql(sql, rewritten_sql, sizeof(rewritten_sql),
                                          dr->count_distinct_col) == 0) {
            exec_sql = rewritten_sql;
        }
    } else if (dr->requires_avg_rewrite || dr->nhaving_preds > 0 ||
               dr->nhaving_extra_agg_exprs > 0 ||
               dr->requires_outer_where_strip ||
               dr->nord_agg_specs > 0) {
        char tmp[8192];
        const char* src = sql;

        /* Ordered aggregate rewrite: replace agg function with raw expr + key.
         * When this is active the query returns per-row values, not a grouped
         * aggregate, so no HAVING / AVG / extra-col rewrite is needed. */
        if (dr->nord_agg_specs > 0) {
            if (sc_rewrite_ord_agg_sql(sql, rewritten_sql, sizeof(rewritten_sql),
                                        dr->ord_agg_specs, dr->nord_agg_specs) == 0) {
                exec_sql = rewritten_sql;
            } else {
                exec_sql = sql;
            }
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                          "scatter: ordered-agg rewrite: %s", exec_sql);
            goto scatter_sql_rewrite_done;
        }

        /* Strip HAVING first (so AVG rewrite doesn't touch HAVING clause).
         * Also strip when AVG rewrite is needed, since HAVING with AVG can't
         * be evaluated on per-shard partials — it's applied post-merge. */
        if (dr->nhaving_preds > 0 || dr->requires_avg_rewrite || dr->nhaving_extra_agg_exprs > 0) {
            if (sc_strip_having(sql, tmp, sizeof(tmp)) == 0) {
                src = tmp;
            }
        }

        /* Outer WHERE strip: remove WHERE from CTE/derived-table shard query so
         * all partial aggregates are returned; the predicate is applied
         * post-merge as HAVING. */
        if (dr->requires_outer_where_strip) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "scatter: stripping outer WHERE from shard SQL");
            char where_tmp[8192];
            if (sc_strip_outer_where(src, where_tmp, sizeof(where_tmp)) == 0) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "scatter: WHERE strip result: %s", where_tmp);
                memcpy(tmp, where_tmp, strlen(where_tmp) + 1);
                src = tmp;
            } else {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "scatter: WHERE strip failed");
            }
        }

        /* Add extra aggregate columns for HAVING evaluation when they're
         * not present in the original SELECT target list. */
        char extra_col_sql[8192];
        if (dr->nhaving_extra_agg_exprs > 0) {
            if (sc_add_extra_select_cols(src, dr->having_extra_agg_exprs,
                                          dr->nhaving_extra_agg_exprs,
                                          extra_col_sql, sizeof(extra_col_sql)) == 0) {
                src = extra_col_sql;
            }
        }
        if (dr->requires_avg_rewrite) {
            if (sc_rewrite_avg_sql(src, rewritten_sql, sizeof(rewritten_sql)) != 0) {
                memcpy(rewritten_sql, src, strlen(src) + 1);
            }
        } else {
            memcpy(rewritten_sql, src, strlen(src) + 1);
        }
        exec_sql = rewritten_sql;
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "scatter: rewritten sql: %s", exec_sql);
scatter_sql_rewrite_done:;
    }

    /* GROUP BY + LIMIT push-down correction.
     *
     * When a scatter query carries GROUP BY and LIMIT, the LIMIT must not reach
     * the shard backends.  Each shard would independently truncate its partial
     * group result to the top-N rows before the proxy can merge them, so groups
     * with a high *global* aggregate but a low per-shard count would silently
     * disappear.  Strip LIMIT/OFFSET from the shard SQL here; the global LIMIT
     * is applied post-merge in Phase C (LIMIT / OFFSET section below).
     *
     * sc_strip_limit_offset() is a no-op when the query has no LIMIT or OFFSET,
     * so calling it unconditionally here is safe.  For non-GROUP-BY scatter
     * queries (plain SELECT with ORDER BY LIMIT) keeping the LIMIT in the shard
     * SQL is a valid push-down optimization and is intentionally preserved. */
    char groupby_shard_sql[8192];
    if ((dr->ngroup_key_cols > 0 || dr->limit_offset > 0) && (dr->limit_count > 0 || dr->limit_offset > 0)) {
        if (sc_strip_limit_offset(exec_sql, groupby_shard_sql,
                                   sizeof(groupby_shard_sql)) == 0 &&
            strlen(groupby_shard_sql) < strlen(exec_sql)) {
            exec_sql = groupby_shard_sql;
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE,
                "scatter: stripped LIMIT/OFFSET from shard "
                "query to prevent incorrect per-shard truncation");
        }
    }

    /* -----------------------------------------------------------------------
     * Trace context: prepend W3C traceparent as SQL block comment so backend
     * servers can correlate queries with the client's distributed trace.
     * Applied AFTER all SQL rewrites so the comment survives rewriting.
     * ----------------------------------------------------------------------- */
    char traced_sql[8192 + 96];
    if (obs && obs->trace_ctx && !keel_trace_id_is_zero(obs->trace_ctx->trace_id)) {
        char tc_comment[96];
        size_t tc_len = keel_trace_format_sql_comment(obs->trace_ctx,
                                                       tc_comment, sizeof(tc_comment));
        if (tc_len > 0) {
            size_t slen = strlen(exec_sql);
            if (tc_len + 1 + slen < sizeof(traced_sql)) {
                memcpy(traced_sql, tc_comment, tc_len);
                traced_sql[tc_len] = ' ';
                memcpy(traced_sql + tc_len + 1, exec_sql, slen + 1);
                exec_sql = traced_sql;
            }
        }
    }

    /* -----------------------------------------------------------------------
     * Parallel shard dispatch
     *
     * For each healthy shard with a pool, we spawn one POSIX thread that
     * borrows a connection, sends the query, and collects all rows into its
     * own keel_scatter_result_t.  After all threads are joined the per-shard
     * results are merged (appended row-by-row) into a single combined result
     * before the merge pipeline runs.
     * ----------------------------------------------------------------------- */
    scatter_shard_task_t tasks[SC_MAX_SHARDS];
    pthread_t           threads[SC_MAX_SHARDS];
    size_t              ntasks = 0;

    for (size_t si = 0;
         si < server_pool->count && si < server_pool_count && ntasks < SC_MAX_SHARDS;
         si++) {
        const keel_backend_server_t* srv = &server_pool->servers[si];
        if (!srv->host || !srv->host[0]) continue;
        if (!srv->healthy) continue;

        if (!server_pools || !server_pools[si]) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: shard %zu (%s:%u) has no connection pool — skipping",
                si, srv->host, (unsigned)srv->port);
            continue;
        }

        scatter_shard_task_t* t = &tasks[ntasks];
        t->pool          = server_pools[si];
        t->sql           = exec_sql;
        t->max_mem_bytes = max_mem_bytes;
        t->spill_dir     = spill_dir;
        t->shard_idx     = si;
        t->host          = srv->host;
        t->port          = srv->port;
        t->result        = NULL;
        t->ncols         = 0;
        t->cmd_tag[0]    = '\0';
        t->rc            = 0;
        t->errbuf[0]     = '\0';

        if (pthread_create(&threads[ntasks], NULL, scatter_shard_thread, t) != 0) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: pthread_create failed for shard %zu (%s:%u): %s",
                si, srv->host, (unsigned)srv->port, strerror(errno));
            continue;
        }
        ntasks++;
    }

    /* Join all shard threads */
    for (size_t ti = 0; ti < ntasks; ti++)
        pthread_join(threads[ti], NULL);

    /* Log per-shard results and count failures */
    size_t failed_shards = 0;
    for (size_t ti = 0; ti < ntasks; ti++) {
        if (tasks[ti].rc != 0) {
            failed_shards++;
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: shard %zu (%s:%u) failed: %s",
                tasks[ti].shard_idx, tasks[ti].host,
                (unsigned)tasks[ti].port, tasks[ti].errbuf);
        }
    }

    /* Merge per-shard results: find reference schema (first success with rows),
     * create combined result, append all rows. */
    keel_scatter_col_desc_t col_descs[128];
    memset(col_descs, 0, sizeof(col_descs));
    uint16_t ncols = 0;
    keel_scatter_result_t* result = NULL;

    /* Find reference schema */
    for (size_t ti = 0; ti < ntasks && ncols == 0; ti++) {
        if (tasks[ti].rc == 0 && tasks[ti].ncols > 0) {
            ncols = tasks[ti].ncols;
            memcpy(col_descs, tasks[ti].col_descs,
                   (size_t)ncols * sizeof(keel_scatter_col_desc_t));
        }
    }

    /* Create combined result and append all per-shard rows */
    if (ncols > 0) {
        size_t mem = max_mem_bytes > 0 ? max_mem_bytes : SCATTER_DEFAULT_MEM;
        result = keel_scatter_result_create(ncols, col_descs, mem, spill_dir);
        if (!result) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: combined result alloc failed (OOM)");
            for (size_t ti = 0; ti < ntasks; ti++)
                if (tasks[ti].result) keel_scatter_result_destroy(tasks[ti].result);
            sc_send_error(client_fd, "scatter: out of memory");
            sc_send_ready_for_query(client_fd);
            return -1;
        }

        for (size_t ti = 0; ti < ntasks; ti++) {
            keel_scatter_result_t* sr = tasks[ti].result;
            if (!sr) continue;
            keel_scatter_result_iter_t it;
            if (keel_scatter_result_iter_init(&it, sr) == KEEL_OK) {
                const keel_scatter_col_val_t* vals = NULL;
                while (keel_scatter_result_iter_next(&it, &vals))
                    keel_scatter_result_append(result, vals);
                keel_scatter_result_iter_close(&it);
            }
            keel_scatter_result_destroy(tasks[ti].result);
            tasks[ti].result = NULL;
        }
    } else {
        /* No successful shard — free any partial results */
        for (size_t ti = 0; ti < ntasks; ti++)
            if (tasks[ti].result) {
                keel_scatter_result_destroy(tasks[ti].result);
                tasks[ti].result = NULL;
            }
    }

    /* -----------------------------------------------------------------------
     * Audit log: emit SCATTER event with outcome metrics
     * ----------------------------------------------------------------------- */
    uint64_t elapsed_us = sc_now_us() - t_start_us;
    if (obs && obs->audit_log) {
        keel_audit_emit_scatter(
            (keel_audit_log_t*)obs->audit_log,
            obs->username, obs->database,
            sql,                    /* original SQL, not traced/rewritten */
            ntasks, failed_shards,
            elapsed_us);
    }

    char errbuf[512] = {0};
    if (failed_shards > 0 && !result)
        snprintf(errbuf, sizeof(errbuf),
                 "scatter: all %zu shard(s) failed", ntasks);

    if (!result || ncols == 0) {
        /* No row results from any shard.  Two sub-cases:
         *   (a) DML scatter (UPDATE / DELETE / INSERT): each shard returned
         *       only a CommandComplete tag like "UPDATE 50".  We aggregate
         *       the per-shard row counts and reply with a single tag whose
         *       count is the sum across shards.  Tag verb is taken from the
         *       first non-empty shard tag.
         *   (b) Genuine empty SELECT result (or all shards failed): emit the
         *       previous behaviour (error or empty RowDescription/CC). */
        if (errbuf[0]) {
            sc_send_error(client_fd, errbuf);
        } else {
            const char* verb = NULL;
            size_t      verb_len = 0;
            uint64_t    total_rows = 0;
            bool        any_tag = false;
            for (size_t ti = 0; ti < ntasks; ti++) {
                if (tasks[ti].rc != 0) continue;
                const char* tag = tasks[ti].cmd_tag;
                if (!tag[0]) continue;
                any_tag = true;
                /* Parse tag: "VERB [oid] count" — last whitespace-separated
                 * token is the affected-row count for UPDATE/DELETE/SELECT;
                 * for INSERT it is "INSERT <oid> <rows>". */
                const char* sp = strrchr(tag, ' ');
                uint64_t n = 0;
                if (sp) {
                    char* endp = NULL;
                    unsigned long long v = strtoull(sp + 1, &endp, 10);
                    if (endp != sp + 1) n = (uint64_t)v;
                }
                total_rows += n;
                if (!verb) {
                    /* Verb is the first whitespace-separated token. */
                    const char* end = strchr(tag, ' ');
                    verb     = tag;
                    verb_len = end ? (size_t)(end - tag) : strlen(tag);
                }
            }
            if (any_tag && verb && verb_len > 0) {
                char out_tag[96];
                if (verb_len >= sizeof(out_tag) - 32) verb_len = sizeof(out_tag) - 32;
                /* INSERT requires "INSERT 0 <rows>" form. */
                if (verb_len == 6 && strncasecmp(verb, "INSERT", 6) == 0) {
                    snprintf(out_tag, sizeof(out_tag),
                             "INSERT 0 %llu", (unsigned long long)total_rows);
                } else {
                    snprintf(out_tag, sizeof(out_tag), "%.*s %llu",
                             (int)verb_len, verb,
                             (unsigned long long)total_rows);
                }
                sc_send_command_complete_str(client_fd, out_tag);
            } else if (ncols > 0) {
                /* Shards returned a schema but zero rows: send RowDescription +
                 * CommandComplete so clients receive a well-formed empty result set
                 * rather than a bare ReadyForQuery which violates the wire protocol. */
                sendbuf_t empty_sb = { NULL, 0, 0 };
                empty_sb.data = (uint8_t*)keel_malloc(SCATTER_SEND_BUF);
                if (empty_sb.data) {
                    empty_sb.cap = SCATTER_SEND_BUF;
                    sc_send_row_description(&empty_sb, client_fd, ncols, col_descs);
                    sc_send_command_complete(&empty_sb, client_fd, 0);
                    keel_free(empty_sb.data);
                }
            } else {
                /* No tag, no schema: send a generic empty SELECT. */
                sendbuf_t empty_sb = { NULL, 0, 0 };
                empty_sb.data = (uint8_t*)keel_malloc(64);
                if (empty_sb.data) {
                    empty_sb.cap = 64;
                    sc_send_command_complete(&empty_sb, client_fd, 0);
                    keel_free(empty_sb.data);
                }
            }
        }
        sc_send_ready_for_query(client_fd);
        return errbuf[0] ? -1 : 0;
    }

    /* Apply merge operations as specified by the dispatch result */
    keel_error_t merr = KEEL_OK;

    /* Phase D: scalar aggregates (no GROUP BY) */
    if (dr->requires_count_distinct) {
        /* Each shard returned DISTINCT values of count_distinct_col (col 0).
         * Deduplicate across shards by grouping on col 0, then count rows. */
        keel_group_col_spec_t gspec = { .col_index = 0 };
        merr = keel_scatter_result_group_aggs(result, &gspec, 1, NULL, 0);
        if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: count_distinct group failed: %d", (int)merr);
        }
        /* Count the deduplicated rows */
        uint64_t ndistinct = (uint64_t)keel_scatter_result_row_count(result);

        /* Build a synthetic 1-column int8 result with the count */
        keel_scatter_col_desc_t cnt_col;
        memset(&cnt_col, 0, sizeof(cnt_col));
        memcpy(cnt_col.name, "count", 5);
        cnt_col.type     = KEEL_COL_TYPE_INT64;
        cnt_col.type_len = -1;
        cnt_col.format   = KEEL_WIRE_TEXT;
        keel_scatter_result_t* cnt_result =
            keel_scatter_result_create(1, &cnt_col, max_mem_bytes, spill_dir);
        if (!cnt_result) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: count_distinct result alloc failed (OOM)");
            keel_scatter_result_destroy(result);
            sc_send_error(client_fd, "scatter: out of memory building COUNT DISTINCT result");
            sc_send_ready_for_query(client_fd);
            return -1;
        }
        char cnt_str[32];
        int csn = snprintf(cnt_str, sizeof(cnt_str), "%" PRIu64, ndistinct);
        keel_scatter_col_val_t cv = { .len = csn, .data = cnt_str };
        keel_scatter_result_append(cnt_result, &cv);
        keel_scatter_result_destroy(result);
        result = cnt_result;
        ncols = 1;
        col_descs[0] = cnt_col;
    }

    /* Phase: Ordered Aggregate Post-Merge.
     *
     * For STRING_AGG, ARRAY_AGG, JSONB_AGG, PERCENTILE_CONT, PERCENTILE_DISC:
     * each shard returned raw rows (value + optional key column).
     * Sort by key, then compute the aggregate in-proxy over all rows. */
    if (dr->nord_agg_specs > 0 && result && ncols > 0) {
        for (uint16_t si = 0; si < dr->nord_agg_specs; si++) {
            const keel_ord_agg_spec_t* sp = &dr->ord_agg_specs[si];
            bool is_percentile = (sp->func == KEEL_ORD_AGG_PERCENTILE_CONT ||
                                  sp->func == KEEL_ORD_AGG_PERCENTILE_DISC);

            /* Sort: for PERCENTILE the value is col 0; for others key is col 1 */
            keel_sort_key_t sort_key;
            sort_key.col_index = is_percentile ? 0 : (sp->has_key ? 1 : 0);
            sort_key.dir       = sp->key_dir;
            sort_key.nulls     = KEEL_SORT_NULLS_DEFAULT;
            merr = keel_scatter_result_sort(result, &sort_key, 1);
            if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "scatter: ord_agg sort failed: %d", (int)merr);

            uint64_t nrows_agg = keel_scatter_result_row_count(result);
            char     agg_buf[65536];
            size_t   agg_len = 0;
            keel_col_type_t result_type = KEEL_COL_TYPE_TEXT;

            if (sp->func == KEEL_ORD_AGG_STRING_AGG) {
                size_t sep_len = strlen(sp->separator);
                keel_scatter_result_iter_t it;
                if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                    bool first_r = true;
                    const keel_scatter_col_val_t* vals;
                    while (keel_scatter_result_iter_next(&it, &vals)) {
                        if (!first_r && sep_len > 0 &&
                            agg_len + sep_len < sizeof(agg_buf) - 1) {
                            memcpy(agg_buf + agg_len, sp->separator, sep_len);
                            agg_len += sep_len;
                        }
                        if (vals[0].len > 0 && vals[0].data) {
                            size_t vl = (size_t)vals[0].len;
                            if (agg_len + vl < sizeof(agg_buf) - 1) {
                                memcpy(agg_buf + agg_len, vals[0].data, vl);
                                agg_len += vl;
                            }
                        }
                        first_r = false;
                    }
                    keel_scatter_result_iter_close(&it);
                }
                result_type = KEEL_COL_TYPE_TEXT;

            } else if (sp->func == KEEL_ORD_AGG_ARRAY_AGG) {
                /* Build PostgreSQL text-array literal: {val1,val2,...} */
                agg_buf[agg_len++] = '{';
                keel_scatter_result_iter_t it;
                if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                    bool first_r = true;
                    const keel_scatter_col_val_t* vals;
                    while (keel_scatter_result_iter_next(&it, &vals)) {
                        if (!first_r && agg_len < sizeof(agg_buf) - 1)
                            agg_buf[agg_len++] = ',';
                        if (vals[0].len > 0 && vals[0].data) {
                            size_t vl = (size_t)vals[0].len;
                            if (agg_len + vl < sizeof(agg_buf) - 2) {
                                memcpy(agg_buf + agg_len, vals[0].data, vl);
                                agg_len += vl;
                            }
                        }
                        first_r = false;
                    }
                    keel_scatter_result_iter_close(&it);
                }
                if (agg_len < sizeof(agg_buf) - 1) agg_buf[agg_len++] = '}';
                result_type = KEEL_COL_TYPE_TEXT_ARRAY;

            } else if (sp->func == KEEL_ORD_AGG_JSONB_AGG) {
                /* Build JSON array: [{...},{...},...] */
                agg_buf[agg_len++] = '[';
                keel_scatter_result_iter_t it;
                if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                    bool first_r = true;
                    const keel_scatter_col_val_t* vals;
                    while (keel_scatter_result_iter_next(&it, &vals)) {
                        if (!first_r && agg_len < sizeof(agg_buf) - 1)
                            agg_buf[agg_len++] = ',';
                        if (vals[0].len > 0 && vals[0].data) {
                            size_t vl = (size_t)vals[0].len;
                            if (agg_len + vl < sizeof(agg_buf) - 2) {
                                memcpy(agg_buf + agg_len, vals[0].data, vl);
                                agg_len += vl;
                            }
                        }
                        first_r = false;
                    }
                    keel_scatter_result_iter_close(&it);
                }
                if (agg_len < sizeof(agg_buf) - 1) agg_buf[agg_len++] = ']';
                result_type = KEEL_COL_TYPE_JSONB;

            } else if (sp->func == KEEL_ORD_AGG_PERCENTILE_CONT) {
                double fraction = sp->percentile;
                if (nrows_agg == 0) {
                    agg_len = 0;
                } else if (nrows_agg == 1) {
                    keel_scatter_result_iter_t it;
                    if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                        const keel_scatter_col_val_t* vals;
                        if (keel_scatter_result_iter_next(&it, &vals) && vals[0].len > 0) {
                            size_t vl = (size_t)vals[0].len < sizeof(agg_buf)-1
                                        ? (size_t)vals[0].len : sizeof(agg_buf)-1;
                            memcpy(agg_buf, vals[0].data, vl);
                            agg_len = vl;
                        }
                        keel_scatter_result_iter_close(&it);
                    }
                } else {
                    double row_idx  = fraction * (double)(nrows_agg - 1);
                    size_t lo_idx   = (size_t)row_idx;
                    size_t hi_idx   = lo_idx + 1;
                    if (hi_idx >= (size_t)nrows_agg) hi_idx = (size_t)(nrows_agg - 1);
                    double frac_part = row_idx - (double)lo_idx;
                    double lo_val = 0.0, hi_val = 0.0;
                    char vtmp[64];
                    keel_scatter_result_iter_t it;
                    if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                        size_t cur = 0;
                        const keel_scatter_col_val_t* vals;
                        while (keel_scatter_result_iter_next(&it, &vals)) {
                            if (cur == lo_idx && vals[0].len > 0) {
                                size_t vl = (size_t)vals[0].len < sizeof(vtmp)-1
                                            ? (size_t)vals[0].len : sizeof(vtmp)-1;
                                memcpy(vtmp, vals[0].data, vl); vtmp[vl] = '\0';
                                lo_val = strtod(vtmp, NULL);
                            }
                            if (cur == hi_idx && vals[0].len > 0) {
                                size_t vl = (size_t)vals[0].len < sizeof(vtmp)-1
                                            ? (size_t)vals[0].len : sizeof(vtmp)-1;
                                memcpy(vtmp, vals[0].data, vl); vtmp[vl] = '\0';
                                hi_val = strtod(vtmp, NULL);
                            }
                            if (cur > hi_idx) break;
                            cur++;
                        }
                        keel_scatter_result_iter_close(&it);
                    }
                    double result_val = lo_val + frac_part * (hi_val - lo_val);
                    agg_len = (size_t)snprintf(agg_buf, sizeof(agg_buf),
                                               "%.17g", result_val);
                }
                result_type = KEEL_COL_TYPE_FLOAT64;

            } else if (sp->func == KEEL_ORD_AGG_PERCENTILE_DISC) {
                /* Pick value at floor(fraction * n), clamped to [0, n-1] */
                size_t idx = (nrows_agg > 0)
                    ? (size_t)(sp->percentile * (double)nrows_agg)
                    : 0;
                if (idx >= (size_t)nrows_agg && nrows_agg > 0)
                    idx = (size_t)(nrows_agg - 1);
                keel_scatter_result_iter_t it;
                if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                    size_t cur = 0;
                    const keel_scatter_col_val_t* vals;
                    while (keel_scatter_result_iter_next(&it, &vals)) {
                        if (cur == idx && vals[0].len > 0) {
                            size_t vl = (size_t)vals[0].len < sizeof(agg_buf)-1
                                        ? (size_t)vals[0].len : sizeof(agg_buf)-1;
                            memcpy(agg_buf, vals[0].data, vl);
                            agg_len = vl;
                            break;
                        }
                        cur++;
                    }
                    keel_scatter_result_iter_close(&it);
                }
                result_type = (ncols > 0) ? col_descs[0].type : KEEL_COL_TYPE_INT32;
            } else if (sp->func == KEEL_ORD_AGG_JSON_OBJECT_AGG) {
                /* Each shard returned a single row containing its own
                 * json_object_agg result.  Concatenate the per-shard JSON
                 * objects by stripping outer braces and joining inner
                 * content with ','.  Wrap the final result in '{ ... }'. */
                agg_buf[agg_len++] = '{';
                bool any_kv = false;
                keel_scatter_result_iter_t it;
                if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                    const keel_scatter_col_val_t* vals;
                    while (keel_scatter_result_iter_next(&it, &vals)) {
                        if (vals[0].len <= 0 || !vals[0].data) continue;
                        const char* s = vals[0].data;
                        size_t      n = (size_t)vals[0].len;
                        /* Strip leading whitespace + opening '{' */
                        size_t i = 0;
                        while (i < n && (s[i] == ' ' || s[i] == '\t' ||
                                         s[i] == '\n' || s[i] == '\r')) i++;
                        if (i < n && s[i] == '{') i++;
                        /* Strip trailing whitespace + closing '}' */
                        size_t j = n;
                        while (j > i && (s[j-1] == ' ' || s[j-1] == '\t' ||
                                         s[j-1] == '\n' || s[j-1] == '\r')) j--;
                        if (j > i && s[j-1] == '}') j--;
                        /* Trim inner padding whitespace at both ends */
                        while (i < j && (s[i] == ' ' || s[i] == '\t' ||
                                         s[i] == '\n' || s[i] == '\r')) i++;
                        while (j > i && (s[j-1] == ' ' || s[j-1] == '\t' ||
                                         s[j-1] == '\n' || s[j-1] == '\r')) j--;
                        if (j <= i) continue;  /* shard returned '{}' */
                        size_t plen = j - i;
                        if (any_kv) {
                            if (agg_len + 1 >= sizeof(agg_buf) - 2) break;
                            agg_buf[agg_len++] = ',';
                        }
                        if (agg_len + plen >= sizeof(agg_buf) - 2) {
                            plen = sizeof(agg_buf) - 2 - agg_len;
                        }
                        memcpy(agg_buf + agg_len, s + i, plen);
                        agg_len += plen;
                        any_kv = true;
                    }
                    keel_scatter_result_iter_close(&it);
                }
                if (agg_len + 1 < sizeof(agg_buf)) agg_buf[agg_len++] = '}';
                result_type = (ncols > 0) ? col_descs[0].type : KEEL_COL_TYPE_TEXT;
            }

            agg_buf[agg_len] = '\0';

            /* Build a 1-column result with the aggregated value */
            keel_scatter_col_desc_t agg_col;
            memset(&agg_col, 0, sizeof(agg_col));
            agg_col = col_descs[0];
            agg_col.type = result_type;

            keel_scatter_result_t* agg_result =
                keel_scatter_result_create(1, &agg_col, max_mem_bytes, spill_dir);
            if (!agg_result) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "scatter: ord_agg result alloc failed (OOM)");
                break;
            }
            keel_scatter_col_val_t cv = { .len = (int32_t)agg_len, .data = agg_buf };
            keel_scatter_result_append(agg_result, &cv);

            keel_scatter_result_destroy(result);
            result    = agg_result;
            ncols     = 1;
            col_descs[0] = agg_col;
        }
    }

    if (dr->nagg_specs > 0 && dr->ngroup_key_cols == 0) {
        merr = keel_scatter_result_merge_aggs(result, dr->agg_specs, dr->nagg_specs);
        if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "scatter: merge_aggs failed: %d", (int)merr);
        }
        /* If AVG rewrite needed, finalize and truncate extra COUNT columns */
        if (dr->requires_avg_rewrite && dr->navg_finalize_specs > 0) {
            merr = keel_scatter_result_finalize_avg(result, dr->avg_finalize_specs, dr->navg_finalize_specs);
            if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "scatter: finalize_avg failed: %d", (int)merr);
            /* Fix type for AVG output columns: use FLOAT64 */
            for (uint16_t s = 0; s < dr->navg_finalize_specs; s++) {
                int16_t oc = dr->avg_finalize_specs[s].out_col;
                if (oc >= 0 && (uint16_t)oc < ncols)
                    col_descs[(uint16_t)oc].type = KEEL_COL_TYPE_FLOAT64;
            }
            /* Remove the appended COUNT columns from the result */
            if (ncols > dr->navg_finalize_specs)
                ncols -= dr->navg_finalize_specs;
        }
        /* Outer avg finalize: ROUND(sum/count) post-CTE pattern — compute
         * the division in-proxy WITHOUT removing the COUNT column. */
        if (dr->requires_outer_avg_finalize && dr->navg_finalize_specs > 0) {
            merr = keel_scatter_result_finalize_avg(result,
                dr->avg_finalize_specs, dr->navg_finalize_specs);
            if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "scatter: outer finalize_avg (D) failed: %d", (int)merr);
            for (uint16_t s = 0; s < dr->navg_finalize_specs; s++) {
                int16_t oc = dr->avg_finalize_specs[s].out_col;
                if (oc >= 0 && (uint16_t)oc < ncols)
                    col_descs[(uint16_t)oc].type = KEEL_COL_TYPE_FLOAT64;
            }
            /* NOTE: do NOT decrement ncols — COUNT col is a real SELECT target */
        }
    }

    /* Phase E: GROUP BY with aggregates */
    if (dr->ngroup_key_cols > 0) {
        merr = keel_scatter_result_group_aggs(result,
                                          dr->group_key_cols, dr->ngroup_key_cols,
                                          dr->nagg_specs > 0 ? dr->agg_specs : NULL,
                                          dr->nagg_specs);
        if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "scatter: group_aggs failed: %d", (int)merr);
        }
        /* Finalize AVG if needed and truncate extra COUNT columns */
        if (dr->requires_avg_rewrite && dr->navg_finalize_specs > 0) {
            merr = keel_scatter_result_finalize_avg(result, dr->avg_finalize_specs, dr->navg_finalize_specs);
            if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "scatter: finalize_avg failed: %d", (int)merr);
            /* Fix type for AVG output columns: use FLOAT64 */
            for (uint16_t s = 0; s < dr->navg_finalize_specs; s++) {
                int16_t oc = dr->avg_finalize_specs[s].out_col;
                if (oc >= 0 && (uint16_t)oc < ncols)
                    col_descs[(uint16_t)oc].type = KEEL_COL_TYPE_FLOAT64;
            }
            if (ncols > dr->navg_finalize_specs)
                ncols -= dr->navg_finalize_specs;
        }
        /* Outer avg finalize for GROUP BY case */
        if (dr->requires_outer_avg_finalize && dr->navg_finalize_specs > 0) {
            merr = keel_scatter_result_finalize_avg(result,
                dr->avg_finalize_specs, dr->navg_finalize_specs);
            if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "scatter: outer finalize_avg (E) failed: %d", (int)merr);
            for (uint16_t s = 0; s < dr->navg_finalize_specs; s++) {
                int16_t oc = dr->avg_finalize_specs[s].out_col;
                if (oc >= 0 && (uint16_t)oc < ncols)
                    col_descs[(uint16_t)oc].type = KEEL_COL_TYPE_FLOAT64;
            }
        }
    }

    /* Phase H: HAVING post-filter */
    if (dr->nhaving_preds > 0) {
        merr = keel_scatter_result_apply_having(result, dr->having_preds, dr->nhaving_preds);
        if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "scatter: apply_having failed: %d", (int)merr);
    }
    /* Strip extra aggregate columns that were added for HAVING evaluation */
    if (dr->nhaving_extra_agg_exprs > 0 && ncols > dr->nhaving_extra_agg_exprs)
        ncols -= dr->nhaving_extra_agg_exprs;

    /* Phase F: window function global recomputation.
     *
     * Applies to global-ranking window functions (ROW_NUMBER, RANK,
     * DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST) that have no PARTITION BY.
     * The result is sorted per-spec by the window ORDER BY, per-shard values
     * (which are local and incorrect) are overwritten with globally correct
     * values.  Phase C (ORDER BY) runs after to apply the final client sort. */
    if (dr->nwindow_col_specs > 0) {
        merr = keel_scatter_result_window_compute(result,
                                              dr->window_col_specs,
                                              dr->nwindow_col_specs);
        if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter: window_compute failed: %d", (int)merr);
    }

    /* Phase C: ORDER BY */
    if (dr->norder_keys > 0) {
        merr = keel_scatter_result_sort(result, dr->order_keys, dr->norder_keys);
        if (merr != KEEL_OK && merr != KEEL_ERR_NOT_SUPPORTED)
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "scatter: sort failed: %d", (int)merr);
    }

    /* Phase: Scatter dedup — remove consecutive duplicate rows after ORDER BY.
     * Used when a DISTINCT CTE is joined to itself (same rows from all shards). */
    if (dr->requires_scatter_dedup && result &&
        keel_scatter_result_row_count(result) > 1) {
        keel_scatter_result_t* dedup_r =
            keel_scatter_result_create(ncols, col_descs, max_mem_bytes, spill_dir);
        if (dedup_r) {
            keel_scatter_result_iter_t it;
            if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
                char prev_buf[4096]; size_t prev_len = 0;
                bool first_d = true;
                const keel_scatter_col_val_t* vals;
                while (keel_scatter_result_iter_next(&it, &vals)) {
                    char cur_buf[4096]; size_t cur_len = 0;
                    for (uint16_t ci = 0; ci < ncols; ci++) {
                        if (vals[ci].len >= 0 && vals[ci].data) {
                            size_t vl = (size_t)vals[ci].len;
                            if (cur_len + vl + 1 < sizeof(cur_buf)) {
                                memcpy(cur_buf + cur_len, vals[ci].data, vl);
                                cur_len += vl;
                                cur_buf[cur_len++] = '\0';
                            }
                        } else {
                            if (cur_len + 1 < sizeof(cur_buf))
                                cur_buf[cur_len++] = '\x01';
                        }
                    }
                    bool is_dup = !first_d &&
                                  cur_len == prev_len &&
                                  memcmp(cur_buf, prev_buf, cur_len) == 0;
                    if (!is_dup) {
                        keel_scatter_result_append(dedup_r, vals);
                        if (cur_len <= sizeof(prev_buf)) {
                            memcpy(prev_buf, cur_buf, cur_len);
                            prev_len = cur_len;
                        }
                    }
                    first_d = false;
                }
                keel_scatter_result_iter_close(&it);
            }
            keel_scatter_result_destroy(result);
            result = dedup_r;
        }
    }

    /* LIMIT / OFFSET */
    if (dr->limit_count > 0 || dr->limit_offset > 0) {
        keel_scatter_result_apply_limit(result, dr->limit_count, dr->limit_offset);
    }

    /* Encode and send to client */
    sendbuf_t sb = { NULL, 0, 0 };
    sb.data = (uint8_t*)keel_malloc(SCATTER_SEND_BUF);
    if (!sb.data) {
        keel_scatter_result_destroy(result);
        return -1;
    }
    sb.cap = SCATTER_SEND_BUF;

    int rc = 0;
    rc |= sc_send_row_description(&sb, client_fd, ncols, col_descs);

    /* Count rows for CommandComplete */
    uint64_t nrows = result->row_count;

    /* ---- Write back observable output stats to caller ---- */
    if (obs) {
        obs->rows_merged_out = nrows;
        obs->spilled_out     = keel_scatter_result_spilled(result);
    }

    rc |= sc_send_data_rows(&sb, client_fd, result, ncols);
    rc |= sc_send_command_complete(&sb, client_fd, nrows);
    rc |= sc_send_ready_for_query(client_fd);

    keel_free(sb.data);
    keel_scatter_result_destroy(result);
    return rc;
}

/* ============================================================================
 * Scatter write with two-phase commit (C2)
 * ============================================================================ */

/**
 * @brief Per-shard capture buffer for RETURNING rows in scatter DML.
 *
 * When a scatter write SQL contains a RETURNING clause, each shard sends
 * back a RowDescription ('T'), zero or more DataRow ('D') messages, and a
 * CommandComplete ('C') tag.  The proxy must merge these per-shard rows
 * into a single result set sent to the client (one RowDescription, all
 * DataRows concatenated, one aggregated CommandComplete).
 *
 * For non-RETURNING writes no 'T'/'D' is emitted and the buffers stay
 * empty; only `cmd_tag` is populated (e.g. "DELETE 5").
 */
typedef struct {
    uint8_t* rowdesc;       /* full 'T' message bytes (header + payload), or NULL */
    size_t   rowdesc_len;
    uint8_t* rows;          /* concatenation of full 'D' messages */
    size_t   rows_len;
    size_t   rows_cap;
    char     cmd_tag[96];   /* CommandComplete tag, e.g. "DELETE 3" */
} sc_write_capture_t;

static void sc_capture_init(sc_write_capture_t* c) {
    memset(c, 0, sizeof(*c));
}

static void sc_capture_free(sc_write_capture_t* c) {
    if (!c) return;
    keel_free(c->rowdesc);
    keel_free(c->rows);
    memset(c, 0, sizeof(*c));
}

static int sc_capture_append_row(sc_write_capture_t* c,
                                  const uint8_t hdr[5],
                                  const uint8_t* body, size_t bl)
{
    size_t need = c->rows_len + 5 + bl;
    if (need > c->rows_cap) {
        size_t ncap = c->rows_cap ? c->rows_cap : 4096;
        while (ncap < need) ncap *= 2;
        uint8_t* nb = (uint8_t*)keel_realloc(c->rows, ncap);
        if (!nb) return -1;
        c->rows = nb;
        c->rows_cap = ncap;
    }
    memcpy(c->rows + c->rows_len, hdr, 5);
    if (bl > 0 && body) memcpy(c->rows + c->rows_len + 5, body, bl);
    c->rows_len += 5 + bl;
    return 0;
}

/**
 * @brief Like sc_exec_cmd but capture RowDescription, DataRow, and the
 * CommandComplete tag emitted by the shard.  Used for the write SQL itself
 * so RETURNING rows are forwarded to the client.
 */
static int sc_exec_capture(int fd, const char* sql,
                            sc_write_capture_t* cap,
                            char* errbuf, size_t errlen)
{
    uint8_t qbuf[4096];
    size_t qlen = sc_build_query(qbuf, sizeof(qbuf), sql);
    if (qlen == 0 || sc_write_full(fd, qbuf, qlen) < 0) {
        snprintf(errbuf, errlen, "scatter-write: send failed for: %.128s", sql);
        return -1;
    }

    int rc = 0;
    uint8_t hdr[5];
    for (;;) {
        if (sc_read_full(fd, hdr, 5) < 0) {
            snprintf(errbuf, errlen, "scatter-write: read failed after: %.128s", sql);
            return -1;
        }
        char type   = (char)hdr[0];
        uint32_t ml = sc_get_u32(hdr + 1);
        if (ml < 4) { snprintf(errbuf, errlen, "scatter-write: bad msglen"); return -1; }
        uint32_t bl = ml - 4;

        uint8_t  sbuf[4096];
        uint8_t* body = NULL;
        bool     dyn  = false;
        if (bl > 0) {
            dyn  = (bl > sizeof(sbuf));
            body = dyn ? (uint8_t*)keel_malloc(bl) : sbuf;
            if (!body) { snprintf(errbuf, errlen, "scatter-write: OOM"); return -1; }
            if (sc_read_full(fd, body, bl) < 0) {
                if (dyn) keel_free(body);
                snprintf(errbuf, errlen, "scatter-write: read body failed");
                return -1;
            }
        }

        if (type == 'E' && bl > 0) {
            const uint8_t* p = body, *end = body + bl;
            while (p < end && *p) {
                char ft = (char)*p++;
                const char* fv = (const char*)p;
                size_t fl = strnlen(fv, (size_t)(end - p));
                p += fl + 1;
                if (ft == 'M') {
                    snprintf(errbuf, errlen, "scatter-write: backend error: %s", fv);
                    rc = -1;
                }
            }
        } else if (type == 'T' && cap && !cap->rowdesc && bl > 0) {
            /* Save the first RowDescription verbatim (header + body). */
            cap->rowdesc = (uint8_t*)keel_malloc((size_t)5 + bl);
            if (cap->rowdesc) {
                memcpy(cap->rowdesc, hdr, 5);
                memcpy(cap->rowdesc + 5, body, bl);
                cap->rowdesc_len = 5 + bl;
            }
        } else if (type == 'D' && cap) {
            (void)sc_capture_append_row(cap, hdr, body, bl);
        } else if (type == 'C' && cap && bl > 0) {
            /* CommandComplete tag is a NUL-terminated string at body[0..]. */
            size_t copy = bl < sizeof(cap->cmd_tag) ? bl : sizeof(cap->cmd_tag) - 1;
            memcpy(cap->cmd_tag, body, copy);
            cap->cmd_tag[copy] = '\0';
        }

        if (dyn) keel_free(body);
        if (type == 'Z') break; /* ReadyForQuery */
    }
    return rc;
}

/**
 * @brief Send a simple SQL command (no result rows expected) and drain to
 * ReadyForQuery.  Returns 0 on success, -1 if an ErrorResponse was received
 * or I/O failed.  Sets errbuf on failure.
 */
static int sc_exec_cmd(int fd, const char* sql, char* errbuf, size_t errlen)
{
    uint8_t qbuf[4096];
    size_t qlen = sc_build_query(qbuf, sizeof(qbuf), sql);
    if (qlen == 0 || sc_write_full(fd, qbuf, qlen) < 0) {
        snprintf(errbuf, errlen, "scatter-write: send failed for: %.128s", sql);
        return -1;
    }

    int rc = 0;
    uint8_t hdr[5];
    for (;;) {
        if (sc_read_full(fd, hdr, 5) < 0) {
            snprintf(errbuf, errlen, "scatter-write: read failed after: %.128s", sql);
            return -1;
        }
        char type   = (char)hdr[0];
        uint32_t ml = sc_get_u32(hdr + 1);
        if (ml < 4) { snprintf(errbuf, errlen, "scatter-write: bad msglen"); return -1; }
        uint32_t bl = ml - 4;
        if (bl > 0) {
            uint8_t* body = NULL;
            uint8_t sbuf[4096];
            bool dyn = (bl > sizeof(sbuf));
            body = dyn ? (uint8_t*)keel_malloc(bl) : sbuf;
            if (!body) { snprintf(errbuf, errlen, "scatter-write: OOM"); return -1; }
            if (sc_read_full(fd, body, bl) < 0) {
                if (dyn) keel_free(body);
                snprintf(errbuf, errlen, "scatter-write: read body failed");
                return -1;
            }
            if (type == 'E') { /* ErrorResponse */
                /* Extract message field 'M' */
                const uint8_t* p = body, *end = body + bl;
                while (p < end && *p) {
                    char ft = (char)*p++;
                    const char* fv = (const char*)p;
                    size_t fl = strnlen(fv, (size_t)(end - p));
                    p += fl + 1;
                    if (ft == 'M') {
                        snprintf(errbuf, errlen, "scatter-write: backend error: %s", fv);
                        rc = -1;
                    }
                }
            }
            if (dyn) keel_free(body);
        }
        if (type == 'Z') break; /* ReadyForQuery */
    }
    return rc;
}

int keel_engine_scatter_write(
    const keel_server_pool_t*      server_pool,
    struct backend_pool**          server_pools,
    size_t                         server_pool_count,
    const char*                    sql,
    const keel_dispatch_result_t*  dr,
    int                            client_fd)
{
    if (!server_pool || !sql || !dr || client_fd < 0) return -1;

    keel_2pc_coord_t* coord = (dr->twopc_required && dr->twopc)
                              ? (keel_2pc_coord_t*)dr->twopc : NULL;

    /* -----------------------------------------------------------------------
     * Per-shard state for the 2PC write protocol.
     * ----------------------------------------------------------------------- */
    typedef struct {
        backend_conn_t*    be;
        size_t             shard_idx;
        bool               prepared;  /* PREPARE TRANSACTION succeeded */
        bool               active;    /* BEGIN+write sent, not yet prepared */
        int                orig_flags; /* saved O_NONBLOCK flags for restore */
        char               errbuf[256];
        sc_write_capture_t cap;       /* RETURNING rows + CommandComplete tag */
    } sc_write_shard_t;

    sc_write_shard_t shards[SC_MAX_SHARDS];
    size_t nshards = 0;

    /* -----------------------------------------------------------------------
     * Phase 1: BEGIN + write SQL + PREPARE TRANSACTION on each shard.
     * ----------------------------------------------------------------------- */
    for (size_t si = 0;
         si < server_pool->count && si < server_pool_count && nshards < SC_MAX_SHARDS;
         si++) {
        const keel_backend_server_t* srv = &server_pool->servers[si];
        if (!srv->host || !srv->host[0] || !srv->healthy) continue;
        if (!server_pools || !server_pools[si]) continue;

        backend_conn_t* be = backend_pool_borrow(server_pools[si], 0);
        if (!be) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "scatter-write: shard %zu (%s:%u) — pool exhausted",
                si, srv->host, (unsigned)srv->port);
            continue;
        }

        int fd = be->fd;
        sc_write_shard_t* sh = &shards[nshards++];
        sh->be        = be;
        sh->shard_idx = si;
        sh->prepared  = false;
        sh->active    = false;
        sh->orig_flags = -1;
        sh->errbuf[0] = '\0';
        sc_capture_init(&sh->cap);

        /* Backend connections are O_NONBLOCK; sc_exec_cmd uses blocking I/O.
         * Temporarily clear O_NONBLOCK — restored before pool return in Phase 2. */
        sh->orig_flags = fcntl(fd, F_GETFL);
        if (sh->orig_flags < 0 || fcntl(fd, F_SETFL, sh->orig_flags & ~O_NONBLOCK) < 0) { /* NOLINT(keel-blocking): dedicated scatter worker thread */
            snprintf(sh->errbuf, sizeof sh->errbuf,
                     "scatter-write: fcntl(O_NONBLOCK) failed: %s", strerror(errno));
            close(sh->be->fd);
            sh->be->fd = -1;
            backend_pool_discard(server_pools[si], sh->be, BACKEND_CLOSE_REASON_IO_ERROR);
            continue;
        }

        /* BEGIN */
        if (sc_exec_cmd(fd, "BEGIN", sh->errbuf, sizeof sh->errbuf) != 0) {
            /* Connection is broken: close it and discard from the pool so
             * active_count is decremented and the slot can be refilled. */
            close(sh->be->fd);
            sh->be->fd = -1;
            backend_pool_discard(server_pools[si], sh->be, BACKEND_CLOSE_REASON_IO_ERROR);
            continue;
        }
        sh->active = true;

        /* Write SQL */
        if (sc_exec_capture(fd, sql, &sh->cap, sh->errbuf, sizeof sh->errbuf) != 0) {
            /* BEGIN was sent but the write failed.  The connection is now in
             * an unknown state (possibly mid-transaction).  Discard it so the
             * pool can refill the slot and avoid stale-transaction cascades. */
            close(sh->be->fd);
            sh->be->fd = -1;
            backend_pool_discard(server_pools[si], sh->be, BACKEND_CLOSE_REASON_IO_ERROR);
            sh->active = false;
            continue;
        }

        /* PREPARE TRANSACTION 'gid' */
        const char* gid = coord
            ? keel_2pc_coord_gid(coord, si)
            : NULL;

        if (gid) {
            char prepare_sql[320];
            snprintf(prepare_sql, sizeof prepare_sql,
                     "PREPARE TRANSACTION '%s'", gid);
            if (sc_exec_cmd(fd, prepare_sql, sh->errbuf, sizeof sh->errbuf) == 0) {
                sh->prepared = true;
                sh->active   = false;
                keel_2pc_coord_prepare(coord, si);
            } else {
                keel_2pc_coord_prepare_failed(coord, si);
            }
        } else {
            /* No 2PC: commit immediately (best-effort fanout) */
            sc_exec_cmd(fd, "COMMIT", sh->errbuf, sizeof sh->errbuf);
            sh->active = false;
        }
    }

    /* -----------------------------------------------------------------------
     * Phase 2: COMMIT PREPARED / ROLLBACK PREPARED / ROLLBACK.
     * ----------------------------------------------------------------------- */
    bool all_ok = !coord || keel_2pc_coord_all_prepared(coord);

    if (coord) {
        if (all_ok)
            keel_2pc_coord_commit_all(coord);
        else
            keel_2pc_coord_rollback_all(coord);
    }

    size_t failed = 0;
    for (size_t i = 0; i < nshards; i++) {
        sc_write_shard_t* sh = &shards[i];
        if (!sh->be) continue;
        int fd = sh->be->fd;
        bool conn_broken = false;

        if (sh->prepared) {
            const char* gid = keel_2pc_coord_gid(coord, sh->shard_idx);
            char phase2_sql[320];
            if (all_ok) {
                snprintf(phase2_sql, sizeof phase2_sql,
                         "COMMIT PREPARED '%s'", gid);
            } else {
                snprintf(phase2_sql, sizeof phase2_sql,
                         "ROLLBACK PREPARED '%s'", gid);
            }
            if (sc_exec_cmd(fd, phase2_sql, sh->errbuf, sizeof sh->errbuf) != 0) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "scatter-write: shard %zu phase-2 %s failed: %s",
                    sh->shard_idx, all_ok ? "COMMIT" : "ROLLBACK", sh->errbuf);
                failed++;
                conn_broken = true;
            }
        } else if (sh->active) {
            /* PREPARE didn't run; just rollback */
            sc_exec_cmd(fd, "ROLLBACK", sh->errbuf, sizeof sh->errbuf);
            failed++;
        } else if (sh->errbuf[0]) {
            failed++;
        }

        if (conn_broken) {
            close(sh->be->fd);
            sh->be->fd = -1;
            backend_pool_discard(server_pools[sh->shard_idx], sh->be, BACKEND_CLOSE_REASON_IO_ERROR);
        }
        /* Restore O_NONBLOCK before returning to pool. */
        if (!conn_broken && sh->be->fd >= 0 && sh->orig_flags >= 0)
            fcntl(sh->be->fd, F_SETFL, sh->orig_flags);
        backend_pool_return(server_pools[sh->shard_idx], sh->be,
                            /* recycle */ false);
    }

    /* -----------------------------------------------------------------------
     * Send merged result to client.
     *
     * For RETURNING-bearing scatter writes we forward the first shard's
     * RowDescription, then concatenate every shard's DataRows.  The
     * CommandComplete tag verb is taken from the first non-empty shard tag
     * (e.g. "DELETE", "UPDATE", "INSERT") and the count is the sum of the
     * per-shard tag counts (which equals the total number of RETURNING rows
     * for write commands).
     *
     * For non-RETURNING writes the capture has no RowDescription and no
     * DataRows; we only emit the aggregated CommandComplete.
     * ----------------------------------------------------------------------- */
    if (failed == 0 && all_ok) {
        /* Pick reference RowDescription (first shard that produced one). */
        const uint8_t* rd_bytes = NULL;
        size_t         rd_len   = 0;
        for (size_t i = 0; i < nshards; i++) {
            if (shards[i].cap.rowdesc) {
                rd_bytes = shards[i].cap.rowdesc;
                rd_len   = shards[i].cap.rowdesc_len;
                break;
            }
        }
        if (rd_bytes && rd_len > 0)
            (void)sc_write_full(client_fd, rd_bytes, rd_len);

        /* Forward all captured DataRows. */
        for (size_t i = 0; i < nshards; i++) {
            if (shards[i].cap.rows && shards[i].cap.rows_len > 0)
                (void)sc_write_full(client_fd, shards[i].cap.rows,
                                    shards[i].cap.rows_len);
        }

        /* Aggregate CommandComplete tag.  For each shard's tag, take verb
         * from the first one and sum the trailing integer count. */
        const char* verb     = NULL;
        size_t      verb_len = 0;
        uint64_t    total_rows = 0;
        for (size_t i = 0; i < nshards; i++) {
            const char* tag = shards[i].cap.cmd_tag;
            if (!tag[0]) continue;
            if (!verb) {
                const char* end = strchr(tag, ' ');
                verb     = tag;
                verb_len = end ? (size_t)(end - tag) : strlen(tag);
            }
            const char* sp = strrchr(tag, ' ');
            if (sp) {
                char* endp = NULL;
                unsigned long long v = strtoull(sp + 1, &endp, 10);
                if (endp != sp + 1) total_rows += (uint64_t)v;
            }
        }

        char out_tag[96];
        if (verb && verb_len > 0) {
            if (verb_len >= sizeof(out_tag) - 32) verb_len = sizeof(out_tag) - 32;
            if (verb_len == 6 && strncasecmp(verb, "INSERT", 6) == 0) {
                snprintf(out_tag, sizeof out_tag,
                         "INSERT 0 %llu", (unsigned long long)total_rows);
            } else {
                snprintf(out_tag, sizeof out_tag, "%.*s %llu",
                         (int)verb_len, verb,
                         (unsigned long long)total_rows);
            }
        } else {
            /* No tag captured: fall back to a benign UPDATE 0 tag. */
            snprintf(out_tag, sizeof out_tag, "UPDATE %llu",
                     (unsigned long long)nshards);
        }
        sc_send_command_complete_str(client_fd, out_tag);
    } else {
        sc_send_error(client_fd, "scatter-write: 2PC failed; transaction rolled back");
    }
    sc_send_ready_for_query(client_fd);

    /* Free per-shard capture buffers. */
    for (size_t i = 0; i < nshards; i++)
        sc_capture_free(&shards[i].cap);

    return (failed == 0 && all_ok) ? 0 : -1;
}
