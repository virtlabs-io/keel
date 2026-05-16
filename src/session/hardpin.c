/**
 * @file hardpin.c
 * @brief Conservative SQL scanners for detecting features that disable multiplexing.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The hard-pin scanner is intentionally not a full parser. Its job is to catch
 * obviously stateful constructs cheaply enough for the frontend hot path. When
 * in doubt it prefers over-pinning, because reduced reuse is operationally safer
 * than accidentally reassigning a backend that still carries client-specific
 * state.
 */

#include "keel/session/hardpin.h"

#include <string.h>
#include <ctype.h>

/* ============================================================================
 * Internal: Case-insensitive substring search
 * ============================================================================ */

/**
 * @brief Find the first case-insensitive occurrence of a byte substring.
 *
 * @param haystack Buffer to search.
 * @param hlen Haystack length in bytes.
 * @param needle Target substring.
 * @param nlen Needle length in bytes.
 * @return Pointer to the first match, or `NULL` if absent.
 */
static const char* ci_memmem(const char* haystack, size_t hlen,
                              const char* needle, size_t nlen)
{
    if (nlen == 0) return haystack;
    if (nlen > hlen) return NULL;

    for (size_t i = 0; i <= hlen - nlen; i++) {
        bool match = true;
        for (size_t j = 0; j < nlen; j++) {
            if (tolower((unsigned char)haystack[i + j]) !=
                tolower((unsigned char)needle[j])) {
                match = false;
                break;
            }
        }
        if (match) return &haystack[i];
    }
    return NULL;
}

/**
 * @brief Search for a case-insensitive keyword bounded by non-alphanumeric edges.
 *
 * This avoids matching identifiers that merely contain the keyword as a suffix or
 * prefix, which keeps the scanner conservative without being wildly noisy.
 *
 * @param haystack Buffer to search.
 * @param hlen Haystack length.
 * @param needle Keyword to match.
 * @param nlen Keyword length.
 * @return `true` if a word-bounded match is found.
 */
static bool ci_contains_word(const char* haystack, size_t hlen,
                              const char* needle, size_t nlen)
{
    const char* p = haystack;
    size_t remaining = hlen;

    while (remaining >= nlen) {
        const char* found = ci_memmem(p, remaining, needle, nlen);
        if (!found) return false;

        size_t offset = (size_t)(found - haystack);
        bool left_ok  = (found == haystack) ||
                        !isalnum((unsigned char)found[-1]);
        bool right_ok = (offset + nlen >= hlen) ||
                        !isalnum((unsigned char)found[nlen]);

        if (left_ok && right_ok) return true;

        /* Advance past this occurrence */
        size_t skip = (size_t)(found - p) + 1;
        p += skip;
        remaining -= skip;
    }
    return false;
}

/**
 * @brief Trim leading ASCII whitespace from a query prefix.
 *
 * @param s Query pointer.
 * @param len [in,out] Remaining length after trimming.
 * @return Pointer to the first non-whitespace byte.
 */
static const char* skip_ws(const char* s, size_t* len)
{
    while (*len > 0 && isspace((unsigned char)*s)) {
        s++; (*len)--;
    }
    return s;
}

/**
 * @brief Test whether a statement begins with a keyword token.
 *
 * Prefix tests are cheaper and usually more reliable for statement-class
 * detection than arbitrary substring scans.
 *
 * @param query Query text.
 * @param len Query length in bytes.
 * @param kw Keyword to match.
 * @param kwlen Keyword length.
 * @return `true` if the trimmed statement starts with the keyword token.
 */
static bool starts_with_kw(const char* query, size_t len,
                            const char* kw, size_t kwlen)
{
    const char* q = skip_ws(query, &len);
    if (len < kwlen) return false;

    for (size_t i = 0; i < kwlen; i++) {
        if (tolower((unsigned char)q[i]) != tolower((unsigned char)kw[i]))
            return false;
    }
    /* Must be followed by whitespace or end */
    return (len == kwlen) || !isalnum((unsigned char)q[kwlen]);
}

/* ============================================================================
 * OSC (Online Schema Change) Shadow-Table Scanner — shared by pg + mysql
 * ============================================================================ */

/**
 * @brief Detect queries that belong to an Online Schema Change tool session.
 *
 * gh-ost uses:
 *   - An inline comment `/ * gh-ost * /` (without spaces) in every DML it runs
 *   - Shadow tables: _<orig>_gho (ghost), _<orig>_ghc (changelog), _<orig>_del
 *
 * pt-online-schema-change uses:
 *   - Shadow tables: _<orig>_new, _<orig>_old
 *   - Heartbeat table: _pt_heartbeat or pt_osc_<db>_<table>_new
 *
 * Detection is intentionally conservative (false-positive safe, not strict):
 * - Any occurrence of the `gh-ost` comment or the `_gho`/`_ghc`/`_del` suffixes
 *   is enough to set the pin.  Regular tables that happen to end in `_gho` are
 *   an edge-case the operator can mitigate via a declarative query rule.
 * - `_new` / `_old` suffixes on identifiers starting with `_` match pt-osc.
 */
static keel_pin_reason_t keel_hardpin_scan_osc(const char* query, size_t len)
{
    if (!query || len == 0) return KEEL_PIN_NONE;

    /* ── gh-ost inline comment ──────────────────────────────────────────── */
    /* Search for the literal 7-byte sequence "gh-ost" */
    static const char ghc[] = "gh-ost";
    for (size_t i = 0; i + 6 <= len; i++) {
        if (query[i]   == 'g' && query[i+1] == 'h' && query[i+2] == '-' &&
            query[i+3] == 'o' && query[i+4] == 's' && query[i+5] == 't') {
            return KEEL_PIN_OSC;
        }
    }
    (void)ghc;

    /* ── Identifier suffix scan ─────────────────────────────────────────── *
     * Walk the query looking for identifiers (alphanumeric + underscore).
     * When an identifier starts with '_', check for known OSC suffixes.    */
    size_t i = 0;
    while (i < len) {
        /* Find the start of an identifier that begins with '_' */
        if (query[i] != '_') { i++; continue; }

        /* Consume the full identifier: _, then alphanumeric / underscore */
        size_t start = i;
        i++;
        while (i < len && (query[i] == '_' || isalnum((unsigned char)query[i])))
            i++;

        size_t id_len = i - start;
        if (id_len < 5) continue;  /* too short for any suffix */

        const char* id = query + start;

        /* gh-ost suffixes: _gho, _ghc, _del */
        if (id_len >= 4) {
            const char* tail = id + id_len - 4;
            if ((tail[0] == '_') &&
                ((tail[1]=='g' && tail[2]=='h' && tail[3]=='o') ||  /* _gho */
                 (tail[1]=='g' && tail[2]=='h' && tail[3]=='c') ||  /* _ghc */
                 (tail[1]=='d' && tail[2]=='e' && tail[3]=='l')))   /* _del */
                return KEEL_PIN_OSC;
        }

        /* pt-osc suffixes: _new, _old (only on identifiers that start with _) */
        if (id_len >= 4) {
            const char* tail = id + id_len - 4;
            if ((tail[0] == '_') &&
                ((tail[1]=='n' && tail[2]=='e' && tail[3]=='w') ||  /* _new */
                 (tail[1]=='o' && tail[2]=='l' && tail[3]=='d')))   /* _old */
                return KEEL_PIN_OSC;
        }

        /* pt-osc heartbeat: _pt_heartbeat (13 chars) or _pt_osc_* (9+ chars) */
        if (id_len >= 9 &&
            id[0]=='_' && id[1]=='p' && id[2]=='t' && id[3]=='_') {
            /* _pt_heartbeat (id_len >= 13) or _pt_osc_* (id_len >= 9) */
            if ((id_len >= 13 &&
                 id[4]=='h' && id[5]=='e' && id[6]=='a' && id[7]=='r') ||
                (id_len >= 9  &&
                 id[4]=='o' && id[5]=='s' && id[6]=='c' && id[7]=='_'))
                return KEEL_PIN_OSC;
        }
    }

    return KEEL_PIN_NONE;
}

/* ============================================================================
 * PostgreSQL Hard-Pin Scanner
 * ============================================================================ */

/**
 * @brief Scan a PostgreSQL statement for backend-local features that forbid reuse.
 */
keel_pin_reason_t keel_hardpin_scan_postgres(const char* query, size_t len)
{
    if (!query || len == 0) return KEEL_PIN_NONE;

    keel_pin_reason_t reasons = KEEL_PIN_NONE;

    /* --- LISTEN: pins the backend for async notification delivery.
     * UNLISTEN releases the pin (handled by classify_sql pin_clr).
     * NOTIFY sends a notification but does not create session state. */
    if (starts_with_kw(query, len, "listen", 6)) {
        reasons |= KEEL_PIN_LISTEN;
    }

    /* --- TEMP TABLE --- */
    if (ci_contains_word(query, len, "temp", 4) &&
        ci_contains_word(query, len, "table", 5)) {
        reasons |= KEEL_PIN_TEMP_TABLE;
    }
    if (ci_contains_word(query, len, "temporary", 9) &&
        ci_contains_word(query, len, "table", 5)) {
        reasons |= KEEL_PIN_TEMP_TABLE;
    }

    /* --- DECLARE CURSOR --- */
    if (starts_with_kw(query, len, "declare", 7) &&
        ci_contains_word(query, len, "cursor", 6)) {
        reasons |= KEEL_PIN_CURSOR;
    }

    /* --- SET ROLE / SET SESSION AUTHORIZATION --- */
    if (starts_with_kw(query, len, "set", 3)) {
        if (ci_contains_word(query, len, "role", 4)) {
            reasons |= KEEL_PIN_SET_ROLE;
        }
        if (ci_contains_word(query, len, "session", 7) &&
            ci_contains_word(query, len, "authorization", 13)) {
            reasons |= KEEL_PIN_SET_ROLE;
        }
        /* SET session-level parameter (not SET LOCAL) */
        if (!ci_contains_word(query, len, "local", 5)) {
            reasons |= KEEL_PIN_SESSION_SET;
        }
    }

    /* --- Advisory locks --- */
    if (ci_contains_word(query, len, "pg_advisory_lock", 16) ||
        ci_contains_word(query, len, "pg_try_advisory_lock", 20)) {
        reasons |= KEEL_PIN_ADVISORY_LOCK;
    }

    /* --- COPY --- */
    if (starts_with_kw(query, len, "copy", 4)) {
        reasons |= KEEL_PIN_COPY;
    }

    /* --- PREPARE (named prepared statement) --- */
    if (starts_with_kw(query, len, "prepare", 7)) {
        reasons |= KEEL_PIN_PREPARED_STMT;
    }

    /* --- DISCARD ALL --- */
    if (starts_with_kw(query, len, "discard", 7)) {
        /* DISCARD ALL is actually a de-pin event, but we flag it so the
         * engine can clear the profile. Not a pin reason per se — clear
         * any existing pin. Return 0 for DISCARD. */
        return KEEL_PIN_NONE;
    }

    /* --- Online Schema Change (gh-ost / pt-osc) --- */
    reasons |= keel_hardpin_scan_osc(query, len);

    return reasons;
}

/* ============================================================================
 * MySQL Hard-Pin Scanner
 * ============================================================================ */

/**
 * @brief Scan a MySQL statement for backend-local features that forbid reuse.
 */
keel_pin_reason_t keel_hardpin_scan_mysql(const char* query, size_t len)
{
    if (!query || len == 0) return KEEL_PIN_NONE;

    keel_pin_reason_t reasons = KEEL_PIN_NONE;

    /* --- GET_LOCK / RELEASE_LOCK --- */
    if (ci_contains_word(query, len, "get_lock", 8) ||
        ci_contains_word(query, len, "release_lock", 12) ||
        ci_contains_word(query, len, "is_free_lock", 12)) {
        reasons |= KEEL_PIN_GET_LOCK;
    }

    /* --- User variables (@var) --- */
    for (size_t i = 0; i < len; i++) {
        if (query[i] == '@' && (i + 1 < len) && query[i + 1] != '@') {
            /* Single @ followed by non-@ = user variable */
            reasons |= KEEL_PIN_USER_VARIABLE;
            break;
        }
    }

    /* --- CREATE TEMPORARY TABLE --- */
    if (ci_contains_word(query, len, "temporary", 9) &&
        ci_contains_word(query, len, "table", 5)) {
        reasons |= KEEL_PIN_TEMP_TABLE;
    }

    /* --- LOCK TABLES --- */
    if (starts_with_kw(query, len, "lock", 4) &&
        ci_contains_word(query, len, "tables", 6)) {
        reasons |= KEEL_PIN_LOCK_TABLE;
    }

    /* --- SQL_CALC_FOUND_ROWS / FOUND_ROWS() --- */
    if (ci_contains_word(query, len, "sql_calc_found_rows", 19) ||
        ci_contains_word(query, len, "found_rows", 10)) {
        reasons |= KEEL_PIN_FOUND_ROWS;
    }

    /* --- PREPARE / EXECUTE / DEALLOCATE (SQL-level prepared statements) --- */
    if (starts_with_kw(query, len, "prepare", 7) ||
        starts_with_kw(query, len, "execute", 7) ||
        starts_with_kw(query, len, "deallocate", 10)) {
        reasons |= KEEL_PIN_PREPARED_STMT;
    }

    /* --- SET (session-level variables) --- */
    if (starts_with_kw(query, len, "set", 3)) {
        /* SET @@session.var or SET GLOBAL don't pin, but plain SET does */
        if (!ci_contains_word(query, len, "global", 6)) {
            reasons |= KEEL_PIN_SESSION_SET;
        }
    }

    /* --- Online Schema Change (gh-ost / pt-osc) --- */
    reasons |= keel_hardpin_scan_osc(query, len);

    return reasons;
}

/* ============================================================================
 * Generic Dispatcher
 * ============================================================================ */

/**
 * @brief Dispatch to the protocol-specific scanner selected by `proto_name`.
 */
keel_pin_reason_t keel_hardpin_scan(const char* proto_name,
                                   const char* query, size_t len)
{
    if (!proto_name || !query || len == 0) return KEEL_PIN_NONE;

    if (strcasecmp(proto_name, "postgres") == 0 ||
        strcasecmp(proto_name, "postgresql") == 0) {
        return keel_hardpin_scan_postgres(query, len);
    }

    if (strcasecmp(proto_name, "mysql") == 0) {
        return keel_hardpin_scan_mysql(query, len);
    }

    return KEEL_PIN_NONE;
}
