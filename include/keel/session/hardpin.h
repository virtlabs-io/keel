/**
 * @file hardpin.h
 * @brief Conservative query scanners for detecting non-virtualizable backend affinity.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Some SQL features create backend-local state that KEEL cannot safely replay or
 * transfer to another server. When such a feature is observed, the session must
 * be hard-pinned: the currently borrowed backend remains exclusively attached to
 * that client until disconnect or explicit cleanup semantics are known.
 *
 * KEEL chooses a conservative scanner rather than a full SQL parser here. The
 * consequences are deliberate:
 *
 * - detection is cheap enough to run on the hot path;
 * - false positives are acceptable because they only reduce reuse;
 * - false negatives would be correctness bugs, so the keyword sets lean toward
 *   safety over precision.
 */

#ifndef KEEL_HARDPIN_H
#define KEEL_HARDPIN_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

/* ============================================================================
 * Hard-Pin Reason Bitmask
 *
 * Returned by the hardpin scanner and stored in session_t.pin_reason.
 * Plugin-to-core: these bits tell the core which constraints prevent backend
 * multiplexing for this session.
 * ============================================================================ */
typedef enum keel_pin_reason {
    KEEL_PIN_NONE              = 0,
    /* PostgreSQL reasons */
    KEEL_PIN_TEMP_TABLE        = (1 << 0),  /**< Temporary table active */
    KEEL_PIN_LISTEN            = (1 << 1),  /**< LISTEN active */
    KEEL_PIN_UNLISTEN          = (1 << 2),  /**< UNLISTEN pending */
    KEEL_PIN_DECLARE_CURSOR    = (1 << 3),  /**< Cursor declared */
    KEEL_PIN_COPY              = (1 << 4),  /**< COPY mode active */
    KEEL_PIN_SET_ROLE          = (1 << 5),  /**< SET ROLE active */
    KEEL_PIN_SET_SESSION_AUTH  = (1 << 6),  /**< SET SESSION AUTHORIZATION */
    KEEL_PIN_ADVISORY_LOCK     = (1 << 7),  /**< Advisory lock held */
    KEEL_PIN_REPLICATION       = (1 << 8),  /**< Replication slot */
    KEEL_PIN_EXTENDED_PROTOCOL = (1 << 9),  /**< Extended query protocol active */
    /* MySQL reasons */
    KEEL_PIN_GET_LOCK          = (1 << 10), /**< GET_LOCK() held */
    KEEL_PIN_USER_VARIABLE     = (1 << 11), /**< User variable set */
    KEEL_PIN_MYSQL_CURSOR      = (1 << 12), /**< Non-standard cursor */
    KEEL_PIN_PREPARED_STMT     = (1 << 13), /**< Prepared statement handle */
    /* Shared reasons */
    KEEL_PIN_SESSION_SET       = (1 << 14), /**< SET session-level param */
    KEEL_PIN_CURSOR            = (1 << 3),  /**< Alias for DECLARE_CURSOR */
    KEEL_PIN_LOCK_TABLE        = (1 << 15), /**< LOCK TABLES (MySQL) */
    KEEL_PIN_FOUND_ROWS        = (1 << 16), /**< SQL_CALC_FOUND_ROWS (MySQL) */
    /**
     * Online Schema Change (gh-ost / pt-osc) tool connection detected.
     *
     * Set when a query touches a gh-ost shadow table (_<t>_gho, _ghc, _del)
     * or a pt-osc shadow table (_<t>_new, _<t>_old), or carries a gh-ost
     * inline comment.  Implies: primary-only routing + exclusive backend affinity
     * (the OSC tool must see a consistent DDL state on a single backend).
     */
    KEEL_PIN_OSC               = (1 << 17), /**< Online schema change session */
} keel_pin_reason_t;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Scan a PostgreSQL statement for features that force exclusive backend affinity.
 *
 * @param query Query text, not required to be NUL-terminated.
 * @param len Query length in bytes.
 * @return Bitmask of detected pin reasons, or `KEEL_PIN_NONE`.
 */
keel_pin_reason_t keel_hardpin_scan_postgres(const char* query, size_t len);

/**
 * @brief Scan a MySQL statement for features that force exclusive backend affinity.
 *
 * @param query Query text, not required to be NUL-terminated.
 * @param len Query length in bytes.
 * @return Bitmask of detected pin reasons, or `KEEL_PIN_NONE`.
 */
keel_pin_reason_t keel_hardpin_scan_mysql(const char* query, size_t len);

/**
 * @brief Dispatch hard-pin scanning by protocol family name.
 *
 * @param proto_name Protocol identifier such as `"postgres"` or `"mysql"`.
 * @param query Query text to scan.
 * @param len Query length in bytes.
 * @return Protocol-specific pin-reason bitmask, or `KEEL_PIN_NONE` if unsupported.
 */
keel_pin_reason_t keel_hardpin_scan(const char* proto_name,
                                   const char* query, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_HARDPIN_H */
