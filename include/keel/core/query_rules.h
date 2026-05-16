/**
 * @file query_rules.h
 * @brief Declarative query routing and rewriting rules.
 *
 * Rules are loaded from [query_rule.N] INI sections and evaluated in order
 * at the KEEL_HOOK_BEFORE_ROUTE pipeline stage.
 *
 * Each rule may match on:
 *   match_regex  - POSIX extended regex matched against SQL text
 *   match_user   - exact client username
 *   match_db     - exact database name
 *
 * All specified matchers must match (AND semantics). First matching rule wins.
 *
 * Actions:
 *   action = route    route_to = primary | replica | any
 *   action = block    error_msg = <optional rejection message>
 *   action = rewrite  rewrite_to = <replacement SQL>
 *
 * Config example:
 *   [query_rule.0]
 *   match_regex = ^SELECT .* FROM audit_log
 *   action      = route
 *   route_to    = replica
 *
 *   [query_rule.1]
 *   match_regex = ^DROP
 *   action      = block
 *   error_msg   = DDL not allowed through the proxy
 */

#ifndef KEEL_QUERY_RULES_H
#define KEEL_QUERY_RULES_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel_hook.h"
#include "keel/core/ini.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Types
 * ============================================================================ */

/** Action to take when a rule matches. */
typedef enum keel_qr_action {
    KEEL_QR_ACTION_ROUTE   = 0,
    KEEL_QR_ACTION_BLOCK   = 1,
    KEEL_QR_ACTION_REWRITE = 2,
} keel_qr_action_t;

/** Routing target for action == KEEL_QR_ACTION_ROUTE. */
typedef enum keel_qr_route {
    KEEL_QR_ROUTE_PRIMARY = 0,
    KEEL_QR_ROUTE_REPLICA = 1,
    KEEL_QR_ROUTE_ANY     = 2,
} keel_qr_route_t;

/*
 * Opaque storage for a compiled POSIX regex_t.
 * 64 bytes covers regex_t on Linux x86-64 (actual size is 64 bytes).
 * Aligned to 8 bytes to satisfy the platform ABI.
 */
#define KEEL_REGEX_STORAGE_SIZE 64
typedef struct { _Alignas(8) char _buf[KEEL_REGEX_STORAGE_SIZE]; }
    keel_regex_storage_t;

/**
 * @brief A single compiled query rule.
 */
typedef struct keel_query_rule {
    /* Matchers */
    char*               match_regex;    /**< Original pattern string (owned) */
    keel_regex_storage_t regex_storage; /**< Compiled POSIX ERE (opaque) */
    bool                 regex_valid;   /**< true if regex compiled OK */
    char*               match_user;     /**< Username to match (owned, or NULL) */
    char*               match_db;       /**< Database to match (owned, or NULL) */

    /* Action */
    keel_qr_action_t    action;
    keel_qr_route_t     route_to;       /**< For ROUTE action */
    char*               error_msg;      /**< For BLOCK action (owned) */
    char*               rewrite_to;     /**< For REWRITE action (owned) */

    /* Pre-built PostgreSQL Simple Query ('Q') wire message for the rewrite SQL.
     * Allocated once at load time; length = 5 + strlen(rewrite_to) + 1.
     * Points to NULL when action != KEEL_QR_ACTION_REWRITE. */
    uint8_t*            rewrite_packet;     /**< Pre-built 'Q' wire message (owned) */
    size_t              rewrite_packet_len; /**< Length of rewrite_packet */

    /* Optional SQL rewrite policy knobs — applied on top of any REWRITE action.
     * When any of these are set, keel_sql_rewrite() is invoked in engine_flow.c
     * to prepend the corresponding SET statements before the SQL text. */
    uint32_t            statement_timeout_ms;  /**< >0: inject SET LOCAL statement_timeout */
    bool                force_read_only;       /**< true: inject SET TRANSACTION READ ONLY */
    char*               inject_search_path;    /**< non-NULL: inject SET search_path TO '...' */

    /* Metadata */
    char*               name;           /**< Section name (owned) */
    uint32_t            priority;       /**< Load order (section number) */
    bool                enabled;        /**< Rule active? */
} keel_query_rule_t;

/**
 * @brief Immutable, reference-counted rule list.
 */
typedef struct keel_query_rules {
    keel_query_rule_t* rules;
    size_t             count;
    uint32_t           refcnt;
} keel_query_rules_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

keel_query_rules_t* keel_query_rules_create(void);
void                keel_query_rules_destroy(keel_query_rules_t* rules);
void                keel_query_rules_ref(keel_query_rules_t* rules);
void                keel_query_rules_unref(keel_query_rules_t* rules);

/* ============================================================================
 * Config loading
 * ============================================================================ */

keel_error_t keel_query_rules_load(const keel_config_t* config,
                                   keel_query_rules_t** out);

/* ============================================================================
 * Hot-reload
 * ============================================================================ */

void keel_query_rules_replace(keel_query_rules_t** slot,
                              keel_query_rules_t*  new_rules);

/* ============================================================================
 * Hook integration
 * ============================================================================ */

bool keel_query_rules_hook(keel_hook_ctx_t* ctx);

/* ============================================================================
 * Introspection
 * ============================================================================ */

const char* keel_qr_action_name(keel_qr_action_t action);
const char* keel_qr_route_name(keel_qr_route_t route);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_QUERY_RULES_H */
