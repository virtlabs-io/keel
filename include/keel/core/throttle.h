/**
 * @file throttle.h
 * @brief Query-rate throttling and per-rule token-bucket rate limiting.
 *
 * Declarative rate limits loaded from [throttle.N] INI sections.  Each rule
 * defines a token bucket and a set of matchers.  When a query matches a rule
 * and the bucket is empty the query is rejected with a PostgreSQL error.
 *
 * Rule evaluation:
 *   - Rules are evaluated in index order.  The first matching rule wins.
 *   - All specified matchers must match (AND semantics).
 *   - If no rule matches the query passes through unthrottled.
 *
 * Token bucket algorithm:
 *   - Each rule maintains a global bucket (not per-client in v1).
 *   - Bucket refills at @c rate_rps tokens per second.
 *   - Maximum capacity is @c burst tokens (initial fill = burst).
 *   - One token is consumed per query.  If the bucket has no tokens the
 *     query is rejected.
 *
 * Configuration example:
 * ======================
 *   [throttle.0]
 *   ; Global ceiling: max 1 000 queries/s across all clients
 *   rate_rps  = 1000
 *   burst     = 500
 *
 *   [throttle.1]
 *   ; Per-user: "reporting" user capped at 50 queries/s globally
 *   match_user = reporting
 *   rate_rps   = 50
 *   burst      = 20
 *   error_msg  = reporting user rate limit exceeded
 *
 *   [throttle.2]
 *   ; Per-client connection: each client_fd gets its own 10 rps bucket
 *   per_client = true
 *   rate_rps   = 10
 *   burst      = 5
 *
 *   [throttle.3]
 *   ; Block expensive full-table scans
 *   match_regex = SELECT .* FROM large_table\b
 *   rate_rps    = 5
 *   burst       = 2
 *
 * Integration:
 * ============
 * Load rules at startup with keel_throttle_rules_load(), register the
 * returned instance on the hook registry as a KEEL_HOOK_BEFORE_ROUTE native
 * hook via keel_throttle_rules_register_hook().
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_THROTTLE_H
#define KEEL_THROTTLE_H

#include "keel_error.h"
#include "keel_hook.h"
#include "keel/core/ini.h"

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>

/* POSIX regex */
#define _POSIX_C_SOURCE 200809L
#include <regex.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Token Bucket
 * ============================================================================ */

/**
 * @brief A single token-bucket instance.
 *
 * Thread-safe: a mutex guards the refill+consume operation so that the
 * (tokens, last_refill_ns) pair is updated atomically.
 */
typedef struct keel_token_bucket {
    pthread_mutex_t  mutex;
    double           tokens;         /**< Current token count                */
    uint64_t         last_refill_ns; /**< Timestamp of last refill (ns)      */
    double           rate_rps;       /**< Token refill rate (per second)     */
    double           burst;          /**< Maximum token capacity             */
} keel_token_bucket_t;

/**
 * @brief Initialise a token bucket filled to @p burst.
 */
void keel_token_bucket_init(keel_token_bucket_t *tb,
                            double rate_rps,
                            double burst);

/**
 * @brief Attempt to consume one token.
 *
 * Refills the bucket proportionally to elapsed time since the last call,
 * then tries to take one token.
 *
 * @param tb      Bucket to consume from.
 * @param now_ns  Current monotonic time in nanoseconds.
 * @return        true if the token was granted, false if rate-limited.
 */
bool keel_token_bucket_consume(keel_token_bucket_t *tb, uint64_t now_ns);

/**
 * @brief Destroy a token bucket (releases mutex).
 */
void keel_token_bucket_destroy(keel_token_bucket_t *tb);

/* ============================================================================
 * Throttle Rule
 * ============================================================================ */

/* ============================================================================
 * Per-client bucket map (open-addressing hash table, fd → token_bucket)
 * ============================================================================ */

#define KEEL_THROTTLE_CLIENT_MAP_SLOTS 256  /**< Must be a power of two */

typedef struct keel_client_bucket_entry {
    int                   fd;      /**< client_fd key; -1 = empty slot      */
    keel_token_bucket_t   bucket;
} keel_client_bucket_entry_t;

/**
 * @brief Per-rule hash table mapping client_fd → token bucket.
 *
 * Fixed-size open-addressing table with linear probing.  Evicts the oldest
 * idle entry when full (clock-hand scan).
 */
typedef struct keel_client_bucket_map {
    keel_client_bucket_entry_t slots[KEEL_THROTTLE_CLIENT_MAP_SLOTS];
    pthread_mutex_t            mutex;
    double                     rate_rps;
    double                     burst;
} keel_client_bucket_map_t;

void keel_client_bucket_map_init(keel_client_bucket_map_t *m,
                                  double rate_rps, double burst);
void keel_client_bucket_map_destroy(keel_client_bucket_map_t *m);
bool keel_client_bucket_map_consume(keel_client_bucket_map_t *m,
                                     int client_fd, uint64_t now_ns);

/**
 * @brief A single throttle rule loaded from [throttle.N] INI section.
 */
typedef struct keel_throttle_rule {
    char            *name;           /**< INI section name (for logs)        */

    /* Matchers (all non-NULL matchers must match) */
    char            *match_regex;    /**< POSIX ERE against SQL text; NULL=any */
    char            *match_user;     /**< Exact client username; NULL=any    */
    char            *match_db;       /**< Exact database name; NULL=any      */

    /* Compiled regex */
    regex_t          regex_storage;  /**< Compiled POSIX regex               */
    bool             regex_valid;    /**< true if regex_storage is compiled   */

    /* Token bucket — global across all clients (when per_client=false) */
    keel_token_bucket_t bucket;

    /* Per-client bucket map (when per_client=true) */
    bool                     per_client;  /**< true = isolate rate per client_fd */
    keel_client_bucket_map_t client_map;  /**< Valid only when per_client=true   */

    /* Configuration */
    char            *error_msg;      /**< Rejection message; NULL=default    */
    bool             enabled;        /**< false = rule is skipped            */
} keel_throttle_rule_t;

/* ============================================================================
 * Throttle Rules Collection
 * ============================================================================ */

/**
 * @brief Collection of throttle rules with reference counting.
 */
typedef struct keel_throttle_rules {
    keel_throttle_rule_t *rules;     /**< Array of rules, ascending by index */
    size_t                count;
    int                   refcnt;    /**< Reference count for hot-reload     */

    /* Aggregate stats */
    _Atomic uint64_t      queries_throttled; /**< Total rejections            */
} keel_throttle_rules_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate an empty throttle rules collection.
 */
keel_throttle_rules_t *keel_throttle_rules_create(void);

/**
 * @brief Release one reference; free when refcnt reaches zero.
 */
void keel_throttle_rules_unref(keel_throttle_rules_t *tr);

/**
 * @brief Increment reference count.
 */
void keel_throttle_rules_ref(keel_throttle_rules_t *tr);

/**
 * @brief Load throttle rules from [throttle.N] INI sections.
 *
 * @param config  Parsed configuration.
 * @param out     Receives newly created rules.  *out is set to NULL on error.
 * @return        KEEL_OK, or an error code if allocation fails.
 */
keel_error_t keel_throttle_rules_load(const keel_config_t *config,
                                      keel_throttle_rules_t **out);

/**
 * @brief Atomically replace *slot with new_rules (thread-safe hot-reload).
 */
void keel_throttle_rules_replace(keel_throttle_rules_t **slot,
                                 keel_throttle_rules_t  *new_rules);

/* ============================================================================
 * Evaluation
 * ============================================================================ */

/**
 * @brief Check whether @p query should be throttled.
 *
 * Evaluates rules in order.  The first matching rule is consulted.
 *
 * @param tr          Rules collection.
 * @param sql         SQL text to classify.
 * @param username    Client username (may be NULL).
 * @param database    Target database (may be NULL).
 * @param client_fd   Client file descriptor; used as key for per-client buckets
 *                    (pass -1 if unavailable — falls back to global bucket).
 * @param now_ns      Current monotonic time in nanoseconds (for bucket refill).
 * @param[out] error_msg  Set to the rejection message when throttled.
 * @return            true if the query is throttled (should be rejected),
 *                    false if it should pass through.
 */
bool keel_throttle_check(keel_throttle_rules_t *tr,
                         const char *sql,
                         const char *username,
                         const char *database,
                         int         client_fd,
                         uint64_t    now_ns,
                         const char **error_msg);

/* ============================================================================
 * Hook integration
 * ============================================================================ */

/**
 * @brief Register the throttle rules as a KEEL_HOOK_BEFORE_ROUTE native hook.
 *
 * The hook evaluates rules on each routed query.  When a rule fires the hook
 * returns false and populates the hook context's error_msg field.
 *
 * @param registry  Hook registry to register into.
 * @param rules     Rules to evaluate.  Ownership is retained by the caller.
 * @return          KEEL_OK, or error on allocation failure.
 */
keel_error_t keel_throttle_rules_register_hook(keel_hook_registry_t  *registry,
                                               keel_throttle_rules_t *rules);

/* ============================================================================
 * Introspection
 * ============================================================================ */

/**
 * @brief Return total queries throttled across all rules.
 */
uint64_t keel_throttle_total_rejected(const keel_throttle_rules_t *tr);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_THROTTLE_H */
