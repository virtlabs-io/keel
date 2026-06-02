/**
 * @file router_weighted.c
 * @brief Core read/write routing and weighted server-selection logic.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This module owns KEEL's default routing policy. It decides whether a query
 * may use a replica, builds the eligible server set for the decision, and then
 * selects one concrete backend using the configured balancing strategy.
 *
 * Major responsibilities:
 * - transaction- and session-aware read/write split decisions
 * - weighted primary/replica balancing for read traffic
 * - health-aware server exclusion and degraded-server weight reduction
 * - operational stats about routing outcomes and failovers
 */

#include "keel/core/router.h"
#include "keel/core/sharding.h"
#include "keel/core/ini.h"
#include "keel/core/config_reload.h"
#include "keel/core/scatter_2pc.h"
#include "keel/parser/parser_registry.h"
#include "keel/sql/query_tree.h"
#include "keel/sql/sql_ast.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/encoding.h"
#include "keel/util/util.h"

#include <stdatomic.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

static keel_qt_query_t* router_parse_postgresql_sql(keel_str_t sql,
                                                    keel_parse_result_t* result)
{
    if (!result) return NULL;

    const keel_parser_plugin_ops_t* ops = keel_parser_builtin_postgresql_sql();
    keel_parse_input_t input = {
        .data = (const uint8_t*)sql.data,
        .len = sql.len,
        .language = KEEL_LANG_SQL,
        .dialect = KEEL_DIALECT_SQL_POSTGRESQL,
    };

    keel_parse_status_t st = ops->parse(&input, result);
    if (st != KEEL_PARSE_OK) {
        return NULL;
    }
    return keel_parse_result_legacy_qt(result);
}

/* ============================================================================
 * Default timing constants
 * ============================================================================
 * Override via INI (per-group):
 *   health_check_interval_ms = 5000
 *   connect_timeout_ms       = 5000
 *   query_timeout_ms         = 30000
 */

/** Health-check probe interval in milliseconds */
#define KEEL_DEFAULT_HEALTH_CHECK_INTERVAL_MS  5000U
/** Backend connect timeout in milliseconds */
#define KEEL_DEFAULT_CONNECT_TIMEOUT_MS        5000U
/** Backend query (health-check) timeout in milliseconds */
#define KEEL_DEFAULT_QUERY_TIMEOUT_MS         30000U

/* ============================================================================
 * Route-reason name table
 * ============================================================================ */

const char* keel_route_reason_name(keel_route_reason_t r)
{
    switch (r) {
    case KEEL_ROUTE_REASON_NORMAL:            return "NORMAL";
    case KEEL_ROUTE_REASON_IN_TRANSACTION:    return "IN_TRANSACTION";
    case KEEL_ROUTE_REASON_PINNED_SESSION:    return "PINNED_SESSION";
    case KEEL_ROUTE_REASON_PINNED_PS:         return "PINNED_PS";
    case KEEL_ROUTE_REASON_HARD_PINNED:       return "HARD_PINNED";
    case KEEL_ROUTE_REASON_WRITE_REQUIRED:    return "WRITE_REQUIRED";
    case KEEL_ROUTE_REASON_READ_SPLIT:        return "READ_SPLIT";
    case KEEL_ROUTE_REASON_FAILOVER_PRIMARY:  return "FAILOVER_PRIMARY";
    case KEEL_ROUTE_REASON_LAG_EXCEEDED:      return "LAG_EXCEEDED";
    case KEEL_ROUTE_REASON_HEALTH_DEGRADED:   return "HEALTH_DEGRADED";
    case KEEL_ROUTE_REASON_NO_PRIMARY:        return "NO_PRIMARY";
    case KEEL_ROUTE_REASON_CID_BLOCKED:       return "CID_BLOCKED";
    case KEEL_ROUTE_REASON_TIMELINE_STALE:    return "TIMELINE_STALE";
    case KEEL_ROUTE_REASON_PATRONI_UNAVAIL:   return "PATRONI_UNAVAIL";
    case KEEL_ROUTE_REASON_ROLE_FLAPPING:     return "ROLE_FLAPPING";
    case KEEL_ROUTE_REASON_DDL:               return "DDL";
    case KEEL_ROUTE_REASON_TRANSACTION_CTRL:  return "TRANSACTION_CTRL";
    case KEEL_ROUTE_REASON_SEMANTIC_UNSAFE:   return "SEMANTIC_UNSAFE";
    case KEEL_ROUTE_REASON_CONSISTENCY_PRIMARY: return "CONSISTENCY_PRIMARY";
    case KEEL_ROUTE_REASON_UNKNOWN_FUNCTION:  return "UNKNOWN_FUNCTION";
    case KEEL_ROUTE_REASON_COMMIT_AMBIGUOUS:  return "COMMIT_AMBIGUOUS";
    case KEEL_ROUTE_REASON_OLD_PRIMARY_FENCED:return "OLD_PRIMARY_FENCED";
    case KEEL_ROUTE_REASON_DEGRADED_MODE:     return "DEGRADED_MODE";
    case KEEL_ROUTE_REASON_WAIT_CATCHUP:      return "WAIT_CATCHUP";
    default:                                  return "UNKNOWN";
    }
}

/* Factor name table: parallel array to the bit positions of
 * `keel_route_factor_t`. NULL slots are reserved for future flags so the
 * indices stay stable across versions. */
static const char* const k_route_factor_names[32] = {
    [0]  = "IN_TRANSACTION",
    [1]  = "SESSION_PINNED",
    [2]  = "HAS_TEMP_TABLE",
    [3]  = "STMT_CLASS_WRITE",
    [4]  = "STMT_CLASS_DDL",
    [5]  = "STMT_CLASS_TXN_CTL",
    [6]  = "UNKNOWN_FUNCTION",
    [7]  = "VOLATILE_FUNCTION",
    [8]  = "SECURITY_DEFINER",
    [9]  = "WRITE_TRIGGER",
    [10] = "WRITE_RULE",
    [11] = "PARSE_FAILED",
    [12] = "REPLICA_LAG",
    [13] = "NO_REPLICAS",
    [14] = "FAILOVER_FALLBACK",
    [15] = "STICKY_PRIMARY",
    [16] = "COMMIT_IN_DOUBT",
    [17] = "REPLICA_OK",
    [18] = "USER_PINNED",
    [19] = "CONSISTENCY_TOKEN",
    [20] = "NODE_FENCED",
    [21] = "DEGRADED_MODE",
};

size_t keel_route_factors_to_json_array(uint32_t factors,
                                        char* out,
                                        size_t out_size)
{
    if (!out || out_size == 0) {
        return 0;
    }
    size_t pos = 0;
    int written = snprintf(out + pos, out_size - pos, "[");
    if (written > 0) pos += (size_t)written;
    bool first = true;
    for (int i = 0; i < 32; ++i) {
        if (!(factors & (1u << i)) || !k_route_factor_names[i]) continue;
        if (pos >= out_size) break;
        written = snprintf(out + pos, out_size - pos, "%s\"%s\"",
                           first ? "" : ",",
                           k_route_factor_names[i]);
        if (written > 0) pos += (size_t)written;
        first = false;
    }
    if (pos < out_size) {
        written = snprintf(out + pos, out_size - pos, "]");
        if (written > 0) pos += (size_t)written;
    }
    return pos;
}

size_t keel_route_decision_to_json(const keel_route_decision_t* decision,
                                   uint64_t query_hash,
                                   char* out,
                                   size_t out_size)
{
    if (!out || out_size == 0) {
        return 0;
    }

    if (!decision) {
        int n = snprintf(out, out_size, "{\"error\":\"missing_decision\"}");
        return n > 0 ? (size_t)n : 0;
    }

    char server_name[128];
    char reason_text[256];
    keel_json_escape(server_name, sizeof server_name,
                     decision->server ? decision->server->name : "");
    keel_json_escape(reason_text, sizeof reason_text,
                     decision->reason ? decision->reason : "");

    const char* role = "unknown";
    if (decision->server) {
        role = decision->server->role == KEEL_SERVER_PRIMARY
            ? "primary"
            : "replica";
    }

    char shard[32];
    if (decision->shard_index == SIZE_MAX) {
        snprintf(shard, sizeof shard, "null");
    } else {
        snprintf(shard, sizeof shard, "%zu", decision->shard_index);
    }

    char factors_arr[512];
    keel_route_factors_to_json_array(decision->decision_factors,
                                     factors_arr, sizeof factors_arr);

    int n = snprintf(out, out_size,
        "{\"query_hash\":\"0x%016llx\","
        "\"route\":\"%s\","
        "\"server\":\"%s\","
        "\"is_read\":%s,"
        "\"was_pinned\":%s,"
        "\"shard_index\":%s,"
        "\"reason_code\":\"%s\","
        "\"reason\":\"%s\","
        "\"factors\":%s}",
        (unsigned long long)query_hash,
        role,
        server_name,
        decision->is_read ? "true" : "false",
        decision->was_pinned ? "true" : "false",
        shard,
        keel_route_reason_name(decision->reason_code),
        reason_text,
        factors_arr);
    return n > 0 ? (size_t)n : 0;
}

/* ============================================================================
 * Internal Types
 * ============================================================================ */

/** Maximum servers per router */
#define KEEL_ROUTER_MAX_SERVERS 64

/** Maximum shard rules per router */
#define KEEL_ROUTER_MAX_SHARD_RULES 16

/**
 * @brief Internal server entry with runtime state
 */
typedef struct router_server {
    keel_route_server_t  config;         /**< User-visible config */
    char*               name;           /**< Owned copy of name */
    char*               host;           /**< Owned copy of host */
    bool                active;         /**< Server is active */

    /* Failover-manager role state — orthogonal to config.role (RW/RO/WO)
     * and config.health. Owned by keel_router_observe_primary() and the
     * manual override API. */
    keel_node_role_state_t role_state;

    /* Weighted selection state */
    int                 effective_weight; /**< Current effective weight */
    int                 current_weight;   /**< Weight for current round */

    /* Statistics */
    uint64_t            routes;         /**< Times selected */
    uint64_t            errors;         /**< Routing errors */
} router_server_t;

/**
 * @brief Router structure
 */
struct keel_router {
    keel_router_config_t config;
    
    /* Server lists */
    router_server_t     servers[KEEL_ROUTER_MAX_SERVERS];
    size_t              server_count;
    
    /* Index caches for quick lookup by role */
    size_t              rw_indices[KEEL_ROUTER_MAX_SERVERS];
    size_t              rw_count;
    size_t              ro_indices[KEEL_ROUTER_MAX_SERVERS];
    size_t              ro_count;
    size_t              wo_indices[KEEL_ROUTER_MAX_SERVERS];
    size_t              wo_count;
    
    /* Round-robin state */
    size_t              rr_write_idx;
    size_t              rr_read_idx;
    size_t              rr_all_idx;
    
    /* Random state */
    unsigned int        rand_seed;
    
    /* Statistics */
    keel_router_stats_t  stats;
    
    /* Mutex protecting temp_arena for concurrent dispatch from multiple workers */
    pthread_mutex_t      dispatch_mutex;

    /* Arena for temporary allocations (query parsing) */
    keel_arena_t*        temp_arena;

    /* Shard rule registry */
    keel_shard_rule_t    shard_rules[KEEL_ROUTER_MAX_SHARD_RULES];
    char*               shard_rule_tables[KEEL_ROUTER_MAX_SHARD_RULES];  /* owned copies */
    char*               shard_rule_columns[KEEL_ROUTER_MAX_SHARD_RULES]; /* owned copies */
    size_t              shard_rule_count;

    /* Failover-manager state (proposal §3). The mutex guards `epoch` and
     * per-server `role_state` transitions; the routing fast path reads
     * `role_state` without the lock — `rebuild_indices()` runs under the
     * same observer that updated it, and stale reads at most cause one
     * extra eligible candidate that the next route call will exclude. */
    pthread_mutex_t      epoch_mu;
    keel_cluster_epoch_t epoch;
    _Atomic bool         degraded_mode; /**< True when no PRIMARY is currently observed. */
};

/* ============================================================================
 * Default Configuration
 * ============================================================================ */

/**
 * @brief Return the default router configuration.
 *
 * @return Fully initialized default configuration structure.
 */
keel_router_config_t keel_router_config_default(void) {
    return (keel_router_config_t){
        .strategy = KEEL_ROUTE_STRATEGY_WEIGHTED_ROUND_ROBIN,
        .read_write_split = true,
        .primary_read_weight = 0.5,     /* Primary gets half replica weight for reads */
        .failover_to_primary = true,
        .auto_detect_readonly = true,
        .health_check_enabled = true,
        .health_check_interval = KEEL_MSEC(KEEL_DEFAULT_HEALTH_CHECK_INTERVAL_MS),
        .health_check_retries = 3,
        .connect_timeout = KEEL_MSEC(KEEL_DEFAULT_CONNECT_TIMEOUT_MS),
        .query_timeout = KEEL_MSEC(KEEL_DEFAULT_QUERY_TIMEOUT_MS),
        /* Scatter is experimental; operators must opt in per worker group. */
        .scatter_merge_enabled = false,
        .consistency_mode = KEEL_CONSISTENCY_READ_YOUR_WRITES,
        .max_replica_lag_bytes = 16ULL * 1024ULL * 1024ULL,
        .max_replica_catchup_ms = 50,
        .stale_read_policy = KEEL_STALE_READ_ROUTE_PRIMARY,
        .failover = {
            .provider                     = KEEL_FAILOVER_PROVIDER_NONE,
            .detection_interval_ms        = 500,
            .failure_threshold            = 3,
            .promotion_grace_ms           = 3000,
            .old_primary_fencing_required = true,
            .allow_ambiguous_write_retry  = false,
            .read_during_failover         = KEEL_FAILOVER_READ_PRIMARY_ONLY,
            .transaction_during_failover  = KEEL_FAILOVER_TXN_FAIL,
        },
    };
}

/* ============================================================================
 * Router Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate and initialize a router instance.
 *
 * @param config Optional caller-supplied configuration, or `NULL` for defaults.
 * @return New router on success, or `NULL` on allocation failure.
 */
keel_router_t* keel_router_create(const keel_router_config_t* config) {
    keel_router_t* router = keel_calloc(1, sizeof(keel_router_t));
    if (!router) {
        return NULL;
    }
    
    router->config = config ? *config : keel_router_config_default();
    router->rand_seed = (unsigned int)time(NULL);

    /* Create temporary arena for query parsing */
    router->temp_arena = keel_arena_create(4096);
    if (!router->temp_arena) {
        keel_free(router);
        return NULL;
    }

    /* Initialize dispatch mutex for thread-safe concurrent access to temp_arena */
    pthread_mutex_init(&router->dispatch_mutex, NULL);

    /* Failover-manager epoch state. Generation starts at 0 ("no primary
     * observed yet"); the first keel_router_observe_primary() bumps it to
     * 1 and exits degraded mode. */
    pthread_mutex_init(&router->epoch_mu, NULL);
    memset(&router->epoch, 0, sizeof(router->epoch));
    atomic_store(&router->degraded_mode, false);

    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Created router: strategy=%d, read_write_split=%d, primary_read_weight=%.2f",
                 router->config.strategy,
                 router->config.read_write_split,
                 router->config.primary_read_weight);
    
    return router;
}

/**
 * @brief Destroy a router and release all owned memory.
 *
 * @param router Router handle, or `NULL`.
 * @return
 */
void keel_router_destroy(keel_router_t* router) {
    if (!router) {
        return;
    }
    
    /* Free server name/host strings */
    for (size_t i = 0; i < router->server_count; i++) {
        keel_free(router->servers[i].name);
        keel_free(router->servers[i].host);
    }

    /* Free shard rule strings */
    for (size_t i = 0; i < router->shard_rule_count; i++) {
        keel_free(router->shard_rule_tables[i]);
        keel_free(router->shard_rule_columns[i]);
    }
    
    if (router->temp_arena) {
        keel_arena_destroy(router->temp_arena);
    }

    pthread_mutex_destroy(&router->dispatch_mutex);
    pthread_mutex_destroy(&router->epoch_mu);

    keel_free(router);
}

/* ============================================================================
 * Server Management
 * ============================================================================ */

/**
 * @brief Rebuild per-role index caches after server membership changes.
 *
 * @param router Router handle.
 * @return
 */
static void rebuild_indices(keel_router_t* router) {
    router->rw_count = 0;
    router->ro_count = 0;
    router->wo_count = 0;
    
    for (size_t i = 0; i < router->server_count; i++) {
        if (!router->servers[i].active) {
            continue;
        }
        /* Failover-manager fencing: DEMOTED and DRAINING servers are
         * removed from the routing pool. The server entry remains in the
         * topology (visible to admin / metrics) but no new traffic is
         * dispatched to it. DEMOTED is sticky until the operator unfences
         * via keel_router_set_node_role_state(). */
        keel_node_role_state_t rs = router->servers[i].role_state;
        if (rs == KEEL_NODE_STATE_DEMOTED ||
            rs == KEEL_NODE_STATE_DRAINING) {
            continue;
        }

        keel_server_role_t role = router->servers[i].config.role;
        
        switch (role) {
        case KEEL_SERVER_ROLE_RW:
            router->rw_indices[router->rw_count++] = i;
            break;
        case KEEL_SERVER_ROLE_RO:
            router->ro_indices[router->ro_count++] = i;
            break;
        case KEEL_SERVER_ROLE_WO:
            router->wo_indices[router->wo_count++] = i;
            break;
        default: /* AUTO — not indexed until probe resolves */
            break;
        }
    }
}

/**
 * @brief Recompute effective per-server weights for the next selection cycle.
 *
 * @param router Router handle.
 * @param for_reads `true` when computing weights for read routing.
 * @return
 */
static void calculate_weights(keel_router_t* router, bool for_reads) {
    for (size_t i = 0; i < router->server_count; i++) {
        router_server_t* srv = &router->servers[i];
        
        if (!srv->active || srv->config.health == KEEL_HEALTH_DOWN) {
            srv->effective_weight = 0;
            continue;
        }
        
        int base_weight = srv->config.weight > 0 ? srv->config.weight : 100;
        
        if (for_reads && srv->config.role == KEEL_SERVER_ROLE_RW) {
            /* RW nodes get reduced weight for reads (prefer RO nodes) */
            srv->effective_weight = (int)(base_weight * router->config.primary_read_weight);
        } else {
            srv->effective_weight = base_weight;
        }
        
        /* Apply health-based reduction for degraded servers */
        if (srv->config.health == KEEL_HEALTH_DEGRADED) {
            srv->effective_weight /= 2;
        }
    }
}

/**
 * @brief Add one server to the router's server table.
 *
 * @param router Router handle.
 * @param server Server definition to copy.
 * @return `KEEL_OK` on success, or an error for invalid input, duplicates,
 *         overflow, or allocation failure.
 */
keel_error_t keel_router_add_server(keel_router_t* router, 
                                   const keel_route_server_t* server) {
    if (!router || !server || !server->name || !server->host) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    if (router->server_count >= KEEL_ROUTER_MAX_SERVERS) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Router server limit reached");
        return KEEL_ERR_OVERFLOW;
    }
    
    /* Check for duplicate name */
    for (size_t i = 0; i < router->server_count; i++) {
        if (router->servers[i].active && 
            strcmp(router->servers[i].name, server->name) == 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Duplicate server name: %s", server->name);
            return KEEL_ERR_ALREADY_EXISTS;
        }
    }
    
    /* Add server */
    router_server_t* srv = &router->servers[router->server_count];
    memset(srv, 0, sizeof(*srv));
    
    srv->config = *server;
    srv->name = keel_strdup(server->name);
    srv->host = keel_strdup(server->host);
    srv->config.name = srv->name;
    srv->config.host = srv->host;
    srv->config.health = KEEL_HEALTH_UNKNOWN;
    srv->active = true;
    srv->effective_weight = server->weight > 0 ? server->weight : 100;
    srv->current_weight = 0;
    
    if (!srv->name || !srv->host) {
        keel_free(srv->name);
        keel_free(srv->host);
        return KEEL_ERR_NOMEM;
    }
    
    router->server_count++;
    rebuild_indices(router);
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Added server: name=%s host=%s:%d role=%s weight=%d",
                 server->name, server->host, server->port,
                 server->role == KEEL_SERVER_ROLE_RW ? "RW" :
                 server->role == KEEL_SERVER_ROLE_RO ? "RO" :
                 server->role == KEEL_SERVER_ROLE_WO ? "WO" : "AUTO",
                 server->weight);
    
    return KEEL_OK;
}

/**
 * @brief Logically remove a server by name.
 *
 * @param router Router handle.
 * @param name Server name.
 * @return `KEEL_OK` if found, otherwise an error code.
 */
keel_error_t keel_router_remove_server(keel_router_t* router, const char* name) {
    if (!router || !name) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    for (size_t i = 0; i < router->server_count; i++) {
        if (router->servers[i].active && 
            strcmp(router->servers[i].name, name) == 0) {
            router->servers[i].active = false;
            rebuild_indices(router);
            KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, "Removed server: %s", name);
            return KEEL_OK;
        }
    }
    
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Find a configured active server by name.
 *
 * @param router Router handle.
 * @param name Server name.
 * @return Mutable server config pointer, or `NULL` if not found.
 */
keel_route_server_t* keel_router_get_server(keel_router_t* router, const char* name) {
    if (!router || !name) {
        return NULL;
    }
    
    for (size_t i = 0; i < router->server_count; i++) {
        if (router->servers[i].active && 
            strcmp(router->servers[i].name, name) == 0) {
            return &router->servers[i].config;
        }
    }
    
    return NULL;
}

/**
 * @brief Update the health state of a named server.
 *
 * @param router Router handle.
 * @param name Server name.
 * @param health New health state.
 * @return
 */
void keel_router_set_server_health(keel_router_t* router, 
                                   const char* name,
                                   keel_server_health_t health) {
    keel_route_server_t* srv = keel_router_get_server(router, name);
    if (srv) {
        keel_server_health_t old_health = srv->health;
        srv->health = health;
        srv->last_health_check = keel_time_now();
        
        if (old_health != health) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                         "Server %s health changed: %d -> %d",
                         name, old_health, health);
        }
    }
}

/* ============================================================================
 * Failover manager — node role state and cluster epoch
 * ============================================================================
 *
 * The failover manager owns the role-state axis (PRIMARY/REPLICA/DEMOTED/...)
 * and a monotonic cluster epoch. Topology observers (Patroni REST, SQL
 * discovery, MySQL probe) call keel_router_observe_primary() after each
 * refresh; the manager increments the epoch on flip, fences the previous
 * primary per `failover.old_primary_fencing_required`, and rebuilds the
 * routing indices so subsequent dispatches do not touch the demoted node.
 *
 * v0.5-alpha fencing is process-level only: KEEL refuses to route NEW
 * traffic to a DEMOTED node. In-flight transactions still observe the
 * existing commit-in-doubt semantics in engine_flow. External STONITH or
 * IP-takeover style fencing is left as a future opt-in hook.
 */

const char* keel_node_role_state_name(keel_node_role_state_t s)
{
    switch (s) {
    case KEEL_NODE_STATE_UNKNOWN:   return "UNKNOWN";
    case KEEL_NODE_STATE_PRIMARY:   return "PRIMARY";
    case KEEL_NODE_STATE_REPLICA:   return "REPLICA";
    case KEEL_NODE_STATE_UNHEALTHY: return "UNHEALTHY";
    case KEEL_NODE_STATE_DRAINING:  return "DRAINING";
    case KEEL_NODE_STATE_DEMOTED:   return "DEMOTED";
    default:                        return "UNKNOWN";
    }
}

/* Find the internal slot for `name`. Returns NULL when not found or when
 * `name`/`router` are NULL. Caller must hold whatever consistency guarantee
 * it needs — this is a private lookup. */
static router_server_t* find_server_slot(keel_router_t* router, const char* name)
{
    if (!router || !name) return NULL;
    for (size_t i = 0; i < router->server_count; i++) {
        if (router->servers[i].active &&
            strcmp(router->servers[i].name, name) == 0) {
            return &router->servers[i];
        }
    }
    return NULL;
}

void keel_router_get_cluster_epoch(const keel_router_t* router,
                                   keel_cluster_epoch_t* out)
{
    if (!out) return;
    memset(out, 0, sizeof(*out));
    if (!router) return;
    pthread_mutex_lock((pthread_mutex_t*)&router->epoch_mu);
    *out = router->epoch;
    pthread_mutex_unlock((pthread_mutex_t*)&router->epoch_mu);
}

bool keel_router_observe_primary(keel_router_t* router,
                                 const char* name,
                                 uint32_t timeline_id)
{
    if (!router) return false;

    pthread_mutex_lock(&router->epoch_mu);

    /* Case A: caller signals "primary lost". Move into degraded mode and
     * mark the previously-known primary UNHEALTHY so the routing pool
     * cannot drain into a dead node. We deliberately do NOT bump the
     * epoch — the cluster is between primaries; the next concrete
     * observation will flip generation. */
    if (!name || !*name) {
        bool flipped = false;
        if (router->epoch.primary_name[0] != '\0') {
            router_server_t* prev = find_server_slot(router,
                                                    router->epoch.primary_name);
            if (prev && prev->role_state == KEEL_NODE_STATE_PRIMARY) {
                prev->role_state = KEEL_NODE_STATE_UNHEALTHY;
                rebuild_indices(router);
                flipped = true;
            }
            router->epoch.primary_name[0] = '\0';
        }
        atomic_store(&router->degraded_mode, true);
        pthread_mutex_unlock(&router->epoch_mu);
        if (flipped) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "failover: primary lost; router entered DEGRADED mode "
                "(read_during_failover=%d, transaction_during_failover=%d)",
                (int)router->config.failover.read_during_failover,
                (int)router->config.failover.transaction_during_failover);
        }
        return flipped;
    }

    /* Case B: same primary observed (no-op). */
    if (strcmp(router->epoch.primary_name, name) == 0 &&
        router->epoch.timeline_id == timeline_id) {
        /* If we were in degraded mode and the observer re-confirms the
         * primary, exit degraded mode but keep generation. */
        if (atomic_load(&router->degraded_mode)) {
            atomic_store(&router->degraded_mode, false);
            KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                "failover: primary '%s' re-confirmed; router exited DEGRADED mode",
                name);
        }
        pthread_mutex_unlock(&router->epoch_mu);
        return false;
    }

    /* Case C: real flip. Fence the previous primary if any, promote the
     * new one, bump generation. */
    bool fencing_required = router->config.failover.old_primary_fencing_required;
    char prev_name[64];
    snprintf(prev_name, sizeof(prev_name), "%s", router->epoch.primary_name);

    if (prev_name[0] != '\0' && strcmp(prev_name, name) != 0) {
        router_server_t* prev = find_server_slot(router, prev_name);
        if (prev) {
            prev->role_state = fencing_required
                ? KEEL_NODE_STATE_DEMOTED
                : KEEL_NODE_STATE_DRAINING;
            /* Demote the previous primary's static role so that if the
             * operator later clears the fence with
             * keel_router_set_node_role_state(..., UNKNOWN), the node
             * rejoins the pool as a replica, not as a stale primary. */
            prev->config.role = KEEL_SERVER_REPLICA;
        }
    }

    router_server_t* cur = find_server_slot(router, name);
    if (cur) {
        cur->role_state  = KEEL_NODE_STATE_PRIMARY;
        cur->config.role = KEEL_SERVER_PRIMARY;
        if (timeline_id != 0) {
            cur->config.timeline_id = timeline_id;
        }
    }

    router->epoch.generation++;
    snprintf(router->epoch.primary_name, sizeof(router->epoch.primary_name),
             "%s", name);
    router->epoch.timeline_id   = timeline_id;
    router->epoch.observed_at_ms = (uint64_t)(keel_time_now() / 1000000LL);

    rebuild_indices(router);
    atomic_store(&router->degraded_mode, false);

    uint64_t new_gen = router->epoch.generation;
    pthread_mutex_unlock(&router->epoch_mu);

    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
        "failover: epoch flip generation=%llu primary='%s' timeline=%u "
        "(previous='%s' state=%s fencing=%s)",
        (unsigned long long)new_gen, name, (unsigned)timeline_id,
        prev_name[0] ? prev_name : "(none)",
        fencing_required ? "DEMOTED" : "DRAINING",
        fencing_required ? "on" : "off");
    return true;
}

keel_error_t keel_router_set_node_role_state(keel_router_t* router,
                                             const char* name,
                                             keel_node_role_state_t state)
{
    if (!router || !name) return KEEL_ERR_INVALID_ARG;
    pthread_mutex_lock(&router->epoch_mu);
    router_server_t* srv = find_server_slot(router, name);
    if (!srv) {
        pthread_mutex_unlock(&router->epoch_mu);
        return KEEL_ERR_NOT_FOUND;
    }
    keel_node_role_state_t old = srv->role_state;
    srv->role_state = state;
    rebuild_indices(router);
    pthread_mutex_unlock(&router->epoch_mu);
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
        "failover: manual override server='%s' role_state %s -> %s",
        name,
        keel_node_role_state_name(old),
        keel_node_role_state_name(state));
    return KEEL_OK;
}

keel_node_role_state_t keel_router_get_node_role_state(const keel_router_t* router,
                                                       const char* name)
{
    if (!router || !name) return KEEL_NODE_STATE_UNKNOWN;
    for (size_t i = 0; i < router->server_count; i++) {
        if (router->servers[i].active &&
            strcmp(router->servers[i].name, name) == 0) {
            return router->servers[i].role_state;
        }
    }
    return KEEL_NODE_STATE_UNKNOWN;
}

/**
 * @brief Count active servers of a given role that are usable for routing.
 *
 * @param router Router handle.
 * @param role Target role.
 * @return Number of healthy-or-unknown servers for that role.
 */
size_t keel_router_count_healthy(const keel_router_t* router, keel_server_role_t role) {
    if (!router) {
        return 0;
    }
    
    size_t count = 0;
    for (size_t i = 0; i < router->server_count; i++) {
        const router_server_t* srv = &router->servers[i];
        if (srv->active && srv->config.role == role &&
            (srv->config.health == KEEL_HEALTH_UP || 
             srv->config.health == KEEL_HEALTH_UNKNOWN)) {
            count++;
        }
    }
    return count;
}

/* ============================================================================
 * Weighted Selection Algorithms
 * ============================================================================ */

/**
 * @brief Select one server using smooth weighted round-robin.
 *
 * This algorithm provides smooth distribution across servers.
 * Each server has a current_weight that starts at 0.
 * On each selection:
 *   1. Add effective_weight to each server's current_weight
 *   2. Select server with highest current_weight
 *   3. Subtract total_weight from selected server's current_weight
 */
static router_server_t* select_weighted_round_robin(
    keel_router_t* router,
    size_t* indices,
    size_t count
) {
    if (count == 0) {
        return NULL;
    }
    
    int total_weight = 0;
    router_server_t* best = NULL;
    int best_weight = 0;
    
    /* Pass 1: Add effective weights and find best */
    for (size_t i = 0; i < count; i++) {
        router_server_t* srv = &router->servers[indices[i]];
        
        if (!srv->active || srv->config.health == KEEL_HEALTH_DOWN) {
            continue;
        }
        
        srv->current_weight += srv->effective_weight;
        total_weight += srv->effective_weight;
        
        if (!best || srv->current_weight > best_weight) {
            best = srv;
            best_weight = srv->current_weight;
        }
    }
    
    if (!best) {
        return NULL;
    }
    
    /* Subtract total weight from selected server */
    best->current_weight -= total_weight;
    
    return best;
}

/**
 * @brief Select one server using a weighted random draw.
 *
 * @param router Router handle.
 * @param indices Candidate server indices.
 * @param count Number of candidates.
 * @return Selected server, or `NULL` if no candidate is routable.
 */
static router_server_t* select_random_weighted(
    keel_router_t* router,
    size_t* indices,
    size_t count
) {
    if (count == 0) {
        return NULL;
    }
    
    /* Calculate total weight */
    int total_weight = 0;
    for (size_t i = 0; i < count; i++) {
        router_server_t* srv = &router->servers[indices[i]];
        if (srv->active && srv->config.health != KEEL_HEALTH_DOWN) {
            total_weight += srv->effective_weight;
        }
    }
    
    if (total_weight == 0) {
        return NULL;
    }
    
    /* Random selection */
    int target = rand_r(&router->rand_seed) % total_weight;
    int current = 0;
    
    for (size_t i = 0; i < count; i++) {
        router_server_t* srv = &router->servers[indices[i]];
        if (!srv->active || srv->config.health == KEEL_HEALTH_DOWN) {
            continue;
        }
        
        current += srv->effective_weight;
        if (current > target) {
            return srv;
        }
    }
    
    /* Fallback to first available */
    for (size_t i = 0; i < count; i++) {
        router_server_t* srv = &router->servers[indices[i]];
        if (srv->active && srv->config.health != KEEL_HEALTH_DOWN) {
            return srv;
        }
    }
    
    return NULL;
}

/**
 * @brief Select one server from a prepared candidate list using the configured strategy.
 *
 * @param router Router handle.
 * @param indices Candidate indices.
 * @param count Candidate count.
 * @param for_reads `true` when performing read routing.
 * @return Selected server, or `NULL` if none is available.
 */
static router_server_t* select_server(
    keel_router_t* router,
    size_t* indices,
    size_t count,
    bool for_reads
) {
    if (count == 0) {
        return NULL;
    }
    
    /* Calculate effective weights */
    calculate_weights(router, for_reads);
    
    router_server_t* selected = NULL;
    
    switch (router->config.strategy) {
    case KEEL_ROUTE_STRATEGY_WEIGHTED_ROUND_ROBIN:
        selected = select_weighted_round_robin(router, indices, count);
        break;
        
    case KEEL_ROUTE_STRATEGY_RANDOM:
        selected = select_random_weighted(router, indices, count);
        break;
        
    case KEEL_ROUTE_STRATEGY_ROUND_ROBIN:
        /* Simple round-robin without weights */
        for (size_t attempts = 0; attempts < count; attempts++) {
            size_t idx = router->rr_all_idx++ % count;
            router_server_t* srv = &router->servers[indices[idx]];
            if (srv->active && srv->config.health != KEEL_HEALTH_DOWN) {
                selected = srv;
                break;
            }
        }
        break;
        
    case KEEL_ROUTE_STRATEGY_FIRST_AVAILABLE:
        for (size_t i = 0; i < count; i++) {
            router_server_t* srv = &router->servers[indices[i]];
            if (srv->active && srv->config.health != KEEL_HEALTH_DOWN) {
                selected = srv;
                break;
            }
        }
        break;
        
    case KEEL_ROUTE_STRATEGY_LEAST_CONNECTIONS:
        /* Select server with least active connections */
        {
            int min_conns = INT32_MAX;
            for (size_t i = 0; i < count; i++) {
                router_server_t* srv = &router->servers[indices[i]];
                if (!srv->active || srv->config.health == KEEL_HEALTH_DOWN) {
                    continue;
                }
                if (srv->config.active_conns < min_conns) {
                    min_conns = srv->config.active_conns;
                    selected = srv;
                }
            }
        }
        break;
    }
    
    return selected;
}

/* ============================================================================
 * Routing Decision
 * ============================================================================ */

/**
 * @brief Determine whether a query is eligible for replica routing.
 *
 * @param router Router handle.
 * @param qt Parsed query tree, or `NULL`.
 * @param session Session routing context, or `NULL`.
 * @return `true` if the query may run on a read-only node.
 */
static bool should_use_readonly(
    keel_router_t* router,
    const keel_qt_query_t* qt,
    const keel_route_session_t* session
) {
    /* Not enabled? */
    if (!router->config.read_write_split) {
        return false;
    }

    if (router->config.consistency_mode == KEEL_CONSISTENCY_PRIMARY_ONLY) {
        return false;
    }
    
    /* Session is pinned? */
    if (session && session->pinned_server) {
        return false;  /* Will use pinned server regardless */
    }

    if (router->config.consistency_mode == KEEL_CONSISTENCY_READ_YOUR_WRITES &&
        session && session->requires_consistent_read) {
        return false;
    }
    
    /* In a transaction? Must stick to same server */
    if (session && session->in_transaction) {
        return false;  /* Transactions go to RW */
    }
    
    /* Has temp tables? Need to stay on same connection */
    if (session && session->has_temp_tables) {
        return false;
    }
    
    if (!qt) {
        return false;
    }

    if (qt->has_error ||
        (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE) ||
        qt->func_count > 0) {
        return false;
    }

    return keel_qt_can_use_replica(qt);
}

static bool query_is_replica_safe_read(const keel_qt_query_t* qt)
{
    if (!qt) {
        return false;
    }
    if (qt->has_error ||
        (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE) ||
        qt->func_count > 0) {
        return false;
    }
    return keel_qt_can_use_replica(qt);
}

static bool consistency_forces_primary(
    const keel_router_t* router,
    const keel_qt_query_t* qt,
    const keel_route_session_t* session
) {
    if (!router || !query_is_replica_safe_read(qt)) {
        return false;
    }
    if (session && session->pinned_server) {
        return false;
    }
    if (session && (session->in_transaction || session->has_temp_tables)) {
        return false;
    }
    if (router->config.consistency_mode == KEEL_CONSISTENCY_PRIMARY_ONLY) {
        return true;
    }
    return router->config.consistency_mode == KEEL_CONSISTENCY_READ_YOUR_WRITES &&
           session &&
           session->requires_consistent_read;
}

/**
 * @brief Should the router emit a WAIT_CATCHUP decision for this request?
 *
 * True when the operator picked `stale_read_policy=wait` AND the session
 * carries a consistency token (`requires_consistent_read` + non-empty
 * `required_consistency_token.value`). The actual replica-safety check
 * is done by the caller via `consistency_forces_primary()` — this helper
 * only encodes the policy precondition that flips the consistency path
 * from primary-fallback to park-and-probe.
 *
 * Returning true does not by itself emit WAIT_CATCHUP: the read path also
 * needs an eligible replica. If no replica is available the routing logic
 * falls back to the legacy primary path with `KEEL_DF_NO_REPLICAS`.
 */
static bool should_wait_for_catchup(
    const keel_router_t* router,
    const keel_route_session_t* session)
{
    if (!router || !session) {
        return false;
    }
    if (router->config.stale_read_policy != KEEL_STALE_READ_WAIT) {
        return false;
    }
    if (router->config.consistency_mode != KEEL_CONSISTENCY_READ_YOUR_WRITES) {
        return false;
    }
    if (!session->requires_consistent_read) {
        return false;
    }
    return session->required_consistency_token.value[0] != '\0';
}

/**
 * @brief Build the read-routing candidate list.
 *
 * @return Number of indices written to `out_indices`.
 */
static size_t build_read_indices(
    keel_router_t* router,
    size_t* out_indices,
    size_t max
) {
    size_t count = 0;
    
    /* Add all RO servers */
    for (size_t i = 0; i < router->ro_count && count < max; i++) {
        out_indices[count++] = router->ro_indices[i];
    }
    
    /* Add RW servers if configured for read distribution */
    if (router->config.primary_read_weight > 0) {
        for (size_t i = 0; i < router->rw_count && count < max; i++) {
            out_indices[count++] = router->rw_indices[i];
        }
    }
    
    return count;
}

/**
 * @brief Build the read-routing candidate list for a specific shard.
 *
 * @param router       Router whose server index arrays are consulted.
 * @param shard_index  Zero-based shard identifier; only servers whose
 *                     `shard_id` matches are included.
 * @param out_indices  Caller-allocated array of at least `max` elements that
 *                     receives the indices of eligible servers.
 * @param max          Maximum number of indices to write.
 * @return Number of indices written to `out_indices`.
 *
 * Notes:
 * - Read-only (RO) servers for the shard are listed first.
 * - When `router->config.primary_read_weight > 0`, read-write (RW) servers
 *   for the shard are appended so the primary can serve read queries.
 * - Returns 0 when no servers match `shard_index`.
 */
static size_t build_read_indices_for_shard(
    keel_router_t* router,
    size_t shard_index,
    size_t* out_indices,
    size_t max
) {
    size_t count = 0;

    for (size_t i = 0; i < router->ro_count && count < max; i++) {
        size_t idx = router->ro_indices[i];
        if (router->servers[idx].config.shard_id == shard_index) {
            out_indices[count++] = idx;
        }
    }

    if (router->config.primary_read_weight > 0) {
        for (size_t i = 0; i < router->rw_count && count < max; i++) {
            size_t idx = router->rw_indices[i];
            if (router->servers[idx].config.shard_id == shard_index) {
                out_indices[count++] = idx;
            }
        }
    }

    return count;
}

/**
 * @brief Build the write-routing candidate list.
 *
 * @return Number of indices written to `out_indices`.
 */
static size_t build_write_indices(
    keel_router_t* router,
    size_t* out_indices,
    size_t max
) {
    size_t count = 0;
    
    /* Add all RW servers */
    for (size_t i = 0; i < router->rw_count && count < max; i++) {
        out_indices[count++] = router->rw_indices[i];
    }
    
    /* Add WO servers */
    for (size_t i = 0; i < router->wo_count && count < max; i++) {
        out_indices[count++] = router->wo_indices[i];
    }
    
    return count;
}

/**
 * @brief Build the write-routing candidate list for a specific shard.
 *
 * @param router       Router whose server index arrays are consulted.
 * @param shard_index  Zero-based shard identifier.
 * @param out_indices  Caller-allocated array of at least `max` elements.
 * @param max          Maximum number of indices to write.
 * @return Number of indices written to `out_indices`.
 *
 * Notes:
 * - Read-write (RW) servers for the shard are listed first, followed by
 *   write-only (WO) servers.
 * - Returns 0 when no servers match `shard_index`.
 */
static size_t build_write_indices_for_shard(
    keel_router_t* router,
    size_t shard_index,
    size_t* out_indices,
    size_t max
) {
    size_t count = 0;

    for (size_t i = 0; i < router->rw_count && count < max; i++) {
        size_t idx = router->rw_indices[i];
        if (router->servers[idx].config.shard_id == shard_index) {
            out_indices[count++] = idx;
        }
    }

    for (size_t i = 0; i < router->wo_count && count < max; i++) {
        size_t idx = router->wo_indices[i];
        if (router->servers[idx].config.shard_id == shard_index) {
            out_indices[count++] = idx;
        }
    }

    return count;
}

/**
 * @brief Core routing dispatcher — selects a server for a query.
 *
 * @param router           Router instance.
 * @param qt               Pre-parsed query tree, used to determine read vs.
 *                         write classification.  May be `NULL` if parsing
 *                         failed; the router falls back to writes in that case.
 * @param session          Current session state, or `NULL` for stateless
 *                         routing.  Controls session pinning, transaction
 *                         state, and scatter-write validation.
 * @param use_shard_filter When `true`, only servers matching `shard_index` are
 *                         considered.
 * @param shard_index      Target shard (ignored when `use_shard_filter` is
 *                         `false`).
 * @param[out] decision    Populated with the selected server, routing reason,
 *                         and read/write flag.
 * @return `KEEL_OK` on success, `KEEL_ERR_SHARD_CROSS_TX` when the requested
 *         shard was not part of the active scatter-write transaction,
 *         `KEEL_ERR_UNAVAILABLE` when no eligible server is available, or
 *         `KEEL_ERR_INVALID_ARG` for `NULL` router/decision pointers.
 *
 * Routing steps (in order):
 * 1. Session-pinned server shortcut.
 * 2. Cross-transaction scatter-write shard validation (Feature 7).
 * 3. `should_use_readonly()` classification.
 * 4. Candidate list construction via `build_read_indices_for_shard()` or
 *    `build_write_indices_for_shard()` (shard-filtered) or their non-sharded
 *    counterparts.
 * 5. `select_server()` weighted selection.
 * 6. Optional failover to primary when no replica is available.
 */
static keel_error_t route_internal_ex(keel_router_t* router,
                                      const keel_qt_query_t* qt,
                                      const keel_route_session_t* session,
                                      bool use_shard_filter,
                                      size_t shard_index,
                                      keel_route_decision_t* decision,
                                      bool simulate) {
    if (!router || !decision) {
        return KEEL_ERR_INVALID_ARG;
    }

    memset(decision, 0, sizeof(*decision));
    decision->shard_index = use_shard_filter ? shard_index : SIZE_MAX;
    if (!simulate) router->stats.total_routes++;

    /* Commit-in-doubt is sacred: refuse to route any further query while
     * the session has an unresolved COMMIT outcome. The caller (engine) is
     * responsible for surfacing the resolution to the client; routing here
     * would risk side-effects on a fresh backend that the client cannot
     * reconcile with the uncertain prior commit. */
    if (session && session->commit_in_doubt) {
        decision->reason      = "commit-in-doubt: prior COMMIT outcome unresolved";
        decision->reason_code = KEEL_ROUTE_REASON_COMMIT_AMBIGUOUS;
        decision->decision_factors |= KEEL_DF_COMMIT_IN_DOUBT;
        return KEEL_ERR_UNAVAILABLE;
    }

    /* Failover-manager: degraded mode gating.
     *
     * When keel_router_observe_primary() was called with NULL (primary
     * unreachable) and no concrete new primary has been observed yet, the
     * router enters degraded mode. Per the [failover] policy:
     *   - read_during_failover=REJECT       : refuse reads (default ALLOW)
     *   - transaction_during_failover=REJECT: refuse anything inside a tx
     *     (and refuse writes outright, since they cannot be routed)
     */
    if (atomic_load(&router->degraded_mode)) {
        bool is_read = query_is_replica_safe_read(qt);
        bool in_tx   = session && session->in_transaction;
        bool reject_reads =
            router->config.failover.read_during_failover
                == KEEL_FAILOVER_READ_REJECT;
        bool reject_tx =
            router->config.failover.transaction_during_failover
                == KEEL_FAILOVER_TXN_FAIL;
        bool refuse =
            (!is_read)                         /* writes can never succeed */
            || (reject_reads && is_read)
            || (reject_tx   && in_tx);
        if (refuse) {
            decision->is_read     = is_read;
            decision->reason      = "degraded mode: cluster has no primary";
            decision->reason_code = KEEL_ROUTE_REASON_DEGRADED_MODE;
            decision->decision_factors |= KEEL_DF_DEGRADED_MODE;
            return KEEL_ERR_UNAVAILABLE;
        }
    }

    if (session && session->pinned_server) {
        decision->server      = session->pinned_server;
        decision->was_pinned  = true;
        decision->reason      = "session pinned";
        decision->reason_code = KEEL_ROUTE_REASON_PINNED_SESSION;
        decision->decision_factors |= KEEL_DF_SESSION_PINNED;
        if (!simulate) router->stats.pinned_routes++;
        return KEEL_OK;
    }

    if (router->config.consistency_mode == KEEL_CONSISTENCY_READ_YOUR_WRITES &&
        router->config.stale_read_policy == KEEL_STALE_READ_REJECT &&
        session && session->requires_consistent_read &&
        !session->in_transaction && !session->has_temp_tables &&
        query_is_replica_safe_read(qt)) {
        decision->is_read = true;
        decision->reason = "consistent read rejected: no replica catch-up proof";
        decision->reason_code = KEEL_ROUTE_REASON_LAG_EXCEEDED;
        decision->decision_factors |= KEEL_DF_CONSISTENCY_TOKEN;
        return KEEL_ERR_UNAVAILABLE;
    }

    /* Feature 7: Cross-transaction shard validation.
     * If a scatter write was recorded for this transaction and we are now
     * routing a single-shard query, reject it if the target shard was not
     * among the scatter write participants. */
    if (use_shard_filter
        && shard_index < KEEL_SCATTER_MAX_SHARDS
        && session
        && session->in_transaction
        && session->has_scatter_write
        && !(session->scatter_shards_mask & (1ULL << shard_index))) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "Shard %zu is not a participant in the active scatter write "
                      "(mask=0x%llx); cross-tx rejected",
                      shard_index,
                      (unsigned long long)session->scatter_shards_mask);
        return KEEL_ERR_SHARD_CROSS_TX;
    }

    bool force_primary_for_consistency =
        consistency_forces_primary(router, qt, session);
    bool is_read = should_use_readonly(router, qt, session);

    /* WAIT-policy escape hatch: when the operator picked
     * `stale_read_policy=wait` and the session has a consistency token, do
     * NOT force primary. Instead the read path will select a candidate
     * replica and emit `KEEL_ROUTE_REASON_WAIT_CATCHUP`; the engine then
     * parks the session in the per-worker catch-up manager (see
     * keel/engine/catchup.h) until a probe proves the replica reached the
     * token. If no eligible replica exists this collapses to the legacy
     * primary fallback. */
    bool wait_catchup_mode = false;
    if (force_primary_for_consistency && should_wait_for_catchup(router, session)) {
        force_primary_for_consistency = false;
        is_read                       = true;
        wait_catchup_mode             = true;
    }

    if (force_primary_for_consistency) {
        is_read = false;
    }
    decision->is_read = is_read || force_primary_for_consistency;

    router_server_t* selected = NULL;

    if (is_read) {
        size_t read_indices[KEEL_ROUTER_MAX_SERVERS];
        size_t read_count = use_shard_filter
            ? build_read_indices_for_shard(router, shard_index, read_indices, KEEL_ROUTER_MAX_SERVERS)
            : build_read_indices(router, read_indices, KEEL_ROUTER_MAX_SERVERS);

        if (read_count > 0) {
            selected = select_server(router, read_indices, read_count, true);
        }

        if (wait_catchup_mode && selected && selected->config.role != KEEL_SERVER_PRIMARY) {
            /* Replica picked, but session must be parked until it catches
             * up to the token. The engine consumes wait_server_index +
             * wait_token + wait_max_ms and enqueues a waiter; on REACHED
             * the original SQL is re-dispatched against this same replica.
             * If `selected` is the primary (added via primary_read_weight
             * when no replica is eligible) fall through to the normal
             * read/fallback path — there is nothing to wait for. */
            size_t srv_idx = (size_t)(selected - router->servers);
            decision->server             = NULL;
            decision->wait_server_index  = srv_idx;
            decision->wait_token         = session->required_consistency_token;
            decision->wait_max_ms        = router->config.max_replica_catchup_ms;
            decision->reason             = "consistent read: parked until replica reaches token";
            decision->reason_code        = KEEL_ROUTE_REASON_WAIT_CATCHUP;
            decision->decision_factors  |= (KEEL_DF_WAIT_CATCHUP | KEEL_DF_CONSISTENCY_TOKEN);
            if (!simulate) {
                router->stats.read_routes++;
            }
            return KEEL_OK;
        }

        if (!selected && router->config.failover_to_primary) {
            size_t fallback_indices[KEEL_ROUTER_MAX_SERVERS];
            size_t fallback_count = use_shard_filter
                ? build_write_indices_for_shard(router, shard_index, fallback_indices, KEEL_ROUTER_MAX_SERVERS)
                : build_write_indices(router, fallback_indices, KEEL_ROUTER_MAX_SERVERS);
            if (fallback_count > 0) {
                selected = select_server(router, fallback_indices, fallback_count, false);
            }
            if (selected) {
                if (!simulate) router->stats.failover_routes++;
                decision->reason      = use_shard_filter ? "single-shard failover to RW" : "failover to RW";
                decision->reason_code = KEEL_ROUTE_REASON_FAILOVER_PRIMARY;
            }
        }

        if (selected) {
            if (!simulate) router->stats.read_routes++;
            if (!decision->reason) {
                decision->reason      = use_shard_filter ? "single-shard read query" : "read query";
                decision->reason_code = KEEL_ROUTE_REASON_READ_SPLIT;
                decision->decision_factors |= KEEL_DF_REPLICA_OK;
            } else {
                /* Failover-to-primary case */
                decision->decision_factors |= (KEEL_DF_NO_REPLICAS | KEEL_DF_FAILOVER_FALLBACK);
            }
        }
    } else {
        size_t write_indices[KEEL_ROUTER_MAX_SERVERS];
        size_t write_count = use_shard_filter
            ? build_write_indices_for_shard(router, shard_index, write_indices, KEEL_ROUTER_MAX_SERVERS)
            : build_write_indices(router, write_indices, KEEL_ROUTER_MAX_SERVERS);

        if (write_count > 0) {
            selected = select_server(router, write_indices, write_count, false);
        }

        if (selected) {
            if (!simulate) {
                if (force_primary_for_consistency)
                    router->stats.read_routes++;
                else
                    router->stats.write_routes++;
            }
            if (force_primary_for_consistency) {
                if (session && session->required_consistency_token.timeline_id != 0 &&
                    selected->config.timeline_id != 0 &&
                    session->required_consistency_token.timeline_id != selected->config.timeline_id) {
                    /* Failover-manager: consistency token was minted on a
                     * different PG timeline than the current primary. If
                     * the operator opted into `stale_read_policy=REJECT`,
                     * refuse the route to avoid phantom reads after a
                     * re-fork. Otherwise (legacy default) just tag the
                     * decision with TIMELINE_STALE for observability. */
                    if (router->config.stale_read_policy
                            == KEEL_STALE_READ_REJECT) {
                        decision->server      = NULL;
                        decision->reason      = "consistency token timeline is stale";
                        decision->reason_code = KEEL_ROUTE_REASON_TIMELINE_STALE;
                        decision->decision_factors |=
                            (KEEL_DF_CONSISTENCY_TOKEN | KEEL_DF_STICKY_PRIMARY);
                        return KEEL_ERR_UNAVAILABLE;
                    }
                    decision->reason      = "consistency token timeline is stale";
                    decision->reason_code = KEEL_ROUTE_REASON_TIMELINE_STALE;
                } else {
                    decision->reason      = "consistency token requires primary";
                    decision->reason_code = KEEL_ROUTE_REASON_CONSISTENCY_PRIMARY;
                }
                decision->decision_factors |=
                    (KEEL_DF_CONSISTENCY_TOKEN | KEEL_DF_STICKY_PRIMARY);
            } else if (session && session->in_transaction) {
                decision->reason      = use_shard_filter ? "single-shard transaction" : "in transaction";
                decision->reason_code = KEEL_ROUTE_REASON_IN_TRANSACTION;
                decision->decision_factors |= KEEL_DF_IN_TRANSACTION;
            } else if (session && session->has_temp_tables) {
                decision->reason      = "session has temp tables";
                decision->reason_code = KEEL_ROUTE_REASON_HARD_PINNED;
                decision->decision_factors |= KEEL_DF_HAS_TEMP_TABLE;
            } else if (qt && qt->operation == KEEL_QT_OP_TRANSACTION) {
                decision->reason      = "transaction control";
                decision->reason_code = KEEL_ROUTE_REASON_TRANSACTION_CTRL;
                decision->decision_factors |= KEEL_DF_STMT_CLASS_TXN_CTL;
            } else if (qt && qt->operation == KEEL_QT_OP_DDL) {
                decision->reason      = "DDL statement";
                decision->reason_code = KEEL_ROUTE_REASON_DDL;
                decision->decision_factors |= KEEL_DF_STMT_CLASS_DDL;
            } else if (qt && qt->func_count > 0) {
                /* Conservative-by-default function policy: any function call
                 * outside the pg_catalog allowlist forces primary. The richer
                 * `keel_metadata_analyze_query()` path (router_plugin.c)
                 * promotes this to VOLATILE / SECURITY_DEFINER / TRIGGER
                 * once the function is known. */
                decision->reason      = "unknown function call (conservative)";
                decision->reason_code = KEEL_ROUTE_REASON_UNKNOWN_FUNCTION;
                decision->decision_factors |= KEEL_DF_UNKNOWN_FUNCTION;
            } else if (qt && (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE)) {
                decision->reason      = "partial parse requires RW";
                decision->reason_code = KEEL_ROUTE_REASON_SEMANTIC_UNSAFE;
                decision->decision_factors |= KEEL_DF_PARSE_FAILED;
            } else if (qt && qt->has_error) {
                decision->reason      = "parse error requires RW";
                decision->reason_code = KEEL_ROUTE_REASON_SEMANTIC_UNSAFE;
                decision->decision_factors |= KEEL_DF_PARSE_FAILED;
            } else if (qt && qt->operation == KEEL_QT_OP_WRITE) {
                decision->reason      = use_shard_filter ? "single-shard write query" : "write query";
                decision->reason_code = KEEL_ROUTE_REASON_WRITE_REQUIRED;
                decision->decision_factors |= KEEL_DF_STMT_CLASS_WRITE;
            } else {
                decision->reason      = use_shard_filter ? "single-shard RW required" : "RW required";
                decision->reason_code = KEEL_ROUTE_REASON_WRITE_REQUIRED;
                decision->decision_factors |= KEEL_DF_STMT_CLASS_WRITE;
            }
        }
    }

    if (!selected) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "No available server for routing");
        return KEEL_ERR_UNAVAILABLE;
    }

    if (!simulate) selected->routes++;
    decision->server = &selected->config;

    /* Per-shard counter for single-shard routed queries */
    if (!simulate && use_shard_filter && shard_index < KEEL_SCATTER_MAX_SHARDS) {
        router->stats.shard_single_routes[shard_index]++;
    }

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                   "Routed to %s (%s:%d): %s",
                   selected->name, selected->host, selected->config.port,
                   decision->reason);

    return KEEL_OK;
}

/* Live-path wrapper: stats mutations enabled. */
static keel_error_t route_internal(keel_router_t* router,
                                   const keel_qt_query_t* qt,
                                   const keel_route_session_t* session,
                                   bool use_shard_filter,
                                   size_t shard_index,
                                   keel_route_decision_t* decision) {
    return route_internal_ex(router, qt, session, use_shard_filter,
                             shard_index, decision, /*simulate=*/false);
}

/**
 * @brief Route a parsed query tree to one concrete backend server.
 *
 * @param router Router handle.
 * @param qt Parsed query tree, or `NULL` if parsing failed upstream.
 * @param session Session routing context.
 * @param[out] decision Routing decision result.
 * @return `KEEL_OK` on success, or an error when no backend is available or
 *         the inputs are invalid.
 *
 * Main decision flow:
 * - honor session pinning first
 * - classify the query as replica-eligible or RW-required
 * - build the relevant candidate list
 * - run the configured balancing strategy
 * - optionally fail over read traffic to RW nodes
 */
keel_error_t keel_router_route(keel_router_t* router,
                              const keel_qt_query_t* qt,
                              const keel_route_session_t* session,
                              keel_route_decision_t* decision) {
    return route_internal(router, qt, session, false, SIZE_MAX, decision);
}

/**
 * @brief Parse raw SQL and then delegate to `keel_router_route()`.
 *
 * @param router Router handle.
 * @param sql Raw SQL text.
 * @param session Session routing context.
 * @param[out] decision Routing decision result.
 * @return Router error code.
 */
keel_error_t keel_router_route_sql(keel_router_t* router,
                                  keel_str_t sql,
                                  const keel_route_session_t* session,
                                  keel_route_decision_t* decision) {
    if (!router || !decision) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    /* Parse through the frontend-bound parser plugin contract. */
    keel_parse_result_t parse;
    keel_qt_query_t* qt = router_parse_postgresql_sql(sql, &parse);
    bool semantic_unsafe = (qt == NULL) ||
        parse.status != KEEL_PARSE_OK ||
        (qt->operation == KEEL_QT_OP_READ &&
         parse.plan.safety != KEEL_SAFETY_SAFE_REPLICA) ||
        (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE);

    const keel_qt_query_t* route_qt = semantic_unsafe &&
        qt && qt->operation == KEEL_QT_OP_READ ? NULL : qt;

    /* Route using Query Tree bridge (may be NULL for parse/semantic errors). */
    keel_error_t err = keel_router_route(router, route_qt, session, decision);
    if (err == KEEL_OK && semantic_unsafe && decision &&
        decision->reason_code == KEEL_ROUTE_REASON_WRITE_REQUIRED) {
        decision->reason = "semantic uncertainty requires RW";
        decision->reason_code = KEEL_ROUTE_REASON_SEMANTIC_UNSAFE;
    }
    keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
    return err;
}

/* ============================================================================
 * Per-query route explainer (read-only simulation)
 * ============================================================================ */

static const char* server_role_name(keel_server_role_t r) {
    switch ((int)r) {
        case KEEL_SERVER_PRIMARY:    return "rw";
        case KEEL_SERVER_REPLICA:    return "ro";
        case KEEL_SERVER_STANDBY:    return "auto";
        default:                     return "unknown";
    }
}

static const char* server_health_name(keel_server_health_t h) {
    switch ((int)h) {
        case KEEL_HEALTH_UNKNOWN:     return "unknown";
        case KEEL_HEALTH_UP:          return "healthy";
        case KEEL_HEALTH_DEGRADED:    return "degraded";
        case KEEL_HEALTH_DOWN:        return "down";
        case KEEL_HEALTH_MAINTENANCE: return "maintenance";
        default:                      return "unknown";
    }
}

/* Fill the eligible-targets table for an explanation. Considers the same
 * pool the live router would have considered (read-pool vs write-pool) and
 * marks the row that matches `decision->server` as selected. Safe to call
 * even when the decision returned an error (will produce an empty table). */
static void explain_fill_targets(const keel_router_t*       router,
                                 const keel_route_decision_t* decision,
                                 keel_route_explanation_t*  out) {
    if (!router || !out) return;
    out->target_count = 0;

    bool want_read = decision->is_read;
    const char* selected_name =
        (decision->server && decision->server->name)
            ? decision->server->name
            : NULL;

    for (size_t i = 0; i < router->server_count
         && out->target_count < KEEL_ROUTE_EXPLAIN_MAX_TARGETS; i++) {
        const router_server_t* srv = &router->servers[i];
        if (!srv->active) continue;

        bool eligible;
        if (decision->was_pinned) {
            eligible = (selected_name && srv->name &&
                        strcmp(srv->name, selected_name) == 0);
        } else if (want_read) {
            /* Read pool = replicas (healthy) + optional primary failover. */
            eligible = (srv->config.role == KEEL_SERVER_REPLICA &&
                        srv->config.health == KEEL_HEALTH_UP);
            if (!eligible && router->config.failover_to_primary &&
                srv->config.role == KEEL_SERVER_PRIMARY &&
                srv->config.health == KEEL_HEALTH_UP) {
                eligible = true;
            }
        } else {
            /* Write pool = primary, healthy. */
            eligible = (srv->config.role == KEEL_SERVER_PRIMARY &&
                        srv->config.health == KEEL_HEALTH_UP);
        }

        keel_route_target_info_t* t = &out->targets[out->target_count++];
        memset(t, 0, sizeof *t);
        if (srv->name) {
            strncpy(t->name, srv->name, sizeof t->name - 1);
        }
        if (srv->host) {
            strncpy(t->host, srv->host, sizeof t->host - 1);
        }
        t->port         = srv->config.port;
        t->role         = srv->config.role;
        t->health       = srv->config.health;
        t->weight       = srv->config.weight;
        t->was_eligible = eligible;
        t->was_selected = (selected_name && srv->name &&
                           strcmp(srv->name, selected_name) == 0);
    }
}

keel_error_t keel_router_explain_sql(keel_router_t*              router,
                                     keel_str_t                  sql,
                                     const keel_route_session_t* session,
                                     keel_route_explanation_t*   out) {
    if (!router || !out || !sql.data || sql.len == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    memset(out, 0, sizeof *out);
    out->simulated = true;

    size_t excerpt_len =
        sql.len < sizeof(out->sql_excerpt) - 1
            ? sql.len
            : sizeof(out->sql_excerpt) - 1;
    memcpy(out->sql_excerpt, sql.data, excerpt_len);
    out->sql_excerpt[excerpt_len] = '\0';

    /* Parse via the same plugin path the live routers use, then route under
     * the dispatch mutex with simulate=true so no counter is bumped. */
    keel_parse_result_t parse;
    keel_qt_query_t* qt = router_parse_postgresql_sql(sql, &parse);

    bool semantic_unsafe = (qt == NULL) ||
        parse.status != KEEL_PARSE_OK ||
        (qt->operation == KEEL_QT_OP_READ &&
         parse.plan.safety != KEEL_SAFETY_SAFE_REPLICA) ||
        (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE);
    out->parse_failed = (qt == NULL) || parse.status != KEEL_PARSE_OK;

    const keel_qt_query_t* route_qt = semantic_unsafe &&
        qt && qt->operation == KEEL_QT_OP_READ ? NULL : qt;

    pthread_mutex_lock(&router->dispatch_mutex);
    keel_error_t err = route_internal_ex(router, route_qt, session,
                                         /*use_shard_filter=*/false,
                                         SIZE_MAX, &out->decision,
                                         /*simulate=*/true);
    if (err == KEEL_OK && semantic_unsafe &&
        out->decision.reason_code == KEEL_ROUTE_REASON_WRITE_REQUIRED) {
        out->decision.reason = "semantic uncertainty requires RW";
        out->decision.reason_code = KEEL_ROUTE_REASON_SEMANTIC_UNSAFE;
        out->decision.decision_factors |= KEEL_DF_PARSE_FAILED;
    }
    explain_fill_targets(router, &out->decision, out);
    pthread_mutex_unlock(&router->dispatch_mutex);

    keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
    return err;
}

size_t keel_route_explanation_to_json(const keel_route_explanation_t* exp,
                                      uint64_t                        query_hash,
                                      char*                           out,
                                      size_t                          out_size) {
    if (!out || out_size == 0) return 0;
    if (!exp) {
        int n = snprintf(out, out_size, "{\"error\":\"missing_explanation\"}");
        return n > 0 ? (size_t)n : 0;
    }

    size_t pos = 0;
    int n;

    char sql_esc[512];
    keel_json_escape(sql_esc, sizeof sql_esc, exp->sql_excerpt);

    char decision_json[1024];
    keel_route_decision_to_json(&exp->decision, query_hash,
                                decision_json, sizeof decision_json);

    n = snprintf(out + pos, out_size - pos,
        "{\"simulated\":%s,"
        "\"sql\":\"%s\","
        "\"parse_failed\":%s,"
        "\"decision\":%s,"
        "\"eligible_targets\":[",
        exp->simulated ? "true" : "false",
        sql_esc,
        exp->parse_failed ? "true" : "false",
        decision_json);
    if (n < 0) return 0;
    pos += (size_t)n;
    if (pos >= out_size) return pos;

    for (size_t i = 0; i < exp->target_count && pos < out_size; i++) {
        const keel_route_target_info_t* t = &exp->targets[i];
        char name_esc[128], host_esc[256];
        keel_json_escape(name_esc, sizeof name_esc, t->name);
        keel_json_escape(host_esc, sizeof host_esc, t->host);
        n = snprintf(out + pos, out_size - pos,
            "%s{\"name\":\"%s\",\"host\":\"%s\",\"port\":%u,"
            "\"role\":\"%s\",\"health\":\"%s\",\"weight\":%d,"
            "\"selected\":%s,\"eligible\":%s}",
            i == 0 ? "" : ",",
            name_esc, host_esc, (unsigned)t->port,
            server_role_name(t->role),
            server_health_name(t->health),
            t->weight,
            t->was_selected ? "true" : "false",
            t->was_eligible ? "true" : "false");
        if (n < 0) break;
        pos += (size_t)n;
    }

    if (pos < out_size) {
        n = snprintf(out + pos, out_size - pos, "]}");
        if (n > 0) pos += (size_t)n;
    }
    return pos;
}

/**
 * @brief Route a SQL string to the appropriate server using a shard rule,
 *        without bound parameters.
 *
 * @param router    Router instance.
 * @param sql       SQL text to route.
 * @param session   Current session state, or `NULL`.
 * @param rule      Shard rule that identifies the sharding column and count.
 * @param[out] decision  Populated with the selected server on success.
 * @return `KEEL_OK` on success or an error code.
 *
 * Notes:
 * - Convenience wrapper around `keel_router_route_sharded_sql_bound()`
 *   with `params = NULL` (useful for non-parameterized queries).
 * - The SQL is parsed and the shard key is extracted; the function fails with
 *   `KEEL_ERR_SQL_PARSE` if the statement cannot be parsed.
 */
keel_error_t keel_router_route_sharded_sql(keel_router_t* router,
                                           keel_str_t sql,
                                           const keel_route_session_t* session,
                                           const keel_shard_rule_t* rule,
                                           keel_route_decision_t* decision) {
    return keel_router_route_sharded_sql_bound(router, sql, session, rule, NULL, decision);
}

/**
 * @brief Route a SQL string to the appropriate server using a shard rule and
 *        optional bound parameter hints.
 *
 * @param router    Router instance.
 * @param sql       SQL text to route.
 * @param session   Current session state, or `NULL`.
 * @param rule      Shard rule.
 * @param params    Bound parameters to resolve `$N`/`?` placeholders in the
 *                  shard key expression.  May be `NULL` for non-parameterized
 *                  statements.
 * @param[out] decision  Populated with the selected server on success.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` for `NULL` required
 *         arguments, `KEEL_ERR_SQL_PARSE` if the SQL cannot be parsed, or
 *         another error code from key extraction / shard mapping / routing.
 *
 * Notes:
 * - Parses the SQL once using `keel_sql_analyze_full()`.
 * - Extracts the shard key from the AST via `keel_shard_extract_key_ast()`.
 * - Maps the key to a shard index via `keel_shard_map_key_bound()`.
 * - Delegates the final routing decision to `route_internal()`.
 */
keel_error_t keel_router_route_sharded_sql_bound(keel_router_t* router,
                                                 keel_str_t sql,
                                                 const keel_route_session_t* session,
                                                 const keel_shard_rule_t* rule,
                                                 const keel_shard_bound_params_t* params,
                                                 keel_route_decision_t* decision) {
    if (!router || !rule || !decision) {
        return KEEL_ERR_INVALID_ARG;
    }

    keel_parse_result_t parse;
    keel_qt_query_t* qt = router_parse_postgresql_sql(sql, &parse);
    if (!qt || !qt->ast) {
        keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
        return KEEL_ERR_SQL_PARSE;
    }
    bool semantic_unsafe =
        parse.status != KEEL_PARSE_OK ||
        (qt->operation == KEEL_QT_OP_READ &&
         parse.plan.safety != KEEL_SAFETY_SAFE_REPLICA) ||
        (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE);

    keel_shard_key_t shard_key;
    keel_error_t err = keel_shard_extract_key_ast(qt->ast, rule, &shard_key);
    if (err != KEEL_OK) {
        keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
        return err;
    }

    size_t shard_index = SIZE_MAX;
    err = keel_shard_map_key_bound(&shard_key, params, rule->shard_count, &shard_index);
    if (err != KEEL_OK) {
        keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
        return err;
    }

    const keel_qt_query_t* route_qt = semantic_unsafe &&
        qt->operation == KEEL_QT_OP_READ ? NULL : qt;
    err = route_internal(router, route_qt, session, true, shard_index, decision);
    if (err == KEEL_OK && semantic_unsafe && decision &&
        decision->reason_code == KEEL_ROUTE_REASON_WRITE_REQUIRED) {
        decision->reason = "semantic uncertainty requires RW";
        decision->reason_code = KEEL_ROUTE_REASON_SEMANTIC_UNSAFE;
    }
    keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
    return err;
}

/**
 * @brief Determine the shard plan for a SQL string using an explicit rule and
 *        optional bound parameters, without selecting a server.
 *
 * @param router  Router instance (used for the temp arena).
 * @param sql     SQL text to analyze.
 * @param rule    Shard rule specifying the sharding column and count.
 * @param params  Optional bound parameters for parameterized statements.
 * @param[out] plan  Receives the plan kind (`SINGLE`, `SCATTER`, or
 *                   `UNSUPPORTED`) and the resolved shard index for `SINGLE`
 *                   plans.
 * @return Nothing.  On invalid input `plan->kind` is set to
 *         `KEEL_SHARD_PLAN_UNSUPPORTED`.
 *
 * Notes:
 * - Unlike `keel_router_route_sharded_sql_bound()`, this function does not
 *   touch any server state and does not produce a routing decision; it is
 *   purely informational.
 * - Useful for logging, debugging, and pre-flight planning before the
 *   actual dispatch.
 */
void keel_router_plan_sharded_sql(keel_router_t* router,
                                  keel_str_t sql,
                                  const keel_shard_rule_t* rule,
                                  const keel_shard_bound_params_t* params,
                                  keel_shard_plan_t* plan) {
    if (!plan) return;

    if (!router || !rule) {
        plan->kind        = KEEL_SHARD_PLAN_UNSUPPORTED;
        plan->shard_index = SIZE_MAX;
        return;
    }

    keel_arena_reset(router->temp_arena);
    keel_shard_plan(sql, rule, params, router->temp_arena, plan);
}

/* ============================================================================
 * Shard rule registry
 * ============================================================================ */

keel_error_t keel_router_add_shard_rule(keel_router_t* router,
                                        const char* table,
                                        const char* column,
                                        size_t shard_count) {
    if (!router || !table || !column || shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    /* Check for existing entry to overwrite */
    for (size_t i = 0; i < router->shard_rule_count; i++) {
        if (strcasecmp(router->shard_rule_tables[i], table) == 0) {
            /* Feature 10: warn when shard_count changes.
             * There is no persistent plan cache; keel_arena_reset() is called
             * at the start of every keel_router_dispatch_sql() invocation, so
             * stale plans cannot survive across dispatch calls. */
            if (router->shard_rules[i].shard_count != shard_count) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                              "Shard rule '%s': shard_count changed %zu -> %zu; "
                              "in-flight plans using the old count are now stale",
                              table,
                              router->shard_rules[i].shard_count,
                              shard_count);
            }
            /* Overwrite in place */
            keel_free(router->shard_rule_columns[i]);
            router->shard_rule_columns[i] = keel_strdup(column);
            if (!router->shard_rule_columns[i]) return KEEL_ERR_NOMEM;
            router->shard_rules[i].column      = router->shard_rule_columns[i];
            router->shard_rules[i].shard_count = shard_count;
            return KEEL_OK;
        }
    }

    if (router->shard_rule_count >= KEEL_ROUTER_MAX_SHARD_RULES) {
        return KEEL_ERR_OVERFLOW;
    }

    size_t idx = router->shard_rule_count;
    router->shard_rule_tables[idx]  = keel_strdup(table);
    router->shard_rule_columns[idx] = keel_strdup(column);
    if (!router->shard_rule_tables[idx] || !router->shard_rule_columns[idx]) {
        keel_free(router->shard_rule_tables[idx]);
        keel_free(router->shard_rule_columns[idx]);
        router->shard_rule_tables[idx]  = NULL;
        router->shard_rule_columns[idx] = NULL;
        return KEEL_ERR_NOMEM;
    }

    router->shard_rules[idx] = (keel_shard_rule_t){
        .table       = router->shard_rule_tables[idx],
        .column      = router->shard_rule_columns[idx],
        .shard_count = shard_count,
    };
    router->shard_rule_count++;
    return KEEL_OK;
}

/**
 * @brief Remove a shard rule by table name from the router's registry.
 *
 * @param router  Router instance.
 * @param table   Case-insensitive table name to look up.
 * @return `KEEL_OK` when the rule was found and removed,
 *         `KEEL_ERR_NOT_FOUND` when no matching rule exists, or
 *         `KEEL_ERR_INVALID_ARG` for `NULL` arguments.
 *
 * Notes:
 * - Uses a compact-by-swap strategy: the removed entry is replaced with the
 *   last entry in the array, so order is not preserved.
 * - After removal, pointers embedded in the remaining `keel_shard_rule_t`
 *   entries (`rule.table`, `rule.column`) are updated to reflect the compacted
 *   positions.
 */
keel_error_t keel_router_remove_shard_rule(keel_router_t* router,
                                            const char* table) {
    if (!router || !table) return KEEL_ERR_INVALID_ARG;

    for (size_t i = 0; i < router->shard_rule_count; i++) {
        if (strcasecmp(router->shard_rule_tables[i], table) == 0) {
            keel_free(router->shard_rule_tables[i]);
            keel_free(router->shard_rule_columns[i]);
            /* Compact array */
            size_t last = router->shard_rule_count - 1;
            if (i < last) {
                router->shard_rule_tables[i]  = router->shard_rule_tables[last];
                router->shard_rule_columns[i] = router->shard_rule_columns[last];
                router->shard_rules[i]        = router->shard_rules[last];
                /* Fix the .table/.column pointers after compaction */
                router->shard_rules[i].table  = router->shard_rule_tables[i];
                router->shard_rules[i].column = router->shard_rule_columns[i];
            }
            router->shard_rule_tables[last]  = NULL;
            router->shard_rule_columns[last] = NULL;
            router->shard_rule_count--;
            return KEEL_OK;
        }
    }
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Look up a shard rule by table name.
 *
 * @param router  Router instance.
 * @param table   Case-insensitive table name.
 * @return Pointer to the matching `keel_shard_rule_t` within the router's
 *         internal array, or `NULL` if no match is found or either argument
 *         is `NULL`.
 *
 * Notes:
 * - The returned pointer is valid only until the next mutating operation on
 *   the router (add/remove/hot-reload).  Callers must not store it beyond a
 *   single call frame without external synchronization.
 */
const keel_shard_rule_t* keel_router_get_shard_rule(const keel_router_t* router,
                                                     const char* table) {
    if (!router || !table) return NULL;

    for (size_t i = 0; i < router->shard_rule_count; i++) {
        if (strcasecmp(router->shard_rule_tables[i], table) == 0) {
            return &router->shard_rules[i];
        }
    }
    return NULL;
}

/**
 * @brief Return the number of shard rules currently registered in the router.
 *
 * @param router  Router instance.  Passing `NULL` returns 0.
 * @return Count of shard rules.
 */
size_t keel_router_shard_rule_count(const keel_router_t* router) {
    return router ? router->shard_rule_count : 0;
}

/**
 * @brief Return a shard rule by its position in the registry.
 *
 * @param router  Router instance.
 * @param idx     Zero-based index into the rule registry.
 * @return Pointer to the `keel_shard_rule_t` at position `idx`, or `NULL`
 *         when `router` is `NULL` or `idx >= shard_rule_count`.
 *
 * Notes:
 * - The pointer lifetime caveat from `keel_router_get_shard_rule()` applies:
 *   it is invalidated by any mutating operation on the router.
 */
const keel_shard_rule_t* keel_router_get_shard_rule_at(const keel_router_t* router,
                                                        size_t idx) {
    if (!router || idx >= router->shard_rule_count) return NULL;
    return &router->shard_rules[idx];
}

/* ============================================================================
 * Feature 6: Range-based shard rule registration
 * ============================================================================ */

keel_error_t keel_router_add_shard_rule_range(keel_router_t* router,
                                               const char*    table,
                                               const char*    column,
                                               const int64_t* thresholds,
                                               size_t         shard_count) {
    if (!router || !table || !column || !thresholds || shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }
    if (shard_count > KEEL_SHARD_RANGE_MAX_THRESHOLDS) {
        return KEEL_ERR_OVERFLOW;
    }

    /* Check for existing entry to overwrite */
    for (size_t i = 0; i < router->shard_rule_count; i++) {
        if (strcasecmp(router->shard_rule_tables[i], table) == 0) {
            /* Feature 10: warn when shard_count changes (same rationale as hash variant) */
            if (router->shard_rules[i].shard_count != shard_count) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                              "Shard rule '%s' (range): shard_count changed %zu -> %zu; "
                              "in-flight plans using the old count are now stale",
                              table,
                              router->shard_rules[i].shard_count,
                              shard_count);
            }
            keel_free(router->shard_rule_columns[i]);
            router->shard_rule_columns[i] = keel_strdup(column);
            if (!router->shard_rule_columns[i]) return KEEL_ERR_NOMEM;
            router->shard_rules[i].column          = router->shard_rule_columns[i];
            router->shard_rules[i].shard_count     = shard_count;
            router->shard_rules[i].strategy        = KEEL_SHARD_STRATEGY_RANGE;
            router->shard_rules[i].threshold_count = shard_count;
            memcpy(router->shard_rules[i].thresholds, thresholds,
                   shard_count * sizeof(int64_t));
            return KEEL_OK;
        }
    }

    if (router->shard_rule_count >= KEEL_ROUTER_MAX_SHARD_RULES) {
        return KEEL_ERR_OVERFLOW;
    }

    size_t idx = router->shard_rule_count;
    router->shard_rule_tables[idx]  = keel_strdup(table);
    router->shard_rule_columns[idx] = keel_strdup(column);
    if (!router->shard_rule_tables[idx] || !router->shard_rule_columns[idx]) {
        keel_free(router->shard_rule_tables[idx]);
        keel_free(router->shard_rule_columns[idx]);
        router->shard_rule_tables[idx]  = NULL;
        router->shard_rule_columns[idx] = NULL;
        return KEEL_ERR_NOMEM;
    }

    router->shard_rules[idx] = (keel_shard_rule_t){
        .table           = router->shard_rule_tables[idx],
        .column          = router->shard_rule_columns[idx],
        .shard_count     = shard_count,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = shard_count,
    };
    memcpy(router->shard_rules[idx].thresholds, thresholds,
           shard_count * sizeof(int64_t));
    router->shard_rule_count++;
    return KEEL_OK;
}

/* ============================================================================
 * Feature 7: Multi-shard transaction coordination
 * ============================================================================ */

void keel_router_record_scatter_write(keel_route_session_t*      session,
                                      const keel_scatter_plan_t* plan) {
    if (!session || !plan) return;
    session->has_scatter_write   = true;
    session->scatter_shards_mask |= plan->participating_shards_mask;
}

/**
 * @brief Clear the scatter-write participation record for the current transaction.
 *
 * @param session  Session state to clear.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Should be called at `COMMIT`/`ROLLBACK` boundaries to reset
 *   `has_scatter_write` and `scatter_shards_mask`.
 * - After this call, `route_internal()` will no longer reject single-shard
 *   queries on the grounds of missing scatter-write participation.
 */
void keel_router_clear_scatter_participation(keel_route_session_t* session) {
    if (!session) return;
    session->has_scatter_write   = false;
    session->scatter_shards_mask = 0;
}

/* ============================================================================
 * Feature 8: Shard migration management
 * ============================================================================ */

keel_error_t keel_router_set_shard_migration(keel_router_t* router,
                                              const char*    table,
                                              size_t         src_shard,
                                              size_t         dst_shard) {
    if (!router || !table) return KEEL_ERR_INVALID_ARG;
    if (src_shard == dst_shard) return KEEL_ERR_INVALID_ARG;

    for (size_t i = 0; i < router->shard_rule_count; i++) {
        if (strcasecmp(router->shard_rule_tables[i], table) == 0) {
            keel_shard_rule_t* rule = &router->shard_rules[i];
            if (src_shard >= rule->shard_count || dst_shard >= rule->shard_count) {
                return KEEL_ERR_INVALID_ARG;
            }
            rule->state             = KEEL_SHARD_STATE_MIGRATING;
            rule->migrate_src_shard = src_shard;
            rule->migrate_dst_shard = dst_shard;
            return KEEL_OK;
        }
    }
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Clear an active shard migration on the named table.
 *
 * @param router  Router instance.
 * @param table   Case-insensitive table name whose migration state should be
 *                cleared.
 * @return `KEEL_OK` on success, `KEEL_ERR_NOT_FOUND` if the table has no
 *         registered shard rule, or `KEEL_ERR_INVALID_ARG` for `NULL` args.
 *
 * Notes:
 * - Resets the rule's `state` to `KEEL_SHARD_STATE_NORMAL` and clears the
 *   `migrate_src_shard` / `migrate_dst_shard` fields.
 * - Once cleared, subsequent routing calls will no longer apply the
 *   dual-write or read-from-new intercept logic for this table.
 */
keel_error_t keel_router_clear_shard_migration(keel_router_t* router,
                                                const char*    table) {
    if (!router || !table) return KEEL_ERR_INVALID_ARG;

    for (size_t i = 0; i < router->shard_rule_count; i++) {
        if (strcasecmp(router->shard_rule_tables[i], table) == 0) {
            keel_shard_rule_t* rule = &router->shard_rules[i];
            rule->state             = KEEL_SHARD_STATE_NORMAL;
            rule->migrate_src_shard = 0;
            rule->migrate_dst_shard = 0;
            return KEEL_OK;
        }
    }
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Compute the shard plan for a SQL string using all registered shard rules.
 *
 * @param router   Router instance.
 * @param sql      SQL text to analyze.
 * @param params   Optional bound parameters for placeholder resolution.
 * @param[out] plan  Receives the first non-`UNSUPPORTED` plan produced by the
 *                   registered rules.  If all rules return `UNSUPPORTED`,
 *                   `plan->kind = KEEL_SHARD_PLAN_UNSUPPORTED`.
 * @return Nothing.
 *
 * Notes:
 * - Iterates registered shard rules in order and returns the first rule that
 *   yields a meaningful plan (`SINGLE` or `SCATTER`).
 * - Does not mutate any server state; purely informational.
 * - A `NULL` router, empty SQL, or zero shard rules immediately sets the plan
 *   to `KEEL_SHARD_PLAN_UNSUPPORTED`.
 */
void keel_router_plan_sql(keel_router_t* router,
                          keel_str_t sql,
                          const keel_shard_bound_params_t* params,
                          keel_shard_plan_t* plan) {
    if (!plan) return;

    plan->kind        = KEEL_SHARD_PLAN_UNSUPPORTED;
    plan->shard_index = SIZE_MAX;

    if (!router || !sql.data || sql.len == 0 || router->shard_rule_count == 0) {
        return;
    }

    keel_arena_reset(router->temp_arena);

    /* Try each registered rule; return the first non-UNSUPPORTED outcome. */
    for (size_t i = 0; i < router->shard_rule_count; i++) {
        keel_shard_plan_t candidate;
        keel_shard_plan(sql, &router->shard_rules[i], params, router->temp_arena, &candidate);

        if (candidate.kind != KEEL_SHARD_PLAN_UNSUPPORTED) {
            *plan = candidate;
            return;
        }
    }
    /* All rules returned UNSUPPORTED */
}

/* ============================================================================
 * Scatter (fan-out) routing
 * ============================================================================ */

/**
 * @brief Route one shard of a scatter operation without a query tree.
 *
 * @p is_write directly controls the read/write split decision, bypassing
 * the query-tree classification that single-query routing uses.
 */
static keel_error_t route_shard_for_scatter(keel_router_t*              router,
                                             const keel_route_session_t* session,
                                             bool                        is_write,
                                             size_t                      shard_index,
                                             keel_route_decision_t*      decision) {
    memset(decision, 0, sizeof(*decision));
    decision->shard_index = shard_index;
    router->stats.total_routes++;

    if (session && session->pinned_server) {
        decision->server      = session->pinned_server;
        decision->was_pinned  = true;
        decision->reason      = "session pinned";
        decision->reason_code = KEEL_ROUTE_REASON_PINNED_SESSION;
        router->stats.pinned_routes++;
        return KEEL_OK;
    }

    /* Session state can force the write path regardless of is_write */
    bool use_read = !is_write
                    && router->config.read_write_split
                    && !(session && (session->in_transaction || session->has_temp_tables));

    router_server_t* selected = NULL;

    if (use_read) {
        size_t read_indices[KEEL_ROUTER_MAX_SERVERS];
        size_t read_count = build_read_indices_for_shard(
            router, shard_index, read_indices, KEEL_ROUTER_MAX_SERVERS);

        if (read_count > 0) {
            selected = select_server(router, read_indices, read_count, true);
        }

        if (!selected && router->config.failover_to_primary) {
            size_t fallback[KEEL_ROUTER_MAX_SERVERS];
            size_t fcount = build_write_indices_for_shard(
                router, shard_index, fallback, KEEL_ROUTER_MAX_SERVERS);
            if (fcount > 0) {
                selected = select_server(router, fallback, fcount, false);
            }
            if (selected) {
                router->stats.failover_routes++;
                decision->reason      = "scatter read failover to RW";
                decision->reason_code = KEEL_ROUTE_REASON_FAILOVER_PRIMARY;
            }
        }

        if (selected) {
            decision->is_read    = true;
            router->stats.read_routes++;
            if (!decision->reason) {
                decision->reason      = "scatter read";
                decision->reason_code = KEEL_ROUTE_REASON_READ_SPLIT;
            }
        }
    } else {
        size_t write_indices[KEEL_ROUTER_MAX_SERVERS];
        size_t wcount = build_write_indices_for_shard(
            router, shard_index, write_indices, KEEL_ROUTER_MAX_SERVERS);

        if (wcount > 0) {
            selected = select_server(router, write_indices, wcount, false);
        }

        if (selected) {
            router->stats.write_routes++;
            decision->reason      = "scatter write";
            decision->reason_code = KEEL_ROUTE_REASON_WRITE_REQUIRED;
        }
    }

    if (!selected) {
        return KEEL_ERR_UNAVAILABLE;
    }

    selected->routes++;
    decision->server = &selected->config;
    return KEEL_OK;
}

/**
 * @brief Fan out a scatter query to all shards defined by a rule.
 *
 * @param router    Router instance.
 * @param session   Session state, or `NULL`.
 * @param rule      Shard rule specifying the number of shards.
 * @param is_write  `true` for write (fan-out to write servers), `false` for
 *                  read (fan-out to read servers with optional failover).
 * @param[out] out  Receives one routing decision per shard; `out->count`
 *                  equals `rule->shard_count`.  `out->failed` counts shards
 *                  for which no eligible server was available.
 * @return `KEEL_OK` on success (even when some shards failed; check
 *         `out->failed`), `KEEL_ERR_INVALID_ARG` for `NULL` required
 *         arguments or an empty rule, or `KEEL_ERR_OVERFLOW` when
 *         `rule->shard_count > KEEL_SCATTER_MAX_SHARDS`.
 *
 * Notes:
 * - On write scatter, `out->participating_shards_mask` is set for every shard
 *   that was successfully routed; pass to `keel_router_record_scatter_write()`
 *   to enable cross-transaction validation.
 * - Router statistics (`shard_scatter_hits`, `shard_scatter_failed`) are
 *   updated unconditionally.
 */
keel_error_t keel_router_scatter_servers(keel_router_t*              router,
                                          const keel_route_session_t* session,
                                          const keel_shard_rule_t*    rule,
                                          bool                        is_write,
                                          keel_scatter_plan_t*        out) {
    if (!router || !rule || !out) {
        return KEEL_ERR_INVALID_ARG;
    }
    if (rule->shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }
    if (rule->shard_count > KEEL_SCATTER_MAX_SHARDS) {
        return KEEL_ERR_OVERFLOW;
    }

    memset(out, 0, sizeof(*out));
    out->count = rule->shard_count;

    for (size_t i = 0; i < rule->shard_count; i++) {
        keel_error_t err = route_shard_for_scatter(
            router, session, is_write, i, &out->decisions[i]);
        if (err != KEEL_OK) {
            out->failed++;
        } else if (is_write && i < KEEL_SCATTER_MAX_SHARDS) {
            /* Feature 7: record successfully routed write shards */
            out->participating_shards_mask |= (1ULL << i);
        }
    }

    /* Aggregate scatter stats */
    router->stats.shard_scatter_hits++;
    router->stats.shard_scatter_failed += out->failed;

    return KEEL_OK;
}

/**
 * @brief Public wrapper around the router's replica-eligibility test.
 *
 * @return `true` if the query may use a replica.
 */
bool keel_router_can_use_replica(const keel_router_t* router,
                                 const keel_qt_query_t* qt,
                                 const keel_route_session_t* session) {
    if (!router) {
        return false;
    }
    
    return should_use_readonly((keel_router_t*)router, qt, session);
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Copy router statistics into caller storage.
 *
 * @param router Router handle.
 * @param[out] stats Destination stats structure.
 * @return
 */
void keel_router_get_stats(const keel_router_t* router, keel_router_stats_t* stats) {
    if (!router || !stats) {
        return;
    }
    *stats = router->stats;
}

void keel_router_record_scatter_merge_ns(keel_router_t* router, uint64_t elapsed_ns)
{
    if (!router) return;
    router->stats.scatter_merge_ops++;
    router->stats.scatter_merge_total_ns += elapsed_ns;
    if (elapsed_ns > router->stats.scatter_merge_max_ns)
        router->stats.scatter_merge_max_ns = elapsed_ns;

    /* Update histogram buckets (cumulative: each bucket counts observations ≤ upper bound) */
    static const uint64_t bounds[KEEL_SCATTER_HIST_NBUCKETS] = KEEL_SCATTER_HIST_BOUNDS_NS;
    for (int b = 0; b < KEEL_SCATTER_HIST_NBUCKETS; b++) {
        if (elapsed_ns <= bounds[b]) {
            /* Increment this and all higher buckets (cumulative) */
            for (int j = b; j <= KEEL_SCATTER_HIST_NBUCKETS; j++)
                router->stats.scatter_merge_hist[j]++;
            return;
        }
    }
    /* Exceeds all finite bounds: only +Inf bucket */
    router->stats.scatter_merge_hist[KEEL_SCATTER_HIST_NBUCKETS]++;
}

void keel_router_record_2pc_outcome(keel_router_t* router,
                                    uint64_t started,
                                    uint64_t prepared,
                                    uint64_t prepare_failed,
                                    uint64_t committed,
                                    uint64_t rolled_back)
{
    if (!router) return;
    router->stats.twopc_started       += started;
    router->stats.twopc_prepared      += prepared;
    router->stats.twopc_prepare_failed += prepare_failed;
    router->stats.twopc_committed     += committed;
    router->stats.twopc_rolled_back   += rolled_back;
}

void keel_router_track_unsupported_pattern(keel_router_t* router,
                                           keel_scatter_unsupported_kind_t kind)
{
    if (!router) return;
    if ((unsigned)kind >= (unsigned)KEEL_SCATTER_UNSUPPORTED_KIND_COUNT) return;
    router->stats.scatter_unsupported_pattern[kind]++;
}

/**
 * @brief Reset router and per-server route counters.
 *
 * @param router Router handle.
 * @return
 */
void keel_router_reset_stats(keel_router_t* router) {
    if (!router) {
        return;
    }
    memset(&router->stats, 0, sizeof(router->stats));
    
    for (size_t i = 0; i < router->server_count; i++) {
        router->servers[i].routes = 0;
        router->servers[i].errors = 0;
    }
}

/**
 * @brief Print a human-readable router state dump.
 *
 * @param router Router handle.
 * @param out Destination stream.
 * @return
 */
void keel_router_dump(const keel_router_t* router, FILE* out) {
    if (!router || !out) {
        return;
    }
    
    fprintf(out, "Router Configuration:\n");
    fprintf(out, "  Strategy: %d\n", router->config.strategy);
    fprintf(out, "  Read/Write Split: %s\n", 
            router->config.read_write_split ? "enabled" : "disabled");
    fprintf(out, "  Primary Read Weight: %.2f\n", router->config.primary_read_weight);
    
    fprintf(out, "\nServers (%zu total):\n", router->server_count);
    
    for (size_t i = 0; i < router->server_count; i++) {
        const router_server_t* srv = &router->servers[i];
        if (!srv->active) continue;
        
        const char* health_str = "unknown";
        switch (srv->config.health) {
        case KEEL_HEALTH_UP: health_str = "up"; break;
        case KEEL_HEALTH_DOWN: health_str = "down"; break;
        case KEEL_HEALTH_DEGRADED: health_str = "degraded"; break;
        case KEEL_HEALTH_MAINTENANCE: health_str = "maintenance"; break;
        default: break;
        }
        
        fprintf(out, "  %s (%s:%d):\n", srv->name, srv->host, srv->config.port);
        fprintf(out, "    Role: %s\n", 
                srv->config.role == KEEL_SERVER_ROLE_RW ? "RW" :
                srv->config.role == KEEL_SERVER_ROLE_RO ? "RO" :
                srv->config.role == KEEL_SERVER_ROLE_WO ? "WO" : "AUTO");
        fprintf(out, "    Health: %s\n", health_str);
        fprintf(out, "    Weight: %d (effective: %d)\n", 
                srv->config.weight, srv->effective_weight);
        fprintf(out, "    Routes: %lu\n", (unsigned long)srv->routes);
    }
    
    fprintf(out, "\nStatistics:\n");
    fprintf(out, "  Total Routes: %lu\n", (unsigned long)router->stats.total_routes);
    fprintf(out, "  Read Routes: %lu\n", (unsigned long)router->stats.read_routes);
    fprintf(out, "  Write Routes: %lu\n", (unsigned long)router->stats.write_routes);
    fprintf(out, "  Pinned Routes: %lu\n", (unsigned long)router->stats.pinned_routes);
    fprintf(out, "  Failover Routes: %lu\n", (unsigned long)router->stats.failover_routes);
}

/* ============================================================================
 * Feature 14: Prometheus text-format metrics
 * ============================================================================ */

size_t keel_router_write_prometheus(const keel_router_t* router,
                                    char*                buf,
                                    size_t               buf_size) {
    if (!router || !buf || buf_size == 0) return 0;

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);

    int written = 0;
    int rem     = (int)buf_size;
    char* p     = buf;

#define PROM_APPEND(fmt, ...) \
    do { \
        int _n = snprintf(p, (size_t)rem, fmt, ##__VA_ARGS__); \
        if (_n < 0 || _n >= rem) goto done; \
        p += _n; rem -= _n; written += _n; \
    } while (0)

    PROM_APPEND(
        "# HELP keel_router_total_routes Total routing decisions made\n"
        "# TYPE keel_router_total_routes counter\n"
        "keel_router_total_routes %llu\n",
        (unsigned long long)stats.total_routes);

    PROM_APPEND(
        "# HELP keel_router_read_routes Queries routed to replica servers\n"
        "# TYPE keel_router_read_routes counter\n"
        "keel_router_read_routes %llu\n",
        (unsigned long long)stats.read_routes);

    PROM_APPEND(
        "# HELP keel_router_write_routes Queries routed to primary server\n"
        "# TYPE keel_router_write_routes counter\n"
        "keel_router_write_routes %llu\n",
        (unsigned long long)stats.write_routes);

    PROM_APPEND(
        "# HELP keel_router_failover_routes Queries rerouted due to server failure\n"
        "# TYPE keel_router_failover_routes counter\n"
        "keel_router_failover_routes %llu\n",
        (unsigned long long)stats.failover_routes);

    PROM_APPEND(
        "# HELP keel_router_pinned_routes Queries routed due to session pinning\n"
        "# TYPE keel_router_pinned_routes counter\n"
        "keel_router_pinned_routes %llu\n",
        (unsigned long long)stats.pinned_routes);

    PROM_APPEND(
        "# HELP keel_router_scatter_hits Scatter fan-outs dispatched\n"
        "# TYPE keel_router_scatter_hits counter\n"
        "keel_router_scatter_hits %llu\n",
        (unsigned long long)stats.shard_scatter_hits);

    PROM_APPEND(
        "# HELP keel_router_scatter_failed Scatter shards with no available server\n"
        "# TYPE keel_router_scatter_failed counter\n"
        "keel_router_scatter_failed %llu\n",
        (unsigned long long)stats.shard_scatter_failed);

    /* Per-shard gauge — emit only non-zero shards */
    PROM_APPEND(
        "# HELP keel_router_shard_routes Single-shard routes per shard index\n"
        "# TYPE keel_router_shard_routes counter\n");

    for (int s = 0; s < KEEL_SCATTER_MAX_SHARDS; s++) {
        if (stats.shard_single_routes[s] == 0) continue;
        PROM_APPEND("keel_router_shard_routes{shard=\"%d\"} %llu\n",
                    s, (unsigned long long)stats.shard_single_routes[s]);
    }

    /* Scatter merge latency */
    PROM_APPEND(
        "# HELP keel_router_scatter_merge_ops_total Completed scatter merge operations\n"
        "# TYPE keel_router_scatter_merge_ops_total counter\n"
        "keel_router_scatter_merge_ops_total %llu\n",
        (unsigned long long)stats.scatter_merge_ops);

    PROM_APPEND(
        "# HELP keel_router_scatter_merge_ns_total Cumulative scatter merge wall-clock ns\n"
        "# TYPE keel_router_scatter_merge_ns_total counter\n"
        "keel_router_scatter_merge_ns_total %llu\n",
        (unsigned long long)stats.scatter_merge_total_ns);

    PROM_APPEND(
        "# HELP keel_router_scatter_merge_max_ns Longest single scatter merge in ns\n"
        "# TYPE keel_router_scatter_merge_max_ns gauge\n"
        "keel_router_scatter_merge_max_ns %llu\n",
        (unsigned long long)stats.scatter_merge_max_ns);

    /* Scatter merge latency histogram — exposes P50/P95/P99 in Grafana.
     * Bucket upper bounds match KEEL_SCATTER_HIST_BOUNDS_NS (seconds). */
    {
        static const double bounds_s[KEEL_SCATTER_HIST_NBUCKETS] = {
            0.001, 0.005, 0.010, 0.025, 0.050,
            0.100, 0.250, 0.500, 1.000, 2.500
        };
        PROM_APPEND(
            "# HELP keel_router_scatter_merge_duration_seconds"
            " Scatter-merge operation latency histogram (connect+query+merge)\n"
            "# TYPE keel_router_scatter_merge_duration_seconds histogram\n");
        for (int b = 0; b < KEEL_SCATTER_HIST_NBUCKETS; b++) {
            PROM_APPEND(
                "keel_router_scatter_merge_duration_seconds_bucket{le=\"%.3f\"} %llu\n",
                bounds_s[b],
                (unsigned long long)stats.scatter_merge_hist[b]);
        }
        PROM_APPEND(
            "keel_router_scatter_merge_duration_seconds_bucket{le=\"+Inf\"} %llu\n"
            "keel_router_scatter_merge_duration_seconds_sum %.9f\n"
            "keel_router_scatter_merge_duration_seconds_count %llu\n",
            (unsigned long long)stats.scatter_merge_hist[KEEL_SCATTER_HIST_NBUCKETS],
            (double)stats.scatter_merge_total_ns / 1e9,
            (unsigned long long)stats.scatter_merge_ops);
    }

    /* Two-phase commit counters */
    PROM_APPEND(
        "# HELP keel_router_2pc_started_total Scatter write transactions where 2PC was initiated\n"
        "# TYPE keel_router_2pc_started_total counter\n"
        "keel_router_2pc_started_total %llu\n",
        (unsigned long long)stats.twopc_started);

    PROM_APPEND(
        "# HELP keel_router_2pc_prepared_total Participants that acknowledged PREPARE TRANSACTION\n"
        "# TYPE keel_router_2pc_prepared_total counter\n"
        "keel_router_2pc_prepared_total %llu\n",
        (unsigned long long)stats.twopc_prepared);

    PROM_APPEND(
        "# HELP keel_router_2pc_prepare_failed_total Participants that rejected PREPARE TRANSACTION\n"
        "# TYPE keel_router_2pc_prepare_failed_total counter\n"
        "keel_router_2pc_prepare_failed_total %llu\n",
        (unsigned long long)stats.twopc_prepare_failed);

    PROM_APPEND(
        "# HELP keel_router_2pc_committed_total Participants that completed COMMIT PREPARED\n"
        "# TYPE keel_router_2pc_committed_total counter\n"
        "keel_router_2pc_committed_total %llu\n",
        (unsigned long long)stats.twopc_committed);

    PROM_APPEND(
        "# HELP keel_router_2pc_rolled_back_total Participants rolled back (normal or abort path)\n"
        "# TYPE keel_router_2pc_rolled_back_total counter\n"
        "keel_router_2pc_rolled_back_total %llu\n",
        (unsigned long long)stats.twopc_rolled_back);

    /* Per-kind counter for scatter dispatches whose result correctness depends
     * on patterns the engine does not fully merge.  Operators alert on this to
     * detect silent-wrong-result risk in production workloads. */
    {
        static const char* kind_labels[KEEL_SCATTER_UNSUPPORTED_KIND_COUNT] = {
            "percentile",
            "window_func",
            "recursive_cte",
            "union_all",
            "dml_returning",
            "ddl",
            "gate_disabled",
        };
        PROM_APPEND(
            "# HELP keel_scatter_unsupported_pattern_total Scatter dispatches whose"
            " correctness relies on patterns KEEL does not fully merge\n"
            "# TYPE keel_scatter_unsupported_pattern_total counter\n");
        for (int k = 0; k < KEEL_SCATTER_UNSUPPORTED_KIND_COUNT; k++) {
            PROM_APPEND(
                "keel_scatter_unsupported_pattern_total{kind=\"%s\"} %llu\n",
                kind_labels[k],
                (unsigned long long)stats.scatter_unsupported_pattern[k]);
        }
    }

#undef PROM_APPEND

done:
    return (size_t)written;
}

/* ============================================================================
 * Rule persistence from config
 * ============================================================================ */

/** Context passed to the per-section iterator callback. */
typedef struct {
    keel_router_t*        router;
    const keel_config_t*  config;
    size_t                loaded;
    size_t                prefix_len; /* bytes to skip to reach table name */
} load_rules_ctx_t;

/**
 * @brief Iterator callback: called once per "worker_group.GROUP.shard_rule.*" section.
 *
 * Extracts the table name from the section suffix, reads `column` and
 * `shard_count`, and registers the rule with the router.
 */
static void load_rule_section(const char* section, void* ctx_ptr) {
    load_rules_ctx_t* ctx = (load_rules_ctx_t*)ctx_ptr;

    /* The table name is everything after the per-group prefix. */
    const char* table = section + ctx->prefix_len;
    if (!table || table[0] == '\0') {
        return;
    }

    const char* column = keel_config_get_string(ctx->config, section, "column", NULL);
    int64_t shard_count_i = keel_config_get_int(ctx->config, section, "shard_count", 0);

    if (!column || column[0] == '\0' || shard_count_i <= 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "Skipping [%s]: missing or invalid column/shard_count", section);
        return;
    }

    keel_error_t err = keel_router_add_shard_rule(
        ctx->router, table, column, (size_t)shard_count_i);

    if (err == KEEL_OK) {
        ctx->loaded++;
        KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                      "Loaded shard rule: table=%s column=%s shards=%lld",
                      table, column, (long long)shard_count_i);
    } else {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "Failed to register shard rule for table %s: err=%d",
                      table, err);
    }
}

/**
 * @brief Load all `[worker_group.GROUP.shard_rule.TABLE]` INI sections into a
 *        router's registry.
 *
 * @param router      Target router.
 * @param config      Parsed INI configuration.
 * @param group_name  Worker group name whose shard rules should be loaded.
 * @return Number of shard rules successfully loaded and registered.
 *
 * Notes:
 * - Iterates sections with the `"worker_group.<group_name>.shard_rule."`
 *   prefix via `keel_config_iter_sections_prefix()`.
 * - Each section must supply `column` (string) and `shard_count` (integer > 0)
 *   keys; sections missing either key are silently skipped.
 * - Internally calls `keel_router_add_shard_rule()`, which overwrites any
 *   existing rule for the same table name.
 * - Intended for initial configuration load.  For live reload use
 *   `keel_config_reload_shard_rules()`.
 */
size_t keel_router_load_shard_rules_from_config(keel_router_t*        router,
                                                const keel_config_t*  config,
                                                const char*           group_name) {
    if (!router || !config || !group_name || group_name[0] == '\0') {
        return 0;
    }

    char prefix[256];
    snprintf(prefix, sizeof(prefix), "worker_group.%s.shard_rule.", group_name);
    load_rules_ctx_t ctx = { router, config, 0, strlen(prefix) };
    keel_config_iter_sections_prefix(config, prefix, load_rule_section, &ctx);
    return ctx.loaded;
}

/* ============================================================================
 * Feature 13: hot-reload of shard rules (keel_config_reload_shard_rules)
 * ============================================================================ */

/**
 * Context for the reload iteration: same as load_rules_ctx but also tracks
 * which table names were seen so we can remove stale rules afterwards.
 */
typedef struct {
    keel_router_t*        router;
    const keel_config_t*  config;
    keel_reload_result_t* result;
    size_t                prefix_len; /* bytes to skip to reach table name */
    /* Track names of tables seen in the new config (max 64) */
    char  seen_tables[64][128];
    size_t seen_count;
} reload_rules_ctx_t;

/**
 * @brief Hot-reload callback: process one `[worker_group.GROUP.shard_rule.TABLE]` section.
 *
 * @param section   Full INI section name (e.g. `"worker_group.myapp.shard_rule.orders"`).
 * @param ctx_ptr   Pointer to the `reload_rules_ctx_t` accumulator.
 * @return Nothing.
 *
 * Notes:
 * - Tracks every visited table name in `ctx->seen_tables` so stale rules can
 *   be identified and removed by the caller after iteration completes.
 * - Supports both `"hash"` (default) and `"range"` sharding strategies:
 *   - `"hash"`: delegates to `keel_router_add_shard_rule()`.
 *   - `"range"`: generates uniform thresholds and calls
 *     `keel_router_add_shard_rule_range()`.
 * - Updates `res->applied`, `res->errors`, `res->skipped` counters.
 */
static void reload_rule_section(const char* section, void* ctx_ptr) {
    reload_rules_ctx_t* ctx = (reload_rules_ctx_t*)ctx_ptr;
    keel_reload_result_t* res = ctx->result;

    const char* table = section + ctx->prefix_len;  /* skip per-group prefix */
    if (!table || table[0] == '\0') {
        if (res) res->skipped++;
        return;
    }

    /* Record as seen (for stale-rule removal) */
    if (ctx->seen_count < 64) {
        strncpy(ctx->seen_tables[ctx->seen_count], table,
                sizeof(ctx->seen_tables[0]) - 1);
        ctx->seen_count++;
    }

    const char* column = keel_config_get_string(ctx->config, section, "column", NULL);
    int64_t shard_count_i = keel_config_get_int(ctx->config, section, "shard_count", 0);

    if (!column || column[0] == '\0' || shard_count_i <= 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "hot-reload: skipping [%s]: missing/invalid column or shard_count",
                      section);
        if (res) res->skipped++;
        return;
    }

    const char* strategy_str = keel_config_get_string(ctx->config, section, "strategy", "hash");
    bool is_range = (strategy_str && strcmp(strategy_str, "range") == 0);

    keel_error_t err;
    if (is_range) {
        /* Build uniform thresholds: shard k holds keys in the range
         * ( (k * INT64_MAX/n), ((k+1) * INT64_MAX/n) ].
         * Last threshold is INT64_MAX so all values are covered. */
        size_t n = (size_t)shard_count_i;
        if (n > KEEL_SHARD_RANGE_MAX_THRESHOLDS) {
            n = KEEL_SHARD_RANGE_MAX_THRESHOLDS;
        }
        int64_t thresholds[KEEL_SHARD_RANGE_MAX_THRESHOLDS];
        for (size_t k = 0; k < n - 1; k++) {
            /* Avoid integer overflow: scale INT64_MAX */
            thresholds[k] = (int64_t)(((double)(k + 1) / (double)n) * (double)INT64_MAX);
        }
        thresholds[n - 1] = INT64_MAX;

        err = keel_router_add_shard_rule_range(
            ctx->router, table, column, thresholds, n);
    } else {
        err = keel_router_add_shard_rule(
            ctx->router, table, column, (size_t)shard_count_i);
    }

    if (err == KEEL_OK) {
        if (res) res->applied++;
        KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                      "hot-reload: applied shard rule table=%s column=%s shards=%lld strategy=%s",
                      table, column, (long long)shard_count_i,
                      is_range ? "range" : "hash");
    } else {
        if (res) res->errors++;
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "hot-reload: failed to apply shard rule for table=%s err=%d",
                      table, err);
    }
}

/**
 * @brief Hot-reload all shard rules for a specific worker group from a
 *        configuration snapshot.
 *
 * @param config      New parsed configuration to load rules from.
 * @param group_name  Worker group name whose rules should be reloaded.
 * @param router      Router to update in place.
 * @param result      Optional statistics accumulator (`applied`, `errors`,
 *                    `skipped` counts).  May be `NULL`.
 * @return Nothing.
 *
 * Behavior:
 * - Iterates `[worker_group.<group_name>.shard_rule.*]` sections and applies
 *   additions or updates using `reload_rule_section()`.
 * - Does **not** automatically remove rules whose tables are absent from the
 *   new configuration; callers that need stale-rule removal must compare
 *   `seen_tables` against the current registry themselves.
 *
 * Notes:
 * - Safe to call at runtime; the router does not lock during iteration, so
 *   callers should serialize reload calls with an external mutex if multiple
 *   threads may modify the router concurrently.
 */
void keel_config_reload_shard_rules(const keel_config_t*  config,
                                    const char*           group_name,
                                    keel_router_t*        router,
                                    keel_reload_result_t* result) {
    if (!config || !group_name || group_name[0] == '\0' || !router) return;

    char prefix[256];
    snprintf(prefix, sizeof(prefix), "worker_group.%s.shard_rule.", group_name);

    reload_rules_ctx_t ctx = {
        .router     = router,
        .config     = config,
        .result     = result,
        .prefix_len = strlen(prefix),
        .seen_count = 0,
    };
    keel_config_iter_sections_prefix(config, prefix,
                                     reload_rule_section, &ctx);
}

/* ============================================================================
 * Combined plan + dispatch
 * ============================================================================ */

/* ============================================================================
 * Phase C — ORDER BY / LIMIT extraction from scatter SELECT AST
 * ============================================================================ */

/**
 * @brief Attempt to resolve an ORDER BY expression to a 0-based column index.
 *
 * Supported forms:
 *   - Integer ordinal:  ORDER BY 1, 2, 3  → col_index = ordinal - 1
 *   - Bare column name: ORDER BY name     → linear scan of SELECT target list
 *
 * Returns KEEL_SORT_COL_UNRESOLVED (-1) for any expression that cannot be
 * cheaply resolved (complex expressions, subqueries, etc.).
 */
static int16_t resolve_order_col_sql(const keel_sql_node_t*  expr,
                                      const keel_sql_list_t*  targets,
                                      const char*             sql,
                                      size_t                  sql_len)
{
    if (!expr) return KEEL_SORT_COL_UNRESOLVED;

    /* Integer ordinal: ORDER BY 1 */
    if (expr->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
        const keel_sql_expr_literal_t* lit = (const keel_sql_expr_literal_t*)expr;
        if (lit->lit_type == KEEL_SQL_LIT_INT && lit->value.int_val >= 1) {
            int64_t ord = lit->value.int_val;
            if (targets && (size_t)ord <= targets->count)
                return (int16_t)(ord - 1);
        }
        return KEEL_SORT_COL_UNRESOLVED;
    }

    /* Column reference: ORDER BY col  or  ORDER BY alias */
    if (expr->kind == KEEL_SQL_NODE_EXPR_COLUMN && targets) {
        const keel_sql_expr_column_t* col = (const keel_sql_expr_column_t*)expr;
        /* Walk the SELECT target list looking for a matching alias or col name */
        const keel_sql_node_t* tgt = targets->head;
        int16_t idx = 0;
        while (tgt) {
            const keel_sql_select_target_t* st =
                (const keel_sql_select_target_t*)tgt;
            /* Check alias first */
            if (st->alias.data && st->alias.len == col->column.len &&
                strncasecmp(st->alias.data, col->column.data,
                            (size_t)col->column.len) == 0) {
                return idx;
            }
            /* Check bare column ref in the target expression */
            if (st->expr && st->expr->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
                const keel_sql_expr_column_t* tc =
                    (const keel_sql_expr_column_t*)st->expr;
                if (tc->column.len == col->column.len &&
                    strncasecmp(tc->column.data, col->column.data,
                                (size_t)col->column.len) == 0) {
                    return idx;
                }
            }
            tgt = tgt->next;
            idx++;
        }
    }

    /* Expression matching by structural comparison.
     * For GROUP BY (col op lit), find a SELECT target with same binary structure.
     * This handles cases like GROUP BY (value > 30) matching SELECT (value > 30). */
    if (targets && expr->kind == KEEL_SQL_NODE_EXPR_BINARY) {
        const keel_sql_expr_binary_t* gbin = (const keel_sql_expr_binary_t*)expr;
        /* Only handle (column op literal) patterns */
        if (gbin->left && gbin->right &&
            gbin->left->kind  == KEEL_SQL_NODE_EXPR_COLUMN &&
            gbin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
            const keel_sql_expr_column_t*  gcol =
                (const keel_sql_expr_column_t*)gbin->left;
            const keel_sql_expr_literal_t* glit =
                (const keel_sql_expr_literal_t*)gbin->right;

            const keel_sql_node_t* tgt = targets->head;
            int16_t idx = 0;
            while (tgt) {
                const keel_sql_select_target_t* st =
                    (const keel_sql_select_target_t*)tgt;
                if (st->expr && st->expr->kind == KEEL_SQL_NODE_EXPR_BINARY) {
                    const keel_sql_expr_binary_t* sbin =
                        (const keel_sql_expr_binary_t*)st->expr;
                    if (sbin->op == gbin->op &&
                        sbin->left  && sbin->left->kind  == KEEL_SQL_NODE_EXPR_COLUMN &&
                        sbin->right && sbin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                        const keel_sql_expr_column_t*  scol =
                            (const keel_sql_expr_column_t*)sbin->left;
                        const keel_sql_expr_literal_t* slit =
                            (const keel_sql_expr_literal_t*)sbin->right;
                        /* Compare column names and literal values */
                        bool cols_match = (scol->column.len == gcol->column.len &&
                            strncasecmp(scol->column.data, gcol->column.data,
                                        (size_t)gcol->column.len) == 0);
                        bool lits_match = (slit->lit_type == glit->lit_type);
                        if (lits_match) {
                            switch (glit->lit_type) {
                            case KEEL_SQL_LIT_INT:
                                lits_match = (slit->value.int_val == glit->value.int_val);
                                break;
                            case KEEL_SQL_LIT_FLOAT:
                                lits_match = (slit->value.float_val == glit->value.float_val);
                                break;
                            case KEEL_SQL_LIT_STRING:
                                lits_match = (slit->value.str_val.len == glit->value.str_val.len &&
                                    strncasecmp(slit->value.str_val.data, glit->value.str_val.data,
                                                (size_t)glit->value.str_val.len) == 0);
                                break;
                            default: lits_match = false; break;
                            }
                        }
                        if (cols_match && lits_match)
                            return idx;
                    }
                }
                tgt = tgt->next;
                idx++;
            }
        }
    }

    return KEEL_SORT_COL_UNRESOLVED;
}

static int16_t resolve_order_col(const keel_sql_node_t*  expr,
                                  const keel_sql_list_t*  targets)
{
    return resolve_order_col_sql(expr, targets, NULL, 0);
}

/**
 * @brief True if @p name (length @p l) is a Tier-2 global ranking window function.
 */
static bool wfunc_is_tier2(const char* name, size_t l)
{
    return (l == 10 && strncasecmp(name, "row_number",   10) == 0) ||
           (l ==  4 && strncasecmp(name, "rank",          4) == 0) ||
           (l == 10 && strncasecmp(name, "dense_rank",   10) == 0) ||
           (l ==  5 && strncasecmp(name, "ntile",         5) == 0) ||
           (l == 12 && strncasecmp(name, "percent_rank", 12) == 0) ||
           (l ==  9 && strncasecmp(name, "cume_dist",     9) == 0);
}

/**
 * @brief Map a function name to @c keel_window_func_t.
 *
 * Assumes the caller has already validated via wfunc_is_tier2().
 */
static keel_window_func_t wfunc_from_name(const char* name, size_t l)
{
    if (l ==  4 && strncasecmp(name, "rank",         4) == 0) return KEEL_WFUNC_RANK;
    if (l == 10 && strncasecmp(name, "dense_rank",  10) == 0) return KEEL_WFUNC_DENSE_RANK;
    if (l ==  5 && strncasecmp(name, "ntile",        5) == 0) return KEEL_WFUNC_NTILE;
    if (l == 12 && strncasecmp(name, "percent_rank",12) == 0) return KEEL_WFUNC_PERCENT_RANK;
    if (l ==  9 && strncasecmp(name, "cume_dist",    9) == 0) return KEEL_WFUNC_CUME_DIST;
    return KEEL_WFUNC_ROW_NUMBER; /* row_number or default */
}

/**
 * @brief True if @p name (length @p l) is a Tier-3 value-access window function.
 */
static bool wfunc_is_tier3(const char* name, size_t l)
{
    return (l ==  3 && strncasecmp(name, "lag",         3) == 0) ||
           (l ==  4 && strncasecmp(name, "lead",        4) == 0) ||
           (l == 11 && strncasecmp(name, "first_value", 11) == 0) ||
           (l == 10 && strncasecmp(name, "last_value",  10) == 0) ||
           (l ==  9 && strncasecmp(name, "nth_value",    9) == 0);
}

/**
 * @brief Map a Tier-3 function name to @c keel_window_func_t.
 *
 * Assumes the caller has already validated via wfunc_is_tier3().
 */
static keel_window_func_t wfunc_tier3_from_name(const char* name, size_t l)
{
    if (l ==  4 && strncasecmp(name, "lead",        4) == 0) return KEEL_WFUNC_LEAD;
    if (l == 11 && strncasecmp(name, "first_value", 11) == 0) return KEEL_WFUNC_FIRST_VALUE;
    if (l == 10 && strncasecmp(name, "last_value",  10) == 0) return KEEL_WFUNC_LAST_VALUE;
    if (l ==  9 && strncasecmp(name, "nth_value",    9) == 0) return KEEL_WFUNC_NTH_VALUE;
    return KEEL_WFUNC_LAG; /* lag (l==3) or default */
}

/**
 * @brief True if @p name is a Tier-4 aggregate window function.
 *
 * Tier 4: SUM, COUNT, MIN, MAX, AVG used with OVER (...).  Unlike standard
 * aggregates these functions are computed in-memory after scatter by
 * iterating the frame for every row.
 */
static bool wfunc_is_tier4(const char* name, size_t l)
{
    return (l == 3 && strncasecmp(name, "sum",   3) == 0) ||
           (l == 5 && strncasecmp(name, "count", 5) == 0) ||
           (l == 3 && strncasecmp(name, "min",   3) == 0) ||
           (l == 3 && strncasecmp(name, "max",   3) == 0) ||
           (l == 3 && strncasecmp(name, "avg",   3) == 0);
}

/**
 * @brief Map a Tier-4 aggregate window function name to @c keel_window_func_t.
 */
static keel_window_func_t wfunc_tier4_from_name(const char* name, size_t l)
{
    if (l == 5 && strncasecmp(name, "count", 5) == 0) return KEEL_WFUNC_AGG_COUNT;
    if (l == 3 && strncasecmp(name, "min",   3) == 0) return KEEL_WFUNC_AGG_MIN;
    if (l == 3 && strncasecmp(name, "max",   3) == 0) return KEEL_WFUNC_AGG_MAX;
    if (l == 3 && strncasecmp(name, "avg",   3) == 0) return KEEL_WFUNC_AGG_AVG;
    return KEEL_WFUNC_AGG_SUM; /* sum or default */
}

/**
 * @brief Extract PARTITION BY list as sort keys (ASC, NULLS DEFAULT).
 *
 * Returns the number of keys extracted (capped at KEEL_SCATTER_MAX_ORDER_KEYS).
 */
static uint16_t wfunc_extract_partition_keys(const keel_sql_list_t*  pb,
                                               const keel_sql_list_t*  targets,
                                               keel_sort_key_t*        out)
{
    if (!pb || pb->count == 0) return 0;
    uint16_t n = 0;
    const keel_sql_node_t* item = pb->head;
    while (item && n < KEEL_SCATTER_MAX_ORDER_KEYS) {
        const keel_sql_node_t* expr = item;
        if (item->kind == KEEL_SQL_NODE_SELECT_TARGET) {
            const keel_sql_select_target_t* st = (const keel_sql_select_target_t*)item;
            expr = st->expr;
        }
        out[n].col_index = resolve_order_col(expr, targets);
        out[n].dir       = KEEL_SORT_ASC;
        out[n].nulls     = KEEL_SORT_NULLS_DEFAULT;
        n++;
        item = item->next;
    }
    return n;
}

/**
 * @brief Convert a SQL AST frame bound enum to the scatter-store enum.
 */
static keel_frame_bound_type_t wfunc_convert_frame_bound(keel_sql_frame_bound_t b)
{
    switch (b) {
    case KEEL_SQL_FRAME_UNBOUNDED_PRECEDING: return KEEL_FRAME_UNBOUNDED_PRECEDING;
    case KEEL_SQL_FRAME_OFFSET_PRECEDING:    return KEEL_FRAME_N_PRECEDING;
    case KEEL_SQL_FRAME_CURRENT_ROW:         return KEEL_FRAME_CURRENT_ROW;
    case KEEL_SQL_FRAME_OFFSET_FOLLOWING:    return KEEL_FRAME_N_FOLLOWING;
    case KEEL_SQL_FRAME_UNBOUNDED_FOLLOWING: return KEEL_FRAME_UNBOUNDED_FOLLOWING;
    }
    return KEEL_FRAME_CURRENT_ROW;
}

/**
 * @brief Extract the integer offset N from a frame bound offset expression.
 *
 * Returns 0 if the expression is absent or not a literal integer.
 */
static int64_t wfunc_extract_frame_n(const keel_sql_node_t* expr)
{
    if (!expr || expr->kind != KEEL_SQL_NODE_EXPR_LITERAL) return 0;
    const keel_sql_expr_literal_t* lit = (const keel_sql_expr_literal_t*)expr;
    return (lit->lit_type == KEEL_SQL_LIT_INT) ? lit->value.int_val : 0;
}

/**
 * @brief True if the window spec's PARTITION BY list includes @p shard_col.
 *
 * A NULL or empty partition_by always returns false.
 */
static bool wfunc_partition_covers_shard(const keel_sql_window_spec_t* ws,
                                          const char*                   shard_col)
{
    if (!ws || !shard_col || !ws->partition_by || ws->partition_by->count == 0)
        return false;
    size_t slen = strlen(shard_col);
    const keel_sql_node_t* item = ws->partition_by->head;
    while (item) {
        /* Unwrap SELECT target wrapper if present */
        const keel_sql_node_t* expr = item;
        if (item->kind == KEEL_SQL_NODE_SELECT_TARGET) {
            const keel_sql_select_target_t* st = (const keel_sql_select_target_t*)item;
            expr = st->expr;
        }
        if (expr && expr->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
            const keel_sql_expr_column_t* col =
                (const keel_sql_expr_column_t*)expr;
            if (col->column.len == (int32_t)slen &&
                strncasecmp(col->column.data, shard_col, slen) == 0)
                return true;
        }
        item = item->next;
    }
    return false;
}

/**
 * @brief Extract ORDER BY sort keys from a window spec into @p keys_out.
 *
 * Returns the number of keys extracted (capped at KEEL_SCATTER_MAX_ORDER_KEYS).
 */
static uint16_t wfunc_extract_order_keys(const keel_sql_window_spec_t* ws,
                                          const keel_sql_list_t*        targets,
                                          keel_sort_key_t*              keys_out)
{
    if (!ws || !ws->order_by || ws->order_by->count == 0) return 0;
    uint16_t n = 0;
    const keel_sql_node_t* item = ws->order_by->head;
    while (item && n < KEEL_SCATTER_MAX_ORDER_KEYS) {
        const keel_sql_order_item_t* oi = (const keel_sql_order_item_t*)item;
        keel_sort_key_t* k = &keys_out[n];
        k->col_index = resolve_order_col(oi->expr, targets);
        k->dir   = (oi->direction == KEEL_SQL_ORDER_DESC)
                   ? KEEL_SORT_DESC : KEEL_SORT_ASC;
        switch (oi->nulls) {
        case KEEL_SQL_NULLS_FIRST: k->nulls = KEEL_SORT_NULLS_FIRST;  break;
        case KEEL_SQL_NULLS_LAST:  k->nulls = KEEL_SORT_NULLS_LAST;   break;
        default:                   k->nulls = KEEL_SORT_NULLS_DEFAULT; break;
        }
        n++;
        item = item->next;
    }
    return n;
}

/**
 * @brief Populate order_keys, norder_keys, limit_count, limit_offset,
 *        agg_specs, nagg_specs, requires_avg_rewrite, avg_finalize_specs,
 *        having_preds, group_key_cols, and requires_merge in @p out from the
 *        SQL AST.
 *
 * Only called for SCATTER dispatches on SELECT statements.
 *
 * @p shard_col is the sharding column for the matched rule (may be NULL).
 * It is used to implement Tier-1 window function support: when PARTITION BY
 * includes the shard column, each shard's window computation is locally
 * correct and no forced-single fallback is needed.
 *
 * AVG rewrite convention
 * ----------------------
 * For each AVG(x) at SELECT position @c col_idx, the shard query must be
 * rewritten to @c SUM(x), COUNT(x).  This function records:
 *   - agg_spec for col_idx with func=SUM
 *   - agg_spec for (total_targets + k) with func=COUNT  (appended COUNT col)
 *   - avg_finalize_spec: {sum_col=col_idx, count_col=total_targets+k, out_col=col_idx}
 * where @c k is the zero-based index of this AVG among all AVGs in the query.
 */

/**
 * @brief Build SQL text for a simple aggregate function call (e.g. "SUM(value)",
 * "COUNT(*)") from the AST function node.  Used when HAVING references an
 * aggregate that is not present in the SELECT target list.
 */
static void having_agg_sql_text(const keel_sql_expr_func_t* fn,
                                 char* out, size_t outsz) {
    size_t n = (size_t)snprintf(out, outsz, "%.*s(",
                                (int)fn->name.len, fn->name.data);
    if (n >= outsz) { if (outsz > 0) out[0] = '\0'; return; }
    if (fn->args && fn->args->head) {
        const keel_sql_node_t* arg = fn->args->head;
        if (arg->kind == KEEL_SQL_NODE_EXPR_STAR) {
            snprintf(out + n, outsz - n, "*)");
        } else if (arg->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
            const keel_sql_expr_column_t* col = (const keel_sql_expr_column_t*)arg;
            snprintf(out + n, outsz - n, "%.*s)", (int)col->column.len, col->column.data);
        } else {
            snprintf(out + n, outsz - n, "*)");
        }
    } else {
        snprintf(out + n, outsz - n, "*)");
    }
}

static void scatter_extract_merge_spec_impl(const keel_sql_node_t*  ast,
                                             const char*             shard_col,
                                             keel_dispatch_result_t* out,
                                             const keel_sql_list_t*  ctx_with);

static void scatter_extract_merge_spec(const keel_sql_node_t*  ast,
                                        const char*             shard_col,
                                        keel_dispatch_result_t* out)
{
    scatter_extract_merge_spec_impl(ast, shard_col, out, NULL);
}

static void scatter_extract_merge_spec_impl(const keel_sql_node_t*  ast,
                                             const char*             shard_col,
                                             keel_dispatch_result_t* out,
                                             const keel_sql_list_t*  ctx_with)
{
    if (!ast || ast->kind != KEEL_SQL_NODE_STMT_SELECT) return;

    const keel_sql_stmt_select_t* sel = (const keel_sql_stmt_select_t*)ast;

    /* ------------------------------------------------------------------ */
    /* Count total SELECT targets (needed for AVG→SUM+COUNT column offset) */
    /* ------------------------------------------------------------------ */
    int16_t total_targets = 0;
    if (sel->targets) {
        const keel_sql_node_t* tn = sel->targets->head;
        while (tn) { total_targets++; tn = tn->next; }
    }

    /* ------------------------------------------------------------------ */
    /* Aggregate functions in SELECT target list                           */
    /* ------------------------------------------------------------------ */
    if (sel->targets && sel->targets->count > 0) {
        const keel_sql_node_t* tnode = sel->targets->head;
        int16_t col_idx = 0;
        while (tnode) {
            const keel_sql_select_target_t* st =
                (const keel_sql_select_target_t*)tnode;
            const keel_sql_node_t* expr = st->expr;

            /* Phase F: window function handling
             *
             * Tier 1: PARTITION BY covers shard key → each shard's window
             *   computation is locally correct; allow scatter natively.
             *   No merge spec needed; PostgreSQL handles it per-shard.
             *
             * Tier 2: No PARTITION BY + supported global-ranking function →
             *   scatter, collect all rows, then Phase F rewrites the column
             *   with globally correct values.
             *
             * Unsupported (PARTITION BY ≠ shard key, LAG/LEAD/…) → demote
             *   to single-shard via has_window_funcs=true. */
            if (expr && expr->kind == KEEL_SQL_NODE_EXPR_WINDOW) {
                const keel_sql_expr_func_t* wfn =
                    (const keel_sql_expr_func_t*)expr;
                const keel_sql_window_spec_t* ws = NULL;
                if (wfn->over && wfn->over->kind == KEEL_SQL_NODE_WINDOW_SPEC)
                    ws = (const keel_sql_window_spec_t*)wfn->over;

                const char* fname = wfn->name.data;
                size_t      flen  = wfn->name.len;
                bool has_partition = ws && ws->partition_by && ws->partition_by->count > 0;

                if (has_partition && wfunc_partition_covers_shard(ws, shard_col)) {
                    /* Tier 1: partition key ⊇ shard key — per-shard correct */
                    /* No merge spec needed; scatter proceeds normally */
                } else if (!has_partition && wfunc_is_tier2(fname, flen) &&
                           out->nwindow_col_specs < KEEL_SCATTER_MAX_WINDOW_COLS) {
                    /* Tier 2: global ranking — collect, sort, rewrite column */
                    keel_window_col_spec_t* wspec =
                        &out->window_col_specs[out->nwindow_col_specs];
                    wspec->col_index  = col_idx;
                    wspec->func       = wfunc_from_name(fname, flen);
                    wspec->ntile_n    = 0;
                    /* Extract NTILE(n) argument */
                    if (wspec->func == KEEL_WFUNC_NTILE &&
                        wfn->args && wfn->args->head &&
                        wfn->args->head->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                        const keel_sql_expr_literal_t* lit =
                            (const keel_sql_expr_literal_t*)wfn->args->head;
                        if (lit->lit_type == KEEL_SQL_LIT_INT)
                            wspec->ntile_n = lit->value.int_val;
                    }
                    if (wspec->ntile_n <= 0) wspec->ntile_n = 1;
                    /* Extract window ORDER BY keys */
                    wspec->norder_keys = wfunc_extract_order_keys(ws, sel->targets,
                                                                   wspec->order_keys);
                    out->nwindow_col_specs++;
                    out->requires_merge = true;
                } else if (wfunc_is_tier3(fname, flen) &&
                           out->nwindow_col_specs < KEEL_SCATTER_MAX_WINDOW_COLS) {
                    /* Tier 3: value-access — scatter, collect all rows, Phase F
                     * sorts by (partition, order) and rewrites the column value. */
                    keel_window_col_spec_t* wspec =
                        &out->window_col_specs[out->nwindow_col_specs];
                    memset(wspec, 0, sizeof *wspec);
                    wspec->col_index       = col_idx;
                    wspec->func            = wfunc_tier3_from_name(fname, flen);
                    wspec->source_col      = -1;      /* resolved below */
                    wspec->val_offset      = 1;       /* default offset  */
                    wspec->default_val_len = -1;      /* SQL NULL default */
                    /* Default frame: ROWS UNBOUNDED PRECEDING → CURRENT ROW */
                    wspec->frame_mode          = KEEL_FRAME_ROWS;
                    wspec->frame_start.type    = KEEL_FRAME_UNBOUNDED_PRECEDING;
                    wspec->frame_start.n       = 0;
                    wspec->frame_end.type      = KEEL_FRAME_CURRENT_ROW;
                    wspec->frame_end.n         = 0;

                    /* 1st arg: source column */
                    if (wfn->args && wfn->args->head)
                        wspec->source_col = resolve_order_col(wfn->args->head,
                                                               sel->targets);

                    /* LAG / LEAD: optional 2nd arg = offset, 3rd arg = default */
                    if ((wspec->func == KEEL_WFUNC_LAG ||
                         wspec->func == KEEL_WFUNC_LEAD) &&
                        wfn->args && wfn->args->count >= 2) {
                        const keel_sql_node_t* a2 = wfn->args->head
                                                   ? wfn->args->head->next : NULL;
                        if (a2 && a2->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                            const keel_sql_expr_literal_t* l2 =
                                (const keel_sql_expr_literal_t*)a2;
                            if (l2->lit_type == KEEL_SQL_LIT_INT)
                                wspec->val_offset = l2->value.int_val;
                        }
                        if (wfn->args->count >= 3 && a2) {
                            const keel_sql_node_t* a3 = a2->next;
                            if (a3 && a3->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                                const keel_sql_expr_literal_t* l3 =
                                    (const keel_sql_expr_literal_t*)a3;
                                if (l3->lit_type == KEEL_SQL_LIT_INT) {
                                    wspec->default_val_len =
                                        snprintf(wspec->default_val, 64,
                                                 "%" PRId64, l3->value.int_val);
                                } else if (l3->lit_type == KEEL_SQL_LIT_STRING &&
                                           l3->value.str_val.len > 0) {
                                    size_t dlen = (size_t)l3->value.str_val.len;
                                    if (dlen >= sizeof wspec->default_val)
                                        dlen = sizeof(wspec->default_val) - 1;
                                    memcpy(wspec->default_val,
                                           l3->value.str_val.data, dlen);
                                    wspec->default_val_len = (int32_t)dlen;
                                }
                            }
                        }
                    }

                    /* NTH_VALUE: 2nd arg = n (1-based position within frame) */
                    if (wspec->func == KEEL_WFUNC_NTH_VALUE &&
                        wfn->args && wfn->args->count >= 2) {
                        const keel_sql_node_t* a2 = wfn->args->head
                                                   ? wfn->args->head->next : NULL;
                        if (a2 && a2->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                            const keel_sql_expr_literal_t* l2 =
                                (const keel_sql_expr_literal_t*)a2;
                            if (l2->lit_type == KEEL_SQL_LIT_INT)
                                wspec->val_offset = l2->value.int_val;
                        }
                    }

                    /* PARTITION BY keys */
                    if (ws && ws->partition_by)
                        wspec->npartition_keys =
                            wfunc_extract_partition_keys(ws->partition_by,
                                                          sel->targets,
                                                          wspec->partition_keys);

                    /* ORDER BY keys */
                    wspec->norder_keys = wfunc_extract_order_keys(ws, sel->targets,
                                                                    wspec->order_keys);

                    /* Frame spec (FIRST_VALUE, LAST_VALUE, NTH_VALUE) */
                    if (ws && ws->frame_spec &&
                        ws->frame_spec->kind == KEEL_SQL_NODE_FRAME_SPEC) {
                        const keel_sql_frame_spec_t* fs =
                            (const keel_sql_frame_spec_t*)ws->frame_spec;
                        wspec->frame_mode       = (keel_frame_mode_t)fs->mode;
                        wspec->frame_start.type =
                            wfunc_convert_frame_bound(fs->start_bound);
                        wspec->frame_start.n    =
                            wfunc_extract_frame_n(fs->start_offset);
                        wspec->frame_end.type   =
                            wfunc_convert_frame_bound(fs->end_bound);
                        wspec->frame_end.n      =
                            wfunc_extract_frame_n(fs->end_offset);
                    }

                    out->nwindow_col_specs++;
                    out->requires_merge = true;
                } else if (wfunc_is_tier4(fname, flen) &&
                           out->nwindow_col_specs < KEEL_SCATTER_MAX_WINDOW_COLS) {
                    /* Tier 4: aggregate window function — scatter all rows,
                     * compute the aggregate within each partition+frame
                     * in-memory after collecting all shard results. */
                    keel_window_col_spec_t* wspec =
                        &out->window_col_specs[out->nwindow_col_specs];
                    memset(wspec, 0, sizeof *wspec);
                    wspec->col_index  = col_idx;
                    wspec->func       = wfunc_tier4_from_name(fname, flen);
                    wspec->source_col = -1; /* resolved below */

                    /* Source column: first argument (or -1 for COUNT(*)) */
                    if (wspec->func != KEEL_WFUNC_AGG_COUNT &&
                        wfn->args && wfn->args->head &&
                        wfn->args->head->kind != KEEL_SQL_NODE_EXPR_STAR) {
                        wspec->source_col = resolve_order_col(wfn->args->head,
                                                               sel->targets);
                    }

                    /* Partition keys */
                    if (ws && ws->partition_by)
                        wspec->npartition_keys =
                            wfunc_extract_partition_keys(ws->partition_by,
                                                          sel->targets,
                                                          wspec->partition_keys);

                    /* ORDER BY keys */
                    wspec->norder_keys = wfunc_extract_order_keys(ws, sel->targets,
                                                                    wspec->order_keys);

                    /* Frame spec */
                    wspec->frame_mode       = KEEL_FRAME_ROWS;
                    wspec->frame_start.type = KEEL_FRAME_UNBOUNDED_PRECEDING;
                    wspec->frame_start.n    = 0;
                    wspec->frame_end.type   = KEEL_FRAME_UNBOUNDED_FOLLOWING;
                    wspec->frame_end.n      = 0;
                    if (ws && ws->frame_spec &&
                        ws->frame_spec->kind == KEEL_SQL_NODE_FRAME_SPEC) {
                        const keel_sql_frame_spec_t* fs =
                            (const keel_sql_frame_spec_t*)ws->frame_spec;
                        wspec->frame_mode       = (keel_frame_mode_t)fs->mode;
                        wspec->frame_start.type =
                            wfunc_convert_frame_bound(fs->start_bound);
                        wspec->frame_start.n    =
                            wfunc_extract_frame_n(fs->start_offset);
                        wspec->frame_end.type   =
                            wfunc_convert_frame_bound(fs->end_bound);
                        wspec->frame_end.n      =
                            wfunc_extract_frame_n(fs->end_offset);
                    }

                    out->nwindow_col_specs++;
                    out->requires_merge = true;
                } else {
                    /* Unsupported: force single-shard fallback */
                    out->has_window_funcs = true;
                }
            }

            if (expr && (expr->kind == KEEL_SQL_NODE_EXPR_FUNC ||
                         expr->kind == KEEL_SQL_NODE_EXPR_AGGR)) {
                const keel_sql_expr_func_t* fn =
                    (const keel_sql_expr_func_t*)expr;
                const char* n = fn->name.data;
                size_t      l = fn->name.len;

                /* ---- Ordered aggregate functions (require scatter rewrite) ---- */
                keel_ord_agg_func_t oaf = KEEL_ORD_AGG_NONE;
                if      (l == 10 && strncasecmp(n, "string_agg", 10) == 0) oaf = KEEL_ORD_AGG_STRING_AGG;
                else if (l ==  9 && strncasecmp(n, "array_agg",   9) == 0) oaf = KEEL_ORD_AGG_ARRAY_AGG;
                else if (l ==  9 && strncasecmp(n, "jsonb_agg",   9) == 0) oaf = KEEL_ORD_AGG_JSONB_AGG;
                else if (l == 15 && strncasecmp(n, "percentile_cont", 15) == 0) oaf = KEEL_ORD_AGG_PERCENTILE_CONT;
                else if (l == 15 && strncasecmp(n, "percentile_disc", 15) == 0) oaf = KEEL_ORD_AGG_PERCENTILE_DISC;
                else if (l == 15 && strncasecmp(n, "json_object_agg", 15) == 0) oaf = KEEL_ORD_AGG_JSON_OBJECT_AGG;

                if (oaf != KEEL_ORD_AGG_NONE &&
                    out->nord_agg_specs < KEEL_SCATTER_MAX_ORD_AGGS) {
                    keel_ord_agg_spec_t* osp = &out->ord_agg_specs[out->nord_agg_specs];
                    memset(osp, 0, sizeof *osp);
                    osp->func    = oaf;
                    osp->key_dir = KEEL_SORT_ASC;

                    /* json_object_agg is not rewritten — each shard computes
                     * its own JSON object natively and the proxy concatenates
                     * the per-shard objects.  Skip replacement-SQL synthesis. */
                    if (oaf == KEEL_ORD_AGG_JSON_OBJECT_AGG) {
                        osp->has_key = false;
                        osp->replacement_sql[0] = '\0';
                        out->nord_agg_specs++;
                        out->requires_merge = true;
                        tnode = tnode->next;
                        col_idx++;
                        continue;
                    }

                    /* Extract separator for STRING_AGG */
                    if (oaf == KEEL_ORD_AGG_STRING_AGG && fn->args &&
                        fn->args->count >= 2 && fn->args->head &&
                        fn->args->head->next &&
                        fn->args->head->next->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                        const keel_sql_expr_literal_t* sep =
                            (const keel_sql_expr_literal_t*)fn->args->head->next;
                        if (sep->lit_type == KEEL_SQL_LIT_STRING) {
                            size_t sl = sep->value.str_val.len;
                            if (sl >= sizeof(osp->separator)) sl = sizeof(osp->separator) - 1;
                            memcpy(osp->separator, sep->value.str_val.data, sl);
                            osp->separator[sl] = '\0';
                        }
                    }

                    /* Extract percentile fraction */
                    if ((oaf == KEEL_ORD_AGG_PERCENTILE_CONT ||
                         oaf == KEEL_ORD_AGG_PERCENTILE_DISC) &&
                        fn->args && fn->args->head &&
                        fn->args->head->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                        const keel_sql_expr_literal_t* lit =
                            (const keel_sql_expr_literal_t*)fn->args->head;
                        if (lit->lit_type == KEEL_SQL_LIT_FLOAT)
                            osp->percentile = lit->value.float_val;
                        else if (lit->lit_type == KEEL_SQL_LIT_INT)
                            osp->percentile = (double)lit->value.int_val;
                    }

                    /* Extract ORDER BY key from fn->order_by */
                    if (fn->order_by) {
                        const keel_sql_order_item_t* oi =
                            (const keel_sql_order_item_t*)fn->order_by;
                        osp->key_dir = (oi->direction == KEEL_SQL_ORDER_DESC)
                                       ? KEEL_SORT_DESC : KEEL_SORT_ASC;
                        /* Build replacement SQL:
                         * For non-PERCENTILE: "expr_col, key_col"
                         * For PERCENTILE:     "key_col" (the ORDER BY column IS the value) */
                        char expr_part[128] = {0};
                        char key_part[128]  = {0};
                        if (fn->args && fn->args->head) {
                            const keel_sql_node_t* a0 = fn->args->head;
                            if (a0->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
                                const keel_sql_expr_column_t* ec =
                                    (const keel_sql_expr_column_t*)a0;
                                size_t el = ec->column.len < 127 ? ec->column.len : 127;
                                memcpy(expr_part, ec->column.data, el);
                                expr_part[el] = '\0';
                            } else {
                                /* Non-simple expr: use positional column ref */
                                snprintf(expr_part, sizeof(expr_part), "*");
                            }
                        }
                        if (oi->expr &&
                            oi->expr->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
                            const keel_sql_expr_column_t* kc =
                                (const keel_sql_expr_column_t*)oi->expr;
                            size_t kl = kc->column.len < 127 ? kc->column.len : 127;
                            memcpy(key_part, kc->column.data, kl);
                            key_part[kl] = '\0';
                        }

                        if (oaf == KEEL_ORD_AGG_PERCENTILE_CONT ||
                            oaf == KEEL_ORD_AGG_PERCENTILE_DISC) {
                            /* Replacement: just the ORDER BY key column */
                            snprintf(osp->replacement_sql, sizeof(osp->replacement_sql),
                                     "%s", key_part[0] ? key_part : "value");
                            osp->has_key = false;
                        } else {
                            /* Replacement: expr_col, key_col.
                             * Cap each %s to keep the combined output within
                             * sizeof(osp->replacement_sql) (256) and silence
                             * -Wformat-truncation; expr_part/key_part are
                             * already ≤ 127 bytes so this only truncates the
                             * theoretical worst case. */
                            if (key_part[0]) {
                                snprintf(osp->replacement_sql, sizeof(osp->replacement_sql),
                                         "%.124s, %.124s", expr_part, key_part);
                                osp->has_key = true;
                            } else {
                                snprintf(osp->replacement_sql, sizeof(osp->replacement_sql),
                                         "%s", expr_part);
                                osp->has_key = false;
                            }
                        }
                    } else if (fn->args && fn->args->head) {
                        /* No ORDER BY: just use expr */
                        const keel_sql_node_t* a0 = fn->args->head;
                        if (a0->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
                            const keel_sql_expr_column_t* ec =
                                (const keel_sql_expr_column_t*)a0;
                            size_t el = ec->column.len < sizeof(osp->replacement_sql)-1
                                        ? ec->column.len : sizeof(osp->replacement_sql)-1;
                            memcpy(osp->replacement_sql, ec->column.data, el);
                            osp->replacement_sql[el] = '\0';
                        }
                        osp->has_key = false;
                    }

                    /* Store agg function source position (name points into SQL) */
                    osp->agg_sql_start = (uint32_t)(n - /* base SQL */ n + fn->base.loc.offset);
                    osp->agg_sql_len   = (uint32_t)fn->base.loc.length;

                    out->nord_agg_specs++;
                    out->requires_merge = true;
                    tnode = tnode->next;
                    col_idx++;
                    continue; /* Don't add standard agg_spec for these */
                }

                keel_agg_func_t af = KEEL_AGG_NONE;
                if      (l == 5 && strncasecmp(n, "count", 5) == 0) af = KEEL_AGG_COUNT;
                else if (l == 3 && strncasecmp(n, "sum",   3) == 0) af = KEEL_AGG_SUM;

                /* Detect COUNT(DISTINCT col) — needs special dedup handling */
                if (af == KEEL_AGG_COUNT && fn->distinct && fn->args &&
                    fn->args->count == 1 && fn->args->head &&
                    fn->args->head->kind == KEEL_SQL_NODE_EXPR_COLUMN &&
                    !out->requires_count_distinct) {
                    const keel_sql_expr_column_t* dcol =
                        (const keel_sql_expr_column_t*)fn->args->head;
                    size_t clen = dcol->column.len;
                    if (clen >= sizeof(out->count_distinct_col))
                        clen = sizeof(out->count_distinct_col) - 1;
                    memcpy(out->count_distinct_col, dcol->column.data, clen);
                    out->count_distinct_col[clen] = '\0';
                    out->requires_count_distinct = true;
                    out->requires_merge = true;
                    tnode = tnode->next;
                    col_idx++;
                    continue; /* Don't add agg_spec; handle specially */
                }
                else if (l == 3 && strncasecmp(n, "min",   3) == 0) af = KEEL_AGG_MIN;
                else if (l == 3 && strncasecmp(n, "max",   3) == 0) af = KEEL_AGG_MAX;
                else if (l == 3 && strncasecmp(n, "avg",   3) == 0) {
                    /* AVG rewrite: emit SUM at col_idx, COUNT at an appended
                     * column, and record an avg_finalize_spec. */
                    out->requires_avg_rewrite = true;
                    if (out->nagg_specs < KEEL_SCATTER_MAX_AGG_COLS) {
                        /* SUM at the original column position */
                        out->agg_specs[out->nagg_specs].col_index = col_idx;
                        out->agg_specs[out->nagg_specs].func      = KEEL_AGG_SUM;
                        out->nagg_specs++;
                    }
                    /* COUNT at total_targets + navg_finalize_specs (appended) */
                    int16_t count_col =
                        (int16_t)(total_targets + (int16_t)out->navg_finalize_specs);
                    if (out->nagg_specs < KEEL_SCATTER_MAX_AGG_COLS) {
                        out->agg_specs[out->nagg_specs].col_index = count_col;
                        out->agg_specs[out->nagg_specs].func      = KEEL_AGG_COUNT;
                        out->nagg_specs++;
                    }
                    /* Record finalize spec */
                    if (out->navg_finalize_specs < KEEL_SCATTER_MAX_AVG_SPECS) {
                        out->avg_finalize_specs[out->navg_finalize_specs].sum_col   = col_idx;
                        out->avg_finalize_specs[out->navg_finalize_specs].count_col = count_col;
                        out->avg_finalize_specs[out->navg_finalize_specs].out_col   = col_idx;
                        out->navg_finalize_specs++;
                    }
                    out->requires_merge = true;
                    tnode = tnode->next;
                    col_idx++;
                    continue; /* skip the normal agg_spec append below */
                }
                if (af != KEEL_AGG_NONE &&
                    out->nagg_specs < KEEL_SCATTER_MAX_AGG_COLS) {
                    out->agg_specs[out->nagg_specs].col_index = col_idx;
                    out->agg_specs[out->nagg_specs].func      = af;
                    out->nagg_specs++;
                    out->requires_merge = true;
                }
            }
            tnode = tnode->next;
            col_idx++;
        }
    }

    /* ------------------------------------------------------------------ */
    /* ORDER BY                                                            */
    /* For UNION ALL / INTERSECT / EXCEPT the parser attaches the         */
    /* trailing ORDER BY clause to the right-hand SELECT.  If the left    */
    /* (outer) SELECT has no ORDER BY, check the right branch too.        */
    /* ------------------------------------------------------------------ */
    const keel_sql_list_t* effective_order_by = sel->order_by;
    const keel_sql_list_t* effective_targets  = sel->targets;
    if (!effective_order_by && sel->set_op != KEEL_SQL_SET_NONE && sel->set_right &&
        sel->set_right->kind == KEEL_SQL_NODE_STMT_SELECT) {
        const keel_sql_stmt_select_t* rhs =
            (const keel_sql_stmt_select_t*)sel->set_right;
        if (rhs->order_by && rhs->order_by->count > 0) {
            effective_order_by = rhs->order_by;
            /* Use left targets for column resolution (same column positions). */
        }
    }
    if (effective_order_by && effective_order_by->count > 0) {
        const keel_sql_node_t* item = effective_order_by->head;
        while (item && out->norder_keys < KEEL_SCATTER_MAX_ORDER_KEYS) {
            const keel_sql_order_item_t* oi = (const keel_sql_order_item_t*)item;
            keel_sort_key_t* k = &out->order_keys[out->norder_keys];

            k->col_index = resolve_order_col(oi->expr, effective_targets);
            k->dir   = (oi->direction == KEEL_SQL_ORDER_DESC)
                       ? KEEL_SORT_DESC : KEEL_SORT_ASC;
            switch (oi->nulls) {
            case KEEL_SQL_NULLS_FIRST:   k->nulls = KEEL_SORT_NULLS_FIRST;   break;
            case KEEL_SQL_NULLS_LAST:    k->nulls = KEEL_SORT_NULLS_LAST;    break;
            default:                     k->nulls = KEEL_SORT_NULLS_DEFAULT;  break;
            }

            out->norder_keys++;
            item = item->next;
        }
        out->requires_merge = true;
    }

    /* ------------------------------------------------------------------ */
    /* LIMIT / OFFSET                                                      */
    /* ------------------------------------------------------------------ */
    if (sel->limit && sel->limit->kind == KEEL_SQL_NODE_CLAUSE_LIMIT) {
        const keel_sql_limit_t* lim = (const keel_sql_limit_t*)sel->limit;

        if (lim->count && lim->count->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
            const keel_sql_expr_literal_t* lit =
                (const keel_sql_expr_literal_t*)lim->count;
            if (lit->lit_type == KEEL_SQL_LIT_INT && lit->value.int_val > 0)
                out->limit_count = (uint64_t)lit->value.int_val;
        }

        if (lim->offset && lim->offset->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
            const keel_sql_expr_literal_t* lit =
                (const keel_sql_expr_literal_t*)lim->offset;
            if (lit->lit_type == KEEL_SQL_LIT_INT && lit->value.int_val > 0)
                out->limit_offset = (uint64_t)lit->value.int_val;
        }

        if (out->limit_count > 0 || out->limit_offset > 0)
            out->requires_merge = true;
    }

    /* ------------------------------------------------------------------ */
    /* GROUP BY                                                            */
    /* ------------------------------------------------------------------ */
    if (sel->group_by && sel->group_by->count > 0) {
        const keel_sql_node_t* item = sel->group_by->head;
        while (item && out->ngroup_key_cols < KEEL_SCATTER_MAX_GROUP_COLS) {
            int16_t ci = resolve_order_col_sql(item, sel->targets, NULL, 0);
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "scatter: GROUP BY item kind=%d ci=%d", (int)item->kind, (int)ci);
            if (ci != KEEL_SORT_COL_UNRESOLVED) {
                out->group_key_cols[out->ngroup_key_cols].col_index = ci;
                out->ngroup_key_cols++;
                out->requires_merge = true;
            }
            item = item->next;
        }
    }

    /* ------------------------------------------------------------------ */
    /* SELECT DISTINCT — cross-shard deduplication                         */
    /*                                                                     */
    /* `SELECT DISTINCT col_list FROM t` runs unchanged on each shard, so  */
    /* each shard returns its own distinct rows.  Values that appear on    */
    /* more than one shard would be reported multiple times.  Treat the    */
    /* whole target list as an implicit GROUP BY so the scatter-merge      */
    /* deduplicates across shards (Phase E group_aggs path).               */
    /*                                                                     */
    /* Skipped when:                                                       */
    /*   - an explicit GROUP BY is already present (ngroup_key_cols > 0)   */
    /*   - the query already has scalar aggregates (nagg_specs > 0)        */
    /*     — DISTINCT over aggregate output is rare and would conflict     */
    /*       with the aggregate merge path                                 */
    /*   - DISTINCT ON (specific columns) — not handled here               */
    /* ------------------------------------------------------------------ */
    if (sel->distinct && !sel->distinct_on &&
        out->ngroup_key_cols == 0 && out->nagg_specs == 0 &&
        sel->targets && sel->targets->count > 0) {
        const keel_sql_node_t* tn = sel->targets->head;
        int16_t ci = 0;
        while (tn && out->ngroup_key_cols < KEEL_SCATTER_MAX_GROUP_COLS) {
            out->group_key_cols[out->ngroup_key_cols].col_index = ci;
            out->ngroup_key_cols++;
            ci++;
            tn = tn->next;
        }
        if (out->ngroup_key_cols > 0)
            out->requires_merge = true;
    }

    /* ------------------------------------------------------------------ */
    /* HAVING clause — extract simple binary predicates                    */
    /*                                                                     */
    /* Supports: AGG_FUNC(...) OP literal                                  */
    /* where OP is one of =, <>, <, <=, >, >=                             */
    /* The LHS aggregate is matched against the agg_specs already built   */
    /* above.  Unsupported expressions are silently ignored.              */
    /* ------------------------------------------------------------------ */
    if (sel->having &&
        sel->having->kind == KEEL_SQL_NODE_EXPR_BINARY &&
        out->nhaving_preds < KEEL_SCATTER_MAX_HAVING_PREDS) {

        const keel_sql_expr_binary_t* bin =
            (const keel_sql_expr_binary_t*)sel->having;

        /* Map binary operator to keel_cmp_op_t */
        keel_cmp_op_t cmp_op;
        bool op_ok = true;
        switch (bin->op) {
        case KEEL_SQL_BINOP_EQ: cmp_op = KEEL_CMP_EQ; break;
        case KEEL_SQL_BINOP_NE: cmp_op = KEEL_CMP_NE; break;
        case KEEL_SQL_BINOP_LT: cmp_op = KEEL_CMP_LT; break;
        case KEEL_SQL_BINOP_LE: cmp_op = KEEL_CMP_LE; break;
        case KEEL_SQL_BINOP_GT: cmp_op = KEEL_CMP_GT; break;
        case KEEL_SQL_BINOP_GE: cmp_op = KEEL_CMP_GE; break;
        default: op_ok = false; break;
        }

        if (op_ok && bin->left && bin->right &&
            bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) {

            const keel_sql_expr_literal_t* rlit =
                (const keel_sql_expr_literal_t*)bin->right;

            /* Find the column index that the LHS expression refers to.
             * Strategy: if LHS is an AGGR/FUNC node, search agg_specs for
             * the matching function at any col_index, and use that col_index.
             * If LHS is a column reference, use resolve_order_col. */
            int16_t having_col = KEEL_SORT_COL_UNRESOLVED;

            if (bin->left->kind == KEEL_SQL_NODE_EXPR_FUNC ||
                bin->left->kind == KEEL_SQL_NODE_EXPR_AGGR) {
                const keel_sql_expr_func_t* lfn =
                    (const keel_sql_expr_func_t*)bin->left;
                keel_agg_func_t laf = KEEL_AGG_NONE;
                const char* ln = lfn->name.data;
                size_t      ll = lfn->name.len;
                if      (ll == 5 && strncasecmp(ln, "count", 5) == 0) laf = KEEL_AGG_COUNT;
                else if (ll == 3 && strncasecmp(ln, "sum",   3) == 0) laf = KEEL_AGG_SUM;
                else if (ll == 3 && strncasecmp(ln, "min",   3) == 0) laf = KEEL_AGG_MIN;
                else if (ll == 3 && strncasecmp(ln, "max",   3) == 0) laf = KEEL_AGG_MAX;
                else if (ll == 3 && strncasecmp(ln, "avg",   3) == 0) laf = KEEL_AGG_AVG;

                /* Find the first matching agg_spec.
                 * For AVG, look in avg_finalize_specs (out_col holds the final avg). */
                if (laf == KEEL_AGG_AVG) {
                    /* AVG HAVING: use the out_col from avg_finalize_specs */
                    if (out->navg_finalize_specs > 0) {
                        having_col = out->avg_finalize_specs[0].out_col;
                    }
                } else if (laf != KEEL_AGG_NONE) {
                    for (uint16_t ai = 0; ai < out->nagg_specs; ai++) {
                        if (out->agg_specs[ai].func == laf) {
                            having_col = out->agg_specs[ai].col_index;
                            break;
                        }
                    }
                    /* If not found in SELECT targets, add as an extra column */
                    if (having_col == KEEL_SORT_COL_UNRESOLVED &&
                        out->nhaving_extra_agg_exprs < 4 &&
                        out->nagg_specs < KEEL_SCATTER_MAX_AGG_COLS) {
                        uint16_t ei = out->nhaving_extra_agg_exprs;
                        having_col = total_targets + (int16_t)ei;
                        having_agg_sql_text(lfn,
                            out->having_extra_agg_exprs[ei],
                            sizeof(out->having_extra_agg_exprs[ei]));
                        out->agg_specs[out->nagg_specs].func      = laf;
                        out->agg_specs[out->nagg_specs].col_index = having_col;
                        out->nagg_specs++;
                        out->nhaving_extra_agg_exprs++;
                        out->requires_merge = true;
                    }
                }
            } else {
                /* Column reference */
                having_col = resolve_order_col(bin->left, sel->targets);
            }

            if (having_col != KEEL_SORT_COL_UNRESOLVED) {
                keel_having_pred_t* pred =
                    &out->having_preds[out->nhaving_preds];
                pred->col_index = having_col;
                pred->op        = cmp_op;

                /* Encode RHS literal as a text string */
                switch (rlit->lit_type) {
                case KEEL_SQL_LIT_INT: {
                    int n = snprintf(pred->literal, sizeof pred->literal,
                                     "%lld", (long long)rlit->value.int_val);
                    pred->literal_len = (int32_t)(n > 0 ? n : 0);
                    break;
                }
                case KEEL_SQL_LIT_FLOAT: {
                    int n = snprintf(pred->literal, sizeof pred->literal,
                                     "%.17g", rlit->value.float_val);
                    pred->literal_len = (int32_t)(n > 0 ? n : 0);
                    break;
                }
                case KEEL_SQL_LIT_STRING: {
                    size_t sl = rlit->value.str_val.len;
                    if (sl >= sizeof pred->literal)
                        sl = sizeof pred->literal - 1;
                    memcpy(pred->literal, rlit->value.str_val.data, sl);
                    pred->literal[sl] = '\0';
                    pred->literal_len = (int32_t)sl;
                    break;
                }
                case KEEL_SQL_LIT_NULL:
                    pred->literal_len = -1;
                    break;
                default:
                    pred->literal_len = -1;
                    break;
                }

                out->nhaving_preds++;
            }
        }
    }

    /* ------------------------------------------------------------------ */
    /* CTE merge spec: when the outer SELECT is a pure projection over a  */
    /* non-recursive CTE and has no aggregates of its own, extract the    */
    /* GROUP BY + agg_specs from the CTE's inner query.  This lets KEEL   */
    /* properly scatter-merge queries like:                                */
    /*   WITH agg AS (SELECT k, SUM(v) FROM t GROUP BY k)                 */
    /*   SELECT k, total FROM agg ORDER BY k                              */
    /*                                                                     */
    /* ctx_with allows chained CTEs: when a CTE body references another   */
    /* CTE name, we look it up in the parent WITH clause.                  */
    /* ------------------------------------------------------------------ */
    const keel_sql_list_t* effective_with =
        sel->with_clause ? sel->with_clause : ctx_with;

    if (effective_with && !sel->with_recursive &&
        out->nagg_specs == 0 && out->ngroup_key_cols == 0 && sel->from &&
        sel->from->kind == KEEL_SQL_NODE_TABLE_REF) {

        const keel_sql_table_ref_t* tref =
            (const keel_sql_table_ref_t*)sel->from;
        keel_str_t cte_name = tref->table;

        /* Walk effective_with list to find the CTE being referenced */
        const keel_sql_node_t* cn = effective_with->head;
        while (cn) {
            if (cn->kind == KEEL_SQL_NODE_CLAUSE_CTE) {
                const keel_sql_cte_t* cte = (const keel_sql_cte_t*)cn;
                if (cte->name.len == cte_name.len &&
                    strncasecmp(cte->name.data, cte_name.data,
                                cte_name.len) == 0 &&
                    cte->query) {
                    /* Recursively extract from the CTE body.
                     * Pass effective_with so chained CTEs can resolve
                     * further references (e.g. ranked → base). */
                    scatter_extract_merge_spec_impl(cte->query, shard_col, out,
                                                    effective_with);
                    break;
                }
            }
            cn = cn->next;
        }

        /* If the outer SELECT has a WHERE clause on post-aggregate columns
         * (e.g. WHERE total > 140 after a CTE with GROUP BY), strip the
         * WHERE from per-shard SQL and apply it post-merge as HAVING. */
        if (sel->where &&
            (out->nagg_specs > 0 || out->ngroup_key_cols > 0) &&
            sel->where->kind == KEEL_SQL_NODE_EXPR_BINARY &&
            out->nhaving_preds < KEEL_SCATTER_MAX_HAVING_PREDS) {

            const keel_sql_expr_binary_t* bin =
                (const keel_sql_expr_binary_t*)sel->where;
            keel_cmp_op_t cmp_op;
            bool op_ok = true;
            switch (bin->op) {
            case KEEL_SQL_BINOP_EQ: cmp_op = KEEL_CMP_EQ; break;
            case KEEL_SQL_BINOP_NE: cmp_op = KEEL_CMP_NE; break;
            case KEEL_SQL_BINOP_LT: cmp_op = KEEL_CMP_LT; break;
            case KEEL_SQL_BINOP_LE: cmp_op = KEEL_CMP_LE; break;
            case KEEL_SQL_BINOP_GT: cmp_op = KEEL_CMP_GT; break;
            case KEEL_SQL_BINOP_GE: cmp_op = KEEL_CMP_GE; break;
            default: op_ok = false; break;
            }
            if (op_ok && bin->left && bin->right &&
                bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                const keel_sql_expr_literal_t* rlit =
                    (const keel_sql_expr_literal_t*)bin->right;
                int16_t pred_col = resolve_order_col(bin->left, sel->targets);
                if (pred_col != KEEL_SORT_COL_UNRESOLVED) {
                    keel_having_pred_t* pred =
                        &out->having_preds[out->nhaving_preds];
                    pred->col_index = pred_col;
                    pred->op        = cmp_op;
                    switch (rlit->lit_type) {
                    case KEEL_SQL_LIT_INT: {
                        int nn = snprintf(pred->literal, sizeof pred->literal,
                                         "%lld", (long long)rlit->value.int_val);
                        pred->literal_len = (int32_t)(nn > 0 ? nn : 0);
                        break;
                    }
                    case KEEL_SQL_LIT_FLOAT: {
                        int nn = snprintf(pred->literal, sizeof pred->literal,
                                         "%.17g", rlit->value.float_val);
                        pred->literal_len = (int32_t)(nn > 0 ? nn : 0);
                        break;
                    }
                    case KEEL_SQL_LIT_STRING: {
                        size_t sl = rlit->value.str_val.len;
                        if (sl >= sizeof pred->literal) sl = sizeof pred->literal - 1;
                        memcpy(pred->literal, rlit->value.str_val.data, sl);
                        pred->literal[sl] = '\0';
                        pred->literal_len = (int32_t)sl;
                        break;
                    }
                    default:
                        pred->literal_len = -1;
                        break;
                    }
                    out->nhaving_preds++;
                    out->requires_outer_where_strip = true;
                }
            }
        }

        /* Detect ROUND(sum_col / count_col, N) pattern in outer targets after
         * CTE extraction set up a COUNT + SUM in agg_specs.
         * Adds an avg_finalize_spec for the division expression column and sets
         * requires_outer_avg_finalize so the division is computed post-merge
         * WITHOUT removing columns (unlike the standard AVG rewrite). */
        if (out->nagg_specs >= 2 && sel->targets &&
            out->navg_finalize_specs < KEEL_SCATTER_MAX_AVG_SPECS) {
            int16_t t_idx = 0;
            const keel_sql_node_t* t = sel->targets->head;
            while (t) {
                const keel_sql_select_target_t* st =
                    (const keel_sql_select_target_t*)t;
                const keel_sql_node_t* texpr = st ? st->expr : NULL;
                /* Look for FUNC(BINARY_DIV(...)) — e.g., ROUND(sum/count, N) */
                if (texpr &&
                    (texpr->kind == KEEL_SQL_NODE_EXPR_FUNC ||
                     texpr->kind == KEEL_SQL_NODE_EXPR_AGGR)) {
                    const keel_sql_expr_func_t* tfn =
                        (const keel_sql_expr_func_t*)texpr;
                    if (tfn->args && tfn->args->head &&
                        tfn->args->head->kind == KEEL_SQL_NODE_EXPR_BINARY) {
                        const keel_sql_expr_binary_t* divop =
                            (const keel_sql_expr_binary_t*)tfn->args->head;
                        if (divop->op == KEEL_SQL_BINOP_DIV &&
                            divop->left && divop->right) {
                            /* Unwrap CAST on lhs */
                            const keel_sql_node_t* lhs = divop->left;
                            while (lhs && lhs->kind == KEEL_SQL_NODE_EXPR_CAST) {
                                const keel_sql_expr_cast_t* c =
                                    (const keel_sql_expr_cast_t*)lhs;
                                lhs = c->expr;
                            }
                            const keel_sql_node_t* rhs = divop->right;
                            while (rhs && rhs->kind == KEEL_SQL_NODE_EXPR_CAST) {
                                const keel_sql_expr_cast_t* c =
                                    (const keel_sql_expr_cast_t*)rhs;
                                rhs = c->expr;
                            }
                            int16_t lhs_ci = resolve_order_col(lhs, sel->targets);
                            int16_t rhs_ci = resolve_order_col(rhs, sel->targets);
                            if (lhs_ci >= 0 && rhs_ci >= 0 &&
                                lhs_ci != t_idx && rhs_ci != t_idx) {
                                int16_t sum_ci = -1, cnt_ci = -1;
                                for (uint16_t ai = 0; ai < out->nagg_specs; ai++) {
                                    if (out->agg_specs[ai].col_index == lhs_ci &&
                                        out->agg_specs[ai].func == KEEL_AGG_SUM)
                                        sum_ci = lhs_ci;
                                    if (out->agg_specs[ai].col_index == rhs_ci &&
                                        out->agg_specs[ai].func == KEEL_AGG_COUNT)
                                        cnt_ci = rhs_ci;
                                }
                                if (sum_ci >= 0 && cnt_ci >= 0 &&
                                    out->navg_finalize_specs < KEEL_SCATTER_MAX_AVG_SPECS) {
                                    out->avg_finalize_specs[out->navg_finalize_specs].sum_col   = sum_ci;
                                    out->avg_finalize_specs[out->navg_finalize_specs].count_col = cnt_ci;
                                    out->avg_finalize_specs[out->navg_finalize_specs].out_col   = t_idx;
                                    out->navg_finalize_specs++;
                                    out->requires_outer_avg_finalize = true;
                                    out->requires_merge = true;
                                }
                            }
                        }
                    }
                }
                t_idx++;
                t = t->next;
            }
        }

        /* Detect CTE used twice via JOIN: when the outer FROM is not a simple
         * TABLE_REF but a JOIN, and the CTE uses SELECT DISTINCT, scatter
         * results will contain duplicate rows. Flag for post-sort dedup. */
    } else if (effective_with && !sel->with_recursive &&
               out->nagg_specs == 0 && out->ngroup_key_cols == 0 &&
               out->nord_agg_specs == 0 &&
               sel->from &&
               sel->from->kind != KEEL_SQL_NODE_TABLE_REF) {
        /* Check if any CTE uses SELECT DISTINCT */
        const keel_sql_node_t* cn = effective_with->head;
        while (cn) {
            if (cn->kind == KEEL_SQL_NODE_CLAUSE_CTE) {
                const keel_sql_cte_t* cte = (const keel_sql_cte_t*)cn;
                if (cte->query &&
                    cte->query->kind == KEEL_SQL_NODE_STMT_SELECT) {
                    const keel_sql_stmt_select_t* cs =
                        (const keel_sql_stmt_select_t*)cte->query;
                    if (cs->distinct) {
                        out->requires_scatter_dedup = true;
                        out->requires_merge = true;
                        break;
                    }
                }
            }
            cn = cn->next;
        }
    }

    /* ------------------------------------------------------------------ */
    /* Derived table (inline subquery) merge spec: when the outer SELECT   */
    /* uses a subquery as FROM source and has no aggregates of its own,   */
    /* extract the GROUP BY + agg_specs from the subquery body.           */
    /*   SELECT category, total                                            */
    /*   FROM (SELECT category, SUM(value) AS total FROM events            */
    /*         GROUP BY category) t                                        */
    /*   WHERE total > 150                                                 */
    /* ------------------------------------------------------------------ */
    if (sel->from) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "router: from->kind=%d TABLE_SUBQUERY=%d nagg=%u ngroup=%u with=%p ctx_with=%p",
            (int)sel->from->kind, (int)KEEL_SQL_NODE_TABLE_SUBQUERY,
            out->nagg_specs, out->ngroup_key_cols,
            (void*)sel->with_clause, (void*)ctx_with);
    }
    if (!sel->with_clause && !ctx_with &&
        out->nagg_specs == 0 && out->ngroup_key_cols == 0 &&
        out->nord_agg_specs == 0 &&
        sel->from &&
        sel->from->kind == KEEL_SQL_NODE_TABLE_SUBQUERY) {

        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "(nagg=%u ngroup=%u)", out->nagg_specs, out->ngroup_key_cols);

        const keel_sql_table_subquery_t* tsq =
            (const keel_sql_table_subquery_t*)sel->from;
        if (tsq->subquery) {
            scatter_extract_merge_spec_impl(tsq->subquery, shard_col, out, NULL);

            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "router: derived-table inner extraction done "
                "(nagg=%u ngroup=%u where_kind=%d)",
                out->nagg_specs, out->ngroup_key_cols,
                sel->where ? (int)sel->where->kind : -1);

            /* Apply outer WHERE as having_pred post-merge */
            if (sel->where &&
                (out->nagg_specs > 0 || out->ngroup_key_cols > 0) &&
                sel->where->kind == KEEL_SQL_NODE_EXPR_BINARY &&
                out->nhaving_preds < KEEL_SCATTER_MAX_HAVING_PREDS) {

                const keel_sql_expr_binary_t* bin =
                    (const keel_sql_expr_binary_t*)sel->where;
                keel_cmp_op_t cmp_op;
                bool op_ok = true;
                switch (bin->op) {
                case KEEL_SQL_BINOP_EQ: cmp_op = KEEL_CMP_EQ; break;
                case KEEL_SQL_BINOP_NE: cmp_op = KEEL_CMP_NE; break;
                case KEEL_SQL_BINOP_LT: cmp_op = KEEL_CMP_LT; break;
                case KEEL_SQL_BINOP_LE: cmp_op = KEEL_CMP_LE; break;
                case KEEL_SQL_BINOP_GT: cmp_op = KEEL_CMP_GT; break;
                case KEEL_SQL_BINOP_GE: cmp_op = KEEL_CMP_GE; break;
                default: op_ok = false; break;
                }
                if (op_ok && bin->left && bin->right &&
                    bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                    const keel_sql_expr_literal_t* rlit =
                        (const keel_sql_expr_literal_t*)bin->right;
                    int16_t pred_col = resolve_order_col(bin->left, sel->targets);
                    if (pred_col != KEEL_SORT_COL_UNRESOLVED) {
                        keel_having_pred_t* pred =
                            &out->having_preds[out->nhaving_preds];
                        pred->col_index = pred_col;
                        pred->op        = cmp_op;
                        switch (rlit->lit_type) {
                        case KEEL_SQL_LIT_INT: {
                            int nn = snprintf(pred->literal, sizeof pred->literal,
                                             "%lld", (long long)rlit->value.int_val);
                            pred->literal_len = (int32_t)(nn > 0 ? nn : 0);
                            break;
                        }
                        case KEEL_SQL_LIT_FLOAT: {
                            int nn = snprintf(pred->literal, sizeof pred->literal,
                                             "%.17g", rlit->value.float_val);
                            pred->literal_len = (int32_t)(nn > 0 ? nn : 0);
                            break;
                        }
                        case KEEL_SQL_LIT_STRING: {
                            size_t sl = rlit->value.str_val.len;
                            if (sl >= sizeof pred->literal) sl = sizeof pred->literal - 1;
                            memcpy(pred->literal, rlit->value.str_val.data, sl);
                            pred->literal[sl] = '\0';
                            pred->literal_len = (int32_t)sl;
                            break;
                        }
                        default:
                            pred->literal_len = -1;
                            break;
                        }
                        out->nhaving_preds++;
                        out->requires_outer_where_strip = true;
                    }
                }
            }
        }
    }
}

keel_error_t keel_router_dispatch_sql(keel_router_t*                   router,                                      keel_str_t                        sql,
                                      const keel_route_session_t*       session,
                                      const keel_shard_bound_params_t*  params,
                                      bool                              is_write,
                                      keel_dispatch_result_t*           out) {
    if (!router || !sql.data || sql.len == 0 || !out) {
        return KEEL_ERR_INVALID_ARG;
    }

    memset(out, 0, sizeof(*out));

    if (router->shard_rule_count == 0) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    pthread_mutex_lock(&router->dispatch_mutex);

    keel_error_t result = KEEL_ERR_NOT_SUPPORTED;
    keel_parse_result_t parse;

    /* Parse once through the configured parser contract; reused for the
     * SINGLE routing path and transitional sharding bridge. */
    keel_qt_query_t* qt = router_parse_postgresql_sql(sql, &parse);

    /* ---------------------------------------------------------------------- */
    /* Fail-closed gate: WITH RECURSIVE across sharded tables.                */
    /*                                                                        */
    /* When a recursive CTE references a sharded table the planner sometimes  */
    /* classifies the statement as SCATTER (engine_scatter concatenates rows  */
    /* from each shard's local recursion → silently wrong results — see       */
    /* docs/LIMITATIONS.md §1.1) and sometimes as UNSUPPORTED (planner can't  */
    /* extract a shard key past the CTE → engine falls back to single-shard,  */
    /* which is correct but masks the underlying hazard). To make the proxy   */
    /* deterministic, we reject the statement up front whenever the outer    */
    /* SELECT carries WITH RECURSIVE *and* the query touches any registered  */
    /* shard table. Recursive CTEs over non-sharded tables fall through to   */
    /* the rule loop and ultimately to the unsharded fallback.               */
    /* ---------------------------------------------------------------------- */
    if (qt && qt->ast && qt->ast->kind == KEEL_SQL_NODE_STMT_SELECT &&
        ((const keel_sql_stmt_select_t*)qt->ast)->with_recursive)
    {
        bool touches_sharded = false;
        for (size_t i = 0; i < qt->table_count && !touches_sharded; i++) {
            keel_str_t t = qt->tables[i].table;
            if (t.len == 0) continue;
            for (size_t r = 0; r < router->shard_rule_count; r++) {
                const char* rt = router->shard_rules[r].table;
                size_t rl = rt ? strlen(rt) : 0;
                if (rl == t.len && rl > 0 && strncasecmp(rt, t.data, rl) == 0) {
                    touches_sharded = true;
                    break;
                }
            }
        }
        if (touches_sharded) {
            keel_router_track_unsupported_pattern(
                router, KEEL_SCATTER_UNSUPPORTED_RECURSIVE_CTE);
            out->reject_reason = KEEL_DISPATCH_REJECT_RECURSIVE_CTE;
            snprintf(out->reject_message, sizeof out->reject_message,
                "WITH RECURSIVE across sharded tables is not supported "
                "(silent-wrong-result risk); pin the session or run on a single shard");
            result = KEEL_ERR_NOT_SUPPORTED;
            goto dispatch_done;
        }
    }

    /* Iterate registered rules looking for the first non-UNSUPPORTED outcome. */
    for (size_t i = 0; i < router->shard_rule_count; i++) {
        const keel_shard_rule_t* rule = &router->shard_rules[i];
        keel_shard_plan_t plan;
        keel_shard_plan(sql, rule, params, router->temp_arena, &plan);

        if (plan.kind == KEEL_SHARD_PLAN_UNSUPPORTED) {
            continue;
        }

        if (plan.kind == KEEL_SHARD_PLAN_SINGLE) {
            out->kind = KEEL_DISPATCH_SINGLE;

            /* Feature 8: migration intercept */
            if (rule->state == KEEL_SHARD_STATE_MIGRATING
                && (plan.shard_index == rule->migrate_src_shard
                    || plan.shard_index == rule->migrate_dst_shard)) {
                bool is_modifying = qt
                    ? !should_use_readonly(router, qt, session)
                    : is_write;
                if (is_modifying) {
                    /* Dual-write: scatter to both src and dst shard */
                    out->kind           = KEEL_DISPATCH_SCATTER;
                    out->scatter.count  = 2;
                    out->scatter.failed = 0;
                    keel_error_t e0 = route_shard_for_scatter(
                        router, session, true,
                        rule->migrate_src_shard, &out->scatter.decisions[0]);
                    keel_error_t e1 = route_shard_for_scatter(
                        router, session, true,
                        rule->migrate_dst_shard, &out->scatter.decisions[1]);
                    out->scatter.failed = (e0 != KEEL_OK ? 1 : 0) + (e1 != KEEL_OK ? 1 : 0);
                    /* Track scatter stats */
                    router->stats.shard_scatter_hits++;
                    router->stats.shard_scatter_failed += out->scatter.failed;
                    result = KEEL_OK;
                } else {
                    /* Read-from-new: route reads to the destination shard */
                    result = route_internal(router, qt, session,
                                         /*use_shard_filter=*/true,
                                         rule->migrate_dst_shard, &out->single);
                }
            } else {
                result = route_internal(
                    router, qt, session,
                    /*use_shard_filter=*/true, plan.shard_index, &out->single);
            }
            goto dispatch_done;
        }

        /* KEEL_SHARD_PLAN_SCATTER */
        out->kind = KEEL_DISPATCH_SCATTER;

        /* ------------------------------------------------------------------ */
        /* Fail-closed gates BEFORE we materialise the scatter plan.          */
        /*                                                                    */
        /* 1. `WITH RECURSIVE …` over sharded tables produces silently wrong  */
        /*    results (each shard evaluates the recursion against its local   */
        /*    slice; rows duplicate / corrupt globally — see                  */
        /*    docs/LIMITATIONS.md §1.1). Reject regardless of the gate.       */
        /* 2. When `scatter_merge = off` (default), any scatter classification */
        /*    is rejected so unsupported shapes never silently fan out.       */
        /*                                                                    */
        /* Both populate out->reject_reason so the engine emits a clear       */
        /* PostgreSQL/MySQL error instead of falling back to single-shard.    */
        /* ------------------------------------------------------------------ */
        if (qt && qt->ast && qt->ast->kind == KEEL_SQL_NODE_STMT_SELECT) {
            const keel_sql_stmt_select_t* rsel =
                (const keel_sql_stmt_select_t*)qt->ast;
            if (rsel->with_recursive) {
                keel_router_track_unsupported_pattern(
                    router, KEEL_SCATTER_UNSUPPORTED_RECURSIVE_CTE);
                out->reject_reason = KEEL_DISPATCH_REJECT_RECURSIVE_CTE;
                snprintf(out->reject_message, sizeof out->reject_message,
                    "WITH RECURSIVE across shards is not supported "
                    "(silent-wrong-result risk); pin the session or run on a single shard");
                result = KEEL_ERR_NOT_SUPPORTED;
                goto dispatch_done;
            }
        }
        if (!router->config.scatter_merge_enabled) {
            keel_router_track_unsupported_pattern(
                router, KEEL_SCATTER_UNSUPPORTED_GATE_DISABLED);
            out->reject_reason = KEEL_DISPATCH_REJECT_SCATTER_DISABLED;
            snprintf(out->reject_message, sizeof out->reject_message,
                "scatter-merge dispatch is disabled "
                "(set scatter_merge = on in the worker_group to opt in to the experimental feature)");
            result = KEEL_ERR_NOT_SUPPORTED;
            goto dispatch_done;
        }

        keel_error_t err = keel_router_scatter_servers(router, session, rule, is_write, &out->scatter);
        if (err == KEEL_OK && qt && qt->ast)
            scatter_extract_merge_spec(qt->ast, rule ? rule->column : NULL, out);

        /* ------------------------------------------------------------------ */
        /* Phase H — Window function fallback                                 */
        /* When OVER clauses are present, scatter-merge is impossible: route  */
        /* to a single shard (shard 0 / primary) instead.                    */
        /* ------------------------------------------------------------------ */
        if (err == KEEL_OK && out->has_window_funcs) {
            out->kind                = KEEL_DISPATCH_SINGLE;
            out->window_forced_single = true;
            memset(&out->scatter, 0, sizeof out->scatter);
            err = route_internal(router, qt, session,
                                  /*use_shard_filter=*/true, 0, &out->single);
            result = err;
            goto dispatch_done;
        }

        /* ------------------------------------------------------------------ */
        /* Phase 8 — Constant CTE single-shard fallback                       */
        /* WITH cte AS (SELECT <constant-expr>) SELECT … FROM cte             */
        /* The CTE body has no FROM clause and therefore produces the same    */
        /* row(s) on every shard.  Scattering would duplicate them; instead   */
        /* route to a single shard so the proxy returns the CTE's natural    */
        /* row count (e.g. COUNT(*) over a constant CTE returns 1, not N).   */
        /* Conservative: only when outer FROM is a single TABLE_REF that      */
        /* names a CTE whose body has no FROM clause and is non-recursive.   */
        /* ------------------------------------------------------------------ */
        if (err == KEEL_OK && !is_write && qt && qt->ast &&
            qt->ast->kind == KEEL_SQL_NODE_STMT_SELECT) {
            const keel_sql_stmt_select_t* osel =
                (const keel_sql_stmt_select_t*)qt->ast;
            if (osel->with_clause && !osel->with_recursive && osel->from &&
                osel->from->kind == KEEL_SQL_NODE_TABLE_REF) {
                const keel_sql_table_ref_t* otref =
                    (const keel_sql_table_ref_t*)osel->from;
                const keel_sql_node_t* cn = osel->with_clause->head;
                bool is_constant_cte = false;
                while (cn) {
                    if (cn->kind == KEEL_SQL_NODE_CLAUSE_CTE) {
                        const keel_sql_cte_t* cte =
                            (const keel_sql_cte_t*)cn;
                        if (!cte->recursive && cte->query &&
                            cte->query->kind == KEEL_SQL_NODE_STMT_SELECT &&
                            cte->name.len == otref->table.len &&
                            strncasecmp(cte->name.data, otref->table.data,
                                        otref->table.len) == 0) {
                            const keel_sql_stmt_select_t* cbody =
                                (const keel_sql_stmt_select_t*)cte->query;
                            if (cbody->from == NULL) {
                                is_constant_cte = true;
                            }
                            break;
                        }
                    }
                    cn = cn->next;
                }
                if (is_constant_cte) {
                    out->kind = KEEL_DISPATCH_SINGLE;
                    memset(&out->scatter, 0, sizeof out->scatter);
                    err = route_internal(router, qt, session,
                                          /*use_shard_filter=*/true, 0,
                                          &out->single);
                    result = err;
                    goto dispatch_done;
                }
            }
        }

        /* ------------------------------------------------------------------ */
        /* Phase H — 2PC coordinator for scatter writes                       */
        /* Allocate and initialise a two-phase commit coordinator so the      */
        /* caller can drive PREPARE/COMMIT/ROLLBACK across all shards.        */
        /* ------------------------------------------------------------------ */
        if (err == KEEL_OK && is_write &&
            out->scatter.participating_shards_mask != 0) {
            keel_2pc_coord_t* coord =
                (keel_2pc_coord_t*)keel_malloc(sizeof *coord);
            if (coord) {
                uint64_t session_id =
                    session ? (uint64_t)(uintptr_t)session : 0;
                static _Atomic uint64_t twopc_seq = 0;
                uint64_t seq = atomic_fetch_add(&twopc_seq, UINT64_C(1));
                keel_2pc_coord_init(coord, session_id, seq);
                if (keel_2pc_coord_begin(coord, &out->scatter) == KEEL_OK) {
                    out->twopc          = coord;
                    out->twopc_required = true;
                    router->stats.twopc_started++;
                } else {
                    keel_free(coord);
                }
            }
        }

        result = err;
        goto dispatch_done;
    }

dispatch_done:
    /* Telemetry: tag scatter dispatches whose correctness depends on patterns
     * the engine does not fully merge.  Operators alert on
     * keel_scatter_unsupported_pattern_total{kind=...} for silent-wrong-result
     * risk.  Best-effort: only inspected when dispatch succeeded. */
    if (result == KEEL_OK) {
        if (out->has_window_funcs || out->window_forced_single) {
            keel_router_track_unsupported_pattern(
                router, KEEL_SCATTER_UNSUPPORTED_WINDOW_FUNC);
        }
        for (uint16_t oi = 0; oi < out->nord_agg_specs; oi++) {
            keel_ord_agg_func_t f = out->ord_agg_specs[oi].func;
            if (f == KEEL_ORD_AGG_PERCENTILE_CONT ||
                f == KEEL_ORD_AGG_PERCENTILE_DISC) {
                keel_router_track_unsupported_pattern(
                    router, KEEL_SCATTER_UNSUPPORTED_PERCENTILE);
                break;
            }
        }
        if (qt && qt->ast && qt->ast->kind == KEEL_SQL_NODE_STMT_SELECT &&
            out->kind == KEEL_DISPATCH_SCATTER) {
            const keel_sql_stmt_select_t* sel =
                (const keel_sql_stmt_select_t*)qt->ast;
            if (sel->with_recursive) {
                /* Already rejected above; defensive no-op kept for clarity. */
                keel_router_track_unsupported_pattern(
                    router, KEEL_SCATTER_UNSUPPORTED_RECURSIVE_CTE);
            }
            if (sel->set_op != KEEL_SQL_SET_NONE) {
                keel_router_track_unsupported_pattern(
                    router, KEEL_SCATTER_UNSUPPORTED_UNION_ALL);
            }
        }
        /* DML with RETURNING dispatched as scatter: Phase 3 forwards the rows
         * but cross-shard ordering of RETURNING is not preserved.  Operators
         * alert on this kind to detect order-sensitive consumers. */
        if (qt && qt->ast && out->kind == KEEL_DISPATCH_SCATTER) {
            bool has_returning = false;
            switch (qt->ast->kind) {
                case KEEL_SQL_NODE_STMT_INSERT:
                    has_returning = ((const keel_sql_stmt_insert_t*)qt->ast)
                                        ->returning != NULL;
                    break;
                case KEEL_SQL_NODE_STMT_UPDATE:
                    has_returning = ((const keel_sql_stmt_update_t*)qt->ast)
                                        ->returning != NULL;
                    break;
                case KEEL_SQL_NODE_STMT_DELETE:
                    has_returning = ((const keel_sql_stmt_delete_t*)qt->ast)
                                        ->returning != NULL;
                    break;
                default:
                    break;
            }
            if (has_returning) {
                keel_router_track_unsupported_pattern(
                    router, KEEL_SCATTER_UNSUPPORTED_DML_RETURNING);
            }
        }
        /* DDL fanned out to all shards: success here means the schema was
         * applied to every shard.  This is the intended path but operators
         * alert on the counter to know when their fleet's schema changed. */
        if (qt && qt->ast && out->kind == KEEL_DISPATCH_SCATTER) {
            switch (qt->ast->kind) {
                case KEEL_SQL_NODE_STMT_CREATE:
                case KEEL_SQL_NODE_STMT_ALTER:
                case KEEL_SQL_NODE_STMT_DROP:
                case KEEL_SQL_NODE_STMT_TRUNCATE:
                    keel_router_track_unsupported_pattern(
                        router, KEEL_SCATTER_UNSUPPORTED_DDL);
                    break;
                default:
                    break;
            }
        }
    }
    keel_parse_result_free(keel_parser_builtin_postgresql_sql(), &parse);
    pthread_mutex_unlock(&router->dispatch_mutex);
    return result;
}

/* ============================================================================
 * Scatter result aggregation
 * ============================================================================ */

void keel_route_agg_init(keel_route_agg_t* result,
                              keel_route_merge_fn  merge,
                              void*                  user_ctx) {
    if (!result) {
        return;
    }
    memset(result, 0, sizeof(*result));
    result->merge    = merge;
    result->user_ctx = user_ctx;
}

/**
 * @brief Deliver one shard's result rows into an aggregation context.
 *
 * @param result       Scatter result aggregator.
 * @param shard_index  Zero-based shard that produced these rows.
 * @param rows         Pointer to the row data; `NULL` signals a shard
 *                     failure (no rows available).
 * @param row_count    Number of rows in `rows`.
 * @return Nothing.
 *
 * Behavior:
 * - A `NULL` `rows` pointer increments `result->shards_failed`.
 * - When `result->merge` is set, calls it with `(result, shard_index, rows,
 *   row_count, user_ctx)` to allow caller-defined aggregation (e.g. sorting
 *   or de-duplicating rows from multiple shards).
 * - Unconditionally updates `result->total_rows` and
 *   `result->shards_completed` counters.
 *
 * Notes:
 * - Pairs with `keel_route_agg_init()` for setup and with
 *   `keel_router_scatter_servers()` for the routing step that precedes it.
 */
void keel_route_agg_feed(keel_route_agg_t* result,
                              size_t                 shard_index,
                              const void*            rows,
                              size_t                 row_count) {
    if (!result) {
        return;
    }

    if (!rows) {
        result->shards_failed++;
        return;
    }

    if (result->merge) {
        result->merge(result, shard_index, rows, row_count, result->user_ctx);
    }

    result->total_rows       += row_count;
    result->shards_completed++;
}

/* ============================================================================
 * Feature 15: Query timeout enforcement
 * ============================================================================ */

keel_error_t keel_router_dispatch_sql_timed(
    keel_router_t*                   router,
    keel_str_t                        sql,
    const keel_route_session_t*       session,
    const keel_shard_bound_params_t*  params,
    bool                              is_write,
    keel_duration_t                   timeout,
    keel_dispatch_result_t*           out) {
    if (!router || !out) return KEEL_ERR_INVALID_ARG;

    /* Resolve effective timeout */
    keel_duration_t effective = timeout;
    if (effective == 0) {
        effective = router->config.query_timeout;
    }

    keel_time_t t_start = keel_time_now();

    keel_error_t err = keel_router_dispatch_sql(router, sql, session,
                                                params, is_write, out);

    /* Check timeout AFTER the call: if the dispatch itself took too long,
     * discard the result and report timeout. */
    if (effective > 0) {
        keel_duration_t elapsed = keel_time_diff(t_start, keel_time_now());
        if (elapsed >= effective) {
            memset(out, 0, sizeof(*out));
            return KEEL_ERR_QUERY_TIMEOUT;
        }
    }

    return err;
}
