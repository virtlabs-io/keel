/**
 * @file connpool.c
 * @brief Shard-aware synchronous connection pool for the routing layer.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "connpool.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"
#include "keel/log/log.h"

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>

/* ============================================================================
 * Default values
 * ============================================================================ */

#define CONNPOOL_DEFAULT_MAX_CONNS             16
#define CONNPOOL_DEFAULT_CONNECT_TIMEOUT_MS  5000
#define CONNPOOL_DEFAULT_ACQUIRE_TIMEOUT_MS  5000
#define CONNPOOL_DEFAULT_HEALTH_INTERVAL_MS 60000
#define CONNPOOL_ACQUIRE_SPIN_SLEEP_US        500  /* 0.5 ms between acquire retries */

/* ============================================================================
 * Internal pool structure
 * ============================================================================ */

struct keel_connpool {
    const keel_route_server_t* server;      /* borrowed; lives longer than pool */

    /* Resolved host for fast re-connect */
    char  resolved_host[256];
    int   resolved_port;

    /* Configuration (resolved) */
    size_t   min_conns;
    size_t   max_conns;
    uint32_t idle_timeout_ms;
    uint32_t connect_timeout_ms;
    uint32_t acquire_timeout_ms;
    uint32_t health_check_interval_ms;
    bool   (*health_probe)(keel_conn_t* conn, void* udata);
    void*    health_probe_udata;

    /* Connection slab */
    keel_conn_t  slots[KEEL_CONNPOOL_MAX_CONNS];
    size_t       slot_count;           /* logical capacity (≤ max_conns) */

    /* Protects slots[], stats, and slot_count during concurrent access */
    pthread_mutex_t lock;

    /* Stats */
    keel_connpool_stats_t stats;

    /* Housekeeping */
    keel_time_t last_health_check;
};

/* ============================================================================
 * Internal helpers
 * ============================================================================ */

/** Open a non-blocking TCP connection to host:port.
 *  Returns a connected fd ≥ 0, or -1 on failure. */
static int connpool_open_socket(const char* host, int port,
                                uint32_t timeout_ms) {
    struct addrinfo hints = {0};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo* res = NULL;
    if (getaddrinfo(host, port_str, &hints, &res) != 0 || !res) {
        return -1;
    }

    int fd = socket(res->ai_family, res->ai_socktype | SOCK_NONBLOCK, res->ai_protocol);
    if (fd < 0) {
        freeaddrinfo(res);
        return -1;
    }

    int rc = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);

    if (rc == 0) {
        /* Connected immediately (loopback, etc.) */
        return fd;
    }

    if (errno != EINPROGRESS) {
        close(fd);
        return -1;
    }

    /* Wait for connect to complete */
    struct pollfd pfd = { .fd = fd, .events = POLLOUT };
    int ms = (timeout_ms == 0) ? CONNPOOL_DEFAULT_CONNECT_TIMEOUT_MS : (int)timeout_ms;
    rc = poll(&pfd, 1, ms);
    if (rc <= 0) {
        close(fd);
        return -1;
    }

    int so_err = 0;
    socklen_t len = sizeof(so_err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len) != 0 || so_err != 0) {
        close(fd);
        return -1;
    }

    /* Switch back to blocking */
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    }

    return fd;
}

/** Default health probe: zero-byte send. Returns true if the fd is alive. */
static bool connpool_default_probe(keel_conn_t* conn, void* udata) {
    (void)udata;
    if (conn->fd < 0) return false;
    ssize_t r = send(conn->fd, "", 0, MSG_NOSIGNAL);
    return (r == 0);
}

/** Close one slot's socket and reset its state. */
static void connpool_close_slot(keel_connpool_t* pool, keel_conn_t* conn) {
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
        pool->stats.destroys++;
    }
    conn->state          = KEEL_CONN_CLOSED;
    conn->in_transaction = false;
    conn->backend_pid    = 0;
    conn->cancel_secret  = 0;
    conn->userdata       = NULL;
}

/** Try to open a new connection for a CLOSED slot.
 * The caller must set conn->state after this returns KEEL_OK. */
static keel_error_t connpool_open_slot(keel_connpool_t* pool, keel_conn_t* conn) {
    int fd = connpool_open_socket(pool->resolved_host,
                                  pool->resolved_port,
                                  pool->connect_timeout_ms);
    if (fd < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "connpool: failed to connect to %s:%d",
                      pool->resolved_host, pool->resolved_port);
        return KEEL_ERR_CONNECT;
    }

    keel_time_t now = keel_time_now();
    conn->fd             = fd;
    conn->created_at_ns  = (uint64_t)now;
    conn->last_used_ns   = (uint64_t)now;
    conn->in_transaction = false;
    conn->backend_pid    = 0;
    conn->cancel_secret  = 0;
    conn->userdata       = NULL;

    pthread_mutex_lock(&pool->lock);
    pool->stats.creates++;
    pthread_mutex_unlock(&pool->lock);
    return KEEL_OK;
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

keel_connpool_t* keel_connpool_create(const keel_route_server_t* server,
                                      const keel_connpool_config_t* config) {
    if (!server || !server->host || server->port == 0) return NULL;

    keel_connpool_t* pool = keel_calloc(1, sizeof(keel_connpool_t));
    if (!pool) return NULL;

    pool->server = server;
    strncpy(pool->resolved_host, server->host, sizeof(pool->resolved_host) - 1);
    pool->resolved_port = server->port;

    /* Apply config with defaults */
    if (config) {
        pool->min_conns                = config->min_conns;
        pool->max_conns                = config->max_conns    ? config->max_conns
                                                              : CONNPOOL_DEFAULT_MAX_CONNS;
        pool->idle_timeout_ms          = config->idle_timeout_ms;
        pool->connect_timeout_ms       = config->connect_timeout_ms ? config->connect_timeout_ms
                                                                     : CONNPOOL_DEFAULT_CONNECT_TIMEOUT_MS;
        pool->acquire_timeout_ms       = config->acquire_timeout_ms ? config->acquire_timeout_ms
                                                                     : CONNPOOL_DEFAULT_ACQUIRE_TIMEOUT_MS;
        pool->health_check_interval_ms = config->health_check_interval_ms
                                          ? config->health_check_interval_ms
                                          : CONNPOOL_DEFAULT_HEALTH_INTERVAL_MS;
        pool->health_probe             = config->health_probe;
        pool->health_probe_udata       = config->health_probe_udata;
    } else {
        pool->max_conns                = CONNPOOL_DEFAULT_MAX_CONNS;
        pool->connect_timeout_ms       = CONNPOOL_DEFAULT_CONNECT_TIMEOUT_MS;
        pool->acquire_timeout_ms       = CONNPOOL_DEFAULT_ACQUIRE_TIMEOUT_MS;
        pool->health_check_interval_ms = CONNPOOL_DEFAULT_HEALTH_INTERVAL_MS;
    }

    if (pool->max_conns > KEEL_CONNPOOL_MAX_CONNS) {
        pool->max_conns = KEEL_CONNPOOL_MAX_CONNS;
    }
    pool->slot_count = pool->max_conns;

    /* Initialize all slots as CLOSED */
    for (size_t i = 0; i < pool->slot_count; i++) {
        pool->slots[i].fd    = -1;
        pool->slots[i].state = KEEL_CONN_CLOSED;
    }

    pthread_mutex_init(&pool->lock, NULL);
    pool->last_health_check = keel_time_now();
    return pool;
}

void keel_connpool_destroy(keel_connpool_t* pool) {
    if (!pool) return;
    for (size_t i = 0; i < pool->slot_count; i++) {
        connpool_close_slot(pool, &pool->slots[i]);
    }
    pthread_mutex_destroy(&pool->lock);
    keel_free(pool);
}

/* ============================================================================
 * Acquire / release
 * ============================================================================ */

keel_error_t keel_connpool_acquire(keel_connpool_t* pool, keel_conn_t** conn_out) {
    if (!pool || !conn_out) return KEEL_ERR_INVALID_ARG;

    keel_time_t deadline = keel_time_add(keel_time_now(),
                                          KEEL_MSEC(pool->acquire_timeout_ms));

    for (;;) {
        pthread_mutex_lock(&pool->lock);

        /* 1. Look for an existing idle slot */
        for (size_t i = 0; i < pool->slot_count; i++) {
            if (pool->slots[i].state == KEEL_CONN_IDLE) {
                pool->slots[i].state = KEEL_CONN_ACTIVE;
                pool->stats.borrows++;
                pool->stats.hits++;
                *conn_out = &pool->slots[i];
                pthread_mutex_unlock(&pool->lock);
                return KEEL_OK;
            }
        }

        /* 2. Look for a free (CLOSED) slot to open */
        for (size_t i = 0; i < pool->slot_count; i++) {
            if (pool->slots[i].state == KEEL_CONN_CLOSED) {
                /* Reserve the slot under the lock before connecting */
                pool->slots[i].state = KEEL_CONN_ACTIVE;
                pthread_mutex_unlock(&pool->lock);
                keel_error_t err = connpool_open_slot(pool, &pool->slots[i]);
                if (err != KEEL_OK) {
                    pthread_mutex_lock(&pool->lock);
                    pool->slots[i].state = KEEL_CONN_CLOSED;
                    pthread_mutex_unlock(&pool->lock);
                    return err;
                }
                pthread_mutex_lock(&pool->lock);
                pool->stats.borrows++;
                pool->stats.misses++;
                *conn_out = &pool->slots[i];
                pthread_mutex_unlock(&pool->lock);
                return KEEL_OK;
            }
        }

        /* 3. All slots active — check deadline */
        if (keel_time_after(keel_time_now(), deadline)) {
            pool->stats.timeouts++;
            pthread_mutex_unlock(&pool->lock);
            return KEEL_ERR_POOL_TIMEOUT;
        }

        pthread_mutex_unlock(&pool->lock);

        /* 4. Brief sleep outside the lock */
        struct timespec ts = { .tv_sec = 0, .tv_nsec = CONNPOOL_ACQUIRE_SPIN_SLEEP_US * 1000 };
        nanosleep(&ts, NULL);
    }
}

void keel_connpool_release(keel_connpool_t* pool, keel_conn_t* conn, bool reusable) {
    if (!pool || !conn) return;

    pthread_mutex_lock(&pool->lock);
    pool->stats.returns++;

    if (!reusable || conn->fd < 0 || conn->in_transaction) {
        connpool_close_slot(pool, conn);
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    conn->state        = KEEL_CONN_IDLE;
    conn->last_used_ns = (uint64_t)keel_time_now();
    pthread_mutex_unlock(&pool->lock);
}

/* ============================================================================
 * Housekeeping
 * ============================================================================ */

size_t keel_connpool_evict_idle(keel_connpool_t* pool) {
    if (!pool || pool->idle_timeout_ms == 0) return 0;

    keel_time_t now     = keel_time_now();
    keel_duration_t threshold = KEEL_MSEC(pool->idle_timeout_ms);
    size_t evicted = 0;

    /* Respect min_conns floor */
    size_t idle_count = 0;
    for (size_t i = 0; i < pool->slot_count; i++) {
        if (pool->slots[i].state == KEEL_CONN_IDLE) idle_count++;
    }

    for (size_t i = 0; i < pool->slot_count; i++) {
        keel_conn_t* c = &pool->slots[i];
        if (c->state != KEEL_CONN_IDLE) continue;
        if (idle_count <= pool->min_conns) break;

        keel_duration_t age = keel_time_diff((keel_time_t)c->last_used_ns, now);
        if (age >= threshold) {
            connpool_close_slot(pool, c);
            pool->stats.idle_evicts++;
            evicted++;
            idle_count--;
        }
    }
    return evicted;
}

size_t keel_connpool_health_check(keel_connpool_t* pool) {
    if (!pool) return 0;

    bool (*probe)(keel_conn_t*, void*) = pool->health_probe
                                           ? pool->health_probe
                                           : connpool_default_probe;
    size_t evicted = 0;

    for (size_t i = 0; i < pool->slot_count; i++) {
        keel_conn_t* c = &pool->slots[i];
        if (c->state != KEEL_CONN_IDLE) continue;

        if (!probe(c, pool->health_probe_udata)) {
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                           "connpool: health probe failed for %s:%d slot %zu; evicting",
                           pool->resolved_host, pool->resolved_port, i);
            connpool_close_slot(pool, c);
            pool->stats.health_evicts++;
            evicted++;
        }
    }

    pool->last_health_check = keel_time_now();
    return evicted;
}

int keel_connpool_warm(keel_connpool_t* pool) {
    if (!pool) return -1;

    size_t opened = 0;
    for (size_t i = 0; i < pool->slot_count && opened < pool->min_conns; i++) {
        keel_conn_t* c = &pool->slots[i];
        /* Count all non-CLOSED as already "open" */
        if (c->state != KEEL_CONN_CLOSED) {
            opened++;
            continue;
        }
        if (connpool_open_slot(pool, c) == KEEL_OK) {
                c->state = KEEL_CONN_IDLE;
                opened++;
        }
    }
    return (int)opened;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

void keel_connpool_get_stats(const keel_connpool_t* pool,
                              keel_connpool_stats_t* stats) {
    if (!pool || !stats) return;
    pthread_mutex_lock(&((keel_connpool_t*)pool)->lock);
    *stats = pool->stats;

    /* Recompute live gauges */
    stats->active = 0;
    stats->idle   = 0;
    stats->total  = 0;
    for (size_t i = 0; i < pool->slot_count; i++) {
        switch (pool->slots[i].state) {
        case KEEL_CONN_IDLE:   stats->idle++;  stats->total++; break;
        case KEEL_CONN_ACTIVE: stats->active++; stats->total++; break;
        default: break;
        }
    }
    pthread_mutex_unlock(&((keel_connpool_t*)pool)->lock);
}

/* ============================================================================
 * Registry
 * ============================================================================ */

struct keel_connpool_registry {
    keel_connpool_config_t  default_config;
    bool                    has_default;
    /* Parallel arrays keyed by server->name */
    char*            names[KEEL_CONNPOOL_MAX_SERVERS];
    keel_connpool_t* pools[KEEL_CONNPOOL_MAX_SERVERS];
    size_t           count;
};

keel_connpool_registry_t* keel_connpool_registry_create(
    const keel_connpool_config_t* default_config) {
    keel_connpool_registry_t* reg = keel_calloc(1, sizeof(keel_connpool_registry_t));
    if (!reg) return NULL;
    if (default_config) {
        reg->default_config = *default_config;
        reg->has_default    = true;
    }
    return reg;
}

void keel_connpool_registry_destroy(keel_connpool_registry_t* reg) {
    if (!reg) return;
    for (size_t i = 0; i < reg->count; i++) {
        keel_connpool_destroy(reg->pools[i]);
        keel_free(reg->names[i]);
    }
    keel_free(reg);
}

keel_connpool_t* keel_connpool_registry_get(keel_connpool_registry_t* reg,
                                             const keel_route_server_t* server) {
    if (!reg || !server || !server->name) return NULL;

    /* Linear scan — registry is small (≤ KEEL_CONNPOOL_MAX_SERVERS) */
    for (size_t i = 0; i < reg->count; i++) {
        if (strcmp(reg->names[i], server->name) == 0) {
            return reg->pools[i];
        }
    }

    /* Not found — create a new pool */
    if (reg->count >= KEEL_CONNPOOL_MAX_SERVERS) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
                       "connpool_registry: server limit %d reached",
                       KEEL_CONNPOOL_MAX_SERVERS);
        return NULL;
    }

    char* name_copy = keel_strdup(server->name);
    if (!name_copy) return NULL;

    keel_connpool_t* pool = keel_connpool_create(
        server,
        reg->has_default ? &reg->default_config : NULL);
    if (!pool) {
        keel_free(name_copy);
        return NULL;
    }

    reg->names[reg->count] = name_copy;
    reg->pools[reg->count] = pool;
    reg->count++;
    return pool;
}

size_t keel_connpool_registry_evict_idle(keel_connpool_registry_t* reg) {
    if (!reg) return 0;
    size_t total = 0;
    for (size_t i = 0; i < reg->count; i++) {
        total += keel_connpool_evict_idle(reg->pools[i]);
    }
    return total;
}

size_t keel_connpool_registry_health_check(keel_connpool_registry_t* reg) {
    if (!reg) return 0;
    size_t total = 0;
    for (size_t i = 0; i < reg->count; i++) {
        total += keel_connpool_health_check(reg->pools[i]);
    }
    return total;
}

void keel_connpool_registry_get_stats(const keel_connpool_registry_t* reg,
                                       keel_connpool_stats_t* stats) {
    if (!reg || !stats) return;
    memset(stats, 0, sizeof(*stats));
    for (size_t i = 0; i < reg->count; i++) {
        keel_connpool_stats_t s;
        keel_connpool_get_stats(reg->pools[i], &s);
        stats->borrows       += s.borrows;
        stats->returns       += s.returns;
        stats->creates       += s.creates;
        stats->destroys      += s.destroys;
        stats->hits          += s.hits;
        stats->misses        += s.misses;
        stats->timeouts      += s.timeouts;
        stats->health_evicts += s.health_evicts;
        stats->idle_evicts   += s.idle_evicts;
        stats->active        += s.active;
        stats->idle          += s.idle;
        stats->total         += s.total;
    }
}
