/**
 * @file router_discovery.c
 * @brief Cluster discovery, health probing, and failover observation helpers.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file abstracts topology discovery away from the router itself. It can
 * discover cluster membership from SQL probes or control-plane APIs such as
 * Patroni, apply that topology to the router, and optionally run a background
 * thread that keeps the view fresh.
 */

#include "keel/core/router_discovery.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/util.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>     /* struct timeval — needed explicitly on musl libc */
#include <netdb.h>
#include <arpa/inet.h>
#include <errno.h>

/* ============================================================================
 * Discovery Structure
 * ============================================================================ */

/** Maximum number of per-server states tracked for flap detection. */
#define KEEL_DISC_MAX_SERVER_STATES 16

/**
 * @brief Per-server tracking record: role, timeline, and flap counters.
 *
 * Indexed by position in a small flat array and matched by @c name on each
 * topology refresh.  The array is at most KEEL_DISC_MAX_SERVER_STATES entries
 * and is O(n) scanned — topology is small and refreshes are infrequent.
 */
typedef struct keel_disc_server_state {
    char               name[64];       /**< Server identifier (matches keel_server_info.name) */
    keel_server_role_t last_role;       /**< Last confirmed role */
    int                last_timeline;   /**< Last seen WAL timeline (0 = unknown) */
    uint32_t           flap_count;      /**< Total role-change events for this server */
    keel_time_t        last_role_change; /**< Timestamp of most recent role change */
    bool               dampened;        /**< Currently suppressed: flapping too fast */
} keel_disc_server_state_t;

struct keel_discovery {
    keel_discovery_config_t  config;

    /* Background thread */
    pthread_t               thread;
    _Atomic bool            running;
    _Atomic bool            should_stop;

    /* Router to update */
    keel_router_t*           router;

    /* Failover detection */
    keel_failover_callback_fn failover_cb;
    void*                   failover_user_data;

    /* Role-change detection */
    keel_role_change_callback_fn role_change_cb;
    void*                       role_change_user_data;

    /* Per-server state (for flap tracking + timeline) */
    keel_disc_server_state_t server_states[KEEL_DISC_MAX_SERVER_STATES];
    size_t                   server_state_count;

    /* Current primary timeline (for failover events) */
    int                     primary_timeline;

    /* Last known topology */
    char*                   last_primary;
    keel_time_t              last_refresh;

    /* Thread synchronization */
    pthread_mutex_t         mutex;
    pthread_cond_t          cond;
};

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @brief Return the default discovery configuration.
 *
 * @return Default discovery configuration.
 */
keel_discovery_config_t keel_discovery_config_default(void) {
    return (keel_discovery_config_t){
        .method         = KEEL_DISCOVER_SQL,
        .probe_fn       = NULL,          /* set by caller (e.g. keel_pg_discovery_probe) */
        .probe_timeout  = 5 * 1000000000ULL,      /* 5 seconds */
        .probe_retries  = 3,
        .probe_interval = 10 * 1000000000ULL,    /* 10 seconds */
        .max_lag_bytes  = 100 * 1024 * 1024,      /* 100 MB */
        .max_lag_seconds = 30.0,
        .patroni_url    = NULL,
        .cluster_name   = NULL,
        .monitor_connstr = NULL,
        .formation      = NULL,
        .service_name   = NULL,
        .consul_url     = NULL,
        .etcd_endpoints = NULL,
        /* Flap dampening: suppress if > 3 role changes within 30 s */
        .flap_dampening_window_s   = 30,
        .flap_dampening_threshold  = 3,
    };
}

/* ============================================================================
 * Per-server state helpers (flap tracking + timeline)
 * ============================================================================ */

/**
 * @brief Locate or create the per-server state record for @p name.
 *
 * Returns NULL only when the server-state table is full and @p name is new.
 * This is a linear scan; the table is at most KEEL_DISC_MAX_SERVER_STATES.
 */
static keel_disc_server_state_t*
disc_get_server_state(keel_discovery_t* disc, const char* name)
{
    for (size_t i = 0; i < disc->server_state_count; i++) {
        if (strcmp(disc->server_states[i].name, name) == 0)
            return &disc->server_states[i];
    }
    if (disc->server_state_count >= KEEL_DISC_MAX_SERVER_STATES)
        return NULL;
    keel_disc_server_state_t* s = &disc->server_states[disc->server_state_count++];
    memset(s, 0, sizeof(*s));
    strncpy(s->name, name, sizeof(s->name) - 1);
    s->last_role     = KEEL_SERVER_ROLE_AUTO;  /* unknown until first probe */
    s->last_timeline = 0;
    return s;
}

/**
 * @brief Evaluate whether @p s is currently flapping and update the dampened flag.
 *
 * Flapping is defined as @c flap_count role changes within the configured
 * @c flap_dampening_window_s.  Once dampened, a server remains dampened until
 * no role change is observed for a full window.
 *
 * @return true if the new role change should be suppressed.
 */
static bool
disc_is_flapping(keel_discovery_t* disc, keel_disc_server_state_t* s,
                 keel_time_t now_ns)
{
    uint32_t window_s = disc->config.flap_dampening_window_s;
    uint32_t threshold = disc->config.flap_dampening_threshold;
    if (window_s == 0 || threshold == 0)
        return false;  /* dampening disabled */

    uint64_t window_ns = (uint64_t)window_s * 1000000000ULL;
    if (s->last_role_change > 0 && (now_ns - s->last_role_change) > window_ns) {
        /* Outside window — reset dampening */
        s->dampened = false;
    }
    if (s->flap_count >= threshold &&
        s->last_role_change > 0 &&
        (now_ns - s->last_role_change) <= window_ns) {
        s->dampened = true;
    }
    return s->dampened;
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Create a discovery instance.
 *
 * @param config Optional discovery configuration.
 * @return Discovery instance on success, or `NULL` on allocation/init failure.
 */
keel_discovery_t* keel_discovery_create(const keel_discovery_config_t* config) {
    keel_discovery_t* disc = keel_calloc(1, sizeof(*disc));
    if (!disc) {
        return NULL;
    }
    
    if (config) {
        disc->config = *config;
    } else {
        disc->config = keel_discovery_config_default();
    }
    
    if (pthread_mutex_init(&disc->mutex, NULL) != 0) {
        keel_free(disc);
        return NULL;
    }
    
    if (pthread_cond_init(&disc->cond, NULL) != 0) {
        pthread_mutex_destroy(&disc->mutex);
        keel_free(disc);
        return NULL;
    }
    
    const char* method_str = "SQL";
    switch (disc->config.method) {
        case KEEL_DISCOVER_PATRONI: method_str = "Patroni"; break;
        case KEEL_DISCOVER_PGAUTOFAILOVER: method_str = "pg_auto_failover"; break;
        case KEEL_DISCOVER_CONSUL: method_str = "Consul"; break;
        case KEEL_DISCOVER_ETCD: method_str = "etcd"; break;
        default: break;
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Created discovery instance (method=%s, interval=%lums)",
                 method_str,
                 (unsigned long)(disc->config.probe_interval / 1000000));
    
    return disc;
}

/**
 * @brief Destroy a discovery instance and stop its background thread if needed.
 *
 * @param disc Discovery handle, or `NULL`.
 * @return
 */
void keel_discovery_destroy(keel_discovery_t* disc) {
    if (!disc) return;
    
    /* Stop background thread */
    keel_discovery_stop(disc);
    
    pthread_cond_destroy(&disc->cond);
    pthread_mutex_destroy(&disc->mutex);
    keel_free(disc->last_primary);
    keel_free(disc);
}

/* ============================================================================
 * Probing — delegates entirely to the configured probe_fn callback
 * ============================================================================ */

/**
 * @brief Probe one server endpoint and fill a server-info record.
 *
 * This function is intentionally database-agnostic.  All protocol-specific
 * work is performed by @p disc->config.probe_fn, which is provided by the
 * probe layer at configuration time.  When no probe function is configured
 * the server health is reported as @c KEEL_HEALTH_UNKNOWN.
 */
keel_error_t keel_discovery_probe(
    keel_discovery_t* disc,
    const char* host,
    uint16_t port,
    const char* user,
    const char* pass,
    const char* dbname,
    keel_server_info_t* info
) {
    if (!disc || !host || !info) return KEEL_ERR_INVALID_ARG;

    memset(info, 0, sizeof(*info));
    strncpy(info->host, host, sizeof(info->host) - 1);
    info->port       = port;
    info->probe_time = keel_time_now();

    if (!disc->config.probe_fn) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                       "Probe %s:%d skipped: no probe_fn configured", host, port);
        info->health = KEEL_HEALTH_UNKNOWN;
        return KEEL_OK;
    }

    uint32_t timeout_ms = (uint32_t)(disc->config.probe_timeout / 1000000);
    if (timeout_ms == 0) timeout_ms = 5000;

    keel_discovery_probe_params_t params = {
        .timeout_ms      = timeout_ms,
        .max_lag_bytes   = disc->config.max_lag_bytes,
        .max_lag_seconds = disc->config.max_lag_seconds,
    };

    keel_error_t err = disc->config.probe_fn(host, port, user, pass, dbname,
                                              &params, info);
    info->response_time = keel_time_now() - info->probe_time;
    return err;
}

/**
 * @brief Probe server state through an existing connection handle.
 *
 * In-pool probing requires access to the connection pool's internal fd/state
 * which belongs to a higher-level subsystem.  Implementations are injected
 * from outside the core; this stub returns UNKNOWN until one is wired in.
 */
keel_error_t keel_discovery_probe_conn(
    keel_discovery_t* disc,
    void* conn,
    keel_server_info_t* info
) {
    if (!disc || !conn || !info) return KEEL_ERR_INVALID_ARG;

    memset(info, 0, sizeof(*info));
    info->probe_time = keel_time_now();

    if (disc->config.conn_probe_fn) {
        keel_discovery_probe_params_t params = {
            .timeout_ms      = (uint32_t)(disc->config.probe_timeout / 1000000),
            .max_lag_bytes   = disc->config.max_lag_bytes,
            .max_lag_seconds = disc->config.max_lag_seconds,
        };
        return disc->config.conn_probe_fn(conn, &params, info);
    }

    /* No in-pool probe callback registered — health stays UNKNOWN. */
    info->health = KEEL_HEALTH_UNKNOWN;
    return KEEL_OK;
}

/* ============================================================================
 * Patroni REST API HTTP Client
 * ============================================================================
 *
 * Implements a minimal blocking HTTP/1.0 GET client (no external dependencies).
 * Patroni exposes cluster topology via:
 *   GET /cluster  -> { "members": [...], ... }
 *   GET /patroni  -> single-node info, optionally with "members": [...]
 */

#define PATRONI_DEFAULT_PORT   8008
#define PATRONI_RECV_TIMEOUT   5        /* connect/recv timeout (seconds) */
#define PATRONI_BUF_SIZE      (32*1024) /* max response buffer */

/**
 * Parse Patroni URL: "http://host:port/path", "host:port", "host:port/path"
 * Fills host_out, *port_out (default PATRONI_DEFAULT_PORT), path_out.
 */
static void patroni_parse_url(const char* url,
                               char* host_out, size_t host_sz,
                               int*  port_out,
                               char* path_out, size_t path_sz) {
    const char* p = url;
    const char* sch = strstr(p, "://");
    if (sch) p = sch + 3;

    const char* slash = strchr(p, '/');
    if (slash) {
        strncpy(path_out, slash, path_sz - 1);
        path_out[path_sz - 1] = '\0';
    } else {
        path_out[0] = '\0';
    }

    size_t hplen = slash ? (size_t)(slash - p) : strlen(p);
    char hp[300];
    if (hplen >= sizeof(hp)) hplen = sizeof(hp) - 1;
    memcpy(hp, p, hplen);
    hp[hplen] = '\0';

    if (hp[0] == '[') { /* IPv6 literal */
        const char* close = strchr(hp, ']');
        if (close) {
            size_t l = (size_t)(close - hp - 1);
            if (l >= host_sz) l = host_sz - 1;
            memcpy(host_out, hp + 1, l);
            host_out[l] = '\0';
            *port_out = (close[1] == ':') ? atoi(close + 2) : PATRONI_DEFAULT_PORT;
        }
        return;
    }

    const char* colon = strchr(hp, ':');
    if (colon) {
        size_t l = (size_t)(colon - hp);
        if (l >= host_sz) l = host_sz - 1;
        memcpy(host_out, hp, l);
        host_out[l] = '\0';
        *port_out = atoi(colon + 1);
    } else {
        strncpy(host_out, hp, host_sz - 1);
        host_out[host_sz - 1] = '\0';
        *port_out = PATRONI_DEFAULT_PORT;
    }
}

/**
 * @brief Execute a minimal blocking HTTP/1.0 GET request.
 *
 * @param host Remote host.
 * @param port Remote port.
 * @param path Request path.
 * @param[out] buf Response buffer receiving the raw HTTP response.
 * @param bufsz Response buffer size.
 * @param[out] body_out Pointer to the first byte after the HTTP headers.
 * @return Response body length on success, or `-1` on resolution/socket/protocol failure.
 */
static ssize_t patroni_http_get(const char* host, int port, const char* path,
                                 char* buf, size_t bufsz,
                                 const char** body_out) {
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    if (getaddrinfo(host, portstr, &hints, &res) != 0 || !res)
        return -1;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return -1; }

    struct timeval tv = { PATRONI_RECV_TIMEOUT, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        freeaddrinfo(res); close(fd); return -1;
    }
    freeaddrinfo(res);

    char req[640];
    int reqlen = snprintf(req, sizeof(req),
        "GET %s HTTP/1.0\r\n"
        "Host: %s:%d\r\n"
        "Accept: application/json\r\n"
        "Connection: close\r\n"
        "\r\n",
        (path && path[0]) ? path : "/", host, port);

    if (send(fd, req, (size_t)reqlen, 0) != (ssize_t)reqlen) {
        close(fd); return -1;
    }

    size_t total = 0;
    ssize_t n;
    while (total < bufsz - 1 &&
           (n = recv(fd, buf + total, bufsz - 1 - total, 0)) > 0) {
        total += (size_t)n;
    }
    close(fd);
    buf[total] = '\0';

    const char* hend = strstr(buf, "\r\n\r\n");
    if (!hend) return -1;

    *body_out = hend + 4;
    return (ssize_t)(total - (size_t)(*body_out - buf));
}

/* ---- Tiny JSON helpers (no external parser) ---- */

/**
 * @brief Find the raw JSON value pointer immediately after a named key.
 */
static const char* json_val(const char* json, const char* key) {
    const char* p = strstr(json, key);
    if (!p) return NULL;
    p += strlen(key);
    while (*p == ' ' || *p == '\t') p++;
    if (*p != ':') return NULL;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    return p;
}

/**
 * @brief Extract a simple JSON string field into caller storage.
 */
static bool json_str(const char* obj, const char* key,
                     char* buf, size_t sz) {
    const char* v = json_val(obj, key);
    if (!v || *v != '"') return false;
    v++;
    size_t i = 0;
    while (*v && *v != '"' && i < sz - 1) buf[i++] = *v++;
    buf[i] = '\0';
    return true;
}

/**
 * @brief Extract a simple JSON integer field.
 */
static bool json_int_val(const char* obj, const char* key, int* out) {
    const char* v = json_val(obj, key);
    if (!v) return false;
    char* end;
    long val = strtol(v, &end, 10);
    if (end == v) return false;
    *out = (int)val;
    return true;
}

/**
 * Parse Patroni JSON into keel_cluster_topology_t.
 *
 * Handles:
 *  /cluster  -> { "members": [ {name,host,port,role,state}, ... ], ... }
 *  /patroni  -> { "role":"master","state":"running","name":"n1","host":"h1",
 *                 "port":5432, "members":[...] }
 *
 * Primary role strings: "master" (Patroni v2), "leader" (Patroni v3),
 *                       "primary" (pg_auto_failover compat)
 */
/**
 * @brief Parse Patroni JSON into a KEEL topology structure.
 *
 * @return `KEEL_OK` on success, or a memory error when allocation fails.
 */
static keel_error_t patroni_parse_topology(const char* json,
                                            size_t json_len,
                                            keel_cluster_topology_t* topo) {
    (void)json_len;

    size_t cap = 8;
    topo->servers = keel_calloc(cap, sizeof(keel_server_info_t));
    if (!topo->servers) return KEEL_ERR_NOMEM;
    topo->server_count  = 0;
    topo->primary_index = (size_t)-1;

    const char* members_kw = strstr(json, "\"members\"");
    if (!members_kw) {
        /* Single-node /patroni response with no members array */
        keel_server_info_t* srv = &topo->servers[0];
        char role[32] = {0}, state[32] = {0};
        json_str(json, "\"role\"",  role,      sizeof(role));
        json_str(json, "\"state\"", state,     sizeof(state));
        json_str(json, "\"name\"",  srv->name, sizeof(srv->name));
        json_str(json, "\"host\"",  srv->host, sizeof(srv->host));
        int pv = 5432; json_int_val(json, "\"port\"", &pv);
        srv->port       = (uint16_t)pv;
        srv->is_primary = (strncmp(role, "master",  6) == 0 ||
                           strncmp(role, "primary", 7) == 0 ||
                           strncmp(role, "leader",  6) == 0);
        srv->health = (strncmp(state, "running", 7) == 0)
                      ? KEEL_HEALTH_UP : KEEL_HEALTH_DOWN;
        if (!srv->name[0])
            strncpy(srv->name, "node0", sizeof(srv->name) - 1);
        topo->server_count  = 1;
        if (srv->is_primary) topo->primary_index = 0;
        return KEEL_OK;
    }

    /* Walk the members JSON array */
    const char* p = strchr(members_kw, '[');
    if (!p) return KEEL_OK;
    p++;

    while (*p) {
        while (*p && *p != '{' && *p != ']') p++;
        if (!*p || *p == ']') break;

        const char* obj_start = p++;
        int depth = 1;
        while (*p && depth > 0) {
            if      (*p == '{') depth++;
            else if (*p == '}') depth--;
            p++;
        }

        size_t obj_len = (size_t)(p - obj_start);
        char obj[640];
        if (obj_len >= sizeof(obj)) obj_len = sizeof(obj) - 1;
        memcpy(obj, obj_start, obj_len);
        obj[obj_len] = '\0';

        /* Grow array if needed */
        if (topo->server_count >= cap) {
            cap *= 2;
            keel_server_info_t* tmp = keel_realloc(
                topo->servers, cap * sizeof(keel_server_info_t));
            if (!tmp) return KEEL_ERR_NOMEM;
            topo->servers = tmp;
        }

        keel_server_info_t* srv = &topo->servers[topo->server_count];
        memset(srv, 0, sizeof(*srv));

        char role[32] = {0}, state[32] = {0};
        json_str(obj, "\"name\"",  srv->name, sizeof(srv->name));
        json_str(obj, "\"host\"",  srv->host, sizeof(srv->host));
        json_str(obj, "\"role\"",  role,      sizeof(role));
        json_str(obj, "\"state\"", state,     sizeof(state));
        int pv = 5432; json_int_val(obj, "\"port\"", &pv);
        srv->port       = (uint16_t)pv;
        srv->is_primary = (strncmp(role, "master",    6) == 0 ||
                           strncmp(role, "primary",   7) == 0 ||
                           strncmp(role, "leader",    6) == 0);
        srv->health = (strncmp(state, "running",   7) == 0 ||
                       strncmp(state, "streaming", 9) == 0)
                      ? KEEL_HEALTH_UP : KEEL_HEALTH_DOWN;

        if (srv->is_primary)
            topo->primary_index = topo->server_count;

        topo->server_count++;
    }

    return KEEL_OK;
}

/**
 * Contacts Patroni REST API and returns populated topology.
 * Tries /cluster first, then falls back to /patroni.
 */
/**
 * @brief Query Patroni endpoints and return the discovered topology.
 */
keel_error_t patroni_discover(const char* base_url,
                              keel_cluster_topology_t* topo) {
    char host[256] = {0};
    int  port      = PATRONI_DEFAULT_PORT;
    char base_path[512] = {0};
    patroni_parse_url(base_url, host, sizeof(host), &port,
                      base_path, sizeof(base_path));

    /* Trim trailing slash */
    size_t bplen = strlen(base_path);
    if (bplen > 0 && base_path[bplen - 1] == '/')
        base_path[--bplen] = '\0';

    char buf[PATRONI_BUF_SIZE];
    const char* body = NULL;
    ssize_t body_len;

    char path[600];
    snprintf(path, sizeof(path), "%s/cluster", base_path);
    body_len = patroni_http_get(host, port, path, buf, sizeof(buf), &body);

    if (body_len < 2 || !body) {
        snprintf(path, sizeof(path), "%s/patroni", base_path);
        body_len = patroni_http_get(host, port, path, buf, sizeof(buf), &body);
    }

    if (body_len < 2 || !body) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                      "Patroni: could not reach API at %s (host=%s port=%d)",
                      base_url, host, port);
        return KEEL_ERR_IO;
    }

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                   "Patroni: received %zd bytes from %s", body_len, base_url);

    return patroni_parse_topology(body, (size_t)body_len, topo);
}

/* ============================================================================
 * Cluster Discovery
 * ============================================================================ */

/**
 * @brief Refresh cluster topology according to the configured discovery method.
 */
keel_error_t keel_discovery_refresh(
    keel_discovery_t* disc,
    keel_cluster_topology_t** topology
) {
    if (!disc || !topology) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    *topology = NULL;
    
    switch (disc->config.method) {
        case KEEL_DISCOVER_SQL:
            /* For SQL discovery, we need to probe all known servers
             * This requires the router to already have servers configured */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL, 
                          "SQL discovery: probe configured servers");
            break;
            
        case KEEL_DISCOVER_PATRONI: {
            /* Query Patroni REST API to discover cluster topology */
            if (!disc->config.patroni_url) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                              "Patroni discovery: no URL configured");
                return KEEL_ERR_INVALID_ARG;
            }

            keel_cluster_topology_t* topo = keel_calloc(1, sizeof(*topo));
            if (!topo) return KEEL_ERR_NOMEM;

            disc->last_refresh  = keel_time_now();
            topo->discovered_at = disc->last_refresh;
            topo->cluster_name  = disc->config.cluster_name;

            keel_error_t perr = patroni_discover(disc->config.patroni_url, topo);
            if (perr != KEEL_OK) {
                keel_topology_free(topo);
                return perr;
            }

            KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                          "Patroni: discovered %zu server(s), primary_index=%zu",
                          topo->server_count, topo->primary_index);

            *topology = topo;
            return KEEL_OK;
        }
            
        case KEEL_DISCOVER_PGAUTOFAILOVER:
            /* Query pg_auto_failover monitor */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                          "pg_auto_failover discovery: %s",
                          disc->config.monitor_connstr ? disc->config.monitor_connstr : "(not configured)");
            break;
            
        case KEEL_DISCOVER_CONSUL:
            /* Query Consul service registry */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                          "Consul discovery: %s/%s",
                          disc->config.consul_url ? disc->config.consul_url : "(not configured)",
                          disc->config.service_name ? disc->config.service_name : "(no service)");
            break;
            
        case KEEL_DISCOVER_ETCD:
            /* Query etcd */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                          "etcd discovery: %s",
                          disc->config.etcd_endpoints ? disc->config.etcd_endpoints : "(not configured)");
            break;
    }
    
    disc->last_refresh = keel_time_now();
    
    /* Placeholder topology - real implementation would populate this */
    keel_cluster_topology_t* topo = keel_calloc(1, sizeof(*topo));
    if (!topo) {
        return KEEL_ERR_NOMEM;
    }
    
    topo->primary_index = (size_t)-1;
    topo->discovered_at = disc->last_refresh;
    
    *topology = topo;
    return KEEL_OK;
}

/**
 * @brief Free a topology snapshot and its owned server array.
 *
 * @param topology Topology snapshot, or `NULL`.
 * @return
 */
void keel_topology_free(keel_cluster_topology_t* topology) {
    if (!topology) return;
    keel_free(topology->servers);
    keel_free(topology);
}

/**
 * @brief Apply a discovered topology snapshot to the router.
 *
 * @return `KEEL_OK` on success, or an error for invalid input.
 */
keel_error_t keel_discovery_apply(
    keel_discovery_t* disc,
    keel_router_t* router,
    const keel_cluster_topology_t* topology
) {
    if (!disc || !router || !topology) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    /*
     * Apply topology changes to router:
     * 1. Mark servers not in topology as DOWN
     * 2. Add new servers from topology
     * 3. Update roles (primary/replica)
     * 4. Update health based on lag
     */
    
    for (size_t i = 0; i < topology->server_count; i++) {
        const keel_server_info_t* srv = &topology->servers[i];
        
        /* Check if server exists in router */
        keel_route_server_t* existing = keel_router_get_server(router, srv->name);
        
        if (existing) {
            /* Update health */
            keel_server_health_t health = KEEL_HEALTH_UP;
            
            if (srv->health == KEEL_HEALTH_DOWN) {
                health = KEEL_HEALTH_DOWN;
            } else if (!srv->is_primary && 
                       (srv->lag_bytes > disc->config.max_lag_bytes ||
                        srv->lag_seconds > disc->config.max_lag_seconds)) {
                health = KEEL_HEALTH_DEGRADED;
            }
            
            keel_router_set_server_health(router, srv->name, health);
            existing->timeline_id = (uint32_t)(srv->timeline > 0 ? srv->timeline : 0);

            /* Update role if changed (e.g. after a failover / promotion). */
            {
                keel_route_server_t* rsrv = keel_router_get_server(router, srv->name);
                if (rsrv) {
                    keel_server_role_t new_role = srv->is_primary
                        ? KEEL_SERVER_PRIMARY : KEEL_SERVER_REPLICA;
                    if (rsrv->role != new_role) {
                        keel_time_t now = keel_time_now();

                        /* Per-server flap tracking */
                        keel_disc_server_state_t* ss =
                            disc_get_server_state(disc, srv->name);
                        int old_tl = ss ? ss->last_timeline : 0;
                        int new_tl = srv->timeline;
                        bool dampened = false;

                        if (ss) {
                            ss->flap_count++;
                            dampened = disc_is_flapping(disc, ss, now);
                            ss->last_role_change = now;
                            ss->last_role        = new_role;
                            ss->last_timeline    = new_tl;
                        }

                        /* Always emit a structured log — dampened ones are
                         * logged at DEBUG so they don't flood the operator. */
                        keel_server_role_t old_role = rsrv->role;
                        if (dampened) {
                            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                                "role-change suppressed (flapping): "
                                "server='%s' old_role=%d new_role=%d "
                                "timeline=%d->%d flap_count=%u",
                                srv->name,
                                (int)rsrv->role, (int)new_role,
                                old_tl, new_tl,
                                ss ? ss->flap_count : 0);
                        } else {
                            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                                "role-change: server='%s' old_role=%d new_role=%d "
                                "timeline=%d->%d flap_count=%u",
                                srv->name,
                                (int)rsrv->role, (int)new_role,
                                old_tl, new_tl,
                                ss ? ss->flap_count : 0);
                            rsrv->role = new_role;
                        }

                        /* Invoke role-change callback (dampened events too,
                         * with event->dampened=true). */
                        if (disc->role_change_cb) {
                            keel_role_change_event_t ev = {
                                .old_role     = old_role,
                                .new_role     = new_role,
                                .old_timeline = old_tl,
                                .new_timeline = new_tl,
                                .changed_at   = now,
                                .flap_count   = ss ? ss->flap_count : 0,
                                .dampened     = dampened,
                            };
                            strncpy(ev.server_name, srv->name,
                                    sizeof(ev.server_name) - 1);
                            disc->role_change_cb(
                                disc->role_change_user_data, &ev);
                        }
                    } else {
                        /* Role unchanged — still update timeline cache */
                        keel_disc_server_state_t* ss =
                            disc_get_server_state(disc, srv->name);
                        if (ss)
                            ss->last_timeline = srv->timeline;
                    }
                }
            }
        } else {
            /* Add new server */
            keel_route_server_t new_srv = {
                .name = srv->name,
                .host = srv->host,
                .port = srv->port,
                .role = srv->is_primary ? KEEL_SERVER_PRIMARY : KEEL_SERVER_REPLICA,
                .timeline_id = (uint32_t)(srv->timeline > 0 ? srv->timeline : 0),
                .weight = 100,
                .health = srv->health,
            };
            
            keel_error_t err = keel_router_add_server(router, &new_srv);
            if (err != KEEL_OK && err != KEEL_ERR_ALREADY_EXISTS) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                             "Failed to add server %s: %d", srv->name, err);
            }
        }
    }
    
    /* Check for primary change (failover) */
    if (topology->primary_index != (size_t)-1) {
        const char* new_primary = topology->servers[topology->primary_index].name;
        int new_tl = topology->servers[topology->primary_index].timeline;

        pthread_mutex_lock(&disc->mutex);

        if (disc->last_primary &&
            strcmp(disc->last_primary, new_primary) != 0) {
            int old_tl = disc->primary_timeline;

            /* Emit a structured failover warning with timeline info */
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "FAILOVER: old_primary='%s' new_primary='%s' "
                "old_timeline=%d new_timeline=%d",
                disc->last_primary, new_primary, old_tl, new_tl);

            if (disc->failover_cb) {
                keel_failover_event_t event = {
                    .old_primary  = disc->last_primary,
                    .new_primary  = new_primary,
                    .detected_at  = keel_time_now(),
                    .reason       = "topology change",
                    .old_timeline = old_tl,
                    .new_timeline = new_tl,
                };
                disc->failover_cb(disc->failover_user_data, &event);
            }
        }

        disc->primary_timeline = new_tl;
        keel_free(disc->last_primary);
        disc->last_primary = keel_strdup(new_primary);

        pthread_mutex_unlock(&disc->mutex);

        /* Failover-manager: feed the primary observation through so the
         * router bumps its cluster epoch and fences any prior primary
         * per the configured `failover.old_primary_fencing_required`. */
        keel_router_observe_primary(router, new_primary, (uint32_t)new_tl);
    } else {
        /* No primary in the topology snapshot — signal degraded mode so
         * the router can refuse traffic per [failover] policy. */
        keel_router_observe_primary(router, NULL, 0);
    }

    return KEEL_OK;
}

/* ============================================================================
 * Background Discovery
 * ============================================================================ */

/**
 * @brief Background discovery thread main loop.
 *
 * @param arg Discovery handle.
 * @return `NULL` on thread exit.
 */
static void* discovery_thread(void* arg) {
    keel_discovery_t* disc = arg;
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, "Discovery thread started");
    
    while (!atomic_load_explicit(&disc->should_stop, memory_order_acquire)) {
        /* Refresh topology */
        keel_cluster_topology_t* topology = NULL;
        keel_error_t err = keel_discovery_refresh(disc, &topology);
        
        if (err == KEEL_OK && topology && disc->router) {
            keel_discovery_apply(disc, disc->router, topology);
        }
        
        keel_topology_free(topology);
        
        /* Wait for next interval or stop signal */
        pthread_mutex_lock(&disc->mutex);
        
        if (!atomic_load_explicit(&disc->should_stop, memory_order_relaxed)) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            
            uint64_t interval_ns = disc->config.probe_interval;
            ts.tv_sec += interval_ns / 1000000000ULL;
            ts.tv_nsec += interval_ns % 1000000000ULL;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            
            pthread_cond_timedwait(&disc->cond, &disc->mutex, &ts);
        }
        
        pthread_mutex_unlock(&disc->mutex);
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, "Discovery thread stopped");
    
    atomic_store_explicit(&disc->running, false, memory_order_release);
    return NULL;
}

/**
 * @brief Start the background discovery thread.
 */
keel_error_t keel_discovery_start(
    keel_discovery_t* disc,
    keel_router_t* router
) {
    if (!disc || !router) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    if (atomic_load_explicit(&disc->running, memory_order_acquire)) {
        return KEEL_ERR_ALREADY_INITIALIZED;
    }
    
    disc->router = router;
    atomic_store_explicit(&disc->should_stop, false, memory_order_relaxed);
    atomic_store_explicit(&disc->running,     true,  memory_order_release);
    
    int err = pthread_create(&disc->thread, NULL, discovery_thread, disc);
    if (err != 0) {
        atomic_store_explicit(&disc->running, false, memory_order_relaxed);
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Failed to start discovery thread: %d", err);
        return KEEL_ERR_IO;
    }
    
    return KEEL_OK;
}

/**
 * @brief Stop the background discovery thread if it is running.
 *
 * @param disc Discovery handle.
 * @return
 */
void keel_discovery_stop(keel_discovery_t* disc) {
    if (!disc || !atomic_load_explicit(&disc->running, memory_order_acquire)) {
        return;
    }
    
    pthread_mutex_lock(&disc->mutex);
    atomic_store_explicit(&disc->should_stop, true, memory_order_release);
    pthread_cond_signal(&disc->cond);
    pthread_mutex_unlock(&disc->mutex);
    
    pthread_join(disc->thread, NULL);
}

/**
 * @brief Check whether the background discovery thread is running.
 *
 * @return `true` when discovery is active.
 */
bool keel_discovery_is_running(const keel_discovery_t* disc) {
    return disc && atomic_load_explicit(&disc->running, memory_order_acquire);
}

/* ============================================================================
 * Failover Callback
 * ============================================================================ */

/**
 * @brief Register a failover callback.
 *
 * @param disc Discovery handle.
 * @param callback Callback function.
 * @param user_data Opaque callback context.
 * @return
 */
void keel_discovery_on_failover(
    keel_discovery_t* disc,
    keel_failover_callback_fn callback,
    void* user_data
) {
    if (!disc) return;

    pthread_mutex_lock(&disc->mutex);
    disc->failover_cb = callback;
    disc->failover_user_data = user_data;
    pthread_mutex_unlock(&disc->mutex);
}

/**
 * @brief Register a role-change callback.
 *
 * @param disc      Discovery handle.
 * @param callback  Callback (NULL to deregister).
 * @param user_data Opaque value forwarded to every invocation.
 */
void keel_discovery_on_role_change(
    keel_discovery_t*            disc,
    keel_role_change_callback_fn callback,
    void*                        user_data
) {
    if (!disc) return;

    pthread_mutex_lock(&disc->mutex);
    disc->role_change_cb        = callback;
    disc->role_change_user_data = user_data;
    pthread_mutex_unlock(&disc->mutex);
}
