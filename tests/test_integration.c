/**
 * @file test_integration.c
 * @brief Docker-backed integration harness and minimal PostgreSQL probe client.
 *
 * This file does two distinct jobs for the higher-level test binaries:
 *
 * - manage the lifecycle and discovery of the containerized PostgreSQL test
 *   cluster;
 * - provide a small direct PostgreSQL client capable of startup, authentication,
 *   and simple query execution so tests can validate backend state outside the
 *   proxy under test.
 *
 * Keeping both pieces together avoids duplicating fragile cluster-discovery and
 * wire-protocol bootstrap logic across many integration files.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_integration.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>

/* For MD5 password auth - cross-platform using OpenSSL.
 * Note: MD5 is required for PostgreSQL's MD5 authentication protocol,
 * not for cryptographic security. */
#if defined(__APPLE__)
    /* Use CommonCrypto on macOS - suppress deprecation warnings since
     * MD5 is required for PostgreSQL protocol compatibility */
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wdeprecated-declarations"
    #define COMMON_DIGEST_FOR_OPENSSL
    #include <CommonCrypto/CommonDigest.h>
    #define KEEL_MD5_DIGEST_LENGTH CC_MD5_DIGEST_LENGTH
#else
    /* OpenSSL 3.0+ EVP API (legacy MD5_Init/Update/Final are deprecated) */
    #include <openssl/evp.h>
    #define KEEL_MD5_DIGEST_LENGTH 16
#endif

/* ============================================================================
 * MD5 Password Helper (for PostgreSQL MD5 auth)
 * ============================================================================ */

/**
 * @brief Convert a binary MD5 digest to lowercase hexadecimal.
 *
 * @param digest Input digest bytes.
 * @param[out] out Output buffer with room for 32 hex chars plus terminator.
 * @return
 */
static void md5_to_hex(unsigned char* digest, char* out) {
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < KEEL_MD5_DIGEST_LENGTH; i++) {
        out[i * 2] = hex[(digest[i] >> 4) & 0xf];
        out[i * 2 + 1] = hex[digest[i] & 0xf];
    }
    out[KEEL_MD5_DIGEST_LENGTH * 2] = '\0';
}

/* Platform-abstracted MD5 helper */
/**
 * @brief Hash one or more byte segments with MD5 using the platform backend.
 *
 * MD5 is used here only because PostgreSQL's legacy MD5 authentication protocol
 * still requires it for compatibility; the helper is not meant as a generic
 * security primitive.
 *
 * @param data Array of input segment pointers.
 * @param len Array of segment lengths.
 * @param count Number of segments.
 * @param[out] digest Result digest buffer.
 * @return
 */
static void compute_md5(const void* data[], const size_t len[], size_t count,
                        unsigned char digest[/*KEEL_MD5_DIGEST_LENGTH*/]) {
#if defined(__APPLE__)
    CC_MD5_CTX ctx;
    CC_MD5_Init(&ctx);
    for (size_t i = 0; i < count; i++)
        CC_MD5_Update(&ctx, data[i], (CC_LONG)len[i]);
    CC_MD5_Final(digest, &ctx);
#else
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_md5(), NULL);
    for (size_t i = 0; i < count; i++)
        EVP_DigestUpdate(ctx, data[i], len[i]);
    EVP_DigestFinal_ex(ctx, digest, NULL);
    EVP_MD_CTX_free(ctx);
#endif
}

/**
 * Generate PostgreSQL MD5 password hash
 * Format: 'md5' + md5(md5(password + user) + salt)
 */
static char* pg_md5_password(const char* user, const char* password, 
                              const uint8_t salt[4]) {
    unsigned char digest[KEEL_MD5_DIGEST_LENGTH];
    char hex[33];
    
    /* First hash: md5(password + user) */
    const void*  d1[] = { password, user };
    const size_t l1[] = { strlen(password), strlen(user) };
    compute_md5(d1, l1, 2, digest);
    md5_to_hex(digest, hex);
    
    /* Second hash: md5(first_hash + salt) */
    const void*  d2[] = { hex, salt };
    const size_t l2[] = { 32,  4 };
    compute_md5(d2, l2, 2, digest);
    md5_to_hex(digest, hex);
    
    /* Result: 'md5' + hex */
    char* result = malloc(36);  /* 'md5' + 32 hex + null */
    if (result) {
        strcpy(result, "md5");
        strcat(result, hex);
    }
    return result;
}

#if defined(__APPLE__)
    #pragma clang diagnostic pop
#endif

/* ============================================================================
 * Simple PostgreSQL Connection (minimal implementation for testing)
 * ============================================================================ */

#define PG_PROTOCOL_VERSION_3 ((3 << 16) | 0)
#define MAX_ERROR_LEN 256

struct integ_pg_conn {
    int         fd;
    bool        connected;
    bool        ready;
    char        txn_status;
    char        last_error[MAX_ERROR_LEN];
    uint8_t*    recv_buf;
    size_t      recv_buf_size;
    size_t      recv_len;
    char*       user;       /* For MD5 auth */
    char*       password;   /* For MD5 auth */
};

typedef struct integ_pg_endpoint {
    char     host[128];
    uint16_t port;
} integ_pg_endpoint_t;

/* Forward declarations */
static bool pg_send_startup(integ_pg_conn_t* conn, const char* user, 
                            const char* database);
static bool pg_send_password(integ_pg_conn_t* conn, const char* password);
static bool pg_send_md5_password(integ_pg_conn_t* conn, const uint8_t salt[4]);
static bool pg_process_auth(integ_pg_conn_t* conn);
static bool pg_recv_until_ready(integ_pg_conn_t* conn);
static ssize_t pg_recv(integ_pg_conn_t* conn, void* buf, size_t len);
static ssize_t pg_send(integ_pg_conn_t* conn, const void* buf, size_t len);
static bool try_pg_endpoint(const char* host, uint16_t port);
static void integ_init_endpoints(void);

static bool               g_endpoints_initialized = false;
static integ_pg_endpoint_t g_endpoints[3];

static uint16_t env_port_or_default(const char* env_name, uint16_t fallback)
{
    const char* value = getenv(env_name);
    if (!value || !*value)
        return fallback;

    char* end = NULL;
    long parsed = strtol(value, &end, 10);
    if (!end || *end != '\0' || parsed <= 0 || parsed > 65535)
        return fallback;
    return (uint16_t)parsed;
}

static void copy_host(char* dst, size_t dst_len, const char* src)
{
    if (dst_len == 0)
        return;
    if (!src)
        src = "";
    strncpy(dst, src, dst_len - 1);
    dst[dst_len - 1] = '\0';
}

/**
 * @brief Probe a prioritized list of host/port candidates for one cluster node.
 *
 * Integration tests may run in different environments: dev containers, local
 * Docker, CI, or direct compose networks. This helper tries several plausible
 * addresses and caches the first one that successfully answers a simple probe.
 *
 * @param index Endpoint slot to populate.
 * @param hosts Candidate hostnames.
 * @param ports Candidate ports aligned with `hosts`.
 * @param count Number of candidates.
 * @return true when one reachable endpoint was discovered.
 */
static bool resolve_endpoint_candidates(size_t index,
                                        const char* const hosts[],
                                        const uint16_t ports[],
                                        size_t count)
{
    for (size_t i = 0; i < count; i++) {
        if (!hosts[i] || !*hosts[i] || ports[i] == 0)
            continue;
        if (!try_pg_endpoint(hosts[i], ports[i]))
            continue;

        copy_host(g_endpoints[index].host, sizeof(g_endpoints[index].host), hosts[i]);
        g_endpoints[index].port = ports[i];
        return true;
    }
    return false;
}

/**
 * @brief Lazily discover and cache the PostgreSQL cluster endpoints.
 *
 * The discovery order prefers explicit environment overrides, then common
 * container-network hostnames, then localhost port forwards. The result is
 * cached process-wide because repeated probing would slow multi-test binaries
 * and produce noisy logs.
 *
 * @return
 */
static void integ_init_endpoints(void)
{
    if (g_endpoints_initialized)
        return;

    const char* env_host1 = getenv("KEEL_TEST_PG_HOST1");
    const char* env_host2 = getenv("KEEL_TEST_PG_HOST2");
    const char* env_host3 = getenv("KEEL_TEST_PG_HOST3");
    const char* pg_host = getenv("PGHOST");

    uint16_t env_port1 = env_port_or_default("KEEL_TEST_PG_PORT1", 0);
    uint16_t env_port2 = env_port_or_default("KEEL_TEST_PG_PORT2", 0);
    uint16_t env_port3 = env_port_or_default("KEEL_TEST_PG_PORT3", 0);
    uint16_t pg_port = env_port_or_default("PGPORT", 5432);

    const char* node1_hosts[] = {
        env_host1,
        pg_host,
        "pgsql-01",
        "pgsql-01",
        "127.0.0.1"
    };
    const uint16_t node1_ports[] = {
        env_port1,
        pg_host && *pg_host ? pg_port : 0,
        5432,
        5432,
        INTEG_PG_PORT1
    };

    const char* node2_hosts[] = {
        env_host2,
        "pgsql-02",
        "pg-replica1",
        "127.0.0.1"
    };
    const uint16_t node2_ports[] = {
        env_port2,
        5432,
        5432,
        INTEG_PG_PORT2
    };

    const char* node3_hosts[] = {
        env_host3,
        "pgsql-03",
        "pg-replica2",
        "127.0.0.1"
    };
    const uint16_t node3_ports[] = {
        env_port3,
        5432,
        5432,
        INTEG_PG_PORT3
    };

    resolve_endpoint_candidates(0, node1_hosts, node1_ports,
                                sizeof(node1_hosts) / sizeof(node1_hosts[0]));
    resolve_endpoint_candidates(1, node2_hosts, node2_ports,
                                sizeof(node2_hosts) / sizeof(node2_hosts[0]));
    resolve_endpoint_candidates(2, node3_hosts, node3_ports,
                                sizeof(node3_hosts) / sizeof(node3_hosts[0]));

    if (g_endpoints[0].host[0] == '\0') {
        copy_host(g_endpoints[0].host, sizeof(g_endpoints[0].host), "127.0.0.1");
        g_endpoints[0].port = INTEG_PG_PORT1;
    }
    if (g_endpoints[1].host[0] == '\0') {
        copy_host(g_endpoints[1].host, sizeof(g_endpoints[1].host), "127.0.0.1");
        g_endpoints[1].port = INTEG_PG_PORT2;
    }
    if (g_endpoints[2].host[0] == '\0') {
        copy_host(g_endpoints[2].host, sizeof(g_endpoints[2].host), "127.0.0.1");
        g_endpoints[2].port = INTEG_PG_PORT3;
    }

    g_endpoints_initialized = true;
}

const char* integ_get_primary_host(void) {
    integ_init_endpoints();
    return g_endpoints[0].host;
}

const char* integ_get_node_host(int node_index) {
    integ_init_endpoints();
    if (node_index < 1 || node_index > 3)
        return NULL;
    return g_endpoints[node_index - 1].host;
}

uint16_t integ_get_node_port(int node_index) {
    integ_init_endpoints();
    if (node_index < 1 || node_index > 3)
        return 0;
    return g_endpoints[node_index - 1].port;
}

static bool try_pg_endpoint(const char* host, uint16_t port) {
    integ_pg_conn_t* conn = integ_pg_connect(
        host, port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    if (!conn)
        return false;

    bool ok = integ_pg_exec(conn, "SELECT 1");
    integ_pg_close(conn);
    return ok;
}

/* ============================================================================
 * Docker/Cluster Management
 * ============================================================================ */

/**
 * @brief Check whether Docker-backed integration tests can run.
 *
 * @return true when the Docker CLI is present and the daemon responds.
 */
bool integ_docker_available(void) {
    /* Check if docker command exists and daemon is running */
    int ret = system("docker info >/dev/null 2>&1");
    if (ret != 0) {
        /* Try to give a helpful message */
        ret = system("which docker >/dev/null 2>&1");
        if (ret == 0) {
            fprintf(stderr, "Note: Docker is installed but daemon is not running.\n");
            fprintf(stderr, "      Start Docker Desktop or run 'dockerd' first.\n");
        }
        return false;
    }
    return true;
}

bool integ_cluster_running(void) {
    integ_init_endpoints();
    return try_pg_endpoint(g_endpoints[0].host, g_endpoints[0].port);
}

/**
 * @brief Start the compose-defined streaming test cluster.
 *
 * @param wait_for_ready Whether to block until the cluster answers queries.
 * @return true on success.
 */
bool integ_cluster_start(bool wait_for_ready) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), 
        "cd \"%s/docker\" && docker compose -f compose.streaming.yml up -d 2>&1",
        INTEG_DOCKER_DIR);
    
    int ret = system(cmd);
    if (ret != 0) {
        fprintf(stderr, "Failed to start cluster. Command: %s\n", cmd);
        return false;
    }
    
    if (wait_for_ready) {
        return integ_wait_for_cluster(60);  /* 60 second timeout */
    }
    
    return true;
}

bool integ_cluster_stop(bool remove_volumes) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
        "cd \"%s/docker\" && docker compose -f compose.streaming.yml down %s 2>/dev/null",
        INTEG_DOCKER_DIR,
        remove_volumes ? "-v" : "");
    return system(cmd) == 0;
}

uint16_t integ_get_primary_port(void) {
    integ_init_endpoints();

    if (strcmp(g_endpoints[0].host, "127.0.0.1") != 0 ||
        g_endpoints[0].port != INTEG_PG_PORT1) {
        return g_endpoints[0].port;
    }

    /* Try to find primary via Patroni API */
    FILE* fp = popen("curl -s http://localhost:8008/leader 2>/dev/null | grep -o 'patroni[0-9]' | head -1", "r");
    if (!fp) return INTEG_PG_PORT1;
    
    char buf[32];
    if (fgets(buf, sizeof(buf), fp)) {
        pclose(fp);
        if (strstr(buf, "patroni1")) return INTEG_PG_PORT1;
        if (strstr(buf, "patroni2")) return INTEG_PG_PORT2;
        if (strstr(buf, "patroni3")) return INTEG_PG_PORT3;
    } else {
        pclose(fp);
    }
    
    return INTEG_PG_PORT1;  /* Default */
}

bool integ_wait_for_cluster(int timeout_secs) {
    integ_init_endpoints();

    for (int i = 0; i < timeout_secs; i++) {
        if (try_pg_endpoint(g_endpoints[0].host, g_endpoints[0].port))
            return true;
        
        sleep(1);
        printf("Waiting for cluster... (%d/%d)\n", i + 1, timeout_secs);
    }
    
    return false;
}

/* ============================================================================
 * PostgreSQL Connection Implementation
 * ============================================================================ */

/**
 * @brief Create a minimal direct PostgreSQL connection for harness operations.
 *
 * The client performs a non-blocking TCP connect, runs the PostgreSQL startup
 * sequence, and records enough state to issue simple test queries afterward.
 * It is intentionally minimal and synchronous once connected because it exists
 * only to support the test harness.
 *
 * @param host Hostname.
 * @param port Port number.
 * @param user Username.
 * @param password Password.
 * @param database Database name.
 * @return Connected harness client, or `NULL` on failure.
 */
integ_pg_conn_t* integ_pg_connect(const char* host, uint16_t port,
                                   const char* user, const char* password,
                                   const char* database) {
    integ_pg_conn_t* conn = calloc(1, sizeof(integ_pg_conn_t));
    if (!conn) return NULL;
    
    conn->recv_buf_size = 16384;
    conn->recv_buf = malloc(conn->recv_buf_size);
    if (!conn->recv_buf) {
        free(conn);
        return NULL;
    }
    
    /* Create socket */
    conn->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (conn->fd < 0) {
        snprintf(conn->last_error, MAX_ERROR_LEN, "socket() failed: %s", strerror(errno));
        free(conn->recv_buf);
        free(conn);
        return NULL;
    }
    
    /* Set socket options */
    int opt = 1;
    setsockopt(conn->fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
    
    /* Set non-blocking for connect */
    int flags = fcntl(conn->fd, F_GETFL, 0);
    fcntl(conn->fd, F_SETFL, flags | O_NONBLOCK);
    
    /* Connect */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        struct hostent* he = gethostbyname(host);
        if (!he) {
            snprintf(conn->last_error, MAX_ERROR_LEN, "Host not found: %s", host);
            close(conn->fd);
            free(conn->recv_buf);
            free(conn);
            return NULL;
        }
        memcpy(&addr.sin_addr, he->h_addr, he->h_length);
    }
    
    int ret = connect(conn->fd, (struct sockaddr*)&addr, sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        snprintf(conn->last_error, MAX_ERROR_LEN, "connect() failed: %s", strerror(errno));
        close(conn->fd);
        free(conn->recv_buf);
        free(conn);
        return NULL;
    }
    
    /* Wait for connect */
    struct pollfd pfd = {.fd = conn->fd, .events = POLLOUT};
    ret = poll(&pfd, 1, INTEG_CONNECT_TIMEOUT_MS);
    if (ret <= 0 || !(pfd.revents & POLLOUT)) {
        snprintf(conn->last_error, MAX_ERROR_LEN, "Connect timeout");
        close(conn->fd);
        free(conn->recv_buf);
        free(conn);
        return NULL;
    }
    
    /* Check for connect error */
    int error = 0;
    socklen_t len = sizeof(error);
    getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &error, &len);
    if (error) {
        snprintf(conn->last_error, MAX_ERROR_LEN, "Connect failed: %s", strerror(error));
        close(conn->fd);
        free(conn->recv_buf);
        free(conn);
        return NULL;
    }
    
    /* Set blocking for simplicity */
    fcntl(conn->fd, F_SETFL, flags);
    
    conn->connected = true;
    
    /* Store credentials for MD5 auth */
    conn->user = strdup(user);
    conn->password = strdup(password);
    
    /* Send startup message */
    if (!pg_send_startup(conn, user, database)) {
        close(conn->fd);
        free(conn->recv_buf);
        free(conn->user);
        free(conn->password);
        free(conn);
        return NULL;
    }
    
    /* Process authentication */
    if (!pg_process_auth(conn)) {
        close(conn->fd);
        free(conn->recv_buf);
        free(conn->user);
        free(conn->password);
        free(conn);
        return NULL;
    }
    
    conn->ready = true;
    return conn;
}

void integ_pg_close(integ_pg_conn_t* conn) {
    if (!conn) return;
    
    if (conn->connected && conn->fd >= 0) {
        /* Send Terminate message */
        uint8_t term[5] = {'X', 0, 0, 0, 4};
        pg_send(conn, term, sizeof(term));
        close(conn->fd);
    }
    
    free(conn->recv_buf);
    free(conn->user);
    free(conn->password);
    free(conn);
}

bool integ_pg_is_alive(integ_pg_conn_t* conn) {
    if (!conn || !conn->connected) return false;
    
    /* Try a simple query */
    return integ_pg_exec(conn, "SELECT 1");
}

bool integ_pg_exec(integ_pg_conn_t* conn, const char* query) {
    if (!conn || !conn->ready) return false;
    
    /* Build Query message */
    size_t query_len = strlen(query);
    size_t msg_len = 4 + query_len + 1;
    
    uint8_t* msg = malloc(1 + msg_len);
    if (!msg) return false;
    
    msg[0] = 'Q';  /* Query */
    uint32_t net_len = htonl(msg_len);
    memcpy(msg + 1, &net_len, 4);
    memcpy(msg + 5, query, query_len + 1);
    
    if (pg_send(conn, msg, 1 + msg_len) != (ssize_t)(1 + msg_len)) {
        free(msg);
        snprintf(conn->last_error, MAX_ERROR_LEN, "Failed to send query");
        return false;
    }
    free(msg);
    
    /* Wait for ReadyForQuery */
    return pg_recv_until_ready(conn);
}

bool integ_pg_query_int(integ_pg_conn_t* conn, const char* query, int64_t* result) {
    if (!conn || !conn->ready || !result) return false;
    
    /* Build Query message */
    size_t query_len = strlen(query);
    size_t msg_len = 4 + query_len + 1;
    
    uint8_t* msg = malloc(1 + msg_len);
    if (!msg) return false;
    
    msg[0] = 'Q';
    uint32_t net_len = htonl(msg_len);
    memcpy(msg + 1, &net_len, 4);
    memcpy(msg + 5, query, query_len + 1);
    
    if (pg_send(conn, msg, 1 + msg_len) != (ssize_t)(1 + msg_len)) {
        free(msg);
        return false;
    }
    free(msg);
    
    /* Process response looking for DataRow */
    bool found = false;
    while (1) {
        uint8_t type;
        if (pg_recv(conn, &type, 1) != 1) break;
        
        uint32_t len;
        if (pg_recv(conn, &len, 4) != 4) break;
        len = ntohl(len) - 4;
        
        if (len > conn->recv_buf_size) {
            conn->recv_buf = realloc(conn->recv_buf, len);
            conn->recv_buf_size = len;
        }
        
        if (pg_recv(conn, conn->recv_buf, len) != (ssize_t)len) break;
        
        if (type == 'D') {  /* DataRow */
            /* Parse first column */
            if (len >= 6) {
                int16_t ncols = ntohs(*(int16_t*)conn->recv_buf);
                if (ncols >= 1) {
                    int32_t col_len;
                    memcpy(&col_len, conn->recv_buf + 2, sizeof(col_len));
                    col_len = ntohl(col_len);
                    if (col_len > 0 && col_len < 32) {
                        char val[32];
                        memcpy(val, conn->recv_buf + 6, col_len);
                        val[col_len] = '\0';
                        *result = strtoll(val, NULL, 10);
                        found = true;
                    }
                }
            }
        } else if (type == 'Z') {  /* ReadyForQuery */
            conn->txn_status = conn->recv_buf[0];
            break;
        } else if (type == 'E') {  /* Error */
            snprintf(conn->last_error, MAX_ERROR_LEN, "Query error");
        }
    }
    
    return found;
}

bool integ_pg_query_string(integ_pg_conn_t* conn, const char* query,
                            char* result, size_t result_size) {
    if (!conn || !conn->ready || !result) return false;
    
    /* Build Query message */
    size_t query_len = strlen(query);
    size_t msg_len = 4 + query_len + 1;
    
    uint8_t* msg = malloc(1 + msg_len);
    if (!msg) return false;
    
    msg[0] = 'Q';
    uint32_t net_len = htonl(msg_len);
    memcpy(msg + 1, &net_len, 4);
    memcpy(msg + 5, query, query_len + 1);
    
    if (pg_send(conn, msg, 1 + msg_len) != (ssize_t)(1 + msg_len)) {
        free(msg);
        return false;
    }
    free(msg);
    
    /* Process response looking for DataRow */
    bool found = false;
    while (1) {
        uint8_t type;
        if (pg_recv(conn, &type, 1) != 1) break;
        
        uint32_t len;
        if (pg_recv(conn, &len, 4) != 4) break;
        len = ntohl(len) - 4;
        
        if (len > conn->recv_buf_size) {
            conn->recv_buf = realloc(conn->recv_buf, len);
            conn->recv_buf_size = len;
        }
        
        if (pg_recv(conn, conn->recv_buf, len) != (ssize_t)len) break;
        
        if (type == 'D') {  /* DataRow */
            if (len >= 6) {
                int16_t ncols = ntohs(*(int16_t*)conn->recv_buf);
                if (ncols >= 1) {
                    int32_t col_len;
                    memcpy(&col_len, conn->recv_buf + 2, sizeof(col_len));
                    col_len = ntohl(col_len);
                    if (col_len > 0 && (size_t)col_len < result_size - 1) {
                        memcpy(result, conn->recv_buf + 6, col_len);
                        result[col_len] = '\0';
                        found = true;
                    }
                }
            }
        } else if (type == 'Z') {
            conn->txn_status = conn->recv_buf[0];
            break;
        } else if (type == 'E') {
            snprintf(conn->last_error, MAX_ERROR_LEN, "Query error");
        }
    }
    
    return found;
}

const char* integ_pg_last_error(integ_pg_conn_t* conn) {
    return conn ? conn->last_error : "NULL connection";
}

/* ============================================================================
 * Internal PostgreSQL Protocol Helpers
 * ============================================================================ */

static ssize_t pg_recv(integ_pg_conn_t* conn, void* buf, size_t len) {
    size_t total = 0;
    uint8_t* p = buf;
    
    while (total < len) {
        struct pollfd pfd = {.fd = conn->fd, .events = POLLIN};
        int ret = poll(&pfd, 1, INTEG_QUERY_TIMEOUT_MS);
        if (ret <= 0) break;
        
        ssize_t n = recv(conn->fd, p + total, len - total, 0);
        if (n <= 0) break;
        total += n;
    }
    
    return total;
}

static ssize_t pg_send(integ_pg_conn_t* conn, const void* buf, size_t len) {
    return send(conn->fd, buf, len, 0);
}

static bool pg_send_startup(integ_pg_conn_t* conn, const char* user, 
                            const char* database) {
    /* Calculate message length */
    size_t len = 4 +  /* length */
                 4 +  /* protocol version */
                 strlen("user") + 1 + strlen(user) + 1 +
                 strlen("database") + 1 + strlen(database) + 1 +
                 1;   /* terminator */
    
    uint8_t* msg = malloc(len);
    if (!msg) return false;
    
    uint8_t* p = msg;
    
    /* Length */
    uint32_t net_len = htonl(len);
    memcpy(p, &net_len, 4);
    p += 4;
    
    /* Protocol version 3.0 */
    uint32_t version = htonl(PG_PROTOCOL_VERSION_3);
    memcpy(p, &version, 4);
    p += 4;
    
    /* user */
    memcpy(p, "user", 5);
    p += 5;
    memcpy(p, user, strlen(user) + 1);
    p += strlen(user) + 1;
    
    /* database */
    memcpy(p, "database", 9);
    p += 9;
    memcpy(p, database, strlen(database) + 1);
    p += strlen(database) + 1;
    
    /* Terminator */
    *p = 0;
    
    ssize_t sent = pg_send(conn, msg, len);
    free(msg);
    
    return sent == (ssize_t)len;
}

static bool pg_send_password(integ_pg_conn_t* conn, const char* password) {
    size_t pwd_len = strlen(password);
    size_t msg_len = 4 + pwd_len + 1;
    
    uint8_t* msg = malloc(1 + msg_len);
    if (!msg) return false;
    
    msg[0] = 'p';  /* PasswordMessage */
    uint32_t net_len = htonl(msg_len);
    memcpy(msg + 1, &net_len, 4);
    memcpy(msg + 5, password, pwd_len + 1);
    
    ssize_t sent = pg_send(conn, msg, 1 + msg_len);
    free(msg);
    
    return sent == (ssize_t)(1 + msg_len);
}

static bool pg_send_md5_password(integ_pg_conn_t* conn, const uint8_t salt[4]) {
    /* Generate MD5 password hash: 'md5' + md5(md5(password + user) + salt) */
    char* md5_hash = pg_md5_password(conn->user, conn->password, salt);
    if (!md5_hash) {
        snprintf(conn->last_error, MAX_ERROR_LEN, "Failed to generate MD5 hash");
        return false;
    }
    
    bool result = pg_send_password(conn, md5_hash);
    free(md5_hash);
    return result;
}

static bool pg_process_auth(integ_pg_conn_t* conn) {
    while (1) {
        uint8_t type;
        if (pg_recv(conn, &type, 1) != 1) {
            snprintf(conn->last_error, MAX_ERROR_LEN, "Failed to read message type");
            return false;
        }
        
        uint32_t len;
        if (pg_recv(conn, &len, 4) != 4) {
            snprintf(conn->last_error, MAX_ERROR_LEN, "Failed to read message length");
            return false;
        }
        len = ntohl(len) - 4;
        
        if (len > conn->recv_buf_size) {
            conn->recv_buf = realloc(conn->recv_buf, len);
            if (!conn->recv_buf) return false;
            conn->recv_buf_size = len;
        }
        
        if (len > 0 && pg_recv(conn, conn->recv_buf, len) != (ssize_t)len) {
            snprintf(conn->last_error, MAX_ERROR_LEN, "Failed to read message body");
            return false;
        }
        
        switch (type) {
        case 'R': {  /* Authentication */
            uint32_t auth_type = ntohl(*(uint32_t*)conn->recv_buf);
            
            if (auth_type == 0) {  /* AuthOk */
                /* Continue to wait for ReadyForQuery */
            } else if (auth_type == 3) {  /* CleartextPassword */
                if (!pg_send_password(conn, conn->password)) {
                    snprintf(conn->last_error, MAX_ERROR_LEN, "Failed to send password");
                    return false;
                }
            } else if (auth_type == 5) {  /* MD5Password */
                /* MD5 auth: md5(md5(password + user) + salt) */
                /* Salt is in bytes 4-7 of the auth message */
                if (len < 8) {
                    snprintf(conn->last_error, MAX_ERROR_LEN, "MD5 auth message too short");
                    return false;
                }
                if (!pg_send_md5_password(conn, conn->recv_buf + 4)) {
                    return false;
                }
            } else if (auth_type == 10) {  /* SASL */
                snprintf(conn->last_error, MAX_ERROR_LEN, "SASL auth not implemented in test client");
                return false;
            } else {
                snprintf(conn->last_error, MAX_ERROR_LEN, "Unknown auth type: %d", auth_type);
                return false;
            }
            break;
        }
        
        case 'K':  /* BackendKeyData */
            /* Store if needed, but we don't use it */
            break;
        
        case 'S':  /* ParameterStatus */
            /* Ignore for now */
            break;
        
        case 'Z':  /* ReadyForQuery */
            conn->txn_status = conn->recv_buf[0];
            return true;
        
        case 'E': {  /* ErrorResponse */
            /* Parse error message */
            size_t pos = 0;
            while (pos < len && conn->recv_buf[pos]) {
                char field = conn->recv_buf[pos++];
                const char* val = (const char*)(conn->recv_buf + pos);
                pos += strlen(val) + 1;
                
                if (field == 'M') {
                    snprintf(conn->last_error, MAX_ERROR_LEN, "%s", val);
                }
            }
            return false;
        }
        
        default:
            /* Ignore unknown messages during auth */
            break;
        }
    }
}

static bool pg_recv_until_ready(integ_pg_conn_t* conn) {
    bool had_error = false;
    
    while (1) {
        uint8_t type;
        if (pg_recv(conn, &type, 1) != 1) return false;
        
        uint32_t len;
        if (pg_recv(conn, &len, 4) != 4) return false;
        len = ntohl(len) - 4;
        
        if (len > conn->recv_buf_size) {
            conn->recv_buf = realloc(conn->recv_buf, len);
            conn->recv_buf_size = len;
        }
        
        if (len > 0 && pg_recv(conn, conn->recv_buf, len) != (ssize_t)len) return false;
        
        if (type == 'Z') {  /* ReadyForQuery */
            conn->txn_status = conn->recv_buf[0];
            return !had_error;  /* Return false if we had an error */
        } else if (type == 'E') {  /* Error */
            had_error = true;
            size_t pos = 0;
            while (pos < len && conn->recv_buf[pos]) {
                char field = conn->recv_buf[pos++];
                const char* val = (const char*)(conn->recv_buf + pos);
                pos += strlen(val) + 1;
                if (field == 'M') {
                    snprintf(conn->last_error, MAX_ERROR_LEN, "%s", val);
                }
            }
            /* Continue to wait for ReadyForQuery */
        }
    }
}
