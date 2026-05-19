/**
 * @file cluster.c
 * @brief Multi-proxy HA cluster mode implementation.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file implements the cluster coordination subsystem described in
 * cluster.h. The implementation uses a single dedicated thread that manages:
 *   - A listening socket for incoming peer connections.
 *   - Periodic heartbeat sends to all known peers.
 *   - Heartbeat receive and peer health state machine updates.
 *   - Configuration gossip (checksum comparison + sync).
 *   - Server topology change propagation.
 *
 * The cluster thread uses blocking I/O with poll() for multiplexing. This
 * is intentionally simpler than the reactor-based worker architecture because
 * cluster traffic is low-frequency control-plane data (1 heartbeat/sec per
 * peer), not high-throughput data plane. Simplicity and debuggability are
 * prioritized over raw throughput here.
 *
 * Thread safety:
 *   - Peer state fields use atomic operations for lock-free reads from
 *     admin/stats threads.
 *   - The peer array is protected by a lightweight mutex for structural
 *     modifications (add/remove peer).
 *   - The cluster thread is the only writer for health state; other threads
 *     only read atomics.
 */

#include "keel/core/cluster.h"
#include "keel/core/compress.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/util/platform_compat.h"
#include "keel/util/util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#if defined(__linux__)
#include <sys/epoll.h>
#endif

/* ============================================================================
 * Internal State
 * ============================================================================ */

struct keel_cluster {
    keel_cluster_config_t config;

    /* Peer table */
    keel_cluster_peer_t   peers[KEEL_CLUSTER_MAX_PEERS];
    size_t                peer_count;
    pthread_mutex_t       peer_lock;

    /* Cluster thread */
    pthread_t             thread;
    _Atomic(bool)         running;
    int                   listen_fd;

    /* Statistics */
    _Atomic(uint64_t)     heartbeats_sent;
    _Atomic(uint64_t)     heartbeats_received;
    _Atomic(uint64_t)     sync_requests_sent;
    _Atomic(uint64_t)     sync_responses_sent;
    _Atomic(uint64_t)     discovered_peers_total;
    _Atomic(uint64_t)     config_reconciliations;
    _Atomic(uint64_t)     last_sync_apply_ns;
    _Atomic(uint64_t)     server_notifications;

    /* Config state */
    _Atomic(uint64_t)     config_checksum;

    /* Timing */
    uint64_t              start_time_ns;

    /* Optional engine stats callback (populated after engine creation) */
    keel_cluster_stats_cb_t stats_cb;
    void*                   stats_cb_data;

    /* Optional server topology change callback */
    keel_cluster_server_notify_cb_t server_notify_cb;
    void*                           server_notify_cb_data;

    /* -----------------------------------------------------------------------
     * Leader Election State
     * ----------------------------------------------------------------------- */
    _Atomic(int)      role;                         /* keel_cluster_role_t */
    _Atomic(uint64_t) term;                         /* Current election term */
    _Atomic(uint64_t) last_leader_contact_ns;       /* Monotonic ns; reset by leader heartbeats */
    pthread_mutex_t   election_lock;                /* Protects voted_for and leader_id */
    char              voted_for[KEEL_CLUSTER_MAX_NODE_ID]; /* Who we voted for in current term */
    char              leader_id[KEEL_CLUSTER_MAX_NODE_ID]; /* Currently known leader, or empty */
    uint32_t          election_timeout_ms;          /* Per-node randomised timeout */

    /* Election counters */
    _Atomic(uint64_t) elections_started;
    _Atomic(uint64_t) elections_won;
    _Atomic(uint64_t) votes_granted;
    _Atomic(uint64_t) votes_denied;
    _Atomic(uint64_t) leader_stepdowns;

    /* 2PC quorum config commit state (leader side) */
    _Atomic(uint64_t) config_txn_counter; /* Monotonic transaction ID */

    /* 2PC pending state (follower side) */
    pthread_mutex_t   config_commit_lock;
    uint64_t          pending_txn_id;     /* 0 = no pending prepare */
    uint64_t          pending_checksum;
};

/* ============================================================================
 * Time Helpers
 * ============================================================================ */

/**
 * @brief Return the current monotonic time in nanoseconds.
 *
 * Uses CLOCK_MONOTONIC so the result is suitable for measuring elapsed time
 * and heartbeat intervals. Not affected by wall-clock adjustments.
 *
 * @return Nanoseconds since an unspecified epoch.
 */
static uint64_t cluster_now_ns(void) { return (uint64_t)keel_time_now(); }

/**
 * @brief Return the current wall-clock time in whole seconds.
 *
 * Uses CLOCK_REALTIME and is intended for recording discovery timestamps
 * (e.g., peer->discovered_at_sec) that are compared against human-readable
 * time rather than elapsed durations.
 *
 * @return Seconds since the Unix epoch.
 */
static uint64_t cluster_now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec;
}

/* ============================================================================
 * Socket Helpers
 * ============================================================================ */

/**
 * @brief Set a file descriptor to non-blocking mode.
 *
 * @param fd File descriptor to modify.
 * @return 0 on success, -1 on error (errno set by fcntl).
 */
static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief Disable Nagle's algorithm (TCP_NODELAY) on a socket.
 *
 * Reduces latency for small control-plane messages by preventing buffering.
 *
 * @param fd TCP socket file descriptor.
 * @return 0 on success, -1 on error (errno set by setsockopt).
 */
static int set_nodelay(int fd) {
    int val = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
}

/**
 * @brief Create and bind a non-blocking TCP listening socket.
 *
 * Binds to the given IPv4 address and port, enables SO_REUSEADDR, calls
 * listen(), and sets the socket to non-blocking mode.
 *
 * @param addr IPv4 address string (e.g. "0.0.0.0").
 * @param port TCP port to listen on.
 * @return Listening file descriptor on success, -1 on failure.
 */
static int create_listen_socket(const char* addr, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    if (inet_pton(AF_INET, addr, &sa.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 16) < 0) {
        close(fd);
        return -1;
    }

    set_nonblocking(fd);
    return fd;
}

/**
 * @brief Establish a non-blocking TCP connection to a cluster peer.
 *
 * Creates a non-blocking socket, initiates a connect(), and uses poll() to
 * wait up to @p timeout_ms milliseconds for the connection to complete.
 * TCP_NODELAY is enabled on success.
 *
 * @param addr  IPv4 address string of the remote peer.
 * @param port  TCP port of the remote peer.
 * @param timeout_ms Maximum milliseconds to wait for the connection.
 * @return Connected file descriptor on success, -1 on failure.
 */
static int connect_to_peer(const char* addr, uint16_t port, uint32_t timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    set_nonblocking(fd);
    set_nodelay(fd);

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    if (inet_pton(AF_INET, addr, &sa.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    int rc = connect(fd, (struct sockaddr*)&sa, sizeof(sa));
    if (rc < 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }

    if (rc < 0) {
        /* Wait for connect to complete using epoll (O(1), no signal overhead) */
        rc = keel_fd_wait(fd, KEEL_FD_WAIT_WRITE, (int)timeout_ms);
        if (rc <= 0) {
            close(fd);
            return -1;
        }
        int err = 0;
        socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err != 0) {
            close(fd);
            return -1;
        }
    }

    return fd;
}

/* ============================================================================
 * Wire Protocol Helpers
 * ============================================================================ */

/**
 * @brief Helper macro: map a cluster config codec byte to keel_compress_codec_t.
 *
 * Used at every send_msg() call site so that the configured compression codec
 * is passed consistently without repeating the cast.
 */
#define CLUSTER_CODEC(c) \
    ((keel_compress_codec_t)((c)->config.compress_codec))

/**
 * @brief Fill a cluster wire-protocol message header.
 *
 * Writes the magic number, protocol version, message type, payload length
 * (with compression codec encoded in the top 2 bits), and node identifier
 * into @p hdr.
 *
 * @param hdr         Header structure to populate.
 * @param msg_type    One of the KEEL_CLUSTER_MSG_* type constants.
 * @param wire_len    Byte length of the (possibly compressed) payload.
 * @param codec_flag  KEEL_CLUSTER_WIRE_NONE/ZLIB/ZSTD — OR-ed into the
 *                    top 2 bits of the payload_len wire field.
 * @param node_id     Null-terminated local node identifier string, or NULL.
 */
static void encode_header(keel_cluster_msg_header_t* hdr,
                          uint8_t msg_type, uint16_t wire_len,
                          uint16_t codec_flag, const char* node_id) {
    hdr->magic = htonl(KEEL_CLUSTER_MAGIC);
    hdr->version = KEEL_CLUSTER_PROTO_VERSION;
    hdr->msg_type = msg_type;
    /* Encode codec in the top 2 bits, actual length in bits 13-0 */
    hdr->payload_len = htons((uint16_t)((wire_len & KEEL_CLUSTER_PAYLOAD_LEN_MASK)
                                        | (codec_flag & KEEL_CLUSTER_COMPRESS_MASK)));
    memset(hdr->node_id, 0, sizeof(hdr->node_id));
    if (node_id) {
        size_t len = strlen(node_id);
        if (len >= sizeof(hdr->node_id)) len = sizeof(hdr->node_id) - 1;
        memcpy(hdr->node_id, node_id, len);
    }
}

/**
 * @brief Validate a received cluster wire-protocol header.
 *
 * Checks the magic number and protocol version to ensure the message
 * originated from a compatible keel cluster peer.
 *
 * @param hdr Received header (fields in network byte order).
 * @return true if the header is valid, false otherwise.
 */
static bool validate_header(const keel_cluster_msg_header_t* hdr) {
    return ntohl(hdr->magic) == KEEL_CLUSTER_MAGIC &&
           hdr->version == KEEL_CLUSTER_PROTO_VERSION;
}

/** Map a KEEL_CLUSTER_WIRE_* flag (top 2 bits) to a keel_compress_codec_t value. */
static keel_compress_codec_t cluster_codec_flag_to_enum(uint16_t flag)
{
    switch (flag & KEEL_CLUSTER_COMPRESS_MASK) {
    case KEEL_CLUSTER_WIRE_ZLIB: return KEEL_COMPRESS_GZIP;
    case KEEL_CLUSTER_WIRE_ZSTD: return KEEL_COMPRESS_ZSTD;
    default:                     return KEEL_COMPRESS_NONE;
    }
}

/** Map a keel_compress_codec_t to the matching KEEL_CLUSTER_WIRE_* flag. */
static uint16_t cluster_enum_to_codec_flag(keel_compress_codec_t codec)
{
    switch (codec) {
    case KEEL_COMPRESS_GZIP: return KEEL_CLUSTER_WIRE_ZLIB;
    case KEEL_COMPRESS_ZSTD: return KEEL_CLUSTER_WIRE_ZSTD;
    default:                 return KEEL_CLUSTER_WIRE_NONE;
    }
}

/**
 * @brief Serialize and send a cluster message over a connected socket.
 *
 * When @p compress_codec is not KEEL_COMPRESS_NONE and payload_len is above
 * the compression threshold, the payload is compressed before transmission
 * and the codec is signalled in the top 2 bits of the wire payload_len field.
 * If compression produces a larger result than the original the uncompressed
 * payload is sent instead (with KEEL_CLUSTER_COMPRESS_NONE).
 *
 * @param fd             Connected TCP socket file descriptor.
 * @param msg_type       One of the KEEL_CLUSTER_MSG_* type constants.
 * @param payload        Pointer to the payload bytes, or NULL if none.
 * @param payload_len    Byte length of @p payload (0 if none).
 * @param node_id        Local node identifier to embed in the header.
 * @param compress_codec Desired codec (KEEL_COMPRESS_NONE disables compression).
 * @return 0 on success, -1 if the full message could not be sent.
 */
static int send_msg(int fd, uint8_t msg_type, const void* payload,
                    uint16_t payload_len, const char* node_id,
                    keel_compress_codec_t compress_codec) {
    /* Compression scratch buffer — sized for worst-case gzip/zstd expansion */
    uint8_t cbuf[KEEL_CLUSTER_MAX_PAYLOAD];
    const void  *wire_payload  = payload;
    uint16_t     wire_len      = payload_len;
    uint16_t     codec_flag    = KEEL_CLUSTER_WIRE_NONE;

    if (compress_codec != KEEL_COMPRESS_NONE && payload && payload_len > 0
            && payload_len <= KEEL_CLUSTER_MAX_PAYLOAD) {
        size_t bound = keel_compress_bound(compress_codec, payload_len);
        if (bound <= sizeof(cbuf)) {
            ssize_t clen = keel_compress(compress_codec,
                                         payload, payload_len,
                                         cbuf,    sizeof(cbuf));
            if (clen > 0 && (size_t)clen < payload_len) {
                /* Compression was beneficial */
                wire_payload = cbuf;
                wire_len     = (uint16_t)clen;
                codec_flag   = cluster_enum_to_codec_flag(compress_codec);
            }
            /* else: fall through and send uncompressed */
        }
    }

    if (wire_len > KEEL_CLUSTER_MAX_PAYLOAD) return -1;

    keel_cluster_msg_header_t hdr;
    encode_header(&hdr, msg_type, wire_len, codec_flag, node_id);

    /* Send header + payload atomically */
    uint8_t buf[sizeof(hdr) + KEEL_CLUSTER_MAX_PAYLOAD];
    memcpy(buf, &hdr, sizeof(hdr));
    if (wire_len > 0 && wire_payload)
        memcpy(buf + sizeof(hdr), wire_payload, wire_len);

    size_t total = sizeof(hdr) + wire_len;
    ssize_t sent = send(fd, buf, total, MSG_NOSIGNAL);
    return (sent == (ssize_t)total) ? 0 : -1;
}

/**
 * @brief Receive a cluster message from a connected socket.
 *
 * Waits up to @p timeout_ms milliseconds for data, reads the fixed-size
 * header, validates it, reads the (possibly compressed) payload, and
 * decompresses it transparently before returning.  On return, @p payload
 * always contains the decompressed data and @p hdr->payload_len is set to
 * the decompressed length (for callers that inspect it directly).
 *
 * @param fd          Connected TCP socket file descriptor.
 * @param hdr         Output: populated message header (payload_len = decompressed).
 * @param payload     Buffer for the decompressed message payload.
 * @param payload_cap Capacity of @p payload in bytes.
 * @param timeout_ms  Maximum milliseconds to wait for the header.
 * @return 0 on success, -1 on timeout, validation failure, or capacity overflow.
 */
static int recv_msg(int fd, keel_cluster_msg_header_t* hdr,
                    void* payload, size_t payload_cap, uint32_t timeout_ms) {
    /* Wait for data */
    int rc = keel_fd_wait(fd, KEEL_FD_WAIT_READ, (int)timeout_ms);
    if (rc <= 0) return -1;

    /* Read header */
    ssize_t n = recv(fd, hdr, sizeof(*hdr), MSG_WAITALL);
    if (n != (ssize_t)sizeof(*hdr)) return -1;

    if (!validate_header(hdr)) return -1;

    /* Decode compression flag and actual wire payload length */
    uint16_t raw_field  = ntohs(hdr->payload_len);
    uint16_t codec_flag = (uint16_t)(raw_field & KEEL_CLUSTER_COMPRESS_MASK);
    uint16_t wire_plen  = (uint16_t)(raw_field & KEEL_CLUSTER_PAYLOAD_LEN_MASK);

    if (wire_plen > KEEL_CLUSTER_MAX_PAYLOAD) return -1;

    if (wire_plen > 0) {
        if (codec_flag == KEEL_CLUSTER_WIRE_NONE) {
            /* Uncompressed — read directly into caller buffer */
            if (wire_plen > payload_cap) return -1;
            n = recv(fd, payload, wire_plen, MSG_WAITALL);
            if (n != (ssize_t)wire_plen) return -1;
            /* Expose decompressed length as plain payload_len */
            hdr->payload_len = htons(wire_plen);
        } else {
            /* Compressed — read into a temporary buffer, then decompress */
            uint8_t cbuf[KEEL_CLUSTER_MAX_PAYLOAD];
            n = recv(fd, cbuf, wire_plen, MSG_WAITALL);
            if (n != (ssize_t)wire_plen) return -1;

            keel_compress_codec_t codec = cluster_codec_flag_to_enum(codec_flag);
            ssize_t dlen = keel_decompress(codec, cbuf, (size_t)wire_plen,
                                           payload, payload_cap);
            if (dlen < 0) return -1;
            /* Expose decompressed length */
            hdr->payload_len = htons((uint16_t)dlen);
        }
    } else {
        hdr->payload_len = 0;
    }

    return 0;
}

/**
 * @brief Determine whether an address or node ID refers to this node.
 *
 * Returns true if @p node_id matches the local node ID, or if both @p addr
 * and @p port match the configured listen address and port.
 *
 * @param c       Cluster instance.
 * @param node_id Remote node identifier string (may be NULL or empty).
 * @param addr    Remote IPv4 address string (may be NULL or empty).
 * @param port    Remote TCP port.
 * @return true if the peer describes this node.
 */
static bool cluster_is_self(const keel_cluster_t* c,
                            const char* node_id,
                            const char* addr,
                            uint16_t port) {
    if (!c) return false;
    if (node_id && node_id[0] != '\0' && strcmp(node_id, c->config.node_id) == 0) {
        return true;
    }
    if (addr && addr[0] != '\0' &&
        strcmp(addr, c->config.listen_addr) == 0 &&
        port == c->config.listen_port) {
        return true;
    }
    return false;
}

/**
 * @brief Count the number of currently active (enrolled) peers.
 *
 * Acquires peer_lock and counts peers with the @c active flag set,
 * regardless of their health status.
 *
 * @param c Cluster instance.
 * @return Number of active peer entries, or 0 if @p c is NULL.
 */
static uint16_t cluster_active_peer_count(const keel_cluster_t* c) {
    if (!c) return 0;
    uint16_t count = 0;
    pthread_mutex_lock(&((keel_cluster_t*)c)->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i].active) count++;
    }
    pthread_mutex_unlock(&((keel_cluster_t*)c)->peer_lock);
    return count;
}

/**
 * @brief Check whether a peer identified by address or node ID is already known.
 *
 * Searches the active peer table for a match by (addr, port) or by node_id.
 * Acquires peer_lock for the search.
 *
 * @param c       Cluster instance.
 * @param addr    IPv4 address string of the candidate peer.
 * @param port    TCP port of the candidate peer.
 * @param node_id Node identifier of the candidate peer (may be NULL or empty).
 * @return true if a matching active peer was found.
 */
static bool cluster_has_peer(keel_cluster_t* c,
                             const char* addr,
                             uint16_t port,
                             const char* node_id) {
    if (!c || !addr || addr[0] == '\0' || port == 0) return false;

    bool found = false;
    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        const keel_cluster_peer_t* peer = &c->peers[i];
        if (!peer->active) continue;

        bool addr_match = strcmp(peer->addr, addr) == 0 && peer->port == port;
        bool id_match = node_id && node_id[0] != '\0' &&
                        peer->node_id[0] != '\0' &&
                        strcmp(peer->node_id, node_id) == 0;
        if (addr_match || id_match) {
            found = true;
            break;
        }
    }
    pthread_mutex_unlock(&c->peer_lock);
    return found;
}

/**
 * @brief Clamp a configuration value to a safe range, falling back on error.
 *
 * If @p value is within [min_v, max_v] it is returned unchanged; otherwise
 * @p fallback is returned. Used during config reconciliation to prevent
 * unsafe runtime parameter swings.
 *
 * @param value    Incoming value to validate.
 * @param min_v    Minimum acceptable value (inclusive).
 * @param max_v    Maximum acceptable value (inclusive).
 * @param fallback Value to return when @p value is out of range.
 * @return The validated value or @p fallback.
 */
static uint32_t cluster_reconcile_u32(uint32_t value,
                                      uint32_t min_v,
                                      uint32_t max_v,
                                      uint32_t fallback) {
    if (value < min_v || value > max_v) return fallback;
    return value;
}

/**
 * @brief Apply a peer's configuration payload to the local cluster config.
 *
 * Uses a deterministic ordering rule (only accept from peers that sort before
 * the local node_id) to prevent config ping-pong. Each numeric field is
 * validated via cluster_reconcile_u32() before being applied.
 *
 * @param c            Cluster instance.
 * @param cfg          Received sync configuration (fields in network byte order).
 * @param from_node_id Node identifier of the sender.
 * @return true if any local configuration field was changed.
 */
static bool cluster_apply_sync_config(keel_cluster_t* c,
                                      const keel_cluster_sync_config_t* cfg,
                                      const char* from_node_id) {
    if (!c || !cfg) return false;

    /* Deterministic rule to avoid two-way config ping-pong:
     * apply only from peers that sort before this node ID. */
    if (from_node_id && from_node_id[0] != '\0' &&
        c->config.node_id[0] != '\0' &&
        strcmp(from_node_id, c->config.node_id) >= 0) {
        return false;
    }

    uint32_t remote_interval = ntohl(cfg->heartbeat_interval_ms);
    uint32_t remote_timeout = ntohl(cfg->heartbeat_timeout_ms);
    uint32_t remote_threshold = ntohl(cfg->failure_threshold);
    bool remote_auto_sync = cfg->auto_sync != 0;

    /* Keep reconciliation conservative and bounded to avoid unsafe runtime swings. */
    uint32_t interval = cluster_reconcile_u32(remote_interval, 100, 60000,
                                              c->config.heartbeat_interval_ms);
    uint32_t timeout = cluster_reconcile_u32(remote_timeout, 500, 120000,
                                             c->config.heartbeat_timeout_ms);
    uint32_t threshold = cluster_reconcile_u32(remote_threshold, 1, 16,
                                               c->config.failure_threshold);

    /* Timeout must never be shorter than heartbeat interval. */
    if (timeout < interval) timeout = interval;

    /* peer_lock also guards the plain config fields (heartbeat_interval_ms,
     * heartbeat_timeout_ms, failure_threshold, auto_sync) against races with
     * keel_cluster_get_stats() which reads them from the caller thread. */
    pthread_mutex_lock(&c->peer_lock);
    bool changed = false;
    if (interval != c->config.heartbeat_interval_ms) {
        c->config.heartbeat_interval_ms = interval;
        changed = true;
    }
    if (timeout != c->config.heartbeat_timeout_ms) {
        c->config.heartbeat_timeout_ms = timeout;
        changed = true;
    }
    if (threshold != c->config.failure_threshold) {
        c->config.failure_threshold = threshold;
        changed = true;
    }
    if (remote_auto_sync != c->config.auto_sync) {
        c->config.auto_sync = remote_auto_sync;
        changed = true;
    }
    pthread_mutex_unlock(&c->peer_lock);

    if (changed) {
        atomic_fetch_add(&c->config_reconciliations, 1);
        atomic_store(&c->last_sync_apply_ns, cluster_now_ns());
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Reconciled safe config from %s: interval=%u timeout=%u threshold=%u auto_sync=%u",
            (from_node_id && from_node_id[0] != '\0') ? from_node_id : "(unknown)",
            c->config.heartbeat_interval_ms,
            c->config.heartbeat_timeout_ms,
            c->config.failure_threshold,
            c->config.auto_sync ? 1U : 0U);
    }

    return changed;
}

/**
 * @brief Register a new peer or update an existing one in the peer table.
 *
 * Searches the active peer table for a matching entry by (addr, port) or
 * node_id. If found, fills in any missing node_id and upgrades the source
 * if appropriate. If not found, inserts a new entry (up to
 * KEEL_CLUSTER_MAX_PEERS). Acquires peer_lock for all mutations.
 *
 * @param c       Cluster instance.
 * @param addr    IPv4 address string of the peer.
 * @param port    TCP port of the peer.
 * @param node_id Node identifier string (may be NULL or empty).
 * @param source  How this peer was discovered (bootstrap, join, gossip, admin).
 * @return 0 on success, -1 if parameters are invalid or the table is full.
 */
static int cluster_add_or_update_peer(keel_cluster_t* c,
                                      const char* addr,
                                      uint16_t port,
                                      const char* node_id,
                                      keel_peer_source_t source) {
    if (!c || !addr || addr[0] == '\0' || port == 0) return -1;

    pthread_mutex_lock(&c->peer_lock);

    for (size_t i = 0; i < c->peer_count; i++) {
        keel_cluster_peer_t* peer = &c->peers[i];
        if (!peer->active) continue;

        bool addr_match = strcmp(peer->addr, addr) == 0 && peer->port == port;
        bool id_match = node_id && node_id[0] != '\0' &&
                        peer->node_id[0] != '\0' &&
                        strcmp(peer->node_id, node_id) == 0;
        if (!addr_match && !id_match) continue;

        if (addr_match && node_id && node_id[0] != '\0' && peer->node_id[0] == '\0') {
            size_t len = strnlen(node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
            memcpy(peer->node_id, node_id, len);
            peer->node_id[len] = '\0';
        }
        if (peer->source == KEEL_PEER_SOURCE_BOOTSTRAP &&
            source != KEEL_PEER_SOURCE_BOOTSTRAP) {
            peer->source = (uint8_t)source;
        }
        pthread_mutex_unlock(&c->peer_lock);
        return 0;
    }

    size_t slot = c->peer_count;
    for (size_t i = 0; i < c->peer_count; i++) {
        if (!c->peers[i].active) {
            slot = i;
            break;
        }
    }

    if (slot >= KEEL_CLUSTER_MAX_PEERS) {
        pthread_mutex_unlock(&c->peer_lock);
        return -1;
    }

    keel_cluster_peer_t* peer = &c->peers[slot];
    memset(peer, 0, sizeof(*peer));
    size_t addr_len = strnlen(addr, KEEL_CLUSTER_MAX_ADDR - 1);
    memcpy(peer->addr, addr, addr_len);
    peer->addr[addr_len] = '\0';
    peer->port = port;
    peer->active = true;
    peer->source = (uint8_t)source;
    peer->discovered_at_sec = cluster_now_sec();
    if (node_id && node_id[0] != '\0') {
        size_t id_len = strnlen(node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(peer->node_id, node_id, id_len);
        peer->node_id[id_len] = '\0';
    }
    atomic_store(&peer->status, KEEL_PEER_UNKNOWN);

    if (slot == c->peer_count) {
        c->peer_count++;
    }

    if (source == KEEL_PEER_SOURCE_JOIN || source == KEEL_PEER_SOURCE_GOSSIP) {
        atomic_fetch_add(&c->discovered_peers_total, 1);
    }

    pthread_mutex_unlock(&c->peer_lock);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] Discovered peer %s:%u source=%u%s%s",
        addr, port, (unsigned)source,
        (node_id && node_id[0] != '\0') ? " node_id=" : "",
        (node_id && node_id[0] != '\0') ? node_id : "");
    return 0;
}

/**
 * @brief Populate a sync-response payload with local config and peer snapshots.
 *
 * Fills @p resp with the current config checksum, heartbeat parameters, and
 * a snapshot of all active non-self peers (up to KEEL_CLUSTER_MAX_PEERS).
 * Fields are written in network byte order.
 *
 * @param c    Cluster instance (source of truth).
 * @param resp Output structure to populate.
 */
static void cluster_build_sync_response(keel_cluster_t* c,
                                        keel_cluster_sync_response_t* resp) {
    memset(resp, 0, sizeof(*resp));
    resp->config_checksum = htobe64(atomic_load(&c->config_checksum));
    resp->config.heartbeat_interval_ms = htonl(c->config.heartbeat_interval_ms);
    resp->config.heartbeat_timeout_ms = htonl(c->config.heartbeat_timeout_ms);
    resp->config.failure_threshold = htonl(c->config.failure_threshold);
    resp->config.auto_sync = c->config.auto_sync ? 1 : 0;

    uint16_t out = 0;
    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count && out < KEEL_CLUSTER_MAX_PEERS; i++) {
        const keel_cluster_peer_t* p = &c->peers[i];
        if (!p->active) continue;
        if (cluster_is_self(c, p->node_id, p->addr, p->port)) continue;

        keel_cluster_peer_snapshot_t* dst = &resp->peers[out++];
        memcpy(dst->node_id, p->node_id, sizeof(dst->node_id));
        memcpy(dst->addr, p->addr, sizeof(dst->addr));
        dst->port = htons(p->port);
        dst->status = (uint8_t)atomic_load(&p->status);
        dst->source = p->source;
    }
    pthread_mutex_unlock(&c->peer_lock);

    resp->peer_count = htons(out);
}

/**
 * @brief Merge a received sync-response into local cluster state.
 *
 * Applies the remote config via cluster_apply_sync_config() and enrolls any
 * previously unknown peers from the gossip peer list.
 *
 * @param c            Cluster instance.
 * @param resp         Received sync-response payload (network byte order).
 * @param from_node_id Node identifier of the peer that sent the response.
 * @return Number of newly discovered peers added to the local peer table.
 */
static size_t cluster_apply_sync_response(keel_cluster_t* c,
                                          const keel_cluster_sync_response_t* resp,
                                          const char* from_node_id) {
    if (!c || !resp) return 0;

    uint64_t remote_cksum = be64toh(resp->config_checksum);
    (void)cluster_apply_sync_config(c, &resp->config, from_node_id);
    if (remote_cksum != 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Applied sync payload from %s checksum=0x%016llx",
            from_node_id && from_node_id[0] != '\0' ? from_node_id : "(unknown)",
            (unsigned long long)remote_cksum);
    }

    atomic_store(&c->last_sync_apply_ns, cluster_now_ns());

    size_t discovered = 0;
    uint16_t peer_count = ntohs(resp->peer_count);
    if (peer_count > KEEL_CLUSTER_MAX_PEERS) peer_count = KEEL_CLUSTER_MAX_PEERS;

    for (uint16_t i = 0; i < peer_count; i++) {
        const keel_cluster_peer_snapshot_t* snap = &resp->peers[i];
        uint16_t port = ntohs(snap->port);
        if (snap->addr[0] == '\0' || port == 0) continue;
        if (cluster_is_self(c, snap->node_id, snap->addr, port)) continue;

        bool existed = cluster_has_peer(c, snap->addr, port, snap->node_id);

        if (cluster_add_or_update_peer(c,
                                       snap->addr,
                                       port,
                                       snap->node_id,
                                       KEEL_PEER_SOURCE_GOSSIP) == 0) {
            if (!existed) discovered++;
        }
    }

    return discovered;
}

/**
 * @brief Open a connection to a peer and perform a full config+gossip sync.
 *
 * Sends a KEEL_CLUSTER_MSG_SYNC_REQUEST and waits for a
 * KEEL_CLUSTER_MSG_SYNC_RESPONSE. The response is applied via
 * cluster_apply_sync_response(). Called when a config checksum mismatch or
 * peer-count discrepancy is detected during a heartbeat exchange.
 *
 * @param c      Cluster instance.
 * @param peer   Target peer to request sync from.
 * @param reason Short human-readable string logged with the sync event.
 * @return 0 on success, -1 if the connection or protocol exchange failed.
 */
static int cluster_request_sync_from_peer(keel_cluster_t* c,
                                          keel_cluster_peer_t* peer,
                                          const char* reason) {
    int fd = connect_to_peer(peer->addr, peer->port, c->config.heartbeat_timeout_ms);
    if (fd < 0) {
        return -1;
    }

    keel_cluster_sync_request_t req = {
        .requester_checksum = htobe64(atomic_load(&c->config_checksum)),
        .known_peer_count = htons(cluster_active_peer_count(c)),
        .include_peers = 1,
        ._pad = 0,
    };

    if (send_msg(fd, KEEL_CLUSTER_MSG_SYNC_REQUEST, &req, sizeof(req), c->config.node_id, CLUSTER_CODEC(c)) < 0) {
        close(fd);
        return -1;
    }
    atomic_fetch_add(&c->sync_requests_sent, 1);

    keel_cluster_msg_header_t resp_hdr;
    keel_cluster_sync_response_t resp;
    int rc = recv_msg(fd, &resp_hdr, &resp, sizeof(resp), c->config.heartbeat_timeout_ms);
    close(fd);

    if (rc < 0 || resp_hdr.msg_type != KEEL_CLUSTER_MSG_SYNC_RESPONSE) {
        return -1;
    }

    size_t merged = cluster_apply_sync_response(c, &resp, resp_hdr.node_id);
    if (merged > 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Sync from %s:%u (%s) merged %zu peers",
            peer->addr, peer->port, reason ? reason : "periodic", merged);
    }
    return 0;
}

/**
 * @brief Send a KEEL_CLUSTER_MSG_JOIN message to a single peer.
 *
 * Connects to the peer, sends this node's listen address and port so the
 * peer can add us to its own peer table, then closes the connection.
 * Connection failures are silently ignored.
 *
 * @param c    Cluster instance.
 * @param peer Target peer to notify.
 */
static void cluster_send_join_to_peer(keel_cluster_t* c, keel_cluster_peer_t* peer) {
    int fd = connect_to_peer(peer->addr, peer->port, c->config.heartbeat_timeout_ms);
    if (fd < 0) {
        return;
    }

    keel_cluster_join_t join;
    memset(&join, 0, sizeof(join));
    join.listen_port = htons(c->config.listen_port);
    size_t addr_len = strnlen(c->config.listen_addr, sizeof(join.listen_addr) - 1);
    memcpy(join.listen_addr, c->config.listen_addr, addr_len);
    join.listen_addr[addr_len] = '\0';

    (void)send_msg(fd, KEEL_CLUSTER_MSG_JOIN, &join, sizeof(join), c->config.node_id, CLUSTER_CODEC(c));
    close(fd);
}

/**
 * @brief Broadcast a JOIN announcement to all currently known peers.
 *
 * Snapshots the active peer index list under peer_lock, then calls
 * cluster_send_join_to_peer() for each peer outside the lock so that
 * the accept loop is not blocked during the broadcast.
 *
 * @param c Cluster instance.
 */
static void cluster_broadcast_join(keel_cluster_t* c) {
    if (!c) return;

    size_t join_targets[KEEL_CLUSTER_MAX_PEERS];
    size_t join_count = 0;

    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i].active) {
            join_targets[join_count++] = i;
        }
    }
    pthread_mutex_unlock(&c->peer_lock);

    for (size_t i = 0; i < join_count; i++) {
        cluster_send_join_to_peer(c, &c->peers[join_targets[i]]);
    }
}

/* ============================================================================
 * Leader Election — Persistent State
 * ============================================================================ */

/**
 * @brief Write term and voted_for to disk atomically (rename over temp file).
 *
 * Called after incrementing the term or granting a vote to ensure the
 * election invariant "vote at most once per term" survives a process restart.
 * Silently ignores errors — the worst outcome is a redundant vote on restart,
 * which is acceptable since the network partitions that would exploit it are
 * unlikely in the restart window.
 *
 * @param c Cluster instance.
 */

/* Forward declarations for VIP helpers (defined in Phase 3 section below) */
static void cluster_vip_acquire(keel_cluster_t* c);
static void cluster_vip_release(keel_cluster_t* c);

static void cluster_save_persistent_state(keel_cluster_t* c) {
    if (!c || c->config.election_state_path[0] == '\0') return;

    char tmp_path[264];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", c->config.election_state_path);

    FILE* f = fopen(tmp_path, "w");
    if (!f) return;

    uint64_t term = atomic_load(&c->term);
    pthread_mutex_lock(&c->election_lock);
    fprintf(f, "term=%" PRIu64 "\nvoted_for=%s\n", term, c->voted_for);
    pthread_mutex_unlock(&c->election_lock);
    fclose(f);

    rename(tmp_path, c->config.election_state_path);
}

/**
 * @brief Load persisted election state (term, voted_for) from disk.
 *
 * Called once during keel_cluster_create().  A missing or corrupt file is
 * silently ignored — the node starts at term 0 with no prior vote.
 *
 * @param c Cluster instance.
 */
static void cluster_load_persistent_state(keel_cluster_t* c) {
    if (!c || c->config.election_state_path[0] == '\0') return;

    FILE* f = fopen(c->config.election_state_path, "r");
    if (!f) return;

    uint64_t term = 0;
    char voted_for[KEEL_CLUSTER_MAX_NODE_ID];
    memset(voted_for, 0, sizeof(voted_for));

    char line[KEEL_CLUSTER_MAX_NODE_ID + 20];
    while (fgets(line, sizeof(line), f)) {
        uint64_t t;
        if (sscanf(line, "term=%" SCNu64, &t) == 1) {
            term = t;
        } else if (strncmp(line, "voted_for=", 10) == 0) {
            size_t len = strnlen(line + 10, KEEL_CLUSTER_MAX_NODE_ID - 1);
            while (len > 0 && (line[10 + len - 1] == '\n' || line[10 + len - 1] == '\r'))
                len--;
            memcpy(voted_for, line + 10, len);
            voted_for[len] = '\0';
        }
    }
    fclose(f);

    if (term > 0) {
        atomic_store(&c->term, term);
        pthread_mutex_lock(&c->election_lock);
        size_t vlen = strnlen(voted_for, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(c->voted_for, voted_for, vlen);
        c->voted_for[vlen] = '\0';
        pthread_mutex_unlock(&c->election_lock);

        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Loaded election state: term=%" PRIu64 " voted_for=%s",
            term, voted_for[0] ? voted_for : "(none)");
    }
}

/* ============================================================================
 * Leader Election — Role Transitions
 * ============================================================================ */

/**
 * @brief Count total active nodes in this cluster (self + non-LEFT peers).
 *
 * Used to compute the majority quorum threshold.
 *
 * @param c Cluster instance.
 * @return Number of active nodes (always >= 1).
 */
static int cluster_count_active_nodes(const keel_cluster_t* c) {
    int count = 1;  /* self */
    pthread_mutex_lock(&((keel_cluster_t*)c)->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i].active &&
            atomic_load(&c->peers[i].status) != KEEL_PEER_LEFT) {
            count++;
        }
    }
    pthread_mutex_unlock(&((keel_cluster_t*)c)->peer_lock);
    return count;
}

/**
 * @brief Transition to FOLLOWER, updating the term if it advanced.
 *
 * Safe to call from any state.  Clears voted_for when the term advances so
 * the node can vote freely in the new term.  Does NOT reset
 * last_leader_contact_ns — callers that know a valid leader should call
 * cluster_recognize_leader() instead.
 *
 * @param c        Cluster instance.
 * @param new_term Term to step down into (must be >= current term).
 */
static void cluster_step_down(keel_cluster_t* c, uint64_t new_term) {
    uint64_t old_term = atomic_load(&c->term);

    if (new_term > old_term) {
        atomic_store(&c->term, new_term);
        pthread_mutex_lock(&c->election_lock);
        memset(c->voted_for, 0, sizeof(c->voted_for));
        pthread_mutex_unlock(&c->election_lock);
        cluster_save_persistent_state(c);
    }

    int old_role = atomic_load(&c->role);
    if (old_role != KEEL_CLUSTER_ROLE_FOLLOWER) {
        if (old_role == KEEL_CLUSTER_ROLE_LEADER) {
            cluster_vip_release(c);
            atomic_fetch_add(&c->leader_stepdowns, 1);
        }
        atomic_store(&c->role, KEEL_CLUSTER_ROLE_FOLLOWER);
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Stepped down to follower (term %" PRIu64 ")", new_term);
    }
}

/**
 * @brief Acknowledge a remote node as the current cluster leader.
 *
 * Updates the local term if needed, steps down from CANDIDATE or LEADER,
 * records the leader identity, and resets the election timeout so the node
 * does not start a spurious election while a valid leader is active.
 *
 * @param c         Cluster instance.
 * @param term      Leader's term (must be >= local term to be recognized).
 * @param leader_id Node ID of the leader (copied into local state).
 */
static void cluster_recognize_leader(keel_cluster_t* c,
                                     uint64_t term,
                                     const char* leader_id) {
    uint64_t local_term = atomic_load(&c->term);

    if (term > local_term) {
        atomic_store(&c->term, term);
        pthread_mutex_lock(&c->election_lock);
        memset(c->voted_for, 0, sizeof(c->voted_for));
        pthread_mutex_unlock(&c->election_lock);
        cluster_save_persistent_state(c);
    }

    int old_role = atomic_load(&c->role);
    if (old_role == KEEL_CLUSTER_ROLE_LEADER) {
        cluster_vip_release(c);
        atomic_fetch_add(&c->leader_stepdowns, 1);
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Stepping down: recognized %s as leader for term %" PRIu64,
            leader_id ? leader_id : "(unknown)", term);
    } else if (old_role == KEEL_CLUSTER_ROLE_CANDIDATE) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Abandoning candidacy: %s is leader for term %" PRIu64,
            leader_id ? leader_id : "(unknown)", term);
    }
    if (old_role != KEEL_CLUSTER_ROLE_FOLLOWER)
        atomic_store(&c->role, KEEL_CLUSTER_ROLE_FOLLOWER);

    pthread_mutex_lock(&c->election_lock);
    if (leader_id && leader_id[0] != '\0') {
        size_t len = strnlen(leader_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(c->leader_id, leader_id, len);
        c->leader_id[len] = '\0';
    }
    pthread_mutex_unlock(&c->election_lock);

    atomic_store(&c->last_leader_contact_ns, cluster_now_ns());
}

/**
 * @brief Transition to LEADER for the given term.
 *
 * Records the election win, sets leader_id to self, and logs the result.
 *
 * @param c    Cluster instance.
 * @param term Winning election term.
 */
static void cluster_become_leader(keel_cluster_t* c, uint64_t term) {
    atomic_store(&c->role, KEEL_CLUSTER_ROLE_LEADER);

    pthread_mutex_lock(&c->election_lock);
    size_t id_len = strnlen(c->config.node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
    memcpy(c->leader_id, c->config.node_id, id_len);
    c->leader_id[id_len] = '\0';
    pthread_mutex_unlock(&c->election_lock);

    atomic_fetch_add(&c->elections_won, 1);
    /* Leader considers itself in contact with itself */
    atomic_store(&c->last_leader_contact_ns, cluster_now_ns());

    /* Acquire the floating VIP (no-op if vip is empty) */
    cluster_vip_acquire(c);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] Elected leader for term %" PRIu64 " (node=%s)",
        term, c->config.node_id);
}

/* ============================================================================
 * Leader Election — Vote Exchange
 * ============================================================================ */

/**
 * @brief Send a VOTE_REQUEST to a single peer and collect the response.
 *
 * Uses half the heartbeat_timeout_ms as the per-message deadline so that an
 * election round completes in well under one heartbeat interval even for
 * clusters with high-latency links.
 *
 * Side-effect: if the response carries a higher term, calls cluster_step_down()
 * which reverts this node to FOLLOWER, preventing the caller from winning.
 *
 * @param c    Cluster instance.
 * @param peer Target peer.
 * @param term The term this node is campaigning for.
 * @return true if the vote was granted.
 */
static bool cluster_send_vote_request(keel_cluster_t* c,
                                      keel_cluster_peer_t* peer,
                                      uint64_t term) {
    /* Use a short timeout proportional to heartbeat_interval_ms.
     * This keeps the election blocking window small relative to the HB cycle
     * and prevents elections from starving the accept/drain loop on heavily
     * loaded systems or in tests where multiple operations race. */
    uint32_t timeout = c->config.heartbeat_interval_ms * 2;
    if (timeout < 150)  timeout = 150;
    if (timeout > 2000) timeout = 2000;

    int fd = connect_to_peer(peer->addr, peer->port, timeout);
    if (fd < 0) return false;

    keel_cluster_vote_request_t req;
    memset(&req, 0, sizeof(req));
    req.term = htobe64(term);
    size_t id_len = strnlen(c->config.node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
    memcpy(req.candidate_id, c->config.node_id, id_len);

    if (send_msg(fd, KEEL_CLUSTER_MSG_VOTE_REQUEST, &req, sizeof(req),
                 c->config.node_id, CLUSTER_CODEC(c)) < 0) {
        close(fd);
        return false;
    }

    keel_cluster_msg_header_t resp_hdr;
    keel_cluster_vote_response_t resp;
    memset(&resp, 0, sizeof(resp));
    int rc = recv_msg(fd, &resp_hdr, &resp, sizeof(resp), timeout);
    close(fd);

    if (rc < 0 || resp_hdr.msg_type != KEEL_CLUSTER_MSG_VOTE_RESPONSE)
        return false;

    uint64_t resp_term = be64toh(resp.term);
    if (resp_term > term) {
        cluster_step_down(c, resp_term);
        return false;
    }

    return resp.vote_granted != 0;
}

/**
 * @brief Run one full leader election round.
 *
 * Increments the term, votes for self, requests votes from all active
 * non-LEFT peers, and calls cluster_become_leader() if a majority is reached.
 * On failure or if outranked mid-election, reverts to FOLLOWER.
 *
 * This function blocks the cluster thread for up to
 * O(peers) × heartbeat_timeout_ms/2.  For the typical 3-node deployment that
 * is ≤ 2 × 2.5 s = 5 s worst-case, but usually < 20 ms on a LAN.
 *
 * @param c Cluster instance.
 */
static void cluster_run_election(keel_cluster_t* c) {
    if (!c->config.election_enabled) return;

    /* Increment term and vote for self */
    uint64_t new_term = atomic_fetch_add(&c->term, 1) + 1;

    pthread_mutex_lock(&c->election_lock);
    size_t id_len = strnlen(c->config.node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
    memcpy(c->voted_for, c->config.node_id, id_len);
    c->voted_for[id_len] = '\0';
    memset(c->leader_id, 0, sizeof(c->leader_id));
    pthread_mutex_unlock(&c->election_lock);

    cluster_save_persistent_state(c);
    atomic_store(&c->role, KEEL_CLUSTER_ROLE_CANDIDATE);
    atomic_fetch_add(&c->elections_started, 1);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] Starting election for term %" PRIu64, new_term);

    /* Snapshot peer indices under the lock */
    size_t peer_indices[KEEL_CLUSTER_MAX_PEERS];
    size_t peer_count = 0;
    int total_nodes = 1;  /* self */

    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i].active &&
            atomic_load(&c->peers[i].status) != KEEL_PEER_LEFT) {
            peer_indices[peer_count++] = i;
            total_nodes++;
        }
    }
    pthread_mutex_unlock(&c->peer_lock);

    int quorum = total_nodes / 2 + 1;

    /* Single-node cluster — auto-elect immediately */
    if (total_nodes == 1) {
        cluster_become_leader(c, new_term);
        return;
    }

    int votes = 1;  /* own vote */

    for (size_t i = 0; i < peer_count; i++) {
        /* Abort if we've been outranked by an incoming message */
        if (atomic_load(&c->role) != KEEL_CLUSTER_ROLE_CANDIDATE)
            return;

        bool granted = cluster_send_vote_request(
            c, &c->peers[peer_indices[i]], new_term);

        if (granted) {
            votes++;
            const char* peer_name = c->peers[peer_indices[i]].node_id[0]
                ? c->peers[peer_indices[i]].node_id
                : c->peers[peer_indices[i]].addr;
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE,
                "[cluster] Vote granted by %s (term %" PRIu64
                ", votes=%d/%d needed=%d)",
                peer_name, new_term, votes, total_nodes, quorum);
        }

        if (votes >= quorum) {
            cluster_become_leader(c, new_term);
            return;
        }
    }

    /* Did not reach quorum */
    if (atomic_load(&c->role) == KEEL_CLUSTER_ROLE_CANDIDATE) {
        atomic_store(&c->role, KEEL_CLUSTER_ROLE_FOLLOWER);
        /* Back off before next attempt */
        atomic_store(&c->last_leader_contact_ns, cluster_now_ns());
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Election for term %" PRIu64
            " failed: got %d/%d votes (need %d)",
            new_term, votes, total_nodes, quorum);
    }
}

/* ============================================================================
 * Heartbeat Logic
 * ============================================================================ */

/**
 * @brief Build a heartbeat payload from the current local cluster state.
 *
 * Populates all fields of @p hb (config checksum, uptime, connection counts,
 * active peer count, and node state) in network byte order.
 *
 * @param c  Cluster instance.
 * @param hb Output heartbeat structure to populate.
 */

/* ============================================================================
 * Phase 3 — Floating VIP (Linux only)
 * ============================================================================ */

/**
 * @brief Fork-exec a command and wait for it to finish.
 *
 * stdin/stdout/stderr are redirected to /dev/null.  Returns the exit status,
 * or -1 on fork/exec failure.
 */
static int cluster_run_cmd(const char* prog, const char* const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        int dn = open("/dev/null", O_RDWR);
        if (dn >= 0) {
            dup2(dn, STDIN_FILENO);
            dup2(dn, STDOUT_FILENO);
            dup2(dn, STDERR_FILENO);
            close(dn);
        }
        execvp(prog, (char* const*)argv);
        _exit(127);
    }
    int status;
    if (waitpid(pid, &status, 0) < 0) return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

#ifdef __linux__
/**
 * @brief Acquire the configured VIP on the local interface.
 *
 * Runs:  ip addr add <vip> dev <interface>
 *        arping -U -q -I <interface> -c 3 <ip>   (gratuitous ARP)
 *
 * Requires CAP_NET_ADMIN.  No-ops if vip or vip_interface is empty.
 */
static void cluster_vip_acquire(keel_cluster_t* c) {
    if (!c->config.vip[0] || !c->config.vip_interface[0]) return;

    const char* add_argv[] = {
        "ip", "addr", "add", c->config.vip,
        "dev", c->config.vip_interface, NULL
    };
    int rc = cluster_run_cmd("ip", add_argv);
    /* rc==2 means address already exists (RTNETLINK answers: File exists) — OK */
    if (rc != 0 && rc != 2) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[cluster] VIP acquire failed: ip addr add returned %d", rc);
        return;
    }
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] VIP %s acquired on %s",
        c->config.vip, c->config.vip_interface);

    /* Gratuitous ARP — strip /prefixlen to get bare IP for arping */
    char ip_only[64];
    snprintf(ip_only, sizeof(ip_only), "%s", c->config.vip);
    char* slash = strchr(ip_only, '/');
    if (slash) *slash = '\0';

    const char* arp_argv[] = {
        "arping", "-U", "-q", "-I", c->config.vip_interface,
        "-c", "3", ip_only, NULL
    };
    (void)cluster_run_cmd("arping", arp_argv);
}

/**
 * @brief Release the configured VIP from the local interface.
 *
 * Runs: ip addr del <vip> dev <interface>
 */
static void cluster_vip_release(keel_cluster_t* c) {
    if (!c->config.vip[0] || !c->config.vip_interface[0]) return;

    const char* del_argv[] = {
        "ip", "addr", "del", c->config.vip,
        "dev", c->config.vip_interface, NULL
    };
    int rc = cluster_run_cmd("ip", del_argv);
    if (rc != 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[cluster] VIP release failed: ip addr del returned %d", rc);
        return;
    }
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] VIP %s released from %s",
        c->config.vip, c->config.vip_interface);
}
#else
static void cluster_vip_acquire(keel_cluster_t* c) { (void)c; }
static void cluster_vip_release(keel_cluster_t* c) { (void)c; }
#endif /* __linux__ */

static void build_heartbeat(const keel_cluster_t* c, keel_cluster_heartbeat_t* hb) {
    uint64_t now = cluster_now_ns();
    uint64_t uptime = (now - c->start_time_ns) / 1000000000ULL;

    hb->config_checksum = htobe64(atomic_load(&c->config_checksum));
    hb->uptime_sec = htobe64(uptime);
    hb->term = htobe64(atomic_load(&c->term));
    uint32_t cli = 0, be = 0, srv = 0;
    if (c->stats_cb)
        c->stats_cb(c->stats_cb_data, &cli, &be, &srv);
    hb->num_clients  = htonl(cli);
    hb->num_backends = htonl(be);
    hb->num_servers  = htonl(srv);
    hb->num_peers = htons(cluster_active_peer_count(c));
    hb->state = 1;                 /* ACTIVE */
    hb->role  = (uint8_t)atomic_load(&c->role);
    pthread_mutex_lock(&((keel_cluster_t*)c)->election_lock);
    memcpy(hb->leader_id, c->leader_id, sizeof(hb->leader_id));
    pthread_mutex_unlock(&((keel_cluster_t*)c)->election_lock);
}

/**
 * @brief Send a heartbeat to a peer and process the acknowledgement.
 *
 * Connects to @p peer, sends a HEARTBEAT message, and waits for a
 * HEARTBEAT_ACK. On success, updates the peer's status to KEEL_PEER_UP and
 * records latency and state fields. On failure, increments failure counters
 * and transitions the peer to KEEL_PEER_SUSPECT or KEEL_PEER_DOWN based on
 * the configured failure threshold. If a config checksum mismatch or peer
 * count discrepancy is detected in the ACK, triggers a sync request.
 *
 * @param c    Cluster instance.
 * @param peer Target peer to heartbeat.
 */
static void send_heartbeat_to_peer(keel_cluster_t* c, keel_cluster_peer_t* peer) {
    uint64_t start = cluster_now_ns();

    int fd = connect_to_peer(peer->addr, peer->port, c->config.heartbeat_timeout_ms);
    if (fd < 0) {
        uint32_t fails = atomic_fetch_add(&peer->consecutive_failures, 1) + 1;
        atomic_fetch_add(&peer->total_failures, 1);
        if (fails >= c->config.failure_threshold)
            atomic_store(&peer->status, KEEL_PEER_DOWN);
        else
            atomic_store(&peer->status, KEEL_PEER_SUSPECT);
        return;
    }

    keel_cluster_heartbeat_t hb;
    build_heartbeat(c, &hb);

    int rc = send_msg(fd, KEEL_CLUSTER_MSG_HEARTBEAT, &hb, sizeof(hb),
                      c->config.node_id, CLUSTER_CODEC(c));
    if (rc < 0) {
        close(fd);
        atomic_fetch_add(&peer->consecutive_failures, 1);
        atomic_fetch_add(&peer->total_failures, 1);
        return;
    }

    atomic_fetch_add(&c->heartbeats_sent, 1);

    /* Wait for ACK — use a shorter timeout than heartbeat_timeout_ms
     * so we don't block the accept loop for too long when two nodes
     * send heartbeats simultaneously. */
    keel_cluster_msg_header_t resp_hdr;
    keel_cluster_heartbeat_t resp_hb;
    int ack_timeout = c->config.heartbeat_interval_ms;
    if (ack_timeout < 500) ack_timeout = 500;
    rc = recv_msg(fd, &resp_hdr, &resp_hb, sizeof(resp_hb), ack_timeout);
    close(fd);

    if (rc < 0) {
        /* Don't downgrade a peer that was recently marked UP by the
         * incoming heartbeat handler (handles simultaneous send where
         * the outgoing ACK times out but the incoming heartbeat succeeds). */
        int current = atomic_load(&peer->status);
        if (current != KEEL_PEER_UP) {
            atomic_fetch_add(&peer->consecutive_failures, 1);
            atomic_fetch_add(&peer->total_failures, 1);
            uint32_t fails = atomic_load(&peer->consecutive_failures);
            if (fails >= c->config.failure_threshold)
                atomic_store(&peer->status, KEEL_PEER_DOWN);
            else
                atomic_store(&peer->status, KEEL_PEER_SUSPECT);
        }
        return;
    }

    /* Success — update peer state */
    uint64_t end = cluster_now_ns();
    uint64_t latency_us = (end - start) / 1000;

    atomic_store(&peer->consecutive_failures, 0);
    atomic_store(&peer->last_heartbeat_ns, end);
    atomic_store(&peer->last_latency_us, latency_us);
    atomic_fetch_add(&peer->total_heartbeats, 1);
    atomic_fetch_add(&c->heartbeats_received, 1);

    /* Parse peer state from ACK */
    if (resp_hdr.msg_type == KEEL_CLUSTER_MSG_HEARTBEAT_ACK) {
        atomic_store(&peer->config_checksum, be64toh(resp_hb.config_checksum));
        atomic_store(&peer->uptime_sec, be64toh(resp_hb.uptime_sec));
        atomic_store(&peer->num_clients, ntohl(resp_hb.num_clients));
        atomic_store(&peer->num_backends, ntohl(resp_hb.num_backends));
        atomic_store(&peer->engine_state, resp_hb.state);

        /* Detect config mismatch and trigger a sync request.
         *
         * Cooldown guard: both nodes simultaneously heartbeat each other,
         * which means A's cluster thread blocks inside cluster_request_sync_from_peer
         * (waiting for B's SYNC_RESPONSE) while B's heartbeat to A is waiting for
         * an ACK that A cannot send (A is blocked). B's ACK wait times out so B
         * never reads A's checksum from A's ACK, breaking reconciliation.
         *
         * Fix: only issue one sync request per peer per heartbeat_timeout_ms window.
         * This ensures the cluster thread is idle often enough for the other side
         * to land a successful heartbeat and detect the mismatch in its own turn.
         */
        uint64_t local_cksum = atomic_load(&c->config_checksum);
        uint64_t peer_cksum = atomic_load(&peer->config_checksum);
        if (c->config.auto_sync && peer_cksum != local_cksum &&
            peer_cksum != 0 && local_cksum != 0) {
            uint64_t now_ns2 = cluster_now_ns();
            uint64_t last_sync = atomic_load(&peer->last_sync_request_ns);
            uint64_t cooldown_ns = (uint64_t)c->config.heartbeat_timeout_ms * 1000000ULL;
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[cluster] Config mismatch with peer %s:%u "
                "(local=0x%016llx, peer=0x%016llx)",
                peer->addr, peer->port,
                (unsigned long long)local_cksum,
                (unsigned long long)peer_cksum);
            if (now_ns2 - last_sync >= cooldown_ns) {
                atomic_store(&peer->last_sync_request_ns, now_ns2);
                (void)cluster_request_sync_from_peer(c, peer, "checksum-mismatch");
            }
        } else if (c->config.auto_sync) {
            uint16_t remote_peers = ntohs(resp_hb.num_peers);
            uint16_t local_peers = cluster_active_peer_count(c);
            if (remote_peers > local_peers) {
                (void)cluster_request_sync_from_peer(c, peer, "peer-gossip");
            }
        }
    }

    /* Mark UP last so readers see all updated fields once status flips. */
    atomic_store(&peer->status, KEEL_PEER_UP);

    /* Copy node_id from response if peer hasn't identified yet */
    pthread_mutex_lock(&c->peer_lock);
    if (peer->node_id[0] == '\0' && resp_hdr.node_id[0] != '\0') {
        size_t len = strnlen(resp_hdr.node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(peer->node_id, resp_hdr.node_id, len);
        peer->node_id[len] = '\0';
    }
    pthread_mutex_unlock(&c->peer_lock);
}

/* ============================================================================
 * Incoming Connection Handler
 * ============================================================================ */

/**
 * @brief Dispatch an incoming cluster message received on an accepted fd.
 *
 * Reads the wire-protocol header and payload from @p client_fd, then
 * switches on the message type:
 *   - HEARTBEAT: sends an ACK and updates the matching peer's health state.
 *   - JOIN: registers the new peer and sends a HEARTBEAT_ACK.
 *   - SYNC_REQUEST: builds and sends a full sync response.
 *   - SYNC_RESPONSE: merges the payload into local state.
 *   - LEAVE: marks the departing peer as KEEL_PEER_LEFT.
 *   - NOTIFY_SERVER: deserialises and dispatches to the registered `server_notify_cb`.
 * Closes @p client_fd before returning.
 *
 * @param c         Cluster instance.
 * @param client_fd Accepted TCP socket (ownership transferred; always closed).
 */
static void handle_incoming(keel_cluster_t* c, int client_fd) {
    keel_cluster_msg_header_t hdr;
    uint8_t payload[sizeof(keel_cluster_sync_response_t)];

    if (recv_msg(client_fd, &hdr, payload, sizeof(payload), 3000) < 0) {
        close(client_fd);
        return;
    }

    switch (hdr.msg_type) {
    case KEEL_CLUSTER_MSG_HEARTBEAT: {
        /* Respond with ACK containing our state */
        keel_cluster_heartbeat_t ack;
        build_heartbeat(c, &ack);
        send_msg(client_fd, KEEL_CLUSTER_MSG_HEARTBEAT_ACK, &ack, sizeof(ack),
                 c->config.node_id, CLUSTER_CODEC(c));
        atomic_fetch_add(&c->heartbeats_received, 1);

        /* Update peer info if we know this sender.
         * First try matching by node_id; fall back to source address
         * for bootstrap peers that don't yet have a node_id recorded. */
        char sender_id[KEEL_CLUSTER_MAX_NODE_ID];
        size_t id_len = strnlen(hdr.node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(sender_id, hdr.node_id, id_len);
        sender_id[id_len] = '\0';

        /* Resolve the source IP address for fallback matching */
        char src_addr[KEEL_CLUSTER_MAX_ADDR] = {0};
        struct sockaddr_in sa;
        socklen_t sa_len = sizeof(sa);
        if (getpeername(client_fd, (struct sockaddr*)&sa, &sa_len) == 0)
            inet_ntop(AF_INET, &sa.sin_addr, src_addr, sizeof(src_addr));

        pthread_mutex_lock(&c->peer_lock);
        ssize_t match = -1;

        /* Pass 1: exact node_id match */
        if (sender_id[0] != '\0') {
            for (size_t i = 0; i < c->peer_count; i++) {
                if (c->peers[i].active &&
                    strcmp(c->peers[i].node_id, sender_id) == 0) {
                    match = (ssize_t)i;
                    break;
                }
            }
        }

        /* Pass 2: match by source IP for peers with empty node_id */
        if (match < 0 && src_addr[0] != '\0') {
            for (size_t i = 0; i < c->peer_count; i++) {
                if (c->peers[i].active &&
                    c->peers[i].node_id[0] == '\0' &&
                    strcmp(c->peers[i].addr, src_addr) == 0) {
                    match = (ssize_t)i;
                    break;
                }
            }
        }

        if (match >= 0) {
            keel_cluster_peer_t* p = &c->peers[match];
            keel_cluster_heartbeat_t* hb =
                (keel_cluster_heartbeat_t*)payload;
            atomic_store(&p->config_checksum, be64toh(hb->config_checksum));
            atomic_store(&p->uptime_sec, be64toh(hb->uptime_sec));
            atomic_store(&p->num_clients, ntohl(hb->num_clients));
            atomic_store(&p->num_backends, ntohl(hb->num_backends));
            atomic_store(&p->engine_state, hb->state);
            /* Mark UP last so readers see updated fields. */
            atomic_store(&p->status, KEEL_PEER_UP);
            atomic_store(&p->consecutive_failures, 0);
            atomic_store(&p->last_heartbeat_ns, cluster_now_ns());

            /* Learn the peer's node_id on first contact */
            if (p->node_id[0] == '\0' && sender_id[0] != '\0') {
                size_t len = strnlen(sender_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
                memcpy(p->node_id, sender_id, len);
                p->node_id[len] = '\0';
            }
        }
        pthread_mutex_unlock(&c->peer_lock);

        /* Process election state embedded in the heartbeat payload.
         * If the sender claims to be LEADER in a term >= ours, recognize
         * it as the current leader and reset the election timeout. */
        if (c->config.election_enabled) {
            keel_cluster_heartbeat_t* hb_in = (keel_cluster_heartbeat_t*)payload;
            uint64_t peer_term = be64toh(hb_in->term);
            uint8_t  peer_role = hb_in->role;
            if (peer_term > 0) {
                uint64_t local_term = atomic_load(&c->term);
                if (peer_role == KEEL_CLUSTER_ROLE_LEADER &&
                    peer_term >= local_term) {
                    char lid[KEEL_CLUSTER_MAX_NODE_ID];
                    size_t ll = strnlen(hb_in->leader_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
                    memcpy(lid, hb_in->leader_id, ll);
                    lid[ll] = '\0';
                    cluster_recognize_leader(c, peer_term,
                                             lid[0] ? lid : sender_id);
                } else if (peer_term > local_term) {
                    cluster_step_down(c, peer_term);
                }
            }
        }
        break;
    }

    case KEEL_CLUSTER_MSG_JOIN: {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Peer JOIN from %s", hdr.node_id);
        keel_cluster_join_t join = {0};
        if (ntohs(hdr.payload_len) >= sizeof(join)) {
            memcpy(&join, payload, sizeof(join));
        }

        char sender_id[KEEL_CLUSTER_MAX_NODE_ID];
        size_t id_len = strnlen(hdr.node_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(sender_id, hdr.node_id, id_len);
        sender_id[id_len] = '\0';

        char src_addr[KEEL_CLUSTER_MAX_ADDR] = {0};
        struct sockaddr_in sa;
        socklen_t sa_len = sizeof(sa);
        if (getpeername(client_fd, (struct sockaddr*)&sa, &sa_len) == 0) {
            inet_ntop(AF_INET, &sa.sin_addr, src_addr, sizeof(src_addr));
        }

        uint16_t peer_port = ntohs(join.listen_port);
        const char* peer_addr = join.listen_addr[0] != '\0' ? join.listen_addr : src_addr;
        if (peer_addr[0] != '\0' && peer_port != 0 &&
            !cluster_is_self(c, sender_id, peer_addr, peer_port)) {
            (void)cluster_add_or_update_peer(c,
                                             peer_addr,
                                             peer_port,
                                             sender_id,
                                             KEEL_PEER_SOURCE_JOIN);
        }

        /* ACK the join */
        keel_cluster_heartbeat_t ack;
        build_heartbeat(c, &ack);
        send_msg(client_fd, KEEL_CLUSTER_MSG_HEARTBEAT_ACK, &ack, sizeof(ack),
                 c->config.node_id, CLUSTER_CODEC(c));
        break;
    }

    case KEEL_CLUSTER_MSG_SYNC_REQUEST: {
        keel_cluster_sync_response_t resp;
        cluster_build_sync_response(c, &resp);
        send_msg(client_fd,
                 KEEL_CLUSTER_MSG_SYNC_RESPONSE,
                 &resp,
                 sizeof(resp),
                 c->config.node_id, CLUSTER_CODEC(c));
        atomic_fetch_add(&c->sync_responses_sent, 1);
        break;
    }

    case KEEL_CLUSTER_MSG_SYNC_RESPONSE: {
        if (ntohs(hdr.payload_len) >= sizeof(keel_cluster_sync_response_t)) {
            keel_cluster_sync_response_t* resp =
                (keel_cluster_sync_response_t*)payload;
            (void)cluster_apply_sync_response(c, resp, hdr.node_id);
        }
        break;
    }

    case KEEL_CLUSTER_MSG_LEAVE: {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Peer LEAVE from %s", hdr.node_id);
        /* Mark peer as LEFT */
        pthread_mutex_lock(&c->peer_lock);
        for (size_t i = 0; i < c->peer_count; i++) {
            if (c->peers[i].active &&
                strcmp(c->peers[i].node_id, hdr.node_id) == 0) {
                atomic_store(&c->peers[i].status, KEEL_PEER_LEFT);
                break;
            }
        }
        pthread_mutex_unlock(&c->peer_lock);
        break;
    }

    case KEEL_CLUSTER_MSG_VOTE_REQUEST: {
        if (ntohs(hdr.payload_len) < sizeof(keel_cluster_vote_request_t))
            break;

        keel_cluster_vote_request_t* vreq = (keel_cluster_vote_request_t*)payload;
        uint64_t req_term = be64toh(vreq->term);

        char cand_id[KEEL_CLUSTER_MAX_NODE_ID];
        size_t clen = strnlen(vreq->candidate_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
        memcpy(cand_id, vreq->candidate_id, clen);
        cand_id[clen] = '\0';

        keel_cluster_vote_response_t vresp;
        memset(&vresp, 0, sizeof(vresp));
        bool grant = false;
        bool term_updated = false;

        pthread_mutex_lock(&c->election_lock);
        uint64_t local_term = atomic_load(&c->term);

        if (req_term > local_term) {
            /* Higher term: update, clear prior vote, step down */
            atomic_store(&c->term, req_term);
            memset(c->voted_for, 0, sizeof(c->voted_for));
            if (atomic_load(&c->role) != KEEL_CLUSTER_ROLE_FOLLOWER)
                atomic_store(&c->role, KEEL_CLUSTER_ROLE_FOLLOWER);
            term_updated = true;
            local_term = req_term;
        }

        if (req_term == local_term &&
            (c->voted_for[0] == '\0' ||
             strcmp(c->voted_for, cand_id) == 0)) {
            memcpy(c->voted_for, cand_id, clen);
            c->voted_for[clen] = '\0';
            grant = true;
        }
        pthread_mutex_unlock(&c->election_lock);

        if (term_updated)
            cluster_save_persistent_state(c);

        vresp.term = htobe64(atomic_load(&c->term));
        vresp.vote_granted = grant ? 1 : 0;

        if (grant) {
            /* Granting a vote counts as leader-timeout reset */
            atomic_store(&c->last_leader_contact_ns, cluster_now_ns());
            atomic_fetch_add(&c->votes_granted, 1);
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE,
                "[cluster] Granted vote to %s for term %" PRIu64,
                cand_id[0] ? cand_id : "(unknown)", req_term);
        } else {
            atomic_fetch_add(&c->votes_denied, 1);
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE,
                "[cluster] Denied vote to %s for term %" PRIu64
                " (already voted for %s)",
                cand_id[0] ? cand_id : "(unknown)", req_term,
                c->voted_for[0] ? c->voted_for : "(none)");
        }

        send_msg(client_fd, KEEL_CLUSTER_MSG_VOTE_RESPONSE,
                 &vresp, sizeof(vresp), c->config.node_id, CLUSTER_CODEC(c));
        break;
    }

    case KEEL_CLUSTER_MSG_VOTE_RESPONSE:
        /* Unsolicited VOTE_RESPONSE: discard (we're not mid-election here) */
        break;

    case KEEL_CLUSTER_MSG_CONFIG_PREPARE: {
        /* Follower side of 2PC: store pending state and reply with ACK/NACK.
         *
         * Accept if:
         *   - The prepare comes from the current leader (or a higher term),
         *   - We don't already have a conflicting pending transaction.
         * On accept: record pending_txn_id/checksum and reply accepted=1.
         * On reject: reply accepted=0 with our current term so the leader
         *            can step down if it has been displaced.
         */
        keel_cluster_config_ack_t ack;
        memset(&ack, 0, sizeof(ack));
        ack.term = htobe64(atomic_load(&c->term));

        if (ntohs(hdr.payload_len) >= sizeof(keel_cluster_config_prepare_t)) {
            keel_cluster_config_prepare_t* prep =
                (keel_cluster_config_prepare_t*)payload;
            uint64_t prep_term   = be64toh(prep->term);
            uint64_t prep_txn    = be64toh(prep->txn_id);
            uint64_t prep_cksum  = be64toh(prep->new_checksum);
            uint64_t local_term  = atomic_load(&c->term);

            if (prep_term >= local_term) {
                /* Accept */
                pthread_mutex_lock(&c->config_commit_lock);
                c->pending_txn_id    = prep_txn;
                c->pending_checksum  = prep_cksum;
                pthread_mutex_unlock(&c->config_commit_lock);

                ack.term     = htobe64(prep_term);
                ack.txn_id   = htobe64(prep_txn);
                ack.accepted = 1;

                /* Update our term if needed */
                if (prep_term > local_term)
                    atomic_store(&c->term, prep_term);
            } else {
                /* NACK — our term is higher, leader needs to step down */
                ack.txn_id   = htobe64(prep_txn);
                ack.accepted = 0;
            }
        }
        send_msg(client_fd, KEEL_CLUSTER_MSG_CONFIG_ACK,
                 &ack, sizeof(ack), c->config.node_id, CLUSTER_CODEC(c));
        break;
    }

    case KEEL_CLUSTER_MSG_CONFIG_COMMIT: {
        /* Follower side of 2PC: apply or discard the pending transaction. */
        if (ntohs(hdr.payload_len) >= sizeof(keel_cluster_config_commit_t)) {
            keel_cluster_config_commit_t* msg =
                (keel_cluster_config_commit_t*)payload;
            uint64_t commit_txn = be64toh(msg->txn_id);

            pthread_mutex_lock(&c->config_commit_lock);
            if (c->pending_txn_id == commit_txn && msg->commit) {
                /* The actual config payload is applied via gossip; here we
                 * just update the checksum hint and clear the pending slot. */
                if (c->pending_checksum != 0)
                    atomic_store(&c->config_checksum, c->pending_checksum);
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[cluster] 2PC commit applied (txn=%" PRIu64
                    ", checksum=0x%016llx)",
                    commit_txn, (unsigned long long)c->pending_checksum);
            }
            c->pending_txn_id   = 0;
            c->pending_checksum = 0;
            pthread_mutex_unlock(&c->config_commit_lock);
        }
        break;
    }

    case KEEL_CLUSTER_MSG_CONFIG_ACK:
        /* Unsolicited ACK: discard (not mid-commit here) */
        break;

    case KEEL_CLUSTER_MSG_NOTIFY_SERVER: {
        if (ntohs(hdr.payload_len) >= sizeof(keel_cluster_server_notify_t)) {
            keel_cluster_server_notify_t* notify =
                (keel_cluster_server_notify_t*)payload;
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[cluster] Server change from %s: action=%d host=%s port=%u",
                hdr.node_id, notify->action, notify->host, ntohs(notify->port));
            if (c->server_notify_cb) {
                c->server_notify_cb(c->server_notify_cb_data,
                                    notify->action,
                                    notify->host,
                                    ntohs(notify->port),
                                    notify->role,
                                    ntohl(notify->weight));
            }
        }
        break;
    }

    default:
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[cluster] Unknown message type %d from %s",
            hdr.msg_type, hdr.node_id);
        break;
    }

    close(client_fd);
}

/* ============================================================================
 * Cluster Thread
 * ============================================================================ */

/**
 * @brief Main event loop for the cluster management thread.
 *
 * Runs until atomic_load(&c->running) becomes false. Each iteration:
 *   1. Uses epoll_wait() on the listen socket with a deadline-based timeout.
 *   2. Accepts and dispatches any incoming connections via handle_incoming().
 *   3. Sends heartbeats to all eligible peers when the heartbeat interval
 *      has elapsed, draining incoming connections between individual sends
 *      to prevent simultaneous-send deadlocks.
 *
 * @param arg Pointer to the keel_cluster_t instance.
 * @return Always NULL.
 */
static void* cluster_thread_func(void* arg) {
    keel_cluster_t* c = (keel_cluster_t*)arg;

    const int lfd = c->listen_fd;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] Thread started — node=%s listen=%s:%u peers=%zu",
        c->config.node_id, c->config.listen_addr,
        c->config.listen_port, c->peer_count);

    uint64_t last_heartbeat_ns = 0;

#if defined(__linux__)
    /* Create a per-thread epoll fd for the listen socket.
     * epoll_wait() is O(1) regardless of fd value and has no
     * signal-mask overhead compared to poll(). */
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
            "[cluster] epoll_create1 failed: %s", strerror(errno));
        return NULL;
    }
    struct epoll_event cluster_ev = { .events = EPOLLIN, .data.fd = lfd };
    epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &cluster_ev);
    struct epoll_event cluster_ready;
#endif

    while (atomic_load(&c->running)) {
        uint64_t hb_interval_ns = (uint64_t)c->config.heartbeat_interval_ms * 1000000ULL;
        int timeout_ms = 100;

        uint64_t now = cluster_now_ns();
        if (last_heartbeat_ns > 0) {
            uint64_t elapsed = now - last_heartbeat_ns;
            if (elapsed < hb_interval_ns) {
                uint64_t remaining_ms = (hb_interval_ns - elapsed) / 1000000ULL;
                if (remaining_ms < (uint64_t)timeout_ms)
                    timeout_ms = (int)remaining_ms;
            } else {
                timeout_ms = 0;
            }
        }

        /* Factor in election timeout so the loop wakes up in time to trigger
         * an election even if no heartbeat traffic arrives. */
        if (c->config.election_enabled &&
            atomic_load(&c->role) == KEEL_CLUSTER_ROLE_FOLLOWER) {
            uint64_t elect_ns = (uint64_t)c->election_timeout_ms * 1000000ULL;
            uint64_t last_contact = atomic_load(&c->last_leader_contact_ns);
            if (now > last_contact) {
                uint64_t since = now - last_contact;
                if (since < elect_ns) {
                    uint64_t rem_ms = (elect_ns - since) / 1000000ULL;
                    if (rem_ms < (uint64_t)timeout_ms)
                        timeout_ms = (int)rem_ms;
                } else {
                    timeout_ms = 0;
                }
            }
        }

#if defined(__linux__)
        int rc = epoll_wait(epfd, &cluster_ready, 1, timeout_ms);
        if (rc < 0) {
            if (errno == EINTR) continue;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[cluster] epoll_wait error: %s", strerror(errno));
            break;
        }
        bool accept_ready = (rc > 0) &&
            (cluster_ready.events & (EPOLLIN | EPOLLERR | EPOLLHUP));
#else
        /* BSD/macOS fallback */
        fd_set rset; FD_ZERO(&rset); FD_SET(lfd, &rset);
        struct timeval tv = { .tv_sec = timeout_ms / 1000,
                              .tv_usec = (timeout_ms % 1000) * 1000 };
        int rc = select(lfd + 1, &rset, NULL, NULL, &tv);
        if (rc < 0) { if (errno == EINTR) continue; break; }
        bool accept_ready = (rc > 0) && FD_ISSET(lfd, &rset);
#endif

        if (accept_ready) {
            struct sockaddr_in peer_addr;
            socklen_t addr_len = sizeof(peer_addr);
            int client_fd = accept(lfd,
                                   (struct sockaddr*)&peer_addr, &addr_len);
            if (client_fd >= 0) {
                set_nodelay(client_fd);
                handle_incoming(c, client_fd);
            }
        }

        /* Send heartbeats if interval has elapsed. */
        now = cluster_now_ns();
        if (now - last_heartbeat_ns >= hb_interval_ns) {
            size_t hb_targets[KEEL_CLUSTER_MAX_PEERS];
            size_t hb_count = 0;

            pthread_mutex_lock(&c->peer_lock);
            for (size_t i = 0; i < c->peer_count; i++) {
                if (!c->peers[i].active) continue;
                int status = atomic_load(&c->peers[i].status);
                if (status == KEEL_PEER_LEFT) continue;
                hb_targets[hb_count++] = i;
            }
            pthread_mutex_unlock(&c->peer_lock);

            for (size_t s = 0; s < hb_count; s++) {
                /* Drain incoming connections between sends */
#if defined(__linux__)
                struct epoll_event drain_ev;
                while (epoll_wait(epfd, &drain_ev, 1, 0) > 0 &&
                       (drain_ev.events & EPOLLIN)) {
                    struct sockaddr_in pa;
                    socklen_t pa_len = sizeof(pa);
                    int cfd = accept(lfd, (struct sockaddr*)&pa, &pa_len);
                    if (cfd < 0) break;
                    set_nodelay(cfd);
                    handle_incoming(c, cfd);
                }
#else
                fd_set drset; FD_ZERO(&drset); FD_SET(lfd, &drset);
                struct timeval z = {0, 0};
                while (select(lfd + 1, &drset, NULL, NULL, &z) > 0 &&
                       FD_ISSET(lfd, &drset)) {
                    struct sockaddr_in pa;
                    socklen_t pa_len = sizeof(pa);
                    int cfd = accept(lfd, (struct sockaddr*)&pa, &pa_len);
                    if (cfd < 0) break;
                    set_nodelay(cfd);
                    handle_incoming(c, cfd);
                    FD_ZERO(&drset); FD_SET(lfd, &drset);
                }
#endif
                send_heartbeat_to_peer(c, &c->peers[hb_targets[s]]);
            }
            last_heartbeat_ns = now;
        }

        /* Trigger an election if we haven't heard from a leader within the
         * randomised election timeout.  Only checked when we are a FOLLOWER
         * so that a node mid-election (CANDIDATE) is not re-triggered. */
        if (c->config.election_enabled &&
            atomic_load(&c->role) == KEEL_CLUSTER_ROLE_FOLLOWER) {
            uint64_t now2 = cluster_now_ns();
            uint64_t elect_ns = (uint64_t)c->election_timeout_ms * 1000000ULL;
            uint64_t last_contact = atomic_load(&c->last_leader_contact_ns);
            if (now2 - last_contact >= elect_ns)
                cluster_run_election(c);
        }
    }

#if defined(__linux__)
    close(epfd);
#endif
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[cluster] Thread stopping");
    return NULL;
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate and initialise a new cluster instance.
 *
 * Copies @p config, initialises all atomic fields, the peer mutex, and
 * the bootstrap peer table. If config->node_id is empty a hostname-derived
 * identifier is generated automatically. Does not start the cluster thread;
 * call keel_cluster_start() for that.
 *
 * @param config Cluster configuration (copied; caller may free after return).
 * @return Heap-allocated cluster instance on success, NULL on allocation
 *         failure or if @p config is NULL.
 */
keel_cluster_t* keel_cluster_create(const keel_cluster_config_t* config) {
    if (!config) return NULL;

    keel_cluster_t* c = keel_calloc(1, sizeof(keel_cluster_t));
    if (!c) return NULL;

    c->config = *config;
    c->listen_fd = -1;
    atomic_store(&c->running, false);
    atomic_store(&c->config_checksum, 0);
    c->start_time_ns = cluster_now_ns();

    pthread_mutex_init(&c->peer_lock, NULL);
    pthread_mutex_init(&c->election_lock, NULL);
    pthread_mutex_init(&c->config_commit_lock, NULL);
    atomic_store(&c->config_txn_counter, 0);

    /* Generate node_id from hostname if not set */
    if (c->config.node_id[0] == '\0') {
        char hostname[KEEL_CLUSTER_MAX_NODE_ID];
        if (gethostname(hostname, sizeof(hostname)) == 0) {
            char port_suffix[8];
            int port_len = snprintf(port_suffix, sizeof(port_suffix), ":%u",
                                    (unsigned)config->listen_port);
            if (port_len < 0) port_len = 0;

            /* Reserve room for ':port' and trailing NUL to avoid truncation warnings. */
            size_t max_host_len = 0;
            if ((size_t)port_len + 1 < sizeof(c->config.node_id)) {
                max_host_len = sizeof(c->config.node_id) - (size_t)port_len - 1;
            }

            hostname[sizeof(hostname) - 1] = '\0';
            snprintf(c->config.node_id, sizeof(c->config.node_id),
                     "%.*s%s", (int)max_host_len, hostname, port_suffix);
        } else {
            snprintf(c->config.node_id, sizeof(c->config.node_id),
                     "node-%u", config->listen_port);
        }
    }

    /* Initialize bootstrap peers */
    for (size_t i = 0; i < config->initial_peer_count; i++) {
        keel_cluster_peer_t* peer = &c->peers[c->peer_count];
        memset(peer, 0, sizeof(*peer));
        size_t addr_len = strnlen(config->initial_peers[i].addr,
                                  KEEL_CLUSTER_MAX_ADDR - 1);
        memcpy(peer->addr, config->initial_peers[i].addr, addr_len);
        peer->addr[addr_len] = '\0';
        peer->port = config->initial_peers[i].port;
        peer->active = true;
        peer->source = KEEL_PEER_SOURCE_BOOTSTRAP;
        peer->discovered_at_sec = cluster_now_sec();
        atomic_store(&peer->status, KEEL_PEER_UNKNOWN);
        c->peer_count++;
    }

    /* -----------------------------------------------------------------------
     * Election state initialisation
     * --------------------------------------------------------------------- */
    atomic_store(&c->role, KEEL_CLUSTER_ROLE_FOLLOWER);
    atomic_store(&c->term, 0);
    atomic_store(&c->last_leader_contact_ns, cluster_now_ns());
    atomic_store(&c->elections_started, 0);
    atomic_store(&c->elections_won, 0);
    atomic_store(&c->votes_granted, 0);
    atomic_store(&c->votes_denied, 0);
    atomic_store(&c->leader_stepdowns, 0);

    if (c->config.election_enabled) {
        /* Per-node randomised election timeout:
         *   base  = 2 × heartbeat_interval_ms
         *   jitter = 0 .. heartbeat_interval_ms  (FNV-1a hash of node_id)
         * This ensures no two nodes share the same timeout, preventing
         * split-vote deadlocks without external configuration. */
        uint32_t base  = c->config.heartbeat_interval_ms * 2;
        if (base < 500) base = 500;
        uint32_t range = c->config.heartbeat_interval_ms;
        if (range < 100) range = 100;

        uint32_t h = 2166136261u;
        for (const char* p = c->config.node_id; *p; p++) {
            h ^= (uint8_t)(*p);
            h *= 16777619u;  /* FNV-1a 32-bit */
        }
        c->election_timeout_ms = base + (h % (range + 1));

        /* Derive the persistent state path from node_id when the caller
         * left it empty.  Uses /tmp so it works in containers without
         * /var/lib/keel being present. */
        if (c->config.election_state_path[0] == '\0') {
            char safe_id[KEEL_CLUSTER_MAX_NODE_ID];
            size_t id_len = strnlen(c->config.node_id, sizeof(safe_id) - 1);
            for (size_t i = 0; i < id_len; i++) {
                char ch = c->config.node_id[i];
                safe_id[i] = ((ch >= 'a' && ch <= 'z') ||
                              (ch >= 'A' && ch <= 'Z') ||
                              (ch >= '0' && ch <= '9') ||
                              ch == '-' || ch == '_') ? ch : '_';
            }
            safe_id[id_len] = '\0';
            snprintf(c->config.election_state_path,
                     sizeof(c->config.election_state_path),
                     "/tmp/keel-cluster-%s.state", safe_id);
        }

        cluster_load_persistent_state(c);

        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
            "[cluster] Election enabled: timeout=%ums state=%s",
            c->election_timeout_ms, c->config.election_state_path);
    }

    return c;
}

/**
 * @brief Bind the listen socket and start the cluster background thread.
 *
 * Creates the TCP listen socket, sets the running flag, spawns the cluster
 * thread, and broadcasts a JOIN message to all bootstrap peers. The listen
 * socket is left open until keel_cluster_stop() is called.
 *
 * @param c Cluster instance created by keel_cluster_create().
 * @return 0 on success, -1 if @p c is NULL, clustering is disabled,
 *         the listen socket could not be bound, or thread creation failed.
 */
int keel_cluster_start(keel_cluster_t* c) {
    if (!c || !c->config.enabled) return -1;

    /* Already running — reject the call without touching any struct fields.
     * This also prevents the TSan data race where cluster_thread_func reads
     * c->listen_fd while a second keel_cluster_start() call writes it. */
    if (atomic_load(&c->running)) return -1;

    c->listen_fd = create_listen_socket(c->config.listen_addr,
                                        c->config.listen_port);
    if (c->listen_fd < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
            "[cluster] Failed to bind %s:%u: %s",
            c->config.listen_addr, c->config.listen_port, strerror(errno));
        return -1;
    }

    atomic_store(&c->running, true);

    int rc = pthread_create(&c->thread, NULL, cluster_thread_func, c);
    if (rc != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
            "[cluster] Failed to create thread: %s", strerror(rc));
        atomic_store(&c->running, false);
        close(c->listen_fd);
        c->listen_fd = -1;
        return -1;
    }

    pthread_setname_np(c->thread, "keel-cluster");

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] Started — node=%s listen=%s:%u peers=%zu",
        c->config.node_id, c->config.listen_addr,
        c->config.listen_port, c->peer_count);

    /* Announce ourselves to bootstrap peers so they can discover and enroll us. */
    cluster_broadcast_join(c);

    return 0;
}

/**
 * @brief Gracefully stop the cluster and join the background thread.
 *
 * Sends a KEEL_CLUSTER_MSG_LEAVE to every UP/SUSPECT peer, clears the
 * running flag, shuts down the listen socket to unblock poll(), then
 * calls pthread_join() to wait for the thread to exit. Safe to call from
 * any thread. No-op if the cluster is not running.
 *
 * @param c Cluster instance.
 */
void keel_cluster_stop(keel_cluster_t* c) {
    if (!c || !atomic_load(&c->running)) return;

    /* Send LEAVE to all active peers */
    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (!c->peers[i].active) continue;
        int status = atomic_load(&c->peers[i].status);
        if (status != KEEL_PEER_UP && status != KEEL_PEER_SUSPECT) continue;

        int fd = connect_to_peer(c->peers[i].addr, c->peers[i].port, 1000);
        if (fd >= 0) {
            send_msg(fd, KEEL_CLUSTER_MSG_LEAVE, NULL, 0, c->config.node_id, CLUSTER_CODEC(c));
            close(fd);
        }
    }
    pthread_mutex_unlock(&c->peer_lock);

    /* Signal thread to stop and wake it from poll(). We use shutdown()
     * rather than close() so the fd stays valid for the thread's cached
     * copy until after pthread_join() completes. */
    atomic_store(&c->running, false);
    if (c->listen_fd >= 0)
        shutdown(c->listen_fd, SHUT_RDWR);

    pthread_join(c->thread, NULL);

    /* Now safe to close — thread has exited */
    if (c->listen_fd >= 0) {
        close(c->listen_fd);
        c->listen_fd = -1;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "[cluster] Stopped");
}

/**
 * @brief Stop (if running) and free a cluster instance.
 *
 * Calls keel_cluster_stop() if necessary, destroys the peer mutex, and
 * releases the heap memory. After this call @p c is invalid.
 *
 * @param c Cluster instance to destroy. No-op if NULL.
 */
void keel_cluster_destroy(keel_cluster_t* c) {
    if (!c) return;

    if (atomic_load(&c->running))
        keel_cluster_stop(c);

    if (c->listen_fd >= 0)
        close(c->listen_fd);

    pthread_mutex_destroy(&c->peer_lock);
    pthread_mutex_destroy(&c->election_lock);
    pthread_mutex_destroy(&c->config_commit_lock);
    keel_free(c);
}

const keel_cluster_config_t* keel_cluster_get_config(const keel_cluster_t* c) {
    if (!c) return NULL;
    return &c->config;
}

/* ============================================================================
 * Peer Management
 * ============================================================================ */

/**
 * @brief Return the total number of peer table entries (active and inactive).
 *
 * Acquires peer_lock and returns peer_count, which includes both active
 * and logically removed (inactive) slots. Use keel_cluster_get_peer() with
 * an index loop to inspect individual entries.
 *
 * @param c Cluster instance.
 * @return Total number of peer slots used, or 0 if @p c is NULL.
 */
size_t keel_cluster_peer_count(const keel_cluster_t* c) {
    if (!c) return 0;
    pthread_mutex_lock(&((keel_cluster_t*)c)->peer_lock);
    size_t n = c->peer_count;
    pthread_mutex_unlock(&((keel_cluster_t*)c)->peer_lock);
    return n;
}

/**
 * @brief Copy a snapshot of a peer entry into caller-supplied storage.
 *
 * Non-atomic identity fields (node_id, addr) are copied under peer_lock;
 * each atomic field is loaded individually to satisfy TSan. The snapshot
 * is point-in-time consistent but may be stale by the time the caller
 * reads it.
 *
 * @param c     Cluster instance.
 * @param index Zero-based index into the peer table (< keel_cluster_peer_count()).
 * @param peer  Output structure populated with the snapshot.
 * @return true on success, false if @p c or @p peer is NULL or @p index is
 *         out of range.
 */
bool keel_cluster_get_peer(const keel_cluster_t* c, size_t index,
                           keel_cluster_peer_t* peer) {
    if (!c || index >= c->peer_count || !peer) return false;
    const keel_cluster_peer_t *src = &c->peers[index];

    /* Hold peer_lock for the non-atomic identity fields (node_id may
     * be written by the cluster thread on first contact). */
    pthread_mutex_lock(&((keel_cluster_t*)c)->peer_lock);
    memcpy(peer->node_id, src->node_id, sizeof(peer->node_id));
    memcpy(peer->addr, src->addr, sizeof(peer->addr));
    peer->port   = src->port;
    peer->active = src->active;
    peer->source = src->source;
    peer->discovered_at_sec = src->discovered_at_sec;
    pthread_mutex_unlock(&((keel_cluster_t*)c)->peer_lock);

    /* Snapshot each atomic field individually so TSan sees proper
     * atomic loads instead of a raw struct memcpy. */
    peer->status               = atomic_load(&src->status);
    peer->consecutive_failures = atomic_load(&src->consecutive_failures);
    peer->last_heartbeat_ns    = atomic_load(&src->last_heartbeat_ns);
    peer->last_latency_us      = atomic_load(&src->last_latency_us);
    peer->total_heartbeats     = atomic_load(&src->total_heartbeats);
    peer->total_failures       = atomic_load(&src->total_failures);
    peer->config_checksum      = atomic_load(&src->config_checksum);
    peer->uptime_sec           = atomic_load(&src->uptime_sec);
    peer->num_clients          = atomic_load(&src->num_clients);
    peer->num_backends         = atomic_load(&src->num_backends);
    peer->engine_state         = atomic_load(&src->engine_state);
    peer->last_sync_request_ns = atomic_load(&src->last_sync_request_ns);

    return true;
}

/**
 * @brief Administratively add a peer to the cluster.
 *
 * Delegates to cluster_add_or_update_peer() with source
 * KEEL_PEER_SOURCE_ADMIN. The peer will be contacted during the next
 * heartbeat interval.
 *
 * @param c    Cluster instance.
 * @param addr IPv4 address string of the new peer.
 * @param port TCP cluster port of the new peer.
 * @return 0 on success, -1 if parameters are invalid or the table is full.
 */
int keel_cluster_add_peer(keel_cluster_t* c, const char* addr, uint16_t port) {
    return cluster_add_or_update_peer(c,
                                      addr,
                                      port,
                                      NULL,
                                      KEEL_PEER_SOURCE_ADMIN);
}

/**
 * @brief Administratively remove a peer from the cluster.
 *
 * Finds the first active peer matching (addr, port) and marks it inactive.
 * The peer will no longer receive heartbeats. Thread-safe.
 *
 * @param c    Cluster instance.
 * @param addr IPv4 address string of the peer to remove.
 * @param port TCP cluster port of the peer to remove.
 * @return 0 if the peer was found and removed, -1 otherwise.
 */
int keel_cluster_remove_peer(keel_cluster_t* c, const char* addr, uint16_t port) {
    if (!c || !addr) return -1;

    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (c->peers[i].active &&
            strcmp(c->peers[i].addr, addr) == 0 &&
            c->peers[i].port == port) {
            c->peers[i].active = false;
            pthread_mutex_unlock(&c->peer_lock);
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[cluster] Removed peer %s:%u", addr, port);
            return 0;
        }
    }
    pthread_mutex_unlock(&c->peer_lock);
    return -1;
}

/* ============================================================================
 * Gossip / Sync
 * ============================================================================ */

/**
 * @brief Atomically update the local configuration checksum.
 *
 * The checksum is included in every heartbeat and sync message so that
 * peers can detect configuration drift. Callers should recompute and set
 * the checksum whenever the proxy configuration changes.
 *
 * @param c        Cluster instance.
 * @param checksum New checksum value (caller-defined; 0 means unknown).
 */
void keel_cluster_set_config_checksum(keel_cluster_t* c, uint64_t checksum) {
    if (c) atomic_store(&c->config_checksum, checksum);
}

/**
 * @brief Atomically read the current local configuration checksum.
 *
 * @param c Cluster instance.
 * @return Current checksum, or 0 if @p c is NULL.
 */
uint64_t keel_cluster_get_config_checksum(const keel_cluster_t* c) {
    return c ? atomic_load(&c->config_checksum) : 0;
}

/**
 * @brief Snapshot the current runtime configuration into caller storage.
 *
 * Copies node identity, listen address, timing parameters, active peer
 * count, config checksum, and running state into @p config.
 *
 * @param c      Cluster instance.
 * @param config Output structure to populate.
 * @return true on success, false if either pointer is NULL.
 */
bool keel_cluster_get_runtime_config(const keel_cluster_t* c,
                                     keel_cluster_runtime_config_t* config) {
    if (!c || !config) return false;

    memset(config, 0, sizeof(*config));
    memcpy(config->node_id, c->config.node_id, sizeof(config->node_id));
    memcpy(config->listen_addr, c->config.listen_addr, sizeof(config->listen_addr));
    config->listen_port = c->config.listen_port;
    config->active_peers = cluster_active_peer_count(c);
    config->heartbeat_interval_ms = c->config.heartbeat_interval_ms;
    config->heartbeat_timeout_ms = c->config.heartbeat_timeout_ms;
    config->failure_threshold = c->config.failure_threshold;
    config->config_checksum = atomic_load(&c->config_checksum);
    config->auto_sync = c->config.auto_sync ? 1U : 0U;
    config->running = atomic_load(&c->running) ? 1U : 0U;
    return true;
}

/**
 * @brief Broadcast a server topology change notification to all UP peers.
 *
 * Builds a KEEL_CLUSTER_MSG_NOTIFY_SERVER message and delivers it to every
 * peer currently in the KEEL_PEER_UP state. Failures to deliver to
 * individual peers are silently ignored.
 *
 * @param c      Cluster instance.
 * @param action Server action code (add/remove/modify).
 * @param host   Null-terminated hostname or IP address of the affected server.
 * @param port   Port of the affected server.
 * @param role   Server role (primary/replica).
 * @param weight Relative routing weight.
 * @return Always 0 (individual delivery failures are non-fatal).
 */
int keel_cluster_notify_server_change(keel_cluster_t* c,
                                      uint8_t action,
                                      const char* host, uint16_t port,
                                      uint8_t role, uint32_t weight) {
    if (!c || !host) return -1;

    keel_cluster_server_notify_t notify;
    memset(&notify, 0, sizeof(notify));
    notify.action = action;
    notify.role = role;
    notify.port = htons(port);
    notify.weight = htonl(weight);
    size_t host_len = strlen(host);
    if (host_len >= sizeof(notify.host)) host_len = sizeof(notify.host) - 1;
    memcpy(notify.host, host, host_len);

    int sent = 0;
    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (!c->peers[i].active) continue;
        int status = atomic_load(&c->peers[i].status);
        if (status != KEEL_PEER_UP) continue;

        int fd = connect_to_peer(c->peers[i].addr, c->peers[i].port,
                                 c->config.heartbeat_timeout_ms);
        if (fd >= 0) {
            if (send_msg(fd, KEEL_CLUSTER_MSG_NOTIFY_SERVER, &notify,
                         sizeof(notify), c->config.node_id, CLUSTER_CODEC(c)) == 0) {
                sent++;
            }
            close(fd);
        }
    }
    pthread_mutex_unlock(&c->peer_lock);

    atomic_fetch_add(&c->server_notifications, (uint64_t)sent);
    return 0;
}

/* ============================================================================
 * Stats / Introspection
 * ============================================================================ */

/**
 * @brief Register a server-topology-change callback.
 */
void keel_cluster_set_server_notify_cb(keel_cluster_t* c,
                                        keel_cluster_server_notify_cb_t cb,
                                        void* user_data) {
    if (!c) return;
    c->server_notify_cb      = cb;
    c->server_notify_cb_data = user_data;
}

/**
 * @brief Register a live-stats callback for heartbeat field population.
 */
void keel_cluster_set_stats_cb(keel_cluster_t* c,
                                keel_cluster_stats_cb_t cb,
                                void* user_data) {
    if (!c) return;
    c->stats_cb      = cb;
    c->stats_cb_data = user_data;
}

/**
 * @brief Collect a point-in-time statistics snapshot for the cluster.
 *
 * Walks the peer table to tally per-status peer counts and discovered peers,
 * then copies all atomic counters (heartbeats sent/received, sync events,
 * last sync timestamp, server notifications) and current timing configuration
 * into @p stats.
 *
 * @param c     Cluster instance.
 * @param stats Output structure to populate. Zeroed before writing.
 */
void keel_cluster_get_stats(const keel_cluster_t* c, keel_cluster_stats_t* stats) {
    if (!c || !stats) return;
    memset(stats, 0, sizeof(*stats));

    for (size_t i = 0; i < c->peer_count; i++) {
        if (!c->peers[i].active) continue;
        stats->total_peers++;
        int s = atomic_load(&c->peers[i].status);
        switch (s) {
        case KEEL_PEER_UP:      stats->peers_up++; break;
        case KEEL_PEER_SUSPECT: stats->peers_suspect++; break;
        case KEEL_PEER_DOWN:    stats->peers_down++; break;
        case KEEL_PEER_LEFT:    stats->peers_left++; break;
        case KEEL_PEER_UNKNOWN: stats->peers_unknown++; break;
        default: break;
        }

        if (c->peers[i].source == KEEL_PEER_SOURCE_JOIN ||
            c->peers[i].source == KEEL_PEER_SOURCE_GOSSIP) {
            stats->discovered_peers++;
        }
    }

    stats->heartbeats_sent = atomic_load(&c->heartbeats_sent);
    stats->heartbeats_received = atomic_load(&c->heartbeats_received);
    stats->sync_requests_sent = atomic_load(&c->sync_requests_sent);
    stats->sync_responses_sent = atomic_load(&c->sync_responses_sent);
    stats->discovered_peers_total = atomic_load(&c->discovered_peers_total);
    stats->config_reconciliations = atomic_load(&c->config_reconciliations);
    stats->last_sync_apply_ns = atomic_load(&c->last_sync_apply_ns);
    stats->server_notifications = atomic_load(&c->server_notifications);
    pthread_mutex_lock(&((keel_cluster_t*)c)->peer_lock);
    stats->local_heartbeat_interval_ms = c->config.heartbeat_interval_ms;
    stats->local_heartbeat_timeout_ms = c->config.heartbeat_timeout_ms;
    stats->local_failure_threshold = c->config.failure_threshold;
    stats->local_auto_sync = c->config.auto_sync ? 1U : 0U;
    pthread_mutex_unlock(&((keel_cluster_t*)c)->peer_lock);

    /* Election statistics */
    stats->elections_started  = atomic_load(&c->elections_started);
    stats->elections_won      = atomic_load(&c->elections_won);
    stats->votes_granted      = atomic_load(&c->votes_granted);
    stats->votes_denied       = atomic_load(&c->votes_denied);
    stats->leader_stepdowns   = atomic_load(&c->leader_stepdowns);
    stats->local_role         = (keel_cluster_role_t)atomic_load(&c->role);
    stats->local_term         = atomic_load(&c->term);
    pthread_mutex_lock(&((keel_cluster_t*)c)->election_lock);
    memcpy(stats->local_leader_id, c->leader_id, sizeof(stats->local_leader_id));
    pthread_mutex_unlock(&((keel_cluster_t*)c)->election_lock);
}

/**
 * @brief Return whether the cluster thread is currently running.
 */
bool keel_cluster_is_active(const keel_cluster_t* c) {
    return c && atomic_load(&c->running);
}

/* ============================================================================
 * Leader Election Public API
 * ============================================================================ */

keel_cluster_role_t keel_cluster_get_role(const keel_cluster_t* c) {
    if (!c) return KEEL_CLUSTER_ROLE_FOLLOWER;
    return (keel_cluster_role_t)atomic_load(&c->role);
}

uint64_t keel_cluster_get_term(const keel_cluster_t* c) {
    return c ? atomic_load(&c->term) : 0;
}

void keel_cluster_get_leader_id(const keel_cluster_t* c,
                                char* buf, size_t bufsize) {
    if (!buf || bufsize == 0) return;
    buf[0] = '\0';
    if (!c) return;
    pthread_mutex_lock(&((keel_cluster_t*)c)->election_lock);
    size_t len = strnlen(c->leader_id, KEEL_CLUSTER_MAX_NODE_ID - 1);
    if (len >= bufsize) len = bufsize - 1;
    memcpy(buf, c->leader_id, len);
    buf[len] = '\0';
    pthread_mutex_unlock(&((keel_cluster_t*)c)->election_lock);
}

/* ============================================================================
 * Phase 4 — Quorum-Gated Config Commit (2-Phase Commit)
 * ============================================================================ */

/**
 * @brief Send CONFIG_PREPARE to a single peer and collect the CONFIG_ACK.
 *
 * @return 1 if the peer accepted, 0 if rejected, -1 on comms failure.
 */
static int cluster_send_config_prepare(keel_cluster_t* c,
                                       keel_cluster_peer_t* peer,
                                       uint64_t term,
                                       uint64_t txn_id,
                                       uint64_t new_checksum) {
    uint32_t timeout = c->config.heartbeat_interval_ms * 4;
    if (timeout < 400)  timeout = 400;
    if (timeout > 5000) timeout = 5000;

    int fd = connect_to_peer(peer->addr, peer->port, timeout);
    if (fd < 0) return -1;

    keel_cluster_config_prepare_t prep;
    prep.term         = htobe64(term);
    prep.txn_id       = htobe64(txn_id);
    prep.new_checksum = htobe64(new_checksum);

    if (send_msg(fd, KEEL_CLUSTER_MSG_CONFIG_PREPARE,
                 &prep, sizeof(prep), c->config.node_id, CLUSTER_CODEC(c)) < 0) {
        close(fd);
        return -1;
    }

    keel_cluster_msg_header_t resp_hdr;
    keel_cluster_config_ack_t ack;
    int rc = recv_msg(fd, &resp_hdr, &ack, sizeof(ack), timeout);
    close(fd);

    if (rc < 0 || resp_hdr.msg_type != KEEL_CLUSTER_MSG_CONFIG_ACK)
        return -1;

    /* If peer reports a higher term we must step down */
    uint64_t peer_term = be64toh(ack.term);
    if (peer_term > atomic_load(&c->term)) {
        cluster_step_down(c, peer_term);
        return -1;
    }

    return ack.accepted ? 1 : 0;
}

/**
 * @brief Send CONFIG_COMMIT (or abort) to a single peer — best-effort.
 */
static void cluster_send_config_commit(keel_cluster_t* c,
                                       keel_cluster_peer_t* peer,
                                       uint64_t term,
                                       uint64_t txn_id,
                                       bool commit) {
    uint32_t timeout = c->config.heartbeat_interval_ms;
    if (timeout < 200) timeout = 200;

    int fd = connect_to_peer(peer->addr, peer->port, timeout);
    if (fd < 0) return;

    keel_cluster_config_commit_t msg;
    msg.term   = htobe64(term);
    msg.txn_id = htobe64(txn_id);
    msg.commit = commit ? 1 : 0;
    memset(msg._pad, 0, sizeof(msg._pad));

    send_msg(fd, KEEL_CLUSTER_MSG_CONFIG_COMMIT,
             &msg, sizeof(msg), c->config.node_id, CLUSTER_CODEC(c));
    close(fd);
}

/**
 * @brief Leader-side two-phase config commit.
 *
 * 1. Broadcasts CONFIG_PREPARE to all active non-LEFT peers.
 * 2. Waits for majority ACKs.
 * 3. On quorum: sends CONFIG_COMMIT to all, returns 0.
 * 4. On no quorum: sends CONFIG_ABORT, returns -1.
 */
int keel_cluster_quorum_commit(keel_cluster_t* c, uint64_t new_checksum) {
    if (!c || !keel_cluster_is_active(c)) return -1;

    /* Only the leader may commit config changes */
    if (atomic_load(&c->role) != KEEL_CLUSTER_ROLE_LEADER) return -1;

    uint64_t term   = atomic_load(&c->term);
    uint64_t txn_id = atomic_fetch_add(&c->config_txn_counter, 1) + 1;

    /* Snapshot peer indices */
    size_t peer_idx[KEEL_CLUSTER_MAX_PEERS];
    size_t peer_count = 0;
    int    total_nodes = 1; /* self */

    pthread_mutex_lock(&c->peer_lock);
    for (size_t i = 0; i < c->peer_count; i++) {
        if (!c->peers[i].active) continue;
        if (atomic_load(&c->peers[i].status) == KEEL_PEER_LEFT) continue;
        peer_idx[peer_count++] = i;
        total_nodes++;
    }
    pthread_mutex_unlock(&c->peer_lock);

    int quorum  = total_nodes / 2 + 1;
    int accepts = 1; /* self always accepts */

    for (size_t i = 0; i < peer_count; i++) {
        /* Abort early if we were displaced mid-round */
        if (atomic_load(&c->role) != KEEL_CLUSTER_ROLE_LEADER)
            return -1;

        int r = cluster_send_config_prepare(
            c, &c->peers[peer_idx[i]], term, txn_id, new_checksum);
        if (r == 1) accepts++;
    }

    bool ok = (accepts >= quorum);

    /* Phase 2: send commit or abort to all peers that participated */
    for (size_t i = 0; i < peer_count; i++) {
        cluster_send_config_commit(
            c, &c->peers[peer_idx[i]], term, txn_id, ok);
    }

    if (!ok) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[cluster] 2PC aborted (txn=%" PRIu64
            "): got %d/%d accepts (need %d)",
            txn_id, accepts, total_nodes, quorum);
        return -1;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "[cluster] 2PC committed (txn=%" PRIu64
        ", checksum=0x%016llx, accepts=%d/%d)",
        txn_id, (unsigned long long)new_checksum, accepts, total_nodes);
    return 0;
}
