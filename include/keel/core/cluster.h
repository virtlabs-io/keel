/**
 * @file cluster.h
 * @brief Multi-proxy HA cluster mode — peer discovery, gossip, and health.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This header defines the cluster subsystem that enables multiple KEEL
 * instances to coordinate as a high-availability group. The cluster module
 * provides:
 *
 *   - **Peer Discovery**: Bootstrap from a static peer list or DNS SRV records.
 *   - **Heartbeat Health Monitoring**: Periodic TCP health probes to every peer
 *     with configurable timeout and failure threshold.
 *   - **Configuration Gossip**: Propagate configuration changes (admin commands,
 *     server topology) to peers via checksum-based sync over a lightweight
 *     binary protocol on a dedicated cluster port.
 *   - **Cluster State Replication**: Share pool assignments, failover status,
 *     and aggregated metrics across the cluster for unified visibility.
 *
 * Architecture:
 *   ┌──────────────────────────────────────────────────┐
 *   │                  KEEL Node A                      │
 *   │  ┌────────────┐  ┌────────────┐  ┌────────────┐ │
 *   │  │  Cluster    │  │  Gossip    │  │  Heartbeat │ │
 *   │  │  Manager    │──│  Protocol  │──│  Monitor   │ │
 *   │  └────────────┘  └────────────┘  └────────────┘ │
 *   │        │                │                │       │
 *   │        └────────────────┼────────────────┘       │
 *   │                    Cluster Port                   │
 *   └────────────────────────┬─────────────────────────┘
 *                            │ TCP
 *   ┌────────────────────────┴─────────────────────────┐
 *   │                  KEEL Node B                      │
 *   └──────────────────────────────────────────────────┘
 *
 * The cluster uses a simple pull-based gossip protocol:
 *   1. Each node periodically sends a HEARTBEAT to all known peers.
 *   2. HEARTBEAT carries the node's config checksum and state summary.
 *   3. If a peer detects a config checksum mismatch, it sends a SYNC_REQUEST.
 *   4. The node responds with a SYNC_RESPONSE containing the full config delta.
 *
 * All cluster communication uses a dedicated TCP port separate from the
 * proxy data plane, ensuring cluster coordination never interferes with
 * client traffic.
 */

#ifndef KEEL_CLUSTER_H
#define KEEL_CLUSTER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

/** Maximum number of peers in a cluster. */
#define KEEL_CLUSTER_MAX_PEERS      16

/** Maximum length of a node identifier string. */
#define KEEL_CLUSTER_MAX_NODE_ID    64

/** Maximum length of a peer address string. */
#define KEEL_CLUSTER_MAX_ADDR       256

/** Cluster wire protocol magic bytes. */
#define KEEL_CLUSTER_MAGIC          0x4B454C43U  /* "KELC" */

/** Cluster wire protocol version. */
#define KEEL_CLUSTER_PROTO_VERSION  4

/* ============================================================================
 * Wire-protocol compression
 * ============================================================================
 *
 * The top 2 bits of the packed payload_len field carry the compression codec
 * used for the message payload.  The remaining 14 bits are the actual length
 * of the (possibly compressed) payload (max 16 383 bytes, sufficient for all
 * cluster control messages).
 *
 *   bits 15-14:  codec   00=NONE  01=ZLIB  10=ZSTD
 *   bits 13-0:   length  0..16383
 *
 * Compression is transparent to the caller of send_msg / recv_msg.  The
 * cluster configuration controls whether compression is applied and the
 * minimum payload size that triggers it (compress_threshold_bytes).
 */

/** Mask to extract/set the codec bits from the wire payload_len field. */
#define KEEL_CLUSTER_COMPRESS_MASK   UINT16_C(0xC000)
/** Mask to extract the actual payload length from the wire field. */
#define KEEL_CLUSTER_PAYLOAD_LEN_MASK UINT16_C(0x3FFF)
/** Maximum payload length that can be encoded in the 14-bit length field. */
#define KEEL_CLUSTER_MAX_PAYLOAD     UINT16_C(0x3FFF)

/**
 * Codec IDs stored in keel_cluster_config_t.compress_codec and used at the
 * application level.  These are compact 0/1/2 values that map 1:1 to the
 * keel_compress_codec_t enum in compress.h.
 */
/** No compression (default). */
#define KEEL_CLUSTER_COMPRESS_NONE   ((uint8_t)0)
/** zlib deflate compression. */
#define KEEL_CLUSTER_COMPRESS_ZLIB   ((uint8_t)1)
/** zstd compression (preferred for WAN). */
#define KEEL_CLUSTER_COMPRESS_ZSTD   ((uint8_t)2)

/**
 * Wire-encoding flags embedded in the top 2 bits of the payload_len field.
 * These are internal to the wire protocol; user code should use the
 * KEEL_CLUSTER_COMPRESS_* constants above.
 */
/** Wire flag: no compression. */
#define KEEL_CLUSTER_WIRE_NONE       UINT16_C(0x0000)
/** Wire flag: zlib compression. */
#define KEEL_CLUSTER_WIRE_ZLIB       UINT16_C(0x4000)
/** Wire flag: zstd compression. */
#define KEEL_CLUSTER_WIRE_ZSTD       UINT16_C(0x8000)

/* ============================================================================
 * Node Role (Leader Election)
 * ============================================================================ */

/**
 * @brief This node's role in the leader-election state machine.
 *
 * Election uses a simplified Raft-style majority vote:
 *   - FOLLOWER: default state; defers to the current leader.
 *   - CANDIDATE: actively soliciting votes after election timeout.
 *   - LEADER: won a majority-quorum election; announces itself in heartbeats.
 *
 * Transitions:
 *   FOLLOWER  → CANDIDATE  when election_timeout elapses without leader contact
 *   CANDIDATE → LEADER     when majority of active nodes grant votes
 *   CANDIDATE → FOLLOWER   on vote failure or higher-term message received
 *   LEADER    → FOLLOWER   on higher-term message received
 */
typedef enum keel_cluster_role {
    KEEL_CLUSTER_ROLE_FOLLOWER  = 0,  /**< Default; following the current leader. */
    KEEL_CLUSTER_ROLE_CANDIDATE = 1,  /**< Running for leader in current term. */
    KEEL_CLUSTER_ROLE_LEADER    = 2,  /**< Elected leader for the current term. */
} keel_cluster_role_t;

/* ============================================================================
 * Peer Health Status
 * ============================================================================ */

typedef enum keel_peer_status {
    KEEL_PEER_UNKNOWN   = 0,  /**< Initial state, never contacted. */
    KEEL_PEER_UP        = 1,  /**< Responding to heartbeats. */
    KEEL_PEER_SUSPECT   = 2,  /**< Missed heartbeats below threshold. */
    KEEL_PEER_DOWN      = 3,  /**< Exceeded failure threshold. */
    KEEL_PEER_LEFT      = 4,  /**< Gracefully departed the cluster. */
} keel_peer_status_t;

/**
 * @brief How a peer was first discovered.
 */
typedef enum keel_peer_source {
    KEEL_PEER_SOURCE_BOOTSTRAP = 0, /**< From initial static config */
    KEEL_PEER_SOURCE_ADMIN     = 1, /**< Added manually via admin */
    KEEL_PEER_SOURCE_JOIN      = 2, /**< Learned from incoming JOIN */
    KEEL_PEER_SOURCE_GOSSIP    = 3, /**< Learned from peer list gossip */
} keel_peer_source_t;

/* ============================================================================
 * Cluster Message Types
 * ============================================================================ */

typedef enum keel_cluster_msg_type {
    KEEL_CLUSTER_MSG_HEARTBEAT      = 1,  /**< Periodic health + state summary. */
    KEEL_CLUSTER_MSG_HEARTBEAT_ACK  = 2,  /**< Acknowledgement with peer state. */
    KEEL_CLUSTER_MSG_SYNC_REQUEST   = 3,  /**< Request full config sync. */
    KEEL_CLUSTER_MSG_SYNC_RESPONSE  = 4,  /**< Config data payload. */
    KEEL_CLUSTER_MSG_JOIN           = 5,  /**< Node joining the cluster. */
    KEEL_CLUSTER_MSG_LEAVE          = 6,  /**< Node gracefully leaving. */
    KEEL_CLUSTER_MSG_NOTIFY_SERVER  = 7,  /**< Server topology change. */
    KEEL_CLUSTER_MSG_VOTE_REQUEST   = 8,  /**< Request votes for leader election. */
    KEEL_CLUSTER_MSG_VOTE_RESPONSE  = 9,  /**< Grant or deny a vote. */
    KEEL_CLUSTER_MSG_CONFIG_PREPARE = 10, /**< Leader 2PC: propose config change. */
    KEEL_CLUSTER_MSG_CONFIG_COMMIT  = 11, /**< Leader 2PC: commit or abort. */
    KEEL_CLUSTER_MSG_CONFIG_ACK     = 12, /**< Peer 2PC: accept or reject prepare. */
} keel_cluster_msg_type_t;

/* ============================================================================
 * Wire Protocol Structures
 * ============================================================================ */

/**
 * @brief Header for all cluster wire messages.
 *
 * All multi-byte fields are in network byte order (big-endian).
 */
typedef struct __attribute__((packed)) keel_cluster_msg_header {
    uint32_t magic;          /**< KEEL_CLUSTER_MAGIC */
    uint8_t  version;        /**< Protocol version */
    uint8_t  msg_type;       /**< keel_cluster_msg_type_t */
    uint16_t payload_len;    /**< Length of payload following this header */
    char     node_id[KEEL_CLUSTER_MAX_NODE_ID]; /**< Sender node ID */
} keel_cluster_msg_header_t;

/**
 * @brief Heartbeat message payload.
 */
typedef struct __attribute__((packed)) keel_cluster_heartbeat {
    uint64_t config_checksum;   /**< XXHash64 of current running config */
    uint64_t uptime_sec;        /**< Seconds since node started */
    uint64_t term;              /**< Current election term (0 = election disabled) */
    uint32_t num_clients;       /**< Active client connections */
    uint32_t num_backends;      /**< Active backend connections */
    uint32_t num_servers;       /**< Configured backend servers */
    uint16_t num_peers;         /**< Known peer count */
    uint8_t  state;             /**< keel_engine_state_t of this node */
    uint8_t  role;              /**< keel_cluster_role_t of this node */
    char     leader_id[KEEL_CLUSTER_MAX_NODE_ID]; /**< Current leader node_id, or empty */
} keel_cluster_heartbeat_t;

/**
 * @brief Join message payload used for peer discovery.
 *
 * The source TCP port of an incoming JOIN connection is ephemeral, so a node
 * must advertise the stable cluster listen address and port it expects peers
 * to connect back to.
 */
typedef struct __attribute__((packed)) keel_cluster_join {
    uint16_t listen_port;                        /**< Cluster listen port */
    char     listen_addr[KEEL_CLUSTER_MAX_ADDR]; /**< Cluster listen address */
} keel_cluster_join_t;

/**
 * @brief Sync request payload.
 */
typedef struct __attribute__((packed)) keel_cluster_sync_request {
    uint64_t requester_checksum;  /**< Requesting node config checksum */
    uint16_t known_peer_count;    /**< Requesting node current peer count */
    uint8_t  include_peers;       /**< Whether a peer list is requested */
    uint8_t  _pad;
} keel_cluster_sync_request_t;

/**
 * @brief Safe cluster runtime config fields that can be reconciled from peers.
 */
typedef struct __attribute__((packed)) keel_cluster_sync_config {
    uint32_t heartbeat_interval_ms; /**< Peer heartbeat interval */
    uint32_t heartbeat_timeout_ms;  /**< Peer heartbeat timeout */
    uint32_t failure_threshold;     /**< Peer failure threshold */
    uint8_t  auto_sync;             /**< Peer auto_sync setting */
    uint8_t  _pad[3];
} keel_cluster_sync_config_t;

/**
 * @brief One peer entry in a sync response.
 */
typedef struct __attribute__((packed)) keel_cluster_peer_snapshot {
    char     node_id[KEEL_CLUSTER_MAX_NODE_ID]; /**< Peer node id */
    char     addr[KEEL_CLUSTER_MAX_ADDR];       /**< Peer listen address */
    uint16_t port;                              /**< Peer listen port */
    uint8_t  status;                            /**< keel_peer_status_t */
    uint8_t  source;                            /**< keel_peer_source_t */
} keel_cluster_peer_snapshot_t;

/**
 * @brief Sync response payload.
 */
typedef struct __attribute__((packed)) keel_cluster_sync_response {
    uint64_t config_checksum; /**< Responder config checksum */
    keel_cluster_sync_config_t config; /**< Reconciliable config snapshot */
    uint16_t peer_count;      /**< Number of valid entries in peers[] */
    uint16_t _pad;
    keel_cluster_peer_snapshot_t peers[KEEL_CLUSTER_MAX_PEERS];
} keel_cluster_sync_response_t;

/**
 * @brief Vote request payload for leader election.
 */
typedef struct __attribute__((packed)) keel_cluster_vote_request {
    uint64_t term;                                    /**< Candidate's election term */
    char     candidate_id[KEEL_CLUSTER_MAX_NODE_ID];  /**< Requesting node's ID */
} keel_cluster_vote_request_t;

/**
 * @brief Vote response payload.
 */
typedef struct __attribute__((packed)) keel_cluster_vote_response {
    uint64_t term;          /**< Responder's current term (may be > request term) */
    uint8_t  vote_granted;  /**< 1 = granted, 0 = denied */
    uint8_t  _pad[7];
} keel_cluster_vote_response_t;

/**
 * @brief 2PC prepare payload: leader proposes a config-change transaction.
 *
 * Peers that accept increment their pending transaction slot.  They reply
 * with KEEL_CLUSTER_MSG_CONFIG_ACK.  The leader waits for quorum accepts
 * before sending KEEL_CLUSTER_MSG_CONFIG_COMMIT.
 */
typedef struct __attribute__((packed)) keel_cluster_config_prepare {
    uint64_t term;          /**< Leader's current election term */
    uint64_t txn_id;        /**< Monotonic transaction counter (leader-scoped) */
    uint64_t new_checksum;  /**< Proposed config checksum hint (0 = reload) */
} keel_cluster_config_prepare_t;

/**
 * @brief 2PC commit/abort payload sent after quorum is reached (or not).
 */
typedef struct __attribute__((packed)) keel_cluster_config_commit {
    uint64_t term;
    uint64_t txn_id;
    uint8_t  commit;    /**< 1 = commit, 0 = abort */
    uint8_t  _pad[7];
} keel_cluster_config_commit_t;

/**
 * @brief Peer response to KEEL_CLUSTER_MSG_CONFIG_PREPARE.
 */
typedef struct __attribute__((packed)) keel_cluster_config_ack {
    uint64_t term;
    uint64_t txn_id;
    uint8_t  accepted;  /**< 1 = accepted, 0 = rejected */
    uint8_t  _pad[7];
} keel_cluster_config_ack_t;

/**
 * @brief Server topology change notification payload.
 */
typedef struct __attribute__((packed)) keel_cluster_server_notify {
    uint8_t  action;            /**< 0=add, 1=remove, 2=update */
    uint8_t  role;              /**< keel_server_role_t */
    uint16_t port;              /**< Server port */
    uint32_t weight;            /**< Load balancing weight */
    char     host[128];         /**< Server hostname */
} keel_cluster_server_notify_t;

/* ============================================================================
 * Peer Descriptor
 * ============================================================================ */

/**
 * @brief Runtime state for a single cluster peer.
 */
typedef struct keel_cluster_peer {
    char     node_id[KEEL_CLUSTER_MAX_NODE_ID];   /**< Peer's self-assigned ID */
    char     addr[KEEL_CLUSTER_MAX_ADDR];          /**< IP address or hostname */
    uint16_t port;                                  /**< Cluster port */
    bool     active;                                /**< Slot in use */
    uint8_t  source;                                /**< keel_peer_source_t */
    uint8_t  _pad0[7];
    uint64_t discovered_at_sec;                     /**< First discovery wall clock sec */

    /* Health tracking */
    _Atomic(int)      status;              /**< keel_peer_status_t */
    _Atomic(uint32_t) consecutive_failures; /**< Missed heartbeats */
    _Atomic(uint64_t) last_heartbeat_ns;   /**< Last successful heartbeat */
    _Atomic(uint64_t) last_latency_us;     /**< Round-trip time of last heartbeat */
    _Atomic(uint64_t) total_heartbeats;    /**< Lifetime heartbeat count */
    _Atomic(uint64_t) total_failures;      /**< Lifetime failure count */

    /* Peer state from last heartbeat */
    _Atomic(uint64_t) config_checksum;     /**< Peer's config checksum */
    _Atomic(uint64_t) uptime_sec;          /**< Peer's uptime */
    _Atomic(uint32_t) num_clients;         /**< Peer's active clients */
    _Atomic(uint32_t) num_backends;        /**< Peer's active backends */
    _Atomic(uint8_t)  engine_state;        /**< Peer's engine lifecycle state */

    /* Sync request cooldown — prevents both nodes from blocking each other
     * with simultaneous sync requests when their heartbeats overlap. */
    _Atomic(uint64_t) last_sync_request_ns; /**< Timestamp of last outbound sync request (ns) */
} keel_cluster_peer_t;

/* ============================================================================
 * Cluster Configuration
 * ============================================================================ */

/**
 * @brief Static configuration for cluster mode, loaded from INI [cluster].
 */
typedef struct keel_cluster_config {
    bool     enabled;                     /**< Master switch for cluster mode */
    char     node_id[KEEL_CLUSTER_MAX_NODE_ID]; /**< This node's unique identifier */
    char     listen_addr[KEEL_CLUSTER_MAX_ADDR]; /**< Bind address for cluster port */
    uint16_t listen_port;                  /**< Cluster communication port */
    uint32_t heartbeat_interval_ms;        /**< How often to send heartbeats */
    uint32_t heartbeat_timeout_ms;         /**< Timeout before marking peer suspect */
    uint32_t failure_threshold;            /**< Consecutive failures before DOWN */
    bool     auto_sync;                    /**< Auto-sync config on checksum mismatch */

    /* Wire-protocol compression (WAN / cross-region deployments) */
    uint8_t  compress_codec;               /**< KEEL_CLUSTER_COMPRESS_NONE/ZLIB/ZSTD */
    uint32_t compress_threshold_bytes;     /**< Only compress payloads >= this size */

    /* Leader Election */
    bool     election_enabled;             /**< Enable leader election (needs 3+ nodes for split-brain safety) */
    char     election_state_path[256];     /**< Persist term+voted_for across restarts; empty = auto (/tmp/...) */

    /* Floating VIP (bare-metal / VM deployments — Linux only) */
    char     vip[64];             /**< e.g. "192.168.1.100/24"; empty = disabled */
    char     vip_interface[32];   /**< e.g. "eth0"; empty = disabled */

    /** Bootstrap peer list (up to KEEL_CLUSTER_MAX_PEERS) */
    struct {
        char     addr[KEEL_CLUSTER_MAX_ADDR];
        uint16_t port;
    } initial_peers[KEEL_CLUSTER_MAX_PEERS];
    size_t   initial_peer_count;
} keel_cluster_config_t;

/** Default cluster configuration. */
#define KEEL_CLUSTER_CONFIG_DEFAULT {                               \
    .enabled = false,                                             \
    .node_id = "",                                                \
    .listen_addr = "0.0.0.0",                                     \
    .listen_port = 9100,                                          \
    .heartbeat_interval_ms = 1000,                                \
    .heartbeat_timeout_ms = 5000,                                 \
    .failure_threshold = 3,                                       \
    .auto_sync = true,                                            \
    .compress_codec = KEEL_CLUSTER_COMPRESS_NONE,                 \
    .compress_threshold_bytes = 256,                              \
    .initial_peer_count = 0,                                      \
    .election_enabled = true,                                     \
    .election_state_path = "",                                    \
    .vip = "",                                                    \
    .vip_interface = "",                                          \
}

/* ============================================================================
 * Cluster Handle
 * ============================================================================ */

/** Opaque cluster manager handle. */
typedef struct keel_cluster keel_cluster_t;

/* ============================================================================
 * Lifecycle API
 * ============================================================================ */

/**
 * @brief Create a cluster manager from the given configuration.
 *
 * The manager is created in a stopped state. Call keel_cluster_start() to
 * begin heartbeating and listening for peer connections.
 *
 * @param config Cluster configuration.
 * @return Cluster handle, or NULL on allocation failure.
 */
keel_cluster_t* keel_cluster_create(const keel_cluster_config_t* config);

/**
 * @brief Start the cluster manager.
 *
 * Binds the cluster listening socket, spawns the cluster thread, and begins
 * heartbeating to all known peers.
 *
 * @param cluster Cluster handle.
 * @return 0 on success, -1 on error.
 */
int keel_cluster_start(keel_cluster_t* cluster);

/**
 * @brief Stop the cluster manager and leave the cluster gracefully.
 *
 * Sends LEAVE messages to all peers, stops the heartbeat timer, closes the
 * listening socket, and joins the cluster thread.
 *
 * @param cluster Cluster handle.
 */
void keel_cluster_stop(keel_cluster_t* cluster);

/**
 * @brief Destroy the cluster manager and free all resources.
 *
 * @param cluster Cluster handle, or NULL (no-op).
 */
void keel_cluster_destroy(keel_cluster_t* cluster);

/**
 * @brief Return a read-only pointer to the cluster's static configuration.
 *
 * The pointer is valid for the lifetime of @p cluster.  Callers must not
 * modify the returned config.
 *
 * @param cluster Cluster handle.
 * @return Pointer to the config, or NULL if @p cluster is NULL.
 */
const keel_cluster_config_t* keel_cluster_get_config(const keel_cluster_t* cluster);

/* ============================================================================
 * Peer Management API
 * ============================================================================ */

/**
 * @brief Get the number of active peers (including this node).
 *
 * @param cluster Cluster handle.
 * @return Number of active peers.
 */
size_t keel_cluster_peer_count(const keel_cluster_t* cluster);

/**
 * @brief Get a read-only snapshot of a specific peer.
 *
 * @param cluster Cluster handle.
 * @param index Peer index (0 to peer_count-1).
 * @param[out] peer Output peer descriptor.
 * @return true if the peer exists and was copied, false otherwise.
 */
bool keel_cluster_get_peer(const keel_cluster_t* cluster, size_t index,
                           keel_cluster_peer_t* peer);

/**
 * @brief Add a peer to the cluster at runtime.
 *
 * @param cluster Cluster handle.
 * @param addr Peer address (IP or hostname).
 * @param port Peer cluster port.
 * @return 0 on success, -1 if the peer table is full.
 */
int keel_cluster_add_peer(keel_cluster_t* cluster, const char* addr, uint16_t port);

/**
 * @brief Remove a peer from the cluster.
 *
 * @param cluster Cluster handle.
 * @param addr Peer address.
 * @param port Peer cluster port.
 * @return 0 on success, -1 if peer not found.
 */
int keel_cluster_remove_peer(keel_cluster_t* cluster, const char* addr, uint16_t port);

/* ============================================================================
 * Gossip / Sync API
 * ============================================================================ */

/**
 * @brief Update the local config checksum.
 *
 * Called after any configuration change so that peers can detect the mismatch
 * and request a sync.
 *
 * @param cluster Cluster handle.
 * @param checksum New XXHash64 config checksum.
 */
void keel_cluster_set_config_checksum(keel_cluster_t* cluster, uint64_t checksum);

/**
 * @brief Get the current local config checksum.
 *
 * @param cluster Cluster handle.
 * @return Current config checksum.
 */
uint64_t keel_cluster_get_config_checksum(const keel_cluster_t* cluster);

/**
 * @brief Local runtime cluster configuration snapshot.
 *
 * Represents this node's current runtime-safe config values after any
 * reconciliation that may have been applied via sync payloads.
 */
typedef struct keel_cluster_runtime_config {
    char     node_id[KEEL_CLUSTER_MAX_NODE_ID]; /**< Local node id */
    char     listen_addr[KEEL_CLUSTER_MAX_ADDR]; /**< Local listen address */
    uint16_t listen_port;                        /**< Local listen port */
    uint16_t active_peers;                       /**< Currently active peers */
    uint32_t heartbeat_interval_ms;              /**< Local heartbeat interval */
    uint32_t heartbeat_timeout_ms;               /**< Local heartbeat timeout */
    uint32_t failure_threshold;                  /**< Local failure threshold */
    uint64_t config_checksum;                    /**< Local config checksum */
    uint8_t  auto_sync;                          /**< Local auto_sync setting */
    uint8_t  running;                            /**< Cluster thread running */
} keel_cluster_runtime_config_t;

/**
 * @brief Get a snapshot of local runtime cluster config values.
 *
 * @param cluster Cluster handle.
 * @param[out] config Output snapshot.
 * @return true on success, false on invalid input.
 */
bool keel_cluster_get_runtime_config(const keel_cluster_t* cluster,
                                     keel_cluster_runtime_config_t* config);

/**
 * @brief Broadcast a server topology change to all peers.
 *
 * @param cluster Cluster handle.
 * @param action  0=add, 1=remove, 2=update.
 * @param host    Server hostname.
 * @param port    Server port.
 * @param role    Server role.
 * @param weight  Server weight.
 * @return 0 on success, -1 on error.
 */
int keel_cluster_notify_server_change(keel_cluster_t* cluster,
                                      uint8_t action,
                                      const char* host, uint16_t port,
                                      uint8_t role, uint32_t weight);

/* ============================================================================
 * Stats / Introspection
 * ============================================================================ */

/**
 * @brief Cluster-wide statistics snapshot.
 */
typedef struct keel_cluster_stats {
    size_t   total_peers;            /**< Total peers (all states) */
    size_t   peers_up;               /**< Peers in UP state */
    size_t   peers_suspect;          /**< Peers in SUSPECT state */
    size_t   peers_down;             /**< Peers in DOWN state */
    size_t   peers_left;             /**< Peers in LEFT state */
    size_t   peers_unknown;          /**< Peers in UNKNOWN state */
    size_t   discovered_peers;       /**< Peers learned via JOIN/GOSSIP */
    uint64_t heartbeats_sent;        /**< Total heartbeats sent */
    uint64_t heartbeats_received;    /**< Total heartbeats received */
    uint64_t sync_requests_sent;     /**< Config sync requests sent */
    uint64_t sync_responses_sent;    /**< Config sync responses sent */
    uint64_t discovered_peers_total; /**< Lifetime discovered peers count */
    uint64_t config_reconciliations; /**< Times local safe config was reconciled */
    uint64_t last_sync_apply_ns;     /**< Last successful sync payload apply time */
    uint64_t server_notifications;   /**< Server change notifications sent */
    uint32_t local_heartbeat_interval_ms; /**< Current local heartbeat interval */
    uint32_t local_heartbeat_timeout_ms;  /**< Current local heartbeat timeout */
    uint32_t local_failure_threshold;     /**< Current local failure threshold */
    uint8_t  local_auto_sync;             /**< Current local auto_sync */

    /* Election statistics */
    uint64_t elections_started;           /**< Times this node started an election */
    uint64_t elections_won;               /**< Times this node became leader */
    uint64_t votes_granted;               /**< Votes granted to other candidates */
    uint64_t votes_denied;                /**< Votes denied to other candidates */
    uint64_t leader_stepdowns;            /**< Times this node stepped down from leader */
    keel_cluster_role_t local_role;       /**< Current node role */
    uint64_t local_term;                  /**< Current election term */
    char     local_leader_id[KEEL_CLUSTER_MAX_NODE_ID]; /**< Known leader node_id, or empty */
} keel_cluster_stats_t;

/**
 * @brief Collect cluster statistics.
 *
 * @param cluster Cluster handle.
 * @param[out] stats Output statistics snapshot.
 */
void keel_cluster_get_stats(const keel_cluster_t* cluster, keel_cluster_stats_t* stats);

/**
 * @brief Callback type for querying live connection counts from the engine.
 *
 * Used to populate the num_clients / num_backends fields in outgoing
 * heartbeat messages so that peer nodes can see our load without
 * creating a hard dependency between cluster.c and engine.h.
 *
 * @param user_data Opaque pointer supplied to keel_cluster_set_stats_cb().
 * @param[out] out_clients  Number of active client (frontend) connections.
 * @param[out] out_backends Number of active backend connections.
 * @param[out] out_servers  Number of configured backend servers.
 */
typedef void (*keel_cluster_stats_cb_t)(void* user_data,
                                        uint32_t* out_clients,
                                        uint32_t* out_backends,
                                        uint32_t* out_servers);

/**
 * @brief Register a live-stats callback so heartbeats carry real load data.
 *
 * Replaces the default zeros in heartbeat num_clients / num_backends /
 * num_servers fields with values supplied by @p cb at heartbeat time.
 * Pass NULL to unregister (zeros are used again).
 *
 * @param cluster   Cluster handle.
 * @param cb        Callback function (may be NULL).
 * @param user_data Opaque pointer passed back to @p cb.
 */
void keel_cluster_set_stats_cb(keel_cluster_t* cluster,
                                keel_cluster_stats_cb_t cb,
                                void* user_data);

/**
 * @brief Callback type for receiving server topology change notifications.
 *
 * Invoked when a NOTIFY_SERVER message is received from a peer.
 *
 * @param user_data Opaque pointer supplied to keel_cluster_set_server_notify_cb().
 * @param action    0 = add, 1 = remove, 2 = update.
 * @param host      Affected server hostname or IP.
 * @param port      Affected server port.
 * @param role      Server role (0 = primary, 1 = replica, etc.).
 * @param weight    Server routing weight.
 */
typedef void (*keel_cluster_server_notify_cb_t)(void* user_data,
                                                uint8_t action,
                                                const char* host,
                                                uint16_t port,
                                                uint8_t role,
                                                uint32_t weight);

/**
 * @brief Register a server-topology-change callback.
 *
 * When a NOTIFY_SERVER message arrives from a peer, @p cb is called in the
 * cluster thread context.  The caller must ensure any shared data access is
 * thread-safe.  Pass NULL to unregister.
 *
 * @param cluster   Cluster handle.
 * @param cb        Callback function (may be NULL).
 * @param user_data Opaque pointer passed back to @p cb.
 */
void keel_cluster_set_server_notify_cb(keel_cluster_t* cluster,
                                       keel_cluster_server_notify_cb_t cb,
                                       void* user_data);

/**
 * @brief Check if cluster mode is enabled and running.
 *
 * @param cluster Cluster handle (may be NULL).
 * @return true if cluster is active.
 */
bool keel_cluster_is_active(const keel_cluster_t* cluster);

/* ============================================================================
 * Leader Election API
 * ============================================================================ */

/**
 * @brief Get the current node's role in the election state machine.
 *
 * Returns KEEL_CLUSTER_ROLE_FOLLOWER if cluster is NULL or election is
 * disabled.
 *
 * @param cluster Cluster handle (may be NULL).
 * @return Current role.
 */
keel_cluster_role_t keel_cluster_get_role(const keel_cluster_t* cluster);

/**
 * @brief Get the current election term.
 *
 * @param cluster Cluster handle (may be NULL).
 * @return Current term number, or 0 if election is disabled.
 */
uint64_t keel_cluster_get_term(const keel_cluster_t* cluster);

/**
 * @brief Copy the current known leader node_id into a caller-supplied buffer.
 *
 * Writes an empty string if no leader is known or cluster is NULL.
 *
 * @param cluster Cluster handle (may be NULL).
 * @param buf     Output buffer.
 * @param bufsize Capacity of buf (including NUL terminator).
 */
void keel_cluster_get_leader_id(const keel_cluster_t* cluster,
                                char* buf, size_t bufsize);
int  keel_cluster_quorum_commit(keel_cluster_t* cluster, uint64_t new_checksum);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CLUSTER_H */
