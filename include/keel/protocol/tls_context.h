/**
 * @file tls_context.h
 * @brief Userspace TLS session management and kTLS handoff API.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * KEEL's TLS model is intentionally split into two phases. The handshake and any
 * unsupported steady-state traffic run through userspace OpenSSL objects backed by
 * memory BIOs so the reactor can stay non-blocking and transport-agnostic. Once the
 * handshake succeeds, the implementation may optionally hand the live session keys
 * to Linux kTLS so encryption/decryption can move into the kernel and preserve the
 * proxy's zero-copy fast path. This API exposes that phased design explicitly.
 */

#ifndef KEEL_TLS_CONTEXT_H
#define KEEL_TLS_CONTEXT_H

#include "keel_types.h"
#include "keel_error.h"

#include <openssl/ssl.h>
#include <openssl/x509.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * TLS Configuration Types
 * ============================================================================ */

/**
 * @brief TLS mode enumeration
 */
typedef enum keel_tls_mode {
    KEEL_TLS_DISABLE = 0,   /**< No TLS (plaintext only) */
    KEEL_TLS_PREFER,        /**< TLS if available, fallback to plaintext */
    KEEL_TLS_REQUIRE,       /**< TLS mandatory, reject plaintext */
} keel_tls_mode_t;

/**
 * @brief TLS peer verification mode
 */
typedef enum keel_tls_verify_mode {
    KEEL_TLS_VERIFY_NONE = 0,       /**< No peer verification */
    KEEL_TLS_VERIFY_OPTIONAL,       /**< Peer cert optional but verified if sent */
    KEEL_TLS_VERIFY_REQUIRED,       /**< Peer cert required and verified */
} keel_tls_verify_mode_t;

/**
 * @brief TLS protocol version constraints
 */
typedef enum keel_tls_version {
    KEEL_TLS_VERSION_AUTO = 0,      /**< Auto-negotiate (TLS 1.2+) */
    KEEL_TLS_VERSION_1_2 = 0x0303,  /**< TLS 1.2 minimum */
    KEEL_TLS_VERSION_1_3 = 0x0304,  /**< TLS 1.3 minimum */
} keel_tls_version_t;

/**
 * @brief TLS configuration for listener or backend
 */
typedef struct keel_tls_config {
    keel_tls_mode_t         mode;               /**< TLS mode (disable/prefer/require) */
    keel_tls_verify_mode_t  verify_peer;        /**< Peer verification mode */
    keel_tls_version_t      min_version;        /**< Minimum TLS version */
    keel_tls_version_t      max_version;        /**< Maximum TLS version (0=auto) */
    
    char*                   cert_file;          /**< Path to certificate file (PEM) */
    char*                   key_file;           /**< Path to private key file (PEM) */
    char*                   ca_file;            /**< Path to CA certificate file (PEM) */
    char*                   ciphers;            /**< TLS 1.2 cipher list (OpenSSL format) */
    char*                   ciphersuites;       /**< TLS 1.3 ciphersuites (colon-separated) */
    
    bool                    ktls_enabled;       /**< Enable Kernel TLS acceleration */
    size_t                  read_timeout_ms;    /**< TLS read timeout */
    size_t                  handshake_timeout_ms; /**< TLS handshake timeout */
} keel_tls_config_t;

/* ============================================================================
 * TLS Context (Per-Connection)
 * ============================================================================ */

/**
 * @brief Opaque TLS context handle
 *
 * Represents a TLS session for a single connection. Tracks:
 * - OpenSSL SSL/BIO objects
 * - kTLS state (if available)
 * - Protocol version, cipher, session state
 */
typedef struct keel_tls_context keel_tls_context_t;

/**
 * @brief TLS context state
 */
typedef enum keel_tls_state {
    KEEL_TLS_STATE_INIT = 0,        /**< Initial state */
    KEEL_TLS_STATE_HANDSHAKE,       /**< TLS handshake in progress */
    KEEL_TLS_STATE_ESTABLISHED,     /**< TLS handshake complete */
    KEEL_TLS_STATE_KTLS_ACTIVE,     /**< Kernel TLS activated (kernel handles encryption) */
    KEEL_TLS_STATE_SHUTDOWN,        /**< TLS shutdown initiated */
    KEEL_TLS_STATE_CLOSED,          /**< TLS closed */
    KEEL_TLS_STATE_ERROR,           /**< Error state */
} keel_tls_state_t;

/**
 * @brief Get TLS context state
 *
 * @param ctx TLS context
 * @return Current state
 */
keel_tls_state_t keel_tls_context_state(const keel_tls_context_t* ctx);

/**
 * @brief Get negotiated TLS version (e.g., 0x0304 for TLS 1.3)
 *
 * @param ctx TLS context
 * @return TLS version or 0 if not negotiated
 */
uint16_t keel_tls_context_version(const keel_tls_context_t* ctx);

/**
 * @brief Get negotiated cipher name (e.g., "TLS_AES_256_GCM_SHA384")
 *
 * @param ctx TLS context
 * @return Cipher name (internal pointer, valid for context lifetime)
 */
const char* keel_tls_context_cipher(const keel_tls_context_t* ctx);

/**
 * @brief Check if Kernel TLS acceleration is active
 *
 * When true, the kernel is handling encryption/decryption and zero-copy
 * splice() may be used on the socket.
 *
 * @param ctx TLS context
 * @return true if kTLS is active, false otherwise
 */
bool keel_tls_context_ktls_active(const keel_tls_context_t* ctx);

/* ============================================================================
 * TLS Peer Information (After Handshake)
 * ============================================================================ */

/**
 * @brief TLS peer certificate information
 */
typedef struct keel_tls_peer_info {
    bool                    has_cert;               /**< Peer sent a certificate */
    char                    subject[512];           /**< Subject DN */
    char                    issuer[512];            /**< Issuer DN */
    X509*                   cert;                   /**< OpenSSL cert object (internal) */
} keel_tls_peer_info_t;

/**
 * @brief Get peer certificate information
 *
 * @param ctx TLS context.
 * @param[out] info Output peer info structure.
 * @return KEEL_OK on success, error on failure/no peer cert
 */
keel_error_t keel_tls_context_peer_info(
    const keel_tls_context_t* ctx,
    keel_tls_peer_info_t* info
);

/* ============================================================================
 * TLS Context Lifecycle
 * ============================================================================ */

/**
 * @brief Create a TLS context for a socket
 *
 * Initializes OpenSSL BIOs and prepares for TLS handshake.
 * Does not perform handshake yet.
 *
 * @param config TLS configuration.
 * @param is_server `true` for server-side use, `false` for client-side use.
 * @param[out] ctx_out Output TLS context handle.
 * @return KEEL_OK on success
 *
 * @note Caller must free context with keel_tls_context_destroy()
 */
keel_error_t keel_tls_context_create(
    const keel_tls_config_t* config,
    bool is_server,
    keel_tls_context_t** ctx_out
);

/**
 * @brief Destroy a TLS context
 *
 * Closes OpenSSL BIOs and frees all associated resources.
 *
 * @param ctx TLS context to destroy
 */
void keel_tls_context_destroy(keel_tls_context_t* ctx);

/* ============================================================================
 * TLS Handshake (Async)
 * ============================================================================ */

/**
 * @brief TLS handshake result
 */
typedef enum keel_tls_hs_result {
    KEEL_TLS_HS_COMPLETE = 0,      /**< Handshake complete, ready for kTLS */
    KEEL_TLS_HS_WANT_READ = 1,     /**< Need to read more data for handshake */
    KEEL_TLS_HS_WANT_WRITE = 2,    /**< Need to write handshake data */
    KEEL_TLS_HS_ERROR = -1,        /**< Handshake failed */
} keel_tls_hs_result_t;

/**
 * @brief Perform TLS handshake step (non-blocking)
 *
 * Makes one step of progress on the TLS handshake. Should be called
 * repeatedly until it returns KEEL_TLS_HS_COMPLETE.
 *
 * @param ctx TLS context
 * @param return_info Optional output for error details
 * @return Handshake result (see keel_tls_hs_result_t)
 *
 * @note This is memory-BIO based (no socket I/O). Caller handles socket I/O.
 */
keel_tls_hs_result_t keel_tls_handshake_step(
    keel_tls_context_t* ctx,
    char* return_info
);

/**
 * @brief Get data to send during handshake
 *
 * After keel_tls_handshake_step() returns KEEL_TLS_HS_WANT_WRITE,
 * this retrieves the handshake data to send on the socket.
 *
 * @param ctx TLS context.
 * @param[out] buf Output buffer.
 * @param buf_size Buffer size
 * @return Number of bytes to send (0 if nothing to send)
 */
ssize_t keel_tls_get_handshake_data(
    keel_tls_context_t* ctx,
    uint8_t* buf,
    size_t buf_size
);

/**
 * @brief Feed received handshake data into TLS context
 *
 * Processes data received from the socket during handshake.
 *
 * @param ctx TLS context
 * @param data Received data
 * @param len Data length
 * @return KEEL_OK on success
 */
keel_error_t keel_tls_feed_handshake_data(
    keel_tls_context_t* ctx,
    const uint8_t* data,
    size_t len
);

/* ============================================================================
 * Data Transfer (After Handshake)
 * ============================================================================ */

/**
 * @brief Read decrypted application data
 *
 * Reads plaintext data that has been decrypted from the TLS stream.
 * May not consume all buffered data in one call.
 *
 * @param ctx TLS context.
 * @param[out] buf Output buffer.
 * @param buf_size Buffer size
 * @return Number of bytes read (negative = error)
 *
 * @note This is for the initial TLS handshake phase. After kTLS is activated,
 *       the kernel handles decryption transparently.
 */
ssize_t keel_tls_read_decrypted(
    keel_tls_context_t* ctx,
    uint8_t* buf,
    size_t buf_size
);

/**
 * @brief Write plaintext to be encrypted and sent
 *
 * Buffers plaintext to be encrypted during TLS record formation.
 *
 * @param ctx TLS context
 * @param data Plaintext data
 * @param len Data length
 * @return Number of bytes buffered (negative = error)
 */
ssize_t keel_tls_write_plaintext(
    keel_tls_context_t* ctx,
    const uint8_t* data,
    size_t len
);

/**
 * @brief Get encrypted data to send
 *
 * Retrieves encrypted TLS records ready to send on socket.
 *
 * @param ctx TLS context
 * @param buf Output buffer
 * @param buf_size Buffer size
 * @return Number of encrypted bytes to send
 */
ssize_t keel_tls_get_encrypted_to_send(
    keel_tls_context_t* ctx,
    uint8_t* buf,
    size_t buf_size
);

/**
 * @brief Feed encrypted data received from socket
 *
 * Processes encrypted TLS records received from the socket.
 *
 * @param ctx TLS context
 * @param data Encrypted data
 * @param len Data length
 * @return KEEL_OK on success
 */
keel_error_t keel_tls_feed_encrypted(
    keel_tls_context_t* ctx,
    const uint8_t* data,
    size_t len
);

/* ============================================================================
 * kTLS Acceleration (After Handshake)
 * ============================================================================ */

/**
 * @brief Activate Kernel TLS on a connection (worker thread function)
 *
 * Called by worker threads after TLS handshake completes and we have the
 * socket file descriptor. Attempts to install kTLS keys in the kernel.
 *
 * On success:
 * - Context state changes to KEEL_TLS_STATE_KTLS_ACTIVE
 * - Kernel handles encryption/decryption transparently
 * - Zero-copy splice() can be used for data forwarding
 *
 * On failure:
 * - Falls back to userspace TLS
 * - Data still encrypted/decrypted, just in userspace
 *
 * @param ctx TLS context (must be ESTABLISHED after handshake)
 * @param fd Socket file descriptor (client or backend connection)
 * @return KEEL_OK if kTLS installed, KEEL_ERR_* if unavailable/failed
 *
 * @note This is called from worker threads, not from reactor callbacks
 * @note Requires Linux 4.17+ for kernel TLS support
 * @note Cipher must be kTLS-compatible (AES-GCM, CHACHA20-POLY1305)
 */
keel_error_t keel_tls_context_activate_ktls(
    keel_tls_context_t* ctx,
    int fd
);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Initialize TLS subsystem
 *
 * Must be called once at startup before creating any contexts.
 * Initializes OpenSSL, detects kTLS support, etc.
 *
 * @return KEEL_OK on success
 */
keel_error_t keel_tls_init(void);

/**
 * @brief Cleanup TLS subsystem
 *
 * Must be called at shutdown to free OpenSSL resources.
 */
void keel_tls_cleanup(void);

/**
 * @brief Check if Kernel TLS is available on this platform
 *
 * Returns true if the kernel supports SOL_TLS socket option.
 *
 * @return true if kTLS available, false otherwise
 */
bool keel_ktls_available(void);

/**
 * @brief Check if a specific cipher is compatible with kTLS
 *
 * Not all OpenSSL ciphers work with kernel TLS. This checks if a
 * specific cipher (by name) is known to be kTLS-compatible.
 *
 * @param cipher_name OpenSSL cipher name
 * @return true if kTLS compatible, false otherwise
 */
bool keel_ktls_cipher_compatible(const char* cipher_name);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief TLS statistics
 */
typedef struct keel_tls_stats {
    uint64_t    connections_total;          /**< Total TLS connections */
    uint64_t    connections_succeeded;      /**< Successfully negotiated */
    uint64_t    connections_failed;         /**< Handshake failures */
    uint64_t    ktls_active;                /**< Active kTLS connections */
    uint64_t    ktls_fallback;              /**< Failed to activate kTLS */
    uint64_t    bytes_encrypted;            /**< Bytes encrypted */
    uint64_t    bytes_decrypted;            /**< Bytes decrypted */
    uint64_t    cert_reloads;              /**< Certificate reload events */
    uint64_t    cert_reload_failures;      /**< Failed certificate reloads */
    uint64_t    downgrade_rejected;        /**< Plaintext connections rejected (REQUIRE mode) */
} keel_tls_stats_t;

/**
 * @brief Get TLS statistics
 *
 * @return Global TLS statistics
 */
keel_tls_stats_t keel_tls_get_stats(void);

/**
 * @brief Reset TLS statistics
 */
void keel_tls_reset_stats(void);

/**
 * @brief Reload TLS certificates from disk
 *
 * Creates new SSL_CTX objects from the provided configuration.
 * Existing connections continue with the old context; new connections
 * use the reloaded certificates. Thread-safe.
 *
 * @param server_config Server TLS config (may be NULL to skip server reload)
 * @param client_config Client TLS config (may be NULL to skip client reload)
 * @return KEEL_OK on success, KEEL_ERR_TLS on failure
 */
keel_error_t keel_tls_reload_certs(
    const keel_tls_config_t* server_config,
    const keel_tls_config_t* client_config
);

/**
 * @brief Increment the downgrade-rejected counter
 *
 * Called when a plaintext connection is rejected because tls_mode=require.
 */
void keel_tls_stat_downgrade_rejected(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_TLS_CONTEXT_H */
