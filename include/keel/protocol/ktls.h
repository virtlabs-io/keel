/**
 * @file ktls.h
 * @brief Linux kernel-TLS detection and installation API.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * kTLS exists in KEEL to reconcile two normally competing goals: transport-layer
 * encryption and zero-copy forwarding. The handshake still happens in userspace so
 * OpenSSL can negotiate ciphers and certificates, but compatible sessions may then
 * be migrated into kernel-managed TX/RX state. This header describes that boundary
 * and the inspection hooks used by the rest of the transport stack.
 */

#ifndef KEEL_KTLS_H
#define KEEL_KTLS_H

#include "keel_types.h"
#include "keel_error.h"
#include "tls_context.h"

#include <openssl/ssl.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * kTLS Availability & Detection
 * ============================================================================ */

/**
 * @brief Detect if kTLS is available on this system
 *
 * Checks:
 * - Linux platform
 * - Kernel version 4.17+
 * - SOL_TLS socket option support (via setsockopt test)
 * - CONFIG_TLS kernel config enabled
 *
 * This should be called at startup to determine overall capability.
 *
 * @return true if kTLS can be used, false otherwise
 */
bool keel_ktls_detect_support(void);

/**
 * @brief Get detailed kTLS support information
 *
 * For debugging/logging purposes.
 *
 * @param[out] major_out Kernel major version.
 * @param[out] minor_out Kernel minor version.
 * @param[out] patch_out Kernel patch version.
 * @return true if kTLS supported, false otherwise
 */
bool keel_ktls_get_kernel_version(
    int* major_out,
    int* minor_out,
    int* patch_out
);

/* ============================================================================
 * kTLS Cipher Compatibility
 * ============================================================================ */

/**
 * @brief Supported kTLS cipher suites
 *
 * Not all OpenSSL ciphers work with kernel TLS. This enum lists
 * the known supported combinations.
 */
typedef enum keel_ktls_cipher_type {
    KEEL_KTLS_CIPHER_AES_128_GCM = 0,    /**< AES-128-GCM (TLS 1.2) */
    KEEL_KTLS_CIPHER_AES_256_GCM,        /**< AES-256-GCM (TLS 1.2) */
    KEEL_KTLS_CIPHER_CHACHA20_POLY1305,  /**< CHACHA20-POLY1305 (TLS 1.2) */
    KEEL_KTLS_CIPHER_AES_128_CCM,        /**< AES-128-CCM (TLS 1.2 - limited support) */
    KEEL_KTLS_CIPHER_AES_256_CCM,        /**< AES-256-CCM (TLS 1.2 - limited support) */
    KEEL_KTLS_CIPHER_TLS13_AES_128_GCM,  /**< TLS 1.3 AES-128-GCM */
    KEEL_KTLS_CIPHER_TLS13_AES_256_GCM,  /**< TLS 1.3 AES-256-GCM */
    KEEL_KTLS_CIPHER_TLS13_CHACHA20,     /**< TLS 1.3 CHACHA20-POLY1305 */
    KEEL_KTLS_CIPHER_UNKNOWN = -1,
} keel_ktls_cipher_type_t;

/**
 * @brief Identify kTLS cipher type from OpenSSL descriptor
 *
 * Maps OpenSSL cipher suite to a known kTLS type.
 * Returns KEEL_KTLS_CIPHER_UNKNOWN if not recognized or unsupported.
 *
 * @param ssl OpenSSL SSL object (after handshake)
 * @return kTLS cipher type, or KEEL_KTLS_CIPHER_UNKNOWN
 */
keel_ktls_cipher_type_t keel_ktls_identify_cipher(SSL* ssl);

/**
 * @brief Check if a cipher is known to be kTLS-compatible
 *
 * @param cipher_name OpenSSL cipher name (e.g., "TLS_AES_256_GCM_SHA384")
 * @return true if kTLS compatible on this kernel, false otherwise
 */
bool keel_ktls_cipher_supported(const char* cipher_name);

/* ============================================================================
 * kTLS Installation (After Handshake)
 * ============================================================================ */

/**
 * @brief Install kTLS TX (encryption) on a socket
 *
 * Called after TLS handshake completes. Extracts encryption keys from
 * the OpenSSL context and installs them in the kernel for TX encryption.
 *
 * Requirements:
 * - TLS handshake must be complete
 * - Cipher must be kTLS-compatible
 * - Socket must be connected and ready
 *
 * On success:
 * - Socket is now kernel-managed for TX
 * - Kernel handles encryption transparently
 * - Plaintext writes receive encrypted data on the line
 *
 * @param fd Socket file descriptor
 * @param ssl OpenSSL SSL object (after handshake)
 * @return KEEL_OK on success, KEEL_ERR_* on failure
 *
 * On error, the socket remains in userspace TLS mode (fallback).
 */
keel_error_t keel_ktls_install_tx(int fd, SSL* ssl);

/**
 * @brief Install kTLS RX (decryption) on a socket
 *
 * Called after TLS handshake completes. Similar to keel_ktls_install_tx()
 * but for RX direction.
 *
 * @param fd Socket file descriptor
 * @param ssl OpenSSL SSL object (after handshake)
 * @return KEEL_OK on success, KEEL_ERR_* on failure
 */
keel_error_t keel_ktls_install_rx(int fd, SSL* ssl);

/**
 * @brief Install kTLS bidirectional (both TX and RX)
 *
 * Convenience function combining TX and RX installation.
 * If either fails, the socket remains in userspace TLS.
 *
 * @param fd Socket file descriptor
 * @param ssl OpenSSL SSL object (after handshake)
 * @return KEEL_OK on success, KEEL_ERR_* on failure
 */
keel_error_t keel_ktls_install_bidirectional(int fd, SSL* ssl);

/* ============================================================================
 * kTLS Status & Inspection
 * ============================================================================ */

/**
 * @brief Check if kTLS is active on a socket
 *
 * @param fd Socket file descriptor
 * @return true if kTLS TX/RX is installed, false otherwise
 */
bool keel_ktls_is_active(int fd);

/**
 * @brief Get kTLS mode on a socket
 *
 * @param fd Socket file descriptor.
 * @param[out] tx_out Set to `true` when TX is kernel-managed.
 * @param[out] rx_out Set to `true` when RX is kernel-managed.
 * @return KEEL_OK on success
 */
keel_error_t keel_ktls_get_mode(int fd, bool* tx_out, bool* rx_out);

/* ============================================================================
 * Zero-Copy with kTLS (Splice Integration)
 * ============================================================================ */

/**
 * @brief Check if a socket can use splice with kTLS
 *
 * After kTLS is installed, splice() can be used on the socket,
 * but with some limitations:
 * - Record boundaries are maintained (max ~16KB per splice)
 * - Some kernel versions have restrictions
 *
 * @param fd Socket file descriptor
 * @return true if splice-compatible with kTLS, false otherwise
 */
bool keel_ktls_splice_compatible(int fd);

/**
 * @brief Optimal splice size for kTLS socket
 *
 * kTLS maintains TLS record boundaries (typically ~16KB).
 * Returning optimal size helps efficiency.
 *
 * @param fd Socket file descriptor
 * @return Recommended splice size in bytes (typically 16384)
 */
size_t keel_ktls_optimal_splice_size(int fd);

/* ============================================================================
 * kTLS Statistics & Debugging
 * ============================================================================ */

/**
 * @brief kTLS statistics
 */
typedef struct keel_ktls_stats {
    uint64_t    installations_attempted;    /**< Attempted setsockopt calls */
    uint64_t    installations_succeeded;    /**< Successful installations */
    uint64_t    installations_failed;       /**< Failed installations */
    uint64_t    cipher_incompatible;        /**< Rejected due to unsupported cipher */
    uint64_t    kernel_errors;              /**< Kernel setsockopt errors */
    uint64_t    splice_operations_total;    /**< Splice ops on kTLS sockets */
    uint64_t    splice_bytes_transferred;   /**< Bytes via kTLS-enabled splice */
} keel_ktls_stats_t;

/**
 * @brief Get kTLS statistics
 *
 * @return Global kTLS statistics
 */
keel_ktls_stats_t keel_ktls_get_stats(void);

/**
 * @brief Reset kTLS statistics
 */
void keel_ktls_reset_stats(void);

/* ============================================================================
 * Platform-Specific Implementation
 * ============================================================================ */

/**
 * @brief Platform detection macro
 */
#if defined(__linux__)
    #define KEEL_HAS_KTLS 1
#else
    #define KEEL_HAS_KTLS 0
#endif

/**
 * @brief kTLS initialization (called from tls_init)
 *
 * @return KEEL_OK on success
 */
keel_error_t keel_ktls_init(void);

/**
 * @brief kTLS cleanup (called from tls_cleanup)
 */
void keel_ktls_cleanup(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_KTLS_H */
