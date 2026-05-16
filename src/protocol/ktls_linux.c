/**
 * @file ktls_linux.c
 * @brief Linux-specific kernel-TLS installation and inspection logic.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This implementation is necessarily platform-specific because it reaches into the
 * Linux `SOL_TLS` interface and mirrors some kernel TLS crypto-info structures that
 * userspace must populate after a successful OpenSSL handshake. The code is careful
 * to treat kTLS as an optimization layer: installation failure should preserve a
 * functioning userspace TLS session instead of breaking connectivity.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "keel/protocol/ktls.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <linux/version.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* ============================================================================
 * kTLS Socket Options (from linux/tls.h)
 * ============================================================================ */

#if !defined(SOL_TLS)
#define SOL_TLS 282
#endif

#if !defined(TLS_TX)
#define TLS_TX 1
#endif

#if !defined(TLS_RX)
#define TLS_RX 2
#endif

/* TLS record types */
#define TLS_RECORD_TYPE_DATA 23
#define TLS_RECORD_TYPE_CONTROL 24

/* TLS versions */
#define TLS_VERSION_MINOR_1_2 3
#define TLS_VERSION_MINOR_1_3 4

/* Cipher offload flags */
#define TLS_CRYPTO_INFO_READY 1

/* Maximum IV size */
#define TLS_MAX_IV_SIZE 12

/* ============================================================================
 * OpenSSL Structures
 * ============================================================================ */

/* Forward declarations for internal OpenSSL access */

/* ============================================================================
 * Global kTLS State
 * ============================================================================ */

static struct {
    bool        initialized;
    bool        available;
    int         kernel_major;
    int         kernel_minor;
    int         kernel_patch;
    keel_ktls_stats_t stats;
} g_ktls_state = {
    .initialized = false,
    .available = false,
    .stats = {0}
};

static pthread_mutex_t g_ktls_lock = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================================
 * Kernel Version Detection
 * ============================================================================ */

/**
 * @brief Determine whether the build-time Linux headers imply baseline kTLS support.
 *
 * @return `true` when the detected kernel version is new enough for kTLS.
 */
static bool detect_kernel_version(void)
{
    /* Parse LINUX_VERSION_CODE macro */
    int version = LINUX_VERSION_CODE;
    g_ktls_state.kernel_major = (version >> 16) & 0xFF;
    g_ktls_state.kernel_minor = (version >> 8) & 0xFF;
    g_ktls_state.kernel_patch = version & 0xFF;

    /* kTLS available since 4.17 */
    if (g_ktls_state.kernel_major > 4) return true;
    if (g_ktls_state.kernel_major == 4 && g_ktls_state.kernel_minor >= 17) return true;

    return false;
}

/* ============================================================================
 * kTLS Configuration Structs (from linux/tls.h)
 * ============================================================================ */

struct tls_crypto_info {
    unsigned short version;
    unsigned short cipher_type;
};

struct tls_aead_params {
    /* Salt/IV constants */
    unsigned char iv[TLS_MAX_IV_SIZE];
    unsigned char key[32];  /* 256-bit max */
    unsigned char seq[8];   /* TLS sequence number */
};

struct tls12_crypto_info_aes_gcm_128 {
    struct tls_crypto_info info;
    unsigned char iv[TLS_MAX_IV_SIZE];
    unsigned char key[16];
    unsigned char salt[4];
    unsigned char rec_seq[8];
};

struct tls12_crypto_info_aes_gcm_256 {
    struct tls_crypto_info info;
    unsigned char iv[TLS_MAX_IV_SIZE];
    unsigned char key[32];
    unsigned char salt[4];
    unsigned char rec_seq[8];
};

struct tls12_crypto_info_chacha20_poly1305 {
    struct tls_crypto_info info;
    unsigned char iv[TLS_MAX_IV_SIZE];
    unsigned char key[32];
    unsigned char salt[12];
    unsigned char rec_seq[8];
};

/* Cipher type constants */
#define TLS_CIPHER_AES_GCM_128 51
#define TLS_CIPHER_AES_GCM_256 52
#define TLS_CIPHER_CHACHA20_POLY1305 54

#define TLS_CIPHER_AES_CCM_128 59
#define TLS_CIPHER_AES_CCM_256 60

#define TLS_CIPHER_SM4_GCM 82
#define TLS_CIPHER_SM4_CCM 83

/* TLS 1.3 cipher types */
#define TLS_CIPHER_TLS13_AES_128_GCM_SHA256 23
#define TLS_CIPHER_TLS13_AES_256_GCM_SHA384 24
#define TLS_CIPHER_TLS13_CHACHA20_POLY1305_SHA256 27

/* ============================================================================
 * Cipher Suite Mapping
 * ============================================================================ */

/**
 * @brief Get OpenSSL cipher suite ID
 */
static uint32_t get_openssl_cipher_id(SSL* ssl)
{
    const SSL_CIPHER* cipher = SSL_get_current_cipher(ssl);
    if (!cipher) return 0;
    
    return (uint32_t)SSL_CIPHER_get_id(cipher);
}

/**
 * @brief Map OpenSSL cipher to kTLS cipher type
 */
static int map_cipher_to_ktls_type(SSL* ssl, int tls_version)
{
    uint32_t cipher_id = get_openssl_cipher_id(ssl);

    /* TLS 1.2 ciphers */
    if (tls_version == TLS1_2_VERSION) {
        switch (cipher_id) {
            case 0x1301: /* TLS_AES_128_GCM_SHA256 (mapped from TLS 1.3 nomenclature) */
            case 0x00009C: /* TLS_RSA_WITH_AES_128_GCM_SHA256 */
            case 0x0000009E: /* TLS_DHE_RSA_WITH_AES_128_GCM_SHA256 */
            case 0x000000A8: /* TLS_DH_RSA_WITH_AES_128_GCM_SHA256 */
                return TLS_CIPHER_AES_GCM_128;

            case 0x1302: /* TLS_AES_256_GCM_SHA384 */
            case 0x00009D: /* TLS_RSA_WITH_AES_256_GCM_SHA384 */
            case 0x000000A3: /* TLS_DHE_RSA_WITH_AES_256_GCM_SHA384 */
                return TLS_CIPHER_AES_GCM_256;

            case 0x1303: /* TLS_CHACHA20_POLY1305_SHA256 */
            case 0x0000CCA9: /* TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 */
            case 0x0000CCAA: /* TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 */
                return TLS_CIPHER_CHACHA20_POLY1305;

            default:
                return -1;  /* Unsupported */
        }
    }

    /* TLS 1.3 ciphers */
    if (tls_version == TLS1_3_VERSION) {
        switch (cipher_id) {
            case 0x1301: /* TLS_AES_128_GCM_SHA256 */
                return TLS_CIPHER_TLS13_AES_128_GCM_SHA256;

            case 0x1302: /* TLS_AES_256_GCM_SHA384 */
                return TLS_CIPHER_TLS13_AES_256_GCM_SHA384;

            case 0x1303: /* TLS_CHACHA20_POLY1305_SHA256 */
                return TLS_CIPHER_TLS13_CHACHA20_POLY1305_SHA256;

            default:
                return -1;
        }
    }

    return -1;  /* Unsupported TLS version or cipher */
}

/* ============================================================================
 * Key Extraction from OpenSSL (Public API only)
 * ============================================================================ */

/**
 * @brief Select the PRF hash algorithm for a TLS 1.2 cipher suite.
 *
 * @param c OpenSSL cipher object from the active SSL session.
 * @return Pointer to EVP_sha384 if the cipher name contains "SHA384",
 *         otherwise pointer to EVP_sha256.
 */
static const EVP_MD* tls12_prf_md_for_cipher(const SSL_CIPHER* c)
{
    const char* name = c ? SSL_CIPHER_get_name(c) : NULL;
    if (!name) return EVP_sha256();
    return (strstr(name, "SHA384") != NULL) ? EVP_sha384() : EVP_sha256();
}

/**
 * @brief Derive the TLS 1.2 key block from the session master secret via the PRF.
 *
 * @param ssl          Active SSL session object after handshake completion.
 * @param key_len      Length in bytes of each write key (16 for AES-128, 32 for AES-256).
 * @param fixed_iv_len Length in bytes of each fixed IV component.
 * @param out          Buffer to receive the derived key block.
 * @param out_len      Required size of @p out (must equal 2*(key_len+fixed_iv_len)).
 * @return 0 on success, -1 on any derivation failure.
 */
static int derive_tls12_key_block(
    SSL* ssl,
    size_t key_len,
    size_t fixed_iv_len,
    unsigned char* out,
    size_t out_len)
{
    if (!ssl || !out || out_len == 0) return -1;

    SSL_SESSION* sess = SSL_get_session(ssl);
    if (!sess) return -1;

    unsigned char master[64];
    size_t master_len = SSL_SESSION_get_master_key(sess, master, sizeof(master));
    if (master_len == 0) return -1;

    unsigned char client_random[SSL3_RANDOM_SIZE];
    unsigned char server_random[SSL3_RANDOM_SIZE];
    size_t cr_len = SSL_get_client_random(ssl, client_random, sizeof(client_random));
    size_t sr_len = SSL_get_server_random(ssl, server_random, sizeof(server_random));
    if (cr_len != SSL3_RANDOM_SIZE || sr_len != SSL3_RANDOM_SIZE) return -1;

    unsigned char seed[SSL3_RANDOM_SIZE * 2];
    memcpy(seed, server_random, SSL3_RANDOM_SIZE);
    memcpy(seed + SSL3_RANDOM_SIZE, client_random, SSL3_RANDOM_SIZE);

    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_TLS1_PRF, NULL);
    if (!pctx) return -1;

    int ok = -1;
    const SSL_CIPHER* c = SSL_get_current_cipher(ssl);
    const EVP_MD* prf_md = tls12_prf_md_for_cipher(c);
    size_t produced = out_len;

    if (EVP_PKEY_derive_init(pctx) <= 0) goto done;
    if (EVP_PKEY_CTX_set_tls1_prf_md(pctx, prf_md) <= 0) goto done;
    if (EVP_PKEY_CTX_set1_tls1_prf_secret(pctx, master, (int)master_len) <= 0) goto done;
    if (EVP_PKEY_CTX_add1_tls1_prf_seed(
            pctx, (const unsigned char *)"key expansion", (int)strlen("key expansion")) <= 0) goto done;
    if (EVP_PKEY_CTX_add1_tls1_prf_seed(pctx, seed, (int)sizeof(seed)) <= 0) goto done;
    if (EVP_PKEY_derive(pctx, out, &produced) <= 0) goto done;
    if (produced != out_len) goto done;

    (void)key_len;
    (void)fixed_iv_len;
    ok = 0;
done:
    EVP_PKEY_CTX_free(pctx);
    OPENSSL_cleanse(master, sizeof(master));
    return ok;
}

/**
 * @brief Extract AES-GCM key material for a TLS 1.2 session into kTLS crypto buffers.
 *
 * @param ssl          Active SSL session after the TLS 1.2 handshake.
 * @param is_tx        Non-zero to extract TX (write) keys; zero for RX (read).
 * @param key_len      Key length in bytes (16 for AES-128-GCM, 32 for AES-256-GCM).
 * @param key_out      Output buffer for the cipher key.
 * @param key_out_len  Capacity of @p key_out; must be at least @p key_len.
 * @param salt_out     Output buffer for the fixed IV salt (4 bytes).
 * @param salt_out_len Capacity of @p salt_out; must be at least 4.
 * @param rec_seq_out  Output buffer for the TLS record sequence number (8 bytes).
 * @param rec_seq_len  Capacity of @p rec_seq_out; must be at least 8.
 * @return 0 on success, -1 on failure.
 */
static int fill_tls12_aes_gcm_material(
    SSL* ssl,
    int is_tx,
    size_t key_len,
    unsigned char* key_out,
    size_t key_out_len,
    unsigned char* salt_out,
    size_t salt_out_len,
    unsigned char* rec_seq_out,
    size_t rec_seq_len)
{
    if (!ssl || !key_out || !salt_out || !rec_seq_out) return -1;
    if (key_out_len < key_len || salt_out_len < 4 || rec_seq_len < 8) return -1;

    const size_t fixed_iv_len = 4;
    const size_t block_len = 2 * (key_len + fixed_iv_len);
    unsigned char block[2 * (32 + 4)];
    if (block_len > sizeof(block)) return -1;

    if (derive_tls12_key_block(ssl, key_len, fixed_iv_len, block, block_len) != 0) {
        return -1;
    }

    const unsigned char* client_write_key = block;
    const unsigned char* server_write_key = block + key_len;
    const unsigned char* client_write_iv = block + (2 * key_len);
    const unsigned char* server_write_iv = block + (2 * key_len) + fixed_iv_len;

    int is_server = SSL_is_server(ssl);
    const unsigned char* use_key = NULL;
    const unsigned char* use_iv = NULL;

    if (is_tx) {
        use_key = is_server ? server_write_key : client_write_key;
        use_iv = is_server ? server_write_iv : client_write_iv;
    } else {
        use_key = is_server ? client_write_key : server_write_key;
        use_iv = is_server ? client_write_iv : server_write_iv;
    }

    memcpy(key_out, use_key, key_len);
    memcpy(salt_out, use_iv, fixed_iv_len);
    memset(rec_seq_out, 0, 8);
    OPENSSL_cleanse(block, sizeof(block));
    return 0;
}

/* ===================================================================== */
static int install_aes_gcm_128(int fd, SSL* ssl, int is_tx)
{
    if (!ssl || fd < 0) return -1;
    if (SSL_version(ssl) != TLS1_2_VERSION) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS,
            "ktls: TLS1.2 required for manual AES-128-GCM install");
        return -1;
    }

    struct tls12_crypto_info_aes_gcm_128 crypto_info;
    memset(&crypto_info, 0, sizeof(crypto_info));
    crypto_info.info.version = TLS1_2_VERSION;
    crypto_info.info.cipher_type = TLS_CIPHER_AES_GCM_128;
    memset(crypto_info.iv, 0, sizeof(crypto_info.iv));

    if (fill_tls12_aes_gcm_material(ssl, is_tx, 16,
            crypto_info.key, sizeof(crypto_info.key),
            crypto_info.salt, sizeof(crypto_info.salt),
            crypto_info.rec_seq, sizeof(crypto_info.rec_seq)) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
            "ktls: Failed to derive TLS1.2 AES-128-GCM key material");
        return -1;
    }

    int sol_tls = is_tx ? TLS_TX : TLS_RX;
    if (setsockopt(fd, SOL_TLS, sol_tls, &crypto_info, sizeof(crypto_info)) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
            "ktls: setsockopt AES-128-GCM %s failed: %s",
            is_tx ? "TX" : "RX", strerror(errno));
        OPENSSL_cleanse(&crypto_info, sizeof(crypto_info));
        return -1;
    }

    OPENSSL_cleanse(&crypto_info, sizeof(crypto_info));
    return 0;
}

/**
 * @brief Install AES-256-GCM on socket
 */
static int install_aes_gcm_256(int fd, SSL* ssl, int is_tx)
{
    if (!ssl || fd < 0) return -1;
    if (SSL_version(ssl) != TLS1_2_VERSION) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS,
            "ktls: TLS1.2 required for manual AES-256-GCM install");
        return -1;
    }

    struct tls12_crypto_info_aes_gcm_256 crypto_info;
    memset(&crypto_info, 0, sizeof(crypto_info));
    crypto_info.info.version = TLS1_2_VERSION;
    crypto_info.info.cipher_type = TLS_CIPHER_AES_GCM_256;
    memset(crypto_info.iv, 0, sizeof(crypto_info.iv));

    if (fill_tls12_aes_gcm_material(ssl, is_tx, 32,
            crypto_info.key, sizeof(crypto_info.key),
            crypto_info.salt, sizeof(crypto_info.salt),
            crypto_info.rec_seq, sizeof(crypto_info.rec_seq)) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
            "ktls: Failed to derive TLS1.2 AES-256-GCM key material");
        return -1;
    }

    int sol_tls = is_tx ? TLS_TX : TLS_RX;
    if (setsockopt(fd, SOL_TLS, sol_tls, &crypto_info, sizeof(crypto_info)) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
            "ktls: setsockopt AES-256-GCM %s failed: %s",
            is_tx ? "TX" : "RX", strerror(errno));
        OPENSSL_cleanse(&crypto_info, sizeof(crypto_info));
        return -1;
    }

    OPENSSL_cleanse(&crypto_info, sizeof(crypto_info));
    return 0;
}

/**
 * @brief Install CHACHA20-POLY1305 on socket
 */
static int install_chacha20_poly1305(int fd, SSL* ssl, int is_tx)
{
    (void)fd;
    (void)ssl;
    (void)is_tx;
    KEEL_LOG_WARN(KEEL_LOG_CAT_TLS,
        "ktls: CHACHA20-POLY1305 manual install unsupported by this public-API backend");
    return -1;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Probe whether the running kernel supports kTLS and cache the result.
 *
 * On first call this initializes the kTLS subsystem; subsequent calls return
 * the cached result without acquiring a lock.
 *
 * @return `true` if kTLS is available, `false` otherwise.
 */
bool keel_ktls_detect_support(void)
{
    if (g_ktls_state.initialized) {
        return g_ktls_state.available;
    }

    pthread_mutex_lock(&g_ktls_lock);
    if (g_ktls_state.initialized) {
        pthread_mutex_unlock(&g_ktls_lock);
        return g_ktls_state.available;
    }

    keel_error_t err = keel_ktls_init();
    g_ktls_state.available = KEEL_IS_OK(err);

    pthread_mutex_unlock(&g_ktls_lock);
    return g_ktls_state.available;
}

/**
 * @brief Retrieve the build-time Linux kernel version components.
 *
 * @param major_out Receives the major version number; may be NULL.
 * @param minor_out Receives the minor version number; may be NULL.
 * @param patch_out Receives the patch level; may be NULL.
 * @return `true` if kTLS support was detected, `false` if unavailable.
 */
bool keel_ktls_get_kernel_version(int* major_out, int* minor_out, int* patch_out)
{
    if (!keel_ktls_detect_support()) {
        return false;
    }

    if (major_out) *major_out = g_ktls_state.kernel_major;
    if (minor_out) *minor_out = g_ktls_state.kernel_minor;
    if (patch_out) *patch_out = g_ktls_state.kernel_patch;

    return true;
}

/**
 * @brief Map the negotiated OpenSSL cipher to a kTLS cipher type enum value.
 *
 * @param ssl Active SSL session object after handshake completion.
 * @return The matching `keel_ktls_cipher_type_t` constant, or
 *         `KEEL_KTLS_CIPHER_UNKNOWN` if the cipher is not supported for kTLS offload.
 */
keel_ktls_cipher_type_t keel_ktls_identify_cipher(SSL* ssl)
{
    if (!ssl) return KEEL_KTLS_CIPHER_UNKNOWN;

    int cipher_type = map_cipher_to_ktls_type(ssl, SSL_version(ssl));
    if (cipher_type < 0) return KEEL_KTLS_CIPHER_UNKNOWN;

    /* Map back to enum */
    switch (cipher_type) {
        case TLS_CIPHER_AES_GCM_128: return KEEL_KTLS_CIPHER_AES_128_GCM;
        case TLS_CIPHER_AES_GCM_256: return KEEL_KTLS_CIPHER_AES_256_GCM;
        case TLS_CIPHER_CHACHA20_POLY1305: return KEEL_KTLS_CIPHER_CHACHA20_POLY1305;
        default: return KEEL_KTLS_CIPHER_UNKNOWN;
    }
}

/**
 * @brief Test whether a cipher name string identifies a kTLS-eligible cipher.
 *
 * @param cipher_name Null-terminated OpenSSL cipher name string.
 * @return `true` if the cipher is AES-128-GCM, AES-256-GCM, or CHACHA20-POLY1305;
 *         `false` if @p cipher_name is NULL or does not match any supported cipher.
 */
bool keel_ktls_cipher_supported(const char* cipher_name)
{
    if (!cipher_name) return false;

    /* Common kTLS-compatible ciphers */
    return strstr(cipher_name, "AES_128_GCM") != NULL ||
           strstr(cipher_name, "AES_256_GCM") != NULL ||
           strstr(cipher_name, "CHACHA20_POLY1305") != NULL;
}

/**
 * @brief Install the kTLS TX (send) path on a connected socket.
 *
 * @param fd  Connected TCP socket file descriptor with `TCP_ULP` already set.
 * @param ssl Active SSL session object after handshake completion.
 * @return `KEEL_OK` on success, `KEEL_ERR_NOT_SUPPORTED` if kTLS is unavailable
 *         or the cipher is incompatible, `KEEL_ERR_IO` on setsockopt failure.
 */
keel_error_t keel_ktls_install_tx(int fd, SSL* ssl)
{
    if (fd < 0 || !ssl) return KEEL_ERR_INVALID_ARG;
    if (!g_ktls_state.available) return KEEL_ERR_NOT_SUPPORTED;

    pthread_mutex_lock(&g_ktls_lock);
    g_ktls_state.stats.installations_attempted++;

    int cipher_type = map_cipher_to_ktls_type(ssl, SSL_version(ssl));
    if (cipher_type < 0) {
        g_ktls_state.stats.cipher_incompatible++;
        g_ktls_state.stats.installations_failed++;
        pthread_mutex_unlock(&g_ktls_lock);
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "ktls: Unsupported cipher for TX installation");
        return KEEL_ERR_NOT_SUPPORTED;
    }

    int ret = -1;
    switch (cipher_type) {
        case TLS_CIPHER_AES_GCM_128:
            ret = install_aes_gcm_128(fd, ssl, 1);
            break;
        case TLS_CIPHER_AES_GCM_256:
            ret = install_aes_gcm_256(fd, ssl, 1);
            break;
        case TLS_CIPHER_CHACHA20_POLY1305:
            ret = install_chacha20_poly1305(fd, ssl, 1);
            break;
        default:
            g_ktls_state.stats.cipher_incompatible++;
            g_ktls_state.stats.installations_failed++;
            pthread_mutex_unlock(&g_ktls_lock);
            return KEEL_ERR_NOT_SUPPORTED;
    }

    if (ret == 0) {
        g_ktls_state.stats.installations_succeeded++;
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "ktls: TX installation succeeded (fd=%d, cipher=%d)",
                      fd, cipher_type);
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_OK;
    } else {
        g_ktls_state.stats.installations_failed++;
        g_ktls_state.stats.kernel_errors++;
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "ktls: TX installation failed (fd=%d)", fd);
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_ERR_IO;
    }
}

/**
 * @brief Install the kTLS RX (receive) path on a connected socket.
 *
 * @param fd  Connected TCP socket file descriptor with `TCP_ULP` already set.
 * @param ssl Active SSL session object after handshake completion.
 * @return `KEEL_OK` on success, `KEEL_ERR_NOT_SUPPORTED` if kTLS is unavailable
 *         or the cipher is incompatible, `KEEL_ERR_IO` on setsockopt failure.
 */
keel_error_t keel_ktls_install_rx(int fd, SSL* ssl)
{
    if (fd < 0 || !ssl) return KEEL_ERR_INVALID_ARG;
    if (!g_ktls_state.available) return KEEL_ERR_NOT_SUPPORTED;

    /* Similar to TX */
    pthread_mutex_lock(&g_ktls_lock);
    g_ktls_state.stats.installations_attempted++;

    int cipher_type = map_cipher_to_ktls_type(ssl, SSL_version(ssl));
    if (cipher_type < 0) {
        g_ktls_state.stats.cipher_incompatible++;
        g_ktls_state.stats.installations_failed++;
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_ERR_NOT_SUPPORTED;
    }

    int ret = -1;
    switch (cipher_type) {
        case TLS_CIPHER_AES_GCM_128:
            ret = install_aes_gcm_128(fd, ssl, 0);
            break;
        case TLS_CIPHER_AES_GCM_256:
            ret = install_aes_gcm_256(fd, ssl, 0);
            break;
        case TLS_CIPHER_CHACHA20_POLY1305:
            ret = install_chacha20_poly1305(fd, ssl, 0);
            break;
        default:
            g_ktls_state.stats.cipher_incompatible++;
            g_ktls_state.stats.installations_failed++;
            pthread_mutex_unlock(&g_ktls_lock);
            return KEEL_ERR_NOT_SUPPORTED;
    }

    if (ret == 0) {
        g_ktls_state.stats.installations_succeeded++;
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_OK;
    } else {
        g_ktls_state.stats.installations_failed++;
        g_ktls_state.stats.kernel_errors++;
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_ERR_IO;
    }
}

/**
 * @brief Install kTLS on both TX and RX directions of a connected socket.
 *
 * @param fd  Connected TCP socket file descriptor with `TCP_ULP` already set.
 * @param ssl Active SSL session object after handshake completion.
 * @return `KEEL_OK` only if both directions succeed; the first error encountered
 *         otherwise.
 */
keel_error_t keel_ktls_install_bidirectional(int fd, SSL* ssl)
{
    keel_error_t err_tx = keel_ktls_install_tx(fd, ssl);
    keel_error_t err_rx = keel_ktls_install_rx(fd, ssl);

    if (KEEL_IS_OK(err_tx) && KEEL_IS_OK(err_rx)) {
        return KEEL_OK;
    }

    return (KEEL_IS_ERR(err_tx)) ? err_tx : err_rx;
}

/**
 * @brief Test whether kTLS is active (TX or RX) on a socket.
 *
 * @param fd Socket file descriptor to inspect.
 * @return `true` if at least one direction has kTLS enabled, `false` otherwise.
 */
bool keel_ktls_is_active(int fd)
{
    bool tx = false, rx = false;
    return KEEL_IS_OK(keel_ktls_get_mode(fd, &tx, &rx)) && (tx || rx);
}

/**
 * @brief Query the kernel for the active kTLS directions on a socket.
 *
 * @param fd      Socket file descriptor to inspect.
 * @param tx_out  Set to `true` if kTLS TX is active; may be NULL.
 * @param rx_out  Set to `true` if kTLS RX is active; may be NULL.
 * @return `KEEL_OK` always (individual direction flags encode availability).
 */
keel_error_t keel_ktls_get_mode(int fd, bool* tx_out, bool* rx_out)
{
    if (tx_out) *tx_out = false;
    if (rx_out) *rx_out = false;

    /* Query kernel */
    unsigned char crypto_info[64];
    socklen_t len = sizeof(crypto_info);

    if (getsockopt(fd, SOL_TLS, TLS_TX, crypto_info, &len) == 0) {
        if (tx_out) *tx_out = true;
    }

    len = sizeof(crypto_info);
    if (getsockopt(fd, SOL_TLS, TLS_RX, crypto_info, &len) == 0) {
        if (rx_out) *rx_out = true;
    }

    return KEEL_OK;
}

/**
 * @brief Indicate whether the socket is eligible for zero-copy splice.
 *
 * @param fd Socket file descriptor to test.
 * @return `true` if kTLS is active on the socket and splice is supported.
 */
bool keel_ktls_splice_compatible(int fd)
{
    return keel_ktls_is_active(fd);
}

/**
 * @brief Return the recommended splice chunk size for kTLS-accelerated sockets.
 *
 * @param fd Socket file descriptor (currently unused).
 * @return Optimal chunk size in bytes (one maximum TLS record: 16384 bytes).
 */
size_t keel_ktls_optimal_splice_size(int fd)
{
    /* TLS records are typically 16KB max */
    (void)fd;
    return 16384;
}

/**
 * @brief Return a snapshot of the global kTLS installation statistics.
 *
 * @return A copy of `keel_ktls_stats_t` captured under the global lock.
 */
keel_ktls_stats_t keel_ktls_get_stats(void)
{
    pthread_mutex_lock(&g_ktls_lock);
    keel_ktls_stats_t stats = g_ktls_state.stats;
    pthread_mutex_unlock(&g_ktls_lock);
    return stats;
}

/**
 * @brief Reset all kTLS installation counters to zero.
 */
void keel_ktls_reset_stats(void)
{
    pthread_mutex_lock(&g_ktls_lock);
    memset(&g_ktls_state.stats, 0, sizeof(g_ktls_state.stats));
    pthread_mutex_unlock(&g_ktls_lock);
}

/**
 * @brief Initialize the kTLS subsystem and probe kernel support.
 *
 * Safe to call multiple times; idempotent after first successful initialization.
 *
 * @return `KEEL_OK` if kTLS is available, `KEEL_ERR_NOT_SUPPORTED` if the
 *         kernel does not support kTLS, or `KEEL_ERR_IO` on socket probe failure.
 */
keel_error_t keel_ktls_init(void)
{
    pthread_mutex_lock(&g_ktls_lock);

    if (g_ktls_state.initialized) {
        pthread_mutex_unlock(&g_ktls_lock);
        return g_ktls_state.available ? KEEL_OK : KEEL_ERR_NOT_SUPPORTED;
    }

    /* Detect kernel version */
    if (!detect_kernel_version()) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "ktls: Kernel version %d.%d.%d does not support TLS",
                      g_ktls_state.kernel_major,
                      g_ktls_state.kernel_minor,
                      g_ktls_state.kernel_patch);
        g_ktls_state.initialized = true;
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Test SOL_TLS availability */
    int test_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (test_fd < 0) {
        g_ktls_state.initialized = true;
        pthread_mutex_unlock(&g_ktls_lock);
        return KEEL_ERR_IO;
    }

    /* Try to query TLS support (will fail but tells us if SOL_TLS exists) */
    unsigned char dummy[64];
    socklen_t dummy_len = sizeof(dummy);
    int ret = getsockopt(test_fd, SOL_TLS, TLS_TX, dummy, &dummy_len);
    close(test_fd);

    if (ret == 0 || errno == EBADF) {
        /* SOL_TLS exists */
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "ktls: Kernel TLS support detected (kernel %d.%d.%d)",
                      g_ktls_state.kernel_major,
                      g_ktls_state.kernel_minor,
                      g_ktls_state.kernel_patch);
        g_ktls_state.available = true;
    } else {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "ktls: Kernel TLS not available: %s", strerror(errno));
        g_ktls_state.available = false;
    }

    g_ktls_state.initialized = true;
    pthread_mutex_unlock(&g_ktls_lock);

    return g_ktls_state.available ? KEEL_OK : KEEL_ERR_NOT_SUPPORTED;
}

/**
 * @brief Shut down the kTLS subsystem and clear all cached state.
 */
void keel_ktls_cleanup(void)
{
    pthread_mutex_lock(&g_ktls_lock);
    g_ktls_state.initialized = false;
    g_ktls_state.available = false;
    memset(&g_ktls_state.stats, 0, sizeof(g_ktls_state.stats));
    pthread_mutex_unlock(&g_ktls_lock);
}
