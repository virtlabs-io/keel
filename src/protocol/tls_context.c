/**
 * @file tls_context.c
 * @brief Userspace TLS session management with optional kTLS activation.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The TLS layer sits between the protocol engine and the transport sockets. It uses
 * OpenSSL memory BIOs so handshakes can be driven incrementally by the event loop
 * without blocking on socket I/O, and it preserves enough post-handshake metadata to
 * attempt a handoff into Linux kTLS. That two-stage design keeps the common case
 * portable while still enabling zero-copy acceleration where the platform supports it.
 */

#include "keel/protocol/tls_context.h"
#include "keel/protocol/ktls.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <string.h>
#include <pthread.h>

/* ============================================================================
 * Global TLS State
 * ============================================================================ */

static struct {
    bool                initialized;
    SSL_CTX*            server_ctx;      /* Server-side SSL context */
    SSL_CTX*            client_ctx;      /* Client-side SSL context */
    keel_tls_stats_t    stats;
    pthread_mutex_t     lock;
} g_tls_global = {
    .initialized = false,
    .server_ctx = NULL,
    .client_ctx = NULL,
    .stats = {0},
    .lock = PTHREAD_MUTEX_INITIALIZER,
};

/* ============================================================================
 * TLS Context Structure
 * ============================================================================ */

/**
 * @brief Internal TLS context structure
 */
struct keel_tls_context {
    bool                is_server;              /* true=server, false=client */
    SSL*                ssl;                    /* OpenSSL SSL object */
    BIO*                bio_read;               /* Memory BIO for reads */
    BIO*                bio_write;              /* Memory BIO for writes */
    keel_tls_state_t    state;                  /* Current state */
    keel_tls_config_t   config;                 /* TLS configuration */
    
    /* Handshake tracking */
    int                 handshake_ret;         /* Last SSL_do_handshake() result */
    int                 ssl_error;             /* Last SSL error code */
    
    /* Session info (after handshake) */
    uint16_t            negotiated_version;    /* TLS version (e.g., 0x0304) */
    char                cipher_name[64];       /* Negotiated cipher */
    bool                ktls_active;           /* kTLS successfully installed */
    
    /* Peer certificate info */
    X509*               peer_cert;             /* Peer certificate (if any) */
    
    /* Statistics */
    uint64_t            bytes_encrypted;       /* Bytes encrypted */
    uint64_t            bytes_decrypted;       /* Bytes decrypted */
};

/* ============================================================================
 * OpenSSL Context Setup
 * ============================================================================ */

/**
 * @brief Create and configure an OpenSSL `SSL_CTX` for server or client use.
 *
 * @param is_server Selects server- versus client-side defaults.
 * @param config Effective TLS policy.
 * @return Configured OpenSSL context, or `NULL` on failure.
 */
static SSL_CTX* create_ssl_context(bool is_server, const keel_tls_config_t* config)
{
    if (!config) {
        return NULL;
    }

    /* Choose method based on TLS version constraints */
    const SSL_METHOD* method = is_server ? TLS_server_method() : TLS_client_method();
    if (!method) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to get SSL method");
        return NULL;
    }

    SSL_CTX* ctx = SSL_CTX_new(method);
    if (!ctx) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to create SSL_CTX");
        return NULL;
    }

    /* Set minimum/maximum TLS versions */
    long options = 0;
    
    if (config->min_version != KEEL_TLS_VERSION_AUTO) {
        if (SSL_CTX_set_min_proto_version(ctx, config->min_version) == 0) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Failed to set min TLS version: %d", 
                         config->min_version);
        }
    }

    if (config->max_version != KEEL_TLS_VERSION_AUTO && config->max_version != 0) {
        if (SSL_CTX_set_max_proto_version(ctx, config->max_version) == 0) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Failed to set max TLS version: %d",
                         config->max_version);
        }
    }

    /* Disable old protocols */
    options |= SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
    
    /* Enable better ciphers */
    options |= SSL_OP_CIPHER_SERVER_PREFERENCE;
    
    SSL_CTX_set_options(ctx, options);

    /* Apply cipher suite policy (TLS 1.2) */
    if (config->ciphers && config->ciphers[0]) {
        if (SSL_CTX_set_cipher_list(ctx, config->ciphers) == 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Invalid cipher list: %s", config->ciphers);
            SSL_CTX_free(ctx);
            return NULL;
        }
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Cipher list set: %s", config->ciphers);
    }

    /* Apply ciphersuite policy (TLS 1.3) */
    if (config->ciphersuites && config->ciphersuites[0]) {
        if (SSL_CTX_set_ciphersuites(ctx, config->ciphersuites) == 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Invalid TLS 1.3 ciphersuites: %s", config->ciphersuites);
            SSL_CTX_free(ctx);
            return NULL;
        }
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: TLS 1.3 ciphersuites set: %s", config->ciphersuites);
    }

    /* Load certificate and key for server-side */
    if (is_server) {
        if (!config->cert_file || !config->key_file) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Server requires cert_file and key_file");
            SSL_CTX_free(ctx);
            return NULL;
        }

        if (SSL_CTX_use_certificate_file(ctx, config->cert_file, SSL_FILETYPE_PEM) <= 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to load certificate from %s: %s",
                          config->cert_file, ERR_reason_error_string(ERR_get_error()));
            SSL_CTX_free(ctx);
            return NULL;
        }

        if (SSL_CTX_use_PrivateKey_file(ctx, config->key_file, SSL_FILETYPE_PEM) <= 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to load private key from %s: %s",
                          config->key_file, ERR_reason_error_string(ERR_get_error()));
            SSL_CTX_free(ctx);
            return NULL;
        }

        if (!SSL_CTX_check_private_key(ctx)) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Private key does not match certificate");
            SSL_CTX_free(ctx);
            return NULL;
        }
    }

    /* Set peer verification for client-side */
    if (!is_server) {
        if (config->verify_peer == KEEL_TLS_VERIFY_REQUIRED) {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                              NULL);
        } else if (config->verify_peer == KEEL_TLS_VERIFY_OPTIONAL) {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
        } else {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
        }

        if (config->ca_file) {
            if (SSL_CTX_load_verify_locations(ctx, config->ca_file, NULL) <= 0) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Failed to load CA file: %s", config->ca_file);
            }
        }

        /* Load client certificate + key for mTLS (client presenting cert to server) */
        if (config->cert_file && config->key_file) {
            if (SSL_CTX_use_certificate_file(ctx, config->cert_file, SSL_FILETYPE_PEM) <= 0) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Failed to load client certificate from %s: %s",
                             config->cert_file, ERR_reason_error_string(ERR_get_error()));
            } else if (SSL_CTX_use_PrivateKey_file(ctx, config->key_file, SSL_FILETYPE_PEM) <= 0) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Failed to load client key from %s: %s",
                             config->key_file, ERR_reason_error_string(ERR_get_error()));
            }
        }
    }

    /* Set peer verification for server-side (client certificates) */
    if (is_server) {
        if (config->verify_peer == KEEL_TLS_VERIFY_REQUIRED) {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                              NULL);
        } else if (config->verify_peer == KEEL_TLS_VERIFY_OPTIONAL) {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
        } else {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
        }

        if (config->ca_file) {
            if (SSL_CTX_load_verify_locations(ctx, config->ca_file, NULL) <= 0) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Failed to load CA file: %s", config->ca_file);
            }
        }
    }

    return ctx;
}

/**
 * @brief Copy config structure
 */
static void copy_tls_config(keel_tls_config_t* dst, const keel_tls_config_t* src)
{
    if (!dst || !src) return;

    memcpy(dst, src, sizeof(keel_tls_config_t));

    /* Deep copy strings */
    if (src->cert_file) {
        dst->cert_file = keel_strdup(src->cert_file);
    }
    if (src->key_file) {
        dst->key_file = keel_strdup(src->key_file);
    }
    if (src->ca_file) {
        dst->ca_file = keel_strdup(src->ca_file);
    }
    if (src->ciphers) {
        dst->ciphers = keel_strdup(src->ciphers);
    }
    if (src->ciphersuites) {
        dst->ciphersuites = keel_strdup(src->ciphersuites);
    }
}

/**
 * @brief Free config strings
 */
static void free_tls_config(keel_tls_config_t* config)
{
    if (!config) return;
    keel_free(config->cert_file);
    keel_free(config->key_file);
    keel_free(config->ca_file);
    keel_free(config->ciphers);
    keel_free(config->ciphersuites);
    memset(config, 0, sizeof(*config));
}

/* ===================================================================== */
keel_error_t keel_tls_init(void)
{
    pthread_mutex_lock(&g_tls_global.lock);

    if (g_tls_global.initialized) {
        pthread_mutex_unlock(&g_tls_global.lock);
        return KEEL_OK;
    }

    /* Initialize OpenSSL */
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();

    /* Initialize kTLS subsystem */
    keel_ktls_init();

    KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Subsystem initialized");
    g_tls_global.initialized = true;

    pthread_mutex_unlock(&g_tls_global.lock);
    return KEEL_OK;
}

/**
 * @brief Tear down the global TLS subsystem and free all SSL contexts.
 */
void keel_tls_cleanup(void)
{
    pthread_mutex_lock(&g_tls_global.lock);

    if (!g_tls_global.initialized) {
        pthread_mutex_unlock(&g_tls_global.lock);
        return;
    }

    if (g_tls_global.server_ctx) {
        SSL_CTX_free(g_tls_global.server_ctx);
        g_tls_global.server_ctx = NULL;
    }

    if (g_tls_global.client_ctx) {
        SSL_CTX_free(g_tls_global.client_ctx);
        g_tls_global.client_ctx = NULL;
    }

    keel_ktls_cleanup();
    
    EVP_cleanup();
    ERR_free_strings();

    g_tls_global.initialized = false;

    pthread_mutex_unlock(&g_tls_global.lock);
    KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Subsystem cleaned up");
}

/**
 * @brief Return whether kernel TLS is available on this platform.
 *
 * @return `true` if kTLS is detected and operational, `false` otherwise.
 */
bool keel_ktls_available(void)
{
    return keel_ktls_detect_support();
}

/**
 * @brief Test whether a cipher name is compatible with kernel TLS offload.
 *
 * @param cipher_name Null-terminated OpenSSL cipher name string.
 * @return `true` if the cipher can be offloaded to kTLS, `false` otherwise.
 */
bool keel_ktls_cipher_compatible(const char* cipher_name)
{
    return keel_ktls_cipher_supported(cipher_name);
}

/* ===================================================================== */
keel_error_t keel_tls_context_create(
    const keel_tls_config_t* config,
    bool is_server,
    keel_tls_context_t** ctx_out)
{
    if (!config || !ctx_out) {
        return KEEL_ERR_INVALID_ARG;
    }

    keel_error_t err = keel_tls_init();
    if (KEEL_IS_ERR(err)) {
        return err;
    }

    /* Get or create SSL_CTX */
    SSL_CTX* ssl_ctx = NULL;
    pthread_mutex_lock(&g_tls_global.lock);

    if (is_server) {
        if (!g_tls_global.server_ctx) {
            g_tls_global.server_ctx = create_ssl_context(true, config);
            if (!g_tls_global.server_ctx) {
                pthread_mutex_unlock(&g_tls_global.lock);
                return KEEL_ERR_TLS;
            }
        }
        ssl_ctx = g_tls_global.server_ctx;
    } else {
        if (!g_tls_global.client_ctx) {
            g_tls_global.client_ctx = create_ssl_context(false, config);
            if (!g_tls_global.client_ctx) {
                pthread_mutex_unlock(&g_tls_global.lock);
                return KEEL_ERR_TLS;
            }
        }
        ssl_ctx = g_tls_global.client_ctx;
    }

    pthread_mutex_unlock(&g_tls_global.lock);

    /* Create context structure */
    keel_tls_context_t* ctx = keel_calloc(1, sizeof(keel_tls_context_t));
    if (!ctx) {
        return KEEL_ERR_NOMEM;
    }

    ctx->is_server = is_server;
    ctx->state = KEEL_TLS_STATE_INIT;
    copy_tls_config(&ctx->config, config);

    /* Create SSL object */
    ctx->ssl = SSL_new(ssl_ctx);
    if (!ctx->ssl) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to create SSL object: %s",
                      ERR_reason_error_string(ERR_get_error()));
        free_tls_config(&ctx->config);
        keel_free(ctx);
        return KEEL_ERR_TLS;
    }

    /* Create memory BIOs */
    ctx->bio_read = BIO_new(BIO_s_mem());
    ctx->bio_write = BIO_new(BIO_s_mem());
    if (!ctx->bio_read || !ctx->bio_write) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to create memory BIOs");
        if (ctx->bio_read) BIO_free(ctx->bio_read);
        if (ctx->bio_write) BIO_free(ctx->bio_write);
        SSL_free(ctx->ssl);
        free_tls_config(&ctx->config);
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }

    /* Set non-blocking mode on BIOs */
    BIO_set_nbio(ctx->bio_read, 1);
    BIO_set_nbio(ctx->bio_write, 1);

    /* Connect BIOs to SSL */
    SSL_set_bio(ctx->ssl, ctx->bio_read, ctx->bio_write);

    /* Set server/client mode */
    if (is_server) {
        SSL_set_accept_state(ctx->ssl);
    } else {
        SSL_set_connect_state(ctx->ssl);
    }

    ctx->state = KEEL_TLS_STATE_HANDSHAKE;

    pthread_mutex_lock(&g_tls_global.lock);
    g_tls_global.stats.connections_total++;
    pthread_mutex_unlock(&g_tls_global.lock);

    *ctx_out = ctx;
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "tls: Context created (server=%d)", is_server);

    return KEEL_OK;
}

/**
 * @brief Release all resources associated with a TLS context.
 *
 * @param ctx TLS context to destroy; no-op if NULL.
 */
void keel_tls_context_destroy(keel_tls_context_t* ctx)
{
    if (!ctx) {
        return;
    }

    if (ctx->ssl) {
        SSL_free(ctx->ssl);  /* Also frees BIOs */
        ctx->ssl = NULL;
    }

    if (ctx->peer_cert) {
        X509_free(ctx->peer_cert);
        ctx->peer_cert = NULL;
    }

    free_tls_config(&ctx->config);
    keel_free(ctx);
}

/* ============================================================================
 * Public API: State Query
 * ============================================================================ */

/**
 * @brief Return the current state of a TLS context.
 *
 * @param ctx TLS context to query.
 * @return Current `keel_tls_state_t` value, or `KEEL_TLS_STATE_ERROR` if @p ctx is NULL.
 */
keel_tls_state_t keel_tls_context_state(const keel_tls_context_t* ctx)
{
    if (!ctx) {
        return KEEL_TLS_STATE_ERROR;
    }
    return ctx->state;
}

/**
 * @brief Return the negotiated TLS protocol version.
 *
 * @param ctx TLS context to query (must be in ESTABLISHED or later state).
 * @return Negotiated version (e.g., 0x0303 for TLS 1.2, 0x0304 for TLS 1.3),
 *         or 0 if @p ctx is NULL or the handshake has not yet completed.
 */
uint16_t keel_tls_context_version(const keel_tls_context_t* ctx)
{
    if (!ctx) {
        return 0;
    }
    return ctx->negotiated_version;
}

/**
 * @brief Return the negotiated cipher suite name as a string.
 *
 * @param ctx TLS context to query.
 * @return Null-terminated cipher name, or an empty string if @p ctx is NULL
 *         or the handshake has not yet completed.
 */
const char* keel_tls_context_cipher(const keel_tls_context_t* ctx)
{
    if (!ctx) {
        return "";
    }
    return (ctx->cipher_name[0] != '\0') ? ctx->cipher_name : "";
}

/**
 * @brief Test whether kernel TLS is currently active for this context.
 *
 * @param ctx TLS context to query.
 * @return `true` if kTLS was successfully installed, `false` otherwise or if @p ctx is NULL.
 */
bool keel_tls_context_ktls_active(const keel_tls_context_t* ctx)
{
    if (!ctx) {
        return false;
    }
    return ctx->ktls_active;
}

/**
 * @brief Retrieve peer certificate information from an established TLS session.
 *
 * @param ctx  TLS context (must be in ESTABLISHED or later state).
 * @param info Output structure populated with subject, issuer, and certificate pointer.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` if either argument is NULL,
 *         or `KEEL_ERR_INVALID_STATE` if the handshake has not completed.
 */
keel_error_t keel_tls_context_peer_info(
    const keel_tls_context_t* ctx,
    keel_tls_peer_info_t* info)
{
    if (!ctx || !info) {
        return KEEL_ERR_INVALID_ARG;
    }

    memset(info, 0, sizeof(*info));

    if (ctx->state < KEEL_TLS_STATE_ESTABLISHED) {
        return KEEL_ERR_INVALID_STATE;
    }

    X509* cert = SSL_get_peer_certificate(ctx->ssl);
    if (!cert) {
        info->has_cert = false;
        return KEEL_OK;
    }

    info->has_cert = true;
    info->cert = cert;

    /* Extract subject and issuer DNs */
    X509_NAME* subject = X509_get_subject_name(cert);
    X509_NAME* issuer = X509_get_issuer_name(cert);

    if (subject) {
        X509_NAME_oneline(subject, info->subject, sizeof(info->subject) - 1);
    }
    if (issuer) {
        X509_NAME_oneline(issuer, info->issuer, sizeof(info->issuer) - 1);
    }

    X509_free(cert);
    info->cert = NULL;

    return KEEL_OK;
}

/* ===================================================================== */
keel_tls_hs_result_t keel_tls_handshake_step(
    keel_tls_context_t* ctx,
    char* return_info)
{
    if (!ctx) {
        return KEEL_TLS_HS_ERROR;
    }

    if (ctx->state > KEEL_TLS_STATE_HANDSHAKE) {
        /* Already complete */
        return KEEL_TLS_HS_COMPLETE;
    }

    /* Try to progress handshake */
    int ret = SSL_do_handshake(ctx->ssl);

    if (ret == 1) {
        /* Handshake complete! */
        ctx->state = KEEL_TLS_STATE_ESTABLISHED;

        /* Capture negotiated parameters */
        ctx->negotiated_version = (uint16_t)SSL_version(ctx->ssl);
        const SSL_CIPHER* cipher = SSL_get_current_cipher(ctx->ssl);
        if (cipher) {
            strncpy(ctx->cipher_name, SSL_CIPHER_get_name(cipher),
                   sizeof(ctx->cipher_name) - 1);
        }

        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Handshake complete (version=0x%04x, cipher=%s)",
                     ctx->negotiated_version, ctx->cipher_name);

        pthread_mutex_lock(&g_tls_global.lock);
        g_tls_global.stats.connections_succeeded++;
        pthread_mutex_unlock(&g_tls_global.lock);

        /* Attempt kTLS activation if enabled and supported */
        if (ctx->config.ktls_enabled && keel_ktls_available()) {
            /* Note: kTLS requires socket FD, which is obtained at I/O time.
             * This attempt will be deferred to the worker when socket is ready.
             * See: frontend_tls_async.c / backend_tls_async.c for actual installation.
             */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "tls: kTLS activation deferred to worker thread");
        }

        return KEEL_TLS_HS_COMPLETE;
    }

    /* Check for errors */
    int ssl_err = SSL_get_error(ctx->ssl, ret);

    switch (ssl_err) {
        case SSL_ERROR_WANT_READ:
            ctx->ssl_error = ssl_err;
            return KEEL_TLS_HS_WANT_READ;

        case SSL_ERROR_WANT_WRITE:
            ctx->ssl_error = ssl_err;
            return KEEL_TLS_HS_WANT_WRITE;

        case SSL_ERROR_SSL:
        case SSL_ERROR_SYSCALL:
        default:
            ctx->state = KEEL_TLS_STATE_ERROR;
            if (return_info) {
                snprintf(return_info, 256, "SSL error: %s",
                        ERR_reason_error_string(ERR_get_error()));
            }
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Handshake failed: %s",
                          ERR_reason_error_string(ERR_get_error()));

            pthread_mutex_lock(&g_tls_global.lock);
            g_tls_global.stats.connections_failed++;
            pthread_mutex_unlock(&g_tls_global.lock);

            return KEEL_TLS_HS_ERROR;
    }
}

/**
 * @brief Drain outbound handshake bytes that OpenSSL has prepared to send.
 *
 * The caller must transmit these bytes to the peer before calling
 * `keel_tls_handshake_step()` again.
 *
 * @param ctx      TLS context in HANDSHAKE state.
 * @param buf      Buffer to receive outbound handshake bytes.
 * @param buf_size Capacity of @p buf in bytes.
 * @return Number of bytes written to @p buf, 0 if none pending, or -1 on error.
 */
ssize_t keel_tls_get_handshake_data(
    keel_tls_context_t* ctx,
    uint8_t* buf,
    size_t buf_size)
{
    if (!ctx || !buf || buf_size == 0) {
        return -1;
    }

    /* Read from write BIO (data to send to peer) */
    int available = BIO_ctrl_pending(ctx->bio_write);
    if (available <= 0) {
        return 0;
    }

    size_t to_read = (size_t)available;
    if (to_read > buf_size) {
        to_read = buf_size;
    }

    int n = BIO_read(ctx->bio_write, buf, (int)to_read);
    if (n <= 0) {
        return 0;
    }

    return (ssize_t)n;
}

/**
 * @brief Deliver inbound handshake bytes received from the peer into the BIO.
 *
 * Call this before each invocation of `keel_tls_handshake_step()` whenever
 * new data arrives from the peer.
 *
 * @param ctx  TLS context in HANDSHAKE state.
 * @param data Pointer to inbound data received from the peer.
 * @param len  Number of bytes in @p data.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` if any argument is NULL/zero,
 *         `KEEL_ERR_INVALID_STATE` if the handshake is already complete,
 *         or `KEEL_ERR_IO` on BIO write failure.
 */
keel_error_t keel_tls_feed_handshake_data(
    keel_tls_context_t* ctx,
    const uint8_t* data,
    size_t len)
{
    if (!ctx || !data || len == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    if (ctx->state > KEEL_TLS_STATE_HANDSHAKE) {
        return KEEL_ERR_INVALID_STATE;
    }

    /* Write to read BIO (data from peer) */
    int n = BIO_write(ctx->bio_read, data, (int)len);
    if (n < 0 || (size_t)n != len) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to feed handshake data to BIO");
        return KEEL_ERR_IO;
    }

    return KEEL_OK;
}

/* ===================================================================== */
ssize_t keel_tls_read_decrypted(
    keel_tls_context_t* ctx,
    uint8_t* buf,
    size_t buf_size)
{
    if (!ctx || !buf || buf_size == 0) {
        return -1;
    }

    if (ctx->state < KEEL_TLS_STATE_ESTABLISHED) {
        return -1;
    }

    int n = SSL_read(ctx->ssl, buf, (int)buf_size);
    if (n > 0) {
        ctx->bytes_decrypted += (uint64_t)n;
        return (ssize_t)n;
    }

    int ssl_err = SSL_get_error(ctx->ssl, n);
    if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE) {
        return 0;  /* No data available */
    }

    if (ssl_err == SSL_ERROR_ZERO_RETURN) {
        return 0;  /* Connection closed */
    }

    return -1;
}

/**
 * @brief Encrypt plaintext application data and stage it for sending.
 *
 * After this call, retrieve the ciphertext via `keel_tls_get_encrypted_to_send()`.
 *
 * @param ctx  Established TLS context.
 * @param data Plaintext data to encrypt.
 * @param len  Length of @p data in bytes.
 * @return Positive byte count on success, 0 if the operation would block,
 *         or -1 on error.
 */
ssize_t keel_tls_write_plaintext(
    keel_tls_context_t* ctx,
    const uint8_t* data,
    size_t len)
{
    if (!ctx || !data || len == 0) {
        return -1;
    }

    if (ctx->state < KEEL_TLS_STATE_ESTABLISHED) {
        return -1;
    }

    int n = SSL_write(ctx->ssl, data, (int)len);
    if (n > 0) {
        ctx->bytes_encrypted += (uint64_t)n;
        return (ssize_t)n;
    }

    int ssl_err = SSL_get_error(ctx->ssl, n);
    if (ssl_err == SSL_ERROR_WANT_WRITE || ssl_err == SSL_ERROR_WANT_READ) {
        return 0;  /* Would block */
    }

    return -1;
}

/**
 * @brief Drain encrypted bytes staged by OpenSSL and ready to send to the peer.
 *
 * @param ctx      Established TLS context.
 * @param buf      Buffer to receive ciphertext.
 * @param buf_size Capacity of @p buf in bytes.
 * @return Number of bytes written to @p buf, 0 if none pending, or -1 on error.
 */
ssize_t keel_tls_get_encrypted_to_send(
    keel_tls_context_t* ctx,
    uint8_t* buf,
    size_t buf_size)
{
    if (!ctx || !buf || buf_size == 0) {
        return -1;
    }

    /* Read from write BIO (encrypted data to send) */
    int available = BIO_ctrl_pending(ctx->bio_write);
    if (available <= 0) {
        return 0;
    }

    size_t to_read = (size_t)available;
    if (to_read > buf_size) {
        to_read = buf_size;
    }

    int n = BIO_read(ctx->bio_write, buf, (int)to_read);
    if (n <= 0) {
        return 0;
    }

    return (ssize_t)n;
}

/**
 * @brief Deliver inbound encrypted bytes received from the peer into the receive BIO.
 *
 * @param ctx  Established TLS context.
 * @param data Pointer to inbound encrypted data from the peer.
 * @param len  Number of bytes in @p data.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` if any argument is NULL/zero,
 *         `KEEL_ERR_INVALID_STATE` if the context is not established,
 *         or `KEEL_ERR_IO` on BIO write failure.
 */
keel_error_t keel_tls_feed_encrypted(
    keel_tls_context_t* ctx,
    const uint8_t* data,
    size_t len)
{
    if (!ctx || !data || len == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    if (ctx->state < KEEL_TLS_STATE_ESTABLISHED) {
        return KEEL_ERR_INVALID_STATE;
    }

    /* Write to read BIO (encrypted data from peer) */
    int n = BIO_write(ctx->bio_read, data, (int)len);
    if (n < 0 || (size_t)n != len) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to feed encrypted data to BIO");
        return KEEL_ERR_IO;
    }

    return KEEL_OK;
}

/* ============================================================================
 * Public API: kTLS Activation (Called by worker threads after handshake)
 * ============================================================================ */

/**
 * @brief Activate Kernel TLS on a socket (internal helper)
 *
 * Called by worker threads after TLS handshake completes and socket FD is available.
 * Attempts to install kTLS via setsockopt. If successful, subsequent data
 * transfers will use kernel-managed encryption/decryption.
 *
 * @param ctx TLS context (must be in ESTABLISHED state)
 * @param fd Socket file descriptor
 * @return KEEL_OK if kTLS installed, KEEL_ERR_* if failed or unavailable
 */
keel_error_t keel_tls_context_activate_ktls(
    keel_tls_context_t* ctx,
    int fd)
{
    if (!ctx || fd < 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    if (ctx->state != KEEL_TLS_STATE_ESTABLISHED) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: Cannot activate kTLS in state %d", ctx->state);
        return KEEL_ERR_INVALID_STATE;
    }

    if (!ctx->config.ktls_enabled || !keel_ktls_available()) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "tls: kTLS not enabled or available");
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Check cipher compatibility */
    keel_ktls_cipher_type_t cipher = keel_ktls_identify_cipher(ctx->ssl);
    if (cipher == KEEL_KTLS_CIPHER_UNKNOWN) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Cipher '%s' not supported by kTLS, using userspace TLS",
                     ctx->cipher_name);
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Attempt bidirectional kTLS installation */
    keel_error_t err = keel_ktls_install_bidirectional(fd, ctx->ssl);
    if (KEEL_IS_OK(err)) {
        ctx->state = KEEL_TLS_STATE_KTLS_ACTIVE;
        ctx->ktls_active = true;
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: kTLS activated on fd=%d (cipher=%s)", fd, ctx->cipher_name);

        pthread_mutex_lock(&g_tls_global.lock);
        g_tls_global.stats.ktls_active++;
        pthread_mutex_unlock(&g_tls_global.lock);

        return KEEL_OK;
    } else {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "tls: kTLS installation failed, falling back to userspace TLS");

        pthread_mutex_lock(&g_tls_global.lock);
        g_tls_global.stats.ktls_fallback++;
        pthread_mutex_unlock(&g_tls_global.lock);

        return err;
    }
}

/* ===================================================================== */
keel_tls_stats_t keel_tls_get_stats(void)
{
    pthread_mutex_lock(&g_tls_global.lock);
    keel_tls_stats_t stats = g_tls_global.stats;
    pthread_mutex_unlock(&g_tls_global.lock);
    return stats;
}

/**
 * @brief Reset all global TLS session counters to zero.
 */
void keel_tls_reset_stats(void)
{
    pthread_mutex_lock(&g_tls_global.lock);
    memset(&g_tls_global.stats, 0, sizeof(g_tls_global.stats));
    pthread_mutex_unlock(&g_tls_global.lock);
}

/* ===================================================================== */
keel_error_t keel_tls_reload_certs(
    const keel_tls_config_t* server_config,
    const keel_tls_config_t* client_config)
{
    pthread_mutex_lock(&g_tls_global.lock);

    if (!g_tls_global.initialized) {
        pthread_mutex_unlock(&g_tls_global.lock);
        return KEEL_ERR_INVALID_STATE;
    }

    bool any_failed = false;

    /* Reload server context */
    if (server_config && server_config->mode != KEEL_TLS_DISABLE) {
        SSL_CTX* new_ctx = create_ssl_context(true, server_config);
        if (new_ctx) {
            SSL_CTX* old_ctx = g_tls_global.server_ctx;
            g_tls_global.server_ctx = new_ctx;
            /* Old context freed after lock release; existing SSL objects
             * hold a reference via SSL_new() so they remain valid. */
            if (old_ctx) SSL_CTX_free(old_ctx);
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Server certificates reloaded");
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to reload server certificates");
            any_failed = true;
        }
    }

    /* Reload client context */
    if (client_config && client_config->mode != KEEL_TLS_DISABLE) {
        SSL_CTX* new_ctx = create_ssl_context(false, client_config);
        if (new_ctx) {
            SSL_CTX* old_ctx = g_tls_global.client_ctx;
            g_tls_global.client_ctx = new_ctx;
            if (old_ctx) SSL_CTX_free(old_ctx);
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "tls: Client certificates reloaded");
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "tls: Failed to reload client certificates");
            any_failed = true;
        }
    }

    if (any_failed) {
        g_tls_global.stats.cert_reload_failures++;
    } else {
        g_tls_global.stats.cert_reloads++;
    }

    pthread_mutex_unlock(&g_tls_global.lock);
    return any_failed ? KEEL_ERR_TLS : KEEL_OK;
}

/**
 * @brief Increment the downgrade-rejection counter in the global TLS statistics.
 *
 * Call this whenever a TLS version downgrade attempt is detected and blocked.
 */
void keel_tls_stat_downgrade_rejected(void)
{
    pthread_mutex_lock(&g_tls_global.lock);
    g_tls_global.stats.downgrade_rejected++;
    pthread_mutex_unlock(&g_tls_global.lock);
}
