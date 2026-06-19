/**
 * @file auth.c
 * @brief Authentication manager implementation and PostgreSQL-compatible auth helpers.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file implements the frontend authentication side of KEEL. It provides
 * naming helpers, user/provider bookkeeping, PostgreSQL MD5 compatibility, and
 * SCRAM-SHA-256 primitives used by the higher-level protocol/auth flow.
 *
 * Scope notes:
 * - frontend auth is intentionally distinct from backend server authentication
 * - OpenSSL-backed helpers are compiled conditionally
 * - the user database is currently a simple linked list rather than a hash map
 */

#include "keel/core/auth.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel_error.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <sys/eventfd.h>
#include <stdatomic.h>
#include <time.h>
#include <sys/time.h>     /* struct timeval — needed explicitly on musl libc */
#include <unistd.h>

#ifdef KEEL_HAS_OPENSSL
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>   /* CRYPTO_memcmp — constant-time comparison */
#endif

/* Note: ldap_ctx_t is defined in the LDAP section below.
 * keel_auth_get_verify_fd() implementation lives there too. */

/* ============================================================================
 * MD5 Password Helper (PostgreSQL compatible)
 * ============================================================================ */

#ifdef KEEL_HAS_OPENSSL
/**
 * @brief Compute PostgreSQL MD5 password hash: "md5" + md5(md5(password + user) + salt)
 * 
 * @param user     Username
 * @param password Plaintext password
 * @param salt     4-byte salt from server
 * @return Allocated string "md5<32hex>" or NULL on failure. Caller must free.
 */
static char* keel_md5_password(const char* user, const char* password, const uint8_t salt[4])
{
    static const char hex[] = "0123456789abcdef";
    unsigned char inner[16], outer[16];
    
    /* Step 1: md5(password + user) */
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (!mdctx) return NULL;
    
    if (EVP_DigestInit_ex(mdctx, EVP_md5(), NULL) != 1 ||
        EVP_DigestUpdate(mdctx, password, strlen(password)) != 1 ||
        EVP_DigestUpdate(mdctx, user, strlen(user)) != 1 ||
        EVP_DigestFinal_ex(mdctx, inner, NULL) != 1) {
        EVP_MD_CTX_free(mdctx);
        return NULL;
    }
    
    /* Convert inner hash to hex string */
    char inner_hex[33];
    for (int i = 0; i < 16; i++) {
        inner_hex[i * 2]     = hex[inner[i] >> 4];
        inner_hex[i * 2 + 1] = hex[inner[i] & 0x0f];
    }
    inner_hex[32] = '\0';
    
    /* Step 2: md5(inner_hex + salt) */
    if (EVP_DigestInit_ex(mdctx, EVP_md5(), NULL) != 1 ||
        EVP_DigestUpdate(mdctx, inner_hex, 32) != 1 ||
        EVP_DigestUpdate(mdctx, salt, 4) != 1 ||
        EVP_DigestFinal_ex(mdctx, outer, NULL) != 1) {
        EVP_MD_CTX_free(mdctx);
        return NULL;
    }
    EVP_MD_CTX_free(mdctx);
    
    /* Build result: "md5" + 32 hex chars + null */
    char* result = keel_malloc(36);
    if (!result) return NULL;
    
    result[0] = 'm'; result[1] = 'd'; result[2] = '5';
    for (int i = 0; i < 16; i++) {
        result[3 + i * 2]     = hex[outer[i] >> 4];
        result[3 + i * 2 + 1] = hex[outer[i] & 0x0f];
    }
    result[35] = '\0';
    return result;
}
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

#define SCRAM_SHA256_DIGEST_LEN 32
#define SCRAM_NONCE_LEN 24
#define SCRAM_DEFAULT_ITERATIONS 4096
#define MAX_PROVIDERS 16
#define MAX_USERS 1024

/* ============================================================================
 * Internal Types
 * ============================================================================ */

/**
 * @brief SCRAM-SHA-256 server-side authentication context
 */
typedef struct {
    /* Server nonce (client nonce + server extension) */
    char* server_nonce;
    char* client_nonce;
    
    /* From user database */
    uint8_t salt[32];
    size_t salt_len;
    int iterations;
    uint8_t stored_key[SCRAM_SHA256_DIGEST_LEN];
    uint8_t server_key[SCRAM_SHA256_DIGEST_LEN];
    
    /* Messages for signature verification */
    char* client_first_bare;
    char* server_first;
    char* client_final_without_proof;
    
    /* State */
    int step;
    bool has_derived_keys; /* true when stored_key/server_key derived from plaintext */
    
    /* Pending message to send */
    char* pending_message;
    size_t pending_len;
    int pending_type;
} scram_server_ctx_t;

/**
 * @brief User entry in the user database
 */
typedef struct user_entry {
    keel_auth_user_t user;
    struct user_entry* next;
} user_entry_t;

/**
 * @brief Authentication manager
 */
struct keel_auth_manager {
    keel_auth_provider_t* providers[MAX_PROVIDERS];
    size_t provider_count;
    
    keel_auth_method_t default_method;
    bool allow_clear_password;
    int scram_iterations;
    
    /* User database (hash map would be better, but simple linked list for now) */
    user_entry_t* users;
    size_t user_count;
    
    /* External user lookup */
    keel_auth_user_lookup_fn user_lookup;
    void* user_lookup_data;
};

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Return a human-readable name for an authentication state.
 */
const char* keel_auth_state_name(keel_auth_state_t state) {
    switch (state) {
    case KEEL_AUTH_STATE_INIT:      return "INIT";
    case KEEL_AUTH_STATE_CHALLENGE: return "CHALLENGE";
    case KEEL_AUTH_STATE_VERIFY:    return "VERIFY";
    case KEEL_AUTH_STATE_SUCCESS:   return "SUCCESS";
    case KEEL_AUTH_STATE_FAILED:    return "FAILED";
    case KEEL_AUTH_STATE_ERROR:     return "ERROR";
    default:                       return "UNKNOWN";
    }
}

/**
 * @brief Return a human-readable name for an authentication method.
 */
const char* keel_auth_method_name(keel_auth_method_t method) {
    switch (method) {
    case KEEL_AUTH_NONE:          return "none";
    case KEEL_AUTH_TRUST:         return "trust";
    case KEEL_AUTH_PASSWORD:      return "password";
    case KEEL_AUTH_MD5:           return "md5";
    case KEEL_AUTH_SCRAM_SHA_256: return "scram-sha-256";
    case KEEL_AUTH_GSSAPI:        return "gss";
    case KEEL_AUTH_SSPI:          return "sspi";
    case KEEL_AUTH_CERTIFICATE:   return "cert";
    case KEEL_AUTH_LDAP:          return "ldap";
    case KEEL_AUTH_RADIUS:        return "radius";
    case KEEL_AUTH_PAM:           return "pam";
    case KEEL_AUTH_REJECT:        return "reject";
    case KEEL_AUTH_PASSTHROUGH:   return "passthrough";
    default:                     return "unknown";
    }
}

/* ============================================================================
 * Base64 Encoding/Decoding
 * ============================================================================ */

static const char base64_chars[] = 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/**
 * @brief Base64-encode a binary buffer.
 *
 * @param data Source bytes.
 * @param len Source length.
 * @return Heap-allocated Base64 string, or `NULL` on allocation failure.
 */
static char* base64_encode(const uint8_t* data, size_t len) {
    size_t out_len = ((len + 2) / 3) * 4;
    char* out = keel_malloc(out_len + 1);
    if (!out) return NULL;
    
    size_t i, j;
    for (i = 0, j = 0; i < len;) {
        uint32_t octet_a = i < len ? data[i++] : 0;
        uint32_t octet_b = i < len ? data[i++] : 0;
        uint32_t octet_c = i < len ? data[i++] : 0;
        
        uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;
        
        out[j++] = base64_chars[(triple >> 18) & 0x3f];
        out[j++] = base64_chars[(triple >> 12) & 0x3f];
        out[j++] = base64_chars[(triple >> 6) & 0x3f];
        out[j++] = base64_chars[triple & 0x3f];
    }
    
    /* Padding */
    size_t mod = len % 3;
    if (mod == 1) {
        out[out_len - 1] = '=';
        out[out_len - 2] = '=';
    } else if (mod == 2) {
        out[out_len - 1] = '=';
    }
    
    out[out_len] = '\0';
    return out;
}

/**
 * @brief Decode one Base64 alphabet character.
 *
 * @return Sextet value, or `-1` for invalid characters.
 */
static int base64_decode_char(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

/**
 * @brief Decode a Base64 string into a heap-allocated byte buffer.
 *
 * @param str Base64 text.
 * @param[out] out_len Decoded byte length.
 * @return Heap-allocated byte buffer, or `NULL` on malformed input/allocation failure.
 */
static uint8_t* base64_decode(const char* str, size_t* out_len) {
    size_t len = strlen(str);
    if (len % 4 != 0) return NULL;
    
    size_t decoded_len = (len / 4) * 3;
    if (len > 0 && str[len - 1] == '=') decoded_len--;
    if (len > 1 && str[len - 2] == '=') decoded_len--;
    
    uint8_t* out = keel_malloc(decoded_len);
    if (!out) return NULL;
    
    size_t i, j;
    for (i = 0, j = 0; i < len;) {
        int a = str[i] == '=' ? 0 : base64_decode_char(str[i]); i++;
        int b = str[i] == '=' ? 0 : base64_decode_char(str[i]); i++;
        int c = str[i] == '=' ? 0 : base64_decode_char(str[i]); i++;
        int d = str[i] == '=' ? 0 : base64_decode_char(str[i]); i++;
        
        if (a < 0 || b < 0 || c < 0 || d < 0) {
            keel_free(out);
            return NULL;
        }
        
        uint32_t triple = ((uint32_t)a << 18) | ((uint32_t)b << 12) | 
                          ((uint32_t)c << 6) | (uint32_t)d;
        
        if (j < decoded_len) out[j++] = (uint8_t)(triple >> 16);
        if (j < decoded_len) out[j++] = (uint8_t)(triple >> 8);
        if (j < decoded_len) out[j++] = (uint8_t)triple;
    }
    
    *out_len = decoded_len;
    return out;
}

/* ============================================================================
 * Crypto Helpers (OpenSSL)
 * ============================================================================ */

#ifdef KEEL_HAS_OPENSSL

/**
 * @brief Compute an HMAC-SHA-256 digest.
 */
static bool hmac_sha256(const uint8_t* key, size_t key_len,
                        const uint8_t* data, size_t data_len,
                        uint8_t out[SCRAM_SHA256_DIGEST_LEN]) {
    unsigned int out_len = SCRAM_SHA256_DIGEST_LEN;
    return HMAC(EVP_sha256(), key, (int)key_len, data, data_len, out, &out_len) != NULL;
}

/**
 * @brief Compute a SHA-256 digest.
 */
static bool sha256_hash(const uint8_t* data, size_t len, 
                        uint8_t out[SCRAM_SHA256_DIGEST_LEN]) {
    return SHA256(data, len, out) != NULL;
}

/**
 * @brief XOR one byte buffer in place with another buffer of equal length.
 */
static void xor_bytes(uint8_t* a, const uint8_t* b, size_t len) {
    for (size_t i = 0; i < len; i++) {
        a[i] ^= b[i];
    }
}

/**
 * @brief Derive the SCRAM salted password using PBKDF2-HMAC-SHA-256.
 */
static bool pbkdf2_sha256(const char* password, const uint8_t* salt, size_t salt_len,
                          int iterations, uint8_t out[SCRAM_SHA256_DIGEST_LEN]) {
    return PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                              salt, (int)salt_len,
                              iterations, EVP_sha256(),
                              SCRAM_SHA256_DIGEST_LEN, out) == 1;
}

/**
 * @brief Generate a random nonce and encode it as Base64 text.
 *
 * @param[out] buf Destination text buffer.
 * @param len Random byte count before Base64 encoding.
 * @return `true` on success, `false` on RNG or allocation failure.
 */
static bool generate_nonce(char* buf, size_t len) {
    uint8_t* random = keel_malloc(len);
    if (!random) return false;
    
    if (RAND_bytes(random, (int)len) != 1) {
        keel_free(random);
        return false;
    }
    
    char* b64 = base64_encode(random, len);
    keel_free(random);
    
    if (!b64) return false;
    
    strncpy(buf, b64, len * 2);
    buf[len * 2 - 1] = '\0';
    keel_free(b64);
    
    return true;
}

#endif /* KEEL_HAS_OPENSSL */

/* ============================================================================
 * SCRAM-SHA-256 Server Provider Implementation
 * ============================================================================ */

/** @brief Return the SCRAM-SHA-256 provider name string. */
static const char* scram_name(void) {
    return "scram-sha-256";
}

/** @brief Return the auth method enum value for SCRAM-SHA-256. */
static keel_auth_method_t scram_method(void) {
    return KEEL_AUTH_SCRAM_SHA_256;
}

/**
 * @brief Initialize the SCRAM-SHA-256 provider (no-op).
 *
 * @param provider Provider instance.
 * @param config   Unused configuration pointer.
 * @return KEEL_OK always.
 */
static keel_error_t scram_init(keel_auth_provider_t* provider, const void* config) {
    (void)config;
    provider->provider_data = NULL;
    return KEEL_OK;
}

/**
 * @brief Destroy the SCRAM-SHA-256 provider (no-op).
 *
 * @param provider Provider instance to destroy.
 */
static void scram_destroy(keel_auth_provider_t* provider) {
    (void)provider;
}

/**
 * @brief Free a SCRAM server authentication context and all associated resources.
 *
 * @param sctx SCRAM server context to free. No-op if NULL.
 */
static void scram_free_server_ctx(scram_server_ctx_t* sctx) {
    if (!sctx) return;
    keel_free(sctx->server_nonce);
    keel_free(sctx->client_nonce);
    keel_free(sctx->client_first_bare);
    keel_free(sctx->server_first);
    keel_free(sctx->client_final_without_proof);
    keel_free(sctx->pending_message);
    keel_free(sctx);
}

/**
 * @brief Start a SCRAM-SHA-256 authentication exchange.
 *
 * Allocates an auth context, copies the user's SCRAM keys when available (or
 * generates a fake salt for timing-attack resistance when the user is unknown),
 * and prepares the initial AuthenticationSASL message.
 *
 * @param provider  SCRAM provider instance.
 * @param username  Client-supplied username.
 * @param user      Looked-up user record, or NULL if not found.
 * @param ctx_out   Output: newly allocated auth context.
 * @return KEEL_OK on success, error code otherwise.
 */
static keel_error_t scram_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
#ifndef KEEL_HAS_OPENSSL
    (void)provider; (void)username; (void)user; (void)ctx_out;
    return KEEL_ERR_NOT_SUPPORTED;
#else
    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;
    
    ctx->provider = provider;
    ctx->state = KEEL_AUTH_STATE_INIT;
    ctx->username = keel_strdup(username);
    ctx->user = user;
    
    if (!ctx->username) {
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    
    scram_server_ctx_t* sctx = keel_calloc(1, sizeof(scram_server_ctx_t));
    if (!sctx) {
        keel_free(ctx->username);
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    
    /* Copy user's SCRAM keys if available */
    if (user && user->has_scram_keys) {
        memcpy(sctx->stored_key, user->stored_key, SCRAM_SHA256_DIGEST_LEN);
        memcpy(sctx->server_key, user->server_key, SCRAM_SHA256_DIGEST_LEN);
        sctx->iterations = user->iterations > 0 ? user->iterations : SCRAM_DEFAULT_ITERATIONS;
        
        /* Parse salt from password_salt */
        if (user->password_salt) {
            size_t salt_len;
            uint8_t* salt = base64_decode(user->password_salt, &salt_len);
            if (salt && salt_len <= sizeof(sctx->salt)) {
                memcpy(sctx->salt, salt, salt_len);
                sctx->salt_len = salt_len;
            }
            keel_free(salt);
        }
    } else {
        /* Generate random salt for fake response (timing attack mitigation) */
        RAND_bytes(sctx->salt, 16);
        sctx->salt_len = 16;
        sctx->iterations = SCRAM_DEFAULT_ITERATIONS;

        /* If user has a plaintext password, derive SCRAM keys on the fly
         * using the same salt that will be sent to the client. */
        if (user && user->password_hash && user->password_hash[0] &&
            strncmp(user->password_hash, "md5", 3) != 0) {
            uint8_t salted_pw[SCRAM_SHA256_DIGEST_LEN];
            uint8_t client_key[SCRAM_SHA256_DIGEST_LEN];
            /* Use sctx->salt (already randomised above) for derivation so the
             * key matches what the client will compute from the server-first msg */
            if (pbkdf2_sha256(user->password_hash, sctx->salt, sctx->salt_len,
                              sctx->iterations, salted_pw) &&
                hmac_sha256(salted_pw, SCRAM_SHA256_DIGEST_LEN,
                            (const uint8_t*)"Client Key", 10, client_key) &&
                sha256_hash(client_key, SCRAM_SHA256_DIGEST_LEN, sctx->stored_key) &&
                hmac_sha256(salted_pw, SCRAM_SHA256_DIGEST_LEN,
                            (const uint8_t*)"Server Key", 10, sctx->server_key)) {
                sctx->has_derived_keys = true;
            }
        }
    }
    
    sctx->step = 0;
    ctx->auth_data = sctx;
    
    /* Build SASL authentication request message */
    /* PostgreSQL message: AuthenticationSASL with mechanism list */
    const char* mechanism = "SCRAM-SHA-256";
    size_t mech_len = strlen(mechanism);
    
    /* Message format: int32 auth_type (10) + mechanism list (null-terminated strings + final null) */
    size_t msg_len = 4 + mech_len + 1 + 1;  /* type + mechanism + null + final null */
    sctx->pending_message = keel_malloc(msg_len);
    if (!sctx->pending_message) {
        scram_free_server_ctx(sctx);
        keel_free(ctx->username);
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    
    /* Auth type 10 = SASL */
    uint8_t* p = (uint8_t*)sctx->pending_message;
    p[0] = 0; p[1] = 0; p[2] = 0; p[3] = 10;
    memcpy(p + 4, mechanism, mech_len + 1);
    p[4 + mech_len + 1] = 0;  /* Final null terminator */
    
    sctx->pending_len = msg_len;
    sctx->pending_type = 10;  /* AuthenticationSASL */
    
    ctx->state = KEEL_AUTH_STATE_CHALLENGE;
    *ctx_out = ctx;
    
    return KEEL_OK;
#endif
}

/**
 * @brief Process an incoming SCRAM client message.
 *
 * Handles step 0 (client-first-message) and step 1 (client-final-message),
 * verifying the client proof and computing the server signature on success.
 *
 * @param ctx  Active auth context.
 * @param data Raw message bytes received from the client.
 * @param len  Byte count of @p data.
 * @return New authentication state.
 */
static keel_auth_state_t scram_process(
    keel_auth_context_t* ctx,
    const void* data,
    size_t len)
{
#ifndef KEEL_HAS_OPENSSL
    (void)ctx; (void)data; (void)len;
    return KEEL_AUTH_STATE_ERROR;
#else
    scram_server_ctx_t* sctx = ctx->auth_data;
    if (!sctx) return KEEL_AUTH_STATE_ERROR;

    const char* msg = (const char*)data;

    /* NOTE: do NOT log `msg` / raw SCRAM data — it contains authentication
     * material (nonce, ClientProof, client-first-message-bare) that must
     * never appear in log files.  Log only non-secret diagnostics. */
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_AUTH, "[SCRAM] step=%d received_len=%zu",
                   sctx->step, len);
    
    switch (sctx->step) {
    case 0: {
        /* Expecting client-first-message */
        /* Format: gs2-header "," client-first-message-bare */
        /* gs2-header = "n,," or "y,," or "p=..." */
        
        /* Skip gs2-header (find second comma) */
        const char* p = msg;
        if (*p != 'n' && *p != 'y' && *p != 'p') {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            ctx->error_message = keel_strdup("Invalid GS2 header");
            return KEEL_AUTH_STATE_FAILED;
        }
        
        /* Find client-first-message-bare (after "n,,") */
        const char* comma1 = strchr(p, ',');
        if (!comma1) {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            return KEEL_AUTH_STATE_FAILED;
        }
        const char* comma2 = strchr(comma1 + 1, ',');
        if (!comma2) {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            return KEEL_AUTH_STATE_FAILED;
        }
        
        sctx->client_first_bare = keel_strdup(comma2 + 1);
        if (!sctx->client_first_bare) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* Extract client nonce from client-first-message-bare */
        /* Format: n=username,r=nonce,... */
        const char* nonce_start = strstr(sctx->client_first_bare, "r=");
        if (!nonce_start) {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            return KEEL_AUTH_STATE_FAILED;
        }
        nonce_start += 2;
        
        const char* nonce_end = strchr(nonce_start, ',');
        size_t nonce_len = nonce_end ? (size_t)(nonce_end - nonce_start) : strlen(nonce_start);
        
        sctx->client_nonce = keel_strndup(nonce_start, nonce_len);
        if (!sctx->client_nonce) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* Generate server nonce extension */
        char server_ext[33];
        if (!generate_nonce(server_ext, 16)) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        size_t server_nonce_len = nonce_len + strlen(server_ext) + 1;
        sctx->server_nonce = keel_malloc(server_nonce_len);
        if (!sctx->server_nonce) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        snprintf(sctx->server_nonce, server_nonce_len, "%s%s", 
                 sctx->client_nonce, server_ext);
        
        /* Build server-first-message */
        /* Format: r=combined_nonce,s=salt_b64,i=iterations */
        char* salt_b64 = base64_encode(sctx->salt, sctx->salt_len);
        if (!salt_b64) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        size_t sf_len = strlen(sctx->server_nonce) + strlen(salt_b64) + 32;
        sctx->server_first = keel_malloc(sf_len);
        if (!sctx->server_first) {
            keel_free(salt_b64);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        snprintf(sctx->server_first, sf_len, "r=%s,s=%s,i=%d",
                 sctx->server_nonce, salt_b64, sctx->iterations);
        keel_free(salt_b64);
        
        /* Build pending SASL continue message */
        keel_free(sctx->pending_message);
        size_t server_first_len = strlen(sctx->server_first);
        sctx->pending_len = 4 + server_first_len;
        sctx->pending_message = keel_malloc(sctx->pending_len);
        if (!sctx->pending_message) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* Auth type 11 = SASL Continue */
        uint8_t* out = (uint8_t*)sctx->pending_message;
        out[0] = 0; out[1] = 0; out[2] = 0; out[3] = 11;
        memcpy(out + 4, sctx->server_first, server_first_len);
        sctx->pending_type = 11;
        
        sctx->step = 1;
        ctx->state = KEEL_AUTH_STATE_CHALLENGE;
        return KEEL_AUTH_STATE_CHALLENGE;
    }
    
    case 1: {
        /* Expecting client-final-message */
        /* Format: c=channel_binding,r=nonce,p=proof */
        
        /* Verify nonce */
        const char* nonce_start = strstr(msg, "r=");
        if (!nonce_start) {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            return KEEL_AUTH_STATE_FAILED;
        }
        nonce_start += 2;
        
        const char* nonce_end = strchr(nonce_start, ',');
        size_t nonce_len = nonce_end ? (size_t)(nonce_end - nonce_start) : strlen(nonce_start);
        
        if (strlen(sctx->server_nonce) != nonce_len ||
            strncmp(nonce_start, sctx->server_nonce, nonce_len) != 0) {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            ctx->error_message = keel_strdup("Nonce mismatch");
            return KEEL_AUTH_STATE_FAILED;
        }
        
        /* Extract client-final-without-proof and proof */
        const char* proof_start = strstr(msg, ",p=");
        if (!proof_start) {
            ctx->state = KEEL_AUTH_STATE_FAILED;
            return KEEL_AUTH_STATE_FAILED;
        }
        
        sctx->client_final_without_proof = keel_strndup(msg, (size_t)(proof_start - msg));
        if (!sctx->client_final_without_proof) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        proof_start += 3;  /* Skip ",p=" */
        
        /* Decode client proof */
        size_t proof_len;
        uint8_t* client_proof = base64_decode(proof_start, &proof_len);
        if (!client_proof || proof_len != SCRAM_SHA256_DIGEST_LEN) {
            keel_free(client_proof);
            ctx->state = KEEL_AUTH_STATE_FAILED;
            return KEEL_AUTH_STATE_FAILED;
        }
        
        /* Verify proof if we have user credentials */
        if ((!ctx->user || !ctx->user->has_scram_keys) && !sctx->has_derived_keys) {
            /* No valid user - fail auth */
            keel_free(client_proof);
            ctx->state = KEEL_AUTH_STATE_FAILED;
            ctx->error_message = keel_strdup("Unknown user");
            return KEEL_AUTH_STATE_FAILED;
        }
        
        /* Compute AuthMessage */
        size_t auth_msg_len = strlen(sctx->client_first_bare) + 1 +
                              strlen(sctx->server_first) + 1 +
                              strlen(sctx->client_final_without_proof) + 1;
        char* auth_message = keel_malloc(auth_msg_len);
        if (!auth_message) {
            keel_free(client_proof);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        snprintf(auth_message, auth_msg_len, "%s,%s,%s",
                 sctx->client_first_bare, sctx->server_first,
                 sctx->client_final_without_proof);
        
        /* ClientSignature = HMAC(StoredKey, AuthMessage) */
        uint8_t client_sig[SCRAM_SHA256_DIGEST_LEN];
        if (!hmac_sha256(sctx->stored_key, SCRAM_SHA256_DIGEST_LEN,
                         (const uint8_t*)auth_message, strlen(auth_message), client_sig)) {
            keel_free(auth_message);
            keel_free(client_proof);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* RecoveredClientKey = ClientProof XOR ClientSignature */
        uint8_t recovered_key[SCRAM_SHA256_DIGEST_LEN];
        memcpy(recovered_key, client_proof, SCRAM_SHA256_DIGEST_LEN);
        xor_bytes(recovered_key, client_sig, SCRAM_SHA256_DIGEST_LEN);
        keel_free(client_proof);
        
        /* VerifyStoredKey = H(RecoveredClientKey) */
        uint8_t verify_key[SCRAM_SHA256_DIGEST_LEN];
        if (!sha256_hash(recovered_key, SCRAM_SHA256_DIGEST_LEN, verify_key)) {
            keel_free(auth_message);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* Compare with stored key.
         *
         * A3 (review_20260618_01.md): use CRYPTO_memcmp instead of memcmp.
         * libc memcmp short-circuits on the first mismatched byte, leaking
         * byte-by-byte timing information about *which* prefix of the
         * recovered StoredKey agrees with the stored value. An attacker
         * with many auth attempts (e.g. co-located on the same host as
         * the admin socket, or LAN-adjacent) can statistically recover
         * StoredKey and forge a valid ClientProof. CRYPTO_memcmp takes the
         * same number of cycles regardless of where (or whether) the
         * buffers differ. Both buffers are exactly
         * SCRAM_SHA256_DIGEST_LEN (32) bytes — no length-mismatch
         * concern. */
        if (CRYPTO_memcmp(verify_key, sctx->stored_key, SCRAM_SHA256_DIGEST_LEN) != 0) {
            /* NOTE: auth_message contains the SCRAM auth-message string which
             * is derived from exchanged nonces and salts — do NOT log it.
             * Log only byte lengths so operators can diagnose protocol
             * framing bugs without leaking authentication material. */
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                           "[SCRAM] ClientProof verification FAILED: "
                           "cfb_len=%zu sf_len=%zu cfwp_len=%zu",
                           strlen(sctx->client_first_bare),
                           strlen(sctx->server_first),
                           strlen(sctx->client_final_without_proof));
            keel_free(auth_message);
            ctx->state = KEEL_AUTH_STATE_FAILED;
            ctx->error_message = keel_strdup("Invalid password");
            return KEEL_AUTH_STATE_FAILED;
        }
        
        /* Authentication successful! Build server-final-message */
        /* ServerSignature = HMAC(ServerKey, AuthMessage) */
        uint8_t server_sig[SCRAM_SHA256_DIGEST_LEN];
        if (!hmac_sha256(sctx->server_key, SCRAM_SHA256_DIGEST_LEN,
                         (const uint8_t*)auth_message, strlen(auth_message), server_sig)) {
            keel_free(auth_message);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        keel_free(auth_message);
        
        char* sig_b64 = base64_encode(server_sig, SCRAM_SHA256_DIGEST_LEN);
        if (!sig_b64) {
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* Build server-final-message: v=server_signature */
        size_t sf_len = 2 + strlen(sig_b64) + 1;
        char* server_final = keel_malloc(sf_len);
        if (!server_final) {
            keel_free(sig_b64);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        snprintf(server_final, sf_len, "v=%s", sig_b64);
        keel_free(sig_b64);
        
        /* Build pending SASL final message */
        keel_free(sctx->pending_message);
        size_t server_final_len = strlen(server_final);
        sctx->pending_len = 4 + server_final_len;
        sctx->pending_message = keel_malloc(sctx->pending_len);
        if (!sctx->pending_message) {
            keel_free(server_final);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            return KEEL_AUTH_STATE_ERROR;
        }
        
        /* Auth type 12 = SASL Final */
        uint8_t* out = (uint8_t*)sctx->pending_message;
        out[0] = 0; out[1] = 0; out[2] = 0; out[3] = 12;
        memcpy(out + 4, server_final, server_final_len);
        sctx->pending_type = 12;
        keel_free(server_final);
        
        sctx->step = 2;
        ctx->state = KEEL_AUTH_STATE_SUCCESS;
        return KEEL_AUTH_STATE_SUCCESS;
    }
    
    default:
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }
#endif
}

/**
 * @brief Retrieve the pending outbound SCRAM authentication message.
 *
 * Transfers ownership of the message buffer to the caller; subsequent calls
 * return KEEL_ERR_NOT_FOUND until the next step populates a new message.
 *
 * @param ctx      Active auth context.
 * @param msg_out  Output: pointer to message buffer (caller takes ownership).
 * @param len_out  Output: message byte length.
 * @param type_out Output: PostgreSQL authentication message type code.
 * @return KEEL_OK, or KEEL_ERR_NOT_FOUND if no pending message.
 */
static keel_error_t scram_get_message(
    keel_auth_context_t* ctx,
    void** msg_out,
    size_t* len_out,
    int* type_out)
{
    scram_server_ctx_t* sctx = ctx->auth_data;
    if (!sctx || !sctx->pending_message) {
        return KEEL_ERR_NOT_FOUND;
    }
    
    *msg_out = sctx->pending_message;
    *len_out = sctx->pending_len;
    *type_out = sctx->pending_type;
    
    /* Transfer ownership */
    sctx->pending_message = NULL;
    sctx->pending_len = 0;
    
    return KEEL_OK;
}

/**
 * @brief Free a SCRAM auth context and all owned memory.
 *
 * @param ctx Auth context to free. No-op if NULL.
 */
static void scram_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    
    scram_free_server_ctx(ctx->auth_data);
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t scram_sha256_ops = {
    .name = scram_name,
    .method = scram_method,
    .init = scram_init,
    .destroy = scram_destroy,
    .start = scram_start,
    .process = scram_process,
    .get_message = scram_get_message,
    .free_context = scram_free_context,
};

/**
 * @brief Return the ops table for the SCRAM-SHA-256 auth provider.
 *
 * @return Pointer to the static SCRAM-SHA-256 provider operations structure.
 */
const keel_auth_provider_ops_t* keel_auth_scram_sha256_ops(void) {
    return &scram_sha256_ops;
}

/* ============================================================================
 * MD5 Provider
 * ============================================================================ */

typedef struct {
    uint8_t salt[4];            /* MD5 salt sent to client */
    void* pending_message;      /* Message to send */
    size_t pending_len;
    int pending_type;           /* PostgreSQL auth type */
} md5_server_ctx_t;

/** @brief Return the MD5 provider name string. */
static const char* md5_name(void) { return "md5"; }
/** @brief Return the auth method enum value for MD5. */
static keel_auth_method_t md5_method(void) { return KEEL_AUTH_MD5; }
/**
 * @brief Initialize the MD5 provider (no-op).
 * @param p Unused provider pointer.
 * @param c Unused config pointer.
 * @return KEEL_OK always.
 */
static keel_error_t md5_init(keel_auth_provider_t* p, const void* c) { (void)p; (void)c; return KEEL_OK; }
/**
 * @brief Destroy the MD5 provider (no-op).
 * @param p Unused provider pointer.
 */
static void md5_destroy(keel_auth_provider_t* p) { (void)p; }

/**
 * @brief Free an MD5 server authentication context.
 *
 * @param ctx MD5 server context to free. No-op if NULL.
 */
static void md5_free_server_ctx(md5_server_ctx_t* ctx) {
    if (!ctx) return;
    keel_free(ctx->pending_message);
    keel_free(ctx);
}

/**
 * @brief Start an MD5 password authentication exchange.
 *
 * Generates a random 4-byte salt, builds the AuthenticationMD5Password
 * message, and returns a new auth context.
 *
 * @param provider  MD5 provider instance.
 * @param username  Client-supplied username.
 * @param user      Looked-up user record (may be NULL).
 * @param ctx_out   Output: newly allocated auth context.
 * @return KEEL_OK on success, error code otherwise.
 */
static keel_error_t md5_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
    (void)user;
    
    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;
    
    ctx->provider = provider;
    ctx->state = KEEL_AUTH_STATE_INIT;
    ctx->username = keel_strdup(username);
    ctx->user = user;
    
    if (!ctx->username) {
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    
    md5_server_ctx_t* mctx = keel_calloc(1, sizeof(md5_server_ctx_t));
    if (!mctx) {
        keel_free(ctx->username);
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    
#ifdef KEEL_HAS_OPENSSL
    /* Generate random salt */
    if (RAND_bytes(mctx->salt, 4) != 1) {
        keel_free(mctx);
        keel_free(ctx->username);
        keel_free(ctx);
        return KEEL_ERR_AUTH;
    }
#else
    /* Fallback: use current time mixed with address */
    uint32_t seed = (uint32_t)((uintptr_t)ctx ^ (uintptr_t)mctx);
    mctx->salt[0] = (uint8_t)(seed & 0xFF);
    mctx->salt[1] = (uint8_t)((seed >> 8) & 0xFF);
    mctx->salt[2] = (uint8_t)((seed >> 16) & 0xFF);
    mctx->salt[3] = (uint8_t)((seed >> 24) & 0xFF);
#endif
    
    ctx->auth_data = mctx;
    
    /* Build AuthenticationMD5Password message */
    /* Message body: int32 auth_type (5) + 4 bytes salt */
    size_t msg_len = 4 + 4;  /* type + salt */
    mctx->pending_message = keel_malloc(msg_len);
    if (!mctx->pending_message) {
        md5_free_server_ctx(mctx);
        keel_free(ctx->username);
        keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    
    uint8_t* p = (uint8_t*)mctx->pending_message;
    p[0] = 0; p[1] = 0; p[2] = 0; p[3] = 5;  /* Auth type 5 = MD5 */
    memcpy(p + 4, mctx->salt, 4);
    
    mctx->pending_len = msg_len;
    mctx->pending_type = 5;  /* AuthenticationMD5Password */
    
    ctx->state = KEEL_AUTH_STATE_CHALLENGE;
    *ctx_out = ctx;
    
    return KEEL_OK;
}

/**
 * @brief Process the client's MD5 password response.
 *
 * Recomputes the expected MD5 hash from the stored plaintext password and the
 * challenge salt, then performs a constant-time comparison.
 *
 * @param ctx  Active auth context.
 * @param data Client password response bytes.
 * @param len  Byte count of @p data.
 * @return New authentication state.
 */
static keel_auth_state_t md5_process(
    keel_auth_context_t* ctx,
    const void* data,
    size_t len)
{
#ifndef KEEL_HAS_OPENSSL
    (void)ctx; (void)data; (void)len;
    ctx->state = KEEL_AUTH_STATE_ERROR;
    ctx->error_message = keel_strdup("MD5 auth not supported (no OpenSSL)");
    return KEEL_AUTH_STATE_ERROR;
#else
    md5_server_ctx_t* mctx = ctx->auth_data;
    if (!mctx) {
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }
    
    /* Client sends password response: 'p' + length + password_hash + null */
    /* The data we receive is just the password hash (null-terminated) */
    const char* client_hash = (const char*)data;
    
    /* Validate: the PostgreSQL MD5 response is exactly "md5" + 32 hex
     * chars = 35 bytes. The wire framing strips the trailing NUL before
     * calling us, so `len` is the payload size without it.
     *
     * A4/M10 (review_20260618_01.md): the previous check was `len < 35`,
     * which accepted arbitrarily long responses starting with "md5" and
     * then fed them to strcmp — both a timing channel ( strcmp scans
     * until the first NUL/mismatch) and a wasteful-input vector.
     * Tightening to `len != 35` rejects malformed responses at the door
     * so the constant-time compare below always runs on exactly 35 bytes. */
    if (len != 35 || strncmp(client_hash, "md5", 3) != 0) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "Invalid MD5 response format");
        ctx->state = KEEL_AUTH_STATE_FAILED;
        ctx->error_message = keel_strdup("Invalid MD5 password format");
        return KEEL_AUTH_STATE_FAILED;
    }
    
    /* If no user or no password, fail */
    if (!ctx->user || !ctx->user->password_hash) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "No password stored for user %s", ctx->username);
        ctx->state = KEEL_AUTH_STATE_FAILED;
        ctx->error_message = keel_strdup("password authentication failed");
        return KEEL_AUTH_STATE_FAILED;
    }
    
    /* Compute expected hash: md5(md5(password + user) + salt) */
    /* For MD5, password_hash contains the plaintext password */
    char* expected = keel_md5_password(ctx->username, ctx->user->password_hash, mctx->salt);
    if (!expected) {
        ctx->state = KEEL_AUTH_STATE_ERROR;
        ctx->error_message = keel_strdup("MD5 hash computation failed");
        return KEEL_AUTH_STATE_ERROR;
    }
    
    /* A4 (review_20260618_01.md): the function-level docstring at line ~1069
     * already promises "constant-time comparison" but the previous body used
     * strcmp, which short-circuits on the first differing byte. CRYPTO_memcmp
     * runs in time independent of where the buffers first differ.
     *
     * Both `client_hash` (validated to len == 35 above) and `expected`
     * (always "md5" + 32 hex from keel_md5_password) are exactly 35 bytes,
     * so a fixed-length constant-time compare is correct here. */
    bool match = (CRYPTO_memcmp(client_hash, expected, 35) == 0);
    keel_free(expected);
    
    if (match) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "MD5 auth successful for user %s", ctx->username);
        ctx->state = KEEL_AUTH_STATE_SUCCESS;
        return KEEL_AUTH_STATE_SUCCESS;
    } else {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "MD5 auth failed for user %s", ctx->username);
        ctx->state = KEEL_AUTH_STATE_FAILED;
        ctx->error_message = keel_strdup("password authentication failed");
        return KEEL_AUTH_STATE_FAILED;
    }
#endif
}

/**
 * @brief Retrieve the pending outbound MD5 authentication message.
 *
 * Transfers ownership of the buffer to the caller.
 *
 * @param ctx      Active auth context.
 * @param msg_out  Output: message buffer (caller takes ownership).
 * @param len_out  Output: message byte length.
 * @param type_out Output: PostgreSQL authentication message type code.
 * @return KEEL_OK, or KEEL_ERR_NOT_FOUND if no pending message.
 */
static keel_error_t md5_get_message(
    keel_auth_context_t* ctx,
    void** msg_out,
    size_t* len_out,
    int* type_out)
{
    md5_server_ctx_t* mctx = ctx->auth_data;
    if (!mctx || !mctx->pending_message) {
        return KEEL_ERR_NOT_FOUND;
    }
    
    *msg_out = mctx->pending_message;
    *len_out = mctx->pending_len;
    *type_out = mctx->pending_type;
    
    /* Transfer ownership */
    mctx->pending_message = NULL;
    mctx->pending_len = 0;
    
    return KEEL_OK;
}

/**
 * @brief Free an MD5 auth context and all owned memory.
 *
 * @param ctx Auth context to free. No-op if NULL.
 */
static void md5_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    md5_free_server_ctx(ctx->auth_data);
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t md5_ops = {
    .name = md5_name,
    .method = md5_method,
    .init = md5_init,
    .destroy = md5_destroy,
    .start = md5_start,
    .process = md5_process,
    .get_message = md5_get_message,
    .free_context = md5_free_context,
};

/**
 * @brief Return the ops table for the MD5 auth provider.
 *
 * @return Pointer to the static MD5 provider operations structure.
 */
const keel_auth_provider_ops_t* keel_auth_md5_ops(void) {
    return &md5_ops;
}

/* ============================================================================
 * Trust Provider (No Authentication)
 * ============================================================================ */

/** @brief Return the trust provider name string. */
static const char* trust_name(void) { return "trust"; }
/** @brief Return the auth method enum value for TRUST. */
static keel_auth_method_t trust_method(void) { return KEEL_AUTH_TRUST; }
/**
 * @brief Initialize the trust provider (no-op).
 * @param p Unused provider pointer.
 * @param c Unused config pointer.
 * @return KEEL_OK always.
 */
static keel_error_t trust_init(keel_auth_provider_t* p, const void* c) { (void)p; (void)c; return KEEL_OK; }
/**
 * @brief Destroy the trust provider (no-op).
 * @param p Unused provider pointer.
 */
static void trust_destroy(keel_auth_provider_t* p) { (void)p; }

/**
 * @brief Start a trust (no-auth) session.
 *
 * Immediately sets state to SUCCESS; no challenge is issued.
 *
 * @param provider  Trust provider instance.
 * @param username  Client-supplied username.
 * @param user      Looked-up user record (unused).
 * @param ctx_out   Output: newly allocated auth context.
 * @return KEEL_OK on success, KEEL_ERR_NOMEM on allocation failure.
 */
static keel_error_t trust_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
    (void)user;
    
    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;
    
    ctx->provider = provider;
    ctx->username = keel_strdup(username);
    ctx->state = KEEL_AUTH_STATE_SUCCESS;
    
    *ctx_out = ctx;
    return KEEL_OK;
}

/**
 * @brief Process a client message for the trust provider (no-op).
 *
 * @param ctx Active auth context.
 * @param d   Unused data pointer.
 * @param l   Unused data length.
 * @return Current auth state (always SUCCESS for trust).
 */
static keel_auth_state_t trust_process(keel_auth_context_t* ctx, const void* d, size_t l) {
    (void)d; (void)l;
    return ctx->state;
}

/**
 * @brief Retrieve a pending message for the trust provider.
 *
 * Trust authentication never sends a challenge, so this always returns
 * KEEL_ERR_NOT_FOUND.
 *
 * @param ctx Unused auth context.
 * @param m   Unused message output pointer.
 * @param l   Unused length output pointer.
 * @param t   Unused type output pointer.
 * @return KEEL_ERR_NOT_FOUND always.
 */
static keel_error_t trust_get_message(keel_auth_context_t* ctx, void** m, size_t* l, int* t) {
    (void)ctx; (void)m; (void)l; (void)t;
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Free a trust auth context and all owned memory.
 *
 * @param ctx Auth context to free. No-op if NULL.
 */
static void trust_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t trust_ops = {
    .name = trust_name,
    .method = trust_method,
    .init = trust_init,
    .destroy = trust_destroy,
    .start = trust_start,
    .process = trust_process,
    .get_message = trust_get_message,
    .free_context = trust_free_context,
};

/**
 * @brief Return the ops table for the trust (no-auth) provider.
 *
 * @return Pointer to the static trust provider operations structure.
 */
const keel_auth_provider_ops_t* keel_auth_trust_ops(void) {
    return &trust_ops;
}

/* ============================================================================
 * Authentication Manager Implementation
 * ============================================================================ */

/**
 * @brief Create and initialize an authentication manager.
 *
 * Registers the built-in SCRAM-SHA-256, MD5, and trust providers automatically.
 *
 * @param config Optional manager configuration. If NULL, defaults are used
 *               (SCRAM-SHA-256 with 4096 iterations).
 * @return Heap-allocated manager, or NULL on allocation failure.
 */
keel_auth_manager_t* keel_auth_manager_create(const keel_auth_manager_config_t* config) {
    keel_auth_manager_t* mgr = keel_calloc(1, sizeof(keel_auth_manager_t));
    if (!mgr) return NULL;
    
    if (config) {
        mgr->default_method = config->default_method;
        mgr->allow_clear_password = config->allow_clear_password;
        mgr->scram_iterations = config->scram_iterations > 0 ? 
                                 config->scram_iterations : SCRAM_DEFAULT_ITERATIONS;
    } else {
        mgr->default_method = KEEL_AUTH_SCRAM_SHA_256;
        mgr->scram_iterations = SCRAM_DEFAULT_ITERATIONS;
    }
    
    /* Register built-in providers */
    keel_auth_manager_register(mgr, keel_auth_scram_sha256_ops(), NULL);
    keel_auth_manager_register(mgr, keel_auth_md5_ops(), NULL);
    keel_auth_manager_register(mgr, keel_auth_trust_ops(), NULL);
    
    return mgr;
}

/**
 * @brief Destroy an authentication manager and free all resources.
 *
 * Destroys all registered providers and frees the internal user list.
 *
 * @param mgr Manager to destroy. No-op if NULL.
 */
void keel_auth_manager_destroy(keel_auth_manager_t* mgr) {
    if (!mgr) return;
    
    /* Destroy providers */
    for (size_t i = 0; i < mgr->provider_count; i++) {
        if (mgr->providers[i]) {
            mgr->providers[i]->ops->destroy(mgr->providers[i]);
            keel_free(mgr->providers[i]);
        }
    }
    
    /* Free users */
    user_entry_t* entry = mgr->users;
    while (entry) {
        user_entry_t* next = entry->next;
        keel_free(entry->user.username);
        keel_free(entry->user.password_hash);
        keel_free(entry->user.password_salt);
        keel_free(entry->user.pool_name);
        keel_free(entry->user.database);
        keel_free(entry);
        entry = next;
    }
    
    keel_free(mgr);
}

/**
 * @brief Register an authentication provider with the manager.
 *
 * Allocates a provider instance, calls its init op, and appends it to the
 * manager's provider list.
 *
 * @param mgr    Manager to register the provider with.
 * @param ops    Provider operations table.
 * @param config Optional provider-specific configuration.
 * @return KEEL_OK on success, or an error code on failure.
 */
keel_error_t keel_auth_manager_register(
    keel_auth_manager_t* mgr,
    const keel_auth_provider_ops_t* ops,
    const void* config)
{
    if (!mgr || !ops) return KEEL_ERR_INVALID_ARG;
    if (mgr->provider_count >= MAX_PROVIDERS) return KEEL_ERR_OVERFLOW;
    
    keel_auth_provider_t* provider = keel_calloc(1, sizeof(keel_auth_provider_t));
    if (!provider) return KEEL_ERR_NOMEM;
    
    provider->ops = ops;
    
    keel_error_t err = ops->init(provider, config);
    if (err != KEEL_OK) {
        keel_free(provider);
        return err;
    }
    
    mgr->providers[mgr->provider_count++] = provider;
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Registered auth provider: %s", ops->name());
    
    return KEEL_OK;
}

/**
 * @brief Look up a registered provider by authentication method.
 *
 * @param mgr    Manager to search.
 * @param method Auth method to find.
 * @return Matching provider, or NULL if no provider handles @p method.
 */
keel_auth_provider_t* keel_auth_manager_get_provider(
    keel_auth_manager_t* mgr,
    keel_auth_method_t method)
{
    if (!mgr) return NULL;
    
    for (size_t i = 0; i < mgr->provider_count; i++) {
        if (mgr->providers[i]->ops->method() == method) {
            return mgr->providers[i];
        }
    }
    
    return NULL;
}

/**
 * @brief Override the default user lookup callback.
 *
 * When set, the manager calls @p lookup instead of its internal linked-list
 * search when resolving usernames during auth start.
 *
 * @param mgr       Manager to configure.
 * @param lookup    User lookup function, or NULL to restore the default.
 * @param user_data Opaque pointer forwarded to every @p lookup call.
 */
void keel_auth_manager_set_user_lookup(
    keel_auth_manager_t* mgr,
    keel_auth_user_lookup_fn lookup,
    void* user_data)
{
    if (!mgr) return;
    mgr->user_lookup = lookup;
    mgr->user_lookup_data = user_data;
}

/**
 * @brief Default user lookup that searches the manager's internal linked list.
 *
 * @param username  Username to look up.
 * @param user_out  Output: pointer to matched user record.
 * @param user_data Opaque pointer cast to keel_auth_manager_t.
 * @return KEEL_OK if found, KEEL_ERR_NOT_FOUND otherwise.
 */
static keel_error_t default_user_lookup(
    const char* username,
    const keel_auth_user_t** user_out,
    void* user_data)
{
    keel_auth_manager_t* mgr = user_data;
    
    user_entry_t* entry = mgr->users;
    while (entry) {
        if (strcmp(entry->user.username, username) == 0) {
            *user_out = &entry->user;
            return KEEL_OK;
        }
        entry = entry->next;
    }
    
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Begin an authentication exchange for the given username.
 *
 * Looks up the user, selects the manager's default method provider, and
 * delegates to the provider's start op.
 *
 * @param mgr      Authentication manager.
 * @param username Client-supplied username.
 * @param ctx_out  Output: newly allocated auth context.
 * @return KEEL_OK on success, error code otherwise.
 */
keel_error_t keel_auth_manager_start(
    keel_auth_manager_t* mgr,
    const char* username,
    keel_auth_context_t** ctx_out)
{
    if (!mgr || !username || !ctx_out) return KEEL_ERR_INVALID_ARG;
    
    /* Look up user */
    const keel_auth_user_t* user = NULL;
    keel_error_t err;
    
    if (mgr->user_lookup) {
        err = mgr->user_lookup(username, &user, mgr->user_lookup_data);
    } else {
        err = default_user_lookup(username, &user, mgr);
    }
    
    /* Get provider for default method */
    keel_auth_provider_t* provider = keel_auth_manager_get_provider(mgr, mgr->default_method);
    if (!provider) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "No provider for auth method %s",
                      keel_auth_method_name(mgr->default_method));
        return KEEL_ERR_NOT_FOUND;
    }
    
    /* Set user lookup on provider */
    provider->user_lookup = mgr->user_lookup ? mgr->user_lookup : default_user_lookup;
    provider->user_lookup_data = mgr->user_lookup ? mgr->user_lookup_data : mgr;
    
    return provider->ops->start(provider, username, user, ctx_out);
}

/**
 * @brief Feed a client message to an active auth context.
 *
 * @param ctx  Active auth context.
 * @param data Client message bytes.
 * @param len  Byte count of @p data.
 * @return New authentication state after processing.
 */
keel_auth_state_t keel_auth_process(
    keel_auth_context_t* ctx,
    const void* data,
    size_t len)
{
    if (!ctx || !ctx->provider) return KEEL_AUTH_STATE_ERROR;
    return ctx->provider->ops->process(ctx, data, len);
}

/**
 * @brief Retrieve the next outbound authentication message.
 *
 * @param ctx      Active auth context.
 * @param msg_out  Output: message buffer (caller takes ownership).
 * @param len_out  Output: byte length of the message.
 * @param type_out Output: PostgreSQL authentication message type code.
 * @return KEEL_OK on success, KEEL_ERR_NOT_FOUND if no pending message.
 */
keel_error_t keel_auth_get_message(
    keel_auth_context_t* ctx,
    void** msg_out,
    size_t* len_out,
    int* type_out)
{
    if (!ctx || !ctx->provider) return KEEL_ERR_INVALID_ARG;
    return ctx->provider->ops->get_message(ctx, msg_out, len_out, type_out);
}

/**
 * @brief Free an auth context returned by keel_auth_manager_start().
 *
 * Delegates to the owning provider's free_context op.
 *
 * @param ctx Auth context to free. No-op if NULL or has no provider.
 */
void keel_auth_context_free(keel_auth_context_t* ctx) {
    if (!ctx || !ctx->provider) return;
    ctx->provider->ops->free_context(ctx);
}

/**
 * @brief Return the current state of an auth context.
 *
 * @param ctx Auth context to query.
 * @return Current state, or KEEL_AUTH_STATE_ERROR if @p ctx is NULL.
 */
keel_auth_state_t keel_auth_get_state(keel_auth_context_t* ctx) {
    if (!ctx) return KEEL_AUTH_STATE_ERROR;
    return ctx->state;
}



/* ============================================================================
 * User Database Management
 * ============================================================================ */

/**
 * @brief Add a user record to the manager's internal user database.
 *
 * Duplicates all string fields. Returns KEEL_ERR_ALREADY_EXISTS if a user
 * with the same username is already registered.
 *
 * @param mgr  Authentication manager.
 * @param user User record to copy and insert.
 * @return KEEL_OK on success, error code otherwise.
 */
keel_error_t keel_auth_add_user(
    keel_auth_manager_t* mgr,
    const keel_auth_user_t* user)
{
    if (!mgr || !user || !user->username) return KEEL_ERR_INVALID_ARG;
    
    /* Check if user already exists */
    user_entry_t* entry = mgr->users;
    while (entry) {
        if (strcmp(entry->user.username, user->username) == 0) {
            return KEEL_ERR_ALREADY_EXISTS;
        }
        entry = entry->next;
    }
    
    /* Create new entry */
    entry = keel_calloc(1, sizeof(user_entry_t));
    if (!entry) return KEEL_ERR_NOMEM;
    
    entry->user.username = keel_strdup(user->username);
    if (user->password_hash) entry->user.password_hash = keel_strdup(user->password_hash);
    if (user->password_salt) entry->user.password_salt = keel_strdup(user->password_salt);
    entry->user.iterations = user->iterations;
    entry->user.has_scram_keys = user->has_scram_keys;
    if (user->has_scram_keys) {
        memcpy(entry->user.stored_key, user->stored_key, 32);
        memcpy(entry->user.server_key, user->server_key, 32);
    }
    if (user->pool_name) entry->user.pool_name = keel_strdup(user->pool_name);
    if (user->database) entry->user.database = keel_strdup(user->database);
    entry->user.superuser = user->superuser;
    entry->user.can_login = user->can_login;
    
    /* Add to list */
    entry->next = mgr->users;
    mgr->users = entry;
    mgr->user_count++;
    
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "Added user: %s (scram_keys=%d)",
                  user->username, user->has_scram_keys);
    
    return KEEL_OK;
}

/**
 * @brief Look up a user by username.
 *
 * Uses the manager's external lookup callback if one has been set, otherwise
 * searches the internal linked-list database.
 *
 * @param mgr      Authentication manager.
 * @param username Username to search for.
 * @param user_out Output: pointer to the matching user record (not a copy).
 * @return KEEL_OK if found, KEEL_ERR_NOT_FOUND otherwise.
 */
keel_error_t keel_auth_lookup_user(
    keel_auth_manager_t* mgr,
    const char* username,
    const keel_auth_user_t** user_out)
{
    if (!mgr || !username || !user_out) return KEEL_ERR_INVALID_ARG;
    
    if (mgr->user_lookup) {
        return mgr->user_lookup(username, user_out, mgr->user_lookup_data);
    }
    
    return default_user_lookup(username, user_out, mgr);
}

/**
 * @brief Load users from a pgbouncer-compatible userlist file.
 *
 * Each non-blank, non-comment line must have the form:
 *   "username" "password_or_hash"
 *
 * Lines beginning with '#' and blank lines are skipped.
 * Quotes around both fields are mandatory; everything else on the line after
 * the second closing quote is ignored.
 *
 * Password formats recognised:
 *  - SCRAM-SHA-256$iter:salt:stored_key:server_key  (stored verbatim as hash)
 *  - md5<hex32>                                      (stored verbatim as hash)
 *  - anything else                                   (stored as plain text; the
 *                                                    SCRAM/MD5 provider hashes
 *                                                    it on first verification)
 *
 * @param mgr      Auth manager to populate.
 * @param filepath Path to the userlist file.
 * @return KEEL_OK on success, KEEL_ERR_IO if the file cannot be opened.
 */
keel_error_t keel_auth_load_userlist(
    keel_auth_manager_t* mgr,
    const char* filepath)
{
    if (!mgr || !filepath || !filepath[0]) return KEEL_ERR_INVALID_ARG;

    FILE* f = fopen(filepath, "r");
    if (!f) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "auth: failed to open userlist file '%s': %s",
            filepath, strerror(errno));
        return KEEL_ERR_IO;
    }

    char line[2048];
    int  loaded = 0;

    while (fgets(line, sizeof(line), f)) {
        /* Strip trailing newline */
        size_t llen = strlen(line);
        while (llen > 0 && (line[llen-1] == '\n' || line[llen-1] == '\r'))
            line[--llen] = '\0';

        /* Skip blank lines and comments */
        const char* p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (!*p || *p == '#') continue;

        /* Parse first quoted token (username) */
        if (*p != '"') continue;
        p++;
        const char* ustart = p;
        while (*p && *p != '"') p++;
        if (!*p) continue;
        size_t ulen = (size_t)(p - ustart);
        p++;                          /* skip closing '"' */

        /* Skip whitespace between tokens */
        while (*p == ' ' || *p == '\t') p++;
        if (*p != '"') continue;
        p++;
        const char* pwstart = p;
        while (*p && *p != '"') p++;
        if (!*p) continue;
        size_t pwlen = (size_t)(p - pwstart);

        if (ulen == 0 || pwlen == 0) continue;

        char username[256];
        char password[1024];
        if (ulen  >= sizeof(username))  ulen  = sizeof(username)  - 1;
        if (pwlen >= sizeof(password))  pwlen = sizeof(password)  - 1;
        memcpy(username, ustart,  ulen);  username[ulen]  = '\0';
        memcpy(password, pwstart, pwlen); password[pwlen] = '\0';

        keel_auth_user_t u = {0};
        char* parsed_salt = NULL;
        u.username      = username;
        u.password_hash = password;
        u.can_login     = true;

        /* If the userlist contains a pre-hashed SCRAM verifier, parse it
         * so the SCRAM provider uses stored_key/server_key directly. */
        if (strncmp(password, "SCRAM-SHA-256$", 14) == 0) {
            if (keel_auth_scram_parse_hash(password,
                                           &parsed_salt,
                                           &u.iterations,
                                           u.stored_key,
                                           u.server_key) == KEEL_OK) {
                u.has_scram_keys = true;
                u.password_salt = parsed_salt;
            } else {
                KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                    "auth: invalid SCRAM verifier for user '%s' in '%s'; treating as plaintext",
                    username, filepath);
            }
        }

        keel_error_t err = keel_auth_add_user(mgr, &u);
        if (err == KEEL_OK) {
            loaded++;
        } else if (err != KEEL_ERR_ALREADY_EXISTS) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                "auth: failed to add user '%s' from userlist (err=%d)", username, (int)err);
        }

        if (parsed_salt) {
            keel_free(parsed_salt);
            parsed_salt = NULL;
        }
    }

    fclose(f);
    KEEL_LOG_INFO(KEEL_LOG_CAT_AUTH,
        "auth: loaded %d user(s) from '%s'", loaded, filepath);
    return KEEL_OK;
}

/* ============================================================================
 * Password Hashing Utilities
 * ============================================================================ */

/**
 * @brief Hash a plaintext password using SCRAM-SHA-256 / PBKDF2.
 *
 * Generates a random 16-byte salt, runs PBKDF2-HMAC-SHA-256 for @p iterations
 * rounds, then derives and encodes StoredKey and ServerKey.
 *
 * Output format: @c "SCRAM-SHA-256$<iterations>:<salt_b64>:<stored_b64>:<server_b64>"
 *
 * @param password    Plaintext password to hash.
 * @param iterations  PBKDF2 iteration count; clamped to at least 4096.
 * @param hash_out    Output: heap-allocated hash string (caller must free).
 * @return KEEL_OK on success, error code otherwise.
 */
keel_error_t keel_auth_scram_hash_password(
    const char* password,
    int iterations,
    char** hash_out)
{
#ifndef KEEL_HAS_OPENSSL
    (void)password; (void)iterations; (void)hash_out;
    return KEEL_ERR_NOT_SUPPORTED;
#else
    if (!password || !hash_out) return KEEL_ERR_INVALID_ARG;
    if (iterations < SCRAM_DEFAULT_ITERATIONS) iterations = SCRAM_DEFAULT_ITERATIONS;
    
    /* Generate random salt */
    uint8_t salt[16];
    if (RAND_bytes(salt, sizeof(salt)) != 1) {
        return KEEL_ERR_AUTH;
    }
    
    /* Compute salted password */
    uint8_t salted_password[SCRAM_SHA256_DIGEST_LEN];
    if (!pbkdf2_sha256(password, salt, sizeof(salt), iterations, salted_password)) {
        return KEEL_ERR_AUTH;
    }
    
    /* Compute ClientKey = HMAC(SaltedPassword, "Client Key") */
    uint8_t client_key[SCRAM_SHA256_DIGEST_LEN];
    if (!hmac_sha256(salted_password, SCRAM_SHA256_DIGEST_LEN,
                     (const uint8_t*)"Client Key", 10, client_key)) {
        return KEEL_ERR_AUTH;
    }
    
    /* StoredKey = H(ClientKey) */
    uint8_t stored_key[SCRAM_SHA256_DIGEST_LEN];
    if (!sha256_hash(client_key, SCRAM_SHA256_DIGEST_LEN, stored_key)) {
        return KEEL_ERR_AUTH;
    }
    
    /* ServerKey = HMAC(SaltedPassword, "Server Key") */
    uint8_t server_key[SCRAM_SHA256_DIGEST_LEN];
    if (!hmac_sha256(salted_password, SCRAM_SHA256_DIGEST_LEN,
                     (const uint8_t*)"Server Key", 10, server_key)) {
        return KEEL_ERR_AUTH;
    }
    
    /* Encode to string format: SCRAM-SHA-256$iterations:salt:stored_key:server_key */
    char* salt_b64 = base64_encode(salt, sizeof(salt));
    char* stored_b64 = base64_encode(stored_key, SCRAM_SHA256_DIGEST_LEN);
    char* server_b64 = base64_encode(server_key, SCRAM_SHA256_DIGEST_LEN);
    
    if (!salt_b64 || !stored_b64 || !server_b64) {
        keel_free(salt_b64);
        keel_free(stored_b64);
        keel_free(server_b64);
        return KEEL_ERR_NOMEM;
    }
    
    size_t len = strlen("SCRAM-SHA-256$") + 10 + 1 + strlen(salt_b64) + 1 + 
                 strlen(stored_b64) + 1 + strlen(server_b64) + 1;
    char* hash = keel_malloc(len);
    if (!hash) {
        keel_free(salt_b64);
        keel_free(stored_b64);
        keel_free(server_b64);
        return KEEL_ERR_NOMEM;
    }
    
    snprintf(hash, len, "SCRAM-SHA-256$%d:%s$%s:%s",
             iterations, salt_b64, stored_b64, server_b64);
    
    keel_free(salt_b64);
    keel_free(stored_b64);
    keel_free(server_b64);
    
    *hash_out = hash;
    return KEEL_OK;
#endif
}

/**
 * @brief Parse a SCRAM-SHA-256 hash string into its component parts.
 *
 * Expects the format produced by keel_auth_scram_hash_password():
 * @c "SCRAM-SHA-256$<iterations>:<salt_b64>:<stored_b64>:<server_b64>"
 *
 * @param hash        Hash string to parse.
 * @param salt_out    Output: heap-allocated Base64 salt string (caller must free).
 * @param iterations  Output: iteration count.
 * @param stored_key  Output: 32-byte StoredKey.
 * @param server_key  Output: 32-byte ServerKey.
 * @return KEEL_OK on success, KEEL_ERR_INVALID_ARG on malformed input.
 */
keel_error_t keel_auth_scram_parse_hash(
    const char* hash,
    char** salt_out,
    int* iterations,
    uint8_t stored_key[32],
    uint8_t server_key[32])
{
#ifndef KEEL_HAS_OPENSSL
    (void)hash; (void)salt_out; (void)iterations; (void)stored_key; (void)server_key;
    return KEEL_ERR_NOT_SUPPORTED;
#else
    if (!hash) return KEEL_ERR_INVALID_ARG;
    
    /* Parse format: SCRAM-SHA-256$iterations:salt:stored_key:server_key */
    if (strncmp(hash, "SCRAM-SHA-256$", 14) != 0) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    const char* p = hash + 14;
    
    /* Parse iterations */
    char* colon = strchr(p, ':');
    if (!colon) return KEEL_ERR_INVALID_ARG;
    
    *iterations = atoi(p);
    p = colon + 1;
    
    /* Parse salt.  The salt/stored_key separator is '$' in the
     * PostgreSQL-standard format (SCRAM-SHA-256$iter:salt$stored:server)
     * but was ':' in older Keel builds.  Accept either so existing
     * userlist files keep working while new hashes match PostgreSQL. */
    colon = strchr(p, '$');
    if (!colon) colon = strchr(p, ':');
    if (!colon) return KEEL_ERR_INVALID_ARG;
    
    *salt_out = keel_strndup(p, (size_t)(colon - p));
    p = colon + 1;
    
    /* Parse stored_key */
    colon = strchr(p, ':');
    if (!colon) {
        keel_free(*salt_out);
        *salt_out = NULL;
        return KEEL_ERR_INVALID_ARG;
    }
    
    char* stored_b64 = keel_strndup(p, (size_t)(colon - p));
    size_t stored_len;
    uint8_t* stored = base64_decode(stored_b64, &stored_len);
    keel_free(stored_b64);
    
    if (!stored || stored_len != 32) {
        keel_free(stored);
        keel_free(*salt_out);
        *salt_out = NULL;
        return KEEL_ERR_INVALID_ARG;
    }
    memcpy(stored_key, stored, 32);
    keel_free(stored);
    p = colon + 1;
    
    /* Parse server_key */
    size_t server_len;
    uint8_t* server = base64_decode(p, &server_len);
    if (!server || server_len != 32) {
        keel_free(server);
        keel_free(*salt_out);
        *salt_out = NULL;
        return KEEL_ERR_INVALID_ARG;
    }
    memcpy(server_key, server, 32);
    keel_free(server);
    
    return KEEL_OK;
#endif
}

/* ============================================================================
 * Certificate Identity Provider
 * ============================================================================
 *
 * Authenticates clients by extracting the Common Name (CN) from their TLS
 * client certificate. No password challenge is issued. The CN is mapped
 * directly to a pool username. Requires TLS client-cert verification to be
 * enabled on the frontend listener.
 * ============================================================================ */

/** @brief Return the cert provider name. */
static const char* cert_name(void) { return "cert"; }
/** @brief Return the cert auth method enum. */
static keel_auth_method_t cert_method(void) { return KEEL_AUTH_CERTIFICATE; }
/** @brief No-op init. */
static keel_error_t cert_init(keel_auth_provider_t* p, const void* c) { (void)p; (void)c; return KEEL_OK; }
/** @brief No-op destroy. */
static void cert_destroy(keel_auth_provider_t* p) { (void)p; }

/**
 * @brief Start certificate identity auth.
 *
 * Immediately succeeds: the peer cert CN is taken as the username without
 * issuing any additional challenge. The caller is responsible for verifying
 * that a valid peer cert was presented before calling this provider.
 */
static keel_error_t cert_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
    (void)user;

    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;

    ctx->provider = provider;
    ctx->username = keel_strdup(username);
    if (!ctx->username) { keel_free(ctx); return KEEL_ERR_NOMEM; }
    /* Certificate identity: the TLS layer already authenticated the client.
     * No further challenge is required. */
    ctx->state = KEEL_AUTH_STATE_SUCCESS;
    *ctx_out = ctx;
    return KEEL_OK;
}

/**
 * @brief `keel_auth_provider_ops_t::process` for the certificate provider.
 *
 * @param ctx  Authentication context (state must already be SUCCESS).
 * @param d    Ignored (no additional data is needed after TLS verification).
 * @param l    Ignored.
 * @return Current authentication state (always `KEEL_AUTH_STATE_SUCCESS` for
 *         a valid certificate).
 *
 * Notes:
 * - Certificate authentication is fully decided during `cert_start()`.  This
 *   function is a no-op pass-through that simply returns the already-set
 *   state.
 */
static keel_auth_state_t cert_process(keel_auth_context_t* ctx, const void* d, size_t l) {
    (void)d; (void)l;
    return ctx->state;
}

/**
 * @brief `keel_auth_provider_ops_t::get_message` for the certificate provider.
 *
 * @param ctx  Authentication context.
 * @param m    Unused output.
 * @param l    Unused output.
 * @param t    Unused output.
 * @return Always `KEEL_ERR_NOT_FOUND` because the certificate provider sends
 *         no challenge messages; TLS mutual authentication is handled at the
 *         transport layer before this provider is invoked.
 */
static keel_error_t cert_get_message(keel_auth_context_t* ctx, void** m, size_t* l, int* t) {
    (void)ctx; (void)m; (void)l; (void)t;
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief `keel_auth_provider_ops_t::free_context` for the certificate provider.
 *
 * @param ctx  Authentication context to destroy.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Frees `ctx->username`, `ctx->error_message`, and `ctx` itself.
 * - No provider-specific `auth_data` is allocated by the cert provider, so
 *   no additional cleanup is required.
 */
static void cert_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t cert_ops = {
    .name = cert_name,
    .method = cert_method,
    .init = cert_init,
    .destroy = cert_destroy,
    .start = cert_start,
    .process = cert_process,
    .get_message = cert_get_message,
    .free_context = cert_free_context,
};

/**
 * @brief Return the vtable for the TLS client-certificate authentication provider.
 *
 * @return Pointer to the static `cert_ops` vtable.  The pointer is valid for
 *         the lifetime of the process.
 *
 * Notes:
 * - Certificate authentication relies entirely on TLS mutual authentication
 *   having been performed at the transport layer.  This provider simply marks
 *   the connection as authenticated without issuing any PostgreSQL challenge
 *   message.
 * - Suitable for private networks where clients present client certificates;
 *   NOT a substitute for password-based auth in shared environments.
 */
const keel_auth_provider_ops_t* keel_auth_cert_ops(void) { return &cert_ops; }

/* ============================================================================
 * LDAP Authentication Provider
 * ============================================================================
 *
 * Two-phase: first binds with the service account to search for the user DN,
 * then re-binds with the user DN and the client-supplied password to verify.
 * Falls back to direct-bind (uid=<user>,<dn_suffix>) when no base_dn search
 * is configured.
 *
 * Threading model
 * ---------------
 * LDAP binds are synchronous (ldap_sasl_bind_s may block for seconds).
 * To avoid stalling the io_uring worker thread, ldap_process() offloads the
 * actual bind to a per-provider thread pool of LDAP_POOL_THREADS threads.
 * A per-connection eventfd (EFD_NONBLOCK|EFD_CLOEXEC) is written by the
 * pool thread once the bind completes; the engine arms a reactor recv on
 * that fd and calls keel_engine_flow_resume_auth() when it fires.
 *
 * Result cache
 * ------------
 * Each provider holds a fixed-size LRU cache of LDAP_CACHE_SIZE entries.
 * The cache key is SHA-256(username ":" password).  Cache TTL is
 * LDAP_CACHE_TTL_S seconds.  A mutex protects the cache; the hot path is
 * a simple linear scan over LDAP_CACHE_SIZE entries (default 256) which is
 * fast in L1.
 *
 * Compile-time guard: KEEL_HAS_LDAP (defined when -lldap is found).
 * When absent the provider is still registered but always rejects with a
 * clear log message.
 * ============================================================================ */

#ifdef KEEL_HAS_LDAP
#include <ldap.h>
#endif

#define LDAP_POOL_THREADS  4
#define LDAP_CACHE_SIZE    256
#define LDAP_CACHE_TTL_S   300

/* ----------------------------------------------------------------------------
 * Cache entry
 * -------------------------------------------------------------------------- */
typedef struct {
    uint8_t           key[32];      /* SHA-256(username ":" password) */
    keel_auth_state_t result;       /* SUCCESS or FAILED */
    int64_t           expires_ns;   /* monotonic ns; 0 = empty slot */
} ldap_cache_entry_t;

/* ----------------------------------------------------------------------------
 * Async work item (heap-allocated, one per in-flight bind)
 * -------------------------------------------------------------------------- */
typedef struct ldap_work_item ldap_work_item_t;
struct ldap_work_item {
    keel_auth_context_t* ctx;           /* auth context to update on completion */
    char                 username[256]; /* username (for DN search) */
    char                 user_dn[1024]; /* resolved DN (empty = search required) */
    char                 password[512]; /* client password (cleared after use) */
    int                  notify_fd;     /* eventfd to write 1 when done */
    uint8_t              cache_key[32]; /* cache key to store result under */
    ldap_work_item_t*    next;
};

/* ----------------------------------------------------------------------------
 * Provider data — config + thread pool + cache
 * -------------------------------------------------------------------------- */
typedef struct {
    /* Configuration (deep-copied from keel_auth_ldap_config_t) */
    char* ldap_url;
    char* base_dn;
    char* bind_dn;
    char* bind_password;
    char* search_filter;
    char* dn_suffix;
    bool  start_tls;
    bool  tls_reqcert;
    int   timeout_s;

    /* Thread pool */
    pthread_t         threads[LDAP_POOL_THREADS];
    pthread_mutex_t   pool_mu;
    pthread_cond_t    pool_cv;
    ldap_work_item_t* queue_head;
    ldap_work_item_t* queue_tail;
    bool              pool_shutdown;

    /* Result cache */
    ldap_cache_entry_t cache[LDAP_CACHE_SIZE];
    pthread_mutex_t    cache_mu;
    int                cache_hand;   /* clock-hand for eviction */
} ldap_provider_data_t;

/* ----------------------------------------------------------------------------
 * Per-connection auth context
 * -------------------------------------------------------------------------- */
struct ldap_ctx {
    char*  username;
    char*  password;         /* Received from client; cleared after bind */
    void*  pending_message;
    size_t pending_len;
    int    pending_type;     /* 3 = CleartextPassword */
    int    notify_fd;        /* eventfd; -1 before async dispatch */
    _Atomic keel_auth_state_t async_result;  /* written by pool thread */
    uint8_t cache_key[32];   /* SHA-256(username ":" password) */
};
typedef struct ldap_ctx ldap_ctx_t;

/**
 * @brief Return the notify eventfd for a VERIFY-state auth context.
 *
 * Both the LDAP and PAM providers set state=VERIFY when they dispatch an
 * asynchronous authentication to a worker thread, and store the eventfd in
 * ctx->verify_fd.  All other providers leave it as -1.
 */
int keel_auth_get_verify_fd(keel_auth_context_t* ctx) {
    if (!ctx || ctx->state != KEEL_AUTH_STATE_VERIFY) return -1;
    return ctx->verify_fd;
}

/* ----------------------------------------------------------------------------
 * Monotonic clock helper
 * -------------------------------------------------------------------------- */
static int64_t ldap_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ----------------------------------------------------------------------------
 * Cache helpers
 * -------------------------------------------------------------------------- */

/** Compute the cache key SHA-256(username ":" password).
 *  When KEEL_HAS_OPENSSL is not defined we fall back to a simple XOR
 *  fingerprint — the cache is a best-effort optimisation, not security. */
static void ldap_cache_keygen(const char* username, const char* password,
                               uint8_t key[32])
{
#ifdef KEEL_HAS_OPENSSL
    unsigned int md_len = 32;
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (mdctx) {
        EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(mdctx, username, strlen(username));
        EVP_DigestUpdate(mdctx, ":", 1);
        EVP_DigestUpdate(mdctx, password, strlen(password));
        EVP_DigestFinal_ex(mdctx, key, &md_len);
        EVP_MD_CTX_free(mdctx);
    } else {
        memset(key, 0, 32);
    }
#else
    /* Fallback: key = first 32 bytes of username+password XOR-folded */
    memset(key, 0, 32);
    size_t ulen = strlen(username), plen = strlen(password);
    for (size_t i = 0; i < ulen; i++) key[i % 32] ^= (uint8_t)username[i];
    for (size_t i = 0; i < plen; i++) key[(ulen + i) % 32] ^= (uint8_t)password[i];
#endif
}

/** Probe the cache.  Returns SUCCESS/FAILED on hit, INIT (==0) on miss. */
static keel_auth_state_t ldap_cache_get(ldap_provider_data_t* d,
                                         const uint8_t key[32])
{
    int64_t now = ldap_now_ns();
    pthread_mutex_lock(&d->cache_mu);
    for (int i = 0; i < LDAP_CACHE_SIZE; i++) {
        ldap_cache_entry_t* e = &d->cache[i];
        if (e->expires_ns == 0) continue;
        if (memcmp(e->key, key, 32) != 0) continue;
        if (now > e->expires_ns) {
            e->expires_ns = 0; /* expired */
            break;
        }
        keel_auth_state_t r = e->result;
        pthread_mutex_unlock(&d->cache_mu);
        return r;
    }
    pthread_mutex_unlock(&d->cache_mu);
    return KEEL_AUTH_STATE_INIT;
}

/** Store a result in the cache, evicting the oldest entry if full. */
static void ldap_cache_put(ldap_provider_data_t* d,
                            const uint8_t key[32],
                            keel_auth_state_t result)
{
    int64_t expire = ldap_now_ns() + (int64_t)LDAP_CACHE_TTL_S * 1000000000LL;
    pthread_mutex_lock(&d->cache_mu);
    /* Look for an empty slot first */
    int victim = -1;
    for (int i = 0; i < LDAP_CACHE_SIZE; i++) {
        if (d->cache[i].expires_ns == 0) { victim = i; break; }
    }
    if (victim < 0) {
        /* All slots occupied — use clock-hand eviction */
        victim = d->cache_hand;
        d->cache_hand = (d->cache_hand + 1) % LDAP_CACHE_SIZE;
    }
    memcpy(d->cache[victim].key, key, 32);
    d->cache[victim].result     = result;
    d->cache[victim].expires_ns = expire;
    pthread_mutex_unlock(&d->cache_mu);
}

/* ----------------------------------------------------------------------------
 * Core synchronous LDAP bind (runs on a pool thread)
 * -------------------------------------------------------------------------- */
#ifdef KEEL_HAS_LDAP
static keel_auth_state_t ldap_do_bind(ldap_provider_data_t* d,
                                       const char* user_dn,
                                       const char* password)
{
    LDAP* ld = NULL;
    int rc = ldap_initialize(&ld, d->ldap_url);
    if (rc != LDAP_SUCCESS) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "LDAP user bind init failed: %s", ldap_err2string(rc));
        return KEEL_AUTH_STATE_ERROR;
    }
    int protocol = LDAP_VERSION3;
    ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &protocol);
    /* Suppress canonical-name (reverse-DNS) lookups that can stall for
     * seconds on networks without properly configured reverse zones. */
#ifdef LDAP_OPT_NOCANON
    ldap_set_option(ld, LDAP_OPT_NOCANON, LDAP_OPT_ON);
#endif
    struct timeval tv = { .tv_sec = d->timeout_s, .tv_usec = 0 };
    ldap_set_option(ld, LDAP_OPT_NETWORK_TIMEOUT, &tv);
    ldap_set_option(ld, LDAP_OPT_TIMEOUT, &tv);
    if (d->start_tls) ldap_start_tls_s(ld, NULL, NULL);

    struct berval creds;
    creds.bv_val = (char*)password;
    creds.bv_len = strlen(password);
    rc = ldap_sasl_bind_s(ld, user_dn, LDAP_SASL_SIMPLE, &creds, NULL, NULL, NULL);
    ldap_unbind_ext_s(ld, NULL, NULL);

    if (rc == LDAP_SUCCESS)            return KEEL_AUTH_STATE_SUCCESS;
    if (rc == LDAP_INVALID_CREDENTIALS) return KEEL_AUTH_STATE_FAILED;
    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH, "LDAP bind error: %s", ldap_err2string(rc));
    return KEEL_AUTH_STATE_ERROR;
}

/** Resolve user_dn via service-account search.  Returns true on success. */
static bool ldap_search_dn(ldap_provider_data_t* d,
                            const char* username,
                            char* dn_buf, size_t dn_cap)
{
    LDAP* ld = NULL;
    int rc = ldap_initialize(&ld, d->ldap_url);
    if (rc != LDAP_SUCCESS) return false;

    int protocol = LDAP_VERSION3;
    ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &protocol);
#ifdef LDAP_OPT_NOCANON
    ldap_set_option(ld, LDAP_OPT_NOCANON, LDAP_OPT_ON);
#endif
    struct timeval tv = { .tv_sec = d->timeout_s, .tv_usec = 0 };
    ldap_set_option(ld, LDAP_OPT_NETWORK_TIMEOUT, &tv);
    ldap_set_option(ld, LDAP_OPT_TIMEOUT, &tv);

    if (d->start_tls) {
        rc = ldap_start_tls_s(ld, NULL, NULL);
        if (rc != LDAP_SUCCESS) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                "LDAP STARTTLS (search) failed: %s", ldap_err2string(rc));
            ldap_unbind_ext_s(ld, NULL, NULL);
            return false;
        }
    }

    struct berval svc;
    svc.bv_val = d->bind_password ? (char*)d->bind_password : (char*)"";
    svc.bv_len = d->bind_password ? strlen(d->bind_password) : 0;
    rc = ldap_sasl_bind_s(ld, d->bind_dn, LDAP_SASL_SIMPLE, &svc, NULL, NULL, NULL);
    if (rc != LDAP_SUCCESS) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "LDAP service bind failed: %s", ldap_err2string(rc));
        ldap_unbind_ext_s(ld, NULL, NULL);
        return false;
    }

    /* Build LDAP search filter: replace first %u or %s placeholder with the
     * username.  Using manual substitution avoids snprintf treating %u as an
     * unsigned integer format specifier and also prevents format-string
     * injection if the username contains '%'. */
    char filter_buf[512];
    {
        const char* tmpl = d->search_filter ? d->search_filter : "(uid=%s)";
        char* out = filter_buf;
        const char* out_end = filter_buf + sizeof(filter_buf) - 1;
        const char* p = tmpl;
        bool replaced = false;
        while (*p && out < out_end) {
            if (!replaced && p[0] == '%' && (p[1] == 'u' || p[1] == 's')) {
                const char* u = username;
                while (*u && out < out_end) *out++ = *u++;
                p += 2;
                replaced = true;
            } else {
                *out++ = *p++;
            }
        }
        *out = '\0';
    }

    char* attrs[] = { LDAP_NO_ATTRS, NULL };
    LDAPMessage* result = NULL;
    rc = ldap_search_ext_s(ld, d->base_dn, LDAP_SCOPE_SUBTREE,
                            filter_buf, attrs, 0, NULL, NULL, &tv, 1, &result);
    if (rc != LDAP_SUCCESS || !result) {
        if (result) ldap_msgfree(result);
        ldap_unbind_ext_s(ld, NULL, NULL);
        return false;
    }

    LDAPMessage* entry = ldap_first_entry(ld, result);
    if (!entry) {
        ldap_msgfree(result);
        ldap_unbind_ext_s(ld, NULL, NULL);
        return false;
    }

    char* found = ldap_get_dn(ld, entry);
    bool ok = false;
    if (found) {
        snprintf(dn_buf, dn_cap, "%s", found);
        ldap_memfree(found);
        ok = true;
    }
    ldap_msgfree(result);
    ldap_unbind_ext_s(ld, NULL, NULL);
    return ok;
}
#endif /* KEEL_HAS_LDAP */

/* ----------------------------------------------------------------------------
 * Thread pool worker
 * -------------------------------------------------------------------------- */
static void* ldap_pool_worker(void* arg) {
    ldap_provider_data_t* d = arg;
    for (;;) {
        pthread_mutex_lock(&d->pool_mu);
        while (!d->queue_head && !d->pool_shutdown)
            pthread_cond_wait(&d->pool_cv, &d->pool_mu);
        if (d->pool_shutdown && !d->queue_head) {
            pthread_mutex_unlock(&d->pool_mu);
            return NULL;
        }
        ldap_work_item_t* item = d->queue_head;
        d->queue_head = item->next;
        if (!d->queue_head) d->queue_tail = NULL;
        pthread_mutex_unlock(&d->pool_mu);

        /* Perform LDAP DN search (if needed) + bind — all on thread pool,
         * never blocking the io reactor thread. */
#ifdef KEEL_HAS_LDAP
        keel_auth_state_t result;
        const char* user_dn = item->user_dn;

        /* If user_dn is empty, we need to search for it first. */
        if (user_dn[0] == '\0') {
            char dn_buf[1024] = {0};
            if (!ldap_search_dn(d, item->username, dn_buf, sizeof(dn_buf))) {
                result = KEEL_AUTH_STATE_FAILED;
            } else {
                result = ldap_do_bind(d, dn_buf, item->password);
            }
        } else {
            result = ldap_do_bind(d, user_dn, item->password);
        }
#else
        keel_auth_state_t result = KEEL_AUTH_STATE_ERROR;
#endif
        /* Clear password from memory ASAP */
        memset(item->password, 0, sizeof(item->password));

        /* Write result into auth context */
        keel_auth_context_t* ctx = item->ctx;
        ldap_ctx_t* lctx = ctx->auth_data;
        if (lctx) {
            atomic_store_explicit(&lctx->async_result, result, memory_order_release);
            /* Update state on ctx so postgres_flow can read it */
            ctx->state = result;
            if (result == KEEL_AUTH_STATE_FAILED) {
                /* Error message is set here; postgres_flow picks it up */
                if (!ctx->error_message)
                    ctx->error_message = keel_strdup("password authentication failed");
            }
        }

        /* Cache the result */
        ldap_cache_put(d, item->cache_key, result);

        /* Wake the reactor — write 1 to the eventfd */
        uint64_t v = 1;
        if (write(item->notify_fd, &v, sizeof(v)) < 0) {
            /* Client may have disconnected; log but don't crash */
            KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                "LDAP pool: eventfd write failed: %s", strerror(errno));
        }
        /* Note: notify_fd is owned by the auth context (closed in ldap_free_context) */
        keel_free(item);
    }
}

static const char* ldap_name(void) { return "ldap"; }
static keel_auth_method_t ldap_method(void) { return KEEL_AUTH_LDAP; }

/* ----------------------------------------------------------------------------
 * Provider lifecycle
 * -------------------------------------------------------------------------- */

static keel_error_t ldap_init(keel_auth_provider_t* provider, const void* cfg_raw) {
    const keel_auth_ldap_config_t* cfg = cfg_raw;
    if (!cfg || !cfg->url) return KEEL_ERR_INVALID_ARG;

    ldap_provider_data_t* d = keel_calloc(1, sizeof(ldap_provider_data_t));
    if (!d) return KEEL_ERR_NOMEM;

    d->ldap_url       = cfg->url           ? keel_strdup(cfg->url)             : NULL;
    d->base_dn        = cfg->base_dn       ? keel_strdup(cfg->base_dn)         : NULL;
    d->bind_dn        = cfg->bind_dn       ? keel_strdup(cfg->bind_dn)         : NULL;
    d->bind_password  = cfg->bind_password ? keel_strdup(cfg->bind_password)   : NULL;
    d->search_filter  = cfg->search_filter ? keel_strdup(cfg->search_filter)   : NULL;
    d->dn_suffix      = cfg->dn_suffix     ? keel_strdup(cfg->dn_suffix)       : NULL;
    d->start_tls      = cfg->start_tls;
    d->tls_reqcert    = cfg->tls_reqcert;
    d->timeout_s      = cfg->timeout_s > 0 ? cfg->timeout_s : 5;
    d->cache_hand     = 0;

    pthread_mutex_init(&d->pool_mu,  NULL);
    pthread_cond_init (&d->pool_cv,  NULL);
    pthread_mutex_init(&d->cache_mu, NULL);

    /* Start thread pool */
    for (int i = 0; i < LDAP_POOL_THREADS; i++) {
        if (pthread_create(&d->threads[i], NULL, ldap_pool_worker, d) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "LDAP: failed to start pool thread %d: %s", i, strerror(errno));
            /* Mark remaining threads as invalid */
            for (int j = i; j < LDAP_POOL_THREADS; j++)
                d->threads[j] = 0;
            break;
        }
    }

    provider->provider_data = d;
    return KEEL_OK;
}

static void keel_ldap_destroy(keel_auth_provider_t* p) {
    if (!p || !p->provider_data) return;
    ldap_provider_data_t* d = p->provider_data;

    /* Signal all pool threads to exit */
    pthread_mutex_lock(&d->pool_mu);
    d->pool_shutdown = true;
    pthread_cond_broadcast(&d->pool_cv);
    pthread_mutex_unlock(&d->pool_mu);

    /* Join threads */
    for (int i = 0; i < LDAP_POOL_THREADS; i++) {
        if (d->threads[i]) {
            pthread_join(d->threads[i], NULL);
            d->threads[i] = 0;
        }
    }

    /* Drain any remaining queue items (threads have exited) */
    ldap_work_item_t* it = d->queue_head;
    while (it) {
        ldap_work_item_t* nx = it->next;
        memset(it->password, 0, sizeof(it->password));
        keel_free(it);
        it = nx;
    }

    pthread_mutex_destroy(&d->pool_mu);
    pthread_cond_destroy(&d->pool_cv);
    pthread_mutex_destroy(&d->cache_mu);

    keel_free(d->ldap_url);
    keel_free(d->base_dn);
    keel_free(d->bind_dn);
    keel_free(d->bind_password);
    keel_free(d->search_filter);
    keel_free(d->dn_suffix);
    keel_free(d);
    p->provider_data = NULL;
}

/* ----------------------------------------------------------------------------
 * Per-connection start
 * -------------------------------------------------------------------------- */
static keel_error_t ldap_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
    (void)user;

    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;

    ldap_ctx_t* lctx = keel_calloc(1, sizeof(ldap_ctx_t));
    if (!lctx) { keel_free(ctx); return KEEL_ERR_NOMEM; }

    ctx->provider  = provider;
    ctx->username  = keel_strdup(username);
    if (!ctx->username) { keel_free(lctx); keel_free(ctx); return KEEL_ERR_NOMEM; }

    lctx->username = keel_strdup(username);
    if (!lctx->username) {
        keel_free(lctx); keel_free(ctx->username); keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    lctx->notify_fd = -1;
    atomic_init(&lctx->async_result, KEEL_AUTH_STATE_INIT);

    /* Build AuthenticationCleartextPassword message (type 3, int32(3)) */
    lctx->pending_message = keel_malloc(4);
    if (!lctx->pending_message) {
        keel_free(lctx->username); keel_free(lctx);
        keel_free(ctx->username); keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    uint8_t* msg = lctx->pending_message;
    msg[0] = 0; msg[1] = 0; msg[2] = 0; msg[3] = 3;
    lctx->pending_len  = 4;
    lctx->pending_type = 3;

    ctx->auth_data  = lctx;
    ctx->state      = KEEL_AUTH_STATE_CHALLENGE;
    ctx->verify_fd  = -1;
    *ctx_out = ctx;
    return KEEL_OK;
}

/* ----------------------------------------------------------------------------
 * Password processing — dispatches to thread pool
 * -------------------------------------------------------------------------- */
static keel_auth_state_t ldap_process(
    keel_auth_context_t* ctx,
    const void* data,
    size_t len)
{
    ldap_ctx_t* lctx = ctx->auth_data;
    if (!lctx) { ctx->state = KEEL_AUTH_STATE_ERROR; return KEEL_AUTH_STATE_ERROR; }

    ldap_provider_data_t* d = ctx->provider->provider_data;
    if (!d) { ctx->state = KEEL_AUTH_STATE_ERROR; return KEEL_AUTH_STATE_ERROR; }

    const char* password = (const char*)data;
    if (!password || len == 0) {
        ctx->state = KEEL_AUTH_STATE_FAILED;
        ctx->error_message = keel_strdup("Empty password");
        return KEEL_AUTH_STATE_FAILED;
    }

    /* Save password for async path */
    if (lctx->password) { memset(lctx->password, 0, strlen(lctx->password)); keel_free(lctx->password); }
    lctx->password = keel_strndup(password, len);
    if (!lctx->password) { ctx->state = KEEL_AUTH_STATE_ERROR; return KEEL_AUTH_STATE_ERROR; }

#ifndef KEEL_HAS_LDAP
    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
        "LDAP auth requested but keel was built without libldap");
    ctx->state = KEEL_AUTH_STATE_ERROR;
    ctx->error_message = keel_strdup("LDAP support not compiled in");
    return KEEL_AUTH_STATE_ERROR;
#else
    /* ---- Check result cache ---- */
    ldap_cache_keygen(lctx->username, lctx->password, lctx->cache_key);
    keel_auth_state_t cached = ldap_cache_get(d, lctx->cache_key);
    if (cached == KEEL_AUTH_STATE_SUCCESS || cached == KEEL_AUTH_STATE_FAILED) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_AUTH,
            "LDAP cache hit for user %s: %s",
            lctx->username, cached == KEEL_AUTH_STATE_SUCCESS ? "pass" : "fail");
        if (cached == KEEL_AUTH_STATE_FAILED)
            ctx->error_message = keel_strdup("password authentication failed");
        ctx->state = cached;
        return cached;
    }

    /* ---- Resolve user DN ---- */
    /* If dn_suffix is configured, build the DN deterministically (no network).
     * If only base_dn is configured, leave user_dn_buf empty — the thread
     * pool worker will call ldap_search_dn so the io thread is never blocked
     * by a synchronous LDAP network call. */
    char user_dn_buf[1024] = {0};

    if (d->dn_suffix) {
        snprintf(user_dn_buf, sizeof(user_dn_buf), "uid=%s,%s",
                 lctx->username, d->dn_suffix);
    } else if (!d->base_dn) {
        ctx->state = KEEL_AUTH_STATE_ERROR;
        ctx->error_message = keel_strdup("LDAP: no dn_suffix or base_dn configured");
        return KEEL_AUTH_STATE_ERROR;
    }
    /* user_dn_buf is either a full DN (dn_suffix path) or empty (base_dn
     * path — thread pool will do ldap_search_dn). */

    /* ---- Create eventfd for reactor notification ---- */
    int efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) {
        /* Fall back to synchronous search+bind on eventfd failure */
        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
            "LDAP: eventfd failed (%s), falling back to sync bind", strerror(errno));
        const char* sync_dn = user_dn_buf[0] ? user_dn_buf : NULL;
        char dn_fallback[1024] = {0};
        if (!sync_dn) {
            if (!ldap_search_dn(d, lctx->username, dn_fallback, sizeof(dn_fallback))) {
                ctx->state = KEEL_AUTH_STATE_FAILED;
                ctx->error_message = keel_strdup("User not found in LDAP");
                return KEEL_AUTH_STATE_FAILED;
            }
            sync_dn = dn_fallback;
        }
        keel_auth_state_t r = ldap_do_bind(d, sync_dn, lctx->password);
        ldap_cache_put(d, lctx->cache_key, r);
        if (r == KEEL_AUTH_STATE_FAILED)
            ctx->error_message = keel_strdup("password authentication failed");
        ctx->state = r;
        return r;
    }
    lctx->notify_fd = efd;
    ctx->verify_fd  = efd;

    /* ---- Build work item and enqueue ---- */
    ldap_work_item_t* item = keel_calloc(1, sizeof(ldap_work_item_t));
    if (!item) {
        close(efd);
        lctx->notify_fd = -1;
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }
    item->ctx       = ctx;
    item->notify_fd = efd;
    memcpy(item->cache_key, lctx->cache_key, 32);
    snprintf(item->username, sizeof(item->username), "%s", lctx->username);
    snprintf(item->user_dn,  sizeof(item->user_dn),  "%s", user_dn_buf);
    snprintf(item->password, sizeof(item->password), "%.*s", (int)len, password);
    item->next = NULL;

    pthread_mutex_lock(&d->pool_mu);
    if (d->pool_shutdown) {
        pthread_mutex_unlock(&d->pool_mu);
        close(efd);
        lctx->notify_fd = -1;
        keel_free(item);
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }
    if (d->queue_tail) d->queue_tail->next = item;
    else               d->queue_head = item;
    d->queue_tail = item;
    pthread_mutex_unlock(&d->pool_mu);
    pthread_cond_signal(&d->pool_cv);

    /* Return VERIFY — reactor will arm the eventfd and call resume_auth() */
    atomic_store_explicit(&lctx->async_result, KEEL_AUTH_STATE_VERIFY,
                          memory_order_release);
    ctx->state = KEEL_AUTH_STATE_VERIFY;
    return KEEL_AUTH_STATE_VERIFY;
#endif /* KEEL_HAS_LDAP */
}

static keel_error_t ldap_get_message(keel_auth_context_t* ctx, void** m, size_t* l, int* t) {
    ldap_ctx_t* lctx = ctx->auth_data;
    if (!lctx || !lctx->pending_message) return KEEL_ERR_NOT_FOUND;
    *m = lctx->pending_message;
    *l = lctx->pending_len;
    *t = lctx->pending_type;
    lctx->pending_message = NULL;
    lctx->pending_len     = 0;
    return KEEL_OK;
}

static void ldap_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    ldap_ctx_t* lctx = ctx->auth_data;
    if (lctx) {
        if (lctx->password) {
            memset(lctx->password, 0, strlen(lctx->password));
            keel_free(lctx->password);
        }
        if (lctx->notify_fd >= 0) {
            close(lctx->notify_fd);
            lctx->notify_fd = -1;
        }
        keel_free(lctx->username);
        keel_free(lctx->pending_message);
        keel_free(lctx);
    }
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t ldap_auth_ops = {
    .name         = ldap_name,
    .method       = ldap_method,
    .init         = ldap_init,
    .destroy      = keel_ldap_destroy,
    .start        = ldap_start,
    .process      = ldap_process,
    .get_message  = ldap_get_message,
    .free_context = ldap_free_context,
};

const keel_auth_provider_ops_t* keel_auth_ldap_ops(void) { return &ldap_auth_ops; }

/**
 * @brief Initialize an LDAP authentication provider with the given configuration.
 *
 * @param provider  Provider struct to initialize.
 * @param config    Configuration block; `config->url` is required.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` for `NULL` arguments,
 *         or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Notes:
 * - Convenience wrapper that sets `provider->ops` and calls `ldap_init()`.
 * - Equivalent to creating a provider instance and calling `ops->init()`
 *   manually.
 */
keel_error_t keel_auth_ldap_init(keel_auth_provider_t* provider,
                                  const keel_auth_ldap_config_t* config) {
    if (!provider || !config) return KEEL_ERR_INVALID_ARG;
    provider->ops = keel_auth_ldap_ops();
    return ldap_init(provider, config);
}

/* ============================================================================
 * PAM Authentication Provider
 * ============================================================================
 *
 * Uses libpam to authenticate clients. Sends AuthenticationCleartextPassword
 * (type 3) to elicit a clear-text password, then runs pam_authenticate().
 *
 * Threading model
 * ---------------
 * pam_authenticate() is synchronous and may block for seconds (e.g. when
 * talking to a remote RADIUS or LDAP back-end via PAM modules).  To avoid
 * stalling the io_uring worker, pam_auth_process() offloads the call to a
 * per-provider thread pool of PAM_POOL_THREADS threads.  An eventfd written
 * by the pool thread wakes the reactor, which calls
 * keel_engine_flow_resume_auth() to complete the handshake.
 *
 * Compile-time guard: KEEL_HAS_PAM.
 * ============================================================================ */

#ifdef KEEL_HAS_PAM
#include <security/pam_appl.h>
#endif

#define PAM_POOL_THREADS 2

/* Async work item — one per in-flight pam_authenticate() call */
typedef struct pam_work_item pam_work_item_t;
struct pam_work_item {
    keel_auth_context_t* ctx;
    char                 username[256];
    char                 service[128];
    char                 password[512];   /* cleared immediately after use */
    int                  notify_fd;       /* eventfd to signal on completion */
    pam_work_item_t*     next;
};

typedef struct {
    char*            service_name;
    /* Thread pool */
    pthread_t        threads[PAM_POOL_THREADS];
    pthread_mutex_t  pool_mu;
    pthread_cond_t   pool_cv;
    pam_work_item_t* queue_head;
    pam_work_item_t* queue_tail;
    bool             pool_shutdown;
} pam_provider_data_t;

/* pam_ctx_t is forward-declared before keel_auth_get_verify_fd; full definition here. */
struct pam_ctx {
    char*  username;
    char*  password;                        /* cleared after async dispatch */
    void*  pending_message;
    size_t pending_len;
    int    pending_type;
    int    notify_fd;                       /* eventfd; -1 before dispatch */
    _Atomic keel_auth_state_t async_result; /* written by pool thread */
};
typedef struct pam_ctx pam_ctx_t;

#ifdef KEEL_HAS_PAM
/* PAM conversation function: supplies the stored password on request */
static int pam_conv_fn(int num_msg, const struct pam_message** msg,
                        struct pam_response** resp, void* appdata_ptr) {
    const char* password = (const char*)appdata_ptr;
    /* NOTE: The PAM library owns and frees these allocations via system free().
     * Using keel_malloc/keel_strdup here would cause a mismatch because PAM
     * calls the system free() on *resp after the conversation function returns.
     * This is an intentional system-API boundary. NOLINT(keel-syscall) */
    struct pam_response* r = calloc((size_t)num_msg, sizeof(struct pam_response)); /* NOLINT(keel-syscall) */
    if (!r) return PAM_BUF_ERR;
    for (int i = 0; i < num_msg; i++) {
        if (msg[i]->msg_style == PAM_PROMPT_ECHO_OFF ||
            msg[i]->msg_style == PAM_PROMPT_ECHO_ON) {
            r[i].resp = strdup(password ? password : ""); /* NOLINT(keel-syscall) */
            r[i].resp_retcode = 0;
        }
    }
    *resp = r;
    return PAM_SUCCESS;
}

/* Thread pool worker: runs pam_authenticate() and signals the reactor via eventfd. */
static void* pam_pool_worker(void* arg) {
    pam_provider_data_t* d = arg;
    for (;;) {
        pthread_mutex_lock(&d->pool_mu);
        while (!d->queue_head && !d->pool_shutdown)
            pthread_cond_wait(&d->pool_cv, &d->pool_mu);
        if (d->pool_shutdown && !d->queue_head) {
            pthread_mutex_unlock(&d->pool_mu);
            return NULL;
        }
        pam_work_item_t* item = d->queue_head;
        d->queue_head = item->next;
        if (!d->queue_head) d->queue_tail = NULL;
        pthread_mutex_unlock(&d->pool_mu);

        /* Run PAM authenticate (may block for seconds) */
        struct pam_conv conv = {
            .conv        = pam_conv_fn,
            .appdata_ptr = item->password,
        };
        pam_handle_t* pamh = NULL;
        keel_auth_state_t result = KEEL_AUTH_STATE_ERROR;
        int ret = pam_start(item->service, item->username, &conv, &pamh);
        if (ret == PAM_SUCCESS) {
            ret = pam_authenticate(pamh, PAM_SILENT | PAM_DISALLOW_NULL_AUTHTOK);
            pam_end(pamh, ret);
            result = (ret == PAM_SUCCESS) ? KEEL_AUTH_STATE_SUCCESS
                                          : KEEL_AUTH_STATE_FAILED;
        } else {
            if (pamh) pam_end(pamh, ret);
        }

        /* Clear password from memory ASAP */
        memset(item->password, 0, sizeof(item->password));

        /* Write result into auth context */
        keel_auth_context_t* ctx = item->ctx;
        pam_ctx_t* pctx = ctx->auth_data;
        if (pctx) {
            atomic_store_explicit(&pctx->async_result, result, memory_order_release);
            ctx->state = result;
            if (result == KEEL_AUTH_STATE_FAILED && !ctx->error_message)
                ctx->error_message = keel_strdup("password authentication failed");
        }

        /* Wake the reactor */
        uint64_t v = 1;
        if (write(item->notify_fd, &v, sizeof(v)) < 0) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                "PAM pool: eventfd write failed: %s", strerror(errno));
        }
        keel_free(item);
    }
}
#endif /* KEEL_HAS_PAM */

static const char* pam_auth_name(void) { return "pam"; }
static keel_auth_method_t pam_auth_method(void) { return KEEL_AUTH_PAM; }

/**
 * @brief `keel_auth_provider_ops_t::init` for the PAM provider.
 *
 * @param provider  Provider struct to populate.
 * @param cfg_raw   Pointer to a `keel_auth_pam_config_t`.  May be `NULL`, in
 *                  which case `"keel"` is used as the PAM service name.
 * @return `KEEL_OK` on success or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Notes:
 * - Deep-copies `cfg->service_name` into heap storage.
 * - When `cfg` is `NULL` or `cfg->service_name` is `NULL`, the default
 *   service name `"keel"` is used.  This corresponds to the PAM service
 *   configuration file at `/etc/pam.d/keel`.
 */
static keel_error_t pam_auth_init(keel_auth_provider_t* provider, const void* cfg_raw) {
    const keel_auth_pam_config_t* cfg = cfg_raw;
    pam_provider_data_t* d = keel_calloc(1, sizeof(pam_provider_data_t));
    if (!d) return KEEL_ERR_NOMEM;
    const char* svc = (cfg && cfg->service_name) ? cfg->service_name : "keel";
    d->service_name = keel_strdup(svc);
    if (!d->service_name) { keel_free(d); return KEEL_ERR_NOMEM; }

    pthread_mutex_init(&d->pool_mu, NULL);
    pthread_cond_init(&d->pool_cv,  NULL);

#ifdef KEEL_HAS_PAM
    for (int i = 0; i < PAM_POOL_THREADS; i++) {
        if (pthread_create(&d->threads[i], NULL, pam_pool_worker, d) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "PAM: failed to start pool thread %d: %s", i, strerror(errno));
            for (int j = i; j < PAM_POOL_THREADS; j++)
                d->threads[j] = 0;
            break;
        }
    }
#endif

    provider->provider_data = d;
    return KEEL_OK;
}

/**
 * @brief `keel_auth_provider_ops_t::destroy` for the PAM provider.
 *
 * @param p  Provider to tear down.  Passing `NULL` is safe.
 * @return Nothing.
 */
static void pam_auth_destroy(keel_auth_provider_t* p) {
    if (!p || !p->provider_data) return;
    pam_provider_data_t* d = p->provider_data;

    pthread_mutex_lock(&d->pool_mu);
    d->pool_shutdown = true;
    pthread_cond_broadcast(&d->pool_cv);
    pthread_mutex_unlock(&d->pool_mu);

    for (int i = 0; i < PAM_POOL_THREADS; i++) {
        if (d->threads[i]) {
            pthread_join(d->threads[i], NULL);
            d->threads[i] = 0;
        }
    }

    /* Drain remaining queued items (threads have exited) */
    pam_work_item_t* it = d->queue_head;
    while (it) {
        pam_work_item_t* nx = it->next;
        memset(it->password, 0, sizeof(it->password));
        keel_free(it);
        it = nx;
    }

    pthread_mutex_destroy(&d->pool_mu);
    pthread_cond_destroy(&d->pool_cv);
    keel_free(d->service_name);
    keel_free(d);
    p->provider_data = NULL;
}

/**
 * @brief `keel_auth_provider_ops_t::start` for the PAM provider.
 *
 * @param provider   PAM provider instance.
 * @param username   Client-supplied user name.
 * @param user       Pre-fetched user record (not used by the PAM provider).
 * @param[out] ctx_out  Receives the newly allocated authentication context.
 * @return `KEEL_OK` on success or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Behavior:
 * - Allocates a `keel_auth_context_t` and a `pam_ctx_t` holding the user name.
 * - Pre-populates a pending `AuthenticationCleartextPassword` message
 *   (PostgreSQL protocol type 3, 4-byte big-endian integer value 3) to
 *   request a clear-text password from the client.
 * - Sets `ctx->state = KEEL_AUTH_STATE_CHALLENGE`; the actual
 *   `pam_authenticate()` call happens in `pam_auth_process()`.
 */
static keel_error_t pam_auth_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
    (void)user;

    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;

    pam_ctx_t* pctx = keel_calloc(1, sizeof(pam_ctx_t));
    if (!pctx) { keel_free(ctx); return KEEL_ERR_NOMEM; }

    ctx->provider = provider;
    ctx->username = keel_strdup(username);
    if (!ctx->username) { keel_free(pctx); keel_free(ctx); return KEEL_ERR_NOMEM; }

    pctx->username = keel_strdup(username);
    if (!pctx->username) { keel_free(pctx); keel_free(ctx->username); keel_free(ctx); return KEEL_ERR_NOMEM; }

    /* AuthenticationCleartextPassword = type 3 */
    pctx->pending_message = keel_malloc(4);
    if (!pctx->pending_message) {
        keel_free(pctx->username); keel_free(pctx); keel_free(ctx->username); keel_free(ctx);
        return KEEL_ERR_NOMEM;
    }
    uint8_t* m = pctx->pending_message;
    m[0] = 0; m[1] = 0; m[2] = 0; m[3] = 3;
    pctx->pending_len  = 4;
    pctx->pending_type = 3;
    pctx->notify_fd    = -1;
    atomic_init(&pctx->async_result, KEEL_AUTH_STATE_INIT);

    ctx->auth_data  = pctx;
    ctx->state      = KEEL_AUTH_STATE_CHALLENGE;
    ctx->verify_fd  = -1;
    *ctx_out = ctx;
    return KEEL_OK;
}

/**
 * @brief `keel_auth_provider_ops_t::process` for the PAM provider.
 *
 * @param ctx   Authentication context populated by `pam_auth_start()`.
 * @param data  Clear-text password bytes received from the client.
 * @param len   Length of `data` in bytes.
 * @return `KEEL_AUTH_STATE_SUCCESS` when PAM authenticates successfully,
 *         `KEEL_AUTH_STATE_FAILED` for bad credentials,
 *         `KEEL_AUTH_STATE_ERROR` for PAM internal errors or when PAM
 *         support was not compiled in.
 *
 * Notes:
 * - Uses a `pam_conv` callback that supplies the stored password to any
 *   `PAM_PROMPT_ECHO_OFF` or `PAM_PROMPT_ECHO_ON` challenges (see
 *   `pam_conv_fn()`).
 * - The password is copied into `pctx->password` before the PAM call so
 *   the `pam_conv` callback pointer remains valid throughout `pam_authenticate()`.
 * - When `KEEL_HAS_PAM` is not defined, all requests are rejected with
 *   `KEEL_AUTH_STATE_ERROR`.
 */
static keel_auth_state_t pam_auth_process(
    keel_auth_context_t* ctx,
    const void* data,
    size_t len)
{
    pam_ctx_t* pctx = ctx->auth_data;
    if (!pctx) { ctx->state = KEEL_AUTH_STATE_ERROR; return KEEL_AUTH_STATE_ERROR; }

    pam_provider_data_t* d = ctx->provider->provider_data;

    const char* password = (const char*)data;
    if (!password || len == 0) {
        ctx->state = KEEL_AUTH_STATE_FAILED;
        ctx->error_message = keel_strdup("Empty password");
        return KEEL_AUTH_STATE_FAILED;
    }

#ifndef KEEL_HAS_PAM
    (void)d;
    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH, "PAM auth requested but keel was built without libpam");
    ctx->state = KEEL_AUTH_STATE_ERROR;
    ctx->error_message = keel_strdup("PAM support not compiled in");
    return KEEL_AUTH_STATE_ERROR;
#else
    const char* service = (d && d->service_name) ? d->service_name : "keel";

    /* Save password for the pool thread */
    if (pctx->password) {
        memset(pctx->password, 0, strlen(pctx->password));
        keel_free(pctx->password);
    }
    pctx->password = keel_strndup(password, len);
    if (!pctx->password) {
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }

    /* Create eventfd for reactor notification */
    int efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) {
        /* Fall back to synchronous authenticate when eventfd is unavailable */
        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
            "PAM: eventfd failed (%s), falling back to sync authenticate",
            strerror(errno));
        struct pam_conv conv = { .conv = pam_conv_fn, .appdata_ptr = pctx->password };
        pam_handle_t* pamh = NULL;
        int ret = pam_start(service, pctx->username, &conv, &pamh);
        if (ret != PAM_SUCCESS) {
            if (pamh) pam_end(pamh, ret);
            ctx->state = KEEL_AUTH_STATE_ERROR;
            ctx->error_message = keel_strdup("PAM session error");
            return KEEL_AUTH_STATE_ERROR;
        }
        ret = pam_authenticate(pamh, PAM_SILENT | PAM_DISALLOW_NULL_AUTHTOK);
        pam_end(pamh, ret);
        keel_auth_state_t r = (ret == PAM_SUCCESS) ? KEEL_AUTH_STATE_SUCCESS
                                                   : KEEL_AUTH_STATE_FAILED;
        if (r == KEEL_AUTH_STATE_FAILED)
            ctx->error_message = keel_strdup("password authentication failed");
        ctx->state = r;
        return r;
    }
    pctx->notify_fd = efd;
    ctx->verify_fd  = efd;

    /* Build work item and enqueue for a pool thread */
    if (!item) {
        close(efd);
        pctx->notify_fd = -1;
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }
    item->ctx       = ctx;
    item->notify_fd = efd;
    snprintf(item->username, sizeof(item->username), "%s", pctx->username);
    snprintf(item->service,  sizeof(item->service),  "%s", service);
    snprintf(item->password, sizeof(item->password), "%.*s", (int)len, password);
    item->next = NULL;

    pthread_mutex_lock(&d->pool_mu);
    if (d->pool_shutdown) {
        pthread_mutex_unlock(&d->pool_mu);
        close(efd);
        pctx->notify_fd = -1;
        keel_free(item);
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }
    if (d->queue_tail) d->queue_tail->next = item;
    else               d->queue_head = item;
    d->queue_tail = item;
    pthread_mutex_unlock(&d->pool_mu);
    pthread_cond_signal(&d->pool_cv);

    /* Return VERIFY — reactor will arm the eventfd and call resume_auth() */
    atomic_store_explicit(&pctx->async_result, KEEL_AUTH_STATE_VERIFY,
                          memory_order_release);
    ctx->state = KEEL_AUTH_STATE_VERIFY;
    return KEEL_AUTH_STATE_VERIFY;
#endif /* KEEL_HAS_PAM */
}

/**
 * @brief `keel_auth_provider_ops_t::get_message` for the PAM provider.
 *
 * @param ctx  Authentication context.
 * @param[out] m  Receives a pointer to the pending challenge message.
 * @param[out] l  Receives the message length in bytes.
 * @param[out] t  Receives the PostgreSQL message type code (3 = CleartextPassword).
 * @return `KEEL_OK` when a pending message is available,
 *         `KEEL_ERR_NOT_FOUND` otherwise.
 *
 * Notes:
 * - Transfers ownership of the pending buffer; sets `pctx->pending_message`
 *   to `NULL` after the first consumption.
 */
static keel_error_t pam_auth_get_message(keel_auth_context_t* ctx, void** m, size_t* l, int* t) {
    pam_ctx_t* pctx = ctx->auth_data;
    if (!pctx || !pctx->pending_message) return KEEL_ERR_NOT_FOUND;
    *m = pctx->pending_message;
    *l = pctx->pending_len;
    *t = pctx->pending_type;
    pctx->pending_message = NULL;
    pctx->pending_len = 0;
    return KEEL_OK;
}

/**
 * @brief `keel_auth_provider_ops_t::free_context` for the PAM provider.
 *
 * @param ctx  Authentication context to destroy.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Frees the inner `pam_ctx_t` (including `username`, `password`, and
 *   any unconsumed pending message) then the outer `keel_auth_context_t`.
 */
static void pam_auth_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    pam_ctx_t* pctx = ctx->auth_data;
    if (pctx) {
        if (pctx->notify_fd >= 0) {
            close(pctx->notify_fd);
            pctx->notify_fd = -1;
        }
        if (pctx->password) {
            memset(pctx->password, 0, strlen(pctx->password));
            keel_free(pctx->password);
        }
        keel_free(pctx->username);
        keel_free(pctx->pending_message);
        keel_free(pctx);
    }
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t pam_auth_ops_tbl = {
    .name = pam_auth_name,
    .method = pam_auth_method,
    .init = pam_auth_init,
    .destroy = pam_auth_destroy,
    .start = pam_auth_start,
    .process = pam_auth_process,
    .get_message = pam_auth_get_message,
    .free_context = pam_auth_free_context,
};

/**
 * @brief Return the vtable for the PAM authentication provider.
 *
 * @return Pointer to the static `pam_auth_ops_tbl` vtable.
 *
 * Notes:
 * - When `KEEL_HAS_PAM` is not compiled in, the vtable is still valid but
 *   `pam_auth_process()` always returns `KEEL_AUTH_STATE_ERROR`.
 */
const keel_auth_provider_ops_t* keel_auth_pam_ops(void) { return &pam_auth_ops_tbl; }

/**
 * @brief Initialize a PAM authentication provider with the given configuration.
 *
 * @param provider  Provider struct to initialize.
 * @param config    Optional PAM configuration block.  When `NULL`, the default
 *                  PAM service name `"keel"` is used.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` if `provider` is
 *         `NULL`, or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Notes:
 * - Convenience wrapper that sets `provider->ops` and calls `pam_auth_init()`.
 */
keel_error_t keel_auth_pam_init(keel_auth_provider_t* provider,
                                 const keel_auth_pam_config_t* config) {
    if (!provider) return KEEL_ERR_INVALID_ARG;
    provider->ops = keel_auth_pam_ops();
    return pam_auth_init(provider, config);
}

/* ============================================================================
 * auth_query Provider
 * ============================================================================
 *
 * Fetches the stored password hash from a backend SQL query, then delegates
 * authentication to the configured upstream provider (SCRAM or MD5).
 *
 * The query runs synchronously via a dedicated libpq connection.  This path
 * is NOT on the hot data path — it is used only for authentication and the
 * connection is short-lived.
 *
 * Compile-time guard: KEEL_HAS_LIBPQ (defined when libpq-fe.h is found).
 * ============================================================================ */

#ifdef KEEL_HAS_LIBPQ
#include <libpq-fe.h>
#endif

typedef struct {
    char* query;
    char* conn_string;
    keel_auth_method_t upstream_method;
    int   timeout_s;
} auth_query_provider_data_t;

typedef struct {
    char* username;
    /* Delegate context created after password fetch */
    keel_auth_context_t* delegate_ctx;
    /* Cleartext challenge stage */
    void* pending_message;
    size_t pending_len;
    int    pending_type;
    bool   fetched;          /* True once we ran the query */
    /* Cached fetched hash for retry */
    char* fetched_hash;
    /* Provider for delegated auth */
    keel_auth_provider_t  delegate_provider;
    const keel_auth_provider_ops_t* upstream_ops;
} auth_query_ctx_t;

static const char* aq_name(void)   { return "auth_query"; }
static keel_auth_method_t aq_method(void) { return KEEL_AUTH_SCRAM_SHA_256; }

static keel_error_t aq_init(keel_auth_provider_t* provider, const void* cfg_raw) {
    const keel_auth_query_config_t* cfg = cfg_raw;
    if (!cfg || !cfg->query || !cfg->conn_string) return KEEL_ERR_INVALID_ARG;

    auth_query_provider_data_t* d = keel_calloc(1, sizeof(auth_query_provider_data_t));
    if (!d) return KEEL_ERR_NOMEM;
    d->query       = keel_strdup(cfg->query);
    d->conn_string = keel_strdup(cfg->conn_string);
    d->upstream_method = cfg->upstream_method ? cfg->upstream_method : KEEL_AUTH_SCRAM_SHA_256;
    d->timeout_s   = cfg->timeout_s > 0 ? cfg->timeout_s : 3;

    if (!d->query || !d->conn_string) {
        keel_free(d->query); keel_free(d->conn_string); keel_free(d);
        return KEEL_ERR_NOMEM;
    }
    provider->provider_data = d;
    return KEEL_OK;
}

static void aq_destroy(keel_auth_provider_t* p) {
    if (!p || !p->provider_data) return;
    auth_query_provider_data_t* d = p->provider_data;
    keel_free(d->query);
    keel_free(d->conn_string);
    keel_free(d);
    p->provider_data = NULL;
}

static keel_error_t aq_start(
    keel_auth_provider_t* provider,
    const char* username,
    const keel_auth_user_t* user,
    keel_auth_context_t** ctx_out)
{
    (void)user;

    auth_query_provider_data_t* d = provider->provider_data;
    if (!d) return KEEL_ERR_INVALID_ARG;

    keel_auth_context_t* ctx = keel_calloc(1, sizeof(keel_auth_context_t));
    if (!ctx) return KEEL_ERR_NOMEM;

    auth_query_ctx_t* aqctx = keel_calloc(1, sizeof(auth_query_ctx_t));
    if (!aqctx) { keel_free(ctx); return KEEL_ERR_NOMEM; }

    ctx->provider = provider;
    ctx->username = keel_strdup(username);
    if (!ctx->username) { keel_free(aqctx); keel_free(ctx); return KEEL_ERR_NOMEM; }

    aqctx->username = keel_strdup(username);
    if (!aqctx->username) { keel_free(aqctx); keel_free(ctx->username); keel_free(ctx); return KEEL_ERR_NOMEM; }

    /* Select upstream ops */
    switch (d->upstream_method) {
    case KEEL_AUTH_MD5:           aqctx->upstream_ops = keel_auth_md5_ops();         break;
    default:                      aqctx->upstream_ops = keel_auth_scram_sha256_ops(); break;
    }

    ctx->auth_data = aqctx;

    /* Fetch the stored password hash from the backend now (blocking) */
    keel_auth_user_t fetched_user = {0};
    char* hash = NULL;

#ifdef KEEL_HAS_LIBPQ
    {
        /* Build connection string with connect_timeout */
        char cs_buf[1024];
        snprintf(cs_buf, sizeof(cs_buf), "%s connect_timeout=%d",
                 d->conn_string, d->timeout_s);

        PGconn* conn = PQconnectdb(cs_buf);
        if (PQstatus(conn) != CONNECTION_OK) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "auth_query: backend connect failed: %s", PQerrorMessage(conn));
            PQfinish(conn);
            /* Fail auth rather than crashing */
            ctx->state = KEEL_AUTH_STATE_FAILED;
            ctx->error_message = keel_strdup("auth_query: backend unavailable");
            *ctx_out = ctx;
            return KEEL_OK; /* Return OK so the failure propagates via state */
        }

        const char* params[1] = { username };
        PGresult* res = PQexecParams(conn, d->query,
                                      1, NULL, params, NULL, NULL, 0);
        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
            const char* val = PQgetvalue(res, 0, 0);
            hash = val ? keel_strdup(val) : NULL;
        }
        PQclear(res);
        PQfinish(conn);
    }
#else
    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
        "auth_query: built without libpq; cannot fetch password from backend");
    ctx->state = KEEL_AUTH_STATE_FAILED;
    ctx->error_message = keel_strdup("auth_query: libpq not available");
    *ctx_out = ctx;
    return KEEL_OK;
#endif /* KEEL_HAS_LIBPQ */

    if (!hash) {
        ctx->state = KEEL_AUTH_STATE_FAILED;
        ctx->error_message = keel_strdup("password authentication failed");
        *ctx_out = ctx;
        return KEEL_OK;
    }

    aqctx->fetched_hash = hash;

    /* Parse the hash and populate the user record */
    fetched_user.username = (char*)username;
    fetched_user.password_hash = hash;

    /* Parse SCRAM hash if applicable */
    if (strncmp(hash, "SCRAM-SHA-256$", 14) == 0) {
        char* salt_b64 = NULL;
        if (keel_auth_scram_parse_hash(hash, &salt_b64, &fetched_user.iterations,
                                        fetched_user.stored_key, fetched_user.server_key) == KEEL_OK) {
            fetched_user.has_scram_keys = true;
            fetched_user.password_salt = salt_b64;
        }
    }

    /* Delegate start to the upstream provider */
    aqctx->delegate_provider.ops = aqctx->upstream_ops;
    aqctx->delegate_provider.provider_data = NULL;
    aqctx->upstream_ops->init(&aqctx->delegate_provider, NULL);

    keel_error_t err = aqctx->upstream_ops->start(
        &aqctx->delegate_provider, username, &fetched_user, &aqctx->delegate_ctx);

    keel_free(fetched_user.password_salt);

    if (err != KEEL_OK) {
        ctx->state = KEEL_AUTH_STATE_ERROR;
        *ctx_out = ctx;
        return KEEL_OK;
    }

    /* Copy first message from delegate */
    void* msg = NULL; size_t mlen = 0; int mtype = 0;
    if (aqctx->upstream_ops->get_message(aqctx->delegate_ctx, &msg, &mlen, &mtype) == KEEL_OK) {
        aqctx->pending_message = msg;
        aqctx->pending_len     = mlen;
        aqctx->pending_type    = mtype;
    }

    ctx->state = aqctx->delegate_ctx->state;
    *ctx_out = ctx;
    return KEEL_OK;
}

static keel_auth_state_t aq_process(keel_auth_context_t* ctx, const void* data, size_t len) {
    auth_query_ctx_t* aqctx = ctx->auth_data;
    if (!aqctx || !aqctx->delegate_ctx) {
        ctx->state = KEEL_AUTH_STATE_ERROR;
        return KEEL_AUTH_STATE_ERROR;
    }

    keel_auth_state_t s = aqctx->upstream_ops->process(aqctx->delegate_ctx, data, len);
    ctx->state = s;

    /* Forward any new pending message from delegate */
    if (!aqctx->pending_message) {
        void* msg = NULL; size_t mlen = 0; int mtype = 0;
        if (aqctx->upstream_ops->get_message(aqctx->delegate_ctx, &msg, &mlen, &mtype) == KEEL_OK) {
            aqctx->pending_message = msg;
            aqctx->pending_len     = mlen;
            aqctx->pending_type    = mtype;
        }
    }
    return s;
}

static keel_error_t aq_get_message(keel_auth_context_t* ctx, void** m, size_t* l, int* t) {
    auth_query_ctx_t* aqctx = ctx->auth_data;
    if (!aqctx || !aqctx->pending_message) return KEEL_ERR_NOT_FOUND;
    *m = aqctx->pending_message;
    *l = aqctx->pending_len;
    *t = aqctx->pending_type;
    aqctx->pending_message = NULL;
    aqctx->pending_len = 0;
    return KEEL_OK;
}

static void aq_free_context(keel_auth_context_t* ctx) {
    if (!ctx) return;
    auth_query_ctx_t* aqctx = ctx->auth_data;
    if (aqctx) {
        keel_free(aqctx->username);
        keel_free(aqctx->fetched_hash);
        keel_free(aqctx->pending_message);
        if (aqctx->delegate_ctx)
            aqctx->upstream_ops->free_context(aqctx->delegate_ctx);
        aqctx->upstream_ops->destroy(&aqctx->delegate_provider);
        keel_free(aqctx);
    }
    keel_free(ctx->username);
    keel_free(ctx->error_message);
    keel_free(ctx);
}

static const keel_auth_provider_ops_t aq_ops_tbl = {
    .name = aq_name,
    .method = aq_method,
    .init = aq_init,
    .destroy = aq_destroy,
    .start = aq_start,
    .process = aq_process,
    .get_message = aq_get_message,
    .free_context = aq_free_context,
};

const keel_auth_provider_ops_t* keel_auth_query_ops(void) { return &aq_ops_tbl; }

keel_error_t keel_auth_query_init(keel_auth_provider_t* provider,
                                   const keel_auth_query_config_t* config) {
    if (!provider || !config) return KEEL_ERR_INVALID_ARG;
    provider->ops = keel_auth_query_ops();
    return aq_init(provider, config);
}
