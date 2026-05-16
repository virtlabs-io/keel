/**
 * @file cloud_auth.c
 * @brief Cloud-native authentication token providers for backend connections.
 *
 * Implements the keel_cloud_auth_ops_t interface for:
 *   - AWS RDS IAM   (SigV4 pre-signed URL, pure HMAC-SHA-256)
 *   - GCP Cloud SQL (service account file or metadata server)
 *   - Azure AD      (managed identity IMDS endpoint)
 *   - Static        (file or environment variable)
 *
 * Token caching and refresh is handled by the generic cache layer; individual
 * providers are only responsible for fetching a fresh token.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/core/cloud_auth.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/encoding.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#ifdef KEEL_HAS_OPENSSL
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#endif

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

/* ============================================================================
 * Token cache — generic layer shared by all providers
 * ============================================================================ */

/**
 * @brief Initialise a token cache and bind it to a provider.
 *
 * @param cache     Cache structure to initialise (must not be NULL).
 * @param provider  Cloud auth provider that will supply fresh tokens.
 * @param margin_s  Seconds before token expiry at which a refresh is
 *                  triggered; defaults to 60 when <= 0.
 */
void keel_cloud_token_cache_init(keel_cloud_token_cache_t* cache,
                                 keel_cloud_auth_provider_t* provider,
                                 int margin_s) {
    memset(cache, 0, sizeof(*cache));
    cache->provider = provider;
    cache->refresh_margin_s = margin_s > 0 ? margin_s : 60;
}

/**
 * @brief Destroy a token cache and release all associated resources.
 *
 * Zeroes the cached secret before freeing it and calls the provider's
 * destroy callback if one is registered.
 *
 * @param cache  Cache to destroy; a NULL pointer is silently ignored.
 */
void keel_cloud_token_cache_destroy(keel_cloud_token_cache_t* cache) {
    if (!cache) return;
    if (cache->cached_token) {
        /* Zero out the token before freeing */
        memset(cache->cached_token, 0, cache->cached_token_len);
        keel_free(cache->cached_token);
    }
    if (cache->provider && cache->provider->ops && cache->provider->ops->destroy)
        cache->provider->ops->destroy(cache->provider);
    memset(cache, 0, sizeof(*cache));
}

/**
 * @brief Return an authentication password/token for a backend connection.
 *
 * Returns a cached token when still valid, otherwise fetches a fresh one
 * from the bound provider. Falls back to @p static_pw when the cache or
 * provider is unavailable, or when token fetch fails.
 *
 * @param cache      Token cache bound to a provider (may be NULL).
 * @param host       Backend hostname used by some providers (e.g. AWS RDS).
 * @param port       Backend port used by some providers.
 * @param user       Database user; passed to the provider's fetch_token.
 * @param static_pw  Fallback password returned when cloud auth is unavailable.
 * @return           Pointer to a NUL-terminated password/token string.  The
 *                   returned pointer is valid until the next call or cache
 *                   destruction.  Never NULL when @p static_pw is non-NULL.
 */
const char* keel_cloud_auth_get_password(keel_cloud_token_cache_t* cache,
                                         const char* host, uint16_t port,
                                         const char* user,
                                         const char* static_pw) {
    if (!cache || !cache->provider)
        return static_pw;

    time_t now = time(NULL);

    /* Return cached token if still valid */
    if (cache->valid && cache->cached_token) {
        if (cache->expires_at == 0 ||
            now < cache->expires_at - cache->refresh_margin_s) {
            return cache->cached_token;
        }
    }

    /* Fetch fresh token */
    keel_cloud_token_t tok = {0};
    keel_error_t err = cache->provider->ops->fetch_token(
        cache->provider, host, port, user, &tok);

    if (err != KEEL_OK || !tok.token) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth: token fetch failed for %s@%s:%u (error %d)",
                       user ? user : "?", host ? host : "?", port, (int)err);
        /* Fall back to static pw if token fetch fails */
        return static_pw;
    }

    /* Swap cached token */
    if (cache->cached_token) {
        memset(cache->cached_token, 0, cache->cached_token_len);
        keel_free(cache->cached_token);
    }
    cache->cached_token = tok.token;
    cache->cached_token_len = tok.token_len;
    cache->expires_at = tok.expires_at;
    cache->valid = true;

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_AUTH,
                   "cloud_auth: refreshed token for %s@%s:%u (expires=%ld)",
                   user ? user : "?", host ? host : "?", port,
                   (long)tok.expires_at);

    return cache->cached_token;
}

/* ============================================================================
 * AWS RDS IAM token provider — SigV4 pre-signed URL
 *
 * The "token" is a SigV4-signed HTTPS URL of the form:
 *   <host>:<port>/?Action=connect&DBUser=<user>&X-Amz-Algorithm=AWS4-HMAC-SHA256&...
 *
 * This is computed entirely locally using HMAC-SHA-256 — no network I/O.
 * Token lifetime is 15 minutes; we set expiry at 14 minutes.
 * ============================================================================ */

/**
 * @brief Duplicate a C string using the keel allocator.
 *
 * @param s  String to duplicate; NULL is passed through as NULL.
 * @return   Heap-allocated copy of @p s, or NULL if @p s is NULL or
 *           allocation fails.
 */
static char* safe_strdup(const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char* d = keel_malloc(len + 1);
    if (d) { memcpy(d, s, len); d[len] = '\0'; }
    return d;
}

#ifdef KEEL_HAS_OPENSSL

typedef struct aws_iam_provider {
    keel_cloud_auth_provider_t base;
    char* region;
    char* access_key_id;
    char* secret_access_key;
    char* session_token;
} aws_iam_provider_t;

/* hex_encode removed: use keel_hex_encode() from keel/util/encoding.h */

/**
 * @brief HMAC-SHA-256.
 */
static bool hmac_sha256(const uint8_t* key, size_t key_len,
                         const uint8_t* data, size_t data_len,
                         uint8_t out[32]) {
    unsigned int out_len = 32;
    return HMAC(EVP_sha256(), key, (int)key_len, data, data_len,
                out, &out_len) != NULL;
}

/**
 * @brief Compute AWS SigV4 signing key for a given date.
 */
static bool aws_signing_key(const char* secret_key, const char* date_str,
                             const char* region, const char* service,
                             uint8_t out[32]) {
    char prefixed[256];
    snprintf(prefixed, sizeof(prefixed), "AWS4%s", secret_key);

    uint8_t k_date[32];
    if (!hmac_sha256((const uint8_t*)prefixed, strlen(prefixed),
                     (const uint8_t*)date_str, strlen(date_str), k_date))
        return false;

    uint8_t k_region[32];
    if (!hmac_sha256(k_date, 32, (const uint8_t*)region, strlen(region), k_region))
        return false;

    uint8_t k_service[32];
    if (!hmac_sha256(k_region, 32, (const uint8_t*)service, strlen(service), k_service))
        return false;

    return hmac_sha256(k_service, 32, (const uint8_t*)"aws4_request", 12, out);
}

/**
 * @brief URL-encode a string (minimal: only encode non-unreserved chars).
 */
static size_t url_encode(const char* src, char* dst, size_t dst_size) {
    static const char unreserved[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
    size_t pos = 0;
    for (size_t i = 0; src[i] && pos + 4 < dst_size; i++) {
        if (strchr(unreserved, src[i])) {
            dst[pos++] = src[i];
        } else {
            pos += (size_t)snprintf(dst + pos, dst_size - pos, "%%%02X",
                                    (unsigned char)src[i]);
        }
    }
    dst[pos] = '\0';
    return pos;
}

/**
 * @brief Fetch an AWS RDS IAM authentication token (SigV4 pre-signed URL).
 *
 * Constructs a SigV4-signed URL entirely in memory (no network I/O).
 * Credentials are taken from the provider config or from the environment
 * variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
 * AWS_SESSION_TOKEN.
 *
 * @param prov       Provider instance cast to aws_iam_provider_t.
 * @param host       RDS endpoint hostname used in the canonical request.
 * @param port       RDS port used in the canonical request.
 * @param user       Database user embedded in the pre-signed URL.
 * @param token_out  Output token populated on success.
 * @return           KEEL_OK on success, KEEL_ERR_AUTH on credential or
 *                   signing failure, KEEL_ERR_NOMEM on allocation failure.
 */
static keel_error_t aws_iam_fetch_token(keel_cloud_auth_provider_t* prov,
                                         const char* host, uint16_t port,
                                         const char* user,
                                         keel_cloud_token_t* token_out) {
    aws_iam_provider_t* aws = (aws_iam_provider_t*)prov;

    /* Determine credentials: provider config or env */
    const char* access_key = aws->access_key_id;
    const char* secret_key = aws->secret_access_key;
    const char* session_token = aws->session_token;

    if (!access_key) access_key = getenv("AWS_ACCESS_KEY_ID");
    if (!secret_key) secret_key = getenv("AWS_SECRET_ACCESS_KEY");
    if (!session_token) session_token = getenv("AWS_SESSION_TOKEN");

    if (!access_key || !secret_key) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(aws): missing AWS credentials");
        return KEEL_ERR_AUTH;
    }
    if (!aws->region) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(aws): missing region");
        return KEEL_ERR_AUTH;
    }

    /* ISO 8601 timestamps */
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);
    char amz_date[20], date_str[12];
    strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &tm);
    strftime(date_str, sizeof(date_str), "%Y%m%d", &tm);

    const char* service = "rds-db";
    const int token_ttl = 900; /* 15 minutes */

    /* Build canonical query string (parameters sorted alphabetically) */
    char encoded_user[256];
    url_encode(user, encoded_user, sizeof(encoded_user));

    char credential[256];
    snprintf(credential, sizeof(credential), "%s/%s/%s/%s/aws4_request",
             access_key, date_str, aws->region, service);
    char encoded_cred[512];
    url_encode(credential, encoded_cred, sizeof(encoded_cred));

    char canonical_qs[2048];
    snprintf(canonical_qs, sizeof(canonical_qs),
             "Action=connect"
             "&DBUser=%s"
             "&X-Amz-Algorithm=AWS4-HMAC-SHA256"
             "&X-Amz-Credential=%s"
             "&X-Amz-Date=%s"
             "&X-Amz-Expires=%d"
             "%s%s",
             encoded_user,
             encoded_cred,
             amz_date,
             token_ttl,
             session_token ? "&X-Amz-Security-Token=" : "",
             session_token ? session_token : "");

    /* Canonical request */
    /* GET \n / \n <qs> \n host:<host>:<port>\n \n host \n UNSIGNED-PAYLOAD */
    char host_header[256];
    snprintf(host_header, sizeof(host_header), "%s:%u", host, port);

    char canonical_req[4096];
    snprintf(canonical_req, sizeof(canonical_req),
             "GET\n"
             "/\n"
             "%s\n"
             "host:%s\n"
             "\n"
             "host\n"
             "UNSIGNED-PAYLOAD",
             canonical_qs, host_header);

    /* Hash canonical request */
    uint8_t cr_hash[32];
    SHA256((const uint8_t*)canonical_req, strlen(canonical_req), cr_hash);
    char cr_hash_hex[65];
    keel_hex_encode(cr_hash, cr_hash_hex, 32); cr_hash_hex[64] = '\0';

    /* String to sign */
    char string_to_sign[4096];
    snprintf(string_to_sign, sizeof(string_to_sign),
             "AWS4-HMAC-SHA256\n"
             "%s\n"
             "%s/%s/%s/aws4_request\n"
             "%s",
             amz_date, date_str, aws->region, service, cr_hash_hex);

    /* Compute signature */
    uint8_t signing_key[32];
    if (!aws_signing_key(secret_key, date_str, aws->region, service, signing_key))
        return KEEL_ERR_AUTH;

    uint8_t sig[32];
    if (!hmac_sha256(signing_key, 32,
                     (const uint8_t*)string_to_sign, strlen(string_to_sign), sig))
        return KEEL_ERR_AUTH;

    char sig_hex[65];
    keel_hex_encode(sig, sig_hex, 32); sig_hex[64] = '\0';

    /* Assemble final token: <host>:<port>/?<qs>&X-Amz-Signature=<sig> */
    size_t token_size = strlen(host_header) + 3 + strlen(canonical_qs) +
                        25 + 64 + 1;
    char* token = keel_malloc(token_size);
    if (!token) return KEEL_ERR_NOMEM;

    int tlen = snprintf(token, token_size,
                        "%s/?%s&X-Amz-Signature=%s",
                        host_header, canonical_qs, sig_hex);

    token_out->token = token;
    token_out->token_len = (size_t)tlen;
    token_out->expires_at = now + token_ttl - 60; /* Refresh 1 min before expiry */

    return KEEL_OK;
}

/**
 * @brief Destroy an AWS IAM provider and securely wipe credentials.
 *
 * Zeroes all secret key material before freeing to prevent it from
 * lingering in memory.
 *
 * @param prov  Provider instance to destroy (cast to aws_iam_provider_t).
 */
static void aws_iam_destroy(keel_cloud_auth_provider_t* prov) {
    aws_iam_provider_t* aws = (aws_iam_provider_t*)prov;
    keel_free(aws->region);
    if (aws->access_key_id) {
        memset(aws->access_key_id, 0, strlen(aws->access_key_id));
        keel_free(aws->access_key_id);
    }
    if (aws->secret_access_key) {
        memset(aws->secret_access_key, 0, strlen(aws->secret_access_key));
        keel_free(aws->secret_access_key);
    }
    if (aws->session_token) {
        memset(aws->session_token, 0, strlen(aws->session_token));
        keel_free(aws->session_token);
    }
    keel_free(aws);
}

static const keel_cloud_auth_ops_t aws_iam_ops = {
    .name        = "aws-rds-iam",
    .type        = KEEL_CLOUD_AUTH_AWS_IAM,
    .create      = NULL,  /* Uses keel_cloud_auth_aws_create */
    .fetch_token = aws_iam_fetch_token,
    .destroy     = aws_iam_destroy,
};

/**
 * @brief Create an AWS RDS IAM authentication provider.
 *
 * Allocates and initialises a provider that generates SigV4 pre-signed
 * RDS connection URLs.  Credentials may be supplied via @p config or
 * via the standard AWS environment variables.
 *
 * @param out     Receives the newly created provider on success.
 * @param config  AWS region and optional credential overrides; must not
 *                be NULL and must supply a non-NULL region.
 * @return        KEEL_OK on success, KEEL_ERR_INVALID_ARG if required
 *                arguments are missing, KEEL_ERR_NOMEM on allocation
 *                failure.
 */
keel_error_t keel_cloud_auth_aws_create(keel_cloud_auth_provider_t** out,
                                        const keel_cloud_aws_config_t* config) {
    if (!out || !config) return KEEL_ERR_INVALID_ARG;

    aws_iam_provider_t* aws = keel_calloc(1, sizeof(*aws));
    if (!aws) return KEEL_ERR_NOMEM;

    aws->base.ops = &aws_iam_ops;
    aws->base.data = aws;
    aws->region = safe_strdup(config->region);
    aws->access_key_id = safe_strdup(config->access_key_id);
    aws->secret_access_key = safe_strdup(config->secret_access_key);
    aws->session_token = safe_strdup(config->session_token);

    if (!aws->region) {
        aws_iam_destroy(&aws->base);
        return KEEL_ERR_INVALID_ARG;
    }

    *out = &aws->base;
    return KEEL_OK;
}

#else /* !KEEL_HAS_OPENSSL */

/**
 * @brief Stub: AWS RDS IAM provider requires OpenSSL.
 *
 * Always returns KEEL_ERR_AUTH with a log message when the library was
 * compiled without OpenSSL support (KEEL_HAS_OPENSSL not defined).
 *
 * @param out     Unused.
 * @param config  Unused.
 * @return        Always KEEL_ERR_AUTH.
 */
keel_error_t keel_cloud_auth_aws_create(keel_cloud_auth_provider_t** out,
                                        const keel_cloud_aws_config_t* config) {
    (void)out; (void)config;
    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                   "cloud_auth(aws): requires OpenSSL (compile with KEEL_HAS_OPENSSL)");
    return KEEL_ERR_AUTH;
}

#endif /* KEEL_HAS_OPENSSL */

/* ============================================================================
 * GCP Cloud SQL IAM token provider
 *
 * Supports two token acquisition methods:
 * 1. Service account JSON key file → JWT signing → OAuth2 token exchange
 * 2. GCP metadata server (http://metadata.google.internal) for VM/GKE
 * 3. CLOUDSQL_ACCESS_TOKEN_FILE env fallback (external helper)
 *
 * JWT flow (service account key file):
 *   - Parse JSON key: extract client_email and private_key (PEM)
 *   - Build JWT: {"alg":"RS256","typ":"JWT"}.{iss,scope,aud,iat,exp}
 *   - Sign with RSA-SHA-256
 *   - POST to https://oauth2.googleapis.com/token
 *   - Parse access_token from response
 *
 * Metadata server flow (no key file):
 *   - GET http://metadata.google.internal/computeMetadata/v1/instance/
 *         service-accounts/default/token
 *   - Header: Metadata-Flavor: Google
 *   - Parse access_token and expires_in from response JSON
 * ============================================================================ */

/* ---- Base64url encoding for JWT ---- */

static const char b64url_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/**
 * @brief Base64url-encode a byte array into a caller-supplied buffer.
 *
 * Encoding follows RFC 4648 §5 (URL-safe alphabet, no padding).
 *
 * @param in        Input bytes.
 * @param len       Number of input bytes.
 * @param out       Output buffer; NUL-terminated on return.
 * @param out_size  Size of @p out in bytes.
 * @return          Number of characters written (excluding NUL terminator).
 */
static size_t base64url_encode(const uint8_t* in, size_t len,
                                char* out, size_t out_size) {
    size_t pos = 0;
    for (size_t i = 0; i < len && pos + 4 < out_size; ) {
        uint32_t a = in[i++];
        uint32_t b = (i < len) ? in[i++] : 0;
        uint32_t c = (i < len) ? in[i++] : 0;
        uint32_t triple = (a << 16) | (b << 8) | c;

        size_t rem = len - (i - (i < len ? 0 : (i > len ? 2 : 1)));
        (void)rem;
        out[pos++] = b64url_table[(triple >> 18) & 0x3F];
        out[pos++] = b64url_table[(triple >> 12) & 0x3F];
        if (i - 1 < len) out[pos++] = b64url_table[(triple >> 6) & 0x3F];
        if (i     <= len) out[pos++] = b64url_table[triple & 0x3F];
    }
    out[pos] = '\0';
    return pos;
}

/**
 * @brief Base64url-encode a byte array into a newly allocated string.
 *
 * Encoding follows RFC 4648 §5 (URL-safe alphabet, no padding).
 * The caller is responsible for freeing the returned string with keel_free().
 *
 * @param data  Input bytes.
 * @param len   Number of input bytes.
 * @return      Heap-allocated NUL-terminated base64url string, or NULL on
 *              allocation failure.
 */
/* Simpler base64url encode that handles padding correctly */
static char* base64url_encode_alloc(const uint8_t* data, size_t len) {
    size_t out_len = ((len + 2) / 3) * 4 + 1;
    char* out = keel_malloc(out_len);
    if (!out) return NULL;

    size_t pos = 0;
    size_t i = 0;
    while (i < len) {
        uint32_t a = data[i++];
        uint32_t b = (i < len) ? data[i++] : 0;
        uint32_t c = (i < len) ? data[i++] : 0;
        uint32_t triple = (a << 16) | (b << 8) | c;

        out[pos++] = b64url_table[(triple >> 18) & 0x3F];
        out[pos++] = b64url_table[(triple >> 12) & 0x3F];

        size_t mod = (i < len + 2) ? 3 : ((len - (i - 3)) + 2);
        (void)mod;

        /* Only emit 3rd char if we had >= 2 bytes, 4th if >= 3 bytes */
        size_t chunk_start = i - ((i <= len) ? 3 : (len % 3 == 0 ? 3 : len % 3));
        size_t chunk_len = (i <= len) ? 3 : (len - chunk_start);

        if (chunk_len >= 2) out[pos++] = b64url_table[(triple >> 6) & 0x3F];
        if (chunk_len >= 3) out[pos++] = b64url_table[triple & 0x3F];
    }
    /* No padding in base64url */
    out[pos] = '\0';
    return out;
}

/* ---- Simple HTTP GET for metadata endpoints ---- */

/**
 * @brief Perform a plain HTTP/1.0 GET request and return the raw response.
 *
 * Opens a TCP connection to @p host:@p port, sends a minimal GET request
 * with optional extra headers, and reads the full response into @p buf.
 * A 5-second send/receive timeout is applied.
 *
 * @param host          Hostname or IP address to connect to.
 * @param port          TCP port to connect to.
 * @param path          Request-URI (e.g. "/metadata/...").
 * @param extra_headers Additional HTTP headers to include, or NULL.
 * @param buf           Buffer to receive the raw HTTP response.
 * @param bufsz         Size of @p buf in bytes.
 * @return              Total bytes received, or -1 on connection/send error.
 */
static ssize_t cloud_http_get(const char* host, int port, const char* path,
                               const char* extra_headers,
                               char* buf, size_t bufsz) {
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

    struct timeval tv = { 5, 0 }; /* 5 second timeout */
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        freeaddrinfo(res); close(fd); return -1;
    }
    freeaddrinfo(res);

    char req[1024];
    int reqlen = snprintf(req, sizeof(req),
        "GET %s HTTP/1.0\r\n"
        "Host: %s\r\n"
        "%s"
        "Connection: close\r\n"
        "\r\n",
        path, host, extra_headers ? extra_headers : "");

    if (send(fd, req, (size_t)reqlen, MSG_NOSIGNAL) != (ssize_t)reqlen) {
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

    return (ssize_t)total;
}

/* ---- Simple HTTP POST for OAuth2 token exchange ---- */

/**
 * @brief Perform a plain HTTP/1.0 POST request and return the raw response.
 *
 * Opens a TCP connection to @p host:@p port, sends the request headers
 * followed by @p body, and reads the full response into @p buf.  A
 * 10-second send/receive timeout is applied.
 *
 * @param host          Hostname or IP address to connect to.
 * @param port          TCP port to connect to.
 * @param path          Request-URI (e.g. "/token").
 * @param content_type  Value for the Content-Type header.
 * @param body          Request body bytes.
 * @param body_len      Length of @p body in bytes.
 * @param buf           Buffer to receive the raw HTTP response.
 * @param bufsz         Size of @p buf in bytes.
 * @return              Total bytes received, or -1 on connection/send error.
 */
static ssize_t cloud_http_post(const char* host, int port, const char* path,
                                const char* content_type,
                                const char* body, size_t body_len,
                                char* buf, size_t bufsz) {
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

    struct timeval tv = { 10, 0 }; /* 10 second timeout */
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        freeaddrinfo(res); close(fd); return -1;
    }
    freeaddrinfo(res);

    char req[2048];
    int reqlen = snprintf(req, sizeof(req),
        "POST %s HTTP/1.0\r\n"
        "Host: %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        path, host, content_type, body_len);

    if (send(fd, req, (size_t)reqlen, MSG_NOSIGNAL) != (ssize_t)reqlen) {
        close(fd); return -1;
    }
    if (send(fd, body, body_len, MSG_NOSIGNAL) != (ssize_t)body_len) {
        close(fd); return -1;
    }

    size_t total = 0;
    ssize_t rd;
    while (total < bufsz - 1 &&
           (rd = recv(fd, buf + total, bufsz - 1 - total, 0)) > 0) {
        total += (size_t)rd;
    }
    close(fd);
    buf[total] = '\0';

    return (ssize_t)total;
}

/* ---- Tiny JSON helpers ---- */

/**
 * @brief Extract a JSON string value by key from a JSON object literal.
 *
 * Scans @p json for the first occurrence of @p key and copies the
 * associated string value into @p out.  Handles a single level of
 * backslash escaping for \n.
 *
 * @param json    NUL-terminated JSON text to search.
 * @param key     JSON key name (without quotes).
 * @param out     Buffer to receive the extracted value.
 * @param out_sz  Size of @p out in bytes.
 * @return        @p out on success, or NULL if the key is not found or
 *                the value is not a JSON string.
 */
static const char* json_find_string(const char* json, const char* key,
                                     char* out, size_t out_sz) {
    /* Search for "key" : "value" */
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char* p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    while (*p == ' ' || *p == '\t' || *p == ':') p++;
    if (*p != '"') return NULL;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i < out_sz - 1) {
        if (*p == '\\' && *(p + 1)) {
            p++; /* skip escaped chars */
            if (*p == 'n') { out[i++] = '\n'; p++; continue; }
        }
        out[i++] = *p++;
    }
    out[i] = '\0';
    return out;
}

/**
 * @brief Extract a JSON integer (or quoted integer) value by key.
 *
 * @param json  NUL-terminated JSON text to search.
 * @param key   JSON key name (without quotes).
 * @return      Parsed integer value, or -1 if the key is not found.
 */
static int64_t json_find_int(const char* json, const char* key) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char* p = strstr(json, needle);
    if (!p) return -1;
    p += strlen(needle);
    while (*p == ' ' || *p == '\t' || *p == ':') p++;
    if (*p == '"') p++; /* Handle quoted numbers */
    return strtoll(p, NULL, 10);
}

typedef struct gcp_iam_provider {
    keel_cloud_auth_provider_t base;
    char* service_account_file;
    char* client_email;     /* Extracted from JSON key */
    EVP_PKEY* private_key;  /* RSA key for JWT signing */
} gcp_iam_provider_t;

/**
 * @brief Build and sign a Google OAuth2 JWT assertion.
 *
 * @return Heap-allocated JWT string, or NULL on error. Caller must keel_free().
 */
static char* gcp_build_jwt(gcp_iam_provider_t* gcp) {
    if (!gcp->client_email || !gcp->private_key) return NULL;

    time_t now = time(NULL);

    /* JWT header: {"alg":"RS256","typ":"JWT"} */
    const char* header_json = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
    char* header_b64 = base64url_encode_alloc(
        (const uint8_t*)header_json, strlen(header_json));
    if (!header_b64) return NULL;

    /* JWT payload */
    char payload[1024];
    int plen = snprintf(payload, sizeof(payload),
        "{\"iss\":\"%s\","
        "\"scope\":\"https://www.googleapis.com/auth/cloud-platform\","
        "\"aud\":\"https://oauth2.googleapis.com/token\","
        "\"iat\":%ld,"
        "\"exp\":%ld}",
        gcp->client_email, (long)now, (long)(now + 3600));

    char* payload_b64 = base64url_encode_alloc(
        (const uint8_t*)payload, (size_t)plen);
    if (!payload_b64) { keel_free(header_b64); return NULL; }

    /* Signing input: header.payload */
    size_t input_len = strlen(header_b64) + 1 + strlen(payload_b64);
    char* signing_input = keel_malloc(input_len + 1);
    if (!signing_input) {
        keel_free(header_b64); keel_free(payload_b64); return NULL;
    }
    snprintf(signing_input, input_len + 1, "%s.%s", header_b64, payload_b64);

    /* RSA-SHA256 sign */
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (!mdctx) {
        keel_free(header_b64); keel_free(payload_b64);
        keel_free(signing_input); return NULL;
    }

    size_t sig_len = 0;
    char* jwt = NULL;

    if (EVP_DigestSignInit(mdctx, NULL, EVP_sha256(), NULL, gcp->private_key) != 1 ||
        EVP_DigestSignUpdate(mdctx, signing_input, input_len) != 1 ||
        EVP_DigestSignFinal(mdctx, NULL, &sig_len) != 1) {
        goto jwt_err;
    }

    uint8_t* sig = keel_malloc(sig_len);
    if (!sig) goto jwt_err;

    if (EVP_DigestSignFinal(mdctx, sig, &sig_len) != 1) {
        keel_free(sig); goto jwt_err;
    }

    char* sig_b64 = base64url_encode_alloc(sig, sig_len);
    keel_free(sig);
    if (!sig_b64) goto jwt_err;

    /* Assemble: header.payload.signature */
    size_t jwt_len = strlen(header_b64) + 1 + strlen(payload_b64) + 1 +
                     strlen(sig_b64);
    jwt = keel_malloc(jwt_len + 1);
    if (jwt)
        snprintf(jwt, jwt_len + 1, "%s.%s.%s", header_b64, payload_b64, sig_b64);
    keel_free(sig_b64);

jwt_err:
    EVP_MD_CTX_free(mdctx);
    keel_free(header_b64);
    keel_free(payload_b64);
    keel_free(signing_input);
    return jwt;
}

/**
 * @brief Parse a GCP service account JSON key file and extract credentials.
 */
static keel_error_t gcp_parse_key_file(gcp_iam_provider_t* gcp,
                                        const char* path) {
    FILE* f = fopen(path, "r");
    if (!f) return KEEL_ERR_AUTH;

    char buf[16384]; /* Service account JSON keys are typically ~2-3 KB */
    size_t nread = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    if (nread == 0) return KEEL_ERR_AUTH;
    buf[nread] = '\0';

    /* Extract client_email */
    char email[256];
    if (!json_find_string(buf, "client_email", email, sizeof(email))) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(gcp): missing client_email in key file");
        return KEEL_ERR_AUTH;
    }
    gcp->client_email = safe_strdup(email);

    /* Extract private_key (PEM-encoded RSA key) */
    char pem_escaped[8192];
    if (!json_find_string(buf, "private_key", pem_escaped, sizeof(pem_escaped))) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(gcp): missing private_key in key file");
        return KEEL_ERR_AUTH;
    }

    /* The JSON has literal \n — json_find_string already converts them */
    BIO* bio = BIO_new_mem_buf(pem_escaped, (int)strlen(pem_escaped));
    if (!bio) return KEEL_ERR_AUTH;

    gcp->private_key = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
    BIO_free(bio);

    if (!gcp->private_key) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(gcp): failed to parse private_key PEM");
        return KEEL_ERR_AUTH;
    }

    return KEEL_OK;
}

/**
 * @brief Fetch GCP token via JWT assertion grant.
 */
static keel_error_t gcp_fetch_via_jwt(gcp_iam_provider_t* gcp,
                                       keel_cloud_token_t* token_out) {
    char* jwt = gcp_build_jwt(gcp);
    if (!jwt) return KEEL_ERR_AUTH;

    /* POST to OAuth2 token endpoint */
    char post_body[8192];
    int blen = snprintf(post_body, sizeof(post_body),
        "grant_type=urn%%3Aietf%%3Aparams%%3Aoauth%%3Agrant-type%%3Ajwt-bearer"
        "&assertion=%s", jwt);
    keel_free(jwt);

    char resp[8192];
    ssize_t rlen = cloud_http_post(
        "oauth2.googleapis.com", 443, "/token",
        "application/x-www-form-urlencoded",
        post_body, (size_t)blen, resp, sizeof(resp));

    /* Note: This is HTTP, not HTTPS.  For production, TLS should be used.
     * In cloud environments, metadata traffic is typically on internal
     * networks.  The JWT signing itself provides authentication. */
    if (rlen <= 0) {
        /* Retry on port 80 for testing environments */
        rlen = cloud_http_post(
            "oauth2.googleapis.com", 80, "/token",
            "application/x-www-form-urlencoded",
            post_body, (size_t)blen, resp, sizeof(resp));
    }

    if (rlen <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(gcp): OAuth2 token exchange failed");
        return KEEL_ERR_AUTH;
    }

    /* Find HTTP body after headers */
    const char* body = strstr(resp, "\r\n\r\n");
    if (body) body += 4;
    else body = resp;

    char access_token[4096];
    if (!json_find_string(body, "access_token", access_token,
                           sizeof(access_token))) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(gcp): no access_token in OAuth2 response");
        return KEEL_ERR_AUTH;
    }

    int64_t expires_in = json_find_int(body, "expires_in");
    if (expires_in <= 0) expires_in = 3600;

    size_t tlen = strlen(access_token);
    char* token = keel_malloc(tlen + 1);
    if (!token) return KEEL_ERR_NOMEM;
    memcpy(token, access_token, tlen + 1);

    token_out->token = token;
    token_out->token_len = tlen;
    token_out->expires_at = time(NULL) + expires_in;

    return KEEL_OK;
}

/**
 * @brief Fetch GCP token from metadata server (VM/GKE environments).
 */
static keel_error_t gcp_fetch_via_metadata(keel_cloud_token_t* token_out) {
    char resp[8192];
    ssize_t rlen = cloud_http_get(
        "metadata.google.internal", 80,
        "/computeMetadata/v1/instance/service-accounts/default/token",
        "Metadata-Flavor: Google\r\n",
        resp, sizeof(resp));

    if (rlen <= 0) return KEEL_ERR_AUTH;

    /* Find HTTP body */
    const char* body = strstr(resp, "\r\n\r\n");
    if (body) body += 4;
    else body = resp;

    char access_token[4096];
    if (!json_find_string(body, "access_token", access_token,
                           sizeof(access_token)))
        return KEEL_ERR_AUTH;

    int64_t expires_in = json_find_int(body, "expires_in");
    if (expires_in <= 0) expires_in = 3600;

    size_t tlen = strlen(access_token);
    char* token = keel_malloc(tlen + 1);
    if (!token) return KEEL_ERR_NOMEM;
    memcpy(token, access_token, tlen + 1);

    token_out->token = token;
    token_out->token_len = tlen;
    token_out->expires_at = time(NULL) + expires_in;

    return KEEL_OK;
}

/**
 * @brief Fetch a GCP Cloud SQL IAM access token.
 *
 * Tries three strategies in order:
 *  1. JWT assertion via service account key (if a key file was loaded).
 *  2. GCP metadata server (IMDS) for VM / GKE environments.
 *  3. Token file specified by CLOUDSQL_ACCESS_TOKEN_FILE or
 *     GOOGLE_APPLICATION_CREDENTIALS environment variables.
 *
 * @param prov       Provider instance cast to gcp_iam_provider_t.
 * @param host       Unused (GCP tokens are not endpoint-specific).
 * @param port       Unused.
 * @param user       Unused.
 * @param token_out  Output token populated on success.
 * @return           KEEL_OK on success, KEEL_ERR_AUTH if all strategies
 *                   fail, KEEL_ERR_NOMEM on allocation failure.
 */
static keel_error_t gcp_iam_fetch_token(keel_cloud_auth_provider_t* prov,
                                         const char* host, uint16_t port,
                                         const char* user,
                                         keel_cloud_token_t* token_out) {
    (void)host; (void)port; (void)user;
    gcp_iam_provider_t* gcp = (gcp_iam_provider_t*)prov;

    /* Strategy 1: JWT assertion via service account key */
    if (gcp->private_key && gcp->client_email) {
        keel_error_t err = gcp_fetch_via_jwt(gcp, token_out);
        if (err == KEEL_OK) return KEEL_OK;
        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                      "cloud_auth(gcp): JWT token exchange failed, "
                      "trying metadata server");
    }

    /* Strategy 2: GCP metadata server (VM/GKE) */
    {
        keel_error_t err = gcp_fetch_via_metadata(token_out);
        if (err == KEEL_OK) return KEEL_OK;
    }

    /* Strategy 3: Token file fallback (external helper) */
    const char* path = getenv("CLOUDSQL_ACCESS_TOKEN_FILE");
    if (!path) path = getenv("GOOGLE_APPLICATION_CREDENTIALS");
    if (path) {
        FILE* f = fopen(path, "r");
        if (f) {
            char buf[4096];
            size_t nread = fread(buf, 1, sizeof(buf) - 1, f);
            fclose(f);
            if (nread > 0) {
                buf[nread] = '\0';
                while (nread > 0 && (buf[nread-1] == '\n' || buf[nread-1] == '\r'))
                    buf[--nread] = '\0';
                char* token = keel_malloc(nread + 1);
                if (!token) return KEEL_ERR_NOMEM;
                memcpy(token, buf, nread + 1);
                memset(buf, 0, sizeof(buf));
                token_out->token = token;
                token_out->token_len = nread;
                token_out->expires_at = time(NULL) + 3000;
                return KEEL_OK;
            }
        }
    }

    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                   "cloud_auth(gcp): all token acquisition methods failed");
    return KEEL_ERR_AUTH;
}

/**
 * @brief Destroy a GCP IAM provider and release its resources.
 *
 * Frees the service account file path, client e-mail, and the in-memory
 * RSA private key.
 *
 * @param prov  Provider instance to destroy (cast to gcp_iam_provider_t).
 */
static void gcp_iam_destroy(keel_cloud_auth_provider_t* prov) {
    gcp_iam_provider_t* gcp = (gcp_iam_provider_t*)prov;
    keel_free(gcp->service_account_file);
    keel_free(gcp->client_email);
    if (gcp->private_key)
        EVP_PKEY_free(gcp->private_key);
    keel_free(gcp);
}

static const keel_cloud_auth_ops_t gcp_iam_ops = {
    .name        = "gcp-cloud-sql-iam",
    .type        = KEEL_CLOUD_AUTH_GCP_IAM,
    .create      = NULL,
    .fetch_token = gcp_iam_fetch_token,
    .destroy     = gcp_iam_destroy,
};

/**
 * @brief Create a GCP Cloud SQL IAM authentication provider.
 *
 * If a service account key file is available (via @p config or the
 * GOOGLE_APPLICATION_CREDENTIALS environment variable) it is parsed
 * immediately so that JWT-based token exchange is available at fetch
 * time.  When no key file is found the provider falls back to the GCP
 * metadata server at runtime.
 *
 * @param out     Receives the newly created provider on success.
 * @param config  Optional GCP configuration; may be NULL to rely entirely
 *                on environment variables.
 * @return        KEEL_OK on success, KEEL_ERR_INVALID_ARG if @p out is
 *                NULL, KEEL_ERR_NOMEM on allocation failure.
 */
keel_error_t keel_cloud_auth_gcp_create(keel_cloud_auth_provider_t** out,
                                        const keel_cloud_gcp_config_t* config) {
    if (!out) return KEEL_ERR_INVALID_ARG;

    gcp_iam_provider_t* gcp = keel_calloc(1, sizeof(*gcp));
    if (!gcp) return KEEL_ERR_NOMEM;

    gcp->base.ops = &gcp_iam_ops;
    gcp->base.data = gcp;

    /* Try to parse service account key file for native JWT auth */
    const char* key_path = (config && config->service_account_file) ?
        config->service_account_file :
        getenv("GOOGLE_APPLICATION_CREDENTIALS");

    if (key_path) {
        gcp->service_account_file = safe_strdup(key_path);
        keel_error_t err = gcp_parse_key_file(gcp, key_path);
        if (err == KEEL_OK) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_AUTH,
                          "cloud_auth(gcp): loaded service account key "
                          "for %s", gcp->client_email);
        } else {
            /* Not fatal — file might be a plain access token, or we'll
             * fall back to metadata server at fetch time */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_AUTH,
                           "cloud_auth(gcp): key file parse failed, "
                           "will try metadata server at fetch time");
        }
    }

    *out = &gcp->base;
    return KEEL_OK;
}

/* ============================================================================
 * Azure AD / Entra managed-identity token provider
 *
 * Supports three token acquisition methods (in priority order):
 * 1. Azure IMDS (Instance Metadata Service) — for VMs and AKS pods
 *    GET http://169.254.169.254/metadata/identity/oauth2/token
 *    ?api-version=2018-02-01&resource=<resource>
 *    Header: Metadata: true
 *
 * 2. AZURE_POSTGRESQL_ACCESS_TOKEN env var — direct token injection
 * 3. AZURE_ACCESS_TOKEN_FILE env var — token file (external helper)
 *
 * For user-assigned managed identity, specify client_id in config.
 * Default resource: https://ossrdbms-aad.database.windows.net
 * ============================================================================ */

typedef struct azure_ad_provider {
    keel_cloud_auth_provider_t base;
    char* client_id;
    char* resource;
} azure_ad_provider_t;

#define AZURE_DEFAULT_RESOURCE "https://ossrdbms-aad.database.windows.net"

/**
 * @brief Fetch token from Azure IMDS endpoint.
 */
static keel_error_t azure_fetch_via_imds(azure_ad_provider_t* az,
                                          keel_cloud_token_t* token_out) {
    const char* resource = az->resource ? az->resource : AZURE_DEFAULT_RESOURCE;

    /* Build query path */
    char path[1024];
    int plen = snprintf(path, sizeof(path),
        "/metadata/identity/oauth2/token"
        "?api-version=2018-02-01"
        "&resource=%s", resource);

    if (az->client_id)
        plen += snprintf(path + plen, sizeof(path) - (size_t)plen,
                         "&client_id=%s", az->client_id);

    char resp[8192];
    ssize_t rlen = cloud_http_get(
        "169.254.169.254", 80, path,
        "Metadata: true\r\n",
        resp, sizeof(resp));

    if (rlen <= 0) return KEEL_ERR_AUTH;

    /* Find HTTP body */
    const char* body = strstr(resp, "\r\n\r\n");
    if (body) body += 4;
    else body = resp;

    char access_token[4096];
    if (!json_find_string(body, "access_token", access_token,
                           sizeof(access_token)))
        return KEEL_ERR_AUTH;

    /* expires_on is a Unix timestamp string */
    int64_t expires_on = json_find_int(body, "expires_on");
    if (expires_on <= 0) {
        /* Try expires_in as fallback */
        int64_t expires_in = json_find_int(body, "expires_in");
        expires_on = time(NULL) + (expires_in > 0 ? expires_in : 3600);
    }

    size_t tlen = strlen(access_token);
    char* token = keel_malloc(tlen + 1);
    if (!token) return KEEL_ERR_NOMEM;
    memcpy(token, access_token, tlen + 1);

    token_out->token = token;
    token_out->token_len = tlen;
    token_out->expires_at = (time_t)expires_on;

    return KEEL_OK;
}

/**
 * @brief Fetch an Azure AD / Entra managed-identity access token.
 *
 * Tries three strategies in order:
 *  1. Azure IMDS endpoint (169.254.169.254) for VMs and AKS pods.
 *  2. AZURE_POSTGRESQL_ACCESS_TOKEN environment variable.
 *  3. Token file specified by AZURE_ACCESS_TOKEN_FILE.
 *
 * @param prov       Provider instance cast to azure_ad_provider_t.
 * @param host       Unused (Azure tokens are not endpoint-specific).
 * @param port       Unused.
 * @param user       Unused.
 * @param token_out  Output token populated on success.
 * @return           KEEL_OK on success, KEEL_ERR_AUTH if all strategies
 *                   fail, KEEL_ERR_NOMEM on allocation failure.
 */
static keel_error_t azure_ad_fetch_token(keel_cloud_auth_provider_t* prov,
                                          const char* host, uint16_t port,
                                          const char* user,
                                          keel_cloud_token_t* token_out) {
    (void)host; (void)port; (void)user;
    azure_ad_provider_t* az = (azure_ad_provider_t*)prov;

    /* Strategy 1: Azure IMDS (works on Azure VMs and AKS pods) */
    {
        keel_error_t err = azure_fetch_via_imds(az, token_out);
        if (err == KEEL_OK) return KEEL_OK;
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(azure): IMDS not available, "
                       "trying env/file fallback");
    }

    /* Strategy 2: AZURE_POSTGRESQL_ACCESS_TOKEN env var */
    const char* direct_token = getenv("AZURE_POSTGRESQL_ACCESS_TOKEN");
    if (direct_token && strlen(direct_token) > 0) {
        size_t len = strlen(direct_token);
        char* token = keel_malloc(len + 1);
        if (!token) return KEEL_ERR_NOMEM;
        memcpy(token, direct_token, len + 1);
        token_out->token = token;
        token_out->token_len = len;
        token_out->expires_at = time(NULL) + 3000; /* ~50 min */
        return KEEL_OK;
    }

    /* Strategy 3: AZURE_ACCESS_TOKEN_FILE */
    const char* path = getenv("AZURE_ACCESS_TOKEN_FILE");
    if (path) {
        FILE* f = fopen(path, "r");
        if (f) {
            char buf[4096];
            size_t nread = fread(buf, 1, sizeof(buf) - 1, f);
            fclose(f);
            if (nread > 0) {
                buf[nread] = '\0';
                while (nread > 0 && (buf[nread-1] == '\n' || buf[nread-1] == '\r'))
                    buf[--nread] = '\0';
                char* token = keel_malloc(nread + 1);
                if (!token) return KEEL_ERR_NOMEM;
                memcpy(token, buf, nread + 1);
                memset(buf, 0, sizeof(buf));
                token_out->token = token;
                token_out->token_len = nread;
                token_out->expires_at = time(NULL) + 3000;
                return KEEL_OK;
            }
        }
    }

    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                   "cloud_auth(azure): all token acquisition methods failed");
    return KEEL_ERR_AUTH;
}

/**
 * @brief Destroy an Azure AD provider and release its resources.
 *
 * @param prov  Provider instance to destroy (cast to azure_ad_provider_t).
 */
static void azure_ad_destroy(keel_cloud_auth_provider_t* prov) {
    azure_ad_provider_t* az = (azure_ad_provider_t*)prov;
    keel_free(az->client_id);
    keel_free(az->resource);
    keel_free(az);
}

static const keel_cloud_auth_ops_t azure_ad_ops = {
    .name        = "azure-ad",
    .type        = KEEL_CLOUD_AUTH_AZURE_AD,
    .create      = NULL,
    .fetch_token = azure_ad_fetch_token,
    .destroy     = azure_ad_destroy,
};

/**
 * @brief Create an Azure AD / Entra managed-identity authentication provider.
 *
 * Supports user-assigned managed identities via @p config->client_id and
 * custom token audiences via @p config->resource.  When @p config is NULL
 * the system-assigned identity with the default PostgreSQL resource is used.
 *
 * @param out     Receives the newly created provider on success.
 * @param config  Optional Azure configuration; may be NULL.
 * @return        KEEL_OK on success, KEEL_ERR_INVALID_ARG if @p out is
 *                NULL, KEEL_ERR_NOMEM on allocation failure.
 */
keel_error_t keel_cloud_auth_azure_create(keel_cloud_auth_provider_t** out,
                                          const keel_cloud_azure_config_t* config) {
    if (!out) return KEEL_ERR_INVALID_ARG;

    azure_ad_provider_t* az = keel_calloc(1, sizeof(*az));
    if (!az) return KEEL_ERR_NOMEM;

    az->base.ops = &azure_ad_ops;
    az->base.data = az;
    if (config) {
        az->client_id = safe_strdup(config->client_id);
        az->resource = safe_strdup(config->resource);
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_AUTH,
                  "cloud_auth(azure): provider created (client_id=%s, resource=%s)",
                  az->client_id ? az->client_id : "system-identity",
                  az->resource ? az->resource : AZURE_DEFAULT_RESOURCE);

    *out = &az->base;
    return KEEL_OK;
}

/* ============================================================================
 * Static file/env token provider
 * ============================================================================ */

typedef struct static_provider {
    keel_cloud_auth_provider_t base;
    keel_cloud_auth_type_t type;
    char* path;                      /* File path or env variable name */
    int   refresh_interval_s;
} static_provider_t;

/**
 * @brief Fetch a static token from an environment variable or file.
 *
 * For KEEL_CLOUD_AUTH_STATIC_ENV the value of the named environment
 * variable is returned as the token.  For KEEL_CLOUD_AUTH_STATIC_FILE
 * the first line (stripped of trailing newlines) of the file is used.
 * The stack buffer containing the secret is zeroed after copying.
 *
 * @param prov       Provider instance cast to static_provider_t.
 * @param host       Unused.
 * @param port       Unused.
 * @param user       Unused.
 * @param token_out  Output token populated on success.
 * @return           KEEL_OK on success, KEEL_ERR_AUTH if the source is
 *                   unavailable or empty, KEEL_ERR_NOMEM on allocation
 *                   failure.
 */
static keel_error_t static_fetch_token(keel_cloud_auth_provider_t* prov,
                                        const char* host, uint16_t port,
                                        const char* user,
                                        keel_cloud_token_t* token_out) {
    (void)host; (void)port; (void)user;
    static_provider_t* sp = (static_provider_t*)prov;

    if (!sp->path) return KEEL_ERR_AUTH;

    if (sp->type == KEEL_CLOUD_AUTH_STATIC_ENV) {
        const char* val = getenv(sp->path);
        if (!val || strlen(val) == 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                           "cloud_auth(env): env var %s not set", sp->path);
            return KEEL_ERR_AUTH;
        }
        size_t len = strlen(val);
        char* token = keel_malloc(len + 1);
        if (!token) return KEEL_ERR_NOMEM;
        memcpy(token, val, len + 1);
        token_out->token = token;
        token_out->token_len = len;
        token_out->expires_at = sp->refresh_interval_s > 0 ?
            time(NULL) + sp->refresh_interval_s : 0;
        return KEEL_OK;
    }

    /* KEEL_CLOUD_AUTH_STATIC_FILE */
    FILE* f = fopen(sp->path, "r");
    if (!f) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                       "cloud_auth(file): cannot open %s: %s",
                       sp->path, strerror(errno));
        return KEEL_ERR_AUTH;
    }

    char buf[4096];
    size_t nread = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);

    if (nread == 0) return KEEL_ERR_AUTH;
    buf[nread] = '\0';

    while (nread > 0 && (buf[nread - 1] == '\n' || buf[nread - 1] == '\r'))
        buf[--nread] = '\0';

    char* token = keel_malloc(nread + 1);
    if (!token) return KEEL_ERR_NOMEM;
    memcpy(token, buf, nread + 1);

    /* Zero the stack buffer that held the secret */
    memset(buf, 0, sizeof(buf));

    token_out->token = token;
    token_out->token_len = nread;
    token_out->expires_at = sp->refresh_interval_s > 0 ?
        time(NULL) + sp->refresh_interval_s : 0;
    return KEEL_OK;
}

/**
 * @brief Destroy a static token provider and release its resources.
 *
 * @param prov  Provider instance to destroy (cast to static_provider_t).
 */
static void static_destroy(keel_cloud_auth_provider_t* prov) {
    static_provider_t* sp = (static_provider_t*)prov;
    keel_free(sp->path);
    keel_free(sp);
}

static const keel_cloud_auth_ops_t static_ops = {
    .name        = "static",
    .type        = KEEL_CLOUD_AUTH_STATIC_FILE,
    .create      = NULL,
    .fetch_token = static_fetch_token,
    .destroy     = static_destroy,
};

/**
 * @brief Create a static file or environment-variable token provider.
 *
 * @param out     Receives the newly created provider on success.
 * @param type    Either KEEL_CLOUD_AUTH_STATIC_FILE or
 *                KEEL_CLOUD_AUTH_STATIC_ENV.
 * @param config  Provider configuration supplying the file path or env
 *                variable name and optional refresh interval.
 * @return        KEEL_OK on success, KEEL_ERR_INVALID_ARG if arguments
 *                are invalid or the type is unrecognised, KEEL_ERR_NOMEM
 *                on allocation failure.
 */
keel_error_t keel_cloud_auth_static_create(keel_cloud_auth_provider_t** out,
                                           keel_cloud_auth_type_t type,
                                           const keel_cloud_static_config_t* config) {
    if (!out || !config || !config->path) return KEEL_ERR_INVALID_ARG;
    if (type != KEEL_CLOUD_AUTH_STATIC_FILE &&
        type != KEEL_CLOUD_AUTH_STATIC_ENV)
        return KEEL_ERR_INVALID_ARG;

    static_provider_t* sp = keel_calloc(1, sizeof(*sp));
    if (!sp) return KEEL_ERR_NOMEM;

    sp->base.ops = &static_ops;
    sp->base.data = sp;
    sp->type = type;
    sp->path = safe_strdup(config->path);
    sp->refresh_interval_s = config->refresh_interval_s;

    *out = &sp->base;
    return KEEL_OK;
}
