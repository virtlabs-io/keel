/**
 * @file cloud_auth.h
 * @brief Cloud-native authentication token providers for backend connections.
 *
 * Provides a pluggable token-provider abstraction for fetching short-lived
 * authentication tokens from cloud identity services.  Each provider
 * encapsulates vendor-specific credential exchange (AWS SigV4 pre-signing,
 * GCP OAuth2 metadata, Azure managed-identity token endpoint) behind a
 * uniform fetch/refresh API.
 *
 * Tokens are cached in the backend pool and refreshed automatically before
 * expiry so that SCRAM / cleartext-password handshakes always use a valid
 * credential without blocking the reactor on network round-trips.
 *
 * Supported providers:
 *   - AWS RDS IAM   — generates a pre-signed URL token using SigV4
 *   - GCP Cloud SQL — fetches an OAuth2 access token for Cloud SQL IAM
 *   - Azure AD      — fetches a managed-identity token for Azure DB
 *   - Static        — wraps an env-var or file path (for external renewal)
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_CLOUD_AUTH_H
#define KEEL_CLOUD_AUTH_H

#include "keel_types.h"
#include "keel_error.h"

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Cloud auth provider type enum
 * ============================================================================ */

typedef enum keel_cloud_auth_type {
    KEEL_CLOUD_AUTH_NONE = 0,        /**< Static password (no cloud auth) */
    KEEL_CLOUD_AUTH_AWS_IAM,         /**< AWS RDS IAM token (SigV4 pre-signed) */
    KEEL_CLOUD_AUTH_GCP_IAM,         /**< GCP Cloud SQL IAM (OAuth2 token) */
    KEEL_CLOUD_AUTH_AZURE_AD,        /**< Azure AD / Entra managed identity */
    KEEL_CLOUD_AUTH_STATIC_FILE,     /**< Password read from file at refresh time */
    KEEL_CLOUD_AUTH_STATIC_ENV,      /**< Password read from env var at refresh time */
} keel_cloud_auth_type_t;

/* ============================================================================
 * Token result
 * ============================================================================ */

/**
 * @brief A fetched authentication token and its metadata.
 */
typedef struct keel_cloud_token {
    char*    token;                  /**< NUL-terminated token string (allocated) */
    size_t   token_len;             /**< Length of token (excluding NUL) */
    time_t   expires_at;            /**< Absolute expiry (UNIX epoch), 0 = unknown */
} keel_cloud_token_t;

/* ============================================================================
 * Provider configuration
 * ============================================================================ */

/**
 * @brief AWS RDS IAM configuration.
 *
 * Uses AWS credentials (from env or instance profile) to generate a
 * pre-signed URL that serves as the database password for IAM-authenticated
 * RDS instances.  Token lifetime is 15 minutes; refresh at 14 minutes.
 */
typedef struct keel_cloud_aws_config {
    const char* region;              /**< AWS region (e.g. "us-east-1") */
    const char* access_key_id;       /**< AWS_ACCESS_KEY_ID (NULL = env/instance) */
    const char* secret_access_key;   /**< AWS_SECRET_ACCESS_KEY (NULL = env/instance) */
    const char* session_token;       /**< AWS_SESSION_TOKEN (NULL = none) */
} keel_cloud_aws_config_t;

/**
 * @brief GCP Cloud SQL IAM configuration.
 *
 * Obtains an OAuth2 access token from the GCP metadata server or from a
 * service account key file, used as the password for IAM-authenticated
 * Cloud SQL connections.
 */
typedef struct keel_cloud_gcp_config {
    const char* service_account_file; /**< Path to service account JSON key (NULL = metadata) */
} keel_cloud_gcp_config_t;

/**
 * @brief Azure AD (Entra) managed-identity configuration.
 *
 * Fetches a token from the Azure IMDS endpoint using managed identity,
 * used as the password for Azure Database for PostgreSQL/MySQL.
 */
typedef struct keel_cloud_azure_config {
    const char* client_id;           /**< Managed identity client ID (NULL = system identity) */
    const char* resource;            /**< Resource URI (default: "https://ossrdbms-aad.database.windows.net") */
} keel_cloud_azure_config_t;

/**
 * @brief Static file/env credential rotation configuration.
 */
typedef struct keel_cloud_static_config {
    const char* path;                /**< File path or env variable name */
    int         refresh_interval_s;  /**< Re-read interval in seconds (0 = once) */
} keel_cloud_static_config_t;

/* ============================================================================
 * Provider operations vtable
 * ============================================================================ */

typedef struct keel_cloud_auth_provider keel_cloud_auth_provider_t;

/**
 * @brief Cloud auth provider operations.
 *
 * Each cloud auth method implements this vtable.  The `fetch_token` operation
 * may read files, compute HMAC signatures, or make HTTP requests depending on
 * the provider.  Providers MUST be safe to call from a single thread (the
 * worker's reactor thread); expensive operations should be minimised or cached.
 */
typedef struct keel_cloud_auth_ops {
    const char* name;                /**< Human-readable provider name */
    keel_cloud_auth_type_t type;     /**< Provider type identifier */

    /** Create provider instance from config. */
    keel_error_t (*create)(keel_cloud_auth_provider_t** out,
                           const void* config);

    /** Fetch or refresh the token.  Caller frees token->token. */
    keel_error_t (*fetch_token)(keel_cloud_auth_provider_t* prov,
                                const char* host, uint16_t port,
                                const char* user,
                                keel_cloud_token_t* token_out);

    /** Destroy provider instance. */
    void (*destroy)(keel_cloud_auth_provider_t* prov);
} keel_cloud_auth_ops_t;

/**
 * @brief Cloud auth provider instance.
 */
struct keel_cloud_auth_provider {
    const keel_cloud_auth_ops_t* ops;    /**< Provider operations */
    void*                        data;   /**< Provider-specific state */
};

/* ============================================================================
 * Token cache (embedded in backend pool)
 * ============================================================================ */

/**
 * @brief Token cache entry for a backend pool.
 *
 * The pool keeps a single cached token and refreshes it when the token is
 * within `refresh_margin_s` seconds of expiry.  All connections from the
 * pool share the same cached token.
 */
typedef struct keel_cloud_token_cache {
    keel_cloud_auth_provider_t* provider;        /**< Active provider (NULL = static pw) */
    char*                       cached_token;    /**< Current valid token */
    size_t                      cached_token_len;/**< Token length */
    time_t                      expires_at;      /**< Token expiry time */
    int                         refresh_margin_s;/**< Refresh this many seconds before expiry */
    bool                        valid;           /**< Cache has a usable token */
} keel_cloud_token_cache_t;

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Get the effective password for a backend pool connection.
 *
 * If cloud auth is configured, returns the cached (or freshly fetched) token.
 * Otherwise, returns the static password from pool config.
 *
 * The returned pointer is valid until the next call to this function on the
 * same cache, or until the cache is destroyed.
 *
 * @param cache     Token cache (from pool).
 * @param host      Backend hostname (for AWS SigV4 scope).
 * @param port      Backend port.
 * @param user      Backend username.
 * @param static_pw Fallback static password (from pool config).
 * @return          Password string to use, or NULL on error.
 */
const char* keel_cloud_auth_get_password(keel_cloud_token_cache_t* cache,
                                         const char* host, uint16_t port,
                                         const char* user,
                                         const char* static_pw);

/**
 * @brief Initialize a token cache with a cloud auth provider.
 *
 * @param cache         Token cache to initialise.
 * @param provider      Cloud auth provider (takes ownership), or NULL.
 * @param margin_s      Refresh margin in seconds before expiry.
 */
void keel_cloud_token_cache_init(keel_cloud_token_cache_t* cache,
                                 keel_cloud_auth_provider_t* provider,
                                 int margin_s);

/**
 * @brief Destroy a token cache, releasing the cached token and provider.
 */
void keel_cloud_token_cache_destroy(keel_cloud_token_cache_t* cache);

/* ============================================================================
 * Built-in provider constructors
 * ============================================================================ */

/** Create an AWS RDS IAM token provider. */
keel_error_t keel_cloud_auth_aws_create(keel_cloud_auth_provider_t** out,
                                        const keel_cloud_aws_config_t* config);

/** Create a GCP Cloud SQL IAM token provider. */
keel_error_t keel_cloud_auth_gcp_create(keel_cloud_auth_provider_t** out,
                                        const keel_cloud_gcp_config_t* config);

/** Create an Azure AD managed-identity token provider. */
keel_error_t keel_cloud_auth_azure_create(keel_cloud_auth_provider_t** out,
                                          const keel_cloud_azure_config_t* config);

/** Create a static file/env token provider. */
keel_error_t keel_cloud_auth_static_create(keel_cloud_auth_provider_t** out,
                                           keel_cloud_auth_type_t type,
                                           const keel_cloud_static_config_t* config);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CLOUD_AUTH_H */
