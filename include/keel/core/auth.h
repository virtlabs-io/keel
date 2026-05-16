/**
 * @file auth.h
 * @brief Public API for KEEL's pluggable client authentication framework.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This module provides a pluggable authentication framework that supports
 * multiple authentication methods. The pooler authenticates clients
 * independently from the backend database connections.
 *
 * Supported authentication methods:
 * - SCRAM-SHA-256 (default, recommended)
 * - MD5 (legacy, for compatibility)
 * - Trust (no authentication, for development only)
 * - Certificate (TLS client certificates)
 * - LDAP (via auth_ldap plugin)
 * - Active Directory (via auth_ad plugin)
 * - Kerberos/GSSAPI (via auth_gssapi plugin)
 *
 * The authentication flow:
 * 1. Client connects and sends StartupMessage
 * 2. Pooler extracts username and initiates auth based on configuration
 * 3. Auth provider handles the challenge/response exchange
 * 4. On success, client is associated with a pool
 * 5. Backend connections are established with pooler's credentials
 */

#ifndef KEEL_AUTH_H
#define KEEL_AUTH_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/protocol/protocol.h"   /* For keel_auth_method_t */

#include <stdbool.h>
#include <stddef.h>

/* Forward declarations */
typedef struct keel_auth_provider keel_auth_provider_t;
typedef struct keel_auth_context keel_auth_context_t;
typedef struct keel_auth_manager keel_auth_manager_t;

/* ============================================================================
 * Authentication State
 * ============================================================================ */

/**
 * @brief Per-connection authentication state machine stages.
 */
typedef enum keel_auth_state {
    KEEL_AUTH_STATE_INIT = 0,        /**< Initial state */
    KEEL_AUTH_STATE_CHALLENGE,       /**< Waiting for client response */
    KEEL_AUTH_STATE_VERIFY,          /**< Verifying credentials */
    KEEL_AUTH_STATE_SUCCESS,         /**< Authentication succeeded */
    KEEL_AUTH_STATE_FAILED,          /**< Authentication failed */
    KEEL_AUTH_STATE_ERROR,           /**< Internal error */
} keel_auth_state_t;

/* ============================================================================
 * User Database Interface
 * ============================================================================ */

/**
 * @brief User credentials and policy attributes resolved for a login attempt.
 */
typedef struct keel_auth_user {
    char*   username;               /**< Username */
    char*   password_hash;          /**< Hashed password (method-dependent) */
    char*   password_salt;          /**< Salt for password (if applicable) */
    int     iterations;             /**< PBKDF2 iterations (SCRAM) */
    uint8_t stored_key[32];         /**< StoredKey for SCRAM */
    uint8_t server_key[32];         /**< ServerKey for SCRAM */
    bool    has_scram_keys;         /**< True if SCRAM keys are precomputed */
    char*   pool_name;              /**< Default pool for this user */
    char*   database;               /**< Default database */
    bool    superuser;              /**< Has superuser privileges */
    bool    can_login;              /**< Can login (vs. role-only user) */
} keel_auth_user_t;

/**
 * @brief Callback used by authentication providers to resolve user records.
 *
 * Called by auth providers to look up user credentials.
 *
 * @param username  Username to look up
 * @param user_out  Output: user credentials (caller must not free)
 * @param user_data Callback user data
 * @return `KEEL_OK` when the user was resolved, `KEEL_ERR_NOT_FOUND` when the
 *         username does not exist, or another error code for lookup failures.
 */
typedef keel_error_t (*keel_auth_user_lookup_fn)(
    const char* username,
    const keel_auth_user_t** user_out,
    void* user_data
);

/* ============================================================================
 * Authentication Provider Interface
 * ============================================================================ */

/**
 * @brief Virtual table implemented by each authentication provider.
 *
 * Each authentication method implements these operations.
 */
typedef struct keel_auth_provider_ops {
    /**
    * @brief Return the human-readable provider name.
     */
    const char* (*name)(void);
    
    /**
    * @brief Return the protocol-facing authentication method identifier.
     */
    keel_auth_method_t (*method)(void);
    
    /**
     * @brief Initialize provider
     *
     * @param provider Provider instance
     * @param config   Provider-specific configuration
    * @return `KEEL_OK` on success or an error code if provider setup failed.
     */
    keel_error_t (*init)(keel_auth_provider_t* provider, const void* config);
    
    /**
    * @brief Tear down provider state and free provider-owned resources.
    * @return
     */
    void (*destroy)(keel_auth_provider_t* provider);
    
    /**
     * @brief Start authentication for a user
     *
     * Creates an auth context and generates the initial challenge.
     *
     * @param provider    Provider instance
     * @param username    Username from client
     * @param user        User credentials (may be NULL for external auth)
     * @param ctx_out     Output: new auth context
    * @return `KEEL_OK` on success or an error code if authentication could
    *         not be started for the supplied user.
     */
    keel_error_t (*start)(
        keel_auth_provider_t* provider,
        const char* username,
        const keel_auth_user_t* user,
        keel_auth_context_t** ctx_out
    );
    
    /**
     * @brief Process client response
     *
     * @param ctx      Auth context
     * @param data     Client response data
     * @param len      Response length
    * @return Updated authentication state after processing the client payload.
     */
    keel_auth_state_t (*process)(
        keel_auth_context_t* ctx,
        const void* data,
        size_t len
    );
    
    /**
     * @brief Get challenge/response message to send
     *
     * @param ctx       Auth context
     * @param msg_out   Output: message data (caller must free)
     * @param len_out   Output: message length
     * @param type_out  Output: PostgreSQL auth message type
    * @return `KEEL_OK` when a protocol message was produced, or an error code
    *         if no message is available or provider state is invalid.
     */
    keel_error_t (*get_message)(
        keel_auth_context_t* ctx,
        void** msg_out,
        size_t* len_out,
        int* type_out
    );
    
    /**
    * @brief Release all memory associated with an authentication context.
    * @return
     */
    void (*free_context)(keel_auth_context_t* ctx);
    
} keel_auth_provider_ops_t;

/**
 * @brief Authentication provider instance
 */
struct keel_auth_provider {
    const keel_auth_provider_ops_t* ops;     /**< Provider operations */
    void* provider_data;                     /**< Provider-specific data */
    keel_auth_user_lookup_fn user_lookup;    /**< User lookup function */
    void* user_lookup_data;                  /**< User lookup callback data */
};

/**
 * @brief Authentication context (per-connection auth session)
 */
struct keel_auth_context {
    keel_auth_provider_t* provider;          /**< Provider handling this auth */
    keel_auth_state_t state;                 /**< Current state */
    char* username;                         /**< Username being authenticated */
    const keel_auth_user_t* user;            /**< User record (if found) */
    void* auth_data;                        /**< Method-specific auth data */
    char* error_message;                    /**< Error message if failed */
    int verify_fd;                          /**< eventfd for VERIFY-state async providers; -1 otherwise */
};

/* ============================================================================
 * Authentication Manager
 * ============================================================================ */

/**
 * @brief Configuration used when creating the shared authentication manager.
 */
typedef struct keel_auth_manager_config {
    keel_auth_method_t default_method;       /**< Default auth method */
    bool allow_clear_password;              /**< Allow clear text passwords */
    int scram_iterations;                   /**< SCRAM PBKDF2 iterations */
    const char* userlist_file;              /**< Path to userlist file */
} keel_auth_manager_config_t;

/**
 * @brief Create the shared authentication manager and register core policy.
 *
 * @param config  Configuration
 * @return New manager instance, or `NULL` if memory allocation or initial
 *         setup failed.
 */
keel_auth_manager_t* keel_auth_manager_create(const keel_auth_manager_config_t* config);

/**
 * @brief Destroy an authentication manager and all registered providers.
 * @return
 */
void keel_auth_manager_destroy(keel_auth_manager_t* mgr);

/**
 * @brief Register an authentication provider
 *
 * @param mgr      Auth manager
 * @param ops      Provider operations
 * @param config   Provider configuration
 * @return `KEEL_OK` on success or an error code if registration failed.
 */
keel_error_t keel_auth_manager_register(
    keel_auth_manager_t* mgr,
    const keel_auth_provider_ops_t* ops,
    const void* config
);

/**
 * @brief Get provider for a method
 *
 * @param mgr     Auth manager
 * @param method  Auth method
 * @return Matching provider, or `NULL` when the method is unsupported.
 */
keel_auth_provider_t* keel_auth_manager_get_provider(
    keel_auth_manager_t* mgr,
    keel_auth_method_t method
);

/**
 * @brief Install the callback used to resolve users during authentication.
 *
 * @param mgr        Auth manager
 * @param lookup     Lookup function
 * @param user_data Opaque user data forwarded to `lookup`.
 * @return
 */
void keel_auth_manager_set_user_lookup(
    keel_auth_manager_t* mgr,
    keel_auth_user_lookup_fn lookup,
    void* user_data
);

/**
 * @brief Start authentication for a client
 *
 * Determines the appropriate auth method and creates an auth context.
 *
 * @param mgr       Auth manager
 * @param username  Client username
 * @param ctx_out   Output: auth context
 * @return `KEEL_OK` on success or an error code when no provider can service
 *         the user or a context cannot be created.
 */
keel_error_t keel_auth_manager_start(
    keel_auth_manager_t* mgr,
    const char* username,
    keel_auth_context_t** ctx_out
);

/**
 * @brief Process authentication response
 *
 * @param ctx   Auth context
 * @param data  Response data
 * @param len   Response length
 * @return Updated authentication state after consuming the client response.
 */
keel_auth_state_t keel_auth_process(
    keel_auth_context_t* ctx,
    const void* data,
    size_t len
);

/**
 * @brief Get next message to send to client
 *
 * @param ctx       Auth context
 * @param msg_out   Output: message data
 * @param len_out   Output: message length
 * @param type_out  Output: auth message type
 * @return `KEEL_OK` when a message is available for the client, or an error
 *         code if the context cannot produce one.
 */
keel_error_t keel_auth_get_message(
    keel_auth_context_t* ctx,
    void** msg_out,
    size_t* len_out,
    int* type_out
);

/**
 * @brief Free an authentication context created by the manager or a provider.
 * @return
 */
void keel_auth_context_free(keel_auth_context_t* ctx);

/**
 * @brief Get current auth state
 */
keel_auth_state_t keel_auth_get_state(keel_auth_context_t* ctx);

/**
 * @brief Get the notify eventfd for an async auth context.
 *
 * Valid only while ctx->state == KEEL_AUTH_STATE_VERIFY.
 * The fd is written by the auth worker thread (exactly 8 bytes, value 1)
 * once the LDAP/PAM bind completes.  The engine arms a reactor read on
 * this fd; when it fires, call keel_engine_flow_resume_auth().
 *
 * @param ctx  Auth context in VERIFY state.
 * @return The eventfd file descriptor, or -1 if ctx is NULL or not in VERIFY
 *         state, or if the provider does not support async operation.
 */
int keel_auth_get_verify_fd(keel_auth_context_t* ctx);

/**
 * @brief Get auth state name
 */
const char* keel_auth_state_name(keel_auth_state_t state);

/**
 * @brief Get auth method name
 */
const char* keel_auth_method_name(keel_auth_method_t method);

/* ============================================================================
 * User Database Management
 * ============================================================================ */

/**
 * @brief Load users from userlist file
 *
 * File format (similar to pgbouncer):
 *   "username" "password_hash"
 *
 * Password formats:
 *   - SCRAM-SHA-256$iterations:salt:stored_key:server_key
 *   - md5<hash>
 *   - plain text (will be hashed)
 *
 * @param mgr       Auth manager
 * @param filepath  Path to userlist file
 * @return KEEL_OK on success
 */
keel_error_t keel_auth_load_userlist(
    keel_auth_manager_t* mgr,
    const char* filepath
);

/**
 * @brief Add user to auth manager
 *
 * @param mgr   Auth manager
 * @param user  User to add (copied)
 * @return KEEL_OK on success
 */
keel_error_t keel_auth_add_user(
    keel_auth_manager_t* mgr,
    const keel_auth_user_t* user
);

/**
 * @brief Remove user from auth manager
 */
keel_error_t keel_auth_remove_user(
    keel_auth_manager_t* mgr,
    const char* username
);

/**
 * @brief Look up user by username
 */
keel_error_t keel_auth_lookup_user(
    keel_auth_manager_t* mgr,
    const char* username,
    const keel_auth_user_t** user_out
);

/* ============================================================================
 * Password Hashing Utilities
 * ============================================================================ */

/**
 * @brief Hash password for SCRAM-SHA-256 storage
 *
 * @param password    Plain text password
 * @param iterations  PBKDF2 iterations (4096 minimum)
 * @param hash_out    Output: hash string (caller must free)
 * @return KEEL_OK on success
 */
keel_error_t keel_auth_scram_hash_password(
    const char* password,
    int iterations,
    char** hash_out
);

/**
 * @brief Parse SCRAM-SHA-256 stored hash
 *
 * @param hash        Stored hash string
 * @param salt_out    Output: salt (base64, caller must free)
 * @param iterations  Output: iterations
 * @param stored_key  Output: stored key (32 bytes)
 * @param server_key  Output: server key (32 bytes)
 * @return KEEL_OK on success
 */
keel_error_t keel_auth_scram_parse_hash(
    const char* hash,
    char** salt_out,
    int* iterations,
    uint8_t stored_key[32],
    uint8_t server_key[32]
);

/* ============================================================================
 * Built-in Provider Registration
 * ============================================================================ */

/**
 * @brief Get SCRAM-SHA-256 provider operations
 */
const keel_auth_provider_ops_t* keel_auth_scram_sha256_ops(void);

/**
 * @brief Get MD5 provider operations
 */
const keel_auth_provider_ops_t* keel_auth_md5_ops(void);

/**
 * @brief Get trust provider operations
 */
const keel_auth_provider_ops_t* keel_auth_trust_ops(void);

/**
 * @brief Get certificate identity provider operations.
 *
 * Extracts the TLS peer certificate CN as the username; no password
 * challenge is issued. Requires the session's TLS peer cert to be
 * populated before keel_auth_manager_authenticate() is called.
 */
const keel_auth_provider_ops_t* keel_auth_cert_ops(void);

/* ============================================================================
 * Enterprise Authentication Providers
 * ============================================================================ */

/**
 * @brief Configuration for the LDAP authentication provider.
 */
typedef struct keel_auth_ldap_config {
    const char* url;            /**< LDAP URL, e.g. "ldap://ldap.example.com:389" */
    const char* base_dn;        /**< Search base DN for user lookup */
    const char* bind_dn;        /**< Service-account DN for search bind (NULL = anon) */
    const char* bind_password;  /**< Service-account password (NULL = anon) */
    const char* search_filter;  /**< User search filter, %s replaced by username */
    const char* dn_suffix;      /**< DN suffix for direct bind: uid=%s,<dn_suffix> */
    bool        start_tls;      /**< Upgrade connection with STARTTLS */
    bool        tls_reqcert;    /**< Require valid server certificate */
    int         timeout_s;      /**< Connect/search timeout in seconds (default 5) */
} keel_auth_ldap_config_t;

/**
 * @brief Initialize an LDAP auth provider with the given config.
 *
 * @param provider  Provider instance (ops already set via keel_auth_ldap_ops)
 * @param config    Pointer to a keel_auth_ldap_config_t
 * @return KEEL_OK on success, error code if libldap is unavailable or config invalid.
 */
keel_error_t keel_auth_ldap_init(keel_auth_provider_t* provider,
                                  const keel_auth_ldap_config_t* config);

/**
 * @brief Get LDAP provider operations.
 *
 * Implements simple-bind authentication: connects to the LDAP server and
 * performs a DN bind with the supplied username/password.
 * Requires libldap (compile-time optional; returns reject ops when absent).
 */
const keel_auth_provider_ops_t* keel_auth_ldap_ops(void);

/**
 * @brief Configuration for the PAM authentication provider.
 */
typedef struct keel_auth_pam_config {
    const char* service_name;   /**< PAM service name (default "keel") */
} keel_auth_pam_config_t;

/**
 * @brief Initialize a PAM auth provider with the given config.
 *
 * @param provider  Provider instance
 * @param config    Pointer to a keel_auth_pam_config_t
 * @return KEEL_OK on success.
 */
keel_error_t keel_auth_pam_init(keel_auth_provider_t* provider,
                                 const keel_auth_pam_config_t* config);

/**
 * @brief Get PAM provider operations.
 *
 * Uses libpam to authenticate clients. The clear-text password challenge
 * (AuthenticationCleartextPassword) is sent to the client and the response
 * is forwarded to the PAM conversation function.
 * Requires libpam (compile-time optional; returns reject ops when absent).
 */
const keel_auth_provider_ops_t* keel_auth_pam_ops(void);

/**
 * @brief Configuration for the auth_query provider.
 *
 * auth_query executes a parameterized SQL query against a backend to retrieve
 * the stored password for a user. The password is then verified locally using
 * the configured upstream_method (default: scram-sha-256).
 *
 * Query must accept one parameter ($1 = username) and return one row with
 * one column containing the stored password hash in SCRAM or MD5 format.
 *
 * Example:
 *   SELECT password FROM auth.users WHERE username = $1
 */
typedef struct keel_auth_query_config {
    const char* query;          /**< SQL query (one $1 placeholder for username) */
    const char* conn_string;    /**< Connection string: host=... user=... dbname=... */
    keel_auth_method_t upstream_method; /**< Method used to verify the fetched hash */
    int         timeout_s;      /**< Query timeout in seconds (default 3) */
} keel_auth_query_config_t;

/**
 * @brief Initialize an auth_query provider with the given config.
 */
keel_error_t keel_auth_query_init(keel_auth_provider_t* provider,
                                   const keel_auth_query_config_t* config);

/**
 * @brief Get auth_query provider operations.
 *
 * Fetches the stored password from a backend database, then delegates to
 * the configured upstream_method provider for the actual challenge/response.
 */
const keel_auth_provider_ops_t* keel_auth_query_ops(void);

#endif /* KEEL_AUTH_H */
