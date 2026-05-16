/**
 * @file protocol.h
 * @brief Legacy protocol abstraction types shared between engine and protocol implementations.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This header predates the newer protocol-flow vtable but still defines a useful
 * vocabulary shared across the codebase: protocol kinds, auth methods, protocol-
 * level states, parsed message/query metadata, and callback-style abstractions for
 * frontend/backend interactions. In practice, newer code tends to use
 * `protocol_flow.h` for the main engine contract and `protocol.h` for common type
 * definitions that should remain protocol-neutral.
 */

#ifndef KEEL_PROTOCOL_H
#define KEEL_PROTOCOL_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/mem/mem.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations for compatibility */
typedef struct keel_session keel_session_fwd_t;

/* Forward declarations */
typedef struct keel_proto_frontend keel_proto_frontend_t;
typedef struct keel_proto_backend keel_proto_backend_t;
typedef struct keel_proto_message keel_proto_message_t;
typedef struct keel_proto_query keel_proto_query_t;

/* ============================================================================
 * Protocol Types
 * ============================================================================ */

/**
 * @brief Supported database protocols
 */
typedef enum keel_proto_type {
    KEEL_PROTO_UNKNOWN = 0,
    KEEL_PROTO_POSTGRESQL,
    KEEL_PROTO_MYSQL,
    KEEL_PROTO_MARIADB,
    KEEL_PROTO_ORACLE,
    /* Future protocols */
} keel_proto_type_t;

/**
 * @brief Protocol version
 */
typedef struct keel_proto_version {
    uint16_t major;
    uint16_t minor;
} keel_proto_version_t;

/**
 * @brief Authentication methods
 */
typedef enum keel_auth_method {
    KEEL_AUTH_NONE = 0,          /**< No authentication */
    KEEL_AUTH_TRUST,             /**< Trust (no password) */
    KEEL_AUTH_PASSWORD,          /**< Clear text password */
    KEEL_AUTH_MD5,               /**< MD5 password hash */
    KEEL_AUTH_SCRAM_SHA_256,     /**< SCRAM-SHA-256 */
    KEEL_AUTH_GSSAPI,            /**< Kerberos/GSSAPI */
    KEEL_AUTH_SSPI,              /**< SSPI (Windows) */
    KEEL_AUTH_CERTIFICATE,       /**< TLS client certificate */
    KEEL_AUTH_LDAP,              /**< LDAP */
    KEEL_AUTH_RADIUS,            /**< RADIUS */
    KEEL_AUTH_PAM,               /**< PAM */
    KEEL_AUTH_QUERY,             /**< auth_query: verify via SQL query to a backend */
    KEEL_AUTH_REJECT,            /**< Always reject */
    KEEL_AUTH_PASSTHROUGH,       /**< Forward credentials to backend (relay mode) */
} keel_auth_method_t;

/**
 * @brief Connection state (protocol-level)
 */
typedef enum keel_proto_state {
    KEEL_PROTO_STATE_INIT = 0,
    KEEL_PROTO_STATE_STARTUP,
    KEEL_PROTO_STATE_AUTH,
    KEEL_PROTO_STATE_READY,
    KEEL_PROTO_STATE_QUERY,
    KEEL_PROTO_STATE_COPY,
    KEEL_PROTO_STATE_ERROR,
    KEEL_PROTO_STATE_CLOSING,
} keel_proto_state_t;

/**
 * @brief Query type classification
 */
typedef enum keel_query_type {
    KEEL_QUERY_UNKNOWN = 0,
    
    /* Read queries */
    KEEL_QUERY_SELECT,
    KEEL_QUERY_SHOW,
    KEEL_QUERY_EXPLAIN,
    
    /* Write queries */
    KEEL_QUERY_INSERT,
    KEEL_QUERY_UPDATE,
    KEEL_QUERY_DELETE,
    KEEL_QUERY_TRUNCATE,
    
    /* DDL */
    KEEL_QUERY_CREATE,
    KEEL_QUERY_ALTER,
    KEEL_QUERY_DROP,
    
    /* Transaction control */
    KEEL_QUERY_BEGIN,
    KEEL_QUERY_COMMIT,
    KEEL_QUERY_ROLLBACK,
    KEEL_QUERY_SAVEPOINT,
    
    /* Session control */
    KEEL_QUERY_SET,
    KEEL_QUERY_RESET,
    KEEL_QUERY_DISCARD,
    
    /* Prepared statements */
    KEEL_QUERY_PREPARE,
    KEEL_QUERY_EXECUTE,
    KEEL_QUERY_DEALLOCATE,
    
    /* Copy */
    KEEL_QUERY_COPY,
    
    /* Other */
    KEEL_QUERY_CALL,
    KEEL_QUERY_DO,

    /* DML: MERGE/UPSERT */
    KEEL_QUERY_MERGE,             /**< MERGE / UPSERT */

    /* Admin / maintenance */
    KEEL_QUERY_MAINTENANCE,       /**< VACUUM, REINDEX, CLUSTER */
    KEEL_QUERY_LOCK,              /**< LOCK TABLE */
    KEEL_QUERY_LISTEN_NOTIFY,     /**< LISTEN or NOTIFY */
    KEEL_QUERY_UNLISTEN,           /**< UNLISTEN — releases LISTEN pin */
} keel_query_type_t;

/**
 * @brief Query flags
 */
typedef enum keel_query_flags {
    KEEL_QUERY_FLAG_NONE         = 0,
    KEEL_QUERY_FLAG_READ_ONLY    = (1 << 0),  /**< Query is read-only */
    KEEL_QUERY_FLAG_WRITE        = (1 << 1),  /**< Query modifies data */
    KEEL_QUERY_FLAG_DDL          = (1 << 2),  /**< Query is DDL */
    KEEL_QUERY_FLAG_TRANSACTION  = (1 << 3),  /**< Query affects transaction */
    KEEL_QUERY_FLAG_SESSION      = (1 << 4),  /**< Query affects session state */
    KEEL_QUERY_FLAG_MULTI        = (1 << 5),  /**< Multiple statements */
    KEEL_QUERY_FLAG_CACHEABLE    = (1 << 6),  /**< Query result is cacheable */
} keel_query_flags_t;

/* ============================================================================
 * Protocol Message
 * ============================================================================ */

/**
 * @brief Protocol message (parsed)
 */
struct keel_proto_message {
    uint8_t     type;           /**< Message type code */
    size_t      length;         /**< Total message length */
    const void* data;           /**< Message data (after type/length) */
    size_t      data_len;       /**< Data length */
};

/* ============================================================================
 * Query Information
 * ============================================================================ */

/**
 * @brief Parsed query information
 */
struct keel_proto_query {
    keel_str_t       sql;            /**< Original SQL text */
    keel_query_type_t type;          /**< Query type */
    uint32_t        flags;          /**< Query flags */
    
    /* For prepared statements */
    keel_str_t       stmt_name;      /**< Statement name (if prepared) */
    
    /* For COPY */
    bool            copy_in;        /**< COPY IN (client sends data) */
    
    /* Routing hints */
    bool            needs_primary;  /**< Must go to primary */
    bool            in_transaction; /**< Part of transaction */
};

/* ============================================================================
 * Protocol Operations Interface
 * ============================================================================ */

/**
 * @brief Frontend callbacks
 */
typedef struct keel_proto_frontend_callbacks {
    /**
     * @brief Client sent startup/authentication data
     */
    void (*on_startup)(
        keel_proto_frontend_t* frontend,
        const char*           user,
        const char*           database,
        void*                 user_data
    );
    
    /**
     * @brief Client completed authentication
     */
    void (*on_auth_complete)(
        keel_proto_frontend_t* frontend,
        keel_error_t           status,
        void*                 user_data
    );
    
    /**
     * @brief Client sent a query
     */
    void (*on_query)(
        keel_proto_frontend_t*   frontend,
        const keel_proto_query_t* query,
        void*                   user_data
    );
    
    /**
     * @brief Parse request (extended protocol)
     */
    void (*on_parse)(
        keel_proto_frontend_t* frontend,
        keel_str_t             stmt_name,
        keel_str_t             sql,
        void*                 user_data
    );
    
    /**
     * @brief Bind request (extended protocol)
     */
    void (*on_bind)(
        keel_proto_frontend_t* frontend,
        keel_str_t             portal_name,
        keel_str_t             stmt_name,
        const void*           params,
        size_t                params_len,
        void*                 user_data
    );
    
    /**
     * @brief Execute request (extended protocol)
     */
    void (*on_execute)(
        keel_proto_frontend_t* frontend,
        keel_str_t             portal_name,
        size_t                max_rows,
        void*                 user_data
    );
    
    /**
     * @brief Describe request
     */
    void (*on_describe)(
        keel_proto_frontend_t* frontend,
        char                  type,  /* 'S' = statement, 'P' = portal */
        keel_str_t             name,
        void*                 user_data
    );
    
    /**
     * @brief Sync request
     */
    void (*on_sync)(
        keel_proto_frontend_t* frontend,
        void*                 user_data
    );
    
    /**
     * @brief Close request
     */
    void (*on_close)(
        keel_proto_frontend_t* frontend,
        char                  type,
        keel_str_t             name,
        void*                 user_data
    );
    
    /**
     * @brief COPY data from client
     */
    void (*on_copy_data)(
        keel_proto_frontend_t* frontend,
        const void*           data,
        size_t                len,
        void*                 user_data
    );
    
    /**
     * @brief COPY done from client
     */
    void (*on_copy_done)(
        keel_proto_frontend_t* frontend,
        void*                 user_data
    );
    
    /**
     * @brief COPY failed from client
     */
    void (*on_copy_fail)(
        keel_proto_frontend_t* frontend,
        keel_str_t             error,
        void*                 user_data
    );
    
    /**
     * @brief Client terminated connection
     */
    void (*on_terminate)(
        keel_proto_frontend_t* frontend,
        void*                 user_data
    );
    
    /**
     * @brief Protocol error
     */
    void (*on_error)(
        keel_proto_frontend_t* frontend,
        keel_error_t           error,
        const char*           message,
        void*                 user_data
    );
    
    void* user_data;
    
} keel_proto_frontend_callbacks_t;

/**
 * @brief Backend callbacks
 */
typedef struct keel_proto_backend_callbacks {
    /**
     * @brief Connected to server
     */
    void (*on_connect)(
        keel_proto_backend_t* backend,
        keel_error_t          status,
        void*                user_data
    );
    
    /**
     * @brief Authentication required
     */
    void (*on_auth_request)(
        keel_proto_backend_t* backend,
        keel_auth_method_t    method,
        const void*          data,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief Authentication complete
     */
    void (*on_auth_complete)(
        keel_proto_backend_t* backend,
        keel_error_t          status,
        void*                user_data
    );
    
    /**
     * @brief Backend is ready for queries
     */
    void (*on_ready)(
        keel_proto_backend_t* backend,
        char                 tx_status,  /* 'I', 'T', 'E' */
        void*                user_data
    );
    
    /**
     * @brief Row description received
     */
    void (*on_row_description)(
        keel_proto_backend_t* backend,
        const void*          desc,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief Data row received
     */
    void (*on_data_row)(
        keel_proto_backend_t* backend,
        const void*          row,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief Command complete
     */
    void (*on_command_complete)(
        keel_proto_backend_t* backend,
        keel_str_t            tag,
        void*                user_data
    );
    
    /**
     * @brief Parse complete
     */
    void (*on_parse_complete)(
        keel_proto_backend_t* backend,
        void*                user_data
    );
    
    /**
     * @brief Bind complete
     */
    void (*on_bind_complete)(
        keel_proto_backend_t* backend,
        void*                user_data
    );
    
    /**
     * @brief Close complete
     */
    void (*on_close_complete)(
        keel_proto_backend_t* backend,
        void*                user_data
    );
    
    /**
     * @brief No data (empty result)
     */
    void (*on_no_data)(
        keel_proto_backend_t* backend,
        void*                user_data
    );
    
    /**
     * @brief Ready for COPY IN
     */
    void (*on_copy_in)(
        keel_proto_backend_t* backend,
        const void*          format_info,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief COPY OUT data
     */
    void (*on_copy_out_data)(
        keel_proto_backend_t* backend,
        const void*          data,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief COPY done
     */
    void (*on_copy_done)(
        keel_proto_backend_t* backend,
        void*                user_data
    );
    
    /**
     * @brief Error response from server
     */
    void (*on_error_response)(
        keel_proto_backend_t* backend,
        const void*          error_fields,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief Notice from server
     */
    void (*on_notice)(
        keel_proto_backend_t* backend,
        const void*          notice_fields,
        size_t               len,
        void*                user_data
    );
    
    /**
     * @brief Parameter status change
     */
    void (*on_parameter_status)(
        keel_proto_backend_t* backend,
        keel_str_t            name,
        keel_str_t            value,
        void*                user_data
    );
    
    /**
     * @brief Backend key data (for cancel)
     */
    void (*on_backend_key)(
        keel_proto_backend_t* backend,
        uint32_t             pid,
        uint32_t             key,
        void*                user_data
    );
    
    /**
     * @brief Connection lost
     */
    void (*on_disconnect)(
        keel_proto_backend_t* backend,
        keel_error_t          reason,
        void*                user_data
    );
    
    void* user_data;
    
} keel_proto_backend_callbacks_t;

/* ============================================================================
 * Protocol Implementation Interface
 * ============================================================================ */

/*
 * NOTE: The old keel_proto_impl_t vtable has been removed.
 * The new protocol interface is defined in <keel/protocol/protocol_vtable.h>.
 * See keel_protocol_vtable_t for the VTable-based protocol abstraction.
 */

/* ============================================================================
 * Query Type Utilities
 * ============================================================================ */

/**
 * @brief Check if query type is read-only
 */
KEEL_CONST bool keel_query_type_is_read(keel_query_type_t type);

/**
 * @brief Check if query type modifies data
 */
KEEL_CONST bool keel_query_type_is_write(keel_query_type_t type);

/**
 * @brief Check if query type is DDL
 */
KEEL_CONST bool keel_query_type_is_ddl(keel_query_type_t type);

/**
 * @brief Check if query type affects transaction
 */
KEEL_CONST bool keel_query_type_is_transaction(keel_query_type_t type);

/**
 * @brief Get query type name
 */
KEEL_PURE const char* keel_query_type_name(keel_query_type_t type);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PROTOCOL_H */
