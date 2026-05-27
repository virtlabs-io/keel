/**
 * @file frontend_plugin.h
 * @brief Frontend plugin contract boundary.
 *
 * A frontend owns wire/protocol behavior.  It decodes client messages, chooses
 * the configured parser, applies protocol-specific error handling, and encodes
 * responses.  SQL grammar and language semantics belong in parser plugins.
 */

#ifndef KEEL_FRONTEND_PLUGIN_H
#define KEEL_FRONTEND_PLUGIN_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct keel_client_conn keel_client_conn_t;
typedef struct keel_frontend_msg keel_frontend_msg_t;
typedef struct keel_backend_msg keel_backend_msg_t;

typedef struct keel_frontend_plugin_ops {
    const char* name;
    const char* version;

    int (*init)(const void* config);

    int (*on_accept)(keel_client_conn_t* client);
    int (*decode_message)(keel_client_conn_t* client, keel_frontend_msg_t* msg);
    int (*handle_message)(keel_client_conn_t* client, keel_frontend_msg_t* msg);
    int (*encode_response)(keel_client_conn_t* client, keel_backend_msg_t* msg);

    void (*on_close)(keel_client_conn_t* client);
    void (*shutdown)(void);
} keel_frontend_plugin_ops_t;

#ifdef __cplusplus
}
#endif

#endif /* KEEL_FRONTEND_PLUGIN_H */
