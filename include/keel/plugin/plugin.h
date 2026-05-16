/**
 * @file plugin.h
 * @brief Core ↔ Plugin API Contract
 *
 * Extends the protocol flow vtable with the full plugin contract described
 * in the correctness-hardening design.  All new callbacks are OPTIONAL —
 * plugins that don't implement them set the function pointer to NULL and
 * core falls back to safe defaults.
 *
 * REQUIRED (already in keel_proto_flow_vtable_t):
 *   create_context, destroy_context, frame_len, on_fe_msg, on_be_msg,
 *   build_cleanup, backend_reuse_gate, generate_error
 *
 * NEW OPTIONAL:
 *   get_info, classify_error, capture_consistency_token,
 *   replica_reached_token, begin_stream, stream_write, end_stream,
 *   cleanup_slot, probe_backend, get_backend_metadata, get_metrics
 *
 * DESIGN:
 *   Rather than a second vtable, the new callbacks are added directly to
 *   keel_proto_flow_vtable_t. This keeps a single registration path and
 *   avoids dual-vtable dispatch overhead on the hot path.
 *
 * GUARANTEES:
 *   - If plugin_classify_client_message (on_fe_msg) returns potentially_stateful,
 *     core will NOT release backend until on_be_msg confirms/clears.
 *   - plugin_apply_state (build_state_sync) MUST be idempotent.
 *   - plugin_capture_consistency_token MUST be cheap (<1ms).
 *   - All plugin-owned strings (consistency_token, error messages) remain valid
 *     until the next call on the same context, or context destruction.
 */

#ifndef KEEL_PLUGIN_H
#define KEEL_PLUGIN_H

#include "keel/plugin/plugin_types.h"
#include "keel/protocol/protocol_flow.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Plugin Helper Macros
 * ============================================================================
 *
 * Check whether a specific optional callback is available.
 */

/**
 * @brief Check whether a specific optional plugin callback is registered.
 *
 * @param vtable   Protocol flow vtable pointer (may be NULL).
 * @param callback Name of the callback field to test.
 * @return Non-zero (true) if @p vtable is non-NULL and @p callback is non-NULL.
 */
#define KEEL_PLUGIN_HAS(vtable, callback) \
    ((vtable) != NULL && (vtable)->callback != NULL)

/**
 * @brief Call a plugin callback if available, otherwise return a fallback value.
 *
 * @param vtable   Protocol flow vtable pointer (may be NULL).
 * @param callback Name of the callback field to invoke.
 * @param fallback Expression to evaluate when the callback is absent.
 * @param ...      Arguments forwarded to the callback.
 * @return The callback's return value, or @p fallback.
 */
#define KEEL_PLUGIN_CALL_OR(vtable, callback, fallback, ...) \
    (KEEL_PLUGIN_HAS(vtable, callback)                       \
        ? (vtable)->callback(__VA_ARGS__)                    \
        : (fallback))

/* ============================================================================
 * Core-Side Plugin Utilities
 * ============================================================================ */

/**
 * @brief Query plugin capabilities.
 *
 * Returns the plugin's capability bitmask.  Core uses this to adapt behavior
 * (e.g., disable consistency token tracking if KEEL_PCAP_CONSISTENCY_TOKEN
 * is not set).
 *
 * @param vtable  Protocol flow vtable
 * @return Capabilities bitmask, or 0 if get_info is not implemented
 */
static inline keel_plugin_caps_t
keel_plugin_capabilities(const keel_proto_flow_vtable_t* vtable)
{
    if (!vtable || !vtable->get_info) return 0;
    keel_plugin_info_t info;
    vtable->get_info(&info);
    return info.capabilities;
}

/**
 * @brief Check if plugin supports a specific capability.
 *
 * @param vtable  Protocol flow vtable to inspect.
 * @param cap     Capability flag to test (e.g., `KEEL_PCAP_CONSISTENCY_TOKEN`).
 * @return `true` if the plugin reports support for @p cap.
 */
static inline bool
keel_plugin_has_cap(const keel_proto_flow_vtable_t* vtable, keel_plugin_cap_t cap)
{
    return (keel_plugin_capabilities(vtable) & cap) != 0;
}

/* ============================================================================
 * Core-Side Consistency Token Helpers
 * ============================================================================ */

/**
 * @brief Capture consistency token after a successful write.
 *
 * Core calls this after a write completes.  If the plugin supports it,
 * the token is stored in the session profile for subsequent read-after-write
 * consistency checks.
 *
 * @param vtable  Protocol flow vtable
 * @param ctx     Plugin context
 * @param be_fd   Backend file descriptor
 * @param out     Output token
 * @return 0 on success, -1 if not supported or error
 */
static inline int
keel_plugin_capture_token(const keel_proto_flow_vtable_t* vtable,
                         void* ctx, int be_fd,
                         keel_consistency_token_t* out)
{
    if (!vtable || !vtable->capture_consistency_token) return -1;
    return vtable->capture_consistency_token(ctx, be_fd, out);
}

/**
 * @brief Check if replica has reached the consistency token.
 *
 * @param vtable     Protocol flow vtable
 * @param ctx        Plugin context
 * @param replica_fd Replica backend file descriptor
 * @param token      Token to check against
 * @param timeout_ms Timeout (0 = just check, don't wait)
 * @param reached    Output: true if replica is caught up
 * @return 0 on success, -1 if not supported or error
 */
static inline int
keel_plugin_check_replica(const keel_proto_flow_vtable_t* vtable,
                         void* ctx, int replica_fd,
                         const keel_consistency_token_t* token,
                         int timeout_ms, bool* reached)
{
    if (!vtable || !vtable->replica_reached_token) {
        *reached = false;
        return -1;
    }
    return vtable->replica_reached_token(ctx, replica_fd, token, timeout_ms, reached);
}

/* ============================================================================
 * Core-Side Error Classification Helper
 * ============================================================================ */

/**
 * @brief Classify a server error frame.
 *
 * @param vtable Protocol flow vtable
 * @param ctx    Plugin context
 * @param data   Raw server frame data
 * @param len    Frame length
 * @param out    Output error info
 * @return 0 on success, -1 if not supported
 */
static inline int
keel_plugin_classify_error(const keel_proto_flow_vtable_t* vtable,
                          void* ctx,
                          const uint8_t* data, size_t len,
                          keel_error_info_t* out)
{
    if (!vtable || !vtable->classify_error) return -1;
    return vtable->classify_error(ctx, data, len, out);
}

/* ============================================================================
 * Core-Side Cleanup Helper
 * ============================================================================ */

/**
 * @brief Ask plugin to clean a returned backend slot.
 *
 * Core calls this instead of unconditionally sending DISCARD ALL.
 * If the plugin supports selective reset, it generates minimal
 * SET/RESET commands.  Falls back to build_cleanup() if not available.
 *
 * @param vtable   Protocol flow vtable.
 * @param ctx      Plugin context for this connection.
 * @param be_fd    Backend file descriptor being returned to the pool.
 * @param profile  Session state profile describing what must be reset.
 * @param opts     Cleanup options controlling the reset scope.
 * @param buf      Output buffer for the generated wire commands.
 * @param buf_len  Size of @p buf in bytes.
 * @return Bytes of wire commands placed in @p buf, or -1 on error.
 */
static inline ssize_t
keel_plugin_cleanup_slot(const keel_proto_flow_vtable_t* vtable,
                        void* ctx, int be_fd,
                        const struct state_profile* profile,
                        keel_cleanup_opts_t opts,
                        uint8_t* buf, size_t buf_len)
{
    if (vtable && vtable->cleanup_slot)
        return vtable->cleanup_slot(ctx, be_fd, profile, opts, buf, buf_len);
    /* Fallback: use existing build_cleanup with FULL mode */
    if (vtable && vtable->build_cleanup)
        return vtable->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, buf_len);
    return -1;
}

/* ============================================================================
 * Core-Side Probe Helper
 * ============================================================================ */

/**
 * @brief Probe a backend's health and replication status.
 *
 * Delegates to the plugin's probe_backend callback.  Returns -1 if
 * the plugin does not implement probing.
 *
 * @param vtable  Protocol flow vtable.
 * @param ctx     Plugin context.
 * @param be_fd   Backend file descriptor.
 * @param out     [out] Probe result (alive, role, lag).
 * @return 0 on success, -1 if unsupported or error.
 */
static inline int
keel_plugin_probe(const keel_proto_flow_vtable_t* vtable,
                 void* ctx, int be_fd,
                 keel_probe_result_t* out)
{
    if (!vtable || !vtable->probe_backend) return -1;
    return vtable->probe_backend(ctx, be_fd, out);
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PLUGIN_H */
