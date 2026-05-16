/**
 * @file session.c
 * @brief Session lifecycle helpers, validated state transitions, and teardown routines.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module owns the mechanics of turning a recycled `keel_session_t` into a
 * fresh connection context and then tearing it back down safely for reuse. The
 * code is intentionally simple because it sits under the worker and engine hot
 * path: no locks, fixed transition rules, explicit descriptor ownership, and a
 * strict cleanup sequence that clears every field capable of influencing pooling
 * safety or protocol behavior.
 */

#include "keel/session/session.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* ============================================================================
 * Time Utilities
 * ============================================================================ */

/**
 * @brief Return a monotonic nanosecond timestamp for session accounting.
 *
 * Monotonic time is used so idle and age calculations remain correct even if the
 * system wall clock jumps.
 *
 * @return Current `CLOCK_MONOTONIC` timestamp in nanoseconds.
 */
static uint64_t get_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * State Names
 * ============================================================================ */

static const char* state_names[] = {
    [KEEL_SESSION_INIT]           = "INIT",
    [KEEL_SESSION_STARTUP]        = "STARTUP",
    [KEEL_SESSION_AUTH]           = "AUTH",
    [KEEL_SESSION_BACKEND_CONNECT]= "BACKEND_CONNECT",
    [KEEL_SESSION_READY]          = "READY",
    [KEEL_SESSION_QUERY]          = "QUERY",
    [KEEL_SESSION_COPY]           = "COPY",
    [KEEL_SESSION_CLOSING]        = "CLOSING",
    [KEEL_SESSION_CLOSED]         = "CLOSED",
    /* Spec §8 granular states */
    [KEEL_SESSION_FE_READ]        = "FE_READ",
    [KEEL_SESSION_FE_CLASSIFY]    = "FE_CLASSIFY",
    [KEEL_SESSION_FE_WAIT_BACKEND]= "FE_WAIT_BACKEND",
    [KEEL_SESSION_BE_SYNC]        = "BE_SYNC",
    [KEEL_SESSION_STREAM_COPY]    = "STREAM_COPY",
    [KEEL_SESSION_STREAM_SPLICE]  = "STREAM_SPLICE",
    [KEEL_SESSION_HARD_PIN]       = "HARD_PIN",
};

/**
 * @brief Return a human-readable name string for a session state enum value.
 *
 * @param state Session state to name.
 * @return Constant string such as `"INIT"` or `"READY"`, or `"UNKNOWN"` if
 *         @p state is outside the valid range.
 */
const char* keel_session_state_name(keel_session_state_t state)
{
    if (state >= 0 && state < KEEL_SESSION_STATE_COUNT) {
        return state_names[state];
    }
    return "UNKNOWN";
}

/* ============================================================================
 * State Transition Validation
 * ============================================================================ */

/**
 * @brief Valid state transitions
 * 
 * INIT → STARTUP → AUTH → BACKEND_CONNECT → READY
 *                   ↓
 *                READY ↔ QUERY
 *                   ↓       ↓
 *                COPY   CLOSING
 *                   ↓       ↓
 *               CLOSING → CLOSED
 */
/**
 * @brief Validate a requested lifecycle transition against the session state graph.
 *
 * The graph is intentionally explicit instead of inferred from numeric ordering.
 * That keeps the allowed control-flow edges readable and makes impossible jumps
 * fail fast, which is more useful during debugging than quietly coercing state.
 *
 * @param from Current session state.
 * @param to Requested next state.
 * @return `true` if the transition is allowed by the runtime state machine.
 */
static bool is_valid_transition(keel_session_state_t from, keel_session_state_t to)
{
    switch (from) {
        case KEEL_SESSION_INIT:
            return to == KEEL_SESSION_STARTUP ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_STARTUP:
            return to == KEEL_SESSION_AUTH ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_AUTH:
            return to == KEEL_SESSION_BACKEND_CONNECT ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_BACKEND_CONNECT:
            return to == KEEL_SESSION_READY ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_READY:
            return to == KEEL_SESSION_QUERY ||
                   to == KEEL_SESSION_COPY ||
                   to == KEEL_SESSION_FE_READ ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_QUERY:
            return to == KEEL_SESSION_READY ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_COPY:
            return to == KEEL_SESSION_READY ||
                   to == KEEL_SESSION_CLOSING;
                   
        case KEEL_SESSION_CLOSING:
            return to == KEEL_SESSION_CLOSED;
            
        case KEEL_SESSION_CLOSED:
            /* Can transition back to INIT for session reuse */
            return to == KEEL_SESSION_INIT;

        /* --- Spec §8 Granular State Transitions --- */

        case KEEL_SESSION_FE_READ:
            return to == KEEL_SESSION_FE_CLASSIFY ||
                   to == KEEL_SESSION_CLOSING;

        case KEEL_SESSION_FE_CLASSIFY:
            return to == KEEL_SESSION_FE_WAIT_BACKEND ||
                   to == KEEL_SESSION_STREAM_SPLICE ||
                   to == KEEL_SESSION_STREAM_COPY ||
                   to == KEEL_SESSION_HARD_PIN ||
                   to == KEEL_SESSION_READY ||
                   to == KEEL_SESSION_CLOSING;

        case KEEL_SESSION_FE_WAIT_BACKEND:
            return to == KEEL_SESSION_BE_SYNC ||
                   to == KEEL_SESSION_STREAM_COPY ||
                   to == KEEL_SESSION_STREAM_SPLICE ||
                   to == KEEL_SESSION_CLOSING;

        case KEEL_SESSION_BE_SYNC:
            return to == KEEL_SESSION_STREAM_COPY ||
                   to == KEEL_SESSION_STREAM_SPLICE ||
                   to == KEEL_SESSION_CLOSING;

        case KEEL_SESSION_STREAM_COPY:
            return to == KEEL_SESSION_READY ||
                   to == KEEL_SESSION_FE_READ ||
                   to == KEEL_SESSION_CLOSING;

        case KEEL_SESSION_STREAM_SPLICE:
            return to == KEEL_SESSION_READY ||
                   to == KEEL_SESSION_FE_READ ||
                   to == KEEL_SESSION_CLOSING;

        case KEEL_SESSION_HARD_PIN:
            return to == KEEL_SESSION_FE_READ ||
                   to == KEEL_SESSION_STREAM_COPY ||
                   to == KEEL_SESSION_CLOSING;

        default:
            return false;
    }
}

/* ============================================================================
 * Session Initialization
 * ============================================================================ */

/**
 * @brief Initialize a recycled session slot for a new frontend connection.
 *
 * Preserves slab bookkeeping fields, zeroes everything else, then populates
 * the minimum state required for protocol negotiation on the accepted socket.
 * Residual buffers are reset and both creation and last-activity timestamps
 * are set to the current monotonic clock.
 *
 * @param session Session object to initialize (must be non-NULL).
 * @param client_fd Accepted frontend file descriptor (must be ≥ 0).
 * @return 0 on success, -1 if @p session is NULL or @p client_fd is negative.
 */
int keel_session_init(
    keel_session_t* session,
    int client_fd)
{
    if (session == NULL || client_fd < 0) {
        return -1;
    }
    
    /* Preserve allocator metadata that logically belongs to the slab, not the
     * frontend connection currently occupying this slot. */
    uint32_t slab_index = session->slab_index;
    keel_session_t* next_free = session->next_free;
    
    /* Reset all per-client state in one shot so a recycled slot cannot leak
     * protocol, authentication, or backend-borrow metadata from a prior user. */
    memset(session, 0, sizeof(*session));
    
    /* Reattach slab bookkeeping after the bulk reset. */
    session->slab_index = slab_index;
    session->next_free = next_free;
    
    /* Populate the minimum state required for the engine to start protocol
     * negotiation on the accepted frontend socket. */
    session->client_fd = client_fd;
    session->server_fd = -1;
    session->state    = KEEL_SESSION_INIT;
    session->mode     = KEEL_MODE_STARTUP;
    session->mode_c2s = KEEL_MODE_STARTUP;
    session->mode_s2c = KEEL_MODE_STARTUP;
    session->plugin_state = NULL;
    session->fast_forward_mode = 0;
    
    /* Residual buffers start empty because no bytes have yet been consumed from
     * either socket direction. */
    keel_residual_init(&session->client_residual);
    keel_residual_init(&session->server_residual);
    
    /* Creation time doubles as initial last-activity time. */
    session->created_at = get_time_ns();
    session->last_activity = session->created_at;
    
    return 0;
}

/* ============================================================================
 * State Management
 * ============================================================================ */

/**
 * @brief Advance the session to a new lifecycle state after graph validation.
 *
 * The transition is checked against the runtime state graph via
 * `is_valid_transition()`. Invalid edges are logged at DEBUG level and
 * rejected. On success the `last_activity` timestamp is updated.
 *
 * @param session Target session.
 * @param state Requested next state.
 * @return 0 on success, -1 if @p session is NULL or the transition is invalid.
 */
int keel_session_set_state(keel_session_t* session, keel_session_state_t state)
{
    if (session == NULL) {
        return -1;
    }
    
    if (!is_valid_transition(session->state, state)) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN, "Session %lu: invalid state transition %s → %s",
                    (unsigned long)session->id,
                    keel_session_state_name(session->state),
                    keel_session_state_name(state));
        return -1;
    }
    
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN, "Session %lu: state %s → %s",
                (unsigned long)session->id,
                keel_session_state_name(session->state),
                keel_session_state_name(state));
    
    /* Update the state only after validation and logging so diagnostics reflect
     * the actual edge that was traversed. */
    session->state = state;
    session->last_activity = get_time_ns();
    
    return 0;
}

/**
 * @brief Override the I/O transport mode for this session.
 *
 * The engine calls this when it determines the appropriate forwarding path
 * (analyze, peek, pipe, stream, or splice). Setting the mode here causes
 * `keel_session_get_mode()` to return it directly instead of deriving a
 * default from the coarse session state.
 *
 * @param session Target session.
 * @param mode New transport mode to apply.
 */
void keel_session_set_mode(keel_session_t* session, keel_mode_t mode)
{
    if (session != NULL) {
        session->mode     = mode;
        /* Sync per-direction modes so that direction-aware callers of
         * keel_session_get_mode() see the same value until overridden with
         * keel_session_set_mode_dir(). */
        session->mode_c2s = mode;
        session->mode_s2c = mode;
    }
}

void keel_session_set_mode_dir(keel_session_t* session,
                                keel_direction_t direction,
                                keel_mode_t mode)
{
    if (!session) return;
    if (direction == KEEL_DIR_CLIENT_TO_SERVER) {
        session->mode_c2s = mode;
    } else {
        session->mode_s2c = mode;
    }
}

/**
 * @brief Refresh the last-activity timestamp to the current monotonic clock.
 *
 * Called whenever a session produces or consumes meaningful I/O so that idle
 * detection and keepalive timers remain accurate.
 *
 * @param session Session to touch.
 */
void keel_session_touch(keel_session_t* session)
{
    if (session != NULL) {
        session->last_activity = get_time_ns();
    }
}

/**
 * @brief Test whether a session has been inactive longer than a given timeout.
 *
 * Uses `CLOCK_MONOTONIC` so the result is immune to wall-clock adjustments.
 *
 * @param session Session to check.
 * @param timeout_ns Inactivity threshold in nanoseconds.
 * @return `true` if the session's last-activity age exceeds @p timeout_ns,
 *         `false` if the session is NULL or still within the window.
 */
bool keel_session_is_idle(keel_session_t* session, uint64_t timeout_ns)
{
    if (session == NULL) {
        return false;
    }
    
    uint64_t now = get_time_ns();
    return (now - session->last_activity) > timeout_ns;
}

/* ============================================================================
 * Error Handling
 * ============================================================================ */

/**
 * @brief Record an error code and optional message on the session.
 *
 * The message is safely truncated to fit `session->error_msg` and is always
 * NUL-terminated. Passing NULL for @p msg clears the message field.
 *
 * @param session Target session.
 * @param code Numeric error code stored in `last_error`.
 * @param msg Human-readable error string, or NULL to clear.
 */
void keel_session_set_error(keel_session_t* session, int code, const char* msg)
{
    if (session == NULL) {
        return;
    }
    
    session->last_error = code;
    
    if (msg != NULL) {
        strncpy(session->error_msg, msg, sizeof(session->error_msg) - 1);
        session->error_msg[sizeof(session->error_msg) - 1] = '\0';
    } else {
        session->error_msg[0] = '\0';
    }
    
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN, "Session %lu: error %d - %s",
                (unsigned long)session->id, code, session->error_msg);
}

/* ============================================================================
 * Connection Management
 * ============================================================================ */

/**
 * @brief Close all file descriptors owned by a session and transition to CLOSED.
 *
 * Advances the state to CLOSING (if not already there) before closing the
 * frontend and backend file descriptors, then transitions to CLOSED. Pipe
 * pointers are cleared but pipe objects are not freed here — the worker that
 * owns the pipe pool is responsible for their lifecycle.
 *
 * @param session Session to close.
 */
void keel_session_close(keel_session_t* session)
{
    if (session == NULL) {
        return;
    }
    
    /* Transition to closing state if not already */
    if (session->state != KEEL_SESSION_CLOSING && 
        session->state != KEEL_SESSION_CLOSED) {
        keel_session_set_state(session, KEEL_SESSION_CLOSING);
    }
    
    /* Close file descriptors */
    if (session->client_fd >= 0) {
        close(session->client_fd);
        session->client_fd = -1;
    }
    
    if (session->server_fd >= 0) {
        close(session->server_fd);
        session->server_fd = -1;
    }
    
    /* Release pipes if allocated */
    /* Note: Pipe release should be done by worker since it owns the pool */
    session->c2s_pipe = NULL;
    session->s2c_pipe = NULL;
    
    /* Transition to closed */
    session->state = KEEL_SESSION_CLOSED;
}

/**
 * @brief Fully tear down a session and reset it to a reusable baseline.
 *
 * Calls `keel_session_close()` to shut down file descriptors, then clears
 * plugin state, residual buffers, user credentials, counters, pin metadata,
 * and the backend-connection pointer. After this call the slot is safe to
 * return to the free list.
 *
 * @param session Session to clean up.
 */
void keel_session_cleanup(keel_session_t* session)
{
    if (session == NULL) {
        return;
    }
    
    /* Ensure closed */
    keel_session_close(session);
    
    /* Destroy plugin context — owned by the flow vtable, freed via
     * keel_session_flow_destroy().  Clear the pointer so any stale
     * references are caught early. */
    session->plugin_state = NULL;
    session->fast_forward_mode = 0;
    
    /* Clear residual buffers */
    keel_residual_clear(&session->client_residual);
    keel_residual_clear(&session->server_residual);
    
    /* Clear user info */
    memset(session->username, 0, sizeof(session->username));
    memset(session->database, 0, sizeof(session->database));
    memset(session->error_msg, 0, sizeof(session->error_msg));
    
    /* Reset counters */
    session->query_count = 0;
    session->flags = 0;
    session->last_error = 0;

    if (session->state_profile) {
        keel_free(session->state_profile);
        session->state_profile = NULL;
    }
    session->pin_reason = 0;
    session->hard_pinned = false;
    session->state_hash = 0;
    session->in_transaction = false;
    session->backend_conn = NULL;
}

/* ============================================================================
 * Session Data Movement Helpers
 * ============================================================================ */

/**
 * @brief Determine I/O mode based on protocol and session state
 */
keel_mode_t keel_session_get_mode(keel_session_t* session, keel_direction_t direction)
{
    if (session == NULL) {
        return KEEL_MODE_ANALYZE;
    }

    /* Check for a direction-specific override first.  KEEL_MODE_STARTUP (== 0)
     * means "no override set" because it is the initial zero value and is never
     * a valid run-time mode for a live session. */
    if (direction == KEEL_DIR_CLIENT_TO_SERVER &&
            session->mode_c2s != KEEL_MODE_STARTUP) {
        return session->mode_c2s;
    }
    if (direction == KEEL_DIR_SERVER_TO_CLIENT &&
            session->mode_s2c != KEEL_MODE_STARTUP) {
        return session->mode_s2c;
    }

    /* If the flow already selected a non-startup mode, trust that decision.
     * This lets protocol-specific logic promote the session into pipe, stream,
     * or splice paths without re-deriving the choice here. */
    if (session->mode != KEEL_MODE_STARTUP) {
        return session->mode;
    }
    
    /* Otherwise derive a conservative default from coarse session state.
     * Analyze during setup, peek while idle/ready, and escalate to heavier
     * transport modes only once the session is actively moving query payloads. */
    switch (session->state) {
        case KEEL_SESSION_INIT:
        case KEEL_SESSION_STARTUP:
        case KEEL_SESSION_AUTH:
            return KEEL_MODE_ANALYZE;
            
        case KEEL_SESSION_READY:
            return KEEL_MODE_PEEK;
            
        case KEEL_SESSION_QUERY:
            return KEEL_MODE_PIPE;
            
        case KEEL_SESSION_COPY:
            return KEEL_MODE_STREAM;
            
        default:
            return KEEL_MODE_ANALYZE;
    }
}

/**
 * @brief Check if session can use zero-copy splice
 */
bool keel_session_can_splice(keel_session_t* session)
{
    if (session == NULL) {
        return false;
    }
    
    /* Zero-copy forwarding is only possible when both directional pipes exist.
     * A single missing pipe means the worker must stay on read/write paths. */
    if (session->c2s_pipe == NULL || session->s2c_pipe == NULL) {
        return false;
    }
    
    /* Check splice flag */
    return (session->flags & KEEL_SESSION_FLAG_SPLICE) != 0;
}

/**
 * @brief Enable splice mode for session
 */
void keel_session_enable_splice(keel_session_t* session, 
                               keel_pipe_t* c2s_pipe,
                               keel_pipe_t* s2c_pipe)
{
    if (session == NULL) {
        return;
    }
    
    session->c2s_pipe = c2s_pipe;
    session->s2c_pipe = s2c_pipe;
    
    if (c2s_pipe != NULL && s2c_pipe != NULL) {
        session->flags |= KEEL_SESSION_FLAG_SPLICE;
    } else {
        /* Partial pipe availability is treated as "splice disabled" because the
         * transport layer expects symmetric setup when it enters that fast path. */
        session->flags &= ~KEEL_SESSION_FLAG_SPLICE;
    }
}

/* ============================================================================
 * Session Statistics
 * ============================================================================ */

/**
 * @brief Increment the per-session query counter by one.
 *
 * Called by the engine each time a complete query cycle completes so that
 * session-level throughput metrics remain accurate.
 *
 * @param session Target session.
 */
void keel_session_increment_query_count(keel_session_t* session)
{
    if (session != NULL) {
        session->query_count++;
    }
}

/**
 * @brief Return the elapsed time since the session was created, in nanoseconds.
 *
 * Measured against `CLOCK_MONOTONIC` using the timestamp recorded in
 * `keel_session_init()`.
 *
 * @param session Target session.
 * @return Age in nanoseconds, or 0 if @p session is NULL.
 */
uint64_t keel_session_get_age_ns(keel_session_t* session)
{
    if (session == NULL) {
        return 0;
    }
    return get_time_ns() - session->created_at;
}

/**
 * @brief Return the elapsed time since the session last saw activity, in nanoseconds.
 *
 * Measures against the `last_activity` timestamp, which is updated by
 * `keel_session_touch()` and `keel_session_set_state()`.
 *
 * @param session Target session.
 * @return Idle duration in nanoseconds, or 0 if @p session is NULL.
 */
uint64_t keel_session_get_idle_ns(keel_session_t* session)
{
    if (session == NULL) {
        return 0;
    }
    return get_time_ns() - session->last_activity;
}
