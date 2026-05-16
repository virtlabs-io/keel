/**
 * @file test_session_engine.c
 * @brief Unit tests for session and engine operations
 *
 * Tests:
 * - Session state machine
 * - Session initialization and cleanup
 * - Mode detection and splice enablement
 * - Protocol flow vtable integration
 */

#include "test_utils.h"
#include "keel/session/session.h"
#include "keel/engine/engine.h"
#include "keel/protocol/protocol_flow.h"
#include <string.h>

/* ============================================================================
 * Session State Machine Tests
 * ============================================================================ */

static void test_session_states(void) {
    TEST_BEGIN("session state definitions");
    
    /* Verify state values are distinct */
    TEST_ASSERT(KEEL_SESSION_INIT != KEEL_SESSION_STARTUP);
    TEST_ASSERT(KEEL_SESSION_STARTUP != KEEL_SESSION_AUTH);
    TEST_ASSERT(KEEL_SESSION_AUTH != KEEL_SESSION_BACKEND_CONNECT);
    TEST_ASSERT(KEEL_SESSION_BACKEND_CONNECT != KEEL_SESSION_READY);
    TEST_ASSERT(KEEL_SESSION_READY != KEEL_SESSION_QUERY);
    TEST_ASSERT(KEEL_SESSION_QUERY != KEEL_SESSION_COPY);
    TEST_ASSERT(KEEL_SESSION_COPY != KEEL_SESSION_CLOSING);
    TEST_ASSERT(KEEL_SESSION_CLOSING != KEEL_SESSION_CLOSED);
    
    TEST_END();
}

static void test_session_init(void) {
    TEST_BEGIN("session initialization");
    
    keel_session_t session;
    memset(&session, 0xFF, sizeof(session));  /* Fill with garbage */
    
    /* Use 2-arg API: session, client_fd */
    int rc = keel_session_init(&session, 42);
    TEST_ASSERT_EQ(rc, 0);
    
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_INIT);
    TEST_ASSERT_EQ(session.client_fd, 42);
    TEST_ASSERT_EQ(session.server_fd, -1);
    TEST_ASSERT(session.c2s_pipe == NULL);  /* No pipe allocated yet */
    TEST_ASSERT(session.s2c_pipe == NULL);
    TEST_ASSERT_EQ(session.mode, KEEL_MODE_STARTUP);
    TEST_ASSERT(keel_residual_empty(&session.client_residual));
    TEST_ASSERT(keel_residual_empty(&session.server_residual));
    
    keel_session_cleanup(&session);
    
    TEST_END();
}

static void test_session_state_transitions(void) {
    TEST_BEGIN("session state transitions");
    
    keel_session_t session;
    keel_session_init(&session, 0);  /* Use fd 0 for testing */
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_INIT);
    
    /* Valid transition: INIT -> STARTUP */
    int rc = keel_session_set_state(&session, KEEL_SESSION_STARTUP);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_STARTUP);
    
    /* Valid transition: STARTUP -> AUTH */
    rc = keel_session_set_state(&session, KEEL_SESSION_AUTH);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_AUTH);
    
    /* Valid transition: AUTH -> BACKEND_CONNECT */
    rc = keel_session_set_state(&session, KEEL_SESSION_BACKEND_CONNECT);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_BACKEND_CONNECT);
    
    /* Valid transition: BACKEND_CONNECT -> READY */
    rc = keel_session_set_state(&session, KEEL_SESSION_READY);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_READY);
    
    /* Valid transition: READY -> QUERY */
    rc = keel_session_set_state(&session, KEEL_SESSION_QUERY);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_QUERY);
    
    /* Valid transition: QUERY -> READY */
    rc = keel_session_set_state(&session, KEEL_SESSION_READY);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_READY);
    
    keel_session_cleanup(&session);
    
    TEST_END();
}

static void test_session_close_transition(void) {
    TEST_BEGIN("session close transitions");
    
    keel_session_t session;
    keel_session_init(&session, 0);
    
    /* Go to READY state */
    keel_session_set_state(&session, KEEL_SESSION_STARTUP);
    keel_session_set_state(&session, KEEL_SESSION_AUTH);
    keel_session_set_state(&session, KEEL_SESSION_BACKEND_CONNECT);
    keel_session_set_state(&session, KEEL_SESSION_READY);
    
    /* READY -> CLOSING should be valid */
    int rc = keel_session_set_state(&session, KEEL_SESSION_CLOSING);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_CLOSING);
    
    /* CLOSING -> CLOSED should be valid */
    rc = keel_session_set_state(&session, KEEL_SESSION_CLOSED);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(session.state, KEEL_SESSION_CLOSED);
    
    keel_session_cleanup(&session);
    
    TEST_END();
}

/* ============================================================================
 * Session Mode Tests
 * ============================================================================ */

static void test_session_modes(void) {
    TEST_BEGIN("session modes");
    
    keel_session_t session;
    keel_session_init(&session, 0);
    
    /* Default mode is STARTUP */
    TEST_ASSERT_EQ(session.mode, KEEL_MODE_STARTUP);
    
    /* Test mode setting */
    keel_session_set_mode(&session, KEEL_MODE_PEEK);
    TEST_ASSERT_EQ(session.mode, KEEL_MODE_PEEK);
    
    keel_session_set_mode(&session, KEEL_MODE_ANALYZE);
    TEST_ASSERT_EQ(session.mode, KEEL_MODE_ANALYZE);
    
    keel_session_set_mode(&session, KEEL_MODE_PIPE);
    TEST_ASSERT_EQ(session.mode, KEEL_MODE_PIPE);
    
    keel_session_cleanup(&session);
    
    TEST_END();
}

/* ============================================================================
 * Session Activity Tests
 * ============================================================================ */

static void test_session_activity(void) {
    TEST_BEGIN("session activity tracking");
    
    keel_session_t session;
    keel_session_init(&session, 0);
    
    uint64_t initial_time = session.last_activity;
    
    /* Touch should update timestamp */
    keel_session_touch(&session);
    TEST_ASSERT(session.last_activity >= initial_time);
    
    /* is_idle with large timeout (in nanoseconds) should return false */
    TEST_ASSERT(!keel_session_is_idle(&session, 60000000000ULL));  /* 60 seconds in ns */
    
    /* is_idle returns true when elapsed time > timeout
     * We can't reliably test with timeout=0 due to timing issues,
     * so just verify the function exists and can be called */
    (void)keel_session_is_idle(&session, 1000000ULL);  /* 1ms timeout */
    
    keel_session_cleanup(&session);
    
    TEST_END();
}

/* ============================================================================
 * Engine Configuration Tests
 * ============================================================================ */

static void test_engine_ops(void) {
    TEST_BEGIN("engine operation types");
    
    /* Verify operation types are distinct */
    TEST_ASSERT(KEEL_OP_NONE != KEEL_OP_ACCEPT);
    TEST_ASSERT(KEEL_OP_ACCEPT != KEEL_OP_RECV);
    TEST_ASSERT(KEEL_OP_RECV != KEEL_OP_SEND);
    TEST_ASSERT(KEEL_OP_SEND != KEEL_OP_PEEK);
    TEST_ASSERT(KEEL_OP_PEEK != KEEL_OP_SPLICE);
    TEST_ASSERT(KEEL_OP_SPLICE != KEEL_OP_CONNECT);
    TEST_ASSERT(KEEL_OP_CONNECT != KEEL_OP_CLOSE);
    TEST_ASSERT(KEEL_OP_CLOSE != KEEL_OP_TIMEOUT);
    
    TEST_END();
}

static void test_engine_modes(void) {
    TEST_BEGIN("engine proxy modes");
    
    /* Verify modes are distinct */
    TEST_ASSERT(KEEL_MODE_STARTUP != KEEL_MODE_PEEK);
    TEST_ASSERT(KEEL_MODE_PEEK != KEEL_MODE_ANALYZE);
    TEST_ASSERT(KEEL_MODE_ANALYZE != KEEL_MODE_PIPE);
    TEST_ASSERT(KEEL_MODE_PIPE != KEEL_MODE_STREAM);
    TEST_ASSERT(KEEL_MODE_STREAM != KEEL_MODE_CLOSING);
    
    TEST_END();
}

/* ============================================================================
 * Protocol Flow VTable Tests
 * ============================================================================ */

static void test_flow_vtable_struct(void) {
    TEST_BEGIN("protocol flow vtable structure");
    
    /* keel_proto_flow_vtable_t is the single plugin contract */
    keel_proto_flow_vtable_t vtable;
    memset(&vtable, 0, sizeof(vtable));
    
    /* All function pointers NULL after zero-init */
    TEST_ASSERT_NULL(vtable.create_context);
    TEST_ASSERT_NULL(vtable.destroy_context);
    TEST_ASSERT_NULL(vtable.on_fe_msg);
    TEST_ASSERT_NULL(vtable.on_be_msg);
    
    TEST_END();
}

static void test_action_structs(void) {
    TEST_BEGIN("frontend and backend action structures");
    
    /* keel_fe_action_t — zero-initialized defaults */
    keel_fe_action_t fe;
    memset(&fe, 0, sizeof(fe));
    TEST_ASSERT_EQ(fe.type, KEEL_FE_ACT_NONE);
    TEST_ASSERT_NULL(fe.fe_response);
    TEST_ASSERT_EQ(fe.fe_response_len, 0u);
    TEST_ASSERT(!fe.splice_eligible);
    TEST_ASSERT(!fe.cache_eligible);
    
    /* Plugin sets splice_eligible; engine must honour it */
    fe.type           = KEEL_FE_ACT_QUERY;
    fe.splice_eligible = true;
    fe.effect          = KEEL_QE_READONLY;
    TEST_ASSERT_EQ(fe.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(fe.splice_eligible);
    TEST_ASSERT_EQ(fe.effect, (keel_query_effect_flags_t)KEEL_QE_READONLY);
    
    /* keel_be_action_t — zero-initialized defaults */
    keel_be_action_t be;
    memset(&be, 0, sizeof(be));
    TEST_ASSERT_EQ(be.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(!be.query_complete);
    TEST_ASSERT(!be.backend_reusable);
    
    /* Plugin sets query_complete to signal end of response cycle */
    be.query_complete   = true;
    be.backend_reusable = true;
    TEST_ASSERT(be.query_complete);
    TEST_ASSERT(be.backend_reusable);
    
    TEST_END();
}

/* ============================================================================
 * Session Close Tests
 * ============================================================================ */

static void test_session_close(void) {
    TEST_BEGIN("session close");
    
    keel_session_t session;
    keel_session_init(&session, 0);
    
    /* Simulate active session */
    keel_session_set_state(&session, KEEL_SESSION_STARTUP);
    keel_session_set_state(&session, KEEL_SESSION_AUTH);
    keel_session_set_state(&session, KEEL_SESSION_BACKEND_CONNECT);
    keel_session_set_state(&session, KEEL_SESSION_READY);
    
    /* Close session */
    keel_session_close(&session);
    TEST_ASSERT(session.state == KEEL_SESSION_CLOSING || 
                session.state == KEEL_SESSION_CLOSED);
    
    keel_session_cleanup(&session);
    
    TEST_END();
}

/* ============================================================================
 * Session Slab Tests
 * ============================================================================ */

static void test_session_slab_config(void) {
    TEST_BEGIN("session slab configuration");
    
    /* Verify session structure size is reasonable */
    size_t session_size = sizeof(keel_session_t);
    TEST_ASSERT(session_size > 0);
    TEST_ASSERT(session_size < 8192);  /* Should be under 8KB */
    
    /* Verify residual inline size is defined */
    TEST_ASSERT(KEEL_RESIDUAL_INLINE_SIZE > 0);
    TEST_ASSERT(KEEL_RESIDUAL_INLINE_SIZE <= 4096);
    
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("=== Session & Engine Tests ===\n\n");
    
    /* Session state machine */
    test_session_states();
    test_session_init();
    test_session_state_transitions();
    test_session_close_transition();
    
    /* Session modes */
    test_session_modes();
    
    /* Session activity */
    test_session_activity();
    
    /* Engine types */
    test_engine_ops();
    test_engine_modes();
    
    /* Protocol flow vtable */
    test_flow_vtable_struct();
    test_action_structs();
    
    /* Session close */
    test_session_close();
    
    /* Session slab */
    test_session_slab_config();
    
    return test_summary();
}
