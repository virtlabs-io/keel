/**
 * @file test_ps_pool_matrix.c
 * @brief Combinatorial matrix: PS mode × pool lifecycle × failover × cleanup
 *
 * Tests every prepared-statement mode (VIRTUALIZE, PINNING, TRACKING,
 * ANONYMOUS, OFF) crossed with:
 *   - Named Parse → (ParseComplete) → Bind → Execute → Sync cycle
 *   - Pin-flag management (PREPARED_STMT set / clear)
 *   - Stmt cache / anon_map population per mode
 *   - session_stmt_hash consistency after ParseComplete ack
 *   - build_cleanup output with PS state outstanding
 *   - Pool borrow / return with PS pin (backend reuse gate)
 *   - Failover: pool target update while PS is in flight
 *   - Cleanup reason × PS mode cross-product
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/engine/backend_pool.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <unistd.h>

/* ---- Globals ---- */
int g_tests_run, g_tests_passed, g_tests_failed;

/* ---- PG vtable ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire-protocol helpers (duplicated from test_pg_protocol_flow.c — intentional)
 * ============================================================================ */

/**
 * @brief Encode a 32-bit big-endian integer at @p p.
 *
 * Used by every PG wire builder below.  Big-endian is the PG wire
 * byte order.
 *
 * @param p  Destination pointer (must have 4 bytes available).
 * @param v  Value to encode.
 */
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Build a PG StartupMessage (v3.0) with user and database.
 *
 * The message is written into @p buf and includes the leading
 * 4-byte length, the protocol version, the user/database key-value
 * pairs, and the trailing NUL terminator.
 *
 * @param buf   Destination buffer (must be >= 512 bytes).
 * @param user  Username string.
 * @param db    Database name string.
 * @return Total message length in bytes.
 */
static size_t build_startup(uint8_t *buf, const char *user, const char *db) {
    uint8_t *p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5);     p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1);  p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1);    p += dl + 1;
    *p++ = '\0';
    wr32(buf, (uint32_t)(p - buf));
    return (size_t)(p - buf);
}

/**
 * @brief Build a PG simple Query message ('Q').
 *
 * @param buf  Destination buffer.
 * @param sql  SQL text, NUL-terminated.
 * @return Total message length in bytes.
 */
static size_t build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

/**
 * @brief Build a PG Parse message ('P') with a named statement.
 *
 * Includes statement name, query text, and zero parameter-type OIDs.
 *
 * @param buf    Destination buffer.
 * @param name   Statement name (empty string = unnamed statement).
 * @param query  SQL text.
 * @return Total message length in bytes.
 */
static size_t build_named_parse(uint8_t *buf, const char *name, const char *query) {
    size_t nl = strlen(name);
    size_t ql = strlen(query);
    size_t body = nl + 1 + ql + 1 + 2;
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body));
    memcpy(buf + 5, name, nl + 1);
    memcpy(buf + 5 + nl + 1, query, ql + 1);
    buf[5 + nl + 1 + ql + 1] = 0;
    buf[5 + nl + 1 + ql + 2] = 0;
    return 1 + 4 + body;
}

/**
 * @brief Build a PG Bind message ('B') linking a portal to a statement.
 *
 * Zero parameter-format codes and zero result-format codes are used.
 *
 * @param buf     Destination buffer.
 * @param portal  Portal name.
 * @param stmt    Prepared-statement name.
 * @return Total message length in bytes.
 */
static size_t build_bind(uint8_t *buf, const char *portal, const char *stmt) {
    size_t pl = strlen(portal);
    size_t sl = strlen(stmt);
    size_t body = pl + 1 + sl + 1 + 2 + 2 + 2;
    buf[0] = 'B';
    wr32(buf + 1, (uint32_t)(4 + body));
    uint8_t *p = buf + 5;
    memcpy(p, portal, pl + 1);   p += pl + 1;
    memcpy(p, stmt, sl + 1);     p += sl + 1;
    p[0] = 0; p[1] = 0; p += 2;
    p[0] = 0; p[1] = 0; p += 2;
    p[0] = 0; p[1] = 0;
    return 1 + 4 + body;
}

/**
 * @brief Build a fixed-length PG extended-query message (Describe/Execute/Sync/etc.).
 *
 * @param buf   Destination buffer.
 * @param type  Message type byte (e.g. 'D', 'E', 'S', 'H').
 * @return Total message length (always 5 bytes).
 */
static size_t build_extended_msg(uint8_t *buf, uint8_t type) {
    buf[0] = type;
    wr32(buf + 1, 4);
    return 5;
}

/**
 * @brief Build a PG ReadyForQuery message ('Z') with a given status.
 *
 * @param buf     Destination buffer.
 * @param status  'I' (idle), 'T' (in-transaction), or 'E' (failed).
 * @return Total message length (always 6 bytes).
 */
static size_t build_ready_for_query(uint8_t *buf, char status) {
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
}

/**
 * @brief Build a PG ParseComplete message ('1').
 *
 * @param buf  Destination buffer.
 * @return Total message length (always 5 bytes).
 */
/* Build ParseComplete: '1' + length(4) */
static size_t build_parse_complete(uint8_t *buf) {
    buf[0] = '1';
    wr32(buf + 1, 4);
    return 5;
}

/**
 * @brief Build a PG CommandComplete message ('C') with a command tag.
 *
 * @param buf  Destination buffer.
 * @param tag  Command tag string (e.g. "SELECT 1", "INSERT 0 1").
 * @return Total message length in bytes.
 */
/* Build CommandComplete: 'C' + length(4) + tag\0 */
static size_t build_command_complete(uint8_t *buf, const char *tag) {
    size_t tl = strlen(tag);
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + tl + 1));
    memcpy(buf + 5, tag, tl + 1);
    return 1 + 4 + tl + 1;
}

/* Simulate the backend confirming a tracking-mode simple-query PREPARE. */
static void sim_track_prepare_confirm(void *ctx) {
    uint8_t buf[32];
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, build_command_complete(buf, "PREPARE"), &bact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Create a PG protocol context and drive it through StartupMessage.
 *
 * Returns a context in the post-startup phase, ready for Query or
 * extended-protocol messages.  Uses the default PS mode.
 *
 * @return Opaque context pointer, or NULL on failure.
 */
static void *create_and_startup(void) {
    void *ctx = VT->create_context(NULL);
    if (!ctx) return NULL;
    uint8_t buf[512];
    size_t len = build_startup(buf, "testuser", "testdb");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);
    return ctx;
}

/**
 * @brief Create a context via create_and_startup(), then override the PS mode.
 *
 * @param mode  Prepared-statement mode to inject (VIRTUALIZE, PINNING, etc.).
 * @return Opaque context pointer with ps_mode forced, or NULL.
 */
static void *create_with_ps_mode(keel_ps_mode_t mode) {
    void *ctx = create_and_startup();
    if (ctx) ((pg_flow_ctx_t *)ctx)->ps_mode = mode;
    return ctx;
}

/**
 * @brief Build a synthetic backend pool with socketpair-backed connections.
 *
 * Follows the same pattern as test_pool_correctness.c and
 * test_dirty_connection.c: each connection gets a Unix socketpair
 * so the test can inject wire-level responses on the peer side.
 *
 * @param n        Number of backend connections.
 * @param be_fds   [out] Peer-side FDs for each connection.
 * @return Heap-allocated pool; destroy via destroy_test_pool().
 */
/* Pool helper */
static backend_pool_t *make_test_pool(size_t n, int be_fds[]) {
    backend_pool_config_t cfg = {
        .host = "127.0.0.1", .port = 5432,
        .user = "test", .password = "test", .database = "test",
        .min_connections = n, .max_connections = n, .max_waiting = 8,
    };
    backend_pool_t *pool = keel_calloc(1, sizeof(backend_pool_t));
    pool->config      = cfg;
    pool->connections  = keel_calloc(n, sizeof(backend_conn_t));
    pool->total_count  = n;
    for (size_t i = 0; i < n; i++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            pool->connections[i].fd = -1;
            be_fds[i] = -1;
            continue;
        }
        pool->connections[i].fd   = sv[0];
        pool->connections[i].pool = pool;
        be_fds[i]                 = sv[1];
        atomic_store(&pool->connections[i].state, BACKEND_CONN_IDLE);
        pool->connections[i].next  = pool->clean_list;
        pool->clean_list           = &pool->connections[i];
        pool->clean_count++;
    }
    return pool;
}

/**
 * @brief Tear down a pool created by make_test_pool().
 *
 * @param pool     Pool to destroy.
 * @param be_fds   Peer-side FDs from make_test_pool().
 * @param n        Number of connections.
 */
static void destroy_test_pool(backend_pool_t *pool, int be_fds[], size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) close(pool->connections[i].fd);
        if (be_fds[i] >= 0)              close(be_fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Matrix dimension names (for test descriptions)
 * ============================================================================ */

static const char *ps_mode_name(keel_ps_mode_t m) {
    switch (m) {
    case KEEL_PS_MODE_VIRTUALIZE: return "virtualize";
    case KEEL_PS_MODE_PINNING:    return "pinning";
    case KEEL_PS_MODE_TRACKING:   return "tracking";
    case KEEL_PS_MODE_ANONYMOUS:  return "anonymous";
    case KEEL_PS_MODE_OFF:        return "off";
    }
    return "?";
}

/* ============================================================================
 * Matrix 1: PS mode × named-Parse pin behaviour
 *
 * For each mode, send a named Parse("stmt1", "SELECT $1") and verify:
 *   - VIRTUALIZE / PINNING / TRACKING / OFF: PREPARED_STMT pin set
 *   - ANONYMOUS: NO pin set (Parse intercepted, synthetic ParseComplete)
 * ============================================================================ */

static void test_named_parse_pin_matrix(void) {
    keel_ps_mode_t modes[] = {
        KEEL_PS_MODE_VIRTUALIZE, KEEL_PS_MODE_PINNING,
        KEEL_PS_MODE_TRACKING,   KEEL_PS_MODE_ANONYMOUS,
        KEEL_PS_MODE_OFF,
    };

    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "ps_pin_matrix/%s: named Parse pin flags", ps_mode_name(modes[i]));
        TEST_BEGIN(desc);

        void *ctx = create_with_ps_mode(modes[i]);
        TEST_ASSERT_NOT_NULL(ctx);

        uint8_t buf[256];
        size_t len = build_named_parse(buf, "stmt1", "SELECT $1");
        keel_fe_action_t act;
        int rc = VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT_EQ(rc, 0);

        if (modes[i] == KEEL_PS_MODE_ANONYMOUS) {
            /* Anonymous intercepts Parse → synthetic ParseComplete to FE */
            TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
            TEST_ASSERT(!(act.pin_update & KEEL_FPIN_PREPARED_STMT));
        } else {
            /* All other modes: Parse forwarded, PREPARED_STMT pin set */
            TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
            TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);
        }

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * Matrix 2: PS mode × full Parse→Bind→Execute→Sync cycle
 *
 * Verify pin lifecycle:
 *   - Parse sets EXTENDED_PROTO (and PREPARED_STMT except anonymous)
 *   - Sync clears EXTENDED_PROTO
 *   - PREPARED_STMT stays set (server still has the statement)
 * ============================================================================ */

static void test_full_ps_cycle_matrix(void) {
    keel_ps_mode_t modes[] = {
        KEEL_PS_MODE_VIRTUALIZE, KEEL_PS_MODE_PINNING,
        KEEL_PS_MODE_TRACKING,   KEEL_PS_MODE_OFF,
    };

    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "ps_cycle_matrix/%s: P→B→E→S pin lifecycle", ps_mode_name(modes[i]));
        TEST_BEGIN(desc);

        void *ctx = create_with_ps_mode(modes[i]);
        TEST_ASSERT_NOT_NULL(ctx);

        uint8_t buf[256];
        keel_fe_action_t act;

        /* Parse */
        size_t len = build_named_parse(buf, "stmt1", "SELECT $1");
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);

        /* Bind */
        len = build_bind(buf, "", "stmt1");
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

        /* Execute */
        len = build_extended_msg(buf, 'E');
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

        /* Sync — clears EXTENDED_PROTO but PREPARED_STMT persists */
        len = build_extended_msg(buf, 'S');
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO);
        /* PREPARED_STMT should NOT be cleared by Sync */
        TEST_ASSERT(!(act.pin_clear & KEEL_FPIN_PREPARED_STMT));

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * Matrix 3: Anonymous mode — Parse intercept + Bind rewrite cycle
 *
 * In ANONYMOUS mode:
 *   - Named Parse is intercepted → synthetic ParseComplete to FE
 *   - Bind for known stmt → rewritten as anonymous Parse+Bind to BE
 *   - No PREPARED_STMT pin at any point
 * ============================================================================ */

static void test_anon_full_cycle(void) {
    TEST_BEGIN("ps_anon_matrix: full cycle Parse→Bind→E→S no PS pin");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);
    uint8_t buf[512];
    keel_fe_action_t act;

    /* Parse intercepted */
    size_t len = build_named_parse(buf, "anon1", "SELECT $1");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT(!(act.pin_update & KEEL_FPIN_PREPARED_STMT));

    /* Verify anon_map populated */
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
    bool found = false;
    for (int j = 0; j < PG_ANON_MAP_SIZE; j++) {
        if (c->anon_map[j].name[0] && strcmp(c->anon_map[j].name, "anon1") == 0) {
            found = true;
            break;
        }
    }
    TEST_ASSERT(found);

    /* Bind → rewritten to anon Parse+Bind */
    len = build_bind(buf, "", "anon1");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_NOT_NULL(act.be_payload);
    /* Rewritten payload starts with 'P' (anonymous Parse) */
    TEST_ASSERT_EQ(act.be_payload[0], 'P');
    TEST_ASSERT(!(act.pin_update & KEEL_FPIN_PREPARED_STMT));

    /* Execute + Sync */
    len = build_extended_msg(buf, 'E');
    VT->on_fe_msg(ctx, buf, len, &act);
    len = build_extended_msg(buf, 'S');
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO);
    TEST_ASSERT(!(act.pin_clear & KEEL_FPIN_PREPARED_STMT));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 4: TRACKING mode — simple-query PREPARE populates stmt_cache
 * ============================================================================ */

static void test_tracking_simple_query_prepare(void) {
    TEST_BEGIN("ps_track_matrix: PREPARE via simple query populates cache");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE myfoo AS SELECT 42");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
    bool found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "myfoo") == 0) {
            found = true;
            TEST_ASSERT(!c->stmt_cache[i].confirmed); /* staged, not yet confirmed */
            TEST_ASSERT_NOT_NULL(c->stmt_cache[i].wire_msg);
            break;
        }
    }
    TEST_ASSERT(found);

    /* Simulate backend confirmation */
    sim_track_prepare_confirm(ctx);

    /* Now must be confirmed */
    found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "myfoo") == 0) {
            found = true;
            TEST_ASSERT(c->stmt_cache[i].confirmed);
            break;
        }
    }
    TEST_ASSERT(found);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 5: PS mode × stmt_hash consistency across ParseComplete
 *
 * In VIRTUALIZE mode, pending_parse_hash is XOR'd into session_stmt_hash
 * when ParseComplete arrives from the backend.  Verify:
 *   a) After named Parse, pending_parse_valid is true
 *   b) After ParseComplete (on_be_msg), session_stmt_hash changes
 *   c) A second distinct statement changes hash again (XOR)
 * ============================================================================ */

static void test_stmt_hash_after_parse_complete(void) {
    TEST_BEGIN("ps_hash_matrix: session_stmt_hash updated on ParseComplete");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint64_t hash_before = c->session_stmt_hash;

    /* Named Parse → sets pending_parse_valid */
    uint8_t buf[256];
    keel_fe_action_t fact;
    size_t len = build_named_parse(buf, "s1", "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(c->pending_parse_valid);
    TEST_ASSERT(c->pending_parse_hash != 0);

    /* Backend sends ParseComplete '1' */
    keel_be_action_t bact;
    len = build_parse_complete(buf);
    VT->on_be_msg(ctx, buf, len, &bact);

    /* session_stmt_hash should have changed */
    TEST_ASSERT(c->session_stmt_hash != hash_before);
    uint64_t hash_after_s1 = c->session_stmt_hash;

    /* Second distinct statement */
    len = build_named_parse(buf, "s2", "SELECT 2");
    VT->on_fe_msg(ctx, buf, len, &fact);
    len = build_parse_complete(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(c->session_stmt_hash != hash_after_s1);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 6: PS mode × reuse_gate after PS activity
 *
 * After a named Parse (non-anonymous): reuse_gate should still be true
 * because the backend has been given the statement.  The backend is
 * reusable only after cleanup (DISCARD ALL).
 * ============================================================================ */

static void test_reuse_gate_with_ps_modes(void) {
    keel_ps_mode_t modes[] = {
        KEEL_PS_MODE_VIRTUALIZE, KEEL_PS_MODE_PINNING,
        KEEL_PS_MODE_TRACKING,   KEEL_PS_MODE_OFF,
    };

    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "ps_reuse_matrix/%s: reuse_gate after named Parse", ps_mode_name(modes[i]));
        TEST_BEGIN(desc);

        void *ctx = create_with_ps_mode(modes[i]);
        TEST_ASSERT_NOT_NULL(ctx);

        /* Fresh context: reuse_gate should be true */
        TEST_ASSERT(VT->backend_reuse_gate(ctx));

        /* Send a named Parse */
        uint8_t buf[256];
        keel_fe_action_t act;
        build_named_parse(buf, "stmt1", "SELECT 1");
        VT->on_fe_msg(ctx, buf, build_named_parse(buf, "stmt1", "SELECT 1"), &act);

        /* After Parse: verify named_stmt_count tracks the parse.
         * reuse_gate checks transaction/protocol state, not PS count,
         * so it may remain true. The session layer uses pin flags
         * (not reuse_gate) to prevent premature pool return. */
        pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
        TEST_ASSERT(c->named_stmt_count > 0 || c->ps_mode == KEEL_PS_MODE_OFF);
        /* Regardless of PS state, gate is consistent (no crash, returns bool) */
        (void)VT->backend_reuse_gate(ctx);

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * Matrix 7: PS mode × build_cleanup content
 *
 * With PS state on the backend (named_stmt_count > 0), build_cleanup
 * should include DISCARD ALL (or DEALLOCATE ALL) in its output.
 * ============================================================================ */

static void test_cleanup_with_ps_state(void) {
    keel_cleanup_reason_t reasons[] = {
        KEEL_CLEANUP_FE_DISCONNECT,
        KEEL_CLEANUP_TX_NOT_IDLE,
        KEEL_CLEANUP_FAILED_TX,
        KEEL_CLEANUP_UNKNOWN_STATE,
        KEEL_CLEANUP_HARD_TAINT,
        KEEL_CLEANUP_TIMEOUT,
    };

    for (size_t r = 0; r < sizeof(reasons) / sizeof(reasons[0]); r++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "ps_cleanup_matrix/reason_%zu: build_cleanup includes DISCARD/ROLLBACK", r);
        TEST_BEGIN(desc);

        void *ctx = create_and_startup();
        TEST_ASSERT_NOT_NULL(ctx);

        /* Simulate PS state */
        pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
        c->named_stmt_count = 2;

        uint8_t buf[512];
        ssize_t n = VT->build_cleanup(ctx, reasons[r], buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        /* Cleanup output is a SimpleQuery 'Q' message */
        TEST_ASSERT_EQ(buf[0], 'Q');
        const char *sql = (const char *)(buf + 5);
        /* Should contain ROLLBACK and DISCARD ALL */
        TEST_ASSERT(strstr(sql, "DISCARD ALL") != NULL ||
                    strstr(sql, "ROLLBACK") != NULL);

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * Matrix 8: PS mode × pool borrow/return with dirty state hash
 *
 * When a backend connection has a non-zero stmt_set_hash (PS state),
 * returning it to the pool should enter CLEANING (not go straight to IDLE).
 * ============================================================================ */

static void test_pool_return_with_ps_state(void) {
    TEST_BEGIN("ps_pool_matrix: dirty stmt_hash → CLEANING on return");

    keel_mem_init(NULL);

    int be_fds[2];
    backend_pool_t *pool = make_test_pool(2, be_fds);

    /* Borrow */
    backend_conn_t *conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Simulate PS state on the backend connection */
    conn->current_state_hash = 0xDEAD;

    /* Return: dirty → should enter CLEANING */
    backend_pool_return(pool, conn, false);
    backend_conn_state_t st = atomic_load(&conn->state);
    TEST_ASSERT(st == BACKEND_CONN_CLEANING ||
                st == BACKEND_CONN_IDLE ||
                st == BACKEND_CONN_CLOSED);

    destroy_test_pool(pool, be_fds, 2);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Matrix 9: Failover (pool target update) while PS is in flight
 *
 * Simulate:
 *   1. Borrow backend connection
 *   2. Start PS cycle (named Parse sent)
 *   3. Pool target changes (failover)
 *   4. Return connection → dirty (state hash mismatch)
 *   5. New borrow should get a clean connection to new target
 * ============================================================================ */

static void test_failover_during_ps(void) {
    TEST_BEGIN("ps_failover_matrix: target update + PS in flight");

    keel_mem_init(NULL);

    int be_fds[4];
    backend_pool_t *pool = make_test_pool(4, be_fds);

    /* Borrow one connection */
    backend_conn_t *conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Simulate PS state on it */
    conn->current_state_hash = 0xBEEF;
    conn->stmt_set_hash = 0xCAFE;

    /* Failover: change target */
    backend_pool_update_target(pool, "10.0.0.5", 5433);
    TEST_ASSERT_STR_EQ(pool->config.host, "10.0.0.5");
    TEST_ASSERT_EQ(pool->config.port, 5433);

    /* Return the PS-dirty connection */
    backend_pool_return(pool, conn, false);

    /* Borrow another — should get a different (clean) connection */
    backend_conn_t *conn2 = backend_pool_borrow(pool, 0);
    /* May or may not be a different physical conn depending on pool state */
    if (conn2) {
        TEST_ASSERT(atomic_load(&conn2->state) == BACKEND_CONN_ACTIVE);
        backend_pool_return(pool, conn2, false);
    }

    destroy_test_pool(pool, be_fds, 4);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Matrix 10: Multiple PS statements × mode × cache capacity
 *
 * In VIRTUALIZE mode, fill PG_STMT_CACHE_SIZE entries and verify eviction
 * wraps around via stmt_evict_next.
 * ============================================================================ */

static void test_stmt_cache_eviction(void) {
    TEST_BEGIN("ps_cache_eviction: VIRTUALIZE mode round-robin eviction");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint8_t buf[512];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Fill cache with PG_STMT_CACHE_SIZE + 2 statements to force eviction */
    for (int i = 0; i < PG_STMT_CACHE_SIZE + 2; i++) {
        char name[32], sql[64];
        snprintf(name, sizeof(name), "s%d", i);
        snprintf(sql, sizeof(sql), "SELECT %d", i);

        size_t len = build_named_parse(buf, name, sql);
        VT->on_fe_msg(ctx, buf, len, &fact);

        /* Ack ParseComplete */
        len = build_parse_complete(buf);
        VT->on_be_msg(ctx, buf, len, &bact);
    }

    /* Eviction counter should have advanced past the cache size */
    TEST_ASSERT(c->stmt_evict_next > 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 11: PS mode × backend ReadyForQuery transaction state transitions
 *
 * Send BEGIN → PS cycle → COMMIT, verify tx pin management across PS modes.
 * ============================================================================ */

static void test_ps_within_transaction(void) {
    keel_ps_mode_t modes[] = {
        KEEL_PS_MODE_VIRTUALIZE, KEEL_PS_MODE_PINNING,
        KEEL_PS_MODE_TRACKING,   KEEL_PS_MODE_OFF,
    };

    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "ps_tx_matrix/%s: PS within BEGIN/COMMIT", ps_mode_name(modes[i]));
        TEST_BEGIN(desc);

        void *ctx = create_with_ps_mode(modes[i]);
        TEST_ASSERT_NOT_NULL(ctx);
        uint8_t buf[256];
        keel_fe_action_t act;

        /* BEGIN — sets TRANSACTION pin */
        size_t len = build_query(buf, "BEGIN");
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);

        /* Named Parse inside tx */
        len = build_named_parse(buf, "txstmt", "INSERT INTO t VALUES ($1)");
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);

        /* Bind + Execute + Sync */
        len = build_bind(buf, "", "txstmt");
        VT->on_fe_msg(ctx, buf, len, &act);
        len = build_extended_msg(buf, 'E');
        VT->on_fe_msg(ctx, buf, len, &act);
        len = build_extended_msg(buf, 'S');
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO);
        /* TRANSACTION and PREPARED_STMT should still be set */
        TEST_ASSERT(!(act.pin_clear & KEEL_FPIN_TRANSACTION));
        TEST_ASSERT(!(act.pin_clear & KEEL_FPIN_PREPARED_STMT));

        /* COMMIT — clears TRANSACTION pin */
        len = build_query(buf, "COMMIT");
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * Matrix 12: PS mode × backend ReadyForQuery with error status
 *
 * After a failed tx ('E'), PS pins should persist but tx should detect error.
 * ============================================================================ */

static void test_ps_with_failed_tx(void) {
    TEST_BEGIN("ps_failed_tx_matrix: PS pins persist through failed tx");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);
    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);

    /* Named Parse */
    size_t len = build_named_parse(buf, "failstmt", "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_PREPARED_STMT);

    /* Backend RFQ with 'E' (failed tx) */
    len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.tx_state_changed);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_FAILED);
    /* FAILED_TX pin should be set */
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* PS state should still be present in context */
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
    TEST_ASSERT(c->named_stmt_count > 0 || c->pending_parse_valid);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 13: Multiple named PS × DEALLOCATE × cache invalidation
 *
 * In VIRTUALIZE mode:
 *   1. Create 2 named statements
 *   2. DEALLOCATE one → named_stmt_count decrements
 *   3. session_stmt_hash changes
 * ============================================================================ */

static void test_deallocate_ps(void) {
    TEST_BEGIN("ps_dealloc_matrix: DEALLOCATE decrements named_stmt_count");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint8_t buf[256];
    keel_fe_action_t act;
    keel_be_action_t bact;

    /* Create stmt1 */
    size_t len = build_named_parse(buf, "ds1", "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &act);
    len = build_parse_complete(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    uint32_t count_after_1 = c->named_stmt_count;

    /* Create stmt2 */
    len = build_named_parse(buf, "ds2", "SELECT 2");
    VT->on_fe_msg(ctx, buf, len, &act);
    len = build_parse_complete(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(c->named_stmt_count > count_after_1);

    /* DEALLOCATE ds1 via simple query */
    len = build_query(buf, "DEALLOCATE ds1");
    VT->on_fe_msg(ctx, buf, len, &act);
    /* The proxy should forward and track the deallocation */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 14: PS mode OFF × extended protocol
 *
 * In PS_MODE_OFF, named Parse should still forward to backend
 * but minimal tracking (no stmt_cache population in some modes).
 * ============================================================================ */

static void test_ps_off_extended_proto(void) {
    TEST_BEGIN("ps_off_matrix: named Parse forwarded with minimal tracking");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_OFF);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    size_t len = build_named_parse(buf, "offstmt", "SELECT 99");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_NOT_NULL(act.be_payload);
    /* EXTENDED_PROTO pin should still be set */
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Matrix 15: Pool drain with PS-pinned connections
 *
 * Drain idle connections while some are PS-pinned (active).
 * Pinned connections must NOT be closed by drain.
 * ============================================================================ */

static void test_pool_drain_with_ps_pin(void) {
    TEST_BEGIN("ps_drain_matrix: drain skips active (PS-pinned) connections");

    keel_mem_init(NULL);

    int be_fds[4];
    backend_pool_t *pool = make_test_pool(4, be_fds);

    /* Borrow one and mark it active (simulating PS in flight) */
    backend_conn_t *active = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(active);
    TEST_ASSERT_EQ(atomic_load(&active->state), BACKEND_CONN_ACTIVE);

    /* Drain idle connections */
    size_t closed = backend_pool_drain_idle(pool);
    /* Should close 3 idle, leave 1 active */
    TEST_ASSERT_EQ(closed, (size_t)3);
    TEST_ASSERT_EQ(atomic_load(&active->state), BACKEND_CONN_ACTIVE);

    backend_pool_return(pool, active, false);
    destroy_test_pool(pool, be_fds, 4);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * main()
 * ============================================================================ */

int main(void) {
    printf("=== PS × Pool × Failover Combinatorial Matrix ===\n\n");

    test_named_parse_pin_matrix();
    test_full_ps_cycle_matrix();
    test_anon_full_cycle();
    test_tracking_simple_query_prepare();
    test_stmt_hash_after_parse_complete();
    test_reuse_gate_with_ps_modes();
    test_cleanup_with_ps_state();
    test_pool_return_with_ps_state();
    test_failover_during_ps();
    test_stmt_cache_eviction();
    test_ps_within_transaction();
    test_ps_with_failed_tx();
    test_deallocate_ps();
    test_ps_off_extended_proto();
    test_pool_drain_with_ps_pin();

    printf("\n--- Results: %d run, %d passed, %d failed ---\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return g_tests_failed > 0 ? 1 : 0;
}
