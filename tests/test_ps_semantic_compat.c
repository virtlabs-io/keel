/**
 * @file test_ps_semantic_compat.c
 * @brief Phase 4 acceptance gate tests: prepared-statement semantic compatibility.
 *
 * Exercises the following invariants for VIRTUALIZE mode (extended protocol)
 * and for the cross-backend pool compatibility predicate:
 *
 *   - DDL bumps schema_epoch and clears stmt_set_hash (VIRTUALIZE).
 *   - DROP + CREATE on the same table name each bump schema_epoch (VIRTUALIZE).
 *   - SET search_path changes semantic_profile_hash and search_path_hash (VIRTUALIZE).
 *   - SET ROLE changes semantic_profile_hash and role_hash (VIRTUALIZE).
 *   - DISCARD PLANS clears stmt_set_hash and bumps schema_epoch (VIRTUALIZE).
 *   - Unknown utility (CALL) marks semantic_unknown (VIRTUALIZE).
 *   - Unnamed Parse ("") does not contribute to session_stmt_hash (VIRTUALIZE).
 *   - backend_pool_stmt_compatible() rejects mismatched semantic profiles even
 *     when stmt_set_hash values are identical.
 *
 * All VIRTUALIZE mode tests use only the public vtable API and white-box
 * inspection of pg_flow_ctx_t (via postgres_flow_internal.h), mirroring the
 * approach used in test_pg_protocol_flow.c.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/engine/backend_pool.h"
#include <string.h>
#include <stdio.h>

/* Public vtable for the PostgreSQL flow plugin. */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire-protocol message builders (subset needed by these tests)
 * ============================================================================ */

static inline void wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/* Protocol V3 StartupMessage with user + database */
static size_t build_startup(uint8_t* buf, const char* user, const char* db) {
    uint8_t* p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5); p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1); p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1); p += dl + 1;
    *p++ = '\0';
    uint32_t total = (uint32_t)(p - buf);
    wr32(buf, total);
    return total;
}

/* Simple Query 'Q' */
static size_t build_query(uint8_t* buf, const char* sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

/* Extended protocol Parse 'P': type(1) + len(4) + name\0 + query\0 + numparams(2) */
static size_t build_named_parse(uint8_t* buf, const char* name, const char* query) {
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

/* ============================================================================
 * Test fixture helpers
 * ============================================================================ */

/**
 * @brief Create a startup-complete context using the given PS mode.
 *
 * Matches the create_with_ps_mode() helper from test_pg_protocol_flow.c.
 */
static void* create_with_ps_mode(keel_ps_mode_t mode) {
    void* ctx = VT->create_context(NULL);
    if (!ctx) return NULL;

    uint8_t buf[512];
    size_t len = build_startup(buf, "testuser", "testdb");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    ((pg_flow_ctx_t*)ctx)->ps_mode = mode;
    return ctx;
}

/**
 * @brief Confirm an in-flight extended-protocol Parse by delivering a backend
 *        ParseComplete ('1') message through on_be_msg.
 *
 * For VIRTUALIZE mode, the backend returns '1' after accepting the Parse.
 * This is the minimal confirmation path — no CommandComplete or ReadyForQuery
 * is needed because those belong to the full query cycle, not the Parse phase.
 */
static void sim_virt_parse_confirm(void* ctx) {
    uint8_t buf[8];
    keel_be_action_t bact;
    buf[0] = '1';
    wr32(buf + 1, 4);
    VT->on_be_msg(ctx, buf, 5, &bact);
}

/* ============================================================================
 * VIRTUALIZE mode — extended protocol semantic tests
 * ============================================================================ */

/**
 * Test 1: DDL after a confirmed named Parse invalidates the stmt set and bumps
 * the schema epoch.  Mirrors test_ps_tracking_ddl_invalidates_stmt_set but
 * exercises the VIRTUALIZE (extended protocol 'P') code path.
 */
static void test_virt_ddl_invalidates_stmt_set(void) {
    TEST_BEGIN("ps_mode/virtualize: DDL invalidates stmt set and bumps schema epoch");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Parse + confirm — session_stmt_hash must be non-zero afterwards */
    size_t len = build_named_parse(buf, "q1", "SELECT id FROM users WHERE id = $1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);
    TEST_ASSERT(!before.semantic_unknown);

    /* DDL: ALTER TABLE touches the schema */
    len = build_query(buf, "ALTER TABLE users ADD COLUMN created_at timestamptz");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(after.stmt_set_hash, 0ULL);
    TEST_ASSERT(after.schema_epoch > before.schema_epoch);
    TEST_ASSERT(!after.semantic_unknown);

    VT->destroy_context(ctx);
    TEST_END();
}

/**
 * Test 2: DROP TABLE then CREATE TABLE on the same name each bump schema_epoch
 * independently.  Two DDL operations must produce two distinct epochs so that
 * a connection that was valid after DROP cannot be reused after CREATE.
 */
static void test_virt_drop_create_same_name_invalidates(void) {
    TEST_BEGIN("ps_mode/virtualize: DROP + CREATE same name bumps epoch twice");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Parse + confirm */
    size_t len = build_named_parse(buf, "qdc", "SELECT * FROM items");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);

    keel_stmt_compat_profile_t p0;
    memset(&p0, 0, sizeof(p0));
    rc = VT->get_stmt_compat_profile(ctx, &p0);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(p0.stmt_set_hash != 0);

    /* DROP TABLE */
    len = build_query(buf, "DROP TABLE items");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t p1;
    memset(&p1, 0, sizeof(p1));
    rc = VT->get_stmt_compat_profile(ctx, &p1);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(p1.stmt_set_hash, 0ULL);
    TEST_ASSERT(p1.schema_epoch > p0.schema_epoch);

    /* CREATE TABLE — another DDL on the same name */
    len = build_query(buf, "CREATE TABLE items (id bigint PRIMARY KEY)");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t p2;
    memset(&p2, 0, sizeof(p2));
    rc = VT->get_stmt_compat_profile(ctx, &p2);
    TEST_ASSERT_EQ(rc, 0);
    /* CREATE is also DDL — schema_epoch must advance again */
    TEST_ASSERT(p2.schema_epoch > p1.schema_epoch);
    TEST_ASSERT_EQ(p2.stmt_set_hash, 0ULL);

    VT->destroy_context(ctx);
    TEST_END();
}

/**
 * Test 3: Changing search_path after a confirmed Parse must alter
 * both search_path_hash and semantic_profile_hash.  stmt_set_hash also
 * changes because context_sig is folded into each entry hash.
 */
static void test_virt_search_path_change_updates_profile(void) {
    TEST_BEGIN("ps_mode/virtualize: SET search_path changes semantic profile");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Parse + confirm */
    size_t len = build_named_parse(buf, "qsp", "SELECT now()");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    /* Change search_path */
    len = build_query(buf, "SET search_path TO tenant_a, public");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT(after.search_path_hash != before.search_path_hash);
    TEST_ASSERT(after.semantic_profile_hash != before.semantic_profile_hash);
    /* role_hash must remain unchanged — only search_path moved */
    TEST_ASSERT_EQ(after.role_hash, before.role_hash);
    /* stmt_set_hash changes because context_sig is folded in */
    TEST_ASSERT(after.stmt_set_hash != before.stmt_set_hash);
    /* schema_epoch is unaffected by a SET command */
    TEST_ASSERT_EQ(after.schema_epoch, before.schema_epoch);
    TEST_ASSERT(!after.semantic_unknown);

    VT->destroy_context(ctx);
    TEST_END();
}

/**
 * Test 4: SET ROLE changes role_hash and semantic_profile_hash.
 * search_path_hash must remain untouched.
 */
static void test_virt_set_role_updates_profile(void) {
    TEST_BEGIN("ps_mode/virtualize: SET ROLE changes semantic profile");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Parse + confirm */
    size_t len = build_named_parse(buf, "qrole", "SELECT current_user");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    /* Change role */
    len = build_query(buf, "SET ROLE app_reader");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT(after.role_hash != before.role_hash);
    TEST_ASSERT(after.semantic_profile_hash != before.semantic_profile_hash);
    /* search_path must be unaffected */
    TEST_ASSERT_EQ(after.search_path_hash, before.search_path_hash);
    /* schema_epoch is unaffected */
    TEST_ASSERT_EQ(after.schema_epoch, before.schema_epoch);
    TEST_ASSERT(!after.semantic_unknown);

    VT->destroy_context(ctx);
    TEST_END();
}

/**
 * Test 5: DISCARD PLANS clears all prepared-statement state, zeroes
 * stmt_set_hash, and bumps schema_epoch.  This matches the behaviour
 * verified for TRACKING mode in test_pg_protocol_flow.c but exercises
 * VIRTUALIZE's extended-protocol code path.
 */
static void test_virt_discard_plans_clears_stmt_hash(void) {
    TEST_BEGIN("ps_mode/virtualize: DISCARD PLANS zeroes stmt_set_hash");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Parse + confirm */
    size_t len = build_named_parse(buf, "qdp", "SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    /* DISCARD PLANS */
    len = build_query(buf, "DISCARD PLANS");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(after.stmt_set_hash, 0ULL);
    TEST_ASSERT(after.schema_epoch > before.schema_epoch);
    TEST_ASSERT(!after.semantic_unknown);

    VT->destroy_context(ctx);
    TEST_END();
}

/**
 * Test 6: An unknown utility command (CALL) marks semantic_unknown and
 * zeroes stmt_set_hash.  A connection in this state cannot be reused for
 * any other session.
 */
static void test_virt_unknown_utility_marks_semantic_unknown(void) {
    TEST_BEGIN("ps_mode/virtualize: unknown utility (CALL) marks semantic_unknown");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Parse + confirm */
    size_t len = build_named_parse(buf, "qunk", "SELECT 42");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);
    TEST_ASSERT(!before.semantic_unknown);

    /* CALL — classified as unknown utility */
    len = build_query(buf, "CALL do_work()");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(after.semantic_unknown);
    TEST_ASSERT_EQ(after.stmt_set_hash, 0ULL);

    VT->destroy_context(ctx);
    TEST_END();
}

/**
 * Test 7: An unnamed Parse message (stmt_name == "") must not affect
 * session_stmt_hash.  Unnamed statements are transient and are never
 * replayed to a new backend — including them in the session hash would
 * cause spurious hash mismatches.
 */
static void test_virt_unnamed_parse_skips_session_hash(void) {
    TEST_BEGIN("ps_mode/virtualize: unnamed Parse does not affect session_stmt_hash");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_VIRTUALIZE);
    TEST_ASSERT_NOT_NULL(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT_EQ(c->session_stmt_hash, 0ULL);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Send unnamed Parse ("") */
    size_t len = build_named_parse(buf, "", "SELECT version()");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    /* session_stmt_hash must remain zero — unnamed stmt has no pending parse */
    TEST_ASSERT_EQ(c->session_stmt_hash, 0ULL);
    /* pending_parse_valid must not be set (or pending_parse_hash must be 0) */
    TEST_ASSERT(!c->pending_parse_valid || c->pending_parse_hash == 0);

    /* Confirm that a subsequent named Parse *does* register */
    len = build_named_parse(buf, "named1", "SELECT 1");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_virt_parse_confirm(ctx);
    TEST_ASSERT(c->session_stmt_hash != 0ULL);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Cross-backend pool semantic-compatibility tests
 * ============================================================================ */

/**
 * Test 8: backend_pool_stmt_compatible() must reject a backend whose
 * semantic profile differs from the session's required profile, even if
 * stmt_set_hash matches.  This validates the Phase 4 gate: a reuse
 * candidate with the right statement set but a different context
 * (different role, search_path, schema_epoch, or GUC hash) must not
 * be borrowed.
 */
static void test_pool_compat_mismatch_profile_rejected(void) {
    TEST_BEGIN("pool/compat: matching stmt_set_hash + mismatched profile rejected");

    /* Baseline profile representing the current session */
    keel_stmt_compat_profile_t session = {
        .stmt_set_hash         = 0xDEADBEEFCAFE0001ULL,
        .semantic_profile_hash = 0x1111222233334444ULL,
        .schema_epoch          = 7,
        .role_hash             = 0xAAAABBBBCCCCDDDDULL,
        .search_path_hash      = 0x0101020203030404ULL,
        .guc_hash              = 0xFEDCBA9876543210ULL,
        .semantic_unknown      = false,
    };

    /* Candidate connection with a fully matching profile */
    backend_conn_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.stmt_set_hash = session.stmt_set_hash;
    conn.stmt_profile  = session;

    /* Exact match must succeed */
    TEST_ASSERT(backend_pool_stmt_compatible(&session, &conn));

    /* Mismatched schema_epoch: same stmt hash, different DDL epoch */
    conn.stmt_profile.schema_epoch = session.schema_epoch + 1;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));
    conn.stmt_profile.schema_epoch = session.schema_epoch;   /* restore */

    /* Mismatched role_hash */
    conn.stmt_profile.role_hash ^= 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));
    conn.stmt_profile.role_hash = session.role_hash;

    /* Mismatched search_path_hash */
    conn.stmt_profile.search_path_hash ^= 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));
    conn.stmt_profile.search_path_hash = session.search_path_hash;

    /* Mismatched guc_hash */
    conn.stmt_profile.guc_hash ^= 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));
    conn.stmt_profile.guc_hash = session.guc_hash;

    /* Mismatched semantic_profile_hash alone (e.g. combined hash differs) */
    conn.stmt_profile.semantic_profile_hash ^= 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));
    conn.stmt_profile.semantic_profile_hash = session.semantic_profile_hash;

    /* semantic_unknown on the backend side always fails */
    conn.stmt_profile.semantic_unknown = true;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));
    conn.stmt_profile.semantic_unknown = false;

    /* semantic_unknown in the required profile also always fails */
    keel_stmt_compat_profile_t unknown_session = session;
    unknown_session.semantic_unknown = true;
    TEST_ASSERT(!backend_pool_stmt_compatible(&unknown_session, &conn));

    /* After restoring all fields, compatibility is re-established */
    TEST_ASSERT(backend_pool_stmt_compatible(&session, &conn));

    TEST_END();
}

/**
 * Test 9: stmt_set_hash itself must differ between the session and the backend
 * for the predicate to fail.  Confirms the fast-path check in
 * backend_pool_stmt_compatible().
 */
static void test_pool_compat_mismatched_stmt_hash_rejected(void) {
    TEST_BEGIN("pool/compat: mismatched stmt_set_hash rejected");

    keel_stmt_compat_profile_t session = {
        .stmt_set_hash         = 0xAAAAAAAABBBBBBBBULL,
        .semantic_profile_hash = 0x1234567890ABCDEFULL,
        .schema_epoch          = 1,
        .role_hash             = 0x1111111122222222ULL,
        .search_path_hash      = 0x3333333344444444ULL,
        .guc_hash              = 0x5555555566666666ULL,
        .semantic_unknown      = false,
    };

    backend_conn_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.stmt_profile = session;

    /* stmt_set_hash on the conn differs from session */
    conn.stmt_set_hash = session.stmt_set_hash ^ 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&session, &conn));

    /* Correct stmt_set_hash restores compatibility */
    conn.stmt_set_hash = session.stmt_set_hash;
    TEST_ASSERT(backend_pool_stmt_compatible(&session, &conn));

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("=== test_ps_semantic_compat ===\n\n");

    /* VIRTUALIZE mode — extended protocol semantic tests */
    test_virt_ddl_invalidates_stmt_set();
    test_virt_drop_create_same_name_invalidates();
    test_virt_search_path_change_updates_profile();
    test_virt_set_role_updates_profile();
    test_virt_discard_plans_clears_stmt_hash();
    test_virt_unknown_utility_marks_semantic_unknown();
    test_virt_unnamed_parse_skips_session_hash();

    /* Cross-backend pool semantic-compatibility tests */
    test_pool_compat_mismatch_profile_rejected();
    test_pool_compat_mismatched_stmt_hash_rejected();

    return test_summary();
}
