/**
 * @file test_pg_protocol_flow.c
 * @brief Wire-level regression suite for the PostgreSQL protocol flow plugin.
 *
 * This file is intentionally exhaustive because the PostgreSQL flow plugin is
 * one of KEEL's most semantics-heavy components. The tests validate not only
 * packet parsing but also the higher-level policy layered on top of PostgreSQL
 * traffic: transaction tracking, pin/quarantine decisions, prepared-statement
 * bookkeeping, cleanup generation, and safe reuse-gate detection.
 *
 * The suite uses hand-built wire messages rather than a client library so each
 * test can isolate one protocol transition precisely and assert the plugin's
 * interpretation without unrelated client behavior interfering.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/plugin/plugin_types.h"
#include "keel/session/state_profile.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/protocol/pg_backend_auth.h"
#include "keel/protocol/mysql_backend_auth.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>

/* The tests exercise the built-in PostgreSQL flow vtable directly so failures
 * stay localized to the plugin contract instead of the surrounding worker
 * runtime. */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;

/* ---- Shorthand ---- */
#define VT (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire-protocol message builders
 * ============================================================================ */

/**
 * @brief Write a 32-bit big-endian integer into a PostgreSQL wire buffer.
 *
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Build a protocol V3 StartupMessage with `user` and `database` fields.
 *
 * @param buf Destination buffer.
 * @param user User name to encode.
 * @param db Database name to encode.
 * @return Total frame length in bytes.
 */
static size_t build_startup(uint8_t* buf, const char* user, const char* db) {
    uint8_t* p = buf + 4;           /* skip length placeholder */
    wr32(p, 0x00030000); p += 4;    /* protocol 3.0 */
    memcpy(p, "user", 5); p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1); p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1); p += dl + 1;
    *p++ = '\0';                     /* terminator */
    uint32_t total = (uint32_t)(p - buf);
    wr32(buf, total);
    return total;
}

/* Build an SSL request: length(4) + code(4) */
static size_t build_ssl_request(uint8_t* buf) {
    wr32(buf, 8);
    wr32(buf + 4, 80877103);
    return 8;
}

/* Build a Cancel request: length(4) + code(4) + pid(4) + secret(4) */
static size_t build_cancel_request(uint8_t* buf, uint32_t pid, uint32_t secret) {
    wr32(buf, 16);
    wr32(buf + 4, 80877102);
    wr32(buf + 8, pid);
    wr32(buf + 12, secret);
    return 16;
}

/* Build a Simple Query message: 'Q' + length(4) + sql\0 */
static size_t build_query(uint8_t* buf, const char* sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

/* Build a minimal extended protocol message: type(1) + length(4) */
static size_t build_extended_msg(uint8_t* buf, uint8_t type) {
    buf[0] = type;
    wr32(buf + 1, 4);     /* length = 4 (just the length field itself) */
    return 5;
}

/* Build a named Parse message: 'P' + len + stmt_name\0 + query\0 + int16(0) */
static size_t build_named_parse(uint8_t* buf, const char* name, const char* query) {
    size_t nl = strlen(name);
    size_t ql = strlen(query);
    size_t body = nl + 1 + ql + 1 + 2;  /* name\0 + query\0 + numparams(2) */
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body));
    memcpy(buf + 5, name, nl + 1);
    memcpy(buf + 5 + nl + 1, query, ql + 1);
    buf[5 + nl + 1 + ql + 1] = 0;
    buf[5 + nl + 1 + ql + 2] = 0;
    return 1 + 4 + body;
}

/* Build Bind message: 'B' + length(4) + portal\0 + stmt_name\0 + 0+0+0 (no params/formats) */
static size_t build_bind(uint8_t* buf, const char* portal, const char* stmt_name) {
    size_t pl = strlen(portal);
    size_t sl = strlen(stmt_name);
    /* portal\0 + stmt\0 + num_fmt(2) + num_params(2) + num_result_fmts(2) */
    size_t body = pl + 1 + sl + 1 + 2 + 2 + 2;
    buf[0] = 'B';
    wr32(buf + 1, (uint32_t)(4 + body));
    uint8_t* p = buf + 5;
    memcpy(p, portal, pl + 1);    p += pl + 1;
    memcpy(p, stmt_name, sl + 1); p += sl + 1;
    p[0] = 0; p[1] = 0; p += 2;  /* num_format_codes = 0 */
    p[0] = 0; p[1] = 0; p += 2;  /* num_params = 0 */
    p[0] = 0; p[1] = 0;          /* num_result_fmts = 0 */
    return 1 + 4 + body;
}

/* Build Terminate message: 'X' + length(4) */
static size_t build_terminate(uint8_t* buf) {
    buf[0] = 'X';
    wr32(buf + 1, 4);
    return 5;
}

/* Build Flush message: 'H' + length(4) */
static size_t build_flush(uint8_t* buf) {
    buf[0] = 'H';
    wr32(buf + 1, 4);
    return 5;
}

/* Build Close message: 'C' + length(4) + 'S' + name\0 */
static size_t build_close(uint8_t* buf, const char* name) {
    size_t nl = strlen(name);
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + 1 + nl + 1));
    buf[5] = 'S';
    memcpy(buf + 6, name, nl + 1);
    return 1 + 4 + 1 + nl + 1;
}

/* Build Password message: 'p' + length(4) + password\0 */
static size_t build_password(uint8_t* buf, const char* pw) {
    size_t pl = strlen(pw);
    buf[0] = 'p';
    wr32(buf + 1, (uint32_t)(4 + pl + 1));
    memcpy(buf + 5, pw, pl + 1);
    return 1 + 4 + pl + 1;
}

/* Build CopyData: 'd' + length(4) + data */
static size_t build_copy_data(uint8_t* buf, const uint8_t* data, size_t dlen) {
    buf[0] = 'd';
    wr32(buf + 1, (uint32_t)(4 + dlen));
    if (dlen > 0) memcpy(buf + 5, data, dlen);
    return 1 + 4 + dlen;
}

/* Build CopyDone: 'c' + length(4) */
static size_t build_copy_done(uint8_t* buf) {
    buf[0] = 'c';
    wr32(buf + 1, 4);
    return 5;
}

/* Build CopyFail: 'f' + length(4) + message\0 */
static size_t build_copy_fail(uint8_t* buf, const char* msg) {
    size_t ml = strlen(msg);
    buf[0] = 'f';
    wr32(buf + 1, (uint32_t)(4 + ml + 1));
    memcpy(buf + 5, msg, ml + 1);
    return 1 + 4 + ml + 1;
}

/* ---- Backend message builders ---- */

/* Build ReadyForQuery: 'Z' + 5(4) + status(1) */
static size_t build_ready_for_query(uint8_t* buf, char status) {
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
}

/* Build a backend CommandComplete: 'C' + len(4) + tag\0 */
static size_t build_command_complete(uint8_t* buf, const char* tag) {
    size_t tl = strlen(tag);
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + tl + 1));
    memcpy(buf + 5, tag, tl + 1);
    return 1 + 4 + tl + 1;
}

/* Simulate the backend confirming a tracking-mode simple-query PREPARE.
 * Sends CommandComplete("PREPARE") then ReadyForQuery('I') through on_be_msg.
 * Must be called after on_fe_msg("PREPARE ...") to transition the staged
 * cache entry from confirmed=false → confirmed=true and update session_stmt_hash. */
static void sim_track_prepare_confirm(void* ctx) {
    uint8_t buf[32];
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, build_command_complete(buf, "PREPARE"), &bact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
}

/* Build AuthenticationOk: 'R' + 8(4) + 0(4) */
static size_t build_auth_ok(uint8_t* buf) {
    buf[0] = 'R';
    wr32(buf + 1, 8);
    wr32(buf + 5, 0);
    return 9;
}

/* Build ParameterStatus: 'S' + length(4) + key\0 + value\0 */
static size_t build_param_status(uint8_t* buf, const char* key, const char* val) {
    size_t kl = strlen(key) + 1;
    size_t vl = strlen(val) + 1;
    buf[0] = 'S';
    wr32(buf + 1, (uint32_t)(4 + kl + vl));
    memcpy(buf + 5, key, kl);
    memcpy(buf + 5 + kl, val, vl);
    return 1 + 4 + kl + vl;
}

/* Build BackendKeyData: 'K' + 12(4) + pid(4) + secret(4) */
static size_t build_backend_key_data(uint8_t* buf, uint32_t pid, uint32_t secret) {
    buf[0] = 'K';
    wr32(buf + 1, 12);
    wr32(buf + 5, pid);
    wr32(buf + 9, secret);
    return 13;
}

/* Build CopyInResponse: 'G' + length(4) + format(1) + num_cols(2) */
static size_t build_copy_in_response(uint8_t* buf) {
    buf[0] = 'G';
    wr32(buf + 1, 7);
    buf[5] = 0;           /* text format */
    buf[6] = 0; buf[7] = 0; /* 0 columns */
    return 8;
}

/* Build CopyOutResponse: 'H' + length(4) + format(1) + num_cols(2) */
static size_t build_copy_out_response(uint8_t* buf) {
    buf[0] = 'H';
    wr32(buf + 1, 7);
    buf[5] = 0;
    buf[6] = 0; buf[7] = 0;
    return 8;
}

/* Build CopyBothResponse: 'W' + length(4) + format(1) + num_cols(2) */
static size_t build_copy_both_response(uint8_t* buf) {
    buf[0] = 'W';
    wr32(buf + 1, 7);
    buf[5] = 0;
    buf[6] = 0; buf[7] = 0;
    return 8;
}

/* ============================================================================
 * Context bootstrap helpers
 * ============================================================================
 *
 * Many tests care about post-startup behavior rather than startup parsing
 * itself. These helpers move a fresh flow context into the steady-state phase
 * so the individual tests can focus on one later protocol transition.
 */
/**
 * @brief Create a PostgreSQL flow context and advance it through startup.
 *
 * @return Ready-to-use flow context, or `NULL` on allocation failure.
 */
static void* create_and_startup(void) {
    void* ctx = VT->create_context(NULL);
    if (!ctx) return NULL;

    uint8_t buf[512];
    size_t len = build_startup(buf, "testuser", "testdb");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);
    /* After startup, context is in "startup_complete" state */
    return ctx;
}

/* ============================================================================
 * 1) STARTUP / HANDSHAKE TESTS
 * ============================================================================ */

static void test_startup_normal(void) {
    TEST_BEGIN("startup: normal V3 handshake");

    void* ctx = VT->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    size_t len = build_startup(buf, "alice", "mydb");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_AUTH_COMPLETE);
    TEST_ASSERT_NOT_NULL(act.fe_response);
    TEST_ASSERT(act.fe_response_len > 0);

    /* Handshake response should start with AuthenticationOk ('R') */
    TEST_ASSERT_EQ(act.fe_response[0], 'R');

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_startup_ssl_request(void) {
    TEST_BEGIN("startup: SSL request returns 'N'");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[16];
    size_t len = build_ssl_request(buf);
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SSL_REQUEST);
    TEST_ASSERT_NOT_NULL(act.fe_response);
    TEST_ASSERT_EQ(act.fe_response_len, (size_t)1);
    TEST_ASSERT_EQ(act.fe_response[0], 'N');

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_startup_cancel_request(void) {
    TEST_BEGIN("startup: cancel request");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[32];
    size_t len = build_cancel_request(buf, 12345, 67890);
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_CANCEL_REQUEST);
    TEST_ASSERT_NOT_NULL(act.be_payload);
    TEST_ASSERT_EQ(act.be_payload_len, len);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_startup_bad_version(void) {
    TEST_BEGIN("startup: unsupported protocol version → error");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[16];
    wr32(buf, 8);
    wr32(buf + 4, 0x00020000);   /* protocol V2 — unsupported */
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, 8, &act);

    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_startup_too_short(void) {
    TEST_BEGIN("startup: message too short → error");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[4] = {0, 0, 0, 4};
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, 4, &act);

    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_startup_database_defaults_to_username(void) {
    TEST_BEGIN("startup: database defaults to username");

    void* ctx = VT->create_context(NULL);

    /* Build startup with user only, no database param */
    uint8_t buf[256];
    uint8_t* p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5); p += 5;
    memcpy(p, "bob", 4); p += 4;
    *p++ = '\0';
    uint32_t total = (uint32_t)(p - buf);
    wr32(buf, total);

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, total, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_AUTH_COMPLETE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_startup_with_application_name(void) {
    TEST_BEGIN("startup: application_name parsed");

    void* ctx = VT->create_context(NULL);

    uint8_t buf[512];
    uint8_t* p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5); p += 5;
    memcpy(p, "alice", 6); p += 6;
    memcpy(p, "database", 9); p += 9;
    memcpy(p, "mydb", 5); p += 5;
    memcpy(p, "application_name", 17); p += 17;
    memcpy(p, "pgbench", 8); p += 8;
    *p++ = '\0';
    uint32_t total = (uint32_t)(p - buf);
    wr32(buf, total);

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, total, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_AUTH_COMPLETE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 2) FRAME LENGTH DETECTION
 * ============================================================================ */

static void test_frame_len_startup_incomplete(void) {
    TEST_BEGIN("frame_len: startup incomplete (< 4 bytes)");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[2] = {0, 0};
    ssize_t fl = VT->frame_len(ctx, buf, 2, 0);
    TEST_ASSERT_EQ(fl, (ssize_t)0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_frame_len_startup_complete(void) {
    TEST_BEGIN("frame_len: startup message length");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[64];
    size_t len = build_startup(buf, "u", "d");
    ssize_t fl = VT->frame_len(ctx, buf, len, 0);
    TEST_ASSERT_EQ(fl, (ssize_t)len);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_frame_len_regular_message(void) {
    TEST_BEGIN("frame_len: regular message");

    void* ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[32];
    size_t len = build_query(buf, "SELECT 1");
    ssize_t fl = VT->frame_len(ctx, buf, len, 0);
    TEST_ASSERT_EQ(fl, (ssize_t)len);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_frame_len_incomplete(void) {
    TEST_BEGIN("frame_len: incomplete regular message (< 5 bytes)");

    void* ctx = create_and_startup();
    uint8_t buf[3] = {'Q', 0, 0};
    ssize_t fl = VT->frame_len(ctx, buf, 3, 0);
    TEST_ASSERT_EQ(fl, (ssize_t)0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_frame_len_backend_message(void) {
    TEST_BEGIN("frame_len: backend direction");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_ready_for_query(buf, 'I');
    ssize_t fl = VT->frame_len(ctx, buf, len, 1);
    TEST_ASSERT_EQ(fl, (ssize_t)len);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 3) SIMPLE QUERY — READS
 * ============================================================================ */

static void test_simple_query_select(void) {
    TEST_BEGIN("query: SELECT routed to REPLICA, READONLY");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "SELECT id, name FROM users");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_SQL);
    TEST_ASSERT(act.effect & KEEL_QE_READONLY);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_REPLICA);
    TEST_ASSERT(act.cache_eligible);
    TEST_ASSERT_NOT_NULL(act.be_payload);
    TEST_ASSERT_EQ(act.be_payload_len, len);
    TEST_ASSERT_NOT_NULL(act.sql_view);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_show(void) {
    TEST_BEGIN("query: SHOW routed to REPLICA, READONLY");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "SHOW search_path");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_READONLY);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_REPLICA);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_explain(void) {
    TEST_BEGIN("query: EXPLAIN routed to REPLICA, READONLY");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "EXPLAIN SELECT 1");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_READONLY);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 3b) SIMPLE QUERY — WRITES
 * ============================================================================ */

static void test_simple_query_insert(void) {
    TEST_BEGIN("query: INSERT → WRITE, PRIMARY");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "INSERT INTO t VALUES (1)");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_update(void) {
    TEST_BEGIN("query: UPDATE → WRITE, PRIMARY");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "UPDATE t SET x=1 WHERE id=2");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_delete(void) {
    TEST_BEGIN("query: DELETE → WRITE, PRIMARY");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "DELETE FROM t WHERE id=3");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_truncate(void) {
    TEST_BEGIN("query: TRUNCATE → WRITE, PRIMARY");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "TRUNCATE t");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 3c) SIMPLE QUERY — DDL
 * ============================================================================ */

static void test_simple_query_create_table(void) {
    TEST_BEGIN("query: CREATE TABLE → DDL|WRITE");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "CREATE TABLE t (id int)");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_DDL);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_alter(void) {
    TEST_BEGIN("query: ALTER → DDL|WRITE");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "ALTER TABLE t ADD COLUMN x int");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_DDL);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_drop(void) {
    TEST_BEGIN("query: DROP → DDL|WRITE");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "DROP TABLE t");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_DDL);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 3d) SIMPLE QUERY — TX CONTROL
 * ============================================================================ */

static void test_simple_query_begin(void) {
    TEST_BEGIN("query: BEGIN → BEGINS_TX, pins TRANSACTION");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "BEGIN");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_BEGINS_TX);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_commit(void) {
    TEST_BEGIN("query: COMMIT → ENDS_TX, clears TRANSACTION");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "COMMIT");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_simple_query_rollback(void) {
    TEST_BEGIN("query: ROLLBACK → ENDS_TX, clears TRANSACTION");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "ROLLBACK");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 4) EXTENDED PROTOCOL
 * ============================================================================ */

static void test_extended_parse(void) {
    TEST_BEGIN("extended: Parse sets EXTENDED_PROTO pin");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_extended_msg(buf, 'P');
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_EXTENDED);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);
    TEST_ASSERT_NOT_NULL(act.be_payload);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_extended_bind(void) {
    TEST_BEGIN("extended: Bind sets EXTENDED_PROTO pin");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_extended_msg(buf, 'B');
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_EXTENDED);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_extended_execute(void) {
    TEST_BEGIN("extended: Execute sets EXTENDED_PROTO pin");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_extended_msg(buf, 'E');
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_extended_describe(void) {
    TEST_BEGIN("extended: Describe sets EXTENDED_PROTO pin");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_extended_msg(buf, 'D');
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_extended_sync_clears_pin(void) {
    TEST_BEGIN("extended: Sync clears EXTENDED_PROTO pin");

    void* ctx = create_and_startup();

    /* First set the pin via Parse */
    uint8_t buf[16];
    keel_fe_action_t act;
    size_t len = build_extended_msg(buf, 'P');
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    /* Now Sync should clear it */
    len = build_extended_msg(buf, 'S');
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_extended_full_cycle(void) {
    TEST_BEGIN("extended: P→B→E→S full cycle");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    keel_fe_action_t act;

    /* Parse */
    VT->on_fe_msg(ctx, build_extended_msg(buf, 'P') ? buf : buf,
                   build_extended_msg(buf, 'P'), &act);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    /* Bind */
    size_t len = build_extended_msg(buf, 'B');
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    /* Execute */
    len = build_extended_msg(buf, 'E');
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    /* Sync — clears pin */
    len = build_extended_msg(buf, 'S');
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 5) COPY LIFECYCLE
 * ============================================================================ */

static void test_copy_in_lifecycle(void) {
    TEST_BEGIN("copy-in: CopyInResponse → data → done → Z clears COPY pin");

    void* ctx = create_and_startup();
    keel_be_action_t bact;

    /* Backend sends CopyInResponse */
    uint8_t buf[64];
    size_t len = build_copy_in_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.enters_copy_mode);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_COPY);

    /* Frontend sends CopyData */
    uint8_t data[] = {1, 2, 3};
    keel_fe_action_t fact;
    len = build_copy_data(buf, data, sizeof(data));
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(fact.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);
    TEST_ASSERT_EQ(fact.msg_kind, KEEL_MSG_KIND_COPY);
    TEST_ASSERT(fact.splice_eligible);

    /* Frontend sends CopyDone */
    len = build_copy_done(buf);
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(fact.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);
    TEST_ASSERT_EQ(fact.msg_kind, KEEL_MSG_KIND_COPY);

    /* Backend sends ReadyForQuery — should exit copy mode */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.exits_copy_mode);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_COPY);
    TEST_ASSERT(bact.query_complete);
    TEST_ASSERT(bact.backend_reusable);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_copy_in_fail(void) {
    TEST_BEGIN("copy-in: CopyFail forwarded to backend");

    void* ctx = create_and_startup();
    keel_be_action_t bact;

    /* Enter copy mode */
    uint8_t buf[128];
    size_t len = build_copy_in_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.enters_copy_mode);

    /* CopyFail */
    keel_fe_action_t fact;
    len = build_copy_fail(buf, "client canceled");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(fact.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);
    TEST_ASSERT_EQ(fact.msg_kind, KEEL_MSG_KIND_COPY);

    /* ReadyForQuery after error exits copy mode */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.exits_copy_mode);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_COPY);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_copy_out_lifecycle(void) {
    TEST_BEGIN("copy-out: CopyOutResponse enters copy mode, Z exits");

    void* ctx = create_and_startup();
    keel_be_action_t bact;

    /* Backend sends CopyOutResponse */
    uint8_t buf[32];
    size_t len = build_copy_out_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.enters_copy_mode);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_COPY);

    /* After backend sends all data ... ReadyForQuery */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.exits_copy_mode);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_COPY);
    TEST_ASSERT(bact.backend_reusable);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_copy_both_response(void) {
    TEST_BEGIN("copy-both: CopyBothResponse enters copy mode");

    void* ctx = create_and_startup();
    keel_be_action_t bact;

    uint8_t buf[32];
    size_t len = build_copy_both_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.enters_copy_mode);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_COPY);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 6) MULTIPLE READS WITHOUT TRANSACTION (auto-commit)
 * ============================================================================ */

static void test_multiple_reads_autocommit(void) {
    TEST_BEGIN("multi-read autocommit: each SELECT+Z(I) is reusable");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Query 1: SELECT */
    size_t len = build_query(buf, "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_READONLY);
    TEST_ASSERT_EQ(fact.route_hint, KEEL_FROUTE_REPLICA);

    /* Backend responds: ReadyForQuery(I) */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.query_complete);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);

    /* Query 2: another SELECT */
    len = build_query(buf, "SELECT count(*) FROM t");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_READONLY);

    /* Backend responds again: ReadyForQuery(I) */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.query_complete);

    /* Query 3: yet another SELECT */
    len = build_query(buf, "SELECT * FROM t WHERE id=5");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_READONLY);

    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);

    /* Reuse gate should always be true between queries */
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 7) MULTIPLE WRITES WITHOUT TRANSACTION (auto-commit)
 * ============================================================================ */

static void test_multiple_writes_autocommit(void) {
    TEST_BEGIN("multi-write autocommit: each write+Z(I) is reusable");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Write 1: INSERT */
    size_t len = build_query(buf, "INSERT INTO t VALUES (1)");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(fact.route_hint, KEEL_FROUTE_PRIMARY);

    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.query_complete);

    /* Write 2: UPDATE */
    len = build_query(buf, "UPDATE t SET x=2 WHERE id=1");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_WRITE);

    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);

    /* Write 3: DELETE */
    len = build_query(buf, "DELETE FROM t WHERE id=1");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_WRITE);

    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);

    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 8) MULTIPLE READS WITHIN BEGIN/COMMIT
 * ============================================================================ */

static void test_reads_within_transaction(void) {
    TEST_BEGIN("reads in txn: BEGIN→SELECT→SELECT→COMMIT lifecycle");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    size_t len = build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_BEGINS_TX);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_TRANSACTION);

    /* Backend: Z(T) — now in transaction */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!bact.backend_reusable);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_TRANSACTION);

    /* Reuse gate should be false during transaction */
    TEST_ASSERT(!VT->backend_reuse_gate(ctx));

    /* SELECT 1 */
    len = build_query(buf, "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_READONLY);

    /* Z(T) — still in transaction */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!bact.backend_reusable);

    /* SELECT 2 */
    len = build_query(buf, "SELECT count(*) FROM t");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_READONLY);

    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* COMMIT */
    len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(fact.pin_clear & KEEL_FPIN_TRANSACTION);

    /* Z(I) — back to idle */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_TRANSACTION);

    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 9) MIXED READ/WRITE WITHIN BEGIN/COMMIT
 * ============================================================================ */

static void test_mixed_rw_in_transaction(void) {
    TEST_BEGIN("mixed rw in txn: BEGIN→INSERT→SELECT→UPDATE→COMMIT");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_TRANSACTION);

    /* Z(T) */
    size_t len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* INSERT */
    len = build_query(buf, "INSERT INTO t VALUES (10)");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(fact.route_hint, KEEL_FROUTE_PRIMARY);

    /* Z(T) */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);

    /* SELECT within transaction — still READONLY but routed to replica */
    len = build_query(buf, "SELECT * FROM t");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_READONLY);

    /* Z(T) */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* UPDATE */
    len = build_query(buf, "UPDATE t SET x=99 WHERE id=10");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_WRITE);

    /* Z(T) */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);

    /* COMMIT */
    len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_ENDS_TX);

    /* Z(I) */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_transaction_rollback_cycle(void) {
    TEST_BEGIN("txn: BEGIN→INSERT→ROLLBACK→Z(I) resets cleanly");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    size_t len = build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fact);

    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* INSERT */
    len = build_query(buf, "INSERT INTO t VALUES (42)");
    VT->on_fe_msg(ctx, buf, len, &fact);

    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);

    /* ROLLBACK */
    len = build_query(buf, "ROLLBACK");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(fact.pin_clear & KEEL_FPIN_TRANSACTION);

    /* Z(I) */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10) CORNER CASES
 * ============================================================================ */

static void test_set_statement(void) {
    TEST_BEGIN("corner: SET → SETS_STATE, not cache eligible");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "SET search_path TO public, extra");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_SETS_STATE);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_reset_statement(void) {
    TEST_BEGIN("corner: RESET → SETS_STATE");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "RESET search_path");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_SETS_STATE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_discard_all(void) {
    TEST_BEGIN("corner: DISCARD ALL → SETS_STATE");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "DISCARD ALL");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_SETS_STATE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_prepare_statement(void) {
    TEST_BEGIN("corner: PREPARE → HARD_PIN + PREPARED_STMT + QUARANTINE");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE myplan AS SELECT 1");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_HARD_PIN);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);
    /* PREPARE triggers quarantine via hardpin scanner */
    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_listen_quarantine(void) {
    TEST_BEGIN("corner: LISTEN → QUARANTINE + POTENTIALLY_STATEFUL");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "LISTEN my_channel");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_temp_table_quarantine(void) {
    TEST_BEGIN("corner: CREATE TEMP TABLE → QUARANTINE");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "CREATE TEMP TABLE t_tmp (id int)");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_declare_cursor_quarantine(void) {
    TEST_BEGIN("corner: DECLARE CURSOR → QUARANTINE");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "DECLARE mycur CURSOR FOR SELECT 1");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_failed_tx_z_error(void) {
    TEST_BEGIN("corner: Z('E') → TX_FAILED, pins TRANSACTION+FAILED_TX");

    void* ctx = create_and_startup();
    uint8_t buf[32];
    keel_be_action_t bact;

    /* Simulate: backend reports error state */
    size_t len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);

    TEST_ASSERT(bact.tx_state_changed);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_FAILED);
    TEST_ASSERT(!bact.backend_reusable);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* Reuse gate should be false */
    TEST_ASSERT(!VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_failed_tx_recovery(void) {
    TEST_BEGIN("corner: Z('E') → ROLLBACK → Z('I') recovers");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Enter failed tx */
    size_t len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* ROLLBACK */
    len = build_query(buf, "ROLLBACK");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_ENDS_TX);

    /* Z(I) — recovered */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_FAILED_TX);

    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_terminate_message(void) {
    TEST_BEGIN("corner: Terminate → ACT_TERMINATE");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_terminate(buf);
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_TERMINATE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_flush_forwarded(void) {
    TEST_BEGIN("corner: Flush → FORWARD_TO_BACKEND");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_flush(buf);
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);
    TEST_ASSERT_NOT_NULL(act.be_payload);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_close_forwarded(void) {
    TEST_BEGIN("corner: Close → QUERY (extended proto pipeline)");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_close(buf, "mystmt");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_EXTENDED);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_named_parse_pins_prepared_stmt(void) {
    TEST_BEGIN("prepared: named Parse sets PREPARED_STMT pin");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_named_parse(buf, "mystmt", "SELECT 1");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_EXTENDED);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_unnamed_parse_no_prepared_pin(void) {
    TEST_BEGIN("prepared: unnamed Parse does NOT set PREPARED_STMT pin");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_extended_msg(buf, 'P');
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.pin_update & KEEL_FPIN_EXTENDED_PROTO);
    TEST_ASSERT(!(act.pin_update & KEEL_FPIN_PREPARED_STMT));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_close_clears_prepared_pin(void) {
    TEST_BEGIN("prepared: Close last named stmt clears PREPARED_STMT pin");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    keel_fe_action_t act;

    /* Create one named stmt */
    size_t len = build_named_parse(buf, "s1", "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);

    /* Close it — should clear the pin */
    len = build_close(buf, "s1");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_close_with_remaining_stmts_no_clear(void) {
    TEST_BEGIN("prepared: Close with remaining stmts keeps pin");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    keel_fe_action_t act;

    /* Create two named stmts */
    size_t len = build_named_parse(buf, "s1", "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &act);
    len = build_named_parse(buf, "s2", "SELECT 2");
    VT->on_fe_msg(ctx, buf, len, &act);

    /* Close first — should NOT clear pin (one left) */
    len = build_close(buf, "s1");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(!(act.pin_clear & KEEL_FPIN_PREPARED_STMT));

    /* Close second — should clear pin (none left) */
    len = build_close(buf, "s2");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ==========================================================================
 * Wire-protocol message builders
 * ============================================================================
 *
 * These builders emit minimal but valid PostgreSQL frames. They are not meant
 * to be a reusable packet library; they simply make each test scenario explicit
 * without drowning the assertions in byte-literal boilerplate.
 */

/* ==========================================================================
 * Prepared Statement Mode Tests
 * ============================================================================
 *
 * Tests for KEEL_PS_MODE_PINNING, KEEL_PS_MODE_TRACKING, and
 * KEEL_PS_MODE_ANONYMOUS.  Each test creates a fresh context via
 * create_and_startup() and then casts it to pg_flow_ctx_t* (white-box)
 * to set the ps_mode field and to inspect internal state.
 * =========================================================================== */

/* Helper: create a startup-complete context with a custom PS mode */
static void* create_with_ps_mode(keel_ps_mode_t mode) {
    void* ctx = create_and_startup();
    if (ctx) ((pg_flow_ctx_t*)ctx)->ps_mode = mode;
    return ctx;
}

/* ---------------------------------------------------------------------------
 * 12a) Pinning mode
 * --------------------------------------------------------------------------- */

static void test_ps_pinning_named_parse_sets_pin(void) {
    TEST_BEGIN("ps_mode/pinning: named Parse still sets PREPARED_STMT pin");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_PINNING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t  len = build_named_parse(buf, "mystmt", "SELECT $1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    /* The protocol layer does not change for PINNING; the distinction is
     * in engine_flow.c (borrow_pinned vs. hash-match).  Confirm the pin
     * flag IS set so the engine can act on it. */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_pinning_unnamed_parse_no_ps_pin(void) {
    TEST_BEGIN("ps_mode/pinning: unnamed Parse does NOT set PREPARED_STMT pin");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_PINNING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[16];
    size_t  len = build_extended_msg(buf, 'P');
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(!(act.pin_update & KEEL_FPIN_PREPARED_STMT));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ---------------------------------------------------------------------------
 * 12b) Tracking mode
 * --------------------------------------------------------------------------- */

static void test_ps_tracking_prepare_stored_in_cache(void) {
    TEST_BEGIN("ps_mode/tracking: 'PREPARE name AS sql' populates stmt_cache");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t  len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    /* The query is forwarded normally */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_NOT_NULL(act.be_payload);

    /* After on_fe_msg: entry is staged (valid=true, confirmed=false) pending
     * CommandComplete("PREPARE") from the backend. */
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    bool found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "trackstmt") == 0) {
            found = true;
            TEST_ASSERT(!c->stmt_cache[i].confirmed);  /* staged, not yet confirmed */
            TEST_ASSERT_NOT_NULL(c->stmt_cache[i].wire_msg);
            break;
        }
    }
    TEST_ASSERT(found);

    /* Simulate backend confirming: CommandComplete("PREPARE") + ReadyForQuery('I') */
    sim_track_prepare_confirm(ctx);

    /* After confirmation: entry must be confirmed and session hash non-zero */
    found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "trackstmt") == 0) {
            found = true;
            TEST_ASSERT(c->stmt_cache[i].confirmed);
            break;
        }
    }
    TEST_ASSERT(found);
    TEST_ASSERT(c->session_stmt_hash != 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_prepare_hard_pin_stripped(void) {
    TEST_BEGIN("ps_mode/tracking: HARD_PIN stripped from PREPARE effect");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t  len = build_query(buf, "PREPARE pintest AS SELECT 2");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    /* KEEL_QE_HARD_PIN must NOT be in act->effect for TRACKING mode */
    TEST_ASSERT(!(act.effect & KEEL_QE_HARD_PIN));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_prepare_with_param_types(void) {
    TEST_BEGIN("ps_mode/tracking: PREPARE with type list stored");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    size_t  len = build_query(buf,
        "PREPARE typed_stmt(int4, text) AS SELECT $1::int + 1, $2");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    bool found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "typed_stmt") == 0) {
            found = true;
            break;
        }
    }
    TEST_ASSERT(found);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_prepare_lowercase(void) {
    TEST_BEGIN("ps_mode/tracking: lowercase 'prepare ... as' stored");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t  len = build_query(buf, "prepare lowstmt as select 42");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    bool found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "lowstmt") == 0) {
            found = true;
            break;
        }
    }
    TEST_ASSERT(found);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_non_prepare_passes_through(void) {
    TEST_BEGIN("ps_mode/tracking: non-PREPARE query is unchanged");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[64];
    size_t  len = build_query(buf, "SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_NOT_NULL(act.be_payload);
    /* No cache entries should have been added */
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    int entries = 0;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++)
        if (c->stmt_cache[i].valid) entries++;
    TEST_ASSERT_EQ(entries, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_search_path_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: search_path change rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;

    len = build_query(buf, "SET search_path TO tenant_a, public");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != old_hash);

    bool found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "trackstmt") == 0) {
            found = true;
            TEST_ASSERT_EQ(c->stmt_cache[i].context_sig, c->stmt_context_sig);
            break;
        }
    }
    TEST_ASSERT(found);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_set_role_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: SET ROLE rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE rolestmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;

    len = build_query(buf, "SET ROLE app_reader");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != old_hash);
    TEST_ASSERT_STR_EQ(c->stmt_role, "app_reader");

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_reset_role_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: RESET ROLE rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE rolestmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    len = build_query(buf, "SET ROLE app_reader");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    uint64_t role_hash = c->session_stmt_hash;

    len = build_query(buf, "RESET ROLE");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != role_hash);
    TEST_ASSERT_STR_EQ(c->stmt_role, "");

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_stmt_compat_profile_hashes_update(void) {
    TEST_BEGIN("ps_mode/tracking: stmt compat profile hashes update on semantic changes");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    size_t len = build_query(buf, "PREPARE pcompat AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    keel_stmt_compat_profile_t p0;
    memset(&p0, 0, sizeof(p0));
    rc = VT->get_stmt_compat_profile(ctx, &p0);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(p0.stmt_set_hash != 0);
    TEST_ASSERT(!p0.semantic_unknown);

    len = build_query(buf, "SET search_path TO tenant_sem, public");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    keel_stmt_compat_profile_t p1;
    memset(&p1, 0, sizeof(p1));
    rc = VT->get_stmt_compat_profile(ctx, &p1);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(p1.semantic_profile_hash != p0.semantic_profile_hash);
    TEST_ASSERT(p1.search_path_hash != p0.search_path_hash);
    TEST_ASSERT(p1.role_hash == p0.role_hash);

    len = build_query(buf, "SET ROLE app_reader");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    keel_stmt_compat_profile_t p2;
    memset(&p2, 0, sizeof(p2));
    rc = VT->get_stmt_compat_profile(ctx, &p2);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(p2.semantic_profile_hash != p1.semantic_profile_hash);
    TEST_ASSERT(p2.role_hash != p1.role_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_deallocate_keeps_pin_with_remaining_stmt(void) {
    TEST_BEGIN("ps_mode/tracking: DEALLOCATE by name keeps pin while stmts remain");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    size_t len = build_query(buf, "PREPARE pdel1 AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    len = build_query(buf, "PREPARE pdel2 AS SELECT 2");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    len = build_query(buf, "DEALLOCATE pdel1");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!(act.pin_clear & KEEL_FPIN_PREPARED_STMT));

    keel_be_action_t bact;
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(rc, 0);

    ssize_t err_len = VT->generate_error(ctx, "26000",
        "prepared statement \"pdel1\" does not exist", buf, sizeof(buf));
    TEST_ASSERT(err_len > 0);
    rc = VT->on_be_msg(ctx, buf, (size_t)err_len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_ABSORB);

    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_NOT_NULL(bact.fe_payload);
    TEST_ASSERT_EQ(bact.fe_payload[0], 'C');

    keel_stmt_compat_profile_t after_first;
    memset(&after_first, 0, sizeof(after_first));
    rc = VT->get_stmt_compat_profile(ctx, &after_first);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(after_first.stmt_set_hash != 0);
    TEST_ASSERT(after_first.stmt_set_hash != before.stmt_set_hash);

    len = build_query(buf, "DEALLOCATE pdel2");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    keel_stmt_compat_profile_t after_second;
    memset(&after_second, 0, sizeof(after_second));
    rc = VT->get_stmt_compat_profile(ctx, &after_second);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(after_second.stmt_set_hash, 0ULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_ddl_invalidates_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: DDL invalidates stmt set and bumps schema epoch");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    size_t len = build_query(buf, "PREPARE pddl AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    len = build_query(buf, "ALTER TABLE t ADD COLUMN c int");
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

static void test_ps_tracking_discard_plans_invalidates_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: DISCARD PLANS invalidates stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    size_t len = build_query(buf, "PREPARE pdisc AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    len = build_query(buf, "DISCARD PLANS");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(after.stmt_set_hash, 0ULL);
    TEST_ASSERT(after.schema_epoch > before.schema_epoch);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_discard_all_resets_guc_context(void) {
    TEST_BEGIN("ps_mode/tracking: DISCARD ALL resets GUC and role context");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Prepare a statement so the stmt set is non-empty */
    size_t len = build_query(buf, "PREPARE pdatest AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    /* Apply GUC and role changes */
    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "SET search_path TO tenant_a, public"),
                       &act);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "SET ROLE app_reader"), &act);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(strcmp(c->stmt_search_path, "") != 0);
    TEST_ASSERT(strcmp(c->stmt_role, "") != 0);
    TEST_ASSERT(c->session_stmt_hash != 0);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);

    /* DISCARD ALL must clear stmts, reset GUCs and role */
    len = build_query(buf, "DISCARD ALL");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    /* Stmts cleared */
    TEST_ASSERT_EQ(c->session_stmt_hash, 0ULL);
    /* GUC and role fields reset to empty (session defaults) */
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "");
    TEST_ASSERT_STR_EQ(c->stmt_role, "");
    TEST_ASSERT_STR_EQ(c->stmt_session_auth, "");

    /* Compatibility profile hashes must match a fresh context */
    void* fresh = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(fresh);
    pg_flow_ctx_t* fc = (pg_flow_ctx_t*)fresh;
    TEST_ASSERT_EQ(c->stmt_search_path_hash, fc->stmt_search_path_hash);
    TEST_ASSERT_EQ(c->stmt_role_hash, fc->stmt_role_hash);
    TEST_ASSERT_EQ(c->stmt_guc_hash, fc->stmt_guc_hash);
    VT->destroy_context(fresh);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_unknown_semantic_marks_incompatible(void) {
    TEST_BEGIN("ps_mode/tracking: unknown utility marks semantic_unknown");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    size_t len = build_query(buf, "PREPARE punk AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    len = build_query(buf, "CALL do_work()");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t p;
    memset(&p, 0, sizeof(p));
    rc = VT->get_stmt_compat_profile(ctx, &p);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(p.semantic_unknown);
    TEST_ASSERT_EQ(p.stmt_set_hash, 0ULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_set_session_auth_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: SET SESSION AUTHORIZATION rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE authstmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;

    len = build_query(buf, "SET SESSION AUTHORIZATION app_user");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != old_hash);
    TEST_ASSERT_STR_EQ(c->stmt_session_auth, "app_user");

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_reset_session_auth_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: RESET SESSION AUTHORIZATION rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE authstmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    len = build_query(buf, "SET SESSION AUTHORIZATION app_user");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    uint64_t auth_hash = c->session_stmt_hash;

    len = build_query(buf, "RESET SESSION AUTHORIZATION");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != auth_hash);
    TEST_ASSERT_STR_EQ(c->stmt_session_auth, "");

    VT->destroy_context(ctx);
    TEST_END();
}

#define DEFINE_STMT_GUC_REHASH_TEST(fn_name, title_text, change_sql, field_name, expected_value) \
static void fn_name(void) { \
    TEST_BEGIN(title_text); \
    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING); \
    TEST_ASSERT_NOT_NULL(ctx); \
    uint8_t buf[512]; \
    size_t len = build_query(buf, "PREPARE gucstmt AS SELECT 1"); \
    keel_fe_action_t act; \
    int rc = VT->on_fe_msg(ctx, buf, len, &act); \
    TEST_ASSERT_EQ(rc, 0); \
    sim_track_prepare_confirm(ctx); \
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx; \
    uint64_t old_hash = c->session_stmt_hash; \
    len = build_query(buf, change_sql); \
    rc = VT->on_fe_msg(ctx, buf, len, &act); \
    TEST_ASSERT_EQ(rc, 0); \
    TEST_ASSERT(c->session_stmt_hash != old_hash); \
    TEST_ASSERT_STR_EQ(c->field_name, expected_value); \
    VT->destroy_context(ctx); \
    TEST_END(); \
}

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_timezone_rehashes_stmt_set,
    "ps_mode/tracking: SET TimeZone rehashes stmt set",
    "SET TimeZone TO 'UTC'",
    stmt_timezone,
    "UTC")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_datestyle_rehashes_stmt_set,
    "ps_mode/tracking: SET DateStyle rehashes stmt set",
    "SET DateStyle TO 'ISO, DMY'",
    stmt_datestyle,
    "ISO, DMY")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_intervalstyle_rehashes_stmt_set,
    "ps_mode/tracking: SET IntervalStyle rehashes stmt set",
    "SET IntervalStyle TO 'sql_standard'",
    stmt_intervalstyle,
    "sql_standard")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_standard_conforming_strings_rehashes_stmt_set,
    "ps_mode/tracking: SET standard_conforming_strings rehashes stmt set",
    "SET standard_conforming_strings TO on",
    stmt_standard_conforming_strings,
    "on")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_backslash_quote_rehashes_stmt_set,
    "ps_mode/tracking: SET backslash_quote rehashes stmt set",
    "SET backslash_quote TO on",
    stmt_backslash_quote,
    "on")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_escape_string_warning_rehashes_stmt_set,
    "ps_mode/tracking: SET escape_string_warning rehashes stmt set",
    "SET escape_string_warning TO off",
    stmt_escape_string_warning,
    "off")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_default_tablespace_rehashes_stmt_set,
    "ps_mode/tracking: SET default_tablespace rehashes stmt set",
    "SET default_tablespace TO pg_default",
    stmt_default_tablespace,
    "pg_default")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_temp_tablespaces_rehashes_stmt_set,
    "ps_mode/tracking: SET temp_tablespaces rehashes stmt set",
    "SET temp_tablespaces TO pg_default",
    stmt_temp_tablespaces,
    "pg_default")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_default_table_access_method_rehashes_stmt_set,
    "ps_mode/tracking: SET default_table_access_method rehashes stmt set",
    "SET default_table_access_method TO heap",
    stmt_default_table_access_method,
    "heap")

DEFINE_STMT_GUC_REHASH_TEST(
    test_ps_tracking_set_row_security_rehashes_stmt_set,
    "ps_mode/tracking: SET row_security rehashes stmt set",
    "SET row_security TO off",
    stmt_row_security,
    "off")

static void test_ps_tracking_reset_all_clears_semantic_gucs(void) {
    TEST_BEGIN("ps_mode/tracking: RESET ALL clears stmt semantic GUCs");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE gucstmt AS SELECT 1"),
                           &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t baseline_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "SET TimeZone TO 'UTC'"), &act);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "SET DateStyle TO 'ISO, DMY'"), &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != baseline_hash);
    uint64_t changed_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "RESET ALL"), &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_STR_EQ(c->stmt_timezone, "");
    TEST_ASSERT_STR_EQ(c->stmt_datestyle, "");
    TEST_ASSERT(c->session_stmt_hash != changed_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_set_local_search_path_rehashes_and_reverts(void) {
    TEST_BEGIN("ps_mode/tracking: SET LOCAL search_path rehashes and reverts at tx end");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t fact;
    keel_be_action_t bact;
    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE localstmt AS SELECT 1"),
                           &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t baseline_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT_EQ(rc, 0);

    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "SET LOCAL search_path TO tenant_local, public"),
                       &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "tenant_local, public");
    TEST_ASSERT(c->session_stmt_hash != baseline_hash);

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "COMMIT"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "");
    TEST_ASSERT_EQ(c->session_stmt_hash, baseline_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_set_config_local_search_path_rehashes_and_reverts(void) {
    TEST_BEGIN("ps_mode/tracking: set_config(search_path,...,true) rehashes and reverts at rollback");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t fact;
    keel_be_action_t bact;
    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE localstmt AS SELECT 1"),
                           &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t baseline_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT_EQ(rc, 0);

    rc = VT->on_fe_msg(ctx, buf,
        build_query(buf,
            "SELECT set_config('search_path', 'tenant_cfg, public', true)"),
        &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "tenant_cfg, public");
    TEST_ASSERT(c->session_stmt_hash != baseline_hash);

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "ROLLBACK"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "");
    TEST_ASSERT_EQ(c->session_stmt_hash, baseline_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_set_config_session_search_path_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: set_config(search_path,...,false) rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t fact;
    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE localstmt AS SELECT 1"),
                           &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t baseline_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf,
        build_query(buf,
            "SELECT set_config('search_path', 'tenant_func, public', false)"),
        &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "tenant_func, public");
    TEST_ASSERT_STR_EQ(c->stmt_search_path_session, "tenant_func, public");
    TEST_ASSERT(c->session_stmt_hash != baseline_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_set_config_session_timezone_rehashes_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: set_config(TimeZone,...,false) rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t fact;
    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE localstmt AS SELECT 1"),
                           &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t baseline_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf,
        build_query(buf,
            "SELECT set_config('TimeZone', 'UTC', false)"),
        &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!c->stmt_timezone_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_timezone, "UTC");
    TEST_ASSERT_STR_EQ(c->stmt_timezone_session, "UTC");
    TEST_ASSERT(c->session_stmt_hash != baseline_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_set_local_search_path_rehashes_and_reverts(void) {
    TEST_BEGIN("ps_mode/tracking: extended SET LOCAL search_path rehashes and reverts at tx end");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t fact;
    keel_be_action_t bact;
    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE localstmt AS SELECT 1"),
                           &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t baseline_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT_EQ(rc, 0);

    rc = VT->on_fe_msg(ctx, buf,
                       build_named_parse(buf, "",
                           "SET LOCAL search_path TO tenant_exec, public"),
                       &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_fe_msg(ctx, buf, build_extended_msg(buf, 'E'), &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "tenant_exec, public");
    TEST_ASSERT(c->session_stmt_hash != baseline_hash);

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "COMMIT"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "");
    TEST_ASSERT_EQ(c->session_stmt_hash, baseline_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_local_overlay_and_temp_shadow_rehash_order_on_rollback(void) {
    TEST_BEGIN("ps_mode/tracking: local overlay plus temp shadow rehashes in rollback order");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[512];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    int rc = VT->on_fe_msg(ctx, buf,
                           build_query(buf, "PREPARE mixstmt AS SELECT 1"),
                           &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t base_epoch = c->stmt_temp_epoch;

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT_EQ(rc, 0);

    rc = VT->on_fe_msg(ctx, buf,
        build_query(buf,
            "SELECT set_config('search_path', 'pg_temp, public', true)"),
        &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "pg_temp, public");
    uint64_t after_local_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "CREATE TEMP TABLE mixstmt_shadow (id int)"),
                       &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, base_epoch + 1);
    TEST_ASSERT(c->stmt_temp_tx_rollback_reset_pending);
    TEST_ASSERT(c->session_stmt_hash != after_local_hash);
    uint64_t in_tx_hash = c->session_stmt_hash;

    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "ROLLBACK"), &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!c->stmt_search_path_local_active);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "");
    TEST_ASSERT(!c->stmt_temp_tx_rollback_reset_pending);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, base_epoch + 2);
    TEST_ASSERT(c->session_stmt_hash != in_tx_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_temp_table_bumps_temp_epoch(void) {
    TEST_BEGIN("ps_mode/tracking: CREATE TEMP TABLE bumps temp stmt epoch");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;
    uint64_t old_epoch = c->stmt_temp_epoch;

    len = build_query(buf, "CREATE TEMP TABLE t_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != old_hash);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, old_epoch + 1);

    bool found = false;
    for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
        if (c->stmt_cache[i].valid &&
            strcmp(c->stmt_cache[i].name, "trackstmt") == 0) {
            found = true;
            TEST_ASSERT_EQ(c->stmt_cache[i].context_sig, c->stmt_context_sig);
            break;
        }
    }
    TEST_ASSERT(found);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_temp_shadow_execute_discards_plans(void) {
    TEST_BEGIN("ps_mode/tracking: temp shadow rewrites next EXECUTE with DISCARD PLANS");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE pshadow AS SELECT COUNT(*) FROM shadow_target");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(c->session_stmt_hash != 0);

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->in_transaction);

    len = build_query(buf, "CREATE TEMP TABLE shadow_target (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_discard_plans_before_execute);

    len = build_query(buf, "EXECUTE pshadow");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(fact.be_payload != buf);
    TEST_ASSERT_EQ(fact.be_payload[0], 'Q');
    TEST_ASSERT(strstr((const char*)fact.be_payload + 5,
                       "DISCARD PLANS;EXECUTE pshadow") != NULL);
    TEST_ASSERT(!c->stmt_discard_plans_before_execute);
    TEST_ASSERT(c->stmt_discard_plans_absorb_pending);

    len = build_command_complete(buf, "DISCARD PLANS");
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(!c->stmt_discard_plans_absorb_pending);

    len = build_command_complete(buf, "SELECT 1");
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(bact.type != KEEL_BE_ACT_ABSORB);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_temp_table_bumps_temp_epoch(void) {
    TEST_BEGIN("ps_mode/tracking: extended Execute temp DDL bumps temp stmt epoch");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;
    uint64_t old_epoch = c->stmt_temp_epoch;

    len = build_named_parse(buf, "", "CREATE TEMP TABLE ext_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->session_stmt_hash != old_hash);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, old_epoch + 1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_temp_on_commit_drop_rehashes_at_tx_end(void) {
    TEST_BEGIN("ps_mode/tracking: temp ON COMMIT DROP rehashes again at txn end");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_create_epoch = c->stmt_temp_epoch;
    uint64_t before_create_hash = c->session_stmt_hash;

    len = build_query(buf,
        "CREATE TEMP TABLE t_tmp (id int) ON COMMIT DROP");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_create_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_create_hash);
    TEST_ASSERT(c->stmt_temp_tx_reset_pending);

    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_temp_tx_reset_pending);

    uint64_t before_commit_epoch = c->stmt_temp_epoch;
    uint64_t before_commit_hash = c->session_stmt_hash;

    len = build_query(buf, "COMMIT");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'I');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_commit_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_commit_hash);
    TEST_ASSERT(!c->stmt_temp_tx_reset_pending);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_temp_on_commit_drop_rehashes_at_tx_end(void) {
    TEST_BEGIN("ps_mode/tracking: extended temp ON COMMIT DROP rehashes again at txn end");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_execute_epoch = c->stmt_temp_epoch;
    uint64_t before_execute_hash = c->session_stmt_hash;

    len = build_named_parse(buf, "", "CREATE TEMP TABLE ext_tmp (id int) ON COMMIT DROP");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_execute_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_execute_hash);
    TEST_ASSERT(c->stmt_temp_tx_reset_pending);

    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(c->stmt_temp_tx_reset_pending);

    uint64_t before_commit_epoch = c->stmt_temp_epoch;
    uint64_t before_commit_hash = c->session_stmt_hash;

    len = build_query(buf, "COMMIT");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'I');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_commit_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_commit_hash);
    TEST_ASSERT(!c->stmt_temp_tx_reset_pending);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_temp_on_commit_delete_rows_rehashes_at_tx_end(void) {
    TEST_BEGIN("ps_mode/tracking: temp ON COMMIT DELETE ROWS rehashes again at txn end");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_create_epoch = c->stmt_temp_epoch;
    len = build_query(buf,
        "CREATE TEMP TABLE t_tmp (id int) ON COMMIT DELETE ROWS");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_create_epoch + 1);
    TEST_ASSERT(c->stmt_temp_tx_reset_pending);

    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_commit_epoch = c->stmt_temp_epoch;
    len = build_query(buf, "COMMIT");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'I');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_commit_epoch + 1);
    TEST_ASSERT(!c->stmt_temp_tx_reset_pending);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_temp_on_commit_delete_rows_rehashes_at_tx_end(void) {
    TEST_BEGIN("ps_mode/tracking: extended temp ON COMMIT DELETE ROWS rehashes again at txn end");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_create_epoch = c->stmt_temp_epoch;
    len = build_named_parse(buf, "", "CREATE TEMP TABLE ext_tmp (id int) ON COMMIT DELETE ROWS");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_create_epoch + 1);
    TEST_ASSERT(c->stmt_temp_tx_reset_pending);

    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_commit_epoch = c->stmt_temp_epoch;
    len = build_query(buf, "COMMIT");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'I');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_commit_epoch + 1);
    TEST_ASSERT(!c->stmt_temp_tx_reset_pending);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_discard_temp_rehashes_temp_context(void) {
    TEST_BEGIN("ps_mode/tracking: DISCARD TEMP rehashes temp context");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    len = build_query(buf, "CREATE TEMP TABLE t_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_discard_epoch = c->stmt_temp_epoch;
    uint64_t before_discard_hash = c->session_stmt_hash;

    len = build_query(buf, "DISCARD TEMP");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_discard_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_discard_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_discard_temp_rehashes_temp_context(void) {
    TEST_BEGIN("ps_mode/tracking: extended Execute DISCARD TEMP rehashes temp context");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    len = build_query(buf, "CREATE TEMP TABLE t_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_discard_epoch = c->stmt_temp_epoch;
    uint64_t before_discard_hash = c->session_stmt_hash;

    len = build_named_parse(buf, "", "DISCARD TEMP");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_discard_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_discard_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_discard_plans_invalidates_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: extended Execute DISCARD PLANS invalidates stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    size_t len = build_query(buf, "PREPARE epdisc AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(c->session_stmt_hash != 0);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    /* Send DISCARD PLANS via extended protocol */
    len = build_named_parse(buf, "", "DISCARD PLANS");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    keel_stmt_compat_profile_t after;
    memset(&after, 0, sizeof(after));
    rc = VT->get_stmt_compat_profile(ctx, &after);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(after.stmt_set_hash, 0ULL);
    TEST_ASSERT(after.schema_epoch > before.schema_epoch);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_discard_all_resets_guc_context(void) {
    TEST_BEGIN("ps_mode/tracking: extended Execute DISCARD ALL resets GUC and role context");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    /* Prepare a statement */
    size_t len = build_query(buf, "PREPARE epdatest AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    /* Apply GUC and role changes via Simple Query */
    rc = VT->on_fe_msg(ctx, buf,
                       build_query(buf, "SET search_path TO ext_schema, public"),
                       &fact);
    TEST_ASSERT_EQ(rc, 0);
    rc = VT->on_fe_msg(ctx, buf, build_query(buf, "SET ROLE ext_reader"), &fact);
    TEST_ASSERT_EQ(rc, 0);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(strcmp(c->stmt_search_path, "") != 0);
    TEST_ASSERT(strcmp(c->stmt_role, "") != 0);
    TEST_ASSERT(c->session_stmt_hash != 0);

    /* Send DISCARD ALL via extended protocol */
    len = build_named_parse(buf, "", "DISCARD ALL");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    /* Stmts cleared */
    TEST_ASSERT_EQ(c->session_stmt_hash, 0ULL);
    /* GUC and role fields reset to empty */
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "");
    TEST_ASSERT_STR_EQ(c->stmt_role, "");
    TEST_ASSERT_STR_EQ(c->stmt_session_auth, "");

    /* Hashes must match a fresh context */
    void* fresh = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(fresh);
    pg_flow_ctx_t* fc = (pg_flow_ctx_t*)fresh;
    TEST_ASSERT_EQ(c->stmt_search_path_hash, fc->stmt_search_path_hash);
    TEST_ASSERT_EQ(c->stmt_role_hash, fc->stmt_role_hash);
    TEST_ASSERT_EQ(c->stmt_guc_hash, fc->stmt_guc_hash);
    VT->destroy_context(fresh);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_ddl_invalidates_stmt_set(void) {
    TEST_BEGIN("ps_mode/tracking: extended Execute DDL invalidates stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    size_t len = build_query(buf, "PREPARE epddl AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(c->session_stmt_hash != 0);

    keel_stmt_compat_profile_t before;
    memset(&before, 0, sizeof(before));
    rc = VT->get_stmt_compat_profile(ctx, &before);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(before.stmt_set_hash != 0);

    /* Send ALTER TABLE via extended protocol */
    len = build_named_parse(buf, "", "ALTER TABLE t ADD COLUMN x int");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
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

static void test_ps_tracking_drop_table_rehashes_temp_context(void) {
    TEST_BEGIN("ps_mode/tracking: DROP TABLE rehashes temp context after temp state exists");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "CREATE TEMP TABLE t_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_drop_epoch = c->stmt_temp_epoch;
    uint64_t before_drop_hash = c->session_stmt_hash;

    len = build_query(buf, "DROP TABLE t_tmp");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_drop_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_drop_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_drop_table_rehashes_temp_context(void) {
    TEST_BEGIN("ps_mode/tracking: extended Execute DROP TABLE rehashes temp context after temp state exists");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "CREATE TEMP TABLE t_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_drop_epoch = c->stmt_temp_epoch;
    uint64_t before_drop_hash = c->session_stmt_hash;

    len = build_named_parse(buf, "", "DROP TABLE t_tmp");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_drop_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_drop_hash);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_temp_create_rehashes_again_on_rollback(void) {
    TEST_BEGIN("ps_mode/tracking: temp create rehashes again on rollback");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_create_epoch = c->stmt_temp_epoch;
    uint64_t before_create_hash = c->session_stmt_hash;

    len = build_query(buf, "CREATE TEMP TABLE rb_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_create_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_create_hash);
    TEST_ASSERT(c->stmt_temp_tx_rollback_reset_pending);

    uint64_t before_rollback_epoch = c->stmt_temp_epoch;
    uint64_t before_rollback_hash = c->session_stmt_hash;

    len = build_query(buf, "ROLLBACK");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'I');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_rollback_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_rollback_hash);
    TEST_ASSERT(!c->stmt_temp_tx_rollback_reset_pending);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_tracking_extended_temp_create_rehashes_again_on_rollback(void) {
    TEST_BEGIN("ps_mode/tracking: extended temp create rehashes again on rollback");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    size_t len = build_query(buf, "PREPARE trackstmt AS SELECT 1");
    int rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    len = build_query(buf, "BEGIN");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'T');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);

    uint64_t before_create_epoch = c->stmt_temp_epoch;
    uint64_t before_create_hash = c->session_stmt_hash;

    len = build_named_parse(buf, "", "CREATE TEMP TABLE rb_ext_tmp (id int)");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_extended_msg(buf, 'E');
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_create_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_create_hash);
    TEST_ASSERT(c->stmt_temp_tx_rollback_reset_pending);

    uint64_t before_rollback_epoch = c->stmt_temp_epoch;
    uint64_t before_rollback_hash = c->session_stmt_hash;

    len = build_query(buf, "ROLLBACK");
    rc = VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT_EQ(rc, 0);
    len = build_ready_for_query(buf, 'I');
    rc = VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(c->stmt_temp_epoch, before_rollback_epoch + 1);
    TEST_ASSERT(c->session_stmt_hash != before_rollback_hash);
    TEST_ASSERT(!c->stmt_temp_tx_rollback_reset_pending);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ---------------------------------------------------------------------------
 * 12c) Anonymous mode
 * --------------------------------------------------------------------------- */

static void test_ps_anon_named_parse_intercepted(void) {
    TEST_BEGIN("ps_mode/anonymous: named Parse gives synthetic ParseComplete");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t  len = build_named_parse(buf, "anonstmt", "SELECT $1");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    /* Must NOT be forwarded to backend */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT(act.be_payload == NULL);

    /* Must return a synthetic ParseComplete ('1') to the client */
    TEST_ASSERT_NOT_NULL(act.fe_response);
    TEST_ASSERT(act.fe_response_len >= 1);
    TEST_ASSERT_EQ(act.fe_response[0], '1');

    /* PREPARED_STMT pin must NOT be requested (backend stays clean) */
    TEST_ASSERT(!(act.pin_update & KEEL_FPIN_PREPARED_STMT));

    /* Entry must be in anon_map */
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    bool found_in_map = false;
    for (int i = 0; i < PG_ANON_MAP_SIZE; i++) {
        if (c->anon_map[i].name[0] != '\0' &&
            strcmp(c->anon_map[i].name, "anonstmt") == 0) {
            found_in_map = true;
            break;
        }
    }
    TEST_ASSERT(found_in_map);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_named_parse_not_in_named_stmt_count(void) {
    TEST_BEGIN("ps_mode/anonymous: named Parse does NOT increment named_stmt_count");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t  len = build_named_parse(buf, "anon2", "SELECT 2");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    /* Because we intercepted the Parse and never sent it to the backend,
     * the backend has no named statement, so named_stmt_count stays 0. */
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT_EQ((int)c->named_stmt_count, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_unnamed_parse_passes_through(void) {
    TEST_BEGIN("ps_mode/anonymous: unnamed Parse is NOT intercepted");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    /* Unnamed Parse: stmt_name is empty string */
    uint8_t buf[256];
    size_t  len = build_named_parse(buf, "", "SELECT 99");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    /* Unnamed: must pass through to backend normally */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_NOT_NULL(act.be_payload);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_bind_rewrites_to_anon_parse_bind(void) {
    TEST_BEGIN("ps_mode/anonymous: Bind for known stmt rewritten to Parse+Bind");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    /* 1. Intercept a named Parse to populate anon_map */
    uint8_t buf[512];
    size_t  len = build_named_parse(buf, "myanon", "SELECT 42");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);

    /* 2. Send a Bind referencing "myanon" */
    len = build_bind(buf, "", "myanon");
    VT->on_fe_msg(ctx, buf, len, &act);

    /* The rewritten payload must start with 'P' (anonymous Parse) */
    TEST_ASSERT_NOT_NULL(act.be_payload);
    TEST_ASSERT_EQ(act.be_payload[0], 'P');

    /* After the Parse there must be a 'B' (Bind) */
    bool has_bind = false;
    if (act.be_payload_len > 5) {
        /* Parse length field is at bytes [1..4] (big-endian int32).
         * Total Parse bytes on wire = 1 (type byte) + parse_msg_len. */
        uint32_t parse_msg_len = ((uint32_t)act.be_payload[1] << 24)
                               | ((uint32_t)act.be_payload[2] << 16)
                               | ((uint32_t)act.be_payload[3] << 8)
                               |  (uint32_t)act.be_payload[4];
        size_t bind_off = 1 + parse_msg_len;
        if (bind_off < act.be_payload_len)
            has_bind = (act.be_payload[bind_off] == 'B');
    }
    TEST_ASSERT(has_bind);

    /* The rewritten Bind must reference the anonymous (empty) stmt name.
     * Wire layout after bind_off: 'B'|len4|portal\0|stmt_name\0|...
     * portal is empty (""), so: b[5]=0 (portal NUL), b[6]=0 (anon stmt NUL). */
    if (act.be_payload_len > 5) {
        uint32_t parse_msg_len = ((uint32_t)act.be_payload[1] << 24)
                               | ((uint32_t)act.be_payload[2] << 16)
                               | ((uint32_t)act.be_payload[3] << 8)
                               |  (uint32_t)act.be_payload[4];
        size_t bind_off = 1 + parse_msg_len;
        if (bind_off + 6 < act.be_payload_len) {
            const uint8_t* b = act.be_payload + bind_off;
            TEST_ASSERT_EQ(b[5], 0);  /* empty portal NUL */
            TEST_ASSERT_EQ(b[6], 0);  /* anonymous stmt name NUL */
        }
    }

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_bind_unknown_stmt_passes_through(void) {
    TEST_BEGIN("ps_mode/anonymous: Bind for unknown stmt passes through");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    /* Bind for a stmt that was never PREPARE'd (not in anon_map) */
    uint8_t buf[256];
    size_t  len = build_bind(buf, "", "ghoststmt");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(rc, 0);

    /* Should pass through with the original Bind payload */
    TEST_ASSERT_NOT_NULL(act.be_payload);
    TEST_ASSERT_EQ(act.be_payload[0], 'B');

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_close_removes_map_entry(void) {
    TEST_BEGIN("ps_mode/anonymous: Close removes entry from anon_map");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    /* Populate anon_map via a named Parse */
    uint8_t buf[256];
    keel_fe_action_t act;
    size_t  len = build_named_parse(buf, "closeme", "SELECT 7");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);

    /* Confirm the entry is in the map */
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    bool before = false;
    for (int i = 0; i < PG_ANON_MAP_SIZE; i++)
        if (c->anon_map[i].name[0] && strcmp(c->anon_map[i].name, "closeme") == 0)
            before = true;
    TEST_ASSERT(before);

    /* Send Close for the statement */
    len = build_close(buf, "closeme");
    VT->on_fe_msg(ctx, buf, len, &act);

    /* Entry must be gone */
    bool after = false;
    for (int i = 0; i < PG_ANON_MAP_SIZE; i++)
        if (c->anon_map[i].name[0] && strcmp(c->anon_map[i].name, "closeme") == 0)
            after = true;
    TEST_ASSERT(!after);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_discard_all_clears_map(void) {
    TEST_BEGIN("ps_mode/anonymous: DISCARD ALL clears anon_map");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;

    /* Populate anon_map with two entries */
    size_t len = build_named_parse(buf, "da1", "SELECT 1");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    len = build_named_parse(buf, "da2", "SELECT 2");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    int cnt_before = 0;
    for (int i = 0; i < PG_ANON_MAP_SIZE; i++)
        if (c->anon_map[i].name[0]) cnt_before++;
    TEST_ASSERT(cnt_before >= 2);

    /* DISCARD ALL simple query — frees the lazy-alloc'd anon_map */
    len = build_query(buf, "DISCARD ALL");
    VT->on_fe_msg(ctx, buf, len, &act);

    /* After DISCARD ALL the anon_map must be freed (NULL). */
    TEST_ASSERT_EQ(c->anon_map, NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ps_anon_multiple_stmts_all_intercepted(void) {
    TEST_BEGIN("ps_mode/anonymous: multiple named Parse messages all intercepted");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_ANONYMOUS);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    const char* names[] = { "q1", "q2", "q3", "q4" };
    const char* sqls[]  = { "SELECT 1", "SELECT 2",
                             "INSERT INTO t VALUES(1)", "UPDATE t SET x=1" };

    for (int i = 0; i < 4; i++) {
        size_t len = build_named_parse(buf, names[i], sqls[i]);
        VT->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
        TEST_ASSERT_EQ(act.be_payload, NULL);
    }

    /* All 4 entries must be in anon_map */
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    for (int i = 0; i < 4; i++) {
        bool found = false;
        for (int j = 0; j < PG_ANON_MAP_SIZE; j++)
            if (c->anon_map[j].name[0] &&
                strcmp(c->anon_map[j].name, names[i]) == 0)
                found = true;
        TEST_ASSERT(found);
    }

    /* named_stmt_count stays 0: backend never received any Parse */
    TEST_ASSERT_EQ((int)c->named_stmt_count, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_password_forwarded(void) {
    TEST_BEGIN("corner: password → FORWARD_TO_BACKEND");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_password(buf, "secret123");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_unknown_message_forwarded(void) {
    TEST_BEGIN("corner: unknown message type → FORWARD_TO_BACKEND");

    void* ctx = create_and_startup();
    /* Build msg with type 'Z' (not a valid FE type) -- 'Z' + length(4) */
    uint8_t buf[16];
    buf[0] = 'Z';   /* Not a valid frontend message */
    wr32(buf + 1, 4);
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, 5, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_regular_msg_too_short(void) {
    TEST_BEGIN("corner: regular message < 5 bytes → ERROR");

    void* ctx = create_and_startup();
    uint8_t buf[4] = {'Q', 0, 0, 0};
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, 4, &act);

    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_multi_statement_query(void) {
    TEST_BEGIN("corner: multi-statement → MULTI_STMT flag");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "SELECT 1; SELECT 2");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_MULTI_STMT);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_multi_statement_with_begin(void) {
    TEST_BEGIN("corner: multi-stmt with BEGIN pins TRANSACTION");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "BEGIN; SELECT 1");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_MULTI_STMT);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_large_query_splice_eligible(void) {
    TEST_BEGIN("corner: large query (>8KB) → splice eligible");

    void* ctx = create_and_startup();
    /* Build a SQL string > 8192 bytes */
    char sql[9000];
    memset(sql, ' ', sizeof(sql) - 1);
    memcpy(sql, "SELECT ", 7);
    sql[sizeof(sql) - 1] = '\0';

    uint8_t buf[9100];
    size_t len = build_query(buf, sql);
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.splice_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_small_query_not_splice_eligible(void) {
    TEST_BEGIN("corner: small query (< 8KB) → not splice eligible");

    void* ctx = create_and_startup();
    uint8_t buf[64];
    size_t len = build_query(buf, "SELECT 1");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(!act.splice_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10b) BACKEND MESSAGE CORNER CASES
 * ============================================================================ */

static void test_be_auth_ok(void) {
    TEST_BEGIN("be: AuthenticationOk → AUTH_PROGRESS");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_auth_ok(buf);
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, len, &bact);

    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_AUTH_PROGRESS);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_parameter_status(void) {
    TEST_BEGIN("be: ParameterStatus → profile update");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_param_status(buf, "server_version", "16.0");
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, len, &bact);

    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(bact.has_profile_update);
    TEST_ASSERT_NOT_NULL(bact.profile_key);
    TEST_ASSERT_NOT_NULL(bact.profile_value);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_parameter_status_search_path_rehashes_stmt_set(void) {
    TEST_BEGIN("be: ParameterStatus(search_path) rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t qbuf[256];
    size_t qlen = build_query(qbuf, "PREPARE psctx AS SELECT 1");
    keel_fe_action_t fact;
    int rc = VT->on_fe_msg(ctx, qbuf, qlen, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;

    uint8_t buf[128];
    size_t len = build_param_status(buf, "search_path", "tenant_b, public");
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, len, &bact);

    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(c->session_stmt_hash != old_hash);
    TEST_ASSERT_STR_EQ(c->stmt_search_path, "tenant_b, public");

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_parameter_status_timezone_rehashes_stmt_set(void) {
    TEST_BEGIN("be: ParameterStatus(TimeZone) rehashes stmt set");

    void* ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t qbuf[256];
    size_t qlen = build_query(qbuf, "PREPARE psctx AS SELECT 1");
    keel_fe_action_t fact;
    int rc = VT->on_fe_msg(ctx, qbuf, qlen, &fact);
    TEST_ASSERT_EQ(rc, 0);
    sim_track_prepare_confirm(ctx);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    uint64_t old_hash = c->session_stmt_hash;

    uint8_t buf[128];
    size_t len = build_param_status(buf, "TimeZone", "UTC");
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, len, &bact);

    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(c->session_stmt_hash != old_hash);
    TEST_ASSERT_STR_EQ(c->stmt_timezone, "UTC");

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_backend_key_data(void) {
    TEST_BEGIN("be: BackendKeyData parsed correctly");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    size_t len = build_backend_key_data(buf, 9999, 12345);
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, len, &bact);

    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_too_short(void) {
    TEST_BEGIN("be: message too short → ERROR");

    void* ctx = create_and_startup();
    uint8_t buf[3] = {'Z', 0, 0};
    keel_be_action_t bact;
    int rc = VT->on_be_msg(ctx, buf, 3, &bact);

    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_unknown_message(void) {
    TEST_BEGIN("be: unknown message type → FORWARD_FE");

    void* ctx = create_and_startup();
    /* Build a message with type 'X' (not a standard BE message) */
    uint8_t buf[8];
    buf[0] = 'X';
    wr32(buf + 1, 4);
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, 5, &bact);

    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_is_data_frame(void) {
    TEST_BEGIN("be: is_data_frame classifier");

    TEST_ASSERT(VT->is_data_frame != NULL);

    /* 'D' (DataRow) is the only PG backend frame safe to splice without
     * invoking on_be_msg.  Even just the tag byte is sufficient. */
    uint8_t d_row[9] = {'D', 0, 0, 0, 7, 0, 1, 0, 4};
    TEST_ASSERT( VT->is_data_frame(NULL, d_row, sizeof(d_row)));
    uint8_t d_tag[1] = {'D'};
    TEST_ASSERT( VT->is_data_frame(NULL, d_tag, 1));

    /* Control / terminal frames must return false */
    uint8_t rfq[6]  = {'Z', 0, 0, 0, 5, 'I'};  /* ReadyForQuery */
    uint8_t cc[9]   = {'C', 0, 0, 0, 7, 0, 0, 0, 0};  /* CommandComplete */
    uint8_t errf[9] = {'E', 0, 0, 0, 7, 0, 0, 0, 0};  /* ErrorResponse */
    uint8_t ps[9]   = {'S', 0, 0, 0, 7, 0, 0, 0, 0};  /* ParameterStatus */
    uint8_t auth[9] = {'R', 0, 0, 0, 7, 0, 0, 0, 0};  /* Auth */
    TEST_ASSERT(!VT->is_data_frame(NULL, rfq,   sizeof(rfq)));
    TEST_ASSERT(!VT->is_data_frame(NULL, cc,    sizeof(cc)));
    TEST_ASSERT(!VT->is_data_frame(NULL, errf,  sizeof(errf)));
    TEST_ASSERT(!VT->is_data_frame(NULL, ps,    sizeof(ps)));
    TEST_ASSERT(!VT->is_data_frame(NULL, auth,  sizeof(auth)));

    /* Zero-length header: not enough bytes to classify */
    uint8_t any[1] = {'D'};
    TEST_ASSERT(!VT->is_data_frame(NULL, any, 0));

    TEST_END();
}

static void test_be_z_idle_clears_failed_tx(void) {
    TEST_BEGIN("be: Z(I) clears FAILED_TX pin");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    keel_be_action_t bact;

    /* First enter failed tx */
    size_t len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* Then recover to idle */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_FAILED_TX);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(bact.backend_reusable);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10c) COPY WITHIN TRANSACTION
 * ============================================================================ */

static void test_copy_in_within_transaction(void) {
    TEST_BEGIN("corner: COPY IN within transaction");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    size_t len = build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fact);

    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* COPY ... FROM STDIN */
    len = build_query(buf, "COPY t FROM STDIN");
    VT->on_fe_msg(ctx, buf, len, &fact);

    /* Backend enters copy mode */
    len = build_copy_in_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.enters_copy_mode);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_COPY);

    /* Reuse gate should be false (in_copy + in_transaction) */
    TEST_ASSERT(!VT->backend_reuse_gate(ctx));

    /* CopyDone */
    len = build_copy_done(buf);
    VT->on_fe_msg(ctx, buf, len, &fact);

    /* Z(T) — back in transaction, copy mode exited */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.exits_copy_mode);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_COPY);
    TEST_ASSERT(!bact.backend_reusable);  /* still in tx */

    /* COMMIT */
    len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);

    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10d) EXTENDED PROTOCOL WITHIN TRANSACTION
 * ============================================================================ */

static void test_extended_proto_in_transaction(void) {
    TEST_BEGIN("corner: extended protocol within explicit transaction");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    size_t len = build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fact);

    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);

    /* Parse */
    len = build_extended_msg(buf, 'P');
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    /* Bind */
    len = build_extended_msg(buf, 'B');
    VT->on_fe_msg(ctx, buf, len, &fact);

    /* Execute */
    len = build_extended_msg(buf, 'E');
    VT->on_fe_msg(ctx, buf, len, &fact);

    /* Sync — clears EXTENDED_PROTO but TRANSACTION still pinned */
    len = build_extended_msg(buf, 'S');
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_clear & KEEL_FPIN_EXTENDED_PROTO);

    /* Z(T) — still in transaction */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!bact.backend_reusable);

    /* COMMIT */
    len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);

    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10e) SSL request followed by normal startup
 * ============================================================================ */

static void test_ssl_then_startup(void) {
    TEST_BEGIN("corner: SSL request 'N' then normal V3 startup");

    void* ctx = VT->create_context(NULL);
    uint8_t buf[512];
    keel_fe_action_t act;

    /* SSL request */
    size_t len = build_ssl_request(buf);
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SSL_REQUEST);

    /* Normal startup after SSL rejection */
    len = build_startup(buf, "alice", "mydb");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_AUTH_COMPLETE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10f) Multiple transactions back-to-back
 * ============================================================================ */

static void test_back_to_back_transactions(void) {
    TEST_BEGIN("corner: two complete transactions back-to-back");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Transaction 1 */
    VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT(!bact.backend_reusable);

    VT->on_fe_msg(ctx, buf, build_query(buf, "INSERT INTO t VALUES(1)"), &fact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);

    VT->on_fe_msg(ctx, buf, build_query(buf, "COMMIT"), &fact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT(bact.backend_reusable);

    /* Transaction 2 — immediately */
    VT->on_fe_msg(ctx, buf, build_query(buf, "BEGIN"), &fact);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_TRANSACTION);

    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT(!bact.backend_reusable);

    VT->on_fe_msg(ctx, buf, build_query(buf, "SELECT 1"), &fact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);

    VT->on_fe_msg(ctx, buf, build_query(buf, "COMMIT"), &fact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10g) ReadyForQuery transitions
 * ============================================================================ */

static void test_z_transitions_i_t_e_i(void) {
    TEST_BEGIN("corner: Z transitions I→T→E→I (full cycle)");

    void* ctx = create_and_startup();
    uint8_t buf[16];
    keel_be_action_t bact;

    /* I → T */
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'T'), &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!bact.backend_reusable);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_TRANSACTION);

    /* T → E */
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'E'), &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_FAILED);
    TEST_ASSERT(!bact.backend_reusable);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* E → I (after ROLLBACK) */
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_FAILED_TX);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 11) UTILITY FUNCTIONS
 * ============================================================================ */

static void test_fingerprint_basic(void) {
    TEST_BEGIN("util: fingerprint produces non-zero hash");

    void* ctx = create_and_startup();
    uint64_t h = VT->fingerprint(ctx, "SELECT 1", 8);
    TEST_ASSERT(h != 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_fingerprint_case_insensitive(void) {
    TEST_BEGIN("util: fingerprint is case-insensitive");

    void* ctx = create_and_startup();
    uint64_t h1 = VT->fingerprint(ctx, "SELECT 1", 8);
    uint64_t h2 = VT->fingerprint(ctx, "select 1", 8);
    TEST_ASSERT_EQ(h1, h2);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_fingerprint_whitespace_normalized(void) {
    TEST_BEGIN("util: fingerprint normalizes whitespace");

    void* ctx = create_and_startup();
    uint64_t h1 = VT->fingerprint(ctx, "SELECT\t1", 8);
    uint64_t h2 = VT->fingerprint(ctx, "SELECT 1", 8);
    TEST_ASSERT_EQ(h1, h2);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_fingerprint_different(void) {
    TEST_BEGIN("util: different queries have different fingerprints");

    void* ctx = create_and_startup();
    uint64_t h1 = VT->fingerprint(ctx, "SELECT 1", 8);
    uint64_t h2 = VT->fingerprint(ctx, "SELECT 2", 8);
    TEST_ASSERT(h1 != h2);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_fe_disconnect(void) {
    TEST_BEGIN("util: build_cleanup FE_DISCONNECT → ROLLBACK + DISCARD ALL");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, sizeof(buf));

    TEST_ASSERT(n > 0);
    TEST_ASSERT_EQ(buf[0], 'Q');
    const char* sql = (const char*)(buf + 5);
    TEST_ASSERT(strstr(sql, "ROLLBACK") != NULL);
    TEST_ASSERT(strstr(sql, "DISCARD ALL") != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_unknown_state(void) {
    TEST_BEGIN("util: build_cleanup UNKNOWN_STATE → DISCARD ALL");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_UNKNOWN_STATE, buf, sizeof(buf));

    TEST_ASSERT(n > 0);
    const char* sql = (const char*)(buf + 5);
    TEST_ASSERT(strstr(sql, "DISCARD ALL") != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_failed_tx(void) {
    TEST_BEGIN("util: build_cleanup FAILED_TX → ROLLBACK + DISCARD ALL");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FAILED_TX, buf, sizeof(buf));

    TEST_ASSERT(n > 0);
    const char* sql = (const char*)(buf + 5);
    TEST_ASSERT(strstr(sql, "ROLLBACK") != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_buffer_too_small(void) {
    TEST_BEGIN("util: build_cleanup buffer too small → -1");

    void* ctx = create_and_startup();
    uint8_t buf[4];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, (ssize_t)-1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_reuse_gate_fresh(void) {
    TEST_BEGIN("util: reuse_gate true after startup");

    void* ctx = create_and_startup();
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_startup(void) {
    TEST_BEGIN("util: generate_startup produces valid PG V3 startup");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    ssize_t n = VT->generate_startup(ctx, "testuser", "testdb", buf, sizeof(buf));

    TEST_ASSERT(n > 0);
    /* First 4 bytes = length, next 4 = protocol V3 */
    uint32_t ver = ((uint32_t)buf[4] << 24) | ((uint32_t)buf[5] << 16) |
                   ((uint32_t)buf[6] << 8)  |  (uint32_t)buf[7];
    TEST_ASSERT_EQ(ver, (uint32_t)0x00030000);
    /* Should contain user and database params */
    TEST_ASSERT(memmem(buf, (size_t)n, "user", 4) != NULL);
    TEST_ASSERT(memmem(buf, (size_t)n, "testuser", 8) != NULL);
    TEST_ASSERT(memmem(buf, (size_t)n, "database", 8) != NULL);
    TEST_ASSERT(memmem(buf, (size_t)n, "testdb", 6) != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_startup_buffer_too_small(void) {
    TEST_BEGIN("util: generate_startup buffer too small → -1");

    void* ctx = create_and_startup();
    uint8_t buf[4];
    ssize_t n = VT->generate_startup(ctx, "u", "d", buf, sizeof(buf));
    TEST_ASSERT_EQ(n, (ssize_t)-1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_error(void) {
    TEST_BEGIN("util: generate_error produces valid PG 'E' message");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    ssize_t n = VT->generate_error(ctx, "42000", "test error", buf, sizeof(buf));

    TEST_ASSERT(n > 0);
    TEST_ASSERT_EQ(buf[0], 'E');
    /* Should contain SQLSTATE and message */
    TEST_ASSERT(memmem(buf, (size_t)n, "42000", 5) != NULL);
    TEST_ASSERT(memmem(buf, (size_t)n, "test error", 10) != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_error_buffer_too_small(void) {
    TEST_BEGIN("util: generate_error buffer too small → -1");

    void* ctx = create_and_startup();
    uint8_t buf[4];
    ssize_t n = VT->generate_error(ctx, "42000", "test error", buf, sizeof(buf));
    TEST_ASSERT_EQ(n, (ssize_t)-1);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 13) REPLICATION / TRANSACTION TRACKING (spec §TXN-TRACK)
 * ===========================================================================
 *
 * White-box tests for txn_tracking = on:
 *   a) COMMIT simple query is rewritten to the XID-probe message
 *   b) When txn_tracking is off, COMMIT passes through unchanged
 *   c) 'T' (RowDescription) absorbed while xid_probe_active
 *   d) 'D' (DataRow) absorbed, XID captured via commit_xid_captured
 *   e) 'C'("SELECT ...") absorbed, xid_probe_active cleared
 *   f) 'C'("COMMIT") and 'Z' are forwarded to the client as normal
 * =========================================================================== */

/* Helper: create a startup-complete context with txn_tracking = on */
static void* create_with_txn_tracking(void) {
    void* ctx = create_and_startup();
    if (ctx) ((pg_flow_ctx_t*)ctx)->txn_tracking = true;
    return ctx;
}

/* Helpers to build minimal PostgreSQL backend wire messages */
static size_t build_be_row_description(uint8_t* buf) {
    /* 'T' + int32(6) + int16(0)   — zero-field RowDescription */
    buf[0] = 'T';
    buf[1] = 0; buf[2] = 0; buf[3] = 0; buf[4] = 6;
    buf[5] = 0; buf[6] = 0;
    return 7;
}

static size_t build_be_data_row_text(uint8_t* buf, const char* val) {
    /* 'D' + int32(len) + int16(1) + int32(vlen) + val */
    size_t vlen = strlen(val);
    uint32_t msglen = (uint32_t)(4 + 2 + 4 + vlen);
    buf[0] = 'D';
    buf[1] = (msglen >> 24) & 0xff; buf[2] = (msglen >> 16) & 0xff;
    buf[3] = (msglen >>  8) & 0xff; buf[4] = (msglen      ) & 0xff;
    buf[5] = 0; buf[6] = 1;           /* ncols = 1 */
    uint32_t cl = (uint32_t)vlen;
    buf[7]  = (cl >> 24) & 0xff; buf[8]  = (cl >> 16) & 0xff;
    buf[9]  = (cl >>  8) & 0xff; buf[10] = (cl      ) & 0xff;
    memcpy(buf + 11, val, vlen);
    return (size_t)(11 + vlen);
}

static size_t build_be_command_complete(uint8_t* buf, const char* tag) {
    /* 'C' + int32(len) + tag + '\0' */
    size_t tlen = strlen(tag) + 1;  /* include NUL */
    uint32_t msglen = (uint32_t)(4 + tlen);
    buf[0] = 'C';
    buf[1] = (msglen >> 24) & 0xff; buf[2] = (msglen >> 16) & 0xff;
    buf[3] = (msglen >>  8) & 0xff; buf[4] = (msglen      ) & 0xff;
    memcpy(buf + 5, tag, tlen);
    return (size_t)(5 + tlen);
}

/* --- 13a: COMMIT is rewritten when txn_tracking = on --- */
static void test_txn_track_commit_rewrite(void) {
    TEST_BEGIN("txn_track: COMMIT simple query is rewritten to XID-probe message");

    void* ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[32];
    size_t  len = build_query(buf, "COMMIT");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);

    /* be_payload must point to the static XID-probe rewrite, NOT the original */
    TEST_ASSERT(act.be_payload != buf);
    TEST_ASSERT(act.be_payload_len >= 5);
    /* Must start with 'Q' */
    TEST_ASSERT_EQ(act.be_payload[0], (uint8_t)'Q');
    /* SQL must contain both txid_current and COMMIT */
    const char* sql = (const char*)(act.be_payload + 5);
    TEST_ASSERT(strstr(sql, "txid_current") != NULL);
    TEST_ASSERT(strstr(sql, "COMMIT") != NULL);

    /* xid_probe_active should be set */
    TEST_ASSERT(((pg_flow_ctx_t*)ctx)->xid_probe_active);

    VT->destroy_context(ctx);
    TEST_END();
}

/* --- 13b: No rewrite when txn_tracking is off --- */
static void test_txn_track_no_rewrite_disabled(void) {
    TEST_BEGIN("txn_track: COMMIT passes through unchanged when tracking disabled");

    void* ctx = create_and_startup();   /* default: txn_tracking = false */
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[32];
    size_t  len = build_query(buf, "COMMIT");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    /* be_payload should point to the original buffer */
    TEST_ASSERT(act.be_payload == buf);
    TEST_ASSERT(!((pg_flow_ctx_t*)ctx)->xid_probe_active);

    VT->destroy_context(ctx);
    TEST_END();
}

/* --- 13c: RowDescription absorbed while xid_probe_active --- */
static void test_txn_track_absorbs_row_description(void) {
    TEST_BEGIN("txn_track: RowDescription absorbed while xid_probe_active");

    void* ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    ((pg_flow_ctx_t*)ctx)->xid_probe_active = true;

    uint8_t buf[16];
    size_t  len = build_be_row_description(buf);
    keel_be_action_t act;
    int rc = VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(act.fe_payload == NULL || act.fe_payload_len == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* --- 13d: DataRow absorbed, XID captured --- */
static void test_txn_track_captures_xid_from_datarow(void) {
    TEST_BEGIN("txn_track: DataRow absorbed and XID captured in act.commit_xid");

    void* ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    ((pg_flow_ctx_t*)ctx)->xid_probe_active = true;

    uint8_t buf[64];
    size_t  len = build_be_data_row_text(buf, "9876543");
    keel_be_action_t act;
    int rc = VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(act.commit_xid_captured);
    TEST_ASSERT_EQ(act.commit_xid, (uint64_t)9876543);

    VT->destroy_context(ctx);
    TEST_END();
}

/* --- 13e: CommandComplete("SELECT 1") absorbed, xid_probe_active cleared --- */
static void test_txn_track_absorbs_select_commandcomplete(void) {
    TEST_BEGIN("txn_track: C(SELECT) absorbed and xid_probe_active cleared");

    void* ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    ((pg_flow_ctx_t*)ctx)->xid_probe_active = true;

    uint8_t buf[32];
    size_t  len = build_be_command_complete(buf, "SELECT 1");
    keel_be_action_t act;
    int rc = VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(!((pg_flow_ctx_t*)ctx)->xid_probe_active);

    VT->destroy_context(ctx);
    TEST_END();
}

/* --- 13f: CommandComplete("COMMIT") forwarded after probe --- */
static void test_txn_track_commit_commandcomplete_forwarded(void) {
    TEST_BEGIN("txn_track: C(COMMIT) forwarded normally after xid probe");

    void* ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    /* xid_probe_active = false: the SELECT was already absorbed */
    ((pg_flow_ctx_t*)ctx)->xid_probe_active = false;

    uint8_t buf[32];
    size_t  len = build_be_command_complete(buf, "COMMIT");
    keel_be_action_t act;
    int rc = VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(!act.commit_xid_captured);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 14) UNKNOWN-STATE CLASSIFICATION
 * ============================================================================ */

static void test_do_block_marks_unknown_state(void) {
    TEST_BEGIN("query: DO $$ ... $$ → WRITE + UNKNOWN_STATE");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "DO $$ BEGIN PERFORM pg_sleep(1); END $$");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT(act.effect & KEEL_QE_UNKNOWN_STATE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_call_marks_unknown_state(void) {
    TEST_BEGIN("query: CALL proc() → WRITE + UNKNOWN_STATE");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "CALL my_procedure(42)");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT(act.effect & KEEL_QE_UNKNOWN_STATE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_merge_is_write_not_unknown(void) {
    TEST_BEGIN("query: MERGE → WRITE, no UNKNOWN_STATE");

    void* ctx = create_and_startup();
    uint8_t buf[256];
    size_t len = build_query(buf, "MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET v=s.v");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT(!(act.effect & KEEL_QE_UNKNOWN_STATE));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_vacuum_is_write_not_unknown(void) {
    TEST_BEGIN("query: VACUUM → WRITE, no UNKNOWN_STATE");

    void* ctx = create_and_startup();
    uint8_t buf[128];
    size_t len = build_query(buf, "VACUUM ANALYZE my_table");
    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT(!(act.effect & KEEL_QE_UNKNOWN_STATE));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * §15 — PG backend auth pure functions (pg_backend_auth.c)
 * ============================================================================
 */

static void test_pg_b64_roundtrip(void) {
    TEST_BEGIN("pg_b64: encode/decode roundtrip");

    const uint8_t input[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE };
    char encoded[64];
    uint8_t decoded[64];

    size_t enc_len = pg_b64_encode(input, sizeof(input), encoded, sizeof(encoded));
    TEST_ASSERT(enc_len > 0);
    encoded[enc_len] = '\0';

    size_t dec_len = pg_b64_decode(encoded, enc_len, decoded, sizeof(decoded));
    TEST_ASSERT_EQ(dec_len, sizeof(input));
    TEST_ASSERT(memcmp(decoded, input, sizeof(input)) == 0);

    /* Empty input */
    TEST_ASSERT_EQ(pg_b64_encode(NULL, 0, encoded, sizeof(encoded)), (size_t)0);
    TEST_ASSERT_EQ(pg_b64_decode("", 0, decoded, sizeof(decoded)), (size_t)0);

    /* Buffer too small */
    TEST_ASSERT_EQ(pg_b64_encode(input, sizeof(input), encoded, 1), (size_t)0);

    TEST_END();
}

static void test_pg_build_startup_message(void) {
    TEST_BEGIN("pg_build_startup_message: valid and edge cases");

    uint8_t buf[256];
    ssize_t n;

    n = pg_build_startup_message("postgres", "mydb", buf, sizeof(buf));
    TEST_ASSERT(n > 0);

    /* First 4 bytes = total length, next 4 = protocol 196608 */
    uint32_t total = ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16)
                   | ((uint32_t)buf[2] << 8)  |  (uint32_t)buf[3];
    TEST_ASSERT_EQ(total, (uint32_t)n);

    uint32_t proto = ((uint32_t)buf[4] << 24) | ((uint32_t)buf[5] << 16)
                   | ((uint32_t)buf[6] << 8)  |  (uint32_t)buf[7];
    TEST_ASSERT_EQ(proto, 196608U);

    /* Buffer too small */
    n = pg_build_startup_message("postgres", "mydb", buf, 4);
    TEST_ASSERT(n < 0);

    TEST_END();
}

static void test_pg_build_password_message(void) {
    TEST_BEGIN("pg_build_password_message: valid and edge cases");

    uint8_t buf[128];
    ssize_t n = pg_build_password_message("mypassword", buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    /* Tag should be 'p' */
    TEST_ASSERT_EQ(buf[0], (uint8_t)'p');

    /* Buffer too small */
    n = pg_build_password_message("mypassword", buf, 2);
    TEST_ASSERT(n < 0);

    TEST_END();
}

static void test_pg_sasl_initial_response(void) {
    TEST_BEGIN("pg_sasl_initial_response: SCRAM-SHA-256");

    uint8_t buf[256];
    const char *data = "n,,n=user,r=raNdOmNoNcE";
    ssize_t n = pg_sasl_initial_response("SCRAM-SHA-256",
                                          data, strlen(data),
                                          buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT_EQ(buf[0], (uint8_t)'p');

    /* Buffer too small */
    n = pg_sasl_initial_response("SCRAM-SHA-256", data, strlen(data), buf, 2);
    TEST_ASSERT(n < 0);

    TEST_END();
}

static void test_pg_sasl_response(void) {
    TEST_BEGIN("pg_sasl_response: client-final-message");

    uint8_t buf[256];
    const uint8_t data[] = "c=biws,r=serverNonce,p=AAAA";
    ssize_t n = pg_sasl_response(data, sizeof(data) - 1, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT_EQ(buf[0], (uint8_t)'p');

    /* Buffer too small */
    n = pg_sasl_response(data, sizeof(data) - 1, buf, 2);
    TEST_ASSERT(n < 0);

    TEST_END();
}

static void test_pg_scram_client_first(void) {
    TEST_BEGIN("pg_scram_build_client_first: generates valid packet");

    uint8_t buf[512];
    pg_scram_ctx_t scram;
    memset(&scram, 0, sizeof(scram));

    ssize_t n = pg_scram_build_client_first("testuser", &scram, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    /* Tag should be 'p' (SASLInitialResponse) */
    TEST_ASSERT_EQ(buf[0], (uint8_t)'p');
    /* Client nonce should be populated */
    TEST_ASSERT(scram.client_nonce_b64[0] != '\0');
    /* client_first_bare should contain "n=testuser" */
    TEST_ASSERT(strstr(scram.client_first_bare, "testuser") != NULL);

    TEST_END();
}

/* ============================================================================
 * §16 — MySQL backend auth pure functions (mysql_backend_auth.c)
 * ============================================================================
 */

static void test_my_scramble_native(void) {
    TEST_BEGIN("my_scramble_native: produces 20-byte hash");

    /* Minimal smoke test: deterministic for known inputs */
    uint8_t scramble[20] = { 0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,
                              0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,0x10,
                              0x11,0x12,0x13,0x14 };
    uint8_t out1[20] = {0};
    uint8_t out2[20] = {0};

    my_scramble_native("password", scramble, sizeof(scramble), out1);
    my_scramble_native("password", scramble, sizeof(scramble), out2);

    /* Same inputs → same output (deterministic) */
    TEST_ASSERT(memcmp(out1, out2, 20) == 0);

    /* Different password → different output */
    uint8_t out3[20] = {0};
    my_scramble_native("different", scramble, sizeof(scramble), out3);
    TEST_ASSERT(memcmp(out1, out3, 20) != 0);

    /* Empty password */
    uint8_t out4[20] = {0};
    my_scramble_native("", scramble, sizeof(scramble), out4);
    (void)out4;  /* just check it doesn't crash */

    TEST_END();
}

static void test_my_scramble_caching_sha2(void) {
    TEST_BEGIN("my_scramble_caching_sha2: produces 32-byte hash");

    uint8_t scramble[20] = { 0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,
                              0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,0x10,
                              0x11,0x12,0x13,0x14 };
    uint8_t out1[32] = {0};
    uint8_t out2[32] = {0};

    my_scramble_caching_sha2("password", scramble, sizeof(scramble), out1);
    my_scramble_caching_sha2("password", scramble, sizeof(scramble), out2);

    TEST_ASSERT(memcmp(out1, out2, 32) == 0);

    uint8_t out3[32] = {0};
    my_scramble_caching_sha2("different", scramble, sizeof(scramble), out3);
    TEST_ASSERT(memcmp(out1, out3, 32) != 0);

    TEST_END();
}

static void test_my_parse_greeting_valid(void) {
    TEST_BEGIN("my_parse_greeting: valid v10 handshake");

    /* Minimal MySQL v10 Initial Handshake Packet:
     * 4-byte header + protocol_version(1) + server_version\0(varies) +
     * connection_id(4) + auth_data_1(8) + filler(1) +
     * capability_flags_1(2) + character_set(1) + status_flags(2) +
     * capability_flags_2(2) + auth_plugin_data_len(1) + reserved(10) +
     * auth_data_2(max(13,len-8)) + auth_plugin_name\0
     */
    uint8_t pkt[128];
    memset(pkt, 0, sizeof(pkt));

    size_t off = 0;

    /* MySQL 4-byte header: length(3) + seq_id(1) */
    pkt[off++] = 0x4a;  /* length low byte — will fix later */
    pkt[off++] = 0x00;
    pkt[off++] = 0x00;
    pkt[off++] = 0x00;  /* seq_id = 0 */

    /* Protocol version */
    pkt[off++] = 10;

    /* Server version "8.0.30\0" */
    const char *ver = "8.0.30";
    memcpy(pkt + off, ver, strlen(ver) + 1);
    off += strlen(ver) + 1;

    /* Connection ID (4 bytes LE) */
    pkt[off++] = 0x01; pkt[off++] = 0x00; pkt[off++] = 0x00; pkt[off++] = 0x00;

    /* Auth plugin data part 1 (8 bytes) */
    for (int i = 0; i < 8; i++) pkt[off++] = (uint8_t)(i + 1);

    /* Filler */
    pkt[off++] = 0x00;

    /* Capability flags low 2 bytes: set PROTOCOL_41 + PLUGIN_AUTH + SECURE_CONN */
    uint32_t caps = MY_CAP_PROTOCOL_41 | MY_CAP_PLUGIN_AUTH | MY_CAP_SECURE_CONNECTION;
    pkt[off++] = (uint8_t)(caps & 0xFF);
    pkt[off++] = (uint8_t)((caps >> 8) & 0xFF);

    /* Character set */
    pkt[off++] = 0x21;  /* utf8_general_ci */

    /* Status flags */
    pkt[off++] = 0x02; pkt[off++] = 0x00;

    /* Capability flags high 2 bytes */
    pkt[off++] = (uint8_t)((caps >> 16) & 0xFF);
    pkt[off++] = (uint8_t)((caps >> 24) & 0xFF);

    /* Auth plugin data length (total = 8+13 = 21) */
    pkt[off++] = 21;

    /* Reserved 10 bytes */
    for (int i = 0; i < 10; i++) pkt[off++] = 0x00;

    /* Auth plugin data part 2 (13 bytes = max(13, 21-8) = 13) */
    for (int i = 0; i < 13; i++) pkt[off++] = (uint8_t)(0x10 + i);

    /* Auth plugin name "mysql_native_password\0" */
    const char *plugin = "mysql_native_password";
    memcpy(pkt + off, plugin, strlen(plugin) + 1);
    off += strlen(plugin) + 1;

    /* Fix header length */
    uint32_t body_len = (uint32_t)(off - 4);
    pkt[0] = (uint8_t)(body_len & 0xFF);
    pkt[1] = (uint8_t)((body_len >> 8) & 0xFF);
    pkt[2] = (uint8_t)((body_len >> 16) & 0xFF);

    my_handshake_info_t info;
    memset(&info, 0, sizeof(info));
    int rc = my_parse_greeting(pkt, off, &info);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(info.scramble_len > 0);
    TEST_ASSERT(strcmp(info.plugin, "mysql_native_password") == 0);

    TEST_END();
}

static void test_my_parse_greeting_invalid(void) {
    TEST_BEGIN("my_parse_greeting: invalid packets return -1");

    my_handshake_info_t info;
    memset(&info, 0, sizeof(info));

    /* Too short */
    uint8_t short_pkt[3] = {0};
    TEST_ASSERT(my_parse_greeting(short_pkt, 3, &info) < 0);

    /* NULL data */
    TEST_ASSERT(my_parse_greeting(NULL, 0, &info) < 0);

    TEST_END();
}

static void test_my_parse_auth_result_ok(void) {
    TEST_BEGIN("my_parse_auth_result: OK packet");

    /* Minimal OK packet: 4-byte header + 0x00 + affected_rows(var) + ... */
    uint8_t pkt[16];
    memset(pkt, 0, sizeof(pkt));
    pkt[0] = 0x07;  /* length */
    pkt[1] = 0x00;
    pkt[2] = 0x00;
    pkt[3] = 0x02;  /* seq_id */
    pkt[4] = MY_OK_MARKER;
    /* affected_rows = 0, last_insert_id = 0, status = 0, warnings = 0 */

    my_auth_result_t result;
    memset(&result, 0, sizeof(result));
    int rc = my_parse_auth_result(pkt, sizeof(pkt), &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ((int)result.type, (int)MY_AUTH_OK);

    TEST_END();
}

static void test_my_parse_auth_result_err(void) {
    TEST_BEGIN("my_parse_auth_result: ERR packet");

    /* ERR packet: 4-byte header + 0xFF + error_code(2) + '#' + sqlstate(5) + msg */
    uint8_t pkt[32];
    memset(pkt, 0, sizeof(pkt));
    pkt[0] = 0x17;  /* length low byte */
    pkt[1] = 0x00;
    pkt[2] = 0x00;
    pkt[3] = 0x02;  /* seq_id */
    pkt[4] = MY_ERR_MARKER;
    pkt[5] = 0x15;  /* error code low */
    pkt[6] = 0x04;  /* error code high = 0x0415 = 1045 Access denied */
    pkt[7] = '#';
    memcpy(pkt + 8, "28000", 5);
    memcpy(pkt + 13, "Access denied", 14);

    my_auth_result_t result;
    memset(&result, 0, sizeof(result));
    int rc = my_parse_auth_result(pkt, 4 + 1 + 2 + 1 + 5 + 13, &result);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ((int)result.type, (int)MY_AUTH_ERR);
    TEST_ASSERT_EQ((int)result.err_code, 1045);

    TEST_END();
}

static void test_my_build_handshake_response(void) {
    TEST_BEGIN("my_build_handshake_response: valid output");

    my_handshake_info_t hs;
    memset(&hs, 0, sizeof(hs));
    hs.server_caps = MY_CAP_PROTOCOL_41 | MY_CAP_SECURE_CONNECTION
                   | MY_CAP_PLUGIN_AUTH;
    hs.scramble_len = 20;
    for (int i = 0; i < 20; i++) hs.scramble[i] = (uint8_t)(i + 1);
    hs.seq_id = 1;
    memcpy(hs.plugin, "mysql_native_password", 22);

    uint8_t buf[512];
    ssize_t n = my_build_handshake_response(&hs, "testuser", "testdb",
                                            "password", buf, sizeof(buf));
    TEST_ASSERT(n > 0);

    /* Buffer too small */
    n = my_build_handshake_response(&hs, "testuser", "testdb",
                                    "password", buf, 4);
    TEST_ASSERT(n < 0);

    TEST_END();
}

static void test_my_build_auth_switch_response(void) {
    TEST_BEGIN("my_build_auth_switch_response: valid output");

    uint8_t scramble[20];
    for (int i = 0; i < 20; i++) scramble[i] = (uint8_t)(i + 1);

    uint8_t buf[256];
    ssize_t n = my_build_auth_switch_response("mysql_native_password",
                                               scramble, 20,
                                               "password", 3,
                                               buf, sizeof(buf));
    TEST_ASSERT(n > 0);

    /* Buffer too small */
    n = my_build_auth_switch_response("mysql_native_password",
                                      scramble, 20,
                                      "password", 3,
                                      buf, 4);
    TEST_ASSERT(n < 0);

    TEST_END();
}

static void test_my_build_rsa_key_request(void) {
    TEST_BEGIN("my_build_rsa_key_request: valid output");

    uint8_t buf[64];
    ssize_t n = my_build_rsa_key_request(4, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    /* Packet body should be 0x02 (RSA key request marker) */
    TEST_ASSERT_EQ(buf[4], 0x02);

    /* Buffer too small */
    n = my_build_rsa_key_request(4, buf, 4);
    TEST_ASSERT(n < 0);

    TEST_END();
}

/* ============================================================================
 * MAIN
 * ============================================================================ */
int main(void) {
    printf("=== Comprehensive PG Protocol Flow Tests ===\n\n");

    /* 1) Startup/Handshake */
    test_startup_normal();
    test_startup_ssl_request();
    test_startup_cancel_request();
    test_startup_bad_version();
    test_startup_too_short();
    test_startup_database_defaults_to_username();
    test_startup_with_application_name();

    /* 2) Frame Length */
    test_frame_len_startup_incomplete();
    test_frame_len_startup_complete();
    test_frame_len_regular_message();
    test_frame_len_incomplete();
    test_frame_len_backend_message();

    /* 3) Simple Query — reads */
    test_simple_query_select();
    test_simple_query_show();
    test_simple_query_explain();

    /* 3b) Simple Query — writes */
    test_simple_query_insert();
    test_simple_query_update();
    test_simple_query_delete();
    test_simple_query_truncate();

    /* 3c) Simple Query — DDL */
    test_simple_query_create_table();
    test_simple_query_alter();
    test_simple_query_drop();

    /* 3d) Simple Query — TX control */
    test_simple_query_begin();
    test_simple_query_commit();
    test_simple_query_rollback();

    /* 4) Extended Protocol */
    test_extended_parse();
    test_extended_bind();
    test_extended_execute();
    test_extended_describe();
    test_extended_sync_clears_pin();
    test_extended_full_cycle();

    /* 5) COPY */
    test_copy_in_lifecycle();
    test_copy_in_fail();
    test_copy_out_lifecycle();
    test_copy_both_response();

    /* 6) Multiple reads without transaction */
    test_multiple_reads_autocommit();

    /* 7) Multiple writes without transaction */
    test_multiple_writes_autocommit();

    /* 8) Reads within transaction */
    test_reads_within_transaction();

    /* 9) Mixed read/write in transaction */
    test_mixed_rw_in_transaction();
    test_transaction_rollback_cycle();

    /* 10) Corner cases */
    test_set_statement();
    test_reset_statement();
    test_discard_all();
    test_prepare_statement();
    test_listen_quarantine();
    test_temp_table_quarantine();
    test_declare_cursor_quarantine();
    test_failed_tx_z_error();
    test_failed_tx_recovery();
    test_terminate_message();
    test_flush_forwarded();
    test_close_forwarded();
    test_named_parse_pins_prepared_stmt();
    test_unnamed_parse_no_prepared_pin();
    test_close_clears_prepared_pin();
    test_close_with_remaining_stmts_no_clear();

    /* 12) Prepared statement pooling modes */
    test_ps_pinning_named_parse_sets_pin();
    test_ps_pinning_unnamed_parse_no_ps_pin();
    test_ps_tracking_prepare_stored_in_cache();
    test_ps_tracking_prepare_hard_pin_stripped();
    test_ps_tracking_prepare_with_param_types();
    test_ps_tracking_prepare_lowercase();
    test_ps_tracking_non_prepare_passes_through();
    test_ps_tracking_search_path_rehashes_stmt_set();
    test_ps_tracking_set_role_rehashes_stmt_set();
    test_ps_tracking_reset_role_rehashes_stmt_set();
    test_ps_tracking_stmt_compat_profile_hashes_update();
    test_ps_tracking_deallocate_keeps_pin_with_remaining_stmt();
    test_ps_tracking_ddl_invalidates_stmt_set();
    test_ps_tracking_discard_plans_invalidates_stmt_set();
    test_ps_tracking_discard_all_resets_guc_context();
    test_ps_tracking_unknown_semantic_marks_incompatible();
    test_ps_tracking_set_session_auth_rehashes_stmt_set();
    test_ps_tracking_reset_session_auth_rehashes_stmt_set();
    test_ps_tracking_set_timezone_rehashes_stmt_set();
    test_ps_tracking_set_datestyle_rehashes_stmt_set();
    test_ps_tracking_set_intervalstyle_rehashes_stmt_set();
    test_ps_tracking_set_standard_conforming_strings_rehashes_stmt_set();
    test_ps_tracking_set_backslash_quote_rehashes_stmt_set();
    test_ps_tracking_set_escape_string_warning_rehashes_stmt_set();
    test_ps_tracking_set_default_tablespace_rehashes_stmt_set();
    test_ps_tracking_set_temp_tablespaces_rehashes_stmt_set();
    test_ps_tracking_set_default_table_access_method_rehashes_stmt_set();
    test_ps_tracking_set_row_security_rehashes_stmt_set();
    test_ps_tracking_reset_all_clears_semantic_gucs();
    test_ps_tracking_set_local_search_path_rehashes_and_reverts();
    test_ps_tracking_set_config_local_search_path_rehashes_and_reverts();
    test_ps_tracking_set_config_session_search_path_rehashes_stmt_set();
    test_ps_tracking_set_config_session_timezone_rehashes_stmt_set();
    test_ps_tracking_extended_set_local_search_path_rehashes_and_reverts();
    test_ps_tracking_local_overlay_and_temp_shadow_rehash_order_on_rollback();
    test_ps_tracking_temp_table_bumps_temp_epoch();
    test_ps_tracking_temp_shadow_execute_discards_plans();
    test_ps_tracking_extended_temp_table_bumps_temp_epoch();
    test_ps_tracking_temp_on_commit_drop_rehashes_at_tx_end();
    test_ps_tracking_extended_temp_on_commit_drop_rehashes_at_tx_end();
    test_ps_tracking_temp_on_commit_delete_rows_rehashes_at_tx_end();
    test_ps_tracking_extended_temp_on_commit_delete_rows_rehashes_at_tx_end();
    test_ps_tracking_drop_table_rehashes_temp_context();
    test_ps_tracking_extended_drop_table_rehashes_temp_context();
    test_ps_tracking_discard_temp_rehashes_temp_context();
    test_ps_tracking_extended_discard_temp_rehashes_temp_context();
    test_ps_tracking_extended_discard_plans_invalidates_stmt_set();
    test_ps_tracking_extended_discard_all_resets_guc_context();
    test_ps_tracking_extended_ddl_invalidates_stmt_set();
    test_ps_tracking_temp_create_rehashes_again_on_rollback();
    test_ps_tracking_extended_temp_create_rehashes_again_on_rollback();
    test_ps_anon_named_parse_intercepted();
    test_ps_anon_named_parse_not_in_named_stmt_count();
    test_ps_anon_unnamed_parse_passes_through();
    test_ps_anon_bind_rewrites_to_anon_parse_bind();
    test_ps_anon_bind_unknown_stmt_passes_through();
    test_ps_anon_close_removes_map_entry();
    test_ps_anon_discard_all_clears_map();
    test_ps_anon_multiple_stmts_all_intercepted();

    test_password_forwarded();
    test_unknown_message_forwarded();
    test_regular_msg_too_short();
    test_multi_statement_query();
    test_multi_statement_with_begin();
    test_large_query_splice_eligible();
    test_small_query_not_splice_eligible();
    test_be_auth_ok();
    test_be_parameter_status();
    test_be_parameter_status_search_path_rehashes_stmt_set();
    test_be_parameter_status_timezone_rehashes_stmt_set();
    test_be_backend_key_data();
    test_be_too_short();
    test_be_unknown_message();
    test_be_is_data_frame();
    test_be_z_idle_clears_failed_tx();
    test_copy_in_within_transaction();
    test_extended_proto_in_transaction();
    test_ssl_then_startup();
    test_back_to_back_transactions();
    test_z_transitions_i_t_e_i();

    /* 11) Utility functions */
    test_fingerprint_basic();
    test_fingerprint_case_insensitive();
    test_fingerprint_whitespace_normalized();
    test_fingerprint_different();
    test_build_cleanup_fe_disconnect();
    test_build_cleanup_unknown_state();
    test_build_cleanup_failed_tx();
    test_build_cleanup_buffer_too_small();
    test_reuse_gate_fresh();
    test_generate_startup();
    test_generate_startup_buffer_too_small();
    test_generate_error();
    test_generate_error_buffer_too_small();

    /* 13) Replication / transaction tracking */
    test_txn_track_commit_rewrite();
    test_txn_track_no_rewrite_disabled();
    test_txn_track_absorbs_row_description();
    test_txn_track_captures_xid_from_datarow();
    test_txn_track_absorbs_select_commandcomplete();
    test_txn_track_commit_commandcomplete_forwarded();

    /* 14) Unknown-state classification (KEEL_QE_UNKNOWN_STATE) */
    test_do_block_marks_unknown_state();
    test_call_marks_unknown_state();
    test_merge_is_write_not_unknown();
    test_vacuum_is_write_not_unknown();

    /* 15) PG backend auth pure functions */
    test_pg_b64_roundtrip();
    test_pg_build_startup_message();
    test_pg_build_password_message();
    test_pg_sasl_initial_response();
    test_pg_sasl_response();
    test_pg_scram_client_first();

    /* 16) MySQL backend auth pure functions */
    test_my_scramble_native();
    test_my_scramble_caching_sha2();
    test_my_parse_greeting_valid();
    test_my_parse_greeting_invalid();
    test_my_parse_auth_result_ok();
    test_my_parse_auth_result_err();
    test_my_build_handshake_response();
    test_my_build_auth_switch_response();
    test_my_build_rsa_key_request();

    printf("\n");
    return test_summary();
}
