/**
 * @file test_mysql_protocol_flow.c
 * @brief Wire-level regression suite for the MySQL protocol flow plugin.
 *
 * MySQL protocol behavior differs sharply from PostgreSQL in framing,
 * handshake order, result-set structure, prepared-statement verbs, and cleanup
 * semantics. This suite therefore exercises the plugin directly with hand-built
 * packets so each protocol edge can be tested in isolation.
 *
 * Beyond raw parsing, the suite verifies KEEL's higher-level decisions around
 * transaction state, pinning triggers, result-set lifecycle, cleanup packet
 * generation, and backend error classification.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/plugin/plugin_types.h"
#include "keel/session/state_profile.h"
#include "keel/mem/mem.h"
#include <string.h>
#include <stdio.h>

/* The plugin vtable is exercised directly so failures stay attributable to the
 * MySQL flow contract rather than the surrounding worker/runtime machinery. */
extern const keel_proto_flow_vtable_t keel_proto_flow_mysql;

/* ---- Shorthand ---- */
#define VT (&keel_proto_flow_mysql)
#define MY_HDR 4

/* ============================================================================
 * Wire-protocol message builders (MySQL)
 * ============================================================================
 *
 * These helpers encode just enough of the MySQL wire format to express the test
 * scenario at hand. They are intentionally narrow and deterministic, which is
 * more valuable for regression tests than a feature-complete packet library.
 */

/**
 * @brief Write a 24-bit little-endian packet length field.
 *
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wrle24(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16);
}

/**
 * @brief Wrap a payload in a standard MySQL packet header.
 *
 * @param buf Destination buffer.
 * @param seq_id Packet sequence id.
 * @param payload Payload bytes.
 * @param plen Payload length.
 * @return Total packet length including the header.
 */
static size_t build_mysql_packet(uint8_t* buf, uint8_t seq_id,
                                  const uint8_t* payload, size_t plen) {
    wrle24(buf, (uint32_t)plen);
    buf[3] = seq_id;
    if (plen > 0) memcpy(buf + MY_HDR, payload, plen);
    return MY_HDR + plen;
}

/* Build a client handshake response:
 * caps(4) + max_pkt(4) + charset(1) + reserved(23) + user\0 + auth_len + db\0 */
static size_t build_handshake_response(uint8_t* buf, uint8_t seq_id,
                                        const char* user, const char* db) {
    uint8_t payload[256];
    size_t pos = 0;

    /* Capability flags */
    uint32_t caps = 0x0000f7ff | (1U << 19);  /* protocol41 + plugin_auth */
    if (db && db[0]) caps |= (1U << 3);       /* CONNECT_WITH_DB */
    payload[pos++] = (uint8_t)caps;
    payload[pos++] = (uint8_t)(caps >> 8);
    payload[pos++] = (uint8_t)(caps >> 16);
    payload[pos++] = (uint8_t)(caps >> 24);

    /* Max packet size = 16MB */
    payload[pos++] = 0xFF; payload[pos++] = 0xFF;
    payload[pos++] = 0xFF; payload[pos++] = 0x00;

    /* Charset: utf8mb4 */
    payload[pos++] = 0x2d;

    /* Reserved (23 zeros) */
    memset(payload + pos, 0, 23); pos += 23;

    /* Username (NUL-terminated) */
    size_t ul = strlen(user);
    memcpy(payload + pos, user, ul); pos += ul;
    payload[pos++] = 0;

    /* Auth response: length=0 */
    payload[pos++] = 0;

    /* Database (NUL-terminated) */
    if (db && db[0]) {
        size_t dl = strlen(db);
        memcpy(payload + pos, db, dl); pos += dl;
        payload[pos++] = 0;
    }

    return build_mysql_packet(buf, seq_id, payload, pos);
}

/* Build a COM_QUERY packet */
static size_t build_com_query(uint8_t* buf, uint8_t seq_id, const char* sql) {
    size_t sl = strlen(sql);
    uint8_t payload[4096];
    payload[0] = 0x03;  /* COM_QUERY */
    memcpy(payload + 1, sql, sl);
    return build_mysql_packet(buf, seq_id, payload, 1 + sl);
}

/* Build a COM_xxx single-byte command packet */
static size_t build_com_simple(uint8_t* buf, uint8_t seq_id, uint8_t cmd) {
    return build_mysql_packet(buf, seq_id, &cmd, 1);
}

/* Build a COM_xxx with string payload (e.g., COM_INIT_DB "dbname") */
static size_t build_com_with_payload(uint8_t* buf, uint8_t seq_id,
                                      uint8_t cmd, const char* payload_str) {
    size_t sl = strlen(payload_str);
    uint8_t payload[512];
    payload[0] = cmd;
    memcpy(payload + 1, payload_str, sl);
    return build_mysql_packet(buf, seq_id, payload, 1 + sl);
}

/* Build COM_STMT_PREPARE */
static size_t build_com_stmt_prepare(uint8_t* buf, uint8_t seq_id, const char* sql) {
    size_t sl = strlen(sql);
    uint8_t payload[4096];
    payload[0] = 0x16;  /* COM_STMT_PREPARE */
    memcpy(payload + 1, sql, sl);
    return build_mysql_packet(buf, seq_id, payload, 1 + sl);
}

/* Build COM_STMT_EXECUTE (stmt_id only, minimal) */
static size_t build_com_stmt_execute(uint8_t* buf, uint8_t seq_id, uint32_t stmt_id) {
    uint8_t payload[16];
    payload[0] = 0x17;  /* COM_STMT_EXECUTE */
    payload[1] = (uint8_t)stmt_id;
    payload[2] = (uint8_t)(stmt_id >> 8);
    payload[3] = (uint8_t)(stmt_id >> 16);
    payload[4] = (uint8_t)(stmt_id >> 24);
    payload[5] = 0;  /* flags: CURSOR_TYPE_NO_CURSOR */
    payload[6] = 1; payload[7] = 0; payload[8] = 0; payload[9] = 0; /* iteration count */
    return build_mysql_packet(buf, seq_id, payload, 10);
}

/* Build COM_STMT_CLOSE */
static size_t build_com_stmt_close(uint8_t* buf, uint8_t seq_id, uint32_t stmt_id) {
    uint8_t payload[5];
    payload[0] = 0x19;  /* COM_STMT_CLOSE */
    payload[1] = (uint8_t)stmt_id;
    payload[2] = (uint8_t)(stmt_id >> 8);
    payload[3] = (uint8_t)(stmt_id >> 16);
    payload[4] = (uint8_t)(stmt_id >> 24);
    return build_mysql_packet(buf, seq_id, payload, 5);
}

/* ---- Backend response builders ---- */

/* Build OK packet: [0x00][affected_rows_lenenc][last_insert_id_lenenc][status(2)][warnings(2)] */
static size_t build_ok(uint8_t* buf, uint8_t seq_id,
                        uint64_t affected_rows, uint64_t last_insert_id,
                        uint16_t status_flags) {
    uint8_t payload[32];
    size_t pos = 0;
    payload[pos++] = 0x00;  /* OK marker */

    /* affected_rows as lenenc */
    if (affected_rows < 0xFB) {
        payload[pos++] = (uint8_t)affected_rows;
    } else {
        payload[pos++] = 0xFC;
        payload[pos++] = (uint8_t)(affected_rows & 0xFF);
        payload[pos++] = (uint8_t)((affected_rows >> 8) & 0xFF);
    }

    /* last_insert_id as lenenc */
    if (last_insert_id < 0xFB) {
        payload[pos++] = (uint8_t)last_insert_id;
    } else {
        payload[pos++] = 0xFC;
        payload[pos++] = (uint8_t)(last_insert_id & 0xFF);
        payload[pos++] = (uint8_t)((last_insert_id >> 8) & 0xFF);
    }

    /* status_flags (2 bytes LE) */
    payload[pos++] = (uint8_t)(status_flags & 0xFF);
    payload[pos++] = (uint8_t)((status_flags >> 8) & 0xFF);

    /* warnings (2 bytes LE) */
    payload[pos++] = 0; payload[pos++] = 0;

    return build_mysql_packet(buf, seq_id, payload, pos);
}

/* Build ERR packet: [0xFF][errno(2)][#][sqlstate(5)][message] */
static size_t build_err(uint8_t* buf, uint8_t seq_id,
                         uint16_t errn, const char* sqlstate, const char* msg) {
    uint8_t payload[256];
    size_t pos = 0;
    payload[pos++] = 0xFF;
    payload[pos++] = (uint8_t)(errn & 0xFF);
    payload[pos++] = (uint8_t)((errn >> 8) & 0xFF);
    payload[pos++] = '#';
    memcpy(payload + pos, sqlstate, 5); pos += 5;
    size_t ml = strlen(msg);
    memcpy(payload + pos, msg, ml); pos += ml;
    return build_mysql_packet(buf, seq_id, payload, pos);
}

/* Build EOF packet: [0xFE][warnings(2)][status_flags(2)] */
static size_t build_eof(uint8_t* buf, uint8_t seq_id, uint16_t status_flags) {
    uint8_t payload[5];
    payload[0] = 0xFE;
    payload[1] = 0; payload[2] = 0;  /* warnings */
    payload[3] = (uint8_t)(status_flags & 0xFF);
    payload[4] = (uint8_t)((status_flags >> 8) & 0xFF);
    return build_mysql_packet(buf, seq_id, payload, 5);
}

/* Build LOCAL INFILE request: [0xFB][filename] */
static size_t build_local_infile(uint8_t* buf, uint8_t seq_id, const char* filename) {
    size_t fl = strlen(filename);
    uint8_t payload[256];
    payload[0] = 0xFB;
    memcpy(payload + 1, filename, fl);
    return build_mysql_packet(buf, seq_id, payload, 1 + fl);
}

/* Build a column-count packet (lenenc integer) */
static size_t build_column_count(uint8_t* buf, uint8_t seq_id, uint64_t count) {
    uint8_t payload[8];
    size_t pos = 0;
    if (count < 0xFB) {
        payload[pos++] = (uint8_t)count;
    } else {
        payload[pos++] = 0xFC;
        payload[pos++] = (uint8_t)(count & 0xFF);
        payload[pos++] = (uint8_t)((count >> 8) & 0xFF);
    }
    return build_mysql_packet(buf, seq_id, payload, pos);
}

/* Build a minimal column definition packet (just needs to be non-OK/ERR/EOF) */
static size_t build_column_def(uint8_t* buf, uint8_t seq_id) {
    /* Just put some non-marker bytes as payload */
    uint8_t payload[] = {0x03, 'd', 'e', 'f', 0x01, 'x', 0x01, 'y',
                          0x01, 'z', 0x01, 'a', 0x0C, 0x2d, 0x00,
                          0x14, 0x00, 0x00, 0x00, 0xFD, 0x00, 0x00,
                          0x1F, 0x00, 0x00};
    return build_mysql_packet(buf, seq_id, payload, sizeof(payload));
}

/* Build a data row packet (text resultset row) */
static size_t build_data_row(uint8_t* buf, uint8_t seq_id, const char* value) {
    /* lenenc string: [len][data] */
    size_t vl = strlen(value);
    uint8_t payload[256];
    payload[0] = (uint8_t)vl;
    memcpy(payload + 1, value, vl);
    return build_mysql_packet(buf, seq_id, payload, 1 + vl);
}

/* ---- Context bootstrap helper ----
 *
 * Many tests care about post-handshake command handling. This helper advances a
 * fresh flow context through the client handshake response phase so the test
 * can start from the first steady-state command. */
/**
 * @brief Create a MySQL flow context and feed it one client handshake response.
 *
 * @param user User name to advertise.
 * @param db Database to select.
 * @return Initialized flow context.
 */
static void* do_handshake(const char* user, const char* db) {
    void* ctx = VT->create_context(NULL);
    uint8_t buf[512];
    size_t len = build_handshake_response(buf, 1, user, db);
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);
    return ctx;
}

/* ============================================================================
 * 1) Frame Length Detection
 * ============================================================================ */

static void test_frame_len(void) {
    TEST_BEGIN("frame_len");
    void* ctx = VT->create_context(NULL);

    /* Incomplete header */
    uint8_t short_buf[] = {0x01, 0x00};
    TEST_ASSERT_EQ(VT->frame_len(ctx, short_buf, 2, 0), 0);

    /* 3 bytes header = still incomplete */
    uint8_t three_buf[] = {0x01, 0x00, 0x00};
    TEST_ASSERT_EQ(VT->frame_len(ctx, three_buf, 3, 0), 0);

    /* Exactly 4-byte header with payload_len=1 → total=5 */
    uint8_t pkt4[] = {0x01, 0x00, 0x00, 0x00, 0x0e};
    TEST_ASSERT_EQ(VT->frame_len(ctx, pkt4, 5, 0), 5);

    /* Zero-length payload → total=4 */
    uint8_t pkt0[] = {0x00, 0x00, 0x00, 0x00};
    TEST_ASSERT_EQ(VT->frame_len(ctx, pkt0, 4, 0), 4);

    /* Large payload: 0x000100 = 256 → total=260 */
    uint8_t pkt_large[] = {0x00, 0x01, 0x00, 0x00};
    TEST_ASSERT_EQ(VT->frame_len(ctx, pkt_large, 4, 0), 260);

    /* Max valid: 0xFFFFFF → total = 4 + 16777215 */
    uint8_t pkt_max[] = {0xFF, 0xFF, 0xFF, 0x00};
    TEST_ASSERT_EQ(VT->frame_len(ctx, pkt_max, 4, 0), (ssize_t)(MY_HDR + 0xFFFFFF));

    /* Direction should not matter */
    TEST_ASSERT_EQ(VT->frame_len(ctx, pkt4, 5, 1), 5);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 2) Handshake / Auth
 * ============================================================================ */

static void test_handshake_normal(void) {
    TEST_BEGIN("handshake_normal");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[512];
    size_t len = build_handshake_response(buf, 1, "testuser", "testdb");

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_AUTH_COMPLETE);
    TEST_ASSERT(act.fe_response != NULL);
    TEST_ASSERT(act.fe_response_len > 0);
    /* OK packet starts with: [hdr][0x00] */
    TEST_ASSERT_EQ(act.fe_response[MY_HDR], 0x00);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_handshake_short_packet(void) {
    TEST_BEGIN("handshake_short");
    void* ctx = VT->create_context(NULL);

    /* Packet too short for handshake (< MY_HDR + 32) */
    uint8_t buf[16];
    wrle24(buf, 10);
    buf[3] = 1;
    memset(buf + 4, 0, 10);

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, 14, &act);

    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_handshake_no_database(void) {
    TEST_BEGIN("handshake_no_db");
    void* ctx = VT->create_context(NULL);

    /* Build handshake without CONNECT_WITH_DB flag */
    uint8_t buf[512];
    size_t len = build_handshake_response(buf, 1, "admin", "");

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_AUTH_COMPLETE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 3) COM_QUERY: SQL Classification
 * ============================================================================ */

static void test_query_select(void) {
    TEST_BEGIN("query_select");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SELECT id FROM users WHERE active = 1");

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_SQL);
    TEST_ASSERT(act.effect & KEEL_QE_READONLY);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_REPLICA);
    TEST_ASSERT(act.cache_eligible);
    TEST_ASSERT(act.sql_view != NULL);
    TEST_ASSERT(act.sql_view_len > 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_show(void) {
    TEST_BEGIN("query_show");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SHOW DATABASES");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_READONLY);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_REPLICA);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_explain(void) {
    TEST_BEGIN("query_explain");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "EXPLAIN SELECT 1");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_READONLY);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_REPLICA);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_insert(void) {
    TEST_BEGIN("query_insert");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "INSERT INTO t(a) VALUES(1)");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_update(void) {
    TEST_BEGIN("query_update");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "UPDATE users SET active = 0 WHERE id = 1");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_delete(void) {
    TEST_BEGIN("query_delete");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "DELETE FROM sessions WHERE expired = 1");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_truncate(void) {
    TEST_BEGIN("query_truncate");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "TRUNCATE TABLE logs");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_ddl_create(void) {
    TEST_BEGIN("query_ddl_create");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "CREATE TABLE test (id INT PRIMARY KEY)");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_DDL);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);
    TEST_ASSERT_EQ(act.route_hint, KEEL_FROUTE_PRIMARY);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_ddl_alter(void) {
    TEST_BEGIN("query_ddl_alter");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "ALTER TABLE test ADD COLUMN name VARCHAR(50)");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_DDL);
    TEST_ASSERT(act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_ddl_drop(void) {
    TEST_BEGIN("query_ddl_drop");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "DROP TABLE IF EXISTS test");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_DDL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_begin(void) {
    TEST_BEGIN("query_begin");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "BEGIN");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_BEGINS_TX);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_TX_CONTROL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_commit(void) {
    TEST_BEGIN("query_commit");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "COMMIT");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_TX_CONTROL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_rollback(void) {
    TEST_BEGIN("query_rollback");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "ROLLBACK");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_set(void) {
    TEST_BEGIN("query_set");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET @@session.wait_timeout = 300");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_SETS_STATE);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_STATE_CHANGE);
    TEST_ASSERT(!act.cache_eligible);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 4) COM_STMT_* : Prepared Statements
 * ============================================================================ */

static void test_stmt_prepare(void) {
    TEST_BEGIN("stmt_prepare");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_stmt_prepare(buf, 0, "SELECT * FROM t WHERE id = ?");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_SQL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);
    TEST_ASSERT(act.sql_view != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_execute(void) {
    TEST_BEGIN("stmt_execute");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_stmt_execute(buf, 0, 1);

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT_EQ(act.msg_kind, KEEL_MSG_KIND_OTHER);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_close(void) {
    TEST_BEGIN("stmt_close");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_stmt_close(buf, 0, 1);

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_reset(void) {
    TEST_BEGIN("stmt_reset");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    uint8_t payload[] = {0x1a, 0x01, 0x00, 0x00, 0x00};  /* COM_STMT_RESET + stmt_id */
    size_t len = build_mysql_packet(buf, 0, payload, sizeof(payload));

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 5) COM_INIT_DB, COM_CHANGE_USER, COM_PING, COM_QUIT
 * ============================================================================ */

static void test_com_init_db(void) {
    TEST_BEGIN("com_init_db");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_with_payload(buf, 0, 0x02, "newdb");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_com_change_user(void) {
    TEST_BEGIN("com_change_user");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_with_payload(buf, 0, 0x11, "newuser");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_NEED_BACKEND_AUTH);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_com_ping(void) {
    TEST_BEGIN("com_ping");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_simple(buf, 0, 0x0e);

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_com_quit(void) {
    TEST_BEGIN("com_quit");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_simple(buf, 0, 0x01);

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_TERMINATE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 6) COM_RESET_CONNECTION, COM_SET_OPTION, COM_FIELD_LIST
 * ============================================================================ */

static void test_com_reset_connection(void) {
    TEST_BEGIN("com_reset_connection");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_simple(buf, 0, 0x1f);

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);
    /* Should clear transaction + prepared + quarantine pins */
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_PREPARED_STMT);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_com_set_option(void) {
    TEST_BEGIN("com_set_option");
    void* ctx = do_handshake("user", "db");

    /* COM_SET_OPTION: cmd(1) + option(2) */
    uint8_t payload[] = {0x1b, 0x00, 0x00};
    uint8_t buf[128];
    size_t len = build_mysql_packet(buf, 0, payload, sizeof(payload));

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_com_field_list(void) {
    TEST_BEGIN("com_field_list");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_com_with_payload(buf, 0, 0x04, "users");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 7) Backend Responses: OK, ERR, EOF, LOCAL INFILE
 * ============================================================================ */

static void test_be_ok_idle(void) {
    TEST_BEGIN("be_ok_idle");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    /* OK with status = 0x0002 (AUTOCOMMIT only, no IN_TRANS) */
    size_t len = build_ok(buf, 1, 0, 0, 0x0002);

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(act.tx_state_changed);
    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(act.backend_reusable);
    TEST_ASSERT(act.query_complete);
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_ok_in_transaction(void) {
    TEST_BEGIN("be_ok_in_tx");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    /* OK with status = 0x0003 (AUTOCOMMIT + IN_TRANS) */
    size_t len = build_ok(buf, 1, 0, 0, 0x0003);

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!act.backend_reusable);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(act.query_complete);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_ok_lenenc_large(void) {
    TEST_BEGIN("be_ok_lenenc_large");
    void* ctx = do_handshake("user", "db");

    /* Build OK with affected_rows=300 (needs 2-byte lenenc: 0xFC, 0x2C, 0x01)
     * and last_insert_id=0, status=0x0002 */
    uint8_t payload[32];
    size_t pos = 0;
    payload[pos++] = 0x00;  /* OK marker */
    payload[pos++] = 0xFC;  /* 2-byte lenenc prefix */
    payload[pos++] = 0x2C;  /* 300 & 0xFF */
    payload[pos++] = 0x01;  /* 300 >> 8 */
    payload[pos++] = 0x00;  /* last_insert_id = 0 */
    payload[pos++] = 0x02;  /* status_flags low: AUTOCOMMIT */
    payload[pos++] = 0x00;  /* status_flags high */
    payload[pos++] = 0x00;  /* warnings low */
    payload[pos++] = 0x00;  /* warnings high */

    uint8_t buf[64];
    size_t len = build_mysql_packet(buf, 1, payload, pos);

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(act.tx_state_changed);
    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(act.query_complete);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_err(void) {
    TEST_BEGIN("be_err");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1064, "42000", "Syntax error");

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_ERROR);
    TEST_ASSERT(act.query_complete);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_eof(void) {
    TEST_BEGIN("be_eof");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[64];
    /* EOF with status = 0x0002 (AUTOCOMMIT, no IN_TRANS) */
    size_t len = build_eof(buf, 5, 0x0002);

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    /* EOF in idle state → query complete */
    TEST_ASSERT(act.tx_state_changed);
    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(act.query_complete);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_eof_in_transaction(void) {
    TEST_BEGIN("be_eof_in_tx");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[64];
    /* EOF with IN_TRANS flag */
    size_t len = build_eof(buf, 5, 0x0003);

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!act.backend_reusable);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_local_infile(void) {
    TEST_BEGIN("be_local_infile");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    size_t len = build_local_infile(buf, 1, "/tmp/data.csv");

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(act.enters_copy_mode);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_COPY);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 8) Result-Set State Machine
 * ============================================================================ */

static void test_result_set_full_cycle(void) {
    TEST_BEGIN("result_set_cycle");
    void* ctx = do_handshake("user", "db");
    keel_be_action_t act;

    /* Step 1: Column count = 2 */
    uint8_t buf[256];
    size_t len = build_column_count(buf, 1, 2);
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_FORWARD_FE);
    TEST_ASSERT(!act.query_complete);

    /* Step 2: Column definition #1 */
    len = build_column_def(buf, 2);
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(!act.query_complete);

    /* Step 3: Column definition #2 */
    len = build_column_def(buf, 3);
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(!act.query_complete);

    /* Step 4: EOF (end of column defs) */
    len = build_eof(buf, 4, 0x0002);
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(!act.query_complete);  /* Waiting for data rows + final EOF */

    /* Step 5: Data row */
    len = build_data_row(buf, 5, "hello");
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(!act.query_complete);

    /* Step 6: Data row */
    len = build_data_row(buf, 6, "world");
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(!act.query_complete);

    /* Step 7: Final EOF (end of data rows) */
    len = build_eof(buf, 7, 0x0002);
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.query_complete);
    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_IDLE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 9) Transaction Lifecycle
 * ============================================================================ */

static void test_transaction_lifecycle(void) {
    TEST_BEGIN("tx_lifecycle");
    void* ctx = do_handshake("user", "db");
    keel_fe_action_t fe_act;
    keel_be_action_t be_act;
    uint8_t buf[512];

    /* BEGIN */
    size_t len = build_com_query(buf, 0, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fe_act);
    TEST_ASSERT(fe_act.effect & KEEL_QE_BEGINS_TX);
    TEST_ASSERT(fe_act.pin_update & KEEL_FPIN_TRANSACTION);

    /* Backend OK with IN_TRANS */
    len = build_ok(buf, 1, 0, 0, 0x0003);
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT_EQ(be_act.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!be_act.backend_reusable);

    /* Reuse gate should be false */
    TEST_ASSERT(!VT->backend_reuse_gate(ctx));

    /* INSERT in transaction */
    len = build_com_query(buf, 0, "INSERT INTO t(a) VALUES(1)");
    VT->on_fe_msg(ctx, buf, len, &fe_act);
    TEST_ASSERT(fe_act.effect & KEEL_QE_WRITE);

    /* Backend OK still in TX */
    len = build_ok(buf, 1, 1, 0, 0x0003);
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT_EQ(be_act.tx_status, KEEL_TX_ACTIVE);

    /* COMMIT */
    len = build_com_query(buf, 0, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fe_act);
    TEST_ASSERT(fe_act.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(fe_act.pin_clear & KEEL_FPIN_TRANSACTION);

    /* Backend OK without IN_TRANS */
    len = build_ok(buf, 1, 0, 0, 0x0002);
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT_EQ(be_act.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(be_act.backend_reusable);

    /* Reuse gate should be true */
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10) LOAD DATA LOCAL INFILE Lifecycle
 * ============================================================================ */

static void test_load_data_lifecycle(void) {
    TEST_BEGIN("load_data_lifecycle");
    void* ctx = do_handshake("user", "db");
    keel_be_action_t be_act;
    uint8_t buf[256];

    /* Backend sends LOCAL INFILE request */
    size_t len = build_local_infile(buf, 1, "/tmp/data.csv");
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT(be_act.enters_copy_mode);
    TEST_ASSERT(be_act.pin_update & KEEL_FPIN_COPY);

    /* Reuse gate should be false (in COPY) */
    TEST_ASSERT(!VT->backend_reuse_gate(ctx));

    /* Backend sends OK after data transfer */
    len = build_ok(buf, 3, 100, 0, 0x0002);
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT(be_act.exits_copy_mode);
    TEST_ASSERT(be_act.pin_clear & KEEL_FPIN_COPY);
    TEST_ASSERT(be_act.query_complete);

    /* Reuse gate should be true now */
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_load_data_err_abort(void) {
    TEST_BEGIN("load_data_err_abort");
    void* ctx = do_handshake("user", "db");
    keel_be_action_t be_act;
    uint8_t buf[256];

    /* Backend sends LOCAL INFILE request */
    size_t len = build_local_infile(buf, 1, "/tmp/data.csv");
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT(be_act.enters_copy_mode);

    /* Backend sends ERR (file not found, etc.) */
    len = build_err(buf, 2, 1017, "HY000", "File not found");
    VT->on_be_msg(ctx, buf, len, &be_act);
    TEST_ASSERT(be_act.exits_copy_mode);
    TEST_ASSERT(be_act.pin_clear & KEEL_FPIN_COPY);
    TEST_ASSERT(be_act.query_complete);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 11) Multi-Result Set
 * ============================================================================ */

static void test_multi_result_set(void) {
    TEST_BEGIN("multi_result_set");
    void* ctx = do_handshake("user", "db");
    keel_be_action_t act;
    uint8_t buf[256];

    /* First result: OK with MORE_RESULTS_EXISTS (bit 3) */
    size_t len = build_ok(buf, 1, 5, 0, 0x000A);  /* AUTOCOMMIT + MORE_RESULTS */
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(!act.query_complete);  /* More results coming */

    /* Second result: OK without MORE_RESULTS */
    len = build_ok(buf, 2, 3, 0, 0x0002);  /* AUTOCOMMIT only */
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.query_complete);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 12) Utility Functions
 * ============================================================================ */

static void test_fingerprint(void) {
    TEST_BEGIN("fingerprint");
    void* ctx = VT->create_context(NULL);

    /* Same SQL with different case → same fingerprint */
    uint64_t h1 = VT->fingerprint(ctx, "SELECT 1", 8);
    uint64_t h2 = VT->fingerprint(ctx, "select 1", 8);
    TEST_ASSERT_EQ(h1, h2);

    /* Different SQL → different fingerprint */
    uint64_t h3 = VT->fingerprint(ctx, "SELECT 2", 8);
    TEST_ASSERT(h1 != h3);

    /* Whitespace normalization: tabs/newlines → spaces */
    uint64_t h4 = VT->fingerprint(ctx, "SELECT\t1", 8);
    uint64_t h5 = VT->fingerprint(ctx, "SELECT 1", 8);
    TEST_ASSERT_EQ(h4, h5);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_general(void) {
    TEST_BEGIN("build_cleanup_general");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[64];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, sizeof(buf));

    /* Should be COM_RESET_CONNECTION (0x1f) = 5 bytes */
    TEST_ASSERT_EQ(n, 5);
    TEST_ASSERT_EQ(buf[4], 0x1f);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_tx_rollback(void) {
    TEST_BEGIN("build_cleanup_tx_rollback");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[64];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_TX_NOT_IDLE, buf, sizeof(buf));

    /* Should be COM_QUERY "ROLLBACK" */
    TEST_ASSERT(n > 5);
    TEST_ASSERT_EQ(buf[MY_HDR], 0x03);  /* COM_QUERY */
    TEST_ASSERT(memcmp(buf + MY_HDR + 1, "ROLLBACK", 8) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_failed_tx(void) {
    TEST_BEGIN("build_cleanup_failed_tx");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[64];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FAILED_TX, buf, sizeof(buf));

    /* Should also be ROLLBACK */
    TEST_ASSERT(n > 5);
    TEST_ASSERT_EQ(buf[MY_HDR], 0x03);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_cleanup_buffer_too_small(void) {
    TEST_BEGIN("build_cleanup_small_buf");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[3];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, -1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_reuse_gate(void) {
    TEST_BEGIN("reuse_gate");
    void* ctx = do_handshake("user", "db");

    /* After handshake: reusable, not in tx → gate open */
    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    /* Simulate entering transaction via OK with IN_TRANS */
    uint8_t buf[64];
    size_t len = build_ok(buf, 1, 0, 0, 0x0003);
    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    /* Should be pinned */
    TEST_ASSERT(!VT->backend_reuse_gate(ctx));

    /* Simulate leaving transaction */
    len = build_ok(buf, 2, 0, 0, 0x0002);
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT(VT->backend_reuse_gate(ctx));

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_startup(void) {
    TEST_BEGIN("generate_startup");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[512];
    ssize_t n = VT->generate_startup(ctx, "dbuser", "mydb", buf, sizeof(buf));

    TEST_ASSERT(n > MY_HDR + 32);
    /* Header seq_id should be 1 */
    TEST_ASSERT_EQ(buf[3], 1);
    /* Should contain username somewhere in payload */
    TEST_ASSERT(memmem(buf + MY_HDR, (size_t)n - MY_HDR, "dbuser", 6) != NULL);
    /* Should contain database */
    TEST_ASSERT(memmem(buf + MY_HDR, (size_t)n - MY_HDR, "mydb", 4) != NULL);
    /* Should contain auth plugin */
    TEST_ASSERT(memmem(buf + MY_HDR, (size_t)n - MY_HDR,
                        "mysql_native_password", 21) != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_startup_buffer_overflow(void) {
    TEST_BEGIN("generate_startup_overflow");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[8];  /* Too small */
    ssize_t n = VT->generate_startup(ctx, "user", "db", buf, sizeof(buf));
    TEST_ASSERT_EQ(n, -1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_error(void) {
    TEST_BEGIN("generate_error");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[256];
    ssize_t n = VT->generate_error(ctx, "42000", "Syntax error", buf, sizeof(buf));

    TEST_ASSERT(n > MY_HDR);
    TEST_ASSERT_EQ(buf[MY_HDR], 0xFF);  /* ERR marker */
    /* errno = 1105 (0x0451) */
    TEST_ASSERT_EQ(buf[MY_HDR + 1], 0x51);
    TEST_ASSERT_EQ(buf[MY_HDR + 2], 0x04);
    /* '#' marker */
    TEST_ASSERT_EQ(buf[MY_HDR + 3], '#');
    /* SQLSTATE "42000" */
    TEST_ASSERT(memcmp(buf + MY_HDR + 4, "42000", 5) == 0);
    /* Message text */
    TEST_ASSERT(memmem(buf + MY_HDR + 9, (size_t)n - MY_HDR - 9,
                        "Syntax error", 12) != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_error_default_sqlstate(void) {
    TEST_BEGIN("generate_error_default_state");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[256];
    ssize_t n = VT->generate_error(ctx, NULL, "Unknown", buf, sizeof(buf));

    TEST_ASSERT(n > 0);
    /* Default SQLSTATE should be HY000 */
    TEST_ASSERT(memcmp(buf + MY_HDR + 4, "HY000", 5) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_generate_error_buffer_overflow(void) {
    TEST_BEGIN("generate_error_overflow");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[8];
    ssize_t n = VT->generate_error(ctx, "HY000", "Some error", buf, sizeof(buf));
    TEST_ASSERT_EQ(n, -1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_build_state_sync(void) {
    TEST_BEGIN("build_state_sync");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[256];
    ssize_t n = VT->build_state_sync(ctx, NULL, NULL, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 13) Phase 5: get_info, classify_error, cleanup_slot, get_metrics
 * ============================================================================ */

static void test_get_info(void) {
    TEST_BEGIN("get_info");
    keel_plugin_info_t info;
    VT->get_info(&info);

    TEST_ASSERT_STR_EQ(info.name, "mysql");
    TEST_ASSERT_EQ(info.default_port, 3306);
    TEST_ASSERT_EQ(info.api_version, KEEL_PLUGIN_API_V1);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_TEXT_PROTOCOL);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_BINARY_PROTOCOL);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_EXTENDED_QUERY);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_PREPARED_DETECT);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_GTID);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_STREAMING_LOAD);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_AUTH_NATIVE);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_AUTH_CACHING_SHA2);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_DISCARD_ALL);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_PROBE_HEALTH);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_SSL);

    TEST_END();
}

static void test_classify_error_syntax(void) {
    TEST_BEGIN("classify_error_syntax");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1064, "42000", "Syntax error");

    keel_error_info_t info;
    int rc = VT->classify_error(ctx, buf, len, &info);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(info.error_class, KEEL_ERR_SQL_ERROR);
    TEST_ASSERT_EQ(info.error_code, 1064);
    TEST_ASSERT(info.connection_ok);
    TEST_ASSERT(info.sqlstate != NULL);
    TEST_ASSERT(memcmp(info.sqlstate, "42000", 5) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_too_many_connections(void) {
    TEST_BEGIN("classify_error_resource");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1040, "08004", "Too many connections");

    keel_error_info_t info;
    VT->classify_error(ctx, buf, len, &info);

    TEST_ASSERT_EQ(info.error_class, KEEL_ERR_RESOURCE_LIMIT);
    TEST_ASSERT_EQ(info.error_code, 1040);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_access_denied(void) {
    TEST_BEGIN("classify_error_auth");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1045, "28000", "Access denied for user");

    keel_error_info_t info;
    VT->classify_error(ctx, buf, len, &info);

    TEST_ASSERT_EQ(info.error_class, KEEL_ERR_AUTH_FAILURE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_shutdown(void) {
    TEST_BEGIN("classify_error_shutdown");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1053, "08S01", "Server shutdown in progress");

    keel_error_info_t info;
    VT->classify_error(ctx, buf, len, &info);

    TEST_ASSERT_EQ(info.error_class, KEEL_ERR_BACKEND_FATAL);
    TEST_ASSERT(!info.connection_ok);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_deadlock(void) {
    TEST_BEGIN("classify_error_deadlock");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1213, "40001", "Deadlock found");

    keel_error_info_t info;
    VT->classify_error(ctx, buf, len, &info);

    TEST_ASSERT_EQ(info.error_class, KEEL_ERR_TRANSIENT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_protocol(void) {
    TEST_BEGIN("classify_error_protocol");
    void* ctx = VT->create_context(NULL);

    uint8_t buf[256];
    size_t len = build_err(buf, 1, 1156, "08S01", "Got packets out of order");

    keel_error_info_t info;
    VT->classify_error(ctx, buf, len, &info);

    TEST_ASSERT_EQ(info.error_class, KEEL_ERR_PROTO_VIOLATION);
    TEST_ASSERT(!info.connection_ok);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_invalid_packet(void) {
    TEST_BEGIN("classify_error_invalid");
    void* ctx = VT->create_context(NULL);

    /* Not an ERR packet */
    uint8_t buf[16];
    size_t len = build_ok(buf, 1, 0, 0, 0x0002);

    keel_error_info_t info;
    int rc = VT->classify_error(ctx, buf, len, &info);
    TEST_ASSERT_EQ(rc, -1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_error_null_data(void) {
    TEST_BEGIN("classify_error_null");
    void* ctx = VT->create_context(NULL);

    keel_error_info_t info;
    int rc = VT->classify_error(ctx, NULL, 0, &info);
    TEST_ASSERT_EQ(rc, -1);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_cleanup_slot_full(void) {
    TEST_BEGIN("cleanup_slot_full");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[64];
    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_FULL, .timeout_ms = 0 };
    ssize_t n = VT->cleanup_slot(ctx, -1, NULL, opts, buf, sizeof(buf));

    /* Should be COM_RESET_CONNECTION */
    TEST_ASSERT_EQ(n, 5);
    TEST_ASSERT_EQ(buf[4], 0x1f);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_cleanup_slot_selective_empty(void) {
    TEST_BEGIN("cleanup_slot_selective_empty");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[256];
    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_SELECTIVE, .timeout_ms = 0 };
    /* No profile or empty count → nothing to clean */
    ssize_t n = VT->cleanup_slot(ctx, -1, NULL, opts, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_cleanup_slot_selective_with_params(void) {
    TEST_BEGIN("cleanup_slot_selective_params");
    void* ctx = do_handshake("user", "db");

    state_profile_t profile;
    state_profile_init(&profile);
    state_profile_set(&profile, "wait_timeout", "300");
    state_profile_set(&profile, "max_execution_time", "1000");

    uint8_t buf[1024];
    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_SELECTIVE, .timeout_ms = 0 };
    ssize_t n = VT->cleanup_slot(ctx, -1, &profile, opts, buf, sizeof(buf));

    /* Should produce COM_QUERY with SET @@session.xxx = DEFAULT statements */
    TEST_ASSERT(n > MY_HDR + 1);
    TEST_ASSERT_EQ(buf[MY_HDR], 0x03);  /* COM_QUERY */
    /* Should contain "SET @@session." */
    TEST_ASSERT(memmem(buf + MY_HDR + 1, (size_t)n - MY_HDR - 1,
                        "SET @@session.", 14) != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_cleanup_slot_destroy(void) {
    TEST_BEGIN("cleanup_slot_destroy");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[64];
    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_DESTROY, .timeout_ms = 0 };
    ssize_t n = VT->cleanup_slot(ctx, -1, NULL, opts, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_get_metrics(void) {
    TEST_BEGIN("get_metrics");
    void* ctx = do_handshake("user", "db");

    /* Do a SET query to increment state_changes counter */
    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET @@session.wait_timeout = 300");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    keel_plugin_metrics_t metrics;
    int rc = VT->get_metrics(ctx, &metrics);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(metrics.state_changes >= 1);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 14) Edge Cases / Corner Cases
 * ============================================================================ */

static void test_command_too_short(void) {
    TEST_BEGIN("command_too_short");
    void* ctx = do_handshake("user", "db");

    /* Packet with no payload byte after header */
    uint8_t buf[4];
    wrle24(buf, 0);
    buf[3] = 0;

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, 4, &act);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_msg_too_short(void) {
    TEST_BEGIN("be_msg_too_short");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[4];
    wrle24(buf, 0);
    buf[3] = 0;

    keel_be_action_t act;
    int rc = VT->on_be_msg(ctx, buf, 4, &act);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_ERROR);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_unknown_command(void) {
    TEST_BEGIN("unknown_command");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[128];
    uint8_t payload[] = {0xEE};  /* Unknown command byte */
    size_t len = build_mysql_packet(buf, 0, payload, 1);

    keel_fe_action_t act;
    int rc = VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_FORWARD_TO_BACKEND);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_vtable_identity(void) {
    TEST_BEGIN("vtable_identity");
    TEST_ASSERT_STR_EQ(VT->name, "mysql");
    TEST_ASSERT_EQ(VT->default_port, 3306);
    TEST_ASSERT(VT->create_context != NULL);
    TEST_ASSERT(VT->destroy_context != NULL);
    TEST_ASSERT(VT->frame_len != NULL);
    TEST_ASSERT(VT->on_fe_msg != NULL);
    TEST_ASSERT(VT->on_be_msg != NULL);
    TEST_ASSERT(VT->is_data_frame != NULL);
    TEST_ASSERT(VT->fingerprint != NULL);
    TEST_ASSERT(VT->build_cleanup != NULL);
    TEST_ASSERT(VT->backend_reuse_gate != NULL);
    TEST_ASSERT(VT->generate_startup != NULL);
    TEST_ASSERT(VT->generate_error != NULL);
    TEST_ASSERT(VT->get_info != NULL);
    TEST_ASSERT(VT->classify_error != NULL);
    TEST_ASSERT(VT->cleanup_slot != NULL);
    TEST_ASSERT(VT->probe_backend != NULL);
    TEST_ASSERT(VT->get_metrics != NULL);
    TEST_END();
}

static void test_be_is_data_frame(void) {
    TEST_BEGIN("be: is_data_frame classifier");

    TEST_ASSERT(VT->is_data_frame != NULL);

    /* Non-control first payload bytes = result-set text row → safe to skip */
    uint8_t row[MY_HDR + 1] = {5, 0, 0, 1, 0x04}; /* len=5, seq=1, fb=0x04 */
    TEST_ASSERT( VT->is_data_frame(NULL, row, sizeof(row)));
    row[MY_HDR] = 0x01;  /* 1-byte length-encoded column value */
    TEST_ASSERT( VT->is_data_frame(NULL, row, sizeof(row)));
    row[MY_HDR] = 0x7F;  /* ASCII printable */
    TEST_ASSERT( VT->is_data_frame(NULL, row, sizeof(row)));

    /* 0x00 = OK packet: carries tx state flags → must NOT skip */
    row[MY_HDR] = 0x00;
    TEST_ASSERT(!VT->is_data_frame(NULL, row, sizeof(row)));

    /* 0xFF = ERR packet → must NOT skip */
    row[MY_HDR] = 0xFF;
    TEST_ASSERT(!VT->is_data_frame(NULL, row, sizeof(row)));

    /* 0xFE = EOF packet (conservative: skip even the length-encoded edge case) */
    row[MY_HDR] = 0xFE;
    TEST_ASSERT(!VT->is_data_frame(NULL, row, sizeof(row)));

    /* Too-short header (need MY_HDR+1 bytes minimum) */
    uint8_t short_buf[MY_HDR] = {5, 0, 0, 1};
    TEST_ASSERT(!VT->is_data_frame(NULL, short_buf, MY_HDR));

    TEST_END();
}

static void test_context_lifecycle(void) {
    TEST_BEGIN("context_lifecycle");
    void* ctx = VT->create_context(NULL);
    TEST_ASSERT(ctx != NULL);
    VT->destroy_context(ctx);
    /* Destroy NULL should not crash */
    VT->destroy_context(NULL);
    TEST_END();
}

/* ============================================================================
 * 15) Hardpin / Quarantine Detection
 * ============================================================================ */

static void test_query_get_lock(void) {
    TEST_BEGIN("query_get_lock");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SELECT GET_LOCK('mylock', 10)");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_user_variable(void) {
    TEST_BEGIN("query_user_variable");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SELECT @my_var := 42");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_create_temp_table(void) {
    TEST_BEGIN("query_temp_table");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "CREATE TEMPORARY TABLE tmp (id INT)");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    /* Should have DDL + quarantine */
    TEST_ASSERT(act.effect & (KEEL_QE_DDL | KEEL_QE_WRITE));
    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_query_lock_tables(void) {
    TEST_BEGIN("query_lock_tables");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "LOCK TABLES users WRITE");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.effect & KEEL_QE_POTENTIALLY_STATEFUL);
    TEST_ASSERT(act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 16) SSV: SET statement key/value extraction (frontend)
 * ============================================================================ */

static void test_set_session_variable(void) {
    TEST_BEGIN("ssv: SET @@session.wait_timeout = 300");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET @@session.wait_timeout = 300");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    TEST_ASSERT(act.effect & KEEL_QE_SETS_STATE);
    TEST_ASSERT(act.has_state_delta);
    TEST_ASSERT_NOT_NULL(act.state_key);
    TEST_ASSERT_EQ(act.state_key_len, (size_t)12);  /* "wait_timeout" */
    TEST_ASSERT(memcmp(act.state_key, "wait_timeout", 12) == 0);
    TEST_ASSERT_NOT_NULL(act.state_value);
    TEST_ASSERT_EQ(act.state_value_len, (size_t)3);
    TEST_ASSERT(memcmp(act.state_value, "300", 3) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_set_global_variable(void) {
    TEST_BEGIN("ssv: SET @@global.max_connections = 200");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET @@global.max_connections = 200");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.has_state_delta);
    TEST_ASSERT_EQ(act.state_key_len, (size_t)15);
    TEST_ASSERT(memcmp(act.state_key, "max_connections", 15) == 0);
    TEST_ASSERT_EQ(act.state_value_len, (size_t)3);
    TEST_ASSERT(memcmp(act.state_value, "200", 3) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_set_bare_variable(void) {
    TEST_BEGIN("ssv: SET autocommit = 1");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET autocommit = 1");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.has_state_delta);
    TEST_ASSERT_EQ(act.state_key_len, (size_t)10);
    TEST_ASSERT(memcmp(act.state_key, "autocommit", 10) == 0);
    TEST_ASSERT_EQ(act.state_value_len, (size_t)1);
    TEST_ASSERT(memcmp(act.state_value, "1", 1) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_set_names(void) {
    TEST_BEGIN("ssv: SET NAMES utf8mb4");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET NAMES utf8mb4");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.has_state_delta);
    TEST_ASSERT_EQ(act.state_key_len, (size_t)20);
    TEST_ASSERT(memcmp(act.state_key, "character_set_client", 20) == 0);
    TEST_ASSERT_EQ(act.state_value_len, (size_t)7);
    TEST_ASSERT(memcmp(act.state_value, "utf8mb4", 7) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_set_quoted_value(void) {
    TEST_BEGIN("ssv: SET @@session.time_zone = '+00:00'");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET @@session.time_zone = '+00:00'");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.has_state_delta);
    TEST_ASSERT_EQ(act.state_key_len, (size_t)9);
    TEST_ASSERT(memcmp(act.state_key, "time_zone", 9) == 0);
    /* Quoted value should have quotes stripped */
    TEST_ASSERT_EQ(act.state_value_len, (size_t)6);
    TEST_ASSERT(memcmp(act.state_value, "+00:00", 6) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_set_at_at_variable(void) {
    TEST_BEGIN("ssv: SET @@sql_mode = 'STRICT_TRANS_TABLES'");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SET @@sql_mode = 'STRICT_TRANS_TABLES'");

    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.has_state_delta);
    TEST_ASSERT_EQ(act.state_key_len, (size_t)8);
    TEST_ASSERT(memcmp(act.state_key, "sql_mode", 8) == 0);
    TEST_ASSERT_EQ(act.state_value_len, (size_t)19);
    TEST_ASSERT(memcmp(act.state_value, "STRICT_TRANS_TABLES", 19) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 17) SSV: SESSION_TRACK parsing in OK packets (backend)
 * ============================================================================ */

/**
 * @brief Build an OK packet with SESSION_TRACK data for a system variable change.
 *
 * Layout (CLIENT_SESSION_TRACK style):
 *   [0x00][affected_rows=0][last_insert_id=0][status_flags(2)][warnings(2)]
 *   [info_string(lenenc)][session_state_changes(lenenc)]
 *
 * A SESSION_TRACK_SYSTEM_VARIABLES (type 0) entry:
 *   [type=0][data_len(lenenc)][name(lenenc_str)][value(lenenc_str)]
 */
static size_t build_ok_with_session_track(uint8_t* buf, uint8_t seq_id,
                                           uint16_t status_flags,
                                           const char* var_name,
                                           const char* var_value) {
    uint8_t payload[512];
    size_t pos = 0;

    payload[pos++] = 0x00;  /* OK marker */
    payload[pos++] = 0x00;  /* affected_rows = 0 */
    payload[pos++] = 0x00;  /* last_insert_id = 0 */

    /* status_flags with SESSION_STATE_CHANGED bit set */
    uint16_t sf = status_flags | (1U << 14);  /* SERVER_SESSION_STATE_CHANGED */
    payload[pos++] = (uint8_t)(sf & 0xFF);
    payload[pos++] = (uint8_t)((sf >> 8) & 0xFF);

    /* warnings = 0 */
    payload[pos++] = 0; payload[pos++] = 0;

    /* info string (empty) */
    payload[pos++] = 0x00;  /* lenenc 0 = empty string */

    /* Build the session_state_changes block.
     * Type 0 entry: [0x00][entry_data_len][name_lenenc][value_lenenc] */
    size_t nlen = strlen(var_name);
    size_t vlen = strlen(var_value);

    /* entry_data = [name_lenenc_byte][name_bytes][value_lenenc_byte][value_bytes] */
    size_t entry_data_len = 1 + nlen + 1 + vlen;

    /* The entry: [type=0][entry_data_len_lenenc][entry_data_bytes] */
    size_t entry_total = 1 + 1 + entry_data_len;  /* type + lenenc(entry_data_len) + data */

    /* session_state_changes block: [lenenc(entry_total)][entry_bytes] */
    payload[pos++] = (uint8_t)entry_total;  /* lenenc prefix for whole block */

    /* Entry: type = SESSION_TRACK_SYSTEM_VARIABLES */
    payload[pos++] = 0x00;

    /* Entry data length */
    payload[pos++] = (uint8_t)entry_data_len;

    /* Variable name (lenenc string) */
    payload[pos++] = (uint8_t)nlen;
    memcpy(payload + pos, var_name, nlen); pos += nlen;

    /* Variable value (lenenc string) */
    payload[pos++] = (uint8_t)vlen;
    memcpy(payload + pos, var_value, vlen); pos += vlen;

    return build_mysql_packet(buf, seq_id, payload, pos);
}

static void test_be_ok_session_track_sysvar(void) {
    TEST_BEGIN("ssv: OK with SESSION_TRACK system variable");
    void* ctx = do_handshake("user", "db");

    /* Send a SET query first to put flow in correct state */
    uint8_t qbuf[512];
    size_t qlen = build_com_query(qbuf, 0, "SET @@session.wait_timeout = 300");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, qbuf, qlen, &fe_act);

    /* Now feed the OK response with SESSION_TRACK */
    uint8_t buf[512];
    size_t len = build_ok_with_session_track(buf, 1, 0x0002, /* AUTOCOMMIT */
                                              "wait_timeout", "300");

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.query_complete);
    TEST_ASSERT(act.has_profile_update);
    TEST_ASSERT_NOT_NULL(act.profile_key);
    TEST_ASSERT_EQ(act.profile_key_len, (size_t)12);
    TEST_ASSERT(memcmp(act.profile_key, "wait_timeout", 12) == 0);
    TEST_ASSERT_NOT_NULL(act.profile_value);
    TEST_ASSERT_EQ(act.profile_value_len, (size_t)3);
    TEST_ASSERT(memcmp(act.profile_value, "300", 3) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_ok_session_track_charset(void) {
    TEST_BEGIN("ssv: OK with SESSION_TRACK character_set_client");
    void* ctx = do_handshake("user", "db");

    uint8_t qbuf[512];
    size_t qlen = build_com_query(qbuf, 0, "SET NAMES utf8mb4");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, qbuf, qlen, &fe_act);

    uint8_t buf[512];
    size_t len = build_ok_with_session_track(buf, 1, 0x0002,
                                              "character_set_client", "utf8mb4");

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT(act.has_profile_update);
    TEST_ASSERT_EQ(act.profile_key_len, (size_t)20);
    TEST_ASSERT(memcmp(act.profile_key, "character_set_client", 20) == 0);
    TEST_ASSERT_EQ(act.profile_value_len, (size_t)7);
    TEST_ASSERT(memcmp(act.profile_value, "utf8mb4", 7) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_ok_no_session_track(void) {
    TEST_BEGIN("ssv: OK without SESSION_STATE_CHANGED has no profile_update");
    void* ctx = do_handshake("user", "db");

    uint8_t qbuf[512];
    size_t qlen = build_com_query(qbuf, 0, "SELECT 1");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, qbuf, qlen, &fe_act);

    /* Plain OK without SESSION_STATE_CHANGED flag */
    uint8_t buf[512];
    size_t len = build_ok(buf, 1, 0, 0, 0x0002);

    keel_be_action_t act;
    VT->on_be_msg(ctx, buf, len, &act);

    TEST_ASSERT(!act.has_profile_update);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 18) Prepared-statement tracking (PS map, stmt_active_count, session_stmt_hash)
 * ============================================================================ */

/* Build a PREPARE_OK response:
 * [0x00][stmt_id(4)][num_cols(2)][num_params(2)][0x00][warnings(2)] */
static size_t build_prepare_ok(uint8_t* buf, uint8_t seq_id,
                                uint32_t stmt_id,
                                uint16_t num_cols, uint16_t num_params) {
    uint8_t payload[12];
    payload[0]  = 0x00;                         /* OK marker */
    payload[1]  = (uint8_t)stmt_id;
    payload[2]  = (uint8_t)(stmt_id >> 8);
    payload[3]  = (uint8_t)(stmt_id >> 16);
    payload[4]  = (uint8_t)(stmt_id >> 24);
    payload[5]  = (uint8_t)(num_cols & 0xFF);
    payload[6]  = (uint8_t)((num_cols >> 8) & 0xFF);
    payload[7]  = (uint8_t)(num_params & 0xFF);
    payload[8]  = (uint8_t)((num_params >> 8) & 0xFF);
    payload[9]  = 0x00;                         /* reserved */
    payload[10] = 0x00; payload[11] = 0x00;     /* warnings */
    return build_mysql_packet(buf, seq_id, payload, 12);
}

/* Helper: feed a COM_STMT_PREPARE followed by PREPARE_OK(0 cols, 0 params). */
static void do_prepare_and_ok(void* ctx, const char* sql, uint32_t stmt_id) {
    uint8_t buf[512];
    keel_fe_action_t fe_act;
    keel_be_action_t be_act;

    /* Client sends COM_STMT_PREPARE */
    size_t flen = build_com_stmt_prepare(buf, 0, sql);
    VT->on_fe_msg(ctx, buf, flen, &fe_act);

    /* Backend responds with PREPARE_OK (no params, no cols) */
    size_t blen = build_prepare_ok(buf, 1, stmt_id, 0, 0);
    VT->on_be_msg(ctx, buf, blen, &be_act);
}

static void test_stmt_prepare_registers_stmt(void) {
    TEST_BEGIN("ps: COM_STMT_PREPARE+PREPARE_OK registers statement");
    void* ctx = do_handshake("user", "db");

    do_prepare_and_ok(ctx, "SELECT ? FROM t", 42);

    /* get_stmt_replay must return exactly one packet for stmt_id=42 */
    uint8_t* rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t rhash = 0;
    int rc = VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &rhash);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(rcnt, 1u);
    TEST_ASSERT(rbuf != NULL);
    /* The packet must be a COM_STMT_PREPARE (0x16) */
    TEST_ASSERT_EQ(rbuf[MY_HDR], 0x16);
    keel_free(rbuf);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_close_clears_pin_when_last(void) {
    TEST_BEGIN("ps: COM_STMT_CLOSE clears PREPARED_STMT pin when last stmt closed");
    void* ctx = do_handshake("user", "db");

    do_prepare_and_ok(ctx, "SELECT ? FROM t", 7);

    /* Close the only prepared statement */
    uint8_t buf[64];
    size_t len = build_com_stmt_close(buf, 0, 7);
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT(fe_act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_close_preserves_pin_with_multiple(void) {
    TEST_BEGIN("ps: COM_STMT_CLOSE preserves pin when other stmts remain");
    void* ctx = do_handshake("user", "db");

    do_prepare_and_ok(ctx, "SELECT ? FROM t", 1);
    do_prepare_and_ok(ctx, "INSERT INTO t VALUES (?)", 2);

    /* Close only the first statement */
    uint8_t buf[64];
    size_t len = build_com_stmt_close(buf, 0, 1);
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    /* Pin must NOT be cleared because stmt_id=2 is still live */
    TEST_ASSERT(!(fe_act.pin_clear & KEEL_FPIN_PREPARED_STMT));

    /* Closing the second clears it */
    len = build_com_stmt_close(buf, 0, 2);
    VT->on_fe_msg(ctx, buf, len, &fe_act);
    TEST_ASSERT(fe_act.pin_clear & KEEL_FPIN_PREPARED_STMT);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_reset_connection_clears_map(void) {
    TEST_BEGIN("ps: COM_RESET_CONNECTION clears stmt map");
    void* ctx = do_handshake("user", "db");

    do_prepare_and_ok(ctx, "SELECT ? FROM t", 99);

    /* Trigger COM_RESET_CONNECTION */
    uint8_t buf[64];
    size_t len = build_com_simple(buf, 0, 0x1f /* COM_RESET_CONNECTION */);
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    /* After reset, get_stmt_replay must return nothing */
    uint8_t* rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t rhash = 0;
    VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &rhash);
    TEST_ASSERT_EQ(rcnt, 0u);
    TEST_ASSERT(rbuf == NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_replay_empty(void) {
    TEST_BEGIN("ps: get_stmt_replay returns empty for fresh session");
    void* ctx = do_handshake("user", "db");

    uint8_t* rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t rhash = 0;
    int rc = VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &rhash);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(rcnt, 0u);
    TEST_ASSERT(rbuf == NULL);
    TEST_ASSERT_EQ(rlen, 0u);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_stmt_replay_active(void) {
    TEST_BEGIN("ps: get_stmt_replay returns correct packets for active stmts");
    void* ctx = do_handshake("user", "db");

    do_prepare_and_ok(ctx, "SELECT ? FROM t", 3);
    do_prepare_and_ok(ctx, "UPDATE t SET x=?", 4);

    uint8_t* rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t rhash = 0;
    int rc = VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &rhash);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(rcnt, 2u);
    TEST_ASSERT(rbuf != NULL);
    TEST_ASSERT(rlen > 0);

    /* Both packets must start with COM_STMT_PREPARE (0x16) */
    size_t pos = 0;
    uint32_t found = 0;
    while (pos + MY_HDR < rlen) {
        uint32_t pl = (uint32_t)rbuf[pos]
                    | ((uint32_t)rbuf[pos+1] << 8)
                    | ((uint32_t)rbuf[pos+2] << 16);
        TEST_ASSERT_EQ(rbuf[pos + MY_HDR], 0x16);
        pos += MY_HDR + pl;
        found++;
    }
    TEST_ASSERT_EQ(found, 2u);

    keel_free(rbuf);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 19) SESSION_TRACK_SCHEMA and SESSION_TRACK_GTIDS in OK packets
 * ============================================================================ */

/**
 * @brief Build an OK with a SESSION_TRACK_SCHEMA entry (type 1).
 *
 * Entry format:
 *   [type=1][entry_len(lenenc)][schema_name(lenenc_str)]
 */
static size_t build_ok_with_session_track_schema(uint8_t* buf, uint8_t seq_id,
                                                   uint16_t status_flags,
                                                   const char* schema) {
    uint8_t payload[256];
    size_t pos = 0;

    payload[pos++] = 0x00;  /* OK marker */
    payload[pos++] = 0x00;  /* affected_rows = 0 */
    payload[pos++] = 0x00;  /* last_insert_id = 0 */

    uint16_t sf = status_flags | (1U << 14);  /* SESSION_STATE_CHANGED */
    payload[pos++] = (uint8_t)(sf & 0xFF);
    payload[pos++] = (uint8_t)((sf >> 8) & 0xFF);
    payload[pos++] = 0; payload[pos++] = 0;  /* warnings */

    /* info string (empty) */
    payload[pos++] = 0x00;

    /* Build entry: [type=1][entry_len][schema_lenenc_str] */
    size_t slen = strlen(schema);
    /* entry_data: [schema_len_byte(1)][schema_bytes] */
    size_t entry_data_len = 1 + slen;
    size_t entry_total    = 1 + 1 + entry_data_len;  /* type + lenenc(data_len) + data */

    payload[pos++] = (uint8_t)entry_total;  /* session_state block len (lenenc) */
    payload[pos++] = 0x01;                  /* type = SESSION_TRACK_SCHEMA */
    payload[pos++] = (uint8_t)entry_data_len;
    payload[pos++] = (uint8_t)slen;
    memcpy(payload + pos, schema, slen); pos += slen;

    return build_mysql_packet(buf, seq_id, payload, pos);
}

/**
 * @brief Build an OK with a SESSION_TRACK_GTIDS entry (type 3).
 *
 * Entry format:
 *   [type=3][entry_len(lenenc)][encoding_spec(1)][gtid(lenenc_str)]
 */
static size_t build_ok_with_session_track_gtids(uint8_t* buf, uint8_t seq_id,
                                                  uint16_t status_flags,
                                                  const char* gtid) {
    uint8_t payload[512];
    size_t pos = 0;

    payload[pos++] = 0x00;  /* OK marker */
    payload[pos++] = 0x00;
    payload[pos++] = 0x00;

    uint16_t sf = status_flags | (1U << 14);
    payload[pos++] = (uint8_t)(sf & 0xFF);
    payload[pos++] = (uint8_t)((sf >> 8) & 0xFF);
    payload[pos++] = 0; payload[pos++] = 0;

    /* info string (empty) */
    payload[pos++] = 0x00;

    /* Build entry: [type=3][entry_len][0x00][gtid_lenenc_str] */
    size_t glen = strlen(gtid);
    /* entry_data: [0x00 enc_spec][gtid_len_byte][gtid_bytes] */
    size_t entry_data_len = 1 + 1 + glen;
    size_t entry_total    = 1 + 1 + entry_data_len;

    payload[pos++] = (uint8_t)entry_total;
    payload[pos++] = 0x03;  /* type = SESSION_TRACK_GTIDS */
    payload[pos++] = (uint8_t)entry_data_len;
    payload[pos++] = 0x00;  /* encoding_spec = TEXT */
    payload[pos++] = (uint8_t)glen;
    memcpy(payload + pos, gtid, glen); pos += glen;

    return build_mysql_packet(buf, seq_id, payload, pos);
}

static void test_be_ok_session_track_schema(void) {
    TEST_BEGIN("ssv: OK with SESSION_TRACK_SCHEMA updates ctx->database");
    void* ctx = do_handshake("user", "db");

    uint8_t qbuf[512];
    keel_fe_action_t fe_act;
    size_t qlen = build_com_query(qbuf, 0, "USE newdb");
    VT->on_fe_msg(ctx, qbuf, qlen, &fe_act);

    uint8_t buf[512];
    size_t len = build_ok_with_session_track_schema(buf, 1, 0x0002, "newdb");
    keel_be_action_t be_act;
    VT->on_be_msg(ctx, buf, len, &be_act);

    /* The profile update must reflect the new schema name */
    TEST_ASSERT(be_act.has_profile_update);
    TEST_ASSERT(be_act.profile_key != NULL);
    TEST_ASSERT(be_act.profile_value != NULL);
    /* The value must equal "newdb" */
    TEST_ASSERT(strncmp(be_act.profile_value, "newdb", be_act.profile_value_len) == 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_be_ok_session_track_gtids(void) {
    TEST_BEGIN("ssv: OK with SESSION_TRACK_GTIDS captures write GTID");
    void* ctx = do_handshake("user", "db");

    uint8_t qbuf[512];
    keel_fe_action_t fe_act;
    size_t qlen = build_com_query(qbuf, 0, "INSERT INTO t VALUES (1)");
    VT->on_fe_msg(ctx, qbuf, qlen, &fe_act);

    const char* gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42";
    uint8_t buf[512];
    size_t len = build_ok_with_session_track_gtids(buf, 1, 0x0002, gtid);
    keel_be_action_t be_act;
    VT->on_be_msg(ctx, buf, len, &be_act);

    /* After GTID tracking, SELECT @keel_write_gtid must return the captured GTID */
    uint8_t qbuf2[512];
    size_t qlen2 = build_com_query(qbuf2, 0, "SELECT @keel_write_gtid");
    keel_fe_action_t fe_act2;
    VT->on_fe_msg(ctx, qbuf2, qlen2, &fe_act2);

    /* Should be intercepted and returned synthetically */
    TEST_ASSERT_EQ(fe_act2.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT(fe_act2.fe_response != NULL);
    TEST_ASSERT(fe_act2.fe_response_len > 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 20) notify_write_lsn stores GTID for RYW intercepts
 * ============================================================================ */

static void test_notify_write_lsn_stores_value(void) {
    TEST_BEGIN("ryw: notify_write_lsn stores GTID");
    void* ctx = do_handshake("user", "db");

    const char* gtid = "12345678-1234-1234-1234-123456789abc:1-10";
    VT->notify_write_lsn(ctx, gtid);

    /* SELECT @keel_write_gtid should now be intercepted with the stored value */
    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SELECT @keel_write_gtid");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT_EQ(fe_act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT(fe_act.fe_response != NULL);
    TEST_ASSERT(fe_act.fe_response_len > 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 21) RYW: SET @keel_write_gtid and SELECT @keel_write_gtid intercepts
 * ============================================================================ */

static void test_ryw_set_keel_write_gtid(void) {
    TEST_BEGIN("ryw: SET @keel_write_gtid intercepted with inject_consistency_lsn");
    void* ctx = do_handshake("user", "db");

    const char* sql = "SET @keel_write_gtid = 'aabb-ccdd:1'";
    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, sql);
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    /* Must be intercepted — no round-trip to backend */
    TEST_ASSERT_EQ(fe_act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT(fe_act.fe_response != NULL);
    /* inject_consistency_lsn must contain the GTID */
    TEST_ASSERT(strlen(fe_act.inject_consistency_lsn) > 0);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_select_keel_write_gtid(void) {
    TEST_BEGIN("ryw: SELECT @keel_write_gtid returns stored value");
    void* ctx = do_handshake("user", "db");

    /* First store a value */
    VT->notify_write_lsn(ctx, "uuid:1-5");

    /* SELECT @keel_write_gtid should be intercepted */
    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "SELECT @keel_write_gtid");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT_EQ(fe_act.type, KEEL_FE_ACT_SEND_FE);

    /* Also test the @@ variant */
    len = build_com_query(buf, 0, "SELECT @@keel_write_gtid");
    keel_fe_action_t fe_act2;
    VT->on_fe_msg(ctx, buf, len, &fe_act2);
    TEST_ASSERT_EQ(fe_act2.type, KEEL_FE_ACT_SEND_FE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 22) classify_sql_mysql: CALL, DO, XA
 * ============================================================================ */

static void test_classify_call_unknown_state(void) {
    TEST_BEGIN("classify: CALL gets KEEL_QE_UNKNOWN_STATE + QUARANTINE");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "CALL my_proc(1, 2)");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT(fe_act.effect & KEEL_QE_UNKNOWN_STATE);
    TEST_ASSERT(fe_act.pin_update & KEEL_FPIN_QUARANTINE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_do_is_write(void) {
    TEST_BEGIN("classify: DO is a write effect");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "DO SLEEP(0)");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT(fe_act.effect & KEEL_QE_WRITE);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_xa_start(void) {
    TEST_BEGIN("classify: XA START begins transaction with HARD_PIN");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "XA START 'trx1'");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT(fe_act.effect & KEEL_QE_BEGINS_TX);
    TEST_ASSERT(fe_act.effect & KEEL_QE_HARD_PIN);

    VT->destroy_context(ctx);
    TEST_END();
}

static void test_classify_xa_commit(void) {
    TEST_BEGIN("classify: XA COMMIT ends transaction with HARD_PIN");
    void* ctx = do_handshake("user", "db");

    uint8_t buf[512];
    size_t len = build_com_query(buf, 0, "XA COMMIT 'trx1'");
    keel_fe_action_t fe_act;
    VT->on_fe_msg(ctx, buf, len, &fe_act);

    TEST_ASSERT(fe_act.effect & KEEL_QE_ENDS_TX);
    TEST_ASSERT(fe_act.effect & KEEL_QE_HARD_PIN);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * main()
 * ============================================================================ */

int main(void)
{
    printf("=== MySQL Protocol Flow Tests ===\n\n");

    /* 1) Frame length */
    test_frame_len();

    /* 2) Handshake */
    test_handshake_normal();
    test_handshake_short_packet();
    test_handshake_no_database();

    /* 3) COM_QUERY: SQL classification */
    test_query_select();
    test_query_show();
    test_query_explain();
    test_query_insert();
    test_query_update();
    test_query_delete();
    test_query_truncate();
    test_query_ddl_create();
    test_query_ddl_alter();
    test_query_ddl_drop();
    test_query_begin();
    test_query_commit();
    test_query_rollback();
    test_query_set();

    /* 4) COM_STMT_* */
    test_stmt_prepare();
    test_stmt_execute();
    test_stmt_close();
    test_stmt_reset();

    /* 5) Other commands */
    test_com_init_db();
    test_com_change_user();
    test_com_ping();
    test_com_quit();

    /* 6) Session control commands */
    test_com_reset_connection();
    test_com_set_option();
    test_com_field_list();

    /* 7) Backend responses */
    test_be_ok_idle();
    test_be_ok_in_transaction();
    test_be_ok_lenenc_large();
    test_be_err();
    test_be_eof();
    test_be_eof_in_transaction();
    test_be_local_infile();

    /* 8) Result-set state machine */
    test_result_set_full_cycle();

    /* 9) Transaction lifecycle */
    test_transaction_lifecycle();

    /* 10) LOAD DATA lifecycle */
    test_load_data_lifecycle();
    test_load_data_err_abort();

    /* 11) Multi-result set */
    test_multi_result_set();

    /* 12) Utility functions */
    test_fingerprint();
    test_build_cleanup_general();
    test_build_cleanup_tx_rollback();
    test_build_cleanup_failed_tx();
    test_build_cleanup_buffer_too_small();
    test_reuse_gate();
    test_generate_startup();
    test_generate_startup_buffer_overflow();
    test_generate_error();
    test_generate_error_default_sqlstate();
    test_generate_error_buffer_overflow();
    test_build_state_sync();

    /* 13) Phase 5 */
    test_get_info();
    test_classify_error_syntax();
    test_classify_error_too_many_connections();
    test_classify_error_access_denied();
    test_classify_error_shutdown();
    test_classify_error_deadlock();
    test_classify_error_protocol();
    test_classify_error_invalid_packet();
    test_classify_error_null_data();
    test_cleanup_slot_full();
    test_cleanup_slot_selective_empty();
    test_cleanup_slot_selective_with_params();
    test_cleanup_slot_destroy();
    test_get_metrics();

    /* 14) Edge cases */
    test_command_too_short();
    test_be_msg_too_short();
    test_unknown_command();
    test_vtable_identity();
    test_be_is_data_frame();
    test_context_lifecycle();

    /* 15) Hardpin / Quarantine */
    test_query_get_lock();
    test_query_user_variable();
    test_query_create_temp_table();
    test_query_lock_tables();

    /* 16) SSV: SET statement key/value extraction */
    test_set_session_variable();
    test_set_global_variable();
    test_set_bare_variable();
    test_set_names();
    test_set_quoted_value();
    test_set_at_at_variable();

    /* 17) SSV: SESSION_TRACK parsing in OK packets */
    test_be_ok_session_track_sysvar();
    test_be_ok_session_track_charset();
    test_be_ok_no_session_track();

    /* 18) Prepared-statement tracking */
    test_stmt_prepare_registers_stmt();
    test_stmt_close_clears_pin_when_last();
    test_stmt_close_preserves_pin_with_multiple();
    test_stmt_reset_connection_clears_map();
    test_stmt_replay_empty();
    test_stmt_replay_active();

    /* 19) SESSION_TRACK_SCHEMA and SESSION_TRACK_GTIDS */
    test_be_ok_session_track_schema();
    test_be_ok_session_track_gtids();

    /* 20) notify_write_lsn */
    test_notify_write_lsn_stores_value();

    /* 21) RYW: SET / SELECT @keel_write_gtid */
    test_ryw_set_keel_write_gtid();
    test_ryw_select_keel_write_gtid();

    /* 22) classify: CALL, DO, XA */
    test_classify_call_unknown_state();
    test_classify_do_is_write();
    test_classify_xa_start();
    test_classify_xa_commit();

    printf("\n=== Summary ===\n");
    return test_summary();
}
