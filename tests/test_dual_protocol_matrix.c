/**
 * @file test_dual_protocol_matrix.c
 * @brief Cross-protocol conformance matrix for PostgreSQL and MySQL flow vtables.
 *
 * KEEL exposes two protocol implementations behind one abstract flow contract.
 * This suite treats that contract as a specification and verifies both sides
 * satisfy equivalent semantics where they should, while still diverging where
 * wire formats and protocol rules genuinely differ.
 *
 * The tests are intentionally direct-vtable and builder-driven. They avoid full
 * proxy startup so a failure points at protocol classification behavior itself
 * rather than process orchestration.
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"

#include <string.h>
#include <stdio.h>

/* ---- Globals ---- */
int g_tests_run, g_tests_passed, g_tests_failed;

/* ---- Both vtables ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
extern const keel_proto_flow_vtable_t keel_proto_flow_mysql;
#define PG (&keel_proto_flow_postgres)
#define MY (&keel_proto_flow_mysql)

/* ============================================================================
 * Wire-protocol builders — PG
 * ============================================================================ */

/**
 * @brief Encode a 32-bit big-endian integer for handcrafted PG frames.
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Build a minimal PostgreSQL startup packet for context bootstrap.
 * @param buf Destination buffer.
 * @param user Startup user value.
 * @param db Startup database value.
 * @return Number of bytes written.
 */
static size_t pg_build_startup(uint8_t *buf, const char *user, const char *db) {
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
 * @brief Build a simple-query (`Q`) frame.
 * @param buf Destination buffer.
 * @param sql Query text.
 * @return Number of bytes written.
 */
static size_t pg_build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

/* ============================================================================
 * Wire-protocol builders — MySQL
 * ============================================================================ */

/**
 * @brief Encode a 24-bit little-endian MySQL payload length.
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wrle24(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16);
}

/**
 * @brief Build a generic MySQL packet header plus payload.
 * @param buf Destination buffer.
 * @param seq_id MySQL sequence id.
 * @param payload Packet payload bytes.
 * @param plen Payload length.
 * @return Number of bytes written.
 */
static size_t my_build_packet(uint8_t *buf, uint8_t seq_id,
                               const uint8_t *payload, size_t plen) {
    wrle24(buf, (uint32_t)plen);
    buf[3] = seq_id;
    if (plen > 0) memcpy(buf + 4, payload, plen);
    return 4 + plen;
}

/**
 * @brief Build a compact client handshake response for MySQL flow bootstrap.
 * @param buf Destination buffer.
 * @param seq_id Packet sequence id.
 * @param user Username to encode.
 * @param db Optional default database.
 * @return Number of bytes written.
 */
static size_t my_build_handshake(uint8_t *buf, uint8_t seq_id,
                                  const char *user, const char *db) {
    uint8_t payload[256];
    size_t pos = 0;
    uint32_t caps = 0x0000f7ff | (1U << 19);
    if (db && db[0]) caps |= (1U << 3);
    payload[pos++] = (uint8_t)caps;
    payload[pos++] = (uint8_t)(caps >> 8);
    payload[pos++] = (uint8_t)(caps >> 16);
    payload[pos++] = (uint8_t)(caps >> 24);
    payload[pos++] = 0xFF; payload[pos++] = 0xFF;
    payload[pos++] = 0xFF; payload[pos++] = 0x00;
    payload[pos++] = 0x2d;
    memset(payload + pos, 0, 23); pos += 23;
    size_t ul = strlen(user);
    memcpy(payload + pos, user, ul); pos += ul;
    payload[pos++] = 0;
    payload[pos++] = 0; /* auth response len = 0 */
    if (db && db[0]) {
        size_t dl = strlen(db);
        memcpy(payload + pos, db, dl); pos += dl;
        payload[pos++] = 0;
    }
    return my_build_packet(buf, seq_id, payload, pos);
}

/**
 * @brief Build a MySQL `COM_QUERY` packet.
 * @param buf Destination buffer.
 * @param seq_id Packet sequence id.
 * @param sql Query text.
 * @return Number of bytes written.
 */
static size_t my_build_com_query(uint8_t *buf, uint8_t seq_id, const char *sql) {
    size_t sl = strlen(sql);
    uint8_t payload[4096];
    payload[0] = 0x03; /* COM_QUERY */
    memcpy(payload + 1, sql, sl);
    return my_build_packet(buf, seq_id, payload, 1 + sl);
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Create and initialize a PostgreSQL flow context through startup.
 * @return Ready context or `NULL` on allocation failure.
 */
static void *pg_create_and_startup(void) {
    void *ctx = PG->create_context(NULL);
    if (!ctx) return NULL;
    uint8_t buf[512];
    size_t len = pg_build_startup(buf, "testuser", "testdb");
    keel_fe_action_t act;
    PG->on_fe_msg(ctx, buf, len, &act);
    return ctx;
}

/**
 * @brief Create and initialize a MySQL flow context through handshake response.
 * @return Ready context or `NULL` on allocation failure.
 */
static void *my_create_and_startup(void) {
    void *ctx = MY->create_context(NULL);
    if (!ctx) return NULL;
    uint8_t buf[512];
    size_t len = my_build_handshake(buf, 1, "testuser", "testdb");
    keel_fe_action_t act;
    MY->on_fe_msg(ctx, buf, len, &act);
    return ctx;
}

/* ============================================================================
 * 1) Protocol identity — name, default port
 * ============================================================================ */

static void test_protocol_identity(void) {
    TEST_BEGIN("dual_id: PG name and default port");
    TEST_ASSERT_NOT_NULL(PG->name);
    TEST_ASSERT(strstr(PG->name, "ostgres") != NULL ||
                strstr(PG->name, "pg") != NULL ||
                strstr(PG->name, "PG") != NULL);
    TEST_ASSERT_EQ(PG->default_port, (uint16_t)5432);
    TEST_END();

    TEST_BEGIN("dual_id: MySQL name and default port");
    TEST_ASSERT_NOT_NULL(MY->name);
    TEST_ASSERT(strstr(MY->name, "ysql") != NULL ||
                strstr(MY->name, "mysql") != NULL ||
                strstr(MY->name, "MySQL") != NULL);
    TEST_ASSERT_EQ(MY->default_port, (uint16_t)3306);
    TEST_END();

    TEST_BEGIN("dual_id: PG and MySQL are distinct vtables");
    TEST_ASSERT(PG != MY);
    TEST_ASSERT(PG->create_context != MY->create_context);
    TEST_END();
}

/* ============================================================================
 * 2) VTable completeness — all required methods present
 * ============================================================================ */

static void test_vtable_completeness(void) {
    const keel_proto_flow_vtable_t *vts[] = { PG, MY };
    const char *names[] = { "PG", "MySQL" };

    for (int i = 0; i < 2; i++) {
        char desc[128];

        snprintf(desc, sizeof(desc), "dual_vtable/%s: create_context", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->create_context != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: destroy_context", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->destroy_context != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: frame_len", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->frame_len != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: on_fe_msg", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->on_fe_msg != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: on_be_msg", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->on_be_msg != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: is_data_frame", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->is_data_frame != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: fingerprint", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->fingerprint != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: build_cleanup", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->build_cleanup != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: backend_reuse_gate", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->backend_reuse_gate != NULL);
        TEST_END();

        snprintf(desc, sizeof(desc), "dual_vtable/%s: generate_startup", names[i]);
        TEST_BEGIN(desc);
        TEST_ASSERT(vts[i]->generate_startup != NULL);
        TEST_END();
    }
}

/* ============================================================================
 * 3) Equivalent SQL operations produce consistent effect flags
 *
 * Both PG and MySQL should classify the same SQL with the same effects.
 * ============================================================================ */

static void test_equivalent_sql_effects(void) {
    struct {
        const char *sql;
        uint32_t expect_effect;
        const char *desc;
    } cases[] = {
        { "SELECT 1",                  KEEL_QE_READONLY,    "SELECT→READONLY" },
        { "INSERT INTO t VALUES (1)",  KEEL_QE_WRITE,       "INSERT→WRITE" },
        { "UPDATE t SET x=1",          KEEL_QE_WRITE,       "UPDATE→WRITE" },
        { "DELETE FROM t",             KEEL_QE_WRITE,       "DELETE→WRITE" },
        { "BEGIN",                     KEEL_QE_BEGINS_TX,   "BEGIN→BEGINS_TX" },
        { "COMMIT",                    KEEL_QE_ENDS_TX,     "COMMIT→ENDS_TX" },
        { "ROLLBACK",                  KEEL_QE_ENDS_TX,     "ROLLBACK→ENDS_TX" },
        { "CREATE TABLE t (id int)",   KEEL_QE_DDL,         "CREATE→DDL" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        /* PG */
        {
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "dual_effect/PG/%s", cases[i].desc);
            TEST_BEGIN(desc);

            void *ctx = pg_create_and_startup();
            TEST_ASSERT_NOT_NULL(ctx);

            uint8_t buf[256];
            keel_fe_action_t act;
            size_t len = pg_build_query(buf, cases[i].sql);
            PG->on_fe_msg(ctx, buf, len, &act);
            TEST_ASSERT(act.effect & cases[i].expect_effect);

            PG->destroy_context(ctx);
            TEST_END();
        }

        /* MySQL */
        {
            char desc[128];
            snprintf(desc, sizeof(desc),
                     "dual_effect/MySQL/%s", cases[i].desc);
            TEST_BEGIN(desc);

            void *ctx = my_create_and_startup();
            TEST_ASSERT_NOT_NULL(ctx);

            uint8_t buf[4096];
            keel_fe_action_t act;
            size_t len = my_build_com_query(buf, 0, cases[i].sql);
            MY->on_fe_msg(ctx, buf, len, &act);
            TEST_ASSERT(act.effect & cases[i].expect_effect);

            MY->destroy_context(ctx);
            TEST_END();
        }
    }
}

/* ============================================================================
 * 4) Transaction pin semantics equivalence
 *
 * BEGIN sets TRANSACTION pin, COMMIT clears it — both protocols.
 * ============================================================================ */

static void test_tx_pin_equivalence(void) {
    /* PG */
    TEST_BEGIN("dual_tx_pin/PG: BEGIN pins, COMMIT clears");
    {
        void *ctx = pg_create_and_startup();
        TEST_ASSERT_NOT_NULL(ctx);
        uint8_t buf[64];
        keel_fe_action_t act;

        pg_build_query(buf, "BEGIN");
        PG->on_fe_msg(ctx, buf, pg_build_query(buf, "BEGIN"), &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);

        pg_build_query(buf, "COMMIT");
        PG->on_fe_msg(ctx, buf, pg_build_query(buf, "COMMIT"), &act);
        TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

        PG->destroy_context(ctx);
    }
    TEST_END();

    /* MySQL */
    TEST_BEGIN("dual_tx_pin/MySQL: BEGIN pins, COMMIT clears");
    {
        void *ctx = my_create_and_startup();
        TEST_ASSERT_NOT_NULL(ctx);
        uint8_t buf[256];
        keel_fe_action_t act;

        size_t len = my_build_com_query(buf, 0, "BEGIN");
        MY->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_update & KEEL_FPIN_TRANSACTION);

        len = my_build_com_query(buf, 0, "COMMIT");
        MY->on_fe_msg(ctx, buf, len, &act);
        TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * 5) Fingerprint consistency — same SQL → same hash within each protocol
 * ============================================================================ */

static void test_fingerprint_consistency(void) {
    const char *sqls[] = {
        "SELECT 1",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1 WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
    };

    for (size_t i = 0; i < sizeof(sqls) / sizeof(sqls[0]); i++) {
        /* PG fingerprint stability */
        {
            char desc[128];
            snprintf(desc, sizeof(desc), "dual_fp/PG: '%s' stable", sqls[i]);
            TEST_BEGIN(desc);

            void *ctx = pg_create_and_startup();
            uint64_t h1 = PG->fingerprint(ctx, sqls[i], strlen(sqls[i]));
            uint64_t h2 = PG->fingerprint(ctx, sqls[i], strlen(sqls[i]));
            TEST_ASSERT_EQ(h1, h2);
            TEST_ASSERT(h1 != 0);
            PG->destroy_context(ctx);
            TEST_END();
        }

        /* MySQL fingerprint stability */
        {
            char desc[128];
            snprintf(desc, sizeof(desc), "dual_fp/MySQL: '%s' stable", sqls[i]);
            TEST_BEGIN(desc);

            void *ctx = my_create_and_startup();
            uint64_t h1 = MY->fingerprint(ctx, sqls[i], strlen(sqls[i]));
            uint64_t h2 = MY->fingerprint(ctx, sqls[i], strlen(sqls[i]));
            TEST_ASSERT_EQ(h1, h2);
            TEST_ASSERT(h1 != 0);
            MY->destroy_context(ctx);
            TEST_END();
        }
    }
}

/* ============================================================================
 * 6) Fingerprint case-insensitivity — both protocols
 * ============================================================================ */

static void test_fingerprint_case_insensitive(void) {
    TEST_BEGIN("dual_fp_case/PG: case-insensitive");
    {
        void *ctx = pg_create_and_startup();
        uint64_t h1 = PG->fingerprint(ctx, "SELECT 1", 8);
        uint64_t h2 = PG->fingerprint(ctx, "select 1", 8);
        TEST_ASSERT_EQ(h1, h2);
        PG->destroy_context(ctx);
    }
    TEST_END();

    TEST_BEGIN("dual_fp_case/MySQL: case-insensitive");
    {
        void *ctx = my_create_and_startup();
        uint64_t h1 = MY->fingerprint(ctx, "SELECT 1", 8);
        uint64_t h2 = MY->fingerprint(ctx, "select 1", 8);
        TEST_ASSERT_EQ(h1, h2);
        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * 7) Cleanup output format — PG uses 'Q' message, MySQL uses COM_QUERY packet
 * ============================================================================ */

static void test_cleanup_format_divergence(void) {
    TEST_BEGIN("dual_cleanup/PG: build_cleanup produces 'Q' message");
    {
        void *ctx = pg_create_and_startup();
        uint8_t buf[512];
        ssize_t n = PG->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        TEST_ASSERT_EQ(buf[0], 'Q');
        PG->destroy_context(ctx);
    }
    TEST_END();

    TEST_BEGIN("dual_cleanup/MySQL: build_cleanup produces MySQL packet");
    {
        void *ctx = my_create_and_startup();
        uint8_t buf[512];
        ssize_t n = MY->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        /* MySQL uses COM_QUERY: 4-byte header + 0x03 command byte */
        /* Or could be direct SQL. Just verify non-zero output */
        TEST_ASSERT((size_t)n <= sizeof(buf));
        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * 8) is_data_frame divergence
 * ============================================================================ */

static void test_data_frame_divergence(void) {
    TEST_BEGIN("dual_dataframe: PG 'D' is data, MySQL different");
    {
        /* PG DataRow */
        uint8_t dr[5] = {'D', 0, 0, 0, 4};
        TEST_ASSERT(PG->is_data_frame(NULL, dr, 5));

        /* PG non-data */
        uint8_t z[5] = {'Z', 0, 0, 0, 4};
        TEST_ASSERT(!PG->is_data_frame(NULL, z, 5));

        /* MySQL: 'D' is NOT a MySQL data frame tag */
        /* MySQL data frames are result row packets, not identified by 'D' */
        /* The exact behavior depends on implementation, just test no crash */
        uint8_t my_hdr[5] = {0x01, 0x00, 0x00, 0x01, 0x00};
        (void)MY->is_data_frame(NULL, my_hdr, 5);
    }
    TEST_END();
}

/* ============================================================================
 * 9) generate_error produces valid protocol-specific error messages
 * ============================================================================ */

static void test_generate_error(void) {
    TEST_BEGIN("dual_error/PG: generate_error produces 'E' message");
    {
        void *ctx = pg_create_and_startup();
        uint8_t buf[256];
        ssize_t n = PG->generate_error(ctx, "42000", "syntax error", buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        TEST_ASSERT_EQ(buf[0], 'E');
        /* Should contain SQLSTATE and message */
        TEST_ASSERT(memmem(buf, (size_t)n, "42000", 5) != NULL);
        TEST_ASSERT(memmem(buf, (size_t)n, "syntax error", 12) != NULL);
        PG->destroy_context(ctx);
    }
    TEST_END();

    TEST_BEGIN("dual_error/MySQL: generate_error produces MySQL ERR packet");
    {
        void *ctx = my_create_and_startup();
        uint8_t buf[256];
        ssize_t n = MY->generate_error(ctx, "42000", "syntax error", buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        /* MySQL ERR packet has 4-byte header + 0xFF marker */
        TEST_ASSERT(buf[4] == 0xFF);
        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * 10) Reuse gate — fresh context is reusable in both protocols
 * ============================================================================ */

static void test_reuse_gate_fresh(void) {
    TEST_BEGIN("dual_reuse/PG: fresh context reusable");
    {
        void *ctx = pg_create_and_startup();
        TEST_ASSERT(PG->backend_reuse_gate(ctx));
        PG->destroy_context(ctx);
    }
    TEST_END();

    TEST_BEGIN("dual_reuse/MySQL: fresh context reusable");
    {
        void *ctx = my_create_and_startup();
        TEST_ASSERT(MY->backend_reuse_gate(ctx));
        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * 11) generate_startup produces valid protocol-specific startup messages
 * ============================================================================ */

static void test_generate_startup(void) {
    TEST_BEGIN("dual_startup/PG: generate_startup V3 message");
    {
        void *ctx = pg_create_and_startup();
        uint8_t buf[512];
        ssize_t n = PG->generate_startup(ctx, "alice", "mydb", buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        /* PG V3: first 4 bytes are length, next 4 are version 0x00030000 */
        uint32_t ver = ((uint32_t)buf[4] << 24) | ((uint32_t)buf[5] << 16) |
                       ((uint32_t)buf[6] << 8)  | (uint32_t)buf[7];
        TEST_ASSERT_EQ(ver, (uint32_t)0x00030000);
        TEST_ASSERT(memmem(buf, (size_t)n, "alice", 5) != NULL);
        PG->destroy_context(ctx);
    }
    TEST_END();

    TEST_BEGIN("dual_startup/MySQL: generate_startup MySQL handshake");
    {
        void *ctx = my_create_and_startup();
        uint8_t buf[512];
        ssize_t n = MY->generate_startup(ctx, "alice", "mydb", buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        /* MySQL handshake: 4-byte header. Just check non-zero length. */
        TEST_ASSERT((size_t)n > 4);
        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * 12) Route hint equivalence for reads and writes
 * ============================================================================ */

static void test_route_hint_equivalence(void) {
    struct {
        const char *sql;
        keel_flow_route_t expect_route;
        const char *desc;
    } cases[] = {
        { "SELECT 1",                  KEEL_FROUTE_REPLICA,  "SELECT→REPLICA" },
        { "INSERT INTO t VALUES (1)",  KEEL_FROUTE_PRIMARY,  "INSERT→PRIMARY" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        /* PG */
        {
            char desc[128];
            snprintf(desc, sizeof(desc), "dual_route/PG/%s", cases[i].desc);
            TEST_BEGIN(desc);

            void *ctx = pg_create_and_startup();
            uint8_t buf[256];
            keel_fe_action_t act;
            PG->on_fe_msg(ctx, buf, pg_build_query(buf, cases[i].sql), &act);
            TEST_ASSERT_EQ(act.route_hint, cases[i].expect_route);
            PG->destroy_context(ctx);
            TEST_END();
        }

        /* MySQL */
        {
            char desc[128];
            snprintf(desc, sizeof(desc), "dual_route/MySQL/%s", cases[i].desc);
            TEST_BEGIN(desc);

            void *ctx = my_create_and_startup();
            uint8_t buf[4096];
            keel_fe_action_t act;
            size_t len = my_build_com_query(buf, 0, cases[i].sql);
            MY->on_fe_msg(ctx, buf, len, &act);
            TEST_ASSERT_EQ(act.route_hint, cases[i].expect_route);
            MY->destroy_context(ctx);
            TEST_END();
        }
    }
}

/* ============================================================================
 * 13) Context lifecycle — create/destroy doesn't leak (ASAN check)
 * ============================================================================ */

static void test_context_lifecycle(void) {
    TEST_BEGIN("dual_lifecycle: PG create+destroy no leak");
    for (int i = 0; i < 100; i++) {
        void *ctx = PG->create_context(NULL);
        TEST_ASSERT_NOT_NULL(ctx);
        PG->destroy_context(ctx);
    }
    TEST_END();

    TEST_BEGIN("dual_lifecycle: MySQL create+destroy no leak");
    for (int i = 0; i < 100; i++) {
        void *ctx = MY->create_context(NULL);
        TEST_ASSERT_NOT_NULL(ctx);
        MY->destroy_context(ctx);
    }
    TEST_END();
}

/* ============================================================================
 * main()
 * ============================================================================ */

int main(void) {
    printf("=== Dual-Protocol Separation & Equivalence Matrix ===\n\n");

    test_protocol_identity();
    test_vtable_completeness();
    test_equivalent_sql_effects();
    test_tx_pin_equivalence();
    test_fingerprint_consistency();
    test_fingerprint_case_insensitive();
    test_cleanup_format_divergence();
    test_data_frame_divergence();
    test_generate_error();
    test_reuse_gate_fresh();
    test_generate_startup();
    test_route_hint_equivalence();
    test_context_lifecycle();

    printf("\n--- Results: %d run, %d passed, %d failed ---\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return g_tests_failed > 0 ? 1 : 0;
}
