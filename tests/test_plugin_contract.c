/**
 * @file test_plugin_contract.c
 * @brief Tests for the Phase 5 core - plugin API contract.
 *
 * Validates:
 *   - Plugin get_info / capabilities reporting
 *   - Error classification (PG ErrorResponse, MySQL ERR packet)
 *   - Cleanup slot (selective vs full)
 *   - Plugin helper macros (KEEL_PLUGIN_HAS, CALL_OR)
 *   - Vtable NULL safety (optional callbacks absent)
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/plugin/plugin.h"
#include "keel/plugin/plugin_types.h"
#include "keel/session/state_profile.h"
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

/**
 * Write helper for tests.
 * Assigning write()'s return value to a local var satisfies the
 * warn_unused_result attribute without turning real errors into noise.
 */
static inline void test_write(int fd, const void* buf, size_t len) {
    ssize_t n = write(fd, buf, len);
    (void)n;
}

/* ---- Access the built-in vtables ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
extern const keel_proto_flow_vtable_t keel_proto_flow_mysql;

/* ============================================================================
 * Test: PG get_info reports expected capabilities
 * ============================================================================ */
static int test_pg_get_info(void) {
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_postgres, get_info));

    keel_plugin_info_t info;
    keel_proto_flow_postgres.get_info(&info);

    TEST_ASSERT_STR_EQ(info.name, "postgres");
    TEST_ASSERT_EQ(info.default_port, 5432);
    TEST_ASSERT_EQ(info.api_version, KEEL_PLUGIN_API_V1);

    TEST_ASSERT(info.capabilities & KEEL_PCAP_TEXT_PROTOCOL);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_EXTENDED_QUERY);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_CONSISTENCY_TOKEN);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_STREAMING_COPY);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_STATE_PROFILE);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_SELECTIVE_RESET);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_DISCARD_ALL);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_PROBE_HEALTH);

    return 0;
}

/* ============================================================================
 * Test: MySQL get_info reports expected capabilities
 * ============================================================================ */
static int test_mysql_get_info(void) {
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_mysql, get_info));

    keel_plugin_info_t info;
    keel_proto_flow_mysql.get_info(&info);

    TEST_ASSERT_STR_EQ(info.name, "mysql");
    TEST_ASSERT_EQ(info.default_port, 3306);
    TEST_ASSERT_EQ(info.api_version, KEEL_PLUGIN_API_V1);

    TEST_ASSERT(info.capabilities & KEEL_PCAP_BINARY_PROTOCOL);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_GTID);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_AUTH_NATIVE);

    /* MySQL should have GTID-based consistency token caps */
    TEST_ASSERT(info.capabilities & KEEL_PCAP_CONSISTENCY_TOKEN);
    TEST_ASSERT(info.capabilities & KEEL_PCAP_POSITION_WAIT);

    /* MySQL should NOT have PG-specific caps */
    TEST_ASSERT(!(info.capabilities & KEEL_PCAP_STREAMING_COPY));

    return 0;
}

/* ============================================================================
 * Test: Plugin capability helpers
 * ============================================================================ */
static int test_capability_helpers(void) {
    keel_plugin_caps_t caps = keel_plugin_capabilities(&keel_proto_flow_postgres);
    TEST_ASSERT(caps != 0);
    TEST_ASSERT(keel_plugin_has_cap(&keel_proto_flow_postgres, KEEL_PCAP_PROBE_HEALTH));

    /* NULL vtable returns 0 capabilities */
    TEST_ASSERT_EQ(keel_plugin_capabilities(NULL), (keel_plugin_caps_t)0);
    TEST_ASSERT(!keel_plugin_has_cap(NULL, KEEL_PCAP_TEXT_PROTOCOL));

    return 0;
}

/* ============================================================================
 * Helper: build a PG ErrorResponse message
 * ============================================================================ */
/**
 * @brief Build a well-formed PG ErrorResponse message.
 *
 * Populates Severity (S+V), SQLSTATE (C), and Message (M) fields.
 * The caller controls the severity / sqlstate / message so each
 * test can exercise a different error-classification branch.
 *
 * @param buf       Destination buffer (caller must ensure >= 256 bytes).
 * @param buf_len   Buffer capacity (unused; kept for API symmetry).
 * @param severity  Severity string ("ERROR", "FATAL", "PANIC").
 * @param sqlstate  5-char SQLSTATE code.
 * @param message   Human-readable error message.
 * @return Total message length in bytes.
 */
static size_t build_pg_error(uint8_t* buf, size_t buf_len,
                              const char* severity,
                              const char* sqlstate,
                              const char* message) {
    (void)buf_len;
    uint8_t* p = buf;
    *p++ = 'E';
    uint8_t* len_pos = p; p += 4; /* placeholder for length */

    *p++ = 'S';
    size_t sl = strlen(severity) + 1;
    memcpy(p, severity, sl); p += sl;

    *p++ = 'V';
    memcpy(p, severity, sl); p += sl;

    *p++ = 'C';
    size_t cl = strlen(sqlstate) + 1;
    memcpy(p, sqlstate, cl); p += cl;

    *p++ = 'M';
    size_t ml = strlen(message) + 1;
    memcpy(p, message, ml); p += ml;

    *p++ = '\0';

    uint32_t total_len = (uint32_t)(p - buf - 1);
    len_pos[0] = (uint8_t)(total_len >> 24);
    len_pos[1] = (uint8_t)(total_len >> 16);
    len_pos[2] = (uint8_t)(total_len >> 8);
    len_pos[3] = (uint8_t)(total_len);

    return (size_t)(p - buf);
}

/* ============================================================================
 * Test: PG classify_error - SQL error (normal, connection OK)
 * ============================================================================ */
static int test_pg_classify_sql_error(void) {
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_postgres, classify_error));

    void* ctx = keel_proto_flow_postgres.create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    size_t len = build_pg_error(buf, sizeof(buf), "ERROR", "42P01",
                                 "relation does not exist");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_postgres.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_SQL_ERROR);
    TEST_ASSERT(einfo.connection_ok == true);
    TEST_ASSERT(einfo.sqlstate != NULL);
    TEST_ASSERT(strncmp(einfo.sqlstate, "42P01", 5) == 0);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG classify_error - connection exception (08xxx -> FATAL)
 * ============================================================================ */
static int test_pg_classify_connection_error(void) {
    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    uint8_t buf[256];
    size_t len = build_pg_error(buf, sizeof(buf), "FATAL", "08006",
                                 "connection_failure");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_postgres.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_BACKEND_FATAL);
    TEST_ASSERT(einfo.connection_ok == false);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG classify_error - resource limit (53xxx)
 * ============================================================================ */
static int test_pg_classify_resource_error(void) {
    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    uint8_t buf[256];
    size_t len = build_pg_error(buf, sizeof(buf), "ERROR", "53300",
                                 "too many connections for role");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_postgres.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_RESOURCE_LIMIT);
    TEST_ASSERT(einfo.connection_ok == true);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG classify_error - serialization failure (40001)
 * ============================================================================ */
static int test_pg_classify_serialization_failure(void) {
    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    uint8_t buf[256];
    size_t len = build_pg_error(buf, sizeof(buf), "ERROR", "40001",
                                 "could not serialize access");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_postgres.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_IDEMPOTENT_SAFE);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Helper: build a MySQL ERR packet
 * ============================================================================ */
/**
 * @brief Build a MySQL ERR_Packet with the standard 4-byte header.
 *
 * Layout: 3-byte payload length + sequence_id(1) + 0xFF marker +
 * error_number(2) + '#' + sqlstate(5) + message.
 *
 * @param buf       Destination buffer.
 * @param errn      MySQL error number (e.g. 1213 = deadlock).
 * @param sqlstate  5-char SQLSTATE.
 * @param msg       Human-readable error message.
 * @return Total packet length in bytes.
 */
static size_t build_mysql_err(uint8_t* buf, uint16_t errn,
                               const char* sqlstate, const char* msg) {
    size_t ml = strlen(msg);
    size_t pl = 1 + 2 + 1 + 5 + ml;
    buf[0] = (uint8_t)(pl);
    buf[1] = (uint8_t)(pl >> 8);
    buf[2] = (uint8_t)(pl >> 16);
    buf[3] = 1;
    size_t p = 4;
    buf[p++] = 0xFF;
    buf[p++] = (uint8_t)(errn);
    buf[p++] = (uint8_t)(errn >> 8);
    buf[p++] = '#';
    memcpy(buf + p, sqlstate, 5); p += 5;
    memcpy(buf + p, msg, ml); p += ml;
    return p;
}

/* ============================================================================
 * Test: MySQL classify_error - deadlock (1213 -> TRANSIENT)
 * ============================================================================ */
static int test_mysql_classify_deadlock(void) {
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_mysql, classify_error));

    void* ctx = keel_proto_flow_mysql.create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[128];
    size_t len = build_mysql_err(buf, 1213, "40001", "Deadlock found");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_mysql.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_TRANSIENT);
    TEST_ASSERT_EQ(einfo.error_code, (uint32_t)1213);
    TEST_ASSERT(einfo.connection_ok == true);

    keel_proto_flow_mysql.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: MySQL classify_error - too many connections (1040)
 * ============================================================================ */
static int test_mysql_classify_resource(void) {
    void* ctx = keel_proto_flow_mysql.create_context(NULL);

    uint8_t buf[128];
    size_t len = build_mysql_err(buf, 1040, "08004", "Too many connections");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_mysql.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_RESOURCE_LIMIT);

    keel_proto_flow_mysql.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: MySQL classify_error - shutdown (1053 -> BACKEND_FATAL)
 * ============================================================================ */
static int test_mysql_classify_shutdown(void) {
    void* ctx = keel_proto_flow_mysql.create_context(NULL);

    uint8_t buf[128];
    size_t len = build_mysql_err(buf, 1053, "08S01", "Server shutdown");

    keel_error_info_t einfo;
    int rc = keel_proto_flow_mysql.classify_error(ctx, buf, len, &einfo);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(einfo.error_class, KEEL_ERR_BACKEND_FATAL);
    TEST_ASSERT(einfo.connection_ok == false);

    keel_proto_flow_mysql.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG cleanup_slot - SELECTIVE mode generates RESET commands
 * ============================================================================ */
static int test_pg_cleanup_selective(void) {
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_postgres, cleanup_slot));

    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    state_profile_t profile;
    memset(&profile, 0, sizeof(profile));
    strncpy(profile.sorted_params[0].key, "search_path",
            sizeof(profile.sorted_params[0].key) - 1);
    strncpy(profile.sorted_params[0].value, "public,extra",
            sizeof(profile.sorted_params[0].value) - 1);
    strncpy(profile.sorted_params[1].key, "work_mem",
            sizeof(profile.sorted_params[1].key) - 1);
    strncpy(profile.sorted_params[1].value, "128MB",
            sizeof(profile.sorted_params[1].value) - 1);
    profile.count = 2;

    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_SELECTIVE, .timeout_ms = 0 };
    uint8_t buf[512];
    ssize_t n = keel_proto_flow_postgres.cleanup_slot(ctx, -1, &profile, opts,
                                                      buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(buf[0] == 'Q');

    const char* sql = (const char*)(buf + 5);
    TEST_ASSERT(strstr(sql, "RESET search_path") != NULL);
    TEST_ASSERT(strstr(sql, "RESET work_mem") != NULL);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG cleanup_slot - FULL mode generates DISCARD ALL
 * ============================================================================ */
static int test_pg_cleanup_full(void) {
    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_FULL, .timeout_ms = 0 };
    uint8_t buf[256];
    ssize_t n = keel_proto_flow_postgres.cleanup_slot(ctx, -1, NULL, opts,
                                                      buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(buf[0] == 'Q');

    const char* sql = (const char*)(buf + 5);
    TEST_ASSERT(strstr(sql, "DISCARD ALL") != NULL);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG cleanup_slot - DESTROY returns 0
 * ============================================================================ */
static int test_pg_cleanup_destroy(void) {
    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_DESTROY, .timeout_ms = 0 };
    uint8_t buf[256];
    ssize_t n = keel_proto_flow_postgres.cleanup_slot(ctx, -1, NULL, opts,
                                                      buf, sizeof(buf));
    TEST_ASSERT_EQ(n, (ssize_t)0);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: KEEL_PLUGIN_HAS / CALL_OR with NULL callbacks
 * ============================================================================ */
static int test_plugin_null_safety(void) {
    /* MySQL now implements cleanup_slot and probe_backend */
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_mysql, cleanup_slot));
    TEST_ASSERT(KEEL_PLUGIN_HAS(&keel_proto_flow_mysql, probe_backend));

    /* NULL vtable safety */
    const keel_proto_flow_vtable_t* null_vt = NULL;
    TEST_ASSERT(!KEEL_PLUGIN_HAS(null_vt, classify_error));

    /* MySQL cleanup_slot FULL should return COM_RESET_CONNECTION (5 bytes) */
    void* ctx = keel_proto_flow_mysql.create_context(NULL);
    keel_cleanup_opts_t opts = { .mode = KEEL_CLEANUP_FULL, .timeout_ms = 0 };
    uint8_t buf[64];
    ssize_t n = keel_proto_flow_mysql.cleanup_slot(ctx, -1,
                                                    NULL, opts, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, (ssize_t)5);

    /* keel_plugin_cleanup_slot also works (uses implementation, not fallback) */
    n = keel_plugin_cleanup_slot(&keel_proto_flow_mysql, ctx, -1,
                                 NULL, opts, buf, sizeof(buf));
    TEST_ASSERT_EQ(n, (ssize_t)5);

    keel_proto_flow_mysql.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG classify_error rejects non-error messages
 * ============================================================================ */
static int test_pg_classify_rejects_non_error(void) {
    void* ctx = keel_proto_flow_postgres.create_context(NULL);

    /* Send a ReadyForQuery message ('Z') - not an error */
    uint8_t z_msg[] = { 'Z', 0, 0, 0, 5, 'I' };
    keel_error_info_t einfo;
    int rc = keel_proto_flow_postgres.classify_error(ctx, z_msg, sizeof(z_msg), &einfo);
    TEST_ASSERT_EQ(rc, -1);

    /* NULL data */
    rc = keel_proto_flow_postgres.classify_error(ctx, NULL, 0, &einfo);
    TEST_ASSERT_EQ(rc, -1);

    keel_proto_flow_postgres.destroy_context(ctx);
    return 0;
}

/* ============================================================================
 * Test: PG capture_consistency_token — reads LSN from fake DataRow
 * ============================================================================ */
static int test_pg_capture_consistency_token(void) {
    TEST_BEGIN("pg: capture_consistency_token parses DataRow LSN");
    TEST_ASSERT(keel_proto_flow_postgres.capture_consistency_token != NULL);

    int sv[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    /* Build minimal PG response: only the DataRow matters for the scanner.
     * DataRow ('D'):
     *   1 byte type = 'D'
     *   4 bytes int32_be message_length = 4+2+4+9 = 19
     *   2 bytes int16_be ncols = 1
     *   4 bytes int32_be col_len = 9
     *   9 bytes data = "0/16B3740"
     */
    static const uint8_t fake_response[] = {
        /* RowDescription stub (ignored by scanner — just needs to precede DataRow) */
        'T', 0,0,0,6, 0,0,
        /* DataRow */
        'D', 0,0,0,19,  0,1,  0,0,0,9,  '0','/','1','6','B','3','7','4','0',
        /* CommandComplete: "SELECT 1\0" = 9 bytes body → msglen = 4+9 = 13 */
        'C', 0,0,0,13, 'S','E','L','E','C','T',' ','1','\0',
        /* ReadyForQuery */
        'Z', 0,0,0,5, 'I'
    };
    test_write(sv[1], fake_response, sizeof(fake_response));

    keel_consistency_token_t tok;
    int rc = keel_proto_flow_postgres.capture_consistency_token(NULL, sv[0], &tok);
    close(sv[0]);
    close(sv[1]);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_STR_EQ(tok.value, "0/16B3740");
    TEST_ASSERT(tok.captured_at_ns != 0);
    return 0;
}

/* ============================================================================
 * Test: PG replica_reached_token — handles 't' and 'f' boolean DataRow
 * ============================================================================ */
static int test_pg_replica_reached_token(void) {
    TEST_BEGIN("pg: replica_reached_token — bool 't' means reached");
    TEST_ASSERT(keel_proto_flow_postgres.replica_reached_token != NULL);

    /* Sub-test A: boolean 't' → reached = true */
    {
        int sv[2];
        TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

        static const uint8_t response_true[] = {
            'T', 0,0,0,6, 0,0,
            /* DataRow: ncols=1, col_len=1, value='t' */
            'D', 0,0,0,11,  0,1,  0,0,0,1,  't',
            /* CommandComplete: "SELECT 1\0" = 9 bytes body → msglen = 4+9 = 13 */
            'C', 0,0,0,13, 'S','E','L','E','C','T',' ','1','\0',
            'Z', 0,0,0,5, 'I'
        };
        test_write(sv[1], response_true, sizeof(response_true));

        keel_consistency_token_t tok;
        memset(&tok, 0, sizeof(tok));
        strncpy(tok.value, "0/16B3740", sizeof(tok.value) - 1);

        bool reached = false;
        int rc = keel_proto_flow_postgres.replica_reached_token(
                     NULL, sv[0], &tok, 0, &reached);
        close(sv[0]);
        close(sv[1]);
        TEST_ASSERT_EQ(rc, 0);
        TEST_ASSERT(reached);
    }

    /* Sub-test B: empty token → trivially reached, no socket I/O needed */
    {
        int sv[2];
        TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        close(sv[1]);   /* nothing will be read */

        keel_consistency_token_t empty_tok;
        memset(&empty_tok, 0, sizeof(empty_tok));   /* value[0] = '\0' */

        bool reached = false;
        int rc = keel_proto_flow_postgres.replica_reached_token(
                     NULL, sv[0], &empty_tok, 0, &reached);
        close(sv[0]);
        TEST_ASSERT_EQ(rc, 0);
        TEST_ASSERT(reached);
    }
    return 0;
}

/* ============================================================================
 * Test: MySQL capture_consistency_token — reads GTID from fake result set
 * ============================================================================ */
static int test_mysql_capture_consistency_token(void) {
    TEST_BEGIN("mysql: capture_consistency_token parses GTID result");
    TEST_ASSERT(keel_proto_flow_mysql.capture_consistency_token != NULL);

    int sv[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    /* Build minimal MySQL text-resultset response for SELECT @@gtid_executed.
     * Layout (no CLIENT_DEPRECATE_EOF):
     *   pkt1 (seq=1): column count = 1    (payload: 0x01)
     *   pkt2 (seq=2): dummy column def    (payload: anything non-ERR/EOF)
     *   pkt3 (seq=3): EOF                 (0xFE + 4 status bytes)
     *   pkt4 (seq=4): row data            (LenEnc str "gtid-test-value")
     *   pkt5 (seq=5): EOF
     */
    static const char* gtid_val = "gtid-test-value"; /* 15 bytes */
    static const uint8_t pkt1[] = { 0x01,0x00,0x00, 0x01, 0x01 };
    static const uint8_t pkt2[] = { 0x01,0x00,0x00, 0x02, 0x00 };
    static const uint8_t pkt3[] = { 0x05,0x00,0x00, 0x03, 0xFE,0x00,0x00,0x00,0x00 };
    /* pkt4: header(3+1) + LenEnc(1) + data(15) = 20 bytes */
    uint8_t pkt4[20];
    pkt4[0]=0x10; pkt4[1]=0x00; pkt4[2]=0x00;  /* payload_len = 16 = 1+15 */
    pkt4[3]=0x04;                               /* seq_id = 4 */
    pkt4[4]=0x0F;                               /* LenInt = 15 */
    memcpy(pkt4+5, gtid_val, 15);
    static const uint8_t pkt5[] = { 0x05,0x00,0x00, 0x05, 0xFE,0x00,0x00,0x00,0x00 };

    test_write(sv[1], pkt1, sizeof(pkt1));
    test_write(sv[1], pkt2, sizeof(pkt2));
    test_write(sv[1], pkt3, sizeof(pkt3));
    test_write(sv[1], pkt4, sizeof(pkt4));
    test_write(sv[1], pkt5, sizeof(pkt5));

    keel_consistency_token_t tok;
    int rc = keel_proto_flow_mysql.capture_consistency_token(NULL, sv[0], &tok);
    close(sv[0]);
    close(sv[1]);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_STR_EQ(tok.value, "gtid-test-value");
    TEST_ASSERT(tok.captured_at_ns != 0);
    return 0;
}

/* ============================================================================
 * Test: MySQL replica_reached_token — "0" means reached, "1" means timeout
 * ============================================================================ */
static int test_mysql_replica_reached_token(void) {
    TEST_BEGIN("mysql: replica_reached_token — '0' means reached");
    TEST_ASSERT(keel_proto_flow_mysql.replica_reached_token != NULL);

    /* Sub-test A: "0" → reached */
    {
        int sv[2];
        TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

        /* Build fake row response where row value = "0" (WAIT returns 0=reached) */
        static const uint8_t rp1[] = { 0x01,0x00,0x00, 0x01, 0x01 };
        static const uint8_t rp2[] = { 0x01,0x00,0x00, 0x02, 0x00 };
        static const uint8_t rp3[] = { 0x05,0x00,0x00, 0x03, 0xFE,0x00,0x00,0x00,0x00 };
        /* Row: payload_len=2 (LenInt=1 + "0"), seq=4 */
        static const uint8_t rp4[] = { 0x02,0x00,0x00, 0x04, 0x01, '0' };
        static const uint8_t rp5[] = { 0x05,0x00,0x00, 0x05, 0xFE,0x00,0x00,0x00,0x00 };

        test_write(sv[1], rp1, sizeof(rp1));
        test_write(sv[1], rp2, sizeof(rp2));
        test_write(sv[1], rp3, sizeof(rp3));
        test_write(sv[1], rp4, sizeof(rp4));
        test_write(sv[1], rp5, sizeof(rp5));

        keel_consistency_token_t tok;
        memset(&tok, 0, sizeof(tok));
        /* Real-shape GTID set (UUID = hex+hyphens). Plain placeholders like
         * "uuid:1-5" are now rejected by the charset validator in
         * myf_replica_reached_token added with the GTID-injection fix. */
        strncpy(tok.value,
                "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
                sizeof(tok.value) - 1);

        bool reached = false;
        int rc = keel_proto_flow_mysql.replica_reached_token(
                     NULL, sv[0], &tok, 0, &reached);
        close(sv[0]);
        close(sv[1]);
        TEST_ASSERT_EQ(rc, 0);
        TEST_ASSERT(reached);
    }

    /* Sub-test B: empty token → trivially reached */
    {
        int sv[2];
        TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        close(sv[1]);

        keel_consistency_token_t empty_tok;
        memset(&empty_tok, 0, sizeof(empty_tok));

        bool reached = false;
        int rc = keel_proto_flow_mysql.replica_reached_token(
                     NULL, sv[0], &empty_tok, 0, &reached);
        close(sv[0]);
        TEST_ASSERT_EQ(rc, 0);
        TEST_ASSERT(reached);
    }
    return 0;
}

/* ============================================================================
 * Main
 * ============================================================================ */
int main(void) {
    printf("=== Plugin Contract Tests ===\n\n");

    test_pg_get_info();
    test_mysql_get_info();
    test_capability_helpers();
    test_pg_classify_sql_error();
    test_pg_classify_connection_error();
    test_pg_classify_resource_error();
    test_pg_classify_serialization_failure();
    test_mysql_classify_deadlock();
    test_mysql_classify_resource();
    test_mysql_classify_shutdown();
    test_pg_cleanup_selective();
    test_pg_cleanup_full();
    test_pg_cleanup_destroy();
    test_plugin_null_safety();
    test_pg_classify_rejects_non_error();
    test_pg_capture_consistency_token();
    test_pg_replica_reached_token();
    test_mysql_capture_consistency_token();
    test_mysql_replica_reached_token();

    printf("\n");
    return test_summary();
}
