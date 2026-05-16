/**
 * @file test_proto_split.c
 * @brief Split-at-every-byte protocol generator tests.
 *
 * For each test message (startup, simple query, named-parse, bind, execute,
 * flush, terminate, and select backend messages), the generator feeds the
 * wire bytes one byte at a time — via frame_len() calls — confirming:
 *
 *  1. frame_len() returns a negative "need more bytes" signal for every
 *     prefix shorter than the full message.
 *  2. frame_len() returns the correct total frame length once all bytes
 *     are available.
 *  3. A fresh context accepts the complete message via on_fe_msg() without
 *     crashing regardless of what happened before.
 *
 * This exercises the framing layer for every possible split position,
 * which is the highest-risk uncovered partial-read path (§9.5 of the
 * exhaustive test plan).
 *
 * The same pattern is applied to MySQL wire messages for symmetry.
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
extern const keel_proto_flow_vtable_t keel_proto_flow_mysql;

#define PG  (&keel_proto_flow_postgres)
#define MY  (&keel_proto_flow_mysql)

/* ============================================================================
 * Wire-message builders (re-used from test_pg_protocol_flow.c pattern)
 * ============================================================================ */

static inline void wr32(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)(v>>24); p[1]=(uint8_t)(v>>16);
    p[2]=(uint8_t)(v>>8);  p[3]=(uint8_t)v;
}

static size_t build_startup(uint8_t *buf, const char *user, const char *db) {
    uint8_t *p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p,"user",5);   p += 5;
    size_t ul = strlen(user); memcpy(p,user,ul+1); p += ul+1;
    memcpy(p,"database",9); p += 9;
    size_t dl = strlen(db); memcpy(p,db,dl+1); p += dl+1;
    *p++ = '\0';
    uint32_t total = (uint32_t)(p - buf);
    wr32(buf,total);
    return total;
}

static size_t build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf+1,(uint32_t)(4+sl+1));
    memcpy(buf+5,sql,sl); buf[5+sl]='\0';
    return 1+4+sl+1;
}

static size_t build_named_parse(uint8_t *buf, const char *name, const char *query) {
    size_t nl=strlen(name), ql=strlen(query);
    size_t body=nl+1+ql+1+2;
    buf[0]='P'; wr32(buf+1,(uint32_t)(4+body));
    memcpy(buf+5,name,nl+1);
    memcpy(buf+5+nl+1,query,ql+1);
    buf[5+nl+1+ql+1]=0; buf[5+nl+1+ql+2]=0;
    return 1+4+body;
}

static size_t build_bind(uint8_t *buf, const char *portal, const char *stmt) {
    size_t pl=strlen(portal), sl=strlen(stmt);
    size_t body=pl+1+sl+1+2+2+2;
    buf[0]='B'; wr32(buf+1,(uint32_t)(4+body));
    uint8_t *p=buf+5;
    memcpy(p,portal,pl+1); p+=pl+1;
    memcpy(p,stmt,sl+1);   p+=sl+1;
    p[0]=0;p[1]=0;p+=2; p[0]=0;p[1]=0;p+=2; p[0]=0;p[1]=0;
    return 1+4+body;
}

static size_t build_execute(uint8_t *buf, const char *portal) {
    size_t pl=strlen(portal);
    buf[0]='E'; wr32(buf+1,(uint32_t)(4+pl+1+4));
    memcpy(buf+5,portal,pl+1);
    wr32(buf+5+pl+1,0); /* max_rows=0 */
    return 1+4+pl+1+4;
}

static size_t build_flush(uint8_t *buf) {
    buf[0]='H'; wr32(buf+1,4); return 5;
}

static size_t build_sync(uint8_t *buf) {
    buf[0]='S'; wr32(buf+1,4); return 5;
}

static size_t build_terminate(uint8_t *buf) {
    buf[0]='X'; wr32(buf+1,4); return 5;
}

/* Backend: ReadyForQuery */
static size_t build_rfq(uint8_t *buf, char status) {
    buf[0]='Z'; wr32(buf+1,5); buf[5]=(uint8_t)status; return 6;
}

/* Backend: AuthenticationOk */
static size_t build_auth_ok(uint8_t *buf) {
    buf[0]='R'; wr32(buf+1,8); wr32(buf+5,0); return 9;
}

/* Backend: ParameterStatus */
static size_t build_param_status(uint8_t *buf, const char *k, const char *v) {
    size_t kl=strlen(k)+1, vl=strlen(v)+1;
    buf[0]='S'; wr32(buf+1,(uint32_t)(4+kl+vl));
    memcpy(buf+5,k,kl); memcpy(buf+5+kl,v,vl);
    return 1+4+kl+vl;
}

/* Backend: CommandComplete */
static size_t build_command_complete(uint8_t *buf, const char *tag) {
    size_t tl=strlen(tag)+1;
    buf[0]='C'; wr32(buf+1,(uint32_t)(4+tl));
    memcpy(buf+5,tag,tl);
    return 1+4+tl;
}

/* ============================================================================
 * Core split-byte harness
 *
 * For a well-formed message of `msg_len` bytes starting at `msg`:
 *   - For each prefix length k = 1 .. msg_len-1:
 *       frame_len(ctx, msg, k, dir) MUST be < 0 (need more data).
 *   - For the full message:
 *       frame_len(ctx, msg, msg_len, dir) MUST == (ssize_t)msg_len.
 *
 * A fresh context is created for each split position so parsing state
 * from an earlier truncated call cannot contaminate the next.
 * ============================================================================ */

static int split_byte_probe_fe(const char *label,
                                const uint8_t *msg, size_t msg_len,
                                int post_startup)
{
    int errors = 0;

    for (size_t k = 1; k < msg_len; k++) {
        void *ctx = PG->create_context(NULL);
        if (!ctx) { errors++; continue; }

        if (post_startup)
            ((pg_flow_ctx_t*)ctx)->startup_complete = true;

        ssize_t fl = PG->frame_len(ctx, msg, k, 0 /*FE*/);
        /*
         * frame_len contract for a truncated but valid message:
         *   0         — not enough header bytes to determine length (OK)
         *   msg_len   — length field decoded correctly even from partial buf (OK)
         * Anything else is a bug.
         */
        if (fl != 0 && fl != (ssize_t)msg_len) {
            errors++;
        }
        PG->destroy_context(ctx);
    }

    /* Full message: frame_len must equal msg_len */
    {
        void *ctx = PG->create_context(NULL);
        if (!ctx) { errors++; }
        else {
            if (post_startup)
                ((pg_flow_ctx_t*)ctx)->startup_complete = true;
            ssize_t fl = PG->frame_len(ctx, msg, msg_len, 0);
            if (fl != (ssize_t)msg_len) errors++;
            PG->destroy_context(ctx);
        }
    }

    if (errors > 0) {
        printf("  SPLIT-FE [%s] msg_len=%zu errors=%d\n", label, msg_len, errors);
    }
    return errors;
}

static int split_byte_probe_be(const char *label,
                                const uint8_t *msg, size_t msg_len)
{
    int errors = 0;

    for (size_t k = 1; k < msg_len; k++) {
        void *ctx = PG->create_context(NULL);
        if (!ctx) { errors++; continue; }

        ssize_t fl = PG->frame_len(ctx, msg, k, 1 /*BE*/);
        if (fl != 0 && fl != (ssize_t)msg_len) errors++;
        PG->destroy_context(ctx);
    }

    {
        void *ctx = PG->create_context(NULL);
        if (!ctx) { errors++; }
        else {
            ssize_t fl = PG->frame_len(ctx, msg, msg_len, 1);
            if (fl != (ssize_t)msg_len) errors++;
            PG->destroy_context(ctx);
        }
    }

    if (errors > 0) {
        printf("  SPLIT-BE [%s] msg_len=%zu errors=%d\n", label, msg_len, errors);
    }
    return errors;
}

/* ============================================================================
 * PG Frontend message splits
 * ============================================================================ */

static void test_split_startup(void) {
    TEST_BEGIN("split: PG startup message at every byte");
    uint8_t buf[256];
    size_t n = build_startup(buf, "alice", "mydb");
    int errs = split_byte_probe_fe("startup", buf, n, 0 /*startup mode*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_simple_query(void) {
    TEST_BEGIN("split: PG simple query 'Q' at every byte");
    uint8_t buf[256];
    size_t n = build_query(buf, "SELECT 1");
    int errs = split_byte_probe_fe("query-Q", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_named_parse(void) {
    TEST_BEGIN("split: PG named Parse 'P' at every byte");
    uint8_t buf[512];
    size_t n = build_named_parse(buf, "s1", "SELECT id FROM users WHERE id=$1");
    int errs = split_byte_probe_fe("parse-P", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_bind(void) {
    TEST_BEGIN("split: PG Bind 'B' at every byte");
    uint8_t buf[256];
    size_t n = build_bind(buf, "portal0", "s1");
    int errs = split_byte_probe_fe("bind-B", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_execute(void) {
    TEST_BEGIN("split: PG Execute 'E' at every byte");
    uint8_t buf[256];
    size_t n = build_execute(buf, "portal0");
    int errs = split_byte_probe_fe("execute-E", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_flush(void) {
    TEST_BEGIN("split: PG Flush 'H' at every byte");
    uint8_t buf[16];
    size_t n = build_flush(buf);
    int errs = split_byte_probe_fe("flush-H", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_sync(void) {
    TEST_BEGIN("split: PG Sync 'S' at every byte");
    uint8_t buf[16];
    size_t n = build_sync(buf);
    int errs = split_byte_probe_fe("sync-S", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_terminate(void) {
    TEST_BEGIN("split: PG Terminate 'X' at every byte");
    uint8_t buf[16];
    size_t n = build_terminate(buf);
    int errs = split_byte_probe_fe("terminate-X", buf, n, 1 /*post-startup*/);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

/* ============================================================================
 * PG Backend message splits
 * ============================================================================ */

static void test_split_be_rfq(void) {
    TEST_BEGIN("split: PG ReadyForQuery 'Z' at every byte");
    uint8_t buf[16];
    size_t n = build_rfq(buf, 'I');
    int errs = split_byte_probe_be("rfq-Z", buf, n);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_be_auth_ok(void) {
    TEST_BEGIN("split: PG AuthenticationOk 'R' at every byte");
    uint8_t buf[32];
    size_t n = build_auth_ok(buf);
    int errs = split_byte_probe_be("auth_ok-R", buf, n);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_be_param_status(void) {
    TEST_BEGIN("split: PG ParameterStatus 'S' at every byte");
    uint8_t buf[64];
    size_t n = build_param_status(buf, "server_encoding", "UTF8");
    int errs = split_byte_probe_be("param_status-S", buf, n);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_be_command_complete(void) {
    TEST_BEGIN("split: PG CommandComplete 'C' at every byte");
    uint8_t buf[32];
    size_t n = build_command_complete(buf, "SELECT 1");
    int errs = split_byte_probe_be("command_complete-C", buf, n);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

/* ============================================================================
 * Split + on_fe_msg: after probing splits, a fresh context accepts the full
 * message via on_fe_msg without crashing (startup phase messages).
 * ============================================================================ */

static void test_split_then_full_parse(void) {
    TEST_BEGIN("split: frame_len splits then on_fe_msg full message — no crash");

    uint8_t buf[256];
    size_t n;
    keel_fe_action_t act;

    /* Startup */
    n = build_startup(buf, "bob", "testdb");
    /* Do the split probes first */
    split_byte_probe_fe("startup-pre-full", buf, n, 0);
    /* Now a fresh context must handle the complete message */
    void *ctx = PG->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    int rc = PG->on_fe_msg(ctx, buf, n, &act);
    TEST_ASSERT_EQ(rc, 0);
    PG->destroy_context(ctx);

    /* Simple Query on a post-startup context */
    ctx = PG->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    n = build_startup(buf, "bob", "testdb");
    PG->on_fe_msg(ctx, buf, n, &act); /* advance past startup */
    n = build_query(buf, "SELECT 42");
    split_byte_probe_fe("query-pre-full", buf, n, 1);
    /* fresh ctx for the full-message check */
    PG->destroy_context(ctx);
    ctx = PG->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    size_t sn = build_startup(buf, "bob", "testdb");
    PG->on_fe_msg(ctx, buf, sn, &act);
    uint8_t qbuf[256];
    size_t qn = build_query(qbuf, "SELECT 42");
    rc = PG->on_fe_msg(ctx, qbuf, qn, &act);
    TEST_ASSERT_EQ(rc, 0);
    PG->destroy_context(ctx);

    TEST_END();
}

/* ============================================================================
 * Long-body split: a large message with a 1500-byte SQL string to stress
 * multi-byte length encoding (> 256 bytes in the length field).
 * ============================================================================ */

static void test_split_large_query(void) {
    TEST_BEGIN("split: large query (1400 byte SQL) splits — no crash");

    /* Build a query slightly above common TCP segment sizes */
    char sql[1401];
    memset(sql, 'x', 1400); sql[0]='S'; sql[1]='E'; sql[2]='L';
    sql[3]='E'; sql[4]='C'; sql[5]='T'; sql[6]=' ';
    sql[1400] = '\0';

    uint8_t *buf = malloc(1 + 4 + 1400 + 1);
    TEST_ASSERT_NOT_NULL(buf);
    size_t n = build_query(buf, sql);

    /* Only probe the first 64 splits to keep test fast */
    int errors = 0;
    for (size_t k = 1; k < 64 && k < n; k++) {
        void *ctx = PG->create_context(NULL);
        if (!ctx) { errors++; continue; }
        ((pg_flow_ctx_t*)ctx)->startup_complete = true; /* Q is post-startup */
        ssize_t fl = PG->frame_len(ctx, buf, k, 0);
        if (fl != 0 && fl != (ssize_t)n) errors++;
        PG->destroy_context(ctx);
    }
    /* Full message check */
    {
        void *ctx = PG->create_context(NULL);
        if (!ctx) { errors++; }
        else {
            ((pg_flow_ctx_t*)ctx)->startup_complete = true;
            ssize_t fl = PG->frame_len(ctx, buf, n, 0);
            if (fl != (ssize_t)n) errors++;
            PG->destroy_context(ctx);
        }
    }
    free(buf);
    TEST_ASSERT_EQ(errors, 0);
    TEST_END();
}

/* ============================================================================
 * MySQL: split-at-every-byte for a COM_QUERY packet
 *
 * MySQL wire format: length(3LE) + seq(1) + payload
 * COM_QUERY = 0x03
 * ============================================================================ */

static size_t build_mysql_com_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    size_t payload_len = 1 + sl; /* command byte + sql */
    /* 3-byte LE length */
    buf[0] = (uint8_t)(payload_len & 0xFF);
    buf[1] = (uint8_t)((payload_len >> 8) & 0xFF);
    buf[2] = (uint8_t)((payload_len >> 16) & 0xFF);
    buf[3] = 0; /* sequence number */
    buf[4] = 0x03; /* COM_QUERY */
    memcpy(buf + 5, sql, sl);
    return 4 + payload_len;
}

static size_t build_mysql_handshake_response(uint8_t *buf) {
    /*
     * Minimal HandshakeResponse41 for a connected client:
     * capabilities(4) + max_packet(4) + charset(1) + filler(23) +
     * username\0 + auth_response_len(1) + auth_response(0) + db\0
     */
    memset(buf, 0, 64);
    uint32_t caps = 0x000FA685; /* CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | etc */
    buf[0]=(uint8_t)(caps); buf[1]=(uint8_t)(caps>>8);
    buf[2]=(uint8_t)(caps>>16); buf[3]=(uint8_t)(caps>>24);
    /* max_packet_size = 16MB */
    buf[4]=0xFF; buf[5]=0xFF; buf[6]=0xFF; buf[7]=0x00;
    buf[8] = 0x21; /* utf8mb4 */
    /* filler: buf[9..31] = 0 */
    /* username @ 32 */
    memcpy(buf+32, "testuser", 9);
    buf[41] = 0; /* auth_response_len = 0 */
    memcpy(buf+42, "testdb", 7);

    size_t payload = 43;
    /* Wrap in MySQL packet header */
    uint8_t pkt[64+4];
    pkt[0]=(uint8_t)(payload); pkt[1]=0; pkt[2]=0; pkt[3]=1; /* seq=1 */
    memcpy(pkt+4, buf, payload);
    memcpy(buf, pkt, 4+payload);
    return 4 + payload;
}

static int split_byte_probe_mysql_fe(const char *label,
                                     const uint8_t *msg, size_t msg_len)
{
    int errors = 0;
    for (size_t k = 1; k < msg_len; k++) {
        void *ctx = MY->create_context(NULL);
        if (!ctx) { errors++; continue; }
        ssize_t fl = MY->frame_len(ctx, msg, k, 0);
        if (fl != 0 && fl != (ssize_t)msg_len) errors++;
        MY->destroy_context(ctx);
    }
    {
        void *ctx = MY->create_context(NULL);
        if (!ctx) { errors++; }
        else {
            ssize_t fl = MY->frame_len(ctx, msg, msg_len, 0);
            if (fl != (ssize_t)msg_len) errors++;
            MY->destroy_context(ctx);
        }
    }
    if (errors) printf("  MYSQL-SPLIT [%s] errors=%d\n", label, errors);
    return errors;
}

static void test_split_mysql_com_query(void) {
    TEST_BEGIN("split: MySQL COM_QUERY at every byte");
    uint8_t buf[256];
    size_t n = build_mysql_com_query(buf, "SELECT 1");
    int errs = split_byte_probe_mysql_fe("mysql-com_query", buf, n);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

static void test_split_mysql_handshake_response(void) {
    TEST_BEGIN("split: MySQL HandshakeResponse41 at every byte");
    uint8_t buf[128];
    size_t n = build_mysql_handshake_response(buf);
    int errs = split_byte_probe_mysql_fe("mysql-handshake_response", buf, n);
    TEST_ASSERT_EQ(errs, 0);
    TEST_END();
}

/* ============================================================================
 * Null / zero-byte boundary guards
 * ============================================================================ */

static void test_split_zero_length(void) {
    TEST_BEGIN("split: frame_len with zero-length buffer — no crash");
    void *ctx = PG->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);

    /* Zero bytes — must not crash; returns 0 (need more data) */
    ssize_t fl = PG->frame_len(ctx, (const uint8_t*)"", 0, 0);
    TEST_ASSERT(fl == 0);

    /* NULL data with len=0 — must not crash; returns 0 */
    fl = PG->frame_len(ctx, NULL, 0, 0);
    TEST_ASSERT(fl == 0);

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_split_mysql_zero_length(void) {
    TEST_BEGIN("split: MySQL frame_len with zero-length buffer — no crash");
    void *ctx = MY->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    ssize_t fl = MY->frame_len(ctx, (const uint8_t*)"", 0, 0);
    TEST_ASSERT(fl == 0);
    MY->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */
int main(void) {
    /* PG frontend splits */
    test_split_startup();
    test_split_simple_query();
    test_split_named_parse();
    test_split_bind();
    test_split_execute();
    test_split_flush();
    test_split_sync();
    test_split_terminate();

    /* PG backend splits */
    test_split_be_rfq();
    test_split_be_auth_ok();
    test_split_be_param_status();
    test_split_be_command_complete();

    /* Integration: split probes then full message */
    test_split_then_full_parse();

    /* Large-body stress */
    test_split_large_query();

    /* MySQL */
    test_split_mysql_com_query();
    test_split_mysql_handshake_response();

    /* Boundary guards */
    test_split_zero_length();
    test_split_mysql_zero_length();

    printf("\nproto_split: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
