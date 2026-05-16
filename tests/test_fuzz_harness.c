/**
 * @file test_fuzz_harness.c
 * @brief Shared fuzz and deterministic crash-regression harness for protocol parsers.
 *
 * This file gives the protocol layer two complementary safety nets.
 *
 * First, it exposes the conventional `LLVMFuzzerTestOneInput()` entry point so
 * AFL++ or libFuzzer-style workflows can mutate arbitrary frontend/backend byte
 * streams and look for memory corruption, hangs, or undefined behavior.
 *
 * Second, it doubles as a normal test binary with a deterministic corpus of
 * historically suspicious inputs. That keeps parser hardening in the standard
 * `ctest` path even when nobody is running a full fuzz campaign locally.
 *
 * The design deliberately feeds the same bytes to both PostgreSQL and MySQL
 * parsers. The intent is not semantic validity; it is resilience. A malformed
 * frame for one protocol is still useful chaos for the other because both sides
 * must reject hostile input without crashing or leaking memory.
 */

#include "keel/protocol/protocol_flow.h"
#include "keel/mem/mem.h"
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * External vtables
 * ============================================================================
 */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
extern const keel_proto_flow_vtable_t keel_proto_flow_mysql;

/* ============================================================================
 * Core Fuzz Function
 *
 * This is the AFL++ entry point AND the internal helper used by both
 * the AFL harness and the unit-test battery.
 * ============================================================================
 */

/**
 * @brief Feed arbitrary bytes into both protocol parsers and require only safe
 *        failure semantics.
 * @param data Candidate input buffer.
 * @param size Buffer length.
 * @return Always `0` so fuzzing engines interpret rejection as success rather
 *         than a harness failure.
 *
 * The contract is intentionally weak on parse success and very strict on memory
 * safety. Inputs may be truncated, oversized, protocol-misaligned, or nonsense.
 * The parsers are free to reject them in any ordinary way, but they must not
 * crash, loop forever, overflow, or trigger sanitizer findings while doing so.
 */
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size == 0) return 0;

    /* --- PostgreSQL parser --- */
    {
        void *ctx = keel_proto_flow_postgres.create_context(NULL);
        if (ctx) {
            keel_fe_action_t act;
            memset(&act, 0, sizeof(act));
            /* Feed as frontend message — must not crash */
            (void)keel_proto_flow_postgres.on_fe_msg(ctx, data, size, &act);

            /* Also try as backend message (simulates response parsing) */
            if (keel_proto_flow_postgres.on_be_msg) {
                keel_be_action_t be_act;
                memset(&be_act, 0, sizeof(be_act));
                (void)keel_proto_flow_postgres.on_be_msg(ctx, data, size, &be_act);
            }

            keel_proto_flow_postgres.destroy_context(ctx);
        }
    }

    /* --- MySQL parser --- */
    {
        void *ctx = keel_proto_flow_mysql.create_context(NULL);
        if (ctx) {
            keel_fe_action_t act;
            memset(&act, 0, sizeof(act));
            (void)keel_proto_flow_mysql.on_fe_msg(ctx, data, size, &act);

            if (keel_proto_flow_mysql.on_be_msg) {
                keel_be_action_t be_act;
                memset(&be_act, 0, sizeof(be_act));
                (void)keel_proto_flow_mysql.on_be_msg(ctx, data, size, &be_act);
            }

            keel_proto_flow_mysql.destroy_context(ctx);
        }
    }

    return 0;
}

/* ============================================================================
 * Unit-test battery — runs deterministically without AFL
 * ============================================================================
 */

/* Simple pass/fail counters (reuse test_utils style) */
static int s_run = 0, s_passed = 0, s_failed = 0;

#define FUZZ_ASSERT_NODEATH(label, buf, sz) \
    do { \
        s_run++; \
        printf("  [fuzz] %-55s ", (label)); \
        LLVMFuzzerTestOneInput((buf), (sz)); \
        /* If we reach here we didn't crash */ \
        printf("OK\n"); \
        s_passed++; \
    } while (0)

/* ---- Wire-protocol helpers ---- */

/**
 * @brief Encode a big-endian 32-bit length field for handcrafted PG messages.
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wr32be(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Execute a deterministic set of hostile inputs without an external fuzz
 *        engine.
 * @return
 *
 * These cases act as a regression suite for bugs that once seemed plausible or
 * common in similar proxies: huge lengths, truncated headers, unknown opcodes,
 * repeated degenerate byte patterns, and oversized names. The coverage is far
 * shallower than a real fuzz campaign, but it is cheap enough to run on every
 * sanitizer-enabled test cycle.
 */
static void run_unit_battery(void)
{
    printf("\n=== Fuzz Harness Unit Battery ===\n\n");

    /* 1. Zero-byte input */
    FUZZ_ASSERT_NODEATH("empty input (0 bytes)", NULL, 0);

    /* 2. Single byte — every possible message type byte */
    uint8_t single = 0;
    for (int i = 0; i < 256; i++) {
        single = (uint8_t)i;
        s_run++;
        LLVMFuzzerTestOneInput(&single, 1);
        s_passed++;
    }
    printf("  [fuzz] all 256 single-byte inputs                              OK\n");

    /* 3. Truncated PostgreSQL header (only 3 bytes instead of 5) */
    {
        uint8_t buf[3] = { 'Q', 0x00, 0x00 };
        FUZZ_ASSERT_NODEATH("PG Simple Query — truncated header (3 bytes)", buf, 3);
    }

    /* 4. PG Simple Query with maximum realistic length */
    {
        const size_t sql_len = 65536;
        uint8_t *buf = malloc(1 + 4 + sql_len + 1);
        if (buf) {
            buf[0] = 'Q';
            wr32be(buf + 1, (uint32_t)(4 + sql_len + 1));
            memset(buf + 5, 'A', sql_len);
            buf[5 + sql_len] = '\0';
            FUZZ_ASSERT_NODEATH("PG Simple Query — 64 KiB SQL string",
                                buf, 1 + 4 + sql_len + 1);
            free(buf);
        }
    }

    /* 5. PG Parse (extended query) with a very long statement name */
    {
        const size_t name_len = 65535;
        /* 'P' + len(4) + name(name_len+1) + query\0 + numparams(2) */
        size_t total = 1 + 4 + name_len + 1 + 1 + 2;
        uint8_t *buf = calloc(1, total);
        if (buf) {
            buf[0] = 'P';
            wr32be(buf + 1, (uint32_t)(4 + name_len + 1 + 1 + 2));
            memset(buf + 5, 'x', name_len);
            buf[5 + name_len]     = '\0'; /* name terminator */
            buf[5 + name_len + 1] = '\0'; /* empty query terminator */
            buf[5 + name_len + 2] = 0;
            buf[5 + name_len + 3] = 0;   /* numparams = 0 */
            FUZZ_ASSERT_NODEATH("PG Parse — 64 KiB statement name", buf, total);
            free(buf);
        }
    }

    /* 6. PG with claimed length far greater than actual data */
    {
        uint8_t buf[9] = { 'Q', 0xFF, 0xFF, 0xFF, 0xFF, 'S', 'E', 'L', '\0' };
        FUZZ_ASSERT_NODEATH("PG Query — claimed length = 0xFFFFFFFF (overflow)",
                            buf, sizeof(buf));
    }

    /* 7. PG — unknown message type byte */
    {
        uint8_t types[] = { 0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF };
        for (size_t i = 0; i < sizeof(types); i++) {
            uint8_t buf[5];
            buf[0] = types[i];
            wr32be(buf + 1, 4);
            s_run++;
            LLVMFuzzerTestOneInput(buf, 5);
            s_passed++;
        }
        printf("  [fuzz] PG unknown message types (0x00,0x01,0x7F,0x80,0xFE,0xFF) OK\n");
    }

    /* 8. All-zeros buffer of increasing size */
    {
        uint8_t zeros[256];
        memset(zeros, 0, sizeof(zeros));
        for (size_t sz = 1; sz <= sizeof(zeros); sz++) {
            s_run++;
            LLVMFuzzerTestOneInput(zeros, sz);
            s_passed++;
        }
        printf("  [fuzz] all-zeros buffer, sizes 1..256                          OK\n");
    }

    /* 9. All-0xFF buffer */
    {
        uint8_t ones[256];
        memset(ones, 0xFF, sizeof(ones));
        FUZZ_ASSERT_NODEATH("all-0xFF buffer (256 bytes)", ones, sizeof(ones));
    }

    /* 10. MySQL — truncated handshake response (5 bytes) */
    {
        /* MySQL client → server: capability_flags(4) + max_packet_size(4) + ... */
        uint8_t buf[5] = { 0x85, 0xa6, 0xFF, 0x01, 0x00 };
        FUZZ_ASSERT_NODEATH("MySQL — truncated client handshake (5 bytes)", buf, 5);
    }

    /* 11. MySQL — COM_QUERY with empty payload */
    {
        /* MySQL COM_QUERY packet: seq(1) + length(3-LE) + type(1) + payload */
        uint8_t buf[5] = { 0x01, 0x00, 0x00, 0x00, 0x03 /* COM_QUERY */ };
        FUZZ_ASSERT_NODEATH("MySQL — COM_QUERY with empty payload", buf, 5);
    }

    /* 12. MySQL — COM_STMT_PREPARE with a 32 KiB statement */
    {
        const size_t sql_len = 32768;
        size_t pkt_len = 1 + sql_len; /* type + sql */
        /* MySQL packet: pkt_len(3-LE) + seq(1) + type(1) + sql */
        uint8_t hdr[4];
        hdr[0] = (uint8_t)(pkt_len & 0xFF);
        hdr[1] = (uint8_t)((pkt_len >> 8) & 0xFF);
        hdr[2] = (uint8_t)((pkt_len >> 16) & 0xFF);
        hdr[3] = 0; /* seq */

        uint8_t *buf = calloc(1, 4 + pkt_len + 1);  /* +1 for null terminator */
        if (buf) {
            memcpy(buf, hdr, 4);
            buf[4] = 0x16; /* COM_STMT_PREPARE */
            memset(buf + 5, 'A', sql_len - 1);
            buf[4 + pkt_len] = '\0';
            FUZZ_ASSERT_NODEATH("MySQL — COM_STMT_PREPARE with 32 KiB statement",
                                buf, 4 + pkt_len);
            free(buf);
        }
    }

    /* 13. Alternating valid/invalid PG messages (sequence attack) */
    {
        uint8_t buf[30];
        /* Valid Query: 'Q' + len(4) + "SELECT 1\0" */
        buf[0] = 'Q';
        wr32be(buf + 1, 14);
        memcpy(buf + 5, "SELECT 1\0", 9);
        /* Followed by garbage */
        memset(buf + 14, 0xAB, 16);
        FUZZ_ASSERT_NODEATH("PG valid+garbage concatenated (30 bytes)", buf, 30);
    }

    /* 14. Integer overflow: claimed body length causes integer wrap */
    {
        uint8_t buf[5];
        buf[0] = 'P'; /* Parse */
        wr32be(buf + 1, 0xFFFFFFFEU); /* 4294967294 – very large, body wraps */
        FUZZ_ASSERT_NODEATH("PG Parse — length = 0xFFFFFFFE (near-overflow)", buf, 5);
    }

    printf("\n  %d inputs tested, %d OK, %d unexpected crashes\n",
           s_run, s_passed, s_failed);
}

/* ============================================================================
 * Phase B — Split-at-every-byte generator (§30 item #2)
 *
 * Take a well-formed complete protocol message and feed prefixes of lengths
 * 1, 2, 3, … N to the parsers.  This exercises all partial-read code paths
 * (incomplete header, incomplete length field, header present but body absent,
 * body partially received).  No crash or memory error is acceptable at any
 * prefix length.
 * ============================================================================
 */

static void run_split_battery(const char *label,
                               const uint8_t *msg, size_t msg_len)
{
    printf("  [split] %-48s ", label);
    for (size_t cut = 1; cut <= msg_len; cut++) {
        LLVMFuzzerTestOneInput(msg, cut);
        s_run++;
        s_passed++;
    }
    printf("OK (%zu prefixes)\n", msg_len);
}

static void run_split_phase(void)
{
    printf("\n=== Fuzz Harness Phase B — Split-at-Every-Byte ===\n\n");

    /* PG1: Simple Query "SELECT 1;" */
    {
        const char *sql = "SELECT 1;";
        size_t sql_len = strlen(sql) + 1; /* include NUL */
        size_t total = 1 + 4 + sql_len;
        uint8_t *buf = calloc(1, total);
        if (buf) {
            buf[0] = 'Q';
            wr32be(buf + 1, (uint32_t)(4 + sql_len));
            memcpy(buf + 5, sql, sql_len);
            run_split_battery("PG SimpleQuery 'SELECT 1;'", buf, total);
            free(buf);
        }
    }

    /* PG2: Parse (unnamed prepared stmt "SELECT $1") */
    {
        const char *stmt_name = "";   /* unnamed */
        const char *query     = "SELECT $1";
        size_t n_len  = strlen(stmt_name) + 1;
        size_t q_len  = strlen(query)     + 1;
        /* body: name\0 + query\0 + numparams(2) */
        size_t body   = n_len + q_len + 2;
        size_t total  = 1 + 4 + body;
        uint8_t *buf  = calloc(1, total);
        if (buf) {
            buf[0] = 'P';
            wr32be(buf + 1, (uint32_t)(4 + body));
            size_t off = 5;
            memcpy(buf + off, stmt_name, n_len); off += n_len;
            memcpy(buf + off, query,     q_len); off += q_len;
            buf[off] = 0; buf[off+1] = 0; /* numparams = 0 */
            run_split_battery("PG Parse 'SELECT $1'", buf, total);
            free(buf);
        }
    }

    /* PG3: Bind message (zero params, unnamed portal) */
    {
        /* Bind: portal\0 + stmt\0 + nfmt(2) + nparams(2) + nresult(2) */
        const char *portal = "";
        const char *stmt   = "";
        size_t p_len = strlen(portal) + 1;
        size_t s_len = strlen(stmt)   + 1;
        size_t body  = p_len + s_len + 2 + 2 + 2;
        size_t total = 1 + 4 + body;
        uint8_t *buf = calloc(1, total);
        if (buf) {
            buf[0] = 'B';
            wr32be(buf + 1, (uint32_t)(4 + body));
            memcpy(buf + 5, portal, p_len);
            memcpy(buf + 5 + p_len, stmt, s_len);
            /* nfmt=0, nparams=0, nresult=0 are already zero */
            run_split_battery("PG Bind (unnamed portal, no params)", buf, total);
            free(buf);
        }
    }

    /* PG4: Execute + Sync combo */
    {
        /* Execute: portal\0 + maxrows(4) */
        const char *portal = "";
        size_t p_len = strlen(portal) + 1;
        size_t exec_body = p_len + 4;
        /* Sync: just the length field (no body) */
        size_t sync_body = 0;
        size_t total = (1 + 4 + exec_body) + (1 + 4 + sync_body);
        uint8_t *buf = calloc(1, total);
        if (buf) {
            size_t off = 0;
            buf[off++] = 'E';
            wr32be(buf + off, (uint32_t)(4 + exec_body)); off += 4;
            memcpy(buf + off, portal, p_len); off += p_len;
            wr32be(buf + off, 0); off += 4; /* maxrows = unlimited */
            buf[off++] = 'S';
            wr32be(buf + off, 4); /* Sync has no body */
            run_split_battery("PG Execute+Sync (no params)", buf, total);
            free(buf);
        }
    }

    /* PG5: SSL request (startup-phase special packet) */
    {
        uint8_t ssl_req[8];
        wr32be(ssl_req,     8);        /* total length */
        wr32be(ssl_req + 4, 80877103); /* SSLRequest magic */
        run_split_battery("PG SSLRequest (startup)", ssl_req, sizeof(ssl_req));
    }

    /* PG6: CancelRequest */
    {
        uint8_t cancel[16];
        wr32be(cancel,      16);       /* total length */
        wr32be(cancel + 4,  80877102); /* CancelRequest magic */
        wr32be(cancel + 8,  0x0001);   /* PID */
        wr32be(cancel + 12, 0xABCD);   /* secret */
        run_split_battery("PG CancelRequest", cancel, sizeof(cancel));
    }

    /* MySQL1: COM_QUERY "SELECT 1" */
    {
        const char *sql = "SELECT 1";
        size_t sql_len  = strlen(sql);
        size_t pkt_body = 1 + sql_len; /* type + sql (no NUL in MySQL) */
        size_t total    = 4 + pkt_body;
        uint8_t *buf    = calloc(1, total);
        if (buf) {
            /* 3-byte LE packet length */
            buf[0] = (uint8_t)(pkt_body & 0xFF);
            buf[1] = (uint8_t)((pkt_body >> 8) & 0xFF);
            buf[2] = 0;
            buf[3] = 0;          /* sequence number */
            buf[4] = 0x03;       /* COM_QUERY */
            memcpy(buf + 5, sql, sql_len);
            run_split_battery("MySQL COM_QUERY 'SELECT 1'", buf, total);
            free(buf);
        }
    }

    /* MySQL2: COM_PING */
    {
        uint8_t ping[5] = { 0x01, 0x00, 0x00, 0x00, 0x0E /* COM_PING */ };
        run_split_battery("MySQL COM_PING", ping, sizeof(ping));
    }

    /* MySQL3: COM_QUIT */
    {
        uint8_t quit[5] = { 0x01, 0x00, 0x00, 0x00, 0x01 /* COM_QUIT */ };
        run_split_battery("MySQL COM_QUIT", quit, sizeof(quit));
    }

    /* MySQL4: COM_STMT_PREPARE "SELECT $1" */
    {
        const char *sql   = "SELECT $1";
        size_t sql_len    = strlen(sql);
        size_t pkt_body   = 1 + sql_len;
        size_t total      = 4 + pkt_body;
        uint8_t *buf      = calloc(1, total);
        if (buf) {
            buf[0] = (uint8_t)(pkt_body & 0xFF);
            buf[1] = (uint8_t)((pkt_body >> 8) & 0xFF);
            buf[2] = 0;
            buf[3] = 0;
            buf[4] = 0x16; /* COM_STMT_PREPARE */
            memcpy(buf + 5, sql, sql_len);
            run_split_battery("MySQL COM_STMT_PREPARE 'SELECT $1'", buf, total);
            free(buf);
        }
    }

    printf("\n  Split-at-every-byte phase: %d inputs, %d OK, %d failed\n",
           s_run, s_passed, s_failed);
}

/* ============================================================================
 * main — unit-test entry point (not used by AFL++)
 * ============================================================================
 */
#ifndef __AFL_FUZZ_TESTCASE_BUF

int main(void)
{
    keel_mem_init(NULL);

    printf("=== Fuzz Harness Tests (Phase A — AFL++ Protocol Safety) ===\n");

    run_unit_battery();

    run_split_phase();

    keel_mem_shutdown();

    if (s_failed > 0) {
        fprintf(stderr, "\nFAIL: %d crash(es) detected during fuzzing battery.\n",
                s_failed);
        return 1;
    }

    printf("\nAll fuzz battery inputs handled without crashes.\n");
    return 0;
}

#endif /* !__AFL_FUZZ_TESTCASE_BUF */
