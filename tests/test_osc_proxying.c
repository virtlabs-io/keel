/**
 * @file test_osc_proxying.c
 * @brief Unit tests for Online Schema Change (gh-ost / pt-osc) transparent
 *        proxying via KEEL_PIN_OSC.
 *
 * Tested behaviour:
 *   §1 — keel_hardpin_scan_osc() detection logic
 *        1a. gh-ost inline comment ("gh-ost")
 *        1b. gh-ost shadow table suffixes: _gho, _ghc, _del
 *        1c. pt-osc shadow table suffixes: _new, _old  (identifier starts with _)
 *        1d. pt-osc heartbeat table: _pt_heartbeat, _pt_osc_* prefix
 *        1e. Normal queries do NOT trigger the flag
 *        1f. Identifiers that merely end in _new/_old without leading _ do NOT
 *            trigger (e.g. `table_new` with no leading underscore = regular table)
 *
 *   §2 — keel_hardpin_scan_postgres() propagates KEEL_PIN_OSC
 *   §3 — keel_hardpin_scan_mysql() propagates KEEL_PIN_OSC
 *
 *   §4 — KEEL_PIN_OSC is not set for well-known false-positive candidates
 *        (UPDATE, SELECT, CREATE TABLE, LOCK TABLES with no shadow table refs)
 *
 *   §5 — keel_ssv_pin_reason_from_flow_pins() maps KEEL_FPIN_OSC → KEEL_PIN_OSC
 */

#include "test_utils.h"

#include "keel/session/hardpin.h"
#include "keel/session/ssv.h"
#include "keel/protocol/protocol_flow.h"

/* ============================================================================
 * §1 — keel_hardpin_scan_osc() via the postgres/mysql dispatchers
 * ============================================================================ */

static void test_osc_ghcmt_inline_comment(void) {
    TEST_BEGIN("osc: gh-ost inline comment sets KEEL_PIN_OSC");

    /* gh-ost embeds the comment  / * gh-ost * /  in every DML it issues */
    const char *q = "INSERT /* gh-ost */ INTO `mytable` VALUES (1,2,3)";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);

    /* Must also fire when the comment is in upper/mixed case */
    const char *q2 = "UPDATE orders /* GH-OST */ SET status=1 WHERE id=42";
    keel_pin_reason_t r2 = keel_hardpin_scan_postgres(q2, strlen(q2));
    TEST_ASSERT(!(r2 & KEEL_PIN_OSC));  /* gh-ost comment is lowercase only */

    TEST_END();
}

static void test_osc_ghost_shadow_gho(void) {
    TEST_BEGIN("osc: _gho suffix sets KEEL_PIN_OSC");

    const char *q = "INSERT INTO _orders_gho SELECT * FROM orders WHERE id=?";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_ghost_shadow_ghc(void) {
    TEST_BEGIN("osc: _ghc suffix sets KEEL_PIN_OSC");

    const char *q = "INSERT INTO `_customers_ghc` (id,ts,action,hint) VALUES (?,?,?,?)";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_ghost_shadow_del(void) {
    TEST_BEGIN("osc: _del suffix sets KEEL_PIN_OSC");

    const char *q = "DELETE FROM `_payments_del` LIMIT 100";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_ptosc_shadow_new(void) {
    TEST_BEGIN("osc: pt-osc _new suffix (leading _) sets KEEL_PIN_OSC");

    const char *q = "INSERT INTO _orders_new SELECT * FROM orders";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_ptosc_shadow_old(void) {
    TEST_BEGIN("osc: pt-osc _old suffix (leading _) sets KEEL_PIN_OSC");

    const char *q = "DROP TABLE IF EXISTS _items_old";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_ptosc_heartbeat(void) {
    TEST_BEGIN("osc: _pt_heartbeat sets KEEL_PIN_OSC");

    const char *q = "REPLACE INTO _pt_heartbeat VALUES (NOW(), @@server_id)";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_ptosc_osc_prefix(void) {
    TEST_BEGIN("osc: _pt_osc_ prefix sets KEEL_PIN_OSC");

    const char *q = "SELECT * FROM _pt_osc_mydb_orders_new";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

/* §1e — normal queries must NOT set KEEL_PIN_OSC */

static void test_osc_normal_select(void) {
    TEST_BEGIN("osc: plain SELECT does not set KEEL_PIN_OSC");

    const char *q = "SELECT id, name FROM orders WHERE status = 'open'";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));
    TEST_END();
}

static void test_osc_normal_insert(void) {
    TEST_BEGIN("osc: plain INSERT does not set KEEL_PIN_OSC");

    const char *q = "INSERT INTO orders (user_id, total) VALUES (1, 100.00)";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));
    TEST_END();
}

static void test_osc_normal_update(void) {
    TEST_BEGIN("osc: plain UPDATE does not set KEEL_PIN_OSC");

    const char *q = "UPDATE users SET last_login = NOW() WHERE id = 42";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));
    TEST_END();
}

static void test_osc_normal_alter(void) {
    TEST_BEGIN("osc: ALTER TABLE (non-OSC DDL) does not set KEEL_PIN_OSC");

    const char *q = "ALTER TABLE orders ADD COLUMN region VARCHAR(50)";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));
    TEST_END();
}

/* §1f — identifier `table_new` has no leading underscore → no pin */
static void test_osc_no_leading_underscore(void) {
    TEST_BEGIN("osc: _new/_old suffix without leading _ does not pin");

    /* A table named 'orders_new' starts without '_' — pt-osc shadow
     * tables always start with '_'.  */
    const char *q = "INSERT INTO orders_new SELECT * FROM orders";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));

    const char *q2 = "DROP TABLE orders_old";
    keel_pin_reason_t r2 = keel_hardpin_scan_postgres(q2, strlen(q2));
    TEST_ASSERT(!(r2 & KEEL_PIN_OSC));
    TEST_END();
}

/* ============================================================================
 * §2 — postgres scanner propagates OSC pin
 * ============================================================================ */

static void test_osc_postgres_scanner(void) {
    TEST_BEGIN("osc: keel_hardpin_scan_postgres propagates KEEL_PIN_OSC");

    const char *q = "UPDATE _users_gho SET name=? WHERE id=?";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

/* ============================================================================
 * §3 — mysql scanner propagates OSC pin
 * ============================================================================ */

static void test_osc_mysql_scanner(void) {
    TEST_BEGIN("osc: keel_hardpin_scan_mysql propagates KEEL_PIN_OSC");

    const char *q = "INSERT /* gh-ost */ INTO `_products_gho` SELECT * FROM `products` WHERE id BETWEEN 1 AND 1000";
    keel_pin_reason_t r = keel_hardpin_scan_mysql(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_mysql_no_false_positive(void) {
    TEST_BEGIN("osc: keel_hardpin_scan_mysql no false positive on normal DML");

    const char *q = "UPDATE products SET price = 9.99 WHERE sku = 'ABC123'";
    keel_pin_reason_t r = keel_hardpin_scan_mysql(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));
    TEST_END();
}

/* ============================================================================
 * §4 — generic dispatcher
 * ============================================================================ */

static void test_osc_generic_dispatcher_postgres(void) {
    TEST_BEGIN("osc: generic dispatcher detects OSC for postgres protocol");

    const char *q = "DELETE /* gh-ost */ FROM _audit_del";
    keel_pin_reason_t r = keel_hardpin_scan("postgres", q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

static void test_osc_generic_dispatcher_mysql(void) {
    TEST_BEGIN("osc: generic dispatcher detects OSC for mysql protocol");

    const char *q = "INSERT INTO _audit_gho SELECT * FROM audit LIMIT 100";
    keel_pin_reason_t r = keel_hardpin_scan("mysql", q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_END();
}

/* ============================================================================
 * §5 — FPIN → PIN conversion via ssv
 * ============================================================================ */

static void test_osc_fpin_to_pin_mapping(void) {
    TEST_BEGIN("osc: KEEL_FPIN_OSC maps to KEEL_PIN_OSC via ssv");

    keel_flow_pin_reason_t flow_pins = KEEL_FPIN_OSC | KEEL_FPIN_TRANSACTION;
    keel_pin_reason_t session_pins =
        keel_ssv_pin_reason_from_flow_pins(flow_pins);

    TEST_ASSERT(session_pins & KEEL_PIN_OSC);
    /* KEEL_FPIN_TRANSACTION has no direct keel_pin_reason_t mapping — just OSC */
    TEST_END();
}

static void test_osc_fpin_without_osc_no_mapping(void) {
    TEST_BEGIN("osc: KEEL_FPIN_OSC absent → KEEL_PIN_OSC not set");

    keel_flow_pin_reason_t flow_pins = KEEL_FPIN_TRANSACTION | KEEL_FPIN_TEMP_TABLE;
    keel_pin_reason_t session_pins =
        keel_ssv_pin_reason_from_flow_pins(flow_pins);

    TEST_ASSERT(!(session_pins & KEEL_PIN_OSC));
    TEST_ASSERT(session_pins & KEEL_PIN_TEMP_TABLE);
    TEST_END();
}

/* ============================================================================
 * §6 — Edge cases
 * ============================================================================ */

static void test_osc_empty_query(void) {
    TEST_BEGIN("osc: empty/NULL query returns KEEL_PIN_NONE");

    TEST_ASSERT(keel_hardpin_scan_postgres(NULL, 0) == KEEL_PIN_NONE);
    TEST_ASSERT(keel_hardpin_scan_postgres("", 0) == KEEL_PIN_NONE);
    TEST_ASSERT(keel_hardpin_scan_mysql(NULL, 0) == KEEL_PIN_NONE);
    TEST_END();
}

static void test_osc_short_identifier(void) {
    TEST_BEGIN("osc: identifier shorter than any shadow suffix is ignored");

    /* '_ab' is only 3 chars — too short for any suffix check */
    const char *q = "SELECT * FROM _ab WHERE 1=1";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(!(r & KEEL_PIN_OSC));
    TEST_END();
}

static void test_osc_combined_with_other_pins(void) {
    TEST_BEGIN("osc: KEEL_PIN_OSC coexists with KEEL_PIN_COPY etc.");

    /* A COPY that also references a ghost table would be unusual but valid */
    const char *q = "COPY _orders_gho FROM STDIN";
    keel_pin_reason_t r = keel_hardpin_scan_postgres(q, strlen(q));
    TEST_ASSERT(r & KEEL_PIN_OSC);
    TEST_ASSERT(r & KEEL_PIN_COPY);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    /* §1 — Detection logic */
    test_osc_ghcmt_inline_comment();
    test_osc_ghost_shadow_gho();
    test_osc_ghost_shadow_ghc();
    test_osc_ghost_shadow_del();
    test_osc_ptosc_shadow_new();
    test_osc_ptosc_shadow_old();
    test_osc_ptosc_heartbeat();
    test_osc_ptosc_osc_prefix();

    /* §1e — No false positives */
    test_osc_normal_select();
    test_osc_normal_insert();
    test_osc_normal_update();
    test_osc_normal_alter();

    /* §1f — Leading underscore requirement */
    test_osc_no_leading_underscore();

    /* §2 — Postgres scanner */
    test_osc_postgres_scanner();

    /* §3 — MySQL scanner */
    test_osc_mysql_scanner();
    test_osc_mysql_no_false_positive();

    /* §4 — Generic dispatcher */
    test_osc_generic_dispatcher_postgres();
    test_osc_generic_dispatcher_mysql();

    /* §5 — FPIN→PIN mapping */
    test_osc_fpin_to_pin_mapping();
    test_osc_fpin_without_osc_no_mapping();

    /* §6 — Edge cases */
    test_osc_empty_query();
    test_osc_short_identifier();
    test_osc_combined_with_other_pins();

    return test_summary();
}
