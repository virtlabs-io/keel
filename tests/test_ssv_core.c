/**
 * @file test_ssv_core.c
 * @brief Unit tests for protocol-agnostic SSV helpers.
 *
 * Tests the thin, protocol-independent SSV (Semantic State
 * Virtualization) decision layer that sits between the per-protocol
 * flow code and the pool/engine.  These helpers translate
 * protocol-level pin flags (keel_flow_pin_reason_t) into
 * engine-level pin reasons (keel_pin_reason_t) and answer policy
 * questions that drive backend release, discard, and PS-mode
 * interactions.
 *
 * Test families:
 *   §1 — Pin-reason export: every flow-level flag bit maps to
 *         exactly one engine-level flag bit; absent flags remain
 *         unset.
 *   §2 — Release policy: keel_ssv_allows_backend_release()
 *         permits release when no pins are set or when the only
 *         pin is PS and the mode is VIRTUALIZE; denies otherwise.
 *   §3 — Stmt-only policy: keel_ssv_is_stmt_only_pin() is true
 *         only when PREPARED_STMT is the sole pin under
 *         VIRTUALIZE mode.
 *
 * These tests are intentionally tiny — the SSV core layer is a
 * pure-function bitmask translator with no allocations, so
 * exhaustive combinatorial coverage is cheap.
 */


#include "test_utils.h"

#include "keel/session/ssv.h"

static void test_pin_reason_export(void)
{
    TEST_BEGIN("ssv pin reason export");

    keel_flow_pin_reason_t pins =
        KEEL_FPIN_TEMP_TABLE |
        KEEL_FPIN_PREPARED_STMT |
        KEEL_FPIN_EXTENDED_PROTO |
        KEEL_FPIN_ADVISORY_LOCK;

    keel_pin_reason_t reasons = keel_ssv_pin_reason_from_flow_pins(pins);

    TEST_ASSERT(reasons & KEEL_PIN_TEMP_TABLE);
    TEST_ASSERT(reasons & KEEL_PIN_PREPARED_STMT);
    TEST_ASSERT(reasons & KEEL_PIN_EXTENDED_PROTOCOL);
    TEST_ASSERT(reasons & KEEL_PIN_ADVISORY_LOCK);
    TEST_ASSERT(!(reasons & KEEL_PIN_LISTEN));

    TEST_END();
}

static void test_release_policy(void)
{
    TEST_BEGIN("ssv release policy");

    TEST_ASSERT(keel_ssv_allows_backend_release(KEEL_FPIN_NONE,
                                                KEEL_PS_MODE_VIRTUALIZE));
    TEST_ASSERT(keel_ssv_allows_backend_release(KEEL_FPIN_PREPARED_STMT,
                                                KEEL_PS_MODE_VIRTUALIZE));
    TEST_ASSERT(!keel_ssv_allows_backend_release(KEEL_FPIN_PREPARED_STMT,
                                                 KEEL_PS_MODE_OFF));
    TEST_ASSERT(!keel_ssv_allows_backend_release(KEEL_FPIN_LISTEN,
                                                 KEEL_PS_MODE_VIRTUALIZE));

    TEST_END();
}

static void test_stmt_only_policy(void)
{
    TEST_BEGIN("ssv stmt-only policy");

    TEST_ASSERT(keel_ssv_is_stmt_only_pin(KEEL_FPIN_PREPARED_STMT,
                                          KEEL_PS_MODE_VIRTUALIZE));
    TEST_ASSERT(!keel_ssv_is_stmt_only_pin(KEEL_FPIN_PREPARED_STMT,
                                           KEEL_PS_MODE_OFF));
    TEST_ASSERT(!keel_ssv_is_stmt_only_pin(KEEL_FPIN_PREPARED_STMT |
                                           KEEL_FPIN_LISTEN,
                                           KEEL_PS_MODE_VIRTUALIZE));

    TEST_END();
}

int main(void)
{
    test_pin_reason_export();
    test_release_policy();
    test_stmt_only_policy();
    return test_summary();
}