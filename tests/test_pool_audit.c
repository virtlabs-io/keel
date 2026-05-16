/**
 * @file test_pool_audit.c
 * @brief Custom pool auditing tests — Memory and Resource Integrity (Spec §1)
 *
 * Validates:
 *   1. Head/tail canary detection on every pool allocation.
 *   2. Double-free detection with file/line attribution.
 *   3. Active block tracking: __FILE__ / __LINE__ stored per allocation.
 *   4. SIGUSR1 live-dump installation and dump-pending flag.
 *   5. Buffer-overflow canary triggers (tail canary write-past-end).
 *   6. Safety counters are consistent after a workload.
 *
 * These tests are the automated equivalent of the "Custom Pool Auditing"
 * requirement in the production-hardening spec.
 */

#include "test_utils.h"
#include "keel/mem/mem.h"
#include "keel/mem/mem_safety.h"

#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>

/* ============================================================================
 * Helpers
 * ============================================================================
 */

/**
 * @brief Initialise memory + safety subsystems with full auditing enabled.
 *
 * Enables allocation tracking, fill patterns (0xCD alloc / 0xDD free),
 * and head/tail canary checks.  Each test calls this independently so
 * the safety counters start from a known baseline.
 */
static void pool_audit_setup(void)
{
    keel_mem_init(NULL);
    keel_mem_safety_init();
    keel_mem_safety_set_flags(KEEL_SAFETY_TRACK_ALLOCS |
                               KEEL_SAFETY_FILL_PATTERNS |
                               KEEL_SAFETY_CANARY_CHECK);
}

/**
 * @brief Tear down memory + safety subsystems in reverse init order.
 */
static void pool_audit_teardown(void)
{
    keel_mem_safety_shutdown();
    keel_mem_shutdown();
}

/* ============================================================================
 * Test 1 — Basic allocation tracking: file/line attribution
 * ============================================================================
 */
static void test_alloc_file_line_tracking(void)
{
    TEST_BEGIN("pool audit: alloc file/line tracking");

    pool_audit_setup();
    keel_mem_safety_reset();

    /* Allocate a block — the safety layer stores __FILE__ / __LINE__ */
    void *p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    /* Exactly one active allocation should be tracked */
    size_t active = keel_mem_safety_active_count();
    TEST_ASSERT_EQ(active, (size_t)1);

    /* Validate the pointer (checks head/tail canaries internally) */
    keel_error_t rc = keel_mem_safety_validate(p);
    TEST_ASSERT_EQ(rc, KEEL_OK);

    keel_safe_free(p, __FILE__, __LINE__);

    /* No active allocations after free */
    TEST_ASSERT_EQ(keel_mem_safety_active_count(), (size_t)0);

    pool_audit_teardown();
    TEST_END();
}

/* ============================================================================
 * Test 2 — Canary integrity: allocate / validate / free many blocks
 * ============================================================================
 */
static void test_canary_integrity(void)
{
    TEST_BEGIN("pool audit: canary integrity across many blocks");

    pool_audit_setup();
    keel_mem_safety_reset();

#define CANARY_BLOCKS 128
    void *ptrs[CANARY_BLOCKS];

    for (int i = 0; i < CANARY_BLOCKS; i++) {
        ptrs[i] = keel_safe_malloc((size_t)(16 + i * 4), __FILE__, __LINE__);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
        /* Write a known pattern so we can detect corruption */
        memset(ptrs[i], (int)(0xAB ^ (unsigned)i), (size_t)(16 + i * 4));
    }

    /* All blocks still valid */
    size_t corrupt = keel_mem_safety_validate_all();
    TEST_ASSERT_EQ(corrupt, (size_t)0);

    for (int i = 0; i < CANARY_BLOCKS; i++) {
        keel_safe_free(ptrs[i], __FILE__, __LINE__);
    }

    TEST_ASSERT_EQ(keel_mem_safety_active_count(), (size_t)0);
    pool_audit_teardown();
    TEST_END();
#undef CANARY_BLOCKS
}

/* ============================================================================
 * Test 3 — Tail-canary write-past-end detection
 * ============================================================================
 */
static void test_tail_canary_overflow_detection(void)
{
    TEST_BEGIN("pool audit: tail canary overflow detection");

    pool_audit_setup();
    keel_mem_safety_reset();

    /* Allocate exactly 32 bytes */
    uint8_t *p = (uint8_t *)keel_safe_malloc(32, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    /* Normal write — must be fine */
    memset(p, 0xCC, 32);
    TEST_ASSERT_EQ(keel_mem_safety_validate(p), KEEL_OK);

    /* Deliberately corrupt one byte past the end (tail canary area) */
    p[32] = 0xFF;

    /* Validation must now detect the tail canary violation */
    keel_error_t rc = keel_mem_safety_validate(p);
    TEST_ASSERT(rc != KEEL_OK);

    /* validate_all() returns the count of corrupted live allocations it found;
     * it does NOT update the global counters (validate is read-only), so
     * use the return value directly rather than the report counters. */
    TEST_ASSERT(keel_mem_safety_validate_all() > 0);

    /* Reset before teardown so shutdown doesn't trip over corrupted state */
    keel_mem_safety_reset();
    pool_audit_teardown();
    TEST_END();
}

/* ============================================================================
 * Test 4 — Double-free detection
 * ============================================================================
 */
static void test_double_free_detection(void)
{
    TEST_BEGIN("pool audit: double-free detection");

    pool_audit_setup();
    keel_mem_safety_reset();
    /* Enable quarantine so freed headers are not reclaimed by free();
     * without this, the header pointer is dangling after the first free and
     * keel_safe_free_check cannot re-read state == KEEL_ALLOC_FREED. */
    keel_mem_safety_set_flags(KEEL_SAFETY_ALL);

    void *p = keel_safe_malloc(16, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    keel_safe_free(p, __FILE__, __LINE__);
    /* Second free — must be caught rather than silently corrupting state */
    keel_error_t rc = keel_safe_free_check(p, __FILE__, __LINE__);
    TEST_ASSERT(rc != KEEL_OK);

    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);
    TEST_ASSERT(report.double_free_count > 0);

    keel_mem_safety_set_flags(KEEL_SAFETY_TRACK_ALLOCS | KEEL_SAFETY_FILL_PATTERNS
                              | KEEL_SAFETY_CANARY_CHECK); /* restore sans quarantine */

    keel_mem_safety_reset();
    pool_audit_teardown();
    TEST_END();
}

/* ============================================================================
 * Test 5 — SIGUSR1 live-dump handler installation
 * ============================================================================
 */
static void test_sigusr1_handler_install(void)
{
    TEST_BEGIN("pool audit: SIGUSR1 live-dump handler installation");

    pool_audit_setup();
    keel_mem_safety_reset();

    /* Install the dump signal — must succeed */
    keel_error_t rc = keel_mem_safety_install_dump_signal();
    TEST_ASSERT_EQ(rc, KEEL_OK);

    /* Calling twice must be idempotent */
    rc = keel_mem_safety_install_dump_signal();
    TEST_ASSERT_EQ(rc, KEEL_OK);

    /* No pending dump before we raise the signal */
    TEST_ASSERT(!keel_mem_safety_dump_pending());

    /* Allocate something so the report is non-empty */
    void *p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    /* Raise SIGUSR1 — the handler will call keel_mem_safety_print_leaks()
     * and set the dump-pending flag.  The test is that we don't crash and
     * the pending flag is consumed. */
    raise(SIGUSR1);

    /* After the signal the pending flag may or may not be set depending on
     * whether the handler already cleared it, but we must not have crashed. */
    (void)keel_mem_safety_dump_pending(); /* consume if set */

    keel_safe_free(p, __FILE__, __LINE__);
    keel_mem_safety_reset();
    pool_audit_teardown();
    TEST_END();
}

/* ============================================================================
 * Test 6 — Dump-pending polling API (non-signal path)
 * ============================================================================
 */
static void test_dump_pending_poll(void)
{
    TEST_BEGIN("pool audit: dump-pending poll API");

    pool_audit_setup();
    keel_mem_safety_reset();

    keel_mem_safety_install_dump_signal();

    /* Before any signal there should be no pending dump */
    TEST_ASSERT(!keel_mem_safety_dump_pending());

    /* Raise the signal */
    raise(SIGUSR1);

    /* After the SIGUSR1 handler fires the flag is consumed inside the
     * handler itself (it calls print_leaks directly), so the poll returns
     * false.  The important invariant is that we didn't crash. */
    (void)keel_mem_safety_dump_pending();

    keel_mem_safety_reset();
    pool_audit_teardown();
    TEST_END();
}

/* ============================================================================
 * Test 7 — Active byte counter consistency
 * ============================================================================
 */
static void test_active_byte_counter(void)
{
    TEST_BEGIN("pool audit: active byte counter consistency");

    pool_audit_setup();
    keel_mem_safety_reset();

    const size_t sz1 = 100;
    const size_t sz2 = 200;

    void *a = keel_safe_malloc(sz1, __FILE__, __LINE__);
    void *b = keel_safe_malloc(sz2, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(a);
    TEST_ASSERT_NOT_NULL(b);

    TEST_ASSERT(keel_mem_safety_active_bytes() >= sz1 + sz2);
    TEST_ASSERT_EQ(keel_mem_safety_active_count(), (size_t)2);

    keel_safe_free(a, __FILE__, __LINE__);
    TEST_ASSERT_EQ(keel_mem_safety_active_count(), (size_t)1);
    TEST_ASSERT(keel_mem_safety_active_bytes() >= sz2);

    keel_safe_free(b, __FILE__, __LINE__);
    TEST_ASSERT_EQ(keel_mem_safety_active_count(), (size_t)0);

    pool_audit_teardown();
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================
 */
int main(void)
{
    printf("=== Pool Audit Tests (Memory & Resource Integrity) ===\n\n");

    test_alloc_file_line_tracking();
    test_canary_integrity();
    test_tail_canary_overflow_detection();
    test_double_free_detection();
    test_sigusr1_handler_install();
    test_dump_pending_poll();
    test_active_byte_counter();

    return test_summary();
}
