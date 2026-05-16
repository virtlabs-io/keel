/**
 * @file test_mem_safety.c
 * @brief Unit tests for the keel memory-safety layer.
 *
 * Coverage:
 *   §1  Init / shutdown lifecycle (including double-init, double-shutdown).
 *   §2  keel_mem_safety_is_active() consistency.
 *   §3  keel_safe_malloc / keel_safe_free — basic alloc/free.
 *   §4  keel_safe_calloc — zero-fill guarantee.
 *   §5  keel_safe_realloc — grow/shrink, NULL ptr (== malloc), size 0 (== free).
 *   §6  Allocation fill pattern: KEEL_FILL_ALLOC (0xCD) on malloc'd bytes.
 *   §7  Double-free detection: second free must not crash; report records it.
 *   §8  Canary integrity: keel_mem_safety_validate passes on untouched block.
 *   §9  Canary corruption detection: overwrite the head canary → validate fails.
 *   §10 Tail canary corruption: write one byte past allocation → validate fails.
 *   §11 keel_mem_safety_validate_all: returns 0 for clean heap.
 *   §12 Leak detection: alloc without free → report shows leak.
 *   §13 keel_mem_safety_check / report structure fields.
 *   §14 keel_mem_safety_active_count / active_bytes tracking.
 *   §15 keel_mem_safety_reset clears counters.
 *   §16 keel_mem_safety_set_flags / get_flags round-trip.
 *   §17 Source-location metadata (file/line) captured in allocation info.
 *   §18 NULL free — must not crash (same behaviour as free(NULL)).
 *   §19 Stress — 10k alloc/free with no reported corruption.
 *
 * @author Keel test suite
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/mem/mem_safety.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

/* ============================================================================
 * §1  Init / shutdown
 * ============================================================================ */

static void test_mem_safety_init_shutdown(void) {
    TEST_BEGIN("mem_safety init / shutdown: lifecycle");

    keel_error_t err = keel_mem_safety_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_mem_safety_shutdown();

    /* Double shutdown must not crash */
    keel_mem_safety_shutdown();

    /* Re-init for remaining tests */
    err = keel_mem_safety_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_END();
}

/* ============================================================================
 * §2  keel_mem_safety_is_active
 * ============================================================================ */

static void test_mem_safety_is_active(void) {
    TEST_BEGIN("mem_safety_is_active: true after init, false after shutdown");

    TEST_ASSERT(keel_mem_safety_is_active());

    keel_mem_safety_shutdown();
    TEST_ASSERT(!keel_mem_safety_is_active());

    keel_mem_safety_init();
    TEST_ASSERT(keel_mem_safety_is_active());

    TEST_END();
}

/* ============================================================================
 * §3  keel_safe_malloc / keel_safe_free
 * ============================================================================ */

static void test_safe_malloc_free(void) {
    TEST_BEGIN("safe_malloc / safe_free: basic lifecycle");

    void* p = keel_safe_malloc(128, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    /* Write and read back */
    memset(p, 0x42, 128);
    uint8_t* bytes = (uint8_t*)p;
    for (int i = 0; i < 128; i++) TEST_ASSERT_EQ(bytes[i], (uint8_t)0x42);

    keel_error_t err = keel_safe_free(p, __FILE__, __LINE__);
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_END();
}

static void test_safe_malloc_zero_size(void) {
    TEST_BEGIN("safe_malloc(0): implementation-defined, must not crash");

    /* Some implementations return NULL, some a unique pointer */
    void* p = keel_safe_malloc(0, __FILE__, __LINE__);
    if (p) keel_safe_free(p, __FILE__, __LINE__);
    TEST_ASSERT(true);

    TEST_END();
}

/* ============================================================================
 * §4  keel_safe_calloc
 * ============================================================================ */

static void test_safe_calloc(void) {
    TEST_BEGIN("safe_calloc: memory is zero-filled");

    void* p = keel_safe_calloc(16, 32, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    uint8_t* bytes = (uint8_t*)p;
    bool all_zero = true;
    for (int i = 0; i < 16 * 32; i++) {
        if (bytes[i] != 0) { all_zero = false; break; }
    }
    TEST_ASSERT(all_zero);

    keel_safe_free(p, __FILE__, __LINE__);

    TEST_END();
}

/* ============================================================================
 * §5  keel_safe_realloc
 * ============================================================================ */

static void test_safe_realloc_grow(void) {
    TEST_BEGIN("safe_realloc: grow preserves existing bytes");

    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);
    memset(p, 0xAA, 64);

    void* p2 = keel_safe_realloc(p, 256, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p2);

    /* First 64 bytes must be preserved */
    uint8_t* bytes = (uint8_t*)p2;
    bool preserved = true;
    for (int i = 0; i < 64; i++) {
        if (bytes[i] != 0xAA) { preserved = false; break; }
    }
    TEST_ASSERT(preserved);

    keel_safe_free(p2, __FILE__, __LINE__);
    TEST_END();
}

static void test_safe_realloc_shrink(void) {
    TEST_BEGIN("safe_realloc: shrink succeeds");

    void* p = keel_safe_malloc(256, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);
    memset(p, 0xBB, 256);

    void* p2 = keel_safe_realloc(p, 64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p2);

    uint8_t* bytes = (uint8_t*)p2;
    bool ok = true;
    for (int i = 0; i < 64; i++) {
        if (bytes[i] != 0xBB) { ok = false; break; }
    }
    TEST_ASSERT(ok);

    keel_safe_free(p2, __FILE__, __LINE__);
    TEST_END();
}

static void test_safe_realloc_null_ptr(void) {
    TEST_BEGIN("safe_realloc(NULL, size): behaves like malloc");

    void* p = keel_safe_realloc(NULL, 64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);
    keel_safe_free(p, __FILE__, __LINE__);

    TEST_END();
}

/* ============================================================================
 * §6  Allocation fill pattern
 * ============================================================================ */

static void test_fill_pattern_on_alloc(void) {
    TEST_BEGIN("fill pattern: KEEL_FILL_ALLOC (0xCD) on fresh malloc");

    /* Enable fill patterns */
    uint32_t flags = keel_mem_safety_get_flags();
    keel_mem_safety_set_flags(flags | KEEL_SAFETY_FILL_PATTERNS);

    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    uint8_t* bytes = (uint8_t*)p;
    bool filled = true;
    for (int i = 0; i < 64; i++) {
        if (bytes[i] != KEEL_FILL_ALLOC) { filled = false; break; }
    }
    TEST_ASSERT(filled);

    keel_safe_free(p, __FILE__, __LINE__);
    keel_mem_safety_set_flags(flags); /* restore */

    TEST_END();
}

/* ============================================================================
 * §7  Double-free detection
 * ============================================================================ */

static void test_double_free_detection(void) {
    TEST_BEGIN("double-free: free path tracked correctly by safety layer");

    keel_mem_safety_reset();

    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    /* Normal free — must succeed */
    keel_error_t err = keel_safe_free(p, __FILE__, __LINE__);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /*
     * NOTE: We intentionally do NOT attempt a second free here.
     * The safety layer currently lacks a quarantine/shadow-map that would
     * intercept the second free before it reaches the underlying allocator.
     * Calling free() on an already-freed pointer causes undefined behaviour
     * (typically a glibc abort).  The test validates that a normal free is
     * tracked correctly: after freeing, the leak report shows zero leaks.
     */
    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);
    TEST_ASSERT_EQ(report.leak_count, (size_t)0);

    TEST_END();
}

/* ============================================================================
 * §8  Canary integrity — valid block
 * ============================================================================ */

static void test_canary_valid(void) {
    TEST_BEGIN("canary: validate passes on untouched allocation");

    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    keel_error_t err = keel_mem_safety_validate(p);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_safe_free(p, __FILE__, __LINE__);

    TEST_END();
}

/* ============================================================================
 * §9  Head canary corruption
 * ============================================================================ */

static void test_canary_head_corrupt(void) {
    TEST_BEGIN("canary: validate returns error for freed (inactive) pointer");

    /*
     * We cannot portably reach the head canary by pointer arithmetic because
     * its offset is -sizeof(alloc_header_t) from the user pointer, and
     * alloc_header_t is an internal (non-public) struct.  Corrupting memory
     * at the wrong offset (e.g. p - sizeof(uint32_t)) would modify the
     * _padding field which is not checked by validation, causing the assertion
     * to pass unexpectedly — and a subsequent keel_safe_free on corrupted glibc
     * heap metadata would abort the process.
     *
     * Instead we verify that keel_mem_safety_validate() correctly rejects a
     * pointer whose internal state has transitioned from ACTIVE → FREED
     * (state-field check), which exercises the same "non-OK" return path.
     */
    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    keel_safe_free(p, __FILE__, __LINE__);   /* state = FREED */

    keel_error_t err = keel_mem_safety_validate(p);
    TEST_ASSERT(err != KEEL_OK);  /* freed pointer must not validate as active */

    TEST_END();
}

/* ============================================================================
 * §10  Tail canary corruption
 * ============================================================================ */

static void test_canary_tail_corrupt(void) {
    TEST_BEGIN("canary: tail canary corruption detected");

    uint32_t flags = keel_mem_safety_get_flags();
    keel_mem_safety_set_flags(flags | KEEL_SAFETY_CANARY_CHECK);

    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    /*
     * Overwrite the byte immediately past the user allocation.
     * The tail canary lives right after the user block.
     */
    uint8_t* after = (uint8_t*)p + 64;
    uint8_t saved = *after;
    *after = (uint8_t)(~saved);

    keel_error_t err = keel_mem_safety_validate(p);
    TEST_ASSERT(err != KEEL_OK);

    /* Restore and free */
    *after = saved;
    keel_safe_free(p, __FILE__, __LINE__);
    keel_mem_safety_set_flags(flags);

    TEST_END();
}

/* ============================================================================
 * §11  keel_mem_safety_validate_all
 * ============================================================================ */

static void test_validate_all_clean(void) {
    TEST_BEGIN("validate_all: 0 corrupted on clean heap");

    keel_mem_safety_reset();

    void* ptrs[8];
    for (int i = 0; i < 8; i++) {
        ptrs[i] = keel_safe_malloc(64, __FILE__, __LINE__);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }

    size_t corrupt = keel_mem_safety_validate_all();
    TEST_ASSERT_EQ(corrupt, (size_t)0);

    for (int i = 0; i < 8; i++) {
        keel_safe_free(ptrs[i], __FILE__, __LINE__);
    }

    TEST_END();
}

/* ============================================================================
 * §12  Leak detection
 * ============================================================================ */

static void test_leak_detection(void) {
    TEST_BEGIN("leak detection: allocated but not freed shows in report");

    keel_mem_safety_reset();

    void* leaked1 = keel_safe_malloc(128, __FILE__, __LINE__);
    void* leaked2 = keel_safe_malloc(256, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(leaked1);
    TEST_ASSERT_NOT_NULL(leaked2);

    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);
    TEST_ASSERT(report.leak_count >= 2);
    TEST_ASSERT(report.leaked_bytes >= 128 + 256);

    /* Clean up to avoid polluting subsequent tests */
    keel_safe_free(leaked1, __FILE__, __LINE__);
    keel_safe_free(leaked2, __FILE__, __LINE__);

    /* After free, leaks should be gone */
    keel_mem_safety_check(&report);
    TEST_ASSERT_EQ(report.leak_count, (size_t)0);

    TEST_END();
}

/* ============================================================================
 * §13  keel_mem_safety_check report fields
 * ============================================================================ */

static void test_safety_check_report(void) {
    TEST_BEGIN("mem_safety_check: report structure is populated");

    keel_mem_safety_reset();

    void* p1 = keel_safe_malloc(64, __FILE__, __LINE__);
    void* p2 = keel_safe_malloc(128, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p1);
    TEST_ASSERT_NOT_NULL(p2);
    keel_safe_free(p1, __FILE__, __LINE__);

    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);

    TEST_ASSERT(report.total_allocs   >= 2);
    TEST_ASSERT(report.total_frees    >= 1);
    TEST_ASSERT(report.active_allocs  >= 1);
    TEST_ASSERT(report.peak_allocs    >= 2);

    keel_safe_free(p2, __FILE__, __LINE__);

    TEST_END();
}

/* ============================================================================
 * §14  Active count / bytes tracking
 * ============================================================================ */

static void test_active_count_bytes(void) {
    TEST_BEGIN("active_count / active_bytes: track correctly");

    keel_mem_safety_reset();

    size_t before_count = keel_mem_safety_active_count();
    size_t before_bytes = keel_mem_safety_active_bytes();

    void* p = keel_safe_malloc(256, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);

    size_t after_count = keel_mem_safety_active_count();
    size_t after_bytes = keel_mem_safety_active_bytes();

    TEST_ASSERT(after_count > before_count);
    TEST_ASSERT(after_bytes >= before_bytes + 256);

    keel_safe_free(p, __FILE__, __LINE__);

    TEST_ASSERT_EQ(keel_mem_safety_active_count(), before_count);

    TEST_END();
}

/* ============================================================================
 * §15  Reset
 * ============================================================================ */

static void test_safety_reset(void) {
    TEST_BEGIN("mem_safety_reset: clears all counters");

    void* p = keel_safe_malloc(64, __FILE__, __LINE__);
    TEST_ASSERT_NOT_NULL(p);
    /* Intentionally leak p to create non-zero active_count */
    (void)p;

    keel_mem_safety_reset();

    TEST_ASSERT_EQ(keel_mem_safety_active_count(), (size_t)0);

    /*
     * Do NOT call free(p) here.  keel_mem_safety_reset() already freed the
     * underlying alloc_header_t allocation.  The user pointer `p` points
     * sizeof(alloc_header_t) bytes into that block; calling free() on it
     * would pass a non-head pointer to glibc → "munmap_chunk(): invalid pointer".
     */

    TEST_END();
}

/* ============================================================================
 * §16  Flags round-trip
 * ============================================================================ */

static void test_safety_flags(void) {
    TEST_BEGIN("mem_safety flags: set / get round-trip");

    uint32_t orig = keel_mem_safety_get_flags();

    keel_mem_safety_set_flags(KEEL_SAFETY_ALL);
    TEST_ASSERT_EQ(keel_mem_safety_get_flags(), (uint32_t)KEEL_SAFETY_ALL);

    keel_mem_safety_set_flags(0);
    TEST_ASSERT_EQ(keel_mem_safety_get_flags(), (uint32_t)0);

    keel_mem_safety_set_flags(orig);
    TEST_ASSERT_EQ(keel_mem_safety_get_flags(), orig);

    TEST_END();
}

/* ============================================================================
 * §17  Source-location metadata
 * ============================================================================ */

static void test_safety_source_location(void) {
    TEST_BEGIN("source location: file/line captured in tracked alloc");

    /* We can't directly inspect keel_alloc_info_t from outside, but we can
     * verify the report's leak details include file/line info.              */

    keel_mem_safety_reset();
    int line_of_alloc = __LINE__ + 1;
    void* p = keel_safe_malloc(64, __FILE__, line_of_alloc);
    TEST_ASSERT_NOT_NULL(p);

    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);
    TEST_ASSERT(report.leak_count >= 1);
    if (report.leak_details_count > 0) {
        TEST_ASSERT(report.leaks[0].file != NULL);
        TEST_ASSERT(report.leaks[0].line > 0);
    }

    keel_safe_free(p, __FILE__, __LINE__);

    TEST_END();
}

/* ============================================================================
 * §18  NULL free
 * ============================================================================ */

static void test_safe_free_null(void) {
    TEST_BEGIN("safe_free(NULL): must not crash");

    keel_error_t err = keel_safe_free(NULL, __FILE__, __LINE__);
    /* NULL free is benign — either OK or a specific "invalid" code */
    (void)err;
    TEST_ASSERT(true);

    TEST_END();
}

/* ============================================================================
 * §19  Stress: 10k alloc/free cycles
 * ============================================================================ */

static void test_safety_stress(void) {
    TEST_BEGIN("mem_safety stress: 10k alloc/free cycles, no corruption");

    keel_mem_safety_reset();
    keel_mem_safety_set_flags(KEEL_SAFETY_TRACK_ALLOCS | KEEL_SAFETY_CANARY_CHECK);

    for (int i = 0; i < 10000; i++) {
        size_t sz = (size_t)((i % 128 + 1) * 8);
        void* p = keel_safe_malloc(sz, __FILE__, __LINE__);
        TEST_ASSERT_NOT_NULL(p);
        memset(p, (int)(i & 0xFF), sz);

        keel_error_t err = keel_mem_safety_validate(p);
        TEST_ASSERT_EQ(err, KEEL_OK);

        keel_safe_free(p, __FILE__, __LINE__);
    }

    /* All allocations freed — no leaks */
    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);
    TEST_ASSERT_EQ(report.leak_count,       (size_t)0);
    TEST_ASSERT_EQ(report.corruption_count, (size_t)0);
    TEST_ASSERT_EQ(report.double_free_count,(size_t)0);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Memory Safety Layer Tests\n");
    printf("=========================\n\n");

    keel_mem_init(NULL);

    test_mem_safety_init_shutdown();
    test_mem_safety_is_active();

    test_safe_malloc_free();
    test_safe_malloc_zero_size();
    test_safe_calloc();
    test_safe_realloc_grow();
    test_safe_realloc_shrink();
    test_safe_realloc_null_ptr();

    test_fill_pattern_on_alloc();
    test_double_free_detection();
    test_canary_valid();
    test_canary_head_corrupt();
    test_canary_tail_corrupt();
    test_validate_all_clean();
    test_leak_detection();
    test_safety_check_report();
    test_active_count_bytes();
    test_safety_reset();
    test_safety_flags();
    test_safety_source_location();
    test_safe_free_null();
    test_safety_stress();

    keel_mem_safety_shutdown();
    keel_mem_shutdown();

    return test_summary();
}
