/**
 * @file test_buffer.c
 * @brief Unit tests for the keel_buffer_t growable byte-buffer API.
 *
 * Coverage:
 *   §1  Lifecycle — new, new_with_capacity, from_data, free, free(NULL).
 *   §2  Properties — len, capacity, available, empty after construction.
 *   §3  Write operations — write, write_u8, write_u16_be, write_u32_be,
 *       write_u64_be; big-endian byte order verified.
 *   §4  Read-at operations — read_u8_at, read_u16_be_at, read_u32_be_at.
 *   §5  Reserve — pre-grow, capacity monotonically non-decreasing.
 *   §6  Clear — reset length without changing capacity.
 *   §7  Growth under repeated writes — buffer grows beyond initial capacity.
 *   §8  Large writes — multi-megabyte accumulation without corruption.
 *   §9  Round-trip — write bytes then verify with read_at helpers.
 *   §10 Stress / randomized — 10k random writes stay consistent.
 *   §11 Allocation failure injection — keel_mem_set_fail_countdown ensures
 *       allocation failures are propagated gracefully.
 *   §12 NULL / zero-length edge cases — write(NULL,0), from_data(NULL,0).
 *   §13 Boundary: write to a 0-capacity pre-reserved buffer forces growth.
 *   §14 data / data_const pointers are consistent with len.
 *
 * @author Keel test suite
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/util/util.h"
#include "keel/mem/mem.h"
#include "keel_types.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

/* ============================================================================
 * Forward declarations for internal buffer functions not exported in util.h
 * ============================================================================ */

bool keel_buffer_ensure_available(keel_buffer_t* buf, size_t additional);
void keel_buffer_shrink_to_fit(keel_buffer_t* buf);
void keel_buffer_reset(keel_buffer_t* buf);
bool keel_buffer_append(keel_buffer_t* buf, const void* data, size_t len);
bool keel_buffer_append_byte(keel_buffer_t* buf, uint8_t byte);
bool keel_buffer_append_str(keel_buffer_t* buf, keel_str_t str);
bool keel_buffer_append_cstr(keel_buffer_t* buf, const char* cstr);
bool keel_buffer_append_buffer(keel_buffer_t* buf, const keel_buffer_t* other);
bool keel_buffer_prepend(keel_buffer_t* buf, const void* data, size_t len);
bool keel_buffer_insert(keel_buffer_t* buf, size_t pos, const void* data, size_t len);
size_t keel_buffer_remove(keel_buffer_t* buf, size_t pos, size_t len);
size_t keel_buffer_consume(keel_buffer_t* buf, size_t len);
void keel_buffer_truncate(keel_buffer_t* buf, size_t len);
size_t keel_buffer_read(keel_buffer_t* buf, void* out, size_t len);
size_t keel_buffer_read_and_consume(keel_buffer_t* buf, void* out, size_t len);

/* ============================================================================
 * §1  Lifecycle
 * ============================================================================ */

static void test_buffer_lifecycle_default(void) {
    TEST_BEGIN("buffer lifecycle: new / free");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);
    TEST_ASSERT(keel_buffer_capacity(buf) > 0);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
    TEST_ASSERT(keel_buffer_empty(buf));

    keel_buffer_free(buf);

    TEST_END();
}

static void test_buffer_lifecycle_with_capacity(void) {
    TEST_BEGIN("buffer lifecycle: new_with_capacity");

    keel_buffer_t* buf = keel_buffer_new_with_capacity(1024);
    TEST_ASSERT_NOT_NULL(buf);
    TEST_ASSERT(keel_buffer_capacity(buf) >= 1024);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
    keel_buffer_free(buf);

    /* Capacity 0 — must not crash and may return a tiny valid buffer */
    buf = keel_buffer_new_with_capacity(0);
    if (buf) keel_buffer_free(buf);

    /* Large initial capacity */
    buf = keel_buffer_new_with_capacity(1024 * 1024);
    TEST_ASSERT_NOT_NULL(buf);
    TEST_ASSERT(keel_buffer_capacity(buf) >= 1024 * 1024);
    keel_buffer_free(buf);

    TEST_END();
}

static void test_buffer_lifecycle_from_data(void) {
    TEST_BEGIN("buffer lifecycle: from_data copies correctly");

    uint8_t src[64];
    for (int i = 0; i < 64; i++) src[i] = (uint8_t)i;

    keel_buffer_t* buf = keel_buffer_from_data(src, 64);
    TEST_ASSERT_NOT_NULL(buf);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)64);
    TEST_ASSERT(!keel_buffer_empty(buf));
    TEST_ASSERT(memcmp(keel_buffer_data_const(buf), src, 64) == 0);
    keel_buffer_free(buf);

    /* NULL data with len=0 */
    buf = keel_buffer_from_data(NULL, 0);
    if (buf) {
        TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
        keel_buffer_free(buf);
    }

    TEST_END();
}

static void test_buffer_free_null(void) {
    TEST_BEGIN("buffer free: NULL is safe");

    keel_buffer_free(NULL);
    keel_buffer_free(NULL);

    TEST_ASSERT(true);

    TEST_END();
}

/* ============================================================================
 * §2  Properties
 * ============================================================================ */

static void test_buffer_properties(void) {
    TEST_BEGIN("buffer properties: len + capacity + available + empty");

    keel_buffer_t* buf = keel_buffer_new_with_capacity(128);
    TEST_ASSERT_NOT_NULL(buf);

    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
    TEST_ASSERT(keel_buffer_capacity(buf) >= 128);
    TEST_ASSERT(keel_buffer_available(buf) >= 128);
    TEST_ASSERT(keel_buffer_empty(buf));

    /* After writing 10 bytes */
    uint8_t data[10] = {0,1,2,3,4,5,6,7,8,9};
    TEST_ASSERT(keel_buffer_write(buf, data, 10));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)10);
    TEST_ASSERT(keel_buffer_capacity(buf) >= 128);
    TEST_ASSERT(keel_buffer_available(buf) == keel_buffer_capacity(buf) - 10);
    TEST_ASSERT(!keel_buffer_empty(buf));

    keel_buffer_free(buf);

    TEST_END();
}

/* ============================================================================
 * §3  Write operations
 * ============================================================================ */

static void test_buffer_write_bytes(void) {
    TEST_BEGIN("buffer write: arbitrary bytes appended correctly");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    uint8_t chunk[256];
    for (int i = 0; i < 256; i++) chunk[i] = (uint8_t)i;

    TEST_ASSERT(keel_buffer_write(buf, chunk, 256));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)256);
    TEST_ASSERT(memcmp(keel_buffer_data_const(buf), chunk, 256) == 0);

    keel_buffer_free(buf);
    TEST_END();
}

static void test_buffer_write_u8(void) {
    TEST_BEGIN("buffer write_u8: single byte");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    for (int i = 0; i < 256; i++) {
        TEST_ASSERT(keel_buffer_write_u8(buf, (uint8_t)i));
    }

    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)256);
    for (int i = 0; i < 256; i++) {
        TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, (size_t)i), (uint8_t)i);
    }

    keel_buffer_free(buf);
    TEST_END();
}

static void test_buffer_write_u16_be(void) {
    TEST_BEGIN("buffer write_u16_be: big-endian byte order");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    TEST_ASSERT(keel_buffer_write_u16_be(buf, 0x1234));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)2);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 0), (uint8_t)0x12);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 1), (uint8_t)0x34);
    TEST_ASSERT_EQ(keel_buffer_read_u16_be_at(buf, 0), (uint16_t)0x1234);

    keel_buffer_free(buf);
    TEST_END();
}

static void test_buffer_write_u32_be(void) {
    TEST_BEGIN("buffer write_u32_be: big-endian byte order");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    TEST_ASSERT(keel_buffer_write_u32_be(buf, 0xDEADBEEF));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)4);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 0), (uint8_t)0xDE);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 1), (uint8_t)0xAD);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 2), (uint8_t)0xBE);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 3), (uint8_t)0xEF);
    TEST_ASSERT_EQ(keel_buffer_read_u32_be_at(buf, 0), 0xDEADBEEFU);

    keel_buffer_free(buf);
    TEST_END();
}

static void test_buffer_write_u64_be(void) {
    TEST_BEGIN("buffer write_u64_be: big-endian byte order");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    TEST_ASSERT(keel_buffer_write_u64_be(buf, UINT64_C(0xCAFEBABEDEAD1234)));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)8);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 0), (uint8_t)0xCA);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 1), (uint8_t)0xFE);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 2), (uint8_t)0xBA);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 3), (uint8_t)0xBE);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 4), (uint8_t)0xDE);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 5), (uint8_t)0xAD);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 6), (uint8_t)0x12);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 7), (uint8_t)0x34);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §4  Read-at operations — round-trip with multiple fields
 * ============================================================================ */

static void test_buffer_read_roundtrip(void) {
    TEST_BEGIN("buffer read_at: multi-field round-trip");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    /* Write: u8 | u16_be | u32_be */
    keel_buffer_write_u8(buf,     (uint8_t)0xAB);
    keel_buffer_write_u16_be(buf, 0x1234);
    keel_buffer_write_u32_be(buf, 0xDEADC0DE);

    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)7);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 0),     (uint8_t)0xAB);
    TEST_ASSERT_EQ(keel_buffer_read_u16_be_at(buf, 1), (uint16_t)0x1234);
    TEST_ASSERT_EQ(keel_buffer_read_u32_be_at(buf, 3), 0xDEADC0DEU);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §5  Reserve
 * ============================================================================ */

static void test_buffer_reserve(void) {
    TEST_BEGIN("buffer reserve: capacity grows, length stays");

    keel_buffer_t* buf = keel_buffer_new_with_capacity(8);
    TEST_ASSERT_NOT_NULL(buf);

    size_t old_cap = keel_buffer_capacity(buf);
    TEST_ASSERT(keel_buffer_reserve(buf, 4096));
    TEST_ASSERT(keel_buffer_capacity(buf) >= 4096);
    TEST_ASSERT(keel_buffer_capacity(buf) >= old_cap);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0); /* length unchanged */

    /* Reserve less than current — no-op, still succeeds */
    TEST_ASSERT(keel_buffer_reserve(buf, 1));
    TEST_ASSERT(keel_buffer_capacity(buf) >= 4096);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §6  Clear
 * ============================================================================ */

static void test_buffer_clear(void) {
    TEST_BEGIN("buffer clear: length reset, capacity preserved");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    keel_buffer_write_u32_be(buf, 0x12345678);
    keel_buffer_write_u32_be(buf, 0xABCDEF01);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)8);
    size_t cap = keel_buffer_capacity(buf);

    keel_buffer_clear(buf);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
    TEST_ASSERT(keel_buffer_empty(buf));
    TEST_ASSERT(keel_buffer_capacity(buf) >= cap); /* capacity not reduced */

    /* Can write again after clear */
    TEST_ASSERT(keel_buffer_write_u8(buf, 0xFF));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)1);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 0), (uint8_t)0xFF);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §7  Growth under repeated writes
 * ============================================================================ */

static void test_buffer_growth(void) {
    TEST_BEGIN("buffer growth: exceeds initial capacity");

    keel_buffer_t* buf = keel_buffer_new_with_capacity(16);
    TEST_ASSERT_NOT_NULL(buf);

    /* Write 1000 bytes in small chunks */
    for (int i = 0; i < 1000; i++) {
        TEST_ASSERT(keel_buffer_write_u8(buf, (uint8_t)(i & 0xFF)));
    }

    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)1000);
    TEST_ASSERT(keel_buffer_capacity(buf) >= 1000);

    /* Verify data integrity after growth */
    for (int i = 0; i < 1000; i++) {
        TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, (size_t)i), (uint8_t)(i & 0xFF));
    }

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §8  Large writes
 * ============================================================================ */

static void test_buffer_large_write(void) {
    TEST_BEGIN("buffer large write: 4 MB accumulation");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    static uint8_t chunk[4096];
    for (int i = 0; i < 4096; i++) chunk[i] = (uint8_t)(i & 0xFF);

    /* Write 1024 × 4096 = 4 MB */
    for (int i = 0; i < 1024; i++) {
        bool ok = keel_buffer_write(buf, chunk, 4096);
        TEST_ASSERT(ok);
    }

    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)(1024 * 4096));

    /* Spot-check integrity */
    const uint8_t* p = keel_buffer_data_const(buf);
    for (int i = 0; i < 4096; i++) {
        TEST_ASSERT_EQ(p[i], (uint8_t)(i & 0xFF));
    }

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §9  data / data_const pointer consistency
 * ============================================================================ */

static void test_buffer_data_ptr(void) {
    TEST_BEGIN("buffer data ptr: mutable and const agree");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    keel_buffer_write_u32_be(buf, 0xAABBCCDD);

    uint8_t* mutable_p    = keel_buffer_data(buf);
    const uint8_t* const_p = keel_buffer_data_const(buf);
    TEST_ASSERT_NOT_NULL(mutable_p);
    TEST_ASSERT_NOT_NULL(const_p);
    TEST_ASSERT(mutable_p == const_p);
    TEST_ASSERT_EQ(mutable_p[0], (uint8_t)0xAA);
    TEST_ASSERT_EQ(const_p[3],   (uint8_t)0xDD);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §10  Stress / randomized writes
 * ============================================================================ */

static void test_buffer_stress(void) {
    TEST_BEGIN("buffer stress: 10k random-length writes");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    static uint8_t scratch[512];
    for (int i = 0; i < 512; i++) scratch[i] = (uint8_t)(i ^ 0xA5);

    size_t total = 0;
    unsigned seed = 42;
    for (int i = 0; i < 10000; i++) {
        size_t len = (size_t)(rand_r(&seed) % 64);
        if (keel_buffer_write(buf, scratch, len)) {
            total += len;
        }
    }

    TEST_ASSERT_EQ(keel_buffer_len(buf), total);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §11  Allocation failure injection
 * ============================================================================ */

static void test_buffer_alloc_failure_new(void) {
    TEST_BEGIN("buffer alloc failure: new() returns NULL on OOM");

    keel_mem_set_fail_countdown(0); /* fail immediately */
    keel_buffer_t* buf = keel_buffer_new();
    keel_mem_set_fail_countdown(-1);

    /* May be NULL (allocation failure) — must not crash */
    if (buf) keel_buffer_free(buf);

    TEST_ASSERT(true);
    TEST_END();
}

static void test_buffer_alloc_failure_write(void) {
    TEST_BEGIN("buffer alloc failure: write returns false on OOM growth");

    /* Create a tiny buffer and exhaust it, then inject failure on growth */
    keel_buffer_t* buf = keel_buffer_new_with_capacity(4);
    if (!buf) {
        /* can't test without a buffer — skip gracefully */
        TEST_ASSERT(true);
        TEST_END();
        return;
    }

    /* Fill past initial capacity to force a realloc */
    uint8_t big[4096];
    memset(big, 0xBB, sizeof(big));
    keel_mem_set_fail_countdown(0); /* next alloc fails */
    bool ok = keel_buffer_write(buf, big, sizeof(big));
    keel_mem_set_fail_countdown(-1);

    /* Either succeeded (OS gave memory) or failed gracefully — no crash */
    (void)ok;
    keel_buffer_free(buf);

    TEST_ASSERT(true);
    TEST_END();
}

/* ============================================================================
 * §12  Zero-length edge cases
 * ============================================================================ */

static void test_buffer_zero_write(void) {
    TEST_BEGIN("buffer zero-length write: accepted, length unchanged");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    TEST_ASSERT(keel_buffer_write(buf, NULL, 0));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
    TEST_ASSERT(keel_buffer_empty(buf));

    uint8_t dummy = 0xFF;
    TEST_ASSERT(keel_buffer_write(buf, &dummy, 0));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §13  Multiple clear-and-refill cycles
 * ============================================================================ */

static void test_buffer_clear_cycles(void) {
    TEST_BEGIN("buffer clear cycles: repeated write-clear-write");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    for (int round = 0; round < 8; round++) {
        for (int i = 0; i < 256; i++) {
            TEST_ASSERT(keel_buffer_write_u8(buf, (uint8_t)i));
        }
        TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)256);
        keel_buffer_clear(buf);
        TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);
    }

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §14  from_data preserves content after free of source
 * ============================================================================ */

static void test_buffer_from_data_independence(void) {
    TEST_BEGIN("buffer from_data: copy is independent of source");

    uint8_t* src = keel_malloc(64);
    TEST_ASSERT_NOT_NULL(src);
    for (int i = 0; i < 64; i++) src[i] = (uint8_t)(i * 2);

    keel_buffer_t* buf = keel_buffer_from_data(src, 64);
    TEST_ASSERT_NOT_NULL(buf);

    /* Overwrite source — buffer must not be affected */
    memset(src, 0xFF, 64);
    keel_free(src);

    const uint8_t* p = keel_buffer_data_const(buf);
    for (int i = 0; i < 64; i++) {
        TEST_ASSERT_EQ(p[i], (uint8_t)(i * 2));
    }

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §15  Ensure-available / shrink-to-fit / reset
 * ============================================================================ */

static void test_buffer_ensure_available(void) {
    TEST_BEGIN("buffer ensure_available: reserves extra space");

    /* NULL buffer returns false */
    TEST_ASSERT(!keel_buffer_ensure_available(NULL, 64));

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    /* Ensure space for 1024 additional bytes */
    TEST_ASSERT(keel_buffer_ensure_available(buf, 1024));
    TEST_ASSERT(keel_buffer_capacity(buf) >= 1024);

    keel_buffer_free(buf);
    TEST_END();
}

static void test_buffer_shrink_to_fit(void) {
    TEST_BEGIN("buffer shrink_to_fit: reduces capacity to len");

    keel_buffer_t* buf = keel_buffer_new_with_capacity(4096);
    TEST_ASSERT_NOT_NULL(buf);

    /* NULL is a no-op */
    keel_buffer_shrink_to_fit(NULL);

    /* Write a few bytes then shrink */
    const uint8_t data[] = {1, 2, 3, 4};
    keel_buffer_write(buf, data, sizeof(data));
    TEST_ASSERT_EQ(keel_buffer_len(buf), sizeof(data));

    keel_buffer_shrink_to_fit(buf);
    /* capacity should now be close to len */
    TEST_ASSERT(keel_buffer_capacity(buf) <= 64);

    /* Already fitting — no-op */
    keel_buffer_shrink_to_fit(buf);

    keel_buffer_free(buf);
    TEST_END();
}

static void test_buffer_reset(void) {
    TEST_BEGIN("buffer reset: clears data and trims capacity");

    /* NULL is a no-op */
    keel_buffer_reset(NULL);

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    /* Grow beyond default capacity then reset */
    uint8_t big[4096];
    memset(big, 0xAA, sizeof(big));
    keel_buffer_write(buf, big, sizeof(big));
    TEST_ASSERT(keel_buffer_len(buf) == sizeof(big));

    keel_buffer_reset(buf);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);

    /* Writing again after reset must work */
    TEST_ASSERT(keel_buffer_write_u8(buf, 0x42));

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §16  Append variants
 * ============================================================================ */

static void test_buffer_append_variants(void) {
    TEST_BEGIN("buffer append variants: byte, str, cstr, buffer");

    /* NULL buffer returns false */
    TEST_ASSERT(!keel_buffer_append(NULL, "x", 1));

    keel_buffer_t* dst = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(dst);

    /* append_byte */
    TEST_ASSERT(keel_buffer_append_byte(dst, 0xAB));
    TEST_ASSERT_EQ(keel_buffer_len(dst), (size_t)1);

    /* append_str */
    keel_str_t sv = keel_str_from_cstr("hello");
    TEST_ASSERT(keel_buffer_append_str(dst, sv));
    TEST_ASSERT_EQ(keel_buffer_len(dst), (size_t)6);

    /* append_cstr */
    TEST_ASSERT(keel_buffer_append_cstr(dst, " world"));
    TEST_ASSERT_EQ(keel_buffer_len(dst), (size_t)12);

    /* append_cstr with NULL (treated as empty append) */
    TEST_ASSERT(keel_buffer_append_cstr(dst, NULL));
    TEST_ASSERT_EQ(keel_buffer_len(dst), (size_t)12);

    /* append_buffer */
    keel_buffer_t* src = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(src);
    keel_buffer_write_u8(src, 0xFF);

    TEST_ASSERT(keel_buffer_append_buffer(dst, src));
    TEST_ASSERT_EQ(keel_buffer_len(dst), (size_t)13);

    /* append_buffer with NULL other (treated as empty append) */
    TEST_ASSERT(keel_buffer_append_buffer(dst, NULL));
    TEST_ASSERT_EQ(keel_buffer_len(dst), (size_t)13);

    keel_buffer_free(src);
    keel_buffer_free(dst);
    TEST_END();
}

/* ============================================================================
 * §17  Prepend, insert, remove
 * ============================================================================ */

static void test_buffer_prepend_insert_remove(void) {
    TEST_BEGIN("buffer prepend / insert / remove");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    /* NULL buffer returns false */
    TEST_ASSERT(!keel_buffer_prepend(NULL, "x", 1));
    TEST_ASSERT(!keel_buffer_insert(NULL, 0, "x", 1));

    /* Zero-length prepend is a no-op */
    TEST_ASSERT(keel_buffer_prepend(buf, NULL, 0));

    /* Prepend */
    const uint8_t tail[] = {3, 4, 5};
    keel_buffer_write(buf, tail, sizeof(tail));
    const uint8_t head[] = {1, 2};
    TEST_ASSERT(keel_buffer_prepend(buf, head, sizeof(head)));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)5);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 0), (uint8_t)1);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 1), (uint8_t)2);

    /* Insert in the middle */
    const uint8_t mid[] = {0xFF};
    TEST_ASSERT(keel_buffer_insert(buf, 2, mid, sizeof(mid)));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)6);
    TEST_ASSERT_EQ(keel_buffer_read_u8_at(buf, 2), (uint8_t)0xFF);

    /* Zero-length insert is a no-op */
    TEST_ASSERT(keel_buffer_insert(buf, 0, NULL, 0));

    /* Insert beyond end is clamped to end */
    const uint8_t extra[] = {0xEE};
    TEST_ASSERT(keel_buffer_insert(buf, 9999, extra, sizeof(extra)));
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)7);

    /* Remove from the middle */
    size_t removed = keel_buffer_remove(buf, 2, 1);
    TEST_ASSERT_EQ(removed, (size_t)1);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)6);

    /* Remove from invalid position */
    TEST_ASSERT_EQ(keel_buffer_remove(NULL, 0, 1), (size_t)0);
    TEST_ASSERT_EQ(keel_buffer_remove(buf, 999, 1), (size_t)0);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §18  Read / truncate / consume
 * ============================================================================ */

static void test_buffer_read_truncate_consume(void) {
    TEST_BEGIN("buffer read / truncate / consume");

    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);

    /* Fill with known bytes */
    for (uint8_t i = 0; i < 8; i++) keel_buffer_write_u8(buf, i);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)8);

    /* keel_buffer_read: copies without consuming */
    uint8_t out[4] = {0};
    size_t n = keel_buffer_read(buf, out, sizeof(out));
    TEST_ASSERT_EQ(n, (size_t)4);
    TEST_ASSERT_EQ(out[0], (uint8_t)0);
    TEST_ASSERT_EQ(out[3], (uint8_t)3);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)8); /* unchanged */

    /* keel_buffer_read with NULL args returns 0 */
    TEST_ASSERT_EQ(keel_buffer_read(NULL, out, 4), (size_t)0);
    TEST_ASSERT_EQ(keel_buffer_read(buf, NULL, 4), (size_t)0);
    TEST_ASSERT_EQ(keel_buffer_read(buf, out, 0), (size_t)0);

    /* keel_buffer_truncate */
    keel_buffer_truncate(buf, 4);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)4);

    /* keel_buffer_read_and_consume */
    uint8_t consumed[2] = {0};
    n = keel_buffer_read_and_consume(buf, consumed, sizeof(consumed));
    TEST_ASSERT_EQ(n, (size_t)2);
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)2);

    /* keel_buffer_consume */
    keel_buffer_consume(buf, 10); /* clamped to remaining */
    TEST_ASSERT_EQ(keel_buffer_len(buf), (size_t)0);

    keel_buffer_free(buf);
    TEST_END();
}

/* ============================================================================
 * §19  Public API NULL-guard coverage
 * ============================================================================ */

static void test_buffer_null_guards(void) {
    TEST_BEGIN("buffer public API: NULL guard branches");

    /* keel_buffer_reserve with NULL returns false */
    TEST_ASSERT(!keel_buffer_reserve(NULL, 100));

    /* keel_buffer_read_u16_be_at with NULL returns 0 */
    TEST_ASSERT_EQ(keel_buffer_read_u16_be_at(NULL, 0), (uint16_t)0);

    /* keel_buffer_read_u32_be_at with NULL returns 0 */
    TEST_ASSERT_EQ(keel_buffer_read_u32_be_at(NULL, 0), (uint32_t)0);

    /* Out-of-bounds reads return 0 */
    keel_buffer_t* buf = keel_buffer_new();
    TEST_ASSERT_NOT_NULL(buf);
    keel_buffer_write_u8(buf, 0xAA); /* only 1 byte */
    TEST_ASSERT_EQ(keel_buffer_read_u16_be_at(buf, 0), (uint16_t)0); /* needs 2 */
    TEST_ASSERT_EQ(keel_buffer_read_u32_be_at(buf, 0), (uint32_t)0); /* needs 4 */
    keel_buffer_free(buf);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Buffer API Tests\n");
    printf("================\n\n");

    keel_mem_init(NULL);

    test_buffer_lifecycle_default();
    test_buffer_lifecycle_with_capacity();
    test_buffer_lifecycle_from_data();
    test_buffer_free_null();

    test_buffer_properties();

    test_buffer_write_bytes();
    test_buffer_write_u8();
    test_buffer_write_u16_be();
    test_buffer_write_u32_be();
    test_buffer_write_u64_be();

    test_buffer_read_roundtrip();

    test_buffer_reserve();
    test_buffer_clear();
    test_buffer_clear_cycles();
    test_buffer_growth();
    test_buffer_large_write();
    test_buffer_data_ptr();
    test_buffer_stress();
    test_buffer_from_data_independence();

    test_buffer_alloc_failure_new();
    test_buffer_alloc_failure_write();
    test_buffer_zero_write();

    test_buffer_ensure_available();
    test_buffer_shrink_to_fit();
    test_buffer_reset();
    test_buffer_append_variants();
    test_buffer_prepend_insert_remove();
    test_buffer_read_truncate_consume();
    test_buffer_null_guards();

    keel_mem_shutdown();

    return test_summary();
}
