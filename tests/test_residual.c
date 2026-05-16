/**
 * @file test_residual.c
 * @brief Unit tests for residual buffer (session.h, residual.c)
 *
 * Tests:
 * - Initialization and cleanup
 * - Inline buffer operations (small data)
 * - Chunk chain operations (large data)
 * - Append, consume, peek, linearize
 * - Edge cases and boundary conditions
 */

#include "test_utils.h"
#include "keel/session/session.h"
#include "keel/mem/mem.h"
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

/* ============================================================================
 * Residual Buffer Initialization Tests
 * ============================================================================ */

static void test_residual_init(void) {
    TEST_BEGIN("residual init");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Should start empty */
    TEST_ASSERT_EQ(keel_residual_len(&res), 0);
    TEST_ASSERT(keel_residual_empty(&res));
    
    keel_residual_clear(&res);
    
    TEST_END();
}

/* ============================================================================
 * Inline Buffer Tests (small data, no chunks)
 * ============================================================================ */

static void test_residual_inline_small(void) {
    TEST_BEGIN("residual inline small data");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Append small data (fits in inline buffer) */
    const char* data = "Hello, World!";
    size_t len = strlen(data);
    
    int rc = keel_residual_append(&res, data, len);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(keel_residual_len(&res), len);
    TEST_ASSERT(!keel_residual_empty(&res));
    
    /* Peek at data */
    size_t peek_len = 0;
    const void* peek_ptr = keel_residual_peek(&res, &peek_len);
    TEST_ASSERT_NOT_NULL(peek_ptr);
    TEST_ASSERT_EQ(peek_len, len);
    TEST_ASSERT(memcmp(peek_ptr, data, len) == 0);
    
    /* Consume data */
    char consume_buf[64];
    size_t consumed = keel_residual_consume(&res, consume_buf, len);
    TEST_ASSERT_EQ(consumed, len);
    TEST_ASSERT_EQ(keel_residual_len(&res), 0);
    TEST_ASSERT(keel_residual_empty(&res));
    
    keel_residual_clear(&res);
    
    TEST_END();
}

static void test_residual_inline_full(void) {
    TEST_BEGIN("residual inline buffer full");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Fill inline buffer exactly */
    char data[KEEL_RESIDUAL_INLINE_SIZE];
    memset(data, 'A', sizeof(data));
    
    int rc = keel_residual_append(&res, data, sizeof(data));
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(keel_residual_len(&res), sizeof(data));
    
    /* Peek to verify */
    size_t peek_len = 0;
    const void* peek_ptr = keel_residual_peek(&res, &peek_len);
    TEST_ASSERT_NOT_NULL(peek_ptr);
    TEST_ASSERT_EQ(peek_len, sizeof(data));
    TEST_ASSERT(memcmp(peek_ptr, data, sizeof(data)) == 0);
    
    keel_residual_clear(&res);
    
    TEST_END();
}

/* ============================================================================
 * Chunk Chain Tests (large data, overflow)
 * ============================================================================ */

static void test_residual_chunk_overflow(void) {
    TEST_BEGIN("residual chunk overflow");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Append more data than inline buffer can hold */
    size_t total_size = KEEL_RESIDUAL_INLINE_SIZE + 1024;
    char* data = (char*)malloc(total_size);
    TEST_ASSERT_NOT_NULL(data);
    
    /* Fill with pattern */
    for (size_t i = 0; i < total_size; i++) {
        data[i] = (char)(i & 0xFF);
    }
    
    int rc = keel_residual_append(&res, data, total_size);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(keel_residual_len(&res), total_size);
    
    /* Linearize into a buffer to get contiguous view */
    char* linear = (char*)malloc(total_size);
    TEST_ASSERT_NOT_NULL(linear);
    ssize_t linear_len = keel_residual_linearize(&res, linear, total_size);
    TEST_ASSERT_EQ((size_t)linear_len, total_size);
    TEST_ASSERT(memcmp(linear, data, total_size) == 0);
    
    free(linear);
    free(data);
    keel_residual_clear(&res);
    
    TEST_END();
}

static void test_residual_multiple_chunks(void) {
    TEST_BEGIN("residual multiple chunks");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Append data in multiple calls */
    const char* part1 = "First part of data";
    const char* part2 = " - Second part";
    const char* part3 = " - Third part ends here.";
    
    int rc = keel_residual_append(&res, part1, strlen(part1));
    TEST_ASSERT_EQ(rc, 0);
    
    rc = keel_residual_append(&res, part2, strlen(part2));
    TEST_ASSERT_EQ(rc, 0);
    
    rc = keel_residual_append(&res, part3, strlen(part3));
    TEST_ASSERT_EQ(rc, 0);
    
    size_t expected_len = strlen(part1) + strlen(part2) + strlen(part3);
    TEST_ASSERT_EQ(keel_residual_len(&res), expected_len);
    
    /* Build expected string */
    char expected[256];
    snprintf(expected, sizeof(expected), "%s%s%s", part1, part2, part3);
    
    /* Linearize into a buffer and compare */
    char linear[256];
    ssize_t linear_len = keel_residual_linearize(&res, linear, sizeof(linear));
    TEST_ASSERT_EQ((size_t)linear_len, expected_len);
    TEST_ASSERT(memcmp(linear, expected, expected_len) == 0);
    
    keel_residual_clear(&res);
    
    TEST_END();
}

/* ============================================================================
 * Partial Consume Tests
 * ============================================================================ */

static void test_residual_partial_consume(void) {
    TEST_BEGIN("residual partial consume");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    const char* data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    size_t len = strlen(data);
    
    keel_residual_append(&res, data, len);
    TEST_ASSERT_EQ(keel_residual_len(&res), len);
    
    /* Consume first 10 bytes */
    char consume_buf[32];
    size_t consumed = keel_residual_consume(&res, consume_buf, 10);
    TEST_ASSERT_EQ(consumed, 10);
    TEST_ASSERT_EQ(keel_residual_len(&res), len - 10);
    TEST_ASSERT(memcmp(consume_buf, data, 10) == 0);
    
    /* Peek remaining */
    size_t peek_len = 0;
    const void* peek_ptr = keel_residual_peek(&res, &peek_len);
    if (peek_ptr) {
        TEST_ASSERT_EQ(peek_len, len - 10);
        TEST_ASSERT(memcmp(peek_ptr, data + 10, len - 10) == 0);
    }
    
    /* Consume more */
    consumed = keel_residual_consume(&res, consume_buf, 8);
    TEST_ASSERT_EQ(consumed, 8);
    TEST_ASSERT_EQ(keel_residual_len(&res), len - 18);
    
    keel_residual_clear(&res);
    
    TEST_END();
}

/* ============================================================================
 * Edge Case Tests
 * ============================================================================ */

static void test_residual_edge_cases(void) {
    TEST_BEGIN("residual edge cases");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Empty buffer peek */
    size_t peek_len = 0;
    const void* peek_ptr = keel_residual_peek(&res, &peek_len);
    /* May return NULL or empty */
    (void)peek_ptr;
    
    /* Empty buffer consume (should be no-op) */
    size_t consumed = keel_residual_consume(&res, NULL, 0);
    TEST_ASSERT_EQ(consumed, 0);
    TEST_ASSERT(keel_residual_empty(&res));
    
    /* Append zero bytes */
    int rc = keel_residual_append(&res, "data", 0);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(keel_residual_empty(&res));
    
    /* Append and clear */
    keel_residual_append(&res, "test", 4);
    TEST_ASSERT_EQ(keel_residual_len(&res), 4);
    
    keel_residual_clear(&res);
    TEST_ASSERT(keel_residual_empty(&res));
    TEST_ASSERT_EQ(keel_residual_len(&res), 0);
    
    TEST_END();
}

static void test_residual_peek_partial(void) {
    TEST_BEGIN("residual peek partial");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    const char* data = "This is a longer message for testing partial peek";
    size_t len = strlen(data);
    
    keel_residual_append(&res, data, len);
    
    /* Peek returns pointer to all data */
    size_t peek_len = 0;
    const void* peek_ptr = keel_residual_peek(&res, &peek_len);
    TEST_ASSERT_NOT_NULL(peek_ptr);
    TEST_ASSERT_EQ(peek_len, len);
    
    /* Length should be unchanged */
    TEST_ASSERT_EQ(keel_residual_len(&res), len);
    
    keel_residual_clear(&res);
    
    TEST_END();
}

/* ============================================================================
 * Stress Tests
 * ============================================================================ */

static void test_residual_stress_append_consume(void) {
    TEST_BEGIN("residual stress append/consume");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Repeatedly append and consume in small increments */
    const char pattern[] = "01234567890123456789";  /* 20 bytes */
    char consume_buf[32];
    
    for (int i = 0; i < 100; i++) {
        /* Append pattern */
        int rc = keel_residual_append(&res, pattern, sizeof(pattern) - 1);
        TEST_ASSERT_EQ(rc, 0);
        
        /* Consume half */
        if (keel_residual_len(&res) >= 10) {
            keel_residual_consume(&res, consume_buf, 10);
        }
    }
    
    /* Should have accumulated data */
    TEST_ASSERT(!keel_residual_empty(&res));
    
    /* Clear all */
    keel_residual_clear(&res);
    TEST_ASSERT(keel_residual_empty(&res));
    
    TEST_END();
}

static void test_residual_large_buffer(void) {
    TEST_BEGIN("residual large buffer");
    
    keel_residual_t res;
    keel_residual_init(&res);
    
    /* Append 1MB of data */
    size_t size = 1024 * 1024;
    char* data = (char*)malloc(size);
    TEST_ASSERT_NOT_NULL(data);
    
    /* Fill with pattern */
    for (size_t i = 0; i < size; i++) {
        data[i] = (char)(i % 256);
    }
    
    int rc = keel_residual_append(&res, data, size);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(keel_residual_len(&res), size);
    
    /* Consume in chunks */
    size_t chunk = 4096;
    size_t consumed_total = 0;
    char* consume_buf = (char*)malloc(chunk);
    TEST_ASSERT_NOT_NULL(consume_buf);
    
    while (!keel_residual_empty(&res)) {
        size_t remaining = keel_residual_len(&res);
        size_t to_consume = (remaining < chunk) ? remaining : chunk;
        size_t consumed = keel_residual_consume(&res, consume_buf, to_consume);
        consumed_total += consumed;
    }
    
    TEST_ASSERT_EQ(consumed_total, size);
    TEST_ASSERT(keel_residual_empty(&res));
    
    free(consume_buf);
    free(data);
    keel_residual_clear(&res);
    
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("=== Residual Buffer Tests ===\n\n");
    
    /* Initialization */
    test_residual_init();
    
    /* Inline buffer */
    test_residual_inline_small();
    test_residual_inline_full();
    
    /* Chunk chain */
    test_residual_chunk_overflow();
    test_residual_multiple_chunks();
    
    /* Partial consume */
    test_residual_partial_consume();
    
    /* Edge cases */
    test_residual_edge_cases();
    test_residual_peek_partial();
    
    /* Stress tests */
    test_residual_stress_append_consume();
    test_residual_large_buffer();
    
    return test_summary();
}
