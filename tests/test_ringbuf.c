/**
 * @file test_ringbuf.c
 * @brief Unit tests for SPSC, MPSC, and byte-level ring buffer primitives.
 *
 * Ring buffers are the primary internal transport for migration messages and
 * inter-worker coordination. This suite checks lifecycle, push/pop semantics,
 * fill-and-drain boundary behavior, zero-copy slot access, and the byte-oriented
 * variant used for streaming data. The SPSC and MPSC structures are exercised
 * single-threaded here to validate correctness before the concurrency stress
 * suite applies hostile scheduling.
 */

#include "test_utils.h"
#include "keel/mem/ringbuf.h"

#include <stdio.h>
#include <string.h>

/* ============================================================================
 * SPSC Ring Buffer Tests
 * ============================================================================ */

static void test_spsc_create_destroy(void) {
    printf("  Testing SPSC create/destroy...\n");
    
    keel_spsc_ringbuf_t* rb = keel_spsc_ringbuf_create(16, sizeof(int));
    TEST_ASSERT(rb != NULL);
    
    /* Newly created should be empty */
    TEST_ASSERT(keel_spsc_ringbuf_is_empty(rb));
    TEST_ASSERT(!keel_spsc_ringbuf_is_full(rb));
    TEST_ASSERT(keel_spsc_ringbuf_size(rb) == 0);
    
    keel_spsc_ringbuf_destroy(rb);
    
    /* NULL should not crash */
    keel_spsc_ringbuf_destroy(NULL);
    
    printf("    PASSED\n");
}

static void test_spsc_push_pop(void) {
    printf("  Testing SPSC push/pop...\n");
    
    keel_spsc_ringbuf_t* rb = keel_spsc_ringbuf_create(8, sizeof(int));
    TEST_ASSERT(rb != NULL);
    
    int value = 42;
    
    /* Push value */
    TEST_ASSERT(keel_spsc_ringbuf_try_push(rb, &value));
    TEST_ASSERT(!keel_spsc_ringbuf_is_empty(rb));
    TEST_ASSERT(keel_spsc_ringbuf_size(rb) == 1);
    
    /* Pop value */
    int popped = 0;
    TEST_ASSERT(keel_spsc_ringbuf_try_pop(rb, &popped));
    TEST_ASSERT(popped == 42);
    TEST_ASSERT(keel_spsc_ringbuf_is_empty(rb));
    
    /* Pop from empty should fail */
    TEST_ASSERT(!keel_spsc_ringbuf_try_pop(rb, &popped));
    
    keel_spsc_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

static void test_spsc_fill_and_drain(void) {
    printf("  Testing SPSC fill and drain...\n");
    
    /* Capacity 8 means 7 usable slots (one reserved) */
    keel_spsc_ringbuf_t* rb = keel_spsc_ringbuf_create(8, sizeof(int));
    TEST_ASSERT(rb != NULL);
    
    /* Fill it up - power of 2 capacity 8, so 7 slots usable */
    for (int i = 0; i < 7; i++) {
        TEST_ASSERT(keel_spsc_ringbuf_try_push(rb, &i));
    }
    
    /* Should be full now (one slot reserved) */
    TEST_ASSERT(keel_spsc_ringbuf_is_full(rb));
    
    /* Push to full should fail */
    int extra = 99;
    TEST_ASSERT(!keel_spsc_ringbuf_try_push(rb, &extra));
    
    /* Drain it */
    for (int i = 0; i < 7; i++) {
        int value;
        TEST_ASSERT(keel_spsc_ringbuf_try_pop(rb, &value));
        TEST_ASSERT(value == i);
    }
    
    TEST_ASSERT(keel_spsc_ringbuf_is_empty(rb));
    
    keel_spsc_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

static void test_spsc_zero_copy(void) {
    printf("  Testing SPSC zero-copy operations...\n");
    
    keel_spsc_ringbuf_t* rb = keel_spsc_ringbuf_create(8, sizeof(int));
    TEST_ASSERT(rb != NULL);
    
    /* Prepare push - get slot pointer */
    int* slot = (int*)keel_spsc_ringbuf_prepare_push(rb);
    TEST_ASSERT(slot != NULL);
    
    /* Write directly */
    *slot = 123;
    
    /* Commit push */
    keel_spsc_ringbuf_commit_push(rb);
    
    TEST_ASSERT(keel_spsc_ringbuf_size(rb) == 1);
    
    /* Prepare pop - get slot pointer */
    int* read_slot = (int*)keel_spsc_ringbuf_prepare_pop(rb);
    TEST_ASSERT(read_slot != NULL);
    TEST_ASSERT(*read_slot == 123);
    
    /* Commit pop */
    keel_spsc_ringbuf_commit_pop(rb);
    
    TEST_ASSERT(keel_spsc_ringbuf_is_empty(rb));
    
    keel_spsc_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * MPSC Ring Buffer Tests
 * ============================================================================ */

static void test_mpsc_create_destroy(void) {
    printf("  Testing MPSC create/destroy...\n");
    
    keel_mpsc_ringbuf_t* rb = keel_mpsc_ringbuf_create(16, sizeof(int));
    TEST_ASSERT(rb != NULL);
    
    TEST_ASSERT(keel_mpsc_ringbuf_is_empty(rb));
    
    keel_mpsc_ringbuf_destroy(rb);
    keel_mpsc_ringbuf_destroy(NULL);
    
    printf("    PASSED\n");
}

static void test_mpsc_push_pop(void) {
    printf("  Testing MPSC push/pop...\n");
    
    keel_mpsc_ringbuf_t* rb = keel_mpsc_ringbuf_create(8, sizeof(int));
    TEST_ASSERT(rb != NULL);
    
    /* Push values */
    for (int i = 0; i < 4; i++) {
        TEST_ASSERT(keel_mpsc_ringbuf_try_push(rb, &i));
    }
    
    /* Pop values */
    for (int i = 0; i < 4; i++) {
        int value;
        TEST_ASSERT(keel_mpsc_ringbuf_try_pop(rb, &value));
        TEST_ASSERT(value == i);
    }
    
    TEST_ASSERT(keel_mpsc_ringbuf_is_empty(rb));
    
    keel_mpsc_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Byte Ring Buffer Tests
 * ============================================================================ */

static void test_byte_ringbuf_create_destroy(void) {
    printf("  Testing byte ring buffer create/destroy...\n");
    
    keel_byte_ringbuf_t* rb = keel_byte_ringbuf_create(1024);
    TEST_ASSERT(rb != NULL);
    
    TEST_ASSERT(keel_byte_ringbuf_readable(rb) == 0);
    TEST_ASSERT(keel_byte_ringbuf_writable(rb) > 0);
    
    keel_byte_ringbuf_destroy(rb);
    keel_byte_ringbuf_destroy(NULL);
    
    printf("    PASSED\n");
}

static void test_byte_ringbuf_write_read(void) {
    printf("  Testing byte ring buffer write/read...\n");
    
    keel_byte_ringbuf_t* rb = keel_byte_ringbuf_create(64);
    TEST_ASSERT(rb != NULL);
    
    const char* message = "Hello, World!";
    size_t len = strlen(message);
    
    /* Write */
    size_t written = keel_byte_ringbuf_write(rb, message, len);
    TEST_ASSERT(written == len);
    TEST_ASSERT(keel_byte_ringbuf_readable(rb) == len);
    
    /* Read */
    char buffer[64] = {0};
    size_t read = keel_byte_ringbuf_read(rb, buffer, sizeof(buffer));
    TEST_ASSERT(read == len);
    TEST_ASSERT(memcmp(buffer, message, len) == 0);
    TEST_ASSERT(keel_byte_ringbuf_readable(rb) == 0);
    
    keel_byte_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

static void test_byte_ringbuf_peek_consume(void) {
    printf("  Testing byte ring buffer peek/consume...\n");
    
    keel_byte_ringbuf_t* rb = keel_byte_ringbuf_create(64);
    TEST_ASSERT(rb != NULL);
    
    const char* message = "Test123";
    keel_byte_ringbuf_write(rb, message, 7);
    
    /* Peek without consuming */
    char peek_buf[8] = {0};
    size_t peeked = keel_byte_ringbuf_peek(rb, peek_buf, 4);
    TEST_ASSERT(peeked == 4);
    TEST_ASSERT(memcmp(peek_buf, "Test", 4) == 0);
    
    /* Data should still be readable */
    TEST_ASSERT(keel_byte_ringbuf_readable(rb) == 7);
    
    /* Consume without reading */
    size_t consumed = keel_byte_ringbuf_consume(rb, 4);
    TEST_ASSERT(consumed == 4);
    TEST_ASSERT(keel_byte_ringbuf_readable(rb) == 3);
    
    /* Read remaining */
    char remain[4] = {0};
    size_t read = keel_byte_ringbuf_read(rb, remain, sizeof(remain));
    TEST_ASSERT(read == 3);
    TEST_ASSERT(memcmp(remain, "123", 3) == 0);
    
    keel_byte_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

static void test_byte_ringbuf_wraparound(void) {
    printf("  Testing byte ring buffer wraparound...\n");
    
    /* Small buffer to force wraparound */
    keel_byte_ringbuf_t* rb = keel_byte_ringbuf_create(16);
    TEST_ASSERT(rb != NULL);
    
    /* Write and read to advance pointers */
    const char* data1 = "12345678";
    keel_byte_ringbuf_write(rb, data1, 8);
    
    char buf[8];
    keel_byte_ringbuf_read(rb, buf, 8);
    
    /* Now write data that will wrap around */
    const char* data2 = "ABCDEFGH";
    size_t written = keel_byte_ringbuf_write(rb, data2, 8);
    TEST_ASSERT(written == 8);
    
    /* Read should handle wraparound */
    size_t read = keel_byte_ringbuf_read(rb, buf, 8);
    TEST_ASSERT(read == 8);
    TEST_ASSERT(memcmp(buf, data2, 8) == 0);
    
    keel_byte_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Log Entry Tests
 * ============================================================================ */

static void test_log_ringbuf(void) {
    printf("  Testing log ring buffer...\n");
    
    keel_spsc_ringbuf_t* rb = keel_log_ringbuf_create(8);
    TEST_ASSERT(rb != NULL);
    
    /* Create a log entry */
    keel_log_entry_t entry = {
        .timestamp = 1234567890,
        .level = 2,  /* INFO */
        .line = 42,
        .file_len = 10,
        .msg_len = 11,
    };
    memcpy(entry.data, "test.c", 7);
    memcpy(entry.data + 10, "Hello World", 11);
    
    /* Push log entry */
    TEST_ASSERT(keel_spsc_ringbuf_try_push(rb, &entry));
    
    /* Pop and verify */
    keel_log_entry_t popped;
    TEST_ASSERT(keel_spsc_ringbuf_try_pop(rb, &popped));
    TEST_ASSERT(popped.timestamp == entry.timestamp);
    TEST_ASSERT(popped.level == entry.level);
    TEST_ASSERT(popped.line == entry.line);
    
    keel_spsc_ringbuf_destroy(rb);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Running ring buffer tests...\n");
    
    /* SPSC tests */
    test_spsc_create_destroy();
    test_spsc_push_pop();
    test_spsc_fill_and_drain();
    test_spsc_zero_copy();
    
    /* MPSC tests */
    test_mpsc_create_destroy();
    test_mpsc_push_pop();
    
    /* Byte buffer tests */
    test_byte_ringbuf_create_destroy();
    test_byte_ringbuf_write_read();
    test_byte_ringbuf_peek_consume();
    test_byte_ringbuf_wraparound();
    
    /* Log entry tests */
    test_log_ringbuf();
    
    printf("All ring buffer tests passed!\n");
    return 0;
}
