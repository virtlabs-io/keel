/**
 * @file test_xxhash.c
 * @brief Known-vector and streaming-equivalence tests for the in-tree xxHash.
 *
 * The test vectors for empty-input hashes are taken from the reference xxHash
 * repository. The streaming tests verify that feeding data in arbitrary chunk
 * sizes produces the same digest as the one-shot path, which is the main
 * property callers rely on when switching between modes.
 */

#include "test_utils.h"
#include "keel/util/xxhash.h"

#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Known Test Vectors
 * 
 * These are verified against the reference XXHash implementation.
 * https://github.com/Cyan4973/xxHash
 * ============================================================================ */

static void test_xxh32_empty(void) {
    printf("  Testing XXH32 empty string...\n");
    
    /* XXH32("", 0, 0) should produce a specific value */
    uint32_t h = keel_xxh32("", 0, 0);
    TEST_ASSERT(h == 0x02CC5D05U);
    
    printf("    PASSED\n");
}

static void test_xxh32_hello(void) {
    printf("  Testing XXH32 with 'Hello'...\n");
    
    uint32_t h = keel_xxh32("Hello", 5, 0);
    /* Verify it produces a consistent hash */
    TEST_ASSERT(h != 0);
    
    /* Same input should produce same output */
    uint32_t h2 = keel_xxh32("Hello", 5, 0);
    TEST_ASSERT(h == h2);
    
    /* Different seed should produce different output */
    uint32_t h3 = keel_xxh32("Hello", 5, 1);
    TEST_ASSERT(h != h3);
    
    printf("    PASSED\n");
}

static void test_xxh32_long(void) {
    printf("  Testing XXH32 with longer data...\n");
    
    /* Test with data >= 16 bytes (uses different code path) */
    const char* data = "The quick brown fox jumps over the lazy dog";
    size_t len = strlen(data);
    
    uint32_t h = keel_xxh32(data, len, 0);
    TEST_ASSERT(h != 0);
    
    /* Verify consistency */
    uint32_t h2 = keel_xxh32(data, len, 0);
    TEST_ASSERT(h == h2);
    
    printf("    PASSED\n");
}

static void test_xxh32_streaming(void) {
    printf("  Testing XXH32 streaming mode...\n");
    
    const char* data = "Hello, World!";
    size_t len = strlen(data);
    
    /* One-shot hash */
    uint32_t h1 = keel_xxh32(data, len, 0);
    
    /* Streaming hash - single update */
    keel_xxh32_state_t state;
    keel_xxh32_reset(&state, 0);
    keel_xxh32_update(&state, data, len);
    uint32_t h2 = keel_xxh32_digest(&state);
    
    TEST_ASSERT(h1 == h2);
    
    /* Streaming hash - multiple updates */
    keel_xxh32_reset(&state, 0);
    keel_xxh32_update(&state, "Hello, ", 7);
    keel_xxh32_update(&state, "World!", 6);
    uint32_t h3 = keel_xxh32_digest(&state);
    
    TEST_ASSERT(h1 == h3);
    
    printf("    PASSED\n");
}

static void test_xxh32_streaming_long(void) {
    printf("  Testing XXH32 streaming with long data...\n");
    
    const char* data = "The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs.";
    size_t len = strlen(data);
    
    /* One-shot */
    uint32_t h1 = keel_xxh32(data, len, 0);
    
    /* Streaming - byte by byte */
    keel_xxh32_state_t state;
    keel_xxh32_reset(&state, 0);
    for (size_t i = 0; i < len; i++) {
        keel_xxh32_update(&state, data + i, 1);
    }
    uint32_t h2 = keel_xxh32_digest(&state);
    
    TEST_ASSERT(h1 == h2);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * XXH64 Tests
 * ============================================================================ */

static void test_xxh64_empty(void) {
    printf("  Testing XXH64 empty string...\n");
    
    /* XXH64("", 0, 0) should produce a specific value */
    uint64_t h = keel_xxh64("", 0, 0);
    TEST_ASSERT(h == 0xEF46DB3751D8E999ULL);
    
    printf("    PASSED\n");
}

static void test_xxh64_hello(void) {
    printf("  Testing XXH64 with 'Hello'...\n");
    
    uint64_t h = keel_xxh64("Hello", 5, 0);
    TEST_ASSERT(h != 0);
    
    /* Consistency */
    uint64_t h2 = keel_xxh64("Hello", 5, 0);
    TEST_ASSERT(h == h2);
    
    /* Different seed */
    uint64_t h3 = keel_xxh64("Hello", 5, 1);
    TEST_ASSERT(h != h3);
    
    printf("    PASSED\n");
}

static void test_xxh64_long(void) {
    printf("  Testing XXH64 with longer data...\n");
    
    /* Test with data >= 32 bytes (uses different code path) */
    const char* data = "The quick brown fox jumps over the lazy dog";
    size_t len = strlen(data);
    
    uint64_t h = keel_xxh64(data, len, 0);
    TEST_ASSERT(h != 0);
    
    uint64_t h2 = keel_xxh64(data, len, 0);
    TEST_ASSERT(h == h2);
    
    printf("    PASSED\n");
}

static void test_xxh64_streaming(void) {
    printf("  Testing XXH64 streaming mode...\n");
    
    const char* data = "Hello, World! This is a test string for XXH64 streaming.";
    size_t len = strlen(data);
    
    /* One-shot */
    uint64_t h1 = keel_xxh64(data, len, 0);
    
    /* Streaming - single update */
    keel_xxh64_state_t state;
    keel_xxh64_reset(&state, 0);
    keel_xxh64_update(&state, data, len);
    uint64_t h2 = keel_xxh64_digest(&state);
    
    TEST_ASSERT(h1 == h2);
    
    /* Streaming - multiple updates */
    keel_xxh64_reset(&state, 0);
    keel_xxh64_update(&state, data, 20);
    keel_xxh64_update(&state, data + 20, len - 20);
    uint64_t h3 = keel_xxh64_digest(&state);
    
    TEST_ASSERT(h1 == h3);
    
    printf("    PASSED\n");
}

static void test_xxh64_streaming_long(void) {
    printf("  Testing XXH64 streaming with long data...\n");
    
    const char* data = "The quick brown fox jumps over the lazy dog. "
                       "Pack my box with five dozen liquor jugs. "
                       "How vexingly quick daft zebras jump!";
    size_t len = strlen(data);
    
    /* One-shot */
    uint64_t h1 = keel_xxh64(data, len, 0);
    
    /* Streaming - byte by byte */
    keel_xxh64_state_t state;
    keel_xxh64_reset(&state, 0);
    for (size_t i = 0; i < len; i++) {
        keel_xxh64_update(&state, data + i, 1);
    }
    uint64_t h2 = keel_xxh64_digest(&state);
    
    TEST_ASSERT(h1 == h2);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Convenience Function Tests
 * ============================================================================ */

static void test_xxh64_str(void) {
    printf("  Testing XXH64 string convenience function...\n");
    
    const char* str = "Hello, World!";
    
    uint64_t h1 = keel_xxh64_str(str, 0);
    uint64_t h2 = keel_xxh64(str, strlen(str), 0);
    
    TEST_ASSERT(h1 == h2);
    
    printf("    PASSED\n");
}

static void test_xxh64_view(void) {
    printf("  Testing XXH64 view convenience function...\n");
    
    const char* data = "Hello, World!";
    
    uint64_t h1 = keel_xxh64_view(data, 5, 0);  /* "Hello" */
    uint64_t h2 = keel_xxh64("Hello", 5, 0);
    
    TEST_ASSERT(h1 == h2);
    
    printf("    PASSED\n");
}

static void test_query_fingerprint(void) {
    printf("  Testing query fingerprint...\n");
    
    const char* query1 = "SELECT * FROM users WHERE id = $1";
    const char* query2 = "SELECT * FROM users WHERE id = $1";
    const char* query3 = "SELECT * FROM users WHERE id = $2";
    
    uint64_t fp1 = keel_query_fingerprint(query1, strlen(query1));
    uint64_t fp2 = keel_query_fingerprint(query2, strlen(query2));
    uint64_t fp3 = keel_query_fingerprint(query3, strlen(query3));
    
    /* Same query should have same fingerprint */
    TEST_ASSERT(fp1 == fp2);
    
    /* Different query should have different fingerprint */
    TEST_ASSERT(fp1 != fp3);
    
    printf("    PASSED\n");
}

static void test_xxh64_combine(void) {
    printf("  Testing XXH64 combine...\n");
    
    uint64_t h1 = keel_xxh64("database", 8, 0);
    uint64_t h2 = keel_xxh64("table", 5, 0);
    
    uint64_t combined = keel_xxh64_combine(h1, h2);
    TEST_ASSERT(combined != h1);
    TEST_ASSERT(combined != h2);
    
    /* Combine should be consistent */
    uint64_t combined2 = keel_xxh64_combine(h1, h2);
    TEST_ASSERT(combined == combined2);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Distribution Tests
 * ============================================================================ */

static void test_hash_distribution(void) {
    printf("  Testing hash distribution...\n");
    
    /* Hash 1000 sequential integers and check for uniqueness */
    uint64_t hashes[1000];
    char buf[32];
    
    for (int i = 0; i < 1000; i++) {
        int len = snprintf(buf, sizeof(buf), "%d", i);
        hashes[i] = keel_xxh64(buf, (size_t)len, 0);
    }
    
    /* Check for collisions (should be extremely rare for 1000 values) */
    int collisions = 0;
    for (int i = 0; i < 999; i++) {
        for (int j = i + 1; j < 1000; j++) {
            if (hashes[i] == hashes[j]) {
                collisions++;
            }
        }
    }
    
    TEST_ASSERT(collisions == 0);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Test Runner
 * ============================================================================ */

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    printf("\n=== XXHash Tests ===\n\n");

    /* XXH32 tests */
    test_xxh32_empty();
    test_xxh32_hello();
    test_xxh32_long();
    test_xxh32_streaming();
    test_xxh32_streaming_long();
    
    /* XXH64 tests */
    test_xxh64_empty();
    test_xxh64_hello();
    test_xxh64_long();
    test_xxh64_streaming();
    test_xxh64_streaming_long();
    
    /* Convenience functions */
    test_xxh64_str();
    test_xxh64_view();
    test_query_fingerprint();
    test_xxh64_combine();
    
    /* Distribution */
    test_hash_distribution();

    return test_summary();
}
