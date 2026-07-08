/**
 * @file test_mem.c
 * @brief Unit tests for the custom memory management layer.
 *
 * The memory subsystem is exercised from the outside of its public API because
 * the rest of the codebase depends on it before almost anything else. This suite
 * checks allocation contracts, zeroing, alignment, duplication, arena lifecycle,
 * and pool reuse so regressions in the allocator surface immediately rather than
 * manifesting as mysterious corruption in higher-level subsystems.
 */

#include "test_utils.h"
#include "keel/mem/mem.h"
#include <string.h>
#include <stdint.h>

/* ============================================================================
 * Memory Initialization Tests
 * ============================================================================ */

static void test_mem_init_shutdown(void) {
    TEST_BEGIN("memory init/shutdown");
    
    /* Initialize with defaults */
    keel_error_t err = keel_mem_init(NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Shutdown should be safe */
    keel_mem_shutdown();
    
    /* Re-initialize with custom config */
    keel_mem_config_t config = keel_mem_config_default();
    config.default_arena_size = 32 * 1024;
    config.max_cached_arenas = 8;
    
    err = keel_mem_init(&config);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Allocate something to ensure it works */
    void* p = keel_malloc(100);
    TEST_ASSERT_NOT_NULL(p);
    keel_free(p);
    
    keel_mem_shutdown();
    
    /* Final init for remaining tests */
    err = keel_mem_init(NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    TEST_END();
}

/* ============================================================================
 * Basic Allocation Tests
 * ============================================================================ */

static void test_basic_alloc(void) {
    TEST_BEGIN("basic allocation");
    
    /* Test malloc */
    void* p = keel_malloc(1024);
    TEST_ASSERT_NOT_NULL(p);
    keel_free(p);
    
    /* Test zero-size malloc */
    p = keel_malloc(0);
    /* Implementation may return NULL or valid ptr for size 0 */
    if (p) keel_free(p);
    
    /* Test large allocation */
    p = keel_malloc(1024 * 1024);  /* 1MB */
    TEST_ASSERT_NOT_NULL(p);
    keel_free(p);
    
    /* Test calloc and verify zeroed */
    p = keel_calloc(10, 100);
    TEST_ASSERT_NOT_NULL(p);
    
    char* cp = (char*)p;
    bool all_zero = true;
    for (size_t i = 0; i < 1000; i++) {
        if (cp[i] != 0) {
            all_zero = false;
            break;
        }
    }
    TEST_ASSERT(all_zero);
    keel_free(p);
    
    /* Test calloc edge cases */
    p = keel_calloc(0, 100);
    if (p) keel_free(p);
    
    p = keel_calloc(100, 0);
    if (p) keel_free(p);
    
    TEST_END();
}

static void test_realloc(void) {
    TEST_BEGIN("realloc");
    
    /* Realloc NULL should act like malloc */
    void* p = keel_realloc(NULL, 100);
    TEST_ASSERT_NOT_NULL(p);
    
    /* Write data to verify it survives realloc */
    memset(p, 0xAB, 100);
    
    /* Grow allocation */
    p = keel_realloc(p, 200);
    TEST_ASSERT_NOT_NULL(p);
    
    /* Verify original data preserved */
    uint8_t* bytes = (uint8_t*)p;
    bool data_preserved = true;
    for (size_t i = 0; i < 100; i++) {
        if (bytes[i] != 0xAB) {
            data_preserved = false;
            break;
        }
    }
    TEST_ASSERT(data_preserved);
    
    /* Shrink allocation */
    p = keel_realloc(p, 50);
    TEST_ASSERT_NOT_NULL(p);
    
    /* Realloc to 0 should free (implementation dependent) */
    void* p2 = keel_realloc(p, 0);
    /* p is now invalid, p2 may be NULL or valid */
    if (p2) keel_free(p2);
    
    TEST_END();
}

static void test_free_null(void) {
    TEST_BEGIN("free NULL safety");
    
    /* Free NULL should be safe */
    keel_free(NULL);
    keel_free(NULL);  /* Multiple times */
    
    TEST_ASSERT(true);  /* If we get here, it worked */
    
    TEST_END();
}

/* ============================================================================
 * Aligned Allocation Tests
 * ============================================================================ */

static void test_aligned_alloc(void) {
    TEST_BEGIN("aligned allocation");
    
    /* Test various alignments */
    size_t alignments[] = {16, 32, 64, 128, 256, 512, 1024, 4096};
    
    for (size_t i = 0; i < sizeof(alignments)/sizeof(alignments[0]); i++) {
        size_t align = alignments[i];
        void* p = keel_aligned_alloc(align, 1000);
        TEST_ASSERT_NOT_NULL(p);
        
        /* Verify alignment */
        uintptr_t addr = (uintptr_t)p;
        TEST_ASSERT_EQ(addr % align, 0);
        
        /* Write to it to ensure it's usable */
        memset(p, 0xFF, 1000);
        
        keel_aligned_free(p);
    }
    
    TEST_END();
}

/* ============================================================================
 * String/Memory Duplication Tests
 * ============================================================================ */

static void test_memdup(void) {
    TEST_BEGIN("memory duplication");
    
    /* Test memdup */
    uint8_t source[256];
    for (int i = 0; i < 256; i++) {
        source[i] = (uint8_t)i;
    }
    
    void* dup = keel_memdup(source, sizeof(source));
    TEST_ASSERT_NOT_NULL(dup);
    TEST_ASSERT(memcmp(source, dup, sizeof(source)) == 0);
    TEST_ASSERT(dup != source);  /* Must be a copy */
    keel_free(dup);
    
    /* Test memdup with zero size */
    dup = keel_memdup(source, 0);
    if (dup) keel_free(dup);
    
    /* Test memdup with NULL source */
    dup = keel_memdup(NULL, 100);
    TEST_ASSERT_NULL(dup);
    
    TEST_END();
}

static void test_strdup(void) {
    TEST_BEGIN("string duplication");
    
    /* Test strdup */
    const char* original = "Hello, World!";
    char* copy = keel_strdup(original);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_STR_EQ(copy, original);
    TEST_ASSERT(copy != original);
    keel_free(copy);
    
    /* Test empty string */
    copy = keel_strdup("");
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_EQ(copy[0], '\0');
    keel_free(copy);
    
    /* Test NULL */
    copy = keel_strdup(NULL);
    TEST_ASSERT_NULL(copy);
    
    TEST_END();
}

static void test_strndup(void) {
    TEST_BEGIN("string duplication with limit");
    
    const char* original = "Hello, World!";
    
    /* Copy less than full length */
    char* copy = keel_strndup(original, 5);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_STR_EQ(copy, "Hello");
    keel_free(copy);
    
    /* Copy more than available - should stop at null terminator */
    copy = keel_strndup(original, 100);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_STR_EQ(copy, original);
    keel_free(copy);
    
    /* Copy zero characters */
    copy = keel_strndup(original, 0);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_EQ(copy[0], '\0');
    keel_free(copy);
    
    TEST_END();
}

/* ============================================================================
 * Arena Allocator Tests
 * ============================================================================ */

static void test_arena_basic(void) {
    TEST_BEGIN("arena basic operations");
    
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);
    
    /* Multiple allocations */
    void* p1 = keel_arena_alloc(arena, 100);
    TEST_ASSERT_NOT_NULL(p1);
    
    void* p2 = keel_arena_alloc(arena, 200);
    TEST_ASSERT_NOT_NULL(p2);
    TEST_ASSERT(p2 != p1);
    
    void* p3 = keel_arena_alloc(arena, 300);
    TEST_ASSERT_NOT_NULL(p3);
    
    /* Write to allocations */
    memset(p1, 0xAA, 100);
    memset(p2, 0xBB, 200);
    memset(p3, 0xCC, 300);
    
    /* Reset should allow reuse */
    keel_arena_reset(arena);
    
    void* p4 = keel_arena_alloc(arena, 100);
    TEST_ASSERT_NOT_NULL(p4);
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_calloc(void) {
    TEST_BEGIN("arena calloc");
    
    keel_arena_t* arena = keel_arena_create(0);  /* Default size */
    TEST_ASSERT_NOT_NULL(arena);
    
    void* p = keel_arena_calloc(arena, 10, 100);
    TEST_ASSERT_NOT_NULL(p);
    
    /* Verify zeroed */
    uint8_t* bytes = (uint8_t*)p;
    bool all_zero = true;
    for (size_t i = 0; i < 1000; i++) {
        if (bytes[i] != 0) {
            all_zero = false;
            break;
        }
    }
    TEST_ASSERT(all_zero);
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_aligned(void) {
    TEST_BEGIN("arena aligned allocation");
    
    keel_arena_t* arena = keel_arena_create(0);
    TEST_ASSERT_NOT_NULL(arena);
    
    /* Test various alignments */
    size_t alignments[] = {16, 32, 64, 128, 256};
    
    for (size_t i = 0; i < sizeof(alignments)/sizeof(alignments[0]); i++) {
        void* p = keel_arena_alloc_aligned(arena, 100, alignments[i]);
        TEST_ASSERT_NOT_NULL(p);
        
        uintptr_t addr = (uintptr_t)p;
        TEST_ASSERT_EQ(addr % alignments[i], 0);
    }
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_mark_restore(void) {
    TEST_BEGIN("arena mark/restore");
    
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);
    
    /* First allocation */
    void* p1 = keel_arena_alloc(arena, 100);
    TEST_ASSERT_NOT_NULL(p1);
    memset(p1, 0xAA, 100);
    
    /* Save mark */
    keel_arena_mark_t mark = keel_arena_save(arena);
    
    /* More allocations */
    void* p2 = keel_arena_alloc(arena, 200);
    TEST_ASSERT_NOT_NULL(p2);
    
    void* p3 = keel_arena_alloc(arena, 300);
    TEST_ASSERT_NOT_NULL(p3);
    
    /* Restore to mark - p2 and p3 should be "freed" */
    keel_arena_restore(arena, mark);
    
    /* New allocation should reuse space */
    void* p4 = keel_arena_alloc(arena, 50);
    TEST_ASSERT_NOT_NULL(p4);
    
    /* p1 should still be valid (before mark) */
    uint8_t* bytes = (uint8_t*)p1;
    TEST_ASSERT_EQ(bytes[0], 0xAA);
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_strdup(void) {
    TEST_BEGIN("arena strdup");
    
    keel_arena_t* arena = keel_arena_create(0);
    TEST_ASSERT_NOT_NULL(arena);
    
    const char* original = "Test string for arena";
    char* copy = keel_arena_strdup(arena, original);
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_STR_EQ(copy, original);
    
    /* Empty string */
    copy = keel_arena_strdup(arena, "");
    TEST_ASSERT_NOT_NULL(copy);
    TEST_ASSERT_EQ(copy[0], '\0');
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_sprintf(void) {
    TEST_BEGIN("arena sprintf");
    
    keel_arena_t* arena = keel_arena_create(0);
    TEST_ASSERT_NOT_NULL(arena);
    
    char* s = keel_arena_sprintf(arena, "Value: %d, Name: %s", 42, "test");
    TEST_ASSERT_NOT_NULL(s);
    TEST_ASSERT_STR_EQ(s, "Value: 42, Name: test");
    
    /* Multiple sprintf calls */
    char* s2 = keel_arena_sprintf(arena, "%s %s", "Hello", "World");
    TEST_ASSERT_NOT_NULL(s2);
    TEST_ASSERT_STR_EQ(s2, "Hello World");
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_stats(void) {
    TEST_BEGIN("arena statistics");
    
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);
    
    size_t used, committed;
    keel_arena_stats(arena, &used, &committed);
    TEST_ASSERT_EQ(used, 0);
    TEST_ASSERT(committed > 0);
    
    /* Allocate some memory */
    void* p = keel_arena_alloc(arena, 1000);
    TEST_ASSERT_NOT_NULL(p);
    
    keel_arena_stats(arena, &used, &committed);
    TEST_ASSERT(used >= 1000);
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

static void test_arena_large_allocation(void) {
    TEST_BEGIN("arena large allocation");
    
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);
    
    /* Allocate more than initial block size - should create new block */
    void* p1 = keel_arena_alloc(arena, 100 * 1024);  /* 100KB */
    TEST_ASSERT_NOT_NULL(p1);
    
    void* p2 = keel_arena_alloc(arena, 100);
    TEST_ASSERT_NOT_NULL(p2);
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

/* ============================================================================
 * Pool Allocator Tests
 * ============================================================================ */

static void test_pool_basic(void) {
    TEST_BEGIN("pool basic operations");
    
    keel_pool_config_t config = {
        .object_size = 64,
        .initial_count = 16,
        .max_count = 0,
        .zero_on_alloc = false,
    };
    
    keel_pool_t* pool = keel_pool_create(&config);
    TEST_ASSERT_NOT_NULL(pool);
    
    void* p1 = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p1);
    
    void* p2 = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p2);
    TEST_ASSERT(p1 != p2);
    
    keel_pool_free(pool, p1);
    
    void* p3 = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p3);
    /* Might be same as p1 (reused) */
    
    keel_pool_free(pool, p2);
    keel_pool_free(pool, p3);
    
    keel_pool_destroy(pool);
    
    TEST_END();
}

static void test_pool_zero_on_alloc(void) {
    TEST_BEGIN("pool zero on alloc");
    
    keel_pool_config_t config = {
        .object_size = 128,
        .initial_count = 8,
        .max_count = 0,
        .zero_on_alloc = true,  /* Enable zeroing */
    };
    
    keel_pool_t* pool = keel_pool_create(&config);
    TEST_ASSERT_NOT_NULL(pool);
    
    void* p = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p);
    
    /* Verify zeroed */
    uint8_t* bytes = (uint8_t*)p;
    bool all_zero = true;
    for (size_t i = 0; i < 128; i++) {
        if (bytes[i] != 0) {
            all_zero = false;
            break;
        }
    }
    TEST_ASSERT(all_zero);
    
    /* Dirty the memory */
    memset(p, 0xFF, 128);
    keel_pool_free(pool, p);
    
    /* Re-allocate - should be zeroed again */
    void* p2 = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p2);
    
    bytes = (uint8_t*)p2;
    all_zero = true;
    for (size_t i = 0; i < 128; i++) {
        if (bytes[i] != 0) {
            all_zero = false;
            break;
        }
    }
    TEST_ASSERT(all_zero);
    
    keel_pool_free(pool, p2);
    keel_pool_destroy(pool);
    
    TEST_END();
}

static void test_pool_stats(void) {
    TEST_BEGIN("pool statistics");
    
    keel_pool_config_t config = {
        .object_size = 32,
        .initial_count = 16,
        .max_count = 0,
        .zero_on_alloc = false,
    };
    
    keel_pool_t* pool = keel_pool_create(&config);
    TEST_ASSERT_NOT_NULL(pool);
    
    size_t allocated, available, total;
    
    /* Initially none allocated */
    keel_pool_stats(pool, &allocated, &available, &total);
    TEST_ASSERT_EQ(allocated, 0);
    TEST_ASSERT(available > 0);
    TEST_ASSERT(total >= 16);
    
    /* Allocate some */
    void* ptrs[8];
    for (int i = 0; i < 8; i++) {
        ptrs[i] = keel_pool_alloc(pool);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }
    
    keel_pool_stats(pool, &allocated, &available, &total);
    TEST_ASSERT_EQ(allocated, 8);
    
    /* Free some */
    for (int i = 0; i < 4; i++) {
        keel_pool_free(pool, ptrs[i]);
    }
    
    keel_pool_stats(pool, &allocated, &available, &total);
    TEST_ASSERT_EQ(allocated, 4);
    
    /* Free rest */
    for (int i = 4; i < 8; i++) {
        keel_pool_free(pool, ptrs[i]);
    }
    
    keel_pool_destroy(pool);
    
    TEST_END();
}

static void test_pool_exhaust_and_grow(void) {
    TEST_BEGIN("pool exhaust and grow");
    
    keel_pool_config_t config = {
        .object_size = 16,
        .initial_count = 4,  /* Start small */
        .max_count = 0,      /* Unlimited */
        .zero_on_alloc = false,
    };
    
    keel_pool_t* pool = keel_pool_create(&config);
    TEST_ASSERT_NOT_NULL(pool);
    
    /* Allocate more than initial count - should grow */
    void* ptrs[20];
    for (int i = 0; i < 20; i++) {
        ptrs[i] = keel_pool_alloc(pool);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }
    
    /* Free all */
    for (int i = 0; i < 20; i++) {
        keel_pool_free(pool, ptrs[i]);
    }
    
    keel_pool_destroy(pool);
    
    TEST_END();
}

static void test_pool_max_limit(void) {
    TEST_BEGIN("pool max limit");
    
    keel_pool_config_t config = {
        .object_size = 32,
        .initial_count = 4,
        .max_count = 8,  /* Limit to 8 objects */
        .zero_on_alloc = false,
    };
    
    keel_pool_t* pool = keel_pool_create(&config);
    TEST_ASSERT_NOT_NULL(pool);
    
    /* Allocate up to limit */
    void* ptrs[8];
    for (int i = 0; i < 8; i++) {
        ptrs[i] = keel_pool_alloc(pool);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }
    
    /* Next allocation should fail */
    void* p = keel_pool_alloc(pool);
    TEST_ASSERT_NULL(p);
    
    /* Free one and retry */
    keel_pool_free(pool, ptrs[0]);
    p = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p);
    
    /* Cleanup */
    keel_pool_free(pool, p);
    for (int i = 1; i < 8; i++) {
        keel_pool_free(pool, ptrs[i]);
    }
    
    keel_pool_destroy(pool);
    
    TEST_END();
}

static void test_pool_double_free_guard(void) {
    TEST_BEGIN("pool double free guard");

    keel_pool_config_t config = {
        .object_size = 256,
        .object_align = 64,
        .initial_count = 4,
        .max_count = 0,
        .zero_on_alloc = true,
    };

    keel_pool_t* pool = keel_pool_create(&config);
    TEST_ASSERT_NOT_NULL(pool);

    void* p = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(p);
    TEST_ASSERT_EQ(((uintptr_t)p & 63u), (uintptr_t)0);

    keel_pool_free(pool, p);
    /* Must be ignored safely (no free-list corruption). */
    keel_pool_free(pool, p);

    void* a = keel_pool_alloc(pool);
    void* b = keel_pool_alloc(pool);
    TEST_ASSERT_NOT_NULL(a);
    TEST_ASSERT_NOT_NULL(b);

    keel_pool_free(pool, a);
    keel_pool_free(pool, b);
    keel_pool_destroy(pool);

    TEST_END();
}

/* ============================================================================
 * Memory Statistics Tests
 * ============================================================================ */

static void test_mem_stats(void) {
    TEST_BEGIN("memory statistics");
    
    keel_mem_stats_t stats;
    keel_mem_stats_get(&stats);
    
    /* Stats should be initialized */
    size_t initial_allocs = stats.allocation_count;
    
    /* Allocate some memory */
    void* p1 = keel_malloc(1000);
    void* p2 = keel_malloc(2000);
    
    keel_mem_stats_get(&stats);
    TEST_ASSERT(stats.allocation_count >= initial_allocs + 2);
    TEST_ASSERT(stats.bytes_allocated >= 3000);
    
    keel_free(p1);
    keel_free(p2);
    
    keel_mem_stats_get(&stats);
    TEST_ASSERT_EQ(stats.allocation_count, initial_allocs);
    
    TEST_END();
}

/* ============================================================================
 * Stress Tests
 * ============================================================================ */

static void test_stress_alloc_free(void) {
    TEST_BEGIN("stress: alloc/free cycles");
    
    #define STRESS_COUNT 1000
    void* ptrs[STRESS_COUNT];
    
    /* Many small allocations */
    for (int i = 0; i < STRESS_COUNT; i++) {
        ptrs[i] = keel_malloc((i % 100) + 1);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }
    
    /* Free in reverse order */
    for (int i = STRESS_COUNT - 1; i >= 0; i--) {
        keel_free(ptrs[i]);
    }
    
    /* Random-ish pattern */
    for (int i = 0; i < STRESS_COUNT; i++) {
        ptrs[i] = keel_malloc((i * 7) % 1000 + 1);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }
    
    /* Free every other, then the rest */
    for (int i = 0; i < STRESS_COUNT; i += 2) {
        keel_free(ptrs[i]);
        ptrs[i] = NULL;
    }
    for (int i = 1; i < STRESS_COUNT; i += 2) {
        keel_free(ptrs[i]);
    }
    
    #undef STRESS_COUNT
    
    TEST_END();
}

static void test_stress_arena(void) {
    TEST_BEGIN("stress: arena operations");
    
    keel_arena_t* arena = keel_arena_create(0);
    TEST_ASSERT_NOT_NULL(arena);
    
    /* Many allocations */
    for (int round = 0; round < 10; round++) {
        for (int i = 0; i < 100; i++) {
            void* p = keel_arena_alloc(arena, (i % 50) + 1);
            TEST_ASSERT_NOT_NULL(p);
        }
        keel_arena_reset(arena);
    }
    
    keel_arena_destroy(arena);
    
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Memory Management Tests (Comprehensive)\n");
    printf("========================================\n\n");
    
    /* Initialization */
    test_mem_init_shutdown();
    
    /* Basic operations */
    test_basic_alloc();
    test_realloc();
    test_free_null();
    
    /* Aligned allocation */
    test_aligned_alloc();
    
    /* String/memory duplication */
    test_memdup();
    test_strdup();
    test_strndup();
    
    /* Arena allocator */
    test_arena_basic();
    test_arena_calloc();
    test_arena_aligned();
    test_arena_mark_restore();
    test_arena_strdup();
    test_arena_sprintf();
    test_arena_stats();
    test_arena_large_allocation();
    
    /* Pool allocator */
    test_pool_basic();
    test_pool_zero_on_alloc();
    test_pool_stats();
    test_pool_exhaust_and_grow();
    test_pool_max_limit();
    test_pool_double_free_guard();
    
    /* Statistics */
    test_mem_stats();
    
    /* Stress tests */
    test_stress_alloc_free();
    test_stress_arena();
    
    /* Cleanup */
    keel_mem_shutdown();
    
    return test_summary();
}
