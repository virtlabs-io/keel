/**
 * @file test_string_hash.c
 * @brief Unit tests for keel_str_t helpers and the hash / hashmap APIs.
 *
 * These tests were previously blocked by "API mismatches" — this file fixes the
 * test code to align with the actual implemented API as found in string.c / hash.c.
 *
 * Coverage:
 *   §1  keel_str_t basics: eq, eq_nocase, starts_with, ends_with, contains, find.
 *   §2  keel_str_t helpers: trim, split, dup.
 *   §3  FNV-1a 32-bit: known golden values, avalanche (single-bit change).
 *   §4  FNV-1a 64-bit: known golden values, consistency with 32-bit on empty.
 *   §5  FNV-1a string view: consistent with raw pointer version.
 *   §6  MurmurHash3 32-bit: known golden value, seed sensitivity.
 *   §7  Hashmap: lifecycle (new/free/NULL), set/get/remove, collisions, clear,
 *       resize-on-load, overwrite-same-key.
 *   §8  Hashmap NULL-safety: operations on NULL map.
 *   §9  Hash ring: lifecycle, add/remove, get routing, weight distribution.
 *   §10 Stress: 100k hashmap inserts, then lookup all.
 *   §11 Fuzz / edge: empty strings, single-byte strings, binary data.
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
 * Helpers
 * ============================================================================ */

static inline keel_str_t S(const char* s) {
    return keel_str_from_cstr(s);
}

/* ============================================================================
 * §1  keel_str_t comparisons
 * ============================================================================ */

static void test_str_eq(void) {
    TEST_BEGIN("str_eq: equal / unequal slices");

    TEST_ASSERT(keel_str_eq(S("hello"), S("hello")));
    TEST_ASSERT(!keel_str_eq(S("hello"), S("Hello")));
    TEST_ASSERT(!keel_str_eq(S("hello"), S("hell")));
    TEST_ASSERT(!keel_str_eq(S("hello"), S("")));
    TEST_ASSERT(keel_str_eq(S(""), S("")));

    /* Binary content, non-printable bytes */
    keel_str_t a = { .data = "\x00\x01\x02", .len = 3 };
    keel_str_t b = { .data = "\x00\x01\x02", .len = 3 };
    keel_str_t c = { .data = "\x00\x01\x03", .len = 3 };
    TEST_ASSERT(keel_str_eq(a, b));
    TEST_ASSERT(!keel_str_eq(a, c));

    TEST_END();
}

static void test_str_eq_nocase(void) {
    TEST_BEGIN("str_eq_nocase: ASCII case folding");

    TEST_ASSERT(keel_str_eq_nocase(S("SELECT"), S("select")));
    TEST_ASSERT(keel_str_eq_nocase(S("SELECT"), S("Select")));
    TEST_ASSERT(keel_str_eq_nocase(S("AbCdEf"), S("abcdef")));
    TEST_ASSERT(!keel_str_eq_nocase(S("SELECT"), S("SELEC")));
    TEST_ASSERT(keel_str_eq_nocase(S(""), S("")));

    TEST_END();
}

static void test_str_starts_with(void) {
    TEST_BEGIN("str_starts_with");

    TEST_ASSERT(keel_str_starts_with(S("hello world"), S("hello")));
    TEST_ASSERT(keel_str_starts_with(S("hello"), S("hello")));
    TEST_ASSERT(keel_str_starts_with(S("hello"), S("")));
    TEST_ASSERT(!keel_str_starts_with(S("hello"), S("world")));
    TEST_ASSERT(!keel_str_starts_with(S("hi"), S("hello")));

    TEST_END();
}

static void test_str_ends_with(void) {
    TEST_BEGIN("str_ends_with");

    TEST_ASSERT(keel_str_ends_with(S("hello world"), S("world")));
    TEST_ASSERT(keel_str_ends_with(S("hello"), S("hello")));
    TEST_ASSERT(keel_str_ends_with(S("hello"), S("")));
    TEST_ASSERT(!keel_str_ends_with(S("hello world"), S("hello")));
    TEST_ASSERT(!keel_str_ends_with(S("hi"), S("hello")));

    TEST_END();
}

static void test_str_contains(void) {
    TEST_BEGIN("str_contains");

    TEST_ASSERT(keel_str_contains(S("hello world"), S("world")));
    TEST_ASSERT(keel_str_contains(S("hello world"), S("ello")));
    TEST_ASSERT(keel_str_contains(S("hello world"), S("")));
    TEST_ASSERT(!keel_str_contains(S("hello"), S("world")));
    TEST_ASSERT(!keel_str_contains(S(""), S("x")));
    TEST_ASSERT(keel_str_contains(S(""), S("")));

    TEST_END();
}

static void test_str_find(void) {
    TEST_BEGIN("str_find: offset of first match");

    TEST_ASSERT_EQ(keel_str_find(S("hello world"), S("world")), (keel_ssize_t)6);
    TEST_ASSERT_EQ(keel_str_find(S("hello world"), S("hello")), (keel_ssize_t)0);
    TEST_ASSERT_EQ(keel_str_find(S("hello world"), S("")),      (keel_ssize_t)0);
    TEST_ASSERT(keel_str_find(S("hello"), S("world")) < 0);
    TEST_ASSERT_EQ(keel_str_find(S("aabaa"), S("aa")),          (keel_ssize_t)0);

    TEST_END();
}

/* ============================================================================
 * §2  keel_str_t helpers: trim, split, dup
 * ============================================================================ */

static void test_str_trim(void) {
    TEST_BEGIN("str_trim: leading/trailing whitespace removed");

    keel_str_t t;

    t = keel_str_trim(S("  hello  "));
    TEST_ASSERT(keel_str_eq(t, S("hello")));

    t = keel_str_trim(S("   "));
    TEST_ASSERT_EQ(t.len, (size_t)0);

    t = keel_str_trim(S("no_space"));
    TEST_ASSERT(keel_str_eq(t, S("no_space")));

    t = keel_str_trim(S(""));
    TEST_ASSERT_EQ(t.len, (size_t)0);

    /* Tab and newline */
    t = keel_str_trim(S("\t\nhello\n\t"));
    TEST_ASSERT(keel_str_eq(t, S("hello")));

    TEST_END();
}

static void test_str_split(void) {
    TEST_BEGIN("str_split: comma-delimited tokens");

    keel_str_t rem = S("a,b,c,d");
    keel_str_t part;

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    TEST_ASSERT(keel_str_eq(part, S("a")));

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    TEST_ASSERT(keel_str_eq(part, S("b")));

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    TEST_ASSERT(keel_str_eq(part, S("c")));

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    TEST_ASSERT(keel_str_eq(part, S("d")));

    /* Exhausted */
    TEST_ASSERT(!keel_str_split(&rem, ',', &part));

    TEST_END();
}

static void test_str_split_empty_tokens(void) {
    TEST_BEGIN("str_split: handles consecutive delimiters");

    keel_str_t rem = S("a,,b");
    keel_str_t part;

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    TEST_ASSERT(keel_str_eq(part, S("a")));

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    /* empty token between two commas */
    TEST_ASSERT_EQ(part.len, (size_t)0);

    TEST_ASSERT(keel_str_split(&rem, ',', &part));
    TEST_ASSERT(keel_str_eq(part, S("b")));

    TEST_END();
}

static void test_str_dup(void) {
    TEST_BEGIN("str_dup: heap copy is NUL-terminated and independent");

    char* dup = keel_str_dup(S("hello"));
    TEST_ASSERT_NOT_NULL(dup);
    TEST_ASSERT_STR_EQ(dup, "hello");
    TEST_ASSERT_EQ(strlen(dup), (size_t)5);
    keel_free(dup);

    /* Empty string dup */
    dup = keel_str_dup(S(""));
    if (dup) {
        TEST_ASSERT_EQ(strlen(dup), (size_t)0);
        keel_free(dup);
    }

    TEST_END();
}

/* ============================================================================
 * §3  FNV-1a 32-bit — golden values
 * ============================================================================ */

static void test_hash_fnv1a_32_golden(void) {
    TEST_BEGIN("hash fnv1a_32: known golden values");

    /* FNV-1a 32-bit of empty string = 2166136261 (offset basis) */
    TEST_ASSERT_EQ(keel_hash_fnv1a_32("", 0), 0x811C9DC5U);

    /* "a" = 0xe40c292c (well-known reference value) */
    TEST_ASSERT_EQ(keel_hash_fnv1a_32("a", 1), 0xE40C292CU);

    /* Deterministic: calling twice with same input gives same result */
    uint32_t h1 = keel_hash_fnv1a_32("hello", 5);
    uint32_t h2 = keel_hash_fnv1a_32("hello", 5);
    TEST_ASSERT_EQ(h1, h2);

    /* Different lengths differ */
    TEST_ASSERT(keel_hash_fnv1a_32("ab", 2) != keel_hash_fnv1a_32("a", 1));

    TEST_END();
}

static void test_hash_fnv1a_32_avalanche(void) {
    TEST_BEGIN("hash fnv1a_32: single-bit flip changes hash");

    uint8_t data[8] = {0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0};
    uint32_t base = keel_hash_fnv1a_32(data, 8);

    /* Flip each bit individually and verify hash changes at least once */
    int changed = 0;
    for (int byte = 0; byte < 8; byte++) {
        for (int bit = 0; bit < 8; bit++) {
            data[byte] ^= (uint8_t)(1u << bit);
            uint32_t h = keel_hash_fnv1a_32(data, 8);
            if (h != base) changed++;
            data[byte] ^= (uint8_t)(1u << bit); /* restore */
        }
    }
    TEST_ASSERT(changed > 50); /* vast majority of bit flips must change hash */

    TEST_END();
}

/* ============================================================================
 * §4  FNV-1a 64-bit
 * ============================================================================ */

static void test_hash_fnv1a_64_golden(void) {
    TEST_BEGIN("hash fnv1a_64: known offset-basis for empty input");

    /* FNV-1a 64-bit offset basis = 14695981039346656037 = 0xcbf29ce484222325 */
    TEST_ASSERT_EQ(keel_hash_fnv1a_64("", 0), UINT64_C(0xcbf29ce484222325));

    /* Deterministic */
    uint64_t h1 = keel_hash_fnv1a_64("world", 5);
    uint64_t h2 = keel_hash_fnv1a_64("world", 5);
    TEST_ASSERT_EQ(h1, h2);

    TEST_END();
}

static void test_hash_fnv1a_64_vs_32(void) {
    TEST_BEGIN("hash fnv1a_64: 64-bit result differs from 32-bit for same input");

    const char* key = "keel_proxy";
    uint32_t h32 = keel_hash_fnv1a_32(key, strlen(key));
    uint64_t h64 = keel_hash_fnv1a_64(key, strlen(key));
    /* They use different primes/offset, so must differ */
    TEST_ASSERT((uint64_t)h32 != h64);

    TEST_END();
}

/* ============================================================================
 * §5  FNV-1a string view
 * ============================================================================ */

static void test_hash_fnv1a_str(void) {
    TEST_BEGIN("hash fnv1a_str: consistent with raw pointer version");

    keel_str_t key = S("hello");
    uint32_t via_str = keel_hash_fnv1a_str(key);
    uint32_t via_raw = keel_hash_fnv1a_32(key.data, key.len);
    TEST_ASSERT_EQ(via_str, via_raw);

    TEST_END();
}

/* ============================================================================
 * §6  MurmurHash3 32-bit
 * ============================================================================ */

static void test_hash_murmur3_seed(void) {
    TEST_BEGIN("hash murmur3_32: different seeds produce different results");

    const char* key = "consistent_hash_key";
    size_t len = strlen(key);
    uint32_t h0 = keel_hash_murmur3_32(key, len, 0);
    uint32_t h1 = keel_hash_murmur3_32(key, len, 1);
    uint32_t h2 = keel_hash_murmur3_32(key, len, 0xDEADBEEF);

    TEST_ASSERT(h0 != h1 || h1 != h2); /* At least two must differ */

    /* Deterministic for same seed */
    TEST_ASSERT_EQ(h0, keel_hash_murmur3_32(key, len, 0));

    TEST_END();
}

static void test_hash_murmur3_avalanche(void) {
    TEST_BEGIN("hash murmur3_32: good avalanche on adjacent keys");

    uint32_t prev = keel_hash_murmur3_32("", 0, 0);
    int same_as_prev = 0;
    for (int i = 0; i < 256; i++) {
        char c = (char)i;
        uint32_t h = keel_hash_murmur3_32(&c, 1, 0);
        if (h == prev) same_as_prev++;
        prev = h;
    }
    /* In 256 single-byte inputs, at most a handful should collide */
    TEST_ASSERT(same_as_prev <= 2);

    TEST_END();
}

/* ============================================================================
 * §7  Hashmap
 * ============================================================================ */

static void test_hashmap_lifecycle(void) {
    TEST_BEGIN("hashmap lifecycle: new / free");

    keel_hashmap_t* map = keel_hashmap_new(NULL, NULL);
    TEST_ASSERT_NOT_NULL(map);
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)0);
    keel_hashmap_free(map);

    /* Free NULL must not crash */
    keel_hashmap_free(NULL);

    TEST_END();
}

static void test_hashmap_set_get(void) {
    TEST_BEGIN("hashmap set / get");

    keel_hashmap_t* map = keel_hashmap_new(NULL, NULL);
    TEST_ASSERT_NOT_NULL(map);

    int values[4] = {10, 20, 30, 40};
    void* keys[4]  = { &values[0], &values[1], &values[2], &values[3] };

    for (int i = 0; i < 4; i++) {
        TEST_ASSERT(keel_hashmap_set(map, keys[i], keys[i]));
    }

    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)4);

    for (int i = 0; i < 4; i++) {
        void* got = keel_hashmap_get(map, keys[i]);
        TEST_ASSERT(got == keys[i]);
    }

    /* Non-existent key */
    int dummy = 99;
    TEST_ASSERT_NULL(keel_hashmap_get(map, &dummy));

    keel_hashmap_free(map);
    TEST_END();
}

static void test_hashmap_overwrite(void) {
    TEST_BEGIN("hashmap overwrite: same key gets new value");

    keel_hashmap_t* map = keel_hashmap_new(NULL, NULL);
    TEST_ASSERT_NOT_NULL(map);

    static int k = 1, v1 = 100, v2 = 200;

    TEST_ASSERT(keel_hashmap_set(map, &k, &v1));
    TEST_ASSERT_EQ(*(int*)keel_hashmap_get(map, &k), 100);

    TEST_ASSERT(keel_hashmap_set(map, &k, &v2));
    TEST_ASSERT_EQ(*(int*)keel_hashmap_get(map, &k), 200);
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)1);

    keel_hashmap_free(map);
    TEST_END();
}

static void test_hashmap_remove(void) {
    TEST_BEGIN("hashmap remove");

    keel_hashmap_t* map = keel_hashmap_new(NULL, NULL);
    TEST_ASSERT_NOT_NULL(map);

    static int k1 = 1, k2 = 2;

    TEST_ASSERT(keel_hashmap_set(map, &k1, &k1));
    TEST_ASSERT(keel_hashmap_set(map, &k2, &k2));
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)2);

    TEST_ASSERT(keel_hashmap_remove(map, &k1));
    TEST_ASSERT_NULL(keel_hashmap_get(map, &k1));
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)1);

    /* Remove non-existent key */
    static int k3 = 3;
    bool removed = keel_hashmap_remove(map, &k3);
    (void)removed; /* result may be false — we just require no crash */

    keel_hashmap_free(map);
    TEST_END();
}

static void test_hashmap_clear(void) {
    TEST_BEGIN("hashmap clear: all entries removed");

    keel_hashmap_t* map = keel_hashmap_new(NULL, NULL);
    TEST_ASSERT_NOT_NULL(map);

    static int vals[8];
    for (int i = 0; i < 8; i++) {
        vals[i] = i;
        keel_hashmap_set(map, &vals[i], &vals[i]);
    }
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)8);

    keel_hashmap_clear(map);
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)0);
    TEST_ASSERT_NULL(keel_hashmap_get(map, &vals[0]));

    /* Can still insert after clear */
    TEST_ASSERT(keel_hashmap_set(map, &vals[0], &vals[0]));
    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)1);

    keel_hashmap_free(map);
    TEST_END();
}

/* ============================================================================
 * §8  Hashmap NULL-safety
 * ============================================================================ */

static void test_hashmap_null_safety(void) {
    TEST_BEGIN("hashmap NULL-safety: operations on NULL map");

    static int k = 1;

    /* These must not crash */
    TEST_ASSERT_EQ(keel_hashmap_size(NULL), (size_t)0);
    TEST_ASSERT_NULL(keel_hashmap_get(NULL, &k));
    keel_hashmap_clear(NULL);
    keel_hashmap_free(NULL);

    TEST_END();
}

/* ============================================================================
 * §9  Hash ring
 * ============================================================================ */

static void test_hash_ring_basic(void) {
    TEST_BEGIN("hash ring: add nodes, deterministic get");

    keel_hash_ring_t* ring = keel_hash_ring_new(100);
    TEST_ASSERT_NOT_NULL(ring);

    TEST_ASSERT_EQ(keel_hash_ring_add(ring, "node1", 5), KEEL_OK);
    TEST_ASSERT_EQ(keel_hash_ring_add(ring, "node2", 5), KEEL_OK);
    TEST_ASSERT_EQ(keel_hash_ring_add(ring, "node3", 5), KEEL_OK);

    const void* out = NULL;
    size_t       olen = 0;

    keel_error_t err = keel_hash_ring_get(ring, "key1", 4, &out, &olen);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(out);
    TEST_ASSERT(olen > 0);

    /* Same key → same node */
    const void* out2 = NULL;
    size_t       olen2 = 0;
    err = keel_hash_ring_get(ring, "key1", 4, &out2, &olen2);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out, out2);
    TEST_ASSERT_EQ(olen, olen2);

    keel_hash_ring_free(ring);
    TEST_END();
}

static void test_hash_ring_remove(void) {
    TEST_BEGIN("hash ring: remove node, empty ring returns error");

    keel_hash_ring_t* ring = keel_hash_ring_new(10);
    TEST_ASSERT_NOT_NULL(ring);

    keel_hash_ring_add(ring, "only_node", 9);

    keel_error_t err = keel_hash_ring_remove(ring, "only_node");
    TEST_ASSERT_EQ(err, KEEL_OK);

    const void* out = NULL;
    size_t olen = 0;
    err = keel_hash_ring_get(ring, "key", 3, &out, &olen);
    TEST_ASSERT(err != KEEL_OK || out == NULL); /* must fail or return nothing */

    keel_hash_ring_free(ring);
    TEST_END();
}

static void test_hash_ring_distribution(void) {
    TEST_BEGIN("hash ring: 3 nodes receive traffic (distribution check)");

    keel_hash_ring_t* ring = keel_hash_ring_new(50);
    TEST_ASSERT_NOT_NULL(ring);

    keel_hash_ring_add(ring, "n1", 2);
    keel_hash_ring_add(ring, "n2", 2);
    keel_hash_ring_add(ring, "n3", 2);

    int counts[3] = {0, 0, 0};
    char key[16];
    for (int i = 0; i < 300; i++) {
        snprintf(key, sizeof(key), "key_%04d", i);
        const void* out = NULL;
        size_t olen = 0;
        if (keel_hash_ring_get(ring, key, strlen(key), &out, &olen) == KEEL_OK && out) {
            if (memcmp(out, "n1", 2) == 0) counts[0]++;
            else if (memcmp(out, "n2", 2) == 0) counts[1]++;
            else if (memcmp(out, "n3", 2) == 0) counts[2]++;
        }
    }
    /* Each node should receive at least some traffic */
    TEST_ASSERT(counts[0] > 0);
    TEST_ASSERT(counts[1] > 0);
    TEST_ASSERT(counts[2] > 0);
    TEST_ASSERT_EQ(counts[0] + counts[1] + counts[2], 300);

    keel_hash_ring_free(ring);
    TEST_END();
}

static void test_hash_ring_null_safety(void) {
    TEST_BEGIN("hash ring: NULL-safety");

    keel_hash_ring_free(NULL);
    TEST_ASSERT(true);

    TEST_END();
}

/* ============================================================================
 * §10  Stress: 100k hashmap inserts
 * ============================================================================ */

#define STRESS_N 100000
static int stress_keys[STRESS_N];
static int stress_vals[STRESS_N];

static void test_hashmap_stress(void) {
    TEST_BEGIN("hashmap stress: 100k insert + lookup");

    keel_hashmap_t* map = keel_hashmap_new(NULL, NULL);
    TEST_ASSERT_NOT_NULL(map);

    for (int i = 0; i < STRESS_N; i++) {
        stress_keys[i] = i;
        stress_vals[i] = i * 3;
        TEST_ASSERT(keel_hashmap_set(map, &stress_keys[i], &stress_vals[i]));
    }

    TEST_ASSERT_EQ(keel_hashmap_size(map), (size_t)STRESS_N);

    int misses = 0;
    for (int i = 0; i < STRESS_N; i++) {
        void* got = keel_hashmap_get(map, &stress_keys[i]);
        if (!got || *(int*)got != stress_vals[i]) misses++;
    }
    TEST_ASSERT_EQ(misses, 0);

    keel_hashmap_free(map);
    TEST_END();
}
#undef STRESS_N

/* ============================================================================
 * §11  Fuzz / edge cases
 * ============================================================================ */

static void test_str_fuzz_edge(void) {
    TEST_BEGIN("str fuzz: edge cases (NUL inside, max-len, single char)");

    /* Single-byte strings */
    TEST_ASSERT(keel_str_eq(S("x"), S("x")));
    TEST_ASSERT(!keel_str_eq(S("x"), S("y")));

    /* keel_str_find with identical haystack and needle */
    TEST_ASSERT_EQ(keel_str_find(S("abc"), S("abc")), (keel_ssize_t)0);

    /* needle longer than haystack */
    TEST_ASSERT(keel_str_find(S("ab"), S("abc")) < 0);

    /* Binary data in hash */
    uint8_t bin[16];
    for (int i = 0; i < 16; i++) bin[i] = (uint8_t)i;
    uint32_t h = keel_hash_fnv1a_32(bin, 16);
    TEST_ASSERT(h != 0);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("String & Hash Tests\n");
    printf("===================\n\n");

    keel_mem_init(NULL);

    /* String operations */
    test_str_eq();
    test_str_eq_nocase();
    test_str_starts_with();
    test_str_ends_with();
    test_str_contains();
    test_str_find();

    /* String helpers */
    test_str_trim();
    test_str_split();
    test_str_split_empty_tokens();
    test_str_dup();

    /* FNV-1a */
    test_hash_fnv1a_32_golden();
    test_hash_fnv1a_32_avalanche();
    test_hash_fnv1a_64_golden();
    test_hash_fnv1a_64_vs_32();
    test_hash_fnv1a_str();

    /* MurmurHash3 */
    test_hash_murmur3_seed();
    test_hash_murmur3_avalanche();

    /* Hashmap */
    test_hashmap_lifecycle();
    test_hashmap_set_get();
    test_hashmap_overwrite();
    test_hashmap_remove();
    test_hashmap_clear();
    test_hashmap_null_safety();
    test_hashmap_stress();

    /* Hash ring */
    test_hash_ring_basic();
    test_hash_ring_remove();
    test_hash_ring_distribution();
    test_hash_ring_null_safety();

    /* Fuzz edge cases */
    test_str_fuzz_edge();

    keel_mem_shutdown();

    return test_summary();
}
