/**
 * @file test_string_view.c
 * @brief Unit tests for the zero-copy string-view API.
 *
 * String views are the primary zero-allocation text representation used through
 * parser, router, and config paths. This suite validates construction, equality,
 * slicing, trimming, search, split iteration, and hash stability to ensure the
 * foundation stays safe for callers that rely on pointer/length semantics with
 * no implicit null terminator.
 */

#include "test_utils.h"
#include "keel/util/string_view.h"

#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Construction Tests
 * ============================================================================ */

static void test_string_view_create(void) {
    printf("  Testing string view creation...\n");
    
    /* Create from pointer and length */
    const char* data = "Hello, World!";
    keel_str_view_t view = keel_str_view(data, 5);
    
    TEST_ASSERT(view.data == data);
    TEST_ASSERT(view.len == 5);
    
    /* Create empty view */
    keel_str_view_t empty = keel_str_view_empty();
    TEST_ASSERT(empty.data == NULL);
    TEST_ASSERT(empty.len == 0);
    TEST_ASSERT(keel_str_view_is_empty(empty));
    
    /* Create from C string */
    keel_str_view_t cstr = keel_str_view_cstr("Hello");
    TEST_ASSERT(cstr.len == 5);
    TEST_ASSERT(!keel_str_view_is_empty(cstr));
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Comparison Tests
 * ============================================================================ */

static void test_string_view_comparison(void) {
    printf("  Testing string view comparison...\n");
    
    keel_str_view_t hello = keel_str_view_cstr("Hello");
    keel_str_view_t hello2 = keel_str_view_cstr("Hello");
    keel_str_view_t world = keel_str_view_cstr("World");
    keel_str_view_t hello_lower = keel_str_view_cstr("hello");
    
    /* Exact equality */
    TEST_ASSERT(keel_str_view_eq(hello, hello2));
    TEST_ASSERT(!keel_str_view_eq(hello, world));
    
    /* Case-insensitive equality */
    TEST_ASSERT(keel_str_view_eq_nocase(hello, hello_lower));
    TEST_ASSERT(!keel_str_view_eq_nocase(hello, world));
    
    /* C string comparison */
    TEST_ASSERT(keel_str_view_eq_cstr(hello, "Hello"));
    TEST_ASSERT(!keel_str_view_eq_cstr(hello, "World"));
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Slicing Tests
 * ============================================================================ */

static void test_string_view_slicing(void) {
    printf("  Testing string view slicing...\n");
    
    keel_str_view_t full = keel_str_view_cstr("Hello, World!");
    
    /* Substring with length */
    keel_str_view_t sub = keel_str_view_substr_len(full, 0, 5);
    TEST_ASSERT(keel_str_view_eq_cstr(sub, "Hello"));
    
    sub = keel_str_view_substr_len(full, 7, 5);
    TEST_ASSERT(keel_str_view_eq_cstr(sub, "World"));
    
    /* Prefix/suffix slicing */
    keel_str_view_t prefix = keel_str_view_prefix(full, 5);
    TEST_ASSERT(keel_str_view_eq_cstr(prefix, "Hello"));
    
    keel_str_view_t suffix = keel_str_view_suffix(full, 6);
    TEST_ASSERT(keel_str_view_eq_cstr(suffix, "World!"));
    
    /* Remove prefix */
    keel_str_view_t after = keel_str_view_remove_prefix(full, 7);
    TEST_ASSERT(keel_str_view_eq_cstr(after, "World!"));
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Prefix/Suffix Tests
 * ============================================================================ */

static void test_string_view_prefix_suffix(void) {
    printf("  Testing string view prefix/suffix...\n");
    
    keel_str_view_t str = keel_str_view_cstr("SELECT * FROM users");
    
    /* Starts with */
    TEST_ASSERT(keel_str_view_starts_with_cstr(str, "SELECT"));
    TEST_ASSERT(!keel_str_view_starts_with_cstr(str, "INSERT"));
    
    /* Ends with */
    TEST_ASSERT(keel_str_view_ends_with(str, keel_str_view_cstr("users")));
    TEST_ASSERT(!keel_str_view_ends_with(str, keel_str_view_cstr("orders")));
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Trimming Tests
 * ============================================================================ */

static void test_string_view_trim(void) {
    printf("  Testing string view trimming...\n");
    
    keel_str_view_t padded = keel_str_view_cstr("  Hello World  ");
    
    keel_str_view_t trimmed = keel_str_view_trim(padded);
    TEST_ASSERT(keel_str_view_eq_cstr(trimmed, "Hello World"));
    
    keel_str_view_t left = keel_str_view_trim_left(padded);
    TEST_ASSERT(keel_str_view_eq_cstr(left, "Hello World  "));
    
    keel_str_view_t right = keel_str_view_trim_right(padded);
    TEST_ASSERT(keel_str_view_eq_cstr(right, "  Hello World"));
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Search Tests
 * ============================================================================ */

static void test_string_view_search(void) {
    printf("  Testing string view search...\n");
    
    keel_str_view_t str = keel_str_view_cstr("Hello, World, Hello!");
    
    /* Find substring */
    size_t pos = keel_str_view_find(str, keel_str_view_cstr("World"));
    TEST_ASSERT(pos == 7);
    
    /* Find character */
    pos = keel_str_view_find_char(str, ',');
    TEST_ASSERT(pos == 5);
    
    /* Find non-existent should return -1 */
    pos = keel_str_view_find(str, keel_str_view_cstr("Foo"));
    TEST_ASSERT(pos == (size_t)-1);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Split Tests
 * ============================================================================ */

static void test_string_view_split(void) {
    printf("  Testing string view split...\n");
    
    keel_str_view_t csv = keel_str_view_cstr("apple,banana,cherry");
    
    keel_str_view_split_t split = keel_str_view_split_init(csv, ',');
    keel_str_view_t token;
    int count = 0;
    
    while (keel_str_view_split_next(&split, &token)) {
        count++;
        if (count == 1) TEST_ASSERT(keel_str_view_eq_cstr(token, "apple"));
        if (count == 2) TEST_ASSERT(keel_str_view_eq_cstr(token, "banana"));
        if (count == 3) TEST_ASSERT(keel_str_view_eq_cstr(token, "cherry"));
    }
    
    TEST_ASSERT(count == 3);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Hash Tests
 * ============================================================================ */

static void test_string_view_hash(void) {
    printf("  Testing string view hash...\n");
    
    keel_str_view_t str1 = keel_str_view_cstr("Hello");
    keel_str_view_t str2 = keel_str_view_cstr("Hello");
    keel_str_view_t str3 = keel_str_view_cstr("World");
    
    /* Same strings should have same hash */
    uint64_t h1 = keel_str_view_hash(str1);
    uint64_t h2 = keel_str_view_hash(str2);
    TEST_ASSERT(h1 == h2);
    
    /* Different strings should (probably) have different hash */
    uint64_t h3 = keel_str_view_hash(str3);
    TEST_ASSERT(h1 != h3);
    
    printf("    PASSED\n");
}

/* ============================================================================
 * Case-insensitive C-string comparison Tests
 * ============================================================================ */

static void test_string_view_eq_cstr_nocase(void) {
    printf("  Testing string view eq_cstr_nocase...\n");

    keel_str_view_t hello = keel_str_view_cstr("Hello");
    keel_str_view_t empty = keel_str_view_empty();

    /* NULL cstr treated as empty */
    TEST_ASSERT(keel_str_view_eq_cstr_nocase(empty, NULL));
    TEST_ASSERT(!keel_str_view_eq_cstr_nocase(hello, NULL));

    /* Case-insensitive match */
    TEST_ASSERT(keel_str_view_eq_cstr_nocase(hello, "HELLO"));
    TEST_ASSERT(keel_str_view_eq_cstr_nocase(hello, "hello"));
    TEST_ASSERT(!keel_str_view_eq_cstr_nocase(hello, "World"));

    printf("    PASSED\n");
}

/* ============================================================================
 * Integer and float conversion Tests
 * ============================================================================ */

static void test_string_view_to_int64(void) {
    printf("  Testing string view to_int64...\n");

    int64_t val = 0;

    /* NULL out pointer */
    TEST_ASSERT(!keel_str_view_to_int64(keel_str_view_cstr("1"), NULL));

    /* Empty string */
    TEST_ASSERT(!keel_str_view_to_int64(keel_str_view_empty(), &val));

    /* Oversized string (> 31 chars) */
    TEST_ASSERT(!keel_str_view_to_int64(
        keel_str_view_cstr("12345678901234567890123456789012"), &val));

    /* Valid positive integer */
    TEST_ASSERT(keel_str_view_to_int64(keel_str_view_cstr("42"), &val));
    TEST_ASSERT(val == 42);

    /* Valid negative integer */
    TEST_ASSERT(keel_str_view_to_int64(keel_str_view_cstr("-99"), &val));
    TEST_ASSERT(val == -99);

    /* Trailing garbage */
    TEST_ASSERT(!keel_str_view_to_int64(keel_str_view_cstr("12abc"), &val));

    printf("    PASSED\n");
}

static void test_string_view_to_uint64(void) {
    printf("  Testing string view to_uint64...\n");

    uint64_t val = 0;

    /* NULL out pointer */
    TEST_ASSERT(!keel_str_view_to_uint64(keel_str_view_cstr("1"), NULL));

    /* Empty string */
    TEST_ASSERT(!keel_str_view_to_uint64(keel_str_view_empty(), &val));

    /* Negative number rejected */
    TEST_ASSERT(!keel_str_view_to_uint64(keel_str_view_cstr("-1"), &val));

    /* Oversized string */
    TEST_ASSERT(!keel_str_view_to_uint64(
        keel_str_view_cstr("12345678901234567890123456789012"), &val));

    /* Valid value */
    TEST_ASSERT(keel_str_view_to_uint64(keel_str_view_cstr("100"), &val));
    TEST_ASSERT(val == 100U);

    /* Trailing garbage */
    TEST_ASSERT(!keel_str_view_to_uint64(keel_str_view_cstr("5x"), &val));

    printf("    PASSED\n");
}

static void test_string_view_to_double(void) {
    printf("  Testing string view to_double...\n");

    double val = 0.0;

    /* NULL out pointer */
    TEST_ASSERT(!keel_str_view_to_double(keel_str_view_cstr("1.0"), NULL));

    /* Empty string */
    TEST_ASSERT(!keel_str_view_to_double(keel_str_view_empty(), &val));

    /* Oversized string (> 63 chars) */
    TEST_ASSERT(!keel_str_view_to_double(keel_str_view_cstr(
        "1234567890123456789012345678901234567890123456789012345678901234"), &val));

    /* Valid value */
    TEST_ASSERT(keel_str_view_to_double(keel_str_view_cstr("3.14"), &val));
    TEST_ASSERT(val > 3.13 && val < 3.15);

    /* Trailing garbage */
    TEST_ASSERT(!keel_str_view_to_double(keel_str_view_cstr("1.0x"), &val));

    printf("    PASSED\n");
}

/* ============================================================================
 * Case-insensitive hash Tests
 * ============================================================================ */

static void test_string_view_hash_nocase(void) {
    printf("  Testing string view hash_nocase...\n");

    keel_str_view_t lower = keel_str_view_cstr("hello");
    keel_str_view_t upper = keel_str_view_cstr("HELLO");
    keel_str_view_t mixed = keel_str_view_cstr("HeLLo");
    keel_str_view_t other = keel_str_view_cstr("world");

    uint64_t h1 = keel_str_view_hash_nocase(lower);
    uint64_t h2 = keel_str_view_hash_nocase(upper);
    uint64_t h3 = keel_str_view_hash_nocase(mixed);
    uint64_t h4 = keel_str_view_hash_nocase(other);

    /* All case variants produce the same hash */
    TEST_ASSERT(h1 == h2);
    TEST_ASSERT(h1 == h3);

    /* Different word produces different hash */
    TEST_ASSERT(h1 != h4);

    printf("    PASSED\n");
}

/* ============================================================================
 * Split edge-case Tests
 * ============================================================================ */

static void test_string_view_split_edge_cases(void) {
    printf("  Testing string view split edge cases...\n");

    keel_str_view_t token;

    /* NULL split pointer returns false */
    TEST_ASSERT(!keel_str_view_split_next(NULL, &token));

    /* NULL token pointer returns false */
    keel_str_view_split_t split = keel_str_view_split_init(keel_str_view_cstr("a"), ',');
    TEST_ASSERT(!keel_str_view_split_next(&split, NULL));

    /* Exhausted iterator (remaining.data == NULL) returns false */
    keel_str_view_split_t done = keel_str_view_split_init(keel_str_view_empty(), ',');
    done.remaining.data = NULL;
    TEST_ASSERT(!keel_str_view_split_next(&done, &token));

    printf("    PASSED\n");
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Running string view tests...\n");
    
    test_string_view_create();
    test_string_view_comparison();
    test_string_view_slicing();
    test_string_view_prefix_suffix();
    test_string_view_trim();
    test_string_view_search();
    test_string_view_split();
    test_string_view_hash();
    test_string_view_eq_cstr_nocase();
    test_string_view_to_int64();
    test_string_view_to_uint64();
    test_string_view_to_double();
    test_string_view_hash_nocase();
    test_string_view_split_edge_cases();
    
    printf("All string view tests passed!\n");
    return 0;
}
