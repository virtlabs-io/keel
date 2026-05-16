/**
 * @file util.h
 * @brief Umbrella declarations for KEEL's small, dependency-light utility APIs.
 *
 * This header exposes the low-level helpers that do not fit cleanly into a
 * larger subsystem but are reused widely across the codebase: immutable string
 * helpers, growable byte buffers, non-cryptographic hash functions, a minimal
 * open-addressed hashmap, a simplified consistent-hash ring, and monotonic/
 * realtime time primitives.
 *
 * The design goal is portability and predictable behavior rather than maximum
 * abstraction. Most types here are intentionally tiny and explicit so call
 * sites can choose whether ownership stays borrowed, copied, or transferred.
 * More specialized facilities such as streaming xxHash and zero-copy string
 * views live in dedicated headers to keep this umbrella contract broadly useful
 * without forcing every consumer to include everything else.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_UTIL_H
#define KEEL_UTIL_H

#include "keel_types.h"
#include "keel_error.h"

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * String Utilities
 * ============================================================================ */

/**
 * @brief Compare two immutable string slices for byte-for-byte equality.
 *
 * This is the cheapest equality test for KEEL's owned-or-borrowed string type:
 * it first checks lengths and then performs a raw byte comparison without any
 * locale, collation, or null-termination rules.
 *
 * @param a First string slice.
 * @param b Second string slice.
 * @return true when both slices have the same length and identical bytes.
 */
bool keel_str_eq(keel_str_t a, keel_str_t b);

/**
 * @brief Compare two string slices case-insensitively using ASCII folding.
 *
 * The implementation lowercases each byte with `tolower()` and is therefore
 * suitable for protocol tokens, identifiers, and configuration keys that are
 * expected to live in the ASCII subset. It is not intended as full Unicode
 * case folding.
 *
 * @param a First string slice.
 * @param b Second string slice.
 * @return true when both slices are equal under ASCII case folding.
 */
bool keel_str_eq_nocase(keel_str_t a, keel_str_t b);

/**
 * @brief Test whether a string begins with a given prefix.
 *
 * @param str Candidate string.
 * @param prefix Required prefix.
 * @return true when `prefix` fits entirely at the start of `str`.
 */
bool keel_str_starts_with(keel_str_t str, keel_str_t prefix);

/**
 * @brief Test whether a string ends with a given suffix.
 *
 * @param str Candidate string.
 * @param suffix Required suffix.
 * @return true when `suffix` fits entirely at the end of `str`.
 */
bool keel_str_ends_with(keel_str_t str, keel_str_t suffix);

/**
 * @brief Test whether a string contains a given substring.
 *
 * @param haystack String to search.
 * @param needle Substring to locate.
 * @return true when `needle` appears anywhere in `haystack`.
 */
bool keel_str_contains(keel_str_t haystack, keel_str_t needle);

/**
 * @brief Find the first exact byte match of a substring.
 *
 * The current implementation uses a straightforward forward scan. That keeps
 * the helper small and branch-predictable for the short protocol and SQL token
 * strings seen in KEEL. More advanced substring algorithms would add setup
 * cost and code size for little observed benefit here.
 *
 * @param haystack String to search.
 * @param needle Substring to locate.
 * @return Zero-based byte offset of the first match, or `-1` when absent.
 */
keel_ssize_t keel_str_find(keel_str_t haystack, keel_str_t needle);

/**
 * @brief Remove leading and trailing ASCII whitespace without copying.
 *
 * The returned slice still aliases the original storage.
 *
 * @param str String to trim.
 * @return Trimmed view into the original bytes.
 */
keel_str_t keel_str_trim(keel_str_t str);

/**
 * @brief Split a string incrementally using an in-place iterator state.
 *
 * Instead of allocating an array of tokens, this helper mutates `remaining` to
 * point at the unconsumed suffix and writes the next token into `part`. The
 * approach matches KEEL's preference for streaming parsers and bounded
 * allocations.
 *
 * @param[in,out] remaining Remaining substring to consume.
 * @param delim Delimiter byte.
 * @param[out] part Next token when one is available.
 * @return true when a token was produced, false when iteration is exhausted.
 */
bool keel_str_split(keel_str_t* remaining, char delim, keel_str_t* part);

/**
 * @brief Duplicate a string slice into a null-terminated heap buffer.
 *
 * This is the escape hatch from KEEL's slice-based APIs when a caller needs an
 * owning C string for external libraries or longer-lived storage.
 *
 * @param str Source slice.
 * @return Newly allocated null-terminated buffer, or `NULL` on allocation
 *         failure.
 */
char* keel_str_dup(keel_str_t str);

/* ============================================================================
 * Buffer Utilities
 * ============================================================================ */

/* Opaque buffer type */
typedef struct keel_buffer keel_buffer_t;

/**
 * @brief Allocate a new growable byte buffer with the default initial capacity.
 *
 * @return Newly allocated buffer, or `NULL` on allocation failure.
 */
keel_buffer_t* keel_buffer_new(void);

/**
 * @brief Allocate a new growable byte buffer with a caller-provided capacity.
 *
 * @param capacity Initial byte capacity to reserve.
 * @return Newly allocated buffer, or `NULL` on allocation failure.
 */
keel_buffer_t* keel_buffer_new_with_capacity(size_t capacity);

/**
 * @brief Create a new buffer initialized with a copy of existing bytes.
 *
 * @param data Source bytes to copy, or `NULL` when `len` is zero.
 * @param len Number of bytes to copy.
 * @return Newly allocated buffer containing the copied bytes, or `NULL` on
 *         allocation failure.
 */
keel_buffer_t* keel_buffer_from_data(const void* data, size_t len);

/**
 * @brief Destroy a buffer and release its owned storage.
 *
 * @param buf Buffer to destroy. `NULL` is ignored.
 * @return
 */
void keel_buffer_free(keel_buffer_t* buf);

/**
 * @brief Get current length of data in buffer
 */
size_t keel_buffer_len(const keel_buffer_t* buf);

/**
 * @brief Get buffer capacity
 */
size_t keel_buffer_capacity(const keel_buffer_t* buf);

/**
 * @brief Get available space in buffer
 */
size_t keel_buffer_available(const keel_buffer_t* buf);

/**
 * @brief Check if buffer is empty
 */
bool keel_buffer_empty(const keel_buffer_t* buf);

/**
 * @brief Get pointer to buffer data
 */
uint8_t* keel_buffer_data(keel_buffer_t* buf);

/**
 * @brief Get const pointer to buffer data
 */
const uint8_t* keel_buffer_data_const(const keel_buffer_t* buf);

/**
 * @brief Clear buffer (reset length to 0)
 */
void keel_buffer_clear(keel_buffer_t* buf);

/**
 * @brief Ensure the buffer can hold at least the requested total capacity.
 *
 * This does not change the current logical length.
 *
 * @param buf Buffer to grow.
 * @param n Minimum capacity in bytes.
 * @return true on success, false on allocation failure or invalid input.
 */
bool keel_buffer_reserve(keel_buffer_t* buf, size_t n);

/**
 * @brief Append arbitrary bytes to the end of a buffer.
 *
 * @param buf Destination buffer.
 * @param data Bytes to append.
 * @param len Number of bytes to append.
 * @return true when the bytes were appended successfully.
 */
bool keel_buffer_write(keel_buffer_t* buf, const void* data, size_t len);

/**
 * @brief Write a single byte
 */
bool keel_buffer_write_u8(keel_buffer_t* buf, uint8_t val);

/**
 * @brief Write uint16 in big-endian
 */
bool keel_buffer_write_u16_be(keel_buffer_t* buf, uint16_t val);

/**
 * @brief Write uint32 in big-endian
 */
bool keel_buffer_write_u32_be(keel_buffer_t* buf, uint32_t val);

/**
 * @brief Write uint64 in big-endian
 */
bool keel_buffer_write_u64_be(keel_buffer_t* buf, uint64_t val);

/**
 * @brief Read uint8 from buffer position
 */
uint8_t keel_buffer_read_u8_at(const keel_buffer_t* buf, size_t pos);

/**
 * @brief Read uint16 big-endian from buffer position
 */
uint16_t keel_buffer_read_u16_be_at(const keel_buffer_t* buf, size_t pos);

/**
 * @brief Read uint32 big-endian from buffer position
 */
uint32_t keel_buffer_read_u32_be_at(const keel_buffer_t* buf, size_t pos);

/* ============================================================================
 * Hash Functions
 * ============================================================================ */

/**
 * @brief Compute the classic 32-bit FNV-1a hash.
 *
 * FNV-1a is tiny and dependency-free, making it a good default for small keys,
 * configuration tokens, and internal lookup structures where cryptographic
 * resistance is unnecessary.
 *
 * @param data Input bytes.
 * @param len Length in bytes.
 * @return 32-bit hash value.
 */
uint32_t keel_hash_fnv1a_32(const void* data, size_t len);

/**
 * @brief Compute the 64-bit FNV-1a hash variant.
 *
 * @param data Input bytes.
 * @param len Length in bytes.
 * @return 64-bit hash value.
 */
uint64_t keel_hash_fnv1a_64(const void* data, size_t len);

/**
 * @brief FNV-1a hash for string view
 */
uint32_t keel_hash_fnv1a_str(keel_str_t str);

/**
 * @brief Compute MurmurHash3 x86 32-bit for better avalanche on mixed keys.
 *
 * MurmurHash3 costs slightly more than FNV-1a but tends to distribute better
 * for structured keys. KEEL keeps both options because some call sites prefer
 * tiny code size while others care more about dispersion.
 *
 * @param data Input bytes.
 * @param len Length in bytes.
 * @param seed Caller-chosen seed to vary hash domains.
 * @return 32-bit hash value.
 */
uint32_t keel_hash_murmur3_32(const void* data, size_t len, uint32_t seed);

/* ============================================================================
 * Hash Map
 * ============================================================================ */

typedef struct keel_hashmap keel_hashmap_t;

/**
 * @brief Hash function type for hashmap
 */
typedef uint64_t (*keel_hash_fn)(const void* key);

/**
 * @brief Equality function type for hashmap
 */
typedef bool (*keel_eq_fn)(const void* a, const void* b);

/**
 * @brief Allocate a small generic hashmap.
 *
 * The table stores opaque key/value pointers and relies on caller-supplied hash
 * and equality callbacks. When callbacks are omitted, pointer identity is used.
 * The implementation is intentionally simple and optimized for small utility
 * workloads rather than fully featured container semantics.
 *
 * @param hash_fn Hash callback, or `NULL` for pointer hashing.
 * @param eq_fn Equality callback, or `NULL` for pointer equality.
 * @return Newly allocated hashmap, or `NULL` on allocation failure.
 */
keel_hashmap_t* keel_hashmap_new(keel_hash_fn hash_fn, keel_eq_fn eq_fn);

/**
 * @brief Free hashmap
 */
void keel_hashmap_free(keel_hashmap_t* map);

/**
 * @brief Get value from hashmap
 */
void* keel_hashmap_get(const keel_hashmap_t* map, const void* key);

/**
 * @brief Set value in hashmap
 */
bool keel_hashmap_set(keel_hashmap_t* map, void* key, void* value);

/**
 * @brief Remove key from hashmap
 */
bool keel_hashmap_remove(keel_hashmap_t* map, const void* key);

/**
 * @brief Get number of entries
 */
size_t keel_hashmap_size(const keel_hashmap_t* map);

/**
 * @brief Clear all entries
 */
void keel_hashmap_clear(keel_hashmap_t* map);

/* ============================================================================
 * Consistent Hash Ring
 * ============================================================================ */

/**
 * @brief Opaque hash ring type
 */
typedef struct keel_hash_ring keel_hash_ring_t;

/**
 * @brief Create a new hash ring
 */
keel_hash_ring_t* keel_hash_ring_new(size_t virtual_nodes);

/**
 * @brief Free hash ring
 */
void keel_hash_ring_free(keel_hash_ring_t* ring);

/**
 * @brief Register a node identifier with the simplified consistent-hash ring.
 *
 * KEEL's current ring implementation stores node identifiers and chooses a
 * target by hashing the key and taking a modulo over the node count. The API is
 * intentionally shaped like a consistent-hash ring so the routing strategy can
 * be improved later without changing most callers.
 *
 * @param ring Ring to modify.
 * @param node_id Node identifier bytes to copy.
 * @param len Length of `node_id`.
 * @return `KEEL_OK` on success or an error code on invalid input/allocation
 *         failure.
 */
keel_error_t keel_hash_ring_add(keel_hash_ring_t* ring, const char* node_id, size_t len);

/**
 * @brief Remove a node from the ring
 */
keel_error_t keel_hash_ring_remove(keel_hash_ring_t* ring, const char* node_id);

/**
 * @brief Resolve a key to one registered node.
 *
 * @param ring Ring to query.
 * @param key Key bytes to hash.
 * @param key_len Length of `key`.
 * @param[out] node_data Pointer to the selected node identifier.
 * @param[out] node_len Length of the selected identifier.
 * @return `KEEL_OK` on success or an error code when the ring is invalid or
 *         empty.
 */
keel_error_t keel_hash_ring_get(keel_hash_ring_t* ring, const char* key, size_t key_len,
                               const void** node_data, size_t* node_len);

/* ============================================================================
 * Time Utilities
 * ============================================================================ */

/**
 * @brief Read the current monotonic clock in nanoseconds.
 *
 * Monotonic time is suitable for intervals, deadlines, and performance
 * measurement because it does not jump with wall-clock corrections.
 *
 * @return Monotonic timestamp in nanoseconds, or `0` when the underlying clock
 *         read fails.
 */
keel_time_t keel_time_now(void);

/**
 * @brief Read current monotonic time in milliseconds (coarse resolution).
 *
 * Uses `CLOCK_MONOTONIC_COARSE` for reduced vDSO overhead.  Suitable for
 * timeout accounting (e.g. waiter expiry, backoff tracking) where ~4 ms
 * granularity is acceptable.  Not suitable for sub-millisecond latency
 * measurement — use `keel_time_now()` for that.
 *
 * @return Milliseconds since an arbitrary epoch, or `0` on clock failure.
 */
uint64_t keel_time_now_ms(void);

/**
 * @brief Read the current realtime clock in nanoseconds since the Unix epoch.
 *
 * This is appropriate for user-visible timestamps and log records, but not for
 * elapsed-time measurement because NTP or manual clock changes can move it.
 *
 * @return Realtime timestamp in nanoseconds, or `0` when the clock read fails.
 */
keel_time_t keel_time_realtime(void);

/**
 * @brief Compute difference between two times
 */
keel_duration_t keel_time_diff(keel_time_t start, keel_time_t end);

/**
 * @brief Add duration to time
 */
keel_time_t keel_time_add(keel_time_t t, keel_duration_t d);

/**
 * @brief Check if time a is before time b
 */
bool keel_time_before(keel_time_t a, keel_time_t b);

/**
 * @brief Check if time a is after time b
 */
bool keel_time_after(keel_time_t a, keel_time_t b);

/**
 * @brief Format time as ISO 8601 string
 */
size_t keel_time_format_iso8601(keel_time_t t, char* buf, size_t len);

/**
 * @brief Format time as local date-time string
 */
size_t keel_time_format_local(keel_time_t t, char* buf, size_t len);

/**
 * @brief Format duration as human-readable string
 */
size_t keel_duration_format(keel_duration_t d, char* buf, size_t len);

/**
 * @brief Parse a human-readable duration string.
 *
 * Supported suffixes currently include `ns`, `us`, `µs`, `ms`, `s`, `m`,
 * `min`, `h`, and `hr`. A bare number defaults to seconds to match common
 * configuration expectations.
 *
 * @param str Duration string to parse.
 * @param[out] out Parsed duration in nanoseconds.
 * @return true on success, false on syntax or range failure.
 */
bool keel_duration_parse(const char* str, keel_duration_t* out);

/**
 * @brief Convert duration to nanoseconds
 */
int64_t keel_duration_to_ns(keel_duration_t d);

/**
 * @brief Convert duration to microseconds
 */
int64_t keel_duration_to_us(keel_duration_t d);

/**
 * @brief Create duration from nanoseconds
 */
keel_duration_t keel_duration_ns(int64_t ns);

/**
 * @brief Create duration from microseconds
 */
keel_duration_t keel_duration_us(int64_t us);

/**
 * @brief Create duration from milliseconds
 */
keel_duration_t keel_duration_ms(int64_t ms);

/**
 * @brief Create duration from seconds
 */
keel_duration_t keel_duration_sec(int64_t sec);

/**
 * @brief Create duration from minutes
 */
keel_duration_t keel_duration_min(int64_t min);

/**
 * @brief Create duration from hours
 */
keel_duration_t keel_duration_hr(int64_t hr);

/**
 * @brief Convert duration to seconds
 */
double keel_duration_to_sec(keel_duration_t d);

/**
 * @brief Convert duration to milliseconds
 */
int64_t keel_duration_to_ms(keel_duration_t d);

/**
 * @brief Block the current thread for the requested duration.
 *
 * This is a convenience wrapper around `nanosleep()` for synchronous code. It
 * should not be confused with event-driven timers used in the I/O hot path.
 *
 * @param d Duration to sleep. Non-positive durations are ignored.
 * @return
 */
void keel_sleep(keel_duration_t d);

/**
 * @brief Sleep for milliseconds
 */
void keel_sleep_ms(int64_t ms);

/**
 * @brief Sleep for microseconds
 */
void keel_sleep_us(int64_t us);

/**
 * @brief Sleep for nanoseconds
 */
void keel_sleep_ns(int64_t ns);

/* Stopwatch for measuring elapsed time */
typedef struct keel_stopwatch {
    keel_time_t start;
    keel_time_t elapsed;
    bool       running;
} keel_stopwatch_t;

/**
 * @brief Start or restart a simple elapsed-time stopwatch.
 *
 * The stopwatch stores a monotonic start time plus an accumulated elapsed
 * duration so callers can measure code sections without dealing with raw clock
 * arithmetic.
 *
 * @param sw Stopwatch to start.
 * @return
 */
void keel_stopwatch_start(keel_stopwatch_t* sw);

/**
 * @brief Stop a running stopwatch and accumulate the elapsed interval.
 *
 * @param sw Stopwatch to stop.
 * @return
 */
void keel_stopwatch_stop(keel_stopwatch_t* sw);

/**
 * @brief Reset a stopwatch to the zero, not-running state.
 *
 * @param sw Stopwatch to reset.
 * @return
 */
void keel_stopwatch_reset(keel_stopwatch_t* sw);

/**
 * @brief Read the current elapsed duration of a stopwatch.
 *
 * If the stopwatch is still running, the function folds in the current
 * monotonic time before returning.
 *
 * @param sw Stopwatch to inspect.
 * @return Elapsed duration, or zero when `sw` is `NULL`.
 */
keel_duration_t keel_stopwatch_elapsed(const keel_stopwatch_t* sw);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_UTIL_H */
