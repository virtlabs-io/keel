/**
 * @file buffer.c
 * @brief Growable byte-buffer implementation for small serialization tasks.
 *
 * `keel_buffer_t` is the minimal mutable counterpart to KEEL's immutable slice
 * types. It is used where code needs to accumulate protocol frames, temporary
 * payloads, or formatted data incrementally.
 *
 * The implementation chooses a simple contiguous allocation rather than a rope
 * or linked chunk list because most utility and protocol payloads are modest in
 * size, and contiguous storage keeps appends, reads, and endian helpers easy to
 * reason about.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel_types.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"

#include <string.h>

/* Default initial capacity */
#define KEEL_BUFFER_INIT_CAPACITY 256

/* Growth factor (1.5x) */
#define KEEL_BUFFER_GROW_FACTOR 3 / 2

/* ============================================================================
 * Dynamic Buffer Structure
 * ============================================================================ */

struct keel_buffer {
    uint8_t* data;
    size_t   len;
    size_t   cap;
};

/* ============================================================================
 * Buffer Creation
 * ============================================================================ */

/**
 * @brief Allocate a new buffer with the default initial capacity.
 *
 * @return Newly allocated buffer, or `NULL` on allocation failure.
 */
keel_buffer_t* keel_buffer_new(void) {
    return keel_buffer_new_with_capacity(KEEL_BUFFER_INIT_CAPACITY);
}

/**
 * @brief Allocate a new buffer pre-reserved to at least `capacity` bytes.
 *
 * @param capacity Desired initial capacity in bytes.
 * @return Newly allocated buffer, or `NULL` on allocation failure.
 */
keel_buffer_t* keel_buffer_new_with_capacity(size_t capacity) {
    keel_buffer_t* buf = keel_malloc(sizeof(keel_buffer_t));
    if (!buf) {
        return NULL;
    }
    
    buf->data = keel_malloc(capacity);
    if (!buf->data) {
        keel_free(buf);
        return NULL;
    }
    
    buf->len = 0;
    buf->cap = capacity;
    
    return buf;
}

/**
 * @brief Allocate a new buffer pre-populated with a copy of `data`.
 *
 * @param data Source bytes to copy, may be `NULL` when `len` is 0.
 * @param len Number of bytes to copy from `data`.
 * @return Newly allocated buffer containing the copied data, or `NULL` on failure.
 */
keel_buffer_t* keel_buffer_from_data(const void* data, size_t len) {
    keel_buffer_t* buf = keel_buffer_new_with_capacity(len > 0 ? len : 1);
    if (!buf) {
        return NULL;
    }
    
    if (data && len > 0) {
        memcpy(buf->data, data, len);
        buf->len = len;
    }
    
    return buf;
}

/**
 * @brief Free a buffer and its backing storage.
 *
 * Safe to call with `NULL`.
 *
 * @param buf Buffer to free.
 */
void keel_buffer_free(keel_buffer_t* buf) {
    if (!buf) {
        return;
    }
    
    keel_free(buf->data);
    keel_free(buf);
}

/* ============================================================================
 * Buffer Properties
 * ============================================================================ */

/**
 * @brief Return the number of bytes currently stored in the buffer.
 *
 * @param buf Buffer to query.
 * @return Byte count, or 0 if `buf` is `NULL`.
 */
size_t keel_buffer_len(const keel_buffer_t* buf) {
    return buf ? buf->len : 0;
}

/**
 * @brief Return the total allocated capacity of the buffer.
 *
 * @param buf Buffer to query.
 * @return Capacity in bytes, or 0 if `buf` is `NULL`.
 */
size_t keel_buffer_capacity(const keel_buffer_t* buf) {
    return buf ? buf->cap : 0;
}

/**
 * @brief Return the number of bytes that can be appended without reallocation.
 *
 * @param buf Buffer to query.
 * @return Available bytes (capacity minus length), or 0 if `buf` is `NULL`.
 */
size_t keel_buffer_available(const keel_buffer_t* buf) {
    return buf ? buf->cap - buf->len : 0;
}

/**
 * @brief Check whether the buffer contains no bytes.
 *
 * @param buf Buffer to query.
 * @return `true` if `buf` is `NULL` or its length is zero.
 */
bool keel_buffer_empty(const keel_buffer_t* buf) {
    return !buf || buf->len == 0;
}

/**
 * @brief Return a mutable pointer to the buffer's raw byte storage.
 *
 * @param buf Buffer to query.
 * @return Pointer to the first byte, or `NULL` if `buf` is `NULL`.
 */
uint8_t* keel_buffer_data(keel_buffer_t* buf) {
    return buf ? buf->data : NULL;
}

/**
 * @brief Return a read-only pointer to the buffer's raw byte storage.
 *
 * @param buf Buffer to query.
 * @return Const pointer to the first byte, or `NULL` if `buf` is `NULL`.
 */
const uint8_t* keel_buffer_data_const(const keel_buffer_t* buf) {
    return buf ? buf->data : NULL;
}

/* ============================================================================
 * Buffer Capacity Management
 * ============================================================================ */

/**
 * @brief Grow a buffer to satisfy a minimum capacity requirement.
 *
 * The policy mixes power-of-two rounding for small buffers with gentler growth
 * afterward. That keeps very small buffers from reallocating repeatedly while
 * avoiding the steeper memory overhead of always doubling larger allocations.
 *
 * @param buf Buffer to grow.
 * @param min_cap Minimum required capacity.
 * @return true on success, false on allocation failure.
 */
static bool keel_buffer_grow(keel_buffer_t* buf, size_t min_cap) {
    if (buf->cap >= min_cap) {
        return true;
    }
    
    size_t new_cap = buf->cap * KEEL_BUFFER_GROW_FACTOR;
    if (new_cap < min_cap) {
        new_cap = min_cap;
    }
    
    /* Round up to next power of 2 for small buffers */
    if (new_cap < 4096) {
        size_t p = 1;
        while (p < new_cap) {
            p <<= 1;
        }
        new_cap = p;
    }
    
    uint8_t* new_data = keel_realloc(buf->data, new_cap);
    if (!new_data) {
        return false;
    }
    
    buf->data = new_data;
    buf->cap = new_cap;
    
    return true;
}

/**
 * @brief Ensure the buffer has at least `capacity` bytes of total capacity.
 *
 * @param buf Buffer to grow.
 * @param capacity Minimum required total capacity in bytes.
 * @return `true` on success, `false` if `buf` is `NULL` or reallocation fails.
 */
bool keel_buffer_reserve(keel_buffer_t* buf, size_t capacity) {
    if (!buf) {
        return false;
    }
    return keel_buffer_grow(buf, capacity);
}

/**
 * @brief Ensure at least `additional` bytes of free space are available.
 *
 * @param buf Buffer to grow if necessary.
 * @param additional Number of additional bytes that must fit without reallocation.
 * @return `true` on success, `false` if `buf` is `NULL` or reallocation fails.
 */
bool keel_buffer_ensure_available(keel_buffer_t* buf, size_t additional) {
    if (!buf) {
        return false;
    }
    return keel_buffer_grow(buf, buf->len + additional);
}

/**
 * @brief Release excess capacity so that allocated memory equals the current length.
 *
 * A no-op when capacity already equals length or when `buf` is `NULL`.
 *
 * @param buf Buffer to compact.
 */
void keel_buffer_shrink_to_fit(keel_buffer_t* buf) {
    if (!buf || buf->len == buf->cap) {
        return;
    }
    
    size_t new_cap = buf->len > 0 ? buf->len : 1;
    uint8_t* new_data = keel_realloc(buf->data, new_cap);
    if (new_data) {
        buf->data = new_data;
        buf->cap = new_cap;
    }
}

/* ============================================================================
 * Buffer Modification
 * ============================================================================ */

/**
 * @brief Set the logical length to zero without releasing memory.
 *
 * @param buf Buffer to clear.
 */
void keel_buffer_clear(keel_buffer_t* buf) {
    if (buf) {
        buf->len = 0;
    }
}

/**
 * @brief Clear the buffer and shrink its backing store back to the default capacity.
 *
 * Unlike `keel_buffer_clear`, this may free memory when the buffer has grown
 * beyond `KEEL_BUFFER_INIT_CAPACITY`.
 *
 * @param buf Buffer to reset.
 */
void keel_buffer_reset(keel_buffer_t* buf) {
    if (!buf) {
        return;
    }
    
    /* Shrink to default capacity */
    if (buf->cap > KEEL_BUFFER_INIT_CAPACITY) {
        uint8_t* new_data = keel_realloc(buf->data, KEEL_BUFFER_INIT_CAPACITY);
        if (new_data) {
            buf->data = new_data;
            buf->cap = KEEL_BUFFER_INIT_CAPACITY;
        }
    }
    buf->len = 0;
}

/**
 * @brief Append raw bytes to the end of a buffer.
 *
 * @param buf Destination buffer.
 * @param data Source bytes.
 * @param len Number of bytes to append.
 * @return true on success, false on invalid input or allocation failure.
 */
bool keel_buffer_append(keel_buffer_t* buf, const void* data, size_t len) {
    if (!buf || (!data && len > 0)) {
        return false;
    }
    
    if (len == 0) {
        return true;
    }
    
    if (!keel_buffer_grow(buf, buf->len + len)) {
        return false;
    }
    
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    
    return true;
}

/**
 * @brief Append a single byte to the end of a buffer.
 *
 * @param buf Destination buffer.
 * @param byte Byte value to append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_byte(keel_buffer_t* buf, uint8_t byte) {
    return keel_buffer_append(buf, &byte, 1);
}

/**
 * @brief Append a `keel_str_t` slice to a buffer.
 *
 * @param buf Destination buffer.
 * @param str Slice to append (data pointer and length).
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_str(keel_buffer_t* buf, keel_str_t str) {
    return keel_buffer_append(buf, str.data, str.len);
}

/**
 * @brief Append a NUL-terminated C string to a buffer.
 *
 * The terminating NUL byte is not copied. A `NULL` `cstr` is treated as a
 * zero-length append.
 *
 * @param buf Destination buffer.
 * @param cstr C string to append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_cstr(keel_buffer_t* buf, const char* cstr) {
    if (!cstr) {
        return true;
    }
    return keel_buffer_append(buf, cstr, strlen(cstr));
}

/**
 * @brief Append the contents of another buffer to this buffer.
 *
 * A `NULL` `other` is treated as a zero-length append.
 *
 * @param buf Destination buffer.
 * @param other Source buffer whose bytes are copied.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_buffer(keel_buffer_t* buf, const keel_buffer_t* other) {
    if (!other) {
        return true;
    }
    return keel_buffer_append(buf, other->data, other->len);
}

/**
 * @brief Prepend raw bytes to the beginning of a buffer.
 *
 * All existing bytes are shifted right to make room.
 *
 * @param buf Destination buffer.
 * @param data Source bytes to insert at position 0.
 * @param len Number of bytes to prepend.
 * @return `true` on success, `false` on invalid input or allocation failure.
 */
bool keel_buffer_prepend(keel_buffer_t* buf, const void* data, size_t len) {
    if (!buf || (!data && len > 0)) {
        return false;
    }
    
    if (len == 0) {
        return true;
    }
    
    if (!keel_buffer_grow(buf, buf->len + len)) {
        return false;
    }
    
    memmove(buf->data + len, buf->data, buf->len);
    memcpy(buf->data, data, len);
    buf->len += len;
    
    return true;
}

/**
 * @brief Insert raw bytes at an arbitrary position within a buffer.
 *
 * Bytes at and after `pos` are shifted right. If `pos` exceeds the current
 * length, the data is appended at the end.
 *
 * @param buf Destination buffer.
 * @param pos Byte offset at which to insert.
 * @param data Source bytes.
 * @param len Number of bytes to insert.
 * @return `true` on success, `false` on invalid input or allocation failure.
 */
bool keel_buffer_insert(keel_buffer_t* buf, size_t pos, const void* data, size_t len) {
    if (!buf || (!data && len > 0)) {
        return false;
    }
    
    if (pos > buf->len) {
        pos = buf->len;
    }
    
    if (len == 0) {
        return true;
    }
    
    if (!keel_buffer_grow(buf, buf->len + len)) {
        return false;
    }
    
    if (pos < buf->len) {
        memmove(buf->data + pos + len, buf->data + pos, buf->len - pos);
    }
    memcpy(buf->data + pos, data, len);
    buf->len += len;
    
    return true;
}

/**
 * @brief Remove a byte range from the middle of a buffer.
 *
 * The surviving suffix is shifted left with `memmove()`, which keeps the data
 * contiguous at the cost of $O(n)$ movement. That tradeoff is acceptable for
 * the moderate buffer sizes this helper is intended for.
 *
 * @param buf Buffer to modify.
 * @param pos Starting byte offset.
 * @param len Maximum number of bytes to remove.
 * @return Number of bytes actually removed.
 */
size_t keel_buffer_remove(keel_buffer_t* buf, size_t pos, size_t len) {
    if (!buf || pos >= buf->len) {
        return 0;
    }
    
    if (len > buf->len - pos) {
        len = buf->len - pos;
    }
    
    if (pos + len < buf->len) {
        memmove(buf->data + pos, buf->data + pos + len, buf->len - pos - len);
    }
    buf->len -= len;
    
    return len;
}

/**
 * @brief Remove `len` bytes from the front of a buffer.
 *
 * Equivalent to `keel_buffer_remove(buf, 0, len)`.
 *
 * @param buf Buffer to consume from.
 * @param len Maximum number of bytes to discard.
 * @return Number of bytes actually consumed.
 */
size_t keel_buffer_consume(keel_buffer_t* buf, size_t len) {
    return keel_buffer_remove(buf, 0, len);
}

/**
 * @brief Reduce the logical length of a buffer to at most `len` bytes.
 *
 * A no-op when `len` is already >= the current length.
 *
 * @param buf Buffer to truncate.
 * @param len New maximum length.
 */
void keel_buffer_truncate(keel_buffer_t* buf, size_t len) {
    if (buf && len < buf->len) {
        buf->len = len;
    }
}

/* ============================================================================
 * Buffer Reading
 * ============================================================================ */

/**
 * @brief Copy up to `len` bytes from the front of the buffer into `out`.
 *
 * The buffer is not modified; use `keel_buffer_read_and_consume` to also
 * advance the read position.
 *
 * @param buf Source buffer.
 * @param out Destination for the copied bytes.
 * @param len Maximum number of bytes to read.
 * @return Number of bytes actually copied.
 */
size_t keel_buffer_read(keel_buffer_t* buf, void* out, size_t len) {
    if (!buf || !out || len == 0) {
        return 0;
    }
    
    if (len > buf->len) {
        len = buf->len;
    }
    
    memcpy(out, buf->data, len);
    
    return len;
}

/**
 * @brief Copy up to `len` bytes from the front of the buffer into `out` and
 * remove them from the buffer.
 *
 * @param buf Source buffer.
 * @param out Destination for the copied bytes.
 * @param len Maximum number of bytes to read.
 * @return Number of bytes copied and consumed.
 */
size_t keel_buffer_read_and_consume(keel_buffer_t* buf, void* out, size_t len) {
    size_t read = keel_buffer_read(buf, out, len);
    keel_buffer_consume(buf, read);
    return read;
}

/**
 * @brief Read a single byte at `pos` without consuming it.
 *
 * @param buf Source buffer.
 * @param pos Byte offset to inspect.
 * @return The byte at `pos`, or 0 if `pos` is out of range or `buf` is `NULL`.
 */
uint8_t keel_buffer_peek_byte(const keel_buffer_t* buf, size_t pos) {
    if (!buf || pos >= buf->len) {
        return 0;
    }
    return buf->data[pos];
}

/* ============================================================================
 * Integer serialization (network byte order)
 * ============================================================================ */

/**
 * @brief Append a `uint8_t` value to the buffer.
 *
 * @param buf Destination buffer.
 * @param val Value to append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_u8(keel_buffer_t* buf, uint8_t val) {
    return keel_buffer_append(buf, &val, 1);
}

/**
 * @brief Append a `uint16_t` value in big-endian byte order.
 *
 * @param buf Destination buffer.
 * @param val Value to encode and append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_u16(keel_buffer_t* buf, uint16_t val) {
    uint8_t bytes[2] = {
        (uint8_t)(val >> 8),
        (uint8_t)(val & 0xff)
    };
    return keel_buffer_append(buf, bytes, 2);
}

/**
 * @brief Append a `uint32_t` value in big-endian byte order.
 *
 * @param buf Destination buffer.
 * @param val Value to encode and append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_u32(keel_buffer_t* buf, uint32_t val) {
    uint8_t bytes[4] = {
        (uint8_t)(val >> 24),
        (uint8_t)(val >> 16),
        (uint8_t)(val >> 8),
        (uint8_t)(val & 0xff)
    };
    return keel_buffer_append(buf, bytes, 4);
}

/**
 * @brief Append an `int16_t` value in big-endian byte order.
 *
 * @param buf Destination buffer.
 * @param val Signed 16-bit value to encode and append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_i16(keel_buffer_t* buf, int16_t val) {
    return keel_buffer_append_u16(buf, (uint16_t)val);
}

/**
 * @brief Append an `int32_t` value in big-endian byte order.
 *
 * @param buf Destination buffer.
 * @param val Signed 32-bit value to encode and append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_append_i32(keel_buffer_t* buf, int32_t val) {
    return keel_buffer_append_u32(buf, (uint32_t)val);
}

/**
 * @brief Consume and decode a `uint8_t` from the front of the buffer.
 *
 * @param buf Source buffer.
 * @param[out] out Receives the decoded value.
 * @return `true` on success, `false` if the buffer holds fewer than 1 byte.
 */
bool keel_buffer_read_u8(keel_buffer_t* buf, uint8_t* out) {
    if (!buf || !out || buf->len < 1) {
        return false;
    }
    *out = buf->data[0];
    keel_buffer_consume(buf, 1);
    return true;
}

/**
 * @brief Consume and decode a big-endian `uint16_t` from the front of the buffer.
 *
 * @param buf Source buffer.
 * @param[out] out Receives the decoded value.
 * @return `true` on success, `false` if the buffer holds fewer than 2 bytes.
 */
bool keel_buffer_read_u16(keel_buffer_t* buf, uint16_t* out) {
    if (!buf || !out || buf->len < 2) {
        return false;
    }
    *out = ((uint16_t)buf->data[0] << 8) | buf->data[1];
    keel_buffer_consume(buf, 2);
    return true;
}

/**
 * @brief Consume and decode a big-endian `uint32_t` from the front of the buffer.
 *
 * @param buf Source buffer.
 * @param[out] out Receives the decoded value.
 * @return `true` on success, `false` if the buffer holds fewer than 4 bytes.
 */
bool keel_buffer_read_u32(keel_buffer_t* buf, uint32_t* out) {
    if (!buf || !out || buf->len < 4) {
        return false;
    }
    *out = ((uint32_t)buf->data[0] << 24) |
           ((uint32_t)buf->data[1] << 16) |
           ((uint32_t)buf->data[2] << 8) |
           buf->data[3];
    keel_buffer_consume(buf, 4);
    return true;
}

/**
 * @brief Consume and decode a big-endian `int16_t` from the front of the buffer.
 *
 * @param buf Source buffer.
 * @param[out] out Receives the decoded signed value.
 * @return `true` on success, `false` if the buffer holds fewer than 2 bytes.
 */
bool keel_buffer_read_i16(keel_buffer_t* buf, int16_t* out) {
    uint16_t val;
    if (!keel_buffer_read_u16(buf, &val)) {
        return false;
    }
    *out = (int16_t)val;
    return true;
}

/**
 * @brief Consume and decode a big-endian `int32_t` from the front of the buffer.
 *
 * @param buf Source buffer.
 * @param[out] out Receives the decoded signed value.
 * @return `true` on success, `false` if the buffer holds fewer than 4 bytes.
 */
bool keel_buffer_read_i32(keel_buffer_t* buf, int32_t* out) {
    uint32_t val;
    if (!keel_buffer_read_u32(buf, &val)) {
        return false;
    }
    *out = (int32_t)val;
    return true;
}

/* ============================================================================
 * String conversion
 * ============================================================================ */

/**
 * @brief Return a non-owning `keel_str_t` view of the buffer contents.
 *
 * The returned slice is valid only as long as the buffer is not modified.
 *
 * @param buf Buffer to view.
 * @return A `keel_str_t` pointing into `buf`'s storage, or an empty slice for `NULL`.
 */
keel_str_t keel_buffer_to_str(const keel_buffer_t* buf) {
    if (!buf) {
        return (keel_str_t){ .data = "", .len = 0 };
    }
    return (keel_str_t){ .data = (const char*)buf->data, .len = buf->len };
}

/**
 * @brief Return a non-owning `keel_buf_t` view of the buffer contents.
 *
 * The returned descriptor is valid only as long as the buffer is not modified.
 *
 * @param buf Buffer to view.
 * @return A `keel_buf_t` pointing into `buf`'s storage, or a zero-length descriptor for `NULL`.
 */
keel_buf_t keel_buffer_to_buf(const keel_buffer_t* buf) {
    if (!buf) {
        return (keel_buf_t){ .data = NULL, .len = 0 };
    }
    return (keel_buf_t){ .data = buf->data, .len = buf->len };
}

/* ============================================================================
 * Ownership transfer
 * ============================================================================ */

/**
 * @brief Transfer ownership of the backing allocation out of a buffer.
 *
 * After the transfer, the buffer is reinitialized to an empty minimal state so
 * callers may keep using the object itself without reallocating the wrapper.
 *
 * @param buf Buffer relinquishing ownership.
 * @param[out] out_len Returned logical length of the detached data.
 * @return Owned byte buffer, or `NULL` when `buf` is `NULL`.
 */
uint8_t* keel_buffer_take(keel_buffer_t* buf, size_t* out_len) {
    if (!buf) {
        if (out_len) *out_len = 0;
        return NULL;
    }
    
    uint8_t* data = buf->data;
    if (out_len) {
        *out_len = buf->len;
    }
    
    /* Reset buffer to empty state */
    buf->data = keel_malloc(1);
    buf->len = 0;
    buf->cap = buf->data ? 1 : 0;
    
    return data;
}

/* ============================================================================
 * API aliases to match util.h declarations
 * ============================================================================ */

/* Write operations (alias for append) */
/**
 * @brief Write raw bytes to the buffer (alias for `keel_buffer_append`).
 *
 * @param buf Destination buffer.
 * @param data Source bytes.
 * @param len Number of bytes to write.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_write(keel_buffer_t* buf, const void* data, size_t len) {
    return keel_buffer_append(buf, data, len);
}

/**
 * @brief Write a single byte (alias for `keel_buffer_append_u8`).
 *
 * @param buf Destination buffer.
 * @param val Byte value to write.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_write_u8(keel_buffer_t* buf, uint8_t val) {
    return keel_buffer_append_u8(buf, val);
}

/**
 * @brief Write a big-endian `uint16_t` (alias for `keel_buffer_append_u16`).
 *
 * @param buf Destination buffer.
 * @param val Value to encode and write.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_write_u16_be(keel_buffer_t* buf, uint16_t val) {
    return keel_buffer_append_u16(buf, val);
}

/**
 * @brief Write a big-endian `uint32_t` (alias for `keel_buffer_append_u32`).
 *
 * @param buf Destination buffer.
 * @param val Value to encode and write.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_write_u32_be(keel_buffer_t* buf, uint32_t val) {
    return keel_buffer_append_u32(buf, val);
}

/**
 * @brief Write a big-endian `uint64_t` to the buffer.
 *
 * @param buf Destination buffer.
 * @param val 64-bit value to encode in big-endian order and append.
 * @return `true` on success, `false` on allocation failure.
 */
bool keel_buffer_write_u64_be(keel_buffer_t* buf, uint64_t val) {
    uint8_t bytes[8] = {
        (uint8_t)(val >> 56),
        (uint8_t)(val >> 48),
        (uint8_t)(val >> 40),
        (uint8_t)(val >> 32),
        (uint8_t)(val >> 24),
        (uint8_t)(val >> 16),
        (uint8_t)(val >> 8),
        (uint8_t)(val & 0xff)
    };
    return keel_buffer_append(buf, bytes, 8);
}

/* Read-at-position operations (non-consuming reads at specific offset) */
/**
 * @brief Read a `uint8_t` at `pos` without advancing the buffer (alias for `keel_buffer_peek_byte`).
 *
 * @param buf Source buffer.
 * @param pos Byte offset to read.
 * @return The byte value, or 0 if `pos` is out of range.
 */
uint8_t keel_buffer_read_u8_at(const keel_buffer_t* buf, size_t pos) {
    return keel_buffer_peek_byte(buf, pos);
}

/**
 * @brief Read a big-endian `uint16_t` at `pos` without consuming any bytes.
 *
 * @param buf Source buffer.
 * @param pos Byte offset of the first byte of the 16-bit value.
 * @return Decoded value, or 0 if fewer than 2 bytes remain at `pos`.
 */
uint16_t keel_buffer_read_u16_be_at(const keel_buffer_t* buf, size_t pos) {
    if (!buf || pos + 2 > buf->len) {
        return 0;
    }
    return ((uint16_t)buf->data[pos] << 8) | buf->data[pos + 1];
}

/**
 * @brief Read a big-endian `uint32_t` at `pos` without consuming any bytes.
 *
 * @param buf Source buffer.
 * @param pos Byte offset of the first byte of the 32-bit value.
 * @return Decoded value, or 0 if fewer than 4 bytes remain at `pos`.
 */
uint32_t keel_buffer_read_u32_be_at(const keel_buffer_t* buf, size_t pos) {
    if (!buf || pos + 4 > buf->len) {
        return 0;
    }
    return ((uint32_t)buf->data[pos] << 24) |
           ((uint32_t)buf->data[pos + 1] << 16) |
           ((uint32_t)buf->data[pos + 2] << 8) |
           buf->data[pos + 3];
}
