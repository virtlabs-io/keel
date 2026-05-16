/**
 * @file ringbuf.h
 * @brief Lock-free ring-buffer primitives for inter-thread data movement.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * These structures underpin low-latency producer/consumer pipelines inside the
 * proxy. The API exposes both copy-based and zero-copy-style prepare/commit flows
 * so callers can trade convenience for fewer copies when they already control the
 * producer/consumer discipline. The implementation isolates head and tail fields
 * on separate cache lines to reduce false sharing under sustained traffic.
 */

#ifndef KEEL_RINGBUF_H
#define KEEL_RINGBUF_H

#include "keel_types.h"
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * SPSC Ring Buffer - Single Producer, Single Consumer
 * ============================================================================ */

/**
 * @brief SPSC ring buffer
 *
 * Cache-line aligned to prevent false sharing between producer and consumer.
 */
typedef struct keel_spsc_ringbuf {
    KEEL_CACHE_ALIGNED atomic_size_t head;       /**< Write position (producer) */
    KEEL_CACHE_ALIGNED atomic_size_t tail;       /**< Read position (consumer) */
    size_t                          capacity;   /**< Number of slots (power of 2) */
    size_t                          mask;       /**< capacity - 1 for fast modulo */
    size_t                          slot_size;  /**< Size of each slot */
    uint8_t*                        slots;      /**< Slot data */
} keel_spsc_ringbuf_t;

/**
 * @brief Create an SPSC ring buffer
 *
 * @param capacity  Number of slots (will be rounded up to power of 2)
 * @param slot_size Size of each slot in bytes
 * @return Ring buffer, or NULL on failure
 */
keel_spsc_ringbuf_t* keel_spsc_ringbuf_create(size_t capacity, size_t slot_size);

/**
 * @brief Destroy an SPSC ring buffer
 */
void keel_spsc_ringbuf_destroy(keel_spsc_ringbuf_t* rb);

/**
 * @brief Check if ring buffer is empty
 */
bool keel_spsc_ringbuf_is_empty(keel_spsc_ringbuf_t* rb);

/**
 * @brief Check if ring buffer is full
 */
bool keel_spsc_ringbuf_is_full(keel_spsc_ringbuf_t* rb);

/**
 * @brief Get number of items in ring buffer
 */
size_t keel_spsc_ringbuf_size(keel_spsc_ringbuf_t* rb);

/**
 * @brief Try to push an item (producer only)
 *
 * @param rb   Ring buffer
 * @param data Data to copy into slot
 * @return true on success, false if full
 */
bool keel_spsc_ringbuf_try_push(keel_spsc_ringbuf_t* rb, const void* data);

/**
 * @brief Try to pop an item (consumer only)
 *
 * @param rb Ring buffer.
 * @param[out] data Destination for copied data.
 * @return true on success, false if empty
 */
bool keel_spsc_ringbuf_try_pop(keel_spsc_ringbuf_t* rb, void* data);

/**
 * @brief Get pointer to slot for direct write (producer only)
 *
 * For zero-copy writes, use this to get a slot pointer, write directly,
 * then call commit_push().
 *
 * @param rb Ring buffer
 * @return Pointer to slot, or NULL if full
 */
void* keel_spsc_ringbuf_prepare_push(keel_spsc_ringbuf_t* rb);

/**
 * @brief Commit a prepared push (producer only)
 *
 * @param rb Ring buffer
 */
void keel_spsc_ringbuf_commit_push(keel_spsc_ringbuf_t* rb);

/**
 * @brief Get pointer to slot for direct read (consumer only)
 *
 * For zero-copy reads, use this to get a slot pointer, read directly,
 * then call commit_pop().
 *
 * @param rb Ring buffer
 * @return Pointer to slot, or NULL if empty
 */
void* keel_spsc_ringbuf_prepare_pop(keel_spsc_ringbuf_t* rb);

/**
 * @brief Commit a prepared pop (consumer only)
 *
 * @param rb Ring buffer
 */
void keel_spsc_ringbuf_commit_pop(keel_spsc_ringbuf_t* rb);

/* ============================================================================
 * MPSC Ring Buffer - Multiple Producers, Single Consumer
 * ============================================================================ */

/**
 * @brief MPSC slot with sequence number for coordination
 */
typedef struct keel_mpsc_slot {
    atomic_size_t sequence;     /**< Sequence number for coordination */
    uint8_t       data[];       /**< Slot data */
} keel_mpsc_slot_t;

/**
 * @brief MPSC ring buffer
 *
 * Uses sequence numbers per slot to coordinate multiple producers.
 */
typedef struct keel_mpsc_ringbuf {
    KEEL_CACHE_ALIGNED atomic_size_t head;       /**< Write position (producers) */
    KEEL_CACHE_ALIGNED atomic_size_t tail;       /**< Read position (consumer) */
    size_t                          capacity;   /**< Number of slots (power of 2) */
    size_t                          mask;       /**< capacity - 1 */
    size_t                          slot_size;  /**< User data size per slot */
    size_t                          slot_stride;/**< Total slot size with header */
    uint8_t*                        slots;      /**< Slot data */
} keel_mpsc_ringbuf_t;

/**
 * @brief Create an MPSC ring buffer
 *
 * @param capacity  Number of slots (will be rounded up to power of 2)
 * @param slot_size Size of each slot's user data
 * @return Ring buffer, or NULL on failure
 */
keel_mpsc_ringbuf_t* keel_mpsc_ringbuf_create(size_t capacity, size_t slot_size);

/**
 * @brief Destroy an MPSC ring buffer
 */
void keel_mpsc_ringbuf_destroy(keel_mpsc_ringbuf_t* rb);

/**
 * @brief Check if ring buffer is empty
 */
bool keel_mpsc_ringbuf_is_empty(keel_mpsc_ringbuf_t* rb);

/**
 * @brief Try to push an item (any producer thread)
 *
 * Lock-free but may spin briefly on contention.
 *
 * @param rb   Ring buffer
 * @param data Data to copy into slot
 * @return true on success, false if full
 */
bool keel_mpsc_ringbuf_try_push(keel_mpsc_ringbuf_t* rb, const void* data);

/**
 * @brief Try to pop an item (consumer thread only)
 *
 * Wait-free.
 *
 * @param rb Ring buffer.
 * @param[out] data Destination for copied data.
 * @return true on success, false if empty
 */
bool keel_mpsc_ringbuf_try_pop(keel_mpsc_ringbuf_t* rb, void* data);

/* ============================================================================
 * Log Entry for Async Logging
 * ============================================================================ */

/**
 * @brief Log entry for async logging ring buffer
 */
typedef struct keel_log_entry {
    uint64_t    timestamp;      /**< Nanoseconds since epoch */
    uint32_t    level;          /**< Log level */
    uint32_t    category;       /**< Log category */
    uint32_t    line;           /**< Source line */
    uint16_t    file_len;       /**< Length of filename */
    uint16_t    msg_len;        /**< Length of message */
    char        data[232];      /**< Filename + message (null-separated) */
} keel_log_entry_t;

/* Ensure log entry fits in 256 bytes for cache efficiency */
_Static_assert(sizeof(keel_log_entry_t) == 256, "Log entry must be 256 bytes");

/**
 * @brief Create an async logging ring buffer
 *
 * @param capacity Number of log entries
 * @return SPSC ring buffer configured for log entries
 */
KEEL_INLINE keel_spsc_ringbuf_t* keel_log_ringbuf_create(size_t capacity) {
    return keel_spsc_ringbuf_create(capacity, sizeof(keel_log_entry_t));
}

/* ============================================================================
 * Byte Ring Buffer - for variable-length data
 * ============================================================================ */

/**
 * @brief Byte ring buffer for variable-length messages
 *
 * Unlike the slot-based ring buffers, this stores raw bytes with
 * length prefixes, suitable for variable-length protocol messages.
 */
typedef struct keel_byte_ringbuf {
    KEEL_CACHE_ALIGNED atomic_size_t head;   /**< Write position */
    KEEL_CACHE_ALIGNED atomic_size_t tail;   /**< Read position */
    size_t              capacity;           /**< Buffer size (power of 2) */
    size_t              mask;               /**< capacity - 1 */
    uint8_t*            buffer;             /**< Ring buffer data */
} keel_byte_ringbuf_t;

/**
 * @brief Create a byte ring buffer
 *
 * @param capacity Buffer size (will be rounded up to power of 2)
 * @return Ring buffer, or NULL on failure
 */
keel_byte_ringbuf_t* keel_byte_ringbuf_create(size_t capacity);

/**
 * @brief Destroy a byte ring buffer
 */
void keel_byte_ringbuf_destroy(keel_byte_ringbuf_t* rb);

/**
 * @brief Get available space for writing
 */
size_t keel_byte_ringbuf_writable(keel_byte_ringbuf_t* rb);

/**
 * @brief Get available data for reading
 */
size_t keel_byte_ringbuf_readable(keel_byte_ringbuf_t* rb);

/**
 * @brief Write bytes to ring buffer
 *
 * @param rb   Ring buffer
 * @param data Data to write
 * @param len  Number of bytes
 * @return Number of bytes written
 */
size_t keel_byte_ringbuf_write(keel_byte_ringbuf_t* rb, const void* data, size_t len);

/**
 * @brief Read bytes from ring buffer
 *
 * @param rb Ring buffer.
 * @param[out] data Destination buffer.
 * @param len  Maximum bytes to read
 * @return Number of bytes read
 */
size_t keel_byte_ringbuf_read(keel_byte_ringbuf_t* rb, void* data, size_t len);

/**
 * @brief Peek bytes without consuming
 *
 * @param rb Ring buffer.
 * @param[out] data Destination buffer.
 * @param len  Maximum bytes to peek
 * @return Number of bytes peeked
 */
size_t keel_byte_ringbuf_peek(keel_byte_ringbuf_t* rb, void* data, size_t len);

/**
 * @brief Consume bytes without reading
 *
 * @param rb  Ring buffer
 * @param len Number of bytes to consume
 * @return Number of bytes consumed
 */
size_t keel_byte_ringbuf_consume(keel_byte_ringbuf_t* rb, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_RINGBUF_H */
