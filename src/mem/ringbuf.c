/**
 * @file ringbuf.c
 * @brief Lock-free SPSC, MPSC, and byte-ring implementations.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * These implementations prioritize predictable producer/consumer behavior over a
 * maximally generic API. Each structure encodes a specific concurrency contract,
 * which lets the code use the minimum required atomic ordering instead of falling
 * back to coarser locking. The cost is that misuse across the wrong thread model
 * is undefined at the semantic level even if the code compiles.
 */

#include "keel/mem/ringbuf.h"
#include "keel/mem/mem.h"

#include <string.h>

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Round an integer up to the next power of two.
 *
 * Ring buffers rely on power-of-two capacities so index wrap can use a cheap
 * bitmask instead of a modulo division.
 *
 * @param n Requested minimum value.
 * @return Smallest power of two greater than or equal to `n`.
 */
static size_t next_power_of_2(size_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
}

/* ============================================================================
 * SPSC Ring Buffer Implementation
 * ============================================================================ */

/**
 * @brief Create an SPSC ring buffer with cache-line-aligned control fields.
 *
 * @param capacity Requested slot count. Rounded up to a power of two.
 * @param slot_size Bytes per slot.
 * @return New ring buffer, or `NULL` on allocation failure or invalid input.
 */
keel_spsc_ringbuf_t* keel_spsc_ringbuf_create(size_t capacity, size_t slot_size) {
    if (capacity == 0 || slot_size == 0) return NULL;
    
    /* Round capacity to power of 2 */
    capacity = next_power_of_2(capacity);
    
    /* Allocate ring buffer structure */
    keel_spsc_ringbuf_t* rb = keel_aligned_alloc(KEEL_CACHE_LINE_SIZE, sizeof(keel_spsc_ringbuf_t));
    if (!rb) return NULL;
    
    /* Allocate slot array */
    rb->slots = keel_aligned_alloc(KEEL_CACHE_LINE_SIZE, capacity * slot_size);
    if (!rb->slots) {
        keel_free(rb);
        return NULL;
    }
    
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    rb->capacity = capacity;
    rb->mask = capacity - 1;
    rb->slot_size = slot_size;
    
    return rb;
}

/**
 * @brief Destroy an SPSC ring buffer and its slot storage.
 *
 * @param rb Ring buffer to destroy. `NULL` is accepted.
 * @return
 */
void keel_spsc_ringbuf_destroy(keel_spsc_ringbuf_t* rb) {
    if (!rb) return;
    keel_aligned_free(rb->slots);
    keel_aligned_free(rb);
}

/**
 * @brief Test whether the SPSC ring buffer contains no items.
 *
 * Loads both head and tail with `memory_order_acquire` so that any slot
 * writes by the producer are visible before the consumer considers the
 * buffer empty.
 *
 * @param rb Ring buffer to query. `NULL` is treated as empty.
 * @return `true` if the buffer is empty, `false` otherwise.
 */
bool keel_spsc_ringbuf_is_empty(keel_spsc_ringbuf_t* rb) {
    if (!rb) return true;
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    return head == tail;
}

/**
 * @brief Test whether the SPSC ring buffer has no free slots.
 *
 * Uses the standard power-of-two ring condition: `(head + 1) & mask == tail`.
 * One slot is always kept empty to distinguish the full state from empty.
 *
 * @param rb Ring buffer to query. `NULL` is treated as full.
 * @return `true` if the buffer is full, `false` otherwise.
 */
bool keel_spsc_ringbuf_is_full(keel_spsc_ringbuf_t* rb) {
    if (!rb) return true;
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    return ((head + 1) & rb->mask) == tail;
}

/**
 * @brief Return the number of items currently stored in the SPSC ring buffer.
 *
 * Computes `(head - tail) & mask`.  The result is a snapshot; by the time
 * the caller uses it, the producer or consumer may have advanced.
 *
 * @param rb Ring buffer to query. `NULL` returns 0.
 * @return Number of occupied slots.
 */
size_t keel_spsc_ringbuf_size(keel_spsc_ringbuf_t* rb) {
    if (!rb) return 0;
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    return (head - tail) & rb->mask;
}

/**
 * @brief Attempt to enqueue a slot into an SPSC buffer.
 *
 * @param rb Ring buffer.
 * @param data Source payload copied into the producer-owned slot.
 * @return `true` on success, or `false` if the buffer is full or inputs are invalid.
 */
bool keel_spsc_ringbuf_try_push(keel_spsc_ringbuf_t* rb, const void* data) {
    if (!rb || !data) return false;
    
    /* Load head (we own it as producer) */
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t next_head = (head + 1) & rb->mask;
    
    /* Check if full (need to acquire tail for this check) */
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    if (next_head == tail) {
        return false;  /* Full */
    }
    
    /* Copy data to slot */
    void* slot = rb->slots + (head * rb->slot_size);
    memcpy(slot, data, rb->slot_size);
    
    /* Publish the new head (release so consumer sees the data) */
    atomic_store_explicit(&rb->head, next_head, memory_order_release);
    
    return true;
}

/**
 * @brief Attempt to dequeue a slot from an SPSC buffer.
 *
 * @param rb Ring buffer.
 * @param[out] data Destination buffer that receives the slot contents.
 * @return `true` on success, or `false` if the buffer is empty or inputs are invalid.
 */
bool keel_spsc_ringbuf_try_pop(keel_spsc_ringbuf_t* rb, void* data) {
    if (!rb || !data) return false;
    
    /* Load tail (we own it as consumer) */
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    
    /* Check if empty (need to acquire head for this check) */
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    if (head == tail) {
        return false;  /* Empty */
    }
    
    /* Copy data from slot */
    void* slot = rb->slots + (tail * rb->slot_size);
    memcpy(data, slot, rb->slot_size);
    
    /* Advance tail (release so producer can reuse the slot) */
    size_t next_tail = (tail + 1) & rb->mask;
    atomic_store_explicit(&rb->tail, next_tail, memory_order_release);
    
    return true;
}

/**
 * @brief Reserve the next producer slot for a zero-copy SPSC push.
 *
 * Returns a direct pointer into the ring's slot storage so the caller can
 * write the payload in place.  The slot is not visible to the consumer
 * until `keel_spsc_ringbuf_commit_push()` is called.
 *
 * Usage pattern:
 * @code
 * void* slot = keel_spsc_ringbuf_prepare_push(rb);
 * if (slot) { memcpy(slot, &item, rb->slot_size); keel_spsc_ringbuf_commit_push(rb); }
 * @endcode
 *
 * @param rb Ring buffer. `NULL` returns `NULL`.
 * @return Pointer to the reserved slot, or `NULL` if the buffer is full.
 *
 * Notes:
 * - Must be called only by the single producer thread.
 * - The returned pointer is only valid until `commit_push` is called.
 */
void* keel_spsc_ringbuf_prepare_push(keel_spsc_ringbuf_t* rb) {
    if (!rb) return NULL;
    
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t next_head = (head + 1) & rb->mask;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    if (next_head == tail) {
        return NULL;  /* Full */
    }
    
    return rb->slots + (head * rb->slot_size);
}

/**
 * @brief Publish the slot prepared by `keel_spsc_ringbuf_prepare_push()`.
 *
 * Advances the head index with `memory_order_release` so the consumer
 * observes the completed write.  Must be paired with a preceding
 * `keel_spsc_ringbuf_prepare_push()` that returned a non-`NULL` slot.
 *
 * @param rb Ring buffer. `NULL` is a safe no-op.
 *
 * Notes:
 * - Must be called only by the single producer thread.
 */
void keel_spsc_ringbuf_commit_push(keel_spsc_ringbuf_t* rb) {
    if (!rb) return;
    
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t next_head = (head + 1) & rb->mask;
    atomic_store_explicit(&rb->head, next_head, memory_order_release);
}

/**
 * @brief Peek at the oldest occupied slot for a zero-copy SPSC pop.
 *
 * Returns a direct pointer to the oldest item so the caller can read it
 * without an extra copy.  The slot remains occupied until
 * `keel_spsc_ringbuf_commit_pop()` is called to advance the tail.
 *
 * Usage pattern:
 * @code
 * void* slot = keel_spsc_ringbuf_prepare_pop(rb);
 * if (slot) { process(slot); keel_spsc_ringbuf_commit_pop(rb); }
 * @endcode
 *
 * @param rb Ring buffer. `NULL` returns `NULL`.
 * @return Pointer to the oldest slot, or `NULL` if the buffer is empty.
 *
 * Notes:
 * - Must be called only by the single consumer thread.
 * - The returned pointer is only valid until `commit_pop` is called.
 */
void* keel_spsc_ringbuf_prepare_pop(keel_spsc_ringbuf_t* rb) {
    if (!rb) return NULL;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    
    if (head == tail) {
        return NULL;  /* Empty */
    }
    
    return rb->slots + (tail * rb->slot_size);
}

/**
 * @brief Release the slot consumed by `keel_spsc_ringbuf_prepare_pop()`.
 *
 * Advances the tail index with `memory_order_release` so the producer can
 * reuse the freed slot.  Must be paired with a preceding
 * `keel_spsc_ringbuf_prepare_pop()` that returned a non-`NULL` slot.
 *
 * @param rb Ring buffer. `NULL` is a safe no-op.
 *
 * Notes:
 * - Must be called only by the single consumer thread.
 */
void keel_spsc_ringbuf_commit_pop(keel_spsc_ringbuf_t* rb) {
    if (!rb) return;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t next_tail = (tail + 1) & rb->mask;
    atomic_store_explicit(&rb->tail, next_tail, memory_order_release);
}

/* ============================================================================
 * MPSC Ring Buffer Implementation
 * ============================================================================ */

/**
 * @brief Translate a logical sequence index into the owning MPSC slot.
 *
 * @param rb Ring buffer.
 * @param index Logical producer/consumer sequence value.
 * @return Pointer to the slot that currently represents that sequence window.
 */
static keel_mpsc_slot_t* mpsc_get_slot(keel_mpsc_ringbuf_t* rb, size_t index) {
    size_t offset = (index & rb->mask) * rb->slot_stride;
    return (keel_mpsc_slot_t*)(void*)(rb->slots + offset);
}

/**
 * @brief Create an MPSC ring buffer using per-slot sequence counters.
 *
 * @param capacity Requested slot count. Rounded up to a power of two.
 * @param slot_size User payload size per slot.
 * @return New ring buffer, or `NULL` on failure.
 */
keel_mpsc_ringbuf_t* keel_mpsc_ringbuf_create(size_t capacity, size_t slot_size) {
    if (capacity == 0 || slot_size == 0) return NULL;
    
    /* Round capacity to power of 2 */
    capacity = next_power_of_2(capacity);
    
    /* Calculate slot stride (header + data, aligned) */
    size_t slot_stride = sizeof(keel_mpsc_slot_t) + slot_size;
    slot_stride = (slot_stride + 7) & ~(size_t)7;  /* 8-byte align */
    
    /* Allocate ring buffer structure */
    keel_mpsc_ringbuf_t* rb = keel_aligned_alloc(KEEL_CACHE_LINE_SIZE, sizeof(keel_mpsc_ringbuf_t));
    if (!rb) return NULL;
    
    /* Allocate slot array */
    rb->slots = keel_aligned_alloc(KEEL_CACHE_LINE_SIZE, capacity * slot_stride);
    if (!rb->slots) {
        keel_free(rb);
        return NULL;
    }
    
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    rb->capacity = capacity;
    rb->mask = capacity - 1;
    rb->slot_size = slot_size;
    rb->slot_stride = slot_stride;
    
    /* Initialize slot sequences */
    for (size_t i = 0; i < capacity; i++) {
        keel_mpsc_slot_t* slot = mpsc_get_slot(rb, i);
        atomic_init(&slot->sequence, i);
    }
    
    return rb;
}

/**
 * @brief Destroy an MPSC ring buffer and release its slot storage.
 *
 * Frees the cache-line-aligned slot array and the ring buffer header.
 * All in-flight slots are discarded.
 *
 * @param rb Ring buffer to destroy. `NULL` is a safe no-op.
 */
void keel_mpsc_ringbuf_destroy(keel_mpsc_ringbuf_t* rb) {
    if (!rb) return;
    keel_aligned_free(rb->slots);
    keel_aligned_free(rb);
}

/**
 * @brief Test whether the MPSC ring buffer contains no ready-to-consume items.
 *
 * Loads head and tail with `memory_order_acquire`.  Note that a `false`
 * result does not guarantee the next `try_pop` will succeed—a producer
 * may have claimed a slot but not yet published it.
 *
 * @param rb Ring buffer to query. `NULL` is treated as empty.
 * @return `true` if head == tail (no published items), `false` otherwise.
 */
bool keel_mpsc_ringbuf_is_empty(keel_mpsc_ringbuf_t* rb) {
    if (!rb) return true;
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    return head == tail;
}

/**
 * @brief Attempt to publish a slot into an MPSC ring.
 *
 * Producers compete using CAS on the shared head index and then publish readiness
 * by updating the slot sequence number.
 *
 * @param rb Ring buffer.
 * @param data Source payload copied into the claimed slot.
 * @return `true` on success, or `false` if the buffer is full or inputs are invalid.
 */
bool keel_mpsc_ringbuf_try_push(keel_mpsc_ringbuf_t* rb, const void* data) {
    if (!rb || !data) return false;
    
    size_t head;
    keel_mpsc_slot_t* slot;
    
    for (;;) {
        head = atomic_load_explicit(&rb->head, memory_order_relaxed);
        slot = mpsc_get_slot(rb, head);
        
        size_t seq = atomic_load_explicit(&slot->sequence, memory_order_acquire);
        intptr_t diff = (intptr_t)seq - (intptr_t)head;
        
        if (diff == 0) {
            /* Slot is available, try to claim it */
            if (atomic_compare_exchange_weak_explicit(
                    &rb->head, &head, head + 1,
                    memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            /* Buffer is full */
            return false;
        }
        /* diff > 0: another producer got this slot, retry with new head */
    }
    
    /* Write data to slot */
    memcpy(slot->data, data, rb->slot_size);
    
    /* Publish: set sequence to head + 1 so consumer knows it's ready */
    atomic_store_explicit(&slot->sequence, head + 1, memory_order_release);
    
    return true;
}

/**
 * @brief Attempt to dequeue one slot from an MPSC ring.
 *
 * @param rb Ring buffer.
 * @param[out] data Destination buffer that receives the payload.
 * @return `true` on success, or `false` if the next slot is not yet ready.
 */
bool keel_mpsc_ringbuf_try_pop(keel_mpsc_ringbuf_t* rb, void* data) {
    if (!rb || !data) return false;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    keel_mpsc_slot_t* slot = mpsc_get_slot(rb, tail);
    
    size_t seq = atomic_load_explicit(&slot->sequence, memory_order_acquire);
    intptr_t diff = (intptr_t)seq - (intptr_t)(tail + 1);
    
    if (diff < 0) {
        /* Slot not ready yet (empty or being written) */
        return false;
    }
    
    /* Read data */
    memcpy(data, slot->data, rb->slot_size);
    
    /* Mark slot as available for reuse: set sequence to tail + capacity */
    atomic_store_explicit(&slot->sequence, tail + rb->capacity, memory_order_release);
    
    /* Advance tail */
    atomic_store_explicit(&rb->tail, tail + 1, memory_order_relaxed);
    
    return true;
}

/* ============================================================================
 * Byte Ring Buffer Implementation
 * ============================================================================ */

/**
 * @brief Create a byte-oriented ring buffer for variable-length payload streams.
 *
 * @param capacity Requested byte capacity. Rounded up to a power of two.
 * @return New byte ring buffer, or `NULL` on failure.
 */
keel_byte_ringbuf_t* keel_byte_ringbuf_create(size_t capacity) {
    if (capacity == 0) return NULL;
    
    /* Round capacity to power of 2 */
    capacity = next_power_of_2(capacity);
    
    keel_byte_ringbuf_t* rb = keel_aligned_alloc(KEEL_CACHE_LINE_SIZE, sizeof(keel_byte_ringbuf_t));
    if (!rb) return NULL;
    
    rb->buffer = keel_aligned_alloc(KEEL_CACHE_LINE_SIZE, capacity);
    if (!rb->buffer) {
        keel_free(rb);
        return NULL;
    }
    
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    rb->capacity = capacity;
    rb->mask = capacity - 1;
    
    return rb;
}

/**
 * @brief Destroy a byte ring buffer and release its backing storage.
 *
 * Frees the data buffer and the ring buffer header, both of which are
 * cache-line-aligned allocations.
 *
 * @param rb Ring buffer to destroy. `NULL` is a safe no-op.
 */
void keel_byte_ringbuf_destroy(keel_byte_ringbuf_t* rb) {
    if (!rb) return;
    keel_aligned_free(rb->buffer);
    keel_aligned_free(rb);
}

/**
 * @brief Return the number of contiguous writable bytes available.
 *
 * Reports how many bytes can be written before the ring is full.  One byte
 * is always reserved to distinguish the full state from empty, so the
 * maximum writable capacity is `capacity - 1`.
 *
 * @param rb Ring buffer to query. `NULL` returns 0.
 * @return Number of bytes that can be written in the next `write` call.
 */
size_t keel_byte_ringbuf_writable(keel_byte_ringbuf_t* rb) {
    if (!rb) return 0;
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    /* Leave one byte to distinguish full from empty */
    return (rb->capacity - 1 - ((head - tail) & rb->mask));
}

/**
 * @brief Return the number of bytes available to read.
 *
 * Computes `(head - tail) & mask`.  The result is a snapshot; concurrent
 * writes may increase it between the call and the subsequent `read`.
 *
 * @param rb Ring buffer to query. `NULL` returns 0.
 * @return Number of unconsumed bytes currently in the buffer.
 */
size_t keel_byte_ringbuf_readable(keel_byte_ringbuf_t* rb) {
    if (!rb) return 0;
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    return (head - tail) & rb->mask;
}

/**
 * @brief Write as many bytes as fit into the byte ring.
 *
 * @param rb Ring buffer.
 * @param data Source bytes.
 * @param len Requested write length.
 * @return Actual number of bytes committed.
 */
size_t keel_byte_ringbuf_write(keel_byte_ringbuf_t* rb, const void* data, size_t len) {
    if (!rb || !data || len == 0) return 0;
    
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    
    /* Available space (leave one byte) */
    size_t avail = (rb->capacity - 1 - ((head - tail) & rb->mask));
    if (len > avail) len = avail;
    if (len == 0) return 0;
    
    /* Copy data - may need to wrap around */
    size_t head_pos = head & rb->mask;
    size_t first_chunk = rb->capacity - head_pos;
    
    if (first_chunk >= len) {
        memcpy(rb->buffer + head_pos, data, len);
    } else {
        memcpy(rb->buffer + head_pos, data, first_chunk);
        memcpy(rb->buffer, (const uint8_t*)data + first_chunk, len - first_chunk);
    }
    
    atomic_store_explicit(&rb->head, head + len, memory_order_release);
    return len;
}

/**
 * @brief Read and consume bytes from the byte ring.
 *
 * @param rb Ring buffer.
 * @param[out] data Destination buffer.
 * @param len Maximum number of bytes to read.
 * @return Actual number of bytes copied out.
 */
size_t keel_byte_ringbuf_read(keel_byte_ringbuf_t* rb, void* data, size_t len) {
    if (!rb || !data || len == 0) return 0;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    
    size_t avail = (head - tail) & rb->mask;
    if (len > avail) len = avail;
    if (len == 0) return 0;
    
    /* Copy data - may wrap around */
    size_t tail_pos = tail & rb->mask;
    size_t first_chunk = rb->capacity - tail_pos;
    
    if (first_chunk >= len) {
        memcpy(data, rb->buffer + tail_pos, len);
    } else {
        memcpy(data, rb->buffer + tail_pos, first_chunk);
        memcpy((uint8_t*)data + first_chunk, rb->buffer, len - first_chunk);
    }
    
    atomic_store_explicit(&rb->tail, tail + len, memory_order_release);
    return len;
}

/**
 * @brief Copy bytes from the byte ring without advancing the consumer position.
 *
 * @param rb Ring buffer.
 * @param[out] data Destination buffer.
 * @param len Maximum number of bytes to inspect.
 * @return Number of bytes copied.
 */
size_t keel_byte_ringbuf_peek(keel_byte_ringbuf_t* rb, void* data, size_t len) {
    if (!rb || !data || len == 0) return 0;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    
    size_t avail = (head - tail) & rb->mask;
    if (len > avail) len = avail;
    if (len == 0) return 0;
    
    size_t tail_pos = tail & rb->mask;
    size_t first_chunk = rb->capacity - tail_pos;
    
    if (first_chunk >= len) {
        memcpy(data, rb->buffer + tail_pos, len);
    } else {
        memcpy(data, rb->buffer + tail_pos, first_chunk);
        memcpy((uint8_t*)data + first_chunk, rb->buffer, len - first_chunk);
    }
    
    /* Don't advance tail - this is a peek */
    return len;
}

/**
 * @brief Advance the consumer position without copying data.
 *
 * Equivalent to calling `keel_byte_ringbuf_read()` with a discard buffer,
 * but cheaper because no copy is performed.  Useful when the caller has
 * already processed the data via `keel_byte_ringbuf_peek()`.
 *
 * @param rb  Ring buffer. `NULL` is a safe no-op returning 0.
 * @param len Number of bytes to discard.  Clamped to the readable amount.
 * @return Actual number of bytes consumed.
 */
size_t keel_byte_ringbuf_consume(keel_byte_ringbuf_t* rb, size_t len) {
    if (!rb || len == 0) return 0;
    
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    
    size_t avail = (head - tail) & rb->mask;
    if (len > avail) len = avail;
    
    atomic_store_explicit(&rb->tail, tail + len, memory_order_release);
    return len;
}
