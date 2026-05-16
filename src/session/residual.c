/**
 * @file residual.c
 * @brief Hybrid inline-plus-chunk buffering for partial protocol frames.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Residual buffering bridges the mismatch between socket reads and protocol frame
 * boundaries. The common case is tiny, so bytes are kept inline inside the
 * session object when possible. Only when residual data outgrows that inline
 * budget does the code allocate chained overflow chunks.
 *
 * That design keeps the usual partial-header path allocation-free while still
 * supporting arbitrarily large buffered tails for COPY and streaming flows.
 */

#include "keel/session/session.h"
#include "keel/mem/mem.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Default chunk size for overflow allocation */
#define RESIDUAL_CHUNK_SIZE 4096

/* ============================================================================
 * Initialization
 * ============================================================================ */

/**
 * @brief Reset a residual buffer to the empty baseline.
 */
void keel_residual_init(keel_residual_t* res)
{
    assert(res != NULL);
    
    memset(res->inline_buf, 0, KEEL_RESIDUAL_INLINE_SIZE);
    res->inline_used = 0;
    res->head = NULL;
    res->tail = NULL;
    res->total_size = 0;
    res->expected_len = 0;
    res->header_complete = false;
}

/* ============================================================================
 * Append Data
 * ============================================================================ */

/**
 * @brief Append bytes to the residual tail, spilling to heap chunks as needed.
 */
int keel_residual_append(keel_residual_t* res, const void* data, size_t len)
{
    assert(res != NULL);
    
    if (len == 0) {
        return 0;
    }
    
    const uint8_t* src = (const uint8_t*)data;
    
    /* Keep the hot path allocation-free by consuming any remaining inline space
     * before touching the heap-backed overflow chain. */
    if (res->inline_used < KEEL_RESIDUAL_INLINE_SIZE && res->head == NULL) {
        size_t inline_avail = KEEL_RESIDUAL_INLINE_SIZE - res->inline_used;
        size_t copy_len = (len < inline_avail) ? len : inline_avail;
        
        memcpy(res->inline_buf + res->inline_used, src, copy_len);
        res->inline_used += copy_len;
        src += copy_len;
        len -= copy_len;
        
        if (len == 0) {
            return 0;
        }
    }
    
    /* Remaining bytes no longer fit inline, so the residual graduates to the
     * overflow chain representation. */
    
    /* Reuse slack in the current tail chunk before allocating a new one. */
    if (res->tail != NULL) {
        size_t tail_avail = res->tail->size - res->tail->used;
        if (tail_avail > 0) {
            size_t copy_len = (len < tail_avail) ? len : tail_avail;
            memcpy(res->tail->data + res->tail->used, src, copy_len);
            res->tail->used += copy_len;
            res->total_size += copy_len;
            src += copy_len;
            len -= copy_len;
            
            if (len == 0) {
                return 0;
            }
        }
    }
    
    /* Allocate enough chunk space to absorb the remaining payload. The minimum
     * chunk size smooths repeated append patterns; very large tails may use a
     * single oversized chunk to avoid immediate follow-on allocations. */
    while (len > 0) {
        size_t chunk_data_size = (len < RESIDUAL_CHUNK_SIZE) ? RESIDUAL_CHUNK_SIZE : len;
        
        keel_residual_chunk_t* chunk = (keel_residual_chunk_t*)keel_malloc(
            sizeof(keel_residual_chunk_t) + chunk_data_size
        );
        
        if (chunk == NULL) {
            return -1;  /* Allocation failure */
        }
        
        chunk->next = NULL;
        chunk->size = chunk_data_size;
        
        size_t copy_len = (len < chunk_data_size) ? len : chunk_data_size;
        memcpy(chunk->data, src, copy_len);
        chunk->used = copy_len;
        
        /* Link chunk into chain */
        if (res->tail != NULL) {
            res->tail->next = chunk;
        } else {
            res->head = chunk;
        }
        res->tail = chunk;
        res->total_size += copy_len;
        
        src += copy_len;
        len -= copy_len;
    }
    
    return 0;
}

/* ============================================================================
 * Consume Data
 * ============================================================================ */

/**
 * @brief Consume bytes from the residual head in FIFO order.
 */
size_t keel_residual_consume(keel_residual_t* res, void* dest, size_t len)
{
    assert(res != NULL);
    
    size_t total_consumed = 0;
    uint8_t* dst = (uint8_t*)dest;
    
    /* Inline bytes are always logically first because data only spills to chunks
     * after the inline segment has already been filled. */
    if (res->inline_used > 0) {
        size_t consume_len = (len < res->inline_used) ? len : res->inline_used;
        
        if (dst != NULL) {
            memcpy(dst, res->inline_buf, consume_len);
            dst += consume_len;
        }
        
        /* Shift remaining inline data */
        if (consume_len < res->inline_used) {
            memmove(res->inline_buf, res->inline_buf + consume_len,
                    res->inline_used - consume_len);
        }
        res->inline_used -= consume_len;
        
        total_consumed += consume_len;
        len -= consume_len;
        
        if (len == 0) {
            return total_consumed;
        }
    }
    
    /* Once inline storage is empty, drain heap chunks in order. */
    while (len > 0 && res->head != NULL) {
        keel_residual_chunk_t* chunk = res->head;
        size_t chunk_avail = chunk->used;
        size_t consume_len = (len < chunk_avail) ? len : chunk_avail;
        
        if (dst != NULL) {
            memcpy(dst, chunk->data, consume_len);
            dst += consume_len;
        }
        
        total_consumed += consume_len;
        res->total_size -= consume_len;
        len -= consume_len;
        
        if (consume_len == chunk_avail) {
            /* Consumed entire chunk, free it */
            res->head = chunk->next;
            if (res->head == NULL) {
                res->tail = NULL;
            }
            keel_free(chunk);
        } else {
            /* Partial consume, shift remaining data */
            memmove(chunk->data, chunk->data + consume_len,
                    chunk->used - consume_len);
            chunk->used -= consume_len;
        }
    }
    
    return total_consumed;
}

/* ============================================================================
 * Peek Data
 * ============================================================================ */

/**
 * @brief Expose the first contiguous residual segment without removing it.
 */
const void* keel_residual_peek(keel_residual_t* res, size_t* len)
{
    assert(res != NULL);
    assert(len != NULL);
    
    /* If all data is in inline buffer, return it directly */
    if (res->head == NULL) {
        *len = res->inline_used;
        return (res->inline_used > 0) ? res->inline_buf : NULL;
    }
    
    /* If inline buffer is full, return the first chunk */
    if (res->inline_used == KEEL_RESIDUAL_INLINE_SIZE) {
        *len = res->head->used;
        return res->head->data;
    }
    
    /* Data spans inline and chunks - return inline portion only
     * (caller will need to consume and call again) */
    *len = res->inline_used;
    return (res->inline_used > 0) ? res->inline_buf : NULL;
}

/* ============================================================================
 * Clear
 * ============================================================================ */

/**
 * @brief Free all overflow chunks and return to the initialized state.
 */
void keel_residual_clear(keel_residual_t* res)
{
    assert(res != NULL);
    
    /* Free all chunks */
    keel_residual_chunk_t* chunk = res->head;
    while (chunk != NULL) {
        keel_residual_chunk_t* next = chunk->next;
        keel_free(chunk);
        chunk = next;
    }
    
    /* Reset to initial state */
    keel_residual_init(res);
}

/* ============================================================================
 * Utility: Linearize (for cases where we need contiguous data)
 * ============================================================================ */

/**
 * @brief Copy the entire residual into caller-provided contiguous storage.
 *
 * @param res Residual buffer to linearize.
 * @param buf [out] Destination buffer.
 * @param buf_len Capacity of `buf`.
 * @return Byte count copied, or `-1` if the buffer is too small.
 */
ssize_t keel_residual_linearize(const keel_residual_t* res, void* buf, size_t buf_len)
{
    assert(res != NULL);
    
    size_t total_len = keel_residual_len(res);
    if (buf_len < total_len) {
        return -1;
    }
    
    uint8_t* dst = (uint8_t*)buf;
    
    /* Copy inline portion */
    if (res->inline_used > 0) {
        memcpy(dst, res->inline_buf, res->inline_used);
        dst += res->inline_used;
    }
    
    /* Copy chunks */
    const keel_residual_chunk_t* chunk = res->head;
    while (chunk != NULL) {
        memcpy(dst, chunk->data, chunk->used);
        dst += chunk->used;
        chunk = chunk->next;
    }
    
    return (ssize_t)total_len;
}

/* ============================================================================
 * Utility: Compact (move chunk data to inline if possible)
 * ============================================================================ */

/**
 * @brief Pull overflow bytes back into inline storage when the buffered tail shrinks.
 */
void keel_residual_compact(keel_residual_t* res)
{
    assert(res != NULL);
    
    /* If inline buffer has room and chunks exist, consolidate */
    while (res->inline_used < KEEL_RESIDUAL_INLINE_SIZE && res->head != NULL) {
        size_t inline_avail = KEEL_RESIDUAL_INLINE_SIZE - res->inline_used;
        keel_residual_chunk_t* chunk = res->head;
        
        size_t copy_len = (chunk->used < inline_avail) ? chunk->used : inline_avail;
        memcpy(res->inline_buf + res->inline_used, chunk->data, copy_len);
        res->inline_used += copy_len;
        res->total_size -= copy_len;
        
        if (copy_len == chunk->used) {
            /* Consumed entire chunk */
            res->head = chunk->next;
            if (res->head == NULL) {
                res->tail = NULL;
            }
            keel_free(chunk);
        } else {
            /* Partial consume */
            memmove(chunk->data, chunk->data + copy_len, chunk->used - copy_len);
            chunk->used -= copy_len;
            break;
        }
    }
}
