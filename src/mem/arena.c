/**
 * @file arena.c
 * @brief Bump-pointer arena allocator implementation.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Arenas are used when lifetime grouping matters more than individual frees. The
 * implementation keeps a linked list of blocks, hands out aligned slices by bumping
 * a `used` cursor, and amortizes deallocation into `reset` or `destroy`. This is a
 * good match for parser scratch space, request-scoped temporary objects, and other
 * control-plane workloads that naturally discard whole groups of allocations.
 */

#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>

/* Default arena block size: 64KB provides good balance between
 * memory usage and allocation frequency */
#define DEFAULT_BLOCK_SIZE (64 * 1024)

/* Minimum alignment for all allocations - 16 bytes ensures
 * proper alignment for SSE/AVX operations */
#define ARENA_ALIGNMENT 16

/**
 * @brief Arena block - a chunk of memory
 */
typedef struct keel_arena_block {
    struct keel_arena_block* next;   /**< Next block in chain */
    size_t                  size;   /**< Block size (usable) */
    size_t                  used;   /**< Bytes used */
    uint8_t                 data[]; /**< Flexible array for data */
} keel_arena_block_t;

/**
 * @brief Arena allocator
 */
struct keel_arena {
    keel_arena_block_t*  head;       /**< Current block */
    keel_arena_block_t*  blocks;     /**< All blocks (for freeing) */
    size_t              block_size; /**< Default block size */
    size_t              total_used; /**< Total bytes used */
    size_t              total_size; /**< Total bytes allocated */
};

/* ============================================================================
 * Block Management
 * ============================================================================ */

/**
 * @brief Create a new arena block with at least min_size bytes
 *
 * Allocates a new memory block for the arena. The actual size is
 * the maximum of min_size and DEFAULT_BLOCK_SIZE, aligned up to
 * ARENA_ALIGNMENT.
 *
 * Block structure:
 * ```
 * +---------------------+------------------+
 * | keel_arena_block_t   | data[] (usable)  |
 * | (next, size, used)  |                  |
 * +---------------------+------------------+
 * ```
 *
 * @param min_size  Minimum usable size required
 * @return New block, or NULL on allocation failure
 */
static keel_arena_block_t* arena_block_create(size_t min_size) {
    size_t size = keel_max(min_size, DEFAULT_BLOCK_SIZE);
    size = keel_align_up(size, ARENA_ALIGNMENT);
    
    keel_arena_block_t* block = keel_malloc(sizeof(keel_arena_block_t) + size);
    if (!block) {
        return NULL;
    }
    
    block->next = NULL;
    block->size = size;
    block->used = 0;
    
    return block;
}

/**
 * @brief Free an arena block
 *
 * @param block  Block to free (may be NULL)
 */
static void arena_block_destroy(keel_arena_block_t* block) {
    keel_free(block);
}

/* ============================================================================
 * Arena Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new arena allocator
 *
 * Creates an arena with an initial memory block. The initial_size
 * parameter controls the size of each block allocated by this arena.
 *
 * @param initial_size  Size for memory blocks (0 = 64KB default)
 * @return New arena handle, or NULL on allocation failure
 *
 * @example
 * ```c
 * // Default size (64KB blocks)
 * keel_arena_t* arena = keel_arena_create(0);
 * 
 * // Larger blocks for heavy allocation workloads
 * keel_arena_t* big = keel_arena_create(1024 * 1024);  // 1MB
 * ```
 */
keel_arena_t* keel_arena_create(size_t initial_size) {
    keel_arena_t* arena = keel_malloc(sizeof(keel_arena_t));
    if (!arena) {
        return NULL;
    }
    
    arena->block_size = initial_size > 0 ? initial_size : DEFAULT_BLOCK_SIZE;
    arena->head = NULL;
    arena->blocks = NULL;
    arena->total_used = 0;
    arena->total_size = 0;
    
    /* Create initial block */
    arena->head = arena_block_create(arena->block_size);
    if (!arena->head) {
        keel_free(arena);
        return NULL;
    }
    
    arena->blocks = arena->head;
    arena->total_size = arena->head->size;
    
    return arena;
}

/**
 * @brief Destroy an arena and free all its memory
 *
 * Releases all memory allocated by the arena, including:
 * - All memory blocks
 * - The arena structure itself
 *
 * All pointers obtained from this arena become invalid after this call.
 *
 * @param arena  Arena to destroy (may be NULL, no-op)
 */
void keel_arena_destroy(keel_arena_t* arena) {
    if (!arena) {
        return;
    }
    
    /* Free all blocks */
    keel_arena_block_t* block = arena->blocks;
    while (block) {
        keel_arena_block_t* next = block->next;
        arena_block_destroy(block);
        block = next;
    }
    
    keel_free(arena);
}

/* ============================================================================
 * Allocation
 * ============================================================================ */

/**
 * @brief Allocate memory from the arena with default alignment
 *
 * Convenience wrapper that calls keel_arena_alloc_aligned() with
 * the default ARENA_ALIGNMENT (16 bytes).
 *
 * @param arena  Arena to allocate from
 * @param size   Number of bytes needed
 * @return Pointer to allocated memory, or NULL on failure
 */
void* keel_arena_alloc(keel_arena_t* arena, size_t size) {
    return keel_arena_alloc_aligned(arena, size, ARENA_ALIGNMENT);
}

/**
 * @brief Allocate zeroed memory from the arena
 *
 * Similar to calloc() - allocates count * size bytes and
 * initializes all bytes to zero.
 *
 * @param arena  Arena to allocate from
 * @param count  Number of elements
 * @param size   Size of each element
 * @return Pointer to zeroed memory, or NULL on failure
 */
void* keel_arena_calloc(keel_arena_t* arena, size_t count, size_t size) {
    size_t total = count * size;
    void* ptr = keel_arena_alloc(arena, total);
    if (ptr) {
        memset(ptr, 0, total);
    }
    return ptr;
}

/**
 * @brief Allocate aligned memory from the arena
 *
 * This is the core allocation function. It implements bump allocation:
 * 1. Calculate aligned position within current block
 * 2. If fits, bump the used pointer and return
 * 3. If not, allocate a new block and retry
 *
 * Algorithm details:
 * - Alignment is achieved by calculating padding needed
 * - New blocks are inserted at head for cache locality
 * - Large allocations get their own appropriately-sized block
 *
 * @param arena      Arena to allocate from
 * @param size       Number of bytes needed
 * @param alignment  Required alignment (must be power of 2)
 * @return Pointer to aligned memory, or NULL on failure
 *
 * @note If alignment is not power of 2, ARENA_ALIGNMENT is used
 */
void* keel_arena_alloc_aligned(keel_arena_t* arena, size_t size, size_t alignment) {
    if (!arena || size == 0) {
        return NULL;
    }
    
    if (!keel_is_power_of_2(alignment)) {
        alignment = ARENA_ALIGNMENT;
    }
    
    /* Try to allocate from current block */
    keel_arena_block_t* block = arena->head;
    
    while (block) {
        /* Calculate aligned position */
        uintptr_t current = (uintptr_t)(block->data + block->used);
        uintptr_t aligned = keel_align_up(current, alignment);
        size_t padding = aligned - current;
        
        if (block->used + padding + size <= block->size) {
            /* Fits in this block */
            block->used += padding + size;
            arena->total_used += padding + size;
            return (void*)aligned;
        }
        
        block = block->next;
    }
    
    /* Need new block */
    size_t block_size = keel_max(size + alignment, arena->block_size);
    keel_arena_block_t* new_block = arena_block_create(block_size);
    if (!new_block) {
        return NULL;
    }
    
    /* Insert at head for faster allocation */
    new_block->next = arena->head;
    arena->head = new_block;
    
    /* Add to blocks list if not already head */
    if (arena->blocks != new_block) {
        /* Find tail and append (or we could track tail) */
        /* For simplicity, we'll just prepend to blocks list too */
        /* Actually, let's link properly: head is for allocation, blocks for cleanup */
        new_block->next = arena->blocks;
        arena->blocks = new_block;
        arena->head = new_block;
    } else {
        arena->blocks = new_block;
    }
    
    arena->total_size += new_block->size;
    
    /* Allocate from new block */
    uintptr_t current = (uintptr_t)(new_block->data);
    uintptr_t aligned = keel_align_up(current, alignment);
    size_t padding = aligned - current;
    
    new_block->used = padding + size;
    arena->total_used += padding + size;
    
    return (void*)aligned;
}

/* ============================================================================
 * Reset and Stats
 * ============================================================================ */

/**
 * @brief Reset an arena without freeing memory
 *
 * Resets all blocks to empty state, allowing the arena to be reused.
 * This is much faster than destroying and recreating the arena.
 *
 * Use case: When processing multiple independent items, reset between
 * each item to reuse memory without system calls.
 *
 * @code
 * for (int i = 0; i < 1000; i++) {
 *     process_request(arena);
 *     keel_arena_reset(arena);  // Ready for next request
 * }
 * @endcode
 *
 * @param arena  Arena to reset (NULL is safe no-op)
 */
void keel_arena_reset(keel_arena_t* arena) {
    if (!arena) {
        return;
    }
    
    /* Reset all blocks but keep them allocated */
    for (keel_arena_block_t* block = arena->blocks; block; block = block->next) {
        block->used = 0;
    }
    
    arena->head = arena->blocks;
    arena->total_used = 0;
}

/**
 * @brief Get arena memory statistics
 *
 * Returns current memory usage of the arena:
 * - used: Bytes currently allocated to user
 * - committed: Total bytes allocated from system
 *
 * Efficiency can be calculated as: used / committed
 *
 * @param arena      Arena to query (NULL returns zeros)
 * @param used       [out] Bytes used by allocations (may be NULL)
 * @param committed  [out] Total bytes allocated (may be NULL)
 */
void keel_arena_stats(const keel_arena_t* arena, size_t* used, size_t* committed) {
    if (!arena) {
        if (used) *used = 0;
        if (committed) *committed = 0;
        return;
    }
    
    if (used) *used = arena->total_used;
    if (committed) *committed = arena->total_size;
}

/* ============================================================================
 * Save/Restore (Checkpoint/Rollback)
 * ============================================================================ */

/**
 * @brief Save the current arena state for later restore
 *
 * Creates a checkpoint of the arena's allocation state. This can be
 * restored later with keel_arena_restore() to "rollback" allocations.
 *
 * Use case: Speculative parsing where you allocate AST nodes, then
 * rollback if the parse fails.
 *
 * @code
 * keel_arena_mark_t mark = keel_arena_save(arena);
 * node_t* node = try_parse_expression(arena);
 * if (!node) {
 *     keel_arena_restore(arena, mark);  // Undo allocations
 * }
 * @endcode
 *
 * @param arena  Arena to checkpoint
 * @return Mark containing the current position
 */
keel_arena_mark_t keel_arena_save(keel_arena_t* arena) {
    keel_arena_mark_t mark = {0};
    if (arena) {
        mark.pos = arena->total_used;
    }
    return mark;
}

/**
 * @brief Restore arena to a previously saved state
 *
 * Rolls back allocations to the state captured by keel_arena_save().
 * Memory is not actually freed, but will be reused by subsequent
 * allocations.
 *
 * @warning Pointers allocated after the save point become invalid!
 *
 * @param arena  Arena to restore
 * @param mark   Previously saved mark from keel_arena_save()
 */
void keel_arena_restore(keel_arena_t* arena, keel_arena_mark_t mark) {
    if (!arena) {
        return;
    }
    
    /* Simple implementation: reset all blocks and reallocate up to mark */
    /* More sophisticated: track block positions in mark */
    
    /* For now, just update total_used (won't actually free memory) */
    if (mark.pos < arena->total_used) {
        /* Find the right block and position */
        size_t remaining = mark.pos;
        for (keel_arena_block_t* block = arena->blocks; block; block = block->next) {
            if (remaining <= block->size) {
                block->used = remaining;
                arena->head = block;
                /* Reset subsequent blocks */
                for (keel_arena_block_t* b = block->next; b; b = b->next) {
                    b->used = 0;
                }
                break;
            }
            remaining -= block->used;
        }
        arena->total_used = mark.pos;
    }
}

/* ============================================================================
 * String Operations
 * ============================================================================ */

/**
 * @brief Duplicate a string into the arena
 *
 * Allocates strlen(str)+1 bytes and copies the string including
 * the null terminator. Equivalent to strdup() but uses arena memory.
 *
 * @param arena  Arena to allocate from
 * @param str    String to duplicate (NULL returns NULL)
 * @return Copy of string in arena, or NULL on failure
 */
char* keel_arena_strdup(keel_arena_t* arena, const char* str) {
    if (!arena || !str) {
        return NULL;
    }
    
    size_t len = strlen(str);
    char* dup = keel_arena_alloc(arena, len + 1);
    if (dup) {
        memcpy(dup, str, len + 1);
    }
    return dup;
}

/**
 * @brief Format a string into the arena (printf-style)
 *
 * Similar to sprintf but allocates the result in the arena.
 * Uses a two-pass approach: first measures required size,
 * then allocates and formats.
 *
 * @code
 * char* msg = keel_arena_sprintf(arena, "User %s has %d items", name, count);
 * @endcode
 *
 * @param arena  Arena to allocate from
 * @param fmt    Printf-style format string
 * @param ...    Format arguments
 * @return Formatted string in arena, or NULL on failure
 */
char* keel_arena_sprintf(keel_arena_t* arena, const char* fmt, ...) {
    if (!arena || !fmt) {
        return NULL;
    }
    
    va_list args, args_copy;
    va_start(args, fmt);
    va_copy(args_copy, args);
    
    /* Calculate required size */
    int len = vsnprintf(NULL, 0, fmt, args);
    va_end(args);
    
    if (len < 0) {
        va_end(args_copy);
        return NULL;
    }
    
    char* buf = keel_arena_alloc(arena, (size_t)len + 1);
    if (buf) {
        vsnprintf(buf, (size_t)len + 1, fmt, args_copy);
    }
    
    va_end(args_copy);
    return buf;
}
