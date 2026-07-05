/**
 * @file dlmalloc.h
 * @brief Public mspace API declarations for dlmalloc (ONLY_MSPACES build).
 *
 * This header exposes only the mspace_* functions that keel uses. dlmalloc.c is
 * compiled with ONLY_MSPACES=1, so no global malloc/free symbols are emitted.
 *
 * Doug Lea's malloc, version 2.8.6 — MIT-0 license.
 */

#ifndef KEEL_DLMALLOC_H
#define KEEL_DLMALLOC_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * An mspace is an opaque handle to an independently-managed heap region.
 * Internally it is a pointer to the mstate struct within the managed region.
 */
typedef void* mspace;

/**
 * @brief Create an mspace that manages a caller-supplied memory region.
 *
 * @param base     Start of the region (must be properly aligned).
 * @param capacity Size of the region in bytes.
 * @param locked   Non-zero to enable an internal pthread mutex (thread-safe).
 * @return Opaque mspace handle, or NULL on failure.
 */
mspace create_mspace_with_base(void* base, size_t capacity, int locked);

/**
 * @brief Destroy an mspace.
 *
 * Does NOT release the underlying memory — the caller owns the region and
 * is responsible for munmap/free'ing it afterward.
 *
 * @return Total bytes of memory managed by the space.
 */
size_t destroy_mspace(mspace msp);

/** @brief malloc within an mspace. */
void* mspace_malloc(mspace msp, size_t bytes);

/** @brief calloc within an mspace. */
void* mspace_calloc(mspace msp, size_t n_elements, size_t elem_size);

/** @brief realloc within an mspace. */
void* mspace_realloc(mspace msp, void* mem, size_t newsize);

/** @brief free within an mspace. */
void  mspace_free(mspace msp, void* mem);

/** @brief Aligned allocation within an mspace. */
void* mspace_memalign(mspace msp, size_t alignment, size_t bytes);

/**
 * @brief Return the total bytes consumed by the mspace's internal bookkeeping
 * and live allocations (i.e. the current high-water mark within the region).
 */
size_t mspace_footprint(mspace msp);

/**
 * @brief Return the peak footprint reached since the mspace was created.
 */
size_t mspace_max_footprint(mspace msp);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_DLMALLOC_H */
