#[[
 @file CMakeLists.txt
 @brief Build rules for dlmalloc — Doug Lea's allocator (MIT-0 license).

 Compiled in ONLY_MSPACES mode: only the mspace_* API is available.
 The global malloc/free/calloc symbols are NOT defined, so libc remains
 fully functional for bootstrap-phase allocations (config parsing, etc.).

 Compile-time knobs used:
   ONLY_MSPACES=1     Expose only the mspace API; no global malloc/free.
   HAVE_MORECORE=0    Disable sbrk() growth; pool memory comes from our mmap.
   MMAP_CLEARS=1      mmap pages are zero-initialised on Linux (skip memset).
   NO_MALLOC_STATS=1  Disable built-in malloc_stats(); we use our own counters.
   FOOTERS=0          No footer canaries — our keel_alloc_header already does this.
   USE_LOCKS=1        Enable per-mspace pthread mutex. Required because g_mspace is
                      shared across all worker threads; without this flag the locked=1
                      argument to create_mspace_with_base is silently ignored and every
                      concurrent mspace_malloc/mspace_free pair is a data race.
]]

add_library(dlmalloc STATIC dlmalloc.c)

target_compile_definitions(dlmalloc PRIVATE
    ONLY_MSPACES=1
    HAVE_MORECORE=0
    MMAP_CLEARS=1
    NO_MALLOC_STATS=1
    FOOTERS=0
    USE_LOCKS=1
)

# Third-party code: suppress all warnings so CI stays clean.
target_compile_options(dlmalloc PRIVATE -w)

# Export the include directory so keelmem can find dlmalloc.h.
target_include_directories(dlmalloc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
