#[[
 @file CMakeLists.txt
 @brief Build rules for the KEEL memory-management library.
 @author Charly Batista

 Copyright (C) Virtlabs, https://virtlabs.io

 This target groups the allocator wrappers, arena/pool/slab implementations,
 lock-free ring buffers, memory-safety tooling, and NUMA support into a single
 reusable library. The layout makes the architectural split explicit: runtime
 code links `keelmem` for both hot-path allocation primitives and diagnostics,
 while the actual process entry point remains separate in `src/main`.
]]

set(_keelmem_sources
    mem.c
    arena.c
    pool.c
    ringbuf.c
    numa.c
)
if(KEEL_ENABLE_MEM_SAFETY)
    list(APPEND _keelmem_sources mem_safety.c)
endif()

add_library(keelmem STATIC ${_keelmem_sources})

target_include_directories(keelmem PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_BINARY_DIR}/include
)

# keelmem uses keel_log_enabled/keel_log_internal from keellog.
# When building with MSan or TSan, fall back to system malloc — those sanitizers
# require every library to be instrumented, which is non-trivial for the
# mimalloc submodule.  KEEL_USE_SYSTEM_MALLOC activates shims in mem.c.
if(KEEL_ALLOC_BACKEND STREQUAL "system")
    target_compile_definitions(keelmem PRIVATE KEEL_USE_SYSTEM_MALLOC=1)
    target_link_libraries(keelmem PUBLIC keellog PUBLIC Threads::Threads)
else()
    target_link_libraries(keelmem PUBLIC keellog PUBLIC Threads::Threads PUBLIC mimalloc-static)
endif()

target_compile_options(keelmem PRIVATE
    ${KEEL_WARNING_FLAGS}
    ${KEEL_SANITIZER_FLAGS}
)

if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelmem PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelmem PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()
