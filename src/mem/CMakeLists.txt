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

add_library(keelmem STATIC
    mem.c
    arena.c
    pool.c
    slab.c
    ringbuf.c
    mem_safety.c
    numa.c
)

target_include_directories(keelmem PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_BINARY_DIR}/include
)

# keelmem uses keel_log_enabled/keel_log_internal from keellog
target_link_libraries(keelmem PUBLIC keellog PUBLIC Threads::Threads)

target_compile_options(keelmem PRIVATE
    ${KEEL_WARNING_FLAGS}
    ${KEEL_SANITIZER_FLAGS}
)

if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelmem PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelmem PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()
