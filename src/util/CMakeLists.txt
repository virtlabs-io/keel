# =============================================================================
# src/util/CMakeLists.txt — Core utility and logging libraries
# =============================================================================
#
# Utility support is split deliberately into two targets:
#
#   keellog  — Minimal logging library with NO keelmem dependency.
#              Low-level code can emit diagnostics without pulling in the
#              full allocator/utility stack, breaking the circular dependency
#              that would otherwise exist between mem → log → mem.
#              Contains: log.c (structured log core), print.c (formatted output).
#
#   keelutil — Broader helper collection layered on top of keellog + keelmem:
#              time.c (monotonic clocks, deadline arithmetic),
#              error.c (errno wrappers, error string formatting),
#              hash.c (consistent-hash ring, string hashing),
#              net.c (socket helpers, address resolution),
#              string.c (safe string operations),
#              buffer.c (growable byte buffers),
#              string_view.c (zero-copy string slices),
#              xxhash.c (fast non-cryptographic hashing).
# =============================================================================

# ---- Logging library (no keelmem dependency - breaks circular dep) ----
add_library(keellog STATIC
    log.c
    print.c
    encoding.c
)

target_include_directories(keellog
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
)

target_link_libraries(keellog PUBLIC Threads::Threads)

set_target_properties(keellog PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)

# ---- Full utility library ----
add_library(keelutil STATIC
    time.c
    error.c
    hash.c
    net.c
    string.c
    buffer.c
    string_view.c
    xxhash.c
)

target_include_directories(keelutil
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(keelutil PUBLIC keelmem PUBLIC keellog PUBLIC Threads::Threads)

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelutil PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelutil PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keelutil PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
