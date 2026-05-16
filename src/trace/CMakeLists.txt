# =============================================================================
# src/trace/CMakeLists.txt — Distributed tracing (OTLP export, W3C context)
# =============================================================================
#
# `keeltrace` implements the lightweight distributed tracing subsystem:
#
#   - W3C Trace Context (traceparent) parsing and generation
#   - Span lifecycle with inline storage (zero hot-path allocations)
#   - Per-worker SPSC ring buffers for finished spans
#   - Async exporter thread with OTLP/HTTP JSON output
#   - Head-based sampling (configurable rate in ppm)
#
# Compile-time guard: This module is always built, but the tracer is only
# activated at runtime when [tracing] enabled = true in the config file.
# =============================================================================

add_library(keeltrace STATIC
    trace.c
)

target_include_directories(keeltrace
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

# Dependencies:
#   keelmem  — heap allocation for ring buffers and export batches
#   keellog  — lifecycle logging (exporter start/stop, errors)
#   Threads  — exporter thread, pthread_cond for flush signalling
target_link_libraries(keeltrace
    PUBLIC  keelmem
    PUBLIC  keellog
    PUBLIC  Threads::Threads
)

target_compile_options(keeltrace PRIVATE
    ${KEEL_WARNING_FLAGS}
    ${KEEL_SANITIZER_FLAGS}
)

if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keeltrace PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keeltrace PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keeltrace PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
