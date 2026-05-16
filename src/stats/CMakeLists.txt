# =============================================================================
# src/stats/CMakeLists.txt — Observability and instrumentation framework
# =============================================================================
#
# `keelstats` contains the cold-path half of KEEL's observability stack:
#
#   - Worker context allocation (per-worker stats structs, histogram buckets)
#   - Histogram aggregation (P50/P95/P99 latency computation)
#   - Process snapshots (FD counts, memory RSS, CPU time via /proc)
#   - Optional Linux system sampling (KEEL_ENABLE_SYSTEM_STATS)
#
# The hot-path counter and latency macros (KEEL_STAT_INC, KEEL_STAT_LATENCY)
# live in the public header <keel/stats.h> so workers can record events
# without paying function-call cost — they compile to atomic increments
# on cache-line-aligned counters.
#
# Stats tiers:
#   L1-L2: Always on (counters, gauges) — zero measurable overhead
#   L3: KEEL_ENABLE_SYSTEM_STATS — /proc sampling every ~1 s
#   L4: KEEL_ENABLE_TRACE — USDT probe points for perf/bpftrace
#   L5: KEEL_ENABLE_FULL_STATS — per-query SQL fingerprint aggregation
# =============================================================================

add_library(keelstats STATIC
    stats.c
)

target_include_directories(keelstats
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

# Dependencies:
#   keelmem  — allocator for collector/context storage
#   keellog  — lifecycle and reset logging
target_link_libraries(keelstats
    PUBLIC  keelmem
    PUBLIC  keellog
    PUBLIC  Threads::Threads
)

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelstats PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelstats PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keelstats PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
