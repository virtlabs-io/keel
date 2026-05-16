#
# KEEL Log Subsystem Build Definition
# Copyright (c) Virtlabs, https://virtlabs.io
# License: GNU Affero General Public License v3.0
# Author: Charly Batista
#
# Builds the structured logging plugin framework, the built-in sink plugins,
# the dynamic sink loader, and the query-logging layer that translates parsed
# SQL events into structured log records.

add_library(keellog_plugin STATIC
    log_plugin_stdout.c
    log_plugin_file.c
    log_plugin_syslog.c
    log_plugin_loader.c
    query_log.c
    audit_log.c
)

target_include_directories(keellog_plugin
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
)

# We link against:
#   keellog      — the existing core log system (for KEEL_LOG_INFO etc.)
#   keelsql      — SQL parser/analyzer and query tree (for tree logging)
#   keelmem      — arena allocator (for query tree parsing)
#   dl          — dlopen/dlsym/dlclose for plugin loading
#   Threads     — pthread_mutex for thread safety
target_link_libraries(keellog_plugin
    PUBLIC  keellog
    PUBLIC  keelsql
    PUBLIC  keelmem
    PRIVATE ${CMAKE_DL_LIBS}
    PUBLIC  Threads::Threads
)

set_target_properties(keellog_plugin PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keellog_plugin PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keellog_plugin PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()
