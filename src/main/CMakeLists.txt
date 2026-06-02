#[[
 @file CMakeLists.txt
 @brief Build rules for the KEEL process entry-point executable.
 @author Charly Batista

 Copyright (C) Virtlabs, https://virtlabs.io

 This program is free software: you can redistribute it and/or modify it under
 the terms of the GNU Affero General Public License as published by the Free
 Software Foundation, either version 3 of the License, or (at your option) any
 later version.

 This target is intentionally thin: it compiles the single bootstrap translation
 unit in this directory and links it against the subsystem libraries that hold
 the real data-plane logic. Keeping the executable target small makes it clear
 that `src/main` is orchestration glue rather than another feature library.

 The dependency list is nevertheless important because it documents the runtime
 surface pulled into the final binary: engine orchestration, worker/session flow,
 protocol implementations, admin endpoints, hooks, probes, memory utilities,
 logging plugins, and TLS support all converge here.
]]

add_executable(keel
    main.c
)

# Export dynamic symbols so that diagnostic backtraces (e.g. allocator
# corruption reports in src/mem/mem.c) can resolve frame addresses to
# function names without needing addr2line + the original build tree.
set_target_properties(keel PROPERTIES ENABLE_EXPORTS ON)

target_include_directories(keel
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)

# Link against new architecture libraries (engine, reactor, worker, session)
target_link_libraries(keel
    PRIVATE keelengine
    PRIVATE keel_engine_flow
    PRIVATE keel_worker_new
    PRIVATE keel_session
    PRIVATE keel_arch_common
    PRIVATE keel_arch_platform
    PRIVATE keel_proto_flow_registry
    PRIVATE keel_proto_postgres
    PRIVATE keel_proto_mysql
    PRIVATE keellog_plugin
    PRIVATE keelstats
    PRIVATE keelcore
    PRIVATE keel_admin
    PRIVATE keel_probe
    PRIVATE keel_failover
    PRIVATE keel_hook
    PRIVATE keelsql
    PRIVATE keelmem
    PRIVATE keelutil
    PRIVATE keel_tls
    PRIVATE keeltrace
)

if(KEEL_ENABLE_OTLP)
    target_link_libraries(keel PRIVATE keel_otlp)
endif()

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_link_options(keel PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keel PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    OUTPUT_NAME keel
)

# Install target
install(TARGETS keel
    RUNTIME DESTINATION bin
)
