#
# KEEL Engine Subsystem Build Definition
# Copyright (c) Virtlabs, https://virtlabs.io
# License: GNU Affero General Public License v3.0
# Author: Charly Batista
#
# This CMake file builds the engine lifecycle library and the protocol-agnostic
# flow/state-machine support library it depends on. Splitting the flow logic
# into a separate static target avoids circular dependencies with the worker
# implementation while preserving a clean link graph for the full engine.

# Engine flow — protocol-agnostic session driver
# Separate lib so worker can link it without circular dep
add_library(keel_engine_flow STATIC
    engine_flow.c
    engine_scatter.c
    engine_catchup_bridge.c
    invariant.c
    session_flow_pins.c
    state_machine.c
)

target_include_directories(keel_engine_flow
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
)

target_link_libraries(keel_engine_flow
    PUBLIC keel_session
    PUBLIC keel_proto_flow_registry
    PUBLIC keel_proto_postgres
    PUBLIC keel_proto_mysql
    PUBLIC keel_arch_platform
    PUBLIC keel_hook
    PUBLIC keellog_plugin
    PUBLIC keelsql
    PUBLIC keelmem
    PUBLIC keelutil
    PUBLIC keel_tls
    PUBLIC keeltrace
    PUBLIC OpenSSL::Crypto
    PUBLIC keelcore
    PUBLIC keel_probe
)

# Full engine (startup, lifecycle)
add_library(keelengine STATIC
    engine.c
)

target_include_directories(keelengine
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
)

target_link_libraries(keelengine
    PUBLIC keel_session
    PUBLIC keel_arch_common
    PUBLIC keel_arch_platform
    PUBLIC keel_worker_new
    PUBLIC keel_engine_flow
    PUBLIC keel_proto_flow_registry
    PUBLIC keelsql
    PUBLIC keelmem
    PUBLIC keelutil
    PUBLIC keeltrace
)

# Platform-specific dependencies
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(URING_FOUND)
        target_link_libraries(keelengine PUBLIC ${URING_LIBRARIES})
    endif()
endif()
