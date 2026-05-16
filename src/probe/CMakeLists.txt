#[[
 @file CMakeLists.txt
 @brief Build rules for the probe and failover-control library.
 @author Charly Batista

 Copyright (C) Virtlabs, https://virtlabs.io

 The probe library packages the protocol-specific health checkers together with
 the registry and manager that turn raw probe observations into routing-visible
 topology updates. Linking the implementations into one library keeps startup and
 deployment simple while still preserving a plugin-style abstraction boundary in
 code through `keel_probe_ops_t`.
]]

add_library(keel_probe STATIC
    probe_common.c
    probe_registry.c
    probe_postgres.c
    probe_postgres_discovery.c
    probe_patroni.c
    probe_mysql.c
    probe_manager.c
)

target_include_directories(keel_probe
    PUBLIC  ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)

target_compile_options(keel_probe PRIVATE ${KEEL_WARNING_FLAGS})

# Link dependencies
target_link_libraries(keel_probe
    PRIVATE keelmem
    PRIVATE keelutil
    PRIVATE keel_proto_postgres
    PRIVATE Threads::Threads
    PRIVATE OpenSSL::SSL
    PRIVATE OpenSSL::Crypto
)

set_target_properties(keel_probe PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
)
