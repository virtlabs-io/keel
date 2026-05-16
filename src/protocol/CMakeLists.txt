#[[
 @file CMakeLists.txt
 @brief Build rules for protocol flows, backend-auth helpers, and TLS transport support.
 @author Charly Batista

 Copyright (C) Virtlabs, https://virtlabs.io

 The protocol subsystem is split into multiple libraries on purpose. Flow plugins
 remain protocol-specific and link only what they need; the flow registry provides
 shared discovery; and TLS support is isolated so deployments that reason about
 transport costs or security boundaries can see that dependency edge clearly.
]]

#[[
 Protocol flow registry (name → flow vtable mapping).
 This is the canonical dispatch table for protocol implementations; older
 protocol-vtable mechanisms have been retired.
]]
add_library(keel_proto_flow_registry STATIC
    protocol_flow_registry.c
)

target_include_directories(keel_proto_flow_registry
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(keel_proto_flow_registry
    PUBLIC Threads::Threads
)

#[[
 PostgreSQL protocol plugin and backend-auth helpers.
]]
add_library(keel_proto_postgres STATIC
    postgres/postgres_flow.c
    postgres/pg_backend_auth.c
    postgres/pg_scatter.c
)

target_include_directories(keel_proto_postgres
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(keel_proto_postgres
    PUBLIC keelutil
    PUBLIC keelsql
    PUBLIC keel_proto_flow_registry
    PUBLIC OpenSSL::Crypto
)

#[[
 MySQL/MariaDB protocol plugin and backend-auth helpers.
]]
add_library(keel_proto_mysql STATIC
    mysql/mysql_flow.c
    mysql/mysql_backend_auth.c
    mysql/mysql_scatter.c
)

target_include_directories(keel_proto_mysql
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(keel_proto_mysql
    PUBLIC keelutil
    PUBLIC keelsql
    PUBLIC keel_proto_flow_registry
    PUBLIC OpenSSL::Crypto
)

#[[
 Convenience target that groups the flow registry and built-in protocol plugins.
]]
add_library(keel_protocol INTERFACE)
target_link_libraries(keel_protocol
    INTERFACE keel_proto_flow_registry
    INTERFACE keel_proto_postgres
    INTERFACE keel_proto_mysql
)

#[[
 TLS context management and Linux kTLS integration.
]]
add_library(keel_tls STATIC
    tls_context.c
    tls_auto.c
    ktls_linux.c
)

target_include_directories(keel_tls
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(keel_tls
    PUBLIC OpenSSL::Crypto
    PUBLIC OpenSSL::SSL
    PUBLIC keelmem
    PUBLIC keelutil
)
