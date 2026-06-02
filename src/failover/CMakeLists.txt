#[[
 @file CMakeLists.txt
 @brief Build rules for the failover-manager library.
 @author Charly Batista

 Copyright (C) Virtlabs, https://virtlabs.io
]]

add_library(keel_failover STATIC
    failover_manager.c
)

target_include_directories(keel_failover
    PUBLIC  ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)

target_compile_options(keel_failover PRIVATE ${KEEL_WARNING_FLAGS})

target_link_libraries(keel_failover
    PRIVATE keelmem
    PRIVATE keelutil
    PRIVATE Threads::Threads
)
