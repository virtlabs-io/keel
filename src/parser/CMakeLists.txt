# =============================================================================
# Parser plugin contract and builtin parser implementations
# =============================================================================

add_library(keelparser STATIC
    semantic_plan.c
    parser_registry.c
    parser_dispatch.c
    sql/postgresql/pg_sql_parser.c
)

target_include_directories(keelparser
    PUBLIC ${KEEL_INCLUDE_DIR}
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(keelparser
    PUBLIC keelmem
    PUBLIC keelutil
    PUBLIC keelsql
)

if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelparser PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelparser PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keelparser PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
