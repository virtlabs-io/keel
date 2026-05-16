# =============================================================================
# src/sql/CMakeLists.txt — Layered SQL analysis pipeline
# =============================================================================
#
# This target groups KEEL's layered SQL analysis pipeline:
#
#   lexer.c      — Allocation-free token scanner for hot-path query classification.
#                  Operates on raw bytes without copying, returning tokens that
#                  encode the verb, statement type, and effect flags needed for
#                  the router's read/write split decision.
#
#   analyzer.c   — Higher-level query-type classification that combines lexer
#                  output with semantic rules (e.g. SELECT FOR UPDATE → write).
#
#   parser.c     — Pragmatic recursive-descent parser for common statements.
#                  Extracts table names, prepared-statement identifiers, and
#                  transaction control verbs without building a full AST.
#
#   query_tree.c — Semantic Query Tree lifting for cache and invalidation logic.
#                  Builds a lightweight tree representation used by the query
#                  logging and hook subsystems.
#
# Keeping the pieces together in one library makes the boundary between
# protocol handling and SQL-awareness explicit without forcing every caller
# onto the full parser path.
# =============================================================================

add_library(keelsql STATIC
    lexer.c
    analyzer.c
    parser.c
    query_tree.c
)

target_include_directories(keelsql
    PUBLIC ${KEEL_INCLUDE_DIR}
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(keelsql
    PUBLIC keelmem
    PUBLIC keelutil
)

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelsql PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelsql PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keelsql PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
