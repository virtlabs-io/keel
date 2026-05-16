# ============================================================================
# Virtlabs KEEL
# Administrative subsystem build definition
# Author: Charly Batista
# License: GNU Affero General Public License v3.0
# https://virtlabs.io
# ============================================================================
#
# This target builds the admin/control-plane library used for:
# - PostgreSQL-wire operational commands
# - Prometheus/OpenMetrics export
# - low-frequency operator introspection paths

add_library(keel_admin STATIC
    admin.c
    web_ui.c
)

target_include_directories(keel_admin
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)

target_link_libraries(keel_admin
    PRIVATE keelstats
    PRIVATE keelengine
    PRIVATE keelcore
    PRIVATE keelsql
    PRIVATE keelmem
    PRIVATE pthread
)

# Minimal library containing only the embedded HTML string.
# Tests link this instead of full keel_admin to avoid pulling in the engine.
add_library(keel_web_ui STATIC
    web_ui.c
)

target_include_directories(keel_web_ui
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)
