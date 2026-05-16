# ============================================================================
# Virtlabs KEEL
# CLI subsystem build definition
# Author: Charly Batista
# License: GNU Affero General Public License v3.0
# https://virtlabs.io
# ============================================================================
#
# Builds the standalone `keel-cli` utility used to query the admin endpoint from
# shells, scripts, and operator workflows.

add_executable(keel-cli
    keel_cli.c
)

target_include_directories(keel-cli
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_BINARY_DIR}/include
)

set_target_properties(keel-cli PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    OUTPUT_NAME keel-cli
)

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_link_options(keel-cli PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

# Install target
install(TARGETS keel-cli
    RUNTIME DESTINATION bin
)
