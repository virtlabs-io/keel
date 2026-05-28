# ============================================================================
# Virtlabs KEEL
# Core subsystem build definition
# Author: Charly Batista
# License: GNU Affero General Public License v3.0
# https://virtlabs.io
# ============================================================================
#
# Builds the core runtime services used by the pooler outside of the engine
# event loop itself:
# - authentication helpers
# - configuration parsing
# - router selection, metadata, and discovery logic

add_library(keelcore STATIC
    auth.c
    cloud_auth.c
    cluster.c
    config.c
    config_migrate.c
    config_yaml.c
    query_cache.c
    query_rules.c
    throttle.c
    compress.c
    sharding.c
    scatter_store.c
    scatter_2pc.c
    router_weighted.c
    router_plugin.c
    router_metadata.c
    router_discovery.c
    # connpool.c and proxy_session.c are TEST SUPPORT UTILITIES.
    # They live in tests/support/ and are compiled into the test_support
    # static library.  They are NOT part of the production keelcore library.
)

target_include_directories(keelcore
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(keelcore
    PUBLIC keelmem
    PUBLIC keelutil
    PUBLIC keelsql
    PUBLIC keelparser
    PUBLIC keel_hook
    PUBLIC z
    PUBLIC m
)

# libyaml (hard dep since config v2): prefer the PkgConfig imported target
# (carries include dirs + libs) but fall back to manual variables.
if(TARGET PkgConfig::YAML)
    target_link_libraries(keelcore PUBLIC PkgConfig::YAML)
else()
    target_include_directories(keelcore PUBLIC ${YAML_INCLUDE_DIRS})
    target_link_libraries(keelcore PUBLIC ${YAML_LIBRARIES})
endif()

# OpenSSL for SCRAM-SHA-256 authentication
find_package(OpenSSL)
if(OpenSSL_FOUND)
    target_link_libraries(keelcore PRIVATE OpenSSL::Crypto)
    target_compile_definitions(keelcore PUBLIC KEEL_HAS_OPENSSL=1)
endif()

# Optional: libzstd for cluster wire-protocol compression
if(KEEL_HAS_ZSTD)
    target_include_directories(keelcore PRIVATE ${ZSTD_INCLUDE_DIR})
    target_link_libraries(keelcore PRIVATE ${ZSTD_LIBRARY})
    target_compile_definitions(keelcore PUBLIC KEEL_HAS_ZSTD=1)
else()
    target_compile_definitions(keelcore PUBLIC KEEL_HAS_ZSTD=0)
endif()

# Optional: libldap for LDAP authentication
if(KEEL_HAS_LDAP)
    target_include_directories(keelcore PRIVATE ${LDAP_INCLUDE_DIR})
    target_link_libraries(keelcore PRIVATE ${LDAP_LIBRARY})
    target_compile_definitions(keelcore PUBLIC KEEL_HAS_LDAP=1)
endif()

# Optional: libpam for PAM authentication
if(KEEL_HAS_PAM)
    target_include_directories(keelcore PRIVATE ${PAM_INCLUDE_DIR})
    target_link_libraries(keelcore PRIVATE ${PAM_LIBRARY})
    target_compile_definitions(keelcore PUBLIC KEEL_HAS_PAM=1)
endif()

# Optional: libpq for auth_query (SQL-based password lookup)
if(KEEL_HAS_LIBPQ)
    target_include_directories(keelcore PRIVATE ${PQ_INCLUDE_DIR})
    target_link_libraries(keelcore PRIVATE ${PQ_LIBRARY})
    target_compile_definitions(keelcore PUBLIC KEEL_HAS_LIBPQ=1)
endif()

# Coverage support
if(KEEL_ENABLE_COVERAGE)
    target_compile_options(keelcore PRIVATE ${KEEL_COVERAGE_FLAGS})
    target_link_options(keelcore PRIVATE ${KEEL_COVERAGE_FLAGS})
endif()

set_target_properties(keelcore PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)
