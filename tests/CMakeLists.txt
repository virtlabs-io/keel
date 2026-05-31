# =============================================================================
# tests/CMakeLists.txt — Test suite build graph for the Keel proxy
# =============================================================================
#
# The test tree mixes several styles of verification:
# - small unit tests that link only the subsystem under test,
# - heavier integration/runtime tests that compose large slices of the proxy,
# - protocol and security regression binaries that intentionally mirror real
#   deployment link graphs more closely than a classic unit test would.
#
# This file therefore serves as both build configuration and an index of how
# the project thinks about correctness boundaries.
#
# Test binary naming convention:
#   test_<module>            — focused unit tests for a single subsystem
#   test_<module>_<variant>  — specialised variant (e.g. test_tls_security)
#   test_<feature>_matrix    — combinatorial cross-feature matrix tests
#   test_<feature>_e2e       — Docker-backed end-to-end integration tests
#
# Linking strategy:
#   Unit tests link the minimum set of static libraries.  Integration-style
#   tests link the full proxy library graph and use --start-group / --end-group
#   on Linux to resolve cyclic dependencies between static archives.
#
# Module labels (ctest -L <label>):
#   util, sql, core, session, pool, protocol, engine, hardening, integration
#
# Per-module custom targets (cmake --build . --target test-<module>):
#   test-util, test-sql, test-core, test-session, test-pool, test-protocol,
#   test-engine, test-hardening, test-integration, test-all
# =============================================================================

find_package(Threads REQUIRED)

# Shared assertion/test-counter support for the small C unit tests.
# Every test binary links this static library to get the TEST_ASSERT,
# TEST_START, TEST_PASS, and test_summary() macros/functions.
add_library(test_utils STATIC
    test_utils.c
)

# =============================================================================
# Test Support Library
# =============================================================================
#
# Synchronous connection pool (keel_connpool_t) and proxy session
# (keel_client_session_t) are TEST SUPPORT UTILITIES, not production engine
# code.  Production pooling uses src/worker/backend_pool.c (async, per-worker,
# reactor-integrated).  These utilities live in tests/support/ and are compiled
# into this library so each test binary that needs real TCP connectivity can
# link it without depending on the full async engine.
add_library(test_support STATIC
    support/connpool.c
    support/proxy_session.c
)
target_include_directories(test_support
    PUBLIC  ${CMAKE_SOURCE_DIR}/include
    PUBLIC  ${CMAKE_BINARY_DIR}/include
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/support
)
target_link_libraries(test_support PUBLIC keelcore Threads::Threads)

# Coverage instrumentation is applied at the executable layer so individual test
# binaries can be run under ctest while still contributing to the aggregate
# coverage report when that mode is enabled.
if(KEEL_ENABLE_COVERAGE)
    set(TEST_COVERAGE_LINK_FLAGS --coverage)
endif()

# Linux static linking needs start/end group markers for the larger runtime-style
# tests where several static libraries depend on one another cyclically.
# Without these markers, the GNU linker makes a single pass and misses symbols
# that appear in a later archive but are needed by an earlier one.  macOS ld64
# resolves cycles automatically, so the markers are empty on Darwin.
if(UNIX AND NOT APPLE)
    set(LINUX_START_GROUP -Wl,--start-group)
    set(LINUX_END_GROUP -Wl,--end-group)
else()
    set(LINUX_START_GROUP "")
    set(LINUX_END_GROUP "")
endif()

target_include_directories(test_utils
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

# ============================================================================
# Unit and Focused Regression Tests
# ============================================================================
# Each test below exercises one subsystem in isolation (or near-isolation).
# They link only the libraries strictly needed for that module, keeping
# compile and link times fast and failure diagnostics narrow.
# ============================================================================

# Memory tests (arena, slab, pool allocators)
add_executable(test_mem test_mem.c)
target_link_libraries(test_mem PRIVATE test_utils keelutil Threads::Threads)
target_link_options(test_mem PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_mem COMMAND test_mem)

# Distributed tracing tests (W3C context, spans, ring buffer, OTLP)
add_executable(test_trace test_trace.c)
target_link_libraries(test_trace PRIVATE test_utils keeltrace keelmem keellog Threads::Threads)
target_link_options(test_trace PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_trace COMMAND test_trace)

# Authentication tests (SCRAM-SHA-256, MD5)
add_executable(test_auth test_auth.c)
target_link_libraries(test_auth PRIVATE 
    test_utils 
    keelcore 
    OpenSSL::Crypto 
    Threads::Threads
)
target_compile_definitions(test_auth PRIVATE KEEL_HAS_OPENSSL=1)
target_link_options(test_auth PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_auth COMMAND test_auth)

# Admin console authentication tests
add_executable(test_admin_auth test_admin_auth.c)
target_link_libraries(test_admin_auth PRIVATE
    test_utils
    keelcore
    OpenSSL::Crypto
    Threads::Threads
)
target_compile_definitions(test_admin_auth PRIVATE KEEL_HAS_OPENSSL=1)
target_link_options(test_admin_auth PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_admin_auth COMMAND test_admin_auth)

# Cloud-native authentication tests
add_executable(test_cloud_auth test_cloud_auth.c)
target_link_libraries(test_cloud_auth PRIVATE
    test_utils
    keelcore
    OpenSSL::Crypto
    Threads::Threads
)
target_compile_definitions(test_cloud_auth PRIVATE KEEL_HAS_OPENSSL=1)
target_link_options(test_cloud_auth PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cloud_auth COMMAND test_cloud_auth)
set_tests_properties(test_cloud_auth PROPERTIES LABELS "openssl;core")

# Cloud-native authentication internals (white-box) tests
add_executable(test_cloud_auth_internals test_cloud_auth_internals.c)
target_include_directories(test_cloud_auth_internals PRIVATE
    ${CMAKE_SOURCE_DIR}/src/core
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_BINARY_DIR}/include
)
target_link_libraries(test_cloud_auth_internals PRIVATE
    test_utils
    keelmem
    keelutil
    keelsql
    OpenSSL::Crypto
    Threads::Threads
)
target_compile_definitions(test_cloud_auth_internals PRIVATE KEEL_HAS_OPENSSL=1)
target_link_options(test_cloud_auth_internals PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cloud_auth_internals COMMAND test_cloud_auth_internals)
set_tests_properties(test_cloud_auth_internals PROPERTIES LABELS "openssl;core")

# Cloud authentication end-to-end (E2E) integration tests
add_executable(test_cloud_auth_e2e test_cloud_auth_e2e.c)
target_link_libraries(test_cloud_auth_e2e PRIVATE
    test_utils
    keelcore
    OpenSSL::Crypto
    Threads::Threads
)
target_compile_definitions(test_cloud_auth_e2e PRIVATE KEEL_HAS_OPENSSL=1)
target_link_options(test_cloud_auth_e2e PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cloud_auth_e2e COMMAND test_cloud_auth_e2e)
set_tests_properties(test_cloud_auth_e2e PROPERTIES LABELS "openssl;core")

# Connection lifecycle management tests
add_executable(test_conn_lifecycle test_conn_lifecycle.c)
target_include_directories(test_conn_lifecycle PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_conn_lifecycle PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keel_proto_flow_registry
    keelcore
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_conn_lifecycle PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_conn_lifecycle COMMAND test_conn_lifecycle)

# SQL Parser tests
add_executable(test_parser test_parser.c)
target_link_libraries(test_parser PRIVATE 
    test_utils 
    keelsql 
    Threads::Threads
)
target_link_options(test_parser PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_parser COMMAND test_parser)

# Parser plugin registry and builtin PostgreSQL semantic-plan contract
add_executable(test_parser_registry test_parser_registry.c)
target_link_libraries(test_parser_registry PRIVATE
    test_utils
    keelparser
    Threads::Threads
)
target_link_options(test_parser_registry PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_parser_registry COMMAND test_parser_registry)
set_tests_properties(test_parser_registry PROPERTIES LABELS "parser;core")

# Admin SQL query language tests
add_executable(test_admin_sql test_admin_sql.c)
target_link_libraries(test_admin_sql PRIVATE
    test_utils
    keelsql
    keelmem
    Threads::Threads
)
target_link_options(test_admin_sql PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_admin_sql COMMAND test_admin_sql)

# Admin SQL parser adversarial fuzz test (§17.1, §30.13)
add_executable(test_admin_sql_fuzz test_admin_sql_fuzz.c)
target_link_libraries(test_admin_sql_fuzz PRIVATE
    test_utils
    keelsql
    keelmem
    Threads::Threads
)
target_link_options(test_admin_sql_fuzz PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_admin_sql_fuzz COMMAND test_admin_sql_fuzz)
set_tests_properties(test_admin_sql_fuzz PROPERTIES LABELS "sql;hardening;fuzz")

# Scatter SQL parser adversarial fuzz harness (Phase 1 — scatter query surface).
# Exercises COUNT(DISTINCT), malformed aggregates, window function mixing,
# no-shard-key fall-through, injection patterns, and boundary inputs.
# Binary seeds in fuzz_seeds/scatter_*.bin serve as AFL++ starting corpus.
add_executable(test_scatter_sql_fuzz test_scatter_sql_fuzz.c)
target_link_libraries(test_scatter_sql_fuzz PRIVATE
    test_utils
    keelsql
    keelmem
    Threads::Threads
)
target_link_options(test_scatter_sql_fuzz PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_scatter_sql_fuzz COMMAND test_scatter_sql_fuzz)
set_tests_properties(test_scatter_sql_fuzz PROPERTIES
    LABELS "sql;sharding;fuzz;hardening"
    TIMEOUT 60)

# Query result caching tests
add_executable(test_query_cache test_query_cache.c)
target_link_libraries(test_query_cache PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_query_cache PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_query_cache COMMAND test_query_cache)
set_tests_properties(test_query_cache PROPERTIES LABELS "openssl;core")

# Sharding helper tests
add_executable(test_sharding test_sharding.c)
target_link_libraries(test_sharding PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_sharding PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_sharding COMMAND test_sharding)

# Scatter result store tests (Phase A)
add_executable(test_scatter_store test_scatter_store.c)
target_link_libraries(test_scatter_store PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_scatter_store PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_scatter_store COMMAND test_scatter_store)
set_tests_properties(test_scatter_store PROPERTIES LABELS "core;sharding")

# Window function Phase F unit tests (ROW_NUMBER, RANK, DENSE_RANK, NTILE,
# PERCENT_RANK, CUME_DIST - both in-memory and spill paths)
add_executable(test_scatter_window test_scatter_window.c)
target_link_libraries(test_scatter_window PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_scatter_window PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_scatter_window COMMAND test_scatter_window)
set_tests_properties(test_scatter_window PROPERTIES LABELS "core;sharding")

# Two-phase commit coordinator tests (Phase G)
add_executable(test_scatter_2pc test_scatter_2pc.c)
target_link_libraries(test_scatter_2pc PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_scatter_2pc PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_scatter_2pc COMMAND test_scatter_2pc)
set_tests_properties(test_scatter_2pc PROPERTIES LABELS "core;sharding")
# Scatter merge end-to-end integration tests (requires a live PostgreSQL node)
# Exercises the full scatter -> merge pipeline: COUNT, SUM, AVG finalisation,
# GROUP BY hash-merge, HAVING post-filter, and ORDER BY sort-merge.
# Set KEEL_TEST_PG_HOST1 / KEEL_TEST_PG_PORT1 to point at a live instance.
add_executable(test_scatter_merge_e2e test_scatter_merge_e2e.c)
target_link_libraries(test_scatter_merge_e2e PRIVATE
    test_utils
    test_integration
    keelcore
    Threads::Threads
)
target_link_options(test_scatter_merge_e2e PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_scatter_merge_e2e COMMAND test_scatter_merge_e2e)
set_tests_properties(test_scatter_merge_e2e PROPERTIES
    LABELS "e2e;sharding;integration"
    TIMEOUT 120)

# Two-phase commit fault-injection integration tests (requires a live PostgreSQL
# node with max_prepared_transactions > 0).  Tests skip gracefully when 2PC is
# disabled.  Set KEEL_TEST_PG_HOST1 / KEEL_TEST_PG_PORT1 and ensure
# max_prepared_transactions >= 10 in postgresql-overrides.conf.
add_executable(test_2pc_fault_inject test_2pc_fault_inject.c)
target_link_libraries(test_2pc_fault_inject PRIVATE
    test_utils
    test_integration
    keelcore
    Threads::Threads
)
target_link_options(test_2pc_fault_inject PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_2pc_fault_inject COMMAND test_2pc_fault_inject)
set_tests_properties(test_2pc_fault_inject PROPERTIES
    LABELS "e2e;sharding;2pc;integration"
    TIMEOUT 120)


# Connection pool tests (Feature 11)
add_executable(test_connpool test_connpool.c)
target_link_libraries(test_connpool PRIVATE
    test_utils
    test_support
    keelcore
    Threads::Threads
)
target_include_directories(test_connpool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_options(test_connpool PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_connpool COMMAND test_connpool)

# Proxy session tests (Feature 12)
add_executable(test_proxy_session test_proxy_session.c)
target_link_libraries(test_proxy_session PRIVATE
    test_utils
    test_support
    keelcore
    Threads::Threads
)
target_include_directories(test_proxy_session PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_options(test_proxy_session PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_proxy_session COMMAND test_proxy_session)

# Shard hot-reload tests (Feature 13)
add_executable(test_shard_hot_reload test_shard_hot_reload.c)
target_link_libraries(test_shard_hot_reload PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_shard_hot_reload PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_shard_hot_reload COMMAND test_shard_hot_reload)

# Prometheus router metrics tests (Feature 14)
add_executable(test_router_metrics test_router_metrics.c)
target_link_libraries(test_router_metrics PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_router_metrics PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_router_metrics COMMAND test_router_metrics)

# Query timeout tests (Feature 15)
add_executable(test_router_timeout test_router_timeout.c)
target_link_libraries(test_router_timeout PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_router_timeout PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_router_timeout COMMAND test_router_timeout)

# Catch-up wait list (Phase 2a — reactor-owned WAIT loop scaffolding)
add_executable(test_catchup_waitlist test_catchup_waitlist.c)
target_link_libraries(test_catchup_waitlist PRIVATE
    test_utils
    keel_worker_new
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_catchup_waitlist PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_catchup_waitlist COMMAND test_catchup_waitlist)

# Stress / fuzz tests for router dispatch and connpool (Tier 6 / Feature 15+)
add_executable(test_router_stress test_router_stress.c)
target_link_libraries(test_router_stress PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_router_stress PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_router_stress COMMAND test_router_stress)

add_executable(test_connpool_stress test_connpool_stress.c)
target_link_libraries(test_connpool_stress PRIVATE
    test_utils
    test_support
    keelcore
    Threads::Threads
)
target_include_directories(test_connpool_stress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_options(test_connpool_stress PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_connpool_stress COMMAND test_connpool_stress)

# Regression torture: observer-thread + waiter wakeup invariant
# Pins the ACTIVE_WITHOUT_OWNER race that surfaced when adding a new
# observer pthread (failover_manager) alongside the worker pool.
add_executable(test_pool_observer_race test_pool_observer_race.c)
target_include_directories(test_pool_observer_race PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_pool_observer_race PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keel_proto_flow_registry
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_pool_observer_race PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_pool_observer_race COMMAND test_pool_observer_race)

# Connection pool exhaustion and wait-queue tests
add_executable(test_connpool_exhaust test_connpool_exhaust.c)
target_link_libraries(test_connpool_exhaust PRIVATE
    test_utils
    test_support
    keelcore
    Threads::Threads
)
target_include_directories(test_connpool_exhaust PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_options(test_connpool_exhaust PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_connpool_exhaust COMMAND test_connpool_exhaust)

# Allocation-failure injection tests
add_executable(test_alloc_inject test_alloc_inject.c)
target_link_libraries(test_alloc_inject PRIVATE
    test_utils
    test_support
    keelcore
    Threads::Threads
)
target_include_directories(test_alloc_inject PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_options(test_alloc_inject PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_alloc_inject COMMAND test_alloc_inject)

# Router tests (weighted routing, read/write splitting)
add_executable(test_router test_router.c)
target_link_libraries(test_router PRIVATE 
    test_utils 
    keelcore
    Threads::Threads
)
target_link_options(test_router PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_router COMMAND test_router)

# Failover-manager tests (cluster epoch, fencing, degraded mode, timeline)
add_executable(test_failover_manager test_failover_manager.c)
target_link_libraries(test_failover_manager PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_failover_manager PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_failover_manager COMMAND test_failover_manager)
set_tests_properties(test_failover_manager PROPERTIES LABELS "core")

# Router explainer (read-only per-query simulation)
add_executable(test_router_explain test_router_explain.c)
target_link_libraries(test_router_explain PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_router_explain PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_router_explain COMMAND test_router_explain)
set_tests_properties(test_router_explain PROPERTIES LABELS "core")

# Router Plugin tests (pluggable routing, metadata cache, discovery)
add_executable(test_router_plugin test_router_plugin.c)
target_link_libraries(test_router_plugin PRIVATE 
    test_utils 
    keelcore
    Threads::Threads
)
target_link_options(test_router_plugin PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_router_plugin COMMAND test_router_plugin)

# Utility library tests (strings, hash, hash ring, time)
add_executable(test_util test_util.c)
target_link_libraries(test_util PRIVATE 
    test_utils 
    keelutil 
    Threads::Threads
)
target_link_options(test_util PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_util COMMAND test_util)

# SQL tests (lexer, parser, query classification)
add_executable(test_sql test_sql.c)
target_link_libraries(test_sql PRIVATE 
    test_utils 
    keelsql 
    Threads::Threads
)
target_link_options(test_sql PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_sql COMMAND test_sql)

# Configuration tests (INI parser)
add_executable(test_config test_config.c)
target_link_libraries(test_config PRIVATE 
    test_utils 
    keelcore
    Threads::Threads
)
target_link_options(test_config PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_config COMMAND test_config)

# Configuration tests (YAML loader + converters)
add_executable(test_config_yaml test_config_yaml.c)
target_link_libraries(test_config_yaml PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_config_yaml PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_config_yaml COMMAND test_config_yaml)
# libyaml is not compiled with MSan instrumentation (same reason OpenSSL tests
# are excluded). Skip this test in MSan builds to avoid false-positive
# use-of-uninitialized-value reports from uninstrumented libyaml memory.
set_tests_properties(test_config_yaml PROPERTIES LABELS "core;openssl")

# Issue 9 — Result Cache Must Remain Disabled: config-layer gate tests
add_executable(test_result_cache_gate test_result_cache_gate.c)
target_link_libraries(test_result_cache_gate PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_result_cache_gate PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_result_cache_gate COMMAND test_result_cache_gate)
set_tests_properties(test_result_cache_gate PROPERTIES LABELS "core")

# Configuration reload tests (SIGHUP live reload infrastructure)
add_executable(test_config_reload test_config_reload.c)
target_link_libraries(test_config_reload PRIVATE 
    test_utils 
    keelcore
    Threads::Threads
)
target_link_options(test_config_reload PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_config_reload COMMAND test_config_reload)
set_tests_properties(test_config_reload PROPERTIES LABELS "core")

# NOTIFY/LISTEN transparent proxying unit tests
add_executable(test_listen_notify test_listen_notify.c)
target_link_libraries(test_listen_notify PRIVATE
    test_utils
    keelsql
    keel_session
    keelcore
    keelstats
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_listen_notify PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_listen_notify COMMAND test_listen_notify)
set_tests_properties(test_listen_notify PROPERTIES LABELS "core")

# Declarative query routing/blocking/rewriting rules
add_executable(test_query_rules test_query_rules.c)
target_link_libraries(test_query_rules PRIVATE
    test_utils
    keelcore
    Threads::Threads
)
target_link_options(test_query_rules PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_query_rules COMMAND test_query_rules)
set_tests_properties(test_query_rules PROPERTIES LABELS "core")

# Online Schema Change (gh-ost / pt-osc) transparent proxying unit tests
add_executable(test_osc_proxying test_osc_proxying.c)
target_link_libraries(test_osc_proxying PRIVATE
    test_utils
    keel_session
    keelcore
    Threads::Threads
)
target_link_options(test_osc_proxying PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_osc_proxying COMMAND test_osc_proxying)
set_tests_properties(test_osc_proxying PROPERTIES LABELS "core")

add_executable(test_ryw_propagation test_ryw_propagation.c)
target_link_libraries(test_ryw_propagation PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_ryw_propagation PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ryw_propagation COMMAND test_ryw_propagation)
set_tests_properties(test_ryw_propagation PROPERTIES LABELS "core")

add_executable(test_web_ui test_web_ui.c)
target_link_libraries(test_web_ui PRIVATE
    test_utils
    keel_web_ui
    Threads::Threads
)
target_link_options(test_web_ui PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_web_ui COMMAND test_web_ui)
set_tests_properties(test_web_ui PROPERTIES LABELS "core")

# ── Audit log unit tests ──────────────────────────────────────────────────────
add_executable(test_audit_log test_audit_log.c)
target_link_libraries(test_audit_log PRIVATE
    test_utils
    keellog_plugin
    keellog
    keelcore
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_audit_log PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_audit_log COMMAND test_audit_log)
set_tests_properties(test_audit_log PROPERTIES LABELS "core")

# ── Token-bucket query throttling unit tests ──────────────────────────────────
add_executable(test_throttle test_throttle.c)
target_link_libraries(test_throttle PRIVATE
    test_utils
    keelcore
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_throttle PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_throttle COMMAND test_throttle)
set_tests_properties(test_throttle PROPERTIES LABELS "core")

# ── Protocol compression (zlib/gzip) unit tests ───────────────────────────────
add_executable(test_compress test_compress.c)
target_link_libraries(test_compress PRIVATE
    test_utils
    keelcore
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_compress PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_compress COMMAND test_compress)
set_tests_properties(test_compress PROPERTIES LABELS "core")

# TLS/kTLS regression tests (handshake, data path, kTLS accounting)
add_executable(test_tls_ktls test_tls_ktls.c)
target_link_libraries(test_tls_ktls PRIVATE
    test_utils
    keel_tls
    keelmem
    keelutil
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
)
target_link_options(test_tls_ktls PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_tls_ktls COMMAND test_tls_ktls)

# TLS security tests (cipher policy, mTLS, cert reload, downgrade, drain)
add_executable(test_tls_security test_tls_security.c)
target_link_libraries(test_tls_security PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_tls
    keel_session
    keelengine
    keel_engine_flow
    keel_worker_new
    keelcore
    keelstats
    keel_probe
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keellog_plugin
    keellog
    keel_arch_platform
    keel_arch_common
    keelmem
    keelutil
    keelsql
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_tls_security PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_tls_security COMMAND test_tls_security)

# Drain / shutdown lifecycle tests
add_executable(test_drain_shutdown test_drain_shutdown.c)
target_link_libraries(test_drain_shutdown PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keelengine
    keel_engine_flow
    keel_worker_new
    keel_session
    keelcore
    keelstats
    keel_probe
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_tls
    keel_hook
    keellog_plugin
    keellog
    keel_arch_platform
    keel_arch_common
    keelmem
    keelutil
    keelsql
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
if(KEEL_USE_IOURING AND URING_FOUND)
    target_link_libraries(test_drain_shutdown PRIVATE uring)
endif()
target_link_options(test_drain_shutdown PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_drain_shutdown COMMAND test_drain_shutdown)

# Admin live integration test (admin TCP console + Prometheus HTTP)
add_executable(test_admin_live test_admin_live.c)
target_link_libraries(test_admin_live PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_admin
    keelengine
    keel_engine_flow
    keel_worker_new
    keel_session
    keelcore
    keelstats
    keel_probe
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_tls
    keel_hook
    keellog_plugin
    keellog
    keel_arch_platform
    keel_arch_common
    keelmem
    keelutil
    keelsql
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
if(KEEL_USE_IOURING AND URING_FOUND)
    target_link_libraries(test_admin_live PRIVATE uring)
endif()
target_link_options(test_admin_live PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_admin_live COMMAND test_admin_live)
set_tests_properties(test_admin_live PROPERTIES LABELS "core" TIMEOUT 30)

# JSON admin exporter (§23.3): GET /api/observability/metrics.json.
# Builds in both the default (no OTLP) and OTLP-enabled configurations;
# OTLP-specific assertions are gated by `KEEL_HAS_OTLP` in the C source.
add_executable(test_admin_metrics_json test_admin_metrics_json.c)
target_link_libraries(test_admin_metrics_json PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_admin
    $<$<BOOL:${KEEL_ENABLE_OTLP}>:keel_otlp>
    keelengine
    keel_engine_flow
    keel_worker_new
    keel_session
    keelcore
    keelstats
    keel_probe
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_tls
    keel_hook
    keellog_plugin
    keellog
    keel_arch_platform
    keel_arch_common
    keelmem
    keelutil
    keelsql
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
if(KEEL_USE_IOURING AND URING_FOUND)
    target_link_libraries(test_admin_metrics_json PRIVATE uring)
endif()
target_link_options(test_admin_metrics_json PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_admin_metrics_json COMMAND test_admin_metrics_json)
set_tests_properties(test_admin_metrics_json PROPERTIES LABELS "core" TIMEOUT 30)

# Mock proxy integration tests — exercises engine_flow, worker, backend_connect_async
# using an in-process mock PostgreSQL backend (no real database needed).
add_executable(test_mock_proxy test_mock_proxy.c)
target_link_libraries(test_mock_proxy PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keelengine
    keel_engine_flow
    keel_worker_new
    keel_session
    keelcore
    keelstats
    keel_probe
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_tls
    keel_hook
    keellog_plugin
    keellog
    keel_arch_platform
    keel_arch_common
    keelmem
    keelutil
    keelsql
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
if(KEEL_USE_IOURING AND URING_FOUND)
    target_link_libraries(test_mock_proxy PRIVATE uring)
endif()
target_link_options(test_mock_proxy PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_mock_proxy COMMAND test_mock_proxy)
set_tests_properties(test_mock_proxy PROPERTIES LABELS "core" TIMEOUT 120)

# Graceful worker restart tests
add_executable(test_graceful_restart test_graceful_restart.c)
target_link_libraries(test_graceful_restart PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keelengine
    keel_engine_flow
    keel_worker_new
    keel_session
    keelcore
    keelstats
    keel_probe
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_tls
    keel_hook
    keellog_plugin
    keellog
    keel_arch_platform
    keel_arch_common
    keelmem
    keelutil
    keelsql
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
if(KEEL_USE_IOURING AND URING_FOUND)
    target_link_libraries(test_graceful_restart PRIVATE uring)
endif()
target_link_options(test_graceful_restart PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_graceful_restart COMMAND test_graceful_restart)

# TLS auto-generate tests
add_executable(test_tls_auto test_tls_auto.c)
target_link_libraries(test_tls_auto PRIVATE
    test_utils
    keel_tls
    OpenSSL::Crypto
    OpenSSL::SSL
    Threads::Threads
)
target_compile_definitions(test_tls_auto PRIVATE KEEL_HAS_OPENSSL=1)
target_link_options(test_tls_auto PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_tls_auto COMMAND test_tls_auto)

# Logging tests
add_executable(test_log test_log.c)
target_link_libraries(test_log PRIVATE 
    test_utils 
    keelutil 
    Threads::Threads
)
target_link_options(test_log PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_log COMMAND test_log)

# Query logging / log plugin tests
add_executable(test_query_log test_query_log.c)
target_link_libraries(test_query_log PRIVATE
    test_utils
    keellog_plugin
    keelutil
    Threads::Threads
)
target_link_options(test_query_log PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_query_log COMMAND test_query_log)

# Stats alignment regression tests
add_executable(test_stats_alignment test_stats_alignment.c)
target_link_libraries(test_stats_alignment PRIVATE
    test_utils
    keelstats
    keelmem
    Threads::Threads
)
target_link_options(test_stats_alignment PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_stats_alignment COMMAND test_stats_alignment)

# Histogram bucket correctness tests (log2 histogram, percentiles, snapshots)
add_executable(test_histogram test_histogram.c)
target_link_libraries(test_histogram PRIVATE
    test_utils
    keelstats
    keelmem
    Threads::Threads
)
target_link_options(test_histogram PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_histogram COMMAND test_histogram)

# OTLP JSON and protobuf encoding tests
add_executable(test_otlp_encoding test_otlp_encoding.c)
target_link_libraries(test_otlp_encoding PRIVATE
    test_utils
    keeltrace
    keelmem
    keellog
    Threads::Threads
)
target_link_options(test_otlp_encoding PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_otlp_encoding COMMAND test_otlp_encoding)

# OTLP metrics encoder round-trip (chunk 7b — only when KEEL_ENABLE_OTLP=ON).
if(KEEL_ENABLE_OTLP)
    add_executable(test_otlp_metrics_encode test_otlp_metrics_encode.c)
    target_link_libraries(test_otlp_metrics_encode PRIVATE
        test_utils
        keel_otlp
        keelmem
        Threads::Threads
    )
    target_link_options(test_otlp_metrics_encode PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_metrics_encode COMMAND test_otlp_metrics_encode)

    add_executable(test_otlp_http test_otlp_http.c)
    target_link_libraries(test_otlp_http PRIVATE
        test_utils
        keel_otlp
        keelmem
        Threads::Threads
    )
    target_link_options(test_otlp_http PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_http COMMAND test_otlp_http)

    add_executable(test_otlp_snapshot_queue test_otlp_snapshot_queue.c)
    target_link_libraries(test_otlp_snapshot_queue PRIVATE
        test_utils
        keel_otlp
        keelmem
        Threads::Threads
    )
    target_link_options(test_otlp_snapshot_queue PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_snapshot_queue COMMAND test_otlp_snapshot_queue)

    add_executable(test_otlp_exporter test_otlp_exporter.c)
    target_link_libraries(test_otlp_exporter PRIVATE
        test_utils
        keel_otlp
        keelmem
        Threads::Threads
    )
    target_link_options(test_otlp_exporter PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_exporter COMMAND test_otlp_exporter)

    add_executable(test_otlp_exporter_json test_otlp_exporter_json.c)
    target_link_libraries(test_otlp_exporter_json PRIVATE
        test_utils
        keel_otlp
        keelmem
        Threads::Threads
    )
    target_link_options(test_otlp_exporter_json PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_exporter_json COMMAND test_otlp_exporter_json)

    add_executable(test_otlp_config test_otlp_config.c)
    target_link_libraries(test_otlp_config PRIVATE
        test_utils
        keel_otlp
        keelcore
        keelmem
        keelutil
        keellog
        Threads::Threads
    )
    target_link_options(test_otlp_config PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_config COMMAND test_otlp_config)

    # Admin HTTP route GET /api/observability/exporter.json (chunk 7d-4a).
    add_executable(test_admin_otlp_exporter test_admin_otlp_exporter.c)
    target_link_libraries(test_admin_otlp_exporter PRIVATE
        test_utils
        ${LINUX_START_GROUP}
        keel_admin
        keel_otlp
        keelengine
        keel_engine_flow
        keel_worker_new
        keel_session
        keelcore
        keelstats
        keel_probe
        keel_proto_postgres
        keel_proto_mysql
        keel_proto_flow_registry
        keel_tls
        keel_hook
        keellog_plugin
        keellog
        keel_arch_platform
        keel_arch_common
        keelmem
        keelutil
        keelsql
        OpenSSL::SSL
        OpenSSL::Crypto
        ${LINUX_END_GROUP}
        Threads::Threads
    )
    if(KEEL_USE_IOURING AND URING_FOUND)
        target_link_libraries(test_admin_otlp_exporter PRIVATE uring)
    endif()
    target_link_options(test_admin_otlp_exporter PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_admin_otlp_exporter COMMAND test_admin_otlp_exporter)
    set_tests_properties(test_admin_otlp_exporter PROPERTIES TIMEOUT 30)

    # OTLP snapshot → Prometheus text formatter unit tests (chunk A, §23.2).
    add_executable(test_otlp_prom_format test_otlp_prom_format.c)
    target_link_libraries(test_otlp_prom_format PRIVATE
        test_utils
        keel_otlp
        keelstats
        keelmem
        keellog
        Threads::Threads
    )
    target_link_options(test_otlp_prom_format PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_prom_format COMMAND test_otlp_prom_format)

    # Admin HTTP route GET /api/observability/metrics.prom (chunk A, §23.2).
    add_executable(test_admin_metrics_prom test_admin_metrics_prom.c)
    target_link_libraries(test_admin_metrics_prom PRIVATE
        test_utils
        ${LINUX_START_GROUP}
        keel_admin
        keel_otlp
        keelengine
        keel_engine_flow
        keel_worker_new
        keel_session
        keelcore
        keelstats
        keel_probe
        keel_proto_postgres
        keel_proto_mysql
        keel_proto_flow_registry
        keel_tls
        keel_hook
        keellog_plugin
        keellog
        keel_arch_platform
        keel_arch_common
        keelmem
        keelutil
        keelsql
        OpenSSL::SSL
        OpenSSL::Crypto
        ${LINUX_END_GROUP}
        Threads::Threads
    )
    if(KEEL_USE_IOURING AND URING_FOUND)
        target_link_libraries(test_admin_metrics_prom PRIVATE uring)
    endif()
    target_link_options(test_admin_metrics_prom PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_admin_metrics_prom COMMAND test_admin_metrics_prom)
    set_tests_properties(test_admin_metrics_prom PROPERTIES TIMEOUT 30)

    # Negative + fault-injection tests (chunk E, §29.6).
    add_executable(test_otlp_fault_injection test_otlp_fault_injection.c)
    target_link_libraries(test_otlp_fault_injection PRIVATE
        test_utils
        ${LINUX_START_GROUP}
        keel_admin
        keel_otlp
        keelengine
        keel_engine_flow
        keel_worker_new
        keel_session
        keelcore
        keelstats
        keel_probe
        keel_proto_postgres
        keel_proto_mysql
        keel_proto_flow_registry
        keel_tls
        keel_hook
        keellog_plugin
        keellog
        keel_arch_platform
        keel_arch_common
        keelmem
        keelutil
        keelsql
        OpenSSL::SSL
        OpenSSL::Crypto
        ${LINUX_END_GROUP}
        Threads::Threads
    )
    if(KEEL_USE_IOURING AND URING_FOUND)
        target_link_libraries(test_otlp_fault_injection PRIVATE uring)
    endif()
    target_link_options(test_otlp_fault_injection PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_fault_injection COMMAND test_otlp_fault_injection)
    set_tests_properties(test_otlp_fault_injection PROPERTIES TIMEOUT 30)

    # End-to-end OTLP/HTTP roundtrip with in-process protobuf-decoding
    # mock receiver (chunk D, §29.5).
    add_executable(test_otlp_e2e test_otlp_e2e.c)
    target_link_libraries(test_otlp_e2e PRIVATE
        test_utils
        ${LINUX_START_GROUP}
        keel_admin
        keel_otlp
        keelengine
        keel_engine_flow
        keel_worker_new
        keel_session
        keelcore
        keelstats
        keel_probe
        keel_proto_postgres
        keel_proto_mysql
        keel_proto_flow_registry
        keel_tls
        keel_hook
        keellog_plugin
        keellog
        keel_arch_platform
        keel_arch_common
        keelmem
        keelutil
        keelsql
        OpenSSL::SSL
        OpenSSL::Crypto
        ${LINUX_END_GROUP}
        Threads::Threads
    )
    if(KEEL_USE_IOURING AND URING_FOUND)
        target_link_libraries(test_otlp_e2e PRIVATE uring)
    endif()
    target_link_options(test_otlp_e2e PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_e2e COMMAND test_otlp_e2e)
    set_tests_properties(test_otlp_e2e PROPERTIES TIMEOUT 30)

    # Observability hot-path microbenchmark (chunk F, §29.7). In-process,
    # no external dependencies, runs in every CI job. Validates per-call
    # cost of snapshot projection + encode + exporter_submit and that the
    # aggregator keeps up with its tick interval.
    add_executable(test_otlp_overhead_bench test_otlp_overhead_bench.c)
    target_link_libraries(test_otlp_overhead_bench PRIVATE
        test_utils ${LINUX_START_GROUP}
        keel_otlp keelstats keelcore keelmem keelutil
        OpenSSL::SSL OpenSSL::Crypto ${LINUX_END_GROUP} Threads::Threads
    )
    if(KEEL_USE_IOURING AND URING_FOUND)
        target_link_libraries(test_otlp_overhead_bench PRIVATE uring)
    endif()
    target_link_options(test_otlp_overhead_bench PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_overhead_bench COMMAND test_otlp_overhead_bench)
    set_tests_properties(test_otlp_overhead_bench PROPERTIES TIMEOUT 60)

    # Stats → OTLP-snapshot conversion unit tests (chunk 7d-4c).
    add_executable(test_otlp_snapshot_build test_otlp_snapshot_build.c)
    target_link_libraries(test_otlp_snapshot_build PRIVATE
        test_utils
        keel_otlp
        keelstats
        keelmem
        keellog
        Threads::Threads
    )
    target_link_options(test_otlp_snapshot_build PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_snapshot_build COMMAND test_otlp_snapshot_build)

    # End-to-end aggregator + exporter + mock HTTP collector (chunk 7d-4c).
    add_executable(test_otlp_aggregator test_otlp_aggregator.c)
    target_link_libraries(test_otlp_aggregator PRIVATE
        test_utils
        keel_otlp
        keelstats
        keelmem
        keellog
        Threads::Threads
    )
    target_link_options(test_otlp_aggregator PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_otlp_aggregator COMMAND test_otlp_aggregator)
    set_tests_properties(test_otlp_aggregator PROPERTIES TIMEOUT 30)
endif()

# NDJSON log record format tests
add_executable(test_ndjson_log test_ndjson_log.c)
target_link_libraries(test_ndjson_log PRIVATE
    test_utils
    keellog_plugin
    keelutil
    Threads::Threads
)
target_link_options(test_ndjson_log PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ndjson_log COMMAND test_ndjson_log)

# String View tests (zero-copy string handling)
add_executable(test_string_view test_string_view.c)
target_link_libraries(test_string_view PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keelutil 
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_string_view PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_string_view COMMAND test_string_view)

# Ring Buffer tests (lock-free SPSC/MPSC buffers)
add_executable(test_ringbuf test_ringbuf.c)
target_link_libraries(test_ringbuf PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keelutil 
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_ringbuf PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ringbuf COMMAND test_ringbuf)

# XXHash tests (fast non-cryptographic hashing)
add_executable(test_xxhash test_xxhash.c)
target_link_libraries(test_xxhash PRIVATE 
    test_utils 
    keelutil 
    Threads::Threads
)
target_link_options(test_xxhash PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_xxhash COMMAND test_xxhash)

# Residual Buffer tests
add_executable(test_residual test_residual.c)
target_link_libraries(test_residual PRIVATE 
    test_utils 
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_residual PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_residual COMMAND test_residual)

# Session & Engine tests
add_executable(test_session_engine test_session_engine.c)
target_link_libraries(test_session_engine PRIVATE 
    test_utils 
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_session_engine PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_session_engine COMMAND test_session_engine)

# SSV core tests (protocol-agnostic helpers)
add_executable(test_ssv_core test_ssv_core.c)
target_link_libraries(test_ssv_core PRIVATE
    test_utils
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_ssv_core PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ssv_core COMMAND test_ssv_core)

# SSV atom/consistency tests
add_executable(test_ssv_atom test_ssv_atom.c)
target_link_libraries(test_ssv_atom PRIVATE
    test_utils
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_ssv_atom PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ssv_atom COMMAND test_ssv_atom)

# Hook/Trigger system tests
add_executable(test_hooks test_hooks.c)
target_link_libraries(test_hooks PRIVATE
    test_utils
    keel_hook
    keelmem
    keellog
    Threads::Threads
)
target_link_options(test_hooks PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_hooks COMMAND test_hooks)

# Route Cache tests (L1 query→route cache, collision, eviction, LRU)
add_executable(test_route_cache test_route_cache.c)
target_link_libraries(test_route_cache PRIVATE
    test_utils
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_route_cache PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_route_cache COMMAND test_route_cache)

# Route Cache stress tests (adversarial collisions, probe overflow, hit-rate)
add_executable(test_route_cache_stress test_route_cache_stress.c)
target_link_libraries(test_route_cache_stress PRIVATE
    test_utils
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_route_cache_stress PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_route_cache_stress COMMAND test_route_cache_stress)

# Admission Control tests (frontend/backend limits, wait queue, pressure)
add_executable(test_admission test_admission.c)
target_link_libraries(test_admission PRIVATE
    test_utils
    keel_session
    keelmem
    Threads::Threads
)
target_link_options(test_admission PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_admission COMMAND test_admission)

# Backpressure/hardening tests (Issue 10: memory, FD, pool wait timeout limits)
add_executable(test_backpressure test_backpressure.c)
target_include_directories(test_backpressure PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_backpressure PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelcore
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_backpressure PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_backpressure COMMAND test_backpressure)

# Pool Correctness tests (CLEANING state, admission control, quarantine)
add_executable(test_pool_correctness test_pool_correctness.c)
target_include_directories(test_pool_correctness PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_pool_correctness PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keel_proto_flow_registry
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_pool_correctness PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_pool_correctness COMMAND test_pool_correctness)

# Plugin Contract tests (Phase 5 — capabilities, error classification, cleanup)
add_executable(test_plugin_contract test_plugin_contract.c)
target_link_libraries(test_plugin_contract PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_plugin_contract PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_plugin_contract COMMAND test_plugin_contract)

# PG Protocol Flow tests (comprehensive PostgreSQL wire protocol coverage)
add_executable(test_pg_protocol_flow test_pg_protocol_flow.c)
target_link_libraries(test_pg_protocol_flow PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_pg_protocol_flow PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_pg_protocol_flow COMMAND test_pg_protocol_flow)

# Split-at-every-byte protocol framing generator tests
add_executable(test_proto_split test_proto_split.c)
target_link_libraries(test_proto_split PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_proto_split PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_proto_split COMMAND test_proto_split)

# Prepared-statement × failover × TLS hash-preservation tests
add_executable(test_ps_failover_tls test_ps_failover_tls.c)
target_link_libraries(test_ps_failover_tls PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_ps_failover_tls PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ps_failover_tls COMMAND test_ps_failover_tls)

# MySQL Protocol Flow tests (comprehensive MySQL wire protocol coverage)
add_executable(test_mysql_protocol_flow test_mysql_protocol_flow.c)
target_link_libraries(test_mysql_protocol_flow PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_mysql_protocol_flow PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_mysql_protocol_flow COMMAND test_mysql_protocol_flow)

# Cancel Forwarding tests (PG/MySQL cancel key encoding, dispatch, credential swap)
add_executable(test_cancel_forwarding test_cancel_forwarding.c)
target_link_libraries(test_cancel_forwarding PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_cancel_forwarding PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cancel_forwarding COMMAND test_cancel_forwarding)

# Failover tests (pool target update, drain, waiter timeout, health-aware routing)
add_executable(test_failover test_failover.c)
target_link_libraries(test_failover PRIVATE 
    test_utils 
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keelcore
    keel_probe
    keellog
    keellog_plugin
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_failover PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_failover COMMAND test_failover)

# Issue 8 — Failover gate & commit-in-doubt tests
add_executable(test_failover_gates test_failover_gates.c)
target_link_libraries(test_failover_gates PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keelcore
    keel_probe
    keellog
    keellog_plugin
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_failover_gates PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_failover_gates COMMAND test_failover_gates)

# Connection Migration tests (eligibility, FD transfer, ring serialisation)
add_executable(test_migration test_migration.c)
target_include_directories(test_migration PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_migration PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keelcore
    keelengine
    keel_probe
    keellog
    keellog_plugin
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_migration PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_migration COMMAND test_migration)
set_tests_properties(test_migration PROPERTIES LABELS "session")

# Cluster Mode tests (multi-proxy HA peer discovery, heartbeat, gossip)
add_executable(test_cluster test_cluster.c)
target_link_libraries(test_cluster PRIVATE
    test_utils
    keelcore
    keelmem
    OpenSSL::Crypto
    Threads::Threads
)
target_link_options(test_cluster PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cluster COMMAND test_cluster)
set_tests_properties(test_cluster PROPERTIES LABELS "core" TIMEOUT 60)

# ──────────────────────────────────────────────────────────────────────────────
# Cluster Phase 1-4 Unit Tests
#
# Exercises leader election (Phase 1), role-aware API (Phase 2), VIP config
# (Phase 3), and quorum-gated 2PC config commits (Phase 4).
# Includes unit, integration, regression, memory, performance, and e2e tests.
# ──────────────────────────────────────────────────────────────────────────────
add_executable(test_cluster_election test_cluster_election.c)
target_link_libraries(test_cluster_election PRIVATE
    test_utils
    keelcore
    keelmem
    OpenSSL::Crypto
    Threads::Threads
)
target_link_options(test_cluster_election PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cluster_election COMMAND test_cluster_election)
set_tests_properties(test_cluster_election PROPERTIES
    LABELS "core;cluster;election"
    TIMEOUT 120)

# ──────────────────────────────────────────────────────────────────────────────
# Cluster Wire-Protocol Fuzz Tests
#
# Adversarial / fuzz corpus for the cluster TCP wire protocol:
# truncated messages, bit-flips, overflow values, unknown types, concurrent
# senders, and pseudo-random corpus sweeps.
#
# Also exposes LLVMFuzzerTestOneInput for AFL++ / libFuzzer campaigns.
# ──────────────────────────────────────────────────────────────────────────────
add_executable(test_cluster_fuzz test_cluster_fuzz.c)
target_link_libraries(test_cluster_fuzz PRIVATE
    test_utils
    keelcore
    keelmem
    OpenSSL::Crypto
    Threads::Threads
)
target_link_options(test_cluster_fuzz PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_cluster_fuzz COMMAND test_cluster_fuzz)
set_tests_properties(test_cluster_fuzz PROPERTIES
    LABELS "core;cluster;fuzz"
    TIMEOUT 300)

# ============================================================================
# Production-Hardening Tests (Spec §1-§5 Phase A/B/C)
# ============================================================================
# These tests exercise correctness properties that only manifest under stress,
# concurrent access, or adversarial input.  They link the full proxy library
# graph and frequently use multiple threads to surface race conditions.
#
# Categories:
#   - Memory auditing (canary integrity, SIGUSR1 live dump)
#   - FD leak detection
#   - State machine exhaustive walks and fuzz harnesses
#   - Dirty connection isolation
#   - Concurrency stress (connection storms, thundering herd)
#   - Protocol fuzz harness (AFL++ / libfuzzer compatible)
# ============================================================================

# Pool Audit tests — canary integrity, SIGUSR1 live dump, active block tracking
add_executable(test_pool_audit test_pool_audit.c)
target_link_libraries(test_pool_audit PRIVATE
    test_utils
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_pool_audit PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_pool_audit COMMAND test_pool_audit)

# File Descriptor Tracking tests — no FD leaks under socket open/close cycles
add_executable(test_fd_tracking test_fd_tracking.c)
target_link_libraries(test_fd_tracking PRIVATE
    test_utils
    Threads::Threads
)
target_link_options(test_fd_tracking PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_fd_tracking COMMAND test_fd_tracking)

# State Machine tests (Phase B) — IDLE→IN_TX→TX_ERROR→ROLLBACK→IDLE transitions
# and sticky-bit / backend-FD consistency at every step
add_executable(test_state_machine test_state_machine.c)
target_link_libraries(test_state_machine PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_state_machine PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_state_machine COMMAND test_state_machine)

# State Contract tests — contract-driven state model: derived enums, transition
# matrices, contract sync, invariant checks, event journal, derived predicates
add_executable(test_state_contracts test_state_contracts.c)
target_link_libraries(test_state_contracts PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_state_contracts PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_state_contracts COMMAND test_state_contracts)

# State Machine Sequence Walk — exhaustive DFS of all transition matrices
# (phase, replay, CID), every legal edge, every illegal edge, combined lifecycle
add_executable(test_sm_sequence_walk test_sm_sequence_walk.c)
target_link_libraries(test_sm_sequence_walk PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_sm_sequence_walk PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_sm_sequence_walk COMMAND test_sm_sequence_walk)

# State Machine Fuzz Harness — AFL++/libfuzzer entry point + deterministic battery
# Interprets random byte-streams as state machine command sequences
add_executable(test_sm_fuzz test_sm_fuzz.c)
target_link_libraries(test_sm_fuzz PRIVATE
    ${LINUX_START_GROUP}
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_sm_fuzz PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_sm_fuzz COMMAND test_sm_fuzz)

# State Machine Concurrent Stress — multi-threaded lifecycle, journal, contract
# derivation, and thundering-herd phase transition tests
add_executable(test_sm_stress test_sm_stress.c)
target_link_libraries(test_sm_stress PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_sm_stress PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_sm_stress COMMAND test_sm_stress)

# Dirty Connection Audit (Spec §3) — connection cleaning before pool reuse,
# cross-session isolation, state-hash mismatch detection
add_executable(test_dirty_connection test_dirty_connection.c)
target_link_libraries(test_dirty_connection PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_dirty_connection PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_dirty_connection COMMAND test_dirty_connection)

# Concurrency Stress tests (Spec §4) — connection storm, thundering herd,
# RLIMIT_MEMLOCK check, port recycling, pool counter coherence under contention
add_executable(test_concurrency_stress test_concurrency_stress.c)
target_link_libraries(test_concurrency_stress PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_concurrency_stress PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_concurrency_stress COMMAND test_concurrency_stress)

# Fuzz Harness (Phase A — AFL++ entry point + deterministic unit battery)
# When AFL++ is used: afl-clang-fast -DAFL_FUZZ ... and wrap in afl-fuzz.
# As a unit test: feeds known-bad inputs under ASAN/UBSAN to catch regressions.
add_executable(test_fuzz_harness test_fuzz_harness.c)
target_link_libraries(test_fuzz_harness PRIVATE
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_fuzz_harness PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_fuzz_harness COMMAND test_fuzz_harness)

# Shadow-diff harness regression test — ensures pgbench meta-script inputs
# are rejected with a clear fast-fail message.
add_test(NAME test_shadow_diff_harness
         COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test_shadow_diff_harness.sh ${CMAKE_SOURCE_DIR})

# Runtime security harness regression test — verifies privilege drop and
# seccomp baseline mode enforcement via /proc process status.
add_test(NAME test_runtime_security_harness
     COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test_runtime_security_harness.sh
         ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR} baseline)

add_test(NAME test_runtime_security_strict_harness
     COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test_runtime_security_harness.sh
         ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR} strict)

# ============================================================================
# Track 3 — Correctness Hardening (Combinatorial Matrix Tests)
# ============================================================================
# These tests enumerate the Cartesian product of feature combinations
# (e.g. prepared statements × pool modes × failover × cleanup) and verify
# that every combination produces correct behaviour.  They catch subtle
# interactions that per-feature unit tests miss.
#
# Each matrix test is self-contained and deterministic — no Docker or
# external services required.
# ============================================================================

# PS × pool mode × failover × cleanup combinatorial matrix
add_executable(test_ps_pool_matrix test_ps_pool_matrix.c)
target_include_directories(test_ps_pool_matrix PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_ps_pool_matrix PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_ps_pool_matrix PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ps_pool_matrix COMMAND test_ps_pool_matrix)

# Phase 4 acceptance gate: prepared-statement semantic compatibility
# Exercises VIRTUALIZE mode (extended-protocol Parse path) and the
# backend_pool_stmt_compatible() predicate for cross-backend reuse.
add_executable(test_ps_semantic_compat test_ps_semantic_compat.c)
target_include_directories(test_ps_semantic_compat PRIVATE
    ${CMAKE_SOURCE_DIR}/src/worker
)
target_link_libraries(test_ps_semantic_compat PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_engine_flow
    keel_session
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_ps_semantic_compat PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_ps_semantic_compat COMMAND test_ps_semantic_compat)

# TLS/splice × data frames × large result sets × partial writes matrix
add_executable(test_tls_splice_matrix test_tls_splice_matrix.c)
target_link_libraries(test_tls_splice_matrix PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_tls_splice_matrix PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_tls_splice_matrix COMMAND test_tls_splice_matrix)

# Session context × hooks × route changes × migration matrix
add_executable(test_session_hooks_matrix test_session_hooks_matrix.c)
target_link_libraries(test_session_hooks_matrix PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_hook
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keellog
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_session_hooks_matrix PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_session_hooks_matrix COMMAND test_session_hooks_matrix)

# Crash/recovery × commit-in-doubt × XID × cleanup matrix
add_executable(test_crash_recovery_matrix test_crash_recovery_matrix.c)
target_link_libraries(test_crash_recovery_matrix PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_crash_recovery_matrix PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_crash_recovery_matrix COMMAND test_crash_recovery_matrix)

# Async deferred-BEGIN pre-query replay (PR #4) regression tests
add_executable(test_pre_query_replay test_pre_query_replay.c)
target_link_libraries(test_pre_query_replay PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_worker_new
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_pre_query_replay PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_pre_query_replay COMMAND test_pre_query_replay)

# Dual-protocol PG/MySQL separation & equivalence matrix
add_executable(test_dual_protocol_matrix test_dual_protocol_matrix.c)
target_link_libraries(test_dual_protocol_matrix PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_engine_flow
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_dual_protocol_matrix PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_dual_protocol_matrix COMMAND test_dual_protocol_matrix)

# Formal Invariant Model — compatibility matrix, pin conflicts, cross-feature checks
add_executable(test_invariant_model test_invariant_model.c)
target_link_libraries(test_invariant_model PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_engine_flow
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keel_hook
    keellog_plugin
    keellog
    keel_tls
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_invariant_model PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_invariant_model COMMAND test_invariant_model)

# Runtime Mode Tier tests (tier parsing, gate macros, session flow init overrides)
add_executable(test_runtime_mode test_runtime_mode.c)
target_link_libraries(test_runtime_mode PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_engine_flow
    keel_worker_new
    keel_proto_postgres
    keel_proto_mysql
    keel_proto_flow_registry
    keel_session
    keelmem
    keelutil
    keelsql
    keelstats
    keel_arch_common
    keel_arch_platform
    keel_hook
    keellog_plugin
    keellog
    keel_tls
    OpenSSL::SSL
    OpenSSL::Crypto
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_runtime_mode PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_runtime_mode COMMAND test_runtime_mode)

# Session-context preservation tests (state profile, diff SQL, round-trip)
add_executable(test_state_context test_state_context.c)
target_link_libraries(test_state_context PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_session
    keelmem
    keelutil
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_state_context PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_state_context COMMAND test_state_context)

# ============================================================================
# Integration / E2E Tests (require Docker, not in ctest by default)
# ============================================================================
# These tests spin up real PostgreSQL / MySQL instances via Docker Compose,
# connect through a live Keel proxy, and verify end-to-end behaviour
# including authentication, TLS, prepared statements, and session state.
#
# The test_integration static library provides shared Docker lifecycle
# helpers (compose up/down, wait-for-ready, connection wrappers).
#
# Run manually:  ./test_e2e  or  ./test_proxy_e2e  (Docker must be running)
# ============================================================================

add_library(test_integration STATIC
    test_integration.c
)
target_include_directories(test_integration
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${CMAKE_BINARY_DIR}/include
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
target_compile_definitions(test_integration PRIVATE
    INTEG_DOCKER_DIR="${CMAKE_SOURCE_DIR}"
)
target_link_libraries(test_integration PRIVATE keelutil OpenSSL::Crypto)

# End-to-end integration tests (requires Docker)
add_executable(test_e2e test_e2e.c)
target_link_libraries(test_e2e PRIVATE 
    test_utils 
    test_integration
    Threads::Threads
)
target_link_options(test_e2e PRIVATE ${TEST_COVERAGE_LINK_FLAGS})

# Proxy E2E integration tests (requires Docker)
add_executable(test_proxy_e2e test_proxy_e2e.c)
target_link_libraries(test_proxy_e2e PRIVATE 
    test_utils 
    test_integration
    Threads::Threads
)
target_link_options(test_proxy_e2e PRIVATE ${TEST_COVERAGE_LINK_FLAGS})

# Dedicated proxy E2E integration tests for PostgreSQL semantic stmt replay.
add_executable(test_proxy_ssv_e2e test_proxy_ssv_e2e.c)
target_link_libraries(test_proxy_ssv_e2e PRIVATE
    test_utils
    test_integration
    Threads::Threads
)
target_link_options(test_proxy_ssv_e2e PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_proxy_ssv_e2e COMMAND test_proxy_ssv_e2e)

# Copy test config file to build directory
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/keel_test.ini 
               ${CMAKE_CURRENT_BINARY_DIR}/keel_test.ini COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/keel_ssv_test.ini
               ${CMAKE_CURRENT_BINARY_DIR}/keel_ssv_test.ini COPYONLY)

# ============================================================================
# Test Properties
# ============================================================================
# Default timeout: 60 seconds for unit/hardening tests, 180 seconds for
# integration tests.  All tests run from the build directory so they can
# find test config files copied by configure_file().
# ============================================================================

set_tests_properties(
    test_mem
    test_auth
    test_trace
    test_parser
    test_router
    test_router_plugin
    test_util
    test_sql
    test_config
    test_result_cache_gate
    test_log
    test_query_log
    test_stats_alignment
    test_histogram
    test_otlp_encoding
    test_ndjson_log
    test_string_view
    test_ringbuf
    test_xxhash
    test_residual
    test_session_engine
    test_hooks
    test_route_cache
    test_admission
    test_pool_correctness
    test_plugin_contract
    test_pg_protocol_flow
    test_mysql_protocol_flow
    test_failover
    test_pool_audit
    test_fd_tracking
    test_state_machine
    test_state_contracts
    test_sm_sequence_walk
    test_sm_fuzz
    test_sm_stress
    test_dirty_connection
    test_concurrency_stress
    test_fuzz_harness
    test_shadow_diff_harness
    test_runtime_security_harness
    test_ps_pool_matrix
    test_ps_semantic_compat
    test_tls_splice_matrix
    test_session_hooks_matrix
    test_crash_recovery_matrix
    test_pre_query_replay
    test_dual_protocol_matrix
    test_drain_shutdown
    test_graceful_restart
    test_tls_auto
    PROPERTIES
    TIMEOUT 60
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

set_tests_properties(test_runtime_security_strict_harness
    PROPERTIES
    TIMEOUT 60
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

set_tests_properties(test_proxy_ssv_e2e
    PROPERTIES
    TIMEOUT 180
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ============================================================================
# Per-Module Test Labels
# ============================================================================
# Label every test with its module so  ctest -L <module>  runs only that slice.
# Each label also has a matching custom build+run target; see below.
#
# Labels serve three purposes:
#   1. Developer workflow: run only the tests for the module being changed.
#   2. CI matrix: split the test suite across parallel CI jobs by label.
#   3. Sanitizer exclusion: ctest -LE openssl skips OpenSSL-dependent tests
#      under MSan (system OpenSSL is not MSan-instrumented).
# ============================================================================

set_tests_properties(test_mem test_util test_string_view test_ringbuf
                     test_xxhash test_log test_query_log test_stats_alignment
                     test_histogram test_ndjson_log
    PROPERTIES LABELS "util")

set_tests_properties(test_trace test_otlp_encoding
    PROPERTIES LABELS "trace")

set_tests_properties(test_parser test_sql test_admin_sql
    PROPERTIES LABELS "sql")

set_tests_properties(test_auth test_config test_result_cache_gate test_router test_router_plugin
    test_cloud_auth test_cloud_auth_internals test_cloud_auth_e2e test_cluster
    test_cluster_election test_cluster_fuzz
    PROPERTIES LABELS "core")

set_tests_properties(test_residual test_session_engine test_hooks
                     test_route_cache test_admission
    PROPERTIES LABELS "session")

set_tests_properties(test_pool_correctness test_conn_lifecycle
                     test_backpressure
    PROPERTIES LABELS "pool")

set_tests_properties(test_plugin_contract test_pg_protocol_flow
                     test_mysql_protocol_flow
    PROPERTIES LABELS "protocol")

set_tests_properties(test_pg_protocol_flow
    PROPERTIES LABELS "protocol openssl")

set_tests_properties(test_failover
    PROPERTIES LABELS "engine")

set_tests_properties(test_proxy_ssv_e2e
    PROPERTIES LABELS "integration")

# Production-hardening tests (Spec §1–§5)
set_tests_properties(test_pool_audit test_fd_tracking
    PROPERTIES LABELS "hardening")

set_tests_properties(test_state_machine test_state_contracts test_dirty_connection
                     test_sm_sequence_walk test_sm_fuzz test_sm_stress
    PROPERTIES LABELS "hardening")

set_tests_properties(test_concurrency_stress test_fuzz_harness
    PROPERTIES LABELS "hardening")

set_tests_properties(test_stats_alignment
    PROPERTIES LABELS "hardening")

set_tests_properties(test_shadow_diff_harness
    PROPERTIES LABELS "hardening")

set_tests_properties(test_runtime_security_harness
    PROPERTIES LABELS "hardening")

set_tests_properties(test_tls_security test_tls_ktls test_drain_shutdown
                     test_graceful_restart test_tls_auto
    PROPERTIES LABELS "hardening")

set_tests_properties(test_ps_pool_matrix test_ps_semantic_compat
                     test_tls_splice_matrix
                     test_session_hooks_matrix test_crash_recovery_matrix
                     test_pre_query_replay
                     test_dual_protocol_matrix test_invariant_model
    PROPERTIES LABELS "hardening")

set_tests_properties(test_runtime_security_strict_harness
    PROPERTIES LABELS "seccomp_strict")

# Tests that link/exercise OpenSSL — must be excluded from MSAN runs
# because the system OpenSSL shared library is not MSAN-instrumented.
# Usage:  ctest -LE openssl   (to skip these under MSAN)
set_property(TEST test_auth test_tls_security test_tls_ktls
             test_runtime_security_harness
             test_runtime_security_strict_harness
             test_tls_auto test_admin_auth
             test_cloud_auth test_cloud_auth_internals test_cloud_auth_e2e
             test_query_cache
    APPEND PROPERTY LABELS "openssl")

# ============================================================================
# New Gap-Coverage Tests (added to reach 100% module coverage)
# ============================================================================

# Probe subsystem (previously ZERO coverage)
add_executable(test_probe test_probe.c)
target_link_libraries(test_probe PRIVATE
    test_utils
    keel_probe
    keelmem
    keelutil
    keelcore
    OpenSSL::Crypto
    Threads::Threads
)
target_link_options(test_probe PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_probe COMMAND test_probe)
set_tests_properties(test_probe PROPERTIES LABELS "core openssl" TIMEOUT 30)

# Buffer API (growable byte buffer)
add_executable(test_buffer test_buffer.c)
target_link_libraries(test_buffer PRIVATE
    test_utils
    keelutil
    keelmem
    Threads::Threads
)
target_link_options(test_buffer PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_buffer COMMAND test_buffer)
set_tests_properties(test_buffer PROPERTIES LABELS "util" TIMEOUT 30)

# String helpers + FNV-1a/MurmurHash3 + hashmap + hash ring
add_executable(test_string_hash test_string_hash.c)
target_link_libraries(test_string_hash PRIVATE
    test_utils
    keelutil
    keelmem
    Threads::Threads
)
target_link_options(test_string_hash PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_string_hash COMMAND test_string_hash)
set_tests_properties(test_string_hash PROPERTIES LABELS "util" TIMEOUT 60)

# Encoding helpers (hex_encode, json_escape)
add_executable(test_encoding test_encoding.c)
target_link_libraries(test_encoding PRIVATE
    test_utils
    keelutil
    keelmem
    Threads::Threads
)
target_link_options(test_encoding PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_encoding COMMAND test_encoding)
set_tests_properties(test_encoding PROPERTIES LABELS "util" TIMEOUT 30)

# Durable operational replay log
add_executable(test_replay_log test_replay_log.c)
target_link_libraries(test_replay_log PRIVATE
    test_utils
    keelutil
    keelmem
    Threads::Threads
)
target_link_options(test_replay_log PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_replay_log COMMAND test_replay_log)
set_tests_properties(test_replay_log PROPERTIES LABELS "util;hardening" TIMEOUT 30)

# NUMA topology and NUMA-aware allocator
add_executable(test_numa test_numa.c)
target_link_libraries(test_numa PRIVATE
    test_utils
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_numa PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_numa COMMAND test_numa)
set_tests_properties(test_numa PROPERTIES LABELS "util" TIMEOUT 60)

# Memory safety layer (canaries, double-free, leak detection)
add_executable(test_mem_safety test_mem_safety.c)
target_link_libraries(test_mem_safety PRIVATE
    test_utils
    keelmem
    keelutil
    Threads::Threads
)
target_link_options(test_mem_safety PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_mem_safety COMMAND test_mem_safety)
set_tests_properties(test_mem_safety PROPERTIES LABELS "hardening" TIMEOUT 60)

# I/O reactor (epoll/io_uring backend)
add_executable(test_reactor test_reactor.c)
target_link_libraries(test_reactor PRIVATE
    test_utils
    ${LINUX_START_GROUP}
    keel_arch_platform
    keel_arch_common
    keelengine
    keelmem
    keelutil
    ${LINUX_END_GROUP}
    Threads::Threads
)
target_link_options(test_reactor PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_reactor COMMAND test_reactor)
set_tests_properties(test_reactor PROPERTIES LABELS "core" TIMEOUT 120)

# Log plugin system (stdout / file / syslog / dynamic loader)
add_executable(test_log_plugins test_log_plugins.c)
target_link_libraries(test_log_plugins PRIVATE
    test_utils
    keellog_plugin
    keellog
    keelutil
    keelmem
    Threads::Threads
)
target_link_options(test_log_plugins PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
add_test(NAME test_log_plugins COMMAND test_log_plugins)
set_tests_properties(test_log_plugins PROPERTIES LABELS "core" TIMEOUT 60)

# ============================================================================
# Per-Module Custom Build+Run Targets
# ============================================================================
# Convenience targets that build only the test binaries in a module, then
# run them with verbose output.  Faster than `make && ctest` when iterating
# on a single subsystem.
#
# Usage:
#   cmake --build <builddir> --target test-util
#   cmake --build <builddir> --target test-sql
#   cmake --build <builddir> --target test-core
#   cmake --build <builddir> --target test-session
#   cmake --build <builddir> --target test-pool
#   cmake --build <builddir> --target test-protocol
#   cmake --build <builddir> --target test-engine
#   cmake --build <builddir> --target test-all    # run all unit tests
# ============================================================================

add_custom_target(test-util
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^util$" --output-on-failure -j
        DEPENDS test_mem test_util test_string_view test_ringbuf
            test_xxhash test_log test_query_log test_stats_alignment
            test_histogram test_ndjson_log
            test_buffer test_string_hash test_encoding test_replay_log test_numa
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running util module tests"
    USES_TERMINAL
)

add_custom_target(test-sql
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^sql$" --output-on-failure -j
    DEPENDS test_parser test_sql test_admin_sql test_admin_sql_fuzz
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running sql module tests"
    USES_TERMINAL
)

add_custom_target(test-core
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^core$" --output-on-failure -j
    DEPENDS test_auth test_config test_result_cache_gate test_config_reload test_router test_router_plugin test_cluster
            test_cluster_election test_cluster_fuzz
            test_audit_log test_throttle test_compress
            test_probe test_reactor test_log_plugins
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running core module tests"
    USES_TERMINAL
)

add_custom_target(test-session
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^session$" --output-on-failure -j
    DEPENDS test_residual test_session_engine test_hooks test_route_cache test_admission
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running session module tests"
    USES_TERMINAL
)

add_custom_target(test-pool
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^pool$" --output-on-failure -j
    DEPENDS test_pool_correctness test_backpressure
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running pool module tests"
    USES_TERMINAL
)

add_custom_target(test-protocol
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^protocol$" --output-on-failure -j
    DEPENDS test_plugin_contract test_pg_protocol_flow test_mysql_protocol_flow
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running protocol module tests"
    USES_TERMINAL
)

add_custom_target(test-engine
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^engine$" --output-on-failure -j
    DEPENDS test_failover
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running engine module tests"
    USES_TERMINAL
)

add_custom_target(test-integration
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^integration$" --output-on-failure -j
    DEPENDS test_proxy_ssv_e2e
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running Docker-backed integration tests"
    USES_TERMINAL
)

add_custom_target(test-hardening
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
            -L "^hardening$" --output-on-failure -j
    DEPENDS test_pool_audit test_fd_tracking test_state_machine
            test_sm_sequence_walk test_sm_fuzz test_sm_stress
            test_dirty_connection test_concurrency_stress test_fuzz_harness
            test_shadow_diff_harness test_runtime_security_harness test_stats_alignment
            test_tls_security test_tls_ktls test_drain_shutdown
            test_graceful_restart test_tls_auto
            test_ps_pool_matrix test_ps_semantic_compat
            test_tls_splice_matrix test_session_hooks_matrix
            test_crash_recovery_matrix test_pre_query_replay
            test_dual_protocol_matrix test_invariant_model
            test_mem_safety test_replay_log
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running production-hardening tests (Spec §1-§5 Phase A/B)"
    USES_TERMINAL
)

add_custom_target(test-hardening-strict
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG>
        -L "^seccomp_strict$" --output-on-failure -j
    DEPENDS test_runtime_security_strict_harness
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running opt-in strict seccomp hardening tests"
    USES_TERMINAL
)

add_custom_target(test-all
    COMMAND ${CMAKE_CTEST_COMMAND} -C $<CONFIG> --output-on-failure -j
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running all unit tests"
    USES_TERMINAL
)

# =============================================================================
# Memory policy lint — forbid direct system allocator calls outside the mem/,
# arch/, and cli/ subsystems.  Runs as a ctest case so it shows in CI results.
# =============================================================================
add_test(
    NAME  check_forbidden_syscalls
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_forbidden_syscalls.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_forbidden_syscalls PROPERTIES
    LABELS "lint"
    TIMEOUT 30
)

# =============================================================================
# Reactor-blocking lint — forbid blocking syscalls in hot-path files.
# Pre-existing violations are listed in scripts/forbidden_blocking_baseline.txt
# and are expected to shrink to zero during the v0.2 reactor refactor.
# =============================================================================
add_test(
    NAME  check_forbidden_blocking
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_forbidden_blocking.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_forbidden_blocking PROPERTIES
    LABELS "lint"
    TIMEOUT 30
)

# =============================================================================
# Observability metrics invariants gate.
# Forbids ad-hoc mutation of state-machine fields whose transitions feed
# reason-coded metrics (backend close_reason, commit_in_doubt/commit_in_flight).
# See the observability proposal invariants.
# =============================================================================
add_test(
    NAME  check_metrics_invariants
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_metrics_invariants.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_metrics_invariants PROPERTIES
    LABELS "lint"
    TIMEOUT 30
)

# =============================================================================
# User-facing metrics reference parity gate.
# Ensures every metric registered in keel_prom_format.c's metadata table is
# documented in docs/METRICS_REFERENCE.md, and vice versa, so the operator
# reference cannot drift out of sync with the actual exported metric set.
# =============================================================================
add_test(
    NAME  check_metrics_reference
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_metrics_reference.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_metrics_reference PROPERTIES
    LABELS "lint"
    TIMEOUT 30
)

# =============================================================================
# Correctness messaging gate.
# Prevents public Markdown docs from reintroducing broad production claims that
# conflict with the conservative correctness-under-failure contract.
# =============================================================================
add_test(
    NAME  check_dangerous_marketing_claims
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_dangerous_marketing_claims.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_dangerous_marketing_claims PROPERTIES
    LABELS "lint"
    TIMEOUT 30
)

add_test(
    NAME  check_doc_release_mentions
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_doc_release_mentions.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_doc_release_mentions PROPERTIES
    LABELS "lint"
    TIMEOUT 30
)

add_test(
    NAME  check_chaos_manifest
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_chaos_manifest.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_chaos_manifest PROPERTIES
    LABELS "lint;hardening"
    TIMEOUT 30
)

add_test(
    NAME  check_correctness_gates
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_correctness_gates.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_correctness_gates PROPERTIES
    LABELS "lint;hardening"
    TIMEOUT 30
)

# =============================================================================
# Issue 9 — Result Cache gate: validate binary-level config rejection.
# Runs the keel binary with --check-config to verify that result_cache=on
# is rejected without experimental_features=true.
# =============================================================================
add_test(
    NAME  check_result_cache_gate
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_result_cache_gate.sh
            ${CMAKE_SOURCE_DIR}
            ${CMAKE_BINARY_DIR}
)
set_tests_properties(check_result_cache_gate PROPERTIES
    LABELS "gate"
    TIMEOUT 30
)

# =============================================================================
# Auth-log safety gate — no authentication material in log calls.
# Scans source for KEEL_LOG_* calls whose format strings embed sensitive
# labels (data=, proof=, nonce=, auth_msg=, …) with a %s specifier, or
# that pass raw SCRAM wire-message variables to %s inside AUTH-category
# log calls.  Must pass on every branch before merging to main.
# =============================================================================
add_test(
    NAME  check_auth_log_safety
    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/check_auth_log_safety.sh
            ${CMAKE_SOURCE_DIR}
)
set_tests_properties(check_auth_log_safety PROPERTIES
    LABELS "gate;security"
    TIMEOUT 30
)

# =============================================================================
# PostgreSQL Parse/Execute integration test
# Connects to a live PostgreSQL to verify extended-protocol Parse ('P' message)
# vs simple-query PREPARE handling, specifically the plan-invalidation behaviour
# when a temp table shadows a permanent table.
# Requires: libpq available, live PostgreSQL on 127.0.0.1:15432.
# =============================================================================
if(KEEL_HAS_LIBPQ)
    add_executable(test_pg_parse test_pg_parse.c)
    target_include_directories(test_pg_parse PRIVATE ${PQ_INCLUDE_DIR})
    target_link_libraries(test_pg_parse PRIVATE ${PQ_LIBRARY})
    target_link_options(test_pg_parse PRIVATE ${TEST_COVERAGE_LINK_FLAGS})
    add_test(NAME test_pg_parse COMMAND test_pg_parse)
    set_tests_properties(test_pg_parse PROPERTIES
        LABELS "e2e;integration"
        SKIP_RETURN_CODE 77
        TIMEOUT 60)
endif()

# build_tests — compile every test executable without running them.
# Use this before ctest when the build was configured with a specific --target
# (e.g. --target keel) that skips test binaries:
#
#   cmake --build <builddir> --target build_tests -j$(nproc)
#   ctest --test-dir <builddir> --output-on-failure
#
add_custom_target(build_tests DEPENDS
    test_admin_auth
    test_admin_sql
    test_admin_sql_fuzz
    test_admission
    test_alloc_inject
    test_audit_log
    test_auth
    test_backpressure
    test_buffer
    test_cancel_forwarding
    test_cloud_auth
    test_cloud_auth_e2e
    test_cloud_auth_internals
    test_cluster
    test_cluster_election
    test_cluster_fuzz
    test_compress
    test_concurrency_stress
    test_config
    test_result_cache_gate
    test_replay_log
    test_config_reload
    test_conn_lifecycle
    test_connpool
    test_connpool_exhaust
    test_connpool_stress
    test_crash_recovery_matrix
    test_pre_query_replay
    test_dirty_connection
    test_drain_shutdown
    test_dual_protocol_matrix
    test_e2e
    test_encoding
    test_failover
    test_fd_tracking
    test_fuzz_harness
    test_graceful_restart
    test_histogram
    test_hooks
    test_invariant_model
    test_listen_notify
    test_log
    test_log_plugins
    test_mem
    test_mem_safety
    test_migration
    test_mysql_protocol_flow
    test_ndjson_log
    test_numa
    test_osc_proxying
    test_otlp_encoding
    test_parser
    test_pg_protocol_flow
    test_plugin_contract
    test_pool_audit
    test_pool_correctness
    test_probe
    test_proto_split
    test_proxy_e2e
    test_proxy_session
    test_proxy_ssv_e2e
    test_ps_failover_tls
    test_ps_pool_matrix
    test_ps_semantic_compat
    test_query_cache
    test_query_log
    test_query_rules
    test_reactor
    test_residual
    test_ringbuf
    test_route_cache
    test_route_cache_stress
    test_router
    test_router_metrics
    test_router_plugin
    test_router_stress
    test_router_timeout
    test_runtime_mode
    test_ryw_propagation
    test_scatter_2pc
    test_scatter_merge_e2e
    test_scatter_sql_fuzz
    test_scatter_store
    test_scatter_window
    test_session_engine
    test_session_hooks_matrix
    test_shard_hot_reload
    test_sharding
    test_sm_fuzz
    test_sm_sequence_walk
    test_sm_stress
    test_sql
    test_ssv_atom
    test_ssv_core
    test_state_context
    test_state_contracts
    test_state_machine
    test_stats_alignment
    test_string_hash
    test_string_view
    test_throttle
    test_tls_auto
    test_tls_ktls
    test_tls_security
    test_tls_splice_matrix
    test_result_cache_gate
    test_trace
    test_util
    test_web_ui
    test_xxhash
)

# test_pg_parse is only built when libpq is available.
if(KEEL_HAS_LIBPQ)
    add_dependencies(build_tests test_pg_parse)
endif()
