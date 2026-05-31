# =============================================================================
# src/worker/CMakeLists.txt — Per-worker reactor loop and async helpers
# =============================================================================
#
# This target is the execution core of KEEL: the per-worker reactor loop plus
# the async helpers it delegates to for backend acquisition, frontend TLS, and
# inter-worker migration.  Keeping these sources in one library makes the hot
# path's internal interfaces easy to evolve without turning every helper into a
# separately versioned public component.
#
# Source files:
#   worker.c               — Main worker thread entry, reactor event dispatch
#   backend_pool.c         — Backend connection pool management (acquire/release)
#   backend_connect_async.c — Non-blocking backend connection establishment
#   frontend_tls_async.c   — Non-blocking frontend TLS handshake (OpenSSL BIO)
#   migration.c            — Inter-worker session migration (FD passing + state)
#   worker_catchup.c       — Reactor-owned replica catch-up wait list (Phase 2)
# =============================================================================

add_library(keel_worker_new STATIC
    worker.c
    backend_pool.c
    backend_connect_async.c
    frontend_tls_async.c
    migration.c
    worker_catchup.c
)

target_include_directories(keel_worker_new
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(keel_worker_new
    PUBLIC keel_arch_common
    PUBLIC keel_arch_platform
    PUBLIC keel_session
    PUBLIC keel_engine_flow
    PUBLIC keel_proto_flow_registry
    PUBLIC keelsql
    PUBLIC keelmem
    PUBLIC keelutil
    PUBLIC keelstats
    PUBLIC keelcore
    PUBLIC keel_protocol
    PUBLIC keel_tls
    PUBLIC Threads::Threads
)
