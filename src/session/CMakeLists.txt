# =============================================================================
# src/session/CMakeLists.txt — Worker-local session runtime library
# =============================================================================
#
# This target groups the worker-local session runtime pieces that support safe
# backend multiplexing: lifecycle state, residual buffering, canonical state
# profiles, semantic-state virtualisation helpers, hard-pin detection, route
# caching, and admission control.  Keeping them in a dedicated static library
# lets engine and worker layers depend on a stable runtime contract without
# pulling in protocol-specific implementations.
#
# Source files:
#   session.c       — Session lifecycle state machine and slot management
#   residual.c      — Residual read buffer for partial protocol messages
#   state_profile.c — Canonical session-context snapshot (SET/SEARCH_PATH/etc.)
#   ssv.c           — Semantic State Virtualisation (replay across backends)
#   hardpin.c       — Hard-pin detection (forces session→backend 1:1 binding)
#   route_cache.c   — L1 query→route lookup cache (per worker, lock-free)
#   admission.c     — Frontend/backend connection limits and wait queue
# =============================================================================

add_library(keel_session STATIC
    session.c
    residual.c
    state_profile.c
    ssv.c
    hardpin.c
    route_cache.c
    admission.c
)

target_include_directories(keel_session
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(keel_session
    PUBLIC keelmem
    PUBLIC keelutil
)
