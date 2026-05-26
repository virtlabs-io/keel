# =============================================================================
# src/observability/CMakeLists.txt
# =============================================================================
# Container for observability backends that live outside the hot path:
# OTLP (metrics + future logs), and later compatibility exporters that
# don't fit in src/admin (Prometheus remote-write, etc.).
# Only compiled when KEEL_ENABLE_OTLP=ON.
# =============================================================================

add_subdirectory(otlp)
