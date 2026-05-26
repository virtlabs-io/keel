# =============================================================================
# src/observability/otlp/CMakeLists.txt — OTLP metrics exporter
# =============================================================================
# Three-layer module per proposals/v0.2-alpha_observability.md §22.3:
#
#   keel_otlp_encode   — snapshot → nanopb message conversion (cold path)
#   keel_otlp_http     — bounded-timeout HTTP/1.1 POST client
#   keel_otlp_exporter — aggregation timer + retry/backoff + queue drain
#
# Generated OTel proto files live in ${CMAKE_BINARY_DIR}/generated/otel and
# are produced at build time from the pinned third_party/otel-proto submodule
# using nanopb's generator (which needs Python + the `protobuf` module, or a
# system protoc; the helper venv at third_party/build-venv satisfies both).
# =============================================================================

set(KEEL_OTLP_NANOPB_DIR    ${CMAKE_SOURCE_DIR}/third_party/nanopb)
set(KEEL_OTLP_PROTO_DIR     ${CMAKE_SOURCE_DIR}/third_party/otel-proto)
set(KEEL_OTLP_BUILD_VENV    ${CMAKE_SOURCE_DIR}/third_party/build-venv)
set(KEEL_OTLP_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated/otel)

if(EXISTS ${KEEL_OTLP_BUILD_VENV}/bin/python)
    set(KEEL_OTLP_PYTHON ${KEEL_OTLP_BUILD_VENV}/bin/python)
else()
    find_program(KEEL_OTLP_PYTHON NAMES python3 python REQUIRED)
endif()

set(KEEL_OTLP_PROTO_FILES
    opentelemetry/proto/common/v1/common.proto
    opentelemetry/proto/resource/v1/resource.proto
    opentelemetry/proto/metrics/v1/metrics.proto
    opentelemetry/proto/collector/metrics/v1/metrics_service.proto
)

set(KEEL_OTLP_GENERATED_SOURCES "")
set(KEEL_OTLP_GENERATED_HEADERS "")
set(KEEL_OTLP_PROTO_INPUTS "")
foreach(_proto IN LISTS KEEL_OTLP_PROTO_FILES)
    string(REGEX REPLACE "\\.proto$" ".pb.c" _src ${_proto})
    string(REGEX REPLACE "\\.proto$" ".pb.h" _hdr ${_proto})
    list(APPEND KEEL_OTLP_GENERATED_SOURCES ${KEEL_OTLP_GENERATED_DIR}/${_src})
    list(APPEND KEEL_OTLP_GENERATED_HEADERS ${KEEL_OTLP_GENERATED_DIR}/${_hdr})
    list(APPEND KEEL_OTLP_PROTO_INPUTS      ${KEEL_OTLP_PROTO_DIR}/${_proto})
endforeach()

add_custom_command(
    OUTPUT  ${KEEL_OTLP_GENERATED_SOURCES} ${KEEL_OTLP_GENERATED_HEADERS}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${KEEL_OTLP_GENERATED_DIR}
    COMMAND ${KEEL_OTLP_PYTHON}
            ${KEEL_OTLP_NANOPB_DIR}/generator/nanopb_generator.py
            --proto-path=${KEEL_OTLP_PROTO_DIR}
            --proto-path=${KEEL_OTLP_NANOPB_DIR}/generator/proto
            --output-dir=${KEEL_OTLP_GENERATED_DIR}
            ${KEEL_OTLP_PROTO_INPUTS}
    DEPENDS ${KEEL_OTLP_PROTO_INPUTS}
    COMMENT "Generating OTLP nanopb sources from pinned otel-proto"
    VERBATIM
)

add_custom_target(keel_otlp_codegen DEPENDS ${KEEL_OTLP_GENERATED_SOURCES})

add_library(keel_otlp STATIC
    keel_otlp_encode.c
    keel_otlp_http.c
    keel_otlp_exporter.c
    keel_snapshot_queue.c
    keel_exporter_json.c
    keel_otlp_config.c
    keel_otlp_aggregator.c
    keel_prom_format.c
    ${KEEL_OTLP_NANOPB_DIR}/pb_common.c
    ${KEEL_OTLP_NANOPB_DIR}/pb_encode.c
    ${KEEL_OTLP_NANOPB_DIR}/pb_decode.c
    ${KEEL_OTLP_GENERATED_SOURCES}
)
add_dependencies(keel_otlp keel_otlp_codegen)

target_include_directories(keel_otlp
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC  ${CMAKE_SOURCE_DIR}/include
    PUBLIC  ${CMAKE_BINARY_DIR}/include
    PUBLIC  ${KEEL_OTLP_NANOPB_DIR}
    PUBLIC  ${KEEL_OTLP_GENERATED_DIR}
)

target_link_libraries(keel_otlp
    PUBLIC  keelstats
    PUBLIC  keelmem
    PUBLIC  keelcore
    PRIVATE Threads::Threads
)

# Third-party generated sources are exempt from KEEL's strict warning set.
set_source_files_properties(
    ${KEEL_OTLP_GENERATED_SOURCES}
    ${KEEL_OTLP_NANOPB_DIR}/pb_common.c
    ${KEEL_OTLP_NANOPB_DIR}/pb_encode.c
    ${KEEL_OTLP_NANOPB_DIR}/pb_decode.c
    PROPERTIES COMPILE_FLAGS "-w"
)

target_compile_definitions(keel_otlp PUBLIC KEEL_HAS_OTLP=1)
