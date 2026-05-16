#!/bin/bash
# OSS-Fuzz build script for KEEL.
#
# This script is called by the OSS-Fuzz infrastructure inside the
# oss-fuzz builder Docker image.  It is also runnable locally with:
#
#   python infra/helper.py build_fuzzers keel --sanitizer address
#
# Required environment variables (set by OSS-Fuzz):
#   $SRC        — checked-out source trees (keel at $SRC/keel)
#   $OUT        — destination for fuzz target binaries and corpus archives
#   $WORK       — scratch directory
#   $SANITIZER  — "address" | "memory" | "undefined" | "coverage" | "none"
#   $CC / $CXX  — compiler wrappers provided by OSS-Fuzz
#   $CFLAGS / $CXXFLAGS — sanitizer and coverage flags

set -euo pipefail

KEEL_SRC="${SRC}/keel"
BUILD_DIR="${WORK}/build"

# ── Translate OSS-Fuzz sanitizer to KEEL CMake options ──────────────────────
CMAKE_SANITIZER_ARGS=""
case "${SANITIZER}" in
    address)
        CMAKE_SANITIZER_ARGS="-DKEEL_ENABLE_ASAN=ON -DKEEL_ENABLE_UBSAN=ON"
        ;;
    memory)
        CMAKE_SANITIZER_ARGS="-DKEEL_ENABLE_MSAN=ON"
        ;;
    undefined)
        CMAKE_SANITIZER_ARGS="-DKEEL_ENABLE_UBSAN=ON"
        ;;
    coverage|none)
        CMAKE_SANITIZER_ARGS=""
        ;;
esac

# ── Configure ────────────────────────────────────────────────────────────────
cmake -S "${KEEL_SRC}" -B "${BUILD_DIR}" \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_COMPILER="${CC}" \
    -DCMAKE_CXX_COMPILER="${CXX}" \
    -DCMAKE_C_FLAGS="${CFLAGS}" \
    -DCMAKE_EXE_LINKER_FLAGS="${LIB_FUZZING_ENGINE}" \
    -DKEEL_ENABLE_FUZZ=ON \
    -DKEEL_USE_IOURING=OFF \
    -DKEEL_ENABLE_HARDENING=OFF \
    -DKEEL_ENABLE_LUA=OFF \
    -DKEEL_ENABLE_PYTHON=OFF \
    -DKEEL_ENABLE_COVERAGE=OFF \
    ${CMAKE_SANITIZER_ARGS}

# ── Build fuzz targets ────────────────────────────────────────────────────────
FUZZ_TARGETS=(
    test_fuzz_harness
    test_admin_sql_fuzz
    test_sm_fuzz
    test_cluster_fuzz
)

cmake --build "${BUILD_DIR}" \
    --target "${FUZZ_TARGETS[@]}" \
    -- -j"$(nproc)"

# ── Install binaries into $OUT ────────────────────────────────────────────────
for target in "${FUZZ_TARGETS[@]}"; do
    cp "${BUILD_DIR}/tests/${target}" "${OUT}/${target}"
done

# ── Package seed corpora ──────────────────────────────────────────────────────
# OSS-Fuzz expects a zip archive named <target>_seed_corpus.zip per fuzzer.
# We use the shared seeds for all targets; targets will discard irrelevant bytes.
SEEDS_DIR="${KEEL_SRC}/tests/fuzz_seeds"
if [[ -d "${SEEDS_DIR}" ]]; then
    for target in "${FUZZ_TARGETS[@]}"; do
        zip -j "${OUT}/${target}_seed_corpus.zip" "${SEEDS_DIR}"/*.bin 2>/dev/null || true
    done
fi
