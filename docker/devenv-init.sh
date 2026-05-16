#!/usr/bin/env bash
# Runs inside the devenv container at startup.
# Performs a one-time cmake configure if the build directory is not yet
# initialised, then executes whatever CMD was passed (default: bash).

set -euo pipefail

BUILD_DIR="/keel/build-linux"

if [[ ! -f "${BUILD_DIR}/CMakeCache.txt" ]]; then
    echo "[devenv] Configuring cmake (first run)..."
    mkdir -p "${BUILD_DIR}"
    cmake -B "${BUILD_DIR}" -G Ninja \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DKEEL_USE_IOURING=ON \
        -DKEEL_USE_EPOLL=ON \
        -DKEEL_ENABLE_TESTS=ON \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        /keel
    echo "[devenv] cmake configure done."
else
    echo "[devenv] cmake already configured — skipping configure step."
fi

exec "$@"
