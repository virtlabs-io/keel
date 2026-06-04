#!/usr/bin/env bash
# =============================================================================
# KEEL - Linux Docker helper (run from macOS or any Docker host)
# =============================================================================
# Usage:
#   ./docker/build-linux.sh <command>
#
# Commands:
#   build     Build the keel binary inside a Linux container (default)
#   test      Build + run the full unit-test suite
#   image     Produce a minimal runtime Docker image  (tag: keel:linux)
#   run       Launch keel from the runtime image (mount config from ./etc/)
#   shell     Open an interactive shell in the build container
#   valgrind  Re-run tests under Valgrind inside the container
#   clean     Remove Docker images and local build-linux/ artefacts
#
# Apple Silicon note:
#   By default the image is built for your host architecture (arm64 on M-series
#   Macs).  To cross-compile for linux/amd64 pass:
#     KEEL_DOCKER_PLATFORM=linux/amd64 ./docker/build-linux.sh build
#
# Build variants:
#   KEEL_VARIANT=core (default) — no Lua/Python scripting hooks (smaller,
#                                  fewer dynamic dependencies, smaller attack
#                                  surface).
#   KEEL_VARIANT=full           — Lua 5.4 + Python 3 embedded for hook
#                                  scripting. Use for plugin development.
#   Example:
#     KEEL_VARIANT=full ./docker/build-linux.sh test
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile.linux"

: "${KEEL_DOCKER_PLATFORM:=}"
: "${KEEL_BUILD_IMAGE:=keel-linux-build}"
: "${KEEL_TEST_IMAGE:=keel-linux-test}"
: "${KEEL_RUNTIME_IMAGE:=keel:linux}"
: "${KEEL_CONFIG_FILE:=${PROJECT_DIR}/etc/keel-mix.ini}"
: "${KEEL_PUBLISH_IMAGE:=ghcr.io/virtlabs/keel}"
: "${KEEL_PUBLISH_TAG:=latest}"
: "${KEEL_PUBLISH_PLATFORMS:=linux/amd64,linux/arm64}"
# KEEL_VARIANT selects the scripting feature set baked into the image:
#   core  → no Lua/Python (default, smaller attack surface)
#   full  → Lua 5.4 + Python 3 embedded
: "${KEEL_VARIANT:=core}"
case "${KEEL_VARIANT}" in
    core|full) ;;
    *) echo "ERROR: unknown KEEL_VARIANT='${KEEL_VARIANT}' (expected: core|full)" >&2; exit 2 ;;
esac

BUILD_ARGS=()
[[ -n "${KEEL_DOCKER_PLATFORM}" ]] && BUILD_ARGS+=(--platform "${KEEL_DOCKER_PLATFORM}")
BUILD_ARGS+=(--build-arg "KEEL_VARIANT=${KEEL_VARIANT}")

_r='\033[0;31m'; _g='\033[0;32m'; _y='\033[1;33m'; _b='\033[0;34m'; _n='\033[0m'
info()    { echo -e "${_b}[INFO]${_n}    $*"; }
success() { echo -e "${_g}[SUCCESS]${_n} $*"; }
warn()    { echo -e "${_y}[WARN]${_n}    $*"; }
error()   { echo -e "${_r}[ERROR]${_n}   $*"; }

require_docker() {
    if ! command -v docker &>/dev/null; then
        error "Docker is not installed or not in PATH."
        exit 1
    fi
}

# Build binary only (--target builder)
cmd_build() {
    info "Building keel Linux binary (io_uring, RelWithDebInfo)..."
    docker build ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
        --target builder \
        --tag  "${KEEL_BUILD_IMAGE}" \
        --file "${DOCKERFILE}" \
        "${PROJECT_DIR}"
    success "Binary built -> image: ${KEEL_BUILD_IMAGE}"
    info "To extract binary locally:"
    info "  docker create --name _keel_extract ${KEEL_BUILD_IMAGE}"
    info "  mkdir -p build-linux && docker cp _keel_extract:/keel/build-linux/src/main/keel build-linux/"
    info "  docker rm _keel_extract"
}

# Build + run unit tests (--target tester)
cmd_test() {
    info "Building keel and running unit tests on Linux..."
    docker build ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
        --target tester \
        --tag  "${KEEL_TEST_IMAGE}" \
        --file "${DOCKERFILE}" \
        "${PROJECT_DIR}"
    success "All tests passed -> image: ${KEEL_TEST_IMAGE}"
}

# Minimal runtime image (--target runner)
cmd_image() {
    info "Building minimal keel runtime image..."
    docker build ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
        --target runner \
        --tag  "${KEEL_RUNTIME_IMAGE}" \
        --file "${DOCKERFILE}" \
        "${PROJECT_DIR}"
    success "Runtime image ready -> ${KEEL_RUNTIME_IMAGE}"
    info "Run it with:  ./docker/build-linux.sh run"
}

# Run keel from the runtime image
cmd_run() {
    if ! docker image inspect "${KEEL_RUNTIME_IMAGE}" &>/dev/null; then
        warn "Runtime image not found - building it now..."
        cmd_image
    fi
    if [[ ! -f "${KEEL_CONFIG_FILE}" ]]; then
        error "Config file not found: ${KEEL_CONFIG_FILE}"
        error "Set KEEL_CONFIG_FILE to an existing .ini file."
        exit 1
    fi
    local cfg_basename
    cfg_basename="$(basename "${KEEL_CONFIG_FILE}")"
    info "Starting keel  config: ${KEEL_CONFIG_FILE}"
    info "  PostgreSQL proxy -> host port 7432"
    info "  MySQL proxy      -> host port 7306"
    info "  Admin console    -> psql -h 127.0.0.1 -p 6433 -U admin"
    info "  Prometheus       -> http://127.0.0.1:9101/metrics"
    info "  Press Ctrl-C to stop."
    docker run --rm -it \
        --name keel \
        --publish 7432:7432 \
        --publish 7306:7306 \
        --publish 6433:6433 \
        --publish 9101:9101 \
        --volume "${KEEL_CONFIG_FILE}:/etc/keel/${cfg_basename}:ro" \
        "${KEEL_RUNTIME_IMAGE}" \
        -c "/etc/keel/${cfg_basename}"
}

# Interactive shell in builder image
cmd_shell() {
    if ! docker image inspect "${KEEL_BUILD_IMAGE}" &>/dev/null; then
        warn "Build image not found - building it now..."
        cmd_build
    fi
    info "Opening shell  (source: /keel   build: /keel/build-linux)"
    docker run --rm -it --name keel-shell "${KEEL_BUILD_IMAGE}" bash
}

# Valgrind memory checks
cmd_valgrind() {
    if ! docker image inspect "${KEEL_BUILD_IMAGE}" &>/dev/null; then
        warn "Build image not found - building it now..."
        cmd_build
    fi
    info "Running unit tests under Valgrind..."
    docker run --rm --name keel-valgrind "${KEEL_BUILD_IMAGE}" bash -c '
        set -e; cd /keel/build-linux; pass=0; fail=0
        for t in tests/test_*; do
            [[ -x "$t" ]] || continue
            echo "=== valgrind: $t ==="
            valgrind --leak-check=full --error-exitcode=1 "$t" 2>&1 && ((pass++)) || ((fail++))
        done
        echo ""; echo "Valgrind: $pass passed, $fail failed"; [[ $fail -eq 0 ]]
    '
    success "Valgrind checks passed"
}

# ─────────────────────────────────────────────────────────────────────────────
# Dev workflow
#
# Provides a fast inner-loop dev environment on macOS backed by a Linux
# container that uses io_uring/epoll (the target OS).  The project source is
# bind-mounted so edits on the host are immediately visible inside the
# container without a rebuild of the Docker image.
#
# Sub-commands:
#   dev up           Start postgres + devenv container
#   dev down         Stop and remove the dev containers/network
#   dev build-image  (Re)build the devenv Docker image
#   dev build        Incremental cmake/ninja build (fast, seconds not minutes)
#   dev test         Run the full unit-test suite inside the container
#   dev run          Build + run keel proxy (connects to dev postgres)
#   dev sbprep       Run sysbench prepare (create tables in sbtest db)
#   dev sbrun        Run sysbench oltp_read_write (32 threads, 30 s)
#   dev sysbench     sbprep + sbrun in one step
#   dev shell        Open an interactive bash shell in the devenv container
# ─────────────────────────────────────────────────────────────────────────────
DEV_COMPOSE="${SCRIPT_DIR}/compose/pg-dev.yml"
DEV_CONTAINER="keel-devenv"
DEV_IMAGE="keel-devenv"
KEEL_DEV_CFG="/keel/docker/keel/keel-dev.ini"

# Sysbench defaults (override via env)
: "${SB_TABLES:=4}"
: "${SB_TABLE_SIZE:=100000}"
: "${SB_THREADS:=32}"
: "${SB_TIME:=30}"
# Where keel proxy listens inside the container
: "${KEEL_HOST:=127.0.0.1}"
: "${KEEL_PORT:=7432}"
# PostgreSQL direct credentials (for sysbench prepare)
: "${PG_HOST:=postgres}"
: "${PG_PORT:=5432}"
: "${PG_USER:=sbtest}"
: "${PG_PASS:=sbtest}"
: "${PG_DB:=sbtest}"

_dev_compose() { docker compose -f "${DEV_COMPOSE}" "$@"; }

_dev_exec() {
    # Run a command in the (already-running) devenv container.
    docker exec -it "${DEV_CONTAINER}" "$@"
}

_dev_exec_bg() {
    docker exec "${DEV_CONTAINER}" "$@"
}

_ensure_devenv_running() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${DEV_CONTAINER}$"; then
        warn "devenv container not running — starting dev environment..."
        cmd_dev_up
        sleep 2
    fi
}

cmd_dev_up() {
    info "Starting dev environment (postgres + devenv)..."
    _dev_compose up -d --build
    info "Waiting for postgres to be healthy..."
    for i in $(seq 1 30); do
        if _dev_compose exec postgres pg_isready -U postgres >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    success "Dev environment ready."
    info "  keel proxy (after 'dev run'):  host 127.0.0.1:7432"
    info "  postgres direct:               psql -h 127.0.0.1 -p 5499 -U postgres"
    info "  shell:                         ./docker/build-linux.sh dev shell"
}

cmd_dev_down() {
    info "Stopping dev environment..."
    _dev_compose down
    success "Dev environment stopped."
}

cmd_dev_build_image() {
    info "Building devenv Docker image (${DEV_IMAGE})..."
    docker build "${BUILD_ARGS[@]}" \
        --target devenv \
        --tag    "${DEV_IMAGE}" \
        --file   "${SCRIPT_DIR}/Dockerfile.linux" \
        "${PROJECT_DIR}"
    success "devenv image built: ${DEV_IMAGE}"
}

cmd_dev_build() {
    _ensure_devenv_running
    info "Building keel (incremental ninja build inside container)..."
    _dev_exec_bg bash -c "cmake --build /keel/build-linux --target keel 2>&1"
    success "Build complete.  Binary: /keel/build-linux/src/main/keel"
}

cmd_dev_test() {
    _ensure_devenv_running
    info "Running unit tests..."
    _dev_exec bash -c "cd /keel/build-linux && ctest --output-on-failure -j\$(nproc)"
}

cmd_dev_run() {
    _ensure_devenv_running
    cmd_dev_build
    info "Starting keel proxy inside container  (listen 0.0.0.0:7432)..."
    info "  Connect from macOS: psql -h 127.0.0.1 -p 7432 -U postgres"
    info "  Press Ctrl-C to stop."
    _dev_exec /keel/build-linux/src/main/keel "${KEEL_DEV_CFG}"
}

cmd_dev_sbprep() {
    _ensure_devenv_running
    info "Running sysbench prepare (tables=${SB_TABLES} size=${SB_TABLE_SIZE})..."
    _dev_exec_bg sysbench oltp_read_write \
        --db-driver=pgsql \
        --pgsql-host="${PG_HOST}" --pgsql-port="${PG_PORT}" \
        --pgsql-user="${PG_USER}" --pgsql-password="${PG_PASS}" \
        --pgsql-db="${PG_DB}" \
        --tables="${SB_TABLES}" --table-size="${SB_TABLE_SIZE}" \
        prepare
    success "sysbench tables prepared."
}

cmd_dev_sbrun() {
    _ensure_devenv_running
    info "Running sysbench oltp_read_write (threads=${SB_THREADS} time=${SB_TIME}s) through keel..."
    info "  (keel must be running — start it with: dev run)"
    _dev_exec sysbench oltp_read_write \
        --db-driver=pgsql \
        --pgsql-host="${KEEL_HOST}" --pgsql-port="${KEEL_PORT}" \
        --pgsql-user="${PG_USER}" --pgsql-password="${PG_PASS}" \
        --pgsql-db="${PG_DB}" \
        --tables="${SB_TABLES}" --table-size="${SB_TABLE_SIZE}" \
        --threads="${SB_THREADS}" --time="${SB_TIME}" \
        run
}

cmd_dev_sysbench() {
    cmd_dev_sbprep
    info "Starting keel in background for sysbench test..."
    _dev_exec_bg bash -c \
        "/keel/build-linux/src/main/keel ${KEEL_DEV_CFG} >/tmp/keel-dev.log 2>&1 &"
    sleep 2
    cmd_dev_sbrun
    _dev_exec_bg pkill -f "keel.*keel-dev.ini" 2>/dev/null || true
}

cmd_dev_shell() {
    _ensure_devenv_running
    info "Opening shell in devenv container  (source: /keel  build: /keel/build-linux)"
    _dev_exec bash
}

cmd_dev() {
    local subcmd="${1:-help}"
    shift || true
    case "${subcmd}" in
        up)           cmd_dev_up           ;;
        down)         cmd_dev_down         ;;
        build-image)  cmd_dev_build_image  ;;
        build)        cmd_dev_build        ;;
        test)         cmd_dev_test         ;;
        run)          cmd_dev_run          ;;
        sbprep)       cmd_dev_sbprep       ;;
        sbrun)        cmd_dev_sbrun        ;;
        sysbench)     cmd_dev_sysbench     ;;
        shell)        cmd_dev_shell        ;;
        help|--help|-h)
            echo "dev sub-commands:"
            echo "  up            Start postgres + devenv container"
            echo "  down          Stop the dev environment"
            echo "  build-image   (Re)build the devenv Docker image"
            echo "  build         Incremental keel build inside container"
            echo "  test          Run unit-test suite inside container"
            echo "  run           Build + run keel proxy (needs 'up' first)"
            echo "  sbprep        sysbench prepare (create tables)"
            echo "  sbrun         sysbench run through keel proxy"
            echo "  sysbench      sbprep + background keel + sbrun (all-in-one)"
            echo "  shell         Interactive bash shell in devenv container"
            echo ""
            echo "Typical first-time workflow:"
            echo "  ./docker/build-linux.sh dev up"
            echo "  ./docker/build-linux.sh dev build"
            echo "  ./docker/build-linux.sh dev sysbench"
            ;;
        *)
            error "Unknown dev sub-command: ${subcmd}"
            cmd_dev help
            exit 1
            ;;
    esac
}

# Multi-arch publish with docker buildx
# Pushes to ${KEEL_PUBLISH_IMAGE}:${KEEL_PUBLISH_TAG} for both amd64 + arm64.
#
# Prerequisites:
#   docker buildx create --use --name keel-builder
#
# Usage:
#   ./docker/build-linux.sh publish
#   KEEL_PUBLISH_TAG=<release-tag> ./docker/build-linux.sh publish
#   KEEL_PUBLISH_IMAGE=myrepo/keel KEEL_PUBLISH_TAG=edge ./docker/build-linux.sh publish
cmd_publish() {
    if ! docker buildx version &>/dev/null; then
        error "docker buildx is required for multi-arch builds."
        error "Install: https://docs.docker.com/buildx/working-with-buildx/"
        exit 1
    fi
    local image_tag="${KEEL_PUBLISH_IMAGE}:${KEEL_PUBLISH_TAG}"
    info "Building multi-arch image for: ${KEEL_PUBLISH_PLATFORMS}"
    info "Target: ${image_tag}"
    docker buildx build \
        --platform "${KEEL_PUBLISH_PLATFORMS}" \
        --target runner \
        --tag "${image_tag}" \
        --file "${PROJECT_DIR}/Dockerfile" \
        --push \
        "${PROJECT_DIR}"
    success "Published: ${image_tag}"
    info "Pull with: docker pull ${image_tag}"
}

# Multi-arch test build (no push) — verifies the official Dockerfile builds
cmd_publish_dry_run() {
    if ! docker buildx version &>/dev/null; then
        error "docker buildx is required."
        exit 1
    fi
    local image_tag="${KEEL_PUBLISH_IMAGE}:${KEEL_PUBLISH_TAG}"
    info "Dry-run multi-arch build (no push) for: ${KEEL_PUBLISH_PLATFORMS}"
    docker buildx build \
        --platform "${KEEL_PUBLISH_PLATFORMS}" \
        --target runner \
        --tag "${image_tag}" \
        --file "${PROJECT_DIR}/Dockerfile" \
        "${PROJECT_DIR}"
    success "Dry-run build OK: ${image_tag}"
}

# Clean up
cmd_clean() {
    info "Removing Docker images..."
    docker rmi -f "${KEEL_BUILD_IMAGE}"   2>/dev/null || true
    docker rmi -f "${KEEL_TEST_IMAGE}"    2>/dev/null || true
    docker rmi -f "${KEEL_RUNTIME_IMAGE}" 2>/dev/null || true
    docker rm  -f keel keel-shell keel-valgrind _keel_extract 2>/dev/null || true
    info "Removing local build-linux/..."
    rm -rf "${PROJECT_DIR}/build-linux"
    success "Clean complete"
}

show_usage() {
    echo "KEEL Linux Docker helper"
    echo "========================"
    echo "Usage: ./docker/build-linux.sh <command>"
    echo ""
    echo "Commands:"
    echo "  build        Compile the keel binary in a Linux container  (default)"
    echo "  test         Compile + run the full unit-test suite"
    echo "  image        Build a minimal runtime image (tag: keel:linux)"
    echo "  run          Launch keel from the runtime image"
    echo "  shell        Open an interactive shell in the build container"
    echo "  valgrind     Memory-check unit tests with Valgrind"
    echo "  publish      Multi-arch build + push to registry (requires buildx)"
    echo "  publish-dry  Multi-arch build without push (smoke test)"
    echo "  clean        Remove Docker images and local build-linux/ artefacts"
    echo "  dev <sub>    Inner-loop dev env (Linux/io_uring) — run 'dev help'"
    echo ""
    echo "Environment variables:"
    echo "  KEEL_DOCKER_PLATFORM     Docker --platform (e.g. linux/amd64)  default: native"
    echo "  KEEL_BUILD_IMAGE         Builder image name  default: keel-linux-build"
    echo "  KEEL_TEST_IMAGE          Tester image name   default: keel-linux-test"
    echo "  KEEL_RUNTIME_IMAGE       Runtime image name  default: keel:linux"
    echo "  KEEL_CONFIG_FILE         Config for run      default: ./etc/keel-mix.ini"
    echo "  KEEL_PUBLISH_IMAGE       Registry image      default: ghcr.io/virtlabs/keel"
    echo "  KEEL_PUBLISH_TAG         Image tag           default: latest"
    echo "  KEEL_PUBLISH_PLATFORMS   Build platforms     default: linux/amd64,linux/arm64"
    echo ""
    echo "Examples:"
    echo "  ./docker/build-linux.sh build"
    echo "  ./docker/build-linux.sh test"
    echo "  ./docker/build-linux.sh image"
    echo "  ./docker/build-linux.sh run"
    echo "  KEEL_PUBLISH_TAG=1.0.0 ./docker/build-linux.sh publish"
    echo "  KEEL_CONFIG_FILE=./etc/keel-pg.ini ./docker/build-linux.sh run"
    echo "  KEEL_DOCKER_PLATFORM=linux/amd64  ./docker/build-linux.sh test"
}

require_docker
cd "${PROJECT_DIR}"

case "${1:-build}" in
    build)       cmd_build              ;;
    test)        cmd_test               ;;
    image)       cmd_image              ;;
    run)         cmd_run                ;;
    shell)       cmd_shell              ;;
    valgrind)    cmd_valgrind           ;;
    publish)     cmd_publish            ;;
    publish-dry) cmd_publish_dry_run    ;;
    clean)       cmd_clean              ;;
    dev)         cmd_dev "${@:2}"      ;;
    -h|--help|help) show_usage          ;;
    *)
        error "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
