#!/usr/bin/env bash
# build_test_image.sh — Build the canonical KEEL test image (keel:test)
#
# Builds docker/Dockerfile.linux and tags the runner stage as keel:test.
# Skips the rebuild if the image already exists and no source file is newer
# than the image (pass --force to always rebuild).
#
# Usage:
#   scripts/build_test_image.sh [--force] [--tag TAG]
#
# Environment:
#   KEEL_TEST_IMAGE   Override the image tag (default: keel:test)
#   KEEL_BUILD_ARGS   Extra --build-arg flags forwarded to docker build
#
# The image is consumed by every test compose file via:
#   KEEL_IMAGE=keel:test docker compose -f docker/compose/<file>.yml up -d
#
# To upgrade the image for a session without touching the default:
#   KEEL_TEST_IMAGE=keel:test-pr42 scripts/build_test_image.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${KEEL_TEST_IMAGE:-keel:test}"
DOCKERFILE="docker/Dockerfile.linux"
FORCE=0

for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        --tag)   shift; IMAGE="$1" ;;
        --tag=*) IMAGE="${arg#--tag=}" ;;
    esac
done

# ---------------------------------------------------------------------------
# Staleness check: skip rebuild if the image exists and no tracked source
# file is newer than its creation timestamp.
# ---------------------------------------------------------------------------
if [[ $FORCE -eq 0 ]]; then
    if docker image inspect "$IMAGE" &>/dev/null; then
        IMAGE_DATE=$(docker image inspect "$IMAGE" \
            --format '{{.Created}}' | cut -c1-19 | tr 'T' ' ')
        NEWER=$(find "$ROOT/src" "$ROOT/include" "$ROOT/cmake" \
                     "$ROOT/CMakeLists.txt" "$ROOT/docker/$DOCKERFILE" \
                     "$ROOT/third_party/mimalloc" \
                -newer <(date -d "$IMAGE_DATE" +%s 2>/dev/null \
                    || date -j -f "%Y-%m-%d %H:%M:%S" "$IMAGE_DATE" +%s 2>/dev/null \
                    || echo 0) \
                -print -quit 2>/dev/null || true)
        if [[ -z "$NEWER" ]]; then
            echo "[test-image] $IMAGE is up-to-date — skipping rebuild."
            echo "[test-image] Use --force to rebuild unconditionally."
            exit 0
        fi
        echo "[test-image] Source changed since last build — rebuilding."
    fi
fi

echo "[test-image] Building $IMAGE from $DOCKERFILE ..."
cd "$ROOT"

docker build \
    --file "$DOCKERFILE" \
    --target runner \
    --tag "$IMAGE" \
    --build-arg KEEL_BUILD_TYPE="${KEEL_BUILD_TYPE:-RelWithDebInfo}" \
    ${KEEL_BUILD_ARGS:-} \
    .

echo "[test-image] Done: $IMAGE"
docker image inspect "$IMAGE" --format \
    '  Size:    {{.Size}} bytes
  Created: {{.Created}}'
