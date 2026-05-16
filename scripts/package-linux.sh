#!/usr/bin/env bash
# ============================================================================
# package-linux.sh — Build release packages (tar.gz, DEB, RPM)
# ============================================================================
#
# Builds KEEL in Release mode, stages the installation via DESTDIR, and
# creates distributable packages for Linux deployment.
#
# Packaging stages:
#   1. cmake configure with CMAKE_BUILD_TYPE=Release and install prefix /usr.
#   2. Build with parallel make (JOBS cores).
#   3. DESTDIR staged install into a temporary directory.
#   4. Create a tar.gz archive from the staged tree.
#   5. Generate DEB and RPM packages via CPack.
#      Package name and version are read from CPackConfig.cmake.
#
# Output artifacts (written to OUT_DIR):
#   - <name>-<version>-linux-<arch>.tar.gz   — portable tarball
#   - <name>-<version>-linux-<arch>.deb      — Debian/Ubuntu package
#   - <name>-<version>-linux-<arch>.rpm      — RHEL/Fedora package
#
# Environment Variables:
#   BUILD_DIR   Build directory (default: <root>/build-package)
#   OUT_DIR     Output directory for packages (default: <root>/artifacts/packages)
#   JOBS        Parallel build jobs (default: nproc)
#
# Prerequisites:
#   - cmake, make on PATH
#   - dpkg-deb for DEB generation
#   - rpmbuild for RPM generation
#
# Exit Codes:
#   0  All packages built successfully
#   1  Build or packaging failure
#
# Usage:
#   ./scripts/package-linux.sh
#   JOBS=4 OUT_DIR=/tmp/packages ./scripts/package-linux.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build-package}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/artifacts/packages}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
JOBS="${JOBS:-$(nproc)}"
GENERATORS="${GENERATORS:-DEB;RPM}"

mkdir -p "$OUT_DIR"

echo "[package] root:      $ROOT_DIR"
echo "[package] build dir: $BUILD_DIR"
echo "[package] out dir:   $OUT_DIR"
echo "[package] type:      $BUILD_TYPE"
echo "[package] generators:$GENERATORS"

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
  -DCMAKE_C_STANDARD=23 \
  -DKEEL_ENABLE_TESTS=OFF \
  -DKEEL_ENABLE_HARDENING=ON

cmake --build "$BUILD_DIR" -j"$JOBS"

CPACK_CONFIG="$BUILD_DIR/CPackConfig.cmake"
PKG_NAME="$(sed -n 's/^set(CPACK_PACKAGE_NAME "\(.*\)")$/\1/p' "$CPACK_CONFIG" | head -1)"
PKG_VERSION="$(sed -n 's/^set(CPACK_PACKAGE_VERSION "\(.*\)")$/\1/p' "$CPACK_CONFIG" | head -1)"
PKG_ARCH="$(uname -m)"

if [[ -z "$PKG_NAME" || -z "$PKG_VERSION" ]]; then
  echo "[package] failed to read package metadata from $CPACK_CONFIG" >&2
  exit 1
fi

# Build a tar.gz payload from a staged root filesystem so /etc files are included.
# Use DESTDIR so absolute install destinations (for example /etc/keel) are
# redirected into the stage root instead of touching the host filesystem.
STAGE_DIR="$(mktemp -d "${OUT_DIR}/.stage.XXXXXX")"
ROOTFS_DIR="$STAGE_DIR/${PKG_NAME}-${PKG_VERSION}-Linux-${PKG_ARCH}"
mkdir -p "$ROOTFS_DIR"

DESTDIR="$ROOTFS_DIR" cmake --install "$BUILD_DIR" --prefix /usr

TGZ_FILE="$OUT_DIR/${PKG_NAME}-${PKG_VERSION}-Linux-${PKG_ARCH}.tar.gz"
tar -czf "$TGZ_FILE" -C "$STAGE_DIR" "$(basename "$ROOTFS_DIR")"
echo "[package] generated TGZ: $TGZ_FILE"

IFS=';' read -r -a gens <<< "$GENERATORS"
for gen in "${gens[@]}"; do
  echo "[package] generating $gen"
  cpack --config "$BUILD_DIR/CPackConfig.cmake" -G "$gen" -B "$OUT_DIR"
done

echo "[package] generated files:"
ls -1 "$OUT_DIR"
