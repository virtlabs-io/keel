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
KEEL_PACKAGE_DISTRO="${KEEL_PACKAGE_DISTRO:-}"

mkdir -p "$OUT_DIR"

echo "[package] root:      $ROOT_DIR"
echo "[package] build dir: $BUILD_DIR"
echo "[package] out dir:   $OUT_DIR"
echo "[package] type:      $BUILD_TYPE"
echo "[package] generators:$GENERATORS"
echo "[package] distro:    ${KEEL_PACKAGE_DISTRO:-<none>}"

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
  -DCMAKE_C_STANDARD=23 \
  -DKEEL_PACKAGE_DISTRO="$KEEL_PACKAGE_DISTRO" \
  -DKEEL_ENABLE_TESTS=OFF \
  -DKEEL_ENABLE_HARDENING=ON

cmake --build "$BUILD_DIR" -j"$JOBS"

CPACK_CONFIG="$BUILD_DIR/CPackConfig.cmake"
PKG_NAME="$(sed -n 's/^set(CPACK_PACKAGE_NAME "\(.*\)")$/\1/p' "$CPACK_CONFIG" | head -1)"
PKG_VERSION="$(sed -n 's/^set(CPACK_PACKAGE_VERSION "\(.*\)")$/\1/p' "$CPACK_CONFIG" | head -1)"
PKG_FILE_NAME="$(sed -n 's/^set(CPACK_PACKAGE_FILE_NAME "\(.*\)")$/\1/p' "$CPACK_CONFIG" | head -1)"
PKG_ARCH="$(uname -m)"

if [[ -z "$PKG_NAME" || -z "$PKG_VERSION" || -z "$PKG_FILE_NAME" ]]; then
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

TGZ_FILE="$OUT_DIR/${PKG_FILE_NAME}.tar.gz"
tar -czf "$TGZ_FILE" -C "$STAGE_DIR" "$(basename "$ROOTFS_DIR")"
echo "[package] generated TGZ: $TGZ_FILE"

IFS=';' read -r -a gens <<< "$GENERATORS"
for gen in "${gens[@]}"; do
  echo "[package] generating $gen"
  before_file_list="$(mktemp)"
  after_file_list="$(mktemp)"
  find "$OUT_DIR" -maxdepth 1 -type f | sort > "$before_file_list"
  cpack --config "$BUILD_DIR/CPackConfig.cmake" -G "$gen" -B "$OUT_DIR"
  find "$OUT_DIR" -maxdepth 1 -type f | sort > "$after_file_list"

  mapfile -t new_files < <(comm -13 "$before_file_list" "$after_file_list")
  rm -f "$before_file_list" "$after_file_list"

  case "$gen" in
    DEB) ext="deb" ;;
    RPM) ext="rpm" ;;
    *) ext="" ;;
  esac

  if [[ -n "$ext" ]]; then
    package_file=""
    for file in "${new_files[@]}"; do
      if [[ "$file" == *."$ext" ]]; then
        package_file="$file"
        break
      fi
    done

    if [[ -z "$package_file" ]]; then
      echo "[package] failed to locate generated .$ext artifact for $gen" >&2
      exit 1
    fi

    final_file="$OUT_DIR/${PKG_FILE_NAME}.$ext"
    if [[ "$package_file" != "$final_file" ]]; then
      mv "$package_file" "$final_file"
    fi
    echo "[package] normalized $gen artifact: $final_file"
  fi
done

echo "[package] generated files:"
ls -1 "$OUT_DIR"
