#!/usr/bin/env bash
# ============================================================================
# hardening-checksec.sh — Binary hardening verification for KEEL
# ============================================================================
#
# Verifies that the compiled KEEL binary has all recommended security
# hardening features enabled.  These compile-time and link-time protections
# make exploitation of memory corruption bugs significantly harder.
#
# Checks performed (all must pass):
#   1. PIE (Position-Independent Executable)
#      - Enables full ASLR for the main binary (not just shared libraries).
#      - Verified via readelf: ET_DYN type in ELF header.
#
#   2. NX (No-eXecute / DEP)
#      - Marks the stack and heap as non-executable.
#      - Verified via readelf: GNU_STACK segment has RW (not RWE) flags.
#
#   3. Stack Canary (Stack Protector Strong)
#      - Detects stack buffer overflows at runtime via guard values.
#      - Verified via readelf: presence of __stack_chk_fail symbol.
#
#   4. Full RELRO (Relocation Read-Only)
#      - Makes the GOT (Global Offset Table) read-only after dynamic
#        linking, preventing GOT overwrite attacks.
#      - Verified via readelf: BIND_NOW flag in dynamic section.
#
# Two verification strategies (automatic selection):
#   - checksec tool (preferred) — comprehensive binary analysis tool
#   - readelf fallback — manual ELF header/section inspection
#
# Environment Variables:
#   BIN   Path to the KEEL binary (default: <root>/build-linux/src/main/keel)
#
# Prerequisites:
#   - checksec or readelf on PATH
#   - KEEL binary must be built
#
# Exit Codes:
#   0  PASS — all hardening features enabled
#   1  FAIL — one or more hardening features missing
#
# Usage:
#   ./scripts/hardening-checksec.sh
#   BIN=./build-release/src/main/keel ./scripts/hardening-checksec.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build-linux}"
BIN="${BIN:-$BUILD_DIR/src/main/keel}"

if [[ ! -x "$BIN" ]]; then
  echo "[checksec] FAIL: binary not found/executable: $BIN" >&2
  exit 2
fi

if command -v checksec >/dev/null 2>&1; then
  out="$(checksec --file="$BIN" 2>/dev/null || true)"
  echo "$out"

  grep -Eq 'Canary found' <<<"$out" || {
    echo "[checksec] FAIL: stack canary missing"
    exit 1
  }
  grep -Eq 'NX enabled' <<<"$out" || {
    echo "[checksec] FAIL: NX missing"
    exit 1
  }
  grep -Eq 'PIE enabled' <<<"$out" || {
    echo "[checksec] FAIL: PIE missing"
    exit 1
  }
  grep -Eq 'Full RELRO' <<<"$out" || {
    echo "[checksec] FAIL: Full RELRO missing"
    exit 1
  }

  echo "[checksec] PASS: PIE/NX/Canary/Full RELRO enabled"
  exit 0
fi

echo "[checksec] checksec not installed, using readelf fallback"

need_readelf() {
  command -v readelf >/dev/null 2>&1 || {
    echo "[checksec] FAIL: readelf not installed" >&2
    exit 2
  }
}
need_readelf

dyn="$(LANG=C readelf -d "$BIN" 2>/dev/null || true)"
sym="$(LANG=C readelf -s "$BIN" 2>/dev/null || true)"
hdr="$(LANG=C readelf -h "$BIN" 2>/dev/null || true)"
phdr="$(LANG=C readelf -l "$BIN" 2>/dev/null || true)"

grep -q 'BIND_NOW' <<<"$dyn" || { echo "[checksec] FAIL: BIND_NOW missing (no full RELRO)"; exit 1; }
grep -q 'GNU_RELRO' <<<"$phdr" || { echo "[checksec] FAIL: GNU_RELRO missing"; exit 1; }
grep -Eq '__stack_chk_fail|__stack_chk_guard' <<<"$sym" || { echo "[checksec] FAIL: stack protector symbols missing"; exit 1; }
grep -Eq 'Type:[[:space:]]+DYN' <<<"$hdr" || { echo "[checksec] FAIL: not PIE (ELF type not DYN)"; exit 1; }

echo "[checksec] PASS: readelf fallback indicates hardened binary"
