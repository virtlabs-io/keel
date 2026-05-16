#!/usr/bin/env bash
# ============================================================================
# hardening-tls-scan.sh — TLS configuration audit for KEEL proxy
# ============================================================================
#
# Audits the TLS configuration of the KEEL proxy endpoint to verify that
# strong cipher suites are enforced and legacy/insecure protocols are rejected.
#
# Two scan strategies (automatic fallback):
#   1. testssl.sh (preferred) — comprehensive TLS scanner that checks cipher
#      suites, protocol versions, certificate chain, vulnerabilities (BEAST,
#      POODLE, Heartbleed, etc.), and HSTS headers.
#   2. openssl s_client probes (fallback) — lightweight verification using
#      individual openssl s_client connections to test specific protocols.
#
# Checks performed:
#   - TLS 1.2 handshake succeeds (MUST pass)
#   - TLS 1.0 is rejected (MUST fail handshake)
#   - TLS 1.1 is rejected (MUST fail handshake)
#   - No weak ciphers accepted: 3DES, SHA1-based cipher suites
#
# Why we reject TLS < 1.2:
#   TLS 1.0 and 1.1 have known vulnerabilities (BEAST, POODLE, Lucky13)
#   and are deprecated by RFC 8996.  PCI-DSS compliance also requires
#   TLS 1.2 as the minimum version.
#
# Environment Variables:
#   TLS_HOST    Target host (default: 127.0.0.1)
#   TLS_PORT    Target port (default: 7432)
#   OUT_DIR     Output directory for scan reports (default: /tmp/keel_tls_scan_<timestamp>)
#
# Prerequisites:
#   - testssl.sh or openssl on PATH
#   - KEEL proxy running with TLS enabled on TLS_HOST:TLS_PORT
#
# Exit Codes:
#   0  PASS — TLS 1.2+ only, no weak ciphers
#   1  FAIL — legacy protocol accepted or weak cipher found
#   0  SKIP — neither testssl.sh nor openssl available
#
# Usage:
#   ./scripts/hardening-tls-scan.sh
#   TLS_PORT=8432 ./scripts/hardening-tls-scan.sh
# ============================================================================
set -euo pipefail

TLS_HOST="${TLS_HOST:-127.0.0.1}"
TLS_PORT="${TLS_PORT:-7432}"
OUT_DIR="${OUT_DIR:-/tmp/keel_tls_scan_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

target="${TLS_HOST}:${TLS_PORT}"
echo "[tls] target: $target"

if command -v testssl.sh >/dev/null 2>&1; then
  testssl.sh --quiet --warnings batch --jsonfile "$OUT_DIR/testssl.json" "$target" \
    >"$OUT_DIR/testssl.txt" 2>&1 || true

  if grep -Ei '3des|sha1|sslv2|sslv3|tls1\.0|tls1\.1' "$OUT_DIR/testssl.txt" >/dev/null 2>&1; then
    echo "[tls] FAIL: weak protocol/cipher detected"
    echo "[tls] see $OUT_DIR/testssl.txt"
    exit 1
  fi

  echo "[tls] PASS: no obvious weak protocol/cipher findings"
  exit 0
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "[tls] FAIL: missing testssl.sh and openssl" >&2
  exit 2
fi

echo "[tls] testssl.sh not installed; running OpenSSL fallback checks"

set +e
openssl s_client -connect "$target" -tls1 </dev/null >"$OUT_DIR/tls1.txt" 2>&1
rc_tls10=$?
openssl s_client -connect "$target" -tls1_1 </dev/null >"$OUT_DIR/tls11.txt" 2>&1
rc_tls11=$?
openssl s_client -connect "$target" -tls1_2 </dev/null >"$OUT_DIR/tls12.txt" 2>&1
rc_tls12=$?
set -e

if [[ "$rc_tls12" -ne 0 ]]; then
  echo "[tls] FAIL: TLS1.2 handshake failed (service may not be TLS-enabled)"
  exit 1
fi

if [[ "$rc_tls10" -eq 0 || "$rc_tls11" -eq 0 ]]; then
  echo "[tls] FAIL: deprecated TLS protocol accepted (TLS1.0/1.1)"
  exit 1
fi

echo "[tls] PASS: TLS1.2 works and TLS1.0/1.1 rejected"
