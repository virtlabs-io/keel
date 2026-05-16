#!/usr/bin/env bash
# ============================================================================
# generate-test-certs.sh — Generate ephemeral PKI for development and testing
# ============================================================================
#
# Creates a self-signed CA and issues server + client certificates for local
# testing of KEEL's TLS and mTLS features.  All material is written to a
# single output directory (default: etc/certs/) and is fully deterministic
# given the same arguments.
#
# Generated files:
#   ca.crt / ca.key         — Root CA keypair
#   ca-bundle.pem           — CA bundle (alias of ca.crt)
#   frontend-server.crt/key — KEEL frontend listener certificate (localhost)
#   frontend-server-chain.crt — frontend-server.crt + ca.crt (full chain)
#   backend-server.crt/key  — Upstream backend TLS test certificate
#   backend-server-chain.crt — backend-server.crt + ca.crt (full chain)
#   client.crt/key          — Client mTLS certificate
#   client-chain.crt        — client.crt + ca.crt (full chain)
#   server.pem / server.key — Compatibility aliases → frontend-server.*
#   client.pem / client.key.pem — Compatibility aliases → client.*
#
# Environment Variables:
#   CERT_DIR     Output directory (default: <repo>/etc/certs)
#   CERT_DAYS    Certificate validity in days (default: 3650)
#   KEY_BITS     RSA key size (default: 2048)
#
# Exit Codes:
#   0  All certificates generated successfully
#   1  Missing openssl or generation failure
#
# Usage:
#   ./scripts/generate-test-certs.sh
#   CERT_DIR=/tmp/test-certs ./scripts/generate-test-certs.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="${CERT_DIR:-$ROOT_DIR/etc/certs}"
CERT_DAYS="${CERT_DAYS:-3650}"
KEY_BITS="${KEY_BITS:-2048}"

command -v openssl >/dev/null 2>&1 || {
    echo "[certs] ERROR: openssl not found on PATH" >&2
    exit 1
}

mkdir -p "$CERT_DIR"

echo "[certs] Generating test PKI in $CERT_DIR"
echo "[certs]   validity: ${CERT_DAYS} days, key size: ${KEY_BITS} bits"

# ---------------------------------------------------------------------------
# 1. Root CA
# ---------------------------------------------------------------------------
echo "[certs] Creating root CA..."
openssl req -x509 -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    -days "$CERT_DAYS" \
    -subj "/CN=KEEL Test CA/O=KEEL Development/C=US" \
    2>/dev/null

cp "$CERT_DIR/ca.crt" "$CERT_DIR/ca-bundle.pem"

# ---------------------------------------------------------------------------
# 2. Frontend server certificate (KEEL proxy listener)
# ---------------------------------------------------------------------------
echo "[certs] Creating frontend server certificate..."
cat > "$CERT_DIR/frontend-server.ext" <<EOF
subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
EOF

openssl req -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout "$CERT_DIR/frontend-server.key" \
    -out "$CERT_DIR/frontend-server.csr" \
    -subj "/CN=localhost/O=KEEL Development/C=US" \
    2>/dev/null

openssl x509 -req \
    -in "$CERT_DIR/frontend-server.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/frontend-server.crt" \
    -days "$CERT_DAYS" \
    -extfile "$CERT_DIR/frontend-server.ext" \
    2>/dev/null

cat "$CERT_DIR/frontend-server.crt" "$CERT_DIR/ca.crt" \
    > "$CERT_DIR/frontend-server-chain.crt"

# ---------------------------------------------------------------------------
# 3. Backend server certificate (upstream database TLS)
# ---------------------------------------------------------------------------
echo "[certs] Creating backend server certificate..."
cat > "$CERT_DIR/backend-server.ext" <<EOF
subjectAltName=DNS:backend.local,DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
EOF

openssl req -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout "$CERT_DIR/backend-server.key" \
    -out "$CERT_DIR/backend-server.csr" \
    -subj "/CN=backend.local/O=KEEL Development/C=US" \
    2>/dev/null

openssl x509 -req \
    -in "$CERT_DIR/backend-server.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/backend-server.crt" \
    -days "$CERT_DAYS" \
    -extfile "$CERT_DIR/backend-server.ext" \
    2>/dev/null

cat "$CERT_DIR/backend-server.crt" "$CERT_DIR/ca.crt" \
    > "$CERT_DIR/backend-server-chain.crt"

# ---------------------------------------------------------------------------
# 4. Client certificate (mTLS)
# ---------------------------------------------------------------------------
echo "[certs] Creating client certificate..."
cat > "$CERT_DIR/client.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF

openssl req -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout "$CERT_DIR/client.key" \
    -out "$CERT_DIR/client.csr" \
    -subj "/CN=keel-client/O=KEEL Development/C=US" \
    2>/dev/null

openssl x509 -req \
    -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/client.crt" \
    -days "$CERT_DAYS" \
    -extfile "$CERT_DIR/client.ext" \
    2>/dev/null

cat "$CERT_DIR/client.crt" "$CERT_DIR/ca.crt" \
    > "$CERT_DIR/client-chain.crt"

# ---------------------------------------------------------------------------
# 5. Compatibility aliases
# ---------------------------------------------------------------------------
echo "[certs] Creating compatibility aliases..."
cp "$CERT_DIR/frontend-server.crt" "$CERT_DIR/server.pem"
cp "$CERT_DIR/frontend-server.key" "$CERT_DIR/server.key"
cp "$CERT_DIR/client.crt"          "$CERT_DIR/client.pem"
cp "$CERT_DIR/client.key"          "$CERT_DIR/client.key.pem"

# ---------------------------------------------------------------------------
# Clean up CSR files (not needed at runtime)
# ---------------------------------------------------------------------------
rm -f "$CERT_DIR"/*.csr

echo "[certs] Done. Generated files:"
ls -1 "$CERT_DIR"/*.crt "$CERT_DIR"/*.key "$CERT_DIR"/*.pem 2>/dev/null | \
    sed "s|^$CERT_DIR/|  |"
