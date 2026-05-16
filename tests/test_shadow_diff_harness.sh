#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <repo_root>" >&2
  exit 2
fi

ROOT_DIR="$1"
SCRIPT="$ROOT_DIR/tests/hardening/hardening-shadow-diff.sh"

if [[ ! -f "$SCRIPT" ]]; then
  echo "[harness-shadow] FAIL: missing script: $SCRIPT" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# 1) Meta-command lines (\\...) must be rejected before any DB connection attempt.
cat > "$tmpdir/meta.sql" <<'SQL'
\\set aid random(1, 100)
SELECT 1;
SQL

set +e
out1="$(SQL_FILE="$tmpdir/meta.sql" "$SCRIPT" 2>&1)"
rc1=$?
set -e

if [[ $rc1 -ne 2 ]]; then
  echo "[harness-shadow] FAIL: expected exit=2 for meta-command SQL, got rc=$rc1" >&2
  echo "$out1" >&2
  exit 1
fi

if ! printf '%s\n' "$out1" | grep -F "meta-commands" >/dev/null; then
  echo "[harness-shadow] FAIL: expected meta-command rejection message" >&2
  echo "$out1" >&2
  exit 1
fi

# 2) pgbench variable placeholders (:var) must also be rejected early.
cat > "$tmpdir/vars.sql" <<'SQL'
SELECT :aid;
SQL

set +e
out2="$(SQL_FILE="$tmpdir/vars.sql" "$SCRIPT" 2>&1)"
rc2=$?
set -e

if [[ $rc2 -ne 2 ]]; then
  echo "[harness-shadow] FAIL: expected exit=2 for variable placeholder SQL, got rc=$rc2" >&2
  echo "$out2" >&2
  exit 1
fi

if ! printf '%s\n' "$out2" | grep -F "variable placeholders" >/dev/null; then
  echo "[harness-shadow] FAIL: expected variable-placeholder rejection message" >&2
  echo "$out2" >&2
  exit 1
fi

echo "[harness-shadow] PASS"
