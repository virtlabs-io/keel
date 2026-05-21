#!/usr/bin/env bash
# =============================================================================
# check_auth_log_safety.sh — Release gate: no auth material in log calls
#
# Scans C source files for KEEL_LOG_* calls that could expose authentication
# material (nonces, proofs, raw SCRAM messages, passwords, tokens, keys, …)
# in log output.
#
# Detection strategy — two fast grep passes:
#
#   Pattern 1 (label=value leaks):
#     A KEEL_LOG_* call whose format string contains a sensitive label
#     immediately followed by  =%s  /  =%.Ns  /  =%.*s.
#     The `=` is required so that prose like "No password stored for user %s"
#     is NOT flagged (the username logged there is safe; only label=value
#     pairs that embed the secret itself are violations).
#     Examples caught:   "data=%.80s"   "auth_msg=%.100s"   "proof=%s"
#     Examples ignored:  "received_len=%zu"   "No password stored for user %s"
#
#   Pattern 2 (raw variable inside AUTH category log):
#     A KEEL_LOG_* call tagged KEEL_LOG_CAT_AUTH that passes a variable
#     named `msg` or `data` as a %s argument.  These names are the
#     conventional holders of raw SCRAM wire bytes in auth.c.
#
# Suppression:  annotate with  /* NOLINT(keel-authlog) */  on the same line
# when a site has been explicitly audited and confirmed safe.
#
# Excluded paths: src/mem/, src/arch/, src/cli/
#
# Usage (called by CTest):
#   check_auth_log_safety.sh [root_dir]
#
# Returns 0 (pass) or 1 (fail, with violation list printed).
# =============================================================================

set -euo pipefail

ROOT="${1:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)}"

violations=0
tmpout=$(mktemp)
trap 'rm -f "$tmpout"' EXIT

# ---------------------------------------------------------------------------
# Pattern 1 — label=<string-specifier> leaks in format strings.
#
# Requires keyword= (with literal equals) immediately before the format
# specifier so that prose descriptions (e.g. "No password stored") are not
# flagged.
# ---------------------------------------------------------------------------
P1='\bKEEL_LOG_(ERROR|WARN|INFO|DEBUG|TRACE)\b[^;]*"[^"]*\b(data|password|token|secret|proof|nonce|salt|client_key|server_key|stored_key|auth_msg|signature)=%(\.[0-9]+)?\*?s'

# ---------------------------------------------------------------------------
# Pattern 2 — bare sensitive variable passed to %s inside AUTH log calls.
#
# Scoped to KEEL_LOG_CAT_AUTH to avoid false positives from the rest of the
# codebase.  Catches the idiom:  KEEL_LOG_ERROR(..., "…%s", msg)
# ---------------------------------------------------------------------------
P2='\bKEEL_LOG_(ERROR|WARN|INFO|DEBUG|TRACE)\s*\([^;]*KEEL_LOG_CAT_AUTH[^;]*%(\.[0-9]+)?\*?s[^;]*,\s*(msg|data)\b'

# Directories excluded from scanning
excl_args=(
    --exclude-dir=mem
    --exclude-dir=arch
    --exclude-dir=cli
)

# Run both patterns over C source + headers; filter out NOLINT-annotated lines
for pattern in "$P1" "$P2"; do
    grep -rn --include="*.c" --include="*.h" \
        "${excl_args[@]}" \
        -E "$pattern" \
        "$ROOT/src" "$ROOT/tests" 2>/dev/null \
    | grep -v 'NOLINT(keel-authlog)' \
    >> "$tmpout" || true
done

# Count and display unique violations
if [[ -s "$tmpout" ]]; then
    sort -u "$tmpout"
    violations=$(sort -u "$tmpout" | wc -l)
fi

echo ""
if [[ $violations -eq 0 ]]; then
    echo "check_auth_log_safety: PASSED — no authentication material found in log calls"
    exit 0
else
    echo "check_auth_log_safety: FAILED — ${violations} violation(s) found"
    echo ""
    echo "  Fix: remove the secret field from the format string."
    echo "       Log only lengths/counts (e.g. received_len=%zu, step=%d)."
    echo "       If the site is intentional, annotate with:"
    echo "         /* NOLINT(keel-authlog) */"
    exit 1
fi
