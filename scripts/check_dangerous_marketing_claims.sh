#!/usr/bin/env bash
# check_dangerous_marketing_claims.sh - keep public docs conservative.

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

pattern='zero[[:space:]]+downtime|transparent[[:space:]]+failover|fully[[:space:]]+stateless|universal[[:space:]]+SQL[[:space:]]+compatibility|automatic[[:space:]]+correctness'

matches=$(grep -RInEi \
    --include='*.md' \
    --exclude-dir='.git' \
    --exclude-dir='build' \
    --exclude-dir='build-coverage' \
    --exclude-dir='build-debug' \
    --exclude-dir='build-otlp' \
    --exclude-dir='build-test' \
    --exclude-dir='coverage-html' \
    --exclude-dir='third_party' \
    --exclude-dir='node_modules' \
    -- "$pattern" "$root" || true)

if [[ -n "$matches" ]]; then
    cat >&2 <<'EOF'
FAIL: risky production claims found in Markdown docs.

Use precise language instead: correctness-first, conservative routing,
explicit state ownership, observable routing decisions, safe degradation,
and fail-closed semantics.
EOF
    printf '%s\n' "$matches" >&2
    exit 1
fi

echo "OK: Markdown docs avoid risky production claims."