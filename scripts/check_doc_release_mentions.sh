#!/usr/bin/env bash
# check_doc_release_mentions.sh - keep main docs free of KEEL release labels.

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

paths=(
    "$root/README.md"
    "$root/SECURITY.md"
    "$root/CONTRIBUTING.md"
    "$root/CODE_OF_CONDUCT.md"
    "$root/docs"
    "$root/docker"
    "$root/bench"
    "$root/examples"
    "$root/monitoring"
    "$root/man"
    "$root/tests/chaos"
)

existing=()
for path in "${paths[@]}"; do
    [[ -e "$path" ]] && existing+=("$path")
done

pattern='\b(v0\.[0-9]+(\.[0-9]+)?(-alpha|-beta|-rc[0-9]*)?|alpha-[0-9]+\.[0-9]+(\.[0-9]+)?)\b'

matches=$(grep -RInE \
    --include='*.md' \
    --include='*.rst' \
    --include='*.txt' \
    --include='*.adoc' \
    --include='*.1' \
    --include='*.5' \
    --include='*.7' \
    --exclude='CHANGELOG.md' \
    --exclude-dir='.git' \
    --exclude-dir='build' \
    --exclude-dir='build-coverage' \
    --exclude-dir='build-debug' \
    --exclude-dir='build-otlp' \
    --exclude-dir='build-test' \
    --exclude-dir='coverage-html' \
    --exclude-dir='third_party' \
    --exclude-dir='node_modules' \
    -- "$pattern" "${existing[@]}" || true)

if [[ -n "$matches" ]]; then
    cat >&2 <<'EOF'
FAIL: KEEL release labels found in project documentation.

Keep release numbers and branch-specific version notes in changelog or release
note documents. Main documentation should describe current behavior, support
rules, and technical protocol/schema versions without KEEL release labels.
EOF
    printf '%s\n' "$matches" >&2
    exit 1
fi

echo "OK: project documentation avoids KEEL release labels outside release notes."