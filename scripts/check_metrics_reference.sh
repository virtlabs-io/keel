#!/usr/bin/env bash
# check_metrics_reference.sh — Enforce parity between every metric
# registered in KEEL source code and the user-facing reference
# (proposals/v0.2-alpha observability §26).
#
# Rule
# ----
# The union of metrics registered in:
#   - src/observability/otlp/keel_prom_format.c   (k_meta[])
#   - src/admin/admin.c                           (PROM_COUNTER /
#                                                  PROM_GAUGE macros and
#                                                  raw `# HELP` lines)
# must appear, one name per `code-fenced` token, in
#   docs/METRICS_REFERENCE.md
# The reverse must also hold: every backtick-quoted `keel_xxx` /
# `proxy_xxx` token in the doc must correspond to a registered metric
# (with an allow-list for the §7 roadmap section).
#
# Usage
# -----
#   scripts/check_metrics_reference.sh [root]

set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
extractor="$root/scripts/extract_metrics_metadata.py"
doc="$root/docs/METRICS_REFERENCE.md"

if [[ ! -x "$extractor" ]]; then
    echo "FAIL: extractor not executable: $extractor" >&2
    exit 2
fi
if [[ ! -f "$doc" ]]; then
    echo "FAIL: reference doc not found: $doc" >&2
    exit 2
fi

mapfile -t code_names < <("$extractor" --names | sort -u)

mapfile -t doc_names < <(
    grep -oE '`(keel|proxy)_[A-Za-z0-9_]+`' "$doc" \
        | tr -d '`' \
        | sort -u
)

# Allow-list of doc-only names — names that intentionally appear without
# being registered yet (e.g. roadmap families in §7).
mapfile -t allowlist < <(awk '
    /^## 7\. Metrics planned for later tiers/ { in7 = 1; next }
    /^## / && in7                              { in7 = 0 }
    in7 {
        while (match($0, /`(keel|proxy)[._][A-Za-z0-9_.]+`/)) {
            tok = substr($0, RSTART + 1, RLENGTH - 2)
            print tok
            $0 = substr($0, RSTART + RLENGTH)
        }
    }
' "$doc" | sort -u)

is_allowlisted() {
    local name="$1"
    for a in "${allowlist[@]}"; do
        [[ "$a" == "$name" ]] && return 0
        if [[ "${a//./_}" == "$name" ]]; then return 0; fi
    done
    return 1
}

missing_in_doc=()
missing_in_code=()
if (( ${#code_names[@]} > 0 || ${#doc_names[@]} > 0 )); then
    # Use comm against sorted, unique files instead of nested grep loops:
    # the per-iteration `printf | grep` form is O(N*M) and forks ~N*M
    # processes which races under parallel ctest -j load.
    code_file=$(mktemp); doc_file=$(mktemp)
    trap 'rm -f "$code_file" "$doc_file"' EXIT
    printf '%s\n' "${code_names[@]}" | LC_ALL=C sort -u > "$code_file"
    printf '%s\n' "${doc_names[@]}"  | LC_ALL=C sort -u > "$doc_file"
    mapfile -t missing_in_doc < <(LC_ALL=C comm -23 "$code_file" "$doc_file")
    mapfile -t doc_only       < <(LC_ALL=C comm -13 "$code_file" "$doc_file")
    for n in "${doc_only[@]}"; do
        is_allowlisted "$n" || missing_in_code+=("$n")
    done
fi

status=0
if (( ${#missing_in_doc[@]} > 0 )); then
    echo "FAIL: metrics registered in source but missing from docs/METRICS_REFERENCE.md:" >&2
    printf '  - %s\n' "${missing_in_doc[@]}" >&2
    status=1
fi
if (( ${#missing_in_code[@]} > 0 )); then
    echo "FAIL: metrics documented in docs/METRICS_REFERENCE.md (outside §7 roadmap) but not registered in source:" >&2
    printf '  - %s\n' "${missing_in_code[@]}" >&2
    status=1
fi

if (( status == 0 )); then
    echo "OK: ${#code_names[@]} metrics in sync between source registries and reference doc."
fi
exit "$status"
