#!/usr/bin/env bash
# check_metrics_invariants.sh — Enforce v0.2-alpha observability invariants.
#
# Rationale
# ---------
# proposals/v0.2-alpha_observability.md §27/§28 require that the state
# machines feeding reason-coded metrics be mutated only through their
# owning module so that every transition is observable and accountable
# exactly once.  This script grep-enforces a small set of those rules
# that the current tree already satisfies.  New rules will be added as
# the corresponding refactors land (commit-state enum, pin-mask helper,
# centralized timing module, etc.).
#
# Rules enforced today
# --------------------
#   R1  backend_conn->close_reason may only be assigned in
#       src/worker/backend_pool.c.  Every other backend close path
#       must call backend_pool_close_connection() / the locked variant
#       so the reason is recorded and the per-reason counter is bumped.
#
#   R2  session_flow.commit_in_doubt / commit_in_flight (and the
#       session_t mirror commit_in_doubt) may only be assigned in
#       src/engine/engine_flow.c, src/engine/state_machine.c, and
#       src/session/session.c.  Tests (tests/) are also exempt because
#       they build synthetic states.  This keeps the commit-result
#       state machine — which feeds keel.transaction.commit.* metrics
#       — concentrated and auditable.
#
# Exceptions
# ----------
#   * Lines annotated with /* NOLINT(keel-metrics) */ are skipped.
#   * Pure comment lines are skipped.
#
# Usage
# -----
#   scripts/check_metrics_invariants.sh [--report-only] [root]
#
#   default        gate mode: exit non-zero on any violation
#   --report-only  list violations and exit 0 (for inventory)

set -euo pipefail

MODE="gate"
ROOT=""

for arg in "$@"; do
    case "$arg" in
        --report-only) MODE="report" ;;
        --help|-h)
            sed -n '2,40p' "$0"
            exit 0
            ;;
        *) ROOT="$arg" ;;
    esac
done

if [[ -z "$ROOT" ]]; then
    ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)"
fi

violations=0

# ---------------------------------------------------------------------------
# Rule scanner.
#
# Args: <rule-id> <human-description> <allowed-paths-regex> <forbidden-regex>
#
# allowed-paths-regex matches against the workspace-relative path; files
# matching it are skipped.  forbidden-regex matches against a single line
# of source.  Pure comment lines and lines containing NOLINT(keel-metrics)
# are always skipped.
# ---------------------------------------------------------------------------
scan_rule() {
    local rule_id="$1"
    local description="$2"
    local allowed_re="$3"
    local forbidden_re="$4"

    while IFS= read -r -d '' file; do
        local rel="${file#"$ROOT/"}"
        if [[ "$rel" =~ $allowed_re ]]; then
            continue
        fi

        awk -v rel="$rel" \
            -v rule="$rule_id" \
            -v desc="$description" \
            -v pat="$forbidden_re" '
            {
                line = $0
                s = line
                sub(/^[[:space:]]+/, "", s)
                if (s == "" || s ~ /^\/\// || s ~ /^\/\*/ || s ~ /^\*/) next
                if (index(line, "NOLINT(keel-metrics)") > 0) next

                # Strip string literals so log messages cannot trigger the rule.
                cleaned = line
                gsub(/"[^"]*"/, "\"\"", cleaned)

                if (match(cleaned, pat)) {
                    printf "%s\t%s:%d\t%s\n", rule, rel, NR, line
                }
            }
        ' "$file"
    done < <(find "$ROOT/src" "$ROOT/include" \
        \( -name "*.c" -o -name "*.h" \) \
        -not -path "*/.git/*" \
        -print0 2>/dev/null)
}

# Collect hits across all rules.
ALL_HITS=""

# Rule R1 — backend close_reason mutation must live in backend_pool.c only.
R1_HITS=$(scan_rule \
    "R1" \
    "backend_conn->close_reason assigned outside src/worker/backend_pool.c" \
    '^src/worker/backend_pool\.c$' \
    '(->|[.])close_reason[[:space:]]*=' \
    || true)
if [[ -n "$R1_HITS" ]]; then
    ALL_HITS+="$R1_HITS"$'\n'
fi

# Rule R2 — commit_in_doubt / commit_in_flight mutations live in the engine
# commit state machine (and tests build synthetic states freely).
R2_HITS=$(scan_rule \
    "R2" \
    "commit_in_doubt/commit_in_flight assigned outside engine commit state machine" \
    '^(src/engine/engine_flow\.c|src/engine/state_machine\.c|src/session/session\.c|tests/)' \
    '(->|[.])(commit_in_doubt|commit_in_flight)[[:space:]]*=' \
    || true)
if [[ -n "$R2_HITS" ]]; then
    ALL_HITS+="$R2_HITS"$'\n'
fi

# Count and emit.
if [[ -n "$ALL_HITS" ]]; then
    # Strip trailing blank line.
    ALL_HITS="${ALL_HITS%$'\n'}"
    while IFS= read -r _; do
        violations=$((violations + 1))
    done <<< "$ALL_HITS"
fi

case "$MODE" in
    report)
        printf 'Metrics invariants inventory:\n\n'
        if (( violations == 0 )); then
            printf '  (none)\n'
        else
            printf '%s\n' "$ALL_HITS" \
                | awk -F'\t' '{ printf "  [%s] %s\n      %s\n", $1, $2, $3 }'
        fi
        printf '\nTotal: %d violation(s).\n' "$violations"
        exit 0
        ;;
    gate)
        if (( violations > 0 )); then
            printf 'ERROR: %d metrics-invariant violation(s) found.\n\n' "$violations"
            printf '%s\n' "$ALL_HITS" \
                | awk -F'\t' '{ printf "  [%s] %s\n      %s\n\n", $1, $2, $3 }'
            printf 'Resolve by either:\n'
            printf '  1. Routing the mutation through the owning module\n'
            printf '     (backend_pool.c for backend close, the engine commit\n'
            printf '     state machine for commit_in_doubt/commit_in_flight).\n'
            printf '  2. Annotating the line with /* NOLINT(keel-metrics) */\n'
            printf '     and a comment explaining why the exception is safe.\n'
            printf '\n'
            printf 'See proposals/v0.2-alpha_observability.md §27/§28.\n'
            exit 1
        fi
        printf 'OK: metrics invariants satisfied.\n'
        exit 0
        ;;
esac
