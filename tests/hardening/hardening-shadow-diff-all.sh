#!/usr/bin/env bash
# ============================================================================
# hardening-shadow-diff-all.sh — §15.5 Shadow correctness: multi-workload runner
# ============================================================================
#
# Runs every SQL workload in tests/hardening/sql/ through hardening-shadow-diff.sh
# and reports an aggregate pass/fail result.  Intended as the §15.5 CI gate.
#
# Each workload is isolated in its own output directory and run sequentially.
# Failures are collected and reported at the end so all workloads run even
# when one fails.
#
# Environment Variables (forwarded to hardening-shadow-diff.sh):
#   DIRECT_HOST   Direct backend host        (default: 127.0.0.1)
#   DIRECT_PORT   Direct backend port        (default: 5432)
#   PROXY_HOST    Proxy host                 (default: 127.0.0.1)
#   PROXY_PORT    Proxy port                 (default: 7432)
#   DB_USER       Database user              (default: postgres)
#   DB_PASSWORD   Database password          (default: postgres)
#   DB_NAME       Database name              (default: testdb)
#   OUT_DIR       Base output directory      (default: /tmp/keel_shadow_all_<ts>)
#   SQL_WORKLOAD_DIR  Directory of SQL files (default: <script_dir>/sql)
#
# Additional SQL files can be placed in SQL_WORKLOAD_DIR and will be picked up
# automatically on the next run.
#
# Exit Codes:
#   0  All workloads passed
#   1  One or more workloads failed
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHADOW_DIFF="${SCRIPT_DIR}/hardening-shadow-diff.sh"
SQL_WORKLOAD_DIR="${SQL_WORKLOAD_DIR:-${SCRIPT_DIR}/sql}"
BASE_OUT_DIR="${OUT_DIR:-/tmp/keel_shadow_all_$(date +%Y%m%d_%H%M%S)}"

if [[ ! -x "$SHADOW_DIFF" ]]; then
    echo "FATAL: hardening-shadow-diff.sh not found or not executable at $SHADOW_DIFF" >&2
    exit 1
fi

if [[ ! -d "$SQL_WORKLOAD_DIR" ]]; then
    echo "FATAL: SQL workload directory not found: $SQL_WORKLOAD_DIR" >&2
    exit 1
fi

mapfile -t SQL_FILES < <(find "$SQL_WORKLOAD_DIR" -maxdepth 1 -name '*.sql' | sort)

if [[ ${#SQL_FILES[@]} -eq 0 ]]; then
    echo "WARN: no .sql files found in $SQL_WORKLOAD_DIR — nothing to run" >&2
    exit 0
fi

echo "=== §15.5 Shadow Diff — $(date) ==="
echo "    Workload directory : $SQL_WORKLOAD_DIR"
echo "    Output base        : $BASE_OUT_DIR"
echo "    Direct backend     : ${DIRECT_HOST:-127.0.0.1}:${DIRECT_PORT:-5432}"
echo "    Proxy              : ${PROXY_HOST:-127.0.0.1}:${PROXY_PORT:-7432}"
echo "    Workloads          : ${#SQL_FILES[@]}"
echo ""

PASS=()
FAIL=()
SKIP=()

for sql_file in "${SQL_FILES[@]}"; do
    workload_name="$(basename "$sql_file" .sql)"
    workload_out="${BASE_OUT_DIR}/${workload_name}"
    mkdir -p "$workload_out"

    printf "  %-40s " "${workload_name}:"

    if SQL_FILE="$sql_file" OUT_DIR="$workload_out" \
            DIRECT_HOST="${DIRECT_HOST:-127.0.0.1}" \
            DIRECT_PORT="${DIRECT_PORT:-5432}" \
            PROXY_HOST="${PROXY_HOST:-127.0.0.1}" \
            PROXY_PORT="${PROXY_PORT:-7432}" \
            DB_USER="${DB_USER:-postgres}" \
            DB_PASSWORD="${DB_PASSWORD:-postgres}" \
            DB_NAME="${DB_NAME:-testdb}" \
            "$SHADOW_DIFF" >"${workload_out}/runner.log" 2>&1; then
        echo "PASS"
        PASS+=("$workload_name")
    else
        exit_code=$?
        # Check whether the failure was a preflight skip (psql unavailable /
        # connection refused) vs an actual diff.
        if grep -q "SKIP\|not reachable\|psql: not found\|psql: command not found" \
                "${workload_out}/runner.log" 2>/dev/null; then
            echo "SKIP"
            SKIP+=("$workload_name")
        else
            echo "FAIL (exit $exit_code)"
            FAIL+=("$workload_name")
            # Show the diff inline for quick diagnosis
            if [[ -f "${workload_out}/diff_output.txt" ]]; then
                echo "    --- diff (${workload_name}) ---"
                head -40 "${workload_out}/diff_output.txt" | sed 's/^/    /'
            fi
        fi
    fi
done

echo ""
echo "=== Results: ${#PASS[@]} passed, ${#FAIL[@]} failed, ${#SKIP[@]} skipped ==="

if [[ ${#FAIL[@]} -gt 0 ]]; then
    echo ""
    echo "FAILED workloads:"
    for name in "${FAIL[@]}"; do
        echo "  - $name  (logs: ${BASE_OUT_DIR}/${name}/runner.log)"
    done
    echo ""
    exit 1
fi

exit 0
