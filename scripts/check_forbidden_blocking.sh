#!/usr/bin/env bash
# check_forbidden_blocking.sh — Enforce reactor-only invariant on hot-path files
#
# Rationale
# ---------
# Keel is a reactor-driven proxy: worker threads must never block on a single
# fd because doing so stalls every other session multiplexed onto the same
# reactor.  The hot-path files listed in HOT_PATH_FILES below are documented
# as async/io_uring-driven; any blocking syscall in those files is a latency
# bug waiting to happen.
#
# Forbidden calls on a hot-path source line
# -----------------------------------------
#   recv, send, read, write,
#   recvfrom, sendto, recvmsg, sendmsg, readv, writev
#       → allowed if the same line also contains MSG_DONTWAIT
#         (the kernel's explicit nonblocking flag)
#
#   select, pselect, poll, ppoll, epoll_wait,
#   sleep, usleep, nanosleep, clock_nanosleep,
#   keel_fd_wait
#       → always forbidden (no flag form exists)
#
#   fcntl(F_SETFL, flags & ~O_NONBLOCK)
#       → forbidden in hot paths because subsequent I/O can block the reactor
#
# Exceptions
# ----------
#   1. Lines annotated with /* NOLINT(keel-blocking) */ are tolerated.
#      Use this for legitimate cases such as one-shot writes to an
#      internal eventfd, where the call is provably nonblocking from
#      context that the linter cannot see.
#
#   2. Lines listed in scripts/forbidden_blocking_baseline.txt are
#      grandfathered violations from before the gate was introduced.
#      The baseline is meant to shrink over time; refactors that
#      replace blocking I/O with reactor-driven state machines should
#      remove the corresponding line from the baseline.
#
#   3. Pure comment lines (// ... or /* ...) are skipped.
#
# Usage
# -----
#   scripts/check_forbidden_blocking.sh [--report-only|--regen-baseline] [root]
#
#   default          gate mode: exit non-zero on un-baselined violations
#   --report-only    print all violations and exit 0 (for inventory)
#   --regen-baseline rewrite scripts/forbidden_blocking_baseline.txt
#                    from the current tree (use sparingly)

set -euo pipefail

MODE="gate"
ROOT=""

for arg in "$@"; do
    case "$arg" in
        --report-only)    MODE="report" ;;
        --regen-baseline) MODE="regen"  ;;
        --help|-h)
            sed -n '2,46p' "$0"
            exit 0
            ;;
        *) ROOT="$arg" ;;
    esac
done

if [[ -z "$ROOT" ]]; then
    ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)"
fi

BASELINE_FILE="$ROOT/scripts/forbidden_blocking_baseline.txt"

# Hot-path files — must be reactor-driven, no blocking I/O.
HOT_PATH_FILES=(
    "src/worker/worker.c"
    "src/worker/backend_pool.c"
    "src/worker/backend_connect_async.c"
    "src/worker/frontend_tls_async.c"
    "src/worker/migration.c"
    "src/engine/engine.c"
    "src/engine/engine_flow.c"
    "src/engine/engine_scatter.c"
    "src/engine/state_machine.c"
)

# Single-pass awk scanner: emits one line per violation in the form
#   <relpath>:<lineno>:<callname>\t<source line>
# Comments and NOLINT-annotated lines are skipped.  recv/send-family calls
# that contain MSG_DONTWAIT on the same line are skipped.
scan_one() {
    local relpath="$1"
    local file="$ROOT/$relpath"
    [[ -f "$file" ]] || return 0

    awk -v rel="$relpath" '
        BEGIN {
            io_re      = "\\<(recv|send|read|write|recvfrom|sendto|recvmsg|sendmsg|readv|writev)[[:space:]]*\\("
            always_re  = "\\<(select|pselect|poll|ppoll|epoll_wait|sleep|usleep|nanosleep|clock_nanosleep|keel_fd_wait)[[:space:]]*\\("
            clear_nb_re = "F_SETFL.*~[[:space:]]*O_NONBLOCK|~[[:space:]]*O_NONBLOCK.*F_SETFL"
            pend_call  = ""
            pend_line  = 0
            pend_text  = ""
        }

        function emit_pending() {
            if (pend_call != "") {
                printf "%s:%d:%s\t%s\n", rel, pend_line, pend_call, pend_text
                pend_call = ""
                pend_line = 0
                pend_text = ""
            }
        }

        {
            line = $0

            # Strip leading whitespace for comment detection.
            s = line
            sub(/^[[:space:]]+/, "", s)
            if (s == "" || s ~ /^\/\// || s ~ /^\/\*/ || s ~ /^\*/) next

            # Strip string literals so log messages cannot accidentally
            # whitelist (via embedded "MSG_DONTWAIT") or trigger (via
            # embedded "send(...)") the linter.
            cleaned = line
            gsub(/"[^"]*"/, "\"\"", cleaned)

            # Per-line NOLINT escape hatch: clears any pending I/O call too
            # (the annotation covers the whole statement).
            if (index(line, "NOLINT(keel-blocking)") > 0) {
                pend_call = ""
                next
            }

            # If a previous line opened an I/O call that has not yet been
            # resolved, look for MSG_DONTWAIT or the statement terminator.
            if (pend_call != "") {
                if (index(cleaned, "MSG_DONTWAIT") > 0) {
                    pend_call = ""
                } else if (index(cleaned, ";") > 0) {
                    emit_pending()
                }
                # Otherwise the statement is still in progress; keep waiting.
            }

            # First I/O call on this line (only when nothing is pending,
            # to avoid double-counting on continuation lines).
            if (pend_call == "" && match(cleaned, io_re)) {
                tok = substr(cleaned, RSTART, RLENGTH)
                sub(/[[:space:]]*\($/, "", tok)
                if (index(cleaned, "MSG_DONTWAIT") > 0) {
                    # Nonblocking — accept.
                } else if (index(cleaned, ";") > 0) {
                    printf "%s:%d:%s\t%s\n", rel, NR, tok, line
                } else {
                    pend_call = tok
                    pend_line = NR
                    pend_text = line
                }
            }

            # Always-forbidden calls (no flag form exists, so no need to
            # track across lines for whitelisting).
            if (match(cleaned, always_re)) {
                tok2 = substr(cleaned, RSTART, RLENGTH)
                sub(/[[:space:]]*\($/, "", tok2)
                printf "%s:%d:%s\t%s\n", rel, NR, tok2, line
            }

            # Clearing O_NONBLOCK makes later send/recv calls potentially
            # blocking even if those calls are elsewhere.
            if (match(cleaned, clear_nb_re)) {
                printf "%s:%d:%s\t%s\n", rel, NR, "clear_nonblock", line
            }
        }

        END {
            emit_pending()
        }
    ' "$file"
}

# Collect all hits in one shot.
ALL_HITS_RAW=""
for f in "${HOT_PATH_FILES[@]}"; do
    out=$(scan_one "$f") || true
    if [[ -n "$out" ]]; then
        ALL_HITS_RAW+="$out"$'\n'
    fi
done

# Load baseline into an associative array.
declare -A BASELINE=()
if [[ -f "$BASELINE_FILE" ]]; then
    while IFS= read -r bline; do
        bline="${bline%%#*}"
        bline="${bline#"${bline%%[![:space:]]*}"}"
        bline="${bline%"${bline##*[![:space:]]}"}"
        [[ -z "$bline" ]] && continue
        BASELINE["$bline"]=1
    done < "$BASELINE_FILE"
fi

declare -a ALL_SIGS=()
declare -a NEW_HITS=()
new_count=0
total_count=0

if [[ -n "$ALL_HITS_RAW" ]]; then
    while IFS= read -r raw; do
        [[ -z "$raw" ]] && continue
        sig="${raw%%$'\t'*}"
        ALL_SIGS+=("$sig")
        total_count=$((total_count + 1))
        if [[ -z "${BASELINE[$sig]:-}" ]]; then
            new_count=$((new_count + 1))
            NEW_HITS+=("$raw")
        fi
    done <<< "$ALL_HITS_RAW"
fi

# Sort signatures by file, then numeric line number, then call name —
# emitted as a helper so report and regen modes share formatting.
sort_sigs() {
    if (( ${#ALL_SIGS[@]} == 0 )); then return 0; fi
    printf '%s\n' "${ALL_SIGS[@]}" \
        | awk -F: '{ printf "%s:%010d:%s\n", $1, $2, $3 }' \
        | sort \
        | awk -F: '{ sub(/^0+/, "", $2); if ($2 == "") $2 = "0"; printf "%s:%s:%s\n", $1, $2, $3 }'
}

case "$MODE" in
    regen)
        {
            printf '# scripts/forbidden_blocking_baseline.txt\n'
            printf '#\n'
            printf '# Auto-generated by scripts/check_forbidden_blocking.sh --regen-baseline.\n'
            printf '# One signature per line: <relpath>:<lineno>:<callname>\n'
            printf '#\n'
            printf '# Each entry is a grandfathered blocking call in a hot-path file.\n'
            printf '# The list is expected to shrink over time as the reactor-driven\n'
            printf '# refactors land.  When a blocking call is removed (or annotated\n'
            printf '# with NOLINT(keel-blocking) for a verified-safe case), drop the\n'
            printf '# matching line from this file.\n'
            printf '#\n'
            sort_sigs
        } > "$BASELINE_FILE"
        printf 'Regenerated baseline with %d entries: %s\n' \
            "$total_count" "$BASELINE_FILE"
        exit 0
        ;;
    report)
        printf 'Reactor-blocking inventory across %d hot-path file(s):\n\n' \
            "${#HOT_PATH_FILES[@]}"
        if (( total_count == 0 )); then
            printf '  (none)\n'
        else
            sort_sigs | sed 's/^/  /'
        fi
        printf '\nTotal: %d call site(s).\n' "$total_count"
        exit 0
        ;;
    gate)
        if (( new_count > 0 )); then
            printf 'ERROR: %d new forbidden blocking call(s) in hot-path files:\n\n' \
                "$new_count"
            for h in "${NEW_HITS[@]}"; do
                printf '  %s\n' "$h"
            done
            printf '\n'
            printf 'Hot-path files (listed in scripts/check_forbidden_blocking.sh)\n'
            printf 'must be fully reactor-driven.  Resolve by either:\n'
            printf '\n'
            printf '  1. Replacing the blocking call with an io_uring/reactor\n'
            printf '     state-machine step (preferred).\n'
            printf '  2. Adding MSG_DONTWAIT to a recv/send/recvmsg/sendmsg call\n'
            printf '     whose fd is guaranteed nonblocking.\n'
            printf '  3. Annotating the line with /* NOLINT(keel-blocking) */\n'
            printf '     and a comment explaining why the call cannot block.\n'
            printf '\n'
            printf 'The baseline file (scripts/forbidden_blocking_baseline.txt)\n'
            printf 'must NOT be extended for new code.  It exists only to\n'
            printf 'grandfather pre-existing violations during the v0.2 refactor.\n'
            exit 1
        fi
        printf 'OK: %d baselined blocking call(s); no new violations.\n' \
            "$total_count"
        exit 0
        ;;
esac
