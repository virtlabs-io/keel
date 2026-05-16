#!/usr/bin/env bash
# check_forbidden_syscalls.sh — Enforce keel memory policy
#
# Scans C source and header files for direct system allocator calls that are
# forbidden by keel's memory management policy.  All dynamic allocation must
# go through the keel memory layer (keel_malloc, keel_calloc, keel_realloc,
# keel_free, keel_strdup, keel_strndup, keel_memdup).
#
# Exceptions:
#   1. src/mem/         — the allocator implementation itself wraps system calls.
#   2. src/arch/        — low-level OS abstractions may wrap system APIs.
#   3. include/keel/mem/— memory subsystem headers (KEEL_SAFE_* macros etc.)
#   4. Lines annotated with NOLINT(keel-syscall) — mandatory system API
#      boundaries (e.g. PAM conversation function) where the external library
#      owns and frees the memory with system free().
#   5. Comment-only lines (// ...  or  /* ... ).
#
# Usage:
#   scripts/check_forbidden_syscalls.sh [root_dir]
#
# Returns 0 if no violations found, 1 otherwise.

set -euo pipefail

ROOT="${1:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)}"

# Forbidden call pattern (word-boundary anchored bare identifier + open-paren)
FORBIDDEN_RE='\b(malloc|calloc|realloc|free|strdup|strndup)\s*\('

# Paths whose files are unconditionally allowed to use system allocators
ALLOWED_PATHS=(
    "src/mem/"
    "src/arch/"
    "include/keel/mem/"
    "src/cli/"
)

violations=0

while IFS= read -r -d '' file; do
    # Skip files in allowed paths
    skip=0
    for allowed in "${ALLOWED_PATHS[@]}"; do
        if [[ "$file" == *"$allowed"* ]]; then
            skip=1
            break
        fi
    done
    [[ $skip -eq 1 ]] && continue

    rel="${file#"$ROOT/"}"
    lineno=0

    while IFS= read -r line; do
        lineno=$((lineno + 1))

        # Skip pure comment lines
        stripped="${line#"${line%%[![:space:]]*}"}"
        case "$stripped" in
            "//"*|"/*"*|"*"*) continue ;;
        esac

        # Skip lines explicitly exempted
        [[ "$line" == *"NOLINT(keel-syscall)"* ]] && continue

        # Fast check: does the line contain any of the forbidden substrings?
        case "$line" in
            *malloc*|*calloc*|*realloc*|*strdup*|*strndup*|*" free("*|*"	free("*|*"(free("*|*";free("*) ;;
            *) continue ;;
        esac

        # Strip known keel_ and third-party prefixed identifiers so they do not
        # shadow the bare pattern check.
        cleaned=$(printf '%s' "$line" \
            | sed 's/keel_[a-zA-Z_]*[[:space:]]*([^)]*/KEEL_CALL/g' \
            | sed 's/\(EVP\|BIO\|X509\|ASN1\|HMAC\|EC\|RSA\|DH\|ERR\|OCSP\|ENGINE\)_[a-zA-Z_]*[[:space:]]*(/THIRD_PARTY_CALL(/g' \
            | sed 's/ldap_[a-zA-Z_]*[[:space:]]*(/THIRD_PARTY_CALL(/g' \
            | sed 's/pgbuf_[a-zA-Z_]*[[:space:]]*(/THIRD_PARTY_CALL(/g' \
            | sed 's/freeaddrinfo[[:space:]]*(/THIRD_PARTY_CALL(/g' \
            | sed 's/lua_[a-zA-Z_]*[[:space:]]*(/THIRD_PARTY_CALL(/g' \
            | sed 's/ZSTD_[a-zA-Z_]*[[:space:]]*(/THIRD_PARTY_CALL(/g')

        # Now check if any forbidden bare call remains
        if printf '%s' "$cleaned" | grep -qP "$FORBIDDEN_RE" 2>/dev/null; then
            echo "$rel:$lineno: forbidden system allocator call:"
            echo "  $line"
            violations=$((violations + 1))
        fi
    done < "$file"
done < <(find "$ROOT/src" "$ROOT/include" \
    \( -name "*.c" -o -name "*.h" \) \
    -not -path "*/.git/*" \
    -print0 2>/dev/null)

if [[ $violations -gt 0 ]]; then
    printf '\nERROR: %d forbidden system allocator call(s) found.\n\n' "$violations"
    printf 'All dynamic allocation must use the keel memory layer:\n'
    printf '  keel_malloc, keel_calloc, keel_realloc, keel_free,\n'
    printf '  keel_strdup, keel_strndup, keel_memdup\n\n'
    printf 'If a system API boundary requires the system allocator (e.g. PAM),\n'
    printf 'annotate the line with:  /* NOLINT(keel-syscall) */\n'
    exit 1
fi

printf 'OK: No forbidden system allocator calls found.\n'
exit 0

set -euo pipefail

ROOT="${1:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)}"

# Forbidden patterns: bare system allocator calls (word-boundary anchored)
# We match the call form: identifier(  — this avoids matching keel_malloc etc.
# The negative look-ahead for keel_ prefixes is handled by excluding lines
# that already have keel_ before the keyword.
FORBIDDEN_PATTERN='\b(malloc|calloc|realloc|free|strdup|strndup)\s*\('

# Directories that are explicitly allowed to use system allocators
ALLOWED_DIRS=(
    "src/mem/"
    "src/arch/"
    "src/cli/"
)

# File extensions to scan
EXTENSIONS=(-name "*.c" -o -name "*.h")

violations=0
violation_files=()

while IFS= read -r -d '' file; do
    # Skip files in allowed directories
    skip=0
    for allowed in "${ALLOWED_DIRS[@]}"; do
        if [[ "$file" == *"$allowed"* ]]; then
            skip=1
            break
        fi
    done
    [[ $skip -eq 1 ]] && continue

    # Get relative path for display
    rel="${file#"$ROOT/"}"

    # Scan line by line
    lineno=0
    while IFS= read -r line; do
        lineno=$((lineno + 1))

        # Skip pure comment lines
        stripped="${line#"${line%%[![:space:]]*}"}"   # ltrim
        if [[ "$stripped" == //* || "$stripped" == "/*"* || "$stripped" == "*"* ]]; then
            continue
        fi

        # Skip lines with NOLINT(keel-syscall) annotation
        if [[ "$line" == *"NOLINT(keel-syscall)"* ]]; then
            continue
        fi

        # Skip lines that are keel_ wrappers (contain keel_malloc etc.)
        if echo "$line" | grep -qE 'keel_(malloc|calloc|realloc|free|strdup|strndup|memdup|aligned)'; then
            # Still check if there's also a bare call on the same line
            # (unlikely but safe to check)
            :
        fi

        # Check for forbidden patterns
        # Exclude lines where the match is part of a longer identifier
        # by using grep -P with word boundaries
        if echo "$line" | grep -qP "$FORBIDDEN_PATTERN"; then
            # Exclude keel_ prefixed versions that also match (e.g. keel_free contains "free")
            # Strip all keel_* occurrences and check if bare call remains
            cleaned=$(echo "$line" | sed 's/keel_[a-z_]*//g')
            if echo "$cleaned" | grep -qP "$FORBIDDEN_PATTERN"; then
                # Also exclude known third-party free functions by name
                if echo "$line" | grep -qP '\b(EVP_|BIO_|X509_|ASN1_|DH_|EC_|RSA_|HMAC_|ERR_|ldap_|pg_|pgbuf_|freeaddrinfo\b|ifaddrs|lua_|ZSTD_|zstd_|inflate|deflate)'; then
                    # Check if the match is ONLY in such third-party calls
                    only_third_party=$(echo "$cleaned" | sed 's/\b\(EVP_\|BIO_\|X509_\|ASN1_\|DH_\|EC_\|RSA_\|HMAC_\|ERR_\|ldap_\|pg_\|pgbuf_\)[a-zA-Z_]*\s*(//g' | sed 's/freeaddrinfo\s*(//g')
                    if ! echo "$only_third_party" | grep -qP "$FORBIDDEN_PATTERN"; then
                        continue
                    fi
                fi

                echo "$rel:$lineno: forbidden system allocator call: $line"
                violations=$((violations + 1))
                violation_files+=("$rel")
            fi
        fi
    done < "$file"
done < <(find "$ROOT/src" "$ROOT/include" \
    \( "${EXTENSIONS[@]}" \) \
    -not -path "*/\.git/*" \
    -print0 2>/dev/null)

if [[ $violations -gt 0 ]]; then
    echo ""
    echo "ERROR: Found $violations forbidden system allocator call(s) in $(echo "${violation_files[@]}" | tr ' ' '\n' | sort -u | wc -l) file(s)."
    echo ""
    echo "All dynamic allocation must use the keel memory layer:"
    echo "  keel_malloc, keel_calloc, keel_realloc, keel_free,"
    echo "  keel_strdup, keel_strndup, keel_memdup"
    echo ""
    echo "If a system API boundary requires system allocator (e.g. PAM),"
    echo "annotate the line with:  /* NOLINT(keel-syscall) */"
    exit 1
fi

echo "OK: No forbidden system allocator calls found."
exit 0
