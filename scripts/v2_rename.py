#!/usr/bin/env python3
"""
One-shot v2 key rename inside string literals only.
Rewrites src/main/main.c and (if listed) other consumer .c files in place.
- Replaces  keel_config_get_int(... "K_ms"|"K_bytes" ...)
  with     keel_config_get_duration_ms(... "K" ...)  or  keel_config_get_bytes(...)
- Inside any other "..." string literal, replaces a renamed word as a
  whole token (boundary = non-[A-Za-z0-9_]). This catches log format
  strings like  "[%s] pool_wait_timeout_ms: %llu -> %lld"  while
  leaving C identifiers (wg->pool_wait_timeout_ms) untouched.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

DURATION_KEYS = {
    "shutdown_timeout_ms":              "shutdown_timeout",
    "log_interval_ms":                  "log_interval",
    "otlp_timeout_ms":                  "otlp_timeout",
    "otlp_interval_ms":                 "otlp_interval",
    "heartbeat_interval_ms":            "heartbeat_interval",
    "heartbeat_timeout_ms":             "heartbeat_timeout",
    "flush_interval_ms":                "flush_interval",
    "export_timeout_ms":                "export_timeout",
    "idle_timeout_ms":                  "idle_timeout",
    "connect_timeout_ms":               "connect_timeout",
    "pool_prune_interval_ms":           "pool_prune_interval",
    "pool_refill_interval_ms":          "pool_refill_interval",
    "pool_refill_backoff_ms":           "pool_refill_backoff",
    "pool_wait_timeout_ms":             "pool_wait_timeout",
    "rebalance_interval_ms":            "rebalance_interval",
    "sqpoll_idle_ms":                   "sqpoll_idle",
    "sticky_primary_ttl_ms":            "sticky_primary_ttl",
    "tls_handshake_timeout_ms":         "tls_handshake_timeout",
    "tls_read_timeout_ms":              "tls_read_timeout",
    "backend_tls_handshake_timeout_ms": "backend_tls_handshake_timeout",
    "backend_tls_read_timeout_ms":      "backend_tls_read_timeout",
    "max_connection_age_ms":            "max_connection_age",
}

BYTES_KEYS = {
    "session_max_buffered_bytes": "session_max_buffered",
    "backend_max_replay_bytes":   "backend_max_replay",
    "otlp_encode_buf_bytes":      "otlp_encode_buf",
    "compress_threshold_bytes":   "compress_threshold",
}

# Special: was MiB-multiplier int -> now bytes-typed.
SPECIAL_KEYS = {
    "scatter_merge_max_mem_mb": "scatter_merge_max_mem",
}

ALL_RENAMES = {**DURATION_KEYS, **BYTES_KEYS, **SPECIAL_KEYS}

# 1. Rewrite keel_config_get_int(...)  call sites whose key argument is
#    a renamed key. The key string may live on the same line as the call
#    or on a continuation line, so we operate on the joined call text
#    using a non-greedy regex.

CALL_PATTERN = re.compile(
    r'keel_config_get_int\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*"([^"]+)"',
    re.MULTILINE,
)


def rewrite_calls(src: str) -> tuple[str, int]:
    n = 0
    out = []
    last = 0
    for m in CALL_PATTERN.finditer(src):
        key = m.group(3)
        if key in DURATION_KEYS:
            new_key = DURATION_KEYS[key]
            replacement = f'keel_config_get_duration_ms({m.group(1)}, {m.group(2)}, "{new_key}"'
        elif key in BYTES_KEYS or key in SPECIAL_KEYS:
            new_key = ALL_RENAMES[key]
            replacement = f'keel_config_get_bytes({m.group(1)}, {m.group(2)}, "{new_key}"'
        else:
            continue
        out.append(src[last:m.start()])
        out.append(replacement)
        last = m.end()
        n += 1
    out.append(src[last:])
    return "".join(out), n


# 2. Inside string literals (excluding the call-site key strings already
#    handled above), replace old-key tokens with new-key tokens.
STRING_LITERAL = re.compile(r'"((?:\\.|[^"\\])*)"')

TOKEN_BOUNDARY_PATTERNS = {
    re.compile(rf'(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])'): new
    for old, new in ALL_RENAMES.items()
}


def rewrite_string_literals(src: str) -> tuple[str, int]:
    n = 0

    def replace(match: re.Match) -> str:
        nonlocal n
        inner = match.group(1)
        new_inner = inner
        for pat, new_token in TOKEN_BOUNDARY_PATTERNS.items():
            new_inner, k = pat.subn(new_token, new_inner)
            n += k
        return f'"{new_inner}"'

    return STRING_LITERAL.sub(replace, src), n


def process(path: Path) -> None:
    src = path.read_text()
    src2, n_calls = rewrite_calls(src)
    src3, n_strings = rewrite_string_literals(src2)
    if src3 != src:
        path.write_text(src3)
        print(f"{path}: {n_calls} call-site renames, {n_strings} in-string renames")
    else:
        print(f"{path}: no changes")


if __name__ == "__main__":
    for p in sys.argv[1:]:
        process(Path(p))
