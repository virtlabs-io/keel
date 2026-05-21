#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <repo_root> <build_dir> <baseline|strict>" >&2
  exit 2
fi

ROOT_DIR="$1"
BUILD_DIR="$2"
MODE="$3"

if [[ "$MODE" != "baseline" && "$MODE" != "strict" ]]; then
  echo "[harness-security] FAIL: invalid mode '$MODE'" >&2
  exit 2
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "[harness-security] SKIP: seccomp is Linux-only"
  exit 0
fi

KEEL_BIN="$BUILD_DIR/src/main/keel"
if [[ ! -x "$KEEL_BIN" ]]; then
  echo "[harness-security] FAIL: binary not found: $KEEL_BIN" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
pid=""
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    # Use SIGTERM so keel can drain in-flight io_uring operations cleanly.
    # kill -9 leaves the worker thread stuck in the kernel's io_uring cancel
    # path until the TCP retransmission timer expires (~127s default).
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT

cfg="$tmpdir/security.ini"
listen_port="${KEEL_TEST_SECURITY_PORT:-}"
if [[ -z "$listen_port" ]]; then
  listen_port="$(
    python3 -c 'import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()'
  )"
fi

cat > "$cfg" <<EOF
[keel]
log_level = 0

[worker_group.smoke]
name = smoke
bind_addr = 127.0.0.1
bind_port = $listen_port
num_workers = 1
protocol = postgres

[security]
privilege_drop = false
require_privilege_drop = false
seccomp = $MODE
require_seccomp = false
no_new_privs = true
EOF

# Strict seccomp blocks the ptrace/kill syscalls that ASAN's LeakSanitizer
# uses to scan heap at process exit, causing the process to hang indefinitely.
# Disable LSan leak detection for this test; memory correctness is validated
# by the sanitizer build's other (non-seccomp) tests.
# Note: ASAN+LSan combined builds check ASAN_OPTIONS; standalone -fsanitize=leak
# builds check LSAN_OPTIONS.  Set both to be safe.
if [[ "$MODE" == "strict" ]]; then
  export ASAN_OPTIONS="${ASAN_OPTIONS:+${ASAN_OPTIONS}:}detect_leaks=0"
  export LSAN_OPTIONS="${LSAN_OPTIONS:+${LSAN_OPTIONS}:}detect_leaks=0"
fi

allow_nonewprivs_fallback=0
if [[ -f /.dockerenv || -n "${KEEL_TEST_ALLOW_RELAXED_NO_NEW_PRIVS:-}" ]]; then
  allow_nonewprivs_fallback=1
fi

"$KEEL_BIN" -c "$cfg" >"$tmpdir/keel.out" 2>&1 &
pid=$!

# Wait until process is running and security policy has had a chance to apply.
for _ in $(seq 1 50); do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "[harness-security] FAIL: process exited early in mode=$MODE" >&2
    cat "$tmpdir/keel.out" >&2 || true
    exit 1
  fi
  if [[ -f "/proc/$pid/status" ]]; then
    if awk '/^Seccomp:/ { if ($2 >= 2) ok=1 } END { exit(ok ? 0 : 1) }' "/proc/$pid/status"; then
      break
    fi
  fi
  sleep 0.05
done

if ! kill -0 "$pid" 2>/dev/null; then
  echo "[harness-security] FAIL: process terminated before status checks" >&2
  cat "$tmpdir/keel.out" >&2 || true
  exit 1
fi

status="/proc/$pid/status"
if [[ ! -r "$status" ]]; then
  echo "[harness-security] FAIL: cannot read $status" >&2
  exit 1
fi

nonewprivs="$(awk '/^NoNewPrivs:/ {print $2}' "$status" | tail -n1)"
seccomp="$(awk '/^Seccomp:/ {print $2}' "$status" | tail -n1)"

if [[ "$nonewprivs" != "1" ]]; then
  if [[ "$allow_nonewprivs_fallback" == "1" ]]; then
    echo "[harness-security] WARN: expected NoNewPrivs=1, got '${nonewprivs:-missing}' (mode=$MODE); accepting in containerized build environment" >&2
  else
    echo "[harness-security] FAIL: expected NoNewPrivs=1, got '${nonewprivs:-missing}' (mode=$MODE)" >&2
    cat "$tmpdir/keel.out" >&2 || true
    exit 1
  fi
fi

# 2 == SECCOMP_MODE_FILTER
# Docker build containers (and some CI sandboxes) apply their own seccomp
# profile that blocks the seccomp(2) syscall, preventing keel from loading
# its own filter.  When /.dockerenv is present or the relaxed-env override
# is set we accept Seccomp<2 as a warning so the build is not gated on a
# host-kernel policy that is outside our control.
if [[ -z "$seccomp" || "$seccomp" -lt 2 ]]; then
  if [[ "$allow_nonewprivs_fallback" == "1" ]]; then
    echo "[harness-security] WARN: expected Seccomp>=2, got '${seccomp:-missing}' (mode=$MODE); accepting in containerized build environment" >&2
  else
    echo "[harness-security] FAIL: expected Seccomp>=2, got '${seccomp:-missing}' (mode=$MODE)" >&2
    cat "$tmpdir/keel.out" >&2 || true
    exit 1
  fi
fi

echo "[harness-security] PASS mode=$MODE NoNewPrivs=$nonewprivs Seccomp=$seccomp"
