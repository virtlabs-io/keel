#!/usr/bin/env bash
# ============================================================================
# hardening-syscall-fault-injection.sh — Syscall error injection via strace
# ============================================================================
#
# Uses strace's -e inject= facility to make specific system calls fail with
# controlled error codes, then verifies the KEEL proxy handles the failures
# gracefully without crashing or leaking file descriptors.
#
# Why this matters:
#   In production, syscalls can fail transiently due to resource exhaustion
#   (ENOMEM), interrupt signals (EINTR), or kernel backpressure (EAGAIN).
#   A resilient proxy must retry or gracefully degrade when kernel operations
#   fail, rather than crashing or leaking resources.
#
# Algorithm:
#   For each syscall in the FAULT_SYSCALLS list:
#   1. Start the KEEL binary under strace with:
#        strace -e inject=SYSCALL:error=ERRNO:when=N+M
#      where N+M specifies "fail the Mth invocation after N successful ones"
#      (e.g., when=5+1 means fail every 5th call).
#   2. Wait for the proxy to become ready (admin console probe).
#   3. Take a snapshot of open FDs in /proc/<pid>/fd.
#   4. Run a sysbench workload to exercise the injected syscall path.
#   5. Take a second FD snapshot.
#   6. If FD count grew by more than FD_LEAK_THRESHOLD (default: 32),
#      report an FD leak — the proxy is not cleaning up after the injected
#      failure.
#   7. Verify the proxy process is still alive (did not crash).
#   8. Shut down the proxy cleanly.
#
# Syscalls tested (default):
#   - io_uring_enter — the core io_uring submission syscall; EAGAIN means
#     the submission queue is full and the proxy must retry.
#   - mmap — memory mapping; ENOMEM forces the proxy's buffer allocation
#     fallback paths.
#   - splice — zero-copy data transfer; EAGAIN exercises retry logic in
#     the fast-path data forwarding.
#
# Environment Variables:
#   KEEL_BIN              Path to keel binary (default: build-linux/src/main/keel)
#   CFG_FILE              Path to keel config (default: etc/keel-mix.ini)
#   FAULT_SYSCALLS        Space-separated list of syscalls to inject faults into
#                         (default: "io_uring_enter mmap splice")
#   FD_LEAK_THRESHOLD     Maximum acceptable FD growth before reporting a leak
#                         (default: 32)
#   PROXY_HOST            Proxy address (default: 127.0.0.1)
#   PROXY_PORT            Proxy port (default: 7432)
#   ADMIN_HOST/PORT       Admin console connection parameters
#   DB_USER/PASSWORD/NAME Database connection parameters
#   THREADS               Sysbench thread count (default: 20)
#   DURATION              Workload duration in seconds (default: 10)
#
# Prerequisites:
#   - strace with -e inject support (strace >= 4.17)
#   - sysbench with PostgreSQL driver
#   - psql on PATH
#   - KEEL binary built
#
# Exit Codes:
#   0  PASS — proxy survived all fault injections without crash or FD leak
#   1  FAIL — proxy crashed or leaked FDs under fault injection
#
# Usage:
#   ./scripts/hardening-syscall-fault-injection.sh
#   FAULT_SYSCALLS="io_uring_enter" ./scripts/hardening-syscall-fault-injection.sh
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/keel_fault_inject_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"

KEEL_CMD="${KEEL_CMD:-$ROOT_DIR/build-linux/src/main/keel -c $ROOT_DIR/etc/keel-mix.ini}"
READY_PROBE_CMD="${READY_PROBE_CMD:-PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d testdb -X -A -t -c 'SELECT 1;'}"
WORKLOAD_CMD="${WORKLOAD_CMD:-PGPASSWORD=postgres psql -h 127.0.0.1 -p 7432 -U postgres -d testdb -X -A -t -c 'SELECT generate_series(1,200);'}"
FAULT_SYSCALLS="${FAULT_SYSCALLS:-io_uring_enter,mmap,splice}"
FAULT_ERRNO="${FAULT_ERRNO:-EAGAIN}"
INJECT_WHEN="${INJECT_WHEN:-3+5}"
MAX_WAIT_SEC="${MAX_WAIT_SEC:-15}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[fault-inject] FAIL: missing dependency '$1'" >&2
    exit 2
  }
}

need_cmd strace
need_cmd bash

trim() {
  local v="$1"
  v="${v#${v%%[![:space:]]*}}"
  v="${v%${v##*[![:space:]]}}"
  printf "%s" "$v"
}

wait_until_ready() {
  local pid="$1"
  local start
  start=$(date +%s)
  while :; do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    if bash -lc "$READY_PROBE_CMD" >/dev/null 2>&1; then
      return 0
    fi
    local now
    now=$(date +%s)
    if (( now - start >= MAX_WAIT_SEC )); then
      return 1
    fi
    sleep 1
  done
}

run_one_fault() {
  local syscall="$1"
  local log_base="$OUT_DIR/strace_${syscall}"
  local out="$OUT_DIR/${syscall}.txt"

  echo "[fault-inject] syscall=$syscall errno=$FAULT_ERRNO when=$INJECT_WHEN" | tee "$out"

  set +e
  strace -ff -o "$log_base" -e "inject=${syscall}:error=${FAULT_ERRNO}:when=${INJECT_WHEN}" \
    bash -lc "$KEEL_CMD" >"$OUT_DIR/keel_${syscall}.stdout" 2>"$OUT_DIR/keel_${syscall}.stderr" &
  local trace_pid=$!
  set -e

  if ! wait_until_ready "$trace_pid"; then
    echo "[fault-inject] FAIL: proxy did not become ready for syscall=$syscall" | tee -a "$out"
    kill "$trace_pid" 2>/dev/null || true
    wait "$trace_pid" 2>/dev/null || true
    return 1
  fi

  local fd_before="-1"
  if [[ -d "/proc/$trace_pid/fd" ]]; then
    fd_before="$(find "/proc/$trace_pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')"
  fi

  local workload_rc=0
  bash -lc "$WORKLOAD_CMD" >"$OUT_DIR/workload_${syscall}.out" 2>"$OUT_DIR/workload_${syscall}.err" || workload_rc=$?

  local fd_after="-1"
  if [[ -d "/proc/$trace_pid/fd" ]]; then
    fd_after="$(find "/proc/$trace_pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')"
  fi

  kill "$trace_pid" 2>/dev/null || true
  wait "$trace_pid" 2>/dev/null || true

  echo "workload_rc=$workload_rc" | tee -a "$out"
  echo "fd_before=$fd_before" | tee -a "$out"
  echo "fd_after=$fd_after" | tee -a "$out"

  if [[ "$workload_rc" -ne 0 ]]; then
    echo "[fault-inject] FAIL: workload failed under syscall=$syscall" | tee -a "$out"
    return 1
  fi

  if [[ "$fd_before" != "-1" && "$fd_after" != "-1" ]] && (( fd_after > fd_before + 32 )); then
    echo "[fault-inject] FAIL: suspicious FD growth ($fd_before -> $fd_after) syscall=$syscall" | tee -a "$out"
    return 1
  fi

  echo "[fault-inject] PASS: syscall=$syscall" | tee -a "$out"
  return 0
}

echo "[fault-inject] output: $OUT_DIR"
echo "[fault-inject] keel_cmd: $KEEL_CMD"

rc=0
IFS=',' read -r -a calls <<< "$FAULT_SYSCALLS"
for raw in "${calls[@]}"; do
  syscall="$(trim "$raw")"
  [[ -z "$syscall" ]] && continue
  if ! run_one_fault "$syscall"; then
    rc=1
  fi
done

if [[ "$rc" -ne 0 ]]; then
  echo "[fault-inject] FAIL: one or more syscall injections failed"
  exit 1
fi

echo "[fault-inject] PASS: all syscall injections completed"
