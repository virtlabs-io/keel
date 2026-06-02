#!/bin/bash
# tests/integration/test_pg_jdbc_prepared.sh
#
# Regression test for KEEL's prepared-statement pooling under concurrent
# JDBC load.  Reproduces:
#
#   ERROR: prepared statement "S_1" already exists
#
# observed when multiple pgJDBC clients (prepareThreshold=1) share a
# KEEL backend pool in VIRTUALIZE mode.  Brings up the Patroni stack +
# KEEL, then launches N concurrent pgJDBC clients each running M
# iterations of a long-lived named PreparedStatement and asserts that
# every iteration succeeds.
#
# Exit codes:
#   0 — every concurrent client passed all iterations
#   1 — at least one client reported an error / stale read / ambiguous
#   2 — environment setup failed
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JDBC_DIR="${SCRIPT_DIR}/jdbc"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CLIENTS="${KEEL_JDBC_CLIENTS:-8}"
ITERATIONS="${KEEL_JDBC_ITERATIONS:-50}"
KEEL_HOST="${KEEL_JDBC_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_JDBC_PORT:-7432}"
DSN="postgres://postgres:postgres@${KEEL_HOST}:${KEEL_PORT}/postgres"
OUTDIR="$(mktemp -d -t keel-jdbc-XXXXXX)"
IMAGE_TAG="keel-jdbc-runner:test"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cleanup() {
    [[ -n "${OUTDIR:-}" && -d "${OUTDIR}" ]] && rm -rf "${OUTDIR}"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
need() { command -v "$1" >/dev/null 2>&1 || { err "missing required tool: $1"; exit 2; }; }
need docker
need java || true   # if no host java, fall back to docker runner below
need javac || true

[[ -f "${JDBC_DIR}/Main.java" && -f "${JDBC_DIR}/postgresql.jar" ]] || {
    err "JDBC reproducer missing in ${JDBC_DIR}"
    exit 2
}

# ---------------------------------------------------------------------------
# Wait for KEEL to be reachable (assumed to be up already; e.g. via the
# Patroni compose stack in docker/postgres-patroni).  Tests are responsible
# for orchestrating the stack — this script ONLY drives the JDBC client.
# ---------------------------------------------------------------------------
log "Probing KEEL at ${KEEL_HOST}:${KEEL_PORT} ..."
for _ in $(seq 1 30); do
    if (echo > "/dev/tcp/${KEEL_HOST}/${KEEL_PORT}") 2>/dev/null; then
        log "KEEL is reachable"
        break
    fi
    sleep 1
done
if ! (echo > "/dev/tcp/${KEEL_HOST}/${KEEL_PORT}") 2>/dev/null; then
    err "KEEL not reachable at ${KEEL_HOST}:${KEEL_PORT} — bring up the stack first"
    exit 2
fi

# ---------------------------------------------------------------------------
# Build or compile the JDBC client.  Prefer host JDK if present; otherwise
# build a one-shot Docker image from tests/integration/jdbc/Dockerfile.
# ---------------------------------------------------------------------------
USE_DOCKER=0
if command -v javac >/dev/null 2>&1 && command -v java >/dev/null 2>&1; then
    log "Compiling Main.java with host JDK"
    ( cd "${JDBC_DIR}" && javac -cp postgresql.jar Main.java )
else
    log "No host JDK — building Docker image ${IMAGE_TAG}"
    docker build -q -t "${IMAGE_TAG}" "${JDBC_DIR}" >/dev/null
    USE_DOCKER=1
fi

run_one_client() {
    local idx="$1"
    local out="${OUTDIR}/run_${idx}.json"
    if [[ "${USE_DOCKER}" -eq 1 ]]; then
        docker run --rm --network host "${IMAGE_TAG}" \
            --dsn "${DSN}" --iterations "${ITERATIONS}" --json \
            > "${out}" 2>&1 || true
    else
        ( cd "${JDBC_DIR}" && java -cp .:postgresql.jar Main \
            --dsn "${DSN}" --iterations "${ITERATIONS}" --json \
            > "${out}" 2>&1 || true )
    fi
}

# ---------------------------------------------------------------------------
# Launch N concurrent clients
# ---------------------------------------------------------------------------
log "Launching ${CLIENTS} concurrent JDBC clients (${ITERATIONS} iter each) ..."
pids=()
for i in $(seq 1 "${CLIENTS}"); do
    run_one_client "$i" &
    pids+=("$!")
done
for pid in "${pids[@]}"; do
    wait "$pid" || true
done

# ---------------------------------------------------------------------------
# Aggregate results
# ---------------------------------------------------------------------------
total_errors=0
total_stale=0
total_ambig=0
total_passed=0
failed_clients=()
for i in $(seq 1 "${CLIENTS}"); do
    out="${OUTDIR}/run_${i}.json"
    if ! [[ -s "$out" ]]; then
        err "client ${i}: no output"
        failed_clients+=("${i}")
        continue
    fi
    passed=$(grep -oE '"passed": [0-9]+' "$out" | head -1 | awk '{print $2}')
    errors=$(grep -oE '"errors": [0-9]+' "$out" | head -1 | awk '{print $2}')
    stale=$(grep -oE '"stale_reads": [0-9]+' "$out" | head -1 | awk '{print $2}')
    ambig=$(grep -oE '"ambiguous": [0-9]+' "$out" | head -1 | awk '{print $2}')
    first_err=$(grep -oE '"first_error_seq": -?[0-9]+' "$out" | head -1 | awk '{print $2}')
    total_passed=$((total_passed + ${passed:-0}))
    total_errors=$((total_errors + ${errors:-0}))
    total_stale=$((total_stale + ${stale:-0}))
    total_ambig=$((total_ambig + ${ambig:-0}))
    if [[ "${errors:-0}" -ne 0 || "${stale:-0}" -ne 0 || "${ambig:-0}" -ne 0 ]]; then
        msg=$(grep -m1 '"error":' "$out" || true)
        warn "client ${i}: passed=${passed} errors=${errors} stale=${stale} ambig=${ambig} first_err_seq=${first_err}"
        [[ -n "$msg" ]] && warn "  first error: ${msg}"
        failed_clients+=("${i}")
    else
        log "client ${i}: passed=${passed}"
    fi
done

log "Aggregate: passed=${total_passed} errors=${total_errors} stale=${total_stale} ambiguous=${total_ambig}"
if [[ ${#failed_clients[@]} -gt 0 ]]; then
    err "${#failed_clients[@]}/${CLIENTS} client(s) had non-pass outcomes: ${failed_clients[*]}"
    err "Per-client JSON kept in: ${OUTDIR}"
    trap - EXIT  # keep OUTDIR for debugging
    exit 1
fi
log "All ${CLIENTS} JDBC clients passed ${ITERATIONS} iterations"
exit 0
