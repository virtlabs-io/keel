#!/usr/bin/env bash
# =============================================================================
# trace_overhead.sh — Measure tracing overhead on Keel throughput
# =============================================================================
#
# Runs two identical pgbench workloads through Keel — one with tracing
# disabled, one with tracing enabled at 100% sampling — and compares the
# resulting TPS to quantify the overhead of the tracing subsystem.
#
# Usage:
#   ./trace_overhead.sh <keel_binary> <config_file> <connstr> [duration] [clients]
#
# Example:
#   ./trace_overhead.sh ../build/src/keel ../etc/keel-pg.ini \
#     "host=127.0.0.1 port=6432 dbname=keeltest user=postgres" 60 64
#
# Prerequisites:
#   - pgbench on PATH
#   - PostgreSQL backend running with the bench schema loaded
#   - An OTLP collector listening (or a sink like /dev/null) for the
#     tracing-enabled run — set OTLP_ENDPOINT to override the default
# =============================================================================
set -euo pipefail

KEEL="${1:?Usage: $0 <keel_binary> <config> <connstr> [duration] [clients]}"
CONFIG="${2:?}"
CONNSTR="${3:?}"
DURATION="${4:-60}"
CLIENTS="${5:-64}"
THREADS="${6:-$(( CLIENTS < $(nproc) ? CLIENTS : $(nproc) ))}"
OTLP_ENDPOINT="${OTLP_ENDPOINT:-http://localhost:4318/v1/traces}"

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$BASE_DIR/results/trace_overhead_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

log() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
start_keel() {
    local label="$1" cfg="$2"
    log "Starting Keel ($label) ..."
    "$KEEL" -c "$cfg" &
    KEEL_PID=$!
    sleep 2  # Wait for listeners to bind
    if ! kill -0 "$KEEL_PID" 2>/dev/null; then
        log "ERROR: Keel failed to start ($label)"
        exit 1
    fi
}

stop_keel() {
    if [[ -n "${KEEL_PID:-}" ]] && kill -0 "$KEEL_PID" 2>/dev/null; then
        kill "$KEEL_PID"
        wait "$KEEL_PID" 2>/dev/null || true
    fi
}

run_pgbench() {
    local label="$1" out="$RESULTS_DIR/$label.txt"
    log "Running pgbench ($label): ${DURATION}s, ${CLIENTS} clients, ${THREADS} threads"
    pgbench "$CONNSTR" \
        -n -r \
        -T "$DURATION" \
        -c "$CLIENTS" \
        -j "$THREADS" \
        --random-seed=1 \
        --file="$BASE_DIR/scripts/read_point.sql"@45 \
        --file="$BASE_DIR/scripts/read_range.sql"@15 \
        --file="$BASE_DIR/scripts/read_aggregate.sql"@10 \
        --file="$BASE_DIR/scripts/write_update.sql"@20 \
        --file="$BASE_DIR/scripts/write_insert.sql"@8 \
        --file="$BASE_DIR/scripts/write_delete.sql"@2 \
        2>&1 | tee "$out"
    echo ""
}

extract_tps() {
    grep -oP 'tps = \K[0-9.]+' "$1" | head -1
}

trap stop_keel EXIT

# ---------------------------------------------------------------------------
# Run 1: Tracing DISABLED (baseline)
# ---------------------------------------------------------------------------
log "=== Phase 1: Baseline (tracing disabled) ==="
CFG_BASELINE="$RESULTS_DIR/keel_baseline.ini"
cp "$CONFIG" "$CFG_BASELINE"
# Ensure tracing is off
if grep -q '^\[tracing\]' "$CFG_BASELINE"; then
    sed -i '/^\[tracing\]/,/^\[/ s/^enabled.*/enabled = false/' "$CFG_BASELINE"
else
    printf "\n[tracing]\nenabled = false\n" >> "$CFG_BASELINE"
fi

start_keel "baseline" "$CFG_BASELINE"
run_pgbench "baseline"
stop_keel

sleep 2

# ---------------------------------------------------------------------------
# Run 2: Tracing ENABLED (100% sampling)
# ---------------------------------------------------------------------------
log "=== Phase 2: Tracing (100% sampling) ==="
CFG_TRACING="$RESULTS_DIR/keel_tracing.ini"
cp "$CONFIG" "$CFG_TRACING"
# Enable tracing at 100%
if grep -q '^\[tracing\]' "$CFG_TRACING"; then
    sed -i '/^\[tracing\]/,/^\[/ {
        s/^enabled.*/enabled = true/
        s/^sample_rate_ppm.*/sample_rate_ppm = 1000000/
        s/^endpoint.*/endpoint = '"${OTLP_ENDPOINT//\//\\/}"'/
    }' "$CFG_TRACING"
else
    cat >> "$CFG_TRACING" <<EOF

[tracing]
enabled = true
endpoint = ${OTLP_ENDPOINT}
sample_rate_ppm = 1000000
service_name = keel-bench
batch_size = 512
flush_interval_ms = 2000
EOF
fi

start_keel "tracing" "$CFG_TRACING"
run_pgbench "tracing"
stop_keel

# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------
log "=== Results ==="

TPS_BASE=$(extract_tps "$RESULTS_DIR/baseline.txt")
TPS_TRACE=$(extract_tps "$RESULTS_DIR/tracing.txt")

if [[ -n "$TPS_BASE" && -n "$TPS_TRACE" ]]; then
    OVERHEAD=$(awk "BEGIN { printf \"%.2f\", (1 - $TPS_TRACE / $TPS_BASE) * 100 }")
    printf "  Baseline TPS:       %s\n" "$TPS_BASE"
    printf "  Tracing TPS:        %s\n" "$TPS_TRACE"
    printf "  Overhead:           %s%%\n" "$OVERHEAD"

    # Write machine-readable summary
    cat > "$RESULTS_DIR/summary.json" <<EOF
{
  "baseline_tps": ${TPS_BASE},
  "tracing_tps": ${TPS_TRACE},
  "overhead_pct": ${OVERHEAD},
  "duration_s": ${DURATION},
  "clients": ${CLIENTS},
  "threads": ${THREADS},
  "sample_rate_ppm": 1000000
}
EOF
    log "Results saved to $RESULTS_DIR/summary.json"
else
    log "WARNING: Could not extract TPS from pgbench output"
    log "Check $RESULTS_DIR/baseline.txt and $RESULTS_DIR/tracing.txt"
fi
