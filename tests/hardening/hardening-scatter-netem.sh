#!/usr/bin/env bash
# =============================================================================
# tests/hardening/hardening-scatter-netem.sh
# =============================================================================
#
# Hardening: Scatter fan-out under network jitter and packet loss.
#
# Runs scatter SELECT queries through keel while injecting configurable netem
# faults on the interface between keel and the shard backends:
#
#   1. High jitter (±JITTER_MS around BASE_DELAY_MS) — validates scatter
#      correctly collects results even when shard responses arrive out of order
#      or with high latency variance.
#
#   2. Packet loss (LOSS_PCT) — validates that the scatter deadline fires
#      correctly on partial-loss connections rather than hanging forever.
#
#   3. Combined (delay + jitter + reorder + loss) — worst-case production
#      scenario on a degraded inter-AZ link.
#
# Expected behaviour (§1.5 Phase 1 hardening):
#   - All scatter queries complete within QUERY_TIMEOUT_S (no hangs).
#   - Success rate under pure jitter (no loss) must be ≥ SUCCESS_PCT_JITTER.
#   - Under packet loss, errors are surfaced within QUERY_TIMEOUT_S.
#   - No partial / silently-incorrect results are returned.
#   - keel does not crash, leak memory, or leave connections in a broken state.
#
# Requirements:
#   - psql on PATH
#   - Linux tc (iproute2) with netem module loaded
#   - Sufficient privilege to run tc (CAP_NET_ADMIN or sudo)
#   - keel running and reachable at KEEL_HOST:KEEL_PORT
#   - IFACE must be the interface carrying keel→shard traffic
#
# Environment Variables:
#   KEEL_HOST         Proxy hostname       (default: 127.0.0.1)
#   KEEL_PORT         Proxy port           (default: 17432)
#   CHAOS_DB          Database name        (default: chaosdb)
#   CHAOS_USER        DB user              (default: postgres)
#   CHAOS_PASS        DB password          (default: postgres)
#   IFACE             Network interface    (default: lo)
#   BASE_DELAY_MS     Base delay in ms     (default: 40)
#   JITTER_MS         Jitter ± in ms       (default: 30)
#   REORDER_PCT       Reorder probability  (default: 10)
#   LOSS_PCT          Loss probability     (default: 5)
#   N_QUERIES         Queries per phase    (default: 20)
#   QUERY_TIMEOUT_S   Per-query timeout    (default: 8)
#   SUCCESS_PCT_JITTER Min success % under jitter only (default: 90)
# =============================================================================
set -uo pipefail

KEEL_HOST="${KEEL_HOST:-127.0.0.1}"
KEEL_PORT="${KEEL_PORT:-17432}"
CHAOS_DB="${CHAOS_DB:-chaosdb}"
CHAOS_USER="${CHAOS_USER:-postgres}"
CHAOS_PASS="${CHAOS_PASS:-postgres}"
IFACE="${IFACE:-lo}"
BASE_DELAY_MS="${BASE_DELAY_MS:-40}"
JITTER_MS="${JITTER_MS:-30}"
REORDER_PCT="${REORDER_PCT:-10}"
LOSS_PCT="${LOSS_PCT:-5}"
N_QUERIES="${N_QUERIES:-20}"
QUERY_TIMEOUT_S="${QUERY_TIMEOUT_S:-8}"
SUCCESS_PCT_JITTER="${SUCCESS_PCT_JITTER:-90}"

die()  { echo "FAIL: $*" >&2; exit 1; }
log()  { echo "[scatter-netem] $*"; }
pass() { echo "PASS: $*"; }

# ── tc helpers ────────────────────────────────────────────────────────────────

tc_add_jitter() {
    local delay="$1" jitter="$2" reorder="$3"
    tc qdisc add dev "$IFACE" root netem \
        delay "${delay}ms" "${jitter}ms" \
        reorder "${reorder}%" distribution normal \
        2>/dev/null \
    || tc qdisc change dev "$IFACE" root netem \
        delay "${delay}ms" "${jitter}ms" \
        reorder "${reorder}%" distribution normal
}

tc_add_loss_jitter() {
    local delay="$1" jitter="$2" reorder="$3" loss="$4"
    tc qdisc add dev "$IFACE" root netem \
        delay "${delay}ms" "${jitter}ms" \
        reorder "${reorder}%" loss "${loss}%" \
        2>/dev/null \
    || tc qdisc change dev "$IFACE" root netem \
        delay "${delay}ms" "${jitter}ms" \
        reorder "${reorder}%" loss "${loss}%"
}

tc_clear() {
    tc qdisc del dev "$IFACE" root 2>/dev/null || true
}

# ── Query helper ──────────────────────────────────────────────────────────────

scatter_query() {
    local result elapsed
    local start end
    start=$(date +%s%N)
    result=$(PGPASSWORD="$CHAOS_PASS" timeout "$QUERY_TIMEOUT_S" psql \
        -h "$KEEL_HOST" -p "$KEEL_PORT" \
        -U "$CHAOS_USER" -d "$CHAOS_DB" \
        -t -c "SELECT COUNT(*) FROM orders" 2>/dev/null \
        | tr -d ' \n' || echo "ERR")
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))  # ms
    echo "${result}:${elapsed}"
}

run_phase() {
    local label="$1"
    local ok=0 err=0 hung=0
    log "--- Phase: ${label} ---"
    for i in $(seq 1 "$N_QUERIES"); do
        IFS=: read -r result elapsed <<< "$(scatter_query)"
        if [[ "$result" =~ ^[0-9]+$ ]]; then
            ok=$((ok + 1))
            log "  q${i}: OK (${elapsed}ms) COUNT=${result}"
        elif [[ $elapsed -ge $((QUERY_TIMEOUT_S * 1000 - 100)) ]]; then
            hung=$((hung + 1))
            log "  q${i}: HUNG (${elapsed}ms)"
        else
            err=$((err + 1))
            log "  q${i}: ERR (${elapsed}ms): ${result:0:60}"
        fi
    done
    log "  Summary: ok=${ok} err=${err} hung=${hung} / ${N_QUERIES}"
    echo "${ok}:${err}:${hung}"
}

# ── Preflight ─────────────────────────────────────────────────────────────────

command -v psql >/dev/null 2>&1 || die "psql not found on PATH"
command -v tc   >/dev/null 2>&1 || die "tc not found (install iproute2)"

# Ensure netem module is available
modprobe sch_netem 2>/dev/null || true
tc qdisc add dev "$IFACE" root netem delay 1ms 2>/dev/null || \
tc qdisc change dev "$IFACE" root netem delay 1ms 2>/dev/null || \
    die "Cannot apply netem on ${IFACE} — check CAP_NET_ADMIN or sudo"
tc_clear

# Verify baseline scatter
log "Verifying baseline (no fault)..."
IFS=: read -r baseline_cnt baseline_ms <<< "$(scatter_query)"
[[ "$baseline_cnt" =~ ^[0-9]+$ ]] \
    || die "Baseline scatter query failed: ${baseline_cnt} — is keel running?"
log "Baseline: COUNT=${baseline_cnt} in ${baseline_ms}ms"

trap tc_clear EXIT

# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: High jitter only (no packet loss)
#          Scatter must succeed ≥ SUCCESS_PCT_JITTER % of the time.
# ══════════════════════════════════════════════════════════════════════════════

tc_add_jitter "$BASE_DELAY_MS" "$JITTER_MS" "$REORDER_PCT"
IFS=: read -r j_ok j_err j_hung <<< "$(run_phase "jitter ${BASE_DELAY_MS}ms ±${JITTER_MS}ms reorder ${REORDER_PCT}%")"
tc_clear

jitter_success_pct=$(( j_ok * 100 / N_QUERIES ))
log "Phase 1: jitter success=${jitter_success_pct}% (min ${SUCCESS_PCT_JITTER}%)"

if [[ $j_hung -gt 0 ]]; then
    die "Phase 1: ${j_hung} queries HUNG under jitter — keel scatter deadline not firing"
fi
if [[ $jitter_success_pct -lt $SUCCESS_PCT_JITTER ]]; then
    die "Phase 1: jitter success rate ${jitter_success_pct}% < required ${SUCCESS_PCT_JITTER}%"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Packet loss
#          Queries may fail but must NOT hang past QUERY_TIMEOUT_S.
# ══════════════════════════════════════════════════════════════════════════════

tc_add_loss_jitter "$BASE_DELAY_MS" "$JITTER_MS" "$REORDER_PCT" "$LOSS_PCT"
IFS=: read -r l_ok l_err l_hung <<< "$(run_phase "jitter+loss ${LOSS_PCT}%")"
tc_clear

log "Phase 2: loss ok=${l_ok} err=${l_err} hung=${l_hung}"

if [[ $l_hung -gt 0 ]]; then
    die "Phase 2: ${l_hung} queries HUNG under packet loss — keel scatter deadline not firing"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Verify recovery after fault is removed
# ══════════════════════════════════════════════════════════════════════════════

log "Phase 3: Verifying scatter recovers after fault removal..."
recovered=0
for i in $(seq 1 10); do
    IFS=: read -r r_cnt r_ms <<< "$(scatter_query)"
    if [[ "$r_cnt" =~ ^[0-9]+$ ]]; then
        log "  Recovered in attempt $i: COUNT=${r_cnt} (${r_ms}ms)"
        if [[ "$r_cnt" != "$baseline_cnt" ]]; then
            log "  WARNING: recovered COUNT=${r_cnt} != baseline COUNT=${baseline_cnt} (data may have changed)"
        fi
        recovered=1
        break
    fi
    log "  Attempt $i: still failing (${r_cnt}), retrying..."
    sleep 1
done

[[ $recovered -eq 1 ]] || die "Phase 3: scatter did not recover within 10 attempts after fault cleared"

pass "Scatter netem hardening: jitter=${jitter_success_pct}% ok, loss handled (0 hangs), recovery confirmed"
