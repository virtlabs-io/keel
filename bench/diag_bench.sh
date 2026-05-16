#!/bin/bash
# ============================================================================
# diag_bench.sh — Quick diagnostic benchmark with connection pool sampling
# ============================================================================
#
# A lightweight diagnostic script that starts the KEEL proxy, runs a short
# sysbench TPC-C-like workload, and samples connection pool state mid-test
# to help debug routing, pooling, and performance issues.
#
# Diagnostic stages:
#   1. Start KEEL proxy with the specified configuration.
#   2. Launch sysbench tpcc_like_persistent workload (read/write split
#      enabled via READ_TXN_MODE=readonly).
#   3. Mid-test (5 seconds in): sample backend connection counts per
#      PostgreSQL host via pg_stat_activity, and query the KEEL admin
#      console for pool statistics (SHOW STATS).
#   4. Wait for sysbench to finish and report final TPS/latency results.
#
# This script is designed for quick iteration during development — it gives
# immediate visibility into whether connections are being distributed across
# backends and how the pool is behaving under load.
#
# Environment Variables:
#   READ_TXN_MODE   Transaction mode for reads (set to "readonly" for R/W split)
#
# Prerequisites:
#   - KEEL binary built at build-linux/src/main/keel
#   - sysbench with PostgreSQL driver
#   - psql for admin console queries
#   - PostgreSQL backends running and reachable
#
# Usage:
#   ./bench/diag_bench.sh
# ============================================================================
set -e

export READ_TXN_MODE=readonly
export PGPASSWORD=qaz123

THREADS=${1:-700}
TIME=${2:-20}
echo "=== Starting KEEL ==="
cd /keel
build-linux/src/main/keel -c etc/keel-pg-test.ini &
KEEL_PID=$!
sleep 3

echo "=== Running sysbench: threads=$THREADS time=${TIME}s ==="
sysbench /keel/bench/tpcc_like_persistent.lua \
  --db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port=7432 \
  --pgsql-user=usr_test --pgsql-password=qaz123 --pgsql-db=testdb \
  --scale=10 --read-pct=70 --threads=$THREADS --time=$TIME run > /tmp/bench.log 2>&1 &
BENCH_PID=$!

# Wait for test to stabilize
sleep $((TIME / 2))

echo ""
echo "=== Mid-test backend connection counts ==="
for h in pgsql-01 pgsql-02 pgsql-03; do
  echo -n "  $h: "
  psql -h "$h" -p 5432 -U usr_test -d testdb -t -A -c \
    "SELECT count(*)||' total, '|| \
            count(*) filter(where state='active')||' active, '|| \
            count(*) filter(where state='idle')||' idle, '|| \
            count(*) filter(where state='idle in transaction')||' idle_in_tx' \
     FROM pg_stat_activity WHERE usename='usr_test'"
done

echo ""
echo "=== Mid-test KEEL stats (pool) ==="
psql -h 127.0.0.1 -p 16433 -U admin -t -A -c "SHOW STATS" 2>/dev/null | \
  grep -E 'pool_borrows|pool_returns|pool_hits|pool_misses|flow_wait_pool|queries_read|queries_write|errors_timeout'

# Wait for bench to finish
wait $BENCH_PID 2>/dev/null

echo ""
echo "=== Final benchmark result ==="
grep -A20 'SQL statistics' /tmp/bench.log

echo ""
echo "=== Final KEEL stats ==="
psql -h 127.0.0.1 -p 16433 -U admin -t -A -c "SHOW STATS" 2>/dev/null | \
  grep -E 'pool_borrows|pool_returns|pool_hits|pool_misses|flow_wait_pool|queries_read|queries_write|errors_timeout|pool_wait_queue'

# Cleanup
pkill -9 keel 2>/dev/null || true
echo ""
echo "=== Done ==="
