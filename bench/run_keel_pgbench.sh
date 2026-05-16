#!/usr/bin/env bash
# =============================================================================
# run_keel_pgbench.sh — Weighted pgbench harness for Keel R/W-split benchmarks
# =============================================================================
#
# Drives a pgbench workload through Keel's connection pooler using a realistic
# read/write distribution modelled after a multi-tenant SaaS backend.
#
# Script weights (total = 100):
#   45 %  read_point.sql      — Single-row PK lookup  (hot path, index-only)
#   15 %  read_range.sql      — Range scan by tenant + date window (LIMIT 50)
#   10 %  read_aggregate.sql  — COUNT/SUM/AVG aggregate per tenant (7-day)
#   20 %  write_update.sql    — UPDATE account balance/score
#    8 %  write_insert.sql    — INSERT new event with JSONB payload
#    2 %  write_delete.sql    — DELETE old processed events (SKIP LOCKED)
#
# The mix yields a 70 / 30 read-to-write ratio, which is representative of
# OLTP workloads that benefit from read-replica offloading.  The heavy read
# bias stresses Keel's routing accuracy, while the non-trivial write share
# ensures the primary path and transaction handling are exercised.
#
# Reproducibility:
#   --random-seed=1 pins pgbench's PRNG so successive runs produce the same
#   query sequence.  This makes latency histograms directly comparable across
#   builds without needing to average many iterations.
#
# Usage:
#   ./run_keel_pgbench.sh "<connstr>" <duration_sec> <clients> <threads>
#
# Arguments:
#   connstr   — libpq connection string (host, port, dbname, user, etc.)
#   duration  — test duration in seconds (e.g. 300 for a 5-minute run)
#   clients   — number of concurrent pgbench client connections
#   threads   — pgbench threads (should ≤ clients; ≤ CPU cores for best results)
#
# Prerequisites:
#   - PostgreSQL pgbench binary on PATH.
#   - The target database must already have the Keel benchmark schema loaded
#     (see schema.sql) and optionally expanded with seed.sql.
#   - Keel must be listening on the port referenced in the connection string.
#
# pgbench flags:
#   -n    — skip vacuuming (schema has no built-in pgbench tables)
#   -r    — report per-statement latencies in the final summary
#   -P 5  — print progress every 5 seconds during the run
# =============================================================================
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 \"<connstr>\" <duration_seconds> <clients> <threads>"
  echo "Example: $0 ./run_keel_pgbench.sh \
  \"host=127.0.0.1 port=6432 dbname=keeltest user=postgres\" \
  300 \
  128 \
  16"
  exit 1
fi

CONNSTR="$1"
DURATION="$2"
CLIENTS="$3"
THREADS="$4"

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPTS_DIR="$BASE_DIR/scripts"

exec pgbench "$CONNSTR" \
  -n \
  -r \
  -P 5 \
  -T "$DURATION" \
  -c "$CLIENTS" \
  -j "$THREADS" \
  --random-seed=1 \
  --file="$SCRIPTS_DIR/read_point.sql"@45 \
  --file="$SCRIPTS_DIR/read_range.sql"@15 \
  --file="$SCRIPTS_DIR/read_aggregate.sql"@10 \
  --file="$SCRIPTS_DIR/write_update.sql"@20 \
  --file="$SCRIPTS_DIR/write_insert.sql"@8 \
  --file="$SCRIPTS_DIR/write_delete.sql"@2
