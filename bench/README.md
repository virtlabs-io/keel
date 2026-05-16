# KEEL pgbench Stress Test Kit

This kit is designed to stress test **KEEL** with a workload that is approximately **70% reads** and **30% writes**, using **single SQL statements** in **autocommit mode**.

The key goal is to avoid explicit transaction blocks so KEEL can classify and route each statement independently:
- **read-only SELECTs** should be eligible for replica routing
- **writes** should go to the primary

## What is included

- `schema.sql` — schema, indexes, and helper objects
- `seed.sql` — optional extra data generation helpers
- `scripts/` — individual pgbench script files, each containing **one statement only**
- `run_keel_pgbench.sh` — launcher for weighted mixed workload tests
- `verify_routing.sql` — SQL checks to validate behavior during and after the run
- `notes.md` — tuning notes and caveats for KEEL-specific benchmarking

## Workload mix

This kit uses the following target mix:

- **45%** point lookups
- **15%** range reads
- **10%** aggregate/reporting reads
- **20%** updates
- **8%** inserts
- **2%** deletes

Total:
- **70% reads**
- **30% writes**

## Important design choice

Each script file contains **exactly one SQL statement** and **no BEGIN/COMMIT**.
That means pgbench will issue the statements in autocommit mode, which is what you want for KEEL routing tests.

## Example usage

Initialize schema:

```bash
psql "host=<keel-host> port=<keel-port> dbname=<db> user=<user>" -f schema.sql
```

Optional: load extra seed helpers:

```bash
psql "host=<keel-host> port=<keel-port> dbname=<db> user=<user>" -f seed.sql
```

Run the benchmark:

```bash
chmod +x run_keel_pgbench.sh
./run_keel_pgbench.sh \
  "host=127.0.0.1 port=6432 dbname=keeltest user=postgres" \
  300 \
  128 \
  16
```

Arguments:
1. PostgreSQL connection string
2. duration in seconds
3. number of clients
4. number of worker threads

Example:

```bash
./run_keel_pgbench.sh \
  "host=10.0.0.10 port=5432 dbname=appdb user=app password=secret" \
  600 \
  256 \
  32
```

## Verify KEEL routing

Run these checks during the test:

```bash
psql "host=<primary> dbname=<db> user=<user>" -f verify_routing.sql
```

Also inspect:
- KEEL route counters by backend role
- primary `pg_stat_statements`
- replica `pg_stat_statements`
- primary `pg_stat_database`
- replica `pg_stat_database`
- connection distribution across nodes

## Notes

### 1. Use KEEL, not a direct server, as the target
Point pgbench to KEEL's frontend listener, not directly to PostgreSQL.

### 2. Make sure replicas allow the reads you are sending
If some reads use functions or settings that force primary routing in KEEL, the read ratio observed in the cluster may differ from the script weight.

### 3. Prefer realistic query shapes
Do not only use `SELECT 1`. Use PK lookups, range reads, and small aggregates.

### 4. Scale clients carefully
At high concurrency, the generator can become the bottleneck. If needed, run multiple pgbench clients from multiple hosts.

### 5. Turn on observability first
Before ramping concurrency, make sure you can answer:
- how many reads went to replicas?
- how many reads leaked to primary?
- how many writes hit replicas by mistake?
- what was p95/p99 latency by statement class?
- where did connections accumulate?

## Suggested progression

1. **Sanity phase**: 8 clients, 2 threads, 60 seconds
2. **Warm phase**: 32 clients, 4 threads, 180 seconds
3. **Pressure phase**: 128 clients, 16 threads, 300 seconds
4. **Saturation phase**: 256 to 1024 clients, depending on hardware
5. **Multi-generator phase**: several pgbench hosts simultaneously

## What success looks like

- write statements only hit the primary
- eligible read-only statements are distributed to replicas
- no connection leaks
- no backend explosion on the primary
- no route oscillation
- no growing latency cliff caused by misrouting or pool starvation
- no crash or parser/routing regression under sustained mixed load
