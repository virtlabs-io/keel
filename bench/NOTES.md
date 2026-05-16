# KEEL Benchmarking Notes

## 1. Why this kit exists

The built-in pgbench workload is not appropriate for validating KEEL routing behavior because it is centered around transaction blocks. For a proxy/router, what matters is whether **individual SQL statements** are classified and sent to the correct backend.

This kit forces a mixed workload of independent statements.

## 2. Why autocommit matters

For KEEL, explicit transaction blocks can force transaction pinning to the primary or to a single backend, depending on router semantics.

If you want true read scaling validation, the generator must produce read-only statements outside an explicit `BEGIN ... COMMIT` block.

## 3. What to measure

### Functional correctness
- no writes on replicas
- read-only statements routed to replicas when eligible
- no transaction-state leakage between pooled sessions
- no prepared-state contamination
- no session taint false positives

### Performance
- TPS / QPS overall
- per-class latency: point read, range read, aggregate read, update, insert, delete
- p50 / p95 / p99 latency
- backend connection counts
- queueing inside KEEL
- parser and router CPU time
- pool checkout latency
- primary CPU and replica CPU separately

### Stability
- memory growth over time
- FD growth over time
- stuck sessions
- retry storms
- route oscillation
- replica lag under sustained writes

## 4. Strong recommendation

Capture metrics from four places at the same time:
1. pgbench output
2. KEEL internal metrics/logs
3. primary PostgreSQL statistics
4. replica PostgreSQL statistics

A benchmark without all four is usually misleading.

## 5. Suggested test matrix

### A. Routing correctness
- 1 client
- 5 clients
- 20 clients
- verify exact statement placement

### B. Concurrency scaling
- 32 clients
- 64 clients
- 128 clients
- 256 clients
- 512 clients
- 1024 clients

### C. Pool pressure
Repeat the same test while shrinking backend pool sizes.

### D. Replica stress
Use 1 replica, then 2 replicas, then 3 replicas.

### E. Read skew
Try:
- 90/10 read/write
- 70/30 read/write
- 50/50 read/write

### F. Query complexity
Try:
- PK-only reads
- PK + range reads
- PK + range + aggregate reads

### G. Failure scenarios
- restart one replica during load
- pause replication apply on one replica
- add latency between KEEL and replicas
- kill backend connections abruptly

## 6. Important limitation of this simple kit

This is a good synthetic stress harness, but not a perfect application model.
Once KEEL behaves correctly under this kit, the next step should be replaying production-like SQL traces.
