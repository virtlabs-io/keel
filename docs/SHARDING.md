# KEEL Horizontal Sharding

> **Last updated:** 2026-05-08  
> **Codebase locations:**  
> — `include/keel/core/sharding.h` — public API for shard-key extraction and planning  
> — `include/keel/core/router.h` — router integration, scatter, dispatch  
> — `src/core/sharding.c` — implementation (798 lines)  
> — `src/core/router_weighted.c` — routing engine (includes scatter, migration, dispatch)  
> — `tests/test_sharding.c` — 100+ assertions across all phases  
> — `tests/test_shard_hot_reload.c` — hot-reload regression suite

---

## Table of Contents

1. [What Is Horizontal Sharding in KEEL?](#1-what-is-horizontal-sharding-in-keel)
2. [Architecture Overview](#2-architecture-overview)
3. [Comparison With Other Approaches](#3-comparison-with-other-approaches)
4. [When To Use Horizontal Sharding](#4-when-to-use-horizontal-sharding)
5. [When NOT To Use Horizontal Sharding](#5-when-not-to-use-horizontal-sharding)
6. [Shard Rules](#6-shard-rules)
7. [Shard Key Extraction — Deep Dive](#7-shard-key-extraction--deep-dive)
8. [Shard Mapping Algorithms](#8-shard-mapping-algorithms)
9. [Routing Plan Lifecycle](#9-routing-plan-lifecycle)
10. [Scatter Fan-out](#10-scatter-fan-out)
11. [Multi-Shard Transaction Coordination](#11-multi-shard-transaction-coordination)
12. [Live Shard Migration](#12-live-shard-migration)
13. [Configuration Reference](#13-configuration-reference)
14. [Hot-Reload via SIGHUP](#14-hot-reload-via-sighup)
15. [Admin SQL Interface](#15-admin-sql-interface)
16. [Prometheus Metrics](#16-prometheus-metrics)
17. [Connection Pool Integration](#17-connection-pool-integration)
18. [Risks and Corner Cases](#18-risks-and-corner-cases)
19. [Architectural Decisions and Rationale](#19-architectural-decisions-and-rationale)
20. [Implementation State and Feature Map](#20-implementation-state-and-feature-map)
21. [Roadmap — What Remains](#21-roadmap--what-remains)
22. [Full API Reference](#22-full-api-reference)

---

## 1. What Is Horizontal Sharding in KEEL?

KEEL's horizontal sharding lets a single proxy instance transparently distribute data
across multiple physical PostgreSQL servers (**shards**) without requiring any change
to the application.  The application connects to KEEL using the standard PostgreSQL
wire protocol; KEEL intercepts every query, parses the SQL, identifies which shard
holds the relevant rows, and forwards the query to that shard.

**Core capabilities:**

- **Transparent**: applications use the standard pg wire protocol; zero code changes.
- **In-process, zero-latency routing**: shard keys are extracted from the SQL AST; no
  metadata server round-trip is required for the routing decision.
- **Multi-strategy mapping**: HASH (`xxhash64 % N`) and RANGE (ordered INT64
  threshold table) are supported per rule.
- **Scatter fan-out**: queries without a shard predicate are automatically broadcast
  to all shards, with per-shard read/write split.
- **Controlled migration**: shard rebalancing (move rows from shard A to shard B)
  uses dual-write + read-from-new semantics and requires an operator-tested
  migration window.
- **Hot-reload**: shard rules can be updated at runtime via SIGHUP without restarting
  the proxy.
- **Observability**: Prometheus counters, admin SQL, per-shard routing stats.
- **Multi-table**: up to 16 independent shard rules in one router.

---

## 2. Architecture Overview

### High-level data flow

```
  Application (Postgres client)
        |  SQL text / prepared statement + $N bindings
        v
+------------------------------------------------------------------+
|                         KEEL Proxy                               |
|                                                                  |
|  +-------------------------------------------------------------+ |
|  |   keel_router_dispatch_sql()          (one call)            | |
|  |                                                             | |
|  |   for each registered shard rule:                           | |
|  |     +-----------------------------------------------+      | |
|  |     |  keel_shard_plan()                            |      | |
|  |     |    1. keel_sql_parse()    -> AST (arena)      |      | |
|  |     |    2. keel_shard_extract_key_ast()            |      | |
|  |     |         +- shard_extract_select()             |      | |
|  |     |         +- shard_extract_insert()             |      | |
|  |     |         +- shard_extract_update()             |      | |
|  |     |         +- shard_extract_delete()             |      | |
|  |     |    3. keel_shard_map_key_bound_rule()         |      | |
|  |     |         +- HASH: xxhash64(val) % N            |      | |
|  |     |         +- RANGE: binary threshold lookup     |      | |
|  |     |    -> keel_shard_plan_t { SINGLE | SCATTER |  |      | |
|  |     |                           UNSUPPORTED }       |      | |
|  |     +-----------------------------------------------+      | |
|  |                                                             | |
|  |   SINGLE   -> route_internal(shard_index)                   | |
|  |   SCATTER  -> keel_router_scatter_servers() x N shards      | |
|  |   UNSUPPORT-> KEEL_ERR_NOT_SUPPORTED                        | |
|  +-------------------------------------------------------------+ |
|                                                                  |
|  Migration intercept (KEEL_SHARD_STATE_MIGRATING)                |
|    write -> dual-write (src shard + dst shard)                   |
|    read  -> redirect to dst shard                                |
|                                                                  |
+------------------------------------------------------------------+
        |                              |
        v                              v
  PostgreSQL shard 0          PostgreSQL shard N-1
  (shard_id=0)                (shard_id=N-1)
```

### Per-shard server topology

Each physical shard can have its own primary and one or more replicas.  KEEL applies
its normal read/write split within each shard:

```
  Shard 0                         Shard 1
  +--------------------+          +--------------------+
  | pg-shard0-primary  |          | pg-shard1-primary  |
  |  role=RW           |<--write  |  role=RW           |
  |  shard_id=0        |          |  shard_id=1        |
  +--------------------+          +--------------------+
  +--------------------+          +--------------------+
  | pg-shard0-replica  |          | pg-shard1-replica  |
  |  role=RO           |<--read   |  role=RO           |
  |  shard_id=0        |          |  shard_id=1        |
  +--------------------+          +--------------------+
```

KEEL tracks which `shard_id` each server belongs to.  During single-shard routing the
read/write split applies only within that shard's server set.  During scatter fan-out,
each shard gets its own read/write decision independently.

### Component dependencies

```
sharding.c
  +-- keel_sql_parse()          (sql/sql.h)
  +-- keel_str_eq_nocase()      (util/util.h)
  +-- keel_xxh64()              (util/xxhash.h)

router_weighted.c
  +-- sharding.h                (shard_plan_t, shard_rule_t, shard_key_t)
  +-- keel_shard_plan()         (sharding.c)
  +-- keel_shard_map_key_bound_rule()
  +-- build_read_indices_for_shard()   (internal)
  +-- build_write_indices_for_shard()  (internal)
  +-- route_shard_for_scatter()        (internal)
```

---

## 3. Comparison With Other Approaches

### KEEL vs application-level sharding

| Dimension | Application-level | KEEL (proxy-level) |
|-----------|------------------|--------------------|
| Code changes | Large (every query must route explicitly) | None (transparent) |
| Migration | Requires application deploy | SIGHUP or config file |
| Cross-language | Each service re-implements | Centralised in proxy |
| Scatter aggregation | Custom per service | Built-in merge callbacks |
| Shard count change | Application downtime or coordination | Hot-reload, no downtime |

### KEEL vs native database partitioning (Postgres declarative partitioning)

| Dimension | Postgres native partitioning | KEEL horizontal sharding |
|-----------|------------------------------|--------------------------|
| Physical distribution | All partitions on one node | Each shard on its own server |
| Write throughput | Bounded by single-node I/O | Linear with shard count |
| Read scalability | Limited to one primary | Per-shard replica reads |
| Admin complexity | DDL (ALTER TABLE, PARTITION OF) | INI config + SIGHUP |
| Cross-shard joins | Native planner | Not supported; must denormalise |
| HA per shard | Manual streaming replication | Native shard-level HA handled by KEEL failover |

### KEEL vs Citus (PostgreSQL distributed extension)

| Dimension | Citus | KEEL |
|-----------|-------|------|
| Architecture | PostgreSQL extension (coordinator) | Standalone proxy |
| SQL compatibility | High (extension intercepts at executor) | Subset (no JOIN routing) |
| Deployment complexity | Extension install, coordinator setup | Single binary |
| Multi-database | No | Yes (each worker_group is independent) |
| Protocol transparency | Full (wire-compatible coordinator) | Full (pass-through wire proxy) |
| Cross-shard joins | Supported via executor | Not supported |
| Scatter aggregation | SQL GROUP BY passes through | Manual merge callback |
| Migration | Rebalancer daemon | Per-rule MIGRATING state |
| Overhead per query | Extension call overhead (~us) | Regex-free AST parse (~us) |
| Language | C extension | C23, no PG dependency |

### KEEL vs Vitess (MySQL proxy sharding)

| Dimension | Vitess | KEEL |
|-----------|--------|------|
| Target DB | MySQL primary | PostgreSQL primary (MySQL supported) |
| SQL rewriting | Yes (normalises queries) | No rewriting, pass-through |
| VSchema | Complex declarative schema | Simple INI config |
| Cross-shard txns | 2PC with dedicated TM | Scatter write + cross-tx guard |
| Operational complexity | High (etcd, vtgate, vtctld) | Low (single config file) |
| Scatter aggregation | Via VTGate V3 | Manual merge callback |
| Connection pooling | Built-in (tablet-level) | Built-in (keel_connpool_t) |

### KEEL vs pgPool-II

| Dimension | pgPool-II | KEEL |
|-----------|-----------|------|
| Sharding | Pattern-based (regex) | AST-based (SQL parse) |
| Accuracy | False positives on string patterns | Structurally correct |
| Bound parameters ($N) | Not supported for routing | First-class support |
| Multiple rules | No per-table rules | 16 rules, per-table strategies |
| Hot-reload | Reload conf | SIGHUP, per-rule |
| Read/write split | Yes | Yes, per shard |
| Language | C | C23 |

---

## 4. When To Use Horizontal Sharding

KEEL horizontal sharding is the right choice when:

1. **Write throughput is the bottleneck.** A single PostgreSQL primary can saturate at
   ~50-200k TPS depending on workload. Sharding distributes writes across N primaries,
   yielding near-linear write throughput growth with shard count.

2. **Dataset size exceeds single-server capacity.** When the working set no longer
   fits in a server's RAM, index lookups become I/O-bound. Each shard holds 1/N of
   the data, restoring RAM locality.

3. **You have a natural partition key.** Tenant ID, user ID, region, or any high-
   cardinality column that appears in every query targeting the sharded table is
   ideal. Queries without the shard key in their WHERE clause scatter (fan-out) to
   all shards, which is acceptable if rare.

4. **You need independent HA per shard.** Each shard can have its own replica and
   failover policy. A shard failure does not affect other shards.

5. **You want transparent sharding without application changes.** KEEL routes
   correctly based solely on the SQL text. No ORM plugin, driver, or service-mesh
   annotation is required.

6. **You need controlled rebalancing.** KEEL's `KEEL_SHARD_STATE_MIGRATING` allows
  adding a new shard and progressively moving rows with dual-write + read-from-new
  semantics, compared to partition reattachment (which requires an exclusive lock)
  or pg_repack.

---

## 5. When NOT To Use Horizontal Sharding

Horizontal sharding is the wrong tool in these situations:

### 5.1 Cross-shard JOIN queries are your main pattern

KEEL does not support routing a JOIN that spans two sharded tables on different
shards.  If your application routinely queries:

```sql
SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.region = 'EU';
```

...and `users` and `orders` are on different shards, KEEL will scatter (or fail with
UNSUPPORTED depending on the table-rule match), and your application must reassemble
results in memory.  If this pattern is frequent, consider:
- Co-locating the tables on the same shard (same `shard_id`)
- Using Citus with reference tables
- Denormalising (embed order data in user rows)

### 5.2 Frequent aggregate queries across all data

`SELECT SUM(amount) FROM orders` has no shard predicate and will scatter to all N
shards.  KEEL's `keel_route_merge_fn` lets you aggregate per-shard row sets, but
this is manual.  For analytical workloads, a dedicated OLAP database (ClickHouse,
Redshift, BigQuery, or Postgres with pg_partman) is more appropriate.

### 5.3 Total data volume is still < 100 GB

At this scale, PostgreSQL with a well-tuned replica and proper indexing will outperform
any sharded setup.  Sharding adds operational complexity (N times the servers, backup
schedules, schema migrations) that is unjustifiable until the single-server ceiling is
genuinely hit.

### 5.4 Your schema has no high-cardinality partition key

If every table has many-to-many relationships with no dominant access pattern through
one key, scatter queries become the common case rather than the exception.  A scatter
on N shards adds N x RTT latency and N x CPU on the proxy.

### 5.5 You require full ACID across shards

KEEL's cross-shard transaction guard (`KEEL_ERR_SHARD_CROSS_TX`) prevents a single
transaction from writing to a shard that was not part of its scatter write.  However,
KEEL does not implement two-phase commit (2PC) between shards.  If your application
needs a single database transaction to atomically write to two different shards (e.g.
transfer money between a user on shard 0 and a user on shard 1), KEEL alone cannot
guarantee atomicity.  Options: application-level saga, XA transactions (not currently
supported), or keep both users on the same shard via consistent hashing.

### 5.6 Your shard key changes value frequently

If `user_id` is the shard key and users can be reassigned to a new tenant (and thus a
different shard), migrating rows is expensive.  Each row move requires dual-write and
careful bookkeeping.  Design your key to be immutable once assigned.

### 5.7 You need DDL on sharded tables

DDL (`CREATE TABLE`, `ALTER TABLE`, `DROP INDEX`) is classified as UNSUPPORTED by the
shard router.  You must run DDL directly on each shard server, or use a schema
migration tool that connects to each shard independently.  KEEL passes DDL through to
the default backend (no shard routing), which will only reach one shard.

---

## 6. Shard Rules

A **shard rule** is the unit of configuration that ties one table to one shard key
column and a mapping strategy.

```c
typedef struct keel_shard_rule {
    const char*           table;           /* "users"  (case-insensitive) */
    const char*           column;          /* "id"                        */
    size_t                shard_count;     /* 4                           */
    keel_shard_strategy_t strategy;        /* HASH (default) or RANGE     */

    /* RANGE strategy fields */
    int64_t               thresholds[64]; /* inclusive upper bounds       */
    size_t                threshold_count; /* must equal shard_count       */

    /* Migration fields */
    keel_shard_state_t    state;           /* NORMAL or MIGRATING          */
    size_t                migrate_src_shard;
    size_t                migrate_dst_shard;
} keel_shard_rule_t;
```

**Registry limits:**
- Maximum 16 rules per router (`KEEL_ROUTER_MAX_SHARD_RULES`)
- Maximum 64 shards per rule (`KEEL_SCATTER_MAX_SHARDS`, also the RANGE threshold cap)

Rules are looked up by table name (case-insensitive).  Adding a rule for a table
that already has one **overwrites** it in place.  Shard count changes emit a warning
log entry but are accepted.

---

## 7. Shard Key Extraction — Deep Dive

### 7.1 Overview

Extraction is performed by `keel_shard_extract_key_ast()`, which receives a pre-parsed
AST root node and a shard rule.  It dispatches to a statement-specific extractor.

```
keel_shard_extract_key_ast(ast, rule, key_out)
    +-- STMT_SELECT  -> shard_extract_select()
    +-- STMT_INSERT  -> shard_extract_insert()
    +-- STMT_UPDATE  -> shard_extract_update()
    +-- STMT_DELETE  -> shard_extract_delete()
    +-- other        -> KEEL_ERR_NOT_SUPPORTED
```

### 7.2 Internal extraction context

```c
typedef struct shard_extract_ctx {
    keel_str_t table_name;   /* rule->table                      */
    keel_str_t table_alias;  /* FROM users u -> alias = "u"      */
    keel_str_t shard_column; /* rule->column                     */
    bool       found;        /* at least one candidate found     */
    bool       conflict;     /* two candidates with != values    */
    keel_shard_key_t key;    /* first candidate accumulated      */
} shard_extract_ctx_t;
```

The conflict flag is set when the WHERE clause contains two incompatible equalities
on the same shard column (`WHERE id = 1 AND id = 2`).  Such queries return
UNSUPPORTED rather than silently routing to either shard.

### 7.3 SELECT extraction

```
shard_extract_select(select, rule, key_out):
    GUARD: no WITH clause, no UNION/INTERSECT/EXCEPT, has WHERE
    GUARD: FROM is a single table ref
    GUARD: FROM table name == rule->table  (case-insensitive)

    ctx.table_alias = alias (if any)
    shard_extract_from_where(select->where, &ctx)
    if ctx.conflict  -> UNSUPPORTED
    if !ctx.found    -> NOT_FOUND (SCATTER-eligible)
    *key_out = ctx.key
    return KEEL_OK
```

**Column qualification rules:**

| WHERE clause form | Matches? |
|-------------------|---------|
| `WHERE id = 42` | Yes (unqualified) |
| `WHERE users.id = 42` | Yes (table-qualified) |
| `WHERE u.id = 42` (with `FROM users u`) | Yes (alias-qualified) |
| `WHERE other_table.id = 42` | No (wrong table qualifier) |

### 7.4 INSERT extraction

Only single-row `VALUES` inserts are supported.  The shard column must appear in the
column list:

```
shard_extract_insert(ins, rule, key_out):
    GUARD: no WITH clause, has column list, has VALUES source (not INSERT...SELECT)
    GUARD: table name == rule->table

    col_idx = position of rule->column in ins->columns list
    if not found -> NOT_FOUND

    val_node = ins->values[first row][col_idx]
    if scalar (int / string / bool / $N param) -> OK
    else -> UNSUPPORTED
```

Multi-row `VALUES` inserts (`INSERT INTO t (id) VALUES (1), (2)`) extract only from
the **first row**.  This is a current limitation (see Section 21).

### 7.5 UPDATE extraction

```
shard_extract_update(update, rule, key_out):
    GUARD: no WITH clause, has WHERE
    GUARD: target table == rule->table

    alias = update->table->alias OR update->alias (PostgreSQL UPDATE ... AS alias)
    shard_extract_from_where(update->where, &ctx)
    ...
```

**Multi-table UPDATE (Feature 9):**

PostgreSQL allows `UPDATE t1 SET ... FROM t2 WHERE t1.id = t2.id AND t1.id = $1`.
KEEL extracts the shard key only from direct equality predicates on the _target_
table.  The FROM clause is intentionally ignored.  This means:

- `UPDATE users SET ... FROM accounts a WHERE users.id = $1` -> SINGLE (id extracted)
- `UPDATE users SET ... FROM accounts a WHERE a.id = $1` -> SCATTER (id not bound on users)

### 7.6 DELETE extraction

Identical to UPDATE but without a FROM clause.  A DELETE with no WHERE clause returns
NOT_FOUND and is scatter-eligible.

### 7.7 WHERE clause recursion

`shard_extract_from_where()` recursively walks the expression tree:

```
shard_extract_from_where(node, ctx):
    if node is BINARY_AND:
        recurse left, recurse right
    if node is BINARY_EQ:
        identify (column, scalar) pair
        if column == shard column:
            shard_record_candidate(ctx, &scalar_key)
    else:
        skip (OR, IN, BETWEEN, subquery, etc. -- all yield SCATTER)
```

**Why only AND and EQ?**

`OR` would require routing to the union of all resulting shard indices -- effectively
a scatter.  `IN (...)` could theoretically be decomposed into per-shard sub-queries,
but that would require query rewriting (a planned future feature).  The safe default
is to scatter, ensuring correctness at the cost of efficiency.

---

## 8. Shard Mapping Algorithms

### 8.1 HASH strategy (default)

The hash strategy maps a key value to a shard index using a deterministic, uniform
hash function.

**Integer keys:**

```
shard_index = (uint64_t)int64_value % shard_count
```

Negative values are safe: C23 guarantees that casting a negative `int64_t` to
`uint64_t` produces a two's-complement bit pattern, and modulo distributes uniformly
across the shard range.

**String and UUID keys:**

```
hash = xxhash64(string.data, string.len, seed=0)
shard_index = hash % shard_count
```

xxHash64 is chosen for:
- **Speed**: ~10 GB/s on modern CPUs; negligible cost for typical key lengths.
- **Stability**: deterministic across platforms, endianness-independent.
- **Quality**: extremely low collision rate and excellent distribution (passes all
  SMHasher tests).
- **Seed 0**: fixed seed ensures routing is reproducible without configuration.

**Boolean keys:**

```
shard_index = (key == true ? 1 : 0) % shard_count
```

Boolean sharding is useful only with exactly 2 shards (true/false partition).

### 8.2 RANGE strategy

Range partitioning supports INT64 keys only.  The rule carries a sorted array of
inclusive upper bounds (`thresholds[]`), one per shard.

```
shard_index = first i where int64_value <= thresholds[i]
             (last shard if value exceeds all thresholds)
```

**Example -- 4 shards with balanced user IDs up to 40 million:**

```
thresholds = [10_000_000, 20_000_000, 30_000_000, INT64_MAX]
id =  7_000_000  -> shard 0  (7M <= 10M)
id = 15_000_000  -> shard 1  (15M <= 20M)
id = 25_000_000  -> shard 2  (25M <= 30M)
id = 50_000_000  -> shard 3  (50M > 30M -> last shard)
```

Configured via:
```ini
[shard_rule.users]
column      = id
shard_count = 4
strategy    = range
```

Thresholds are supplied programmatically via `keel_router_add_shard_rule_range()`.

**Range vs hash trade-offs:**

| Property | HASH | RANGE |
|----------|------|-------|
| Distribution | Uniform (statistical) | Explicit (operator-controlled) |
| Range queries | Scatter to all shards | Can be targeted (future) |
| Hot spots | Self-balancing (uniform hash) | Possible if key skew exists |
| Key type | INT64, STRING, BOOL, $N | INT64 only |
| Migration | Consistent hash needed for splits | Threshold table update |
| Config | `shard_count` only | `thresholds[]` array |

### 8.3 Bound parameter resolution ($N)

KEEL supports PostgreSQL-style positional parameters in shard predicates:

```sql
SELECT * FROM users WHERE id = $1
```

The caller supplies a `keel_shard_bound_params_t` with the bound values:

```c
keel_shard_bound_params_t params = { .count = 1 };
params.values[0].kind              = KEEL_SHARD_KEY_INT64;
params.values[0].value.int64_value = 42;  /* $1 = 42 */
```

Resolution:

```
keel_shard_map_key_bound_rule(key, params, rule, &shard_index)
    if key.kind == PARAM:
        idx = key.param_index  /* 1-based */
        if idx < 1 or idx > params.count -> KEEL_ERR_NOT_FOUND
        bound = params.values[idx - 1]
        if bound.kind == PARAM or NONE -> KEEL_ERR_NOT_SUPPORTED
        delegate to keel_shard_map_key_rule(bound, rule, ...)
    else:
        delegate directly
```

If params is NULL for a PARAM key, the result is SCATTER.  This is intentional: when
a prepared statement is executed without its bindings, KEEL safely fans out rather than
routing incorrectly.

---

## 9. Routing Plan Lifecycle

### 9.1 Plan types

```c
typedef enum {
    KEEL_SHARD_PLAN_SINGLE      = 0,  /* one shard, shard_index is valid */
    KEEL_SHARD_PLAN_SCATTER     = 1,  /* fan-out to all shards            */
    KEEL_SHARD_PLAN_UNSUPPORTED = 2,  /* cannot shard-route this stmt     */
} keel_shard_plan_kind_t;
```

**Decision tree:**

```
keel_shard_plan(sql, rule, params, arena, plan):

    Parse sql -> ast
    if parse error -> UNSUPPORTED

    if ast.kind not in {SELECT, INSERT, UPDATE, DELETE}:
        -> UNSUPPORTED  (DDL, COPY, COMMIT, VACUUM, ...)

    err = keel_shard_extract_key_ast(ast, rule, &key)

    if err == NOT_FOUND:
        -> SCATTER  (valid DML, no shard predicate)

    if err != OK:
        -> UNSUPPORTED  (JOIN, WITH, wrong table, conflicting predicates)

    err = keel_shard_map_key_bound_rule(&key, params, rule, &shard_index)

    if err == NOT_FOUND:
        -> SCATTER  (PARAM key but no binding provided)

    if err != OK:
        -> UNSUPPORTED

    -> SINGLE, shard_index = shard_index
```

### 9.2 Multi-rule dispatch

`keel_router_plan_sql()` iterates all registered rules and returns the first
non-UNSUPPORTED outcome:

```
keel_router_plan_sql(router, sql, params, plan):
    parse sql once -> qt (query tree, reused across rule iterations)
    for i in 0 .. router.shard_rule_count-1:
        keel_shard_plan(sql, &router.shard_rules[i], params, arena, &candidate)
        if candidate.kind != UNSUPPORTED:
            *plan = candidate
            return
    *plan = UNSUPPORTED
```

The first matching rule wins.  Rule ordering matters when multiple tables appear in a
single query (rare; KEEL only extracts keys from the primary table of each statement).

### 9.3 Combined dispatch (keel_router_dispatch_sql)

The primary production entry-point:

```
keel_router_dispatch_sql(router, sql, session, params, is_write, out):

    for each registered rule:
        keel_shard_plan(sql, rule, params, arena, &plan)

        if plan == UNSUPPORTED: continue

        if plan == SINGLE:
            if rule.state == MIGRATING and plan.shard_index in {src, dst}:
                if write:
                    -> SCATTER(2): dual-write to src + dst
                else:
                    -> SINGLE to dst (read-from-new)
            else:
                -> SINGLE to plan.shard_index

        if plan == SCATTER:
            -> SCATTER(N): fan-out to all N shards

    return KEEL_ERR_NOT_SUPPORTED  (no rule matched)
```

---

## 10. Scatter Fan-out

### 10.1 Mechanics

When the plan is SCATTER, `keel_router_scatter_servers()` resolves one routing
decision per shard:

```c
keel_error_t keel_router_scatter_servers(
    keel_router_t*              router,
    const keel_route_session_t* session,
    const keel_shard_rule_t*    rule,
    bool                        is_write,
    keel_scatter_plan_t*        out);
```

For each shard index `i` in `0 .. rule->shard_count - 1`:
- Calls `route_shard_for_scatter(router, session, is_write, i, &decision)`
- `is_write = true`  -> selects from `build_write_indices_for_shard(i)`  -> primary servers
- `is_write = false` -> selects from `build_read_indices_for_shard(i)`   -> replicas first, primary fallback

Shards with no available server have `decisions[i].server == NULL` and increment
`out->failed`.  The function always returns `KEEL_OK`; partial failures are left to
the caller.

### 10.2 Scatter result aggregation

```c
/* Defined in include/keel/core/router.h */
typedef void (*keel_route_merge_fn)(keel_route_agg_t* result,
                                    size_t             shard_index,
                                    const void*        rows,
                                    size_t             row_count,
                                    void*              user_ctx);

struct keel_route_agg {
    keel_route_merge_fn  merge;            /* Optional merge callback */
    void*                user_ctx;         /* Forwarded to merge */
    void*                data;             /* Caller-managed output buffer */
    uint64_t             total_rows;       /* Cumulative rows across all shards */
    size_t               shards_completed; /* Shards that responded with data */
    size_t               shards_failed;    /* Shards that provided NULL rows */
};
typedef struct keel_route_agg keel_route_agg_t;
```

Use pattern:

```c
keel_route_agg_t result;
keel_route_agg_init(&result, my_merge_fn, &my_ctx);
result.data = calloc(MAX_ROWS, sizeof(my_row_t));

for (size_t i = 0; i < scatter.count; i++) {
    if (!scatter.decisions[i].server) continue;
    my_row_t shard_rows[MAX_SHARD_ROWS];
    size_t row_count = fetch_from_shard(scatter.decisions[i].server, shard_rows);
    keel_route_agg_feed(&result, i, shard_rows, row_count);
}
/* result.total_rows contains the combined row count */
```

### 10.3 Scatter and transactions

Scatter within a transaction is subject to cross-shard write tracking (Section 11).
Once a scatter write occurs, subsequent single-shard writes in the same transaction
must target a participating shard, or `KEEL_ERR_SHARD_CROSS_TX` is returned.

---

### 10.4 Scatter-Merge Aggregation Engine

When a SCATTER query contains aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`,
`MAX`, `COUNT(DISTINCT col)`), or `GROUP BY`, `HAVING`, `ORDER BY`, or `LIMIT`,
KEEL runs the query through the **scatter-merge engine** (`engine_scatter.c`).

The engine fans out to all shards in parallel, collects partial result sets, and
applies a multi-phase merge pipeline before returning a single coherent result to
the client.

#### Supported aggregate merge operations

| Function | Merge operation |
|----------|-----------------|
| `COUNT(*)`/`COUNT(col)` | `SUM` of partial counts |
| `SUM(col)` | `SUM` of partial sums |
| `AVG(col)` | Internal `SUM+COUNT` rewrite; divided at merge |
| `MIN(col)` | `MIN` of partial minimums |
| `MAX(col)` | `MAX` of partial maximums |
| `COUNT(DISTINCT col)` | Cross-shard hash deduplication |
| `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` / `NTILE(n)` / `PERCENT_RANK()` | Phase F global recompute |
| `LAG()` / `LEAD()` / `FIRST_VALUE()` / `LAST_VALUE()` / Running `SUM OVER` | Phase F pass-through recompute |
| `PARTITION BY` | Partitions formed from globally merged rows |

#### Merge phases

| Phase | Trigger | Description |
|-------|---------|-------------|
| **D** | `nagg_specs > 0 && ngroup_key_cols == 0` | Scalar aggregate merge |
| **D** | `requires_count_distinct` | COUNT DISTINCT deduplication |
| **E** | `ngroup_key_cols > 0` | GROUP BY hash aggregation |
| **F** | `nwindow_col_specs > 0` | **Window function global recomputation** — overwrites per-shard values |
| **H** | `nhaving_preds > 0` | HAVING post-filter |
| **C** | `norder_keys > 0` | ORDER BY global merge-sort |
| **L** | `limit_count > 0 \|\| limit_offset > 0` | Global LIMIT/OFFSET truncation |

#### LIMIT correctness with GROUP BY

When `GROUP BY` and `LIMIT` are combined, the engine uses `sc_strip_limit_offset()`
to remove the LIMIT/OFFSET clause from the per-shard SQL before dispatch.  Without
this, each shard would truncate its partial groups before KEEL can aggregate them —
silently dropping groups with high global aggregates but low per-shard counts.
The global LIMIT is applied post-merge after all groups have been summed.

#### Example: aggregate scatter query

```sql
-- Connect to KEEL (not directly to any shard)
SELECT status,
       COUNT(*)        AS order_count,
       SUM(amount)     AS revenue,
       AVG(amount)     AS avg_order
FROM orders
GROUP BY status
HAVING SUM(amount) > 100
ORDER BY revenue DESC
LIMIT 5;
```

KEEL:
1. Detects no shard-key predicate → SCATTER plan.
2. Strips `LIMIT 5` from shard SQL (GROUP BY present).
3. Sends `SELECT status, COUNT(*), SUM(amount), SUM(amount), COUNT(amount) FROM orders GROUP BY status` to each shard.
4. Merges partial group rows → global group sums.
5. Filters groups where `SUM(amount) > 100` (HAVING).
6. Sorts by `revenue DESC`.
7. Truncates to 5 rows.
8. Sends `RowDescription` + `DataRow` × ≤5 + `CommandComplete` to the client.

#### EXPLAIN SHARD PLAN FOR

```sql
-- Admin port (default 7433)
EXPLAIN SHARD PLAN FOR
  'SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status ORDER BY 2 DESC LIMIT 5';
```

```
 kind    | shard_index | shard_count | agg_type  | merge_strategy   | has_order_by | has_limit
---------+-------------+-------------+-----------+------------------+--------------+-----------
 SCATTER | -           | 2           | GROUP_AGG | GROUP+SORT+LIMIT | true         | true
```

`merge_strategy = GROUP+SORT+LIMIT` confirms all three post-merge phases are active.

For full details on the scatter-merge engine, merge algorithm, Prometheus metrics,
performance, and limitations, see [SCATTER_MERGE.md](SCATTER_MERGE.md).

---

## 11. Multi-Shard Transaction Coordination

### 11.1 Problem statement

Consider a transaction that first does a scatter UPDATE (all shards) and then a single-
shard SELECT.  If the SELECT resolves to a shard that was not included in the scatter,
it may read stale data, violating the read-your-writes guarantee.

### 11.2 Participation tracking

After a successful scatter write, call:

```c
keel_router_record_scatter_write(session, &scatter_plan);
```

This sets `session->has_scatter_write = true` and ORs the shard bitmask:

```c
session->scatter_shards_mask |= scatter_plan.participating_shards_mask;
```

`participating_shards_mask` is a 64-bit mask where bit `i` is set if shard `i` was
successfully reached during the scatter write.

### 11.3 Cross-transaction validation

On each subsequent single-shard query within the same transaction:

```
route_internal(router, ..., use_shard_filter=true, shard_index=N, ...):
    if session.has_scatter_write
       AND shard_index < 64
       AND NOT (session.scatter_shards_mask & (1 << shard_index)):
        return KEEL_ERR_SHARD_CROSS_TX  (-902)
```

This prevents a transaction that wrote to shards {0,1,2,3} from subsequently reading
from shard 4 (which has a different snapshot).

### 11.4 Cleanup

At COMMIT or ROLLBACK:

```c
keel_router_clear_scatter_participation(session);
/* resets: has_scatter_write = false, scatter_shards_mask = 0 */
```

### 11.5 Bitmask capacity

The participation mask is `uint64_t`, so the maximum cross-shard tracking capacity is
64 shards.  This matches `KEEL_SCATTER_MAX_SHARDS`.

---

## 12. Live Shard Migration

### 12.1 Use case

When the dataset grows, you may need to split shard 0 into shard 0 and shard 4 (by
adding a new server and moving half the rows).  KEEL supports this with zero
application downtime.

### 12.2 Migration state machine

```
     set_migration(src=0, dst=4)
NORMAL -----------------------------------------> MIGRATING
                                                      |
                                             (copy rows src->dst
                                              using application or
                                              external tool)
                                                      |
                                          clear_migration(table)
                                                      v
                                                    NORMAL
                                               (both shards now
                                                serve their halves)
```

### 12.3 Routing during migration

While `rule.state == KEEL_SHARD_STATE_MIGRATING` and the plan resolves to either
`migrate_src_shard` or `migrate_dst_shard`:

- **Write** -> SCATTER(2): sent to both `src_shard` and `dst_shard` (dual-write)
- **Read**  -> SINGLE to `dst_shard` (read-from-new ensures fresh data)

Writes to shards **not** involved in migration route normally.

### 12.4 Migration procedure (step-by-step)

```bash
# 1. Add new shard server to KEEL config
echo '[worker_group.app.servers]
  shard4 = host=pg-shard4 ... role=RW weight=100 shard_id=4' >> keel.ini

# 2. Activate migration mode
keel-admin "ALTER SHARD RULE users MIGRATING FROM 0 TO 4;"
# (or call keel_router_set_shard_migration(router, "users", 0, 4))

# 3. Copy rows from shard 0 to shard 4 (your migration tool / SQL script)
#    KEEL now dual-writes new mutations; copy can lag by a bounded amount

# 4. Wait for copy to finish (monitor lag)

# 5. Clear migration -> normal routing
keel-admin "ALTER SHARD RULE users CLEAR MIGRATION;"
# (or call keel_router_clear_shard_migration(router, "users"))

# 6. Update shard_count in rule
#    (hot-reload via SIGHUP or config change)
```

### 12.5 Migration limitations

- Only one migration (one src/dst pair) per rule at a time.
- `migrate_src_shard == migrate_dst_shard` is rejected with KEEL_ERR_INVALID_ARG.
- Row deletion during migration must be applied to **both** shards.
- No built-in progress tracking; external tooling must monitor copy lag.

---

## 13. Configuration Reference

### 13.1 Server declaration with shard_id

Every backend server must declare its shard membership:

```ini
[worker_group.app.servers]
# Shard 0
shard0-primary = host=pg0 port=5432 dbname=mydb user=keel password=pw role=RW weight=100 shard_id=0
shard0-replica = host=pg0r port=5432 dbname=mydb user=keel password=pw role=RO weight=80  shard_id=0

# Shard 1
shard1-primary = host=pg1 port=5432 dbname=mydb user=keel password=pw role=RW weight=100 shard_id=1
shard1-replica = host=pg1r port=5432 dbname=mydb user=keel password=pw role=RO weight=80  shard_id=1
```

`shard_id` is a 0-based index that must match a shard index in the shard rules.
Servers without `shard_id` (or with `shard_id` not referenced by any rule) participate
in non-sharded routing only.

### 13.2 Shard rule sections

```ini
[shard_rule.<table_name>]
column      = <column_name>    # required
shard_count = <N>              # required; must be > 0 and <= 64
strategy    = hash             # optional; "hash" (default) or "range"
```

Section name after `shard_rule.` is the table name (case-insensitive match at routing
time).

**HASH rule example:**

```ini
[shard_rule.users]
column      = id
shard_count = 8
strategy    = hash
```

**RANGE rule example:**

```ini
[shard_rule.users]
column      = id
shard_count = 4
strategy    = range
# Thresholds are set programmatically via keel_router_add_shard_rule_range().
# INI hot-reload reconstructs them from the stored threshold values.
```

> **Note:** RANGE thresholds are currently supplied programmatically via
> `keel_router_add_shard_rule_range()`.  The INI hot-reload path reconstructs the
> threshold values from the stored rule state; to change thresholds at runtime,
> use the API and then SIGHUP.  A future release will add direct INI threshold
> configuration (see Section 21).

### 13.3 Multiple rules

```ini
[shard_rule.users]
column      = id
shard_count = 4

[shard_rule.orders]
column      = order_id
shard_count = 4

[shard_rule.tenants]
column      = slug
shard_count = 16
strategy    = hash
```

### 13.4 Complete two-shard example

```ini
[keel]
log_level = 2

[prometheus]
enabled     = true
listen_addr = 0.0.0.0
port        = 9101

[worker_group.app]
bind_addr        = 0.0.0.0
bind_port        = 6432
num_workers      = 4
max_client_conns = 500
min_pool_size    = 4
max_pool_size    = 32
pool_mode        = transaction
auth_type        = trust
query_routing    = on

[worker_group.app.servers]
shard0 = host=pg-shard0 port=5432 dbname=appdb user=keel password=pw role=RW weight=100 shard_id=0
shard1 = host=pg-shard1 port=5432 dbname=appdb user=keel password=pw role=RW weight=100 shard_id=1

[shard_rule.users]
column      = id
shard_count = 2
strategy    = hash

[shard_rule.orders]
column      = user_id
shard_count = 2
strategy    = hash
```

### 13.5 Configuration key reference

| Key | Section | Type | Default | Description |
|-----|---------|------|---------|-------------|
| `shard_id` | `worker_group.*.servers` | integer >= 0 | — | Logical shard index for this server |
| `column` | `shard_rule.*` | string | — | **Required.** Shard key column name |
| `shard_count` | `shard_rule.*` | integer 1-64 | — | **Required.** Number of shards for this table |
| `strategy` | `shard_rule.*` | `hash`/`range` | `hash` | Mapping strategy |

---

## 14. Hot-Reload via SIGHUP

### 14.1 Trigger

```bash
kill -SIGHUP $(pidof keel)
# or
keel-admin "RELOAD;"
```

### 14.2 What gets reloaded

`keel_config_reload_shard_rules()` re-reads all `[shard_rule.*]` INI sections from the
config file on disk and applies changes to the router's in-memory registry:

- **New section** -> new rule added (`keel_router_add_shard_rule()`)
- **Existing section, same params** -> no-op (rule unchanged)
- **Existing section, different `shard_count` or `strategy`** -> rule overwritten,
  warning logged
- **Section removed** -> rule removed from registry
- **Invalid section** (missing `column` or invalid `shard_count`) -> skipped,
  `result.errors++`

### 14.3 Result counters

```c
typedef struct keel_reload_result {
    size_t rules_added;
    size_t rules_removed;
    size_t rules_unchanged;
    size_t errors;
} keel_reload_result_t;
```

### 14.4 Atomicity

The hot-reload is applied entry-by-entry within the router's flat array.  There is no
all-or-nothing transaction; if an error occurs mid-reload, already-applied changes are
not rolled back.  In practice this is safe because each rule is independent.

### 14.5 Impact on in-flight queries

Rules are read under the router's internal lock.  A routing decision in flight holds a
pointer to a rule copy (not a reference to the registry entry).  The reload overwrites
the registry entry but does not disturb in-flight pointers.  This is safe for the
current implementation where rules are stored as value types in a flat array.

---

## 15. Admin SQL Interface

KEEL's admin database (connect to the `keel` virtual database on the admin port)
exposes sharding information through SQL and command syntax.

### 15.1 List shard rules

```sql
-- Virtual table
SELECT * FROM shard_rules;
-- Returns: table | column | shard_count

-- Command form
SHOW SHARD RULES;
```

### 15.2 Explain routing plan

```sql
EXPLAIN SHARD PLAN FOR 'SELECT * FROM users WHERE id = 42';
-- Returns: kind | shard_index
-- kind: SINGLE | SCATTER | UNSUPPORTED
-- shard_index: 0-based (only for SINGLE)
```

Useful for verifying that a query routes where you expect.  Can be used with `$N`
parameters (bindings are not passed; PARAM keys yield SCATTER).

### 15.3 Example session

```
psql -h 127.0.0.1 -p 6433 -d keel -U keel

keel=# SHOW SHARD RULES;
  table  | column  | shard_count
---------+---------+-------------
 users   | id      |           4
 orders  | user_id |           4
(2 rows)

keel=# EXPLAIN SHARD PLAN FOR 'UPDATE users SET name = $1 WHERE id = $2';
   kind  | shard_index
---------+-------------
 SCATTER | -
(1 row)

keel=# EXPLAIN SHARD PLAN FOR 'SELECT * FROM users WHERE id = 99';
  kind   | shard_index
---------+-------------
 SINGLE  | 3
(1 row)
```

---

## 16. Prometheus Metrics

All routing metrics are exposed at `GET /metrics` on the Prometheus listener port.

### 16.1 Shard-specific metrics

| Metric | Type | Description |
|--------|------|-------------|
| `keel_router_shard_hits{shard="N"}` | counter | Queries routed to shard N via single routing |
| `keel_router_scatter_hits` | counter | Total scatter fan-outs dispatched |
| `keel_router_scatter_failed` | counter | Scatter shards with no available server |

### 16.2 General routing metrics

| Metric | Type | Description |
|--------|------|-------------|
| `keel_router_queries_total` | counter | All queries entering the router |
| `keel_router_routes_single` | counter | Queries resolved as single-shard |
| `keel_router_routes_scatter` | counter | Queries resolved as scatter |
| `keel_router_route_errors` | counter | KEEL_ERR_NOT_SUPPORTED + routing failures |
| `keel_router_timeouts` | counter | KEEL_ERR_QUERY_TIMEOUT events |
| `keel_router_servers_healthy` | gauge | Healthy backend servers (all shards) |

### 16.3 Alerting rules (recommended)

```yaml
# Scatter rate alert -- high scatter rate means missing shard predicates
- alert: KeelHighScatterRate
  expr: rate(keel_router_scatter_hits[5m]) / rate(keel_router_queries_total[5m]) > 0.1
  for: 10m
  annotations:
    summary: "More than 10% of queries are scattering (check shard key usage)"

# Shard unavailable
- alert: KeelScatterPartialFailure
  expr: rate(keel_router_scatter_failed[5m]) > 0
  for: 1m
  annotations:
    summary: "One or more shards have no available server"

# Hot shard -- one shard receiving >> 1/N of traffic
- alert: KeelShardHotspot
  expr: |
    max(keel_router_shard_hits) /
    avg(keel_router_shard_hits) > 2
  for: 5m
  annotations:
    summary: "Possible shard hotspot: one shard receiving 2x average traffic"
```

---

## 17. Connection Pool Integration

Each shard's server has its own `keel_connpool_t` connection pool, managed by the
`keel_connpool_registry_t`.  The sharding layer picks a server per shard via the
router, and the pool layer manages the actual TCP connections.

```
keel_router_dispatch_sql()
    -> keel_route_decision_t { server }
        -> keel_connpool_registry_get(server->name)
            -> keel_connpool_acquire()
                -> PQexec(conn, sql)
                    -> keel_connpool_release(conn)
```

Pool parameters are configured per `worker_group`:

```ini
[worker_group.app]
min_pool_size = 4     # pre-opened connections per server
max_pool_size = 32    # hard cap per server
pool_mode     = transaction   # session | transaction | statement
```

With 4 shards and `max_pool_size = 32`, the maximum total connection count to all
backends is `4 shards x 2 servers/shard (primary + replica) x 32 = 256`.

---

## 18. Risks and Corner Cases

### 18.1 Shard count change after data insertion

**Risk:** If you change `shard_count` from 4 to 8, the hash function produces
different shard indices for existing rows.  Any existing row will route to the wrong
shard until the data is physically moved.

**Mitigation:**
- Never change `shard_count` on a live, non-empty table without a full data migration.
- Use the migration workflow (Section 12) to add shards one at a time.
- KEEL logs a warning on shard count change during hot-reload.

### 18.2 Multi-row INSERT scatter

`INSERT INTO t (id) VALUES (1), (2), (3)` -- KEEL extracts the shard key from the
**first row only** (id=1 -> shard 1 % N).  Rows 2 and 3 may belong to different
shards.  If `id=1` -> shard 0 and `id=2` -> shard 1, row 2 is silently
sent to the wrong shard.

**Mitigation:**
- Use single-row INSERTs for sharded tables (one `INSERT ... VALUES(...)` per row).
- Use the application-level scatter loop for bulk inserts (split by shard key, one
  batch per shard).
- This limitation is tracked in the roadmap (Section 21).

### 18.3 Prepared statements with deferred binding

A prepared `SELECT * FROM users WHERE id = $1` has no literal value.  KEEL returns
SCATTER when params is NULL.  If the proxy executes a Bind message and the client
does not provide the shard key binding, the query scatters.

**Mitigation:**
- Always pass the full `keel_shard_bound_params_t` when executing prepared statements.
- The proxy dispatch loop must extract bindings from the PostgreSQL Bind message and
  pass them to `keel_router_dispatch_sql()`.

### 18.4 OR predicates on the shard key

`WHERE id = 1 OR id = 2` is not extracted as a shard predicate.  The query scatters.

**Mitigation:**
- Use `IN (1, 2)` (also scatters today, but tracked for future per-value fan-out).
- Decompose the application query into two separate single-shard queries.

### 18.5 IN-list routing

`WHERE id IN (1, 2, 3)` scatters today.  A future optimisation could decompose the IN
list into per-shard sub-lists and issue one query per affected shard.

### 18.6 Scatter latency amplification

A scatter query waits for all N shards to respond.  The observed latency is
`max(shard_0_latency, ..., shard_N-1_latency)`, not the average.  A single slow or
unavailable shard blocks the full scatter.

**Mitigation:**
- Set per-query timeouts (`keel_router_dispatch_sql_timed()`).
- Monitor `keel_router_scatter_failed` to detect persistent shard unavailability.
- Design the schema so scatter queries are rare (< 10% of traffic).

### 18.7 Cross-shard atomicity

KEEL does not implement 2PC between shards.  A scatter write to shards {0,1,2,3} can
partially succeed if one shard crashes mid-flight.  The application must handle partial
failure (read-repair, idempotent writes, sagas).

### 18.8 CTEs and writable CTEs

CTEs (`WITH`) always cause SCATTER — the shard key extractor guards against `WITH`
clauses.  The full SQL (including the CTE) is sent as-is to every shard.

For **writable CTEs** (`WITH inserted AS (INSERT INTO ...)`), the INSERT scatters to
all shards under 2PC.  This is usually not what you want.  If you need a single-shard
INSERT, use a plain `INSERT INTO` (no CTE wrapper).

For **read-only CTEs**, the CTE is evaluated independently on each shard.  Rows from
all shards are concatenated.  If rows from the sharded table are co-located (same
shard key), joins within the CTE will be correct.  If the CTE joins tables sharded
on different keys, cross-shard pairs will be silently missed.

### 18.9 RETURNING clause

`INSERT ... RETURNING id` and `UPDATE ... RETURNING *` -- KEEL routes these correctly
(INSERT uses the values clause, UPDATE uses the WHERE clause), but the RETURNING clause
is not parsed for shard routing purposes.  The query is forwarded to the correct shard
and the RETURNING result is passed through.

### 18.10 Schema-qualified table names

`SELECT * FROM public.users WHERE id = 42` -- the schema prefix (`public.`) may or may
not be stripped by the SQL parser.  If the parser preserves the schema prefix,
KEEL's case-insensitive table match against `"users"` will fail and the query will
SCATTER or be UNSUPPORTED.

**Mitigation:**
- Use unqualified table names in queries for sharded tables.
- Or configure the rule with the schema-qualified name if the parser preserves it.
- This is tracked as a known limitation (Section 21).

### 18.11 Window functions — per-shard values overwritten

When a query contains supported window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE,
PERCENT_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, running SUM/AVG), Phase F
overwrites the per-shard window column values with globally correct values computed
after merging all rows.

- **Before Phase F**: each shard returns its own row numbers (1..N_shard).  These
  values are incorrect for global ordering.
- **After Phase F**: the proxy overwrites the window column with globally correct
  values (1..N_total).

If the window query uses a shard-key predicate in WHERE (routes SINGLE), Phase F does
not run — PostgreSQL computes the window function natively on that one shard.

### 18.12 LAST_VALUE default frame

`LAST_VALUE(col) OVER (ORDER BY x)` uses the default window frame `ROWS BETWEEN
UNBOUNDED PRECEDING AND CURRENT ROW`.  This means `LAST_VALUE` returns the same
value as the current row's `col`, not the last value in the partition.

Use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` to get the true last
value:

```sql
SELECT col, LAST_VALUE(col) OVER (
    PARTITION BY category
    ORDER BY score DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS max_in_category
FROM products;
```

### 18.13 STRING_AGG and ARRAY_AGG across shards

`STRING_AGG(name, ', ')` and `ARRAY_AGG(col)` have no cross-shard merge
implementation.  Each shard computes its own aggregate and returns one row.  The
proxy concatenates these rows, so the client receives N rows (one per shard) rather
than a single merged result.

**Workarounds:**
- Post-process in the application (concatenate N string_agg values).
- Use `GROUP BY shard_key` so the aggregate is naturally single-shard.
- For JSONB: use `jsonb_agg` within a shard-pinned query.

### 18.14 Recursive CTEs and cross-shard hierarchies

`WITH RECURSIVE` CTEs are sent as-is to each shard.  The recursion is evaluated
per-shard, not cross-shard.  If a parent row is on shard 0 and its child row is on
shard 1, the recursive CTE on shard 0 will not find the child.

**Mitigation:** Co-locate parent and child rows by using the same shard key for the
parent ID.  For organisational hierarchies, store the entire tree in a single unsharded
table (no shard rule for that table).

---

## 19. Architectural Decisions and Rationale

### 19.1 In-process SQL parsing instead of query rewriting

**Decision:** Parse the SQL AST at the proxy using the internal `keel_sql_parse()`
engine.  Never rewrite the SQL text.

**Rationale:**
- **Zero-copy forwarding**: after the routing decision, the original SQL bytes are
  forwarded verbatim.  There is no string allocation or reconstruction.
- **No correctness edge cases from rewriting**: query rewriting is notoriously fragile
  (escaping, quoting, parameter indices).  Passing the original query avoids this.
- **Single parse for all decisions**: the AST is computed once per query and reused
  for read/write classification, shard key extraction, and query classification.
- **Predictable overhead**: the parser is arena-based and reset between queries.  No
  GC pressure.

### 19.2 xxHash64 for string keys

**Decision:** Use xxHash64 with seed=0 for string and UUID shard key mapping.

**Rationale:**
- **Speed**: xxHash64 processes ~10 GB/s; for typical UUID/string keys (< 100 bytes)
  the cost is a few nanoseconds.
- **Stability**: the algorithm is fully deterministic -- the same input always produces
  the same output, on any machine, on any OS.
- **Distribution quality**: passes all SMHasher tests; excellent avalanche.
- **Alternatives rejected**: FNV-1a (simpler, slower, lower quality); MurmurHash3
  (good quality, not endian-neutral); CityHash (Google-internal, less portable).

### 19.3 Flat array rule registry (not a hash map)

**Decision:** Store shard rules in a flat `keel_shard_rule_t[16]` array.

**Rationale:**
- **Locality**: all 16 rules fit in ~2 cache lines.  Hash-map lookup would incur
  pointer chasing.
- **Simplicity**: no allocation, no rehashing, trivially serialisable.
- **Practical limit**: 16 sharded tables per router is a reasonable upper bound for
  most deployments.  OLTP schemas rarely have more than a handful of hot sharded
  tables.
- **Linear scan cost**: iterating 16 rules to find the first match costs < 100 ns at
  routing throughput (millions of queries/second), which is negligible.

### 19.4 Plan outcome as enum (not error codes)

**Decision:** `keel_shard_plan()` never returns an error; all outcomes are encoded
in the `keel_shard_plan_t` enum (SINGLE / SCATTER / UNSUPPORTED).

**Rationale:**
- The caller always needs to handle all three cases.  A function that returns an error
  AND a plan would require the caller to check both.
- UNSUPPORTED is a valid, expected outcome (DDL, non-sharded queries).  It should not
  look like an error.
- This aligns with the single-entry-point philosophy: one call, one structured result.

### 19.5 AND-only predicate extraction

**Decision:** Only walk AND conjunctions.  OR, IN, BETWEEN, subquery predicates yield
SCATTER (not an error).

**Rationale:**
- **Correctness over efficiency**: incorrectly routing an OR predicate to one shard
  when rows exist on multiple shards is a data-loss bug.  SCATTER is always safe.
- **Future extensibility**: OR/IN decomposition can be added later (routing to a
  computed set of shards) without breaking the existing SCATTER path.

### 19.6 Shard state as per-rule field (not per-session)

**Decision:** Migration state (`KEEL_SHARD_STATE_MIGRATING`, `migrate_src_shard`,
`migrate_dst_shard`) is stored on the `keel_shard_rule_t` itself.

**Rationale:**
- Migration is a global router state, not per-connection.  Every session must see the
  same migration behaviour.
- Storing it on the rule makes the intercept free: the routing hot path already loads
  the rule; the migration check is a branch on one field.

### 19.7 Bitmask for scatter participation (not a list)

**Decision:** Use `uint64_t scatter_shards_mask` to track scatter write participants.

**Rationale:**
- **O(1) set, O(1) test**: bit operations are a single instruction.
- **No allocation**: the bitmask lives in the session struct on the stack.
- **64-shard cap**: aligns with `KEEL_SCATTER_MAX_SHARDS = 64`.

---

## 20. Implementation State and Feature Map

| Feature | Phase / Tier | Status | File |
|---------|-------------|--------|------|
| SELECT shard-key extraction (literal + alias) | Phase 1 | Complete | sharding.c |
| INT64 / STRING / BOOL / PARAM key kinds | Phase 1 | Complete | sharding.c |
| INSERT shard-key extraction | Phase 2 | Complete | sharding.c |
| UPDATE shard-key extraction | Phase 2 | Complete | sharding.c |
| DELETE shard-key extraction | Phase 2 | Complete | sharding.c |
| Bound-parameter ($N) resolution | Phase 3 | Complete | sharding.c |
| `keel_shard_plan_t` / `keel_shard_plan()` | Phase 4 | Complete | sharding.c |
| Router-embedded shard rule registry | Phase 5 | Complete | router_weighted.c |
| `keel_router_scatter_servers()` | Phase 6 | Complete | router_weighted.c |
| Rule persistence via INI `[shard_rule.*]` | Tier 1 | Complete | router_weighted.c |
| `keel_router_dispatch_sql()` combined dispatch | Tier 1 | Complete | router_weighted.c |
| Scatter result aggregation (`keel_route_agg_t`) | Tier 2 | Complete | router_weighted.c |
| Per-shard routing counters | Tier 2 | Complete | router_weighted.c |
| Admin virtual table (shard_rules, EXPLAIN SHARD PLAN) | Tier 2 | Complete | admin.c |
| RANGE strategy (`KEEL_SHARD_STRATEGY_RANGE`) | Tier 3 | Complete | sharding.c |
| Multi-shard transaction coordinator | Tier 3 | Complete | router_weighted.c |
| Shard migration state (dual-write + read-from-new) | Tier 3 | Complete | router_weighted.c |
| Connection pool per shard (`keel_connpool_t`) | Tier 4 | Complete | connpool.c |
| Proxy session (`keel_client_session_t`) | Tier 4 | Complete | proxy_session.c |
| Config hot-reload (`keel_config_reload_shard_rules`) | Tier 4 | Complete | router_weighted.c |
| Prometheus metrics | Tier 4 | Complete | router_weighted.c |
| Query timeout (`keel_router_dispatch_sql_timed`) | Tier 5 | Complete | router_weighted.c |
| Multi-table UPDATE FROM shard extraction | Tier 5 | Complete | sharding.c |
| Hot-reload change detection + result counters | Tier 5 | Complete | router_weighted.c |
| **Window functions Phase F** (ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, running SUM/AVG) | Tier 5 | **Complete** | engine_scatter.c |
| **PARTITION BY** in window functions (per-partition global recompute) | Tier 5 | **Complete** | engine_scatter.c |
| **CTE (WITH clause) scatter pass-through** | Tier 5 | **Complete** | sharding.c (guard) |
| **JSONB / array / composite type pass-through** | Tier 5 | **Complete** | engine_scatter.c |
| Scatter-merge engine Phase D (scalar aggs) | Tier 5 | Complete | engine_scatter.c |
| Scatter-merge engine Phase E (GROUP BY) | Tier 5 | Complete | engine_scatter.c |
| Scatter-merge engine Phase H (HAVING) | Tier 5 | Complete | engine_scatter.c |
| Scatter-merge engine Phase C (ORDER BY sort, external spill) | Tier 5 | Complete | engine_scatter.c |
| Scatter-merge engine Phase L (LIMIT/OFFSET) | Tier 5 | Complete | engine_scatter.c |
| COUNT(DISTINCT) cross-shard deduplication | Tier 5 | Complete | engine_scatter.c |

**Test coverage:** 344+ sharding assertions across `test_sharding.c`, `test_shard_hot_reload.c`,
and `test_scatter_window.c`; plus 139 end-to-end assertions in
`tests/e2e/test_sharding_comprehensive.py`.

---

## 21. Roadmap — What Remains

These items are tracked but not yet implemented.

### 21.1 Multi-row INSERT routing

**Current:** Only the first row's shard key is extracted.  
**Goal:** Detect when all rows in a multi-row `VALUES` list hash to the same shard and
route to that shard; otherwise split into per-shard `INSERT` batches.  
**Complexity:** Medium -- requires query rewriting for the split path.

### 21.2 IN-list decomposition

**Current:** `WHERE id IN (1, 2, 3)` scatters.  
**Goal:** Decompose the IN list by shard, issue one query per shard with only that
shard's values, merge results.  
**Complexity:** High -- requires query rewriting + result merging.

### 21.3 OR predicate routing

**Current:** `WHERE id = 1 OR id = 2` scatters.  
**Goal:** Same as IN-list decomposition.

### 21.4 Range strategy INI threshold configuration

**Current:** RANGE thresholds are set via API only; INI specifies only `strategy=range`.  
**Goal:** Add `thresholds = 1000000, 2000000, 3000000` (or similar) to INI config.  
**Complexity:** Low -- config parser extension.

### 21.5 Schema-qualified table name matching

**Current:** `SELECT * FROM public.users WHERE id=1` may not match rule `users`.  
**Goal:** Strip schema prefix (or allow rule table to include schema) when matching.  
**Complexity:** Low -- parser/comparison fix.

### 21.6 Cross-shard JOIN routing

**Current:** JOINs -> UNSUPPORTED.  
**Goal:** If both joined tables use the same shard key and the join predicate equates
their keys, route to a single shard.  
**Complexity:** Very high -- requires multi-table AST analysis.

### 21.7 STRING_AGG / ARRAY_AGG cross-shard merge

**Current:** Each shard returns its own aggregate; no cross-shard merge.  
**Goal:** Merge `STRING_AGG` and `ARRAY_AGG` results across shards in the scatter-merge
engine.  
**Complexity:** Medium -- requires ordered-merge of text/array values.

### 21.8 CUME_DIST and NTH_VALUE in Phase F

**Current:** `CUME_DIST()` and `NTH_VALUE(col, n)` are not handled by Phase F.  
**Goal:** Add Phase F support for `CUME_DIST` and `NTH_VALUE`.  
**Complexity:** Low-medium -- extend `keel_pg_result_window_compute()`.

### 21.9 Two-phase commit for cross-shard writes

**Current:** No 2PC; partial scatter write failure is possible.  
**Goal:** Optional 2PC support using PostgreSQL's `PREPARE TRANSACTION` /
`COMMIT PREPARED`.  
**Complexity:** High -- requires transaction coordinator integration.

### 21.10 Consistent hashing (virtual nodes)

**Current:** Modulo hashing -- shard count change requires full data migration.  
**Goal:** Consistent hash ring with virtual nodes so adding a shard only migrates 1/N
of the data.  
**Complexity:** High -- hash function change (breaks existing deployments).

### 21.11 Shard migration progress tracking

**Current:** No built-in progress counter during migration.  
**Goal:** Track estimated row count remaining and expose via admin SQL.  
**Complexity:** Medium -- requires bookkeeping tied to external copy tool.

### 21.12 Maximum shard count increase

**Current:** 64 shards (`KEEL_SCATTER_MAX_SHARDS`), limited by `uint64_t` bitmask.  
**Goal:** Support > 64 shards by replacing the bitmask with a dynamic bitset.  
**Complexity:** Low-medium -- data structure change in session.

### 21.13 Per-shard connection pool for scatter

**Current:** Scatter engine opens a new TCP connection per shard per query.  
**Goal:** Persistent connection pool per shard reused across scatter queries.  
**Complexity:** Medium -- lifecycle management for per-shard pool in scatter engine.

---

## 22. Full API Reference

### include/keel/core/sharding.h

#### Types

| Type | Description |
|------|-------------|
| `keel_shard_key_kind_t` | `NONE`, `INT64`, `STRING`, `BOOL`, `PARAM` |
| `keel_shard_strategy_t` | `KEEL_SHARD_STRATEGY_HASH` (0) or `KEEL_SHARD_STRATEGY_RANGE` (1) |
| `keel_shard_state_t` | `KEEL_SHARD_STATE_NORMAL` (0) or `KEEL_SHARD_STATE_MIGRATING` (1) |
| `keel_shard_rule_t` | Table + column + shard_count + strategy + thresholds + migration state |
| `keel_shard_key_t` | Extracted key: kind + table + column + union{int64, string, bool, param_index} |
| `keel_shard_bound_params_t` | Array of up to `KEEL_SHARD_MAX_PARAMS` (64) resolved `$N` values |
| `keel_shard_plan_kind_t` | `SINGLE` / `SCATTER` / `UNSUPPORTED` |
| `keel_shard_plan_t` | `kind` + `shard_index` (valid only for SINGLE) |

#### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `KEEL_SHARD_MAX_PARAMS` | 64 | Maximum `$N` parameter count |
| `KEEL_SHARD_RANGE_MAX_THRESHOLDS` | 64 | Maximum RANGE thresholds per rule |

#### Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `keel_shard_extract_key_ast` | `(ast, rule, key_out) -> keel_error_t` | Extract shard key from pre-parsed AST |
| `keel_shard_extract_key_sql` | `(sql, rule, key_out, arena) -> keel_error_t` | Parse SQL then extract key |
| `keel_shard_map_key` | `(key, shard_count, idx_out) -> keel_error_t` | HASH mapping, no rule |
| `keel_shard_map_key_rule` | `(key, rule, idx_out) -> keel_error_t` | HASH or RANGE mapping per rule |
| `keel_shard_map_key_bound` | `(key, params, shard_count, idx_out) -> keel_error_t` | Resolve `$N` then HASH map |
| `keel_shard_map_key_bound_rule` | `(key, params, rule, idx_out) -> keel_error_t` | Resolve `$N` then HASH/RANGE map |
| `keel_shard_plan` | `(sql, rule, params, arena, plan) -> void` | Full parse->extract->bind->map->plan |

---

### include/keel/core/router.h -- sharding extensions

#### Rule registry

| Function | Description |
|----------|-------------|
| `keel_router_add_shard_rule(router, table, col, N)` | Register HASH rule (overwrites existing) |
| `keel_router_add_shard_rule_range(router, table, col, thresholds, N)` | Register RANGE rule |
| `keel_router_remove_shard_rule(router, table)` | Remove rule by table name |
| `keel_router_get_shard_rule(router, table)` | Lookup by name -> `const keel_shard_rule_t*` |
| `keel_router_get_shard_rule_at(router, idx)` | Lookup by index (for iteration) |
| `keel_router_shard_rule_count(router)` | Count of registered rules |

#### Planning

| Function | Description |
|----------|-------------|
| `keel_router_plan_sql(router, sql, params, plan)` | Try all rules; first non-UNSUPPORTED wins |
| `keel_router_plan_sharded_sql(router, sql, rule, params, plan)` | Plan against one explicit rule |
| `keel_router_route_sharded_sql(router, sql, session, rule, decision)` | Route SINGLE (literals only) |
| `keel_router_route_sharded_sql_bound(router, sql, session, rule, params, decision)` | Route SINGLE with `$N` |

#### Scatter

| Function / Type | Description |
|-----------------|-------------|
| `KEEL_SCATTER_MAX_SHARDS` (64) | Maximum shards per scatter |
| `keel_scatter_plan_t` | `decisions[64]`, `count`, `failed`, `participating_shards_mask` |
| `keel_router_scatter_servers(router, session, rule, is_write, out)` | Resolve N shard decisions |

#### Combined dispatch

| Function / Type | Description |
|-----------------|-------------|
| `keel_dispatch_kind_t` | `KEEL_DISPATCH_SINGLE` or `KEEL_DISPATCH_SCATTER` |
| `keel_dispatch_result_t` | Tagged union: `kind` + `single` or `scatter` |
| `keel_router_dispatch_sql(router, sql, session, params, is_write, out)` | Plan + route (handles migration) |
| `keel_router_dispatch_sql_timed(router, sql, session, params, is_write, timeout, out)` | Dispatch with deadline |

#### Scatter aggregation

| Function / Type | Description |
|-----------------|-------------|
| `keel_route_merge_fn` | `void (*)(keel_route_agg_t*, size_t shard_index, const void* rows, size_t n, void* ctx)` |
| `keel_route_agg_t` | `total_rows`, `shards_completed`, `shards_failed`, `merge`, `user_ctx`, `data` |
| `keel_route_agg_init(result, merge, ctx)` | Initialise aggregation state |
| `keel_route_agg_feed(result, shard_index, rows, n)` | Deliver per-shard rows; calls merge or increments failed |

#### Transaction coordination

| Function | Description |
|----------|-------------|
| `keel_router_record_scatter_write(session, plan)` | Record scatter write participation |
| `keel_router_clear_scatter_participation(session)` | Reset bitmask at COMMIT/ROLLBACK |

#### Migration

| Function | Description |
|----------|-------------|
| `keel_router_set_shard_migration(router, table, src, dst)` | Enter MIGRATING state |
| `keel_router_clear_shard_migration(router, table)` | Return to NORMAL state |

#### Statistics

| Function / Type | Description |
|-----------------|-------------|
| `keel_router_stats_t` | Counters: queries, single/scatter routes, per-shard hits, timeouts, errors |
| `keel_router_get_stats(router, stats)` | Copy stats out |
| `keel_router_reset_stats(router)` | Zero all counters |
| `keel_router_write_prometheus(router, buf, len)` | Prometheus text format |

---

### include/keel/core/config_reload.h

| Symbol | Description |
|--------|-------------|
| `keel_reload_result_t` | `rules_added`, `rules_removed`, `rules_unchanged`, `errors` |
| `keel_config_reload_shard_rules(config, router, result)` | Re-read `[shard_rule.*]` sections and update router |

---

### Error codes

| Code | Value | Description |
|------|-------|-------------|
| `KEEL_OK` | 0 | Success |
| `KEEL_ERR_NOT_FOUND` | — | No shard predicate found (caller may scatter) |
| `KEEL_ERR_NOT_SUPPORTED` | — | Statement not shard-routable (DDL, JOIN, wrong table) |
| `KEEL_ERR_INVALID_ARG` | — | NULL input or zero shard_count |
| `KEEL_ERR_OVERFLOW` | — | shard_count > KEEL_SCATTER_MAX_SHARDS |
| `KEEL_ERR_SQL_PARSE` | — | SQL could not be parsed |
| `KEEL_ERR_SHARD_CROSS_TX` | -902 | Single-shard query targeting non-participating shard |
| `KEEL_ERR_QUERY_TIMEOUT` | -901 | Dispatch exceeded time budget |
