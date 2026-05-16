# KEEL Scatter-Merge Engine

This document is the primary reference for KEEL's scatter-merge aggregation engine.
It covers architecture, the merge algorithm, supported SQL, configuration, performance
characteristics, error behavior, and a complete worked example.

**Related:** [SHARDING.md](SHARDING.md) · [QUERY_FLOW.md](QUERY_FLOW.md) · [OPERATIONS.md](OPERATIONS.md) · [TESTING.md](TESTING.md)

> **Last updated:** 2026-05-09 — Phase F window function support, CTE behavior,
> JSONB pass-through, NULL semantics, and corner cases documented.

---

## Table of Contents

1. [What Is Scatter-Merge?](#1-what-is-scatter-merge)
2. [Architecture](#2-architecture)
3. [Query Lifecycle](#3-query-lifecycle)
4. [Merge Algorithm — Phase-by-Phase](#4-merge-algorithm--phase-by-phase)
5. [Supported SQL](#5-supported-sql)
6. [Window Functions (Phase F)](#6-window-functions-phase-f)
7. [CTEs and Subqueries](#7-ctes-and-subqueries)
8. [JSONB, Arrays, and Composite Types](#8-jsonb-arrays-and-composite-types)
9. [LIMIT and OFFSET Semantics](#9-limit-and-offset-semantics)
10. [Two-Phase Commit (Scatter Writes)](#10-two-phase-commit-scatter-writes)
11. [EXPLAIN SHARD PLAN FOR](#11-explain-shard-plan-for)
12. [NULL Handling and Edge Cases](#12-null-handling-and-edge-cases)
13. [Configuration Reference](#13-configuration-reference)
14. [Performance Characteristics](#14-performance-characteristics)
15. [Prometheus Metrics](#15-prometheus-metrics)
16. [Error Behavior](#16-error-behavior)
17. [Limitations and Unsupported Patterns](#17-limitations-and-unsupported-patterns)
18. [Corner Cases Reference](#18-corner-cases-reference)
19. [End-to-End Tutorial](#19-end-to-end-tutorial)

---

## 1. What Is Scatter-Merge?

When KEEL routes a query that targets a sharded table and no shard-key predicate
narrows the query to a single shard, it executes a **scatter**: it fans the SQL
out to every shard in parallel, collects all partial result sets, and **merges**
them into a single, coherent response before returning anything to the client.

The client never sees individual shard results. From its perspective, the query
behaves exactly like a single-backend query — it sends one SQL statement and
receives one result set over the standard PostgreSQL wire protocol.

### Example

```sql
-- Two shards hold the `orders` table (shard 0: id % 2 = 0, shard 1: id % 2 = 1)
SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders
GROUP BY status
ORDER BY total DESC
LIMIT 10;
```

KEEL sends this query to both shards, receives partial group rows from each,
sums the `cnt` and `total` columns per status group, sorts globally, and
returns the final top-10 rows to the client.

---

## 2. Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │                   CLIENT                    │
                        │   SELECT status, COUNT(*), SUM(amount) ...  │
                        └─────────────────────┬───────────────────────┘
                                              │  PostgreSQL wire v3
                                              ▼
                        ┌─────────────────────────────────────────────┐
                        │                    KEEL                     │
                        │                                             │
                        │  ┌──────────────┐   ┌────────────────────┐ │
                        │  │  SQL Analyzer │   │  Shard Router      │ │
                        │  │  (analyzer.c)│──▶│  (router.h)        │ │
                        │  └──────────────┘   └────────┬───────────┘ │
                        │                              │ SCATTER plan │
                        │                    ┌─────────▼───────────┐ │
                        │                    │  keel_dispatch_      │ │
                        │                    │  result_t  (dr)      │ │
                        │                    │  nagg_specs          │ │
                        │                    │  ngroup_key_cols     │ │
                        │                    │  norder_keys         │ │
                        │                    │  limit_count/offset  │ │
                        │                    └─────────┬───────────┘ │
                        │                              │              │
                        │              ┌───────────────▼────────────┐│
                        │              │  engine_scatter_execute()  ││
                        │              │  (engine_scatter.c)        ││
                        │              └──┬────────┬────────┬───────┘│
                        │                 │        │        │         │
                        └─────────────────┼────────┼────────┼─────────┘
                                          │        │        │
                             TCP connect  │        │        │  TCP connect
                             + SCRAM auth │        │        │  + SCRAM auth
                                          │        │        │
                          ┌───────────────▼─┐   ┌─▼──────────────────┐
                          │   SHARD 0        │   │   SHARD 1           │
                          │  (PostgreSQL)    │   │  (PostgreSQL)       │
                          │  partial rows    │   │  partial rows       │
                          └───────────────┬─┘   └─┬────────────────────┘
                                          │        │
                                          ▼        ▼
                          ┌──────────────────────────────────────────┐
                          │         Merge Pipeline (in KEEL)          │
                          │                                           │
                          │  Phase D: scalar aggregates               │
                          │  Phase D: COUNT DISTINCT                  │
                          │  Phase E: GROUP BY aggregation            │
                          │  Phase F: window function recompute       │
                          │  Phase H: HAVING filter                   │
                          │  Phase C: ORDER BY merge-sort             │
                          │  Phase L: global LIMIT / OFFSET           │
                          └───────────────────────────────────────────┘
                                              │
                                              ▼
                                     RowDescription + DataRow × N
                                     + CommandComplete + ReadyForQuery
                                              │
                                              ▼
                                          CLIENT
```

### Key Components

| Component | File | Role |
|-----------|------|------|
| `keel_router_dispatch_sql()` | `src/core/router_weighted.c` | Analyzes SQL, produces `keel_dispatch_result_t` |
| `keel_engine_scatter_execute()` | `src/engine/engine_scatter.c` | Fan-out, per-shard exec, merge pipeline |
| `keel_2pc_coord_*` | `src/core/scatter_2pc.c` | Two-phase commit coordinator for scatter writes |
| `show_shard_plan()` | `src/admin/admin.c` | `EXPLAIN SHARD PLAN FOR` virtual table |
| `keel_router_record_scatter_merge_ns()` | `src/core/router_weighted.c` | Histogram recording |
| `keel_router_write_prometheus()` | `src/core/router_weighted.c` | `/metrics` endpoint |

---

## 3. Query Lifecycle

```
Client sends Query message
        │
        ▼
keel_router_dispatch_sql(router, sql, session, stats, read_only, &dr)
        │
        ├── SQL analysis: type, shard key, agg specs, group keys, order keys,
        │   LIMIT/OFFSET, HAVING predicates, window function guard
        │
        ├── Plan kind == SCATTER?  ──No──▶  single-shard path (not scatter-merge)
        │
        ▼ Yes
keel_engine_scatter_execute(
    server_pool, server_pool_count,
    sql,
    &dr,           ← dispatch result (agg specs, group keys, ordering, limit)
    client_fd,     ← write results directly to client
    max_mem_bytes, ← memory cap for in-memory sort (spills to spill_dir)
    spill_dir      ← spill directory for large ORDER BY sorts
)
        │
        ├─ For each shard i in [0, dr.shard_count):
        │       sc_exec_shard(pool[i], sql_for_shard, &partial_result[i])
        │            │
        │            ├─ blocking TCP connect (SCATTER_CONNECT_TIMEOUT_MS = 5 000 ms)
        │            ├─ SCRAM-SHA-256 auth
        │            ├─ send Simple Query message
        │            ├─ recv RowDescription + DataRow* + CommandComplete
        │            └─ store rows in keel_pg_result_t
        │
        ├─ Merge pipeline (see §4)
        │
        └─ Encode merged rows → client_fd (RowDescription + DataRow × N +
                                           CommandComplete + ReadyForQuery)
```

### SQL Rewriting for Shards

When `GROUP BY` is present together with `LIMIT` or `OFFSET`, KEEL must **strip**
the LIMIT/OFFSET from the per-shard SQL. Otherwise each shard would truncate its
partial groups and the global merge would silently lose groups.

`sc_strip_limit_offset()` in `engine_scatter.c` performs a token-level rewrite of
the SQL string before forwarding it to each shard. The global LIMIT/OFFSET is still
enforced after the merge.

---

## 4. Merge Algorithm — Phase-by-Phase

### Phase D — Scalar Aggregates

**Triggered when:** `dr.nagg_specs > 0 && dr.ngroup_key_cols == 0`

All shards return a single aggregate row each. KEEL combines them by applying
the aggregate function over the partial values:

| Aggregate | Merge Operation |
|-----------|-----------------|
| `COUNT(*)` / `COUNT(col)` | `SUM` of shard counts |
| `SUM(col)` | `SUM` of shard sums |
| `MIN(col)` | `MIN` of shard minimums |
| `MAX(col)` | `MAX` of shard maximums |
| `AVG(col)` | Rewritten internally: `SUM(col) / SUM(COUNT(col))` — shards return `SUM` + `COUNT`, KEEL divides at the end |
| `COUNT(DISTINCT col)` | Hash-deduplicated across all shard values (see below) |

### Phase D — COUNT DISTINCT

**Triggered when:** `dr.requires_count_distinct == true`

KEEL collects all `(shard_index, value)` pairs from every shard, builds a
de-duplication hash table, and counts unique values across all shards. This
is accurate regardless of whether the same value appears on multiple shards.

### Phase E — GROUP BY Aggregation

**Triggered when:** `dr.ngroup_key_cols > 0`

1. Collect all partial-group rows from all shards.
2. Build a group hash table keyed on the group key columns.
3. For each incoming row, merge its aggregate columns into the matching group
   entry using the same per-aggregate rules as Phase D.
4. Emit one output row per group after all shards are processed.

### Phase H — HAVING Filter

**Triggered when:** `dr.nhaving_preds > 0`

Applied after Phase E. Each merged group row is evaluated against the HAVING
predicates. Rows that do not satisfy the filter are discarded before the
result is sent to the client.

### Phase C — ORDER BY Merge-Sort

**Triggered when:** `dr.norder_keys > 0`

After aggregation, the merged rows are sorted using a comparison function
built from `dr.order_keys[]`. The sort respects `ASC`/`DESC` and `NULLS
FIRST`/`NULLS LAST` semantics. For large result sets that exceed
`max_mem_bytes`, rows are spilled to `spill_dir` and the final sort is a
multi-way external merge.

### Phase L — Global LIMIT / OFFSET

**Triggered when:** `dr.limit_count > 0 || dr.limit_offset > 0`

Applied after Phase C. Only rows in the window `[offset, offset+count)` are
forwarded to the client. When `GROUP BY` is present, LIMIT/OFFSET was already
stripped from the per-shard SQL (see §3), so this is a pure post-merge
truncation.

### Phase F — Window Function Global Recomputation

**Triggered when:** `dr.nwindow_col_specs > 0`

Phase F runs **after Phase E (GROUP BY) and before Phase H (HAVING)**. It
takes the globally merged, sorted row set and recomputes window function output
columns from scratch, overwriting the per-shard window values (which are
incorrect because each shard only sees its local data subset).

See §6 for full detail, supported functions, and worked examples.

---

## 5. Supported SQL

### Aggregate Functions

| Function | Scatter-merge support | Notes |
|----------|-----------------------|-------|
| `COUNT(*)` | ✅ Full merge | SUM of per-shard counts |
| `COUNT(col)` | ✅ Full merge | Non-NULL counting, cross-shard |
| `COUNT(DISTINCT col)` | ✅ Full merge | Hash deduplication across all shards |
| `SUM(col)` | ✅ Full merge | Numeric types |
| `AVG(col)` | ✅ Full merge | Internal `SUM+COUNT` rewrite; divided at end |
| `MIN(col)` | ✅ Full merge | Any comparable type |
| `MAX(col)` | ✅ Full merge | Any comparable type |
| `ROW_NUMBER() OVER (...)` | ✅ Phase F global recompute | Globally correct sequential numbers |
| `RANK() OVER (...)` | ✅ Phase F global recompute | Ties get same rank; gaps after tie groups |
| `DENSE_RANK() OVER (...)` | ✅ Phase F global recompute | Ties share rank; no gaps between groups |
| `NTILE(n) OVER (...)` | ✅ Phase F global recompute | PostgreSQL bucket distribution semantics |
| `PERCENT_RANK() OVER (...)` | ✅ Phase F global recompute | (rank-1)/(N-1) after global sort |
| `LAG(col, n) OVER (...)` | ✅ Phase F pass-through | Recomputed after all shard rows collected |
| `LEAD(col, n) OVER (...)` | ✅ Phase F pass-through | Recomputed after all shard rows collected |
| `FIRST_VALUE(col) OVER (...)`| ✅ Phase F pass-through | Uses merged + sorted global result |
| `LAST_VALUE(col) OVER (...)`| ✅ Phase F pass-through | Requires UNBOUNDED FOLLOWING frame |
| Running `SUM(col) OVER (...)` | ✅ Phase F pass-through | Running totals recomputed globally |
| `PARTITION BY` (any window) | ✅ Phase F per-partition | Partitions formed from globally merged rows |
| `STRING_AGG(col, sep)` | ⚠️ Per-shard only | Each shard returns its own aggregate row; no cross-shard merge — see §17 |
| `ARRAY_AGG(col)` | ⚠️ Per-shard only | Each shard returns its own aggregate row; no cross-shard merge — see §17 |
| `jsonb_agg(col)` | ⚠️ Per-shard only | Each shard returns its own JSON aggregate |
| `PERCENTILE_CONT(f)` | ⚠️ Per-shard only | Percentile over per-shard subset, not global |
| `PERCENTILE_DISC(f)` | ⚠️ Per-shard only | Percentile over per-shard subset, not global |

### Clauses and Constructs

| Clause | Scatter support | Notes |
|--------|-----------------|-------|
| `WHERE <shard_key> = N` | ✅ SINGLE route | No scatter — routes to one shard |
| `WHERE <expr>` (no shard key) | ✅ SCATTER + per-shard filter | Full scatter with filter pushed to shards |
| `WHERE <expr>` with `IN (...)` | ✅ SCATTER | IN is not decomposed; always scatters |
| `WHERE <expr>` with `BETWEEN` | ✅ SCATTER | Always scatters |
| `WHERE <expr>` with `LIKE` | ✅ SCATTER | Always scatters |
| `WHERE <expr>` with `OR` | ✅ SCATTER | OR prevents single-shard routing |
| `WHERE <col> IS NULL` | ✅ SCATTER | NULL checks always scatter |
| `GROUP BY col, ...` | ✅ Full merge | Multi-column keys; partial groups merged cross-shard |
| `HAVING <predicate>` | ✅ Post-merge filter | Applied to globally merged groups |
| `ORDER BY col [ASC\|DESC]` | ✅ Global sort | Multi-key; NULLS FIRST/LAST respected |
| `ORDER BY col NULLS LAST\|FIRST` | ✅ | Propagated to Phase C global sort |
| `LIMIT n` | ✅ Global, post-merge | Pushed to shards only when no GROUP BY |
| `OFFSET n` | ✅ Global, post-merge | Always post-merge |
| `WITH <cte> AS (...)` (read-only) | ✅ SCATTER pass-through | Full CTE sent to each shard; rows concatenated — see §7 |
| `WITH RECURSIVE <cte> AS (...)` | ✅ SCATTER pass-through | Recursive CTE evaluated on each shard; rows concatenated |
| Subquery in `SELECT` (scalar) | ✅ SCATTER pass-through | Each shard evaluates; rows concatenated |
| Subquery in `WHERE` (`EXISTS`) | ✅ SCATTER pass-through | Each shard evaluates; rows concatenated |
| Subquery in `FROM` (derived table) | ✅ SCATTER pass-through | Sent as-is to each shard |
| `UNION ALL` | ✅ SCATTER pass-through | Both sides evaluated per shard; rows concatenated |
| `RETURNING` (INSERT/UPDATE/DELETE) | ✅ SINGLE or SCATTER | Returning rows from each shard concatenated |
| `ON CONFLICT DO UPDATE` (UPSERT) | ✅ SINGLE route | Shard key must be in VALUES; routes to correct shard |
| `INSERT INTO t (col) VALUES (v1), (v2)` | ⚠️ First-row routing | Multi-row VALUES: routing decided by first row only — see §17 |
| Cross-shard JOINs | ❌ Not supported | JOIN across two sharded tables routes to UNSUPPORTED; see §17 |
| `CREATE TABLE` / DDL | ❌ Pass to default backend | DDL bypasses shard routing entirely |

---

## 6. Window Functions (Phase F)

### Why Per-Shard Window Values Are Wrong

When a query includes a window function such as `ROW_NUMBER() OVER (ORDER BY
score DESC)`, each shard computes it against its local data subset only. With 2
shards holding rows with scores `{100, 80, 60}` and `{90, 70, 50}` respectively:

- Shard 0 returns: `ROW_NUMBER` values 1, 2, 3 (local order)
- Shard 1 returns: `ROW_NUMBER` values 1, 2, 3 (local order)

After concatenation the client would see row numbers 1-3 twice — both wrong and
duplicated. Phase F fixes this.

### How Phase F Works

1. The proxy scatter-merges all rows from all shards as normal.
2. For each window function column in `dr.window_col_specs[]`:
   a. Sort the merged result by the window's `ORDER BY` key (per partition).
   b. Walk the sorted rows and rewrite the window column value in each row with
      the globally correct value.
3. The original per-shard window values are discarded entirely.
4. Phase C (ORDER BY) and Phase L (LIMIT) run afterwards on the corrected data.

The implementation is `keel_pg_result_window_compute()` called from
`engine_scatter.c` Phase F.

### Supported Window Functions

| Function | Type | Notes |
|----------|------|-------|
| `ROW_NUMBER()` | Global recompute | Unique sequential number 1..N regardless of ties |
| `RANK()` | Global recompute | Tied rows share rank; gaps exist after tie groups |
| `DENSE_RANK()` | Global recompute | Tied rows share rank; no gaps between groups |
| `NTILE(n)` | Global recompute | Rows distributed into n buckets; extra rows go to earlier buckets |
| `PERCENT_RANK()` | Global recompute | `(rank - 1) / (N - 1)` after global sort |
| `LAG(col, n [, default])` | Pass-through recompute | Offset n rows back within partition; recomputed globally |
| `LEAD(col, n [, default])` | Pass-through recompute | Offset n rows forward within partition; recomputed globally |
| `FIRST_VALUE(col)` | Pass-through recompute | First value in the window frame; globally correct |
| `LAST_VALUE(col)` | Pass-through recompute | Last value in the window frame; needs explicit frame |
| Running `SUM(col) OVER (...)` | Pass-through recompute | Cumulative sum recomputed from globally sorted rows |
| Running `AVG(col) OVER (...)` | Pass-through recompute | Cumulative average recomputed globally |
| `PARTITION BY <col>` | Supported | Partitions formed from globally merged rows |

### Worked Examples

#### Example 1 — Global ROW_NUMBER across shards

```sql
-- Give every user a globally correct rank by score
SELECT user_id, score,
       ROW_NUMBER() OVER (ORDER BY score DESC) AS global_rank
FROM leaderboard
ORDER BY global_rank;
```

KEEL routes this as SCATTER (no shard-key predicate in WHERE), collects all
rows from all shards, applies Phase F to recompute `global_rank` from 1 to N
across the globally sorted result, then applies Phase C (ORDER BY) and returns.

Expected output with 6 rows across 2 shards:

```
 user_id | score | global_rank
---------+-------+-------------
    1    |  100  |      1
    4    |   90  |      2
    2    |   80  |      3
    5    |   70  |      4
    3    |   60  |      5
    6    |   50  |      6
```

#### Example 2 — RANK with ties

```sql
SELECT name, score,
       RANK() OVER (ORDER BY score DESC) AS rank
FROM competition;
```

Rows with equal scores receive the same rank; the next rank skips (e.g., two
rows tied at rank 1 means the next rank is 3). This is computed globally after
merging all shard rows.

#### Example 3 — PARTITION BY

```sql
SELECT region, user_id, revenue,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rank_in_region
FROM sales
ORDER BY region, rank_in_region;
```

Phase F partitions the merged rows by `region` and assigns row numbers
independently within each partition. Each region's rows are ranked from 1 to
the number of rows in that region regardless of which shard they came from.

#### Example 4 — Running total with SUM OVER

```sql
SELECT order_date, amount,
       SUM(amount) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) AS running_total
FROM orders
ORDER BY order_date;
```

KEEL collects all rows, sorts globally by `order_date`, then recomputes
`running_total` as a cumulative sum across the globally sorted sequence. Per-
shard running totals are discarded.

#### Example 5 — LAG for day-over-day comparison

```sql
SELECT order_date, daily_revenue,
       LAG(daily_revenue, 1) OVER (ORDER BY order_date) AS prev_day_revenue,
       daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY order_date) AS delta
FROM daily_summary
ORDER BY order_date;
```

After merging and globally sorting by `order_date`, Phase F populates `LAG`
values from the preceding row in the globally correct sequence.

### Correctness Boundaries

| Scenario | Behavior |
|----------|----------|
| `ROW_NUMBER / RANK / DENSE_RANK / NTILE / PERCENT_RANK` | ✅ Always correct globally |
| `LAG / LEAD` with global `ORDER BY` | ✅ Correct — recomputed after global sort |
| `LAG / LEAD` with `PARTITION BY` across shards | ✅ Correct — partitions formed from globally merged rows |
| `FIRST_VALUE / LAST_VALUE` with default frame | ✅ Correct for FIRST_VALUE; LAST_VALUE may need explicit frame |
| `LAST_VALUE` without `ROWS UNBOUNDED FOLLOWING` | ⚠️ Returns last row in default frame (same row), not last in partition. Use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. |
| `LAG / LEAD` with a WHERE clause that filters to single shard | ✅ SINGLE route — window computed natively by PostgreSQL; no Phase F needed |
| `NTH_VALUE(col, n)` | ⚠️ Not implemented in Phase F; treated as pass-through, may be incorrect cross-shard |
| `CUME_DIST()` | ⚠️ Not implemented in Phase F; treated as pass-through, may be incorrect cross-shard |

### When `window_forced_single` Is Set

The router sets `window_forced_single = true` when a window function query
cannot be correctly handled via SCATTER (for example, if the analyzer detects
a window function type not supported by Phase F). In this case the query is
forced to route as SINGLE to the shard that holds the matching data. If no
single shard can be identified (no shard key predicate), the query returns
`UNSUPPORTED`.

**What this means in practice:**

- If your window query has a shard-key predicate in WHERE (`WHERE user_id = $1`),
  KEEL routes it SINGLE to the correct shard and the window function is
  evaluated natively by PostgreSQL. This is always correct.
- If your window query has no shard-key predicate and uses an unsupported window
  function, the query returns `UNSUPPORTED`. In this case, options are:
  1. Add a shard-key predicate to pin to a single shard.
  2. Use a supported window function (ROW_NUMBER, RANK, DENSE_RANK, NTILE,
     PERCENT_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, running SUM/AVG).
  3. Route directly to a shard backend for an analytics-specific query.

---

## 7. CTEs and Subqueries

### CTE (WITH Clause) Routing Rules

KEEL's shard key extractor (`shard_extract_select()`) contains an early guard:

```
GUARD: no WITH clause
```

Any SELECT that contains a `WITH ...` CTE will have key extraction return
`NOT_FOUND`, making the query SCATTER-eligible regardless of the CTE or main
query contents. The **full original SQL** (including the CTE) is sent as-is to
every shard.

#### Non-Recursive CTEs

```sql
-- Correct: CTE scatters, each shard evaluates the CTE locally
WITH high_value_orders AS (
    SELECT user_id, SUM(amount) AS total
    FROM orders
    GROUP BY user_id
    HAVING SUM(amount) > 1000
)
SELECT u.name, h.total
FROM users u
JOIN high_value_orders h ON u.id = h.user_id
ORDER BY h.total DESC;
```

**Behavior:** Each shard evaluates the full CTE and the main query against its
local data. KEEL concatenates result rows from all shards. The ORDER BY is then
applied globally via Phase C.

**When this is correct:** The CTE references the sharded table and the join
is within the same shard. Both `users` and `orders` rows for a given `user_id`
are co-located on the same shard (assuming both tables use `user_id` as their
shard key).

**When this can produce duplicates or incorrect joins:** If `users` and `orders`
are sharded on different keys (e.g., `users` on `id` and `orders` on `created_at`),
rows for the same user may be on different shards. The per-shard JOIN will miss
cross-shard user-order pairs. Consider co-locating related tables with the same
shard key to avoid this.

#### Recursive CTEs

```sql
-- Each shard evaluates the recursive CTE independently on its data
WITH RECURSIVE org_tree AS (
    SELECT id, parent_id, name FROM departments WHERE parent_id IS NULL
    UNION ALL
    SELECT d.id, d.parent_id, d.name
    FROM departments d
    JOIN org_tree t ON d.parent_id = t.id
)
SELECT * FROM org_tree;
```

**Behavior:** Each shard runs the recursive CTE against its local subset of
`departments`. KEEL concatenates rows from all shards. The recursion is
per-shard, not cross-shard. If the hierarchy spans multiple shards (e.g.,
parent and child departments on different shards), the recursive traversal will
be incomplete.

**Recommendation:** For hierarchical data requiring cross-shard traversal,
store the table on a single shard (no shard rule for that table) or use a
reference/replicated table pattern.

#### Writable CTEs

```sql
-- Writable CTE with RETURNING
WITH inserted AS (
    INSERT INTO orders (user_id, amount, status)
    VALUES ($1, $2, 'pending')
    RETURNING id, user_id, amount
)
SELECT * FROM inserted;
```

**Behavior:** Treated as SCATTER (no key extraction for WITH clauses). This
means the INSERT will be sent to **all shards**, each shard will attempt the
insert, and KEEL will use 2PC for atomicity. The RETURNING rows from all shards
are concatenated.

**Warning:** This is usually not what you want — you likely want the INSERT to
go to a specific shard based on `user_id`. Use a plain `INSERT INTO orders ...`
without a CTE to get single-shard routing via the shard key. Writable CTEs
will scatter to all shards.

### Subquery Routing Rules

| Subquery Form | Routing | Notes |
|---------------|---------|-------|
| `SELECT ... FROM (SELECT ...) sub` | SCATTER | Derived table treated as pass-through |
| `WHERE col = (SELECT ... LIMIT 1)` | SCATTER | Scalar subquery in WHERE always scatters |
| `WHERE EXISTS (SELECT ...)` | SCATTER | EXISTS subquery always scatters |
| `WHERE col IN (SELECT ...)` | SCATTER | IN subquery always scatters |
| `SELECT (SELECT ... LIMIT 1) AS col` | SCATTER | Scalar subquery in SELECT always scatters |

For all subquery forms, the **full SQL including the subquery** is forwarded
as-is to every shard. KEEL does not decompose or transform subqueries.

### CTE Corner Cases

| Scenario | Expected Behavior |
|----------|-------------------|
| CTE with `LIMIT` in main query | LIMIT stripped for shards (if GROUP BY also present); applied post-merge |
| CTE referencing unsharded table | Entire query scatters; each shard evaluates CTE against its local unsharded data |
| CTE with `INSERT ... RETURNING` | Scatter-write to all shards; 2PC; RETURNING rows concatenated |
| Recursive CTE with cross-shard parent-child links | Incomplete traversal — parent and child must be on the same shard |
| CTE that returns only one row (single-shard data) | Returns N copies (one per shard); add WHERE shard_key = $1 to avoid scatter |
| Multiple CTEs in one query | All treated as SCATTER; entire SQL sent as-is to each shard |

---

## 8. JSONB, Arrays, and Composite Types

### JSONB Pass-Through

KEEL is a transparent proxy at the SQL level. For the purpose of routing and
aggregation, all column values are treated as opaque text. This means **all
PostgreSQL JSONB operators work correctly** because the proxy never parses JSON
content — it simply forwards queries to the shards and the shards handle JSONB
natively.

#### Supported JSONB Operations

| Operator / Function | Scatter behavior | Notes |
|---------------------|-----------------|-------|
| `->>` (text value of key) | ✅ SCATTER | Pushed to shards as-is |
| `->` (JSON value of key) | ✅ SCATTER | Pushed to shards as-is |
| `#>>` (text value of path) | ✅ SCATTER | Pushed to shards as-is |
| `#>` (JSON value of path) | ✅ SCATTER | Pushed to shards as-is |
| `@>` (contains) | ✅ SCATTER | Containment check on each shard |
| `<@` (is contained by) | ✅ SCATTER | Containment check on each shard |
| `?` (key exists) | ✅ SCATTER | Key existence check on each shard |
| `?\\|` (any key exists) | ✅ SCATTER | Array key existence on each shard |
| `?&` (all keys exist) | ✅ SCATTER | Array key existence on each shard |
| `@@` (JSONPath match) | ✅ SCATTER | Sent as-is |
| `@?` (JSONPath exists) | ✅ SCATTER | Sent as-is |
| `jsonb_set(...)` | ✅ SCATTER | UPDATE expression, handled by shard |
| `jsonb_agg(col)` | ⚠️ Per-shard | Returns N rows (one per shard); no cross-shard merge |
| `json_object_agg(k, v)` | ⚠️ Per-shard | Returns N rows (one per shard); no cross-shard merge |
| `jsonb_build_object(...)` | ✅ SCATTER | Produces per-row JSON, no aggregation |

#### Example: JSONB Filter Scatter

```sql
-- Find users whose metadata contains a specific badge
SELECT id, name, metadata->>'tier' AS tier
FROM users
WHERE metadata @> '{"badges": ["early_adopter"]}';
```

KEEL scatters this query to all shards (no shard-key predicate). Each shard
evaluates the `@>` containment check natively. Results are concatenated.

#### Example: JSONB Field Access with GROUP BY

```sql
-- Count users per tier using a JSONB field
SELECT metadata->>'tier' AS tier, COUNT(*) AS user_count
FROM users
GROUP BY metadata->>'tier'
ORDER BY user_count DESC;
```

KEEL scatters and applies Phase E (GROUP BY) to merge partial counts from each
shard. The JSONB field access `metadata->>'tier'` is included in the group key
and handled by PostgreSQL on each shard.

#### Example: JSONB PATH Query

```sql
-- Find orders with items containing a product over $100
SELECT id, created_at, items
FROM orders
WHERE items @? '$[*] ? (@.price > 100)';
```

Scatters to all shards. The JSONPath expression is evaluated natively by
PostgreSQL >= 12 on each shard. No KEEL transformation.

### Array Types

Arrays behave like JSONB: the proxy treats them as opaque text. All PostgreSQL
array operators (`@>`, `<@`, `&&`, `ANY(...)`, `ALL(...)`, `array_length`,
`unnest`) work correctly because they are evaluated on the shard.

```sql
-- Find users with a specific tag
SELECT id, name FROM users WHERE tags @> ARRAY['beta_tester'];

-- Unnest tags and count (scatters; cross-shard aggregation works via Phase E)
SELECT tag, COUNT(*) AS user_count
FROM users, unnest(tags) AS tag
GROUP BY tag
ORDER BY user_count DESC;
```

### Composite Types

Composite types (`ROW(...)`, record types) are supported as opaque column values.
You cannot shard by a composite column, but they can appear anywhere else in a
query without restriction.

### Limitations

- `jsonb_agg`, `json_object_agg`, `array_agg`, and `string_agg` aggregate one
  row per shard (see §17). For analytics requiring a single merged JSON or array
  across all shards, use a direct backend connection or an external aggregation
  step.
- JSONB fields cannot be used as shard keys. Shard key extraction only supports
  scalar types (INT64, TEXT, BOOL, UUID, bound parameters `$N`).

---

## 9. LIMIT and OFFSET Semantics

This section explains a subtle correctness requirement (also described in §3 Query Lifecycle).

### The Problem

Given:
```sql
SELECT category, SUM(revenue) AS rev
FROM sales
GROUP BY category
ORDER BY rev DESC
LIMIT 3;
```

With 4 categories (A, B, C, D) distributed across 2 shards:

- Shard 0 might hold rows for A and D.
- Shard 1 might hold rows for B, C, and also more rows for A and D.

If KEEL forwarded `LIMIT 3` to each shard, shard 0 would return only its top-3
groups — potentially truncating rows for D before they were merged with shard 1's
contribution. After the merge, D's true global aggregate would be silently missing.

### The Fix

`sc_strip_limit_offset()` rewrites the shard SQL to remove `LIMIT` and `OFFSET`
clauses whenever `GROUP BY` is present. This ensures each shard returns all of its
partial groups. KEEL then:

1. Merges all partial groups globally.
2. Applies `ORDER BY` to the merged result.
3. Applies `LIMIT n OFFSET m` as a final post-merge truncation.

### When LIMIT Is Pushed Down (Safe)

When there is **no `GROUP BY`**, LIMIT can safely be forwarded to each shard.
For example:

```sql
SELECT * FROM events ORDER BY ts DESC LIMIT 10;
```

Each shard returns its top-10 rows. KEEL merges those N×10 candidate rows,
re-sorts globally, and applies the final `LIMIT 10`. The result is always correct
because no aggregation is involved.

---

## 10. Two-Phase Commit (Scatter Writes)

When a scatter query is a **write** (INSERT, UPDATE, DELETE across multiple shards),
KEEL uses a built-in two-phase commit coordinator to maintain atomicity.

### GID Format

Each shard gets a deterministic transaction identifier:

```
keel_<session_id>_<sequence>_s<shard_index>
```

Example: `keel_ab12cd34_1_s0`, `keel_ab12cd34_1_s1`

The GID is stable across retries for the same (session, sequence, shard) triple,
which allows crash recovery to identify in-doubt transactions.

### Coordinator State Machine

```
INIT
  │
  ├─ PREPARE ALL shards ─────────────────────────────────▶ PREPARED
  │       │                                                    │
  │       └─ any shard fails PREPARE ─────────────────▶ COMMIT ALL ──▶ COMMITTED
  │                                                                           │
  └─ any shard fails ──────────────────────────────────▶ ROLLBACK ALL ──▶ ROLLED_BACK
```

**Terminal state guards:** Once the coordinator reaches `COMMITTED` or
`ROLLED_BACK`, any further call to `commit_all()` or `rollback_all()` returns
`KEEL_ERR_INVALID_ARG` and leaves the state unchanged. This prevents accidental
double-commit or overwriting of a committed state.

### Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `KEEL_2PC_MAX_PARTICIPANTS` | 64 | Maximum shards in one 2PC round |
| `KEEL_SCATTER_MAX_SHARDS` | 64 | Maximum shard count per rule |
| `SCATTER_CONNECT_TIMEOUT_MS` | 5 000 ms | TCP connect timeout per shard |
| `SCATTER_READ_TIMEOUT_MS` | 30 000 ms | `SO_RCVTIMEO` per shard socket |

---

## 11. EXPLAIN SHARD PLAN FOR

The admin console command `EXPLAIN SHARD PLAN FOR '<sql>'` returns a 7-column
result showing how KEEL would route a query:

```sql
-- Connect to the KEEL admin port (default 7433)
psql -h 127.0.0.1 -p 7433 -U admin keeldb

EXPLAIN SHARD PLAN FOR 'SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status ORDER BY 2 DESC LIMIT 5';
```

### Output Columns

| Column | Type | Values | Meaning |
|--------|------|--------|---------|
| `kind` | text | `SINGLE` / `SCATTER` / `UNSUPPORTED` | Routing type |
| `shard_index` | text | `0`, `1`, … or `-` | Target shard (SINGLE only) |
| `shard_count` | text | `2`, `4`, … or `-` | Fan-out width (SCATTER only) |
| `agg_type` | text | `NONE` / `SCALAR_AGG` / `AVG` / `GROUP_BY` / `GROUP_AGG` / `COUNT_DISTINCT` / `-` | Merge strategy class |
| `merge_strategy` | text | e.g. `GROUP+SORT+LIMIT` / `PASSTHROUGH` / `-` | Phase combination |
| `has_order_by` | text | `true` / `false` / `-` | Whether ORDER BY is present |
| `has_limit` | text | `true` / `false` / `-` | Whether LIMIT is present |

### Example Output

```
 kind    | shard_index | shard_count | agg_type  | merge_strategy  | has_order_by | has_limit
---------+-------------+-------------+-----------+-----------------+--------------+-----------
 SCATTER | -           | 2           | GROUP_AGG | GROUP+SORT+LIMIT| true         | true
```

### Use Cases

- Verify a query will scatter before running it in production.
- Confirm that `GROUP BY + LIMIT` correctly shows `merge_strategy = GROUP+SORT+LIMIT`
  (not `GROUP+LIMIT`, indicating LIMIT is post-merge only).
- Debug window function queries to confirm Phase F is active (`merge_strategy` will include `WINDOW`).
- Debug unexpectedly `UNSUPPORTED` queries.

---

## 12. NULL Handling and Edge Cases

### NULL Semantics in Aggregates

| Scenario | Behavior |
|----------|---------|
| `COUNT(col)` where col is NULL | NULLs excluded from count (same as PostgreSQL) |
| `COUNT(*)` | NULLs in columns do not affect row count |
| `SUM(col)` with all NULLs | Returns NULL (not 0) — same as PostgreSQL |
| `AVG(col)` with all NULLs | Returns NULL |
| `MIN(col)` / `MAX(col)` with NULLs | NULLs ignored; returns NULL only if all values are NULL |
| `GROUP BY col` where col has NULLs | NULL is its own group key (all NULLs grouped together) |
| `ORDER BY col NULLS LAST` | NULLs sorted last |
| `ORDER BY col NULLS FIRST` | NULLs sorted first |
| `ORDER BY col ASC` (default) | NULLS LAST in PostgreSQL; Phase C preserves this |
| `ORDER BY col DESC` (default) | NULLS FIRST in PostgreSQL; Phase C preserves this |

### Empty Shard Results

If a shard returns zero rows:
- For `COUNT(*)`: that shard contributes 0 to the sum.
- For `MIN`/`MAX`/`AVG`/`SUM`: the shard contributes no values.
- For `GROUP BY`: no groups from that shard.
- For window functions: zero rows from that shard are included in the merged set before Phase F.

### Integer Overflow

`SUM(bigint)` across N shards is computed in 64-bit arithmetic. Overflow is
not detected by KEEL — it will overflow silently in the same way that
PostgreSQL overflow would. Use `SUM(col::numeric)` to avoid overflow for very
large sums.

### Empty Result Set

If all shards return zero rows:
- `SELECT COUNT(*) ...` returns a single row with value 0.
- `SELECT SUM(col) ...` returns a single row with NULL.
- `SELECT ... GROUP BY ...` returns zero rows.
- Window functions: zero rows returned (Phase F is a no-op on an empty result).

### Ties in Merge Phases

When multiple rows have identical sort keys in Phase C:
- The relative order between tied rows is **undefined** (not stable).
- For `RANK()`, ties correctly receive the same rank value.
- For `ROW_NUMBER()`, ties receive distinct sequential numbers in an
  implementation-defined order (whichever shard's row happened to arrive first).
  If you need deterministic tie-breaking, add a tiebreaker column to ORDER BY:
  ```sql
  ORDER BY score DESC, id ASC
  ```

---

## 13. Configuration Reference

### Shard Rule (required)

Scatter-merge requires at least one shard rule in `keel.ini`. Without a shard
rule, all queries route to the default single backend.

```ini
[shard_rule.orders]
table        = orders
column       = user_id
shard_count  = 4
strategy     = hash        # hash (default) or range

[shard_backend.0]
host         = shard0.db.example.com
port         = 5432
database     = orders_db
user         = app_user
password     = secret

[shard_backend.1]
host         = shard1.db.example.com
port         = 5432
database     = orders_db
user         = app_user
password     = secret

[shard_backend.2]
host         = shard2.db.example.com
port         = 5432
database     = orders_db
user         = app_user
password     = secret

[shard_backend.3]
host         = shard3.db.example.com
port         = 5432
database     = orders_db
user         = app_user
password     = secret
```

### Scatter Limits

| Parameter | Where | Default | Description |
|-----------|-------|---------|-------------|
| `KEEL_SCATTER_MAX_SHARDS` | compile-time | 64 | Maximum shards per rule |
| `SCATTER_CONNECT_TIMEOUT_MS` | compile-time | 5 000 | TCP connect timeout (ms) |
| `SCATTER_READ_TIMEOUT_MS` | compile-time | 30 000 | `SO_RCVTIMEO` per shard (ms) |
| `max_mem_bytes` | runtime (per query) | configurable | Memory cap before spill |
| `spill_dir` | runtime (per query) | `/tmp` | Spill directory for large sorts |

### SIGHUP Hot-Reload

Shard rules can be modified in `keel.ini` and reloaded without restart:

```bash
kill -HUP "$(cat /var/run/keel.pid)"
```

The router updates its in-memory rule registry atomically. In-flight scatter
queries complete with the old rules; new queries see the new rules immediately.

---

## 14. Performance Characteristics

### Latency Components

A scatter query incurs these additional costs compared to a single-shard query:

| Component | Typical Cost | Notes |
|-----------|-------------|-------|
| TCP connect × N shards | 0.1–2 ms each | Blocking, parallelised per shard |
| SCRAM-SHA-256 auth × N | 0.5–3 ms each | Blocking, per new connection |
| Network RTT × N (parallel) | 0.1–5 ms | Parallel fan-out |
| Merge pipeline | < 1 ms | In-memory for small result sets |
| Sort (large result sets) | variable | Spills to disk if > `max_mem_bytes` |

Total overhead for a 4-shard scatter with local backends: **typically 5–30 ms**.

### TCP Connection Reuse

The scatter engine currently opens **a new TCP connection per shard per query**.
This is the dominant cost for scatter queries to remote shards. For best
performance:

- Run KEEL on the same host or low-latency network as the shard backends.
- Consider persistent connection pooling per shard (roadmap item).
- Use KEEL's benchmark scripts (`bench/run_scatter_pgbench.sh`,
  `bench/measure_scatter_conn_overhead.sh`) to measure overhead in your
  environment.

### pgbench Baseline

The included benchmark suite measures:

- **Scatter throughput** vs. direct single-shard throughput.
- **Connection overhead** (SCRAM auth cost per shard).
- **Merge overhead** (in-memory aggregation time).

Expected overhead threshold (configurable in `bench/run_scatter_pgbench.sh`):

| Metric | Threshold |
|--------|-----------|
| Throughput degradation | < 10% (`THROUGHPUT_THRESHOLD=0.10`) |
| Scatter connection overhead | < 20% (`OVERHEAD_THRESHOLD=0.20`) |

---

## 15. Prometheus Metrics

### Scatter-Merge Duration Histogram

```
keel_router_scatter_merge_duration_seconds
```

A Prometheus histogram measuring the end-to-end wall-clock time for each
scatter-merge execution (from fan-out start to last byte written to the client).

**Bucket boundaries (upper bounds):**
`1ms`, `5ms`, `10ms`, `25ms`, `50ms`, `100ms`, `250ms`, `500ms`, `1s`, `2.5s`, `+Inf`

### Example PromQL

```promql
# P99 scatter-merge latency over the last 5 minutes
histogram_quantile(0.99,
  rate(keel_router_scatter_merge_duration_seconds_bucket[5m])
)

# P95 scatter-merge latency
histogram_quantile(0.95,
  rate(keel_router_scatter_merge_duration_seconds_bucket[5m])
)

# Scatter-merge operations per second
rate(keel_router_scatter_merge_duration_seconds_count[1m])

# Average scatter-merge latency
rate(keel_router_scatter_merge_duration_seconds_sum[1m])
  /
rate(keel_router_scatter_merge_duration_seconds_count[1m])
```

### Grafana Panel (JSON snippet)

```json
{
  "title": "Scatter-Merge P99 Latency",
  "type": "graph",
  "targets": [{
    "expr": "histogram_quantile(0.99, rate(keel_router_scatter_merge_duration_seconds_bucket[5m]))",
    "legendFormat": "P99"
  }, {
    "expr": "histogram_quantile(0.95, rate(keel_router_scatter_merge_duration_seconds_bucket[5m]))",
    "legendFormat": "P95"
  }]
}
```

The pre-built Grafana dashboard at `etc/grafana/keel-dashboard.json` includes
a scatter-merge panel with P50/P95/P99 quantile lines.

### Additional Shard Metrics

```promql
# Single-shard hit rate per rule
keel_router_shard_single_routes_total{rule="orders"}

# Scatter hits (fan-out queries)
keel_router_shard_scatter_hits_total{rule="orders"}

# Scatter failures (at least one shard returned an error)
keel_router_shard_scatter_failed_total{rule="orders"}
```

---

## 16. Error Behavior

### Per-Shard Failures

If one shard returns an error during scatter execution:

1. The error is logged at `WARN` level with shard index and error message.
2. The remaining shards continue executing.
3. The error message is propagated to the client via the standard PostgreSQL
   `ErrorResponse` message with `SQLSTATE 58030` (I/O error).
4. Partial results are **not** sent to the client when an error occurs.

### Timeout Behavior

`SO_RCVTIMEO` is set to `SCATTER_READ_TIMEOUT_MS` (30 000 ms) on each shard
socket after `connect()` succeeds. If a shard does not respond within 30 seconds:

1. The socket is closed.
2. The shard is counted as a failure.
3. The error is propagated as described above.

`SCATTER_CONNECT_TIMEOUT_MS` (5 000 ms) governs the TCP connect phase.

### Out-of-Memory Protection

If the in-memory merge buffer would exceed `max_mem_bytes`, KEEL spills the
intermediate result to `spill_dir`. If `spill_dir` is unavailable or the spill
itself fails, KEEL returns `KEEL_ERR_OUT_OF_MEMORY` to the client without
sending partial results.

### Two-Phase Commit Failures

For scatter writes using 2PC:

| Scenario | Result |
|----------|--------|
| All shards PREPARE successfully | `COMMIT ALL` → `COMMITTED` |
| Any shard fails PREPARE | `ROLLBACK ALL` → `ROLLED_BACK`, error to client |
| Network failure during COMMIT | In-doubt transactions on prepared shards; use `SHOW PREPARED TRANSACTIONS` on each shard to recover manually |
| `rollback_all()` after `COMMITTED` | `KEEL_ERR_INVALID_ARG`; state unchanged |
| `commit_all()` after `ROLLED_BACK` | `KEEL_ERR_INVALID_ARG`; state unchanged |

---

## 17. Limitations and Unsupported Patterns

Each limitation listed here is covered by a corresponding `@pytest.mark.xfail`
test in `tests/e2e/test_sharding_limitations.py`.  When a limitation is fixed,
remove the `xfail` marker from the matching test.

### Hard Limits

| Limit | Value | Constant |
|-------|-------|----------|
| Maximum shards per rule | 64 | `KEEL_SCATTER_MAX_SHARDS` |
| Maximum shard rules | 16 | `KEEL_ROUTER_MAX_SHARD_RULES` |
| Maximum window columns (Phase F) | 8 | `KEEL_SCATTER_MAX_WINDOW_COLS` |
| Maximum 2PC participants | 64 | `KEEL_2PC_MAX_PARTICIPANTS` |
| Shard connect timeout | 5 000 ms | `SCATTER_CONNECT_TIMEOUT_MS` |
| Shard read timeout | 30 000 ms | `SCATTER_READ_TIMEOUT_MS` |

---

### Category A — Scatter Aggregate Limitations

#### A1 — PERCENTILE_CONT / PERCENTILE_DISC

`PERCENTILE_CONT(f) WITHIN GROUP (ORDER BY col)` and `PERCENTILE_DISC(f)` are
**ordered-set aggregates** that require a globally sorted view of all rows.
KEEL scatters the query to each shard independently; each shard computes its
own percentile over its local rows and returns one value.  The proxy receives N
rows (one per shard) rather than one globally correct percentile.

**Workaround:** Run analytics against a direct single-backend connection, or
pre-aggregate data into an unsharded analytics table.

**Test:** `TestScatterAggregateLimitations::test_percentile_cont_scatter_global`,
`test_percentile_disc_scatter_global`

#### A2 — STRING_AGG / ARRAY_AGG / jsonb_agg / json_object_agg

These aggregates have no cross-shard merge implementation.  Each shard computes
its own aggregate and returns one row.  The proxy concatenates those rows, so
the client receives N rows (one per shard) instead of a single merged result.

| Aggregate | Client sees |
|-----------|-------------|
| `STRING_AGG(name, ',')` | N comma-separated strings (one per shard) |
| `ARRAY_AGG(col)` | N arrays (one per shard) |
| `jsonb_agg(col)` | N JSON arrays (one per shard) |
| `json_object_agg(k, v)` | N JSON objects (one per shard) |

**Workaround:** Post-process in the application; or use a shard-pinned query
(`WHERE shard_key = $1`) if all the data lives on one shard.

**Tests:** `test_string_agg_scatter_single_row`, `test_array_agg_scatter_single_row`,
`test_jsonb_agg_scatter_single_row`, `test_json_object_agg_scatter_single_row`

#### A3 — COUNT(DISTINCT) via Subquery Double-Counts Cross-Shard

```sql
-- Returns 61 × num_shards instead of 61 (global distinct count)
SELECT COUNT(*) FROM (SELECT DISTINCT age FROM users) sub;
```

Each shard runs `SELECT DISTINCT` on its local rows and returns its local
distinct count.  The scatter engine sums those per-shard counts rather than
deduplicating the values cross-shard.

**Correct alternative:** Use `COUNT(DISTINCT col)` directly (Phase D hash
deduplication handles this correctly):

```sql
SELECT COUNT(DISTINCT age) FROM users;  -- correct: 61
```

**Test:** `test_count_distinct_via_subquery_global`

#### A4 — SELECT DISTINCT Cross-Shard Does Not Deduplicate

`SELECT DISTINCT col FROM sharded_table` evaluates DISTINCT per-shard.
Values that appear on multiple shards are not removed cross-shard, so the
result contains duplicates.

**Workaround:** Use `COUNT(DISTINCT col)` for counting; for the actual values
union shard results and deduplicate in the application.

**Test:** `test_select_distinct_cross_shard_deduplication`

---

### Category B — Window Function Scatter Limitations

#### B1 — NTH_VALUE Not Implemented in Phase F

`NTH_VALUE(col, n) OVER (...)` is not handled by Phase F
(`keel_pg_result_window_compute()`).  Each shard computes NTH_VALUE over its
local rows; the values are incorrect for global ordering.

**Workaround:** Post-process: use `ROW_NUMBER()` (Phase F ✓), then filter by
`rn = n` in a wrapping query.

**Test:** `TestWindowFunctionLimitations::test_nth_value_scatter_correct`

#### B2 — CUME_DIST Not Implemented in Phase F

`CUME_DIST() OVER (ORDER BY col)` is not handled by Phase F.  Each shard
computes its own cumulative distribution fraction over local rows only, giving
values in (0, 1] relative to the shard, not the global dataset.

**Workaround:** Use `PERCENT_RANK()` as an approximation (Phase F ✓); or
compute CUME_DIST post-aggregation in the application.

**Test:** `test_cume_dist_scatter_correct`

#### B3 — LAST_VALUE Default Frame Semantics

`LAST_VALUE(col) OVER (ORDER BY x)` uses the default PostgreSQL window frame
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.  This means LAST_VALUE
returns the **current row's own value**, not the last value in the partition.

This is correct PostgreSQL behaviour, but it surprises users who expect
LAST_VALUE to mean "last row in the partition".  Add an explicit frame:

```sql
LAST_VALUE(col) OVER (
    PARTITION BY category
    ORDER BY score DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
```

Phase F handles the explicit frame correctly.

**Test:** `test_last_value_default_frame_returns_partition_last`

---

### Category C — DML / RETURNING Limitations

#### C1 — RETURNING Rows Not Merged in Scatter DML

`DELETE ... RETURNING`, `UPDATE ... RETURNING`, and `INSERT ... RETURNING`
on scatter tables: KEEL routes the DML correctly (each shard executes on its
local rows), but the RETURNING rows from each shard are **not concatenated**
in the client result.  The client receives an empty result set or a subset of
the expected rows.

**Workaround:** After the DML, issue a follow-up `SELECT` to retrieve the
affected rows:

```sql
-- Instead of: DELETE FROM orders WHERE status = 'cancelled' RETURNING id
BEGIN;
DELETE FROM orders WHERE status = 'cancelled';
-- separately: SELECT id FROM orders WHERE status = 'cancelled' -- run BEFORE delete
COMMIT;
```

**Tests:** `TestDMLReturningLimitations::test_scatter_delete_returning_rows`,
`test_scatter_update_returning_rows`, `test_scatter_insert_returning_id`

---

### Category D — Routing Limitations

#### D1 — Cross-Shard JOIN Returns UNSUPPORTED

A JOIN that spans two sharded tables on different shard keys returns an
`UNSUPPORTED` error.  KEEL cannot route a query that must read rows from
different shards and join them.

```sql
-- UNSUPPORTED: users sharded on id, orders sharded on order_id
SELECT u.name, o.amount
FROM users u JOIN orders o ON o.order_id = u.id;
```

**Workaround:** Co-locate tables on the same shard key; or use an unsharded
lookup table; or join in the application after separate scatter queries.

**Test:** `TestRoutingLimitations::test_cross_shard_join_executes`

#### D2 — Multi-Row VALUES INSERT Routes by First Row Only

`INSERT INTO t VALUES (row1), (row2), (row3)` — `shard_extract_insert()` reads
only the **first VALUES row** to determine the shard.  All rows are sent to
that single shard, regardless of the other rows' shard keys.

```sql
-- DANGER: routes by user_id=1 shard; users 2 and 3 go to the wrong shard
INSERT INTO orders (user_id, amount) VALUES (1, 100), (2, 200), (3, 300);
```

**Safe alternatives:**
- Send separate single-row `INSERT` statements for each row.
- If all rows share the same shard key, multi-row INSERT is safe.
- Use application-level batching grouped by shard key.

**Test:** `test_multi_row_insert_routes_each_row`

#### D3 — OR on Shard Key Always Scatters

`WHERE id = $1 OR id = $2` is not decomposed; the query scatters to all shards
even if both ids reside on the same shard.

**Workaround:** Use `WHERE id IN ($1, $2)` (same routing today, but IN-list
pruning is on the roadmap); or issue separate queries.

**Test:** `test_or_shard_key_single_routes`

#### D4 — Composite Shard Key (Tuple Equality) Not Recognised

`WHERE (col_a, col_b) = ($1, $2)` — tuple / row constructor equality is not
parsed by the shard-key extractor.  The query scatters instead of routing to
the single shard.

**Test:** `test_composite_key_routes_single`

#### D5 — Schema-Qualified Table Names May Break Routing

`SELECT * FROM public.users WHERE id = 1` — the `public.` prefix may not be
stripped by the SQL parser.  If preserved, KEEL's case-insensitive match
against `"users"` fails and the query scatters or returns UNSUPPORTED.

**Workaround:** Use unqualified table names in queries against sharded tables.

**Test:** `test_schema_qualified_table_routes_single`

#### D6 — BETWEEN on Shard Key Always Scatters

`WHERE id BETWEEN $1 AND $2` is not recognised as a single-shard predicate
(even when `$1 = $2`, i.e., a point query written as a range).

**Test:** `test_between_shard_key_single_route`

#### D7 — IN-List Not Shard-Pruned

`WHERE id IN (1, 2, 3)` scatters to all shards.  KEEL does not hash each
IN-list value and prune to the subset of shards that actually hold matching
rows.

**Test:** `test_in_list_routes_to_subset_of_shards`

---

### Category E — CTE Limitations

#### E1 — Writable CTE Scatters INSERT to All Shards

```sql
WITH ins AS (INSERT INTO users(id, name) VALUES (1, 'x') RETURNING id)
SELECT id FROM ins;
```

The WITH-clause guard in the shard router forces any CTE query to scatter.
The INSERT is therefore sent to **all shards**, creating N duplicate rows
(one per shard).

**Workaround:** Use a plain `INSERT INTO` (no CTE wrapper).  If RETURNING is
needed, issue a follow-up `SELECT` by primary key.

**Test:** `TestCTELimitations::test_writable_cte_inserts_to_correct_shard_only`

#### E2 — Read-Only CTE Row Duplication

```sql
WITH cte AS (SELECT 42 AS val) SELECT COUNT(*) FROM cte;
-- Returns 2 (or N) instead of 1
```

KEEL evaluates the CTE on each shard independently.  A constant CTE (no shard
key filter) returns one copy per shard.  The proxy concatenates N copies.

**Workaround:** For pure computation CTEs that must run once, route to a
non-sharded backend; or add a `WHERE shard_key = $1` inside the CTE so it
runs single-shard.

**Test:** `test_constant_cte_single_row`

#### E3 — Recursive CTE Cross-Shard Hierarchy Traversal Incomplete

`WITH RECURSIVE` CTEs are evaluated per-shard.  If parent and child rows are
on different shards, the recursive expansion on the parent's shard will not
find the child.

**Workaround:** Co-locate parent and child rows using the same shard key; or
store hierarchical data in an unsharded table.

**Test:** `test_recursive_cte_cross_shard_hierarchy`

---

### Category F — Global Ordering / Pagination Limitations

#### F1 — LIMIT with Large OFFSET Performance

```sql
SELECT * FROM orders ORDER BY created_at DESC LIMIT 10 OFFSET 10000;
```

KEEL must fetch `OFFSET + LIMIT` rows from each shard before discarding the
offset prefix.  For large offsets, this is O(shards × offset) rows read
internally even though only `LIMIT` rows are returned.

**Workaround:** Use keyset (cursor) pagination:
```sql
-- Next page: WHERE created_at < $last_created_at ORDER BY created_at DESC LIMIT 10
```

**Test:** `TestGlobalOrderingLimitations::test_order_by_limit_offset_correct_result`

#### F2 — LIMIT Without ORDER BY Is Non-Deterministic

`SELECT * FROM sharded_table LIMIT N` without `ORDER BY` returns an
implementation-defined subset that may differ between invocations (depends on
shard ordering and network timing).

**Test:** `test_limit_without_order_by_is_deterministic`

---

### Category G — Protocol / DDL Limitations

#### G1 — DDL Not Scatter-Routed

`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, and all other DDL
statements are **not scatter-routed**.  They are forwarded to the default
backend (one shard) only.

**Workaround:** Run DDL directly on each shard backend using a migration tool
(e.g., Flyway, Liquibase) that connects to each shard individually.

**Test:** `TestProtocolDDLLimitations::test_alter_table_applies_to_all_shards`

#### G2 — Multi-Statement Queries Not Supported

Queries containing multiple statements separated by `;` are not supported.
Send each statement individually.

**Test:** `test_multi_statement_query_executes_all`

#### G3 — COPY FROM / COPY TO Not Shard-Routed

`COPY FROM STDIN` and `COPY TO STDOUT` are not shard-routed.  Issuing COPY
through KEEL will error or route to one shard only.

**Workaround:** Use per-shard COPY connections or application-level row
distribution.

**Test:** `test_copy_command_not_supported`

---

### Multi-Row INSERT Routing (detail)

See Category D2 above.

---

### Full ACID Cross-Shard

2PC provides **atomicity** for scatter writes (all shards commit or all rollback).
However:
- **Isolation** is per-shard. There is no serializable isolation across shards.
- A cross-shard transaction that reads shard 0 then writes to shard 1 may
  observe inconsistencies if another transaction interleaves.
- For strong consistency, ensure each transaction touches only one shard
  (single-shard routing via shard key predicates).

---

## 18. Corner Cases Reference

A quick-reference table of edge cases that have surprised production users.
Items marked ⚠️ are **known limitations** covered by `test_sharding_limitations.py`.

| Scenario | What Happens | What To Do |
|----------|-------------|------------|
| `SELECT COUNT(*) FROM orders` (no WHERE) | Scatters; Phase D merges counts → single correct row | Expected behavior |
| `SELECT COUNT(*) FROM orders WHERE id = 42` | SINGLE route if `id` is shard key | Expected behavior |
| `SELECT * FROM orders LIMIT 5` | Scatters with LIMIT pushed down; N shards return up to 5 rows each; global re-sort + LIMIT 5 applied | You may get up to N×5 intermediate rows in memory |
| `SELECT * FROM orders GROUP BY status LIMIT 5` | Scatters without LIMIT pushed down; all groups collected then top-5 returned | Correct but fetches all groups from all shards |
| `SELECT ... ORDER BY col NULLS LAST LIMIT 10` | NULLs sorted last globally; Phase L returns top 10 | Correct |
| `INSERT INTO orders (user_id) VALUES ($1)` with $1 unbound | Scatters (no binding → NOT_FOUND → SCATTER) | Provide binding or use literal value |
| `WHERE id = 1 AND id = 2` (conflicting predicates) | UNSUPPORTED (conflict detected) | Fix the query — it can never match any row |
| `WHERE id IN (1, 2, 3)` | ⚠️ Scatters (IN not decomposed — D7) | Acceptable; use keyset pagination if performance matters |
| `WHERE id = 1 OR id = 2` | ⚠️ Scatters (OR prevents single routing — D3) | Use IN or separate queries if single routing needed |
| `UPDATE orders SET status='done'` (no WHERE) | Scatters; 2PC write to all shards | Expected |
| `DELETE FROM orders` (no WHERE) | Scatters; 2PC delete on all shards | Correct; destructive — be careful |
| `DELETE FROM orders WHERE ... RETURNING id` | ⚠️ RETURNING rows not merged (C1) | Follow up with SELECT after the DML |
| `UPDATE orders SET ... RETURNING *` | ⚠️ RETURNING rows not merged (C1) | Same as above |
| `WITH cte AS (...) SELECT * FROM cte` | Scatters (WITH guard); full SQL to each shard | Use shard key in main WHERE if possible |
| `WITH ins AS (INSERT INTO t ...) SELECT ...` | ⚠️ INSERT scatters to ALL shards (E1) | Use plain INSERT; no CTE wrapper |
| `WITH cte AS (SELECT 42) SELECT COUNT(*) FROM cte` | ⚠️ Returns N instead of 1 (E2) | Run on non-sharded connection |
| `INSERT INTO orders VALUES (1, 10), (2, 20)` (multi-row) | ⚠️ Routes by first row only; row 2 goes to wrong shard (D2) | Use single-row inserts or batch by shard |
| `SELECT jsonb_agg(data) FROM events` | ⚠️ Each shard returns its own aggregate; N rows (A2) | Merge in application |
| `SELECT STRING_AGG(name, ',') FROM users` | ⚠️ Each shard returns its own aggregate; N rows (A2) | Merge in application |
| `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col) FROM t` | ⚠️ Per-shard percentile; not globally correct (A1) | Use single-backend analytics connection |
| `SELECT DISTINCT col FROM sharded_table` | ⚠️ Per-shard DISTINCT; cross-shard duplicates not removed (A4) | Use COUNT(DISTINCT col) for counts |
| `SELECT COUNT(*) FROM (SELECT DISTINCT col FROM t) sub` | ⚠️ Returns N × distinct count (A3) | Use COUNT(DISTINCT col) directly |
| `NTH_VALUE(col, 2) OVER (ORDER BY id)` | ⚠️ Phase F not implemented; per-shard value wrong (B1) | Post-process ROW_NUMBER result |
| `CUME_DIST() OVER (ORDER BY col)` | ⚠️ Phase F not implemented; per-shard fraction (B2) | Use PERCENT_RANK() instead |
| `ROW_NUMBER() OVER (PARTITION BY region ORDER BY score DESC)` | Phase F partitions by region globally; correct | Expected |
| `LAST_VALUE(col) OVER (ORDER BY ts)` | ⚠️ Returns current row's value (default frame — B3) | Add `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` |
| `WITH RECURSIVE tree AS (...) ...` | ⚠️ Each shard runs recursion on local data; cross-shard links missed (E3) | Use an unsharded table for hierarchies |
| `SELECT * FROM public.users WHERE id = 1` | ⚠️ Schema prefix may break routing (D5) | Use unqualified table names |
| `WHERE (id, name) = (1, 'x')` | ⚠️ Tuple equality not shard-routed (D4) | Use `WHERE id = 1 AND name = 'x'` |
| `JOIN` across two sharded tables | ⚠️ UNSUPPORTED error (D1) | Co-locate on same shard key |
| `ALTER TABLE` / `CREATE INDEX` via KEEL | ⚠️ Routed to default backend only (G1) | Run DDL directly on each shard |
| Transaction: scatter write then single-shard write to different shard | `KEEL_ERR_SHARD_CROSS_TX` (-902) | Keep all writes in a transaction on the same shard, or scatter all writes |
| Shard goes down during scatter | Partial results not sent; error propagated to client | Implement retry logic; shard failure error has SQLSTATE 58030 |

---

## 19. End-to-End Tutorial

This tutorial walks through setting up scatter-merge from scratch, including
creating the sharded table, inserting data, and running aggregate queries.

### Prerequisites

- KEEL running with at least 2 PostgreSQL shard backends.
- The `orders` table created on both shards with identical schema.

### Step 1: Create the Schema on Each Shard

Run on **both** shard servers directly:

```sql
CREATE TABLE orders (
    id        BIGSERIAL PRIMARY KEY,
    user_id   BIGINT NOT NULL,       -- shard key
    status    TEXT   NOT NULL,
    amount    NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX ON orders (user_id);
CREATE INDEX ON orders (status);
```

### Step 2: Configure KEEL

Add to `keel.ini`:

```ini
[shard_rule.orders]
table        = orders
column       = user_id
shard_count  = 2
strategy     = hash

[shard_backend.0]
host         = shard0.local
port         = 5432
database     = mydb
user         = keel_user
password     = secret

[shard_backend.1]
host         = shard1.local
port         = 5432
database     = mydb
user         = keel_user
password     = secret
```

Restart KEEL or send `SIGHUP` to apply.

### Step 3: Insert Data Through KEEL

Connect to KEEL (not directly to any shard):

```sql
psql -h 127.0.0.1 -p 5432 -U app_user mydb
```

```sql
-- KEEL routes each INSERT to the correct shard based on user_id
INSERT INTO orders (user_id, status, amount) VALUES
    (1, 'completed', 99.99),
    (2, 'pending',   49.50),
    (3, 'completed', 149.00),
    (4, 'cancelled', 29.00),
    (5, 'completed', 199.99),
    (6, 'pending',   79.00),
    (7, 'completed', 59.99),
    (8, 'cancelled', 19.00);
```

### Step 4: Run Aggregate Queries

```sql
-- Count by status (scatter-merge: GROUP BY without shard key)
SELECT status, COUNT(*) AS cnt
FROM orders
GROUP BY status
ORDER BY cnt DESC;
```

```
   status    | cnt
-------------+-----
 completed   |   4
 pending     |   2
 cancelled   |   2
```

```sql
-- Revenue by status with HAVING filter
SELECT status, SUM(amount) AS revenue
FROM orders
GROUP BY status
HAVING SUM(amount) > 100
ORDER BY revenue DESC;
```

```
   status    | revenue
-------------+---------
 completed   |  508.97
 pending     |  128.50
```

```sql
-- AVG order value per status (transparent AVG rewrite)
SELECT status, AVG(amount) AS avg_order
FROM orders
GROUP BY status;
```

```
   status    | avg_order
-------------+-----------
 cancelled   |     24.00
 completed   |    127.24
 pending     |     64.25
```

```sql
-- Top-3 categories by total revenue, with LIMIT (safe post-merge)
SELECT status, SUM(amount) AS total
FROM orders
GROUP BY status
ORDER BY total DESC
LIMIT 3;
```

```
   status    |  total
-------------+---------
 completed   |  508.97
 pending     |  128.50
 cancelled   |   48.00
```

### Step 5: Verify the Shard Plan

```sql
-- Connect to the admin port
psql -h 127.0.0.1 -p 7433 -U admin keeldb

EXPLAIN SHARD PLAN FOR 'SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status ORDER BY 2 DESC LIMIT 3';
```

```
 kind    | shard_index | shard_count | agg_type  | merge_strategy   | has_order_by | has_limit
---------+-------------+-------------+-----------+------------------+--------------+-----------
 SCATTER | -           | 2           | GROUP_AGG | GROUP+SORT+LIMIT | true         | true
```

This confirms:
- `kind = SCATTER` — both shards will be queried.
- `shard_count = 2` — fan-out to 2 shards.
- `agg_type = GROUP_AGG` — GROUP BY with aggregates.
- `merge_strategy = GROUP+SORT+LIMIT` — all three merge phases active.
- `has_limit = true` — LIMIT is applied post-merge (not pushed down to shards).

### Step 6: Monitor in Prometheus

```bash
curl http://127.0.0.1:9090/metrics | grep scatter_merge
```

```
# HELP keel_router_scatter_merge_duration_seconds Scatter-merge query end-to-end latency
# TYPE keel_router_scatter_merge_duration_seconds histogram
keel_router_scatter_merge_duration_seconds_bucket{le="0.001"} 12
keel_router_scatter_merge_duration_seconds_bucket{le="0.005"} 47
keel_router_scatter_merge_duration_seconds_bucket{le="0.01"} 61
...
keel_router_scatter_merge_duration_seconds_count 67
keel_router_scatter_merge_duration_seconds_sum 0.412
```
