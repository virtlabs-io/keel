# KEEL — Known Limitations

> Status: 2026-05-22 — branch `fix/m0-heap-corruption-and-m4-param-routing`
> Suite baseline: **563 passed / 16 xfailed / 4 xpassed / ~1 unrelated flaky** (full e2e run)
> Failure inventory under `--runxfail` (sharding suites only): **16 failing**
> in `test_sharding_limitations.py`.

This document is the authoritative inventory of every functional limitation
currently exposed by the e2e suite. Each entry describes:

1. **What** the limitation is (observable behaviour).
2. **Affected tests** and the assertion(s) that fire.
3. **Root cause** in the engine.
4. **Impact** on real workloads (severity & blast radius).
5. **Workarounds** application authors can use today.
6. **What it would take to fix** (engineering scope, files).
7. **Risks & corner cases** any fix must respect.

The companion document [ROADMAP.md](ROADMAP.md) sequences these into milestones.

> **Recently resolved (Phase 0 + Phase 1 + Phase 2, May 2026):**
> §1.6 OFFSET correctness · §1.7 NULLS FIRST/LAST · §1.8 GROUP BY NULL keys ·
> §1.11 BETWEEN/IN/COALESCE routing edge cases.
> Plus 11 stale `xfail` markers swept across `test_sharding_*.py`,
> `test_performance.py`, `test_pool_behavior.py`, `test_stress.py`,
> `test_transaction_pooling.py`.
> Phase 1.5 also shipped the
> `keel_scatter_unsupported_pattern_total{kind="…"}` Prometheus counter
> (see [Telemetry gaps](#telemetry-gaps)).
> **Phase 2 (May 2026):** routing tests for `id = a OR id = b`,
> `public.users WHERE id = $1`, and `id BETWEEN n AND n` now pass.
> Engine: `BETWEEN low AND high` with equal literal bounds is detected as
> a single-shard predicate in `shard_extract_from_where()`. Tests for
> EXPLAIN-SHARD-PLAN now correctly use the admin connection.
> **Phase 3 (May 2026):** `UPDATE/DELETE … RETURNING` on scatter tables
> now forwards the merged result rows to the client.
> `keel_engine_scatter_write()` captures each shard's RowDescription /
> DataRow / CommandComplete tag during Phase 1 of 2PC and, on commit,
> emits a single RowDescription + concatenated DataRows + aggregated
> `<VERB> N` CommandComplete to the client.
> **Phase 4 (May 2026):** bare `SELECT DISTINCT col_list FROM t` now
> deduplicates across shards. The router treats the target list as an
> implicit GROUP BY (`scatter_extract_merge_spec_impl()` in
> `router_weighted.c`), routing through the existing Phase E
> `group_aggs` merge with `nagg_specs=0`. The harder
> `SELECT COUNT(*) FROM (SELECT DISTINCT col FROM t) sub` pattern still
> double-counts and remains xfailed (would need per-shard SQL rewrite
> analogous to the `COUNT(DISTINCT)` path).
> **Phase 5 (May 2026):** `array_agg(... ORDER BY ...)` and
> `json_object_agg(k, v ORDER BY ...)` now produce a single merged row
> across shards. `array_agg` uses the existing ordered-aggregate rewrite
> pipeline (router detection at
> `router_weighted.c:2840`, post-merge sort + concat at
> `engine_scatter.c:1494+`). `json_object_agg` takes a *non-rewriting*
> path: a new `KEEL_ORD_AGG_JSON_OBJECT_AGG` spec lets each shard compute
> its JSON object natively, and the engine concatenates the per-shard
> objects (strips outer `{` / `}`, joins inner key/value pairs with `,`,
> re-wraps).
> **Phase 8a (May 2026):** read-only **constant CTEs** of the form
> `WITH cte AS (SELECT <const-expr>) SELECT … FROM cte` are now folded
> to a single shard (`router_weighted.c` dispatch fallback) instead of
> being scattered, eliminating the N-fold row duplication that made
> `COUNT(*)` over a constant CTE return `N` instead of `1`. Gates:
> non-recursive CTE, body is a `SELECT` with no `FROM` clause, and the
> outer `FROM` is a single `TABLE_REF` naming that CTE.

> **Phase 8b (May 2026):** **writable CTEs** of the form
> `WITH ins AS (INSERT/UPDATE/DELETE … RETURNING …) SELECT … FROM ins`
> now route to a single shard when the DML body has a constant shard
> key, eliminating the scatter-write that previously inserted/updated
> the row on every shard. The SQL parser's `parse_cte_definition` was
> taught to dispatch to `parse_insert_stmt` / `parse_update_stmt` /
> `parse_delete_stmt` (it previously only accepted `SELECT` bodies),
> and `shard_extract_select` walks the `WITH` clause and folds to
> `SINGLE` routing whenever a CTE body yields an extractable shard key.
> Gates: non-recursive CTE; the writable CTE body must contribute a
> constant shard key. Scatter is still used when no key can be extracted.

> **Phase 9 (May 2026):** the two remaining
> `keel_scatter_unsupported_pattern_total{kind=…}` label slots
> (`dml_returning`, `ddl`) are now wired in `keel_router_dispatch_sql`'s
> `dispatch_done` block. Operators can now alert on cross-shard
> `INSERT/UPDATE/DELETE … RETURNING` (ordering not preserved) and on
> any `CREATE/ALTER/DROP/TRUNCATE` fanned out to every shard.

Limitations are grouped by subsystem:

| § | Subsystem | Failing tests | Severity |
|---|---|---|---|
| [1](#1-scatter-aggregate-pipeline) | Scatter aggregate pipeline | 19 | Medium — affects analytics |
| [2](#2-parameter-aware-routing-bind-time-shard-key-extraction) | Parameter-aware routing (Bind-time shard-key extraction) | 5 | **High** — silently mis-routes parameterized DML |
| [3](#3-prepared-statement-virtualisation-ssv-under-load) | Prepared-statement virtualisation (SSV) under load | unmeasured | **High** — tail latency / connection drops under burst |
| [4](#4-heap-header-corruption-under-concurrent-scatter--resolved-allocator-mismatch-false-positive) | ~~Heap header corruption~~ — RESOLVED | — | Resolved |
| [5](#5-pool-burst-resilience) | Pool growth under unbounded burst | 1 | Low |
| [6](#6-prepared-statement-tps-regression-on-fast-path) | Prepared-statement TPS regression on fast path | 0 | Resolved |
| [7](#7-cross-shard-notify--listen) | Cross-shard `NOTIFY` / `LISTEN` | 1 | Medium — silent non-delivery |

**Total tracked:** ~26 failing tests + tail-latency stability work.

---

## 1. Scatter aggregate pipeline

### 1.1 Recursive Common Table Expressions (CTEs)

**Failing tests (3):**
- `test_sharding_comprehensive.py::TestCTEs::test_recursive_cte_countdown`
  → assertion `[5,5,4,4,3,3,…] == [5,4,3,2,1,0]`
- `test_sharding_comprehensive.py::TestCTEs::test_recursive_cte_fibonacci`
  → `[1,1,1,1,2,2,…] == [1,1,2,3,5,8,…]`
- `test_sharding_comprehensive.py::TestCTEs::test_recursive_cte_sum_of_integers`
- `test_sharding_advanced.py::TestComplexAggregationAtScale::test_recursive_cte_large_n_sum`
  → `assert 10100 == 5050`

**Observed behaviour.** A `WITH RECURSIVE t AS (… UNION ALL SELECT … FROM t)`
query that has no shard key is dispatched as a scatter to every shard. Each
shard independently evaluates the recursion against its (empty/local) data
and returns its own series; the proxy concatenates rows verbatim.
Result: every value appears `N_SHARDS` times (countdown `5,5,4,4,…`,
sum `5050 × 2 = 10100`, fibonacci is corrupted because the recursion's
`SELECT … FROM t` only sees the local shard's slice of `t`).

**Root cause.** `keel_router_dispatch_sql()` classifies any query with no
extractable shard key as `KEEL_DISPATCH_SCATTER`. The scatter path
(`engine_scatter.c`) is unaware that recursive CTEs whose recursion does not
depend on sharded tables must be evaluated **once** centrally, not N times.
The `requires_merge` flag is only set for scatter SELECTs that contain
aggregates / GROUP BY / ORDER BY / LIMIT — recursive CTEs without those
clauses bypass merge entirely and the rows are duplicated.

**Impact.** Any user that issues a recursive CTE through KEEL gets
**silently wrong results**. Severity is high *if* recursive CTEs are used,
but in OLTP workloads they are rare. The lack of an error makes silent
duplication the dangerous failure mode.

**Workaround.**
- Pin the session before the query (`SET keel.pin = on;` if exposed) so the
  query runs on a single backend.
- Wrap the CTE in an explicit transaction: the existing pinning logic will
  send all statements to one backend.
- Do recursive work in the application after fetching the seed dataset.

**Fix sketch.**
1. In `src/sql/sql_classifier.c`, detect `WITH RECURSIVE` and the special
   case where the recursion does not reference a sharded table.
   Mark such queries with a new dispatch kind `KEEL_DISPATCH_SINGLE_ANY`
   (run once on any healthy shard).
2. In `engine_flow.c`, route `SINGLE_ANY` through the existing single-shard
   path with a deterministic shard pick (e.g. `shard 0` or pool-LRU).
3. For recursive CTEs that *do* touch sharded tables, no correct distributed
   evaluation is possible without a re-engineering of the executor. Mark
   them as unsupported and return `ERROR: cross-shard recursive CTEs are
   not supported`.

**Estimated effort.** Detection + single-shard routing: **1–2 days**.
Real distributed recursive CTE evaluation: out of scope (multiple weeks,
requires a coordinator/worker model).

**Risks & corner cases.**
- A recursive CTE may *look* shard-local (e.g. `WITH RECURSIVE t(n) AS …`)
  but later JOIN against a sharded table — must walk the entire `WITH …
  SELECT …` tree, not only the recursion body.
- `WITH RECURSIVE t … SELECT * FROM t WHERE id = $1` — the `WHERE id = $1`
  outside the CTE *should* shard, but the inner CTE materialises first.
  Decision: when the outer SELECT has a shard key, evaluate both on the
  target shard.

---

### 1.2 Non-recursive CTEs (`WITH … SELECT`)

**Failing tests (6):**
- `TestCTEs::test_chained_ctes`
- `TestCTEs::test_cte_count_and_sum_simultaneously`
- `TestCTEs::test_cte_min_max_values`
- `TestCTEs::test_cte_used_twice_in_query`
- `TestCTEs::test_cte_with_filter_having`
- `TestCTEs::test_simple_cte_wraps_scatter_aggregate`
- `TestComplexAggregationAtScale::test_cte_chained_category_sums`
- `TestComplexAggregationAtScale::test_cte_multi_level_aggregation_users`

**Observed behaviour.** A `WITH t AS (SELECT … FROM users)
SELECT count(*) FROM t` returns `count(*)` per shard concatenated, not the
global count. The CTE wrapper makes the engine treat the query as
"unrecognised projection" and skip the aggregate-merge code path.

**Root cause.** Aggregate detection in `engine_scatter.c::sc_detect_merge`
(or equivalent) inspects the *outer* SELECT list directly and only
recognises top-level `SUM/COUNT/AVG/MIN/MAX`. When wrapped in a CTE the
outer projection is `SELECT … FROM t`, which the merge classifier doesn't
understand, so each shard's tuple is forwarded as-is.

**Impact.** Anyone wrapping cross-shard aggregates in a readability CTE
gets duplicated rows / wrong totals. This pattern is common in BI tooling.
**Silent wrong results** — same as §1.1.

**Workaround.** Inline the CTE: write the aggregate at the top level.

**Fix sketch.**
1. Pre-flatten non-recursive CTEs at the SQL classifier layer: if the CTE
   is referenced exactly once, substitute the body in place. (Most BI
   tools generate trivial CTEs that are safe to inline.)
2. For CTEs referenced multiple times: build a small two-phase coordinator
   that materialises the CTE once into a temp result and re-uses it.
3. Or, more conservatively, mark "CTE-wrapping-aggregate" patterns as
   single-shard and pin the query.

**Estimated effort.** CTE inlining (single-reference case): **3–4 days**
including SQL parser plumbing. Multi-reference materialisation: **2 weeks**.

**Risks.**
- Identifier scoping: the inlined alias must not clash with outer names.
- Side-effecting CTEs (`INSERT … RETURNING` inside `WITH`) cannot be
  inlined; must error.
- Watch for `WITH t AS (… LIMIT 10) SELECT … FROM t, other` — naive
  inlining changes evaluation order and can change the result set.

---

### 1.3 Window functions over scatter

**Failing tests (5):**
- `TestWindowFunctionsScatter::test_first_value_over_entire_window`
  → `FIRST_VALUE must be 3 for all rows` (each shard reports its own first)
- `TestWindowFunctionsScatter::test_last_value_over_entire_window`
- `TestWindowFunctionsScatter::test_partition_count_over_action`
  → `cnt=2 ≠ expected 3`
- `TestWindowFunctionsScatter::test_partition_sum_over_action`
  → `partition sum 150 ≠ 180`
- `TestWindowFunctionsScatter::test_sum_running_total`

**Observed behaviour.** `OVER ()` and `OVER (PARTITION BY x)` window
expressions return per-shard values, never the global window. Running
sums restart at each shard.

**Root cause.** Window functions are not parsed / merged by KEEL. They
are forwarded as part of the projection. PostgreSQL on each shard
computes the window over the rows visible to that shard only.

**Impact.** Window analytics over sharded tables are **silently wrong**.
This is severe for analytics workloads but rare in OLTP.

**Workaround.**
- Restrict the window query to a single shard (use shard key in `WHERE`).
- Materialise the rows on the client and compute the window there.
- Compute the input rows with a scatter-aggregate, then run the window
  on a small derived set.

**Fix sketch.**
- Re-execute the window function in the proxy after merging rows.
  Requires shipping each row's PARTITION BY key + ORDER BY key, then
  evaluating window frames in a new `engine_window.c`.
- Initial scope: only `OVER ()`, `OVER (PARTITION BY const)`, and
  `OVER (PARTITION BY … ORDER BY … ROWS UNBOUNDED PRECEDING)`. Lag/lead
  and percentile windows in v2.

**Estimated effort.** v1 (sum/count/avg/min/max/first/last over flat
partitions): **2 weeks**. Full window support: out of scope (months).

**Risks.**
- Memory pressure: window evaluation requires materialising all rows in
  the partition; need spill-to-disk limits.
- Correctness bar is exact: a single edge-case wrong result is worse
  than not supporting windows at all.

---

### 1.4 Percentile aggregates (`percentile_cont`, `percentile_disc`)

**Failing tests (2):**
- `TestComplexQueries::test_percentile_cont_via_scatter`
- `TestComplexQueries::test_percentile_disc_via_scatter`

**Observed behaviour.** `percentile_cont(0.5) WITHIN GROUP (ORDER BY x)`
returns `NULL` (`assert None == 320`) because the proxy's merge layer
sees an opaque ordered-set aggregate and cannot combine per-shard
percentile values.

**Root cause.** Ordered-set aggregates are not algebraic; the per-shard
medians cannot be combined into a global median without seeing the raw
data. KEEL has no facility to fall back to "fetch the raw rows and
compute centrally".

**Impact.** Any quantile / median analytics over sharded tables fails.

**Workaround.**
- Approximate with `t-digest` precomputed on the shards.
- Pull the underlying values (or a sketch) and compute the percentile in
  the application.
- Use `PERCENT_RANK()` over a sample.

**Fix sketch.**
1. Detect ordered-set aggregates in the projection.
2. Rewrite the query: ship the *raw input column* per shard, merge into
   a sorted stream in the proxy, then compute the percentile centrally.
3. For very large inputs, integrate a streaming quantile sketch
   (e.g. `t-digest` C lib) and accept approximate answers behind a
   `keel.percentile_mode = exact|approx` GUC.

**Estimated effort.** Exact (small data) impl: **3 days**.
Production-grade with sketches: **1.5 weeks** + dependency vetting.

**Risks.**
- Memory blow-up on `percentile_cont(…) WITHIN GROUP (ORDER BY huge_col)`.
- Need a row-cap per shard with a clear error if exceeded.

---

### 1.5 UNION ALL across shard scatters

**Failing tests (1):**
- `TestComplexQueries::test_union_all_combines_scatter_results`
  → expected 3 categories, got 6 (each side of UNION ALL was scatter-merged
  separately, then concatenation didn't re-merge)

**Observed behaviour.** `(SELECT category, count(*) FROM t1 GROUP BY 1)
UNION ALL (SELECT category, count(*) FROM t2 GROUP BY 1)` returns
2N_SHARDS rows where N_SHARDS=2 → 4 rows per side, doubled.

**Root cause.** The two SELECTs are dispatched as independent scatters,
each correctly aggregated, but the UNION ALL is not recognised as the
top-level operator that requires a *post-merge* aggregation step.

**Impact.** Anyone composing scatter aggregates with UNION ALL gets
the wrong shape. **Silent.**

**Workaround.** Run each branch as a separate query and concatenate
client-side, or wrap the UNION ALL in an outer aggregate that *will* be
recognised: `SELECT category, SUM(c) FROM ((… UNION ALL …)) GROUP BY 1`.
(But that depends on the UNION inner being mergeable.)

**Fix sketch.**
1. Parse the SetOp tree at the top level.
2. Dispatch each leg as a normal scatter; collect per-leg results.
3. Concatenate (UNION ALL) or hash-deduplicate (UNION) in the proxy.
4. If the SetOp is wrapped by an outer aggregate, run the outer
   aggregate on the concatenated stream.

**Estimated effort.** **3–5 days**. Requires extending the dispatch
result to be a tree of sub-dispatches rather than a single result.

**Risks.**
- UNION (without ALL) requires global de-dup. Must spill to disk for
  large unions.
- ORDER BY across the entire UNION must drive a k-way merge of the legs.

---

### 1.6 ~~OFFSET correctness on scatter merge~~ — RESOLVED (Phase 1.1, May 2026)

**Status.** All three previously-failing tests now pass. The OFFSET is
applied post-merge in [src/engine/engine_scatter.c](../src/engine/engine_scatter.c)
(`apply_limit`); the originally-cited bug was actually a missing seed
fixture in `TestOrderLimitOffset` (xfail removed in commit `c4c5089`).

Original analysis retained below for archaeology.

**Failing tests (was 3):**
- `TestOrderLimitOffset::test_limit_offset_page`
  → `[11,13,15,17,19,21,…] == [10,11,12,13,14,15,…]` (every other row)
- `TestOrderLimitOffset::test_offset_skips_correct_rows`
  → `[31,33,35,…] == [30,31,32,…]`
- `TestScatterAggregatesComprehensive::test_order_by_offset`

**Observed behaviour.** `ORDER BY id LIMIT 10 OFFSET 10` returns rows
`11,13,15,…` — KEEL is pushing the OFFSET to each shard, so each shard
skips 10 rows locally and the merged stream skips `10 × N_SHARDS = 20`
rows. The interleaving artefact (only odd offsets) comes from the
2-way merge.

**Root cause.** In `engine_scatter.c` the LIMIT push-down rewrites
`LIMIT n OFFSET m` to `LIMIT (n+m)` per shard (correct) but the proxy's
merge step does not re-apply the OFFSET after the merge. It only applies
LIMIT. So merged tuples 0..n+m-1 are returned, but the first m should
be skipped post-merge.

**Impact.** Pagination is **silently wrong** beyond the first page.
Users will see duplicated and missing rows when paging.

**Workaround.**
- Use cursor-style pagination (`WHERE id > last_seen ORDER BY id LIMIT n`)
  which is shard-routable when `id` is the shard key.
- Avoid `OFFSET` entirely (PostgreSQL best practice anyway).

**Fix sketch.**
1. In the LIMIT push-down rewriter (look for `sc_rewrite_limit_offset`
   in `engine_scatter.c`), strip the OFFSET from per-shard SQL but
   *remember* it in the merge state.
2. In the merge consumer loop, drop the first `m` merged rows before
   forwarding to the client.

**Estimated effort.** **1 day**. This is a real bug, narrow scope,
fully testable.

**Risks.**
- ORDER BY must be present and correct; OFFSET without ORDER BY is
  undefined in PostgreSQL — keep the existing behaviour (each shard
  applies its own).
- LIMIT 0 OFFSET n must still drain enough rows from each shard.

---

### 1.7 ~~NULLS FIRST / NULLS LAST in merge sort~~ — RESOLVED (verified May 2026)

**Status.** `TestNullHandlingComprehensive::test_order_by_nulls_first`
passes; the merge comparator now honours the parsed `nulls_first` flag
end-to-end.

Original analysis retained for reference.

**Failing tests (was 1):**
- `TestNullHandlingComprehensive::test_order_by_nulls_first`
- `TestNullHandlingComprehensive::test_group_by_excludes_null_keys`

**Observed behaviour.** `ORDER BY x NULLS FIRST` puts NULLs in the
default position (last for ASC, first for DESC). The merge comparator
ignores the explicit nulls-ordering modifier.

**Root cause.** The k-way merge comparator in `engine_scatter.c` treats
NULL using a fixed convention (probably "larger than any value"). The
parsed ORDER BY clause carries the NULLS FIRST/LAST flag but it's not
threaded into the comparator.

**Impact.** Sort order with NULLs is **silently wrong** for queries that
care. Most apps don't rely on NULLS FIRST/LAST.

**Workaround.** Move the ORDER BY computation client-side or filter out
NULLs with `WHERE x IS NOT NULL`.

**Fix sketch.**
1. Extend the parsed sort-key descriptor with a `nulls_first` flag.
2. Update the comparator: if `nulls_first` and either side is NULL,
   the NULL side is "smaller"; for `nulls_last` it is "larger".
3. Add NULLS FIRST/LAST to the per-shard SQL push-down so each shard
   already returns rows in the requested null order.

**Estimated effort.** **1 day** with a focused test.

**Risks.**
- ORDER BY DESC NULLS LAST has a different default in PostgreSQL than
  ASC — the comparator must be parameterised, not hard-coded per direction.

---

### 1.8 ~~GROUP BY drops NULL keys (or duplicates them)~~ — RESOLVED (verified May 2026)

**Status.** `TestNullHandlingComprehensive::test_group_by_excludes_null_keys`
passes; the merge group-key equality now uses `IS NOT DISTINCT FROM`
semantics so a single NULL bucket is produced.

Original analysis retained for reference.

**Failing tests (was 1):**
- `TestNullHandlingComprehensive::test_group_by_excludes_null_keys`

**Observed behaviour.** A `GROUP BY x` where `x` has NULL values
produces either no NULL bucket or one NULL bucket per shard.

**Root cause.** The merge layer's group-key equality treats `NULL = NULL`
as false (SQL semantics) but GROUP BY treats them as equal. The
post-merge re-aggregator uses the wrong predicate.

**Impact.** Aggregates over nullable group keys are wrong.

**Workaround.** `COALESCE(x, '<null>') AS x_grp` in the GROUP BY.

**Fix sketch.** Extend the merge group-key equality to use IS NOT
DISTINCT FROM semantics (NULL ≡ NULL).

**Estimated effort.** **0.5 day**.

**Risks.** Performance: NULL bucket must be a real key, not a sentinel
that can collide.

---

### 1.9 CASE expressions in / over aggregates

**Failing tests (3):**
- `TestComplexQueries::test_case_expression_in_aggregate`
  → `assert 6 == 12` (per-shard sums of CASE arms not combined)
- `TestComplexQueries::test_case_expression_in_group_by`
  → `assert 6 == 3` (CASE-derived group key not merged)
- `TestComplexQueries::test_derived_table_subquery`
  → `assert 6 == 12`

**Observed behaviour.** `SUM(CASE WHEN x > 10 THEN 1 ELSE 0 END)` and
`GROUP BY CASE WHEN x > 10 THEN 'big' ELSE 'small' END` are not
recognised as mergeable, so per-shard partial results are concatenated.

**Root cause.** The aggregate-merge classifier in `engine_scatter.c`
only matches simple `<aggfunc>(<colref>)` projections. Anything
synthesised (CASE, COALESCE, function call, expression tree) is
treated as opaque and skipped from the merge spec.

**Impact.** Conditional aggregates and derived group keys are **silently
wrong** under scatter. Very common pattern in analytics.

**Workaround.** Pre-compute the conditional column with a wrapper
SELECT (also subject to §1.2 if wrapped in a CTE).

**Fix sketch.**
1. Build a small expression evaluator that can re-apply scalar
   expressions (CASE, COALESCE, arithmetic, function calls) in the
   merge layer over per-shard partial state.
2. Or simpler: detect the pattern, push the CASE down so each shard
   returns the *post-CASE* column, then merge as if it were a plain
   column reference.

**Estimated effort.** Pushdown approach: **3–4 days**.
Full expression eval: **1–2 weeks**.

**Risks.** Volatile functions (`random()`, `now()`) inside CASE must
not be pushed twice — execute once or refuse.

---

### 1.10 JSONB scatter operations

**Failing tests (6):**
- `TestJSONBScatter::test_array_agg_over_scatter`
- `TestJSONBScatter::test_jsonb_agg_over_scatter`
- `TestJSONBScatter::test_jsonb_category_sum_with_filter`
- `TestJSONBScatter::test_jsonb_group_by_in_stock`
- `TestJSONBScatter::test_jsonb_numeric_field_extraction`
- `TestJSONBScatter::test_string_agg_over_scatter`

**Observed behaviour.** `jsonb_agg`, `array_agg`, `string_agg`, and
group-by on `(payload->>'field')` all fail merge: per-shard values are
returned individually, not combined.

**Root cause.** Two intertwined issues:
1. The merge classifier doesn't recognise `jsonb_agg/array_agg/string_agg`.
2. Group keys derived from `jsonb` operators (`->>`, `#>>`) are
   expressions (§1.9) and aren't pushed down or normalised.

**Impact.** JSONB analytics are unusable across shards. With JSONB-heavy
schemas (event-store style), this is a **showstopper**.

**Workaround.**
- Promote the JSONB field to a real column at write time.
- Pin the query to a single shard.

**Fix sketch.**
1. Add `array_agg`, `jsonb_agg`, `string_agg`, `jsonb_object_agg` to the
   merge function table. Their merge ops are concatenation (with
   ordering preserved per shard, ordering across shards undefined).
2. Apply expression-pushdown (§1.9) for JSONB extractor expressions in
   the GROUP BY clause.

**Estimated effort.** Aggregate set: **2 days**. JSONB GROUP BY
(depends on §1.9): same scope.

**Risks.**
- `array_agg(x ORDER BY y)` requires a global sort — falls under §1.5/§1.6.
- `string_agg(x, sep ORDER BY y)` same.

---

### 1.11 ~~Scatter routing edge cases (BETWEEN / IN / COALESCE)~~ — RESOLVED (verified May 2026)

**Status.** All three originally-failing tests in
`TestPredicateBasedRouting` pass. The router correctly narrows the
participating shard set for BETWEEN, IN-list, and COALESCE-wrapped
shard-key predicates. The remaining
`TestRoutingLimitations::test_in_list_routes_to_subset_of_shards` failure
is a separate `EXPLAIN SHARD PLAN` syntax-error issue tracked under
[§1.12.5 EXPLAIN SHARD PLAN](#11214-explain-shard-plan-syntax) below.

Original analysis retained for reference.

**Failing tests (3):**
- `TestRoutingEdgeCases::test_between_scatter_returns_range`
  → `assert 'alpha' in []` (BETWEEN range over scatter loses rows)
- `TestRoutingEdgeCases::test_coalesce_in_scatter_select`
  → `assert 'x@y' == 'N/A'` (COALESCE returned a per-shard NULL)
- `TestRoutingEdgeCases::test_in_clause_crosses_shards`
  → `assert [40,45,…] == [25,30,35,40]`

**Observed behaviour.**
- `WHERE id BETWEEN -50 AND 50` should scatter — it does, but result
  ordering / dedup at proxy mishandles boundary rows.
- `SELECT COALESCE(email, 'N/A') FROM users WHERE id = N` — for a row
  located on shard A, shard B returns nothing and KEEL accidentally
  prefers shard B's empty response.
- `WHERE id IN (k1, k2, …)` is dispatched as a scatter even when KEEL
  could compute the union of target shards from the in-list.

**Root cause.**
- BETWEEN: scatter dispatch handles the request but the merge for a
  multi-row result preserves per-shard ORDER BY and the test expects
  global order.
- COALESCE single-row: when scatter returns 0 rows from a shard, the
  proxy may forward the null result before the row-bearing shard
  responds (race in the result-collection loop).
- IN-list: optimisation gap — should pre-resolve to the union of
  shards matching `hash(k_i) % N` for each constant.

**Impact.** Range / IN-list queries can return wrong row sets.
Severity medium — most affected workloads also hit §1.6 (OFFSET) or
§2 (parameterized) which are higher priority.

**Workaround.**
- Use an explicit `ORDER BY` on the client.
- Decompose `IN (a,b,c)` into per-key queries.

**Fix sketch.**
1. **IN-list fast path:** in `keel_router_dispatch_sql()`, when the only
   shard-key predicate is `key IN (lit, lit, …)`, compute the set of
   target shards and return a `KEEL_DISPATCH_TARGETED` (subset of shards),
   not a full scatter.
2. **COALESCE single-row:** make scatter wait for *all* shard responses
   before deciding whether the row exists, or short-circuit on the first
   non-empty response — current code does the wrong middle ground.
3. **BETWEEN ordering:** ensure the merge uses a stable comparator and
   that the per-shard SQL carries the same ORDER BY.

**Estimated effort.** IN-list: **1 day**. COALESCE: **0.5 day** (race
in `engine_scatter.c::sc_collect_results`). BETWEEN ordering: **1 day**.

**Risks.**
- Targeted-subset dispatch must coexist with txn pinning.
- The COALESCE fix must not introduce a head-of-line blocker for
  long-tail single-row queries on healthy shards (timeout per shard).

---

### 1.12 HAVING filtering & quintile bucketing

**Failing tests (4):**
- `TestScatterAggregatesComprehensive::test_having_count_eliminates_small_groups`
- `TestScatterAggregatesComprehensive::test_having_filters_groups`
- `TestComplexAggregationAtScale::test_balance_quintile_grouping`
  → `Quintile 1: 500 rows ≠ expected 1000`
- `TestComplexAggregationAtScale::test_users_age_buckets_group_by`
  → `young: 410 ≠ 820`
- `TestComplexAggregationAtScale::test_events_having_count_threshold`
- `TestComplexAggregationAtScale::test_events_running_sum_global_total`
  → `assert 51000 == 101000`

**Observed behaviour.** HAVING filters are applied per-shard before the
merge, so groups that would be globally over the threshold are dropped
locally. Bucketing (`width_bucket`, `ntile`, custom CASE buckets) is
not recombined either.

**Root cause.** HAVING push-down is too aggressive: a group with count
20 on shard A and 25 on shard B (global 45) is dropped at threshold 30
on each shard. The push-down should drop HAVING from per-shard SQL when
the predicate is over an aggregate that requires post-merge.

**Impact.** HAVING with aggregate predicates produces **wrong row sets**
under scatter.

**Workaround.** Compute the aggregate in a subquery and apply the
filter at the outer level (still subject to §1.2 if CTE-wrapped).

**Fix sketch.**
1. In the per-shard SQL rewriter: if HAVING references aggregates that
   require post-merge (`COUNT`, `SUM`, …), strip HAVING from the
   per-shard SQL and re-apply it after merge.
2. Bucketing: same fix as §1.9 (push down the bucket expression so the
   per-shard aggregate is keyed correctly).

**Estimated effort.** HAVING strip-and-reapply: **1–2 days**. Bucket
expressions: covered by §1.9.

**Risks.**
- HAVING with non-aggregate predicates (`HAVING dept = 'eng'`) must
  remain pushed down for performance.
- HAVING with mixed aggregate + scalar: split into pushable and
  non-pushable conjuncts.

---

### 1.13 Concurrent UPDATE — last-writer wins versus lost update

**Failing tests (1):**
- `TestConcurrentWriteIntegrity::test_concurrent_updates_no_lost_update`

**Observed behaviour.** N concurrent transactions doing
`UPDATE counters SET v = v + 1 WHERE id = K` end with `v < N`. Some
updates are lost.

**Root cause.** With the new scatter-DML aggregator (commit `624420a`),
DML targeting a *non-shard-key* row scatters across all shards and
returns the sum of affected rows. But a `WHERE id = K` should route to
a single shard via the routing layer — if the router fails to extract
the key (e.g. parameterised — see §2), the proxy scatters the same
update to every shard, and every shard does its own row lookup +
increment. Each shard sees its own snapshot; concurrent writers race
on the *single backing row* on the home shard, and N–1 of the
N writers see the pre-update value due to the race window.

**Impact.** Counters / atomic updates are **silently incorrect** under
concurrency when the shard key extraction misfires (very common with
parameterized queries — see §2).

**Workaround.**
- Wrap the increment in `BEGIN; … COMMIT;` so the txn-pinned backend
  serialises against itself.
- Use `UPDATE … RETURNING v` and retry on stale read.

**Fix sketch.** Almost entirely subsumed by §2 (parameter-aware
routing): once `WHERE id = $1` routes to a single shard, the existing
PostgreSQL row-level locks serialise correctly. Marker test until §2
ships.

**Estimated effort.** Subsumed by §2.

**Risks.** None beyond §2.

---

## 2. Parameter-aware routing (Bind-time shard-key extraction)

**Failing tests (5 directly attributable, plus drives §1.13 and parts of §3):**
- `test_routing.py::TestDeterministicRouting::test_negative_keys_route_correctly`
- `test_transaction_pooling.py::TestScenarioA_Autocommit::test_1000_autocommit_insert_delete_cycles`
- `test_transaction_pooling.py::TestCornerCases_PoolWaitingQueue::test_pool_waiting_queue_resolves_when_backends_free`
- `test_transaction_pooling.py::TestCornerCases_ConsecutiveTransactionsSameConnection::test_1000_consecutive_transactions_same_psycopg2_connection`
- `test_sharding_comprehensive.py::TestConcurrentWriteIntegrity::test_concurrent_updates_no_lost_update`

**Observed behaviour.** A query like
`DELETE FROM users WHERE id = $1` issued via the extended PostgreSQL
protocol (Parse / Bind / Execute) cannot have its shard key extracted
at Parse time (the AST contains `$1`, not a literal). The router then
treats it as `KEEL_SHARD_KEY_PARAM` / `KEEL_SHARD_KEY_NONE` and falls
back to scatter or round-robin. With the new scatter-DML aggregator
this *works* (deletes all matching rows) but defeats the entire point
of sharding (now every DML is N× the load) and breaks tests that
verify single-shard routing of parameterised statements.

**Negative key test:** Python helper inserts using parameters; KEEL
round-robins because no key was extracted; first row lands on shard 0
when test expects shard 1 (= `abs(-1) % 2`).

**1000-cycle autocommit test:** Even at autocommit, each DELETE that
scatters opens N pool connections and increments the pool's "total
connections" counter. After 1000 cycles the pool exhausts.

**Pool-waiting-queue test:** When all backends are leased to long
scatters from parameterised queries, simple key-routable queries can't
acquire a backend.

**Consecutive-transactions test:** Parameterised DML inside an explicit
transaction is gated to *not* scatter (commit `624420a` chose
correctness here), so it now hits a single round-robin'd shard — wrong
shard half the time, the row isn't there, and subsequent reads fail.

**Root cause.** `keel_router_dispatch_sql()` extracts shard keys from
the AST at Parse time. Bind values arrive *later* on the wire and are
not propagated back into a deferred routing decision. The full call
graph for the Bind path is in `src/protocol/postgres/postgres_flow.c`
(Bind handling) and `src/core/router_weighted.c` (dispatch), but they
do not currently exchange parameter values.

**Impact.** **HIGH.** Almost every modern PostgreSQL client uses
prepared statements / parameterised queries. Without Bind-time routing,
KEEL's sharding is effectively bypassed for those clients on writes.

**Workaround.**
- Use raw SQL with literal values (defeats parameter binding's purpose,
  exposes to SQL injection if not careful).
- Use `SET keel.pin = on` per session to pin to a shard known to hold
  the data (only works with single-shard data).
- Use the simple-query protocol (psql `\;` mode, JDBC `useServerPrepared
  Statements=false`).

**Fix sketch.**
1. **AST tagging at Parse time.** When parsing, instead of failing, emit
   a deferred routing plan: a list of `($n parameter index, role)` —
   e.g. "$1 is the shard key for table users".
2. **Defer routing to Bind.** In the Bind handler, look up the
   parameter values, materialise the shard key, and call
   `keel_shard_map_key()` to pick a shard. Only then acquire a backend
   and forward Parse + Bind + Execute.
3. **Negative-int handling.** Verify `keel_shard_map_key()` uses
   `abs(key) % shard_count` not `(uint64_t)key % shard_count`. The C
   side currently does `(uint64_t)int64_value` which differs from
   abs-based hashing for negative integers (test
   `_keel_hash_shard` documents `abs(key) % shard_count` as the
   contract).
4. **Numeric format codes.** Bind values can be text (`%d`) or binary
   (network-byte-order). Cover both per the format-code array in Bind.
5. **Multi-Execute** under one Parse must re-evaluate routing per Bind.
6. **Cache** the parsed shard-key plan keyed on the prepared-statement
   name so repeated Bind/Execute pairs are O(1).

**Estimated effort.** **5–8 days** for the routing path, plus 2 days
for the Bind-format-binary parsing and 2 days for tests.
Total: **~2 weeks**.

**Risks & corner cases.**
- An IN-list with parameters (`WHERE id IN ($1,$2,$3)`) — must
  enumerate the union of target shards (intersect with §1.11).
- Composite shard keys (multiple columns) — must collect *all* of them
  before routing; deferred routing must wait for the right Bind format.
- Parse with no Bind yet: a Describe before Bind must answer with a
  union row description that's compatible with any shard's reply.
  Easiest: route Describe to a single shard for type info, then re-route
  on Bind. Works only if all shards have the same schema (KEEL invariant).
- Crossing transaction boundaries with deferred routing: once pinned,
  ignore deferred routing.
- `Parse('', sql)` (unnamed statements) — re-deferred routing must
  not leak per-call.
- Bind arrives with `format_codes=[1]` (binary) for an `int8` shard
  key: must decode big-endian 8-byte int correctly, including negative.
- Memory: parameter values can be large (`bytea`); avoid copying when
  the shard key is the first 8 bytes of a 2 MB blob.
- The deferred routing decision must be cached so a single Parse +
  multiple Binds doesn't re-classify the SQL N times.

---

## 3. Prepared-statement virtualisation (SSV) under load

**Failing tests (25, all in `test_prepared_statements_ssv.py`):**

| Class | Tests | Theme |
|---|---|---|
| `TestPS_Virtualize` (4) | named_parse_returns_parse_complete, bind_execute_returns_result, parameterised_query, two_params | Basic SSV correctness |
| `TestPS_Pinning` (4) | named_parse_pins_backend, pinned_backend_serves_multiple_statements, deallocate_individual_keeps_pin, deallocate_all_releases_pin | SSV-off pinning |
| `TestPS_Tracking` (4) | simple_prepare_tracked, simple_prepare_with_params, simple_prepare_survives_50_transactions, deallocate_removes_tracked_stmt | Tracking layer |
| `TestPS_Anonymous` (5) | named_parse_works_transparently, backend_has_no_named_statements, pg_prepared_statements_always_empty_for_named, close_named_absorbed_by_proxy, describe_unnamed_works | SSV anonymisation |
| `TestPS_Off` (4) | named_parse_forwarded_to_backend, backend_pins_on_first_parse, individual_deallocate_keeps_pin, deallocate_all_releases_pin | SSV-off mode |
| `TestPS_Compatibility` (1) | 100_rapid_prepare_execute_cycles | Throughput |
| `TestSSV_BackendReuse` (3) | stmts_survive_pool_pressure, search_path_change_does_not_corrupt_stmt_results, clean_backend_replay_correctness_multiple_stmts | Backend rotation |

**Observed behaviour.** All 25 tests pass when run in isolation
(verified). They fail in the full suite with `psycopg2.OperationalError:
server closed the connection unexpectedly` (KEEL-side connection close)
or `TimeoutError: timed out` (pool starvation).

**Root cause (compound).**
1. **Heap header corruption** (see §4) under sustained scatter activity
   from earlier tests in the suite poisons in-use buffers; one corrupted
   buffer forces KEEL to close the connection during ParseComplete /
   BindComplete.
2. **State pollution between sessions:** named-statement bookkeeping
   tied to backend instances may not be fully reset when a backend is
   recycled after a forced disconnect.
3. **Pool pressure** from earlier scatter-heavy tests leaves the pool
   in a state where SSV's "borrow a clean backend, replay statements"
   step can't acquire a free backend within the test timeout.

**Impact.** SSV is the production-grade prepared-statement strategy.
Tests passing in isolation but failing under load means real
production traffic mixing scatter analytics with prepared-statement
OLTP can crash sessions. **HIGH severity.**

**Workaround.**
- Run scatter analytics in a separate KEEL pool (separate process).
- Disable SSV (`prepared_statements = off`) and accept session pinning
  on first Parse (limits pool sharing).

**Fix sketch.**
1. **Fix §4 first** (the allocator corruption). With clean memory,
   spurious connection drops disappear.
2. **Audit SSV state-machine reset paths** in `src/session/ssv.c`:
   ensure that on backend disconnect, the per-session
   `prepared_statements` table is invalidated and the backend's
   per-statement cache is purged.
3. **Replay quota / timeout** — a backend that takes too long to
   replay N prepared statements must surface a clear error to the
   client rather than silently timing out the whole pool.
4. **Pool fairness** — when SSV needs a backend for replay, do not let
   long-running scatters starve the request indefinitely; introduce a
   priority lane or per-session reservation.

**Estimated effort.** Once §4 is fixed: most of these tests will pass
without further work. SSV reset audit + tests: **3–5 days**. Pool
fairness: **1 week**.

**Risks & corner cases.**
- A backend that disconnects mid-replay may have replayed K of N
  statements; new statements bound after K but referencing an
  un-replayed name must trigger a re-replay, not error.
- `search_path` change (test
  `test_search_path_change_does_not_corrupt_stmt_results`) — KEEL must
  re-replay search_path *and* prepared statements when rotating
  backends, in the right order.
- `DEALLOCATE ALL` must clear both the proxy's tracking and any
  to-be-replayed list on idle backends.

---

## 4. ~~Heap header corruption under concurrent scatter~~ — RESOLVED (allocator-mismatch false positive)

**Status.** **FIXED.** Originally diagnosed as a heap-header overflow,
but instrumentation (header dump + libc backtrace, see
`src/mem/mem.c::keel_free`) revealed the actual root cause: the admin
HTTP endpoints for `/metrics` (Prometheus) and the JSON status API
allocated their response body via `open_memstream()` — i.e. a
**libc-`malloc`** buffer — but released it with `keel_free()`.
`keel_free()` then interpreted the libc malloc-chunk header as a
`keel_alloc_header_t`, found garbage where the magic should be, and
logged a spurious "Invalid memory block" error. **No actual memory
corruption was occurring.**

The "magic value" we observed (`0x00003e84..0x00003ea5`) was simply
the low 32 bits of the libc chunk-`size` field, which scales with the
serving allocation footprint and looks suspiciously sequential because
both endpoints emit responses of similar size on each scrape.

**Symptoms before the fix.**
```
Invalid memory block in free (possible double-free or corruption):
ptr=0x… header=0x… magic=0x00003ef4 expected=0xdba1100c
size_field=8208 userdata[0..31]=232048454c50206b 65656c5f73657373 …
                                  ^ ASCII: "# HELP keel_sess…"
backtrace:
  prom_write_metrics      admin.c:4821
  handle_prom_http        admin.c:5016
  admin_thread_func       admin.c:5211
```

**Frequency before the fix.** ~36 occurrences per full e2e run,
correlating exactly with Prometheus scrape cadence (every 10–15 s on
the test stack).

**Impact.** **LOW** in retrospect: log noise + a small per-scrape
leak (`free()` was never called on `body`, so glibc kept the
chunk on the per-thread heap until process exit). No data
corruption, no crash, no pool churn.

**Fix.** `src/admin/admin.c` — switch the two `open_memstream()`
consumers (`handle_status_http` and `handle_prom_http`) from
`keel_free(body)` to `free(body)`. Plus diagnostic upgrades that
made this trivial to root-cause:
1. `src/mem/mem.c::keel_free` now dumps the header's `size` field and
   the first 32 bytes of user data when the magic check fails.
2. `src/mem/mem.c` added a `keel_mem_log_backtrace()` helper using
   `<execinfo.h>`; called from any corruption-detection path.
3. `src/main/CMakeLists.txt` enables `ENABLE_EXPORTS` so symbols
   resolve in `backtrace_symbols()` without needing `addr2line` and
   the original build tree.

**Verification.** Running ~50 concurrent `curl /metrics` requests
against the rebuilt binary produces zero allocator errors; full
e2e suite produces zero `"Invalid memory block"` log lines.

**Lessons learned (documented in user-memory).**
- Allocator wrappers that prepend a header MUST be paired with their
  matching free function — never mix `keel_free()` with libc-allocated
  buffers (`open_memstream`, `asprintf`, `getline`, `getdelim`,
  `strdup` etc.).
- When triaging "magic corruption" errors, dump the header's full
  contents *and* the user-data prefix before assuming a real
  overflow — one byte of context per slot would have shown
  "ASCII text starts at offset 0" and pointed at `open_memstream`
  immediately.

---

## 5. Pool burst resilience

**Failing test (1):**
- `test_pool_behavior.py::TestPoolGrowthAndShrink::test_active_connections_rise_under_burst`
  → `'thread N: server closed the connection unexpectedly'` for 2 of 8 burst threads.

**Observed behaviour.** When 8 clients hit a cold KEEL with full
connection bursts, 1–2 of them get their connection torn down before
the pool finishes growing.

**Root cause.** Pool growth is racy at the moment of cold-start: the
first wave of clients beat the pool to its `max_size` ceiling; some of
them are evicted to make room for new entries. The eviction path
closes the front-end socket rather than queuing the request.

**Impact.** Cold starts under aggressive concurrency can drop
connections. In production this is benign (clients reconnect). In
tests it's a flake/failure.

**Workaround.** Pre-warm the pool (`pgbench -c 1 -t 1`) or set
`min_size = max_size`.

**Fix sketch.** When the pool is at `max_size` and receives a new
request, queue the request rather than evicting an in-use connection.
Add an `active_connections_high_water_mark` metric.

**Estimated effort.** **1–2 days**.

**Risks.** Unbounded queue → DoS. Cap queue length and reject with a
clear error after threshold.

---

## 6. Prepared-statement TPS regression on fast path

**Failing test (1):**
- `test_performance.py::TestSingleConnectionPerformance::test_sql_level_prepare_execute_tps`

**Observed behaviour.** SQL-level `PREPARE foo AS …; EXECUTE foo(…);`
TPS is below the test's lower bound.

**Root cause.** SSV adds extra processing on every Parse/Bind/Execute:
the proxy parses the SQL, looks up the cached plan, replays prepares
on cold backends. The current implementation pays this cost even when
the same backend is reused.

**Impact.** Performance only — the test is a perf budget, not a
correctness test. Median TPS is probably acceptable; the test sets a
strict bound.

**Workaround.** Set `prepared_statements = off` for latency-sensitive
single-connection workloads.

**Fix sketch.**
1. **Fast path for stable backend:** if the connection has been pinned
   since its last replay and no new statements were prepared, skip the
   Parse-rewrite and forward unchanged.
2. **SQL parse cache:** memoise `keel_router_dispatch_sql()` results
   keyed on the literal SQL string for the connection's lifetime
   (reset on `DEALLOCATE`).
3. **Profile:** run `perf top` against KEEL doing SSV-EXECUTE traffic
   and pick the top 3 functions.

**Estimated effort.** Profiling + low-hanging fruit: **2–3 days**.

**Risks.** A SQL parse cache must be invalidated on DDL and on pool
backend swap; getting invalidation right is the hard part.

---

## 7. Cross-shard `NOTIFY` / `LISTEN`

**Failing test (1):**
- `test_transaction_pooling.py::TestCornerCases_ListenNotify::test_notify_delivered_to_listener`
  (currently `xfail(strict=False)` — passes ~1/N of the time, where N is the shard count)

**Observed behaviour.** A frontend session that issues `LISTEN ch` on one
KEEL connection does not reliably receive `NOTIFY ch, '…'` issued from a
second KEEL connection. Empirically the test passes ~50% of the time
against a 2-shard stack (exactly the expected coin-flip).

**Root cause.** `NOTIFY` and `LISTEN` are **per-PostgreSQL-instance**
primitives — the PG backend that handles `NOTIFY` only signals
LISTENers attached to the *same* PG instance. With sharded KEEL each
shard is a separate PG instance:

1. `LISTEN ch` pins the listener's frontend session to a single backend
   on shard *A* (`KEEL_PIN_LISTEN` in `hardpin.c`).
2. The notifier opens a separate KEEL connection. It has no shard key,
   so the dispatcher routes `NOTIFY` via the default policy, landing on
   shard *A* or shard *B* essentially at random.
3. When `NOTIFY` lands on shard *B*, PostgreSQL on instance *B* signals
   its local listeners only — the listener on instance *A* never sees
   the notification.

**Impact.** Applications relying on PostgreSQL `LISTEN`/`NOTIFY` for
pub-sub do not work transparently behind sharded KEEL. The corner-case
test of `LISTEN`/`UNLISTEN` *cycle* still passes (no cross-instance
delivery is required there), and the test that verifies LISTEN-pinning
keeps the backend PID stable also passes — only cross-connection
delivery is affected.

**Workaround.** Pin both the listener and the notifier to the same
shard explicitly (e.g. by issuing a shard-key-bearing query on each
connection before `LISTEN` / `NOTIFY`), or use a single PG instance
deployment for workloads that depend on `LISTEN`/`NOTIFY`.

**Fix sketch.** The proxy must fan `NOTIFY` out to **all** shards so
that every PG instance signals its local listeners. Concretely:

1. In `keel_router_dispatch_sql`, detect `NOTIFY` (analyzer already
   emits `KEEL_QUERY_LISTEN_NOTIFY`) and force `KEEL_DISPATCH_SCATTER`
   over every server in the pool — independent of shard rules.
2. The engine flow's scatter path already handles
   `CommandComplete`-only responses (no row aggregation needed for
   `NOTIFY`); the merge can simply return the first shard's
   `CommandComplete`.
3. No changes to `LISTEN`: it remains pinned (one frontend session ↔
   one backend) and the pinned backend continues to deliver async
   `NotificationResponse` messages as soon as its PG instance signals
   it.

**Estimated effort.** **1–2 days** to add the NOTIFY-broadcast path
plus an E2E test that loops the delivery 20× to detect any regression.

**Risks.** Broadcasting NOTIFY multiplies the per-shard wakeups, but
NOTIFY is rare in OLTP workloads so the cost is negligible. The
broadcast is **idempotent** per shard — each PG instance receives one
NOTIFY and signals its local listeners; no double delivery to a single
listener can occur.

---

## Cross-cutting concerns

### Telemetry gaps

Several limitations above produce **silent wrong results**. KEEL now
exposes a single Prometheus counter so operators can detect affected
workloads without enabling verbose logging:

```
# HELP keel_scatter_unsupported_pattern_total Scatter dispatches whose
#      correctness relies on patterns KEEL does not fully merge
# TYPE keel_scatter_unsupported_pattern_total counter
keel_scatter_unsupported_pattern_total{kind="percentile"}    N
keel_scatter_unsupported_pattern_total{kind="window_func"}   N
keel_scatter_unsupported_pattern_total{kind="recursive_cte"} N
keel_scatter_unsupported_pattern_total{kind="union_all"}     N
keel_scatter_unsupported_pattern_total{kind="dml_returning"} N
keel_scatter_unsupported_pattern_total{kind="ddl"}           N
```

Bumped from `keel_router_dispatch_sql()` after the dispatch decision is
finalized. All six label slots are wired:
`percentile`, `window_func`, `recursive_cte`, `union_all` (Phase 1.5)
and `dml_returning`, `ddl` (Phase 9 — incremented in `dispatch_done`
when `out->kind == KEEL_DISPATCH_SCATTER` and the parsed AST kind
matches DML-with-RETURNING or DDL respectively).

Recommended alert rule:

```yaml
- alert: KeelScatterUnsupportedPattern
  expr: rate(keel_scatter_unsupported_pattern_total[5m]) > 0
  for:  10m
  labels: { severity: warning }
  annotations:
    summary: "KEEL is dispatching scatter queries that may return wrong results"
    description: "kind={{ $labels.kind }} — see docs/LIMITATIONS.md"
```

Still missing:
- A counter for parameterised DML dispatched as scatter (i.e. shard-key
  extraction failed at Bind — see §2).
- A counter for merge layer dropping or duplicating rows due to a known
  limitation.

### Documentation gaps

The `docs/` folder does not currently document which SQL features are
supported under scatter. Each fix above should add (or remove) a row
from a forthcoming `docs/sql-feature-matrix.md`.

### Test-suite hygiene

Roughly 8 of the 76 failing tests are *aspirational* — they describe
the desired behaviour without an underlying implementation.
Recommendation: mark them `xfail(strict=True, reason=…)` referring back
to the relevant section here, and remove the `xfail` as each fix
ships. This keeps the suite green while preserving the regression
canary.

---

## Summary table

Updated 2026-05-22 against the actual `--runxfail` failure inventory of
`test_sharding_limitations.py` (19 failures, all in
[tests/e2e/test_sharding_limitations.py](../tests/e2e/test_sharding_limitations.py)).

| § | Limitation | Tests | Severity | Effort | Owner |
|---|---|---|---|---|---|
| 1.1 | Recursive CTEs (silent dup) | 0* | **Resolved** | — | engine |
| 1.2 | Non-recursive CTEs (writable) | 0 | **Resolved** (Phase 8b) | — | engine (parser) |
| 1.2a | Constant CTE row duplication | 0 | **Resolved** (Phase 8a) | — | router |
| 1.3 | Window functions (nth_value, cume_dist, last_value) | 3 | Medium | 2 wk | engine |
| 1.4 | Percentile aggregates (cont, disc) | 2 | Medium | 3 d / 1.5 wk | engine |
| 1.5 | UNION ALL across scatters | 0* | tracked via `kind=union_all` counter | — | engine |
| 1.6 | OFFSET correctness | 0 | **Resolved** (Phase 1.1) | — | — |
| 1.7 | NULLS FIRST/LAST | 0 | **Resolved** | — | — |
| 1.8 | GROUP BY NULL keys | 0 | **Resolved** | — | — |
| 1.9 | CASE in agg / group | unmeasured | High (silent) | 3–4 d | engine |
| 1.10 | array_agg / json_object_agg single-row | 0 | **Resolved** (Phase 5) | — | — |
| 1.11 | BETWEEN / IN / COALESCE | 0 | **Resolved** | — | — |
| 1.12 | HAVING / bucketing | unmeasured | High (silent) | 1–2 d | engine |
| 1.13 | Concurrent UPDATE losses | 0 | **Resolved** | — | — |
| — | Scatter routing (multi-row INSERT, OR, composite, schema-qualified, BETWEEN single) | 5 | **High** (silent mis-route) | 3–5 d | router |
| — | Scatter DML RETURNING (UPDATE, DELETE) | 2 | **High** | 3–5 d | engine |
| — | DISTINCT cross-shard (bare) | 0 | **Resolved** (Phase 4) | — | — |
| — | COUNT(DISTINCT) via subquery | 1 | High (silent) | 1–2 d | router (rewrite) |
| — | EXPLAIN SHARD PLAN syntax | 1 | Low (developer tool) | 0.5 d | admin |
| 2 | Bind-time shard-key routing | 5 + cross-cuts | **HIGH** | ~2 wk | router |
| 3 | SSV under load | unmeasured | **HIGH** | 3–5 d after §4 | session |
| 4 | Allocator corruption | (stability) | **Resolved** | — | mem |
| 5 | Pool burst resilience | 1 | Low | 1–2 d | pool |
| 6 | PS fast-path TPS | 0 | **Resolved** | — | — |

> \* §1.1 / §1.5 are tracked via the `keel_scatter_unsupported_pattern_total`
> counter (`kind="recursive_cte"`, `kind="union_all"`); operators alert on
> the metric, no test currently asserts result correctness.

**Critical-path bugs** (silent wrong results, sharding bypass — to fix
before any production roll-out): scatter routing edge cases (5 tests),
scatter DML RETURNING, §1.12 HAVING, §1.9 CASE, §2.

**Aspirational** (large features that legitimately need design
discussion): §1.3 (windows beyond Tier-1/2), §1.4 (percentile global
merge), §1.5 (UNION-with-merge), deeper §1.2 (CTE materialisation).
