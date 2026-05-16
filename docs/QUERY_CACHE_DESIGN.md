# Query Result Caching — Design & Implementation Guide

**Objective:** Cache repeated identical read queries in an in-process per-worker LRU store to eliminate backend round-trips.

---

## Overview

Query result caching eliminates repeated backend round-trips for hot read queries.

```
Client query "SELECT * FROM users WHERE id = 42"
    ↓
Keel cache lookup (by query digest)
    ├─ HIT → return cached result immediately (no backend borrow)
    └─ MISS → forward to backend, accumulate result, cache it, return
```

---

## Architecture

### Placement: Per-Worker Cache

Each worker owns one `keel_query_cache_t` instance.  The cache is **not shared
across workers** — there is no inter-process or cross-thread coordination.  Each
worker's LRU operates independently and accumulates its own hit/miss counters.

```
Worker 0 → keel_query_cache_t  (capacity: 256 entries)
Worker 1 → keel_query_cache_t  (capacity: 256 entries)
...
```

Cache is created in `keel_worker_init()` when `result_cache = on` (via
`keel_query_cache_create()`), and destroyed in `keel_worker_cleanup()`.

### Cache Key (Query Digest)

```
Digest = SHA-256(normalized query text)
```

**Normalization rules (in order):**
1. Strip `--` line comments
2. Collapse runs of whitespace to a single space
3. Uppercase all non-string-literal characters

String literal content is preserved verbatim — there is **no replacement of
numeric or string constants with `?`**.  Queries with different literal values
(e.g. `WHERE id = 1` vs `WHERE id = 2`) produce different digests and are
cached independently.

### Cacheability Rules

A query is **not cacheable** and returns `KEEL_CACHE_NON_CACHEABLE` from
`keel_query_cache_digest()` if any of the following apply:

| Pattern | Example | Reason |
|---------|---------|--------|
| Non-SELECT | `INSERT`, `UPDATE`, `DELETE`, `DDL` | Mutates state |
| `FOR UPDATE` | `SELECT … FOR UPDATE` | Acquires row lock |
| `FOR SHARE` | `SELECT … FOR SHARE` | Acquires shared row lock (PG-specific keyword; MySQL uses `LOCK IN SHARE MODE`) |
| Volatile functions | `NOW()`, `RANDOM()`, `UUID()`, `SYSDATE()` | Time/session-dependent |
| `WITH` (CTE) | `WITH cte AS (…)` | May include writes via `WITH … INSERT` |
| `UNION` | `SELECT … UNION SELECT …` | Non-deterministic ordering without `ORDER BY` |
| Inside transaction | `BEGIN … SELECT …` | Session state makes result non-reproducible |

The engine also skips caching if `act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL)`
is set (detected by the frontend SQL classifier).

### Cache Storage

```c
/* Opaque LRU cache instance (per worker) */
typedef struct keel_query_cache keel_query_cache_t;

/* Statistics snapshot */
typedef struct keel_query_cache_stats {
    uint64_t hits;
    uint64_t misses;
    uint64_t evictions;
    uint64_t expirations;
    size_t   entries_count;
    size_t   memory_used_bytes;
    size_t   memory_max_bytes;
} keel_query_cache_stats_t;
```

Internally the implementation uses an open-addressing hash table with a load factor
of 0.75, initial capacity of 1 024 entries, and doubles when the table is 75% full.
Eviction follows LRU order.  The result buffer for each entry is a heap-allocated
copy of the full PostgreSQL wire-protocol response bytes.

A single result exceeding 16 MiB during accumulation causes the in-flight capture
to be discarded (the result is forwarded to the client but not stored).

---

## Engine Integration

### Hot Path (engine_flow.c)

#### 1 — Cache GET (before backend borrow)

In `on_fe_data`, when the frontend action is `KEEL_FE_ACT_QUERY`:

```
Conditions checked before borrowing a backend connection:
  - worker->query_cache != NULL          (cache enabled)
  - act.type == KEEL_FE_ACT_QUERY        (simple query, not prepared)
  - !session->in_transaction             (autocommit only)
  - !(act.effect & (KEEL_QE_WRITE|KEEL_QE_DDL))  (read-only)
  - keel_query_cache_digest() == KEEL_OK (digest computed, query cacheable)

On HIT:  send cached bytes directly to client, set phase = KEEL_PHASE_READY,
         continue to next event (backend never borrowed).

On MISS: set sf->cache_pending = true, copy 32-byte digest to sf->cache_digest.
         Proceed to normal backend borrow path.
```

#### 2 — Response Accumulation (on_be_data)

At the top of `on_be_data`, if `sf->cache_pending`:

```
- Grow sf->cache_capture_buf via keel_realloc
- memcpy incoming bytes into the buffer
- Track total accumulated length in sf->cache_capture_len
- On KEEL_BE_ACT_ERROR: free buffer, clear cache_pending (never cache errors)
```

There is a 16 MiB hard cap per response.  If exceeded the capture is aborted and
cleared, but the response still flows to the client.

#### 3 — Cache PUT (on query_complete)

When `query_complete` is detected in `on_be_data`, if `sf->cache_pending`:

```
- Subtract residual bytes (server_residual) to avoid caching incomplete frames
- keel_query_cache_put(worker->query_cache,
                       sf->cache_digest,
                       sf->cache_capture_buf,
                       sf->cache_capture_len,
                       ttl_ms=0 /* use cache default */)
- Free sf->cache_capture_buf
- Clear sf->cache_pending
```

### Session Flow Fields

These fields are added to `keel_session_flow_t` to support cache capture:

```c
bool     cache_pending;          /* Capture in progress */
uint8_t  cache_digest[32];       /* SHA-256 digest of in-flight query */
uint8_t* cache_capture_buf;      /* Accumulating backend response bytes */
size_t   cache_capture_len;      /* Bytes accumulated so far */
size_t   cache_capture_cap;      /* Allocated capacity of capture_buf */
```

---

## API Reference

### Lifecycle

```c
/* Create a new per-worker cache instance.
 * default_ttl_ms: TTL applied when put() is called with ttl_ms = 0 (default: 3 000 ms)
 * max_size_mb:    Memory cap in MB; 0 = no cap (default: 256)
 */
keel_error_t keel_query_cache_create(keel_query_cache_t** cache_out,
                                     int    default_ttl_ms,
                                     size_t max_size_mb);

/* Destroy the cache and free all memory (NULL-safe). */
void keel_query_cache_destroy(keel_query_cache_t* cache);
```

### Lookup & Store

```c
/* Lookup cached result.
 * Returns KEEL_OK + fills result_out/result_len on HIT.
 * Returns KEEL_CACHE_MISS on miss or expired entry.
 * Returned pointer is valid until the next cache operation.
 */
keel_error_t keel_query_cache_get(keel_query_cache_t* cache,
                                  const uint8_t       digest[32],
                                  const uint8_t**     result_out,
                                  size_t*             result_len);

/* Store a result.
 * Result bytes are copied.  LRU entry is evicted if cache is full.
 * ttl_ms = 0 uses the cache default.
 */
keel_error_t keel_query_cache_put(keel_query_cache_t* cache,
                                  const uint8_t  digest[32],
                                  const uint8_t* result,
                                  size_t         result_len,
                                  int            ttl_ms);
```

### Invalidation

```c
/* Mark a single entry as expired (next get() returns MISS). */
keel_error_t keel_query_cache_expire(keel_query_cache_t* cache,
                                     const uint8_t digest[32]);

/* Evict all entries that read from the named table.
 * Called internally on INSERT/UPDATE/DELETE if write-tracking is active.
 * Table name is case-insensitive.
 */
keel_error_t keel_query_cache_invalidate_table(keel_query_cache_t* cache,
                                               const char* table);

/* Flush all entries. */
keel_error_t keel_query_cache_flush(keel_query_cache_t* cache);
```

### Statistics & Digest

```c
/* Snapshot statistics.  Counters are approximate in concurrent scenarios. */
keel_error_t keel_query_cache_stats(keel_query_cache_t*       cache,
                                    keel_query_cache_stats_t* stats_out);

/* Normalize query and compute 32-byte SHA-256 digest.
 * Returns KEEL_OK + fills digest_out if the query is cacheable.
 * Returns KEEL_CACHE_NON_CACHEABLE otherwise.
 */
keel_error_t keel_query_cache_digest(const char* query,
                                     uint8_t     digest_out[32]);

/* Quick cacheability check without computing a digest. */
bool keel_query_cache_is_cacheable(const char* query);
```

---

## Configuration

The cache is configured via the `[worker_group.*]` INI section:

```ini
[worker_group.primary]
result_cache = on          # Enable query result caching (default: off)
```

There are currently no per-rule TTL overrides or per-table caching rules in the
configuration layer.  The default TTL (`3 000 ms`) and default max-size (`256 MB`)
are used for all queries.  These can be changed at compile time via the constants
in `query_cache.c`.

### Splice Bypass Interaction

When `result_cache = on`, the zero-copy splice bypass (S2C DataRow path) is
**disabled**.  All backend response bytes are copied into userspace so they can be
accumulated for storage in the cache.  Set `result_cache = off` (the default) to
retain the splice bypass.

---

## Limitations & Known Issues

| # | Description |
|---|-------------|
| 1 | `FOR SHARE` is detected by its PostgreSQL keyword.  MySQL uses `LOCK IN SHARE MODE`; this is currently not detected — MySQL `SELECT … LOCK IN SHARE MODE` may be cached when it should not be. |
| 2 | No literal parameterisation: `WHERE id = 1` and `WHERE id = 2` are separate cache entries.  High-cardinality predicates will quickly evict each other under LRU. |
| 3 | Cache is per-worker.  On a proxy with N workers, the same backend result may be fetched N times and stored once per worker. |
| 4 | No distributed cache or cross-proxy coherency. |

---

## Write Invalidation

When a `WRITE` or `DDL` statement executes (detected via `KEEL_QE_WRITE | KEEL_QE_DDL`
in the engine hot path), the engine:

1. Copies the write query text into `sf->cache_inval_sql` (heap allocation).
2. Sets `sf->cache_inval_pending = true`.
3. At `query_complete` (when the backend's `ReadyForQuery` arrives), re-parses the
   write SQL using `keel_sql_analyze_full()` to extract the set of affected tables via
   `keel_qt_get_invalidated_tables()`.
4. Calls `keel_query_cache_invalidate_table(worker->query_cache, table_name)` for each
   affected table, evicting all cache entries whose SQL references that table.
5. Frees `sf->cache_inval_sql` and clears `sf->cache_inval_pending`.

This ensures that after a `DELETE FROM orders …` completes, any cached
`SELECT … FROM orders …` results are evicted before the next read query is served.

The feature is active only when `result_cache = on` in the worker group config and
`worker->query_cache` is non-NULL.

---

## Testing

| File | Test ID | Coverage |
|------|---------|----------|
| `tests/test_query_cache.c` | 52 | Hash table, LRU eviction, TTL expiry, `is_cacheable` checks (14 cases, ~110 assertions) |

---

## Future Enhancements

1. **Literal parameterisation** — replace numeric/string constants with `?` before hashing to improve cache utilisation for parametric queries.
2. **Per-rule TTL configuration** — allow `[query-rule.*]` sections to set per-pattern TTL.
3. **`FLUSH QUERY CACHE` admin command** — expose via the admin socket.
4. **`SHOW CACHE STATS` admin command** — surface per-worker hit/miss/eviction counters.
5. **Distributed cache** — Redis-backed cache for multi-proxy deployments.

---

## References

- Implementation: `src/core/query_cache.c`, `include/keel/core/query_cache.h`
- Engine integration: `src/engine/engine_flow.c` (`on_fe_data`, `on_be_data`)
- Worker lifecycle: `src/worker/worker.c` (`keel_worker_init`, `keel_worker_cleanup`)
