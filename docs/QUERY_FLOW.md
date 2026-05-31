# KEEL Query Execution Flow

This document traces the complete lifecycle of a SQL query through the KEEL database proxy — from the moment bytes arrive on the client socket to the moment results are returned.

## Table of Contents

- [Overview](#overview)
- [Query Flow Diagram](#query-flow-diagram)
- [Phase 1: Query Arrival](#phase-1-query-arrival)
  - [io_uring Recv](#io_uring-recv)
  - [Protocol Framing](#protocol-framing)
- [Phase 2: Protocol Dispatch](#phase-2-protocol-dispatch)
  - [Frontend Message Handler](#frontend-message-handler)
  - [Action Types](#action-types)
- [Phase 3: SQL Analysis](#phase-3-sql-analysis)
  - [Lexer](#lexer)
  - [Fast Analyzer](#fast-analyzer)
  - [Query Classification](#query-classification)
  - [Full Parser (Optional)](#full-parser-optional)
  - [Query Tree](#query-tree)
- [Phase 4: Route Cache](#phase-4-route-cache)
- [Phase 5: Routing Decision](#phase-5-routing-decision)
  - [Route Hint](#route-hint)
  - [Sticky Primary](#sticky-primary)
  - [Pool Selection](#pool-selection)
  - [Backend Borrow](#backend-borrow)
- [Phase 6: State Sync](#phase-6-state-sync)
- [Prepared Statement Virtualization](#prepared-statement-virtualization)
- [Replication Tracking](#replication-tracking)
  - [XID Capture](#xid-capture)
  - [Commit-in-Doubt Resolution](#commit-in-doubt-resolution)
- [WAL LSN Consistency Tokens](#wal-lsn-consistency-tokens)
  - [Capture After Write](#capture-after-write)
  - [Replica Reached Check](#replica-reached-check)
- [Phase 7: Query Forwarding](#phase-7-query-forwarding)
  - [Non-Blocking Send](#non-blocking-send)
  - [Jumbo Messages](#jumbo-messages)
  - [COPY Fast Path](#copy-fast-path)
- [Phase 8: Backend Response](#phase-8-backend-response)
  - [Backend Recv Loop](#backend-recv-loop)
  - [Result Forwarding](#result-forwarding)
  - [ReadyForQuery Detection](#readyforquery-detection)
- [Phase 9: Connection Return](#phase-9-connection-return)
  - [Transaction State Update](#transaction-state-update)
  - [Pool Return](#pool-return)
  - [Query Logging](#query-logging)
- [Protocol Vtable](#protocol-vtable)
- [Key Data Structures](#key-data-structures)
- [End-to-End Sequence Diagram](#end-to-end-sequence-diagram)

---

## Overview

KEEL operates as a **protocol-aware Layer 7 proxy**. It does not blindly forward bytes — it understands the PostgreSQL and MySQL wire protocols at the message level, parses SQL for routing decisions, and manages backend connection state between queries.

**Key principles:**
- Every I/O operation is async (io_uring) — zero blocking on the hot path
- SQL analysis uses a fast token-based analyzer for routing (no full parse needed for simple queries)
- Backend connections are borrowed from a pool per-query and returned immediately after
- Protocol vtables eliminate `strcmp()` branching in the hot path

---

## Query Flow Diagram

```
                          CLIENT
                            │
                    ┌───────▼───────┐
                    │  io_uring     │
                    │  recv(64KB)   │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │  frame_len()  │ Protocol framing
                    │  extract msg  │ (PG: type+4B len, MySQL: 3B+seq)
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │  on_fe_msg()  │ Protocol dispatch
                    │  → action     │ (type, route_hint, effect, payload)
                    └───────┬───────┘
                            │
                 ┌──────────┼──────────┐
                 │          │          │
              SSL_REQUEST  AUTH   QUERY/FORWARD
              (decline)   (handshake)  │
                                ┌──────▼───────┐
                                │ SQL analyze  │ keel_sql_analyze()
                                │ R/W classify │ (lexer → first tokens)
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ Route cache  │ XXHash64 lookup
                                │ 1024 entries │ 8-probe linear search
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ Route decide │ sticky-primary check
                                │ primary/     │ replica RR selection
                                │ replica      │ health check filter
                                └──────┬───────┘
                                       │
                          ┌────────────┼────────────────────────────┐
                          │                                         │
                    shard plan == SINGLE                  shard plan == SCATTER
                          │                                         │
                          │                          ┌──────────────▼─────────────┐
                          │                          │ keel_engine_scatter_execute │
                          │                          │ (engine_scatter.c)          │
                          │                          │                             │
                          │                          │  ┌──────────────────────┐  │
                          │                          │  │ sc_exec_shard × N    │  │
                          │                          │  │ blocking TCP connect  │  │
                          │                          │  │ + SCRAM-SHA-256 auth  │  │
                          │                          │  │ + Simple Query        │  │
                          │                          │  └──────────┬───────────┘  │
                          │                          │             │ partial rows  │
                          │                          │  ┌──────────▼───────────┐  │
                          │                          │  │ Merge pipeline        │  │
                          │                          │  │ Phase D: scalar agg   │  │
                          │                          │  │ Phase E: GROUP BY     │  │
                          │                          │  │ Phase H: HAVING       │  │
                          │                          │  │ Phase C: ORDER BY     │  │
                          │                          │  │ Phase L: LIMIT/OFFSET │  │
                          │                          │  └──────────┬───────────┘  │
                          │                          │             │ merged rows   │
                          │                          │  ┌──────────▼───────────┐  │
                          │                          │  │ RowDescription +      │  │
                          │                          │  │ DataRow × N +         │  │
                          │                          │  │ CommandComplete        │  │
                          │                          └──┼──────────┬────────────┘  │
                          │                             │          │               │
                          │                             │     client_fd            │
                          │                             │   (ReadyForQuery)        │
                          │                             └──────────┘               │
                          │                                                        │
                   (single-backend path continues below)                           │
                          │                                                        │
                          ▼                                     ◀──────────────────┘
                                ┌──────▼───────┐
                                │ pool_borrow  │ 5-tier CAS search
                                │ (or queue)   │ clean → idle → dirty
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ state_sync   │ SET replay if needs_sync
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ send to BE   │ non-blocking send
                                │ (+ deferred  │ (io_uring if partial)
                                │  if partial) │
                                └──────┬───────┘
                                       │
                              BACKEND DATABASE
                                       │
                                ┌──────▼───────┐
                                │ recv from BE │ io_uring recv(64KB)
                                │ frame loop   │
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ on_be_msg()  │ Protocol dispatch
                                │ forward to   │ (forward rows → client)
                                │ client       │
                                └──────┬───────┘
                                       │
                          ┌────────────┼────────────┐
                          │                         │
                   fast_network_path         normal userspace
                   ┌──────▼───────┐         ┌──────▼───────┐
                   │ MSG_PEEK hdr │         │ batch send   │
                   │ type=='D'?   │         │ to client    │
                   └──────┬───────┘         └──────┬───────┘
                     yes/ │ \no                    │
               ┌─────────┘  └────────┐             │
        ┌──────▼───────┐   ┌─────────▼──┐          │
        │ splice(2)    │   │ exit splice│          │
        │ BE→pipe→FE   │   │ → engine   │          │
        │ (zero-copy)  │   │   path     │          │
        └──────┬───────┘   └────────────┘          │
               │                                   │
               └───────────────┬───────────────────┘
                               │
                        ┌──────▼───────┐
                        │ ReadyForQuery│ Transaction state update
                        │ detected     │ Pool return + wake waiter
                        └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ query_log    │ Emit if configured
                                │ emit         │
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │ re-arm FE    │ Back to recv(client_fd)
                                │ recv         │
                                └──────────────┘
```

---

## Phase 1: Query Arrival

### io_uring Recv

**File:** `src/worker/worker.c`
**Callback:** `on_client_recv_complete()`

Each session has a pending `keel_reactor_recv()` operation on `client_fd`:

```c
keel_reactor_recv(worker->reactor, client_fd,
                 recv_ctx->buffer, sizeof(recv_ctx->buffer),  // 64KB
                 0, recv_ctx, on_client_recv_complete);
```

When data arrives, `on_client_recv_complete()` fires with the buffer and byte count. It then calls the engine flow:

```c
keel_flow_result_t result = keel_engine_flow_on_fe_data(
    &recv_ctx->flow, session, data, len);
```

### Protocol Framing

**File:** `src/engine/engine_flow.c`
**Vtable method:** `flow->frame_len(ctx, data, len, direction)`

The flow engine extracts complete protocol messages from the recv buffer:

```
while (pos < len) {
    ssize_t flen = flow->frame_len(ctx, data+pos, len-pos, 0);
    if (flen == 0) break;      // Need more data (partial header)
    if (flen < 0) return ERROR; // Framing error

    // Process complete message at data+pos, length flen
    flow->on_fe_msg(ctx, data+pos, flen, &action);
    pos += flen;
}
```

**PostgreSQL framing:** Type byte (1B) + length (4B, big-endian, includes self) = message. Minimum frame: 5 bytes.

**MySQL framing:** Length (3B, little-endian) + sequence number (1B) = header. Payload follows. Minimum frame: 4 bytes.

---

## Phase 2: Protocol Dispatch

### Frontend Message Handler

**Vtable method:** `flow->on_fe_msg(ctx, data, len, action_out)`

Each complete frontend message is dispatched through the protocol vtable. The handler parses the message, determines its intent, and returns an action struct:

```c
typedef struct keel_fe_action {
    keel_fe_action_type_t    type;           // What to do
    const uint8_t*          fe_response;    // Data to send to client (auth, errors)
    size_t                  fe_response_len;
    const uint8_t*          be_payload;     // Data to forward to backend
    size_t                  be_payload_len;
    uint8_t                 route_hint;     // PRIMARY / REPLICA / ANY
    uint8_t                 effect;         // READONLY / WRITE / DDL / TX / SESSION
    uint32_t                pin_update;     // Pin reasons to SET
    uint32_t                pin_clear;      // Pin reasons to CLEAR
} keel_fe_action_t;
```

### Action Types

| Action | Description | Flow |
|--------|-------------|------|
| `SSL_REQUEST` | Client requests SSL/TLS | Send 'N' (decline), re-arm recv |
| `AUTH_CONTINUE` | Auth handshake in progress | Send auth response, re-arm recv |
| `AUTH_COMPLETE` | Auth finished | Send AuthOK + ReadyForQuery, phase = READY |
| `QUERY` | SQL query to execute | Analyze SQL → route → borrow → forward |
| `FORWARD_TO_BACKEND` | Passthrough to backend | Borrow if needed → forward |
| `TERMINATE` | Client disconnect | Close session |
| `ERROR` | Protocol/auth error | Send error to client, close |

---

## Phase 3: SQL Analysis

### Lexer

**File:** `src/sql/lexer.c` (636 lines)

Hand-written tokenizer that breaks SQL text into tokens without allocation:

```c
keel_sql_lexer_t lexer;
keel_sql_lexer_init(&lexer, sql);

keel_sql_token_t token;
while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK) {
    // token.type: KEYWORD, IDENTIFIER, NUMBER, STRING, OPERATOR, PUNCT, ...
    // token.text: keel_str_t (pointer + length into original SQL)
}
```

**Token types:** `KEYWORD`, `IDENTIFIER`, `NUMBER`, `STRING`, `OPERATOR`, `PUNCTUATION`, `COMMENT`, `DOLLAR_QUOTED`

**Keyword lookup:** `keel_sql_lookup_keyword(text)` — fast hash-based keyword identification returning enum values for SQL keywords (SELECT, INSERT, BEGIN, SET, etc.).

### Fast Analyzer

**File:** `src/sql/analyzer.c` (786 lines)
**Function:** `keel_sql_analyze(sql, &result)`

The "hot path" analyzer — examines only the first few tokens to classify the query:

```c
keel_proto_query_t result;
keel_sql_analyze(KEEL_STR("SELECT * FROM users WHERE id = 1"), &result);
// result.type = KEEL_QUERY_SELECT
// result.flags = KEEL_QUERY_FLAG_READ_ONLY
// result.needs_primary = false
```

**Zero allocation.** Uses the lexer's string views into the original query text.

### Query Classification

The analyzer maps the leading keyword to a query type and read/write flags:

| Leading Keyword | Type | Flags | Primary Required |
|-----------------|------|-------|-----------------|
| `SELECT` | `SELECT` | `READ_ONLY` | No (unless `FOR UPDATE`) |
| `SHOW` | `SHOW` | `READ_ONLY` | No |
| `EXPLAIN` | `EXPLAIN` | `READ_ONLY` | No |
| `INSERT` | `INSERT` | `WRITE` | Yes |
| `UPDATE` | `UPDATE` | `WRITE` | Yes |
| `DELETE` | `DELETE` | `WRITE` | Yes |
| `CREATE` | `CREATE` | `DDL` | Yes |
| `ALTER` | `ALTER` | `DDL` | Yes |
| `DROP` | `DROP` | `DDL` | Yes |
| `TRUNCATE` | `TRUNCATE` | `DDL` | Yes |
| `BEGIN` / `START` | `TRANSACTION` | `BEGINS_TX` | Yes |
| `COMMIT` | `TRANSACTION` | `ENDS_TX` | Current backend |
| `ROLLBACK` | `TRANSACTION` | `ENDS_TX` | Current backend |
| `SET` | `SET` | `SESSION` | Current backend |
| `PREPARE` | `PREPARE` | `SESSION` | Current backend |
| `COPY` | `COPY` | `WRITE` / `COPY_IN` | Depends on direction |
| `WITH` (CTE) | Recursive scan | Depends on body | Scans for DML |

**Special cases:**
- `SELECT ... FOR UPDATE/SHARE` → classified as WRITE (needs primary)
- `WITH ... INSERT/UPDATE/DELETE` → classified as WRITE
- `EXECUTE` → depends on the underlying prepared statement
- Multi-statement (`;` separated) → each statement analyzed independently

### Full Parser (Optional)

**File:** `src/sql/parser.c` (1791 lines)
**Function:** `keel_sql_parse(sql, arena)`

Full recursive-descent SQL parser. Only invoked when query logging with `log_query_tree = true` or when the fast analyzer cannot determine the query nature.

```c
keel_arena_t* arena = keel_arena_create(0);
keel_qt_query_t* qt = keel_sql_parse(sql, arena);
// qt->operation, qt->tables, qt->columns, qt->where_clause, ...
keel_arena_destroy(arena);  // Frees everything
```

**Parser structure:** Recursive descent with the arena allocating all AST nodes. Grammar covers SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE, DROP, JOIN, subqueries, CTEs, window functions, etc.

### Query Tree

**File:** `src/sql/query_tree.c` (961 lines)

AST representation produced by the full parser:

```
keel_qt_query_t
├── operation          // SELECT, INSERT, UPDATE, DELETE, DDL, TX, ...
├── tables[]           // Referenced table names
├── columns[]          // Selected/modified columns
├── where_clause       // Filter expression tree
├── joins[]            // JOIN specifications
├── group_by[]         // GROUP BY columns
├── order_by[]         // ORDER BY specifications
├── limit / offset     // LIMIT/OFFSET values
├── ctes[]             // Common Table Expressions
└── subqueries[]       // Nested queries
```

The query tree enables rich query logging showing table access patterns, filter conditions, and join structure.

---

## Phase 4: Route Cache

**File:** `src/session/route_cache.c` (193 lines)

Per-worker L1 route cache that avoids re-analyzing identical queries:

```
route_cache_t (per worker, thread-local)
├── entries[1024]      // Fixed-size hash table (ROUTE_CACHE_SIZE)
│   ├── query_hash     // XXHash64 of query text
│   ├── query_len      // Length for collision disambiguation
│   ├── route_type     // PRIMARY / REPLICA / ANY
│   └── timestamp      // LRU tick for eviction
├── hits / misses      // Cache statistics
└── tick               // Monotonic counter
```

**Algorithm:**
1. Hash query text with XXHash64 (seed `0xCA00CE01`)
2. Compute slot: `hash & ROUTE_CACHE_MASK` (1023)
3. Linear probe up to `ROUTE_CACHE_MAX_PROBE` (8) slots
4. **Hit:** `query_hash` + `query_len` match → return cached route
5. **Miss:** Analyze query, insert into cache (evict oldest in probe chain)

**Why XXHash64?** Extremely fast (processes 32 bytes/cycle on modern CPUs), good distribution, zero-allocation. The hash quality is critical because collisions cause unnecessary full SQL analysis.

---

## Phase 5: Routing Decision

### Route Hint

The protocol vtable returns a `route_hint` with each query action:

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `KEEL_FROUTE_ANY` | Use any backend (default) |
| 1 | `KEEL_FROUTE_PRIMARY` | Must use primary (writes, DDL, tx) |
| 2 | `KEEL_FROUTE_REPLICA` | Prefer replica (reads) |

### Sticky Primary

**File:** `src/engine/engine_flow.c`

After a write query, subsequent reads from the same session are forced to the primary for a configurable TTL (default `KEEL_STICKY_PRIMARY_TTL_MS`) when no exact write-position token has been captured. If the session carries a real LSN/GTID token, KEEL keeps routing reads to primary until a reactor-owned catch-up verifier can prove a replica has reached that token:

```c
if (route == KEEL_FROUTE_REPLICA && sf->last_write_ns != 0) {
    uint64_t now = engine_now_ns();
    uint64_t ttl_ms = sf->sticky_primary_ttl_ms
                        ? sf->sticky_primary_ttl_ms
                        : KEEL_STICKY_PRIMARY_TTL_MS;
    if (keel_ssv_requires_primary(sf->consistency_atoms, now, ttl_ms)) {
        route = KEEL_FROUTE_PRIMARY;  // token present — force primary
    } else if (!keel_ssv_consistency_ttl_ok(sf->consistency_atoms, now, ttl_ms)) {
        route = KEEL_FROUTE_PRIMARY;  // legacy TTL active — force primary
    } else {
        sf->last_write_ns = 0;        // timestamp-only TTL expired
    }
}
```

`keel_ssv_consistency_ttl_ok()` is now only the timestamp-only fallback. `keel_ssv_requires_primary()` is the authoritative token check: a populated LSN/GTID token does not expire by wall-clock time alone.

This prevents stale reads from async replicas after writes (read-after-write consistency).

### Pool Selection

Based on the final route:

```c
switch (route) {
case KEEL_FROUTE_REPLICA:
    // Round-robin across healthy replicas
    for (attempt = 0; attempt < worker->replica_pool_count; attempt++) {
        idx = (rr_counter + attempt) % replica_pool_count;
        if (server_pool->servers[idx].healthy) {
            pool = worker->replica_pools[idx];
            break;
        }
    }
    if (!pool) pool = worker->primary_pool;  // Fallback
    break;

case KEEL_FROUTE_PRIMARY:
default:
    pool = worker->primary_pool;
    break;
}
```

### Backend Borrow

```c
backend_conn_t* be_conn;

if (sf->pins != KEEL_FPIN_NONE) {
    // Session is pinned — must reuse same backend
    be_conn = backend_pool_borrow_pinned(pool, session);
} else {
    be_conn = backend_pool_borrow(pool, session->state_hash);
}

if (!be_conn) {
    // Pool exhausted — queue in wait queue
    backend_pool_queue_wait(pool, session, pool);
    return KEEL_FLOW_WAIT_POOL;
}

session->backend_conn = be_conn;
session->server_fd = be_conn->fd;
```

---

## Phase 6: State Sync

If `be_conn->needs_sync` is true (connection has different SET state than the session requires), the engine replays the session's state profile:

```
State Profile (SET parameters):
  SET search_path = 'public, app';
  SET statement_timeout = '5000';
  SET timezone = 'UTC';
```

The state profile is tracked as a hash + ordered key-value list. When borrowed, the engine compares the session's `state_hash` against the connection's `current_state_hash`. If they differ, it sends SET commands to align the backend state before forwarding the query.

### Protocol-Specific State Sync

`build_state_sync()` is implemented per-protocol via `keel_proto_flow_vtable_t`:

| Protocol | Vtable Function | SQL Generation | Wire Format |
|----------|-----------------|----------------|-------------|
| PostgreSQL | `pgf_build_state_sync` | `generate_sync_sql(bp, sp, &result)` → minimal `SET`/`RESET` diff | Simple Query `'Q'` message |
| MySQL | `myf_build_state_sync` | `generate_sync_sql(bp, sp, &result)` → minimal `SET @@session.x = 'value'` diff | `COM_QUERY` (0x03) packet |

Both protocols share `generate_sync_sql()` from `include/keel/session/state_profile.h`, which produces a minimal SQL string covering only parameters that differ between the backend profile (`bp`) and the session profile (`sp`). This avoids redundant `SET` round-trips on connection reuse.

---

## Prepared Statement Virtualization

### Overview

**Problem:** In transaction-pooling mode, sessions share backend connections across queries. Named prepared statements (created via `PREPARE name AS …` or PostgreSQL's extended-query Parse message) are server-side objects bound to a specific backend connection. When the backend is returned to the pool and a *different* backend is borrowed for the next query, the new backend has no record of those statements — `EXECUTE name` would fail with "prepared statement does not exist."

**Solution:** KEEL's prepared statement virtualization mechanism automatically replays all of the session's confirmed Parse messages to any freshly assigned backend *before* forwarding the client's pending message, making statement reuse transparent to the application.

### Session Stmt Cache

**File:** `src/protocol/postgres/postgres_flow.c`  
**Struct:** `pg_flow_ctx_t`

Each PostgreSQL session flow context maintains an in-memory statement cache (`PG_STMT_CACHE_SIZE = 32` slots):

| Field | Type | Description |
|-------|------|-------------|
| `name[64]` | `char` | Prepared statement name |
| `wire_msg` | `uint8_t*` | Original Parse wire message bytes |
| `wire_msg_len` | `size_t` | Length of wire message |
| `valid` | `bool` | Slot in use |
| `confirmed` | `bool` | Backend sent ParseComplete (safe to replay) |

The session-level `session_stmt_hash` is an XXHash64 fingerprint of all confirmed statement names. It is updated whenever a Parse succeeds (`confirmed = true`) or a Close removes a statement. This single 64-bit value is used as a pool key to find backends that already have the matching set of statements pre-loaded.

### PS-Aware Pool Borrow

**File:** `src/worker/backend_pool.c`  
**Function:** `backend_pool_borrow_with_stmts(pool, required_stmt_hash)`

When a session has the `KEEL_FPIN_PREPARED_STMT` pin, the engine calls `backend_pool_borrow_with_stmts()` instead of the normal `backend_pool_borrow()`:

```
Step 1 — Exact hash match (zero-overhead reuse):
  Search idle_list for conn where stmt_set_hash == required_stmt_hash.
  → CAS IDLE → ACTIVE, needs_sync = false.  No replay needed.

Step 2 — Stmt-clean fallback:
  Search idle_list and clean_list for conn where stmt_set_hash == 0.
  Connections with a *different* non-zero hash are excluded.
  (They hold another session's stmts and would require a full DISCARD ALL.)

Step 3 — Forced eviction (dirty):
  Attempt a non-blocking DISCARD ALL on a dirty connection to clear
  another session's stmts before handing the slot over for replay.

Step 4 — Queue:
  All approaches exhausted → session queued in wait_queue.
```

### `pgf_get_stmt_replay` — Building the Replay Buffer

**File:** `src/protocol/postgres/postgres_flow.c`  
**Function:** `pgf_get_stmt_replay(ctx, replay_buf_out, replay_len_out, stmt_count_out, hash_out)`

Called by the engine when a hash mismatch is detected. Returns a heap-allocated buffer containing concatenated Parse wire messages for every `valid && confirmed` entry in the stmt cache, **plus a trailing Sync message** (`{'S', 0, 0, 0, 4}`, 5 bytes):

```c
/* Pass 1: sum wire message lengths */
for (int i = 0; i < PG_STMT_CACHE_SIZE; i++) {
    if (!e->valid || !e->confirmed || e->name[0] == '\0') continue;
    total_len += e->wire_msg_len;
    stmt_count++;
}

/* Pass 2: build buffer — concatenate all Parse frames */
uint8_t* buf_with_sync = keel_malloc(total_len + sizeof(pg_sync));
/* ... memcpy each wire_msg ... */
memcpy(buf_with_sync + total_len, pg_sync, sizeof(pg_sync));  /* Append Sync */
```

**Why the trailing Sync?**  
PostgreSQL's extended-query protocol only emits `ParseComplete` responses *after* receiving a `Sync` message. Without a trailing Sync, the backend queues all responses indefinitely and KEEL's `WAIT_STMT_REPLAY` handler never receives any `ParseComplete` — a permanent deadlock.

### WAIT_STMT_REPLAY State Machine

**File:** `src/engine/engine_flow.c`  
**Flow result:** `KEEL_FLOW_WAIT_STMT_REPLAY`

After the replay buffer is sent to the backend, the session enters `WAIT_STMT_REPLAY`. The backend will respond with:

```
ParseComplete('1') × N   +   ReadyForQuery('Z')
```

The `on_be_data` handler processes this in two phases, controlled by two fields in `keel_session_flow_t`:

**Phase 1 — ParseComplete counting (`stmt_replay_count`):**

```c
while (scan_pos < len && sf->stmt_replay_count > 0) {
    if (msg_type == '1') {           /* ParseComplete */
        sf->stmt_replay_count--;
        if (sf->stmt_replay_count == 0) {
            sf->stmt_replay_rfq_pending = true;  /* Transition to Phase 2 */
            break;
        }
    } else if (msg_type == 'E') {    /* ErrorResponse */
        /* Send Sync to exit error-recovery, forward error to client */
    }
    scan_pos += 1 + msg_len;
}
```

**Phase 2 — RFQ drain (`stmt_replay_rfq_pending`):**

```c
if (sf->stmt_replay_rfq_pending) {
    while (scan_pos < len) {
        if (mtype == 'Z') {           /* ReadyForQuery from replay Sync */
            sf->stmt_replay_rfq_pending = false;
            break;
        }
        scan_pos += 1 + mlen;
    }
}
```

**Why drain the RFQ before forwarding orig_msg?**  
The `Sync` appended to the replay buffer causes the backend to emit a `ReadyForQuery('Z')`. If this `Z` is *not* consumed and `orig_msg` is forwarded immediately, then when `on_be_data` later enters `WAIT_BACKEND`, it sees the replay `Z` *first* and interprets it as "transaction complete" — returning the backend to the pool while the actual response to orig_msg is still in-flight. The session then hangs permanently waiting for a response that will never arrive.

**Completion — forward orig_msg:**

```c
if (sf->stmt_replay_count == 0 && !sf->stmt_replay_rfq_pending) {
    /* Stamp the backend so future borrows can reuse it without replay */
    be_conn->stmt_set_hash = sf->stmt_replay_hash;

    /* Forward the original client message (Parse/Bind/Execute/Sync) */
    keel_try_send_nb(session->server_fd,
                     sf->stmt_replay_orig_msg, sf->stmt_replay_orig_len);
    return KEEL_FLOW_WAIT_BACKEND;
}
return KEEL_FLOW_WAIT_STMT_REPLAY;  /* Still waiting */
```

### End-to-End PS Replay Sequence

```
Client                    KEEL Proxy                   Backend
  │                          │                             │
  │  Parse("stmt_X",         │                             │
  │   "SELECT …") + Sync     │                             │
  │─────────────────────────►│                             │
  │                          │                             │
  │                          │ • Session has PREPARED_STMT pin
  │                          │ • borrow_with_stmts(hash)   │
  │                          │   → stmt-clean backend      │
  │                          │ • hash mismatch: need replay │
  │                          │                             │
  │                          │  [Parse(stmt_1)+…+Sync]     │
  │                          │  (replay buffer)            │
  │                          │────────────────────────────►│
  │                          │                             │ ParseComplete×N
  │                          │                             │ ReadyForQuery('I')
  │                          │◄────────────────────────────│
  │                          │                             │
  │                          │ Phase 1: count ParseCompletes (stmt_replay_count → 0)
  │                          │ Phase 2: drain RFQ (stmt_replay_rfq_pending → false)
  │                          │ Stamp be.stmt_set_hash      │
  │                          │                             │
  │                          │  Parse("stmt_X",            │
  │                          │   "SELECT …") + Sync        │
  │                          │  [original client message]  │
  │                          │────────────────────────────►│
  │                          │                             │ ParseComplete
  │                          │                             │ ReadyForQuery('I')
  │                          │◄────────────────────────────│
  │  ParseComplete           │                             │
  │  ReadyForQuery           │                             │
  │◄─────────────────────────│                             │
  │                          │ • Return backend to pool    │
  │                          │   (idle_list, hash stamped) │
  │                          │ • Next borrow w/ same hash: │
  │                          │   exact match → NO replay   │
```

### `keel_session_flow_t` — PS Replay Fields

| Field | Type | Description |
|-------|------|-------------|
| `stmt_replay_buf` | `uint8_t*` | Heap-allocated Parse frames + trailing Sync |
| `stmt_replay_len` | `size_t` | Total bytes in replay buffer |
| `stmt_replay_count` | `uint32_t` | ParseComplete responses still expected |
| `stmt_replay_orig_msg` | `const uint8_t*` | Original client message held until replay completes |
| `stmt_replay_orig_len` | `size_t` | Length of original client message |
| `stmt_replay_hash` | `uint64_t` | `stmt_set_hash` to stamp on backend after successful replay |
| `stmt_replay_rfq_pending` | `bool` | `true` = all ParseCompletes received but replay RFQ not yet drained |
| `stmt_replay_needs_discard` | `bool` | `true` = waiting for DISCARD ALL RFQ before sending replay |

---

## Prepared Statement Pooling Modes

The proxy supports four strategies for handling named prepared statements in
transaction-pooling mode.  The strategy is selected per worker-group via the
`prepared_statement` INI key.

### Mode Comparison

| Mode | Config value | Backend sees PS? | Hard-pin? | Replay? | Rewrite? |
|---|---|---|---|---|---|
| **Virtualize** (default) | `virtualize` | Yes | No | Yes | No |
| **Pinning** | `pinning` | Yes | Yes | No | No |
| **Tracking** | `tracking` | Yes | No | Yes | No |
| **Anonymous** | `anonymous` | No | No | No | Yes |

### Virtualize (default — spec §17)

Named prepared statements are shadowed in the session's `stmt_cache`.  When a
new backend is borrowed and its `stmt_set_hash` does not match the session's
current statement set, `pgf_get_stmt_replay()` builds a concatenated buffer
of all `Parse` wire messages and the engine enters `KEEL_FLOW_WAIT_STMT_REPLAY`
to replay them to the new backend before forwarding the client's Bind/Execute.

**Pros:** Fully poolable; no backend state leak.
**Cons:** Replay latency on first use with a new backend.

### Pinning

When the first `Parse` message (or Simple Query `PREPARE`) for a named
statement is received, `KEEL_FPIN_PREPARED_STMT` is set AND treated as a
hard-pin.  `backend_pool_borrow_pinned()` is used so the session keeps the
same backend connection for the lifetime of the prepared-statement set.

On session release:
- `be->current_state_hash` is set to a cleanup sentinel
- `backend_pool_return()` routes the backend through `DISCARD ALL`
- The backend goes to `clean_list` only after cleanup is confirmed

**Pros:** Zero replay overhead; deterministic backend for PS-heavy workloads.
**Cons:** One backend connection per PS-using session (reduces pool utilisation).

```ini
# In [worker_group.<name>]
prepared_statement = pinning
```

### Tracking

Identical to Virtualize, but also intercepts **Simple Query** `PREPARE name AS
body` syntax.  The statement name and body SQL are parsed by
`tracking_parse_prepare()`, a synthetic `Parse` wire message is built and
stored in the `stmt_cache`, and `KEEL_QE_HARD_PIN` is stripped from the action
effect so the engine uses the replay path instead of hard-pin.

This is useful for clients (e.g. some ORMs and psycopg2 in autocommit mode)
that mix Simple Query `PREPARE` with extended-protocol `Bind/Execute`.

```ini
prepared_statement = tracking
```

### Anonymous

The most aggressive pooling strategy.  Named `Parse` messages are **intercepted
and suppressed** — the backend never receives them.  A synthetic `ParseComplete`
response is synthesised and returned directly to the client.  The statement
name → SQL mapping is stored in `ctx->anon_map`.

At `Bind` time, the proxy performs a JIT rewrite:
1. Looks up `stmt_name` in `anon_map` to retrieve the SQL text.
2. Constructs an anonymous `Parse('', sql, 0 params)` message.
3. Rewrites the `Bind` to reference the unnamed statement (`''`).
4. Forwards the `Parse + Bind` pair to the backend.
5. The backend executes the query statelessly; the prepared-statement handle
   is discarded after the `Execute` response.

Backend connections have `stmt_set_hash == 0` at all times; they are always
returned to `clean_list` — no replay, no DISCARD ALL.

**Pros:** Maximum pool reuse; backends are completely stateless.
**Cons:** One full Parse round-trip per Execute (vs. zero for a true named PS).
  Large SQL bodies are retransmitted on every Bind.  Clients that rely on
  server-side type coercion via `ParseComplete ParameterDescription` may see
  differences (parameter OIDs are not cached across Binds).

```ini
prepared_statement = anonymous
```

### Poisoned-State Cleanup (all modes)

When any PS mode returns a backend to the pool with residual prepared-statement
state, the pool's `DISCARD ALL` pipeline is triggered:

1. `be->current_state_hash` is set to a non-zero sentinel.
2. `backend_pool_return()` detects the non-clean state and sends
   `DISCARD ALL;` to the backend.
3. The response is scanned by `check_pg_reusable_gate()` for `ReadyForQuery('I')`.
4. On confirmation the backend's `current_state_hash` and `stmt_set_hash` are
   cleared and it joins `clean_list`.

---

## Replication Tracking

Enabled via `transaction_tracking = on` in the `[worker_group.X]` INI section.
When active, the proxy instruments every `COMMIT` to atomically capture the
transaction ID, enabling commit-in-doubt recovery if the backend dies between
send and acknowledgement.  See [TRANSACTION_TRACKING.md](TRANSACTION_TRACKING.md)
for the full design and implementation details.

### XID Capture

**Config:** `transaction_tracking = on`

The PostgreSQL protocol plugin rewrites every outgoing `COMMIT` (simple query)
as a compound statement that atomically returns the XID before committing:

```sql
SELECT txid_current() AS _keel_txid; COMMIT;
```

The `DataRow` from `txid_current()` is intercepted by the backend response
handler (`pgf_on_be_msg`) and the integer XID stored in
`ctx->xid_probe_result` (the per-session protocol context).  The engine picks
this up from `act.commit_xid` in `keel_be_action_t` and stores it in
`sf->pending_commit_xid`.  The synthetic `CommandComplete(SELECT 1)` and
`RowDescription` / `DataRow` messages from the probe SELECT are absorbed
(never forwarded to the client); only the real `CommandComplete(COMMIT)` and
the final `ReadyForQuery('I')` reach the client.

The rewrite is performed by `pgf_on_fe_msg()` in response to a Simple Query
`COMMIT` when `txn_tracking` is enabled and no XID probe is already in flight.

Flow:

```
  FE: 'Q' "COMMIT"   →  pgf_on_fe_msg()
      │
      ├─ txn_tracking && qtype == COMMIT && !xid_probe_active
      │   → overwrite act->be_payload with kPgXidCommitMsg
      │   → set ctx->xid_probe_active = true
      ▼
  BE: 'T' RowDescription(_keel_txid)   → absorbed (KEEL_BE_ACT_ABSORB)
  BE: 'D' DataRow("12345")             → absorbed; ctx->xid_probe_result = 12345
  BE: 'C' CommandComplete("SELECT 1")  → absorbed; ctx->xid_probe_active = false
  BE: 'C' CommandComplete("COMMIT")    → forwarded to client
  BE: 'Z' ReadyForQuery('I')           → forwarded; engine clears commit_in_flight
```

**Content-based probe recovery:** If `xid_probe_active` is cleared prematurely
(e.g. a stale CommandComplete from a pipelined query trips the defensive clear
before the `RowDescription` arrives), the `pgf_on_be_msg` handler detects the
`_keel_txid` column name in any incoming `RowDescription` and re-asserts
`xid_probe_active`.  This prevents the probe's response from leaking to the
client as a spurious result set.

### Commit-in-Doubt Resolution

If the backend socket closes **after** keel sends the compound COMMIT but
**before** `ReadyForQuery('I')` is received, the transaction outcome is
ambiguous.  The engine detects this in `keel_engine_flow_on_be_data()` via
`sf->commit_in_flight` and calls `keel_engine_flow_handle_commit_doubt()`:

1. **No XID captured** (`sf->pending_commit_xid == 0`) — the backend died
   before `DataRow` arrived.  Outcome truly unknown.  Error `08006` is
   synthesised and returned to the client.

2. **XID captured** — borrow a fresh connection from the primary pool and
   issue:
   ```sql
   SELECT txid_status(XID)
   ```
   The result is one of:
   - `"committed"` → synthesise `CommandComplete(COMMIT)` + `ReadyForQuery('I')` and return success to the client.
   - `"aborted"` → synthesise error `40000` (transaction rollback).
   - `"in progress"` → (should not occur after socket close) synthesise `08006`.
   - `"NULL"` (XID too old / wrapped) → synthesise `08006` with hint to check `txid_status()` manually.

3. **No primary pool available** — synthesise `08006` with XID hint for manual
   operator verification.

All synthesised error messages include the XID in the detail field so DBAs and
client applications can verify via `SELECT txid_status(XID)` outside the proxy.

---

## WAL LSN Consistency Tokens

WAL LSN tokens provide **read-after-write consistency** for sessions that read
from replicas after performing a write on the primary.  A token is captured
immediately after each WRITE/DDL completes, and any subsequent read that is
routed to a replica first checks that the replica has replayed up to that LSN.

> **Status (PostgreSQL):** `capture_consistency_token` and
> `replica_reached_token` are **fully implemented** in
> `src/protocol/postgres/postgres_flow.c`.  The engine now calls
> `capture_consistency_token` on the backend-complete path and stores the
> result in both `sf->last_write_token` and the SSV consistency atoms
> (`sf->consistency_atoms`).  See [TRANSACTION_TRACKING.md](TRANSACTION_TRACKING.md)
> for the capture machinery and [SSV_POSTGRES_IMPLEMENTATION.md §22](SSV_POSTGRES_IMPLEMENTATION.md)
> for the atom layer.
>
> **Status (MySQL):** GTID-based tokens are pending implementation.

### Capture After Write — Deferred Architecture

**Vtable method:** `capture_consistency_token(ctx, be_fd, out_token)`

The capture is deliberately **deferred to the backend-complete path**, not
issued immediately on the FE data path.  This is critical for correctness
on non-blocking sockets:

```
WRONG (old approach — caused D-without-T bug):
  FE path: receives WRITE query → immediately calls capture_consistency_token()
           → sends SELECT pg_current_wal_lsn() on the non-blocking backend fd
           → recv() returns EAGAIN (backend is still processing the write)
           → the SELECT SQL sits in the kernel write buffer
  BE path: backend processes WRITE, then processes the leaked SELECT
           → sends T + D + CommandComplete + ReadyForQuery
           → this response arrives as the first bytes of the NEXT query
           → libpq sees DataRow('D') without prior RowDescription('T')

CORRECT (current approach — flag-deferred):
  FE path: sets sf->capture_lsn_pending = true
           → no I/O on the backend socket
  BE path: on query_complete (ReadyForQuery received for the WRITE)
           → temporarily set backend fd to blocking: fcntl(fd, F_SETFL, ~O_NONBLOCK)
           → call capture_consistency_token() — backend is now idle
           → pgf_capture_consistency_token() drains T+D+C+Z completely
           → restore O_NONBLOCK
```

**Engine implementation** (`engine_flow.c`):
```c
// FE path — do NOT call capture yet, just set the flag
if (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL))
    sf->capture_lsn_pending = true;

// BE path — after ReadyForQuery confirms the write completed
if (sf->capture_lsn_pending && session->backend_conn &&
    flow->capture_consistency_token) {
    int be_fd = session->backend_conn->fd;
    int flags = fcntl(be_fd, F_GETFL);
    if (flags >= 0) {
        fcntl(be_fd, F_SETFL, flags & ~O_NONBLOCK);
        int cap_rc = flow->capture_consistency_token(
            sf->ctx, be_fd, &sf->last_write_token);
        fcntl(be_fd, F_SETFL, flags);
        if (cap_rc == 0 && sf->last_write_token.value[0] != '\0') {
            keel_ssv_consistency_set_token(
                sf->consistency_atoms, &sf->last_write_token);
        }
    }
    sf->capture_lsn_pending = false;
}
```

**PostgreSQL implementation** (`pgf_capture_consistency_token`):
```sql
SELECT pg_current_wal_lsn()
```
Called on a socket temporarily in blocking mode.  The function reads
messages one at a time (5-byte header → body) until `ReadyForQuery ('Z')`,
fully draining `RowDescription + DataRow + CommandComplete + ReadyForQuery`.
The LSN text (e.g., `0/16B3740`) is stored in
`keel_consistency_token_t.value[128]` with a monotonic timestamp in
`captured_at_ns`.

**MySQL implementation:** GTID-based tokens are pending implementation.

### Replica Reached Check

**Vtable method:** `replica_reached_token(ctx, replica_fd, token, timeout_ms, out_reached)`

Called before forwarding a read to a replica when `sf->last_write_token` is
non-empty.  Returns `true` in `out_reached` if the replica has applied the
required WAL position.

**PostgreSQL implementation** (`pgf_replica_reached_token`):
```sql
SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn()) >= 'TOKEN'::pg_lsn
```
The `COALESCE` makes this query safe on the primary as well (where
`pg_last_wal_replay_lsn()` returns NULL).  Returns a single boolean column
(`'t'` / `'f'`).  The function reads message-by-message until `'Z'`,
identical to the capture path, ensuring no bytes are left unread.

**MySQL implementation:** `WAIT_FOR_EXECUTED_GTID_SET` is pending
implementation.

If `timeout_ms = 0`, the comparison is non-blocking (snapshot check only).
If the replica has not reached the token, the engine falls back to the primary.

---

## Phase 7: Query Forwarding

### Non-Blocking Send

**Function:** `keel_try_send_nb()`

```c
ssize_t sent = keel_try_send_nb(session->server_fd, payload, payload_len);
if (sent < 0) return KEEL_FLOW_ERROR;
if ((size_t)sent < payload_len) {
    // Partial send — defer remainder to io_uring
    return defer_send(sf, server_fd, payload + sent, payload_len - sent,
                      KEEL_FLOW_WAIT_BACKEND);
}
```

The first send attempt is synchronous non-blocking (`MSG_DONTWAIT`). If the socket buffer is full, the remainder is deferred to an io_uring send operation. This avoids an unnecessary io_uring submission for the common case (small queries that fit in the TCP buffer).

### Jumbo Messages

When a protocol message is larger than the 64KB recv buffer:

1. `frame_len()` reports the full message length (e.g., 500KB)
2. Engine detects `flen > available_bytes` → sets `jumbo_msg = true`
3. Calls `on_fe_msg()` with the available prefix (enough for SQL classification)
4. Forwards available bytes to backend
5. Records `fe_fwd_remaining = flen - available` bytes
6. Subsequent `on_client_recv_complete()` calls forward continuation data without re-parsing

### COPY Fast Path

When `sf->pins & KEEL_FPIN_COPY` is set (COPY IN active):

```
All FE data → direct forward to backend (no per-message framing)
Scan for CopyDone('c') or CopyFail('f') to detect end
Track partial headers across buffer boundaries
```

This bypasses the normal message-by-message processing for bulk data transfer, achieving near-wire-speed throughput for `COPY FROM STDIN` and `pgbench -i` workloads.

---

## Phase 8: Backend Response

### Backend Recv Loop

**File:** `src/engine/engine_flow.c`
**Function:** `keel_engine_flow_on_be_data()`

After forwarding the query, the engine arms a recv on the backend fd:

```c
keel_reactor_recv(worker->reactor, session->server_fd,
                 recv_ctx->be_buffer, sizeof(recv_ctx->be_buffer),
                 0, recv_ctx, on_backend_recv_complete);
```

### Result Forwarding

Backend data is processed message-by-message:

```
while (pos < len) {
    flen = flow->frame_len(ctx, data+pos, len-pos, 1);  // direction=1 (backend)
    
    flow->on_be_msg(ctx, data+pos, flen, &action);
    
    switch (action.type) {
    case KEEL_BE_ACT_FORWARD_FE:
        send(client_fd, data+pos, flen);    // Forward to client
        break;
    case KEEL_BE_ACT_QUERY_COMPLETE:
        // Update transaction state, return connection
        break;
    }
    pos += flen;
}
```

In practice, the engine often forwards the entire backend recv buffer to the client in one shot when all messages are `FORWARD_FE`, avoiding per-message send syscalls.

### Zero-Copy Splice Bypass (fast_network_path)

**Files:** `src/engine/engine_flow.c`, `src/worker/worker.c`  
**Config:** `fast_network_path = on` (default: on), `result_cache = off` (default: off)  
**Flow result:** `KEEL_FLOW_SPLICE_BYPASS`

When `fast_network_path = on`, the engine detects result sets containing DataRow frames
and signals the worker to enter a **zero-copy splice bypass** loop. Instead of copying
backend data into userspace for protocol processing, the worker transfers DataRow frames
directly from the backend socket to the client socket through a kernel pipe:

```
┌─────────────┐         ┌──────────┐         ┌─────────────┐
│ Backend FD  │─splice─►│  Kernel  │─splice─►│ Client FD   │
│ (recv buf)  │         │   Pipe   │         │ (send buf)  │
└─────────────┘         └──────────┘         └─────────────┘
        Zero userspace copies — data never leaves kernel space
```

**How it works:**

1. The engine processes the initial backend response (RowDescription, etc.) normally
   through `on_be_msg()`, which sets `fast_forward_mode = 1` and `splice_eligible`.
2. At the end of `keel_engine_flow_on_be_data()`, if `fast_forward_mode` is active and
   the session has a valid `s2c_pipe`, the engine returns `KEEL_FLOW_SPLICE_BYPASS`
   instead of `KEEL_FLOW_WAIT_BACKEND`.
3. The worker enters `worker_splice_bypass_loop()`, which:
   - **Peeks** at the next 5-byte PG message header via `keel_peek()` (MSG_PEEK)
   - If type = `'D'` (DataRow): calls `keel_splice_transfer()` to splice the exact
     frame length from backend socket → pipe → client socket. **Zero userspace copy.**
   - If type ≠ `'D'` (ReadyForQuery, ErrorResponse, CommandComplete, etc.): exits
     splice mode and rearms normal `recv()` so the terminal frame goes through the
     full engine protocol path.
   - If `WOULDBLOCK`: rearms recv with `on_splice_bypass_recv()` callback; when data
     arrives, the callback re-enters the peek+splice loop.
4. When the `on_splice_bypass_recv()` callback fires:
   - If the received data starts with `'D'`: sends it directly to the client (one
     userspace copy for this batch), then re-enters the splice loop for subsequent data.
   - If the data starts with a non-DataRow frame: routes through `keel_engine_flow_on_be_data()`
     for full protocol processing.

**Safety guarantees:**

- Only DataRow (`'D'`) frames are spliced — all control messages (ReadyForQuery `'Z'`,
  ErrorResponse `'E'`, CommandComplete `'C'`, ParameterStatus `'S'`, etc.) always go
  through the full engine path for state tracking.
- Splice is disabled when `result_cache = on` since cached queries
  need data captured in userspace.
- Splice is disabled when `splice_eligible = false` (plugin/hook decision).
- Transaction tracking, commit-in-doubt recovery, and LSN capture are unaffected —
  those states prevent `fast_forward_mode` from being set.

**Config interaction:**

| Config | Default | Effect on splice bypass |
|--------|---------|------------------------|
| `fast_network_path` | `on` | Enables/disables the entire splice bypass |
| `result_cache` | `off` | When `on`, forces userspace path for cache accumulation |

### ReadyForQuery Detection

**PostgreSQL:** `ReadyForQuery` message (type 'Z', 6 bytes) with transaction status byte:
- `'I'` = Idle (no transaction)
- `'T'` = In transaction block
- `'E'` = Failed transaction

**MySQL:** `OK_Packet` or `EOF_Packet` after result set, with `SERVER_STATUS_IN_TRANS` flag.

The protocol vtable's `on_be_msg()` handler detects these markers and signals query completion.

---

## Phase 9: Connection Return

### Transaction State Update

After ReadyForQuery/OK is detected:

```c
// Update pin state
if (act.pin_clear) sf->pins &= ~act.pin_clear;
if (act.pin_update) sf->pins |= act.pin_update;

// Track transaction state
if (tx_status == 'I') {
    session->in_transaction = false;
} else {
    session->in_transaction = true;
}

// Track write timestamp for sticky-primary
if (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL)) {
    sf->last_write_ns = engine_now_ns();
}
```

### Pool Return

If the session has no active pins (not in a transaction, no prepared statements, etc.):

```c
if (sf->pins == KEEL_FPIN_NONE && !session->in_transaction) {
    /* Unknown-state rule: if the protocol adapter flagged a command it
     * could not semantically model, force DISCARD ALL on the next borrow
     * so the backend is returned to a clean baseline. */
    if (keel_ssv_consistency_has_unknown(sf->consistency_atoms)) {
        be->current_state_hash = 0xFFFFFFFFFFFFFFFFULL;  // sentinel
        be->stmt_set_hash      = 0;
    }
    backend_pool_return(pool, be_conn, false);
    session->backend_conn = NULL;
    session->server_fd = -1;
}
```

The connection goes back to the pool's idle list, immediately available for other sessions.  If the unknown-state flag was set, the next borrower will receive a backend marked for DISCARD ALL cleanup.

### Query Logging

**File:** `src/log/query_log.c`
**Function:** `keel_query_log_emit()`

If query logging is enabled, a log entry is emitted after query completion:

```c
keel_query_log_t* qlog = keel_query_log_get_global();
if (qlog && qlog->config.mode != KEEL_QUERY_LOG_NONE) {
    keel_query_log_record_t qr = {
        .sql = query_text,
        .session = session,
        .duration_ns = end_time - start_time,
        .route = route_hint,
        .query_type = result.type,
    };
    keel_query_log_emit(qlog, session, &qr);
}
```

**Log modes:**
- `none` — disabled
- `all` — every query
- `read` — SELECT/SHOW only
- `write` — INSERT/UPDATE/DELETE only

**Log fields** (configurable): timestamp, source IP:port, destination IP:port, username, database, SQL text (truncatable), query tree (if enabled), duration, route.

---

## Protocol Vtable

**File:** `include/keel/protocol_flow.h`
**Struct:** `keel_proto_flow_vtable_t`

The protocol vtable provides ~25 virtual methods that abstract all protocol-specific behavior:

| Method | Purpose |
|--------|---------|
| `frame_len(ctx, data, len, dir)` | Calculate complete message length |
| `on_fe_msg(ctx, data, len, action)` | Dispatch frontend message → action |
| `on_be_msg(ctx, data, len, action)` | Dispatch backend message → action |
| `generate_greeting(ctx, buf, len)` | Server greeting (MySQL) |
| `generate_error(ctx, code, msg, buf, len)` | Protocol error message |
| `generate_ready_for_query(ctx, buf, len)` | ReadyForQuery (PG) |
| `generate_auth_ok(ctx, buf, len)` | Auth success message |
| `is_query_complete(ctx, data, len)` | Detect end of query cycle |
| `get_tx_status(ctx)` | Current transaction status |
| `build_state_sync(ctx, buf, len)` | Build state-sync prelude (search_path / SET) |
| `build_cleanup(ctx, reason, buf, len)` | Build cleanup SQL on backend return |
| `backend_reuse_gate(ctx, cond)` | Can backend be reused? |
| `capture_consistency_token(ctx, be_fd, out)` | Capture WAL LSN / GTID after write |
| `replica_reached_token(ctx, fd, tok, ms, out)` | Check replica has replayed to token |
| `probe_backend(ctx, fd)` | Liveness ping (health probe) |
| `get_backend_metadata(ctx, fd, out)` | Read role / version / recovery status |
| `get_stmt_replay(ctx, buf, len)` | Build PS replay buffer for new backend |
| `rewrite_execute_anonymous(ctx, buf, ...)` | Rewrite named Bind→anonymous |
| `name` | Protocol name string |

**Registered implementations:**
- `keel_proto_flow_postgres` — PostgreSQL v3 wire protocol
- `keel_proto_flow_mysql` — MySQL client/server protocol

---

## Key Data Structures

| Structure | File | Purpose |
|-----------|------|---------|
| `keel_session_flow_t` | engine_flow.h | Per-session flow state machine (PS replay, XID capture, commit-in-doubt, LSN token fields, SSV consistency atoms) |
| `keel_fe_action_t` | engine_flow.h | Frontend message dispatch result |
| `keel_be_action_t` | engine_flow.h | Backend message dispatch result |
| `keel_flow_result_t` | engine_flow.h | Flow result enum: OK, WAIT_BACKEND, WAIT_POOL, SEND_PENDING, LINKED_SEND, WAIT_STMT_REPLAY, WAIT_COMMIT_CHECK, **SPLICE_BYPASS**, CLOSED, ERROR |
| `keel_proto_flow_vtable_t` | protocol_flow.h | Protocol plugin vtable (~25 methods) |
| `keel_ssv_atom_t` | ssv_atom.h | Typed semantic atom (domain, virt/cost class, key, value union) |
| `keel_consistency_token_t` | plugin_types.h | WAL LSN / GTID token (char[128] + monotonic timestamp) |
| `keel_proto_query_t` | protocol.h | SQL analysis result (type, flags, needs_primary) |
| `keel_sql_lexer_t` | sql.h | Zero-alloc SQL tokenizer |
| `keel_sql_token_t` | sql.h | Token (type + string view) |
| `keel_qt_query_t` | sql.h | Full query AST (arena-allocated) |
| `route_cache_t` | route_cache.h | Per-worker 1024-entry XXHash64 hashtable |
| `route_cache_entry_t` | route_cache.h | Cache entry (hash, length, route, timestamp) |
| `recv_context_t` | worker.c | 2 × 64KB buffers + flow state per session |
| `backend_conn_t` | backend_pool.h | Backend connection (fd, state, profile) |
| `keel_query_log_t` | query_log.h | Query logging state + config |
| `keel_query_log_record_t` | query_log.h | Single query log entry |

---

## End-to-End Sequence Diagram

```
┌────────┐          ┌──────────────┐          ┌──────────┐
│ Client │          │  KEEL Worker │          │ Backend  │
└───┬────┘          └───────┬──────┘          └─────┬────┘
    │                       │                       │
    │  Query: "SELECT       │                       │
    │   * FROM users"       │                       │
    │──────────────────────►│                       │
    │                       │                       │
    │     ┌─────────────────┤                       │
    │     │ 1. frame_len()  │                       │
    │     │    → 29 bytes   │                       │
    │     │                 │                       │
    │     │ 2. on_fe_msg()  │                       │
    │     │    → QUERY      │                       │
    │     │    route=REPLICA│                       │
    │     │    effect=READ  │                       │
    │     │                 │                       │
    │     │ 3. route_cache  │                       │
    │     │    lookup(hash) │                       │
    │     │    → miss/hit   │                       │
    │     │                 │                       │
    │     │ 4. sticky check │                       │
    │     │    → no override│                       │
    │     │                 │                       │
    │     │ 5. pool select  │                       │
    │     │    → replica[0] │                       │
    │     │                 │                       │
    │     │ 6. pool_borrow  │                       │
    │     │    CAS IDLE→ACT │                       │
    │     │    needs_sync=F │                       │
    │     └─────────────────┤                       │
    │                       │                       │
    │                       │  Forward query        │
    │                       │──────────────────────►│
    │                       │                       │
    │                       │  RowDescription       │
    │                       │◄──────────────────────│
    │  RowDescription       │                       │
    │◄──────────────────────│                       │
    │                       │  DataRow(s)           │
    │                       │◄──────────────────────│
    │  DataRow(s)           │                       │
    │◄──────────────────────│                       │
    │                       │  CommandComplete      │
    │                       │◄──────────────────────│
    │  CommandComplete      │                       │
    │◄──────────────────────│                       │
    │                       │  ReadyForQuery('I')   │
    │                       │◄──────────────────────│
    │  ReadyForQuery        │                       │
    │◄──────────────────────│                       │
    │                       │                       │
    │     ┌──────────────── ┤                       │
    │     │ 7. tx_status=I  │                       │
    │     │    pins=NONE    │                       │
    │     │                 │                       │
    │     │ 8. pool_return  │                       │
    │     │    → clean_list │                       │
    │     │    wake waiter  │                       │
    │     │                 │                       │
    │     │ 9. query_log    │                       │
    │     │    emit(SELECT) │                       │
    │     │                 │                       │
    │     │10. re-arm FE    │                       │
    │     │    recv         │                       │
    │     └─────────────────┤                       │
    │                       │                       │
```

---

## SQL Query Rewriter

`keel_sql_rewrite()` (`src/sql/analyzer.c`) is a lightweight transformation stage
that applies policy-driven SQL text modifications before a query reaches the backend.

### Supported Rewrites

| Option | Effect |
|--------|--------|
| `search_path` | Prepends `SET search_path TO '<value>'; ` before the query |
| `add_statement_timeout` + `statement_timeout` | Prepends `SET LOCAL statement_timeout = '<ms>ms'; ` |
| `add_read_only` | Prepends `SET TRANSACTION READ ONLY; ` |

### Fast Path

When `opts` is NULL or no option is set, `keel_sql_rewrite()` returns the
original `keel_str_t` view unchanged (zero allocation).  When rewrites are
applied, the combined string is allocated from the supplied arena.

### Error Handling

If the arena is exhausted, `keel_sql_rewrite()` returns `KEEL_ERR_OVERFLOW` and
sets `*result` to the original SQL so callers can continue safely.
