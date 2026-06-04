# Prepared Statement Pooling

Prepared statements are one of the hardest problems in connection proxy design.
A client that issues `PREPARE myq AS SELECT ...` creates server-side state that
is bound to the exact backend TCP connection.  If the proxy hands the client a
different backend on the next query, the statement no longer exists and the
client receives `ERROR 26000: prepared statement "myq" does not exist`.

This document explains the five strategies Keel supports, the trade-offs of
each, when to use them, and the configuration knobs that control them.

---

## The Problem

PostgreSQL and MySQL both support **prepared statements** — a two-phase
execution model where the server parses and plans a query once, and the client
can execute it many times with different parameter bindings:

```
Client → PREPARE myq AS SELECT * FROM orders WHERE id = $1   → Backend
Client ← ParseComplete                                         ← Backend

Client → EXECUTE myq (42)                                    → Backend
Client ← RowDescription + DataRow(s) + CommandComplete       ← Backend
```

The named statement `myq` exists only in the server-side session of the backend
connection that received the `PREPARE`.  There is **no global statement
registry** – every backend in the pool is a fully independent PostgreSQL
process.

In a **transaction-mode connection pool**, backend connections are returned to
the pool after each transaction.  The next transaction may land on a different
backend.  When the client sends `EXECUTE myq`, the new backend has never seen
the `PREPARE` and returns an error.

The challenge is particularly acute because:

1. **ORMs** (SQLAlchemy, GORM, Hibernate, Django ORM, Prisma) prepare almost
   every query automatically without application awareness.
2. **pgx** (Go), **asyncpg** (Python), and **node-postgres** send `Parse`
   (extended query protocol) for every query by default.
3. The application developer often has no idea that prepared statements are
   being used at all.

---

## The Five Strategies

The `prepared_statement` config key selects the strategy per worker group.

### 1. `virtualize` (default)

**How it works**

Keel intercepts every named `Parse` message before it reaches the backend.
The raw Parse wire bytes are stored in a per-session cache (up to 512 slots, `PG_STMT_CACHE_SIZE`).
A synthetic `ParseComplete` is returned to the client immediately — the backend
never sees the Parse.

```
Client                    Keel                    Backend
  │                         │                         │
  │── Parse("myq", sql) ──▶ │                         │
  │                         │  [store in stmt_cache]  │
  │◀── ParseComplete ──────  │                         │
  │                         │                         │
  │── Bind("myq", $1=42) ─▶ │                         │
  │                         │── borrow backend ──────▶│
  │                         │   (check hash match)    │
  │                         │── Parse("myq",sql) ─────▶  ← replay if new backend
  │                         │── Sync ─────────────────▶
  │                         │◀─ ParseComplete ─────────
  │                         │── Bind("myq",$1=42) ────▶
  │                         │── Execute ──────────────▶
  │◀──────────── rows ───── │◀──────────── rows ───────
```

When the pool assigns a backend that already has the statement (same
`stmt_set_hash`), Keel skips the replay and sends Bind/Execute directly.

When the pool assigns a backend that does **not** have the statement, Keel
replays all confirmed named Parse messages on the new backend before forwarding
the client's Bind, transparently re-establishing the server-side state.

**Benefits**

- Full connection multiplexing — backend connections are returned to the pool
  after every transaction.
- Completely transparent to the client — it never knows a replay happened.
- Zero client-side changes needed.

**Limitations**

- Replay cost: the first Bind on a new backend incurs one extra round-trip
  (Parse + Sync → ParseComplete).  For long-running applications with many
  prepared statements this cost is amortised quickly.
- Cache size: a session can hold up to 512 named prepared statements
  (`PG_STMT_CACHE_SIZE` in `postgres_flow_internal.h`)
  simultaneously.  If a client creates more than 512 named statements, the
  oldest one is evicted (LRU).  Applications that maintain thousands of named
  statements simultaneously are not well-suited to this mode.
- Only intercepts `Parse` messages in the **extended query protocol**.  If the
  application uses `PREPARE stmt AS ...` via **simple query** (`Q` message),
  use `tracking` mode instead.

**Who should use this**

The vast majority of applications — ORMs, drivers using prepared queries
(`pgx` with `QueryRow`/`Query`), and any framework that uses extended query
protocol.  This is the default and correct choice for most deployments.

**Real-world example**

```ini
[worker_group.api]
# Python application using SQLAlchemy + asyncpg
protocol             = postgresql
prepared_statement   = virtualize   # default — no change needed
max_pool_size        = 50
```

---

### 2. `pinning`

**How it works**

When a client sends its first named `Parse` message, the backend connection is
hard-pinned to that session.  The connection is **not returned to the pool**
until the client disconnects or the connection is closed via a timeout.

The session holds the `KEEL_FPIN_PREPARED_STMT` flag.  The pool borrow
call uses `backend_pool_borrow_pinned()` which reserves the connection
exclusively.

```
Client                    Keel                    Backend
  │                         │                         │
  │── Parse("myq",sql) ───▶ │── Parse("myq",sql) ───▶ │
  │◀── ParseComplete ──────  │◀── ParseComplete ────── │
  │         ↑ session is now hard-pinned to this backend │
  │                         │                         │
  │─ (end of transaction) ─▶│                         │
  │                         │  [connection NOT returned to pool]
  │── Bind("myq",$1=42) ──▶ │── Bind("myq",$1=42) ──▶ │
  │── Execute ─────────────▶│── Execute ─────────────▶ │
  │◀──────────── rows ────── │◀──────────── rows ─────  │
```

**Benefits**

- Perfect protocol fidelity — nothing is intercepted or replayed.
- Handles any PS usage pattern, including statement-level cursors, multiple
  open portals, and binary data types.
- Lowest latency on the PS path (no hash comparison, no replay round-trip).

**Limitations**

- **Connection exhaustion**: every session that creates a prepared statement
  holds a backend connection permanently.  If you have 1 000 concurrent
  sessions, you need 1 000 backend connections.  This defeats the purpose of
  connection pooling for applications with many concurrent users.
- Not suitable for transaction-mode pools with high concurrency.
- Backend connections are wasted during idle time between transactions.

**Who should use this**

- Applications with very few long-lived sessions (e.g., an internal reporting
  tool used by 10 analysts).
- Applications where prepared statements are used with binary parameters,
  cursors, or COPY that cannot survive a replay.
- Migration scenarios where you need exact PgBouncer "session mode" semantics.

**Real-world example**

```ini
[worker_group.reporting]
# 5 long-lived reporting sessions, each holding many open cursors
protocol             = postgresql
prepared_statement   = pinning
max_pool_size        = 10   # matches the number of reporting sessions
```

---

### 3. `tracking`

**How it works**

`tracking` is an extension of `virtualize` that additionally intercepts
`PREPARE stmt_name AS sql_body` statements sent via the **simple query
protocol** (a `Q` message containing a PREPARE statement).

When Keel detects a `PREPARE` in a simple query, it parses the statement name
and SQL body, synthesises a Parse wire message, and stores it in the stmt_cache
exactly as `virtualize` would for an extended-query Parse.  The PREPARE itself
is still forwarded to the backend (the backend still creates the statement), but
now Keel has a record it can replay on new backends.

```
Client                     Keel                     Backend
  │                          │                          │
  │── Q("PREPARE myq AS ..") ▶│── Q("PREPARE myq..") ──▶│
  │                          │  [also store in cache]   │
  │◀── CommandComplete ──────│◀── CommandComplete ────── │
  │                          │                          │
  │── Execute myq($1=42) ──▶ │  [same replay as virtualize if needed]
```

Replay works identically to `virtualize` — when the client uses the prepared
statement, Keel checks whether the assigned backend has the hash and replays
if not.

**Benefits**

All of `virtualize`'s benefits, plus support for applications that use the
simple query PREPARE/EXECUTE flow.

**Limitations**

Same as `virtualize`, with the addition of:
- More complex SQL parsing to extract stmt name + body from the PREPARE text.
- Edge cases in SQL parsing may miss unusual PREPARE forms (e.g. `PREPARE stmt
  (int4, text) AS ...` with explicit type list).

**Who should use this**

- Applications using older Java JDBC drivers (pre-PgJDBC 9.4) that issue
  `PREPARE` via simple query.
- Applications that mix `PREPARE stmt AS ...` (simple) with extended-query
  Bind/Execute.
- ORM layers built on top of a lower-level adapter that doesn't use extended
  query.

**Real-world example**

```ini
[worker_group.java_app]
# Spring Boot application using an older JDBC driver
protocol             = postgresql
prepared_statement   = tracking
max_pool_size        = 100
```

---

### 4. `anonymous`

**How it works**

Keel rewrites every named `Parse` message to an anonymous (`''`) Parse before
forwarding it to the backend.  The corresponding `Bind` and `Execute` messages
are rewritten to reference the anonymous statement name `''`.

The rewrite happens entirely within the proxy.  The backend sees only
anonymous Parse/Bind/Execute cycles and never accumulates named statement state.

```
Client                     Keel                     Backend
  │                          │                          │
  │── Parse("myq",sql) ─────▶│── Parse("",sql) ────────▶│
  │◀── ParseComplete ─────── │◀── ParseComplete ─────── │
  │                          │                          │
  │── Bind("myq",$1=42) ────▶│── Bind("",$1=42) ───────▶│
  │── Execute("myq") ───────▶│── Execute("") ──────────▶│
  │◀──────────── rows ─────  │◀──────────── rows ─────── │
  │                          │                          │
  │  (backend returns to pool — no DISCARD ALL needed)  │
```

Because the backend never has named statements, connections can always be
returned to the pool without issuing `DISCARD ALL`.

**Benefits**

- Cleanest pool return path — DISCARD ALL is never needed for statement cleanup.
- Suitable for high-churn workloads where parse caching would be evicted anyway.
- Prevents accumulation of server-side statement state.

**Limitations**

- **No backend-side parse caching**: PostgreSQL's planner caches the parse tree
  and plan for named statements; anonymous statements are re-planned on every
  Execute.  For complex queries with expensive planning this is a significant
  performance regression.
- The client cannot legitimately use `Close(stmt_name)` to deallocate a
  statement after the proxy has rewritten its name — Close messages for named
  statements go through the proxy's cache and are silently absorbed.
- Binary parameters (`Bind` with `format_code = 1`) work correctly because
  the Bind rewrite preserves all parameter data.
- `Describe(statement, "myq")` is transparently rewritten to `Describe(statement, "")`.

**Who should use this**

- Applications making very short-lived connections that never re-use a named
  statement across multiple Bind/Execute invocations in the same session.
- Scenarios where backend memory pressure from accumulated client-named
  statements is a concern.
- Testing/development environments where query plan caching is not important.

**Real-world example**

```ini
[worker_group.microservice]
# Node.js microservice with very short-lived connections
# Each request creates and immediately uses one prepared statement
protocol             = postgresql
prepared_statement   = anonymous
max_pool_size        = 20
```

---

### 5. `off`

**How it works**

Keel hard-pins the backend connection to the session on the first `PREPARE` (or
named `Parse`).  No prepared-statement replay, tracking, or caching is performed
— the `Parse` / `PREPARE` is forwarded directly to the backend and the named
statement lives only on that pinned backend connection.

The pin is held until the client sends `DEALLOCATE ALL`, `DISCARD ALL`, or
disconnects.  While the pin is active the backend connection is **not returned to
the pool**, effectively converting the session to session-mode semantics for the
duration.

```
Client                    Keel                    Backend
  │                         │                         │
  │── Parse("myq",sql) ───▶ │── Parse("myq",sql) ───▶ │
  │◀── ParseComplete ──────  │◀── ParseComplete ────── │
  │         ↑ session is now hard-pinned to this backend │
  │                         │                         │
  │── Bind("myq",$1=42) ──▶ │── Bind("myq",$1=42) ──▶ │
  │── Execute ─────────────▶│── Execute ─────────────▶ │
  │◀──────────── rows ────── │◀──────────── rows ─────  │
  │                         │                         │
  │  (end of transaction)   │                         │
  │                         │  [connection NOT returned — hard-pinned] │
  │                         │                         │
  │── DEALLOCATE ALL ──────▶ │── DEALLOCATE ALL ──────▶ │
  │◀── CommandComplete ──── │◀── CommandComplete ──── │
  │                         │  [pin released, backend returned to pool] │
```

**Benefits**

- Zero overhead: no hash comparison, no replay, no stmt cache, no rewrite.
- Perfect protocol fidelity — nothing is intercepted or modified.
- Handles all PS features including cursors, portals, and binary data types.
- Predictable: identical behavior to PgBouncer session-mode for prepared
  statements.

**Limitations**

- **Connection exhaustion**: like `pinning`, every session that uses prepared
  statements holds a backend permanently while the pin is active.
- No transparent replay on backend reassignment — if the backend is lost, all
  named statements are gone.
- Pin is only released on `DEALLOCATE ALL`, `DISCARD ALL`, or disconnect.
  Individual `DEALLOCATE stmt_name` does not release the pin (other named
  statements may still exist on the backend).

**Who should use this**

- Migration from PgBouncer session-mode where exact behavioral parity is needed.
- Applications with very few concurrent sessions that rely on long-lived
  named statements and cannot tolerate any proxy intervention.
- Scenarios where the application manages its own prepared-statement lifecycle
  and explicitly deallocates all statements between workloads.

**Real-world example**

```ini
[worker_group.legacy]
# PgBouncer session-mode migration — 20 persistent connections
protocol             = postgresql
prepared_statement   = off
max_pool_size        = 25
```

---

## Comparison Table

| Feature | `virtualize` | `pinning` | `tracking` | `anonymous` | `off` |
|---------|:---:|:---:|:---:|:---:|:---:|
| True connection multiplexing | ✅ | ❌ | ✅ | ✅ | ❌ |
| Extended-query Parse intercept | ✅ | — | ✅ | ✅ | — |
| Simple-query PREPARE intercept | ❌ | — | ✅ | ❌ | — |
| No backend PS state ever | ❌ | ❌ | ❌ | ✅ | ❌ |
| Backend plan cache used | ✅ | ✅ | ✅ | ❌ | ✅ |
| Pool efficiency | High | Low | High | High | Low |
| Replay round-trip on new backend | ✅ | — | ✅ | ❌ | — |
| Binary parameter support | ✅ | ✅ | ✅ | ✅ | ✅ |
| Cursor / HOLD portal support | ❌ | ✅ | ❌ | ❌ | ✅ |
| Pin released on DEALLOCATE ALL | — | ❌ | — | — | ✅ |
| Default | ✅ | | | | |

---

## Configuration Reference

All keys are set in the `[worker_group.<name>]` section.

```ini
# Prepared-statement pooling strategy.
# Values: virtualize | pinning | tracking | anonymous | off
# Default: virtualize
prepared_statement = virtualize
```

---

## MySQL Prepared Statements

MySQL uses a binary protocol for prepared statements, distinct from the PostgreSQL
extended-query protocol.  The key verbs are:

| Client message | Server response |
|---|---|
| `COM_STMT_PREPARE` (0x16) + SQL text | `PREPARE_OK` (0x00) + stmt_id(4) + num_cols(2) + num_params(2) |
| `COM_STMT_EXECUTE` (0x17) + stmt_id + params | Result set or OK/ERR |
| `COM_STMT_CLOSE` (0x19) + stmt_id | *No response* |
| `COM_STMT_RESET` (0x1A) + stmt_id | OK |

Statement IDs are assigned by the **backend** at `PREPARE_OK` time and are not
transferable — each backend connection has its own namespace.

### How Keel tracks MySQL prepared statements

Keel maintains a per-session **statement map** (`my_stmt_entry_t stmt_map[64]`) that
records the SQL text for every active prepared statement.

**Registration flow:**

```
Client → COM_STMT_PREPARE("SELECT ? FROM t") → Backend
                                                Backend → PREPARE_OK(stmt_id=7)
Keel stores: stmt_map[i] = { stmt_id=7, sql="SELECT ? FROM t" }
stmt_active_count++
session_stmt_hash ^= fnv1a(7)   ← hash contribution
```

**Close flow:**

```
Client → COM_STMT_CLOSE(stmt_id=7) → Backend  [no response]
Keel removes stmt_map[i]
stmt_active_count--
session_stmt_hash ^= fnv1a(7)   ← un-fold the hash
if stmt_active_count == 0:
    pin_clear |= KEEL_FPIN_PREPARED_STMT
```

`COM_RESET_CONNECTION` calls `my_stmt_clear_all()`, zeroing the entire map and
resetting all counters, because the server discards all server-side state.

### Session hash and backend reuse

`session_stmt_hash` is an FNV-1a XOR accumulator over all active stmt_ids.  When
the engine borrows a backend for a session that holds prepared statements, it compares
the session's hash against the backend's hash.  A match means the backend already has
compatible server-side state and no replay is needed.

### Replay for new backends (get_stmt_replay)

When the engine assigns a new backend whose hash does not match, it calls
`get_stmt_replay()` to obtain a buffer of `COM_STMT_PREPARE` packets:

```
get_stmt_replay(ctx, &buf, &len, &count, &hash)
  → buf   = [COM_STMT_PREPARE "SELECT ? FROM t"][COM_STMT_PREPARE "UPDATE ..."]
  → count = 2
  → hash  = session_stmt_hash
```

The engine sends this replay buffer to the new backend before forwarding the
client's `COM_STMT_EXECUTE`.  The backend assigns fresh stmt_ids; the engine
updates the session's mapping accordingly.

**Capacity:** 64 statements per session (`MY_STMT_MAP_SIZE`).  Statements that
exceed this limit are not tracked.  SQL text is stored up to 1 024 bytes
(`MY_STMT_SQL_MAX`); longer SQL is truncated in the store (replay still works but
the backend may reject statements with non-matching SQL when strict hashing is enabled).

### Comparison with PostgreSQL

| Feature | PostgreSQL (`virtualize`) | MySQL |
|---|---|---|
| Statement namespace | Named (string key) | Numeric (backend-assigned int32) |
| ID transferable? | Yes (name is portable) | No (backend-assigned) |
| Map size | 512 (`PG_STMT_CACHE_SIZE`) | 64 (`MY_STMT_MAP_SIZE`) |
| Session hash | FNV-1a over SQL bytes | FNV-1a over stmt_id bytes |
| Replay unit | `Parse` wire message | `COM_STMT_PREPARE` wire message |
| Close signal | `Close('S', name)` | `COM_STMT_CLOSE(stmt_id)` — no response |
| Reset signal | `DEALLOCATE ALL` / `DISCARD ALL` | `COM_RESET_CONNECTION` |


---

## Internal Implementation

### Stmt cache

Each session's protocol context (`pg_flow_ctx_t`) holds a fixed-size cache of
up to `PG_STMT_CACHE_SIZE` (512) named statement entries:

```c
typedef struct pg_stmt_entry {
    char        name[64];       /* Statement name (NUL-terminated)        */
    uint64_t    hash;           /* FNV-1a hash of name for set comparisons */
    uint8_t*    wire_msg;       /* Raw Parse wire bytes to replay          */
    size_t      wire_msg_len;   /* Length of wire_msg                      */
    bool        valid;          /* Slot occupied                           */
    bool        confirmed;      /* ParseComplete received from backend      */
} pg_stmt_entry_t;
```

A `session_stmt_hash` (XOR of all confirmed entry hashes) acts as a fast
fingerprint of the session's statement set.  The pool borrow path compares
`session_stmt_hash` against `backend_conn->stmt_set_hash` to determine whether
replay is necessary.

### PS replay (§17)

The PS replay sequence is:

1. FE sends a named `Bind` (or `Execute`).
2. Engine calls `pgf_get_stmt_replay()` to build a replay buffer.
3. Replay buffer contains all confirmed Parse wire messages + a trailing `Sync`.
4. Engine enters `KEEL_FLOW_WAIT_STMT_REPLAY` state.
5. Replay buffer is sent to the new backend.
6. Engine counts incoming `ParseComplete ('1')` responses.
7. When count reaches 0, engine forwards the original client Bind/Execute.
8. Backend `stmt_set_hash` is updated.

### SSV (Semantic State Virtualization) Integration

The SSV engine extends the basic PS replay mechanism with a richer consistency model:

- **Hash-bucket pool index:** Backends in the pool are indexed by their `stmt_set_hash`.
  When borrowing a backend, the pool first looks up backends with a matching hash, allowing
  O(1) bypass of the replay path when a compatible backend is available.
- **OPAQUE domain split:** PS state and GUC session state (search_path, timezone, etc.) are
  tracked in separate hash domains.  This allows the SSV engine to identify backends that
  have the right PS set but need only a GUC sync, or vice versa, minimizing replay cost.
- **CONFIG domain atoms:** Session configuration state (SET variables) is tracked as first-class
  SSV atoms alongside PS atoms.  Changes to `search_path`, `timezone`, `datestyle`,
  `client_encoding`, and other GUCs are recorded and replayed on backend transitions.
- **Consistency atoms with WAL LSN:** The SSV atom layer (`keel_ssv_atom_t`) associates LSN
  tokens with session state changes, enabling `keel_ssv_requires_primary()` to force routing
  to the primary if a session's state depends on recent writes that replicas may not have
  replayed yet.
- **XOR hash cancellation fix:** A bug was fixed where `DEALLOCATE` followed by re-`PREPARE`
  of the same statement name could cancel out in the XOR hash, causing silent state divergence
  between session and backend.

See [SSV_POSTGRES_IMPLEMENTATION.md](SSV_POSTGRES_IMPLEMENTATION.md) for the full SSV
architecture and implementation details.

### Anonymous mode rewrite

`pgf_rewrite_execute_anonymous()` rewrites a `Bind` message in-place:
- Sets the prepared-statement name field to `''` (empty string, 1 NUL byte).
- Adjusts the 4-byte length field accordingly.
- Rewrites `Execute` similarly.
- The original named `Parse` was already rewritten to `''` at send time.

### Pinning mode

When `ps_mode == KEEL_PS_MODE_PINNING`, the first named `Parse` sets
`KEEL_FPIN_PREPARED_STMT` on the session's pin flags.  The engine's pool borrow
logic calls `backend_pool_borrow_pinned()` for any session carrying this flag,
which pins the backend to that session for its entire lifetime.  The backend
is not returned to the pool on `ReadyForQuery` while the flag is set.

---

## Interaction with Transaction Tracking

When `transaction_tracking = on`, Keel captures the transaction XID on every
COMMIT to enable commit-in-doubt recovery and WAL LSN-based read-after-write
consistency (see [TRANSACTION_TRACKING.md](TRANSACTION_TRACKING.md)).

The two features are orthogonal:

| `prepared_statement` | `transaction_tracking` | Notes |
|---|---|---|
| `virtualize` | `on` | Most capable; recommended for replicated clusters |
| `virtualize` | `off` | Default; best for single-server or synchronous replication |
| `pinning` | `on` | XID tracking works; connection pinning reduces pool efficiency |
| `anonymous` | `on` | Works; no PS overhead, no plan cache |

`transaction_tracking` has no effect on how prepared statements are pooled.

---

## Choosing the Right Mode

```
Does your application use prepared statements?
├── No (plain queries only)
│   └── Any mode works; use the default (virtualize)
│
├── Yes — via extended query protocol (Parse/Bind/Execute)
│   ├── Fewer than 512 simultaneous named statements per session?
│   │   └── ✅ virtualize (default)
│   ├── Uses binary parameters / open cursors / HOLD portals?
│   │   └── ✅ pinning     (at the cost of lower pool efficiency)
│   ├── Very short-lived sessions, no plan cache benefit?
│   │   └── ✅ anonymous
│   └── Migrating from PgBouncer session-mode, need exact parity?
│       └── ✅ off          (hard-pin, release on DEALLOCATE ALL / DISCARD ALL)
│
└── Yes — also via simple-query PREPARE stmt AS ...
    └── ✅ tracking
```
