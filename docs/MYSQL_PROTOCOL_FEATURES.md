# MySQL Protocol Feature Parity

This document describes the MySQL protocol features that were added to bring
`mysql_flow.c` to feature parity with `postgres_flow.c`.

---

## Table of Contents

1. [Background](#1-background)
2. [Prepared Statement Tracking](#2-prepared-statement-tracking)
3. [Session Statement Hash and Backend Reuse](#3-session-statement-hash-and-backend-reuse)
4. [get_stmt_replay — Vtable Hook](#4-get_stmt_replay--vtable-hook)
5. [SESSION_TRACK_SCHEMA](#5-session_track_schema)
6. [SESSION_TRACK_GTIDS](#6-session_track_gtids)
7. [Cross-Service Read-Your-Writes (RYW)](#7-cross-service-read-your-writes-ryw)
8. [notify_write_lsn — Vtable Hook](#8-notify_write_lsn--vtable-hook)
9. [SQL Classification: CALL, DO, XA](#9-sql-classification-call-do-xa)
10. [Election Stability Fix (CI)](#10-election-stability-fix-ci)
11. [Configuration Reference](#11-configuration-reference)
12. [Wire Protocol Reference](#12-wire-protocol-reference)

---

## 1. Background

Before this work, `mysql_flow.c` handled the basic MySQL connection lifecycle —
handshake, COM_QUERY, COM_STMT_PREPARE/EXECUTE, transaction tracking, LOAD DATA,
result-set state machine, and session-track system-variable extraction — but was
missing several features present in `postgres_flow.c`:

| Feature | PostgreSQL | MySQL (before) | MySQL (after) |
|---------|-----------|----------------|---------------|
| Prepared-statement session map | ✅ 128 slots | ❌ | ✅ 64 slots |
| Session hash for backend matching | ✅ | ❌ | ✅ FNV-1a XOR |
| `get_stmt_replay` vtable hook | ✅ | ❌ | ✅ |
| `notify_write_lsn` vtable hook | ✅ | ❌ | ✅ |
| SESSION_TRACK_SCHEMA (type 1) | ✅ | ❌ | ✅ |
| SESSION_TRACK_GTIDS (type 3) | ✅ | ❌ | ✅ |
| Cross-service RYW intercepts | ✅ | ❌ | ✅ |
| CALL → UNKNOWN_STATE classification | ✅ | ❌ | ✅ |
| DO → WRITE classification | ✅ | ❌ | ✅ |
| XA transaction hard-pin | ✅ | ❌ | ✅ |
| COM_STMT_CLOSE count-tracking + unpin | partial | partial | ✅ |
| COM_RESET_CONNECTION clears stmt map | partial | partial | ✅ |

---

## 2. Prepared Statement Tracking

### Data structures

```c
/* Per-entry in the session statement map */
typedef struct my_stmt_entry {
    uint32_t stmt_id;          /* Backend-assigned statement identifier */
    bool     valid;            /* Slot occupied */
    char     sql[1024];        /* Original SQL text (MY_STMT_SQL_MAX) */
    size_t   sql_len;
} my_stmt_entry_t;
```

These entries live inside `my_flow_ctx_t`:

```c
/* ---- Prepared statement tracking ---- */
my_stmt_entry_t  stmt_map[64];      /* MY_STMT_MAP_SIZE */
uint32_t         stmt_active_count; /* Live entries in stmt_map */
uint64_t         session_stmt_hash; /* FNV-1a XOR of active stmt_id hashes */
char             pending_prepare_sql[1024];
size_t           pending_prepare_sql_len;
```

### Registration flow

```
FE: COM_STMT_PREPARE("SELECT ? FROM orders WHERE id = ?")
        → stashes SQL in ctx->pending_prepare_sql
        → pin_update |= KEEL_FPIN_PREPARED_STMT
        → forwarded to backend

BE: PREPARE_OK [0x00][stmt_id=7(4)][num_cols=1(2)][num_params=2(2)][0x00][warnings(2)]
        → my_stmt_add(ctx, 7, pending_prepare_sql, pending_prepare_sql_len)
        → stmt_active_count = 1
        → session_stmt_hash ^= fnv1a(7)
        → pending_prepare_sql_len = 0  (consumed)
```

### Close flow

```
FE: COM_STMT_CLOSE [0x19][stmt_id=7(4)]   (no server response)
        → my_stmt_remove(ctx, 7)
        → stmt_active_count = 0
        → session_stmt_hash ^= fnv1a(7)   (un-fold)
        → pin_clear |= KEEL_FPIN_PREPARED_STMT   (last stmt gone)
```

When `stmt_active_count > 0` after a close, the pin is **not** cleared because
other statements remain live.

### Reset flow

```
FE: COM_RESET_CONNECTION [0x1f]
        → my_stmt_clear_all(ctx)   (zeros entire stmt_map, resets count/hash)
        → pin_clear |= KEEL_FPIN_PREPARED_STMT | KEEL_FPIN_TRANSACTION | KEEL_FPIN_QUARANTINE
```

---

## 3. Session Statement Hash and Backend Reuse

`session_stmt_hash` is an FNV-1a XOR accumulator over the hashes of all active
stmt_ids:

```c
static uint64_t my_stmt_id_hash(uint32_t stmt_id) {
    uint64_t h = 14695981039346656037ULL;  /* FNV offset basis */
    for (int i = 0; i < 4; i++) {
        h ^= (uint64_t)((stmt_id >> (i * 8)) & 0xFF);
        h *= 1099511628211ULL;             /* FNV prime */
    }
    return h;
}

/* add:    session_stmt_hash ^= my_stmt_id_hash(stmt_id) */
/* remove: session_stmt_hash ^= my_stmt_id_hash(stmt_id)  (same operation) */
```

The engine uses this hash when borrowing a backend for a session that carries
prepared statements:

- **Hash match** → the backend already has the same stmt set; skip replay.
- **Hash mismatch** → call `get_stmt_replay()` and re-prepare on the new backend.

---

## 4. get_stmt_replay — Vtable Hook

```c
int myf_get_stmt_replay(void*     vctx,
                         uint8_t** replay_buf_out,
                         size_t*   replay_len_out,
                         uint32_t* stmt_count_out,
                         uint64_t* hash_out);
```

Builds a contiguous heap-allocated buffer of `COM_STMT_PREPARE` wire packets —
one per active entry in `stmt_map`.  The engine sends this buffer to a new
backend before forwarding the client's `COM_STMT_EXECUTE`.

**Output:**

| Field | Description |
|---|---|
| `*replay_buf_out` | `keel_malloc`'d buffer; caller must `keel_free()` it |
| `*replay_len_out` | Total byte size |
| `*stmt_count_out` | Number of packets in the buffer |
| `*hash_out` | `session_stmt_hash` at time of call |

Returns `0` on success, `-1` on allocation failure.  Returns `0` with `*replay_buf_out = NULL`
and `*stmt_count_out = 0` when no active statements exist.

**Packet format of each entry:**

```
[payload_len(3 LE)][seq=0][0x16 COM_STMT_PREPARE][sql_bytes...]
```

---

## 5. SESSION_TRACK_SCHEMA

MySQL 5.7+ / MariaDB 10.2+ includes a `SESSION_STATE_CHANGED` extension in OK
packets.  When the client changes the active database (e.g. `USE newdb`), the
server appends a `SESSION_TRACK_SCHEMA` (type 1) entry to the OK packet.

**Wire format of the entry inside the `session_state_changes` block:**

```
[type=0x01][entry_len(lenenc)][schema_name(lenenc_str)]
```

**Keel handling** (inside `myf_on_be_msg` SESSION_TRACK parser):

1. Reads the schema name lenenc string.
2. Copies it into `ctx->database`.
3. Sets `act->has_profile_update = true` with `profile_key = "database"` so the
   engine can persist it in the session's state profile.

This ensures that `SHOW keel_session` and pool state-sync always reflect the
current database without issuing extra queries.

---

## 6. SESSION_TRACK_GTIDS

When MySQL Group Replication / binary logging is active and the client negotiates
`CLIENT_SESSION_TRACK`, the server appends a `SESSION_TRACK_GTIDS` (type 3) entry
to the OK packet of every write statement.

**Wire format of the entry:**

```
[type=0x03][entry_len(lenenc)][encoding_spec(1)][gtid_set(lenenc_str)]
```

`encoding_spec` is currently always `0x00` (TEXT).

**Keel handling:**

1. Skips the `encoding_spec` byte.
2. Reads the GTID set string (e.g. `"aaaaaaaa-...:1-42"`).
3. Copies it into `ctx->keel_write_gtid`.

This automatically captures the latest executed GTID from write responses,
populating the cross-service RYW store without any extra SQL round-trips.

---

## 7. Cross-Service Read-Your-Writes (RYW)

Keel intercepts two special SQL patterns in COM_QUERY **without forwarding them
to the backend**:

### SET @keel_write_gtid = '...'

Used by applications or services to inject an externally-sourced write GTID into
the session (e.g. after a write on another microservice's connection pool).

```
Client → COM_QUERY "SET @keel_write_gtid = 'uuid:1-42'"
Keel   → stores 'uuid:1-42' in ctx->keel_write_gtid
Keel   → sets act->inject_consistency_lsn = 'uuid:1-42'
Keel   → sends synthetic OK back to client (no backend round-trip)
```

`inject_consistency_lsn` signals the engine to record the GTID in the session's
consistency atom so that subsequent reads are gated on replicas having replayed
up to this GTID.

### SELECT @keel_write_gtid / SELECT @@keel_write_gtid

Returns the last stored write GTID as a synthetic single-row result set.

```
Client → COM_QUERY "SELECT @keel_write_gtid"
Keel   → builds 5-packet MySQL result set:
          [column_count=1][column_def "keel_write_gtid"][EOF][data_row value][EOF]
Keel   → sends it directly to the client (no backend round-trip)
```

This allows application services to read the GTID from the session and propagate
it to other services via HTTP headers or message queues.

### Wire layout of the synthetic SELECT response

```
Packet 1: [4-byte hdr][0x01]                              ← column count = 1
Packet 2: [4-byte hdr][column_def for "keel_write_gtid"]  ← metadata
Packet 3: [4-byte hdr][0xFE 0x00 0x00 0x00 0x00]         ← EOF (end of metadata)
Packet 4: [4-byte hdr][lenenc value]                       ← data row
Packet 5: [4-byte hdr][0xFE 0x00 0x00 0x00 0x00]         ← EOF (end of rows)
```

---

## 8. notify_write_lsn — Vtable Hook

```c
void myf_notify_write_lsn(void* vctx, const char* lsn);
```

Called by the engine after a successful `capture_consistency_token()` call.
Stores the captured GTID string into `ctx->keel_write_gtid`, making it
available to subsequent `SELECT @keel_write_gtid` intercepts.

The engine calls this hook on the write path so the session's GTID store is
always up to date even for sessions that never issue `SET @keel_write_gtid`
explicitly.

---

## 9. SQL Classification: CALL, DO, XA

### CALL

```c
case KEEL_QUERY_CALL:
    *eff |= KEEL_QE_WRITE | KEEL_QE_UNKNOWN_STATE;
    *pin_set |= KEEL_FPIN_QUARANTINE;
    break;
```

`CALL` invokes a stored procedure which may contain arbitrary DML, DDL, or
control-flow.  Keel conservatively marks the session as:

- `KEEL_QE_WRITE` — the procedure may write.
- `KEEL_QE_UNKNOWN_STATE` — the procedure may leave the session in an
  unpredictable state (open cursors, temp tables, changed session variables).
- `KEEL_FPIN_QUARANTINE` — the backend cannot be returned to the shared pool
  until the session explicitly resets (COM_RESET_CONNECTION or disconnect).

### DO

```c
case KEEL_QUERY_DO:
    *eff |= KEEL_QE_WRITE;
    break;
```

`DO expr` executes an expression for its side effects without returning a result
set.  It is classified as a write because it may call functions with side effects
(e.g. `DO SLEEP(0)`, `DO my_write_func()`).

### XA Distributed Transactions

XA transactions involve a two-phase commit across multiple MySQL connections.
The session state becomes deeply coupled to a specific backend connection for the
duration of the XA branch.

```
XA START 'xid'  / XA BEGIN 'xid'
    → KEEL_QE_BEGINS_TX | KEEL_QE_HARD_PIN

XA COMMIT 'xid' / XA ROLLBACK 'xid'
    → KEEL_QE_ENDS_TX | KEEL_QE_HARD_PIN

XA END 'xid' / XA PREPARE 'xid' / XA RECOVER
    → KEEL_QE_HARD_PIN  (branch still in progress)
```

`KEEL_QE_HARD_PIN` prevents the engine from migrating or reassigning the backend
connection for the lifetime of the XA branch.

**Detection algorithm:**

The keyword scan checks for the two-character prefix `XA` (case-insensitive)
followed by the verb (`START`, `BEGIN`, `COMMIT`, `ROLLBACK`, `END`, `PREPARE`,
`RECOVER`).  It runs after the main switch falls through for `KEEL_QUERY_UNKNOWN`
or when `eff` has no routing-affecting bits set.

---

## 10. Election Stability Fix (CI)

**Commit:** `f20078c`  
**File:** `tests/test_cluster_election.c`

The `test_cluster_election` suite test [36] (`quorum_commit_repeated`) was
intermittently failing in CI with `FAIL: settled is false`.

**Root cause:** With two nodes started simultaneously, both computed nearly
identical election timeouts from their FNV-hashed node IDs.  This caused a
permanent split-vote loop — neither node could ever win term 1.

**Fix:** A 300 ms startup stagger between nodes in `build_cluster()`:

```c
if (i < n - 1) usleep(300000);  /* 300 ms stagger between starts */
```

**Safety analysis:**

| Node | Earliest election fire |
|------|----------------------|
| node-0 | 0 ms + ~650 ms max timeout = 650 ms |
| node-1 | 300 ms + ~500 ms min timeout = 800 ms |

Node-0 always fires at ≤ 650 ms; node-1 cannot fire before 800 ms.  Node-0 is
guaranteed to complete term-1 election before node-1's timer fires.

---

## 11. Configuration Reference

No new configuration keys were added.  The features are activated automatically
based on the existing `protocol = mysql` worker group setting.

```ini
[worker_group.my]
protocol             = mysql
bind_addr            = 0.0.0.0
bind_port            = 3306
pool_min_size        = 5
pool_max_size        = 100
transaction_tracking = on       # Enables RYW + GTID capture
```

---

## 12. Wire Protocol Reference

### COM_STMT_PREPARE (client → server, 0x16)

```
[payload_len(3 LE)][seq_id(1)][0x16][sql_text...]
```

### PREPARE_OK (server → client, 0x00)

```
[payload_len(3 LE)][seq_id(1)]
[0x00]              ← OK marker
[stmt_id(4 LE)]     ← backend-assigned statement ID
[num_columns(2 LE)] ← number of result-set columns
[num_params(2 LE)]  ← number of `?` parameters
[0x00]              ← reserved
[warnings(2 LE)]
```

Followed by `num_params` column-definition packets + EOF, then `num_columns`
column-definition packets + EOF (if nonzero).

### COM_STMT_CLOSE (client → server, 0x19)

```
[payload_len=5(3 LE)][seq_id(1)][0x19][stmt_id(4 LE)]
```

The server sends **no response** to this command.

### SESSION_TRACK entry in OK packet (MySQL 5.7+)

```
OK header → [status_flags(2)] with SERVER_SESSION_STATE_CHANGED bit (1<<14)
          → [info_string(lenenc)]
          → [session_state_changes(lenenc)]
               → [type(1)][entry_len(lenenc)][entry_data...]
               type 0: SESSION_TRACK_SYSTEM_VARIABLES
               type 1: SESSION_TRACK_SCHEMA    ← new handling
               type 3: SESSION_TRACK_GTIDS      ← new handling
```
