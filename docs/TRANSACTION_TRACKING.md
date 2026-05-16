# Transaction Tracking & Replication Safety

This document covers two closely related mechanisms in KEEL that together
provide replication safety and transaction outcome certainty:

1. **XID-based commit-in-doubt recovery** — captures the PostgreSQL transaction
   ID before every COMMIT so that if the backend connection dies mid-flight,
   the outcome can be ascertained on a new connection.

2. **WAL LSN consistency tokens** — captures the write-ahead log position after
   every WRITE or DDL query so that read-after-write can be guaranteed when
   subsequent reads are routed to replicas.

Both mechanisms are gated by the `transaction_tracking = on` INI option in
the `[worker_group.X]` section.  When off, neither mechanism activates and
the proxy behaves as a pure connection multiplexer.

---

## Table of Contents

1. [Why Transaction Tracking?](#why-transaction-tracking)
2. [XID Probe — Commit Rewrite](#xid-probe--commit-rewrite)
   - [Wire Layout](#wire-layout)
   - [FE Path — Rewrite in pgf_on_fe_msg](#fe-path--rewrite-in-pgf_on_fe_msg)
   - [BE Path — Probe Response Absorption](#be-path--probe-response-absorption)
   - [Content-Based Probe Recovery](#content-based-probe-recovery)
   - [commit_in_flight Flag Lifecycle](#commit_in_flight-flag-lifecycle)
3. [Commit-in-Doubt Recovery](#commit-in-doubt-recovery)
   - [Detection](#detection)
   - [txid_status Check](#txid_status-check)
   - [Synthesised Responses](#synthesised-responses)
4. [WAL LSN Consistency Tokens](#wal-lsn-consistency-tokens)
   - [The Deferred Capture Pattern](#the-deferred-capture-pattern)
   - [pgf_capture_consistency_token](#pgf_capture_consistency_token)
   - [pgf_replica_reached_token](#pgf_replica_reached_token)
5. [Configuration Reference](#configuration-reference)
6. [Data Structures](#data-structures)
7. [Sequence Diagrams](#sequence-diagrams)

---

## Why Transaction Tracking?

A connection-pooling proxy multiplexes many client sessions over a smaller
number of backend connections.  Backend connections may be shared across
clients and are returned to the pool after every transaction.

This creates a subtle reliability gap:

```
  Client                    KEEL                    PostgreSQL
    │                         │                         │
    │  COMMIT                 │                         │
    │────────────────────────►│                         │
    │                         │  "SELECT txid_current() │
    │                         │   AS _keel_txid; COMMIT"│
    │                         │────────────────────────►│
    │                         │                         │
    │                         │   [backend OOM / crash] │
    │                         │◄────────────────────────│
    │                         │     TCP RST             │
    │   ???                   │                         │
    │◄────────────────────────│                         │
```

Without tracking, KEEL cannot tell the client whether the transaction
committed or not.  With XID tracking, the proxy can consult
`txid_status(XID)` on a new connection and give the client a definitive answer.

The second problem — stale reads — arises when KEEL routes a read-only query
to a replica *after* a write.  PostgreSQL streaming replication is asynchronous
by default, so a replica may lag behind the primary.  A client that writes a
row on the primary and immediately reads it back may get a stale (missing) result
if the read goes to a replica that hasn't applied the write yet.  WAL LSN tokens
solve this by recording the LSN after the write and verifying that the chosen
replica is caught up before forwarding the read.

---

## XID Probe — Commit Rewrite

### Wire Layout

The static payload `kPgXidCommitMsg` in `postgres_flow.c` is a Simple Query
message containing a compound statement:

```
Offset  Bytes  Meaning
------  -----  -------------------------------------------------
 0       1     Message type = 'Q' (Simple Query)
 1–4     4     Message length = 0x31 (49) = 4 + strlen(sql) + 1
 5–49   45     "SELECT txid_current() AS _keel_txid; COMMIT;\0"
```

The compound statement is executed atomically in a single round-trip.
PostgreSQL processes it as two statements inside the same implicit transaction:
1. `SELECT txid_current() AS _keel_txid` — returns the XID that will commit.
2. `COMMIT` — commits the transaction.

Because `txid_current()` is called *before* the COMMIT executes, the returned
XID is the XID of the transaction being committed.

### FE Path — Rewrite in pgf_on_fe_msg

```
pgf_on_fe_msg()  [postgres_flow.c]
│
├─ message type == 'Q' (Simple Query)
├─ classify_sql() → qtype == KEEL_QUERY_COMMIT
├─ txn_tracking == true
├─ !xid_probe_active               (guard: don't double-rewrite)
│
├─ act->be_payload    = kPgXidCommitMsg    (replace payload)
├─ act->be_payload_len = sizeof(kPgXidCommitMsg)
├─ ctx->xid_probe_active = true
├─ ctx->xid_probe_result = 0               (will be filled by DataRow)
│
└─ KEEL_LOG_TRACE(PROTO, "[XID-PROBE] SET active ctx=%p ...")
```

The engine's `on_be_data` path later sets `sf->commit_in_flight = true`
(via `act.effect & KEEL_QE_ENDS_TX` in `engine_flow.c`) to track that a
COMMIT is en-route to the backend.

### BE Path — Probe Response Absorption

When the backend responds to the compound statement, it sends:

```
'T'  RowDescription      ← col "_keel_txid"
'D'  DataRow             ← data "12345"
'C'  CommandComplete     ← "SELECT 1"
'C'  CommandComplete     ← "COMMIT"
'Z'  ReadyForQuery('I')
```

`pgf_on_be_msg()` intercepts each of these when `xid_probe_active == true`:

| Message | Action |
|---------|--------|
| `'T'` RowDescription | `ABSORB` — never forwarded to client |
| `'D'` DataRow | `ABSORB`; parse col[0] as uint64 → `ctx->xid_probe_result` |
| `'C'` "SELECT 1" | `ABSORB`; clear `xid_probe_active` |
| `'C'` "COMMIT" | Forward to client (normal path, probe cleared) |
| `'Z'` ReadyForQuery | Forward to client (normal path) |

The XID value from the DataRow is propagated to the engine via
`act.commit_xid_captured = true` and `act.commit_xid`.  The engine stores
it in `sf->pending_commit_xid`.

### Content-Based Probe Recovery

In some edge cases (pool-wait resume, DISCARD ALL path), the frontend may
have forwarded the raw `COMMIT` bytes to the backend instead of the rewritten
`kPgXidCommitMsg`.  In that case the XID probe was never sent, but
`xid_probe_active` might have been set incorrectly.

Conversely, `xid_probe_active` might be cleared early by a stale
`CommandComplete` from a pipelined query (e.g. a Savepoint before COMMIT).
If that happens, the `RowDescription` from the probe will be forwarded to
the client, causing `libpq` to fail with:

```
PQprepare() failed: server sent data ("D" message) without prior row description ("T" message)
```

**Defence:** `pgf_on_be_msg` checks every incoming `RowDescription` for the
`_keel_txid` column alias via `memcmp(data + 7, "_keel_txid", 10)`.  If
found and `xid_probe_active` is false, it re-asserts the flag — ensuring
the rest of the probe response is absorbed correctly.

### Interaction with `fast_network_path` (Splice Bypass)

When `fast_network_path = on`, the splice bypass (`worker_splice_bypass_loop()`) forwards
DataRow (`'D'`) messages via zero-copy splice.  However, the splice bypass is **only active
during `fast_forward_mode`** — which is set for simple result forwarding.  During XID probe
absorption (`xid_probe_active == true`), the engine is in the normal protocol processing path,
not fast-forward mode, so the splice bypass is never engaged.  This means:

- **XID probe DataRows** (containing the captured transaction ID) are always processed through
  the full engine path and properly absorbed — never spliced to the client.
- **COMMIT result messages** (`CommandComplete`, `ReadyForQuery`) are also processed normally
  since fast-forward mode is not reactivated until after the probe is complete.
- No special guards are needed in the splice bypass for transaction tracking — the
  `fast_forward_mode` predicate inherently protects the probe sequence.

### commit_in_flight Flag Lifecycle

```
keel_session_flow_t.commit_in_flight

Set   → engine_flow.c: keel_engine_flow_on_fe_data()
         when act.effect & KEEL_QE_ENDS_TX
         (AFTER the payload is sent to the backend — not before)

Clear → engine_flow.c: keel_engine_flow_on_be_data()
         when query_complete fires (ReadyForQuery received)

Error → If backend fd closes WHILE commit_in_flight:
         keel_engine_flow_handle_commit_doubt() is called
```

---

## Commit-in-Doubt Recovery

### Detection

In `keel_engine_flow_on_be_data()`, when the backend connection is lost
(recv returns 0 or -1), the engine checks:

```c
if (sf->commit_in_flight) {
    keel_engine_flow_handle_commit_doubt(sf, session, worker);
}
```

### txid_status Check

`keel_engine_flow_handle_commit_doubt()` (`engine_flow.c`) implements the
recovery:

```
1. Set sf->commit_in_doubt = true
2. Set sf->indoubt_xid = sf->pending_commit_xid
3. Clear sf->commit_in_flight

If indoubt_xid == 0:  → outcome unknown, synthesise 08006, close session

If indoubt_xid != 0:
   4. Borrow a FRESH connection from worker->primary_pool
   5. Send: SELECT txid_status(indoubt_xid)
   6. Return KEEL_FLOW_WAIT_COMMIT_CHECK
      → on_be_data will receive and parse the response
```

The check connection (`sf->xid_check_conn`) is a separate pool borrow that
must be returned after the check.  It is NOT the session's normal backend
connection.

### Synthesised Responses

After receiving the `txid_status()` response in `on_be_data`:

| `txid_status()` result | Synthesised client response |
|------------------------|------------------------------|
| `"committed"` | `CommandComplete("COMMIT") + ReadyForQuery('I')` — session continues normally |
| `"aborted"` | `ErrorResponse(40000: transaction aborted) + ReadyForQuery('E')` |
| `"in progress"` | `ErrorResponse(08006: outcome uncertain)` — session closed |
| `"null"` (XID expired) | `ErrorResponse(08006: check txid_status manually)` — session closed |
| pool unavailable | `ErrorResponse(08006: pool unavailable, check xid manually)` — session closed |

All error responses include the XID value so operators can verify:
```sql
SELECT txid_status(12345);  -- returns 'committed', 'aborted', 'in progress', or null
```

---

## WAL LSN Consistency Tokens

### The Deferred Capture Pattern

The critical design constraint is that `capture_consistency_token` **must not**
be called on the backend socket while the socket is non-blocking and the backend
may still be processing the write.  Calling it prematurely causes:

1. The `SELECT pg_current_wal_lsn()` query to be queued in the kernel write
   buffer on a non-blocking fd.
2. `recv()` returns `EAGAIN` immediately because the write result has not yet
   arrived.
3. The LSN query sits in the write buffer, unprocessed.
4. When the backend eventually becomes idle, it processes the queued LSN query
   and sends `RowDescription + DataRow + CommandComplete + ReadyForQuery`.
5. These messages arrive as the start of the **next** query's response,
   causing `libpq` to see a `RowDescription ('T')` it did not expect, followed
   by `DataRow ('D')` — triggering the `"D message without prior T message"` error.

**Solution — deferred flag:**

```
FE path (keel_engine_flow_on_fe_data):
  if (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL)):
      sf->capture_lsn_pending = true   // just set the flag — no I/O

BE path (keel_engine_flow_on_be_data) — AFTER ReadyForQuery:
  if (sf->capture_lsn_pending && query_complete):
      flags = fcntl(be_fd, F_GETFL)
      fcntl(be_fd, F_SETFL, flags & ~O_NONBLOCK)  // switch to blocking
      flow->capture_consistency_token(ctx, be_fd, &sf->last_write_token)
      fcntl(be_fd, F_SETFL, flags)                 // restore non-blocking
      sf->capture_lsn_pending = false
```

At the point of capture, the backend has just sent `ReadyForQuery`, confirming
it is idle and waiting for the next command.  The socket is temporarily set to
blocking so `recv()` does not return `EAGAIN` mid-response.

### pgf_capture_consistency_token

**File:** `src/protocol/postgres/postgres_flow.c`

Sends `SELECT pg_current_wal_lsn()` on the (blocking) backend fd, then drains
the response completely message-by-message:

```
Send:  'Q' + len4 + "SELECT pg_current_wal_lsn()\0"

Recv loop (until 'Z'):
  Read 5-byte header:  type(1) + length_be32(4)
  Read body:           (length - 4) bytes
  
  if type == 'D' (DataRow):
    parse col[0] → copy LSN string into out->value[]
    set out->captured_at_ns = CLOCK_MONOTONIC_COARSE
    lsn_found = true

  if type == 'Z' (ReadyForQuery):
    return 0 if lsn_found else -1

  (all other messages: T, C, E — silently discard)
```

**Safety invariant:** every byte of the response (`T + D + C + Z`) is consumed
before returning.  This guarantees the socket is empty and the next
`keel_reactor_recv()` will only see new query responses.

**Oversized bodies:** if a message body exceeds the 4096-byte stack buffer, it
is drained in 512-byte chunks without storing.  This is a safety path; LSN
text is always < 32 bytes.

### pgf_replica_reached_token

**File:** `src/protocol/postgres/postgres_flow.c`

Sends `SELECT COALESCE(pg_last_wal_replay_lsn(), pg_current_wal_lsn()) >= 'LSN'::pg_lsn`
on the replica's fd (blocking mode), then:

- Parses the `DataRow` result column as a boolean text (`'t'` / `'f'`).
- Returns `*out_reached = true` if the character is `'t'`.
- Drains all messages until `ReadyForQuery ('Z')`.

The `COALESCE` is essential: on the primary, `pg_last_wal_replay_lsn()`
returns `NULL` (the primary is not in recovery).  `COALESCE` substitutes
`pg_current_wal_lsn()` so the query works correctly on any node.

If `token->value[0] == '\0'` (no prior write in this session), the function
sets `*out_reached = true` immediately without any I/O — there is nothing to
wait for.

---

## Configuration Reference

```ini
[worker_group.pg]
# Enable XID probe + commit-in-doubt + WAL LSN capture
transaction_tracking = on    # default: off
```

When `off` (default):
- No COMMIT rewrite — bare `COMMIT` forwarded as-is.
- `capture_lsn_pending` is never set — no LSN I/O after writes.
- Commit-in-doubt detection is disabled.
- All related fields in `keel_session_flow_t` remain zero.

---

## Data Structures

### keel_session_flow_t (engine_flow.h — relevant fields)

| Field | Type | Purpose |
|-------|------|---------|
| `txn_tracking` | `bool` | Inherited from `worker->txn_tracking` at session init |
| `commit_in_flight` | `bool` | True from FE COMMIT send until BE ReadyForQuery received |
| `pending_commit_xid` | `uint64_t` | XID captured from `_keel_txid` DataRow; 0 if not yet captured |
| `commit_in_doubt` | `bool` | True when backend died while `commit_in_flight` |
| `indoubt_xid` | `uint64_t` | Copy of `pending_commit_xid` at time of doubt |
| `indoubt_check_result` | `int` | Result from `txid_status()` check (0=unknown,1=committed,2=aborted) |
| `xid_check_conn` | `backend_conn_t*` | Borrowed pool connection for txid_status check |
| `capture_lsn_pending` | `bool` | Set by FE path; consumed by BE path to trigger LSN capture |
| `last_write_token` | `keel_consistency_token_t` | Most recent WAL LSN after a write |
| `consistency_atoms[3]` | `keel_ssv_atom_t[3]` | SSV consistency domain atoms: [0] WRITE_LSN (str), [1] WRITE_LSN_TS (u64 ns), [2] UNKNOWN_STATE (bool).  Populated by `keel_ssv_consistency_set_token()` after capture; consumed by sticky-primary routing and pool return |

### pg_flow_ctx_t (postgres_flow_internal.h — XID-probe fields)

| Field | Type | Purpose |
|-------|------|---------|
| `txn_tracking` | `bool` | Inherited from session worker config |
| `xid_probe_active` | `bool` | COMMIT rewrite is in flight; absorption mode on |
| `xid_probe_result` | `uint64_t` | XID value parsed from the probe DataRow |

### keel_consistency_token_t (plugin_types.h)

```c
typedef struct {
    char     value[KEEL_CONSISTENCY_TOKEN_MAX];  /* LSN text, e.g. "0/16B3740\0" */
    uint64_t captured_at_ns;                     /* CLOCK_MONOTONIC_COARSE timestamp */
} keel_consistency_token_t;
```

---

## Sequence Diagrams

### Successful COMMIT with XID Probe

```
Client                 KEEL                    PostgreSQL
  │                     │                         │
  │  'Q' "COMMIT"       │                         │
  │────────────────────►│                         │
  │                     │  pgf_on_fe_msg:         │
  │                     │  rewrite → kPgXidCommitMsg
  │                     │  xid_probe_active=true  │
  │                     │  commit_in_flight=true  │
  │                     │                         │
  │                     │  "SELECT txid_current() │
  │                     │   AS _keel_txid; COMMIT"│
  │                     │────────────────────────►│
  │                     │                         │
  │                     │  'T' RowDescription     │
  │                     │◄────────────────────────│
  │                     │  absorbed (no fwd)      │
  │                     │                         │
  │                     │  'D' DataRow("12345")   │
  │                     │◄────────────────────────│
  │                     │  pending_commit_xid=12345
  │                     │  absorbed (no fwd)      │
  │                     │                         │
  │                     │  'C' "SELECT 1"         │
  │                     │◄────────────────────────│
  │                     │  absorbed               │
  │                     │  xid_probe_active=false │
  │                     │                         │
  │                     │  'C' "COMMIT"           │
  │                     │◄────────────────────────│
  │  'C' "COMMIT"       │                         │
  │◄────────────────────│                         │
  │                     │                         │
  │                     │  'Z' ReadyForQuery('I') │
  │                     │◄────────────────────────│
  │                     │  commit_in_flight=false │
  │                     │  ── if capture_lsn_pending:
  │                     │     fcntl(NOBLOCK off)  │
  │                     │     capture_consistency_token()
  │                     │     fcntl(NOBLOCK on)   │
  │  'Z' RFQ('I')       │                         │
  │◄────────────────────│                         │
```

### Backend Death Mid-COMMIT + Recovery

```
Client                 KEEL                    New connection
  │                     │                         │
  │  'Q' "COMMIT"       │                         │
  │────────────────────►│  commit_in_flight=true  │
  │                     │────── COMMIT ──────────►█
  │                     │             [server crash]
  │                     │◄─────────────── TCP RST │
  │                     │                         │
  │                     │  recv() → EOF           │
  │                     │  commit_in_flight==true │
  │                     │  → handle_commit_doubt()│
  │                     │    indoubt_xid=12345    │
  │                     │                         │
  │                     │     borrow pool conn ──►│
  │                     │     "SELECT txid_status  │
  │                     │      (12345)"           │
  │                     │────────────────────────►│
  │                     │       'D' "committed"   │
  │                     │◄────────────────────────│
  │                     │       'Z' RFQ('I')      │
  │                     │◄────────────────────────│
  │                     │     return pool conn    │
  │                     │                         │
  │  'C' "COMMIT"       │  synthesise             │
  │◄────────────────────│  CommandComplete(COMMIT)│
  │  'Z' RFQ('I')       │  + ReadyForQuery('I')   │
  │◄────────────────────│                         │
```

### WAL LSN Capture + Replica Reached Check

```
Client                 KEEL-Primary         KEEL-Replica
  │                     │                         │
  │  INSERT …           │                         │
  │────────────────────►│                         │
  │                     │─── INSERT ─────────────►│(primary)
  │                     │     [write completes]   │
  │                     │ 'Z' ReadyForQuery ◄────-─│
  │                     │                         │
  │                     │ capture_lsn_pending=true│
  │                     │ fcntl(non-block off)    │
  │                     │ "SELECT pg_current_wal_lsn()"
  │                     │────────────────────────►│(primary)
  │                     │◄─── "0/16B3740" ────────│
  │                     │ last_write_token="0/16B3740"
  │                     │ consistency_atoms[0].str="0/16B3740"
  │                     │ consistency_atoms[1].u64=<monotonic_ns>
  │                     │ fcntl(non-block on)     │
  │  RFQ               │                         │
  │◄────────────────────│                         │
  │                     │                         │
  │  SELECT …           │                         │
  │────────────────────►│                         │
  │                     │ route=REPLICA           │
  │                     │ last_write_token set    │
  │                     │ replica_reached_token() │
  │                     │ "SELECT COALESCE(       │
  │                     │  pg_last_wal_replay_lsn,│
  │                     │  pg_current_wal_lsn)    │
  │                     │  >= '0/16B3740'::pg_lsn"│
  │                     │────────────────────────►│(replica)
  │                     │     'D' "t"             │
  │                     │◄────────────────────────│
  │                     │ out_reached=true        │
  │                     │ route query to replica  │
  │  result rows        │◄── rows ───────────────►│
  │◄────────────────────│                         │
```
