# KEEL Connection Flow

This document describes the complete connection lifecycle in the KEEL database proxy — from listening socket creation through client connection, authentication, backend pool management, and session teardown.

## Table of Contents

- [Overview](#overview)
- [Connection Flow Diagram](#connection-flow-diagram)
- [Port Binding and Accept](#port-binding-and-accept)
  - [Listen Socket](#listen-socket)
  - [Accept Loop](#accept-loop)
- [Session Creation](#session-creation)
  - [Session Allocation](#session-allocation)
  - [Session Structure](#session-structure)
  - [Recv Context](#recv-context)
  - [Protocol Detection](#protocol-detection)
- [Frontend Authentication](#frontend-authentication)
  - [PostgreSQL Authentication](#postgresql-authentication)
  - [MySQL Authentication](#mysql-authentication)
- [Admission Control](#admission-control)
- [Backend Connection Pool](#backend-connection-pool)
  - [Pool Architecture](#pool-architecture)
  - [Connection States](#connection-states)
  - [Pool Partitioning](#pool-partitioning)
  - [Borrow Algorithm](#borrow-algorithm)
  - [Return and Cleanup](#return-and-cleanup)
  - [Wait Queue](#wait-queue)
  - [Async Warmup](#async-warmup)
  - [Pool Maintenance](#pool-maintenance)
- [Backend Connect Async](#backend-connect-async)
- [Backend Authentication](#backend-authentication)
- [Session-to-Backend Binding](#session-to-backend-binding)
  - [Read/Write Splitting](#readwrite-splitting)
  - [Sticky Primary](#sticky-primary)
  - [Session Pinning](#session-pinning)
- [Session Teardown](#session-teardown)
- [Key Data Structures](#key-data-structures)

---

## Overview

KEEL uses a **transaction-aware connection multiplexing** model. Each client connection (session) is not permanently bound to a single backend connection. Instead, backend connections are borrowed from a pool for the duration of a query or transaction, then returned for reuse by other sessions.

Target multiplexing ratio: **1000:50** (1000 frontend sessions sharing 50 backend connections).

**Key design principles:**
- All hot-path operations are thread-local (no locks between workers)
- Connection borrowing uses atomic CAS (compare-and-swap) for state transitions
- Backend connects are fully asynchronous via io_uring — zero `poll()` on the hot path
- Pool cleanup (DISCARD ALL) is non-blocking with deferred drain

---

## Connection Flow Diagram

```
┌──────────┐                     ┌──────────────┐                     ┌──────────────┐
│  Client  │                     │   KEEL Proxy │                     │   Backend    │
│          │                     │   (Worker)   │                     │   Database   │
└────┬─────┘                     └──────┬───────┘                     └──────┬───────┘
     │                                  │                                    │
     │  TCP connect                     │                                    │
     │────────────────────────────────► │                                    │
     │                                  │ on_accept_complete()               │
     │                                  │ ├─ slab_alloc(session)             │
     │                                  │ ├─ pool_alloc(recv_ctx)            │
     │                                  │ ├─ flow_init(protocol)             │
     │                                  │ └─ reactor_recv(client_fd)         │
     │                                  │                                    │
     │  [MySQL: server greeting]        │                                    │
     │◄─────────────────────────────────│                                    │
     │                                  │                                    │
     │  Startup / Handshake             │                                    │
     │────────────────────────────────► │                                    │
     │                                  │ on_fe_msg(STARTUP)                 │
     │                                  │ ├─ parse startup packet            │
     │                                  │ └─ begin auth exchange             │
     │                                  │                                    │
     │  Auth (SCRAM / native)           │                                    │
     │◄───────────────────────────────► │                                    │
     │                                  │                                    │
     │  AuthOK + ReadyForQuery          │                                    │
     │◄─────────────────────────────────│                                    │
     │                                  │ session.state = READY              │
     │                                  │                                    │
     │  Query (SELECT/INSERT/...)       │                                    │
     │────────────────────────────────► │                                    │
     │                                  │ on_client_recv_complete()          │
     │                                  │ ├─ classify query (R/W)            │
     │                                  │ ├─ select pool (primary/replica)   │
     │                                  │ ├─ backend_pool_borrow()           │
     │                                  │ │   └─ CAS: IDLE → ACTIVE          │
     │                                  │ ├─ state_sync (if needed)          │
     │                                  │ └─ forward query ─────────────────►│
     │                                  │                                    │
     │                                  │◄───── result rows ─────────────────│
     │  Result rows                     │                                    │
     │◄─────────────────────────────────│                                    │
     │                                  │ ReadyForQuery                      │
     │                                  │ ├─ backend_pool_return()           │
     │                                  │ │   └─ CAS: ACTIVE → IDLE          │
     │                                  │ └─ wake waiter (if any)            │
     │                                  │                                    │
     │  Disconnect                      │                                    │
     │────────────────────────────────► │                                    │
     │                                  │ close_session()                    │
     │                                  │ ├─ backend_pool_release_session()  │
     │                                  │ ├─ slab_free(session)              │
     │                                  │ └─ pool_free(recv_ctx)             │
     └──────────────────────────────────┴────────────────────────────────────┘
```

---

## Port Binding and Accept

### Listen Socket

**Function:** `create_listen_socket()` in `src/main/main.c`

Each worker group creates one listen socket:

```c
fd = socket(AF_INET, SOCK_STREAM, 0);
setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, ...);   // Fast restart
setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, ...);    // Multi-worker accept
setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, ...);     // Low latency
fcntl(fd, F_SETFL, O_NONBLOCK);
bind(fd, addr:port);
listen(fd, 4096);                                   // Large backlog
```

**`SO_REUSEPORT`** enables the kernel to distribute incoming connections across all worker threads that accept on the same port. This eliminates the thundering-herd problem without application-level load balancing.

### Accept Loop

**File:** `src/worker/worker.c`
**Function:** `on_accept_complete()`

Each worker issues a **single-shot** `keel_reactor_accept()` on the shared listen fd. When a connection arrives, the kernel wakes one worker:

```
Worker event loop:
  ┌─► keel_reactor_accept(listen_fd, single_shot=false)
  │   │
  │   ▼ [connection arrives]
  │   on_accept_complete(worker, client_fd)
  │   ├── [drain mode?] send PG FATAL 57P03 → close → rearm
  │   ├── allocate session + recv_ctx
  │   ├── initialize protocol flow
  │   ├── [TLS] SSLRequest → async TLS handshake → kTLS activation
  │   ├── [MySQL] send greeting
  │   ├── queue keel_reactor_recv(client_fd)
  │   └── rearm accept ──────────────────────────┘
```

**Drain-mode rejection:** When the engine is draining (`keel_engine_is_draining()` returns true), `on_accept_complete()` sends a PostgreSQL `ErrorResponse` (FATAL, SQLSTATE 57P03 "the database system is shutting down") to the connecting client and closes the fd. MySQL connections receive a clean close. No session is allocated.

**Why single-shot, not multishot?** Multishot accept on a shared listen fd causes all CQEs (completion queue entries) to be delivered to one io_uring ring, starving other workers. Single-shot with immediate rearm distributes connections evenly.

---

## Session Creation

### Session Allocation

When `on_accept_complete()` fires with a valid `client_fd`:

1. **Session from slab:** `keel_session_slab_alloc(&worker->sessions)` — O(1) pop from free list
2. **Recv context from pool:** `keel_pool_alloc(worker->recv_ctx_pool)` — O(1) pop from free list
3. **Socket setup:** `O_NONBLOCK` + `TCP_NODELAY` on the client fd
4. **Protocol flow init:** `keel_session_flow_init(&recv_ctx->flow, flow_vt, session)`
5. **Timer registration:** Idle timeout zombie reaper
6. **[MySQL]** Generate and send initial handshake greeting
7. **Queue recv:** `keel_reactor_recv(client_fd, buffer, on_client_recv_complete)`

If any allocation fails (slab exhausted, pool exhausted), the client fd is closed immediately and the accept is rearmed.

### Session Structure

**File:** `include/keel/session/session.h`
**Struct:** `keel_session_t`

```
keel_session_t (main fields)
├── id                     // Unique session ID (per-worker counter)
├── state                  // Session state machine
├── mode                   // I/O mode (startup, proxy, splice)
├── client_fd              // Frontend socket
├── server_fd              // Backend socket (from borrowed connection)
├── c2s_pipe / s2c_pipe    // Splice pipes (Linux zero-copy)
├── client_residual        // Partial frontend packets
├── server_residual        // Partial backend packets
├── protocol / protocol_ctx // Protocol vtable + context
├── worker                 // Owning worker (thread-local)
├── backend_conn           // Borrowed pool connection (or NULL)
├── in_transaction         // Inside BEGIN...COMMIT
├── state_hash             // Hash of SET variables
├── state_profile          // Full SET parameter state
├── pin_reason             // Bitmask (16 reason bits)
├── hard_pinned            // Exclusive backend ownership
├── username[64]           // Authenticated user
├── database[64]           // Target database
├── client_password[256]   // For passthrough auth
├── created_at / last_activity // Timestamps (ns)
├── query_count            // Lifetime query counter
├── next_free              // Slab free-list link
└── slab_index             // Position in slab array
```

**Session state machine:**

```
INIT → STARTUP → AUTH → BACKEND_CONNECT → READY ↔ QUERY → CLOSING → CLOSED
                                            ↕
                                          COPY
```

Granular sub-states:
- `FE_READ` — reading from frontend
- `FE_CLASSIFY` — protocol classify + action generation
- `FE_WAIT_BACKEND` — waiting for backend from pool
- `BE_SYNC` — sending state-sync SQL to backend
- `STREAM_COPY` — userspace read/write forwarding
- `STREAM_SPLICE` — zero-copy splice forwarding
- `HARD_PIN` — exclusive backend ownership

### Recv Context

**Struct:** `recv_context_t` (worker.c, ~400 B per instance)

After the memory architecture redesign, I/O buffers are heap-backed pointers allocated lazily
— not embedded arrays. The pool slot holds only metadata.

```
recv_context_t (~400 B)
├── session           // Owning session
├── idle_timer        // Timer wheel entry
├── flow              // Protocol flow state machine
├── be_ctx            // Backend recv callback context
├── closing / be_pending / fe_pending / send_pending  // Flags
├── fe_buf*           // Frontend recv buffer (64 KB, allocated on accept)
├── fe_cap            // Frontend buffer capacity
├── be_buf*           // Backend recv buffer (64 KB, allocated on first borrow)
├── be_cap            // Backend buffer capacity
├── tls_ctx*          // TLS context (NULL if no TLS)
└── tls_hs_buf*       // TLS handshake buffer (allocated on SSLRequest)
```

Pre-allocated in a pool of 256 slots (~100 KB), auto-grows to `session_pool_size`. Buffers are allocated lazily via `recv_ctx_alloc_fe()`, `recv_ctx_ensure_be()`, and `recv_ctx_ensure_tls_hs()`, and freed together via `recv_ctx_free_bufs()` on session teardown. This reduces idle pool memory by ~300× compared to the previous embedded-array design (~131 KB per slot).

### Protocol Detection

The protocol vtable is resolved at session creation:

```c
const char* proto_name = worker->backend_protocol;  // "postgres" or "mysql"
const keel_proto_flow_vtable_t* flow_vt = keel_proto_flow_get(proto_name);
```

The vtable provides ~25 virtual methods covering framing, dispatch, auth, and state management. This eliminates `strcmp()` branching in the hot path.

---

## Frontend Authentication

### PostgreSQL Authentication

**Protocol:** PostgreSQL v3 wire protocol

1. **TLS negotiation (if enabled):**
   - Client sends `SSLRequest` (8 bytes: length=8, code=80877103)
   - If `tls_mode=require` or `tls_mode=prefer`: proxy responds `'S'`, initiates async TLS handshake via memory BIOs (`keel_frontend_tls_handshake_start()`)
   - If `tls_mode=disable`: proxy responds `'N'`, continues in plaintext
   - **Downgrade protection:** if `tls_mode=require` and client proceeds without TLS, proxy sends FATAL 08004 and closes
   - **kTLS activation:** after successful handshake, if `ktls_enabled=1` and cipher is compatible, installs kernel TLS via `setsockopt(SOL_TLS)` for zero-copy splice
   - **mTLS:** if `tls_verify_peer=require`, peer certificate is validated; subject/issuer DN extracted into `session->tls_peer_subject/issuer`
2. **Client sends StartupMessage:** Contains `user`, `database`, protocol version
3. **Proxy parses startup:** Extracts username/database into `session->username/database`
3. **Auth method selection:**
   - **Trust mode:** Proxy sends `AuthenticationOk` immediately
   - **SCRAM-SHA-256:** Full SCRAM handshake (proxy acts as server)
     - Proxy → Client: `AuthenticationSASL` (mechanism list)
     - Client → Proxy: `SASLInitialResponse` (client-first-message)
     - Proxy → Client: `AuthenticationSASLContinue` (server-first-message)
     - Client → Proxy: `SASLResponse` (client-final-message)
     - Proxy → Client: `AuthenticationSASLFinal` (server-final-message)
4. **Proxy sends:** `AuthenticationOk` + `ReadyForQuery('I')`
5. **Session transitions to:** `KEEL_SESSION_READY`

### MySQL Authentication

**Protocol:** MySQL client/server protocol

1. **Proxy sends greeting** (server-speaks-first):
   - Server version, connection ID, auth plugin data (scramble)
   - Capability flags, character set, status flags
   - Auth plugin: `caching_sha2_password`
2. **Client sends handshake response:**
   - Username, auth response (scrambled password), database
   - Client capabilities, max packet size, character set
3. **Auth verification:**
   - **caching_sha2_password:** Verify response against stored credentials
   - **mysql_native_password:** Legacy fallback
4. **Proxy sends:** `OK_Packet` (auth success)
5. **Session transitions to:** `KEEL_SESSION_READY`

---

## Admission Control

**File:** `src/session/admission.c`, `include/keel/session/admission.h`

Each worker has a thread-local admission controller that enforces resource limits before pool
operations. No atomics required (shared-nothing architecture).

### Limits

| Limit | Config Key | Description |
|-------|-----------|-------------|
| `max_frontends` | `per_worker_max_clients` | Maximum accepted frontend connections per worker (0 = unlimited) |
| `max_backends` | `per_worker_max_pool` | Maximum backend connections in the pool per worker (0 = unlimited) |
| `max_waiting` | `per_worker_max_waiting` | Maximum sessions waiting for a backend per worker (0 = unlimited) |

### Decision Flow

```
New frontend connection arrives
  → keel_admission_try_frontend(&adm)
     ├── cur_frontends < max_frontends → ADMIT_OK (proceed to auth)
     └── at limit → ADMIT_REJECTED (send error, close fd)

Session needs a backend connection
  → keel_admission_try_backend(&adm)
     ├── cur_backends < max_backends → ADMIT_OK (proceed to pool borrow)
     ├── at backend limit, cur_waiting < max_waiting → ADMIT_QUEUED (enter wait queue)
     └── both at limit → ADMIT_REJECTED (send error to client)

Session releases frontend
  → keel_admission_release_frontend(&adm)  // decrements cur_frontends

Backend returned to pool
  → keel_admission_release_backend(&adm)   // decrements cur_backends

Waiter dequeued
  → keel_admission_dequeue_waiter(&adm)    // decrements cur_waiting
```

### Controller Structure

```
keel_admission_t (per-worker, thread-local)
├── max_frontends / max_backends / max_waiting   // Limits (set at init)
├── cur_frontends / cur_backends / cur_waiting    // Current counts
├── total_accepted / total_rejected               // Lifetime stats
├── total_queued / total_queue_timeout            // Wait queue stats
└── peak_frontends / peak_backends / peak_waiting // High-water marks
```

### Observability

Peak counters and lifetime totals are exported via `SHOW STATS_DETAIL` and Prometheus
`/metrics`. The `load_factor` (ratio of current backends to max backends) is useful for
capacity planning and auto-scaling decisions.

---

## Backend Connection Pool

### Pool Architecture

**Files:** `src/worker/backend_pool.c` (1266 lines), `src/worker/backend_pool.h` (403 lines)

Each worker maintains independent pools — one per backend server:

```
Worker 0                      Worker 1
├── primary_pool              ├── primary_pool
│   ├── clean_list            │   ├── clean_list
│   ├── idle_list             │   ├── idle_list
│   ├── dirty_list            │   ├── dirty_list
│   └── wait_queue            │   └── wait_queue
├── replica_pools[0]          ├── replica_pools[0]
│   ├── clean_list            │   ├── clean_list
│   └── ...                   │   └── ...
└── replica_pools[1]          └── replica_pools[1]
```

Pool sizes are divided across workers:
```
Config: min=20, max=60, 4 workers
→ Per-worker per-server: min=5, max=15
→ Total to primary: 4 × 15 = 60 = max_pool_size ✓
```

### Connection States

**Enum:** `backend_conn_state_t`

```
CLOSED ──► [async connect] ──► IDLE ──► ACTIVE ──► IDLE (return)
                                 │         │
                                 │         ├──► TXN_PINNED (in transaction)
                                 │         ├──► STATE_PINNED (SET state)
                                 │         └──► CLEANING (DISCARD ALL sent)
                                 │                  │
                                 │                  ├──► IDLE (cleanup confirmed)
                                 │                  └──► CLOSED (cleanup failed)
                                 │
                                 └──► CLOSED (idle timeout / error)
```

All state transitions use **atomic CAS** (compare-and-swap) to prevent double-borrow races.

### Pool Partitioning

The idle connections are organized into three linked lists (Spec §6):

| List | Contents | Priority | Reuse Cost |
|------|----------|----------|------------|
| `clean_list` | No SET state and no prepared stmts (hash=0, stmt_set_hash=0) | Highest | Zero — ready to use |
| `idle_list` | Known state profile (SET vars applied); may carry `stmt_set_hash` | Medium | May need state sync; PS-aware borrow prefers exact hash match |
| `dirty_list` | Unknown state, needs DISCARD ALL | Lowest | DISCARD ALL + sync |

### Borrow Algorithm

**Function:** `backend_pool_borrow()` / `backend_pool_borrow_profiled()`

The borrow follows a strict 5-tier search order:

```
Tier 1: Exact profile match on idle_list
        → CAS(IDLE → ACTIVE), needs_sync = false
           │
           ▼ [miss]
Tier 2: Clean connection from clean_list
        → CAS(IDLE → ACTIVE), needs_sync = (profile != empty)
           │
           ▼ [miss]
Tier 3: Any idle connection from idle_list
        → CAS(IDLE → ACTIVE), needs_sync = true
           │
           ▼ [miss]
Tier 4: Clean connection from clean_list (for non-zero hash requests)
        → CAS(IDLE → ACTIVE), needs_sync = true
           │
           ▼ [miss]
Tier 5: Dirty connection — attempt non-blocking DISCARD ALL
        → send DISCARD ALL, try immediate recv
        → If ReadyForQuery('I'): recycle, needs_sync = true
        → If EAGAIN/error: close(fd), state = CLOSED
           │
           ▼ [miss]
Return NULL — caller must queue in wait_queue
```

**Prepared-statement borrow** (`backend_pool_borrow_with_stmts(pool, required_stmt_hash)`): Used when the session holds named prepared statements (`KEEL_FPIN_PREPARED_STMT`). Searches for a connection with an exact `stmt_set_hash` match (zero-overhead reuse) before falling back to stmt-clean connections (`stmt_set_hash == 0`). Connections carrying a *different* non-zero hash are excluded to avoid costly DISCARD ALL. If neither is available, forces a non-blocking DISCARD ALL on a dirty connection to evict stale statements. See [Prepared Statement Virtualization](QUERY_FLOW.md#prepared-statement-virtualization) for the full replay protocol.

**Pinned borrow** (`backend_pool_borrow_pinned()`): Checks if session already has a pinned connection. If not, borrows new and pins:
- Admission control: refuses if `pinned_count >= max_pinned` to prevent pool starvation

### Return and Cleanup

**Function:** `backend_pool_return()`

```
backend_pool_return(pool, conn, in_transaction)
├── if in_transaction:
│   └── state = TXN_PINNED (stays pinned)
├── if clean (state_hash == 0, no profile, stmt_set_hash == 0):
│   ├── state = IDLE
│   ├── push to clean_list
│   └── wake one waiter
├── if has prepared stmts (stmt_set_hash != 0, no dirty SET state):
│   ├── state = IDLE
│   ├── push to idle_list (keyed by stmt_set_hash for PS-aware reuse)
│   └── wake one waiter (preferred by borrow_with_stmts if hash matches)
└── if dirty (has SET state or other state):
    ├── state = CLEANING
    ├── send DISCARD ALL (non-blocking)
    ├── try recv ReadyForQuery (MSG_DONTWAIT)
    ├── if confirmed:
    │   ├── clear state hash + profile + stmt_set_hash
    │   ├── push to clean_list
    │   └── wake one waiter
    ├── if EAGAIN:
    │   └── leave in CLEANING (drain_cleaning polls later)
    └── if error/EOF:
        └── close(fd), state = CLOSED
```

### Wait Queue

When `backend_pool_borrow()` returns NULL (all connections busy):

1. Session calls `backend_pool_queue_wait(pool, session, userdata)`
2. A `pool_waiter_t` is allocated from a pre-allocated waiter pool (O(1))
3. Waiter is appended to the FIFO queue
4. When a connection is returned (or a new connection completes), `wake_waiter` fires
5. The wait callback calls `backend_pool_borrow()` again — the just-returned connection is guaranteed available (single-threaded worker)

**Queue limits:**
- `max_waiting`: Up to `per_worker_max × 100` or 10,000, whichever is larger
- `wait_timeout_ms`: Waiters exceeding this are expired with `userdata=NULL` (timeout signal)

### Async Warmup

**Function:** `backend_pool_async_warmup()`

Called once after the reactor is wired into the pool. Replaces the old synchronous pre-connect loop:

```c
void backend_pool_async_warmup(backend_pool_t* pool) {
    for (size_t i = 0; i < pool->config.min_connections; i++) {
        if (backend_pool_refill_one(pool) == 0)
            break;  // No more CLOSED slots
    }
}
```

Each `backend_pool_refill_one()` finds a CLOSED slot and kicks an async io_uring connect. The connect/auth handshake completes asynchronously through the reactor's event loop — no blocking, no `poll()`.

### Pool Maintenance

Driven by timer callbacks in the worker event loop:

| Timer | Interval | Action |
|-------|----------|--------|
| Refill | 100ms (fast) / 5s (slow) | `backend_pool_refill_one()` — reconnect CLOSED slots |
| Prune | 30s | `backend_pool_prune_idle()` — close idle connections beyond `min_connections` |
| Drain cleaning | 100ms | `backend_pool_drain_cleaning()` — poll CLEANING connections for ReadyForQuery |
| Expire waiters | 100ms | `backend_pool_expire_waiters()` — timeout stale wait queue entries |

**Refill backoff:** When the backend rejects connections ("too many clients"), `refill_backoff_until` is set to a future timestamp (exponential backoff). Refill attempts are suppressed until the backoff expires.

**Adaptive refill speed:**
- **Fast refill** (100ms): Active when `wait_queue_size > 0` or `total_count < min_connections`
- **Slow refill** (5s): Active when pool is healthy and stable

---

## Backend Connect Async

**File:** `src/worker/backend_connect_async.c` (~1500 lines)

All backend connections are established asynchronously through io_uring:

```
State Machine:
  INIT ─► TCP_CONNECTING ─► [connected] ─► AUTH_STARTUP
       ─► AUTH_WAIT_RESP ─► AUTH_SCRAM_1 ─► AUTH_SCRAM_2
       ─► AUTH_SCRAM_3 ─► READY
       ─► (any state) ─► FAILED
```

**Steps:**
1. `socket(AF_INET, SOCK_STREAM)` + `O_NONBLOCK` + `TCP_NODELAY`
2. `keel_reactor_connect()` — io_uring async connect (no poll)
3. On connect complete: send startup/handshake message
4. Auth handshake (protocol-specific):
   - **PostgreSQL:** SCRAM-SHA-256 (4 round trips) or trust
   - **MySQL:** caching_sha2_password with RSA public key exchange
5. Each recv/send step is an io_uring operation with a completion callback
6. On auth complete: Move connection to pool's `clean_list`, wake waiter

**No blocking anywhere in this path.** Even the SCRAM handshake (4-8 round trips) is fully async.

---

## Backend Authentication

### PostgreSQL Backend Auth

The proxy authenticates to the backend using the credentials from the config:

1. Send `StartupMessage` with user/database
2. Backend responds with auth request (SCRAM-SHA-256)
3. Proxy performs full SCRAM exchange as a client
4. Backend sends `AuthenticationOk` + parameter status + `ReadyForQuery`

### MySQL Backend Auth

1. Receive server greeting (version, scramble, capabilities)
2. Send handshake response with `caching_sha2_password` scramble
3. Handle auth switch if needed
4. Receive `OK_Packet`

---

## Session-to-Backend Binding

### Read/Write Splitting

When a query arrives, the engine determines which pool to borrow from:

```
Query arrives
├── SQL analysis → determines read/write nature
├── if in_transaction → use current pinned backend
├── if read query → round-robin across replica pools
├── if write query → primary pool
└── if no replicas configured → primary pool
```

**Replica selection:** Round-robin index per worker (`worker->rr_replica_idx`), cycling through healthy replicas.

### Sticky Primary

Certain operations force subsequent queries to the primary, even if they are reads:

- After a write within a transaction: all queries in that transaction go to primary
- After `SET` session variables: session is "sticky" to maintain state consistency
- Explicit `BEGIN READ WRITE`: all queries in the transaction use primary

### Session Pinning

**Field:** `session->pin_reason` (32-bit bitmask)

Reasons a session may be pinned to a specific backend:

| Bit | Reason | Description |
|-----|--------|-------------|
| 0 | `PIN_TRANSACTION` | Inside BEGIN...COMMIT/ROLLBACK |
| 1 | `PIN_PREPARED_STMT` | Has server-side prepared statements |
| 2 | `PIN_TEMP_TABLE` | Created temporary table |
| 3 | `PIN_SET_VARIABLE` | Changed session variable (SET) |
| 4 | `PIN_ADVISORY_LOCK` | Holds advisory lock |
| 5 | `PIN_LISTEN` | PostgreSQL LISTEN/NOTIFY active |
| 6 | `PIN_COPY` | COPY IN/OUT active |
| 7 | `PIN_CURSOR` | Has open cursor (DECLARE CURSOR) |
| 8-15 | Reserved | Future use |

When `pin_reason != 0`, the session uses `backend_pool_borrow_pinned()` which returns the same backend connection across queries.

When all pin reasons clear (e.g., transaction commits, prepared statement closed), the connection is returned to the pool for reuse.

---

## Session Teardown

**Function:** `close_session()` in `src/worker/worker.c`

```
close_session(worker, session, recv_ctx)
├── Cancel any pending io_uring operations
├── Cancel idle timer
├── if session->backend_conn:
│   └── backend_pool_release_session()
│       ├── if in_transaction: send ROLLBACK
│       ├── send DISCARD ALL
│       └── return to pool (or close if cleanup fails)
├── close(session->client_fd)
├── close(session->server_fd) [if direct mode]
├── keel_session_flow_destroy(&recv_ctx->flow)
├── keel_residual_clear(&session->client_residual)
├── keel_residual_clear(&session->server_residual)
├── keel_pool_free(worker->recv_ctx_pool, recv_ctx)
├── keel_session_slab_free(&worker->sessions, session)
└── stats: sessions_closed++, sessions_active--
```

---

## Key Data Structures

| Structure | File | Purpose |
|-----------|------|---------|
| `keel_session_t` | session.h | Frontend client session state |
| `keel_session_slab_t` | session.h | Pre-allocated session array with free list |
| `backend_pool_t` | backend_pool.h | Per-server connection pool with 3-partition idle lists |
| `backend_conn_t` | backend_pool.h | Individual backend connection (fd, state, profile, `stmt_set_hash`) |
| `backend_pool_config_t` | backend_pool.h | Pool sizing and timeout configuration |
| `pool_waiter_t` | backend_pool.h | Wait queue entry (session + callback userdata) |
| `recv_context_t` | worker.c | Per-session metadata (~400 B) + lazy heap-backed I/O buffers |
| `keel_residual_t` | session.h | Partial packet buffer (inline 256B + overflow chain) |
| `keel_pipe_t` | session.h | Splice pipe pair for zero-copy I/O |

### Pool Sizing Reference

| Config Key | Default | Scope | Description |
|------------|---------|-------|-------------|
| `min_pool_size` | 2 | per server | Pre-established connections via async warmup |
| `max_pool_size` | 100 | per server | Maximum connections (divided across workers) |
| `max_conns_per_worker` | 1024 | per worker | Frontend session capacity |
| `idle_timeout_ms` | 300000 (5min) | per connection | Prune idle backends after this |
| `connect_timeout_ms` | 10000 (10s) | per connection | Backend connect deadline |
| `wait_timeout_ms` | 10000 (10s) | per session | Max wait time in queue |

### Connection Lifecycle Summary

```
                    ┌─────────────┐
                    │   CLOSED    │◄──── idle timeout / error / cleanup fail
                    └──────┬──────┘
                           │ async connect (io_uring)
                    ┌──────▼──────┐
                    │   IDLE on   │◄──── return (clean) / DISCARD ALL confirmed
                    │ clean_list  │
                    └──────┬──────┘
                           │ borrow (CAS: IDLE → ACTIVE)
                    ┌──────▼──────┐
                    │   ACTIVE    │ ──── query in flight
                    └──┬───┬──┬──┘
                       │   │  │
            ┌──────────┘   │  └──────────┐
            │              │             │
     ┌──────▼──────┐ ┌─────▼────┐ ┌──────▼──────┐
     │ TXN_PINNED  │ │ CLEANING │ │   IDLE on   │
     │ (in BEGIN)  │ │ (DISCARD │ │  idle_list  │
     └──────┬──────┘ │  sent)   │ │ (has state) │
            │        └────┬─────┘ └─────────────┘
            │             │
            │      ┌──────▼──────┐
            │      │ clean_list  │ (ReadyForQuery confirmed)
            │      └─────────────┘
            │
     ┌──────▼──────┐
     │ COMMIT/     │ → return to pool
     │ ROLLBACK    │
     └─────────────┘
```

---

## Connection Migration

When a worker accumulates significantly more frontend sessions than its peers, idle sessions can be
transferred to an underloaded worker at runtime without the client noticing any interruption.

### Eligibility Conditions

A session is eligible for migration only when all of the following hold:

| Condition | Reason |
|---|---|
| Session state is `READY` | No query in flight |
| No residual input data buffered | Cannot transfer partially-read protocol data |
| No pending write | Cannot transfer mid-write state |
| Backend connection released (in pool) | Backend borrow must be returned before transfer |
| Not inside an explicit transaction | `BEGIN` state is worker-local |
| Not waiting in backend queue | Still logically attached to source worker |
| Target worker found with fewer sessions | No point migrating to equally loaded peer |
| Session not already closing | Teardown already in progress |

### Transfer Mechanism

Migration is driven by the source worker calling `keel_worker_migrate_session()`:

```
Source Worker                          Target Worker
─────────────                          ─────────────
keel_worker_migrate_session()
  └─ keel_migration_can_migrate()       (eligibility check)
  └─ keel_migration_find_target()       (scan pool, pick lowest sessions.allocated)
  └─ keel_migration_send()
       ├─ sendmsg(SCM_RIGHTS)  ──────►  sock_recv receives client fd
       ├─ spsc_ring.push(meta)          ring holds: remote_addr, auth state, DB name, …
       └─ eventfd_write(1)    ──────►  worker main loop wakes via io_uring read
  └─ deferred close on source
       ├─ recv_ctx->closing = true
       └─ close(client_fd)             reactor will not issue new reads

                                        keel_migration_drain()  (in main loop tick)
                                          ├─ spsc_ring.pop(meta)
                                          ├─ recvmsg(SCM_RIGHTS) → new_fd
                                          ├─ keel_worker_on_accept(new_fd)
                                          └─ restore: remote_addr, auth, db metadata
```

### Stats

| Counter | Incremented by |
|---|---|
| `migrations_sent` | `keel_worker_migrate_session()` on successful send |
| `migrations_received` | `keel_migration_drain()` on successful receive |

Both counters are exposed in the per-worker stats snapshot (`keel_stats_basic_t`) and aggregated
in `keel_stats_snapshot_aggregate()`.

### API Summary

| Function | Location | Description |
|---|---|---|
| `keel_migration_init()` | `migration.c` | Create socketpair, SPSC ring, eventfd |
| `keel_migration_destroy()` | `migration.c` | Tear down, close descriptors |
| `keel_migration_can_migrate()` | `migration.c` | Check all eligibility conditions |
| `keel_migration_send()` | `migration.c` | Serialize and transfer session to target |
| `keel_migration_drain()` | `migration.c` | Receive all pending sessions from inbox |
| `keel_migration_find_target()` | `migration.c` | Choose least-loaded worker |
| `keel_worker_migrate_session()` | `worker.c` | High-level: check → find → send → cleanup |
| `keel_worker_on_accept()` | `worker.c` | Attach an already-connected fd (used by drain) |
