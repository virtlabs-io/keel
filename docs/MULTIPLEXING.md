# Connection Multiplexing Architecture

## Table of Contents

1. [Overview](#overview)
2. [Architecture Design](#architecture-design)
3. [Core Concepts](#core-concepts)
4. [Implementation Details](#implementation-details)
5. [Connection Migration](#connection-migration)
6. [Kernel-Level Mechanics](#kernel-level-mechanics)
7. [Codebase Guide](#codebase-guide)
8. [Configuration](#configuration)
9. [Performance Characteristics](#performance-characteristics)
10. [Known Limitations & Challenges](#known-limitations--challenges)
11. [Improvement Roadmap](#improvement-roadmap)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)

---

## Overview

The KEEL connection multiplexing system is a multi-threaded architecture designed to handle thousands of concurrent database connections efficiently. Unlike traditional single-threaded proxies, KEEL uses **worker groups** where each group consists of multiple **worker threads**, each with completely isolated resources.

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **Scalability** | Linear scaling with CPU cores |
| **Isolation** | No lock contention between workers |
| **Efficiency** | Per-worker memory pools eliminate malloc overhead |
| **Kernel Integration** | SO_REUSEPORT for optimal connection distribution |
| **Resilience** | Worker failures don't affect other workers |

### Design Philosophy

```
"Share nothing, scale linearly"
```

The fundamental principle is **complete worker isolation**. Each worker owns its resources entirely:
- No shared mutable state between workers
- No locks needed for per-worker operations
- Each worker has its own reactor, session slab, backend pool, and timer wheel

---

## Architecture Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Worker Manager                                │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  Manages worker groups, lifecycle, global configuration         │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                   │                                     │
│          ┌────────────────────────┼────────────────────────┐            │
│          ▼                        ▼                        ▼            │
│   ┌─────────────┐          ┌─────────────┐          ┌─────────────┐     │
│   │Worker Group │          │Worker Group │          │Worker Group │     │
│   │ Port: 7432  │          │ Port: 6433  │          │ Port: 6434  │     │
│   │  (Main)     │          │ (Analytics) │          │  (Admin)    │     │
│   └─────────────┘          └─────────────┘          └─────────────┘     │
│          │                         │                        │           │
│     ┌────┼────┬────┐         ┌─────┼────┐              ┌────┼────┐      │
│     ▼    ▼    ▼    ▼         ▼     ▼    ▼              ▼    ▼    ▼      │
│   ┌───┐┌───┐┌───┐┌───┐      ┌───┐┌───┐┌───┐          ┌───┐┌───┐┌───┐    │
│   │W0 ││W1 ││W2 ││W3 │      │W0 ││W1 ││W2 │          │W0 ││W1 ││W2 │    │
│   └───┘└───┘└───┘└───┘      └───┘└───┘└───┘          └───┘└───┘└───┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Worker Anatomy

Each worker (W) is a complete, self-contained unit:

```
┌─────────────────────────────────────────────────────────┐
│                      Worker Thread                      │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │   Reactor    │  │ Memory Arena │  │ Slab Allocator│  │
│  │  (io_uring   │  │   (64 KB)    │  │  (sessions)   │  │
│  │  /kqueue     │  └──────────────┘  └───────────────┘  │
│  │  /epoll)     │                                       │
│  └──────────────┘  ┌──────────────┐                     │
│                    │Recv Ctx Pool │                     │
│                    │(~400B meta + │                     │
│                    │ lazy heap)   │                     │
│                    └──────────────┘                     │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Backend Connection Pool             │   │
│  │  ┌────────────────────┐ ┌─────────────────────┐  │   │
│  │  │  Primary Pool      │ │  Replica Pool(s)    │  │   │
│  │  │  (min=10, max=50)  │ │  (weighted routing) │  │   │
│  │  └────────────────────┘ └─────────────────────┘  │   │
│  │  Async refill via reactor (connect + SCRAM)      │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Timer Wheel  │  │ Listen FD    │  │  Pipe Pool   │   │
│  │ (refill,     │  │(SO_REUSEPORT)│  │ (Linux only, │   │
│  │  prune,      │  └──────────────┘  │  for splice) │   │
│  │  idle check) │                    └──────────────┘   │
│  └──────────────┘                                       │
│                                                         │
│  ┌──────────────┐  ┌──────────────────────────────────┐  │
│  │  Statistics  │  │     Migration Channel            │  │
│  │(thread-local)│  │  sock_recv / sock_send (DGRAM)   │  │
│  └──────────────┘  │  inbox ring (SPSC, cap=64)       │  │
│                    │  eventfd wakeup                  │  │
│                    └──────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Session Lifecycle

Each client session follows a well-defined state machine managed by the engine:

```
                    ┌─────────┐
                    │   NEW   │   ← Connection accepted by reactor
                    └────┬────┘
                         │
                         ▼
                  ┌────────────┐
                  │ HANDSHAKE  │  ← PostgreSQL StartupMessage
                  └─────┬──────┘
                        │
                        ▼
                    ┌───────┐
                    │  AUTH │     ← SCRAM-SHA-256 / MD5 / Trust
                    └───┬───┘
                        │
                        ▼
                    ┌───────┐
            ┌──────→│ READY │←─────────┐
            │       └───┬───┘          │
            │           │              │
            │           ▼              │
            │       ┌───────┐          │
            │       │ QUERY │     ← SQL parsed, routed
            │       └───┬───┘          │
            │           │              │
            │           ▼              │
            │      ┌─────────┐         │
            │      │ PROXIED │    ← Data flowing via engine_flow
            │      └────┬────┘         │
            │           │              │
            │           ▼              │
            │     ┌──────────┐         │
            │     │ TXN DONE │    ← ReadyForQuery, pool return
            │     └────┬─────┘         │
            │          │                   │
            │          ├──── PS replay? ───► WAIT_STMT_REPLAY
            │          │    (named PS      │   ← Replay Parse msgs + Sync
            │          │     session)      │   ← Count ParseCompletes
            │          │                  │   ← Drain RFQ from replay Sync
            │          │◄──────────────────┘
            └──────────┴───────────────────┘
                       │
                       ▼
                   ┌────────┐
                   │ CLOSED │     ← Backend returned to pool
                   └────────┘
```

---

## Core Concepts

### 1. Worker Group

A **worker group** is a collection of worker threads that handle connections on a specific port. Each group is independent and can have different configurations (pool sizes, backends, timeouts, etc.).

### 2. Worker Thread

A **worker** is a single thread with its own reactor and all resources:

- **Reactor** — io_uring ring (Linux), kqueue (macOS), or epoll (Linux fallback)
- **Session Slab** — fixed-size allocator for client sessions
- **Recv Context Pool** — O(1) free-list allocator for recv contexts (~400 B metadata shell + lazy heap-backed I/O buffers); eliminates per-connection malloc/free
- **Backend Pool** — per-worker pool of backend connections (primary + replicas)
- **Waiter Pool** — O(1) free-list allocator for pool wait entries (24 bytes each)
- **Timer Wheel** — handles refill timer (100ms), prune timer (30s), idle checks
- **Pipe Pool** — pre-allocated pipes for splice(2) zero-copy (Linux only)
- **Listen FD** — own socket with SO_REUSEPORT for kernel-distributed accepts

### 3. Backend Connection Pool

Each worker maintains its own backend connection pool:
- **Primary pool** — connections to the primary PostgreSQL server
- **Replica pools** — separate pools for each replica, with weighted routing
- **Async refill** — when pool is depleted, new connections are established asynchronously via the reactor (TCP connect + SCRAM-SHA-256 handshake)
- **Wait queue** — sessions that couldn't get a connection are queued and resumed when one becomes available
- **Refill timer** — fires every 100ms, initiates up to 4 async reconnects per tick
- **Prune timer** — fires every 30s, removes idle connections exceeding `min_pool_size`

### 4. Connection Distribution Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| `reuseport` | Each worker has own socket with SO_REUSEPORT; kernel distributes | High-throughput, modern Linux |
| `accept_lock` | Workers share one socket, mutex on accept() | Portability, older kernels |
| `round_robin` | Single acceptor dispatches to workers | Debugging, controlled distribution |

---

## Implementation Details

### Reactor-Driven Worker Loop

Each worker thread runs a reactor event loop:

```c
static void* worker_thread_main(void* arg) {
    keel_worker_t* worker = arg;

    // Create reactor (io_uring / kqueue / epoll)
    worker->reactor = keel_reactor_create(&reactor_config);

    // Create SO_REUSEPORT listen socket
    worker->listen_fd = create_listen_socket(addr, port, SO_REUSEPORT);

    // Register accept with reactor
    keel_reactor_accept(reactor, listen_fd, on_accept_cb, worker);

    // Register timers
    keel_reactor_timer(reactor, 100ms, refill_timer_cb);   // pool refill
    keel_reactor_timer(reactor, 30s,  prune_timer_cb);     // idle prune

    // Main reactor loop
    while (atomic_load(&worker->state) == RUNNING) {
        keel_reactor_submit(reactor);
        keel_reactor_wait(reactor, timeout);
        keel_reactor_process(reactor);  // dispatch completions
    }

    keel_reactor_destroy(reactor);
    return NULL;
}
```

### Non-Blocking Backend Connect

Backend connections are established entirely on the reactor without blocking:

```
Phase 0: TCP connect (non-blocking, reactor-driven)
Phase 1: Receive AuthenticationSASL challenge
Phase 2: Send SASLInitialResponse (SCRAM client-first)
Phase 3: Receive SASLContinue (server-first)
Phase 4: Compute SCRAM proof, send SASLResponse (client-final)
Phase 5: Receive SASLFinal (server-final), verify
Phase 6: Receive AuthenticationOk
Phase 7: Consume ParameterStatus / BackendKeyData
Phase 8: Receive ReadyForQuery → connection ready for use
```

All phases use `keel_reactor_connect()`, `keel_reactor_send()`, and `keel_reactor_recv()` — no blocking system calls.

### Memory Allocation Pattern

Workers use arena allocation for request-scoped memory and slab allocation for sessions:

```c
// Sessions allocated from slab (fixed-size, O(1) alloc/free)
keel_session_t* session = keel_slab_alloc(&worker->session_slab);

// Recv contexts from pool allocator (O(1) free-list, no syscall)
recv_context_t* ctx = keel_pool_alloc(worker->recv_ctx_pool);

// Pool waiters from pool allocator (24B each, free-list)
pool_waiter_t* w = keel_pool_alloc(pool->waiter_pool);

// Per-request data from arena (bump allocation, reset between requests)
void* buf = keel_arena_alloc(worker->arena, size);
```

Benefits:
- **No malloc/free overhead**: Arena is bump-allocated, slab and pool use free-list
- **Cache-friendly**: Related data is contiguous
- **No fragmentation**: Arena resets periodically
- **Thread-safe by isolation**: Each worker has own allocators
- **Zero syscalls on hot path**: Pool alloc/free are pure userspace operations

---

## Connection Migration

Connection migration allows an idle session to be handed from one worker thread to another without interrupting the client. This enables **runtime load rebalancing** — a worker that has accumulated more sessions than its peers can shed sessions to lightly-loaded workers.

### Design

Migration bridges two workers using three mechanisms:

```
 Source Worker (W0)                    Destination Worker (W1)
 ┌─────────────────────────┐           ┌──────────────────────────────┐
 │ keel_worker_migrate_     │           │ migration.inbox (SPSC ring)  │
 │ session()               │           │                              │
 │  ├─ can_migrate(s)?      │           │ keel_migration_drain()       │
 │  ├─ find_target()  ──── pick W1 ──►  │  ├─ pop msg from inbox       │
 │  └─ migration_send()    │  SCM_RIGHTS│  ├─ recvmsg() → client_fd    │
 │      ├─ sendmsg()  ─────────────────►│  ├─ worker_setup_session()   │
 │      ├─ ring_push() ────► inbox ────►│  └─ restore session metadata │
 │      └─ eventfd_write() ──────────► wakeup                         │
 └─────────────────────────┘           └──────────────────────────────┘
```

| Mechanism | Purpose |
|-----------|--------|
| **Unix socketpair** (`SOCK_DGRAM`, non-blocking) | Transfer the client fd via `sendmsg()`+`SCM_RIGHTS` — zero-copy, zero-alloc |
| **SPSC ring buffer** (`keel_spsc_ringbuf_t`, capacity=64) | Carry serialised session state (credentials, timing, residual) alongside the fd |
| **eventfd** write | Wake the destination worker's reactor immediately |

### Migration Eligibility

A session may only be migrated when **all** of the following hold:

| Condition | Rationale |
|-----------|----------|
| `state == KEEL_SESSION_READY` | Not mid-query or mid-startup |
| `!in_transaction` | Uncommitted state cannot be moved |
| `pin_reason == 0` | Not soft-pinned (prepared stmt, SET var) |
| `!hard_pinned` | Not exclusively owned by a backend |
| `backend_conn == NULL` | Connection returned to pool |
| `client_residual` empty | No partial packet buffered |
| `plugin_state == NULL` | No opaque plugin state to carry |
| `client_fd >= 0` | Valid open socket |

### Source-Side Cleanup

Because the io_uring reactor has a pending `recv` on the client fd when a session is idle, migration uses the **deferred-close pattern**: the source worker marks `recv_ctx->closing = true`, closes its copy of `client_fd` (which cancels the io_uring recv), and lets `on_client_recv_complete` clean up the session slot naturally. The destination received its own dup'd fd via SCM_RIGHTS before the source closed its copy.

### Integration with Worker Loop

```c
// worker_thread_func — after keel_timer_wheel_tick():
if (worker->migration.inbox &&
    !keel_spsc_ringbuf_is_empty(worker->migration.inbox)) {
    keel_migration_drain(&worker->migration, worker);
}
```

### Statistics

Two new counters track migration activity per worker:

| Counter | Description |
|---------|------------|
| `migrations_sent` | Sessions handed to another worker |
| `migrations_received` | Sessions adopted from another worker |

Both are aggregated into the global snapshot by `stats.c`.

### API Summary

| Function | Thread | Description |
|----------|--------|---------|
| `keel_migration_init()` | main (startup) | Create socketpair + ring buffer |
| `keel_migration_destroy()` | main (shutdown) | Close sockets, free ring |
| `keel_migration_can_migrate()` | source worker | Check eligibility |
| `keel_migration_send()` | source worker | SCM_RIGHTS FD transfer + ring push |
| `keel_migration_drain()` | dest worker | Pop ring, recvmsg, setup_session |
| `keel_migration_find_target()` | source worker | Return least-loaded worker index |
| `keel_worker_migrate_session()` | source worker | High-level: check → find → send → cleanup |

### Automatic Rebalancing

While the migration infrastructure described above provides the *mechanism* for moving sessions between workers, KEEL also includes an automatic *trigger* that detects load imbalances and initiates migrations without operator intervention.

#### How It Works

Each worker runs a periodic timer callback (`rebalance_timer_cb`) that executes the following algorithm:

```
rebalance_timer_cb(worker):
  1. Compute per-worker session counts:
       my_count = worker.sessions.allocated
       avg = total_sessions / num_workers
       min_count = min(all worker session counts)

  2. Check imbalance threshold:
       if (my_count × 100 ≤ avg × threshold_pct): skip (not overloaded)
       if (my_count ≤ min_count + 1): skip (no meaningful difference)

  3. Migrate eligible sessions:
       target = avg + 1
       migrated = 0
       for each session in this worker:
         if can_migrate(session) && migrated < max_per_tick:
           worker_migrate_session(session) → migrated++
         if sessions.allocated ≤ target: stop

  4. Re-arm timer for next interval
```

#### Key Design Properties

| Property | Detail |
|----------|--------|
| **Threshold-based** | Only triggers when a worker has >1.25× (configurable) the average session count |
| **Convergence guard** | Targets `avg + 1` to avoid oscillation between workers |
| **Minimum difference** | Requires `my_count > min_count + 1` before migrating |
| **Capped migrations** | At most `max_per_tick` (default 4) migrations per check interval |
| **Cooperative** | Each overloaded worker independently sheds load — no central coordinator |
| **Zero overhead when balanced** | If all workers have similar session counts, the check is ~10 comparisons and returns immediately |

#### Configuration

```ini
[worker_group.myapp]
rebalance              = true    # Enable/disable (default: true)
rebalance_interval_ms  = 5000   # Check interval (default: 5000 ms)
rebalance_threshold_pct = 125   # Trigger at 1.25× average (default: 125)
rebalance_max_per_tick = 4      # Max migrations per check (default: 4)
```

All four parameters are tunable at runtime via the `SET` admin command:

```bash
keel-cli "SET rebalance_enabled = false"       # Disable
keel-cli "SET rebalance_interval_ms = 10000"   # Check every 10s
keel-cli "SET rebalance_threshold_pct = 150"   # More tolerant
keel-cli "SET rebalance_max_per_tick = 2"      # Smaller batch
```

#### Monitoring

```bash
keel-cli SHOW REBALANCE
```

Outputs a per-worker table with columns: `worker`, `sessions`, `migrations_sent`, `migrations_received`, `rebalance_checks`, `rebalance_moves`.

**Prometheus metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `keel_rebalance_checks_total` | counter | Number of timer-tick evaluations |
| `keel_rebalance_migrations_total` | counter | Successful rebalance-triggered migrations |
| `keel_rebalance_skipped_total` | counter | Evaluations that found no imbalance |

---

## Kernel-Level Mechanics

### SO_REUSEPORT Deep Dive

`SO_REUSEPORT` (Linux 3.9+) allows multiple sockets to bind to the same address:port. The kernel distributes incoming connections across all listening sockets.

```c
int create_listen_socket(const char* addr, uint16_t port, bool reuseport) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (reuseport) {
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    }

    // TCP optimizations
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    bind(fd, ...);
    listen(fd, backlog);

    return fd;
}
```

#### Kernel Distribution Algorithm

The kernel uses a hash of the connection 4-tuple to select a socket:

```
hash = hash_func(src_ip, src_port, dst_ip, dst_port)
socket_index = hash % num_listening_sockets
```

This provides:
- **Consistent distribution**: Same client always goes to same worker (connection affinity)
- **Lock-free**: No application-level synchronization needed
- **Scalable**: O(1) decision regardless of socket count

### Zero-Copy Splice

On Linux, KEEL uses `splice(2)` for kernel-space data forwarding between client and backend sockets:

```
Client FD → splice → Pipe → splice → Backend FD   (C2S: client queries)
Backend FD → splice → Pipe → splice → Client FD   (S2C: server results)
```

This avoids copying data into userspace for pass-through queries, reducing CPU usage and latency. Each worker maintains a pool of pre-allocated pipes for this purpose.

#### MSG_PEEK + Splice DataRow Bypass (`fast_network_path`)

When `fast_network_path = on` (default) and `result_cache = off` (default), the backend-to-client (S2C) path uses a **zero-copy splice bypass** for PostgreSQL DataRow (`'D'`) messages. Instead of copying every result row into userspace for inspection, the worker:

1. **Peeks** the 5-byte PostgreSQL message header via `recv(MSG_PEEK)` — no data is consumed.
2. If the message type byte is `'D'` (DataRow), **splices** the entire message directly from the backend socket to the client socket through the kernel pipe — zero userspace copy.
3. Repeats steps 1–2 in a tight loop (`worker_splice_bypass_loop()`) until a non-DataRow message is seen (e.g., `CommandComplete`, `ReadyForQuery`, `ErrorResponse`).
4. On a non-DataRow message, **exits** the splice loop and returns to the normal engine flow for proper protocol handling.

```
                    fast_network_path = on
                    ┌──────────────────────────────────────────┐
                    │         worker_splice_bypass_loop()       │
                    │                                          │
Backend FD ──recv(MSG_PEEK, 5)──► type == 'D'? ───yes───►     │
                    │              │                splice(2)   │
                    │              no             BE→pipe→FE    │
                    │              │                │           │
                    │              ▼                ▼           │
                    │         exit to engine    loop back       │
                    │         (normal path)     to peek         │
                    └──────────────────────────────────────────┘
```

**Safety guarantees:**
- Non-DataRow messages (errors, notices, parameter status changes) are always processed through the full engine flow.
- If `result_cache = on`, the bypass is disabled — all rows are copied into userspace for caching.
- The bypass respects `EWOULDBLOCK` / `EAGAIN` — if no data is available, the worker rearms the io_uring recv and resumes the splice loop when data arrives (`on_splice_bypass_recv()`).
- Transaction tracking and prepared statement virtualization remain active; splice bypass only affects the bulk DataRow forwarding within a single query result set.

**Build guard:** The splice bypass is compiled only when `KEEL_HAVE_SPLICE` is defined (Linux). On other platforms, the normal userspace copy path is used.

### accept4() System Call

We use `accept4()` for atomic flag setting:

```c
int client_fd = accept4(listen_fd,
                        (struct sockaddr*)&client_addr,
                        &addr_len,
                        SOCK_NONBLOCK | SOCK_CLOEXEC);
```

### Thread Pinning (CPU Affinity)

Optional CPU pinning for predictable performance:

```c
#ifdef __linux__
static void pin_thread_to_cpu(pthread_t thread, int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
}
#endif
```

---

## Codebase Guide

### File Structure

```
include/keel/
├── reactor.h              # Platform-agnostic reactor API
├── worker.h               # Worker thread, group, manager types
├── engine.h               # Session engine
├── engine_flow.h          # Data flow between client and backend
├── session.h              # Session lifecycle
├── pool.h                 # Connection pool
├── io_splice.h            # Zero-copy splice API
└── ...                    # 37 total headers

src/arch/
├── common/reactor_common.c     # Shared reactor logic
├── linux/
│   ├── reactor_iouring.c       # io_uring implementation
│   ├── reactor_epoll.c         # epoll fallback
│   └── io_splice.c             # splice(2) wrapper
└── macos/
    └── reactor_kqueue.c        # kqueue implementation

src/worker/
├── worker.c                    # Worker thread main loop, timers
├── backend_pool.c/.h           # Per-worker backend connection pool
└── backend_connect_async.c/.h  # Async connect + SCRAM state machine

src/engine/
├── engine.c                    # Session engine core
├── engine_flow.c               # Client↔backend data routing
└── backend_auth.c              # Backend SCRAM-SHA-256 auth helpers

src/session/
├── session.c                   # Session lifecycle
├── admission.c                 # Connection admission control
├── hardpin.c                   # Hard session pinning
├── residual.c                  # Residual data handling
├── route_cache.c               # Route decision cache
└── state_profile.c             # Session state profiling
```

### Key Subsystems

#### Reactor (`src/arch/`)

The reactor provides a unified interface over platform-specific I/O:

```c
keel_reactor_t* keel_reactor_create(config);
keel_reactor_accept(reactor, fd, callback, userdata);
keel_reactor_recv(reactor, fd, buf, len, callback, userdata);
keel_reactor_send(reactor, fd, buf, len, callback, userdata);
keel_reactor_connect(reactor, fd, addr, callback, userdata);
keel_reactor_timer(reactor, interval, callback, userdata);
keel_reactor_submit(reactor);    // Submit pending operations
keel_reactor_wait(reactor, timeout);  // Wait for completions
keel_reactor_process(reactor);   // Dispatch callbacks
keel_reactor_destroy(reactor);
```

#### Backend Pool (`src/worker/backend_pool.c`)

```c
// Borrow a PS-aware connection (exact stmt_set_hash match first,
// then clean connections only — avoids DISCARD ALL)
backend_conn_t* backend_pool_borrow_with_stmts(pool, required_stmt_hash);

// Borrow a connection (returns NULL if pool empty → session queued)
backend_conn_t* backend_pool_borrow(pool);

// Return after transaction
backend_pool_return(pool, conn);

// Queue session for when a connection becomes available
backend_pool_queue_wait(pool, session, callback, userdata);

// Async refill (called by timer, uses reactor)
backend_pool_refill_one(pool, reactor);
```

#### Async Connect (`src/worker/backend_connect_async.c`)

9-phase state machine for non-blocking backend establishment:
- TCP connect via reactor
- Full SCRAM-SHA-256 handshake
- Parameter status consumption
- ReadyForQuery receipt
- Connection added to pool on completion

---

## Configuration

### INI Configuration File

Worker groups are configured in `keel-pg.ini` (PostgreSQL) or `keel-my.ini` (MySQL):

```ini
[worker_group.myapp]
name = myapp
protocol = postgres
bind_addr = 0.0.0.0
bind_port = 7432
num_workers = 0              # 0 = auto (one per CPU core)
worker_strategy = per_core
distribution = reuseport
max_conns_per_worker = 1000
client_idle_timeout = 5m
client_connect_timeout = 10s
buffer_pool_size = 512MB
arena_size = 64KB
io_backend = auto            # auto, iouring, epoll, kqueue

# Pool behavior
pool_mode = transaction
min_pool_size = 10
max_pool_size = 50
idle_timeout = 5m

# Health checking
probe = postgres
probe_interval = 5s
probe_timeout = 3s
probe_retries = 3
failover_delay = 10s

# Server authentication
server_user = postgres
server_password = postgres
server_auth = scram-sha-256

# Zero-copy splice bypass (BE→FE DataRow forwarding)
fast_network_path = on       # MSG_PEEK + splice for DataRow frames (default: on)
result_cache = off           # Result caching — disables splice bypass when on (default: off)

[worker_group.myapp.servers]
primary  = host=primary.db.local port=5432 dbname=mydb role=RW weight=100
replica1 = host=replica1.db.local port=5432 dbname=mydb role=RO weight=100
replica2 = host=replica2.db.local port=5432 dbname=mydb role=RO weight=100
```

### Pluggable Probe System

KEEL uses a pluggable probe system for health checking and role detection:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Probe System Architecture                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         Probe Registry                                │  │
│  │                                                                       │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │  │
│  │  │ postgres │ │ patroni  │ │  mysql   │ │   tcp    │ │   exec   │     │  │
│  │  │   SQL    │ │   HTTP   │ │   SQL    │ │  connect │ │  script  │     │  │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘     │  │
│  │       │            │            │            │            │           │  │
│  └───────┼────────────┼────────────┼────────────┼────────────┼───────────┘  │
│          ▼            ▼            ▼            ▼            ▼              │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                      Backend Servers                               │    │
│   │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐           │    │
│   │  │  PRIMARY    │     │  REPLICA    │     │   DOWN      │           │    │
│   │  └─────────────┘     └─────────────┘     └─────────────┘           │    │
│   └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Built-in Probes

| Probe | Type | Health Check | Role Detection |
|-------|------|--------------|----------------|
| `postgres` | SQL | `SELECT 1` | `pg_is_in_recovery()` |
| `patroni` | HTTP | `GET /health` | `GET /cluster` or `GET /patroni` → JSON `role`/`members[]` |
| `mysql` | SQL | `SELECT 1` | `SELECT @@read_only` |
| `tcp` | TCP | Connect/disconnect | None (health only) |
| `exec` | Command | Exit code | Exit code |

#### Failover Scenarios

**Scenario 1: Primary Crashes**
```
Before:  primary (PRIMARY) ← writes
         replica1 (REPLICA) ← reads
         replica2 (REPLICA) ← reads

Event:   primary stops responding, replica1 promoted externally

After:   primary (DOWN) ← removed from rotation
         replica1 (PRIMARY) ← writes now go here
         replica2 (REPLICA) ← reads
```

**Scenario 2: Replica Failure**
```
Before:  primary (PRIMARY)
         replica1 (REPLICA) ← 50% reads
         replica2 (REPLICA) ← 50% reads

Event:   replica2 becomes unreachable

After:   primary (PRIMARY)
         replica1 (REPLICA) ← 100% reads
         replica2 (DOWN) ← removed, retried periodically
```

### Default Values

| Parameter | Default | Notes |
|-----------|---------|-------|
| `bind_addr` | `0.0.0.0` | All interfaces |
| `bind_port` | `6432` | PostgreSQL pooler convention |
| `strategy` | `per_core` | One worker per CPU |
| `num_workers` | `0` | Auto-detect from CPU count |
| `distribution` | `reuseport` | Kernel distribution |
| `max_conns_per_worker` | `1000` | Per-worker limit |
| `pool_mode` | `transaction` | Transaction-level pooling |
| `min_pool_size` | `10` | Min backend connections |
| `max_pool_size` | `50` | Max backend connections |
| `arena_size` | `64 KB` | Per-worker arena |
| `buffer_pool_size` | `512 MB` | Per-worker buffers |

### Size & Time Units

| Size Unit | Multiplier | Time Unit | Value |
|-----------|------------|-----------|-------|
| B | 1 | s | seconds |
| KB | 1,024 | m | minutes |
| MB | 1,048,576 | h | hours |
| GB | 1,073,741,824 | | |

---

## Performance Characteristics

### Scalability Profile

| Metric | Single Worker | 4 Workers | 8 Workers |
|--------|---------------|-----------|-----------|
| Max connections | 1,000 | 4,000 | 8,000 |
| Backend pool | 10–50 | 40–200 | 80–400 |
| Context switches | Low | Low | Low |

### Memory Usage Per Worker

| Component | Size | Notes |
|-----------|------|-------|
| Arena | 64 KB | Request-scoped allocations |
| Session Slab | ~200 bytes/session | Fixed-size session objects |
| Recv Context Pool | ~400 B/context metadata × 256 initial | Pre-allocated metadata shells; heap I/O buffers grow on demand |
| Waiter Pool | 24 bytes/waiter × 256 initial | Pre-allocated pool wait entries |
| Backend Pool | ~1 KB/connection | Backend connection state |
| Reactor | ~10 KB | io_uring SQ/CQ rings |
| **Total base** | **~34 MB** | Dominated by recv context pool |

### Latency Expectations

| Operation | Expected Latency |
|-----------|------------------|
| accept() | < 1 μs |
| Reactor dispatch | < 10 μs |
| State transition | < 1 μs |
| Arena allocation | < 100 ns |
| Pool borrow (hit) | < 1 μs |
| Async connect | ~1–5 ms (network RTT) |

---

## Session-Context Preservation

A critical design requirement for transaction pooling is preserving session state across
connection reassignment.  KEEL solves this with a **state profile + diff-sync** architecture
that is integrated directly into the multiplexing layer.

> **Full documentation:** [SESSION_CONTEXT.md](SESSION_CONTEXT.md)

### How It Works

Each session owns a `state_profile_t` — a sorted array of up to 64 key-value pairs representing
the session's `SET` parameters.  When a backend connection is returned to the pool after a
transaction, the backend retains its state and the state hash is stamped on the pool entry.

On the next borrow:

1. **Hash match (fast path):** If the pool has a backend with the same state hash, borrow it
   directly — zero sync needed.
2. **Diff sync:** If the backend has different state, generate minimal SQL
   (`SET k1='v1'; RESET k2;`) using a two-pointer merge diff algorithm, send non-blocking,
   and drain the response.

```
Session:   search_path=myschema, timeout=5000
Backend:   search_path=public, work_mem=128MB

Diff:  SET search_path = 'myschema'; SET timeout = '5000'; RESET work_mem;
       ────────────────────────────────────────────────────────────────────
       Sent non-blocking (MSG_DONTWAIT), 10-iteration drain, never stalls reactor
```

### Integration with Worker Architecture

State sync operates entirely within the per-worker hot path — no cross-worker coordination
is needed.  Each worker's backend pool independently tracks state hashes on its connections.
When a session migrates between workers (via SCM_RIGHTS), the session's state hash is
transferred in the migration message; the new worker syncs state on the next borrow.

### What's Preserved

| Feature | Status | Notes |
|---------|:------:|-------|
| `SET` parameters | ✅ | Tracked in state profile, synced automatically |
| Prepared statements | ✅ | Replayed via PS virtualization engine |
| `LISTEN` subscriptions | ⚠️ | Triggers session pinning (automatic) |
| Temporary tables | ⚠️ | Transaction-scoped by default; persistent pins session |
| Advisory locks (txn-level) | ✅ | Released at COMMIT, no pinning needed |
| Advisory locks (session-level) | ⚠️ | Triggers session pinning (automatic) |

### Performance

In practice, 70-90% of borrows hit the hash-match fast path (zero sync).  When sync is needed,
it adds 1-3 SET commands (~50-200 bytes SQL), processed in <100µs by PostgreSQL.

---

## Runtime Mode Tiers

The multiplexing engine supports four runtime tiers that control which per-query features are
active.  This allows deployments that only need basic pooling to avoid the cost of hooks,
routing, transaction tracking, and LSN capture.

> **Full documentation:** [RUNTIME_MODES.md](RUNTIME_MODES.md)

| Tier | Features Active |
|------|----------------|
| `proxy` | Frame extraction + forward only. Session pinned to one backend. |
| `pool` | + Connection pooling + PS replay. Basic stats. |
| `smart` | + R/W routing, sticky-primary, state sync, query logging, full stats. |
| `full` | + Hooks (×4), transaction tracking, LSN capture. |

Each gate is a single `cmp + jcc` instruction on `sf->mode` — zero overhead for disabled
features.

---

## Known Limitations & Challenges

### 1. Connection Migration (Implemented)

Connection migration between workers is fully implemented. Idle sessions
(`KEEL_SESSION_READY`, not in transaction, no pending backend, no residual)
can be handed between workers via `keel_worker_migrate_session()`.

See the [Connection Migration](#connection-migration) section for architecture details.

**Remaining limitation**: Migration is opt-in — callers (e.g., a load-balance probe timer)
must invoke `keel_worker_migrate_session()`. Automatic background rebalancing is planned.

### 2. No Work Stealing

Workers are completely independent. Idle workers cannot help busy workers.

**Future**: Implement work-stealing queue for pending connections.

### 3. Limited Statistics Aggregation

Statistics are per-worker with manual aggregation. No real-time global view.

**Future**: Atomic counters for real-time aggregates, Prometheus metrics endpoint.

### 4. Graceful Shutdown — Resolved

Graceful drain/shutdown is now fully implemented: lifecycle state machine
(CREATED → ACTIVE → DRAINING → STOPPING → STOPPED), configurable drain timeout,
CID-aware force-close that protects commit-in-doubt sessions, PostgreSQL FATAL 57P03
error on drain rejection.  See `test_drain_shutdown` (10 tests, 57 assertions).

---

## Improvement Roadmap

### Completed
- [x] Reactor-driven worker loop (io_uring / kqueue / epoll)
- [x] SO_REUSEPORT distribution
- [x] Per-worker backend connection pools
- [x] Non-blocking backend connect + SCRAM-SHA-256
- [x] Transaction pooling mode
- [x] Zero-copy splice (Linux)
- [x] Timer-driven pool refill and prune
- [x] Read/write splitting with replica pools
- [x] Wait queue for pool exhaustion
- [x] Pluggable probe system
- [x] Single-shot accept with fair worker distribution
- [x] Pool allocators for recv contexts and pool waiters
- [x] Non-blocking dirty connection cleanup (MSG_DONTWAIT)
- [x] Non-blocking backend state sync (MSG_DONTWAIT)
- [x] Auto-raise RLIMIT_NOFILE at startup
- [x] Crash signal handlers (SIGSEGV, SIGABRT, SIGBUS)
- [x] Multishot accept cancellation detection and rearm
- [x] io_uring provided-buffer rings (IORING_REGISTER_PBUF_RING, Linux 5.19+)
- [x] Prepared statement virtualization (PS replay — session continuity across pool reassignment)
- [x] Prepared statement pooling modes — `virtualize` (default), `pinning`, `tracking`, `anonymous` (selectable per worker-group via `prepared_statement` INI key)
- [x] Transaction tracking — atomic XID capture on every COMMIT; commit-in-doubt resolution via `txid_status()` on fresh primary connection (`transaction_tracking = on`)
- [x] Patroni REST API cluster discovery — `GET /cluster` (all members) with `/patroni` fallback; pure-C HTTP/1.0 client, no external dependencies
- [x] MySQL selective session cleanup — `SET @@session.x = value` diff via `generate_sync_sql()` on backend reuse (COM_QUERY)
- [x] MSG_PEEK + splice DataRow bypass — zero-copy BE→FE forwarding for DataRow (`'D'`) messages via `worker_splice_bypass_loop()`; configurable with `fast_network_path` / `result_cache` INI keys

### Planned
- [ ] PostgreSQL WAL LSN consistency tokens — `capture_consistency_token` + `replica_reached_token` vtable implementation
- [ ] MySQL GTID consistency tokens — `SELECT @@gtid_executed` capture + `WAIT_FOR_EXECUTED_GTID_SET()` replica check
- [ ] io_uring multishot accept (revisit when kernel supports per-ring affinity)
- [ ] io_uring multishot recv (IORING_OP_RECV_MULTISHOT, Linux 6.0+)
- [x] Connection migration (idle sessions transferred via SCM_RIGHTS + SPSC ring)
- [ ] Automatic background rebalancing (periodic migration trigger)
- [ ] Work-stealing between workers
- [ ] eBPF-based socket selection
- [ ] NUMA-aware worker placement
- [ ] Prometheus metrics endpoint
- [ ] Health check HTTP endpoint
- [ ] Hot configuration reload
- [ ] Connection draining on shutdown

---

## Best Practices

### 1. Worker Count Selection

```ini
# Match CPU cores (default, recommended)
worker_strategy = per_core
num_workers = 0

# Fixed count for I/O-heavy workloads (over-provision)
worker_strategy = fixed
num_workers = 8
```

### 2. Pool Sizing

```ini
# Conservative: fewer backend connections, higher sharing
min_pool_size = 5
max_pool_size = 20
pool_mode = transaction

# Aggressive: more connections, lower latency
min_pool_size = 20
max_pool_size = 100
```

### 3. Distribution Strategy Selection

| Scenario | Recommended |
|----------|-------------|
| Linux 3.9+ | `reuseport` |
| Older Linux | `accept_lock` |
| macOS/BSD | `accept_lock` |
| Debugging | `round_robin` |

### 4. Monitoring

```bash
# Monitor open file descriptors
ls -la /proc/$(pgrep keel)/fd | wc -l

# Check listening ports
ss -tlnp | grep keel

# Watch connection counts
watch -n1 'ss -s'
```

---

## Troubleshooting

### Workers Not Accepting Connections

**Checks**:
1. Verify port is listening: `ss -tlnp | grep 7432`
2. Check firewall rules
3. Verify SO_REUSEPORT support: `uname -r` (need 3.9+)
4. Check KEEL logs for reactor creation errors

### High CPU Usage

**Likely Causes**:
1. Too many reactor wakeups with no work
2. Aggressive refill timer with many failed connects
3. Check `io_backend` setting — `auto` should pick the best

### Memory Growth

**Likely Causes**:
1. Sessions not properly closed (check for FD leaks)
2. Backend connections accumulating beyond max_pool_size
3. Arena not being reset

**Detection**:
```bash
# Monitor RSS
ps -o rss,vsz,pid -p $(pgrep keel)

# Check open FDs
ls /proc/$(pgrep keel)/fd | wc -l
```

### Pool Exhaustion

**Symptoms**: Queries hang, sessions queued.

**Solutions**:
1. Increase `max_pool_size`
2. Ensure `pool_mode = transaction` (not `session`)
3. Reduce `client_idle_timeout` to free idle sessions faster
4. Check backend connectivity (failed async connects won't refill the pool)

### Uneven Load Distribution

**Causes**: Client affinity from REUSEPORT hash, long-lived connections.

**Solutions**:
1. Use more workers
2. Shorter client idle timeouts force redistribution
3. Check per-worker stats in KEEL logs

---

## Dependencies

### Required

| Library | Purpose |
|---------|---------|
| pthread | Threading |
| OpenSSL | SCRAM-SHA-256 authentication |

### Recommended

| Library | Purpose |
|---------|---------|
| liburing 2.0+ | io_uring reactor on Linux |

### Build-Time

| Tool | Version | Purpose |
|------|---------|---------|
| CMake | 3.25+ | Build system |
| GCC | 13+ | C23 compiler |
| Clang | 17+ | Alternative compiler |

---

## References

- [Linux SO_REUSEPORT](https://lwn.net/Articles/542629/) — LWN article on SO_REUSEPORT
- [io_uring](https://kernel.dk/io_uring.pdf) — io_uring paper by Jens Axboe
- [The C10K Problem](http://www.kegel.com/c10k.html) — Classic scalability reference
- [Nginx Architecture](https://www.nginx.com/blog/inside-nginx-how-we-designed-for-performance-scale/) — Inspiration for worker model

---

*Document Version: 3.0*
*Last Updated: March 2026*
*Author: KEEL Development Team*
