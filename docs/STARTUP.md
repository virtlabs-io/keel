# KEEL Startup Flow

This document describes the complete startup sequence of the KEEL database proxy — from the moment the binary is invoked to the point where worker threads are accepting connections.

## Table of Contents

- [Overview](#overview)
- [Entry Point](#entry-point)
- [Phase 1: Process Initialization](#phase-1-process-initialization)
- [Phase 2: Configuration Loading](#phase-2-configuration-loading)
  - [CLI Options](#cli-options)
  - [INI Configuration Parser](#ini-configuration-parser)
  - [Worker Groups](#worker-groups)
  - [Server Parsing](#server-parsing)
  - [Logging Configuration](#logging-configuration)
  - [Stats, Admin, Prometheus](#stats-admin-prometheus)
- [Phase 3: Log Plugin and Query Logger](#phase-3-log-plugin-and-query-logger)
- [Phase 4: Listen Socket Creation](#phase-4-listen-socket-creation)
- [Phase 5: Engine Creation](#phase-5-engine-creation)
- [Phase 6: Worker Initialization](#phase-6-worker-initialization)
  - [Reactor Creation](#reactor-creation)
  - [Backend Connection Pools](#backend-connection-pools)
  - [Session Slab](#session-slab)
  - [Recv Context Pool](#recv-context-pool)
  - [Pipe Pool and Timer Wheel](#pipe-pool-and-timer-wheel)
- [Phase 7: Engine Start](#phase-7-engine-start)
- [Phase 8: Main Loop](#phase-8-main-loop)
- [Memory Architecture](#memory-architecture)
  - [System Allocator (mem.c)](#system-allocator)
  - [Arena Allocator (arena.c)](#arena-allocator)
  - [Pool Allocator (pool.c)](#pool-allocator)
  - [Slab Allocator (slab.c)](#slab-allocator)
- [Data Structures Reference](#data-structures-reference)
- [Startup Sequence Diagram](#startup-sequence-diagram)

---

## Overview

KEEL follows a two-phase startup model:

1. **Phase 1 (main thread):** Parse CLI, load config, create listen sockets, create engines, configure logging — all sequential, deterministic output.
2. **Phase 2 (parallel):** Start worker threads that initialize their own reactors and begin accepting connections.

This separation guarantees the startup banner prints cleanly before any worker log output interleaves.

---

## Entry Point

**File:** `src/main/main.c`  
**Function:** `main(int argc, char** argv)`

```
main()
├── setbuf(stdout/stderr, NULL)         // Disable buffering
├── raise RLIMIT_NOFILE to hard max     // Up to 1M file descriptors
├── parse_options()                     // CLI arguments
├── keel_config_load()                   // INI file parsing
├── [for each worker_group]:
│   ├── create_listen_socket()          // SO_REUSEPORT + backlog 4096
│   ├── keel_engine_create()             // Engine struct + stats + reactor detect
│   └── keel_engine_start()              // Worker pool init + thread spawn
├── keel_admin_start()                   // Admin console + Prometheus
├── keel_probe_manager_start()           // Health check threads
└── signal loop (sigwait)               // Until SIGINT/SIGTERM
```

---

## Phase 1: Process Initialization

### Buffer and Resource Limits

```c
setbuf(stdout, NULL);   // Line-buffered output for containers
setbuf(stderr, NULL);

struct rlimit rl;
getrlimit(RLIMIT_NOFILE, &rl);
rl.rlim_cur = min(rl.rlim_max, 1048576);  // Raise to hard max, cap at 1M
setrlimit(RLIMIT_NOFILE, &rl);
```

A database proxy may hold thousands of client + backend connections simultaneously. The default soft limit of 1024 is far too low.

---

## Phase 2: Configuration Loading

### CLI Options

**Struct:** `options_t` (main.c)

| Flag | Field | Default | Description |
|------|-------|---------|-------------|
| `-c` | `config_file` | `NULL` | Path to INI config file |
| `-l` | `listen_addr` | `0.0.0.0` | Bind address |
| `-p` | `listen_port` | `6432` | Bind port |
| `-H` | `backend_host` | `127.0.0.1` | Default backend host |
| `-P` | `backend_port` | `5432` | Default backend port |
| `-w` | `num_workers` | `0` (auto) | Worker threads (0 = one per CPU) |
| `-v` | `log_level` | `2` (INFO) | Verbosity (repeatable) |
| `-d` | `daemonize` | `false` | Double-fork daemon mode |

CLI options override the first worker group's config values.

### INI Configuration Parser

**File:** `src/core/config.c` (410 lines)  
**Key struct:** `keel_config_t`

```
keel_config_t
├── path               // Config file path
└── sections ──► config_section_t (linked list)
                 ├── name          // e.g., "worker_group.pg"
                 ├── next
                 └── entries ──► config_entry_t (linked list)
                                 ├── key
                                 ├── value
                                 └── next
```

**Parsing flow:**
1. `keel_config_load(path)` — Opens file, reads line by line (`MAX_LINE_LEN = 4096`)
2. Lines starting with `[` define new sections
3. Lines with `key = value` create entries under the current section
4. `#` and `;` are comment characters
5. Values can be quoted (single or double) — quotes are stripped
6. Whitespace is trimmed from both keys and values

**Access API:**
- `keel_config_get_string(config, section, key, default)`
- `keel_config_get_int(config, section, key, default)`
- `keel_config_get_bool(config, section, key, default)`
- `keel_config_has_section(config, section)`
- `keel_config_iter_sections_prefix(config, prefix, callback, ctx)`
- `keel_config_iter_keys(config, section, callback, ctx)`

### Worker Groups

KEEL supports up to `KEEL_MAX_WORKER_GROUPS` (8) independent worker groups. Each group has its own listen socket, protocol, backend pool, and set of worker threads.

**Struct:** `worker_group_t` (main.c)

```
worker_group_t
├── section[256]           // INI section name, e.g., "worker_group.pg"
├── servers_section[280]   // "<section>.servers"
├── name                   // Display name
├── listen_addr/port       // Bind configuration
├── default_protocol       // "postgres" or "mysql"
├── num_workers            // 0 = auto
├── pin_workers            // CPU affinity (default: true)
├── pool_min_size          // Min backend connections per server (default: 2)
├── pool_max_size          // Max backend connections per server (default: 100)
├── session_pool_size      // Max frontend sessions per worker (default: 1024)
├── buffer_size            // Recv buffer size (default: 65536)
├── idle_timeout_ms        // Backend idle timeout (default: 300000 = 5min)
├── connect_timeout_ms     // Backend connect timeout (default: 10000 = 10s)
├── prepared_statement     // PS pooling mode: virtualize|pinning|tracking|anonymous (default: virtualize)
├── transaction_tracking   // Replication tracking via txid_current(): on|off (default: off)
├── backend_host/port/user/password/database
├── server_pool            // keel_server_pool_t with primary + replicas
├── probe_cfg              // Health probe configuration
├── listen_fd              // Runtime: listen socket FD
├── engine                 // Runtime: keel_engine_t*
└── probe_mgr              // Runtime: keel_probe_manager_t*
```

**Discovery:** `keel_config_iter_sections_prefix(config, "worker_group.", ...)` iterates all sections matching the prefix. Subsections like `worker_group.pg.servers` are filtered out (they contain a `.` after the group name).

If no worker groups are found in the config (or no config file is used), a single implicit "default" group is created from CLI arguments / global defaults.

### Prepared Statement Mode (`prepared_statement`)

Controls how named prepared statements are handled in transaction-pooling mode.

| Value | Description |
|-------|-------------|
| `virtualize` | *(default)* PS metadata cached in proxy; replayed to new backends on demand |
| `pinning` | Backend hard-pinned to session for lifetime of any active named PS |
| `tracking` | Like `virtualize` but also intercepts Simple Query `PREPARE name AS …` syntax |
| `anonymous` | Named PS intercepted; each Bind converted to a one-shot anonymous statement |

```ini
[worker_group.pg]
prepared_statement = virtualize   # or: pinning, tracking, anonymous
```

See [Prepared Statement Pooling Modes](QUERY_FLOW.md#prepared-statement-pooling-modes) for a full comparison.

### Transaction Tracking (`transaction_tracking`)

When `on`, every `COMMIT` is rewritten to a compound statement that atomically
captures the PostgreSQL transaction ID (XID):

```sql
SELECT txid_current() AS _keel_txid; COMMIT;
```

If the backend connection dies after the compound statement is sent but before
`ReadyForQuery` is received, the proxy borrows a fresh primary connection and
calls `SELECT txid_status(XID)` to determine the outcome, then synthesises the
appropriate response for the client (_committed_, _aborted_, or _unknown_).

```ini
[worker_group.pg]
transaction_tracking = on   # default: off
```

> **Note:** `transaction_tracking` is PostgreSQL-only.  It has no effect when
> `default_protocol = mysql`.

### Server Parsing

Each worker group can have a `[worker_group.X.servers]` section defining backend servers:

```ini
[worker_group.pg.servers]
primary = host=10.0.0.1 port=5432 role=RW user=postgres dbname=mydb
replica1 = host=10.0.0.2 port=5432 role=RO
replica2 = host=10.0.0.3 port=5432 role=RO weight=50
```

Each server definition is a space-delimited key=value string parsed into `keel_backend_server_t`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | `const char*` | `127.0.0.1` | Server hostname/IP |
| `port` | `uint16_t` | `5432` | Server port |
| `user` | `const char*` | `postgres` | Auth user |
| `password` | `const char*` | `NULL` | Auth password |
| `database` | `const char*` | `postgres` | Default database |
| `role` | `enum` | `AUTO` | `primary` or `replica` |
| `weight` | `uint32_t` | `100` | Load balancing weight |
| `healthy` | `bool` | `true` | Updated by health probes |

### Logging Configuration

Parsed from `[logging]` section:

| Key | Default | Description |
|-----|---------|-------------|
| `plugin` | `stdout` | `stdout`, `file`, `syslog`, or `.so` path |
| `plugin_path` | `NULL` | Explicit shared library path |
| `log_file` | `NULL` | Path for file plugin |
| `log_level` | `info` | `error`, `warn`, `info`, `debug`, `all` |
| `query_log_mode` | `none` | `none`, `all`, `read`, `write` |
| `log_timestamps` | `true` | Include timestamps |
| `log_source` | `true` | Log client IP/port |
| `log_dest` | `true` | Log backend IP/port |
| `log_username` | `true` | Log authenticated user |
| `log_database` | `true` | Log target database |
| `log_query_tree` | `false` | Log parsed query tree |
| `max_query_len` | `0` (unlimited) | Truncate logged queries |
| `use_colors` | `true` | ANSI color output |

### Stats, Admin, Prometheus

**Stats** (`[stats]` section):
- `level`: `off`, `basic`, `extended`, `system`, `full` — 6 levels controlling detail
- `log_interval_ms`: Periodic automatic stats dump (0 = manual only via `SIGUSR1`)

**Admin** (`[admin]` section): PgBouncer-compatible admin console
- `enabled`, `listen_addr`, `listen_port` (default: 6433), `users`

**Prometheus** (`[prometheus]` section): HTTP `/metrics` endpoint
- `enabled`, `listen_addr`, `port` (default: 9101), `path`

### Security Configuration

Parsed from the `[security]` section.  Applied between configuration loading and worker thread
creation — privilege drop executes after listen socket binding (which may require root), and
seccomp is installed process-wide before the first `accept()`.

| Key | Default | Description |
|-----|---------|-------------|
| `privilege_drop` | `false` | Drop privileges after startup (bind, FD raise) |
| `run_user` | `nobody` | Target Unix user after privilege drop |
| `run_group` | `nogroup` | Target Unix group after privilege drop |
| `require_privilege_drop` | `false` | Fail-closed: abort startup if privilege drop fails |
| `seccomp` | `off` | Seccomp BPF filter: `off`, `baseline` (denylist), `strict` (allowlist) |
| `require_seccomp` | `false` | Fail-closed: abort startup if seccomp cannot be applied |
| `no_new_privs` | `true` | Set `PR_SET_NO_NEW_PRIVS` before seccomp (prevents privilege re-escalation) |

**Startup ordering:**
1. Parse `[security]` keys from INI config
2. Create listen sockets (may require `CAP_NET_BIND_SERVICE` or root for ports < 1024)
3. Apply privilege drop (`setgid()` → `setuid()`) if `privilege_drop = true`
4. Apply seccomp filter if `seccomp != off`
5. Start worker threads (now running under restricted privileges and syscall filter)

---

## Phase 3: Log Plugin and Query Logger

**Files:** `src/log/log_plugin.c`, `src/log/query_log.c`

The logging system uses a plugin architecture with a vtable interface:

```
keel_log_plugin_t
├── name                  // "stdout", "file", "syslog", or custom
├── open(plugin, config)  // Initialize
├── write(plugin, entry)  // Write log entry
├── flush(plugin)         // Flush buffers
├── close(plugin)         // Cleanup
└── destroy(plugin)       // Free resources
```

**Built-in plugins:**
- `keel_log_plugin_stdout_create()` — ANSI-colored console output
- `keel_log_plugin_file_create()` — File-based logging
- `keel_log_plugin_syslog_create()` — syslog integration

**Custom plugins:** Any shared library (`.so`) implementing the vtable can be loaded via `keel_log_plugin_load(path)` using `dlopen()`.

**Query Logger** (`keel_query_log_t`):
- Initialized with `keel_query_log_init(&g_query_log, &config, plugin)`
- Registered globally via `keel_query_log_set_global(&g_query_log)` so worker threads can access it
- Modes: `none` (off), `all` (every query), `read` (SELECT/SHOW only), `write` (INSERT/UPDATE/DELETE only)

---

## Phase 4: Listen Socket Creation

**Function:** `create_listen_socket(addr, port)` (main.c)

Each worker group gets its own listen socket:

```c
int fd = socket(AF_INET, SOCK_STREAM, 0);

setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &1, ...);   // Quick restart
setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &1, ...);    // Multi-thread accept
setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &1, ...);     // Disable Nagle

fcntl(fd, F_SETFL, O_NONBLOCK);  // Non-blocking

bind(fd, addr:port);
listen(fd, 4096);                 // Large backlog for burst handling
```

**`SO_REUSEPORT`** is critical: it allows every worker thread to create its own accept on the same port. The kernel distributes incoming connections across workers, avoiding the thundering herd problem.

---

## Phase 5: Engine Creation

**File:** `src/engine/engine.c`  
**Function:** `keel_engine_create(const keel_engine_config_t* config)`

```
keel_engine_t
├── config                    // Resolved engine configuration
├── num_workers               // CPU count (auto) or configured
├── reactor_type              // io_uring / kqueue / epoll (auto-detected)
├── default_protocol          // Protocol vtable pointer
├── stats_collector           // Per-worker padded stats contexts
├── listen_fd                 // Shared listen socket
├── worker_pool               // Array of keel_worker_t
├── running / stopping        // Lifecycle flags
├── periodic_cb               // Stats dump callback
└── pool_initialized          // Worker pool created flag
```

**Steps:**
1. Allocate engine structure (`keel_calloc`)
2. Resolve worker count: if `num_workers == 0`, call `get_cpu_count()` (reads `/proc/cpuinfo` or `sysconf(_SC_NPROCESSORS_ONLN)`)
3. Create stats collector: `keel_stats_collector_create(level, num_workers)` — allocates cache-line-padded per-worker contexts
4. Detect reactor: `detect_best_reactor()` — prefers io_uring on Linux 5.6+, falls back to epoll; uses kqueue on macOS
5. Resolve protocol vtable: `keel_protocol_get(protocol_name)` — returns the registered protocol vtable

---

## Phase 6: Worker Initialization

**File:** `src/worker/worker.c`  
**Function:** `keel_worker_init(worker, engine, id, listen_fd)`

Each worker is fully self-contained with thread-local data structures. There is no sharing between workers on the hot path.

### Reactor Creation

```c
size_t session_cap = cfg->session_pool_size;  // Default: 1024
uint32_t uring_depth = next_power_of_2(max(session_cap, 4096));  // 4096..65536

keel_reactor_config_t rconfig = KEEL_REACTOR_CONFIG_DEFAULT;
rconfig.queue_depth = uring_depth;
worker->reactor = keel_reactor_create(&rconfig);
```

The io_uring queue depth is scaled to the expected connection count. Each active session holds ~2 SQEs (recv + potential send). The init path halves depth automatically if the kernel rejects the requested size.

**Reactor vtable** (15+ functions):
- `accept`, `recv`, `send`, `close`, `splice`
- `connect`, `timeout`, `cancel`
- `poll_add`, `poll_remove`
- `submit`, `wait`, `process`

### Backend Connection Pools

Per-server pool sizes are divided across workers:

```
Config: min=20, max=60, 4 workers
→ Per-worker per-server: min=5, max=15
→ Total to primary: 4 × 15 = 60 = max_pool_size ✓
```

**Primary pool:**
```c
backend_pool_config_t cfg = {
    .min_connections  = per_worker_min,
    .max_connections  = per_worker_max,
    .max_waiting      = max(per_worker_max * 100, 10000),
    .idle_timeout_ms  = config->pool_idle_timeout_ms,
    .wait_timeout_ms  = config->connect_timeout_ms,
    ...
};
worker->primary_pool = backend_pool_create(&cfg);
backend_pool_set_wait_callback(worker->primary_pool, pool_wait_resume_cb);
```

**Replica pools:** One `backend_pool_t*` per configured replica, stored in `worker->replica_pools[]`.

**Async warmup:** After pool creation, `backend_pool_async_warmup(pool)` fires `min_connections` concurrent io_uring connect operations to pre-establish backend connections before any client arrives. This happens in the reactor's event loop — no blocking `poll()` calls.

### Session Slab

```c
keel_session_slab_init(&worker->sessions, slab_size);
```

Pre-allocates `slab_size` (default: 1024) session slots. Each session is allocated from this slab in O(1) when a client connects, and returned to the slab when the session closes.

### Recv Context Pool

```c
keel_pool_config_t rcfg = {
    .object_size   = sizeof(recv_context_t),  // ~400 B (metadata only, no embedded buffers)
    .object_align  = 64,                      // Cache-line aligned
    .initial_count = 256,                     // ~100 KB upfront (vs ~33 MB before redesign)
    .max_count     = slab_size,               // Grows on demand
    .zero_on_alloc = true,
};
worker->recv_ctx_pool = keel_pool_create(&rcfg);
```

The recv context pool provides O(1) allocation of session metadata without calling `malloc()` on the hot path.

**Memory architecture (after redesign):** I/O buffers are **heap-backed pointers**, not embedded arrays. `recv_context_t` is ~400 B (down from ~131 KB). Buffers are allocated lazily:

| Buffer | Allocated When | Size | Function |
|--------|---------------|------|----------|
| `fe_buf` | Session setup (accept) | 64 KB | `recv_ctx_alloc_fe()` |
| `be_buf` | First backend borrow | 64 KB | `recv_ctx_ensure_be()` |
| `tls_hs_buf` | SSLRequest received | 64 KB | `recv_ctx_ensure_tls_hs()` |

All buffers are freed together via `recv_ctx_free_bufs()` on session teardown. This reduces idle pool memory by ~300× (1024 slots × 400 B = 400 KB vs 1024 × 131 KB = 131 MB).

### Pipe Pool and Timer Wheel

**Pipe pool** (Linux splice only):
```c
keel_pipe_pool_init(&worker->pipes, pool_sz);
```
Pre-creates kernel pipe pairs for zero-copy `splice()` data transfer.

**Timer wheel** (10ms resolution):
```c
keel_timer_wheel_init(&worker->timers, 10);
```
Drives idle timeouts, connect timeouts, and pool pruning without per-timer syscalls.

---

## Phase 7: Engine Start

**Function:** `keel_engine_start(engine, listen_fd)`

1. Set listen socket to non-blocking + `SO_REUSEADDR`
2. `keel_worker_pool_init()` — allocates array of `keel_worker_t`, calls `keel_worker_init()` for each
3. `keel_worker_pool_start()` — calls `keel_worker_start()` for each worker:
   - Queue initial `keel_reactor_accept()` (single-shot per worker)
   - `pthread_create(&worker->thread, NULL, worker_thread_func, worker)`

After `keel_engine_start()` returns, all worker threads are running and competing for incoming connections via the shared `SO_REUSEPORT` socket.

**Probe managers** are started separately after all engines:
```c
keel_probe_manager_start(wg->probe_mgr);
```
Each probe manager runs its own thread, sending periodic health checks to backend servers.

---

## Phase 8: Main Loop

The main thread blocks on signals using `sigtimedwait()`:

```c
while (!g_should_stop) {
    if (interval_ms > 0) {
        sig = sigtimedwait(&sigset, NULL, &ts);
        if (sig < 0 && errno == EAGAIN)
            stats_dump();          // Periodic timer fired
    } else {
        sigwait(&sigset, &sig);    // Wait indefinitely
    }

    if (sig == SIGUSR1) stats_dump();
    if (sig == SIGINT || SIGTERM) g_should_stop = 1;
}
```

**Signals handled:**
| Signal | Action |
|--------|--------|
| `SIGINT` / `SIGTERM` | Graceful shutdown |
| `SIGUSR1` | Dump stats + active pool allocation blocks |
| `SIGPIPE` | Ignored (SIG_IGN) |
| `SIGSEGV` / `SIGABRT` / `SIGBUS` | Crash handler — writes signal name to stderr, re-raises for core dump |

**Shutdown sequence:**
1. `keel_engine_stop()` — signals all workers to stop
2. Print final statistics
3. `keel_admin_stop()` — stop admin console
4. `keel_probe_manager_destroy()` — stop health probes
5. `keel_engine_destroy()` — join worker threads, free all resources
6. `keel_query_log_shutdown()` — flush and close query logger
7. Log plugin: `flush() → close() → destroy()`

---

## Memory Architecture

KEEL provides four allocator tiers, each optimized for different allocation patterns:

### System Allocator

**File:** `src/mem/mem.c` (804 lines)

Wraps `malloc()`/`free()` with a hidden header for debugging and tracking:

```
+───────────────────+────────────────────+
│ keel_alloc_header │ user data          │
│ size, magic       │ (requested bytes)  │
│ [file, line]      │                    │
+───────────────────+────────────────────+
```

- **Magic number:** `0xDBA1100C` — detects double-free, buffer underflow, uninitialized pointers
- **Fill patterns:** `0xCD` (alloc), `0xDD` (free), `0xFD` (guard)
- **Thread safety:** Atomic counters for stats; spinlock for debug allocation list
- **Debug mode** (`KEEL_MEM_DEBUG`): Tracks file:line of every allocation for leak detection
- **API:** `keel_malloc()`, `keel_calloc()`, `keel_realloc()`, `keel_free()`, `keel_strdup()`

### Arena Allocator

**File:** `src/mem/arena.c` (533 lines)

Fast bump allocator for temporary allocations sharing a lifetime:

```
Arena
  └── Block 1 [next] → Block 2 [next] → Block 3 [next] → NULL
       └── data[]         └── data[]         └── data[]
           ^used               ^used               ^used
```

- **Block size:** 64KB default
- **Alignment:** 16 bytes (SSE/AVX compatible)
- **Allocation:** O(1) — bump pointer forward
- **Individual free:** Not supported
- **Reset/Destroy:** O(n) where n = number of blocks — frees all at once
- **Use cases:** SQL parser scratch space, per-request temporaries
- **API:** `keel_arena_create()`, `keel_arena_alloc()`, `keel_arena_strdup()`, `keel_arena_reset()`, `keel_arena_destroy()`

### Pool Allocator

**File:** `src/mem/pool.c` (396 lines)

Fixed-size object pool with O(1) free-list allocation:

```
keel_pool_t
├── free_list ────────────► [obj_A] → [obj_C] → [obj_F] → NULL
└── chunks → [chunk1] → [chunk2] → NULL
              └── data[]   └── data[]
```

- **Allocation:** O(1) — pop from free list
- **Deallocation:** O(1) — push to free list
- **Growth:** Allocates new chunks on demand, never shrinks
- **Minimum object size:** `sizeof(void*)` (pointer used for free-list linking)
- **Use cases:** recv_context_t pool (~400 B metadata slots), connection objects, AST nodes
- **API:** `keel_pool_create()`, `keel_pool_alloc()`, `keel_pool_free()`, `keel_pool_destroy()`

### Slab Allocator

**File:** `src/mem/slab.c` (270 lines)

Variable-size allocator using power-of-2 size classes:

```
keel_slab_t
├── pools[0] ► keel_pool_t (16 bytes)
├── pools[1] ► keel_pool_t (32 bytes)
├── pools[2] ► keel_pool_t (64 bytes)
├── pools[3] ► keel_pool_t (128 bytes)
├── pools[4] ► keel_pool_t (256 bytes)
├── pools[5] ► keel_pool_t (512 bytes)
├── pools[6] ► keel_pool_t (1024 bytes)
├── pools[7] ► keel_pool_t (2048 bytes)
├── pools[8] ► keel_pool_t (4096 bytes)
└── pools[9] ► keel_pool_t (8192 bytes)
```

- **Size classes:** 10 classes from 16B to 8192B (powers of 2)
- **Rounding:** Request size rounds up to nearest class (e.g., 17 bytes → 32-byte slot)
- **Lazy init:** Pools created on first allocation for that size class
- **Fallback:** Requests > 8192 bytes go to system `malloc()`
- **Overhead:** Average ~25% internal fragmentation
- **API:** `keel_slab_create()`, `keel_slab_alloc()`, `keel_slab_destroy()`

---

## Data Structures Reference

| Structure | File | Purpose |
|-----------|------|---------|
| `options_t` | main.c | CLI arguments |
| `worker_group_t` | main.c | Per-group config + runtime handles |
| `keel_proxy_config_t` | main.c | Global proxy defaults |
| `keel_logging_config_t` | main.c | Logging plugin configuration |
| `keel_config_t` | config.c | Parsed INI file (linked list of sections) |
| `keel_engine_t` | engine.c | Engine state, worker pool, stats |
| `keel_engine_config_t` | engine.h | Engine configuration (all tuning knobs) |
| `keel_worker_t` | worker.c | Per-worker thread-local state |
| `keel_reactor_t` | reactor.c | I/O reactor (io_uring/kqueue/epoll) |
| `backend_pool_t` | backend_pool.c | Connection pool per backend server |
| `backend_pool_config_t` | backend_pool.h | Pool sizing and timeouts |
| `keel_server_pool_t` | engine.h | Logical server group (primary + replicas) |
| `keel_backend_server_t` | engine.h | Individual server (host, port, role, weight) |
| `keel_session_slab_t` | session.h | Pre-allocated session array |
| `recv_context_t` | worker.c | Per-session recv buffers + flow state |
| `keel_stats_collector_t` | stats.c | Per-worker padded stats aggregation |
| `keel_log_plugin_t` | log_plugin.h | Logging plugin vtable |
| `keel_query_log_t` | query_log.h | Query logging state + config |
| `keel_probe_config_t` | probe.h | Health probe type, interval, timeouts |
| `keel_admin_config_t` | admin.h | Admin console + Prometheus config |
| `keel_alloc_header` | mem.c | Hidden allocation header for tracking |
| `keel_arena_t` | arena.c | Bump-pointer arena allocator |
| `keel_pool_t` | pool.c | Fixed-size free-list pool |
| `keel_slab_t` | slab.c | Multi-class slab allocator |

---

## Startup Sequence Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          MAIN THREAD                                     │
│                                                                          │
│  ┌─────────────────┐                                                     │
│  │ setbuf + rlimit │ Disable buffering, raise FD limit                   │
│  └──────────────┬──┘                                                     │
│                 │                                                        │
│  ┌──────────────▼──┐                                                     │
│  │ parse_options() │ CLI: -c config -p port -w workers ...               │
│  └──────────────┬──┘                                                     │
│                 │                                                        │
│  ┌──────────────▼───────┐                                                │
│  │ keel_config_load()   │ INI parser → linked list of sections/entries   │
│  └──────────────┬───────┘                                                │
│                 │                                                        │
│  ┌──────────────▼───────────────────┐                                    │
│  │ Parse worker groups              │                                    │
│  │  ├─ [worker_group.pg]            │                                    │
│  │  │   ├─ bind_addr, bind_port     │                                    │
│  │  │   ├─ protocol = postgres      │                                    │
│  │  │   ├─ pool_min/max, workers    │                                    │
│  │  │   └─ [.servers] → primary +   │                                    │
│  │  │       replicas with R/W roles │                                    │
│  │  └─ [worker_group.my]            │                                    │
│  │      └─ protocol = mysql, ...    │                                    │
│  └──────────────┬───────────────────┘                                    │
│                 │                                                        │
│  ┌──────────────▼────────────────┐                                       │
│  │ Log Plugin + Query Logger     │                                       │
│  │  ├─ Choose plugin (stdout/    │                                       │
│  │  │   file/syslog/dlopen)      │                                       │
│  │  ├─ plugin->open(config)      │                                       │
│  │  ├─ Query log: mode/level     │                                       │
│  │  └─ keel_query_log_set_global │                                       │
│  └──────────────┬────────────────┘                                       │
│                 │                                                        │
│  ┌──────────────▼────────────────┐                                       │
│  │ FOR EACH worker_group:        │                                       │
│  │  ┌───────────────────────┐    │                                       │
│  │  │ create_listen_socket  │    │                                       │
│  │  │  SO_REUSEPORT         │    │                                       │
│  │  │  TCP_NODELAY          │    │                                       │
│  │  │  O_NONBLOCK           │    │                                       │
│  │  │  listen(fd, 4096)     │    │                                       │
│  │  └───────────┬───────────┘    │                                       │
│  │  ┌───────────▼───────────┐    │                                       │
│  │  │ keel_engine_create    │    │                                       │
│  │  │  resolve workers      │    │                                       │
│  │  │  create stats         │    │                                       │
│  │  │  detect reactor       │    │                                       │
│  │  │  resolve protocol     │    │                                       │
│  │  └───────────┬───────────┘    │                                       │
│  └──────────────┼────────────────┘                                       │
│                 │                                                        │
│  ┌──────────────▼──────────────────────────────┐                         │
│  │ "Ready" banner + admin/prometheus/probes    │                         │
│  └──────────────┬──────────────────────────────┘                         │
│                 │                                                        │
│  ╔══════════════▼══════════════════════════════╗                         │
│  ║ FOR EACH worker_group:                      ║                         │
│  ║   keel_engine_start(engine, listen_fd)      ║                         │
│  ║     ├── keel_worker_pool_init()             ║                         │
│  ║     │     └── FOR EACH worker:              ║                         │
│  ║     │           keel_worker_init()          ║                         │
│  ║     │            ├── reactor (io_uring)     ║                         │
│  ║     │            ├── backend pools          ║                         │
│  ║     │            ├── async warmup           ║                         │
│  ║     │            ├── session slab           ║                         │
│  ║     │            ├── recv_ctx pool          ║                         │
│  ║     │            ├── pipe pool              ║                         │
│  ║     │            └── timer wheel            ║                         │
│  ║     └── keel_worker_pool_start()            ║                         │
│  ║           └── FOR EACH worker:              ║                         │
│  ║                 keel_reactor_accept()       ║     ┌───────────────┐   │
│  ║                 pthread_create() ═══════════╬════►│ Worker Thread │   │
│  ╚═════════════════════════════════════════════╝     │ (event loop)  │   │
│                 │                                    └───────────────┘   │
│  ┌──────────────▼────────────┐                                           │
│  │ Signal loop               │                                           │
│  │  sigwait(SIGINT/TERM/USR1)│                                           │
│  │  SIGUSR1 → stats_dump()   │                                           │
│  │  SIGINT  → shutdown       │                                           │
│  └───────────────────────────┘                                           │
└──────────────────────────────────────────────────────────────────────────┘
```
