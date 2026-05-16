# Hook & Trigger System

The Keel hook system provides extension points in the query processing pipeline.
Hooks let you inspect, modify, or abort queries at four stages — using **Lua**,
**Python**, or **native C shared libraries** (.so / .dylib).

## Overview

```
Client
  │
  ▼
┌──────────────────────────────────────────────────────────────┐
│  1. AFTER_QUERY_READ   — raw query bytes received            │
│     ↓  hooks fire (Lua / Python / native)                    │
│  2. AFTER_QUERY_PARSE  — SQL classified, query tree built    │
│     ↓  hooks fire                                            │
│  3. BEFORE_ROUTE       — about to pick primary vs replica    │
│     ↓  hooks fire                                            │
│  4. BEFORE_SEND        — payload about to go to backend      │
│     ↓  hooks fire                                            │
│                                                              │
│  Backend ◄───────────────────────────────────────────────────│
└──────────────────────────────────────────────────────────────┘
```

At each point, hooks receive a **mutable context** (`keel_hook_ctx_t`) containing
session info, query data, classification, and routing hints.  Hooks can:

- **Read** all fields (session ID, username, database, SQL, query type, flags…)
- **Modify** mutable fields (route hint, needs_primary, effect flags, splice eligibility)
- **Abort** the query by returning `false` (with an error message)

Hooks execute in **priority order** (lower number = fires first).  If any hook
returns `false`, the chain stops and the query is aborted.

## Hook Points

| # | Point | When | Available Data |
|---|-------|------|----------------|
| 1 | `after_query_read` | Raw query bytes received from client | `raw_query`, `raw_query_len`, session info |
| 2 | `after_query_parse` | SQL parsed and classified | `sql_text`, `query_type`, `query_flags`, `effect_flags`, `needs_primary` |
| 3 | `before_route` | About to pick primary vs replica | All query + classification + `route_hint`, `in_transaction` |
| 4 | `before_send` | Payload about to go to backend | All above + `be_payload`, `be_payload_len`, `splice_eligible` |

## Hook Context

Every hook receives a context struct/table/dict with these fields:

### Read-Only Fields

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | uint64 | Unique session identifier |
| `username` | string | Authenticated user |
| `database` | string | Target database |
| `client_fd` | int | Client file descriptor |
| `server_fd` | int | Backend FD (-1 if not connected) |
| `in_transaction` | bool | Inside BEGIN..COMMIT? |
| `query_count` | uint32 | Queries processed in this session |

### Mutable Fields

| Field | Type | Description |
|-------|------|-------------|
| `sql_text` | string | SQL text (NUL-terminated) |
| `query_type` | uint32 | Query type enum (see below) |
| `query_flags` | uint32 | Query flags bitmap |
| `effect_flags` | uint32 | Effect flags bitmap |
| `needs_primary` | bool | Must route to primary? |
| `route_hint` | enum | `ROUTE_PRIMARY` (0), `ROUTE_REPLICA` (1), `ROUTE_ANY` (2) |
| `splice_eligible` | bool | Eligible for zero-copy splice?  When `true` and `fast_network_path = on`, the result forwarding path may use MSG_PEEK + `splice(2)` to bypass userspace for DataRow frames.  Setting this to `false` in a hook forces all result data through the normal engine path. |
| `error_msg` | string | Error message (set when returning false) |

### Backend Payload (BEFORE_SEND only)

| Field | Type | Description |
|-------|------|-------------|
| `be_payload` | bytes | Payload about to be sent |
| `be_payload_len` | size_t | Payload length in bytes |

### Query Type Values

| Value | Name | Category |
|-------|------|----------|
| 0 | `UNKNOWN` | — |
| 1 | `SELECT` | Read |
| 2 | `SHOW` | Read |
| 3 | `EXPLAIN` | Read |
| 4 | `INSERT` | Write |
| 5 | `UPDATE` | Write |
| 6 | `DELETE` | Write |
| 7 | `TRUNCATE` | Write |
| 8 | `CREATE` | DDL |
| 9 | `ALTER` | DDL |
| 10 | `DROP` | DDL |
| 11 | `BEGIN` | Transaction |
| 12 | `COMMIT` | Transaction |
| 13 | `ROLLBACK` | Transaction |
| 14 | `SAVEPOINT` | Transaction |
| 15 | `SET` | Session |
| 16 | `RESET` | Session |
| 17 | `DISCARD` | Session |
| 18 | `PREPARE` | Prepared |
| 19 | `EXECUTE` | Prepared |
| 20 | `DEALLOCATE` | Prepared |
| 21 | `COPY` | Copy |

## Writing Hooks

### Lua Hooks

Lua support is enabled by default. To force-enable explicitly:

```bash
cmake -DKEEL_ENABLE_LUA=ON ..
```

Hook function signature:

```lua
-- @param ctx  Table with session/query fields
-- @return bool, ctx — (true, ctx) to continue, (false, ctx) to abort
function my_hook(ctx)
    -- Read fields
    local user = ctx.username
    local sql  = ctx.sql_text

    -- Modify mutable fields
    ctx.route_hint    = ctx.ROUTE_PRIMARY
    ctx.needs_primary = true

    -- Abort example:
    -- ctx.error_msg = "Blocked by policy"
    -- return false, ctx

    return true, ctx
end
```

**Example — query logger** (`examples/hooks/lua/query_logger.lua`):

```lua
function log_query(ctx)
    local f = io.open("/tmp/keel_queries.log", "a")
    if not f then return true, ctx end

    local ts   = os.date("%Y-%m-%d %H:%M:%S")
    local rw   = ctx.needs_primary and "WRITE" or "READ"
    local sql  = (ctx.sql_text or ""):gsub("\n", " "):sub(1, 512)

    f:write(string.format(
        "%s | sid=%-6d | %s@%s | %s | %s\n",
        ts, ctx.session_id, ctx.username, ctx.database, rw, sql
    ))
    f:close()
    return true, ctx
end
```

**Example — block dangerous queries** (`examples/hooks/lua/block_dangerous.lua`):

```lua
function check_query(ctx)
    -- Block DROP statements
    if ctx.query_type == 10 then  -- DROP
        ctx.error_msg = "DROP statements are blocked"
        return false, ctx
    end

    -- Block DELETE without WHERE
    if ctx.query_type == 6 then   -- DELETE
        if not (ctx.sql_text or ""):upper():find("WHERE") then
            ctx.error_msg = "DELETE without WHERE is blocked"
            return false, ctx
        end
    end

    return true, ctx
end
```

### Python Hooks

Python support is enabled by default. To force-enable explicitly:

```bash
cmake -DKEEL_ENABLE_PYTHON=ON ..
```

Hook function signature:

```python
def my_hook(ctx: dict) -> tuple[bool, dict]:
    """
    ctx keys: session_id, username, database, sql_text, query_type,
              effect_flags, needs_primary, route_hint, ...

    Returns (True, ctx) to continue, (False, ctx) to abort.
    Set ctx["error_msg"] before returning False.
    """
    return True, ctx
```

**Example — query logger** (`examples/hooks/python/query_logger.py`):

```python
import datetime

def log_query(ctx):
    ts  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rw  = "WRITE" if ctx.get("needs_primary") else "READ"
    sql = (ctx.get("sql_text", "") or "").replace("\n", " ")[:512]

    with open("/tmp/keel_queries.log", "a") as f:
        f.write(f"{ts} | sid={ctx['session_id']} | "
                f"{ctx['username']}@{ctx['database']} | {rw} | {sql}\n")

    return True, ctx
```

**Example — block dangerous queries** (`examples/hooks/python/block_dangerous.py`):

```python
def check_query(ctx):
    qtype = ctx.get("query_type", 0)

    if qtype == 10:  # DROP
        ctx["error_msg"] = "DROP statements are blocked"
        return False, ctx

    if qtype == 6 and "WHERE" not in (ctx.get("sql_text", "") or "").upper():
        ctx["error_msg"] = "DELETE without WHERE is blocked"
        return False, ctx

    return True, ctx
```

### Native Plugins (.so / .dylib)

Native plugins are always available (no special build flags).  They are
shared libraries loaded via `dlopen()` that export a single entry point:

```c
const keel_hook_plugin_info_t* keel_hook_plugin_init(void);
```

**Plugin structure:**

```c
#include "keel_hook.h"

/* Hook callback */
static bool my_hook(keel_hook_ctx_t* ctx) {
    /* Read and modify ctx fields */
    if (some_condition) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                 "Blocked: reason");
        return false;   /* abort */
    }
    return true;        /* continue */
}

/* Hook registration table */
static const keel_hook_plugin_def_t s_hooks[] = {
    {
        .point    = KEEL_HOOK_AFTER_QUERY_PARSE,
        .name     = "my-hook",
        .fn       = my_hook,
        .priority = 100,
    },
};

/* Plugin descriptor */
static const keel_hook_plugin_info_t s_info = {
    .api_version = KEEL_HOOK_PLUGIN_API_VERSION,
    .name        = "my_plugin",
    .version     = "1.0.0",
    .description = "Example plugin",
    .hooks       = s_hooks,
    .hook_count  = 1,
    .shutdown    = NULL,  /* optional cleanup */
};

/* Entry point (called by keel via dlsym) */
const keel_hook_plugin_info_t* keel_hook_plugin_init(void) {
    return &s_info;
}
```

**Build:**

```bash
gcc -shared -fPIC -o my_plugin.so my_plugin.c -I/path/to/keel/include
```

See `examples/hooks/plugins/` for complete examples including a
query logger and a per-user rate limiter.

## Configuration

Hooks are configured **per worker group**, not globally.  Each worker group
has its own `[worker_group.<group>.hooks]` section, so different groups
(e.g., PostgreSQL vs MySQL) can have completely independent hook pipelines.

### Key Format

The configuration key encodes the hook type, trigger point, and name — no
numbers, no ambiguity:

| Hook Type | Key Format | Example |
|-----------|-----------|---------|
| Native plugin | `hook.plugin.<name>` | `hook.plugin.rate_limiter` |
| Lua | `hook.lua.<point>.<name>` | `hook.lua.after_query_parse.query_logger` |
| Python | `hook.python.<point>.<name>` | `hook.python.before_route.force_primary` |

Native plugins self-register their hook points internally, so the key only
needs the plugin name.  Lua and Python hooks fire at exactly one point, so
the point is part of the key.

### Example

```ini
# PostgreSQL group — full logging + policy enforcement
[worker_group.my_pgsql.hooks]

# --- Native .so plugins ---
# Syntax: hook.plugin.<name> = <path>
hook.plugin.query_logger = /usr/lib/keel/query_logger_plugin.so
hook.plugin.rate_limiter = /usr/lib/keel/rate_limiter_plugin.so

# --- Lua hooks ---
# Syntax: hook.lua.<point>.<name> = script=<path> func=<function> priority=<N>
hook.lua.after_query_parse.query_logger = \
    script=/etc/keel/hooks/query_logger.lua \
    func=log_query priority=200

hook.lua.after_query_parse.block_dangerous = \
    script=/etc/keel/hooks/block_dangerous.lua \
    func=check_query priority=50

hook.lua.before_route.force_primary = \
    script=/etc/keel/hooks/force_primary.lua \
    func=route_check priority=100

hook.lua.before_send.latency_sampler = \
    script=/etc/keel/hooks/latency_sampler.lua \
    func=before_send priority=500

# --- Python hooks ---
# Syntax: hook.python.<point>.<name> = module=<module> func=<function> priority=<N>
hook.python.after_query_parse.query_logger = \
    module=hooks.query_logger func=log_query priority=200

hook.python.before_route.force_primary = \
    module=hooks.force_primary func=route_check priority=100

# MySQL group — only rate limiting, no query logging
[worker_group.my_mysql.hooks]
hook.plugin.rate_limiter = /usr/lib/keel/rate_limiter_plugin.so
hook.lua.after_query_parse.block_dangerous = \
    script=/etc/keel/hooks/block_dangerous.lua \
    func=check_query priority=50
```

### Configuration Fields

**Native plugins** (`hook.plugin.<name>`):

| Field | Description |
|-------|-------------|
| Value | Path to the `.so` / `.dylib` shared library |

**Lua hooks** (`hook.lua.<point>.<name>`):

| Field | Required | Description |
|-------|----------|-------------|
| `script` | Yes | Path to the `.lua` file |
| `func` | Yes | Function name to call |
| `priority` | Yes | Execution priority (lower = earlier) |

**Python hooks** (`hook.python.<point>.<name>`):

| Field | Required | Description |
|-------|----------|-------------|
| `module` | Yes | Python module path (dot-separated) |
| `func` | Yes | Function name to call |
| `priority` | Yes | Execution priority (lower = earlier) |

Valid `<point>` values: `after_query_read`, `after_query_parse`, `before_route`, `before_send`.

### Priority Guidelines

| Range | Purpose | Example |
|-------|---------|---------|
| 1–49 | Security, rate limiting | Rate limiter, IP blocklist |
| 50–99 | Query validation, policy | Block DROP/TRUNCATE, enforce read-only |
| 100–199 | Routing overrides | Force-primary for specific users |
| 200–499 | Logging, auditing | Query logger, slow query log |
| 500+ | Telemetry, sampling | Latency sampler, metrics exporter |

## How Hooks Execute

1. The engine reaches a hook point (e.g., after parsing a query).
2. It builds a `keel_hook_ctx_t` from live session/query data.
3. `keel_hook_fire()` iterates the chain in priority order.
4. Each hook receives the context, can read/modify it, and returns `true`/`false`.
5. If a hook returns `false`:
   - The chain **stops** — remaining hooks do not fire.
   - The query is **aborted** with the error message set in `ctx->error_msg`.
   - The engine continues to the next frame (does not crash).
6. If all hooks return `true`, modifications are written back to the engine state.

### Thread Safety

- Each worker group owns its own **`keel_hook_registry_t`** — hooks registered
  in one group never fire in another.  This is the per-group isolation model.
- Each hook fires on the **worker thread** that owns the session.
- There is no cross-worker shared state in the hook context.
- Native plugins with global state (e.g., the rate limiter) must use atomics
  or other synchronization for shared counters.
- Lua and Python interpreters are global — concurrent calls from multiple
  workers are serialized by a lock inside the bridge.

### Performance

- Hook dispatch adds **< 1μs** per hook point if no hooks are registered.
- Native C hooks add negligible overhead (function pointer call).
- Lua hooks add ~5–20μs per call (Lua state + table marshaling).
- Python hooks add ~20–100μs per call (Python GIL + dict marshaling).
- For latency-sensitive workloads, prefer native plugins or Lua over Python.

## Statistics

Hook statistics are tracked per hook point:

| Metric | Description |
|--------|-------------|
| `fire_count` | Number of times the hook point was fired |
| `abort_count` | Number of times a hook returned false |
| `total_ns` | Total time spent in hooks (nanoseconds) |
| `hook_count` | Number of registered hooks at this point |

Access via the C API:

```c
keel_hook_registry_t* reg = keel_engine_get_hook_registry(engine);
keel_hook_stats_t stats = keel_hook_get_stats(reg, KEEL_HOOK_AFTER_QUERY_PARSE);
printf("Fires: %lu, Aborts: %lu, Avg: %lu ns\n",
       stats.fire_count, stats.abort_count,
       stats.fire_count ? stats.total_ns / stats.fire_count : 0);
```

## Build Options

| CMake Option | Default | Description |
|-------------|---------|-------------|
| `KEEL_ENABLE_LUA` | `ON` | Enable Lua hook support (links Lua 5.4 or LuaJIT) |
| `KEEL_ENABLE_PYTHON` | `ON` | Enable Python hook support (links CPython 3.x) |

Native .so plugins are always available (uses `dlopen`/`dlsym`).

```bash
# Build with all hook types enabled
cmake -DKEEL_ENABLE_LUA=ON -DKEEL_ENABLE_PYTHON=ON ..
make -j$(nproc)

# Build with only Lua
cmake -DKEEL_ENABLE_LUA=ON ..

# Build with only native plugin support
cmake -DKEEL_ENABLE_LUA=OFF -DKEEL_ENABLE_PYTHON=OFF ..
```

## Examples

Complete working examples are in `examples/hooks/`:

```
examples/hooks/
├── hooks.ini.example              # Configuration reference
├── lua/
│   ├── query_logger.lua           # Log every query to file
│   ├── block_dangerous.lua        # Block DROP/TRUNCATE/DELETE-without-WHERE
│   ├── force_primary.lua          # Force users/databases to primary
│   └── latency_sampler.lua        # Log payload info before send
├── python/
│   ├── query_logger.py            # Log every query to file
│   ├── block_dangerous.py         # Block dangerous queries
│   ├── force_primary.py           # Force routing to primary
│   └── latency_sampler.py         # Log outbound payload info
└── plugins/
    ├── query_logger_plugin.c      # Native .so query logger
    ├── rate_limiter_plugin.c      # Native .so per-user rate limiter
    └── Makefile.plugins           # Build the .so plugins
```

## C API Reference

```c
#include "keel_hook.h"

/* Registry lifecycle (one per worker group) */
keel_hook_registry_t* keel_hook_registry_create(void);
void                  keel_hook_registry_destroy(keel_hook_registry_t* reg);

/* Register hooks (reg = registry from keel_hook_registry_create) */
keel_hook_handle_t* keel_hook_register(reg, point, name, fn, priority, data);
keel_hook_handle_t* keel_hook_register_lua(reg, point, name, file, func, prio);
keel_hook_handle_t* keel_hook_register_python(reg, point, name, module, func, prio);
keel_error_t        keel_hook_load_plugin(reg, path);

/* Unregister */
void keel_hook_unregister(keel_hook_registry_t* reg, keel_hook_handle_t* handle);

/* Fire hooks */
bool keel_hook_fire(keel_hook_registry_t* reg, keel_hook_point_t point,
                    keel_hook_ctx_t* ctx);

/* Statistics */
keel_hook_stats_t keel_hook_get_stats(keel_hook_registry_t* reg,
                                      keel_hook_point_t point);

/* Engine accessor */
keel_hook_registry_t* keel_engine_get_hook_registry(keel_engine_t* engine);

/* Legacy global API (deprecated — for tests only) */
keel_error_t keel_hook_init(void);
void         keel_hook_shutdown(void);
```

See [keel_hook.h](../include/keel_hook.h) for the complete type definitions.
