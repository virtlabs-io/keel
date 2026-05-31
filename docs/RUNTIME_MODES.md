# Runtime Mode Tiers

> **TL;DR:** KEEL has four runtime tiers — `proxy`, `pool`, `smart`, `full` — that control
> which hot-path features are active.  Lower tiers disable expensive features so users who
> only need basic pooling pay near-zero overhead for capabilities they never use.

---

## Table of Contents

1. [Motivation](#motivation)
2. [Tier Overview](#tier-overview)
3. [Feature Matrix](#feature-matrix)
4. [Configuration](#configuration)
5. [How It Works](#how-it-works)
6. [Performance Impact](#performance-impact)
7. [Choosing a Tier](#choosing-a-tier)
8. [Implementation](#implementation)

---

## Motivation

KEEL is feature-rich: hooks, SQL analysis, R/W routing, transaction tracking, LSN capture,
state sync, query logging.  But not every deployment needs every feature.  A team that uses
KEEL as a simple connection pooler (replacing PgBouncer) should not pay the CPU cost of hook
dispatch, query logging, or LSN capture on every query.

Runtime mode tiers solve this by **gating hot-path features behind a single integer comparison**.
Each gate compiles to one `cmp + jcc` instruction — zero overhead for disabled features.

---

## Tier Overview

| Tier | Config Value | Purpose | Use Case |
|------|:-------------|---------|----------|
| **PROXY** | `proxy` | Minimal pass-through | TCP proxy, no pooling, session-pinned |
| **POOL** | `pool` | Connection pooling + PS replay | PgBouncer replacement, basic pooling |
| **SMART** | `smart` | + R/W routing, query log, state sync | Intelligent routing, observability |
| **FULL** | `full` | + hooks, txn tracking, LSN capture | Full programmability, safety guarantees |

Tiers are **cumulative** — each tier includes everything from the tier below it.

---

## Feature Matrix

| Feature | PROXY | POOL | SMART | FULL |
|---------|:-----:|:----:|:-----:|:----:|
| Frame extraction + forward | ✅ | ✅ | ✅ | ✅ |
| Connection pooling | ❌ | ✅ | ✅ | ✅ |
| Prepared statement replay | ❌ | ✅ | ✅ | ✅ |
| R/W routing + sticky-primary | ❌ | ❌ | ✅ | ✅ |
| Query logging + SQL analysis | ❌ | ❌ | ✅ | ✅ |
| Backend state sync | ❌ | ❌ | ✅ | ✅ |
| Full statistics (per-type counters) | ❌ | basic | ✅ | ✅ |
| Hook dispatch (4 pipeline points) | ❌ | ❌ | ❌ | ✅ |
| Transaction tracking (XID probe) | ❌ | ❌ | ❌ | ✅ |
| WAL LSN / GTID capture | ❌ | ❌ | ❌ | ✅ |

---

## Configuration

Set the tier per worker group in your INI file:

```ini
[worker-group:main]
mode = smart            # proxy | pool | smart | full (default: pool)
workers = 4
listen_port = 7432
```

The default is `pool` for production-safe startup behavior. Raise the tier only
when your deployment explicitly needs routing (`smart`) or hook/LSN features (`full`).

### Example Configurations

**Simple pooler (PgBouncer replacement):**
```ini
[worker-group:main]
mode = pool
prepared_statement = virtualize
```

**Intelligent routing with observability:**
```ini
[worker-group:main]
mode = smart   # hardening tier
query_log = all
```

**Full programmability with hooks and safety:**
```ini
[worker-group:main]
mode = full    # explicit opt-in; not the default production profile
transaction_tracking = on
hook_script_lua = /etc/keel/hooks/audit.lua
```

**TCP proxy (minimal overhead):**
```ini
[worker-group:main]
mode = proxy
# Session is hard-pinned to one backend.
# No pooling, no PS replay, no routing.
```

---

## How It Works

The tier is stored as a `uint8_t` on the session flow structure (`keel_session_flow_t.mode`).
Each feature gate is a macro that compiles to a single integer comparison:

```c
#define KEEL_TIER_HAS_HOOKS(t)     ((t) >= KEEL_TIER_FULL)
#define KEEL_TIER_HAS_ROUTING(t)   ((t) >= KEEL_TIER_SMART)
#define KEEL_TIER_HAS_POOLING(t)   ((t) >= KEEL_TIER_POOL)
```

In the hot path, each gate looks like:

```c
if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY) {
    keel_hook_ctx_t hctx;
    engine_fill_hook_ctx(&hctx, session, &act);  // ~100ns: memset + 30 field copies
    keel_hook_fire(hooks, KEEL_HOOK_AFTER_QUERY_READ, &hctx);
}
```

At `pool` tier, `KEEL_TIER_HAS_HOOKS` is false — the entire block is skipped, including the
expensive `engine_fill_hook_ctx()` call.  The branch predictor quickly learns this pattern; the
effective cost is ~0.5ns per skipped gate.

### Propagation Chain

```
INI file
  → worker_group_t.runtime_mode     (config parsing)
    → keel_engine_config_t.runtime_mode  (engine config)
      → keel_worker_t.runtime_mode       (per-worker)
        → keel_session_flow_t.mode       (per-session, hot path)
```

### PROXY Mode Overrides

PROXY mode automatically forces:
- `ps_mode = KEEL_PS_MODE_OFF` (no prepared statement replay)
- `txn_tracking = false` (no XID probe)

These overrides ensure that PROXY mode is truly minimal — a session is hard-pinned to one
backend with zero pooling sophistication.

---

## Performance Impact

### Per-Query Hot-Path Costs by Tier

| Operation | Cost | PROXY | POOL | SMART | FULL |
|-----------|------|:-----:|:----:|:-----:|:----:|
| Hook dispatch (×4 points) | ~100ns-1µs each | ❌ | ❌ | ❌ | ✅ |
| `engine_fill_hook_ctx()` | ~100ns (memset+30 fields) | ❌ | ❌ | ❌ | ✅ |
| Query logging + SQL analysis | ~0.5-2µs | ❌ | ❌ | ✅ | ✅ |
| Sticky-primary routing | ~10-50ns | ❌ | ❌ | ✅ | ✅ |
| State sync (diff + send) | ~1-5µs (when needed) | ❌ | ❌ | ✅ | ✅ |
| PS replay | ~5-20µs (when needed) | ❌ | ✅ | ✅ | ✅ |
| Transaction tracking (XID) | ~1µs | ❌ | ❌ | ❌ | ✅ |
| LSN capture | ~2-5µs | ❌ | ❌ | ❌ | ✅ |
| Per-type stats counters | ~5ns | ❌ | basic | ✅ | ✅ |

### Estimated Overhead vs Direct Connection

| Tier | Overhead Estimate |
|------|:-----------------:|
| PROXY | ~2-3% (frame extraction only) |
| POOL | ~5-8% (+ pooling + PS replay) |
| SMART | ~8-12% (+ routing + logging + sync) |
| FULL | ~12-15% (+ hooks + txn tracking + LSN) |

---

## Choosing a Tier

```
                     Do you need connection pooling?
                              │
                    ┌─── No ──┤── Yes ──┐
                    ▼                    ▼
                  PROXY          Do you need R/W routing?
                                         │
                               ┌── No ──┤── Yes ──┐
                               ▼                   ▼
                             POOL           Do you need hooks
                                            or txn tracking?
                                                   │
                                         ┌── No ──┤── Yes ──┐
                                         ▼                   ▼
                                       SMART               FULL
```

**When in doubt, start with `pool`.** That is the recommended production tier.
Move to `smart` or `full` only after you have validated the
higher-tier behavior you need in your environment.

---

## Implementation

### Source Files

| File | Changes |
|------|---------|
| [include/keel/engine/runtime_mode.h](../include/keel/engine/runtime_mode.h) | `keel_tier_t` enum, 8 gate macros, parse/name helpers |
| [include/keel/engine/engine.h](../include/keel/engine/engine.h) | `runtime_mode` field in engine config |
| [include/keel/engine/worker.h](../include/keel/engine/worker.h) | `runtime_mode` field in worker struct |
| [include/keel/engine/engine_flow.h](../include/keel/engine/engine_flow.h) | `mode` field in session flow |
| [src/engine/engine_flow.c](../src/engine/engine_flow.c) | 15 hot-path gates |
| [src/main/main.c](../src/main/main.c) | Config parsing, propagation, startup log |
| [src/worker/worker.c](../src/worker/worker.c) | Worker init propagation |

### Test Coverage

| Test | Assertions | Coverage |
|------|:----------:|---------|
| `test_runtime_mode` | 78 | Tier parsing, ordering, gate macros, name round-trip, PROXY overrides, default tier |

---

*Last Updated: March 2026*
