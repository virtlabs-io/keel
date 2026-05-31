# keel-v0.5-beta: async park + reactor-thread resume for `stale_read_policy = wait`

**Status**: proposal  
**Target**: v0.5-beta  
**Depends on**: Patches 2a–2e (all merged in v0.5-alpha)  
**Entry point**: `keel_engine_consult_catchup()` in `include/keel/engine/catchup_bridge.h`

---

## Background

v0.5-alpha ships a **safe-degrade** path: when the router emits
`KEEL_ROUTE_REASON_WAIT_CATCHUP` the engine immediately reroutes the read to
the primary instead of parking the session. This is correct and observable
(`wait_catchup_degraded_to_primary_total`), but every degraded read adds load
to the primary that the replica should have absorbed.

The full policy is: park the session, let the per-worker probe state machine
confirm that the replica crossed the required LSN/GTID, then re-dispatch the
original read on that replica — or fall back to the primary only after the
deadline expires. All the plumbing for this exists; only the **wakeup +
re-dispatch glue** is missing.

---

## Existing infrastructure (no changes needed to these)

| Component | Location | Status |
|---|---|---|
| `keel_catchup_manager_t` with wait list, probe SM, 5 ms tick | `include/keel/engine/catchup.h`, `src/worker/worker_catchup.c` | ✅ merged |
| `catchup_tick_timer_cb` wired in `worker_init()` | `src/worker/worker.c:772` | ✅ merged |
| `keel_engine_consult_catchup()` — enqueue + return `KEEL_FLOW_WAIT_CATCHUP` | `include/keel/engine/catchup_bridge.h` | ✅ merged |
| `keel_catchup_resume_cb(session, outcome, userdata)` callback type | `include/keel/engine/catchup.h:~130` | ✅ merged |
| `KEEL_FLOW_WAIT_CATCHUP` flow result | `include/keel/engine/engine_flow.h:49` | ✅ defined |
| Pending-message stash (`flow.pending_msg` / `pending_msg_len`) | `include/keel/engine/engine_flow.h` | ✅ used by pool-wait path |
| `keel_reactor_timeout()` / `keel_reactor_cancel_timeout()` | `include/keel/reactor/reactor.h:344` | ✅ |

---

## What v0.5-beta must add

### 1. `keel_engine_flow_resume_from_catchup()` — new function in `engine_flow.c`

```c
/*
 * Called from the catchup resume callback (worker-thread reactor context).
 * Drives re-dispatch or graceful fallback depending on the outcome.
 *
 * On REACHED     : re-dispatch using the replica at wait_server_index.
 * On TIMEOUT     : re-dispatch on primary (degrade path, same as 2d-4).
 * On PROBE_FAILED: same as TIMEOUT.
 * On CANCELLED   : close the session.
 *
 * Returns the flow result that the caller must surface to the worker's
 * flow-result switch (identical contract to resume_from_pool).
 */
keel_flow_result_t keel_engine_flow_resume_from_catchup(
    keel_session_flow_t*     sf,
    struct keel_session*     session,
    keel_catchup_outcome_t   outcome,
    size_t                   replica_server_index);  /* from rd.wait_server_index */
```

Implementation sketch:

```c
keel_flow_result_t keel_engine_flow_resume_from_catchup(
    keel_session_flow_t* sf,
    struct keel_session* session,
    keel_catchup_outcome_t outcome,
    size_t replica_server_index)
{
    switch (outcome) {
    case KEEL_CATCHUP_REACHED:
        /* Replica caught up — re-dispatch on the originally selected replica. */
        sf->catchup_resume_forced_server = replica_server_index;
        sf->catchup_resume_use_primary   = false;
        break;

    case KEEL_CATCHUP_TIMEOUT:
    case KEEL_CATCHUP_PROBE_FAILED:
        /* Deadline expired or probe broke — degrade to primary, same as
         * the v0.5-alpha safe-degrade path. */
        sf->catchup_resume_forced_server = SIZE_MAX;  /* sentinel: primary */
        sf->catchup_resume_use_primary   = true;
        break;

    case KEEL_CATCHUP_CANCELLED:
        return KEEL_FLOW_CLOSED;
    }

    /* Re-enter the FE data path with the stashed pending message.
     * The pending message was saved by the call site before enqueuing. */
    KEEL_ASSERT(sf->pending_msg != NULL,
        "catchup resume: pending_msg must be stashed before enqueue");
    return keel_engine_flow_on_fe_data_from_stash(sf, session);
}
```

Two new fields on `keel_session_flow_t` (add to `engine_flow.h`):

```c
size_t   catchup_resume_forced_server;  /**< Replica index to use on REACHED, or SIZE_MAX for primary */
bool     catchup_resume_use_primary;    /**< True if the resume should degrade to primary */
```

The routing selection in `keel_engine_flow_on_fe_data` consults these fields
immediately after the catch-up consultation block (mirroring the existing
`KEEL_FLOW_WAIT_POOL` / `resume_from_pool` pattern).

---

### 2. `on_catchup_resume_cb` — static callback in `worker.c`

```c
typedef struct {
    keel_recv_context_t* recv_ctx;
    size_t               wait_server_index;
} catchup_resume_userdata_t;   /* stack-allocated at enqueue; must be heap for async */

static void on_catchup_resume_cb(
    struct keel_session*   session,
    keel_catchup_outcome_t outcome,
    void*                  userdata)
{
    catchup_resume_userdata_t* ud  = userdata;
    keel_recv_context_t*       rctx = ud->recv_ctx;
    keel_worker_t*             worker = rctx->worker;

    keel_flow_result_t fr = keel_engine_flow_resume_from_catchup(
        &rctx->flow, session, outcome, ud->wait_server_index);

    keel_free(ud);   /* heap-allocated at enqueue time */

    handle_flow_result(worker, session, rctx, fr);  /* existing helper */
}
```

`handle_flow_result` is the extracted helper (or inline equivalent) that
already exists in `worker.c` for processing a `keel_flow_result_t` from pool
resume — `WAIT_BACKEND`, `WAIT_POOL`, `CLOSED`, etc. No new state machine is
needed; the callback drops straight into the existing switch.

---

### 3. Wire the call site in `engine_flow.c` (replace safe-degrade block)

Current v0.5-alpha code (Patch 2d-4, `engine_flow.c:~2771`):

```c
worker->stats.wait_catchup_consulted_total++;
if (keel_engine_should_degrade_to_primary_on_wait(...)) {
    worker->stats.wait_catchup_degraded_to_primary++;
    route = KEEL_FROUTE_WRITE;   /* degrade */
}
```

Replace with:

```c
worker->stats.wait_catchup_consulted_total++;

/* Stash the pending FE message before we may park the session.
 * keel_engine_flow_stash_pending() is a no-op if already stashed. */
keel_engine_flow_stash_pending(sf, act.sql_view, act.sql_view_len);

catchup_resume_userdata_t* ud = keel_malloc(sizeof(*ud));
if (ud) {
    ud->recv_ctx           = recv_ctx;  /* captured from outer scope */
    ud->wait_server_index  = 0;         /* filled in by consult_catchup */

    keel_flow_result_t cr = keel_engine_consult_catchup(
        worker->router,
        worker->catchup,
        session,
        &route_session,
        _wc_qt,
        on_catchup_resume_cb,
        ud,
        &rd);

    switch (cr) {
    case KEEL_FLOW_WAIT_CATCHUP:
        ud->wait_server_index = rd.wait_server_index;
        keel_arena_destroy(_wc_arena);
        return KEEL_FLOW_WAIT_CATCHUP;   /* worker will not re-arm FE recv */

    case KEEL_FLOW_ERROR:
        /* Enqueue rejected (wait list full) — degrade to primary. */
        worker->stats.wait_catchup_degraded_to_primary++;
        route = KEEL_FROUTE_WRITE;
        keel_free(ud);
        break;

    default:
        /* Router chose a server normally (no WAIT) — proceed. */
        keel_free(ud);
        break;
    }
} else {
    /* OOM — degrade to primary to preserve RYW correctness. */
    worker->stats.wait_catchup_degraded_to_primary++;
    route = KEEL_FROUTE_WRITE;
}
```

`wait_server_index` is populated *after* `consult_catchup` returns (the bridge
already stores it in `rd`), so the `ud` back-fill is safe before the
`KEEL_FLOW_WAIT_CATCHUP` return.

---

### 4. Handle `KEEL_FLOW_WAIT_CATCHUP` in `worker.c`'s flow-result switch

Add a case alongside `KEEL_FLOW_WAIT_POOL`:

```c
case KEEL_FLOW_WAIT_CATCHUP:
    /* Session is parked in the per-worker catch-up wait list.
     * The resume callback (on_catchup_resume_cb) will re-enter
     * handle_flow_result when the replica crosses the token or the
     * deadline expires. Do NOT re-arm FE recv — the session is frozen
     * until the callback fires from catchup_tick_timer_cb. */
    KEEL_DEBUG_LOG("W%u: session %lu parked for catchup\n",
                   worker->id, (unsigned long)session->id);
    return;
```

No timer registration needed: `catchup_tick_timer_cb` is already running at
5 ms. The manager will fire `on_catchup_resume_cb` from the tick, which runs
on the reactor thread — no cross-thread wakeup required.

---

### 5. Frame-save contract

The pending FE message must be saved **before** `keel_engine_consult_catchup()`
is called. The existing `flow.pending_msg` / `flow.pending_msg_len` buffer
(already used by `KEEL_FLOW_WAIT_POOL`) is the right place. A new helper:

```c
/* Save sql_view into flow->pending_msg if not already stashed.
 * Called immediately before the catchup enqueue. */
static void keel_engine_flow_stash_pending(
    keel_session_flow_t* sf,
    const char* sql_view, size_t sql_len);
```

On resume, `keel_engine_flow_resume_from_catchup()` calls
`keel_engine_flow_on_fe_data_from_stash()` (the same helper pool resume uses),
which feeds `pending_msg` back through the FE data path. No new machinery —
re-uses the pool-wait stash exactly.

---

## Outcome behavior table

| Outcome | Meaning | Action |
|---|---|---|
| `KEEL_CATCHUP_REACHED` | Replica crossed the required LSN/GTID before deadline | Re-dispatch on replica at `wait_server_index`; clear `catchup_resume_*` fields |
| `KEEL_CATCHUP_TIMEOUT` | `max_replica_catchup_ms` elapsed | Degrade to primary; bump `wait_catchup_degraded_to_primary_total` |
| `KEEL_CATCHUP_PROBE_FAILED` | Probe socket broken / replica unreachable | Same as TIMEOUT |
| `KEEL_CATCHUP_CANCELLED` | Session closed while parked | `KEEL_FLOW_CLOSED`; do not touch session |

---

## Effect on observable counters

- `wait_catchup_consulted_total` — unchanged, incremented at the consultation
  site (same as today).
- `wait_catchup_degraded_to_primary_total` — only increments on TIMEOUT /
  PROBE_FAILED / OOM / wait-list-full; **not** on REACHED. Operators will
  see this counter drop toward 0 when replicas are healthy.

---

## What does NOT change

- `keel_catchup_manager_t` internals, probe state machines, or the 5 ms tick.
- The `catchup_bridge.h` / `keel_engine_consult_catchup()` signature.
- The `keel_catchup_resume_cb` type.
- The safe-degrade logic used when `KEEL_FLOW_ERROR` is returned from the
  bridge (wait-list full) — same primary fallback as today.
- The `in_transaction` short-circuit (already skips the consultation block).

---

## Testing plan

1. **Unit test** `test_engine_flow_catchup_resume` — mock manager that fires
   each of the four outcomes; assert flow result and `pending_msg` consumed.
2. **Live PG e2e** extension of `test_engine_catchup_consult_pg_e2e` — pause
   replica replication, enqueue a waiter, resume replication, confirm REACHED
   fires and the read lands on the replica (not the primary).
3. **Regression**: 139+ existing tests must stay green.
4. **Counter smoke test**: `wait_catchup_degraded_to_primary_total` stays 0
   when replica lag is zero and `stale_read_policy = wait` is active.
