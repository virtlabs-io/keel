# Reactor-Blocking Inventory

Status: **v0.2-alpha planning** &middot; Total call sites: **26** &middot; Gate: [`scripts/check_forbidden_blocking.sh`](../scripts/check_forbidden_blocking.sh)

This document categorizes every blocking call site currently present in the
documented reactor hot-path files. It is the punch-list driving the v0.2-alpha
reactor refactor and the source of truth for which entries should be removed
from [`scripts/forbidden_blocking_baseline.txt`](../scripts/forbidden_blocking_baseline.txt)
as each refactor lands.

Each entry is annotated with one of:

- **Refactor** — replace with a reactor-driven state machine.
- **Annotate** — provably nonblocking (e.g. one-shot 8-byte eventfd I/O);
  add `/* NOLINT(keel-blocking) */` with a justification comment.
- **Defer**    — control-plane code outside the per-session hot loop; lower
  priority but still tracked.

The baseline gate (`scripts/check_forbidden_blocking.sh`) is configured to
fail CI on any *new* violation in a hot-path file, so the inventory can only
shrink.

---

## Category A &mdash; Deferred BEGIN inline send/recv (RESOLVED in PR #4)

The session-flow code previously cleared `O_NONBLOCK`, sent the deferred
`BEGIN` payload, then looped on `recv()` to drain the backend's
`ReadyForQuery` before resuming. That stalled the entire worker reactor for the
duration of one network round-trip.

**Status: RESOLVED.**  `defer_begin_replay()` now enqueues a `KEEL_PQOP_DEFERRED_BEGIN`
pre-query operation.  The reactor's BE-recv completion drives the absorber in
`keel_engine_flow_on_be_data()`, which drains the backend's response stream
and forwards the stashed client payload only after `ReadyForQuery` is confirmed.
All four former call sites have been removed from the baseline.

---

## Category B &mdash; DISCARD ALL Sync inline send (target: PR #4 follow-up)

Variant of category A in the prepared-statement Sync replay path: an inline
`send()` followed by a `recv(... MSG_DONTWAIT)` loop.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/engine/engine_flow.c](../src/engine/engine_flow.c#L2555) | 2555 | `send` | Sync packet for replay path |

**Action:** fold into the same reactor-driven session-flow state machine as
category A.

---

## Category C &mdash; `safe_send_all` helper (target: PR #4 follow-up)

Generic helper that loops `send(... MSG_NOSIGNAL)` until the buffer is fully
written. Used on error/handshake paths where stalling is &ldquo;rare&rdquo;,
but a hostile slow peer can still hold the worker.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/engine/engine_flow.c](../src/engine/engine_flow.c#L441) | 441 | `send` | Loop body |

**Action:** replace callers with a writev-style queued-write descriptor that
the reactor flushes on `EPOLLOUT` / io_uring `SQE_WRITE` completion.

---

## Category D &mdash; Auth notification eventfd read (target: annotate)

Single 8-byte read from a dedicated eventfd used to notify the worker that
the slow auth path has completed. The kernel guarantees this read is
nonblocking when the eventfd was previously signalled, which is the only
path that triggers it.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/engine/engine_flow.c](../src/engine/engine_flow.c#L674) | 674 | `read` | `read(sf->auth_notify_fd, &val, sizeof(val))` |

**Action:** add `/* NOLINT(keel-blocking) */` with a comment pointing at the
eventfd lifecycle invariant. Drop the baseline entry once annotated.

---

## Category E &mdash; Backend pool cleanup (RESOLVED in PR #3)

The conservative pool reusable-gate previously used `keel_fd_wait()` plus a `recv()` with
a 50&nbsp;ms timeout to verify the backend had drained before returning the
connection to the idle pool.

**Status: RESOLVED.**  `keel_fd_wait` has been removed from `backend_pool.c`.
The only remaining recv-family call in that file is a one-byte
`MSG_PEEK | MSG_DONTWAIT` liveness probe (non-blocking by construction).
All DISCARD-ALL sends now route through `keel_reactor_send` / `keel_reactor_recv`
(async reactor paths).  No entries remain in the baseline for this category.

---

## Category F &mdash; Legacy synchronous backend connect (target: PR #5)

`connect_to_backend()` performs a non-blocking `connect()` followed by a
five-second blocking `select()`. Even if no hot-path caller invokes it
today, its presence violates the &ldquo;zero-poll hot path&rdquo; design
and is a foot-gun for future code.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/worker/worker.c](../src/worker/worker.c#L117) | 117 | `select` | 5&nbsp;s timeout |

**Action:** verify no production code path reaches `connect_to_backend()`; if
confirmed, delete the function. If any path remains, move it into a clearly
quarantined translation unit and convert callers to the existing async path
in [src/worker/backend_connect_async.c](../src/worker/backend_connect_async.c).

---

## Category G &mdash; Worker session cleanup sends (target: PR #3 follow-up)

Three `send(... MSG_NOSIGNAL)` calls in worker session-cleanup paths.
Each fd is nonblocking in practice, but the calls do not pass
`MSG_DONTWAIT`, so the gate cannot statically confirm that.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/worker/worker.c](../src/worker/worker.c#L2070) | 2070 | `send` | Vtable-provided cleanup buffer |
| [src/worker/worker.c](../src/worker/worker.c#L2075) | 2075 | `send` | Fallback hardcoded `ROLLBACK` |
| [src/worker/worker.c](../src/worker/worker.c#L2850) | 2850 | `send` | TLS-handshake continuation flush |

**Action:** when the category E refactor lands, this code becomes the
state machine's first step &mdash; route these sends through the same
queued-write descriptor.

---

## Category H &mdash; Internal eventfd writes (target: annotate)

8-byte writes to internal eventfds used to wake worker reactors. The
kernel buffer is always able to accept an 8-byte add, so these never
block in any contemporary kernel.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/worker/worker.c](../src/worker/worker.c#L4501) | 4501 | `write` | `keel_worker_stop()` wake |
| [src/worker/worker.c](../src/worker/worker.c#L4514) | 4514 | `write` | `keel_worker_drain()` wake |
| [src/worker/migration.c](../src/worker/migration.c#L336) | 336 | `write` | Cross-worker handoff wake |

**Action:** add `/* NOLINT(keel-blocking) */` with a one-line comment
citing the eventfd 8-byte-add invariant. Drop these three baseline
entries once annotated.

---

## Category I &mdash; Worker-to-worker FD handoff (target: defer)

`migration.c` performs an `SCM_RIGHTS` fd handoff over a Unix domain
socket during graceful worker shutdown. The send is one-shot and tiny.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/worker/migration.c](../src/worker/migration.c#L76) | 76 | `sendmsg` | `SCM_RIGHTS` ancillary datagram |

**Action:** defer to a post-v0.2 pass. Either add `MSG_DONTWAIT` and
handle `EAGAIN` (queueing the handoff) or annotate with `NOLINT` if a
shutdown-time stall is acceptable.

---

## Category J &mdash; Scatter merge blocking I/O (target: separate refactor)

`engine_scatter.c` uses helper functions (`sc_read_full`, `sc_write_full`)
that loop on `recv()` / `send()` without `MSG_DONTWAIT` during scatter
fan-out. This is a known issue tracked separately from the v0.2 reactor
work.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/engine/engine_scatter.c](../src/engine/engine_scatter.c#L587) | 587 | `recv` | `sc_read_full` loop |
| [src/engine/engine_scatter.c](../src/engine/engine_scatter.c#L598) | 598 | `send` | `sc_write_full` loop |
| [src/engine/engine_scatter.c](../src/engine/engine_scatter.c#L891) | 891 | `send` | Scatter shard-dispatch send |

**Action:** scheduled for a dedicated scatter-async PR after PRs #3/#4/#5
land. Tracked here so the count stays visible.

---

## Category K &mdash; Engine drain / worker-reload waits (target: defer)

Control-plane wait loops in the engine main thread that poll worker
state every 100&nbsp;ms during shutdown drain and during live-reload.
Not on any per-session hot path.

| File | Line | Call | Notes |
|---|---|---|---|
| [src/engine/engine.c](../src/engine/engine.c#L503) | 503 | `nanosleep` | Drain wait loop |
| [src/engine/engine.c](../src/engine/engine.c#L738) | 738 | `nanosleep` | Worker-reload wait loop |

**Action:** replace with an eventfd or condition variable signalled by
worker state transitions. Low priority; safe to leave until a broader
control-plane cleanup.

---

## Refactor sequencing

The agreed ordering for v0.2-alpha (this branch) is:

1. **PR #4** &mdash; categories A, B, C, D (engine_flow async).
2. **PR #3** &mdash; categories E, G, H (backend_pool + worker cleanup async).
3. **PR #5** &mdash; category F (remove or quarantine legacy connect).

Categories I, J, K remain in the baseline after v0.2-alpha and are
addressed in subsequent releases.
