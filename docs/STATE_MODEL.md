# KEEL State Model

## Overview

KEEL operates as a state-machine-driven system with **explicit ownership, explicit transitions, explicit invariants, and derived eligibility rules**. This document is the canonical reference for the formal state model.

**Design rule:** No critical runtime behavior should depend on interpreting more than one canonical state object plus one explicit event. If a branch requires checking three booleans, two pin flags, transport mode, drain mode, and backend cleanup status — that logic is too implicit and should be collapsed into a formal transition or derived predicate.

---

## 1. State Domains

KEEL has nine formal state domains. Each domain has a canonical enum and well-defined transitions.

### 1.1 Session Lifecycle (`keel_session_state_t`)

Tracks the session through its full lifecycle from accept to close.

```
INIT → STARTUP → AUTH → BACKEND_CONNECT → READY ↔ QUERY → CLOSING → CLOSED
                                              ↕        ↕
                                            COPY    FE_READ/FE_CLASSIFY/FE_WAIT_BACKEND
                                                    BE_SYNC/STREAM_COPY/STREAM_SPLICE
                                                    HARD_PIN
```

**18 states** — defined in `keel/session/session.h`.

### 1.2 Session Phase (`keel_session_phase_t`)

Coarse engine-level lifecycle phase. This is the primary decision variable for the engine flow.

| Phase | Meaning |
|-------|---------|
| `HANDSHAKE_AUTH` | Transport handshake + authentication |
| `READY` | Authenticated, waiting for queries |
| `QUERY` | Processing a query cycle |
| `BACKEND_SYNC` | Syncing backend state before query |
| `BACKEND_CLEANING` | Cleanup state machine on backend |
| `CLOSING` | Session is shutting down |

**6 states** — defined in `keel/protocol/protocol_flow.h`.

### 1.3 Backend Binding (`keel_backend_binding_t`)

Tracks session→backend ownership. **This is separate from session phase.** A session can be READY while UNBOUND; it can be in QUERY while SHARED; it can be CLOSING with a HARD_PINNED backend.

| Binding | Meaning |
|---------|---------|
| `UNBOUND` | No backend connection, session idle |
| `BORROW_PENDING` | Queued in pool waiting list |
| `SHARED` | Borrowed backend, returned on query complete |
| `PINNED_TXN` | Pinned due to active transaction |
| `PINNED_STATE` | Pinned due to session state (SET vars, profiled) |
| `PINNED_PS` | Pinned due to prepared statement mode (KEEL_PS_MODE_PINNING) |
| `HARD_PINNED` | Exclusive ownership (LISTEN, TEMP TABLE, CURSOR, etc.) |
| `CID_CHECK` | Using dedicated check connection for txid_status() |

**8 states** — defined in `keel/engine/state_machine.h`.

### 1.4 Backend Connection State (`backend_conn_state_t`)

Tracks the backend connection's own lifecycle, independent of which session owns it.

| State | Meaning |
|-------|---------|
| `IDLE` | In pool, available for borrowing |
| `ACTIVE` | Currently handling a query |
| `TXN_PINNED` | Pinned to session due to transaction |
| `STATE_PINNED` | Pinned due to SET variables or prepared statements |
| `SYNCING` | Synchronizing state before query |
| `CLEANING` | DISCARD ALL sent, awaiting response |
| `CLOSED` | Connection closed/failed |

**7 states** — defined in `keel/engine/backend_pool.h`.

### 1.5 TLS State (`keel_tls_state_t`)

Tracks TLS transport state per connection.

| State | Meaning |
|-------|---------|
| `INIT` | No TLS negotiation started |
| `HANDSHAKE` | TLS handshake in progress |
| `ESTABLISHED` | Userspace TLS active |
| `KTLS_ACTIVE` | Kernel TLS offloaded |
| `SHUTDOWN` | TLS shutdown initiated |
| `CLOSED` | TLS closed |
| `ERROR` | TLS error |

**7 states** — defined in `keel/protocol/tls_context.h`.

Splice eligibility is **derived** from TLS state + message path + platform support — never independently stored as mutable state.

### 1.6 Replay State (`keel_replay_state_t`)

Tracks prepared-statement replay lifecycle. Central correctness risk.

| State | Meaning |
|-------|---------|
| `NONE` | No replay needed |
| `DISCARD_PENDING` | Backend has stale stmts, DISCARD ALL required first |
| `DISCARD_SENT` | DISCARD ALL sent, awaiting ReadyForQuery |
| `SENDING` | Parse messages being written to backend |
| `WAITING` | Waiting for ParseComplete responses |
| `RFQ_PENDING` | All ParseCompletes received, draining Sync's ReadyForQuery |
| `COMPLETE` | Replay finished, ready to forward original message |

**7 states** — defined in `keel/engine/state_machine.h`.

### 1.7 Commit-in-Doubt State (`keel_cid_state_t`)

Tracks the replication uncertainty lifecycle. Too important for scattered booleans.

| State | Meaning |
|-------|---------|
| `NONE` | No commit uncertainty |
| `TRACKING` | Transaction tracking active, XID will be captured |
| `XID_CAPTURED` | txid_current() captured, COMMIT forwarded |
| `COMMIT_SENT` | COMMIT in flight, waiting for response |
| `BACKEND_LOST` | Backend died while COMMIT in flight |
| `CHECK_BORROWING` | Borrowing clean connection for txid_status() |
| `CHECK_SENT` | txid_status() query sent to new primary |
| `RESOLVED_COMMITTED` | XID confirmed committed |
| `RESOLVED_ABORTED` | XID confirmed aborted |
| `RESOLVED_UNKNOWN` | XID status could not be determined |

**10 states** — defined in `keel/engine/state_machine.h`.

### 1.8 Engine Lifecycle (`keel_engine_state_t`)

| State | Meaning |
|-------|---------|
| `CREATED` | Engine allocated, not yet started |
| `ACTIVE` | Running, accepting connections |
| `DRAINING` | Rejecting new connections, finishing active sessions |
| `STOPPING` | Workers signaled to exit |
| `STOPPED` | All workers joined, engine idle |

**5 states** — defined in `keel/engine/engine.h`.

### 1.9 Pin Model (`keel_flow_pin_reason_t`)

Pin reasons use a **bitmask** because multiple pins CAN legitimately coexist (e.g., TRANSACTION + PREPARED_STMT). The invariant checker validates legal combinations via `keel_pins_consistent()`.

**16 pin reasons** — defined in `keel/protocol/protocol_flow.h`. Illegal combinations detected at runtime by `keel_invariant_check_session()`.

---

## 2. Session Contract

The `keel_session_contract_t` provides a **single authoritative view** of a session's allowed behavior, aggregated from the scattered state fields.

```c
typedef struct keel_session_contract {
    keel_session_phase_t       phase;        /* From session_flow.phase */
    keel_backend_binding_t     binding;      /* Derived from backend ownership state */
    keel_flow_pin_reason_t     pins;         /* From session_flow.pins */
    keel_tx_status_t           tx;           /* From session_flow.tx */
    keel_replay_state_t        replay;       /* From replay-related fields */
    keel_cid_state_t           cid;          /* From CID-related fields */
    keel_engine_state_t        engine_state; /* Snapshot of engine lifecycle */
    uint32_t                   invariant_flags; /* Result of last invariant check */
} keel_session_contract_t;
```

Every hot-path decision: **consult contract → perform transition → revalidate invariants.**

---

## 3. Backend Contract

The `keel_backend_contract_t` provides an authoritative view of a backend connection.

```c
typedef struct keel_backend_contract {
    backend_conn_state_t       conn_state;
    keel_quarantine_reason_t   quarantine;     /* NONE or reason for quarantine */
    bool                       has_owner;      /* pinned_session != NULL */
    bool                       has_stmts;      /* stmt_set_hash != 0 */
    bool                       needs_sync;     /* State sync required before use */
} keel_backend_contract_t;
```

---

## 4. Invariants

### 4.1 Ownership Invariants

| ID | Invariant | Domains |
|----|-----------|---------|
| O1 | Session in QUERY/BACKEND_SYNC phase must have binding != UNBOUND | Phase × Binding |
| O2 | Session in READY phase may only be UNBOUND or hold a PINNED binding | Phase × Binding |
| O3 | Backend in ACTIVE must have exactly one owning session | Backend × Session |
| O4 | Backend in IDLE must have no owning session | Backend × Session |
| O5 | Session may not own two normal backends; dual-ref only for CID_CHECK | Binding |

### 4.2 Binding Invariants

| ID | Invariant | Domains |
|----|-----------|---------|
| B1 | PINNED_TXN requires tx == ACTIVE or tx == FAILED | Binding × TX |
| B2 | HARD_PINNED forbids migration | Binding × Migration |
| B3 | UNBOUND required for migration eligibility | Binding × Migration |
| B4 | CID_CHECK requires cid == CHECK_BORROWING or CHECK_SENT | Binding × CID |

### 4.3 Replay Invariants

| ID | Invariant | Domains |
|----|-----------|---------|
| P1 | replay != NONE requires binding != UNBOUND | Replay × Binding |
| P2 | replay == WAITING requires stmt_replay_count > 0 | Replay × internal |
| P3 | replay active during COPY is forbidden | Replay × Session |
| P4 | replay active during failed TX is forbidden | Replay × TX |

### 4.4 CID Invariants

| ID | Invariant | Domains |
|----|-----------|---------|
| C1 | cid == XID_CAPTURED requires pending_commit_xid != 0 | CID × internal |
| C2 | cid == CHECK_SENT requires xid_check_conn != NULL | CID × Backend |
| C3 | cid == NONE implies no active XID check connection | CID × Backend |
| C4 | cid in doubt state cannot be force-closed during drain | CID × Engine |

### 4.5 TLS Invariants

| ID | Invariant | Domains |
|----|-----------|---------|
| T1 | kTLS active implies handshake completed successfully | TLS Internal |
| T2 | PLAINTEXT illegal if TLS mode is REQUIRE | TLS × Config |
| T3 | splice_eligible must derive from TLS state + message path | TLS × Derived |

### 4.6 Drain Invariants

| ID | Invariant | Domains |
|----|-----------|---------|
| D1 | Engine DRAINING → no new sessions may reach READY | Engine × Phase |
| D2 | Force-close must not close CID-protected sessions | Engine × CID |

All invariants from §3 of `keel/engine/invariant.h` (20 violations) remain in force. The invariants above extend them with contract-level checks.

---

## 5. Transition Table — Session Phase

| Current Phase | Event | Preconditions | Next Phase | Side Effects |
|---------------|-------|---------------|------------|--------------|
| HANDSHAKE_AUTH | Auth complete | — | READY | Set binding=UNBOUND |
| READY | Query needs backend | engine ACTIVE | QUERY | Enqueue borrow (binding→BORROW_PENDING) |
| READY | Rebalance triggered | Can migrate | CLOSING | Migration send |
| QUERY | Backend borrowed | — | BACKEND_SYNC or QUERY | Bind backend (binding→SHARED) |
| BACKEND_SYNC | Sync complete | State matches | QUERY | Forward pending msg |
| QUERY | RFQ seen, no pins | tx==IDLE | READY | Return backend (binding→UNBOUND) |
| QUERY | RFQ seen, pinned | tx!=IDLE or pins set | READY | Keep backend (binding→PINNED_*) |
| QUERY | COMMIT sent + tracking | — | QUERY | CID→COMMIT_SENT |
| QUERY | Backend died mid-COMMIT | commit_in_flight | QUERY | CID→BACKEND_LOST |
| QUERY | CID check complete | — | READY | Synthesize response |
| QUERY | COPY begins | — | QUERY | Pin COPY |
| ANY | Client disconnect | — | CLOSING | Cleanup backend |
| ANY | Error | — | CLOSING | Log + close |

## 6. Transition Table — Backend Binding

| Current Binding | Event | Preconditions | Next Binding | Side Effects |
|-----------------|-------|---------------|--------------|--------------|
| UNBOUND | Pool queue | — | BORROW_PENDING | Add to wait queue |
| BORROW_PENDING | Backend available | — | SHARED | Assign backend_conn |
| SHARED | Begin TX | — | PINNED_TXN | Set in_transaction |
| SHARED | RFQ, tx==IDLE | No pins | UNBOUND | Return to pool |
| PINNED_TXN | COMMIT/ROLLBACK | — | SHARED | Clear in_transaction |
| PINNED_TXN | SET detected | — | PINNED_TXN | Also track state |
| SHARED | Hard pin trigger | LISTEN/TEMP/CURSOR | HARD_PINNED | Set hard_pinned |
| HARD_PINNED | Disconnect | — | UNBOUND | Return/close backend |
| ANY_PINNED | Backend lost + CID | commit_in_doubt | CID_CHECK | Borrow check conn |
| CID_CHECK | Check complete | — | UNBOUND | Release check conn |

## 7. Transition Table — Replay State

| Current | Event | Next | Side Effects |
|---------|-------|------|--------------|
| NONE | Borrowed backend, hash mismatch + needs_discard | DISCARD_PENDING | — |
| NONE | Borrowed backend, hash mismatch, no discard | SENDING | Buffer Parse msgs |
| DISCARD_PENDING | DISCARD ALL sent | DISCARD_SENT | — |
| DISCARD_SENT | ReadyForQuery received | SENDING | Forward Parse msgs |
| SENDING | All Parse msgs sent | WAITING | Track expected count |
| WAITING | All ParseComplete received | RFQ_PENDING | — |
| RFQ_PENDING | ReadyForQuery drained | COMPLETE | — |
| COMPLETE | Original msg forwarded | NONE | Clear replay state |

## 8. Transition Table — CID State

| Current | Event | Next | Side Effects |
|---------|-------|------|--------------|
| NONE | txn_tracking + BEGIN | TRACKING | — |
| TRACKING | txid_current() captured | XID_CAPTURED | Store XID |
| XID_CAPTURED | COMMIT forwarded | COMMIT_SENT | commit_in_flight=true |
| COMMIT_SENT | CommandComplete(COMMIT) | NONE | Happy path |
| COMMIT_SENT | Backend lost | BACKEND_LOST | commit_in_doubt=true |
| BACKEND_LOST | Borrow check conn | CHECK_BORROWING | — |
| CHECK_BORROWING | Check conn ready | CHECK_SENT | Send txid_status() |
| CHECK_SENT | Result: committed | RESOLVED_COMMITTED | Synthesize COMMIT |
| CHECK_SENT | Result: aborted | RESOLVED_ABORTED | Synthesize error |
| CHECK_SENT | Result: unknown | RESOLVED_UNKNOWN | Report to client |
| RESOLVED_* | Response sent | NONE | Cleanup |

---

## 9. Derived Predicates

These functions are the **only source of truth** for eligibility decisions. They are derived from the contract, never independently stored.

| Predicate | Derives From |
|-----------|-------------|
| `keel_session_can_migrate()` | phase==READY, binding==UNBOUND, pins==NONE, no residual |
| `keel_session_can_splice()` | TLS state (KTLS_ACTIVE), no pending replay, not in COPY parse |
| `keel_session_can_force_close()` | cid not in doubt states |
| `keel_session_needs_replay()` | Backend stmt_set_hash mismatch with session |
| `keel_backend_can_reuse()` | conn_state==IDLE, quarantine==NONE, not in_transaction |

---

## 10. Event Journal

Debug/hardening builds maintain a rolling event log per session (64 entries):

```c
typedef struct keel_state_event {
    uint64_t    ts_ns;          /* Monotonic timestamp */
    uint32_t    session_id;     /* Session or backend ID */
    uint8_t     domain;         /* Which state domain changed */
    uint8_t     old_state;      /* Previous state value */
    uint8_t     new_state;      /* New state value */
    uint8_t     event_type;     /* Trigger event */
} keel_state_event_t;
```

The journal enables post-mortem analysis of: protocol desync, dirty reuse, orphaned transactions, unexpected RFQ, CID ambiguity, migration failures.

---

## 11. Quarantine Contract

Backends suspected of corruption or ambiguous state are quarantined with explicit reason tracking:

| Reason | Meaning |
|--------|---------|
| `DIRTY_STATE` | Unknown SET/session state |
| `REPLAY_MISMATCH` | PS replay did not produce expected hash |
| `PROTOCOL_DESYNC` | Unexpected message sequence |
| `TLS_MISMATCH` | TLS state inconsistency |
| `FAILED_DISCARD` | DISCARD ALL failed or timed out |
| `FAILED_SYNC` | State sync failed |

Quarantined backends: cleanup attempt → close. Never general reuse. Metrics via quarantine reason counters.

---

## 12. Testing & Verification

The state model is covered by a four-layer test pyramid.

### 12.1 Contract Unit Tests (`test_state_contracts`)

218 assertions / 15 sections exercising:

- Derived enum round-trips for all 9 state domains
- `keel_transition_*()` accept every legal edge and reject every illegal edge
- `keel_derive_session_contract()` produces correct contracts from synthetic `session_flow` structs
- `keel_invariant_check_session()` fires all expected invariant bits
- Event journal ring-buffer record/wrap/clear
- Derived predicates (`can_migrate`, `can_splice`, `can_force_close`, `needs_replay`, `can_reuse`) agree with manually constructed states

### 12.2 Exhaustive Sequence Walk (`test_sm_sequence_walk`)

289 assertions / 14 tests:

- **DFS of every legal edge**: phase (13 edges across 6×6 matrix), replay (full chain + abort), CID (happy path + doubt + all 3 resolution branches)
- **Illegal edge exhaustive rejection**: phase (17 illegal), replay (24 derivable), CID (66 derivable). Every non-legal pair in each matrix is exercised.
- **Combined lifecycle walks**: all bind types (SHARED / PINNED_TXN / PINNED_STATE / PINNED_PS / HARD_PINNED), 10× transaction round-trips, hard-pin upgrade, quarantine cycle

### 12.3 Fuzz / Property-Based (`test_sm_fuzz`)

AFL++/libfuzzer dual-purpose harness, 2 304+ deterministic inputs:

- `LLVMFuzzerTestOneInput` interprets byte pairs as `(opcode, argument)` driving 9 opcodes: PHASE, BIND, UNBIND, BEGIN_TXN, END_TXN, REPLAY, CID, HARD_PIN, QUARANTINE
- Contract invariants validated after every successful transition
- Deterministic battery: 12 scenarios including all 9 × 256 opcode × argument pairs
- No dependency on test_utils — uses its own pass/fail counters, identical to `test_fuzz_harness.c`

### 12.4 Concurrent Stress (`test_sm_stress`)

5 tests, 64 threads each:

| Test | Purpose |
|------|---------|
| Independent lifecycle | HANDSHAKE → bind → 10× txn → unbind → CLOSING per thread, contract asserted mid-transaction |
| Journal stress | 192 events per thread, verifying ring-buffer wrap (`head == count`) |
| Contract derivation storm | 18 phase × tx configurations per thread |
| Thundering herd phase | Barrier-synchronized simultaneous phase transitions |
| Thundering herd lifecycle | Barrier-synchronized bind / txn / unbind |

### 12.5 Sanitizer Coverage

| Sanitizer | Result | Notes |
|-----------|--------|-------|
| ASAN + UBSAN | 48/48 pass | Address errors, buffer overflows, undefined behavior |
| TSAN | 48/48 pass | Data race detection; requires kernel ASLR ≤ 1 on some 6.x kernels (`sysctl vm.mmap_rnd_bits=28` workaround) |
| MSAN (clang) | Pass (with `-LE openssl`) | Uninstrumented OpenSSL produces expected false positives; exclude tests labelled `openssl` via `ctest -LE openssl` |
