# Correctness Under Failure

This document is the v0.4-alpha correctness contract. KEEL's first production
story is not feature breadth; it is conservative PostgreSQL proxying and pooling
that fails closed when routing, state, parser, or failover certainty is missing.

KEEL should be positioned as:

```text
A correctness-first intelligent PostgreSQL proxy/pooler
with protocol awareness, semantic routing, and explicit state ownership.
```

The rule for every unsafe edge is simple:

```text
If KEEL is not certain, the safest route wins.
```

## Routing Safety Rules

| Situation | Required behavior |
|-----------|-------------------|
| Parser error or incomplete frame | Route to primary, reject, or close. Never choose a replica. |
| Unsupported syntax | Route to primary or reject with a PostgreSQL error response. |
| Unknown function or procedure | Route to primary. |
| Parser timeout or resource limit | Route to primary or reject. |
| Semantic ambiguity | Route to primary. |
| Parser plugin failure | Route to primary, reject, or close according to policy. |
| Session state uncertainty | Pin the session/backend until state is clean or close it. |
| Transaction uncertainty | Pin the transaction/backend; do not move it. |
| Failover uncertainty | Reject or pin; never silently replay ambiguous work. |

The router treats query-tree errors, partial parses, and function calls as not
replica-safe. A read-looking statement only reaches a replica when it is
explicitly classified as safe for a replica.

## PostgreSQL Semantic Hazards

The following classes are primary-bound or fail-closed until KEEL has stronger
catalog-backed proof:

```sql
SELECT nextval('x');
SELECT setval('x', 1);
SELECT pg_advisory_lock(1);
SELECT function_that_writes();

WITH x AS (
    DELETE FROM t RETURNING *
)
SELECT * FROM x;

COPY t FROM STDIN;
CREATE TEMP TABLE t(id int);
SET LOCAL statement_timeout = '1s';
LISTEN events;
NOTIFY events;
DO $$ BEGIN PERFORM 1; END $$;
```

The safe default is primary, pin, reject, or close. Replica routing requires
positive proof, not absence of obvious writes.

## Stability Tiers

| Area | v0.4-alpha tier | Release rule |
|------|-----------------|--------------|
| PostgreSQL proxy mode | Stable target | Must pass protocol, TLS, auth, and drain gates. |
| PostgreSQL pool mode | Stable target | Backend reuse requires protocol-confirmed idle state. |
| Smart read routing | Beta | Replica routing only after semantic safety and session cleanliness checks. |
| Prepared statement virtualization | Beta | Requires lifecycle tracking and invalidation tests per deployment. |
| Session virtualization | Experimental | Must not be a default production promise. |
| Sharding and scatter-gather | Experimental | Opt-in only, fail closed when disabled or ambiguous. |
| Multi-shard transactions | Experimental | Blocked on commit-in-doubt crash-recovery coverage. |
| Result cache | Experimental | Disabled by default and gated by `experimental_features`. |
| MySQL | Alpha | Keep parity tests, but do not position as the first production baseline. |
| GraphQL, MCP, natural language parsing | Research | Design and experiments only. |

## Ownership Rules

Exactly one subsystem owns each stateful object at a time:

| Object | Owner |
|--------|-------|
| Frontend connection | Worker |
| Backend socket | Backend pool while idle; owning session while borrowed/pinned |
| Parser AST | Parser plugin |
| Semantic plan | Router/core after parser result publication |
| Prepared statement registry | Session/prepared-statement manager |
| Portal registry | Session/protocol state |
| Transaction state | Protocol/session state machine |
| Temp tables | Owning backend/session binding |
| GUC/search_path/session authorization | Session state profile and SSV layer |
| Advisory locks and LISTEN state | Hard-pinned backend/session binding |
| Route cache | Route engine |
| Metrics snapshot | Telemetry subsystem |

A backend cannot return to the idle pool until every state domain is verified
clean or the backend is closed. Cleanup failure is a close reason, not a warning.

## State Machines

The canonical state-machine reference is [STATE_MODEL.md](STATE_MODEL.md). For
v0.4-alpha release review, the minimum state-machine diagrams and invariants are:

```text
Frontend:    startup -> auth -> ready -> parse -> bind -> execute -> sync -> ready
             error -> sync/ready or terminate

Backend:     idle -> borrowed -> dirty -> cleaning -> idle
             borrowed/dirty/cleaning -> failed -> closing

Transaction: idle -> in_tx -> failed_tx -> idle
             in_tx -> committing -> idle | unknown
             in_tx -> prepared_tx -> committing | failed_tx
```

Every transition must have one owner, one event, one resulting state, and one
observable reason code when it closes or rejects work.

## Prepared Statement Lifecycle

Prepared-statement virtualization remains beta until the runtime tracks and tests:

```text
client statement name
backend statement name
statement SQL hash
parameter types
semantic plan
backend ownership
generation/version
invalidations
```

Invalidation triggers include:

```text
DISCARD ALL
DEALLOCATE
DDL
search_path changes
role changes
temp schema changes
failover
backend death
prepared statement replacement
```

If replay or invalidation is not provably correct, KEEL must pin, reject, or
close. It must not forward queued client traffic after failed replay.

## Failover Semantics

Never replay ambiguous transactions. If KEEL cannot prove the outcome of COMMIT:

```text
disconnect or error the client
mark the transaction outcome unknown
require application-level retry or reconciliation
```

Role changes, timeline switches, Patroni outages, stale replica tokens, and
split-brain windows are conservative-routing events.

## Parser Plugin Containment

Parser plugins must publish a semantic plan without owning routing policy.
Before external plugins are production-supported, the ABI must expose hard
limits for:

```text
max parse time
max AST nodes
max allocations
max recursion depth
max output size
```

Plugin failures must not corrupt router state. A parser result is usable only if
`keel_semantic_plan_valid()` passes and the safety level permits the requested
route.

## Chaos and Replay Requirements

v0.4-alpha work should prioritize deterministic tests for:

- extended PostgreSQL protocol recovery: Parse/Bind/Execute, unnamed statements,
  named statements, portals, cursor fetch, Sync recovery, ErrorResponse recovery,
  CancelRequest, COPY, SASL/SCRAM, TLS errors;
- prepared-statement invalidation after DDL, search_path changes, role changes,
  temp schema changes, backend death, and replacement;
- failover during COMMIT with primary crash, network timeout, client disconnect,
  and replica promotion;
- backend cleanup for temp tables, GUCs, roles, locks, LISTEN, prepared
  statements, search_path, SET LOCAL, and advisory locks;
- parser differential tests comparing PostgreSQL behavior with KEEL semantic
  classification.

Every production bug should become a structured replay fixture containing
frontend messages, backend responses, route decisions, semantic classifications,
timing, worker assignment, and failover events, with secrets redacted.
The durable format and API are defined in
[OPERATIONAL_REPLAY_LOGS.md](OPERATIONAL_REPLAY_LOGS.md).

## Route Decision Explainability

Every route decision should be explainable through a structured record:

```json
{
  "query_hash": "0x81ab...",
  "route": "replica_2",
  "reason": [
    "read_only",
    "replica_safe_semantics",
    "replica_lag_ok",
    "session_clean"
  ]
}
```

When the decision is conservative, the explanation should say why, for example
`semantic_uncertainty`, `session_dirty`, `transaction_pinned`, or
`failover_uncertain`. The core router exposes stable route reason codes through
`keel_route_reason_name()` and can format a compact JSON record with
`keel_route_decision_to_json()` for logs, admin APIs, and replay fixtures.

## Public Claims Policy

Public documentation should use careful terms: correctness-first routing,
conservative fallback, explicit state ownership, observable routing decisions,
safe degradation, and fail-closed behavior. Avoid claims that imply uninterrupted
upgrades, invisible failover, complete statelessness, universal SQL coverage, or
self-proving correctness.

## Release Gate Command

Run deterministic correctness gates with:

```sh
scripts/run_correctness_gates.sh
```

Then run the live Docker chaos gate:

```sh
tests/chaos/run-chaos.sh
```
