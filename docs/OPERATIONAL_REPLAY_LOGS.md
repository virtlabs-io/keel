# Operational Replay Logs

Operational replay logs are the durable artifact used to turn production
correctness failures into deterministic regression fixtures.

The API lives in `include/keel/util/replay_log.h` and writes newline-delimited
JSON. It is protocol-neutral and redacted by construction: callers record
payload length and `payload_hash`, never raw frontend/backend bytes.

## Record Types

| Type | Purpose |
|------|---------|
| `frontend_message` | Frontend event boundary, payload hash, session and transaction IDs. |
| `backend_response` | Backend event boundary and response hash. |
| `route_decision` | Selected route, route reason, and query hash. |
| `semantic_plan` | Parser classification and safety level. |
| `state_transition` | Frontend/backend/transaction state movement. |
| `cid_transition` | Commit-in-doubt lifecycle and final outcome. |
| `failover` | Topology, role, timeline, or health transition. |
| `chaos_event` | Fault injection event from chaos tests. |
| `checkpoint` | Operator or test milestone. |

## Durability Rules

- Release and chaos runs should enable `fsync_each` so the last successful
  record survives process death.
- Raw SQL, passwords, tokens, bind values, and protocol bytes must not be stored.
- Use stable IDs and hashes: `session_id`, `transaction_id`, `connection_id`,
  `worker_id`, `query_hash`, and `payload_hash`.
- A COMMIT outcome that cannot be proven must be recorded as a CID transition
  with an unknown outcome and surfaced to the client; it must not be replayed.

## Fixture Workflow

1. Enable replay logs for a correctness test or production incident window.
2. Archive the NDJSON file with KEEL logs, metrics, topology events, and chaos
   script output.
3. Reduce the NDJSON into a focused regression fixture.
4. Add the fixture to the deterministic gates before closing the incident.

## Current Gate

`test_replay_log` verifies:

- append-only NDJSON output;
- durable flush/fsync path;
- payload redaction by hash and length;
- state-transition and commit-in-doubt records;
- maximum-size enforcement.
