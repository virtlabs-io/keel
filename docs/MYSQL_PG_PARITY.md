# MySQL ↔ PostgreSQL parity for the engine vtable

Status: **phase A landed** · **phase B withdrawn (unsound — see below)**
Scope: `src/protocol/mysql/mysql_flow.c`, `keel_proto_flow_vtable_t`
Owner: protocol team

---

## Background

KEEL's engine drives every protocol through `keel_proto_flow_vtable_t`. Four
hooks in that vtable are critical for the engine's "commit-in-doubt" and
"session reuse" decisions:

| Hook                       | Purpose                                                                                          |
| -------------------------- | ------------------------------------------------------------------------------------------------ |
| `notify_write_lsn`         | Plant the latest committed write-token in the context (for read-your-writes routing).            |
| `replica_reached_token`    | Decide whether a replica is fresh enough to serve a follow-up read.                              |
| `build_commit_doubt_check` | Build a one-shot probe a replacement backend can run to confirm the previous COMMIT actually durably committed. |
| `get_stmt_compat_profile`  | Describe the session's "shape" (role / db / GUCs) so the engine can refuse to reuse incompatible backends. |

On PostgreSQL all four are wired. On MySQL the hooks existed (commit
`79f1646`) but were not driven by the FE/BE wire-flow:

- `commit_xid_captured` was never set, so the engine always hit the
  `NO_XID` short-circuit at `engine_flow.c:3970` and never bothered to
  run a doubt-check.
- `get_stmt_compat_profile` always returned `semantic_unknown = true`,
  which is the conservative signal that disables backend reuse.

This document records what this commit changes and what is still missing.

---

## Phase A — landed in this commit

### Commit-in-doubt: post-COMMIT GTID capture

**Mechanism.** MySQL has no native `txid_status()`. Instead, when
`session_track_gtids = 'OWN_GTID'`, the server emits the just-committed
GTID inside the OK packet's `SESSION_TRACK_GTIDS` payload. The plugin
already parsed that payload into `ctx->keel_write_gtid` for
read-your-writes; we now also use it as the commit token.

**Wire flow.**

```
FE → BE:  COM_QUERY "COMMIT"          (myf_on_fe_msg arms commit_pending)
BE → FE:  OK + SERVER_SESSION_STATE_CHANGED + TRACK_GTIDS=<gtid>
                                       (myf_on_be_msg parses GTID, then,
                                        because commit_pending && query_complete,
                                        sets be_act.commit_xid = FNV64(gtid),
                                        be_act.commit_xid_captured = true)
```

**Error path.** If the OK is actually an ERR packet (deadlock, conflict,
etc.), the plugin clears `commit_pending` so a later unrelated round-trip's
GTID-bearing OK does not falsely look like a commit token.

**Doubt-check probe.** `myf_build_commit_doubt_check` formats the standard
MySQL recipe:

```sql
SELECT GTID_SUBSET('<captured-gtid>', @@global.gtid_executed)
```

The result is a single boolean: 1 → COMMIT was durable, 0 → not durable.
The hook validates the GTID against a strict charset (`[0-9a-fA-F:\-]`) to
prevent injection through `notify_write_lsn`.

### Statement-compat profile

`myf_get_stmt_compat_profile` now populates a real profile derived from
state the plugin already tracks:

| Field              | Source                                                                                       |
| ------------------ | -------------------------------------------------------------------------------------------- |
| `role_hash`        | FNV-1a-64 of the username from the handshake response.                                       |
| `search_path_hash` | FNV-1a-64 of the current database, refreshed on `COM_INIT_DB` and on `USE <db>`.             |
| `schema_epoch`     | Same as `search_path_hash` (every db switch is a schema epoch on MySQL).                     |
| `guc_hash`         | XOR-fold of `FNV64(key) ^ (FNV64(value) + 0x9E3779B97F4A7C15)` for every tracked GUC SET.    |
| `semantic_unknown` | Set true on the first untracked `SET` (e.g. user variables `SET @foo = …`).                  |

The tracked-GUC allowlist matches the set the plugin already replays via
`build_state_sync` so that two backends with equal `guc_hash` are
guaranteed to receive the same state-sync payload.

### Tests

`tests/test_mysql_protocol_flow.c` §23 (commit-in-doubt, 6 cases) and §24
(stmt-compat profile, 6 cases). All 373 cases pass; repo-wide ctest is
117/117 (e2e + pg_parse excluded as usual).

---

## Phase B — pre-COMMIT GTID probe (WITHDRAWN: unsound)

A previous iteration of this work shipped a pre-COMMIT GTID probe that
rewrote `COMMIT` into the multi-statement payload
`SELECT @@gtid_executed AS _keel_token;COMMIT`, captured the GTID set
returned by the embedded `SELECT`, and later resolved commit-in-doubt
episodes on a replacement backend via
`SELECT GTID_SUBSET('<captured_set>', @@global.gtid_executed)`. That
design is unsound and has been fully removed.

### Why it is unsound

The captured `@@gtid_executed` snapshot is taken **before** the new
commit lands. By construction it is therefore already a subset of any
later `@@global.gtid_executed`, so the doubt-check
`GTID_SUBSET(pre_commit_set, post_anything_set)` returns `1`
regardless of whether the in-flight COMMIT actually committed. A lost
COMMIT response would always be falsely resolved as
`RESOLVED_COMMITTED`. Equivalent to assuming success on every timeout.

### What was removed

From `src/protocol/mysql/mysql_flow.c`:

- `kMyXidCommitMsg` rewrite payload.
- FE-side COMMIT rewrite and the `txn_tracking` gate (struct field +
  worker plumbing inside the plugin, plus the test seam
  `keel_mysql_flow_test_enable_txn_tracking`).
- BE-side absorption state machine (column_count → column_def → EOF →
  data_row → EOF MORE_RESULTS) and the `seq_id` rewrite on the forwarded
  COMMIT OK / mid-probe ERR.

From `myf_gen_startup`:

- **Unconditional** advertisement of `CLIENT_MULTI_STATEMENTS` (bit 16)
  and `CLIENT_MULTI_RESULTS` (bit 17) in the handshake response. Those
  caps existed only to support the phase B rewrite. With phase B gone
  the proxy reverts to the upstream MySQL default of rejecting client
  multi-statement bundles, restoring per-session security semantics
  callers expect from a stock MySQL connection.

### Phase A tightening

With phase B removed, the only remaining capture path is phase A's
post-COMMIT SESSION_TRACK_GTIDS parse. That path was tightened so the
capture only fires when the OK packet **delivered a fresh
SESSION_TRACK_GTIDS entry in that same packet** (new local
`gtid_refreshed_this_packet` in `myf_on_be_msg`). The previous
implementation only checked `ctx->keel_write_gtid[0] != '\0'`, which
would have promoted a stale GTID from a prior round-trip or from
`notify_write_lsn()` (RYW intercept) into a commit token — reintroducing
the same soundness defect at a different layer.

### Behavioural contract after the rollback

- Commit-in-doubt resolution returns a token **only** when the COMMIT's
  OK packet carried a fresh `SESSION_TRACK_GTIDS` entry.
- If the OK never arrives, or the server does not have
  `session_track_gtids = OWN_GTID`, no token is captured and the engine
  reports the outcome as **UNKNOWN** (SQLSTATE `08006` / `40000`).
- The mid-COMMIT connection-death window therefore remains an
  acknowledged limitation. This is intentional: returning UNKNOWN is
  the only sound answer absent native MySQL XA or a server-side commit
  log replica can read.

### Negative test

`tests/test_mysql_protocol_flow.c` §25
(`test_stale_gtid_does_not_resolve_lost_commit`) plants a pre-existing
GTID via `notify_write_lsn()`, drives a COMMIT whose OK reply lacks
`SESSION_TRACK_GTIDS`, and asserts `commit_xid_captured == false` and
`commit_xid == 0`. This is the proof that a pre-existing GTID set
cannot resolve a lost COMMIT as committed.
