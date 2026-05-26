# MySQL ↔ PostgreSQL parity for the engine vtable

Status: **phase A landed** (this commit) · phase B deferred to follow-up commit
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

## Phase B — deferred to a follow-up commit

Phase A only covers the narrow window where the OK packet for COMMIT
actually reaches KEEL. The longer window — "client connection dies any
time between issuing COMMIT and receiving its OK" — still falls back to
the engine's `NO_XID` behaviour.

Closing that gap means mirroring PostgreSQL's `kPgXidCommitMsg`
(`postgres_flow.c:2000`) approach:

1. **Negotiate `CLIENT_MULTI_STATEMENTS` and `CLIENT_MULTI_RESULTS`** in
   `myf_gen_startup` so the backend will accept a multi-statement payload.
2. **Rewrite the COMMIT payload** in `myf_on_fe_msg` to
   `SELECT @@gtid_executed AS _keel_token; COMMIT;` (or, more robust,
   `SELECT @@gtid_executed AS _keel_token /*before*/; COMMIT;
   SELECT @@gtid_executed AS _keel_token /*after*/;` to capture both
   sides of the commit boundary).
3. **Add a small result-set absorption state machine** (`MY_RS_GTID_PROBE_*`)
   in `myf_on_be_msg`:
   - Absorb the `_keel_token` result set silently (don't forward to the
     client).
   - Capture the token into `ctx->keel_write_gtid` and stash it in
     `commit_xid` before the COMMIT's own OK is forwarded.
   - **Rewrite the `seq_id`** on the forwarded COMMIT OK so the client
     observes the standard single-OK response with `seq_id = 1`.
4. **Engine integration.** The engine already consumes
   `be_act.commit_xid_captured`; no engine change is required.
5. **Tests.** Add cases for probe absorption, seq_id rewriting, and the
   handshake negotiating both multi-statement caps.

Until phase B lands the documented limitation in `CHANGELOG.md` applies:
mid-COMMIT connection death falls back to `NO_XID` behaviour, which is
identical to KEEL's pre-`79f1646` behaviour and is safe (it errs on the
side of declaring "unknown" rather than guessing).
