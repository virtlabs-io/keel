# KEEL — PostgreSQL Protocol & SQL Edge Cases

> Branch: `fix/sharding-test-readiness` — Updated: 2026-05-20

This document is the authoritative inventory of PostgreSQL wire-protocol
features and SQL patterns that KEEL either does not support or supports with
known restrictions.

Every entry follows the same structure:
1. **Observable behaviour** — what the client sees today.
2. **Root cause** — where in the engine the limitation lives.
3. **Severity** — how dangerous the failure mode is.
4. **Workaround** — what application authors can do today.
5. **Observability** — Prometheus counter / log message to watch.
6. **Production status** — whether the feature is safe to use behind a proxy.

---

## Table of Contents

- [Wire-Protocol Edge Cases](#wire-protocol-edge-cases)
  - [P1 — COPY BOTH (bidirectional streaming)](#p1--copy-both-bidirectional-streaming)
  - [P2 — Multiple result sets from a single Parse message](#p2--multiple-result-sets-from-a-single-parse-message)
  - [P3 — Row description changes between rebinds](#p3--row-description-changes-between-rebinds)
  - [P4 — `ParameterStatus` injection mid-session](#p4--parameterstatus-injection-mid-session)
  - [P5 — Streaming replication protocol (walsender)](#p5--streaming-replication-protocol-walsender)
  - [P6 — Cancel request with wrong PID/secret](#p6--cancel-request-with-wrong-pidsecret)
  - [P7 — SSL renegotiation](#p7--ssl-renegotiation)
  - [P8 — Large object protocol (`lo_*`)](#p8--large-object-protocol-lo_)
  - [P9 — Nested portals in a single Sync cycle](#p9--nested-portals-in-a-single-sync-cycle)
  - [P10 — Extended-query pipeline with multiple syncs](#p10--extended-query-pipeline-with-multiple-syncs)
- [SQL Edge Cases](#sql-edge-cases)
  - [S1 — Recursive CTEs through a scatter pool](#s1--recursive-ctes-through-a-scatter-pool)
  - [S2 — `LISTEN`/`NOTIFY` in a pooled session](#s2--listennotify-in-a-pooled-session)
  - [S3 — `CURSOR` declarations in transaction pooling](#s3--cursor-declarations-in-transaction-pooling)
  - [S4 — Advisory locks](#s4--advisory-locks)
  - [S5 — `SET LOCAL` / `SET` in transaction pooling](#s5--set-local--set-in-transaction-pooling)
  - [S6 — `PREPARE TRANSACTION` (2PC)](#s6--prepare-transaction-2pc)
  - [S7 — Temp tables across pooled connections](#s7--temp-tables-across-pooled-connections)
  - [S8 — `DO` blocks with COMMIT/ROLLBACK (PL/pgSQL)](#s8--do-blocks-with-commitrollback-plpgsql)
  - [S9 — `COPY FROM` with client data in extended protocol](#s9--copy-from-with-client-data-in-extended-protocol)
  - [S10 — Session-level configuration changes (`ALTER ROLE … SET`)](#s10--session-level-configuration-changes-alter-role--set)
- [Auth Edge Cases](#auth-edge-cases)
  - [A1 — GSSAPI / Kerberos](#a1--gssapi--kerberos)
  - [A2 — Certificate-only auth (clientcert=verify-full)](#a2--certificate-only-auth-clientcertverify-full)
  - [A3 — Channel binding (`SCRAM-SHA-256-PLUS`)](#a3--channel-binding-scram-sha-256-plus)
- [Client-Driver Known Restrictions](#client-driver-known-restrictions)

---

## Wire-Protocol Edge Cases

### P1 — COPY BOTH (bidirectional streaming)

**Observable behaviour.**  `COPY … (FORMAT BINARY)` using the `COPY_BOTH`
protocol mode (used by logical replication clients) is not proxied.  The
client receives a `FATAL: cannot COPY TO stdout in a non-interactive session`
or the connection is closed.

**Root cause.**  The proxy's `CopyOutResponse` / `CopyInResponse` handlers are
implemented.  `CopyBothResponse` (tag `W`) is not, because walsender sessions
use a fundamentally different session lifecycle (they never enter `ReadyForQuery`).

**Severity.**  Low for OLTP; fatal for applications attempting to use KEEL as
a proxy for logical replication consumers.

**Workaround.**  Connect logical replication clients directly to the primary,
bypassing KEEL.

**Observability.**  `keel_protocol_error_total{kind="unsupported_copy_both"}`.

**Production status.**  ❌ Unsupported. Do not use KEEL for replication slots.

---

### P2 — Multiple result sets from a single Parse message

**Observable behaviour.**  A single `Parse` message whose query string contains
`;`-separated statements (e.g. `SELECT 1; SELECT 2`) is forwarded to the
backend.  PostgreSQL returns only the first result set.  This is standard
PostgreSQL behaviour, but clients that expect multiple `RowDescription` /
`DataRow` sequences in a single extended-protocol cycle will see only the first.

**Root cause.**  The PostgreSQL extended protocol does not support multiple
statements in a single `Parse` message.  This is a PostgreSQL limitation, not
a KEEL limitation; KEEL correctly forwards the `Parse` unmodified.

**Workaround.**  Issue one `Parse + Bind + Execute` cycle per statement.

**Production status.**  ✅ Behaves identically to a direct PostgreSQL connection.

---

### P3 — Row description changes between rebinds

**Observable behaviour.**  If a prepared statement is created with one schema
and the underlying table is `ALTER TABLE`d while the statement is open, the
client may observe a stale `RowDescription` on subsequent `Bind + Execute`
cycles until the statement is explicitly `Close`d.

**Root cause.**  KEEL caches per-prepared-statement metadata during
virtualisation replay.  Schema changes are not tracked, consistent with how
PostgreSQL itself handles plan invalidation (PostgreSQL raises
`ERROR: cached plan must not change result type`).

**Workaround.**  Close and reopen prepared statements after DDL changes.
Applications should handle `ERROR 0A000: cached plan` and retry.

**Production status.**  ✅ Matches direct PostgreSQL behaviour.

---

### P4 — `ParameterStatus` injection mid-session

**Observable behaviour.**  `ParameterStatus` messages sent by the backend
mid-session (e.g. after `SET TimeZone = 'UTC'`) are forwarded to the client.
When SSV is enabled, the proxy also captures the new value for future pool
reuse.  If SSV is disabled, the parameter change is visible on the current
connection but will be lost when the backend is returned to the pool.

**Root cause.**  Without SSV, the proxy does not track backend-initiated
parameter changes.

**Workaround.**  Enable SSV (`session_context = track`) or use `RESET ALL`
explicitly in a cleanup transaction.

**Observability.**  `keel_ssv_parameter_capture_total`.

**Production status.**  ✅ SSV mode. ⚠️ Without SSV: session state is lost on pool cycle.

---

### P5 — Streaming replication protocol (walsender)

**Observable behaviour.**  Connections using `replication=database` or
`replication=true` in the startup message are not supported.  The proxy closes
the connection immediately with a protocol error.

**Root cause.**  The walsender protocol never enters `ReadyForQuery` state; the
proxy's session lifecycle cannot accommodate it.

**Workaround.**  Connect replication clients directly to the primary.

**Production status.**  ❌ Unsupported.

---

### P6 — Cancel request with wrong PID/secret

**Observable behaviour.**  A cancel request (`CancelRequest`) with an incorrect
PID or secret is silently discarded.  The proxy does not forward it to the
backend.

**Root cause.**  Correct behaviour per the PostgreSQL protocol spec: unknown
cancel keys must be ignored without response.

**Production status.**  ✅ Correct.

---

### P7 — SSL renegotiation

**Observable behaviour.**  Mid-session TLS renegotiation (triggered by
`SSL_renegotiate()` from a client TLS library) is not supported on the frontend
connection.  The session is terminated with a TLS alert.

**Root cause.**  kTLS sessions do not support renegotiation.  Even in non-kTLS
mode, mid-session renegotiation is deliberately unsupported to prevent
renegotiation attacks.

**Workaround.**  Clients should open a new connection if a key rotation is
required.

**Production status.**  ✅ Deliberate; improves security.

---

### P8 — Large object protocol (`lo_*`)

**Observable behaviour.**  `lo_open`, `lo_read`, `lo_write`, `lo_close` etc.
work correctly in transaction pooling mode **only if the full large-object
transaction is completed within a single connection lifetime**.  In pool
mode, a large-object handle created in one transaction cannot be used
in a subsequent transaction on a different backend.

**Root cause.**  Large-object handles are backend-local server-side state.
In transaction pooling mode, the backend changes between transactions.

**Workaround.**  Pin the session during large-object operations using an
explicit transaction (`BEGIN` … `COMMIT`).  The proxy's transaction-pinning
logic will keep the session on the same backend.

**Production status.**  ⚠️ Works when the operation is contained within a
single transaction.

---

### P9 — Nested portals in a single Sync cycle

**Observable behaviour.**  Issuing `Bind + Execute` for multiple portals
before a `Sync` message works correctly.  All `Execute` responses are collected
and forwarded before the proxy emits `ReadyForQuery`.

**Production status.**  ✅ Supported.

---

### P10 — Extended-query pipeline with multiple syncs

**Observable behaviour.**  Clients that pipeline multiple `Parse + Bind +
Execute + Sync` sequences without waiting for the response to each `Sync`
(pipeline mode — supported by psycopg3, libpq ≥ 14) work correctly through
the proxy.

**Production status.**  ✅ Supported. Tested by `suite_torture.py::I6`.

---

## SQL Edge Cases

### S1 — Recursive CTEs through a scatter pool

**Observable behaviour.**  `WITH RECURSIVE t AS (…) SELECT … FROM t` is
dispatched to all shards when no shard key is extractable.  Each shard
evaluates the recursion independently; the proxy concatenates rows.
Result: every value appears `N_SHARDS` times.

**Severity.**  High (silent wrong results).

**Workaround.**  Wrap in an explicit transaction: the proxy pins the session
to one backend.  Alternatively, `SET keel.route = primary` if your version
exposes session routing hints.

**Observability.**  `keel_scatter_unsupported_pattern_total{kind="recursive_cte"}`.

**Production status.**  ❌ Unsupported for sharded pools; ✅ safe in non-sharded pools.

---

### S2 — `LISTEN`/`NOTIFY` in a pooled session

**Observable behaviour.**  `LISTEN channel` pins the connection to the same
backend for the lifetime of the session.  `NOTIFY` is forwarded correctly.
A `LISTEN` session cannot be returned to the pool; the proxy keeps it pinned.

**Root cause.**  LISTEN requires a persistent backend connection to receive
asynchronous notifications.

**Workaround.**  Use a dedicated long-lived connection for LISTEN/NOTIFY, not a
pooled one.  The proxy correctly pins such connections; they just consume a
backend slot permanently.

**Production status.**  ⚠️ Works but consumes a backend slot for the session lifetime.

---

### S3 — `CURSOR` declarations in transaction pooling

**Observable behaviour.**  `DECLARE cursor_name CURSOR FOR SELECT …` creates
backend-local cursor state.  In transaction pooling mode, the cursor is
destroyed when the backend returns to the pool.  `FETCH` on the same cursor
in a subsequent transaction will fail with `ERROR: cursor "X" does not exist`.

**Workaround.**  Wrap DECLARE + FETCH + CLOSE in a single transaction.

**Production status.**  ⚠️ Requires explicit transaction pinning.

---

### S4 — Advisory locks

**Observable behaviour.**  Session-level advisory locks (`pg_advisory_lock()`)
are not safe in transaction pooling mode.  The lock is held by the backend
process; when the backend returns to the pool, the lock is released but the
client application may not be aware of this.

Transaction-level advisory locks (`pg_advisory_xact_lock()`) are safe because
they are released at transaction end, consistent with when the backend is
returned.

**Workaround.**  Use `pg_advisory_xact_lock()` instead of `pg_advisory_lock()`.

**Production status.**  ✅ Transaction-level. ❌ Session-level.

---

### S5 — `SET LOCAL` / `SET` in transaction pooling

**Observable behaviour.**  `SET LOCAL var = val` (transaction-scoped) works
correctly: the value is visible within the transaction and automatically reset
when the transaction ends and the backend is returned to the pool.

`SET var = val` (session-scoped) is captured by SSV when SSV is enabled.
Without SSV, the value is lost on pool cycle.

**Production status.**  ✅ SET LOCAL. ✅ SET with SSV. ⚠️ SET without SSV.

---

### S6 — `PREPARE TRANSACTION` (2PC)

**Observable behaviour.**  `PREPARE TRANSACTION 'name'` is supported within
the distributed 2PC framework for sharded writes.  For non-sharded connections,
`PREPARE TRANSACTION` is forwarded to the backend; the prepared transaction
survives backend recycling.

**Risk.**  If the client disconnects after `PREPARE TRANSACTION` but before
`COMMIT PREPARED` or `ROLLBACK PREPARED`, the proxy enters commit-in-doubt
state.  Operators must monitor `sessions_commit_in_doubt`.

**Production status.**  ✅ Supported within the 2PC framework. ⚠️ Requires operator monitoring.

---

### S7 — Temp tables across pooled connections

**Observable behaviour.**  Temp tables (`CREATE TEMP TABLE`) are backend-local.
In transaction pooling mode, a temp table created in one transaction is not
visible in subsequent transactions (they may land on a different backend).

**Workaround.**  Use `ON COMMIT DROP` to clean up temp tables within the
creating transaction.  For longer-lived temp state, use a regular table with
a session-unique name prefix, or pin the session.

**Production status.**  ⚠️ Use `ON COMMIT DROP` or session pinning.

---

### S8 — `DO` blocks with `COMMIT`/`ROLLBACK` (PL/pgSQL)

**Observable behaviour.**  `DO $$ BEGIN … COMMIT; … END $$` (procedural
COMMIT inside an anonymous block — supported in PostgreSQL ≥ 11) is
forwarded transparently.  The proxy detects the additional `COMMIT` responses
and does not return the backend to the pool prematurely.

**Production status.**  ✅ Supported.

---

### S9 — `COPY FROM` with client data in extended protocol

**Observable behaviour.**  `COPY table FROM STDIN` issued via the simple-query
protocol (`Query` message) works correctly.  `COPY FROM STDIN` issued via the
extended protocol (`Parse + Bind + Execute`) is not a supported PostgreSQL
pattern (PostgreSQL only supports `COPY FROM` in simple-query mode).

**Production status.**  ✅ Simple-query COPY FROM. ❌ Extended-protocol COPY FROM (unsupported by PostgreSQL itself).

---

### S10 — Session-level configuration changes (`ALTER ROLE … SET`)

**Observable behaviour.**  `ALTER ROLE user SET TimeZone = 'UTC'` affects all
future connections for that role, including pooled connections that are
reused after the change.  Because KEEL reuses backend connections, the
effective GUC value on a reused backend may not match the role default until
the backend is recycled.

**Workaround.**  Issue `SIGHUP` to KEEL after `ALTER ROLE … SET` operations to
drain the pool and force new backend connections that pick up the new defaults.

**Production status.**  ⚠️ Pool drain required after `ALTER ROLE … SET`.

---

## Auth Edge Cases

### A1 — GSSAPI / Kerberos

**Observable behaviour.**  `auth_type = gss` in `pg_hba.conf` is not supported
as a backend authentication method.  KEEL itself always authenticates to the
backend using SCRAM-SHA-256 or MD5.

**Workaround.**  Configure the backend to accept password-based auth for the
KEEL service account.  GSSAPI auth is still available for client-to-KEEL
connections via LDAP/PAM integration.

**Production status.**  ❌ GSSAPI backend auth unsupported.

---

### A2 — Certificate-only auth (`clientcert=verify-full`)

**Observable behaviour.**  Client certificate authentication (`cert` auth
method in `pg_hba.conf`) is supported for client-to-proxy connections via
mTLS.  For proxy-to-backend connections, KEEL presents its own certificate
(`backend_ssl_cert` / `backend_ssl_key`).

**Production status.**  ✅ mTLS on frontend. ✅ TLS client cert on backend.

---

### A3 — Channel binding (`SCRAM-SHA-256-PLUS`)

**Observable behaviour.**  `SCRAM-SHA-256-PLUS` (channel binding) is not
supported on the frontend.  Clients that advertise only `SCRAM-SHA-256-PLUS`
and refuse to fall back to `SCRAM-SHA-256` will fail authentication.

**Root cause.**  Channel binding binds the SCRAM exchange to the TLS channel,
which is terminated at the proxy.  A proxy that is transparent to TLS cannot
forward channel binding correctly.

**Workaround.**  Ensure the client's SCRAM method list includes `SCRAM-SHA-256`
(libpq includes it by default).

**Production status.**  ⚠️ SCRAM-SHA-256-PLUS rejected; SCRAM-SHA-256 works.

---

## Client-Driver Known Restrictions

| Driver | Version tested | Known restriction |
|--------|----------------|-------------------|
| libpq / psql | 14–17 | ✅ Full support |
| psycopg2 | 2.9.x | ✅ Full support |
| psycopg3 | 3.1.x | ✅ Full support incl. pipeline mode |
| asyncpg | 0.29.x | ✅ Full support |
| pgx v5 | 5.5.x | ✅ Full support |
| pgjdbc (JDBC) | 42.7.x | ✅ Full support; set `prepareThreshold` ≥ 1 to use server-side prepare |
| node-postgres (pg) | 8.11.x | ✅ Full support |
| Prisma | 5.x | ✅ Full support via `$queryRaw` and ORM queries |
| Hibernate ORM | 6.x | ✅ Full support; uses JDBC under the hood |
| SQLAlchemy | 2.x | ✅ Full support with psycopg2 or psycopg3 dialect |
| GORM (Go) | 1.25.x | ✅ Full support with pgx driver |
| Sequelize | 6.x | ✅ Full support |
| ActiveRecord (Rails) | 7.x | ✅ Full support |
| Diesel (Rust) | 2.x | ✅ Full support |

> Versions marked ✅ have been exercised through the torture suite
> (`suite_torture.py`) or the e2e test suite.  New driver versions should be
> validated with `suite_torture.py` before updating this table.
