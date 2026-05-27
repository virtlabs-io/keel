# Parser Plugin Architecture

KEEL now has an explicit parser boundary:

```text
Frontend -> protocol decoder -> configured parser plugin -> semantic plan -> router/executor
```

The first builtin parser is `sql.postgresql`. It wraps the existing PostgreSQL
SQL analyzer and emits `keel_semantic_plan_t`, while also carrying a temporary
legacy query-tree bridge for current sharding/router code.

## Contracts

| Layer | Owns | Does not own |
|-------|------|--------------|
| Listener | sockets, TLS handshake, accept limits, listener selection | SQL grammar or routing semantics |
| Frontend plugin | wire protocol decode/encode, protocol state, parser selection | AST construction or grammar rules |
| Parser plugin | syntax, dialect classification, parser-owned AST, semantic plan | backend pool choice or primary/replica policy |
| Router | routing policy, shard/backend selection, pinning, failure behavior | raw grammar parsing or tokenization |

## Builtin Parser

`sql.postgresql` is registered through `keel_parser_builtin_postgresql_sql()`.
It supports:

| Input | Result |
|-------|--------|
| read-only PostgreSQL SQL | `KEEL_SEM_READ_ONLY`, `KEEL_SAFETY_SAFE_REPLICA` when no function call is present |
| DML | `KEEL_SEM_WRITE`, primary required |
| DDL | `KEEL_SEM_DDL`, primary and pin required |
| transaction control | `KEEL_SEM_TRANSACTION_CONTROL`, pin required |
| session state | `KEEL_SEM_SESSION_STATE`, pin required |
| function calls | conservative primary routing |
| invalid or empty SQL | parse error/incomplete with fail-closed safety |

Function calls are conservative by design. Until KEEL has trusted catalog
metadata for volatility and side effects, a function in a read-looking query
does not become replica-safe.

## Phase Status

| Phase | Status |
|-------|--------|
| Parser ABI and semantic plan structs | Implemented |
| Parser registry and builtin registration | Implemented |
| Builtin `sql.postgresql` parser plugin | Implemented |
| Router dispatch through parser contract | Started, with legacy query-tree bridge |
| Router consuming only `keel_semantic_plan_t` | Planned |
| Frontend-bound parser config | Planned |
| External `dlopen` parser plugins | Planned |
| Second parser prototype | Planned |

## Production Rules

- `sql.postgresql` is the only production parser target today.
- Parser failure must never route to a replica.
- Unknown or unsafe semantics must choose primary, pin, reject, or close
  according to configured policy.
- Parser AST memory is parser-owned and freed through `free_result()`.
- The current query-tree bridge is transitional and should shrink as routers
  move to `keel_semantic_plan_t`.

