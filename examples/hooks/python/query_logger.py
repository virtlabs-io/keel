"""
query_logger.py — Log every query to a file (Python hook example)

Hook point: after_query_parse  (SQL text + classification available)

Writes one line per query to /tmp/keel_queries.log:
    timestamp | session | user@database | R/W | query_type | SQL

Configuration (inside a worker group):
    [worker_group.<group>.hooks]
    hook.python.after_query_parse.query_logger = \
        module=examples.hooks.python.query_logger \
        func=log_query priority=200
"""

import os
import datetime

# Query type enum → human-readable name  (must match keel_query_type_t)
QUERY_NAMES = {
    0: "UNKNOWN",
    1: "SELECT",    2: "SHOW",       3: "EXPLAIN",
    4: "INSERT",    5: "UPDATE",     6: "DELETE",
    7: "TRUNCATE",
    8: "CREATE",    9: "ALTER",      10: "DROP",
    11: "BEGIN",    12: "COMMIT",    13: "ROLLBACK",
    14: "SAVEPOINT",
    15: "SET",      16: "RESET",     17: "DISCARD",
    18: "PREPARE",  19: "EXECUTE",   20: "DEALLOCATE",
    21: "COPY",
}


def log_query(ctx: dict) -> tuple[bool, dict]:
    """
    Log the query to a text file.

    Parameters
    ----------
    ctx : dict
        Hook context with session info, query data, routing hints.
        See keel_hook_ctx_t in keel_hook.h for the full field list.

    Returns
    -------
    (True, ctx) — always continue processing (logging-only hook).
    """
    logfile = os.environ.get("KEEL_QUERY_LOG", "/tmp/keel_queries.log")

    ts       = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sid      = ctx.get("session_id", 0)
    user     = ctx.get("username", "?")
    db       = ctx.get("database", "?")
    rw       = "WRITE" if ctx.get("needs_primary") else "READ"
    qtype    = QUERY_NAMES.get(ctx.get("query_type", 0), "UNKNOWN")
    sql      = (ctx.get("sql_text", "") or "").replace("\n", " ")[:512]
    in_tx    = "TX" if ctx.get("in_transaction") else "--"
    effects  = f"0x{ctx.get('effect_flags', 0):04X}"
    qcount   = ctx.get("query_count", 0)

    line = (
        f"{ts} | sid={sid:<6d} | {user}@{db} | {rw:<5s} | {qtype:<10s} "
        f"| tx={in_tx} | effects={effects} | #{qcount} | {sql}\n"
    )

    try:
        with open(logfile, "a") as f:
            f.write(line)
    except OSError:
        pass  # silently skip if file can't be opened

    return True, ctx
