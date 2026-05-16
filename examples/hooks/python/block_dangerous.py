"""
block_dangerous.py — Abort dangerous queries (Python hook example)

Hook point: after_query_parse

Blocks:
    - DROP TABLE / DROP DATABASE
    - TRUNCATE
    - DELETE without WHERE (heuristic)
    - Any write from user "readonly"

Configuration (inside a worker group):
    [worker_group.<group>.hooks]
    hook.python.after_query_parse.block_dangerous = \
        module=examples.hooks.python.block_dangerous \
        func=check_query priority=50
"""

# Query type constants (must match keel_query_type_t enum)
QT_INSERT   = 4
QT_UPDATE   = 5
QT_DELETE   = 6
QT_TRUNCATE = 7
QT_DROP     = 10


def check_query(ctx: dict) -> tuple[bool, dict]:
    """
    Check for dangerous queries and abort them with an error message.

    Parameters
    ----------
    ctx : dict
        Hook context — mutable fields can be changed.

    Returns
    -------
    (False, ctx) to abort the query, (True, ctx) to continue.
    """
    qtype = ctx.get("query_type", 0)
    sql   = (ctx.get("sql_text", "") or "").upper()
    user  = ctx.get("username", "")

    # Block all writes from "readonly" user
    if user == "readonly" and ctx.get("needs_primary"):
        ctx["error_msg"] = "User 'readonly' is not allowed to execute write queries"
        return False, ctx

    # Block DROP
    if qtype == QT_DROP:
        ctx["error_msg"] = "DROP statements are blocked by security policy"
        return False, ctx

    # Block TRUNCATE
    if qtype == QT_TRUNCATE:
        ctx["error_msg"] = "TRUNCATE statements are blocked by security policy"
        return False, ctx

    # Block DELETE without WHERE
    if qtype == QT_DELETE and "WHERE" not in sql:
        ctx["error_msg"] = "DELETE without WHERE clause is blocked"
        return False, ctx

    return True, ctx
