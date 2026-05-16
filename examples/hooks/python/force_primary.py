"""
force_primary.py — Force specific users/databases to primary (Python hook example)

Hook point: before_route

Routes queries from certain users or targeting certain databases to the
primary, ensuring strong consistency (read-your-own-writes).

Configuration (inside a worker group):
    [worker_group.<group>.hooks]
    hook.python.before_route.force_primary = \
        module=examples.hooks.python.force_primary \
        func=route_check priority=100
"""

# Users who always go to primary
PRIMARY_USERS = {"admin", "migration", "replicator"}

# Databases that always go to primary
PRIMARY_DATABASES = {"config_db", "system"}

# Route constants (must match keel_hook_route_t)
ROUTE_PRIMARY = 0


def route_check(ctx: dict) -> tuple[bool, dict]:
    """
    Override routing for specific users/databases.

    Parameters
    ----------
    ctx : dict
        Hook context — route_hint and needs_primary are mutable.

    Returns
    -------
    (True, ctx) — always continue (just mutate routing fields).
    """
    user = ctx.get("username", "")
    db   = ctx.get("database", "")

    if user in PRIMARY_USERS or db in PRIMARY_DATABASES:
        ctx["route_hint"]    = ROUTE_PRIMARY
        ctx["needs_primary"] = True

    return True, ctx
