"""
latency_sampler.py — Log outbound payload info (Python hook example)

Hook point: before_send

Logs the final routing decision, payload size, and SQL snippet just
before the query is forwarded to the backend.

Configuration (inside a worker group):
    [worker_group.<group>.hooks]
    hook.python.before_send.latency_sampler = \
        module=examples.hooks.python.latency_sampler \
        func=before_send priority=500
"""

import os
import datetime

ROUTE_NAMES = {0: "PRIMARY", 1: "REPLICA", 2: "ANY"}


def before_send(ctx: dict) -> tuple[bool, dict]:
    """
    Log payload info right before backend send.

    Parameters
    ----------
    ctx : dict
        Hook context — be_payload_len available at BEFORE_SEND point.

    Returns
    -------
    (True, ctx) — always continue.
    """
    logfile = os.environ.get("KEEL_SEND_LOG", "/tmp/keel_send.log")

    ts     = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sid    = ctx.get("session_id", 0)
    route  = ROUTE_NAMES.get(ctx.get("route_hint", 2), "UNKNOWN")
    plen   = ctx.get("be_payload_len", 0)
    splice = "splice" if ctx.get("splice_eligible") else "copy"
    sql    = (ctx.get("sql_text", "") or "").replace("\n", " ")[:200]

    line = f"{ts} | sid={sid:<6d} | → {route} | {plen} bytes | {splice} | {sql}\n"

    try:
        with open(logfile, "a") as f:
            f.write(line)
    except OSError:
        pass

    return True, ctx
