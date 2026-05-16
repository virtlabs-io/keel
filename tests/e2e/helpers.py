"""
helpers.py — Shared utility functions for the KEEL E2E test suite.
"""

from __future__ import annotations

import time
import threading
from typing import Any

import psycopg2
import psycopg2.extensions


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def pg_exec(conn: psycopg2.extensions.connection, sql: str, params=None) -> list[tuple]:
    """Execute *sql* and return all rows. Returns [] for non-SELECT statements."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        except psycopg2.ProgrammingError:
            return []


def pg_scalar(conn: psycopg2.extensions.connection, sql: str, params=None) -> Any:
    """Execute *sql* and return the first column of the first row, or None."""
    rows = pg_exec(conn, sql, params)
    return rows[0][0] if rows else None


def pg_count(conn: psycopg2.extensions.connection, table: str, where: str = "") -> int:
    """Return COUNT(*) for *table*, with an optional WHERE clause."""
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    return pg_scalar(conn, sql) or 0


# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------

def wait_until(
    predicate,
    timeout: float = 10.0,
    interval: float = 0.5,
    msg: str = "condition",
) -> bool:
    """Poll *predicate* every *interval* seconds until True or *timeout*."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Direct shard helpers
# ---------------------------------------------------------------------------

def shard_total_count(
    s0_conn: psycopg2.extensions.connection,
    s1_conn: psycopg2.extensions.connection,
    table: str,
    where: str = "",
) -> int:
    """Return the sum of COUNT(*) on both shards for *table*."""
    return pg_count(s0_conn, table, where) + pg_count(s1_conn, table, where)


def clear_table_on_shards(
    s0_conn: psycopg2.extensions.connection,
    s1_conn: psycopg2.extensions.connection,
    table: str,
) -> None:
    """DELETE all rows from *table* on both shard nodes directly."""
    for conn in (s0_conn, s1_conn):
        pg_exec(conn, f"DELETE FROM {table}")


# ---------------------------------------------------------------------------
# Concurrency helpers
# ---------------------------------------------------------------------------

class WorkerResult:
    """Collect per-thread results and errors from a concurrent test."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.successes: int = 0
        self.errors: list[Exception] = []

    def record_success(self) -> None:
        with self._lock:
            self.successes += 1

    def record_error(self, exc: Exception) -> None:
        with self._lock:
            self.errors.append(exc)

    @property
    def total(self) -> int:
        return self.successes + len(self.errors)


def run_concurrent(worker_fn, n_workers: int, *args, **kwargs) -> WorkerResult:
    """
    Launch *n_workers* threads, each calling ``worker_fn(result, *args, **kwargs)``.
    Returns a WorkerResult after all threads finish.
    """
    result = WorkerResult()
    threads = [
        threading.Thread(target=worker_fn, args=(result, *args), kwargs=kwargs)
        for _ in range(n_workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    return result


# ---------------------------------------------------------------------------
# Prometheus metric parsing
# ---------------------------------------------------------------------------

def parse_prometheus(text: str) -> dict[str, float]:
    """
    Parse Prometheus text format into a flat dict ``{metric_name: value}``.
    Only the last value for each name is kept (no label differentiation).
    """
    metrics: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) == 2:
            name_with_labels = parts[0].split("{")[0]
            try:
                metrics[name_with_labels] = float(parts[1])
            except ValueError:
                pass
    return metrics


def get_metric(metrics_text: str, name: str) -> float | None:
    """Return the current value of a named Prometheus metric, or None."""
    return parse_prometheus(metrics_text).get(name)
