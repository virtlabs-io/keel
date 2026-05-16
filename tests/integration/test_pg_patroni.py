"""test_pg_patroni.py — Patroni HA cluster integration tests.

Tests that a 3-node Patroni + etcd cluster is healthy: roles are correct,
REST probe endpoints return the expected status codes, and PostgreSQL accepts
connections on the primary node.

The compose stack (docker/compose/pg-patroni.yml) is started once per session
by the ``pg_patroni_stack`` fixture in conftest.py.

What is tested
--------------
1. **Health endpoints** — ``GET /health`` returns 200 on every node.
2. **Exactly one primary** — ``GET /primary`` returns 200 on exactly one node;
   all other nodes return a non-200 status.
3. **At least one replica** — ``GET /replica`` returns 200 on at least one
   node.
4. **Primary role in PG** — ``pg_is_in_recovery()`` on the primary's
   PostgreSQL port returns ``false``.
5. **Writes accepted** — An INSERT on the primary executes without error.
"""

from __future__ import annotations

import urllib.error
import urllib.request

import psycopg2
import pytest

pytestmark = [pytest.mark.patroni, pytest.mark.timeout(600)]

# Patroni REST API ports (matches conftest.PATRONI_NODES)
_API_PORTS = [8008, 8009, 8010]


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #

def _http_status(url: str) -> int:
    """Return the HTTP status code for a GET request, or 0 on connection error."""
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception:
        return 0


def _scalar(conn: psycopg2.extensions.connection, sql: str, params=()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row else None


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

class TestPatroniCluster:
    """Verify the pg-patroni compose cluster is healthy and properly elected."""

    def test_all_nodes_health_endpoint(self, patroni_nodes):
        """``GET /health`` must return 200 on every Patroni node (wait up to 120 s)."""
        import time
        # Patroni replicas need extra time after the primary is elected to
        # initialise the streaming connection and report /health 200.
        failures: list[str] = []
        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline:
            failures = []
            for node in patroni_nodes:
                url = f"http://127.0.0.1:{node['api']}/health"
                code = _http_status(url)
                if code != 200:
                    failures.append(
                        f"{node['container']} (:{node['api']}/health) → {code}"
                    )
            if not failures:
                return
            time.sleep(3)

        assert not failures, "Health endpoint failures after 120 s:\n" + "\n".join(failures)

    def test_exactly_one_primary(self, patroni_nodes):
        """``GET /primary`` must return 200 on exactly one node."""
        primaries = [
            node for node in patroni_nodes
            if _http_status(f"http://127.0.0.1:{node['api']}/primary") == 200
        ]
        assert len(primaries) == 1, (
            f"Expected exactly 1 primary via /primary endpoint, found {len(primaries)}: "
            + ", ".join(n["container"] for n in primaries)
        )

    def test_at_least_one_replica(self, patroni_nodes):
        """``GET /replica`` must return 200 on at least one node (wait up to 120 s)."""
        import time
        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline:
            replicas = [
                node for node in patroni_nodes
                if _http_status(f"http://127.0.0.1:{node['api']}/replica") == 200
            ]
            if len(replicas) >= 1:
                return
            time.sleep(3)
        assert False, (
            f"Expected ≥ 1 replica via /replica endpoint after 120 s, found 0"
        )

    def test_primary_count_plus_replica_count_equals_node_count(self, patroni_nodes):
        """Every node must identify as either primary or replica."""
        primaries = [
            n for n in patroni_nodes
            if _http_status(f"http://127.0.0.1:{n['api']}/primary") == 200
        ]
        replicas = [
            n for n in patroni_nodes
            if _http_status(f"http://127.0.0.1:{n['api']}/replica") == 200
        ]
        assert len(primaries) + len(replicas) == len(patroni_nodes), (
            f"{len(primaries)} primary(s) + {len(replicas)} replica(s) ≠ {len(patroni_nodes)} nodes — "
            "one or more nodes may be in an unclassified state"
        )

    def test_primary_pg_is_not_in_recovery(self, patroni_primary_conn):
        """The primary's PostgreSQL instance must not be in recovery mode."""
        in_recovery = _scalar(patroni_primary_conn, "SELECT pg_is_in_recovery()")
        assert in_recovery is False, (
            f"pg_is_in_recovery() on Patroni primary returned {in_recovery!r}; expected false"
        )

    def test_write_accepted_on_primary(self, patroni_primary_conn):
        """A simple DDL + DML round-trip must succeed on the primary."""
        _TEST_TABLE = "keel_patroni_probe"
        try:
            with patroni_primary_conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {_TEST_TABLE}"
                    " (id serial PRIMARY KEY, val text)"
                )
                cur.execute(
                    f"INSERT INTO {_TEST_TABLE} (val) VALUES (%s)",
                    ("patroni_probe",),
                )
                cur.execute(f"SELECT count(*) FROM {_TEST_TABLE} WHERE val = %s", ("patroni_probe",))
                count = cur.fetchone()[0]

            assert int(count) >= 1, f"Inserted row not found in {_TEST_TABLE}"
        finally:
            try:
                with patroni_primary_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {_TEST_TABLE}")
            except Exception:
                pass
