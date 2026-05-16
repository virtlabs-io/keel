"""conftest.py — Fixtures for PostgreSQL integration tests.

Provides session-scoped Docker Compose stack fixtures for:
  - pg-streaming: 1 primary + 2 streaming replicas
  - pg-patroni:   3 Patroni nodes with etcd HA
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Generator

import psycopg2
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
COMPOSE_DIR = REPO_ROOT / "docker" / "compose"

PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DBNAME = "postgres"

# --------------------------------------------------------------------------- #
# Port constants (from compose files)
# --------------------------------------------------------------------------- #

STREAMING_PRIMARY_PORT = 5432
STREAMING_REPLICA1_PORT = 5433
STREAMING_REPLICA2_PORT = 5434

PATRONI_NODES = [
    {"pg": 5432, "api": 8008, "container": "pg-patroni-1"},
    {"pg": 5433, "api": 8009, "container": "pg-patroni-2"},
    {"pg": 5434, "api": 8010, "container": "pg-patroni-3"},
]
ETCD_PORT = 2379


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #

def _compose(compose_file: Path, *args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _wait_for_pg(
    port: int,
    host: str = "127.0.0.1",
    retries: int = 60,
    delay: float = 2.0,
) -> bool:
    for _ in range(retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DBNAME,
                connect_timeout=3,
            )
            conn.close()
            return True
        except psycopg2.Error:
            time.sleep(delay)
    return False


def _pg_conn(port: int) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=port,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DBNAME,
        connect_timeout=10,
    )
    conn.autocommit = True
    return conn


# --------------------------------------------------------------------------- #
# Streaming replication fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def pg_streaming_stack() -> Generator[dict, None, None]:
    """Start the pg-streaming compose stack; tear it down after the session."""
    compose_file = COMPOSE_DIR / "pg-streaming.yml"
    # Force-recreate to ensure clean state even if containers exist from a prior run
    proc = _compose(compose_file, "up", "-d", "--force-recreate", "--remove-orphans")
    if proc.returncode != 0:
        pytest.fail(f"docker compose up failed:\n{proc.stderr[-2000:]}")

    try:
        # Primary must be ready first
        if not _wait_for_pg(STREAMING_PRIMARY_PORT, retries=60, delay=2.0):
            pytest.fail("pg-streaming primary (port 5432) did not become ready within 120 s")

        # Wait for both replicas (they take base-backup from the primary)
        for port in (STREAMING_REPLICA1_PORT, STREAMING_REPLICA2_PORT):
            if not _wait_for_pg(port, retries=60, delay=2.0):
                pytest.fail(f"pg-streaming replica on port {port} did not become ready within 120 s")

        yield {
            "primary_port": STREAMING_PRIMARY_PORT,
            "replica1_port": STREAMING_REPLICA1_PORT,
            "replica2_port": STREAMING_REPLICA2_PORT,
        }
    finally:
        _compose(compose_file, "down", "-v", "--remove-orphans", timeout=90)


@pytest.fixture()
def primary_conn(pg_streaming_stack) -> Generator[psycopg2.extensions.connection, None, None]:
    conn = _pg_conn(pg_streaming_stack["primary_port"])
    yield conn
    conn.close()


@pytest.fixture()
def replica1_conn(pg_streaming_stack) -> Generator[psycopg2.extensions.connection, None, None]:
    conn = _pg_conn(pg_streaming_stack["replica1_port"])
    yield conn
    conn.close()


@pytest.fixture()
def replica2_conn(pg_streaming_stack) -> Generator[psycopg2.extensions.connection, None, None]:
    conn = _pg_conn(pg_streaming_stack["replica2_port"])
    yield conn
    conn.close()


# --------------------------------------------------------------------------- #
# Patroni fixtures
# --------------------------------------------------------------------------- #

def _wait_for_patroni_primary(
    nodes: list[dict],
    retries: int = 40,
    delay: float = 5.0,
) -> int | None:
    """Return the pg port of the node that responds to GET /primary."""
    import urllib.request
    import urllib.error

    for _ in range(retries):
        for node in nodes:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{node['api']}/primary", timeout=3
                ) as resp:
                    if resp.status == 200:
                        return node["pg"]
            except Exception:
                pass
        time.sleep(delay)
    return None


@pytest.fixture(scope="module")
def pg_patroni_stack() -> Generator[dict, None, None]:
    """Start the pg-patroni compose stack; tear it down after the module."""
    compose_file = COMPOSE_DIR / "pg-patroni.yml"
    proc = _compose(compose_file, "up", "-d", "--force-recreate", "--remove-orphans", timeout=60)
    if proc.returncode != 0:
        pytest.fail(f"docker compose up failed:\n{proc.stderr[-2000:]}")

    try:
        # Wait for node1's PG port — Patroni installs packages on first run,
        # so allow a generous wait (up to 10 minutes).
        if not _wait_for_pg(PATRONI_NODES[0]["pg"], retries=120, delay=5.0):
            pytest.fail("pg-patroni node1 (port 5432) did not become ready within 600 s")

        # Wait for a primary to be elected in the cluster
        primary_pg_port = _wait_for_patroni_primary(PATRONI_NODES, retries=40, delay=5.0)
        if primary_pg_port is None:
            pytest.fail("Patroni cluster did not elect a primary within 200 s")

        yield {
            "nodes": PATRONI_NODES,
            "primary_pg_port": primary_pg_port,
        }
    finally:
        _compose(compose_file, "down", "-v", "--remove-orphans", timeout=120)


@pytest.fixture()
def patroni_nodes(pg_patroni_stack) -> list[dict]:
    """Return the list of Patroni node descriptors."""
    return pg_patroni_stack["nodes"]


@pytest.fixture()
def patroni_primary_conn(
    pg_patroni_stack,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Psycopg2 connection to the current Patroni primary."""
    conn = _pg_conn(pg_patroni_stack["primary_pg_port"])
    yield conn
    conn.close()
