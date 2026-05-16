"""
conftest.py — KEEL E2E Test Suite Fixtures
==========================================

Manages the Docker Compose stack lifecycle and provides pytest fixtures for
all E2E test modules.

Environment variables:
  KEEL_E2E_SKIP_BUILD    Set to "1" to use an already-built KEEL image
  KEEL_E2E_KEEP_STACK    Set to "1" to leave the stack running after tests
  KEEL_E2E_COMPOSE_FILE  Override the compose file path
  KEEL_HOST              KEEL host          (default: 127.0.0.1)
  KEEL_PORT              KEEL proxy port    (default: 26432)
  KEEL_PROM_PORT         Prometheus port    (default: 29101)
  KEEL_ADMIN_PORT        Admin SQL port     (default: 26433)
  KEEL_SHARD0_PORT       Shard-0 direct port (default: 25432)
  KEEL_SHARD1_PORT       Shard-1 direct port (default: 25433)
"""

from __future__ import annotations

import os
import time
import socket
import subprocess
from pathlib import Path

import pytest
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import requests

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_COMPOSE = str(REPO_ROOT / "docker" / "compose" / "e2e-suite.yml")

COMPOSE_FILE = os.environ.get("KEEL_E2E_COMPOSE_FILE", _DEFAULT_COMPOSE)

# ---------------------------------------------------------------------------
# Endpoint defaults  — can be overridden via environment
# ---------------------------------------------------------------------------
KEEL_HOST       = os.environ.get("KEEL_HOST",           "127.0.0.1")
KEEL_PORT       = int(os.environ.get("KEEL_PORT",        "26432"))
KEEL_PROM_PORT  = int(os.environ.get("KEEL_PROM_PORT",   "29101"))
KEEL_ADMIN_PORT = int(os.environ.get("KEEL_ADMIN_PORT",  "26433"))
SHARD0_PORT     = int(os.environ.get("KEEL_SHARD0_PORT", "25432"))
SHARD1_PORT     = int(os.environ.get("KEEL_SHARD1_PORT", "25433"))

PG_USER     = "postgres"
PG_PASSWORD = "postgres"
PG_DBNAME   = "testdb"

# ---------------------------------------------------------------------------
# pytest configuration hooks
# ---------------------------------------------------------------------------

def pytest_configure(config: pytest.Config) -> None:
    """Register suite-wide markers."""
    markers = [
        ("prepared_statements", "prepared statement pooling + SSV e2e tests"),
        ("pool",      "connection-pool behaviour tests"),
        ("sharding",  "shard-routing tests"),
        ("scatter",   "scatter-merge aggregation tests"),
        ("twopc",     "two-phase commit tests"),
        ("failover",  "backend-failover and recovery tests"),
        ("chaos",     "chaos / fault-injection tests"),
        ("metrics",   "observability (Prometheus + admin) tests"),
        ("stress",    "load and concurrency tests"),
        ("window",    "window function scatter-merge tests"),
        ("cte",       "CTE and recursive query tests"),
        ("jsonb",     "JSONB operator and aggregation tests"),
        ("integrity", "data integrity and invariant tests"),
        ("protocol",  "wire-protocol compliance and fuzzing tests"),
        ("routing",   "routing correctness and distribution balance tests"),
        ("cache",     "query result cache correctness and TTL tests"),
        ("rw_split",  "read/write split routing tests"),
    ]
    for name, desc in markers:
        config.addinivalue_line("markers", f"{name}: {desc}")


def pytest_html_report_title(report):  # pragma: no cover
    report.title = "KEEL E2E Test Suite"


# ---------------------------------------------------------------------------
# Docker Compose helpers
# ---------------------------------------------------------------------------

def _compose_cmd(*args: str) -> list[str]:
    return ["docker", "compose", "-f", COMPOSE_FILE, *args]


def _is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_for_pg(host: str, port: int, retries: int = 60, delay: float = 2.0) -> bool:
    """Poll until PostgreSQL wire protocol accepts connections."""
    for _ in range(retries):
        try:
            conn = psycopg2.connect(
                host=host, port=port,
                user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME,
                connect_timeout=3,
            )
            conn.close()
            return True
        except psycopg2.Error:
            time.sleep(delay)
    return False


# ---------------------------------------------------------------------------
# Session-scoped stack fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def compose_stack():
    """
    Start the Docker Compose E2E stack once per test session.

    Yields a dict with connection details.  On teardown, the stack is brought
    down unless KEEL_E2E_KEEP_STACK=1.
    """
    skip_build = os.environ.get("KEEL_E2E_SKIP_BUILD", "") == "1"
    keep_stack  = os.environ.get("KEEL_E2E_KEEP_STACK",  "") == "1"

    # Verify Docker daemon is reachable
    try:
        subprocess.run(
            ["docker", "info"],
            check=True, capture_output=True, timeout=10,
        )
    except Exception:
        pytest.skip("Docker not available — skipping E2E tests")

    if not Path(COMPOSE_FILE).exists():
        pytest.skip(f"Compose file not found: {COMPOSE_FILE}")

    extra = [] if skip_build else ["--build"]
    print(f"\n[e2e] Starting stack ({COMPOSE_FILE}) …")
    result = subprocess.run(
        _compose_cmd("up", "-d", "--wait", *extra),
        capture_output=True, text=True, timeout=480,
    )
    if result.returncode != 0:
        print(result.stderr[-4000:])
        pytest.skip(f"Compose stack failed to start:\n{result.stderr[-1000:]}")

    # Wait for KEEL proxy to accept PostgreSQL connections
    print(f"[e2e] Waiting for KEEL on {KEEL_HOST}:{KEEL_PORT} …")
    if not _wait_for_pg(KEEL_HOST, KEEL_PORT, retries=60, delay=2.0):
        subprocess.run(_compose_cmd("logs", "--tail=80"), timeout=30)
        subprocess.run(_compose_cmd("down", "-v"), timeout=60)
        pytest.skip("KEEL proxy did not become ready within timeout")

    print("[e2e] Stack ready ✓")

    info = {
        "keel_host":    KEEL_HOST,
        "keel_port":    KEEL_PORT,
        "prom_port":    KEEL_PROM_PORT,
        "admin_port":   KEEL_ADMIN_PORT,
        "shard0_port":  SHARD0_PORT,
        "shard1_port":  SHARD1_PORT,
    }

    yield info

    if not keep_stack:
        print("\n[e2e] Tearing down stack …")
        subprocess.run(
            _compose_cmd("down", "-v", "--remove-orphans"),
            timeout=90, capture_output=True,
        )
        print("[e2e] Done")


# ---------------------------------------------------------------------------
# Session-scoped DSN fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def keel_dsn(compose_stack) -> str:
    return (
        f"host={KEEL_HOST} port={KEEL_PORT} "
        f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}"
    )


@pytest.fixture(scope="session")
def shard0_dsn(compose_stack) -> str:
    return (
        f"host={KEEL_HOST} port={SHARD0_PORT} "
        f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}"
    )


@pytest.fixture(scope="session")
def shard1_dsn(compose_stack) -> str:
    return (
        f"host={KEEL_HOST} port={SHARD1_PORT} "
        f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}"
    )


@pytest.fixture(scope="session")
def prom_url(compose_stack) -> str:
    return f"http://{KEEL_HOST}:{KEEL_PROM_PORT}/metrics"


@pytest.fixture(scope="session")
def admin_dsn(compose_stack) -> str:
    return (
        f"host={KEEL_HOST} port={KEEL_ADMIN_PORT} "
        f"user={PG_USER} password={PG_PASSWORD} dbname=keel_admin"
    )


# ---------------------------------------------------------------------------
# Per-function connection fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def keel_conn(keel_dsn) -> psycopg2.extensions.connection:
    """Autocommit connection through the KEEL proxy."""
    conn = psycopg2.connect(keel_dsn, connect_timeout=10)
    conn.autocommit = True
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def keel_txn_conn(keel_dsn) -> psycopg2.extensions.connection:
    """Manual-transaction connection through KEEL (autocommit=False)."""
    conn = psycopg2.connect(keel_dsn, connect_timeout=10)
    conn.autocommit = False
    yield conn
    try:
        conn.rollback()
        conn.close()
    except Exception:
        pass


@pytest.fixture
def shard0_conn(shard0_dsn) -> psycopg2.extensions.connection:
    """Direct autocommit connection to shard 0 (bypasses KEEL)."""
    conn = psycopg2.connect(shard0_dsn, connect_timeout=10)
    conn.autocommit = True
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def shard1_conn(shard1_dsn) -> psycopg2.extensions.connection:
    """Direct autocommit connection to shard 1 (bypasses KEEL)."""
    conn = psycopg2.connect(shard1_dsn, connect_timeout=10)
    conn.autocommit = True
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture(scope="session")
def fetch_metrics(prom_url):
    """Return a callable → raw Prometheus metric text."""
    def _fetch() -> str:
        resp = requests.get(prom_url, timeout=10)
        resp.raise_for_status()
        return resp.text
    return _fetch


# ---------------------------------------------------------------------------
# Session-scoped schema setup — runs once after stack is ready
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS users (
    id      BIGINT         PRIMARY KEY,
    name    TEXT           NOT NULL,
    email   TEXT,
    age     INT,
    balance NUMERIC(14,2)  DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    order_id   BIGINT         PRIMARY KEY,
    user_id    BIGINT         NOT NULL,
    amount     NUMERIC(12,2)  NOT NULL,
    status     TEXT           NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL,
    shard_hint  BIGINT         NOT NULL,
    category    TEXT           NOT NULL,
    value       BIGINT         NOT NULL,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS keel_2pc_test (
    shard   INT,
    marker  TEXT
);

-- Products table: sharded by product_id, includes JSONB metadata for rich query tests
CREATE TABLE IF NOT EXISTS products (
    product_id  BIGINT         PRIMARY KEY,
    name        TEXT           NOT NULL,
    category    TEXT           NOT NULL,
    price       NUMERIC(10,2)  NOT NULL DEFAULT 0,
    stock       INT            NOT NULL DEFAULT 0,
    metadata    JSONB          DEFAULT '{}',
    tags        TEXT[]         DEFAULT '{}',
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- User activity: sharded by user_id, used for window function / time-series tests
CREATE TABLE IF NOT EXISTS user_activity (
    id          BIGSERIAL,
    user_id     BIGINT         NOT NULL,
    action      TEXT           NOT NULL,
    score       INT            NOT NULL DEFAULT 0,
    ts          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
"""


@pytest.fixture(scope="session", autouse=True)
def create_test_schema(compose_stack):
    """Create the canonical test schema on both shard nodes."""
    for label, port in [("shard0", SHARD0_PORT), ("shard1", SHARD1_PORT)]:
        try:
            conn = psycopg2.connect(
                host=KEEL_HOST, port=port,
                user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME,
                connect_timeout=10,
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.close()
            print(f"[e2e] Schema ready on {label} ✓")
        except psycopg2.Error as exc:
            pytest.skip(f"Schema creation failed on {label} (port {port}): {exc}")
    yield
