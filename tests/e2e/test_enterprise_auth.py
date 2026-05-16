"""
test_enterprise_auth.py — Enterprise authentication provider end-to-end tests
==============================================================================

Tests the four enterprise auth methods wired into the proxy session flow:

  - Trust mode          (no auth — baseline)
  - SCRAM-SHA-256       (challenge/response, PBKDF2-derived keys)
  - MD5                 (legacy, challenge/response)
  - LDAP                (cleartext relay to an OpenLDAP server)

Each test section spins up a separate KEEL instance configured for that
method.  The Docker Compose stack is assumed to already be running:

  docker compose -f docker/compose/pg-auth-test.yml up -d

Environment variables:
  KEEL_AUTH_HOST          Host running keel containers (default: 127.0.0.1)
  KEEL_AUTH_TRUST_PORT    Port for trust instance      (default: 25501)
  KEEL_AUTH_SCRAM_PORT    Port for SCRAM instance      (default: 25502)
  KEEL_AUTH_LDAP_PORT     Port for LDAP instance       (default: 25503)
  KEEL_AUTH_MD5_PORT      Port for MD5 instance        (default: 25504)
  KEEL_AUTH_TEST_TIMEOUT  per-connect timeout seconds  (default: 10)
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import contextlib
from pathlib import Path
from typing import Generator

import psycopg2
import psycopg2.errors
import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HOST    = os.environ.get("KEEL_AUTH_HOST",         "127.0.0.1")
PORT_TRUST = int(os.environ.get("KEEL_AUTH_TRUST_PORT", "25501"))
PORT_SCRAM = int(os.environ.get("KEEL_AUTH_SCRAM_PORT", "25502"))
PORT_LDAP  = int(os.environ.get("KEEL_AUTH_LDAP_PORT",  "25503"))
PORT_MD5   = int(os.environ.get("KEEL_AUTH_MD5_PORT",   "25504"))
TIMEOUT    = int(os.environ.get("KEEL_AUTH_TEST_TIMEOUT", "10"))

PG_DBNAME  = "testdb"

# Locate compose file relative to this test file
_REPO_ROOT    = Path(__file__).resolve().parent.parent.parent
_AUTH_COMPOSE = str(_REPO_ROOT / "docker" / "compose" / "pg-auth-test.yml")

# Allow extra time for the auth compose stack to start (Docker + KEEL init)
pytestmark = pytest.mark.timeout(300)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wait_for_port(host: str, port: int, timeout: float = 30.0) -> None:
    """Block until TCP port is accepting connections (or raise TimeoutError)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Port {host}:{port} did not become reachable within {timeout}s"
                )
            time.sleep(0.5)


def _wait_for_keel_ready(host: str, port: int, timeout: float = 60.0) -> None:
    """Wait until KEEL is actually ready to handle postgres protocol connections.

    TCP port reachability is not enough — KEEL needs to warm its backend pool
    before it can respond to client connections.  We probe by attempting a
    real psycopg2 connection (with an intentionally wrong password so that
    auth is skipped for trust mode and rejected quickly for others) and retry
    until we get any response that is *not* a timeout.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname=PG_DBNAME,
                user="postgres", password="readiness_probe",
                connect_timeout=3,
            )
            conn.close()
            return  # Connected successfully (trust mode)
        except psycopg2.OperationalError as exc:
            msg = str(exc)
            # Any response that isn't a timeout means KEEL is ready
            if "timeout" not in msg.lower():
                return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"KEEL at {host}:{port} did not become ready within {timeout}s"
            )
        time.sleep(0.5)


def _connect(port: int, user: str, password: str, *, dbname: str = PG_DBNAME,
             connect_timeout: int = TIMEOUT) -> psycopg2.extensions.connection:
    """Open a psycopg2 connection to keel on the given port."""
    return psycopg2.connect(
        host=HOST,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=connect_timeout,
    )


def _connect_fails(port: int, user: str, password: str,
                   dbname: str = PG_DBNAME) -> bool:
    """Return True if the connection is rejected (auth failure or refused)."""
    try:
        conn = _connect(port, user, password, dbname=dbname)
        conn.close()
        return False
    except (psycopg2.OperationalError, psycopg2.errors.InvalidPassword):
        return True


def _simple_query(conn: psycopg2.extensions.connection) -> str:
    """Run SELECT 1 and return the result as a string."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        row = cur.fetchone()
    return str(row[0]) if row else ""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def wait_for_keel_instances() -> Generator[None, None, None]:
    """Start the pg-auth-test compose stack and wait for all four KEEL auth instances."""
    # Start (or ensure running) the auth compose stack
    proc = subprocess.run(
        ["docker", "compose", "-f", _AUTH_COMPOSE,
         "up", "-d", "--remove-orphans"],
        capture_output=True, text=True, timeout=180,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"pg-auth-test compose up failed (rc={proc.returncode}):\n"
            f"{proc.stderr[-2000:]}"
        )

    # Wait for all four instances to be reachable and ready
    for port, name in [
        (PORT_TRUST, "trust"),
        (PORT_SCRAM, "scram"),
        (PORT_LDAP,  "ldap"),
        (PORT_MD5,   "md5"),
    ]:
        try:
            _wait_for_port(HOST, port, timeout=120.0)
            _wait_for_keel_ready(HOST, port, timeout=60.0)
        except TimeoutError:
            pytest.fail(
                f"keel-{name} not reachable at {HOST}:{port} after 180 s — "
                f"auth compose stack may have failed to start"
            )

    yield

    # Teardown: stop and remove the auth compose stack
    subprocess.run(
        ["docker", "compose", "-f", _AUTH_COMPOSE,
         "down", "-v", "--remove-orphans"],
        capture_output=True, timeout=60,
    )


# ---------------------------------------------------------------------------
# §1  Trust mode
# ---------------------------------------------------------------------------

class TestTrustAuth:
    """Trust mode — any client is accepted without a password."""

    def test_connect_with_any_password(self) -> None:
        """Trust mode accepts any password (including empty / wrong)."""
        conn = _connect(PORT_TRUST, "postgres", "wrong_password_does_not_matter")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_connect_with_empty_password(self) -> None:
        conn = _connect(PORT_TRUST, "postgres", "")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_connect_basic_query(self) -> None:
        conn = _connect(PORT_TRUST, "postgres", "")
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            row = cur.fetchone()
        conn.close()
        assert row is not None
        assert row[0] == PG_DBNAME


# ---------------------------------------------------------------------------
# §2  SCRAM-SHA-256
# ---------------------------------------------------------------------------

class TestScramAuth:
    """SCRAM-SHA-256 — full challenge/response handshake."""

    def test_valid_credentials_connect(self) -> None:
        """testuser with correct password should connect successfully."""
        conn = _connect(PORT_SCRAM, "testuser", "testpass123")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_second_user_valid(self) -> None:
        """testuser2 with correct password should also connect."""
        conn = _connect(PORT_SCRAM, "testuser2", "testpass456")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_wrong_password_rejected(self) -> None:
        """Wrong password must be rejected with an auth error."""
        assert _connect_fails(PORT_SCRAM, "testuser", "wrongpassword")

    def test_unknown_user_rejected(self) -> None:
        """Unknown user must be rejected."""
        assert _connect_fails(PORT_SCRAM, "nonexistent_user", "anypassword")

    def test_empty_password_rejected(self) -> None:
        """Empty password must be rejected."""
        assert _connect_fails(PORT_SCRAM, "testuser", "")

    def test_multiple_sequential_connections(self) -> None:
        """Multiple sequential SCRAM handshakes should all succeed."""
        for _ in range(5):
            conn = _connect(PORT_SCRAM, "testuser", "testpass123")
            assert _simple_query(conn) == "1"
            conn.close()

    def test_multiple_concurrent_connections(self) -> None:
        """Multiple concurrent connections should all authenticate correctly."""
        import threading
        errors: list[str] = []

        def worker() -> None:
            try:
                conn = _connect(PORT_SCRAM, "testuser", "testpass123")
                result = _simple_query(conn)
                conn.close()
                if result != "1":
                    errors.append(f"unexpected query result: {result!r}")
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Concurrent SCRAM failures: {errors}"


# ---------------------------------------------------------------------------
# §3  LDAP
# ---------------------------------------------------------------------------

class TestLdapAuth:
    """LDAP — ClearText relay to OpenLDAP; keel sends AuthenticationRequest(3)."""

    def test_valid_ldap_user_connects(self) -> None:
        """ldapuser with correct LDAP password should connect."""
        conn = _connect(PORT_LDAP, "ldapuser", "ldappass123")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_second_ldap_user_connects(self) -> None:
        conn = _connect(PORT_LDAP, "ldapuser2", "ldappass456")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_wrong_ldap_password_rejected(self) -> None:
        """Wrong password for a known LDAP user must be rejected."""
        assert _connect_fails(PORT_LDAP, "ldapuser", "wrongpassword")

    def test_unknown_ldap_user_rejected(self) -> None:
        """User not in LDAP must be rejected."""
        assert _connect_fails(PORT_LDAP, "nosuchldapuser", "anypassword")

    def test_empty_ldap_password_rejected(self) -> None:
        """Empty password must be rejected (anonymous bind is disabled)."""
        assert _connect_fails(PORT_LDAP, "ldapuser", "")

    def test_multiple_sequential_ldap_connections(self) -> None:
        """Multiple sequential connections must all verify successfully."""
        for _ in range(3):
            conn = _connect(PORT_LDAP, "ldapuser", "ldappass123")
            assert _simple_query(conn) == "1"
            conn.close()


# ---------------------------------------------------------------------------
# §4  MD5 (legacy)
# ---------------------------------------------------------------------------

class TestMd5Auth:
    """MD5 challenge/response — deprecated but still supported."""

    def test_valid_credentials_connect(self) -> None:
        conn = _connect(PORT_MD5, "testuser", "testpass123")
        assert _simple_query(conn) == "1"
        conn.close()

    def test_wrong_password_rejected(self) -> None:
        assert _connect_fails(PORT_MD5, "testuser", "wrongpassword")

    def test_unknown_user_rejected(self) -> None:
        assert _connect_fails(PORT_MD5, "nosuchuser", "anypassword")

    def test_multiple_connections(self) -> None:
        for _ in range(3):
            conn = _connect(PORT_MD5, "testuser", "testpass123")
            assert _simple_query(conn) == "1"
            conn.close()


# ---------------------------------------------------------------------------
# §5  Auth rejection sanity — wrong port / wrong method cross-checks
# ---------------------------------------------------------------------------

class TestAuthIsolation:
    """Each instance should only accept credentials valid for its own method."""

    def test_scram_creds_do_not_bypass_ldap(self) -> None:
        """A user from the SCRAM userlist must not be accepted on the LDAP instance
        unless they also exist in LDAP."""
        # 'testuser' is in the SCRAM userlist but NOT in LDAP → must fail
        assert _connect_fails(PORT_LDAP, "testuser", "testpass123")

    def test_trust_instance_does_not_bleed_into_scram(self) -> None:
        """A connection to the SCRAM instance with a bad password must fail even
        though another instance runs in trust mode."""
        assert _connect_fails(PORT_SCRAM, "postgres", "wrong_password")
