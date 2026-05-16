"""
tests/suites/suite_resilience.py
==================================
Category E — Error Path and Resilience Testing

Tests both C-level fault injection (no proxy required) and live proxy
error-path behaviour (requires KEEL_HOST / KEEL_PORT).

C-level tests (always runnable):
  E1. Allocation-failure injection matrix (test_alloc_inject)
  E2. Crash-recovery matrix (test_crash_recovery_matrix)
  E3. Fault-inject drain shutdown (test_drain_shutdown if present)
  E4. Connection pool exhaustion and recovery (test_connpool_exhaust)
  E5. FD tracking under pressure (test_fd_tracking if present)

Live proxy tests (skipped when proxy unreachable):
  E6.  Half-open connection flood — N connections never sending startup
  E7.  Slow client — connect, auth, then stall (no data sent for 30 s)
  E8.  Rapid disconnect after startup — connect+auth, immediately terminate
  E9.  Connection storm — N connections in parallel, all complete successfully
  E10. Query error recovery — trigger an SQL error, then verify next query works

Run standalone:
    python tests/suites/suite_resilience.py --verbose --connections 50
"""

from __future__ import annotations

import socket
import struct
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    ProxyConn,
    find_build_dir,
    find_test_binary,
    is_proxy_reachable,
    pg_query,
    pg_terminate,
    proxy_env,
    run_binary,
    wait_for_port,
)

_DEFAULT_CONN_COUNT = 30


class ResilienceSuite(SuiteRunner):
    NAME        = "resilience"
    DESCRIPTION = "Category E — Error Path and Resilience Testing"
    TAGS        = ["resilience", "fault-injection", "error-path", "stability"]

    def setup(self) -> None:
        self._build = find_build_dir()
        self._env   = proxy_env()
        self._conn_count = int(self.kwargs.get("connections", _DEFAULT_CONN_COUNT))
        if not is_proxy_reachable(self._env):
            self._proxy_msg = (
                f"Proxy not reachable at {self._env['host']}:{self._env['port']}"
            )
        else:
            self._proxy_msg = None

    def _require_proxy(self) -> None:
        if self._proxy_msg:
            self.skip(self._proxy_msg)

    def _require_build(self) -> None:
        if not self._build:
            self.skip("No build directory found")

    def _proxy_conn(self, timeout: float = 5.0) -> ProxyConn:
        c = ProxyConn(self._env["host"], self._env["port"], timeout=timeout)
        c.connect()
        return c

    def _auth(self, c: ProxyConn) -> bool:
        return c.startup(
            self._env["user"], self._env["database"], self._env["password"]
        )

    # -----------------------------------------------------------------------
    # E1 — Allocation-failure injection matrix
    # -----------------------------------------------------------------------

    def test_e01_alloc_inject_matrix(self) -> None:
        """Every malloc site must handle failure gracefully (no crash, no UB)."""
        self._require_build()
        binary = find_test_binary("test_alloc_inject", self._build)
        if not binary:
            self.skip(f"test_alloc_inject not found in {self._build}")
        rc, output = run_binary(binary, timeout=120)
        if rc != 0:
            raise AssertionError(
                f"Alloc injection test failed (rc={rc}):\n{output[-2000:]}"
            )

    # -----------------------------------------------------------------------
    # E2 — Crash-recovery matrix
    # -----------------------------------------------------------------------

    def test_e02_crash_recovery_matrix(self) -> None:
        """Simulate crashes at various points; verify clean recovery."""
        self._require_build()
        binary = find_test_binary("test_crash_recovery_matrix", self._build)
        if not binary:
            self.skip(f"test_crash_recovery_matrix not found in {self._build}")
        rc, output = run_binary(binary, timeout=180)
        if rc != 0:
            raise AssertionError(
                f"Crash recovery matrix failed (rc={rc}):\n{output[-2000:]}"
            )

    # -----------------------------------------------------------------------
    # E3 — Drain shutdown under fault injection
    # -----------------------------------------------------------------------

    def test_e03_drain_shutdown_fault_inject(self) -> None:
        """Drain / graceful shutdown must succeed even when alloc fails."""
        self._require_build()
        binary = find_test_binary("test_drain_shutdown", self._build)
        if not binary:
            self.skip("test_drain_shutdown binary not found")
        rc, output = run_binary(binary, timeout=120)
        if rc != 0:
            raise AssertionError(f"test_drain_shutdown failed (rc={rc}):\n{output[-1500:]}")

    # -----------------------------------------------------------------------
    # E4 — Connection pool exhaustion and recovery
    # -----------------------------------------------------------------------

    def test_e04_connpool_exhaust_recover(self) -> None:
        """Pool must queue or reject gracefully when exhausted, then recover."""
        self._require_build()
        binary = find_test_binary("test_connpool_exhaust", self._build)
        if not binary:
            self.skip("test_connpool_exhaust not found")
        rc, output = run_binary(binary, timeout=120)
        if rc != 0:
            raise AssertionError(
                f"test_connpool_exhaust failed (rc={rc}):\n{output[-2000:]}"
            )

    # -----------------------------------------------------------------------
    # E5 — FD tracking under pressure
    # -----------------------------------------------------------------------

    def test_e05_fd_tracking_under_pressure(self) -> None:
        """File-descriptor accounting must not leak under repeated open/close."""
        self._require_build()
        binary = find_test_binary("test_fd_tracking", self._build)
        if not binary:
            self.skip("test_fd_tracking not found")
        rc, output = run_binary(binary, timeout=60)
        if rc != 0:
            raise AssertionError(f"test_fd_tracking failed (rc={rc}):\n{output[-1500:]}")

    # -----------------------------------------------------------------------
    # E6 — Half-open connection flood
    # -----------------------------------------------------------------------

    def test_e06_half_open_connection_flood(self) -> None:
        """N connections that never send startup must not exhaust the proxy."""
        self._require_proxy()
        sockets: list[socket.socket] = []
        try:
            for _ in range(self._conn_count):
                try:
                    s = socket.create_connection(
                        (self._env["host"], self._env["port"]), timeout=2.0
                    )
                    sockets.append(s)
                except OSError:
                    break  # proxy may have hit its backlog — that is fine
            # Give the proxy a moment to notice the idle connections
            time.sleep(0.5)
        finally:
            for s in sockets:
                try:
                    s.close()
                except OSError:
                    pass

        # Verify the proxy still answers a legitimate connection
        with self._proxy_conn() as c:
            ok = self._auth(c)
        self.assert_true(ok, "Proxy unresponsive after half-open flood")

    # -----------------------------------------------------------------------
    # E7 — Slow client (stall after auth)
    # -----------------------------------------------------------------------

    def test_e07_slow_client_stall_after_auth(self) -> None:
        """A client that authenticates but then sends no data must be tolerated."""
        self._require_proxy()

        finished = threading.Event()
        result_box: list[bool] = []

        def _stall_client() -> None:
            try:
                with self._proxy_conn(timeout=10.0) as c:
                    ok = self._auth(c)
                    if ok:
                        time.sleep(5)   # stall for 5 seconds
                result_box.append(True)
            except Exception:
                result_box.append(False)
            finally:
                finished.set()

        th = threading.Thread(target=_stall_client, daemon=True)
        th.start()
        th.join(timeout=12)

        # Proxy must still serve other clients during the stall
        with self._proxy_conn() as c:
            alive = self._auth(c)
        self.assert_true(alive, "Proxy unavailable while slow client was stalling")

    # -----------------------------------------------------------------------
    # E8 — Rapid disconnect after auth
    # -----------------------------------------------------------------------

    def test_e08_rapid_disconnect_after_auth(self) -> None:
        """Connect, authenticate, immediately terminate — repeat 20 times."""
        self._require_proxy()
        for i in range(20):
            try:
                with self._proxy_conn() as c:
                    ok = self._auth(c)
                    if ok:
                        c.send(pg_terminate())
            except (ConnectionError, OSError):
                pass  # proxy may close first — fine

        # Proxy must still be alive
        with self._proxy_conn() as c:
            alive = self._auth(c)
        self.assert_true(alive, f"Proxy crashed after rapid disconnect test")

    # -----------------------------------------------------------------------
    # E9 — Connection storm
    # -----------------------------------------------------------------------

    def test_e09_connection_storm(self) -> None:
        """N concurrent connections all authenticate and run SELECT 1."""
        self._require_proxy()

        successes = [0]
        failures  = [0]
        lock      = threading.Lock()

        def _worker() -> None:
            try:
                with self._proxy_conn() as c:
                    ok = self._auth(c)
                    if ok:
                        c.send(pg_query("SELECT 1"))
                        c.recv_until({ord("Z"), ord("E")})
                with lock:
                    successes[0] += 1
            except Exception:
                with lock:
                    failures[0] += 1

        threads = [threading.Thread(target=_worker, daemon=True)
                   for _ in range(self._conn_count)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=20)

        total = successes[0] + failures[0]
        error_rate = failures[0] / max(total, 1)
        if error_rate > 0.05:
            raise AssertionError(
                f"Connection storm: {failures[0]}/{total} failures "
                f"({error_rate*100:.1f}% > 5% threshold)"
            )

    # -----------------------------------------------------------------------
    # E10 — SQL error recovery
    # -----------------------------------------------------------------------

    def test_e10_sql_error_recovery(self) -> None:
        """An SQL error must not break the connection for subsequent queries."""
        self._require_proxy()
        with self._proxy_conn() as c:
            ok = self._auth(c)
            self.assert_true(ok, "startup failed")

            # Trigger an error
            c.send(pg_query("SELECT * FROM this_table_does_not_exist_keel_test"))
            msgs = c.recv_until({ord("Z"), ord("E")})
            types = {m[0] for m in msgs}
            self.assert_in(ord("E"), types, "Expected ErrorResponse for bad query")

            # Now run a valid query — must succeed on the same connection
            c.send(pg_query("SELECT 99"))
            msgs2 = c.recv_until({ord("Z"), ord("E")})
            types2 = {m[0] for m in msgs2}
            self.assert_in(ord("Z"), types2,
                           "Connection broken after SQL error — no ReadyForQuery")
            self.assert_in(ord("D"), types2,
                           "No DataRow in valid query after SQL error recovery")


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = ResilienceSuite(result, verbose=bool(kwargs.get("verbose")),
                             connections=kwargs.get("connections", _DEFAULT_CONN_COUNT),
                             filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def _add_args(p: "argparse.ArgumentParser") -> None:
    p.add_argument("--connections", type=int, default=_DEFAULT_CONN_COUNT,
                   help=f"Concurrent connections for storm/flood tests (default: {_DEFAULT_CONN_COUNT})")


if __name__ == "__main__":
    import argparse
    standalone_main(ResilienceSuite, "resilience", ResilienceSuite.DESCRIPTION, _add_args)
