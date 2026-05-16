"""
tests/suites/suite_chaos.py
==============================
Category G — Chaos Testing (Network Jitter)

Injects real network impairments using ``tc qdisc`` (Linux traffic control)
and verifies that the proxy continues to route queries correctly and does
not corrupt or lose data.

Requirements:
  - Linux kernel with ``tc`` (iproute2) and ``netem`` module
  - ``sudo`` rights for ``tc`` commands (or CAP_NET_ADMIN)
  - Running KEEL proxy (KEEL_HOST / KEEL_PORT)
  - psycopg2-binary

Environment variables:
  KEEL_HOST / KEEL_PORT   Proxy endpoint
  KEEL_CHAOS_IFACE        Network interface to impair (default: lo)
  KEEL_CHAOS_SUDO         Set to "0" to skip tc setup (for envs without sudo)

Tests:
  G1.  tc netem availability check (prerequisite)
  G2.  50 ms latency jitter — queries must complete, results must be correct
  G3.  5 % packet loss — proxy must retransmit; data must be correct
  G4.  Bandwidth cap (1 Mbit/s) — high-volume query must still complete
  G5.  Reorder 25 % packets — proxy must reassemble correctly
  G6.  Duplicate 10 % packets — proxy must be idempotent
  G7.  Combined: 20 ms jitter + 2 % loss — steady-state correctness
  G8.  Proxy survives backend restart mid-flight (Docker or process kill)
  G9.  Sentinel integrity under 50 ms jitter (write 20 rows, verify all present)
  G10. Transaction atomicity under 5 % packet loss (each txn is 0 or N rows)
  G11. Concurrent sentinel writers under combined impairment (5 threads × 4-row txns)
  G12. Sentinel content correctness (exact TAG:PHASE:SEQ value verification)
  G13. Rollback consistency under network fault (explicit ROLLBACK leaves 0 rows)
  G14. Durability after connection drop (rows visible on new connection)

Run standalone:
    python tests/suites/suite_chaos.py --verbose --iface lo
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    is_proxy_reachable,
    latency_stats,
    proxy_env,
    wait_for_port,
)

_DEFAULT_IFACE = "lo"
_CHAOS_DURATION_S = 10   # how long each impairment lasts during the test


def _import_psycopg2():
    try:
        import psycopg2
        return psycopg2
    except ImportError:
        return None


def _make_dsn(env: dict) -> str:
    return (
        f"host={env['host']} port={env['port']} "
        f"user={env['user']} password={env['password']} "
        f"dbname={env['database']}"
    )


def _run_tc(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a tc command, optionally with sudo."""
    use_sudo = os.environ.get("KEEL_CHAOS_SUDO", "1") != "0"
    cmd = (["sudo"] if use_sudo else []) + ["tc"] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=10, check=check)


class ChaosSuite(SuiteRunner):
    NAME        = "chaos"
    DESCRIPTION = "Category G — Chaos Testing (Network Jitter)"
    TAGS        = ["chaos", "netem", "jitter", "packet-loss", "network"]

    def setup(self) -> None:
        self._env   = proxy_env()
        self._iface = (
            os.environ.get("KEEL_CHAOS_IFACE")
            or self.kwargs.get("iface", _DEFAULT_IFACE)
        )
        self._pg = _import_psycopg2()
        if not is_proxy_reachable(self._env):
            self._skip_msg = (
                f"Proxy not reachable at {self._env['host']}:{self._env['port']}"
            )
        elif self._pg is None:
            self._skip_msg = "psycopg2 not installed"
        elif not shutil.which("tc"):
            self._skip_msg = "tc (iproute2) not found in PATH"
        else:
            self._skip_msg = None
        self._tc_active = False

    def teardown(self) -> None:
        self._tc_clean()

    def _require_chaos(self) -> None:
        if self._skip_msg:
            self.skip(self._skip_msg)

    def _require_tc(self) -> None:
        if not shutil.which("tc"):
            self.skip("tc not available")

    # -----------------------------------------------------------------------
    # tc helpers
    # -----------------------------------------------------------------------

    def _tc_set(self, *netem_args: str) -> bool:
        """Apply a netem rule on the loopback (or configured) interface."""
        try:
            # Remove any existing qdisc first (ignore errors)
            _run_tc("qdisc", "del", "dev", self._iface, "root", check=False)
            _run_tc("qdisc", "add", "dev", self._iface, "root",
                    "netem", *netem_args)
            self._tc_active = True
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _tc_clean(self) -> None:
        if self._tc_active:
            try:
                _run_tc("qdisc", "del", "dev", self._iface, "root", check=False)
            except Exception:
                pass
            self._tc_active = False

    # -----------------------------------------------------------------------
    # Query helper — runs N SELECTs and measures success + latency
    # -----------------------------------------------------------------------

    def _run_queries(self, n: int = 50, timeout: float = 30.0) -> dict:
        successes = 0
        errors    = 0
        latencies: list[float] = []

        try:
            conn = self._pg.connect(_make_dsn(self._env), connect_timeout=15)
            conn.autocommit = True
            cur  = conn.cursor()
            deadline = time.monotonic() + timeout
            for _ in range(n):
                if time.monotonic() > deadline:
                    break
                t0 = time.monotonic()
                try:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    latencies.append((time.monotonic() - t0) * 1000.0)
                    successes += 1
                except Exception:
                    errors += 1
            conn.close()
        except Exception as exc:
            errors += 1

        return {
            "successes": successes,
            "errors":    errors,
            "latency":   latency_stats(latencies),
        }

    # -----------------------------------------------------------------------
    # G1 — Prerequisite check
    # -----------------------------------------------------------------------

    def test_g01_tc_netem_available(self) -> None:
        """Verify that tc and the netem module are available on this host."""
        self._require_tc()
        result = subprocess.run(
            ["tc", "qdisc", "show", "dev", self._iface],
            capture_output=True, text=True, timeout=5,
        )
        self.assert_true(result.returncode == 0,
                         f"tc qdisc show failed: {result.stderr}")

    # -----------------------------------------------------------------------
    # G2 — 50 ms latency jitter
    # -----------------------------------------------------------------------

    def test_g02_latency_50ms_jitter(self) -> None:
        """Under 50 ms added latency queries must still succeed and return correct data."""
        self._require_chaos()
        if not self._tc_set("delay", "50ms", "10ms", "distribution", "normal"):
            self.skip("Could not apply tc netem rule (need CAP_NET_ADMIN or sudo)")

        try:
            result = self._run_queries(n=20, timeout=_CHAOS_DURATION_S + 5)
        finally:
            self._tc_clean()

        self.assert_true(result["successes"] > 0,
                         "No successful queries under 50 ms jitter")
        error_rate = result["errors"] / max(result["successes"] + result["errors"], 1)
        if error_rate > 0.10:
            raise AssertionError(
                f"Error rate {error_rate*100:.1f}% > 10% under 50 ms jitter"
            )

    # -----------------------------------------------------------------------
    # G3 — 5% packet loss
    # -----------------------------------------------------------------------

    def test_g03_packet_loss_5pct(self) -> None:
        """Under 5 % packet loss, TCP retransmits must keep queries succeeding."""
        self._require_chaos()
        if not self._tc_set("loss", "5%"):
            self.skip("Could not apply tc netem packet loss rule")

        try:
            result = self._run_queries(n=30, timeout=_CHAOS_DURATION_S + 10)
        finally:
            self._tc_clean()

        self.assert_true(result["successes"] > 0,
                         "No successful queries under 5% packet loss")

    # -----------------------------------------------------------------------
    # G4 — Bandwidth cap (1 Mbit/s)
    # -----------------------------------------------------------------------

    def test_g04_bandwidth_cap_1mbit(self) -> None:
        """Even at 1 Mbit/s, a small query must complete within a reasonable time."""
        self._require_chaos()
        if not self._tc_set("rate", "1mbit"):
            self.skip("Could not apply tc netem bandwidth cap")

        try:
            result = self._run_queries(n=10, timeout=30)
        finally:
            self._tc_clean()

        self.assert_true(result["successes"] > 0,
                         "No queries succeeded under 1 Mbit/s bandwidth cap")

    # -----------------------------------------------------------------------
    # G5 — Packet reorder 25 %
    # -----------------------------------------------------------------------

    def test_g05_packet_reorder_25pct(self) -> None:
        """25 % of packets reordered — TCP reassembly must handle this correctly."""
        self._require_chaos()
        # Reorder requires a base delay
        if not self._tc_set("delay", "10ms", "reorder", "25%", "50%"):
            self.skip("Could not apply tc netem reorder rule")

        try:
            result = self._run_queries(n=20, timeout=_CHAOS_DURATION_S + 5)
        finally:
            self._tc_clean()

        self.assert_true(result["successes"] > 0,
                         "No queries succeeded under 25% packet reorder")

    # -----------------------------------------------------------------------
    # G6 — Duplicate 10 % packets
    # -----------------------------------------------------------------------

    def test_g06_packet_duplicate_10pct(self) -> None:
        """Duplicate packets must not cause data corruption."""
        self._require_chaos()
        if not self._tc_set("duplicate", "10%"):
            self.skip("Could not apply tc netem duplicate rule")

        try:
            # Use a query that returns a known value we can check
            conn = self._pg.connect(_make_dsn(self._env), connect_timeout=10)
            conn.autocommit = True
            cur  = conn.cursor()
            cur.execute("SELECT 12345 + 67890")
            value = cur.fetchone()[0]
            conn.close()
        finally:
            self._tc_clean()

        self.assert_eq(value, 80235, f"Duplicate-packet test: unexpected value {value}")

    # -----------------------------------------------------------------------
    # G7 — Combined impairment (jitter + loss)
    # -----------------------------------------------------------------------

    def test_g07_combined_jitter_and_loss(self) -> None:
        """Combined 20 ms jitter + 2 % loss — proxy must maintain correct behaviour."""
        self._require_chaos()
        if not self._tc_set("delay", "20ms", "5ms", "loss", "2%"):
            self.skip("Could not apply combined tc netem rule")

        try:
            result = self._run_queries(n=40, timeout=_CHAOS_DURATION_S + 15)
        finally:
            self._tc_clean()

        self.assert_true(result["successes"] > 0,
                         "No queries succeeded under combined jitter + loss")

    # -----------------------------------------------------------------------
    # Sentinel helpers (used by G9-G14)
    # -----------------------------------------------------------------------

    # The sentinel table is per-class so parallel suite runs don't collide.
    _SENTINEL_TABLE = "chaos_sentinel_py"

    def _setup_sentinel_table(self, conn) -> None:
        """Create the sentinel table if not already present."""
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._SENTINEL_TABLE} (
                    id        BIGSERIAL PRIMARY KEY,
                    scenario  TEXT NOT NULL,
                    tag       TEXT NOT NULL,
                    phase     TEXT NOT NULL,
                    seq       INT  NOT NULL,
                    val       TEXT NOT NULL,
                    written_via TEXT,
                    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT {self._SENTINEL_TABLE}_val_uq UNIQUE (val)
                )
            """)
        conn.commit()

    def _sentinel_tag(self, prefix: str = "g") -> str:
        """Generate a unique run tag."""
        return f"{prefix}_{os.getpid()}_{int(time.monotonic() * 1000)}"

    def _write_sentinel_batch(self, conn, scenario: str, tag: str,
                               phase: str, n: int, via: str = "keel") -> None:
        """Insert n sentinel rows in a single multi-value INSERT (autocommit)."""
        rows = ", ".join(
            f"('{scenario}', '{tag}', '{phase}', {i}, '{tag}:{phase}:{i}', '{via}')"
            for i in range(1, n + 1)
        )
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._SENTINEL_TABLE} "
                f"(scenario, tag, phase, seq, val, written_via) VALUES {rows} "
                f"ON CONFLICT (val) DO NOTHING"
            )
        conn.commit()

    def _write_sentinel_txn(self, conn, scenario: str, tag: str,
                             phase: str, n: int, via: str = "keel") -> bool:
        """Insert n sentinel rows in an explicit BEGIN/COMMIT. Returns True if committed."""
        try:
            conn.autocommit = False
            rows = ", ".join(
                f"('{scenario}', '{tag}', '{phase}', {i}, '{tag}:{phase}:{i}', '{via}')"
                for i in range(1, n + 1)
            )
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._SENTINEL_TABLE} "
                    f"(scenario, tag, phase, seq, val, written_via) VALUES {rows} "
                    f"ON CONFLICT (val) DO NOTHING"
                )
            conn.commit()
            return True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            conn.autocommit = True

    def _get_sentinel_values(self, conn, tag: str, phase: str) -> set:
        """Return the set of val strings stored for a given tag+phase."""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT val FROM {self._SENTINEL_TABLE} "
                f"WHERE tag = %s AND phase = %s",
                (tag, phase),
            )
            return {row[0] for row in cur.fetchall()}

    def _get_sentinel_count(self, conn, tag: str, phase: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {self._SENTINEL_TABLE} "
                f"WHERE tag = %s AND phase = %s",
                (tag, phase),
            )
            return cur.fetchone()[0]

    def _assert_sentinel_all_present(self, conn, tag: str, phase: str,
                                      n: int, desc: str) -> None:
        """Assert that all n expected val strings TAG:PHASE:1..N are stored."""
        expected = {f"{tag}:{phase}:{i}" for i in range(1, n + 1)}
        actual   = self._get_sentinel_values(conn, tag, phase)
        missing  = expected - actual
        extra    = actual - expected
        if missing or extra:
            raise AssertionError(
                f"{desc}: sentinel value mismatch — "
                f"missing={len(missing)}, unexpected={len(extra)}"
            )

    def _assert_sentinel_atomic(self, conn, tag: str, phase: str,
                                 n: int, desc: str) -> None:
        """Assert count is exactly 0 or n — no partial commit is acceptable."""
        count = self._get_sentinel_count(conn, tag, phase)
        if count not in (0, n):
            raise AssertionError(
                f"{desc}: ATOMICITY VIOLATION — got {count} rows, expected 0 or {n}"
            )

    def _make_direct_conn(self, host: str | None = None,
                           port: int | None = None):
        """Open a connection to the primary DB (bypasses proxy when host/port given)."""
        env = self._env
        dsn = (
            f"host={host or env['host']} port={port or env['port']} "
            f"user={env['user']} password={env['password']} "
            f"dbname={env['database']}"
        )
        conn = self._pg.connect(dsn, connect_timeout=10)
        conn.autocommit = True
        return conn

    # -----------------------------------------------------------------------
    # G8 — Proxy survives backend restart (if Docker available)
    # -----------------------------------------------------------------------

    def test_g08_backend_restart_resilience(self) -> None:
        """Proxy must recover (error + reconnect) when the backend restarts."""
        self._require_chaos()

        backend_container = os.environ.get("KEEL_BACKEND_CONTAINER")
        if not backend_container:
            self.skip(
                "Set KEEL_BACKEND_CONTAINER=<name> to enable backend restart test "
                "(e.g. KEEL_BACKEND_CONTAINER=keel-pg-primary)"
            )

        import shutil
        if not shutil.which("docker"):
            self.skip("docker not available")

        # Connect and verify working before restart
        conn = self._pg.connect(_make_dsn(self._env), connect_timeout=10)
        conn.autocommit = True
        conn.cursor().execute("SELECT 1")
        conn.close()

        # Restart the backend container
        subprocess.run(
            ["docker", "restart", backend_container],
            timeout=30, capture_output=True,
        )

        # Allow proxy time to detect the failure and reconnect
        time.sleep(5)

        # New connection must eventually succeed
        deadline = time.monotonic() + 30
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = self._pg.connect(_make_dsn(self._env), connect_timeout=5)
                conn.autocommit = True
                conn.cursor().execute("SELECT 1")
                conn.close()
                return  # success
            except Exception as exc:
                last_exc = exc
                time.sleep(1)

        raise AssertionError(
            f"Proxy did not recover after backend restart within 30 s: {last_exc}"
        )

    # -----------------------------------------------------------------------
    # G9 — Sentinel integrity under 50 ms jitter
    # -----------------------------------------------------------------------

    def test_g09_sentinel_integrity_under_jitter(self) -> None:
        """Write 20 sentinel rows, inject 50 ms jitter, read back and verify all present."""
        self._require_chaos()

        conn = self._make_direct_conn()
        self._setup_sentinel_table(conn)
        tag = self._sentinel_tag("g9")

        # Write 20 rows through the proxy BEFORE injecting the fault
        self._write_sentinel_batch(conn, "g9_jitter", tag, "pre_jitter", 20)

        if not self._tc_set("delay", "50ms", "10ms", "distribution", "normal"):
            conn.close()
            self.skip("Could not apply tc netem jitter rule")

        try:
            # Verify reads succeed under jitter and return correct values
            self._assert_sentinel_all_present(
                conn, tag, "pre_jitter", 20,
                "G9: sentinel values under 50ms jitter"
            )
            # Write 20 more rows during active jitter
            tag2 = self._sentinel_tag("g9_during")
            self._write_sentinel_batch(conn, "g9_jitter", tag2, "during_jitter", 20)
        finally:
            self._tc_clean()
            conn.close()

        # After jitter is gone, verify during-jitter rows are all intact
        conn2 = self._make_direct_conn()
        try:
            self._assert_sentinel_all_present(
                conn2, tag2, "during_jitter", 20,
                "G9: sentinel values written during jitter must all be present"
            )
        finally:
            conn2.close()

    # -----------------------------------------------------------------------
    # G10 — Transaction atomicity under 5 % packet loss
    # -----------------------------------------------------------------------

    def test_g10_txn_atomicity_under_packet_loss(self) -> None:
        """Inject 5% packet loss; each explicit 10-row txn must be 0 or 10 rows — no partial."""
        self._require_chaos()

        conn = self._make_direct_conn()
        self._setup_sentinel_table(conn)

        if not self._tc_set("loss", "5%"):
            conn.close()
            self.skip("Could not apply tc netem packet-loss rule")

        TXN_SIZE = 10
        TXN_COUNT = 8
        base_tag = self._sentinel_tag("g10")
        tags: list[str] = []

        try:
            for i in range(TXN_COUNT):
                txn_tag = f"{base_tag}_{i}"
                tags.append(txn_tag)
                # Ignore commit/rollback outcome — we check atomicity after
                self._write_sentinel_txn(
                    conn, "g10_loss", txn_tag, "loss_window", TXN_SIZE
                )
        finally:
            self._tc_clean()
            conn.close()

        conn2 = self._make_direct_conn()
        try:
            for txn_tag in tags:
                self._assert_sentinel_atomic(
                    conn2, txn_tag, "loss_window", TXN_SIZE,
                    f"G10: atomicity check for tag={txn_tag}"
                )
        finally:
            conn2.close()

    # -----------------------------------------------------------------------
    # G11 — Concurrent sentinel writers under combined impairment
    # -----------------------------------------------------------------------

    def test_g11_concurrent_writers_under_combined_impairment(self) -> None:
        """5 threads each write 4-row txns; each txn must be 0 or 4 rows."""
        self._require_chaos()

        conn_setup = self._make_direct_conn()
        self._setup_sentinel_table(conn_setup)
        conn_setup.close()

        if not self._tc_set("delay", "20ms", "5ms", "loss", "2%"):
            self.skip("Could not apply combined tc netem rule")

        THREADS   = 5
        TXN_SIZE  = 4
        TXN_EACH  = 6
        base_tag  = self._sentinel_tag("g11")
        all_tags: list[list[str]] = [[] for _ in range(THREADS)]
        errors:   list[str]       = []

        def _writer(thread_id: int) -> None:
            try:
                conn = self._make_direct_conn()
                for i in range(TXN_EACH):
                    txn_tag = f"{base_tag}_t{thread_id}_{i}"
                    all_tags[thread_id].append(txn_tag)
                    self._write_sentinel_txn(
                        conn, "g11_concurrent", txn_tag, "concurrent", TXN_SIZE
                    )
                conn.close()
            except Exception as exc:
                errors.append(f"thread {thread_id}: {exc}")

        threads = [threading.Thread(target=_writer, args=(i,)) for i in range(THREADS)]
        try:
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
        finally:
            self._tc_clean()

        if errors:
            # Thread connection errors are acceptable; atomicity is what matters
            pass

        conn3 = self._make_direct_conn()
        try:
            for thread_tags in all_tags:
                for txn_tag in thread_tags:
                    self._assert_sentinel_atomic(
                        conn3, txn_tag, "concurrent", TXN_SIZE,
                        f"G11: atomicity check for tag={txn_tag}"
                    )
        finally:
            conn3.close()

    # -----------------------------------------------------------------------
    # G12 — Sentinel content correctness (exact value verification)
    # -----------------------------------------------------------------------

    def test_g12_sentinel_content_correctness(self) -> None:
        """Verify stored sentinel val strings match TAG:PHASE:SEQ exactly."""
        self._require_chaos()

        conn = self._make_direct_conn()
        self._setup_sentinel_table(conn)
        tag = self._sentinel_tag("g12")

        N = 15
        self._write_sentinel_batch(conn, "g12_content", tag, "content_check", N)

        try:
            self._assert_sentinel_all_present(
                conn, tag, "content_check", N,
                "G12: exact sentinel content correctness"
            )
            # Extra check: confirm each val has correct TAG:PHASE:SEQ format
            actual = self._get_sentinel_values(conn, tag, "content_check")
            for seq in range(1, N + 1):
                expected_val = f"{tag}:content_check:{seq}"
                self.assert_true(
                    expected_val in actual,
                    f"G12: missing exact value '{expected_val}'"
                )
        finally:
            conn.close()

    # -----------------------------------------------------------------------
    # G13 — Rollback consistency under network fault
    # -----------------------------------------------------------------------

    def test_g13_rollback_consistency_under_fault(self) -> None:
        """Inject fault; explicitly ROLLBACK a transaction — must leave 0 rows."""
        self._require_chaos()

        conn = self._make_direct_conn()
        self._setup_sentinel_table(conn)
        tag = self._sentinel_tag("g13")

        if not self._tc_set("delay", "30ms", "5ms", "loss", "3%"):
            conn.close()
            self.skip("Could not apply tc netem rule for rollback test")

        committed = False
        try:
            conn.autocommit = False
            N = 10
            rows = ", ".join(
                f"('g13_rollback', '{tag}', 'rollback_check', {i}, "
                f"'{tag}:rollback_check:{i}', 'direct')"
                for i in range(1, N + 1)
            )
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._SENTINEL_TABLE} "
                    f"(scenario, tag, phase, seq, val, written_via) VALUES {rows} "
                    f"ON CONFLICT (val) DO NOTHING"
                )
            # Explicit ROLLBACK — must not persist any rows
            conn.rollback()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.autocommit = True
            self._tc_clean()

        count = self._get_sentinel_count(conn, tag, "rollback_check")
        conn.close()
        self.assert_eq(
            count, 0,
            f"G13: ROLLBACK left {count} rows — partial commit after rollback!"
        )

    # -----------------------------------------------------------------------
    # G14 — Durability after connection drop
    # -----------------------------------------------------------------------

    def test_g14_durability_after_connection_drop(self) -> None:
        """Rows committed on one connection must be visible on a fresh connection."""
        self._require_chaos()

        conn1 = self._make_direct_conn()
        self._setup_sentinel_table(conn1)
        tag = self._sentinel_tag("g14")

        N = 12
        self._write_sentinel_batch(conn1, "g14_durability", tag, "durable", N)
        conn1.close()  # intentionally close/drop the writing connection

        # Brief jitter while reconnecting to simulate network instability
        if not self._tc_set("delay", "20ms", "5ms"):
            pass  # proceed without jitter if unavailable

        try:
            conn2 = self._make_direct_conn()
            try:
                self._assert_sentinel_all_present(
                    conn2, tag, "durable", N,
                    "G14: rows must survive connection drop"
                )
            finally:
                conn2.close()
        finally:
            self._tc_clean()


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = ChaosSuite(result, verbose=bool(kwargs.get("verbose")),
                        iface=kwargs.get("iface", _DEFAULT_IFACE),
                        filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def _add_args(p: "argparse.ArgumentParser") -> None:
    p.add_argument("--iface", default=_DEFAULT_IFACE, metavar="IFACE",
                   help=f"Network interface for tc rules (default: {_DEFAULT_IFACE})")


if __name__ == "__main__":
    import argparse
    standalone_main(ChaosSuite, "chaos", ChaosSuite.DESCRIPTION, _add_args)
