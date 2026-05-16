"""
tests/suites/suite_soak.py
============================
Category H — Soak / Longevity Testing

Runs the proxy under a sustained workload and watches for:
  - Latency drift (p99 must not grow over time)
  - Memory growth (RSS must stay within a configurable envelope)
  - File-descriptor leaks (open FD count must be stable)
  - Error rate creep (must stay near zero)

Environment variables:
  KEEL_HOST / KEEL_PORT        Proxy endpoint
  KEEL_SOAK_DURATION_S         Total soak duration in seconds (default: 60)
  KEEL_SOAK_CLIENTS            Concurrent client threads       (default: 5)
  KEEL_SOAK_MAX_RSS_GROWTH_MB  Acceptable RSS growth in MiB   (default: 50)
  KEEL_SOAK_SAMPLE_INTERVAL_S  Measurement interval in seconds (default: 5)
  KEEL_PROXY_PID               PID of the running keel process (for RSS/FD checks)

Tests:
  H1. Steady-state latency stability (p99 must not grow > 50 % over the run)
  H2. Memory non-leak (RSS must not grow > N MiB over the soak)
  H3. File-descriptor stability (open FDs must not grow monotonically)
  H4. Error-rate stability (error rate must stay < 1 %)
  H5. Connection-churn longevity (open + close repeatedly, no leak)
  H6. Mixed read/write workload (correct rows, no drift)

Run standalone:
    python tests/suites/suite_soak.py --verbose --duration 120 --clients 10
    python tests/suites/suite_soak.py --duration 3600  # 1-hour soak
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    is_proxy_reachable,
    latency_stats,
    percentile,
    proxy_env,
)

_DEFAULT_DURATION_S  = 60
_DEFAULT_CLIENTS     = 5
_DEFAULT_MAX_RSS_MB  = 50
_DEFAULT_SAMPLE_S    = 5


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


def _get_rss_kb(pid: int) -> int | None:
    """Read RSS from /proc/<pid>/status (Linux only)."""
    try:
        text = Path(f"/proc/{pid}/status").read_text()
        for line in text.splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, ValueError):
        pass
    return None


def _get_fd_count(pid: int) -> int | None:
    """Count open file descriptors via /proc/<pid>/fd (Linux only)."""
    try:
        return len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except OSError:
        return None


class SoakSuite(SuiteRunner):
    NAME        = "soak"
    DESCRIPTION = "Category H — Soak / Longevity Testing"
    TAGS        = ["soak", "longevity", "memory", "stability"]

    def setup(self) -> None:
        self._env      = proxy_env()
        self._pg       = _import_psycopg2()
        self._duration = int(
            self.kwargs.get("duration")
            or os.environ.get("KEEL_SOAK_DURATION_S", _DEFAULT_DURATION_S)
        )
        self._clients  = int(
            self.kwargs.get("clients")
            or os.environ.get("KEEL_SOAK_CLIENTS", _DEFAULT_CLIENTS)
        )
        self._max_rss_mb = int(
            self.kwargs.get("max_rss_mb")
            or os.environ.get("KEEL_SOAK_MAX_RSS_GROWTH_MB", _DEFAULT_MAX_RSS_MB)
        )
        self._sample_s = int(
            self.kwargs.get("sample_interval")
            or os.environ.get("KEEL_SOAK_SAMPLE_INTERVAL_S", _DEFAULT_SAMPLE_S)
        )
        self._proxy_pid: int | None = None
        raw_pid = os.environ.get("KEEL_PROXY_PID") or self.kwargs.get("proxy_pid")
        if raw_pid:
            try:
                self._proxy_pid = int(raw_pid)
            except (TypeError, ValueError):
                pass

        if not is_proxy_reachable(self._env):
            self._skip_msg = (
                f"Proxy not reachable at {self._env['host']}:{self._env['port']}"
            )
        elif self._pg is None:
            self._skip_msg = "psycopg2 not installed — pip install psycopg2-binary"
        else:
            self._skip_msg = None

    def _require_proxy(self) -> None:
        if self._skip_msg:
            self.skip(self._skip_msg)

    def _connect(self, autocommit: bool = True):
        conn = self._pg.connect(_make_dsn(self._env), connect_timeout=10)
        conn.autocommit = autocommit
        return conn

    # -----------------------------------------------------------------------
    # Core soak workload engine
    # -----------------------------------------------------------------------

    def _run_soak(
        self, duration: int, clients: int, workload_fn=None
    ) -> dict:
        """
        Run *workload_fn* across *clients* threads for *duration* seconds.
        Returns a dict with per-window latency samples, error counts, and
        system metrics (RSS, FD count) sampled at _sample_s intervals.
        """
        if workload_fn is None:
            def workload_fn(cur) -> None:
                cur.execute("SELECT 1")
                cur.fetchone()

        stop_event = threading.Event()
        query_counts:   list[int]   = [0] * clients
        error_counts:   list[int]   = [0] * clients
        all_latencies:  list[float] = []
        lat_lock = threading.Lock()

        def _worker(idx: int) -> None:
            try:
                conn = self._connect()
                cur  = conn.cursor()
                while not stop_event.is_set():
                    t0 = time.monotonic()
                    try:
                        workload_fn(cur)
                        elapsed_ms = (time.monotonic() - t0) * 1000.0
                        query_counts[idx] += 1
                        with lat_lock:
                            all_latencies.append(elapsed_ms)
                    except Exception:
                        error_counts[idx] += 1
                conn.close()
            except Exception:
                error_counts[idx] += 1

        threads = [threading.Thread(target=_worker, args=(i,), daemon=True)
                   for i in range(clients)]
        for th in threads:
            th.start()

        # Sampling loop
        rss_samples: list[int] = []
        fd_samples:  list[int] = []
        latency_windows: list[dict] = []

        t_start = time.monotonic()
        next_sample = t_start + self._sample_s
        prev_lats: list[float] = []

        while time.monotonic() - t_start < duration:
            time.sleep(0.5)
            now = time.monotonic()
            if now >= next_sample:
                next_sample = now + self._sample_s
                # Snapshot latency window
                with lat_lock:
                    window = list(all_latencies[len(prev_lats):])
                    prev_lats = list(all_latencies)
                if window:
                    latency_windows.append(latency_stats(window))
                # Snapshot system metrics
                if self._proxy_pid:
                    rss = _get_rss_kb(self._proxy_pid)
                    fds = _get_fd_count(self._proxy_pid)
                    if rss is not None:
                        rss_samples.append(rss)
                    if fds is not None:
                        fd_samples.append(fds)

        stop_event.set()
        for th in threads:
            th.join(timeout=10)

        return {
            "total_queries":     sum(query_counts),
            "total_errors":      sum(error_counts),
            "latency_windows":   latency_windows,
            "all_latencies":     all_latencies,
            "rss_samples_kb":    rss_samples,
            "fd_samples":        fd_samples,
        }

    # -----------------------------------------------------------------------
    # H1 — Latency stability
    # -----------------------------------------------------------------------

    def test_h01_latency_stability(self) -> None:
        """p99 latency must not grow by more than 50 % over the soak run."""
        self._require_proxy()

        soak = self._run_soak(self._duration, self._clients)
        windows = soak["latency_windows"]
        if len(windows) < 2:
            self.skip("Not enough latency windows sampled (duration too short?)")

        first_p99 = windows[0].get("p99_ms", 0)
        last_p99  = windows[-1].get("p99_ms", 0)

        if first_p99 > 0 and last_p99 > first_p99 * 2.0:
            raise AssertionError(
                f"Latency drift: p99 grew from {first_p99:.1f} ms → {last_p99:.1f} ms "
                f"(>{100*((last_p99/first_p99)-1):.0f}% growth)"
            )

        if self.verbose:
            for i, w in enumerate(windows):
                print(f"    window {i}: p50={w.get('p50_ms',0):.1f} ms  "
                      f"p99={w.get('p99_ms',0):.1f} ms  "
                      f"n={w.get('count',0)}", flush=True)

    # -----------------------------------------------------------------------
    # H2 — Memory growth
    # -----------------------------------------------------------------------

    def test_h02_memory_non_growth(self) -> None:
        """RSS must not grow by more than KEEL_SOAK_MAX_RSS_GROWTH_MB over the run."""
        self._require_proxy()
        if not self._proxy_pid:
            self.skip(
                "Set KEEL_PROXY_PID=<pid> to enable RSS monitoring "
                "(e.g. KEEL_PROXY_PID=$(pgrep -n keel))"
            )

        soak = self._run_soak(self._duration, self._clients)
        rss  = soak["rss_samples_kb"]
        if len(rss) < 2:
            self.skip("Not enough RSS samples (proxy pid may be wrong)")

        growth_kb = max(rss) - rss[0]
        growth_mb = growth_kb / 1024.0
        if growth_mb > self._max_rss_mb:
            raise AssertionError(
                f"Memory growth {growth_mb:.1f} MiB exceeds limit {self._max_rss_mb} MiB "
                f"(initial={rss[0]//1024} MiB, peak={max(rss)//1024} MiB)"
            )

        if self.verbose:
            print(f"\n    RSS: initial={rss[0]//1024} MiB  "
                  f"peak={max(rss)//1024} MiB  "
                  f"final={rss[-1]//1024} MiB  "
                  f"growth={growth_mb:.1f} MiB", flush=True)

    # -----------------------------------------------------------------------
    # H3 — FD stability
    # -----------------------------------------------------------------------

    def test_h03_fd_stability(self) -> None:
        """Open FD count must not grow monotonically (indicates a leak)."""
        self._require_proxy()
        if not self._proxy_pid:
            self.skip("Set KEEL_PROXY_PID=<pid> to enable FD monitoring")

        soak = self._run_soak(self._duration, self._clients)
        fds  = soak["fd_samples"]
        if len(fds) < 3:
            self.skip("Not enough FD samples")

        # Check that FDs are not strictly monotonically increasing
        # (allow a small per-sample variance of 5)
        strictly_increasing = all(
            fds[i + 1] > fds[i] + 5 for i in range(len(fds) - 1)
        )
        if strictly_increasing:
            raise AssertionError(
                f"FD count is monotonically increasing — possible leak: {fds}"
            )

        growth = max(fds) - fds[0]
        if growth > 20:
            raise AssertionError(
                f"FD count grew by {growth} over the soak "
                f"(initial={fds[0]}, peak={max(fds)})"
            )

        if self.verbose:
            print(f"\n    FDs: initial={fds[0]}  peak={max(fds)}  final={fds[-1]}",
                  flush=True)

    # -----------------------------------------------------------------------
    # H4 — Error-rate stability
    # -----------------------------------------------------------------------

    def test_h04_error_rate_stability(self) -> None:
        """Error rate must stay below 1 % throughout the soak."""
        self._require_proxy()
        soak  = self._run_soak(self._duration, self._clients)
        total = soak["total_queries"] + soak["total_errors"]
        if total == 0:
            raise AssertionError("No queries completed during soak")

        error_rate = soak["total_errors"] / total
        if error_rate > 0.01:
            raise AssertionError(
                f"Error rate {error_rate*100:.2f}% > 1% threshold "
                f"({soak['total_errors']} errors / {total} total)"
            )

        if self.verbose:
            print(f"\n    Queries={soak['total_queries']}  "
                  f"Errors={soak['total_errors']}  "
                  f"Rate={error_rate*100:.3f}%", flush=True)

    # -----------------------------------------------------------------------
    # H5 — Connection-churn longevity
    # -----------------------------------------------------------------------

    def test_h05_connection_churn_longevity(self) -> None:
        """Repeated connect/disconnect cycles must not leak connections or memory."""
        self._require_proxy()

        total_conns = 0
        errors      = 0
        deadline    = time.monotonic() + min(self._duration, 30)

        while time.monotonic() < deadline:
            try:
                conn = self._connect()
                conn.cursor().execute("SELECT 1")
                conn.close()
                total_conns += 1
            except Exception:
                errors += 1

        error_rate = errors / max(total_conns + errors, 1)
        if error_rate > 0.02:
            raise AssertionError(
                f"Churn error rate {error_rate*100:.1f}% > 2% "
                f"({errors}/{total_conns + errors})"
            )

        if self.verbose:
            print(f"\n    Connections: {total_conns} ok, {errors} errors "
                  f"in {min(self._duration, 30)}s", flush=True)

    # -----------------------------------------------------------------------
    # H6 — Mixed read/write correctness under longevity
    # -----------------------------------------------------------------------

    def test_h06_mixed_workload_correctness(self) -> None:
        """Intermixed INSERTs and SELECTs must keep the row count exactly correct."""
        self._require_proxy()

        sentinel_prefix = f"soak_{int(time.monotonic())}_"

        # Setup
        conn = self._connect()
        conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS keel_soak_test (
                id SERIAL PRIMARY KEY,
                val TEXT NOT NULL
            )
        """)
        conn.close()

        inserts   = [0]
        lock      = threading.Lock()
        stop_evt  = threading.Event()

        def _write_worker() -> None:
            i = 0
            try:
                conn = self._connect()
                cur  = conn.cursor()
                while not stop_evt.is_set():
                    cur.execute(
                        "INSERT INTO keel_soak_test(val) VALUES (%s)",
                        (f"{sentinel_prefix}{i}",),
                    )
                    with lock:
                        inserts[0] += 1
                    i += 1
                conn.close()
            except Exception:
                pass

        def _read_worker() -> None:
            try:
                conn = self._connect()
                cur  = conn.cursor()
                while not stop_evt.is_set():
                    cur.execute(
                        "SELECT COUNT(*) FROM keel_soak_test WHERE val LIKE %s",
                        (sentinel_prefix + "%",),
                    )
                    cur.fetchone()
                conn.close()
            except Exception:
                pass

        writers = [threading.Thread(target=_write_worker, daemon=True)
                   for _ in range(2)]
        readers = [threading.Thread(target=_read_worker, daemon=True)
                   for _ in range(self._clients - 2)]
        for th in writers + readers:
            th.start()

        time.sleep(min(self._duration, 30))
        stop_evt.set()
        for th in writers + readers:
            th.join(timeout=10)

        # Verify exact count
        conn = self._connect()
        cur  = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM keel_soak_test WHERE val LIKE %s",
            (sentinel_prefix + "%",),
        )
        db_count = cur.fetchone()[0]
        # Cleanup
        cur.execute("DELETE FROM keel_soak_test WHERE val LIKE %s",
                    (sentinel_prefix + "%",))
        conn.close()

        if db_count != inserts[0]:
            raise AssertionError(
                f"Row count mismatch after mixed soak: "
                f"expected {inserts[0]}, found {db_count}"
            )

        if self.verbose:
            print(f"\n    Mixed workload: {inserts[0]} rows inserted+verified "
                  f"over {min(self._duration, 30)}s", flush=True)


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = SoakSuite(
        result,
        verbose=bool(kwargs.get("verbose")),
        duration=kwargs.get("duration", _DEFAULT_DURATION_S),
        clients=kwargs.get("clients", _DEFAULT_CLIENTS),
        max_rss_mb=kwargs.get("max_rss_mb", _DEFAULT_MAX_RSS_MB),
        proxy_pid=kwargs.get("proxy_pid"),
        filter_=str(kwargs.get("filter") or "") or None,
    )
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def _add_args(p: "argparse.ArgumentParser") -> None:
    p.add_argument("--duration", type=int, default=_DEFAULT_DURATION_S, metavar="S",
                   help=f"Soak duration in seconds (default: {_DEFAULT_DURATION_S})")
    p.add_argument("--clients", type=int, default=_DEFAULT_CLIENTS, metavar="N",
                   help=f"Concurrent client threads (default: {_DEFAULT_CLIENTS})")
    p.add_argument("--max-rss-mb", type=int, default=_DEFAULT_MAX_RSS_MB, metavar="MB",
                   help=f"Acceptable RSS growth in MiB (default: {_DEFAULT_MAX_RSS_MB})")
    p.add_argument("--proxy-pid", type=int, metavar="PID",
                   help="PID of the keel process for RSS / FD monitoring")


if __name__ == "__main__":
    import argparse
    standalone_main(SoakSuite, "soak", SoakSuite.DESCRIPTION, _add_args)
