"""
tests/suites/suite_throughput.py
=================================
Category C — Saturation & Throughput Testing (Baseline Performance)

Tests connect directly to a running KEEL proxy (env-var configurable).
They are skipped gracefully when no proxy is reachable.

Environment variables:
  KEEL_HOST          Proxy host     (default: 127.0.0.1)
  KEEL_PORT          Proxy port     (default: 5432)
  KEEL_USER          DB user        (default: postgres)
  KEEL_PASSWORD      DB password    (default: postgres)
  KEEL_DATABASE      DB name        (default: postgres)

  KEEL_BENCH_CLIENTS     Number of concurrent clients  (default: 10)
  KEEL_BENCH_DURATION_S  Seconds to run each benchmark (default: 10)
  KEEL_BENCH_BASELINE    Path to baseline JSON file for regression check

Tests:
  C1. Baseline TPS (single connection)
  C2. Latency percentiles (p50 / p95 / p99 / p99.9)
  C3. Concurrent client throughput (N clients)
  C4. Connection churn rate
  C5. Large result-set throughput
  C6. Prepared-statement throughput
  C7. Baseline regression check (compare against saved baseline)

Run standalone:
    python tests/suites/suite_throughput.py --verbose --clients 20 --duration 15
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    is_proxy_reachable,
    latency_stats,
    proxy_env,
    wait_for_port,
)

_DEFAULT_CLIENTS    = 10
_DEFAULT_DURATION_S = 10

# Thresholds — adjust to match your hardware
_MIN_TPS_SINGLE    = 100      # minimum acceptable TPS on single connection
_MIN_TPS_PARALLEL  = 500      # minimum TPS across all concurrent clients
_MAX_P99_MS        = 200.0    # p99 latency budget (ms)
_MAX_CHURN_DELAY_S = 0.100    # max acceptable time for connect + disconnect


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


class ThroughputSuite(SuiteRunner):
    NAME        = "throughput"
    DESCRIPTION = "Category C — Saturation & Throughput Testing"
    TAGS        = ["throughput", "latency", "performance", "pgbench"]

    def setup(self) -> None:
        self._env      = proxy_env()
        self._clients  = int(self.kwargs.get("clients",  _DEFAULT_CLIENTS))
        self._duration = int(self.kwargs.get("duration", _DEFAULT_DURATION_S))
        self._baseline = self.kwargs.get("baseline")

        psycopg2 = _import_psycopg2()
        if psycopg2 is None:
            self._pg = None
        else:
            self._pg = psycopg2

        if not is_proxy_reachable(self._env):
            self._skip_all = (
                f"Proxy not reachable at {self._env['host']}:{self._env['port']} "
                f"— set KEEL_HOST / KEEL_PORT or start the proxy"
            )
        else:
            self._skip_all = None

        self._stats: dict[str, dict] = {}   # accumulated stats for regression check

    def _require_proxy(self) -> None:
        if self._skip_all:
            self.skip(self._skip_all)
        if self._pg is None:
            self.skip("psycopg2 not installed — pip install psycopg2-binary")

    def _connect(self):
        return self._pg.connect(_make_dsn(self._env), connect_timeout=10)

    # -----------------------------------------------------------------------
    # C1 — Baseline TPS (single connection)
    # -----------------------------------------------------------------------

    def test_c1_baseline_tps_single_conn(self) -> None:
        """Measure queries-per-second on a single persistent connection."""
        self._require_proxy()

        conn = self._connect()
        conn.autocommit = True
        cur = conn.cursor()

        t0 = time.monotonic()
        count = 0
        deadline = t0 + self._duration
        while time.monotonic() < deadline:
            cur.execute("SELECT 1")
            cur.fetchone()
            count += 1

        elapsed = time.monotonic() - t0
        conn.close()

        tps = count / elapsed
        self._stats["c1_single_tps"] = {"tps": tps, "queries": count, "elapsed_s": elapsed}

        self.assert_gt(tps, _MIN_TPS_SINGLE,
                       f"Single-connection TPS {tps:.1f} < minimum {_MIN_TPS_SINGLE}")

    # -----------------------------------------------------------------------
    # C2 — Latency percentiles
    # -----------------------------------------------------------------------

    def test_c2_latency_percentiles(self) -> None:
        """Measure p50 / p95 / p99 / p99.9 latency of SELECT 1."""
        self._require_proxy()

        conn = self._connect()
        conn.autocommit = True
        cur = conn.cursor()

        latencies: list[float] = []
        deadline = time.monotonic() + self._duration
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            cur.execute("SELECT 1")
            cur.fetchone()
            latencies.append((time.monotonic() - t0) * 1000.0)

        conn.close()

        stats = latency_stats(latencies)
        self._stats["c2_latency"] = stats

        self.assert_lt(stats["p99_ms"], _MAX_P99_MS,
                       f"p99 latency {stats['p99_ms']:.1f}ms exceeds {_MAX_P99_MS}ms")
        if self.verbose:
            print(
                f"\n    latency: p50={stats['p50_ms']:.2f}ms "
                f"p95={stats['p95_ms']:.2f}ms "
                f"p99={stats['p99_ms']:.2f}ms "
                f"p99.9={stats['p999_ms']:.2f}ms "
                f"(n={stats['count']})",
                flush=True,
            )

    # -----------------------------------------------------------------------
    # C3 — Concurrent client throughput
    # -----------------------------------------------------------------------

    def test_c3_concurrent_clients_throughput(self) -> None:
        """Measure aggregate TPS across N concurrent connections."""
        self._require_proxy()

        counts: list[int] = [0] * self._clients
        errors: list[int] = [0] * self._clients
        stop_event = threading.Event()

        def _worker(idx: int) -> None:
            try:
                conn = self._connect()
                conn.autocommit = True
                cur = conn.cursor()
                while not stop_event.is_set():
                    try:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                        counts[idx] += 1
                    except Exception:
                        errors[idx] += 1
                conn.close()
            except Exception:
                errors[idx] += 1

        threads = [threading.Thread(target=_worker, args=(i,), daemon=True)
                   for i in range(self._clients)]
        t0 = time.monotonic()
        for th in threads:
            th.start()
        time.sleep(self._duration)
        stop_event.set()
        for th in threads:
            th.join(timeout=5)
        elapsed = time.monotonic() - t0

        total = sum(counts)
        total_errors = sum(errors)
        tps = total / elapsed

        self._stats["c3_parallel_tps"] = {
            "tps": tps, "clients": self._clients,
            "queries": total, "errors": total_errors,
        }

        self.assert_gt(tps, _MIN_TPS_PARALLEL,
                       f"Parallel TPS {tps:.1f} < minimum {_MIN_TPS_PARALLEL} "
                       f"({self._clients} clients)")
        if total_errors > total * 0.01:
            raise AssertionError(
                f"Error rate too high: {total_errors}/{total} "
                f"({total_errors/total*100:.1f}%)"
            )

    # -----------------------------------------------------------------------
    # C4 — Connection churn rate
    # -----------------------------------------------------------------------

    def test_c4_connection_churn(self) -> None:
        """Rapid connect → query → disconnect must complete within latency budget."""
        self._require_proxy()

        durations: list[float] = []
        iterations = max(20, self._duration * 2)

        for _ in range(iterations):
            t0 = time.monotonic()
            try:
                conn = self._connect()
                conn.autocommit = True
                conn.cursor().execute("SELECT 1")
                conn.close()
                durations.append((time.monotonic() - t0) * 1000.0)
            except Exception as exc:
                raise AssertionError(f"Connection churn attempt failed: {exc}") from exc

        stats = latency_stats(durations)
        self._stats["c4_churn"] = stats

        self.assert_lt(stats["p99_ms"], _MAX_CHURN_DELAY_S * 1000.0,
                       f"p99 churn time {stats['p99_ms']:.1f}ms exceeds budget "
                       f"{_MAX_CHURN_DELAY_S*1000:.0f}ms")

    # -----------------------------------------------------------------------
    # C5 — Large result-set throughput
    # -----------------------------------------------------------------------

    def test_c5_large_resultset_throughput(self) -> None:
        """Fetch a large result set; measure rows-per-second."""
        self._require_proxy()

        conn = self._connect()
        conn.autocommit = True
        cur = conn.cursor()

        # Generate a large in-memory result set via generate_series
        row_count = 10_000
        t0 = time.monotonic()
        cur.execute(f"SELECT i, md5(i::text) FROM generate_series(1, {row_count}) i")
        rows = cur.fetchall()
        elapsed = time.monotonic() - t0
        conn.close()

        self.assert_eq(len(rows), row_count,
                       f"Expected {row_count} rows, got {len(rows)}")
        rps = len(rows) / elapsed
        self._stats["c5_large_result"] = {"rows": len(rows), "elapsed_s": elapsed, "rps": rps}

        if self.verbose:
            print(f"\n    {rps:.0f} rows/s for {row_count} rows", flush=True)

    # -----------------------------------------------------------------------
    # C6 — Prepared-statement throughput
    # -----------------------------------------------------------------------

    def test_c6_prepared_statement_throughput(self) -> None:
        """Measure TPS for a parameterised prepared statement."""
        self._require_proxy()

        conn = self._connect()
        conn.autocommit = True
        cur = conn.cursor()

        # Prepare once, execute many
        cur.execute("PREPARE keel_bench AS SELECT $1::int + $2::int")
        t0 = time.monotonic()
        count = 0
        deadline = t0 + min(self._duration, 10)
        while time.monotonic() < deadline:
            cur.execute("EXECUTE keel_bench(%s, %s)", (count % 1000, count % 500))
            cur.fetchone()
            count += 1

        elapsed = time.monotonic() - t0
        conn.close()

        tps = count / elapsed
        self._stats["c6_prepared_tps"] = {"tps": tps, "queries": count}

        self.assert_gt(tps, _MIN_TPS_SINGLE,
                       f"Prepared-statement TPS {tps:.1f} < minimum {_MIN_TPS_SINGLE}")

    # -----------------------------------------------------------------------
    # C7 — Baseline regression check
    # -----------------------------------------------------------------------

    def test_c7_regression_vs_baseline(self) -> None:
        """Compare current throughput against a previously saved baseline."""
        if not self._stats:
            self.skip("No throughput measurements collected (earlier tests skipped?)")

        # Look for baseline in the env, then the kwarg, then the default path
        baseline_path = (
            os.environ.get("KEEL_BENCH_BASELINE")
            or self._baseline
        )
        if not baseline_path:
            baseline_path = str(
                Path(__file__).resolve().parent.parent.parent
                / "bench" / "baseline.json"
            )

        p = Path(baseline_path)
        if not p.exists():
            self.skip(f"No baseline file at {p} — skipping regression check")

        try:
            baseline = json.loads(p.read_text())
        except Exception as exc:
            self.skip(f"Could not parse baseline file: {exc}")

        regressions: list[str] = []
        tolerance = float(os.environ.get("KEEL_BENCH_TOLERANCE", "0.20"))

        def _check(key: str, metric: str, higher_is_better: bool) -> None:
            cur_val  = self._stats.get(key, {}).get(metric)
            base_val = baseline.get(key, {}).get(metric)
            if cur_val is None or base_val is None:
                return
            if higher_is_better:
                if cur_val < base_val * (1 - tolerance):
                    regressions.append(
                        f"{key}.{metric}: {cur_val:.2f} is >{tolerance*100:.0f}% below "
                        f"baseline {base_val:.2f}"
                    )
            else:
                if cur_val > base_val * (1 + tolerance):
                    regressions.append(
                        f"{key}.{metric}: {cur_val:.2f} is >{tolerance*100:.0f}% above "
                        f"baseline {base_val:.2f}"
                    )

        _check("c1_single_tps",   "tps",    True)
        _check("c3_parallel_tps", "tps",    True)
        _check("c2_latency",      "p99_ms", False)

        if regressions:
            raise AssertionError(
                f"Performance regression(s) detected:\n" + "\n".join(regressions)
            )


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = ThroughputSuite(result, verbose=bool(kwargs.get("verbose")),
                             clients=kwargs.get("clients", _DEFAULT_CLIENTS),
                             duration=kwargs.get("duration", _DEFAULT_DURATION_S),
                             baseline=kwargs.get("baseline"),
                             filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def _add_args(p: "argparse.ArgumentParser") -> None:
    import argparse
    p.add_argument("--clients",  type=int, default=_DEFAULT_CLIENTS,
                   help=f"Concurrent clients (default: {_DEFAULT_CLIENTS})")
    p.add_argument("--duration", type=int, default=_DEFAULT_DURATION_S,
                   help=f"Benchmark duration in seconds (default: {_DEFAULT_DURATION_S})")
    p.add_argument("--baseline", metavar="FILE",
                   help="Path to baseline JSON for regression check")


if __name__ == "__main__":
    import argparse
    standalone_main(ThroughputSuite, "throughput", ThroughputSuite.DESCRIPTION, _add_args)
