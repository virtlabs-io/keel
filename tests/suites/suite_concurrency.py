"""
tests/suites/suite_concurrency.py
==================================
Category B — Concurrency and Race Condition Testing

Tests:
  B1. Full TSAN build via CTest     — ThreadSanitizer on the whole suite
  B2. Concurrency stress (repeated) — run test_concurrency_stress N times
  B3. State-machine stress          — run test_sm_stress N times
  B4. Connection-pool stress        — run test_connpool_stress (exhaust + recover)
  B5. Router stress                 — run test_router_stress + test_router_metrics
  B6. Ring-buffer concurrent        — run test_ringbuf in parallel processes
  B7. 2PC fault injection           — run test_2pc_fault_inject under TSAN
  B8. No TSAN races in key modules  — targeted TSAN run on concurrency-sensitive tests

Run standalone:
    python tests/suites/suite_concurrency.py --verbose
    python tests/suites/suite_concurrency.py --repeat 5
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    find_build_dir,
    find_test_binary,
    find_tsan_build,
    has_sanitizer_error,
    parse_tsan_races,
    run_binary,
    run_ctest,
)

_DEFAULT_REPEAT = 3   # how many times to repeat stress tests


def _run_binary_subprocess(args: tuple) -> tuple[int, str]:
    """Top-level helper so ProcessPoolExecutor can pickle it."""
    binary_path, timeout = args
    import subprocess
    proc = subprocess.run(
        [binary_path], capture_output=True, text=True, timeout=timeout,
    )
    return proc.returncode, proc.stdout + proc.stderr


class ConcurrencySuite(SuiteRunner):
    NAME        = "concurrency"
    DESCRIPTION = "Category B — Concurrency and Race Condition Testing"
    TAGS        = ["concurrency", "tsan", "race", "stress", "threading"]

    def setup(self) -> None:
        self._repeat = int(self.kwargs.get("repeat", _DEFAULT_REPEAT))

    # -----------------------------------------------------------------------
    # B1 — Full TSAN suite
    # -----------------------------------------------------------------------

    def test_b1_tsan_full_ctest_suite(self) -> None:
        """Run the entire CTest suite under ThreadSanitizer."""
        build = find_tsan_build()
        if not build:
            self.skip(
                "build-tsan/ not found — run: "
                "cmake -B build-tsan -DKEEL_SANITIZE=thread && make -C build-tsan -j$(nproc)"
            )
        env = {
            "TSAN_OPTIONS": "halt_on_error=1:second_deadlock_stack=1",
        }
        rc, output = run_ctest(
            build, timeout=600, env=env, jobs=int(os.cpu_count() or 4),
        )
        races = parse_tsan_races(output)
        if rc != 0 or races:
            detail = "\n".join(races[:10]) if races else output[-3000:]
            raise AssertionError(
                f"TSAN CTest suite failed (rc={rc}, {len(races)} race(s)):\n{detail}"
            )

    # -----------------------------------------------------------------------
    # B2 — Concurrency stress (repeated)
    # -----------------------------------------------------------------------

    def test_b2_concurrency_stress_repeated(self) -> None:
        """Run test_concurrency_stress multiple times — any non-zero exit is a race."""
        build = find_tsan_build() or find_build_dir()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_concurrency_stress", build)
        if not binary:
            self.skip(f"test_concurrency_stress not found in {build}")

        failures: list[int] = []
        for i in range(self._repeat):
            rc, output = run_binary(binary, timeout=60)
            if rc != 0 or has_sanitizer_error(output):
                failures.append(i)

        if failures:
            raise AssertionError(
                f"test_concurrency_stress failed on run(s) {failures} "
                f"out of {self._repeat}"
            )

    # -----------------------------------------------------------------------
    # B3 — State-machine stress
    # -----------------------------------------------------------------------

    def test_b3_state_machine_stress(self) -> None:
        """Repeated state-machine stress — validates no internal lock/state corruption."""
        build = find_tsan_build() or find_build_dir()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_sm_stress", build)
        if not binary:
            self.skip(f"test_sm_stress not found in {build}")

        for i in range(self._repeat):
            rc, output = run_binary(binary, timeout=60)
            if rc != 0:
                raise AssertionError(
                    f"test_sm_stress failed on run {i}/{self._repeat} (rc={rc}):\n{output[-1500:]}"
                )
            if has_sanitizer_error(output):
                raise AssertionError(
                    f"Sanitizer error in test_sm_stress run {i}:\n{output[-1500:]}"
                )

    # -----------------------------------------------------------------------
    # B4 — Connection pool stress
    # -----------------------------------------------------------------------

    def test_b4_connpool_stress(self) -> None:
        """Connection pool must handle exhaustion + recovery without data races."""
        build = find_tsan_build() or find_build_dir()
        if not build:
            self.skip("No build directory found")

        for name in ("test_connpool_stress", "test_connpool_exhaust"):
            binary = find_test_binary(name, build)
            if not binary:
                continue
            rc, output = run_binary(binary, timeout=120)
            if rc != 0:
                raise AssertionError(f"{name} failed (rc={rc}):\n{output[-2000:]}")
            if has_sanitizer_error(output):
                raise AssertionError(f"Race detected in {name}:\n{output[-2000:]}")

    # -----------------------------------------------------------------------
    # B5 — Router stress
    # -----------------------------------------------------------------------

    def test_b5_router_stress(self) -> None:
        """Router must handle concurrent routing decisions without corruption."""
        build = find_tsan_build() or find_build_dir()
        if not build:
            self.skip("No build directory found")

        for name in ("test_router_stress", "test_router_metrics"):
            binary = find_test_binary(name, build)
            if not binary:
                continue
            rc, output = run_binary(binary, timeout=60)
            if rc != 0:
                raise AssertionError(f"{name} failed (rc={rc}):\n{output[-1500:]}")
            if has_sanitizer_error(output):
                raise AssertionError(f"Race detected in {name}:\n{output[-1500:]}")

    # -----------------------------------------------------------------------
    # B6 — Ring-buffer concurrent access
    # -----------------------------------------------------------------------

    def test_b6_ringbuf_parallel(self) -> None:
        """Run test_ringbuf in several parallel processes simultaneously."""
        build = find_tsan_build() or find_build_dir()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_ringbuf", build)
        if not binary:
            self.skip(f"test_ringbuf not found in {build}")

        workers = min(4, int(os.cpu_count() or 2))
        tasks = [(str(binary), 60)] * workers

        failures: list[str] = []
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_binary_subprocess, t): i
                       for i, t in enumerate(tasks)}
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    rc, output = fut.result()
                    if rc != 0 or has_sanitizer_error(output):
                        failures.append(f"worker {idx} rc={rc}")
                except Exception as exc:
                    failures.append(f"worker {idx} raised {exc}")

        if failures:
            raise AssertionError(
                f"test_ringbuf parallel failures: {failures}"
            )

    # -----------------------------------------------------------------------
    # B7 — 2PC fault injection under TSAN
    # -----------------------------------------------------------------------

    def test_b7_2pc_fault_inject_tsan(self) -> None:
        """Two-phase commit must handle fault injection without data races."""
        build = find_tsan_build() or find_build_dir()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_2pc_fault_inject", build)
        if not binary:
            self.skip(f"test_2pc_fault_inject not found in {build}")

        rc, output = run_binary(binary, timeout=120)
        if rc != 0:
            raise AssertionError(
                f"test_2pc_fault_inject failed (rc={rc}):\n{output[-2000:]}"
            )
        races = parse_tsan_races(output)
        if races:
            raise AssertionError(
                f"TSAN races in test_2pc_fault_inject: {races[:5]}"
            )

    # -----------------------------------------------------------------------
    # B8 — Targeted TSAN on concurrency-critical modules
    # -----------------------------------------------------------------------

    def test_b8_tsan_targeted_concurrent_modules(self) -> None:
        """Run a targeted TSAN sweep on modules with known concurrency exposure."""
        build = find_tsan_build()
        if not build:
            self.skip("build-tsan/ not found")

        patterns = [
            "test_concurrency",
            "test_connpool",
            "test_router",
            "test_ringbuf",
        ]
        env = {"TSAN_OPTIONS": "halt_on_error=1"}

        for pattern in patterns:
            rc, output = run_ctest(build, regex=pattern, timeout=120, env=env)
            races = parse_tsan_races(output)
            if races or rc != 0:
                raise AssertionError(
                    f"TSAN races/failures for pattern '{pattern}' (rc={rc}):\n"
                    + "\n".join(races[:5])
                )


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = ConcurrencySuite(result, verbose=bool(kwargs.get("verbose")),
                              repeat=kwargs.get("repeat", _DEFAULT_REPEAT),
                              filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def _add_args(p: "argparse.ArgumentParser") -> None:
    p.add_argument(
        "--repeat", type=int, default=_DEFAULT_REPEAT, metavar="N",
        help=f"How many times to repeat each stress test (default: {_DEFAULT_REPEAT})",
    )


if __name__ == "__main__":
    import argparse
    standalone_main(ConcurrencySuite, "concurrency", ConcurrencySuite.DESCRIPTION, _add_args)
