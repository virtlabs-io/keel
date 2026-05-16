"""
tests/suites/suite_memory.py
============================
Category A — Memory Correctness and Leak Detection

Tests:
  A1. Full ASAN build via CTest  — catches heap-buffer-overflow, use-after-free, …
  A2. LSAN build via CTest       — dedicated leak detection
  A3. Targeted Valgrind run      — independent cross-check (skipped if valgrind absent)
  A4. Allocation-failure injection — every malloc can fail; code must handle it cleanly
  A5. UBSAN coverage             — undefined behaviour in ASAN build output
  A6. Buffer bounds tests        — ring buffer, circular buffer, hash table overflows
  A7. Memory-safety unit tests   — the explicit test_mem_safety binary
  A8. Connection-pool ASAN run   — pool allocator correctness under ASAN

Run standalone:
    python tests/suites/suite_memory.py --verbose
    python tests/suites/suite_memory.py --json > memory_report.json

Run via coordinator:
    python tests/run_tests.py --suite memory
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

# Allow running as a standalone script from any cwd
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    check_command,
    find_asan_build,
    find_test_binary as find_binary,
    find_lsan_build,
    find_test_binary,
    has_sanitizer_error,
    parse_asan_errors,
    run_binary,
    run_ctest,
)


class MemorySuite(SuiteRunner):
    NAME        = "memory"
    DESCRIPTION = "Category A — Memory Correctness and Leak Detection"
    TAGS        = ["memory", "asan", "lsan", "valgrind", "leak"]

    # -----------------------------------------------------------------------
    # A1 — Full ASAN build via CTest
    # -----------------------------------------------------------------------

    def test_a1_asan_full_ctest_suite(self) -> None:
        """Run the entire CTest suite under AddressSanitizer."""
        build = find_asan_build()
        if not build:
            self.skip("build-asan/ not found — run: cmake -B build-asan -DKEEL_SANITIZE=address && make -C build-asan -j$(nproc)")

        env = {
            "ASAN_OPTIONS": "halt_on_error=1:detect_leaks=1:abort_on_error=1",
            "UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1",
        }
        rc, output = run_ctest(build, timeout=600, env=env, jobs=int(os.cpu_count() or 4))
        errors = parse_asan_errors(output)
        if rc != 0 or errors:
            detail = "\n".join(errors[:10]) if errors else output[-3000:]
            raise AssertionError(
                f"ASAN CTest suite failed (rc={rc}, {len(errors)} sanitizer error(s)):\n{detail}"
            )

    # -----------------------------------------------------------------------
    # A2 — LSAN dedicated leak detection
    # -----------------------------------------------------------------------

    def test_a2_lsan_build(self) -> None:
        """Run CTest with leak-sanitizer build (build-lsan/)."""
        build = find_lsan_build()
        if not build:
            self.skip("build-lsan/ not found — run: cmake -B build-lsan -DKEEL_SANITIZE=leak && make -C build-lsan -j$(nproc)")

        env = {
            "LSAN_OPTIONS": "halt_on_error=1:max_leaks=0",
            "ASAN_OPTIONS": "detect_leaks=1",
        }
        rc, output = run_ctest(build, timeout=600, env=env)
        if rc != 0:
            raise AssertionError(f"LSAN build CTest failed (rc={rc}):\n{output[-3000:]}")
        # LSAN writes leak summaries even on rc=0 in some configurations
        if "SUMMARY: LeakSanitizer" in output and "0 bytes" not in output:
            raise AssertionError(f"Memory leaks detected:\n{output[-2000:]}")

    # -----------------------------------------------------------------------
    # A3 — Valgrind spot-check
    # -----------------------------------------------------------------------

    def test_a3_valgrind_mem_safety_binary(self) -> None:
        """Run test_mem_safety under Valgrind Memcheck."""
        if not check_command("valgrind"):
            self.skip("valgrind not installed")

        build = find_asan_build() or _find_any_build()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_mem_safety", build)
        if not binary:
            self.skip(f"test_mem_safety binary not found in {build}")

        rc, output = run_binary(
            Path("valgrind"),
            args=[
                "--tool=memcheck",
                "--error-exitcode=1",
                "--leak-check=full",
                "--show-leak-kinds=definite,indirect",
                "--track-origins=yes",
                "--quiet",
                str(binary),
            ],
            timeout=180,
        )
        if rc != 0:
            raise AssertionError(f"Valgrind memcheck reported errors (rc={rc}):\n{output[-3000:]}")

    # -----------------------------------------------------------------------
    # A4 — Allocation-failure injection
    # -----------------------------------------------------------------------

    def test_a4_alloc_inject(self) -> None:
        """Every malloc-call site must handle allocation failure gracefully."""
        build = find_asan_build() or _find_any_build()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_alloc_inject", build)
        if not binary:
            self.skip(f"test_alloc_inject not found in {build}")

        rc, output = run_binary(binary, timeout=120)
        if rc != 0:
            raise AssertionError(
                f"Alloc injection test failed (rc={rc}):\n{output[-2000:]}"
            )
        # Sanity: if ASAN is active, check for sanitizer errors
        if has_sanitizer_error(output):
            raise AssertionError(f"Sanitizer errors in alloc-inject run:\n{output[-2000:]}")

    # -----------------------------------------------------------------------
    # A5 — UBSAN coverage (from ASAN build output)
    # -----------------------------------------------------------------------

    def test_a5_ubsan_no_undefined_behaviour(self) -> None:
        """UndefinedBehaviorSanitizer must not fire on the default test suite."""
        build = find_asan_build()
        if not build:
            self.skip("build-asan/ not found")

        env = {
            "UBSAN_OPTIONS": "halt_on_error=0:print_stacktrace=1:log_path=/tmp/keel_ubsan",
        }
        rc, output = run_ctest(build, timeout=600, env=env)
        ubsan_errors = [e for e in parse_asan_errors(output)
                        if "UndefinedBehaviorSanitizer" in e or "runtime error" in e]
        if ubsan_errors:
            raise AssertionError(
                f"{len(ubsan_errors)} UBSAN error(s) found:\n" + "\n".join(ubsan_errors[:8])
            )

    # -----------------------------------------------------------------------
    # A6 — Buffer bounds tests
    # -----------------------------------------------------------------------

    def test_a6_buffer_bounds(self) -> None:
        """Ring buffer and circular buffer must not overflow under ASAN."""
        build = find_asan_build() or _find_any_build()
        if not build:
            self.skip("No build directory found")

        for name in ("test_buffer", "test_ringbuf"):
            binary = find_test_binary(name, build)
            if not binary:
                continue
            env = {"ASAN_OPTIONS": "halt_on_error=1:detect_leaks=0"}
            rc, output = run_binary(binary, timeout=60, env=env)
            if rc != 0:
                raise AssertionError(f"{name} failed (rc={rc}):\n{output[-1500:]}")
            if has_sanitizer_error(output):
                raise AssertionError(f"ASAN error in {name}:\n{output[-1500:]}")

    # -----------------------------------------------------------------------
    # A7 — Memory-safety unit tests
    # -----------------------------------------------------------------------

    def test_a7_mem_safety_unit(self) -> None:
        """Dedicated memory-safety test binary (boundary checks, sentinel validation)."""
        build = find_asan_build() or _find_any_build()
        if not build:
            self.skip("No build directory found")

        binary = find_test_binary("test_mem_safety", build)
        if not binary:
            self.skip("test_mem_safety binary not found")

        rc, output = run_binary(binary, timeout=60)
        if rc != 0:
            raise AssertionError(f"test_mem_safety failed (rc={rc}):\n{output[-2000:]}")

    # -----------------------------------------------------------------------
    # A8 — Connection pool allocator under ASAN
    # -----------------------------------------------------------------------

    def test_a8_connpool_asan(self) -> None:
        """Connection pool must not leak or corrupt memory under ASAN."""
        build = find_asan_build()
        if not build:
            self.skip("build-asan/ not found")

        env = {"ASAN_OPTIONS": "halt_on_error=1:detect_leaks=1"}
        rc, output = run_ctest(
            build, regex="test_connpool$", timeout=120, env=env,
        )
        if rc != 0:
            raise AssertionError(
                f"test_connpool ASAN run failed (rc={rc}):\n{output[-2000:]}"
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_any_build() -> "Path | None":
    from tests.suites.common import find_build_dir
    return find_build_dir()


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    """Called by tests/run_tests.py."""
    runner = MemorySuite(result, verbose=bool(kwargs.get("verbose")),
                         filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    standalone_main(MemorySuite, "memory", MemorySuite.DESCRIPTION)
