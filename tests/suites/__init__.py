"""
tests/suites/__init__.py
========================
KEEL Test Suite Framework — base types and runner infrastructure.

Design principles:
  - Each suite (A–H) is runnable standalone:
      python tests/suites/suite_memory.py [--verbose] [--json] [--filter SUBSTR]
  - Each suite exports  run(result, **kwargs)  for the coordinator.
  - SuiteResult has the same shape as the one in run_tests.py so the
    coordinator can treat both interchangeably.
  - Tests that cannot run in the current environment call self.skip(reason)
    and the case is recorded as "skipped" (not "failed").
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class CaseResult:
    name:     str
    status:   str  = "not_run"   # passed | failed | skipped | error
    duration: float = 0.0
    detail:   str  = ""
    tags:     list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class SuiteResult:
    name:       str
    status:     str   = "not_run"
    duration:   float = 0.0
    detail:     str   = ""
    returncode: int   = 0
    cases:      list[CaseResult] = field(default_factory=list)
    metrics:    dict  = field(default_factory=dict)

    # --- coordinator-compatible properties --------------------------------

    @property
    def passed(self) -> int:
        return sum(1 for c in self.cases if c.status == "passed")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.cases if c.status in ("failed", "error"))

    @property
    def skipped(self) -> int:
        return sum(1 for c in self.cases if c.status == "skipped")

    def add(self, case: CaseResult) -> None:
        self.cases.append(case)
        if case.status in ("failed", "error") and self.status != "failed":
            self.status = "failed"
        elif self.status == "not_run" and case.status == "passed":
            self.status = "passed"

    def finalize(self) -> None:
        if not self.cases:
            self.status = "skipped"
        elif self.status == "not_run":
            self.status = "passed"
        self.returncode = 1 if self.status == "failed" else 0

    def as_dict(self) -> dict:
        return {
            "name":       self.name,
            "status":     self.status,
            "duration":   round(self.duration, 3),
            "detail":     self.detail,
            "returncode": self.returncode,
            "passed":     self.passed,
            "failed":     self.failed,
            "skipped":    self.skipped,
            "cases":      [c.as_dict() for c in self.cases],
            "metrics":    self.metrics,
        }


# ---------------------------------------------------------------------------
# Skip sentinel
# ---------------------------------------------------------------------------

class SkipTest(Exception):
    """Raised from within a test method to mark the case as skipped."""


# ---------------------------------------------------------------------------
# Base runner
# ---------------------------------------------------------------------------

class SuiteRunner:
    """
    Base class for all KEEL test suites (A–H).

    Subclass it, add methods prefixed with ``test_``, and optionally
    override ``setup()`` / ``teardown()`` for fixture-level logic.
    """

    NAME:        str       = "unnamed"
    DESCRIPTION: str       = ""
    TAGS:        list[str] = []

    def __init__(self, result: SuiteResult, verbose: bool = False,
                 filter_: str | None = None, **kwargs: Any):
        self.result  = result
        self.verbose = verbose
        self.filter  = filter_
        self.kwargs  = kwargs

    # --- lifecycle ---------------------------------------------------------

    def setup(self) -> None:
        """Called once before all test methods."""

    def teardown(self) -> None:
        """Called once after all test methods (even if some fail)."""

    def run_all(self) -> None:
        self.setup()
        try:
            for name in sorted(dir(self)):
                if not name.startswith("test_"):
                    continue
                if self.filter and self.filter not in name:
                    continue
                method = getattr(self, name)
                if callable(method):
                    self._run_case(name, method)
        finally:
            self.teardown()
        self.result.finalize()

    def _run_case(self, name: str, fn: Callable) -> None:
        case = CaseResult(name=name)
        t0 = time.monotonic()
        try:
            fn()
            case.status = "passed"
        except SkipTest as exc:
            case.status = "skipped"
            case.detail = str(exc)
        except AssertionError as exc:
            case.status = "failed"
            case.detail = str(exc) or traceback.format_exc()
        except Exception:
            case.status = "error"
            case.detail = traceback.format_exc()
        finally:
            case.duration = time.monotonic() - t0

        self.result.add(case)
        if self.verbose:
            sym = {"passed": "✓", "failed": "✗", "skipped": "~", "error": "!"}.get(
                case.status, "?"
            )
            dur = f"{case.duration*1000:.0f}ms"
            print(f"  [{sym}] {name:<55}  {dur}", flush=True)
            if case.detail and case.status not in ("passed", "skipped"):
                for line in case.detail.splitlines()[:6]:
                    print(f"       {line}", flush=True)

    # --- assertion helpers -------------------------------------------------

    def skip(self, reason: str) -> None:
        raise SkipTest(reason)

    def assert_eq(self, a: Any, b: Any, msg: str = "") -> None:
        if a != b:
            raise AssertionError(msg or f"expected {b!r}, got {a!r}")

    def assert_true(self, cond: bool, msg: str = "") -> None:
        if not cond:
            raise AssertionError(msg or "assertion failed")

    def assert_false(self, cond: bool, msg: str = "") -> None:
        if cond:
            raise AssertionError(msg or "expected False")

    def assert_in(self, item: Any, container: Any, msg: str = "") -> None:
        if item not in container:
            raise AssertionError(msg or f"{item!r} not found in container")

    def assert_not_in(self, item: Any, container: Any, msg: str = "") -> None:
        if item in container:
            raise AssertionError(msg or f"{item!r} unexpectedly found in container")

    def assert_lt(self, a: float, b: float, msg: str = "") -> None:
        if not a < b:
            raise AssertionError(msg or f"{a} is not < {b}")

    def assert_gt(self, a: float, b: float, msg: str = "") -> None:
        if not a > b:
            raise AssertionError(msg or f"{a} is not > {b}")


# ---------------------------------------------------------------------------
# Standalone entry point helper
# ---------------------------------------------------------------------------

def standalone_main(
    runner_cls: type[SuiteRunner],
    suite_name: str,
    description: str,
    extra_args_fn: Callable[[argparse.ArgumentParser], None] | None = None,
) -> None:
    """Standard CLI entry point for running a suite standalone."""
    parser = argparse.ArgumentParser(
        prog=f"suite_{suite_name}.py",
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print each test case as it runs")
    parser.add_argument("--json", action="store_true",
                        help="Emit a JSON report to stdout")
    parser.add_argument("--filter", metavar="SUBSTR",
                        help="Only run test methods whose names contain SUBSTR")
    parser.add_argument("--list", action="store_true",
                        help="List all test method names and exit")
    parser.add_argument("--json-out", metavar="PATH",
                        help="Write JSON report to PATH (in addition to normal output)")

    if extra_args_fn:
        extra_args_fn(parser)

    args = parser.parse_args()

    if args.list:
        dummy = SuiteResult(name=suite_name)
        inst = runner_cls.__new__(runner_cls)
        inst.result  = dummy
        inst.verbose = False
        inst.filter  = None
        inst.kwargs  = {}
        print(f"\n{suite_name} test cases:")
        for name in sorted(dir(inst)):
            if name.startswith("test_"):
                print(f"  {name}")
        print()
        return

    kwargs: dict[str, Any] = {}
    for k, v in vars(args).items():
        if k not in ("verbose", "json", "filter", "list"):
            kwargs[k] = v

    result = SuiteResult(name=suite_name)
    t0 = time.monotonic()

    if args.verbose:
        print(f"\n{'='*60}")
        print(f"Running suite: {suite_name}")
        print(f"{'='*60}")

    runner = runner_cls(result, verbose=args.verbose, filter_=args.filter, **kwargs)
    runner.run_all()
    result.duration = time.monotonic() - t0

    if getattr(args, "json_out", None):
        import pathlib
        p = pathlib.Path(args.json_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(result.as_dict(), indent=2))

    if args.json:
        print(json.dumps(result.as_dict(), indent=2))
    else:
        _print_text_summary(result)

    sys.exit(result.returncode)


def _print_text_summary(result: SuiteResult) -> None:
    width = 72
    sym_map = {"passed": "✓", "failed": "✗", "skipped": "~", "error": "!"}
    print(f"\n{'='*width}")
    status_str = result.status.upper()
    print(f"Suite: {result.name}  [{status_str}]  ({result.duration:.2f}s)")
    print(f"  {result.passed} passed  {result.failed} failed  {result.skipped} skipped")

    if result.cases:
        print(f"\n  {'Test':<56} {'Status':>7}  {'ms':>7}")
        print(f"  {'-'*56} {'-'*7}  {'-'*7}")
        for c in result.cases:
            sym = sym_map.get(c.status, "?")
            dur = f"{c.duration*1000:.0f}"
            print(f"  [{sym}] {c.name:<56} {c.status:>7}  {dur:>7}")

    if result.metrics:
        print("\n  --- Metrics ---")
        for key, val in sorted(result.metrics.items()):
            if isinstance(val, dict):
                print(f"  {key}:")
                for k, v in sorted(val.items()):
                    print(f"    {k}: {v}")
            else:
                print(f"  {key}: {val}")

    if result.failed:
        print("\n  Failed:")
        for c in result.cases:
            if c.status in ("failed", "error"):
                print(f"  ✗ {c.name}")
                for line in (c.detail or "").splitlines()[:8]:
                    print(f"      {line}")
    print()
