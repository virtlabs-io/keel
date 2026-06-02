#!/usr/bin/env python3
"""
tests/run_tests.py — KEEL Master Test Coordinator
==================================================

Single entry point to run any combination of KEEL test suites, collect
results, and produce a unified JSON + HTML report.

Suites
------
  unit         C unit/integration tests via CTest (requires a CMake build)
  e2e          Python pytest end-to-end suite (Docker Compose stack)
  integration  Docker-backed integration tests (pg, mysql, cloud-auth, sharding)
  hardening    Security and resilience hardening checks
  chaos        Chaos/fault-injection scenarios (live Docker stack required)
  fuzz         OSS-Fuzz build verification (does not run long-running fuzz jobs)

  Python standalone suites (tests/suites/) — run without Docker:
  memory       Memory correctness & leak detection (ASAN/LSAN/Valgrind)
  concurrency  Race condition detection (TSAN + stress binaries)
  throughput   Throughput & latency baseline vs. a live proxy
  protocol     PostgreSQL wire-protocol compliance & fuzzing (raw TCP)
  resilience   Error path & fault-injection resilience tests
  regression   Regression / integration correctness tests vs. a live proxy
  chaos-py     Network chaos via tc netem (requires root / sudo)
  soak         Longevity / soak tests — memory, FD, error-rate stability

Usage
-----
  # Run all suites (build-dir auto-detected):
  python3 tests/run_tests.py

  # Run specific suites:
  python3 tests/run_tests.py --suite unit --suite e2e

  # Unit tests against an existing build:
  python3 tests/run_tests.py --suite unit --build-dir build

  # E2E only, skip docker image rebuild:
  python3 tests/run_tests.py --suite e2e --no-build

  # CI mode (no interactive prompts, non-zero exit on any failure):
  python3 tests/run_tests.py --ci --suite unit --suite e2e

  # List available suites:
  python3 tests/run_tests.py --list

  # Dry-run (print what would be executed):
  python3 tests/run_tests.py --dry-run

Environment variables
---------------------
  KEEL_BUILD_DIR       Override default build directory (default: build)
  KEEL_E2E_SKIP_BUILD  Set to 1 to skip KEEL Docker image rebuild in e2e suite
  KEEL_E2E_KEEP_STACK  Set to 1 to leave the Docker stack running after e2e
  CI                   If set, enables --ci behaviour automatically
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT   = Path(__file__).resolve().parent.parent
TESTS_DIR   = REPO_ROOT / "tests"
REPORTS_DIR = TESTS_DIR / "reports"

# Build the set of acceptable source-directory strings so we match
# CTestTestfile.cmake entries even when the project was configured via a
# symlink path (e.g. /proj -> /home/charly/proj).
_REPO_ROOT_ALIASES: set[str] = {str(REPO_ROOT)}
try:
    _real = str(Path(REPO_ROOT).resolve(strict=False))
    _REPO_ROOT_ALIASES.add(_real)
    # Also add the path as seen through known top-level symlinks such as /proj
    import os as _os
    for _entry in Path("/").iterdir():
        try:
            if _entry.is_symlink():
                _target = _entry.resolve(strict=False)
                _real_proj = Path(_real)
                try:
                    _rel = _real_proj.relative_to(_target)
                    _REPO_ROOT_ALIASES.add(str(_entry / _rel))
                except ValueError:
                    pass
        except (OSError, PermissionError):
            pass
except Exception:
    pass


# ---------------------------------------------------------------------------
# Colours (disabled when not a TTY or when CI=true)
# ---------------------------------------------------------------------------
_USE_COLOR = sys.stdout.isatty() and os.environ.get("CI", "") == ""

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text

def info(msg: str)  -> None: print(_c("0;36",  f"[i] {msg}"))
def ok(msg: str)    -> None: print(_c("0;32",  f"[✓] {msg}"))
def warn(msg: str)  -> None: print(_c("1;33",  f"[!] {msg}"))
def error(msg: str) -> None: print(_c("0;31",  f"[✗] {msg}"), file=sys.stderr)
def head(msg: str)  -> None: print(_c("1;36",  f"\n{'='*68}\n  {msg}\n{'='*68}"))


# ---------------------------------------------------------------------------
# Suite result dataclass
# ---------------------------------------------------------------------------
class SuiteResult:
    def __init__(self, name: str) -> None:
        self.name       = name
        self.status     = "skipped"   # "passed" | "failed" | "skipped" | "error"
        self.returncode = 0
        self.duration   = 0.0
        self.detail     = ""

    def as_dict(self) -> dict:
        return {
            "suite":      self.name,
            "status":     self.status,
            "returncode": self.returncode,
            "duration_s": round(self.duration, 2),
            "detail":     self.detail,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _find_build_dir(build_dir_arg: Optional[str]) -> Optional[Path]:
    """
    Return the best available CMake build directory compiled for this host.

    Resolution order:
      1. The path supplied by --build-dir (no source-path validation — the
         caller has explicitly chosen that directory).
      2. A prioritised set of well-known names that are expected to be host
         builds: build-host, build-test, build-asan, build-lsan, build-tsan,
         build-release, build.
      3. A broad scan of every directory under REPO_ROOT whose name starts with
         "build" — picks the first one whose CTestTestfile.cmake records the
         current REPO_ROOT as its source directory.

    Directories compiled inside Docker containers carry a different source path
    (e.g. /keel/ or /proj/virtlabs/keel) and are skipped because their test
    executables cannot be run on the host.
    """
    if build_dir_arg:
        p = Path(build_dir_arg)
        resolved = p if p.is_absolute() else REPO_ROOT / p
        if not resolved.exists():
            error(f"--build-dir '{resolved}' does not exist.")
            return None
        if not (resolved / "CTestTestfile.cmake").exists():
            error(f"--build-dir '{resolved}' has no CTestTestfile.cmake — "
                  "is this a CMake build directory?")
            return None
        # Warn but still honour an explicitly-specified Docker build dir so the
        # user can override when they know what they are doing.
        try:
            content = (resolved / "CTestTestfile.cmake").read_text(errors="replace")
            if not any(f"# Source directory: {alias}" in content
                       for alias in _REPO_ROOT_ALIASES):
                warn(
                    f"Build directory '{resolved}' was compiled with a different "
                    "source root (likely a Docker build).  Test executables may "
                    "not run on this host.  Use 'build-host' for a native build."
                )
        except OSError:
            pass
        return resolved

    # Prioritised candidates — host builds first, Docker-compiled last
    priority = [
        "build-host",      # explicit host-build convention
        "build-test",      # CI test builds
        "build-asan",
        "build-lsan",
        "build-tsan",
        "build-pgo",
        "build-bufring-check",
        "build-release",
        "build",           # generic / Docker overlay (may not run on host)
    ]

    # First pass: prioritised names that also have a matching source path
    for candidate in priority:
        p = REPO_ROOT / candidate
        ctest_file = p / "CTestTestfile.cmake"
        if not ctest_file.exists():
            continue
        try:
            content = ctest_file.read_text(errors="replace")
            if any(f"# Source directory: {alias}" in content
                   for alias in _REPO_ROOT_ALIASES):
                return p
        except OSError:
            continue

    # Second pass: broad scan — any build* directory with the right source path
    try:
        for entry in sorted(REPO_ROOT.iterdir()):
            if not entry.is_dir() or not entry.name.startswith("build"):
                continue
            ctest_file = entry / "CTestTestfile.cmake"
            if not ctest_file.exists():
                continue
            try:
                content = ctest_file.read_text(errors="replace")
                if any(f"# Source directory: {alias}" in content
                       for alias in _REPO_ROOT_ALIASES):
                    return entry
            except OSError:
                continue
    except OSError:
        pass

    return None


def _run(cmd: list[str], *, env: Optional[dict] = None, cwd: Optional[Path] = None,
         dry_run: bool = False, timeout: Optional[int] = None) -> int:
    """Run a command, stream output, and return the exit code."""
    full_env = {**os.environ, **(env or {})}
    display   = " ".join(str(c) for c in cmd)
    info(f"Running: {display}")
    if dry_run:
        print(f"  [dry-run] would execute: {display}")
        return 0
    t0 = time.monotonic()
    try:
        result = subprocess.run(cmd, env=full_env, cwd=cwd or REPO_ROOT,
                                timeout=timeout)
        elapsed = time.monotonic() - t0
        print(f"  exit={result.returncode}  elapsed={elapsed:.1f}s")
        return result.returncode
    except subprocess.TimeoutExpired:
        error(f"Command timed out after {timeout}s: {display}")
        return 124
    except FileNotFoundError as exc:
        error(f"Command not found: {exc}")
        return 127


def _check_prereq(cmd: str) -> bool:
    return shutil.which(cmd) is not None


# ---------------------------------------------------------------------------
# Suite runners
# ---------------------------------------------------------------------------

def _cmake_configure(build_dir: Path, dry_run: bool, jobs: int = 0) -> int:
    """
    Run ``cmake -B <build_dir> -S <REPO_ROOT>`` to configure a fresh build.
    Returns the cmake exit code.
    """
    if not _check_prereq("cmake"):
        error("cmake not found in PATH — cannot configure.")
        return 127
    cmd = ["cmake", "-B", str(build_dir), "-S", str(REPO_ROOT)]
    return _run(cmd, dry_run=dry_run)


def _cmake_build(build_dir: Path, dry_run: bool, jobs: int = 0) -> int:
    """
    Run ``cmake --build <build_dir>`` to compile all targets (including tests).
    Returns the cmake exit code.
    """
    if not _check_prereq("cmake"):
        error("cmake not found in PATH — cannot build.")
        return 127
    cmd = ["cmake", "--build", str(build_dir)]
    if jobs > 0:
        cmd += ["--parallel", str(jobs)]
    return _run(cmd, dry_run=dry_run)


def run_unit(result: SuiteResult, build_dir: Optional[Path], dry_run: bool,
             jobs: int = 0, ctest_args: list[str] | None = None,
             cmake_build: bool = False, configure: bool = False) -> None:
    """Run CTest unit + integration tests, optionally building first via cmake."""

    # ── Optional: configure a brand-new build directory ───────────────────
    if configure:
        if build_dir is None:
            build_dir = REPO_ROOT / "build-host"
            info(f"No build dir specified; will configure a new one at {build_dir}")
        info(f"Configuring CMake build in {build_dir} ...")
        rc = _cmake_configure(build_dir, dry_run, jobs)
        if rc != 0:
            result.status  = "error"
            result.detail  = f"cmake configure failed (exit {rc}) in {build_dir}"
            error(result.detail)
            return

    # ── Guard: need a build dir from here on ─────────────────────────────
    if build_dir is None:
        result.status = "error"
        result.detail = (
            "No CMake build directory found.\n"
            "Options:\n"
            "  1. Build manually:  cmake -B build-host && cmake --build build-host\n"
            "  2. Auto-build:      python3 tests/run_tests.py --suite unit --cmake-build\n"
            "  3. Full configure:  python3 tests/run_tests.py --suite unit --configure\n"
            "  4. Specify dir:     python3 tests/run_tests.py --suite unit "
            "--build-dir build-host"
        )
        error(result.detail)
        return

    if not (build_dir / "CTestTestfile.cmake").exists() and not (configure or cmake_build):
        result.status = "error"
        result.detail = (
            f"CTestTestfile.cmake not found in {build_dir}.\n"
            f"The directory exists but has not been fully built yet.\n"
            f"Run with --cmake-build to build it automatically, or:\n"
            f"  cmake --build {build_dir}"
        )
        error(result.detail)
        return

    # ── Optional: build (or rebuild) via cmake ────────────────────────────
    if cmake_build:
        info(f"Building via cmake in {build_dir} ...")
        rc = _cmake_build(build_dir, dry_run, jobs)
        if rc != 0:
            result.status  = "error"
            result.detail  = f"cmake --build failed (exit {rc}) in {build_dir}"
            error(result.detail)
            return

    # ── Guard: CTestTestfile must now exist ───────────────────────────────
    if not (build_dir / "CTestTestfile.cmake").exists():
        result.status = "error"
        result.detail = f"CTestTestfile.cmake still not found in {build_dir} after build step."
        error(result.detail)
        return

    if not _check_prereq("ctest"):
        result.status = "error"
        result.detail = "ctest not found in PATH"
        error(result.detail)
        return

    cmd = ["ctest", "--test-dir", str(build_dir), "--output-on-failure"]
    if jobs > 0:
        cmd += ["-j", str(jobs)]
    cmd += (ctest_args or [])

    t0 = time.monotonic()
    rc = _run(cmd, dry_run=dry_run)
    result.duration   = time.monotonic() - t0
    result.returncode = rc
    result.status     = "passed" if rc == 0 else "skipped" if rc == 77 else "failed"


def run_e2e(result: SuiteResult, dry_run: bool, no_build: bool,
            keep_stack: bool, marker: str = "", parallel: bool = False,
            report_dir: Optional[Path] = None) -> None:
    """Run the Python pytest E2E suite."""
    e2e_dir  = TESTS_DIR / "e2e"
    venv_py  = e2e_dir / ".venv" / "bin" / "python"
    runner   = e2e_dir / "run_e2e.sh"

    if not runner.exists():
        result.status = "error"
        result.detail = f"run_e2e.sh not found at {runner}"
        error(result.detail)
        return

    cmd = ["bash", str(runner)]
    if no_build:
        cmd += ["--no-build"]
    if keep_stack:
        cmd += ["--keep-stack"]
    if marker:
        cmd += ["--only", marker]
    if parallel:
        cmd += ["--parallel"]
    if report_dir:
        cmd += ["--report-dir", str(report_dir)]

    env: dict[str, str] = {}
    if no_build:
        env["KEEL_E2E_SKIP_BUILD"] = "1"
    if keep_stack:
        env["KEEL_E2E_KEEP_STACK"] = "1"

    t0 = time.monotonic()
    rc = _run(cmd, env=env, dry_run=dry_run)
    result.duration   = time.monotonic() - t0
    result.returncode = rc
    result.status     = "passed" if rc == 0 else "skipped" if rc == 77 else "failed"


def run_integration(result: SuiteResult, test_name: str, dry_run: bool,
                    timeout: int = 600) -> None:
    """Run a single integration test from tests/integration/.

    Supports two modes determined by the file extension:
      *.sh  — invoked directly with bash (legacy integration scripts)
      *.py  — invoked via ``python -m pytest`` (modern pytest-based tests)
    """
    integ_dir = TESTS_DIR / "integration"
    script    = integ_dir / test_name

    if not script.exists():
        result.status = "error"
        result.detail = f"Integration script not found: {script}"
        error(result.detail)
        return

    if not _check_prereq("docker"):
        result.status = "error"
        result.detail = "docker not found in PATH"
        error(result.detail)
        return

    if test_name.endswith(".py"):
        # Resolve a Python interpreter that has pytest installed.
        # Priority: integration venv → e2e venv → PATH pytest → sys.executable
        _py_candidates = [
            TESTS_DIR / "integration" / ".venv" / "bin" / "python",
            TESTS_DIR / "e2e" / ".venv" / "bin" / "python",
        ]
        py_exe = next((str(p) for p in _py_candidates if p.exists()), None)
        if py_exe is None:
            pytest_bin = shutil.which("pytest")
            py_exe = sys.executable if pytest_bin is None else None

        if py_exe is not None:
            cmd = [py_exe, "-m", "pytest", str(script), "-v", "--tb=short",
                   f"--timeout={timeout}"]
        else:
            # pytest found directly in PATH
            cmd = [pytest_bin, str(script), "-v", "--tb=short",
                   f"--timeout={timeout}"]
    else:
        cmd = ["bash", str(script)]

    t0 = time.monotonic()
    rc = _run(cmd, dry_run=dry_run, timeout=timeout)
    result.duration   = time.monotonic() - t0
    result.returncode = rc
    result.status     = "passed" if rc == 0 else "skipped" if rc == 77 else "failed"


def run_hardening(result: SuiteResult, dry_run: bool, env_overrides: dict | None = None,
                  timeout: int = 1800) -> None:
    """Run the consolidated hardening suite via tests/hardening/run_all.sh."""
    runner = TESTS_DIR / "hardening" / "run_all.sh"

    if not runner.exists():
        result.status = "error"
        result.detail = f"run_all.sh not found at {runner}"
        error(result.detail)
        return

    t0 = time.monotonic()
    rc = _run(["bash", str(runner)], env=env_overrides, dry_run=dry_run,
              timeout=timeout)
    result.duration   = time.monotonic() - t0
    result.returncode = rc
    result.status     = "passed" if rc == 0 else "skipped" if rc == 77 else "failed"


def run_chaos(result: SuiteResult, dry_run: bool, scenarios: list[str] | None = None,
              timeout: int = 600) -> None:
    """Run chaos scenarios via tests/chaos/run-chaos.sh."""
    runner = TESTS_DIR / "chaos" / "run-chaos.sh"

    if not runner.exists():
        result.status = "error"
        result.detail = f"run-chaos.sh not found at {runner}"
        error(result.detail)
        return

    if not _check_prereq("docker"):
        result.status = "error"
        result.detail = "docker not found in PATH"
        error(result.detail)
        return

    cmd = ["bash", str(runner)] + (scenarios or [])
    t0 = time.monotonic()
    rc = _run(cmd, dry_run=dry_run, timeout=timeout)
    result.duration   = time.monotonic() - t0
    result.returncode = rc
    result.status     = "passed" if rc == 0 else "skipped" if rc == 77 else "failed"


def run_fuzz_build(result: SuiteResult, dry_run: bool) -> None:
    """Verify the OSS-Fuzz build script is syntactically valid and runnable."""
    fuzz_script = REPO_ROOT / "oss-fuzz" / "build.sh"

    if not fuzz_script.exists():
        result.status = "error"
        result.detail = f"oss-fuzz/build.sh not found at {fuzz_script}"
        error(result.detail)
        return

    # Syntax-check only (bash -n); full fuzz builds require OSS-Fuzz infra
    t0 = time.monotonic()
    rc = _run(["bash", "-n", str(fuzz_script)], dry_run=dry_run)
    result.duration   = time.monotonic() - t0
    result.returncode = rc
    result.status     = "passed" if rc == 0 else "failed"
    if rc == 0:
        result.detail = (
            "oss-fuzz/build.sh syntax OK. "
            "Full fuzz builds require: python infra/helper.py build_fuzzers keel"
        )


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>KEEL Test Report — {timestamp}</title>
  <style>
    body {{ font-family: ui-monospace, monospace; background: #0d1117; color: #c9d1d9;
            max-width: 900px; margin: 40px auto; padding: 0 20px; }}
    h1   {{ color: #58a6ff; }}
    h2   {{ color: #8b949e; font-size: 0.9em; font-weight: normal; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 24px; }}
    th   {{ background: #161b22; color: #8b949e; text-align: left; padding: 8px 12px;
            border-bottom: 1px solid #30363d; }}
    td   {{ padding: 8px 12px; border-bottom: 1px solid #21262d; }}
    .passed  {{ color: #3fb950; }}
    .failed  {{ color: #f85149; }}
    .skipped {{ color: #d29922; }}
    .error   {{ color: #ff7b72; }}
    .total   {{ font-weight: bold; background: #161b22; }}
    pre  {{ background: #161b22; padding: 12px; border-radius: 6px;
            overflow-x: auto; font-size: 0.85em; white-space: pre-wrap; }}
  </style>
</head>
<body>
  <h1>KEEL Test Report</h1>
  <h2>Generated: {timestamp} &nbsp;&bull;&nbsp; Duration: {total_duration:.1f}s</h2>

  <table>
    <tr>
      <th>Suite</th><th>Status</th><th>Duration (s)</th><th>Detail</th>
    </tr>
    {rows}
    <tr class="total">
      <td>Total: {total}</td>
      <td>
        <span class="passed">{passed} passed</span> &nbsp;
        <span class="failed">{failed} failed</span> &nbsp;
        <span class="skipped">{skipped} skipped</span>
      </td>
      <td>{total_duration:.1f}s</td>
      <td></td>
    </tr>
  </table>
</body>
</html>
"""

_ROW_TEMPLATE = """\
    <tr>
      <td>{name}</td>
      <td class="{status}">{status}</td>
      <td>{duration:.1f}</td>
      <td><pre>{detail}</pre></td>
    </tr>"""


def generate_reports(results: list[SuiteResult], report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_file   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    total_duration = sum(r.duration for r in results)
    passed   = sum(1 for r in results if r.status == "passed")
    failed   = sum(1 for r in results if r.status == "failed")
    skipped  = sum(1 for r in results if r.status == "skipped")
    errored  = sum(1 for r in results if r.status == "error")

    # JSON
    data = {
        "timestamp":   timestamp,
        "duration_s":  round(total_duration, 2),
        "summary":     {"total": len(results), "passed": passed, "failed": failed,
                        "skipped": skipped, "error": errored},
        "suites":      [r.as_dict() for r in results],
    }
    json_path = report_dir / f"report_{ts_file}.json"
    json_path.write_text(json.dumps(data, indent=2))
    (report_dir / "report_latest.json").write_text(json.dumps(data, indent=2))

    # HTML
    rows = "\n".join(
        _ROW_TEMPLATE.format(
            name=r.name, status=r.status, duration=r.duration,
            detail=(r.detail or "")[:500],
        )
        for r in results
    )
    html = _HTML_TEMPLATE.format(
        timestamp=timestamp,
        total_duration=total_duration,
        rows=rows,
        total=len(results),
        passed=passed,
        failed=failed,
        skipped=skipped,
    )
    html_path = report_dir / f"report_{ts_file}.html"
    html_path.write_text(html)
    (report_dir / "report_latest.html").write_text(html)

    ok(f"Reports written to {report_dir}/")
    print(f"  HTML: {html_path}")
    print(f"  JSON: {json_path}")


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------
def print_summary(results: list[SuiteResult]) -> None:
    head("Test Run Summary")
    total_dur = sum(r.duration for r in results)
    col_w = max(len(r.name) for r in results) + 2

    print(f"  {'Suite':<{col_w}}  {'Status':<10}  {'Duration':>10}")
    print(f"  {'-'*col_w}  {'-'*10}  {'-'*10}")
    for r in results:
        status_colored = {
            "passed":  _c("0;32", "passed"),
            "failed":  _c("0;31", "FAILED"),
            "skipped": _c("1;33", "skipped"),
            "error":   _c("0;31", "ERROR"),
        }.get(r.status, r.status)
        print(f"  {r.name:<{col_w}}  {status_colored:<10}  {r.duration:>9.1f}s")

    print(f"\n  Total duration: {total_dur:.1f}s")

    passed  = sum(1 for r in results if r.status == "passed")
    failed  = sum(1 for r in results if r.status in ("failed", "error"))
    skipped = sum(1 for r in results if r.status == "skipped")

    print(f"  {_c('0;32', f'{passed} passed')}  "
          f"{_c('0;31', f'{failed} failed')}  "
          f"{_c('1;33', f'{skipped} skipped')}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
ALL_SUITES = [
    "unit", "e2e", "integration", "hardening", "chaos", "fuzz",
    # Python standalone suites (tests/suites/)
    "memory", "concurrency", "throughput", "protocol",
    "resilience", "regression", "chaos-py", "soak",
]

SUITE_DESCRIPTIONS = {
    "unit":        "C unit + integration tests via CTest",
    "e2e":         "Python pytest end-to-end suite (Docker, all features)",
    "integration": "Docker-backed integration tests (pg, mysql, cloud-auth, sharding)",
    "hardening":   "Security and resilience hardening checks",
    "chaos":       "Chaos/fault-injection scenarios (live Docker stack required)",
    "fuzz":        "OSS-Fuzz build script syntax verification",
    # Python standalone suites
    "memory":      "Memory correctness & leak detection (ASAN/LSAN/Valgrind)",
    "concurrency": "Race condition detection (TSAN + stress binaries)",
    "throughput":  "Throughput & latency baseline vs. live proxy",
    "protocol":    "PostgreSQL wire-protocol compliance & fuzzing (raw TCP)",
    "resilience":  "Error path & fault-injection resilience tests",
    "regression":  "Regression / integration correctness tests vs. live proxy",
    "chaos-py":    "Network chaos via tc netem (requires root / sudo)",
    "soak":        "Longevity / soak tests — memory, FD, error-rate stability",
}

# ---------------------------------------------------------------------------
# Managed KEEL proxy for protocol/resilience/regression suites
# ---------------------------------------------------------------------------

def _find_keel_binary(build_dir: Optional[Path], hint: Optional[str]) -> Optional[Path]:
    """Locate the keel binary: hint > build_dir > well-known dirs > PATH."""
    if hint:
        p = Path(hint)
        return p if p.is_file() else None
    candidates: list[Path] = []
    if build_dir:
        candidates.append(build_dir / "src" / "main" / "keel")
    for d in ("build-asan", "build", "build-tsan", "build-coverage"):
        candidates.append(REPO_ROOT / d / "src" / "main" / "keel")
    for p in candidates:
        if p.is_file():
            return p
    found = shutil.which("keel")
    return Path(found) if found else None


@contextlib.contextmanager
def _managed_keel_proxy(
    binary: Path,
    pg_host: str,
    pg_port: int,
    pg_user: str,
    pg_password: str,
    pg_database: str,
):
    """
    Start a temporary KEEL proxy (trust auth) pointing at the given PostgreSQL
    backend, yield the proxy port, and shut it down on exit.  Sets KEEL_HOST
    and KEEL_PORT environment variables so suite modules pick them up.
    """
    # Allocate a free port.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        proxy_port: int = _s.getsockname()[1]

    config_text = (
        f"[keel]\n"
        f"config_version = 2\n"
        f"log_level = 0\n"
        f"\n"
        f"[worker_group.test]\n"
        f"bind_addr = 127.0.0.1\n"
        f"bind_port = {proxy_port}\n"
        f"num_workers = 1\n"
        f"max_pool_size = 10\n"
        f"min_pool_size = 0\n"
        f"auth_method = trust\n"
        f"server_user = {pg_user}\n"
        f"server_password = {pg_password}\n"
        f"\n"
        f"[worker_group.test.servers]\n"
        f"primary = host={pg_host} port={pg_port} dbname={pg_database} "
        f"role=primary weight=100\n"
    )
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".ini", prefix="keel_run_tests_", delete=False
    )
    try:
        tmp.write(config_text)
        tmp.close()
        config_path = tmp.name

        proc_env = {**os.environ, "ASAN_OPTIONS": "detect_leaks=0"}
        proc = subprocess.Popen(
            [str(binary), "-c", config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=proc_env,
        )

        # Wait up to 10 s for the proxy port to accept connections.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", proxy_port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            proc.terminate()
            proc.wait(timeout=5)
            raise RuntimeError(
                f"KEEL did not start within 10 s on port {proxy_port}"
            )

        old_host = os.environ.get("KEEL_HOST")
        old_port = os.environ.get("KEEL_PORT")
        os.environ["KEEL_HOST"] = "127.0.0.1"
        os.environ["KEEL_PORT"] = str(proxy_port)
        info(f"  KEEL proxy started on port {proxy_port} (PID {proc.pid})")
        try:
            yield proxy_port
        finally:
            if old_host is not None:
                os.environ["KEEL_HOST"] = old_host
            else:
                os.environ.pop("KEEL_HOST", None)
            if old_port is not None:
                os.environ["KEEL_PORT"] = old_port
            else:
                os.environ.pop("KEEL_PORT", None)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    finally:
        os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# Python suite runner helper
# ---------------------------------------------------------------------------
_SUITE_MODULE_MAP: dict[str, str] = {
    "memory":      "tests.suites.suite_memory",
    "concurrency": "tests.suites.suite_concurrency",
    "throughput":  "tests.suites.suite_throughput",
    "protocol":    "tests.suites.suite_protocol",
    "resilience":  "tests.suites.suite_resilience",
    "regression":  "tests.suites.suite_regression",
    "chaos-py":    "tests.suites.suite_chaos",
    "soak":        "tests.suites.suite_soak",
}


def _run_python_suite(suite_key: str, coord_result: SuiteResult,
                      dry_run: bool, **kwargs: object) -> None:
    """
    Import and run one of the Python standalone suites from tests/suites/.
    Translates the suite's internal SuiteResult back into the coordinator's
    simpler SuiteResult dataclass.
    """
    module_name = _SUITE_MODULE_MAP[suite_key]

    # Make sure the repo root is on sys.path so the import resolves.
    repo_str = str(REPO_ROOT)
    if repo_str not in sys.path:
        sys.path.insert(0, repo_str)

    if dry_run:
        print(f"  [dry-run] would run suite module: {module_name}")
        coord_result.status = "skipped"
        coord_result.detail = "dry-run"
        return

    try:
        import importlib
        mod = importlib.import_module(module_name)
    except ImportError as exc:
        coord_result.status = "error"
        coord_result.detail = f"Import error for {module_name}: {exc}"
        error(coord_result.detail)
        return

    # Each suite module exposes a run(result, **kwargs) that populates a
    # tests.suites.SuiteResult.  Import that dataclass as well.
    try:
        from tests.suites import SuiteResult as PySuiteResult  # type: ignore
    except ImportError as exc:
        coord_result.status = "error"
        coord_result.detail = f"Cannot import tests.suites: {exc}"
        error(coord_result.detail)
        return

    py_result = PySuiteResult(name=suite_key)
    t0 = time.monotonic()
    # Wire build_dir via KEEL_BUILD_DIR so suite modules using find_build_dir()
    # automatically pick up the caller-supplied path.
    build_dir_val = str(kwargs.get("build_dir") or "")
    if build_dir_val:
        os.environ["KEEL_BUILD_DIR"] = build_dir_val
    try:
        mod.run(py_result, **kwargs)
    except Exception as exc:
        coord_result.status = "error"
        coord_result.duration = time.monotonic() - t0
        coord_result.detail = f"Suite raised an unexpected exception: {exc}"
        error(coord_result.detail)
        return

    py_result.finalize()
    coord_result.duration   = py_result.duration or (time.monotonic() - t0)
    coord_result.status     = py_result.status
    coord_result.returncode = 0 if py_result.status == "passed" else 1

    # Build a human-readable detail string
    lines: list[str] = [
        f"{py_result.passed} passed, {py_result.failed} failed, "
        f"{py_result.skipped} skipped ({len(py_result.cases)} total)"
    ]
    for c in py_result.cases:
        if c.status in ("failed", "error"):
            lines.append(f"  FAIL  {c.name}: {(c.detail or '')[:200]}")
    coord_result.detail = "\n".join(lines)


# Integration test scripts, in the order they should run
INTEGRATION_SCRIPTS = [
    "test-pg-e2e-full.sh",
    "test_pg_streaming.py",   # Python pytest (replaces test-pg-streaming.sh)
    "test_pg_patroni.py",    # Python pytest (replaces test-pg-patroni.sh)
    "test-sharding.sh",
    "test-cloud-auth-e2e.sh",
    "test-mysql-replication.sh",
    "test-mysql-group.sh",
    "test-mysql-mariadb.sh",
    "test-mysql-pxc.sh",
    "test_rw_split.sh",
    "test_pg_jdbc_prepared.sh",
]

# Per-script timeout overrides (seconds).  Scripts not listed here use the
# default 600 s.  test_rw_split.sh needs extra time for Docker startup (up to
# 120 s) plus a 300 s pgbench load phase.
_INTEGRATION_TIMEOUTS: dict[str, int] = {
    "test_rw_split.sh": 900,
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_tests.py",
        description=textwrap.dedent("""\
            KEEL master test coordinator.
            Runs any combination of test suites and produces a unified report.
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              # Full run (all suites):
              python3 tests/run_tests.py

              # Unit + E2E only:
              python3 tests/run_tests.py --suite unit --suite e2e

              # Unit tests against a specific build:
              python3 tests/run_tests.py --suite unit --build-dir build-asan

              # CI gate (unit + e2e, fail fast):
              python3 tests/run_tests.py --ci --suite unit --suite e2e

              # List available suites:
              python3 tests/run_tests.py --list
        """),
    )

    p.add_argument(
        "--suite", "-s",
        action="append",
        dest="suites",
        metavar="SUITE",
        choices=ALL_SUITES,
        help=f"Suite(s) to run. May be repeated. Choices: {', '.join(ALL_SUITES)}. "
             "Default: all suites.",
    )
    p.add_argument(
        "--build-dir",
        metavar="DIR",
        help="CMake build directory for the 'unit' suite (default: auto-detect).",
    )
    p.add_argument(
        "--jobs", "-j",
        type=int,
        default=0,
        metavar="N",
        help="Parallel jobs for ctest (default: ctest default).",
    )
    p.add_argument(
        "--no-build",
        action="store_true",
        help="Skip KEEL Docker image rebuild in the e2e suite.",
    )
    p.add_argument(
        "--keep-stack",
        action="store_true",
        help="Keep the Docker Compose stack running after the e2e suite.",
    )
    p.add_argument(
        "--e2e-marker",
        metavar="MARKER",
        help="Restrict e2e pytest run to a single marker "
             "(pool|sharding|scatter|twopc|failover|chaos|metrics|stress).",
    )
    p.add_argument(
        "--parallel",
        action="store_true",
        help="Run e2e pytest tests in parallel (requires pytest-xdist).",
    )
    p.add_argument(
        "--integration-only",
        metavar="SCRIPT",
        help="Run only this script from tests/integration/ (implies --suite integration).",
    )
    p.add_argument(
        "--chaos-scenario",
        action="append",
        metavar="SCENARIO",
        dest="chaos_scenarios",
        help="Run only this chaos scenario (may be repeated).",
    )
    p.add_argument(
        "--report-dir",
        metavar="DIR",
        default=str(REPORTS_DIR),
        help=f"Directory for HTML/JSON reports (default: {REPORTS_DIR}).",
    )
    p.add_argument(
        "--ci",
        action="store_true",
        help="CI mode: disable colours, exit 1 on any failure.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="List available suites and exit.",
    )
    p.add_argument(
        "--cmake-build",
        action="store_true",
        help="Run 'cmake --build <build-dir>' before invoking ctest. "
             "Use this when the build directory exists but binaries are stale or missing.",
    )
    p.add_argument(
        "--configure",
        action="store_true",
        help="Run 'cmake -B <build-dir> -S .' before building and testing. "
             "Use this when no build directory exists yet. "
             "Implies --cmake-build. Default build dir: build-host.",
    )
    p.add_argument(
        "--ctest-args",
        nargs=argparse.REMAINDER,
        help="Extra arguments forwarded to ctest (e.g. --ctest-args -R test_buffer).",
    )

    # ── Python suite-specific options ───────────────────────────────────────
    p.add_argument(
        "--bench-clients",
        type=int,
        default=10,
        metavar="N",
        help="Concurrent clients for the 'throughput' suite (default: 10).",
    )
    p.add_argument(
        "--bench-duration",
        type=int,
        default=10,
        metavar="S",
        help="Duration in seconds per sub-test for the 'throughput' suite (default: 10).",
    )
    p.add_argument(
        "--soak-duration",
        type=int,
        default=60,
        metavar="S",
        help="Total soak test duration in seconds for the 'soak' suite (default: 60).",
    )
    p.add_argument(
        "--soak-clients",
        type=int,
        default=5,
        metavar="N",
        help="Worker threads for the 'soak' suite (default: 5).",
    )
    p.add_argument(
        "--proxy-pid",
        type=int,
        default=None,
        metavar="PID",
        help="PID of the running keel proxy (needed for RSS/FD checks in soak suite).",
    )
    p.add_argument(
        "--keel-binary",
        metavar="PATH",
        default=None,
        help="Path to the keel binary used for protocol/resilience/regression suites. "
             "If omitted, auto-detected from --build-dir or well-known build directories. "
             "Set to 'none' to disable auto-start (tests run against whatever KEEL_PORT points to).",
    )
    p.add_argument(
        "--pg-host",
        default=os.environ.get("KEEL_PG_HOST", os.environ.get("PGHOST", "127.0.0.1")),
        metavar="HOST",
        help="PostgreSQL backend host for the managed proxy (default: 127.0.0.1).",
    )
    p.add_argument(
        "--pg-port",
        type=int,
        default=int(os.environ.get("KEEL_PG_PORT", os.environ.get("PGPORT", "5432"))),
        metavar="PORT",
        help="PostgreSQL backend port for the managed proxy (default: 5432).",
    )
    p.add_argument(
        "--pg-user",
        default=os.environ.get("KEEL_PG_USER", os.environ.get("PGUSER", "postgres")),
        metavar="USER",
        help="PostgreSQL backend user for the managed proxy (default: postgres).",
    )
    p.add_argument(
        "--pg-password",
        default=os.environ.get("KEEL_PG_PASSWORD", os.environ.get("PGPASSWORD", "postgres")),
        metavar="PASS",
        help="PostgreSQL backend password for the managed proxy (default: postgres).",
    )
    p.add_argument(
        "--pg-database",
        default=os.environ.get("KEEL_PG_DATABASE", os.environ.get("PGDATABASE", "postgres")),
        metavar="DB",
        help="PostgreSQL backend database for the managed proxy (default: postgres).",
    )
    p.add_argument(
        "--chaos-iface",
        default="lo",
        metavar="IFACE",
        help="Network interface for tc netem in the 'chaos-py' suite (default: lo).",
    )
    p.add_argument(
        "--concurrency-repeat",
        type=int,
        default=3,
        metavar="N",
        help="Repetitions per stress binary in the 'concurrency' suite (default: 3).",
    )
    p.add_argument(
        "--py-filter",
        metavar="SUBSTR",
        help="Only run cases whose name contains SUBSTR in the Python suites.",
    )
    p.add_argument(
        "--py-verbose",
        action="store_true",
        help="Verbose per-case output for the Python standalone suites.",
    )

    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = build_parser()
    args   = parser.parse_args()

    # CI mode
    if args.ci or os.environ.get("CI"):
        global _USE_COLOR
        _USE_COLOR = False

    if args.list:
        print("\nAvailable test suites:\n")
        for name in ALL_SUITES:
            print(f"  {name:<14}  {SUITE_DESCRIPTIONS[name]}")
        print()
        print("Integration scripts (tests/integration/):")
        for s in INTEGRATION_SCRIPTS:
            exists = "✓" if (TESTS_DIR / "integration" / s).exists() else "✗"
            print(f"  {exists}  {s}")
        print()
        return 0

    selected_suites = args.suites or ALL_SUITES
    # --configure implies --cmake-build
    if args.configure:
        args.cmake_build = True
    build_dir       = _find_build_dir(args.build_dir)
    report_dir      = Path(args.report_dir)

    head(f"KEEL Test Coordinator — {', '.join(selected_suites)}")
    print(f"  Repo root:  {REPO_ROOT}")
    if build_dir:
        print(f"  Build dir:  {build_dir}")
    print(f"  Reports:    {report_dir}")
    print(f"  Dry-run:    {args.dry_run}")
    print()

    results: list[SuiteResult] = []

    # ── Unit (CTest) ────────────────────────────────────────────────────────
    if "unit" in selected_suites:
        r = SuiteResult("unit")
        info(f"Suite: unit (ctest @ {build_dir})")
        run_unit(r, build_dir, args.dry_run, args.jobs,
                 ctest_args=args.ctest_args,
                 cmake_build=args.cmake_build,
                 configure=args.configure)
        results.append(r)
        (ok if r.status == "passed" else error)(f"unit → {r.status} ({r.duration:.1f}s)")

    # ── E2E (pytest) ────────────────────────────────────────────────────────
    if "e2e" in selected_suites:
        r = SuiteResult("e2e")
        info("Suite: e2e (pytest)")
        run_e2e(r, args.dry_run, args.no_build, args.keep_stack,
                marker=args.e2e_marker or "",
                parallel=args.parallel,
                report_dir=report_dir / "e2e")
        results.append(r)
        (ok if r.status == "passed" else error)(f"e2e → {r.status} ({r.duration:.1f}s)")

    # ── Integration (bash scripts) ──────────────────────────────────────────
    if "integration" in selected_suites:
        scripts = [args.integration_only] if args.integration_only else INTEGRATION_SCRIPTS
        for script in scripts:
            r = SuiteResult(f"integration/{script}")
            info(f"Suite: integration/{script}")
            timeout = _INTEGRATION_TIMEOUTS.get(script, 600)
            run_integration(r, script, args.dry_run, timeout=timeout)
            results.append(r)
            (ok if r.status == "passed" else error)(
                f"integration/{script} → {r.status} ({r.duration:.1f}s)"
            )

    # ── Hardening ───────────────────────────────────────────────────────────
    if "hardening" in selected_suites:
        r = SuiteResult("hardening")
        info("Suite: hardening (run_all.sh)")
        env: dict[str, str] = {
            # Always enable the fast/offline checks — mirrors hardening.yml CI defaults.
            # Expensive/infra-dependent checks default to 0; set explicitly to opt in.
            "RUN_HARDENING_TESTS":  "1",
            "RUN_SECURITY_CHECKSEC": "1",
            "RUN_SECURITY_SAST":    "1",
            # Off by default locally — require live proxy / special tools / root
            "RUN_SANITIZERS":      os.environ.get("RUN_SANITIZERS",      "0"),
            "RUN_SHADOW_DIFF":     os.environ.get("RUN_SHADOW_DIFF",     "0"),
            "RUN_SLOW_CLIENT":     os.environ.get("RUN_SLOW_CLIENT",     "0"),
            "RUN_CHAOS_SYSCALLS":  os.environ.get("RUN_CHAOS_SYSCALLS",  "0"),
            "RUN_CHAOS_NETEM":     os.environ.get("RUN_CHAOS_NETEM",     "0"),
            "RUN_CHAOS_ZOMBIE":    os.environ.get("RUN_CHAOS_ZOMBIE",    "0"),
            "RUN_SECURITY_TLS":    os.environ.get("RUN_SECURITY_TLS",    "0"),
            "RUN_SECURITY_SQLMAP": os.environ.get("RUN_SECURITY_SQLMAP", "0"),
            "RUN_PROXY_SSV_E2E":   os.environ.get("RUN_PROXY_SSV_E2E",  "0"),
        }
        if build_dir:
            env["BUILD_DIR"] = str(build_dir)
        run_hardening(r, args.dry_run, env_overrides=env)
        results.append(r)
        (ok if r.status == "passed" else error)(
            f"hardening → {r.status} ({r.duration:.1f}s)"
        )

    # ── Chaos ───────────────────────────────────────────────────────────────
    if "chaos" in selected_suites:
        r = SuiteResult("chaos")
        info("Suite: chaos (run-chaos.sh)")
        run_chaos(r, args.dry_run, scenarios=args.chaos_scenarios)
        results.append(r)
        (ok if r.status == "passed" else error)(f"chaos → {r.status} ({r.duration:.1f}s)")

    # ── Fuzz (build check) ──────────────────────────────────────────────────
    if "fuzz" in selected_suites:
        r = SuiteResult("fuzz")
        info("Suite: fuzz (oss-fuzz/build.sh syntax check)")
        run_fuzz_build(r, args.dry_run)
        results.append(r)
        (ok if r.status == "passed" else error)(f"fuzz → {r.status} ({r.duration:.1f}s)")

    # ── Python standalone suites ────────────────────────────────────────────
    # Common kwargs forwarded to all Python suites (each suite ignores unknown
    # kwargs, so it is safe to pass all of them unconditionally).
    _py_common: dict[str, object] = {
        "verbose":    args.py_verbose,
        "filter":     args.py_filter or "",
        "build_dir":  str(build_dir) if build_dir else "",
    }

    if "memory" in selected_suites:
        r = SuiteResult("memory")
        info("Suite: memory (ASAN/LSAN/Valgrind C test binaries)")
        _run_python_suite("memory", r, args.dry_run, **_py_common)
        results.append(r)
        (ok if r.status == "passed" else error)(f"memory → {r.status} ({r.duration:.1f}s)")

    if "concurrency" in selected_suites:
        r = SuiteResult("concurrency")
        info("Suite: concurrency (TSAN + stress binaries)")
        _run_python_suite("concurrency", r, args.dry_run,
                          repeat=args.concurrency_repeat, **_py_common)
        results.append(r)
        (ok if r.status == "passed" else error)(
            f"concurrency → {r.status} ({r.duration:.1f}s)")

    if "throughput" in selected_suites:
        r = SuiteResult("throughput")
        info("Suite: throughput (TPS/latency vs. live proxy)")
        _run_python_suite("throughput", r, args.dry_run,
                          clients=args.bench_clients,
                          duration=args.bench_duration, **_py_common)
        results.append(r)
        (ok if r.status == "passed" else error)(
            f"throughput → {r.status} ({r.duration:.1f}s)")

    # Suites that require a live KEEL proxy.  Auto-start one unless the user
    # has already pointed KEEL_PORT at a running instance or disabled auto-start.
    _proxy_suites = {"protocol", "resilience", "regression"}
    _need_proxy   = bool(set(selected_suites) & _proxy_suites)
    _keel_binary: Optional[Path] = None
    _auto_start   = False
    if _need_proxy and (args.keel_binary or "").lower() != "none":
        _keel_binary = _find_keel_binary(build_dir, args.keel_binary)
        # Only auto-start if KEEL_PORT is not already explicitly set by the caller.
        _auto_start = (_keel_binary is not None
                       and "KEEL_PORT" not in os.environ
                       and not args.dry_run)
        if _keel_binary and not _auto_start:
            info(f"  KEEL_PORT already set to {os.environ.get('KEEL_PORT')} — skipping auto-start")
        elif not _keel_binary:
            warn("  No keel binary found — protocol/resilience/regression tests will run against KEEL_PORT (default: 5432)")

    def _run_proxy_suite(name: str, desc: str) -> None:
        r = SuiteResult(name)
        info(f"Suite: {name} ({desc})")
        _run_python_suite(name, r, args.dry_run, **_py_common)
        results.append(r)
        (ok if r.status == "passed" else error)(f"{name} → {r.status} ({r.duration:.1f}s)")

    def _run_proxy_suites_block() -> None:
        if "protocol" in selected_suites:
            _run_proxy_suite("protocol", "wire-protocol compliance + fuzzing")
        if "resilience" in selected_suites:
            _run_proxy_suite("resilience", "error path & fault injection")
        if "regression" in selected_suites:
            _run_proxy_suite("regression", "correctness vs. live proxy")

    if _auto_start and _keel_binary is not None:
        with _managed_keel_proxy(
            _keel_binary,
            args.pg_host,
            args.pg_port,
            args.pg_user,
            args.pg_password,
            args.pg_database,
        ):
            _run_proxy_suites_block()
    else:
        _run_proxy_suites_block()

    if "chaos-py" in selected_suites:
        r = SuiteResult("chaos-py")
        info("Suite: chaos-py (tc netem network chaos)")
        _run_python_suite("chaos-py", r, args.dry_run,
                          iface=args.chaos_iface, **_py_common)
        results.append(r)
        (ok if r.status == "passed" else error)(
            f"chaos-py → {r.status} ({r.duration:.1f}s)")

    if "soak" in selected_suites:
        r = SuiteResult("soak")
        info("Suite: soak (longevity / stability)")
        _run_python_suite("soak", r, args.dry_run,
                          duration=args.soak_duration,
                          clients=args.soak_clients,
                          proxy_pid=args.proxy_pid, **_py_common)
        results.append(r)
        (ok if r.status == "passed" else error)(f"soak → {r.status} ({r.duration:.1f}s)")

    # ── Reports ─────────────────────────────────────────────────────────────
    if results:
        generate_reports(results, report_dir)
        print_summary(results)

    # ── Exit code ───────────────────────────────────────────────────────────
    any_failed = any(r.status in ("failed", "error") for r in results)
    if any_failed:
        error("One or more test suites FAILED.")
        return 1

    ok("All test suites passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
