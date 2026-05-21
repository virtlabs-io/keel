#!/usr/bin/env python3
"""
tests/suites/generate_report.py
================================
Generate a human-readable Markdown report from a torture-suite run directory.

Usage:
    python3 tests/suites/generate_report.py <report-dir> [--full]

Options:
    --full    Also append raw Prometheus metrics and full admin/log output.

The report directory is expected to contain:
  report.json          — JSON produced by suite_torture.py --json-out
  admin-final.txt      — KEEL admin console output (optional)
  prometheus-final.txt — Prometheus /metrics scrape (optional)
  keel.log             — KEEL process log (optional)

Outputs:
  <report-dir>/report.md   — written to disk and printed to stdout
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _sym(status: str) -> str:
    return {"passed": "✅", "failed": "❌", "skipped": "⏭", "error": "🔥"}.get(status, "❓")


def _duration(seconds: float) -> str:
    if seconds >= 60:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    if seconds >= 1.0:
        return f"{seconds:.2f}s"
    return f"{seconds * 1000:.0f}ms"


def _ns(val) -> str:
    """Nanoseconds → human-readable string."""
    try:
        ns = float(val)
    except (TypeError, ValueError):
        return "n/a"
    if ns <= 0:
        return "0"
    if ns < 1_000:
        return f"{ns:.0f} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.1f} µs"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.1f} ms"
    return f"{ns / 1_000_000_000:.2f} s"


def _bytes(val) -> str:
    try:
        b = float(val)
    except (TypeError, ValueError):
        return "n/a"
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _pct(num, denom, decimals: int = 1) -> str:
    try:
        n, d = float(num), float(denom)
        return "n/a" if d == 0 else f"{n / d * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return "n/a"


def _i(s) -> int:
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return 0


def _signal(value: int, warn: int, crit: int) -> str:
    """Status indicator: higher value = worse."""
    if value >= crit:
        return "❌"
    if value >= warn:
        return "⚠️"
    return "✅"


# ---------------------------------------------------------------------------
# Prometheus parser
# ---------------------------------------------------------------------------

def _parse_prometheus(text: str) -> dict[str, str]:
    """Parse Prometheus text format → {metric_name_with_labels: value}."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(" ")
        if len(parts) >= 2:
            out[parts[0]] = parts[1]
    return out


def _get(prom: dict[str, str], name: str, fallback: str = "0") -> str:
    """
    Look up a KEEL metric aggregate.  PROM_COUNTER/PROM_GAUGE emit
    '<name>_total' as the unlabelled aggregate; plain gauges use the
    name verbatim.
    """
    if name in prom:
        return prom[name]
    total = name + "_total"
    if total in prom:
        return prom[total]
    return fallback


def _quantile(prom: dict[str, str], histogram: str, q: str) -> str:
    """KEEL emits: keel_query_latency_ns{quantile="0.5"} 12345"""
    return prom.get(f'{histogram}{{quantile="{q}"}}', "0")


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------

def generate(report_dir: Path, full: bool = False) -> str:
    json_file  = report_dir / "report.json"
    admin_file = report_dir / "admin-final.txt"
    prom_file  = report_dir / "prometheus-final.txt"
    log_file   = report_dir / "keel.log"

    if not json_file.exists():
        return f"# Report Error\n\n`report.json` not found in `{report_dir}`\n"

    data     = json.loads(json_file.read_text())
    now      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    status   = data.get("status", "unknown").upper()
    passed   = data.get("passed", 0)
    failed   = data.get("failed", 0)
    skipped  = data.get("skipped", 0)
    total    = passed + failed + skipped
    duration = data.get("duration", 0)
    cases    = data.get("cases", [])
    env      = data.get("metrics", {}).get("env", {})

    status_emoji = {"PASSED": "✅", "FAILED": "❌", "INTERRUPTED": "⚠️"}.get(status, "❓")

    prom: dict[str, str] = {}
    if prom_file.exists():
        prom = _parse_prometheus(prom_file.read_text())
    elif data.get("metrics", {}).get("prometheus"):
        prom = data["metrics"]["prometheus"]

    out: list[str] = []

    def h2(title: str) -> None:
        out.extend(["", f"## {title}", ""])

    def row(*cols) -> str:
        return "| " + " | ".join(str(c) for c in cols) + " |"

    # -----------------------------------------------------------------------
    # Banner
    # -----------------------------------------------------------------------
    soak_s = env.get("soak_s", env.get("KEEL_TORTURE_SOAK_S", "?"))
    out += [
        f"# KEEL Torture Suite — {status} {status_emoji}",
        "",
        f"| | |",
        f"|---|---|",
        f"| **Date** | {now} |",
        f"| **Duration** | {_duration(duration)} |",
        f"| **Soak** | {soak_s}s |",
        "",
    ]

    # -----------------------------------------------------------------------
    # Test Summary
    # -----------------------------------------------------------------------
    h2("📋 Test Summary")
    bar_w  = 36
    filled = round(bar_w * passed / total) if total else 0
    bar    = "█" * filled + "░" * (bar_w - filled)
    out += [
        "```",
        f"  {bar}  {passed}/{total} passed",
        "```",
        "",
        row("✅ Passed", "❌ Failed", "⏭ Skipped", "Total"),
        row("---", "---", "---", "---"),
        row(f"**{passed}**", f"**{failed}**", f"**{skipped}**", f"**{total}**"),
        "",
    ]

    # -----------------------------------------------------------------------
    # Failures — shown prominently
    # -----------------------------------------------------------------------
    failed_cases = [c for c in cases if c["status"] in ("failed", "error")]
    if failed_cases:
        h2("❌ Failures")
        for c in failed_cases:
            detail   = (c.get("detail") or "(no detail)").strip()
            headline = next((ln.strip() for ln in detail.splitlines() if ln.strip()), detail)
            body     = detail.splitlines()
            out += [f"### `{c['name']}`  ({_duration(c.get('duration', 0))})", ""]
            out += [f"> {headline[:300]}", ""]
            if len(body) > 1:
                out += [
                    "<details><summary>Full error</summary>",
                    "",
                    "```",
                    *body[:80],
                    "```",
                    "",
                    "</details>",
                    "",
                ]

    # -----------------------------------------------------------------------
    # KEEL Health Overview
    # -----------------------------------------------------------------------
    if prom:
        h2("🔌 Connection & Pool Health")

        sessions_created = _get(prom, "keel_sessions_created")
        queries_total    = _get(prom, "keel_queries_total")
        bytes_recv       = _get(prom, "keel_bytes_recv")
        bytes_sent       = _get(prom, "keel_bytes_sent")

        pool_borrows   = _get(prom, "keel_pool_borrows")
        pool_hits      = _get(prom, "keel_pool_hits")
        pool_misses    = _get(prom, "keel_pool_misses")
        pool_active    = prom.get("keel_pool_connections_active", "0")
        pool_idle      = prom.get("keel_pool_connections_idle", "0")
        pool_dirty     = prom.get("keel_pool_connections_dirty", "0")
        pool_cleaning  = prom.get("keel_pool_connections_cleaning", "0")
        pool_waiting   = prom.get("keel_pool_waiting_sessions", "0")
        pool_wait_to   = _get(prom, "keel_pool_wait_timeout_events")
        multiplex_raw  = prom.get("proxy_multiplex_ratio", "0")
        uptime_s       = prom.get("keel_uptime_seconds", "0")

        miss_rate = 0
        if _i(pool_borrows) > 0:
            miss_rate = round(_i(pool_misses) / _i(pool_borrows) * 100)

        out += [
            row("Metric", "Value", ""),
            row("---", "---", "---"),
            row("Sessions created", f"{_i(sessions_created):,}", ""),
            row("Queries routed",   f"{_i(queries_total):,}", ""),
            row("Data recv / sent", f"{_bytes(bytes_recv)} / {_bytes(bytes_sent)}", ""),
            row("Pool hit rate",    _pct(pool_hits, pool_borrows),
                _signal(miss_rate, warn=30, crit=60) if _i(pool_borrows) else ""),
            row("Backends active / idle",
                f"{pool_active} / {pool_idle}", ""),
            row("Backends dirty",   pool_dirty,
                _signal(_i(pool_dirty), warn=1, crit=5)),
            row("Backends cleaning", pool_cleaning,
                _signal(_i(pool_cleaning), warn=5, crit=20)),
            row("Sessions waiting for pool", pool_waiting,
                _signal(_i(pool_waiting), warn=1, crit=10)),
            row("Pool wait timeouts", pool_wait_to,
                _signal(_i(pool_wait_to), warn=1, crit=5)),
            row("Multiplex ratio",
                f"{float(multiplex_raw):.1f}×" if multiplex_raw not in ("0", "") else "n/a",
                ""),
            row("KEEL uptime",
                f"{float(uptime_s):.0f}s" if uptime_s not in ("0", "") else "n/a",
                ""),
            "",
        ]

        # --- Error counters ---
        h2("🚨 Error Counters")

        errors_total       = _get(prom, "keel_errors_total")
        errors_proto       = _get(prom, "keel_errors_proto")
        errors_backend     = _get(prom, "keel_errors_backend")
        errors_auth        = _get(prom, "keel_errors_auth")
        errors_timeout     = _get(prom, "keel_errors_timeout")
        desync_total       = prom.get("proxy_state_desync_total", "0")
        orphaned_txns      = prom.get("proxy_orphaned_transactions_total", "0")
        backend_reuse_fail = prom.get("proxy_backend_reuse_failure_total", "0")

        out += [
            row("Error type", "Count", ""),
            row("---", "---", "---"),
            row("Total errors",            f"{_i(errors_total):,}",
                _signal(_i(errors_total), warn=5, crit=50)),
            row("Protocol errors",         errors_proto,
                _signal(_i(errors_proto), warn=1, crit=10)),
            row("Backend errors",          errors_backend,
                _signal(_i(errors_backend), warn=5, crit=30)),
            row("Auth failures",           errors_auth,
                _signal(_i(errors_auth), warn=1, crit=5)),
            row("Timeout errors",          errors_timeout,
                _signal(_i(errors_timeout), warn=5, crit=20)),
            row("Protocol state desyncs",  desync_total,
                _signal(_i(desync_total), warn=1, crit=5)),
            row("Orphaned transactions",   orphaned_txns,
                _signal(_i(orphaned_txns), warn=20, crit=200)),
            row("Backend reuse failures",  backend_reuse_fail,
                _signal(_i(backend_reuse_fail), warn=1, crit=10)),
            "",
        ]

        # --- Backend cleanup ---
        cleanup_ok      = _get(prom, "keel_cleanup_result_success")
        cleanup_proto   = _get(prom, "keel_cleanup_result_protocol_error")
        cleanup_timeout = _get(prom, "keel_cleanup_result_timeout")
        cleanup_eof     = _get(prom, "keel_cleanup_result_backend_eof")
        cleanup_n       = _i(cleanup_ok) + _i(cleanup_proto) + _i(cleanup_timeout) + _i(cleanup_eof)

        if cleanup_n > 0:
            h2("🔄 Backend Cleanup")
            out += [
                row("Outcome", "Count", ""),
                row("---", "---", "---"),
                row("Success",        cleanup_ok,
                    _signal(cleanup_n - _i(cleanup_ok), warn=1, crit=5)),
                row("Protocol error", cleanup_proto,
                    _signal(_i(cleanup_proto), warn=1, crit=5)),
                row("Timeout",        cleanup_timeout,
                    _signal(_i(cleanup_timeout), warn=1, crit=5)),
                row("Backend EOF",    cleanup_eof,
                    _signal(_i(cleanup_eof), warn=5, crit=20)),
                row("Success rate",   _pct(cleanup_ok, cleanup_n), ""),
                "",
            ]

        # --- Latency ---
        latency_metrics = [
            ("Query latency (end-to-end)", "keel_query_latency_ns"),
            ("Backend latency",            "keel_backend_latency_ns"),
            ("Pool wait time",             "keel_wait_latency_ns"),
            ("Backend connect latency",    "keel_connect_latency_ns"),
        ]
        has_latency = any(
            f'{m}{{quantile="0.5"}}' in prom for _, m in latency_metrics
        )
        if has_latency:
            h2("⚡ Latency (P50 / P95 / P99)")
            out += [row("Metric", "P50", "P95", "P99"), row("---", "---", "---", "---")]
            for label, metric in latency_metrics:
                p50 = _quantile(prom, metric, "0.5")
                p95 = _quantile(prom, metric, "0.95")
                p99 = _quantile(prom, metric, "0.99")
                if _i(p50) or _i(p95) or _i(p99):
                    out.append(row(label, _ns(p50), _ns(p95), _ns(p99)))
            out.append("")

    # -----------------------------------------------------------------------
    # Full test results table
    # -----------------------------------------------------------------------
    h2("🧪 All Test Results")
    out += [
        row("#", "Test", "Status", "Duration", "Detail"),
        row("---", "---", "---", "---", "---"),
    ]
    for i, case in enumerate(cases, 1):
        st     = case["status"]
        dur    = _duration(case.get("duration", 0))
        detail = ""
        if st not in ("passed", "skipped"):
            raw   = case.get("detail", "")
            first = next((ln.strip() for ln in raw.splitlines() if ln.strip()), "")
            detail = first[:100].replace("|", "\\|")
        out.append(row(i, f"`{case['name']}`", f"{_sym(st)} {st}", dur, detail))
    out.append("")

    # -----------------------------------------------------------------------
    # KEEL log — errors / warnings only
    # -----------------------------------------------------------------------
    if log_file.exists():
        log_lines   = log_file.read_text().splitlines()
        interesting = [
            ln for ln in log_lines
            if any(tag in ln for tag in ("ERROR", "WARN", "FATAL", "panic", "CRIT"))
        ]
        if interesting:
            h2("⚠️ KEEL Log — Errors / Warnings")
            out += ["```", *interesting[-40:], "```", ""]

    # -----------------------------------------------------------------------
    # Full output (--full only)
    # -----------------------------------------------------------------------
    if full:
        h2("📊 Raw Prometheus Metrics")
        if prom_file.exists():
            out += ["```text", prom_file.read_text().strip(), "```", ""]
        else:
            out += ["*(prometheus-final.txt not available)*", ""]

        if admin_file.exists():
            h2("🖥 Admin Console Output")
            out += ["```", admin_file.read_text().strip(), "```", ""]

        if log_file.exists():
            all_log = log_file.read_text().splitlines()
            h2(f"📄 Full KEEL Log (last {min(len(all_log), 500)} lines)")
            out += ["```", *all_log[-500:], "```", ""]

    # -----------------------------------------------------------------------
    # Footer
    # -----------------------------------------------------------------------
    out += [
        "---",
        "",
        f"*{passed}/{total} passed · {_duration(duration)} · {now} · KEEL Torture Suite*",
        "",
        "Raw JSON: `report.json`"
        + ("" if full else "  \nFull metrics: re-run with `--full` flag"),
        "",
    ]

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = sys.argv[1:]
    full = "--full" in args
    args = [a for a in args if a != "--full"]

    if not args:
        print(f"Usage: {sys.argv[0]} <report-dir> [--full]", file=sys.stderr)
        sys.exit(1)

    report_dir = Path(args[0])
    if not report_dir.is_dir():
        print(f"Error: {report_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    md = generate(report_dir, full=full)

    out_file = report_dir / "report.md"
    out_file.write_text(md)
    print(md)
    print(f"\n[report saved to {out_file}]", file=sys.stderr)


if __name__ == "__main__":
    main()
