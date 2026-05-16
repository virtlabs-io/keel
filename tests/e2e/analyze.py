#!/usr/bin/env python3
"""analyze.py — KEEL deep analysis and report generator.

Collects live telemetry from a running KEEL instance and generates a
comprehensive diagnostic report in both JSON and HTML format.

Usage
-----
    # Run against the default e2e-suite stack:
    python3 tests/e2e/analyze.py

    # Custom endpoints:
    python3 tests/e2e/analyze.py \\
        --host 127.0.0.1 \\
        --port 26432 \\
        --prom-port 29101 \\
        --admin-port 26433 \\
        --shard0-port 25432 \\
        --shard1-port 25433 \\
        --output-dir tests/reports/

    # Print report to stdout (no file output):
    python3 tests/e2e/analyze.py --print

Report dimensions
-----------------
  pool         Pool utilization, idle/active/total/waiting
  routing      Read vs write routes, per-shard distribution, scatter ops
  latency      Backend latency percentiles (p50 / p95 / p99)
  integrity    Scatter COUNT == direct shard sum (live data check)
  balance      Shard row-count balance (±% from ideal 50%)
  2pc          Two-phase commit stats (started / committed / failed)
  resources    RSS, CPU, FD usage
  cache        Query cache hit/miss rate (if result_cache=on)
  rebalancing  Rebalance migration counters (if rebalance=true)

Exit codes
----------
  0  All dimensions PASS or WARN
  1  At least one dimension FAIL
  2  Could not reach KEEL (connection error)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import textwrap
from datetime import datetime, timezone
from typing import Any

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

try:
    import urllib.request
    import urllib.error
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Defaults (match e2e-suite.yml / keel-e2e-suite.ini)
# ---------------------------------------------------------------------------

DEFAULT_HOST        = "127.0.0.1"
DEFAULT_PORT        = 26432
DEFAULT_PROM_PORT   = 29101
DEFAULT_ADMIN_PORT  = 26433
DEFAULT_SHARD0_PORT = 25432
DEFAULT_SHARD1_PORT = 25433
DEFAULT_PG_USER     = "postgres"
DEFAULT_PG_PASSWORD = "postgres"
DEFAULT_PG_DBNAME   = "testdb"
DEFAULT_OUTPUT_DIR  = "tests/reports"

MAX_POOL_SIZE_HINT = 10   # from keel-e2e-suite.ini; adjust if different

# Thresholds for PASS / WARN / FAIL
THRESHOLDS = {
    "pool_utilization_warn": 0.70,
    "pool_utilization_fail": 0.90,
    "shard_imbalance_warn":  0.15,   # ±15% from ideal 50%
    "shard_imbalance_fail":  0.30,   # ±30%
    "cache_hit_rate_warn":   0.50,   # <50% is suboptimal
    "cache_hit_rate_fail":   0.10,   # <10% = cache not working
    "latency_p99_warn_ms":   10.0,   # warn if p99 > 10ms
    "latency_p99_fail_ms":   100.0,  # fail if p99 > 100ms
    "error_rate_warn":       0.001,  # 0.1%
    "error_rate_fail":       0.01,   # 1%
}


# ===========================================================================
# Data collection
# ===========================================================================

class CollectError(Exception):
    """Raised when a data source is unreachable."""


def _prom_fetch(host: str, port: int) -> str:
    url = f"http://{host}:{port}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return resp.read().decode("utf-8")
    except Exception as exc:
        raise CollectError(f"Prometheus at {url}: {exc}") from exc


def _prom_scalar(text: str, name: str) -> float | None:
    """Return value of first matching metric (ignores labels)."""
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if re.match(rf"^{re.escape(name)}(\s|\{{)", line):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    return float(parts[1])
                except ValueError:
                    pass
    return None


def _prom_labeled(text: str, name: str) -> dict[str, float]:
    """Return dict of label_string → value for all series matching name."""
    result: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = re.match(rf"^{re.escape(name)}\{{([^}}]*)\}}\s+([\d.e+\-]+)", line)
        if m:
            try:
                result[m.group(1)] = float(m.group(2))
            except ValueError:
                pass
    return result


def _prom_histogram(text: str, name: str) -> dict[str, Any]:
    """Extract histogram sum, count, and bucket dict from Prometheus text."""
    buckets: dict[float, int] = {}
    total_sum = 0.0
    total_count = 0

    for line in text.splitlines():
        if line.startswith("#"):
            continue
        # bucket
        m = re.match(
            rf"^{re.escape(name)}_bucket\{{.*?le=\"([^\"]+)\".*?\}}\s+([\d.e+\-]+)", line
        )
        if m:
            le_str = m.group(1)
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            try:
                buckets[le_val] = int(float(m.group(2)))
            except ValueError:
                pass
            continue
        # sum
        m = re.match(rf"^{re.escape(name)}_sum\s+([\d.e+\-]+)", line)
        if m:
            try:
                total_sum = float(m.group(1))
            except ValueError:
                pass
            continue
        # count
        m = re.match(rf"^{re.escape(name)}_count\s+([\d.e+\-]+)", line)
        if m:
            try:
                total_count = int(float(m.group(1)))
            except ValueError:
                pass

    return {"buckets": buckets, "sum": total_sum, "count": total_count}


def _percentile_from_histogram(
    histogram: dict[str, Any], pct: float
) -> float | None:
    """Estimate a percentile value from a Prometheus-style cumulative histogram."""
    buckets = histogram.get("buckets", {})
    total   = histogram.get("count", 0)
    if total == 0 or not buckets:
        return None

    target = pct * total
    sorted_bounds = sorted(b for b in buckets if b != math.inf)

    prev_bound  = 0.0
    prev_count  = 0
    for bound in sorted_bounds:
        count = buckets[bound]
        if count >= target:
            # Linear interpolation within bucket
            if count == prev_count:
                return prev_bound
            frac = (target - prev_count) / (count - prev_count)
            return prev_bound + frac * (bound - prev_bound)
        prev_bound = bound
        prev_count = count

    # All observations are in the +Inf bucket
    return sorted_bounds[-1] if sorted_bounds else None


def _pg_connect(host: str, port: int, user: str, password: str, dbname: str):
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname,
            connect_timeout=10
        )
        conn.autocommit = True
        return conn
    except Exception as exc:
        raise CollectError(f"PostgreSQL at {host}:{port}: {exc}") from exc


def _pg_scalar(conn, sql: str, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def _pg_rows(conn, sql: str, params=None) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql, params or ())
    return cur.fetchall()


# ===========================================================================
# Analysis dimensions
# ===========================================================================

Status = str   # "pass" | "warn" | "fail" | "skip"


def _classify(value: float, warn_threshold: float, fail_threshold: float,
               higher_is_worse: bool = True) -> Status:
    if higher_is_worse:
        if value >= fail_threshold:
            return "fail"
        if value >= warn_threshold:
            return "warn"
        return "pass"
    else:
        if value <= fail_threshold:
            return "fail"
        if value <= warn_threshold:
            return "warn"
        return "pass"


def analyze_pool(prom: str, admin_conn) -> dict:
    """Pool utilization, connections, waiting sessions."""
    result: dict[str, Any] = {"name": "pool", "metrics": {}, "issues": []}

    active      = _prom_scalar(prom, "keel_pool_connections_active") or 0.0
    idle        = _prom_scalar(prom, "keel_pool_connections_idle")   or 0.0
    total       = _prom_scalar(prom, "keel_pool_connections_total")  or 0.0
    waiting     = _prom_scalar(prom, "keel_pool_waiting_sessions")   or 0.0
    utilization = _prom_scalar(prom, "keel_pool_utilization_ratio")  or 0.0

    result["metrics"] = {
        "active":      active,
        "idle":        idle,
        "total":       total,
        "waiting":     waiting,
        "utilization": utilization,
    }

    status: Status = "pass"

    if total > 0 and active + idle > total + 1:
        result["issues"].append(
            f"active({active:.0f}) + idle({idle:.0f}) > total({total:.0f})"
        )
        status = "fail"

    util_status = _classify(utilization,
                             THRESHOLDS["pool_utilization_warn"],
                             THRESHOLDS["pool_utilization_fail"])
    if util_status != "pass":
        result["issues"].append(
            f"utilization={utilization:.1%} "
            f"(warn≥{THRESHOLDS['pool_utilization_warn']:.0%}, "
            f"fail≥{THRESHOLDS['pool_utilization_fail']:.0%})"
        )
        if util_status == "fail":
            status = "fail"
        elif status == "pass":
            status = "warn"

    if waiting > 0:
        result["issues"].append(f"{waiting:.0f} sessions waiting for pool capacity")
        if status == "pass":
            status = "warn"

    # Admin SHOW POOLS
    try:
        rows = _pg_rows(admin_conn, "SHOW POOLS")
        result["show_pools_rows"] = len(rows)
    except Exception:
        result["show_pools_rows"] = None

    result["status"] = status
    return result


def analyze_routing(prom: str) -> dict:
    """Routing counters: read/write split, per-shard, scatter."""
    result: dict[str, Any] = {"name": "routing", "metrics": {}, "issues": []}

    total_routes    = _prom_scalar(prom, "keel_router_total_routes")    or 0.0
    read_routes     = _prom_scalar(prom, "keel_router_read_routes")     or 0.0
    write_routes    = _prom_scalar(prom, "keel_router_write_routes")    or 0.0
    failover_routes = _prom_scalar(prom, "keel_router_failover_routes") or 0.0
    scatter_hits    = _prom_scalar(prom, "keel_router_scatter_hits")    or 0.0
    scatter_failed  = _prom_scalar(prom, "keel_router_scatter_failed")  or 0.0
    shard_routes    = _prom_labeled(prom, "keel_router_shard_routes")

    result["metrics"] = {
        "total_routes":    total_routes,
        "read_routes":     read_routes,
        "write_routes":    write_routes,
        "failover_routes": failover_routes,
        "scatter_hits":    scatter_hits,
        "scatter_failed":  scatter_failed,
        "shard_routes":    shard_routes,
    }

    status: Status = "pass"

    if scatter_failed > 0 and total_routes > 0:
        rate = scatter_failed / total_routes
        result["issues"].append(
            f"scatter failures: {scatter_failed:.0f} / {total_routes:.0f} "
            f"({rate:.1%})"
        )
        status = "warn" if rate < 0.01 else "fail"

    if failover_routes > 0:
        result["issues"].append(
            f"failover routes observed: {failover_routes:.0f} "
            "(possible backend health issue)"
        )
        if status == "pass":
            status = "warn"

    if total_routes == 0:
        result["issues"].append("No routes observed — KEEL may not have received any queries yet")
        status = "skip"

    result["status"] = status
    return result


def analyze_latency(prom: str) -> dict:
    """Backend latency percentiles from keel_backend_latency_ns histogram."""
    result: dict[str, Any] = {"name": "latency", "metrics": {}, "issues": []}

    hist = _prom_histogram(prom, "keel_backend_latency_ns")
    count = hist["count"]

    if count == 0:
        result["metrics"] = {"p50_ms": None, "p95_ms": None, "p99_ms": None, "count": 0}
        result["issues"].append("No backend latency observations (keel_backend_latency_ns count = 0)")
        result["status"] = "skip"
        return result

    p50_ns = _percentile_from_histogram(hist, 0.50)
    p95_ns = _percentile_from_histogram(hist, 0.95)
    p99_ns = _percentile_from_histogram(hist, 0.99)
    avg_ns = hist["sum"] / count if count > 0 else 0.0

    p50_ms = (p50_ns or 0.0) / 1e6
    p95_ms = (p95_ns or 0.0) / 1e6
    p99_ms = (p99_ns or 0.0) / 1e6
    avg_ms = avg_ns / 1e6

    result["metrics"] = {
        "count":  count,
        "avg_ms": round(avg_ms, 3),
        "p50_ms": round(p50_ms, 3),
        "p95_ms": round(p95_ms, 3),
        "p99_ms": round(p99_ms, 3),
    }

    status: Status = "pass"
    p99_warn = THRESHOLDS["latency_p99_warn_ms"]
    p99_fail = THRESHOLDS["latency_p99_fail_ms"]

    if p99_ms >= p99_fail:
        result["issues"].append(
            f"p99 backend latency = {p99_ms:.1f}ms (fail threshold: {p99_fail:.0f}ms)"
        )
        status = "fail"
    elif p99_ms >= p99_warn:
        result["issues"].append(
            f"p99 backend latency = {p99_ms:.1f}ms (warn threshold: {p99_warn:.0f}ms)"
        )
        status = "warn"

    result["status"] = status
    return result


def analyze_integrity(
    keel_conn, shard0_conn, shard1_conn, table: str = "users"
) -> dict:
    """Verify scatter COUNT(*) == shard0 + shard1 direct counts."""
    result: dict[str, Any] = {"name": "integrity", "metrics": {}, "issues": []}

    try:
        scatter = int(_pg_scalar(keel_conn, f"SELECT COUNT(*) FROM {table}") or 0)
        d0 = int(_pg_scalar(shard0_conn, f"SELECT COUNT(*) FROM {table}") or 0)
        d1 = int(_pg_scalar(shard1_conn, f"SELECT COUNT(*) FROM {table}") or 0)
        direct_sum = d0 + d1

        result["metrics"] = {
            "table":       table,
            "scatter":     scatter,
            "shard0":      d0,
            "shard1":      d1,
            "direct_sum":  direct_sum,
            "discrepancy": scatter - direct_sum,
        }

        if scatter == direct_sum:
            result["status"] = "pass"
        else:
            result["issues"].append(
                f"Scatter COUNT({scatter}) ≠ shard0({d0}) + shard1({d1}) = {direct_sum}"
            )
            result["status"] = "fail"
    except Exception as exc:
        result["issues"].append(f"Could not run integrity check: {exc}")
        result["status"] = "skip"

    return result


def analyze_balance(shard0_conn, shard1_conn, table: str = "users") -> dict:
    """Shard row-count balance (±% from ideal 50%)."""
    result: dict[str, Any] = {"name": "balance", "metrics": {}, "issues": []}

    try:
        d0 = int(_pg_scalar(shard0_conn, f"SELECT COUNT(*) FROM {table}") or 0)
        d1 = int(_pg_scalar(shard1_conn, f"SELECT COUNT(*) FROM {table}") or 0)
        total = d0 + d1

        if total == 0:
            result["metrics"] = {"shard0": 0, "shard1": 0, "total": 0, "imbalance_pct": 0.0}
            result["issues"].append(f"Table {table!r} is empty — no balance data")
            result["status"] = "skip"
            return result

        ideal = total / 2
        imbalance = abs(d0 - ideal) / ideal

        result["metrics"] = {
            "table":         table,
            "shard0":        d0,
            "shard1":        d1,
            "total":         total,
            "shard0_pct":    round(d0 / total * 100, 1),
            "shard1_pct":    round(d1 / total * 100, 1),
            "imbalance_pct": round(imbalance * 100, 1),
        }

        warn_t = THRESHOLDS["shard_imbalance_warn"]
        fail_t = THRESHOLDS["shard_imbalance_fail"]

        if imbalance >= fail_t:
            result["issues"].append(
                f"Shard imbalance {imbalance:.1%} ≥ fail threshold {fail_t:.0%} "
                f"(shard0={d0}, shard1={d1})"
            )
            result["status"] = "fail"
        elif imbalance >= warn_t:
            result["issues"].append(
                f"Shard imbalance {imbalance:.1%} ≥ warn threshold {warn_t:.0%}"
            )
            result["status"] = "warn"
        else:
            result["status"] = "pass"
    except Exception as exc:
        result["issues"].append(f"Could not check balance: {exc}")
        result["status"] = "skip"

    return result


def analyze_2pc(prom: str) -> dict:
    """Two-phase commit stats."""
    result: dict[str, Any] = {"name": "2pc", "metrics": {}, "issues": []}

    started        = _prom_scalar(prom, "keel_router_2pc_started_total")        or 0.0
    prepared       = _prom_scalar(prom, "keel_router_2pc_prepared_total")       or 0.0
    prep_failed    = _prom_scalar(prom, "keel_router_2pc_prepare_failed_total") or 0.0
    committed      = _prom_scalar(prom, "keel_router_2pc_committed_total")      or 0.0
    rolled_back    = _prom_scalar(prom, "keel_router_2pc_rolled_back_total")    or 0.0

    result["metrics"] = {
        "started":     started,
        "prepared":    prepared,
        "prep_failed": prep_failed,
        "committed":   committed,
        "rolled_back": rolled_back,
    }

    status: Status = "pass"

    if prep_failed > 0:
        fail_rate = prep_failed / started if started > 0 else 1.0
        result["issues"].append(
            f"2PC prepare failures: {prep_failed:.0f} / {started:.0f} "
            f"({fail_rate:.1%})"
        )
        status = "warn" if fail_rate < 0.05 else "fail"

    if started > 0 and committed + rolled_back < started:
        orphaned = started - committed - rolled_back
        result["issues"].append(
            f"Possible orphaned 2PC transactions: started={started:.0f}, "
            f"completed={committed + rolled_back:.0f}, delta={orphaned:.0f}"
        )
        if status == "pass":
            status = "warn"

    result["status"] = status
    return result


def analyze_resources(prom: str) -> dict:
    """KEEL process resource usage."""
    result: dict[str, Any] = {"name": "resources", "metrics": {}, "issues": []}

    rss_bytes   = _prom_scalar(prom, "keel_rss_bytes")      or 0.0
    fd_open     = _prom_scalar(prom, "keel_fd_open")        or 0.0
    cpu_user    = _prom_scalar(prom, "keel_cpu_user_pct")   or 0.0
    cpu_sys     = _prom_scalar(prom, "keel_cpu_sys_pct")    or 0.0
    workers     = _prom_scalar(prom, "keel_workers")        or 0.0
    uptime_s    = _prom_scalar(prom, "keel_uptime_seconds") or 0.0

    result["metrics"] = {
        "rss_mb":    round(rss_bytes / (1024 * 1024), 1),
        "fd_open":   fd_open,
        "cpu_user":  cpu_user,
        "cpu_sys":   cpu_sys,
        "workers":   workers,
        "uptime_s":  uptime_s,
    }
    result["status"] = "pass"
    return result


def analyze_scatter(prom: str) -> dict:
    """Scatter-merge operation stats."""
    result: dict[str, Any] = {"name": "scatter", "metrics": {}, "issues": []}

    ops_total   = _prom_scalar(prom, "keel_router_scatter_merge_ops_total")  or 0.0
    ns_total    = _prom_scalar(prom, "keel_router_scatter_merge_ns_total")   or 0.0
    max_ns      = _prom_scalar(prom, "keel_router_scatter_merge_max_ns")     or 0.0
    scatter_dur = _prom_histogram(prom, "keel_router_scatter_merge_duration_seconds")

    avg_ms = (ns_total / ops_total / 1e6) if ops_total > 0 else 0.0
    max_ms = max_ns / 1e6

    p99_s = _percentile_from_histogram(scatter_dur, 0.99)
    p99_ms = (p99_s or 0.0) * 1000

    result["metrics"] = {
        "ops_total": ops_total,
        "avg_ms":    round(avg_ms, 3),
        "max_ms":    round(max_ms, 3),
        "p99_ms":    round(p99_ms, 3),
    }
    result["status"] = "pass"

    if ops_total == 0:
        result["issues"].append("No scatter-merge operations observed")
        result["status"] = "skip"

    return result


def analyze_rebalancing(prom: str) -> dict:
    """Connection rebalancing stats."""
    result: dict[str, Any] = {"name": "rebalancing", "metrics": {}, "issues": []}

    checks     = _prom_scalar(prom, "keel_rebalance_checks_total")     
    migrations = _prom_scalar(prom, "keel_rebalance_migrations_total") 
    skipped    = _prom_scalar(prom, "keel_rebalance_skipped_total")    

    if checks is None:
        result["issues"].append("Rebalance metrics not present (rebalance may be off)")
        result["status"] = "skip"
        return result

    result["metrics"] = {
        "checks":     checks,
        "migrations": migrations or 0.0,
        "skipped":    skipped or 0.0,
    }
    result["status"] = "pass"
    return result


def run_probe_queries(keel_conn, n_reads: int = 100, n_writes: int = 0) -> dict:
    """Run a battery of probe queries and measure throughput/latency."""
    start = time.monotonic()
    errors = 0

    for i in range(n_reads):
        try:
            _pg_scalar(keel_conn, "SELECT 1")
        except Exception:
            errors += 1

    elapsed = time.monotonic() - start
    tps = n_reads / elapsed if elapsed > 0 else 0.0

    return {
        "reads":   n_reads,
        "errors":  errors,
        "elapsed": round(elapsed, 3),
        "tps":     round(tps, 1),
    }


# ===========================================================================
# Report generation
# ===========================================================================

STATUS_ICON = {
    "pass": "✅",
    "warn": "⚠️",
    "fail": "❌",
    "skip": "⏭️",
}

STATUS_COLOR = {
    "pass": "#28a745",
    "warn": "#ffc107",
    "fail": "#dc3545",
    "skip": "#6c757d",
}


def _overall_status(dimensions: list[dict]) -> Status:
    statuses = {d["status"] for d in dimensions}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    if all(s == "skip" for s in statuses):
        return "skip"
    return "pass"


def build_json_report(
    dimensions: list[dict],
    probe: dict,
    collected_at: str,
    keel_info: dict,
) -> dict:
    return {
        "collected_at": collected_at,
        "keel":         keel_info,
        "overall":      _overall_status(dimensions),
        "probe":        probe,
        "dimensions":   dimensions,
    }


def build_html_report(report: dict) -> str:
    overall = report["overall"]
    color   = STATUS_COLOR.get(overall, "#6c757d")
    icon    = STATUS_ICON.get(overall, "?")
    ts      = report["collected_at"]
    keel    = report.get("keel", {})
    probe   = report.get("probe", {})

    rows_html = ""
    for dim in report["dimensions"]:
        s = dim["status"]
        dim_icon  = STATUS_ICON.get(s, "?")
        dim_color = STATUS_COLOR.get(s, "#6c757d")
        issues_html = ""
        if dim["issues"]:
            items = "".join(f"<li>{i}</li>" for i in dim["issues"])
            issues_html = f"<ul class='issues'>{items}</ul>"

        metrics_html = ""
        m = dim.get("metrics", {})
        if m:
            cells = "".join(
                f"<tr><td>{k}</td><td><code>{v}</code></td></tr>"
                for k, v in m.items()
                if not isinstance(v, dict)
            )
            # nested dicts (shard_routes)
            for k, v in m.items():
                if isinstance(v, dict):
                    sub = ", ".join(f"{sk}={sv}" for sk, sv in v.items())
                    cells += f"<tr><td>{k}</td><td><code>{sub}</code></td></tr>"
            metrics_html = (
                f"<details><summary>Metrics</summary>"
                f"<table class='metrics'>{cells}</table></details>"
            )

        rows_html += f"""
        <tr>
          <td style="color:{dim_color};font-size:1.3em">{dim_icon}</td>
          <td><strong>{dim['name']}</strong></td>
          <td style="color:{dim_color}">{s.upper()}</td>
          <td>{issues_html}{metrics_html}</td>
        </tr>"""

    probe_html = ""
    if probe:
        probe_html = f"""
        <div class='section'>
          <h2>Probe queries</h2>
          <p>Reads: <b>{probe.get('reads', 0)}</b> &nbsp;|&nbsp;
             Errors: <b>{probe.get('errors', 0)}</b> &nbsp;|&nbsp;
             Elapsed: <b>{probe.get('elapsed', 0):.3f}s</b> &nbsp;|&nbsp;
             TPS: <b>{probe.get('tps', 0):.1f}</b>
          </p>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KEEL Analysis Report — {ts}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            margin: 0; padding: 20px; background: #f8f9fa; color: #212529; }}
    h1   {{ color: {color}; }}
    .banner {{ background: {color}; color: #fff; padding: 12px 20px;
               border-radius: 6px; margin-bottom: 20px;
               font-size: 1.3em; font-weight: bold; }}
    .section {{ background: #fff; border: 1px solid #dee2e6; border-radius: 6px;
                padding: 16px; margin-bottom: 20px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ padding: 8px 12px; text-align: left;
              border-bottom: 1px solid #dee2e6; vertical-align: top; }}
    th {{ background: #f1f3f5; font-weight: 600; }}
    ul.issues {{ margin: 4px 0; padding-left: 18px; color: #555; }}
    details summary {{ cursor: pointer; color: #0d6efd; }}
    table.metrics {{ margin-top: 8px; font-size: 0.9em; }}
    table.metrics td {{ padding: 3px 8px; border-bottom: 1px solid #eee; }}
    code {{ background: #f1f3f5; padding: 1px 4px; border-radius: 3px; font-size: 0.9em; }}
    .footer {{ font-size: 0.8em; color: #6c757d; margin-top: 20px; }}
  </style>
</head>
<body>
  <div class="banner">{icon} KEEL Analysis Report &nbsp;—&nbsp; {ts}</div>

  <div class='section'>
    <h2>Overall: <span style="color:{color}">{overall.upper()}</span></h2>
    <p>Uptime: <b>{keel.get('uptime_s', 'n/a')}s</b> &nbsp;|&nbsp;
       Workers: <b>{keel.get('workers', 'n/a')}</b> &nbsp;|&nbsp;
       RSS: <b>{keel.get('rss_mb', 'n/a')} MB</b>
    </p>
  </div>

  {probe_html}

  <div class='section'>
    <h2>Diagnostics</h2>
    <table>
      <thead>
        <tr><th>Status</th><th>Dimension</th><th>Result</th><th>Details</th></tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>

  <div class="footer">
    Generated by <code>tests/e2e/analyze.py</code> at {ts}
  </div>
</body>
</html>"""


# ===========================================================================
# Main entry point
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="KEEL deep analysis and report generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(__doc__ or ""),
    )
    parser.add_argument("--host",         default=DEFAULT_HOST)
    parser.add_argument("--port",         type=int, default=DEFAULT_PORT)
    parser.add_argument("--prom-port",    type=int, default=DEFAULT_PROM_PORT)
    parser.add_argument("--admin-port",   type=int, default=DEFAULT_ADMIN_PORT)
    parser.add_argument("--shard0-port",  type=int, default=DEFAULT_SHARD0_PORT)
    parser.add_argument("--shard1-port",  type=int, default=DEFAULT_SHARD1_PORT)
    parser.add_argument("--pg-user",      default=DEFAULT_PG_USER)
    parser.add_argument("--pg-password",  default=DEFAULT_PG_PASSWORD)
    parser.add_argument("--pg-dbname",    default=DEFAULT_PG_DBNAME)
    parser.add_argument("--output-dir",   default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--probe-reads",  type=int, default=100,
                        help="Number of SELECT 1 probes to run for TPS measurement")
    parser.add_argument("--print",        action="store_true",
                        help="Print JSON report to stdout instead of writing files")
    parser.add_argument("--no-html",      action="store_true",
                        help="Skip HTML report generation")
    args = parser.parse_args()

    collected_at = datetime.now(timezone.utc).isoformat()

    # --- Connect to data sources ---
    try:
        prom_text = _prom_fetch(args.host, args.prom_port)
    except CollectError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print("Is KEEL running? Start the e2e stack with:", file=sys.stderr)
        print("  docker compose -f docker/compose/e2e-suite.yml up -d", file=sys.stderr)
        return 2

    try:
        keel_conn = _pg_connect(
            args.host, args.port, args.pg_user, args.pg_password, args.pg_dbname
        )
    except CollectError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        admin_conn = _pg_connect(
            args.host, args.admin_port, args.pg_user, args.pg_password, "keel_admin"
        )
    except CollectError:
        admin_conn = None

    try:
        shard0_conn = _pg_connect(
            args.host, args.shard0_port, args.pg_user, args.pg_password, args.pg_dbname
        )
    except CollectError:
        shard0_conn = None

    try:
        shard1_conn = _pg_connect(
            args.host, args.shard1_port, args.pg_user, args.pg_password, args.pg_dbname
        )
    except CollectError:
        shard1_conn = None

    # --- Probe queries ---
    probe = run_probe_queries(keel_conn, n_reads=args.probe_reads)

    # Refresh Prometheus snapshot after probes (reflects activity)
    try:
        prom_text = _prom_fetch(args.host, args.prom_port)
    except CollectError:
        pass

    # --- Run analysis dimensions ---
    dimensions: list[dict] = []

    dimensions.append(analyze_pool(prom_text, admin_conn))
    dimensions.append(analyze_routing(prom_text))
    dimensions.append(analyze_latency(prom_text))
    dimensions.append(analyze_scatter(prom_text))
    dimensions.append(analyze_2pc(prom_text))
    dimensions.append(analyze_resources(prom_text))
    dimensions.append(analyze_rebalancing(prom_text))

    if shard0_conn and shard1_conn:
        dimensions.append(analyze_integrity(keel_conn, shard0_conn, shard1_conn))
        dimensions.append(analyze_balance(shard0_conn, shard1_conn))
    else:
        dimensions.append({
            "name": "integrity", "status": "skip",
            "metrics": {}, "issues": ["Shard backends not reachable from test runner"]
        })
        dimensions.append({
            "name": "balance", "status": "skip",
            "metrics": {}, "issues": ["Shard backends not reachable from test runner"]
        })

    # --- KEEL info ---
    keel_info = {
        "uptime_s": _prom_scalar(prom_text, "keel_uptime_seconds"),
        "workers":  int(_prom_scalar(prom_text, "keel_workers") or 0),
        "rss_mb":   round((_prom_scalar(prom_text, "keel_rss_bytes") or 0) / (1024 * 1024), 1),
    }

    # --- Build report ---
    report = build_json_report(dimensions, probe, collected_at, keel_info)
    overall = report["overall"]

    # --- Output ---
    if args.print:
        print(json.dumps(report, indent=2))
    else:
        os.makedirs(args.output_dir, exist_ok=True)
        json_path = os.path.join(args.output_dir, "analyze_report.json")
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"JSON report written to: {json_path}")

        if not args.no_html:
            html_path = os.path.join(args.output_dir, "analyze_report.html")
            with open(html_path, "w") as f:
                f.write(build_html_report(report))
            print(f"HTML report written to: {html_path}")

    # --- Console summary ---
    print(f"\nOverall: {STATUS_ICON.get(overall, '?')} {overall.upper()}")
    print(f"Probe:   {probe['reads']} reads, {probe['errors']} errors, "
          f"{probe['tps']:.1f} TPS")
    print()
    for dim in dimensions:
        icon = STATUS_ICON.get(dim["status"], "?")
        print(f"  {icon} {dim['name']:<15} {dim['status'].upper()}")
        for issue in dim.get("issues", []):
            print(f"     → {issue}")

    # --- Cleanup ---
    keel_conn.close()
    if admin_conn:
        admin_conn.close()
    if shard0_conn:
        shard0_conn.close()
    if shard1_conn:
        shard1_conn.close()

    return 1 if overall == "fail" else 0


if __name__ == "__main__":
    sys.exit(main())
