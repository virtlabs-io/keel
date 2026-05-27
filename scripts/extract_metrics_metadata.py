#!/usr/bin/env python3
"""
extract_metrics_metadata.py — single source of truth for KEEL metric inventory.

Parses both metric-registering sources:
  - src/observability/otlp/keel_prom_format.c  (k_meta[]) — OTLP/Prom pathway
  - src/admin/admin.c                          (PROM_COUNTER/PROM_GAUGE macros
                                                + raw `# HELP` lines)

Emits a sorted JSON array of { name, type, help, surface } records, where
`surface` is one of:
  - "otlp"        (visible via both OTLP/HTTP and admin /metrics)
  - "admin"       (admin /metrics only)

Used by:
  - scripts/check_metrics_reference.sh         (lint parity gate)
  - docs/METRICS_REFERENCE.md generation       (manual regeneration)

Usage:
  scripts/extract_metrics_metadata.py            # default JSON to stdout
  scripts/extract_metrics_metadata.py --names    # one metric name per line
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

OTLP_SRC = ROOT / "src/observability/otlp/keel_prom_format.c"
ADMIN_SRC = ROOT / "src/admin/admin.c"

# k_meta[] row: { "keel_xxx",   "help text" },
KMETA_ROW = re.compile(
    r'\{\s*"(keel_[A-Za-z0-9_]+)"\s*,\s*"([^"]+)"\s*\}'
)

# PROM_COUNTER / PROM_GAUGE("metric", "help", field)
PROM_MACRO = re.compile(
    r'PROM_(COUNTER|GAUGE)\(\s*"([A-Za-z0-9_]+)"\s*,\s*"([^"]+)"'
)

# Raw `fprintf(f, "# HELP keel_xxx help text\n");` / proxy_xxx
RAW_HELP = re.compile(
    r'"# HELP ((?:keel|proxy)_[A-Za-z0-9_]+)\s+([^"\n]+?)(?:\\n)?"\s*\)'
)

# Raw `fprintf(f, "# TYPE keel_xxx (counter|gauge)\n");` / proxy_xxx
RAW_TYPE = re.compile(
    r'"# TYPE ((?:keel|proxy)_[A-Za-z0-9_]+)\s+(counter|gauge)\\n"'
)


def parse_otlp(src: Path) -> list[dict]:
    """Parse keel_prom_format.c k_meta[] table."""
    text = src.read_text()
    # Restrict to the k_meta[] block to avoid false positives elsewhere.
    m = re.search(
        r'static const prom_meta_t k_meta\[\]\s*=\s*\{(.*?)\n\};',
        text,
        flags=re.DOTALL,
    )
    if not m:
        sys.stderr.write(f"FAIL: k_meta[] block not found in {src}\n")
        sys.exit(2)
    block = m.group(1)
    out = []
    for name, help_ in KMETA_ROW.findall(block):
        # Counters carry _total suffix per Prometheus convention.
        mtype = "counter" if name.endswith("_total") else "gauge"
        out.append({
            "name": name,
            "type": mtype,
            "help": help_,
            "surface": "otlp",
        })
    return out


def parse_admin(src: Path) -> list[dict]:
    """Parse admin.c — PROM_COUNTER / PROM_GAUGE macros + raw HELP/TYPE pairs."""
    text = src.read_text()
    out: dict[str, dict] = {}
    for kind, suffix, help_ in PROM_MACRO.findall(text):
        name = f"keel_{suffix}"
        mtype = "counter" if kind == "COUNTER" else "gauge"
        out[name] = {
            "name": name, "type": mtype, "help": help_, "surface": "admin",
        }
    # Raw HELP lines (cluster, trace, /proc, TLS — emitted outside macros).
    helps = dict(RAW_HELP.findall(text))
    types = dict(RAW_TYPE.findall(text))
    for name, help_ in helps.items():
        mtype = types.get(name)
        if mtype is None:
            # Best-effort fallback: _total suffix → counter, else gauge.
            mtype = "counter" if name.endswith("_total") else "gauge"
        out.setdefault(name, {
            "name": name, "type": mtype, "help": help_, "surface": "admin",
        })
    return list(out.values())


def merge(otlp: list[dict], admin: list[dict]) -> list[dict]:
    """Merge with OTLP precedence for the `surface` field."""
    by_name: dict[str, dict] = {}
    for m in admin:
        by_name[m["name"]] = m
    for m in otlp:
        by_name[m["name"]] = m  # OTLP wins → surface="otlp"
    return sorted(by_name.values(), key=lambda r: r["name"])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--names", action="store_true",
                    help="emit one metric name per line instead of JSON")
    args = ap.parse_args()

    otlp = parse_otlp(OTLP_SRC)
    admin = parse_admin(ADMIN_SRC)
    merged = merge(otlp, admin)

    if args.names:
        for m in merged:
            print(m["name"])
    else:
        json.dump(merged, sys.stdout, indent=2)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
