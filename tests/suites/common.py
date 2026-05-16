"""
tests/suites/common.py
======================
Shared utilities for all KEEL Python test suites (A–H).

Provides:
  - Build directory detection
  - CTest runner
  - Network helpers (port polling, raw PostgreSQL wire protocol)
  - ASAN / TSAN output parsing
  - Latency statistics
  - ProxyConn — a raw-TCP PostgreSQL wire protocol client
  - Proxy environment configuration (from env vars)
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Build directories to probe, in preference order
_BUILD_CANDIDATES = [
    "build-host", "build", "build-linux",
    "build-release", "build-asan", "build-tsan",
]

# ---------------------------------------------------------------------------
# Build directory detection
# ---------------------------------------------------------------------------

def find_build_dir(prefer: str | None = None) -> Path | None:
    """Return the first existing CMake build directory."""
    if prefer:
        p = REPO_ROOT / prefer
        if (p / "CTestTestfile.cmake").exists():
            return p
    env_val = os.environ.get("KEEL_BUILD_DIR")
    if env_val:
        p = Path(env_val)
        if (p / "CTestTestfile.cmake").exists():
            return p
    for name in _BUILD_CANDIDATES:
        p = REPO_ROOT / name
        if (p / "CTestTestfile.cmake").exists():
            return p
    return None


def find_asan_build() -> Path | None:
    p = REPO_ROOT / "build-asan"
    return p if (p / "CTestTestfile.cmake").exists() else None


def find_tsan_build() -> Path | None:
    p = REPO_ROOT / "build-tsan"
    return p if (p / "CTestTestfile.cmake").exists() else None


def find_lsan_build() -> Path | None:
    p = REPO_ROOT / "build-lsan"
    return p if (p / "CTestTestfile.cmake").exists() else None


def find_test_binary(name: str, build_dir: Path) -> Path | None:
    """Find a compiled test binary inside a build tree."""
    for sub in ("tests", "src", ""):
        p = build_dir / sub / name
        if p.exists() and p.is_file():
            return p
    # Recursive search inside tests/
    for p in (build_dir / "tests").rglob(name):
        if p.is_file():
            return p
    return None


# ---------------------------------------------------------------------------
# CTest / binary runner helpers
# ---------------------------------------------------------------------------

def run_ctest(
    build_dir: Path,
    regex: str | None = None,
    timeout: int = 600,
    env: dict | None = None,
    jobs: int = 0,
    extra_args: list[str] | None = None,
) -> tuple[int, str]:
    """
    Run ctest inside *build_dir*.  Returns (returncode, combined_stdout_stderr).
    """
    cmd: list[str] = ["ctest", "--output-on-failure", "--no-compress-output"]
    if regex:
        cmd += ["-R", regex]
    if jobs:
        cmd += ["-j", str(jobs)]
    if extra_args:
        cmd += extra_args

    merged_env = {**os.environ, **(env or {})}
    proc = subprocess.run(
        cmd, cwd=build_dir, capture_output=True, text=True,
        timeout=timeout, env=merged_env,
    )
    return proc.returncode, proc.stdout + "\n" + proc.stderr


def run_binary(
    binary: Path,
    args: list[str] | None = None,
    timeout: int = 120,
    env: dict | None = None,
) -> tuple[int, str]:
    """Run a single compiled binary.  Returns (returncode, combined_output)."""
    cmd = [str(binary)] + (args or [])
    merged_env = {**os.environ, **(env or {})}
    proc = subprocess.run(
        cmd, capture_output=True, text=True,
        timeout=timeout, env=merged_env,
    )
    return proc.returncode, proc.stdout + "\n" + proc.stderr


def check_command(name: str) -> bool:
    """Return True if *name* is available on PATH."""
    return shutil.which(name) is not None


# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

def wait_for_port(
    host: str, port: int, timeout: float = 15.0, interval: float = 0.25,
) -> bool:
    """Poll until the TCP port accepts connections. Returns True on success."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Sanitizer output parsing
# ---------------------------------------------------------------------------

_ASAN_RE = re.compile(
    r"(?:AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer):\s*(.+?)(?:\n|$)",
    re.MULTILINE,
)
_TSAN_RE = re.compile(r"ThreadSanitizer:\s*(.+?)(?:\n|$)", re.MULTILINE)


def parse_asan_errors(text: str) -> list[str]:
    return _ASAN_RE.findall(text)


def parse_tsan_races(text: str) -> list[str]:
    return _TSAN_RE.findall(text)


def has_sanitizer_error(text: str) -> bool:
    return bool(_ASAN_RE.search(text) or _TSAN_RE.search(text))


# ---------------------------------------------------------------------------
# Latency statistics
# ---------------------------------------------------------------------------

def percentile(values: list[float], p: float) -> float:
    """Return the p-th percentile of *values* using linear interpolation."""
    if not values:
        return 0.0
    sv = sorted(values)
    idx = (p / 100.0) * (len(sv) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sv) - 1)
    return sv[lo] + (sv[hi] - sv[lo]) * (idx - lo)


def latency_stats(values_ms: list[float]) -> dict:
    if not values_ms:
        return {}
    return {
        "count":   len(values_ms),
        "mean_ms": sum(values_ms) / len(values_ms),
        "min_ms":  min(values_ms),
        "p50_ms":  percentile(values_ms, 50),
        "p95_ms":  percentile(values_ms, 95),
        "p99_ms":  percentile(values_ms, 99),
        "p999_ms": percentile(values_ms, 99.9),
        "max_ms":  max(values_ms),
    }


# ---------------------------------------------------------------------------
# PostgreSQL wire protocol helpers (raw TCP, no libpq)
# ---------------------------------------------------------------------------

PG_PROTO_V3    = 196608  # protocol 3.0  = 3 << 16
PG_SSL_CODE    = 80877103
PG_CANCEL_CODE = 80877102


def pg_startup_msg(
    user: str = "postgres",
    database: str = "postgres",
    app_name: str = "keel-suite",
    extra_params: dict[str, str] | None = None,
) -> bytes:
    """Build a valid PostgreSQL v3 startup message."""
    body = b"user\x00" + user.encode() + b"\x00"
    body += b"database\x00" + database.encode() + b"\x00"
    body += b"application_name\x00" + app_name.encode() + b"\x00"
    for k, v in (extra_params or {}).items():
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    length = 4 + 4 + len(body)
    return struct.pack(">II", length, PG_PROTO_V3) + body


def pg_ssl_request() -> bytes:
    return struct.pack(">II", 8, PG_SSL_CODE)


def pg_cancel_request(pid: int, secret: int) -> bytes:
    return struct.pack(">IIII", 16, PG_CANCEL_CODE, pid, secret)


def pg_query(sql: str) -> bytes:
    body = sql.encode() + b"\x00"
    return b"Q" + struct.pack(">I", 4 + len(body)) + body


def pg_terminate() -> bytes:
    return b"X" + struct.pack(">I", 4)


def pg_sync() -> bytes:
    return b"S" + struct.pack(">I", 4)


def pg_flush() -> bytes:
    return b"H" + struct.pack(">I", 4)


def pg_parse(stmt: str, query: str, param_types: list[int] | None = None) -> bytes:
    body = (
        stmt.encode() + b"\x00"
        + query.encode() + b"\x00"
        + struct.pack(">H", len(param_types or []))
        + b"".join(struct.pack(">I", t) for t in (param_types or []))
    )
    return b"P" + struct.pack(">I", 4 + len(body)) + body


def pg_bind(
    portal: str, stmt: str,
    param_formats: list[int] | None = None,
    params: list[bytes | None] | None = None,
    result_formats: list[int] | None = None,
) -> bytes:
    pfmts = param_formats or []
    pvals = params or []
    rfmts = result_formats or []
    body = (
        portal.encode() + b"\x00"
        + stmt.encode() + b"\x00"
        + struct.pack(">H", len(pfmts))
        + b"".join(struct.pack(">H", f) for f in pfmts)
        + struct.pack(">H", len(pvals))
    )
    for v in pvals:
        if v is None:
            body += struct.pack(">i", -1)
        else:
            body += struct.pack(">I", len(v)) + v
    body += struct.pack(">H", len(rfmts))
    body += b"".join(struct.pack(">H", f) for f in rfmts)
    return b"B" + struct.pack(">I", 4 + len(body)) + body


def pg_describe(kind: str, name: str) -> bytes:
    """Describe a prepared statement ('S') or portal ('P')."""
    body = kind.encode()[:1] + name.encode() + b"\x00"
    return b"D" + struct.pack(">I", 4 + len(body)) + body


def pg_execute(portal: str, max_rows: int = 0) -> bytes:
    body = portal.encode() + b"\x00" + struct.pack(">I", max_rows)
    return b"E" + struct.pack(">I", 4 + len(body)) + body


def pg_close(kind: str, name: str) -> bytes:
    body = kind.encode()[:1] + name.encode() + b"\x00"
    return b"C" + struct.pack(">I", 4 + len(body)) + body


# ---------------------------------------------------------------------------
# ProxyConn — raw wire-level connection to any PostgreSQL-compatible server
# ---------------------------------------------------------------------------

class ProxyConn:
    """
    Raw TCP connection speaking the PostgreSQL v3 wire protocol.

    Does NOT use libpq — all encoding is manual, making it suitable for
    protocol compliance and fuzz tests.
    """

    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host    = host
        self.port    = port
        self.timeout = timeout
        self._sock: socket.socket | None = None
        self.backend_pid:    int = 0
        self.backend_secret: int = 0

    # --- connection management -------------------------------------------

    def connect(self) -> None:
        self._sock = socket.create_connection(
            (self.host, self.port), timeout=self.timeout
        )
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def set_timeout(self, seconds: float) -> None:
        if self._sock:
            self._sock.settimeout(seconds)

    # --- raw I/O ----------------------------------------------------------

    def send(self, data: bytes) -> None:
        assert self._sock, "not connected"
        self._sock.sendall(data)

    def send_fragmented(self, data: bytes, chunk_size: int = 1) -> None:
        """Send *data* in chunks of *chunk_size* bytes (tests reassembly)."""
        assert self._sock
        for i in range(0, len(data), chunk_size):
            self._sock.sendall(data[i : i + chunk_size])
            if chunk_size < len(data):
                time.sleep(0.001)  # give the receiver a chance to see fragments

    def recv_exact(self, n: int) -> bytes:
        assert self._sock
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed by peer")
            buf += chunk
        return buf

    def recv_message(self) -> tuple[int, bytes]:
        """Read exactly one backend message.  Returns (type_byte, body)."""
        hdr = self.recv_exact(5)
        msg_type = hdr[0]
        msg_len  = struct.unpack(">I", hdr[1:5])[0]
        body = self.recv_exact(msg_len - 4) if msg_len > 4 else b""
        return msg_type, body

    def recv_until(
        self, stop_types: set[int], max_msgs: int = 100
    ) -> list[tuple[int, bytes]]:
        """Collect messages until a stop-type byte is seen (inclusive)."""
        msgs: list[tuple[int, bytes]] = []
        for _ in range(max_msgs):
            t, b = self.recv_message()
            msgs.append((t, b))
            if t in stop_types:
                break
        return msgs

    # --- high-level handshake --------------------------------------------

    def startup(
        self,
        user:     str = "postgres",
        database: str = "postgres",
        password: str = "postgres",
    ) -> bool:
        """
        Perform a startup + authentication handshake.
        Returns True when ReadyForQuery is received.
        Stores backend_pid / backend_secret for cancel requests.
        """
        self.send(pg_startup_msg(user, database))
        for _ in range(30):
            t, body = self.recv_message()
            if t == ord("R"):
                auth_type = struct.unpack(">I", body[:4])[0] if len(body) >= 4 else 0
                if auth_type == 0:        # AuthenticationOk
                    pass
                elif auth_type == 3:      # AuthenticationCleartextPassword
                    pw = password.encode() + b"\x00"
                    self.send(b"p" + struct.pack(">I", 4 + len(pw)) + pw)
                elif auth_type == 5:      # AuthenticationMD5Password
                    salt = body[4:8]
                    h1 = hashlib.md5(password.encode() + user.encode()).hexdigest()
                    h2 = "md5" + hashlib.md5(
                        (h1 + salt.decode("latin-1")).encode()
                    ).hexdigest()
                    pw = h2.encode() + b"\x00"
                    self.send(b"p" + struct.pack(">I", 4 + len(pw)) + pw)
                else:
                    return False          # unsupported auth mechanism
            elif t == ord("K"):           # BackendKeyData
                if len(body) >= 8:
                    self.backend_pid, self.backend_secret = struct.unpack(">II", body[:8])
            elif t == ord("S"):           # ParameterStatus — ignore
                pass
            elif t == ord("Z"):           # ReadyForQuery
                return True
            elif t == ord("E"):           # ErrorResponse
                return False
        return False

    # --- context manager -------------------------------------------------

    def __enter__(self) -> "ProxyConn":
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Proxy environment (env-var driven)
# ---------------------------------------------------------------------------

def proxy_env() -> dict:
    """Return proxy connection parameters, driven by environment variables."""
    return {
        "host":     os.environ.get("KEEL_HOST",     "127.0.0.1"),
        "port":     int(os.environ.get("KEEL_PORT",     "5432")),
        "user":     os.environ.get("KEEL_USER",     "postgres"),
        "password": os.environ.get("KEEL_PASSWORD", "postgres"),
        "database": os.environ.get("KEEL_DATABASE", "postgres"),
    }


def is_proxy_reachable(env: dict | None = None) -> bool:
    """Quick check — returns True if the proxy TCP port is accepting connections."""
    e = env or proxy_env()
    return wait_for_port(e["host"], e["port"], timeout=1.0)
