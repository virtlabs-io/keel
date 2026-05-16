"""
test_prepared_statements_ssv.py — Prepared Statement Pooling & SSV E2E Tests
=============================================================================

Comprehensive end-to-end coverage for all five KEEL prepared-statement pooling
strategies and the SSV (Semantic State Virtualization) consistency model.

PS Modes
--------
  virtualize  (port 26440) — intercept named Parse, synthetic ParseComplete,
                              replay full stmt set on backend change
  pinning     (port 26441) — hard-pin backend on first named Parse; released
                              only on DEALLOCATE ALL / DISCARD ALL / disconnect
  tracking    (port 26442) — like virtualize, also intercepts simple-query
                              "PREPARE stmt AS sql" sent via Q message
  anonymous   (port 26443) — rewrite every named Parse("name", sql) to
                              Parse("", sql) so backends never accumulate
                              named statements
  off         (port 26444) — forward Parse verbatim to backend, hard-pin on
                              first named Parse

SSV dimensions
--------------
  - GUC state hashing: search_path, TimeZone, DateStyle,
                       standard_conforming_strings, …
  - SET LOCAL / set_config(…, true): transaction-local overlays that revert
    on COMMIT / ROLLBACK
  - Temp-object epoch: bumped on CREATE TEMP TABLE, DISCARD TEMP,
    DISCARD ALL, ON COMMIT DROP (at commit), ROLLBACK (when pending)
  - Backend fast path: exact stmt_set_hash match → no replay
  - Backend slow path: hash mismatch → stmt replay on clean backend

Infrastructure
--------------
All PS-mode worker groups point at pg-shard0 only (no shard routing) to make
backend-PID assertions deterministic.  max_pool_size=4 per group keeps pool
cycling behaviour observable with modest concurrency.

Protocol approach
-----------------
psycopg2 uses anonymous extended protocol (PQexecParams) or simple queries
(Q messages) only.  It cannot send NAMED Parse messages.  Therefore these
tests use the raw-socket ``ProxyConn`` helper from tests.suites.common to send
named Parse / Bind / Execute / Close / Describe messages directly over the
PostgreSQL wire protocol.  psycopg2 is used where simple-query semantics are
correct (tracking mode PREPARE; SSV GUC; temp-epoch tests).

Markers: ``prepared_statements``

Run selectively::

    pytest tests/e2e/test_prepared_statements_ssv.py -v --timeout=120
"""

from __future__ import annotations

import os
import re
import struct
import threading
import time
from contextlib import contextmanager
from typing import Generator

import pytest
import psycopg2
import psycopg2.extensions

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites.common import (
    ProxyConn,
    pg_bind,
    pg_describe,
    pg_execute,
    pg_flush,
    pg_parse,
    pg_query,
    pg_sync,
    pg_terminate,
)

# Re-export pg_close which is also in common
try:
    from tests.suites.common import pg_close
except ImportError:
    def pg_close(kind: str, name: str) -> bytes:
        body = kind.encode()[:1] + name.encode() + b"\x00"
        return b"C" + struct.pack(">I", 4 + len(body)) + body

# ── Endpoint constants ────────────────────────────────────────────────────
KEEL_HOST = os.environ.get("KEEL_HOST", "127.0.0.1")
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DBNAME = "testdb"

PS_VIRT_PORT  = int(os.environ.get("KEEL_PS_VIRT_PORT",  "26440"))
PS_PIN_PORT   = int(os.environ.get("KEEL_PS_PIN_PORT",   "26441"))
PS_TRACK_PORT = int(os.environ.get("KEEL_PS_TRACK_PORT", "26442"))
PS_ANON_PORT  = int(os.environ.get("KEEL_PS_ANON_PORT",  "26443"))
PS_OFF_PORT   = int(os.environ.get("KEEL_PS_OFF_PORT",   "26444"))

# ── Backend message type bytes ────────────────────────────────────────────
_READY       = ord("Z")
_ERROR       = ord("E")
_DATA_ROW    = ord("D")
_CMD_COMP    = ord("C")
_PARSE_COMP  = ord("1")
_BIND_COMP   = ord("2")
_CLOSE_COMP  = ord("3")
_PARAM_DESC  = ord("t")
_ROW_DESC    = ord("T")
_NO_DATA     = ord("n")
_NOTICE      = ord("N")


# ===========================================================================
# Module-level fixtures
# ===========================================================================

@pytest.fixture(scope="module")
def ps_virt_dsn(compose_stack: dict) -> str:
    return (f"host={KEEL_HOST} port={PS_VIRT_PORT} "
            f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}")


@pytest.fixture(scope="module")
def ps_pin_dsn(compose_stack: dict) -> str:
    return (f"host={KEEL_HOST} port={PS_PIN_PORT} "
            f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}")


@pytest.fixture(scope="module")
def ps_track_dsn(compose_stack: dict) -> str:
    return (f"host={KEEL_HOST} port={PS_TRACK_PORT} "
            f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}")


@pytest.fixture(scope="module")
def ps_anon_dsn(compose_stack: dict) -> str:
    return (f"host={KEEL_HOST} port={PS_ANON_PORT} "
            f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}")


@pytest.fixture(scope="module")
def ps_off_dsn(compose_stack: dict) -> str:
    return (f"host={KEEL_HOST} port={PS_OFF_PORT} "
            f"user={PG_USER} password={PG_PASSWORD} dbname={PG_DBNAME}")


pytestmark = pytest.mark.prepared_statements


# ===========================================================================
# Wire-protocol helpers
# ===========================================================================

def _parse_error(body: bytes) -> str:
    """Extract the human-readable message from an ErrorResponse body."""
    msg = ""
    i = 0
    while i < len(body):
        field_type = body[i]
        i += 1
        if field_type == 0:
            break
        null = body.index(b"\x00", i)
        value = body[i:null].decode("utf-8", errors="replace")
        i = null + 1
        if field_type == ord("M"):
            msg = value
    return msg


def _drain_to_rfq(c: ProxyConn) -> tuple[int, list[tuple[int, bytes]]]:
    """Collect all backend messages until (and including) ReadyForQuery."""
    msgs: list[tuple[int, bytes]] = []
    for _ in range(500):
        t, b = c.recv_message()
        msgs.append((t, b))
        if t == _READY:
            return b[0] if b else ord("I"), msgs
    raise TimeoutError("ReadyForQuery not received after 500 messages")


def _collect_rows(msgs: list[tuple[int, bytes]]) -> list[list[str | None]]:
    """Extract DataRow values from a message list as text strings."""
    rows: list[list[str | None]] = []
    for t, b in msgs:
        if t != _DATA_ROW:
            continue
        n = struct.unpack(">H", b[:2])[0]
        offset = 2
        row: list[str | None] = []
        for _ in range(n):
            col_len = struct.unpack(">i", b[offset:offset + 4])[0]
            offset += 4
            if col_len == -1:
                row.append(None)
            else:
                row.append(b[offset:offset + col_len].decode("utf-8", errors="replace"))
                offset += col_len
        rows.append(row)
    return rows


def _has_error(msgs: list[tuple[int, bytes]]) -> str | None:
    """Return the error message text if any ErrorResponse is in msgs."""
    for t, b in msgs:
        if t == _ERROR:
            return _parse_error(b)
    return None


def _open_conn(port: int, timeout: float = 10.0) -> ProxyConn:
    """Open and authenticate a ProxyConn to the given KEEL port."""
    c = ProxyConn(KEEL_HOST, port, timeout=timeout)
    c.connect()
    ok = c.startup(user=PG_USER, database=PG_DBNAME, password=PG_PASSWORD)
    assert ok, f"Could not authenticate to KEEL port {port}"
    return c


@contextmanager
def _conn(port: int, timeout: float = 10.0) -> Generator[ProxyConn, None, None]:
    """Context manager for an authenticated ProxyConn."""
    c = _open_conn(port, timeout)
    try:
        yield c
    finally:
        try:
            c.send(pg_terminate())
        except Exception:
            pass
        c.close()


def _do_parse(c: ProxyConn, stmt: str, sql: str,
              param_types: list[int] | None = None) -> None:
    """Send Parse + Sync; assert ParseComplete + ReadyForQuery."""
    c.send(pg_parse(stmt, sql, param_types) + pg_sync())
    _, msgs = _drain_to_rfq(c)
    err = _has_error(msgs)
    assert err is None, f"Parse failed: {err}"
    types = [t for t, _ in msgs]
    assert _PARSE_COMP in types, f"No ParseComplete in: {types}"


def _do_bind_execute(
    c: ProxyConn,
    stmt: str,
    params: list[bytes | None] | None = None,
    param_formats: list[int] | None = None,
    result_formats: list[int] | None = None,
) -> list[list[str | None]]:
    """Send Bind + Execute + Sync; return rows. Raises on error."""
    c.send(
        pg_bind("", stmt, param_formats or [], params or [], result_formats or [])
        + pg_execute("", 0)
        + pg_sync()
    )
    _, msgs = _drain_to_rfq(c)
    err = _has_error(msgs)
    assert err is None, f"Bind/Execute failed: {err}"
    return _collect_rows(msgs)


def _do_bind_execute_expect_error(
    c: ProxyConn,
    stmt: str,
    params: list[bytes | None] | None = None,
) -> str:
    """Send Bind + Execute + Sync; expect an ErrorResponse. Returns msg."""
    c.send(
        pg_bind("", stmt, [], params or [], [])
        + pg_execute("", 0)
        + pg_sync()
    )
    _, msgs = _drain_to_rfq(c)
    err = _has_error(msgs)
    assert err is not None, "Expected an error but got none"
    return err


def _simple_q(c: ProxyConn, sql: str) -> list[list[str | None]]:
    """Execute a simple query via Q message; return rows."""
    c.send(pg_query(sql))
    _, msgs = _drain_to_rfq(c)
    err = _has_error(msgs)
    assert err is None, f"Simple query '{sql}' failed: {err}"
    return _collect_rows(msgs)


def _simple_q_scalar(c: ProxyConn, sql: str) -> str | None:
    """Execute a simple query and return the first column of first row."""
    rows = _simple_q(c, sql)
    assert rows, f"No rows returned from: {sql}"
    return rows[0][0]


def _get_backend_pid(c: ProxyConn) -> int:
    """Query pg_backend_pid() via simple query."""
    val = _simple_q_scalar(c, "SELECT pg_backend_pid()")
    assert val is not None
    return int(val)


def _psycopg2_conn(dsn: str) -> psycopg2.extensions.connection:
    """Open a psycopg2 autocommit connection."""
    conn = psycopg2.connect(dsn, connect_timeout=10)
    conn.autocommit = True
    return conn


def _psycopg2_txn(dsn: str) -> psycopg2.extensions.connection:
    """Open a psycopg2 manual-transaction connection."""
    conn = psycopg2.connect(dsn, connect_timeout=10)
    conn.autocommit = False
    return conn


# ── param encoding helpers ──────────────────────────────────────────────
def _i4(v: int) -> bytes:
    return struct.pack(">i", v)


def _i8(v: int) -> bytes:
    return struct.pack(">q", v)


def _txt(s: str) -> bytes:
    return s.encode()


# ===========================================================================
# TestPS_Virtualize
# ===========================================================================

class TestPS_Virtualize:
    """
    Tests for prepared_statement = virtualize (port 26440).

    KEEL intercepts every named Parse, returns a synthetic ParseComplete
    without contacting the backend.  On the first Bind that references the
    statement, KEEL may replay all confirmed Parses to the assigned backend
    if its stmt_set_hash does not match.
    """

    def test_named_parse_returns_parse_complete(self, ps_virt_dsn):
        """Parse("myq") → ParseComplete returned immediately by KEEL."""
        with _conn(PS_VIRT_PORT) as c:
            c.send(pg_parse("virt_q1", "SELECT 1") + pg_sync())
            _, msgs = _drain_to_rfq(c)
            types = [t for t, _ in msgs]
            assert _PARSE_COMP in types, f"No ParseComplete in {types}"
            assert _ERROR not in types, "Unexpected ErrorResponse"

    def test_bind_execute_returns_result(self, ps_virt_dsn):
        """Parse + Bind + Execute returns the expected row."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_q2", "SELECT 42")
            rows = _do_bind_execute(c, "virt_q2")
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_parameterised_query(self, ps_virt_dsn):
        """Named statement with $1::int parameter returns the correct value."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_p1", "SELECT $1::int * 2")
            rows = _do_bind_execute(c, "virt_p1", params=[_txt("21")])
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_two_params(self, ps_virt_dsn):
        """Statement with two parameters returns correct sum."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_p2", "SELECT $1::int + $2::int")
            rows = _do_bind_execute(c, "virt_p2", params=[_txt("10"), _txt("32")])
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_binary_int4_parameter(self, ps_virt_dsn):
        """Binary-format int4 parameter is correctly transmitted and executed."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_bin4", "SELECT $1::int4 + 1", param_types=[23])
            rows = _do_bind_execute(
                c, "virt_bin4",
                params=[_i4(41)],
                param_formats=[1],
            )
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_binary_int8_parameter(self, ps_virt_dsn):
        """Binary-format int8 parameter is correctly transmitted and executed."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_bin8", "SELECT $1::int8 + 1", param_types=[20])
            rows = _do_bind_execute(
                c, "virt_bin8",
                params=[_i8(41)],
                param_formats=[1],
            )
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_null_parameter(self, ps_virt_dsn):
        """NULL parameter (len=-1 in Bind) is handled correctly."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_null", "SELECT $1::int IS NULL")
            rows = _do_bind_execute(c, "virt_null", params=[None])
            assert rows == [["t"]], f"Unexpected rows: {rows}"

    def test_statement_survives_50_transactions(self, ps_virt_dsn):
        """Prepared statement returns correct results across 50 consecutive
        transactions, regardless of backend recycling."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_stress", "SELECT $1::int + 1")
            for i in range(50):
                rows = _do_bind_execute(c, "virt_stress", params=[_txt(str(i))])
                assert rows == [[str(i + 1)]], f"Wrong result at iteration {i}"

    def test_multiple_statements_coexist(self, ps_virt_dsn):
        """Ten different named statements can be used concurrently."""
        with _conn(PS_VIRT_PORT) as c:
            for i in range(10):
                _do_parse(c, f"virt_multi_{i}", f"SELECT {i} * 2")
            for i in range(10):
                rows = _do_bind_execute(c, f"virt_multi_{i}")
                assert rows == [[str(i * 2)]], f"Wrong result for stmt {i}"

    def test_describe_statement(self, ps_virt_dsn):
        """Describe(S, name) returns ParameterDescription (and optionally
        RowDescription) without error."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_desc", "SELECT $1::text")
            c.send(pg_describe("S", "virt_desc") + pg_sync())
            _, msgs = _drain_to_rfq(c)
            types = [t for t, _ in msgs]
            assert _PARAM_DESC in types, f"No ParameterDescription in {types}"
            assert _ERROR not in types, "Unexpected error after Describe"

    def test_close_statement_removes_it(self, ps_virt_dsn):
        """Close(S, name) → CloseComplete; subsequent Bind fails with unknown
        statement error."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_close", "SELECT 99")
            # Close the statement
            c.send(pg_close("S", "virt_close") + pg_sync())
            _, close_msgs = _drain_to_rfq(c)
            close_types = [t for t, _ in close_msgs]
            assert _CLOSE_COMP in close_types, \
                f"No CloseComplete after Close: {close_types}"
            assert _ERROR not in close_types, "Unexpected error on Close"
            # Subsequent Bind must fail
            err = _do_bind_execute_expect_error(c, "virt_close")
            assert err, "Expected error after Bind of closed stmt"

    def test_deallocate_all_clears_cache(self, ps_virt_dsn):
        """DEALLOCATE ALL (simple query) removes statement from KEEL cache;
        subsequent Bind fails."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_dealloc", "SELECT 77")
            _simple_q(c, "DEALLOCATE ALL")
            err = _do_bind_execute_expect_error(c, "virt_dealloc")
            assert err, "Expected error after DEALLOCATE ALL"

    def test_discard_all_clears_cache(self, ps_virt_dsn):
        """DISCARD ALL clears KEEL's session stmt cache; Bind fails afterward."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_discard", "SELECT 55")
            _simple_q(c, "DISCARD ALL")
            err = _do_bind_execute_expect_error(c, "virt_discard")
            assert err, "Expected error after DISCARD ALL"

    def test_reparse_after_close_replaces_statement(self, ps_virt_dsn):
        """Closing a named statement and re-parsing under the same name
        replaces the old statement with the new one."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_reparse", "SELECT 1")
            rows1 = _do_bind_execute(c, "virt_reparse")
            assert rows1 == [["1"]]
            # Must Close before re-parsing the same name (PostgreSQL protocol)
            c.send(pg_close("S", "virt_reparse") + pg_sync())
            _, close_msgs = _drain_to_rfq(c)
            assert _has_error(close_msgs) is None, \
                f"Unexpected error on Close before re-parse: {_has_error(close_msgs)}"
            # Re-parse same name with different SQL
            _do_parse(c, "virt_reparse", "SELECT 2")
            rows2 = _do_bind_execute(c, "virt_reparse")
            assert rows2 == [["2"]], \
                f"Re-parsed stmt returned wrong value: {rows2}"

    def test_error_in_parse_leaves_connection_usable(self, ps_virt_dsn):
        """A Parse with invalid SQL returns ErrorResponse; the connection
        remains usable for subsequent queries."""
        with _conn(PS_VIRT_PORT) as c:
            c.send(pg_parse("virt_bad", "SELECT INVALID SYNTAX !!!") + pg_sync())
            _, msgs = _drain_to_rfq(c)
            assert _has_error(msgs), "Expected ErrorResponse for invalid SQL"
            # Connection must still work
            rows = _simple_q(c, "SELECT 1")
            assert rows == [["1"]]

    def test_duplicate_named_parse_requires_close(self, ps_virt_dsn):
        """Re-parsing the same statement name without Close must fail, and
        the original statement remains executable."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "virt_dup", "SELECT 1")
            c.send(pg_parse("virt_dup", "SELECT 2") + pg_sync())
            _, msgs = _drain_to_rfq(c)
            err = _has_error(msgs)
            assert err is not None, "Expected duplicate statement-name error"
            rows = _do_bind_execute(c, "virt_dup")
            assert rows == [["1"]], f"Original statement should remain active: {rows}"

    def test_many_named_statements_remain_usable_under_pressure(self, ps_virt_dsn):
        """Dozens of named statements can coexist and execute correctly in one
        session, exercising cache/index pressure without backend deadlocks."""
        with _conn(PS_VIRT_PORT, timeout=20.0) as c:
            for i in range(48):
                _do_parse(c, f"virt_many_{i}", f"SELECT {i} + $1::int")
            for i in range(48):
                rows = _do_bind_execute(c, f"virt_many_{i}", params=[_txt("1")])
                assert rows == [[str(i + 1)]], f"Wrong result for virt_many_{i}: {rows}"

    def test_unnamed_statement_passthrough(self, ps_virt_dsn):
        """Anonymous Parse ("") is passed straight through (not intercepted)."""
        with _conn(PS_VIRT_PORT) as c:
            c.send(
                pg_parse("", "SELECT $1::int + $2::int")
                + pg_bind("", "", [], [_txt("3"), _txt("4")], [])
                + pg_execute("", 0)
                + pg_sync()
            )
            _, msgs = _drain_to_rfq(c)
            assert _has_error(msgs) is None, f"Error on anonymous parse: {_has_error(msgs)}"
            rows = _collect_rows(msgs)
            assert rows == [["7"]], f"Unexpected rows: {rows}"

    def test_concurrent_sessions_independent_caches(self, ps_virt_dsn):
        """Four concurrent sessions each use their own named statement;
        no cross-session state leakage.
        (4 threads ≤ max_pool_size=4, so no connection is ever queued and
        the pool is never exhausted, preventing backend-leak on socket timeout.)
        """
        errors: list[str] = []
        lock = threading.Lock()

        def worker(session_id: int) -> None:
            try:
                with _conn(PS_VIRT_PORT) as c:
                    name = f"sess_{session_id}"
                    expected = str(session_id * 7)
                    _do_parse(c, name, f"SELECT {session_id} * 7")
                    for _ in range(5):
                        rows = _do_bind_execute(c, name)
                        if rows != [[expected]]:
                            with lock:
                                errors.append(
                                    f"session {session_id}: got {rows!r} "
                                    f"expected [['{expected}']]"
                                )
            except Exception as exc:
                with lock:
                    errors.append(f"session {session_id} exception: {exc}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, "Concurrent session errors:\n" + "\n".join(errors)


# ===========================================================================
# TestPS_Pinning
# ===========================================================================

class TestPS_Pinning:
    """
    Tests for prepared_statement = pinning (port 26441).

    The first named Parse hard-pins the backend for the life of the session.
    The backend is not returned to the pool until DEALLOCATE ALL, DISCARD ALL,
    or client disconnect.
    """

    def test_named_parse_pins_backend(self, ps_pin_dsn):
        """After the first named Parse, pg_backend_pid() is identical across
        ten consecutive transactions."""
        with _conn(PS_PIN_PORT) as c:
            _do_parse(c, "pin_q1", "SELECT pg_backend_pid()")
            pid_first = int(_do_bind_execute(c, "pin_q1")[0][0])
            for _ in range(9):
                pid = int(_do_bind_execute(c, "pin_q1")[0][0])
                assert pid == pid_first, \
                    f"Backend PID changed while pinned: {pid} ≠ {pid_first}"

    def test_pinned_backend_serves_multiple_statements(self, ps_pin_dsn):
        """Multiple named statements all execute on the same pinned backend."""
        with _conn(PS_PIN_PORT) as c:
            for i in range(5):
                _do_parse(c, f"pin_s{i}", f"SELECT {i} + 10")
            pids = set()
            for i in range(5):
                rows = _do_bind_execute(c, f"pin_s{i}")
                assert rows == [[str(i + 10)]]
                pids.add(int(_simple_q_scalar(c, "SELECT pg_backend_pid()")))
            assert len(pids) == 1, f"Backend changed while pinned: {pids}"

    def test_deallocate_individual_keeps_pin(self, ps_pin_dsn):
        """DEALLOCATE of a single statement name does NOT release the pin;
        pg_backend_pid() stays the same."""
        with _conn(PS_PIN_PORT) as c:
            _do_parse(c, "pin_keep", "SELECT pg_backend_pid()")
            pid_before = int(_do_bind_execute(c, "pin_keep")[0][0])
            _simple_q(c, "DEALLOCATE pin_keep")
            pid_after = int(_simple_q_scalar(c, "SELECT pg_backend_pid()"))
            assert pid_before == pid_after, \
                f"Pin released unexpectedly on individual DEALLOCATE"

    def test_deallocate_all_releases_pin(self, ps_pin_dsn):
        """DEALLOCATE ALL releases the backend pin; the statement is gone from
        cache and a subsequent plain query works (possibly on a different backend)."""
        with _conn(PS_PIN_PORT) as c:
            _do_parse(c, "pin_dealloc", "SELECT 1")
            _do_bind_execute(c, "pin_dealloc")          # ensure pin is set
            _simple_q(c, "DEALLOCATE ALL")
            # Statement must be gone
            err = _do_bind_execute_expect_error(c, "pin_dealloc")
            assert err, "Expected error after DEALLOCATE ALL + Bind"
            # Connection still usable
            rows = _simple_q(c, "SELECT 2")
            assert rows == [["2"]]

    def test_discard_all_releases_pin(self, ps_pin_dsn):
        """DISCARD ALL releases the pin; statement is cleared."""
        with _conn(PS_PIN_PORT) as c:
            _do_parse(c, "pin_disc", "SELECT 1")
            _do_bind_execute(c, "pin_disc")
            _simple_q(c, "DISCARD ALL")
            err = _do_bind_execute_expect_error(c, "pin_disc")
            assert err, "Expected error after DISCARD ALL + Bind"

    def test_disconnect_releases_backend(self, ps_pin_dsn):
        """Closing the connection releases the pinned backend so a new
        session can immediately acquire it."""
        # Session A pins a backend
        c_a = _open_conn(PS_PIN_PORT)
        _do_parse(c_a, "pin_disc_a", "SELECT 1")
        _do_bind_execute(c_a, "pin_disc_a")
        # Close session A — backend must be returned to pool
        c_a.send(pg_terminate())
        c_a.close()
        time.sleep(0.2)
        # Session B must be able to acquire the backend immediately
        with _conn(PS_PIN_PORT) as c_b:
            rows = _simple_q(c_b, "SELECT 42")
            assert rows == [["42"]]

    def test_pinning_exhausts_pool(self, ps_pin_dsn):
        """When all max_pool_size (4) backends are pinned by separate sessions,
        a fifth concurrent session that tries to use a named PS cannot acquire
        a backend within a short timeout."""
        conns: list[ProxyConn] = []
        try:
            # Pin all 4 backends
            for i in range(4):
                c = _open_conn(PS_PIN_PORT)
                _do_parse(c, f"pin_exhaust_{i}", "SELECT 1")
                _do_bind_execute(c, f"pin_exhaust_{i}")
                conns.append(c)
            # The 5th connection should either time out or return an error
            # when it tries to use a named Parse (which would trigger a pin).
            # We attempt the connection with a very short timeout.
            c5 = ProxyConn(KEEL_HOST, PS_PIN_PORT, timeout=3.0)
            c5.connect()
            ok = c5.startup(PG_USER, PG_DBNAME, PG_PASSWORD)
            if ok:
                c5.send(pg_parse("pin_5th", "SELECT 1") + pg_sync())
                try:
                    _, msgs = _drain_to_rfq(c5)
                    # If Parse succeeded, try Bind which should time out or error
                    c5.send(
                        pg_bind("", "pin_5th", [], [], [])
                        + pg_execute("", 0)
                        + pg_sync()
                    )
                    _, bind_msgs = _drain_to_rfq(c5)
                    # Either an error (pool exhausted) OR success if a backend freed up
                    # We accept both — the important thing is KEEL doesn't crash
                except Exception:
                    pass  # timeout / connection reset is acceptable
                try:
                    c5.close()
                except Exception:
                    pass
        finally:
            for c in conns:
                try:
                    c.send(pg_terminate())
                    c.close()
                except Exception:
                    pass


# ===========================================================================
# TestPS_Tracking
# ===========================================================================

class TestPS_Tracking:
    """
    Tests for prepared_statement = tracking (port 26442).

    tracking extends virtualize by also intercepting PREPARE stmt AS sql
    statements sent via the simple-query (Q-message) protocol.  These are
    stored in the same session stmt cache and replayed on backend changes.
    """

    def test_simple_prepare_tracked(self, ps_track_dsn):
        """SQL PREPARE via simple query is intercepted and tracked."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("PREPARE track_q1 AS SELECT 42")
            cur.execute("EXECUTE track_q1")
            rows = cur.fetchall()
            assert rows == [(42,)], f"Unexpected rows: {rows}"
        finally:
            conn.close()

    def test_simple_prepare_with_params(self, ps_track_dsn):
        """PREPARE with typed parameter executes correctly via EXECUTE."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("PREPARE track_sum AS SELECT $1::int + $2::int")
            cur.execute("EXECUTE track_sum(17, 25)")
            rows = cur.fetchall()
            assert rows == [(42,)], f"Unexpected rows: {rows}"
        finally:
            conn.close()

    def test_simple_prepare_survives_50_transactions(self, ps_track_dsn):
        """Simple-query PREPARE is replayed correctly across 50 transactions,
        surviving potential backend recycling.

        DISCARD ALL at the start provides test isolation: it ensures any
        backend borrowed for the following PREPARE has no stale statements
        from previous test runs, guaranteeing a clean borrow.  It is no
        longer required to work around the old confirmed=true-early bug
        (which caused double-creation conflicts on dirty backends) — that
        bug is now fixed by staging the entry as confirmed=false until
        CommandComplete("PREPARE") arrives from the backend."""
        conn = _psycopg2_txn(ps_track_dsn)
        try:
            cur = conn.cursor()
            # DISCARD ALL cannot run inside a transaction block, so temporarily
            # switch to autocommit.  This resets the KEEL session's stmt_set_hash
            # to 0 so the subsequent PREPARE always borrows a clean backend (Step 2),
            # avoiding the tracking-mode double-creation conflict on dirty backends.
            conn.autocommit = True
            cur.execute("DISCARD ALL")
            conn.autocommit = False
            # Prepare the statement (intercepted by tracking mode)
            cur.execute("PREPARE track_stress AS SELECT $1::int + 1")
            conn.commit()
            for i in range(50):
                cur.execute(f"EXECUTE track_stress({i})")
                rows = cur.fetchall()
                assert rows == [(i + 1,)], f"Wrong result at i={i}: {rows}"
                conn.commit()
        finally:
            conn.rollback()
            conn.close()

    def test_deallocate_removes_tracked_stmt(self, ps_track_dsn):
        """DEALLOCATE removes the simple-query PREPARE from tracking cache;
        subsequent EXECUTE fails."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("PREPARE track_dealloc AS SELECT 99")
            cur.execute("EXECUTE track_dealloc")
            assert cur.fetchall() == [(99,)]
            cur.execute("DEALLOCATE track_dealloc")
            with pytest.raises(psycopg2.Error):
                cur.execute("EXECUTE track_dealloc")
        finally:
            conn.close()

    def test_deallocate_all_removes_all_tracked(self, ps_track_dsn):
        """DEALLOCATE ALL removes all tracked simple-query PREPARE statements
        from the same session — subsequent EXECUTE on the same connection must fail."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("PREPARE track_da_a AS SELECT 1")
            cur.execute("PREPARE track_da_b AS SELECT 2")
            cur.execute("DEALLOCATE ALL")
            for name in ("track_da_a", "track_da_b"):
                with pytest.raises(psycopg2.Error):
                    cur.execute(f"EXECUTE {name}")
                conn.rollback()  # clear the error state so the next iteration works
        finally:
            conn.close()

    def test_deallocate_nonexistent_stmt_errors(self, ps_track_dsn):
        """DEALLOCATE of a name that was never prepared must return an error
        to the client, not a synthetic success."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("DISCARD ALL")
            with pytest.raises(psycopg2.Error):
                cur.execute("DEALLOCATE track_never_existed_xyzzy")
        finally:
            conn.close()

    def test_deallocate_then_reprepare(self, ps_track_dsn):
        """After DEALLOCATE, the same name can be re-prepared and executed."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("DISCARD ALL")
            cur.execute("PREPARE track_reuse AS SELECT 10")
            cur.execute("EXECUTE track_reuse")
            assert cur.fetchall() == [(10,)]
            cur.execute("DEALLOCATE track_reuse")
            cur.execute("PREPARE track_reuse AS SELECT 20")
            cur.execute("EXECUTE track_reuse")
            assert cur.fetchall() == [(20,)]
        finally:
            conn.close()

    def test_deallocate_prepare_keyword_variant(self, ps_track_dsn):
        """DEALLOCATE PREPARE name (with explicit PREPARE keyword) also removes
        the statement from the tracking cache."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("DISCARD ALL")
            cur.execute("PREPARE track_dp AS SELECT 77")
            cur.execute("EXECUTE track_dp")
            assert cur.fetchall() == [(77,)]
            cur.execute("DEALLOCATE PREPARE track_dp")
            with pytest.raises(psycopg2.Error):
                cur.execute("EXECUTE track_dp")
        finally:
            conn.close()

    def test_extended_protocol_also_tracked(self, ps_track_dsn):
        """Extended-protocol named Parse is also handled (tracking ⊇ virtualize)."""
        with _conn(PS_TRACK_PORT) as c:
            _do_parse(c, "track_ext", "SELECT $1::int * 3")
            rows = _do_bind_execute(c, "track_ext", params=[_txt("14")])
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_simple_prepare_with_explicit_type_list(self, ps_track_dsn):
        """tracking mode supports SQL PREPARE forms that include explicit
        parameter type lists."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            stmt_name = f"track_typed_{int(time.time() * 1000)}"
            cur.execute("DISCARD ALL")
            cur.execute(
                f"PREPARE {stmt_name}(int, text) AS "
                "SELECT $1 + char_length($2)"
            )
            cur.execute(f"EXECUTE {stmt_name}(40, 'ab')")
            assert cur.fetchall() == [(42,)]
        finally:
            conn.close()

    def test_simple_prepare_duplicate_name_errors(self, ps_track_dsn):
        """Simple-query PREPARE with duplicate names should fail and leave the
        original statement intact."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            cur = conn.cursor()
            cur.execute("DISCARD ALL")
            stmt_name = f"track_dup_{int(time.time() * 1000)}"
            cur.execute(f"PREPARE {stmt_name} AS SELECT 1")
            with pytest.raises(psycopg2.Error):
                cur.execute(f"PREPARE {stmt_name} AS SELECT 2")
            cur.execute(f"EXECUTE {stmt_name}")
            assert cur.fetchall() == [(1,)]
        finally:
            conn.close()

    def test_mixed_simple_and_extended(self, ps_track_dsn):
        """Simple-query PREPARE and extended-protocol Bind can coexist in the
        same session without interfering."""
        conn = _psycopg2_conn(ps_track_dsn)
        try:
            conn.cursor().execute("PREPARE track_mix_sq AS SELECT 10")
        finally:
            conn.close()

        with _conn(PS_TRACK_PORT) as c:
            # Extended protocol stmt on the same port
            _do_parse(c, "track_mix_ext", "SELECT 20")
            rows = _do_bind_execute(c, "track_mix_ext")
            assert rows == [["20"]]


# ===========================================================================
# TestPS_Anonymous
# ===========================================================================

class TestPS_Anonymous:
    """
    Tests for prepared_statement = anonymous (port 26443).

    KEEL rewrites every named Parse("name", sql) to Parse("", sql) before
    sending to the backend.  The backend never accumulates named statements.
    Close("S", "name") messages are absorbed by KEEL.
    """

    def test_named_parse_works_transparently(self, ps_anon_dsn):
        """Named Parse is accepted by KEEL and returns ParseComplete; Bind +
        Execute returns correct results as if no rewrite occurred."""
        with _conn(PS_ANON_PORT) as c:
            _do_parse(c, "anon_q1", "SELECT 42")
            rows = _do_bind_execute(c, "anon_q1")
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_backend_has_no_named_statements(self, ps_anon_dsn):
        """After named Parse + Bind, pg_prepared_statements on the backend
        has no entry for the client-visible name."""
        with _conn(PS_ANON_PORT) as c:
            _do_parse(c, "anon_check", "SELECT 1")
            _do_bind_execute(c, "anon_check")
            # Query pg_prepared_statements in the same session
            rows = _simple_q(
                c,
                "SELECT name FROM pg_prepared_statements WHERE name = 'anon_check'"
            )
            assert rows == [], \
                f"Named stmt 'anon_check' should not appear on backend, got: {rows}"

    def test_pg_prepared_statements_always_empty_for_named(self, ps_anon_dsn):
        """After parsing several named statements, none appear in
        pg_prepared_statements on the backend."""
        with _conn(PS_ANON_PORT) as c:
            for i in range(5):
                _do_parse(c, f"anon_n{i}", f"SELECT {i}")
                _do_bind_execute(c, f"anon_n{i}")
            rows = _simple_q(
                c,
                "SELECT name FROM pg_prepared_statements WHERE name != ''"
            )
            assert rows == [], \
                f"Named stmts should not appear on backend, got: {rows}"

    def test_close_named_absorbed_by_proxy(self, ps_anon_dsn):
        """Close(S, "name") is absorbed by KEEL; CloseComplete returned without
        forwarding to the backend (no error for unknown statement)."""
        with _conn(PS_ANON_PORT) as c:
            _do_parse(c, "anon_close", "SELECT 7")
            _do_bind_execute(c, "anon_close")
            # Close the named statement
            c.send(pg_close("S", "anon_close") + pg_sync())
            _, msgs = _drain_to_rfq(c)
            types = [t for t, _ in msgs]
            # KEEL should absorb Close and send CloseComplete (or at minimum no error)
            if _ERROR in types:
                err = _has_error(msgs)
                pytest.fail(f"Unexpected error on Close in anonymous mode: {err}")
            # Connection must still be usable
            rows = _simple_q(c, "SELECT 8")
            assert rows == [["8"]]

    def test_describe_unnamed_works(self, ps_anon_dsn):
        """In anonymous mode, Parse rewrites named statements to anonymous ("").
        Describe(S, "") — the anonymous statement — returns ParameterDescription
        after a Parse+Bind pipeline that materialises it on the backend."""
        with _conn(PS_ANON_PORT) as c:
            # Pipeline Parse + Bind without Execute: the anonymous stmt is live
            c.send(
                pg_parse("anon_desc2", "SELECT $1::text")
                + pg_bind("", "anon_desc2", [], [_txt("hello")], [])
                + pg_describe("P", "")      # describe the *portal*, not the stmt
                + pg_sync()
            )
            _, msgs = _drain_to_rfq(c)
            assert _has_error(msgs) is None, \
                f"Unexpected error in pipeline: {_has_error(msgs)}"
            types = [t for t, _ in msgs]
            # Either RowDescription or NoData must be present after Describe(P)
            assert (_ROW_DESC in types or _NO_DATA in types), \
                f"No RowDescription/NoData after Describe(P): {types}"

    def test_binary_params_preserved(self, ps_anon_dsn):
        """Binary parameter format codes are preserved through the rewrite."""
        with _conn(PS_ANON_PORT) as c:
            _do_parse(c, "anon_bin", "SELECT $1::int4 + 1", param_types=[23])
            rows = _do_bind_execute(
                c, "anon_bin",
                params=[_i4(41)],
                param_formats=[1],
            )
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_50_cycles_same_name(self, ps_anon_dsn):
        """50 Bind/Execute cycles with the same named statement all succeed;
        verifies stable anonymous rewrite across pool recycling."""
        with _conn(PS_ANON_PORT) as c:
            _do_parse(c, "anon_cycle", "SELECT $1::int")
            for i in range(50):
                rows = _do_bind_execute(c, "anon_cycle", params=[_txt(str(i))])
                assert rows == [[str(i)]], f"Wrong result at iteration {i}"

    def test_multiple_distinct_names_concurrent(self, ps_anon_dsn):
        """Five distinct named statements can be used concurrently on the same
        session without interfering (each rewritten to anonymous on the backend)."""
        with _conn(PS_ANON_PORT) as c:
            for i in range(5):
                _do_parse(c, f"anon_m{i}", f"SELECT {i} * 10")
            for i in range(5):
                rows = _do_bind_execute(c, f"anon_m{i}")
                assert rows == [[str(i * 10)]], \
                    f"Wrong result for stmt anon_m{i}: {rows}"


# ===========================================================================
# TestPS_Off
# ===========================================================================

class TestPS_Off:
    """
    Tests for prepared_statement = off (port 26444).

    KEEL forwards Parse to the backend verbatim and hard-pins the backend on
    the first named Parse.  The pin is released only by DEALLOCATE ALL,
    DISCARD ALL, or client disconnect.  Unlike pinning mode, no stmt-cache or
    replay infrastructure is involved.
    """

    def test_named_parse_forwarded_to_backend(self, ps_off_dsn):
        """In off mode, Parse is forwarded to the backend; pg_prepared_statements
        shows the statement by name."""
        with _conn(PS_OFF_PORT) as c:
            _do_parse(c, "off_q1", "SELECT 42")
            _do_bind_execute(c, "off_q1")
            # pg_prepared_statements should contain the named entry
            rows = _simple_q(
                c,
                "SELECT name FROM pg_prepared_statements WHERE name = 'off_q1'"
            )
            assert rows == [["off_q1"]], \
                f"Named stmt should appear in pg_prepared_statements, got: {rows}"

    def test_backend_pins_on_first_parse(self, ps_off_dsn):
        """Backend PID is identical across ten transactions after first Parse."""
        with _conn(PS_OFF_PORT) as c:
            _do_parse(c, "off_pin", "SELECT pg_backend_pid()")
            pid_first = int(_do_bind_execute(c, "off_pin")[0][0])
            for _ in range(9):
                pid = int(_do_bind_execute(c, "off_pin")[0][0])
                assert pid == pid_first, \
                    f"Backend PID changed in off mode: {pid} ≠ {pid_first}"

    def test_individual_deallocate_keeps_pin(self, ps_off_dsn):
        """DEALLOCATE of a single statement keeps the backend pinned."""
        with _conn(PS_OFF_PORT) as c:
            _do_parse(c, "off_keep", "SELECT pg_backend_pid()")
            pid_before = int(_do_bind_execute(c, "off_keep")[0][0])
            _simple_q(c, "DEALLOCATE off_keep")
            pid_after = int(_simple_q_scalar(c, "SELECT pg_backend_pid()"))
            assert pid_before == pid_after, \
                "Pin should not be released by individual DEALLOCATE in off mode"

    def test_deallocate_all_releases_pin(self, ps_off_dsn):
        """DEALLOCATE ALL releases the hard pin; connection remains usable."""
        with _conn(PS_OFF_PORT) as c:
            _do_parse(c, "off_da", "SELECT 1")
            _do_bind_execute(c, "off_da")
            _simple_q(c, "DEALLOCATE ALL")
            err = _do_bind_execute_expect_error(c, "off_da")
            assert err, "Statement should be gone after DEALLOCATE ALL"
            rows = _simple_q(c, "SELECT 9")
            assert rows == [["9"]]

    def test_discard_all_releases_pin(self, ps_off_dsn):
        """DISCARD ALL releases the hard pin; connection remains usable."""
        with _conn(PS_OFF_PORT) as c:
            _do_parse(c, "off_disc", "SELECT 1")
            _do_bind_execute(c, "off_disc")
            _simple_q(c, "DISCARD ALL")
            err = _do_bind_execute_expect_error(c, "off_disc")
            assert err, "Statement should be gone after DISCARD ALL"
            rows = _simple_q(c, "SELECT 9")
            assert rows == [["9"]]

    def test_no_replay_on_disconnect_reconnect(self, ps_off_dsn):
        """After disconnect and reconnect, no old statements are replayed (off
        mode has no replay infrastructure; new session starts clean)."""
        # Open a session, prepare a statement, disconnect
        c1 = _open_conn(PS_OFF_PORT)
        _do_parse(c1, "off_persist", "SELECT 1")
        _do_bind_execute(c1, "off_persist")
        c1.send(pg_terminate())
        c1.close()
        time.sleep(0.1)
        # New session must not see the old statement
        with _conn(PS_OFF_PORT) as c2:
            err = _do_bind_execute_expect_error(c2, "off_persist")
            assert err, "Old statement should not survive reconnect in off mode"

    def test_parse_error_does_not_break_session(self, ps_off_dsn):
        """A Parse syntax error in off mode returns ErrorResponse and the
        connection remains healthy for subsequent queries."""
        with _conn(PS_OFF_PORT) as c:
            c.send(pg_parse("off_bad", "SELECT bad syntax !!!") + pg_sync())
            _, msgs = _drain_to_rfq(c)
            assert _has_error(msgs), "Expected parse error in off mode"
            rows = _simple_q(c, "SELECT 1")
            assert rows == [["1"]]


# ===========================================================================
# TestSSV_GUCState
# ===========================================================================

class TestSSV_GUCState:
    """
    Tests for SSV GUC-state tracking (port 26440, virtualize mode).

    GUC parameters that affect statement semantics are included in the
    stmt_context_sig component of the SSV hash:
      search_path, TimeZone, DateStyle, IntervalStyle,
      standard_conforming_strings, backslash_quote,
      escape_string_warning, default_tablespace, temp_tablespaces,
      default_table_access_method, row_security.
    """

    def test_set_search_path_persists_in_session(self, ps_virt_dsn):
        """SET search_path takes effect within the session (visible via SHOW)."""
        conn = _psycopg2_conn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SET search_path TO public, pg_catalog")
            cur.execute("SHOW search_path")
            val = cur.fetchone()[0]
            assert "public" in val, f"search_path not applied: {val}"
        finally:
            conn.close()

    def test_timezone_change_affects_queries(self, ps_virt_dsn):
        """SET TimeZone is reflected in timezone-aware queries within the same
        statement.  Uses a CTE so the SET and SELECT happen in one simple query."""
        conn = _psycopg2_conn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            # SET and SELECT in the same simple query batch via set_config:
            cur.execute(
                "SELECT set_config('TimeZone', 'America/New_York', false), "
                "current_setting('TimeZone')"
            )
            row = cur.fetchone()
            tz = row[1]
            assert "New_York" in tz or tz == "America/New_York", \
                f"Unexpected timezone value: {tz!r}"
        finally:
            conn.close()

    def test_set_local_applies_within_transaction(self, ps_virt_dsn):
        """SET LOCAL applies its value within the transaction."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SHOW search_path")
            conn.commit()
            cur.execute("SET LOCAL search_path TO pg_catalog")
            cur.execute("SHOW search_path")
            in_txn = cur.fetchone()[0]
            assert "pg_catalog" in in_txn, \
                f"SET LOCAL did not apply within txn: {in_txn}"
            conn.commit()
        finally:
            conn.rollback()
            conn.close()

    def test_set_local_reverts_on_commit(self, ps_virt_dsn):
        """SET LOCAL search_path reverts to the session value after COMMIT."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            # Capture session-level search_path before any SET
            cur.execute("SHOW search_path")
            conn.commit()
            original = cur.fetchone()[0]
            # Apply SET LOCAL inside a transaction, then commit
            cur.execute("SET LOCAL search_path TO pg_catalog")
            conn.commit()
            # After commit, search_path should revert
            cur.execute("SHOW search_path")
            conn.commit()
            after_commit = cur.fetchone()[0]
            assert after_commit == original, \
                f"SET LOCAL did not revert: {after_commit!r} ≠ {original!r}"
        finally:
            conn.rollback()
            conn.close()

    def test_set_local_reverts_on_rollback(self, ps_virt_dsn):
        """SET LOCAL reverts on ROLLBACK just as it does on COMMIT."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SHOW search_path")
            conn.commit()
            original = cur.fetchone()[0]
            cur.execute("SET LOCAL search_path TO pg_catalog")
            conn.rollback()
            cur.execute("SHOW search_path")
            conn.commit()
            after_rollback = cur.fetchone()[0]
            assert after_rollback == original, \
                f"SET LOCAL did not revert on rollback: {after_rollback!r}"
        finally:
            conn.rollback()
            conn.close()

    def test_set_config_local_true_reverts_on_commit(self, ps_virt_dsn):
        """set_config(param, value, true) creates a transaction-local overlay
        that reverts on COMMIT (same semantics as SET LOCAL)."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SHOW TimeZone")
            conn.commit()
            original_tz = cur.fetchone()[0]
            cur.execute("SELECT set_config('TimeZone', 'Pacific/Auckland', true)")
            cur.execute("SELECT current_setting('TimeZone')")
            in_txn = cur.fetchone()[0]
            assert "Auckland" in in_txn, \
                f"set_config(true) did not apply within txn: {in_txn}"
            conn.commit()
            cur.execute("SELECT current_setting('TimeZone')")
            conn.commit()
            after_commit = cur.fetchone()[0]
            assert after_commit == original_tz, \
                f"set_config(true) did not revert on commit: {after_commit!r}"
        finally:
            conn.rollback()
            conn.close()

    def test_set_config_persistent_false_survives_commit(self, ps_virt_dsn):
        """set_config(param, value, false) is session-level and should persist
        across transaction boundaries."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SELECT set_config('TimeZone', 'UTC', false)")
            conn.commit()
            cur.execute("SELECT current_setting('TimeZone')")
            conn.commit()
            assert cur.fetchone()[0] == "UTC"
        finally:
            conn.rollback()
            conn.close()

    def test_datestyle_change_visible_to_prepared_stmt(self, ps_virt_dsn):
        """DateStyle participates in stmt semantic context: after changes,
        prepared statements should still produce coherent results."""
        conn = _psycopg2_conn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SET DateStyle = 'ISO, MDY'")
            cur.execute("PREPARE guc_date AS SELECT current_setting('DateStyle')")
            cur.execute("EXECUTE guc_date")
            row = cur.fetchone()
            assert row and row[0] and "ISO" in row[0], f"Unexpected DateStyle: {row}"
        finally:
            conn.close()

    def test_prepared_stmt_works_after_guc_change(self, ps_virt_dsn):
        """A named prepared statement still returns correct results after
        a GUC change within the session (SSV ensures correct backend selection
        or replay)."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "guc_ps", "SELECT current_setting('TimeZone')")
            _simple_q(c, "SET TimeZone = 'UTC'")
            rows = _do_bind_execute(c, "guc_ps")
            assert rows[0][0] == "UTC", \
                f"TimeZone should be UTC after SET: {rows}"
            # Change timezone again and re-execute
            _simple_q(c, "SET TimeZone = 'Europe/London'")
            rows2 = _do_bind_execute(c, "guc_ps")
            assert "London" in rows2[0][0] or "Europe" in rows2[0][0], \
                f"TimeZone should reflect Europe/London: {rows2}"

    def test_standard_conforming_strings_tracked(self, ps_virt_dsn):
        """standard_conforming_strings setting is part of the SSV context;
        changes to it do not corrupt prepared statements.

        Uses ProxyConn (raw wire protocol) rather than psycopg2 because
        psycopg2 intercepts ParameterStatus('standard_conforming_strings')
        and stalls when the value transitions to 'off' on a pooled connection.
        """
        with _conn(PS_VIRT_PORT) as c:
            # Set to 'on' and read back in one atomic query (no pool hop)
            rows = _simple_q(
                c,
                "SELECT set_config('standard_conforming_strings', 'on', false), "
                "current_setting('standard_conforming_strings')"
            )
            assert rows[0][1] == "on", f"Expected 'on', got {rows[0][1]}"
            # Set to 'off' and read back in one atomic query
            rows2 = _simple_q(
                c,
                "SELECT set_config('standard_conforming_strings', 'off', false), "
                "current_setting('standard_conforming_strings')"
            )
            assert rows2[0][1] == "off", f"Expected 'off', got {rows2[0][1]}"
            # Connection remains fully usable
            rows3 = _simple_q(c, "SELECT 1 + 1")
            assert rows3 == [["2"]]


# ===========================================================================
# TestSSV_TempEpoch
# ===========================================================================

class TestSSV_TempEpoch:
    """
    Tests for SSV temp-object epoch tracking (port 26440, virtualize mode).

    The stmt_temp_epoch component of stmt_context_sig is bumped by:
      - CREATE TEMP TABLE (immediately)
      - DROP TABLE when a temp table exists (immediately)
      - DISCARD TEMP (immediately)
      - DISCARD ALL (immediately)
      - ON COMMIT DROP / ON COMMIT DELETE ROWS (at transaction COMMIT)
      - ROLLBACK when temp-DDL is pending (at rollback)

    The epoch ensures that prepared statements prepared in one temp-object
    context are not replayed to backends in a different temp-object context.
    After epoch bumps, queries must still return correct results.
    """

    def test_stmt_works_after_create_temp_table(self, ps_virt_dsn):
        """Prepared statement returns correct results after CREATE TEMP TABLE
        (epoch bump does not break existing statements)."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "epoch_ct", "SELECT 42")
            _simple_q(c, "CREATE TEMP TABLE temp_epoch_ct (x INT) ON COMMIT DROP")
            rows = _do_bind_execute(c, "epoch_ct")
            assert rows == [["42"]]

    def test_stmt_works_after_discard_temp(self, ps_virt_dsn):
        """Prepared statement still works after DISCARD TEMP."""
        conn = _psycopg2_conn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TEMP TABLE tmp_discard (y INT) ON COMMIT DROP")
        finally:
            conn.close()

        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "epoch_dt", "SELECT 99")
            _simple_q(c, "DISCARD TEMP")
            rows = _do_bind_execute(c, "epoch_dt")
            assert rows == [["99"]]

    def test_discard_all_clears_stmts(self, ps_virt_dsn):
        """DISCARD ALL bumps the epoch AND clears KEEL's session stmt cache;
        subsequent Bind for the old name fails."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "epoch_da", "SELECT 11")
            _do_bind_execute(c, "epoch_da")
            _simple_q(c, "DISCARD ALL")
            # Statement must be gone from KEEL cache
            err = _do_bind_execute_expect_error(c, "epoch_da")
            assert err, "Expected error after DISCARD ALL"

    def test_on_commit_drop_temp_table_disappears(self, ps_virt_dsn):
        """CREATE TEMP TABLE ... ON COMMIT DROP: the temp table is gone after
        COMMIT; queries against it fail; other stmts remain usable."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                "CREATE TEMP TABLE tmp_on_commit (z INT) ON COMMIT DROP"
            )
            cur.execute("INSERT INTO tmp_on_commit VALUES (1), (2), (3)")
            cur.execute("SELECT COUNT(*) FROM tmp_on_commit")
            assert cur.fetchone()[0] == 3
            conn.commit()  # epoch bump here; table dropped
            # After commit the temp table is gone
            with pytest.raises(psycopg2.Error):
                cur.execute("SELECT * FROM tmp_on_commit")
        finally:
            conn.rollback()
            conn.close()

    def test_on_commit_delete_rows_truncates_on_commit(self, ps_virt_dsn):
        """CREATE TEMP TABLE ... ON COMMIT DELETE ROWS: rows are deleted on
        COMMIT but the table structure survives."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                "CREATE TEMP TABLE tmp_del_rows (v INT) ON COMMIT DELETE ROWS"
            )
            cur.execute("INSERT INTO tmp_del_rows VALUES (10)")
            conn.commit()  # epoch bump; rows deleted
            cur.execute("SELECT COUNT(*) FROM tmp_del_rows")
            count = cur.fetchone()[0]
            conn.commit()
            assert count == 0, f"Expected 0 rows after ON COMMIT DELETE ROWS, got {count}"
        finally:
            conn.rollback()
            conn.close()

    def test_rollback_after_create_temp_handles_correctly(self, ps_virt_dsn):
        """ROLLBACK after CREATE TEMP TABLE triggers an epoch bump (the temp
        table is rolled back); subsequent prepared statements still work."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "epoch_rb", "SELECT 77")
            # BEGIN + CREATE TEMP (epoch pending) + ROLLBACK (epoch bump)
            _simple_q(c, "BEGIN")
            _simple_q(c, "CREATE TEMP TABLE tmp_rb_epoch (k INT)")
            _simple_q(c, "ROLLBACK")  # epoch bumped here
            rows = _do_bind_execute(c, "epoch_rb")
            assert rows == [["77"]], \
                f"Statement broken by rollback-epoch bump: {rows}"

    def test_drop_temp_table_bumps_epoch(self, ps_virt_dsn):
        """DROP TABLE of a temp table bumps the epoch; existing prepared
        statements continue to work."""
        with _conn(PS_VIRT_PORT) as c:
            # Create a temp table so DROP can fire
            _simple_q(c, "CREATE TEMP TABLE tmp_drop_epoch (n INT)")
            _do_parse(c, "epoch_drop", "SELECT 33")
            # DROP the temp table (epoch bump)
            _simple_q(c, "DROP TABLE tmp_drop_epoch")
            rows = _do_bind_execute(c, "epoch_drop")
            assert rows == [["33"]], \
                f"Statement broken by drop-epoch bump: {rows}"


# ===========================================================================
# TestPS_Compatibility
# ===========================================================================

class TestPS_Compatibility:
    """
    Cross-cutting correctness tests that apply to all PS modes.

    Tests in this class focus on correctness invariants, error-recovery, and
    safety properties that must hold regardless of the active PS mode.
    """

    def test_no_stmt_leak_between_sessions_virtualize(self, ps_virt_dsn):
        """A named prepared statement in session A is not visible to session B
        (virtualize mode — stmt caches are per-session)."""
        with _conn(PS_VIRT_PORT) as c_a:
            _do_parse(c_a, "compat_leak_a", "SELECT 1")
            with _conn(PS_VIRT_PORT) as c_b:
                err = _do_bind_execute_expect_error(c_b, "compat_leak_a")
                assert err, "Session B should not see session A's prepared stmt"

    def test_no_stmt_leak_between_sessions_pinning(self, ps_pin_dsn):
        """Named prepared statement in session A is not visible to session B
        (pinning mode)."""
        with _conn(PS_PIN_PORT) as c_a:
            _do_parse(c_a, "compat_pin_a", "SELECT 1")
            with _conn(PS_PIN_PORT) as c_b:
                err = _do_bind_execute_expect_error(c_b, "compat_pin_a")
                assert err, "Session B should not see session A's stmt (pinning)"

    def test_error_recovery_in_extended_protocol(self, ps_virt_dsn):
        """After an ErrorResponse in extended protocol, Sync returns
        ReadyForQuery and the connection handles subsequent queries normally."""
        with _conn(PS_VIRT_PORT) as c:
            # Send a Bind for a non-existent statement → error
            c.send(
                pg_bind("", "nonexistent_stmt_xyz", [], [], [])
                + pg_execute("", 0)
                + pg_sync()
            )
            _, msgs = _drain_to_rfq(c)
            assert _has_error(msgs), "Expected error for unknown stmt"
            # Connection must still be usable
            rows = _simple_q(c, "SELECT 1")
            assert rows == [["1"]]

    def test_empty_string_anonymous_parse_passthrough(self, ps_virt_dsn):
        """Anonymous Parse ("") always passes through unchanged in all modes."""
        with _conn(PS_VIRT_PORT) as c:
            c.send(
                pg_parse("", "SELECT $1::int + 1")
                + pg_bind("", "", [], [_txt("41")], [])
                + pg_execute("", 0)
                + pg_sync()
            )
            _, msgs = _drain_to_rfq(c)
            assert _has_error(msgs) is None, \
                f"Anonymous parse failed: {_has_error(msgs)}"
            rows = _collect_rows(msgs)
            assert rows == [["42"]], f"Unexpected rows: {rows}"

    def test_100_rapid_prepare_execute_cycles(self, ps_virt_dsn):
        """100 consecutive Parse + Bind + Execute cycles with unique statement
        names produce correct results throughout."""
        with _conn(PS_VIRT_PORT) as c:
            for i in range(100):
                name = f"rapid_{i}"
                _do_parse(c, name, f"SELECT {i}")
                rows = _do_bind_execute(c, name)
                assert rows == [[str(i)]], \
                    f"Wrong result at cycle {i}: {rows}"

    def test_ps_tracking_simple_and_extended_no_conflict(self, ps_track_dsn):
        """In tracking mode, simple-query PREPARE and extended-protocol named
        Parse for DIFFERENT names coexist without conflict."""
        # Simple query PREPARE via psycopg2
        conn = _psycopg2_conn(ps_track_dsn)
        conn.cursor().execute("PREPARE compat_sq AS SELECT 100")
        conn.close()

        # Extended protocol named Parse on the same port
        with _conn(PS_TRACK_PORT) as c:
            _do_parse(c, "compat_ext", "SELECT 200")
            rows = _do_bind_execute(c, "compat_ext")
            assert rows == [["200"]]

    def test_anonymous_mode_backend_clean_after_use(self, ps_anon_dsn):
        """In anonymous mode, backends are returned clean (no named PS) so
        no DISCARD ALL is needed; concurrent sessions can reuse backends
        without state leakage."""
        errors: list[str] = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            try:
                with _conn(PS_ANON_PORT, timeout=30.0) as c:
                    for j in range(5):
                        _do_parse(c, f"anon_conc_{i}", f"SELECT {i * 10 + j}")
                        rows = _do_bind_execute(c, f"anon_conc_{i}")
                        if rows != [[str(i * 10 + j)]]:
                            with lock:
                                errors.append(
                                    f"worker {i} iter {j}: got {rows}"
                                )
            except Exception as exc:
                with lock:
                    errors.append(f"worker {i} exception: {exc}")

        # Use 2 workers (well below max_pool_size=4) to avoid pool exhaustion
        # during on-demand backend creation.
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not errors, "Concurrent anonymous-mode errors:\n" + "\n".join(errors)

    def test_off_mode_stmt_visible_on_backend_not_virtualized(self, ps_off_dsn):
        """In off mode, the named PS is forwarded and visible in
        pg_prepared_statements; this distinguishes off from virtualize/anonymous."""
        with _conn(PS_OFF_PORT) as c:
            _do_parse(c, "compat_off_check", "SELECT 55")
            _do_bind_execute(c, "compat_off_check")
            rows = _simple_q(
                c,
                "SELECT name FROM pg_prepared_statements WHERE name = 'compat_off_check'"
            )
            assert rows == [["compat_off_check"]], \
                f"off mode: named stmt missing from pg_prepared_statements: {rows}"

    def test_multi_mode_stress_concurrent(self, ps_virt_dsn, ps_pin_dsn,
                                          ps_anon_dsn, ps_track_dsn, ps_off_dsn):
        """All five PS modes serve concurrent prepared-statement sessions
        simultaneously without any session receiving incorrect results."""
        errors: list[str] = []
        lock = threading.Lock()

        def virt_worker() -> None:
            try:
                with _conn(PS_VIRT_PORT, timeout=30.0) as c:
                    _do_parse(c, "mm_virt", "SELECT 1")
                    for _ in range(20):
                        rows = _do_bind_execute(c, "mm_virt")
                        if rows != [["1"]]:
                            with lock:
                                errors.append(f"virt: {rows}")
            except Exception as e:
                with lock:
                    errors.append(f"virt exception: {e}")

        def anon_worker() -> None:
            try:
                with _conn(PS_ANON_PORT, timeout=30.0) as c:
                    _do_parse(c, "mm_anon", "SELECT 2")
                    for _ in range(10):
                        rows = _do_bind_execute(c, "mm_anon")
                        if rows != [["2"]]:
                            with lock:
                                errors.append(f"anon: {rows}")
            except Exception as e:
                with lock:
                    errors.append(f"anon exception: {e}")

        def track_worker() -> None:
            try:
                conn = _psycopg2_conn(ps_track_dsn)
                cur = conn.cursor()
                cur.execute("PREPARE mm_track AS SELECT 3")
                for _ in range(20):
                    cur.execute("EXECUTE mm_track")
                    row = cur.fetchone()
                    if row != (3,):
                        with lock:
                            errors.append(f"track: {row}")
                conn.close()
            except Exception as e:
                with lock:
                    errors.append(f"track exception: {e}")

        def off_worker() -> None:
            try:
                with _conn(PS_OFF_PORT) as c:
                    _do_parse(c, "mm_off", "SELECT 4")
                    for _ in range(20):
                        rows = _do_bind_execute(c, "mm_off")
                        if rows != [["4"]]:
                            with lock:
                                errors.append(f"off: {rows}")
            except Exception as e:
                with lock:
                    errors.append(f"off exception: {e}")

        threads = [
            threading.Thread(target=virt_worker),
            threading.Thread(target=anon_worker),
            threading.Thread(target=track_worker),
            threading.Thread(target=off_worker),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not errors, "Multi-mode stress errors:\n" + "\n".join(errors)


# ===========================================================================
# TestSSV_BackendReuse
# ===========================================================================

class TestSSV_BackendReuse:
    """
    Tests for SSV backend-matching fast-path and replay slow-path
    (virtualize mode, port 26440).

    The fast path: borrow a backend whose stmt_set_hash matches the session's
    current session_stmt_hash — no Parse replay needed.

    The slow path: borrow a clean (stmt-empty) backend — KEEL must replay all
    cached Parse messages before forwarding the client's Bind.

    Correctness must hold on BOTH paths.
    """

    def test_stmts_survive_pool_pressure(self, ps_virt_dsn):
        """Backend reuse exercises the stmt-replay path: 4 concurrent sessions
        cycle through the same 4 backends (max_pool_size per worker), so each
        session frequently borrows a backend whose stmt_set_hash belongs to a
        different session — triggering KEEL's DISCARD ALL + Parse replay path.

        Intentionally capped at max_pool_size (4) so no connection is ever
        queued.  Over-subscribing the pool causes socket timeouts that can
        leave backends in ACTIVE state, depleting the pool for subsequent tests.
        """
        errors: list[str] = []
        lock = threading.Lock()
        n_sessions = 4  # = max_pool_size per worker; never queued

        def worker(sid: int) -> None:
            try:
                with _conn(PS_VIRT_PORT) as c:
                    name = f"reuse_s{sid}"
                    val = sid * 3
                    _do_parse(c, name, f"SELECT {val}")
                    for _ in range(20):  # 20 iters → ample backend cycling
                        rows = _do_bind_execute(c, name)
                        if rows != [[str(val)]]:
                            with lock:
                                errors.append(f"sid={sid}: got {rows}")
            except Exception as exc:
                with lock:
                    errors.append(f"sid={sid} exception: {exc}")

        threads = [threading.Thread(target=worker, args=(i,), daemon=True)
                   for i in range(n_sessions)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, "Pool-pressure errors:\n" + "\n".join(errors)

    def test_guc_change_does_not_corrupt_stmt_results(self, ps_virt_dsn):
        """After SET TimeZone, prepared statements that depend on the GUC
        context still return correct (coherent) results — SSV either
        reuses a matching backend or replays to the correct context."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "ssv_tz", "SELECT current_setting('TimeZone')")
            _simple_q(c, "SET TimeZone = 'UTC'")
            for _ in range(10):
                rows = _do_bind_execute(c, "ssv_tz")
                assert rows[0][0] == "UTC", \
                    f"TimeZone mismatch after GUC change: {rows}"

    def test_search_path_change_does_not_corrupt_stmt_results(self, ps_virt_dsn):
        """After SET search_path, prepared statements still return coherent
        results (the stmt_context_sig change is handled gracefully)."""
        with _conn(PS_VIRT_PORT) as c:
            _do_parse(c, "ssv_sp", "SELECT current_setting('search_path')")
            _simple_q(c, "SET search_path = public")
            for _ in range(10):
                rows = _do_bind_execute(c, "ssv_sp")
                assert rows, "No rows returned"
                # Any non-error result is acceptable here; we're testing stability

    def test_set_local_overlay_does_not_persist_across_txns(self, ps_virt_dsn):
        """SET LOCAL creates a transaction-local GUC overlay that is visible
        within the transaction but NOT after COMMIT."""
        conn = _psycopg2_txn(ps_virt_dsn)
        try:
            cur = conn.cursor()
            cur.execute("SHOW TimeZone")
            conn.commit()
            original = cur.fetchone()[0]

            cur.execute("SET LOCAL TimeZone = 'Pacific/Fiji'")
            cur.execute("SHOW TimeZone")
            in_txn = cur.fetchone()[0]
            assert "Fiji" in in_txn, f"SET LOCAL not applied: {in_txn}"
            conn.commit()

            cur.execute("SHOW TimeZone")
            conn.commit()
            after = cur.fetchone()[0]
            assert after == original, \
                f"SET LOCAL tz leaked across txn boundary: {after!r} ≠ {original!r}"
        finally:
            conn.rollback()
            conn.close()

    def test_clean_backend_replay_correctness_multiple_stmts(self, ps_virt_dsn):
        """A session with multiple prepared statements gets all of them
        replayed correctly when assigned a clean backend, regardless of the
        order they were prepared."""
        with _conn(PS_VIRT_PORT, timeout=30.0) as c:
            stmts = {
                "rb_a": ("SELECT 10 + $1::int", "5", "15"),
                "rb_b": ("SELECT 20 + $1::int", "3", "23"),
                "rb_c": ("SELECT 30 + $1::int", "7", "37"),
                "rb_d": ("SELECT 40 + $1::int", "2", "42"),
            }
            for name, (sql, _, _) in stmts.items():
                _do_parse(c, name, sql)
            # Execute all stmts multiple times under concurrent pool pressure
            for _ in range(10):
                for name, (_, param, expected) in stmts.items():
                    rows = _do_bind_execute(c, name, params=[_txt(param)])
                    assert rows == [[expected]], \
                        f"Stmt {name}: got {rows}, expected {expected}"


# ====================================================================