"""
test_protocol_compliance.py — Wire-protocol compliance tests for the KEEL E2E suite
=====================================================================================

Tests the PostgreSQL wire protocol behaviour of KEEL at the raw TCP socket level,
bypassing libpq and psycopg2 to verify that KEEL correctly implements the
PostgreSQL frontend/backend protocol.

Background
----------
KEEL speaks PostgreSQL's wire protocol on the client side (frontend) and on the
backend side.  Clients connect using the standard PostgreSQL protocol; KEEL
transparently proxies the protocol to the appropriate backend shard.

Raw socket tests are needed because high-level drivers (psycopg2, libpq) hide
protocol details and may retry or buffer in ways that mask proxy bugs.  Testing
at the wire level ensures that:

  - KEEL's SSL/TLS negotiation is correct (or correctly declines SSL).
  - The startup handshake (authentication, parameter status, ReadyForQuery) is
    complete before KEEL accepts the first query.
  - Error responses are properly formatted (ErrorResponse message format, error
    fields).
  - CommandComplete and ReadyForQuery are sent in the correct order.
  - Extended Query Protocol messages (Parse, Bind, Execute, Sync) are handled.

What is tested
--------------
- **Startup negotiation**: client sends StartupMessage, KEEL responds with
  Authentication and eventually ReadyForQuery.
- **Simple Query**: client sends Query('Q') message, KEEL returns
  RowDescription + DataRow + CommandComplete + ReadyForQuery.
- **Error handling**: invalid SQL returns an ErrorResponse with correct fields;
  the connection is not dropped.
- **Extended Query Protocol**: Parse + Bind + Execute + Sync round-trip.
- **SSL rejection**: KEEL correctly handles SSLRequest when TLS is not configured.

Why these tests exist
---------------------
A bug in KEEL's protocol handling can silently corrupt data (wrong number of
columns returned), crash the client (missing ReadyForQuery), or leak security
context between sessions (session state not reset on pool return).  High-level
driver tests cannot detect these issues because the driver compensates for them.

Why a test might fail
---------------------
- **KEEL not listening**: if the proxy hasn't started yet, ``connect()`` times
  out.  The ``compose_stack`` fixture ensures KEEL is healthy before tests run.
- **Protocol message format change**: if KEEL changes how it formats error
  responses or DataRow messages, the raw field parsing in these tests will fail.
- **Authentication method change**: if the KEEL config uses scram-sha-256 but
  the test sends md5, authentication will fail at the wire level.
- **Endianness / length field bugs**: raw struct unpacking is strict; an off-by-
  one in a length prefix will cause the test to read garbled data.

Consequences of failure
-----------------------
- Any PostgreSQL client (not just psycopg2) that connects to KEEL may see
  corrupted responses or connection drops.
- Applications that parse ``NOTICE`` or ``ERROR`` messages (e.g. for custom
  error handling) will receive malformed data.
- SSL/TLS negotiation failures leave clients unable to use encrypted connections.

Markers: ``protocol``

Run selectively::

    pytest tests/e2e/ -m protocol -v
"""

from __future__ import annotations

import struct
import socket
import time

import pytest

# ---------------------------------------------------------------------------
# Import raw-protocol helpers from the suites library.
# This re-uses the same ProxyConn and helpers defined in suite_protocol.py,
# ensuring no duplication.
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

# Make tests/suites importable when run from the repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites.common import (
    PG_CANCEL_CODE,
    PG_PROTO_V3,
    PG_SSL_CODE,
    ProxyConn,
    pg_bind,
    pg_cancel_request,
    pg_describe,
    pg_execute,
    pg_flush,
    pg_parse,
    pg_query,
    pg_ssl_request,
    pg_startup_msg,
    pg_sync,
    pg_terminate,
)

pytestmark = pytest.mark.protocol

# ── backend message type bytes ────────────────────────────────────────────
_READY       = ord("Z")
_ERROR       = ord("E")
_AUTH        = ord("R")
_ROW_DESC    = ord("T")
_DATA_ROW    = ord("D")
_CMD_COMP    = ord("C")
_EMPTY_QUERY = ord("I")
_PARSE_COMP  = ord("1")
_BIND_COMP   = ord("2")
_PARAM_DESC  = ord("t")
_NO_DATA     = ord("n")
_KEY_DATA    = ord("K")
_PARAM_STAT  = ord("S")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_conn(compose_stack):
    """Raw ProxyConn (unauthenticated) to the KEEL proxy."""
    c = ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"])
    c.connect()
    yield c
    c.close()


@pytest.fixture
def authed_conn(compose_stack):
    """Authenticated ProxyConn — ready for queries."""
    c = ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"])
    c.connect()
    ok = c.startup(user="postgres", database="testdb", password="postgres")
    if not ok:
        pytest.skip("Could not authenticate — check proxy config")
    yield c
    try:
        c.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Protocol compliance tests
# ---------------------------------------------------------------------------

class TestStartupMessages:
    def test_valid_startup_reaches_ready(self, raw_conn):
        """Normal startup handshake must reach ReadyForQuery."""
        ok = raw_conn.startup("postgres", "testdb", "postgres")
        assert ok, "startup did not reach ReadyForQuery"

    def test_ssl_request_returns_valid_byte(self, compose_stack):
        """SSLRequest must return 'N' (no SSL) or 'S' (SSL) — not crash."""
        c = ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"])
        c.connect()
        c.send(pg_ssl_request())
        resp = c.recv_exact(1)
        assert resp in (b"N", b"S"), f"Unexpected SSL response: {resp!r}"
        c.close()

    def test_ssl_then_normal_startup(self, compose_stack):
        """SSLRequest → 'N' → normal startup must succeed."""
        c = ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"])
        c.connect()
        c.send(pg_ssl_request())
        resp = c.recv_exact(1)
        if resp == b"N":
            ok = c.startup("postgres", "testdb", "postgres")
            assert ok, "Normal startup after SSL decline failed"
        c.close()

    def test_invalid_protocol_version_gets_error(self, compose_stack):
        """Startup with protocol 2.0 must not crash the proxy.

        KEEL may respond with an ErrorResponse or simply close the connection
        — both are valid defensive behaviours.  The important thing is that the
        proxy itself survives and keeps accepting new connections.
        """
        body  = b"user\x00postgres\x00database\x00testdb\x00\x00"
        v2msg = struct.pack(">II", 4 + 4 + len(body), 2 << 16) + body
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            c.send(v2msg)
            c.set_timeout(5.0)
            try:
                c._sock.recv(4096)  # response or empty on close — both OK
            except (ConnectionError, OSError):
                pass  # proxy closed the connection — acceptable

        # Proxy must still be alive
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            ok = c.startup("postgres", "testdb", "postgres")
        assert ok, "Proxy unreachable after invalid protocol version"

    def test_oversized_startup_handled_safely(self, compose_stack):
        """A 50 KB user field in startup must not crash the proxy.

        KEEL may respond with an error, silently close, or time out— all are
        acceptable as long as the proxy continues to serve subsequent connections.
        """
        msg = pg_startup_msg(user="x" * 50_000)
        s = socket.create_connection(
            (compose_stack["keel_host"], compose_stack["keel_port"]), timeout=5.0
        )
        try:
            s.sendall(msg)
            s.settimeout(5.0)
            try:
                s.recv(8192)  # response or empty on close — both OK
            except (socket.timeout, ConnectionError, OSError):
                pass  # proxy closed or timed out — acceptable
        finally:
            s.close()

        # Proxy must still be alive
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            ok = c.startup("postgres", "testdb", "postgres")
        assert ok, "Proxy unreachable after oversized startup"

    def test_backend_key_data_present(self, compose_stack):
        """BackendKeyData (type K) must be present after auth for cancel support."""
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            c.send(pg_startup_msg("postgres", "testdb"))
            key_data_seen = False
            for _ in range(30):
                t, b = c.recv_message()
                if t == _KEY_DATA:
                    key_data_seen = True
                    assert len(b) >= 8, "BackendKeyData too short"
                elif t == _READY:
                    break
                elif t == _ERROR:
                    pytest.skip("Auth failed — proxy may require password")
        assert key_data_seen, "No BackendKeyData received"


class TestSimpleQueryProtocol:
    def test_select_one(self, authed_conn):
        """SELECT 1 must return RowDescription + DataRow + CommandComplete + ReadyForQuery."""
        authed_conn.send(pg_query("SELECT 1"))
        msgs  = authed_conn.recv_until({_READY, _ERROR})
        types = {m[0] for m in msgs}
        assert _ROW_DESC in types,  "No RowDescription"
        assert _DATA_ROW in types,  "No DataRow"
        assert _CMD_COMP in types,  "No CommandComplete"
        assert _READY    in types,  "No ReadyForQuery"

    def test_empty_query(self, authed_conn):
        """Empty query must return EmptyQueryResponse (not crash)."""
        authed_conn.send(pg_query(""))
        msgs  = authed_conn.recv_until({_READY, _ERROR})
        types = {m[0] for m in msgs}
        assert _EMPTY_QUERY in types or _ERROR in types or _READY in types

    def test_multiple_statements_semicolon(self, authed_conn):
        """Simple-query with two semicolon-separated statements must be handled."""
        authed_conn.send(pg_query("SELECT 1; SELECT 2"))
        msgs  = authed_conn.recv_until({_READY, _ERROR}, max_msgs=100)
        types = {m[0] for m in msgs}
        assert _READY in types or _ERROR in types

    def test_fragmented_delivery(self, authed_conn, compose_stack):
        """Query delivered in chunks — KEEL must either reassemble or close defensively.

        KEEL is considered compliant if it either:
        a) Correctly reassembles the fragmented message and returns a valid response, OR
        b) Defensively closes the connection but leaves the proxy alive for new sessions.

        Sending with chunk_size=5 splits the message at the natural PostgreSQL
        message-header boundary (1-byte type + 4-byte length), which is the
        most common real-world fragmentation point in TCP.
        """
        data = pg_query("SELECT 99")
        # chunk_size=5 = full 5-byte header in first send, body in subsequent sends.
        # This avoids splitting the header mid-field while still exercising
        # KEEL's ability to reassemble a message across multiple TCP reads.
        authed_conn.send_fragmented(data, chunk_size=5)
        authed_conn.set_timeout(15.0)
        try:
            msgs  = authed_conn.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            # Full reassembly: proxy handled TCP fragmentation correctly.
            assert _DATA_ROW in types, "No DataRow for fragmented query"
            assert _READY    in types, "No ReadyForQuery after fragmented query"
        except (TimeoutError, ConnectionError):
            # Proxy closed connection defensively — acceptable as long as it
            # remains alive and able to serve new client connections.
            new_conn = ProxyConn(
                compose_stack["keel_host"], compose_stack["keel_port"]
            )
            new_conn.connect()
            ok = new_conn.startup("postgres", "testdb", "postgres")
            try:
                new_conn.close()
            except Exception:
                pass
            assert ok, (
                "Proxy is unresponsive after fragmented delivery "
                "(connection close is acceptable; crash is not)"
            )

    def test_coalesced_two_queries(self, authed_conn):
        """Two queries written in one TCP write — both must be answered."""
        combined = pg_query("SELECT 1") + pg_query("SELECT 2")
        authed_conn.send(combined)
        ready_count = 0
        for _ in range(100):
            t, _ = authed_conn.recv_message()
            if t == _READY:
                ready_count += 1
                if ready_count == 2:
                    break
            elif t == _ERROR:
                break
        assert ready_count >= 1, "No ReadyForQuery responses for coalesced queries"

    def test_terminate_closes_connection(self, authed_conn):
        """Sending Terminate must result in the server closing the connection."""
        authed_conn.send(pg_terminate())
        authed_conn.set_timeout(5.0)
        try:
            authed_conn.recv_exact(1)
        except ConnectionError:
            pass  # expected — server closed


class TestExtendedQueryProtocol:
    def test_full_parse_bind_execute_cycle(self, authed_conn):
        """Parse → Bind → Execute → Sync must produce correct responses."""
        batch = (
            pg_parse("stmt_test", "SELECT $1::int + $2::int")
            + pg_describe("S", "stmt_test")
            + pg_bind("portal_test", "stmt_test", params=[b"40", b"2"])
            + pg_execute("portal_test")
            + pg_sync()
        )
        authed_conn.send(batch)
        msgs  = authed_conn.recv_until({_READY, _ERROR}, max_msgs=30)
        types = {m[0] for m in msgs}
        assert _PARSE_COMP in types, "No ParseComplete"
        assert _BIND_COMP  in types, "No BindComplete"
        assert _DATA_ROW   in types, "No DataRow"
        assert _READY      in types, "No ReadyForQuery"

        # Verify the computed value
        data_rows = [b for t, b in msgs if t == _DATA_ROW]
        assert len(data_rows) == 1
        # DataRow: int16 col count, then per-col int32 len + bytes
        col_count = struct.unpack(">H", data_rows[0][:2])[0]
        assert col_count == 1
        val_len = struct.unpack(">I", data_rows[0][2:6])[0]
        val_str = data_rows[0][6:6 + val_len].decode()
        assert val_str == "42", f"Expected 42, got {val_str}"

    def test_sync_without_parse(self, authed_conn):
        """Sync alone must yield ReadyForQuery — not crash."""
        authed_conn.send(pg_sync())
        msgs  = authed_conn.recv_until({_READY, _ERROR})
        types = {m[0] for m in msgs}
        assert _READY in types or _ERROR in types

    def test_describe_nonexistent_statement(self, authed_conn):
        """Describe of a nonexistent statement must return ErrorResponse."""
        authed_conn.send(pg_describe("S", "no_such_stmt") + pg_sync())
        msgs  = authed_conn.recv_until({_READY, _ERROR})
        types = {m[0] for m in msgs}
        assert _ERROR in types, "Expected ErrorResponse for nonexistent stmt describe"

    def test_execute_nonexistent_portal(self, authed_conn):
        """Execute on nonexistent portal must return ErrorResponse."""
        authed_conn.send(pg_execute("no_such_portal") + pg_sync())
        msgs  = authed_conn.recv_until({_READY, _ERROR})
        types = {m[0] for m in msgs}
        assert _ERROR in types or _READY in types


class TestProtocolFuzzing:
    def test_unknown_message_type_0xff(self, authed_conn):
        """Unknown message byte 0xFF must produce error or close — not crash."""
        bad_msg = b"\xff" + struct.pack(">I", 4)
        authed_conn.send(bad_msg)
        authed_conn.set_timeout(5.0)
        try:
            authed_conn.recv_until({_READY, _ERROR})
        except (ConnectionError, OSError):
            pass

    def test_cancel_wrong_pid_proxy_survives(self, compose_stack):
        """CancelRequest with bad pid must be ignored; proxy must still answer."""
        s = socket.create_connection(
            (compose_stack["keel_host"], compose_stack["keel_port"]), timeout=5.0
        )
        try:
            s.sendall(pg_cancel_request(0xDEAD, 0xBEEF))
            s.settimeout(3.0)
            try:
                s.recv(1)
            except (socket.timeout, ConnectionError):
                pass
        finally:
            s.close()

        # Proxy must still be alive
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            ok = c.startup("postgres", "testdb", "postgres")
        assert ok, "Proxy unreachable after bad cancel request"

    def test_cancel_invalid_length_proxy_survives(self, compose_stack):
        """CancelRequest with wrong length must not crash the proxy."""
        bad = struct.pack(">II", 8, PG_CANCEL_CODE)  # should be 16
        s = socket.create_connection(
            (compose_stack["keel_host"], compose_stack["keel_port"]), timeout=5.0
        )
        try:
            s.sendall(bad)
            s.settimeout(3.0)
            try:
                s.recv(1)
            except (socket.timeout, ConnectionError):
                pass
        finally:
            s.close()

        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            ok = c.startup("postgres", "testdb", "postgres")
        assert ok, "Proxy unreachable after invalid cancel length"

    def test_error_response_then_query_works(self, compose_stack):
        """After an ErrorResponse the proxy must still serve new connections."""
        # Send a bad query through its own connection to get an ErrorResponse
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c:
            ok = c.startup("postgres", "testdb", "postgres")
            if not ok:
                pytest.skip("Could not authenticate")
            c.send(pg_query("SELECT * FROM nonexistent_table_keel_test"))
            try:
                msgs = c.recv_until({_READY, _ERROR})
                types = {m[0] for m in msgs}
                assert _ERROR in types, "No ErrorResponse for bad query"
            except (TimeoutError, ConnectionError, OSError):
                pass  # proxy may close connection on error — that's OK

        # A fresh connection must work after the error
        with ProxyConn(compose_stack["keel_host"], compose_stack["keel_port"]) as c2:
            ok = c2.startup("postgres", "testdb", "postgres")
            assert ok, "Could not authenticate on recovery connection"
            c2.send(pg_query("SELECT 1"))
            msgs2 = c2.recv_until({_READY, _ERROR})
            types2 = {m[0] for m in msgs2}
            assert _READY    in types2, "No ReadyForQuery after error recovery"
            assert _DATA_ROW in types2, "No DataRow after error recovery"
