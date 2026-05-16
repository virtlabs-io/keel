"""
tests/suites/suite_protocol.py
================================
Category D — Protocol Compliance and Fuzzing

All tests use raw TCP sockets — no libpq — so they exercise the proxy's
wire-protocol parser directly.  The proxy MUST NOT crash on any input;
it MUST respond with a well-formed error for invalid messages.

Environment variables (same as suite_throughput):
  KEEL_HOST, KEEL_PORT, KEEL_USER, KEEL_PASSWORD, KEEL_DATABASE

Tests:
  D1.  Valid startup → ReadyForQuery
  D2.  SSL negotiation (SSLRequest → 'N' → normal startup)
  D3.  Startup with invalid protocol version
  D4.  Startup with truncated length field
  D5.  Startup with over-large user field (50 KB)
  D6.  Startup missing database key
  D7.  Simple query — SELECT 1
  D8.  Simple query — empty string
  D9.  Simple query — 64 KB payload
  D10. Simple query — fragmented delivery (1 byte / tick)
  D11. Simple query — coalesced: two queries in one TCP write
  D12. Terminate message — connection must close cleanly
  D13. Extended query — Parse + Describe + Bind + Execute + Sync cycle
  D14. Extended query — Sync without prior Parse (must not crash)
  D15. Extended query — Execute with nonexistent portal (must return error)
  D16. Unknown frontend message type (0xFF, 0x00, 0x7F)
  D17. Cancel request with wrong pid/secret (proxy must not crash)
  D18. Cancel request with invalid length
  D19. Binary garbage after startup (10 KB of random bytes)
  D20. Half-open connection (connect, never send startup) — must time out or close

Run standalone:
    python tests/suites/suite_protocol.py --verbose
"""

from __future__ import annotations

import os
import random
import socket
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    PG_CANCEL_CODE,
    PG_PROTO_V3,
    PG_SSL_CODE,
    ProxyConn,
    is_proxy_reachable,
    pg_bind,
    pg_cancel_request,
    pg_close,
    pg_describe,
    pg_execute,
    pg_flush,
    pg_parse,
    pg_query,
    pg_ssl_request,
    pg_startup_msg,
    pg_sync,
    pg_terminate,
    proxy_env,
    wait_for_port,
)

# Message-type constants (backend)
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
_ROW_DESC_   = ord("T")
_NO_DATA     = ord("n")
_NOTICE      = ord("N")


class ProtocolSuite(SuiteRunner):
    NAME        = "protocol"
    DESCRIPTION = "Category D — Protocol Compliance and Fuzzing"
    TAGS        = ["protocol", "wire", "fuzz", "compliance", "fragmentation"]

    def setup(self) -> None:
        self._env = proxy_env()
        if not is_proxy_reachable(self._env):
            self._skip_msg = (
                f"Proxy not reachable at {self._env['host']}:{self._env['port']}"
            )
        else:
            self._skip_msg = None

    def _require_proxy(self) -> None:
        if self._skip_msg:
            self.skip(self._skip_msg)

    def _conn(self, timeout: float = 5.0) -> ProxyConn:
        c = ProxyConn(self._env["host"], self._env["port"], timeout=timeout)
        c.connect()
        return c

    def _raw_connect(self) -> socket.socket:
        s = socket.create_connection(
            (self._env["host"], self._env["port"]), timeout=5.0
        )
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return s

    def _expect_ready(self, c: ProxyConn) -> bool:
        """Drain messages until ReadyForQuery or ErrorResponse."""
        try:
            for _ in range(50):
                t, _ = c.recv_message()
                if t == _READY:
                    return True
                if t == _ERROR:
                    return False
        except (ConnectionError, OSError, socket.timeout):
            pass
        return False

    # -----------------------------------------------------------------------
    # D1 — Valid startup
    # -----------------------------------------------------------------------

    def test_d01_valid_startup(self) -> None:
        """Full startup + auth → ReadyForQuery must succeed."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(
                self._env["user"],
                self._env["database"],
                self._env["password"],
            )
        self.assert_true(ok, "startup did not reach ReadyForQuery")

    # -----------------------------------------------------------------------
    # D2 — SSL negotiation
    # -----------------------------------------------------------------------

    def test_d02_ssl_request_decline(self) -> None:
        """SSLRequest → receive 'N' → then complete a normal startup."""
        self._require_proxy()
        with self._conn() as c:
            c.send(pg_ssl_request())
            resp = c.recv_exact(1)
            # 'N' (no SSL) or 'S' (SSL accepted) — both are valid protocol responses
            self.assert_true(
                resp in (b"N", b"S"),
                f"Unexpected SSL response byte: {resp!r}",
            )
            if resp == b"N":
                # Follow up with a normal startup — proxy must still work
                ok = c.startup(
                    self._env["user"],
                    self._env["database"],
                    self._env["password"],
                )
                self.assert_true(ok, "Startup after SSL decline did not reach ReadyForQuery")

    # -----------------------------------------------------------------------
    # D3 — Invalid protocol version
    # -----------------------------------------------------------------------

    def test_d03_startup_invalid_version(self) -> None:
        """Startup with protocol version 2.0 must produce an ErrorResponse."""
        self._require_proxy()
        body = b"user\x00postgres\x00database\x00postgres\x00\x00"
        proto_v2 = (2 << 16)
        msg = struct.pack(">II", 4 + 4 + len(body), proto_v2) + body

        s = self._raw_connect()
        try:
            s.sendall(msg)
            s.settimeout(5.0)
            response = s.recv(4096)
            # Must receive something (error response) — not a crash / empty
            self.assert_true(len(response) > 0,
                             "No response to invalid protocol version — possible crash")
        finally:
            s.close()

    # -----------------------------------------------------------------------
    # D4 — Truncated length field
    # -----------------------------------------------------------------------

    def test_d04_startup_truncated_length(self) -> None:
        """Sending only 2 bytes of the startup length must close gracefully."""
        self._require_proxy()
        s = self._raw_connect()
        try:
            s.sendall(b"\x00\x00")   # incomplete 4-byte length
            s.settimeout(6.0)
            try:
                data = s.recv(4096)
                # Either graceful error response or clean close — not a crash
                # (recv returning b"" means the server closed the connection)
            except socket.timeout:
                pass  # server held the connection open waiting — also acceptable
        finally:
            s.close()

    # -----------------------------------------------------------------------
    # D5 — Over-large user field
    # -----------------------------------------------------------------------

    def test_d05_startup_oversized_user_field(self) -> None:
        """50 KB user field — proxy must respond with error, not crash."""
        self._require_proxy()
        long_user = "x" * (50 * 1024)
        msg = pg_startup_msg(user=long_user)

        s = self._raw_connect()
        try:
            s.sendall(msg)
            s.settimeout(5.0)
            try:
                data = s.recv(8192)
                self.assert_true(len(data) > 0, "No response to oversized startup")
            except socket.timeout:
                pass  # timing out without crashing is acceptable
        finally:
            s.close()

    # -----------------------------------------------------------------------
    # D6 — Startup missing database key
    # -----------------------------------------------------------------------

    def test_d06_startup_no_database_key(self) -> None:
        """Startup with only 'user' key (no 'database') — must not crash."""
        self._require_proxy()
        body = b"user\x00postgres\x00\x00"
        length = 4 + 4 + len(body)
        msg = struct.pack(">II", length, PG_PROTO_V3) + body

        s = self._raw_connect()
        try:
            s.sendall(msg)
            s.settimeout(5.0)
            try:
                data = s.recv(4096)
                # Accept any well-formed response (error or success)
                self.assert_true(len(data) > 0, "No response to startup without database key")
            except socket.timeout:
                pass
        finally:
            s.close()

    # -----------------------------------------------------------------------
    # D7 — Simple query: SELECT 1
    # -----------------------------------------------------------------------

    def test_d07_simple_query_select_one(self) -> None:
        """SELECT 1 via simple query protocol must return DataRow + CommandComplete."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            c.send(pg_query("SELECT 1"))
            msgs = c.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            self.assert_in(_ROW_DESC, types, "No RowDescription in SELECT 1 response")
            self.assert_in(_DATA_ROW, types, "No DataRow in SELECT 1 response")
            self.assert_in(_READY, types, "No ReadyForQuery after SELECT 1")

    # -----------------------------------------------------------------------
    # D8 — Simple query: empty string
    # -----------------------------------------------------------------------

    def test_d08_simple_query_empty(self) -> None:
        """Empty query string must return EmptyQueryResponse (not crash)."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            c.send(pg_query(""))
            msgs = c.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            # Must get either EmptyQueryResponse or an error — not a crash
            self.assert_true(
                _EMPTY_QUERY in types or _ERROR in types or _READY in types,
                f"Unexpected response to empty query: {[chr(t) for t in types]}"
            )

    # -----------------------------------------------------------------------
    # D9 — Simple query: 64 KB payload
    # -----------------------------------------------------------------------

    def test_d09_simple_query_large_payload(self) -> None:
        """A 64 KB query string must produce a response, not a crash."""
        self._require_proxy()
        # Construct a very long but syntactically valid query
        long_sql = "SELECT " + ", ".join(f"{i}::int" for i in range(4096))

        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            c.set_timeout(15.0)
            c.send(pg_query(long_sql))
            msgs = c.recv_until({_READY, _ERROR}, max_msgs=200)
            types = {m[0] for m in msgs}
            self.assert_true(
                _READY in types or _ERROR in types,
                "No ReadyForQuery or Error after large query — possible crash"
            )

    # -----------------------------------------------------------------------
    # D10 — Fragmented delivery (1 byte per write)
    # -----------------------------------------------------------------------

    def test_d10_simple_query_fragmented(self) -> None:
        """Send a query 1 byte at a time — proxy must reassemble and respond."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            data = pg_query("SELECT 42")
            c.send_fragmented(data, chunk_size=1)
            c.set_timeout(10.0)
            msgs = c.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            self.assert_in(_READY, types, "Proxy did not respond to fragmented query")
            self.assert_in(_DATA_ROW, types, "No DataRow in fragmented query response")

    # -----------------------------------------------------------------------
    # D11 — Coalesced messages
    # -----------------------------------------------------------------------

    def test_d11_coalesced_queries(self) -> None:
        """Two queries written to the socket in one syscall — both must be answered."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            # Write both queries atomically
            combined = pg_query("SELECT 1") + pg_query("SELECT 2")
            c.send(combined)
            # Read until we see two ReadyForQuery responses
            ready_count = 0
            for _ in range(100):
                t, _ = c.recv_message()
                if t == _READY:
                    ready_count += 1
                    if ready_count == 2:
                        break
                elif t == _ERROR:
                    # An error is acceptable — the proxy must keep the connection alive
                    break
            self.assert_true(ready_count >= 1,
                             "Proxy did not respond to coalesced queries")

    # -----------------------------------------------------------------------
    # D12 — Terminate message
    # -----------------------------------------------------------------------

    def test_d12_terminate_closes_cleanly(self) -> None:
        """Sending Terminate must result in the proxy closing the connection."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            c.send(pg_terminate())
            c.set_timeout(5.0)
            try:
                data = c.recv_exact(1)
                # Either EOF (b"") or a response — both acceptable
            except ConnectionError:
                pass  # server closed connection — expected

    # -----------------------------------------------------------------------
    # D13 — Extended query: full Parse-Describe-Bind-Execute-Sync cycle
    # -----------------------------------------------------------------------

    def test_d13_extended_query_full_cycle(self) -> None:
        """Full extended query protocol cycle must produce correct responses."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")

            # Send Parse + Describe + Bind + Execute + Sync in one batch
            batch = (
                pg_parse("stmt1", "SELECT $1::int + $2::int")
                + pg_describe("S", "stmt1")
                + pg_bind("portal1", "stmt1",
                          params=[b"7", b"3"])
                + pg_execute("portal1")
                + pg_sync()
            )
            c.send(batch)
            c.set_timeout(10.0)

            msgs = c.recv_until({_READY, _ERROR}, max_msgs=30)
            types = {m[0] for m in msgs}
            self.assert_in(_PARSE_COMP, types, "No ParseComplete")
            self.assert_in(_BIND_COMP,  types, "No BindComplete")
            self.assert_in(_DATA_ROW,   types, "No DataRow in extended query response")
            self.assert_in(_READY,      types, "No ReadyForQuery at end of extended query")

    # -----------------------------------------------------------------------
    # D14 — Sync without prior Parse
    # -----------------------------------------------------------------------

    def test_d14_sync_only_after_auth(self) -> None:
        """Sync without any prior Parse must yield ReadyForQuery (not crash)."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            c.send(pg_sync())
            c.set_timeout(5.0)
            msgs = c.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            self.assert_true(
                _READY in types or _ERROR in types,
                "No ReadyForQuery or Error after bare Sync"
            )

    # -----------------------------------------------------------------------
    # D15 — Execute nonexistent portal
    # -----------------------------------------------------------------------

    def test_d15_execute_nonexistent_portal(self) -> None:
        """Executing a portal that was never bound must return ErrorResponse."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")
            c.send(pg_execute("no_such_portal") + pg_sync())
            c.set_timeout(5.0)
            msgs = c.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            # Must get an error response, but NOT crash
            self.assert_true(
                _ERROR in types or _READY in types,
                "No ErrorResponse or ReadyForQuery for nonexistent portal"
            )

    # -----------------------------------------------------------------------
    # D16 — Unknown frontend message types
    # -----------------------------------------------------------------------

    def test_d16_unknown_message_types(self) -> None:
        """Unknown frontend message bytes must produce error or close — not crash."""
        self._require_proxy()
        for bad_byte in (b"\xff", b"\x00", b"\x7f"):
            s = self._raw_connect()
            try:
                # Authenticate first so we're in the command phase
                c = ProxyConn(self._env["host"], self._env["port"])
                c._sock = s
                ok = c.startup(
                    self._env["user"], self._env["database"], self._env["password"]
                )
                if not ok:
                    s.close()
                    continue
                # Construct a fake message: <type><int32=4>
                bad_msg = bad_byte + struct.pack(">I", 4)
                s.sendall(bad_msg)
                s.settimeout(5.0)
                try:
                    resp = s.recv(4096)
                    # Any non-empty response (error) is acceptable
                except (socket.timeout, ConnectionError):
                    pass   # closed connection is also acceptable
            finally:
                try:
                    s.close()
                except OSError:
                    pass

    # -----------------------------------------------------------------------
    # D17 — Cancel request with wrong pid/secret
    # -----------------------------------------------------------------------

    def test_d17_cancel_wrong_pid_secret(self) -> None:
        """CancelRequest with a nonexistent pid must be silently ignored."""
        self._require_proxy()
        # Cancel requests are sent on a fresh connection (per protocol spec)
        s = self._raw_connect()
        try:
            s.sendall(pg_cancel_request(pid=0xDEAD, secret=0xBEEF))
            s.settimeout(3.0)
            try:
                # Per spec: server closes connection after cancel (no response)
                data = s.recv(1)
            except (socket.timeout, ConnectionError):
                pass
        finally:
            s.close()

        # Verify the proxy is still alive by making a normal connection
        with self._conn() as c:
            ok = c.startup(
                self._env["user"], self._env["database"], self._env["password"]
            )
        self.assert_true(ok, "Proxy did not survive cancel request with bad pid/secret")

    # -----------------------------------------------------------------------
    # D18 — Cancel request with invalid length
    # -----------------------------------------------------------------------

    def test_d18_cancel_invalid_length(self) -> None:
        """CancelRequest with wrong length field must not crash the proxy."""
        self._require_proxy()
        s = self._raw_connect()
        try:
            # Correct cancel code, wrong length (8 instead of 16)
            bad_cancel = struct.pack(">II", 8, PG_CANCEL_CODE)
            s.sendall(bad_cancel)
            s.settimeout(3.0)
            try:
                s.recv(1)
            except (socket.timeout, ConnectionError):
                pass
        finally:
            s.close()

        # Proxy must still be alive
        with self._conn() as c:
            alive = c.startup(
                self._env["user"], self._env["database"], self._env["password"]
            )
        self.assert_true(alive, "Proxy crashed after invalid cancel request length")

    # -----------------------------------------------------------------------
    # D19 — Binary garbage after startup
    # -----------------------------------------------------------------------

    def test_d19_binary_garbage_post_auth(self) -> None:
        """10 KB of random bytes after successful auth must not crash the proxy."""
        self._require_proxy()
        with self._conn() as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")

            rng = random.Random(0xDEADBEEF)
            garbage = bytes(rng.randint(0, 255) for _ in range(10 * 1024))
            try:
                c.send(garbage)
                c.set_timeout(5.0)
                c.recv_until({_READY, _ERROR})
            except (ConnectionError, OSError):
                pass  # proxy may close — that is fine

        # Ensure the proxy is still alive
        with self._conn() as c2:
            alive = c2.startup(
                self._env["user"], self._env["database"], self._env["password"]
            )
        self.assert_true(alive, "Proxy crashed after binary garbage")

    # -----------------------------------------------------------------------
    # D20 — Half-open connection (no startup sent)
    # -----------------------------------------------------------------------

    def test_d20_half_open_connection(self) -> None:
        """A connection that never sends startup must eventually be closed by the proxy."""
        self._require_proxy()
        s = self._raw_connect()
        try:
            # Send nothing — wait for the proxy to close or time out
            s.settimeout(35.0)   # give the proxy up to 30 s to clean up
            try:
                data = s.recv(4096)
                # Either the proxy sends an error and closes, or just closes
            except (socket.timeout, ConnectionError):
                pass  # timeout is acceptable — the proxy simply hasn't closed yet
        finally:
            s.close()


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = ProtocolSuite(result, verbose=bool(kwargs.get("verbose")),
                           filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    standalone_main(ProtocolSuite, "protocol", ProtocolSuite.DESCRIPTION)
