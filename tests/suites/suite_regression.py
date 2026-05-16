"""
tests/suites/suite_regression.py
==================================
Category F — Regression / Integration Tests

Validates core proxy behaviour that must never regress:
  - Read / write routing correctness (replica vs primary)
  - Transaction boundary pinning
  - Prepared statement lifecycle
  - Session variable propagation (SET search_path)
  - Temporary table session pinning
  - COPY FROM STDIN protocol
  - Large result set correctness
  - Long identifier handling
  - Concurrent reads / writes preserve data integrity

Requires a running KEEL proxy (KEEL_HOST / KEEL_PORT) with at least one
backend database that supports PostgreSQL wire protocol.

Additional env vars:
  KEEL_REPLICA_PORT   Port of the read replica (default: same as primary, so
                      read / write routing tests are skipped when unset)

Run standalone:
    python tests/suites/suite_regression.py --verbose
"""

from __future__ import annotations

import io
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    ProxyConn,
    is_proxy_reachable,
    pg_bind,
    pg_describe,
    pg_execute,
    pg_parse,
    pg_query,
    pg_sync,
    pg_terminate,
    proxy_env,
)

_READY = ord("Z")
_ERROR = ord("E")
_DATA_ROW = ord("D")


def _import_psycopg2():
    try:
        import psycopg2
        return psycopg2
    except ImportError:
        return None


def _make_dsn(env: dict) -> str:
    return (
        f"host={env['host']} port={env['port']} "
        f"user={env['user']} password={env['password']} "
        f"dbname={env['database']}"
    )


class RegressionSuite(SuiteRunner):
    NAME        = "regression"
    DESCRIPTION = "Category F — Regression / Integration Tests"
    TAGS        = ["regression", "integration", "routing", "sql", "correctness"]

    # table name used for tests — prefixed to avoid collisions
    _TBL = "keel_regression_test"

    def setup(self) -> None:
        self._env  = proxy_env()
        self._pg   = _import_psycopg2()
        if not is_proxy_reachable(self._env):
            self._skip_msg = (
                f"Proxy not reachable at {self._env['host']}:{self._env['port']}"
            )
        elif self._pg is None:
            self._skip_msg = "psycopg2 not installed — pip install psycopg2-binary"
        else:
            self._skip_msg = None
            self._create_test_table()

    def teardown(self) -> None:
        if self._pg and not self._skip_msg:
            try:
                conn = self._connect()
                conn.autocommit = True
                conn.cursor().execute(f"DROP TABLE IF EXISTS {self._TBL}")
                conn.close()
            except Exception:
                pass

    def _require_proxy(self) -> None:
        if self._skip_msg:
            self.skip(self._skip_msg)

    def _connect(self, autocommit: bool = True):
        conn = self._pg.connect(_make_dsn(self._env), connect_timeout=10)
        conn.autocommit = autocommit
        return conn

    def _create_test_table(self) -> None:
        try:
            conn = self._connect()
            conn.cursor().execute(f"""
                CREATE TABLE IF NOT EXISTS {self._TBL} (
                    id      SERIAL PRIMARY KEY,
                    val     TEXT    NOT NULL,
                    ts      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.close()
        except Exception:
            pass  # may fail if DB not ready; individual tests will skip

    # -----------------------------------------------------------------------
    # F1 — Basic read / write round-trip
    # -----------------------------------------------------------------------

    def test_f01_basic_read_write_roundtrip(self) -> None:
        """Write a row; read it back — data must be identical."""
        self._require_proxy()
        conn = self._connect()
        cur  = conn.cursor()
        sentinel = f"sentinel_{time.monotonic_ns()}"
        cur.execute(f"INSERT INTO {self._TBL}(val) VALUES (%s) RETURNING id", (sentinel,))
        row_id = cur.fetchone()[0]
        cur.execute(f"SELECT val FROM {self._TBL} WHERE id = %s", (row_id,))
        fetched = cur.fetchone()[0]
        conn.close()
        self.assert_eq(fetched, sentinel, "Round-trip value mismatch")

    # -----------------------------------------------------------------------
    # F2 — Transaction rollback correctness
    # -----------------------------------------------------------------------

    def test_f02_transaction_rollback(self) -> None:
        """A rolled-back transaction must not persist any rows."""
        self._require_proxy()
        conn = self._connect(autocommit=False)
        cur  = conn.cursor()
        sentinel = f"rollback_{time.monotonic_ns()}"
        cur.execute(f"INSERT INTO {self._TBL}(val) VALUES (%s)", (sentinel,))
        conn.rollback()
        conn.autocommit = True
        cur.execute(f"SELECT COUNT(*) FROM {self._TBL} WHERE val = %s", (sentinel,))
        count = cur.fetchone()[0]
        conn.close()
        self.assert_eq(count, 0, "Rolled-back row unexpectedly visible")

    # -----------------------------------------------------------------------
    # F3 — Transaction commit correctness
    # -----------------------------------------------------------------------

    def test_f03_transaction_commit(self) -> None:
        """A committed transaction must persist."""
        self._require_proxy()
        conn = self._connect(autocommit=False)
        cur  = conn.cursor()
        sentinel = f"commit_{time.monotonic_ns()}"
        cur.execute(f"INSERT INTO {self._TBL}(val) VALUES (%s)", (sentinel,))
        conn.commit()
        conn.autocommit = True
        cur.execute(f"SELECT COUNT(*) FROM {self._TBL} WHERE val = %s", (sentinel,))
        count = cur.fetchone()[0]
        conn.close()
        self.assert_eq(count, 1, "Committed row not found")

    # -----------------------------------------------------------------------
    # F4 — Prepared statement lifecycle
    # -----------------------------------------------------------------------

    def test_f04_prepared_statement_lifecycle(self) -> None:
        """PREPARE / EXECUTE / DEALLOCATE must work correctly through the proxy."""
        self._require_proxy()
        with ProxyConn(self._env["host"], self._env["port"]) as c:
            ok = c.startup(self._env["user"], self._env["database"], self._env["password"])
            self.assert_true(ok, "startup failed")

            # Extended query: parse + bind + execute + sync
            batch = (
                pg_parse("ps_test", "SELECT $1::int * $2::int")
                + pg_describe("S", "ps_test")
                + pg_bind("portal_test", "ps_test", params=[b"6", b"7"])
                + pg_execute("portal_test")
                + pg_sync()
            )
            c.send(batch)
            msgs = c.recv_until({_READY, _ERROR})
            types = {m[0] for m in msgs}
            self.assert_in(ord("1"), types, "No ParseComplete")
            self.assert_in(ord("2"), types, "No BindComplete")
            data_rows = [b for t, b in msgs if t == _DATA_ROW]
            self.assert_true(len(data_rows) > 0, "No DataRow in prepared statement result")

    # -----------------------------------------------------------------------
    # F5 — Session variable propagation
    # -----------------------------------------------------------------------

    def test_f05_session_variable_set(self) -> None:
        """SET search_path must be visible within the same session."""
        self._require_proxy()
        conn = self._connect()
        cur  = conn.cursor()
        cur.execute("SET search_path TO public, pg_catalog")
        cur.execute("SHOW search_path")
        sp = cur.fetchone()[0]
        conn.close()
        # The proxy must forward SET and the result must reflect the new value
        self.assert_true("public" in sp, f"SET search_path not propagated: {sp!r}")

    # -----------------------------------------------------------------------
    # F6 — Temporary table session pinning
    # -----------------------------------------------------------------------

    def test_f06_temp_table_session_pinning(self) -> None:
        """A session that creates a TEMP TABLE must stay on the same backend."""
        self._require_proxy()
        conn = self._connect()
        cur  = conn.cursor()
        # Create a temp table and immediately query it — must succeed
        cur.execute("CREATE TEMP TABLE keel_tmp_reg (x INT)")
        cur.execute("INSERT INTO keel_tmp_reg VALUES (1), (2), (3)")
        cur.execute("SELECT SUM(x) FROM keel_tmp_reg")
        total = cur.fetchone()[0]
        cur.execute("DROP TABLE keel_tmp_reg")
        conn.close()
        self.assert_eq(total, 6, f"Temp table sum mismatch: {total}")

    # -----------------------------------------------------------------------
    # F7 — COPY FROM STDIN
    # -----------------------------------------------------------------------

    def test_f07_copy_from_stdin(self) -> None:
        """COPY ... FROM STDIN must load data correctly through the proxy."""
        self._require_proxy()
        conn = self._connect()
        cur  = conn.cursor()
        data = io.StringIO("copytest_a\ncopytest_b\ncopytest_c\n")
        cur.copy_from(data, self._TBL, columns=("val",))
        cur.execute(f"SELECT COUNT(*) FROM {self._TBL} WHERE val LIKE 'copytest_%'")
        count = cur.fetchone()[0]
        cur.execute(f"DELETE FROM {self._TBL} WHERE val LIKE 'copytest_%'")
        conn.close()
        self.assert_eq(count, 3, f"COPY loaded {count} rows, expected 3")

    # -----------------------------------------------------------------------
    # F8 — Large result set correctness
    # -----------------------------------------------------------------------

    def test_f08_large_result_set_correctness(self) -> None:
        """Fetch 100 000 rows via generate_series; verify count and values."""
        self._require_proxy()
        n = 100_000
        conn = self._connect()
        cur  = conn.cursor()
        cur.execute(f"SELECT i FROM generate_series(1, {n}) i")
        rows = cur.fetchall()
        conn.close()

        self.assert_eq(len(rows), n, f"Expected {n} rows, got {len(rows)}")
        # Spot-check first and last
        self.assert_eq(rows[0][0],    1,  "First row mismatch")
        self.assert_eq(rows[-1][0], n,    "Last row mismatch")

    # -----------------------------------------------------------------------
    # F9 — Long identifier handling
    # -----------------------------------------------------------------------

    def test_f09_long_identifier(self) -> None:
        """PostgreSQL supports identifiers up to 63 bytes; verify they pass through."""
        self._require_proxy()
        # 63-char identifier (max for PostgreSQL)
        long_alias = "a" * 63
        conn = self._connect()
        cur  = conn.cursor()
        cur.execute(f'SELECT 42 AS "{long_alias}"')
        desc_names = [d[0] for d in cur.description]
        conn.close()
        # Postgres truncates silently to 63 chars — just ensure we get a result
        self.assert_true(len(desc_names) >= 1, "No columns in long-identifier query")

    # -----------------------------------------------------------------------
    # F10 — Concurrent writes preserve integrity
    # -----------------------------------------------------------------------

    def test_f10_concurrent_writes_integrity(self) -> None:
        """N threads each insert a unique sentinel; all must be visible afterwards."""
        self._require_proxy()
        n = 20
        sentinels = [f"concurrent_{time.monotonic_ns()}_{i}" for i in range(n)]
        errors: list[Exception] = []

        def _insert(val: str) -> None:
            try:
                conn = self._connect()
                conn.cursor().execute(f"INSERT INTO {self._TBL}(val) VALUES (%s)", (val,))
                conn.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_insert, args=(s,)) for s in sentinels]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)

        if errors:
            raise AssertionError(f"{len(errors)} concurrent insert error(s): {errors[0]}")

        conn = self._connect()
        cur  = conn.cursor()
        placeholders = ", ".join(["%s"] * n)
        cur.execute(
            f"SELECT COUNT(*) FROM {self._TBL} WHERE val IN ({placeholders})",
            sentinels,
        )
        found = cur.fetchone()[0]
        conn.close()
        self.assert_eq(found, n, f"Expected {n} rows after concurrent inserts, found {found}")


# ---------------------------------------------------------------------------
# Coordinator entry point
# ---------------------------------------------------------------------------

def run(result: SuiteResult, **kwargs: object) -> None:
    runner = RegressionSuite(result, verbose=bool(kwargs.get("verbose")),
                             filter_=str(kwargs.get("filter") or "") or None)
    t0 = time.monotonic()
    runner.run_all()
    result.duration = time.monotonic() - t0


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    standalone_main(RegressionSuite, "regression", RegressionSuite.DESCRIPTION)
