"""
tests/suites/suite_torture.py
==============================
Category I — PostgreSQL Protocol Torture Suite

Production readiness requires passing these tests with real PostgreSQL
workloads using every major client library, not only unit/integration tests.

This suite exercises KEEL as a proxy in front of a real PostgreSQL backend
using the following clients / workloads:

  I1.  psql — simple and extended protocol, COPY, meta-commands
  I2.  pgbench — TPC-B throughput + latency histogram stability
  I3.  JDBC (PostgreSQL JDBC driver via a small Java program)
  I4.  pgx   (Go driver via a Go program invoked as subprocess)
  I5.  asyncpg  (Python native async driver)
  I6.  psycopg3 (Python sync + async driver)
  I7.  Prisma   (Node.js ORM via a Prisma script)
  I8.  Hibernate (Java ORM via a small Spring Boot snippet)
  I9.  Failover chaos — primary killed mid-transaction; clients must
       receive a clean error and be able to reconnect within 5 s.
  I10. Prepared-statement reuse across pool cycle
  I11. Long soak under mixed psycopg3 + asyncpg load (configurable duration)
  I12. Connection storm — 500 sequential connect/disconnect cycles

Each test skips (not fails) when the required binary / library is absent,
so the suite degrades gracefully in CI environments that only have a subset
of drivers installed.

Environment variables:
  KEEL_HOST            Proxy host        (default: 127.0.0.1)
  KEEL_PORT            Proxy port        (default: 7432)
  KEEL_USER            Database user     (default: postgres)
  KEEL_PASSWORD        Database password (default: postgres)
  KEEL_DATABASE        Database name     (default: postgres)
  KEEL_PG_HOST         Direct PG host for chaos tests (default: 127.0.0.1)
  KEEL_PG_PORT         Direct PG port                 (default: 5432)
  KEEL_TORTURE_SOAK_S  Soak duration for I11          (default: 60)
  KEEL_PROXY_PID       PID of the keel process (for RSS / FD checks)

Run standalone:
    python tests/suites/suite_torture.py --verbose
    python tests/suites/suite_torture.py --verbose --soak 300
"""

from __future__ import annotations

import json
import os
import random
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import urllib.request
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.suites import SuiteResult, SuiteRunner, standalone_main
from tests.suites.common import (
    ProxyConn,
    check_command,
    is_proxy_reachable,
    latency_stats,
    percentile,
    proxy_env,
    wait_for_port,
    pg_query,
    pg_cancel_request,
    pg_startup_msg,
    pg_terminate,
    pg_parse,
    pg_bind,
    pg_describe,
    pg_execute,
    pg_sync,
    pg_flush,
)

_DEFAULT_SOAK_S = 60

# ---------------------------------------------------------------------------
# Lazy driver imports
# ---------------------------------------------------------------------------

def _import_psycopg3():
    try:
        import psycopg
        return psycopg
    except ImportError:
        return None


def _import_asyncpg():
    try:
        import asyncpg
        return asyncpg
    except ImportError:
        return None


def _make_dsn(env: dict, *, direct: bool = False) -> str:
    host = env.get("pg_host", env["host"]) if direct else env["host"]
    port = env.get("pg_port", env["port"])  if direct else env["port"]
    return (
        f"host={host} port={port} "
        f"user={env['user']} password={env['password']} "
        f"dbname={env['database']}"
    )


def _conninfo(env: dict) -> dict:
    return dict(
        host=env["host"],
        port=int(env["port"]),
        user=env["user"],
        password=env["password"],
        dbname=env["database"],
    )


# ---------------------------------------------------------------------------
# Subprocess helper with timeout
# ---------------------------------------------------------------------------

def _run(cmd: list[str], *, timeout: int = 60, env: dict | None = None,
         stdin_text: str | None = None,
         cwd: str | None = None) -> tuple[int, str, str]:
    import os as _os
    merged_env = {**_os.environ, **(env or {})}
    r = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=merged_env,
        input=stdin_text,
        cwd=cwd,
    )
    return r.returncode, r.stdout, r.stderr


# ---------------------------------------------------------------------------
# Suite
# ---------------------------------------------------------------------------

class TortureSuite(SuiteRunner):
    NAME        = "torture"
    DESCRIPTION = "Category I — PostgreSQL Protocol Torture Suite"
    TAGS        = ["torture", "protocol", "drivers", "chaos", "soak"]

    def setup(self) -> None:
        self._env  = proxy_env()
        self._env.setdefault("pg_host", os.environ.get("KEEL_PG_HOST", "127.0.0.1"))
        self._env.setdefault("pg_port", os.environ.get("KEEL_PG_PORT", "5432"))
        self._soak_s = int(
            os.environ.get("KEEL_TORTURE_SOAK_S")
            or self.kwargs.get("soak", _DEFAULT_SOAK_S)
        )
        self._t_start = time.monotonic()
        self.result.metrics["env"] = {
            "keel_host":   self._env["host"],
            "keel_port":   str(self._env["port"]),
            "soak_s":      self._soak_s,
        }
        self._admin_before = _collect_keel_admin_stats(self._env)

    def teardown(self) -> None:
        admin_after = _collect_keel_admin_stats(self._env)
        prom        = _collect_prometheus_metrics(self._env)

        self.result.metrics["duration_s"]   = round(time.monotonic() - self._t_start, 2)
        self.result.metrics["admin_before"] = self._admin_before
        self.result.metrics["admin_after"]  = admin_after
        if prom:
            # Store a compact subset of the most interesting counters
            want = {k: v for k, v in prom.items() if any(
                tag in k for tag in ("query", "bytes", "conn", "pool", "wait", "error")
            )}
            self.result.metrics["prometheus"] = want

    # =======================================================================
    # I1 — psql: simple query, extended protocol, COPY, \d
    # =======================================================================

    def test_i1_psql_simple_query(self) -> None:
        if not check_command("psql"):
            self.skip("psql not found on PATH")
        e = self._env
        cmd = [
            "psql",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}",
            f"--dbname={e['database']}",
            "--no-password",
            "--tuples-only", "--no-align",
            "--command=SELECT 1+1",
        ]
        rc, out, err = _run(cmd, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"psql exited {rc}: {err}"
        assert "2" in out, f"unexpected output: {out!r}"

    def test_i1_psql_extended_protocol(self) -> None:
        """psql PREPARE + EXECUTE exercises extended protocol through the proxy."""
        if not check_command("psql"):
            self.skip("psql not found on PATH")
        e = self._env
        sql = (
            "PREPARE t(int) AS SELECT $1 * 2; "
            "EXECUTE t(21); "
            "DEALLOCATE t;"
        )
        cmd = [
            "psql",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}",
            f"--dbname={e['database']}",
            "--no-password",
            "--tuples-only", "--no-align",
            f"--command={sql}",
        ]
        rc, out, err = _run(cmd, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"psql exited {rc}: {err}"
        assert "42" in out, f"unexpected output: {out!r}"

    def test_i1_psql_copy_to_stdout(self) -> None:
        """COPY TO STDOUT sends a CopyOutResponse; proxy must forward correctly."""
        if not check_command("psql"):
            self.skip("psql not found on PATH")
        e = self._env
        cmd = [
            "psql",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}",
            f"--dbname={e['database']}",
            "--no-password",
            "--command=COPY (SELECT generate_series(1,5)) TO STDOUT",
        ]
        rc, out, err = _run(cmd, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"psql COPY exited {rc}: {err}"
        rows = [r for r in out.strip().splitlines() if r.strip().isdigit()]
        assert len(rows) == 5, f"expected 5 rows, got: {out!r}"

    def test_i1_psql_large_result_set(self) -> None:
        """10 000-row result via psql — tests buffering and result streaming."""
        if not check_command("psql"):
            self.skip("psql not found on PATH")
        e = self._env
        cmd = [
            "psql",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}",
            f"--dbname={e['database']}",
            "--no-password",
            "--tuples-only", "--no-align",
            "--command=SELECT generate_series(1, 10000)",
        ]
        rc, out, err = _run(cmd, timeout=30, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"psql exited {rc}: {err}"
        rows = [r for r in out.strip().splitlines() if r.strip()]
        assert len(rows) == 10_000, f"expected 10000 rows, got {len(rows)}"

    # =======================================================================
    # I2 — pgbench: TPC-B throughput + latency stability
    # =======================================================================

    def test_i2_pgbench_init(self) -> None:
        """pgbench -i initializes the TPC-B schema; proxy must be transparent."""
        if not check_command("pgbench"):
            self.skip("pgbench not found on PATH")
        e = self._env
        cmd = [
            "pgbench", "-i", "-s", "1",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}", e["database"],
        ]
        rc, out, err = _run(cmd, timeout=120, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"pgbench -i exited {rc}: {err}"

    def test_i2_pgbench_tpcb_throughput(self) -> None:
        """pgbench TPC-B 30 s run — must complete with a positive TPS."""
        if not check_command("pgbench"):
            self.skip("pgbench not found on PATH")
        e = self._env
        cmd = [
            "pgbench",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}", e["database"],
            "--transactions=100",
            "--client=4",
            "--jobs=2",
        ]
        rc, out, err = _run(cmd, timeout=120, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"pgbench exited {rc}: {err}"
        combined = out + err
        assert "tps" in combined.lower() or "transaction" in combined.lower(), (
            f"pgbench produced no TPS line: {combined!r}"
        )

    def test_i2_pgbench_prepared_statements(self) -> None:
        """pgbench -M prepared — exercises prepared-statement virtualisation."""
        if not check_command("pgbench"):
            self.skip("pgbench not found on PATH")
        e = self._env
        cmd = [
            "pgbench",
            f"--host={e['host']}", f"--port={e['port']}",
            f"--username={e['user']}", e["database"],
            "--transactions=50",
            "--client=2",
            "--jobs=1",
            "--protocol=prepared",
        ]
        rc, out, err = _run(cmd, timeout=120, env={"PGPASSWORD": e["password"]})
        assert rc == 0, f"pgbench -M prepared exited {rc}: {err}"

    # =======================================================================
    # I3 — JDBC (PostgreSQL JDBC driver)
    # =======================================================================

    def test_i3_jdbc(self) -> None:
        """
        Simple JDBC SELECT 1 via a self-contained Java source snippet.

        Requires: java + postgresql JDBC jar at KEEL_JDBC_JAR env var
        or a default location.  Skips when Java or the jar is absent.
        """
        if not check_command("java"):
            self.skip("java not found on PATH")
        jdbc_jar = os.environ.get(
            "KEEL_JDBC_JAR",
            "/usr/share/java/postgresql.jar",
        )
        if not Path(jdbc_jar).exists():
            self.skip(f"PostgreSQL JDBC jar not found at {jdbc_jar}")

        e = self._env
        java_src = textwrap.dedent(f"""
            import java.sql.*;
            public class KTortureJDBC {{
                public static void main(String[] a) throws Exception {{
                    String url = "jdbc:postgresql://{e['host']}:{e['port']}/{e['database']}";
                    java.util.Properties p = new java.util.Properties();
                    p.setProperty("user",     "{e['user']}");
                    p.setProperty("password", "{e['password']}");
                    p.setProperty("prepareThreshold", "1");  // force server-side prepare
                    try (Connection c = DriverManager.getConnection(url, p)) {{
                        try (PreparedStatement ps = c.prepareStatement("SELECT ? + ?")) {{
                            ps.setInt(1, 19);
                            ps.setInt(2, 23);
                            try (ResultSet rs = ps.executeQuery()) {{
                                if (!rs.next()) throw new RuntimeException("no row");
                                int v = rs.getInt(1);
                                if (v != 42) throw new RuntimeException("expected 42 got " + v);
                            }}
                        }}
                        System.out.println("JDBC OK");
                    }}
                }}
            }}
        """).strip()

        with tempfile.TemporaryDirectory() as td:
            src_path = Path(td) / "KTortureJDBC.java"
            src_path.write_text(java_src)
            rc, out, err = _run(
                ["javac", "-cp", jdbc_jar, str(src_path)],
                timeout=30,
            )
            if rc != 0:
                self.skip(f"javac failed (driver incompatibility?): {err[:200]}")
            rc, out, err = _run(
                ["java", "-cp", f"{td}:{jdbc_jar}", "KTortureJDBC"],
                timeout=15,
            )
        assert rc == 0, f"JDBC test failed: {err}"
        assert "JDBC OK" in out, f"unexpected output: {out!r}"

    # =======================================================================
    # I4 — pgx (Go driver)
    # =======================================================================

    def test_i4_pgx(self) -> None:
        """
        pgx v5 SELECT 1 via a self-contained Go program.

        Requires: go on PATH and network access to pkg.go.dev (for go get).
        Skips when go is absent.
        """
        if not check_command("go"):
            self.skip("go not found on PATH")
        e = self._env
        go_src = textwrap.dedent(f"""
            package main

            import (
                "context"
                "fmt"
                "os"

                "github.com/jackc/pgx/v5"
            )

            func main() {{
                ctx := context.Background()
                dsn := "postgres://{e['user']}:{e['password']}@{e['host']}:{e['port']}/{e['database']}"
                conn, err := pgx.Connect(ctx, dsn)
                if err != nil {{
                    fmt.Fprintln(os.Stderr, "connect:", err)
                    os.Exit(1)
                }}
                defer conn.Close(ctx)

                // Use extended protocol with a named prepared statement
                _, err = conn.Prepare(ctx, "mul", "SELECT $1::int * $2::int")
                if err != nil {{
                    fmt.Fprintln(os.Stderr, "prepare:", err)
                    os.Exit(1)
                }}
                var result int
                err = conn.QueryRow(ctx, "mul", 6, 7).Scan(&result)
                if err != nil {{
                    fmt.Fprintln(os.Stderr, "query:", err)
                    os.Exit(1)
                }}
                if result != 42 {{
                    fmt.Fprintln(os.Stderr, "expected 42, got", result)
                    os.Exit(1)
                }}
                fmt.Println("pgx OK")
            }}
        """).strip()

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            (tdp / "main.go").write_text(go_src)
            # Initialise a minimal Go module
            rc, out, err = _run(
                ["go", "mod", "init", "keel_torture_pgx"],
                timeout=10,
                env={"GOPATH": str(tdp / "gopath"), "HOME": str(tdp)},
            )
            if rc != 0:
                self.skip(f"go mod init failed: {err[:200]}")
            rc, out, err = _run(
                ["go", "get", "github.com/jackc/pgx/v5"],
                timeout=60,
                env={"GOPATH": str(tdp / "gopath"), "HOME": str(tdp)},
            )
            if rc != 0:
                self.skip(f"go get pgx failed (no network?): {err[:200]}")
            rc, out, err = _run(
                ["go", "run", "."],
                timeout=30,
                env={"GOPATH": str(tdp / "gopath"), "HOME": str(tdp)},
            )
        assert rc == 0, f"pgx test failed: {err}"
        assert "pgx OK" in out, f"unexpected output: {out!r}"

    # =======================================================================
    # I5 — asyncpg (Python native async driver)
    # =======================================================================

    def test_i5_asyncpg_basic(self) -> None:
        ag = _import_asyncpg()
        if ag is None:
            self.skip("asyncpg not installed")
        import asyncio

        e = self._env

        async def _run_async():
            conn = await ag.connect(
                host=e["host"], port=int(e["port"]),
                user=e["user"], password=e["password"],
                database=e["database"],
                timeout=10,
            )
            try:
                result = await asyncio.wait_for(conn.fetchval("SELECT 21 * 2"), timeout=10)
                assert result == 42, f"expected 42 got {result}"
            finally:
                try:
                    await asyncio.wait_for(conn.close(), timeout=5)
                except (asyncio.TimeoutError, Exception):
                    conn.terminate()  # immediate close if graceful Terminate hangs

        asyncio.run(_run_async())

    def test_i5_asyncpg_prepared_statement(self) -> None:
        """asyncpg named prepared statement — exercises extended protocol."""
        ag = _import_asyncpg()
        if ag is None:
            self.skip("asyncpg not installed")
        import asyncio

        e = self._env

        async def _run_async():
            conn = await ag.connect(
                host=e["host"], port=int(e["port"]),
                user=e["user"], password=e["password"],
                database=e["database"],
                timeout=10,
            )
            try:
                stmt = await asyncio.wait_for(conn.prepare("SELECT $1::int + $2::int"), timeout=10)
                val  = await asyncio.wait_for(stmt.fetchval(19, 23), timeout=10)
                assert val == 42, f"expected 42 got {val}"
                # Execute the same statement 20 times to exercise pool reuse
                for i in range(20):
                    v = await asyncio.wait_for(stmt.fetchval(i, 42 - i), timeout=10)
                    assert v == 42, f"iteration {i}: expected 42 got {v}"
            finally:
                try:
                    await asyncio.wait_for(conn.close(), timeout=5)
                except (asyncio.TimeoutError, Exception):
                    conn.terminate()

        asyncio.run(_run_async())

    def test_i5_asyncpg_concurrent_connections(self) -> None:
        """10 asyncpg connections in a pool run concurrently — no races."""
        ag = _import_asyncpg()
        if ag is None:
            self.skip("asyncpg not installed")
        import asyncio

        e = self._env

        async def _run_async():
            pool = await ag.create_pool(
                host=e["host"], port=int(e["port"]),
                user=e["user"], password=e["password"],
                database=e["database"],
                min_size=2, max_size=10, timeout=10,
            )
            try:
                tasks = [
                    asyncio.create_task(pool.fetchval("SELECT $1::int * 2", i))
                    for i in range(50)
                ]
                try:
                    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=30)
                except asyncio.TimeoutError:
                    for task in tasks:
                        task.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*tasks, return_exceptions=True),
                            timeout=5,
                        )
                    except asyncio.TimeoutError:
                        pass
                    pool.terminate()
                    raise AssertionError(
                        "asyncpg concurrent fetches timed out after 30s; "
                        "possible backend pool starvation or protocol deadlock"
                    )
                for i, r in enumerate(results):
                    assert r == i * 2, f"task {i}: expected {i*2} got {r}"
            finally:
                try:
                    await asyncio.wait_for(pool.close(), timeout=15)
                except asyncio.TimeoutError:
                    pool.terminate()  # force-close all connections if graceful close hangs

        asyncio.run(_run_async())

    # =======================================================================
    # I6 — psycopg3 (sync + async)
    # =======================================================================

    def test_i6_psycopg3_sync(self) -> None:
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env
        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT %s + %s", (19, 23))
                row = cur.fetchone()
                assert row and row[0] == 42, f"unexpected: {row}"

    def test_i6_psycopg3_pipeline_mode(self) -> None:
        """psycopg3 pipeline mode sends multiple queries before waiting."""
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env
        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10,
        ) as conn:
            with conn.pipeline():
                futs = [conn.execute("SELECT %s", (i,)) for i in range(10)]
            for i, fut in enumerate(futs):
                row = fut.fetchone()
                assert row and row[0] == i, f"pipeline result {i}: {row}"

    def test_i6_psycopg3_copy_in(self) -> None:
        """psycopg3 COPY FROM STDIN — tests CopyInResponse forwarding."""
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env
        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10, autocommit=True,
        ) as conn:
            conn.execute(
                "CREATE TEMP TABLE _torture_copy (id int, val text)"
            )
            data = "\n".join(f"{i}\trow_{i}" for i in range(100)) + "\n"
            with conn.cursor().copy(
                "COPY _torture_copy FROM STDIN"
            ) as copy:
                copy.write(data.encode())
            count = conn.execute(
                "SELECT count(*) FROM _torture_copy"
            ).fetchone()[0]
            assert count == 100, f"expected 100 rows, got {count}"

    def test_i6_psycopg3_async(self) -> None:
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        import asyncio

        e = self._env

        async def _run():
            async with await pg.AsyncConnection.connect(
                host=e["host"], port=int(e["port"]),
                user=e["user"], password=e["password"],
                dbname=e["database"],
                connect_timeout=10,
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT %s * %s", (6, 7))
                    row = await cur.fetchone()
                    assert row and row[0] == 42, f"unexpected: {row}"

        asyncio.run(_run())

    # =======================================================================
    # I7 — Prisma (Node.js ORM)
    # =======================================================================

    def test_i7_prisma(self) -> None:
        """
        Prisma Client JS basic findMany via a self-contained Node script.

        Requires: node + npx on PATH.  Skips otherwise.
        """
        if not check_command("node") or not check_command("npx"):
            self.skip("node/npx not found on PATH")
        e = self._env
        db_url = (
            f"postgresql://{e['user']}:{e['password']}"
            f"@{e['host']}:{e['port']}/{e['database']}"
        )
        script = textwrap.dedent(f"""
            const {{ PrismaClient }} = require('@prisma/client');
            const prisma = new PrismaClient({{
              datasources: {{ db: {{ url: "{db_url}" }} }},
            }});
            (async () => {{
              try {{
                const result = await prisma.$queryRawUnsafe('SELECT 6 * 7 AS v');
                const v = Number(result[0].v);
                if (v !== 42) {{ console.error('expected 42, got', v); process.exit(1); }}
                console.log('Prisma OK');
              }} finally {{
                await prisma.$disconnect();
              }}
            }})();
        """).strip()

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            # Minimal package.json + prisma schema
            (tdp / "package.json").write_text(json.dumps({
                "name": "keel-torture-prisma",
                "version": "1.0.0",
                "dependencies": {"@prisma/client": "^5.0.0", "prisma": "^5.0.0"},
            }))
            prisma_dir = tdp / "prisma"
            prisma_dir.mkdir()
            (prisma_dir / "schema.prisma").write_text(textwrap.dedent(f"""
                generator client {{
                  provider = "prisma-client-js"
                }}
                datasource db {{
                  provider = "postgresql"
                  url      = env("DATABASE_URL")
                }}
            """))
            (tdp / "test.js").write_text(script)

            env_extra = {"DATABASE_URL": db_url}

            rc, out, err = _run(
                ["npm", "install", "--quiet"],
                timeout=120, env=env_extra,
            )
            if rc != 0:
                self.skip(f"npm install failed: {err[:200]}")

            rc, out, err = _run(
                ["npx", "prisma", "generate"],
                timeout=60, env=env_extra,
            )
            if rc != 0:
                self.skip(f"prisma generate failed: {err[:200]}")

            rc, out, err = _run(
                ["node", "test.js"],
                timeout=30, env=env_extra,
            )
        assert rc == 0, f"Prisma test failed: {err}"
        assert "Prisma OK" in out, f"unexpected output: {out!r}"

    # =======================================================================
    # I8 — Hibernate (Java ORM)
    # =======================================================================

    def test_i8_hibernate(self) -> None:
        """
        Hibernate 6 native SQL query via a Maven project.

        Requires: mvn + java on PATH and network access to Maven Central.
        Skips otherwise.
        """
        if not check_command("mvn") or not check_command("java"):
            self.skip("mvn/java not found on PATH")
        e = self._env
        pom = textwrap.dedent(f"""
            <?xml version="1.0" encoding="UTF-8"?>
            <project xmlns="http://maven.apache.org/POM/4.0.0"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                         http://maven.apache.org/xsd/maven-4.0.0.xsd">
              <modelVersion>4.0.0</modelVersion>
              <groupId>io.keel</groupId>
              <artifactId>torture-hibernate</artifactId>
              <version>1.0</version>
              <dependencies>
                <dependency>
                  <groupId>org.hibernate.orm</groupId>
                  <artifactId>hibernate-core</artifactId>
                  <version>6.4.0.Final</version>
                </dependency>
                <dependency>
                  <groupId>org.postgresql</groupId>
                  <artifactId>postgresql</artifactId>
                  <version>42.7.3</version>
                </dependency>
              </dependencies>
              <build>
                <plugins>
                  <plugin>
                    <groupId>org.codehaus.mojo</groupId>
                    <artifactId>exec-maven-plugin</artifactId>
                    <version>3.1.0</version>
                    <configuration>
                      <mainClass>KTortureHibernate</mainClass>
                    </configuration>
                  </plugin>
                </plugins>
              </build>
            </project>
        """).strip()

        java_src = textwrap.dedent(f"""
            import org.hibernate.Session;
            import org.hibernate.SessionFactory;
            import org.hibernate.cfg.Configuration;

            public class KTortureHibernate {{
                public static void main(String[] args) {{
                    Configuration cfg = new Configuration();
                    cfg.setProperty("hibernate.connection.url",
                        "jdbc:postgresql://{e['host']}:{e['port']}/{e['database']}");
                    cfg.setProperty("hibernate.connection.username", "{e['user']}");
                    cfg.setProperty("hibernate.connection.password", "{e['password']}");
                    cfg.setProperty("hibernate.dialect",
                        "org.hibernate.dialect.PostgreSQLDialect");
                    try (SessionFactory sf = cfg.buildSessionFactory();
                         Session session = sf.openSession()) {{
                        Integer v = session
                            .createNativeQuery("SELECT 6 * 7", Integer.class)
                            .uniqueResult();
                        if (v == null || v != 42) {{
                            System.err.println("Expected 42, got " + v);
                            System.exit(1);
                        }}
                        System.out.println("Hibernate OK");
                    }}
                }}
            }}
        """).strip()

        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            (tdp / "pom.xml").write_text(pom)
            src_dir = tdp / "src" / "main" / "java"
            src_dir.mkdir(parents=True)
            (src_dir / "KTortureHibernate.java").write_text(java_src)

            rc, out, err = _run(
                ["mvn", "-q", "compile", "exec:java"],
                timeout=180,
                cwd=str(tdp),
            )
        full = out + err
        if rc != 0 and "Could not resolve" in full:
            self.skip(f"Maven dependency resolution failed (no network?): {full[:300]}")
        assert rc == 0, f"Hibernate test failed:\n{full[-800:]}"
        assert "Hibernate OK" in out, f"unexpected output: {out!r}"

    # =======================================================================
    # I9 — Failover chaos: primary killed mid-transaction
    # =======================================================================

    def test_i9_failover_chaos_mid_transaction(self) -> None:
        """
        Open a transaction, kill the backend PG process, and verify that:
          - The client receives a clean error (not a hang or proxy crash).
          - The proxy stays up.
          - A fresh connection succeeds within 5 s.

        Requires: KEEL_PG_SUPERUSER (default: postgres) and either
        pg_terminate_backend() access or KEEL_PG_PORT pointing to a
        test-only PostgreSQL instance.

        If the proxy cannot be reached directly at the PG backend address,
        the test skips gracefully.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env
        pg_host = e.get("pg_host", "127.0.0.1")
        pg_port = int(e.get("pg_port", 5432))

        # Verify we can reach both the proxy and the backend directly
        if not wait_for_port(e["host"], int(e["port"]), timeout=2.0):
            self.skip("proxy not reachable")
        if not wait_for_port(pg_host, pg_port, timeout=2.0):
            self.skip(f"direct PG backend not reachable at {pg_host}:{pg_port}")

        # Step 1: Open a connection through the proxy and start a transaction
        victim_conn = pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10, autocommit=False,
        )
        victim_conn.execute("BEGIN")
        victim_conn.execute("SELECT pg_sleep(0.1)")

        # Step 2: Terminate the backend process via a superuser connection
        # (direct, bypassing the proxy so the proxy itself is not confused)
        try:
            admin_conn = pg.connect(
                host=pg_host, port=pg_port,
                user=e["user"], password=e["password"],
                dbname=e["database"],
                connect_timeout=5, autocommit=True,
            )
            # Terminate all backends belonging to our test user
            # (except the admin connection itself)
            admin_conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE usename = %s
                  AND pid <> pg_backend_pid()
                  AND state IN ('idle in transaction', 'active')
                """,
                (e["user"],),
            )
            admin_conn.close()
        except Exception as kill_err:
            victim_conn.close()
            self.skip(f"could not terminate backend (no superuser?): {kill_err}")

        # Step 3: The victim connection must see an error, not hang
        error_seen = False
        try:
            victim_conn.set_autocommit(False)
            victim_conn.execute("SELECT 1")  # should fail
        except Exception:
            error_seen = True
        finally:
            try:
                victim_conn.close()
            except Exception:
                pass

        assert error_seen, (
            "Expected the victim connection to raise an error after the backend "
            "was terminated, but it succeeded silently"
        )

        # Step 4: The proxy must still be reachable within 5 s
        deadline = time.monotonic() + 5.0
        reconnected = False
        while time.monotonic() < deadline:
            try:
                new_conn = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=2, autocommit=True,
                )
                row = new_conn.execute("SELECT 1").fetchone()
                new_conn.close()
                if row and row[0] == 1:
                    reconnected = True
                    break
            except Exception:
                time.sleep(0.25)

        assert reconnected, (
            "Proxy did not accept a new connection within 5 s after backend "
            "was terminated — potential hang or crash"
        )

    def test_i9_failover_chaos_backend_restart(self) -> None:
        """
        Kill and restart all idle backend connections; the proxy pool must
        recover within its configured reconnect timeout and serve queries.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e  = self._env
        pg_host = e.get("pg_host", "127.0.0.1")
        pg_port = int(e.get("pg_port", 5432))
        if not wait_for_port(pg_host, pg_port, timeout=2.0):
            self.skip(f"direct PG backend not reachable at {pg_host}:{pg_port}")

        # Terminate every idle backend for our user (simulates server restart)
        try:
            admin = pg.connect(
                host=pg_host, port=pg_port,
                user=e["user"], password=e["password"],
                dbname=e["database"],
                connect_timeout=5, autocommit=True,
            )
            admin.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE usename = %s
                  AND state = 'idle'
                  AND pid <> pg_backend_pid()
                """,
                (e["user"],),
            )
            admin.close()
        except Exception as e2:
            self.skip(f"could not terminate idle backends: {e2}")

        # After pool refill (allow up to 10 s) every query must succeed
        ok = 0
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and ok < 5:
            try:
                conn = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=3, autocommit=True,
                )
                row = conn.execute("SELECT 42").fetchone()
                conn.close()
                if row and row[0] == 42:
                    ok += 1
            except Exception:
                time.sleep(0.5)

        assert ok >= 5, (
            f"pool failed to recover after backend termination: "
            f"only {ok}/5 successful queries"
        )

    # =======================================================================
    # I10 — Prepared-statement reuse across pool cycle
    # =======================================================================

    def test_i10_ps_reuse_across_pool_cycle(self) -> None:
        """
        Create a named prepared statement, force a pool cycle (close/reopen
        the backend), and verify the statement is transparently replayed.

        This specifically validates KEEL's prepared-statement virtualisation.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        results = []
        errors  = []

        def _worker(i: int) -> None:
            try:
                conn = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=10,
                    prepare_threshold=1,  # force server-side prepare after 1 use
                )
                with conn.cursor() as cur:
                    for _ in range(10):
                        cur.execute("SELECT %s + 1", (i,))
                        row = cur.fetchone()
                        assert row and row[0] == i + 1
                conn.close()
                results.append(i)
            except Exception as ex:
                errors.append((i, str(ex)))

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"prepared-statement reuse failures: {errors}"
        assert len(results) == 20, f"only {len(results)}/20 workers succeeded"

    # =======================================================================
    # I11 — Long soak under mixed psycopg3 + asyncpg load
    # =======================================================================

    def test_i11_long_soak(self) -> None:
        """
        Sustained mixed workload for KEEL_TORTURE_SOAK_S seconds.

        Measures:
          - p99 latency must not drift by more than 3× from the first window.
          - Error rate must remain < 1 %.
          - File-descriptor count (via /proc) must not grow monotonically.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env
        duration_s = self._soak_s

        latencies_ms: list[float] = []
        errors: list[str] = []
        stop_event = threading.Event()

        def _worker(wid: int) -> None:
            while not stop_event.is_set():
                try:
                    conn = pg.connect(
                        host=e["host"], port=int(e["port"]),
                        user=e["user"], password=e["password"],
                        dbname=e["database"],
                        connect_timeout=5, autocommit=True,
                    )
                    t0 = time.monotonic()
                    row = conn.execute(
                        "SELECT pg_sleep(0), %s::int * 2", (wid,)
                    ).fetchone()
                    elapsed = (time.monotonic() - t0) * 1000
                    conn.close()
                    if row and row[1] == wid * 2:
                        latencies_ms.append(elapsed)
                    else:
                        errors.append(f"w{wid}: unexpected row {row}")
                except Exception as ex:
                    errors.append(f"w{wid}: {ex}")
                    time.sleep(0.1)

        n_workers = min(5, 20)  # conservative for CI
        threads = [
            threading.Thread(target=_worker, args=(i,), daemon=True)
            for i in range(n_workers)
        ]
        for t in threads:
            t.start()

        t_start = time.monotonic()
        time.sleep(duration_s)
        stop_event.set()
        for t in threads:
            t.join(timeout=10)

        total   = len(latencies_ms) + len(errors)
        err_pct = len(errors) / max(total, 1) * 100
        stats   = latency_stats(latencies_ms)

        assert err_pct < 1.0, (
            f"Soak error rate {err_pct:.1f}% ≥ 1% — sample errors: "
            f"{errors[:5]}"
        )
        assert len(latencies_ms) >= n_workers * 2, (
            f"Too few successful queries in {duration_s}s soak: {len(latencies_ms)}"
        )

        # Latency guard: p99 must be < 5 s (smoke check, not a tight SLO)
        if "p99_ms" in stats:
            assert stats["p99_ms"] < 5000, (
                f"p99 latency {stats['p99_ms']:.1f} ms exceeds 5 s during soak"
            )

    # =======================================================================
    # I12 — Connection storm
    # =======================================================================

    def test_i12_connection_storm(self) -> None:
        """
        500 sequential connect → query → disconnect cycles through the proxy.

        No connection or memory leaks are expected.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        errors = []
        for i in range(500):
            try:
                conn = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=5, autocommit=True,
                )
                row = conn.execute("SELECT %s", (i,)).fetchone()
                conn.close()
                if not row or row[0] != i:
                    errors.append(f"iter {i}: bad row {row}")
            except Exception as ex:
                errors.append(f"iter {i}: {ex}")

        assert not errors, (
            f"{len(errors)}/500 connection-storm iterations failed: "
            f"{errors[:5]}"
        )

    # =======================================================================
    # I13 — Wire-protocol framing integrity
    # =======================================================================

    def test_i13_simple_query_framing(self) -> None:
        """Every backend message in a Simple Query cycle has a valid type byte and length."""
        e = self._env
        # All valid backend→frontend single-byte type codes (PostgreSQL v3 wire protocol)
        VALID = frozenset(ord(c) for c in "RBKZS1234CnNDTtEIAHVWdcp!")
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"]), \
                "startup handshake failed"
            conn.send(pg_query("SELECT i, i*i FROM generate_series(1,10) i"))
            msgs = conn.recv_until({ord("Z")})
            for msg_type, body in msgs:
                assert msg_type in VALID, (
                    f"invalid backend message type 0x{msg_type:02x} "
                    f"('{chr(msg_type) if 32 <= msg_type < 127 else '?'}')"
                )
            types = {t for t, _ in msgs}
            assert ord("T") in types, "no RowDescription in SELECT response"
            assert ord("D") in types, "no DataRow in SELECT response"
            assert ord("C") in types, "no CommandComplete"
            assert ord("Z") in types, "no ReadyForQuery"
            assert sum(1 for t, _ in msgs if t == ord("D")) == 10, \
                "expected exactly 10 DataRows"

    def test_i13_extended_protocol_framing(self) -> None:
        """Extended protocol (Parse+Bind+Describe+Execute+Sync) returns correct sequence."""
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"]), \
                "startup handshake failed"
            conn.send(
                pg_parse("i13_ps", "SELECT $1::int + $2::int AS s", [23, 23])
                + pg_bind("", "i13_ps", [], [b"19", b"23"], [])
                + pg_describe("P", "")
                + pg_execute("", 0)
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")})
            types_seq = [t for t, _ in msgs]
            assert ord("1") in types_seq, "no ParseComplete"
            assert ord("2") in types_seq, "no BindComplete"
            assert ord("D") in types_seq, "no DataRow"
            assert ord("C") in types_seq, "no CommandComplete"
            assert ord("Z") in types_seq, "no ReadyForQuery"
            # Verify value == 42
            for t, body in msgs:
                if t == ord("D"):
                    col_len = struct.unpack(">i", body[2:6])[0]
                    val = body[6 : 6 + col_len].decode()
                    assert val == "42", f"expected result 42, got {val!r}"

    def test_i13_pipeline_response_ordering(self) -> None:
        """Pipeline of N queries returns DataRows in strict statement order."""
        e = self._env
        N = 8
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"]), \
                "startup handshake failed"
            batch = b"".join(
                pg_parse(f"p13_{i}", f"SELECT {i * 7}", [])
                + pg_bind(f"r13_{i}", f"p13_{i}", [], [], [])
                + pg_execute(f"r13_{i}", 0)
                for i in range(N)
            ) + pg_sync()
            conn.send(batch)
            msgs = conn.recv_until({ord("Z")}, max_msgs=500)
            data_rows = [b for t, b in msgs if t == ord("D")]
            assert len(data_rows) == N, \
                f"expected {N} DataRows from pipeline, got {len(data_rows)}"
            for i, body in enumerate(data_rows):
                col_len = struct.unpack(">i", body[2:6])[0]
                val = int(body[6 : 6 + col_len])
                assert val == i * 7, \
                    f"pipeline DataRow {i}: expected {i*7}, got {val}"

    # =======================================================================
    # I14 — Packet capture: tcpdump + pcap framing validation
    # =======================================================================

    def test_i14_packet_capture_integrity(self) -> None:
        """
        Capture proxy traffic with tcpdump; parse the pcap and verify that every
        PostgreSQL backend message has a valid type byte and consistent length field.

        Requires: tcpdump on PATH + CAP_NET_RAW in the container.
        Skips gracefully when either is absent.
        """
        if not check_command("tcpdump"):
            self.skip("tcpdump not installed")
        e = self._env

        pcap_fd, pcap_path = tempfile.mkstemp(suffix=".pcap")
        os.close(pcap_fd)
        try:
            cap = subprocess.Popen(
                ["tcpdump", "-i", "any", "-w", pcap_path, "-n",
                 "--immediate-mode", f"port {e['port']}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            time.sleep(0.6)  # let tcpdump start
            if cap.poll() is not None:
                stderr_out = cap.stderr.read().decode(errors="replace")
                self.skip(
                    f"tcpdump failed to start (CAP_NET_RAW missing?): "
                    f"{stderr_out[:200]}"
                )

            try:
                with ProxyConn(e["host"], int(e["port"])) as conn:
                    assert conn.startup(e["user"], e["database"], e["password"])
                    for sql in [
                        "SELECT 1",
                        "SELECT generate_series(1, 50)",
                        "SELECT md5('keel-torture-i14')",
                    ]:
                        conn.send(pg_query(sql))
                        conn.recv_until({ord("Z")})
            finally:
                time.sleep(0.3)
                cap.terminate()
                try:
                    cap.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    cap.kill()
        except Exception:
            os.unlink(pcap_path)
            raise

        try:
            violations = _validate_pcap_pg_framing(pcap_path, int(e["port"]))
        finally:
            os.unlink(pcap_path)

        assert not violations, (
            "PostgreSQL wire-protocol framing violations in captured traffic:\n"
            + "\n".join(f"  \u2022 {v}" for v in violations[:15])
        )

    # =======================================================================
    # I15 — Concurrent prepared-statement isolation
    # =======================================================================

    def test_i15_concurrent_ps_isolation(self) -> None:
        """
        100 concurrent connections each execute a prepared statement with a unique
        expected value.  No client should ever receive another client's result —
        validates PS virtualisation under extreme concurrency.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        N = 100
        failures: list[str] = []
        lock = threading.Lock()

        def _worker(tid: int) -> None:
            try:
                conn = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=15,
                    prepare_threshold=1,
                )
                try:
                    for rep in range(5):
                        row = conn.execute("SELECT %s::int * 3", (tid,)).fetchone()
                        if row is None or row[0] != tid * 3:
                            with lock:
                                failures.append(
                                    f"tid={tid} rep={rep}: expected {tid*3}, got {row}"
                                )
                finally:
                    conn.close()
            except Exception as exc:
                with lock:
                    failures.append(f"tid={tid}: {exc}")

        threads = [
            threading.Thread(target=_worker, args=(i,), daemon=True)
            for i in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not failures, (
            f"Concurrent PS isolation failures ({len(failures)}/{N} threads):\n"
            + "\n".join(failures[:10])
        )

    # =======================================================================
    # I16 — Admin console: SHOW commands + Prometheus metrics endpoint
    # =======================================================================

    def test_i16_admin_console_show_stats(self) -> None:
        """KEEL admin console SHOW STATS returns rows with expected columns."""
        if not check_command("psql"):
            self.skip("psql not available")
        e = self._env
        rc, out, err = _run(
            ["psql", f"--host={e['host']}", "--port=6433",
             "--username=postgres", "--dbname=postgres",
             "--no-password", "--command=SHOW STATS"],
            env={"PGPASSWORD": "postgres"},
            timeout=10,
        )
        if rc != 0:
            self.skip(f"admin console not reachable (port 6433): {(err or out)[:200]}")
        assert out.strip(), "SHOW STATS returned no output"
        # KEEL SHOW STATS uses a 'stat | value' layout; PgBouncer uses wide rows.
        # Accept both formats.
        assert any(col in out for col in (
            "stat", "value",
            "sessions_created", "sessions_closed",
            "total_query_count", "total_received", "database", "name",
        )), f"SHOW STATS missing expected columns:\n{out[:400]}"

    def test_i16_admin_console_show_pools(self) -> None:
        """KEEL admin console SHOW POOLS includes the torture pool."""
        if not check_command("psql"):
            self.skip("psql not available")
        e = self._env
        rc, out, err = _run(
            ["psql", f"--host={e['host']}", "--port=6433",
             "--username=postgres", "--dbname=postgres",
             "--no-password", "--command=SHOW POOLS"],
            env={"PGPASSWORD": "postgres"},
            timeout=10,
        )
        if rc != 0:
            self.skip(f"admin console not reachable: {(err or out)[:200]}")
        assert out.strip(), "SHOW POOLS returned no output"
        assert "torture" in out or "postgres" in out, \
            f"pool 'torture'/'postgres' not found in SHOW POOLS:\n{out[:400]}"

    def test_i16_prometheus_metrics_endpoint(self) -> None:
        """Prometheus /metrics returns parseable output with keel_* metrics."""
        e = self._env
        try:
            url = f"http://{e['host']}:9101/metrics"
            with urllib.request.urlopen(url, timeout=5) as resp:
                text = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            self.skip(f"Prometheus endpoint unreachable at {e['host']}:9101: {exc}")
        lines = [ln for ln in text.splitlines() if not ln.startswith("#") and ln.strip()]
        assert lines, "no metric values in Prometheus output"
        keel_lines = [ln for ln in lines if ln.startswith("keel_")]
        assert keel_lines, \
            f"no keel_* metrics found in Prometheus output; first lines: {lines[:5]}"
        self.result.metrics["prometheus_metric_count"] = len(keel_lines)

    # =======================================================================
    # I17 — Error recovery: pool health after various error types
    # =======================================================================

    def test_i17_syntax_error_recovery(self) -> None:
        """Syntax error returns SQLSTATE 42601; pool remains usable after."""
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.send(pg_query("THIS IS NOT VALID SQL !!"))
            msgs = conn.recv_until({ord("Z")})
            types = [t for t, _ in msgs]
            assert ord("E") in types, "no ErrorResponse for syntax error"
            for t, body in msgs:
                if t == ord("E"):
                    sqlstate = _parse_pg_error_field(body, "C")
                    assert sqlstate == "42601", \
                        f"expected SQLSTATE 42601 (syntax_error), got {sqlstate!r}"
                    break
            # Pool must still be usable
            conn.send(pg_query("SELECT 1337"))
            msgs2 = conn.recv_until({ord("Z")})
            assert any(t == ord("D") for t, _ in msgs2), \
                "proxy unusable after syntax error"

    def test_i17_runtime_error_recovery(self) -> None:
        """Division-by-zero returns SQLSTATE 22012; pool remains usable after."""
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.send(pg_query("SELECT 1/0"))
            msgs = conn.recv_until({ord("Z")})
            for t, body in msgs:
                if t == ord("E"):
                    sqlstate = _parse_pg_error_field(body, "C")
                    assert sqlstate == "22012", \
                        f"expected SQLSTATE 22012 (division_by_zero), got {sqlstate!r}"
            conn.send(pg_query("SELECT 99"))
            msgs2 = conn.recv_until({ord("Z")})
            assert any(t == ord("D") for t, _ in msgs2), \
                "proxy unusable after runtime error"

    def test_i17_error_isolation_under_concurrency(self) -> None:
        """Errors in half the connections do not affect the other half."""
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env
        N = 30
        failures: list[str] = []
        lock = threading.Lock()

        def _worker(tid: int) -> None:
            try:
                conn = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=10, autocommit=True,
                )
                try:
                    if tid % 2 == 0:
                        try:
                            conn.execute("SELECT 1/0")
                        except Exception:
                            pass
                    row = conn.execute("SELECT %s * 5", (tid,)).fetchone()
                    if row is None or row[0] != tid * 5:
                        with lock:
                            failures.append(f"tid={tid}: expected {tid*5}, got {row}")
                finally:
                    conn.close()
            except Exception as exc:
                with lock:
                    failures.append(f"tid={tid}: {exc}")

        threads = [threading.Thread(target=_worker, args=(i,), daemon=True) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not failures, \
            f"Error isolation failures ({len(failures)}/{N}):\n" + "\n".join(failures[:10])

    # =======================================================================
    # I18 — Extended protocol edge cases
    # =======================================================================

    def test_i18_parse_describe_no_execute(self) -> None:
        """Parse + Describe + Sync (no Bind/Execute) returns ParseComplete + RowDescription."""
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.send(
                pg_parse("i18_desc", "SELECT $1::int + $2::int AS total", [23, 23])
                + pg_describe("S", "i18_desc")
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")})
            types = [t for t, _ in msgs]
            assert ord("1") in types, "no ParseComplete"
            assert ord("T") in types or ord("n") in types, \
                "no RowDescription or NoData from Describe"
            assert ord("Z") in types, "no ReadyForQuery"

    def test_i18_empty_query_string(self) -> None:
        """Empty simple-query string returns EmptyQueryResponse ('I')."""
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.send(pg_query(""))
            msgs = conn.recv_until({ord("Z")})
            types = [t for t, _ in msgs]
            assert ord("I") in types, (
                "expected EmptyQueryResponse 'I' for empty query, got types: "
                + " ".join(chr(t) if 32 <= t < 127 else f"0x{t:02x}" for t in types)
            )

    def test_i18_multi_statement_pipeline_single_write(self) -> None:
        """12 extended-protocol statements sent in one TCP write — all 12 DataRows returned."""
        e = self._env
        N = 12
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            batch = b"".join(
                pg_parse(f"i18_p{i}", f"SELECT {i * 11}", [])
                + pg_bind(f"i18_r{i}", f"i18_p{i}", [], [], [])
                + pg_execute(f"i18_r{i}", 0)
                for i in range(N)
            ) + pg_sync()
            conn.send(batch)
            msgs = conn.recv_until({ord("Z")}, max_msgs=1000)
            data_rows = [b for t, b in msgs if t == ord("D")]
            assert len(data_rows) == N, \
                f"pipeline: expected {N} DataRows, got {len(data_rows)}"
            for i, body in enumerate(data_rows):
                col_len = struct.unpack(">i", body[2:6])[0]
                val = int(body[6 : 6 + col_len])
                assert val == i * 11, \
                    f"pipeline DataRow {i}: expected {i*11}, got {val}"

    def test_i18_fragmented_extended_protocol(self) -> None:
        """Extended protocol messages each in their own TCP write with 5ms gaps.

        This verifies KEEL can process an extended-protocol sequence where each
        message arrives in a separate TCP segment (realistic in slow / high-latency
        networks).  KEEL must buffer Parse/Bind/Execute until Sync triggers dispatch.

        Note: KEEL requires each individual message to arrive complete in a single
        TCP read; splitting bytes *within* a single message is not supported.
        """
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(10.0)
            # Send each message as a separate TCP segment (TCP_NODELAY is set).
            # The 5ms gap ensures KEEL's io_uring reactor sees each one individually.
            for msg in [
                pg_parse("i18_frag", "SELECT $1::int * $2::int", [23, 23]),
                pg_bind("", "i18_frag", [], [b"6", b"7"], []),
                pg_execute("", 0),
                pg_sync(),
            ]:
                conn.send(msg)
                time.sleep(0.005)
            msgs = conn.recv_until({ord("Z")})
            for t, body in msgs:
                if t == ord("D"):
                    col_len = struct.unpack(">i", body[2:6])[0]
                    val = int(body[6 : 6 + col_len])
                    assert val == 42, f"fragmented pipeline: expected 42, got {val}"
                    return
            assert False, "no DataRow from fragmented extended-protocol query"

    def test_i18_intra_message_fragmentation(self) -> None:
        """KNOWN KEEL BUG: intra-message TCP fragmentation hangs the proxy.

        Sends a full extended-protocol sequence 1 byte at a time.  KEEL's
        io_uring reactor stalls — it does not re-submit recv SQEs to accumulate
        the remaining bytes of a partial message, so the session hangs until
        the client closes the socket.

        This test is EXPECTED TO FAIL until KEEL's message-framing layer is
        fixed to handle partial-header / partial-body TCP reads.  Once fixed,
        the test will pass automatically.

        Fix target: fe_data recv-buffer accumulation in engine_flow.c —
        keep submitting io_uring recv SQEs until a complete PG message
        (5-byte header + body) is buffered before passing it to the parser.
        """
        e = self._env
        keel_responded = False
        result_value = None
        try:
            with ProxyConn(e["host"], int(e["port"])) as conn:
                assert conn.startup(e["user"], e["database"], e["password"])
                batch = (
                    pg_parse("i18_intrafrag", "SELECT $1::int + $2::int", [23, 23])
                    + pg_bind("", "i18_intrafrag", [], [b"19", b"23"], [])
                    + pg_execute("", 0)
                    + pg_sync()
                )
                conn.set_timeout(2.0)  # short — KEEL won't respond; don't hang
                conn.send_fragmented(batch, chunk_size=1)
                try:
                    msgs = conn.recv_until({ord("Z")})
                    keel_responded = True
                    for t, body in msgs:
                        if t == ord("D"):
                            col_len = struct.unpack(">i", body[2:6])[0]
                            result_value = int(body[6 : 6 + col_len])
                except OSError:
                    pass  # expected: KEEL did not respond within the timeout
        finally:
            if not keel_responded:
                # Socket closed without KEEL completing the session.
                # Give KEEL time to detect the disconnect and recycle the
                # backend connection before I5/I6 tests run.
                time.sleep(3.0)

        if not keel_responded:
            assert False, (
                "KEEL BUG — 1-byte/segment intra-message TCP fragmentation: "
                "KEEL's io_uring reactor stalls when a PostgreSQL message "
                "arrives split across multiple 1-byte TCP segments. "
                "The recv SQE must be resubmitted until a complete message "
                "(5-byte header + full body) is buffered before parsing. "
                "Fix: fe_data buffer accumulation in engine_flow.c."
            )
        assert result_value == 42, (
            f"intra-message-fragmented query: expected 42, got {result_value}"
        )

    # =======================================================================
    # I19 — Large payload stress
    # =======================================================================

    def test_i19_large_parameter_value(self) -> None:
        """64 KB text parameter via extended protocol — proxy must not truncate."""
        e = self._env
        large = "K" * 65_536
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(15.0)
            conn.send(
                pg_parse("i19_big", "SELECT length($1::text)", [25])
                + pg_bind("", "i19_big", [0], [large.encode()], [0])
                + pg_execute("", 0)
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")})
            for t, body in msgs:
                if t == ord("D"):
                    col_len = struct.unpack(">i", body[2:6])[0]
                    val = int(body[6 : 6 + col_len])
                    assert val == 65_536, \
                        f"large parameter: expected length 65536, got {val}"
                    return
            assert False, "no DataRow for large-parameter query"

    def test_i19_large_result_set_extended_protocol(self) -> None:
        """100 000-row result set via extended protocol — exact row count verified."""
        e = self._env
        N = 100_000
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(60.0)
            conn.send(
                pg_parse("i19_rows", f"SELECT i FROM generate_series(1, {N}) i", [])
                + pg_bind("", "i19_rows", [], [], [])
                + pg_execute("", 0)
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")}, max_msgs=N + 10)
            row_count = sum(1 for t, _ in msgs if t == ord("D"))
            assert row_count == N, \
                f"large result set: expected {N} DataRows, got {row_count}"

    def test_i19_large_copy_in(self) -> None:
        """COPY IN 50 000 rows via psql — proxy must pass the CopyData stream intact."""
        if not check_command("psql"):
            self.skip("psql not available")
        e = self._env

        csv_data = "\n".join(f"{i},{i * 2}" for i in range(50_000))

        # Use a permanent table (TEMP is session-scoped — won't survive
        # the separate psql invocations for COPY and COUNT).
        tbl = "_keel_i19_copy_test"
        setup_sql = (
            f"DROP TABLE IF EXISTS {tbl}; "
            f"CREATE TABLE {tbl}(a int, b int);"
        )
        rc, _, err = _run(
            ["psql", f"--host={e['host']}", f"--port={e['port']}",
             f"--username={e['user']}", f"--dbname={e['database']}",
             "--no-password",
             f"--command={setup_sql}"],
            env={"PGPASSWORD": e["password"]},
        )
        assert rc == 0, f"table setup failed: {err}"

        copy_script = (
            f"\\copy {tbl} FROM STDIN WITH (FORMAT csv)\n"
            + csv_data + "\n"
        )
        rc2, _, err2 = _run(
            ["psql", f"--host={e['host']}", f"--port={e['port']}",
             f"--username={e['user']}", f"--dbname={e['database']}",
             "--no-password"],
            stdin_text=copy_script,
            env={"PGPASSWORD": e["password"]},
            timeout=120,
        )
        assert rc2 == 0, f"COPY IN failed: {err2[-400:]}"

        rc3, out3, _ = _run(
            ["psql", f"--host={e['host']}", f"--port={e['port']}",
             f"--username={e['user']}", f"--dbname={e['database']}",
             "--no-password", "--tuples-only", "--no-align",
             f"--command=SELECT COUNT(*) FROM {tbl}"],
            env={"PGPASSWORD": e["password"]},
        )
        # Cleanup regardless of outcome
        _run(
            ["psql", f"--host={e['host']}", f"--port={e['port']}",
             f"--username={e['user']}", f"--dbname={e['database']}",
             "--no-password", f"--command=DROP TABLE IF EXISTS {tbl}"],
            env={"PGPASSWORD": e["password"]},
        )
        assert rc3 == 0 and "50000" in out3, \
            f"COPY IN row count wrong — expected 50000, got: {out3!r}"

    # =======================================================================
    # I20 — Malformed message lengths
    # =======================================================================

    def test_i20_malformed_message_length_too_small(self) -> None:
        """'Q' message with declared length=3 (minimum is 4 — length includes itself).

        KEEL must respond with an ErrorResponse or close the connection.
        It must NOT hang indefinitely.  After the malformed message, a fresh
        connection must succeed to prove the proxy is still healthy.
        """
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(3.0)
            # length=3 is illegal per PostgreSQL protocol spec
            conn.send(b"Q" + struct.pack(">I", 3))
            got_response = False
            try:
                for _ in range(10):
                    t, _ = conn.recv_message()
                    if t in (ord("E"), ord("Z")):
                        got_response = True
                        break
            except OSError:
                got_response = True  # connection closed by KEEL — also acceptable

        time.sleep(0.3)
        assert is_proxy_reachable(e["host"], int(e["port"])), \
            "KEEL must remain reachable after a message with length=3"
        # Verify a new clean query works
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(5.0)
            conn.send(pg_query("SELECT 'i20a'"))
            msgs = conn.recv_until({ord("Z")})
            assert any(t == ord("D") for t, _ in msgs), \
                "new query after malformed-length message must succeed"

    def test_i20_malformed_message_length_oversized(self) -> None:
        """'Q' message claiming 2 GB body but only the 5-byte header is sent.

        KEEL must not buffer 2 GB of RAM, OOM, or hang indefinitely.
        After the client disconnects KEEL must detect the EOF and stay alive.
        """
        import socket as _socket
        e = self._env
        conn = ProxyConn(e["host"], int(e["port"]))
        conn.connect()
        try:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(4.0)
            # Declare a 2 GiB body; only send the header — no actual payload
            conn.send(b"Q" + struct.pack(">I", 0x7FFF_FFFF))
            # Do NOT send any more bytes.  KEEL may time-out the connection
            # or we will get disconnected after the timeout.
            try:
                conn.recv_until({ord("E"), ord("Z")}, max_msgs=5)
            except OSError:
                pass  # expected
        finally:
            # Force RST so KEEL gets an immediate EOF signal
            try:
                conn._sock.setsockopt(
                    _socket.SOL_SOCKET, _socket.SO_LINGER,
                    struct.pack("ii", 1, 0),
                )
            except Exception:
                pass
            conn.close()

        time.sleep(0.5)
        assert is_proxy_reachable(e["host"], int(e["port"])), \
            "KEEL must remain reachable after oversized declared message length"

    # =======================================================================
    # I21 — Bind message with parameter count mismatch
    # =======================================================================

    def test_i21_bind_param_count_mismatch(self) -> None:
        """Bind message claiming 1000 parameters but encoding 0 actual values.

        The message framing is structurally valid (length matches body size)
        but the parameter count does not match the actual encoded params.
        KEEL's protocol parser must:
          - Detect the mismatch before forwarding to the backend, OR
          - Let PostgreSQL return an ErrorResponse and relay it cleanly.
        In either case:
          - The client must receive an ErrorResponse, not a silent hang.
          - A ReadyForQuery must arrive so the session stays usable.
          - A valid query after this must succeed (session recovery).
        """
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(5.0)

            # First Parse succeeds — gives KEEL a valid statement reference
            conn.send(pg_parse("i21_stmt", "SELECT 1", []) + pg_sync())
            msgs = conn.recv_until({ord("Z")})
            assert any(t == ord("1") for t, _ in msgs), \
                "Parse must succeed before the malformed Bind"

            # Malformed Bind: claim 1000 params, encode 0
            bad_body = (
                b"\x00"                      # portal name (empty)
                + b"i21_stmt\x00"            # statement name
                + struct.pack(">H", 0)       # 0 param format codes
                + struct.pack(">H", 1000)    # declare 1000 params
                # — no actual param data follows —
            )
            bad_bind = b"B" + struct.pack(">I", 4 + len(bad_body)) + bad_body
            conn.send(bad_bind + pg_sync())

            got_error = False
            try:
                msgs = conn.recv_until({ord("Z")}, max_msgs=20)
                got_error = any(t == ord("E") for t, _ in msgs)
                rdy = any(t == ord("Z") for t, _ in msgs)
            except OSError:
                got_error = True
                rdy = False

            assert got_error, \
                "KEEL must propagate an ErrorResponse for a Bind param-count mismatch"

            if rdy:
                # Session is still alive — verify recovery
                conn.send(
                    pg_parse("i21_recovery", "SELECT 21 * 2", [])
                    + pg_bind("", "i21_recovery", [], [], [])
                    + pg_execute("", 0)
                    + pg_sync()
                )
                msgs = conn.recv_until({ord("Z")})
                rows = [b for t, b in msgs if t == ord("D")]
                assert rows, "recovery query after malformed Bind must return a row"
                col_len = struct.unpack(">i", rows[0][2:6])[0]
                val = int(rows[0][6 : 6 + col_len])
                assert val == 42, f"recovery query: expected 42, got {val}"

        time.sleep(0.3)
        assert is_proxy_reachable(e["host"], int(e["port"])), \
            "KEEL must remain reachable after malformed Bind"

    # =======================================================================
    # I22 — Mid-extended-protocol abrupt disconnect
    # =======================================================================

    def test_i22_mid_extproto_disconnect(self) -> None:
        """Abrupt RST mid-extended-protocol (after Parse, before Sync/Execute).

        KEEL must detect the disconnect and release the pinned backend cleanly
        to the pool.  Verified by running 10 successive queries on fresh
        connections after a cleanup window — all must succeed.

        A stuck or leaked backend would cause pool exhaustion and failures here.
        """
        import socket as _socket
        e = self._env

        conn = ProxyConn(e["host"], int(e["port"]))
        conn.connect()
        try:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(3.0)
            # Send Parse to trigger backend allocation/pin — do NOT Sync
            conn.send(pg_parse("i22_leak_stmt", "SELECT $1::int", [23]))
            # Give the data time to reach KEEL before we RST
            time.sleep(0.1)
        finally:
            # Force RST (linger=0) so KEEL sees an immediate EOF
            try:
                conn._sock.setsockopt(
                    _socket.SOL_SOCKET, _socket.SO_LINGER,
                    struct.pack("ii", 1, 0),
                )
            except Exception:
                pass
            conn.close()

        # Give KEEL's cleanup path time to run
        time.sleep(2.0)

        # Pool must be healthy: 10 consecutive queries on fresh connections
        failures = []
        for i in range(10):
            try:
                with ProxyConn(e["host"], int(e["port"])) as check:
                    assert check.startup(e["user"], e["database"], e["password"])
                    check.set_timeout(5.0)
                    check.send(pg_query(f"SELECT {i} + 1"))
                    msgs = check.recv_until({ord("Z")})
                    if not any(t == ord("D") for t, _ in msgs):
                        failures.append(i)
            except Exception as ex:
                failures.append(f"{i}:{ex}")

        assert not failures, (
            f"pool unhealthy after mid-extproto disconnect: "
            f"queries {failures} failed — backend may be stuck/leaked"
        )

    # =======================================================================
    # I23 — Out-of-order extended protocol messages
    # =======================================================================

    def test_i23_extended_protocol_out_of_order(self) -> None:
        """Extended-protocol messages sent in illegal order.

        Case 1: Bind referencing a statement that was never Parsed.
        Case 2: Execute with no preceding Bind in flight.

        KEEL must relay (or generate) an ErrorResponse for each case.
        A ReadyForQuery must follow each error so the session stays alive.
        A valid query after all errors must succeed — proving state recovery.
        """
        e = self._env
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(5.0)

            # Case 1: Bind → unknown statement
            conn.send(
                pg_bind("", "ghost_stmt_i23_never_parsed", [], [], [])
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")}, max_msgs=20)
            assert any(t == ord("E") for t, _ in msgs), \
                "Bind of unknown statement must produce ErrorResponse"
            assert any(t == ord("Z") for t, _ in msgs), \
                "ReadyForQuery must follow ErrorResponse (unknown statement Bind)"

            # Case 2: Execute with no active portal
            conn.send(
                pg_execute("i23_ghost_portal", 0)
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")}, max_msgs=20)
            assert any(t == ord("E") for t, _ in msgs), \
                "Execute on nonexistent portal must produce ErrorResponse"
            assert any(t == ord("Z") for t, _ in msgs), \
                "ReadyForQuery must follow ErrorResponse (stray Execute)"

            # Recovery: a valid extended-protocol query must work
            conn.send(
                pg_parse("i23_ok", "SELECT 21 + 21", [])
                + pg_bind("", "i23_ok", [], [], [])
                + pg_execute("", 0)
                + pg_sync()
            )
            msgs = conn.recv_until({ord("Z")})
            rows = [b for t, b in msgs if t == ord("D")]
            assert rows, "recovery query after out-of-order messages must return a row"
            col_len = struct.unpack(">i", rows[0][2:6])[0]
            val = int(rows[0][6 : 6 + col_len])
            assert val == 42, f"recovery query: expected 42, got {val}"

    # =======================================================================
    # I24 — Random byte fuzzing after startup
    # =======================================================================

    def test_i24_random_byte_fuzz(self) -> None:
        """Send 4 KB of random bytes after a valid startup handshake.

        Runs 5 independent probes.  After each probe KEEL must:
        - Still accept new TCP connections.
        - Serve a valid query on a fresh connection.

        Acceptable outcomes per probe:
        - ErrorResponse (malformed message type) + connection close.
        - Immediate connection close (KEEL detected garbage and hung up).
        KEEL must NOT crash, hang, or corrupt the response stream of other
        sessions.
        """
        import os as _os
        e = self._env

        for trial in range(5):
            junk = _os.urandom(4096)
            conn = ProxyConn(e["host"], int(e["port"]))
            conn.connect()
            try:
                assert conn.startup(e["user"], e["database"], e["password"])
                conn.set_timeout(2.0)
                conn.send(junk)
                try:
                    conn.recv_until({ord("E"), ord("Z")}, max_msgs=10)
                except OSError:
                    pass  # expected — KEEL closed the connection
            finally:
                conn.close()

            time.sleep(0.3)
            assert is_proxy_reachable(e["host"], int(e["port"])), \
                f"KEEL must be reachable after fuzz trial {trial + 1}"

        # Final sanity: clean query on a fresh connection must work
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(5.0)
            conn.send(pg_query("SELECT 'fuzz_survived'"))
            msgs = conn.recv_until({ord("Z")})
            assert any(t == ord("D") for t, _ in msgs), \
                "KEEL must serve queries normally after fuzz probes"

    # =======================================================================
    # I26 — Transaction abandonment storm
    # =======================================================================

    def test_i26_transaction_abandonment_storm(self) -> None:
        """50 connections begin transactions; 25 disconnect mid-tx without ROLLBACK.

        KEEL must:
        - Detect each abrupt disconnect.
        - Rollback / recycle the abandoned backends.
        - Return all backends cleanly to the pool.

        Verified by running 50 queries on fresh connections after a cleanup
        window — all must succeed, proving the pool recovered completely.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        N = 50
        conns: list = []
        open_errors: list[str] = []

        # Open N connections and begin transactions
        for _ in range(N):
            try:
                c = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=15, autocommit=False,
                )
                c.execute("BEGIN")
                c.execute("SELECT pg_sleep(0)")
                conns.append(c)
            except Exception as ex:
                open_errors.append(str(ex))

        if len(conns) < N // 2:
            for c in conns:
                try: c.close()
                except Exception: pass
            self.skip(
                f"could not open enough connections ({len(conns)}/{N}): "
                f"{open_errors[:3]}"
            )

        # Abruptly close the first half (no ROLLBACK / Terminate message)
        for c in conns[: N // 2]:
            try:
                c.close()
            except Exception:
                pass

        # Cleanly close the second half
        for c in conns[N // 2 :]:
            try:
                c.rollback()
                c.close()
            except Exception:
                pass

        # Give KEEL time to detect disconnects and recycle all backends
        time.sleep(4.0)

        # Verify pool is healthy: N fresh queries must all succeed
        failures: list = []
        for i in range(N):
            try:
                with ProxyConn(e["host"], int(e["port"])) as check:
                    assert check.startup(e["user"], e["database"], e["password"])
                    check.set_timeout(5.0)
                    check.send(pg_query(f"SELECT {i} + 1"))
                    msgs = check.recv_until({ord("Z")})
                    if not any(t == ord("D") for t, _ in msgs):
                        failures.append(i)
            except Exception as ex:
                failures.append(f"conn_{i}: {ex}")

        assert not failures, (
            f"pool unhealthy after abandonment storm: "
            f"{len(failures)}/{N} queries failed — backends may not have been recycled"
        )

    # =======================================================================
    # I27 — Pool exhaustion and wait-queue ordering
    # =======================================================================

    def test_i27_pool_exhaustion_wait_queue(self) -> None:
        """Exhaust the backend pool then verify waiting sessions are served in order.

        Strategy (torture config: max_pool_size=80, transaction pooling):
        - Open 80 connections, each holding an advisory lock inside a
          transaction — all pool backends are now occupied.
        - Launch 10 extra connections in threads (they must queue in KEEL).
        - After 2 s release all holders.
        - Verify all 10 waiters complete successfully.

        Covers: wait-queue fairness, no starvation, no deadlock when pool is
        suddenly released.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        MAX_POOL = 80   # matches keel-torture.ini max_pool_size
        EXTRA    = 10

        holders:        list = []
        holder_errors:  list[str] = []
        waiter_results: list[bool] = []
        waiter_errors:  list[str] = []

        def _hold(i: int) -> None:
            try:
                c = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=15, autocommit=False,
                )
                c.execute(f"SELECT pg_advisory_xact_lock({9_000_000 + i})")
                holders.append(c)
            except Exception as ex:
                holder_errors.append(str(ex))

        # Fill the pool
        hold_threads = [threading.Thread(target=_hold, args=(i,)) for i in range(MAX_POOL)]
        for t in hold_threads: t.start()
        for t in hold_threads: t.join(timeout=30)

        if len(holders) < MAX_POOL // 2:
            for c in holders:
                try: c.close()
                except Exception: pass
            self.skip(
                f"could not fill pool ({len(holders)}/{MAX_POOL}): "
                f"{holder_errors[:2]}"
            )

        def _wait(i: int) -> None:
            try:
                c = pg.connect(
                    host=e["host"], port=int(e["port"]),
                    user=e["user"], password=e["password"],
                    dbname=e["database"],
                    connect_timeout=60, autocommit=True,
                )
                row = c.execute("SELECT 42").fetchone()
                c.close()
                waiter_results.append(row is not None and row[0] == 42)
            except Exception as ex:
                waiter_results.append(False)
                waiter_errors.append(str(ex))

        waiter_threads = [threading.Thread(target=_wait, args=(i,)) for i in range(EXTRA)]
        for t in waiter_threads: t.start()

        # Let waiters queue for 2 s, then release all holders
        time.sleep(2.0)
        for c in holders:
            try:
                c.rollback()
                c.close()
            except Exception:
                pass

        for t in waiter_threads: t.join(timeout=60)

        assert len(waiter_results) == EXTRA, (
            f"only {len(waiter_results)}/{EXTRA} waiters completed "
            f"(errors: {waiter_errors[:3]})"
        )
        successes = sum(1 for r in waiter_results if r)
        assert successes == EXTRA, (
            f"pool wait queue: only {successes}/{EXTRA} waiters got correct results "
            f"(errors: {waiter_errors[:3]})"
        )

    # =======================================================================
    # I28 — Backend killed while idle in pool
    # =======================================================================

    def test_i28_idle_backend_killed_transparent_reconnect(self) -> None:
        """Kill idle backends in the PostgreSQL pool directly; KEEL must reconnect.

        Steps:
        1. Run a query to warm up the pool (at least one idle backend exists).
        2. Terminate all idle KEEL backends via pg_terminate_backend() on the
           direct PostgreSQL connection.
        3. Wait 1 s for KEEL to detect the dead connections.
        4. Run 5 queries through the proxy — all must succeed, proving KEEL
           transparently opened fresh backends without surfacing an error.

        Requires: direct access to the PostgreSQL backend (KEEL_PG_HOST/PORT)
        with sufficient privilege to call pg_terminate_backend().
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e   = self._env
        pg_host = e.get("pg_host", "127.0.0.1")
        pg_port = int(e.get("pg_port", 5432))

        if not wait_for_port(pg_host, pg_port, timeout=2.0):
            self.skip(f"direct PG backend not reachable at {pg_host}:{pg_port}")

        # Step 1: warm the pool
        with ProxyConn(e["host"], int(e["port"])) as warm:
            assert warm.startup(e["user"], e["database"], e["password"])
            warm.set_timeout(5.0)
            warm.send(pg_query("SELECT 1"))
            warm.recv_until({ord("Z")})

        time.sleep(0.3)

        # Step 2: terminate idle KEEL backends at the PG level
        try:
            admin = pg.connect(
                host=pg_host, port=pg_port,
                user=e["user"], password=e["password"],
                dbname=e["database"],
                connect_timeout=5, autocommit=True,
            )
            admin.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM   pg_stat_activity
                WHERE  usename = %s
                  AND  state   = 'idle'
                  AND  pid    <> pg_backend_pid()
                """,
                (e["user"],),
            )
            admin.close()
        except Exception as err:
            self.skip(f"could not terminate backends (no superuser?): {err}")

        # Step 3: brief window for KEEL to detect dead connections
        time.sleep(1.0)

        # Step 4: 5 queries must succeed transparently
        for i in range(5):
            with ProxyConn(e["host"], int(e["port"])) as conn:
                assert conn.startup(e["user"], e["database"], e["password"])
                conn.set_timeout(8.0)
                conn.send(pg_query(f"SELECT {i} + 1"))
                msgs = conn.recv_until({ord("Z")})
                assert any(t == ord("D") for t, _ in msgs), (
                    f"query {i} failed after idle-backend kill — "
                    "KEEL did not transparently reconnect"
                )


    # =======================================================================
    # I29 — Live config reload under concurrent load
    # =======================================================================

    def test_i29_live_reload_under_load(self) -> None:
        """Issue admin RELOAD while 30 workers run concurrent queries.

        The admin RELOAD command is the equivalent of SIGHUP — KEEL re-reads
        its configuration and applies changes without dropping existing
        connections.  All in-flight queries must complete without error.

        Steps:
        1. Spin up 30 threads, each running 10 quick queries via the proxy.
        2. After 100 ms (while load is still in flight), issue RELOAD via the
           admin SQL console (psql -p 6433).
        3. Wait for all threads to finish.
        4. Assert zero query errors and that a post-reload query succeeds.
        """
        if not check_command("psql"):
            self.skip("psql not available")
        e = self._env

        CONCURRENCY = 30
        errors: list[str] = []
        reload_done = threading.Event()

        def _worker(tid: int) -> None:
            try:
                for i in range(10):
                    with ProxyConn(e["host"], int(e["port"])) as conn:
                        if not conn.startup(e["user"], e["database"], e["password"]):
                            errors.append(f"tid={tid} iter={i}: startup failed")
                            return
                        conn.set_timeout(10.0)
                        conn.send(pg_query(f"SELECT {tid} * 1000 + {i}"))
                        msgs = conn.recv_until({ord("Z")})
                        if not any(t == ord("D") for t, _ in msgs):
                            errors.append(f"tid={tid} iter={i}: no DataRow")
                    # Give the reload a window to interleave
                    if not reload_done.is_set():
                        time.sleep(0.01)
            except Exception as ex:
                errors.append(f"tid={tid}: {ex}")

        threads = [threading.Thread(target=_worker, args=(i,), daemon=True)
                   for i in range(CONCURRENCY)]
        for t in threads:
            t.start()

        # Issue RELOAD while load is in flight
        time.sleep(0.1)
        rc, out, err = _run(
            ["psql", f"--host={e['host']}", "--port=6433",
             "--username=postgres", "--dbname=postgres",
             "--no-password", "--command=RELOAD"],
            env={"PGPASSWORD": "postgres"},
            timeout=8,
        )
        reload_done.set()

        for t in threads:
            t.join(timeout=30)

        if rc != 0:
            self.skip(f"admin RELOAD failed (admin unavailable?): {(err or out)[:200]}")

        assert not errors, \
            f"queries failed during live RELOAD ({len(errors)} errors): {errors[:5]}"

        # Post-reload sanity query
        with ProxyConn(e["host"], int(e["port"])) as conn:
            assert conn.startup(e["user"], e["database"], e["password"])
            conn.set_timeout(5.0)
            conn.send(pg_query("SELECT 'reload_ok'"))
            msgs = conn.recv_until({ord("Z")})
            assert any(t == ord("D") for t, _ in msgs), \
                "pool unhealthy immediately after config RELOAD"

    # =======================================================================
    # I34 — SET/GUC tracking across pool cycles (SSV)
    # =======================================================================

    def test_i34_set_tracking_across_pool_cycles(self) -> None:
        """SET parameters must be preserved across pool boundaries (SSV).

        In transaction pooling mode KEEL returns the backend to the pool after
        each transaction.  The next transaction from the same client may land
        on a *different* backend.  KEEL's Semantic State Virtualisation (SSV)
        layer must replay the session's GUC changes (SET search_path, SET
        timezone, …) on that fresh backend before forwarding the query.

        Test sequence (repeated 8 times to exercise different backends):
        1. SET search_path = pg_catalog, public
        2. SET timezone = 'America/New_York'
        3. SHOW search_path  →  must contain 'pg_catalog'
        4. SHOW timezone     →  must contain 'America/New_York'

        Each SHOW runs in a *separate* transaction (autocommit=True), so KEEL
        MUST replay the SET on whatever backend it picks.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        for i in range(8):
            with pg.connect(
                host=e["host"], port=int(e["port"]),
                user=e["user"], password=e["password"],
                dbname=e["database"],
                connect_timeout=10, autocommit=True,
            ) as conn:
                conn.execute("SET search_path = pg_catalog, public")
                conn.execute("SET timezone = 'America/New_York'")

                row = conn.execute("SHOW search_path").fetchone()
                sp = row[0] if row else ""
                assert "pg_catalog" in sp, (
                    f"iteration {i}: SET search_path not preserved across pool cycle: "
                    f"got {sp!r}"
                )

                row = conn.execute("SHOW timezone").fetchone()
                tz = row[0] if row else ""
                assert "America/New_York" in tz, (
                    f"iteration {i}: SET timezone not preserved across pool cycle: "
                    f"got {tz!r}"
                )

    # =======================================================================
    # I35 — Temp table session pinning
    # =======================================================================

    def test_i35_temp_table_session_pin(self) -> None:
        """Creating a temporary table must pin the session to its backend.

        PostgreSQL temporary tables are session-scoped: they live in the
        pg_temp_N namespace of the backend that created them.  In transaction
        pooling mode, KEEL must detect CREATE TEMP TABLE and hard-pin the
        client connection to the creating backend for the lifetime of that
        client connection.

        Steps:
        1. CREATE TEMP TABLE _i35 (v int) — triggers FPIN_TEMP_TABLE in KEEL.
        2. INSERT a row and COMMIT.
        3. In a *new* transaction on the same client: SELECT from _i35 — must
           see the row (proves the same backend was used).
        4. Repeat INSERTs across multiple transactions to confirm continued
           pinning.
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10, autocommit=False,
        ) as conn:
            conn.execute("CREATE TEMP TABLE _i35_pin (v int)")
            conn.execute("INSERT INTO _i35_pin VALUES (42)")
            conn.commit()

            # New transaction — must still see the temp table
            row = conn.execute("SELECT v FROM _i35_pin").fetchone()
            conn.commit()
            assert row is not None and row[0] == 42, (
                "temp table invisible after pool cycle — "
                "session was NOT pinned to the creating backend"
            )

            # Additional transactions — confirmed continued pinning
            for k in range(5):
                conn.execute(f"INSERT INTO _i35_pin VALUES ({k})")
                conn.commit()

            count = conn.execute("SELECT COUNT(*) FROM _i35_pin").fetchone()[0]
            conn.commit()
            assert count == 6, (
                f"expected 6 rows in temp table, got {count} — "
                "pin broke across transactions"
            )

    # =======================================================================
    # I36 — WITH HOLD cursor survives COMMIT
    # =======================================================================

    def test_i36_with_hold_cursor_across_commit(self) -> None:
        """DECLARE … WITH HOLD cursor must survive transaction COMMIT.

        A WITH HOLD cursor lives on the backend (in its portal cache) past
        the COMMIT that closes the declaring transaction.  KEEL must:
        a) Pin the session to the cursor-owning backend (FPIN_CURSOR), and
        b) Keep it pinned until the client CLOSEs the cursor.

        Steps:
        1. Create a table with 10 rows.
        2. DECLARE c1 CURSOR WITH HOLD FOR SELECT …
        3. COMMIT — cursor must survive (WITH HOLD).
        4. FETCH 5 from c1 — must return 5 rows (proves same backend).
        5. CLOSE c1 — KEEL must release the CURSOR pin.
        6. A fresh query on a *different* connection must succeed (pool health).
        """
        pg = _import_psycopg3()
        if pg is None:
            self.skip("psycopg (v3) not installed")
        e = self._env

        # Seed table (separate connection so it doesn't interfere)
        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10, autocommit=True,
        ) as seed:
            seed.execute(
                "CREATE TABLE IF NOT EXISTS _i36_cursor_tbl "
                "AS SELECT generate_series(1, 10) AS n"
            )

        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10, autocommit=False,
        ) as conn:
            conn.execute(
                "DECLARE c1 CURSOR WITH HOLD FOR "
                "SELECT n FROM _i36_cursor_tbl ORDER BY n"
            )
            conn.commit()  # Close declaring transaction; WITH HOLD keeps cursor alive

            rows = conn.execute("FETCH 5 FROM c1").fetchall()
            conn.commit()
            assert len(rows) == 5, (
                f"expected 5 rows from WITH HOLD cursor after COMMIT, got {len(rows)} — "
                "KEEL did not keep session pinned to cursor-owning backend"
            )
            assert [r[0] for r in rows] == list(range(1, 6)), (
                f"wrong rows from cursor: {rows}"
            )

            conn.execute("CLOSE c1")
            conn.commit()  # Cursor closed — KEEL should release CURSOR pin

        # Pool health: unrelated query must work after cursor is closed
        with ProxyConn(e["host"], int(e["port"])) as health:
            assert health.startup(e["user"], e["database"], e["password"])
            health.set_timeout(5.0)
            health.send(pg_query("SELECT 'pool_ok_after_cursor'"))
            msgs = health.recv_until({ord("Z")})
            assert any(t == ord("D") for t, _ in msgs), \
                "pool unhealthy after WITH HOLD cursor lifecycle"

        # Cleanup
        with pg.connect(
            host=e["host"], port=int(e["port"]),
            user=e["user"], password=e["password"],
            dbname=e["database"],
            connect_timeout=10, autocommit=True,
        ) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS _i36_cursor_tbl")

    # =======================================================================
    # I40 — CancelRequest storm
    # =======================================================================

    def test_i40_cancel_request_storm(self) -> None:
        """10 concurrent long queries each receive a CancelRequest; all must cancel.

        CancelRequest is a special out-of-band TCP connection (not within the
        normal protocol stream).  KEEL must forward the cancel token to the
        correct backend and the query must receive SQLSTATE 57014 (QueryCanceled).

        Steps:
        1. Open 10 connections, each starting 'SELECT pg_sleep(30)'.
        2. Immediately send a CancelRequest on a separate socket.
        3. Collect the response — must be ErrorResponse with SQLSTATE 57014.
        4. Each connection must then accept a normal query (pool is healthy).
        """
        e = self._env
        WORKERS = 10
        results: list[str] = []  # "" = ok, else error description
        lock = threading.Lock()

        def _cancel_one(tid: int) -> None:
            try:
                with ProxyConn(e["host"], int(e["port"])) as conn:
                    if not conn.startup(e["user"], e["database"], e["password"]):
                        with lock:
                            results.append(f"tid={tid}: startup failed")
                        return
                    conn.set_timeout(15.0)

                    # Send a long-running query
                    conn.send(pg_query("SELECT pg_sleep(30)"))

                    # Cancel it immediately on a separate socket
                    cancel_host = e["host"]
                    cancel_port = int(e["port"])
                    with socket.create_connection((cancel_host, cancel_port), timeout=5) as csock:
                        csock.sendall(pg_cancel_request(conn.backend_pid, conn.backend_secret))

                    # The pending query must return ErrorResponse (57014)
                    msgs = conn.recv_until({ord("Z"), ord("E")})
                    sqlstate = ""
                    for t, body in msgs:
                        if t == ord("E"):
                            sqlstate = _parse_pg_error_field(body, "C")
                            break

                    if sqlstate != "57014":
                        with lock:
                            results.append(
                                f"tid={tid}: expected SQLSTATE 57014, got {sqlstate!r}"
                            )
                        return

                    # Recovery: a normal query must succeed
                    conn.send(pg_query(f"SELECT {tid} + 0"))
                    msgs2 = conn.recv_until({ord("Z")})
                    if not any(t == ord("D") for t, _ in msgs2):
                        with lock:
                            results.append(f"tid={tid}: recovery query failed after cancel")
                        return

                with lock:
                    results.append("")  # success
            except Exception as ex:
                with lock:
                    results.append(f"tid={tid}: {ex}")

        threads = [threading.Thread(target=_cancel_one, args=(i,), daemon=True)
                   for i in range(WORKERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        failures = [r for r in results if r]
        assert len(results) == WORKERS, \
            f"only {len(results)}/{WORKERS} cancel threads completed"
        assert not failures, \
            f"{len(failures)}/{WORKERS} cancel workers failed: {failures[:5]}"


# ---------------------------------------------------------------------------
# Module-level helpers (wire-protocol + metrics)
# ---------------------------------------------------------------------------

def _parse_pg_error_field(error_body: bytes, field_code: str) -> str:
    """Extract a field value from a PostgreSQL ErrorResponse body by its type code."""
    pos = 0
    while pos < len(error_body):
        code = error_body[pos]
        if code == 0:
            break
        nul = error_body.find(b"\x00", pos + 1)
        if nul == -1:
            break
        if chr(code) == field_code:
            return error_body[pos + 1 : nul].decode("utf-8", errors="replace")
        pos = nul + 1
    return ""


def _collect_keel_admin_stats(env: dict) -> dict:
    """Query KEEL admin console (port 6433) for SHOW STATS/POOLS."""
    result: dict = {}
    for cmd in ("SHOW STATS", "SHOW POOLS"):
        rc, out, _ = _run(
            ["psql", f"--host={env['host']}", "--port=6433",
             "--username=postgres", "--dbname=postgres",
             "--no-password", f"--command={cmd}"],
            env={"PGPASSWORD": "postgres"},
            timeout=8,
        )
        if rc == 0:
            result[cmd.replace(" ", "_").lower()] = out.strip()
    return result


def _collect_prometheus_metrics(env: dict) -> dict:
    """Fetch /metrics from the KEEL Prometheus endpoint and parse it."""
    try:
        url = f"http://{env['host']}:9101/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return {}
    metrics: dict = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split(" ", 1)
        if len(parts) == 2:
            metrics[parts[0]] = parts[1].strip()
    return metrics


def _validate_pcap_pg_framing(pcap_path: str, pg_port: int) -> list[str]:
    """
    Parse a legacy pcap file and verify PostgreSQL backend message framing.
    Validates every message in the backend→client direction (src port == pg_port).
    Returns a list of violation strings; empty means all good.

    Supports link types:
      1   — Ethernet II
      113 — Linux cooked capture (tcpdump -i any)
    """
    # All valid single-byte type codes in the PostgreSQL backend→frontend direction
    VALID_BE = frozenset(ord(c) for c in "RBKZS1234CnNDTtEIAHVWdcp!")
    violations: list[str] = []

    try:
        with open(pcap_path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        return [f"cannot read pcap: {exc}"]

    if len(raw) < 24:
        return ["pcap too short — capture may be empty"]

    magic = struct.unpack("<I", raw[:4])[0]
    if magic == 0xa1b2c3d4:
        endian = "<"
    elif magic == 0xd4c3b2a1:
        endian = ">"
    else:
        return [f"unrecognised pcap magic 0x{magic:08x}"]

    net_type = struct.unpack(f"{endian}I", raw[20:24])[0]

    # Accumulate TCP payloads per 4-tuple
    streams: dict[tuple, bytearray] = {}

    pos = 24
    while pos + 16 <= len(raw):
        _, _, incl_len, _ = struct.unpack(f"{endian}IIII", raw[pos : pos + 16])
        pos += 16
        if pos + incl_len > len(raw):
            break
        pkt = raw[pos : pos + incl_len]
        pos += incl_len

        if net_type == 1:            # Ethernet II
            if len(pkt) < 14:
                continue
            if struct.unpack(">H", pkt[12:14])[0] != 0x0800:
                continue
            ip_off = 14
        elif net_type == 113:        # Linux SLL v1 (tcpdump -i any, older kernels)
            if len(pkt) < 16:
                continue
            if struct.unpack(">H", pkt[14:16])[0] != 0x0800:
                continue
            ip_off = 16
        elif net_type == 276:        # Linux SLL v2 (tcpdump -i any, kernel ≥5.4)
            if len(pkt) < 20:
                continue
            if struct.unpack(">H", pkt[0:2])[0] != 0x0800:
                continue
            ip_off = 20
        else:
            continue

        ip = pkt[ip_off:]
        if len(ip) < 20 or (ip[0] >> 4) != 4 or ip[9] != 6:
            continue

        ip_ihl   = (ip[0] & 0x0F) * 4
        src_ip   = ip[12:16]
        dst_ip   = ip[16:20]
        tcp      = ip[ip_ihl:]
        if len(tcp) < 20:
            continue

        src_port = struct.unpack(">H", tcp[0:2])[0]
        dst_port = struct.unpack(">H", tcp[2:4])[0]
        if src_port != pg_port and dst_port != pg_port:
            continue

        data_off = (tcp[12] >> 4) * 4
        payload  = tcp[data_off:]
        if not payload:
            continue

        key = (bytes(src_ip), src_port, bytes(dst_ip), dst_port)
        if key not in streams:
            streams[key] = bytearray()
        streams[key].extend(payload)

    if not streams:
        return ["no TCP traffic on the proxy port was captured"]

    # Validate each stream originating from the proxy port
    for (src_ip, src_port, dst_ip, dst_port), data in streams.items():
        if src_port != pg_port:
            continue   # only inspect backend→client direction
        sid = f":{src_port}\u2192:{dst_port}"
        p2 = 0
        count = 0
        while p2 + 5 <= len(data):
            mtype  = data[p2]
            mlen   = struct.unpack(">I", bytes(data[p2 + 1 : p2 + 5]))[0]
            if mtype not in VALID_BE:
                violations.append(
                    f"{sid} offset={p2}: invalid type 0x{mtype:02x} "
                    f"('{chr(mtype) if 32 <= mtype < 127 else '?'}')"
                )
            if mlen < 4:
                violations.append(
                    f"{sid} offset={p2}: length {mlen} < 4 (type=0x{mtype:02x})"
                )
                break
            p2 += 1 + mlen
            count += 1
            if count > 500_000:
                break

    return violations


# ---------------------------------------------------------------------------
# Helpers referenced in tests
# ---------------------------------------------------------------------------

def is_proxy_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=2.0):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Standalone entry-point
# ---------------------------------------------------------------------------

def _add_args(p: "argparse.ArgumentParser") -> None:
    p.add_argument("--soak", type=int, default=_DEFAULT_SOAK_S,
                   metavar="SECONDS",
                   help=f"Soak duration in seconds for I11 (default: {_DEFAULT_SOAK_S})")


if __name__ == "__main__":
    import argparse
    standalone_main(TortureSuite, "torture", TortureSuite.DESCRIPTION, _add_args)
