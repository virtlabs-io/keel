"""
test_high_pressure_pipeline.py
==============================

High-pressure transaction matrix focused on production hardening:
- thousands of transactions
- explicit BEGIN/COMMIT/SAVEPOINT paths
- prepared statement execution paths
- connection churn to stress borrow/cleanup/replay behavior
"""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest

from helpers import clear_table_on_shards, shard_total_count


pytestmark = [pytest.mark.stress, pytest.mark.pool, pytest.mark.prepared_statements]


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        val = int(raw)
    except ValueError:
        return default
    return max(val, minimum)


@pytest.fixture(autouse=True)
def _clean_pressure_rows(shard0_conn, shard1_conn):
    clear_table_on_shards(shard0_conn, shard1_conn, "users")
    yield
    clear_table_on_shards(shard0_conn, shard1_conn, "users")


class TestHighPressurePipeline:
    @pytest.mark.timeout(240)
    def test_mixed_transaction_and_prepared_statement_pressure(
        self,
        keel_dsn,
        shard0_conn,
        shard1_conn,
    ):
        total_ops = _env_int("KEEL_E2E_HIGH_PRESSURE_TX", 3600, 1200)
        workers = _env_int("KEEL_E2E_HIGH_PRESSURE_WORKERS", 24, 4)
        base_id = _env_int("KEEL_E2E_HIGH_PRESSURE_BASE_ID", 700_000, 10_000)

        errors: list[Exception] = []
        errors_lock = threading.Lock()
        counters = {"ok": 0, "writes": 0}
        counters_lock = threading.Lock()

        def worker_loop(worker_idx: int) -> None:
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(
                        "PREPARE hp_ins(int,text) AS "
                        "INSERT INTO users(id, name, balance) VALUES ($1, $2, 1) "
                        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, balance = users.balance + 1"
                    )
                    cur.execute(
                        "PREPARE hp_upd(int,int) AS "
                        "UPDATE users SET balance = balance + $1 WHERE id = $2"
                    )
                    cur.execute("PREPARE hp_sel(int) AS SELECT id, balance FROM users WHERE id = $1")
                conn.commit()

                local_ok = 0
                local_writes = 0

                for seq in range(worker_idx, total_ops, workers):
                    row_id = base_id + seq
                    op = seq % 6
                    try:
                        with conn.cursor() as cur:
                            if op == 0:
                                cur.execute("BEGIN")
                                cur.execute("EXECUTE hp_ins(%s, %s)", (row_id, f"hp_{seq}"))
                                cur.execute("COMMIT")
                                local_writes += 1
                            elif op == 1:
                                cur.execute("BEGIN")
                                cur.execute("SAVEPOINT hp_sp")
                                cur.execute("EXECUTE hp_ins(%s, %s)", (row_id, f"hp_{seq}"))
                                cur.execute("RELEASE SAVEPOINT hp_sp")
                                cur.execute("COMMIT")
                                local_writes += 1
                            elif op == 2:
                                cur.execute("BEGIN")
                                cur.execute("EXECUTE hp_ins(%s, %s)", (row_id, f"hp_{seq}"))
                                cur.execute("EXECUTE hp_upd(%s, %s)", (1, row_id))
                                cur.execute("COMMIT")
                                local_writes += 1
                            elif op == 3:
                                cur.execute("EXECUTE hp_ins(%s, %s)", (row_id, f"hp_{seq}"))
                                conn.commit()
                                local_writes += 1
                            elif op == 4:
                                cur.execute("EXECUTE hp_sel(%s)", (row_id,))
                                cur.fetchone()
                                conn.commit()
                            else:
                                cur.execute(
                                    "SELECT COUNT(*) FROM users WHERE id >= %s AND id < %s",
                                    (base_id, base_id + total_ops),
                                )
                                cur.fetchone()
                                conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                    local_ok += 1

                with counters_lock:
                    counters["ok"] += local_ok
                    counters["writes"] += local_writes

            except Exception as exc:
                with errors_lock:
                    errors.append(exc)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        started = time.monotonic()
        threads = [threading.Thread(target=worker_loop, args=(idx,)) for idx in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=220)
        elapsed = time.monotonic() - started

        assert not errors, f"high-pressure mixed path errors ({len(errors)}): {errors[:3]}"
        assert counters["ok"] == total_ops, (
            f"expected {total_ops} successful operations, got {counters['ok']}"
        )

        rows = shard_total_count(
            shard0_conn,
            shard1_conn,
            "users",
            f"id >= {base_id} AND id < {base_id + total_ops}",
        )
        assert rows == counters["writes"], (
            f"row count mismatch under pressure: rows={rows}, expected_writes={counters['writes']}"
        )
        print(
            f"\n[pressure] mixed pipeline: {total_ops} ops, "
            f"{counters['writes']} writes, {workers} workers, {elapsed:.2f}s"
        )

    @pytest.mark.timeout(240)
    def test_connection_churn_with_thousands_of_transactions(
        self,
        keel_dsn,
        shard0_conn,
        shard1_conn,
    ):
        total_tx = _env_int("KEEL_E2E_CHURN_TX", 1600, 800)
        workers = _env_int("KEEL_E2E_CHURN_WORKERS", 32, 4)
        base_id = _env_int("KEEL_E2E_CHURN_BASE_ID", 800_000, 10_000)

        lock = threading.Lock()
        errors: list[Exception] = []
        successes = 0

        def churn_tx(seq: int) -> None:
            nonlocal successes
            conn = None
            try:
                conn = psycopg2.connect(keel_dsn, connect_timeout=10)
                conn.autocommit = False
                row_id = base_id + seq
                with conn.cursor() as cur:
                    cur.execute("BEGIN")
                    cur.execute(
                        "INSERT INTO users(id, name, balance) VALUES (%s, %s, 1) "
                        "ON CONFLICT (id) DO UPDATE SET balance = users.balance + 1",
                        (row_id, f"churn_{seq}"),
                    )
                    if (seq % 5) == 0:
                        cur.execute("SELECT id FROM users WHERE id = %s", (row_id,))
                        cur.fetchone()
                    cur.execute("COMMIT")
                with lock:
                    successes += 1
            except Exception as exc:
                with lock:
                    errors.append(exc)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(churn_tx, range(total_tx)))
        elapsed = time.monotonic() - started

        assert not errors, f"connection churn errors ({len(errors)}): {errors[:3]}"
        assert successes == total_tx, f"expected {total_tx} committed tx, got {successes}"

        rows = shard_total_count(
            shard0_conn,
            shard1_conn,
            "users",
            f"id >= {base_id} AND id < {base_id + total_tx}",
        )
        assert rows == total_tx, f"expected {total_tx} churn rows, got {rows}"
        print(
            f"\n[pressure] churn pipeline: {total_tx} tx, "
            f"{workers} workers, {elapsed:.2f}s"
        )
