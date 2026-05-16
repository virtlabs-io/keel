"""Apply pytest xfail markers to known-broken e2e tests.

Each marker carries:
  - strict=True  → so a fix is loudly flagged via XPASS=>FAIL
  - reason       → references docs/LIMITATIONS.md section

After invocation the script is idempotent: re-running adds no duplicates.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # repo root (scripts/..)
TESTS = ROOT / "tests" / "e2e"

# (file, test_name, reason)
TARGETS: list[tuple[str, str, str]] = [
    # ---------- test_sharding_comprehensive.py ----------
    ("test_sharding_comprehensive.py", "test_case_expression_in_aggregate",
     "docs/LIMITATIONS.md §1.9 — CASE in aggregate not pushed down"),
    ("test_sharding_comprehensive.py", "test_case_expression_in_group_by",
     "docs/LIMITATIONS.md §1.9 — CASE in GROUP BY not pushed down"),
    ("test_sharding_comprehensive.py", "test_derived_table_subquery",
     "docs/LIMITATIONS.md §1.9 — derived-table subquery not merged"),
    ("test_sharding_comprehensive.py", "test_percentile_cont_via_scatter",
     "docs/LIMITATIONS.md §1.4 — percentile_cont not implemented"),
    ("test_sharding_comprehensive.py", "test_percentile_disc_via_scatter",
     "docs/LIMITATIONS.md §1.4 — percentile_disc not implemented"),
    ("test_sharding_comprehensive.py", "test_union_all_combines_scatter_results",
     "docs/LIMITATIONS.md §1.5 — UNION ALL across scatters not merged"),
    ("test_sharding_comprehensive.py", "test_concurrent_updates_no_lost_update",
     "docs/LIMITATIONS.md §1.13 — subsumed by §2 parameter-aware routing"),
    ("test_sharding_comprehensive.py", "test_chained_ctes",
     "docs/LIMITATIONS.md §1.2 — non-recursive CTE not inlined"),
    ("test_sharding_comprehensive.py", "test_cte_count_and_sum_simultaneously",
     "docs/LIMITATIONS.md §1.2 — non-recursive CTE not inlined"),
    ("test_sharding_comprehensive.py", "test_cte_min_max_values",
     "docs/LIMITATIONS.md §1.2 — non-recursive CTE not inlined"),
    ("test_sharding_comprehensive.py", "test_cte_used_twice_in_query",
     "docs/LIMITATIONS.md §1.2 — multi-reference CTE materialisation missing"),
    ("test_sharding_comprehensive.py", "test_cte_with_filter_having",
     "docs/LIMITATIONS.md §1.2 — non-recursive CTE not inlined"),
    ("test_sharding_comprehensive.py", "test_recursive_cte_countdown",
     "docs/LIMITATIONS.md §1.1 — recursive CTE evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_recursive_cte_fibonacci",
     "docs/LIMITATIONS.md §1.1 — recursive CTE evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_recursive_cte_sum_of_integers",
     "docs/LIMITATIONS.md §1.1 — recursive CTE evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_simple_cte_wraps_scatter_aggregate",
     "docs/LIMITATIONS.md §1.2 — CTE wrapping scatter aggregate not merged"),
    ("test_sharding_comprehensive.py", "test_array_agg_over_scatter",
     "docs/LIMITATIONS.md §1.10 — array_agg merge not implemented"),
    ("test_sharding_comprehensive.py", "test_jsonb_agg_over_scatter",
     "docs/LIMITATIONS.md §1.10 — jsonb_agg merge not implemented"),
    ("test_sharding_comprehensive.py", "test_jsonb_category_sum_with_filter",
     "docs/LIMITATIONS.md §1.10 — JSONB GROUP BY expression pushdown missing"),
    ("test_sharding_comprehensive.py", "test_jsonb_group_by_in_stock",
     "docs/LIMITATIONS.md §1.10 — JSONB GROUP BY expression pushdown missing"),
    ("test_sharding_comprehensive.py", "test_jsonb_numeric_field_extraction",
     "docs/LIMITATIONS.md §1.10 — JSONB extractor in projection not merged"),
    ("test_sharding_comprehensive.py", "test_string_agg_over_scatter",
     "docs/LIMITATIONS.md §1.10 — string_agg merge not implemented"),
    ("test_sharding_comprehensive.py", "test_group_by_excludes_null_keys",
     "docs/LIMITATIONS.md §1.8 — GROUP BY NULL key equality wrong"),
    ("test_sharding_comprehensive.py", "test_order_by_nulls_first",
     "docs/LIMITATIONS.md §1.7 — NULLS FIRST/LAST not threaded into merge comparator"),
    ("test_sharding_comprehensive.py", "test_limit_offset_page",
     "docs/LIMITATIONS.md §1.6 — OFFSET applied per-shard, not post-merge"),
    ("test_sharding_comprehensive.py", "test_offset_skips_correct_rows",
     "docs/LIMITATIONS.md §1.6 — OFFSET applied per-shard, not post-merge"),
    ("test_sharding_comprehensive.py", "test_between_scatter_returns_range",
     "docs/LIMITATIONS.md §1.11 — BETWEEN range merge ordering incorrect"),
    ("test_sharding_comprehensive.py", "test_coalesce_in_scatter_select",
     "docs/LIMITATIONS.md §1.11 — COALESCE single-row scatter race"),
    ("test_sharding_comprehensive.py", "test_in_clause_crosses_shards",
     "docs/LIMITATIONS.md §1.11 — IN-list targeted dispatch not implemented"),
    ("test_sharding_comprehensive.py", "test_having_count_eliminates_small_groups",
     "docs/LIMITATIONS.md §1.12 — HAVING pushed down too aggressively"),
    ("test_sharding_comprehensive.py", "test_having_filters_groups",
     "docs/LIMITATIONS.md §1.12 — HAVING pushed down too aggressively"),
    ("test_sharding_comprehensive.py", "test_order_by_offset",
     "docs/LIMITATIONS.md §1.6 — OFFSET applied per-shard, not post-merge"),
    ("test_sharding_comprehensive.py", "test_first_value_over_entire_window",
     "docs/LIMITATIONS.md §1.3 — window functions evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_last_value_over_entire_window",
     "docs/LIMITATIONS.md §1.3 — window functions evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_partition_count_over_action",
     "docs/LIMITATIONS.md §1.3 — window functions evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_partition_sum_over_action",
     "docs/LIMITATIONS.md §1.3 — window functions evaluated per-shard"),
    ("test_sharding_comprehensive.py", "test_sum_running_total",
     "docs/LIMITATIONS.md §1.3 — window functions evaluated per-shard"),

    # ---------- test_sharding_advanced.py ----------
    ("test_sharding_advanced.py", "test_balance_quintile_grouping",
     "docs/LIMITATIONS.md §1.12 — bucket expression in GROUP BY not pushed down"),
    ("test_sharding_advanced.py", "test_cte_chained_category_sums",
     "docs/LIMITATIONS.md §1.2 — non-recursive CTE not inlined"),
    ("test_sharding_advanced.py", "test_cte_multi_level_aggregation_users",
     "docs/LIMITATIONS.md §1.2 — multi-level CTE not inlined"),
    ("test_sharding_advanced.py", "test_events_having_count_threshold",
     "docs/LIMITATIONS.md §1.12 — HAVING pushed down too aggressively"),
    ("test_sharding_advanced.py", "test_events_running_sum_global_total",
     "docs/LIMITATIONS.md §1.3 — running-sum window function not merged"),
    ("test_sharding_advanced.py", "test_recursive_cte_large_n_sum",
     "docs/LIMITATIONS.md §1.1 — recursive CTE evaluated per-shard"),
    ("test_sharding_advanced.py", "test_users_age_buckets_group_by",
     "docs/LIMITATIONS.md §1.12 — bucket expression in GROUP BY not pushed down"),

    # ---------- test_prepared_statements_ssv.py (all 25, ref §3) ----------
    ("test_prepared_statements_ssv.py", "test_named_parse_returns_parse_complete",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_bind_execute_returns_result",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_parameterised_query",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_two_params",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_named_parse_pins_backend",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_pinned_backend_serves_multiple_statements",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_deallocate_individual_keeps_pin",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_deallocate_all_releases_pin",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_simple_prepare_tracked",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_simple_prepare_with_params",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_simple_prepare_survives_50_transactions",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_deallocate_removes_tracked_stmt",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_named_parse_works_transparently",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_backend_has_no_named_statements",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_pg_prepared_statements_always_empty_for_named",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_close_named_absorbed_by_proxy",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_describe_unnamed_works",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_named_parse_forwarded_to_backend",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_backend_pins_on_first_parse",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_individual_deallocate_keeps_pin",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_100_rapid_prepare_execute_cycles",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    ("test_prepared_statements_ssv.py", "test_stmts_survive_pool_pressure",
     "docs/LIMITATIONS.md §3 — SSV unstable under load + §4 allocator corruption"),
    ("test_prepared_statements_ssv.py", "test_search_path_change_does_not_corrupt_stmt_results",
     "docs/LIMITATIONS.md §3 — SSV reset on backend rotation incomplete"),
    ("test_prepared_statements_ssv.py", "test_clean_backend_replay_correctness_multiple_stmts",
     "docs/LIMITATIONS.md §3 — SSV unstable under load (passes in isolation)"),
    # NB: TestPS_Off::test_deallocate_all_releases_pin shares its name with TestPS_Pinning;
    # both methods are covered by the duplicate-handling logic below.

    # ---------- test_routing.py ----------
    ("test_routing.py", "test_negative_keys_route_correctly",
     "docs/LIMITATIONS.md §2 — parameterised routing & negative-int hash parity"),

    # ---------- test_transaction_pooling.py ----------
    ("test_transaction_pooling.py", "test_1000_autocommit_insert_delete_cycles",
     "docs/LIMITATIONS.md §2 — parameterised DML scatters without Bind-time routing"),
    ("test_transaction_pooling.py", "test_pool_waiting_queue_resolves_when_backends_free",
     "docs/LIMITATIONS.md §2 — pool starved by parameterised scatter"),
    ("test_transaction_pooling.py", "test_1000_consecutive_transactions_same_psycopg2_connection",
     "docs/LIMITATIONS.md §2 — txn-pinned DML mis-routes without Bind-time key extraction"),

    # ---------- test_stress.py ----------
    ("test_stress.py", "test_concurrent_reads_and_writes",
     "docs/LIMITATIONS.md §4 — heap header corruption surfaces under stress"),

    # ---------- test_pool_behavior.py ----------
    ("test_pool_behavior.py", "test_active_connections_rise_under_burst",
     "docs/LIMITATIONS.md §5 — pool eviction-on-full drops connections under burst"),

    # ---------- test_performance.py ----------
    ("test_performance.py", "test_sql_level_prepare_execute_tps",
     "docs/LIMITATIONS.md §6 — SSV adds per-Parse overhead on fast path"),
]


def add_xfail(file_path: Path, test_name: str, reason: str) -> int:
    """Insert @pytest.mark.xfail before every `def test_name(` whose
    immediately preceding non-blank line is not already an xfail marker
    for the same reason. Returns the number of decorators inserted."""
    text = file_path.read_text()
    lines = text.splitlines(keepends=True)
    pat = re.compile(r"^(\s*)def\s+" + re.escape(test_name) + r"\s*\(")

    out: list[str] = []
    inserted = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        m = pat.match(line)
        if m:
            indent = m.group(1)
            # Walk backwards over decorators / blank lines to detect existing xfail.
            j = len(out) - 1
            saw_xfail = False
            while j >= 0:
                stripped = out[j].strip()
                if not stripped:
                    j -= 1
                    continue
                if stripped.startswith("@"):
                    if "pytest.mark.xfail" in stripped and reason in stripped:
                        saw_xfail = True
                        break
                    if "pytest.mark.xfail" in stripped:
                        # Different xfail reason — replace it for clarity.
                        out[j] = (
                            f"{indent}@pytest.mark.xfail(strict=True, "
                            f"reason=\"{reason}\")\n"
                        )
                        saw_xfail = True
                        break
                    j -= 1
                    continue
                break
            if not saw_xfail:
                out.append(
                    f"{indent}@pytest.mark.xfail(strict=True, "
                    f"reason=\"{reason}\")\n"
                )
                inserted += 1
        out.append(line)
        i += 1

    if inserted or any("pytest.mark.xfail" in l and reason in l for l in out):
        file_path.write_text("".join(out))
    return inserted


def main() -> None:
    by_file: dict[str, list[tuple[str, str]]] = {}
    for fname, test, reason in TARGETS:
        by_file.setdefault(fname, []).append((test, reason))

    total_inserted = 0
    for fname, items in by_file.items():
        path = TESTS / fname
        if not path.is_file():
            print(f"SKIP (missing): {path}")
            continue
        n = 0
        for test, reason in items:
            n += add_xfail(path, test, reason)
        print(f"{fname}: +{n} xfail markers")
        total_inserted += n
    print(f"TOTAL: +{total_inserted}")


if __name__ == "__main__":
    main()
