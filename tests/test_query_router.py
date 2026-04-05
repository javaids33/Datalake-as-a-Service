"""
Test query router — 30+ cases covering every routing branch.
This is the most critical test file in the project.

Tests the core architecture: DuckDB = lightweight client,
Starburst = distributed compute engine.
"""

import os
import pytest

os.environ.setdefault("DLS_MODE", "dev")
os.environ.setdefault("DLS_STARBURST_ENABLED", "true")

from dlsidecar.engine.query_router import Engine, JoinStrategy, QueryRouter, RoutingReason


@pytest.fixture
def router():
    return QueryRouter(
        owned_namespaces={"mydb", "team_alpha"},
        s3_buckets=["team-alpha-data", "team-alpha-staging"],
        scan_threshold=50 * 1024 * 1024 * 1024,  # 50GB
    )


# ── PATH A: DuckDB lightweight local work ────────────────────────────────

class TestDuckDBLocal:
    """DuckDB handles lightweight in-pod work: local files, small scans, data prep."""

    def test_unqualified_table_routes_to_duckdb(self, router):
        plan = router.route("SELECT * FROM events")
        assert plan.engine == Engine.DUCKDB

    def test_owned_namespace_routes_to_duckdb(self, router):
        plan = router.route("SELECT * FROM mydb.events")
        assert plan.engine == Engine.DUCKDB
        assert RoutingReason.DEV_MODE in plan.reasons or RoutingReason.OWNED_NAMESPACE in plan.reasons

    def test_second_owned_namespace(self, router):
        plan = router.route("SELECT * FROM team_alpha.users")
        assert plan.engine == Engine.DUCKDB

    def test_local_duckdb_table(self, router):
        plan = router.route("SELECT * FROM main.local_cache")
        assert plan.engine == Engine.DUCKDB

    def test_temp_table(self, router):
        plan = router.route("SELECT * FROM temp.staging_data")
        assert plan.engine == Engine.DUCKDB

    def test_information_schema(self, router):
        plan = router.route("SELECT * FROM information_schema.tables")
        assert plan.engine == Engine.DUCKDB

    def test_ddl_no_table_ref(self, router):
        plan = router.route("SET memory_limit = '8GB'")
        assert plan.engine == Engine.DUCKDB

    def test_create_table(self, router):
        plan = router.route("CREATE TABLE mydb.new_table AS SELECT 1")
        assert plan.engine == Engine.DUCKDB
        assert RoutingReason.DATA_PREP in plan.reasons

    def test_join_two_owned_tables(self, router):
        plan = router.route("SELECT * FROM mydb.events e JOIN mydb.users u ON e.user_id = u.id")
        assert plan.engine == Engine.DUCKDB

    def test_cte_with_owned_table(self, router):
        plan = router.route("WITH recent AS (SELECT * FROM mydb.events WHERE ts > '2025-01-01') SELECT * FROM recent")
        assert plan.engine == Engine.DUCKDB

    def test_subquery_owned(self, router):
        plan = router.route("SELECT * FROM (SELECT id, name FROM mydb.users) sub")
        assert plan.engine == Engine.DUCKDB

    def test_dev_mode_all_local(self, router):
        plan = router.route("SELECT 1 + 1")
        assert plan.engine == Engine.DUCKDB

    def test_below_scan_threshold(self, router):
        plan = router.route("SELECT * FROM mydb.events", estimated_bytes=1024)
        assert plan.engine == Engine.DUCKDB


# ── Local file operations (DuckDB's strength) ────────────────────────────

class TestLocalFileOps:
    """DuckDB excels at reading local CSV/Parquet/Arrow, data prep, COPY."""

    def test_read_csv(self, router):
        plan = router.route("SELECT * FROM read_csv('/data/input.csv')")
        assert plan.engine == Engine.DUCKDB
        assert RoutingReason.LOCAL_FILE in plan.reasons

    def test_read_parquet(self, router):
        plan = router.route("SELECT * FROM read_parquet('/data/output.parquet')")
        assert plan.engine == Engine.DUCKDB
        assert RoutingReason.LOCAL_FILE in plan.reasons

    def test_copy_to_parquet(self, router):
        plan = router.route("COPY mydb.events TO '/data/export.parquet' (FORMAT PARQUET)")
        assert plan.engine == Engine.DUCKDB
        # COPY matches local file pattern (DuckDB's strength)
        assert RoutingReason.LOCAL_FILE in plan.reasons or RoutingReason.DATA_PREP in plan.reasons

    def test_glob_pattern(self, router):
        plan = router.route("SELECT * FROM glob('/data/partitions/*.parquet')")
        assert plan.engine == Engine.DUCKDB

    def test_describe_table(self, router):
        plan = router.route("DESCRIBE mydb.events")
        assert plan.engine == Engine.DUCKDB

    def test_pragma(self, router):
        plan = router.route("PRAGMA database_size")
        assert plan.engine == Engine.DUCKDB


# ── PATH B: Starburst distributed compute ─────────────────────────────────

class TestStarburstCompute:
    """Starburst handles heavy compute, cross-team queries, large scans."""

    def test_cross_team_catalog(self, router):
        plan = router.route("SELECT * FROM other_team.sales.orders")
        assert plan.engine == Engine.STARBURST
        assert RoutingReason.CROSS_TEAM_REF in plan.reasons
        assert plan.offload_to_starburst is True

    def test_unowned_namespace(self, router):
        plan = router.route("SELECT * FROM finance_db.transactions")
        assert plan.engine == Engine.STARBURST

    def test_explicit_starburst_hint(self, router):
        plan = router.route("SELECT * FROM mydb.events /*+ ENGINE(starburst) */")
        assert plan.engine == Engine.STARBURST
        assert RoutingReason.EXPLICIT_HINT in plan.reasons
        assert plan.offload_to_starburst is True

    def test_size_threshold_exceeded(self, router):
        plan = router.route(
            "SELECT * FROM mydb.events",
            estimated_bytes=100 * 1024 * 1024 * 1024,
        )
        assert plan.engine == Engine.STARBURST
        assert RoutingReason.SIZE_THRESHOLD in plan.reasons

    def test_fully_unowned_join(self, router):
        plan = router.route("SELECT * FROM finance.orders o JOIN hr.employees e ON o.emp_id = e.id")
        assert plan.engine == Engine.STARBURST

    def test_heavy_compute_offload_group_by(self, router):
        """Heavy GROUP BY on non-local data → offload to Starburst."""
        plan = router.route("SELECT region, COUNT(*) FROM finance.sales GROUP BY region")
        assert plan.engine == Engine.STARBURST
        assert RoutingReason.HEAVY_COMPUTE in plan.reasons or RoutingReason.CROSS_TEAM_REF in plan.reasons

    def test_heavy_compute_offload_window(self, router):
        """Window functions on non-local data → offload to Starburst."""
        plan = router.route(
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) FROM finance.events"
        )
        assert plan.engine == Engine.STARBURST

    def test_starburst_offload_flag(self, router):
        plan = router.route("SELECT * FROM finance.orders")
        assert plan.offload_to_starburst is True


# ── PATH C: Hybrid (DuckDB orchestrates, Starburst computes) ─────────────

class TestHybridRouting:
    """DuckDB pulls owned data, Starburst resolves unowned, DuckDB joins."""

    def test_cross_join_owned_and_unowned(self, router):
        plan = router.route("SELECT * FROM mydb.events e JOIN finance.orders o ON e.order_id = o.id")
        assert plan.engine == Engine.HYBRID
        assert plan.join_strategy == JoinStrategy.HYBRID_PULL
        assert RoutingReason.CROSS_JOIN in plan.reasons
        assert len(plan.tables_owned) > 0
        assert len(plan.tables_unowned) > 0

    def test_hybrid_has_both_sql(self, router):
        plan = router.route("SELECT * FROM mydb.events e JOIN other_team.data d ON e.id = d.event_id")
        assert plan.engine == Engine.HYBRID
        assert plan.pushdown_sql is not None
        assert plan.local_sql is not None


# ── Hint parsing ──────────────────────────────────────────────────────────

class TestHintParsing:
    def test_snapshot_hint(self, router):
        plan = router.route("SELECT * FROM mydb.events /*+ SNAPSHOT(12345678) */")
        assert plan.snapshot_id == 12345678

    def test_as_of_hint(self, router):
        plan = router.route("SELECT * FROM mydb.events /*+ AS_OF('2025-04-01 00:00:00') */")
        assert plan.as_of_timestamp == "2025-04-01 00:00:00"

    def test_engine_hint_case_insensitive(self, router):
        plan = router.route("SELECT * FROM mydb.events /*+ engine(STARBURST) */")
        assert plan.engine == Engine.STARBURST

    def test_explicit_duckdb_hint(self, router):
        plan = router.route("SELECT * FROM mydb.events /*+ ENGINE(duckdb) */")
        assert plan.engine == Engine.DUCKDB
        assert RoutingReason.EXPLICIT_HINT in plan.reasons

    def test_no_hints(self, router):
        plan = router.route("SELECT * FROM mydb.events")
        assert plan.snapshot_id is None
        assert plan.as_of_timestamp is None


# ── Edge cases ────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_query(self, router):
        plan = router.route("")
        assert plan.engine == Engine.DUCKDB

    def test_multiple_statements(self, router):
        plan = router.route("SELECT 1; SELECT * FROM mydb.events")
        assert plan.engine == Engine.DUCKDB

    def test_union_owned(self, router):
        plan = router.route("SELECT * FROM mydb.events UNION ALL SELECT * FROM team_alpha.events")
        assert plan.engine == Engine.DUCKDB

    def test_tables_owned_populated(self, router):
        plan = router.route("SELECT * FROM mydb.events")
        assert "mydb.events" in plan.tables_owned

    def test_tables_unowned_populated(self, router):
        plan = router.route("SELECT * FROM finance.transactions")
        assert len(plan.tables_unowned) > 0

    def test_insert_into_is_data_prep(self, router):
        plan = router.route("INSERT INTO mydb.target SELECT * FROM mydb.source")
        assert plan.engine == Engine.DUCKDB
        assert RoutingReason.DATA_PREP in plan.reasons
