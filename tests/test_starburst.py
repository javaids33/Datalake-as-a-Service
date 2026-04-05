"""Test Starburst: thin DuckDB wrapper, catalog attachment, JWT refresh."""

import os

os.environ.setdefault("DLS_MODE", "dev")
os.environ.setdefault("DLS_STARBURST_HOST", "")

import pytest

from dlsidecar.engine.duckdb_engine import DuckDBEngine
from dlsidecar.sources.starburst import StarburstSource


class TestStarburstSource:
    def test_init_requires_duckdb_engine(self):
        engine = DuckDBEngine()
        engine.startup()
        s = StarburstSource(duckdb_engine=engine)
        assert s.name == "starburst"
        engine.shutdown()

    @pytest.mark.asyncio
    async def test_health_check_no_attachment(self):
        engine = DuckDBEngine()
        engine.startup()
        s = StarburstSource(duckdb_engine=engine)
        health = await s.health_check()
        assert health["healthy"] is False
        assert "not attached" in health["error"]
        engine.shutdown()

    @pytest.mark.asyncio
    async def test_list_tables_no_attachment(self):
        engine = DuckDBEngine()
        engine.startup()
        s = StarburstSource(duckdb_engine=engine)
        tables = await s.list_tables()
        assert tables == []
        engine.shutdown()

    def test_refresh_jwt_no_crash(self):
        engine = DuckDBEngine()
        engine.startup()
        s = StarburstSource(duckdb_engine=engine)
        # Should not crash even without a real Starburst endpoint
        s.refresh_jwt("new-token")
        engine.shutdown()


class TestDuckDBCatalogAttachment:
    def test_attach_trino_catalog_no_host(self):
        engine = DuckDBEngine()
        engine.startup()
        # Attaching with empty host will fail gracefully
        result = engine.attach_trino_catalog(host="", port=8443)
        assert result is False
        engine.shutdown()

    def test_detach_nonexistent(self):
        engine = DuckDBEngine()
        engine.startup()
        # Should not crash
        engine.detach_catalog("nonexistent")
        engine.shutdown()

    def test_attached_catalogs_tracking(self):
        engine = DuckDBEngine()
        engine.startup()
        assert engine.attached_catalogs == {}
        engine.shutdown()

    def test_attach_datasource_invalid(self):
        engine = DuckDBEngine()
        engine.startup()
        result = engine.attach_datasource("test", "invalid://dsn", "POSTGRES")
        assert result is False
        engine.shutdown()

    def test_trino_health_check_not_attached(self):
        engine = DuckDBEngine()
        engine.startup()
        health = engine.trino_health_check("starburst")
        assert health["healthy"] is False
        assert "not attached" in health["error"]
        engine.shutdown()

    def test_list_catalog_tables_not_attached(self):
        engine = DuckDBEngine()
        engine.startup()
        tables = engine.list_catalog_tables("starburst")
        assert tables == []
        engine.shutdown()
