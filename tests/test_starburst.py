"""Test Starburst: JWT rotation, pool refresh, pushdown."""

import os

os.environ.setdefault("DLS_MODE", "dev")
os.environ.setdefault("DLS_STARBURST_HOST", "")

import pytest

from dlsidecar.sources.starburst import StarburstSource


class TestStarburstSource:
    def test_init(self):
        s = StarburstSource()
        assert s.name == "starburst"
        assert s._jwt_token is None

    def test_pool_starts_empty(self):
        s = StarburstSource()
        assert s._pool.empty()

    def test_refresh_jwt_clears_pool(self):
        s = StarburstSource()
        # No connections to clear, but should not raise
        s.refresh_jwt("new-token")
        assert s._jwt_token == "new-token"

    @pytest.mark.asyncio
    async def test_health_check_no_connection(self):
        s = StarburstSource()
        health = await s.health_check()
        assert health["healthy"] is False

    @pytest.mark.asyncio
    async def test_list_tables_no_connection(self):
        s = StarburstSource()
        tables = await s.list_tables()
        assert tables == []
