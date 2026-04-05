"""Test Iceberg maintenance: compaction, snapshot expiry, orphan cleanup, manifest rewrite."""

import os

os.environ.setdefault("DLS_MODE", "dev")

import pytest

from dlsidecar.iceberg.maintenance import IcebergMaintenance


class TestIcebergMaintenance:
    def test_init(self):
        m = IcebergMaintenance()
        assert m._catalog is None

    def test_mark_writing(self):
        m = IcebergMaintenance()
        m.mark_writing("mydb.events")
        assert "mydb.events" in m._running_tables

    def test_mark_write_complete(self):
        m = IcebergMaintenance()
        m.mark_writing("mydb.events")
        m.mark_write_complete("mydb.events")
        assert "mydb.events" not in m._running_tables

    def test_mark_write_complete_idempotent(self):
        m = IcebergMaintenance()
        m.mark_write_complete("nonexistent")  # Should not raise

    def test_set_catalog(self):
        m = IcebergMaintenance()
        m.set_catalog("mock_catalog")
        assert m._catalog == "mock_catalog"
