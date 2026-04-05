"""Test governance: audit log, row filter injection, metrics."""

import json
import os

os.environ.setdefault("DLS_MODE", "dev")

import pytest

from dlsidecar.governance.audit import _sql_hash, _sql_preview, emit_audit_event
from dlsidecar.governance.row_filter import RowFilterInjector


class TestAuditLog:
    def test_emit_returns_event_dict(self):
        event = emit_audit_event("query", engine="duckdb", sql="SELECT 1")
        assert event["event_type"] == "query"
        assert event["engine"] == "duckdb"
        assert event["ts"] is not None

    def test_sql_hash_normalized(self):
        h1 = _sql_hash("SELECT  * FROM  events")
        h2 = _sql_hash("select * from events")
        assert h1 == h2

    def test_sql_preview_truncated(self):
        long_sql = "SELECT " + "x, " * 200 + "y FROM t"
        preview = _sql_preview(long_sql, max_len=200)
        assert len(preview) == 200

    def test_sql_preview_none(self):
        assert _sql_preview(None) is None

    def test_sql_hash_none(self):
        assert _sql_hash(None) is None

    def test_all_required_fields_present(self):
        event = emit_audit_event("write", engine="duckdb")
        required = [
            "ts", "event_type", "tenant_id", "pod_name", "namespace",
            "engine", "sql_hash", "sql_preview", "tables_accessed",
            "tables_written", "duration_ms", "rows_scanned", "bytes_scanned",
            "rows_returned", "bytes_returned", "engine_routed_reason",
            "iceberg_snapshot_id", "error", "caller_ip", "auth_principal",
        ]
        for field in required:
            assert field in event


class TestRowFilter:
    def test_no_filter_passthrough(self):
        injector = RowFilterInjector(row_filter="")
        result = injector.inject("SELECT * FROM events")
        assert result == "SELECT * FROM events"

    def test_simple_filter_injection(self):
        injector = RowFilterInjector(row_filter="tenant_id = 'team-alpha'")
        result = injector.inject("SELECT * FROM events", dialect="duckdb")
        assert "tenant_id" in result
        assert "team-alpha" in result

    def test_skips_information_schema(self):
        injector = RowFilterInjector(row_filter="tenant_id = 'team-alpha'")
        result = injector.inject("SELECT * FROM information_schema.tables", dialect="duckdb")
        # Should not inject filter into information_schema
        assert "information_schema" in result

    def test_enabled_property(self):
        assert RowFilterInjector(row_filter="x = 1").enabled is True
        assert RowFilterInjector(row_filter="").enabled is False

    def test_starburst_session_property(self):
        injector = RowFilterInjector(row_filter="tenant_id = 'team-alpha'")
        prop = injector.starburst_session_property()
        assert "SET SESSION" in prop
        assert "query_tag" in prop
