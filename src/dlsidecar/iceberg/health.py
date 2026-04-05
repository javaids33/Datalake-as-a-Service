"""
Iceberg health report — per-table health check endpoint.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from dlsidecar.config import settings

logger = logging.getLogger("dlsidecar.iceberg.health")


class IcebergHealth:
    """Generates per-table health reports for the /iceberg/health endpoint."""

    def __init__(self, catalog=None, hms_source=None, starburst_source=None):
        self._catalog = catalog
        self._hms = hms_source
        self._starburst = starburst_source
        self._last_compaction: dict[str, str] = {}
        self._last_snapshot_expiry: dict[str, str] = {}

    def set_catalog(self, catalog) -> None:
        self._catalog = catalog

    def record_compaction(self, table_id: str) -> None:
        self._last_compaction[table_id] = datetime.now(timezone.utc).isoformat()

    def record_snapshot_expiry(self, table_id: str) -> None:
        self._last_snapshot_expiry[table_id] = datetime.now(timezone.utc).isoformat()

    def get_health(self, table_id: str | None = None) -> list[dict[str, Any]]:
        """Get health report for one table or all managed tables."""
        if self._catalog is None:
            return []

        tables = [table_id] if table_id else self._list_managed_tables()
        reports = []

        for tid in tables:
            try:
                report = self._table_health(tid)
                reports.append(report)
            except Exception as exc:
                reports.append({"table": tid, "error": str(exc)})

        return reports

    def _table_health(self, table_id: str) -> dict[str, Any]:
        table = self._catalog.load_table(table_id)
        current = table.current_snapshot()

        snapshot_count = len(table.metadata.snapshots) if table.metadata.snapshots else 0

        # File stats
        file_count = 0
        total_bytes = 0
        small_file_count = 0
        if current:
            try:
                for manifest in current.manifests(table.io):
                    for entry in manifest.fetch_manifest_entry(table.io):
                        file_count += 1
                        size = entry.data_file.file_size_in_bytes
                        total_bytes += size
                        if size < 32 * 1024 * 1024:
                            small_file_count += 1
            except Exception:
                pass

        avg_mb = round(total_bytes / file_count / 1024 / 1024, 1) if file_count > 0 else 0

        # HMS check
        hms_registered = False
        if self._hms:
            parts = table_id.split(".")
            if len(parts) == 2:
                hms_registered = self._hms.table_exists(parts[0], parts[1])

        # Starburst visibility check
        starburst_visible = False
        if self._starburst:
            try:
                result = self._starburst.execute_query(f"DESCRIBE {table_id}")
                starburst_visible = len(result) > 0
            except Exception:
                pass

        return {
            "table": table_id,
            "snapshot_count": snapshot_count,
            "avg_file_size_mb": avg_mb,
            "small_file_count": small_file_count,
            "orphan_risk": small_file_count > 100,
            "last_compaction": self._last_compaction.get(table_id),
            "last_snapshot_expiry": self._last_snapshot_expiry.get(table_id),
            "hms_registered": hms_registered,
            "starburst_visible": starburst_visible,
        }

    def _list_managed_tables(self) -> list[str]:
        tables = []
        for ns in settings.owned_namespace_set:
            try:
                for table_id in self._catalog.list_tables(ns):
                    tables.append(f"{table_id[0]}.{table_id[1]}" if isinstance(table_id, tuple) else str(table_id))
            except Exception:
                pass
        return tables
