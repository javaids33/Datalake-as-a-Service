"""
Iceberg reader — scan tables with time-travel support via query hints.

Supports:
  SELECT * FROM mydb.events /*+ SNAPSHOT(12345678) */
  SELECT * FROM mydb.events /*+ AS_OF('2025-04-01 00:00:00') */
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa

from dlsidecar.config import settings

logger = logging.getLogger("dlsidecar.iceberg.reader")


class IcebergReader:
    """Reads Iceberg tables with time-travel support."""

    def __init__(self, catalog=None):
        self._catalog = catalog

    def set_catalog(self, catalog) -> None:
        self._catalog = catalog

    def _get_catalog(self):
        if self._catalog is not None:
            return self._catalog

        from pyiceberg.catalog import load_catalog

        config: dict[str, Any] = {"type": settings.iceberg_catalog_type}
        if settings.iceberg_catalog_type == "glue":
            config["warehouse"] = settings.iceberg_warehouse
            if settings.iceberg_glue_catalog_id:
                config["catalog-id"] = settings.iceberg_glue_catalog_id
        elif settings.iceberg_catalog_type == "rest":
            config["uri"] = settings.iceberg_catalog_uri
            config["warehouse"] = settings.iceberg_warehouse
        elif settings.iceberg_catalog_type == "hive":
            config["uri"] = settings.hms_uri
            config["warehouse"] = settings.iceberg_warehouse

        self._catalog = load_catalog("default", **config)
        return self._catalog

    def scan(
        self,
        database: str,
        table_name: str,
        *,
        snapshot_id: int | None = None,
        as_of_timestamp: str | None = None,
        columns: list[str] | None = None,
        row_filter: str | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        """Scan an Iceberg table, optionally at a specific snapshot or timestamp."""
        catalog = self._get_catalog()
        table = catalog.load_table(f"{database}.{table_name}")

        scan = table.scan()

        if snapshot_id is not None:
            scan = scan.use_ref(f"snapshot-{snapshot_id}")
            logger.info("Time-travel scan: snapshot_id=%d", snapshot_id)

        if columns:
            scan = scan.select(*columns)

        if row_filter:
            scan = scan.filter(row_filter)

        result = scan.to_arrow()

        if limit is not None:
            result = result.slice(0, limit)

        logger.info(
            "Scanned %s.%s: %d rows, snapshot=%s",
            database,
            table_name,
            len(result),
            snapshot_id or "latest",
        )
        return result

    def scan_duckdb_sql(
        self,
        s3_path: str,
        *,
        snapshot_id: int | None = None,
        as_of_timestamp: str | None = None,
    ) -> str:
        """Generate DuckDB SQL for scanning an Iceberg table via iceberg_scan().

        This is used when routing to DuckDB directly for owned tables.
        """
        parts = [f"iceberg_scan('{s3_path}'"]
        if snapshot_id is not None:
            parts.append(f", snapshot_id={snapshot_id}")
        if as_of_timestamp is not None:
            parts.append(f", as_of_timestamp='{as_of_timestamp}'")
        parts.append(")")
        return "".join(parts)

    def list_snapshots(self, database: str, table_name: str) -> list[dict[str, Any]]:
        """List all snapshots for a table."""
        catalog = self._get_catalog()
        table = catalog.load_table(f"{database}.{table_name}")

        snapshots = []
        for snapshot in table.metadata.snapshots:
            snapshots.append({
                "snapshot_id": snapshot.snapshot_id,
                "timestamp_ms": snapshot.timestamp_ms,
                "summary": dict(snapshot.summary) if snapshot.summary else {},
                "parent_snapshot_id": snapshot.parent_snapshot_id,
            })
        return snapshots
