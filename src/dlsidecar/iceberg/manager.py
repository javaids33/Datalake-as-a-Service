"""
IcebergManager — write data, schema evolution, HMS registration, post-write metrics.
Uses pyiceberg for all write operations (never raw S3 puts for Iceberg tables).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import pyarrow as pa

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event
from dlsidecar.governance.metrics import (
    BYTES_WRITTEN_TOTAL,
    ICEBERG_COMMITS_TOTAL,
    ROWS_WRITTEN_TOTAL,
    SCHEMA_EVOLUTIONS,
    WRITES_TOTAL,
)

logger = logging.getLogger("dlsidecar.iceberg.manager")


class IcebergSchemaConflictError(Exception):
    """Raised when a write would narrow types or drop columns."""


class IcebergManager:
    """Manages Iceberg table lifecycle: write, schema evolution, HMS registration."""

    def __init__(self):
        self._catalog = None
        self._hms_source = None

    def set_catalog(self, catalog) -> None:
        """Set the pyiceberg catalog instance."""
        self._catalog = catalog

    def set_hms(self, hms_source) -> None:
        """Set the HMS source for table registration."""
        self._hms_source = hms_source

    def _get_catalog(self):
        if self._catalog is not None:
            return self._catalog

        from pyiceberg.catalog import load_catalog

        catalog_config: dict[str, Any] = {
            "type": settings.iceberg_catalog_type,
        }
        if settings.iceberg_catalog_type == "glue":
            catalog_config["warehouse"] = settings.iceberg_warehouse
            if settings.iceberg_glue_catalog_id:
                catalog_config["catalog-id"] = settings.iceberg_glue_catalog_id
        elif settings.iceberg_catalog_type == "rest":
            catalog_config["uri"] = settings.iceberg_catalog_uri
            catalog_config["warehouse"] = settings.iceberg_warehouse
        elif settings.iceberg_catalog_type == "hive":
            catalog_config["uri"] = settings.hms_uri
            catalog_config["warehouse"] = settings.iceberg_warehouse

        self._catalog = load_catalog("default", **catalog_config)
        return self._catalog

    def write(
        self,
        database: str,
        table_name: str,
        data: pa.Table,
        *,
        mode: str = "append",
        partition_by: list[str] | None = None,
        register_hms: bool = True,
    ) -> dict[str, Any]:
        """Write data to an Iceberg table with full lifecycle management.

        Returns dict with: snapshot_id, files_added, bytes_written, rows_written
        """
        start = time.monotonic()
        catalog = self._get_catalog()
        table_id = f"{database}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            # Schema evolution check
            self._evolve_schema_if_needed(table, data.schema, table_id)
        except Exception:
            # Table doesn't exist — create it
            table = self._create_table(catalog, database, table_name, data.schema, partition_by)

        # Write via pyiceberg
        if mode == "overwrite":
            table.overwrite(data)
        else:
            table.append(data)

        # Get latest snapshot info
        table.refresh()
        current_snapshot = table.current_snapshot()
        snapshot_id = current_snapshot.snapshot_id if current_snapshot else None

        # Commit summary properties
        summary = {
            "operation": mode,
            "written-by": "dlsidecar",
            "pod-name": os.environ.get("POD_NAME", "unknown"),
            "tenant": settings.tenant_id,
        }

        duration_ms = (time.monotonic() - start) * 1000
        rows_written = len(data)
        bytes_written = data.nbytes

        # Metrics
        WRITES_TOTAL.labels(format="iceberg", mode=mode, tenant=settings.tenant_id).inc()
        BYTES_WRITTEN_TOTAL.labels(format="iceberg", tenant=settings.tenant_id).inc(bytes_written)
        ROWS_WRITTEN_TOTAL.labels(format="iceberg", tenant=settings.tenant_id).inc(rows_written)
        ICEBERG_COMMITS_TOTAL.labels(tenant=settings.tenant_id).inc()

        # Audit
        emit_audit_event(
            "write",
            engine="duckdb",
            tables_written=[table_id],
            duration_ms=round(duration_ms, 2),
            iceberg_snapshot_id=snapshot_id,
            extra={
                "rows_written": rows_written,
                "bytes_written": bytes_written,
                "write_mode": mode,
            },
        )

        # Register in HMS
        if register_hms and self._hms_source:
            self._register_in_hms(database, table_name, table)

        result = {
            "snapshot_id": snapshot_id,
            "files_added": current_snapshot.summary.get("added-data-files", "0") if current_snapshot and current_snapshot.summary else "0",
            "bytes_written": bytes_written,
            "rows_written": rows_written,
        }
        logger.info("Wrote %d rows to %s (snapshot=%s)", rows_written, table_id, snapshot_id)
        return result

    def _create_table(self, catalog, database: str, table_name: str, schema: pa.Schema, partition_by: list[str] | None):
        """Create a new Iceberg table from Arrow schema."""
        from pyiceberg.schema import Schema as IcebergSchema
        from pyiceberg.types import (
            BooleanType,
            DateType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            NestedField,
            StringType,
            TimestampType,
        )

        # Convert Arrow schema to Iceberg schema
        iceberg_fields = []
        for i, field in enumerate(schema):
            iceberg_type = self._arrow_to_iceberg_type(field.type)
            iceberg_fields.append(
                NestedField(
                    field_id=i + 1,
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable,
                )
            )
        iceberg_schema = IcebergSchema(*iceberg_fields)

        table_id = f"{database}.{table_name}"

        # Create with optional partitioning
        if partition_by:
            from pyiceberg.partitioning import PartitionField, PartitionSpec
            from pyiceberg.transforms import IdentityTransform

            partition_fields = []
            for j, col in enumerate(partition_by):
                source_id = next((f.field_id for f in iceberg_fields if f.name == col), None)
                if source_id is not None:
                    partition_fields.append(
                        PartitionField(
                            source_id=source_id,
                            field_id=1000 + j,
                            transform=IdentityTransform(),
                            name=col,
                        )
                    )
            spec = PartitionSpec(*partition_fields)
            table = catalog.create_table(table_id, schema=iceberg_schema, partition_spec=spec)
        else:
            table = catalog.create_table(table_id, schema=iceberg_schema)

        logger.info("Created Iceberg table: %s", table_id)
        return table

    def _evolve_schema_if_needed(self, table, new_schema: pa.Schema, table_id: str) -> None:
        """Check for schema differences and evolve safely."""
        existing = table.schema()
        existing_names = {f.name: f for f in existing.fields}

        for field in new_schema:
            if field.name not in existing_names:
                # New column — safe to add
                iceberg_type = self._arrow_to_iceberg_type(field.type)
                with table.update_schema() as update:
                    update.add_column(field.name, iceberg_type)
                SCHEMA_EVOLUTIONS.labels(table=table_id).inc()
                logger.info("Schema evolution: added column %s to %s", field.name, table_id)
            else:
                # Check for unsafe type changes
                existing_field = existing_names[field.name]
                if not self._is_safe_type_change(existing_field.field_type, field.type):
                    raise IcebergSchemaConflictError(
                        f"Unsafe schema change for {table_id}.{field.name}: "
                        f"{existing_field.field_type} → {field.type} (type narrowing/drop not allowed)"
                    )

    def _is_safe_type_change(self, existing_iceberg_type, new_arrow_type) -> bool:
        """Check if a type change is safe (widening only)."""
        from pyiceberg.types import DoubleType, FloatType, IntegerType, LongType

        # Same type is always safe
        # Widen: int → long, float → double
        type_name = str(existing_iceberg_type)
        if type_name == "int" and str(new_arrow_type) in ("int64", "int32"):
            return True
        if type_name == "long" and str(new_arrow_type) == "int64":
            return True
        if type_name == "float" and str(new_arrow_type) in ("double", "float64", "float32"):
            return True
        if type_name == "double" and str(new_arrow_type) in ("double", "float64"):
            return True
        # For string and other types, same-type is safe
        return True

    def _arrow_to_iceberg_type(self, arrow_type):
        """Convert an Arrow type to an Iceberg type."""
        from pyiceberg.types import (
            BinaryType,
            BooleanType,
            DateType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            StringType,
            TimestampType,
            TimestamptzType,
        )

        type_str = str(arrow_type)
        mapping = {
            "int8": IntegerType(),
            "int16": IntegerType(),
            "int32": IntegerType(),
            "int64": LongType(),
            "uint8": IntegerType(),
            "uint16": IntegerType(),
            "uint32": LongType(),
            "uint64": LongType(),
            "float16": FloatType(),
            "float32": FloatType(),
            "float": FloatType(),
            "float64": DoubleType(),
            "double": DoubleType(),
            "bool": BooleanType(),
            "string": StringType(),
            "utf8": StringType(),
            "large_string": StringType(),
            "large_utf8": StringType(),
            "binary": BinaryType(),
            "large_binary": BinaryType(),
            "date32": DateType(),
            "date32[day]": DateType(),
        }
        if type_str in mapping:
            return mapping[type_str]
        if "timestamp" in type_str:
            if "tz" in type_str or "UTC" in type_str:
                return TimestamptzType()
            return TimestampType()
        return StringType()

    def _register_in_hms(self, database: str, table_name: str, table) -> None:
        """Register/update table in HMS so Starburst can see it."""
        if not self._hms_source:
            return
        try:
            location = str(table.location()) if hasattr(table, "location") else ""
            schema_columns = []
            for field in table.schema().fields:
                schema_columns.append({"name": field.name, "type": str(field.field_type)})

            self._hms_source.register_table(
                database=database,
                table_name=table_name,
                location=location,
                schema_columns=schema_columns,
                properties={"table_type": "ICEBERG"},
            )
        except Exception as exc:
            logger.error("Failed to register %s.%s in HMS: %s", database, table_name, exc)
