"""
HMS (Hive Metastore) source — PyHive Thrift client for table registration,
listing, and schema operations. The sidecar writes only to its own namespace.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.hms")


class HMSSource(Source):
    name = "hms"

    def __init__(self):
        self._client = None

    async def connect(self) -> None:
        try:
            from pyhive import hive

            # Parse thrift URI: thrift://host:port
            uri = settings.hms_uri
            if uri.startswith("thrift://"):
                uri = uri[len("thrift://"):]
            host, _, port = uri.partition(":")
            port = int(port) if port else 9083

            self._client = hive.connect(
                host=host,
                port=port,
                auth=settings.hms_auth,
                database=settings.hms_database,
            )
            logger.info("Connected to HMS at %s:%d", host, port)
        except Exception as exc:
            logger.error("Failed to connect to HMS: %s", exc)
            raise

    async def disconnect(self) -> None:
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    async def health_check(self) -> dict[str, Any]:
        if self._client is None:
            return {"healthy": False, "latency_ms": None, "error": "not connected"}
        try:
            start = time.monotonic()
            cursor = self._client.cursor()
            cursor.execute("SHOW DATABASES")
            cursor.fetchall()
            latency = (time.monotonic() - start) * 1000
            return {"healthy": True, "latency_ms": round(latency, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if self._client is None:
            return []
        try:
            cursor = self._client.cursor()
            cursor.execute(f"SHOW TABLES IN {settings.hms_database}")
            rows = cursor.fetchall()
            return [f"{settings.hms_database}.{row[0]}" for row in rows]
        except Exception as exc:
            logger.error("Failed to list HMS tables: %s", exc)
            return []

    def register_table(
        self,
        database: str,
        table_name: str,
        location: str,
        schema_columns: list[dict[str, str]],
        partition_columns: list[dict[str, str]] | None = None,
        table_type: str = "EXTERNAL_TABLE",
        serde: str = "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
        input_format: str = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
        output_format: str = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
        properties: dict[str, str] | None = None,
    ) -> None:
        """Register or update a table in HMS so Starburst can see it."""
        if self._client is None:
            raise RuntimeError("HMS not connected")

        cursor = self._client.cursor()

        cols_sql = ", ".join(f"`{c['name']}` {c['type']}" for c in schema_columns)
        partition_sql = ""
        if partition_columns:
            pcols = ", ".join(f"`{c['name']}` {c['type']}" for c in partition_columns)
            partition_sql = f"PARTITIONED BY ({pcols})"

        props = properties or {}
        props_sql = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
        tblproperties = f"TBLPROPERTIES ({props_sql})" if props_sql else ""

        ddl = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table_name}` ({cols_sql})
            {partition_sql}
            ROW FORMAT SERDE '{serde}'
            STORED AS INPUTFORMAT '{input_format}' OUTPUTFORMAT '{output_format}'
            LOCATION '{location}'
            {tblproperties}
        """
        try:
            cursor.execute(ddl)
            logger.info("Registered table %s.%s in HMS at %s", database, table_name, location)
        except Exception as exc:
            logger.error("Failed to register table %s.%s: %s", database, table_name, exc)
            raise

    def alter_table_location(self, database: str, table_name: str, new_location: str) -> None:
        if self._client is None:
            raise RuntimeError("HMS not connected")
        cursor = self._client.cursor()
        cursor.execute(f"ALTER TABLE `{database}`.`{table_name}` SET LOCATION '{new_location}'")

    def msck_repair(self, database: str, table_name: str) -> None:
        """Repair table partitions in HMS."""
        if self._client is None:
            raise RuntimeError("HMS not connected")
        cursor = self._client.cursor()
        cursor.execute(f"MSCK REPAIR TABLE `{database}`.`{table_name}`")

    def table_exists(self, database: str, table_name: str) -> bool:
        if self._client is None:
            return False
        try:
            cursor = self._client.cursor()
            cursor.execute(f"DESCRIBE `{database}`.`{table_name}`")
            cursor.fetchall()
            return True
        except Exception:
            return False
