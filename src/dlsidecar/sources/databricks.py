"""Databricks source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.databricks")


class DatabricksSource(Source):
    name = "databricks"

    def __init__(self):
        self._conn = None

    async def connect(self) -> None:
        if not settings.databricks_enabled:
            return
        from databricks import sql

        self._conn = sql.connect(
            server_hostname=settings.databricks_host,
            http_path=settings.databricks_http_path,
            catalog=settings.databricks_catalog or None,
        )
        logger.info("Connected to Databricks: %s", settings.databricks_host)

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    async def health_check(self) -> dict[str, Any]:
        if not self._conn:
            return {"healthy": False, "latency_ms": None, "error": "not connected"}
        try:
            start = time.monotonic()
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return {"healthy": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if not self._conn:
            return []
        cursor = self._conn.cursor()
        cursor.execute("SHOW TABLES")
        return [row[1] for row in cursor.fetchall()]
