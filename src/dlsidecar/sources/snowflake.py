"""Snowflake source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.snowflake")


class SnowflakeSource(Source):
    name = "snowflake"

    def __init__(self):
        self._conn = None

    async def connect(self) -> None:
        if not settings.snowflake_enabled:
            return
        import snowflake.connector

        self._conn = snowflake.connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            warehouse=settings.snowflake_warehouse,
            role=settings.snowflake_role or None,
        )
        logger.info("Connected to Snowflake: %s", settings.snowflake_account)

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
