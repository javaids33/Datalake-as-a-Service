"""MySQL source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.mysql")


class MySQLSource(Source):
    name = "mysql"

    def __init__(self):
        self._conn = None

    async def connect(self) -> None:
        if not settings.mysql_enabled:
            return
        import mysql.connector

        # Parse DSN: mysql://user:pass@host:port/db
        dsn = settings.mysql_dsn
        self._conn = mysql.connector.connect(dsn=dsn) if dsn else None
        logger.info("Connected to MySQL")

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
        return [row[0] for row in cursor.fetchall()]
