"""PostgreSQL source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.postgres")


class PostgresSource(Source):
    name = "postgres"

    def __init__(self):
        self._conn = None

    async def connect(self) -> None:
        if not settings.postgres_enabled:
            return
        import psycopg

        self._conn = psycopg.connect(settings.postgres_dsn)
        logger.info("Connected to PostgreSQL")

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    async def health_check(self) -> dict[str, Any]:
        if not self._conn:
            return {"healthy": False, "latency_ms": None, "error": "not connected"}
        try:
            start = time.monotonic()
            self._conn.execute("SELECT 1")
            return {"healthy": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if not self._conn:
            return []
        cur = self._conn.execute(
            "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
        )
        return [f"{row[0]}.{row[1]}" for row in cur.fetchall()]
