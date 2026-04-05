"""MongoDB source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.mongo")


class MongoSource(Source):
    name = "mongo"

    def __init__(self):
        self._client = None

    async def connect(self) -> None:
        if not settings.mongo_enabled:
            return
        from pymongo import MongoClient

        self._client = MongoClient(settings.mongo_uri)
        logger.info("Connected to MongoDB")

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    async def health_check(self) -> dict[str, Any]:
        if not self._client:
            return {"healthy": False, "latency_ms": None, "error": "not connected"}
        try:
            start = time.monotonic()
            self._client.admin.command("ping")
            return {"healthy": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if not self._client:
            return []
        tables = []
        for db_name in self._client.list_database_names():
            if db_name in ("admin", "local", "config"):
                continue
            for coll in self._client[db_name].list_collection_names():
                tables.append(f"{db_name}.{coll}")
        return tables
