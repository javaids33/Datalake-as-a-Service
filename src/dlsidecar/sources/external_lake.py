"""External (BYO) data lake source — queryable but not lifecycle-managed."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.external_lake")


class ExternalLakeSource(Source):
    name = "external_lake"

    def __init__(self):
        self._catalog = None

    async def connect(self) -> None:
        if not settings.external_lake_enabled:
            return

        if settings.external_lake_catalog_uri:
            try:
                from pyiceberg.catalog import load_catalog

                self._catalog = load_catalog(
                    "external",
                    type="rest",
                    uri=settings.external_lake_catalog_uri,
                )
                logger.info("Connected to external lake catalog: %s", settings.external_lake_catalog_uri)
            except Exception as exc:
                logger.error("Failed to connect to external lake: %s", exc)

    async def disconnect(self) -> None:
        self._catalog = None

    async def health_check(self) -> dict[str, Any]:
        if not settings.external_lake_enabled:
            return {"healthy": False, "latency_ms": None, "error": "not enabled"}
        if not self._catalog:
            return {"healthy": False, "latency_ms": None, "error": "no catalog"}
        try:
            start = time.monotonic()
            self._catalog.list_namespaces()
            return {"healthy": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if not self._catalog:
            return []
        tables = []
        try:
            for ns in self._catalog.list_namespaces():
                for table_id in self._catalog.list_tables(ns):
                    tables.append(f"external.{table_id[0]}.{table_id[1]}")
        except Exception:
            pass
        return tables
