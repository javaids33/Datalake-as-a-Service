"""Delta Lake source connector."""

from __future__ import annotations

import logging
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.delta")


class DeltaSource(Source):
    name = "delta"

    def __init__(self):
        self._tables: list[str] = []

    async def connect(self) -> None:
        if not settings.delta_enabled:
            return
        self._tables = [p.strip() for p in settings.delta_table_paths.split(",") if p.strip()]
        logger.info("Delta source configured with %d table paths", len(self._tables))

    async def disconnect(self) -> None:
        self._tables = []

    async def health_check(self) -> dict[str, Any]:
        if not self._tables:
            return {"healthy": False, "latency_ms": None, "error": "no tables configured"}
        return {"healthy": True, "latency_ms": None, "error": None}

    async def list_tables(self) -> list[str]:
        return self._tables
