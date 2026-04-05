"""
MotherDuck scale-out bridge — routes queries to MotherDuck when local DuckDB
exceeds memory/size thresholds.
"""

from __future__ import annotations

import logging

from dlsidecar.config import settings

logger = logging.getLogger("dlsidecar.engine.scale_router")


class ScaleRouter:
    """Decides when to offload queries to MotherDuck."""

    def __init__(self):
        self._motherduck_conn = None

    @property
    def enabled(self) -> bool:
        return settings.motherduck_enabled and bool(settings.motherduck_token)

    def should_scale_out(self, estimated_bytes: int | None = None, memory_percent: float | None = None) -> bool:
        if not self.enabled:
            return False
        if memory_percent and memory_percent > settings.scale_threshold_memory_percent:
            logger.info("Memory threshold exceeded (%.1f%%), scaling to MotherDuck", memory_percent)
            return True
        if estimated_bytes and estimated_bytes > settings.scale_threshold_query_bytes:
            logger.info("Query size threshold exceeded (%d bytes), scaling to MotherDuck", estimated_bytes)
            return True
        return False

    async def connect(self) -> None:
        if not self.enabled:
            return
        try:
            import duckdb

            md_db = settings.motherduck_database or "my_db"
            self._motherduck_conn = duckdb.connect(f"md:{md_db}?motherduck_token={settings.motherduck_token}")
            logger.info("Connected to MotherDuck database: %s", md_db)
        except Exception as exc:
            logger.error("Failed to connect to MotherDuck: %s", exc)

    async def disconnect(self) -> None:
        if self._motherduck_conn:
            self._motherduck_conn.close()
            self._motherduck_conn = None
