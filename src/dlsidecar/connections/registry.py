"""
Connection registry — manages startup/shutdown sequence for all data sources,
runs a health loop, and implements reconnect with exponential backoff.
readyz blocks until all enabled sources are healthy.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from dlsidecar.governance.metrics import CONNECTION_RECONNECTS, CONNECTION_STATUS
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.connections")


class ConnectionRegistry:
    """Central registry for all data source connections."""

    def __init__(self, health_interval: float = 30.0, max_backoff: float = 300.0):
        self._sources: dict[str, Source] = {}
        self._health: dict[str, dict[str, Any]] = {}
        self._health_interval = health_interval
        self._max_backoff = max_backoff
        self._backoff: dict[str, float] = {}
        self._health_task: asyncio.Task | None = None
        self._ready = asyncio.Event()

    def register(self, source: Source) -> None:
        self._sources[source.name] = source
        self._health[source.name] = {"healthy": False, "latency_ms": None, "error": "not connected"}
        self._backoff[source.name] = 1.0

    @property
    def sources(self) -> dict[str, Source]:
        return dict(self._sources)

    def get(self, name: str) -> Source | None:
        return self._sources.get(name)

    def health_status(self) -> dict[str, dict[str, Any]]:
        return dict(self._health)

    @property
    def all_healthy(self) -> bool:
        if not self._sources:
            return True
        return all(h.get("healthy", False) for h in self._health.values())

    async def startup(self) -> None:
        """Connect all registered sources. Sets ready event when all healthy."""
        tasks = []
        for name, source in self._sources.items():
            tasks.append(self._connect_source(name, source))
        await asyncio.gather(*tasks, return_exceptions=True)

        if self.all_healthy:
            self._ready.set()

        self._health_task = asyncio.create_task(self._health_loop())

    async def shutdown(self) -> None:
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        tasks = []
        for name, source in self._sources.items():
            tasks.append(self._disconnect_source(name, source))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_ready(self, timeout: float = 60.0) -> bool:
        """Wait until all sources are healthy. Returns False on timeout."""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for all sources to become healthy")
            return False

    async def _connect_source(self, name: str, source: Source) -> None:
        try:
            await source.connect()
            health = await source.health_check()
            self._health[name] = health
            self._backoff[name] = 1.0
            if health.get("healthy"):
                CONNECTION_STATUS.labels(source=name).set(1)
                logger.info("Source %s connected and healthy", name)
            else:
                CONNECTION_STATUS.labels(source=name).set(0)
                logger.warning("Source %s connected but unhealthy: %s", name, health.get("error"))
        except Exception as exc:
            self._health[name] = {"healthy": False, "latency_ms": None, "error": str(exc)}
            CONNECTION_STATUS.labels(source=name).set(0)
            logger.error("Failed to connect source %s: %s", name, exc)

    async def _disconnect_source(self, name: str, source: Source) -> None:
        try:
            await source.disconnect()
            logger.info("Source %s disconnected", name)
        except Exception as exc:
            logger.error("Error disconnecting source %s: %s", name, exc)

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(self._health_interval)
            for name, source in self._sources.items():
                try:
                    start = time.monotonic()
                    health = await source.health_check()
                    health["latency_ms"] = round((time.monotonic() - start) * 1000, 2)
                    self._health[name] = health

                    if health.get("healthy"):
                        CONNECTION_STATUS.labels(source=name).set(1)
                        self._backoff[name] = 1.0
                    else:
                        CONNECTION_STATUS.labels(source=name).set(0)
                        await self._try_reconnect(name, source)
                except Exception as exc:
                    self._health[name] = {"healthy": False, "latency_ms": None, "error": str(exc)}
                    CONNECTION_STATUS.labels(source=name).set(0)
                    await self._try_reconnect(name, source)

            if self.all_healthy:
                self._ready.set()
            else:
                self._ready.clear()

    async def _try_reconnect(self, name: str, source: Source) -> None:
        backoff = self._backoff[name]
        logger.warning("Source %s unhealthy, reconnecting in %.1fs", name, backoff)
        CONNECTION_RECONNECTS.labels(source=name).inc()
        await asyncio.sleep(backoff)
        self._backoff[name] = min(backoff * 2, self._max_backoff)
        await self._connect_source(name, source)
