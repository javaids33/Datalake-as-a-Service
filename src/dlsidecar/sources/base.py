"""Abstract base for all data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Source(ABC):
    """Every data source must implement connect, health_check, and list_tables."""

    name: str = "base"

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the source."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the source."""

    @abstractmethod
    async def health_check(self) -> dict[str, Any]:
        """Return health status: {"healthy": bool, "latency_ms": float, "error": str|None}."""

    @abstractmethod
    async def list_tables(self) -> list[str]:
        """Return list of fully-qualified table names available from this source."""
