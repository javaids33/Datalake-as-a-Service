"""
Query result cache — LRU with SHA256 key, Iceberg-snapshot-aware invalidation.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import OrderedDict
from typing import Any

from dlsidecar.config import settings
from dlsidecar.governance.metrics import QUERY_CACHE_HITS, QUERY_CACHE_MISSES

logger = logging.getLogger("dlsidecar.engine.cache")


class CacheEntry:
    __slots__ = ("result", "created_at", "snapshot_id", "size_bytes")

    def __init__(self, result: Any, snapshot_id: int | None = None, size_bytes: int = 0):
        self.result = result
        self.created_at = time.monotonic()
        self.snapshot_id = snapshot_id
        self.size_bytes = size_bytes


class QueryCache:
    """LRU cache for query results with TTL and Iceberg snapshot invalidation."""

    def __init__(self, max_entries: int = 1000, ttl: int | None = None):
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_entries = max_entries
        self._ttl = ttl or settings.starburst_results_cache_ttl

    @staticmethod
    def _key(sql: str) -> str:
        normalized = " ".join(sql.split()).strip().lower()
        return hashlib.sha256(normalized.encode()).hexdigest()

    def get(self, sql: str) -> Any | None:
        key = self._key(sql)
        entry = self._cache.get(key)
        if entry is None:
            QUERY_CACHE_MISSES.labels(tenant=settings.tenant_id).inc()
            return None

        # TTL check
        if time.monotonic() - entry.created_at > self._ttl:
            del self._cache[key]
            QUERY_CACHE_MISSES.labels(tenant=settings.tenant_id).inc()
            return None

        self._cache.move_to_end(key)
        QUERY_CACHE_HITS.labels(tenant=settings.tenant_id).inc()
        return entry.result

    def put(self, sql: str, result: Any, *, snapshot_id: int | None = None, size_bytes: int = 0) -> None:
        key = self._key(sql)
        self._cache[key] = CacheEntry(result, snapshot_id=snapshot_id, size_bytes=size_bytes)
        self._cache.move_to_end(key)

        while len(self._cache) > self._max_entries:
            self._cache.popitem(last=False)

    def invalidate_snapshot(self, snapshot_id: int) -> int:
        """Invalidate all cache entries associated with a given Iceberg snapshot."""
        keys_to_remove = [k for k, v in self._cache.items() if v.snapshot_id == snapshot_id]
        for k in keys_to_remove:
            del self._cache[k]
        if keys_to_remove:
            logger.info("Invalidated %d cache entries for snapshot %d", len(keys_to_remove), snapshot_id)
        return len(keys_to_remove)

    def clear(self) -> None:
        self._cache.clear()

    @property
    def size(self) -> int:
        return len(self._cache)
