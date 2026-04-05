"""Google Cloud Storage source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.gcs")


class GCSSource(Source):
    name = "gcs"

    def __init__(self):
        self._fs = None

    async def connect(self) -> None:
        if not settings.gcs_enabled:
            return
        import gcsfs

        self._fs = gcsfs.GCSFileSystem(project=settings.gcs_project)
        logger.info("Connected to GCS: project=%s bucket=%s", settings.gcs_project, settings.gcs_bucket)

    async def disconnect(self) -> None:
        self._fs = None

    async def health_check(self) -> dict[str, Any]:
        if not self._fs:
            return {"healthy": False, "latency_ms": None, "error": "not connected"}
        try:
            start = time.monotonic()
            self._fs.ls(settings.gcs_bucket)
            return {"healthy": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if not self._fs:
            return []
        files = self._fs.ls(settings.gcs_bucket)
        return [f for f in files if f.endswith((".parquet", ".csv", ".json"))]
