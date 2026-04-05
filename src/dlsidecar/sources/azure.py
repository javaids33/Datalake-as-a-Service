"""Azure Blob Storage source connector."""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.azure")


class AzureSource(Source):
    name = "azure"

    def __init__(self):
        self._client = None

    async def connect(self) -> None:
        if not settings.azure_enabled:
            return
        from azure.storage.blob import BlobServiceClient

        self._client = BlobServiceClient(
            account_url=f"https://{settings.azure_storage_account}.blob.core.windows.net"
        )
        logger.info("Connected to Azure Storage: %s", settings.azure_storage_account)

    async def disconnect(self) -> None:
        self._client = None

    async def health_check(self) -> dict[str, Any]:
        if not self._client:
            return {"healthy": False, "latency_ms": None, "error": "not connected"}
        try:
            start = time.monotonic()
            container = self._client.get_container_client(settings.azure_container)
            container.get_container_properties()
            return {"healthy": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    async def list_tables(self) -> list[str]:
        if not self._client:
            return []
        container = self._client.get_container_client(settings.azure_container)
        return [blob.name for blob in container.list_blobs() if blob.name.endswith((".parquet", ".csv", ".json"))]
