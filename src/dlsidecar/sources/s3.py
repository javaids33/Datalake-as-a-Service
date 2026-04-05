"""
S3 source — configures DuckDB httpfs secrets per bucket using IRSA.
Never stores access keys. Uses instance metadata credential refresh.
"""

from __future__ import annotations

import logging
from typing import Any

import duckdb

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.s3")


class S3Source(Source):
    name = "s3"

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None):
        self._conn = conn
        self._configured_buckets: list[str] = []

    async def connect(self) -> None:
        if self._conn is None:
            return
        self._configure_httpfs()

    async def disconnect(self) -> None:
        self._configured_buckets = []

    async def health_check(self) -> dict[str, Any]:
        if not self._configured_buckets:
            return {"healthy": False, "latency_ms": None, "error": "no buckets configured"}
        return {"healthy": True, "latency_ms": None, "error": None}

    async def list_tables(self) -> list[str]:
        return [f"s3://{b}" for b in self._configured_buckets]

    def _configure_httpfs(self) -> None:
        """Set up one httpfs SECRET per bucket using IRSA credential provider."""
        bucket_config = settings.bucket_config_parsed
        default_region = settings.s3_region
        endpoint_url = settings.s3_endpoint_url
        path_style = settings.s3_path_style

        for bucket in settings.s3_bucket_list:
            bc = bucket_config.get(bucket, {})
            region = bc.get("region", default_region)
            ep = bc.get("endpoint", endpoint_url)

            secret_name = f"s3_secret_{bucket.replace('-', '_').replace('.', '_')}"

            # Drop existing secret if any (idempotent reconfigure)
            try:
                self._conn.execute(f"DROP SECRET IF EXISTS {secret_name}")
            except Exception:
                pass

            # Use credential_chain provider which picks up IRSA automatically
            parts = [
                f"CREATE SECRET {secret_name} (",
                "  TYPE S3,",
                f"  REGION '{region}',",
                "  PROVIDER CREDENTIAL_CHAIN",
            ]
            if ep:
                parts.append(f", ENDPOINT '{ep}'")
            if path_style:
                parts.append(", URL_STYLE 'path'")
            parts.append(")")

            sql = "\n".join(parts)
            try:
                self._conn.execute(sql)
                self._configured_buckets.append(bucket)
                logger.info("Configured httpfs secret for bucket %s (region=%s)", bucket, region)
            except Exception as exc:
                logger.error("Failed to configure httpfs for bucket %s: %s", bucket, exc)

    def is_owned_bucket(self, s3_path: str) -> bool:
        """Check if an S3 path belongs to one of the team's configured buckets."""
        for bucket in self._configured_buckets:
            if s3_path.startswith(f"s3://{bucket}/") or s3_path.startswith(f"s3a://{bucket}/"):
                return True
        return False
