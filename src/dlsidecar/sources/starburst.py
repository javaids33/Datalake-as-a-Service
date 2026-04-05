"""
Starburst source — thin wrapper around DuckDB's Trino extension.

DuckDB connects directly to Starburst via ATTACH '' AS starburst (TYPE TRINO, ...).
No separate Python trino client. No connection pool. DuckDB handles everything.

Starburst is always the primary federated catalog. The team's S3 bucket is also
a Starburst catalog — both DuckDB (direct via IRSA) and Starburst can access
the same data.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from dlsidecar.config import settings
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.starburst")


class StarburstSource(Source):
    """Starburst connectivity via DuckDB's Trino extension.

    All operations delegate to DuckDBEngine. This class exists to conform
    to the Source ABC for the connection registry (health loop, startup/shutdown).
    """

    name = "starburst"

    def __init__(self, duckdb_engine):
        from dlsidecar.engine.duckdb_engine import DuckDBEngine

        self._engine: DuckDBEngine = duckdb_engine
        self._catalog_name = "starburst"

    async def connect(self) -> None:
        """Attach Starburst as a DuckDB catalog via the Trino extension."""
        jwt_token = self._read_jwt()
        self._engine.attach_trino_catalog(
            catalog_name=self._catalog_name,
            host=settings.starburst_host,
            port=settings.starburst_port,
            jwt_token=jwt_token,
            tls_enabled=settings.starburst_tls_enabled,
            tls_cert_path=settings.starburst_tls_cert_path,
            auth_mode=settings.starburst_auth_mode,
        )

    async def disconnect(self) -> None:
        self._engine.detach_catalog(self._catalog_name)

    async def health_check(self) -> dict[str, Any]:
        return self._engine.trino_health_check(self._catalog_name)

    async def list_tables(self) -> list[str]:
        return self._engine.list_catalog_tables(self._catalog_name, settings.starburst_schema)

    def refresh_jwt(self, new_token: str) -> None:
        """Called by JWTManager on token refresh. Detaches and re-attaches with new token."""
        self._engine.refresh_trino_jwt(new_token, self._catalog_name)

    def _read_jwt(self) -> str | None:
        path = Path(settings.starburst_jwt_token_path)
        if path.exists():
            return path.read_text().strip()
        logger.warning("JWT token file not found: %s", path)
        return None
