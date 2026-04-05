"""
Starburst source — Trino Python client with JWT Bearer auth,
connection pooling, TLS verification, and pushdown support.
"""

from __future__ import annotations

import logging
import queue
import time
from typing import Any

import trino

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event
from dlsidecar.governance.metrics import CROSS_TEAM_QUERIES, QUERIES_TOTAL, QUERY_DURATION
from dlsidecar.sources.base import Source

logger = logging.getLogger("dlsidecar.sources.starburst")


class StarburstSource(Source):
    name = "starburst"

    def __init__(self):
        self._pool: queue.Queue[trino.dbapi.Connection] = queue.Queue()
        self._pool_size = settings.starburst_connection_pool_size
        self._jwt_token: str | None = None

    async def connect(self) -> None:
        self._jwt_token = self._read_jwt()
        for _ in range(self._pool_size):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)

    async def disconnect(self) -> None:
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except (queue.Empty, Exception):
                pass

    async def health_check(self) -> dict[str, Any]:
        conn = None
        try:
            conn = self._pool.get(timeout=5)
            start = time.monotonic()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            latency = (time.monotonic() - start) * 1000
            return {"healthy": True, "latency_ms": round(latency, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}
        finally:
            if conn:
                self._pool.put(conn)

    async def list_tables(self) -> list[str]:
        """List tables visible through Starburst."""
        if self._pool.empty():
            return []
        try:
            rows = self.execute_query(
                f"SHOW TABLES FROM {settings.starburst_catalog}.{settings.starburst_schema}"
            )
            return [
                f"{settings.starburst_catalog}.{settings.starburst_schema}.{row[0]}"
                for row in rows
            ]
        except Exception as exc:
            logger.error("Failed to list Starburst tables: %s", exc)
            return []

    def execute_query(
        self,
        sql: str,
        *,
        tenant_context: str | None = None,
        target_catalog: str | None = None,
    ) -> list[tuple]:
        """Execute SQL on Starburst with pooling, metrics, and audit."""
        conn = None
        try:
            conn = self._pool.get(timeout=5)
            cursor = conn.cursor()

            if tenant_context:
                cursor.execute(f"SET SESSION query_tag = '{tenant_context}'")

            start = time.monotonic()
            cursor.execute(sql)
            rows = cursor.fetchall()
            duration = time.monotonic() - start

            QUERY_DURATION.labels(engine="starburst", tenant=settings.tenant_id).observe(duration)
            QUERIES_TOTAL.labels(engine="starburst", status="success", tenant=settings.tenant_id).inc()

            if target_catalog:
                CROSS_TEAM_QUERIES.labels(tenant=settings.tenant_id, target_catalog=target_catalog).inc()

            return rows
        except Exception as exc:
            QUERIES_TOTAL.labels(engine="starburst", status="error", tenant=settings.tenant_id).inc()
            raise
        finally:
            if conn:
                self._pool.put(conn)

    def refresh_jwt(self, new_token: str) -> None:
        """Called by JWTManager on token refresh. Rebuilds connection pool."""
        logger.info("Refreshing Starburst connection pool with new JWT")
        self._jwt_token = new_token
        old_pool = self._pool
        self._pool = queue.Queue()

        # Drain old pool
        while not old_pool.empty():
            try:
                conn = old_pool.get_nowait()
                conn.close()
            except (queue.Empty, Exception):
                pass

        # Build new pool
        for _ in range(self._pool_size):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)

    def _read_jwt(self) -> str | None:
        from pathlib import Path

        path = Path(settings.starburst_jwt_token_path)
        if path.exists():
            return path.read_text().strip()
        logger.warning("JWT token file not found: %s", path)
        return None

    def _create_connection(self) -> trino.dbapi.Connection | None:
        try:
            kwargs: dict[str, Any] = {
                "host": settings.starburst_host,
                "port": settings.starburst_port,
                "catalog": settings.starburst_catalog,
                "schema": settings.starburst_schema,
            }

            if settings.starburst_auth_mode == "jwt" and self._jwt_token:
                kwargs["http_scheme"] = "https" if settings.starburst_tls_enabled else "http"
                kwargs["auth"] = trino.auth.JWTAuthentication(self._jwt_token)

            if settings.starburst_tls_enabled and settings.starburst_tls_cert_path:
                kwargs["verify"] = settings.starburst_tls_cert_path
            elif settings.starburst_tls_enabled:
                kwargs["verify"] = True

            return trino.dbapi.connect(**kwargs)
        except Exception as exc:
            logger.error("Failed to create Starburst connection: %s", exc)
            return None
