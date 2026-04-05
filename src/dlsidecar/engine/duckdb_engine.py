"""
DuckDB engine — singleton connection, extension loading, httpfs secrets,
thread-safe query execution with audit emission.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import duckdb
import pyarrow as pa

from dlsidecar.config import settings
from dlsidecar.governance.audit import audit_query
from dlsidecar.governance.metrics import (
    BYTES_SCANNED_TOTAL,
    DUCKDB_MEMORY_BYTES,
    QUERIES_TOTAL,
    QUERY_DURATION,
    ROWS_SCANNED_TOTAL,
)

logger = logging.getLogger("dlsidecar.engine.duckdb")


class DuckDBEngine:
    """Singleton DuckDB connection with thread-safe execution.

    DuckDB is the single query engine. External sources (Starburst/Trino,
    Postgres, MySQL) are attached as catalogs via DuckDB extensions.
    """

    def __init__(self):
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()
        self._attached_catalogs: dict[str, str] = {}  # name -> type

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("DuckDB engine not initialized. Call startup() first.")
        return self._conn

    def startup(self) -> duckdb.DuckDBPyConnection:
        """Initialize DuckDB connection with configured memory limit, threads, and extensions."""
        if settings.duckdb_mode == "persistent":
            self._conn = duckdb.connect(settings.duckdb_path)
        else:
            self._conn = duckdb.connect(":memory:")

        self._conn.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
        self._conn.execute(f"SET threads = {settings.duckdb_threads}")

        self._load_extensions()
        logger.info(
            "DuckDB started: mode=%s, memory_limit=%s, threads=%d",
            settings.duckdb_mode,
            settings.duckdb_memory_limit,
            settings.duckdb_threads,
        )
        return self._conn

    def shutdown(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB connection closed")

    def _load_extensions(self) -> None:
        for ext in settings.extension_list:
            try:
                self._conn.execute(f"INSTALL '{ext}'")
                self._conn.execute(f"LOAD '{ext}'")
                logger.info("Loaded DuckDB extension: %s", ext)
            except Exception as exc:
                logger.warning("Failed to load DuckDB extension %s: %s", ext, exc)

    def execute(
        self,
        sql: str,
        params: list[Any] | None = None,
        *,
        engine_label: str = "duckdb",
        tables_accessed: list[str] | None = None,
        engine_routed_reason: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Execute SQL with thread safety, audit logging, and metrics."""
        with self._lock:
            with audit_query(
                sql,
                engine_label,
                tables_accessed=tables_accessed,
                engine_routed_reason=engine_routed_reason,
            ) as ctx:
                start = time.monotonic()
                try:
                    if params:
                        result = self._conn.execute(sql, params)
                    else:
                        result = self._conn.execute(sql)

                    duration = time.monotonic() - start
                    QUERY_DURATION.labels(engine=engine_label, tenant=settings.tenant_id).observe(duration)
                    QUERIES_TOTAL.labels(engine=engine_label, status="success", tenant=settings.tenant_id).inc()
                    return result
                except Exception:
                    QUERIES_TOTAL.labels(engine=engine_label, status="error", tenant=settings.tenant_id).inc()
                    raise

    def execute_arrow(
        self,
        sql: str,
        params: list[Any] | None = None,
        *,
        tables_accessed: list[str] | None = None,
        engine_routed_reason: str | None = None,
    ) -> pa.Table:
        """Execute SQL and return results as an Arrow table."""
        rel = self.execute(
            sql,
            params,
            tables_accessed=tables_accessed,
            engine_routed_reason=engine_routed_reason,
        )
        return rel.arrow()

    def execute_fetchall(
        self,
        sql: str,
        params: list[Any] | None = None,
        *,
        tables_accessed: list[str] | None = None,
        engine_routed_reason: str | None = None,
    ) -> list[tuple]:
        """Execute SQL and return results as list of tuples."""
        rel = self.execute(
            sql,
            params,
            tables_accessed=tables_accessed,
            engine_routed_reason=engine_routed_reason,
        )
        return rel.fetchall()

    def update_memory_metric(self) -> None:
        """Update the DuckDB memory usage Prometheus gauge."""
        try:
            result = self._conn.execute(
                "SELECT current_setting('memory_limit') as limit"
            ).fetchone()
            if result:
                DUCKDB_MEMORY_BYTES.set(0)  # Placeholder — actual usage from pragma
        except Exception:
            pass

    def create_view(self, name: str, sql: str) -> None:
        with self._lock:
            self._conn.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")

    def create_table_from_arrow(self, name: str, table: pa.Table) -> None:
        with self._lock:
            self._conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM arrow_table", {"arrow_table": table})

    # ── Catalog attachment (Trino, Postgres, MySQL, etc.) ─────────────────

    def attach_trino_catalog(
        self,
        catalog_name: str = "starburst",
        host: str = "",
        port: int = 8443,
        jwt_token: str | None = None,
        tls_enabled: bool = True,
        tls_cert_path: str = "",
        auth_mode: str = "jwt",
    ) -> bool:
        """Attach a Trino/Starburst catalog via DuckDB's Trino extension.

        DuckDB connects directly to Starburst using the Trino wire protocol.
        No separate Python trino client needed.
        """
        with self._lock:
            try:
                params = [f"HOST '{host}'", f"PORT {port}"]
                if auth_mode == "jwt" and jwt_token:
                    params.append(f"TOKEN '{jwt_token}'")
                if tls_enabled:
                    params.append("USE_TLS true")
                    if tls_cert_path:
                        params.append(f"TLS_CA_CERT_FILE '{tls_cert_path}'")

                param_str = ", ".join(params)
                sql = f"ATTACH '' AS {catalog_name} (TYPE TRINO, {param_str})"
                self._conn.execute(sql)
                self._attached_catalogs[catalog_name] = "trino"
                logger.info("Attached Trino catalog: %s -> %s:%d", catalog_name, host, port)
                return True
            except Exception as exc:
                logger.error("Failed to attach Trino catalog %s: %s", catalog_name, exc)
                return False

    def detach_catalog(self, catalog_name: str) -> None:
        """Detach a previously attached catalog."""
        with self._lock:
            try:
                self._conn.execute(f"DETACH {catalog_name}")
                self._attached_catalogs.pop(catalog_name, None)
                logger.info("Detached catalog: %s", catalog_name)
            except Exception as exc:
                logger.warning("Failed to detach catalog %s: %s", catalog_name, exc)

    def refresh_trino_jwt(self, new_token: str, catalog_name: str = "starburst") -> None:
        """Re-attach a Trino catalog with a new JWT token.

        Called by JWTManager on token refresh.
        """
        logger.info("Refreshing Trino catalog %s with new JWT", catalog_name)
        self.detach_catalog(catalog_name)
        self.attach_trino_catalog(
            catalog_name=catalog_name,
            host=settings.starburst_host,
            port=settings.starburst_port,
            jwt_token=new_token,
            tls_enabled=settings.starburst_tls_enabled,
            tls_cert_path=settings.starburst_tls_cert_path,
            auth_mode=settings.starburst_auth_mode,
        )

    def trino_health_check(self, catalog_name: str = "starburst") -> dict[str, Any]:
        """Health check for an attached Trino catalog."""
        if catalog_name not in self._attached_catalogs:
            return {"healthy": False, "latency_ms": None, "error": "catalog not attached"}
        try:
            start = time.monotonic()
            self.execute_fetchall(f"SELECT 1 FROM {catalog_name}.information_schema.tables LIMIT 1")
            latency = (time.monotonic() - start) * 1000
            return {"healthy": True, "latency_ms": round(latency, 2), "error": None}
        except Exception as exc:
            return {"healthy": False, "latency_ms": None, "error": str(exc)}

    def list_catalog_tables(self, catalog_name: str = "starburst", schema: str = "default") -> list[str]:
        """List tables visible through an attached catalog."""
        try:
            rows = self.execute_fetchall(
                f"SELECT table_catalog, table_schema, table_name "
                f"FROM {catalog_name}.information_schema.tables "
                f"WHERE table_schema = '{schema}'"
            )
            return [f"{r[0]}.{r[1]}.{r[2]}" for r in rows]
        except Exception as exc:
            logger.error("Failed to list tables from %s: %s", catalog_name, exc)
            return []

    def attach_datasource(self, name: str, dsn: str, ds_type: str) -> bool:
        """Attach an external datasource (postgres, mysql, etc.) via DuckDB extensions."""
        with self._lock:
            try:
                self._conn.execute(f"ATTACH '{dsn}' AS {name} (TYPE {ds_type})")
                self._attached_catalogs[name] = ds_type.lower()
                logger.info("Attached %s datasource: %s", ds_type, name)
                return True
            except Exception as exc:
                logger.error("Failed to attach %s datasource %s: %s", ds_type, name, exc)
                return False

    @property
    def attached_catalogs(self) -> dict[str, str]:
        return dict(self._attached_catalogs)
