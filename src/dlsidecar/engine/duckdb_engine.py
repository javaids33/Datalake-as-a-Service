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
    """Singleton DuckDB connection with thread-safe execution."""

    def __init__(self):
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()

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
