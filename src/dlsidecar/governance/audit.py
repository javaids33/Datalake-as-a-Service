"""
Structured JSON audit logger — emits to stdout for CloudWatch / Fluent Bit pickup.
Every query, write, push, stream, maintenance, attach, and auth event is logged.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

from dlsidecar.config import settings

logger = logging.getLogger("dlsidecar.audit")
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


def _sql_hash(sql: str | None) -> str | None:
    if sql is None:
        return None
    normalized = " ".join(sql.split()).strip().lower()
    return hashlib.sha256(normalized.encode()).hexdigest()


def _sql_preview(sql: str | None, max_len: int = 200) -> str | None:
    if sql is None:
        return None
    return sql[:max_len]


def emit_audit_event(
    event_type: str,
    *,
    engine: str | None = None,
    sql: str | None = None,
    tables_accessed: list[str] | None = None,
    tables_written: list[str] | None = None,
    duration_ms: float | None = None,
    rows_scanned: int | None = None,
    bytes_scanned: int | None = None,
    rows_returned: int | None = None,
    bytes_returned: int | None = None,
    engine_routed_reason: str | None = None,
    iceberg_snapshot_id: int | None = None,
    error: str | None = None,
    caller_ip: str = "127.0.0.1",
    auth_principal: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Emit a structured audit event as JSON to stdout. Returns the event dict."""
    import os

    event = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "tenant_id": settings.tenant_id,
        "pod_name": os.environ.get("POD_NAME", "unknown"),
        "namespace": os.environ.get("POD_NAMESPACE", "unknown"),
        "engine": engine,
        "sql_hash": _sql_hash(sql),
        "sql_preview": _sql_preview(sql),
        "tables_accessed": tables_accessed or [],
        "tables_written": tables_written or [],
        "duration_ms": duration_ms,
        "rows_scanned": rows_scanned,
        "bytes_scanned": bytes_scanned,
        "rows_returned": rows_returned,
        "bytes_returned": bytes_returned,
        "engine_routed_reason": engine_routed_reason,
        "iceberg_snapshot_id": iceberg_snapshot_id,
        "error": error,
        "caller_ip": caller_ip,
        "auth_principal": auth_principal,
    }
    if extra:
        event.update(extra)

    logger.info(json.dumps(event, default=str))
    return event


@contextmanager
def audit_query(
    sql: str,
    engine: str,
    *,
    tables_accessed: list[str] | None = None,
    engine_routed_reason: str | None = None,
    caller_ip: str = "127.0.0.1",
    auth_principal: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Context manager that emits an audit event on exit with duration and error tracking."""
    ctx: dict[str, Any] = {
        "rows_scanned": None,
        "bytes_scanned": None,
        "rows_returned": None,
        "bytes_returned": None,
    }
    start = time.monotonic()
    error = None
    try:
        yield ctx
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        duration_ms = (time.monotonic() - start) * 1000
        emit_audit_event(
            "query",
            engine=engine,
            sql=sql,
            tables_accessed=tables_accessed,
            duration_ms=round(duration_ms, 2),
            rows_scanned=ctx.get("rows_scanned"),
            bytes_scanned=ctx.get("bytes_scanned"),
            rows_returned=ctx.get("rows_returned"),
            bytes_returned=ctx.get("bytes_returned"),
            engine_routed_reason=engine_routed_reason,
            error=error,
            caller_ip=caller_ip,
            auth_principal=auth_principal,
        )
