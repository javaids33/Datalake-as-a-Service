"""Push endpoint: POST /push — data ingestion into the lake."""

from __future__ import annotations

import base64
import io
import json
import logging
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, UploadFile, File
from pydantic import BaseModel

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event

router = APIRouter()
logger = logging.getLogger("dlsidecar.api.push")

_iceberg_manager = None
_duckdb = None


def init(iceberg_manager, duckdb_engine):
    global _iceberg_manager, _duckdb
    _iceberg_manager = iceberg_manager
    _duckdb = duckdb_engine


class PushTarget(BaseModel):
    type: str = "iceberg"  # iceberg | s3_parquet | duckdb_table
    database: str = "default"
    table: str
    mode: str = "append"  # append | overwrite | merge
    partition_by: list[str] | None = None
    register_hms: bool = True


class PushRequest(BaseModel):
    source: str = "inline"  # inline | s3_path
    data: str | None = None  # base64 Arrow IPC | JSON array
    s3_path: str | None = None
    target: PushTarget


@router.post("/push")
async def push(req: PushRequest):
    """Push data into the lake.

    Pipeline: validate schema -> write Iceberg -> register HMS -> confirm Starburst visible
    """
    # Decode data
    arrow_table = _decode_data(req)

    if req.target.type == "iceberg" and _iceberg_manager:
        result = _iceberg_manager.write(
            database=req.target.database,
            table_name=req.target.table,
            data=arrow_table,
            mode=req.target.mode,
            partition_by=req.target.partition_by,
            register_hms=req.target.register_hms,
        )
        return {
            "status": "success",
            "snapshot_id": result.get("snapshot_id"),
            "files_written": result.get("files_added"),
            "bytes_written": result.get("bytes_written"),
            "rows_written": result.get("rows_written"),
            "starburst_query_hint": f"SELECT * FROM {req.target.database}.{req.target.table}",
        }

    elif req.target.type == "duckdb_table" and _duckdb:
        _duckdb.create_table_from_arrow(req.target.table, arrow_table)
        emit_audit_event(
            "write",
            engine="duckdb",
            tables_written=[req.target.table],
            extra={"rows_written": len(arrow_table), "target_type": "duckdb_table"},
        )
        return {
            "status": "success",
            "rows_written": len(arrow_table),
            "target": req.target.table,
        }

    elif req.target.type == "s3_parquet" and _duckdb:
        s3_path = f"s3://{settings.s3_bucket_list[0]}/{req.target.database}/{req.target.table}"
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        # Write via DuckDB COPY
        _duckdb.execute(
            f"COPY (SELECT * FROM arrow_table) TO '{s3_path}' (FORMAT PARQUET)",
        )
        emit_audit_event(
            "write",
            engine="duckdb",
            tables_written=[s3_path],
            extra={"rows_written": len(arrow_table), "target_type": "s3_parquet"},
        )
        return {"status": "success", "path": s3_path, "rows_written": len(arrow_table)}

    return {"status": "error", "message": "Unsupported target type or missing manager"}


def _decode_data(req: PushRequest) -> pa.Table:
    """Decode push data from various sources into an Arrow table."""
    if req.source == "s3_path" and req.s3_path:
        return pq.read_table(req.s3_path)

    if req.data is None:
        raise ValueError("No data provided")

    # Try Arrow IPC (base64 encoded)
    try:
        raw = base64.b64decode(req.data)
        reader = pa.ipc.open_stream(raw)
        return reader.read_all()
    except Exception:
        pass

    # Try JSON array
    try:
        parsed = json.loads(req.data)
        if isinstance(parsed, list) and parsed:
            return pa.Table.from_pylist(parsed)
    except Exception:
        pass

    # Try parquet bytes (base64)
    try:
        raw = base64.b64decode(req.data)
        return pq.read_table(io.BytesIO(raw))
    except Exception:
        pass

    raise ValueError("Could not decode data — expected base64 Arrow IPC, JSON array, or base64 Parquet")
