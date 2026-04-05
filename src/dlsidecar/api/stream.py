"""Stream endpoints: /stream/open, /stream/{id}/batch, /stream/{id}/commit, DELETE /stream/{id}"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import APIRouter, Request, Response

from dlsidecar.config import settings

router = APIRouter()
logger = logging.getLogger("dlsidecar.api.stream")

_ingest_manager = None


def init(ingest_manager):
    global _ingest_manager
    _ingest_manager = ingest_manager


@router.post("/stream/open")
async def stream_open(request: Request):
    """Open a new stream for continuous ingest."""
    body = await request.json()
    database = body.get("database", "default")
    table = body.get("table")
    partition_by = body.get("partition_by")

    if not table:
        return Response(status_code=400, content='{"error": "table is required"}')

    stream_id = str(uuid.uuid4())
    if _ingest_manager:
        _ingest_manager.open_stream(stream_id, database=database, table=table, partition_by=partition_by)

    return {"stream_id": stream_id}


@router.put("/stream/{stream_id}/batch")
async def stream_batch(stream_id: str, request: Request):
    """Receive an Arrow RecordBatch chunk."""
    body = await request.body()
    if _ingest_manager:
        result = _ingest_manager.add_batch(stream_id, body)
        return result
    return {"status": "no ingest manager"}


@router.post("/stream/{stream_id}/commit")
async def stream_commit(stream_id: str):
    """Flush buffer and commit as Iceberg snapshot."""
    if _ingest_manager:
        result = _ingest_manager.commit(stream_id)
        return result
    return {"status": "no ingest manager"}


@router.delete("/stream/{stream_id}")
async def stream_abort(stream_id: str):
    """Abort stream and discard buffer."""
    if _ingest_manager:
        _ingest_manager.abort(stream_id)
    return {"status": "aborted", "stream_id": stream_id}
