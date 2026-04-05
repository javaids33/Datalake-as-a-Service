"""Query endpoints: /query, /query/stream, /query/async"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

import pyarrow as pa
from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from dlsidecar.config import settings
from dlsidecar.engine.cache import QueryCache
from dlsidecar.engine.query_router import Engine, QueryRouter
from dlsidecar.governance.audit import audit_query
from dlsidecar.governance.metrics import QUERIES_TOTAL, QUERY_DURATION
from dlsidecar.governance.row_filter import RowFilterInjector

router = APIRouter()

# Initialized at startup
_duckdb = None
_router = None
_cache = None
_row_filter = None

# Async job store
_async_jobs: dict[str, dict[str, Any]] = {}


def init(duckdb_engine, query_router: QueryRouter, cache: QueryCache):
    global _duckdb, _router, _cache, _row_filter
    _duckdb = duckdb_engine
    _router = query_router
    _cache = cache
    _row_filter = RowFilterInjector()


class QueryRequest(BaseModel):
    sql: str
    format: str = "json"  # json | arrow | parquet | csv
    params: list[Any] | None = None


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    engine: str
    duration_ms: float
    routing_reason: str


@router.post("/query")
async def query(req: QueryRequest, request: Request):
    """Synchronous query execution with format selection."""
    # Check cache
    cached = _cache.get(req.sql) if _cache else None
    if cached is not None:
        return cached

    # Route the query
    plan = _router.route(req.sql)

    # Apply row filter for local queries
    sql = req.sql
    if plan.engine in (Engine.DUCKDB, Engine.HYBRID) and _row_filter and _row_filter.enabled:
        sql = _row_filter.inject(sql)

    start = time.monotonic()

    # All queries go through DuckDB — federated queries use the attached Trino catalog
    result = _duckdb.execute(
        sql,
        req.params,
        engine_label=plan.engine.value,
        tables_accessed=plan.tables_owned + plan.tables_unowned,
        engine_routed_reason=",".join(r.value for r in plan.reasons),
    )
    arrow_table = result.arrow()

    duration_ms = (time.monotonic() - start) * 1000

    # Format response
    if req.format == "arrow":
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, arrow_table.schema)
        writer.write_table(arrow_table)
        writer.close()
        return Response(
            content=sink.getvalue().to_pybytes(),
            media_type="application/vnd.apache.arrow.stream",
        )
    elif req.format == "parquet":
        import io

        import pyarrow.parquet as pq

        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        return Response(content=buf.getvalue(), media_type="application/octet-stream")
    elif req.format == "csv":
        import pyarrow.csv as csv

        buf = pa.BufferOutputStream()
        csv.write_csv(arrow_table, buf)
        return Response(content=buf.getvalue().to_pybytes(), media_type="text/csv")
    else:
        columns = arrow_table.column_names
        rows = arrow_table.to_pydict()
        row_list = []
        if columns:
            n = len(rows[columns[0]])
            for i in range(min(n, settings.max_result_rows)):
                row_list.append([rows[col][i] for col in columns])

        resp = {
            "columns": columns,
            "rows": row_list,
            "row_count": len(row_list),
            "engine": plan.engine.value,
            "duration_ms": round(duration_ms, 2),
            "routing_reason": ",".join(r.value for r in plan.reasons),
        }
        if _cache:
            _cache.put(req.sql, resp)
        return resp


@router.get("/query/stream")
async def query_stream(sql: str = Query(...), format: str = Query("json")):
    """SSE stream for JSON, chunked HTTP for Arrow IPC."""
    plan = _router.route(sql)

    effective_sql = sql
    if plan.engine in (Engine.DUCKDB, Engine.HYBRID) and _row_filter and _row_filter.enabled:
        effective_sql = _row_filter.inject(sql)

    if format == "json":

        async def generate():
            result = _duckdb.execute(effective_sql, tables_accessed=plan.tables_owned)
            batch_size = 1000
            arrow = result.arrow()
            for i in range(0, len(arrow), batch_size):
                chunk = arrow.slice(i, batch_size)
                data = json.dumps(chunk.to_pydict(), default=str)
                yield f"data: {data}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")
    else:

        async def generate_arrow():
            result = _duckdb.execute(effective_sql, tables_accessed=plan.tables_owned)
            arrow = result.arrow()
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, arrow.schema)
            batch_size = 10000
            for i in range(0, len(arrow), batch_size):
                chunk = arrow.slice(i, batch_size)
                for batch in chunk.to_batches():
                    writer.write_batch(batch)
            writer.close()
            yield sink.getvalue().to_pybytes()

        return StreamingResponse(generate_arrow(), media_type="application/vnd.apache.arrow.stream")


@router.post("/query/async")
async def query_async(req: QueryRequest):
    """Async query — returns job_id for polling."""
    job_id = str(uuid.uuid4())
    _async_jobs[job_id] = {"status": "running", "result": None, "error": None}

    async def run():
        try:
            plan = _router.route(req.sql)
            sql = req.sql
            if plan.engine in (Engine.DUCKDB, Engine.HYBRID) and _row_filter and _row_filter.enabled:
                sql = _row_filter.inject(sql)

            result = _duckdb.execute(sql, req.params, tables_accessed=plan.tables_owned)
            arrow = result.arrow()
            columns = arrow.column_names
            rows_dict = arrow.to_pydict()
            row_list = []
            if columns:
                n = len(rows_dict[columns[0]])
                for i in range(min(n, settings.max_result_rows)):
                    row_list.append([rows_dict[col][i] for col in columns])

            _async_jobs[job_id] = {
                "status": "completed",
                "result": {"columns": columns, "rows": row_list, "row_count": len(row_list)},
                "error": None,
            }
        except Exception as exc:
            _async_jobs[job_id] = {"status": "error", "result": None, "error": str(exc)}

    asyncio.create_task(run())
    return {"job_id": job_id}


@router.get("/query/async/{job_id}")
async def query_async_status(job_id: str):
    """Poll async query status."""
    job = _async_jobs.get(job_id)
    if not job:
        return Response(status_code=404, content='{"error": "job not found"}')
    return job
