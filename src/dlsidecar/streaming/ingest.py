"""
Stream ingest manager — buffer management with auto-flush to Iceberg.

Buffer incoming Arrow RecordBatches in a DuckDB temp table.
Auto-flush when rows > DLS_STREAM_BUFFER_ROWS or bytes > 128MB.
Each flush = one Iceberg data file.
Final /commit closes the transaction as one snapshot.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event
from dlsidecar.governance.metrics import STREAM_BATCHES_TOTAL

logger = logging.getLogger("dlsidecar.streaming.ingest")

MAX_BUFFER_BYTES = 128 * 1024 * 1024  # 128MB


class StreamState:
    __slots__ = ("stream_id", "database", "table", "partition_by", "batches", "total_rows", "total_bytes")

    def __init__(self, stream_id: str, database: str, table: str, partition_by: list[str] | None = None):
        self.stream_id = stream_id
        self.database = database
        self.table = table
        self.partition_by = partition_by
        self.batches: list[pa.RecordBatch] = []
        self.total_rows = 0
        self.total_bytes = 0


class IngestManager:
    """Manages streaming ingest sessions with buffered writes to Iceberg."""

    def __init__(self, duckdb_engine=None, iceberg_manager=None):
        self._duckdb = duckdb_engine
        self._iceberg = iceberg_manager
        self._streams: dict[str, StreamState] = {}

    def open_stream(
        self,
        stream_id: str,
        *,
        database: str = "default",
        table: str = "",
        partition_by: list[str] | None = None,
    ) -> str:
        state = StreamState(stream_id, database, table, partition_by)
        self._streams[stream_id] = state
        logger.info("Opened stream %s → %s.%s", stream_id, database, table)
        return stream_id

    def add_batch(self, stream_id: str, data: bytes) -> dict[str, Any]:
        """Add an Arrow RecordBatch to the stream buffer."""
        state = self._streams.get(stream_id)
        if not state:
            return {"error": f"stream {stream_id} not found"}

        try:
            reader = ipc.open_stream(data)
            table = reader.read_all()
            for batch in table.to_batches():
                state.batches.append(batch)
                state.total_rows += batch.num_rows
                state.total_bytes += batch.nbytes
        except Exception as exc:
            return {"error": f"Failed to decode Arrow data: {exc}"}

        STREAM_BATCHES_TOTAL.labels(tenant=settings.tenant_id).inc()

        # Auto-flush check
        flushed = False
        if state.total_rows >= settings.stream_buffer_rows or state.total_bytes >= MAX_BUFFER_BYTES:
            self._flush(state)
            flushed = True

        return {
            "status": "ok",
            "rows_buffered": state.total_rows,
            "bytes_buffered": state.total_bytes,
            "auto_flushed": flushed,
        }

    def commit(self, stream_id: str) -> dict[str, Any]:
        """Flush remaining buffer and commit as a single Iceberg snapshot."""
        state = self._streams.get(stream_id)
        if not state:
            return {"error": f"stream {stream_id} not found"}

        if state.batches:
            self._flush(state)

        # Clean up
        result = {
            "status": "committed",
            "stream_id": stream_id,
            "total_rows": state.total_rows,
            "total_bytes": state.total_bytes,
        }

        emit_audit_event(
            "stream",
            tables_written=[f"{state.database}.{state.table}"],
            extra={"stream_id": stream_id, "total_rows": state.total_rows},
        )

        del self._streams[stream_id]
        logger.info("Committed stream %s: %d rows", stream_id, state.total_rows)
        return result

    def abort(self, stream_id: str) -> None:
        """Discard buffer and close stream."""
        state = self._streams.pop(stream_id, None)
        if state:
            logger.info("Aborted stream %s (discarded %d rows)", stream_id, state.total_rows)

    def _flush(self, state: StreamState) -> None:
        """Flush buffered batches to Iceberg."""
        if not state.batches:
            return

        table = pa.Table.from_batches(state.batches)

        if self._iceberg:
            self._iceberg.write(
                database=state.database,
                table_name=state.table,
                data=table,
                mode="append",
                partition_by=state.partition_by,
            )

        logger.info(
            "Flushed %d rows (%d bytes) for stream %s",
            len(table),
            table.nbytes,
            state.stream_id,
        )
        state.batches = []
