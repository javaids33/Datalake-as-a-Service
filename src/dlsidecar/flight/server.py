"""
Arrow Flight server — high-throughput columnar pull/push for Spark, Trino, DataFusion.
Implements do_get, do_put, list_flights.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event

logger = logging.getLogger("dlsidecar.flight.server")


class DLSFlightServer(flight.FlightServerBase):
    """Arrow Flight server for dlsidecar."""

    def __init__(self, duckdb_engine=None, query_router=None, iceberg_manager=None, **kwargs):
        location = f"grpc://0.0.0.0:{settings.flight_port}"
        super().__init__(location, **kwargs)
        self._duckdb = duckdb_engine
        self._router = query_router
        self._iceberg_manager = iceberg_manager
        self._datasets: dict[str, pa.Table] = {}
        self._lock = threading.Lock()

    def list_flights(self, context, criteria):
        """List available datasets/tables."""
        # List DuckDB tables
        if self._duckdb:
            try:
                rows = self._duckdb.execute_fetchall(
                    "SELECT table_schema, table_name FROM information_schema.tables"
                )
                for schema, name in rows:
                    descriptor = flight.FlightDescriptor.for_path(schema, name)
                    # Get schema for this table
                    try:
                        result = self._duckdb.execute_arrow(f'SELECT * FROM "{schema}"."{name}" LIMIT 0')
                        yield flight.FlightInfo(
                            result.schema,
                            descriptor,
                            [],
                            -1,
                            -1,
                        )
                    except Exception:
                        pass
            except Exception as exc:
                logger.error("Failed to list flights: %s", exc)

    def get_flight_info(self, context, descriptor):
        """Get info about a specific dataset."""
        key = "/".join(p.decode() if isinstance(p, bytes) else p for p in descriptor.path)
        if key in self._datasets:
            table = self._datasets[key]
            return flight.FlightInfo(
                table.schema,
                descriptor,
                [],
                table.num_rows,
                table.nbytes,
            )
        raise flight.FlightUnavailableError(f"Dataset not found: {key}")

    def do_get(self, context, ticket):
        """Execute a query or retrieve a dataset and stream results as Arrow."""
        sql = ticket.ticket.decode("utf-8")

        emit_audit_event("query", engine="flight", sql=sql)

        if self._duckdb:
            plan = self._router.route(sql) if self._router else None
            result = self._duckdb.execute_arrow(sql)
            return flight.RecordBatchStream(result)

        raise flight.FlightUnavailableError("DuckDB engine not available")

    def do_put(self, context, descriptor, reader, writer):
        """Receive data from a client and write to the lake."""
        key = "/".join(p.decode() if isinstance(p, bytes) else p for p in descriptor.path)

        batches = []
        for chunk in reader:
            batches.append(chunk.data)

        if batches:
            table = pa.Table.from_batches(batches)

            with self._lock:
                self._datasets[key] = table

            # If it looks like a table path (db/table), write to Iceberg
            parts = key.split("/")
            if len(parts) == 2 and self._iceberg_manager:
                self._iceberg_manager.write(
                    database=parts[0],
                    table_name=parts[1],
                    data=table,
                )

            emit_audit_event(
                "push",
                engine="flight",
                tables_written=[key],
                extra={"rows_written": len(table), "bytes_written": table.nbytes},
            )


def start_flight_server(duckdb_engine=None, query_router=None, iceberg_manager=None) -> DLSFlightServer:
    """Start the Flight server in a background thread."""
    server = DLSFlightServer(
        duckdb_engine=duckdb_engine,
        query_router=query_router,
        iceberg_manager=iceberg_manager,
    )

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    logger.info("Arrow Flight server started on port %d", settings.flight_port)
    return server
