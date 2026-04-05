"""Catalog endpoints: /catalog/*"""

from __future__ import annotations

from fastapi import APIRouter

from dlsidecar.config import settings

router = APIRouter(prefix="/catalog")

_registry = None
_duckdb = None


def init(registry, duckdb_engine):
    global _registry, _duckdb
    _registry = registry
    _duckdb = duckdb_engine


@router.get("/tables")
async def list_tables():
    """List all tables visible from all connected sources."""
    tables: dict[str, list[str]] = {}

    if _duckdb:
        try:
            result = _duckdb.execute_fetchall("SELECT table_schema, table_name FROM information_schema.tables")
            tables["duckdb"] = [f"{row[0]}.{row[1]}" for row in result]
        except Exception:
            tables["duckdb"] = []

    if _registry:
        for name, source in _registry.sources.items():
            try:
                source_tables = await source.list_tables()
                tables[name] = source_tables
            except Exception:
                tables[name] = []

    return {"tables": tables}


@router.get("/schemas")
async def list_schemas():
    """List schemas/namespaces."""
    schemas = {
        "owned": list(settings.owned_namespace_set),
    }
    if _duckdb:
        try:
            result = _duckdb.execute_fetchall("SELECT schema_name FROM information_schema.schemata")
            schemas["duckdb"] = [row[0] for row in result]
        except Exception:
            schemas["duckdb"] = []
    return {"schemas": schemas}


@router.get("/table/{database}/{table}")
async def describe_table(database: str, table: str):
    """Describe a table's schema."""
    if _duckdb:
        try:
            result = _duckdb.execute_fetchall(f"DESCRIBE {database}.{table}")
            return {
                "table": f"{database}.{table}",
                "columns": [
                    {"name": row[0], "type": row[1], "null": row[2], "key": row[3]}
                    for row in result
                ],
            }
        except Exception as exc:
            return {"error": str(exc)}
    return {"error": "DuckDB not available"}
