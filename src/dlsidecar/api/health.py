"""Health check endpoints: /healthz, /readyz, /status"""

from __future__ import annotations

from fastapi import APIRouter, Response

router = APIRouter()

# These will be set during app startup
_registry = None
_duckdb = None


def init(registry, duckdb_engine):
    global _registry, _duckdb
    _registry = registry
    _duckdb = duckdb_engine


@router.get("/healthz")
async def healthz():
    """Liveness probe — always returns 200 if the process is running."""
    return {"status": "alive"}


@router.get("/readyz")
async def readyz(response: Response):
    """Readiness probe — returns 200 only when all enabled sources are healthy."""
    if _registry and _registry.all_healthy:
        return {"status": "ready"}
    response.status_code = 503
    return {"status": "not_ready", "sources": _registry.health_status() if _registry else {}}


@router.get("/status")
async def status():
    """Detailed status of all connections and components."""
    return {
        "sources": _registry.health_status() if _registry else {},
        "duckdb": "connected" if _duckdb and _duckdb.connection else "disconnected",
    }
