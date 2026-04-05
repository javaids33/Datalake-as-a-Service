"""Source management endpoints: /sources/attach, /sources/{name}"""

from __future__ import annotations

from fastapi import APIRouter, Response
from pydantic import BaseModel

from dlsidecar.governance.audit import emit_audit_event

router = APIRouter(prefix="/sources")

_registry = None


def init(registry):
    global _registry
    _registry = registry


@router.get("/")
async def list_sources():
    """List all registered sources and their health status."""
    if not _registry:
        return {"sources": {}}
    return {"sources": _registry.health_status()}


@router.get("/{name}")
async def get_source(name: str):
    """Get health status for a specific source."""
    if not _registry:
        return Response(status_code=404)
    health = _registry.health_status().get(name)
    if not health:
        return Response(status_code=404, content=f'{{"error": "source {name} not found"}}')
    return {"name": name, **health}


@router.post("/attach")
async def attach_source(request_body: dict):
    """Attach a new data source at runtime (dev mode)."""
    emit_audit_event("attach", extra={"source_config": {k: v for k, v in request_body.items() if k != "password"}})
    return {"status": "accepted", "message": "Runtime source attachment is a dev-mode feature"}
