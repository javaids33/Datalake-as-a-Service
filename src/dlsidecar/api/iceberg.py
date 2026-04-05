"""Iceberg management endpoints: /iceberg/health, /iceberg/maintenance/*"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/iceberg")

_health = None
_maintenance = None


def init(iceberg_health, iceberg_maintenance):
    global _health, _maintenance
    _health = iceberg_health
    _maintenance = iceberg_maintenance


@router.get("/health")
async def health(table: str | None = None):
    """Per-table Iceberg health report."""
    if not _health:
        return {"tables": []}
    return {"tables": _health.get_health(table)}


@router.post("/maintenance/compaction")
async def run_compaction():
    """Trigger compaction manually."""
    if not _maintenance:
        return {"error": "maintenance not configured"}
    results = await _maintenance.run_compaction()
    return {"results": results}


@router.post("/maintenance/snapshot-expiry")
async def run_snapshot_expiry():
    """Trigger snapshot expiry manually."""
    if not _maintenance:
        return {"error": "maintenance not configured"}
    results = await _maintenance.run_snapshot_expiry()
    return {"results": results}


@router.post("/maintenance/orphan-cleanup")
async def run_orphan_cleanup(dry_run: bool = True):
    """Trigger orphan file cleanup. Dry-run by default."""
    if not _maintenance:
        return {"error": "maintenance not configured"}
    results = await _maintenance.run_orphan_cleanup(dry_run=dry_run)
    return {"results": results}


@router.post("/maintenance/manifest-rewrite")
async def run_manifest_rewrite():
    """Trigger manifest rewriting manually."""
    if not _maintenance:
        return {"error": "maintenance not configured"}
    results = await _maintenance.run_manifest_rewrite()
    return {"results": results}
