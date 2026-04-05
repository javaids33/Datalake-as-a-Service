"""Preflight health check endpoint: GET /healthcheck/preflight"""

from __future__ import annotations

from fastapi import APIRouter, Response

from dlsidecar.healthcheck.preflight import PreflightChecker

router = APIRouter(prefix="/healthcheck")


@router.get("/preflight")
async def preflight():
    """Run all preflight checks and return the report.

    Returns a structured report with pass/warn/fail/skip for each check,
    including actionable fix suggestions for every failure.
    """
    checker = PreflightChecker()
    report = await checker.run()
    status_code = 200 if report.healthy else 503
    return Response(
        content=__import__("json").dumps(report.to_dict(), indent=2, default=str),
        media_type="application/json",
        status_code=status_code,
    )


@router.get("/preflight/summary")
async def preflight_summary():
    """Quick summary — just pass/warn/fail counts."""
    checker = PreflightChecker()
    report = await checker.run()
    return {
        "healthy": report.healthy,
        "passed": report.passed,
        "warned": report.warned,
        "failed": report.failed,
        "skipped": report.skipped,
    }
