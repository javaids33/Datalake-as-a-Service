"""Test all API endpoints with mock sources."""

import os

os.environ.setdefault("DLS_MODE", "dev")

import pytest
from fastapi.testclient import TestClient

from dlsidecar.api.health import router as health_router
from dlsidecar.api.metrics import router as metrics_router

# Minimal app for testing
from fastapi import FastAPI

app = FastAPI()
app.include_router(health_router)
app.include_router(metrics_router)

client = TestClient(app)


class TestHealthEndpoints:
    def test_healthz(self):
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "alive"

    def test_readyz_no_registry(self):
        response = client.get("/readyz")
        # Without registry, returns 503
        assert response.status_code == 503

    def test_status(self):
        response = client.get("/status")
        assert response.status_code == 200


class TestMetricsEndpoint:
    def test_metrics_returns_prometheus_format(self):
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "dlsidecar" in response.text or "python" in response.text
