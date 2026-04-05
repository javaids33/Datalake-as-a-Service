"""
Live connection validator for the onboarding wizard.
Tests S3, Starburst, HMS, and Iceberg catalog connections.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from dlsidecar.config import Settings

logger = logging.getLogger("dlsidecar.onboarding.validator")


async def validate_s3(bucket: str, region: str = "us-east-1", endpoint_url: str = "") -> dict[str, Any]:
    """Test S3 bucket accessibility."""
    try:
        import boto3

        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url

        s3 = boto3.client("s3", **kwargs)
        start = time.monotonic()
        s3.head_bucket(Bucket=bucket)
        latency = (time.monotonic() - start) * 1000
        return {"healthy": True, "latency_ms": round(latency, 2), "error": None, "bucket": bucket}
    except Exception as exc:
        return {"healthy": False, "latency_ms": None, "error": str(exc), "bucket": bucket}


async def validate_starburst(host: str, port: int = 8443, auth_mode: str = "jwt", jwt_token: str = "") -> dict[str, Any]:
    """Test Starburst connectivity."""
    try:
        import trino

        kwargs: dict[str, Any] = {"host": host, "port": port, "http_scheme": "https"}
        if auth_mode == "jwt" and jwt_token:
            kwargs["auth"] = trino.auth.JWTAuthentication(jwt_token)

        conn = trino.dbapi.connect(**kwargs)
        start = time.monotonic()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        latency = (time.monotonic() - start) * 1000
        conn.close()
        return {"healthy": True, "latency_ms": round(latency, 2), "error": None}
    except Exception as exc:
        return {"healthy": False, "latency_ms": None, "error": str(exc)}


async def validate_hms(uri: str) -> dict[str, Any]:
    """Test HMS (Hive Metastore) connectivity."""
    try:
        from pyhive import hive

        clean_uri = uri.replace("thrift://", "")
        host, _, port = clean_uri.partition(":")
        port_int = int(port) if port else 9083

        start = time.monotonic()
        conn = hive.connect(host=host, port=port_int, auth="NOSASL")
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        cursor.fetchall()
        latency = (time.monotonic() - start) * 1000
        conn.close()
        return {"healthy": True, "latency_ms": round(latency, 2), "error": None}
    except Exception as exc:
        return {"healthy": False, "latency_ms": None, "error": str(exc)}


async def validate_iceberg_catalog(catalog_type: str, catalog_uri: str = "", warehouse: str = "", glue_catalog_id: str = "") -> dict[str, Any]:
    """Test Iceberg catalog connectivity."""
    try:
        from pyiceberg.catalog import load_catalog

        config: dict[str, Any] = {"type": catalog_type}
        if catalog_type == "rest":
            config["uri"] = catalog_uri
        elif catalog_type == "glue":
            config["warehouse"] = warehouse
            if glue_catalog_id:
                config["catalog-id"] = glue_catalog_id
        elif catalog_type == "hive":
            config["uri"] = catalog_uri
            config["warehouse"] = warehouse

        start = time.monotonic()
        catalog = load_catalog("test", **config)
        namespaces = catalog.list_namespaces()
        latency = (time.monotonic() - start) * 1000
        return {
            "healthy": True,
            "latency_ms": round(latency, 2),
            "error": None,
            "namespaces": [str(ns) for ns in namespaces],
        }
    except Exception as exc:
        return {"healthy": False, "latency_ms": None, "error": str(exc)}


async def validate_all(cfg: Settings) -> dict[str, dict[str, Any]]:
    """Run all applicable connection validations."""
    results: dict[str, dict[str, Any]] = {}

    for bucket in cfg.s3_bucket_list:
        results[f"s3:{bucket}"] = await validate_s3(bucket, cfg.s3_region, cfg.s3_endpoint_url)

    if cfg.starburst_enabled and cfg.starburst_host:
        results["starburst"] = await validate_starburst(cfg.starburst_host, cfg.starburst_port)

    if cfg.hms_enabled and cfg.hms_uri:
        results["hms"] = await validate_hms(cfg.hms_uri)

    if cfg.iceberg_enabled:
        results["iceberg_catalog"] = await validate_iceberg_catalog(
            cfg.iceberg_catalog_type, cfg.iceberg_catalog_uri, cfg.iceberg_warehouse, cfg.iceberg_glue_catalog_id
        )

    return results
